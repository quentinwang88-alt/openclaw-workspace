"""Auth recovery is one-shot, scoped and never replays ambiguous commits."""
from datetime import datetime
import json
from pathlib import Path
import stat
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.neobund_auth import (
    NeoBundAuthError, _credentials_from_values, persist_validated_credentials,
    read_browser_credentials,
)
from app.neobund_publish import NeoBundClient, NeoBundPublishAdapter, AmbiguousPublishError
from app.models import PublishTaskStatus


def response(code=200, payload=None):
    item = requests.Response()
    item.status_code = code
    item._content = json.dumps(payload if payload is not None else {"records": []}).encode()
    return item


class AuthRecoveryTests(unittest.TestCase):
    def test_live_cookie_user_info_envelope(self):
        credentials = _credentials_from_values({}, [{
            'name': 'auth', 'domain': 'www.neobund.ai',
            'value': '{"userInfo":{"access_token":"test-token"}}',
        }])
        self.assertEqual(credentials['access_token'], 'test-token')

    def make_client(self, *responses):
        session = Mock()
        session.request.side_effect = responses
        provider = Mock(return_value={"access_token": "new-secret", "cookie": "auth=new"})
        client = NeoBundClient(session=session, access_token="expired-secret", auth_refresh_provider=provider)
        return client, session, provider

    def test_401_refreshes_once_validates_readonly_then_replays_post(self):
        client, session, provider = self.make_client(response(401), response(), response(payload={"id": 1}))
        self.assertEqual(client.post("/shoppable/video/commit", {"remark": "one"}), {"id": 1})
        provider.assert_called_once()
        self.assertEqual([c.args[0] for c in session.request.call_args_list], ["POST", "GET", "POST"])
        self.assertIn("/tk/auth/list", session.request.call_args_list[1].args[1])
        self.assertEqual(session.request.call_args_list[-1].kwargs["headers"]["Authorization"], "Bearer new-secret")
        self.assertEqual(session.request.call_args_list[0].kwargs["json"], session.request.call_args_list[2].kwargs["json"])

    def test_failed_validation_pauses_remaining_requests_and_redacts(self):
        client, session, provider = self.make_client(response(401), response(401))
        for _ in range(2):
            with self.assertRaises(NeoBundAuthError) as error:
                client.post("/commit", {})
            self.assertTrue(error.exception.auth_failed)
            self.assertNotIn("secret", str(error.exception))
        self.assertEqual(session.request.call_count, 2)
        provider.assert_called_once()
        self.assertEqual(client.access_token, "expired-secret")

    def test_second_auth_rejection_cannot_trigger_another_refresh(self):
        client, session, provider = self.make_client(response(401), response(), response(), response(401))
        client.get("/first")
        with self.assertRaises(NeoBundAuthError):
            client.get("/second")
        provider.assert_called_once()
        self.assertTrue(client.auth_failed)

    def test_replayed_post_401_pauses_without_third_post(self):
        client, session, provider = self.make_client(response(401), response(), response(401))
        with self.assertRaises(NeoBundAuthError):
            client.post("/commit", {})
        self.assertEqual(session.request.call_count, 3)

    def test_post_timeout_never_calls_provider_or_replays(self):
        client, session, provider = self.make_client(requests.Timeout("timeout"))
        with self.assertRaises(requests.Timeout):
            client.post("/commit", {})
        provider.assert_not_called()
        session.request.assert_called_once()

    def test_server_error_never_calls_provider_or_replays(self):
        client, session, provider = self.make_client(response(500))
        with self.assertRaises(requests.HTTPError):
            client.post("/commit", {})
        provider.assert_not_called()
        session.request.assert_called_once()

    def test_json_401_is_explicit_rejection(self):
        client, session, provider = self.make_client(response(payload={"code": 401}), response(), response())
        client.get("/list")
        provider.assert_called_once()

    def test_untrusted_refresh_base_is_rejected_before_reading_browser(self):
        client, session, provider = self.make_client(response(401))
        client.base_url = "https://neobund.ai.attacker.invalid/np"
        with self.assertRaises(NeoBundAuthError):
            client.get("/list")
        provider.assert_not_called()

    def test_config_only_saved_after_validation(self):
        client, _, _ = self.make_client(response(401), response(401))
        client.auth_config_path = "/unused"
        with patch("app.neobund_publish.persist_validated_credentials") as persist:
            with self.assertRaises(NeoBundAuthError):
                client.get("/list")
            persist.assert_not_called()

    def test_atomic_config_preserves_other_fields_and_permissions(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(json.dumps({"existing": {"a": 1}, "neobund_access_token": "old"}))
            persist_validated_credentials(str(path), {"access_token": "new", "cookie": "auth=valid"})
            self.assertEqual(json.loads(path.read_text())["existing"], {"a": 1})
            self.assertEqual(json.loads(path.read_text())["neobund_access_token"], "new")
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertEqual(len(list(Path(directory).iterdir())), 1)

    def test_scoped_cookies_skip_cross_site_and_analytics(self):
        credentials = _credentials_from_values({}, [
            {"name": "auth", "value": '{"access_token":"valid"}', "domain": "www.neobund.ai"},
            {"name": "token", "value": "evil", "domain": "neobund.ai.attacker.invalid"},
            {"name": "_ga", "value": "tracking", "domain": ".neobund.ai"},
        ])
        self.assertEqual(credentials["access_token"], "valid")
        self.assertNotIn("evil", credentials["cookie"])
        self.assertNotIn("tracking", credentials["cookie"])

    def test_browser_skips_non_neobund_targets(self):
        with patch("app.neobund_auth.requests.Session") as constructor, patch("websocket.create_connection") as connect:
            session = constructor.return_value.__enter__.return_value
            session.get.return_value.json.return_value = [{"type": "page", "url": "https://neobund.ai.attacker.invalid"}]
            with self.assertRaises(NeoBundAuthError):
                read_browser_credentials()
            connect.assert_not_called()

    def test_commit_auth_error_propagates_without_reconcile(self):
        client = Mock()
        client.commit_organic_video.side_effect = NeoBundAuthError("paused")
        adapter = NeoBundPublishAdapter(client=client)
        with patch.object(adapter, "_find_committed_task_id_with_retry") as reconcile:
            with self.assertRaises(NeoBundAuthError):
                adapter._commit_once({}, content_type="organic")
            reconcile.assert_not_called()

    def test_batch_status_query_isolates_failure(self):
        adapter = NeoBundPublishAdapter(client=Mock())
        adapter.query_task_status = Mock(side_effect=[RuntimeError("temporary"), PublishTaskStatus(state="published", result="ok")])
        rows = [{"publish_task_id": "neobund:1", "scheduled_for": "2026-09-04 10:00:00"},
                {"publish_task_id": "neobund:2", "scheduled_for": "2026-09-04 11:00:00"}]
        statuses = adapter.query_task_statuses(rows)
        self.assertEqual(statuses["neobund:1"].state, "unknown")
        self.assertEqual(statuses["neobund:2"].state, "published")


if __name__ == "__main__":
    unittest.main()
