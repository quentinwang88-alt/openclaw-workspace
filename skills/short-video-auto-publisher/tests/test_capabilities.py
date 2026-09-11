#!/usr/bin/env python3
"""Account capability reconciliation tests."""

from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from app.capabilities import reconcile_neobund_account_capabilities  # noqa: E402
from app.db import AutoPublishDB  # noqa: E402
from app.models import AccountConfig  # noqa: E402
from app.neobund_publish import NeoBundPublishAdapter  # noqa: E402


class CapabilityReconciliationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = AutoPublishDB(Path(self.temp_dir.name) / "publish.sqlite3")
        self.db.upsert_account_configs(
            [
                AccountConfig(
                    account_id="tocrystal66", account_name="泰国女装1", store_id="THFZ01",
                    account_status="可用", publish_channel="NeoBund",
                    publish_time_1="13:25", publish_time_2="21:30", publish_time_3="",
                    nurture_enabled=True, nurture_daily_count=1,
                ),
                AccountConfig(
                    account_id="wn0didnad6", account_name="泰国女装2", store_id="THFZ01",
                    account_status="可用", publish_channel="NeoBund",
                    publish_time_1="14:30", publish_time_2="18:00", publish_time_3="",
                    nurture_enabled=True, nurture_daily_count=1,
                ),
            ]
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_reconcile_separates_organic_and_shoppable_capabilities(self) -> None:
        client = Mock()
        client.list_tiktok_accounts.return_value = {
            "records": [{"authId": 4834, "username": "tocrystal66", "quotaStatus": 1}]
        }
        client.list_creator_accounts.return_value = {
            "records": [
                {"authId": 45953, "username": "tocrystal66", "quotaStatus": 1},
                {"authId": 45951, "username": "wn0didnad6", "quotaStatus": 1},
            ]
        }
        adapter = NeoBundPublishAdapter(client=client)

        stats = reconcile_neobund_account_capabilities(self.db, adapter)

        self.assertEqual(stats["checked"], 2)
        self.assertEqual(stats["organic_capable"], 1)
        self.assertEqual(stats["shoppable_capable"], 2)
        self.assertEqual(stats["nurture_mismatch"], 1)
        account1 = self.db.get_account_config("tocrystal66")
        account2 = self.db.get_account_config("wn0didnad6")
        self.assertEqual(account1["capability_status"], "ok")
        self.assertEqual(account1["organic_capable"], 1)
        self.assertEqual(account1["shoppable_capable"], 1)
        self.assertEqual(account2["organic_capable"], 0)
        self.assertEqual(account2["shoppable_capable"], 1)

    def test_reconcile_includes_dual_channel_binding_when_default_is_creatok(self) -> None:
        with self.db._connect() as conn:
            conn.execute(
                "UPDATE account_configs SET publish_channel='CreatOK' WHERE account_id='tocrystal66'"
            )
        self.db.replace_account_channel_bindings("tocrystal66", [
            {"publish_channel": "NeoBund", "content_scope": "shoppable"},
            {"publish_channel": "CreatOK", "content_scope": "organic"},
        ])
        client = Mock()
        client.list_tiktok_accounts.return_value = {"records": []}
        client.list_creator_accounts.return_value = {
            "records": [{"authId": 45953, "username": "tocrystal66", "quotaStatus": 1}]
        }
        stats = reconcile_neobund_account_capabilities(
            self.db, NeoBundPublishAdapter(client=client)
        )
        self.assertEqual(stats["checked"], 2)
        self.assertEqual(self.db.get_account_config("tocrystal66")["shoppable_capable"], 1)


if __name__ == "__main__":
    unittest.main()
