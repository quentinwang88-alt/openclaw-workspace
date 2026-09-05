"""Entrypoint regressions: local commit survives failed status projections."""
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import run_pipeline as pipeline


class PipelineResilienceTest(unittest.TestCase):
    def test_neobund_local_proxy_applies_to_api_and_upload_only(self):
        args = pipeline.build_parser().parse_args(['schedule', '--publish-mode', 'neobund'])
        with patch.object(pipeline, 'load_local_config', return_value={"neobund_proxy_url": "http://127.0.0.1:19090"}):
            adapter = pipeline.build_neobund_publish_adapter(args)
        for session in (adapter.client.session, adapter.uploader.session):
            self.assertEqual(session.proxies, {"http": "http://127.0.0.1:19090", "https": "http://127.0.0.1:19090"})
            self.assertFalse(session.trust_env)

    def test_neobund_without_local_proxy_preserves_environment_behavior(self):
        args = pipeline.build_parser().parse_args(['schedule', '--publish-mode', 'neobund'])
        with patch.object(pipeline, 'load_local_config', return_value={}):
            adapter = pipeline.build_neobund_publish_adapter(args)
        for session in (adapter.client.session, adapter.uploader.session):
            self.assertEqual(session.proxies, {})
            self.assertTrue(session.trust_env)

    def test_browser_auth_not_enabled_without_explicit_local_opt_in(self):
        args = pipeline.build_parser().parse_args(['schedule', '--publish-mode', 'neobund'])
        with patch.object(pipeline, 'load_local_config', return_value={}), patch.object(pipeline, 'NeoBundPublishAdapter') as factory:
            pipeline.build_neobund_publish_adapter(args)
        self.assertIsNone(factory.call_args.kwargs['auth_refresh_provider'])

    def test_projection_failure_is_deferred(self):
        with patch('app.publish_writeback.sync_run_manager_statuses', side_effect=RuntimeError('Feishu unavailable')):
            result = pipeline.sync_publish_observability(object(), SimpleNamespace(), client=object())
        self.assertEqual(result['deferred'], 1)
        self.assertIn('Feishu unavailable', result['error'])

    def test_projection_reuses_source_records_and_recovers_next_run(self):
        records = [object()]
        with patch('app.publish_writeback.sync_run_manager_statuses', side_effect=[RuntimeError('timeout'), {'records_updated': 1}]) as sync:
            db, client = object(), object()
            first = pipeline.sync_publish_observability(db, SimpleNamespace(), client=client, records=records)
            second = pipeline.sync_publish_observability(db, SimpleNamespace(), client=client, records=records)
        self.assertEqual(first['deferred'], 1)
        self.assertEqual(second['records_updated'], 1)
        sync.assert_called_with(client, db, records=records)

    def test_history_query_failure_does_not_raise_out_of_stage(self):
        def fail():
            raise RuntimeError('old task query failed')
        self.assertEqual(pipeline.recoverable_stage(fail)['deferred'], 1)


if __name__ == '__main__':
    unittest.main()
