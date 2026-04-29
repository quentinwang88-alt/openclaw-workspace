import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import json


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import src.market_insight_feishu_sync as market_insight_feishu_sync  # noqa: E402


class MarketInsightFeishuSyncerTest(unittest.TestCase):
    def test_maybe_sync_respects_threshold_and_completion(self):
        calls = []
        original = market_insight_feishu_sync.sync_from_output_config
        market_insight_feishu_sync.sync_from_output_config = lambda output_config_path, artifacts_root, purge_target_scope=None: calls.append(  # type: ignore[assignment]
            (str(output_config_path), str(artifacts_root), purge_target_scope)
        ) or {"created": 1}
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            output_config_path = temp_path / "output.json"
            output_config_path.write_text(
                json.dumps({"latest_key": "VN__hair_accessory", "target": {"feishu_url": "https://example.com/wiki/token?table=tbl&view=vew"}}),
                encoding="utf-8",
            )
            try:
                syncer = market_insight_feishu_sync.MarketInsightFeishuSyncer(
                    output_config_path=output_config_path,
                    artifacts_root=temp_path / "artifacts",
                    sync_every_completions=2,
                )
                self.assertIsNone(
                    syncer.maybe_sync(
                        SimpleNamespace(product_snapshot_count=1, total_product_count=5, run_status="running")
                    )
                )
                self.assertEqual(
                    syncer.maybe_sync(
                        SimpleNamespace(product_snapshot_count=2, total_product_count=5, run_status="running")
                    )["created"],
                    1,
                )
                self.assertIsNone(
                    syncer.maybe_sync(
                        SimpleNamespace(product_snapshot_count=2, total_product_count=5, run_status="running")
                    )
                )
                self.assertEqual(
                    syncer.maybe_sync(
                        SimpleNamespace(product_snapshot_count=3, total_product_count=5, run_status="completed")
                    )["run_status"],
                    "completed",
                )
                self.assertEqual(len(calls), 2)
            finally:
                market_insight_feishu_sync.sync_from_output_config = original  # type: ignore[assignment]


if __name__ == "__main__":
    unittest.main()
