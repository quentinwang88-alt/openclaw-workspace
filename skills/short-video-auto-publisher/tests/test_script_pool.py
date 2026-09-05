"""Offline contract tests for unified script-pool publication."""

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
import sys
import tempfile
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.db import AutoPublishDB, is_nurture_candidate
from app.models import AccountConfig
from app.scheduler import (account_can_publish_candidate, is_non_shoppable_candidate,
                           resolve_field_mapping, RUN_MANAGER_FIELD_ALIASES,
                           schedule_slots, sync_run_manager_pool_metadata, sync_videos)
from app.script_pool import register_script_pool_metadata, publishing_product_id
from app.neobund_publish import NeoBundPublishAdapter
from app.publishers import DryRunPublishAdapter, GeeLarkPublishAdapter


class ScriptPoolTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = AutoPublishDB(Path(self.temp.name) / "publish.sqlite")
        self.base = dict(script_id="wsr_001", source_record_id="rec001", prompt="exact original prompt",
                         store_id="MX01", product_id="S260724014590", target_country="墨西哥",
                         target_language="es-MX", short_video_title="Un cambio de look")

    def tearDown(self):
        self.temp.cleanup()

    def candidate(self, purpose, cart="", **extra):
        return SimpleNamespace(script_source="成功脚本复刻", publish_purpose=purpose,
                               content_branch="SUCCESS_SCRIPT_REPLICATION", cart_enabled=cart,
                               product_id="S260724014590", script_pool_registered=True,
                               platform_product_id=extra.get("platform_product_id", "1730000000000000000"))

    def test_four_combinations_and_defaults(self):
        for purpose, cart, expected in [("带货", "是", False), ("带货", "否", True),
                                        ("养号", "是", False), ("养号", "否", True),
                                        ("养号", "", True), ("带货", "", False)]:
            with self.subTest(purpose=purpose, cart=cart):
                item = self.candidate(purpose, cart)
                self.assertEqual(is_non_shoppable_candidate(item), expected)
                self.assertEqual(is_nurture_candidate(item), purpose == "养号")
                self.assertEqual(publishing_product_id(item), "" if expected else "1730000000000000000")

    def test_seeding_is_always_organic_even_conflicting_cart(self):
        item = self.candidate("养号", "是")
        item.content_branch = "SEEDING_ORGANIC"
        self.assertTrue(is_non_shoppable_candidate(item))
        self.assertEqual(publishing_product_id(item), "")

    def test_legacy_fallback_is_preserved_but_internal_code_rejected(self):
        item = self.candidate("", "")
        item.script_pool_registered = False
        item.script_source = "原创脚本"
        item.product_id = "1730000000000000000"
        self.assertEqual(publishing_product_id(item), item.product_id)
        item.product_id = "S260724014590"
        with self.assertRaisesRegex(ValueError, "内部产品编码"):
            publishing_product_id(item)

    def test_platform_capability_not_content_pool_controls_channel(self):
        account = dict(publish_channel="NeoBund", capability_status="ok", organic_capable=0, shoppable_capable=1)
        self.assertTrue(account_can_publish_candidate(account, self.candidate("养号", "是")))
        self.assertFalse(account_can_publish_candidate(account, self.candidate("养号", "否")))

    def test_idempotent_registration_preserves_prompt_and_product(self):
        first = register_script_pool_metadata(self.db, **self.base, publish_purpose="养号")
        second = register_script_pool_metadata(self.db, **self.base, publish_purpose="养号")
        self.assertEqual((first["status"], second["status"]), ("created", "unchanged"))
        row = self.db.get_script_metadata("wsr_001")
        self.assertEqual(row["product_id"], "S260724014590")
        self.assertEqual(row["cart_enabled"], "否")
        self.assertEqual(row["script_text"], self.base["prompt"])

    def test_cart_removed_clears_only_platform_binding(self):
        register_script_pool_metadata(self.db, **self.base, cart_enabled="是", platform_product_id="1730000000000000000")
        register_script_pool_metadata(self.db, **self.base, cart_enabled="否", platform_product_id="1730000000000000000")
        with self.db._connect() as conn:
            row = conn.execute("SELECT * FROM script_pool_bindings").fetchone()
        self.assertEqual(row["platform_product_id"], "")
        self.assertEqual(self.db.get_script_metadata("wsr_001")["product_id"], "S260724014590")

    def test_missing_mapping_keeps_same_product_binding_but_not_changed_product(self):
        register_script_pool_metadata(self.db, **self.base, cart_enabled="是", platform_product_id="1730000000000000000")
        register_script_pool_metadata(self.db, **self.base, cart_enabled="是", platform_product_id=None)
        with self.db._connect() as conn:
            row = conn.execute("SELECT * FROM script_pool_bindings").fetchone()
        self.assertEqual(row["platform_product_id"], "1730000000000000000")
        register_script_pool_metadata(self.db, **dict(self.base, product_id="S260724029604"), cart_enabled="是", platform_product_id=None)
        with self.db._connect() as conn:
            row = conn.execute("SELECT * FROM script_pool_bindings").fetchone()
        self.assertEqual(row["platform_product_id"], "")

    def test_all_execution_statuses_frozen(self):
        register_script_pool_metadata(self.db, **self.base, cart_enabled="是")
        for status in ("提交中", "已排期", "已发布"):
            with self.db._connect() as conn:
                conn.execute("DELETE FROM publish_slots")
                conn.execute("INSERT INTO publish_slots (store_id,account_id,account_name,scheduled_for,canonical_script_key,schedule_status,created_at,updated_at) VALUES ('MX01','account','a','2026-09-03 12:00:00','wsr_001',?,'now','now')", (status,))
            result = register_script_pool_metadata(self.db, **self.base, cart_enabled="否")
            self.assertEqual(result["status"], "frozen")
            self.assertEqual(self.db.get_script_metadata("wsr_001")["cart_enabled"], "是")

    def test_unknown_source_not_silently_original(self):
        with self.assertRaisesRegex(ValueError, "未注册脚本来源"):
            register_script_pool_metadata(self.db, **self.base, script_source="unknown new flow")

    def test_chinese_management_title_is_not_a_mexico_publish_title(self):
        result = register_script_pool_metadata(self.db, **dict(self.base, short_video_title="墨西哥｜养号｜染发膏母版｜S260724014590"), publish_purpose="养号")
        self.assertEqual(result["title_status"], "incompatible_ignored")
        self.assertEqual(self.db.get_script_metadata("wsr_001")["short_video_title"], "")

    def test_wsr_recovery_uses_registered_key_not_batch_item_alias(self):
        fields = {"脚本ID": "wsr_001", "内部脚本键": "wsr:001", "任务来源": "成功脚本复刻", "提示词": "exact prompt",
                  "全球产品ID": "S260724014590", "发布用途": "养号", "是否挂车": "否", "店铺ID": "MX01",
                  "是否发布": True, "跑视频状态": "成功", "视频附件": [{"file_token": "video"}],
                  "短视频标题": "墨西哥｜养号｜中文管理名", "目标国家": "墨西哥"}
        mapping = resolve_field_mapping(list(fields), RUN_MANAGER_FIELD_ALIASES)
        client = Mock()
        client.download_attachment_bytes.return_value = (b"fake-video", "video.mp4", "video/mp4", 10)
        result = sync_videos([SimpleNamespace(record_id="run001", fields=fields)], mapping, self.db,
                             download_dir=Path(self.temp.name), client=client)
        self.assertEqual(result["synced"], 1)
        self.assertIsNotNone(self.db.get_video_asset("wsr_001"))
        self.assertEqual(self.db.get_script_metadata("wsr_001")["short_video_title"], "")

    def test_run_manager_metadata_recovery_and_cancel(self):
        fields = {"脚本ID": "wsr_001", "任务来源": "成功脚本复刻", "提示词": "exact prompt",
                  "全球产品ID": "S260724014590", "产品ID": "1730000000000000000",
                  "发布用途": "养号", "是否挂车": "是", "店铺ID": "MX01"}
        mapping = resolve_field_mapping(list(fields), RUN_MANAGER_FIELD_ALIASES)
        self.assertEqual(sync_run_manager_pool_metadata(self.db, fields, mapping, record_id="run001")["status"], "created")
        fields["是否挂车"] = "否"
        sync_run_manager_pool_metadata(self.db, fields, mapping, record_id="run001")
        row = self.db.get_script_metadata("wsr_001")
        self.assertEqual((row["publish_purpose"], row["cart_enabled"]), ("养号", "否"))

    def test_nurture_without_product_registers(self):
        args = dict(self.base, product_id="")
        self.assertEqual(register_script_pool_metadata(self.db, **args, publish_purpose="养号")["status"], "created")

    def test_invalid_mapping_allows_import_but_blocks_scheduling(self):
        register_script_pool_metadata(self.db, **self.base, publish_purpose="带货", cart_enabled="是")
        self.db.upsert_account_configs([AccountConfig(account_id="account", account_name="a", store_id="MX01", account_status="可用", publish_time_1="12:00", publish_time_2="", publish_time_3="")])
        self.db.upsert_video_asset(script_id="wsr_001", run_manager_record_id="run001", video_source_type="link", video_source_value="https://example.invalid/v.mp4", local_file_path="/tmp/unused.mp4", download_status="下载成功", run_video_status="成功", publish_status="待排期")
        candidates = self.db.list_ready_candidates("MX01")
        self.assertEqual(len(candidates), 1)
        self.assertTrue(candidates[0].script_pool_registered)
        publisher = Mock(spec=DryRunPublishAdapter)
        schedule_slots(self.db, publisher, now=datetime(2026, 9, 3, 11), window_hours=2)
        publisher.create_scheduled_task.assert_not_called()
        with self.db._connect() as conn:
            slot = conn.execute("SELECT * FROM publish_slots").fetchone()
        self.assertIn("平台商品映射", slot["error_message"])
        self.assertEqual(slot["schedule_status"], "待排期")

    def test_neobund_rejects_internal_product_before_network(self):
        client = Mock()
        adapter = NeoBundPublishAdapter(client=client, uploader=Mock())
        with self.assertRaisesRegex(ValueError, "内部产品编码"):
            adapter.create_scheduled_task(account_id="account", video_path="/tmp/unused.mp4", title="title", publish_at=datetime.now(), script_id="wsr_001", product_id="S260724014590")
        self.assertEqual(client.mock_calls, [])

    def test_four_combinations_reach_publish_payload_without_product_leak(self):
        for number, (purpose, cart) in enumerate((("带货", "是"), ("带货", "否"), ("养号", "是"), ("养号", "否"))):
            with self.subTest(purpose=purpose, cart=cart):
                db = AutoPublishDB(Path(self.temp.name) / f"combination-{number}.sqlite")
                register_script_pool_metadata(db, **self.base, publish_purpose=purpose, cart_enabled=cart,
                                              platform_product_id="1730000000000000000")
                db.upsert_account_configs([AccountConfig(account_id="account", account_name="a", store_id="MX01", account_status="可用", publish_time_1="12:00", publish_time_2="", publish_time_3="", nurture_enabled=True)])
                db.upsert_video_asset(script_id="wsr_001", run_manager_record_id="run001", video_source_type="link", video_source_value="https://example.invalid/v.mp4", local_file_path="/tmp/unused.mp4", download_status="下载成功", run_video_status="成功", publish_status="待排期")
                publisher = Mock(spec=DryRunPublishAdapter)
                publisher.create_scheduled_task.return_value = "task-id"
                schedule_slots(db, publisher, now=datetime(2026, 9, 3, 11), window_hours=2)
                publisher.create_scheduled_task.assert_called_once()
                self.assertEqual(publisher.create_scheduled_task.call_args.kwargs["product_id"], "1730000000000000000" if cart == "是" else "")

    def test_geelark_no_cart_clears_configured_product_and_internal_ids_rejected(self):
        adapter = GeeLarkPublishAdapter(token="fake", extra_body_json='{"item":{"productId":"stale-product","productTitle":"stale"}}')
        adapter._request_with_retry = Mock()
        adapter._request_with_retry.return_value.json.return_value = {"data": {"taskIds": ["task-id"]}}
        kwargs = dict(account_id="account", video_path="https://example.invalid/video.mp4", title="title", publish_at=datetime.now(), script_id="wsr_001")
        adapter.create_scheduled_task(**kwargs, product_id="")
        payload = adapter._request_with_retry.call_args.kwargs["json"]
        self.assertNotIn("productId", payload["list"][0])
        self.assertNotIn("productTitle", payload["list"][0])
        adapter._request_with_retry.reset_mock()
        with self.assertRaisesRegex(ValueError, "内部产品编码"):
            adapter.create_scheduled_task(**kwargs, product_id="S260724014590")
        adapter._request_with_retry.assert_not_called()


if __name__ == "__main__":
    unittest.main()
