#!/usr/bin/env python3

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from services.feishu_workflow import (  # noqa: E402
    FIELD_EXECUTE,
    FIELD_CONFIRM_PUBLISH,
    FIELD_PHOTO_REQUEST,
    FIELD_QUANTITY,
    FIELD_PROGRESS,
    FIELD_REVIEW,
    PROGRESS_DONE,
    PROGRESS_REVIEW,
    FeishuTaskWorkflow, FeishuWorkflowError,
    ProductionPresetCatalog,
    dependent_redo_slots,
    quantity_value,
    text_value,
)


class CatalogTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "presets.json"
        task = {
            "account_id": "A", "market": "TH", "language": "th-TH",
            "recipe_id": "R", "theme_id": "T", "hook_strategy": "H",
            "persona_ref": "P", "look_ref": "L", "scene_ref": "S",
        }
        self.path.write_text(json.dumps({
            "default_overlay_profile_id": "OVERLAY_LIGHT_V1",
            "presets": [
                {"name": "固定", "tasks": [task]},
                {"name": "随机", "selection": "deterministic_one", "tasks_from": ["固定"]},
            ],
        }), encoding="utf-8")

    def tearDown(self):
        self.temp.cleanup()

    def test_fixed_and_deterministic_presets(self):
        catalog = ProductionPresetCatalog(self.path)
        self.assertEqual(catalog.names, ["固定", "随机"])
        self.assertEqual(catalog.resolve("固定", "rec1"), catalog.resolve("随机", "rec1"))
        self.assertEqual(catalog.overlay_profile_id, "OVERLAY_LIGHT_V1")
        self.assertEqual(len(catalog.resolve_batch("固定", "rec1", 3)), 3)
        self.assertEqual(len(catalog.resolve_batch("随机", "rec1", 3)), 3)
        fixed_batch = catalog.resolve_batch("固定", "rec1", 3)
        self.assertEqual(fixed_batch[0].hook_strategy, "H")
        self.assertEqual(fixed_batch[1].hook_strategy, "")

    def test_unknown_preset_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "未知生产预设"):
            ProductionPresetCatalog(self.path).resolve("不存在", "rec1")

    def test_random_entry_always_uses_current_multi_look_route(self):
        catalog = ProductionPresetCatalog()
        for record in ("rec1", "rec2", "recvufx3c1c27d"):
            specs = catalog.resolve_batch("TH｜随机养号组合｜轻文字", record, 9)
            self.assertEqual({spec.recipe_id for spec in specs}, {"RECIPE_MULTI_LOOK_V1"})
            self.assertTrue(all(not spec.look_ref for spec in specs))
        preview = catalog.preview("TH｜随机养号组合｜轻文字", "rec1", 2)
        self.assertEqual(preview["routing_policy"], "multi_look_6s_v2")
        self.assertEqual(preview["count_status"], "requested_not_yet_resolved")
        self.assertEqual(preview["videos"][0]["target_duration_ms"], 6000)
        self.assertEqual(preview["videos"][0]["requested_look_count"], 5)

    def test_production_note_reports_actual_frozen_counts_and_timing(self):
        from types import SimpleNamespace
        plan = {"outfit_sequence": [{}, {}, {}],
                "shots": [{"duration_ms": ms} for ms in (1600, 2200, 2200)]}
        note = FeishuTaskWorkflow._production_plan_note([SimpleNamespace(plan_json=plan)])
        self.assertIn("3套穿搭/3页/6秒", note)
        self.assertNotIn("5套", note)

    def test_shipped_catalog_has_automatic_look_preset(self):
        catalog = ProductionPresetCatalog(
            PACKAGE_ROOT / "config" / "feishu_production_presets.json"
        )
        batch = catalog.resolve_batch("TH｜自动匹配穿搭组合｜轻文字", "rec1", 3)
        self.assertEqual(len(batch), 3)
        self.assertTrue(all(spec.look_ref == "" for spec in batch))

    def test_shipped_catalog_exposes_outfit_breakdown_preset(self):
        catalog = ProductionPresetCatalog(
            PACKAGE_ROOT / "config" / "feishu_production_presets.json"
        )
        batch = catalog.resolve_batch("TH｜穿搭拆解首图｜均衡变体", "rec1", 4)
        self.assertEqual(len(batch), 4)
        self.assertTrue(all(
            spec.recipe_id == "RECIPE_OUTFIT_BREAKDOWN_V1" for spec in batch
        ))
        self.assertTrue(all(spec.look_ref == "" for spec in batch))

    def test_shipped_catalog_exposes_eight_native_photo_presets(self):
        path = PACKAGE_ROOT / "config" / "feishu_production_presets.json"
        catalog = ProductionPresetCatalog(path)
        payload = json.loads(path.read_text(encoding="utf-8"))
        photo_presets = [
            item for item in payload["presets"]
            if item.get("media_kind") == "native_photo"
        ]
        self.assertEqual(len(photo_presets), 8)
        self.assertEqual(
            {item["category_key"] for item in photo_presets},
            {"womenswear", "wig"},
        )
        expected_routes = {
            "图文｜TH｜四选一穿搭": (
                "native_photo_product_supply_v1", "REUSE_THEN_GENERATE_MISSING",
            ),
            "图文｜TH｜旅行穿搭": (
                "native_photo_style_plan_v1", "REUSE_THEN_GENERATE_MISSING",
            ),
        }
        recipe_ids = set()
        for item in photo_presets:
            expected_route, expected_assets = expected_routes.get(
                item["name"], ("native_photo_v1", "ASSET_REUSE"),
            )
            self.assertEqual(item["routing_policy"], expected_route)
            self.assertEqual(item["default_product_mode"], "NO_PRODUCT")
            self.assertEqual(item["default_asset_mode"], expected_assets)
            if item.get("status") == "disabled":
                self.assertNotIn(item["name"], catalog.names)
                with self.assertRaisesRegex(FeishuWorkflowError, "NEEDS_CONTENT"):
                    catalog.resolve(item["name"], "photo-config-test")
                continue
            resolved = catalog.resolve(item["name"], "photo-config-test")
            self.assertEqual(len(resolved), 1)
            self.assertTrue(catalog.is_native_photo(item["name"]))
            self.assertEqual(catalog.metadata(item["name"])["routing_policy"], expected_route)
            recipe_ids.add(resolved[0].recipe_id)
        self.assertEqual(len(recipe_ids), 7)
        self.assertIn("PHOTO_TH_TRAVEL_OUTFIT_V2", recipe_ids)

    def test_photo_plan_note_never_reports_video_duration(self):
        from types import SimpleNamespace
        note = FeishuTaskWorkflow._production_plan_note([
            SimpleNamespace(media_kind="native_photo", plan_json={"slides": [{}, {}, {}, {}, {}]})
        ])
        self.assertIn("5张原生图文", note)
        self.assertNotIn("秒", note)


class FieldParsingTest(unittest.TestCase):
    def test_photo_request_field_name_is_stable(self):
        self.assertEqual(FIELD_PHOTO_REQUEST, "图文任务JSON")

    def test_text_value_accepts_feishu_rich_text(self):
        self.assertEqual(text_value([{"text": "173"}, {"text": "42"}]), "17342")
        self.assertEqual(text_value(None), "")

    def test_quantity_defaults_and_validates(self):
        self.assertEqual(quantity_value(None), 1)
        self.assertEqual(quantity_value(3.0), 3)
        with self.assertRaisesRegex(RuntimeError, "1 到 9"):
            quantity_value(10)

    def test_action_prioritizes_review_and_ignores_completed(self):
        self.assertEqual(
            FeishuTaskWorkflow._action({FIELD_EXECUTE: True}), "generate"
        )
        self.assertEqual(
            FeishuTaskWorkflow._action({FIELD_EXECUTE: True, FIELD_REVIEW: "重做P3"}),
            "redo_3",
        )
        self.assertEqual(
            FeishuTaskWorkflow._action({FIELD_REVIEW: "通过"}), "approve"
        )
        self.assertEqual(
            FeishuTaskWorkflow._action({
                FIELD_REVIEW: "待审核", FIELD_PROGRESS: PROGRESS_REVIEW,
            }),
            "",
        )
        self.assertEqual(
            FeishuTaskWorkflow._action({FIELD_REVIEW: "通过", FIELD_PROGRESS: PROGRESS_DONE}),
            "",
        )
        self.assertEqual(
            FeishuTaskWorkflow._action({FIELD_REVIEW: "排期发布", FIELD_PROGRESS: PROGRESS_DONE}),
            "schedule",
        )
        self.assertEqual(
            FeishuTaskWorkflow._action({
                FIELD_CONFIRM_PUBLISH: True, FIELD_PROGRESS: PROGRESS_DONE,
            }),
            "confirm_publish",
        )
        self.assertEqual(
            FeishuTaskWorkflow._action({
                FIELD_EXECUTE: True, FIELD_PROGRESS: PROGRESS_DONE,
            }),
            "",
        )

    def test_redoing_outfit_anchor_invalidates_the_complete_group(self):
        plan = {
            "anchor_slot": 2,
            "recipe_execution": {"content_goal": "outfit_breakdown"},
        }
        self.assertEqual(dependent_redo_slots(plan, [2]), [1, 2, 3, 4, 5])
        self.assertEqual(dependent_redo_slots(plan, [1]), [1])
        self.assertEqual(dependent_redo_slots({"anchor_slot": 2}, [2]), [2])


class PublicationProjectionTest(unittest.TestCase):
    def test_stopped_retry_makes_partial_batch_visible(self):
        progress, note = FeishuTaskWorkflow._publication_projection([
            {"status": "已排期", "account_name": "账号A", "planned_publish_at": "2026-09-05 13:25:00"},
            {"status": "待排班", "error_message": "已停止自动重试（3/3）：上传失败"},
        ])
        self.assertEqual(progress, "发布失败")
        self.assertIn("需处理1", note)
        self.assertIn("已停止自动重试", note)

    def test_retrying_is_counted_without_being_reported_as_failure(self):
        progress, note = FeishuTaskWorkflow._publication_projection([
            {"status": "待排班", "error_message": "等待自动重试（1/3，下次 2026-09-05 10:00:00）：网络失败"},
        ])
        self.assertEqual(progress, "待排班")
        self.assertIn("重试中1", note)

    def test_only_complete_batch_is_published(self):
        progress, note = FeishuTaskWorkflow._publication_projection([
            {"status": "已发布", "account_name": "账号A"},
            {"status": "已发布", "account_name": "账号B"},
        ])
        self.assertEqual(progress, "已发布")
        self.assertIn("已发布2", note)


class ScanTest(unittest.TestCase):
    def test_dry_run_existing_frozen_batch_keeps_legacy_recipe(self):
        from types import SimpleNamespace
        class Repository:
            def get_production_batch(self, record_id):
                return SimpleNamespace(manifest_json={"entries": [{"spec": {"recipe_id": "RECIPE_SCENE_SOLUTION_V1"}}]})
        class Client:
            def list_records(self, page_size=500):
                return [SimpleNamespace(record_id="legacy", fields={FIELD_EXECUTE: True})]
        runner = FeishuTaskWorkflow(repository=Repository(), client=Client(),
            generator=object(), renderer=object())
        report = runner.scan(dry_run=True)
        preview = report["processed"][0]["production_preview"]
        self.assertEqual(preview["source"], "frozen_batch")
        self.assertEqual(preview["recipe_ids"], ["RECIPE_SCENE_SOLUTION_V1"])

    class Record:
        record_id = "rec1"
        fields = {FIELD_EXECUTE: False}

    class Client:
        def list_records(self, page_size=500):
            assert page_size == 500
            return [ScanTest.Record()]

    def test_blank_scan_is_noop(self):
        runner = FeishuTaskWorkflow(
            repository=object(), client=self.Client(),
            catalog=object(), generator=object(), renderer=object(),
        )
        report = runner.scan(dry_run=True)
        self.assertEqual(report["scanned"], 1)
        self.assertEqual(report["eligible"], 0)

    def test_explicit_resume_recovers_only_selected_running_record(self):
        class RunningRecord:
            record_id = "rec_running"
            fields = {FIELD_EXECUTE: False, FIELD_PROGRESS: "生成中"}

        class Client:
            def get_record(self, record_id):
                self.record_id = record_id
                return RunningRecord()

        client = Client()
        runner = FeishuTaskWorkflow(
            repository=object(), client=client,
            catalog=object(), generator=object(), renderer=object(),
        )
        report = runner.scan(
            dry_run=True, record_id="rec_running", resume_running=True
        )
        self.assertEqual(report["eligible"], 1)
        self.assertEqual(report["processed"][0]["action"], "generate")
        self.assertEqual(client.record_id, "rec_running")


if __name__ == "__main__":
    unittest.main()
