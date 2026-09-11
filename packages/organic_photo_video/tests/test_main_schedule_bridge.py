#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import json
import sys
import tempfile
import unittest


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from services.main_schedule_bridge import MainScheduleBridge, MainScheduleBridgeError  # noqa: E402
from app.db import AutoPublishDB  # noqa: E402
from app.models import ScriptMetadata  # noqa: E402


class MainScheduleBridgeTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.video = root / "opv.mp4"
        self.video.write_bytes(b"video")
        self.db = AutoPublishDB(root / "main.sqlite3")
        self.task = SimpleNamespace(
            task_id="task-1", selected_render_id="render-1", target_country="TH",
            product_id="173", theme_id="THEME", feishu_record_id="rec-1",
            source_record_id="rec-1", content_package_id="pkg-1",
            recipe_id="RECIPE_PAIN_POINT_SOLUTION_V1",
            product_snapshot_json={"product": {"category": "outerwear"}},
            copy_json={"title": "Thai outfit"},
            plan_json={"bgm_mood_hints": ["bright"]},
        )
        self.render = SimpleNamespace(
            output_url=str(self.video), output_sha256="sha", duration_ms=9000,
        )
        self.repo = SimpleNamespace(
            get_task=lambda task_id: self.task if task_id == "task-1" else None,
            get_render=lambda render_id: self.render if render_id == "render-1" else None,
            latest_render=lambda task_id: self.render,
        )

    def tearDown(self):
        self.temp.cleanup()

    def test_child_rework_cannot_enqueue_previous_released_render(self):
        self.task.workflow_version = 2
        self.task.active_revision_id = "child"
        self.task.released_revision_id = "parent"
        self.render.origin_revision_id = "parent"
        self.render.render_id = "render-1"
        self.render.publish_ready = True
        with self.assertRaises(MainScheduleBridgeError):
            MainScheduleBridge(self.repo, db=self.db).enqueue_task("task-1")
        self.assertIsNone(self.db.get_script_metadata("opv:task-1"))

    def test_enqueues_idempotent_non_shoppable_candidate(self):
        bridge = MainScheduleBridge(self.repo, db=self.db)
        bridge.enqueue_task("task-1")
        bridge.enqueue_task("task-1")
        metadata = self.db.get_script_metadata("opv:task-1")
        asset = self.db.get_video_asset("opv:task-1")
        self.assertEqual(metadata["store_id"], "THFZ01")
        self.assertEqual(metadata["product_id"], "")
        self.assertEqual(metadata["cart_enabled"], "否")
        self.assertEqual(metadata["script_source"], "图文养号")
        self.assertEqual(metadata["source_record_id"], "rec-1")
        self.assertEqual(metadata["script_slot"], "OPV:task-1")
        self.assertEqual(asset["publish_status"], "待排期")
        context = json.loads(metadata["script_text"])
        self.assertEqual(context["source_product_id"], "173")
        self.assertEqual(context["recipe_id"], "RECIPE_PAIN_POINT_SOLUTION_V1")
        self.assertEqual(context["theme_id"], "THEME")
        self.assertEqual(context["content_package_id"], "pkg-1")

    def test_enqueue_preserves_outfit_bgm_semantics_for_scheduler(self):
        self.task.recipe_id = "RECIPE_OUTFIT_BREAKDOWN_V1"
        self.task.outfit_plan_json = {"layers": ["jacket", "skirt"]}
        MainScheduleBridge(self.repo, db=self.db).enqueue_task("task-1")
        context = json.loads(self.db.get_script_metadata("opv:task-1")["script_text"])
        self.assertIn("outfit breakdown", context["bgm_content_template"])
        self.assertEqual(context["bgm_rhythm_preference"], "")

    def test_multiple_videos_from_one_feishu_row_have_unique_stable_slots(self):
        second = SimpleNamespace(**vars(self.task))
        second.task_id = "task-2"
        second.selected_render_id = "render-2"
        tasks = {"task-1": self.task, "task-2": second}
        self.repo.get_task = lambda task_id: tasks.get(task_id)
        self.repo.get_render = lambda render_id: self.render
        bridge = MainScheduleBridge(self.repo, db=self.db)

        for task_id in ("task-1", "task-2", "task-1", "task-2"):
            bridge.enqueue_task(task_id, feishu_record_id="rec-shared")

        rows = [
            row for row in self.db.list_script_metadata()
            if row.source_record_id == "rec-shared"
        ]
        self.assertEqual(len(rows), 2)
        self.assertEqual(
            {row.script_slot for row in rows},
            {"OPV:task-1", "OPV:task-2"},
        )
        self.assertIsNotNone(self.db.get_video_asset("opv:task-1"))
        self.assertIsNotNone(self.db.get_video_asset("opv:task-2"))

    def test_retry_repairs_legacy_partial_metadata_before_second_video(self):
        self.db.upsert_script_metadata([ScriptMetadata(
            canonical_script_key="opv:task-1",
            script_id="task-1",
            source_record_id="rec-shared",
            script_slot="OPV",
            task_no="task-1",
            store_id="THFZ01",
            product_id="",
            parent_slot="OPV",
            direction_label="图文养号",
            variant_strength="成片",
            target_country="TH",
            product_type="outerwear",
            content_family_key="sha",
            script_text="{}",
            short_video_title="Thai outfit",
            title_source="opv_copy",
            script_source="图文养号",
            publish_purpose="养号",
            cart_enabled="否",
            content_branch="非商品展示型",
        )])
        second = SimpleNamespace(**vars(self.task))
        second.task_id = "task-2"
        second.selected_render_id = "render-2"
        tasks = {"task-1": self.task, "task-2": second}
        self.repo.get_task = lambda task_id: tasks.get(task_id)
        self.repo.get_render = lambda render_id: self.render
        bridge = MainScheduleBridge(self.repo, db=self.db)

        bridge.enqueue_task("task-1", feishu_record_id="rec-shared")
        bridge.enqueue_task("task-2", feishu_record_id="rec-shared")

        first = self.db.get_script_metadata("opv:task-1")
        second_row = self.db.get_script_metadata("opv:task-2")
        self.assertEqual(first["script_slot"], "OPV:task-1")
        self.assertEqual(second_row["script_slot"], "OPV:task-2")

    def test_unconfigured_country_fails_closed(self):
        self.task.target_country = "VN"
        with self.assertRaisesRegex(MainScheduleBridgeError, "未配置 VN"):
            MainScheduleBridge(self.repo, db=self.db).enqueue_task("task-1")

    def test_mexico_wig_routes_to_existing_mx_store(self):
        self.task.target_country = "MX"
        self.task.category_key = "wig"
        result = MainScheduleBridge(self.repo, db=self.db).enqueue_task("task-1")
        self.assertEqual(result["store_id"], "MXJF01")
        self.assertEqual(self.db.get_script_metadata("opv:task-1")["product_type"], "wig")

    def test_explicit_photo_store_is_frozen_in_publish_context(self):
        result = MainScheduleBridge(self.repo, db=self.db).enqueue_task(
            "task-1", store_id="THFZ01"
        )
        context = json.loads(self.db.get_script_metadata("opv:task-1")["script_text"])
        self.assertEqual(result["store_id"], "THFZ01")
        self.assertEqual(context["publish_store_id"], "THFZ01")

    def test_unknown_explicit_store_fails_closed(self):
        with self.assertRaisesRegex(MainScheduleBridgeError, "未知的图文发布店铺"):
            MainScheduleBridge(self.repo, db=self.db).enqueue_task(
                "task-1", store_id="UNKNOWN"
            )

    def test_publish_context_prefers_frozen_product_id_used_by_render(self):
        self.task.product_id = "173661-wrong"
        self.task.product_snapshot_json["product"].update({
            "product_id": "173714-correct",
            "product_name": "浅蓝色短款蓬松外套",
        })
        MainScheduleBridge(self.repo, db=self.db).enqueue_task("task-1")
        metadata = self.db.get_script_metadata("opv:task-1")
        context = json.loads(metadata["script_text"])
        self.assertEqual(context["source_product_id"], "173714-correct")

    def test_th_historical_title_replaces_exact_frozen_name_only(self):
        self.task.product_snapshot_json["product"]["product_name"] = (
            "浅蓝色短款蓬松外套"
        )
        self.task.copy_json["title"] = "浅蓝色短款蓬松外套 3 ลุคไปเที่ยว"
        MainScheduleBridge(self.repo, db=self.db).enqueue_task("task-1")
        metadata = self.db.get_script_metadata("opv:task-1")
        self.assertEqual(
            metadata["short_video_title"],
            "เสื้อตัวนอกตัวนี้ 3 ลุคไปเที่ยว",
        )
        self.assertNotIn("浅蓝色短款蓬松外套", metadata["short_video_title"])

    def test_th_title_without_frozen_name_is_unchanged(self):
        original = "เสื้อตัวนอกตัวเดียว 3 ลุคไปเที่ยว"
        self.task.product_snapshot_json["product"]["product_name"] = (
            "浅蓝色短款蓬松外套"
        )
        self.task.copy_json["title"] = original
        MainScheduleBridge(self.repo, db=self.db).enqueue_task("task-1")
        metadata = self.db.get_script_metadata("opv:task-1")
        self.assertEqual(metadata["short_video_title"], original)


if __name__ == "__main__":
    unittest.main()
