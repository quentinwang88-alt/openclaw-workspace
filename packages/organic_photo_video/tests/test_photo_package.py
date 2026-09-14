from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import sys
import tempfile
import unittest

from PIL import Image, ImageFont

PACKAGE_ROOT = Path(__file__).resolve().parent.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from domain import statuses
from domain.models import QualityReview, TaskRevision
from services.photo_package import (
    NativePhotoProductionFlow, PhotoPackageExporter, PhotoPackageError, _draw_overlay,
    normalize_photo_template,
)
from services.photo_copy_review import frozen_copy_review_sha256
from services.release_gate import (
    ReleaseGateError, contract_hash, freeze_photo_release, validate_photo_upload,
)
from services.workflow_v2 import PhotoPackageReviewService, canonical_hash
from services.main_schedule_bridge import MainScheduleBridge
from services.feishu_workflow import FeishuTaskWorkflow
from app.db import AutoPublishDB


class PhotoRepo:
    def __init__(self, root: Path):
        self.task = SimpleNamespace(
            task_id="photo-task", active_revision_id="photo-rev", released_revision_id=None,
            workflow_version=2, row_version=1, media_kind="native_photo",
            content_package_id="photo-package", requested_shot_count=5,
            task_status=statuses.TASK_IMAGE_REVIEW, plan_json={"actual_shot_count": 5},
            product_snapshot_json={}, copy_json={},
        )
        candidates = {}
        selected = {}
        for index in range(1, 6):
            path = root / f"source-{index}.png"
            Image.new("RGB", (120, 180), (index * 25, 80, 140)).save(path)
            sha = hashlib.sha256(path.read_bytes()).hexdigest()
            asset_id = f"shot-{index}"
            candidates[asset_id] = {
                "asset_id": asset_id, "asset_type": "shot", "path": str(path),
                "sha256": sha, "parent_asset_ids": [], "parameters_hash": "source",
            }
            selected[f"shot:{index}"] = asset_id
        snapshot = {
            "plan": {"actual_shot_count": 5},
            "copy": {"title": "เลือกหนึ่งลุค", "caption": "วันนี้ชอบลุคไหน"},
            "product_snapshot": {}, "workflow_version": 2,
        }
        self.revision = TaskRevision(
            revision_id="photo-rev", task_id="photo-task", revision_no=1,
            plan_snapshot_json=snapshot, input_snapshot_hash=canonical_hash(snapshot),
            asset_manifest_json={
                "schema_version": "opv-selection-manifest-v1",
                "candidates": candidates, "selected": selected,
            },
            selection_hash=canonical_hash(selected), revision_status="working",
        )
        self.package = SimpleNamespace(
            content_package_id="photo-package", task_id="photo-task",
            status=statuses.PACKAGE_GENERATING, photo_manifest_json={},
        )
        self.reviews: list[QualityReview] = []

    def get_task(self, task_id):
        return self.task if task_id == self.task.task_id else None

    def list_tasks_by_source_prefix(self, _source_type, _prefix):
        return [self.task]

    def get_task_revision(self, revision_id):
        return copy.deepcopy(self.revision) if revision_id == self.revision.revision_id else None

    def update_revision_manifest(self, revision_id, *, expected_lock_version, asset_manifest_json, selection_hash):
        assert revision_id == self.revision.revision_id
        assert expected_lock_version == self.revision.lock_version
        self.revision.asset_manifest_json = copy.deepcopy(asset_manifest_json)
        self.revision.selection_hash = selection_hash
        self.revision.lock_version += 1
        return copy.deepcopy(self.revision)

    def get_content_package(self, package_id):
        return self.package if package_id == self.package.content_package_id else None

    def update_content_package(self, package_id, **fields):
        assert package_id == self.package.content_package_id
        for key, value in fields.items():
            setattr(self.package, key, copy.deepcopy(value))

    def transition_task(self, task_id, source, target, **_kwargs):
        assert task_id == self.task.task_id and self.task.task_status == source
        statuses.task_ensure_transition(source, target)
        self.task.task_status = target

    def insert_quality_review(self, review):
        self.reviews.append(review)

    def list_quality_reviews(self, revision_id, *, scope=None, target_id=None):
        return [
            item for item in self.reviews
            if item.revision_id == revision_id
            and (scope is None or item.scope == scope)
            and (target_id is None or item.target_id == target_id)
        ]

    def release_revision(self, task_id, revision_id, **kwargs):
        assert task_id == self.task.task_id and revision_id == self.revision.revision_id
        assert kwargs["review_scope"] == "photo_package"
        assert kwargs["review_target_id"] == self.package.content_package_id
        assert kwargs["expected_selection_hash"] == self.revision.selection_hash
        self.revision.revision_status = "released"
        self.task.released_revision_id = revision_id
        self.task.task_status = "photo_ready"
        self.task.row_version += 1
        self.package.status = statuses.PACKAGE_READY
        return self.task


class PhotoPackageTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.repo = PhotoRepo(self.root)
        self.template = {
            "template_id": "PHOTO_SINGLE_V1", "template_version": 1,
            "width": 360, "height": 640, "cover_index": 1,
        }
        self.specs = [
            {"index": index, "layout": "single", "source_slots": [index]}
            for index in range(1, 6)
        ]

    def tearDown(self):
        self.temp.cleanup()

    def test_native_flow_keeps_output_root_for_default_travel_qa(self):
        root = self.root / "outputs"
        flow = NativePhotoProductionFlow(self.repo, None, output_root=root)
        self.assertEqual(flow.output_root, root)

    def test_exporter_accepts_four_page_travel_contract(self):
        self.repo.task.requested_shot_count = 4
        specs = [
            {"index": index, "layout": "single", "source_slots": [index]}
            for index in range(1, 5)
        ]
        manifest = PhotoPackageExporter(
            self.repo, output_root=self.root / "outputs-four"
        ).export("photo-task", template=self.template, slide_specs=specs)
        self.assertEqual(len(manifest["slides"]), 4)

    def _export_and_release(self):
        package = PhotoPackageExporter(
            self.repo, output_root=self.root / "outputs"
        ).export("photo-task", template=self.template, slide_specs=self.specs)
        review = PhotoPackageReviewService(self.repo).record(
            "photo-task", decision="passed", dimensions={
                "operator_preview": True,
                "content_alignment": True,
                "language_confirmed": True,
            },
            reviewer_type="human", reviewer="alice",
        )
        release = freeze_photo_release(self.repo, self.repo.task)
        return package, review, release

    def test_technical_photo_review_never_releases_content(self):
        PhotoPackageExporter(
            self.repo, output_root=self.root / "outputs"
        ).export("photo-task", template=self.template, slide_specs=self.specs)
        review = PhotoPackageReviewService(self.repo).record(
            "photo-task", decision="passed", dimensions={},
            evidence={
                "schema_version": "opv-technical-check-v1",
                "production_policy": "technical_only",
                "scope": "photo_package",
                "visual_review_performed": False,
                "checks": {
                    "frozen_inputs": True, "file_hash": True,
                    "media_qc": True, "decoded_images": True,
                },
            }, reviewer_type="technical", reviewer="opv_technical_pipeline",
        )
        self.assertEqual(review.decision, "passed")
        self.assertIsNone(self.repo.task.released_revision_id)
        with self.assertRaisesRegex(ReleaseGateError, "已验收|人工内容"):
            freeze_photo_release(self.repo, self.repo.task)

    def test_theme_brief_survives_package_and_release_freeze(self):
        self.repo.revision.plan_snapshot_json["plan"]["theme_brief"] = {
            "theme_key": "AUTUMN_OUTFIT", "label_zh": "秋季暖色穿搭",
            "reference_mode": "STYLE",
        }
        self.repo.revision.input_snapshot_hash = canonical_hash(
            self.repo.revision.plan_snapshot_json
        )
        package, _review, release = self._export_and_release()
        self.assertEqual(package["theme_brief"]["theme_key"], "AUTUMN_OUTFIT")
        self.assertEqual(release["theme_brief"]["reference_mode"], "STYLE")

    def test_theme_change_invalidates_existing_photo_review(self):
        self.repo.revision.plan_snapshot_json["plan"]["theme_brief"] = {
            "theme_key": "COOL_WEATHER_TRAVEL", "travel_theme_type": "CHECK_IN",
        }
        self.repo.revision.input_snapshot_hash = canonical_hash(
            self.repo.revision.plan_snapshot_json
        )
        PhotoPackageExporter(
            self.repo, output_root=self.root / "outputs"
        ).export("photo-task", template=self.template, slide_specs=self.specs)
        PhotoPackageReviewService(self.repo).record(
            "photo-task", decision="passed", dimensions={
                "operator_preview": True,
                "content_alignment": True,
                "language_confirmed": True,
            }, reviewer_type="human", reviewer="alice",
        )
        self.repo.package.photo_manifest_json["theme_brief"]["travel_theme_type"] = "COLOR_MATCH"
        with self.assertRaisesRegex(ReleaseGateError, "人工内容"):
            freeze_photo_release(self.repo, self.repo.task)

    def test_exports_ordered_jpegs_without_video_render(self):
        package = PhotoPackageExporter(
            self.repo, output_root=self.root / "outputs"
        ).export("photo-task", template=self.template, slide_specs=self.specs)
        self.assertEqual(self.repo.task.task_status, "photo_packaging")
        self.assertEqual([item["index"] for item in package["slides"]], [1, 2, 3, 4, 5])
        self.assertEqual(
            list(self.repo.revision.asset_manifest_json["selected"])[-5:],
            [f"slide:{index}" for index in range(1, 6)],
        )
        self.assertTrue(all(Path(item["path"]).is_file() for item in package["slides"]))

    def test_release_and_upload_bind_exact_order_and_bytes(self):
        _package, review, release = self._export_and_release()
        self.assertEqual(review.scope, "photo_package")
        paths = [item["path"] for item in release["slides"]]
        context = {
            "workflow_version": 2, "release_manifest": release,
            "publish_title": "เลือกหนึ่งลุค",
        }
        validate_photo_upload(
            context, media_paths=paths, script_id="photo-task", title="เลือกหนึ่งลุค"
        )
        with self.assertRaisesRegex(ReleaseGateError, "第 1 张|顺序"):
            validate_photo_upload(
                context, media_paths=list(reversed(paths)),
                script_id="photo-task", title="เลือกหนึ่งลุค",
            )
        Path(paths[0]).write_bytes(b"changed")
        with self.assertRaisesRegex(ReleaseGateError, "第 1 张"):
            validate_photo_upload(
                context, media_paths=paths, script_id="photo-task", title="เลือกหนึ่งลุค"
            )

    def test_recipe_can_require_native_approved_copy_before_release(self):
        self.repo.task.recipe_id = "PHOTO_TH_THERMAL_TRANSITION_V1"
        self.repo.get_content_recipe = lambda recipe_id: SimpleNamespace(
            recipe_id=recipe_id,
            recipe_spec_json={
                "release_requirements": {
                    "required_language_review_status": "NATIVE_APPROVED",
                }
            },
        )
        PhotoPackageExporter(
            self.repo, output_root=self.root / "outputs-language-gate"
        ).export("photo-task", template=self.template, slide_specs=self.specs)
        with self.assertRaisesRegex(ReleaseGateError, "LANGUAGE_REVIEW_REQUIRED"):
            PhotoPackageReviewService(self.repo).record(
                "photo-task", decision="passed", dimensions={
                    "operator_preview": True,
                    "content_alignment": True,
                    "language_confirmed": True,
                }, reviewer_type="human", reviewer="alice",
            )
        self.repo.package.photo_manifest_json["copy"]["language_review_status"] = "NATIVE_APPROVED"
        with self.assertRaisesRegex(ReleaseGateError, "缺少审校人"):
            PhotoPackageReviewService(self.repo).record(
                "photo-task", decision="passed", dimensions={
                    "operator_preview": True,
                    "content_alignment": True,
                    "language_confirmed": True,
                }, reviewer_type="human", reviewer="alice",
            )
        self.repo.package.photo_manifest_json["copy"]["language_review"] = {
            "reviewed_by": "native-reviewer",
            "reviewed_at": "2026-09-12T12:00:00+00:00",
            "review_sha256": frozen_copy_review_sha256(
                self.repo.package.photo_manifest_json["copy"]
            ),
        }
        PhotoPackageReviewService(self.repo).record(
            "photo-task", decision="passed", dimensions={
                "operator_preview": True,
                "content_alignment": True,
                "language_confirmed": True,
            }, reviewer_type="human", reviewer="alice",
        )
        release = freeze_photo_release(self.repo, self.repo.task)
        self.assertEqual(release["copy"]["language_review_status"], "NATIVE_APPROVED")

    def test_content_rejection_blocks_later_pass_waiver_and_release(self):
        PhotoPackageExporter(self.repo, output_root=self.root / "outputs").export(
            "photo-task", template=self.template, slide_specs=self.specs)
        service = PhotoPackageReviewService(self.repo)
        rejection = service.record("photo-task", decision="failed", dimensions={"content_valid": False},
            reason_codes=["CONTENT_REJECTED"], reviewer_type="assistant", reviewer="codex_user_directive")
        self.repo.list_content_rejections = lambda task_id: [rejection]
        for decision in ("passed", "waived"):
            with self.assertRaisesRegex(ReleaseGateError, "CONTENT_REJECTED"):
                service.record("photo-task", decision=decision, dimensions={"operator_preview": True}, reviewer="operator")
        with self.assertRaisesRegex(ReleaseGateError, "CONTENT_REJECTED"):
            freeze_photo_release(self.repo, self.repo.task)
        workflow = FeishuTaskWorkflow(self.repo, object(), generator=object(), renderer=object())
        with self.assertRaisesRegex(ReleaseGateError, "CONTENT_REJECTED"):
            workflow._approve_photo_packages(SimpleNamespace(record_id="record"), [self.repo.task])
        self.assertIsNone(self.repo.task.released_revision_id)
        self.assertEqual(len(self.repo.reviews), 1)

    def test_released_package_cannot_be_exported_in_place(self):
        self._export_and_release()
        with self.assertRaisesRegex(PhotoPackageError, "immutable"):
            PhotoPackageExporter(self.repo, output_root=self.root / "outputs").export(
                "photo-task", template=self.template, slide_specs=self.specs
            )

    def test_shipped_choice_layout_exports_real_four_image_cover_with_text(self):
        import json
        config_path = PACKAGE_ROOT / "config/layouts/PHOTO_CHOICE_GRID_V1.json"
        layout = json.loads(config_path.read_text(encoding="utf-8"))
        normalized = normalize_photo_template(layout)
        self.assertEqual(normalized["template_id"], "PHOTO_CHOICE_GRID_V1")
        specs = [
            {"index": 1, "layout": "grid_2x2", "source_slots": [2, 3, 4, 5],
             "overlay_text": "A  B  C  D"},
            *[
                {"index": index, "layout": "single", "source_slots": [index],
                 "overlay_text": chr(63 + index)}
                for index in range(2, 6)
            ],
        ]
        manifest = PhotoPackageExporter(
            self.repo, output_root=self.root / "outputs"
        ).export("photo-task", template=layout, slide_specs=specs)
        with Image.open(manifest["slides"][0]["path"]) as cover:
            self.assertEqual(cover.size, (1080, 1920))
        self.assertEqual(len(manifest["slides"][0]["source_asset_ids"]), 4)

    def test_choice_card_v2_exports_with_separate_last_slide_cta(self):
        import json
        layout = json.loads(
            (PACKAGE_ROOT / "config/layouts/PHOTO_CHOICE_CARD_V2.json").read_text(encoding="utf-8")
        )
        normalized = normalize_photo_template(layout)
        self.assertTrue(normalized["split_last_line_to_bottom"])
        specs = [
            {"index": index, "layout": "single", "source_slots": [index],
             "overlay_text": ("D · ลุคนี้\nคุณชอบลุคไหน?" if index == 5 else f"Look {index}")}
            for index in range(1, 6)
        ]
        manifest = PhotoPackageExporter(self.repo, output_root=self.root / "outputs-v2").export(
            "photo-task", template=layout, slide_specs=specs,
        )
        self.assertEqual(len(manifest["slides"]), 5)

    def test_travel_card_v3_has_safe_offsets_and_large_cover(self):
        import json
        layout = json.loads(
            (PACKAGE_ROOT / "config/layouts/PHOTO_TRAVEL_CARD_V3.json").read_text(
                encoding="utf-8"
            )
        )
        normalized = normalize_photo_template(layout)
        self.assertEqual(normalized["template_version"], 3)
        self.assertEqual(normalized["cover_font_size"], 64)
        self.assertEqual(normalized["top_offset"], 120)
        self.assertEqual(normalized["bottom_offset"], 260)

    def test_split_last_slide_uses_cta_font_size(self):
        from unittest.mock import patch

        image = Image.new("RGB", (1080, 1920), "white")
        requested_sizes = []

        def tracking_font(_template, *, required, size=None):
            requested_sizes.append(size)
            return ImageFont.truetype(
                "/System/Library/Fonts/Supplemental/Arial Unicode.ttf", size=size
            )

        template = {
            "font_size": 42, "detail_font_size": 38, "cta_font_size": 40,
            "min_font_size": 28, "padding_x": 28, "padding_y": 22,
            "top_offset": 120, "bottom_offset": 260,
            "split_last_line_to_bottom": True,
        }
        with patch("services.photo_package._font", side_effect=tracking_font):
            _draw_overlay(
                image, "D look\nPick A B C or D?", template,
                index=5, cover_index=1, total=5,
            )
        self.assertIn(38, requested_sizes)
        self.assertIn(40, requested_sizes)

    def test_rejects_cover_index_outside_ordered_slides(self):
        template = {**self.template, "cover_index": 6}
        with self.assertRaisesRegex(PhotoPackageError, "cover_index"):
            PhotoPackageExporter(
                self.repo, output_root=self.root / "outputs"
            ).export("photo-task", template=template, slide_specs=self.specs)

    def test_released_photo_package_enters_existing_scheduler_without_mp4_or_bgm(self):
        _package, _review, release = self._export_and_release()
        self.repo.task.target_country = "TH"
        self.repo.task.target_locale = "th-TH"
        self.repo.task.account_id = "OPV_TH_TEST_001"
        self.repo.task.product_id = None
        self.repo.task.category_key = "womenswear"
        self.repo.task.theme_id = None
        self.repo.task.recipe_id = "PHOTO_TH_PICK_YOUR_LOOK_V1"
        self.repo.task.feishu_record_id = None
        self.repo.task.source_record_id = "photo-source"
        self.repo.task.plan_json = copy.deepcopy(self.repo.revision.plan_snapshot_json["plan"])
        self.repo.task.copy_json = copy.deepcopy(release["copy"])
        db = AutoPublishDB(self.root / "publisher.sqlite3")
        result = MainScheduleBridge(self.repo, db=db).enqueue_task("photo-task")
        self.assertEqual(result["media_kind"], "native_photo")
        row = db.get_video_asset("opv:photo-task")
        self.assertEqual(row["media_kind"], "native_photo")
        self.assertIsNone(row["local_file_path"])
        manifest = __import__("json").loads(row["photo_manifest_json"])
        self.assertEqual(manifest["manifest_sha256"], release["manifest_sha256"])
        metadata = db.get_script_metadata("opv:photo-task")
        context = __import__("json").loads(metadata["script_text"])
        self.assertEqual(context["audio_mode"], "platform_auto_bgm")
        self.assertNotIn("video_duration_ms", context)

    def test_vn_photo_package_enqueues_to_the_configured_vn_store_once(self):
        """2026-09-14（方案 §3B）：VN 店铺路由补上后，已验收的原生图文包
        必须能按既有桥接入队到 VNPS01，且重复入队不产生重复候选。"""
        _package, _review, release = self._export_and_release()
        self.repo.task.target_country = "VN"
        self.repo.task.target_locale = "vi-VN"
        self.repo.task.account_id = "OPV_VN_TEST_001"
        self.repo.task.product_id = None
        self.repo.task.category_key = "scarf"
        self.repo.task.theme_id = None
        self.repo.task.recipe_id = "PHOTO_TRAVEL_OUTFIT_V3"
        self.repo.task.feishu_record_id = None
        self.repo.task.source_record_id = "vn-photo-source"
        self.repo.task.plan_json = copy.deepcopy(self.repo.revision.plan_snapshot_json["plan"])
        self.repo.task.copy_json = copy.deepcopy(release["copy"])
        db = AutoPublishDB(self.root / "publisher-vn.sqlite3")
        bridge = MainScheduleBridge(self.repo, db=db)

        result = bridge.enqueue_task("photo-task")
        bridge.enqueue_task("photo-task")

        self.assertEqual(result["media_kind"], "native_photo")
        self.assertEqual(result["store_id"], "VNPS01")
        rows = [row for row in db.list_script_metadata()
                if row.source_record_id == "vn-photo-source"]
        self.assertEqual(len(rows), 1, "VN 图文重复入队不得产生重复候选")
        self.assertEqual(rows[0].store_id, "VNPS01")
        context = json.loads(rows[0].script_text)
        self.assertEqual(context["publish_store_id"], "VNPS01")
        self.assertEqual(context["media_kind"], "native_photo")
        self.assertEqual(context["audio_mode"], "platform_auto_bgm")

    def test_feishu_operator_approval_releases_actual_photo_package(self):
        PhotoPackageExporter(
            self.repo, output_root=self.root / "outputs"
        ).export("photo-task", template=self.template, slide_specs=self.specs)

        class Client:
            def __init__(self):
                self.fields = {}

            def update_record_fields(inner, _record_id, fields):
                inner.fields.update(fields)

        client = Client()
        workflow = FeishuTaskWorkflow(
            self.repo, client, generator=object(), renderer=object(),
        )
        result = workflow._approve_and_render(
            SimpleNamespace(record_id="rec-photo", fields={})
        )
        self.assertEqual(result["action"], "approve_native_photo")
        self.assertEqual(self.repo.task.task_status, "photo_ready")
        self.assertEqual(client.fields["审核阶段"], "已验收")
        self.assertFalse(client.fields["确认发布"])


if __name__ == "__main__":
    unittest.main()
