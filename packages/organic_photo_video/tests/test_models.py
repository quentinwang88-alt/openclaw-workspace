#!/usr/bin/env python3
"""Round-trip tests for domain/models.py OPV table mappings."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
import re
import sys
import unittest
from datetime import datetime

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from domain import models
from domain.models import (
    AccountProfile,
    AssetSet,
    ContentPackage,
    ContentRecipe,
    ContentShot,
    ContentTask,
    FeishuOutbox,
    LookFeedback,
    MarketPack,
    MetricSnapshot,
    PublishRecord,
    ProductReferencePack,
    RenderPreset,
    ThemeCatalog,
    VideoRender,
)


def market_pack() -> MarketPack:
    return MarketPack(
        market_pack_id="MP_TH_DEFAULT_V1",
        pack_key="MP_TH_DEFAULT",
        pack_version=1,
        target_country="TH",
        target_locale="th-TH",
        climate_zone="tropical_humid",
        pack_name="泰国默认 Market Pack V1",
        status="active",
        visual_rules_json={"aspect_ratio": "9:16"},
        copy_rules_json={"locale": "th-TH"},
        topic_rules_json={"posting_windows_local": ["19:00-22:00"]},
        safety_rules_json={"banned": ["medical claims"]},
    )


def theme() -> ThemeCatalog:
    return ThemeCatalog(
        theme_id="THEME_X_V1",
        theme_key="THEME_X",
        theme_name="测试主题",
        theme_version=1,
        status="active",
        applicable_markets_json=["TH"],
        product_match_rules_json={"categories": ["dress"]},
        content_plan_rules_json={"copy_tone": "casual"},
        default_storyboard_json={"slots": [{"slot_index": 1}]},
    )


def render_preset() -> RenderPreset:
    return RenderPreset(
        render_preset_id="RP_X_V1",
        preset_key="RP_X",
        preset_name="测试母版",
        status="active",
        motion_rules_json={"allowed_presets": ["slow_push"]},
        transition_rules_json={"allowed_transitions": ["cut"]},
        text_overlay_rules_json={"safe_area_px": {"top": 220}},
        audio_rules_json={"default_strategy": "platform_hot_bgm"},
        output_rules_json={"container": "mp4"},
    )


def account() -> AccountProfile:
    return AccountProfile(
        account_id="OPV_TEST_1",
        account_code="opv-test-1",
        account_name="test",
        target_country="TH",
        default_locale="th-TH",
        timezone="Asia/Bangkok",
        status="testing",
        persona_ref_id="PERSONA_1",
        allowed_look_refs_json=["LOOK_1"],
        core_scene_refs_json=["SCENE_1"],
        default_market_pack_id="MP_TH_DEFAULT_V1",
        default_render_preset_id="RP_X_V1",
    )


class ModelRoundTripTest(unittest.TestCase):
    def _assert_round_trip(self, model, from_row) -> None:
        row = model.to_row()
        self.assertNotIn("created_at", row)
        self.assertNotIn("updated_at", row)
        revived = from_row(row)
        self.assertEqual(revived.to_row(), row)

    def test_market_pack_round_trip_keeps_json_dicts(self) -> None:
        pack = market_pack()
        row = pack.to_row()
        self.assertEqual(row["visual_rules_json"], '{"aspect_ratio":"9:16"}')
        self._assert_round_trip(pack, MarketPack.from_row)

    def test_theme_render_preset_account_round_trip(self) -> None:
        self._assert_round_trip(theme(), ThemeCatalog.from_row)
        self._assert_round_trip(render_preset(), RenderPreset.from_row)
        self._assert_round_trip(account(), AccountProfile.from_row)

    def test_product_reference_pack_round_trip(self) -> None:
        pack = ProductReferencePack(
            pack_id="opv_prp_1",
            product_id="P1",
            variant_key="light_blue",
            product_name="coat",
            status="limited",
            is_default=True,
            assets_json=[{
                "local_path": "/tmp/ref.jpg", "role": "front", "usable": True,
            }],
            asset_fingerprint="a" * 64,
        )
        row = pack.to_row()
        self.assertEqual(row["is_default"], 1)
        self._assert_round_trip(pack, ProductReferencePack.from_row)

    def test_content_task_round_trip_with_optional_plan(self) -> None:
        task = ContentTask(
            task_id="opv_task_20260830_abcdef123456",
            idempotency_key="a" * 64,
            account_id="OPV_TEST_1",
            product_id="PROD_1",
            target_country="TH",
            target_locale="th-TH",
            product_snapshot_json={"reference_images": ["oss://a.jpg"]},
            plan_json={"schema_version": "opv-plan-v1", "shots": []},
            theme_id="THEME_X_V1",
        )
        row = task.to_row()
        self.assertIsInstance(row["plan_json"], str)
        self.assertIsNone(row["copy_json"])
        self._assert_round_trip(task, ContentTask.from_row)

    def test_native_photo_task_allows_no_product(self) -> None:
        task = ContentTask(
            task_id="opv_task_photo_1",
            idempotency_key="b" * 64,
            account_id="OPV_TEST_1",
            product_id=None,
            target_country="MX",
            target_locale="es-MX",
            media_kind="native_photo",
            category_key="wig",
            product_mode="NO_PRODUCT",
        )
        row = task.to_row()
        self.assertIsNone(row["product_id"])
        self.assertEqual(row["media_kind"], "native_photo")
        self._assert_round_trip(task, ContentTask.from_row)

    def test_content_shot_round_trip_bool_and_version(self) -> None:
        shot = ContentShot(
            shot_id="opv_shot_20260830_abcdef123456",
            task_id="opv_task_20260830_abcdef123456",
            slot_index=1,
            slot_role="hero",
            duration_ms=2200,
            shot_version=2,
            source_refs_json=["PERSONA_1", "LOOK_1"],
            is_selected=True,
            qa_json={"score": 0.9},
            outfit_state_ref="FINAL",
        )
        row = shot.to_row()
        self.assertEqual(row["is_selected"], 1)
        revived = ContentShot.from_row(row)
        self.assertTrue(revived.is_selected)
        self.assertEqual(revived.shot_version, 2)
        self.assertEqual(revived.qa_json, {"score": 0.9})
        self.assertEqual(revived.outfit_state_ref, "FINAL")
        self.assertEqual(revived.to_row(), row)

    def test_photo_shot_does_not_require_duration(self) -> None:
        shot = ContentShot(
            shot_id="opv_shot_photo_1",
            task_id="opv_task_photo_1",
            slot_index=1,
            slot_role="choice_grid",
            duration_ms=None,
        )
        self._assert_round_trip(shot, ContentShot.from_row)

    def test_video_render_and_publish_record_round_trip(self) -> None:
        render = VideoRender(
            render_id="opv_render_20260830_abcdef123456",
            task_id="opv_task_20260830_abcdef123456",
            render_preset_id="RP_X_V1",
            copy_snapshot_json={"title": "t"},
            shot_selection_json=["shot1", "shot2"],
            timeline_json=[{"slot_index": 1, "start_ms": 0}],
            publish_ready=True,
            output_metadata_json={"container": "mp4"},
        )
        self._assert_round_trip(render, VideoRender.from_row)
        record = PublishRecord(
            publish_id="opv_pub_20260830_abcdef123456",
            task_id="opv_task_20260830_abcdef123456",
            render_id=render.render_id,
            account_id="OPV_TEST_1",
            caption_snapshot_json={"caption": "c"},
            platform_metadata_json={"audio_strategy": "platform_hot_bgm"},
            planned_publish_at=datetime(2026, 9, 1, 4, 0, 0),
            submitted_at=datetime(2026, 9, 1, 2, 0, 0),
        )
        self._assert_round_trip(record, PublishRecord.from_row)

    def test_native_photo_publish_record_round_trip_without_render(self) -> None:
        record = PublishRecord(
            publish_id="opv_pub_photo_1",
            task_id="opv_task_photo_1",
            render_id=None,
            account_id="OPV_TEST_1",
            media_kind="native_photo",
            content_package_id="opv_package_photo_1",
            revision_id="opv_revision_photo_1",
            main_slot_id=42,
            publisher_account_id="mx-wig-organic",
            publish_channel="creatok",
            provider_task_id="provider-task-1",
            publish_key="c" * 64,
            release_manifest_json={"schema_version": "opv-photo-release-v1"},
        )
        row = record.to_row()
        self.assertIsNone(row["render_id"])
        self.assertIsInstance(row["release_manifest_json"], str)
        self._assert_round_trip(record, PublishRecord.from_row)

    def test_photo_recipe_package_and_asset_set_round_trip(self) -> None:
        recipe = ContentRecipe(
            recipe_id="PHOTO_MX_PICK_HAIR_V1",
            recipe_key="MX_PICK_HAIR",
            content_goal="ORGANIC_ACCOUNT_CONTENT",
            recipe_spec_json={
                "schema_version": "opv-photo-recipe-v1",
                "media_kind": "native_photo",
            },
        )
        self._assert_round_trip(recipe, ContentRecipe.from_row)

        package = ContentPackage(
            content_package_id="opv_package_photo_1",
            task_id="opv_task_photo_1",
            photo_manifest_json={"slides": [{"slot_index": 1}]},
        )
        self._assert_round_trip(package, ContentPackage.from_row)

        asset_set = AssetSet(
            asset_set_id="opv_asset_set_1",
            asset_set_key="MX_WIG_CHOICES",
            asset_set_version=2,
            category_key="wig",
            market="MX",
            status="enabled",
            tags_json={"styles": ["bob", "largo"]},
            manifest_json={"assets": [{"path": "/tmp/wig.jpg", "sha256": "d" * 64}]},
        )
        self._assert_round_trip(asset_set, AssetSet.from_row)

    def test_metric_snapshot_coerces_decimal_to_float(self) -> None:
        captured = datetime(2026, 8, 30, 12, 0, 0)
        row = {
            "metric_id": 7,
            "publish_id": "opv_pub_1",
            "captured_after_hours": 24,
            "captured_at": captured,
            "source_type": "manual",
            "view_count": 1000,
            "like_count": 50,
            "comment_count": 5,
            "share_count": 3,
            "save_count": 8,
            "profile_visit_count": 12,
            "follower_gain_count": 2,
            "avg_watch_time_ms": 8100,
            "completion_rate": Decimal("0.735000"),
            "engagement_rate": Decimal("0.068000"),
            "raw_metrics_json": '{"extra": 1}',
        }
        snapshot = MetricSnapshot.from_row(row)
        self.assertEqual(snapshot.metric_id, 7)
        self.assertIsInstance(snapshot.completion_rate, float)
        self.assertAlmostEqual(snapshot.completion_rate, 0.735)
        self.assertEqual(snapshot.to_row()["completion_rate"], 0.735)

    def test_look_feedback_and_outbox_defaults(self) -> None:
        feedback = LookFeedback(
            feedback_id="opv_fb_1",
            task_id="opv_task_1",
            account_id="OPV_TEST_1",
            product_id="PROD_1",
            feedback_type="human_review",
            decision="promote_candidate",
        )
        self.assertEqual(feedback.promotion_status, "not_requested")
        self._assert_round_trip(feedback, LookFeedback.from_row)

        outbox = FeishuOutbox(
            outbox_id="opv_outbox_1",
            aggregate_type="content_task",
            aggregate_id="opv_task_1",
            operation="upsert_task_row",
            payload_json={"task_id": "opv_task_1"},
        )
        self.assertEqual(outbox.status, "holding")
        self._assert_round_trip(outbox, FeishuOutbox.from_row)


class ModelHelperTest(unittest.TestCase):
    def test_generate_prefixed_id_shape_and_uniqueness(self) -> None:
        first = models.generate_prefixed_id("opv_task")
        second = models.generate_prefixed_id("opv_task")
        self.assertLessEqual(len(first), 64)
        self.assertRegex(first, r"^opv_task_\d{8}_[0-9a-f]{12}$")
        self.assertNotEqual(first, second)

    def test_utc_now_is_naive(self) -> None:
        self.assertIsNone(models.utc_now().tzinfo)

    def test_from_payload_accepts_file_style_and_json_style_keys(self) -> None:
        base = {
            "market_pack_id": "MP_1",
            "pack_key": "MP_1",
            "target_country": "TH",
            "target_locale": "th-TH",
            "pack_name": "p",
        }
        pack = MarketPack.from_payload({**base, "visual_rules": {"a": 1}})
        self.assertEqual(pack.visual_rules_json, {"a": 1})
        pack2 = MarketPack.from_payload({**base, "visual_rules_json": {"b": 2}})
        self.assertEqual(pack2.visual_rules_json, {"b": 2})

    def test_from_payload_rejects_unknown_keys_and_missing_required(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            MarketPack.from_payload({"not_a_field": 1})
        self.assertIn("not_a_field", str(ctx.exception))
        with self.assertRaises(ValueError) as ctx:
            MarketPack.from_payload({"pack_key": "MP_1"})
        self.assertIn("missing required", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
