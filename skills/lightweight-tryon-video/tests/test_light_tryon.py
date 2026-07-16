from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SKILL_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = SKILL_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from light_tryon.database import LightTryonDB  # noqa: E402
from light_tryon.models import ProductInput  # noqa: E402
from light_tryon.planner import plan_product, weighted_scene_sequence  # noqa: E402
from light_tryon.prompting import PROMPT_BUILDER_VERSION, build_jimeng_record, build_prompt  # noqa: E402
from light_tryon.review import export_review_html, set_manual_review  # noqa: E402
from light_tryon.workers import evaluate_qc, probe_video, render_brand_overlay, run_generation_worker, structural_qc  # noqa: E402
from light_tryon.visual_plans import (  # noqa: E402
    confirm_outfit_image,
    create_confirmed_video_jobs,
    orchestrate_visual_plans,
)


class LightTryonTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = LightTryonDB(self.root / "test.sqlite3")
        self.db.init_schema()
        self.seed_counts = self.db.seed_templates(SKILL_DIR / "assets" / "default_templates.json")
        self.product = ProductInput.from_dict(
            {
                "product_id": "SKU-GRAY-TANK-001",
                "product_name": "灰色修身背心",
                "market": "TH",
                "language": "th",
                "category": "tank_top",
                "product_title": "灰色修身短款针织背心",
                "product_images": ["https://example.com/front.jpg", "https://example.com/back.jpg"],
                "core_selling_points": ["修身但不紧", "短款显比例"],
                "target_publish_count": 4,
            }
        )
        self.db.upsert_product(self.product)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _plan_and_build(self) -> list[dict]:
        planned = plan_product(self.db, self.product.product_id)
        self.db.create_jobs(planned)
        jobs = self.db.list_jobs(product_id=self.product.product_id)
        for job in jobs:
            payload = build_prompt(self.db.get_job_context(job["job_id"]))
            self.db.update_prompt(job["job_id"], payload, PROMPT_BUILDER_VERSION)
        return self.db.list_jobs(product_id=self.product.product_id)

    def test_seed_contains_v1_minimums(self) -> None:
        self.assertEqual(self.seed_counts["scenes"], 6)
        self.assertEqual(len(self.db.list_templates("scene", "enabled")), 2)
        self.assertEqual(self.seed_counts["actions"], 9)
        self.assertEqual(self.seed_counts["shot_plans"], 4)
        self.assertEqual(self.seed_counts["stylings"], 6)
        self.assertGreaterEqual(self.seed_counts["subtitles"], 5)
        self.assertEqual(self.seed_counts["personas"], 1)

    def test_weighted_environment_sequence_keeps_default_bedroom(self) -> None:
        scenes = self.db.list_templates("scene", "enabled")
        sequence = weighted_scene_sequence(scenes, 4)
        self.assertEqual([row["scene_id"] for row in sequence], ["SCENE_A_001"] * 4)

    def test_plan_four_jobs(self) -> None:
        jobs = plan_product(self.db, self.product.product_id)
        self.assertEqual(len(jobs), 4)
        self.assertEqual([job.variant_no for job in jobs], [1, 2, 3, 4])
        self.assertEqual([job.duration_seconds for job in jobs], [8, 10, 8, 10])
        self.assertEqual([job.scene_id for job in jobs], ["SCENE_A_001"] * 4)
        self.assertEqual(
            [job.shot_profile_id for job in jobs],
            ["SHOT_FULL_FIXED", "SHOT_UPPER_FIXED", "SHOT_UPPER_THREE_QUARTER", "SHOT_UPPER_PUSH_IN"],
        )
        self.assertGreaterEqual(len({job.action_id for job in jobs}), 3)
        self.assertTrue(all(job.subtitle_id.startswith("SUB_TH_") for job in jobs))
        self.assertEqual(jobs[0].styling_id, "STYLE_001")

    def test_upper_and_outerwear_one_or_five_use_strict_camera_mix(self) -> None:
        for category in ("top", "outerwear"):
            product_id = f"SKU-{category.upper()}-V11"
            self.db.upsert_product(ProductInput.from_dict({
                "product_id": product_id,
                "product_name": "V1.1 镜头策略测试",
                "market": "TH",
                "language": "th",
                "category": category,
                "target_publish_count": 5,
            }))
            five = plan_product(self.db, product_id, count=5)
            expected = (
                ["SHOT_UPPER_FIXED", "SHOT_UPPER_THREE_QUARTER", "SHOT_UPPER_FIXED", "SHOT_UPPER_THREE_QUARTER", "SHOT_UPPER_PUSH_IN"]
                if category == "outerwear"
                else ["SHOT_FULL_FIXED", "SHOT_UPPER_FIXED", "SHOT_UPPER_THREE_QUARTER", "SHOT_UPPER_FIXED", "SHOT_UPPER_PUSH_IN"]
            )
            self.assertEqual([job.shot_profile_id for job in five], expected)
            self.assertEqual([job.scene_id for job in five], ["SCENE_A_001"] * 5)
            one = plan_product(self.db, product_id, count=1)
            self.assertEqual([job.scene_id for job in one], ["SCENE_A_001"])
            self.assertEqual([job.shot_profile_id for job in one], ["SHOT_UPPER_FIXED"])
            for job in five[2 if category == "top" else 0:]:
                action = self.db.get_template("action", job.action_id)
                if job.shot_profile_id == "SHOT_UPPER_THREE_QUARTER":
                    self.assertIn("SHOT_UPPER_FIXED", action["applicable_shot_profiles"])
                else:
                    self.assertIn(job.shot_profile_id, action["applicable_shot_profiles"])
                if category == "outerwear":
                    self.assertIn(job.action_id, {"ACT_008", "ACT_009"})

    def test_prompt_camera_contract_is_conditional_and_self_contained(self) -> None:
        product_id = "SKU-OUTER-CAMERA-V11"
        self.db.upsert_product(ProductInput.from_dict({
            "product_id": product_id,
            "product_name": "浅色外套",
            "market": "TH",
            "language": "th",
            "category": "outerwear",
            "target_publish_count": 5,
        }))
        planned = plan_product(self.db, product_id, count=5)
        self.db.create_jobs(planned)
        payloads = [build_prompt(self.db.get_job_context(job.job_id)) for job in planned]
        fixed, push = payloads[0], payloads[-1]
        self.assertEqual(fixed["generation"]["camera"], "fixed")
        self.assertIn("镜头全程固定，不推拉", fixed["positive_prompt"])
        self.assertEqual(push["generation"]["camera"], "push_in")
        self.assertIn("极慢平稳推近", push["positive_prompt"])
        self.assertNotIn("不推拉", push["positive_prompt"])
        self.assertNotIn("不要镜头推进", push["negative_prompt"])
        self.assertEqual(push["qc_expectations"]["camera_motion"], "push_in")
        self.assertTrue(push["qc_expectations"]["upper_garment_fully_visible"])
        self.assertTrue(push["qc_expectations"]["no_overexposure"])
        self.assertTrue(push["qc_expectations"]["product_color_preserved"])

    def test_plan_is_idempotent_per_version(self) -> None:
        jobs = plan_product(self.db, self.product.product_id)
        first = self.db.create_jobs(jobs)
        second = self.db.create_jobs(jobs)
        self.assertEqual(first, {"created": 4, "existing": 0})
        self.assertEqual(second, {"created": 0, "existing": 4})

    def test_plan_new_version_creates_new_jobs(self) -> None:
        self.db.create_jobs(plan_product(self.db, self.product.product_id, plan_version="v1"))
        result = self.db.create_jobs(plan_product(self.db, self.product.product_id, plan_version="v2"))
        self.assertEqual(result["created"], 4)
        self.assertEqual(len(self.db.list_jobs(product_id=self.product.product_id)), 8)

    def test_recommended_styling_pool_preserves_preference_order(self) -> None:
        self.db.upsert_product(
            ProductInput.from_dict(
                {
                    **self.product.to_dict(),
                    "recommended_styling_pool": ["STYLE_003", "STYLE_001"],
                }
            )
        )
        jobs = plan_product(self.db, self.product.product_id)
        self.assertEqual([jobs[0].styling_id, jobs[1].styling_id], ["STYLE_003", "STYLE_001"])

    def test_action_priority_scene_scope_and_explicit_testing_status(self) -> None:
        base = self.db.get_template("action", "ACT_007")
        cafe_only = {
            **base,
            "action_id": "ACT_TEST_CAFE_ONLY",
            "action_name": "咖啡店限定高优先级",
            "status": "enabled",
            "priority": 999,
            "applicable_categories": ["tank_top"],
            "applicable_scenes": ["ENV_CAFE_001"],
            "applicable_shot_profiles": ["SHOT_UPPER_FIXED"],
        }
        selected = {
            **base,
            "action_id": "ACT_TEST_EXPLICIT",
            "action_name": "显式测试动作",
            "status": "testing",
            "priority": 1000,
            "applicable_categories": ["tank_top"],
            "applicable_scenes": [],
            "applicable_shot_profiles": ["SHOT_UPPER_FIXED"],
        }
        enabled = {
            **base,
            "action_id": "ACT_TEST_ENABLED",
            "action_name": "启用高优先级动作",
            "status": "enabled",
            "priority": 500,
            "applicable_categories": ["tank_top"],
            "applicable_scenes": [],
            "applicable_shot_profiles": ["SHOT_UPPER_FIXED"],
        }
        for row in (cafe_only, selected, enabled):
            self.db.upsert_template("action", row)

        automatic = plan_product(self.db, self.product.product_id, count=1)
        self.assertEqual(automatic[0].action_id, "ACT_TEST_ENABLED")
        self.assertEqual(automatic[0].shot_plan_id, "SHOTPLAN_UPPER_BALANCED")

        self.db.upsert_product(ProductInput.from_dict({
            **self.product.to_dict(),
            "recommended_action_pool": ["ACT_TEST_EXPLICIT"],
            "shot_plan_id": "SHOTPLAN_UPPER_BALANCED",
            "target_publish_count": 1,
        }))
        explicit = plan_product(self.db, self.product.product_id, count=1)
        self.assertEqual(explicit[0].action_id, "ACT_TEST_EXPLICIT")

    def test_explicit_shot_plan_controls_sequence(self) -> None:
        self.db.upsert_product(ProductInput.from_dict({
            **self.product.to_dict(),
            "shot_plan_id": "SHOTPLAN_GENERIC_BALANCED",
            "target_publish_count": 5,
        }))
        jobs = plan_product(self.db, self.product.product_id, count=5)
        self.assertEqual({job.shot_plan_id for job in jobs}, {"SHOTPLAN_GENERIC_BALANCED"})
        self.assertEqual(
            [job.shot_profile_id for job in jobs],
            ["SHOT_FULL_FIXED", "SHOT_FULL_FIXED", "SHOT_UPPER_FIXED", "SHOT_UPPER_THREE_QUARTER", "SHOT_UPPER_PUSH_IN"],
        )

    def test_unknown_language_is_not_silently_localized(self) -> None:
        self.db.upsert_product(
            ProductInput.from_dict(
                {
                    "product_id": "SKU-JP-001",
                    "product_name": "测试上衣",
                    "market": "JP",
                    "language": "ja",
                    "category": "top",
                }
            )
        )
        with self.assertRaisesRegex(ValueError, "没有适用于"):
            plan_product(self.db, "SKU-JP-001")

    def test_prompt_has_content_id_and_clean_plate_rule(self) -> None:
        jobs = self._plan_and_build()
        job = jobs[0]
        payload = job["prompt_payload"]
        self.assertIn(f"【内容ID】{job['job_id']}", payload["positive_prompt"])
        self.assertIn("不出现字幕", payload["positive_prompt"])
        self.assertIn("品牌字标与服装类目如需使用只由后期添加", payload["positive_prompt"])
        self.assertEqual(payload["display_prompt"], payload["positive_prompt"])
        self.assertLess(len(payload["positive_prompt"]), 800)
        self.assertNotIn("template_snapshots", payload["positive_prompt"])
        self.assertNotIn("reference_images", payload["positive_prompt"])
        self.assertEqual(payload["generation"]["ratio"], "9:16")
        self.assertIn(payload["generation"]["duration_seconds"], {8, 10})
        self.assertEqual(len(payload["reference_images"]), 2)
        self.assertEqual(payload["subtitle_plan"]["render_mode"], "post_production_burn_in")

    def test_every_prompt_is_self_contained(self) -> None:
        jobs = self._plan_and_build()
        prompts = [job["prompt_payload"]["positive_prompt"] for job in jobs]
        forbidden = ["与主场景", "主场景相同", "同一套", "同一个女生", "主账号视觉", "原展示区"]
        for prompt in prompts:
            for phrase in forbidden:
                self.assertNotIn(phrase, prompt)
            self.assertIn("暖白色墙面", prompt)
            self.assertIn("灰色落地窗帘", prompt)
            self.assertIn("一名年轻成年女生", prompt)
            self.assertIn("黑色长直发", prompt)
            self.assertIn("浅色素面手机", prompt)

        upper = next(job for job in jobs if job["shot_profile_id"] == "SHOT_UPPER_FIXED")
        self.assertIn("上半身至胯部构图", upper["prompt_payload"]["positive_prompt"])

    def test_one_environment_generates_all_camera_profiles(self) -> None:
        self.db.upsert_product(ProductInput.from_dict({
            **self.product.to_dict(),
            "recommended_scene_pool": ["ENV_CAFE_001"],
            "target_publish_count": 5,
        }))
        jobs = plan_product(self.db, self.product.product_id, count=5)
        self.assertEqual([job.scene_id for job in jobs], ["ENV_CAFE_001"] * 5)
        self.assertEqual(
            [job.shot_profile_id for job in jobs],
            ["SHOT_FULL_FIXED", "SHOT_UPPER_FIXED", "SHOT_UPPER_THREE_QUARTER", "SHOT_UPPER_FIXED", "SHOT_UPPER_PUSH_IN"],
        )

    def test_invalid_preferred_environment_does_not_silently_fallback(self) -> None:
        self.db.upsert_product(ProductInput.from_dict({
            **self.product.to_dict(), "recommended_scene_pool": ["ENV_NOT_FOUND"],
        }))
        with self.assertRaisesRegex(ValueError, "不存在或未启用"):
            plan_product(self.db, self.product.product_id)

    def test_prompt_fingerprint_is_stable(self) -> None:
        planned = plan_product(self.db, self.product.product_id)
        self.db.create_jobs(planned)
        context = self.db.get_job_context(planned[0].job_id)
        one = build_prompt(context)
        two = build_prompt(context)
        self.assertEqual(one["prompt_fingerprint"], two["prompt_fingerprint"])

    def test_brand_plan_starts_on_first_frame_and_disables_dynamic_subtitles(self) -> None:
        persona = self.db.get_template("persona", "PERSONA_001")
        persona.update({
            "brand_overlay_enabled": "enabled",
            "brand_display_name": "TEST STUDIO",
            "brand_style_preset": "cream_serif",
            "brand_primary_color": "cream_white",
            "brand_default_series_title": "",
        })
        self.db.upsert_template("persona", persona)
        jobs = self._plan_and_build()
        payload = jobs[0]["prompt_payload"]
        self.assertTrue(payload["brand_plan"]["enabled"])
        self.assertEqual(payload["brand_plan"]["series_title"], "Easy Layering")
        self.assertEqual(payload["brand_plan"]["display_seconds"], 0.8)
        self.assertEqual(payload["brand_plan"]["fade_in_seconds"], 0.0)
        self.assertTrue(payload["brand_plan"]["cover_from_first_frame"])
        self.assertEqual(payload["brand_plan"]["position"], "center")
        self.assertEqual(payload["brand_plan"]["center_y_ratio"], 0.50)
        self.assertEqual(payload["subtitle_plan"]["render_mode"], "disabled")
        self.assertEqual(payload["subtitle_plan"]["cues"], [])

    def test_brand_renderer_outputs_video_and_cover(self) -> None:
        if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
            self.skipTest("ffmpeg/ffprobe unavailable")
        source = self.root / "source.mp4"
        subprocess.run(
            [
                "ffmpeg", "-y", "-f", "lavfi", "-i", "color=c=#d8d1c6:s=360x640:d=1.4:r=24",
                "-c:v", "libx264", "-pix_fmt", "yuv420p", str(source),
            ],
            check=True,
            capture_output=True,
        )
        output = self.root / "branded.mp4"
        cover = self.root / "cover.jpg"
        result = render_brand_overlay(
            source,
            output,
            {
                "enabled": True,
                "logo_images": [],
                "display_name": "TEST STUDIO",
                "series_title": "Everyday Outerwear",
                "style_preset": "cream_serif",
                "primary_color": "cream_white",
                "display_seconds": 0.8,
                "fade_in_seconds": 0.12,
                "fade_out_seconds": 0.18,
                "max_width_ratio": 0.40,
                "center_y_ratio": 0.66,
            },
            cover_output=cover,
        )
        self.assertTrue(output.is_file())
        self.assertTrue(cover.is_file())
        self.assertEqual(result["overlay"]["display_name"], "TEST STUDIO")
        self.assertTrue(probe_video(output)["has_video_stream"])

    def test_brand_only_persona_change_reuses_visual_plan_and_rebuilds_prompts(self) -> None:
        persona = self.db.get_template("persona", "PERSONA_001")
        persona.update({
            "source_payload": {"persona_id": "PERSONA_001", "prompt_core": persona["prompt_core"]},
            "source_hash": "visual-persona-v1",
        })
        self.db.upsert_template("persona", persona)
        first = orchestrate_visual_plans(
            self.db,
            self.product.product_id,
            source_record_id="rec_brand_only",
            scene_values=["室内INS奶油风"],
            styling_values=["修身上衣加高腰阔腿裤"],
            per_plan_video_count=5,
        )[0]
        confirm_outfit_image(self.db, first["visual_plan_id"], image_path="/tmp/brand-plan-outfit.png")
        create_confirmed_video_jobs(self.db, first["visual_plan_id"])
        before = self.db.list_jobs(source_script_record_id="rec_brand_only")
        self.assertFalse(before[0]["prompt_payload"]["brand_plan"]["enabled"])

        persona = self.db.get_template("persona", "PERSONA_001")
        persona.update({
            "brand_overlay_enabled": "enabled",
            "brand_display_name": "TEST STUDIO",
            "source_payload": {
                "persona_id": "PERSONA_001", "prompt_core": persona["prompt_core"],
                "brand_overlay_enabled": "enabled", "brand_display_name": "TEST STUDIO",
            },
            "source_hash": "brand-config-v2",
        })
        self.db.upsert_template("persona", persona)
        second = orchestrate_visual_plans(
            self.db,
            self.product.product_id,
            source_record_id="rec_brand_only",
            scene_values=["室内INS奶油风"],
            styling_values=["修身上衣加高腰阔腿裤"],
            per_plan_video_count=5,
        )[0]
        self.assertEqual(first["visual_plan_id"], second["visual_plan_id"])
        create_confirmed_video_jobs(self.db, second["visual_plan_id"])
        after = self.db.list_jobs(source_script_record_id="rec_brand_only")
        self.assertTrue(after[0]["prompt_payload"]["brand_plan"]["enabled"])
        self.assertEqual(after[0]["prompt_payload"]["subtitle_plan"]["render_mode"], "disabled")
        self.assertEqual(after[0]["prompt_payload"]["subtitle_plan"]["cues"], [])

    def test_jimeng_record_contract(self) -> None:
        jobs = self._plan_and_build()
        context = self.db.get_job_context(jobs[0]["job_id"])
        record = build_jimeng_record(context, context["job"]["prompt_payload"])
        self.assertEqual(record["状态"], "待处理")
        self.assertEqual(record["任务名"], jobs[0]["job_id"])
        self.assertEqual(record["内容ID"], jobs[0]["job_id"])
        self.assertEqual(record["视频比例"], "9:16")
        self.assertIn(record["视频时长"], {8, 10})
        self.assertEqual(record["模型"], "Seedance 2.0")
        self.assertEqual(record["生成次数"], 1)

    def test_template_status_and_crud(self) -> None:
        self.db.set_template_status("action", "ACT_006", "disabled")
        self.assertEqual(self.db.get_template("action", "ACT_006")["status"], "disabled")
        self.db.set_template_status("action", "ACT_006", "enabled")
        self.assertEqual(self.db.get_template("action", "ACT_006")["status"], "enabled")

    def test_structural_qc_passes_vertical_video(self) -> None:
        result = structural_qc({"duration_seconds": 8.0, "width": 720, "height": 1280})
        self.assertTrue(result["passed"])
        self.assertEqual(result["failures"], [])

    def test_structural_qc_fails_length_and_ratio(self) -> None:
        result = structural_qc({"duration_seconds": 6.0, "width": 1280, "height": 720})
        self.assertFalse(result["passed"])
        self.assertIn("failed_length", result["failures"])
        self.assertIn("failed_aspect_ratio", result["failures"])

    def test_qc_without_vision_requires_manual_review(self) -> None:
        status, result = evaluate_qc(structural_qc({"duration_seconds": 9.0, "width": 720, "height": 1280}))
        self.assertEqual(status, "manual_review")
        self.assertIn("vision_qc_not_run", result["manual_review_reasons"])

    def test_qc_score_thresholds(self) -> None:
        structural = structural_qc({"duration_seconds": 9.0, "width": 720, "height": 1280})
        base = {
            "phone_covers_face": True,
            "clothing_identifiable": True,
            "no_severe_body_anomaly": True,
            "scene_matches_template": True,
            "action_complete": True,
            "visible_anchor_count": 3,
            "camera_motion_matches": True,
            "brightness_adequate": True,
            "no_overexposure": True,
            "product_color_preserved": True,
        }
        passed_status, passed = evaluate_qc(
            structural,
            {**base, "scene_consistency": 28, "person_naturalness": 18, "clothing_clarity": 18, "action_completeness": 14, "realism": 13},
        )
        manual_status, manual = evaluate_qc(
            structural,
            {**base, "scene_consistency": 20, "person_naturalness": 14, "clothing_clarity": 15, "action_completeness": 11, "realism": 10},
        )
        failed_status, failed = evaluate_qc(
            structural,
            {**base, "scene_consistency": 10, "person_naturalness": 8, "clothing_clarity": 10, "action_completeness": 6, "realism": 6},
        )
        self.assertEqual((passed_status, manual_status, failed_status), ("passed", "manual_review", "failed"))
        self.assertGreaterEqual(passed["total_score"], 80)
        self.assertTrue(60 <= manual["total_score"] < 80)
        self.assertLess(failed["total_score"], 60)

    def test_qc_enforces_camera_light_color_and_upper_garment_evidence(self) -> None:
        structural = structural_qc({"duration_seconds": 9.0, "width": 720, "height": 1280})
        vision = {
            "scene_consistency": 30, "person_naturalness": 20, "clothing_clarity": 20,
            "action_completeness": 15, "realism": 15, "phone_covers_face": True,
            "clothing_identifiable": True, "no_severe_body_anomaly": True,
            "scene_matches_template": True, "action_complete": True, "visible_anchor_count": 3,
            "camera_motion_matches": True, "brightness_adequate": True,
            "no_overexposure": True, "product_color_preserved": True,
            "upper_garment_fully_visible": True,
        }
        status, _ = evaluate_qc(structural, vision, {"upper_garment_fully_visible": True})
        self.assertEqual(status, "passed")
        failed_status, failed = evaluate_qc(
            structural, {**vision, "camera_motion_matches": False}, {"upper_garment_fully_visible": True}
        )
        self.assertEqual(failed_status, "failed")
        self.assertIn("failed_camera_motion", failed["failure_codes"])
        missing = dict(vision)
        missing.pop("upper_garment_fully_visible")
        manual_status, manual = evaluate_qc(structural, missing, {"upper_garment_fully_visible": True})
        self.assertEqual(manual_status, "manual_review")
        self.assertIn("upper_garment_fully_visible", manual["missing_required_fields"])

    def test_qc_severe_failure_overrides_high_score(self) -> None:
        structural = structural_qc({"duration_seconds": 9.0, "width": 720, "height": 1280})
        status, result = evaluate_qc(
            structural,
            {
                "scene_consistency": 30,
                "person_naturalness": 20,
                "clothing_clarity": 20,
                "action_completeness": 15,
                "realism": 15,
                "phone_covers_face": False,
                "clothing_identifiable": True,
                "no_severe_body_anomaly": True,
                "scene_matches_template": True,
                "action_complete": True,
                "visible_anchor_count": 3,
            },
        )
        self.assertEqual(status, "failed")
        self.assertIn("failed_face_visibility", result["failure_codes"])

    def test_qc_missing_required_evidence_cannot_auto_pass(self) -> None:
        structural = structural_qc({"duration_seconds": 9.0, "width": 720, "height": 1280})
        status, result = evaluate_qc(
            structural,
            {
                "scene_consistency": 30,
                "person_naturalness": 20,
                "clothing_clarity": 20,
                "action_completeness": 15,
                "realism": 15,
                "visible_anchor_count": 3,
            },
        )
        self.assertEqual(status, "manual_review")
        self.assertIn("phone_covers_face", result["missing_required_fields"])

    def test_command_generation_worker_updates_state(self) -> None:
        jobs = self._plan_and_build()
        output_video = self.root / "fake.mp4"
        output_video.write_bytes(b"fake video payload")
        worker = self.root / "worker.py"
        worker.write_text(
            "import json,sys\n"
            "payload=json.load(sys.stdin)\n"
            f"print(json.dumps({{'output_video_path': {str(output_video)!r}, 'provider_task_id': payload['job']['job_id']}}))\n",
            encoding="utf-8",
        )
        summary = run_generation_worker(self.db, f"{sys.executable} {worker}", limit=1)
        self.assertEqual(summary["success"], 1)
        updated = self.db.get_job(jobs[0]["job_id"])
        self.assertEqual(updated["generation_status"], "success")
        self.assertEqual(updated["raw_video_path"], str(output_video.resolve()))

    def test_review_html_and_manual_decision(self) -> None:
        jobs = self._plan_and_build()
        output_video = self.root / "fake.mp4"
        output_video.write_bytes(b"fake")
        job_id = jobs[0]["job_id"]
        self.db.claim_pending_jobs(1, "test")
        self.db.complete_generation(job_id, {"output_video_path": str(output_video)})
        result = export_review_html(self.db, self.root / "review.html")
        self.assertEqual(result["jobs"], 1)
        text = (self.root / "review.html").read_text(encoding="utf-8")
        self.assertIn(job_id, text)
        decision = set_manual_review(self.db, job_id, "passed", "人工确认可发布")
        self.assertEqual(decision["qc_status"], "passed")
        self.assertEqual(self.db.get_job(job_id)["qc_status"], "passed")

    def test_two_scenes_two_stylings_create_four_gated_visual_plans(self) -> None:
        plans = orchestrate_visual_plans(
            self.db,
            self.product.product_id,
            source_record_id="rec_2x2",
            scene_values=["现代简约卧室", "明亮现代咖啡店"],
            styling_values=["修身上衣加高腰阔腿裤", "日常上衣加直筒牛仔裤"],
            per_plan_video_count=5,
        )
        self.assertEqual(len(plans), 4)
        self.assertEqual(len({(row["scene_id"], row["styling_id"]) for row in plans}), 4)
        self.assertEqual({row["outfit_image_status"] for row in plans}, {"pending"})
        self.assertEqual(self.db.list_jobs(source_script_record_id="rec_2x2"), [])
        with self.assertRaisesRegex(ValueError, "尚未确认"):
            create_confirmed_video_jobs(self.db, plans[0]["visual_plan_id"])

    def test_confirmed_visual_plan_five_jobs_share_one_outfit_image(self) -> None:
        plan = orchestrate_visual_plans(
            self.db,
            self.product.product_id,
            source_record_id="rec_five",
            scene_values=["现代简约卧室"],
            styling_values=["日常上衣加直筒牛仔裤"],
            per_plan_video_count=5,
        )[0]
        image = self.root / "outfit.jpg"
        image.write_bytes(b"outfit")
        confirm_outfit_image(self.db, plan["visual_plan_id"], image_path=str(image))
        result = create_confirmed_video_jobs(self.db, plan["visual_plan_id"])
        self.assertEqual(result["created"], 5)
        jobs = [self.db.get_job(job_id) for job_id in result["job_ids"]]
        self.assertEqual({job["outfit_image_path"] for job in jobs}, {str(image)})
        self.assertEqual({job["visual_plan_id"] for job in jobs}, {plan["visual_plan_id"]})
        self.assertEqual(
            [job["shot_profile_id"] for job in jobs],
            ["SHOT_FULL_FIXED", "SHOT_UPPER_FIXED", "SHOT_UPPER_THREE_QUARTER", "SHOT_UPPER_FIXED", "SHOT_UPPER_PUSH_IN"],
        )
        for job in jobs:
            self.assertEqual(job["prompt_payload"]["reference_images"], [str(image)])
            self.assertEqual(job["prompt_payload"]["reference_roles"][0]["role"], "confirmed_outfit_truth")

    def test_visual_plan_one_to_five_adds_four_without_deleting_history(self) -> None:
        kwargs = {
            "source_record_id": "rec_expand",
            "scene_values": ["现代简约卧室"],
            "styling_values": ["日常上衣加直筒牛仔裤"],
        }
        plan = orchestrate_visual_plans(self.db, self.product.product_id, per_plan_video_count=1, **kwargs)[0]
        image = self.root / "expand.jpg"
        image.write_bytes(b"outfit")
        confirm_outfit_image(self.db, plan["visual_plan_id"], image_path=str(image))
        self.assertEqual(create_confirmed_video_jobs(self.db, plan["visual_plan_id"])["created"], 1)
        expanded = orchestrate_visual_plans(self.db, self.product.product_id, per_plan_video_count=5, **kwargs)[0]
        self.assertEqual(expanded["visual_plan_id"], plan["visual_plan_id"])
        self.assertEqual(create_confirmed_video_jobs(self.db, plan["visual_plan_id"])["created"], 4)
        reduced = orchestrate_visual_plans(self.db, self.product.product_id, per_plan_video_count=1, **kwargs)[0]
        self.assertEqual(create_confirmed_video_jobs(self.db, reduced["visual_plan_id"])["created"], 0)
        self.assertEqual(len(self.db.list_jobs(source_script_record_id="rec_expand")), 5)

    def test_visual_plan_combination_limit_and_auto_conflict(self) -> None:
        with self.assertRaisesRegex(ValueError, "超过上限 6"):
            orchestrate_visual_plans(
                self.db,
                self.product.product_id,
                source_record_id="rec_too_many",
                scene_values=["现代简约卧室", "明亮现代咖啡店"],
                styling_values=[
                    "修身上衣加高腰阔腿裤", "日常上衣加直筒牛仔裤",
                    "吊带或背心加白色短裤", "宽松上衣加休闲短裤",
                ],
                per_plan_video_count=1,
            )
        with self.assertRaisesRegex(ValueError, "不能和具体模板同时选择"):
            orchestrate_visual_plans(
                self.db,
                self.product.product_id,
                source_record_id="rec_auto_conflict",
                scene_values=["自动选择", "现代简约卧室"],
                styling_values=["自动选择"],
                per_plan_video_count=1,
            )

    def test_outfit_request_contains_scene_reference_role(self) -> None:
        scene = self.db.get_template("scene", "SCENE_A_001")
        scene["reference_images"] = ["/tmp/scene-reference.jpg"]
        self.db.upsert_template("scene", scene)
        plan = orchestrate_visual_plans(
            self.db,
            self.product.product_id,
            source_record_id="rec_scene_ref",
            scene_values=["现代简约卧室"],
            styling_values=["日常上衣加直筒牛仔裤"],
            per_plan_video_count=1,
            allow_scene_text_fallback=False,
        )[0]
        request = plan["outfit_request_payload"]
        self.assertEqual(request["scene_reference_mode"], "image")
        self.assertIn("scene_truth", {item["role"] for item in request["references"]})

    def test_scene_reference_is_optional_and_uses_text_fallback(self) -> None:
        scene = self.db.get_template("scene", "SCENE_A_001")
        scene["reference_images"] = []
        self.db.upsert_template("scene", scene)
        plan = orchestrate_visual_plans(
            self.db,
            self.product.product_id,
            source_record_id="rec_scene_text_only",
            scene_values=["现代简约卧室"],
            styling_values=["日常上衣加直筒牛仔裤"],
            per_plan_video_count=1,
            allow_scene_text_fallback=False,
        )[0]
        request = plan["outfit_request_payload"]
        self.assertEqual(request["scene_reference_mode"], "text_fallback")
        self.assertNotIn("scene_truth", {item["role"] for item in request["references"]})
        self.assertIn("场景必须遵循", request["prompt"])

    def test_bottom_color_is_automatically_selected_and_stable_per_visual_plan(self) -> None:
        kwargs = {
            "source_record_id": "rec_random_bottom",
            "scene_values": ["现代简约卧室"],
            "styling_values": ["修身上衣加高腰阔腿裤"],
            "per_plan_video_count": 5,
        }
        first = orchestrate_visual_plans(self.db, self.product.product_id, **kwargs)[0]
        second = orchestrate_visual_plans(self.db, self.product.product_id, **kwargs)[0]
        styling = self.db.get_template("styling", "STYLE_001")

        self.assertIn(first["resolved_bottom_color"], styling["bottom_color"])
        self.assertEqual(first["resolved_bottom_color"], second["resolved_bottom_color"])
        self.assertEqual(first["resolved_bottom_fit"], "高腰、阔腿")
        self.assertEqual(first["visual_plan_id"], second["visual_plan_id"])
        request = first["outfit_request_payload"]
        self.assertEqual(request["resolved_styling"]["bottom_color"], first["resolved_bottom_color"])
        self.assertEqual(request["resolved_styling"]["bottom_fit"], ["高腰", "阔腿"])
        self.assertIn(f"下装颜色已由系统确定为“{first['resolved_bottom_color']}”", request["prompt"])

    def test_empty_bottom_color_pool_uses_type_default_without_operator_input(self) -> None:
        styling = self.db.get_template("styling", "STYLE_002")
        styling["bottom_color"] = []
        self.db.upsert_template("styling", styling)
        plan = orchestrate_visual_plans(
            self.db,
            self.product.product_id,
            source_record_id="rec_default_jeans_color",
            scene_values=["现代简约卧室"],
            styling_values=["日常上衣加直筒牛仔裤"],
            per_plan_video_count=1,
        )[0]
        self.assertEqual(plan["resolved_bottom_color"], "蓝色牛仔")
        self.assertEqual(plan["outfit_request_payload"]["resolved_styling"]["bottom_color"], "蓝色牛仔")
        self.assertEqual(plan["outfit_request_payload"]["resolved_styling"]["bottom_type"], "直筒牛仔裤")

    def test_feishu_structured_scene_fields_override_stale_prompt_core(self) -> None:
        scene = self.db.get_template("scene", "SCENE_A_001")
        scene.update({
            "floor_type": "浅米哑光砖",
            "bed_sheet_color": "米白",
            "curtain_position": "左后方",
            "curtain_color": "深灰",
            "shelf_position": "不出现",
            "prompt_core": "旧描述：浅米色光滑瓷砖，右后方窗帘，右侧后方有置物架。",
            "source_payload": {
                "scene_id": "SCENE_A_001",
                "room_type": "bedroom",
                "floor_type": "浅米哑光砖",
                "bed_sheet_color": "米白",
                "curtain_position": "左后方",
                "curtain_color": "深灰",
                "shelf_position": "不出现",
                "lighting_level": "bright_not_overexposed",
            },
        })
        self.db.upsert_template("scene", scene)
        plan = orchestrate_visual_plans(
            self.db,
            self.product.product_id,
            source_record_id="rec_structured_scene",
            scene_values=["现代简约卧室"],
            styling_values=["日常上衣加直筒牛仔裤"],
            per_plan_video_count=1,
        )[0]
        prompt = plan["outfit_request_payload"]["prompt"]
        self.assertIn("地面材质：浅米哑光砖", prompt)
        self.assertIn("床品颜色：米白", prompt)
        self.assertIn("窗帘位置：左后方", prompt)
        self.assertIn("置物架位置：不出现", prompt)
        self.assertNotIn("右侧后方有置物架", prompt)

    def test_indoor_cream_scene_pool_resolves_one_stable_background_decor_and_light(self) -> None:
        scene = self.db.get_template("scene", "SCENE_A_001")
        scene.update({
            "scene_style": "INS奶油风",
            "background_type_pool": ["极简暖白墙", "垂直百褶帘", "浅原木纹板"],
            "background_cleanliness": "极简",
            "edge_decor_pool": ["琴叶榕", "龟背竹", "极简落地灯"],
            "decor_count": "1件",
            "decor_position": "系统自动边缘",
            "key_light_direction": "系统左右轮换",
            "lighting_style": "oblique_soft_with_fill",
            "lighting_tone": "neutral_warm_no_yellow",
            "prompt_core": "旧卧室描述：右后方置物架和床必须出现。",
        })
        self.db.upsert_template("scene", scene)
        first = orchestrate_visual_plans(
            self.db, self.product.product_id, source_record_id="rec_cream_scene",
            scene_values=["现代简约卧室"], styling_values=["日常上衣加直筒牛仔裤"],
            per_plan_video_count=1,
        )[0]
        second = orchestrate_visual_plans(
            self.db, self.product.product_id, source_record_id="rec_cream_scene",
            scene_values=["现代简约卧室"], styling_values=["日常上衣加直筒牛仔裤"],
            per_plan_video_count=1,
        )[0]
        self.assertEqual(first["visual_plan_id"], second["visual_plan_id"])
        self.assertIn(first["resolved_background_type"], scene["background_type_pool"])
        self.assertTrue(any(item in first["resolved_edge_decor"] for item in scene["edge_decor_pool"]))
        self.assertIn(first["resolved_key_light_direction"], {"左前方45°", "右前方45°"})
        request = first["outfit_request_payload"]
        self.assertIn(first["resolved_background_type"], request["prompt"])
        self.assertIn(first["resolved_edge_decor"], request["prompt"])
        self.assertIn(first["resolved_key_light_direction"], request["prompt"])
        self.assertIn("浅暖白、明亮通透但不偏黄", request["resolved_context"]["scene_display"])
        self.assertNotIn("右后方置物架和床必须出现", request["prompt"])
        image = self.root / "cream-scene.png"
        image.write_bytes(b"image")
        confirm_outfit_image(self.db, first["visual_plan_id"], image_path=str(image))
        result = create_confirmed_video_jobs(self.db, first["visual_plan_id"])
        payload = self.db.get_job(result["job_ids"][0])["prompt_payload"]
        self.assertIn(first["resolved_background_type"], payload["positive_prompt"])
        self.assertIn(first["resolved_edge_decor"], payload["positive_prompt"])
        self.assertIn("不偏黄", payload["positive_prompt"])
        self.assertIn("主背景、边缘装饰及其位置、主光方向全程锁定首帧", payload["positive_prompt"])
        self.assertIn("不得新增、移除或移动家具装饰", payload["positive_prompt"])
        self.assertIn("暖黄偏色", payload["negative_prompt"])
        self.assertTrue(payload["qc_expectations"]["no_yellow_cast"])

    def test_not_required_footwear_means_shoes_must_stay_out_of_frame(self) -> None:
        styling = self.db.get_template("styling", "STYLE_001")
        styling["footwear_visibility"] = "not_required"
        self.db.upsert_template("styling", styling)
        plan = orchestrate_visual_plans(
            self.db,
            self.product.product_id,
            source_record_id="rec_no_shoes",
            scene_values=["现代简约卧室"],
            styling_values=["修身上衣加高腰阔腿裤"],
            per_plan_video_count=1,
        )[0]
        request = plan["outfit_request_payload"]
        self.assertEqual(request["framing"], "upper_body_focus_with_partial_bottom")
        self.assertIn("不展示完整裤腿、脚部或鞋子", request["prompt"])
        self.assertIn("upper_garment_dominant_partial_bottom_visible", request["qc_requirements"])
        self.assertNotIn("full_body_visible", request["qc_requirements"])

    def test_resolved_bottom_color_replaces_conflicting_styling_prose(self) -> None:
        styling = self.db.get_template("styling", "STYLE_002")
        styling["bottom_color"] = ["黑色"]
        styling["prompt_core"] = "商品上衣搭配经典蓝色直筒牛仔裤。"
        styling["source_payload"] = {"styling_id": "STYLE_002", "bottom_color": ["黑色"]}
        self.db.upsert_template("styling", styling)
        plan = orchestrate_visual_plans(
            self.db,
            self.product.product_id,
            source_record_id="rec_black_jeans",
            scene_values=["现代简约卧室"],
            styling_values=["日常上衣加直筒牛仔裤"],
            per_plan_video_count=1,
        )[0]
        prompt = plan["outfit_request_payload"]["prompt"]
        self.assertIn("黑色直筒牛仔裤", prompt)
        self.assertNotIn("经典蓝色", prompt)

    def test_upper_garment_outfit_image_focuses_upper_body_and_only_partial_bottom(self) -> None:
        plan = orchestrate_visual_plans(
            self.db,
            self.product.product_id,
            source_record_id="rec_upper_focus",
            scene_values=["现代简约卧室"],
            styling_values=["日常上衣加直筒牛仔裤"],
            per_plan_video_count=1,
        )[0]
        request = plan["outfit_request_payload"]
        self.assertEqual(request["framing"], "upper_body_focus_with_partial_bottom")
        self.assertIn("上装为绝对视觉主体", request["prompt"])
        self.assertIn("下装只露出腰头、胯部和大腿上半截", request["prompt"])
        self.assertIn("upper_garment_dominant_partial_bottom_visible", request["qc_requirements"])

    def test_outerwear_inner_layer_is_automatic_and_video_prompt_uses_resolved_snapshot(self) -> None:
        product_id = "SKU-CREAM-OUTER-INNER"
        self.db.upsert_product(ProductInput.from_dict({
            "product_id": product_id,
            "product_name": "奶油白短外套",
            "product_title": "奶油白短外套",
            "market": "TH",
            "language": "th",
            "category": "outerwear",
            "product_images": ["https://example.com/outer.jpg"],
            "target_publish_count": 1,
        }))
        scene = self.db.get_template("scene", "SCENE_A_001")
        scene["source_payload"] = {
            "scene_id": "SCENE_A_001", "floor_type": "浅米哑光砖", "shelf_position": "不出现",
        }
        self.db.upsert_template("scene", scene)
        plan = orchestrate_visual_plans(
            self.db,
            product_id,
            source_record_id="rec_outer_inner",
            scene_values=["现代简约卧室"],
            styling_values=["日常上衣加直筒牛仔裤"],
            per_plan_video_count=1,
        )[0]
        self.assertEqual(plan["resolved_inner_type"], "简洁修身纯色圆领打底衫")
        self.assertIn(plan["resolved_inner_color"], {"黑色", "暖灰色"})
        self.assertIn("不得拉开、拉合", plan["resolved_outerwear_state"])
        request = plan["outfit_request_payload"]
        self.assertIn(plan["resolved_inner_color"], request["resolved_context"]["inner_layer"])
        image = self.root / "outer-inner.png"
        image.write_bytes(b"image")
        confirm_outfit_image(self.db, plan["visual_plan_id"], image_path=str(image))
        result = create_confirmed_video_jobs(self.db, plan["visual_plan_id"])
        payload = self.db.get_job(result["job_ids"][0])["prompt_payload"]
        self.assertIn("首屏：开场0-1秒", payload["positive_prompt"])
        self.assertIn(plan["resolved_inner_color"], payload["positive_prompt"])
        self.assertIn("外套及内搭全程锁定首帧状态", payload["positive_prompt"])
        self.assertIn("不得改变外套开合、内搭款式、领口、颜色或露出面积", payload["positive_prompt"])
        self.assertIn("浅米哑光砖", payload["positive_prompt"])
        self.assertIn("不出现置物架", payload["positive_prompt"])
        self.assertNotIn("经典蓝色", payload["positive_prompt"])

    def test_outerwear_inner_layer_can_be_overridden_by_styling_template(self) -> None:
        product_id = "SKU-BLACK-OUTER-CUSTOM-INNER"
        self.db.upsert_product(ProductInput.from_dict({
            "product_id": product_id,
            "product_name": "黑色短外套",
            "market": "TH",
            "language": "th",
            "category": "outerwear",
            "product_images": ["https://example.com/black-outer.jpg"],
            "target_publish_count": 1,
        }))
        styling = self.db.get_template("styling", "STYLE_002")
        styling.update({
            "inner_type": "修身方领针织背心",
            "inner_color": "奶油白",
            "inner_requirements": "领口必须低于外套领口，不增加蕾丝和图案",
        })
        self.db.upsert_template("styling", styling)
        plan = orchestrate_visual_plans(
            self.db,
            product_id,
            source_record_id="rec_custom_inner",
            scene_values=["现代简约卧室"],
            styling_values=["日常上衣加直筒牛仔裤"],
            per_plan_video_count=1,
        )[0]
        self.assertEqual(plan["resolved_inner_type"], "修身方领针织背心")
        self.assertEqual(plan["resolved_inner_color"], "奶油白")
        request = plan["outfit_request_payload"]
        self.assertEqual(request["resolved_styling"]["inner_requirements"], "领口必须低于外套领口，不增加蕾丝和图案")
        self.assertIn("奶油白修身方领针织背心", request["prompt"])
        self.assertIn("内搭补充要求：领口必须低于外套领口，不增加蕾丝和图案", request["prompt"])

    def test_outfit_worker_auto_confirms_and_releases_video_jobs(self) -> None:
        plan = orchestrate_visual_plans(
            self.db,
            self.product.product_id,
            source_record_id="rec_auto_release",
            scene_values=["现代简约卧室"],
            styling_values=["日常上衣加直筒牛仔裤"],
            per_plan_video_count=5,
        )[0]
        output_image = self.root / "auto-outfit.png"
        output_image.write_bytes(b"fake image")
        worker = self.root / "outfit_worker.py"
        worker.write_text(
            "import json,sys\n"
            "json.load(sys.stdin)\n"
            f"print(json.dumps({{'output_image_path': {str(output_image)!r}, 'image_version': 'auto-v1'}}))\n",
            encoding="utf-8",
        )
        from light_tryon.visual_plans import run_outfit_generation_worker

        result = run_outfit_generation_worker(
            self.db,
            f"{sys.executable} {worker}",
            visual_plan_ids=[plan["visual_plan_id"]],
        )
        self.assertEqual(result["auto_confirmed"], 1)
        self.assertEqual(result["created_jobs"], 5)
        updated = self.db.get_visual_plan(plan["visual_plan_id"])
        self.assertEqual(updated["outfit_image_status"], "confirmed")
        self.assertEqual(len(updated["job_ids"]), 5)


if __name__ == "__main__":
    unittest.main()
