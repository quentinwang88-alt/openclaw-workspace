from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch


SKILL_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = SKILL_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from light_tryon.asset_ingestion import backfill_generated_job_assets, process_pending_asset_tags, register_media_asset  # noqa: E402
from light_tryon.assembly_planner import plan_variant_rough_cut, plan_variant_voiceover_cut  # noqa: E402
from light_tryon.content_strategy import build_strategy_pool, sample_execution_variants  # noqa: E402
from light_tryon.database import LightTryonDB  # noqa: E402
from light_tryon.diversity import assess_product_asset_capacity, evaluate_product_diversity  # noqa: E402
from light_tryon.models import ProductInput  # noqa: E402
from light_tryon.narrative_render import render_narrative_voiceover_mix  # noqa: E402
from light_tryon.supplement_shots import compile_supplement_prompt, plan_supplement_shots  # noqa: E402
from light_tryon.tts_bridge import synthesize_variant_tts  # noqa: E402
from light_tryon.voiceover_adapter import (  # noqa: E402
    build_tts_timeline,
    build_voiceover_request,
    normalize_voiceover_response,
)
from light_tryon.voiceover_engine_bridge import (  # noqa: E402
    _ensure_verified_claims,
    _load_engine,
    load_active_voiceover_hooks,
    run_voiceover_engine_variant,
)


class NarrativeEnhancementTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = LightTryonDB(self.root / "test.sqlite3")
        self.db.init_schema()
        self.db.seed_templates(SKILL_DIR / "assets" / "default_templates.json")
        self.product = ProductInput.from_dict({
            "product_id": "SKU-NARRATIVE-001",
            "product_name": "浅色日常外套",
            "market": "TH",
            "language": "th",
            "category": "outerwear",
            "core_selling_points": ["显瘦版型", "袖口细节", "日常通勤", "多色可选"],
        })
        self.db.upsert_product(self.product)

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def hooks() -> list[dict]:
        return [
            {"hook_id": "HOOK_PAIN", "hook_name": "痛点钩子", "hook_type": "pain", "priority": 3},
            {"hook_id": "HOOK_RESULT", "hook_name": "结果钩子", "hook_type": "result", "priority": 2},
            {"hook_id": "HOOK_DETAIL", "hook_name": "细节钩子", "hook_type": "detail", "priority": 1},
        ]

    def test_asset_backfill_does_not_treat_postprocessed_output_as_clean_initial_video(self) -> None:
        final_path = self.root / "captioned-final.mp4"
        final_path.write_bytes(b"postprocessed")

        class FakeDB:
            @staticmethod
            def list_jobs(product_id=None):
                return [{
                    "job_id": "JOB_FINAL_ONLY",
                    "product_id": "SKU-NARRATIVE-001",
                    "raw_video_path": "",
                    "output_video_path": str(final_path),
                }]

        result = backfill_generated_job_assets(FakeDB())

        self.assertEqual(result["processed"], 0)
        self.assertEqual(result["skipped"], 1)
        self.assertEqual(result["items"][0]["reason"], "clean_initial_video_missing")

    def test_weighted_sampling_allows_repeated_hooks_and_selling_points(self) -> None:
        strategies = build_strategy_pool(
            self.product.product_id,
            self.hooks(),
            self.product.core_selling_points,
            available_evidence=["主穿搭图"],
        )
        variants = sample_execution_variants(
            strategies, 50, product_id=self.product.product_id, random_seed="fixed-test-seed",
        )
        strategy_ids = [row.strategy_group_id for row in variants]
        usage = Counter(strategy_ids)
        self.assertEqual(len(variants), 50)
        self.assertGreater(max(usage.values()), 1)
        used = {row.strategy_group_id for row in variants}
        used_hooks = {row.hook_id for row in strategies if row.strategy_group_id in used}
        used_points = {row.primary_selling_point for row in strategies if row.strategy_group_id in used}
        self.assertEqual(used_hooks, {"HOOK_PAIN", "HOOK_RESULT", "HOOK_DETAIL"})
        self.assertEqual(used_points, set(self.product.core_selling_points))
        self.assertEqual(len({row.variant_id for row in variants}), 50)

    def test_sampling_uses_every_unique_strategy_before_exact_repeat(self) -> None:
        strategies = build_strategy_pool(
            self.product.product_id,
            self.hooks(),
            self.product.core_selling_points,
        )
        variants = sample_execution_variants(
            strategies,
            len(strategies),
            product_id=self.product.product_id,
            random_seed="without-replacement-first",
        )
        self.assertEqual(len(strategies), len({row.strategy_group_id for row in variants}))

    def test_product_history_promotes_an_unused_hook_angle(self) -> None:
        hooks = [
            {"hook_id": "HOOK_USED", "hook_name": "已用", "priority": 1},
            {"hook_id": "HOOK_NEW", "hook_name": "未用", "priority": 1},
        ]
        strategies = build_strategy_pool(self.product.product_id, hooks, ["显瘦版型"])
        variants = sample_execution_variants(
            strategies,
            1,
            product_id=self.product.product_id,
            random_seed="history-aware",
            historical_usage={
                "hook_counts": {"HOOK_USED": 8},
                "selling_point_counts": {},
                "pair_counts": {},
                "recent_count": 8,
            },
        )
        selected = next(
            row for row in strategies if row.strategy_group_id == variants[0].strategy_group_id
        )
        self.assertEqual("HOOK_NEW", selected.hook_id)

    def test_hook_fact_prerequisite_filters_invalid_combinations_before_sampling(self) -> None:
        hooks = [
            {
                "hook_id": "BINARY_COMPARISON",
                "hook_name": "双项对比型",
                "allowed_visual_focuses": ["color"],
            },
            {"hook_id": "GENERAL_PRODUCT_SHARE", "hook_name": "通用轻分享型"},
        ]
        strategies = build_strategy_pool(
            self.product.product_id,
            hooks,
            ["显瘦版型", "多色可选"],
        )
        binary_points = {
            row.primary_selling_point for row in strategies if row.hook_id == "BINARY_COMPARISON"
        }
        self.assertEqual(binary_points, {"多色可选"})

    def test_strategy_and_variant_storage_is_idempotent(self) -> None:
        strategies = build_strategy_pool(self.product.product_id, self.hooks(), self.product.core_selling_points)
        for strategy in strategies:
            self.db.upsert_content_strategy(strategy)
        variants = sample_execution_variants(
            strategies, 20, product_id=self.product.product_id, random_seed="same", plan_version="plan-a",
        )
        first = self.db.create_narrative_variants(variants)
        second = self.db.create_narrative_variants(variants)
        self.assertEqual(first, {"created": 20, "existing": 0})
        self.assertEqual(second, {"created": 0, "existing": 20})
        self.assertEqual(len(self.db.list_narrative_variants(self.product.product_id)), 20)

    def test_production_batch_is_idempotent_and_increments_for_new_plan(self) -> None:
        first = self.db.get_or_create_content_batch(
            self.product.product_id,
            "narrative:plan-a",
            canonical_product_id="EXT-PRODUCT",
        )
        retry = self.db.get_or_create_content_batch(
            self.product.product_id,
            "narrative:plan-a",
            canonical_product_id="EXT-PRODUCT",
        )
        second = self.db.get_or_create_content_batch(
            self.product.product_id,
            "narrative:plan-b",
            canonical_product_id="EXT-PRODUCT",
        )
        self.assertEqual(first["batch_id"], retry["batch_id"])
        self.assertEqual(1, first["batch_no"])
        self.assertEqual(2, second["batch_no"])
        self.assertNotEqual(first["batch_id"], second["batch_id"])

    def test_voiceover_adapter_does_not_rewrite_existing_copy(self) -> None:
        strategy = build_strategy_pool(self.product.product_id, self.hooks(), ["显瘦版型"])[0].to_dict()
        variant = {
            "variant_id": "NAR_001", "target_duration_seconds": 22,
        }
        request = build_voiceover_request(self.product.to_dict(), strategy, variant)
        self.assertEqual(request["policy"]["generator"], "existing_voiceover_flow_only")
        self.assertFalse(request["policy"]["allow_downstream_rewrite"])
        original = "ประโยคแรกดีมาก! ประโยคที่สองยังเป็นสไตล์เดิม。"
        response = normalize_voiceover_response({"voiceover_text": original})
        self.assertEqual(response["voiceover_text"], original)
        self.assertEqual(
            "".join("".join(row["speech_text"].split()) for row in response["beats"]),
            "".join(original.split()),
        )
        self.assertFalse(response["downstream_rewritten"])

    def test_existing_voiceover_engine_bridge_uses_governed_hook_and_22s_profile(self) -> None:
        voiceover_root = Path.home() / "voiceover_copy_engine"
        if not (voiceover_root / "voiceover_copy_engine").is_dir():
            self.skipTest("existing voiceover engine is not installed")
        hooks = load_active_voiceover_hooks(voiceover_root, db_path=self.root / "voiceover.sqlite")
        hook = next(row for row in hooks if row["hook_id"] == "GENERAL_PRODUCT_SHARE")
        self.assertTrue(hook["core_intent"])
        self.assertTrue(hook["minimal_structure"])
        self.assertTrue(hook["attention_mechanisms"])
        strategy = {
            "strategy_group_id": "STR_BRIDGE",
            **hook,
            "primary_selling_point": "显瘦版型",
            "secondary_selling_points": ["日常通勤", "多色可选"],
            "visual_focus": "fit",
        }
        variant = {
            "variant_id": "NAR_BRIDGE_001",
            "target_duration_seconds": 22,
        }
        timeline = {
            "duration_ms": 22000,
            "mainline_summary": "外套正面、侧面和完整上身效果",
            "overall_confidence": 0.92,
            "visual_slots": [
                {
                    "start_ms": 0,
                    "end_ms": 4000,
                    "visual_event": "正面展示外套整体版型",
                    "observations": ["上衣主体清楚", "轮廓清楚"],
                    "event_tags": ["tryon_front", "full_body"],
                    "speakable_facts": ["整体版型"],
                    "recommended_line_function": "hook",
                    "product_visibility": "high",
                    "confidence": 0.93,
                },
                {
                    "start_ms": 4000,
                    "end_ms": 8000,
                    "visual_event": "日常室内场景展示完整穿搭",
                    "observations": ["日常穿搭清楚"],
                    "event_tags": ["lifestyle"],
                    "speakable_facts": ["日常穿搭"],
                    "recommended_line_function": "proof",
                    "product_visibility": "high",
                    "confidence": 0.9,
                },
                {
                    "start_ms": 8000,
                    "end_ms": 12000,
                    "visual_event": "并列展示不同颜色",
                    "observations": ["不同颜色清楚"],
                    "event_tags": ["color_comparison"],
                    "speakable_facts": ["多色选择"],
                    "recommended_line_function": "proof",
                    "product_visibility": "high",
                    "confidence": 0.91,
                },
                {
                    "start_ms": 12000,
                    "end_ms": 16000,
                    "visual_event": "轻侧身展示外套轮廓",
                    "observations": ["侧面轮廓可见"],
                    "event_tags": ["side_view"],
                    "speakable_facts": ["版型轮廓"],
                    "recommended_line_function": "proof",
                    "product_visibility": "high",
                    "confidence": 0.9,
                },
                {
                    "start_ms": 16000,
                    "end_ms": 19250,
                    "visual_event": "回到完整穿搭准备收尾",
                    "observations": ["完整上身效果"],
                    "event_tags": ["styled_look"],
                    "speakable_facts": ["上身效果"],
                    "recommended_line_function": "result_and_cta",
                    "product_visibility": "high",
                    "confidence": 0.91,
                },
                {
                    "start_ms": 19250,
                    "end_ms": 22000,
                    "visual_event": "细节画面自然定格",
                    "observations": ["商品细节清楚"],
                    "event_tags": ["detail_macro"],
                    "speakable_facts": ["商品细节"],
                    "recommended_line_function": "ending",
                    "product_visibility": "high",
                    "confidence": 0.91,
                },
            ],
        }
        response = run_voiceover_engine_variant(
            self.product.to_dict(),
            strategy,
            variant,
            timeline,
            root=voiceover_root,
            db_path=self.root / "voiceover.sqlite",
        )
        self.assertEqual(response["status"], "READY_FOR_TTS")
        self.assertTrue(response["voiceover_text"])
        self.assertTrue(response["beats"])
        # Three supported facts can form hook + one connected proof + closing;
        # beat count is not the same thing as claim count after bundle grouping.
        self.assertEqual(len(response["beats"]), 3)
        self.assertEqual(response["selected_claim_count"], 3)
        # The closing line is now bound to the final, actual visual slot rather
        # than being spoken before that detail enters frame.
        self.assertGreaterEqual(response["beats"][-1]["suggested_start_ms"], 19250)
        self.assertRegex(response["beats"][-1]["visual_refs"][0], r"^LTVS_[0-9a-f]{10}_[0-9]{2}$")
        self.assertFalse(response["downstream_rewritten"])

    def test_existing_product_can_gain_new_exact_alias_claim_without_free_writing(self) -> None:
        voiceover_root = Path.home() / "voiceover_copy_engine"
        if not (voiceover_root / "voiceover_copy_engine").is_dir():
            self.skipTest("existing voiceover engine is not installed")
        engine = _load_engine(voiceover_root, db_path=self.root / "voiceover-alias.sqlite")
        claim_ids = _ensure_verified_claims(
            engine,
            self.product.product_id,
            ["这款皮衣整体为柔和的米杏纯白色调"],
            "NAR_ALIAS_TEST",
        )
        keys = {
            (engine.repo.get("claim_concepts", "concept_id", (engine.repo.get("product_claims", "claim_id", claim_id) or {}).get("concept_id")) or {}).get("canonical_key")
            for claim_id in claim_ids
        }
        self.assertIn("soft_ivory_color", keys)
        repeated = _ensure_verified_claims(
            engine,
            self.product.product_id,
            ["这款皮衣整体为柔和的米杏纯白色调"],
            "NAR_ALIAS_TEST_REPEAT",
        )
        self.assertEqual(claim_ids, repeated)

    def test_tts_timeline_uses_actual_beat_durations(self) -> None:
        beats = [
            {"beat_id": "B1", "speech_text": "a"},
            {"beat_id": "B2", "speech_text": "b"},
        ]
        timeline = build_tts_timeline(beats, [1.25, 2.5], pause_seconds=0.2)
        self.assertEqual(timeline[0]["start_seconds"], 0.0)
        self.assertEqual(timeline[0]["end_seconds"], 1.25)
        self.assertEqual(timeline[1]["start_seconds"], 1.45)
        self.assertEqual(timeline[1]["end_seconds"], 3.95)

    def test_supplement_planner_caps_generation_and_builds_self_contained_prompt(self) -> None:
        beats = [
            {"beat_id": "B1", "role": "hook", "speech_text": "hook", "priority": "required"},
            {"beat_id": "B2", "role": "detail", "speech_text": "detail", "required_shot_roles": ["detail_sleeve"], "priority": "required"},
            {"beat_id": "B3", "role": "color", "speech_text": "color", "priority": "required"},
        ]
        planned = plan_supplement_shots(
            "NAR_001", beats, [], reference_assets=["/tmp/outfit.jpg"], max_generated_shots=2,
        )
        self.assertEqual(len(planned), 2)
        context = {
            "product": self.product.to_dict(),
            "scene": {"prompt_core": "明亮室内奶油风空间，浅暖白墙面和垂直百褶帘，画面不偏黄。"},
            "persona": {"prompt_core": "一名年轻成年女生，黑色长发，浅色手机完整遮脸。"},
            "styling": {"prompt_core": "外套搭配白色圆领内搭和高腰直筒裤"},
            "visual_plan": {"resolved_inner_type": "圆领内搭", "resolved_inner_color": "白色"},
        }
        prompt = compile_supplement_prompt(planned[0], context)
        self.assertIn("动作只进行一次", prompt["positive_prompt"])
        self.assertIn("品牌信息只由后期添加", prompt["positive_prompt"])
        self.assertIn("固定镜面试穿机位", prompt["positive_prompt"])
        for phrase in ("与主场景", "主场景相同", "同一套", "同一个女生", "原展示区"):
            self.assertNotIn(phrase, prompt["positive_prompt"])

    def test_supplement_prompt_focuses_points_and_fills_eight_second_timeline(self) -> None:
        shot = {
            "shot_id": "SUP_TEST",
            "shot_role": "detail_sleeve",
            "duration_seconds": 8,
            "reference_assets": ["/tmp/outfit.jpg", "/tmp/product.jpg"],
        }
        context = {
            "product": {
                "product_name": "短款机车外套",
                "core_selling_points": [
                    "米杏纯白色调", "短款立领版型", "金属拉链开合", "袖口铆钉收口",
                    "细腻哑光面料", "宽松不紧绷", "适配多种内搭",
                ],
            },
            "scene": {"prompt_core": "明亮室内奶油风空间，浅暖白墙面，画面不偏黄。"},
            "persona": {"prompt_core": "一名成年女生，浅色手机完整遮脸。"},
            "styling": {"prompt_core": "高腰阔腿裤，颜色选黑色、深灰或浅米色之一"},
            "visual_plan": {
                "resolved_inner_type": "纯色短袖或吊带",
                "resolved_inner_color": "白色",
                "resolved_outerwear_state": "保持首帧开合状态",
            },
        }
        prompt = compile_supplement_prompt(shot, context)["positive_prompt"]
        self.assertIn("6-8秒保持最终稳定姿态", prompt)
        self.assertIn("袖口铆钉收口", prompt)
        self.assertNotIn("金属拉链开合", prompt)
        self.assertIn("内搭类型、颜色、领口和露出面积严格跟随产品穿搭图", prompt)
        self.assertIn("下装颜色、版型和配饰严格跟随第1张产品穿搭图", prompt)
        self.assertNotIn("颜色选黑色、深灰或浅米色之一", prompt)

    def test_asset_registration_and_tagging_are_hash_idempotent(self) -> None:
        video = self.root / "generated.mp4"
        video.write_bytes(b"fake generated video")
        one, created_one = register_media_asset(
            self.db, self.product.product_id, video, expected_tags={"shot_roles": ["fit_turn"]},
        )
        two, created_two = register_media_asset(
            self.db, self.product.product_id, video, expected_tags={"shot_roles": ["fit_turn"]},
        )
        self.assertTrue(created_one)
        self.assertFalse(created_two)
        self.assertEqual(one["asset_id"], two["asset_id"])

        worker = self.root / "tag_worker.py"
        worker.write_text(
            "import json,sys\n"
            "payload=json.load(sys.stdin)\n"
            "print(json.dumps({'status':'ready','observed_tags':{'shot_roles':['fit_turn']},"
            "'qc_result':{'decision':'ready'},'tagger_version':'fake-v1'}))\n",
            encoding="utf-8",
        )
        result = process_pending_asset_tags(
            self.db,
            product_id=self.product.product_id,
            tag_command=f"{sys.executable} {worker}",
        )
        self.assertEqual(result["ready"], 1)
        stored = self.db.get_media_asset(one["asset_id"])
        self.assertEqual(stored["tag_status"], "completed")
        self.assertEqual(stored["observed_tags"]["shot_roles"], ["fit_turn"])

    def test_off_target_supplement_is_routed_to_manual_review(self) -> None:
        video = self.root / "off-target-supplement.mp4"
        video.write_bytes(b"off-target-supplement")
        asset, _ = register_media_asset(
            self.db,
            self.product.product_id,
            video,
            source_job_id="SUP_OFF_TARGET",
            expected_tags={"shot_roles": ["detail_neckline"]},
        )
        worker = self.root / "off_target_tag_worker.py"
        worker.write_text(
            "import json,sys\n"
            "json.load(sys.stdin)\n"
            "print(json.dumps({'status':'ready','observed_tags':{'shot_roles':['hero','result'],"
            "'segments':[{'primary_shot_role':'hero','secondary_roles':['result']}]},"
            "'qc_result':{'decision':'ready'},'tagger_version':'fake-v1'}))\n",
            encoding="utf-8",
        )
        result = process_pending_asset_tags(
            self.db,
            product_id=self.product.product_id,
            tag_command=f"{sys.executable} {worker}",
        )
        self.assertEqual(result["manual_review"], 1)
        stored = self.db.get_media_asset(asset["asset_id"])
        self.assertEqual(stored["asset_status"], "manual_review")
        self.assertEqual(stored["tag_status"], "completed")
        contract = stored["qc_result"]["supplement_semantic_contract"]
        self.assertFalse(contract["matched"])
        self.assertEqual(contract["expected_roles"], ["detail_neckline"])
        self.assertIn("supplement_target_role_not_observed", stored["qc_result"]["manual_review_reasons"])

    def test_matching_supplement_remains_ready(self) -> None:
        video = self.root / "matching-supplement.mp4"
        video.write_bytes(b"matching-supplement")
        asset, _ = register_media_asset(
            self.db,
            self.product.product_id,
            video,
            source_job_id="SUP_MATCHING",
            expected_tags={"shot_roles": ["detail_neckline"]},
        )
        worker = self.root / "matching_tag_worker.py"
        worker.write_text(
            "import json,sys\n"
            "json.load(sys.stdin)\n"
            "print(json.dumps({'status':'ready','observed_tags':{'shot_roles':['detail_neckline'],"
            "'segments':[{'primary_shot_role':'detail','secondary_roles':['detail_neckline']}]},"
            "'qc_result':{'decision':'ready'},'tagger_version':'fake-v1'}))\n",
            encoding="utf-8",
        )
        result = process_pending_asset_tags(
            self.db,
            product_id=self.product.product_id,
            tag_command=f"{sys.executable} {worker}",
        )
        self.assertEqual(result["ready"], 1)
        stored = self.db.get_media_asset(asset["asset_id"])
        self.assertEqual(stored["asset_status"], "ready")
        self.assertTrue(stored["qc_result"]["supplement_semantic_contract"]["matched"])

    def test_capacity_counts_unique_ready_assets_and_warns_before_hard_fill(self) -> None:
        video = self.root / "same-return.mp4"
        video.write_bytes(b"same-return")
        asset, _ = register_media_asset(self.db, self.product.product_id, video)
        self.db.upsert_media_asset({
            **asset,
            "tag_status": "completed",
            "asset_status": "ready",
            "observed_tags": {
                "shot_roles": ["hero", "main_wear_upper"],
                "segments": [{
                    "segment_id": "SEG-CAPACITY",
                    "start_ms": 0,
                    "end_ms": 8000,
                    "primary_shot_role": "hero",
                    "secondary_roles": ["main_wear_upper"],
                    "mixcut_usability": "yes",
                }],
            },
        })
        report = assess_product_asset_capacity(
            self.db, self.product.product_id, target_count=8,
        )
        self.assertEqual(report["unique_ready_assets"], 1)
        self.assertTrue(report["capacity_warning"])
        self.assertLess(report["stable_capacity"], 8)
        self.assertGreater(report["role_deficits"]["detail_closure"], 0)

    def test_cross_variant_planner_rotates_first_asset_and_sequence(self) -> None:
        strategies = build_strategy_pool(self.product.product_id, self.hooks(), ["显瘦版型"])
        for strategy in strategies:
            self.db.upsert_content_strategy(strategy)
        variants = sample_execution_variants(
            strategies,
            2,
            product_id=self.product.product_id,
            target_duration_seconds=22,
            plan_version="diversity-test",
            random_seed="diversity",
        )
        self.db.create_narrative_variants(variants)
        for role in ["hero", "detail", "result", "scene", "ending"]:
            for index in range(2):
                path = self.root / f"{role}-{index}.mp4"
                path.write_bytes(f"{role}-{index}".encode())
                asset, _ = register_media_asset(self.db, self.product.product_id, path)
                self.db.upsert_media_asset({
                    **asset,
                    "tag_status": "completed",
                    "asset_status": "ready",
                    "observed_tags": {
                        "shot_roles": [role],
                        "segments": [{
                            "segment_id": f"SEG-{role}-{index}",
                            "start_ms": 0,
                            "end_ms": 8000,
                            "primary_shot_role": role,
                            "secondary_roles": [],
                            "product_visibility": "high",
                            "mixcut_usability": "yes",
                        }],
                    },
                })
        first = plan_variant_rough_cut(self.db, variants[0].variant_id)
        second = plan_variant_rough_cut(self.db, variants[1].variant_id)
        self.assertNotEqual(first["clips"][0]["asset_id"], second["clips"][0]["asset_id"])
        self.assertNotEqual(
            [row["asset_id"] for row in first["clips"]],
            [row["asset_id"] for row in second["clips"]],
        )
        qc = evaluate_product_diversity(
            self.db, self.product.product_id, plan_version="diversity-test",
        )
        self.assertEqual(qc["duplicate_sequence_count"], 0)

    def test_diversity_qc_requires_at_least_two_planned_videos(self) -> None:
        qc = evaluate_product_diversity(
            self.db, self.product.product_id, plan_version="not-created",
        )
        self.assertEqual(qc["status"], "not_evaluated")
        self.assertIsNone(qc["diversity_score"])

    def test_capacity_requires_segments_long_enough_for_real_voiceover_intervals(self) -> None:
        strategies = build_strategy_pool(self.product.product_id, self.hooks(), ["短款腰线"])
        for strategy in strategies:
            self.db.upsert_content_strategy(strategy)
        variant = sample_execution_variants(
            strategies, 1, product_id=self.product.product_id,
            plan_version="duration-capacity-test", random_seed="duration-capacity",
        )[0]
        self.db.create_narrative_variants([variant])
        self.db.update_narrative_variant(
            variant.variant_id,
            assembly_plan={"beat_alignment": [{
                "start_ms": 0, "end_ms": 5000,
                "required_shot_roles": ["detail_waistline"],
            }]},
        )
        for name, duration in (("short", 3000), ("long", 8000)):
            path = self.root / f"{name}-waist.mp4"
            path.write_bytes(name.encode())
            asset, _ = register_media_asset(self.db, self.product.product_id, path)
            self.db.upsert_media_asset({
                **asset, "tag_status": "completed", "asset_status": "ready",
                "observed_tags": {"shot_roles": ["detail_waistline"], "segments": [{
                    "segment_id": f"SEG-{name}", "start_ms": 0, "end_ms": duration,
                    "primary_shot_role": "detail", "secondary_roles": ["detail_waistline"],
                    "mixcut_usability": "yes",
                }]},
            })
        report = assess_product_asset_capacity(
            self.db, self.product.product_id, target_count=1,
            plan_version="duration-capacity-test",
        )
        self.assertEqual(report["required_role_duration_ms"]["detail_waistline"], 5000)
        self.assertEqual(report["role_counts"]["detail_waistline"], 1)

    def test_visual_tagging_circuit_breaker_defers_remaining_assets(self) -> None:
        for index in range(3):
            path = self.root / f"circuit-{index}.mp4"
            path.write_bytes(f"circuit-{index}".encode())
            register_media_asset(self.db, self.product.product_id, path)
        with patch(
            "light_tryon.asset_ingestion._tag_with_command",
            side_effect=RuntimeError("LLM_CALL_EXHAUSTED: all retries and escalations exhausted"),
        ):
            result = process_pending_asset_tags(
                self.db, product_id=self.product.product_id, limit=3, tag_command="fake-worker",
            )
        self.assertTrue(result["circuit_open"])
        self.assertEqual(result["processed"], 2)
        self.assertEqual(result["deferred"], 1)

    def test_diversity_delivery_gate_blocks_shared_and_unreviewed_contract_assets(self) -> None:
        strategies = build_strategy_pool(self.product.product_id, self.hooks(), ["显瘦版型"])
        for strategy in strategies:
            self.db.upsert_content_strategy(strategy)
        variants = sample_execution_variants(
            strategies, 2, product_id=self.product.product_id,
            plan_version="delivery-gate-test", random_seed="delivery-gate",
        )
        self.db.create_narrative_variants(variants)
        path = self.root / "shared-contract.mp4"
        path.write_bytes(b"shared-contract")
        asset, _ = register_media_asset(self.db, self.product.product_id, path)
        self.db.upsert_media_asset({
            **asset, "tag_status": "completed", "asset_status": "ready",
            "observed_tags": {"shot_roles": ["main_wear_upper"], "segments": [{
                "segment_id": "SEG-SHARED", "start_ms": 0, "end_ms": 22000,
                "primary_shot_role": "hero", "secondary_roles": ["main_wear_upper"],
                "mixcut_usability": "yes",
            }]},
            "qc_result": {"tagging_method": "supplement_source_contract_fallback"},
        })
        for variant in variants:
            self.db.update_narrative_variant(variant.variant_id, assembly_plan={"clips": [{
                "asset_id": asset["asset_id"], "duplicate_group_id": asset["asset_id"],
                "segment_id": "SEG-SHARED", "duration_ms": 22000,
            }]})
        report = evaluate_product_diversity(
            self.db, self.product.product_id, plan_version="delivery-gate-test", persist=True,
        )
        self.assertFalse(report["delivery_gate"]["passed"])
        self.assertEqual(report["contract_fallback_asset_count"], 1)
        stored = self.db.get_narrative_variant(variants[0].variant_id)
        self.assertEqual(stored["assembly_plan"]["final_qc"]["status"], "blocked")

    def test_supplement_action_variant_is_written_into_prompt(self) -> None:
        shot = {
            "shot_id": "SUP_VARIANT",
            "shot_role": "fit_turn",
            "duration_seconds": 8,
            "reference_assets": ["/tmp/outfit.jpg"],
            "expected_tags": {
                "shot_roles": ["fit_turn"],
                "action_variant": "right_quarter",
                "avoid_action_variants": ["left_quarter"],
            },
        }
        context = {
            "product": self.product.to_dict(),
            "scene": {"prompt_core": "明亮室内奶油风空间，浅暖白墙面，画面不偏黄。"},
            "persona": {"prompt_core": "一名成年女生，浅色手机完整遮脸。"},
            "styling": {"prompt_core": "白色内搭和高腰直筒裤"},
            "visual_plan": {"resolved_inner_type": "圆领内搭", "resolved_inner_color": "白色"},
        }
        prompt = compile_supplement_prompt(shot, context)["positive_prompt"]
        self.assertIn("缓慢向右转约20度", prompt)
        self.assertIn("不得复刻这些已有动作版本：left_quarter", prompt)

    def test_targeted_supplement_prompts_include_role_capture_gates(self) -> None:
        context = {
            "product": self.product.to_dict(),
            "scene": {"prompt_core": "明亮室内奶油风空间，浅暖白墙面，画面不偏黄。"},
            "persona": {"prompt_core": "一名成年女生，浅色手机完整遮脸。"},
            "styling": {"prompt_core": "白色内搭和高腰直筒裤"},
            "visual_plan": {"resolved_inner_type": "圆领内搭", "resolved_inner_color": "白色"},
        }
        expectations = {
            "main_wear_upper": "占画面高度60%以上",
            "detail_neckline": "领口与上段门襟合计占画面40%以上",
            "detail_waistline": "完整衣摆与裤腰位于画面中央并占画面40%以上",
        }
        for role, phrase in expectations.items():
            with self.subTest(role=role):
                prompt = compile_supplement_prompt({
                    "shot_id": f"SUP_{role}",
                    "shot_role": role,
                    "duration_seconds": 8,
                    "reference_assets": ["/tmp/outfit.jpg"],
                    "expected_tags": {"shot_roles": [role]},
                }, context)["positive_prompt"]
                self.assertIn("镜头验收硬指标", prompt)
                self.assertIn(phrase, prompt)
                self.assertIn("连续", prompt)

    def test_roughcut_plan_uses_only_observed_ready_segments_and_hits_duration(self) -> None:
        strategies = build_strategy_pool(self.product.product_id, self.hooks(), ["显瘦版型"])
        for strategy in strategies:
            self.db.upsert_content_strategy(strategy)
        variant = sample_execution_variants(
            strategies,
            1,
            product_id=self.product.product_id,
            target_duration_seconds=22,
            plan_version="roughcut-test",
            random_seed="roughcut",
        )[0]
        self.db.create_narrative_variants([variant])
        for index, role in enumerate(["hero", "detail", "result", "scene", "ending"], start=1):
            path = self.root / f"asset-{index}.mp4"
            path.write_bytes(f"fake-video-{index}".encode())
            asset, _ = register_media_asset(self.db, self.product.product_id, path)
            self.db.upsert_media_asset({
                **asset,
                "tag_status": "completed",
                "asset_status": "ready",
                "observed_tags": {
                    "shot_roles": [role],
                    "segments": [{
                        "segment_id": f"SEG-{index}",
                        "start_ms": 0,
                        "end_ms": 8000,
                        "primary_shot_role": role,
                        "secondary_roles": [],
                        "hook_visual_type": "product_reveal" if role == "hero" else "none",
                        "product_visibility": "high",
                        "mixcut_usability": "yes",
                        "confidence": "high",
                        "reason": "实际拉链细节镜头" if role == "detail" else f"实际{role}镜头",
                    }],
                },
            })
        plan = plan_variant_rough_cut(self.db, variant.variant_id)
        self.assertEqual(sum(row["duration_ms"] for row in plan["clips"]), 22000)
        self.assertEqual(plan["visual_timeline"]["visual_slots"][-1]["end_ms"], 22000)
        self.assertEqual(len({row["segment_id"] for row in plan["clips"]}), len(plan["clips"]))
        aligned_lines = [
            {
                "block_id": "B1", "beat_ids": ["B1"], "role": "hook",
                "speech_text": "hook", "chinese_translation": "开头展示",
                "start_ms": 350, "end_ms": 6000,
                "required_shot_roles": ["main_wear_upper"],
            },
            {
                "block_id": "B2", "beat_ids": ["B2"], "role": "proof",
                "speech_text": "zip", "chinese_translation": "拉链结构",
                "start_ms": 6000, "end_ms": 12000,
                "required_shot_roles": ["detail_closure"],
            },
            {
                "block_id": "B3", "beat_ids": ["B3"], "role": "cta",
                "speech_text": "cta", "chinese_translation": "收尾",
                "start_ms": 12000, "end_ms": 17000,
                "required_shot_roles": ["main_wear_upper"],
            },
        ]
        self.db.update_narrative_variant(variant.variant_id, tts_timeline=aligned_lines)
        aligned = plan_variant_voiceover_cut(self.db, variant.variant_id)
        self.assertEqual(aligned["plan_version"], "narrative-voiceover-cut-v4-key-evidence")
        self.assertFalse(aligned["evidence_gaps"])
        self.assertEqual(sum(row["duration_ms"] for row in aligned["clips"]), 22000)
        zip_clips = [row for row in aligned["clips"] if row["beat_id"] == "B2"]
        self.assertTrue(zip_clips)
        self.assertTrue(all(row["primary_shot_role"] == "detail" for row in zip_clips))

    def test_tts_bridge_uses_measured_audio_and_builds_full_length_track(self) -> None:
        ffmpeg = shutil.which("ffmpeg")
        ffprobe = shutil.which("ffprobe")
        if not ffmpeg or not ffprobe:
            self.skipTest("ffmpeg/ffprobe unavailable")
        strategies = build_strategy_pool(self.product.product_id, self.hooks(), ["显瘦版型"])
        for strategy in strategies:
            self.db.upsert_content_strategy(strategy)
        variant = sample_execution_variants(
            strategies,
            1,
            product_id=self.product.product_id,
            target_duration_seconds=22,
            plan_version="tts-test",
            random_seed="tts",
        )[0]
        self.db.create_narrative_variants([variant])
        beats = [
            {"beat_id": "B1", "role": "hook", "speech_text": "สวัสดีค่ะ", "suggested_start_ms": 400},
            {"beat_id": "B2", "role": "proof", "speech_text": "ทรงนี้ใส่ง่าย", "suggested_start_ms": 2750},
            {"beat_id": "B3", "role": "cta", "speech_text": "ลองดูได้เลย", "suggested_start_ms": 19250},
        ]
        self.db.update_narrative_variant(
            variant.variant_id,
            workflow_state="waiting_tts",
            voiceover_status="completed",
            voiceover_response={"voiceover_text": " ".join(row["speech_text"] for row in beats), "beats": beats},
            beat_plan=beats,
        )

        class FakeProvider:
            provider_name = "fake-tts"

            def __init__(self):
                self.calls = 0

            async def synthesize(self, request):
                self.calls += 1
                subprocess.run(
                    [
                        ffmpeg, "-y", "-hide_banner", "-loglevel", "error",
                        "-f", "lavfi", "-i", "sine=frequency=440:duration=0.45",
                        "-c:a", "libmp3lame", str(request.output_path),
                    ],
                    check=True,
                )
                return object()

        fake_provider = FakeProvider()
        result = synthesize_variant_tts(
            self.db,
            variant.variant_id,
            output_dir=self.root / "tts",
            provider=fake_provider,
            ffmpeg_bin=ffmpeg,
            ffprobe_bin=ffprobe,
        )
        self.assertEqual(result["status"], "ready_for_asset_match")
        self.assertEqual(result["narration_mode"], "single_continuous_request")
        self.assertEqual(result["alignment_method"], "duration_weighted_fallback")
        self.assertEqual(fake_provider.calls, 1)
        self.assertTrue(Path(result["voice_track_path"]).is_file())
        self.assertLessEqual(abs(result["voice_track_duration_ms"] - 22000), 50)
        stored = self.db.get_narrative_variant(variant.variant_id)
        self.assertEqual(stored["workflow_state"], "matching_assets")
        self.assertTrue(stored["tts_timeline"])
        self.assertLessEqual(
            stored["tts_timeline"][-1]["start_ms"] - stored["tts_timeline"][-2]["end_ms"],
            900,
        )

    def test_final_mix_contains_voice_audio_and_exact_target_duration(self) -> None:
        ffmpeg = shutil.which("ffmpeg")
        ffprobe = shutil.which("ffprobe")
        if not ffmpeg or not ffprobe:
            self.skipTest("ffmpeg/ffprobe unavailable")
        strategies = build_strategy_pool(self.product.product_id, self.hooks(), ["显瘦版型"])
        for strategy in strategies:
            self.db.upsert_content_strategy(strategy)
        variant = sample_execution_variants(
            strategies,
            1,
            product_id=self.product.product_id,
            target_duration_seconds=22,
            plan_version="final-mix-test",
            random_seed="final-mix",
        )[0]
        self.db.create_narrative_variants([variant])
        roughcut = self.root / "roughcut.mp4"
        voice_track = self.root / "voice.m4a"
        subprocess.run(
            [
                ffmpeg, "-y", "-hide_banner", "-loglevel", "error",
                "-f", "lavfi", "-i", "color=c=white:s=320x568:r=25:d=22",
                "-c:v", "mpeg4", "-an", str(roughcut),
            ],
            check=True,
        )
        subprocess.run(
            [
                ffmpeg, "-y", "-hide_banner", "-loglevel", "error",
                "-f", "lavfi", "-i", "sine=frequency=440:duration=22",
                "-c:a", "aac", "-b:a", "128k", str(voice_track),
            ],
            check=True,
        )
        self.db.update_narrative_variant(
            variant.variant_id,
            workflow_state="matching_assets",
            assembly_plan={
                "roughcut_path": str(roughcut),
                "tts": {"voice_track_path": str(voice_track)},
            },
        )
        output = self.root / "final.mp4"
        result = render_narrative_voiceover_mix(
            self.db,
            variant.variant_id,
            output,
            ffmpeg_bin=ffmpeg,
            ffprobe_bin=ffprobe,
        )
        self.assertEqual(result["status"], "success")
        self.assertLessEqual(abs(result["duration_seconds"] - 22), 0.25)
        stored = self.db.get_narrative_variant(variant.variant_id)
        self.assertEqual(stored["workflow_state"], "final_qc")
        self.assertEqual(stored["assembly_plan"]["final_mix"]["audio_policy"], "voiceover_only_no_random_model_bgm")
        self.assertEqual(stored["assembly_plan"]["final_qc"]["status"], "pending")


if __name__ == "__main__":
    unittest.main()
