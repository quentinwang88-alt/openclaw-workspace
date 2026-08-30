from __future__ import annotations

import sys
import tempfile
import unittest
import json
import sqlite3
from pathlib import Path


SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.content_branches.organic_seeding.contracts import OrganicSeedThemeContract
from core.content_branches.organic_seeding.claim_adapter import (
    CentralClaimProvider,
    OrganicClaimAdapter,
)
from core.content_branches.organic_seeding.creative_qc import evaluate_organic_creative
from core.content_branches.organic_seeding.pipeline import OrganicSeedingPipeline
from core.content_branches.organic_seeding.planner import allocate_seed_themes, normalize_product_truth
from core.content_branches.organic_seeding.qc import (
    evaluate_organic_batch,
    evaluate_organic_script,
    evaluate_organic_visual,
)
from core.content_branches.organic_seeding.normalizers import normalize_voiceover
from core.content_branches.organic_seeding.retention_contract import compile_retention_contract
from core.content_branches.organic_seeding.topic_engine import allocate_topic_contracts
from core.content_branches.organic_seeding.story_planner import (
    build_organic_context_snapshot,
    plan_organic_stories,
)
from core.content_branches.storage import BranchArtifactStorage


class FakeLLM:
    primary_model = "fake-model"
    primary_reasoning_effort = "test"

    def __init__(self) -> None:
        self.calls = []

    def call_json(self, prompt, image_paths=None, max_tokens=0):
        self.calls.append({"prompt": prompt, "images": list(image_paths or [])})
        if "organic-visual-blueprint-v4" in prompt:
            return {
                "schema_version": "organic-visual-blueprint-v4",
                "script_title": "周末造型里的一个小细节",
                "creative_design": {"creator": "日常创作者", "scene": "窗边", "lived_moment": "出门前整理造型"},
                "opening_design": {"subject": "完整造型", "visible_action": "整理袖口", "viewer_value": "看搭配关系"},
                "capture_units": [
                    {"unit_id": "C1", "duration_seconds": 4, "narrative_job": "LIVED_MOMENT", "product_focus": "BACKGROUND", "product_interaction": "NONE", "shot_size": "MEDIUM", "body_coverage": "HEAD_TO_HIP", "camera_relation": "FIXED_TRIPOD", "setup_id": "SETUP_1", "shot": "中景", "camera_action": "固定机位", "subject_action": "拿起随身包准备出门", "authorized_props": [], "visible_zones": ["FRONT"], "product_evidence": "", "evidence_refs": [], "fact_refs": []},
                    {"unit_id": "C2", "duration_seconds": 5, "narrative_job": "RELATION", "product_focus": "SECONDARY", "product_interaction": "NATURAL_USE", "shot_size": "MEDIUM", "body_coverage": "HEAD_TO_HIP", "camera_relation": "FIXED_TRIPOD", "setup_id": "SETUP_1", "shot": "人物与造型关系景", "camera_action": "固定机位", "subject_action": "自然转向门口", "authorized_props": [], "visible_zones": ["FRONT"], "product_evidence": "可见纹理", "evidence_refs": [], "fact_refs": ["FACT_1"]},
                    {"unit_id": "C3", "duration_seconds": 5, "narrative_job": "ATMOSPHERE", "product_focus": "SECONDARY", "product_interaction": "NONE", "shot_size": "MEDIUM", "body_coverage": "HEAD_TO_HIP", "camera_relation": "FIXED_TRIPOD", "setup_id": "SETUP_1", "shot": "稍宽生活景", "camera_action": "固定机位", "subject_action": "关灯离开画面", "authorized_props": [], "visible_zones": ["FRONT"], "product_evidence": "", "evidence_refs": [], "fact_refs": []},
                ],
                "product_role_realization": {"role": "SUPPORTING", "first_prominent_unit": "C2", "memory_residue": "造型细节"},
                "commerce_elements": {"price": False, "promotion": False, "purchase_cta": False, "cart_reference": False},
                "video_generation_prompt": "三个生活片段自然直切，保持同一人物和商品。",
            }
        return {
            "schema_version": "organic-voiceover-v3",
            "target_text": "ก" * 176,
            "chinese_translation": "出门前我原本只看整套是否协调，但最后还是先判断这处纹理和造型有没有关系。画面转到侧面后，纹理没有抢走整体注意力，却让层次更清楚。对我来说，这才是值得保留的细节。",
            "estimated_duration_seconds": 13,
            "hook_surface": "个人观察",
            "content_progression": {
                "situation_or_tension": "出门前判断整套是否协调",
                "personal_judgment": "先看纹理是否与造型有关",
                "visible_evidence": "侧面画面里纹理与整体层次同时可见",
                "takeaway_or_discussion": "保留不抢整体注意力的细节",
            },
            "closing_mode": "OPEN_DISCUSSION",
            "used_fact_refs": ["FACT_1"],
            "experience_claims": [],
            "viewer_payoff_realization": "得到一个搭配观察角度",
            "commerce_elements": {"price": False, "promotion": False, "purchase_cta": False, "cart_reference": False},
        }


class CommerceRepairLLM(FakeLLM):
    def __init__(self, *, repair_succeeds: bool) -> None:
        super().__init__()
        self.repair_succeeds = repair_succeeds

    def call_json(self, prompt, image_paths=None, max_tokens=0):
        if "organic-visual-blueprint-v4" in prompt:
            return super().call_json(prompt, image_paths=image_paths, max_tokens=max_tokens)
        self.calls.append({"prompt": prompt, "images": list(image_paths or [])})
        repairing = "一次且仅一次的商业表达修订" in prompt
        if repairing and self.repair_succeeds:
            target_text = "วันนี้ลองมองแค่สีและสัดส่วนของลุคนี้"
            chinese = "今天只看这套造型的颜色和比例。"
        else:
            target_text = "กดสั่งซื้อเลย"
            chinese = "现在购买"
        return {
            "schema_version": "organic-voiceover-v1",
            "target_text": target_text,
            "chinese_translation": chinese,
            "estimated_duration_seconds": 5,
            "hook_surface": "个人观察",
            "closing_mode": "NATURAL_END",
            "used_fact_refs": ["FACT_1"],
            "experience_claims": [],
            "viewer_payoff_realization": "得到一个搭配观察角度",
            "commerce_elements": {"price": False, "promotion": False, "purchase_cta": False, "cart_reference": False},
        }


class EvidenceLLM(FakeLLM):
    def __init__(self) -> None:
        super().__init__()
        self.evidence_calls = 0

    def call_json(self, prompt, image_paths=None, max_tokens=0):
        if "product-visual-evidence-v1" in prompt:
            self.calls.append({"prompt": prompt, "images": list(image_paths or [])})
            self.evidence_calls += 1
            return {
                "schema_version": "product-visual-evidence-v1",
                "anchors": [
                    {
                        "anchor_type": "COLLAR",
                        "description": "可见小立领",
                        "visible_zones": ["COLLAR"],
                        "confidence": "HIGH",
                    }
                ],
            }
        output = super().call_json(prompt, image_paths=image_paths, max_tokens=max_tokens)
        if "organic-visual-blueprint-v4" in prompt:
            for unit in output.get("capture_units") or []:
                unit["fact_refs"] = []
        else:
            output["used_fact_refs"] = []
        return output


class DensityRepairLLM(FakeLLM):
    def __init__(self) -> None:
        super().__init__()
        self.voice_calls = 0

    def call_json(self, prompt, image_paths=None, max_tokens=0):
        if "organic-visual-blueprint-v4" in prompt:
            return super().call_json(prompt, image_paths=image_paths, max_tokens=max_tokens)
        self.calls.append({"prompt": prompt, "images": list(image_paths or [])})
        self.voice_calls += 1
        if self.voice_calls == 1:
            return {
                "schema_version": "organic-voiceover-v3",
                "target_text": "ก" * 80,
                "chinese_translation": "我只看这处纹理。",
                "estimated_duration_seconds": 6,
                "hook_surface": "个人观察",
                "closing_mode": "NATURAL_END",
                "used_fact_refs": ["FACT_1"],
                "experience_claims": [],
                "viewer_payoff_realization": "得到一个观察角度",
                "commerce_elements": {},
            }
        return {
            "schema_version": "organic-voiceover-v3",
            "target_text": "ก" * 176,
            "chinese_translation": "出门前先看整套是否协调，我原本以为纹理会抢走注意力，但侧面画面里它只补了一层变化。对我来说，细节能被看到、又不盖过整套，才是这次保留它的理由。",
            "estimated_duration_seconds": 13,
            "hook_surface": "先给判断再解释",
            "content_progression": {
                "situation_or_tension": "出门前检查整套协调度",
                "personal_judgment": "担心纹理抢走注意力",
                "visible_evidence": "侧面画面只增加一层变化",
                "takeaway_or_discussion": "保留不盖过整套的细节",
            },
            "closing_mode": "NATURAL_END",
            "used_fact_refs": ["FACT_1"],
            "experience_claims": [],
            "viewer_payoff_realization": "得到一个具体搭配判断方法",
            "commerce_elements": {},
        }


class OrganicSeedingPipelineTest(unittest.TestCase):
    def _product(self):
        return {
            "product_code": "P1", "top_category": "配饰", "product_type": "围巾",
            "target_country": "泰国", "target_language": "泰语",
            "facts": [{"fact_id": "FACT_1", "text": "表面有细密纹理"}],
            "identity_anchors": ["保持参考图颜色与纹理"],
            "negative_constraints": ["不得编造材质"],
        }

    def test_formal_pipeline_keeps_visual_and_voiceover_calls_separate(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            fake = FakeLLM()
            pipeline = OrganicSeedingPipeline(
                llm_client=fake,
                storage=BranchArtifactStorage(Path(temp_dir) / "seed.sqlite3"),
            )
            result = pipeline.run(
                request_id="REQ1", product_context=self._product(), count=1,
                image_paths=["/tmp/product.jpg"],
            )
            self.assertEqual(result["status"], "COMPLETED")
            self.assertEqual(len(fake.calls), 2)
            self.assertEqual(fake.calls[0]["images"], ["/tmp/product.jpg"])
            self.assertEqual(fake.calls[1]["images"], [])
            self.assertEqual(result["publish_policy"]["cart_policy"], "FORBIDDEN")
            self.assertNotIn("selling_argument", str(result))
            item = result["items"][0]
            prompt = item["video_generation_prompt"]
            self.assertIn("【生活时刻】", prompt)
            self.assertIn("【人物与场景】", prompt)
            self.assertIn("【声音后期交接】", prompt)
            self.assertNotIn("商品可见结果：", prompt)
            self.assertNotIn("发布策略", prompt)
            self.assertNotIn("三个生活片段自然直切", prompt)
            self.assertEqual(
                item["video_execution_brief"]["audio_mode"],
                "VOICEOVER_ADDED_IN_POST",
            )
            self.assertEqual(item["video_execution_brief"]["render_profile"], "ORGANIC_LIFESTYLE")
            stages = pipeline.storage.list_stage_artifacts(result["run_id"])
            self.assertEqual(
                {stage["stage_key"] for stage in stages},
                {"TOPIC_PLAN", "PLAN", "VISUAL", "VOICEOVER", "FINAL"},
            )
            self.assertIn("【前3秒留存】", prompt)
            self.assertIn("topic_contract", item)
            self.assertIn("retention_contract", item)
            self.assertIn(item["quality"]["quality_dimensions"]["creative_quality"], {"A", "B", "C"})
            self.assertEqual(item["quality"]["quality_dimensions"]["voiceover_density"], "A")
            self.assertIn("soft_spoken_seconds=10.5-14.4秒", fake.calls[1]["prompt"])
            self.assertIn("organic-spoken-brief-v1", fake.calls[1]["prompt"])
            self.assertIn("不要求把处境、判断、依据、结论写成四句", fake.calls[1]["prompt"])

    def test_preview_and_formal_runs_have_separate_content_identity(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            fake = FakeLLM()
            pipeline = OrganicSeedingPipeline(
                llm_client=fake,
                storage=BranchArtifactStorage(Path(temp_dir) / "seed.sqlite3"),
            )
            preview = pipeline.run(
                request_id="REQ_MODE", product_context=self._product(), count=1,
                image_paths=["/tmp/product.jpg"], preview_only=True,
            )
            formal = pipeline.run(
                request_id="REQ_MODE", product_context=self._product(), count=1,
                image_paths=["/tmp/product.jpg"], preview_only=False,
            )
            self.assertNotEqual(preview["run_id"], formal["run_id"])
            self.assertEqual(formal["status"], "COMPLETED")

    def test_same_objective_allocates_distinct_internal_angles_and_scenes(self):
        themes = allocate_seed_themes(
            self._product(),
            count=5,
            theme_inputs=[{"objective": "STYLE_MEMORY"}],
        )
        self.assertEqual(len({item.angle_family for item in themes}), 5)
        self.assertEqual(len({item.scene_family for item in themes}), 5)
        self.assertEqual(len({item.creative_signature for item in themes}), 5)
        self.assertTrue(all("咖啡" not in item.lived_context for item in themes))
        self.assertEqual(len({item.rhetorical_family for item in themes}), 5)
        self.assertGreaterEqual(len({item.closing_mode for item in themes}), 2)

    def test_same_objective_also_allocates_distinct_topic_families(self):
        themes = allocate_seed_themes(
            self._product(), count=5, theme_inputs=[{"objective": "STYLE_MEMORY"}],
        )
        topics = allocate_topic_contracts(themes, self._product())
        self.assertEqual(len({item.topic_family for item in topics}), 5)
        self.assertTrue(all(item.topic_thesis for item in topics))
        self.assertTrue(all(item.comment_trigger for item in topics))

    def test_retention_contract_has_short_opening_and_dynamic_audio_arc(self):
        theme = allocate_seed_themes(self._product(), count=1)[0]
        topic = allocate_topic_contracts([theme], self._product())[0]
        contract = compile_retention_contract(
            theme=theme, topic=topic, duration_seconds=15,
        )
        self.assertEqual(contract["clip_rhythm"]["opening_clip_seconds"], [1.2, 2.2])
        self.assertEqual(contract["audio_arc"]["opening_energy"], "medium")
        self.assertEqual(contract["audio_arc"]["voiceover_bed_energy"], "low")
        self.assertEqual(contract["voiceover_density"]["target_measured_seconds"], [7.0, 14.4])
        self.assertFalse(contract["voiceover_density"]["minimum_is_hard"])
        self.assertNotIn("semantic_progression", contract["voiceover_density"])

    def test_short_voiceover_is_soft_review_signal_not_auto_repair(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            fake = DensityRepairLLM()
            pipeline = OrganicSeedingPipeline(
                llm_client=fake,
                storage=BranchArtifactStorage(Path(temp_dir) / "seed.sqlite3"),
            )
            result = pipeline.run(
                request_id="REQ_DENSITY_REPAIR",
                product_context=self._product(),
                count=1,
                image_paths=["/tmp/product.jpg"],
            )
            self.assertEqual(result["status"], "COMPLETED")
            self.assertEqual(len(fake.calls), 2)
            self.assertNotIn("voice_repair", result["items"][0]["provenance"])
            self.assertIn(
                result["items"][0]["quality"]["quality_dimensions"]["voiceover_density"],
                {"B", "C"},
            )

    def test_central_claims_are_cleanly_projected_and_story_selected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "voiceover.sqlite"
            connection = sqlite3.connect(db_path)
            connection.executescript(
                """
                CREATE TABLE product_claim_sources (
                  claim_source_id TEXT PRIMARY KEY, product_id TEXT, raw_text TEXT,
                  source_type TEXT, source_ref TEXT, source_hash TEXT,
                  operator_priority TEXT, valid_until TEXT, created_by TEXT, created_at TEXT
                );
                CREATE TABLE product_claims (
                  claim_id TEXT PRIMARY KEY, product_id TEXT, claim_source_id TEXT,
                  concept_id TEXT, source_span TEXT, canonical_claim_zh TEXT,
                  claim_type TEXT, claim_theme TEXT, verification_status TEXT,
                  evidence_requirement TEXT, allowed_strength TEXT,
                  operator_priority TEXT, risk_tags_json TEXT,
                  normalizer_version TEXT, normalizer_confidence REAL,
                  reviewed_by TEXT, review_note TEXT, created_at TEXT, updated_at TEXT
                );
                """
            )
            claims = [
                ("C1", "短款衣长收在腰线附近", "feature", "fit", "VERIFIED", "factual"),
                ("C2", "宽松版型", "feature", "fit", "VERIFIED", "factual"),
                ("C3", "腿部线条视觉更修长", "visual_result", "fit", "VERIFIED", "moderate"),
                ("C4", "小个子上身比例视觉更利落", "visual_result", "fit", "VERIFIED", "soft_only"),
                ("C5", "高领防风", "feature", "unresolved", "UNRESOLVED", "soft_only"),
            ]
            for index, (claim_id, text, claim_type, theme, status, strength) in enumerate(claims, 1):
                source_id = f"S{index}"
                connection.execute(
                    "INSERT INTO product_claim_sources VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (source_id, "P-JACKET", text, "operator_input", f"feishu:x#argument-v2-{index}", f"H{index}", "core", None, "test", "2026-01-01"),
                )
                connection.execute(
                    "INSERT INTO product_claims VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (claim_id, "P-JACKET", source_id, None, text, text, claim_type, theme, status, "video_positive" if status == "VERIFIED" else "source_plus_video", strength, "core", "[]", "v1", 1.0, None, None, "2026-01-01", "2026-01-01"),
                )
            connection.commit()
            connection.close()

            snapshot = CentralClaimProvider(db_path).snapshot("P-JACKET")
            adapter = OrganicClaimAdapter()
            catalogue = adapter.project(snapshot)
            self.assertEqual(sum(item["organic_eligibility"] == "ELIGIBLE" for item in catalogue), 4)
            self.assertEqual(next(item for item in catalogue if item["claim_id"] == "C5")["organic_eligibility"], "DEFERRED")
            enriched = adapter.enrich_product_context(
                {
                    "product_code": "P-JACKET", "product_type": "外套",
                    "target_country": "泰国", "target_language": "泰语",
                    "facts": [], "identity_anchors": ["商品图权威"],
                },
                snapshot,
            )
            truth = normalize_product_truth(enriched)
            context = build_organic_context_snapshot(
                truth, [{"objective": "STYLE_MEMORY", "experience_authority": "NONE"}]
            )
            plan = plan_organic_stories(context, count=5)
            self.assertEqual(len(plan["selected"]), 5)
            self.assertGreaterEqual(
                len({item["core_claim_ref"] for item in plan["selected"]}), 4
            )
            self.assertTrue(all(len(item["allowed_fact_refs"]) <= 2 for item in plan["selected"]))

    def test_creative_qc_is_non_blocking_when_opening_is_flat(self):
        review = evaluate_organic_creative(
            topic_contract={"topic_family": "VISUAL_SURPRISE", "topic_thesis": "先看结果"},
            retention_contract={
                "first_3s_open_loop": "稍后解释",
                "audio_arc": {
                    "opening_energy": "medium", "voiceover_bed_energy": "low",
                    "payoff_energy": "medium",
                },
            },
            visual_blueprint={
                "opening_design": {"visible_action": "安静观察"},
                "capture_units": [
                    {"duration_seconds": 5, "subject_action": "安静观察", "shot_size": "FULL", "camera_relation": "FIXED_TRIPOD"},
                    {"duration_seconds": 5, "subject_action": "继续观察", "shot_size": "FULL", "camera_relation": "FIXED_TRIPOD"},
                    {"duration_seconds": 5, "subject_action": "保持站立", "shot_size": "FULL", "camera_relation": "FIXED_TRIPOD"},
                ],
            },
            voiceover={
                "hook_surface": "个人观察", "chinese_translation": "我只是在看这套造型的关系。",
                "closing_mode": "NATURAL_END", "commerce_elements": {},
            },
        )
        self.assertEqual(review["overall_grade"], "C")
        self.assertFalse(review["hard_block"])
        self.assertEqual(review["production_recommendation"], "CREATIVE_REVISION_RECOMMENDED")

    def test_visual_pre_qc_blocks_internal_thai_and_fixed_camera_motion(self):
        theme = allocate_seed_themes(self._product(), count=1)[0]
        quality = evaluate_organic_visual(
            theme=theme,
            product_truth=self._product(),
            target_language="泰语",
            visual_blueprint={
                "script_title": "ฉากชีวิต",
                "capture_units": [
                    {
                        "duration_seconds": 15,
                        "camera_action": "推近",
                        "camera_relation": "FIXED_TRIPOD",
                        "body_coverage": "HEAD_TO_HIP",
                        "visible_zones": ["FRONT"],
                        "fact_refs": ["FACT_1"],
                    }
                ],
                "commerce_elements": {},
            },
            duration_seconds=15,
        )
        self.assertIn("ORGANIC_VISUAL_INTERNAL_LANGUAGE_NOT_CHINESE", quality["hard_errors"])
        self.assertIn("ORGANIC_CAPTURE_MODE_CONFLICT", quality["hard_errors"])

    def test_visual_pre_qc_blocks_shot_evidence_visibility_conflict(self):
        theme = allocate_seed_themes(self._product(), count=1)[0]
        quality = evaluate_organic_visual(
            theme=theme,
            product_truth=self._product(),
            visual_blueprint={
                "script_title": "细节",
                "capture_units": [
                    {
                        "duration_seconds": 15,
                        "camera_action": "固定机位",
                        "body_coverage": "WAIST_TO_KNEE",
                        "visible_zones": ["COLLAR"],
                        "fact_refs": ["FACT_1"],
                    }
                ],
            },
            duration_seconds=15,
        )
        self.assertIn("ORGANIC_SHOT_EVIDENCE_VISIBILITY_CONFLICT", quality["hard_errors"])

    def test_visual_pre_qc_blocks_sales_like_product_checklist(self):
        theme = allocate_seed_themes(
            self._product(), count=1, theme_inputs=[{"objective": "STYLE_MEMORY", "product_role": "SUPPORTING"}]
        )[0]
        units = [
            {
                "duration_seconds": 5,
                "narrative_job": "PRODUCT_DETAIL",
                "product_focus": "PRIMARY",
                "product_interaction": "DEMONSTRATION",
                "camera_action": "固定机位",
                "body_coverage": "DETAIL_ONLY",
                "visible_zones": ["FRONT"],
                "product_evidence": "逐项展示细节",
                "fact_refs": ["FACT_1"],
            }
            for _ in range(3)
        ]
        quality = evaluate_organic_visual(
            theme=theme,
            product_truth=self._product(),
            visual_blueprint={"script_title": "生活观察", "capture_units": units},
            duration_seconds=15,
        )
        self.assertIn("ORGANIC_LIFESTYLE_RATIO_TOO_LOW", quality["hard_errors"])
        self.assertIn("ORGANIC_PRODUCT_FOCUS_EXCEEDS_ROLE", quality["hard_errors"])
        self.assertIn("ORGANIC_PRODUCT_DEMONSTRATION_FORBIDDEN", quality["hard_errors"])
        self.assertIn("ORGANIC_DETAIL_SHOT_CONFLICTS_WITH_OBJECTIVE", quality["hard_errors"])
        self.assertIn("ORGANIC_PRODUCT_EVIDENCE_CHECKLIST_PATTERN", quality["hard_errors"])

    def test_voiceover_duration_is_measured_instead_of_trusting_model(self):
        normalized = normalize_voiceover(
            {
                "target_text": "ก" * 260,
                "chinese_translation": "长口播",
                "estimated_duration_seconds": 5,
            },
            target_language="泰语",
            duration_seconds=15,
        )
        self.assertEqual(normalized["model_estimated_duration_seconds"], 5)
        self.assertGreater(normalized["estimated_duration_seconds"], 18)

    def test_batch_qc_downgrades_repeated_rhetorical_structure(self):
        results = [
            {
                "seed_theme": {
                    "angle_family": f"A{index}",
                    "creative_signature": f"S{index}",
                    "rhetorical_family": "LIVED_DISCOVERY",
                    "hook_mechanism": "LIVED_MOMENT",
                    "closing_mode": "NATURAL_END",
                },
                "visual_blueprint": {"creative_design": {"scene": f"场景{index}"}},
                "voiceover": {"chinese_translation": f"不同表达{index}"},
            }
            for index in range(4)
        ]
        quality = evaluate_organic_batch(results)
        self.assertEqual(quality["grade"], "C")
        self.assertIn("ORGANIC_BATCH_RHETORICAL_FAMILY_REPEAT", quality["warnings"])

    def test_action_overload_is_review_warning_not_hard_block(self):
        theme = allocate_seed_themes(
            self._product(), count=1, theme_inputs=[{"objective": "STYLE_MEMORY"}]
        )[0]
        qc = evaluate_organic_script(
            theme=theme,
            visual_blueprint={
                "capture_units": [
                    {
                        "duration_seconds": 15,
                        "shot": "镜头环绕并横移推近",
                        "action": "人物走路转身回头整理衣领",
                        "fact_refs": ["FACT_1"],
                    }
                ],
                "commerce_elements": {},
            },
            voiceover={
                "target_text": "ข้อความภาษาไทย",
                "chinese_translation": "只看一个造型细节",
                "used_fact_refs": ["FACT_1"],
                "experience_claims": [],
                "viewer_payoff_realization": "观察角度",
                "commerce_elements": {},
            },
            target_language="泰语",
        )
        self.assertTrue(qc["passed"])
        self.assertIn("ORGANIC_CAPTURE_UNIT_ACTION_OVERLOADED", qc["warnings"])
        self.assertEqual(qc["quality_dimensions"]["execution_grade"], "B")

    def test_unconfirmed_long_term_experience_is_blocked(self):
        theme = OrganicSeedThemeContract(
            theme_id="T1", objective="PERSONAL_POSITION", viewer_payoff="一个审美判断",
            lived_context="出门前", taste_judgment="个人偏好", product_role="SUPPORTING",
            product_prominence="MID", memory_residue="造型感", product_connection="观察纹理",
            allowed_fact_refs=("FACT_1",), experience_authority="NONE",
        )
        qc = evaluate_organic_script(
            theme=theme,
            visual_blueprint={"capture_units": [{"fact_refs": ["FACT_1"]}], "commerce_elements": {}},
            voiceover={
                "target_text": "x", "chinese_translation": "我已经回购很多次",
                "used_fact_refs": ["FACT_1"], "experience_claims": ["已经回购很多次"],
                "viewer_payoff_realization": "判断", "commerce_elements": {},
            },
        )
        self.assertFalse(qc["passed"])
        self.assertIn("ORGANIC_UNAUTHORIZED_EXPERIENCE_CLAIM", qc["hard_errors"])

    def test_commerce_language_is_blocked_even_if_flags_are_false(self):
        theme = OrganicSeedThemeContract(
            theme_id="T1", objective="DISCUSSION", viewer_payoff="一个问题",
            lived_context="日常", taste_judgment="个人偏好", product_role="INCIDENTAL",
            product_prominence="LATE", memory_residue="讨论", product_connection="观察",
            allowed_fact_refs=("FACT_1",), experience_authority="NONE",
        )
        qc = evaluate_organic_script(
            theme=theme,
            visual_blueprint={"capture_units": [{"fact_refs": ["FACT_1"]}], "commerce_elements": {}},
            voiceover={
                "target_text": "buy now", "chinese_translation": "现在购买",
                "used_fact_refs": ["FACT_1"], "experience_claims": [],
                "viewer_payoff_realization": "讨论", "commerce_elements": {},
            },
        )
        self.assertIn("ORGANIC_COMMERCE_LANGUAGE_FORBIDDEN", qc["hard_errors"])
        self.assertEqual(qc["matched_commerce_hits"][0]["field_path"], "voiceover.target_text")

    def test_negated_commerce_guard_is_not_mistaken_for_a_cta(self):
        theme = OrganicSeedThemeContract(
            theme_id="T1", objective="DISCUSSION", viewer_payoff="一个问题",
            lived_context="日常", taste_judgment="个人偏好", product_role="INCIDENTAL",
            product_prominence="LATE", memory_residue="讨论", product_connection="观察",
            allowed_fact_refs=("FACT_1",), experience_authority="NONE",
        )
        qc = evaluate_organic_script(
            theme=theme,
            visual_blueprint={
                "capture_units": [{"fact_refs": ["FACT_1"]}],
                "video_generation_prompt": "画面不得出现价格、优惠、购买按钮、购物车、链接或促销文字。",
                "commerce_elements": {},
            },
            voiceover={
                "target_text": "ข้อความภาษาไทย", "chinese_translation": "只聊造型，不是购买建议",
                "used_fact_refs": ["FACT_1"], "experience_claims": [],
                "viewer_payoff_realization": "讨论", "commerce_elements": {},
            },
        )
        self.assertTrue(qc["passed"])
        self.assertEqual(qc["matched_commerce_hits"], [])
        self.assertGreaterEqual(len(qc["ignored_negated_commerce_hits"]), 5)

    def test_internal_metadata_is_not_scanned_as_publishable_copy(self):
        theme = OrganicSeedThemeContract(
            theme_id="T1", objective="DISCUSSION", viewer_payoff="购买决策参考",
            lived_context="日常", taste_judgment="个人偏好", product_role="INCIDENTAL",
            product_prominence="LATE", memory_residue="讨论", product_connection="观察",
            allowed_fact_refs=("FACT_1",), experience_authority="NONE",
        )
        qc = evaluate_organic_script(
            theme=theme,
            visual_blueprint={"capture_units": [{"fact_refs": ["FACT_1"]}], "commerce_elements": {}},
            voiceover={
                "target_text": "ข้อความภาษาไทย", "chinese_translation": "只看造型关系",
                "used_fact_refs": ["FACT_1"], "experience_claims": [],
                "viewer_payoff_realization": "购买决策参考", "commerce_elements": {},
            },
        )
        self.assertTrue(qc["passed"])

    def test_string_false_commerce_flags_are_not_truthy(self):
        theme = OrganicSeedThemeContract(
            theme_id="T1", objective="DISCUSSION", viewer_payoff="一个问题",
            lived_context="日常", taste_judgment="个人偏好", product_role="INCIDENTAL",
            product_prominence="LATE", memory_residue="讨论", product_connection="观察",
            allowed_fact_refs=("FACT_1",), experience_authority="NONE",
        )
        flags = {"price": "false", "promotion": "false", "purchase_cta": "false", "cart_reference": "false"}
        qc = evaluate_organic_script(
            theme=theme,
            visual_blueprint={"capture_units": [{"fact_refs": ["FACT_1"]}], "commerce_elements": flags},
            voiceover={
                "target_text": "ข้อความภาษาไทย", "chinese_translation": "只看造型关系",
                "used_fact_refs": ["FACT_1"], "experience_claims": [],
                "viewer_payoff_realization": "讨论", "commerce_elements": flags,
            },
        )
        self.assertTrue(qc["passed"])

    def test_selected_item_indices_preserve_original_slots(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            pipeline = OrganicSeedingPipeline(
                llm_client=FakeLLM(),
                storage=BranchArtifactStorage(Path(temp_dir) / "seed.sqlite3"),
            )
            result = pipeline.run(
                request_id="REQ_SELECTED", product_context=self._product(), count=3,
                item_indices=[1, 3], image_paths=["/tmp/product.jpg"],
            )
            self.assertEqual(result["requested_item_indices"], [1, 3])
            self.assertEqual([item["item_index"] for item in result["items"]], [1, 3])
            self.assertEqual(result["requested_count"], 2)

    def test_retry_selection_does_not_change_script_identity(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = BranchArtifactStorage(Path(temp_dir) / "seed.sqlite3")
            pipeline = OrganicSeedingPipeline(llm_client=FakeLLM(), storage=storage)
            full = pipeline.run(
                request_id="REQ_STABLE", product_context=self._product(), count=2,
                image_paths=["/tmp/product.jpg"],
            )
            retry = pipeline.run(
                request_id="REQ_STABLE", product_context=self._product(), count=2,
                item_indices=[1], image_paths=["/tmp/product.jpg"],
            )
            self.assertEqual(full["run_id"], retry["run_id"])
            self.assertEqual(full["items"][0]["script_id"], retry["items"][0]["script_id"])

    def test_successful_product_visual_evidence_is_frozen_for_retry(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = BranchArtifactStorage(Path(temp_dir) / "seed.sqlite3")
            fake = EvidenceLLM()
            pipeline = OrganicSeedingPipeline(llm_client=fake, storage=storage)
            product = self._product()
            product["facts"] = []
            product["identity_anchors"] = ["参考图是目标商品唯一权威"]
            first = pipeline.run(
                request_id="REQ_EVIDENCE", product_context=product, count=1,
                image_paths=["/tmp/product.jpg"],
            )
            retry = pipeline.run(
                request_id="REQ_EVIDENCE", product_context=product, count=1,
                image_paths=["/tmp/product.jpg"],
            )
            self.assertEqual(first["status"], "COMPLETED")
            self.assertEqual(retry["status"], "COMPLETED")
            self.assertEqual(first["run_id"], retry["run_id"])
            self.assertEqual(fake.evidence_calls, 1)

    def test_real_cta_gets_one_controlled_voiceover_repair(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            fake = CommerceRepairLLM(repair_succeeds=True)
            pipeline = OrganicSeedingPipeline(
                llm_client=fake,
                storage=BranchArtifactStorage(Path(temp_dir) / "seed.sqlite3"),
            )
            result = pipeline.run(
                request_id="REQ_REPAIR", product_context=self._product(), count=1,
                image_paths=["/tmp/product.jpg"],
            )
            self.assertEqual(result["status"], "COMPLETED")
            self.assertEqual(len(fake.calls), 3)
            repair = result["items"][0]["provenance"]["commerce_repair"]
            self.assertEqual(repair["scope"], "VOICEOVER")
            self.assertTrue(repair["passed"])

    def test_failed_repair_persists_draft_and_qc_evidence(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "seed.sqlite3"
            storage = BranchArtifactStorage(db_path)
            pipeline = OrganicSeedingPipeline(
                llm_client=CommerceRepairLLM(repair_succeeds=False), storage=storage,
            )
            result = pipeline.run(
                request_id="REQ_PERSIST_FAILURE", product_context=self._product(), count=1,
                image_paths=["/tmp/product.jpg"],
            )
            self.assertEqual(result["status"], "FAILED")
            stored = storage.list_items(result["run_id"])[0]
            evidence = json.loads(stored["result_json"])
            self.assertIn("voiceover", evidence)
            self.assertTrue(evidence["quality"]["matched_commerce_hits"])
            self.assertTrue(evidence["provenance"]["commerce_repair"]["attempted"])

    def test_wrong_target_writing_system_is_blocked(self):
        theme = OrganicSeedThemeContract(
            theme_id="T1", objective="STYLE_MEMORY", viewer_payoff="一个观察角度",
            lived_context="日常", taste_judgment="个人偏好", product_role="SUPPORTING",
            product_prominence="MID", memory_residue="造型", product_connection="观察",
            allowed_fact_refs=("FACT_1",), experience_authority="NONE",
        )
        qc = evaluate_organic_script(
            theme=theme,
            target_language="泰语",
            visual_blueprint={"capture_units": [{"fact_refs": ["FACT_1"]}], "commerce_elements": {}},
            voiceover={
                "target_text": "这不是泰语", "chinese_translation": "中文对照",
                "used_fact_refs": ["FACT_1"], "experience_claims": [],
                "viewer_payoff_realization": "观察", "commerce_elements": {},
            },
        )
        self.assertIn("TARGET_LANGUAGE_THAI_SCRIPT_MISSING", qc["hard_errors"])


if __name__ == "__main__":
    unittest.main()
