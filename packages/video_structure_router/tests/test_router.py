import unittest
from dataclasses import replace

from video_structure_router.models import RouteRequest, StructureCandidate
from video_structure_router.service import StructureRouterService
from video_structure_router.validator import validate_script_against_contract


def candidate(
    key: str,
    beats,
    carrier: str,
    continuity: str,
    cut: str,
    hook: str,
    evidence: str = "BOOTSTRAP",
    shot_median=None,
):
    return StructureCandidate(
        candidate_key=key,
        source_kind="TEST",
        source_run_id="run-test",
        cluster_id=int(key[-1]),
        cluster_version="v1",
        prototype_id=key,
        cluster_status="BOOTSTRAP_CANDIDATE",
        evidence_tier=evidence,
        macro_structure_name=key,
        structure_description=key,
        beat_sequence=list(beats),
        required_beats=list(beats),
        optional_beats=[],
        content_carrier=carrier,
        continuity_mode=continuity,
        cut_density=cut,
        visual_hook_type=hook,
        proof_mechanisms=["NATURAL_USE"],
        ending_pattern="RESULT_HOLD" if "ENDING" in beats else "",
        shot_count_min=int(shot_median) if shot_median is not None else None,
        shot_count_max=int(shot_median) if shot_median is not None else None,
        shot_count_median=shot_median,
        duration_median=15.0,
        member_count=20,
        distinct_videos=20,
        cohesion=0.8,
        extraction_confidence=0.8,
        categories=["配饰"],
        countries=["TH"],
        variation_axes=[],
        representative_cases=[],
        extractor_versions=["test-v1"],
        feature_schema_versions=["feature-v1"],
        compatibility_matrix_versions=["compat-v1"],
        profile_types=["PROMPT_ONLY"],
        independence_levels=["NONE"],
    )


class FakeRepository:
    def load_candidates(self):
        return [
            candidate("c0", ["HOOK", "PROOF"], "STATIC_PRODUCT", "MULTI_CUT", "MEDIUM", "PRODUCT_REVEAL"),
            candidate("c1", ["HOOK", "PROOF", "ENDING"], "HAND_ONLY", "MULTI_CUT", "HIGH", "ACTION_HOOK"),
            candidate("c2", ["HOOK", "USE_PROCESS", "PROOF"], "WEARER_ACTIVE", "CONTINUOUS_LOW_CUT", "LOW", "PERSON_REVEAL"),
            candidate("c3", ["HOOK", "PROOF", "ENDING"], "MIXED", "MULTI_CUT", "LOW", "TEXT_SHOCK"),
        ]


class RouterTest(unittest.TestCase):
    def request(self):
        return RouteRequest(
            request_id="request-1",
            consumer_flow="ORIGINAL_SCRIPT",
            product_code="P1",
            target_country="泰国",
            category="配饰",
            product_type="发饰",
            direction_count=4,
            duration_seconds=15,
            random_seed=9,
            capabilities={
                "allowed_carriers": ["STATIC_PRODUCT", "HAND_ONLY", "WEARER_ACTIVE", "MIXED"],
                "allowed_continuity_modes": ["MULTI_CUT", "CONTINUOUS_LOW_CUT"],
                "min_shots": 4,
                "max_shots": 6,
            },
        )

    def test_selection_is_reproducible_and_slots_are_only_compatibility_labels(self):
        service = StructureRouterService(repository=FakeRepository())
        first = service.select(self.request()).to_dict()
        second = service.select(self.request()).to_dict()
        self.assertEqual(first, second)
        self.assertEqual([item["output_slot"] for item in first["assignments"]], ["S1", "S2", "S3", "S4"])
        self.assertGreaterEqual(
            len({item["macro_family_key"] for item in first["assignments"]}),
            2,
        )
        self.assertTrue(all(item["structure_contract"]["provenance"]["output_slot"] for item in first["assignments"]))

    def test_recent_usage_rotates_exploration_but_keeps_baseline(self):
        first_request = replace(self.request(), direction_count=2)
        service = StructureRouterService(repository=FakeRepository())
        first = service.select(first_request).to_dict()
        baseline = first["assignments"][0]["cluster_id"]
        explored = first["assignments"][1]["cluster_id"]

        second_request = replace(
            self.request(),
            direction_count=2,
            capabilities={
                **self.request().capabilities,
                "recent_cluster_usage": {str(explored): 4},
            },
        )
        second = service.select(second_request).to_dict()
        self.assertEqual(second["assignments"][0]["cluster_id"], baseline)
        self.assertNotEqual(second["assignments"][1]["cluster_id"], explored)

    def test_prompt_only_shot_count_stays_unavailable(self):
        result = StructureRouterService(repository=FakeRepository()).select(self.request()).to_dict()
        for item in result["assignments"]:
            self.assertEqual(item["structure_contract"]["hard_constraints"]["shot_count"], "UNAVAILABLE")
            self.assertIn("shot_count", item["structure_contract"]["unknown_constraints"])

    def test_contract_validator_catches_missing_process(self):
        contract = {
            "hard_constraints": {
                "required_beats": ["HOOK", "USE_PROCESS", "PROOF"],
                "content_carrier": "HAND_ONLY",
                "continuity_mode": "MULTI_CUT",
                "shot_count": "UNAVAILABLE",
            }
        }
        script = {
            "storyboard": [
                {"spoken_line_task": "hook", "shot_content": "商品首镜", "person_action": ""},
                {"spoken_line_task": "proof", "shot_content": "材质细节", "person_action": ""},
            ]
        }
        validation = validate_script_against_contract(script, contract)
        self.assertFalse(validation.valid)
        self.assertTrue(any("USE_PROCESS" in item for item in validation.blocking_issues))

    def test_contract_validator_uses_explicit_structure_fields(self):
        contract = {
            "hard_constraints": {
                "beat_sequence": ["HOOK", "USE_PROCESS", "PROOF"],
                "required_beats": ["HOOK", "USE_PROCESS", "PROOF"],
                "content_carrier": "MIXED",
                "continuity_mode": "MULTI_CUT",
                "visual_hook_type": "PRODUCT_REVEAL",
                "shot_count": "UNAVAILABLE",
            }
        }
        script = {
            "storyboard": [
                {
                    "structure_beat": "HOOK",
                    "carrier_mode": "STATIC_PRODUCT",
                    "continuity_group": "C1",
                    "opening_mechanism": "PRODUCT_REVEAL",
                    "shot_content": "商品独立首镜",
                },
                {
                    "structure_beat": "USE_PROCESS",
                    "carrier_mode": "HAND_ONLY",
                    "continuity_group": "C2",
                    "opening_mechanism": "",
                    "shot_content": "手部完成一次使用动作",
                },
                {
                    "structure_beat": "PROOF",
                    "carrier_mode": "WEARER_ACTIVE",
                    "continuity_group": "C3",
                    "opening_mechanism": "",
                    "shot_content": "人物展示使用结果",
                },
            ]
        }
        validation = validate_script_against_contract(script, contract)
        self.assertTrue(validation.valid, validation.to_dict())
        self.assertEqual(validation.observed["beat_authority"], "EXPLICIT")

    def test_forbidden_beats_are_filtered_before_selection(self):
        request = self.request()
        request.capabilities["forbidden_beats"] = ["USE_PROCESS"]
        result = StructureRouterService(repository=FakeRepository()).select(request).to_dict()
        selected = result["assignments"]
        self.assertTrue(selected)
        self.assertTrue(
            all("USE_PROCESS" not in item["structure_contract"]["hard_constraints"]["beat_sequence"] for item in selected)
        )

    def test_static_product_negation_does_not_count_as_person_presence(self):
        contract = {
            "hard_constraints": {
                "beat_sequence": ["HOOK", "PROOF"],
                "required_beats": ["HOOK", "PROOF"],
                "content_carrier": "STATIC_PRODUCT",
                "continuity_mode": "CONTINUOUS_LOW_CUT",
                "visual_hook_type": "PRODUCT_REVEAL",
                "shot_count": "UNAVAILABLE",
            }
        }
        script = {
            "storyboard": [
                {
                    "structure_beat": "HOOK",
                    "carrier_mode": "STATIC_PRODUCT",
                    "continuity_group": "A",
                    "opening_mechanism": "PRODUCT_REVEAL",
                    "shot_content": "商品独立首镜，无人物",
                    "person_action": "无人物，镜头轻推",
                },
                {
                    "structure_beat": "PROOF",
                    "carrier_mode": "STATIC_PRODUCT",
                    "continuity_group": "A",
                    "opening_mechanism": "",
                    "shot_content": "静物细节与角度变化",
                    "person_action": "无人物，商品保持连续变化",
                },
            ]
        }
        validation = validate_script_against_contract(script, contract)
        self.assertTrue(validation.valid, validation.to_dict())

    def test_static_product_fashion_composition_words_do_not_imply_a_person(self):
        contract = {
            "hard_constraints": {
                "beat_sequence": ["HOOK", "PROOF", "ENDING"],
                "required_beats": ["HOOK", "PROOF", "ENDING"],
                "content_carrier": "STATIC_PRODUCT",
                "continuity_mode": "CONTINUOUS_LOW_CUT",
                "visual_hook_type": "PRODUCT_REVEAL",
                "shot_count": "UNAVAILABLE",
            }
        }
        script = {
            "storyboard": [
                {
                    "structure_beat": "HOOK",
                    "carrier_mode": "STATIC_PRODUCT",
                    "continuity_group": "A",
                    "opening_mechanism": "PRODUCT_REVEAL",
                    "shot_content": "外套放在椅背与衣架上，镜前构图强调上身比例",
                    "person_action": "无人物，商品通过机位变化完成揭示",
                },
                {
                    "structure_beat": "PROOF",
                    "carrier_mode": "STATIC_PRODUCT",
                    "continuity_group": "A",
                    "opening_mechanism": "",
                    "shot_content": "半身人台展示上身版型与肩线细节",
                    "person_action": "无人物，无真人出镜",
                },
                {
                    "structure_beat": "ENDING",
                    "carrier_mode": "STATIC_PRODUCT",
                    "continuity_group": "A",
                    "opening_mechanism": "",
                    "shot_content": "完整搭配作为静物定格收尾",
                    "person_action": "不出现模特",
                },
            ]
        }
        validation = validate_script_against_contract(script, contract)
        self.assertTrue(validation.valid, validation.to_dict())
        self.assertFalse(validation.observed["carrier_text_signals"]["person"])

    def test_static_product_ignores_person_words_in_creative_rationale(self):
        contract = {
            "hard_constraints": {
                "beat_sequence": ["HOOK", "PROOF"],
                "required_beats": ["HOOK", "PROOF"],
                "content_carrier": "STATIC_PRODUCT",
                "continuity_mode": "CONTINUOUS_LOW_CUT",
                "visual_hook_type": "PRODUCT_REVEAL",
                "shot_count": "UNAVAILABLE",
            }
        }
        script = {
            "storyboard": [
                {
                    "structure_beat": "HOOK",
                    "carrier_mode": "STATIC_PRODUCT",
                    "continuity_group": "A",
                    "opening_mechanism": "PRODUCT_REVEAL",
                    "shot_content": "无人物静物镜头，商品挂在浅色墙面",
                    "person_action": "无人物，镜头轻推",
                    "shot_purpose": "避免只靠人物身材证明版型",
                    "style_note": "不要让模特抢走商品焦点",
                },
                {
                    "structure_beat": "PROOF",
                    "carrier_mode": "STATIC_PRODUCT",
                    "continuity_group": "A",
                    "opening_mechanism": "",
                    "shot_content": "商品结构细节静物特写",
                    "person_action": "无真人出镜",
                    "shot_purpose": "穿着者结果留到其它方向验证",
                },
            ]
        }
        validation = validate_script_against_contract(script, contract)
        self.assertTrue(validation.valid, validation.to_dict())
        self.assertFalse(validation.observed["carrier_text_signals"]["person"])

    def test_static_product_still_rejects_explicit_real_person_carrier(self):
        contract = {
            "hard_constraints": {
                "beat_sequence": ["HOOK", "PROOF"],
                "required_beats": ["HOOK", "PROOF"],
                "content_carrier": "STATIC_PRODUCT",
                "continuity_mode": "CONTINUOUS_LOW_CUT",
                "visual_hook_type": "PRODUCT_REVEAL",
                "shot_count": "UNAVAILABLE",
            }
        }
        script = {
            "storyboard": [
                {
                    "structure_beat": "HOOK",
                    "carrier_mode": "STATIC_PRODUCT",
                    "continuity_group": "A",
                    "opening_mechanism": "PRODUCT_REVEAL",
                    "shot_content": "真人模特穿着外套进入画面",
                },
                {
                    "structure_beat": "PROOF",
                    "carrier_mode": "STATIC_PRODUCT",
                    "continuity_group": "A",
                    "opening_mechanism": "",
                    "shot_content": "女生转身展示版型",
                },
            ]
        }
        validation = validate_script_against_contract(script, contract)
        self.assertFalse(validation.valid)
        self.assertTrue(validation.observed["carrier_text_signals"]["person"])


if __name__ == "__main__":
    unittest.main()
