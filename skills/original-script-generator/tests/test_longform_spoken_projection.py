"""Longform spoken input stays independent of shot explanations."""
from copy import deepcopy
import unittest
from unittest.mock import patch

from core.longform.voiceover import (
    build_longform_voiceover_payload, run_longform_voiceover,
    calibrate_longform_voiceover_with_edge,
)


class LongformSpokenProjectionTests(unittest.TestCase):
    def fixture(self):
        master = {
            "target_language": "泰语", "target_country": "泰国",
            "target_duration_seconds": 30,
            "semantic_spine": {
                "hook_id": "AUDIENCE_NEED_CALLOUT",
                "primary_narrative_context": "到寒冷地区旅行，需要叠穿的人",
                "core_buying_reason": "叠穿方便又不压比例",
            },
            "longform_argument_bundle": {
                "primary_argument": {"argument_id": "ARG1", "text": "旅行叠穿"},
                "supporting_arguments": [{"argument_id": "ARG2", "text": "上镜"}],
            },
            "verified_facts": [{"claim_key": "F1", "value": "短款"}],
            "production_world": {"scene": "酒店房间", "outfit": "白色内搭", "character": {"identity": "旅行分享者"}},
        }
        plan = {"segments": [{
            "segment_id": sid, "duration_seconds": 15, "scene_id": f"SCENE_{sid}",
            "scene_block": {"location": location, "narrative_role": "不要透传逐镜论证角色"},
            "capture_units": [{"unit_id": sid + "1", "beat": "PROOF", "information_gain": "不要透传逐镜信息增量"}],
        } for sid, location in (("A", "酒店房间"), ("B", "候车区"))]}
        return master, plan

    def test_projection_keeps_meaning_timing_and_background_not_shot_explanations(self):
        master, plan = self.fixture()
        before = deepcopy(plan)
        payload = build_longform_voiceover_payload(master, plan)
        self.assertEqual(payload["primary_narrative_context"], master["semantic_spine"]["primary_narrative_context"])
        self.assertEqual(payload["core_buying_reason"], "叠穿方便又不压比例")
        self.assertEqual(payload["approved_supporting_arguments"], master["longform_argument_bundle"]["supporting_arguments"])
        self.assertEqual(payload["verified_facts"], master["verified_facts"])
        self.assertEqual([x["scene_location"] for x in payload["semantic_sections"]], ["酒店房间", "候车区"])
        self.assertEqual([x["target_spoken_seconds_range"] for x in payload["semantic_sections"]], [[11.2, 14.4]] * 2)
        for key in ("capture_units", "information_gain", "scene_narrative_role", "不要透传"):
            self.assertNotIn(key, str(payload))
        self.assertEqual(plan, before, "Full visual audit must remain unchanged")

    def test_projection_does_not_require_visual_capture_units(self):
        master, plan = self.fixture()
        for segment in plan["segments"]:
            segment.pop("capture_units")
        self.assertEqual(len(build_longform_voiceover_payload(master, plan)["semantic_sections"]), 2)

    def test_actual_request_keeps_central_resources_and_uses_one_call(self):
        master, plan = self.fixture()
        resources = {
            "hook_guidance": {"core_intent": "回应旅行人群需求"},
            "relationship_language": {"audience_addresses": ["สาวๆ"]},
            "approved_style_references": [{"reference_sample_id": "APPROVED"}],
            "native_rhetoric_contract": {"native_surface_references": [{"text": "原生示例"}]},
        }
        generated = {
            "target_text": "ไปเที่ยวค่ะ ใส่สบาย", "chinese_translation": "旅游穿搭",
            "estimated_seconds": 3.0, "semantic_sections": [
                {"segment_id": "A", "target_text": "ไปเที่ยวค่ะ"},
                {"segment_id": "B", "target_text": "ใส่สบาย"},
            ],
        }
        with patch("core.longform.voiceover.resolve_longform_voiceover_resources", return_value=resources), patch(
            "core.longform.voiceover._invoke_voiceover_model", return_value=generated
        ) as invoke:
            result = run_longform_voiceover(master, plan)
        invoke.assert_called_once()
        sent = invoke.call_args.args[0]["payload"]
        for key, value in resources.items():
            self.assertEqual(sent[key], value)
        self.assertNotIn("capture_units", str(sent))
        self.assertEqual(result["central_resource_snapshot"], resources)
        self.assertEqual(result["duration_fit"]["revision_limit"], 0)

    def test_duration_revision_reuses_resources_and_reduced_input(self):
        master, plan = self.fixture()
        resources = {
            "hook_guidance": {"core_intent": "已冻结钩子"},
            "relationship_language": {"audience_addresses": ["สาวๆ"]},
            "approved_style_references": [{"reference_sample_id": "FROZEN"}],
            "native_rhetoric_contract": {"status": "AVAILABLE"},
        }
        original = {"central_resource_snapshot": resources, "semantic_sections": [
            {"segment_id": sid, "target_text": "短稿"} for sid in ("A", "B")
        ]}
        revised = {"semantic_sections": [
            {"segment_id": sid, "target_text": "补充同一理由的完整表达"} for sid in ("A", "B")
        ]}
        short_measurement = {
            "sections": [{"segment_id": sid, "status": "TOO_SHORT", "coverage_ratio": .5} for sid in ("A", "B")],
            "actual_tts_seconds_total": 15, "all_acceptable": False,
        }
        fit_measurement = {
            "sections": [{"segment_id": sid, "status": "TARGET_FIT", "coverage_ratio": .93} for sid in ("A", "B")],
            "actual_tts_seconds_total": 27.9, "all_acceptable": True,
        }
        with patch("core.longform.voiceover.resolve_longform_voiceover_resources") as resolve, patch(
            "core.longform.voiceover.synthesize_segment_preflight", side_effect=[short_measurement, fit_measurement]
        ) as synth, patch("core.longform.voiceover._invoke_voiceover_model", return_value=revised) as invoke:
            result = calibrate_longform_voiceover_with_edge(master, plan, original, "/tmp/unused-offline", voice_id="test")
        resolve.assert_not_called()
        invoke.assert_called_once()
        self.assertEqual(synth.call_count, 2)
        payload = invoke.call_args.args[0]["payload"]
        self.assertNotIn("capture_units", str(payload))
        self.assertNotIn("scene_narrative_role", str(payload))
        self.assertEqual(payload["actual_tts_revision"]["revision_limit"], 1)
        for key, value in resources.items():
            self.assertEqual(payload[key], value)
        self.assertTrue(result["tts_preflight"]["revision_selected"])
        self.assertEqual(result["central_resource_snapshot"], resources)


if __name__ == "__main__":
    unittest.main()
