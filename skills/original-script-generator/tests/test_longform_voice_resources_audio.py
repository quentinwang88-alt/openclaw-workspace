from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

from core.longform.audio import measure_speech_window, plan_semantic_audio_starts, _binary
from core.longform.voiceover_resources import resolve_longform_voiceover_resources
from core.longform.voiceover import build_longform_voiceover_payload
from core.longform.voiceover import build_longform_voiceover_payload, calibrate_longform_voiceover_with_edge
from core.longform.audio import voiceover_text_hash


class LongformResourcesAndAudioTests(unittest.TestCase):
    def test_spoken_context_uses_frozen_person_not_template_candidates(self):
        payload = build_longform_voiceover_payload({
            "production_world": {
                "character": {"identity": "去冷地旅行的分享者"},
                "persona_contract": {"script_projection": {"speaking_personality": "亲切简洁"}},
                "outfit": "白内搭蓝牛仔", "outfit_contract": {"source_outfit_recipe": "不要透出候选"},
            },
        }, {"segments": []})
        self.assertEqual("去冷地旅行的分享者", payload["production_world"]["persona"])
        self.assertEqual("亲切简洁", payload["production_world"]["speaking_personality"])
        self.assertNotIn("source_outfit_recipe", str(payload))

    def test_existing_central_resources_not_short_form_content_budget(self):
        with mock.patch("core.complete_voiceover_direct.load_active_voiceover_hooks", return_value=[{
            "hook_id": "DETAIL_SURPRISE", "core_intent": "发现", "minimal_structure": ["发现", "原因"],
        }]), mock.patch("core.complete_voiceover_direct._approved_style_references", return_value=[{
            "reference_sample_id": "approved", "reference_excerpt": "表达参考",
        }]), mock.patch("core.complete_voiceover_direct._central_native_rhetoric_contract", return_value={
            "status": "AVAILABLE", "resolved_hook_id": "DETAIL_SURPRISE",
            "native_surface_references": [{"text": "sample"}],
        }) as native, mock.patch("core.complete_voiceover_direct.hook_knowledge_snapshot_hash", return_value="snapshot"):
            result = resolve_longform_voiceover_resources({
                "semantic_spine": {"hook_id": "DETAIL_SURPRISE", "selling_argument": {
                    "audience_need_authority": "APPROVED_SELLING_SCENARIO",
                    "expression_policy": {"rhetorical_conflict_allowed": True},
                    "audience_tension": {"text": "已有痛点"},
                }},
                "longform_argument_bundle": {"primary_argument": {
                    "text": "中央主卖点",
                    "expression_policy": "SEMANTIC_AUTHORITY_NOT_VERBATIM",
                }},
                "target_language": "泰语", "target_country": "泰国",
                "source_lineage": {"top_category": "女装", "product_type": "上装"},
            })
        self.assertEqual(result["hook_guidance"]["minimal_structure"], ["发现", "原因"])
        self.assertEqual(native.call_args.kwargs["product_type"], "上装")
        self.assertTrue(native.call_args.kwargs["audience_need_authorized"])
        self.assertTrue(native.call_args.kwargs["rhetorical_conflict_authorized"])
        self.assertEqual(native.call_args.kwargs["audience_tension"], "已有痛点")
        self.assertEqual(len(result["approved_style_references"]), 1)
        self.assertIn("สาวๆ", result["relationship_language"]["audience_addresses"])
        self.assertNotIn("selling_argument", result)

    def test_string_expression_policy_does_not_grant_conflict_permission(self):
        with mock.patch("core.complete_voiceover_direct.load_active_voiceover_hooks", return_value=[]), mock.patch(
            "core.complete_voiceover_direct._approved_style_references", return_value=[]
        ), mock.patch("core.complete_voiceover_direct._central_native_rhetoric_contract", return_value={}) as native, mock.patch(
            "core.complete_voiceover_direct.hook_knowledge_snapshot_hash", return_value="snapshot"
        ):
            resolve_longform_voiceover_resources({
                "target_language": "泰语", "target_country": "泰国",
                "longform_argument_bundle": {"primary_argument": {
                    "expression_policy": "SEMANTIC_AUTHORITY_NOT_VERBATIM",
                }},
            })
        self.assertFalse(native.call_args.kwargs["rhetorical_conflict_authorized"])

    def test_writer_receives_selected_world_not_template_audit(self):
        payload = build_longform_voiceover_payload({
            "production_world": {"outfit": "已选白裤", "scene": "酒店大堂",
                                 "outfit_contract": {"source_outfit_recipe": "黑裙或白裤", "candidate_ids": [1, 2]}},
            "longform_argument_bundle": {"primary_argument": {"text": "价值1"},
                                         "supporting_arguments": [{"text": "价值2"}, {"text": "价值3"}]},
        }, {"segments": []})
        self.assertNotIn("outfit_contract", payload["production_world"])
        self.assertEqual(payload["production_world"]["outfit"], "已选白裤")
        self.assertEqual(len(payload["approved_supporting_arguments"]), 2)

    def test_bounded_timeline_does_not_pack_all_slack_at_end(self):
        starts = plan_semantic_audio_starts([15, 15], [10, 12.8])
        self.assertGreaterEqual(starts[1], 14.25)
        self.assertLess(starts[1], 15)
        self.assertLess(30 - starts[1] - 12.8, 15.35 - 10.35)

    def test_short_and_three_segment_durations_remain_complete(self):
        for durations in ([10, 10], [13, 12], [12, 12, 11], [15, 15, 15]):
            speech = [duration * .94 for duration in durations]
            starts = plan_semantic_audio_starts(list(durations), speech)
            for i in range(1, len(starts)):
                self.assertGreater(starts[i], starts[i-1] + speech[i-1])
            self.assertLess(starts[-1] + speech[-1], sum(durations))
        with self.assertRaises(RuntimeError):
            plan_semantic_audio_starts([10, 10], [11, 11])

    def test_trim_only_outer_silence_keep_internal_pause(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "audio.wav"
            subprocess.run([
                _binary("ffmpeg"), "-v", "error", "-y", "-f", "lavfi", "-i",
                "aevalsrc=if(between(t\\,0.5\\,1.5)+between(t\\,2.0\\,3.0)\\,0.3*sin(2*PI*440*t)\\,0):s=44100:d=3.5",
                str(path),
            ], check=True, capture_output=True)
            result = measure_speech_window(path)
        self.assertEqual(result["trim_status"], "MEASURED")
        self.assertAlmostEqual(result["trim_start_seconds"], .44, delta=.03)
        self.assertAlmostEqual(result["trim_end_seconds"], 3.06, delta=.03)
        # 2s tone + .5s internal pause + .12s edge safety
        self.assertAlmostEqual(result["effective_tts_seconds"], 2.62, delta=.04)

    def test_legacy_frozen_audio_upgrades_locally_without_second_revision(self):
        with tempfile.TemporaryDirectory() as directory:
            asset = Path(directory) / "old.mp3"
            asset.write_bytes(b"fixture")
            sections = [{"segment_id": "A", "target_text": "old"}]
            original = {"semantic_sections": sections, "tts_preflight": {
                "voice_id": "voice", "revision_attempted": True, "revision_limit": 1,
                "sections": [{"segment_id": "A", "audio_path": str(asset),
                              "text_sha256": voiceover_text_hash("old"),
                              "planned_segment_seconds": 10, "actual_tts_seconds": 9.8}],
            }}
            with mock.patch("core.longform.voiceover.measure_speech_window", return_value={
                "trim_version": "edge-boundary-silence-v1", "effective_tts_seconds": 9.3,
                "raw_tts_seconds": 9.8, "trim_start_seconds": .2, "trim_end_seconds": 9.5,
                "trim_status": "MEASURED",
            }), mock.patch("core.longform.voiceover.synthesize_segment_preflight") as synth, mock.patch(
                "core.longform.voiceover._invoke_voiceover_model"
            ) as model:
                result = calibrate_longform_voiceover_with_edge({}, {
                    "segments": [{"segment_id": "A", "duration_seconds": 10}],
                }, original, directory, voice_id="voice")
            synth.assert_not_called()
            model.assert_not_called()
            self.assertEqual(result["tts_preflight"]["actual_tts_seconds_total"], 9.3)
            self.assertTrue(result["tts_preflight"]["revision_attempted"])


if __name__ == "__main__":
    unittest.main()
