from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from scripts import run_reality_reference_stage0 as stage0


class RealityReferenceStage0RunnerTests(unittest.TestCase):
    def test_direct_share_releases_only_legacy_macro_action_quota(self) -> None:
        blueprint = {
            "schema_version": "complete-script-blueprint-v4-carrier",
            "diversity_contract_id": "CDV_1",
            "presentation_mode": "PERSON_ON_CAMERA",
            "creative_thesis": "分享外套",
            "creator_motivation": "愿意直接分享",
            "viewer_relationship": "像朋友分享",
            "persona": {
                "identity": "创作者", "age_presence": "年轻成年人",
                "appearance": "自然", "hair_makeup": "日常妆发",
                "styling": "日常穿搭", "speaking_personality": "自然",
                "performance_intensity": "轻",
            },
            "scene": {
                "location": "咖啡厅窗边", "moment": "下午",
                "lighting": "自然光", "background": "简洁",
                "camera_setup": "固定手机", "why_this_scene": "日常分享",
            },
            "performance_flow": {
                "entry_state": "已穿好", "behavior_motivation": "直接分享",
                "ending_state": "自然说完",
            },
            "retention_hook": {
                "opening_event": "整体入镜", "delayed_answer": "后续看细节",
                "payoff_time": "3-5s",
            },
            "event_design": {
                "event_motif": "直接分享", "start_state": "已穿好",
                "natural_event": "面对手机说话", "core_result_moment": "细节清楚",
                "end_state": "自然说完",
            },
            "macro_visual_passages": [
                {
                    "passage_no": index, "narrative_role": role,
                    "visible_process": "直接分享", "observable_action": "",
                    "camera_observation": "普通手机", "product_visibility": "FULL",
                    "supported_claim_keys": [],
                }
                for index, role in enumerate(("HOOK", "PROOF", "PROOF"), 1)
            ],
            "visual_language": {
                "image_texture": "手机实拍", "camera_behavior": "稳定",
                "framing_bias": "中景", "editing_rhythm": "直接剪切",
                "anti_template_rules": ["不表演"],
            },
            "voice_identity": {
                "tone": "自然", "relationship_mode": "朋友分享",
                "particle_density": "适量", "sales_pressure": "低",
                "forbidden_tone": "广告腔",
            },
            "audio_direction": {
                "bgm_style": "轻", "environment_sound": "自然",
                "voiceover_priority": "口播优先",
            },
        }
        blueprint = stage0.attach_field_consumers(blueprint)
        contract = {
            "contract_id": "CDV_1",
            "viewer_relationship": "像朋友分享",
            "required_presentation_mode": "PERSON_ON_CAMERA",
            "scene_motif": "咖啡厅窗边",
        }
        validation = stage0._validate_blueprint_for_recording_profile(
            blueprint,
            contract,
            {"enabled": True},
        )
        self.assertTrue(validation["valid"], validation["issues"])

    def test_formal_voiceover_defaults_to_sol_high_wrapper(self) -> None:
        self.assertIn("codex_model_command.py", stage0.DEFAULT_VOICEOVER_MODEL_COMMAND)

    def test_auto_selection_skips_long_candidate(self) -> None:
        selected = stage0._first_auto_selectable_candidate(
            [
                {
                    "candidate_id": "VOC_1_LONG",
                    "selection_readiness": {"auto_selectable": False},
                },
                {
                    "candidate_id": "VOC_2_READY",
                    "selection_readiness": {"auto_selectable": True},
                },
            ]
        )
        self.assertEqual("VOC_2_READY", selected["candidate_id"])

    def test_legacy_candidate_remains_selectable(self) -> None:
        self.assertTrue(stage0._candidate_is_auto_selectable({"candidate_id": "VOC_LEGACY"}))

    def test_stale_local_candidate_is_never_formally_selectable(self) -> None:
        self.assertFalse(
            stage0._candidate_is_auto_selectable(
                {
                    "candidate_id": "VOC_LOCAL_OLD",
                    "copy_generation_mode": "LOCAL_DETERMINISTIC",
                    "selection_readiness": {"auto_selectable": True},
                }
            )
        )

    def test_rendered_stage0_script_prioritizes_visible_clip_video_brief(self) -> None:
        rendered = stage0._render_storyboard(
            {
                "video_generation_brief": {
                    "production_design": {
                        "character_setting": {"identity": "日常穿搭创作者"},
                        "scene_setting": {"location": "窗边"},
                        "outfit_setting": {"styling": "浅色内搭和黑色外套"},
                    },
                    "capture_units": [
                        {
                            "capture_unit_id": "CU_01",
                            "shot_numbers": [1],
                            "structure_role": "HOOK",
                            "observable_change_job": "OPENING_ATTENTION_CHANGE",
                            "framing_guidance": "独立录制开场",
                        }
                    ],
                    "storyboard": [
                        {
                            "shot_no": 1,
                            "shot_content": "前襟近景",
                            "observable_action": "扣好衣服",
                        }
                    ],
                    "instruction": "按可见素材片段直接剪切，不使用数字裁切。",
                    "voiceover": {"target_language": "ข้อความ"},
                    "product_identity_lock": {
                        "must_preserve": ["近黑色短款外套", "圆领", "五颗前襟纽扣", "多余锚点"],
                        "must_not_change": [
                            "禁止双排扣",
                            "禁止改变扣子数量",
                            "禁止替换商品",
                            "多余负向",
                        ],
                    },
                },
                "storyboard": [
                    {
                        "shot_no": 1,
                        "duration": "0-3s",
                        "carrier_mode": "WEARER_ACTIVE",
                        "structure_beat": "HOOK",
                        "shot_content": "前襟近景",
                        "observable_action": "扣好衣服",
                    }
                ],
            }
        )
        self.assertLess(rendered.index("【视频模型主输入"), rendered.index("【内部结构槽位"))
        primary = rendered.split("【制作设定与内部证据", 1)[0]
        self.assertIn("可见素材片段", primary)
        self.assertIn("动作：扣好衣服", primary)
        self.assertIn("执行重点：按可见素材片段直接剪切", primary)
        self.assertIn("商品锁：参考图商品为最高权威", primary)
        self.assertIn("五颗前襟纽扣", primary)
        self.assertIn("禁止双排扣", primary)
        self.assertNotIn("多余锚点", primary)
        self.assertNotIn("多余负向", primary)

    def test_rendered_storyboard_does_not_label_voiceover_continuation_as_silence(self) -> None:
        rendered = stage0._render_storyboard(
            {
                "storyboard": [
                    {
                        "shot_no": 2,
                        "duration": "2-4s",
                        "carrier_mode": "WEARER_ACTIVE",
                        "structure_beat": "PROOF",
                        "audio_actual": "VOICEOVER_CONTINUATION",
                    }
                ]
            }
        )
        self.assertIn("承接前一语义段的连续口播", rendered)
        self.assertNotIn("（静默）", rendered)

    def test_selected_voiceover_candidate_reuses_exact_matching_snapshot(self) -> None:
        direction = {
            "output_slot": "S1",
            "execution_card_id": "EXEC_1",
            "content_bundle_brief": {"content_bundle_id": "CBR_1"},
            "creative_blueprint": {"creative_blueprint_id": "CBP_1"},
        }
        snapshot = {
            "products": [
                {
                    "product_code": "P1",
                    "directions": [
                        {
                            **direction,
                            "voiceover_candidates": [
                                {"candidate_id": "VOC_1_PAIN_REFRAME", "lines": [{"text": "exact"}]},
                                {"candidate_id": "VOC_2_NEED", "lines": [{"text": "other"}]},
                            ],
                        }
                    ],
                }
            ]
        }
        candidates = stage0._cached_voiceover_candidates(
            snapshot,
            product_code="P1",
            direction_result=direction,
            selected_candidate_id="VOC_1_PAIN_REFRAME",
        )
        self.assertEqual("exact", candidates[0]["lines"][0]["text"])
        candidates[0]["lines"][0]["text"] = "mutated"
        self.assertEqual(
            "exact",
            snapshot["products"][0]["directions"][0]["voiceover_candidates"][0]["lines"][0]["text"],
        )

    def test_batch_payload_marks_partial_direction_run(self) -> None:
        payload = stage0._build_batch_payload(
            preview_only=False,
            skip_voiceover=False,
            requested_products=["P1"],
            results=[
                {
                    "product_code": "P1",
                    "status": "PARTIAL",
                    "directions": [{"output_slot": "S1"}],
                    "direction_errors": [{"output_slot": "S4"}],
                }
            ],
            product_errors=[],
        )
        self.assertEqual("PARTIAL", payload["batch_status"])
        self.assertEqual(1, payload["partial_product_count"])

    def test_run_product_continues_after_one_direction_fails(self) -> None:
        storage = MagicMock()
        storage.create_run.return_value = 71
        storage.get_latest_stage_output_json.return_value = None
        creative_storage = MagicMock()
        creative_storage.list_recent_creative_patterns.return_value = []
        creative_storage.reserve_creative_pattern.side_effect = ["U1", "U2"]
        directions = [
            {
                "output_slot": slot,
                "direction_assignment_id": f"D_{slot}",
                "cluster_id": index,
                "cluster_version": "v1",
                "execution_reference": {
                    "execution_card_id": f"E_{slot}",
                    "source_profile_id": f"SP_{slot}",
                    "source_video_id": f"V_{slot}",
                    "selection_score": 90,
                },
                "reference_selection": {},
                "structure_execution_plan": {},
                "p2_lite": {},
                "content_bundle_brief": {},
            }
            for index, slot in enumerate(("S1", "S2"), 1)
        ]

        class FakeLLM:
            calls = 0

            def call_json(self, *args, **kwargs):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("first direction failed")
                if self.calls == 2:
                    return {}
                return {"shots": []}

        context = {
            "source_run_id": 1,
            "record_id": "R1",
            "product_code": "P1",
            "top_category": "女装",
            "target_country": "泰国",
            "target_language": "泰语",
            "product_type": "外套",
            "anchor_card": {},
            "structure_selection": {"data_snapshot_hash": "H"},
            "baselines": {},
        }
        contract = {
            "contract_id": "C1",
            "viewer_relationship": "像朋友一样分享",
            "scene_motif": "玄关墙面",
            "opening_action": "停下看前襟",
            "forbidden_recent_patterns": ["旧模板"],
        }
        with patch.object(stage0, "_latest_product_context", return_value=context), patch.object(
            stage0,
            "build_reality_direction_packages",
            return_value={"status": "READY", "selected_count": 2, "directions": directions},
        ), patch.object(stage0, "build_creative_diversity_contract", return_value=contract), patch.object(
            stage0, "creative_usage_row", return_value={"usage_id": "unused"}
        ), patch.object(stage0, "_record_stage"), patch.object(
            stage0, "build_complete_script_blueprint_prompt", return_value="blueprint"
        ), patch.object(stage0, "build_visual_adaptation_prompt", return_value="visual"), patch.object(
            stage0, "validate_complete_blueprint", return_value={"valid": True, "issues": []}
        ), patch.object(
            stage0, "project_event_blueprint_to_visual_plan", return_value={"shots": []}
        ), patch.object(stage0, "validate_visual_adaptation", return_value={"valid": True, "issues": []}), patch.object(
            stage0,
            "assemble_reality_script",
            return_value={"authenticity_review": {}, "storyboard": []},
        ), patch.object(
            stage0,
            "validate_complete_script",
            return_value={"valid": True, "issues": []},
        ):
            result = stage0.run_product(
                storage,
                creative_storage,
                FakeLLM(),
                product_code="P1",
                directions=2,
                preview_only=False,
                skip_voiceover=True,
                voiceover_root="",
                voiceover_db_path="",
            )

        self.assertEqual("PARTIAL", result["status"])
        self.assertEqual(["S2"], [item["output_slot"] for item in result["directions"]])
        self.assertEqual(["S1"], [item["output_slot"] for item in result["direction_errors"]])
        storage.update_run_status.assert_called_once()

    def test_main_continues_after_one_product_fails_and_writes_partial_result(self) -> None:
        storage = MagicMock()
        creative_storage = MagicMock()
        storage.query_runs_by_product_code.return_value = []
        success = {
            "product_code": "P2",
            "stage0_run_id": 22,
            "status": "COMPLETED",
            "selected_count": 1,
            "completed_count": 1,
            "failed_count": 0,
            "directions": [{"output_slot": "S1", "execution_card_id": "E1", "source_video_id": "V1"}],
            "baselines": {},
        }
        with tempfile.TemporaryDirectory() as temp_dir, patch.object(
            stage0, "PipelineStorage", side_effect=[storage, creative_storage]
        ), patch.object(
            stage0, "run_product", side_effect=[RuntimeError("P1 boom"), success]
        ), patch.object(
            sys,
            "argv",
            [
                "run_reality_reference_stage0.py",
                "--product-code",
                "P1",
                "--product-code",
                "P2",
                "--preview-only",
                "--output-dir",
                temp_dir,
            ],
        ):
            exit_code = stage0.main()
            payload = json.loads((Path(temp_dir) / "stage0_result.json").read_text(encoding="utf-8"))

        self.assertEqual(0, exit_code)
        self.assertEqual(2, payload["processed_product_count"])
        self.assertEqual(1, payload["failed_product_count"])
        self.assertEqual(1, payload["completed_product_count"])
        self.assertEqual(["P1", "P2"], [item["product_code"] for item in payload["products"]])


if __name__ == "__main__":
    unittest.main()
