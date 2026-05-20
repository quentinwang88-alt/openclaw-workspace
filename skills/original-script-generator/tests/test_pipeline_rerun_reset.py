#!/usr/bin/env python3
"""全流程重跑时的前台输出清空回归测试。"""

from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import unittest


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.pipeline import (  # noqa: E402
    OriginalScriptPipeline,
    STATUS_FAILED_INPUT,
    STATUS_FAILED_JSON,
    STATUS_PENDING_RERUN_ALL,
    STATUS_PENDING_RERUN_SCRIPT,
    STATUS_RUNNING_VALIDATE,
)
from core.json_parser import validate_video_prompt_payload  # noqa: E402
from core.storage import PipelineStorage  # noqa: E402


class PipelineRerunResetTest(unittest.TestCase):
    def test_full_flow_rerun_clear_values_cover_primary_outputs(self) -> None:
        values = OriginalScriptPipeline._build_full_flow_rerun_clear_values()

        required_fields = [
            "output_summary",
            "anchor_card_json",
            "opening_strategy_json",
            "styling_plan_json",
            "three_strategies_json",
            "script_s1_json",
            "script_s1",
            "review_s1_json",
            "video_prompt_s1_json",
            "video_prompt_s1",
            "variant_s1_json",
            "script_1_variant_1",
            "script_s4_json",
            "script_s4",
            "review_s4_json",
            "video_prompt_s4_json",
            "video_prompt_s4",
            "variant_s4_json",
            "script_4_variant_5",
        ]

        for field in required_fields:
            self.assertIn(field, values)
            self.assertEqual(values[field], "")

    def test_task_status_keeps_granular_running_and_failure_values(self) -> None:
        pipeline = OriginalScriptPipeline.__new__(OriginalScriptPipeline)
        pipeline.mapping = {"status": "任务状态"}

        self.assertEqual(pipeline._runtime_status(STATUS_RUNNING_VALIDATE), STATUS_RUNNING_VALIDATE)
        self.assertEqual(pipeline._runtime_status(STATUS_FAILED_INPUT), STATUS_FAILED_INPUT)
        self.assertEqual(pipeline._runtime_status(STATUS_FAILED_JSON), STATUS_FAILED_JSON)

    def test_force_rerun_all_defaults_to_clean_full_flow(self) -> None:
        pipeline = OriginalScriptPipeline.__new__(OriginalScriptPipeline)
        pipeline.resume_from_latest_success = False

        self.assertFalse(pipeline._allow_resume_stages(STATUS_PENDING_RERUN_ALL))
        self.assertTrue(pipeline._should_clear_full_flow_outputs(STATUS_PENDING_RERUN_ALL, False))

    def test_resume_from_latest_success_reuses_force_rerun_all_stages(self) -> None:
        pipeline = OriginalScriptPipeline.__new__(OriginalScriptPipeline)
        pipeline.resume_from_latest_success = True

        self.assertTrue(pipeline._allow_resume_stages(STATUS_PENDING_RERUN_ALL))
        self.assertFalse(pipeline._should_clear_full_flow_outputs(STATUS_PENDING_RERUN_ALL, False))

    def test_script_only_rerun_still_allows_resume(self) -> None:
        pipeline = OriginalScriptPipeline.__new__(OriginalScriptPipeline)
        pipeline.resume_from_latest_success = False

        self.assertTrue(pipeline._allow_resume_stages(STATUS_PENDING_RERUN_SCRIPT))

    def test_stage_image_selection_limits_p1_without_limiting_type_guard_too_much(self) -> None:
        pipeline = OriginalScriptPipeline.__new__(OriginalScriptPipeline)
        image_paths = ["1.jpg", "2.jpg", "3.jpg", "4.jpg", "5.jpg"]

        self.assertEqual(
            pipeline._select_stage_image_paths(image_paths, "发饰", "product_type_guard"),
            image_paths[:4],
        )
        self.assertEqual(
            pipeline._select_stage_image_paths(image_paths, "发饰", "anchor_card"),
            image_paths[:3],
        )
        self.assertEqual(
            pipeline._select_stage_image_paths(image_paths, "围巾帽子套装", "anchor_card"),
            image_paths[:4],
        )

    def test_contract_registry_round_trips_anchor_card(self) -> None:
        anchor_card = {
            "category_execution_contract": {
                "display_family": "hair_accessory",
                "product_subtype": "scrunchie",
                "use_case": "low_bun",
                "placement_zone": "bun_area",
                "hold_scope": "bun",
                "orientation": "wrap_around",
                "primary_visual_result": "低髻佩戴后更完整",
                "operation_policy": "result_first_process_avoid",
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = PipelineStorage(Path(tmpdir) / "test.sqlite3")
            storage.upsert_contract_registry(
                contract_key="contract-key",
                product_image_hash="image-hash",
                normalized_product_type="发饰",
                schema_version="test-v1",
                anchor_card=anchor_card,
                source_run_id=1,
                source_record_id="rec1",
            )

            self.assertEqual(storage.get_contract_registry_anchor_card("contract-key"), anchor_card)

    def test_video_prompt_template_preserves_performance(self) -> None:
        script_json = {
            "script_positioning": {"script_title": "S1", "direction_type": "result", "core_primary_selling_point": "发型更完整"},
            "execution_constraints": {"visual_style": "镜前自然记录"},
            "storyboard": [
                {
                    "shot_no": 1,
                    "duration": "0-3s",
                    "shot_content": "已戴好发圈的低髻近景",
                    "voiceover_text_target_language": "This makes the bun look softer.",
                    "spoken_line_task": "hook",
                    "person_action": "人物轻侧头看镜子",
                    "performance": {
                        "gaze": "mirror",
                        "expression_or_micro_reaction": "嘴角轻轻放松",
                        "body_language": "肩膀放松",
                    },
                    "style_note": "不要回到未戴状态",
                }
            ],
        }

        prompt_json = OriginalScriptPipeline._build_video_prompt_fallback_from_script(script_json)
        self.assertEqual(prompt_json["shot_execution"][0]["performance"]["gaze"], "mirror")

    def test_video_prompt_template_drops_non_chinese_style_note(self) -> None:
        script_json = {
            "script_positioning": {"script_title": "S1"},
            "execution_constraints": {"visual_style": "镜前自然记录"},
            "storyboard": [
                {
                    "shot_no": index,
                    "duration": duration,
                    "shot_content": "已戴好发饰的镜前近景",
                    "voiceover_text_target_language": "This feels simple.",
                    "spoken_line_task": task,
                    "person_action": "人物看向镜子里的发饰位置",
                    "performance": {
                        "gaze": "mirror",
                        "expression_or_micro_reaction": "眼神轻微变亮",
                        "body_language": "肩膀放松",
                    },
                    "style_note": "soft polish / clean_mirror_angle",
                }
                for index, duration, task in (
                    (1, "0-3s", "hook"),
                    (2, "3-6s", "proof"),
                    (3, "6-10s", "proof"),
                    (4, "10-15s", "decision"),
                )
            ],
        }

        prompt_json = OriginalScriptPipeline._build_video_prompt_fallback_from_script(script_json)
        self.assertEqual(prompt_json["shot_execution"][0]["style_note"], "")
        validate_video_prompt_payload(prompt_json)

    def test_resume_stage_falls_back_to_stage_cache_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = PipelineStorage(Path(tmpdir) / "test.sqlite3")
            pipeline = OriginalScriptPipeline.__new__(OriginalScriptPipeline)
            pipeline.storage = storage
            context = {"target_country": "US", "target_language": "English", "product_type": "发圈"}
            cache_key = pipeline._build_stage_cache_key("opening_strategy", context)
            run_id = storage.create_run(
                record_id="rec1",
                product_code="sku1",
                input_hash="old-input-hash",
                context=context,
                raw_record_fields={},
            )
            storage.record_stage_result(
                run_id=run_id,
                record_id="rec1",
                product_code="sku1",
                stage_name="opening_strategy",
                stage_order=1,
                status="success",
                prompt_text="prompt",
                input_context=context,
                image_paths=[],
                output_json={"opening": "cached"},
                cache_key=cache_key,
            )

            self.assertEqual(
                pipeline._load_resume_stage_output(
                    record_id="other-record",
                    product_code="other-sku",
                    input_hash="new-input-hash",
                    stage_name="opening_strategy",
                    input_context=context,
                ),
                {"opening": "cached"},
            )


if __name__ == "__main__":
    unittest.main()
