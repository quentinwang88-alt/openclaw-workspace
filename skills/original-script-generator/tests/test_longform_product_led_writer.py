"""Routing isolation and authored-action preservation; no remote calls."""
import ast
import copy
import inspect
from pathlib import Path
import unittest
from unittest.mock import patch

from core.longform.feishu_workbench import build_longform_text_batch
from core.longform.keyframes import build_keyframe_contracts
from core.longform.model import (
    BLUEPRINT_PROMPT_VERSION, DEFAULT_BLUEPRINT_MODEL,
    build_master_script_prompt, generate_master_contract,
)
from core.longform.planner import _execution_units, _segment_prompt, compile_longform_plan
from tests.test_longform_original import fixture
from tests.test_openclaw_original_script_task import adapter


class ProductLedWriterTests(unittest.TestCase):
    def test_author_default_and_authority_are_recorded(self):
        source = fixture(30)
        raw = copy.deepcopy(source)
        raw["product_code"] = "MODEL_INVENTED"
        raw["generation_provenance"] = {"model": "invented"}
        with patch("core.longform.model.OriginalScriptLLMClient") as client:
            client.return_value.call_json.return_value = raw
            result = generate_master_contract(source)
        self.assertEqual(client.call_args.kwargs["primary_model"], "gpt-6-astra")
        self.assertEqual(client.call_args.kwargs["primary_reasoning_effort"], "high")
        self.assertEqual(client.call_args.kwargs["primary_cli_binary"],
                         "/Applications/ChatGPT.app/Contents/Resources/codex")
        self.assertEqual(result["product_code"], source["product_code"])
        self.assertEqual(result["generation_provenance"]["prompt_version"], BLUEPRINT_PROMPT_VERSION)
        self.assertEqual(result["generation_provenance"]["model"], DEFAULT_BLUEPRINT_MODEL)
        self.assertEqual(inspect.signature(build_longform_text_batch).parameters["blueprint_model"].default,
                         DEFAULT_BLUEPRINT_MODEL)

    def test_operator_explicit_override_and_shortform_isolation(self):
        command = adapter.build_runner_command(action="run", limit=1,
                                               blueprint_model=DEFAULT_BLUEPRINT_MODEL)
        self.assertIn(DEFAULT_BLUEPRINT_MODEL, command)
        root = Path(__file__).resolve().parents[1]
        tree = ast.parse((root / "scripts/run_feishu_operation_tasks.py").read_text())
        calls = {node.func.id: node for node in ast.walk(tree)
                 if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                 and node.func.id in {"build_longform_text_batch", "run_script_only"}}
        for fn, expected in (("build_longform_text_batch", "LONGFORM_BLUEPRINT_MODEL"),
                             ("run_script_only", "'gpt-5.6-sol'")):
            value = next(k.value for k in calls[fn].keywords if k.arg == "blueprint_model")
            self.assertEqual(ast.unparse(value), "args.blueprint_model or " + expected)

    def test_connection_shot_is_not_forced_to_invent_proof(self):
        prompt = build_master_script_prompt(fixture(30))
        self.assertIn("不要求每镜都证明卖点", prompt)
        self.assertIn("不限定为微动作", prompt)
        self.assertNotIn("每个单元必须有新的可见信息", prompt)
        self.assertNotIn("两种情况都不得重新穿戴", prompt)

    def test_detail_fallback_preserves_action_and_visual(self):
        units = fixture(30)["capture_units"][:3]
        units[0].update(camera="固定中景", visual_content="外套敞开，露出已选针织内搭",
                        character_action="抬起前襟展示内搭，随后自然放下")
        before = copy.deepcopy(units)
        for mode in ("CONTINUOUS", "DISCONTINUOUS_CUT"):
            projected = _execution_units(units, segment_role="DETAIL_AND_REAL_USE",
                                         detail_focus=["按扣"], incoming_boundary_mode=mode)
            self.assertIn(units[0]["visual_content"], projected[0]["visual_content"])
            self.assertEqual(units[0]["character_action"], projected[0]["character_action"])
            self.assertNotIn("小幅姿态调整", str(projected))
        self.assertEqual(units, before)

    def test_cut_state_reaches_video_and_entry_frame(self):
        master = fixture(30)
        for index, unit in enumerate(master["capture_units"]):
            unit["segment_id"] = "A" if index < 4 else "B"
        master["capture_units"][4].update(
            visual_content="前襟敞开，露出已选内搭", character_action="手自然放下，内搭保持可见",
            camera="商品近景")
        plan = compile_longform_plan(master)
        self.assertEqual(plan["segments"][1]["frame_contract"]["incoming_boundary_mode"], "DISCONTINUOUS_CUT")
        prompt = plan["segments"][1]["video_prompt"]
        self.assertIn("前襟敞开", prompt)
        self.assertNotIn("已完成穿戴状态不变", prompt)
        frame = build_keyframe_contracts(master, plan)["SB_ENTRY"]["prompt"]
        self.assertIn("前襟敞开", frame)
        self.assertIn("不照搬K0的敞合状态", frame)

    def test_continuous_boundary_still_preserves_actual_state(self):
        prompt = _segment_prompt(
            fixture(30), "B", 15, fixture(30)["capture_units"][:3],
            is_first=False, is_final=True, start_bridge={"product_wear_state": "前襟敞开"},
            end_bridge={}, scene_block={}, incoming_boundary_mode="CONTINUOUS",
            outgoing_boundary_mode="NONE", entry_frame_role="ACTUAL_TAIL", segment_visual_role="REAL_USE")
        self.assertIn("上一片段实际尾帧", prompt)
        self.assertIn("不得重新开场、重新穿戴、重新系结", prompt)
        self.assertIn("前襟敞开", prompt)


if __name__ == "__main__":
    unittest.main()
