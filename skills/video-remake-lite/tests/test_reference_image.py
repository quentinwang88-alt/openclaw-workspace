#!/usr/bin/env python3
"""Visual reference workflow contracts without external model calls."""

from pathlib import Path
from types import SimpleNamespace
import tempfile
import sys
import unittest
from unittest.mock import patch
from PIL import Image


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.reference_image import (  # noqa: E402
    APPROVAL_AUTO,
    APPROVAL_MANUAL,
    INPUT_MODE_SCRIPT_CLOTHING,
    ReferenceImageWorkflow,
    STATUS_CONFIRMED,
    STATUS_PENDING_APPROVAL,
    apply_visual_lock_to_video_prompt,
)


MAPPING = {
    "reference_requirements": "参考要求",
    "reference_revision_notes": "参考图修改意见",
    "reference_approval_mode": "参考图审批模式",
    "person_reference_images": "人物参考图",
    "clothing_reference_images": "服装参考图",
    "scene_reference_images": "场景参考图",
    "reference_images": "复刻参考图",
    "ai_reference_image": "AI视频参考图",
}


class FakeClient:
    def upload_attachment(self, *, content, file_name, content_type, size):
        return {"file_token": "generated_1", "name": file_name}


class FakeLLM:
    def __init__(self, conflict="无明显冲突"):
        self.conflict = conflict

    def build_reference_image_spec(self, **_kwargs):
        return {
            "视觉锁定卡": "自然人物，生活场景，动作刚开始",
            "AI参考图提示词": "生成9:16真实手机视频首帧",
            "参考冲突": self.conflict,
        }

    def evaluate_reference_image(self, **_kwargs):
        return {"结论": "通过", "总分": 92, "硬性问题": [], "通过": True}

    def extract_character_appearance_anchor(self, _source_images):
        return "暖棕肤色，偏长椭圆脸，浓弧眉，深棕微卷披肩发。"


class RetryLLM(FakeLLM):
    def __init__(self):
        super().__init__()
        self.calls = 0

    def evaluate_reference_image(self, **_kwargs):
        self.calls += 1
        if self.calls == 1:
            return {
                "结论": "不通过",
                "总分": 60,
                "硬性问题": ["服装结构不完整"],
                "重做指令": "重做时严格保留服装结构。",
                "通过": False,
            }
        return {"结论": "通过", "总分": 92, "硬性问题": [], "通过": True}


class LocalWorkflow(ReferenceImageWorkflow):
    def _run_image2(self, *, output_dir, **_kwargs):
        path = output_dir / "generated.png"
        path.write_bytes(b"png")
        return path


class CapturingWorkflow(LocalWorkflow):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.reference_path_calls = []

    def _run_image2(self, *, reference_paths, output_dir, **_kwargs):
        self.reference_path_calls.append([Path(path) for path in reference_paths])
        path = output_dir / f"generated_{len(self.reference_path_calls)}.png"
        path.write_bytes(b"png")
        return path


class ReferenceImageWorkflowTest(unittest.TestCase):
    def _workflow(self, work_dir):
        workflow = LocalWorkflow(client=FakeClient(), llm_client=FakeLLM(), mapping=MAPPING)
        workflow.work_dir = Path(work_dir)
        return workflow

    def test_auto_mode_confirms_qa_passed_image(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            workflow = self._workflow(temp_dir)
            result = workflow.generate(
                fields={"参考图审批模式": APPROVAL_AUTO},
                outputs={"复刻卡": "card", "最终复刻视频提示词": "prompt"},
                context={},
                source_frame_paths=[],
                task_label="266",
                version=1,
            )

        self.assertEqual(result.status, STATUS_CONFIRMED)
        self.assertTrue(result.sync_ready)

    def test_user_requirements_force_manual_confirmation(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            workflow = self._workflow(temp_dir)
            result = workflow.generate(
                fields={"参考图审批模式": APPROVAL_AUTO, "参考要求": "人物穿白色针织衫"},
                outputs={"复刻卡": "card", "最终复刻视频提示词": "prompt"},
                context={},
                source_frame_paths=[],
                task_label="267",
                version=1,
            )

        self.assertEqual(result.approval_mode, APPROVAL_MANUAL)
        self.assertEqual(result.status, STATUS_PENDING_APPROVAL)
        self.assertFalse(result.sync_ready)

    def test_image2_parser_accepts_progress_text_before_json(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            workflow = ReferenceImageWorkflow(client=FakeClient(), llm_client=FakeLLM(), mapping=MAPPING)
            workflow.image_skill = Path(temp_dir) / "run_pipeline.py"
            workflow.image_skill.write_text("# fixture", encoding="utf-8")
            output_path = Path(temp_dir) / "result.png"
            output_path.write_bytes(b"png")
            raw_output = (
                "[openai-image] generating\\n"
                '{"status":"success","output_image_paths":["' + str(output_path) + '"]}'
            )
            with patch(
                "core.reference_image.subprocess.run",
                return_value=SimpleNamespace(returncode=0, stdout=raw_output, stderr=""),
            ):
                result = workflow._run_image2(
                    task_label="fixture",
                    version=1,
                    prompt="prompt",
                    reference_paths=[],
                    output_dir=Path(temp_dir),
                )

        self.assertEqual(result, output_path)

    def test_canvas_guard_rejects_letterboxed_reference_image(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            image_path = Path(temp_dir) / "letterboxed.png"
            image = Image.new("RGB", (100, 200), "black")
            for y in range(30, 170):
                for x in range(100):
                    image.putpixel((x, y), (160, 110, 80))
            image.save(image_path)
            qa = ReferenceImageWorkflow._apply_canvas_quality_guard(
                image_path,
                {"结论": "通过", "总分": 92, "硬性问题": [], "通过": True},
            )

        self.assertFalse(qa["通过"])
        self.assertIn("黑边", qa["硬性问题"][0])

    def test_resolved_priority_note_is_not_a_reference_conflict(self):
        self.assertFalse(
            ReferenceImageWorkflow._has_unresolved_conflict(
                "复刻卡要求保留黑边，但已按本轮修改意见执行，不保留黑边。"
            )
        )
        self.assertTrue(
            ReferenceImageWorkflow._has_unresolved_conflict(
                "人物参考图要求长发，服装参考图要求同一人物短发，无法同时满足。"
            )
        )

    def test_visual_lock_is_written_into_downstream_video_prompt(self):
        prompt = apply_visual_lock_to_video_prompt(
            "【生成硬设置】\n上下黑边，横向居中，年轻女性穿红绿球衣。",
            "人物外观指纹：长深棕色披肩发。服装结构指纹：立领绿色织带、三粒门襟、袖口金绿几何织带、两侧绿色弧形拼片。",
        )

        self.assertTrue(prompt.startswith("【人物与造型硬锁（参考图优先）】"))
        self.assertIn("三粒门襟", prompt)
        self.assertNotIn("上下黑边", prompt)

    def test_script_and_clothing_mode_excludes_person_and_source_images(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            workflow = self._workflow(temp_dir)
            clothing = Path(temp_dir) / "clothing.png"
            clothing.write_bytes(b"png")
            workflow._download_field_attachments = lambda *_args, **_kwargs: [(clothing, "服装参考图")]
            result = workflow._materialize_reference_inputs(
                fields={"服装参考图": [{"file_token": "clothing_1"}]},
                task_dir=Path(temp_dir),
                source_frame_paths=[Path(temp_dir) / "source.jpg"],
                include_previous_ai=True,
                input_mode=INPUT_MODE_SCRIPT_CLOTHING,
            )

        self.assertEqual(result, [(clothing, "服装参考图")])

    def test_script_and_clothing_retry_does_not_send_previous_ai_image(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            clothing = Path(temp_dir) / "clothing.png"
            clothing.write_bytes(b"png")
            workflow = CapturingWorkflow(
                client=FakeClient(),
                llm_client=RetryLLM(),
                mapping={**MAPPING, "reference_input_mode": "AI参考图素材模式"},
            )
            workflow.work_dir = Path(temp_dir)
            workflow._download_field_attachments = lambda *_args, **_kwargs: [(clothing, "服装参考图")]
            workflow.generate(
                fields={
                    "参考图审批模式": APPROVAL_AUTO,
                    "AI参考图素材模式": INPUT_MODE_SCRIPT_CLOTHING,
                    "服装参考图": [{"file_token": "clothing_1"}],
                },
                outputs={"复刻卡": "card", "最终复刻视频提示词": "prompt"},
                context={},
                source_frame_paths=[Path(temp_dir) / "source.jpg"],
                task_label="268",
                version=1,
            )

        self.assertEqual(workflow.reference_path_calls, [[clothing], [clothing]])


if __name__ == "__main__":
    unittest.main()
