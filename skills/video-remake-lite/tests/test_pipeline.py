#!/usr/bin/env python3
"""Pipeline integration contracts without external Feishu or model calls."""

from pathlib import Path
import sys
import tempfile
import unittest


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.bitable import RemakeRecord  # noqa: E402
from core.llm_client import (  # noqa: E402
    VideoDurationDecision,
    VideoRemakeGenerationResult,
)
from run_pipeline import VideoRemakePipeline, load_reference_action_records  # noqa: E402
from core.reference_image import (  # noqa: E402
    APPROVAL_MANUAL,
    ReferenceImageResult,
    STATUS_PENDING_APPROVAL,
)


MAPPING = {
    "status": "任务状态",
    "video": "视频",
    "script_breakdown": "脚本拆解",
    "remake_card": "复刻卡",
    "remade_script": "复刻后的脚本",
    "final_prompt": "最终复刻视频提示词",
    "video_duration": "视频时长",
    "reference_images": "复刻参考图",
    "sync_status": "同步状态",
    "synced_script_id": "同步到脚本ID",
    "error_message": "错误信息",
    "content_branch": "内容分支",
    "store_id": "店铺ID",
    "target_country": "目标国家",
    "target_language": "目标语言",
    "product_type": "商品类型",
    "remake_mode": None,
    "reference_status": "参考图状态",
    "reference_approval_mode": "参考图审批模式",
    "reference_action": "参考图操作",
    "reference_qa_result": "参考图质检结果",
    "reference_input_fingerprint": "参考输入指纹",
    "visual_lock_card": "视觉锁定卡",
    "ai_reference_prompt": "AI参考图提示词",
    "ai_reference_image": "AI视频参考图",
    "reference_requirements": "参考要求",
    "person_reference_images": "人物参考图",
    "clothing_reference_images": "服装参考图",
    "scene_reference_images": "场景参考图",
}


class FakeClient:
    def __init__(self) -> None:
        self.updates = []
        self.uploads = []

    def update_record_fields(self, record_id, fields):
        self.updates.append((record_id, dict(fields)))
        return True

    def upload_attachment(self, *, content, file_name, content_type, size):
        self.uploads.append(file_name)
        return {"file_token": f"token_{len(self.uploads)}", "name": file_name}


class FakeLLM:
    def __init__(self, result) -> None:
        self.result = result

    def generate_four_fields(self, **_kwargs):
        return self.result


class FakeReferenceWorkflow:
    def generate(self, **_kwargs):
        return ReferenceImageResult(
            attachment={"file_token": "ai_ref_1", "name": "ai_ref.png"},
            visual_lock_card="lock card",
            image_prompt="image prompt",
            status=STATUS_PENDING_APPROVAL,
            approval_mode=APPROVAL_MANUAL,
            version=1,
            qa_result='{"通过": true}',
            input_fingerprint="fingerprint_1",
        )


def build_pipeline(result):
    pipeline = object.__new__(VideoRemakePipeline)
    pipeline.client = FakeClient()
    pipeline.mapping = dict(MAPPING)
    pipeline.llm_client = FakeLLM(result)
    pipeline.reference_workflow = None
    pipeline.stats = {"total": 0, "success": 0, "failed": 0, "not_suitable": 0}
    return pipeline


class VideoRemakePipelineTest(unittest.TestCase):
    def test_suitable_result_writes_supported_duration_and_reference_frames(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = []
            for index in range(3):
                path = Path(temp_dir) / f"frame_{index}.jpg"
                path.write_bytes(b"jpeg")
                paths.append(path)
            result = VideoRemakeGenerationResult(
                outputs={
                    "脚本拆解": "breakdown",
                    "复刻卡": "card",
                    "复刻后的脚本": "script",
                    "最终复刻视频提示词": "prompt",
                },
                duration_decision=VideoDurationDecision(12.0, "原长复刻", 10),
                reference_frames=paths,
            )
            pipeline = build_pipeline(result)
            record = RemakeRecord(
                record_id="rec_1",
                fields={"任务状态": "待开始", "视频": "https://example.com/video.mp4", "任务编号": "300"},
            )

            pipeline._process_single_record(record)

            combined = {}
            for _record_id, fields in pipeline.client.updates:
                combined.update(fields)
            self.assertEqual(combined["视频时长"], 10)
            self.assertEqual(len(combined["复刻参考图"]), 3)
            self.assertEqual(combined["任务状态"], "已完成")
            self.assertEqual(combined["同步状态"], "待同步")

    def test_not_suitable_result_stops_before_final_fields(self) -> None:
        result = VideoRemakeGenerationResult(
            outputs={"脚本拆解": "零、时长决策"},
            duration_decision=VideoDurationDecision(30.0, "不建议复刻", 0, "过长"),
            reference_frames=[],
        )
        pipeline = build_pipeline(result)
        record = RemakeRecord(
            record_id="rec_2",
            fields={"任务状态": "待开始", "视频": "https://example.com/video.mp4", "任务编号": "301"},
        )

        stats = pipeline.process_records([record])

        combined = {}
        for _record_id, fields in pipeline.client.updates:
            combined.update(fields)
        self.assertEqual(stats["not_suitable"], 1)
        self.assertEqual(combined["视频时长"], 0)
        self.assertEqual(combined["任务状态"], "不适合复刻")
        self.assertNotIn("最终复刻视频提示词", combined)

    def test_reference_image_waits_for_manual_confirmation_before_sync(self) -> None:
        result = VideoRemakeGenerationResult(
            outputs={
                "脚本拆解": "breakdown",
                "复刻卡": "card",
                "复刻后的脚本": "script",
                "最终复刻视频提示词": "prompt",
            },
            duration_decision=VideoDurationDecision(8.0, "原长复刻", 8),
            reference_frames=[],
        )
        pipeline = build_pipeline(result)
        pipeline.reference_workflow = FakeReferenceWorkflow()
        record = RemakeRecord(
            record_id="rec_3",
            fields={"任务状态": "待开始", "视频": "https://example.com/video.mp4", "任务编号": "302"},
        )

        pipeline._process_single_record(record)

        combined = {}
        for _record_id, fields in pipeline.client.updates:
            combined.update(fields)
        self.assertEqual(combined["参考图状态"], "待确认")
        self.assertEqual(combined["AI视频参考图"][0]["file_token"], "ai_ref_1")
        self.assertEqual(combined["同步状态"], "等待参考图确认")

    def test_confirm_action_releases_record_for_sync(self) -> None:
        pipeline = build_pipeline(None)
        record = RemakeRecord(
            record_id="rec_4",
            fields={
                "任务状态": "已完成",
                "参考图操作": "确认",
                "参考图状态": "待确认",
                "AI视频参考图": [{"file_token": "ai_ref_1"}],
            },
        )

        stats = pipeline.process_reference_actions([record])

        self.assertEqual(stats["confirmed"], 1)
        _record_id, fields = pipeline.client.updates[-1]
        self.assertEqual(fields["参考图状态"], "已确认")
        self.assertEqual(fields["同步状态"], "待同步")
        self.assertIsNone(fields["参考图操作"])

    def test_changed_reference_inputs_enter_regeneration_queue(self) -> None:
        record = RemakeRecord(
            record_id="rec_5",
            fields={
                "任务状态": "已完成",
                "复刻卡": "card",
                "最终复刻视频提示词": "prompt",
                "参考要求": "换成室外自然光",
                "AI视频参考图": [{"file_token": "ai_ref_1"}],
                "参考输入指纹": "outdated",
            },
        )

        selected = load_reference_action_records([record], MAPPING)

        self.assertEqual([item.record_id for item in selected], ["rec_5"])


if __name__ == "__main__":
    unittest.main()
