#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from remake_video_execution.contracts import SourceSnapshot
from remake_video_execution.executor import submit_frozen_segments
from remake_video_execution.parser import parse_source
from remake_video_execution.planner import plan_segments
from remake_video_execution.providers.plan_c import PlanCMediaAdapter
from remake_video_execution.repository import RemakeExecutionRepository


class MockH3Gateway:
    """Offline H3-shaped gateway. It never performs a network request."""

    def __init__(self) -> None:
        self.submit_calls = 0

    @staticmethod
    def build_segment_request(segment: dict[str, Any], *, start_frame: str, end_frame: str = "",
                              reference_images: list[str] | None = None) -> dict[str, Any]:
        return {
            "test_mode": True,
            "provider": "mock-h3",
            "segmentId": segment["segment_id"],
            "duration": segment["duration_seconds"],
            "generationMode": segment["generation_mode"],
            "prompt": segment["video_prompt"],
            "imagePaths": [path for path in (start_frame, end_frame, *(reference_images or [])) if path],
        }

    def submit(self, request_path: str | Path, *, allow_real_submit: bool) -> dict[str, Any]:
        self.submit_calls += 1
        request = json.loads(Path(request_path).read_text(encoding="utf-8"))
        digest = hashlib.sha256(
            json.dumps(request, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()[:12]
        return {
            "task_id": f"mock_{request['segmentId'].lower()}_{digest}",
            "accepted": bool(allow_real_submit),
            "network_called": False,
        }


def _markdown(report: dict[str, Any]) -> str:
    rows = []
    for item in report["first_pass"]:
        rows.append(
            f"| {item['segment_id']} | {item['duration_seconds']}s | "
            f"{item['status']} | {item['platform_task_id']} |"
        )
    blockers = "、".join(report["source_blockers"]) or "无"
    return "\n".join([
        f"# {report['script_id']} 隔离测试报告",
        "",
        f"- 测试模式：离线 Mock H3（真实网络调用：{str(report['network_called']).lower()}）",
        f"- 源视频时长：{report['duration_seconds']}s",
        f"- 分段：{report['segment_count']} 段，合计 {report['generated_duration_seconds']}s",
        f"- 音频：{report['audio_mode']}；字幕 cue：{report['subtitle_cue_count']}",
        f"- 首轮模拟提交：{report['first_pass_submissions']}；同指纹重跑新增提交：{report['second_pass_submissions']}",
        f"- 正式输入仍阻断：{blockers}",
        "",
        "| 片段 | 时长 | 状态 | Mock task id |",
        "|---|---:|---|---|",
        *rows,
        "",
        "## 结论",
        "",
        "分段计划、局部时间轴、请求落盘、任务登记和幂等重跑均通过。"
        "本报告没有验证真实商品外观、真实视频生成、成片拼接和发布回写。",
    ]) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="用离线 H3 假提交器跑复刻视频分段测试")
    parser.add_argument("--plan", required=True, help="已有计划 JSON")
    parser.add_argument("--start-frame", required=True, help="本地技术占位首帧")
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    plan_path = Path(args.plan).resolve()
    frame_path = Path(args.start_frame).resolve()
    output_dir = Path(args.output_dir).resolve()
    if not plan_path.is_file():
        raise SystemExit(f"计划文件不存在: {plan_path}")
    if not frame_path.is_file():
        raise SystemExit(f"占位首帧不存在: {frame_path}")
    output_dir.mkdir(parents=True, exist_ok=True)

    frozen = json.loads(plan_path.read_text(encoding="utf-8"))
    source = SourceSnapshot(**frozen["source"])
    execution_plan = parse_source(source)
    segment_plan = plan_segments(execution_plan)
    segments = [segment.to_dict() for segment in segment_plan.segments]
    job_id = f"mock_{source.script_id}_{source.source_revision_hash[:12]}"
    repository = RemakeExecutionRepository(output_dir / "state.sqlite3")
    repository.ensure_schema()
    repository.save_plan(job_id, source.to_dict(), {
        **segment_plan.to_dict(),
        "issues": [],
        "test_mode": True,
        "source_blockers_bypassed_for_mock": [issue.code for issue in execution_plan.issues if issue.severity == "BLOCK"],
    })

    gateway = MockH3Gateway()
    adapter = PlanCMediaAdapter(gateway)
    frames = {segment["segment_id"]: str(frame_path) for segment in segments}
    common = dict(
        repository=repository,
        adapter=adapter,
        job_id=job_id,
        segments=segments,
        start_frames=frames,
        request_dir=output_dir / "requests",
        allow_submit=True,
    )
    first = submit_frozen_segments(**common)
    second = submit_frozen_segments(**common)

    durations = {segment["segment_id"]: segment["requested_duration_seconds"] for segment in segments}
    with repository.connect() as connection:
        db_segments = [dict(row) for row in connection.execute(
            "SELECT segment_id,status,platform_task_id FROM remake_segment WHERE job_id=? ORDER BY segment_id",
            (job_id,),
        ).fetchall()]
        attempt_count = connection.execute(
            "SELECT COUNT(*) AS n FROM submission_attempt WHERE job_id=?", (job_id,)
        ).fetchone()["n"]
    first_rows = [{**item, "duration_seconds": durations[item["segment_id"]]} for item in first]
    report = {
        "test_mode": True,
        "network_called": False,
        "source_plan": str(plan_path),
        "mock_start_frame": str(frame_path),
        "script_id": source.script_id,
        "record_id": source.record_id,
        "job_id": job_id,
        "duration_seconds": execution_plan.duration_ms / 1000,
        "segment_count": len(segments),
        "generated_duration_seconds": sum(durations.values()),
        "max_segment_seconds": max(durations.values(), default=0),
        "audio_mode": execution_plan.audio_mode,
        "subtitle_cue_count": len(frozen.get("subtitle_plan", {}).get("cues", [])),
        "source_blockers": [issue.code for issue in execution_plan.issues if issue.severity == "BLOCK"],
        "first_pass_submissions": sum(bool(item["submitted"]) for item in first),
        "second_pass_submissions": sum(bool(item["submitted"]) for item in second),
        "submission_attempt_count": attempt_count,
        "first_pass": first_rows,
        "second_pass": second,
        "database_segments": db_segments,
        "checks": {
            "all_segments_within_15_seconds": all(value <= 15 for value in durations.values()),
            "duration_covered": sum(durations.values()) == execution_plan.duration_ms / 1000,
            "all_requests_written": all((output_dir / "requests" / f"{key}.json").is_file() for key in durations),
            "all_segments_submitted": all(row["status"] == "SUBMITTED" for row in db_segments),
            "idempotent_rerun": gateway.submit_calls == len(segments) and not any(item["submitted"] for item in second),
            "seg02_uses_local_clock": "0-3s 人物缓慢转向左侧" in segments[1]["prompt"] and "10-13s" not in segments[1]["prompt"],
        },
        "limitations": [
            "技术占位图不是该脚本的真实商品参考图，不能验证商品一致性",
            "未调用真实视频生成渠道",
            "未产生视频文件，因此未测试桥帧、拼接、BGM 合成和终检",
            "未写回飞书，也未进入发布流程",
        ],
    }
    if not all(report["checks"].values()):
        raise RuntimeError(f"测试检查未全部通过: {report['checks']}")
    (output_dir / "test_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (output_dir / "test_report.md").write_text(_markdown(report), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
