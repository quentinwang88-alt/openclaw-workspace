from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict

from .audio import finalize_with_voiceover, validate_finalized_media
from .h3_gateway import H3Gateway, write_segment_request
from .media import extract_bridge_candidates, merge_segments, select_bridge_candidate
from .review import export_review_bundle
from .storage import LongformStorage
from .voiceover import DEFAULT_MODEL_COMMAND, calibrate_longform_voiceover_with_edge


class RemoteGenerationPending(RuntimeError):
    """The remote H3 task is healthy but did not finish in this patrol window."""


def _json(value: str) -> Dict[str, Any]:
    try:
        loaded = json.loads(value or "{}")
        return loaded if isinstance(loaded, dict) else {}
    except json.JSONDecodeError:
        return {}


def _segment(row: Dict[str, Any], segment_id: str) -> Dict[str, Any]:
    for item in row.get("segments") or []:
        if str(item.get("segment_id")) == segment_id:
            return item
    raise RuntimeError(f"找不到片段{segment_id}")


def _ordered_segments(plan: Dict[str, Any]) -> list[Dict[str, Any]]:
    segments = [dict(item) for item in plan.get("segments") or []]
    if not 2 <= len(segments) <= 3:
        raise RuntimeError("长视频计划必须包含2至3个有序片段")
    return segments


def _incoming_boundary(plan: Dict[str, Any], segment_id: str) -> Dict[str, Any]:
    for item in dict(plan.get("bridge_contract") or {}).get("boundaries") or []:
        if str(item.get("to_segment") or "") == segment_id:
            return dict(item)
    return {}


def _task_status(response: Dict[str, Any]) -> str:
    return str((((response.get("response") or {}).get("task") or {}).get("status") or "")).lower()


def _persist_report(storage: LongformStorage, job_id: str, asset_root: Path,
                    events: list[Dict[str, Any]]) -> Dict[str, Any]:
    row = storage.get_job(job_id) or {}
    previous = _json(str(row.get("execution_report_json") or "{}"))
    previous_events = list(previous.get("events") or [])
    combined_events = (previous_events + events)[-200:]
    report = {
        "schema_version": "longform-execution-report-v1",
        "job_id": job_id,
        "product_code": row.get("product_code"),
        "status": row.get("status"),
        "segments": [
            {
                "segment_id": item.get("segment_id"),
                "status": item.get("status"),
                "platform_task_id": item.get("platform_task_id"),
                "submit_fingerprint": item.get("submit_fingerprint"),
                "start_frame_path": item.get("start_frame_path"),
                "end_frame_path": item.get("end_frame_path"),
                "output_video_path": item.get("output_video_path"),
                "updated_at": item.get("updated_at"),
            }
            for item in row.get("segments") or []
        ],
        "merged_video_path": row.get("merged_video_path"),
        "final_video_path": row.get("final_video_path"),
        "events": combined_events,
        "updated_at": int(time.time()),
    }
    target = asset_root / job_id / "execution_report.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    storage.update_job(
        job_id, str(row.get("status") or "UNKNOWN"),
        execution_report_json=json.dumps(report, ensure_ascii=False),
    )
    return report


def _submit_segment(storage: LongformStorage, gateway: H3Gateway, row: Dict[str, Any],
                    plan: Dict[str, Any], asset_root: Path, job_id: str,
                    segment_id: str, *, allow_real_submit: bool,
                    events: list[Dict[str, Any]]) -> None:
    current = _segment(row, segment_id)
    status = str(current.get("status") or "")
    if status in {"SUBMITTED", "READY"}:
        return
    segments = _ordered_segments(plan)
    segment_index = next(
        index for index, item in enumerate(segments)
        if str(item.get("segment_id")) == segment_id
    )
    required = "KEYFRAMES_READY" if segment_index == 0 else "BRIDGE_READY"
    if status != required:
        raise RuntimeError(f"片段{segment_id}当前为{status or 'UNKNOWN'}，需要{required}")
    if not allow_real_submit:
        raise RuntimeError("真实 H3 提交必须显式使用 --allow-real-submit")
    start_frame = str(current.get("start_frame_path") or "")
    plan_segment = segments[segment_index]
    needs_end_frame = str(plan_segment.get("generation_mode") or "") == "first_last"
    end_frame = str(current.get("end_frame_path") or "") if needs_end_frame else ""
    required_paths = [start_frame] + ([end_frame] if needs_end_frame else [])
    if any(not value or not Path(value).is_file() for value in required_paths):
        raise RuntimeError(f"片段{segment_id}登记的关键帧不存在")
    request = H3Gateway.build_segment_request(
        plan_segment, start_frame=start_frame, end_frame=end_frame,
    )
    request_path = write_segment_request(
        asset_root / job_id / "h3" / f"segment_{segment_id}.json", request,
    )
    submitted = gateway.submit(request_path, allow_real_submit=True)
    task_id = str(submitted.get("taskId") or "")
    if not task_id:
        raise RuntimeError(f"片段{segment_id}提交成功响应缺少 taskId")
    storage.update_segment(
        job_id, segment_id, status="SUBMITTED", platform_task_id=task_id,
        submit_fingerprint=str(submitted.get("fingerprint") or ""),
        platform_response_json=json.dumps(submitted, ensure_ascii=False),
        start_frame_path=start_frame, end_frame_path=end_frame,
    )
    storage.update_job(job_id, f"{segment_id}_SUBMITTED")
    events.append({"stage": f"H3_{segment_id}_SUBMITTED", "task_id": task_id, "at": int(time.time())})


def _wait_and_download(storage: LongformStorage, gateway: H3Gateway, job_id: str,
                       segment_id: str, asset_root: Path, *, poll_interval_seconds: int,
                       max_wait_seconds: int, events: list[Dict[str, Any]]) -> None:
    row = storage.get_job(job_id) or {}
    current = _segment(row, segment_id)
    if str(current.get("status") or "") == "READY":
        registered = Path(str(current.get("output_video_path") or ""))
        if registered.is_file():
            return
        raise RuntimeError(f"片段{segment_id}状态 READY 但输出文件不存在")
    task_id = str(current.get("platform_task_id") or "")
    if not task_id:
        raise RuntimeError(f"片段{segment_id}缺少 platform_task_id")
    deadline = time.monotonic() + max_wait_seconds
    while True:
        response = gateway.query(task_id)
        storage.update_segment(
            job_id, segment_id,
            platform_response_json=json.dumps(response, ensure_ascii=False),
        )
        status = _task_status(response)
        if status in {"succeeded", "success", "completed"}:
            break
        if status in {"failed", "error", "cancelled", "canceled"}:
            raise RuntimeError(f"片段{segment_id} H3 任务失败: {status}")
        if time.monotonic() >= deadline:
            storage.update_job(job_id, f"{segment_id}_WAITING_REMOTE")
            events.append({
                "stage": f"H3_{segment_id}_WAITING_REMOTE",
                "task_id": task_id,
                "max_wait_seconds": max_wait_seconds,
                "at": int(time.time()),
            })
            raise RemoteGenerationPending(
                f"片段{segment_id}仍在远端生成；下一轮将从任务 {task_id} 继续查询"
            )
        time.sleep(max(5, poll_interval_seconds))
    target = asset_root / job_id / f"segment_{segment_id}.mp4"
    downloaded = gateway.download(task_id, target)
    storage.update_segment(
        job_id, segment_id, status="READY", output_video_path=str(target),
        platform_response_json=json.dumps(downloaded, ensure_ascii=False),
    )
    storage.update_job(job_id, f"{segment_id}_READY")
    events.append({"stage": f"H3_{segment_id}_READY", "task_id": task_id, "at": int(time.time())})


def run_to_final(
    storage: LongformStorage,
    job_id: str,
    *,
    asset_root: str | Path,
    allow_real_submit: bool,
    allow_external_tts: bool,
    manual_bridge: bool = False,
    poll_interval_seconds: int = 30,
    max_wait_seconds: int = 1200,
    voiceover_model_command: str = DEFAULT_MODEL_COMMAND,
) -> Dict[str, Any]:
    root = Path(asset_root).expanduser().resolve()
    events: list[Dict[str, Any]] = []
    row = storage.get_job(job_id)
    if not row:
        raise RuntimeError(f"找不到 job: {job_id}")
    final_path = Path(str(row.get("final_video_path") or ""))
    if str(row.get("status") or "") == "FINAL_READY" and final_path.is_file():
        try:
            validate_finalized_media(final_path)
        except RuntimeError:
            # A prior run may have produced an MP4 container without a usable
            # video/audio payload.  Resume from the already downloaded assets.
            pass
        else:
            review = export_review_bundle(row, root / job_id / "text_review")
            return {"job_id": job_id, "status": "FINAL_READY", "final_video_path": str(final_path),
                    "action": "IDEMPOTENT_REUSE", "review": review}
    plan = _json(str(row.get("plan_json") or "{}"))
    master = _json(str(row.get("master_contract_json") or "{}"))
    gateway = H3Gateway(state_root=root / job_id / "h3_state")
    needs_h3 = any(
        str(item.get("status") or "") != "READY" for item in row.get("segments") or []
    )
    current_segments = list(row.get("segments") or [])
    requires_new_submit = any(
        str(item.get("status") or "") in {"KEYFRAMES_READY", "BRIDGE_READY"}
        or (
            index > 0
            and str(item.get("status") or "") == "PLANNED"
            and str(current_segments[index - 1].get("status") or "") == "READY"
        )
        for index, item in enumerate(current_segments)
    )
    voiceover = _json(str(row.get("voiceover_json") or "{}"))
    if not voiceover.get("target_text"):
        report = _persist_report(storage, job_id, root, events)
        return {"job_id": job_id, "status": "WAITING_VOICEOVER", "report": report}
    if requires_new_submit and not allow_real_submit:
        report = _persist_report(storage, job_id, root, events)
        return {"job_id": job_id, "status": "WAITING_H3_AUTHORIZATION", "report": report}
    if requires_new_submit and not allow_external_tts:
        report = _persist_report(storage, job_id, root, events)
        return {"job_id": job_id, "status": "WAITING_TTS_PREFLIGHT_AUTHORIZATION", "report": report}
    # Validate H3 credentials and runner before any external image/TTS work.
    # This keeps a missing production dependency from consuming unrelated
    # generation resources or changing the job to an ambiguous partial state.
    preflight = gateway.preflight(require_api_key=needs_h3)
    if needs_h3 and not preflight["ready"]:
        raise RuntimeError("；".join(preflight["problems"]))
    if allow_external_tts:
        try:
            calibrated = calibrate_longform_voiceover_with_edge(
                master, plan, voiceover, root / job_id / "voiceover_preflight",
                model_command=voiceover_model_command,
            )
            if calibrated != voiceover:
                voiceover = calibrated
                storage.update_job(
                    job_id, "VOICEOVER_TTS_READY",
                    voiceover_json=json.dumps(voiceover, ensure_ascii=False),
                )
                events.append({
                    "stage": "EDGE_TTS_PREFLIGHT_READY",
                    "tts_preflight": voiceover.get("tts_preflight") or {},
                    "at": int(time.time()),
                })
        except Exception as exc:
            storage.update_job(
                job_id, "FAILED_RETRYABLE",
                error_json=json.dumps({"error": str(exc), "stage": "EDGE_TTS_PREFLIGHT",
                                       "at": int(time.time())}, ensure_ascii=False),
            )
            events.append({"stage": "EDGE_TTS_PREFLIGHT_FAILED", "error": str(exc),
                           "at": int(time.time())})
            _persist_report(storage, job_id, root, events)
            raise
    try:
        row = storage.get_job(job_id) or {}
        segments = _ordered_segments(plan)
        first_segment_id = str(segments[0]["segment_id"])
        if str(_segment(row, first_segment_id).get("status") or "") == "PLANNED":
            report = _persist_report(storage, job_id, root, events)
            return {"job_id": job_id, "status": "WAITING_KEYFRAMES", "report": report}

        for index, plan_segment in enumerate(segments):
            segment_id = str(plan_segment["segment_id"])
            row = storage.get_job(job_id) or {}
            current = _segment(row, segment_id)
            if index > 0 and str(current.get("status") or "") == "PLANNED":
                incoming = _incoming_boundary(plan, segment_id)
                if str(incoming.get("boundary_mode") or "CONTINUOUS") == "DISCONTINUOUS_CUT":
                    raise RuntimeError(
                        f"片段{segment_id}是跨场景硬切，但场景进入帧尚未登记；"
                        "请在付费H3提交前生成或登记片段进入帧 SCENE_ENTRY/SETUP_ENTRY"
                    )
                previous_id = str(segments[index - 1]["segment_id"])
                previous = _segment(row, previous_id)
                if str(previous.get("status") or "") != "READY":
                    raise RuntimeError(f"片段{segment_id}准备桥接前，片段{previous_id}必须READY")
                bridge_root = root / job_id / "bridge_candidates" / f"{previous_id}_to_{segment_id}"
                candidates = extract_bridge_candidates(
                    previous.get("output_video_path") or "", bridge_root,
                )
                events.append({
                    "stage": "BRIDGE_CANDIDATES_READY",
                    "from_segment": previous_id, "to_segment": segment_id,
                    "count": len(candidates), "at": int(time.time()),
                })
                storage.update_job(job_id, f"BRIDGE_{previous_id}_{segment_id}_CANDIDATES_READY")
                if manual_bridge:
                    report = _persist_report(storage, job_id, root, events)
                    return {
                        "job_id": job_id, "status": "WAITING_BRIDGE_REVIEW",
                        "from_segment": previous_id, "to_segment": segment_id,
                        "bridge_candidates": candidates, "report": report,
                    }
                selected = select_bridge_candidate(candidates)
                storage.update_segment(
                    job_id, segment_id, status="BRIDGE_READY",
                    start_frame_path=selected["selected"],
                    platform_response_json=json.dumps(
                        {"bridge_selection": selected}, ensure_ascii=False
                    ),
                )
                storage.update_job(job_id, f"BRIDGE_{previous_id}_{segment_id}_READY")
                events.append({
                    "stage": "BRIDGE_AUTO_SELECTED",
                    "from_segment": previous_id, "to_segment": segment_id,
                    "selection": selected, "at": int(time.time()),
                })

            row = storage.get_job(job_id) or {}
            _submit_segment(
                storage, gateway, row, plan, root, job_id, segment_id,
                allow_real_submit=allow_real_submit, events=events,
            )
            _wait_and_download(
                storage, gateway, job_id, segment_id, root,
                poll_interval_seconds=poll_interval_seconds,
                max_wait_seconds=max_wait_seconds, events=events,
            )

        row = storage.get_job(job_id) or {}
        merged = Path(str(row.get("merged_video_path") or ""))
        if not merged.is_file():
            merged = root / job_id / "merged_silent.mp4"
            merge_segments(
                [
                    _segment(row, str(item["segment_id"]))["output_video_path"]
                    for item in segments
                ],
                merged,
            )
            storage.update_job(job_id, "MERGED", merged_video_path=str(merged))
            events.append({"stage": "MERGED", "path": str(merged), "at": int(time.time())})

        row = storage.get_job(job_id) or {}
        voiceover = _json(str(row.get("voiceover_json") or "{}"))
        if not allow_external_tts:
            report = _persist_report(storage, job_id, root, events)
            return {"job_id": job_id, "status": "WAITING_TTS_AUTHORIZATION", "report": report}
        final_path = root / job_id / "final_video.mp4"
        finalization = finalize_with_voiceover(
            merged, voiceover, final_path, allow_external_tts=allow_external_tts,
            segment_plan=segments,
        )
        storage.update_job(job_id, "FINAL_READY", final_video_path=str(final_path))
        events.append({"stage": "FINAL_READY", "finalization": finalization,
                       "at": int(time.time())})
        final_row = storage.get_job(job_id) or {}
        review = export_review_bundle(final_row, root / job_id / "text_review")
        events.append({"stage": "TEXT_REVIEW_REFRESHED", "review": review,
                       "at": int(time.time())})
        report = _persist_report(storage, job_id, root, events)
        return {
            "job_id": job_id, "status": "FINAL_READY", "final_video_path": str(final_path),
            "finalization": finalization, "report_path": str(root / job_id / "execution_report.json"),
            "report": report, "review": review,
        }
    except RemoteGenerationPending as exc:
        report = _persist_report(storage, job_id, root, events)
        return {
            "job_id": job_id,
            "status": "WAITING_REMOTE",
            "message": str(exc),
            "report": report,
        }
    except Exception as exc:
        storage.update_job(
            job_id, "FAILED_RETRYABLE",
            error_json=json.dumps({"error": str(exc), "at": int(time.time())}, ensure_ascii=False),
        )
        events.append({"stage": "FAILED_RETRYABLE", "error": str(exc), "at": int(time.time())})
        _persist_report(storage, job_id, root, events)
        raise
