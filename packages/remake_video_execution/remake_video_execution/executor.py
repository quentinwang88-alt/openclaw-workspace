from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Sequence

from .providers.plan_c import PlanCMediaAdapter
from .repository import RemakeExecutionRepository


def submit_frozen_segments(
    *,
    repository: RemakeExecutionRepository,
    adapter: PlanCMediaAdapter,
    job_id: str,
    segments: Sequence[Mapping[str, Any]],
    start_frames: Mapping[str, str],
    reference_images: Mapping[str, list[str]] | None = None,
    request_dir: str | Path,
    allow_submit: bool,
) -> list[dict[str, Any]]:
    """Compile and submit frozen segments once per content fingerprint."""
    target_dir = Path(request_dir)
    results: list[dict[str, Any]] = []
    for segment in segments:
        segment_id = str(segment["segment_id"])
        payload = adapter.request_payload(
            segment, start_frame=start_frames[segment_id],
            reference_images=(reference_images or {}).get(segment_id, []),
        )
        fingerprint = adapter.fingerprint(payload)
        status, claimed = repository.claim_submission(job_id, segment_id, fingerprint)
        if not claimed:
            results.append({
                "segment_id": segment_id,
                "fingerprint": fingerprint,
                "status": status,
                "submitted": False,
            })
            continue
        try:
            response = adapter.submit(
                payload,
                request_path=target_dir / f"{segment_id}.json",
                allow_real_submit=allow_submit,
            )
        except PermissionError:
            repository.mark_submission(
                job_id, segment_id, fingerprint,
                status="FAILED_BEFORE_SUBMIT", response={"reason": "submit_not_allowed"},
            )
            raise
        except Exception as exc:
            repository.mark_submission(
                job_id, segment_id, fingerprint,
                status="SUBMISSION_UNKNOWN", response={"error": str(exc)},
            )
            raise
        platform_task_id = str(response.get("task_id") or response.get("taskId") or "")
        repository.mark_submission(
            job_id, segment_id, fingerprint,
            status="SUBMITTED", platform_task_id=platform_task_id, response=dict(response),
        )
        results.append({
            "segment_id": segment_id,
            "fingerprint": fingerprint,
            "status": "SUBMITTED",
            "platform_task_id": platform_task_id,
            "submitted": True,
        })
    return results
