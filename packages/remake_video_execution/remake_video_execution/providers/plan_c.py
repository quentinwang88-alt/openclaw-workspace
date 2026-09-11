from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping


class PlanCMediaAdapter:
    """Thin adapter over the existing H3 gateway without importing original planning."""

    def __init__(self, gateway: Any):
        self.gateway = gateway

    @staticmethod
    def request_payload(
        segment: Mapping[str, Any], *, start_frame: str, end_frame: str = "",
        reference_images: list[str] | None = None,
    ) -> dict[str, Any]:
        if not Path(start_frame).is_file():
            raise ValueError("片段缺少已冻结的开始帧")
        boundary = str(segment.get("incoming_boundary") or "")
        mode = "first_last" if end_frame else ("reference" if boundary == "CUT" else "first_frame")
        if end_frame and not Path(end_frame).is_file():
            raise ValueError("片段结束帧不存在")
        frozen_references = list(dict.fromkeys(reference_images or []))
        if any(not Path(item).is_file() for item in frozen_references):
            raise ValueError("片段参考图不存在")
        return {
            "segment_id": segment["segment_id"],
            "duration_seconds": int(segment["requested_duration_seconds"]),
            "generation_mode": mode,
            "video_prompt": str(segment["prompt"]),
            "incoming_boundary": boundary,
            "start_frame": start_frame,
            "end_frame": end_frame,
            "reference_images": frozen_references if mode == "reference" else [],
        }

    @staticmethod
    def fingerprint(payload: Mapping[str, Any]) -> str:
        normalized = dict(payload)
        for key in ("start_frame", "end_frame"):
            value = str(normalized.get(key) or "")
            normalized[key + "_sha256"] = (
                hashlib.sha256(Path(value).read_bytes()).hexdigest() if value else ""
            )
            normalized.pop(key, None)
        references = [str(item) for item in normalized.pop("reference_images", [])]
        normalized["reference_image_sha256s"] = [
            hashlib.sha256(Path(item).read_bytes()).hexdigest() for item in references
        ]
        return hashlib.sha256(
            json.dumps(normalized, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()

    def preflight(self, *, require_api_key: bool = False) -> dict[str, Any]:
        return self.gateway.preflight(require_api_key=require_api_key)

    def submit(
        self, payload: Mapping[str, Any], *, request_path: str | Path,
        allow_real_submit: bool,
    ) -> dict[str, Any]:
        if not allow_real_submit:
            raise PermissionError("真实媒体提交需要显式 allow_real_submit")
        segment = {
            "segment_id": payload["segment_id"],
            "duration_seconds": payload["duration_seconds"],
            "generation_mode": payload["generation_mode"],
            "video_prompt": payload["video_prompt"],
        }
        request = self.gateway.build_segment_request(
            segment, start_frame=str(payload["start_frame"]),
            end_frame=str(payload.get("end_frame") or ""),
            reference_images=list(payload.get("reference_images") or []),
        )
        target = Path(request_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(request, ensure_ascii=False, indent=2), encoding="utf-8")
        return self.gateway.submit(target, allow_real_submit=True)
