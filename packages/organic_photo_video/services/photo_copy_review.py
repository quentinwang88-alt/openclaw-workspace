"""Auditable native-language approval for TSV photo copy packs."""
from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping, MutableMapping, Sequence


REVIEW_AUDIT_FIELDS = ("reviewed_by", "reviewed_at", "review_sha256")


class PhotoCopyReviewError(ValueError):
    pass


def copy_review_sha256(row: Mapping[str, Any]) -> str:
    payload = {
        "title": _decode(row.get("title")),
        "caption": _decode(row.get("caption")),
        "hashtags": [
            value.strip() for value in str(row.get("hashtags") or "").split("|")
            if value.strip()
        ],
        "slide_texts": [_decode(row.get(f"slide_{index}")) for index in range(1, 6)],
    }
    return _payload_sha256(payload)


def frozen_copy_review_sha256(copy_block: Mapping[str, Any]) -> str:
    payload = {
        "title": str(copy_block.get("title") or "").strip(),
        "caption": str(copy_block.get("caption") or "").strip(),
        "hashtags": [str(value).strip() for value in copy_block.get("hashtags") or []],
        "slide_texts": [str(value).strip() for value in copy_block.get("slide_texts") or []],
    }
    return _payload_sha256(payload)


def _decode(value: Any) -> str:
    return str(value or "").replace("\\n", "\n").strip()


def _payload_sha256(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")).hexdigest()


def validate_native_approval(row: Mapping[str, Any]) -> dict[str, str]:
    status = str(row.get("language_review_status") or "").strip()
    if status != "NATIVE_APPROVED":
        return {}
    reviewer = str(row.get("reviewed_by") or "").strip()
    reviewed_at = str(row.get("reviewed_at") or "").strip()
    digest = str(row.get("review_sha256") or "").strip()
    expected = copy_review_sha256(row)
    if not reviewer or not reviewed_at or digest != expected:
        raise PhotoCopyReviewError(
            "NATIVE_APPROVED 必须包含审校人、审校时间和匹配当前文案的 review_sha256"
        )
    return {"reviewed_by": reviewer, "reviewed_at": reviewed_at,
            "review_sha256": digest}


def approve_copy_rows(
    rows: Sequence[MutableMapping[str, Any]], *, reviewer: str,
    reviewed_at: str, expected_hashes: Mapping[str, str],
) -> list[dict[str, str]]:
    reviewer = str(reviewer or "").strip()
    reviewed_at = str(reviewed_at or "").strip()
    if not reviewer or not reviewed_at:
        raise PhotoCopyReviewError("审校人和审校时间不能为空")
    by_id = {str(row.get("copy_id") or "").strip(): row for row in rows}
    missing = sorted(set(expected_hashes) - set(by_id))
    if missing:
        raise PhotoCopyReviewError("找不到 copy_id：" + "、".join(missing))
    results = []
    for copy_id, expected_hash in expected_hashes.items():
        row = by_id[copy_id]
        actual = copy_review_sha256(row)
        if actual != str(expected_hash or "").strip():
            raise PhotoCopyReviewError(
                f"{copy_id} 文案哈希已变化；预期 {expected_hash}，当前 {actual}"
            )
        row["language_review_status"] = "NATIVE_APPROVED"
        row["reviewed_by"] = reviewer
        row["reviewed_at"] = reviewed_at
        row["review_sha256"] = actual
        results.append({"copy_id": copy_id, "review_sha256": actual,
                        "reviewed_by": reviewer, "reviewed_at": reviewed_at})
    return results
