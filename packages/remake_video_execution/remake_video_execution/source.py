from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Iterable

from .contracts import SourceSnapshot


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return "".join(_text(item) for item in value)
    if isinstance(value, dict):
        return str(value.get("text") or value.get("name") or value.get("url") or "")
    return str(value).strip()


def _attachments(values: Iterable[Any]) -> list[Dict[str, Any]]:
    result: list[Dict[str, Any]] = []
    for value in values:
        if not isinstance(value, list):
            continue
        for item in value:
            if isinstance(item, dict) and (item.get("file_token") or item.get("url")):
                result.append(dict(item))
    return result


def freeze_record(record_id: str, fields: Dict[str, Any], structured_source: Dict[str, Any] | None = None) -> SourceSnapshot:
    prompt = _text(fields.get("视频生成提示词") or fields.get("短视频提示词"))
    duration = int(float(_text(fields.get("视频时长")) or 0) * 1000)
    manifest = _attachments(
        fields.get(name) for name in (
            "统一首帧（系统）", "人物参考图（系统）", "产品图片", "参考图"
        )
    )
    normalized = {
        "record_id": record_id,
        "product_id": _text(fields.get("产品编码") or fields.get("商品ID")),
        "script_id": _text(fields.get("脚本ID")),
        "source_kind": _text(fields.get("脚本来源")),
        "prompt": prompt,
        "duration_ms": duration,
        "target_country": _text(fields.get("目标国家")),
        "target_language": _text(fields.get("目标语言")),
        "publish_purpose": _text(fields.get("发布用途")),
        "cart_enabled": _text(fields.get("是否挂车")),
        "source_voiceover": _text(fields.get("口播_目标语言")),
        "source_voiceover_zh": _text(fields.get("口播_中文") or fields.get("口播_中文对照")),
        "structured_source": structured_source or {},
        "reference_group_id": _text(fields.get("参考图组ID") or (structured_source or {}).get("reference_group_id")),
        "reference_tokens": [item.get("file_token") or item.get("url") for item in manifest],
    }
    digest = hashlib.sha256(
        json.dumps(normalized, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return SourceSnapshot(
        record_id=record_id,
        script_id=normalized["script_id"],
        source_kind=normalized["source_kind"],
        raw_prompt=prompt,
        duration_ms=duration,
        target_country=normalized["target_country"],
        target_language=normalized["target_language"],
        publish_purpose=normalized["publish_purpose"],
        cart_enabled=normalized["cart_enabled"],
        source_voiceover=normalized["source_voiceover"],
        source_voiceover_zh=normalized["source_voiceover_zh"],
        structured_source=structured_source or {},
        reference_manifest=manifest,
        source_revision_hash=digest,
        product_id=normalized["product_id"],
        reference_selection=({"group_id": normalized["reference_group_id"], "status": "BOUND"}
                             if normalized["reference_group_id"] else {}),
    )
