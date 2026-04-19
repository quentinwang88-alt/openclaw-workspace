#!/usr/bin/env python3
"""统一脚本 ID / 历史内容 ID 兼容工具。"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional, Tuple


DEFAULT_PARENT_SLOTS = {
    1: "M1",
    2: "M2",
    3: "M3",
    4: "M4",
}

LEGACY_CONTENT_ID_RE = re.compile(r"^\d{6}$")
STRUCTURED_SCRIPT_ID_RE = re.compile(r"^[A-Za-z0-9-]+_[A-Za-z0-9-]+_(?:M|V\d+)$")
SCRIPT_ID_HEADER_RE = re.compile(r"^【(?:内容ID|脚本ID)】\s*\n-\s*([^\n]+)\s*(?:\n|$)", re.MULTILINE)
SCRIPT_ID_HEADER_BLOCK_RE = re.compile(r"^【(?:内容ID|脚本ID)】\s*\n-\s*[^\n]+\n*", re.MULTILINE)


def normalize_text(raw_value: Any) -> str:
    if raw_value is None:
        return ""
    return str(raw_value).strip()


def normalize_identifier_piece(raw_value: Any) -> str:
    return re.sub(r"\s+", "", normalize_text(raw_value))


def is_legacy_content_id(value: Any) -> bool:
    return bool(LEGACY_CONTENT_ID_RE.fullmatch(normalize_text(value)))


def is_structured_script_id(value: Any) -> bool:
    return bool(STRUCTURED_SCRIPT_ID_RE.fullmatch(normalize_text(value)))


def is_valid_unified_id(value: Any) -> bool:
    return is_legacy_content_id(value) or is_structured_script_id(value)


def extract_unified_id_from_text(text: str) -> str:
    matched = SCRIPT_ID_HEADER_RE.match(str(text or "").strip())
    return normalize_text(matched.group(1)) if matched else ""


def strip_unified_id_block(text: str) -> str:
    return SCRIPT_ID_HEADER_BLOCK_RE.sub("", str(text or "")).strip()


def prepend_unified_id(text: str, script_id: str) -> str:
    stripped = strip_unified_id_block(text)
    if not stripped:
        return stripped
    return f"【脚本ID】\n- {script_id}\n\n{stripped}"


def build_script_id(task_no: str, parent_slot: str, variant_no: Optional[int]) -> str:
    suffix = "M" if variant_no is None else f"V{variant_no}"
    return f"{normalize_identifier_piece(task_no)}_{normalize_identifier_piece(parent_slot)}_{suffix}"


def resolve_task_no(context: Dict[str, Any], record_id: str = "") -> str:
    for key in ("task_no", "product_id", "product_code"):
        value = normalize_identifier_piece(context.get(key))
        if value:
            return value
    return normalize_identifier_piece(record_id) or "unknown"


def resolve_parent_slot(context: Dict[str, Any], script_index: int) -> str:
    key = f"parent_slot_{script_index}"
    value = normalize_identifier_piece(context.get(key))
    return value or DEFAULT_PARENT_SLOTS.get(script_index, f"M{script_index}")


def build_script_id_from_context(
    context: Dict[str, Any],
    script_index: int,
    variant_no: Optional[int],
    record_id: str = "",
) -> str:
    return build_script_id(
        task_no=resolve_task_no(context, record_id=record_id),
        parent_slot=resolve_parent_slot(context, script_index),
        variant_no=variant_no,
    )


def parse_slot_from_logical_name(logical_name: str) -> Tuple[Optional[int], Optional[int]]:
    text = normalize_text(logical_name)
    matched = re.fullmatch(r"(?:script|video_prompt)_s(\d)", text)
    if matched:
        return int(matched.group(1)), None
    matched = re.fullmatch(r"script_(\d)_variant_(\d+)", text)
    if matched:
        return int(matched.group(1)), int(matched.group(2))
    return None, None


def build_context_from_fields(
    fields: Dict[str, Any],
    mapping: Dict[str, Optional[str]],
) -> Dict[str, str]:
    context: Dict[str, str] = {}
    for key in ("task_no", "product_code", "product_id"):
        field_name = mapping.get(key)
        context[key] = normalize_text(fields.get(field_name)) if field_name else ""
    for index in range(1, 5):
        field_name = mapping.get(f"parent_slot_{index}")
        context[f"parent_slot_{index}"] = normalize_text(fields.get(field_name)) if field_name else ""
    return context


def build_script_id_from_fields(
    fields: Dict[str, Any],
    mapping: Dict[str, Optional[str]],
    script_index: int,
    variant_no: Optional[int],
    record_id: str = "",
) -> str:
    context = build_context_from_fields(fields, mapping)
    return build_script_id_from_context(context, script_index, variant_no, record_id=record_id)
