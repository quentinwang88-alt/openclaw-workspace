#!/usr/bin/env python3
"""原创脚本到运行管理表的同步逻辑。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence

from core.bitable import TableRecord


SCRIPT_FIELD_SPECS: List[Dict[str, Any]] = [
    {"logical_name": "script_s1", "task_suffix": "S1", "aliases": ["脚本方向一", "脚本_S1", "脚本S1"]},
    {"logical_name": "script_s1_v1", "task_suffix": "S1V1", "aliases": ["脚本1变体1"]},
    {"logical_name": "script_s1_v2", "task_suffix": "S1V2", "aliases": ["脚本1变体2"]},
    {"logical_name": "script_s1_v3", "task_suffix": "S1V3", "aliases": ["脚本1变体3"]},
    {"logical_name": "script_s1_v4", "task_suffix": "S1V4", "aliases": ["脚本1变体4"]},
    {"logical_name": "script_s1_v5", "task_suffix": "S1V5", "aliases": ["脚本1变体5"]},
    {"logical_name": "script_s2", "task_suffix": "S2", "aliases": ["脚本方向二", "脚本_S2", "脚本S2"]},
    {"logical_name": "script_s2_v1", "task_suffix": "S2V1", "aliases": ["脚本2变体1"]},
    {"logical_name": "script_s2_v2", "task_suffix": "S2V2", "aliases": ["脚本2变体2"]},
    {"logical_name": "script_s2_v3", "task_suffix": "S2V3", "aliases": ["脚本2变体3"]},
    {"logical_name": "script_s2_v4", "task_suffix": "S2V4", "aliases": ["脚本2变体4"]},
    {"logical_name": "script_s2_v5", "task_suffix": "S2V5", "aliases": ["脚本2变体5"]},
    {"logical_name": "script_s3", "task_suffix": "S3", "aliases": ["脚本方向三", "脚本_S3", "脚本S3"]},
    {"logical_name": "script_s3_v1", "task_suffix": "S3V1", "aliases": ["脚本3变体1"]},
    {"logical_name": "script_s3_v2", "task_suffix": "S3V2", "aliases": ["脚本3变体2"]},
    {"logical_name": "script_s3_v3", "task_suffix": "S3V3", "aliases": ["脚本3变体3"]},
    {"logical_name": "script_s3_v4", "task_suffix": "S3V4", "aliases": ["脚本3变体4"]},
    {"logical_name": "script_s3_v5", "task_suffix": "S3V5", "aliases": ["脚本3变体5"]},
    {"logical_name": "script_s4", "task_suffix": "S4", "aliases": ["脚本方向四", "脚本_S4", "脚本S4"]},
    {"logical_name": "script_s4_v1", "task_suffix": "S4V1", "aliases": ["脚本4变体1"]},
    {"logical_name": "script_s4_v2", "task_suffix": "S4V2", "aliases": ["脚本4变体2"]},
    {"logical_name": "script_s4_v3", "task_suffix": "S4V3", "aliases": ["脚本4变体3"]},
    {"logical_name": "script_s4_v4", "task_suffix": "S4V4", "aliases": ["脚本4变体4"]},
    {"logical_name": "script_s4_v5", "task_suffix": "S4V5", "aliases": ["脚本4变体5"]},
]

SOURCE_FIELD_ALIASES: Dict[str, List[str]] = {
    "product_code": ["产品编码", "商品编码", "SKU", "Product Code"],
    "product_type": ["产品类型", "商品类型", "类目类型"],
    "target_language": ["目标语言", "语言", "target_language"],
    "business_category": ["一级类目", "业务大类", "主大类"],
    "product_params": ["产品参数信息", "参数信息", "产品参数"],
    "task_no": ["任务编号", "任务ID", "任务序号", "编号", "产品编码", "商品编码", "SKU", "Product Code"],
    "store_id": ["店铺ID", "店铺", "店铺编号", "店铺名称"],
    "product_id": ["产品ID", "产品编码", "商品编码", "SKU", "Product Code"],
    "parent_slot_1": ["所属母版1"],
    "parent_slot_2": ["所属母版2"],
    "parent_slot_3": ["所属母版3"],
    "parent_slot_4": ["所属母版4"],
    "direction_1": ["母版方向1"],
    "direction_2": ["母版方向2"],
    "direction_3": ["母版方向3"],
    "direction_4": ["母版方向4"],
    "product_images": ["产品图片", "商品图片", "图片"],
    "script_source": ["脚本来源", "来源"],
    "publish_purpose": ["发布用途", "用途"],
    "cart_enabled": ["是否挂车", "挂车"],
    "content_branch": ["内容分支"],
    "sync_enabled": ["是否可同步", "是否可同步脚本"],
    "sync_master_enabled": ["是否可同步母版"],
    "sync_variant_enabled": ["是否可同步子变体"],
    "sync_status": ["同步状态", "同步结果"],
    "sync_time": ["同步时间", "最近同步时间"],
}

for spec in SCRIPT_FIELD_SPECS:
    SOURCE_FIELD_ALIASES[spec["logical_name"]] = list(spec["aliases"])

TARGET_FIELD_ALIASES: Dict[str, List[str]] = {
    "task_name": ["任务名"],
    "prompt": ["提示词"],
    "reference_images": ["参考图"],
    "script_id": ["脚本ID"],
    "short_video_title": ["短视频标题", "标题"],
    "store_id": ["店铺ID"],
    "product_id": ["产品ID"],
    "parent_slot": ["所属母版"],
    "direction_label": ["母版方向"],
    "variant_strength": ["变体强度"],
    "script_source": ["脚本来源", "来源"],
    "publish_purpose": ["发布用途", "用途"],
    "cart_enabled": ["是否挂车", "挂车"],
    "content_branch": ["内容分支"],
    "reference_free": ["免参考图"],
    "task_status": ["任务状态", "状态"],
}

SCRIPT_ID_HEADER_PATTERN = re.compile(r"\A\s*【脚本ID】\s*\n-\s*[^\n\r]+(?:\r?\n){0,2}")


@dataclass
class ScriptSyncTask:
    source_record_id: str
    product_code: str
    script_slot: str
    task_name: str
    prompt_text: str
    reference_images: List[Dict[str, Any]]
    product_type: str = ""
    target_language: str = ""
    business_category: str = ""
    product_params: str = ""
    script_id: str = ""
    short_video_title: str = ""
    store_id: str = ""
    product_id: str = ""
    parent_slot: str = ""
    direction_label: str = ""
    variant_strength: str = ""
    script_source: str = ""
    publish_purpose: str = ""
    cart_enabled: str = ""
    content_branch: str = ""


def is_variant_slot(task_suffix: str) -> bool:
    return "V" in str(task_suffix or "").strip().upper()


def should_sync_slot(fields: Dict[str, Any], mapping: Dict[str, Optional[str]], task_suffix: str) -> bool:
    legacy_enabled = normalize_checkbox(fields.get(mapping["sync_enabled"])) if mapping.get("sync_enabled") else False
    master_enabled = normalize_checkbox(fields.get(mapping["sync_master_enabled"])) if mapping.get("sync_master_enabled") else False
    variant_enabled = normalize_checkbox(fields.get(mapping["sync_variant_enabled"])) if mapping.get("sync_variant_enabled") else False

    if legacy_enabled:
        return True

    if is_variant_slot(task_suffix):
        return variant_enabled

    return master_enabled


def has_any_sync_enabled(fields: Dict[str, Any], mapping: Dict[str, Optional[str]]) -> bool:
    return any(
        [
            normalize_checkbox(fields.get(mapping["sync_enabled"])) if mapping.get("sync_enabled") else False,
            normalize_checkbox(fields.get(mapping["sync_master_enabled"])) if mapping.get("sync_master_enabled") else False,
            normalize_checkbox(fields.get(mapping["sync_variant_enabled"])) if mapping.get("sync_variant_enabled") else False,
        ]
    )


def summarize_sync_scope(tasks: Sequence[ScriptSyncTask]) -> str:
    master_count = sum(1 for task in tasks if not is_variant_slot(task.script_slot))
    variant_count = sum(1 for task in tasks if is_variant_slot(task.script_slot))

    if master_count and variant_count:
        return f"母版+子变体（母版 {master_count} 条，子变体 {variant_count} 条）"
    if master_count:
        return f"母版（{master_count} 条）"
    if variant_count:
        return f"子变体（{variant_count} 条）"
    return "未识别同步范围"


def resolve_field_mapping(field_names: Sequence[str], aliases: Dict[str, List[str]]) -> Dict[str, Optional[str]]:
    mapping: Dict[str, Optional[str]] = {}
    for logical_name, candidates in aliases.items():
        mapping[logical_name] = next((candidate for candidate in candidates if candidate in field_names), None)
    return mapping


def validate_required_fields(mapping: Dict[str, Optional[str]], required_fields: Iterable[str]) -> None:
    missing = [name for name in required_fields if not mapping.get(name)]
    if missing:
        raise ValueError(f"缺少必要字段: {', '.join(missing)}")


def validate_script_fields(mapping: Dict[str, Optional[str]]) -> None:
    if not any(mapping.get(spec["logical_name"]) for spec in SCRIPT_FIELD_SPECS):
        raise ValueError("源表未找到任何脚本字段")


def normalize_checkbox(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "已勾选", "勾选", "checked"}
    return False


def is_nurture_task(task: ScriptSyncTask) -> bool:
    return (
        normalize_text(task.script_source) == "养号复刻"
        or normalize_text(task.publish_purpose) == "养号"
        or normalize_text(task.content_branch) == "非商品展示型"
    )


def extract_attachments(raw_value: Any) -> List[Dict[str, Any]]:
    attachments: List[Dict[str, Any]] = []
    if isinstance(raw_value, list):
        for item in raw_value:
            if isinstance(item, dict) and item.get("file_token"):
                attachments.append(item)
    elif isinstance(raw_value, dict) and raw_value.get("file_token"):
        attachments.append(raw_value)
    return attachments


def normalize_text(raw_value: Any) -> str:
    if raw_value is None:
        return ""
    return str(raw_value).strip()


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def compact_anchor_text(raw_value: Any, max_length: int = 80) -> str:
    text = normalize_text(raw_value)
    if not text:
        return ""

    text = text.replace("\r", "\n")
    segments = [segment.strip() for segment in text.splitlines() if segment.strip()]
    text = "；".join(segments) if segments else text
    text = " ".join(text.split())
    if len(text) <= max_length:
        return text
    return text[: max_length - 1].rstrip("，,；;、 ") + "…"


def remove_script_id_header(prompt_text: Any) -> str:
    text = normalize_text(prompt_text)
    if not text:
        return ""
    return SCRIPT_ID_HEADER_PATTERN.sub("", text, count=1).lstrip()


def prompt_has_script_id_header(prompt_text: Any) -> bool:
    return bool(SCRIPT_ID_HEADER_PATTERN.match(normalize_text(prompt_text)))


def prepend_script_id_header(prompt_text: Any, script_id: str) -> str:
    body = remove_script_id_header(prompt_text)
    script_id_text = normalize_text(script_id)
    if not script_id_text:
        return body
    return f"【脚本ID】\n- {script_id_text}\n\n{body}".rstrip()


def build_prompt_with_anchor(task: ScriptSyncTask) -> str:
    anchor_parts: List[str] = []
    product_type = compact_anchor_text(task.product_type, max_length=24)
    business_category = compact_anchor_text(task.business_category, max_length=16)
    product_params = compact_anchor_text(task.product_params, max_length=80)
    target_language = compact_anchor_text(task.target_language, max_length=24) or "目标国家当地语言"

    if product_type:
        anchor_parts.append(product_type)
    elif business_category:
        anchor_parts.append(business_category)

    if product_params:
        anchor_parts.append(product_params)

    language_guard = (
        f"【口播/字幕语言强制约束】目标语言：{target_language}。\n"
        f"所有会被视频模型朗读或显示的口播、旁白、字幕，必须使用{target_language}，不得使用中文。\n"
        "中文只能作为场景、动作、执行提醒、中文含义等说明文字，不能作为发声/字幕内容。\n"
        f"如果下方脚本的“字幕/旁白”里仍出现中文，请先翻译成{target_language}后再生成视频。"
    )

    if not anchor_parts:
        prompt = f"{language_guard}\n\n{task.prompt_text}"
        return prepend_script_id_header(prompt, task.script_id)

    anchor_text = "｜".join(anchor_parts)
    prompt = f"产品锚点：{anchor_text}\n{language_guard}\n\n{task.prompt_text}"
    return prepend_script_id_header(prompt, task.script_id)


DEFAULT_DIRECTION_LABELS = {
    1: "日常轻分享流",
    2: "问题解决流",
    3: "场景代入流",
    4: "结论先行流",
}


def _direction_index_from_suffix(task_suffix: str) -> int:
    text = str(task_suffix or "").strip().upper()
    if not text.startswith("S") or len(text) < 2 or not text[1].isdigit():
        return 1
    return int(text[1])


def _variant_no_from_suffix(task_suffix: str) -> Optional[int]:
    text = str(task_suffix or "").strip().upper()
    if "V" not in text:
        return None
    try:
        return int(text.split("V", 1)[1])
    except ValueError:
        return None


def _fallback_task_no(fields: Dict[str, Any], mapping: Dict[str, Optional[str]], record_id: str) -> str:
    for logical_name in ("task_no", "product_id", "product_code"):
        field_name = mapping.get(logical_name)
        value = normalize_text(fields.get(field_name)) if field_name else ""
        if value:
            return value
    return record_id


def _parent_slot_value(fields: Dict[str, Any], mapping: Dict[str, Optional[str]], direction_index: int) -> str:
    field_name = mapping.get(f"parent_slot_{direction_index}")
    value = normalize_text(fields.get(field_name)) if field_name else ""
    return value or f"M{direction_index}"


def _direction_label_value(fields: Dict[str, Any], mapping: Dict[str, Optional[str]], direction_index: int) -> str:
    field_name = mapping.get(f"direction_{direction_index}")
    value = normalize_text(fields.get(field_name)) if field_name else ""
    return value or DEFAULT_DIRECTION_LABELS.get(direction_index, "")


def _variant_strength_value(variant_no: Optional[int]) -> str:
    if variant_no is None:
        return "母版"
    if variant_no in {1, 2, 3}:
        return "轻变体"
    return "中变体"


def _build_script_id(task_no: str, parent_slot: str, variant_no: Optional[int]) -> str:
    suffix = "M" if variant_no is None else f"V{variant_no}"
    return f"{task_no}_{parent_slot}_{suffix}".replace(" ", "")


def derive_task_metadata(
    record: TableRecord,
    mapping: Dict[str, Optional[str]],
    task_suffix: str,
    metadata_lookup: Optional[Dict[tuple, Dict[str, str]]] = None,
) -> Dict[str, str]:
    fields = record.fields
    direction_index = _direction_index_from_suffix(task_suffix)
    variant_no = _variant_no_from_suffix(task_suffix)
    derived = {
        "store_id": normalize_text(fields.get(mapping.get("store_id"))),
        "product_id": normalize_text(fields.get(mapping.get("product_id")))
        or normalize_text(fields.get(mapping.get("product_code"))),
        "parent_slot": _parent_slot_value(fields, mapping, direction_index),
        "direction_label": _direction_label_value(fields, mapping, direction_index),
        "variant_strength": _variant_strength_value(variant_no),
        "script_id": "",
        "short_video_title": "",
    }
    task_no = _fallback_task_no(fields, mapping, record.record_id)
    derived["script_id"] = _build_script_id(task_no, derived["parent_slot"], variant_no)
    if metadata_lookup:
        derived.update(metadata_lookup.get((record.record_id, task_suffix), {}))
    return derived


def build_sync_tasks(
    records: Sequence[TableRecord],
    mapping: Dict[str, Optional[str]],
    *,
    product_code: Optional[str] = None,
    record_id: Optional[str] = None,
    limit: Optional[int] = None,
    metadata_lookup: Optional[Dict[tuple, Dict[str, str]]] = None,
) -> List[ScriptSyncTask]:
    tasks: List[ScriptSyncTask] = []

    for record in records:
        if record_id and record.record_id != record_id:
            continue

        fields = record.fields
        product_code_value = normalize_text(fields.get(mapping["product_code"])) if mapping.get("product_code") else ""
        task_name_base = product_code_value or _fallback_task_no(fields, mapping, record.record_id)
        if product_code and task_name_base != product_code:
            continue
        if not task_name_base:
            continue

        if not has_any_sync_enabled(fields, mapping):
            continue

        attachments = extract_attachments(fields.get(mapping["product_images"])) if mapping.get("product_images") else []
        for spec in SCRIPT_FIELD_SPECS:
            if not should_sync_slot(fields, mapping, spec["task_suffix"]):
                continue
            field_name = mapping.get(spec["logical_name"])
            prompt_text = normalize_text(fields.get(field_name)) if field_name else ""
            if not prompt_text:
                continue
            tasks.append(
                ScriptSyncTask(
                    source_record_id=record.record_id,
                    product_code=task_name_base,
                    script_slot=spec["task_suffix"],
                    task_name=f"{task_name_base}.{spec['task_suffix']}",
                    prompt_text=prompt_text,
                    reference_images=attachments,
                    product_type=normalize_text(fields.get(mapping.get("product_type"))) if mapping.get("product_type") else "",
                    target_language=normalize_text(fields.get(mapping.get("target_language"))) if mapping.get("target_language") else "",
                    business_category=normalize_text(fields.get(mapping.get("business_category"))) if mapping.get("business_category") else "",
                    product_params=normalize_text(fields.get(mapping.get("product_params"))) if mapping.get("product_params") else "",
                    script_source=normalize_text(fields.get(mapping.get("script_source"))) if mapping.get("script_source") else "",
                    publish_purpose=normalize_text(fields.get(mapping.get("publish_purpose"))) if mapping.get("publish_purpose") else "",
                    cart_enabled=normalize_text(fields.get(mapping.get("cart_enabled"))) if mapping.get("cart_enabled") else "",
                    content_branch=normalize_text(fields.get(mapping.get("content_branch"))) if mapping.get("content_branch") else "",
                    **derive_task_metadata(record, mapping, spec["task_suffix"], metadata_lookup=metadata_lookup),
                )
            )
            if limit is not None and len(tasks) >= limit:
                return tasks

    return tasks


def build_target_fields(
    task: ScriptSyncTask,
    mapping: Dict[str, Optional[str]],
    *,
    include_publish_metadata: bool = False,
) -> Dict[str, Any]:
    fields: Dict[str, Any] = {}
    if mapping.get("task_name"):
        fields[mapping["task_name"]] = task.task_name
    if mapping.get("prompt"):
        fields[mapping["prompt"]] = build_prompt_with_anchor(task)
    if mapping.get("reference_images"):
        fields[mapping["reference_images"]] = task.reference_images
    if mapping.get("script_id") and task.script_id:
        fields[mapping["script_id"]] = task.script_id
    if include_publish_metadata:
        if mapping.get("short_video_title") and task.short_video_title:
            fields[mapping["short_video_title"]] = task.short_video_title
        if mapping.get("store_id") and task.store_id:
            fields[mapping["store_id"]] = task.store_id
        if mapping.get("product_id") and task.product_id:
            fields[mapping["product_id"]] = task.product_id
        if mapping.get("parent_slot") and task.parent_slot:
            fields[mapping["parent_slot"]] = task.parent_slot
        if mapping.get("direction_label") and task.direction_label:
            fields[mapping["direction_label"]] = task.direction_label
        if mapping.get("variant_strength") and task.variant_strength:
            fields[mapping["variant_strength"]] = task.variant_strength
    if mapping.get("script_source") and task.script_source:
        fields[mapping["script_source"]] = task.script_source
    if mapping.get("publish_purpose") and task.publish_purpose:
        fields[mapping["publish_purpose"]] = task.publish_purpose
    if mapping.get("cart_enabled") and task.cart_enabled:
        fields[mapping["cart_enabled"]] = task.cart_enabled
    if mapping.get("content_branch") and task.content_branch:
        fields[mapping["content_branch"]] = task.content_branch
    if mapping.get("reference_free") and is_nurture_task(task):
        fields[mapping["reference_free"]] = "是"
    return fields


def build_source_success_fields(
    mapping: Dict[str, Optional[str]],
    synced_count: int,
    synced_at: str,
    *,
    sync_scope: str = "",
    cleared_master: bool = False,
    cleared_variant: bool = False,
    cleared_legacy: bool = False,
) -> Dict[str, Any]:
    fields: Dict[str, Any] = {}
    if mapping.get("sync_enabled") and cleared_legacy:
        fields[mapping["sync_enabled"]] = False
    if mapping.get("sync_master_enabled") and cleared_master:
        fields[mapping["sync_master_enabled"]] = False
    if mapping.get("sync_variant_enabled") and cleared_variant:
        fields[mapping["sync_variant_enabled"]] = False
    if mapping.get("sync_status"):
        scope_text = f"{sync_scope}；" if sync_scope else ""
        status_text = f"同步成功：{scope_text}新增 {synced_count} 条；同步时间：{synced_at}"
        fields[mapping["sync_status"]] = status_text
    if mapping.get("sync_time"):
        fields[mapping["sync_time"]] = synced_at
    return fields


def build_source_failure_fields(
    mapping: Dict[str, Optional[str]],
    error_message: str,
    synced_at: str,
    *,
    sync_scope: str = "",
) -> Dict[str, Any]:
    fields: Dict[str, Any] = {}
    if mapping.get("sync_status"):
        scope_text = f"{sync_scope}；" if sync_scope else ""
        fields[mapping["sync_status"]] = f"同步失败：{scope_text}{error_message}；失败时间：{synced_at}"
    if mapping.get("sync_time"):
        fields[mapping["sync_time"]] = synced_at
    return fields


def batch_records(records: Sequence[Dict[str, Any]], batch_size: int = 200) -> List[List[Dict[str, Any]]]:
    size = min(max(batch_size, 1), 500)
    return [list(records[start:start + size]) for start in range(0, len(records), size)]
