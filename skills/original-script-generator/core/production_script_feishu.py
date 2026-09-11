"""Feishu workbench schema and idempotent batch-script export."""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from core.bitable import FeishuBitableClient, TaskRecord, extract_attachments
from core.product_type_resolution import load_type_registry, normalize_product_type
from core.production_script_renderer import build_production_projection


TOP_CATEGORY_OPTIONS: Tuple[str, ...] = ("女装", "配饰")
PRODUCT_TYPE_OPTIONS: Tuple[str, ...] = tuple(
    definition.display_name for definition in load_type_registry().type_definitions
)
TEST_PHASE_OPTIONS: Tuple[str, ...] = ("初测", "复测", "终测", "放大观察")
VIDEO_SPEC_OPTIONS: Tuple[str, ...] = (
    "15秒原创", "20秒", "25秒", "30秒", "35秒", "40秒", "45秒",
)
LONGFORM_SCENE_MODE_OPTIONS: Tuple[str, ...] = ("自动", "单场景", "双场景")
LONGFORM_SCENE_MODE_TO_CODE = {
    "自动": "auto",
    "单场景": "single",
    "双场景": "multi",
}
VIDEO_FORMAT_OPTIONS: Tuple[str, ...] = ("15秒原创", "长视频")
LONGFORM_STATUS_OPTIONS: Tuple[str, ...] = (
    "不适用", "待审核", "待生产", "生成中", "已完成", "生成失败",
)
OPERATION_TASK_STATUS_OPTIONS: Tuple[str, ...] = (
    "待执行",
    "执行中-规划",
    "执行中-脚本生成",
    "已完成",
    "部分完成",
    "失败",
)
TEST_PHASE_TO_CODE = {
    "初测": "INITIAL",
    "复测": "RETEST",
    "终测": "FINAL",
    "放大观察": "SCALE_OBSERVE",
}
FIRST_FRAME_STATUS_OPTIONS: Tuple[str, ...] = (
    "用户未选择",
    "待生成",
    "生成中",
    "缓存复用",
    "已就绪",
    "生成失败",
    "缺少人物参考图",
    "不适用",
)
OUTFIT_SCENE_MATCH_OPTIONS: Tuple[str, ...] = (
    "已匹配",
    "未配置偏好",
    "已回退",
    "不适用",
)


OPERATION_TASK_FIELD_NAMES = {
    "task_id": "任务ID",
    "product_code": "产品编码（需填写）",
    "product_images": "产品图片（需填写）",
    "store_id": "店铺ID（需填写）",
    "top_category": "一级类目（需填写）",
    "product_type": "产品类型（需填写）",
    "target_country": "目标国家",
    "target_language": "目标语言",
    "requested_count": "生成数量（需填写）",
    "test_phase": "测试阶段（可选，默认初测）",
    "video_spec": "视频规格（需填写）",
    "longform_scene_mode": "长视频场景模式（可选）",
    "duration_seconds": "视频时长",
    "random_seed": "随机种子（系统，可留空）",
    "status": "任务状态（需填写，仅选择待执行）",
    "batch_id": "批次ID",
    "planned_count": "计划数量",
    "ready_count": "完成数量",
    "failed_count": "失败数量",
    "summary": "输出摘要",
    "error": "错误信息",
    "last_run_at": "最近执行时间",
}

# Existing tables are migrated in place. These are display-name changes only;
# record values and field types remain unchanged.
OPERATION_TASK_FIELD_RENAMES = {
    "产品编码": "产品编码（需填写）",
    "产品图片": "产品图片（需填写）",
    "店铺ID": "店铺ID（需填写）",
    "一级类目": "一级类目（需填写）",
    "产品类型": "产品类型（需填写）",
    "生成数量": "生成数量（需填写）",
    "测试阶段": "测试阶段（可选，默认初测）",
    "测试阶段（需填写）": "测试阶段（可选，默认初测）",
    "随机种子": "随机种子（系统，可留空）",
    "任务状态": "任务状态（需填写，仅选择待执行）",
    "任务状态（需填写）": "任务状态（需填写，仅选择待执行）",
}

PRODUCTION_SCRIPT_FIELD_NAMES = {
    "script_id": "脚本ID",
    "product_code": "产品编码",
    "product_images": "产品图片",
    "store_id": "店铺ID",
    "batch_id": "批次ID",
    "batch_item_id": "批次ItemID",
    "item_index": "批次序号",
    "script_title": "脚本标题",
    "duration_seconds": "视频时长",
    "video_format": "视频形态（系统）",
    "segment_plan": "分段计划（系统）",
    "longform_job_id": "长视频任务ID（系统）",
    "longform_status": "长视频执行状态（系统）",
    "longform_first_frame": "长视频首帧（系统）",
    "longform_video": "长视频成片（系统）",
    "longform_error": "长视频错误信息（系统）",
    "top_category": "一级类目",
    "product_type": "产品类型",
    "target_country": "目标国家",
    "target_language": "目标语言",
    "core_selling_point": "核心卖点",
    "hook_type": "钩子类型",
    "macro_family": "结构家族",
    "carrier_mode": "承载方式",
    "creative_signature": "创意签名",
    "scene_summary": "场景摘要",
    "target_voiceover": "口播_目标语言",
    "chinese_voiceover": "口播_中文",
    "complete_script": "完整生产脚本",
    "video_prompt": "视频生成提示词",
    "script_type": "脚本类型",
    "processing_status": "处理状态",
    "production_enabled": "进入生产",
    "publish_confirmed": "确认发布",
    "review_note": "审核意见",
    "sync_result": "同步结果",
    "sync_time": "同步时间",
    "run_task_id": "运行任务ID",
    "cluster_id": "结构簇ID",
    "cluster_version": "结构簇版本",
    "selection_run_id": "结构选择RunID",
    "direction_assignment_id": "方向分配ID",
    "content_bundle_id": "内容包ID",
    "creative_contract_id": "创意合同ID",
    "persona_id": "人物模板ID（系统）",
    "persona_name": "人物模板名称（系统）",
    "persona_contract_json": "人物模板合同_JSON（系统）",
    "outfit_template_id": "穿搭模板ID（系统）",
    "outfit_template_name": "穿搭模板名称（系统）",
    "outfit_accessories": "实际配饰（系统）",
    "outfit_scene_match": "穿搭场景匹配（系统）",
    "outfit_scene_contract_json": "穿搭场景关联合同_JSON（系统）",
    "outfit_persona_match": "人物穿搭匹配（系统）",
    "outfit_persona_contract_json": "人物穿搭关联合同_JSON（系统）",
    "reference_strategy": "视觉参考模式（系统）",
    "first_frame_requested": "生成首帧（需勾选）",
    "composite_first_frame": "统一首帧（系统）",
    "first_frame_status": "首帧准备状态（系统）",
    "input_snapshot_hash": "输入快照哈希",
    "model_version": "模型版本",
}


@dataclass(frozen=True)
class FieldSpec:
    name: str
    field_type: int = 1
    ui_type: str = "Text"
    property: Optional[Dict[str, Any]] = None


def _single(name: str, options: Sequence[str]) -> FieldSpec:
    return FieldSpec(
        name=name,
        field_type=3,
        ui_type="SingleSelect",
        property={"options": [{"name": option} for option in options]},
    )


OPERATION_TASK_FIELDS: Tuple[FieldSpec, ...] = (
    FieldSpec("产品编码（需填写）"),
    FieldSpec("产品图片（需填写）", 17, "Attachment"),
    FieldSpec("店铺ID（需填写）"),
    _single("一级类目（需填写）", TOP_CATEGORY_OPTIONS),
    _single("产品类型（需填写）", PRODUCT_TYPE_OPTIONS),
    FieldSpec("目标国家"),
    FieldSpec("目标语言"),
    FieldSpec("生成数量（需填写）", 2, "Number"),
    _single("测试阶段（可选，默认初测）", TEST_PHASE_OPTIONS),
    _single("视频规格（需填写）", VIDEO_SPEC_OPTIONS),
    _single("长视频场景模式（可选）", LONGFORM_SCENE_MODE_OPTIONS),
    # Kept as a hidden compatibility value for historical task rows and
    # downstream snapshots.  New operator input is the controlled 视频规格.
    FieldSpec("视频时长", 2, "Number"),
    FieldSpec("随机种子（系统，可留空）", 2, "Number"),
    _single(
        "任务状态（需填写，仅选择待执行）",
        OPERATION_TASK_STATUS_OPTIONS,
    ),
    FieldSpec("批次ID"),
    FieldSpec("计划数量", 2, "Number"),
    FieldSpec("完成数量", 2, "Number"),
    FieldSpec("失败数量", 2, "Number"),
    FieldSpec("输出摘要"),
    FieldSpec("错误信息"),
    FieldSpec(
        "最近执行时间",
        5,
        "DateTime",
        {"date_formatter": "yyyy-MM-dd HH:mm", "auto_fill": False},
    ),
)


def rename_known_fields(
    client: FeishuBitableClient,
    renames: Dict[str, str],
) -> List[str]:
    """Rename known legacy fields in place without creating duplicates."""
    fields = client.list_fields()
    names = {str(field.get("field_name") or "") for field in fields}
    renamed: List[str] = []
    for field in fields:
        old_name = str(field.get("field_name") or "")
        new_name = renames.get(old_name)
        if not new_name or new_name in names:
            continue
        client.update_field_name(
            str(field.get("field_id")),
            new_name,
            field_type=int(field.get("type") or 1),
        )
        names.discard(old_name)
        names.add(new_name)
        renamed.append(f"{old_name} -> {new_name}")
    return renamed


def ensure_single_select_options(
    client: FeishuBitableClient,
    option_sets: Dict[str, Sequence[str]],
) -> List[str]:
    """Keep operator input fields as controlled single-select enums."""
    fields = client.list_fields()
    updated: List[str] = []
    for field in fields:
        name = str(field.get("field_name") or "")
        desired = option_sets.get(name)
        if not desired:
            continue
        current_options = [
            str(option.get("name") or "")
            for option in ((field.get("property") or {}).get("options") or [])
        ]
        if int(field.get("type") or 0) == 3 and current_options == list(desired):
            continue
        client.update_field_name(
            str(field.get("field_id")),
            name,
            field_type=3,
            property={"options": [{"name": option} for option in desired]},
        )
        updated.append(name)
    return updated


def normalize_operation_top_category(value: Any) -> str:
    text = str(value or "").strip()
    if text in TOP_CATEGORY_OPTIONS:
        return text
    if text in {"轻上装", "上装", "服装", "服饰"}:
        return "女装"
    if text in {
        "首饰", "珠宝", "饰品", "发饰", "头饰", "服饰配件",
        "围巾", "帽子", "围巾帽子套装",
    }:
        return "配饰"
    return text


def normalize_operation_product_type(value: Any, top_category: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    context = normalize_product_type(text, top_category)
    if not context.recognized_by_registry:
        return text
    return load_type_registry().type_by_canonical[context.canonical_type].display_name


def normalize_operation_test_phase(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "INITIAL"
    if text in TEST_PHASE_TO_CODE:
        return TEST_PHASE_TO_CODE[text]
    if text in TEST_PHASE_TO_CODE.values():
        return text
    return "INITIAL"


def normalize_operation_video_spec(value: Any, legacy_duration: Any = None) -> Tuple[str, int]:
    """Return the controlled display value and its exact duration.

    Historical rows only have the numeric ``视频时长`` field.  They continue to
    resolve safely without requiring a table migration, while all new rows use
    the single-select field and cannot enter arbitrary unsupported durations.
    """

    text = str(value or "").strip()
    if text in VIDEO_SPEC_OPTIONS:
        if text == "15秒原创":
            return text, 15
        return text, int(text.removesuffix("秒"))
    try:
        duration = int(round(float(legacy_duration or 15)))
    except (TypeError, ValueError):
        duration = 15
    if duration == 15:
        return "15秒原创", 15
    candidate = f"{duration}秒"
    if candidate in VIDEO_SPEC_OPTIONS:
        return candidate, duration
    return candidate, duration


def normalize_longform_scene_mode(value: Any) -> str:
    text = str(value or "").strip()
    if text in LONGFORM_SCENE_MODE_TO_CODE:
        return LONGFORM_SCENE_MODE_TO_CODE[text]
    if text in LONGFORM_SCENE_MODE_TO_CODE.values():
        return text
    return "auto"

PRODUCTION_SCRIPT_FIELDS: Tuple[FieldSpec, ...] = (
    FieldSpec("产品编码"),
    FieldSpec("产品图片", 17, "Attachment"),
    FieldSpec("店铺ID"),
    FieldSpec("批次ID"),
    FieldSpec("批次ItemID"),
    FieldSpec("批次序号", 2, "Number"),
    FieldSpec("脚本标题"),
    FieldSpec("视频时长", 2, "Number"),
    _single("视频形态（系统）", VIDEO_FORMAT_OPTIONS),
    FieldSpec("分段计划（系统）"),
    FieldSpec("长视频任务ID（系统）"),
    _single("长视频执行状态（系统）", LONGFORM_STATUS_OPTIONS),
    FieldSpec("长视频首帧（系统）", 17, "Attachment"),
    FieldSpec("长视频成片（系统）", 17, "Attachment"),
    FieldSpec("长视频错误信息（系统）"),
    FieldSpec("一级类目"),
    FieldSpec("产品类型"),
    FieldSpec("目标国家"),
    FieldSpec("目标语言"),
    FieldSpec("核心卖点"),
    FieldSpec("钩子类型"),
    FieldSpec("结构家族"),
    FieldSpec("承载方式"),
    FieldSpec("创意签名"),
    FieldSpec("场景摘要"),
    FieldSpec("口播_目标语言"),
    FieldSpec("口播_中文"),
    FieldSpec("完整生产脚本"),
    FieldSpec("视频生成提示词"),
    _single("脚本类型", ("原创脚本",)),
    _single("处理状态", ("待审核", "已选用", "不采用", "生成失败", "已送生产", "同步失败")),
    FieldSpec("进入生产", 7, "Checkbox"),
    FieldSpec("确认发布", 7, "Checkbox"),
    FieldSpec("审核意见"),
    FieldSpec("同步结果"),
    FieldSpec(
        "同步时间",
        5,
        "DateTime",
        {"date_formatter": "yyyy-MM-dd HH:mm", "auto_fill": False},
    ),
    FieldSpec("运行任务ID"),
    FieldSpec("结构簇ID", 2, "Number"),
    FieldSpec("结构簇版本"),
    FieldSpec("结构选择RunID"),
    FieldSpec("方向分配ID"),
    FieldSpec("内容包ID"),
    FieldSpec("创意合同ID"),
    FieldSpec("人物模板ID（系统）"),
    FieldSpec("人物模板名称（系统）"),
    FieldSpec("人物模板合同_JSON（系统）"),
    FieldSpec("穿搭模板ID（系统）"),
    FieldSpec("穿搭模板名称（系统）"),
    FieldSpec("实际配饰（系统）"),
    _single(
        "穿搭场景匹配（系统）",
        OUTFIT_SCENE_MATCH_OPTIONS,
    ),
    FieldSpec("穿搭场景关联合同_JSON（系统）"),
    FieldSpec("人物穿搭匹配（系统）"),
    FieldSpec("人物穿搭关联合同_JSON（系统）"),
    FieldSpec("视觉参考模式（系统）"),
    FieldSpec("生成首帧（需勾选）", 7, "Checkbox"),
    FieldSpec("统一首帧（系统）", 17, "Attachment"),
    _single("首帧准备状态（系统）", FIRST_FRAME_STATUS_OPTIONS),
    FieldSpec("输入快照哈希"),
    FieldSpec("模型版本"),
)


def ensure_fields(
    client: FeishuBitableClient,
    *,
    primary_field_name: str,
    specs: Sequence[FieldSpec],
) -> List[str]:
    fields = client.list_fields()
    names = {str(field.get("field_name") or "") for field in fields}
    if primary_field_name not in names:
        default_primary = next(
            (
                field
                for field in fields
                if str(field.get("field_name") or "") in {"文本", "Text"}
            ),
            None,
        )
        if default_primary:
            client.update_field_name(str(default_primary.get("field_id")), primary_field_name)
            names.discard(str(default_primary.get("field_name") or ""))
            names.add(primary_field_name)
        else:
            raise RuntimeError(f"表格缺少可重命名的主字段，无法创建【{primary_field_name}】")

    for spec in specs:
        if spec.name in names:
            continue
        client.create_field(
            spec.name,
            field_type=spec.field_type,
            ui_type=spec.ui_type,
            property=spec.property,
        )
        names.add(spec.name)
    return sorted(names)


def now_millis() -> int:
    return int(time.time() * 1000)


def _fields_by_script_id(
    records: Iterable[TaskRecord], field_name: str = "脚本ID"
) -> Dict[str, TaskRecord]:
    output: Dict[str, TaskRecord] = {}
    for record in records:
        script_id = str(record.fields.get(field_name) or "").strip()
        if script_id and script_id not in output:
            output[script_id] = record
    return output


def _records_by_batch_item_id(
    records: Iterable[TaskRecord],
    *,
    batch_field_name: str = "批次ID",
    item_field_name: str = "批次ItemID",
) -> Dict[Tuple[str, str], TaskRecord]:
    """Return the canonical row for each frozen batch item.

    ``script_id`` is generated from the complete projection and can legitimately
    change when an interrupted run resumes after an upstream renderer change.
    The frozen ``batch_id + batch_item_id`` pair is the stable identity of the
    planned slot, so it must win when exporting to the human review table.
    Keeping the first row also makes legacy duplicate rows harmless until they
    are explicitly archived by a maintenance action.
    """

    output: Dict[Tuple[str, str], TaskRecord] = {}
    for record in records:
        batch_id = str(record.fields.get(batch_field_name) or "").strip()
        batch_item_id = str(record.fields.get(item_field_name) or "").strip()
        if not batch_id or not batch_item_id:
            continue
        output.setdefault((batch_id, batch_item_id), record)
    return output


def transfer_attachments(
    source_client: FeishuBitableClient,
    target_client: FeishuBitableClient,
    attachments: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    transferred: List[Dict[str, Any]] = []
    for attachment in attachments:
        content, name, content_type, size = source_client.download_attachment_bytes(attachment)
        transferred.append(
            target_client.upload_attachment(
                content=content,
                file_name=name,
                content_type=content_type,
                size=size,
            )
        )
    return transferred


def projection_to_feishu_fields(
    projection: Dict[str, Any],
    *,
    product_images: Sequence[Dict[str, Any]] = (),
    store_id: str = "",
    include_workflow_defaults: bool = True,
) -> Dict[str, Any]:
    f = PRODUCTION_SCRIPT_FIELD_NAMES
    values: Dict[str, Any] = {
        f["script_id"]: projection.get("script_id", ""),
        f["product_code"]: projection.get("product_code", ""),
        f["product_images"]: list(product_images),
        f["store_id"]: store_id,
        f["batch_id"]: projection.get("batch_id", ""),
        f["batch_item_id"]: projection.get("batch_item_id", ""),
        f["item_index"]: projection.get("item_index", 0),
        f["script_title"]: projection.get("script_title", ""),
        f["duration_seconds"]: projection.get("duration_seconds", 15),
        f["video_format"]: projection.get("video_format", "15秒原创"),
        f["segment_plan"]: projection.get("segment_plan", ""),
        f["longform_job_id"]: projection.get("longform_job_id", ""),
        f["longform_status"]: projection.get("longform_status", "不适用"),
        f["longform_error"]: projection.get("longform_error", ""),
        f["top_category"]: projection.get("top_category", ""),
        f["product_type"]: projection.get("product_type", ""),
        f["target_country"]: projection.get("target_country", ""),
        f["target_language"]: projection.get("target_language", ""),
        f["core_selling_point"]: projection.get("core_selling_point", ""),
        f["hook_type"]: projection.get("hook_type", ""),
        f["macro_family"]: projection.get("macro_family", ""),
        f["carrier_mode"]: projection.get("carrier_mode", ""),
        f["creative_signature"]: projection.get("creative_signature", ""),
        f["scene_summary"]: projection.get("scene_summary", ""),
        f["target_voiceover"]: projection.get("target_voiceover", ""),
        f["chinese_voiceover"]: projection.get("chinese_voiceover", ""),
        f["complete_script"]: projection.get("complete_script", ""),
        f["video_prompt"]: projection.get("video_prompt", ""),
        f["script_type"]: projection.get("script_type", "原创脚本"),
        f["cluster_version"]: projection.get("cluster_version", ""),
        f["selection_run_id"]: projection.get("selection_run_id", ""),
        f["direction_assignment_id"]: projection.get("direction_assignment_id", ""),
        f["content_bundle_id"]: projection.get("content_bundle_id", ""),
        f["creative_contract_id"]: projection.get("creative_contract_id", ""),
        f["persona_id"]: projection.get("persona_id", ""),
        f["persona_name"]: projection.get("persona_name", ""),
        f["persona_contract_json"]: projection.get("persona_contract_json", ""),
        f["outfit_template_id"]: projection.get("outfit_template_id", ""),
        f["outfit_template_name"]: projection.get("outfit_template_name", ""),
        f["outfit_accessories"]: projection.get("outfit_accessories", ""),
        f["outfit_scene_match"]: projection.get("outfit_scene_match", ""),
        f["outfit_scene_contract_json"]: projection.get(
            "outfit_scene_contract_json", ""
        ),
        f["outfit_persona_match"]: projection.get("outfit_persona_match", ""),
        f["outfit_persona_contract_json"]: projection.get(
            "outfit_persona_contract_json", ""
        ),
        f["reference_strategy"]: projection.get("reference_strategy", ""),
        f["input_snapshot_hash"]: projection.get("input_snapshot_hash", ""),
        f["model_version"]: projection.get("model_version", ""),
    }
    if projection.get("cluster_id") is not None:
        values[f["cluster_id"]] = projection.get("cluster_id")
    if include_workflow_defaults:
        values[f["processing_status"]] = projection.get("processing_status", "待审核")
        values[f["production_enabled"]] = False
        values[f["first_frame_requested"]] = False
        values[f["first_frame_status"]] = "用户未选择"
    return {key: value for key, value in values.items() if value not in (None, "")}


def export_ready_batch(
    *,
    batch: Any,
    items: Sequence[Any],
    target_client: FeishuBitableClient,
    product_images: Sequence[Dict[str, Any]] = (),
    store_id: str = "",
) -> Dict[str, int]:
    existing_records = target_client.list_records(page_size=100)
    existing_by_script_id = _fields_by_script_id(existing_records)
    existing_by_batch_item_id = _records_by_batch_item_id(existing_records)
    creates: List[Dict[str, Any]] = []
    updated = 0
    skipped = 0

    workflow_fields = {
        PRODUCTION_SCRIPT_FIELD_NAMES["processing_status"],
        PRODUCTION_SCRIPT_FIELD_NAMES["production_enabled"],
        PRODUCTION_SCRIPT_FIELD_NAMES["first_frame_requested"],
        PRODUCTION_SCRIPT_FIELD_NAMES["composite_first_frame"],
        PRODUCTION_SCRIPT_FIELD_NAMES["first_frame_status"],
        PRODUCTION_SCRIPT_FIELD_NAMES["review_note"],
        PRODUCTION_SCRIPT_FIELD_NAMES["sync_result"],
        PRODUCTION_SCRIPT_FIELD_NAMES["sync_time"],
        PRODUCTION_SCRIPT_FIELD_NAMES["run_task_id"],
        PRODUCTION_SCRIPT_FIELD_NAMES["longform_status"],
        PRODUCTION_SCRIPT_FIELD_NAMES["longform_video"],
        PRODUCTION_SCRIPT_FIELD_NAMES["longform_error"],
    }

    for item in items:
        if str(getattr(item, "status", "")) != "SCRIPT_READY":
            skipped += 1
            continue
        projection = build_production_projection(batch=batch, item=item)
        script_id = str(projection.get("script_id") or "").strip()
        if not script_id:
            skipped += 1
            continue
        batch_id = str(projection.get("batch_id") or "").strip()
        batch_item_id = str(projection.get("batch_item_id") or "").strip()
        existing_record = (
            existing_by_batch_item_id.get((batch_id, batch_item_id))
            if batch_id and batch_item_id
            else None
        ) or existing_by_script_id.get(script_id)
        fields = projection_to_feishu_fields(
            projection,
            product_images=product_images,
            store_id=store_id,
            include_workflow_defaults=existing_record is None,
        )
        if existing_record is not None:
            update_fields = {
                key: value for key, value in fields.items() if key not in workflow_fields
            }
            target_client.update_record_fields(existing_record.record_id, update_fields)
            updated += 1
        else:
            creates.append({"fields": fields})

    created_ids = target_client.batch_create_records(creates)
    return {"created": len(created_ids), "updated": updated, "skipped": skipped}


def operation_record_values(record: TaskRecord) -> Dict[str, Any]:
    f = OPERATION_TASK_FIELD_NAMES
    fields = record.fields
    top_category = normalize_operation_top_category(fields.get(f["top_category"]))
    video_spec, duration_seconds = normalize_operation_video_spec(
        fields.get(f["video_spec"]), fields.get(f["duration_seconds"]),
    )
    return {
        "task_id": str(fields.get(f["task_id"]) or record.record_id).strip(),
        "product_code": str(fields.get(f["product_code"]) or "").strip(),
        "product_images": extract_attachments(fields.get(f["product_images"])),
        "store_id": str(fields.get(f["store_id"]) or "").strip(),
        "top_category": top_category,
        "product_type": normalize_operation_product_type(
            fields.get(f["product_type"]), top_category
        ),
        "target_country": str(fields.get(f["target_country"]) or "").strip(),
        "target_language": str(fields.get(f["target_language"]) or "").strip(),
        "requested_count": int(float(fields.get(f["requested_count"]) or 6)),
        "test_phase": normalize_operation_test_phase(fields.get(f["test_phase"])),
        "video_spec": video_spec,
        "video_format": "SHORT_15S" if duration_seconds == 15 else "LONGFORM",
        "duration_seconds": float(duration_seconds),
        "longform_scene_mode": normalize_longform_scene_mode(
            fields.get(f["longform_scene_mode"])
        ),
        "random_seed": int(float(fields.get(f["random_seed"]) or 0)),
        "status": str(fields.get(f["status"]) or "").strip(),
        "batch_id": str(fields.get(f["batch_id"]) or "").strip(),
    }
