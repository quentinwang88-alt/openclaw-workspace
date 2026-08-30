"""Independent Feishu schemas and projections for SEEDING_ORGANIC."""
from __future__ import annotations

from typing import Any, Dict, Sequence, Tuple

from core.production_script_feishu import FieldSpec, PRODUCT_TYPE_OPTIONS


OBJECTIVES: Tuple[str, ...] = (
    "STYLE_MEMORY", "SCENE_ASSOCIATION", "CHOICE_EDUCATION",
    "DETAIL_APPRECIATION", "PERSONAL_POSITION", "DISCUSSION",
)
OBJECTIVE_OPTIONS: Tuple[str, ...] = (
    "风格记忆", "场景联想", "选择参考", "细节欣赏", "个人立场", "互动讨论",
)
OBJECTIVE_TO_CODE = dict(zip(OBJECTIVE_OPTIONS, OBJECTIVES))
OBJECTIVE_TO_LABEL = {value: key for key, value in OBJECTIVE_TO_CODE.items()}
PRODUCT_ROLE_TO_CODE = {"主角": "HERO", "配角": "SUPPORTING", "点缀": "INCIDENTAL"}
PRODUCT_ROLE_TO_LABEL = {value: key for key, value in PRODUCT_ROLE_TO_CODE.items()}
PROMINENCE_TO_CODE = {"前段": "EARLY", "中段": "MID", "后段": "LATE"}
PROMINENCE_TO_LABEL = {value: key for key, value in PROMINENCE_TO_CODE.items()}
EXPERIENCE_TO_CODE = {
    "无历史体验": "NONE",
    "仅当前观察": "CURRENT_OBSERVATION",
    "已确认经历": "OPERATOR_CONFIRMED_HISTORY",
}
EXPERIENCE_TO_LABEL = {value: key for key, value in EXPERIENCE_TO_CODE.items()}
TASK_STATUSES: Tuple[str, ...] = ("待执行", "执行中", "已完成", "部分完成", "失败")


def _single(name: str, options: Sequence[str]) -> FieldSpec:
    return FieldSpec(
        name=name, field_type=3, ui_type="SingleSelect",
        property={"options": [{"name": option} for option in options]},
    )


SEED_OPERATION_FIELDS: Tuple[FieldSpec, ...] = (
    FieldSpec("产品编码"),
    FieldSpec("产品图片", 17, "Attachment"),
    FieldSpec("店铺ID"),
    _single("一级类目", ("女装", "配饰")),
    _single("产品类型", PRODUCT_TYPE_OPTIONS),
    FieldSpec("目标国家"),
    FieldSpec("目标语言"),
    FieldSpec("生成数", 2, "Number"),
    _single("种草目标", OBJECTIVE_OPTIONS),
    FieldSpec("内容要求"),
    _single("体验权限", tuple(EXPERIENCE_TO_CODE)),
    FieldSpec("体验事实"),
    FieldSpec("产品事实"),
    FieldSpec("禁止表达"),
    _single("任务状态", TASK_STATUSES),
    FieldSpec("结果摘要"),
    FieldSpec("错误信息"),
)


SEED_SCRIPT_FIELDS: Tuple[FieldSpec, ...] = (
    FieldSpec("产品编码"),
    FieldSpec("产品图片", 17, "Attachment"),
    FieldSpec("店铺ID"),
    FieldSpec("脚本标题"),
    FieldSpec("时长", 2, "Number"),
    FieldSpec("产品类型"),
    FieldSpec("目标语言"),
    _single("种草目标", OBJECTIVE_OPTIONS),
    FieldSpec("口播"),
    FieldSpec("中文口播"),
    FieldSpec("完整脚本"),
    FieldSpec("视频提示词"),
    FieldSpec("首帧图", 17, "Attachment"),
    FieldSpec("质检结果"),
    _single("发布策略", ("种草不挂车",)),
    _single("处理状态", ("待审核", "已选用", "不采用", "生成失败", "已送生产", "同步失败")),
    FieldSpec("进入生产", 7, "Checkbox"),
    FieldSpec("审核意见"),
    FieldSpec("同步结果"),
    FieldSpec("运行任务ID"),
)


SEED_OPERATION_RENAMES = {"内容主题": "内容要求"}
SEED_SCRIPT_RENAMES = {"视频生成提示词": "视频提示词"}
SEED_OPERATION_PRUNE_FIELDS: Tuple[str, ...] = (
    "时长", "生活场景", "商品角色", "露出时机", "批次ID", "执行时间",
)
SEED_SCRIPT_PRUNE_FIELDS: Tuple[str, ...] = (
    "批次ID", "序号", "目标国家", "观看收益", "商品角色", "露出时机", "体验权限",
    "内容价值分", "广告感分", "体验检查", "脚本类型", "发布用途", "是否挂车",
    "内容分支", "不挂车保护", "同步时间",
)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _lines(value: Any) -> list[str]:
    return [line.strip() for line in _text(value).replace("\r", "\n").split("\n") if line.strip()]


def operation_record_values(record: Any) -> Dict[str, Any]:
    fields = record.fields or {}
    return {
        "task_id": _text(fields.get("任务ID")) or record.record_id,
        "product_code": _text(fields.get("产品编码")),
        "product_images": fields.get("产品图片") or [],
        "store_id": _text(fields.get("店铺ID")),
        "top_category": _text(fields.get("一级类目")),
        "product_type": _text(fields.get("产品类型")),
        "target_country": _text(fields.get("目标国家")),
        "target_language": _text(fields.get("目标语言")),
        "count": int(float(fields.get("生成数") or 1)),
        "duration_seconds": 15.0,
        "status": _text(fields.get("任务状态")),
        "theme": {
            "objective": OBJECTIVE_TO_CODE.get(
                _text(fields.get("种草目标")), _text(fields.get("种草目标"))
            ),
            "lived_context": _text(fields.get("内容要求")),
            "experience_authority": EXPERIENCE_TO_CODE.get(
                _text(fields.get("体验权限")), _text(fields.get("体验权限"))
            ) or "NONE",
            "confirmed_experience_facts": _lines(fields.get("体验事实")),
            "forbidden_expressions": _lines(fields.get("禁止表达")),
            "interaction_ending_allowed": True,
        },
        "facts": _lines(fields.get("产品事实")),
        "identity_anchors": [],
        "negative_constraints": _lines(fields.get("禁止表达")),
    }


def result_to_feishu_fields(
    *, result: Dict[str, Any], run: Dict[str, Any], task: Dict[str, Any], product_images: Sequence[Dict[str, Any]]
) -> Dict[str, Any]:
    theme = result["seed_theme"]
    voice = result["voiceover"]
    quality = result["quality"]
    visual = result["visual_blueprint"]
    dimensions = quality.get("quality_dimensions") or {}
    machine = "安全通过" if quality.get("passed", True) else "安全阻断"
    fact = "事实✓" if dimensions.get("fact_integrity", "PASS") == "PASS" else "事实阻断"
    commerce = "不挂车✓" if dimensions.get("commerce_safety", "PASS") == "PASS" else "商业阻断"
    execution = dimensions.get("execution_grade") or "待评"
    distinctness = dimensions.get("distinctness_grade") or "待评"
    creative = dimensions.get("creative_quality") or "PENDING_HUMAN_REVIEW"
    creative_label = f"创意{creative}" if creative in {"A", "B", "C"} else "创意待审"
    opening = dimensions.get("opening_grade") or ""
    opening_label = f"｜前3秒{opening}" if opening in {"A", "B", "C"} else ""
    language_state = dimensions.get("language_review", "MACHINE_SCREENED_NATIVE_REVIEW_PENDING")
    language = (
        f"{task.get('target_language') or '目标语言'}待母语审"
        if language_state != "BLOCKED" else "语言阻断"
    )
    return {
        "种草脚本ID": result["script_id"],
        "产品编码": task["product_code"],
        "产品图片": list(product_images),
        "店铺ID": task["store_id"],
        "脚本标题": _text(visual.get("script_title")) or theme["memory_residue"],
        "时长": task["duration_seconds"],
        "产品类型": task["product_type"],
        "目标语言": task["target_language"],
        "种草目标": OBJECTIVE_TO_LABEL.get(theme["objective"], theme["objective"]),
        "口播": voice["target_text"],
        "中文口播": voice["chinese_translation"],
        "完整脚本": result["complete_script"],
        "视频提示词": result["video_generation_prompt"],
        "质检结果": (
            f"{machine}｜{fact}｜{commerce}｜执行{execution}｜"
            f"{creative_label}{opening_label}｜批次{distinctness}｜{language}"
        ),
        "发布策略": "种草不挂车",
        "处理状态": "待审核",
        "进入生产": False,
    }


SEED_OPERATION_SELECT_OPTIONS = {
    spec.name: tuple(option["name"] for option in (spec.property or {}).get("options", []))
    for spec in SEED_OPERATION_FIELDS
    if spec.field_type == 3
}
SEED_SCRIPT_SELECT_OPTIONS = {
    spec.name: tuple(option["name"] for option in (spec.property or {}).get("options", []))
    for spec in SEED_SCRIPT_FIELDS
    if spec.field_type == 3
}
