"""Resolve the operator's lightweight native-photo reference input."""
from __future__ import annotations

from typing import Any, Mapping, Sequence


REFERENCE_TYPE_OPTIONS = ("自动判断", "风格参考", "商品参考", "完整穿搭")
REFERENCE_MODE_STYLE = "STYLE"
REFERENCE_MODE_PRODUCT = "PRODUCT"
REFERENCE_MODE_COMPLETE_LOOK = "COMPLETE_LOOK"


def resolve_reference_mode(
    *, selected_type: Any, attachments: Sequence[Any], product_id: str,
    required_role_count: int, requested_count: int = 1,
) -> str:
    """Return one explicit frozen mode; AUTO uses only safe deterministic facts."""
    selected = str(selected_type or "自动判断").strip() or "自动判断"
    mapping = {
        "风格参考": REFERENCE_MODE_STYLE,
        "商品参考": REFERENCE_MODE_PRODUCT,
        "完整穿搭": REFERENCE_MODE_COMPLETE_LOOK,
    }
    if selected in mapping:
        mode = mapping[selected]
    elif selected != "自动判断":
        raise ValueError("未知参考图类型：" + selected)
    elif str(product_id or "").strip():
        mode = REFERENCE_MODE_PRODUCT
    elif required_role_count and len(attachments) == required_role_count * requested_count:
        mode = REFERENCE_MODE_COMPLETE_LOOK
    else:
        mode = REFERENCE_MODE_STYLE
    expected = required_role_count * requested_count
    if mode == REFERENCE_MODE_COMPLETE_LOOK and len(attachments) != expected:
        raise ValueError(
            f"完整穿搭模式生成 {requested_count} 篇需要按每篇 A/B/C/D 上传 {expected} 张图片"
        )
    if mode in {REFERENCE_MODE_STYLE, REFERENCE_MODE_PRODUCT} and not attachments and not product_id:
        raise ValueError("当前参考图模式至少需要上传一张图片或填写产品编码")
    return mode


def freeze_reference_brief(
    *, mode: str, theme: Mapping[str, Any], paths: Sequence[str],
) -> dict[str, Any]:
    return {
        "schema_version": "opv-photo-reference-brief-v1",
        "reference_mode": mode,
        "theme_key": str(theme.get("theme_key") or ""),
        "theme_label_zh": str(theme.get("label_zh") or ""),
        "visual_brief": str(theme.get("visual_brief") or ""),
        "reference_count": len(paths),
        "reference_paths": list(paths),
        "product_consistency": mode == REFERENCE_MODE_PRODUCT,
    }
