"""Shared, branch-neutral product-image evidence snapshot helpers."""
from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List

from .contracts import stable_id


PRODUCT_VISUAL_EVIDENCE_SCHEMA = "product-visual-evidence-v1"


def _text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def build_product_visual_evidence_prompt(product_truth: Dict[str, Any]) -> str:
    compact = {
        "product_code": product_truth.get("product_code"),
        "top_category": product_truth.get("top_category"),
        "product_type": product_truth.get("product_type"),
    }
    return f"""你是商品参考图的可见证据提取器。只观察目标商品，不分析图片中的人物、穿搭、姿势、背景或场景。

硬边界：
- 只记录多张参考图中能够直接看见且相互一致的外观；不得推断材质成分、保暖效果、尺寸、品牌、用途、质量或购买价值。
- 内部描述全部使用简体中文。
- 最多输出6个锚点；不确定或被遮挡的内容不要输出。
- anchor_type 只能使用 COLOR / SILHOUETTE / COLLAR / CLOSURE / SEAM / EDGE / PATTERN / POCKET / SLEEVE / OTHER。
- visible_zones 只能使用 FRONT / BACK / SIDE / COLLAR / FRONT_CENTER / SLEEVE / HEM / FULL_BODY。

任务：
{json.dumps(compact, ensure_ascii=False, indent=2)}

只返回 JSON：
{{
  "schema_version": "{PRODUCT_VISUAL_EVIDENCE_SCHEMA}",
  "anchors": [
    {{"anchor_type": "COLOR", "description": "直接可见的简短中文描述", "visible_zones": ["FULL_BODY"], "confidence": "HIGH"}}
  ]
}}"""


def normalize_product_visual_evidence(payload: Any) -> Dict[str, Any]:
    raw = payload if isinstance(payload, dict) else {}
    anchors: List[Dict[str, Any]] = []
    allowed_types = {
        "COLOR", "SILHOUETTE", "COLLAR", "CLOSURE", "SEAM", "EDGE",
        "PATTERN", "POCKET", "SLEEVE", "OTHER",
    }
    allowed_zones = {
        "FRONT", "BACK", "SIDE", "COLLAR", "FRONT_CENTER", "SLEEVE", "HEM", "FULL_BODY",
    }
    for index, item in enumerate(raw.get("anchors") or [], 1):
        if not isinstance(item, dict):
            continue
        description = _text(item.get("description"))
        anchor_type = _text(item.get("anchor_type")).upper()
        if not description or anchor_type not in allowed_types:
            continue
        zones = tuple(
            zone for zone in (_text(value).upper() for value in item.get("visible_zones") or [])
            if zone in allowed_zones
        )
        anchors.append(
            {
                "anchor_id": stable_id(
                    "VISUAL_ANCHOR_",
                    {"index": index, "type": anchor_type, "description": description, "zones": zones},
                ),
                "anchor_type": anchor_type,
                "description": description,
                "visible_zones": list(zones),
                "confidence": "HIGH" if _text(item.get("confidence")).upper() == "HIGH" else "MEDIUM",
                "authority": "IMAGE_OBSERVED",
            }
        )
        if len(anchors) >= 6:
            break
    return {"schema_version": PRODUCT_VISUAL_EVIDENCE_SCHEMA, "anchors": anchors}


def merge_product_visual_evidence(
    product_truth: Dict[str, Any], evidence: Dict[str, Any]
) -> Dict[str, Any]:
    merged = dict(product_truth)
    anchors = list(evidence.get("anchors") or [])
    merged["visual_anchors"] = anchors
    identity = list(merged.get("identity_anchors") or [])
    generic = [
        value for value in identity
        if "唯一权威" not in _text(value) and "参考图" not in _text(value)
    ]
    for item in anchors:
        description = _text(item.get("description"))
        if description and description not in generic:
            generic.append(description)
    merged["identity_anchors"] = generic[:3] or identity
    return merged


def visual_anchor_ids(product_truth: Dict[str, Any]) -> tuple[str, ...]:
    return tuple(
        _text(item.get("anchor_id"))
        for item in product_truth.get("visual_anchors") or []
        if isinstance(item, dict) and _text(item.get("anchor_id"))
    )
