"""Product Facts builder (layer 1) + structured Outfit Plan (layer 3).

Product Facts freeze the identity anchors every downstream layer must respect;
the Outfit Plan answers "why this styling" BEFORE any image is generated.
Both are rule-based V1 over structured inputs; LLM enrichment can later fill
the ``unknown`` holes without changing the contracts.
"""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Optional

from domain import contracts

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTFIT_RULES_PATH = (
    PACKAGE_ROOT / "config" / "outfit" / "OUTFIT_RULES_TH_V1.json"
)

DEFAULT_PRODUCT_FACTS_VERSION = "1"


def build_product_facts(
    product: Dict[str, Any],
    *,
    snapshot_version: Optional[str] = None,
) -> Dict[str, Any]:
    """Extract opv-product-facts-v1 from a product snapshot dict.

    Structured attributes that Product Truth has not filled yet stay
    ``"unknown"`` explicitly instead of being invented.
    """
    facts_keys = ("color", "material", "fit", "length", "collar", "sleeve", "season")
    facts = {
        key: str(product.get(key) or "unknown") for key in facts_keys
    }
    details = product.get("key_details") or product.get("design_details") or []
    if isinstance(details, str):
        details = [details]
    payload = {
        "schema_version": contracts.PRODUCT_FACTS_SCHEMA_VERSION,
        "product_id": str(product.get("product_id") or ""),
        "product_name": str(product.get("product_name") or ""),
        "category": str(product.get("category") or "unknown"),
        "facts": facts,
        "key_details": [str(d) for d in details],
        "reference_images": [
            str(p) for p in product.get("reference_images", []) if p
        ],
        "locked_features": list(contracts.PRODUCT_LOCKED_FEATURES),
        "snapshot_version": snapshot_version or DEFAULT_PRODUCT_FACTS_VERSION,
    }
    contracts.ensure_valid(
        contracts.validate_product_facts(payload), "product facts"
    )
    return payload


def load_outfit_rules(path: Optional[Path] = None) -> Dict[str, Any]:
    rules_path = Path(path) if path else DEFAULT_OUTFIT_RULES_PATH
    with rules_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _content_key_from_theme(theme_id: str) -> str:
    import re

    from services.copy_writer import THEME_CONTENT_KEY

    match = re.search(r"THEME_[A-Z]{2}_([A-Z_]+?)(?:_V\d+)?$", str(theme_id or ""))
    if not match:
        return "default"
    return THEME_CONTENT_KEY.get(match.group(1).rstrip("_"), "default")


def build_outfit_plan(
    *,
    outfit_plan_id: str,
    theme_id: str,
    product_facts: Dict[str, Any],
    market_pack: Optional[Dict[str, Any]] = None,
    persona: Optional[Dict[str, Any]] = None,
    rules: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Compose the structured outfit plan (opv-outfit-plan-v1).

    Rule-based V1: base styling comes from the content-key rule table, then
    styling_logic answers why: product role, proportion, palette, occasion.
    """
    rules = rules or load_outfit_rules()
    content_key = _content_key_from_theme(theme_id)
    by_key = rules.get("by_content_key") or {}
    base = by_key.get(content_key) or by_key.get("default") or {}
    climate_zone = str((market_pack or {}).get("climate_zone") or "default")
    climate_note = (rules.get("climate_rules") or {}).get(
        climate_zone
    ) or (rules.get("climate_rules") or {}).get("default") or ""

    category = str(product_facts.get("category") or "unknown")
    color = (product_facts.get("facts") or {}).get("color") or "unknown"
    palette = list(base.get("color_palette") or [])
    if color not in ("unknown", "") and color not in palette:
        palette.insert(0, color)

    styling_logic = (
        f"产品是造型核心单品，配色围绕产品主色({color})展开：{('、'.join(palette))}；"
        f"下装选择「{base.get('bottom', {}).get('type', '')}」形成上下比例；"
        f"场景={base.get('occasion', content_key)}，气候({climate_zone})约束：{climate_note}；"
        f"风格方向：{base.get('style_direction', '')}"
    )

    payload = {
        "schema_version": contracts.OUTFIT_PLAN_SCHEMA_VERSION,
        "outfit_plan_id": outfit_plan_id,
        "anchor_product": str(product_facts.get("product_name") or ""),
        "theme": content_key,
        "target_persona": str(
            (persona or {}).get("persona_name")
            or (persona or {}).get("name")
            or (persona or {}).get("persona_id")
            or "default"
        ),
        "body_profile": str(
            (persona or {}).get("body_hint")
            or (persona or {}).get("body_profile")
            or "natural"
        ),
        "occasion": str(base.get("occasion") or content_key),
        "climate": climate_zone,
        "style_direction": str(base.get("style_direction") or ""),
        "color_palette": palette,
        "bottom": base.get("bottom") or {},
        "outerwear": "目标商品本身（外套类时不再叠加外层）",
        "shoes": str(base.get("shoes") or ""),
        "bag": str(base.get("bag") or ""),
        "accessories": str(base.get("accessories") or ""),
        "hair_style": str(
            (persona or {}).get("hair_hint")
            or (persona or {}).get("hair_style")
            or "按人物模板发型"
        ),
        "scene": content_key,
        "styling_logic": styling_logic,
        "product_is_core": True,
        "product_visibility_rules": dict(
            rules.get("product_visibility_rules") or {}
        ),
        "product_role_note": (
            f"产品类目={category}，作为 TARGET_GARMENT 全程穿着；"
            "补充单品是否跨图变化由内容配方的 outfit_state 决定"
        ),
    }
    contracts.ensure_valid(
        contracts.validate_outfit_plan_payload(payload),
        f"outfit plan {outfit_plan_id}",
    )
    return payload


def build_outfit_states(
    outfit_plan: Dict[str, Any], *, content_goal: str
) -> Dict[str, Dict[str, Any]]:
    """Build the explicit outfit states a recipe is allowed to execute.

    Product/persona identity never changes.  Scene recipes keep one frozen
    state; transformation recipes get controlled complementary-item changes.
    Keeping states explicit prevents a narrative prompt from silently
    contradicting the global continuity lock.
    """

    final_state = {
        "state_purpose": "optimized_final_look",
        "bottom": deepcopy(outfit_plan.get("bottom") or {}),
        "shoes": outfit_plan.get("shoes") or "",
        "bag": outfit_plan.get("bag") or "",
        "accessories": outfit_plan.get("accessories") or "",
        "color_palette": list(outfit_plan.get("color_palette") or []),
        "style_direction": outfit_plan.get("style_direction") or "",
        "change_permissions": [],
    }
    if content_goal == "scene_solution":
        return {"FINAL": final_state}

    palette = list(outfit_plan.get("color_palette") or [])
    neutral = palette[-1] if palette else "neutral"
    base_state = {
        "state_purpose": "basic_before_optimization",
        "bottom": {"type": "基础中性直筒下装", "color": neutral},
        "shoes": "基础平底鞋",
        "bag": "无",
        "accessories": "无",
        "color_palette": palette,
        "style_direction": "基础日常穿法",
        "change_permissions": ["bottom", "shoes", "bag", "accessories"],
    }
    states = {"BASE": base_state, "FINAL": final_state}
    if content_goal == "visual_transform":
        alternate = deepcopy(final_state)
        alternate.update(
            {
                "state_purpose": "controlled_alternate_look",
                "bag": "与最终造型同色系的替代小包",
                "accessories": "替换一件极简配饰",
                "style_direction": "同一目标商品的轻量风格切换",
                "change_permissions": ["bag", "accessories"],
            }
        )
        states["ALT_1"] = alternate
    return states
