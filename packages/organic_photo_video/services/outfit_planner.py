"""Product Facts builder (layer 1) + structured Outfit Plan (layer 3).

Product Facts freeze the identity anchors every downstream layer must respect;
the Outfit Plan answers "why this styling" BEFORE any image is generated.
Both are rule-based V1 over structured inputs; LLM enrichment can later fill
the ``unknown`` holes without changing the contracts.
"""

from __future__ import annotations

import json
import re
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


def _look_fields(look: Dict[str, Any], base: Dict[str, Any]) -> Dict[str, Any]:
    """Only fill absent fields; explicit template values (including 'none') win."""
    recipe = deepcopy(look.get("recipe") or {})
    declared_sources = look.get("recipe_field_sources") or {}
    sources = {}

    def choose(name: str, *aliases: str, default: Any = "") -> Any:
        for key in (name, *aliases):
            if key in recipe and recipe[key] is not None and recipe[key] != "":
                sources[name] = declared_sources.get(key) or f"look.recipe.{key}"
                return deepcopy(recipe[key])
        sources[name] = f"rule.{name}"
        return deepcopy(base.get(name, default))

    bottom = choose("bottom", default={})
    if not isinstance(bottom, dict):
        bottom = {"type": str(bottom)}
    for field in ("type", "fit", "color", "material", "length"):
        key = f"bottom_{field}"
        if key in recipe and recipe[key] not in (None, ""):
            target_field = "kind" if field == "type" and bottom.get("type") else field
            bottom[target_field] = deepcopy(recipe[key])
            sources[f"bottom.{target_field}"] = declared_sources.get(key) or f"look.recipe.{key}"
    # Never import a rule's white colour into an explicitly supplied skirt/pants.
    bottom.setdefault("color", "按选中 Look 单品描述，不额外改色")
    result = {
        "top_inner": choose("top_inner"),
        "onepiece": choose("onepiece", "dress"),
        "bottom": bottom,
        "shoes": choose("shoes", "footwear"),
        "socks": choose("socks"),
        "bag": choose("bag"),
        "accessories": choose("accessories"),
        "color_palette": choose("color_palette", "palette", default=[]),
        "style_direction": choose("style_direction", "overall_style"),
        "silhouette": choose("silhouette", "bottom_fit"),
        "target_wear_mode": choose("target_wear_mode", "wear_mode", default="按模板最终穿法，完整展示目标商品"),
        "tucking": choose("tucking"),
        "item_reference_images": choose("item_reference_images", "item_refs", "reference_images", default={}),
        "field_sources": sources,
        "source_look_id": str(look.get("look_id") or look.get("ref_id") or look.get("id") or ""),
        "source_recipe": recipe,
    }
    palette = result["color_palette"]
    result["color_palette"] = [palette] if isinstance(palette, str) else list(palette or [])
    if not result["item_reference_images"]:
        for key in ("item_reference_images", "item_refs"):
            if look.get(key):
                result["item_reference_images"] = deepcopy(look[key])
                sources["item_reference_images"] = f"look.{key}"
                break
    # A single frozen item, not a fresh choice on every image page. Do not
    # split '/' or accessory lists: those can describe colours/layering.
    for field in ("top_inner", "shoes"):
        value = result[field]
        if not isinstance(value, str) or "或" not in value:
            continue
        candidates = [part.strip() for part in re.split(r"或者|或", value) if part.strip()]
        if len(candidates) > 1:
            result[field] = candidates[0]
            prior = sources[field]
            sources[field] = {
                "source": deepcopy(prior),
                "choice": {"policy": "first_explicit_alternative", "selected_index": 0,
                           "selected": candidates[0], "original": value},
            }
    return result


def build_outfit_plan(
    *,
    outfit_plan_id: str,
    theme_id: str,
    product_facts: Dict[str, Any],
    market_pack: Optional[Dict[str, Any]] = None,
    persona: Optional[Dict[str, Any]] = None,
    look: Optional[Dict[str, Any]] = None,
    content_goal: str = "",
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
    selected = _look_fields(look or {}, base)
    climate_zone = str((market_pack or {}).get("climate_zone") or "default")
    climate_note = (rules.get("climate_rules") or {}).get(
        climate_zone
    ) or (rules.get("climate_rules") or {}).get("default") or ""

    category = str(product_facts.get("category") or "unknown")
    color = str((product_facts.get("facts") or {}).get("color") or "").strip()
    explicit_color = color if color.lower() not in {"", "unknown", "product_color"} else ""
    palette = []
    for raw_value in selected.get("color_palette") or []:
        value = str(raw_value or "").strip()
        if not value or value.lower() == "unknown":
            continue
        if value.lower() == "product_color":
            if explicit_color and explicit_color not in palette:
                palette.append(explicit_color)
            continue
        if value not in palette:
            palette.append(value)
    if explicit_color and explicit_color not in palette:
        palette.insert(0, explicit_color)

    selected_bottom = selected["bottom"]
    selected_style = str(selected["style_direction"])
    color_logic = (
        f"产品显式主色={explicit_color}"
        if explicit_color else "产品主色只按商品参考图，不从模板猜测"
    )
    styling_logic = (
        f"产品是造型核心单品，{color_logic}；配套色：{('、'.join(palette))}；"
        f"选中 Look={str((look or {}).get('name') or '规则基线')}；"
        f"下装选择「{selected_bottom.get('type', '')}」形成上下比例；"
        f"场景={base.get('occasion', content_key)}，气候({climate_zone})约束：{climate_note}；"
        f"风格方向：{selected_style}"
    )

    visibility_rules = dict(rules.get("product_visibility_rules") or {})
    if content_goal in {"pain_point_solution", "visual_transform", "multi_look"}:
        visibility_rules["consistency"] = (
            "目标商品与人物始终一致；补充单品严格按 outfit_state 变化"
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
        "style_direction": selected_style,
        "color_palette": palette,
        "top_inner": selected["top_inner"],
        "onepiece": selected["onepiece"],
        "bottom": selected_bottom,
        "outerwear": "目标商品本身（外套类时不再叠加外层）",
        "shoes": selected["shoes"],
        "socks": selected["socks"],
        "bag": selected["bag"],
        "accessories": selected["accessories"],
        "silhouette": selected["silhouette"],
        "target_wear_mode": selected["target_wear_mode"],
        "tucking": selected["tucking"],
        "item_reference_images": selected["item_reference_images"],
        "item_refs": deepcopy(selected["item_reference_images"]),
        "field_sources": selected["field_sources"],
        "source_look_id": selected["source_look_id"],
        "source_recipe": selected["source_recipe"],
        "rule_defaults": deepcopy(base),
        "hair_style": str(
            (persona or {}).get("hair_hint")
            or (persona or {}).get("hair_style")
            or "按人物模板发型"
        ),
        "scene": content_key,
        "styling_logic": styling_logic,
        "product_is_core": True,
        "product_visibility_rules": visibility_rules,
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
    outfit_plan: Dict[str, Any], *, content_goal: str,
    alternate_look: Optional[Dict[str, Any]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Build the explicit outfit states a recipe is allowed to execute.

    Product/persona identity never changes.  Scene recipes keep one frozen
    state; transformation recipes get controlled complementary-item changes.
    Keeping states explicit prevents a narrative prompt from silently
    contradicting the global continuity lock.
    """

    from services.styling_normalizer import outfit_fingerprint

    final_state = {
        "state_purpose": "optimized_final_look",
        "top_inner": outfit_plan.get("top_inner") or "",
        "onepiece": outfit_plan.get("onepiece") or "",
        "bottom": deepcopy(outfit_plan.get("bottom") or {}),
        "shoes": outfit_plan.get("shoes") or "",
        "socks": outfit_plan.get("socks") or "",
        "bag": outfit_plan.get("bag") or "",
        "accessories": outfit_plan.get("accessories") or "",
        "color_palette": list(outfit_plan.get("color_palette") or []),
        "style_direction": outfit_plan.get("style_direction") or "",
        "silhouette": outfit_plan.get("silhouette") or "",
        "tucking": outfit_plan.get("tucking") or "",
        "item_reference_images": deepcopy(outfit_plan.get("item_reference_images") or {}),
        "item_refs": deepcopy(outfit_plan.get("item_reference_images") or outfit_plan.get("item_refs") or {}),
        "source_look_id": outfit_plan.get("source_look_id") or "",
        "field_sources": deepcopy(outfit_plan.get("field_sources") or {}),
        "target_wear_mode": outfit_plan.get("target_wear_mode") or "按模板最终穿法，完整展示目标商品",
        "change_permissions": [],
    }
    final_state["outfit_fingerprint"] = outfit_fingerprint(final_state, include_accessories=False)
    if content_goal in {"scene_solution", "outfit_breakdown"}:
        return {"FINAL": final_state}

    base_state = deepcopy(final_state)
    base_state.update({
        "state_purpose": "same_outfit_basic_wearing",
        "target_wear_mode": "同一套单品的自然基础穿法；保持人物真实身材，不通过压缩身体制造前后差异",
        "tucking": "自然垂放；不新增单品，不改变服装版型",
        "change_permissions": ["target_wear_mode", "tucking"],
        "same_look": True,
    })
    states = {"BASE": base_state, "FINAL": final_state}
    if content_goal == "visual_transform":
        # Exactly two library outfits, never a third rule-invented white look.
        base_state = deepcopy(final_state)
        if alternate_look:
            alternate = _look_fields(alternate_look, outfit_plan.get("rule_defaults") or {})
            alternate.pop("source_recipe", None)
            candidate = {**deepcopy(final_state), **alternate}
            candidate["item_refs"] = deepcopy(candidate["item_reference_images"])
            candidate["outfit_fingerprint"] = outfit_fingerprint(candidate, include_accessories=False)
            if candidate["outfit_fingerprint"] != final_state["outfit_fingerprint"]:
                base_state = candidate
        same_look = base_state["outfit_fingerprint"] == final_state["outfit_fingerprint"]
        base_state.update(state_purpose="library_look_a" if not same_look else "same_look_detail", same_look=same_look)
        final_state.update(state_purpose="library_look_b" if not same_look else "same_look_detail", same_look=same_look)
        states["BASE"] = base_state
        states["ALT_1"] = deepcopy(final_state)
    return states


def freeze_multi_look_state(look: Dict[str, Any], *, index: int = 1, base: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Freeze one real template as one page, without before/after inventions."""
    from services.styling_normalizer import outfit_fingerprint, outfit_visual_features

    state = _look_fields(look, base or {})
    state.pop("source_recipe", None)
    state.update({
        "state_id": f"LOOK_{index:02d}", "look_index": index,
        "state_purpose": "independent_library_full_look",
        "item_refs": deepcopy(state.get("item_reference_images") or {}),
        "label_i18n": deepcopy(look.get("label_i18n") or {}),
        "change_permissions": ["top_inner", "onepiece", "bottom", "shoes", "socks", "bag", "accessories"],
        "identity_constraints": ["same_persona", "same_target_product", "same_product_color", "same_background"],
    })
    state["outfit_fingerprint"] = outfit_fingerprint(state, include_accessories=False)
    state["full_outfit_fingerprint"] = outfit_fingerprint(state)
    state["visible_silhouette"] = outfit_visual_features(state)["silhouette"]
    return state


def build_multi_look_states(outfit_plan: Dict[str, Any], look_snapshots) -> Dict[str, Dict[str, Any]]:
    return {f"LOOK_{index:02d}": freeze_multi_look_state(
        snapshot, index=index, base=outfit_plan.get("rule_defaults") or {}
    ) for index, snapshot in enumerate(look_snapshots, start=1)}
