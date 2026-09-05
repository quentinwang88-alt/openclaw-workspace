"""Shared, deterministic library selection policy (legacy allowlist by default)."""
import copy

from services.asset_compatibility import compatibility_errors, uses_pure_color
from services.styling_normalizer import outfit_fingerprint, outfit_visual_features


def look_candidate_refs(account, asset_reader, *, explicit_ref=""):
    rules = account.operating_rules_json or {}
    mode = str(rules.get("look_selection_mode") or "allowlist")
    if mode not in {"allowlist", "auto_library"}:
        raise ValueError(f"unknown look_selection_mode: {mode}")
    excluded = {str(x) for x in rules.get("exclude_look_refs") or []}
    if explicit_ref:
        if str(explicit_ref) in excluded:
            raise ValueError("显式穿搭在账户排除列表中")
        refs = [str(explicit_ref)]
    elif mode == "auto_library":
        # No silent fallback to six old rows if library discovery is broken.
        refs = asset_reader.list_look_ids()
    else:
        refs = list(account.allowed_look_refs_json or [])
    return [str(x) for x in dict.fromkeys(refs) if str(x) not in excluded]


def supports_dynamic_items(profile_or_plan):
    value = profile_or_plan or {}
    if value.get("item_count_policy") == "dynamic_2_or_3":
        return True
    return any(supports_dynamic_items(value.get(key)) for key in
               ("presentation_profile", "board_layout", "layout", "decomposition", "recipe")
               if isinstance(value.get(key), dict))


def is_dress_recipe(recipe):
    if (recipe or {}).get("onepiece") or (recipe or {}).get("dress"):
        return True
    value = str((recipe or {}).get("bottom") or "").lower()
    return "无独立下装" in value or "no separate bottom" in value


def product_compatible(look, product):
    return not compatibility_errors(look, product)


def prepare_look_for_policy(account, snapshot):
    """Only new library/pure-colour plans may use a disclosed neutral shoe."""
    result = copy.deepcopy(snapshot)
    rules = account.operating_rules_json or {}
    recipe = result.get("recipe") or {}
    if (rules.get("look_selection_mode") == "auto_library" or uses_pure_color(rules)) and not (recipe.get("footwear") or recipe.get("shoes")):
        result.setdefault("recipe", {})["footwear"] = "中性纯色平底运动鞋（无品牌）"
        result.setdefault("recipe_field_sources", {})["footwear"] = {
            "source": "policy_neutral_fallback", "columns": [], "reason": "library_footwear_unspecified"}
        result.setdefault("normalization_warnings", []).append("footwear_neutral_fallback")
        result["outfit_fingerprint"] = outfit_fingerprint(result)
        result["visual_features"] = outfit_visual_features(result)
    return result
