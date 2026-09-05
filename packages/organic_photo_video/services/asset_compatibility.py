"""Explicit compatibility constraints; never infer garment facts from a name."""
from services.styling_normalizer import normalize_product_id

SCENE_FAMILIES = {
    "ENV_AIRPORT_DEPART_001": {"airport_departure_travel", "travel_departure"},
    "SCENE_A_001": {"vanity_tryon", "bedroom", "indoor_tryon"},
    "ENV_CAFE_001": {"cafe_date", "cafe", "cafe_lifestyle"},
}

SCENE_ALIASES = {
    "cafe_dining": "cafe", "cafe_date": "cafe", "cafe_lifestyle": "cafe",
    "home_routine": "bedroom", "vanity_tryon": "bedroom", "indoor_tryon": "bedroom",
    "airport_departure_travel": "airport", "travel_departure": "airport",
}


def uses_pure_color(profile_or_plan):
    value = profile_or_plan or {}
    for key in ("presentation_profile", "visual_profile"):
        if isinstance(value.get(key), dict):
            value = value[key]
            break
    return str(value.get("background_mode") or "").lower() in {"solid_color", "pure_color"}


def scene_preference_mismatch(look, scene_ref="", scene=None):
    def canonical(items):
        return {SCENE_ALIASES.get(str(x).strip().lower(), str(x).strip().lower()) for x in items or []}
    families = canonical((look.get("compatibility") or {}).get("scene_families"))
    actual = canonical((scene or {}).get("families") or SCENE_FAMILIES.get(scene_ref, set()))
    return bool(scene_ref and families and not families.intersection(actual))


def compatibility_errors(look, product, scene_ref="", scene=None, *, pure_color=False):
    compatibility = look.get("compatibility") or {}
    errors = []
    def values(items):
        return {str(x).strip().lower() for x in items or [] if str(x).strip()}
    codes = {normalize_product_id(x).lower() for x in look.get("applicable_product_codes") or [] if normalize_product_id(x)}
    if codes and "*" not in codes and normalize_product_id(product.get("product_id")).lower() not in codes:
        errors.append("product_code")
    types = values(compatibility.get("product_types"))
    category = str(product.get("category") or "").lower()
    if category and types and category not in types:
        errors.append("product_type")
    for field, allowed_field in (("product_fit", "product_fits"), ("climate_profile", "climate_profiles")):
        actual = str(product.get(field) or "").strip().lower()
        allowed = values(compatibility.get(allowed_field))
        if field == "climate_profile" and compatibility.get(field):
            allowed.add(str(compatibility[field]).lower())
        if actual and allowed and actual not in allowed:
            errors.append(field)
    if not pure_color and scene_preference_mismatch(look, scene_ref, scene):
        errors.append("scene_family")
    return errors
