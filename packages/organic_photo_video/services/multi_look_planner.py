"""Frozen multi-look page selection; deterministic soft diversity, no QA calls."""
from collections import Counter
from copy import copy, deepcopy

from services.asset_compatibility import compatibility_errors, uses_pure_color
from services.look_selection import look_candidate_refs, prepare_look_for_policy, is_dress_recipe
from services.outfit_planner import freeze_multi_look_state
from services.styling_normalizer import outfit_fingerprint, outfit_visual_features

MULTI_LOOK_RECIPE_ID = "RECIPE_MULTI_LOOK_V1"


def _visible_axes(state):
    """Ranking-only features: do not change persisted outfit fingerprints."""
    features = outfit_visual_features(state)
    shoe = features["footwear"]
    footwear_family = next((name for name, words in (
        ("BOOTS", ("靴", "boots")), ("LOAFERS", ("乐福", "loafers")),
        ("MARY_JANE", ("玛丽珍", "maryjane")), ("HEELS", ("高跟", "heels")),
        ("SANDALS", ("凉鞋", "sandals")),
        ("SNEAKERS", ("运动鞋", "小白鞋", "老爹鞋", "sneakers")),
    ) if any(word in shoe for word in words)), shoe or "UNSPECIFIED")
    return {"shape": features["silhouette"].partition("_")[2] or features["silhouette"],
            "footwear_family": footwear_family, "inner": features["top_inner"] or features["onepiece"],
            "accessories": "|".join(features[k] for k in ("accessories", "bag", "socks"))}


def _visible_distance(left, right):
    return sum(weight for key, weight in (("shape", 4), ("footwear_family", 3),
                                          ("inner", 2), ("accessories", 1))
               if left[key] != right[key])


def compatible_multi_look_candidates(account, reader, product, scene_ref=""):
    effective_account = copy(account)
    effective_account.operating_rules_json = {**(account.operating_rules_json or {}),
        "presentation_profile": {"background_mode": "solid_color"}}
    candidates, rejected = [], {}
    for ref in look_candidate_refs(account, reader):
        try:
            snapshot = prepare_look_for_policy(effective_account, reader.get_look(ref))
        except (KeyError, ValueError, RuntimeError):
            rejected[ref] = ["unreadable"]
            continue
        errors = compatibility_errors(snapshot, product, scene_ref, pure_color=True)
        recipe = snapshot.get("recipe") or {}
        if account.status == "active" and snapshot.get("status") != "enabled":
            errors.append("not_production_enabled")
        if not (recipe.get("onepiece") or recipe.get("dress")):
            if not recipe.get("top_inner"):
                errors.append("missing_top_inner")
            if not recipe.get("bottom") and not is_dress_recipe(recipe):
                errors.append("missing_bottom")
        if errors:
            rejected[ref] = errors
            continue
        candidates.append(snapshot)
    return candidates, rejected


def history_look_usage(rows):
    usage = Counter()
    for row in rows or []:
        sequence = row.get("look_sequence") or []
        if sequence:
            items = sequence
        else:
            items = [row]
            if row.get("alternate_look_ref") and row.get("alternate_look_ref") != row.get("look_ref"):
                items.append({axis: row.get("alternate_" + axis) for axis in ("look_ref", "outfit_fingerprint", "silhouette_key")})
        for item in items:
            for axis in ("look_ref", "outfit_fingerprint", "silhouette_key"):
                value = item.get(axis) or (item.get("visible_silhouette") if axis == "silhouette_key" else "")
                if value:
                    usage[(axis, str(value))] += 1
    return usage


def select_multi_look_sequence(candidates, *, first_ref="", maximum=5, usage=None, history=None):
    """Distinct major outfits are data identity, not a new aesthetic threshold.

    Three silhouettes and non-adjacency are ranked preferences. If fewer than
    five executable distinct outfits exist, return fewer pages, never invent.
    """
    usage = usage or Counter()
    history = history or Counter()
    prepared = []
    for order, snapshot in enumerate(candidates):
        state = freeze_multi_look_state(snapshot)
        prepared.append({"look_ref": str(snapshot.get("ref_id") or snapshot.get("look_id") or ""),
                         "snapshot": deepcopy(snapshot), "fingerprint": state["outfit_fingerprint"],
                         "outfit_fingerprint": outfit_fingerprint(snapshot),
                         "silhouette_key": state["visible_silhouette"],
                         "visible_axes": _visible_axes(state),
                         "footwear_fallback": "footwear_neutral_fallback" in (snapshot.get("normalization_warnings") or []),
                         "order": order})
    if first_ref and not any(item["look_ref"] == first_ref for item in prepared):
        raise ValueError("显式首图穿搭不在可执行兼容池中")
    selected, fingerprints, silhouettes, shapes, shoes = [], set(), set(), set(), set()
    while len(selected) < min(max(int(maximum), 1), 5):
        available = [item for item in prepared if item["fingerprint"] not in fingerprints]
        if not available:
            break
        if first_ref and not selected:
            chosen = next(item for item in available if item["look_ref"] == first_ref)
        else:
            def score(item):
                silhouette = item["silhouette_key"]
                visible = item["visible_axes"]
                # Balance real outfit exposure across batches before visual
                # tie-breaks, otherwise the same rare skirt/boot wins forever.
                return (usage[("outfit_fingerprint", item["outfit_fingerprint"])]
                        + history[("outfit_fingerprint", item["outfit_fingerprint"])],
                        usage[("look_ref", item["look_ref"])] + history[("look_ref", item["look_ref"])],
                        int(bool(selected) and visible["shape"] in shapes),
                        -_visible_distance(visible, selected[-1]["visible_axes"]) if selected else 0,
                        int(bool(selected) and visible["footwear_family"] in shoes),
                        int(bool(selected) and item["footwear_fallback"]),
                        int(len(silhouettes) < 3 and silhouette in silhouettes),
                        int(bool(selected) and silhouette == selected[-1]["silhouette_key"]),
                        item["order"])
            chosen = min(available, key=score)
        chosen = {k: deepcopy(v) for k, v in chosen.items() if k != "order"}
        index = len(selected) + 1
        chosen.update(slot_index=index, state_id=f"LOOK_{index:02d}")
        selected.append(chosen)
        fingerprints.add(chosen["fingerprint"])
        silhouettes.add(chosen["silhouette_key"])
        shapes.add(chosen["visible_axes"]["shape"])
        shoes.add(chosen["visible_axes"]["footwear_family"])
    if not selected:
        raise ValueError("没有可执行的兼容穿搭模板，不能生成多穿图文")
    notes = []
    if len(selected) < maximum:
        notes.append("fewer_distinct_outfits_reduce_actual_pages")
    if len(silhouettes) < min(3, len(selected)):
        notes.append("silhouette_variety_below_soft_preference")
    if len(shoes) == 1 and len(selected) > 1:
        notes.append("single_footwear_family_in_selected_templates")
    return selected, {"requested_count": maximum, "actual_count": len(selected),
                      "selection_policy": "visible_difference_soft_v2",
                      "duration_ms": sum(multi_look_durations(len(selected))),
                      "available_distinct_count": len({item["fingerprint"] for item in prepared}),
                      "selected_shape_count": len(shapes), "selected_footwear_family_count": len(shoes),
                      "footwear_fallback_count": sum(item["footwear_fallback"] for item in selected),
                      "selected_silhouette_count": len(silhouettes), "soft_preferences": {"minimum_silhouettes": 3, "avoid_similar_adjacent": True},
                      "degradation_reasons": notes}


def multi_look_durations(count):
    if not 1 <= int(count) <= 5:
        raise ValueError("multi-look page count must be 1..5")
    if count == 1:
        return [6000]
    weights = [1000, 1000, 1000, 1400][:count - 1]
    remaining, result = 4400, [1600]
    for index, weight in enumerate(weights):
        duration = remaining - sum(result[1:]) if index == len(weights) - 1 else round(remaining * weight / sum(weights) / 100) * 100
        result.append(int(duration))
    return result


def sequence_axes(sequence):
    return [{key: item[key] for key in ("look_ref", "outfit_fingerprint", "fingerprint", "silhouette_key", "slot_index", "state_id")} for item in sequence]
