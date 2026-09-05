from __future__ import annotations

from typing import Any, Dict, Mapping

from core.product_selling_argument_adapter import load_verified_selling_point_catalog
from core.simplified_complete_script import build_product_identity_lock, _outfit_prompt_projection

from .contracts import recommended_argument_range
from .visual_guidance import segment_visibility


def _dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _unwrap(value: Mapping[str, Any]) -> Dict[str, Any]:
    current = dict(value)
    for key in ("script", "complete_script", "formal_script", "result"):
        nested = current.get(key)
        if isinstance(nested, Mapping) and (
            nested.get("video_generation_brief") or nested.get("production_design")
        ):
            return dict(nested)
    return current


def _compact_persona(value: Mapping[str, Any]) -> Dict[str, Any]:
    persona = _dict(value)
    return {
        "persona_id": _text(persona.get("persona_id")),
        "persona_name": _text(persona.get("persona_name")),
        "identity_lock": _dict(persona.get("identity_lock")),
        "script_projection": _dict(persona.get("script_projection")),
        "prompt_negative": _text(persona.get("prompt_negative")),
        "reference_strategy": _text(persona.get("reference_strategy")),
        "structured_snapshot_hash": _text(persona.get("structured_snapshot_hash")),
        # Asset ids/URLs are consumed by first-frame generation, not by the
        # long-form text model. Keeping them out lowers exposure and tokens.
        "reference_assets_available": bool(
            persona.get("reference_asset_ids") or persona.get("reference_images")
        ),
    }


def _compact_outfit(value: Mapping[str, Any]) -> Dict[str, Any]:
    # Keep the selected contract and its version/hash for replay and audit.
    # Only _outfit_prompt_projection is consumed by the video renderer.
    return _dict(value)


def _argument_role(item: Mapping[str, Any]) -> str:
    claim_type = _text(item.get("claim_type")).lower()
    theme = _text(item.get("claim_theme")).lower()
    material = " ".join((
        _text(item.get("primary_selling_point")),
        _text(item.get("operator_expression")), theme,
    )).lower()
    if claim_type == "visual_result":
        return "STYLE_PAYOFF"
    if claim_type == "scenario" or any(token in material for token in ("旅行", "通勤", "办公室", "场景")):
        return "USAGE_VALUE"
    if any(token in material for token in ("搭配", "拍照", "风格", "颜色")):
        return "STYLE_PAYOFF"
    return "PRODUCT_REASON"


def _build_argument_bundle(
    product_code: str,
    product_type: str,
    duration_seconds: int,
    semantic: Mapping[str, Any],
    fallback_argument: Mapping[str, Any],
    verified_facts: list[Any],
) -> Dict[str, Any]:
    """Read the governed catalog once and project only an authoring-sized bundle."""

    catalog_snapshot = load_verified_selling_point_catalog(
        product_code, product_type=product_type,
    )
    catalog = [dict(item) for item in catalog_snapshot.get("catalog") or [] if isinstance(item, Mapping)]
    source_contract = _dict(semantic.get("source_argument"))
    primary_source_id = _text(
        source_contract.get("source_argument_id")
        or fallback_argument.get("source_argument_id")
        or fallback_argument.get("argument_id")
    )
    primary_claim_ids = set(source_contract.get("source_claim_ids") or [])
    primary = next((
        item for item in catalog
        if _text(item.get("source_argument_id")) == primary_source_id
        and (not primary_claim_ids or primary_claim_ids.intersection(item.get("source_claim_ids") or []))
    ), {})
    if not primary:
        primary = next((item for item in catalog if _text(item.get("source_argument_id")) == primary_source_id), {})
    if not primary:
        primary = dict(fallback_argument)
    primary = {
        **primary,
        "argument_id": _text(primary.get("value_id") or primary.get("argument_id") or primary_source_id or "PRIMARY"),
        "text": _text(primary.get("primary_selling_point") or primary.get("text") or primary.get("operator_expression")),
        "argument_role": "PRIMARY_OUTCOME",
    }

    _, maximum = recommended_argument_range(duration_seconds)
    ordered_source_ids = list(
        _dict(semantic.get("product_market_context")).get("source_argument_ids") or []
    )
    rank = {value: index for index, value in enumerate(ordered_source_ids)}
    candidates = [item for item in catalog if _text(item.get("value_id")) != primary["argument_id"]]
    candidates.sort(key=lambda item: (
        0 if _text(item.get("operator_priority")).lower() == "core" else 1,
        rank.get(_text(item.get("source_argument_id")), 999),
    ))
    supporting = []
    seen_sources = {primary_source_id}
    seen_text = {_text(primary.get("text"))}
    for item in candidates:
        source_id = _text(item.get("source_argument_id"))
        value_text = _text(item.get("primary_selling_point") or item.get("operator_expression"))
        if not value_text or value_text in seen_text or source_id in seen_sources:
            continue
        supporting.append({
            **item,
            "argument_id": _text(item.get("value_id") or source_id),
            "text": value_text,
            "argument_role": _argument_role(item),
            "authority": _text(item.get("authority")) or "CENTRAL_OPERATOR_CATALOG",
        })
        seen_sources.add(source_id)
        seen_text.add(value_text)
        if len(supporting) >= maximum - 1:
            break
    minimum, maximum = recommended_argument_range(duration_seconds)
    return {
        "schema_version": "longform-argument-bundle-v1",
        "primary_argument": primary,
        "supporting_arguments": supporting,
        "visual_facts": [dict(item) if isinstance(item, Mapping) else item for item in verified_facts],
        "content_capacity": {
            "recommended_effective_argument_range": [minimum, maximum],
            "available_catalog_argument_count": len(catalog),
            "selected_effective_argument_count": (1 if primary.get("text") else 0) + len(supporting),
            "catalog_status": catalog_snapshot.get("status"),
            "catalog_snapshot_hash": catalog_snapshot.get("snapshot_hash"),
            "shortfall_is_blocking": False,
        },
    }


def source_from_complete_script(raw: Mapping[str, Any], *, duration_seconds: int = 27,
                                product_code: str = "") -> Dict[str, Any]:
    """Project frozen 15s production authority into a new isolated longform request.

    This is read-only reuse: it does not modify or call the stable 15-second
    path, and the longform model may only add capture units.
    """

    script = _unwrap(raw)
    brief = _dict(script.get("video_generation_brief"))
    production = _dict(brief.get("production_design")) or _dict(script.get("production_design"))
    allocated = _dict(script.get("allocated_direction"))
    bundle = _dict(allocated.get("content_bundle_brief")) or _dict(script.get("content_bundle_brief"))
    semantic = (
        _dict(script.get("semantic_spine_contract"))
        or _dict(bundle.get("semantic_spine_contract"))
        or _dict(brief.get("semantic_spine_contract"))
    )
    semantic_context = _dict(script.get("semantic_context")) or _dict(
        brief.get("semantic_context")
    )
    thesis = _dict(semantic.get("script_thesis"))
    argument = _dict(bundle.get("selling_argument")) or _dict(script.get("selling_argument"))
    voiceover = _dict(script.get("voiceover")) or _dict(brief.get("voiceover"))
    if not argument and _text(voiceover.get("selected_selling_argument_id")):
        argument = {
            "argument_id": _text(voiceover.get("selected_selling_argument_id")),
            "text": _text(semantic_context.get("core_buying_reason")),
            "source": "FROZEN_COMPLETE_SCRIPT",
        }
    world = {
        "character": _dict(production.get("character")) or _dict(production.get("character_setting")),
        "persona_contract": _compact_persona(_dict(brief.get("persona_selection_contract"))),
        "outfit": _text(_dict(production.get("outfit")).get("base_outfit") or _dict(production.get("outfit_setting")).get("styling")),
        "outfit_contract": _compact_outfit(_dict(brief.get("outfit_selection_contract"))),
        "outfit_prompt_projection": _dict(brief.get("outfit_prompt_projection")),
        "scene": _text(_dict(production.get("scene")).get("location") or _dict(production.get("scene_setting")).get("location")),
        "scene_contract": _dict(production.get("scene")) or _dict(production.get("scene_setting")),
        "lighting": _text(_dict(production.get("scene")).get("lighting") or _dict(production.get("scene_setting")).get("lighting")),
        "person_state": _text(_dict(production.get("character" )).get("identity")) or "同一冻结人物",
        "product_wear_state": "沿用完整脚本已经冻结的商品穿戴/展示状态",
        "camera": "普通手机竖屏拍摄，沿用完整脚本的原生记录关系",
        "carrier_mode": _text(brief.get("content_carrier") or production.get("presentation_mode")),
        "presentation_mode": _text(production.get("presentation_mode")),
        "capture_mode": _text(brief.get("capture_mode") or production.get("capture_mode")),
        "visual_saliency": _dict(brief.get("visual_execution_contract")).get("visual_saliency") or {},
        "opening_scene_projection": _dict(brief.get("visual_execution_contract")).get("opening_scene_projection") or {},
    }
    verified = list(brief.get("verified_facts") or script.get("verified_facts") or [])
    if not verified:
        verified = [
            {"claim_key": _text(item.get("claim_key")), "fact_text": _text(item.get("fact_text"))}
            for item in script.get("selling_points_used") or [] if isinstance(item, Mapping)
        ]
    product_code_value = _text(product_code or script.get("product_code") or raw.get("product_code"))
    product_type = _text(
        _dict(semantic.get("product_market_context")).get("product_type")
        or _dict(brief.get("product_truth")).get("canonical_product_type")
    )
    argument_bundle = _build_argument_bundle(
        product_code_value, product_type, int(duration_seconds), semantic, argument, verified,
    )
    return {
        "product_code": product_code_value,
        "target_country": _text(script.get("target_country") or brief.get("target_country") or "泰国"),
        "target_language": _text(script.get("target_language") or brief.get("target_language") or "泰语"),
        "target_duration_seconds": int(duration_seconds),
        "product_identity_lock": _dict(brief.get("product_identity_lock")) or _dict(script.get("product_identity_lock")),
        "product_truth": _dict(brief.get("product_truth")) or _dict(script.get("product_truth")),
        "production_world": world,
        "semantic_spine": {
            "hook_id": _text(allocated.get("requested_hook_id") or script.get("hook_id")),
            "primary_narrative_context": _text(
                semantic_context.get("primary_narrative_context")
                or thesis.get("primary_narrative_context") or thesis.get("audience_situation")
            ),
            "core_buying_reason": _text(
                semantic_context.get("core_buying_reason")
                or thesis.get("core_buying_reason")
                or argument.get("creative_core_value") or argument.get("text")
            ),
            "selling_argument": argument,
            "source_semantic_spine_contract": semantic,
        },
        "verified_facts": verified,
        "approved_supporting_arguments": argument_bundle["supporting_arguments"],
        "longform_argument_bundle": argument_bundle,
        "scene_mode": "single",
        "requested_hook_id": _text(allocated.get("requested_hook_id") or script.get("hook_id")),
        "relationship_language": _dict(script.get("relationship_language")),
        "approved_style_references": list(script.get("approved_style_references") or []),
        "native_rhetoric_contract": _dict(script.get("native_rhetoric_contract")),
        "source_lineage": {
            "source_script_id": _text(
                script.get("complete_script_id") or script.get("script_id")
            ),
            "source_mode": "READ_ONLY_15S_AUTHORITY_PROJECTION",
        },
    }


def source_from_product_plan(
    product_context: Mapping[str, Any],
    frozen_package: Mapping[str, Any],
    *,
    duration_seconds: int,
    product_code: str = "",
    source_plan_item_id: str = "",
) -> Dict[str, Any]:
    """Build long-form authority directly from a frozen product plan.

    This is the first-class new-SKU path.  It reuses the stable planner's
    product, claim, structure, persona, outfit and scene choices without
    generating a hidden 15-second script first.  A completed short script is
    therefore an optional quality asset, never an admission requirement.
    """

    context = _dict(product_context)
    package = _dict(frozen_package)
    seed = _dict(package.get("simplified_creative_seed"))
    product_truth = _dict(seed.get("product_truth"))
    if not product_truth:
        raise RuntimeError("DIRECT_PRODUCT_PLAN 缺少冻结商品事实")
    # The 15-second model-facing seed intentionally omits routing metadata.
    # Long-form identity compilation still needs the already-authoritative
    # product type, so restore it from the operation context without asking a
    # model to infer anything.
    product_truth.setdefault(
        "canonical_product_type", _text(context.get("product_type"))
    )
    product_truth.setdefault(
        "product_code", _text(product_code or context.get("product_code"))
    )

    diversity = _dict(seed.get("diversity_context"))
    creative = _dict(package.get("creative_diversity_contract"))
    persona = (
        _dict(diversity.get("persona_selection_contract"))
        or _dict(package.get("persona_selection_contract"))
        or _dict(creative.get("persona_selection_contract"))
    )
    outfit = (
        _dict(diversity.get("outfit_selection_contract"))
        or _dict(creative.get("outfit_selection_contract"))
    )
    outfit_recipe = _dict(outfit.get("outfit_recipe"))
    scene_reference = _dict(diversity.get("scene_reference"))
    scene_card = _dict(scene_reference.get("execution_card"))
    scene_space = _dict(scene_card.get("space"))
    scene_recipe = _dict(scene_card.get("visual_scene_recipe"))
    scene_location = _text(
        scene_space.get("location")
        or creative.get("scene_motif")
        or creative.get("scene_family_key")
        or "普通生活空间"
    )
    scene_contract = {
        "location": scene_location,
        "subspace": _text(scene_space.get("subspace")),
        "moment": _text(creative.get("persona_state")),
        "lighting": _text(scene_card.get("lighting")),
        "background": _text(
            scene_recipe.get("space_relationship")
            or scene_space.get("background_depth")
        ),
        "material_palette": _text(scene_recipe.get("material_palette")),
        "lighting_texture": _text(scene_recipe.get("lighting_texture")),
        "lived_in_detail": _text(
            scene_recipe.get("lived_in_detail") or scene_card.get("lived_in_trace")
        ),
        "source": "FROZEN_PRODUCT_PLAN",
    }
    character = _dict(persona.get("script_projection"))
    presentation = _text(
        _dict(seed.get("creative_direction")).get("preferred_presentation")
        or creative.get("required_presentation_mode")
    )
    capture_mode = _text(
        _dict(seed.get("creative_direction")).get("capture_mode")
        or creative.get("capture_mode")
    )
    outfit_projection = _outfit_prompt_projection(outfit)
    color_affinity = _dict(outfit.get("product_color_affinity"))
    world = {
        "character": character or {
            "identity": (
                "不适用，无人物出镜"
                if presentation == "STATIC_PRODUCT"
                else _text(creative.get("persona_role")) or "自然分享者"
            )
        },
        "persona_contract": _compact_persona(persona),
        "outfit": _text(
            outfit_projection.get("frozen_outfit")
            or outfit_recipe.get("one_piece")
            or outfit_recipe.get("top")
        ),
        "outfit_contract": _compact_outfit(outfit),
        "outfit_prompt_projection": outfit_projection,
        "outfit_selection_report": {
            "template_id": _text(outfit.get("template_id")),
            "template_version": _text(outfit.get("template_version")),
            "structured_snapshot_hash": _text(outfit.get("structured_snapshot_hash")),
            "color_affinity": color_affinity,
            "eligible_count": len(color_affinity.get("eligible_template_ids") or []),
            "recent_use_count": int(creative.get("outfit_silhouette_recent_count") or 0),
            "batch_use_count": int(creative.get("outfit_silhouette_batch_count") or 0),
            "reuse_reason": "COMPATIBLE_POOL_REUSE" if creative.get("outfit_silhouette_batch_count") else "BATCH_FIRST_USE",
        },
        "outfit_persona_affinity_contract": _dict(creative.get("outfit_persona_affinity_contract")),
        "outfit_scene_affinity_contract": _dict(creative.get("outfit_scene_affinity_contract")),
        "scene": scene_location,
        "scene_contract": scene_contract,
        "lighting": _text(scene_contract.get("lighting")),
        "person_state": _text(creative.get("persona_state")) or _text(character.get("identity")),
        "product_wear_state": "沿用冻结展示方式并保持全片连续",
        "camera": "普通手机竖屏原生记录，按长视频片段连续推进",
        "carrier_mode": _text(
            _dict(
                _dict(package.get("structure_contract")).get("hard_constraints")
            ).get("content_carrier")
        ),
        "presentation_mode": presentation,
        "capture_mode": capture_mode,
        "visual_saliency": {},
        "opening_scene_projection": {},
    }

    world["visual_saliency"] = segment_visibility(
        {"production_world": world, "product_truth": product_truth}, scene_contract,
    )

    content_bundle = _dict(package.get("content_bundle_brief"))
    semantic_contract = (
        _dict(package.get("semantic_spine_contract"))
        or _dict(seed.get("semantic_spine_contract"))
        or _dict(content_bundle.get("semantic_spine_contract"))
    )
    thesis = _dict(semantic_contract.get("script_thesis"))
    argument = _dict(content_bundle.get("selling_argument"))
    verified = [
        dict(item) for item in product_truth.get("approved_claims") or []
        if isinstance(item, Mapping)
        and (_text(item.get("claim_key")) or _text(item.get("fact_text")))
    ]
    product_code_value = _text(
        product_code or context.get("product_code") or product_truth.get("product_code")
    )
    product_type = _text(
        context.get("product_type") or product_truth.get("canonical_product_type")
    )
    argument_bundle = _build_argument_bundle(
        product_code_value,
        product_type,
        int(duration_seconds),
        semantic_contract,
        argument,
        verified,
    )
    requested_hook_id = _text(
        package.get("requested_hook_id")
        or _dict(seed.get("creative_direction")).get("requested_hook_id")
    )
    return {
        "product_code": product_code_value,
        "target_country": _text(context.get("target_country") or "泰国"),
        "target_language": _text(context.get("target_language") or "泰语"),
        "target_duration_seconds": int(duration_seconds),
        "product_identity_lock": build_product_identity_lock(product_truth),
        "product_truth": product_truth,
        "production_world": world,
        "semantic_spine": {
            "hook_id": requested_hook_id,
            "primary_narrative_context": _text(
                thesis.get("primary_narrative_context")
                or thesis.get("audience_situation")
                or _dict(semantic_contract.get("product_market_context")).get(
                    "primary_usage_world"
                )
            ),
            "core_buying_reason": _text(
                thesis.get("core_buying_reason")
                or argument.get("creative_core_value")
                or argument.get("primary_selling_point")
                or argument.get("operator_expression")
            ),
            "selling_argument": argument,
            "source_semantic_spine_contract": semantic_contract,
        },
        "verified_facts": verified,
        "approved_supporting_arguments": argument_bundle["supporting_arguments"],
        "longform_argument_bundle": argument_bundle,
        "scene_mode": "auto",
        "requested_hook_id": requested_hook_id,
        "relationship_language": {},
        "approved_style_references": [],
        "native_rhetoric_contract": {},
        "source_lineage": {
            "source_script_id": "",
            "source_plan_item_id": _text(source_plan_item_id),
            "source_mode": "DIRECT_PRODUCT_PLAN",
        },
    }


def add_supporting_scripts(source: Mapping[str, Any], scripts: list[Mapping[str, Any]],
                           *, limit: int = 2) -> Dict[str, Any]:
    """Add related approved meanings without turning them into extra mainlines."""

    result = dict(source)
    supporting = []
    verified = list(result.get("verified_facts") or [])
    seen_arguments = {
        _text(_dict(_dict(result.get("semantic_spine")).get("selling_argument")).get("argument_id"))
    }
    seen_claims = {_text(item.get("claim_key")) for item in verified if isinstance(item, Mapping)}
    for raw in scripts:
        script = _unwrap(raw)
        brief = _dict(script.get("video_generation_brief"))
        context = _dict(script.get("semantic_context")) or _dict(brief.get("semantic_context"))
        voiceover = _dict(script.get("voiceover")) or _dict(brief.get("voiceover"))
        argument_id = _text(voiceover.get("selected_selling_argument_id"))
        reason = _text(context.get("core_buying_reason"))
        if not argument_id or not reason or argument_id in seen_arguments:
            continue
        supporting.append({
            "argument_id": argument_id,
            "primary_narrative_context": _text(context.get("primary_narrative_context")),
            "supporting_reason": reason,
            "authority": "APPROVED_SAME_PRODUCT_SUPPORT",
            "source_script_id": _text(script.get("complete_script_id")),
        })
        seen_arguments.add(argument_id)
        for item in brief.get("verified_facts") or script.get("verified_facts") or []:
            if not isinstance(item, Mapping):
                continue
            claim_key = _text(item.get("claim_key"))
            if claim_key and claim_key not in seen_claims:
                verified.append(dict(item))
                seen_claims.add(claim_key)
        if len(supporting) >= max(0, int(limit)):
            break
    result["approved_supporting_arguments"] = supporting
    result["verified_facts"] = verified
    bundle = dict(result.get("longform_argument_bundle") or {})
    existing = list(bundle.get("supporting_arguments") or [])
    seen = {_text(item.get("argument_id")) for item in existing if isinstance(item, Mapping)}
    for item in supporting:
        if item["argument_id"] not in seen:
            existing.append(item)
            seen.add(item["argument_id"])
    _, maximum = recommended_argument_range(int(result.get("target_duration_seconds") or 27))
    bundle["supporting_arguments"] = existing[: max(0, maximum - 1)]
    bundle["visual_facts"] = verified
    result["longform_argument_bundle"] = bundle
    return result
