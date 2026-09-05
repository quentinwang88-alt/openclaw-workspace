"""Read-only central expression resources; no short-form content allocation."""
from __future__ import annotations

from typing import Any, Mapping


def resolve_longform_voiceover_resources(master: Mapping[str, Any]) -> dict[str, Any]:
    from core.complete_voiceover_direct import (
        _approved_style_references, _central_native_rhetoric_contract,
        _compact_native_rhetoric_contract, _relationship_language_profile,
        hook_knowledge_snapshot_hash, load_active_voiceover_hooks,
    )

    semantic = dict(master.get("semantic_spine") or {})
    hook_id = str(semantic.get("hook_id") or master.get("requested_hook_id") or "")
    country = str(master.get("target_country") or "")
    language = str(master.get("target_language") or "")
    lineage = dict(master.get("source_lineage") or {})
    truth = dict(master.get("product_truth") or {})
    category = str(master.get("top_category") or lineage.get("top_category") or "")
    product_type = str(master.get("product_type") or lineage.get("product_type")
                       or truth.get("canonical_product_type") or "")
    argument = dict((master.get("longform_argument_bundle") or {}).get("primary_argument")
                    or semantic.get("selling_argument") or {})
    semantic_tags = dict(argument.get("semantic_tags") or {})
    frozen_argument = dict(semantic.get("selling_argument") or {})
    # Argument bundles also use a string enum here (semantic authority, not
    # rhetorical permission). Only mapping values can grant permissions;
    # retain the frozen semantic policy fallback without changing the source.
    expression_policy = next((dict(value) for value in (
        argument.get("expression_policy"), frozen_argument.get("expression_policy"),
        semantic.get("expression_policy"),
    ) if isinstance(value, Mapping) and value), {})
    tension = argument.get("audience_tension") or frozen_argument.get("audience_tension") or ""
    if isinstance(tension, Mapping):
        tension = tension.get("text") or ""
    need_authority = str(argument.get("audience_need_authority")
                         or frozen_argument.get("audience_need_authority") or "").upper()
    diagnostics = []
    try:
        hooks = load_active_voiceover_hooks()
        hook = next((row for row in hooks if str(row.get("hook_id") or "") == hook_id), {})
    except Exception as exc:
        hook = {}
        diagnostics.append("HOOK_PROVIDER_" + type(exc).__name__)
    references = _approved_style_references(
        hook_id, target_country=country, top_category=category, product_type=product_type,
    )
    native = _central_native_rhetoric_contract(
        voiceover_root="", requested_hook_id=hook_id, target_country=country,
        target_language=language, top_category=category, product_type=product_type,
        audience_tension=str(tension),
        audience_need_authorized=(need_authority == "APPROVED_SELLING_SCENARIO"
                                  or bool(semantic_tags.get("audience_need_authorized"))),
        rhetorical_conflict_authorized=bool(expression_policy.get("rhetorical_conflict_allowed")
                                            or semantic_tags.get("rhetorical_conflict_authorized")),
    )
    return {
        "schema_version": "longform-central-resources-v1",
        "scope": {"hook_id": hook_id, "country": country, "language": language,
                  "top_category": category, "product_type": product_type},
        "hook_guidance": {key: hook[key] for key in (
            "hook_id", "hook_name", "hook_type", "core_intent", "attention_mechanisms",
            "minimal_structure", "relation_modes",
        ) if hook.get(key) not in (None, "", [])},
        "relationship_language": _relationship_language_profile(hook_id, target_language=language),
        "approved_style_references": references,
        "native_rhetoric_contract": _compact_native_rhetoric_contract(native),
        "snapshot_hash": hook_knowledge_snapshot_hash(),
        "diagnostics": diagnostics,
        "reference_authority": "EXPRESSION_ONLY_NOT_PRODUCT_FACTS",
    }
