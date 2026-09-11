"""Read-only central expression resources; no short-form content allocation."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Mapping


def _json_safe(value: Any) -> Any:
    """Freeze provider/RDS values before they enter persisted job JSON.

    Some read-only rhetoric diagnostics contain native ``datetime`` objects.
    They are useful for audit but must never make an otherwise successful
    voiceover impossible to persist.
    """
    return json.loads(json.dumps(value, ensure_ascii=False, default=str))


def _remove_sales_surface_examples(native: Mapping[str, Any]) -> dict[str, Any]:
    """Do not teach natural sharing from promotion/live-selling excerpts."""
    output = dict(native)
    kept = []
    removed = []
    sales_tokens = ("[cta]", "สต๊อก", "ไลฟ์", "ไล์", "ราคา", "หมดเร็ว", "รีบเข้า")
    for row in native.get("native_surface_references") or []:
        text = str((row or {}).get("reference_excerpt") or "").lower()
        if any(token in text for token in sales_tokens):
            removed.append(str((row or {}).get("video_id") or ""))
        else:
            kept.append(row)
    output["native_surface_references"] = kept
    diagnostics = dict(output.get("diagnostics") or {})
    diagnostics["longform_sales_surface_excluded_ids"] = removed
    output["diagnostics"] = diagnostics
    if removed and not kept:
        output["fallback_level"] = "WRITING_CASE_AND_STRUCTURE_ONLY"
    return output


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
        allow_cross_hook_language_fallback=True,
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
    native = _remove_sales_surface_examples(native)
    writing_case = {}
    writing_case_audit = {"status": "DISABLED"}
    try:
        engine_root = Path("/Users/likeu3/voiceover_copy_engine")
        if str(engine_root) not in sys.path:
            sys.path.insert(0, str(engine_root))
        from voiceover_copy_engine.services.writing_cases import select_writing_case
        writing_case = select_writing_case(
            language=language, country=country, top_category=category,
            product_type=product_type,
            primary_context=str(semantic.get("primary_narrative_context") or argument.get("text") or ""),
            diagnostics=writing_case_audit,
        ) if os.environ.get("LONGFORM_WRITING_CASE_ENABLED", "0") == "1" else {}
    except Exception as exc:
        diagnostics.append("WRITING_CASE_PROVIDER_" + type(exc).__name__)
    return _json_safe({
        "schema_version": "longform-central-resources-v2-independent-expression-retrieval",
        "scope": {"hook_id": hook_id, "country": country, "language": language,
                  "top_category": category, "product_type": product_type},
        "hook_guidance": {key: hook[key] for key in (
            "hook_id", "hook_name", "hook_type", "core_intent", "attention_mechanisms",
            "minimal_structure", "relation_modes",
        ) if hook.get(key) not in (None, "", [])},
        "relationship_language": _relationship_language_profile(hook_id, target_language=language),
        "approved_style_references": references,
        "approved_style_reference_audit": {
            "selected_count": len(references),
            "selected_ids": [
                str(item.get("reference_sample_id") or "") for item in references
                if item.get("reference_sample_id")
            ],
            "selection_scope": [
                str(item.get("reference_scope") or "") for item in references
                if item.get("reference_scope")
            ],
        },
        "native_rhetoric_contract": _compact_native_rhetoric_contract(native),
        "writing_case_reference": writing_case,
        "writing_case_audit": writing_case_audit,
        "native_rhetoric_audit": native,
        "snapshot_hash": hook_knowledge_snapshot_hash(),
        "diagnostics": diagnostics,
        "reference_authority": "EXPRESSION_ONLY_NOT_PRODUCT_FACTS",
    })
