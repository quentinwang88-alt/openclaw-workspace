"""Pure, backwards-compatible view of a mother for compilation and revisions."""
from __future__ import annotations

from typing import Any

from .hashing import stable_hash
from .models import ExecutionSummary, ReplicationCompileOutput
from .prompts import load_prompt, prompt_fingerprint


EXECUTION_POLICY_VERSION = "mechanism-first-v1"


def execution_summary(contract: Any) -> dict[str, Any]:
    value = contract if isinstance(contract, dict) else contract.model_dump(mode="json")
    if value.get("execution_summary"):
        return ExecutionSummary.model_validate(value["execution_summary"]).model_dump(mode="json")
    # Legacy requirements are retained verbatim, not silently reclassified by
    # keyword heuristics. Their provenance tells callers this is NOT a fresh
    # model separation of mechanism and filming; explicit revisions may refine.
    points = value.get("core_points") or [
        {"point_id": f"legacy_{i}", "requirement": str(requirement), "evidence_source": "legacy_derived"}
        for i, key in enumerate(("hook_engine", "reveal_engine", "proof_engine", "conversion_engine"), 1)
        if (requirement := (value.get("core_mechanism") or {}).get(key))
    ]
    baseline = [str(slot.get("source_content") or "")
                for segment in value.get("segments") or [] for slot in segment.get("slots") or []]
    return {"schema_version": "1", "core_mechanisms": [
                {**point, "evidence_source": "legacy_derived"} for point in points],
            "baseline_shooting": [item for item in baseline if item][:8],
            "variable_expression": list((value.get("global_rules") or {}).get("free_variables") or [])[:10],
            "legacy_constraints": [*(value.get("global_rules") or {}).get("hard_locks", []),
                                   *(value.get("global_rules") or {}).get("forbidden_mutations", [])],
            "provenance": "legacy_derived"}


def compilation_provenance(mother: Any) -> dict[str, Any]:
    return {"execution_policy_version": EXECUTION_POLICY_VERSION,
            "mother_id": mother.mother_id, "mother_version": mother.version,
            "mother_fingerprint": mother.source_hash,
            "compile_prompt_fingerprint": prompt_fingerprint("replication_compile", "compile_sales", "compile_nurture", "compile_high_fidelity", "compile_general"),
            "compile_schema_fingerprint": stable_hash(ReplicationCompileOutput.model_json_schema()),
            "mother_processing": (mother.contract.processing_provenance.model_dump(mode="json")
                                  if mother.contract.processing_provenance else {"provenance": "legacy_unknown"}),
            "mother_review_status": mother.review.review_status}


def load_compile_prompt(publish_purpose: str, modes: set[str]) -> str:
    if publish_purpose not in {"带货", "养号"}:
        raise ValueError("compile purpose must be explicit")
    if not modes or not modes <= {"high_fidelity", "general"}:
        raise ValueError("compile mode must be explicit")
    parts = [load_prompt("replication_compile"),
             load_prompt("compile_sales" if publish_purpose == "带货" else "compile_nurture")]
    parts.extend(load_prompt("compile_" + mode) for mode in sorted(modes))
    return "\n\n".join(parts)
