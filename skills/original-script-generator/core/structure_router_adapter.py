"""Original-script adapter for the shared video structure router."""

from __future__ import annotations

import copy
import hashlib
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
ROUTER_PACKAGE_ROOT = WORKSPACE_ROOT / "packages" / "video_structure_router"
if str(ROUTER_PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(ROUTER_PACKAGE_ROOT))

from video_structure_router import (  # noqa: E402
    RouteRequest,
    RouterStorage,
    StructureRouterService,
    validate_script_against_contract,
    validate_video_prompt_against_contract,
)


def _stable_seed(*values: Any) -> int:
    material = "|".join(str(value or "") for value in values)
    return int(hashlib.sha256(material.encode("utf-8")).hexdigest()[:12], 16)


def select_original_structure_directions(
    *,
    context: Dict[str, Any],
    anchor_card: Dict[str, Any],
    record_id: str,
    input_hash: str,
    direction_count: int = 4,
    random_seed: Optional[int] = None,
    allowed_carriers: Optional[List[str]] = None,
    recent_cluster_usage: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    category_contract = (
        anchor_card.get("category_execution_contract")
        if isinstance(anchor_card.get("category_execution_contract"), dict)
        else {}
    )
    operation_policy = str(category_contract.get("operation_policy") or "").strip()
    forbidden_beats = (
        ["USE_PROCESS"]
        if operation_policy in {"process_forbidden", "static_result_only"}
        else []
    )
    request = RouteRequest(
        request_id=f"{record_id}:{input_hash}",
        consumer_flow="ORIGINAL_SCRIPT",
        product_code=str(context.get("product_code") or ""),
        target_country=str(context.get("target_country") or ""),
        category=str(context.get("top_category") or ""),
        product_type=str(context.get("product_type") or ""),
        direction_count=direction_count,
        duration_seconds=15.0,
        random_seed=(
            int(random_seed)
            if random_seed is not None
            else _stable_seed(
                context.get("product_code"),
                context.get("target_country"),
                context.get("product_type"),
                input_hash,
            )
        ),
        capabilities={
            "allowed_carriers": list(
                allowed_carriers
                or ["HAND_ONLY", "STATIC_PRODUCT", "MIXED", "WEARER_ACTIVE"]
            ),
            # 当前原创脚本 Schema 仍是 4-6 个分镜，不能忠实承载 SINGLE_SHOT。
            "allowed_continuity_modes": ["MULTI_CUT", "CONTINUOUS_LOW_CUT"],
            "forbidden_beats": forbidden_beats,
            "min_shots": 4,
            "max_shots": 6,
            # Planning history only affects exploratory directions.  The
            # highest-evidence baseline remains stable.
            "recent_cluster_usage": dict(recent_cluster_usage or {}),
        },
        product_context={
            "category_execution_contract": category_contract,
            "operation_anchors": anchor_card.get("operation_anchors", []),
            "display_anchors": anchor_card.get("display_anchors", []),
        },
    )
    storage = RouterStorage()
    result = StructureRouterService(storage=storage).select(request)
    return result.to_dict()


def contract_for_slot(selection: Optional[Dict[str, Any]], script_index: int) -> Dict[str, Any]:
    if not isinstance(selection, dict):
        return {}
    slot = f"S{script_index}"
    for assignment in selection.get("assignments", []) or []:
        if isinstance(assignment, dict) and str(assignment.get("output_slot") or "") == slot:
            contract = assignment.get("structure_contract")
            return copy.deepcopy(contract) if isinstance(contract, dict) else {}
    return {}


def structure_direction_packages_for_prompt(selection: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(selection, dict):
        return {}
    assignments: List[Dict[str, Any]] = []
    for value in selection.get("assignments", []) or []:
        if not isinstance(value, dict):
            continue
        assignments.append(
            {
                "output_slot": value.get("output_slot", ""),
                "direction_role": value.get("direction_role", ""),
                "evidence_tier": value.get("evidence_tier", ""),
                "macro_family_key": value.get("macro_family_key", ""),
                "visual_archetype_key": value.get("visual_archetype_key", ""),
                "structure_contract": value.get("structure_contract", {}),
            }
        )
    return {
        "selection_run_id": selection.get("selection_run_id", ""),
        "selection_status": selection.get("selection_status", ""),
        "degraded_reasons": selection.get("degraded_reasons", []),
        "assignments": assignments,
    }


def attach_contract_to_strategy(
    strategy: Dict[str, Any],
    selection: Optional[Dict[str, Any]],
    script_index: int,
) -> Dict[str, Any]:
    result = copy.deepcopy(strategy) if isinstance(strategy, dict) else {}
    contract = contract_for_slot(selection, script_index)
    if not contract:
        return result
    provenance = contract.get("provenance") if isinstance(contract.get("provenance"), dict) else {}
    result["structure_contract"] = contract
    result["structure_direction_id"] = provenance.get("direction_assignment_id", "")
    result["structure_selection_run_id"] = provenance.get("selection_run_id", "")
    result["structure_direction_role"] = provenance.get("direction_role", "")
    result["structure_macro_family_key"] = (
        contract.get("direction_identity", {}).get("macro_family_key", "")
        if isinstance(contract.get("direction_identity"), dict)
        else ""
    )
    return result


def annotate_artifact_with_structure(
    artifact: Dict[str, Any],
    contract: Dict[str, Any],
) -> Dict[str, Any]:
    if not isinstance(artifact, dict) or not contract:
        return artifact
    provenance = contract.get("provenance") if isinstance(contract.get("provenance"), dict) else {}
    artifact["_structure_provenance"] = {
        **provenance,
        "direction_identity": contract.get("direction_identity", {}),
        "evidence": contract.get("evidence", {}),
    }
    return artifact


def validate_script_contract(script_json: Dict[str, Any], contract: Dict[str, Any]) -> Dict[str, Any]:
    if not contract:
        return {"valid": True, "blocking_issues": [], "warnings": [], "observed": {}}
    return validate_script_against_contract(script_json, contract).to_dict()


def validate_video_prompt_contract(prompt_json: Dict[str, Any], contract: Dict[str, Any]) -> Dict[str, Any]:
    if not contract:
        return {"valid": True, "blocking_issues": [], "warnings": [], "observed": {}}
    return validate_video_prompt_against_contract(prompt_json, contract).to_dict()


def bind_structure_application(
    *,
    contract: Dict[str, Any],
    consumer_run_id: str,
    record_id: str,
    product_code: str,
    application_stage: str,
    script_id: str = "",
    content_id: str = "",
    video_prompt_id: str = "",
    metadata: Optional[Dict[str, Any]] = None,
) -> str:
    provenance = contract.get("provenance") if isinstance(contract.get("provenance"), dict) else {}
    selection_run_id = str(provenance.get("selection_run_id") or "")
    direction_assignment_id = str(provenance.get("direction_assignment_id") or "")
    if not selection_run_id or not direction_assignment_id:
        return ""
    return RouterStorage().bind_application(
        selection_run_id=selection_run_id,
        direction_assignment_id=direction_assignment_id,
        consumer_flow="ORIGINAL_SCRIPT",
        consumer_run_id=str(consumer_run_id or ""),
        record_id=record_id,
        product_code=product_code,
        application_stage=application_stage,
        script_id=script_id,
        content_id=content_id,
        video_prompt_id=video_prompt_id,
        metadata=metadata or {},
    )
