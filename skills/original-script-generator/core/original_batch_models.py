"""V1 原创批次数据模型 — 状态枚举、快照与ID生成、数据类"""
from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# ── Status enums ──────────────────────────────────────────────────────

BATCH_STATUSES = frozenset({
    "REQUESTED", "PLANNING", "PLANNED", "PARTIAL_PLANNED",
    "DISPATCHING", "SCRIPT_READY", "PARTIAL_FAILED", "FAILED",
})

ITEM_STATUSES = frozenset({
    "PLANNED", "SCRIPT_RUNNING", "SCRIPT_READY", "SCRIPT_FAILED",
})

ITEM_ROLES = frozenset({"STRUCTURE_MOTHER", "CONTENT_VARIANT", "HOOK_VARIANT"})

TEST_PHASES = frozenset({"INITIAL", "RETEST", "FINAL", "SCALE_OBSERVE"})

EXECUTION_MODES = frozenset({"PLAN_ONLY", "SCRIPT_ONLY"})

TENSION_STATUSES = frozenset({"AVAILABLE", "UNAVAILABLE"})

CONTENT_ANGLE_KEYS = frozenset({
    "FACT_DISCOVERY", "DETAIL_OBSERVATION", "RESULT_STATE",
    "VALUE_"  # prefix for value-direction keys
})

HOOK_ID_BLACKLIST_FOR_NO_TENSION = frozenset({"PAIN_REFRAME"})


# ── ID / hash generators ──────────────────────────────────────────────

def _text(value: Any) -> str:
    return str(value or "").strip()


def _stable_id(prefix: str, material: Any) -> str:
    raw = json.dumps(material, ensure_ascii=False, sort_keys=True, default=str)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20].upper()
    return prefix + digest


def generate_batch_id(
    request_id: str,
    product_code: str,
    input_hash: str,
    data_snapshot_hash: str,
    policy_version: str,
    random_seed: int,
) -> str:
    return _stable_id("OCB_", {
        "request_id": request_id,
        "product_code": product_code,
        "input_hash": input_hash,
        "data_snapshot": data_snapshot_hash,
        "policy": policy_version,
        "seed": random_seed,
    })


def generate_batch_item_id(batch_id: str, item_index: int, allocation_signature: str) -> str:
    return _stable_id("OCI_", {
        "batch_id": batch_id,
        "item_index": item_index,
        "allocation_signature": allocation_signature,
    })


def generate_request_id(
    product_code: str,
    seed: int,
    phase: str,
    script_mode: str = "legacy_v2",
) -> str:
    return _stable_id("OP_ORIGINAL_", {
        "product_code": product_code,
        "seed": seed,
        "phase": phase,
        "script_mode": script_mode,
    })


def build_allocation_signature(
    direction_assignment_id: str,
    content_angle_key: str,
    claim_keys: List[str],
    requested_hook_id: str,
    visual_signature: str,
    selling_argument_id: str = "",
) -> str:
    material = "|".join([
        direction_assignment_id,
        content_angle_key,
        ",".join(sorted(claim_keys)),
        requested_hook_id,
        visual_signature,
        selling_argument_id,
    ])
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def build_input_snapshot(
    product_context: Dict[str, Any],
    selection: Dict[str, Any],
    active_hook_ids: List[str],
    creative_policy_version: str,
    allocation_policy_version: str,
) -> Dict[str, Any]:
    return {
        "source_run_id": product_context.get("source_run_id"),
        "input_hash": product_context.get("input_hash", ""),
        "anchor_card_hash": hashlib.sha256(
            json.dumps(product_context.get("anchor_card", {}), ensure_ascii=False, sort_keys=True, default=str).encode()
        ).hexdigest()[:16],
        "selling_point_catalog_hash": hashlib.sha256(
            json.dumps(product_context.get("selling_point_catalog", []), ensure_ascii=False, sort_keys=True, default=str).encode()
        ).hexdigest()[:16],
        "selling_point_catalog_snapshot": product_context.get(
            "selling_point_catalog_snapshot", {}
        ),
        "selling_point_catalog_sources": product_context.get(
            "selling_point_catalog_sources", {}
        ),
        "selection_run_id": selection.get("selection_run_id", ""),
        "structure_data_snapshot_hash": selection.get("data_snapshot_hash", ""),
        "direction_assignments": [
            {
                "direction_assignment_id": a.get("direction_assignment_id", ""),
                "cluster_id": a.get("cluster_id"),
                "cluster_version": a.get("cluster_version", ""),
            }
            for a in selection.get("assignments", []) if isinstance(a, dict)
        ],
        "active_hook_ids": sorted(active_hook_ids),
        "creative_policy_version": creative_policy_version,
        "allocation_policy_version": allocation_policy_version,
    }


def build_data_snapshot_hash(input_snapshot: Dict[str, Any]) -> str:
    raw = json.dumps(input_snapshot, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


# ── Request model ─────────────────────────────────────────────────────

@dataclass
class BatchRequest:
    request_id: str
    product_code: str
    requested_count: int
    test_phase: str
    duration_seconds: float = 15.0
    execution_mode: str = "PLAN_ONLY"
    auto_video_enabled: bool = False
    random_seed: int = 0
    target_country: str = ""
    target_language: str = ""
    top_category: str = ""
    product_type: str = ""
    source_record_id: str = ""
    script_mode: str = "legacy_v2"

    def __post_init__(self):
        self.requested_count = max(1, min(20, int(self.requested_count)))
        self.test_phase = self.test_phase if self.test_phase in TEST_PHASES else "INITIAL"
        self.execution_mode = self.execution_mode if self.execution_mode in EXECUTION_MODES else "PLAN_ONLY"
        self.script_mode = (
            self.script_mode
            if self.script_mode in {"legacy_v2", "simplified_v1"}
            else "legacy_v2"
        )


# ── Batch record ──────────────────────────────────────────────────────

@dataclass
class BatchRecord:
    batch_id: str
    request_id: str
    product_code: str
    requested_count: int
    test_phase: str
    execution_mode: str
    policy_version: str
    random_seed: int
    data_snapshot_hash: str
    input_snapshot_json: str
    planned_count: int = 0
    ready_count: int = 0
    failed_count: int = 0
    status: str = "REQUESTED"
    error_message: str = ""
    allocation_summary_json: str = ""
    duration_seconds: float = 15.0
    auto_video_enabled: bool = False
    target_country: str = ""
    target_language: str = ""
    top_category: str = ""
    product_type: str = ""
    source_record_id: str = ""


# ── Plan item ─────────────────────────────────────────────────────────

@dataclass
class PlanItem:
    batch_item_id: str
    batch_id: str
    item_index: int
    item_role: str
    product_code: str
    selection_run_id: str
    direction_assignment_id: str
    compatibility_slot: str
    structure_contract_json: str
    allocation_signature: str
    policy_version: str
    item_snapshot_hash: str
    content_bundle_id: str
    content_bundle_json: str
    content_angle_key: str
    audience_tension_status: str
    claim_keys_json: str
    requested_hook_id: str
    eligible_hook_ids_json: str
    creative_contract_id: str = ""
    visual_signature: str = ""
    frozen_direction_package_json: str = ""
    stage_checkpoint_json: str = ""
    status: str = "PLANNED"
    cluster_id: Optional[int] = None
    cluster_version: str = ""
    evidence_tier: str = ""
    macro_family_key: str = ""
    carrier_mode: str = ""
    execution_reference_json: str = ""
    audience_tension_text: str = ""
    actual_hook_id: str = ""
    consumer_run_id: str = ""
    script_id: str = ""
    content_id: str = ""
    video_prompt_id: str = ""
    structure_binding_id: str = ""
    attempt_count: int = 0
    error_code: str = ""
    error_message: str = ""
    result_json: str = ""
