#!/usr/bin/env python3
"""Build Feishu status payloads for weekly Market/Selection Agent runs.

The concrete Feishu client varies by table.  These helpers keep status field
semantics consistent and let sync scripts call `client.update_record(...)`
without each script inventing its own labels.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict


def market_status_fields(
    status: str,
    run_id: str = "",
    brief_status: str = "",
    diagnostics: Dict[str, Any] | None = None,
    timestamp: str = "",
) -> Dict[str, Any]:
    now = timestamp or _now_iso()
    return {
        "market_report_status": status,
        "brief_status": brief_status or ("ready" if status in {"ready", "consumable", "success"} else status),
        "brief_ready_at": now if brief_status in {"ready", "consumable"} or status in {"ready", "consumable", "success"} else "",
        "market_agent_run_id": run_id,
        "market_agent_consumed_at": now if status in {"ready", "consumable", "success"} else "",
        "market_agent_diagnostics": diagnostics or {},
    }


def selection_status_fields(
    status: str,
    run_id: str = "",
    rerun_reason: str = "",
    diagnostics: Dict[str, Any] | None = None,
    timestamp: str = "",
) -> Dict[str, Any]:
    now = timestamp or _now_iso()
    return {
        "selection_run_status": status,
        "selection_run_id": run_id,
        "selection_run_at": now if status in {"success", "partial", "failed"} else "",
        "selection_rerun_reason": rerun_reason,
        "selection_agent_consumed_at": now if status == "success" else "",
        "selection_agent_diagnostics": diagnostics or {},
    }


def standardized_snapshot_status_fields(
    market_consumed: bool = False,
    selection_consumed: bool = False,
    timestamp: str = "",
) -> Dict[str, Any]:
    now = timestamp or _now_iso()
    return {
        "market_agent_consumed_at": now if market_consumed else "",
        "selection_agent_consumed_at": now if selection_consumed else "",
    }


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
