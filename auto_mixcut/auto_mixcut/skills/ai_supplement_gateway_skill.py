from __future__ import annotations

from datetime import datetime, timedelta
import os
from typing import Any

from .context import SkillContext


READY_TO_SUBMIT_PACKAGE_STATUSES = {"created", "待提单", "已创建"}
NEEDS_FEISHU_SYNC_PACKAGE_STATUSES = {"pending_feishu_sync", "feishu_sync_failed"}
RECOVERABLE_FAILED_PACKAGE_STATUSES = {"failed", "失败"}
IN_FLIGHT_PACKAGE_STATUSES = {
    "submitted",
    "generating",
    "returned",
    "imported",
    "已提单",
    "生成中",
    "已生成",
    "已回流",
    "质检中",
    "质检通过",
    "uploaded",
    "downloaded",
    "rendering",
    "observing",
}
IMPORTED_PACKAGE_STATUSES = {"imported", "已回流", "质检通过", "downloaded", "uploaded"}
CONSUMED_PACKAGE_STATUSES = {"consumed", "已消费"}


class AISupplementGatewaySkill:
    """Single source of truth for AI supplement package state and submit budget."""

    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def package_state(self, product_id: str) -> dict[str, int]:
        rows = _list_packages(self.ctx, product_id)
        used_prompt_package_ids = _used_prompt_package_ids(self.ctx, product_id, rows)
        state = {
            "package_count": len(rows),
            "ready_to_submit_count": 0,
            "needs_feishu_sync_count": 0,
            "recoverable_failed_count": 0,
            "inflight_count": 0,
            "imported_package_count": 0,
            "consumed_package_count": 0,
            "failed_package_count": 0,
            "stale_inflight_count": 0,
        }
        for row in rows:
            prompt_id = str(row.get("segment_prompt_id") or "").strip()
            normalized = normalize_package(row)
            if prompt_id in used_prompt_package_ids and normalized == "imported":
                normalized = "consumed"
            if normalized == "ready_to_submit":
                state["ready_to_submit_count"] += 1
            elif normalized == "needs_feishu_sync":
                state["needs_feishu_sync_count"] += 1
            elif normalized == "recoverable_failed":
                state["recoverable_failed_count"] += 1
                if is_stale_inflight_package(row):
                    state["stale_inflight_count"] += 1
            elif normalized == "inflight":
                state["inflight_count"] += 1
            elif normalized == "imported":
                state["imported_package_count"] += 1
            elif normalized == "consumed":
                state["consumed_package_count"] += 1
            elif normalized == "failed":
                state["failed_package_count"] += 1
        state["active_package_count"] = (
            state["ready_to_submit_count"]
            + state["inflight_count"]
        )
        state["future_package_count"] = state["active_package_count"] + state["recoverable_failed_count"]
        return state

    def submit_budget(self, product_id: str, remaining_count: int | None = None, configured_limit: int | None = None) -> dict[str, int]:
        remaining = int(remaining_count if remaining_count is not None else self._remaining_count(product_id))
        remaining = max(0, remaining)
        limit = int(configured_limit if configured_limit is not None else _configured_submit_limit())
        state = self.package_state(product_id)
        return submit_budget_from_state(remaining, state, configured_limit=limit)

    def _remaining_count(self, product_id: str) -> int:
        rows = self.ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC", (product_id,))
        task = rows[0] if rows else {}
        target = int(task.get("requested_variant_count") or task.get("allowed_variant_count") or 0)
        actual = int(task.get("actual_variant_count") or 0)
        return int(task.get("target_remaining_variant_count") or max(0, target - actual))


def normalize_package(row: dict[str, Any]) -> str:
    status = _status(row)
    result_sync = str(row.get("result_sync_status") or "").strip()
    failure = str(row.get("failure_reason") or "").strip()
    if status in CONSUMED_PACKAGE_STATUSES:
        return "consumed"
    if row.get("generated_asset_id") or row.get("generated_segment_id"):
        return "imported"
    if status in IMPORTED_PACKAGE_STATUSES or result_sync in IMPORTED_PACKAGE_STATUSES:
        return "imported"
    if is_stale_inflight_package(row):
        return "recoverable_failed"
    if status in IN_FLIGHT_PACKAGE_STATUSES or result_sync in IN_FLIGHT_PACKAGE_STATUSES:
        return "inflight"
    if status in READY_TO_SUBMIT_PACKAGE_STATUSES or result_sync in READY_TO_SUBMIT_PACKAGE_STATUSES:
        feishu_record_id = str(row.get("feishu_record_id") or "").strip()
        if not feishu_record_id.startswith("rec"):
            return "needs_feishu_sync"
        return "ready_to_submit"
    if status in NEEDS_FEISHU_SYNC_PACKAGE_STATUSES:
        return "needs_feishu_sync"
    if status in RECOVERABLE_FAILED_PACKAGE_STATUSES:
        return "recoverable_failed" if is_recoverable_submit_failure(failure) else "failed"
    return "unknown"


def submit_budget_from_state(remaining_count: int, state: dict[str, Any], configured_limit: int | None = None) -> dict[str, int]:
    remaining = max(0, int(remaining_count or 0))
    limit = int(configured_limit if configured_limit is not None else _configured_submit_limit())
    active = int(state.get("active_package_count") or 0)
    ready = int(state.get("ready_to_submit_count") or 0)
    retry = int(state.get("recoverable_failed_count") or 0)
    inflight = int(state.get("inflight_count") or 0)
    imported = int(state.get("imported_package_count") or 0)
    consumed = int(state.get("consumed_package_count") or 0)
    ready_or_retry = ready + retry
    needed_after_active = max(0, remaining - active)
    needed_after_inflight = max(0, remaining - inflight)
    submit_limit = min(limit, needed_after_active)
    if ready_or_retry > 0:
        submit_limit = min(limit, ready_or_retry, needed_after_inflight)
    return {
        "target_remaining": remaining,
        "remaining_count": remaining,
        "ai_submit_active_count": active,
        "ai_submit_inflight_count": inflight,
        "ready_to_submit_count": ready,
        "recoverable_failed_count": retry,
        "imported_package_count": imported,
        "consumed_package_count": consumed,
        "future_package_count": active + retry,
        "needed_after_active": needed_after_active,
        "needed_after_inflight": needed_after_inflight,
        "submit_limit": max(0, submit_limit),
    }


def is_recoverable_submit_failure(text: str) -> bool:
    lower = str(text or "").lower()
    return any(
        token in lower
        for token in [
            "imini_allow_real_submit",
            "real_submit_disabled",
            "真实提交默认关闭",
            "高峰期",
            "暂时无法提交更多任务",
            "无法提交更多任务",
            "请等待其他任务完成",
            "platform_limited",
            "retry_pending",
            "队列已满",
            "提示词输入失败",
            "prompt_input_failed",
        ]
    )


def is_stale_inflight_package(row: dict[str, Any]) -> bool:
    status = _status(row)
    result_sync = str(row.get("result_sync_status") or "").strip()
    if status not in IN_FLIGHT_PACKAGE_STATUSES and result_sync not in IN_FLIGHT_PACKAGE_STATUSES:
        return False
    if row.get("generated_asset_id") or row.get("generated_segment_id"):
        return False
    timestamp = _package_updated_at(row)
    if timestamp is None:
        return False
    return datetime.utcnow() - timestamp > timedelta(hours=_configured_inflight_stale_hours())


def _package_updated_at(row: dict[str, Any]) -> datetime | None:
    for key in ("updated_at", "created_at", "submitted_at"):
        value = row.get(key)
        parsed = _parse_dt(value)
        if parsed:
            return parsed
    return None


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _list_packages(ctx: SkillContext, product_id: str) -> list[dict[str, Any]]:
    try:
        return ctx.repo.list_where("segment_prompt_packages", "product_id=?", (product_id,))
    except Exception:
        return []


def _used_prompt_package_ids(ctx: SkillContext, product_id: str, package_rows: list[dict[str, Any]]) -> set[str]:
    prompt_ids = {str(row.get("segment_prompt_id") or "").strip() for row in package_rows}
    prompt_ids.discard("")
    if not prompt_ids:
        return set()

    segment_to_prompt: dict[str, str] = {}
    missing_segment_prompt_ids: set[str] = set()
    for row in package_rows:
        prompt_id = str(row.get("segment_prompt_id") or "").strip()
        if not prompt_id:
            continue
        segment_id = str(row.get("generated_segment_id") or "").strip()
        if segment_id:
            segment_to_prompt[segment_id] = prompt_id
        else:
            missing_segment_prompt_ids.add(prompt_id)

    if missing_segment_prompt_ids:
        for chunk in _chunks(sorted(missing_segment_prompt_ids), 200):
            placeholders = ",".join("?" for _ in chunk)
            try:
                segments = ctx.repo.list_where(
                    "segments",
                    f"product_id=? AND prompt_package_id IN ({placeholders})",
                    (product_id, *chunk),
                )
            except Exception:
                segments = []
            for segment in segments:
                segment_id = str(segment.get("segment_id") or "").strip()
                prompt_id = str(segment.get("prompt_package_id") or "").strip()
                if segment_id and prompt_id in prompt_ids:
                    segment_to_prompt[segment_id] = prompt_id

    used_prompt_ids: set[str] = set()
    segment_ids = sorted(segment_to_prompt)
    for chunk in _chunks(segment_ids, 200):
        placeholders = ",".join("?" for _ in chunk)
        try:
            rows = ctx.repo.list_where("output_segments", f"segment_id IN ({placeholders})", tuple(chunk))
        except Exception:
            rows = []
        for row in rows:
            prompt_id = segment_to_prompt.get(str(row.get("segment_id") or ""))
            if prompt_id:
                used_prompt_ids.add(prompt_id)
    return used_prompt_ids


def _chunks(items: list[str], size: int):
    for idx in range(0, len(items), max(1, size)):
        yield items[idx : idx + size]


def _status(row: dict[str, Any]) -> str:
    return str(row.get("package_status") or row.get("status") or "").strip()


def _configured_submit_limit() -> int:
    try:
        return max(1, int(os.environ.get("AUTO_MIXCUT_GUARD_AI_SUBMIT_LIMIT", "5") or "5"))
    except ValueError:
        return 5


def _configured_inflight_stale_hours() -> int:
    try:
        return max(1, int(os.environ.get("AUTO_MIXCUT_AI_PACKAGE_STALE_HOURS", "6") or "6"))
    except ValueError:
        return 6
