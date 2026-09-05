"""RDS-backed automatic publish scheduling for approved OPV videos.

Scheduling and submission are deliberately separate:

* queue_task chooses an account-local slot and creates a publish record;
* tick selects a fresh country BGM only inside the 120-minute window, then
  submits one idempotent NeoBund scheduled task;
* submitted tasks are re-queried after the target time and are never blindly
  resubmitted on an ambiguous response.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from domain import statuses
from domain.models import PublishRecord, utc_now
from services.neobund_publisher import (
    BGM_SELECTION_MAX_LEAD_MINUTES,
    OpvPublishFlow,
    PublishFlowError,
)

NEOBUND_TIMEZONE = "Asia/Shanghai"
MIN_SUBMIT_LEAD_MINUTES = 15
CONFIRM_GRACE_MINUTES = 5
MAX_SCHEDULE_DAYS = 14


class PublishSchedulerError(RuntimeError):
    pass


@dataclass(frozen=True)
class PublishSlot:
    account_id: str
    account_timezone: str
    planned_publish_at_utc: datetime
    account_local_time: datetime
    neobund_wall_time: datetime


def _parse_window(value: str) -> Tuple[int, int]:
    try:
        start_raw, end_raw = str(value).split("-", 1)
        start = datetime.strptime(start_raw.strip(), "%H:%M")
        end = datetime.strptime(end_raw.strip(), "%H:%M")
    except (TypeError, ValueError) as exc:
        raise PublishSchedulerError(f"invalid publish window: {value!r}") from exc
    start_minutes = start.hour * 60 + start.minute
    end_minutes = end.hour * 60 + end.minute
    if end_minutes <= start_minutes:
        raise PublishSchedulerError(f"overnight/empty window is unsupported: {value!r}")
    return start_minutes, end_minutes


def _allocate_counts(windows: Sequence[Tuple[int, int]], total: int) -> List[int]:
    if total < 1:
        raise PublishSchedulerError("daily publish target must be positive")
    durations = [end - start for start, end in windows]
    duration_total = sum(durations)
    raw = [total * duration / duration_total for duration in durations]
    counts = [int(value) for value in raw]
    if total >= len(windows):
        counts = [max(1, value) for value in counts]
    while sum(counts) < total:
        index = max(range(len(raw)), key=lambda i: raw[i] - counts[i])
        counts[index] += 1
    while sum(counts) > total:
        choices = [i for i, value in enumerate(counts) if value > 1]
        if not choices:
            break
        index = min(choices, key=lambda i: raw[i] - counts[i])
        counts[index] -= 1
    return counts


def local_slot_minutes(windows: Sequence[str], daily_target: int) -> List[int]:
    parsed = [_parse_window(value) for value in windows]
    counts = _allocate_counts(parsed, daily_target)
    slots: List[int] = []
    for (start, end), count in zip(parsed, counts):
        duration = end - start
        for index in range(count):
            # Centered, evenly spaced points avoid publishing exactly at a
            # window boundary and remain deterministic across worker runs.
            slots.append(round(start + duration * (index + 1) / (count + 1)))
    return sorted(slots)


class AutoPublishScheduler:
    def __init__(self, repository, publish_flow: OpvPublishFlow, *, clock=None):
        self.repository = repository
        self.publish_flow = publish_flow
        self.clock = clock or utc_now

    def queue_task(self, task_id: str, *, operator: str) -> PublishSlot:
        task = self.repository.get_task(task_id)
        if task is None:
            raise PublishSchedulerError(f"task not found: {task_id}")
        if task.task_status == statuses.TASK_PUBLISH_PREPARING:
            record = self._record_for_task(task_id)
            if record and record.planned_publish_at:
                return self._slot_from_utc(task.account_id, record.planned_publish_at)
        if task.task_status != statuses.TASK_VIDEO_REVIEW:
            raise PublishSchedulerError(
                f"task {task_id} status {task.task_status!r} cannot be queued"
            )
        slot = self.next_slot(task.account_id)
        record = self.publish_flow.prepare_publish(
            task_id,
            operator=operator,
            planned_publish_at=slot.planned_publish_at_utc,
            publish_mode="auto_schedule",
        )
        metadata = dict(record.platform_metadata_json or {})
        metadata["schedule"] = self._schedule_metadata(slot)
        metadata["human_publish_gate"] = {
            "decision": "approved_for_auto_schedule",
            "operator": operator,
            "confirmed_at": self.clock().isoformat(timespec="seconds"),
        }
        self.repository.update_publish_result(
            record.publish_id,
            publish_status=record.publish_status,
            planned_publish_at=slot.planned_publish_at_utc,
            platform_metadata_json=metadata,
        )
        return slot

    def next_slot(
        self, account_id: str, *, after_utc: Optional[datetime] = None
    ) -> PublishSlot:
        account = self.repository.get_account_profile(account_id)
        if account is None:
            raise PublishSchedulerError(f"account not found: {account_id}")
        rules = account.operating_rules_json or {}
        windows = list(rules.get("daily_publish_window_local") or [])
        if not windows:
            raise PublishSchedulerError(
                f"account {account_id} has no daily_publish_window_local"
            )
        volume = rules.get("daily_volume") or {}
        daily_target = int(
            rules.get("daily_publish_target")
            or volume.get("min")
            or 1
        )
        minutes = local_slot_minutes(windows, daily_target)
        timezone_name = str(account.timezone or "UTC")
        account_zone = ZoneInfo(timezone_name)
        now_utc = after_utc or self.clock()
        minimum = now_utc + timedelta(minutes=MIN_SUBMIT_LEAD_MINUTES)
        minimum_local = minimum.replace(tzinfo=timezone.utc).astimezone(account_zone)
        occupied = {
            record.planned_publish_at
            for record in self.repository.list_publish_records_by_account(
                account_id, limit=500
            )
            if record.publish_status != statuses.PUBLISH_FAILED
            and record.planned_publish_at is not None
        }
        for day_offset in range(MAX_SCHEDULE_DAYS + 1):
            local_day = minimum_local.date() + timedelta(days=day_offset)
            for minute in minutes:
                candidate_local = datetime.combine(
                    local_day, time(minute // 60, minute % 60), tzinfo=account_zone
                )
                candidate_utc = candidate_local.astimezone(timezone.utc).replace(tzinfo=None)
                if candidate_utc < minimum or candidate_utc in occupied:
                    continue
                return self._slot_from_utc(account_id, candidate_utc)
        raise PublishSchedulerError(
            f"no free publish slot for account {account_id} in {MAX_SCHEDULE_DAYS} days"
        )

    def tick(
        self,
        *,
        limit: int = 100,
        task_ids: Optional[Iterable[str]] = None,
    ) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        now = self.clock()
        allowed_task_ids = (
            {str(task_id) for task_id in task_ids} if task_ids is not None else None
        )
        due = self.repository.list_publish_records_due(
            statuses.PUBLISH_READY,
            due_before=now + timedelta(minutes=BGM_SELECTION_MAX_LEAD_MINUTES),
            limit=limit,
        )
        if allowed_task_ids is not None:
            due = [record for record in due if record.task_id in allowed_task_ids]
        for record in due:
            try:
                events.append(self._submit_due(record, now))
            except Exception as exc:  # noqa: BLE001 - isolate scheduled tasks
                events.append(self._event(record, "blocked", str(exc)[:500]))
        confirmations = self.repository.list_publish_records_due(
            statuses.PUBLISH_SUBMITTED,
            due_before=now - timedelta(minutes=CONFIRM_GRACE_MINUTES),
            limit=limit,
        )
        if allowed_task_ids is not None:
            confirmations = [
                record
                for record in confirmations
                if record.task_id in allowed_task_ids
            ]
        for record in confirmations:
            try:
                state = self.publish_flow.confirm_result(record.task_id)
                events.append(self._event(record, state))
            except Exception as exc:  # noqa: BLE001 - no resubmit on requery failure
                events.append(self._event(record, "confirm_error", str(exc)[:500]))
        return events

    def _submit_due(self, record: PublishRecord, now: datetime) -> Dict[str, Any]:
        gate = (record.platform_metadata_json or {}).get("human_publish_gate") or {}
        if gate.get("decision") != "approved_for_auto_schedule" or not gate.get("operator"):
            raise PublishSchedulerError(
                "publish record has no explicit human auto-schedule approval"
            )
        planned = record.planned_publish_at
        if planned is None:
            raise PublishSchedulerError("publish record has no planned_publish_at")
        if planned <= now + timedelta(minutes=MIN_SUBMIT_LEAD_MINUTES):
            task = self.repository.get_task(record.task_id)
            if task is None:
                raise PublishSchedulerError(f"task not found: {record.task_id}")
            slot = self.next_slot(task.account_id, after_utc=now)
            self._reschedule(record, slot, reason="missed_submit_lead")
            return self._event(record, "rescheduled", slot.account_local_time.isoformat())

        refreshed = self.publish_flow.refresh_bgm_selection(
            record.task_id, planned_publish_at=planned
        )
        audio = (refreshed.platform_metadata_json or {}).get("audio") or {}
        selected = (audio.get("selected") or {}).get("music_id")
        if not selected:
            # Never auto-publish silently. A later tick retries; if it gets too
            # close, the slot is rolled forward by the lead-time guard above.
            return self._event(
                record,
                "bgm_waiting",
                str(audio.get("fallback_reason") or audio.get("status") or "")[:500],
            )
        self.publish_flow.arm_for_publish(record.task_id)
        task = self.repository.get_task(record.task_id)
        slot = self._slot_from_utc(task.account_id, planned)
        external_id = self.publish_flow.submit(
            record.task_id,
            publish_at=slot.neobund_wall_time,
            mark_ai=True,
        )
        return self._event(record, "submitted", str(external_id or "ambiguous"))

    def _reschedule(self, record: PublishRecord, slot: PublishSlot, *, reason: str) -> None:
        metadata = dict(record.platform_metadata_json or {})
        metadata["schedule"] = self._schedule_metadata(slot)
        metadata["reschedule_reason"] = reason
        metadata["planned_publish_at"] = slot.planned_publish_at_utc.isoformat(
            timespec="seconds"
        )
        audio = dict(metadata.get("audio") or {})
        audio["status"] = "awaiting_selection_window"
        metadata["audio"] = audio
        self.repository.update_publish_result(
            record.publish_id,
            publish_status=record.publish_status,
            planned_publish_at=slot.planned_publish_at_utc,
            platform_metadata_json=metadata,
        )

    def _slot_from_utc(self, account_id: str, value: datetime) -> PublishSlot:
        account = self.repository.get_account_profile(account_id)
        if account is None:
            raise PublishSchedulerError(f"account not found: {account_id}")
        aware = value.replace(tzinfo=timezone.utc)
        local = aware.astimezone(ZoneInfo(account.timezone or "UTC"))
        neobund = aware.astimezone(ZoneInfo(NEOBUND_TIMEZONE)).replace(tzinfo=None)
        return PublishSlot(
            account_id=account_id,
            account_timezone=account.timezone or "UTC",
            planned_publish_at_utc=value,
            account_local_time=local,
            neobund_wall_time=neobund,
        )

    @staticmethod
    def _schedule_metadata(slot: PublishSlot) -> Dict[str, Any]:
        return {
            "contract": "opv-auto-schedule-v1",
            "account_timezone": slot.account_timezone,
            "account_local_time": slot.account_local_time.isoformat(timespec="seconds"),
            "planned_publish_at_utc": slot.planned_publish_at_utc.isoformat(
                timespec="seconds"
            ),
            "neobund_timezone": NEOBUND_TIMEZONE,
            "neobund_wall_time": slot.neobund_wall_time.strftime("%Y-%m-%d %H:%M:%S"),
        }

    def _record_for_task(self, task_id: str) -> Optional[PublishRecord]:
        task = self.repository.get_task(task_id)
        if task is None:
            return None
        render = self.repository.get_render(task.selected_render_id or "")
        return self.repository.get_publish_record_by_render(render.render_id) if render else None

    def _event(self, record: PublishRecord, state: str, detail: str = "") -> Dict[str, Any]:
        task = self.repository.get_task(record.task_id)
        return {
            "publish_id": record.publish_id,
            "task_id": record.task_id,
            "feishu_record_id": task.feishu_record_id if task else None,
            "state": state,
            "detail": detail,
        }
