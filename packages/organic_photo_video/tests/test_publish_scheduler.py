#!/usr/bin/env python3

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys
import unittest

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from domain.models import AccountProfile, ContentTask, PublishRecord  # noqa: E402
from services.publish_scheduler import (  # noqa: E402
    AutoPublishScheduler, local_slot_minutes,
)


def account():
    return AccountProfile(
        account_id="A1", account_code="a1", account_name="A1",
        target_country="TH", default_locale="th-TH", timezone="Asia/Bangkok",
        operating_rules_json={
            "daily_publish_window_local": ["11:30-13:00", "19:00-22:00"],
            "daily_volume": {"min": 5, "max": 10},
        },
    )


class FakeRepo:
    def __init__(self):
        self.account = account()
        self.records = []
        self.task = ContentTask(
            task_id="t1", idempotency_key="a" * 64, account_id="A1",
            product_id="p1", target_country="TH", target_locale="th-TH",
            task_status="publish_preparing", selected_render_id="r1",
        )
        self.updates = []

    def get_account_profile(self, account_id):
        return self.account if account_id == "A1" else None

    def list_publish_records_by_account(self, account_id, limit=500):
        return list(self.records)

    def get_task(self, task_id):
        return self.task if task_id == "t1" else None

    def update_publish_result(self, publish_id, **kwargs):
        self.updates.append((publish_id, kwargs))

    def list_publish_records_due(self, publish_status, due_before, limit=100):
        return [
            row for row in self.records
            if row.publish_status == publish_status
            and row.planned_publish_at is not None
            and row.planned_publish_at <= due_before
        ][:limit]


class FakeFlow:
    def __init__(self, record):
        self.record = record
        self.armed = []
        self.submitted = []

    def refresh_bgm_selection(self, task_id, planned_publish_at=None):
        return self.record

    def arm_for_publish(self, task_id):
        self.armed.append(task_id)

    def submit(self, task_id, **kwargs):
        self.submitted.append((task_id, kwargs))
        return "NB1"


class SlotPlanningTest(unittest.TestCase):
    def test_proportional_slots_are_centered(self):
        self.assertEqual(
            local_slot_minutes(["11:30-13:00", "19:00-22:00"], 5),
            [720, 750, 1185, 1230, 1275],
        )

    def test_next_slot_uses_account_timezone(self):
        repo = FakeRepo()
        now = datetime(2026, 9, 1, 1, 0, 0)  # 08:00 Bangkok
        scheduler = AutoPublishScheduler(repo, object(), clock=lambda: now)
        slot = scheduler.next_slot("A1")
        self.assertEqual(slot.account_local_time.strftime("%Y-%m-%d %H:%M"), "2026-09-01 12:00")
        self.assertEqual(slot.planned_publish_at_utc, datetime(2026, 9, 1, 5, 0, 0))
        self.assertEqual(slot.neobund_wall_time, datetime(2026, 9, 1, 13, 0, 0))

    def test_existing_slot_moves_to_next_slot(self):
        repo = FakeRepo()
        repo.records.append(PublishRecord(
            publish_id="pub0", task_id="old", render_id="r0", account_id="A1",
            planned_publish_at=datetime(2026, 9, 1, 5, 0, 0),
        ))
        scheduler = AutoPublishScheduler(
            repo, object(), clock=lambda: datetime(2026, 9, 1, 1, 0, 0)
        )
        self.assertEqual(
            scheduler.next_slot("A1").planned_publish_at_utc,
            datetime(2026, 9, 1, 5, 30, 0),
        )


class DueSubmitTest(unittest.TestCase):
    def test_due_record_uses_neobund_beijing_wall_time(self):
        repo = FakeRepo()
        record = PublishRecord(
            publish_id="pub1", task_id="t1", render_id="r1", account_id="A1",
            planned_publish_at=datetime(2026, 9, 1, 5, 0, 0),
            platform_metadata_json={
                "audio": {"selected": {"music_id": "m1", "title": "Hit"}},
                "human_publish_gate": {
                    "decision": "approved_for_auto_schedule", "operator": "tester",
                },
            },
        )
        flow = FakeFlow(record)
        scheduler = AutoPublishScheduler(
            repo, flow, clock=lambda: datetime(2026, 9, 1, 4, 0, 0)
        )
        event = scheduler._submit_due(record, datetime(2026, 9, 1, 4, 0, 0))
        self.assertEqual(event["state"], "submitted")
        self.assertEqual(flow.armed, ["t1"])
        self.assertEqual(
            flow.submitted[0][1]["publish_at"], datetime(2026, 9, 1, 13, 0, 0)
        )
        self.assertTrue(flow.submitted[0][1]["mark_ai"])

    def test_tick_task_filter_does_not_submit_other_due_tasks(self):
        repo = FakeRepo()
        allowed = PublishRecord(
            publish_id="pub1", task_id="t1", render_id="r1", account_id="A1",
            planned_publish_at=datetime(2026, 9, 1, 5, 0, 0),
            platform_metadata_json={
                "audio": {"selected": {"music_id": "m1", "title": "Hit"}},
                "human_publish_gate": {
                    "decision": "approved_for_auto_schedule", "operator": "tester",
                },
            },
        )
        blocked = PublishRecord(
            publish_id="pub2", task_id="t2", render_id="r2", account_id="A1",
            planned_publish_at=datetime(2026, 9, 1, 5, 0, 0),
        )
        repo.records = [allowed, blocked]
        flow = FakeFlow(allowed)
        scheduler = AutoPublishScheduler(
            repo, flow, clock=lambda: datetime(2026, 9, 1, 4, 0, 0)
        )
        events = scheduler.tick(task_ids=["t1"])
        self.assertEqual([event["task_id"] for event in events], ["t1"])
        self.assertEqual([call[0] for call in flow.submitted], ["t1"])


if __name__ == "__main__":
    unittest.main()
