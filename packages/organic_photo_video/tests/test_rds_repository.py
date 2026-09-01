#!/usr/bin/env python3
"""Repository behavior tests against a scripted fake connection (no real RDS).

The fake only implements the tiny DBAPI surface RdsRepository uses: cursor(),
execute(sql, params), fetchone/fetchall, rowcount, commit(), close(). Actions
are queued per connection: ("rows", [dict]) / ("rowcount", n) / ("raise", exc).
"""

from __future__ import annotations

from pathlib import Path
import sys
import unittest
from datetime import datetime

import pymysql

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from domain import statuses
from domain.models import (
    ContentShot,
    ContentTask,
    FeishuOutbox,
    MetricSnapshot,
    LookFeedback,
    generate_prefixed_id,
)
from repositories import rds_repository
from repositories.rds_repository import (
    RepositoryError,
    RdsRepository,
    StaleStatusError,
    connect_from_url,
    database_url,
)


class FakeCursor:
    def __init__(self, connection) -> None:
        self._connection = connection
        self.rowcount = 0
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self._connection.statements.append((sql, params))
        kind, payload = self._connection.next_action()
        if kind == "raise":
            raise payload
        if kind == "rowcount":
            self.rowcount = payload
            self._rows = []
        else:  # rows
            self._rows = payload

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows


class FakeConnection:
    def __init__(self, actions=()) -> None:
        self.actions = list(actions)
        self.statements = []
        self.commits = 0
        self.closed = False
        self.rollbacks = 0

    def next_action(self):
        if self.actions:
            return self.actions.pop(0)
        return ("rows", [])

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def close(self):
        self.closed = True

    def rollback(self):
        self.rollbacks += 1


def sample_task(task_id=None, idempotency_key="a" * 64) -> ContentTask:
    return ContentTask(
        task_id=task_id or generate_prefixed_id("opv_task"),
        idempotency_key=idempotency_key,
        account_id="OPV_TEST_1",
        product_id="PROD_1",
        target_country="TH",
        target_locale="th-TH",
        product_snapshot_json={"reference_images": ["oss://a.jpg"]},
    )


def task_row(task: ContentTask) -> dict:
    row = task.to_row()
    row["created_at"] = datetime(2026, 8, 30, 1, 2, 3)
    row["updated_at"] = datetime(2026, 8, 30, 1, 2, 3)
    return row


class CreateTaskIdempotentTest(unittest.TestCase):
    def test_first_insert_returns_created_true(self) -> None:
        task = sample_task()
        connection = FakeConnection(
            [
                ("rowcount", 1),          # INSERT
                ("rows", [task_row(task)]),  # SELECT after insert
            ]
        )
        repo = RdsRepository(lambda: connection)
        created_task, created = repo.create_task_idempotent(task)
        self.assertTrue(created)
        self.assertEqual(created_task.task_id, task.task_id)
        insert_sql, insert_params = connection.statements[0]
        self.assertTrue(insert_sql.startswith("INSERT INTO opv_content_task"))
        self.assertIn("%s", insert_sql)
        self.assertIn(task.idempotency_key, insert_params)
        self.assertEqual(connection.commits, 1)
        self.assertTrue(connection.closed)

    def test_duplicate_key_returns_existing_row_without_raising(self) -> None:
        existing = sample_task(task_id="opv_task_20260830_original123")
        connection = FakeConnection(
            [
                ("raise", pymysql.err.IntegrityError(1062, "Duplicate entry")),
                ("rows", [task_row(existing)]),  # SELECT by idempotency key
            ]
        )
        repo = RdsRepository(lambda: connection)
        duplicate = sample_task(task_id="opv_task_20260830_other456789")
        returned, created = repo.create_task_idempotent(duplicate)
        self.assertFalse(created)
        self.assertEqual(returned.task_id, existing.task_id)

    def test_duplicate_report_but_row_missing_raises(self) -> None:
        connection = FakeConnection(
            [
                ("raise", pymysql.err.IntegrityError(1062, "Duplicate entry")),
                ("rows", []),  # lookup finds nothing
            ]
        )
        repo = RdsRepository(lambda: connection)
        with self.assertRaises(RepositoryError):
            repo.create_task_idempotent(sample_task())


class AtomicApprovalTest(unittest.TestCase):
    def _shots(self):
        return [
            ContentShot(
                shot_id=f"s{i}", task_id="t1", slot_index=i,
                slot_role="hero", duration_ms=2000,
                shot_status=statuses.SHOT_GENERATED, qa_status=statuses.QC_PASSED,
            )
            for i in range(1, 6)
        ]

    def _feedback(self):
        return LookFeedback(
            feedback_id="fb1", task_id="t1", account_id="a1",
            product_id="p1", feedback_type="human_review",
            decision="approve", reviewer="owner",
        )

    def test_group_approval_commits_all_rows_once(self) -> None:
        connection = FakeConnection([
            ("rows", [{"task_status": statuses.TASK_IMAGE_REVIEW,
                       "content_package_id": "pkg1"}]),
            ("rowcount", 5),
            ("rowcount", 5),
            ("rowcount", 1),
            ("rowcount", 1),
            ("rowcount", 1),
        ])
        repo = RdsRepository(lambda: connection)
        repo.commit_group_approval(
            task_id="t1", shots=self._shots(), feedback=self._feedback(),
            group_qa_json={"stage_d": {"decision": "group_approved"}},
            package_fields={
                "selected_image_ids_json": [f"s{i}" for i in range(1, 6)],
                "qa_summary_json": {"ok": True},
            },
        )
        self.assertEqual(connection.commits, 1)
        self.assertEqual(connection.rollbacks, 0)
        self.assertEqual(len(connection.statements), 6)

    def test_group_approval_rolls_back_when_package_is_stale(self) -> None:
        connection = FakeConnection([
            ("rows", [{"task_status": statuses.TASK_IMAGE_REVIEW,
                       "content_package_id": "pkg1"}]),
            ("rowcount", 5),
            ("rowcount", 5),
            ("rowcount", 1),
            ("rowcount", 0),
        ])
        repo = RdsRepository(lambda: connection)
        with self.assertRaises(StaleStatusError):
            repo.commit_group_approval(
                task_id="t1", shots=self._shots(), feedback=self._feedback(),
                group_qa_json={}, package_fields={"qa_summary_json": {}},
            )
        self.assertEqual(connection.commits, 0)
        self.assertEqual(connection.rollbacks, 1)


class GuardedTransitionTest(unittest.TestCase):
    def row_for(self, status: str) -> dict:
        task = sample_task()
        row = task_row(task)
        row["task_status"] = status
        return row

    def test_legal_transition_updates_with_expected_status_guard(self) -> None:
        after = self.row_for(statuses.TASK_PLANNED)
        connection = FakeConnection(
            [
                ("rowcount", 1),   # UPDATE ... WHERE task_status='draft'
                ("rows", [after]),  # SELECT refreshed row
            ]
        )
        repo = RdsRepository(lambda: connection)
        updated = repo.transition_task("t1", statuses.TASK_DRAFT, statuses.TASK_PLANNED)
        self.assertEqual(updated.task_status, statuses.TASK_PLANNED)
        update_sql, update_params = connection.statements[0]
        self.assertIn("WHERE task_id=%s AND task_status=%s", update_sql)
        self.assertEqual(update_params[-2:], ["t1", statuses.TASK_DRAFT])
        self.assertIn("started_at=UTC_TIMESTAMP(6)", update_sql)

    def test_illegal_transition_never_touches_database(self) -> None:
        connection = FakeConnection()
        repo = RdsRepository(lambda: connection)
        with self.assertRaises(Exception):
            repo.transition_task("t1", statuses.TASK_DRAFT, statuses.TASK_PUBLISHED)
        self.assertEqual(connection.statements, [])

    def test_zero_rowcount_raises_stale_with_actual_status(self) -> None:
        current = self.row_for(statuses.TASK_PUBLISHING)
        connection = FakeConnection(
            [
                ("rowcount", 0),
                ("rows", [current]),
            ]
        )
        repo = RdsRepository(lambda: connection)
        with self.assertRaises(StaleStatusError) as ctx:
            repo.transition_task("t1", statuses.TASK_READY_TO_PUBLISH, statuses.TASK_PUBLISHING)
        self.assertIn("publishing", str(ctx.exception))

    def test_failure_transition_persists_failure_fields_and_retry(self) -> None:
        after = self.row_for(statuses.TASK_FAILED)
        connection = FakeConnection([("rowcount", 1), ("rows", [after])])
        repo = RdsRepository(lambda: connection)
        repo.transition_task(
            "t1",
            statuses.TASK_IMAGE_GENERATING,
            statuses.TASK_FAILED,
            failure_code="image_provider_timeout",
            failure_detail="gpt-image-2 timeout",
            increment_retry=True,
        )
        update_sql, update_params = connection.statements[0]
        self.assertIn("failure_code=%s", update_sql)
        self.assertIn("retry_count=retry_count+1", update_sql)
        self.assertIn("image_provider_timeout", update_params)


class UpsertAndOutboxTest(unittest.TestCase):
    def test_metric_snapshot_upserts_on_publish_window(self) -> None:
        snapshot = MetricSnapshot(
            publish_id="opv_pub_1",
            captured_at=datetime(2026, 8, 30, 12, 0, 0),
            captured_after_hours=24,
            view_count=100,
        )
        connection = FakeConnection([("rowcount", 2)])
        repo = RdsRepository(lambda: connection)
        repo.upsert_metric_snapshot(snapshot)
        sql, params = connection.statements[0]
        self.assertTrue(sql.startswith("INSERT INTO opv_metric_snapshot"))
        self.assertIn("ON DUPLICATE KEY UPDATE", sql)
        self.assertNotIn("metric_id", sql.split("ON DUPLICATE")[0].split("(")[1])
        row = snapshot.to_row()
        row.pop("metric_id", None)
        # insert params for every column + update params for all but publish_id
        self.assertEqual(len(params), 2 * len(row) - 1)

    def test_enqueue_outbox_is_idempotent_by_aggregate_key(self) -> None:
        entry = FeishuOutbox(
            outbox_id="opv_outbox_1",
            aggregate_type="content_task",
            aggregate_id="opv_task_1",
            operation="upsert_task_row",
            payload_json={"task_id": "opv_task_1"},
        )
        existing_row = entry.to_row()
        connection = FakeConnection([("rows", [existing_row])])
        repo = RdsRepository(lambda: connection)
        returned, created = repo.enqueue_outbox(entry)
        self.assertFalse(created)
        self.assertEqual(len(connection.statements), 1)  # lookup only, no INSERT

        connection2 = FakeConnection(
            [
                ("rows", []),               # lookup: miss
                ("rowcount", 1),            # INSERT
                ("rows", [existing_row]),   # fetch after insert
            ]
        )
        repo2 = RdsRepository(lambda: connection2)
        returned2, created2 = repo2.enqueue_outbox(entry)
        self.assertTrue(created2)
        self.assertEqual(returned2.outbox_id, entry.outbox_id)

    def test_outbox_status_transition_is_guarded(self) -> None:
        connection = FakeConnection([("rowcount", 1)])
        repo = RdsRepository(lambda: connection)
        repo.update_outbox_status("o1", statuses.OUTBOX_HOLDING, statuses.OUTBOX_PENDING)
        with self.assertRaises(Exception):
            repo.update_outbox_status("o1", statuses.OUTBOX_HOLDING, statuses.OUTBOX_SENT)


class ConnectionConfigTest(unittest.TestCase):
    def test_missing_url_raises_without_leaking_secrets(self) -> None:
        with self.assertRaises(RepositoryError):
            database_url(env={})

    def test_bad_scheme_is_rejected(self) -> None:
        with self.assertRaises(RepositoryError):
            connect_from_url("postgres://user:secret@host/db")

    def test_from_env_uses_override_mapping(self) -> None:
        repo = RdsRepository.from_env(
            {"ORGANIC_PHOTO_VIDEO_DATABASE_URL": "mysql://example_user:example_password@db.example.internal/likeu_ai_database"}
        )
        self.assertIsNotNone(repo)


if __name__ == "__main__":
    unittest.main()
