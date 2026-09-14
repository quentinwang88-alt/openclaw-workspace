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
    AssetSet,
    ContentShot,
    ContentTask,
    FeishuOutbox,
    MetricSnapshot,
    LookFeedback,
    ProductReferencePack,
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
        approval_sql = next(sql for sql, _ in connection.statements if "SET is_selected=1" in sql)
        self.assertIn("qa_status=%s", approval_sql)  # legacy V1 condition is unchanged

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


class FeishuWorkbenchQueryTest(unittest.TestCase):
    def test_latest_product_snapshot_returns_nested_product(self) -> None:
        connection = FakeConnection([("rows", [{
            "product_snapshot_json": '{"product":{"product_name":"coat","reference_images":["/tmp/a.jpg"]}}'
        }])])
        repo = RdsRepository(lambda: connection)
        snapshot = repo.get_latest_product_snapshot("P1")
        self.assertEqual(snapshot["product_name"], "coat")
        sql, params = connection.statements[0]
        self.assertIn("ORDER BY created_at DESC LIMIT 1", sql)
        self.assertEqual(params, ["P1"])

    def test_latest_product_snapshot_requires_references(self) -> None:
        connection = FakeConnection([("rows", [{
            "product_snapshot_json": '{"product":{"reference_images":[]}}'
        }])])
        self.assertIsNone(RdsRepository(lambda: connection).get_latest_product_snapshot("P1"))

    def test_tasks_are_scoped_by_source_prefix(self) -> None:
        first = sample_task(task_id="t1")
        first.source_type = "feishu_opv"
        first.source_record_id = "rec1:1:R:1"
        connection = FakeConnection([("rows", [task_row(first)])])
        rows = RdsRepository(lambda: connection).list_tasks_by_source_prefix(
            "feishu_opv", "rec1:"
        )
        self.assertEqual([item.task_id for item in rows], ["t1"])
        _, params = connection.statements[0]
        self.assertEqual(params, ["feishu_opv", "rec1:%"])


class ProductReferencePackRepositoryTest(unittest.TestCase):
    def test_default_upsert_clears_old_default_in_same_transaction(self) -> None:
        pack = ProductReferencePack(
            pack_id="prp1", product_id="P1", variant_key="blue",
            is_default=True, assets_json=[], asset_fingerprint="a" * 64,
        )
        connection = FakeConnection([("rowcount", 1), ("rowcount", 1)])
        RdsRepository(lambda: connection).upsert_product_reference_pack(pack)
        self.assertIn("SET is_default=0", connection.statements[0][0])
        self.assertTrue(connection.statements[1][0].startswith(
            "INSERT INTO opv_product_reference_pack"
        ))
        self.assertEqual(connection.commits, 1)

    def test_list_packs_orders_default_then_version(self) -> None:
        pack = ProductReferencePack(
            pack_id="prp1", product_id="P1", variant_key="blue",
            assets_json=[], asset_fingerprint="a" * 64,
        )
        row = pack.to_row()
        connection = FakeConnection([("rows", [row])])
        result = RdsRepository(lambda: connection).list_product_reference_packs("P1")
        self.assertEqual(result[0].pack_id, "prp1")
        self.assertIn("is_default DESC, pack_version DESC", connection.statements[0][0])


class PublishScheduleQueryTest(unittest.TestCase):
    def test_due_query_uses_status_time_index_contract(self) -> None:
        connection = FakeConnection([("rows", [])])
        due = datetime(2026, 9, 1, 4, 0, 0)
        rows = RdsRepository(lambda: connection).list_publish_records_due(
            statuses.PUBLISH_READY, due_before=due, limit=20
        )
        self.assertEqual(rows, [])
        sql, params = connection.statements[0]
        self.assertIn("planned_publish_at<=%s", sql)
        self.assertEqual(params, [statuses.PUBLISH_READY, due, 20])


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


class FakeAssetSetStore:
    """Stateful stand-in for ``opv_asset_set`` that models BOTH unique keys.

    The scripted :class:`FakeConnection` above cannot express a real constraint,
    and a double that simply raises proves nothing: the behaviour under test is
    that a version collision **never rewrites another id's row**.  So this store
    enforces the two keys the real table declares::

        PRIMARY KEY (asset_set_id)
        UNIQUE KEY uq_opv_asset_set_version (asset_set_key, asset_set_version)

    A conflicting INSERT raises ``IntegrityError(1062)`` and leaves the table
    untouched, exactly like MySQL does once the failed statement is rolled back
    on connection close.
    """

    def __init__(self, rows=()):
        self.rows = {row["asset_set_id"]: dict(row) for row in rows}
        self.insertions = []       # every attempted INSERT (including failures)
        self.commits = 0

    # -- DBAPI surface used by RdsRepository -------------------------------
    def cursor(self):
        return FakeAssetSetCursor(self)

    def commit(self):
        self.commits += 1

    def close(self):
        pass

    def rollback(self):  # pragma: no cover - the failed statement never landed
        pass

    # -- storage semantics -------------------------------------------------
    def insert(self, row):
        self.insertions.append(dict(row))
        if row["asset_set_id"] in self.rows:
            raise pymysql.err.IntegrityError(
                1062, f"Duplicate entry '{row['asset_set_id']}' for key 'PRIMARY'"
            )
        for existing in self.rows.values():
            if (existing["asset_set_key"] == row["asset_set_key"]
                    and int(existing["asset_set_version"]) == int(row["asset_set_version"])):
                raise pymysql.err.IntegrityError(
                    1062,
                    f"Duplicate entry '{row['asset_set_key']}-{row['asset_set_version']}' "
                    "for key 'uq_opv_asset_set_version'",
                )
        self.rows[row["asset_set_id"]] = dict(row)

    def snapshot(self):
        return {key: dict(value) for key, value in self.rows.items()}


class FakeAssetSetCursor:
    """Parses only the statements the asset-set methods issue."""

    def __init__(self, store):
        self._store = store
        self.rowcount = 0
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows

    def execute(self, sql, params=None):
        store = self._store
        flat = " ".join(sql.split())
        args = tuple(params or ())
        if flat.startswith("INSERT INTO opv_asset_set"):
            columns = flat.split("(", 1)[1].split(")", 1)[0].split(",")
            store.insert(dict(zip(columns, args)))
            self.rowcount = 1
            self._rows = []
            return
        if flat.startswith("SELECT * FROM opv_asset_set WHERE asset_set_id="):
            row = store.rows.get(args[0])
            self._rows = [dict(row)] if row else []
            return
        if flat.startswith("SELECT * FROM opv_asset_set WHERE asset_set_key="):
            key, version = args
            matched = [
                row for row in store.rows.values()
                if row["asset_set_key"] == key
                and int(row["asset_set_version"]) == int(version)
            ]
            self._rows = [dict(matched[0])] if matched else []
            return
        if "SELECT MAX(asset_set_version)" in flat:
            versions = [
                int(row["asset_set_version"]) for row in store.rows.values()
                if row["asset_set_key"] == args[0]
            ]
            self._rows = [{"max_version": max(versions) if versions else None}]
            return
        if flat.startswith("UPDATE opv_asset_set SET status="):
            status, asset_set_id = args
            row = store.rows.get(asset_set_id)
            if row is None:
                self.rowcount = 0
                return
            # MySQL reports *changed* rows, not matched rows.
            self.rowcount = 0 if row["status"] == status else 1
            row["status"] = status
            return
        raise AssertionError(f"unexpected SQL: {flat}")


def sample_asset_set(asset_set_id, *, key="VN_SCARF_CHOICE", version=1,
                     category="scarf", market="VN", status="enabled", seed="a"):
    return AssetSet(
        asset_set_id=asset_set_id,
        asset_set_key=key,
        asset_set_version=version,
        category_key=category,
        market=market,
        status=status,
        tags_json={"seed": seed},
        manifest_json={
            "assets": [{
                "asset_id": f"{seed}-look-a", "role": "look_a",
                "path": "/tmp/does-not-need-to-exist.png", "sha256": seed * 64,
            }]
        },
    )


class AssetSetRegistrationTest(unittest.TestCase):
    """``opv_asset_set`` writes must never rewrite another asset_set_id.

    Regression: the generic ``_upsert`` used ``ON DUPLICATE KEY UPDATE``, so an
    insert that collided on ``uq_opv_asset_set_version`` silently overwrote the
    historical row that held the slot (keeping *its* id) while the caller's
    content-addressed id was never written.  The images had already been paid
    for before anyone noticed.
    """

    def test_disabled_row_still_owns_its_slot_and_is_never_modified(self):
        """验收 1：同 key 的最高版本已 disabled ⇒ 新增用更高版本，旧行逐字段不变。"""
        old = sample_asset_set("ASSET_OLD_DISABLED", version=3, status="disabled",
                               category="womenswear", market="TH")
        store = FakeAssetSetStore([old.to_row()])
        before = store.snapshot()
        repo = RdsRepository(lambda: store)

        self.assertEqual(repo.next_asset_set_version("VN_SCARF_CHOICE"), 3)
        version = repo.next_asset_set_version("VN_SCARF_CHOICE") + 1
        landed = repo.upsert_asset_set(sample_asset_set("ASSET_NEW", version=version))

        self.assertEqual(landed.asset_set_version, 4)
        self.assertEqual(store.snapshot()["ASSET_OLD_DISABLED"], before["ASSET_OLD_DISABLED"])

    def test_another_markets_row_is_not_overwritten_on_a_version_guess(self):
        """验收 2：同 key 已有其他 category/market 的版本，新插入不得覆盖它。"""
        other = sample_asset_set("ASSET_TH_ROW", version=1, category="womenswear", market="TH")
        store = FakeAssetSetStore([other.to_row()])
        before = store.snapshot()
        repo = RdsRepository(lambda: store)

        # 调用方按「本市场 enabled」的旧口径只算出 v1——正是历史 bug 的输入。
        landed = repo.upsert_asset_set(
            sample_asset_set("ASSET_VN_ROW", version=1, category="scarf", market="VN")
        )

        self.assertEqual(landed.asset_set_id, "ASSET_VN_ROW")
        self.assertEqual(landed.asset_set_version, 2)
        self.assertEqual(store.snapshot()["ASSET_TH_ROW"], before["ASSET_TH_ROW"])

    def test_two_assets_racing_for_one_version_both_survive(self):
        """验收 3：两个素材争同 key/version ⇒ 各自存在，谁都不覆盖谁。"""
        store = FakeAssetSetStore()
        repo = RdsRepository(lambda: store)

        first = repo.upsert_asset_set(sample_asset_set("ASSET_R1", version=1, seed="a"))
        second = repo.upsert_asset_set(sample_asset_set("ASSET_R2", version=1, seed="b"))

        self.assertEqual((first.asset_set_id, first.asset_set_version), ("ASSET_R1", 1))
        self.assertEqual((second.asset_set_id, second.asset_set_version), ("ASSET_R2", 2))
        self.assertEqual(sorted(store.rows), ["ASSET_R1", "ASSET_R2"])
        self.assertEqual(store.rows["ASSET_R1"]["manifest_json"],
                         sample_asset_set("ASSET_R1", version=1, seed="a").to_row()["manifest_json"])

    def test_same_id_retry_reuses_the_stored_version(self):
        """验收 4：同素材重复登记返回同一实际 ID/version，不新增记录。"""
        store = FakeAssetSetStore()
        repo = RdsRepository(lambda: store)
        repo.upsert_asset_set(sample_asset_set("ASSET_SAME", version=1))
        # 并发下这次算出来的版本号更高，但 id 相同 ⇒ 必须复用库里的实际版本，
        # 不能因为「版本不同」就判成内容变化。
        again = repo.upsert_asset_set(sample_asset_set("ASSET_SAME", version=7))

        self.assertEqual(again.asset_set_version, 1)
        self.assertEqual(list(store.rows), ["ASSET_SAME"])

    def test_same_id_with_different_content_refuses_in_place_edit(self):
        store = FakeAssetSetStore()
        repo = RdsRepository(lambda: store)
        repo.upsert_asset_set(sample_asset_set("ASSET_SAME", version=1, seed="a"))
        with self.assertRaisesRegex(RepositoryError, "different content"):
            repo.upsert_asset_set(sample_asset_set("ASSET_SAME", version=1, seed="b"))
        self.assertEqual(store.rows["ASSET_SAME"]["manifest_json"],
                         sample_asset_set("ASSET_SAME", version=1, seed="a").to_row()["manifest_json"])

    def test_retries_are_bounded_and_change_nothing(self):
        """重试上限：槽位一直被抢时有限次重试后响亮失败，且不动任何行。"""

        class HostileSlotStore(FakeAssetSetStore):
            def insert(self, row):
                self.insertions.append(dict(row))
                raise pymysql.err.IntegrityError(
                    1062, "Duplicate entry for key 'uq_opv_asset_set_version'"
                )

        blocker = sample_asset_set("ASSET_BLOCKER", version=9, category="womenswear",
                                   market="TH")
        store = HostileSlotStore([blocker.to_row()])
        before = store.snapshot()
        repo = RdsRepository(lambda: store)

        with self.assertRaisesRegex(RepositoryError, "素材登记冲突，可重试登记"):
            repo.upsert_asset_set(sample_asset_set("ASSET_LOSER", version=1))

        self.assertEqual(len(store.insertions), RdsRepository.ASSET_SET_REGISTRATION_ATTEMPTS)
        # 每次重试都从真实唯一键口径重新取版本，且严格递增（不是原地重投）。
        seen = [row["asset_set_version"] for row in store.insertions]
        self.assertEqual(seen, sorted(seen))
        self.assertEqual(len(set(seen)), len(seen))
        self.assertEqual(store.snapshot(), before)

    def test_status_retirement_touches_only_the_status_column(self):
        store = FakeAssetSetStore([sample_asset_set("ASSET_A", version=1).to_row()])
        repo = RdsRepository(lambda: store)
        repo.update_asset_set_status("ASSET_A", "disabled")

        row = store.rows["ASSET_A"]
        self.assertEqual(row["status"], "disabled")
        self.assertEqual(row["asset_set_key"], "VN_SCARF_CHOICE")
        self.assertEqual(row["manifest_json"],
                         sample_asset_set("ASSET_A", version=1).to_row()["manifest_json"])
        with self.assertRaises(StaleStatusError):
            repo.update_asset_set_status("ASSET_MISSING", "disabled")


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
