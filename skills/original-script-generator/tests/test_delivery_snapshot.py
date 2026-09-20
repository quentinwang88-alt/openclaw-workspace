"""A2: the export-time delivery snapshot -- build it, store it, and what blocks.

F4 (review 2026-09-20): the exporter re-renders and re-audits the prompt, writes
the fresh verdict into the 飞书 columns, and left ``result_json`` alone.  The
consumer therefore read the *generation-time* verdict forever, so a re-export
after a renderer fix (``v1 FAIL`` -> ``v2 PASS`` is the real case) could never
terminate.

The snapshot is the fix, and it has three properties that are worth pinning
separately: it is built from the *same* checked projection that produces the
table fields (so the two cannot drift), it is written **before** the sheet is
touched (so a hand-off the frozen source cannot back up never happens), and it
merges into ``result_json`` without disturbing anything else in the row.

These tests use a throwaway database.  Nothing here reads or writes production.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock
from unittest.mock import patch

import core.original_batch_storage as storage_module
import core.production_script_feishu as production_feishu
from core.bitable import TaskRecord
from core.original_batch_storage import BatchStorage
from core.production_script_feishu import (
    SNAPSHOT_ITEM_ID_MISSING,
    SNAPSHOT_STORAGE_UNAVAILABLE,
    export_ready_batch,
)
from core.production_script_renderer import (
    DELIVERY_SNAPSHOT_FIELD,
    DELIVERY_SNAPSHOT_SCHEMA_VERSION,
    build_delivery_snapshot,
)

PROFILE_HASH = "d405e0c3933770bf"
BATCH_ITEM_ID = "OCI_ITEM_1"
INTERNAL_ID = "SCRIPT_8BCEF97D70205D7F6540"
PUBLIC_ID = "SCSCRIPT_40284D07CBFADD28F3F2"
PUBLISHED_PROMPT = (
    "【拍摄片段01｜0-4s｜HOOK】\n画面事件：手持项链贴近镜头。\n"
    "【拍摄片段02｜4-8s｜PROOF】\n画面事件：静物平铺展示细节。"
)
AUDIT = {
    "version": "mixed-execution-audit-v1",
    "status": "PASS",
    "renderer_version": "production-script-renderer-v5-speakable-semantic-mainline",
    "prompt_hash": "0123456789abcdef0123",
    "issues": [],
}


def mixed_contract(*, feature_profile: str = "NECKLACE_MIXED_V1", profile_hash: str = PROFILE_HASH):
    """A frozen mixed contract.  ``feature_profile=""`` models another category."""

    block = {
        "template_id": "NMX_01_WEAR_DETAIL_STATIC",
        "execution_profile": "ACCESSORY_MIXED_TEMPLATE_V1",
        "feature_profile": feature_profile,
        "feature_version": 1,
        "template_version": 1,
    }
    if feature_profile:
        block["necklace_contract"] = {
            "subtype": "SINGLE_LAYER_SINGLE_PENDANT",
            "interaction_mode": "NONE",
            "profile_config_hash": profile_hash,
        }
    return block


def frozen_result(*, contract=None, script_id: str = "S1") -> dict:
    return {
        "status": "SUCCESS",
        "script": {
            "complete_script_id": script_id,
            "video_generation_brief": {
                "category_execution_extension": {
                    "mixed_template_contract": (
                        contract if contract is not None else mixed_contract()
                    )
                }
            },
        },
    }


def make_item(
    *,
    result: dict = None,
    batch_item_id: str = BATCH_ITEM_ID,
    script_id: str = INTERNAL_ID,
    status: str = "SCRIPT_READY",
    frozen: str = "",
):
    return type(
        "Item",
        (),
        {
            "batch_item_id": batch_item_id,
            "script_id": script_id,
            "status": status,
            "result_json": json.dumps(
                result if result is not None else frozen_result(),
                ensure_ascii=False,
            ),
            "frozen_direction_package_json": frozen,
        },
    )()


def projection(*, prompt: str = PUBLISHED_PROMPT, validation: dict = None) -> dict:
    return {
        "script_id": PUBLIC_ID,
        "product_code": "1731275706716555867",
        "batch_id": "BATCH_1",
        "batch_item_id": BATCH_ITEM_ID,
        "item_index": 1,
        "script_title": "项链混合展示",
        "duration_seconds": 15,
        "processing_status": "待审核",
        "video_prompt": prompt,
        "render_validation": dict(validation if validation is not None else AUDIT),
    }


class DeliverySnapshotShapeTest(unittest.TestCase):
    """``build_delivery_snapshot``: one profile, and no second render."""

    def test_an_item_without_the_necklace_v1_contract_gets_no_snapshot(self):
        # ``{}`` and not ``None``: the caller's only branch is truthiness, and
        # every other category has to take exactly the path it took before.
        item = make_item(result=frozen_result(contract=mixed_contract(feature_profile="")))
        self.assertEqual(build_delivery_snapshot(projection=projection(), item=item), {})

    def test_the_snapshot_carries_the_published_text_and_its_audit(self):
        item = make_item()
        snapshot = build_delivery_snapshot(projection=projection(), item=item)
        self.assertEqual(snapshot["schema_version"], DELIVERY_SNAPSHOT_SCHEMA_VERSION)
        self.assertEqual(snapshot["feature_profile"], "NECKLACE_MIXED_V1")
        self.assertEqual(snapshot["prompt_text"], PUBLISHED_PROMPT)
        self.assertEqual(snapshot["render_validation"], AUDIT)
        self.assertEqual(snapshot["frozen_contract_hash"], PROFILE_HASH)
        self.assertTrue(snapshot["created_at"])

    def test_the_two_script_ids_are_recorded_separately(self):
        # The workbench shows the public id; the frozen table's own column holds
        # the internal one.  A consumer has to be able to reach both.
        snapshot = build_delivery_snapshot(projection=projection(), item=make_item())
        self.assertEqual(snapshot["complete_script_id"], PUBLIC_ID)
        self.assertEqual(snapshot["internal_script_id"], INTERNAL_ID)
        self.assertEqual(snapshot["batch_item_id"], BATCH_ITEM_ID)

    def test_the_snapshot_is_the_same_object_the_sheet_was_built_from(self):
        # One render, one audit.  If the snapshot re-rendered, the text in the
        # table and the text the snapshot describes could disagree -- which is
        # the failure this whole round exists to remove.
        projection_payload = projection()
        snapshot = build_delivery_snapshot(projection=projection_payload, item=make_item())
        self.assertIs(snapshot["render_validation"], projection_payload["render_validation"])
        self.assertEqual(snapshot["prompt_text"], projection_payload["video_prompt"])

    def test_the_source_hash_ignores_the_projection_and_the_timestamp(self):
        item = make_item()
        first = build_delivery_snapshot(
            projection=projection(), item=item, created_at="2026-09-20T09:00:00+00:00"
        )
        second = build_delivery_snapshot(
            projection=projection(), item=item, created_at="2026-09-21T11:30:00+00:00"
        )
        self.assertEqual(first["source_content_hash"], second["source_content_hash"])
        # ...and it is not a digest of the published text either: a re-render of
        # the same frozen source must still count as the same source.
        other_text = build_delivery_snapshot(
            projection=projection(prompt="【拍摄片段01｜0-4s｜HOOK】\n画面事件：另一版。"),
            item=item,
        )
        self.assertEqual(
            other_text["source_content_hash"], first["source_content_hash"]
        )

    def test_the_source_hash_follows_the_frozen_source(self):
        first = build_delivery_snapshot(projection=projection(), item=make_item())
        moved = make_item(result=frozen_result(script_id="S2"))
        second = build_delivery_snapshot(projection=projection(), item=moved)
        self.assertNotEqual(
            first["source_content_hash"], second["source_content_hash"]
        )

    def test_a_missing_batch_item_id_is_recorded_verbatim(self):
        # Not this layer's business to refuse; the *writer* refuses, because
        # only it can report that the row was left alone.
        item = make_item(batch_item_id="")
        snapshot = build_delivery_snapshot(projection=projection(), item=item)
        self.assertEqual(snapshot["batch_item_id"], "")
        self.assertEqual(snapshot["prompt_text"], PUBLISHED_PROMPT)


class IsolatedStorageTest(unittest.TestCase):
    """A throwaway ``original_content_item`` table with one frozen row."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.db = self.root / "gen.sqlite3"
        self.storage = BatchStorage(db_path=self.db)
        self.storage.ensure_schema()
        self._env = mock.patch.dict(
            os.environ,
            {"ORIGINAL_SCRIPT_GENERATOR_DB_PATH": str(self.db)},
            clear=False,
        )
        self._env.start()

    def tearDown(self):
        self._env.stop()
        self._tmp.cleanup()

    def seed(self, *, batch_item_id: str = BATCH_ITEM_ID, result: dict = None) -> None:
        """One ready row.  NOT NULL columns come from the real schema, not a
        hand-copied list -- ``ensure_schema`` and the production ALTER TABLE
        migrations do not agree on column order, so a literal INSERT rots."""

        connection = sqlite3.connect(self.db)
        connection.row_factory = sqlite3.Row
        info = list(connection.execute("PRAGMA table_info(original_content_item)"))
        values = {}
        for column in info:
            if column["notnull"] and column["dflt_value"] is None:
                values[column["name"]] = (
                    1 if str(column["type"] or "").upper().startswith("INT") else "X"
                )
        values.update(
            {
                "batch_item_id": batch_item_id,
                "batch_id": "BATCH_1",
                "item_index": 1,
                "item_role": "PRIMARY",
                "product_code": "1731275706716555867",
                "status": "SCRIPT_READY",
                "script_id": INTERNAL_ID,
                "result_json": json.dumps(
                    result if result is not None else frozen_result(),
                    ensure_ascii=False,
                ),
                "stage_checkpoint_json": json.dumps({"stage": "SCRIPT"}, ensure_ascii=False),
                "created_at": "2026-09-19 09:00:00",
                "updated_at": "2026-09-19 10:00:00",
            }
        )
        columns = list(values)
        connection.execute(
            f"INSERT INTO original_content_item ({', '.join(columns)}) "
            f"VALUES ({', '.join('?' * len(columns))})",
            [values[column] for column in columns],
        )
        connection.commit()
        connection.close()

    def read_row(self, batch_item_id: str = BATCH_ITEM_ID) -> dict:
        connection = sqlite3.connect(self.db)
        connection.row_factory = sqlite3.Row
        row = connection.execute(
            "SELECT * FROM original_content_item WHERE batch_item_id=?",
            (batch_item_id,),
        ).fetchone()
        connection.close()
        return dict(row) if row is not None else {}


class MergeItemResultSectionTest(IsolatedStorageTest):
    """The write half: one key, one column, and never a blind overwrite."""

    def test_the_merge_keeps_every_other_result_key_and_sibling_column(self):
        result = frozen_result()
        result["model_lineage"] = {"route": "openai-codex"}
        self.seed(result=result)
        outcome = self.storage.merge_item_result_section(
            BATCH_ITEM_ID, DELIVERY_SNAPSHOT_FIELD, {"schema_version": "v1"}
        )
        self.assertTrue(outcome["ok"], outcome)
        self.assertIsNone(outcome["previous"])
        stored = json.loads(self.read_row()["result_json"])
        self.assertEqual(stored["model_lineage"], {"route": "openai-codex"})
        self.assertEqual(stored["script"]["complete_script_id"], "S1")
        self.assertEqual(stored[DELIVERY_SNAPSHOT_FIELD], {"schema_version": "v1"})
        # The sibling column the execution path owns is untouched...
        row = self.read_row()
        self.assertEqual(json.loads(row["stage_checkpoint_json"]), {"stage": "SCRIPT"})
        # ...and so is the row's own status.
        self.assertEqual(row["status"], "SCRIPT_READY")

    def test_a_missing_item_is_reported_and_nothing_is_created(self):
        outcome = self.storage.merge_item_result_section(
            "OCI_ABSENT", DELIVERY_SNAPSHOT_FIELD, {"schema_version": "v1"}
        )
        self.assertEqual(outcome["reason"], "ITEM_NOT_FOUND")
        self.assertFalse(outcome["ok"])
        connection = sqlite3.connect(self.db)
        count = connection.execute(
            "SELECT COUNT(*) FROM original_content_item"
        ).fetchone()[0]
        connection.close()
        self.assertEqual(count, 0)

    def test_the_callers_precondition_can_refuse_the_write(self):
        # This is the guard the exporter uses: "the stored script is still the
        # one I rendered".  Failing it must leave the row exactly as it was.
        self.seed()
        before = self.read_row()["result_json"]
        outcome = self.storage.merge_item_result_section(
            BATCH_ITEM_ID,
            DELIVERY_SNAPSHOT_FIELD,
            {"schema_version": "v1"},
            expect=lambda payload: False,
            expect_reason="PRECONDITION_FAILED:stale",
        )
        self.assertFalse(outcome["ok"])
        self.assertEqual(outcome["reason"], "PRECONDITION_FAILED:stale")
        self.assertEqual(self.read_row()["result_json"], before)

    def test_a_concurrent_writer_turns_the_merge_into_a_refusal(self):
        # The compare-and-swap is on the previous text, not on a version column.
        # ``_now`` runs after the read and before the write, so a side effect
        # there *is* another writer arriving in between -- which is the only
        # honest way to exercise this path without a second thread.
        self.seed()
        winner = json.dumps({"script": {"complete_script_id": "OTHER"}})

        def other_writer_wins() -> str:
            connection = sqlite3.connect(self.db)
            connection.execute(
                "UPDATE original_content_item SET result_json=? WHERE batch_item_id=?",
                (winner, BATCH_ITEM_ID),
            )
            connection.commit()
            connection.close()
            return "2026-09-20 09:00:00"

        with patch.object(storage_module, "_now", other_writer_wins):
            outcome = self.storage.merge_item_result_section(
                BATCH_ITEM_ID, DELIVERY_SNAPSHOT_FIELD, {"schema_version": "v1"}
            )
        self.assertFalse(outcome["ok"])
        self.assertEqual(outcome["reason"], "RESULT_CHANGED_CONCURRENTLY")
        # The other writer's text survived: we did not clobber it.
        self.assertEqual(self.read_row()["result_json"], winner)

    def test_the_merge_is_idempotent(self):
        self.seed()
        first = self.storage.merge_item_result_section(
            BATCH_ITEM_ID, DELIVERY_SNAPSHOT_FIELD, {"schema_version": "v1"}
        )
        second = self.storage.merge_item_result_section(
            BATCH_ITEM_ID, DELIVERY_SNAPSHOT_FIELD, {"schema_version": "v1"}
        )
        self.assertTrue(second["ok"], second)
        # The second write reports what it replaced, which is the first write.
        self.assertEqual(second["previous"], {"schema_version": "v1"})
        self.assertEqual(first["written"], second["written"])


class ExportPersistsTheSnapshotBeforeTheSheetTest(IsolatedStorageTest):
    """Ordering, and what a refusal costs."""

    class FakeClient:
        """Records only what it was actually asked to write.

        Appending unconditionally would make every blocked case look like a
        hand-off: ``batch_create_records`` is called even with an empty list.
        """

        def __init__(self, journal: list):
            self.journal = journal
            self.updated = []
            self.created = []

        def list_records(self, *, page_size):
            return []

        def update_record_fields(self, record_id, fields):
            self.journal.append("sheet")
            self.updated.append((record_id, fields))

        def batch_create_records(self, records):
            if records:
                self.journal.append("sheet")
            self.created.extend(records)
            return ["new-record"] * len(records)

    def export(self, *, item, storage, journal: list = None):
        journal = [] if journal is None else journal
        client = self.FakeClient(journal)
        with patch.object(
            production_feishu, "build_production_projection", return_value=projection()
        ):
            result = export_ready_batch(
                batch=object(),
                items=[item],
                target_client=client,
                storage=storage,
            )
        return result, client

    def assert_no_sheet_write(self, client) -> None:
        self.assertEqual(client.updated, [], "不该更新飞书行")
        self.assertEqual(client.created, [], "不该新建飞书行")

    def test_the_snapshot_lands_in_the_row_before_the_sheet_is_written(self):
        journal: list = []
        self.seed()
        real = self.storage.merge_item_result_section

        def recording_merge(*args, **kwargs):
            journal.append("snapshot")
            return real(*args, **kwargs)

        with patch.object(self.storage, "merge_item_result_section", recording_merge):
            result, client = self.export(
                item=make_item(), storage=self.storage, journal=journal
            )
        self.assertEqual(journal, ["snapshot", "sheet"])
        self.assertEqual(result["snapshot_written"], 1)
        self.assertEqual(result["snapshot_blocked"], 0)
        self.assertEqual(len(client.created), 1)
        stored = json.loads(self.read_row()["result_json"])
        self.assertEqual(stored[DELIVERY_SNAPSHOT_FIELD]["prompt_text"], PUBLISHED_PROMPT)

    def test_without_storage_the_row_is_not_handed_to_the_sheet(self):
        journal: list = []
        self.seed()
        result, client = self.export(item=make_item(), storage=None, journal=journal)
        self.assertEqual(result["snapshot_written"], 0)
        self.assertEqual(result["snapshot_blocked"], 1)
        self.assertEqual(result["created"], 0)
        self.assertEqual(result["updated"], 0)
        self.assertEqual(journal, [], "没有快照就不该写飞书")
        self.assert_no_sheet_write(client)
        self.assertIn(
            SNAPSHOT_STORAGE_UNAVAILABLE,
            result["snapshot_errors"][BATCH_ITEM_ID],
        )
        # The frozen row is exactly as it was.
        self.assertNotIn(DELIVERY_SNAPSHOT_FIELD, self.read_row()["result_json"])

    def test_an_item_without_a_batch_item_id_is_blocked_and_reported(self):
        journal: list = []
        result, client = self.export(
            item=make_item(batch_item_id=""), storage=self.storage, journal=journal
        )
        self.assertEqual(result["snapshot_blocked"], 1)
        self.assertEqual(journal, [])
        self.assert_no_sheet_write(client)
        self.assertIn(
            SNAPSHOT_ITEM_ID_MISSING, list(result["snapshot_errors"].values())[0]
        )

    def test_a_failed_snapshot_write_keeps_the_frozen_row_untouched(self):
        journal: list = []
        self.seed()
        before = self.read_row()["result_json"]
        with patch.object(
            self.storage,
            "merge_item_result_section",
            return_value={
                "ok": False,
                "reason": "ITEM_NOT_FOUND",
                "previous": None,
                "written": None,
            },
        ):
            result, client = self.export(
                item=make_item(), storage=self.storage, journal=journal
            )
        self.assertEqual(result["snapshot_blocked"], 1)
        self.assertEqual(result["created"], 0)
        self.assertEqual(result["updated"], 0)
        self.assertEqual(journal, [])
        self.assert_no_sheet_write(client)
        self.assertEqual(self.read_row()["result_json"], before)

    def test_a_non_necklace_row_never_touches_the_frozen_payload(self):
        journal: list = []
        other_category = frozen_result(contract=mixed_contract(feature_profile=""))
        self.seed(result=other_category)
        before = self.read_row()["result_json"]
        result, client = self.export(
            item=make_item(result=other_category), storage=self.storage, journal=journal
        )
        # No snapshot is built, so nothing is blocked and nothing is written.
        self.assertEqual(result["snapshot_written"], 0)
        self.assertEqual(result["snapshot_blocked"], 0)
        self.assertEqual(result["snapshot_errors"], {})
        self.assertEqual(self.read_row()["result_json"], before)
        # ...but the row is still handed to the sheet, exactly as before.
        self.assertEqual(journal, ["sheet"])
        self.assertEqual(len(client.created), 1)

    def test_an_uncertain_write_is_reported_rather_than_raised(self):
        # A storage that explodes must not take the export down with it: the
        # row is simply not handed over, and the reason names the failure.
        journal: list = []
        self.seed()
        with patch.object(
            self.storage,
            "merge_item_result_section",
            side_effect=RuntimeError("db gone"),
        ):
            result, client = self.export(
                item=make_item(), storage=self.storage, journal=journal
            )
        self.assertEqual(result["snapshot_blocked"], 1)
        self.assertEqual(journal, [])
        self.assert_no_sheet_write(client)
        self.assertIn("RuntimeError", list(result["snapshot_errors"].values())[0])

    def test_the_row_can_be_handed_over_again_after_a_refusal(self):
        # The refusal is not terminal: once the snapshot write succeeds the same
        # row exports normally.  Otherwise the "re-export forever" loop would
        # simply have moved one step earlier.
        journal: list = []
        self.seed()
        blocked, blocked_client = self.export(
            item=make_item(), storage=None, journal=journal
        )
        self.assertEqual(blocked["snapshot_blocked"], 1)
        self.assert_no_sheet_write(blocked_client)
        journal.clear()
        passed, passed_client = self.export(
            item=make_item(), storage=self.storage, journal=journal
        )
        self.assertEqual(passed["snapshot_written"], 1)
        self.assertEqual(passed["snapshot_blocked"], 0)
        self.assertEqual(len(passed_client.created), 1)
        # The second attempt is the one that left the snapshot behind; the
        # ordering of the two writes is pinned by the journal test above.
        stored = json.loads(self.read_row()["result_json"])
        self.assertEqual(stored[DELIVERY_SNAPSHOT_FIELD]["prompt_text"], PUBLISHED_PROMPT)
        self.assertEqual(journal, ["sheet"])


if __name__ == "__main__":
    unittest.main()
