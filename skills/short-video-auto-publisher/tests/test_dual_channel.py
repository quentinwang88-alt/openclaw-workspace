#!/usr/bin/env python3
"""Dual-channel content-scope scheduling tests (account two-row bindings)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys
import tempfile
import unittest
from dataclasses import dataclass, field as dc_field
from typing import Any, Dict, List

TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
for value in (str(SKILL_DIR), str(SKILL_DIR.parent / "script-run-manager-sync")):
    if value not in sys.path:
        sys.path.insert(0, value)

from app.db import AutoPublishDB
from app.publishers import DryRunPublishAdapter
from app.scheduler import (
    ACCOUNT_FIELD_ALIASES,
    normalize_content_scope,
    resolve_field_mapping,
    row_record,
    row_time,
    schedule_slots,
    sync_accounts,
)


@dataclass
class Rec:
    record_id: str
    fields: Dict[str, Any] = dc_field(default_factory=dict)


def dual_row_account() -> List[Rec]:
    return [
        Rec(
            "rec_neobund",
            {
                "账号ID": "tocrystal66",
                "账号名称": "泰国女装1",
                "店铺ID": "THFZ01",
                "账号状态": "可用",
                "发布渠道": "NeoBund",
                "发布时间1": "13:25",
                "发布时间2": "21:30",
                "内容类型限定": "带货",
            },
        ),
        Rec(
            "rec_creatok",
            {
                "账号ID": "tocrystal66",
                "账号名称": "泰国女装1",
                "店铺ID": "THFZ01",
                "账号状态": "可用",
                "发布渠道": "CreatOK",
                "发布时间1": "15:00",
                "发布时间2": "22:00",
                "内容类型限定": "不带货",
            },
        ),
    ]


class NormalizeScopeTest(unittest.TestCase):
    def test_scope_normalization(self) -> None:
        self.assertEqual(normalize_content_scope("带货"), "shoppable")
        self.assertEqual(normalize_content_scope("不带货"), "organic")
        self.assertEqual(normalize_content_scope(None), "all")
        self.assertEqual(normalize_content_scope("全部"), "all")


class BindingSyncTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = AutoPublishDB(Path(self.tmp.name) / "t.sqlite3")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _sync(self, records: List[Rec]) -> None:
        mapping = resolve_field_mapping(
            ["账号ID", "账号名称", "店铺ID", "账号状态", "发布渠道",
             "发布时间1", "发布时间2", "发布时间3", "内容类型限定"],
            ACCOUNT_FIELD_ALIASES,
        )
        sync_accounts(records, mapping, self.db)

    def test_dual_channel_rows_produce_scoped_bindings(self) -> None:
        self._sync(dual_row_account())
        bindings = self.db.list_account_channel_bindings("tocrystal66")
        by_channel = {b["publish_channel"]: b for b in bindings}
        self.assertEqual(set(by_channel), {"NeoBund", "CreatOK"})
        self.assertEqual(by_channel["NeoBund"]["content_scope"], "shoppable")
        self.assertEqual(by_channel["CreatOK"]["content_scope"], "organic")
        self.assertEqual(by_channel["NeoBund"]["publish_time_1"], "13:25")
        self.assertEqual(by_channel["CreatOK"]["publish_time_2"], "22:00")

    def test_dual_channel_sync_is_independent_of_row_order(self) -> None:
        rows = dual_row_account()
        rows[1].fields.update({"是否开启养号": True, "每日养号条数": 3})
        mapping = resolve_field_mapping(
            list({key for row in rows for key in row.fields}), ACCOUNT_FIELD_ALIASES
        )
        sync_accounts(list(reversed(rows)), mapping, self.db)
        account = self.db.get_account_config("tocrystal66")
        self.assertEqual(account["publish_channel"], "CreatOK")
        self.assertEqual(account["nurture_enabled"], 1)
        self.assertEqual(account["nurture_daily_count"], 3)

    def test_complement_autofill_when_one_side_blank(self) -> None:
        rows = dual_row_account()
        rows[0].fields.pop("内容类型限定")  # NeoBund row blank
        self._sync(rows)
        by_channel = {
            b["publish_channel"]: b["content_scope"]
            for b in self.db.list_account_channel_bindings("tocrystal66")
        }
        self.assertEqual(by_channel["NeoBund"], "shoppable")  # complement of organic
        self.assertEqual(by_channel["CreatOK"], "organic")

    def test_conflicting_scopes_disable_dual_channel(self) -> None:
        rows = dual_row_account()
        rows[1].fields["内容类型限定"] = "带货"  # both shoppable
        self._sync(rows)
        self.assertEqual(
            self.db.list_account_channel_bindings("tocrystal66"), []
        )
        self.assertEqual(self.db.get_account_config("tocrystal66")["account_status"], "暂停")

    def test_overlapping_channel_windows_pause_account(self) -> None:
        rows = dual_row_account()
        rows[1].fields["发布时间1"] = "13:25"
        self._sync(rows)
        self.assertEqual(self.db.list_account_channel_bindings("tocrystal66"), [])
        self.assertEqual(self.db.get_account_config("tocrystal66")["account_status"], "暂停")

    def test_both_blank_with_two_channels_not_enabled(self) -> None:
        rows = dual_row_account()
        rows[0].fields.pop("内容类型限定")
        rows[1].fields.pop("内容类型限定")
        self._sync(rows)
        # ambiguity stays unresolved until the sheet fills one side
        self.assertEqual(
            self.db.list_account_channel_bindings("tocrystal66"), []
        )

    def test_single_channel_row_keeps_full_scope(self) -> None:
        rows = dual_row_account()[:1]
        self._sync(rows)
        bindings = self.db.list_account_channel_bindings("tocrystal66")
        self.assertEqual(len(bindings), 1)
        self.assertEqual(bindings[0]["content_scope"], "shoppable")
        # rows untouched by scope removal stay all-scope
        rows[0].fields.pop("内容类型限定")
        self._sync(rows)
        self.assertEqual(
            self.db.list_account_channel_bindings("tocrystal66")[0]["content_scope"],
            "all",
        )

    def test_row_helpers(self) -> None:
        rows = [
            {
                "publish_channel": r.fields["发布渠道"],
                "publish_time_1": r.fields.get("发布时间1", ""),
                "publish_time_2": r.fields.get("发布时间2", ""),
                "source_record_id": r.record_id,
            }
            for r in dual_row_account()
        ]
        self.assertEqual(row_time(rows, "CreatOK", 1), "15:00")
        self.assertEqual(row_time(rows, "NeoBund", 2), "21:30")
        self.assertEqual(row_record(rows, "CreatOK"), "rec_creatok")


class SlotGenerationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = AutoPublishDB(Path(self.tmp.name) / "t.sqlite3")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _seed_dual_account(self) -> None:
        mapping = resolve_field_mapping(
            ["账号ID", "账号名称", "店铺ID", "账号状态", "发布渠道",
             "发布时间1", "发布时间2", "内容类型限定"],
            ACCOUNT_FIELD_ALIASES,
        )
        sync_accounts(dual_row_account(), mapping, self.db)

    def test_slots_stamped_per_binding_windows(self) -> None:
        self._seed_dual_account()
        created = self.db.generate_future_slots(
            datetime(2026, 9, 8, 0, 0, 0), window_hours=24
        )
        self.assertEqual(created, 4)  # 2 windows x 2 channels
        rows = self.db._connect().execute(
            "SELECT scheduled_for, publish_channel_used FROM publish_slots ORDER BY scheduled_for"
        ).fetchall()
        stamps = [(r["scheduled_for"][11:16], r["publish_channel_used"]) for r in rows]
        self.assertEqual(
            stamps,
            [("13:25", "NeoBund"), ("15:00", "CreatOK"), ("21:30", "NeoBund"), ("22:00", "CreatOK")],
        )

    def test_no_bindings_falls_back_to_account_times(self) -> None:
        mapping = resolve_field_mapping(
            ["账号ID", "账号名称", "店铺ID", "账号状态", "发布渠道", "发布时间1"],
            ACCOUNT_FIELD_ALIASES,
        )
        sync_accounts(
            [Rec("r1", {
                "账号ID": "acc1", "账号名称": "n", "店铺ID": "s",
                "账号状态": "可用", "发布渠道": "GeeLark", "发布时间1": "09:00",
            })],
            mapping,
            self.db,
        )
        created = self.db.generate_future_slots(
            datetime(2026, 9, 8, 0, 0, 0), window_hours=24
        )
        self.assertEqual(created, 1)
        row = self.db._connect().execute(
            "SELECT publish_channel_used FROM publish_slots"
        ).fetchone()
        self.assertEqual(row["publish_channel_used"], "GeeLark")


if __name__ == "__main__":
    unittest.main()
