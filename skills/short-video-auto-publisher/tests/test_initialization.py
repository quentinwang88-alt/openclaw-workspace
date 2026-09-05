#!/usr/bin/env python3
"""账号初始化进度与内容供需统计测试。"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
import sys
import tempfile
import unittest


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from app.db import AutoPublishDB, is_opv_initialization_candidate
from app.models import AccountConfig, ScriptMetadata


class InitializationDBTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = AutoPublishDB(Path(self.temp_dir.name) / "autopublish.sqlite3")
        self.db.upsert_account_configs(
            [
                AccountConfig(
                    account_id="acc-init",
                    account_name="初始化账号",
                    store_id="SHOP-01",
                    account_status="可用",
                    publish_time_1="12:00",
                    publish_time_2="",
                    publish_time_3="",
                    nurture_enabled=True,
                    nurture_daily_count=1,
                    initialization_enabled=True,
                )
            ]
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _upsert_content(
        self,
        suffix: str,
        *,
        canonical_key: str | None = None,
        script_source: str = "图文养号",
        publish_purpose: str = "养号",
        context: dict | None = None,
    ) -> str:
        key = canonical_key or f"opv:{suffix}"
        script_id = f"script-{suffix}"
        self.db.upsert_script_metadata(
            [
                ScriptMetadata(
                    canonical_script_key=key,
                    script_id=script_id,
                    source_record_id=f"record-{suffix}",
                    script_slot="OPV",
                    task_no=suffix,
                    store_id="SHOP-01",
                    product_id="",
                    parent_slot="OPV",
                    direction_label="图文养号",
                    variant_strength="成片",
                    target_country="TH",
                    product_type="apparel",
                    content_family_key=f"family-{suffix}",
                    script_text=json.dumps(context or {}, ensure_ascii=False),
                    short_video_title=f"title-{suffix}",
                    title_source="opv_copy",
                    script_source=script_source,
                    publish_purpose=publish_purpose,
                    cart_enabled="否",
                    content_branch="非商品展示型",
                )
            ]
        )
        self.db.upsert_video_asset(
            canonical_script_key=key,
            script_id=script_id,
            run_manager_record_id=f"record-{suffix}",
            video_source_type="opv_render",
            video_source_value=f"/tmp/{suffix}.mp4",
            local_file_path=f"/tmp/{suffix}.mp4",
            download_status="下载成功",
            run_video_status="已完成",
        )
        return key

    def _insert_slot(
        self,
        *,
        canonical_key: str,
        suffix: str,
        status: str,
        task_id: str = "",
    ) -> None:
        with self.db._connect() as conn:
            conn.execute(
                """
                INSERT INTO publish_slots (
                    store_id, account_id, account_name, scheduled_for,
                    canonical_script_key, script_id, schedule_status,
                    publish_task_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "SHOP-01",
                    "acc-init",
                    "初始化账号",
                    f"2026-09-{int(suffix):02d} 12:00:00",
                    canonical_key,
                    f"script-{suffix}",
                    status,
                    task_id,
                    "2026-09-01 00:00:00",
                    "2026-09-01 00:00:00",
                ),
            )
            if status == "已发布":
                conn.execute(
                    "UPDATE video_assets SET published_at = ? WHERE canonical_script_key = ?",
                    (f"2026-09-{int(suffix):02d} 12:05:00", canonical_key),
                )

    def test_account_config_persists_initialization_flag(self) -> None:
        row = self.db.get_account_config("acc-init")
        self.assertEqual(int(row["initialization_enabled"]), 1)
        columns = {
            str(item["name"])
            for item in self.db._connect().execute("PRAGMA table_info(account_configs)")
        }
        self.assertIn("initialization_enabled", columns)

    def test_ready_candidate_exposes_opv_context(self) -> None:
        self._upsert_content(
            "1",
            context={
                "recipe_id": "RECIPE_PAIN_POINT_SOLUTION_V1",
                "theme_id": "THEME_TH_PETITE_V1",
                "source_product_id": "173",
            },
        )
        candidate = self.db.list_ready_candidates("SHOP-01")[0]
        self.assertTrue(is_opv_initialization_candidate(candidate))
        self.assertEqual(candidate.recipe_id, "RECIPE_PAIN_POINT_SOLUTION_V1")
        self.assertEqual(candidate.theme_id, "THEME_TH_PETITE_V1")
        self.assertEqual(candidate.source_product_id, "173")

    def test_progress_counts_only_published_and_remote_scheduled_opv(self) -> None:
        published = self._upsert_content("1", context={"recipe_id": "RECIPE_ONE"})
        scheduled = self._upsert_content("2", context={"recipe_id": "RECIPE_TWO"})
        no_remote_task = self._upsert_content("3")
        ordinary = self._upsert_content(
            "4", canonical_key="ordinary:4", script_source="原创脚本", publish_purpose="带货"
        )
        self._insert_slot(canonical_key=published, suffix="1", status="已发布", task_id="neo-1")
        self._insert_slot(canonical_key=scheduled, suffix="2", status="已排期", task_id="neo-2")
        self._insert_slot(canonical_key=no_remote_task, suffix="3", status="已排期")
        self._insert_slot(canonical_key=ordinary, suffix="4", status="已发布", task_id="neo-4")

        progress = self.db.get_account_initialization_progress("acc-init")
        self.assertEqual(progress["published_count"], 1)
        self.assertEqual(progress["scheduled_count"], 1)
        self.assertEqual(progress["remaining_count"], 1)
        self.assertEqual(progress["status"], "RUNNING")
        self.assertEqual(progress["first_published_at"], "2026-09-01 12:05:00")
        self.assertEqual(
            self.db.list_account_initialization_recipe_ids("acc-init"),
            {"RECIPE_ONE", "RECIPE_TWO"},
        )
        self.assertEqual(
            self.db.count_scheduled_initialization_for_account_day(
                "acc-init", datetime(2026, 9, 2, 18, 0)
            ),
            1,
        )

    def test_supply_summary_counts_only_unassigned_opv_content(self) -> None:
        published = self._upsert_content("1")
        scheduled = self._upsert_content("2")
        self._upsert_content("3")
        self._upsert_content(
            "4", canonical_key="ordinary:4", script_source="原创脚本", publish_purpose="带货"
        )
        self._insert_slot(canonical_key=published, suffix="1", status="已发布", task_id="neo-1")
        self._insert_slot(canonical_key=scheduled, suffix="2", status="已排期", task_id="neo-2")

        self.assertEqual(self.db.count_available_initialization_content(store_id="SHOP-01"), 1)
        summary = self.db.initialization_supply_summary(store_id="SHOP-01")
        self.assertEqual(
            summary,
            {
                "initialization_accounts": 1,
                "initialization_pending": 1,
                "initialization_completed": 0,
                "required_items": 1,
                "scheduled_items": 1,
                "ready_pool_items": 1,
                "content_gap": 0,
            },
        )

    def test_submission_reservation_prevents_duplicate_remote_creation(self) -> None:
        canonical_key = self._upsert_content("reserve")
        self.db.upsert_account_configs(
            [
                AccountConfig(
                    account_id="acc-other",
                    account_name="另一个账号",
                    store_id="SHOP-01",
                    account_status="可用",
                    publish_time_1="12:00",
                    publish_time_2="",
                    publish_time_3="",
                )
            ]
        )
        now = datetime(2026, 9, 1, 11, 0)
        self.db.generate_future_slots(now, window_hours=2)
        slots = self.db.list_pending_slots(now, window_hours=2)

        first = self.db.reserve_slot_for_submission(
            slot_id=int(slots[0]["slot_id"]),
            canonical_script_key=canonical_key,
            script_id="script-reserve",
            account_id=str(slots[0]["account_id"]),
            account_name=str(slots[0]["account_name"]),
            planned_publish_at=datetime(2026, 9, 1, 12, 0),
        )
        second = self.db.reserve_slot_for_submission(
            slot_id=int(slots[1]["slot_id"]),
            canonical_script_key=canonical_key,
            script_id="script-reserve",
            account_id=str(slots[1]["account_id"]),
            account_name=str(slots[1]["account_name"]),
            planned_publish_at=datetime(2026, 9, 1, 12, 0),
        )

        self.assertTrue(first)
        self.assertFalse(second)
        self.assertEqual(self.db.list_ready_candidates("SHOP-01"), [])
        self.assertEqual(
            self.db.release_slot_submission_reservation(int(slots[0]["slot_id"]), "test release"),
            1,
        )
        self.assertEqual(
            [item.canonical_script_key for item in self.db.list_ready_candidates("SHOP-01")],
            [canonical_key],
        )


if __name__ == "__main__":
    unittest.main()
