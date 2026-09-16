"""Source identity is added to the long-form job store additively.

The production database already contains live original jobs, so the migration
must be ``ALTER TABLE ADD COLUMN`` only: no drop, no rebuild, no backfill that
could rewrite a historical row.  A legacy original job must read back exactly
as it did before, and a remake job must be idempotent per frozen revision.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from core.longform.storage import (
    SOURCE_KIND_ORIGINAL,
    SOURCE_KIND_REMAKE,
    LongformStorage,
    remake_job_id,
)


# The production table exactly as it existed before source identity was added.
LEGACY_JOB_TABLE = """
CREATE TABLE longform_job (
  job_id TEXT PRIMARY KEY,
  product_code TEXT NOT NULL,
  status TEXT NOT NULL,
  master_contract_json TEXT NOT NULL,
  plan_json TEXT NOT NULL,
  keyframe_package_json TEXT NOT NULL,
  voiceover_json TEXT NOT NULL DEFAULT '{}',
  merged_video_path TEXT NOT NULL DEFAULT '',
  final_video_path TEXT NOT NULL DEFAULT '',
  error_json TEXT NOT NULL DEFAULT '{}',
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
"""

LEGACY_SEGMENT_TABLE = """
CREATE TABLE longform_segment (
  job_id TEXT NOT NULL,
  segment_id TEXT NOT NULL,
  status TEXT NOT NULL,
  duration_seconds INTEGER NOT NULL,
  generation_mode TEXT NOT NULL,
  prompt TEXT NOT NULL,
  start_frame_path TEXT NOT NULL DEFAULT '',
  end_frame_path TEXT NOT NULL DEFAULT '',
  platform_task_id TEXT NOT NULL DEFAULT '',
  submit_fingerprint TEXT NOT NULL DEFAULT '',
  output_video_path TEXT NOT NULL DEFAULT '',
  platform_response_json TEXT NOT NULL DEFAULT '{}',
  error_json TEXT NOT NULL DEFAULT '{}',
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (job_id, segment_id)
);
"""


class LegacyUpgradeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = Path(self.temp.name) / "longform_original_video.sqlite3"
        self.storage = LongformStorage(self.db)

    def _build_legacy_db(self) -> None:
        """A pre-migration database holding one finished original job."""

        with sqlite3.connect(self.db) as conn:
            conn.executescript(LEGACY_JOB_TABLE)
            conn.executescript(LEGACY_SEGMENT_TABLE)
            conn.execute(
                """INSERT INTO longform_job
                (job_id, product_code, status, master_contract_json, plan_json,
                 keyframe_package_json, voiceover_json, merged_video_path,
                 final_video_path, created_at, updated_at)
                VALUES ('LFJ_LEGACY', 'P_OLD', 'FINAL_READY', '{"product_code":"P_OLD"}',
                        '{"segments":[]}', '{}', '{"target_text":"旧口播"}',
                        '/tmp/merged.mp4', '/tmp/final.mp4', 1, 2)""",
            )
            conn.execute(
                """INSERT INTO longform_segment
                (job_id, segment_id, status, duration_seconds, generation_mode,
                 prompt, platform_task_id, updated_at)
                VALUES ('LFJ_LEGACY', 'A', 'READY', 15, 'first_frame', 'x',
                        'remote-legacy', 2)""",
            )

    def test_legacy_database_upgrades_in_place_without_losing_rows(self):
        self._build_legacy_db()
        with sqlite3.connect(self.db) as conn:
            before = {str(row[1]) for row in conn.execute("PRAGMA table_info(longform_job)")}
        self.assertNotIn("source_kind", before)

        self.storage.ensure_schema()

        with sqlite3.connect(self.db) as conn:
            after = {str(row[1]) for row in conn.execute("PRAGMA table_info(longform_job)")}
            row = conn.execute(
                "SELECT * FROM longform_job WHERE job_id='LFJ_LEGACY'"
            ).fetchone()
            columns = [description[0] for description in conn.execute(
                "SELECT * FROM longform_job LIMIT 1"
            ).description]
        for column in ("source_kind", "source_record_id", "source_script_id",
                       "source_revision_hash"):
            self.assertIn(column, after)
        # The legacy row is intact and defaults to the historical source kind.
        values = dict(zip(columns, row))
        self.assertEqual(SOURCE_KIND_ORIGINAL, values["source_kind"])
        self.assertEqual("", values["source_record_id"])
        self.assertEqual("", values["source_revision_hash"])
        self.assertEqual('{"product_code":"P_OLD"}', values["master_contract_json"])
        self.assertEqual("FINAL_READY", values["status"])

        # A legacy job is invisible to source lookup and still resumable.
        self.assertIsNone(self.storage.find_job_by_source("LFJ_LEGACY", ""))
        job = self.storage.get_job("LFJ_LEGACY")
        self.assertEqual("remote-legacy", job["segments"][0]["platform_task_id"])
        self.assertEqual("/tmp/final.mp4", job["final_video_path"])

    def test_upgrade_is_idempotent_and_keeps_existing_jobs_saveable(self):
        self._build_legacy_db()
        for _ in range(3):
            self.storage.ensure_schema()
        # A historical job can still be updated without touching source identity.
        self.storage.update_job("LFJ_LEGACY", "MERGED", merged_video_path="/tmp/m2.mp4")
        job = self.storage.get_job("LFJ_LEGACY")
        self.assertEqual("MERGED", job["status"])
        self.assertEqual(SOURCE_KIND_ORIGINAL, job["source_kind"])


class RemakeSourceIdentityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.storage = LongformStorage(Path(self.temp.name) / "state.sqlite3")
        self.storage.ensure_schema()

    def _save(self, job_id: str, *, record_id: str, revision: str,
              kind: str = SOURCE_KIND_REMAKE):
        self.storage.save_plan(
            job_id,
            {"product_code": "P_REMAKE", "source_kind": kind},
            {"segments": [{"segment_id": "A", "duration_seconds": 15,
                           "generation_mode": "first_frame", "video_prompt": "x"}],
             "target_duration_seconds": 15},
            {},
            source_kind=kind, source_record_id=record_id,
            source_script_id="vs_1", source_revision_hash=revision,
        )

    def test_same_frozen_revision_maps_to_one_job(self):
        job_id = remake_job_id("rec1", "rev-a")
        self.assertTrue(job_id.startswith("LFR_"))
        self.assertEqual(job_id, remake_job_id("rec1", "rev-a"))
        self.assertNotEqual(job_id, remake_job_id("rec1", "rev-b"))
        self.assertNotEqual(job_id, remake_job_id("rec2", "rev-a"))

        self._save(job_id, record_id="rec1", revision="rev-a")
        # Re-saving the same frozen identity is an upsert, not a second job.
        self._save(job_id, record_id="rec1", revision="rev-a")
        found = self.storage.find_job_by_source("rec1", "rev-a")
        self.assertEqual(job_id, found["job_id"])
        self.assertEqual(SOURCE_KIND_REMAKE, found["source_kind"])
        with sqlite3.connect(self.storage.db_path) as conn:
            count = conn.execute("SELECT COUNT(*) FROM longform_job").fetchone()[0]
        self.assertEqual(1, count)

    def test_source_lookup_ignores_other_revisions_and_originals(self):
        self._save("LFR_A", record_id="rec1", revision="rev-a")
        self._save("LFR_B", record_id="rec1", revision="rev-b")
        self._save("LFJ_ORIG", record_id="rec1", revision="rev-a",
                   kind=SOURCE_KIND_ORIGINAL)
        self.assertEqual("LFR_A", self.storage.find_job_by_source("rec1", "rev-a")["job_id"])
        self.assertEqual("LFR_B", self.storage.find_job_by_source("rec1", "rev-b")["job_id"])
        self.assertIsNone(self.storage.find_job_by_source("rec1", "rev-c"))
        self.assertIsNone(self.storage.find_job_by_source("", "rev-a"))
        self.assertIsNone(self.storage.find_job_by_source("rec1", ""))

    def test_partial_index_allows_many_legacy_rows_but_one_identity(self):
        # Many original jobs share empty source identity and must coexist.
        for index in range(5):
            self.storage.save_plan(
                f"LFJ_{index}",
                {"product_code": f"P{index}", "source_kind": SOURCE_KIND_ORIGINAL},
                {"segments": [], "target_duration_seconds": 30}, {},
            )
        with sqlite3.connect(self.storage.db_path) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM longform_job WHERE source_record_id=''"
            ).fetchone()[0]
        self.assertEqual(5, count)

        self._save("LFR_DUP_A", record_id="rec9", revision="rev-9")
        with sqlite3.connect(self.storage.db_path) as conn:
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute(
                    """INSERT INTO longform_job
                    (job_id, product_code, status, master_contract_json, plan_json,
                     keyframe_package_json, created_at, updated_at,
                     source_kind, source_record_id, source_script_id, source_revision_hash)
                    VALUES ('LFR_DUP_B', 'P_REMAKE', 'PLANNED', '{}', '{}', '{}', 1, 1,
                            ?, 'rec9', 'vs_1', 'rev-9')""",
                    (SOURCE_KIND_REMAKE,),
                )

    def test_plan_payload_keeps_the_source_identity(self):
        self._save("LFR_PAYLOAD", record_id="rec1", revision="rev-a")
        job = self.storage.get_job("LFR_PAYLOAD")
        # The DB columns drive resume; the compiler's payload is stored verbatim.
        plan = json.loads(job["plan_json"])
        self.assertEqual(1, len(plan["segments"]))
        self.assertEqual("P_REMAKE", json.loads(job["master_contract_json"])["product_code"])
        self.assertEqual(SOURCE_KIND_REMAKE, job["source_kind"])
        self.assertEqual("rec1", job["source_record_id"])
        self.assertEqual("vs_1", job["source_script_id"])
        self.assertEqual("rev-a", job["source_revision_hash"])


if __name__ == "__main__":
    unittest.main()
