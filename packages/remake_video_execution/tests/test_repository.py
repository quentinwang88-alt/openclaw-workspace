from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from remake_video_execution.repository import RemakeExecutionRepository


class RepositoryTests(unittest.TestCase):
    def test_blocking_plan_is_saved_as_needs_input(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = RemakeExecutionRepository(Path(directory) / "state.sqlite3")
            repository.ensure_schema()
            repository.save_plan(
                "job", {"record_id": "rec", "script_id": "vs", "source_revision_hash": "hash"},
                {"segments": [], "issues": [{"severity": "BLOCK", "code": "MISSING"}]},
            )
            with repository.connect() as connection:
                status = connection.execute("SELECT status FROM remake_job").fetchone()["status"]
            self.assertEqual(status, "NEEDS_INPUT")

    def test_uncertain_submission_is_not_reserved_again(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = RemakeExecutionRepository(Path(directory) / "state.sqlite3")
            repository.ensure_schema()
            self.assertEqual(repository.reserve_submission("job", "seg", "fp"), "SUBMITTING")
            repository.mark_submission("job", "seg", "fp", status="SUBMISSION_UNKNOWN")
            self.assertEqual(repository.reserve_submission("job", "seg", "fp"), "SUBMISSION_UNKNOWN")

    def test_claim_reports_whether_attempt_is_new(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = RemakeExecutionRepository(Path(directory) / "state.sqlite3")
            repository.ensure_schema()
            self.assertEqual(repository.claim_submission("job", "seg", "fp"), ("SUBMITTING", True))
            self.assertEqual(repository.claim_submission("job", "seg", "fp"), ("SUBMITTING", False))

    def test_new_source_revision_supersedes_only_unstarted_job(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = RemakeExecutionRepository(Path(directory) / "state.sqlite3")
            repository.ensure_schema()
            plan = {"segments": [], "issues": []}
            repository.save_plan("old", {"record_id": "rec", "script_id": "vs", "source_revision_hash": "old"}, plan)
            repository.save_plan("new", {"record_id": "rec", "script_id": "vs", "source_revision_hash": "new"}, plan)
            with repository.connect() as connection:
                statuses = dict(connection.execute("SELECT job_id,status FROM remake_job").fetchall())
            self.assertEqual(statuses, {"old": "SUPERSEDED", "new": "PLANNED"})


if __name__ == "__main__":
    unittest.main()
