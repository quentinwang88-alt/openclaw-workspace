from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from remake_video_execution.executor import submit_frozen_segments
from remake_video_execution.providers.plan_c import PlanCMediaAdapter
from remake_video_execution.repository import RemakeExecutionRepository


class FakeGateway:
    def __init__(self):
        self.calls = 0

    @staticmethod
    def build_segment_request(segment, *, start_frame, end_frame="", reference_images=None):
        return {"segment": segment, "start_frame": start_frame, "end_frame": end_frame,
                "reference_images": reference_images or []}

    def submit(self, request_path, *, allow_real_submit):
        self.calls += 1
        return {"task_id": f"mock-{Path(request_path).stem}", "accepted": allow_real_submit}


class ExecutorTests(unittest.TestCase):
    def test_second_pass_does_not_submit_same_fingerprint_twice(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            frame = root / "frame.png"
            frame.write_bytes(b"frame")
            repository = RemakeExecutionRepository(root / "state.sqlite3")
            repository.ensure_schema()
            source = {"record_id": "rec", "script_id": "vs", "source_revision_hash": "hash"}
            segment = {
                "segment_id": "SEG_01", "requested_duration_seconds": 10,
                "prompt": "展示商品", "incoming_boundary": "START",
            }
            repository.save_plan("job", source, {"segments": [segment], "issues": []})
            gateway = FakeGateway()
            adapter = PlanCMediaAdapter(gateway)
            kwargs = dict(
                repository=repository, adapter=adapter, job_id="job", segments=[segment],
                start_frames={"SEG_01": str(frame)}, request_dir=root / "requests",
                allow_submit=True,
            )
            first = submit_frozen_segments(**kwargs)
            second = submit_frozen_segments(**kwargs)
            self.assertTrue(first[0]["submitted"])
            self.assertFalse(second[0]["submitted"])
            self.assertEqual(second[0]["status"], "SUBMITTED")
            self.assertEqual(gateway.calls, 1)
            with repository.connect() as connection:
                row = connection.execute("SELECT status,platform_task_id FROM remake_segment").fetchone()
            self.assertEqual(dict(row), {"status": "SUBMITTED", "platform_task_id": "mock-SEG_01"})
