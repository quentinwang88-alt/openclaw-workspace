from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from remake_video_execution.providers.plan_c import PlanCMediaAdapter


class FakeGateway:
    @staticmethod
    def build_segment_request(segment, *, start_frame, end_frame="", reference_images=None):
        return {"prompt": segment["video_prompt"], "imagePaths": [start_frame, *(reference_images or [])], "duration": segment["duration_seconds"]}

    def submit(self, request_path, *, allow_real_submit):
        return {"path": str(request_path), "allowed": allow_real_submit}


class PlanCAdapterTests(unittest.TestCase):
    def test_real_submit_requires_flag_and_writes_gateway_request(self):
        with tempfile.TemporaryDirectory() as directory:
            frame = Path(directory) / "frame.png"
            frame.write_bytes(b"frame")
            payload = PlanCMediaAdapter.request_payload(
                {"segment_id": "SEG_01", "requested_duration_seconds": 10,
                 "prompt": "保持原稿", "incoming_boundary": "START"},
                start_frame=str(frame),
            )
            adapter = PlanCMediaAdapter(FakeGateway())
            with self.assertRaises(PermissionError):
                adapter.submit(payload, request_path=Path(directory) / "request.json", allow_real_submit=False)
            result = adapter.submit(
                payload, request_path=Path(directory) / "request.json", allow_real_submit=True
            )
            self.assertTrue(Path(result["path"]).is_file())
            self.assertTrue(result["allowed"])

    def test_cut_segment_uses_reference_mode_instead_of_resetting_first_frame(self):
        with tempfile.TemporaryDirectory() as directory:
            frame = Path(directory) / "frame.png"
            frame.write_bytes(b"frame")
            payload = PlanCMediaAdapter.request_payload(
                {"segment_id": "SEG_02", "requested_duration_seconds": 8,
                 "prompt": "转向侧面", "incoming_boundary": "CUT"},
                start_frame=str(frame), reference_images=[str(frame)],
            )
            self.assertEqual(payload["generation_mode"], "reference")
            self.assertEqual(payload["reference_images"], [str(frame)])


if __name__ == "__main__":
    unittest.main()
