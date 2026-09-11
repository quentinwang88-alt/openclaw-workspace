import tempfile
import unittest
from pathlib import Path

from core.longform.workflow import RemoteGenerationPending, _wait_and_download
from scripts.run_feishu_longform_production_tasks import (
    _list_records_with_transient_retry,
    _longform_first_frame_writeback,
)


class FakeStorage:
    def __init__(self):
        self.job_status = "A_SUBMITTED"
        self.segment_updates = []

    def get_job(self, _job_id):
        return {
            "segments": [{
                "segment_id": "A",
                "status": "SUBMITTED",
                "platform_task_id": "remote-123",
            }]
        }

    def update_segment(self, *args, **kwargs):
        self.segment_updates.append((args, kwargs))

    def update_job(self, _job_id, status, **_kwargs):
        self.job_status = status


class PendingGateway:
    def query(self, _task_id):
        return {"response": {"task": {"status": "processing"}}}


class LongformRemoteResumeTest(unittest.TestCase):
    def test_generated_k0_is_projected_once_to_longform_workbench(self):
        class Client:
            def __init__(self):
                self.uploads = 0

            def upload_attachment(self, **kwargs):
                self.uploads += 1
                self.kwargs = kwargs
                return {"file_token": "k0-token"}

        class Storage:
            def __init__(self, path):
                self.path = path

            def get_job(self, _job_id):
                return {"segments": [{"start_frame_path": str(self.path)}]}

        with tempfile.TemporaryDirectory() as directory:
            k0 = Path(directory) / "K0.png"
            k0.write_bytes(b"png")
            client = Client()
            fields, error = _longform_first_frame_writeback(
                client, Storage(k0), "LFJ_TEST", {},
            )

        self.assertEqual("", error)
        self.assertEqual(
            [{"file_token": "k0-token"}], fields["长视频首帧（系统）"]
        )
        self.assertEqual("image/png", client.kwargs["content_type"])
        self.assertEqual(1, client.uploads)

        skipped, error = _longform_first_frame_writeback(
            client,
            Storage(Path("/missing")),
            "LFJ_TEST",
            {"长视频首帧（系统）": [{"file_token": "existing"}]},
        )
        self.assertEqual({}, skipped)
        self.assertEqual("", error)
        self.assertEqual(1, client.uploads)

    def test_k0_projection_failure_is_non_blocking(self):
        class Storage:
            def get_job(self, _job_id):
                return {"segments": [{"start_frame_path": "/missing/K0.png"}]}

        fields, error = _longform_first_frame_writeback(
            object(), Storage(), "LFJ_TEST", {},
        )
        self.assertEqual({}, fields)
        self.assertIn("不存在", error)

    def test_feishu_data_not_ready_is_retried_but_other_errors_are_not(self):
        class Client:
            def __init__(self):
                self.calls = 0

            def list_records(self, page_size):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("Data not ready, please try again later")
                return ["ready"]

        client = Client()
        self.assertEqual(
            ["ready"],
            _list_records_with_transient_retry(client, delays=(0,)),
        )
        self.assertEqual(2, client.calls)

    def test_poll_deadline_preserves_remote_task_for_next_patrol(self):
        storage = FakeStorage()
        events = []
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(RemoteGenerationPending, "下一轮"):
                _wait_and_download(
                    storage,
                    PendingGateway(),
                    "LFJ_TEST",
                    "A",
                    Path(directory),
                    poll_interval_seconds=5,
                    max_wait_seconds=0,
                    events=events,
                )

        self.assertEqual("A_WAITING_REMOTE", storage.job_status)
        self.assertEqual("H3_A_WAITING_REMOTE", events[-1]["stage"])
        self.assertEqual("remote-123", events[-1]["task_id"])
        self.assertTrue(storage.segment_updates)


if __name__ == "__main__":
    unittest.main()
