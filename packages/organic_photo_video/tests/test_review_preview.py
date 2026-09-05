"""Review-preview safety tests: no database promotion, no semantic QA gate."""

from __future__ import annotations

import hashlib
import json
import tempfile
from pathlib import Path
import sys
import unittest

PACKAGE_ROOT = Path(__file__).resolve().parent.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from domain.models import ContentShot, ContentTask
from services.review_preview import (
    PREVIEW_WATERMARK,
    ReviewPreviewError,
    ReviewPreviewService,
)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _plan():
    return {
        "shots": [
            {
                "slot_index": index,
                "slot_role": f"p{index}",
                "duration_ms": 2500,
                "motion_preset": "static_hold",
                "transition_out": "cut",
            }
            for index in range(1, 6)
        ]
    }


class PreviewRepo:
    def __init__(self, directory: Path):
        self.task = ContentTask(
            task_id="preview-task", idempotency_key="x" * 64,
            account_id="a", product_id="p", target_country="TH", target_locale="th-TH",
            task_status="image_review", plan_json=_plan(),
            selected_render_id=None, released_revision_id=None,
        )
        self.shots = []
        for index in range(1, 6):
            path = directory / f"p{index}.png"
            path.write_bytes(f"not-a-real-image-{index}".encode())
            self.shots.append(ContentShot(
                shot_id=f"shot-{index}", task_id=self.task.task_id, slot_index=index,
                slot_role=f"p{index}", duration_ms=2500, shot_status="generated",
                # Deliberately failed semantic QA: preview must remain usable
                # as an inspection aid without treating that as a release.
                qa_status="failed", image_url=str(path), image_sha256=_sha(path),
                image_width=941, image_height=1672,
            ))

    def get_task(self, task_id):
        return self.task if task_id == self.task.task_id else None

    def list_shots(self, task_id):
        return list(self.shots) if task_id == self.task.task_id else []


class PreviewRenderer:
    def __init__(self):
        self.slots = None

    def render(self, slots, output_path):
        self.slots = list(slots)
        Path(output_path).write_bytes(b"preview-mp4")
        return True, ""

    def qc_video(self, output_path, expected_ms):
        return {
            "passed": True, "duration_ms": expected_ms,
            "sha256": _sha(Path(output_path)),
            "checks": {"fake_media": True},
        }


class ReviewPreviewTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.repo = PreviewRepo(self.root)
        self.renderer = PreviewRenderer()
        self.service = ReviewPreviewService(self.repo, self.renderer, output_root=self.root)

    def tearDown(self):
        self.tmp.cleanup()

    def test_preview_is_watermarked_and_never_promotes_task_state(self):
        preview = self.service.create("preview-task")
        manifest = json.loads(Path(preview.manifest_path).read_text(encoding="utf-8"))

        self.assertTrue(Path(preview.output_path).is_file())
        self.assertEqual([slot.review_watermark_text for slot in self.renderer.slots], [PREVIEW_WATERMARK] * 5)
        self.assertTrue(manifest["preview_only"])
        self.assertFalse(manifest["publish_ready"])
        self.assertNotIn("render_id", manifest)
        self.assertIsNone(self.repo.task.selected_render_id)
        self.assertIsNone(self.repo.task.released_revision_id)
        self.assertEqual(self.repo.task.task_status, "image_review")

    def test_preview_rejects_stale_input_bytes(self):
        Path(self.repo.shots[2].image_url).write_bytes(b"changed")
        with self.assertRaisesRegex(ReviewPreviewError, "SHA256"):
            self.service.create("preview-task")

    def test_preview_does_not_accept_missing_slot(self):
        self.repo.shots.pop()
        with self.assertRaisesRegex(ReviewPreviewError, "exactly the planned slots"):
            self.service.create("preview-task")
