import tempfile
import unittest
from pathlib import Path

from core.longform.main_schedule_bridge import (
    LongformScheduleBridgeError,
    enqueue_longform_final,
)


class FakeDB:
    def __init__(self):
        self.metadata = []
        self.asset = None

    def upsert_script_metadata(self, items):
        self.metadata.extend(items)
        return len(items)

    def upsert_video_asset(self, **kwargs):
        self.asset = kwargs


class LongformMainScheduleBridgeTest(unittest.TestCase):
    def test_completed_voiceover_video_enters_organic_queue_idempotently(self):
        db = FakeDB()
        with tempfile.TemporaryDirectory() as directory:
            video = Path(directory) / "final.mp4"
            video.write_bytes(b"final-video")
            result = enqueue_longform_final(
                record_id="rec001",
                job_id="LFJ_001",
                final_video_path=video,
                fields={
                    "产品编码": "P001",
                    "店铺ID": "THFZ01",
                    "目标国家": "TH",
                    "视频时长": 25,
                    "产品类型": "outerwear",
                    "口播_目标语言": "ชุดนี้ใส่ง่ายมากค่ะ. ประโยคถัดไป",
                },
                db=db,
            )

        self.assertEqual("longform:LFJ_001", result["canonical_script_key"])
        self.assertEqual("待排期", result["status"])
        self.assertEqual("embedded_voiceover_platform_bgm_low", db.metadata[0].audio_mode)
        self.assertEqual("否", db.metadata[0].cart_enabled)
        self.assertEqual("待排期", db.asset["publish_status"])
        self.assertEqual("longform:LFJ_001", db.asset["canonical_script_key"])

    def test_missing_target_copy_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            video = Path(directory) / "final.mp4"
            video.write_bytes(b"final-video")
            with self.assertRaisesRegex(LongformScheduleBridgeError, "目标语言口播"):
                enqueue_longform_final(
                    record_id="rec001",
                    job_id="LFJ_001",
                    final_video_path=video,
                    fields={"店铺ID": "THFZ01", "目标国家": "TH"},
                    db=FakeDB(),
                )


if __name__ == "__main__":
    unittest.main()
