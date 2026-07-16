from __future__ import annotations

import unittest
from dataclasses import dataclass

from scripts.ingest_tiktok_bgm import (
    STATUS_DONE,
    STATUS_PENDING,
    STATUS_REVIEW,
    make_bgm_id,
    select_records,
    should_process,
    status_updates,
    text_value,
)


@dataclass
class FakeRecord:
    record_id: str
    fields: dict


class TikTokBgmIngestTest(unittest.TestCase):
    def test_text_value_reads_feishu_url_cell(self):
        self.assertEqual(text_value({"link": "https://vm.tiktok.com/demo", "text": "demo"}), "https://vm.tiktok.com/demo")

    def test_only_pending_or_new_link_records_are_selected(self):
        self.assertTrue(should_process({"TK来源链接": "https://www.tiktok.com/@a/video/123", "提取状态": "待提取"}))
        self.assertTrue(should_process({"TK来源链接": "https://www.tiktok.com/@a/video/123"}))
        self.assertFalse(should_process({"TK来源链接": "https://www.tiktok.com/@a/video/123", "提取状态": "已完成"}))
        self.assertFalse(should_process({"提取状态": "待提取"}))

    def test_select_records_obeys_limit(self):
        records = [
            FakeRecord("1", {"TK来源链接": "https://tiktok.com/1", "提取状态": STATUS_PENDING}),
            FakeRecord("2", {"TK来源链接": "https://tiktok.com/2", "提取状态": STATUS_PENDING}),
        ]
        self.assertEqual([item.record_id for item in select_records(records, limit=1)], ["1"])

    def test_status_updates_do_not_apply_license_restrictions(self):
        updates = status_updates(
            fields={"TK来源链接": "https://www.tiktok.com/@a/video/123"},
            attachment={"file_token": "token"},
            item_id="123",
            title="Sample post",
            vocal_type="instrumental",
            needs_review=False,
        )
        self.assertEqual(updates["提取状态"], STATUS_DONE)
        self.assertEqual(updates["AI人声类型"], "纯音乐")
        self.assertNotIn("授权状态", updates)
        self.assertNotIn("状态", updates)
        self.assertEqual(updates["BGM编号"], "BGM_TK_123")

    def test_clear_vocal_signal_requires_review(self):
        updates = status_updates(
            fields={"TK来源链接": "https://www.tiktok.com/@a/video/123", "BGM名称": "Keep me"},
            attachment={"file_token": "token"},
            item_id="123",
            title="Ignored",
            vocal_type="vocal",
            needs_review=True,
        )
        self.assertEqual(updates["提取状态"], STATUS_REVIEW)
        self.assertEqual(updates["AI人声类型"], "明显人声")
        self.assertNotIn("BGM名称", updates)

    def test_song_vocals_do_not_force_review_when_music_asset_is_official(self):
        updates = status_updates(
            fields={"TK来源链接": "https://www.tiktok.com/@a/video/123"},
            attachment={"file_token": "token"},
            item_id="123",
            title="Official song",
            vocal_type="vocal",
            needs_review=False,
        )
        self.assertEqual(updates["提取状态"], STATUS_DONE)
        self.assertEqual(updates["AI人声类型"], "明显人声")

    def test_bgm_id_falls_back_to_source_hash(self):
        first = make_bgm_id("direct", "https://example.com/a")
        second = make_bgm_id("direct", "https://example.com/a")
        self.assertEqual(first, second)
        self.assertTrue(first.startswith("BGM_TK_"))


if __name__ == "__main__":
    unittest.main()
