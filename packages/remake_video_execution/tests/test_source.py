from __future__ import annotations

import unittest

from remake_video_execution.source import freeze_record


class SourceTests(unittest.TestCase):
    def test_voiceover_and_handoff_changes_create_new_revision(self):
        base = {
            "脚本ID": "vs", "脚本来源": "视频复刻", "视频时长": 10,
            "视频生成提示词": "【0-10 秒】\n展示", "目标语言": "泰语",
            "发布用途": "养号", "是否挂车": "否",
            "产品图片": [{"file_token": "img"}], "口播_目标语言": "版本一",
        }
        first = freeze_record("rec", base).source_revision_hash
        second_fields = dict(base)
        second_fields["口播_目标语言"] = "版本二"
        second = freeze_record("rec", second_fields).source_revision_hash
        self.assertNotEqual(first, second)


if __name__ == "__main__":
    unittest.main()
