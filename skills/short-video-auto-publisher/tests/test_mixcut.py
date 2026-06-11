#!/usr/bin/env python3
"""混剪视频接入自动发布测试。"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from app.mixcut import (  # noqa: E402
    _ensure_auto_mixcut_env_defaults,
    _is_publishable_output_qc,
    build_mixcut_metadata,
    build_simple_mixcut_title,
)


class MixcutBridgeTest(unittest.TestCase):
    def test_mixcut_metadata_keeps_product_store_and_material_ids(self) -> None:
        output = {
            "output_id": "OUT_001",
            "product_id": "1734482585843304442",
            "batch_id": "BATCH_001",
            "template_id": "AI_OUTERWEAR_PRODUCT_FIRST_20S",
        }
        product = {
            "product_id": "1734482585843304442",
            "product_name": "米白短款外套",
            "shop_id": "THPS01",
            "market": "TH",
            "category": "womens_outerwear",
        }

        metadata = build_mixcut_metadata(output, product)

        self.assertEqual(metadata.canonical_script_key, "mixcut:OUT_001")
        self.assertEqual(metadata.script_id, "OUT_001")
        self.assertEqual(metadata.source_record_id, "OUT_001")
        self.assertEqual(metadata.product_id, "1734482585843304442")
        self.assertEqual(metadata.store_id, "THPS01")
        self.assertEqual(metadata.script_source, "混剪视频")
        self.assertEqual(metadata.publish_purpose, "混剪视频")
        self.assertEqual(metadata.content_family_key, "mixcut:1734482585843304442:AI_OUTERWEAR_PRODUCT_FIRST_20S")
        self.assertIn("material_id=OUT_001", metadata.script_text)
        self.assertEqual(metadata.short_video_title, "米白短款外套")
        self.assertEqual(metadata.title_source, "mixcut_simple_product_title")

    def test_mixcut_title_uses_manual_title_first(self) -> None:
        title = build_simple_mixcut_title(
            {"short_video_title": "标题：波点抓夹，随手夹也好看"},
            {"product_name": "粉色波点鲨鱼夹", "核心视觉点": "波点"},
        )

        self.assertEqual(title["title"], "波点抓夹，随手夹也好看")
        self.assertEqual(title["source"], "mixcut_manual")

    def test_mixcut_title_adds_short_anchor_phrase(self) -> None:
        title = build_simple_mixcut_title(
            {},
            {
                "product_name": "พร้อมส่ง New Sale 米白短款外套 女装外套 秋冬上衣",
                "核心视觉点": "米白色短款版型, 罗纹立领, 衣长至腰腹位置",
            },
        )

        self.assertEqual(title["title"], "米白短款外套 女装外套 秋冬上衣，米白色短款版型 罗纹立领")
        self.assertEqual(title["source"], "mixcut_simple_product_anchor")

    def test_mixcut_title_reads_anchor_json(self) -> None:
        title = build_simple_mixcut_title(
            {},
            {
                "product_name": "粉色波点抓夹",
                "anchor_json": '{"core_visual_points":["粉色波点","鲨鱼夹"]}',
            },
        )

        self.assertEqual(title["title"], "粉色波点抓夹，粉色波点 鲨鱼夹")
        self.assertEqual(title["source"], "mixcut_simple_product_anchor")

    def test_publishable_output_qc_accepts_human_status(self) -> None:
        self.assertTrue(_is_publishable_output_qc({"人工质检状态": "可发布"}))
        self.assertTrue(_is_publishable_output_qc({"是否可发布": True}))
        self.assertFalse(_is_publishable_output_qc({"人工质检状态": "待检查"}))

    def test_auto_mixcut_env_defaults_use_mysql_when_database_url_exists(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "LIKEU_AI_DATABASE_URL": "mysql+pymysql://user:pass@example.com/db",
            },
            clear=True,
        ):
            _ensure_auto_mixcut_env_defaults()

            import os

            self.assertEqual(os.environ["AUTO_MIXCUT_DB_PROVIDER"], "mysql")
            self.assertEqual(os.environ["AUTO_MIXCUT_OSS_PROVIDER"], "local")


if __name__ == "__main__":
    unittest.main()
