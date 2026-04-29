#!/usr/bin/env python3
"""脚本主数据拆分测试。"""

from __future__ import annotations

import unittest
from dataclasses import dataclass
from pathlib import Path
import sys


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from app.metadata import (
    FallbackTitleGenerator,
    HeuristicTitleGenerator,
    LocalizedLLMTitleGenerator,
    SCRIPT_FIELD_SPECS,
    SOURCE_FIELD_ALIASES,
    build_script_metadata_records,
    build_title_prompt,
    is_title_compatible_with_country,
    resolve_field_mapping,
    sanitize_title,
)
from app.models import ScriptMetadata


@dataclass
class Record:
    record_id: str
    fields: dict


class MetadataTest(unittest.TestCase):
    def test_specs_cover_24_slots(self) -> None:
        self.assertEqual(len(SCRIPT_FIELD_SPECS), 24)

    def test_build_script_metadata_records(self) -> None:
        field_names = [
            "任务编号",
            "店铺ID",
            "产品ID",
            "目标国家",
            "产品类型",
            "所属母版1",
            "母版方向1",
            "脚本方向一",
            "脚本1变体1",
        ]
        mapping = resolve_field_mapping(field_names, SOURCE_FIELD_ALIASES)
        records = [
            Record(
                record_id="rec_1",
                fields={
                    "任务编号": "001",
                    "店铺ID": "SHOP-01",
                    "产品ID": "P1001",
                    "目标国家": "Thailand",
                    "产品类型": "耳环",
                    "所属母版1": "M1",
                    "母版方向1": "日常轻分享流",
                    "脚本方向一": "【脚本定位】\n- 脚本标题：วันนี้ใส่แล้วดูละมุนขึ้น\n- 方向类型：轻分享",
                    "脚本1变体1": "试了这个搭配以后整个人更柔和了",
                },
            )
        ]

        items = build_script_metadata_records(records, mapping, title_generator=HeuristicTitleGenerator())

        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].script_id, "001_M1_M")
        self.assertEqual(items[0].canonical_script_key, "rec_1:S1")
        self.assertEqual(items[0].content_family_key, "P1001_M1")
        self.assertEqual(items[1].script_id, "001_M1_V1")
        self.assertEqual(items[1].canonical_script_key, "rec_1:S1V1")
        self.assertEqual(items[1].variant_strength, "轻变体")
        self.assertTrue(items[0].short_video_title)

    def test_build_script_metadata_records_uses_original_script_for_master_slot(self) -> None:
        field_names = [
            "任务编号",
            "店铺ID",
            "产品ID",
            "目标国家",
            "产品类型",
            "所属母版1",
            "母版方向1",
            "脚本方向一",
            "脚本1变体1",
        ]
        mapping = resolve_field_mapping(field_names, SOURCE_FIELD_ALIASES)
        records = [
            Record(
                record_id="rec_prefinal",
                fields={
                    "任务编号": "009",
                    "店铺ID": "SHOP-09",
                    "产品ID": "P9001",
                    "目标国家": "Thailand",
                    "产品类型": "耳环",
                    "所属母版1": "M1",
                    "母版方向1": "日常轻分享流",
                    "脚本方向一": "【脚本定位】\n- 脚本标题：原脚本",
                    "脚本1变体1": "variant one",
                },
            )
        ]

        items = build_script_metadata_records(records, mapping, title_generator=HeuristicTitleGenerator())

        self.assertEqual(items[0].script_text, "【脚本定位】\n- 脚本标题：原脚本")
        self.assertEqual(items[1].script_text, "variant one")

    def test_build_script_metadata_records_reuses_existing_compatible_title(self) -> None:
        field_names = [
            "任务编号",
            "店铺ID",
            "产品ID",
            "目标国家",
            "产品类型",
            "所属母版1",
            "母版方向1",
            "脚本方向一",
        ]
        mapping = resolve_field_mapping(field_names, SOURCE_FIELD_ALIASES)
        records = [
            Record(
                record_id="rec_cached",
                fields={
                    "任务编号": "010",
                    "店铺ID": "SHOP-10",
                    "产品ID": "P1010",
                    "目标国家": "Thailand",
                    "产品类型": "耳环",
                    "所属母版1": "M1",
                    "母版方向1": "日常轻分享流",
                    "脚本方向一": "หยิบคู่นี้แล้วลุคดูซอฟต์ขึ้น",
                },
            )
        ]

        class ExplodingGenerator:
            source = "llm"

            def generate(self, _: ScriptMetadata) -> str:
                raise AssertionError("should reuse cached title instead of regenerating")

        items = build_script_metadata_records(
            records,
            mapping,
            title_generator=ExplodingGenerator(),
            existing_lookup={
                ("rec_cached", "S1"): {
                    "short_video_title": "หยิบคู่นี้แล้วลุคดูซอฟต์ขึ้นทันที",
                    "title_source": "cached",
                    "script_text": "หยิบคู่นี้แล้วลุคดูซอฟต์ขึ้น",
                    "target_country": "Thailand",
                }
            },
        )

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].short_video_title, "หยิบคู่นี้แล้วลุคดูซอฟต์ขึ้นทันที")
        self.assertEqual(items[0].title_source, "cached")

    def test_build_script_metadata_records_regenerates_incompatible_cached_title(self) -> None:
        field_names = [
            "任务编号",
            "店铺ID",
            "产品ID",
            "目标国家",
            "产品类型",
            "所属母版1",
            "母版方向1",
            "脚本方向一",
        ]
        mapping = resolve_field_mapping(field_names, SOURCE_FIELD_ALIASES)
        records = [
            Record(
                record_id="rec_refresh",
                fields={
                    "任务编号": "011",
                    "店铺ID": "SHOP-11",
                    "产品ID": "P1011",
                    "目标国家": "Thailand",
                    "产品类型": "耳环",
                    "所属母版1": "M1",
                    "母版方向1": "日常轻分享流",
                    "脚本方向一": "หยิบคู่นี้แล้วลุคดูซอฟต์ขึ้น",
                },
            )
        ]

        class ThaiGenerator:
            source = "heuristic"

            def generate(self, _: ScriptMetadata) -> str:
                return "หยิบคู่นี้แล้วลุคดูซอฟต์ขึ้นทันที"

        items = build_script_metadata_records(
            records,
            mapping,
            title_generator=ThaiGenerator(),
            existing_lookup={
                ("rec_refresh", "S1"): {
                    "short_video_title": "标题：今天戴上更温柔了",
                    "title_source": "cached",
                    "script_text": "หยิบคู่นี้แล้วลุคดูซอฟต์ขึ้น",
                    "target_country": "Thailand",
                }
            },
        )

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].short_video_title, "หยิบคู่นี้แล้วลุคดูซอฟต์ขึ้นทันที")
        self.assertEqual(items[0].title_source, "heuristic")

    def test_sanitize_title_removes_leading_title_label(self) -> None:
        self.assertEqual(sanitize_title("视频标题：นี่คือชุดที่หยิบใส่ง่ายมาก"), "นี่คือชุดที่หยิบใส่ง่ายมาก")

    def test_title_prompt_requires_local_language_and_plain_title_only(self) -> None:
        metadata = ScriptMetadata(
            script_id="001_M1_M",
            source_record_id="rec_1",
            script_slot="S1",
            task_no="001",
            store_id="SHOP-01",
            product_id="P1001",
            parent_slot="M1",
            direction_label="日常轻分享流",
            variant_strength="母版",
            target_country="Thailand",
            product_type="耳环",
            content_family_key="P1001_M1",
            script_text="วันนี้ใส่แล้วดูละมุนขึ้น",
            short_video_title="",
            title_source="",
        )
        prompt = build_title_prompt(metadata)
        self.assertIn("请直接使用【泰语】", prompt)
        self.assertIn("不要输出“视频主题：”“视频标题：”“标题：”这类前缀", prompt)

    def test_title_compatibility_checks_country_language(self) -> None:
        self.assertTrue(is_title_compatible_with_country("หยิบคู่นี้แล้วลุคดูซอฟต์ขึ้นทันที", "Thailand"))
        self.assertFalse(is_title_compatible_with_country("视频标题：今天戴上更温柔了", "Thailand"))

    def test_fallback_title_generator_rejects_wrong_language_primary_output(self) -> None:
        metadata = ScriptMetadata(
            script_id="001_M1_M",
            source_record_id="rec_1",
            script_slot="S1",
            task_no="001",
            store_id="SHOP-01",
            product_id="P1001",
            parent_slot="M1",
            direction_label="日常轻分享流",
            variant_strength="母版",
            target_country="Thailand",
            product_type="耳环",
            content_family_key="P1001_M1",
            script_text="วันนี้ใส่แล้วดูละมุนขึ้น",
            short_video_title="",
            title_source="",
        )

        class BadPrimary:
            source = "llm"

            def generate(self, _: ScriptMetadata) -> str:
                return "视频标题：今天戴上更温柔了"

        class GoodSecondary:
            source = "heuristic"

            def generate(self, _: ScriptMetadata) -> str:
                return "หยิบคู่นี้แล้วลุคดูซอฟต์ขึ้นทันที"

        generator = FallbackTitleGenerator(primary=BadPrimary(), secondary=GoodSecondary())
        self.assertEqual(generator.generate(metadata), "หยิบคู่นี้แล้วลุคดูซอฟต์ขึ้นทันที")

    def test_fallback_title_generator_returns_empty_when_both_outputs_invalid(self) -> None:
        metadata = ScriptMetadata(
            script_id="001_M1_M",
            source_record_id="rec_1",
            script_slot="S1",
            task_no="001",
            store_id="SHOP-01",
            product_id="P1001",
            parent_slot="M1",
            direction_label="日常轻分享流",
            variant_strength="母版",
            target_country="Thailand",
            product_type="耳环",
            content_family_key="P1001_M1",
            script_text="วันนี้ใส่แล้วดูละมุนขึ้น",
            short_video_title="",
            title_source="",
        )

        class BadPrimary:
            source = "llm"

            def generate(self, _: ScriptMetadata) -> str:
                return "视频标题：今天戴上更温柔了"

        class BadSecondary:
            source = "heuristic"

            def generate(self, _: ScriptMetadata) -> str:
                return "标题：今天这个好温柔"

        generator = FallbackTitleGenerator(primary=BadPrimary(), secondary=BadSecondary())
        self.assertEqual(generator.generate(metadata), "")

    def test_localized_llm_title_generator_retries_multiple_routes(self) -> None:
        metadata = ScriptMetadata(
            script_id="001_M1_M",
            source_record_id="rec_1",
            script_slot="S1",
            task_no="001",
            store_id="SHOP-01",
            product_id="P1001",
            parent_slot="M1",
            direction_label="日常轻分享流",
            variant_strength="母版",
            target_country="Thailand",
            product_type="耳环",
            content_family_key="P1001_M1",
            script_text="วันนี้ใส่แล้วดูละมุนขึ้น",
            short_video_title="",
            title_source="",
        )

        class FakeLLM:
            def __init__(self, route: str):
                self.route = route

            def generate(self, _: ScriptMetadata) -> str:
                if self.route == "auto":
                    return "视频标题：今天戴上更温柔了"
                if self.route == "backup":
                    return "หยิบคู่นี้แล้วลุคดูซอฟต์ขึ้นทันที"
                return ""

        from unittest.mock import patch

        with patch("app.metadata.LLMTitleGenerator", FakeLLM):
            generator = LocalizedLLMTitleGenerator(preferred_route="auto", extra_routes=("backup", "primary"))
            self.assertEqual(generator.generate(metadata), "หยิบคู่นี้แล้วลุคดูซอฟต์ขึ้นทันที")


if __name__ == "__main__":
    unittest.main()
