#!/usr/bin/env python3
"""标题轻量拆词。"""

from __future__ import annotations

from typing import List

from src.enums import SupportedCategory
from src.models import TitleParseResult


HAIR_KEYWORDS = [
    "抓夹",
    "鲨鱼夹",
    "发夹",
    "发箍",
    "发带",
    "发绳",
    "大肠发圈",
    "盘发",
    "高颅顶",
    "后脑勺",
    "洗脸夹",
    "大号",
    "通勤",
    "韩系",
]

LIGHT_TOP_KEYWORDS = [
    "开衫",
    "针织",
    "薄款",
    "薄外套",
    "防晒",
    "防晒衫",
    "衬衫",
    "冰丝",
    "短款",
    "宽松",
    "V领",
    "圆领",
    "修身",
    "显瘦",
    "空调房",
    "外搭",
    "罩衫",
    "轻薄",
]


class TitleParser(object):
    def parse_title(self, product_title: str) -> TitleParseResult:
        title = (product_title or "").strip()
        if not title:
            return TitleParseResult(title_keyword_tags=[], title_category_hint="", title_category_confidence="")

        hair_tags = self._collect_tags(title, HAIR_KEYWORDS)
        light_top_tags = self._collect_tags(title, LIGHT_TOP_KEYWORDS)
        merged_tags = self._merge_tags(hair_tags, light_top_tags)

        title_category_hint = ""
        title_category_confidence = ""
        if hair_tags and not light_top_tags:
            title_category_hint = SupportedCategory.HAIR_ACCESSORY.value
            title_category_confidence = "high" if len(hair_tags) >= 2 else "medium"
        elif light_top_tags and not hair_tags:
            title_category_hint = SupportedCategory.LIGHT_TOPS.value
            title_category_confidence = "high" if len(light_top_tags) >= 2 else "medium"
        elif len(hair_tags) > len(light_top_tags):
            title_category_hint = SupportedCategory.HAIR_ACCESSORY.value
            title_category_confidence = "medium"
        elif len(light_top_tags) > len(hair_tags):
            title_category_hint = SupportedCategory.LIGHT_TOPS.value
            title_category_confidence = "medium"

        return TitleParseResult(
            title_keyword_tags=merged_tags,
            title_category_hint=title_category_hint,
            title_category_confidence=title_category_confidence,
        )

    def _collect_tags(self, title: str, keywords: List[str]) -> List[str]:
        return [keyword for keyword in keywords if keyword and keyword in title]

    def _merge_tags(self, left: List[str], right: List[str]) -> List[str]:
        merged = []
        seen = set()
        for item in left + right:
            if item in seen:
                continue
            seen.add(item)
            merged.append(item)
        return merged
