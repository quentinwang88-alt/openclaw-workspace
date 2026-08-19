from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Optional, Sequence, Set

from .size_chart_detector import CodexVisionJSONClient, VisionJSONClient


class SizeChartCoverageError(RuntimeError):
    pass


def normalize_size_token(value: str) -> str:
    token = re.sub(r"[\s_-]+", "", str(value).upper())
    aliases = {
        "XXL": "2XL",
        "XXXL": "3XL",
        "XXXXL": "4XL",
        "XXXXXL": "5XL",
        "F": "FREE",
        "FS": "FREE",
        "FREESIZE": "FREE",
        "ONESIZE": "FREE",
        "均码": "FREE",
    }
    return aliases.get(token, token)


def extract_expected_size_tokens(labels: Sequence[str]) -> Set[str]:
    result: Set[str] = set()
    pattern = re.compile(r"(?<![A-Z0-9])(?:[2-9]XL|X{1,5}L|XS|S|M|L)(?![A-Z0-9])", re.I)
    for label in labels:
        text = str(label).upper()
        if any(word in text for word in ("FREE SIZE", "ONE SIZE", "均码")):
            result.add("FREE")
        result.update(normalize_size_token(match.group(0)) for match in pattern.finditer(text))
    return result


def build_coverage_prompt(expected: Sequence[str]) -> str:
    return f"""
你是电商尺码图核对器。只读取图片中明确可见的尺码标签，不推测任何数据。
需要核对的 SKU 尺码为：{', '.join(expected)}。
识别 S/M/L/XL/XXL/2XL 等同类写法；均码、Free size、One size 统一输出 FREE。
只输出合法 JSON：{{"sizes":["S","M","L"]}}。没有看清则输出空数组。
""".strip()


@dataclass(frozen=True)
class SizeChartCoverage:
    expected: Set[str]
    visible: Set[str]

    @property
    def missing(self) -> Set[str]:
        return self.expected - self.visible


class SizeChartCoverageValidator:
    def __init__(self, client: Optional[VisionJSONClient] = None) -> None:
        self.client = client or CodexVisionJSONClient()

    def validate(self, image_path: str, sku_labels: Sequence[str]) -> SizeChartCoverage:
        expected = extract_expected_size_tokens(sku_labels)
        if not expected:
            return SizeChartCoverage(expected=set(), visible=set())
        payload: Any = self.client.call_json(
            build_coverage_prompt(sorted(expected)), [image_path], max_output_tokens=500
        )
        values = payload.get("sizes") if isinstance(payload, dict) else None
        if not isinstance(values, list):
            raise SizeChartCoverageError("尺码图识别结果格式无效")
        visible = {normalize_size_token(value) for value in values if str(value).strip()}
        coverage = SizeChartCoverage(expected=expected, visible=visible)
        if coverage.missing:
            raise SizeChartCoverageError(
                "尺码图未覆盖当前 SKU 尺码：" + ", ".join(sorted(coverage.missing))
            )
        return coverage
