#!/usr/bin/env python3
"""规则预检查。"""

from __future__ import annotations

from typing import Iterable

from src.enums import AnalysisStatus, SUPPORTED_CATEGORIES
from src.models import CandidateTask, PrecheckResult


class RuleChecker(object):
    def check(self, task: CandidateTask, supported_manual_categories: Iterable[str] = None) -> PrecheckResult:
        if not task.product_images:
            return PrecheckResult(
                should_continue=False,
                terminal_status=AnalysisStatus.INSUFFICIENT_INFO.value,
                terminal_reason="缺少产品图片",
            )

        manual_category = (task.manual_category or "").strip()
        if not manual_category:
            return PrecheckResult(should_continue=True)

        supported = set(supported_manual_categories or SUPPORTED_CATEGORIES)
        if manual_category not in supported:
            return PrecheckResult(
                should_continue=False,
                terminal_status=AnalysisStatus.UNSUPPORTED_CATEGORY.value,
                terminal_reason="人工类目不在当前支持范围内",
            )
        return PrecheckResult(should_continue=True)
