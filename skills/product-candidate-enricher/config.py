#!/usr/bin/env python3
"""Default configuration for the product candidate enricher skill."""

from __future__ import annotations

import os


DEFAULT_FEISHU_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/"
    "CtGxwJpTEifSh5kIVtgcM2vCnLf?table=tblKhPn64Q266tRz&view=vewmWdRUHq"
)

DEFAULT_TIMEZONE = os.environ.get("PRODUCT_CANDIDATE_ENRICHER_TIMEZONE", "Asia/Shanghai")
DEFAULT_DATE_FORMATTER = "yyyy-MM-dd"

DEFAULT_LLM_BASE_URL = os.environ.get("PRODUCT_CANDIDATE_ENRICHER_LLM_BASE_URL", "https://yunwu.ai/v1")
DEFAULT_LLM_API_KEY = os.environ.get(
    "PRODUCT_CANDIDATE_ENRICHER_LLM_API_KEY",
    "sk-KiwuEJUQuDhxma0uLfeY6OCrENTmXOUalTZwulc3AEqIDbUj",
)
DEFAULT_LLM_MODEL = os.environ.get("PRODUCT_CANDIDATE_ENRICHER_LLM_MODEL", "gpt-4.1-nano")

DEFAULT_SUBCATEGORIES = (
    "发夹",
    "发簪",
    "发带",
    "发箍",
    "其它",
)

DEFAULT_BATCH_SIZE = 100
DEFAULT_TIMEOUT_SECONDS = 120
