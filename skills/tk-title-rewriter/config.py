#!/usr/bin/env python3
"""Default configuration for the TK title rewriter skill."""

from __future__ import annotations

import os


DEFAULT_TITLE_FIELD_CANDIDATES = (
    "产品标题",
    "原始标题",
    "标题",
    "中文标题",
    "title",
)

DEFAULT_CATEGORY_FIELD_CANDIDATES = (
    "产品类目",
    "类目",
    "品类",
    "category",
)

DEFAULT_OUTPUT_FIELD = "TK标题"
DEFAULT_CN_SUMMARY_FIELD = "优化后的标题（中文）"

DEFAULT_LLM_BASE_URL = os.environ.get(
    "TK_TITLE_REWRITER_LLM_BASE_URL",
    os.environ.get("LLM_API_URL", "https://yunwu.ai/v1"),
)
DEFAULT_LLM_API_KEY = os.environ.get(
    "TK_TITLE_REWRITER_LLM_API_KEY",
    os.environ.get("LLM_API_KEY", os.environ.get("ORIGINAL_SCRIPT_BACKUP_LLM_API_KEY", "")),
)
DEFAULT_LLM_MODEL = os.environ.get(
    "TK_TITLE_REWRITER_LLM_MODEL",
    os.environ.get("LLM_MODEL", "gpt-5.4"),
)
DEFAULT_LLM_TIMEOUT_SECONDS = int(os.environ.get("TK_TITLE_REWRITER_LLM_TIMEOUT_SECONDS", "120"))

DEFAULT_LLM_BATCH_SIZE = 20
DEFAULT_WRITE_BATCH_SIZE = 500
MAX_LLM_BATCH_SIZE = 20
MAX_WRITE_BATCH_SIZE = 500
