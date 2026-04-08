#!/usr/bin/env python3
"""Shared creator_crm model runtime configuration."""

from __future__ import annotations

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from workspace_support import load_repo_env

load_repo_env()

# LLM API 配置 - 火山引擎 doubao（用于评分+风格分析）
LLM_API_URL = os.environ.get("LLM_API_URL", "https://ark.cn-beijing.volces.com/api/coding/v3")
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
LLM_MODEL = os.environ.get("LLM_MODEL", "Doubao-Seed-2.0-pro")

# Category Tagging 专用配置（使用 responses API）
CATEGORY_API_URL = os.environ.get("CATEGORY_API_URL", "https://ark.cn-beijing.volces.com/api/v3")
CATEGORY_API_KEY = os.environ.get("CATEGORY_API_KEY", "")
CATEGORY_MODEL = os.environ.get("CATEGORY_MODEL", "doubao-seed-2-0-mini-260215")

DOUBAO_VISION_MODELS = [
    "doubao-seed-2-0-pro-260215",
]

OPENAI_VISION_MODELS = [
    "gpt-4o",
    "gpt-4o-mini",
    "claude-3-5-sonnet",
    "claude-3-haiku",
]

GEMINI_VISION_MODELS = [
    "gemini-3.1-flash-lite-preview",
    "gemini-2.0-flash",
    "gemini-1.5-flash",
    "gemini-1.5-pro",
]

DEFAULT_VISION_MODELS = DOUBAO_VISION_MODELS + GEMINI_VISION_MODELS + OPENAI_VISION_MODELS

DEFAULT_TIMEOUT_SECONDS = 120
