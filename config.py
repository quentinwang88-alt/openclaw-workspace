#!/usr/bin/env python3
"""项目配置文件。优先从仓库根目录 `.env` 读取本机配置。"""

import os

from workspace_support import load_repo_env

load_repo_env()

# ============================================================
# 飞书多维表格配置
# ============================================================
# 飞书 App Token（多维表格唯一标识，放在 .env 中）
FEISHU_APP_TOKEN = os.environ.get("FEISHU_APP_TOKEN", "")

# 飞书 Table ID（具体表格唯一标识，放在 .env 中）
FEISHU_TABLE_ID = os.environ.get("FEISHU_TABLE_ID", "")

# ============================================================
# AI API 中转服务配置
# ============================================================
# Claude API 中转（ai678.top）
CLAUDE_API_BASE_URL = os.environ.get("CLAUDE_API_BASE_URL", "https://www.ai678.top/v1")
CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY", "")

# Gemini API 中转（yunwu.ai）
GEMINI_API_BASE_URL = os.environ.get("GEMINI_API_BASE_URL", "https://yunwu.ai/v1")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

# ============================================================
# 处理参数配置
# ============================================================
# 封面数量最低要求（生成宫图至少需要的封面数）
MIN_COVER_COUNT = 12

# 封面数量警告阈值（低于此值时警告但仍可继续）
MIN_COVER_COUNT_WARN = 9
