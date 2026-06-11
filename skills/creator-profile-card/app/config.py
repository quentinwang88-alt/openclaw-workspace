"""
达人画像卡配置模块。

优先读环境变量，回退到项目默认值。
"""
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SKILL_ROOT = Path(__file__).resolve().parents[1]

sys.path.insert(0, str(REPO_ROOT))
try:
    from workspace_support import load_repo_env, get_shared_data_dir
    load_repo_env()
except ImportError:
    def get_shared_data_dir():
        p = Path.home() / ".openclaw" / "shared" / "data"
        p.mkdir(parents=True, exist_ok=True)
        return p

try:
    from openclaw_core import read_openclaw_config
except ImportError:
    read_openclaw_config = None


# ── LLM ───────────────────────────────────────────────────
LLM_API_URL = os.environ.get(
    "CREATOR_PROFILE_LLM_API_URL",
    os.environ.get("LLM_API_URL", "https://chatgpt.com/backend-api/codex"),
)
LLM_MODEL = os.environ.get(
    "CREATOR_PROFILE_LLM_MODEL",
    os.environ.get("LLM_MODEL", "gpt-5.5"),
)
LLM_API_KEY = os.environ.get(
    "CREATOR_PROFILE_LLM_API_KEY",
    os.environ.get("LLM_API_KEY", ""),
)
LLM_REASONING_EFFORT = os.environ.get(
    "CREATOR_PROFILE_REASONING_EFFORT", "medium"
)
LLM_TIMEOUT = int(os.environ.get("CREATOR_PROFILE_LLM_TIMEOUT", "180"))

# ── Feishu ────────────────────────────────────────────────
FEISHU_APP_ID = os.environ.get("FEISHU_APP_ID", "")
FEISHU_APP_SECRET = os.environ.get("FEISHU_APP_SECRET", "")
FEISHU_APP_TOKEN = os.environ.get("CREATOR_PROFILE_FEISHU_APP_TOKEN", "")
FEISHU_TABLE_ID = os.environ.get("CREATOR_PROFILE_FEISHU_TABLE_ID", "")

# ── Log Database ──────────────────────────────────────────
LOG_DB_PATH = os.environ.get(
    "CREATOR_PROFILE_LOG_DB_PATH",
    str(get_shared_data_dir() / "creator_profile_logs.sqlite3"),
)

# ── Confidence Thresholds ─────────────────────────────────
CONFIDENCE_AUTO_WRITE = float(os.environ.get("CREATOR_PROFILE_CONFIDENCE_AUTO", "0.75"))
CONFIDENCE_WRITE_WITH_REVIEW = float(os.environ.get("CREATOR_PROFILE_CONFIDENCE_WARN", "0.55"))

# ── Cover Constraints ─────────────────────────────────────
MIN_COVERS_FULL_CONFIDENCE = int(os.environ.get("CREATOR_PROFILE_MIN_COVERS_FULL", "12"))
MIN_COVERS_ANY_AUTO = int(os.environ.get("CREATOR_PROFILE_MIN_COVERS_AUTO", "8"))

# ── Prompt Version ────────────────────────────────────────
PROMPT_VERSION = os.environ.get("CREATOR_PROFILE_PROMPT_VERSION", "v1.0")
