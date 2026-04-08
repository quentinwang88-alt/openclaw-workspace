#!/usr/bin/env python3
"""项目配置。"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from workspace_support import load_repo_env

load_repo_env()

from app.utils.storage_paths import get_creator_monitoring_database_url


@dataclass(frozen=True)
class Settings:
    """运行配置。"""

    database_url: str = os.environ.get(
        "DATABASE_URL",
        get_creator_monitoring_database_url(),
    )
    top_percent_core: float = float(os.environ.get("TOP_PERCENT_CORE", "0.2"))
    volatility_drop_ratio: float = float(os.environ.get("VOLATILITY_DROP_RATIO", "0.6"))
    refund_risk_multiplier: float = float(os.environ.get("REFUND_RISK_MULTIPLIER", "1.5"))
    stop_loss_low_efficiency_ratio: float = float(os.environ.get("STOP_LOSS_LOW_EFFICIENCY_RATIO", "0.5"))
    rolling_window_weeks: int = int(os.environ.get("ROLLING_WINDOW_WEEKS", "4"))
    new_creator_max_weeks: int = int(os.environ.get("NEW_CREATOR_MAX_WEEKS", "4"))
    min_weeks_for_volatility: int = int(os.environ.get("MIN_WEEKS_FOR_VOLATILITY", "3"))
    min_valid_gmv: float = float(os.environ.get("MIN_VALID_GMV", "100"))
    min_valid_order_count: int = int(os.environ.get("MIN_VALID_ORDER_COUNT", "3"))
    min_valid_content_action_count: int = int(os.environ.get("MIN_VALID_CONTENT_ACTION_COUNT", "1"))
    min_valid_sample_count: int = int(os.environ.get("MIN_VALID_SAMPLE_COUNT", "1"))
    min_positive_efficiency_peer_count: int = int(os.environ.get("MIN_POSITIVE_EFFICIENCY_PEER_COUNT", "5"))
    feishu_app_id: str = os.environ.get("FEISHU_APP_ID", "")
    feishu_app_secret: str = os.environ.get("FEISHU_APP_SECRET", "")
    feishu_app_token: str = os.environ.get("FEISHU_APP_TOKEN", "")
    feishu_table_id: str = os.environ.get("FEISHU_TABLE_ID", "")
    feishu_write_batch_size: int = int(os.environ.get("FEISHU_WRITE_BATCH_SIZE", "100"))
    feishu_read_page_size: int = int(os.environ.get("FEISHU_READ_PAGE_SIZE", "200"))
    feishu_enable_sync: bool = os.environ.get("FEISHU_ENABLE_SYNC", "true").lower() == "true"
    platform_default: str = os.environ.get("PLATFORM_DEFAULT", "tiktok")
    country_default: str = os.environ.get("COUNTRY_DEFAULT", "unknown")
    store_default: str = os.environ.get("STORE_DEFAULT", "")
    rule_version: str = os.environ.get("RULE_VERSION", "v1")


def get_settings() -> Settings:
    return Settings()
