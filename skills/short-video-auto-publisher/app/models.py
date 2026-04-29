#!/usr/bin/env python3
"""自动发布系统数据模型。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class ScriptMetadata:
    script_id: str
    source_record_id: str
    script_slot: str
    task_no: str
    store_id: str
    product_id: str
    parent_slot: str
    direction_label: str
    variant_strength: str
    target_country: str
    product_type: str
    content_family_key: str
    script_text: str
    short_video_title: str
    title_source: str
    canonical_script_key: str = ""
    script_source: str = ""
    publish_purpose: str = ""
    cart_enabled: str = ""
    content_branch: str = ""


@dataclass(frozen=True)
class AccountConfig:
    account_id: str
    account_name: str
    store_id: str
    account_status: str
    publish_time_1: str
    publish_time_2: str
    publish_time_3: str
    nurture_enabled: bool = False
    nurture_daily_count: int = 2
    nurture_only: bool = False


@dataclass(frozen=True)
class PublishCandidate:
    script_id: str
    store_id: str
    product_id: str
    content_family_key: str
    short_video_title: str
    local_file_path: str
    publish_video_value: str
    source_record_id: str
    script_slot: str
    product_title: str = ""
    ref_video_id: str = ""
    canonical_script_key: str = ""
    script_source: str = ""
    publish_purpose: str = ""
    cart_enabled: str = ""
    content_branch: str = ""


@dataclass(frozen=True)
class SlotAssignment:
    slot_id: int
    account_id: str
    account_name: str
    store_id: str
    scheduled_for: datetime
    script_id: str
    publish_task_id: str
    canonical_script_key: str = ""


@dataclass(frozen=True)
class PublishTaskStatus:
    state: str
    result: str
    published_at: Optional[str] = None
    error_message: str = ""
