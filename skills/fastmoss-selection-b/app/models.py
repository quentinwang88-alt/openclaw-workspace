#!/usr/bin/env python3
"""领域模型。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class RuleConfig:
    config_id: str
    country: str
    category: str
    enabled: bool
    top_n: int = 50
    new_listing_days_threshold: int = 90
    total_sales_min: int = 500
    total_sales_max: int = 5000
    new_sales_7d_min: int = 120
    old_sales_7d_min: int = 200
    old_sales_ratio_min: float = 0.10
    video_density_max: float = 5.0
    creator_density_max: float = 20.0
    fx_rate_to_rmb: float = 1.0
    platform_fee_rate: float = 0.20
    accessory_head_shipping_rmb: float = 0.2
    light_top_head_shipping_rmb: float = 2.0
    heavy_apparel_head_shipping_rmb: float = 5.0
    accio_chat_id: str = ""
    enable_hermes: bool = True
    rule_version: str = "v1"
    note: str = ""


@dataclass
class BatchRecord:
    record_id: str
    batch_id: str
    data_source: str
    country: str
    category: str
    snapshot_time: str
    source_service: str
    attachments: List[Dict[str, Any]]
    raw_file_name: str
    raw_record_count: Optional[int]
    overall_status: str
    retry_count: int = 0
    captured_at: Optional[datetime] = None
    fields: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ImportResult:
    records: List[Dict[str, Any]]
    warnings: List[str]
    field_mapping: Dict[str, str]
    total_rows: int
    skipped_rows: int


@dataclass
class RuleEvaluationResult:
    shortlist: List[Dict[str, Any]]
    all_records: List[Dict[str, Any]]
    total_candidates: int
    passed_count: int


@dataclass
class AccioResponse:
    batch_id: str
    items: Dict[str, Dict[str, Any]]
    message_id: str
    raw_payload: Dict[str, Any]


@dataclass
class HermesBatchResult:
    status: str
    items: Dict[str, Dict[str, Any]]
    input_path: str
    output_path: Optional[str]
    stdout: str = ""
    stderr: str = ""
    error: str = ""
