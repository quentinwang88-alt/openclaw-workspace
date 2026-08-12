from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class Step(str, Enum):
    CHECK_DUPLICATE = "CHECK_DUPLICATE"
    COLLECT = "COLLECT"
    CLAIM = "CLAIM"
    LOCATE_PRODUCT = "LOCATE_PRODUCT"
    SET_SHOP = "SET_SHOP"
    OPEN_EDITOR = "OPEN_EDITOR"
    SET_PRICE = "SET_PRICE"
    SET_STOCK = "SET_STOCK"
    SET_WAREHOUSE = "SET_WAREHOUSE"
    SET_LOGISTICS = "SET_LOGISTICS"
    TRANSLATE = "TRANSLATE"
    TRANSLATE_SIZE_CHART = "TRANSLATE_SIZE_CHART"
    TRANSLATE_DETAIL_IMAGES = "TRANSLATE_DETAIL_IMAGES"
    TRANSLATE_MAIN_IMAGES = "TRANSLATE_MAIN_IMAGES"
    PREFLIGHT = "PREFLIGHT"
    PUBLISH = "PUBLISH"
    VERIFY = "VERIFY"


WORKFLOW_STEPS: List[Step] = list(Step)


class PricingMode(str, Enum):
    RULE = "RULE"
    FIXED = "FIXED"
    PURCHASE_MULTIPLIER = "PURCHASE_MULTIPLIER"


class ErrorCode(str, Enum):
    ALREADY_PUBLISHED = "ALREADY_PUBLISHED"
    COLLECT_FAILED = "COLLECT_FAILED"
    CLAIM_FAILED = "CLAIM_FAILED"
    PRODUCT_NOT_FOUND = "PRODUCT_NOT_FOUND"
    PRODUCT_MATCH_AMBIGUOUS = "PRODUCT_MATCH_AMBIGUOUS"
    SHOP_BIND_FAILED = "SHOP_BIND_FAILED"
    PRICE_FILL_FAILED = "PRICE_FILL_FAILED"
    STOCK_FILL_FAILED = "STOCK_FILL_FAILED"
    WAREHOUSE_NOT_FOUND = "WAREHOUSE_NOT_FOUND"
    LOGISTICS_FILL_FAILED = "LOGISTICS_FILL_FAILED"
    TRANSLATE_FAILED = "TRANSLATE_FAILED"
    IMAGE_TRANSLATE_FAILED = "IMAGE_TRANSLATE_FAILED"
    SIZE_CHART_DETECTION_FAILED = "SIZE_CHART_DETECTION_FAILED"
    SIZE_CHART_REQUIRED = "SIZE_CHART_REQUIRED"
    PREFLIGHT_FAILED = "PREFLIGHT_FAILED"
    PUBLISH_VALIDATION_FAILED = "PUBLISH_VALIDATION_FAILED"
    PUBLISH_FAILED = "PUBLISH_FAILED"
    PAGE_STRUCTURE_CHANGED = "PAGE_STRUCTURE_CHANGED"
    LOGIN_EXPIRED = "LOGIN_EXPIRED"
    NETWORK_ERROR = "NETWORK_ERROR"
    ELEMENT_TIMEOUT = "ELEMENT_TIMEOUT"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"


class ProductTask(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_id: str = Field(min_length=1)
    miaoshou_product_id: str = Field(min_length=1)
    source_url: str = ""
    target_shop: str = Field(min_length=1)
    market: str = Field(min_length=2)
    category_group: str = Field(min_length=1)
    pricing_rule_id: str = Field(min_length=1)
    pricing_mode: PricingMode = PricingMode.RULE
    fixed_sale_price: Optional[Decimal] = Field(default=None, gt=0)
    purchase_price_multiplier: Optional[Decimal] = Field(
        default=None, gt=0, le=100
    )
    stock_per_sku: Optional[int] = Field(default=None, ge=0)
    size_chart_url: str = ""
    size_chart_path: str = ""
    size_chart_file_token: str = ""
    size_chart_file_name: str = ""
    allow_republish: bool = False
    current_step: Optional[Step] = None

    @field_validator("market", "category_group")
    @classmethod
    def uppercase_codes(cls, value: str) -> str:
        return value.strip().upper()

    @model_validator(mode="after")
    def validate_pricing_inputs(self):
        # Backward compatibility for existing JSON tasks which only provided a
        # fixed_sale_price before pricing_mode existed.
        if self.pricing_mode == PricingMode.RULE and self.fixed_sale_price is not None:
            self.pricing_mode = PricingMode.FIXED
        if (
            self.pricing_mode == PricingMode.RULE
            and self.purchase_price_multiplier is not None
        ):
            self.pricing_mode = PricingMode.PURCHASE_MULTIPLIER
        if self.pricing_mode == PricingMode.FIXED:
            if self.fixed_sale_price is None:
                raise ValueError("FIXED pricing requires fixed_sale_price")
            if self.purchase_price_multiplier is not None:
                raise ValueError("FIXED pricing cannot include a multiplier")
        elif self.pricing_mode == PricingMode.PURCHASE_MULTIPLIER:
            if self.purchase_price_multiplier is None:
                raise ValueError(
                    "PURCHASE_MULTIPLIER pricing requires purchase_price_multiplier"
                )
            if self.fixed_sale_price is not None:
                raise ValueError(
                    "PURCHASE_MULTIPLIER pricing cannot include fixed_sale_price"
                )
        elif self.fixed_sale_price is not None or self.purchase_price_multiplier is not None:
            raise ValueError("RULE pricing cannot include fixed price or multiplier")
        return self


class StepEvent(BaseModel):
    task_id: str
    step: Step
    state: str
    attempt: int = 1
    message: str = ""
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ExecutionResult(BaseModel):
    task_id: str
    success: bool
    current_step: Optional[Step] = None
    platform_product_id: str = ""
    published_status: str = ""
    approval_fingerprint: str = ""
    error_code: Optional[ErrorCode] = None
    error_message: str = ""
    page_url: str = ""
    screenshot: str = ""
    retry_count: int = 0
    preflight: Dict[str, Any] = Field(default_factory=dict)
    events: List[StepEvent] = Field(default_factory=list)


class BrowserSettings(BaseModel):
    profile_dir: Path
    base_url: str
    cdp_url: str = ""
    cdp_connect_timeout_ms: int = 30_000
    channel: str = ""
    product_list_path: str = "/"
    collect_input_path: str = "/common_collect_box/index?fetchType=linkCopy"
    public_collect_box_path: str = "/common_collect_box/items"
    publish_history_path: str = "/tiktok/move_collect/history?status=success"
    headless: bool = False
    slow_mo_ms: int = 0
    timeout_ms: int = 15_000
    navigation_timeout_ms: int = 45_000
    publish_verify_timeout_ms: int = 600_000
    publish_verify_poll_intervals_seconds: List[float] = Field(
        default_factory=lambda: [2, 5, 10, 30]
    )
    image_translation_timeout_ms: int = 240_000
    locale: str = "zh-CN"


class AppConfig(BaseModel):
    browser: BrowserSettings
    markets: Dict[str, Dict[str, Any]]
    shops: Dict[str, Dict[str, Any]]
    pricing_rules: Dict[str, Dict[str, Any]]
    stock_rules: Dict[str, Dict[str, Any]]
    warehouse: Dict[str, Dict[str, Any]]
    logistics_profiles: Dict[str, Dict[str, Any]]
    retry: Dict[str, Any]
    feishu: Dict[str, Any] = Field(default_factory=dict)
