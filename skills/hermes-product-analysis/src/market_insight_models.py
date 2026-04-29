#!/usr/bin/env python3
"""Market Insight v1 data models."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

from src.models import TableSourceConfig


@dataclass
class MarketInsightConfig:
    table_id: str
    table_name: str
    enabled: bool
    source: TableSourceConfig = field(default_factory=TableSourceConfig)
    source_scope: str = "official"
    input_mode: str = "auto"
    default_country: str = ""
    default_category: str = ""
    batch_date: str = ""
    max_samples: int = 50
    min_consumable_product_count: int = 100
    min_consumable_direction_count: int = 5
    min_report_valid_sample_ratio: float = 0.70
    source_currency: str = ""
    price_to_cny_rate: float = 0.0
    price_band_step_rmb: float = 0.0
    price_scale_divisor: float = 1.0
    price_band_edges: List[float] = field(default_factory=lambda: [0, 50, 100, 200, 500, 1000])
    field_map: Dict[str, Any] = field(default_factory=dict)
    output_dir: str = ""
    report_output: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProductRankingSnapshot:
    batch_date: str
    batch_id: str
    country: str
    category: str
    product_id: str
    product_name: str
    shop_name: str
    price_min: Optional[float]
    price_max: Optional[float]
    price_mid: Optional[float]
    sales_7d: float
    gmv_7d: float
    creator_count: float
    video_count: float
    listing_days: Optional[int]
    product_images: List[str] = field(default_factory=list)
    raw_product_images: Any = None
    image_url: str = ""
    product_url: str = ""
    rank_index: int = 0
    listing_datetime: str = ""
    product_age_days: Optional[int] = None
    age_bucket: str = ""
    listing_date_parse_status: str = ""
    raw_category: str = ""
    source_feishu_url: str = ""
    source_app_token: str = ""
    source_table_id: str = ""
    market_id: str = ""
    category_id: str = ""
    market_id_resolution_method: str = ""
    market_id_confidence: float = 0.0
    category_id_resolution_method: str = ""
    category_id_confidence: float = 0.0
    raw_fields: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ShopRankingSnapshot:
    batch_date: str
    batch_id: str
    country: str
    category: str
    shop_name: str
    shop_positioning: str = ""
    sales_7d: float = 0.0
    gmv_7d: float = 0.0
    active_product_count: Optional[float] = None
    new_product_share: Optional[float] = None
    listed_product_count: Optional[float] = None
    creator_count: float = 0.0
    rank_index: int = 0
    raw_fields: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class MarketInsightProductTag:
    is_valid_sample: bool
    style_cluster: str
    style_tags_secondary: List[str] = field(default_factory=list)
    product_form: str = ""
    length_form: str = ""
    element_tags: List[str] = field(default_factory=list)
    value_points: List[str] = field(default_factory=list)
    scene_tags: List[str] = field(default_factory=list)
    reason_short: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @property
    def style_tag_main(self) -> str:
        return self.style_cluster

    @style_tag_main.setter
    def style_tag_main(self, value: str) -> None:
        self.style_cluster = value

    @property
    def product_form_or_result(self) -> str:
        return self.product_form

    @product_form_or_result.setter
    def product_form_or_result(self, value: str) -> None:
        self.product_form = value


@dataclass
class ScoredProductSnapshot:
    snapshot: ProductRankingSnapshot
    tag: MarketInsightProductTag
    heat_score: float
    heat_level: str
    crowd_score: float
    crowd_level: str
    priority_level: str
    target_price_band: str
    direction_canonical_key: str = ""
    direction_family: str = ""
    direction_tier: str = ""
    seasonal_trend: str = ""
    seasonal_trend_short: str = "unclear"
    seasonal_trend_long: str = "unclear"
    content_efficiency_signal: float = 0.0
    content_efficiency_source: str = "missing"
    product_age_days: Optional[int] = None
    age_bucket: str = ""
    listing_date_parse_status: str = ""
    market_id: str = ""
    category_id: str = ""
    market_category_profile_version: str = ""
    default_content_route_preference: str = ""

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["snapshot"] = self.snapshot.to_dict()
        payload["tag"] = self.tag.to_dict()
        return payload


@dataclass
class MarketDirectionCard:
    direction_canonical_key: str
    direction_instance_id: str
    batch_date: str
    country: str
    category: str
    direction_name: str
    style_cluster: str
    direction_family: str = ""
    direction_item_count: int = 0
    direction_sales_median_7d: float = 0.0
    direction_video_density_avg: float = 0.0
    direction_creator_density_avg: float = 0.0
    direction_tier: str = ""
    seasonal_trend: str = "unclear"
    seasonal_trend_short: str = "unclear"
    seasonal_trend_long: str = "unclear"
    content_efficiency_signal: float = 0.0
    content_efficiency_source: str = "missing"
    confidence_sample_score: int = 0
    confidence_consistency_score: int = 0
    confidence_completeness_score: int = 0
    decision_confidence: str = "low"
    confidence_reason_tags: List[str] = field(default_factory=list)
    sample_count: int = 0
    sample_confidence: str = ""
    primary_opportunity_type: str = ""
    risk_tags: List[str] = field(default_factory=list)
    decision_action: str = ""
    default_action_by_type: str = ""
    actual_action: str = ""
    action_override: Dict[str, Any] = field(default_factory=dict)
    action_overrides: List[Dict[str, Any]] = field(default_factory=list)
    action_decision: Dict[str, Any] = field(default_factory=dict)
    demand_structure: Dict[str, Any] = field(default_factory=dict)
    competition_structure: Dict[str, Any] = field(default_factory=dict)
    price_band_analysis: Dict[str, Any] = field(default_factory=dict)
    product_age_structure: Dict[str, Any] = field(default_factory=dict)
    new_product_entry_signal: Dict[str, Any] = field(default_factory=dict)
    observe_reason: List[str] = field(default_factory=list)
    our_capability_fit: Dict[str, Any] = field(default_factory=dict)
    recommended_execution: Dict[str, Any] = field(default_factory=dict)
    scale_condition: List[Dict[str, Any]] = field(default_factory=list)
    stop_loss_condition: List[Dict[str, Any]] = field(default_factory=list)
    batch_comparison: Dict[str, Any] = field(default_factory=dict)
    alert: Dict[str, Any] = field(default_factory=dict)
    top_forms: List[str] = field(default_factory=list)
    form_distribution: Dict[str, float] = field(default_factory=dict)
    form_distribution_by_count: Dict[str, float] = field(default_factory=dict)
    form_distribution_by_sales: Dict[str, float] = field(default_factory=dict)
    top_silhouette_forms: List[str] = field(default_factory=list)
    top_length_forms: List[str] = field(default_factory=list)
    silhouette_distribution_by_count: Dict[str, float] = field(default_factory=dict)
    silhouette_distribution_by_sales: Dict[str, float] = field(default_factory=dict)
    length_distribution_by_count: Dict[str, float] = field(default_factory=dict)
    length_distribution_by_sales: Dict[str, float] = field(default_factory=dict)
    core_elements: List[str] = field(default_factory=list)
    scene_tags: List[str] = field(default_factory=list)
    target_price_bands: List[str] = field(default_factory=list)
    heat_level: str = "medium"
    crowd_level: str = "medium"
    top_value_points: List[str] = field(default_factory=list)
    default_content_route_preference: str = "neutral"
    representative_products: List[Dict[str, str]] = field(default_factory=list)
    priority_level: str = "medium"
    selection_advice: str = ""
    avoid_notes: str = ""
    confidence: float = 0.5
    product_count: int = 0
    market_id: str = ""
    category_id: str = ""
    market_category_profile_version: str = ""
    average_heat_score: float = 0.0
    average_crowd_score: float = 0.0
    direction_key: str = ""
    direction_execution_brief: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @property
    def style_main(self) -> str:
        return self.style_cluster

    @property
    def product_form_or_result(self) -> str:
        return self.top_forms[0] if self.top_forms else ""


@dataclass
class ShopLandscapeSummary:
    batch_date: str
    country: str
    category: str
    input_mode: str
    head_store_pattern: str
    new_product_share_level: str
    sku_structure_level: str
    creator_density_level: str
    competition_pattern: str
    summary_lines: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class VOCLightSummary:
    voc_status: str
    positive_needs: List[str] = field(default_factory=list)
    pain_points: List[str] = field(default_factory=list)
    objections: List[str] = field(default_factory=list)
    scene_keywords: List[str] = field(default_factory=list)
    top_user_words: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class MarketInsightRunResult:
    table_id: str
    table_name: str
    input_mode: str
    batch_date: str
    country: str
    category: str
    artifacts_dir: str
    source_scope: str = "official"
    product_snapshot_count: int = 0
    total_product_count: int = 0
    direction_count: int = 0
    is_consumable: bool = False
    valid_sample_count: int = 0
    invalid_sample_count: int = 0
    valid_sample_ratio: float = 0.0
    quality_gate_passed: bool = False
    quality_gate_reason: str = ""
    shop_summary_generated: bool = False
    voc_status: str = "skipped"
    run_status: str = "completed"
    report_json_path: str = ""
    report_md_path: str = ""
    report_delivery_path: str = ""
    report_doc_url: str = ""
    notification_status: str = ""
    llm_fallback_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class MarketInsightProductRunState:
    table_id: str
    table_name: str
    input_mode: str
    batch_date: str
    country: str
    category: str
    artifacts_dir: str
    source_scope: str
    min_consumable_product_count: int
    min_consumable_direction_count: int
    product_snapshot_path: str
    product_tags_path: str
    direction_cards_path: str
    report_json_path: str
    report_md_path: str
    report_delivery_path: str
    progress_json_path: str
    voc_status: str = "skipped"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class MarketDirectionMatchResult:
    matched_market_direction_id: str = ""
    matched_market_direction_name: str = ""
    matched_market_direction_reason: str = ""
    score: float = 0.0
    market_match_status: str = ""
    cards_available: bool = False
    style_cluster: str = ""
    direction_family: str = ""
    direction_tier: str = ""
    decision_confidence: str = ""
    default_content_route_preference: str = ""
    schema_version: str = ""
    contract_warning: str = ""
    candidate_directions: List[Dict[str, Any]] = field(default_factory=list)
    representative_products: List[Dict[str, Any]] = field(default_factory=list)
    direction_action: str = ""
    product_form: str = ""
    top_forms: List[str] = field(default_factory=list)
    top_silhouette_forms: List[str] = field(default_factory=list)
    top_length_forms: List[str] = field(default_factory=list)
    core_elements: List[str] = field(default_factory=list)
    scene_tags: List[str] = field(default_factory=list)
    target_price_bands: List[str] = field(default_factory=list)
    matched_terms: List[str] = field(default_factory=list)
    direction_execution_brief: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @property
    def style_main(self) -> str:
        return self.style_cluster

    @property
    def product_form_or_result(self) -> str:
        if self.product_form:
            return self.product_form
        return self.top_forms[0] if self.top_forms else ""
