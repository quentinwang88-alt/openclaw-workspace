#!/usr/bin/env python3
"""V2 数据模型。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


class ConfigError(ValueError):
    """表配置异常。"""


class HermesOutputError(ValueError):
    """Hermes 输出异常。"""


@dataclass
class StorePositioningCard:
    store_id: str = ""
    source_record_id: str = ""
    country: str = ""
    category: str = ""
    card_name: str = ""
    style_whitelist: List[str] = field(default_factory=list)
    style_blacklist: List[str] = field(default_factory=list)
    soft_style_blacklist: List[str] = field(default_factory=list)
    hard_style_blacklist: List[str] = field(default_factory=list)
    target_price_bands: List[str] = field(default_factory=list)
    core_scenes: List[str] = field(default_factory=list)
    content_tones: List[str] = field(default_factory=list)
    core_value_points: List[str] = field(default_factory=list)
    target_audience: List[str] = field(default_factory=list)
    selection_principles: List[str] = field(default_factory=list)
    notes: str = ""


@dataclass
class AccioSupplyResult:
    supply_check_status: str = "pending"
    supply_summary: str = ""
    source_url: str = ""
    procurement_price_rmb: Optional[float] = None


@dataclass
class DecisionReason:
    primary_drivers: List[str] = field(default_factory=list)
    secondary_drivers: List[str] = field(default_factory=list)
    supporting_factors: List[str] = field(default_factory=list)
    narrative: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "primary_drivers": list(self.primary_drivers),
            "secondary_drivers": list(self.secondary_drivers),
            "supporting_factors": list(self.supporting_factors),
            "narrative": self.narrative,
        }


@dataclass
class TableSourceConfig:
    feishu_url: str = ""
    app_token: str = ""
    bitable_table_id: str = ""


@dataclass
class ReadFilterConfig:
    status_field: str
    pending_values: List[str] = field(default_factory=lambda: ["待处理", ""])


@dataclass
class TableConfig:
    table_id: str
    table_name: str
    enabled: bool
    source_type: str
    supported_manual_categories: List[str]
    read_filter: ReadFilterConfig
    field_map: Dict[str, str]
    writeback_map: Dict[str, str]
    batch_field: str = ""
    source: TableSourceConfig = field(default_factory=TableSourceConfig)
    static_fields: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CandidateTask:
    source_table_id: str
    source_record_id: str
    source_type: str = "manual"
    batch_id: str = ""
    product_title: str = ""
    product_images: List[str] = field(default_factory=list)
    cost_price: Optional[float] = None
    target_price: Optional[float] = None
    manual_category: str = ""
    product_notes: str = ""
    competitor_notes: str = ""
    competitor_links: List[str] = field(default_factory=list)
    target_market: str = ""
    title_keyword_tags: List[str] = field(default_factory=list)
    title_category_hint: str = ""
    title_category_confidence: str = ""
    final_category: str = ""
    category_confidence: str = ""
    extra_fields: Dict[str, Any] = field(default_factory=dict)

    @property
    def product_name(self) -> str:
        return self.product_title


@dataclass
class TitleParseResult:
    title_keyword_tags: List[str]
    title_category_hint: str
    title_category_confidence: str


@dataclass
class CategoryIdentificationResult:
    predicted_category: str
    confidence: str
    reason: str


@dataclass
class FeatureAnalysisResult:
    analysis_category: str
    feature_scores: Dict[str, str]
    risk_tag: str
    risk_note: str
    brief_observation: str

    def to_dict(self) -> Dict[str, str]:
        payload = {"analysis_category": self.analysis_category}
        payload.update(self.feature_scores)
        payload["risk_tag"] = self.risk_tag
        payload["risk_note"] = self.risk_note
        payload["brief_observation"] = self.brief_observation
        return payload


@dataclass
class ScoredAnalysisResult:
    analysis_category: str
    product_potential: str
    content_potential: str
    batch_priority_score: float
    suggested_action: str
    brief_reason: str
    market_match_score: Optional[float] = 0.0
    market_match_status: str = ""
    store_fit_score: float = 0.0
    content_potential_score: float = 0.0
    core_score_a: float = 0.0
    route_a: str = ""
    core_score_b: float = 0.0
    route_b: str = ""
    supply_check_status: str = "pending"
    supply_summary: str = ""
    competition_reference_level: str = "medium"
    competition_confidence: str = "low"
    decision_reason: DecisionReason = field(default_factory=DecisionReason)
    needs_manual_review: bool = False
    manual_review_reason: str = ""
    observation_tags: List[str] = field(default_factory=list)
    recommended_content_formulas: List[str] = field(default_factory=list)
    reserve_reason: str = ""
    reserve_created_at: int = 0
    reserve_expires_at: int = 0
    reserve_status: str = ""
    sample_check_status: str = ""
    matched_market_direction_id: str = ""
    matched_market_direction_name: str = ""
    matched_market_direction_reason: str = ""
    matched_market_direction_confidence: str = ""
    matched_market_direction_family: str = ""
    matched_market_direction_tier: str = ""
    default_content_route_preference: str = ""
    v2_shadow_result: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PendingAnalysisItem:
    task: CandidateTask
    feature_result: FeatureAnalysisResult
    scored_result: ScoredAnalysisResult


@dataclass
class PrecheckResult:
    should_continue: bool
    terminal_status: Optional[str] = None
    terminal_reason: str = ""
