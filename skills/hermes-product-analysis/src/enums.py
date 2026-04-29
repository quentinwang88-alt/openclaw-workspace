#!/usr/bin/env python3
"""V2 枚举定义。"""

from __future__ import annotations

from enum import Enum


class AnalysisStatus(str, Enum):
    PENDING = "待处理"
    INSUFFICIENT_INFO = "信息不足"
    NEED_MANUAL_CATEGORY = "待人工确认类目"
    UNSUPPORTED_CATEGORY = "当前类目不支持"
    COMPLETED = "已完成分析"
    FAILED = "分析失败"


class SupportedCategory(str, Enum):
    HAIR_ACCESSORY = "发饰"
    LIGHT_TOPS = "轻上装"


class PredictedCategory(str, Enum):
    HAIR_ACCESSORY = "发饰"
    LIGHT_TOPS = "轻上装"
    OTHER = "其他"
    UNKNOWN = "无法判断"


class HermesConfidence(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class FeatureLevel(str, Enum):
    HIGH = "高"
    MEDIUM = "中"
    LOW = "低"


class PotentialLevel(str, Enum):
    HIGH = "高"
    MEDIUM = "中"
    LOW = "低"


class SuggestedAction(str, Enum):
    PRIORITY_TEST = "优先测试"
    LOW_COST_TEST = "低成本试款"
    RESERVE = "先放备用池"
    NEED_MORE_INFO = "补信息后再看"
    HOLD = "暂不建议推进"


class DisplayAnalysisStatus(str, Enum):
    IN_PROGRESS = "分析中"
    COMPLETED = "已完成"
    FAILED = "异常中断"


class MarketMatchStatus(str, Enum):
    MATCHED = "matched"
    WEAK_MATCHED = "weak_matched"
    UNCOVERED = "uncovered"


SUPPORTED_CATEGORIES = {item.value for item in SupportedCategory}
PREDICTED_CATEGORIES = {item.value for item in PredictedCategory}
HERMES_CONFIDENCE_VALUES = {item.value for item in HermesConfidence}
WRITEBACK_CONFIDENCE_VALUES = set(HERMES_CONFIDENCE_VALUES)
WRITEBACK_CONFIDENCE_VALUES.add("manual")
FEATURE_LEVEL_VALUES = {item.value for item in FeatureLevel}
POTENTIAL_LEVEL_VALUES = {item.value for item in PotentialLevel}
SUGGESTED_ACTION_VALUES = {item.value for item in SuggestedAction}
DISPLAY_ANALYSIS_STATUS_VALUES = {item.value for item in DisplayAnalysisStatus}
MARKET_MATCH_STATUS_VALUES = {item.value for item in MarketMatchStatus}

HAIR_FEATURE_FIELDS = [
    "wearing_change_strength",
    "demo_ease",
    "visual_memory_point",
    "homogenization_risk",
    "title_selling_clarity",
    "info_completeness",
]

LIGHT_TOP_FEATURE_FIELDS = [
    "upper_body_change_strength",
    "camera_readability",
    "design_signal_strength",
    "basic_style_escape_strength",
    "title_selling_clarity",
    "info_completeness",
]

FEATURE_FIELDS_BY_CATEGORY = {
    SupportedCategory.HAIR_ACCESSORY.value: HAIR_FEATURE_FIELDS,
    SupportedCategory.LIGHT_TOPS.value: LIGHT_TOP_FEATURE_FIELDS,
}

HAIR_RISK_TAG_VALUES = {
    "同质化偏高",
    "戴上效果不够直观",
    "演示场景较弱",
    "视觉记忆点不足",
    "图片信息不足",
    "标题卖点不清",
    "价格支撑不足",
    "无明显主要风险",
}

LIGHT_TOP_RISK_TAG_VALUES = {
    "同质化偏高",
    "真人上身依赖强",
    "镜头识别度弱",
    "设计点不够集中",
    "脱离基础款能力弱",
    "图片信息不足",
    "标题卖点不清",
    "价格支撑不足",
    "无明显主要风险",
}

RISK_TAG_VALUES_BY_CATEGORY = {
    SupportedCategory.HAIR_ACCESSORY.value: HAIR_RISK_TAG_VALUES,
    SupportedCategory.LIGHT_TOPS.value: LIGHT_TOP_RISK_TAG_VALUES,
}

UNION_RISK_TAG_VALUES = sorted(HAIR_RISK_TAG_VALUES | LIGHT_TOP_RISK_TAG_VALUES)

FEATURE_LEVEL_TO_SCORE = {
    FeatureLevel.HIGH.value: 100,
    FeatureLevel.MEDIUM.value: 60,
    FeatureLevel.LOW.value: 20,
}

REVERSED_FEATURE_LEVEL_TO_SCORE = {
    FeatureLevel.HIGH.value: 20,
    FeatureLevel.MEDIUM.value: 60,
    FeatureLevel.LOW.value: 100,
}
