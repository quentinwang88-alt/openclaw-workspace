#!/usr/bin/env python3
"""Display-name helpers for market insight reports."""

from __future__ import annotations

from typing import Iterable


DISPLAY_MAPS = {
    "confidence": {
        "high": "高",
        "medium": "中",
        "low": "低",
        "insufficient": "不足",
    },
    "new_product_entry_signal": {
        "strong_new_entry": "新品进入信号强",
        "moderate_new_entry": "新品进入信号中等",
        "weak_new_entry": "新品进入信号弱",
        "old_product_dominated": "老品占位明显",
        "noisy_new_supply": "新品供给虚热",
        "few_new_winners": "少数新品赢家",
        "unknown": "信号不明确",
    },
    "risk_tag": {
        "aesthetic_homogeneous": "审美同质化",
        "crowded_direction": "方向拥挤",
        "form_concentration": "承载形态集中",
        "high_video_density": "视频密度偏高",
        "high_creator_density": "达人密度偏高",
        "head_concentrated": "头部集中",
        "sales_distribution_skew": "销量分布偏斜",
        "old_product_dominated": "老品占位明显",
        "few_new_winners": "少数新品赢家",
        "noisy_new_supply": "新品供给虚热",
        "low_sample": "样本偏少",
        "price_band_insufficient": "价格带样本不足",
        "sourcing_fit_low": "供应链匹配偏低",
        "age_data_insufficient": "上架时间数据不足",
        "weak_new_entry_window": "新品进入窗口偏弱",
        "new_entry_signal_unclear": "新品进入信号不明确",
        "weak_conversion_signal": "成交信号偏弱",
        "small_sample_top3_share_high": "小样本Top3占比偏高",
        "adjusted_head_concentration_risk": "修正后头部集中风险",
        "differentiation_angle_insufficient": "差异化切口不足",
        "study_top_capacity_limited": "头部拆解名额已满",
        "high_median_sales": "销量中位数高",
        "high_over_threshold_ratio": "超过行动阈值占比高",
        "local_scene_fit": "本地场景匹配",
    },
    "capability": {
        "high": "高",
        "medium": "中",
        "low": "低",
        "unknown": "未知",
    },
}


def display_enum(value: str, enum_type: str) -> str:
    text = str(value or "")
    return DISPLAY_MAPS.get(enum_type, {}).get(text, text or "待补")


def display_list(values: Iterable[str], enum_type: str) -> str:
    labels = [display_enum(str(value), enum_type) for value in values if str(value or "").strip()]
    return "、".join(labels) if labels else "无"
