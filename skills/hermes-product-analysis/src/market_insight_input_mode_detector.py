#!/usr/bin/env python3
"""Detect Market Insight input mode from headers."""

from __future__ import annotations

from typing import Iterable, Set


PRODUCT_MODE = "product_ranking"
SHOP_MODE = "shop_ranking"
UNKNOWN_MODE = "unknown"

PRODUCT_HEADERS = {
    "商品名称",
    "商品图片",
    "图片",
    "商品链接",
    "TikTok商品落地页地址",
    "FastMoss商品详情页地址",
    "7天销量",
    "7天销售额",
}

SHOP_HEADERS = {
    "店铺名称",
    "店铺定位",
    "近7天销量",
    "近7天销售额",
    "近7天动销商品",
    "在售商品数",
    "新品成交占比",
    "带货达人数",
}


def detect_input_mode(field_names: Iterable[str]) -> str:
    normalized: Set[str] = {str(item or "").strip() for item in field_names if str(item or "").strip()}
    product_hits = len(normalized & PRODUCT_HEADERS)
    shop_hits = len(normalized & SHOP_HEADERS)

    if product_hits >= 2:
        return PRODUCT_MODE
    if shop_hits >= 2:
        return SHOP_MODE
    return UNKNOWN_MODE
