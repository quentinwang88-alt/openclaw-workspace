#!/usr/bin/env python3
"""Configuration for the standalone hair style review skill."""

from __future__ import annotations

DEFAULT_FEISHU_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/"
    "CtGxwJpTEifSh5kIVtgcM2vCnLf?table=tblKhPn64Q266tRz&view=vewmWdRUHq"
)

DEFAULT_TIMEZONE = "Asia/Shanghai"
DEFAULT_MAX_WORKERS = 2
DEFAULT_MAX_TOKENS = 1200

TITLE_FIELD_ALIASES = ("商品标题", "商品名称", "标题")
IMAGE_FIELD_ALIASES = ("商品图片", "图片", "主图")
BASIC_INFO_FIELD_ALIASES = ("商品基础信息", "基础信息", "商品信息", "商品基础资料", "备注")
CANDIDATE_FIELD_ALIASES = ("是否纳入备选", "是否列入备选", "纳入备选")

OUTPUT_STYLE_FIELD = "产品风格"
OUTPUT_RECOMMEND_FIELD = "是否推荐"
OUTPUT_REASON_FIELD = "详细原因"

STATUS_FIELD = "风格分析状态"
STATUS_TIME_FIELD = "风格分析时间"
STATUS_ERROR_FIELD = "风格分析错误信息"

RECOMMENDED_STYLE_OPTIONS = (
    "轻韩系基础温柔风",
    "韩系功能风",
    "轻韩系甜美风",
    "简约通勤风",
    "轻精致风",
    "重甜少女风",
    "儿童幼态风",
    "重礼物感/重拍照风",
    "夸张个性设计风",
    "其他",
)
