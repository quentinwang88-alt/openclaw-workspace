#!/usr/bin/env python3
"""
原创脚本生成业务常量。
"""

SEA_HOME_SCENE_MIN_COUNT = 2
SEA_VARIANT_HOME_SCENE_MIN_COUNT = 3

SEA_COUNTRIES = [
    "Thailand",
    "Vietnam",
    "Malaysia",
    "Indonesia",
    "Philippines",
    "Singapore",
]

SEA_COUNTRY_ALIASES = {
    "thailand": "Thailand",
    "thai": "Thailand",
    "泰国": "Thailand",
    "vietnam": "Vietnam",
    "vietnamese": "Vietnam",
    "越南": "Vietnam",
    "malaysia": "Malaysia",
    "malay": "Malaysia",
    "马来西亚": "Malaysia",
    "indonesia": "Indonesia",
    "indonesian": "Indonesia",
    "印度尼西亚": "Indonesia",
    "印尼": "Indonesia",
    "philippines": "Philippines",
    "filipino": "Philippines",
    "菲律宾": "Philippines",
    "singapore": "Singapore",
    "新加坡": "Singapore",
}

# 兼容旧引用。
SEA_COUNTRY_LIST = set(SEA_COUNTRY_ALIASES.keys())

SEA_HOME_PRIORITY_SCENES = [
    "H1 窗边自然光",
    "H2 镜前/玄关镜前",
    "H3 梳妆台/桌边",
    "H4 床边/坐姿分享",
    "H5 衣柜/穿衣区",
    "卧室镜前",
    "衣柜/穿衣区",
    "梳妆台/桌边",
    "床边",
    "客厅自然走动区域",
    "窗边自然光区域",
    "出门前玄关镜子",
]

HOME_SHARE_SCENE_KEYWORDS = [
    "H1",
    "H2",
    "H3",
    "H4",
    "H5",
    "家中",
    "家里",
    "家内",
    "自然分享",
    "卧室",
    "镜前",
    "镜子前",
    "衣柜",
    "穿衣区",
    "梳妆台",
    "桌边",
    "床边",
    "客厅",
    "窗边",
    "自然光",
    "玄关",
    "出门前",
    "分享感",
    "顺手分享",
    "home",
    "at home",
    "home share",
    "bedroom",
    "mirror",
    "closet",
    "dresser",
    "living room",
    "entryway",
    "window",
    "natural light",
]

SCRIPT_ROLES = [
    "cognitive_reframing",
    "result_delivery",
    "risk_resolution",
    "aura_enhancement",
]

SCRIPT_ROLE_DEFAULTS_BY_STRATEGY = {
    "S1": "cognitive_reframing",
    "S2": "aura_enhancement",
    "S3": "risk_resolution",
    "S4": "result_delivery",
}
