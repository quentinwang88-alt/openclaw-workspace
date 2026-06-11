#!/usr/bin/env python3
"""往已有飞书多维表格添加 9 个达人画像卡字段。"""
import json
import os
import sys
from pathlib import Path

import requests

# ── 从 openclaw.json 或环境变量读取飞书凭证 ──
FEISHU_APP_ID = os.environ.get("FEISHU_APP_ID", "")
FEISHU_APP_SECRET = os.environ.get("FEISHU_APP_SECRET", "")

if not FEISHU_APP_ID or not FEISHU_APP_SECRET:
    config_path = Path.home() / ".openclaw" / "openclaw.json"
    if config_path.exists():
        cfg = json.loads(config_path.read_text())
        feishu_ch = cfg.get("channels", {}).get("feishu", {})
        FEISHU_APP_ID = FEISHU_APP_ID or feishu_ch.get("appId", "")
        FEISHU_APP_SECRET = FEISHU_APP_SECRET or feishu_ch.get("appSecret", "")

if not FEISHU_APP_ID or not FEISHU_APP_SECRET:
    print("❌ 未找到飞书凭证，请设置 FEISHU_APP_ID / FEISHU_APP_SECRET 环境变量或在 ~/.openclaw/openclaw.json 中配置")
    sys.exit(1)

# ── 获取 tenant access token ──
resp = requests.post(
    "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
    json={"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET},
)
token_data = resp.json()
if token_data.get("code") != 0:
    print(f"❌ 获取 token 失败: {token_data}")
    sys.exit(1)
ACCESS_TOKEN = token_data["tenant_access_token"]
HEADERS = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"}
print(f"✅ 已获取 tenant access token")

# ── 目标表信息 ──
TARGET_URL = "https://gcngopvfvo0q.feishu.cn/wiki/GNaHw1xM9ik7tDkBS6Kcfdf8nwg?table=tbluyKELrrCc5qPT&view=vewK4DXTtH"
WIKI_TOKEN = "GNaHw1xM9ik7tDkBS6Kcfdf8nwg"
TABLE_ID = "tbluyKELrrCc5qPT"

# ── 解析 wiki token → app_token ──
resp = requests.get(
    f"https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node",
    headers=HEADERS,
    params={"token": WIKI_TOKEN},
)
wiki_data = resp.json()
if wiki_data.get("code") != 0:
    print(f"❌ 解析 wiki 失败: {wiki_data}")
    sys.exit(1)
APP_TOKEN = wiki_data["data"]["node"]["obj_token"]
print(f"✅ App Token: {APP_TOKEN}")

# ── 查询已有字段 ──
resp = requests.get(
    f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/fields",
    headers=HEADERS,
    params={"page_size": 100},
)
fields_data = resp.json()
existing_names = set()
if fields_data.get("code") == 0:
    for item in fields_data.get("data", {}).get("items", []):
        existing_names.add(item.get("field_name", ""))
print(f"📋 已有字段: {existing_names}")

# ── 9 字段定义 ──
FIELDS = [
    {"field_name": "达人链接", "type": 15, "ui_type": "Url"},
    {"field_name": "历史关系", "type": 3, "ui_type": "SingleSelect", "property": {
        "options": [
            {"name": "出过单", "color": 0},
            {"name": "发过视频", "color": 1},
            {"name": "申请过样品", "color": 2},
            {"name": "聊过未合作", "color": 3},
            {"name": "陌生", "color": 4},
        ]
    }},
    {"field_name": "活跃度", "type": 3, "ui_type": "SingleSelect", "property": {
        "options": [
            {"name": "高", "color": 0},
            {"name": "中", "color": 1},
            {"name": "低", "color": 2},
            {"name": "停更", "color": 3},
        ]
    }},
    {"field_name": "内容类型", "type": 3, "ui_type": "SingleSelect", "property": {
        "options": [
            {"name": "穿搭", "color": 0},
            {"name": "妆发", "color": 1},
            {"name": "首饰试戴", "color": 2},
            {"name": "好物分享", "color": 3},
            {"name": "居家生活", "color": 4},
            {"name": "口播种草", "color": 5},
            {"name": "直播切片", "color": 6},
            {"name": "其他", "color": 7},
        ]
    }},
    {"field_name": "画面风格", "type": 3, "ui_type": "SingleSelect", "property": {
        "options": [
            {"name": "自拍近景", "color": 0},
            {"name": "镜前半身", "color": 1},
            {"name": "全身穿搭", "color": 2},
            {"name": "桌面展示", "color": 3},
            {"name": "家中生活流", "color": 4},
            {"name": "户外街拍", "color": 5},
            {"name": "直播间感", "color": 6},
        ]
    }},
    {"field_name": "适配类目", "type": 4, "ui_type": "MultiSelect", "property": {
        "options": [
            {"name": "发饰", "color": 0},
            {"name": "耳饰", "color": 1},
            {"name": "项链", "color": 2},
            {"name": "围巾", "color": 3},
            {"name": "帽子", "color": 4},
            {"name": "轻上装", "color": 5},
            {"name": "女装", "color": 6},
            {"name": "暂无", "color": 7},
        ]
    }},
    {"field_name": "推荐商品/品类", "type": 1, "ui_type": "Text"},
    {"field_name": "沟通切入点", "type": 1, "ui_type": "Text"},
    {"field_name": "当前动作", "type": 3, "ui_type": "SingleSelect", "property": {
        "options": [
            {"name": "精准沟通", "color": 0},
            {"name": "半自动沟通", "color": 1},
            {"name": "暂缓", "color": 2},
            {"name": "放弃", "color": 3},
        ]
    }},
]

# ── 创建字段 ──
created = []
skipped = []
for field in FIELDS:
    name = field["field_name"]
    if name in existing_names:
        skipped.append(name)
        continue
    payload = {
        "field_name": field["field_name"],
        "type": field["type"],
        "ui_type": field["ui_type"],
    }
    if "property" in field:
        payload["property"] = field["property"]
    resp = requests.post(
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/fields",
        headers=HEADERS,
        json=payload,
    )
    data = resp.json()
    if data.get("code") == 0:
        created.append(name)
        print(f"  ✅ 已创建: {name}")
    else:
        print(f"  ❌ 创建失败: {name} → {data.get('msg', data)}")

print(f"\n📊 结果: 新建 {len(created)} 个, 跳过 {len(skipped)} 个")
if created:
    print(f"   新建: {', '.join(created)}")
if skipped:
    print(f"   已存在: {', '.join(skipped)}")

# ── 输出配置信息 ──
print(f"\n📝 请在 .env 或环境变量中配置:")
print(f"   CREATOR_PROFILE_FEISHU_APP_TOKEN={APP_TOKEN}")
print(f"   CREATOR_PROFILE_FEISHU_TABLE_ID={TABLE_ID}")
print(f"\n   表格地址: https://gcngopvfvo0q.feishu.cn/base/{APP_TOKEN}?table={TABLE_ID}")
