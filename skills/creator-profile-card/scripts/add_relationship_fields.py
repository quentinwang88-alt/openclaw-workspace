#!/usr/bin/env python3
"""往现有达人画像卡表中新增 14 个关系运营字段（V1.1 达人关系运营闭环）。"""
import json, os, sys, requests
from pathlib import Path

# ── 飞书凭证 ──
try:
    from core.bitable import _load_openclaw_config, get_tenant_access_token
    cfg = _load_openclaw_config()
except Exception:
    cfg = json.loads(Path("/sessions/serene-gallant-fermat/mnt/.openclaw/openclaw.json").read_text())

feishu = cfg["channels"]["feishu"]
resp = requests.post(
    "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
    json={"app_id": feishu["appId"], "app_secret": feishu["appSecret"]},
    timeout=30,
)
TOKEN = resp.json()["tenant_access_token"]
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# ── 解析 wiki token → app_token ──
WIKI_TOKEN = "GNaHw1xM9ik7tDkBS6Kcfdf8nwg"
TABLE_ID = "tbluyKELrrCc5qPT"
resp = requests.get(
    "https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node",
    headers=HEADERS, params={"token": WIKI_TOKEN}, timeout=30,
)
APP_TOKEN = resp.json()["data"]["node"]["obj_token"]
print(f"App Token: {APP_TOKEN}")

# ── 查询已有字段 ──
resp = requests.get(
    f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/fields",
    headers=HEADERS, params={"page_size": 100}, timeout=30,
)
existing = set()
for item in resp.json().get("data", {}).get("items", []):
    existing.add(item.get("field_name", ""))
print(f"已有字段 ({len(existing)}): {sorted(existing)}")

# ── 14 个新字段定义 ──
NEW_FIELDS = [
    # 达人层级
    {"field_name": "达人层级", "type": 3, "ui_type": "SingleSelect", "property": {"options": [
        {"name": "A 类", "color": 0}, {"name": "B 类", "color": 1},
        {"name": "C 类", "color": 2}, {"name": "D 类", "color": 3},
    ]}},
    # 关系阶段
    {"field_name": "关系阶段", "type": 3, "ui_type": "SingleSelect", "property": {"options": [
        {"name": "冷", "color": 0}, {"name": "温", "color": 1},
        {"name": "热", "color": 2}, {"name": "合作中", "color": 3},
        {"name": "冷却", "color": 4}, {"name": "放弃", "color": 5},
    ]}},
    # 上次联系时间
    {"field_name": "上次联系时间", "type": 5, "ui_type": "DateTime"},
    # 上次联系类型
    {"field_name": "上次联系类型", "type": 3, "ui_type": "SingleSelect", "property": {"options": [
        {"name": "关系维护", "color": 0}, {"name": "商品邀约", "color": 1},
        {"name": "轻跟进", "color": 2}, {"name": "人工消息", "color": 3},
        {"name": "无", "color": 4},
    ]}},
    # 上次回复时间
    {"field_name": "上次回复时间", "type": 5, "ui_type": "DateTime"},
    # 最新回复状态
    {"field_name": "最新回复状态", "type": 3, "ui_type": "SingleSelect", "property": {"options": [
        {"name": "未回复", "color": 0}, {"name": "已读未回", "color": 1},
        {"name": "普通回复", "color": 2}, {"name": "感兴趣", "color": 3},
        {"name": "拒绝", "color": 4}, {"name": "无效回复", "color": 5},
    ]}},
    # 连续未回复次数
    {"field_name": "连续未回复次数", "type": 2, "ui_type": "Number"},
    # 下次可联系时间
    {"field_name": "下次可联系时间", "type": 5, "ui_type": "DateTime"},
    # 本周建议动作
    {"field_name": "本周建议动作", "type": 3, "ui_type": "SingleSelect", "property": {"options": [
        {"name": "关系维护", "color": 0}, {"name": "商品邀约", "color": 1},
        {"name": "轻跟进", "color": 2}, {"name": "暂缓", "color": 3},
        {"name": "放弃", "color": 4}, {"name": "人工查看", "color": 5},
    ]}},
    # 本周建议原因
    {"field_name": "本周建议原因", "type": 1, "ui_type": "Text"},
    # 处理状态
    {"field_name": "处理状态", "type": 3, "ui_type": "SingleSelect", "property": {"options": [
        {"name": "待处理", "color": 0}, {"name": "已发送", "color": 1},
        {"name": "已暂缓", "color": 2}, {"name": "已放弃", "color": 3},
        {"name": "需人工查看", "color": 4}, {"name": "已回复", "color": 5},
    ]}},
    # 本次话术草稿
    {"field_name": "本次话术草稿", "type": 1, "ui_type": "Text"},
    # 选定商品
    {"field_name": "选定商品", "type": 1, "ui_type": "Text"},
    # 发送结果
    {"field_name": "发送结果", "type": 3, "ui_type": "SingleSelect", "property": {"options": [
        {"name": "未发送", "color": 0}, {"name": "已发送", "color": 1},
        {"name": "修改后发送", "color": 2}, {"name": "不发送", "color": 3},
    ]}},
]

# ── 创建字段 ──
created, skipped, failed = [], [], []
for field in NEW_FIELDS:
    name = field["field_name"]
    if name in existing:
        skipped.append(name)
        continue
    payload = {"field_name": name, "type": field["type"], "ui_type": field["ui_type"]}
    if "property" in field:
        payload["property"] = field["property"]
    resp = requests.post(
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/fields",
        headers=HEADERS, json=payload, timeout=30,
    )
    data = resp.json()
    if data.get("code") == 0:
        created.append(name)
        print(f"  ✅ {name}")
    else:
        failed.append((name, data.get("msg", str(data))))
        print(f"  ❌ {name}: {data.get('msg', data)}")

print(f"\n📊 结果: 新建 {len(created)} / 跳过 {len(skipped)} / 失败 {len(failed)}")
if created:
    print(f"   新建: {', '.join(created)}")
if skipped:
    print(f"   已存在: {', '.join(skipped)}")
if failed:
    for n, e in failed:
        print(f"   失败: {n} → {e}")

print(f"\n📝 表地址: https://gcngopvfvo0q.feishu.cn/base/{APP_TOKEN}?table={TABLE_ID}")
