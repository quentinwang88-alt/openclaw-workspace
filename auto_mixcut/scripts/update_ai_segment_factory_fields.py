#!/usr/bin/env python3
"""补全 AI片段生成任务表 字段枚举值（只更新 property，不改 type）。"""
from __future__ import annotations

import sys
from pathlib import Path

WORKSPACE = Path("/Users/likeu3/.openclaw/workspace")
sys.path.insert(0, str(WORKSPACE / "skills" / "script-run-manager-sync"))

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # noqa: E402

FEISHU_URL = "https://gcngopvfvo0q.feishu.cn/wiki/WyNuwaTThiI3NDk7qyccflrunGe?table=tblVWMXmsAiA6DZV&view=vewCjgjw3s"

info = parse_feishu_bitable_url(FEISHU_URL)
app_token = resolve_wiki_bitable_app_token(info.app_token)
client = FeishuBitableClient(app_token=app_token, table_id=info.table_id)

fields = {f.field_name: f for f in client.list_fields()}

# 场景偏好 was deleted, recreate as MultiSelect
if "场景偏好" not in fields:
    print("Recreating 场景偏好 as MultiSelect...")
    client.create_field("场景偏好", field_type=3, ui_type="MultiSelect", property={"options": [
        {"name": "卧室"}, {"name": "梳妆台"}, {"name": "镜前"}, {"name": "户外"},
        {"name": "空调房"}, {"name": "玄关"}, {"name": "衣柜前"}, {"name": "窗边自然光"},
        {"name": "咖啡店"}, {"name": "商场"}, {"name": "旅行街景"},
    ]})
    fields = {f.field_name: f for f in client.list_fields()}

# Only update property (options), not field type
OPTIONS_MAP = {
    "市场": [
        {"name": "VN"}, {"name": "TH"}, {"name": "MY"}, {"name": "PH"}, {"name": "ID"},
    ],
    "类目": [
        {"name": "发饰"}, {"name": "耳饰"}, {"name": "女装轻上装"},
        {"name": "围巾帽子"}, {"name": "小饰品"},
    ],
    "片段类型": [
        {"name": "商品桌面展示"}, {"name": "手拿商品"}, {"name": "细节氛围"},
        {"name": "佩戴/上身效果"}, {"name": "镜前整理"}, {"name": "居家生活场景"},
        {"name": "出门前场景"}, {"name": "季节/场景氛围"},
    ],
    "状态": [
        {"name": "待生成"}, {"name": "锚点检查中"}, {"name": "提示词生成中"},
        {"name": "生成中"}, {"name": "生成完成"}, {"name": "入库中"},
        {"name": "质检中"}, {"name": "锚点判定中"}, {"name": "镜位计算中"},
        {"name": "部分可用"}, {"name": "全部可用"}, {"name": "全部失败"}, {"name": "已入库"},
        {"name": "商品锚点缺失"}, {"name": "参考图不足"},
        {"name": "生成失败"}, {"name": "上传失败"},
        {"name": "质检失败"}, {"name": "锚点判定失败"}, {"name": "入库失败"},
    ],
    "风格偏好": [
        {"name": "日常"}, {"name": "轻甜"}, {"name": "高级"}, {"name": "温暖"},
        {"name": "清爽"}, {"name": "简约"},
    ],
    "商品锚点状态": [
        {"name": "已确认"}, {"name": "缺失"},
    ],
}

for field_name, options in OPTIONS_MAP.items():
    if field_name not in fields:
        print(f"  SKIP {field_name} (not found)")
        continue
    f = fields[field_name]
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{info.table_id}/fields/{f.field_id}"
    payload = {
        "field_name": field_name,
        "type": f.field_type,
        "ui_type": f.ui_type,
        "property": {"options": options},
    }
    resp = client._request("PUT", url, headers=client._headers(), json=payload)
    result = resp.json()
    if result.get("code") == 0:
        print(f"  OK {field_name}: {len(options)} options")
    else:
        print(f"  FAIL {field_name}: {result.get('msg')}")

print("\nFinal state:")
for f in client.list_fields():
    opts = []
    if f.property and "options" in f.property:
        opts = [o.get("name", "?") for o in f.property["options"]]
    opt_str = ", ".join(opts[:5]) + ("..." if len(opts) > 5 else "")
    print(f"  {f.field_name} ({f.ui_type}): {opt_str}")
