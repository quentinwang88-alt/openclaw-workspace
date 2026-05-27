#!/usr/bin/env python3
"""为 AI片段生成任务表 批量创建字段。"""
from __future__ import annotations

import sys
from pathlib import Path

WORKSPACE = Path("/Users/likeu3/.openclaw/workspace")
sys.path.insert(0, str(WORKSPACE / "skills" / "script-run-manager-sync"))

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # noqa: E402

FEISHU_URL = "https://gcngopvfvo0q.feishu.cn/wiki/WyNuwaTThiI3NDk7qyccflrunGe?table=tblVWMXmsAiA6DZV&view=vewCjgjw3s"

info = parse_feishu_bitable_url(FEISHU_URL)
if not info:
    raise ValueError(f"无法解析飞书URL: {FEISHU_URL}")

app_token = resolve_wiki_bitable_app_token(info.app_token)
print(f"resolved app_token: {app_token}, table_id: {info.table_id}")

client = FeishuBitableClient(app_token=app_token, table_id=info.table_id)

existing = set(client.list_field_names())
print(f"existing fields: {existing}")

FIELDS = [
    ("商品ID", 1, "Text"),
    ("市场", 3, "SingleSelect", {"options": [{"name": "VN"}, {"name": "TH"}, {"name": "MY"}, {"name": "PH"}, {"name": "ID"}]}),
    ("类目", 3, "SingleSelect", {"options": [{"name": "发饰"}, {"name": "耳饰"}, {"name": "女装轻上装"}, {"name": "围巾帽子"}, {"name": "小饰品"}]}),
    ("片段类型", 3, "SingleSelect", {
        "options": [
            {"name": "商品桌面展示"},
            {"name": "手拿商品"},
            {"name": "细节氛围"},
            {"name": "佩戴/上身效果"},
            {"name": "镜前整理"},
            {"name": "居家生活场景"},
            {"name": "出门前场景"},
            {"name": "季节/场景氛围"},
        ]
    }),
    ("生成数量", 2, "Number"),
    ("状态", 3, "SingleSelect", {
        "options": [
            {"name": "待生成"},
            {"name": "锚点检查中"},
            {"name": "提示词生成中"},
            {"name": "生成中"},
            {"name": "生成完成"},
            {"name": "入库中"},
            {"name": "质检中"},
            {"name": "锚点判定中"},
            {"name": "镜位计算中"},
            {"name": "部分可用"},
            {"name": "全部可用"},
            {"name": "全部失败"},
            {"name": "已入库"},
            {"name": "商品锚点缺失"},
            {"name": "参考图不足"},
            {"name": "生成失败"},
            {"name": "上传失败"},
            {"name": "质检失败"},
            {"name": "锚点判定失败"},
            {"name": "入库失败"},
        ]
    }),
    ("参考商品图", 17, "Attachment"),
    ("场景偏好", 3, "MultiSelect", {
        "options": [
            {"name": "卧室"},
            {"name": "梳妆台"},
            {"name": "镜前"},
            {"name": "户外"},
            {"name": "空调房"},
            {"name": "玄关"},
            {"name": "衣柜前"},
            {"name": "窗边自然光"},
            {"name": "咖啡店"},
            {"name": "商场"},
            {"name": "旅行街景"},
        ]
    }),
    ("风格偏好", 3, "SingleSelect", {
        "options": [
            {"name": "日常"},
            {"name": "轻甜"},
            {"name": "高级"},
            {"name": "温暖"},
            {"name": "清爽"},
            {"name": "复古"},
            {"name": "简约"},
        ]
    }),
    ("人物要求", 1, "Text"),
    ("备注", 1, "Text"),
    ("生成任务ID", 1, "Text"),
    ("商品锚点状态", 3, "SingleSelect", {"options": [{"name": "已确认"}, {"name": "缺失"}]}),
    ("Prompt版本", 1, "Text"),
    ("实际生成数量", 2, "Number"),
    ("通过质检数量", 2, "Number"),
    ("入库片段数量", 2, "Number"),
    ("严格通过数量", 2, "Number"),
    ("宽松通过数量", 2, "Number"),
    ("未通过数量", 2, "Number"),
    ("失败原因", 1, "Text"),
    ("生成结果预览", 17, "Attachment"),
    ("最近处理时间", 5, "DateTime"),
]

for field_name, field_type, ui_type, *rest in FIELDS:
    if field_name in existing:
        print(f"  SKIP {field_name} (already exists)")
        continue
    prop = rest[0] if rest else None
    try:
        client.create_field(field_name, field_type=field_type, ui_type=ui_type, property=prop)
        print(f"  OK {field_name} (type={field_type}, ui={ui_type})")
    except Exception as exc:
        print(f"  FAIL {field_name}: {exc}")

print("\ndone. current fields:")
for f in client.list_fields():
    print(f"  {f.field_name}: type={f.field_type}, ui={f.ui_type}")
