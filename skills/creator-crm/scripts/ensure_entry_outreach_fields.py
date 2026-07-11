#!/usr/bin/env python3
"""Ensure lightweight outreach fields exist on the entry-screen Feishu table."""

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import requests

SKILL_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SKILL_DIR))

from core.feishu_reader import FeishuBitableReader  # noqa: E402
from run_pipeline import resolve_feishu_config  # noqa: E402


DEFAULT_FEISHU_URL = (
    "https://gcngopvfvo0q.feishu.cn/base/"
    "FdzGbM1b4aXG2zsr6rncFWEln3g?table=tbluJnxKyXquWcEC&view=vewPrkWWaW"
)


def select_options(names: List[str]) -> Dict[str, Any]:
    return {"options": [{"name": name, "color": idx % 8} for idx, name in enumerate(names)]}


FIELDS: List[Dict[str, Any]] = [
    {"field_name": "建联批次号", "type": 1, "ui_type": "Text"},
    {"field_name": "计划建联产品", "type": 1, "ui_type": "Text"},
    {"field_name": "计划建联产品类目", "type": 1, "ui_type": "Text"},
    {"field_name": "计划建联产品卖点", "type": 1, "ui_type": "Text"},
    {"field_name": "批量建联话术", "type": 1, "ui_type": "Text"},
    {"field_name": "批量建联话术本地语言", "type": 1, "ui_type": "Text"},
    {"field_name": "话术生成状态", "type": 3, "ui_type": "SingleSelect", "property": select_options([
        "待生成", "已生成", "无需生成", "生成失败"
    ])},
    {"field_name": "话术版本", "type": 1, "ui_type": "Text"},
    {"field_name": "话术质量分", "type": 2, "ui_type": "Number"},
    {"field_name": "话术生成原因", "type": 1, "ui_type": "Text"},
    {"field_name": "建联发送状态", "type": 3, "ui_type": "SingleSelect", "property": select_options([
        "待发送", "已发送", "已回复", "不跟进"
    ])},
    {"field_name": "达人已回复", "type": 7, "ui_type": "Checkbox"},
    {"field_name": "回复备注", "type": 1, "ui_type": "Text"},
    {"field_name": "进入维护状态", "type": 3, "ui_type": "SingleSelect", "property": select_options([
        "待回复", "待进入", "已进入", "不进入", "同步失败"
    ])},
    {"field_name": "维护表记录ID", "type": 1, "ui_type": "Text"},
]


def list_fields(app_token: str, table_id: str, token: str) -> List[Dict[str, Any]]:
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
    fields: List[Dict[str, Any]] = []
    page_token = None
    while True:
        params = {"page_size": 100}
        if page_token:
            params["page_token"] = page_token
        response = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params=params, timeout=30)
        data = response.json()
        if data.get("code") != 0:
            raise RuntimeError(f"列字段失败: {json.dumps(data, ensure_ascii=False)}")
        fields.extend(data.get("data", {}).get("items", []))
        if not data.get("data", {}).get("has_more"):
            break
        page_token = data.get("data", {}).get("page_token")
    return fields


def create_field(app_token: str, table_id: str, token: str, spec: Dict[str, Any]) -> Dict[str, Any]:
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
    payload = {"field_name": spec["field_name"], "type": spec["type"], "ui_type": spec["ui_type"]}
    if "property" in spec:
        payload["property"] = spec["property"]
    response = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )
    return response.json()


def main() -> int:
    parser = argparse.ArgumentParser(description="Ensure entry-screen batch outreach fields.")
    parser.add_argument("--feishu-url", default=DEFAULT_FEISHU_URL)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    app_token, table_id = resolve_feishu_config(args.feishu_url)
    reader = FeishuBitableReader(app_token, table_id)
    token = reader._get_access_token()

    existing = {item.get("field_name") for item in list_fields(app_token, table_id, token)}
    created, skipped, failed = [], [], []

    for spec in FIELDS:
        name = spec["field_name"]
        if name in existing:
            skipped.append(name)
            print(f"⏭️ 已存在: {name}")
            continue
        if args.dry_run:
            created.append(name)
            print(f"🧪 将创建: {name}")
            continue
        result = create_field(app_token, table_id, token, spec)
        if result.get("code") == 0:
            created.append(name)
            existing.add(name)
            print(f"✅ 已创建: {name}")
        else:
            failed.append({"field": name, "result": result})
            print(f"❌ 创建失败: {name} -> {json.dumps(result, ensure_ascii=False)}")

    print(json.dumps({"created": created, "skipped": skipped, "failed": failed}, ensure_ascii=False, indent=2))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
