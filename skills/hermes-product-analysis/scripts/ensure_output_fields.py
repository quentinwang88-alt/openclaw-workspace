#!/usr/bin/env python3
"""Ensure Hermes V2 output fields exist in a Feishu bitable."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import requests


ROOT = Path(__file__).resolve().parents[1]
SYNC_SKILL_DIR = ROOT.parent / "script-run-manager-sync"
sys.path.insert(0, str(SYNC_SKILL_DIR))

from core.bitable import (  # type: ignore  # noqa: E402
    FeishuAPIError,
    FeishuBitableClient,
    get_tenant_access_token,
    resolve_wiki_bitable_app_token,
)
from core.feishu_url_parser import parse_feishu_bitable_url  # type: ignore  # noqa: E402


TEXT = {"type": 1, "ui_type": "Text"}
NUMBER = {"type": 2, "ui_type": "Number"}
DATETIME = {"type": 5, "ui_type": "DateTime"}
SINGLE_SELECT = {"type": 3, "ui_type": "SingleSelect"}


def single_select(options: List[str]) -> Dict[str, Any]:
    return {
        "type": SINGLE_SELECT["type"],
        "ui_type": SINGLE_SELECT["ui_type"],
        "property": {
            "options": [{"name": item, "color": index % 54} for index, item in enumerate(options)],
        },
    }


HERMES_OUTPUT_FIELDS = [
    {"name": "分析状态", **single_select(["分析中", "已完成", "异常中断"])},
    {"name": "识别类目", **single_select(["发饰", "轻上装", "其他", "无法判断"])},
    {"name": "识别置信度", **single_select(["manual", "high", "medium", "low"])},
    {"name": "市场匹配分", **NUMBER},
    {"name": "店铺匹配分", **NUMBER},
    {"name": "内容可做性分", **NUMBER},
    {"name": "批内优先级分", **NUMBER},
    {"name": "供给检查状态", **single_select(["pass", "watch", "fail", "pending", "timeout"])},
    {"name": "建议动作", **single_select(["优先测试", "低成本试款", "先放备用池", "补信息后再看", "暂不建议推进"])},
    {"name": "简短理由", **TEXT},
    {"name": "V2总分", **NUMBER},
    {"name": "V2建议动作", **single_select(["进入测品池", "人工复核后进入", "放入观察池", "头部参考留档", "淘汰"])},
    {"name": "V2匹配方向", **TEXT},
    {"name": "V2任务池", **TEXT},
    {"name": "任务适配度", **TEXT},
    {"name": "任务适配理由", **TEXT},
    {"name": "V2任务类型", **TEXT},
    {"name": "生命周期状态", **TEXT},
    {"name": "方向动作", **TEXT},
    {"name": "Brief来源", **TEXT},
    {"name": "差异化结论", **TEXT},
    {"name": "V2一句话理由", **TEXT},
    {"name": "V2风险标签", **TEXT},
    {"name": "分析时间", **DATETIME},
    {"name": "分析异常", **TEXT},
]


def resolve_client(feishu_url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise ValueError(f"无法解析飞书 URL: {feishu_url}")
    app_token = info.app_token
    if "/wiki/" in info.original_url:
        try:
            app_token = resolve_wiki_bitable_app_token(info.app_token)
        except FeishuAPIError:
            app_token = _resolve_embedded_bitable_app_token(info.app_token, info.table_id) or info.app_token
    return FeishuBitableClient(app_token=app_token, table_id=info.table_id)


def _resolve_embedded_bitable_app_token(wiki_token: str, table_id: str) -> str:
    access_token = get_tenant_access_token()
    headers = {"Authorization": f"Bearer {access_token}"}
    node_resp = requests.get(
        "https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node",
        headers=headers,
        params={"token": wiki_token},
        timeout=30,
    )
    node_resp.raise_for_status()
    node_payload = node_resp.json()
    node = node_payload.get("data", {}).get("node", {})
    spreadsheet_token = str(node.get("obj_token") or "").strip()
    if not spreadsheet_token:
        return ""

    metainfo_resp = requests.get(
        f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/metainfo",
        headers=headers,
        timeout=30,
    )
    metainfo_resp.raise_for_status()
    metainfo_payload = metainfo_resp.json()
    for sheet in metainfo_payload.get("data", {}).get("sheets", []):
        block_info = sheet.get("blockInfo") or {}
        block_token = str(block_info.get("blockToken") or "").strip()
        if block_token.endswith(f"_{table_id}"):
            return block_token.rsplit("_", 1)[0]
    return ""


def ensure_fields(client: FeishuBitableClient) -> Dict[str, Any]:
    existing_names = {item.field_name for item in client.list_fields()}
    created = []
    skipped = []
    for spec in HERMES_OUTPUT_FIELDS:
        field_name = str(spec["name"]).strip()
        if field_name in existing_names:
            skipped.append(field_name)
            continue
        client.create_field(
            field_name=field_name,
            field_type=int(spec["type"]),
            ui_type=str(spec["ui_type"]),
            property=spec.get("property"),
        )
        created.append(field_name)
    return {
        "created": created,
        "skipped": skipped,
        "required_fields": [item["name"] for item in HERMES_OUTPUT_FIELDS],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Ensure Hermes V2 output fields exist in a Feishu table.")
    parser.add_argument("--feishu-url", required=True)
    args = parser.parse_args()

    client = resolve_client(args.feishu_url)
    result = ensure_fields(client)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
