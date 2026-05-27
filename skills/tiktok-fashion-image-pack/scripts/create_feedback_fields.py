#!/usr/bin/env python3
"""Create feedback fix fields in the likeU TikTok fashion image pack Feishu bitable."""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any, Dict, List

SKILL_DIR = Path(__file__).resolve().parents[1] / "core"
WORKSPACE_DIR = Path(__file__).resolve().parents[2]
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))
if str(WORKSPACE_DIR) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_DIR))

from feishu import FeishuBitableClient, resolve_client_from_url

FEISHU_URL = "https://gcngopvfvo0q.feishu.cn/wiki/DYHpwJx39iRSnTk5ZkpcPau8nsb?table=tbli89dtn8tOgYdI&view=vewSovaI4K"

FIELDS_TO_CREATE: List[Dict[str, Any]] = [
    {
        "field_name": "反馈目标图",
        "type": 4,
        "property": {
            "options": [
                {"name": "首图", "color": 0},
                {"name": "S1", "color": 1},
                {"name": "S2", "color": 2},
                {"name": "S3", "color": 3},
                {"name": "S4", "color": 4},
                {"name": "S5", "color": 5},
                {"name": "S6", "color": 6},
            ]
        },
    },
    {
        "field_name": "图片反馈问题",
        "type": 1,
    },
    {
        "field_name": "反馈状态",
        "type": 3,
        "property": {
            "options": [
                {"name": "待修正", "color": 0},
                {"name": "修正中", "color": 1},
                {"name": "已修正", "color": 2},
                {"name": "需人工复核", "color": 3},
            ]
        },
    },
    {
        "field_name": "反馈处理方式",
        "type": 3,
        "property": {
            "options": [
                {"name": "局部修正", "color": 0},
                {"name": "整图重生", "color": 1},
            ]
        },
    },
    {
        "field_name": "反馈修正结果",
        "type": 17,
    },
    {
        "field_name": "反馈修正结果_场景图",
        "type": 17,
    },
    {
        "field_name": "反馈修正Prompt",
        "type": 1,
    },
    {
        "field_name": "反馈质检结果",
        "type": 3,
        "property": {
            "options": [
                {"name": "通过", "color": 0},
                {"name": "轻微问题可用", "color": 1},
                {"name": "不通过", "color": 2},
            ]
        },
    },
    {
        "field_name": "反馈质检问题",
        "type": 1,
    },
]


def create_field(client: FeishuBitableClient, field_def: Dict[str, Any]) -> bool:
    field_name = field_def["field_name"]
    url = (
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{client.app_token}/tables/{client.table_id}/fields"
    )
    body = {"field_name": field_name, "type": field_def["type"]}
    if "property" in field_def:
        body["property"] = field_def["property"]
    try:
        response = client._request("POST", url, headers=client._headers(), json=body)
        result = response.json()
        if result.get("code") != 0:
            print(f"  ❌ {field_name}: {result.get('msg')}")
            return False
        print(f"  ✅ {field_name}")
        return True
    except Exception as exc:
        print(f"  ❌ {field_name}: {exc}")
        return False


def main() -> int:
    client, view_id = resolve_client_from_url(FEISHU_URL)
    print(f"表: {client.app_token}/{client.table_id}")

    existing = {field.field_name for field in client.list_fields()}
    print(f"已有 {len(existing)} 个字段")

    created = 0
    skipped = 0
    failed = 0

    for field_def in FIELDS_TO_CREATE:
        name = field_def["field_name"]
        if name in existing:
            print(f"  ⏭️ {name} (已存在，跳过)")
            skipped += 1
            continue
        if create_field(client, field_def):
            created += 1
        else:
            failed += 1
        time.sleep(0.3)

    print(f"\n结果: 新建 {created}, 跳过 {skipped}, 失败 {failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
