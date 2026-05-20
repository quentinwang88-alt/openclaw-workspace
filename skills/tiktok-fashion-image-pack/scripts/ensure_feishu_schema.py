#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path("/Users/likeu3/.openclaw/workspace/skills")
sys.path.insert(0, str(ROOT / "script-run-manager-sync"))

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # type: ignore  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # type: ignore  # noqa: E402


TEXT = {"type": 1, "ui_type": "Text"}
NUMBER = {"type": 2, "ui_type": "Number"}
SINGLE_SELECT = {"type": 3, "ui_type": "SingleSelect"}
DATETIME = {"type": 5, "ui_type": "DateTime"}
CHECKBOX = {"type": 7, "ui_type": "Checkbox"}
URL = {"type": 15, "ui_type": "Url"}
ATTACHMENT = {"type": 17, "ui_type": "Attachment"}


def single_select(options: List[str]) -> Dict[str, Any]:
    return {
        "type": SINGLE_SELECT["type"],
        "ui_type": SINGLE_SELECT["ui_type"],
        "property": {
            "options": [{"name": item, "color": index % 54} for index, item in enumerate(options)],
        },
    }


FIELD_SPECS: List[Dict[str, Any]] = [
    # Minimal human input
    {"name": "任务ID", **TEXT},
    {"name": "原始图片", **ATTACHMENT},
    {"name": "商品链接/供应商链接", **URL},
    {"name": "国家", **single_select(["TH", "VN", "MY", "PH", "ID", "SG"])},
    {"name": "类目", **single_select(["女装上装/外套", "发饰", "配饰", "包袋", "鞋靴"])},
    {"name": "生成图类型", **single_select(["只首图", "首图+详情图"])},
    {"name": "生成状态", **single_select(["待生成", "生成中", "已生成", "需人工复核", "失败", "暂停"])},
    {"name": "备注", **TEXT},
    {"name": "人工覆盖要求", **TEXT},
    # Brand/main-image policy
    {"name": "品牌名", **TEXT},
    {"name": "主图小字策略", **single_select(["likeU + 产品类型", "仅likeU", "仅产品类型", "不加字"])},
    {"name": "主图小字名称", **TEXT},
    # AI-expanded product truth
    {"name": "商品子类", **TEXT},
    {"name": "主色", **TEXT},
    {"name": "疑似多色", **CHECKBOX},
    {"name": "售卖颜色", **TEXT},
    {"name": "材质", **TEXT},
    {"name": "版型", **TEXT},
    {"name": "衣长", **TEXT},
    {"name": "领型", **TEXT},
    {"name": "门襟", **TEXT},
    {"name": "口袋结构", **TEXT},
    {"name": "袖型/袖口", **TEXT},
    {"name": "下摆结构", **TEXT},
    {"name": "核心卖点", **TEXT},
    {"name": "适合场景", **TEXT},
    {"name": "目标人群", **TEXT},
    {"name": "不可改动点", **TEXT},
    {"name": "禁止添加元素", **TEXT},
    {"name": "推荐首图模板", **TEXT},
    {"name": "推荐详情图顺序", **TEXT},
    {"name": "Product Truth JSON", **TEXT},
    {"name": "AI识别置信度", **NUMBER},
    {"name": "需复核原因", **TEXT},
    # Generation outputs and QA
    {"name": "首图结果", **ATTACHMENT},
    {"name": "详情图结果", **ATTACHMENT},
    {"name": "生成Prompt", **TEXT},
    {"name": "质检结果", **single_select(["未质检", "通过", "轻微问题可用", "不通过已重试", "需人工复核"])},
    {"name": "质检问题", **TEXT},
    {"name": "重试次数", **NUMBER},
    {"name": "最后生成时间", **DATETIME},
    {"name": "最终状态", **TEXT},
]


def resolve_client(feishu_url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise SystemExit(f"无法解析飞书 URL: {feishu_url}")
    app_token = info.app_token
    if "/wiki/" in info.original_url:
        app_token = resolve_wiki_bitable_app_token(info.app_token)
    return FeishuBitableClient(app_token=app_token, table_id=info.table_id)


def ensure_fields(client: FeishuBitableClient, dry_run: bool = False) -> Dict[str, List[str]]:
    existing = {field.field_name for field in client.list_fields()}
    created: List[str] = []
    skipped: List[str] = []
    for spec in FIELD_SPECS:
        name = spec["name"]
        if name in existing:
            skipped.append(name)
            continue
        if not dry_run:
            client.create_field(
                field_name=name,
                field_type=int(spec["type"]),
                ui_type=str(spec["ui_type"]),
                property=spec.get("property"),
            )
        created.append(name)
    return {"created": created, "skipped": skipped}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("url")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    client = resolve_client(args.url)
    result = ensure_fields(client, dry_run=args.dry_run)
    print(f"created={len(result['created'])} skipped={len(result['skipped'])}")
    if result["created"]:
        print("created fields:")
        for name in result["created"]:
            print(f"  + {name}")
    if result["skipped"]:
        print("skipped existing fields:")
        for name in result["skipped"]:
            print(f"  = {name}")


if __name__ == "__main__":
    main()
