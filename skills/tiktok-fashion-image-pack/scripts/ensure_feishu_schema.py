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
MULTI_SELECT = {"type": 4, "ui_type": "MultiSelect"}
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


def multi_select(options: List[str]) -> Dict[str, Any]:
    return {
        "type": MULTI_SELECT["type"],
        "ui_type": MULTI_SELECT["ui_type"],
        "property": {
            "options": [{"name": item, "color": index % 54} for index, item in enumerate(options)],
        },
    }


FIELD_SPECS: List[Dict[str, Any]] = [
    # Minimal human input
    {"name": "任务ID", **TEXT},
    {"name": "原始图片", **ATTACHMENT},
    {"name": "原始场景参考图", **ATTACHMENT},
    {"name": "商品链接/供应商链接", **URL},
    {"name": "国家", **single_select(["TH", "VN", "MY", "PH", "ID", "SG"])},
    {"name": "类目", **single_select(["女装上装/外套", "发饰", "配饰", "包袋", "鞋靴"])},
    {"name": "生成图类型", **single_select(["只首图", "只场景图", "首图+场景图", "首图+详情图", "全套图包"])},
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
    {"name": "场景图结果", **ATTACHMENT},
    {"name": "生成Prompt", **TEXT},
    {"name": "场景图Prompt", **TEXT},
    {"name": "质检结果", **single_select(["未质检", "通过", "轻微问题可用", "不通过已重试", "需人工复核"])},
    {"name": "质检问题", **TEXT},
    {"name": "场景图质检结果", **single_select(["未质检", "通过", "轻微问题可用", "不通过", "需人工复核"])},
    {"name": "场景图质检问题", **TEXT},
    {"name": "场景图生成明细", **TEXT},
    {"name": "场景偏好", **single_select(["自动匹配", "咖啡店/商场", "通勤街边", "镜前试穿", "店铺/工作室", "旅行/轻户外", "校园/日常"])},
    {"name": "场景图槽位", **multi_select([
        "S1 主点击试穿",
        "S2 日常氛围",
        "S3 版型/颜色证明",
        "S4 材质结构细节",
        "S5 多色上身1",
        "S6 多色上身2",
    ])},
    {"name": "重试次数", **NUMBER},
    {"name": "最后生成时间", **DATETIME},
    {"name": "最终状态", **TEXT},
    # Title optimization
    {"name": "标题生成状态", **single_select(["待生成", "生成中", "已生成", "需人工复核", "失败", "暂停"])},
    {"name": "原标题/供应商标题", **TEXT},
    {"name": "TK标题", **TEXT},
    {"name": "标题中文摘要", **TEXT},
    {"name": "标题关键词", **TEXT},
    {"name": "标题系列", **TEXT},
    {"name": "标题系列编码", **TEXT},
    {"name": "标题质检结果", **single_select(["通过", "轻微问题可用", "需人工复核", "不通过"])},
    {"name": "标题质检问题", **TEXT},
    {"name": "标题生成Prompt", **TEXT},
    {"name": "标题生成时间", **DATETIME},
    {"name": "标题人工要求", **TEXT},
]

DEFAULT_RECORD_VALUES: Dict[str, Any] = {
    "场景偏好": "自动匹配",
    "场景图槽位": ["S1 主点击试穿", "S2 日常氛围", "S3 版型/颜色证明", "S4 材质结构细节"],
}


def resolve_client(feishu_url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise SystemExit(f"无法解析飞书 URL: {feishu_url}")
    app_token = info.app_token
    if "/wiki/" in info.original_url:
        app_token = resolve_wiki_bitable_app_token(info.app_token)
    return FeishuBitableClient(app_token=app_token, table_id=info.table_id)


def ensure_fields(client: FeishuBitableClient, dry_run: bool = False) -> Dict[str, List[str]]:
    existing_fields = {field.field_name: field for field in client.list_fields()}
    created: List[str] = []
    skipped: List[str] = []
    updated: List[str] = []
    for spec in FIELD_SPECS:
        name = spec["name"]
        if name in existing_fields:
            if should_update_select_options(existing_fields[name], spec):
                if not dry_run:
                    update_field_property(client, existing_fields[name].field_id, spec)
                updated.append(name)
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
    return {"created": created, "skipped": skipped, "updated": updated}


def backfill_default_values(client: FeishuBitableClient, dry_run: bool = False) -> int:
    updated = 0
    for record in client.list_records(page_size=100, limit=None):
        fields = record.fields or {}
        patch = {
            name: value
            for name, value in DEFAULT_RECORD_VALUES.items()
            if is_empty_cell(fields.get(name))
        }
        if not patch:
            continue
        if not dry_run:
            client.update_record_fields(record.record_id, patch)
        updated += 1
    return updated


def is_empty_cell(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, list):
        return len(value) == 0
    return False


def option_names(property_value: Any) -> List[str]:
    options = []
    if isinstance(property_value, dict):
        options = property_value.get("options") or []
    return [str(item.get("name") or "").strip() for item in options if isinstance(item, dict) and item.get("name")]


def should_update_select_options(existing_field: Any, spec: Dict[str, Any]) -> bool:
    if spec.get("ui_type") not in {"SingleSelect", "MultiSelect"}:
        return False
    wanted = option_names(spec.get("property"))
    current = option_names(getattr(existing_field, "property", None))
    return bool(wanted) and any(item not in current for item in wanted)


def update_field_property(client: FeishuBitableClient, field_id: str, spec: Dict[str, Any]) -> None:
    url = (
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{client.app_token}/tables/{client.table_id}/fields/{field_id}"
    )
    payload = {
        "field_name": spec["name"],
        "type": spec["type"],
        "ui_type": spec["ui_type"],
        "property": spec.get("property"),
    }
    response = client._request("PUT", url, headers=client._headers(), json=payload)
    result = response.json()
    if result.get("code") != 0:
        raise RuntimeError(f"更新字段选项失败: {spec['name']} {result.get('msg')}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("url")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--backfill-defaults", action="store_true")
    args = parser.parse_args()
    client = resolve_client(args.url)
    result = ensure_fields(client, dry_run=args.dry_run)
    backfilled = backfill_default_values(client, dry_run=args.dry_run) if args.backfill_defaults else 0
    print(f"created={len(result['created'])} updated={len(result['updated'])} skipped={len(result['skipped'])}")
    if args.backfill_defaults:
        print(f"backfilled default records={backfilled}")
    if result["created"]:
        print("created fields:")
        for name in result["created"]:
            print(f"  + {name}")
    if result["updated"]:
        print("updated fields:")
        for name in result["updated"]:
            print(f"  * {name}")
    if result["skipped"]:
        print("skipped existing fields:")
        for name in result["skipped"]:
            print(f"  = {name}")


if __name__ == "__main__":
    main()
