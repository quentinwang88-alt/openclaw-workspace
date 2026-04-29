#!/usr/bin/env python3
"""Ensure the lightweight Feishu bitable schema exists for FastMoss selection B."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT.parent / "script-run-manager-sync"))

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


TABLE_SCHEMAS = {
    "config": [
        {"name": "config_id", **TEXT},
        {"name": "国家", **TEXT},
        {"name": "类目", **TEXT},
        {"name": "是否启用", **CHECKBOX},
        {"name": "新品天数阈值", **NUMBER},
        {"name": "总销量下限", **NUMBER},
        {"name": "总销量上限", **NUMBER},
        {"name": "新品7天销量下限", **NUMBER},
        {"name": "老品7天销量下限", **NUMBER},
        {"name": "老品7天销量占比下限", **NUMBER},
        {"name": "视频竞争密度上限", **NUMBER},
        {"name": "达人竞争密度上限", **NUMBER},
        {"name": "汇率到人民币", **NUMBER},
        {"name": "平台综合费率", **NUMBER},
        {"name": "配饰发饰头程运费_rmb", **NUMBER},
        {"name": "轻上装头程运费_rmb", **NUMBER},
        {"name": "厚女装头程运费_rmb", **NUMBER},
        {"name": "Accio目标群ID", **TEXT},
        {"name": "是否启用Hermes", **CHECKBOX},
        {"name": "规则版本号", **TEXT},
        {"name": "备注", **TEXT},
    ],
    "batch": [
        {"name": "batch_id", **TEXT},
        {"name": "国家", **TEXT},
        {"name": "类目", **TEXT},
        {"name": "快照时间", **DATETIME},
        {"name": "原始文件附件", **ATTACHMENT},
        {"name": "原始文件名", **TEXT},
        {"name": "原始记录数", **NUMBER},
        {"name": "A导入状态", **TEXT},
        {"name": "B下载状态", **TEXT},
        {"name": "B入库状态", **TEXT},
        {"name": "规则筛选状态", **TEXT},
        {"name": "Accio状态", **TEXT},
        {"name": "Hermes状态", **TEXT},
        {"name": "整体状态", **TEXT},
        {"name": "错误信息", **TEXT},
        {"name": "重试次数", **NUMBER},
        {"name": "最后更新时间", **DATETIME},
    ],
    "workspace": [
        {"name": "work_id", **TEXT},
        {"name": "batch_id", **TEXT},
        {"name": "product_id", **TEXT},
        {"name": "市场", **TEXT},
        {"name": "类目", **TEXT},
        {"name": "商品标题", **TEXT},
        {"name": "商品主图", **ATTACHMENT},
        {"name": "商品链接", **URL},
        {"name": "上架天数", **NUMBER},
        {"name": "7天销量", **NUMBER},
        {"name": "价格", **NUMBER},
        {"name": "采购价", **NUMBER},
        {"name": "毛利率", **NUMBER},
        {"name": "core_score_a", **NUMBER},
        {"name": "采购链接/货源备注", **TEXT},
        {"name": "V2总分", **NUMBER},
        {"name": "V2任务池", **TEXT},
        {"name": "任务适配度", **TEXT},
        {"name": "任务适配理由", **TEXT},
        {"name": "V2匹配方向", **TEXT},
        {"name": "差异化结论", **TEXT},
        {"name": "V2一句话理由", **TEXT},
        {"name": "V2风险标签", **TEXT},
        {"name": "生命周期状态", **TEXT},
        {"name": "人工判断状态", **TEXT},
        {"name": "负责人", **TEXT},
        {"name": "人工备注", **TEXT},
        {"name": "是否加入测品池", **CHECKBOX},
    ],
    "followup": [
        {"name": "followup_id", **TEXT},
        {"name": "来源work_id", **TEXT},
        {"name": "商品名称", **TEXT},
        {"name": "国家", **TEXT},
        {"name": "类目", **TEXT},
        {"name": "跟进开始时间", **DATETIME},
        {"name": "打法", **single_select(["自然流", "达人分销", "投流测试", "组合测试"])},
        {"name": "当前状态", **single_select(["跟进中", "暂停", "已完成", "放弃"])},
        {"name": "7天复盘", **TEXT},
        {"name": "30天复盘", **TEXT},
        {"name": "最终结论", **TEXT},
        {"name": "是否写回经验", **CHECKBOX},
        {"name": "复盘备注", **TEXT},
    ],
}


def resolve_client(feishu_url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise ValueError(f"无法解析飞书 URL: {feishu_url}")
    app_token = info.app_token
    if "/wiki/" in info.original_url:
        app_token = resolve_wiki_bitable_app_token(info.app_token)
    return FeishuBitableClient(app_token=app_token, table_id=info.table_id)


def ensure_schema(client: FeishuBitableClient, specs: List[Dict[str, Any]], table_name: str) -> Dict[str, Any]:
    existing_names = {item.field_name for item in client.list_fields()}
    created = []
    skipped = []
    for spec in specs:
        field_name = str(spec["name"]).strip()
        if field_name in existing_names:
            skipped.append(field_name)
            continue
        try:
            client.create_field(
                field_name=field_name,
                field_type=int(spec["type"]),
                ui_type=str(spec["ui_type"]),
                property=spec.get("property"),
            )
        except Exception as exc:
            raise RuntimeError(f"{table_name} 表创建字段失败: {field_name}: {exc}") from exc
        created.append(field_name)
    return {"created": created, "skipped": skipped}


def main() -> None:
    parser = argparse.ArgumentParser(description="Ensure FastMoss Feishu schema")
    parser.add_argument("--config-url", required=True)
    parser.add_argument("--batch-url", required=True)
    parser.add_argument("--workspace-url", required=True)
    parser.add_argument("--followup-url", required=True)
    args = parser.parse_args()

    url_map = {
        "config": args.config_url,
        "batch": args.batch_url,
        "workspace": args.workspace_url,
        "followup": args.followup_url,
    }

    result = {}
    for table_name, url in url_map.items():
        client = resolve_client(url)
        result[table_name] = ensure_schema(client, TABLE_SCHEMAS[table_name], table_name)

    for table_name, summary in result.items():
        print(f"[{table_name}] created={len(summary['created'])} skipped={len(summary['skipped'])}")
        if summary["created"]:
            for name in summary["created"]:
                print(f"  + {name}")


if __name__ == "__main__":
    main()
