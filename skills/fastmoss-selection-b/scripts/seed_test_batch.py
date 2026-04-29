#!/usr/bin/env python3
"""Seed a real Feishu config row and batch row for FastMoss test runs."""

from __future__ import annotations

import argparse
import json
import mimetypes
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.config import get_settings  # noqa: E402
from app.feishu import FeishuBitableClient, parse_feishu_bitable_url, resolve_wiki_bitable_app_token  # noqa: E402
from app.utils import safe_text, to_feishu_datetime_millis, utc_now_iso  # noqa: E402


def _build_bitable_client(feishu_url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise ValueError("无法解析飞书 URL: {url}".format(url=feishu_url))
    app_token = info.app_token
    if "/wiki/" in info.original_url:
        app_token = resolve_wiki_bitable_app_token(info.app_token)
    return FeishuBitableClient(app_token=app_token, table_id=info.table_id)


def _has_meaningful_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (list, dict)):
        return bool(value)
    return safe_text(value) != ""


def _find_record_id(records: List[Any], unique_field: str, unique_value: str) -> Optional[str]:
    for record in records:
        fields = getattr(record, "fields", {}) or {}
        if safe_text(fields.get(unique_field)) == unique_value:
            return getattr(record, "record_id", "")
    return None


def _find_blank_record_id(records: List[Any]) -> Optional[str]:
    for record in records:
        fields = getattr(record, "fields", {}) or {}
        if not fields or not any(_has_meaningful_value(value) for value in fields.values()):
            return getattr(record, "record_id", "")
    return None


def _upsert_record(client: FeishuBitableClient, unique_field: str, fields: Dict[str, Any]) -> Dict[str, str]:
    unique_value = safe_text(fields.get(unique_field))
    if not unique_value:
        raise ValueError("缺少唯一键字段: {field}".format(field=unique_field))
    records = client.list_records(page_size=200)
    record_id = _find_record_id(records, unique_field, unique_value)
    if record_id:
        client.update_record_fields(record_id, fields)
        return {"action": "updated", "record_id": record_id}
    blank_record_id = _find_blank_record_id(records)
    if blank_record_id:
        client.update_record_fields(blank_record_id, fields)
        return {"action": "reused_blank", "record_id": blank_record_id}
    client.batch_create_records([{"fields": fields}])
    return {"action": "created", "record_id": ""}


def _derive_snapshot_time(file_path: Path) -> str:
    match = re.search(r"_(\d{8})_(\d{6})", file_path.stem)
    if not match:
        raise ValueError("无法从文件名推导快照时间，请显式传 --snapshot-time")
    date_part, time_part = match.groups()
    return (
        "{year}-{month}-{day} {hour}:{minute}:{second}".format(
            year=date_part[0:4],
            month=date_part[4:6],
            day=date_part[6:8],
            hour=time_part[0:2],
            minute=time_part[2:4],
            second=time_part[4:6],
        )
    )


def _derive_batch_id(country: str, category: str, snapshot_time: str) -> str:
    normalized_category = re.sub(r"[^a-z0-9]+", "_", category.lower()).strip("_") or "category"
    compact_time = re.sub(r"[^0-9]", "", snapshot_time)
    return "fm_{country}_{category}_{ts}".format(
        country=country.lower(),
        category=normalized_category,
        ts=compact_time,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed FastMoss real-table test batch")
    parser.add_argument("--source-file", required=True, help="FastMoss Excel file path")
    parser.add_argument("--country", default="VN")
    parser.add_argument("--category", default="时尚配件")
    parser.add_argument("--snapshot-time", help="Override snapshot time, e.g. 2026-04-08 10:04:44")
    parser.add_argument("--batch-id", help="Override batch_id")
    parser.add_argument("--config-id", default="cfg_vn_fashion_accessories_v1")
    parser.add_argument("--fx-rate-to-rmb", type=float, default=3857.06)
    parser.add_argument("--rule-version", default="v1")
    parser.add_argument("--accio-chat-id", default="")
    parser.add_argument("--enable-hermes", action="store_true")
    parser.add_argument("--note", default="首批测试配置：VN 时尚配件样表")
    args = parser.parse_args()

    settings = get_settings()
    if not settings.config_table_url or not settings.batch_table_url:
        raise RuntimeError("缺少真实飞书表 URL 配置，请检查 .env.local")

    source_file = Path(args.source_file).expanduser().resolve()
    if not source_file.exists():
        raise FileNotFoundError("样表不存在: {path}".format(path=source_file))

    frame = pd.read_excel(source_file)
    row_count = len(frame)
    snapshot_time = args.snapshot_time or _derive_snapshot_time(source_file)
    batch_id = args.batch_id or _derive_batch_id(args.country, args.category, snapshot_time)

    config_client = _build_bitable_client(settings.config_table_url)
    batch_client = _build_bitable_client(settings.batch_table_url)

    config_fields = {
        "config_id": args.config_id,
        "国家": args.country,
        "类目": args.category,
        "是否启用": True,
        "新品天数阈值": 90,
        "总销量下限": 500,
        "总销量上限": 5000,
        "新品7天销量下限": 120,
        "老品7天销量下限": 200,
        "老品7天销量占比下限": 0.10,
        "视频竞争密度上限": 5.0,
        "达人竞争密度上限": 20.0,
        "汇率到人民币": args.fx_rate_to_rmb,
        "Accio目标群ID": args.accio_chat_id,
        "是否启用Hermes": bool(args.enable_hermes),
        "规则版本号": args.rule_version,
        "备注": args.note,
    }
    config_result = _upsert_record(config_client, "config_id", config_fields)

    content = source_file.read_bytes()
    content_type = mimetypes.guess_type(source_file.name)[0] or "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    attachment = batch_client.upload_attachment(content, source_file.name, content_type, len(content))
    batch_fields = {
        "batch_id": batch_id,
        "国家": args.country,
        "类目": args.category,
        "快照时间": to_feishu_datetime_millis(snapshot_time),
        "原始文件附件": [attachment],
        "原始文件名": source_file.name,
        "原始记录数": row_count,
        "A导入状态": "已完成",
        "B下载状态": "",
        "B入库状态": "",
        "规则筛选状态": "",
        "Accio状态": "",
        "Hermes状态": "",
        "整体状态": "待B下载",
        "错误信息": "",
        "重试次数": 0,
        "最后更新时间": to_feishu_datetime_millis(utc_now_iso()),
    }
    batch_result = _upsert_record(batch_client, "batch_id", batch_fields)

    print(
        json.dumps(
            {
                "status": "ok",
                "config": {"config_id": args.config_id, **config_result},
                "batch": {"batch_id": batch_id, "row_count": row_count, **batch_result},
                "source_file": str(source_file),
                "snapshot_time": snapshot_time,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
