#!/usr/bin/env python3
"""Refresh product candidate records in Feishu."""

from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence
from zoneinfo import ZoneInfo

from config import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_DATE_FORMATTER,
    DEFAULT_FEISHU_URL,
    DEFAULT_LLM_API_KEY,
    DEFAULT_LLM_BASE_URL,
    DEFAULT_LLM_MODEL,
    DEFAULT_SUBCATEGORIES,
    DEFAULT_TIMEOUT_SECONDS,
    DEFAULT_TIMEZONE,
)
from core.feishu import (
    FeishuAPIError,
    FeishuBitableClient,
    FeishuBitableInfo,
    TableField,
    TableRecord,
    parse_feishu_bitable_url,
    resolve_wiki_bitable_app_token,
)
from core.llm import CandidateLLMClient, LLMError


SKILL_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = SKILL_DIR / "output"

FIELD_PRODUCT_NAME = "商品名称"
FIELD_CHINESE_NAME = "中文名称"
FIELD_SUBCATEGORY = "子类目"
FIELD_PRODUCT_CATEGORY = "商品分类"
FIELD_COUNTRY = "国家/地区"
FIELD_IMAGE = "商品图片"
FIELD_LISTING_TIME = "预估商品上架时间"
FIELD_LISTING_DAYS = "上架天数"

REQUIRED_FIELDS = (
    FIELD_PRODUCT_NAME,
    FIELD_CHINESE_NAME,
    FIELD_SUBCATEGORY,
    FIELD_LISTING_TIME,
    FIELD_LISTING_DAYS,
)


@dataclass
class ProcessorOptions:
    feishu_url: str = DEFAULT_FEISHU_URL
    limit: Optional[int] = None
    record_ids: Optional[List[str]] = None
    dry_run: bool = False
    skip_llm: bool = False
    skip_date_format_update: bool = False
    overwrite_chinese_name: bool = False
    overwrite_subcategory: bool = False
    overwrite_listing_days: bool = False
    timezone_name: str = DEFAULT_TIMEZONE
    subcategories: Sequence[str] = DEFAULT_SUBCATEGORIES
    llm_base_url: str = DEFAULT_LLM_BASE_URL
    llm_api_key: str = DEFAULT_LLM_API_KEY
    llm_model: str = DEFAULT_LLM_MODEL
    max_llm_workers: int = 8
    max_date_workers: int = 48
    batch_size: int = DEFAULT_BATCH_SIZE
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS


def normalize_cell_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value)
    if isinstance(value, dict):
        for key in ("text", "name", "url", "link"):
            cell = value.get(key)
            if isinstance(cell, str) and cell.strip():
                return cell.strip()
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        parts = [normalize_cell_value(item) for item in value]
        return " / ".join(part for part in parts if part)
    return str(value).strip()


def parse_listing_datetime(raw_value: Any, timezone_name: str) -> Optional[datetime]:
    if raw_value is None or raw_value == "":
        return None

    tz = ZoneInfo(timezone_name)
    if isinstance(raw_value, (int, float)):
        timestamp = float(raw_value)
        if timestamp > 1_000_000_000_000:
            timestamp /= 1000.0
        return datetime.fromtimestamp(timestamp, tz=tz)

    if isinstance(raw_value, str):
        value = raw_value.strip()
        for fmt in (
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%Y-%m-%d %H:%M",
            "%Y/%m/%d %H:%M",
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S",
        ):
            try:
                parsed = datetime.strptime(value, fmt)
                return parsed.replace(tzinfo=tz)
            except ValueError:
                continue
    return None


def compute_listing_days(listing_dt: datetime, now: datetime) -> int:
    return max(0, (now.date() - listing_dt.date()).days)


def chunked(items: Sequence[Dict[str, Any]], size: int) -> List[List[Dict[str, Any]]]:
    return [list(items[index : index + size]) for index in range(0, len(items), size)]


def resolve_app_token(info: FeishuBitableInfo) -> str:
    if info.is_wiki:
        return resolve_wiki_bitable_app_token(info.app_token)
    return info.app_token


class ProductCandidateEnricher:
    def __init__(self, options: ProcessorOptions):
        self.options = options
        self.now = datetime.now(ZoneInfo(options.timezone_name))
        self.llm_client: Optional[CandidateLLMClient] = None
        if not options.skip_llm:
            self.llm_client = CandidateLLMClient(
                base_url=options.llm_base_url,
                api_key=options.llm_api_key,
                model=options.llm_model,
                subcategories=options.subcategories,
                timeout=options.timeout_seconds,
            )

    def run(self) -> Dict[str, Any]:
        info = parse_feishu_bitable_url(self.options.feishu_url)
        if info is None:
            raise ValueError(f"无法解析飞书表格链接: {self.options.feishu_url}")

        app_token = resolve_app_token(info)
        client = FeishuBitableClient(app_token=app_token, table_id=info.table_id)

        fields = client.list_fields()
        field_map = {field.field_name: field for field in fields}
        missing = [name for name in REQUIRED_FIELDS if name not in field_map]
        if missing:
            raise ValueError(f"表格缺少必需字段: {', '.join(missing)}")

        formatter_result = self._ensure_date_formatter(
            client=client,
            field=field_map[FIELD_LISTING_TIME],
        )

        record_limit = None if self.options.record_ids else self.options.limit
        records = client.list_records(limit=record_limit)
        if self.options.record_ids:
            target_ids = set(self.options.record_ids)
            records = [record for record in records if record.record_id in target_ids]

        preview: List[Dict[str, Any]] = []
        warnings: List[str] = list(formatter_result["warnings"])
        errors: List[Dict[str, str]] = []
        llm_calls = 0
        updated_records = 0
        skipped_records = 0
        pending_batch_updates: List[Dict[str, Any]] = []

        for record, outcome in self._iter_record_outcomes(records):
            if outcome.get("error"):
                errors.append({"record_id": record.record_id, "error": str(outcome["error"])})
                continue

            result = outcome["result"]
            warnings.extend(result["warnings"])
            if result["llm_called"]:
                llm_calls += 1

            if result["updates"]:
                if self._should_use_batch_write():
                    pending_batch_updates.append(
                        {
                            "record_id": record.record_id,
                            "fields": result["updates"],
                        }
                    )
                else:
                    try:
                        self._write_record_update(
                            client=client,
                            record=record,
                            updates=result["updates"],
                        )
                    except FeishuAPIError as exc:
                        errors.append({"record_id": record.record_id, "error": str(exc)})
                        continue

                updated_records += 1
                preview.append(
                    {
                        "record_id": record.record_id,
                        "商品名称": normalize_cell_value(record.fields.get(FIELD_PRODUCT_NAME)),
                        "updates": result["updates"],
                    }
                )
            else:
                skipped_records += 1

        if pending_batch_updates and not self.options.dry_run:
            try:
                self._write_batch_updates(client=client, updates=pending_batch_updates)
            except FeishuAPIError as exc:
                errors.append({"record_id": "batch_update", "error": str(exc)})

        summary = {
            "success": len(errors) == 0,
            "error": None if not errors else f"部分记录处理失败，共 {len(errors)} 条",
            "data": {
                "feishu_url": self.options.feishu_url,
                "app_token": app_token,
                "table_id": info.table_id,
                "dry_run": self.options.dry_run,
                "timezone": self.options.timezone_name,
                "subcategories": list(self.options.subcategories),
                "stats": {
                    "total_records": len(records),
                    "updated_records": updated_records,
                    "skipped_records": skipped_records,
                    "llm_calls": llm_calls,
                    "error_records": len(errors),
                    "date_formatter_changed": formatter_result["changed"],
                    "date_formatter_target": DEFAULT_DATE_FORMATTER,
                },
                "preview": preview[:20],
                "warnings": warnings,
                "errors": errors,
            },
        }

        summary["data"]["summary_file"] = self._write_summary_file(summary)
        return summary

    def _iter_record_outcomes(self, records: Sequence[TableRecord]):
        worker_count = self._resolve_worker_count(len(records))
        if worker_count <= 1 or len(records) <= 1:
            for record in records:
                try:
                    yield record, {"result": self._build_record_update(record)}
                except Exception as exc:
                    yield record, {"error": str(exc)}
            return

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {
                executor.submit(self._build_record_update, record): record
                for record in records
            }
            for future in as_completed(future_map):
                record = future_map[future]
                try:
                    yield record, {"result": future.result()}
                except Exception as exc:
                    yield record, {"error": str(exc)}

    def _resolve_worker_count(self, record_count: int) -> int:
        if self.options.skip_llm:
            requested = max(1, self.options.max_date_workers)
            return min(requested, max(1, record_count))
        requested = max(1, self.options.max_llm_workers)
        return min(requested, max(1, record_count))

    def _should_use_batch_write(self) -> bool:
        return self.options.skip_llm

    def _write_record_update(
        self,
        client: FeishuBitableClient,
        record: TableRecord,
        updates: Dict[str, Any],
    ) -> None:
        if not updates:
            return
        if self.options.dry_run:
            return
        client.update_record_fields(record.record_id, updates)

    def _write_batch_updates(
        self,
        client: FeishuBitableClient,
        updates: Sequence[Dict[str, Any]],
    ) -> None:
        if self.options.dry_run or not updates:
            return
        for batch in chunked(list(updates), max(1, self.options.batch_size)):
            client.batch_update_records(batch)

    def _ensure_date_formatter(self, client: FeishuBitableClient, field: TableField) -> Dict[str, Any]:
        warnings: List[str] = []
        if self.options.skip_date_format_update:
            return {"changed": False, "warnings": warnings}

        current_property = dict(field.property or {})
        current_formatter = current_property.get("date_formatter")
        if current_formatter == DEFAULT_DATE_FORMATTER:
            return {"changed": False, "warnings": warnings}

        if self.options.dry_run:
            warnings.append(
                f"dry-run: 将把字段 `{FIELD_LISTING_TIME}` 的日期格式从 "
                f"`{current_formatter or '未设置'}` 更新为 `{DEFAULT_DATE_FORMATTER}`"
            )
            return {"changed": False, "warnings": warnings}

        try:
            current_property["date_formatter"] = DEFAULT_DATE_FORMATTER
            client.update_field(
                field_id=field.field_id,
                field_name=field.field_name,
                field_type=field.field_type,
                property=current_property,
                description=field.description,
                ui_type=field.ui_type,
            )
            return {"changed": True, "warnings": warnings}
        except FeishuAPIError as exc:
            warnings.append(f"日期字段格式更新失败: {exc}")
            return {"changed": False, "warnings": warnings}

    def _build_record_update(self, record: TableRecord) -> Dict[str, Any]:
        fields = record.fields
        updates: Dict[str, Any] = {}
        warnings: List[str] = []
        llm_called = False

        product_name = normalize_cell_value(fields.get(FIELD_PRODUCT_NAME))
        chinese_name = normalize_cell_value(fields.get(FIELD_CHINESE_NAME))
        subcategory = normalize_cell_value(fields.get(FIELD_SUBCATEGORY))
        listing_days = normalize_cell_value(fields.get(FIELD_LISTING_DAYS))
        product_category = normalize_cell_value(fields.get(FIELD_PRODUCT_CATEGORY))
        country = normalize_cell_value(fields.get(FIELD_COUNTRY))
        image_url = normalize_cell_value(fields.get(FIELD_IMAGE))

        if not product_name:
            raise ValueError("商品名称为空")

        listing_dt = parse_listing_datetime(fields.get(FIELD_LISTING_TIME), self.options.timezone_name)
        if listing_dt is None:
            warnings.append(f"record {record.record_id}: 无法解析 `{FIELD_LISTING_TIME}`")
        else:
            expected_days = str(compute_listing_days(listing_dt, self.now))
            if self.options.overwrite_listing_days or listing_days != expected_days:
                updates[FIELD_LISTING_DAYS] = expected_days

        llm_needed = (
            not self.options.skip_llm
            and self.llm_client is not None
            and (
                self.options.overwrite_chinese_name
                or self.options.overwrite_subcategory
                or not chinese_name
                or not subcategory
            )
        )
        if llm_needed:
            try:
                llm_result = self.llm_client.translate_and_tag(
                    product_name=product_name,
                    product_category=product_category,
                    country=country,
                    image_url=image_url,
                )
                llm_called = True
                if self.options.overwrite_chinese_name or not chinese_name:
                    if chinese_name != llm_result.chinese_name:
                        updates[FIELD_CHINESE_NAME] = llm_result.chinese_name
                if self.options.overwrite_subcategory or not subcategory:
                    if subcategory != llm_result.subcategory:
                        updates[FIELD_SUBCATEGORY] = llm_result.subcategory
            except LLMError as exc:
                warnings.append(f"record {record.record_id}: LLM 处理失败: {exc}")

        return {
            "updates": updates,
            "warnings": warnings,
            "llm_called": llm_called,
        }

    def _write_summary_file(self, summary: Dict[str, Any]) -> str:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        path = OUTPUT_DIR / f"run_{self.now.strftime('%Y%m%d_%H%M%S')}.json"
        path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        return str(path)


def run_candidate_enrichment(
    feishu_url: str = DEFAULT_FEISHU_URL,
    limit: Optional[int] = None,
    record_ids: Optional[Sequence[str]] = None,
    dry_run: bool = False,
    skip_llm: bool = False,
    skip_date_format_update: bool = False,
    overwrite_chinese_name: bool = False,
    overwrite_subcategory: bool = False,
    overwrite_listing_days: bool = False,
    timezone_name: str = DEFAULT_TIMEZONE,
    subcategories: Optional[Sequence[str]] = None,
    max_llm_workers: int = 8,
    max_date_workers: int = 48,
) -> Dict[str, Any]:
    options = ProcessorOptions(
        feishu_url=feishu_url,
        limit=limit,
        record_ids=list(record_ids) if record_ids else None,
        dry_run=dry_run,
        skip_llm=skip_llm,
        skip_date_format_update=skip_date_format_update,
        overwrite_chinese_name=overwrite_chinese_name,
        overwrite_subcategory=overwrite_subcategory,
        overwrite_listing_days=overwrite_listing_days,
        timezone_name=timezone_name,
        subcategories=tuple(subcategories or DEFAULT_SUBCATEGORIES),
        max_llm_workers=max_llm_workers,
        max_date_workers=max_date_workers,
    )
    return ProductCandidateEnricher(options).run()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Enrich product candidate records in Feishu.")
    parser.add_argument("--feishu-url", default=DEFAULT_FEISHU_URL)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--record-id", action="append", dest="record_ids")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-llm", action="store_true")
    parser.add_argument("--skip-date-format-update", action="store_true")
    parser.add_argument("--overwrite-chinese-name", action="store_true")
    parser.add_argument("--overwrite-subcategory", action="store_true")
    parser.add_argument("--overwrite-listing-days", action="store_true")
    parser.add_argument("--max-llm-workers", type=int, default=8)
    parser.add_argument("--max-date-workers", type=int, default=48)
    parser.add_argument("--timezone", default=DEFAULT_TIMEZONE)
    parser.add_argument(
        "--subcategories",
        help="Comma-separated subcategory list. Defaults to 发夹,发簪,发带,发箍,其它",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    subcategories = DEFAULT_SUBCATEGORIES
    if args.subcategories:
        subcategories = tuple(
            item.strip() for item in args.subcategories.split(",") if item.strip()
        ) or DEFAULT_SUBCATEGORIES

    result = run_candidate_enrichment(
        feishu_url=args.feishu_url,
        limit=args.limit,
        record_ids=args.record_ids,
        dry_run=args.dry_run,
        skip_llm=args.skip_llm,
        skip_date_format_update=args.skip_date_format_update,
        overwrite_chinese_name=args.overwrite_chinese_name,
        overwrite_subcategory=args.overwrite_subcategory,
        overwrite_listing_days=args.overwrite_listing_days,
        max_llm_workers=args.max_llm_workers,
        max_date_workers=args.max_date_workers,
        timezone_name=args.timezone,
        subcategories=subcategories,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
