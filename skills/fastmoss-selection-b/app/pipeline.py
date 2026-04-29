#!/usr/bin/env python3
"""FastMoss 方案 B 主流程。"""

from __future__ import annotations

import json
import mimetypes
import os
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlsplit

import requests

from app.accio import (
    build_accio_message,
    build_accio_request_rows,
    build_accio_workspace_note,
    export_accio_image_bundle,
    export_accio_request,
    parse_accio_response_from_messages,
)
from app.config import Settings, get_settings
from app.db import Database
from app.feishu import (
    FeishuBitableClient,
    FeishuIMClient,
    FeishuAPIError,
    TableRecord,
    parse_feishu_bitable_url,
    resolve_wiki_bitable_app_token,
)
from app.followup import build_followup_row, should_sync_followup
from app.hermes import run_hermes_batch
from app.importer import normalize_fastmoss_rows
from app.models import BatchRecord, RuleConfig
from app.rules import evaluate_rule_engine
from app.utils import (
    build_work_id,
    build_feishu_url_cell,
    clamp,
    coerce_attachment_list,
    ensure_dir,
    json_dumps,
    normalize_bool,
    parse_datetime_value,
    safe_float,
    safe_int,
    safe_text,
    sha256_bytes,
    to_feishu_datetime_millis,
    utc_now_iso,
    write_csv_rows,
)


CONFIG_FIELDS = {
    "config_id": "config_id",
    "country": "国家",
    "category": "类目",
    "enabled": "是否启用",
    "new_listing_days_threshold": "新品天数阈值",
    "total_sales_min": "总销量下限",
    "total_sales_max": "总销量上限",
    "new_sales_7d_min": "新品7天销量下限",
    "old_sales_7d_min": "老品7天销量下限",
    "old_sales_ratio_min": "老品7天销量占比下限",
    "video_density_max": "视频竞争密度上限",
    "creator_density_max": "达人竞争密度上限",
    "fx_rate_to_rmb": "汇率到人民币",
    "platform_fee_rate": "平台综合费率",
    "accessory_head_shipping_rmb": "配饰发饰头程运费_rmb",
    "light_top_head_shipping_rmb": "轻上装头程运费_rmb",
    "heavy_apparel_head_shipping_rmb": "厚女装头程运费_rmb",
    "accio_chat_id": "Accio目标群ID",
    "enable_hermes": "是否启用Hermes",
    "rule_version": "规则版本号",
    "note": "备注",
}

BATCH_FIELDS = {
    "batch_id": "batch_id",
    "country": "国家",
    "category": "类目",
    "snapshot_time": "快照时间",
    "attachments": "原始文件附件",
    "raw_file_name": "原始文件名",
    "raw_record_count": "原始记录数",
    "a_import_status": "A导入状态",
    "download_status": "B下载状态",
    "import_status": "B入库状态",
    "rule_status": "规则筛选状态",
    "accio_status": "Accio状态",
    "hermes_status": "Hermes状态",
    "overall_status": "整体状态",
    "error_message": "错误信息",
    "retry_count": "重试次数",
    "last_updated_at": "最后更新时间",
}

WORKSPACE_FIELDS = {
    "work_id": "work_id",
    "batch_id": "batch_id",
    "product_id": "product_id",
    "country": "市场",
    "category": "类目",
    "product_name": "商品标题",
    "product_image_attachment": "商品主图",
    "product_url": "商品链接",
    "listing_days": "上架天数",
    "sales_7d": "7天销量",
    "avg_price_7d_rmb": "价格",
    "procurement_price_rmb": "采购价",
    "distribution_margin_rate": "毛利率",
    "source_rule_score": "core_score_a",
    "accio_note": "采购链接/货源备注",
    "manual_final_status": "人工判断状态",
    "owner": "负责人",
    "manual_note": "人工备注",
    "followup_flag": "是否加入测品池",
}

FOLLOWUP_FIELDS = {
    "followup_id": "followup_id",
    "source_work_id": "来源work_id",
    "product_name": "商品名称",
    "country": "国家",
    "category": "类目",
    "followup_started_at": "跟进开始时间",
    "strategy": "打法",
    "current_status": "当前状态",
    "review_7d": "7天复盘",
    "review_30d": "30天复盘",
    "final_conclusion": "最终结论",
    "writeback_experience_flag": "是否写回经验",
    "review_note": "复盘备注",
}


def _chunked(rows: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    for index in range(0, len(rows), size):
        yield rows[index : index + size]


class FastMossPipeline(object):
    def __init__(
        self,
        settings: Optional[Settings] = None,
        db: Optional[Database] = None,
        config_client: Optional[Any] = None,
        batch_client: Optional[Any] = None,
        workspace_client: Optional[Any] = None,
        followup_client: Optional[Any] = None,
        messenger: Optional[Any] = None,
        hermes_runner: Optional[Any] = None,
        image_downloader: Optional[Any] = None,
    ):
        self.settings = settings or get_settings()
        self.db = db or Database(self.settings.database_url)
        self.config_client = config_client
        self.batch_client = batch_client
        self.workspace_client = workspace_client
        self.followup_client = followup_client
        self.messenger = messenger
        self.hermes_runner = hermes_runner or run_hermes_batch
        self.image_downloader = image_downloader or _download_image_bytes
        self.db.init_schema()

    @classmethod
    def from_settings(cls, settings: Optional[Settings] = None) -> "FastMossPipeline":
        settings = settings or get_settings()
        config_client = _build_bitable_client(settings.config_table_url) if settings.config_table_url else None
        batch_client = _build_bitable_client(settings.batch_table_url) if settings.batch_table_url else None
        workspace_client = _build_bitable_client(settings.workspace_table_url) if settings.workspace_table_url else None
        followup_client = _build_bitable_client(settings.followup_table_url) if settings.followup_table_url else None
        messenger = FeishuIMClient()
        return cls(
            settings=settings,
            db=Database(settings.database_url),
            config_client=config_client,
            batch_client=batch_client,
            workspace_client=workspace_client,
            followup_client=followup_client,
            messenger=messenger,
        )

    def load_rule_configs(self) -> Dict[Tuple[str, str], RuleConfig]:
        if not self.config_client:
            raise RuntimeError("未配置参数配置表")
        configs = {}
        for record in _list_table_records(self.config_client, self.settings.feishu_read_page_size):
            fields = _record_fields(record)
            config = RuleConfig(
                config_id=safe_text(fields.get(CONFIG_FIELDS["config_id"])) or safe_text(_record_id(record)),
                country=safe_text(fields.get(CONFIG_FIELDS["country"])),
                category=safe_text(fields.get(CONFIG_FIELDS["category"])),
                enabled=normalize_bool(fields.get(CONFIG_FIELDS["enabled"])),
                new_listing_days_threshold=safe_int(fields.get(CONFIG_FIELDS["new_listing_days_threshold"])) or 90,
                total_sales_min=safe_int(fields.get(CONFIG_FIELDS["total_sales_min"])) or 500,
                total_sales_max=safe_int(fields.get(CONFIG_FIELDS["total_sales_max"])) or 5000,
                new_sales_7d_min=safe_int(fields.get(CONFIG_FIELDS["new_sales_7d_min"])) or 120,
                old_sales_7d_min=safe_int(fields.get(CONFIG_FIELDS["old_sales_7d_min"])) or 200,
                old_sales_ratio_min=safe_float(fields.get(CONFIG_FIELDS["old_sales_ratio_min"])) or 0.10,
                video_density_max=safe_float(fields.get(CONFIG_FIELDS["video_density_max"])) or 5.0,
                creator_density_max=safe_float(fields.get(CONFIG_FIELDS["creator_density_max"])) or 20.0,
                fx_rate_to_rmb=safe_float(fields.get(CONFIG_FIELDS["fx_rate_to_rmb"])) or 1.0,
                platform_fee_rate=safe_float(fields.get(CONFIG_FIELDS["platform_fee_rate"])) or 0.20,
                accessory_head_shipping_rmb=safe_float(fields.get(CONFIG_FIELDS["accessory_head_shipping_rmb"])) or 0.2,
                light_top_head_shipping_rmb=safe_float(fields.get(CONFIG_FIELDS["light_top_head_shipping_rmb"])) or 2.0,
                heavy_apparel_head_shipping_rmb=safe_float(fields.get(CONFIG_FIELDS["heavy_apparel_head_shipping_rmb"])) or 5.0,
                accio_chat_id=safe_text(fields.get(CONFIG_FIELDS["accio_chat_id"])),
                enable_hermes=normalize_bool(fields.get(CONFIG_FIELDS["enable_hermes"])),
                rule_version=safe_text(fields.get(CONFIG_FIELDS["rule_version"])) or "v1",
                note=safe_text(fields.get(CONFIG_FIELDS["note"])),
            )
            if not config.enabled:
                continue
            configs[(config.country, config.category)] = config
        return configs

    def process_pending_batches(
        self,
        batch_id: Optional[str] = None,
        limit: Optional[int] = None,
        send_accio: bool = True,
    ) -> Dict[str, Any]:
        batches = self._load_batch_candidates(batch_id=batch_id)
        if limit is not None:
            batches = batches[:limit]
        configs = self.load_rule_configs()
        summary = {"processed": 0, "failed": 0, "skipped": 0, "batches": []}
        for batch in batches:
            rule_config = self._match_rule_config(batch, configs)
            if not rule_config:
                self._mark_batch_failed(batch, "未找到匹配的参数配置")
                summary["failed"] += 1
                summary["batches"].append({"batch_id": batch.batch_id, "status": "failed"})
                continue
            try:
                result = self._process_single_batch(batch, rule_config, send_accio=send_accio)
                summary["processed"] += 1
                summary["batches"].append(result)
            except Exception as exc:
                self._mark_batch_failed(batch, str(exc))
                summary["failed"] += 1
                summary["batches"].append({"batch_id": batch.batch_id, "status": "failed", "error": str(exc)})
        return summary

    def collect_accio_results(
        self,
        batch_id: Optional[str] = None,
        limit: Optional[int] = None,
        run_hermes: bool = True,
    ) -> Dict[str, Any]:
        if not self.batch_client or not self.workspace_client:
            raise RuntimeError("Accio 回收至少需要批次表与工作台配置")
        candidates = self._load_accio_candidates(batch_id=batch_id)
        if limit is not None:
            candidates = candidates[:limit]
        configs = self.load_rule_configs()
        summary = {"updated": 0, "timed_out": 0, "pending": 0}
        for batch in candidates:
            rule_config = self._match_rule_config(batch, configs)
            if not rule_config:
                continue
            local_batch = self.db.get_batch(batch.batch_id) or {}
            requested_at = parse_datetime_value(
                local_batch.get("accio_response_at")
                or local_batch.get("accio_requested_at")
                or local_batch.get("last_updated_at")
            )
            if requested_at:
                age_hours = (datetime.utcnow() - requested_at).total_seconds() / 3600.0
                if age_hours > self.settings.accio_timeout_hours:
                    self._mark_accio_timeout(batch)
                    summary["timed_out"] += 1
                    continue
            chat_id = safe_text(local_batch.get("accio_chat_id")) or rule_config.accio_chat_id
            if not chat_id:
                summary["pending"] += 1
                continue
            since_ts = requested_at.timestamp() if requested_at else None
            messages = self.messenger.list_chat_messages(chat_id, since_timestamp=since_ts) if self.messenger else []
            valid_work_ids = [
                safe_text(row.get("work_id"))
                for row in self.db.list_selection_records(batch.batch_id)
                if safe_text(row.get("work_id"))
            ]
            response = parse_accio_response_from_messages(
                messages,
                batch.batch_id,
                valid_work_ids=valid_work_ids,
            )
            # Fallback: if the request timestamp drifted or the batch was resent,
            # scan recent chat history without the time window and rely on batch_id/work_id matching.
            if not response and self.messenger:
                recent_messages = self.messenger.list_chat_messages(chat_id)
                response = parse_accio_response_from_messages(
                    recent_messages,
                    batch.batch_id,
                    valid_work_ids=valid_work_ids,
                )
            if not response:
                summary["pending"] += 1
                continue
            self._apply_accio_response(batch, response, rule_config)
            summary["updated"] += 1
            if run_hermes:
                self._run_hermes_for_batch(batch, rule_config)
        return summary

    def run_hermes_for_batches(self, batch_id: Optional[str] = None, limit: Optional[int] = None) -> Dict[str, Any]:
        batches = self._load_hermes_candidates(batch_id=batch_id)
        if limit is not None:
            batches = batches[:limit]
        configs = self.load_rule_configs()
        summary = {"completed": 0, "failed": 0}
        for batch in batches:
            rule_config = self._match_rule_config(batch, configs)
            if not rule_config:
                continue
            try:
                self._run_hermes_for_batch(batch, rule_config)
                summary["completed"] += 1
            except Exception:
                summary["failed"] += 1
        return summary

    def sync_followups(self) -> Dict[str, Any]:
        if not self.workspace_client or not self.followup_client:
            raise RuntimeError("同步跟进需要工作台表与复盘表配置")
        workspace_records = _list_table_records(self.workspace_client, self.settings.feishu_read_page_size)
        existing_followups = {
            safe_text(_record_fields(record).get(FOLLOWUP_FIELDS["followup_id"])): (record.record_id, _record_fields(record))
            for record in _list_table_records(self.followup_client, self.settings.feishu_read_page_size)
        }
        create_rows = []
        update_rows = []
        local_rows = []
        for record in workspace_records:
            fields = _record_fields(record)
            workspace_payload = self._workspace_record_to_local(fields)
            if not should_sync_followup(workspace_payload):
                continue
            followup_row = build_followup_row(workspace_payload, existing_row=self._local_followup_by_work_id(workspace_payload["work_id"]))
            local_rows.append(dict(followup_row))
            remote_fields = _followup_remote_fields(followup_row)
            existing = existing_followups.get(followup_row["followup_id"])
            if existing:
                update_rows.append({"record_id": existing[0], "fields": remote_fields})
            else:
                create_rows.append({"fields": remote_fields})
            self.db.upsert_selection_records(
                [
                    {
                        "work_id": workspace_payload["work_id"],
                        "manual_final_status": workspace_payload.get("manual_final_status"),
                        "owner": workspace_payload.get("owner"),
                        "manual_note": workspace_payload.get("manual_note"),
                        "followup_flag": 1,
                        "processed_at": utc_now_iso(),
                    }
                ]
            )
        for chunk in _chunked(create_rows, self.settings.feishu_write_batch_size):
            self.followup_client.batch_create_records(chunk)
        for chunk in _chunked(update_rows, self.settings.feishu_write_batch_size):
            self.followup_client.batch_update_records(chunk)
        self.db.upsert_followup_records(local_rows)
        return {"created": len(create_rows), "updated": len(update_rows)}

    def cleanup_archives(self) -> Dict[str, Any]:
        removed = 0
        now = time.time()
        threshold = self.settings.archive_retention_days * 24 * 3600
        root = self.settings.archive_root
        if not root.exists():
            return {"removed": 0}
        for child in root.iterdir():
            if not child.is_dir():
                continue
            if now - child.stat().st_mtime > threshold:
                shutil.rmtree(child, ignore_errors=True)
                removed += 1
        return {"removed": removed}

    def _load_batch_candidates(self, batch_id: Optional[str] = None) -> List[BatchRecord]:
        if not self.batch_client:
            raise RuntimeError("未配置批次管理表")
        records = _list_table_records(self.batch_client, self.settings.feishu_read_page_size)
        batches = []
        for record in records:
            fields = _record_fields(record)
            current_batch_id = safe_text(fields.get(BATCH_FIELDS["batch_id"]))
            if not current_batch_id:
                continue
            if batch_id and current_batch_id != batch_id:
                continue
            attachments = coerce_attachment_list(fields.get(BATCH_FIELDS["attachments"]))
            overall_status = safe_text(fields.get(BATCH_FIELDS["overall_status"]))
            if not batch_id:
                if overall_status != self.settings.pending_batch_status:
                    continue
                if not attachments:
                    continue
            batches.append(self._to_batch_record(record))
        batches.sort(key=lambda item: parse_datetime_value(item.snapshot_time) or datetime.min)
        return batches

    def _load_accio_candidates(self, batch_id: Optional[str] = None) -> List[BatchRecord]:
        records = self._load_batch_candidates(batch_id=batch_id) if batch_id else []
        if batch_id:
            return records
        all_records = _list_table_records(self.batch_client, self.settings.feishu_read_page_size)
        batches = []
        for record in all_records:
            fields = _record_fields(record)
            accio_status = safe_text(fields.get(BATCH_FIELDS["accio_status"]))
            overall_status = safe_text(fields.get(BATCH_FIELDS["overall_status"]))
            if accio_status not in {"已发送", "待回收"} and overall_status != "规则完成待Accio":
                continue
            batches.append(self._to_batch_record(record))
        return batches

    def _load_hermes_candidates(self, batch_id: Optional[str] = None) -> List[BatchRecord]:
        all_records = _list_table_records(self.batch_client, self.settings.feishu_read_page_size)
        batches = []
        for record in all_records:
            batch = self._to_batch_record(record)
            if batch_id and batch.batch_id != batch_id:
                continue
            if batch_id:
                batches.append(batch)
                continue
            fields = _record_fields(record)
            accio_status = safe_text(fields.get(BATCH_FIELDS["accio_status"]))
            hermes_status = safe_text(fields.get(BATCH_FIELDS["hermes_status"]))
            if accio_status in {"已发送", "已回收", "超时"} and hermes_status != "已完成":
                batches.append(batch)
        return batches

    def _process_single_batch(self, batch: BatchRecord, rule_config: RuleConfig, send_accio: bool) -> Dict[str, Any]:
        archive_dir = ensure_dir(self.settings.archive_root / batch.batch_id)
        self._persist_batch_state(
            batch,
            {
                BATCH_FIELDS["download_status"]: "进行中",
                BATCH_FIELDS["overall_status"]: "待下载",
            },
        )
        file_path, file_hash = self._download_batch_attachment(batch)
        self._persist_batch_state(
            batch,
            {
                BATCH_FIELDS["download_status"]: "已完成",
                BATCH_FIELDS["overall_status"]: "已下载待入库",
            },
            extra={
                "download_time": utc_now_iso(),
                "local_file_path": str(file_path),
                "file_hash": file_hash,
            },
        )

        import_result = normalize_fastmoss_rows(
            str(file_path),
            batch.batch_id,
            batch.snapshot_time,
            rule_config,
        )
        normalized_csv_path = archive_dir / "normalized.csv"
        write_csv_rows(normalized_csv_path, import_result.records)
        self.db.upsert_product_snapshots(import_result.records)
        self._persist_batch_state(
            batch,
            {
                BATCH_FIELDS["import_status"]: "已完成",
                BATCH_FIELDS["overall_status"]: "已入库待规则筛选",
                BATCH_FIELDS["error_message"]: "；".join(import_result.warnings[:5]),
            },
            extra={"import_time": utc_now_iso()},
        )

        rule_result = evaluate_rule_engine(import_result.records, rule_config)
        shortlist_rows = [self._selection_archive_row(batch, row) for row in rule_result.shortlist]
        shortlist_json_path = archive_dir / "shortlist.json"
        shortlist_json_path.write_text(json.dumps(shortlist_rows, ensure_ascii=False, indent=2), encoding="utf-8")
        self.db.upsert_selection_records(shortlist_rows)
        self._upsert_workspace_selection_rows(shortlist_rows)
        if not shortlist_rows:
            self._persist_batch_state(
                batch,
                {
                    BATCH_FIELDS["rule_status"]: "已完成",
                    BATCH_FIELDS["overall_status"]: "已完成",
                    BATCH_FIELDS["error_message"]: "本批次无规则通过商品",
                },
            )
            return {"batch_id": batch.batch_id, "status": "completed", "shortlist_count": 0}

        pending_fields = {
            BATCH_FIELDS["rule_status"]: "已完成",
            BATCH_FIELDS["overall_status"]: "规则完成待Accio",
        }
        if not safe_text(batch.fields.get(BATCH_FIELDS["accio_status"])):
            pending_fields[BATCH_FIELDS["accio_status"]] = "未开始"
        if not safe_text(batch.fields.get(BATCH_FIELDS["hermes_status"])):
            pending_fields[BATCH_FIELDS["hermes_status"]] = "未开始"
        self._persist_batch_state(batch, pending_fields)

        if send_accio:
            chat_id = rule_config.accio_chat_id
            if not chat_id:
                raise RuntimeError("配置缺少 Accio目标群ID")
            self._send_accio_request(batch, shortlist_rows, chat_id, archive_dir)
        return {"batch_id": batch.batch_id, "status": "completed", "shortlist_count": len(shortlist_rows)}

    def _send_accio_request(
        self,
        batch: BatchRecord,
        shortlist_rows: List[Dict[str, Any]],
        chat_id: str,
        archive_dir: Path,
    ) -> None:
        request_rows = build_accio_request_rows(shortlist_rows)
        request_path = export_accio_request(request_rows, archive_dir / "accio_request.xlsx")
        image_bundle_path = export_accio_image_bundle(
            shortlist_rows,
            archive_dir / "accio_images.zip",
            self.image_downloader,
        )
        if self.messenger:
            self.messenger.send_file(chat_id, request_path)
            if image_bundle_path:
                self.messenger.send_file(chat_id, image_bundle_path)
            trigger_text = build_accio_message(batch.batch_id, len(request_rows))
            if self.settings.accio_bot_open_id and hasattr(self.messenger, "send_text_with_mention"):
                self.messenger.send_text_with_mention(
                    chat_id,
                    self.settings.accio_bot_open_id,
                    self.settings.accio_bot_name,
                    trigger_text,
                )
            else:
                self.messenger.send_text(chat_id, trigger_text)
        local_updates = []
        for row in shortlist_rows:
            local_updates.append({"work_id": row["work_id"], "accio_status": "待回收"})
        self.db.upsert_selection_records(local_updates)
        self._persist_batch_state(
            batch,
            {
                BATCH_FIELDS["accio_status"]: "已发送",
                BATCH_FIELDS["overall_status"]: "规则完成待Accio",
            },
            extra={
                "accio_chat_id": chat_id,
                "accio_requested_at": utc_now_iso(),
            },
        )

    def _apply_accio_response(self, batch: BatchRecord, response: Any, rule_config: RuleConfig) -> bool:
        archive_dir = ensure_dir(self.settings.archive_root / batch.batch_id)
        (archive_dir / "accio_response.json").write_text(
            json.dumps(response.raw_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        local_rows = {row["work_id"]: row for row in self.db.list_selection_records(batch.batch_id)}
        local_updates = []
        for work_id, current in local_rows.items():
            accio_item = response.items.get(work_id)
            if not accio_item:
                continue
            margins = _calculate_margins(
                current.get("price_low_rmb"),
                accio_item.get("procurement_price_rmb"),
                current.get("commission_rate"),
                batch.category,
                current.get("product_name"),
                rule_config,
            )
            local_updates.append(
                {
                    "work_id": work_id,
                    "accio_status": "已回收",
                    "accio_source_url": accio_item.get("accio_source_url"),
                    "procurement_price_rmb": accio_item.get("procurement_price_rmb"),
                    "procurement_price_range": accio_item.get("procurement_price_range"),
                    "match_confidence": accio_item.get("match_confidence"),
                    "abnormal_low_price": accio_item.get("abnormal_low_price"),
                    "accio_note": accio_item.get("accio_note"),
                    "pricing_reference_rmb": margins["pricing_reference_rmb"],
                    "platform_fee_rate": margins["platform_fee_rate"],
                    "platform_fee_amount": margins["platform_fee_amount"],
                    "head_shipping_rmb": margins["head_shipping_rmb"],
                    "head_shipping_rule": margins["head_shipping_rule"],
                    "gross_margin_amount": margins["gross_margin_amount"],
                    "gross_margin_rate": margins["gross_margin_rate"],
                    "distribution_margin_amount": margins["distribution_margin_amount"],
                    "distribution_margin_rate": margins["distribution_margin_rate"],
                }
            )
        workspace_updates = []
        for item in local_updates:
            workspace_updates.append(
                {
                    "work_id": item["work_id"],
                    "推荐采购价_rmb": item.get("procurement_price_rmb"),
                    "Accio备注": build_accio_workspace_note(
                        item.get("accio_source_url"),
                        item.get("accio_note"),
                    ),
                    "商品粗毛利率": item.get("gross_margin_rate"),
                    "分销后毛利率": item.get("distribution_margin_rate"),
                }
            )
        self.db.upsert_selection_records(local_updates)
        self._upsert_workspace_partial_updates(workspace_updates)
        refreshed_rows = self.db.list_selection_records(batch.batch_id)
        self._persist_batch_progress(batch, refreshed_rows, extra={"accio_response_at": utc_now_iso()})
        recovered_count = len(self._recovered_selection_rows(refreshed_rows))
        total_count = len(refreshed_rows)
        return total_count > 0 and recovered_count >= total_count

    def _run_hermes_for_batch(self, batch: BatchRecord, rule_config: RuleConfig) -> int:
        selection_rows = self.db.list_selection_records(batch.batch_id)
        if not selection_rows:
            return 0
        eligible_rows = self._eligible_hermes_rows(selection_rows)
        if not eligible_rows:
            self._persist_batch_progress(batch, selection_rows)
            return 0
        if not rule_config.enable_hermes:
            local_updates = []
            workspace_updates = []
            for row in eligible_rows:
                local_updates.append(
                    {
                        "work_id": row["work_id"],
                        "hermes_status": "已完成",
                        "recommended_action": "待人工判断",
                        "recommendation_reason": "参数配置关闭 Hermes，已跳过自动判断",
                    }
                )
                workspace_updates.append(
                    {
                        "work_id": row["work_id"],
                        "Hermes推荐动作": "待人工判断",
                        "Hermes推荐理由": "参数配置关闭 Hermes，已跳过自动判断",
                    }
                )
            self.db.upsert_selection_records(local_updates)
            self._upsert_workspace_partial_updates(workspace_updates)
            refreshed_rows = self.db.list_selection_records(batch.batch_id)
            self._persist_batch_progress(batch, refreshed_rows, extra={"hermes_completed_at": utc_now_iso()})
            return len(eligible_rows)

        archive_root = ensure_dir(self.settings.archive_root / batch.batch_id / "hermes")
        chunk_size = max(1, self.settings.hermes_chunk_size)
        total_eligible = len(eligible_rows)
        processed_count = 0
        errors = []
        for chunk_index, chunk_rows in enumerate(_chunked(eligible_rows, chunk_size), 1):
            archive_dir = ensure_dir(archive_root / "chunk_{index:02d}".format(index=chunk_index))
            hermes_rows = [dict(row, shortlist_count_override=total_eligible) for row in chunk_rows]
            result = self.hermes_runner(
                batch.batch_id,
                hermes_rows,
                archive_dir,
                self.settings.hermes_command_template,
                self.settings.hermes_timeout_seconds,
            )
            if result.status != "success":
                error = result.error or "Hermes 执行失败"
                errors.append("chunk_{index:02d}:{error}".format(index=chunk_index, error=error))
                local_updates = [{"work_id": row["work_id"], "hermes_status": "失败", "risk_warning": error} for row in chunk_rows]
                workspace_updates = [{"work_id": row["work_id"], "Hermes风险提醒": error} for row in chunk_rows]
                self.db.upsert_selection_records(local_updates)
                self._upsert_workspace_partial_updates(workspace_updates)
                continue

            local_updates = []
            workspace_updates = []
            for row in chunk_rows:
                decision = result.items.get(row["work_id"])
                if not decision:
                    local_updates.append({"work_id": row["work_id"], "hermes_status": "失败", "risk_warning": "Hermes 未返回该商品"})
                    workspace_updates.append({"work_id": row["work_id"], "Hermes风险提醒": "Hermes 未返回该商品"})
                    errors.append(
                        "chunk_{index:02d}:{work_id} missing".format(
                            index=chunk_index,
                            work_id=row["work_id"],
                        )
                    )
                    continue
                local_updates.append(
                    {
                        "work_id": row["work_id"],
                        "hermes_status": "已完成",
                        "content_potential_score": decision.get("content_potential_score"),
                        "differentiation_score": decision.get("differentiation_score"),
                        "fit_judgment": decision.get("fit_judgment"),
                        "strategy_suggestion": decision.get("strategy_suggestion"),
                        "recommended_action": decision.get("recommended_action"),
                        "recommendation_reason": decision.get("recommendation_reason"),
                        "risk_warning": decision.get("risk_warning"),
                    }
                )
                workspace_updates.append(
                    {
                        "work_id": row["work_id"],
                        "打法建议": decision.get("strategy_suggestion"),
                        "Hermes推荐动作": decision.get("recommended_action"),
                        "Hermes推荐理由": decision.get("recommendation_reason"),
                        "Hermes风险提醒": decision.get("risk_warning"),
                    }
                )
                processed_count += 1
            self.db.upsert_selection_records(local_updates)
            self._upsert_workspace_partial_updates(workspace_updates)
        refreshed_rows = self.db.list_selection_records(batch.batch_id)
        self._persist_batch_progress(
            batch,
            refreshed_rows,
            extra={"hermes_completed_at": utc_now_iso()} if processed_count > 0 else None,
            extra_error=" | ".join(errors),
        )
        return processed_count

    def _mark_accio_timeout(self, batch: BatchRecord) -> None:
        selection_rows = self.db.list_selection_records(batch.batch_id)
        self.db.upsert_selection_records([{"work_id": row["work_id"], "accio_status": "待人工补录"} for row in selection_rows])
        self._persist_batch_state(
            batch,
            {
                BATCH_FIELDS["accio_status"]: "超时",
                BATCH_FIELDS["error_message"]: "Accio 回收超时",
            },
        )

    def _match_rule_config(self, batch: BatchRecord, configs: Dict[Tuple[str, str], RuleConfig]) -> Optional[RuleConfig]:
        exact = configs.get((batch.country, batch.category))
        if exact:
            return exact
        fallback_country = configs.get((batch.country, ""))
        if fallback_country:
            return fallback_country
        fallback_category = configs.get(("", batch.category))
        if fallback_category:
            return fallback_category
        return None

    def _recovered_selection_rows(self, selection_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [
            row
            for row in selection_rows
            if safe_float(row.get("procurement_price_rmb")) is not None
            or safe_text(row.get("accio_status")) == "无结果跳过"
        ]

    def _is_skipped_hermes_placeholder(self, row: Dict[str, Any]) -> bool:
        return (
            safe_text(row.get("hermes_status")) == "已完成"
            and safe_text(row.get("recommended_action")) == "待人工判断"
            and safe_text(row.get("recommendation_reason"))
            in {
                "参数配置关闭 Hermes，已跳过自动判断",
                "Accio 未返回有效货源，已跳过自动判断",
            }
        )

    def _eligible_hermes_rows(self, selection_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        rows = []
        for row in selection_rows:
            if safe_float(row.get("procurement_price_rmb")) is None:
                continue
            if safe_text(row.get("hermes_status")) == "已完成" and not self._is_skipped_hermes_placeholder(row):
                continue
            rows.append(row)
        return rows

    def _build_batch_progress_error_message(
        self,
        total_count: int,
        recovered_count: int,
        hermes_completed_count: int,
        extra_error: str = "",
    ) -> str:
        parts = []
        if total_count > 0 and recovered_count < total_count:
            parts.append(
                "Accio 部分回收 {recovered}/{total}".format(
                    recovered=recovered_count,
                    total=total_count,
                )
            )
        if recovered_count > 0 and hermes_completed_count > 0 and hermes_completed_count < recovered_count:
            parts.append(
                "Hermes 已处理 {completed}/{recovered}".format(
                    completed=hermes_completed_count,
                    recovered=recovered_count,
                )
            )
        if recovered_count > 0 and hermes_completed_count == recovered_count and recovered_count < total_count:
            parts.append(
                "Hermes 已处理 {completed}/{recovered}".format(
                    completed=hermes_completed_count,
                    recovered=recovered_count,
                )
            )
        if safe_text(extra_error):
            parts.append("Hermes 异常: {error}".format(error=safe_text(extra_error)))
        return "；".join(parts)

    def _persist_batch_progress(
        self,
        batch: BatchRecord,
        selection_rows: List[Dict[str, Any]],
        extra: Optional[Dict[str, Any]] = None,
        extra_error: str = "",
    ) -> None:
        total_count = len(selection_rows)
        recovered_rows = self._recovered_selection_rows(selection_rows)
        recovered_count = len(recovered_rows)
        hermes_completed_count = len(
            [row for row in recovered_rows if safe_text(row.get("hermes_status")) == "已完成"]
        )
        hermes_failed_count = len(
            [row for row in recovered_rows if safe_text(row.get("hermes_status")) == "失败"]
        )

        if recovered_count >= total_count and total_count > 0:
            accio_status = "已回收"
            if hermes_completed_count >= recovered_count:
                hermes_status = "已完成"
                overall_status = "Hermes完成待人审"
            elif hermes_completed_count > 0:
                hermes_status = "部分完成"
                overall_status = "Accio完成待Hermes"
            elif hermes_failed_count > 0:
                hermes_status = "失败"
                overall_status = "Accio完成待Hermes"
            else:
                hermes_status = "未开始"
                overall_status = "Accio完成待Hermes"
        else:
            accio_status = "已发送"
            if hermes_completed_count > 0:
                hermes_status = "部分完成"
            elif hermes_failed_count > 0:
                hermes_status = "失败"
            else:
                hermes_status = "未开始"
            overall_status = "规则完成待Accio"

        error_message = self._build_batch_progress_error_message(
            total_count=total_count,
            recovered_count=recovered_count,
            hermes_completed_count=hermes_completed_count,
            extra_error=extra_error,
        )
        if overall_status == "Hermes完成待人审" and not safe_text(extra_error):
            error_message = ""

        self._persist_batch_state(
            batch,
            {
                BATCH_FIELDS["accio_status"]: accio_status,
                BATCH_FIELDS["hermes_status"]: hermes_status,
                BATCH_FIELDS["overall_status"]: overall_status,
                BATCH_FIELDS["error_message"]: error_message,
            },
            extra=extra,
        )

    def _download_batch_attachment(self, batch: BatchRecord) -> Tuple[Path, str]:
        local_batch = self.db.get_batch(batch.batch_id) or {}
        existing_path = safe_text(local_batch.get("local_file_path"))
        existing_hash = safe_text(local_batch.get("file_hash"))
        if existing_path and existing_hash and Path(existing_path).exists():
            return Path(existing_path), existing_hash
        if not batch.attachments:
            raise RuntimeError("批次缺少原始文件附件")
        attachment = batch.attachments[0]
        content, file_name, _, _ = self.batch_client.download_attachment_bytes(attachment)
        file_hash = sha256_bytes(content)
        download_dir = ensure_dir(self.settings.download_root / batch.batch_id)
        target_path = download_dir / (safe_text(batch.raw_file_name) or file_name)
        target_path.write_bytes(content)
        return target_path, file_hash

    def _persist_batch_state(self, batch: BatchRecord, remote_fields: Dict[str, Any], extra: Optional[Dict[str, Any]] = None) -> None:
        payload = dict(remote_fields)
        payload.setdefault(BATCH_FIELDS["last_updated_at"], utc_now_iso())
        for datetime_field in (BATCH_FIELDS["snapshot_time"], BATCH_FIELDS["last_updated_at"]):
            if datetime_field in payload:
                payload[datetime_field] = to_feishu_datetime_millis(payload[datetime_field])
        if self.batch_client and batch.record_id:
            self.batch_client.update_record_fields(batch.record_id, payload)
        current = self.db.get_batch(batch.batch_id) or {}
        current.update(
            {
                "batch_id": batch.batch_id,
                "batch_record_id": batch.record_id,
                "country": batch.country,
                "category": batch.category,
                "snapshot_time": batch.snapshot_time,
                "raw_file_name": batch.raw_file_name,
                "raw_record_count": batch.raw_record_count,
                "raw_record_json": json_dumps(batch.fields),
            }
        )
        mapping = {
            "download_status": BATCH_FIELDS["download_status"],
            "import_status": BATCH_FIELDS["import_status"],
            "rule_status": BATCH_FIELDS["rule_status"],
            "accio_status": BATCH_FIELDS["accio_status"],
            "hermes_status": BATCH_FIELDS["hermes_status"],
            "overall_status": BATCH_FIELDS["overall_status"],
            "error_message": BATCH_FIELDS["error_message"],
            "retry_count": BATCH_FIELDS["retry_count"],
            "last_updated_at": BATCH_FIELDS["last_updated_at"],
        }
        for local_key, remote_key in mapping.items():
            if remote_key in payload:
                current[local_key] = payload.get(remote_key)
        if extra:
            current.update(extra)
        self.db.upsert_batch(current)

    def _selection_archive_row(self, batch: BatchRecord, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "work_id": build_work_id(batch.batch_id, row["product_id"]),
            "batch_id": batch.batch_id,
            "product_id": row.get("product_id"),
            "country": batch.country,
            "category": batch.category,
            "product_name": row.get("product_name"),
            "shop_name": row.get("shop_name"),
            "product_image": row.get("product_image"),
            "product_url": row.get("product_url"),
            "listing_days": row.get("listing_days"),
            "price_raw": row.get("price_raw"),
            "price_low_local": row.get("price_low_local"),
            "price_high_local": row.get("price_high_local"),
            "price_mid_local": row.get("price_mid_local"),
            "fx_rate_to_rmb": row.get("fx_rate_to_rmb"),
            "price_low_rmb": row.get("price_low_rmb"),
            "price_high_rmb": row.get("price_high_rmb"),
            "price_mid_rmb": row.get("price_mid_rmb"),
            "sales_7d": row.get("sales_7d"),
            "revenue_7d": row.get("revenue_7d"),
            "avg_price_7d_rmb": row.get("avg_price_7d_rmb"),
            "total_sales": row.get("total_sales"),
            "total_revenue": row.get("total_revenue"),
            "avg_price_total_rmb": row.get("avg_price_total_rmb"),
            "creator_count": row.get("creator_count"),
            "creator_order_rate": row.get("creator_order_rate"),
            "video_count": row.get("video_count"),
            "live_count": row.get("live_count"),
            "commission_rate": row.get("commission_rate"),
            "pool_type": row.get("pool_type"),
            "video_competition_density": row.get("video_competition_density"),
            "creator_competition_density": row.get("creator_competition_density"),
            "competition_maturity": row.get("competition_maturity"),
            "source_rule_score": row.get("source_rule_score", row.get("rule_score")),
            "rule_score": row.get("rule_score", row.get("source_rule_score")),
            "rule_pass_reason": row.get("rule_pass_reason"),
            "rule_status": row.get("rule_status"),
            "accio_status": "未开始",
            "hermes_status": "未开始",
            "followup_flag": 0,
            "record_json": json_dumps(row),
        }

    def _upsert_workspace_selection_rows(self, selection_rows: List[Dict[str, Any]]) -> None:
        existing = self._workspace_record_map()
        self._prepare_workspace_image_attachments(selection_rows, existing)
        create_rows = []
        update_rows = []
        for row in selection_rows:
            fields = _workspace_remote_fields(row)
            current = existing.get(row["work_id"])
            if current:
                update_rows.append({"record_id": current[0], "fields": fields})
            else:
                create_rows.append({"fields": fields})
        for chunk in _chunked(create_rows, self.settings.feishu_write_batch_size):
            self.workspace_client.batch_create_records(chunk)
        for chunk in _chunked(update_rows, self.settings.feishu_write_batch_size):
            self.workspace_client.batch_update_records(chunk)

    def _upsert_workspace_partial_updates(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        existing = self._workspace_record_map()
        update_rows = []
        for row in rows:
            work_id = safe_text(row.get("work_id"))
            if not work_id or work_id not in existing:
                continue
            record_id = existing[work_id][0]
            fields = dict(row)
            fields.pop("work_id", None)
            update_rows.append({"record_id": record_id, "fields": fields})
        for chunk in _chunked(update_rows, self.settings.feishu_write_batch_size):
            self.workspace_client.batch_update_records(chunk)

    def _workspace_record_map(self) -> Dict[str, Tuple[str, Dict[str, Any]]]:
        if not self.workspace_client:
            return {}
        mapping = {}
        for record in _list_table_records(self.workspace_client, self.settings.feishu_read_page_size):
            fields = _record_fields(record)
            work_id = safe_text(fields.get(WORKSPACE_FIELDS["work_id"]))
            if work_id:
                mapping[work_id] = (_record_id(record), fields)
        return mapping

    def _workspace_record_to_local(self, fields: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "work_id": safe_text(fields.get(WORKSPACE_FIELDS["work_id"])),
            "batch_id": safe_text(fields.get(WORKSPACE_FIELDS["batch_id"])),
            "product_id": safe_text(fields.get(WORKSPACE_FIELDS["product_id"])),
            "country": safe_text(fields.get(WORKSPACE_FIELDS["country"])),
            "category": safe_text(fields.get(WORKSPACE_FIELDS["category"])),
            "product_name": safe_text(fields.get(WORKSPACE_FIELDS["product_name"])),
            "manual_final_status": safe_text(fields.get(WORKSPACE_FIELDS["manual_final_status"])),
            "owner": safe_text(fields.get(WORKSPACE_FIELDS["owner"])),
            "manual_note": safe_text(fields.get(WORKSPACE_FIELDS["manual_note"])),
            "followup_flag": 1 if normalize_bool(fields.get(WORKSPACE_FIELDS["followup_flag"])) else 0,
            "strategy_suggestion": "",
            "recommended_action": "",
        }

    def _prepare_workspace_image_attachments(
        self,
        selection_rows: List[Dict[str, Any]],
        existing: Dict[str, Tuple[str, Dict[str, Any]]],
    ) -> None:
        if not self.workspace_client or not hasattr(self.workspace_client, "upload_attachment"):
            return
        upload_cache: Dict[str, Dict[str, Any]] = {}
        for row in selection_rows:
            work_id = safe_text(row.get("work_id"))
            existing_item = existing.get(work_id)
            if existing_item:
                current_attachments = coerce_attachment_list(
                    existing_item[1].get(WORKSPACE_FIELDS["product_image_attachment"])
                )
                if current_attachments:
                    row["product_image_attachment"] = current_attachments
                    continue
            image_url = safe_text(row.get("product_image"))
            if not image_url:
                row["product_image_attachment"] = []
                continue
            cached = upload_cache.get(image_url)
            if cached:
                row["product_image_attachment"] = [dict(cached)]
                continue
            try:
                content, file_name, content_type, size = self.image_downloader(image_url, row)
                uploaded = self.workspace_client.upload_attachment(
                    content=content,
                    file_name=file_name,
                    content_type=content_type,
                    size=size,
                )
                upload_cache[image_url] = dict(uploaded)
                row["product_image_attachment"] = [dict(uploaded)]
            except Exception:
                row["product_image_attachment"] = []

    def _local_followup_by_work_id(self, work_id: str) -> Optional[Dict[str, Any]]:
        rows = self.db.fetchall(
            "SELECT * FROM followup_result_archive WHERE source_work_id = :work_id LIMIT 1",
            {"work_id": work_id},
        )
        return rows[0] if rows else None

    def _to_batch_record(self, record: TableRecord) -> BatchRecord:
        fields = _record_fields(record)
        return BatchRecord(
            record_id=_record_id(record),
            batch_id=safe_text(fields.get(BATCH_FIELDS["batch_id"])),
            data_source="",
            country=safe_text(fields.get(BATCH_FIELDS["country"])),
            category=safe_text(fields.get(BATCH_FIELDS["category"])),
            snapshot_time=safe_text(fields.get(BATCH_FIELDS["snapshot_time"])),
            source_service="",
            attachments=coerce_attachment_list(fields.get(BATCH_FIELDS["attachments"])),
            raw_file_name=safe_text(fields.get(BATCH_FIELDS["raw_file_name"])),
            raw_record_count=safe_int(fields.get(BATCH_FIELDS["raw_record_count"])),
            overall_status=safe_text(fields.get(BATCH_FIELDS["overall_status"])),
            retry_count=safe_int(fields.get(BATCH_FIELDS["retry_count"])) or 0,
            captured_at=parse_datetime_value(fields.get(BATCH_FIELDS["snapshot_time"])),
            fields=fields,
        )

    def _mark_batch_failed(self, batch: BatchRecord, error_message: str) -> None:
        retry_count = batch.retry_count + 1
        self._persist_batch_state(
            batch,
            {
                BATCH_FIELDS["overall_status"]: "失败",
                BATCH_FIELDS["error_message"]: error_message[:1000],
                BATCH_FIELDS["retry_count"]: retry_count,
            },
        )


def _build_bitable_client(feishu_url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise ValueError("无法解析飞书 URL: {url}".format(url=feishu_url))
    app_token = info.app_token
    if "/wiki/" in info.original_url:
        app_token = resolve_wiki_bitable_app_token(info.app_token)
    return FeishuBitableClient(app_token=app_token, table_id=info.table_id)


def _list_table_records(client: Any, page_size: int) -> List[TableRecord]:
    if hasattr(client, "list_records"):
        return client.list_records(page_size=page_size)
    if hasattr(client, "list_all_records"):
        items = client.list_all_records()
        normalized = []
        for item in items:
            normalized.append(TableRecord(record_id=item["record_id"], fields=item.get("fields", {})))
        return normalized
    raise RuntimeError("客户端不支持 list_records/list_all_records")


def _record_id(record: Any) -> str:
    record_id = getattr(record, "record_id", None)
    if record_id is not None:
        return safe_text(record_id)
    if isinstance(record, dict):
        return safe_text(record.get("record_id"))
    return ""


def _record_fields(record: Any) -> Dict[str, Any]:
    fields = getattr(record, "fields", None)
    if fields is not None:
        return fields
    if isinstance(record, dict):
        return record.get("fields", {})
    return {}


def _workspace_remote_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    fields = {}
    for key, field_name in WORKSPACE_FIELDS.items():
        if key == "followup_flag":
            fields[field_name] = bool(row.get(key))
            continue
        if key == "product_image":
            fields[field_name] = build_feishu_url_cell(row.get(key), text="查看图片")
            continue
        if key == "product_image_attachment":
            fields[field_name] = row.get(key) or []
            continue
        if key == "product_url":
            fields[field_name] = build_feishu_url_cell(row.get(key), text="查看商品")
            continue
        fields[field_name] = row.get(key)
    return fields


def _followup_remote_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    fields = {}
    for key, field_name in FOLLOWUP_FIELDS.items():
        value = row.get(key)
        if key == "writeback_experience_flag":
            fields[field_name] = bool(value)
        elif key == "followup_started_at":
            fields[field_name] = to_feishu_datetime_millis(value)
        else:
            fields[field_name] = value
    return fields


ACCESSORY_SHIPPING_KEYWORDS = (
    "配饰",
    "发饰",
    "饰品",
    "头饰",
    "耳环",
    "项链",
    "手链",
    "戒指",
    "发圈",
    "发夹",
    "scrunchie",
    "headband",
    "hair clip",
    "hairpin",
    "hair accessory",
    "jewelry",
    "bracelet",
    "necklace",
    "ring",
    "earring",
    "accessory",
    "accessories",
)
HEAVY_APPAREL_SHIPPING_KEYWORDS = (
    "厚女装",
    "厚款",
    "毛衣",
    "针织",
    "卫衣",
    "外套",
    "夹克",
    "棉服",
    "风衣",
    "大衣",
    "羽绒",
    "hoodie",
    "sweater",
    "knit",
    "cardigan",
    "coat",
    "jacket",
    "outerwear",
    "puffer",
    "fleece",
)
LIGHT_TOP_SHIPPING_KEYWORDS = (
    "轻上装",
    "上装",
    "t恤",
    "短袖",
    "背心",
    "吊带",
    "衬衫",
    "tee",
    "t-shirt",
    "shirt",
    "blouse",
    "vest",
    "tank",
    "camisole",
    "top",
)
APPAREL_CATEGORY_KEYWORDS = (
    "女装",
    "服饰",
    "服装",
    "apparel",
    "clothing",
    "fashion",
)


def _infer_head_shipping_rule(category: Any, product_name: Any) -> str:
    text = " ".join(part for part in [safe_text(category).lower(), safe_text(product_name).lower()] if part).strip()
    if not text:
        return "unknown"
    if any(keyword in text for keyword in ACCESSORY_SHIPPING_KEYWORDS):
        return "accessory"
    if any(keyword in text for keyword in HEAVY_APPAREL_SHIPPING_KEYWORDS):
        return "heavy_apparel"
    if any(keyword in text for keyword in LIGHT_TOP_SHIPPING_KEYWORDS):
        return "light_top"
    if any(keyword in text for keyword in APPAREL_CATEGORY_KEYWORDS):
        return "light_top"
    return "unknown"


def _resolve_head_shipping_rmb(rule_config: RuleConfig, category: Any, product_name: Any) -> Tuple[str, float]:
    shipping_rule = _infer_head_shipping_rule(category, product_name)
    if shipping_rule == "accessory":
        return shipping_rule, round(rule_config.accessory_head_shipping_rmb, 4)
    if shipping_rule == "heavy_apparel":
        return shipping_rule, round(rule_config.heavy_apparel_head_shipping_rmb, 4)
    if shipping_rule == "light_top":
        return shipping_rule, round(rule_config.light_top_head_shipping_rmb, 4)
    return shipping_rule, 0.0


def _calculate_margins(
    selling_price_rmb: Any,
    procurement_price_rmb: Any,
    commission_rate: Any,
    category: Any,
    product_name: Any,
    rule_config: RuleConfig,
) -> Dict[str, Optional[float]]:
    selling = safe_float(selling_price_rmb)
    procurement = safe_float(procurement_price_rmb)
    commission = safe_float(commission_rate) or 0.0
    platform_fee_rate = safe_float(rule_config.platform_fee_rate) or 0.0
    head_shipping_rule, head_shipping_rmb = _resolve_head_shipping_rmb(rule_config, category, product_name)
    if selling is None or selling == 0 or procurement is None:
        return {
            "pricing_reference_rmb": selling,
            "platform_fee_rate": platform_fee_rate,
            "platform_fee_amount": None,
            "head_shipping_rmb": head_shipping_rmb,
            "head_shipping_rule": head_shipping_rule,
            "gross_margin_amount": None,
            "gross_margin_rate": None,
            "distribution_margin_amount": None,
            "distribution_margin_rate": None,
        }
    platform_fee_amount = round(selling * platform_fee_rate, 4)
    gross_amount = round(selling - platform_fee_amount - head_shipping_rmb - procurement, 4)
    gross_rate = round(gross_amount / selling, 4)
    distribution_amount = round(gross_amount - selling * commission, 4)
    distribution_rate = round(distribution_amount / selling, 4)
    return {
        "pricing_reference_rmb": selling,
        "platform_fee_rate": platform_fee_rate,
        "platform_fee_amount": platform_fee_amount,
        "head_shipping_rmb": head_shipping_rmb,
        "head_shipping_rule": head_shipping_rule,
        "gross_margin_amount": gross_amount,
        "gross_margin_rate": gross_rate,
        "distribution_margin_amount": distribution_amount,
        "distribution_margin_rate": distribution_rate,
    }


def _download_image_bytes(image_url: str, row: Optional[Dict[str, Any]] = None) -> Tuple[bytes, str, str, int]:
    response = requests.get(
        image_url,
        timeout=30,
        headers={"User-Agent": "Mozilla/5.0"},
    )
    response.raise_for_status()
    content = response.content
    content_type = str(response.headers.get("content-type") or "application/octet-stream").split(";")[0].strip()
    suffix = Path(urlsplit(image_url).path).suffix
    if not suffix:
        suffix = mimetypes.guess_extension(content_type) or ".bin"
    product_id = safe_text((row or {}).get("product_id")) or "product_image"
    file_name = "{product_id}{suffix}".format(product_id=product_id, suffix=suffix)
    return content, file_name, content_type, len(content)
