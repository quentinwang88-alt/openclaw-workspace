#!/usr/bin/env python3
"""表读取与 CandidateTask 映射。"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.feishu import TableRecord, build_bitable_client
from src.models import CandidateTask, ConfigError, ReadFilterConfig, TableConfig, TableSourceConfig


class TableAdapter(object):
    def __init__(self, client_factory=None):
        self.client_factory = client_factory or build_bitable_client

    def load_table_configs(self, config_dir: Path, validate_source: bool = True) -> List[TableConfig]:
        configs = []
        for path in sorted(Path(config_dir).glob("*.json")):
            config = self.load_table_config(path, validate_source=validate_source)
            if config.enabled:
                configs.append(config)
        return configs

    def load_table_config(self, path: Path, validate_source: bool = True) -> TableConfig:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        field_map = payload.get("field_map") or {}
        if not field_map.get("product_images"):
            raise ConfigError("{path}: field_map.product_images 必填".format(path=path))

        read_filter_payload = payload.get("read_filter") or {}
        read_filter = ReadFilterConfig(
            status_field=str(read_filter_payload.get("status_field") or "分析状态").strip(),
            pending_values=[self._safe_text(item) for item in read_filter_payload.get("pending_values") or ["待处理", ""]],
        )
        source_payload = payload.get("source") or {}
        source = TableSourceConfig(
            feishu_url=self._safe_text(source_payload.get("feishu_url")),
            app_token=self._safe_text(source_payload.get("app_token")),
            bitable_table_id=self._safe_text(source_payload.get("bitable_table_id")),
        )
        if validate_source and not any([source.feishu_url, source.app_token and source.bitable_table_id]):
            raise ConfigError("{path}: 缺少 source.feishu_url 或 source.app_token + source.bitable_table_id".format(path=path))

        return TableConfig(
            table_id=self._safe_text(payload.get("table_id")) or path.stem,
            table_name=self._safe_text(payload.get("table_name")) or path.stem,
            enabled=bool(payload.get("enabled", True)),
            source_type=self._safe_text(payload.get("source_type")) or "manual",
            supported_manual_categories=[self._safe_text(item) for item in payload.get("supported_manual_categories") or []],
            read_filter=read_filter,
            field_map={str(key): self._safe_text(value) for key, value in field_map.items() if self._safe_text(value)},
            writeback_map={
                str(key): self._safe_text(value)
                for key, value in (payload.get("writeback_map") or {}).items()
                if self._safe_text(value)
            },
            batch_field=self._safe_text(payload.get("batch_field")),
            source=source,
            static_fields=dict(payload.get("static_fields") or {}),
        )

    def get_client(self, table_config: TableConfig):
        return self.client_factory(
            feishu_url=table_config.source.feishu_url,
            app_token=table_config.source.app_token,
            bitable_table_id=table_config.source.bitable_table_id,
        )

    def read_pending_records(
        self,
        table_config: TableConfig,
        client,
        limit: Optional[int] = None,
        record_scope: str = "pending",
        only_risk_tag: str = "",
    ) -> List[TableRecord]:
        status_field = table_config.read_filter.status_field
        v2_shadow_fields = [
            table_config.writeback_map.get("v2_total_score", ""),
            table_config.writeback_map.get("v2_suggested_action", ""),
            table_config.writeback_map.get("v2_matched_direction", ""),
            table_config.writeback_map.get("v2_differentiation_conclusion", ""),
        ]
        legacy_v2_marker_fields = [
            table_config.writeback_map.get("batch_priority_score", ""),
            table_config.writeback_map.get("market_match_score", ""),
            table_config.writeback_map.get("store_fit_score", ""),
            table_config.writeback_map.get("content_potential_score", ""),
            table_config.writeback_map.get("suggested_action", ""),
            table_config.writeback_map.get("feature_scores_json", ""),
        ]
        risk_tag_field = table_config.writeback_map.get("risk_tag", "")
        pending_values = {self._safe_text(item) for item in table_config.read_filter.pending_values}
        completed_values = {"已完成分析", "已完成"}
        records = []
        for record in client.list_records(page_size=100, limit=None):
            current_value = self._safe_text(record.fields.get(status_field))
            if record_scope == "all":
                should_select = True
            elif record_scope == "completed_missing_v2":
                marker_fields = [field for field in v2_shadow_fields if field] or legacy_v2_marker_fields
                has_v2_marker = any(
                    self._safe_text(record.fields.get(field_name)) for field_name in marker_fields if field_name
                )
                should_select = current_value in completed_values and not has_v2_marker
            elif record_scope == "completed":
                should_select = current_value in completed_values
            else:
                should_select = current_value in pending_values
            if should_select and only_risk_tag:
                current_risk_tag = self._safe_text(record.fields.get(risk_tag_field)) if risk_tag_field else ""
                should_select = current_risk_tag == only_risk_tag
            if should_select:
                records.append(record)
                if limit is not None and len(records) >= limit:
                    break
        return records

    def map_record_to_candidate_task(self, record: TableRecord, table_config: TableConfig, client=None) -> CandidateTask:
        fields = dict(record.fields)
        mapped_field_names = set(table_config.field_map.values())
        if table_config.batch_field:
            mapped_field_names.add(table_config.batch_field)

        product_title = self._mapped_text(fields, table_config, "product_title")
        if not product_title:
            product_title = self._mapped_text(fields, table_config, "product_name")

        static_fields = dict(getattr(table_config, "static_fields", {}) or {})
        static_extra = dict(static_fields.get("extra_fields") or {})
        extra_fields = {key: value for key, value in fields.items() if key not in mapped_field_names}
        extra_fields.update(static_extra)

        task = CandidateTask(
            source_table_id=table_config.table_id,
            source_record_id=record.record_id,
            source_type=table_config.source_type,
            batch_id=self._safe_text(fields.get(table_config.batch_field)) if table_config.batch_field else "",
            product_title=product_title,
            product_images=self._normalize_images(self._mapped_value(fields, table_config, "product_images"), client=client),
            cost_price=self._parse_number(self._mapped_value(fields, table_config, "cost_price")),
            target_price=self._parse_number(self._mapped_value(fields, table_config, "target_price")),
            manual_category=self._mapped_text(fields, table_config, "manual_category") or self._safe_text(static_fields.get("manual_category")),
            product_notes=self._mapped_text(fields, table_config, "product_notes"),
            competitor_notes=self._mapped_text(fields, table_config, "competitor_notes"),
            competitor_links=self._normalize_links(self._mapped_value(fields, table_config, "competitor_links")),
            target_market=self._mapped_text(fields, table_config, "target_market") or self._safe_text(static_fields.get("target_market")),
            extra_fields=extra_fields,
        )
        return task

    def _mapped_value(self, fields: Dict[str, Any], table_config: TableConfig, key: str) -> Any:
        field_name = table_config.field_map.get(key)
        if not field_name:
            return None
        return fields.get(field_name)

    def _mapped_text(self, fields: Dict[str, Any], table_config: TableConfig, key: str) -> str:
        return self._safe_text(self._mapped_value(fields, table_config, key))

    def _normalize_images(self, value: Any, client=None) -> List[str]:
        items = self._to_list(value)
        results = []
        for item in items:
            normalized = self._normalize_image_item(item, client=client)
            if normalized:
                results.append(normalized)
        return results

    def _normalize_image_item(self, value: Any, client=None) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, dict):
            file_token = self._safe_text(value.get("file_token"))
            if file_token:
                if client and hasattr(client, "get_tmp_download_url"):
                    try:
                        resolved_url = self._safe_text(client.get_tmp_download_url(file_token))
                        if resolved_url:
                            return resolved_url
                    except Exception:
                        pass
                return "feishu-file-token:{token}".format(token=file_token)
            for key in ("tmp_url", "url", "preview_url", "path", "link", "href", "text"):
                candidate = self._safe_text(value.get(key))
                if candidate:
                    return candidate
        return self._safe_text(value)

    def _normalize_links(self, value: Any) -> List[str]:
        items = self._to_list(value)
        links = []
        for item in items:
            if isinstance(item, dict):
                for key in ("url", "href", "link", "text"):
                    candidate = self._safe_text(item.get(key))
                    if candidate:
                        links.append(candidate)
                        break
                continue
            text = self._safe_text(item)
            if text:
                links.append(text)
        return links

    def _to_list(self, value: Any) -> List[Any]:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, tuple):
            return list(value)
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return []
            if "\n" in text:
                return [part.strip() for part in text.splitlines() if part.strip()]
            if "," in text and "http" in text:
                return [part.strip() for part in text.split(",") if part.strip()]
            return [text]
        return [value]

    def _parse_number(self, value: Any) -> Optional[float]:
        if value is None or value == "":
            return None
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        text = self._safe_text(value)
        if not text:
            return None
        cleaned = text.replace(",", "")
        match = re.search(r"-?\d+(?:\.\d+)?", cleaned)
        if not match:
            return None
        try:
            return float(match.group(0))
        except ValueError:
            return None

    def _safe_text(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value).strip()
