#!/usr/bin/env python3
"""Feishu table normalization for Market Insight v1."""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from src.market_insight_input_mode_detector import UNKNOWN_MODE, detect_input_mode
from src.market_insight_models import MarketInsightConfig, ProductRankingSnapshot, ShopRankingSnapshot
from src.models import ConfigError, TableSourceConfig
from src.product_age import calculate_product_age_days, parse_listing_datetime, parse_snapshot_datetime
from src.table_adapter import TableAdapter


class MarketInsightTableAdapter(object):
    def __init__(self, client_factory=None):
        self.shared = TableAdapter(client_factory=client_factory)

    def load_table_configs(self, config_dir: Path, validate_source: bool = True) -> List[MarketInsightConfig]:
        configs = []
        for path in sorted(Path(config_dir).glob("*.json")):
            config = self.load_table_config(path, validate_source=validate_source)
            if config.enabled:
                configs.append(config)
        return configs

    def load_table_config(self, path: Path, validate_source: bool = True) -> MarketInsightConfig:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        source_payload = payload.get("source") or {}
        source = TableSourceConfig(
            feishu_url=self.shared._safe_text(source_payload.get("feishu_url")),
            app_token=self.shared._safe_text(source_payload.get("app_token")),
            bitable_table_id=self.shared._safe_text(source_payload.get("bitable_table_id")),
        )
        if validate_source and not any([source.feishu_url, source.app_token and source.bitable_table_id]):
            raise ConfigError("{path}: 缺少 source.feishu_url 或 source.app_token + source.bitable_table_id".format(path=path))
        return MarketInsightConfig(
            table_id=self.shared._safe_text(payload.get("table_id")) or path.stem,
            table_name=self.shared._safe_text(payload.get("table_name")) or path.stem,
            enabled=bool(payload.get("enabled", True)),
            source=source,
            source_scope=self.shared._safe_text(payload.get("source_scope")) or "official",
            input_mode=self.shared._safe_text(payload.get("input_mode")) or "auto",
            default_country=self.shared._safe_text(payload.get("default_country")),
            default_category=self.shared._safe_text(payload.get("default_category")),
            batch_date=self.shared._safe_text(payload.get("batch_date")),
            max_samples=max(
                0,
                int(payload["max_samples"]) if "max_samples" in payload and payload.get("max_samples") is not None else 50,
            ),
            min_consumable_product_count=max(0, int(payload.get("min_consumable_product_count") or 100)),
            min_consumable_direction_count=max(0, int(payload.get("min_consumable_direction_count") or 5)),
            min_report_valid_sample_ratio=float(
                payload.get("min_report_valid_sample_ratio")
                if payload.get("min_report_valid_sample_ratio") is not None
                else (payload.get("report_output") or {}).get("min_valid_sample_ratio", 0.70)
            ),
            source_currency=self.shared._safe_text(payload.get("source_currency")),
            price_to_cny_rate=float(payload.get("price_to_cny_rate") or 0.0),
            price_band_step_rmb=float(payload.get("price_band_step_rmb") or 0.0),
            price_scale_divisor=float(payload.get("price_scale_divisor") or 1.0),
            price_band_edges=[float(item) for item in (payload.get("price_band_edges") or [0, 50, 100, 200, 500, 1000])],
            field_map=payload.get("field_map") or {},
            output_dir=self.shared._safe_text(payload.get("output_dir")),
            report_output=dict(payload.get("report_output") or {}),
        )

    def get_client(self, config: MarketInsightConfig):
        return self.shared.get_client(
            type("ShimConfig", (), {"source": config.source})  # noqa: B018
        )

    def get_field_names(self, client) -> List[str]:
        if hasattr(client, "list_field_names"):
            return client.list_field_names()
        first_records = client.list_records(limit=1)
        if not first_records:
            return []
        return list(first_records[0].fields.keys())

    def detect_input_mode(self, config: MarketInsightConfig, client) -> str:
        if config.input_mode and config.input_mode != "auto":
            return config.input_mode
        detected = detect_input_mode(self.get_field_names(client))
        if detected == UNKNOWN_MODE:
            if config.default_category:
                return "product_ranking"
            raise ConfigError("无法自动识别输入模式，请在配置中显式设置 input_mode")
        return detected

    def read_product_snapshots(
        self,
        config: MarketInsightConfig,
        client,
        batch_date_override: str = "",
        limit: Optional[int] = None,
    ) -> List[ProductRankingSnapshot]:
        batch_date = self._resolve_batch_date(batch_date_override or config.batch_date)
        snapshot_datetime = parse_snapshot_datetime(batch_date)
        effective_limit = limit if limit is not None else (config.max_samples or None)
        records = client.list_records(limit=effective_limit)
        snapshots: List[ProductRankingSnapshot] = []
        for index, record in enumerate(records, start=1):
            fields = dict(record.fields)
            product_name = self._mapped_text(fields, config, "product_name")
            if not product_name:
                continue
            raw_product_images = self._mapped_value(fields, config, "product_images")
            # Delay Feishu tmp-url resolution until a sample is actually analyzed.
            product_images = self.shared._normalize_images(raw_product_images, client=None)
            product_url = self._first_link_value(self._mapped_value(fields, config, "product_url"))
            price_min, price_max, price_mid = self._parse_price_range(self._mapped_value(fields, config, "price_text"))
            raw_category = self._mapped_text(fields, config, "category")
            snapshots.append(
                ProductRankingSnapshot(
                    batch_date=batch_date,
                    batch_id=self._build_batch_id(config.table_id, batch_date),
                    country=self._normalize_country(self._mapped_text(fields, config, "country") or config.default_country),
                    category=self._normalize_market_category(config.default_category or raw_category or product_name),
                    product_id=self._parse_product_id(product_url, rank_index=index),
                    product_name=product_name,
                    shop_name=self._mapped_text(fields, config, "shop_name"),
                    price_min=price_min,
                    price_max=price_max,
                    price_mid=price_mid,
                    sales_7d=self.shared._parse_number(self._mapped_value(fields, config, "sales_7d")) or 0.0,
                    gmv_7d=self.shared._parse_number(self._mapped_value(fields, config, "gmv_7d")) or 0.0,
                    creator_count=self.shared._parse_number(self._mapped_value(fields, config, "creator_count")) or 0.0,
                    video_count=self.shared._parse_number(self._mapped_value(fields, config, "video_count")) or 0.0,
                    listing_days=self._parse_listing_days(self._mapped_value(fields, config, "listing_time"), snapshot_datetime=snapshot_datetime),
                    product_images=product_images,
                    raw_product_images=raw_product_images,
                    image_url=self._first_usable_image_url(product_images),
                    product_url=product_url,
                    rank_index=index,
                    raw_category=raw_category,
                    source_feishu_url=self.shared._safe_text(config.source.feishu_url),
                    source_app_token=self.shared._safe_text(config.source.app_token),
                    source_table_id=self.shared._safe_text(config.source.bitable_table_id),
                    raw_fields=fields,
                )
            )
        return snapshots[:effective_limit] if effective_limit is not None else snapshots

    def read_shop_snapshots(
        self,
        config: MarketInsightConfig,
        client,
        batch_date_override: str = "",
        limit: Optional[int] = None,
    ) -> List[ShopRankingSnapshot]:
        batch_date = self._resolve_batch_date(batch_date_override or config.batch_date)
        effective_limit = limit if limit is not None else (config.max_samples or None)
        records = client.list_records(limit=effective_limit)
        snapshots: List[ShopRankingSnapshot] = []
        for index, record in enumerate(records, start=1):
            fields = dict(record.fields)
            shop_name = self._mapped_text(fields, config, "shop_name")
            if not shop_name:
                continue
            snapshots.append(
                ShopRankingSnapshot(
                    batch_date=batch_date,
                    batch_id=self._build_batch_id(config.table_id, batch_date),
                    country=self._normalize_country(self._mapped_text(fields, config, "country") or config.default_country),
                    category=self._normalize_market_category(config.default_category or self._mapped_text(fields, config, "category")),
                    shop_name=shop_name,
                    shop_positioning=self._mapped_text(fields, config, "shop_positioning"),
                    sales_7d=self.shared._parse_number(self._mapped_value(fields, config, "sales_7d")) or 0.0,
                    gmv_7d=self.shared._parse_number(self._mapped_value(fields, config, "gmv_7d")) or 0.0,
                    active_product_count=self.shared._parse_number(self._mapped_value(fields, config, "active_product_count")),
                    new_product_share=self._parse_percent(self._mapped_value(fields, config, "new_product_share")),
                    listed_product_count=self.shared._parse_number(self._mapped_value(fields, config, "listed_product_count")),
                    creator_count=self.shared._parse_number(self._mapped_value(fields, config, "creator_count")) or 0.0,
                    rank_index=index,
                    raw_fields=fields,
                )
            )
        return snapshots[:effective_limit] if effective_limit is not None else snapshots

    def _mapped_value(self, fields: Dict[str, Any], config: MarketInsightConfig, key: str) -> Any:
        field_name = config.field_map.get(key)
        if isinstance(field_name, list):
            for candidate in field_name:
                value = fields.get(self.shared._safe_text(candidate))
                if value not in (None, "", []):
                    return value
            return None
        field_name = self.shared._safe_text(field_name)
        if not field_name:
            return None
        return fields.get(field_name)

    def _mapped_text(self, fields: Dict[str, Any], config: MarketInsightConfig, key: str) -> str:
        return self.shared._safe_text(self._mapped_value(fields, config, key))

    def _first_link_value(self, value: Any) -> str:
        links = self.shared._normalize_links(value)
        return links[0] if links else self.shared._safe_text(value)

    def _parse_price_range(self, value: Any) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        text = self.shared._safe_text(value)
        if not text:
            return None, None, None
        numbers = [float(item.replace(",", "")) for item in re.findall(r"\d[\d,]*(?:\.\d+)?", text)]
        if not numbers:
            return None, None, None
        if len(numbers) == 1:
            return numbers[0], numbers[0], numbers[0]
        price_min = min(numbers[:2])
        price_max = max(numbers[:2])
        return price_min, price_max, round((price_min + price_max) / 2.0, 2)

    def _parse_percent(self, value: Any) -> Optional[float]:
        text = self.shared._safe_text(value)
        if not text:
            return None
        if "%" in text:
            parsed = self.shared._parse_number(text)
            return parsed / 100.0 if parsed is not None else None
        return self.shared._parse_number(text)

    def _parse_listing_days(self, value: Any, snapshot_datetime: Optional[datetime] = None) -> Optional[int]:
        if value is None or value == "":
            return None
        snapshot_datetime = snapshot_datetime or datetime.now()
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            raw_value = float(value)
            if raw_value > 10_000_000_000:
                candidate = datetime.fromtimestamp(raw_value / 1000.0)
                return calculate_product_age_days(candidate, snapshot_datetime)[0]
            if raw_value > 10_000:
                candidate = datetime.fromtimestamp(raw_value)
                return calculate_product_age_days(candidate, snapshot_datetime)[0]
            return int(raw_value)
        text = self.shared._safe_text(value)
        if not text:
            return None
        if re.fullmatch(r"\d+", text):
            return int(text)
        candidate, status = parse_listing_datetime(text)
        if candidate is not None:
            return calculate_product_age_days(candidate, snapshot_datetime)[0]
        return None

    def _parse_product_id(self, product_url: str, rank_index: int) -> str:
        url = self.shared._safe_text(product_url)
        if not url:
            return "row_{index}".format(index=rank_index)
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        for key in ("product_id", "id", "item_id"):
            value = params.get(key, [""])[0].strip()
            if value:
                return value
        path_match = re.search(r"/(\d{6,})", parsed.path)
        if path_match:
            return path_match.group(1)
        slug = parsed.path.rstrip("/").split("/")[-1].strip()
        if slug:
            return slug
        return "row_{index}".format(index=rank_index)

    def _first_usable_image_url(self, product_images: List[str]) -> str:
        for image in product_images:
            text = self.shared._safe_text(image)
            if text and not self._needs_lazy_image_resolution(text):
                return text
        return ""

    def _needs_lazy_image_resolution(self, image_ref: str) -> bool:
        text = self.shared._safe_text(image_ref)
        return text.startswith("feishu-file-token:") or "/open-apis/drive/v1/medias/batch_get_tmp_download_url" in text

    def _build_batch_id(self, table_id: str, batch_date: str) -> str:
        return "{table_id}_{batch_date}".format(table_id=table_id, batch_date=batch_date.replace("-", ""))

    def _resolve_batch_date(self, explicit_batch_date: str) -> str:
        text = self.shared._safe_text(explicit_batch_date)
        if text:
            return text
        return datetime.now().strftime("%Y-%m-%d")

    def _normalize_country(self, value: str) -> str:
        text = self.shared._safe_text(value).upper()
        if text in {"越南", "VN"}:
            return "VN"
        if text in {"TH", "泰国"}:
            return "TH"
        return text or "UNKNOWN"

    def _normalize_market_category(self, value: str) -> str:
        text = self.shared._safe_text(value)
        lowered = text.lower()
        if lowered in {"hair_accessory", "hair", "发饰"}:
            return "hair_accessory"
        if lowered in {"light_tops", "tops", "轻上装"}:
            return "light_tops"
        if any(keyword in text for keyword in ("发", "夹", "配件", "时尚配件", "饰品")):
            return "hair_accessory"
        if any(keyword in text for keyword in ("上装", "开衫", "罩衫", "衬衫", "针织")):
            return "light_tops"
        return lowered or "unknown"
