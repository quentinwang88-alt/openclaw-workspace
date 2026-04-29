#!/usr/bin/env python3
"""Feishu sync helpers for Market Insight direction cards."""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
SYNC_SKILL_DIR = ROOT.parent / "script-run-manager-sync"
if str(SYNC_SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SYNC_SKILL_DIR))

from core.bitable import FeishuBitableClient  # type: ignore  # noqa: E402


def resolve_app_token(feishu_url: str) -> str:
    from core.feishu_url_parser import parse_feishu_bitable_url  # type: ignore  # noqa: E402

    info = parse_feishu_bitable_url(feishu_url)
    if info and info.app_token:
        return info.app_token
    marker = "/wiki/"
    if marker not in feishu_url:
        raise ValueError(f"无法从 URL 中解析 app_token: {feishu_url}")
    tail = feishu_url.split(marker, 1)[1]
    token = tail.split("?", 1)[0].strip().strip("/")
    if not token:
        raise ValueError(f"无法从 URL 中解析 app_token: {feishu_url}")
    return token


def load_output_config(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def resolve_latest_index(config: Dict[str, Any], artifacts_root: Path) -> Path:
    latest_key = str(config.get("latest_key") or "").strip()
    if not latest_key:
        raise ValueError("output config 缺少 latest_key")
    return artifacts_root / "latest" / f"{latest_key}.json"


def resolve_target_client(config: Dict[str, Any]) -> FeishuBitableClient:
    feishu_url = str((config.get("target") or {}).get("feishu_url") or "").strip()
    if not feishu_url:
        raise ValueError("output config.target.feishu_url 不能为空")
    app_token = resolve_app_token(feishu_url)
    from core.feishu_url_parser import parse_feishu_bitable_url  # type: ignore  # noqa: E402

    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise ValueError(f"无法解析飞书 URL: {feishu_url}")
    return FeishuBitableClient(app_token=app_token, table_id=info.table_id)


def load_cards_from_latest_index(latest_index_path: Path) -> List[Dict[str, Any]]:
    latest_payload = json.loads(latest_index_path.read_text(encoding="utf-8"))
    cards_path = Path(str(latest_payload.get("cards_path") or "").strip())
    if not cards_path.exists():
        raise FileNotFoundError(f"cards_path 不存在: {cards_path}")
    cards = json.loads(cards_path.read_text(encoding="utf-8"))
    if not isinstance(cards, list):
        raise ValueError("market_direction_cards.json 必须是数组")
    return cards


def to_epoch_millis(date_text: str) -> int:
    candidate = datetime.strptime(date_text.strip(), "%Y-%m-%d")
    return int(candidate.timestamp() * 1000)


def build_record_fields(card: Dict[str, Any]) -> Dict[str, Any]:
    representative_products = list(card.get("representative_products") or [])
    representative_ids = [str(item.get("product_id") or "") for item in representative_products if str(item.get("product_id") or "").strip()]
    representative_names = [str(item.get("product_name") or "") for item in representative_products if str(item.get("product_name") or "").strip()]
    top_forms = [str(item or "").strip() for item in list(card.get("top_forms") or []) if str(item or "").strip()]
    form_distribution = dict(card.get("form_distribution") or {})
    form_distribution_by_count = dict(card.get("form_distribution_by_count") or form_distribution)
    form_distribution_by_sales = dict(card.get("form_distribution_by_sales") or {})
    dominant_form = top_forms[0] if top_forms else str(card.get("product_form_or_result") or "")
    brief = dict(card.get("direction_execution_brief") or {})
    return {
        "方向ID": str(card.get("direction_canonical_key") or card.get("direction_instance_id") or ""),
        "方向规范Key": str(card.get("direction_canonical_key") or ""),
        "方向实例ID": str(card.get("direction_instance_id") or ""),
        "批次日期": to_epoch_millis(str(card.get("batch_date") or "")),
        "国家": str(card.get("country") or ""),
        "类目": str(card.get("category") or ""),
        "方向名称": str(card.get("direction_name") or ""),
        "主风格": str(card.get("style_cluster") or card.get("style_main") or ""),
        "方向大类": str(card.get("direction_family") or ""),
        "方向层级": str(card.get("direction_tier") or ""),
        "产品形态/结果": dominant_form,
        "主要承载形态": top_forms,
        "形态分布": "；".join(
            [
                "{name} {value:g}%".format(
                    name=str(name or ""),
                    value=(float(value or 0.0) * 100.0 if float(value or 0.0) <= 1.0 else float(value or 0.0)),
                )
                for name, value in form_distribution.items()
            ]
        ),
        "形态分布_商品数": "；".join(
            [
                "{name} {value:g}%".format(
                    name=str(name or ""),
                    value=(float(value or 0.0) * 100.0 if float(value or 0.0) <= 1.0 else float(value or 0.0)),
                )
                for name, value in form_distribution_by_count.items()
            ]
        ),
        "形态分布_销量": "；".join(
            [
                "{name} {value:g}%".format(
                    name=str(name or ""),
                    value=(float(value or 0.0) * 100.0 if float(value or 0.0) <= 1.0 else float(value or 0.0)),
                )
                for name, value in form_distribution_by_sales.items()
            ]
        ),
        "核心购买动机": list(card.get("top_value_points") or []),
        "核心价值点": list(card.get("top_value_points") or []),
        "核心元素": list(card.get("core_elements") or []),
        "核心场景": list(card.get("scene_tags") or []),
        "目标价格带": list(card.get("target_price_bands") or []),
        "热度等级": str(card.get("heat_level") or ""),
        "拥挤度等级": str(card.get("crowd_level") or ""),
        "优先级": str(card.get("priority_level") or ""),
        "方向商品数": int(card.get("direction_item_count") or card.get("product_count") or 0),
        "方向7日销量中位数": float(card.get("direction_sales_median_7d") or 0.0),
        "平均视频密度": float(card.get("direction_video_density_avg") or 0.0),
        "平均达人密度": float(card.get("direction_creator_density_avg") or 0.0),
        "默认内容路线偏好": str(card.get("default_content_route_preference") or ""),
        "任务类型": str(brief.get("task_type") or ""),
        "目标任务池": str(brief.get("target_pool") or ""),
        "任务Brief来源": str(brief.get("brief_source") or ""),
        "任务Brief置信度": str(brief.get("brief_confidence") or ""),
        "选品任务要求": "；".join([str(item or "").strip() for item in list(brief.get("product_selection_requirements") or []) if str(item or "").strip()]),
        "代表商品ID": "、".join(representative_ids),
        "代表商品名称": " / ".join(representative_names),
        "选品建议": str(card.get("selection_advice") or ""),
        "避坑提示": str(card.get("avoid_notes") or ""),
        "置信度": float(card.get("confidence") or 0.0),
        "商品数": int(card.get("product_count") or 0),
        "平均热度分": float(card.get("average_heat_score") or 0.0),
        "平均拥挤度分": float(card.get("average_crowd_score") or 0.0),
        "方向Key": str(card.get("direction_key") or ""),
        "是否最新批次": True,
    }


def chunked(items: List[Dict[str, Any]], size: int) -> List[List[Dict[str, Any]]]:
    return [items[index:index + size] for index in range(0, len(items), size)]


def _normalize_field_value(value: Any, ui_type: str) -> Any:
    normalized_ui_type = str(ui_type or "").strip().lower()
    if normalized_ui_type in {"text"}:
        if isinstance(value, list):
            return "、".join([str(item or "").strip() for item in value if str(item or "").strip()])
        if isinstance(value, dict):
            return json.dumps(value, ensure_ascii=False, sort_keys=True)
        return value
    if normalized_ui_type in {"multiselect"}:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item or "").strip() for item in value if str(item or "").strip()]
        if isinstance(value, dict):
            return [str(item or "").strip() for item in value.values() if str(item or "").strip()]
        text = str(value or "").strip()
        if not text:
            return []
        normalized = text.replace("；", ",").replace("，", ",").replace("、", ",")
        return [part.strip() for part in normalized.split(",") if part.strip()]
    if normalized_ui_type in {"singleselect"}:
        if isinstance(value, list):
            return str((value[0] if value else "") or "").strip()
        return str(value or "").strip()
    if normalized_ui_type in {"number"}:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    if normalized_ui_type in {"checkbox"}:
        return bool(value)
    return value


def _purge_target_scope_records(
    client: FeishuBitableClient,
    existing_records: List[Any],
    target_country: str,
    target_category: str,
) -> int:
    delete_ids = [
        record.record_id
        for record in existing_records
        if str(record.fields.get("国家") or "").strip() == target_country
        and str(record.fields.get("类目") or "").strip() == target_category
    ]
    for batch in chunked([{"record_id": item} for item in delete_ids], 200):
        client.batch_delete_records([str(item.get("record_id") or "") for item in batch])
    return len(delete_ids)


def sync_cards(
    client: FeishuBitableClient,
    cards: List[Dict[str, Any]],
    purge_target_scope: bool = False,
) -> Dict[str, Any]:
    if not cards:
        return {"created": 0, "updated": 0, "reset_latest_flags": 0, "purged_records": 0, "cards": 0}

    field_names = set()
    field_specs = {}
    if hasattr(client, "list_field_names"):
        try:
            fields = client.list_fields() if hasattr(client, "list_fields") else []
            field_names = {str(item.field_name or "").strip() for item in fields if str(item.field_name or "").strip()}
            field_specs = {
                str(item.field_name or "").strip(): str(item.ui_type or "")
                for item in fields
                if str(item.field_name or "").strip()
            }
        except Exception:
            field_names = set()
            field_specs = {}

    existing_records = client.list_records(page_size=100, limit=None)
    target_country = str(cards[0].get("country") or "")
    target_category = str(cards[0].get("category") or "")
    purged_records = 0
    if purge_target_scope:
        purged_records = _purge_target_scope_records(
            client=client,
            existing_records=existing_records,
            target_country=target_country,
            target_category=target_category,
        )
        existing_records = [
            record
            for record in existing_records
            if not (
                str(record.fields.get("国家") or "").strip() == target_country
                and str(record.fields.get("类目") or "").strip() == target_category
            )
        ]

    existing_map = {}
    reset_updates = []
    for record in existing_records:
        direction_id = str(record.fields.get("方向规范Key") or record.fields.get("方向ID") or "").strip()
        if direction_id:
            existing_map[direction_id] = record.record_id
        if (
            str(record.fields.get("国家") or "").strip() == target_country
            and str(record.fields.get("类目") or "").strip() == target_category
            and bool(record.fields.get("是否最新批次"))
        ):
            reset_updates.append({"record_id": record.record_id, "fields": {"是否最新批次": False}})

    creates = []
    updates = []
    for card in cards:
        record_key = str(card.get("direction_canonical_key") or card.get("direction_instance_id") or "").strip()
        fields = build_record_fields(card)
        if field_names:
            fields = {key: value for key, value in fields.items() if key in field_names}
        if field_specs:
            fields = {
                key: _normalize_field_value(value, field_specs.get(key, ""))
                for key, value in fields.items()
            }
        record_id = existing_map.get(record_key)
        if record_id:
            updates.append({"record_id": record_id, "fields": fields})
        else:
            creates.append({"fields": fields})

    for batch in chunked(reset_updates, 200):
        client.batch_update_records(batch)
    for batch in chunked(creates, 200):
        client.batch_create_records(batch)
    for batch in chunked(updates, 200):
        client.batch_update_records(batch)

    return {
        "created": len(creates),
        "updated": len(updates),
        "reset_latest_flags": len(reset_updates),
        "purged_records": purged_records,
        "cards": len(cards),
    }


def sync_from_output_config(
    output_config_path: Path,
    artifacts_root: Path,
    purge_target_scope: Optional[bool] = None,
) -> Dict[str, Any]:
    config = load_output_config(output_config_path)
    latest_index_path = resolve_latest_index(config, artifacts_root=artifacts_root)
    cards = load_cards_from_latest_index(latest_index_path)
    client = resolve_target_client(config)
    effective_purge_target_scope = bool(config.get("purge_target_scope")) if purge_target_scope is None else bool(purge_target_scope)
    summary = sync_cards(client, cards, purge_target_scope=effective_purge_target_scope)
    summary.update(
        {
            "output_config": str(output_config_path),
            "latest_index_path": str(latest_index_path),
            "target_table_url": str((config.get("target") or {}).get("feishu_url") or ""),
        }
    )
    return summary


class MarketInsightFeishuSyncer(object):
    def __init__(
        self,
        output_config_path: Path,
        artifacts_root: Path,
        sync_every_completions: int = 1,
    ):
        self.output_config_path = Path(output_config_path)
        self.artifacts_root = Path(artifacts_root)
        self.sync_every_completions = max(1, int(sync_every_completions))
        self._last_synced_completed = -1
        self._last_summary: Optional[Dict[str, Any]] = None
        self._output_config = load_output_config(self.output_config_path)
        self._purge_target_scope_on_first_sync = bool(self._output_config.get("purge_target_scope"))
        self._has_purged_target_scope = False

    @property
    def last_summary(self) -> Optional[Dict[str, Any]]:
        return self._last_summary

    def maybe_sync(self, run_result) -> Optional[Dict[str, Any]]:
        completed = int(getattr(run_result, "product_snapshot_count", 0) or 0)
        total = int(getattr(run_result, "total_product_count", 0) or 0)
        run_status = str(getattr(run_result, "run_status", "") or "")
        if completed <= 0:
            return None
        if run_status != "completed" and completed % self.sync_every_completions != 0:
            return None
        if completed == self._last_synced_completed and run_status != "completed":
            return None
        should_purge_target_scope = self._purge_target_scope_on_first_sync and not self._has_purged_target_scope
        summary = sync_from_output_config(
            self.output_config_path,
            artifacts_root=self.artifacts_root,
            purge_target_scope=should_purge_target_scope,
        )
        summary["completed_product_count"] = completed
        summary["total_product_count"] = total
        summary["run_status"] = run_status or "running"
        self._last_synced_completed = completed
        self._last_summary = summary
        if should_purge_target_scope:
            self._has_purged_target_scope = True
        return summary
