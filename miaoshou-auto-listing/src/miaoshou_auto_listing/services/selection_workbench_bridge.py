from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Mapping, Protocol

from ..models import AppConfig
from .feishu_task_table import (
    FeishuTaskError,
    _plain_text,
    attachment_metadata,
    extract_1688_offer_id,
)


class BitableTable(Protocol):
    def list_records(self, *, all_records: bool = False) -> List[Dict[str, Any]]: ...

    def create_record(self, fields: Dict[str, Any]) -> str: ...

    def update_record(self, record_id: str, fields: Dict[str, Any]) -> None: ...

    def batch_update_records(self, records: List[Dict[str, Any]]) -> None: ...


WORKBENCH_FIELDS = {
    "test": "是否测品",
    "lookup_status": "找货状态",
    "source_url": "推荐货源链接",
    "procurement_price": "采购价人民币",
    "market": "市场",
    "shop_alias": "目标店铺",
    "pricing_mode": "定价方式",
    "price": "定价",
    "stock": "每SKU库存",
    "size_chart": "尺码图",
    "confirm": "确认上架",
    "status": "上架状态",
    "task_id": "上架任务ID",
    "result": "上架结果",
    "platform_product_id": "TikTok产品ID",
    "unique_key": "上架唯一键",
    "fingerprint": "上架参数指纹",
    "snapshot_id": "商品快照ID",
}

QUEUE_TRACKING_FIELDS = {
    "source": "任务来源",
    "selection_record_id": "选品记录ID",
    "snapshot_id": "商品快照ID",
    "unique_key": "上架唯一键",
    "fingerprint": "上架参数指纹",
}


@dataclass
class BridgeReport:
    scanned: int = 0
    ready: int = 0
    created: List[str] = field(default_factory=list)
    updated: List[str] = field(default_factory=list)
    reconciled: List[str] = field(default_factory=list)
    blocked: Dict[str, str] = field(default_factory=dict)
    execute_record_ids: List[str] = field(default_factory=list)
    workbench_updates: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def checkbox_checked(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, list):
        return any(checkbox_checked(item) for item in value)
    if isinstance(value, dict):
        return any(
            checkbox_checked(value.get(key))
            for key in ("checked", "value", "text", "name")
            if key in value
        )
    return str(value or "").strip().lower() in {
        "true",
        "1",
        "yes",
        "y",
        "是",
        "勾选",
        "checked",
    }


def canonical_source_url(value: Any) -> tuple[str, str]:
    if isinstance(value, list):
        source_url = next(
            (canonical_source_url(item)[0] for item in value if item), ""
        )
    elif isinstance(value, dict):
        source_url = _plain_text(
            value.get("link") or value.get("url") or value.get("text")
        )
    else:
        source_url = _plain_text(value)
    offer_id = extract_1688_offer_id(source_url)
    return f"https://detail.1688.com/offer/{offer_id}.html", offer_id


def decimal_text(value: Any, label: str) -> str:
    raw = _plain_text(value)
    try:
        parsed = Decimal(raw)
    except InvalidOperation as exc:
        raise FeishuTaskError(f"{label}必须是大于 0 的数字") from exc
    if parsed <= 0:
        raise FeishuTaskError(f"{label}必须是大于 0 的数字")
    return format(parsed.normalize(), "f")


def nonnegative_integer(value: Any) -> int:
    raw = _plain_text(value)
    try:
        parsed = Decimal(raw)
        integer = int(parsed)
    except (InvalidOperation, ValueError) as exc:
        raise FeishuTaskError("每SKU库存必须是非负整数") from exc
    if parsed != integer or integer < 0:
        raise FeishuTaskError("每SKU库存必须是非负整数")
    return integer


def listing_unique_key(workbench_record_id: str, offer_id: str, alias: str) -> str:
    raw = "|".join((workbench_record_id, offer_id, alias.casefold()))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def parameter_fingerprint(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class SelectionListingBridge:
    """Idempotently convert confirmed workbench rows into listing queue rows."""

    def __init__(
        self,
        config: AppConfig,
        workbench: BitableTable,
        queue: BitableTable,
    ) -> None:
        self.config = config
        self.workbench = workbench
        self.queue = queue
        self.queue_fields = config.feishu["fields"]
        self.queue_statuses = config.feishu["statuses"]
        self.alias_market = {
            str(shop.get("task_alias") or "").strip().casefold(): str(
                shop.get("market") or ""
            ).strip().upper()
            for shop in config.shops.values()
            if str(shop.get("task_alias") or "").strip()
        }
        self._pending_workbench_updates: List[Dict[str, Any]] = []

    def sync(self, *, dry_run: bool = True) -> BridgeReport:
        report = BridgeReport()
        workbench_records = self.workbench.list_records(all_records=True)
        queue_records = self.queue.list_records(all_records=True)
        queue_by_id = {
            _plain_text(record.get("record_id")): record for record in queue_records
        }
        queue_by_key = {
            _plain_text((record.get("fields") or {}).get(QUEUE_TRACKING_FIELDS["unique_key"])): record
            for record in queue_records
            if _plain_text(
                (record.get("fields") or {}).get(QUEUE_TRACKING_FIELDS["unique_key"])
            )
        }

        for record in workbench_records:
            record_id = _plain_text(record.get("record_id"))
            fields = record.get("fields") or {}
            report.scanned += 1

            linked_task_id = _plain_text(fields.get(WORKBENCH_FIELDS["task_id"]))
            linked = queue_by_id.get(linked_task_id) if linked_task_id else None
            if linked is None:
                linked_key = _plain_text(fields.get(WORKBENCH_FIELDS["unique_key"]))
                linked = queue_by_key.get(linked_key) if linked_key else None
            if linked is not None:
                linked_id = _plain_text(linked.get("record_id"))
                linked_fields = linked.get("fields") or {}
                linked_status = _plain_text(
                    linked_fields.get(self.queue_fields["status"])
                )
                if (
                    checkbox_checked(fields.get(WORKBENCH_FIELDS["confirm"]))
                    and linked_status in {"", self.queue_statuses["pending"]}
                ):
                    report.ready += 1
                    try:
                        queue_fields, unique_key, fingerprint = self._queue_fields(
                            record_id, fields
                        )
                        linked_key = _plain_text(
                            linked_fields.get(QUEUE_TRACKING_FIELDS["unique_key"])
                        )
                        if linked_key and linked_key != unique_key:
                            raise FeishuTaskError(
                                "已建任务后采购链接或目标店铺发生变化，请先人工处理原待执行任务"
                            )
                    except Exception as exc:
                        message = str(exc)
                        report.blocked[record_id] = message
                        self._stage_workbench_update(
                            record_id,
                            fields,
                            {
                                WORKBENCH_FIELDS["status"]: "异常",
                                WORKBENCH_FIELDS["result"]: f"上架参数校验失败：{message}"[:500],
                            },
                            dry_run,
                        )
                        continue
                    old_fingerprint = _plain_text(
                        linked_fields.get(QUEUE_TRACKING_FIELDS["fingerprint"])
                    )
                    if old_fingerprint != fingerprint:
                        if not dry_run:
                            self.queue.update_record(linked_id, queue_fields)
                        report.updated.append(linked_id)
                    self._link_workbench(
                        record_id,
                        fields,
                        linked_id,
                        unique_key,
                        fingerprint,
                        linked_status,
                        dry_run,
                    )
                    report.execute_record_ids.append(linked_id)
                else:
                    updates = self._reconciliation_updates(linked)
                    updates[WORKBENCH_FIELDS["task_id"]] = linked_id
                    if checkbox_checked(fields.get(WORKBENCH_FIELDS["confirm"])):
                        updates[WORKBENCH_FIELDS["confirm"]] = False
                    self._stage_workbench_update(record_id, fields, updates, dry_run)
                    report.reconciled.append(record_id)
                continue

            if not checkbox_checked(fields.get(WORKBENCH_FIELDS["test"])):
                continue
            confirmed = checkbox_checked(fields.get(WORKBENCH_FIELDS["confirm"]))
            if not confirmed:
                lookup_ready = (
                    _plain_text(fields.get(WORKBENCH_FIELDS["lookup_status"]))
                    == "已找到"
                )
                self._stage_workbench_update(
                    record_id,
                    fields,
                    {
                        WORKBENCH_FIELDS["status"]: (
                            "待确认上架" if lookup_ready else "待找货"
                        ),
                        WORKBENCH_FIELDS["result"]: (
                            "货源已就绪，请确认价格和店铺后勾选确认上架"
                            if lookup_ready
                            else "已选择测品，等待找货完成"
                        ),
                    },
                    dry_run,
                )
                continue

            report.ready += 1
            try:
                queue_fields, unique_key, fingerprint = self._queue_fields(
                    record_id, fields
                )
            except Exception as exc:
                message = str(exc)
                report.blocked[record_id] = message
                self._stage_workbench_update(
                    record_id,
                    fields,
                    {
                        WORKBENCH_FIELDS["status"]: "异常",
                        WORKBENCH_FIELDS["result"]: f"上架参数校验失败：{message}"[:500],
                    },
                    dry_run,
                )
                continue

            existing = queue_by_key.get(unique_key)
            if existing is not None:
                queue_id = _plain_text(existing.get("record_id"))
                existing_fields = existing.get("fields") or {}
                status = _plain_text(
                    existing_fields.get(self.queue_fields["status"])
                )
                old_fingerprint = _plain_text(
                    existing_fields.get(QUEUE_TRACKING_FIELDS["fingerprint"])
                )
                if status in {"", self.queue_statuses["pending"]} and old_fingerprint != fingerprint:
                    if not dry_run:
                        self.queue.update_record(queue_id, queue_fields)
                    report.updated.append(queue_id)
                self._link_workbench(
                    record_id,
                    fields,
                    queue_id,
                    unique_key,
                    fingerprint,
                    status,
                    dry_run,
                )
                if status in {"", self.queue_statuses["pending"]}:
                    report.execute_record_ids.append(queue_id)
                continue

            if dry_run:
                queue_id = f"dry-run:{unique_key}"
            else:
                queue_id = self.queue.create_record(queue_fields)
                queue_by_key[unique_key] = {
                    "record_id": queue_id,
                    "fields": queue_fields,
                }
            report.created.append(queue_id)
            report.execute_record_ids.append(queue_id)
            self._link_workbench(
                record_id,
                fields,
                queue_id,
                unique_key,
                fingerprint,
                self.queue_statuses["pending"],
                dry_run,
            )
        report.workbench_updates = len(self._pending_workbench_updates)
        if not dry_run:
            self._flush_workbench_updates()
        return report

    def _queue_fields(
        self, record_id: str, fields: Dict[str, Any]
    ) -> tuple[Dict[str, Any], str, str]:
        source_url, offer_id = canonical_source_url(
            fields.get(WORKBENCH_FIELDS["source_url"])
        )
        procurement_price = decimal_text(
            fields.get(WORKBENCH_FIELDS["procurement_price"]), "采购价"
        )
        alias = _plain_text(fields.get(WORKBENCH_FIELDS["shop_alias"]))
        configured_market = self.alias_market.get(alias.casefold())
        if not configured_market:
            raise FeishuTaskError(f"未配置的目标店铺别名：{alias or '空'}")
        market = _plain_text(fields.get(WORKBENCH_FIELDS["market"])).upper()
        if market != configured_market:
            raise FeishuTaskError(
                f"工作台市场 {market or '空'} 与店铺市场 {configured_market} 不一致"
            )
        pricing_mode = _plain_text(fields.get(WORKBENCH_FIELDS["pricing_mode"]))
        if pricing_mode not in {"", "固定售价", "采购价倍数"}:
            raise FeishuTaskError("定价方式只能是固定售价或采购价倍数")
        pricing_mode = pricing_mode or "固定售价"
        price = decimal_text(fields.get(WORKBENCH_FIELDS["price"]), "定价")
        if pricing_mode == "采购价倍数" and Decimal(price) > 100:
            raise FeishuTaskError("采购价倍数必须大于 0 且不超过 100")
        stock = nonnegative_integer(fields.get(WORKBENCH_FIELDS["stock"]))
        size_chart = fields.get(WORKBENCH_FIELDS["size_chart"])
        size_chart_meta = attachment_metadata(size_chart)
        unique_key = listing_unique_key(record_id, offer_id, alias)
        fingerprint_payload = {
            "source_url": source_url,
            "procurement_price": procurement_price,
            "shop_alias": alias,
            "market": market,
            "pricing_mode": pricing_mode,
            "price": price,
            "stock": stock,
            "size_chart": size_chart_meta,
        }
        fingerprint = parameter_fingerprint(fingerprint_payload)
        queue_fields: Dict[str, Any] = {
            self.queue_fields["source_url"]: source_url,
            self.queue_fields["shop_alias"]: alias,
            self.queue_fields["pricing_mode"]: pricing_mode,
            self.queue_fields["fixed_sale_price"]: float(Decimal(price)),
            self.queue_fields["stock_per_sku"]: stock,
            self.queue_fields["status"]: self.queue_statuses["pending"],
            self.queue_fields["result"]: "",
            self.queue_fields["platform_product_id"]: "",
            QUEUE_TRACKING_FIELDS["source"]: "选品工作台",
            QUEUE_TRACKING_FIELDS["selection_record_id"]: record_id,
            QUEUE_TRACKING_FIELDS["snapshot_id"]: _plain_text(
                fields.get(WORKBENCH_FIELDS["snapshot_id"])
            ),
            QUEUE_TRACKING_FIELDS["unique_key"]: unique_key,
            QUEUE_TRACKING_FIELDS["fingerprint"]: fingerprint,
        }
        if size_chart:
            queue_fields[self.queue_fields["size_chart"]] = size_chart
        return queue_fields, unique_key, fingerprint

    def _link_workbench(
        self,
        record_id: str,
        current_fields: Dict[str, Any],
        queue_id: str,
        unique_key: str,
        fingerprint: str,
        queue_status: str,
        dry_run: bool,
    ) -> None:
        status = self._workbench_status(queue_status)
        updates = {
            WORKBENCH_FIELDS["task_id"]: queue_id,
            WORKBENCH_FIELDS["unique_key"]: unique_key,
            WORKBENCH_FIELDS["fingerprint"]: fingerprint,
            WORKBENCH_FIELDS["status"]: status,
            WORKBENCH_FIELDS["result"]: "已生成统一上架任务",
            WORKBENCH_FIELDS["confirm"]: False,
        }
        self._stage_workbench_update(record_id, current_fields, updates, dry_run)

    def _stage_workbench_update(
        self,
        record_id: str,
        current_fields: Dict[str, Any],
        updates: Dict[str, Any],
        dry_run: bool,
    ) -> None:
        changed = {
            key: value
            for key, value in updates.items()
            if current_fields.get(key) != value
        }
        if changed:
            self._pending_workbench_updates.append(
                {"record_id": record_id, "fields": changed}
            )

    def _flush_workbench_updates(self) -> None:
        if not self._pending_workbench_updates:
            return
        batch_method = getattr(self.workbench, "batch_update_records", None)
        if callable(batch_method):
            batch_method(self._pending_workbench_updates)
            return
        for item in self._pending_workbench_updates:
            self.workbench.update_record(item["record_id"], item["fields"])

    def _reconciliation_updates(self, queue_record: Dict[str, Any]) -> Dict[str, Any]:
        fields = queue_record.get("fields") or {}
        queue_status = _plain_text(fields.get(self.queue_fields["status"]))
        return {
            WORKBENCH_FIELDS["status"]: self._workbench_status(queue_status),
            WORKBENCH_FIELDS["result"]: _plain_text(
                fields.get(self.queue_fields["result"])
            ),
            WORKBENCH_FIELDS["platform_product_id"]: _plain_text(
                fields.get(self.queue_fields["platform_product_id"])
            ),
            WORKBENCH_FIELDS["unique_key"]: _plain_text(
                fields.get(QUEUE_TRACKING_FIELDS["unique_key"])
            ),
            WORKBENCH_FIELDS["fingerprint"]: _plain_text(
                fields.get(QUEUE_TRACKING_FIELDS["fingerprint"])
            ),
        }

    def _workbench_status(self, queue_status: str) -> str:
        mapping = {
            "": "待执行",
            self.queue_statuses["pending"]: "待执行",
            self.queue_statuses["running"]: "执行中",
            self.queue_statuses["success"]: "成功",
            self.queue_statuses["error"]: "异常",
        }
        return mapping.get(queue_status, "异常")


def _options(*names: str) -> Dict[str, Any]:
    return {"options": [{"name": name} for name in names]}


WORKBENCH_FIELD_SPECS = [
    (
        "目标店铺",
        3,
        "SingleSelect",
        _options(
            "LikeU shop",
            "yours.beauty-1",
            "yours.beauty-2",
            "lunara-1",
            "lunara-2",
            "TOWU123",
        ),
    ),
    ("定价方式", 3, "SingleSelect", _options("固定售价", "采购价倍数")),
    ("定价", 2, "Number", None),
    ("每SKU库存", 2, "Number", None),
    ("尺码图", 17, "Attachment", None),
    ("确认上架", 7, "Checkbox", None),
    (
        "上架状态",
        3,
        "SingleSelect",
        _options("待找货", "待确认上架", "待执行", "执行中", "成功", "异常"),
    ),
    ("上架任务ID", 1, "Text", None),
    ("上架结果", 1, "Text", None),
    ("TikTok产品ID", 1, "Text", None),
    ("上架唯一键", 1, "Text", None),
    ("上架参数指纹", 1, "Text", None),
]

QUEUE_FIELD_SPECS = [
    ("任务来源", 3, "SingleSelect", _options("人工", "选品工作台")),
    ("选品记录ID", 1, "Text", None),
    ("商品快照ID", 1, "Text", None),
    ("上架唯一键", 1, "Text", None),
    ("上架参数指纹", 1, "Text", None),
]


def ensure_bridge_fields(table: Any, specs: List[tuple[Any, ...]]) -> List[str]:
    existing = {
        _plain_text(item.get("field_name")): item for item in table.list_fields()
    }
    changed: List[str] = []
    for name, field_type, ui_type, property in specs:
        current = existing.get(name)
        if current is not None:
            if property and _missing_option_names(current.get("property"), property):
                merged_property = _merge_option_properties(
                    current.get("property"), property
                )
                table.update_field(
                    _plain_text(current.get("field_id")),
                    name,
                    field_type,
                    ui_type,
                    merged_property,
                )
                changed.append(f"更新选项:{name}")
            continue
        table.create_field(name, field_type, ui_type, property)
        existing[name] = {"field_name": name}
        changed.append(f"创建:{name}")
    return changed


def _missing_option_names(current: Any, desired: Dict[str, Any]) -> bool:
    current_names = {
        _plain_text(item.get("name"))
        for item in (current or {}).get("options", [])
        if isinstance(item, dict)
    }
    desired_names = {
        _plain_text(item.get("name"))
        for item in desired.get("options", [])
        if isinstance(item, dict)
    }
    return not desired_names.issubset(current_names)


def _merge_option_properties(current: Any, desired: Dict[str, Any]) -> Dict[str, Any]:
    result = dict(current or {})
    options = [
        dict(item)
        for item in result.get("options", [])
        if isinstance(item, dict) and _plain_text(item.get("name"))
    ]
    names = {_plain_text(item.get("name")) for item in options}
    for item in desired.get("options", []):
        name = _plain_text(item.get("name")) if isinstance(item, dict) else ""
        if name and name not in names:
            options.append({"name": name})
            names.add(name)
    result["options"] = options
    return result
