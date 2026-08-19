from __future__ import annotations

import json
import html
import re
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import requests

from ..models import (
    AppConfig,
    ErrorCode,
    ExecutionResult,
    PricingMode,
    ProductTask,
)


class FeishuTaskError(RuntimeError):
    pass


@dataclass(frozen=True)
class ClaimedTask:
    record_id: str
    shop_alias: str
    task: ProductTask


def _plain_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float, Decimal)):
        return str(value).strip()
    if isinstance(value, dict):
        for key in ("text", "link", "name", "value"):
            if value.get(key) is not None:
                return _plain_text(value[key])
    if isinstance(value, list):
        return "".join(_plain_text(item) for item in value).strip()
    return str(value).strip()


def extract_1688_offer_id(source_url: str) -> str:
    parsed = urlparse(source_url)
    match = re.search(r"/offer/(\d+)\.html", parsed.path)
    if match:
        return match.group(1)
    query_id = parse_qs(parsed.query).get("offerId", [""])[0]
    if re.fullmatch(r"\d+", query_id):
        return query_id
    raise FeishuTaskError("采购链接不是可识别的 1688 商品链接")


def normalize_1688_source_url(source_text: str, *, timeout: int = 30) -> str:
    """Normalize pasted 1688 links, including QR short links with trailing notes."""
    match = re.search(r"https?://[^\s]+", source_text.strip())
    if not match:
        raise FeishuTaskError("采购链接不是可识别的 1688 商品链接")
    source_url = match.group(0).rstrip("，,。；;")
    parsed = urlparse(source_url)
    if parsed.hostname != "qr.1688.com":
        extract_1688_offer_id(source_url)
        return source_url

    try:
        response = requests.get(source_url, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise FeishuTaskError(f"解析 1688 短链接失败：{exc}") from exc
    target = html.unescape(f"{response.url}\n{response.text}")
    offer_id = ""
    for pattern in (
        r"/offer/(\d+)\.html",
        r"[?&](?:offerId|id)=(\d+)",
        r"[\"']?(?:offerId|offer_id|objectId|itemId)[\"']?\s*[:=]\s*[\"']?(\d+)",
    ):
        offer_match = re.search(pattern, target, flags=re.IGNORECASE)
        if offer_match:
            offer_id = offer_match.group(1)
            break
    if not offer_id:
        raise FeishuTaskError("1688 短链接没有返回可识别的商品 ID")
    return f"https://detail.1688.com/offer/{offer_id}.html"


def attachment_metadata(value: Any) -> Dict[str, str]:
    if isinstance(value, list):
        for item in value:
            metadata = attachment_metadata(item)
            if metadata:
                return metadata
        return {}
    if isinstance(value, str):
        return {"url": value.strip()} if value.strip().startswith("http") else {}
    if not isinstance(value, dict):
        return {}
    url = next(
        (
            str(value.get(key) or "").strip()
            for key in ("tmp_url", "url", "download_url", "link")
            if str(value.get(key) or "").strip().startswith("http")
        ),
        "",
    )
    token = str(value.get("file_token") or value.get("token") or "").strip()
    name = str(value.get("name") or value.get("file_name") or "").strip()
    return {
        key: item
        for key, item in {"url": url, "file_token": token, "name": name}.items()
        if item
    }


def task_from_record(
    record: Dict[str, Any], config: AppConfig
) -> ClaimedTask:
    feishu = config.feishu
    fields_config = feishu.get("fields", {})
    fields = record.get("fields") or {}
    record_id = _plain_text(record.get("record_id"))
    if not record_id:
        raise FeishuTaskError("飞书记录缺少 record_id")

    source_url = _plain_text(fields.get(fields_config["source_url"]))
    alias = _plain_text(fields.get(fields_config["shop_alias"]))
    if not source_url:
        raise FeishuTaskError("采购链接不能为空")
    if not alias:
        raise FeishuTaskError("目标店铺不能为空")

    aliases = {
        str(shop.get("task_alias", "")).strip().casefold(): key
        for key, shop in config.shops.items()
        if str(shop.get("task_alias", "")).strip()
    }
    shop_key = aliases.get(alias.casefold())
    if not shop_key:
        raise FeishuTaskError(f"未配置的目标店铺别名: {alias}")
    market = str(config.shops[shop_key].get("market", "")).upper()
    if not market:
        raise FeishuTaskError(f"店铺 {alias} 未配置国家")

    price_text = _plain_text(fields.get(fields_config["fixed_sale_price"]))
    pricing_mode_field = str(fields_config.get("pricing_mode") or "")
    pricing_mode_text = (
        _plain_text(fields.get(pricing_mode_field)) if pricing_mode_field else ""
    )
    pricing_modes = {
        "": PricingMode.FIXED,
        "固定售价": PricingMode.FIXED,
        "采购价倍数": PricingMode.PURCHASE_MULTIPLIER,
    }
    if pricing_mode_text not in pricing_modes:
        raise FeishuTaskError(
            f"定价方式只能是固定售价或采购价倍数，当前为：{pricing_mode_text}"
        )
    pricing_mode = pricing_modes[pricing_mode_text]
    stock_text = _plain_text(fields.get(fields_config["stock_per_sku"]))
    try:
        price_value = Decimal(price_text)
    except InvalidOperation as exc:
        raise FeishuTaskError("定价必须是大于 0 的数字") from exc
    if price_value <= 0:
        raise FeishuTaskError("定价必须是大于 0 的数字")
    if pricing_mode == PricingMode.PURCHASE_MULTIPLIER and price_value > 100:
        raise FeishuTaskError("采购价倍数必须大于 0 且不超过 100")
    try:
        stock_decimal = Decimal(stock_text)
        stock = int(stock_decimal)
    except (InvalidOperation, ValueError) as exc:
        raise FeishuTaskError("每SKU库存必须是非负整数") from exc
    if stock_decimal != stock or stock < 0:
        raise FeishuTaskError("每SKU库存必须是非负整数")

    source_id = extract_1688_offer_id(source_url)
    size_chart_field = fields_config.get("size_chart", "")
    size_chart = attachment_metadata(fields.get(size_chart_field)) if size_chart_field else {}
    task = ProductTask(
        task_id=record_id,
        miaoshou_product_id=source_id,
        source_url=source_url,
        target_shop=shop_key,
        market=market,
        category_group="AUTO",
        pricing_rule_id=f"{market}_ACCESSORY_V1",
        pricing_mode=pricing_mode,
        fixed_sale_price=(price_value if pricing_mode == PricingMode.FIXED else None),
        purchase_price_multiplier=(
            price_value
            if pricing_mode == PricingMode.PURCHASE_MULTIPLIER
            else None
        ),
        stock_per_sku=stock,
        size_chart_url=size_chart.get("url", ""),
        size_chart_file_token=size_chart.get("file_token", ""),
        size_chart_file_name=size_chart.get("name", ""),
        allow_republish=False,
    )
    return ClaimedTask(record_id=record_id, shop_alias=alias, task=task)


class FeishuTaskTable:
    def __init__(
        self,
        config: AppConfig,
        *,
        credentials_path: Optional[Path] = None,
        request_timeout: int = 30,
        table_url: str = "",
    ) -> None:
        self.config = config
        self.settings = dict(config.feishu)
        if table_url:
            self.settings["url"] = table_url
        self.credentials_path = credentials_path or (
            Path.home() / ".openclaw" / "openclaw.json"
        )
        self.request_timeout = request_timeout
        self._token = ""
        self._token_expires_at = 0.0
        self._app_token = ""
        self._table_id = ""
        self._view_id = ""

    # Public table primitives are intentionally small. They let the selection
    # bridge reuse the same authenticated Feishu client without coupling it to
    # the task-claiming workflow below.
    def list_records(self, *, all_records: bool = False) -> List[Dict[str, Any]]:
        return self._list_records(include_view=not all_records)

    def get_record(self, record_id: str) -> Dict[str, Any]:
        return self._get_record(record_id)

    def update_record(self, record_id: str, fields: Dict[str, Any]) -> None:
        self._update_record(record_id, fields)

    def batch_update_records(self, records: List[Dict[str, Any]]) -> None:
        if not records:
            return
        app_token, table_id, _ = self._resolve_target()
        for offset in range(0, len(records), 500):
            self._request(
                "POST",
                f"/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_update",
                json={"records": records[offset : offset + 500]},
            )

    def create_record(self, fields: Dict[str, Any]) -> str:
        app_token, table_id, _ = self._resolve_target()
        result = self._request(
            "POST",
            f"/bitable/v1/apps/{app_token}/tables/{table_id}/records",
            json={"fields": fields},
        )
        record = result.get("data", {}).get("record", {})
        record_id = _plain_text(record.get("record_id"))
        if not record_id:
            raise FeishuTaskError("创建飞书记录后没有返回 record_id")
        return record_id

    def list_fields(self) -> List[Dict[str, Any]]:
        app_token, table_id, _ = self._resolve_target()
        result = self._request(
            "GET",
            f"/bitable/v1/apps/{app_token}/tables/{table_id}/fields",
            params={"page_size": 500},
        )
        return list(result.get("data", {}).get("items", []))

    def create_field(
        self,
        name: str,
        field_type: int,
        ui_type: str,
        property: Optional[Dict[str, Any]] = None,
    ) -> None:
        app_token, table_id, _ = self._resolve_target()
        payload: Dict[str, Any] = {
            "field_name": name,
            "type": field_type,
            "ui_type": ui_type,
        }
        if property is not None:
            payload["property"] = property
        self._request(
            "POST",
            f"/bitable/v1/apps/{app_token}/tables/{table_id}/fields",
            json=payload,
        )

    def update_field(
        self,
        field_id: str,
        name: str,
        field_type: int,
        ui_type: str,
        property: Optional[Dict[str, Any]] = None,
    ) -> None:
        app_token, table_id, _ = self._resolve_target()
        payload: Dict[str, Any] = {
            "field_name": name,
            "type": field_type,
            "ui_type": ui_type,
        }
        if property is not None:
            payload["property"] = property
        self._request(
            "PUT",
            f"/bitable/v1/apps/{app_token}/tables/{table_id}/fields/{field_id}",
            json=payload,
        )

    def claim_next(self) -> Optional[ClaimedTask]:
        fields_config = self.settings["fields"]
        statuses = self.settings["statuses"]
        for record in self._list_records():
            fields = record.get("fields") or {}
            status = _plain_text(fields.get(fields_config["status"]))
            source_url = _plain_text(fields.get(fields_config["source_url"]))
            if status not in {"", statuses["pending"]} or not source_url:
                continue
            record_id = _plain_text(record.get("record_id"))
            try:
                record = self._normalize_record_source(record)
                claimed = task_from_record(record, self.config)
                self._hydrate_size_chart_attachment(claimed)
            except Exception as exc:
                self._update_record(
                    record_id,
                    {
                        fields_config["status"]: statuses.get(
                            "needs_input", statuses["error"]
                        ),
                        fields_config["result"]: f"任务校验失败：{exc}"[:500],
                    },
                )
                continue
            self._update_record(
                record_id,
                {
                    fields_config["status"]: statuses["running"],
                    fields_config["result"]: "",
                    fields_config["platform_product_id"]: "",
                },
            )
            return claimed
        return None

    def claim_for_execute(self, record_id: str) -> ClaimedTask:
        """Claim one row for the normal single-pass listing workflow."""
        statuses = self.settings["statuses"]
        return self._claim_record(
            record_id,
            allowed={
                "",
                statuses["pending"],
                statuses["error"],
                statuses.get("needs_input", ""),
            },
            action="线性上架",
        )

    def claim_for_verification(self, record_id: str) -> ClaimedTask:
        """Claim a submitted row for VERIFY only; never authorize publishing."""
        statuses = self.settings["statuses"]
        return self._claim_record(
            record_id,
            allowed={statuses.get("pending_verification", "待核验")},
            action="发布结果核验",
        )

    def _claim_record(
        self, record_id: str, *, allowed: set[str], action: str
    ) -> ClaimedTask:
        fields = self.settings["fields"]
        statuses = self.settings["statuses"]
        record = self._get_record(record_id)
        current = _plain_text((record.get("fields") or {}).get(fields["status"]))
        if current not in allowed:
            raise FeishuTaskError(
                f"记录当前状态为{current or '空'}，不能执行{action}"
            )
        record = self._normalize_record_source(record)
        claimed = task_from_record(record, self.config)
        self._hydrate_size_chart_attachment(claimed)
        self._update_record(
            record_id,
            {
                fields["status"]: statuses["running"],
                fields["result"]: f"{action}中",
                fields["platform_product_id"]: "",
            },
        )
        return claimed

    def _normalize_record_source(self, record: Dict[str, Any]) -> Dict[str, Any]:
        fields_config = self.settings["fields"]
        source_field = fields_config["source_url"]
        fields = dict(record.get("fields") or {})
        original = _plain_text(fields.get(source_field))
        normalized = normalize_1688_source_url(
            original, timeout=self.request_timeout
        )
        if normalized == original:
            return record
        record_id = _plain_text(record.get("record_id"))
        if record_id:
            self._update_record(record_id, {source_field: normalized})
        updated = dict(record)
        fields[source_field] = normalized
        updated["fields"] = fields
        return updated

    def _hydrate_size_chart_attachment(self, claimed: ClaimedTask) -> None:
        task = claimed.task
        if task.size_chart_path or not task.size_chart_file_token:
            return
        suffix = Path(task.size_chart_file_name).suffix.lower()
        if suffix not in {".jpg", ".jpeg", ".png", ".webp"}:
            suffix = ".jpg"
        directory = Path(__file__).resolve().parents[3] / "runtime" / "feishu_size_charts"
        directory.mkdir(parents=True, exist_ok=True)
        safe_record = "".join(
            character if character.isalnum() or character in "-_." else "_"
            for character in claimed.record_id
        )
        destination = directory / f"{safe_record}{suffix}"
        try:
            response = requests.get(
                "https://open.feishu.cn/open-apis/drive/v1/medias/"
                f"{task.size_chart_file_token}/download",
                headers={"Authorization": f"Bearer {self._access_token()}"},
                timeout=self.request_timeout,
            )
        except requests.RequestException as exc:
            if task.size_chart_url:
                return
            raise FeishuTaskError(f"下载尺码图附件失败：{exc}") from exc
        if response.status_code >= 400:
            if task.size_chart_url:
                return
            raise FeishuTaskError(
                f"下载尺码图附件失败：HTTP {response.status_code}"
            )
        temporary = destination.with_suffix(destination.suffix + ".tmp")
        temporary.write_bytes(response.content)
        temporary.replace(destination)
        task.size_chart_path = str(destination)

    def inspect(self) -> Dict[str, int]:
        """Return a read-only summary. This never claims or updates a record."""
        fields_config = self.settings["fields"]
        statuses = self.settings["statuses"]
        records = self._list_records()
        pending = 0
        actionable = 0
        pending_verification = 0
        needs_input = 0
        errors = 0
        for record in records:
            fields = record.get("fields") or {}
            status = _plain_text(fields.get(fields_config["status"]))
            if status == statuses.get("pending_verification", "待核验"):
                pending_verification += 1
                continue
            if status == statuses.get("needs_input", "待补资料"):
                needs_input += 1
                continue
            if status == statuses["error"]:
                errors += 1
                continue
            if status not in {
                "",
                statuses["pending"],
            }:
                continue
            pending += 1
            if _plain_text(fields.get(fields_config["source_url"])):
                actionable += 1
        return {
            "records": len(records),
            "pending": pending,
            "actionable": actionable,
            "pending_verification": pending_verification,
            "needs_input": needs_input,
            "errors": errors,
        }

    def verification_pending_record_ids(self, limit: int = 50) -> List[str]:
        statuses = self.settings["statuses"]
        fields = self.settings["fields"]
        expected = statuses.get("pending_verification", "待核验")
        record_ids: List[str] = []
        for record in self._list_records():
            values = record.get("fields") or {}
            if _plain_text(values.get(fields["status"])) != expected:
                continue
            if not _plain_text(values.get(fields["source_url"])):
                continue
            record_id = _plain_text(record.get("record_id"))
            if record_id:
                record_ids.append(record_id)
            if len(record_ids) >= limit:
                break
        return record_ids

    def retry_error(self, record_id: str) -> None:
        fields = self.settings["fields"]
        statuses = self.settings["statuses"]
        record = self._get_record(record_id)
        current = _plain_text((record.get("fields") or {}).get(fields["status"]))
        if current not in {
            statuses["error"],
            statuses.get("needs_input", "待补资料"),
        }:
            raise FeishuTaskError(
                "只能重试状态为异常或待补资料的任务，"
                f"当前状态为{current or '空'}"
            )
        self._update_record(
            record_id,
            {fields["status"]: statuses["pending"], fields["result"]: ""},
        )

    def complete(self, claimed: ClaimedTask, result: ExecutionResult) -> None:
        fields = self.settings["fields"]
        statuses = self.settings["statuses"]
        if (
            result.success and bool(result.platform_product_id)
        ) or result.error_code == ErrorCode.ALREADY_PUBLISHED:
            summary = (
                f"发布成功｜{claimed.shop_alias}｜{claimed.task.market}"
                if result.success
                else f"已发布，安全跳过｜{claimed.shop_alias}｜{claimed.task.market}"
            )
            updates = {
                fields["status"]: statuses["success"],
                fields["result"]: summary,
                fields["platform_product_id"]: result.platform_product_id,
            }
        elif result.published_status == "SUBMITTED_PENDING_VERIFICATION":
            step = result.current_step.value if result.current_step else "VERIFY"
            updates = {
                fields["status"]: statuses.get(
                    "pending_verification", statuses["error"]
                ),
                fields["result"]: (
                    f"[{step}] 发布已提交，等待产品ID｜禁止自动重发｜"
                    f"{result.error_message}"
                )[:500],
                fields["platform_product_id"]: "",
            }
        else:
            step = result.current_step.value if result.current_step else "UNKNOWN"
            code = result.error_code.value if result.error_code else "UNKNOWN_ERROR"
            message = result.error_message
            material_error = result.error_code in {
                ErrorCode.SIZE_CHART_REQUIRED,
                ErrorCode.SIZE_CHART_DETECTION_FAILED,
            }
            updates = {
                fields["status"]: (
                    statuses.get("needs_input", statuses["error"])
                    if material_error
                    else statuses["error"]
                ),
                fields["result"]: f"[{step}] {code}：{message}"[:500],
                fields["platform_product_id"]: "",
            }
        self._update_record(claimed.record_id, updates)

    def _credentials(self) -> tuple[str, str]:
        with self.credentials_path.open("r", encoding="utf-8") as handle:
            config = json.load(handle)
        feishu = config.get("channels", {}).get("feishu", {})
        app_id = feishu.get("appId")
        app_secret = feishu.get("appSecret")
        if not app_id or not app_secret:
            first = next(iter(feishu.get("accounts", {}).values()), {})
            app_id = first.get("appId")
            app_secret = first.get("appSecret")
        if not app_id or not app_secret:
            raise FeishuTaskError("OpenClaw 配置中缺少飞书 appId/appSecret")
        return str(app_id), str(app_secret)

    def _access_token(self) -> str:
        if self._token and time.time() < self._token_expires_at:
            return self._token
        app_id, app_secret = self._credentials()
        response = requests.post(
            "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
            json={"app_id": app_id, "app_secret": app_secret},
            timeout=self.request_timeout,
        )
        result = self._response_json(response, "获取飞书 access_token")
        self._token = str(result.get("tenant_access_token") or "")
        if not self._token:
            raise FeishuTaskError("飞书没有返回 tenant_access_token")
        self._token_expires_at = time.time() + int(result.get("expire", 7200)) - 300
        return self._token

    def _resolve_target(self) -> tuple[str, str, str]:
        if self._app_token:
            return self._app_token, self._table_id, self._view_id
        raw_url = str(self.settings.get("url") or "")
        parsed = urlparse(raw_url)
        params = parse_qs(parsed.query)
        table_id = params.get("table", [""])[0]
        view_id = params.get("view", [""])[0]
        if not table_id:
            raise FeishuTaskError("飞书表格 URL 缺少 table 参数")
        if "/wiki/" in parsed.path:
            wiki_token = parsed.path.split("/wiki/", 1)[1]
            result = self._request(
                "GET",
                "/wiki/v2/spaces/get_node",
                params={"token": wiki_token},
                resolve_target=False,
            )
            node = result.get("data", {}).get("node", {})
            if node.get("obj_type") != "bitable":
                raise FeishuTaskError("飞书 wiki 节点不是多维表格")
            app_token = str(node.get("obj_token") or "")
        elif "/base/" in parsed.path:
            app_token = parsed.path.split("/base/", 1)[1]
        else:
            raise FeishuTaskError("无法解析飞书多维表格 URL")
        if not app_token:
            raise FeishuTaskError("无法取得飞书多维表格 app_token")
        self._app_token, self._table_id, self._view_id = app_token, table_id, view_id
        return self._app_token, self._table_id, self._view_id

    def _request(
        self,
        method: str,
        path: str,
        *,
        resolve_target: bool = True,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        if resolve_target:
            self._resolve_target()
        response = requests.request(
            method,
            f"https://open.feishu.cn/open-apis{path}",
            headers={
                "Authorization": f"Bearer {self._access_token()}",
                "Content-Type": "application/json",
            },
            timeout=self.request_timeout,
            **kwargs,
        )
        return self._response_json(response, f"飞书 API {method} {path}")

    @staticmethod
    def _response_json(response: requests.Response, action: str) -> Dict[str, Any]:
        try:
            result = response.json()
        except ValueError as exc:
            raise FeishuTaskError(f"{action} 返回了非 JSON 响应") from exc
        if response.status_code >= 400 or result.get("code") not in (0, None):
            raise FeishuTaskError(
                f"{action} 失败：HTTP {response.status_code}, "
                f"code={result.get('code')}, msg={result.get('msg')}"
            )
        return result

    def _list_records(self, *, include_view: bool = True) -> List[Dict[str, Any]]:
        app_token, table_id, view_id = self._resolve_target()
        records: List[Dict[str, Any]] = []
        page_token = ""
        while True:
            params: Dict[str, Any] = {"page_size": 500}
            if include_view and view_id:
                params["view_id"] = view_id
            if page_token:
                params["page_token"] = page_token
            result = self._request(
                "GET",
                f"/bitable/v1/apps/{app_token}/tables/{table_id}/records",
                params=params,
            )
            data = result.get("data", {})
            records.extend(data.get("items", []))
            if not data.get("has_more"):
                return records
            page_token = str(data.get("page_token") or "")

    def _get_record(self, record_id: str) -> Dict[str, Any]:
        app_token, table_id, _ = self._resolve_target()
        result = self._request(
            "GET",
            f"/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}",
        )
        return result.get("data", {}).get("record", {})

    def _update_record(self, record_id: str, fields: Dict[str, Any]) -> None:
        if not record_id:
            raise FeishuTaskError("无法更新缺少 record_id 的飞书记录")
        app_token, table_id, _ = self._resolve_target()
        self._request(
            "PUT",
            f"/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}",
            json={"fields": fields},
        )
