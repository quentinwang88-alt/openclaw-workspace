#!/usr/bin/env python3
"""Feishu sync helpers for market insight direction sample pools."""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests

ROOT = Path(__file__).resolve().parents[1]
SYNC_SKILL_DIR = ROOT.parent / "script-run-manager-sync"
if str(SYNC_SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SYNC_SKILL_DIR))

from core.bitable import (  # type: ignore  # noqa: E402
    FeishuAPIError,
    FeishuBitableClient,
    get_tenant_access_token,
    resolve_wiki_bitable_app_token,
)
from core.feishu_url_parser import parse_feishu_bitable_url  # type: ignore  # noqa: E402

from src.market_insight_feishu_sync import _normalize_field_value, chunked  # noqa: E402
from src.market_insight_sample_pool import MANUAL_FIELD_NAMES  # noqa: E402


TEXT = {"type": 1, "ui_type": "Text"}
NUMBER = {"type": 2, "ui_type": "Number"}
SINGLE_SELECT = {"type": 3, "ui_type": "SingleSelect"}
MULTI_SELECT = {"type": 4, "ui_type": "MultiSelect"}
CHECKBOX = {"type": 7, "ui_type": "Checkbox"}
ATTACHMENT = {"type": 17, "ui_type": "Attachment"}

DEFAULT_TABLE_NAME = "方向样本商品池"

SAMPLE_TYPE_OPTIONS = ["头部Top10", "代表新品", "少数新品赢家", "老品占位头部", "差异化候选", "待人工确认", "样本不足"]
COUNTRY_OPTIONS = ["VN", "TH", "MY", "PH", "ID", "SG", "US", "other"]
CATEGORY_OPTIONS = ["hair_accessory", "light_tops", "other"]
ACTION_OPTIONS = ["优先低成本验证", "谨慎切入验证", "暗线小样本验证", "拆头部不直接入场", "持续观察", "暂不投入"]
OPPORTUNITY_OPTIONS = ["样本不足", "内容缺口型", "成熟强需求型", "头部集中型", "少数新品赢家", "暗线场景型", "暗线场景候选", "供给泡沫型", "普通观察型"]
MANUAL_STATUS_OPTIONS = ["待查看", "已查看", "值得找同款", "值得拆内容", "可小样本测试", "暂不跟进", "信息不足"]
PRIORITY_OPTIONS = ["P0", "P1", "P2", "P3"]


def single_select(options: List[str]) -> Dict[str, Any]:
    return {"type": SINGLE_SELECT["type"], "ui_type": SINGLE_SELECT["ui_type"], "property": {"options": _options(options)}}


def multi_select(options: List[str]) -> Dict[str, Any]:
    return {"type": MULTI_SELECT["type"], "ui_type": MULTI_SELECT["ui_type"], "property": {"options": _options(options)}}


def _options(options: List[str]) -> List[Dict[str, Any]]:
    results = []
    seen = set()
    for index, item in enumerate(options):
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        results.append({"name": text, "color": index % 54})
    return results


def build_sample_pool_field_specs() -> List[Dict[str, Any]]:
    text_fields = [
        "批次日期",
        "方向名称",
        "样本排名",
        "商品标题",
        "FastMoss链接",
        "商品形态",
        "核心使用场景",
        "差异化机会",
        "人工备注",
    ]
    number_fields = [
        "价格",
        "7日销量",
        "上架天数",
    ]
    multi_fields = {
        "样本类型": SAMPLE_TYPE_OPTIONS,
        "核心价值点": [],
    }
    specs: List[Dict[str, Any]] = [
        {"name": "方向动作", **single_select(ACTION_OPTIONS)},
        {"name": "商品主图", **ATTACHMENT},
    ]
    specs.extend({"name": name, **TEXT} for name in text_fields)
    specs.extend({"name": name, **NUMBER} for name in number_fields)
    specs.extend({"name": name, **multi_select(options)} for name, options in multi_fields.items())
    specs.extend(
        [
            {"name": "是否加入选品池", **CHECKBOX},
            {"name": "人工判断状态", **single_select(MANUAL_STATUS_OPTIONS)},
        ]
    )
    return _dedupe_specs(specs)


def _dedupe_specs(specs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    results = []
    seen = set()
    for spec in specs:
        name = str(spec.get("name") or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        results.append(spec)
    return results


def resolve_app_token_and_original_url(feishu_url: str) -> tuple[str, str]:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise ValueError(f"无法解析飞书 URL: {feishu_url}")
    app_token = info.app_token
    if "/wiki/" in info.original_url:
        try:
            app_token = resolve_wiki_bitable_app_token(info.app_token)
        except FeishuAPIError:
            app_token = _resolve_embedded_bitable_app_token(info.app_token, info.table_id) or info.app_token
    return app_token, info.original_url


def _resolve_embedded_bitable_app_token(wiki_token: str, table_id: str) -> str:
    headers = {"Authorization": f"Bearer {get_tenant_access_token()}"}
    node_resp = requests.get(
        "https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node",
        headers=headers,
        params={"token": wiki_token},
        timeout=30,
    )
    node_payload = node_resp.json()
    node = node_payload.get("data", {}).get("node", {})
    spreadsheet_token = str(node.get("obj_token") or "").strip()
    if not spreadsheet_token:
        return ""
    metainfo_resp = requests.get(
        f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/metainfo",
        headers=headers,
        timeout=30,
    )
    metainfo_payload = metainfo_resp.json()
    for sheet in metainfo_payload.get("data", {}).get("sheets", []):
        block_info = sheet.get("blockInfo") or {}
        block_token = str(block_info.get("blockToken") or "").strip()
        if block_token.endswith(f"_{table_id}"):
            return block_token.rsplit("_", 1)[0]
    return ""


def _headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {get_tenant_access_token()}", "Content-Type": "application/json"}


def _request(method: str, url: str, **kwargs) -> requests.Response:
    last_error: Optional[Exception] = None
    for attempt in range(4):
        try:
            response = requests.request(method, url, timeout=30, **kwargs)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_error = exc
            if attempt < 3:
                time.sleep(2 ** attempt)
    raise FeishuAPIError(f"飞书请求失败: {last_error}")


def list_tables(app_token: str) -> List[Dict[str, Any]]:
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables"
    response = _request("GET", url, headers=_headers(), params={"page_size": 500})
    payload = response.json()
    if payload.get("code") != 0:
        raise FeishuAPIError(f"读取数据表列表失败: {payload.get('msg')}")
    return payload.get("data", {}).get("items", []) or []


def create_table(app_token: str, table_name: str) -> str:
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables"
    response = _request(
        "POST",
        url,
        headers=_headers(),
        json={"table": {"name": table_name}},
        params={"user_id_type": "open_id"},
    )
    payload = response.json()
    if payload.get("code") != 0:
        raise FeishuAPIError(f"创建数据表失败: {payload.get('msg')}")
    return str(payload.get("data", {}).get("table_id") or "").strip()


def list_views(app_token: str, table_id: str) -> List[Dict[str, Any]]:
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/views"
    response = _request("GET", url, headers=_headers(), params={"page_size": 200})
    payload = response.json()
    if payload.get("code") != 0:
        raise FeishuAPIError(f"读取视图列表失败: {payload.get('msg')}")
    return payload.get("data", {}).get("items", []) or []


def ensure_fields(client: FeishuBitableClient, field_specs: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    existing_names = {str(item.field_name or "").strip() for item in client.list_fields()}
    created = []
    skipped = []
    for spec in field_specs:
        field_name = str(spec["name"]).strip()
        if field_name in existing_names:
            skipped.append(field_name)
            continue
        client.create_field(
            field_name=field_name,
            field_type=int(spec["type"]),
            ui_type=str(spec["ui_type"]),
            property=spec.get("property"),
        )
        created.append(field_name)
    return {"created": created, "skipped": skipped}


def build_open_url(original_url: str, table_id: str, view_id: str = "") -> str:
    parsed = urlparse(original_url)
    params = parse_qs(parsed.query)
    params["table"] = [table_id]
    if view_id:
        params["view"] = [view_id]
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, urlencode(params, doseq=True), parsed.fragment))


def create_or_reuse_sample_pool_table(feishu_url: str, table_name: str = DEFAULT_TABLE_NAME) -> Dict[str, Any]:
    app_token, original_url = resolve_app_token_and_original_url(feishu_url)
    target_table = None
    for table in list_tables(app_token):
        if str(table.get("name") or "").strip() == table_name:
            target_table = table
            break
    created_table = False
    if target_table:
        table_id = str(target_table.get("table_id") or "").strip()
    else:
        table_id = create_table(app_token, table_name)
        created_table = True
    client = FeishuBitableClient(app_token=app_token, table_id=table_id)
    _ensure_primary_title_field(client)
    field_result = ensure_fields(client, build_sample_pool_field_specs())
    views = list_views(app_token, table_id)
    first_view_id = str((views[0] or {}).get("view_id") or "").strip() if views else ""
    return {
        "app_token": app_token,
        "table_id": table_id,
        "table_name": table_name,
        "created_table": created_table,
        "created_fields": field_result["created"],
        "skipped_fields": field_result["skipped"],
        "view_id": first_view_id,
        "open_url": build_open_url(original_url, table_id=table_id, view_id=first_view_id),
    }


def _ensure_primary_title_field(client: FeishuBitableClient) -> None:
    fields = client.list_fields()
    if any(str(field.field_name or "").strip() == "商品标题" for field in fields):
        return
    default_fields = [field for field in fields if str(field.field_name or "").strip() in {"多行文本", "文本"}]
    if not default_fields:
        return
    field_id = default_fields[0].field_id
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{client.app_token}/tables/{client.table_id}/fields/{field_id}"
    response = _request(
        "PUT",
        url,
        headers=client._headers(),
        json={"field_name": "商品标题", "type": 1, "ui_type": "Text"},
    )
    payload = response.json()
    if payload.get("code") != 0:
        raise FeishuAPIError(f"重命名主字段失败: {payload.get('msg')}")


def sync_sample_pool(client: FeishuBitableClient, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {"rows": 0, "created": 0, "updated": 0}
    copied_attachment_count = 0
    fields = client.list_fields()
    field_names = {str(item.field_name or "").strip() for item in fields if str(item.field_name or "").strip()}
    field_specs = {
        str(item.field_name or "").strip(): str(item.ui_type or "")
        for item in fields
        if str(item.field_name or "").strip()
    }
    existing_records = client.list_records(page_size=100, limit=None)
    existing_map = {}
    for record in existing_records:
        record_key = _record_key_from_fields(record.fields)
        if record_key:
            existing_map[record_key] = record.record_id
    creates = []
    create_keys: List[str] = []
    updates = []
    for row in rows:
        record_key = _record_key_from_fields(row)
        # Write text/number fields first. Attachments are copied in a second pass
        # because source attachment tokens often belong to another Bitable and can
        # make a full batch request hang or fail without clear row-level feedback.
        fields_payload = {
            key: value
            for key, value in row.items()
            if key in field_names and key != "商品主图"
        }
        record_id = existing_map.get(record_key)
        if record_id:
            fields_payload = {key: value for key, value in fields_payload.items() if key not in MANUAL_FIELD_NAMES}
        fields_payload = {
            key: _normalize_field_value(value, field_specs.get(key, ""))
            for key, value in fields_payload.items()
        }
        if record_id:
            updates.append({"record_id": record_id, "fields": fields_payload})
        else:
            creates.append({"fields": fields_payload})
            create_keys.append(record_key)
    _log_sync_progress(f"写入商品字段：新增 {len(creates)} 行，更新 {len(updates)} 行")
    created_record_map: Dict[str, str] = {}
    processed_create_count = 0
    for batch in chunked(creates, 200):
        batch_start = processed_create_count
        batch_keys = create_keys[batch_start : batch_start + len(batch)]
        created_ids = _batch_create_records_return_ids(client, batch)
        processed_create_count += len(batch)
        for key, record_id in zip(batch_keys, created_ids):
            if key and record_id:
                created_record_map[key] = record_id
    for batch in chunked(updates, 200):
        client.batch_update_records(batch)

    attachment_updates: List[Dict[str, Any]] = []
    if "商品主图" in field_names:
        _log_sync_progress("开始复制商品主图附件")
        refreshed_map = {**existing_map, **created_record_map}
        copied_attachment_count = _copy_row_attachments_to_target(client, rows)
        for row in rows:
            record_id = refreshed_map.get(_record_key_from_fields(row))
            attachments = row.get("商品主图")
            if not record_id or not attachments:
                continue
            attachment_updates.append(
                {
                    "record_id": record_id,
                    "fields": {
                        "商品主图": _normalize_field_value(attachments, field_specs.get("商品主图", "")),
                    },
                }
            )
        _log_sync_progress(f"回填商品主图：{len(attachment_updates)} 行，复制附件 {copied_attachment_count} 个")
        for batch in chunked(attachment_updates, 50):
            client.batch_update_records(batch)
    return {
        "rows": len(rows),
        "created": len(creates),
        "updated": len(updates),
        "attachment_updated": len(attachment_updates),
        "copied_attachments": copied_attachment_count,
    }


def _log_sync_progress(message: str) -> None:
    print(f"[direction-sample-pool] {message}", file=sys.stderr, flush=True)


def _batch_create_records_return_ids(client: FeishuBitableClient, records: List[Dict[str, Any]]) -> List[str]:
    if not records:
        return []
    url = (
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{client.app_token}/tables/{client.table_id}/records/batch_create"
    )
    response = client._request("POST", url, headers=client._headers(), json={"records": records})
    result = response.json()
    if result.get("code") != 0:
        raise FeishuAPIError(f"批量创建记录失败: {result.get('msg')}")
    items = result.get("data", {}).get("records") or []
    return [str(item.get("record_id") or "").strip() for item in items]


def _record_key_from_fields(fields: Dict[str, Any]) -> str:
    batch_date = str(fields.get("批次日期") or "").strip()
    direction_name = str(fields.get("方向名称") or "").strip()
    fastmoss_url = _text_from_feishu_value(fields.get("FastMoss链接"))
    product_title = _text_from_feishu_value(fields.get("商品标题"))
    product_key = fastmoss_url or product_title
    if batch_date and direction_name and product_key:
        return "{batch}__{direction}__{product}".format(batch=batch_date, direction=direction_name, product=product_key)
    return str(fields.get("样本唯一键") or "").strip()


def _text_from_feishu_value(value: Any) -> str:
    if isinstance(value, list):
        parts = []
        for item in value:
            if isinstance(item, dict):
                parts.append(str(item.get("text") or item.get("link") or item.get("url") or "").strip())
            else:
                parts.append(str(item or "").strip())
        return " ".join([part for part in parts if part]).strip()
    if isinstance(value, dict):
        return str(value.get("text") or value.get("link") or value.get("url") or "").strip()
    return str(value or "").strip()


def _copy_row_attachments_to_target(client: FeishuBitableClient, rows: List[Dict[str, Any]]) -> int:
    cache: Dict[str, Dict[str, Any]] = {}
    copied = 0
    total = sum(
        1
        for row in rows
        for attachment in (row.get("商品主图") or [])
        if isinstance(attachment, dict) and str(attachment.get("file_token") or "").strip()
    )
    for row in rows:
        attachments = row.get("商品主图")
        if not isinstance(attachments, list) or not attachments:
            continue
        copied_attachments = []
        for attachment in attachments:
            if not isinstance(attachment, dict):
                continue
            old_token = str(attachment.get("file_token") or "").strip()
            if not old_token:
                continue
            if old_token not in cache:
                _log_sync_progress(f"复制商品主图 {copied + 1}/{total}: {attachment.get('name') or old_token}")
                try:
                    content, file_name, content_type, size = client.download_attachment_bytes(attachment)
                    cache[old_token] = client.upload_attachment(
                        content=content,
                        file_name=file_name,
                        content_type=content_type,
                        size=size,
                    )
                    copied += 1
                except Exception as exc:  # noqa: BLE001 - keep syncing other product rows.
                    _log_sync_progress(f"商品主图复制失败，已跳过: {old_token} ({exc})")
                    cache[old_token] = {}
            if cache.get(old_token):
                copied_attachments.append(cache[old_token])
        row["商品主图"] = copied_attachments
    return copied


def load_latest_sample_pool(latest_index_path: Path) -> List[Dict[str, Any]]:
    latest_payload = json.loads(latest_index_path.read_text(encoding="utf-8"))
    sample_pool_path = Path(str(latest_payload.get("sample_pool_path") or "").strip())
    if not sample_pool_path.exists():
        raise FileNotFoundError(f"sample_pool_path 不存在: {sample_pool_path}")
    payload = json.loads(sample_pool_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("direction_sample_pool.json 必须是数组")
    return payload


def sync_sample_pool_from_output_config(
    output_config_path: Path,
    artifacts_root: Path,
    table_name: str = DEFAULT_TABLE_NAME,
) -> Dict[str, Any]:
    config = json.loads(Path(output_config_path).read_text(encoding="utf-8"))
    latest_key = str(config.get("latest_key") or "").strip()
    if not latest_key:
        raise ValueError("output config 缺少 latest_key")
    latest_index_path = Path(artifacts_root) / "latest" / f"{latest_key}.json"
    target_url = str((config.get("target") or {}).get("feishu_url") or "").strip()
    if not target_url:
        raise ValueError("output config.target.feishu_url 不能为空")
    rows = load_latest_sample_pool(latest_index_path)
    table_info = create_or_reuse_sample_pool_table(target_url, table_name=table_name)
    client = FeishuBitableClient(app_token=table_info["app_token"], table_id=table_info["table_id"])
    summary = sync_sample_pool(client, rows)
    summary.update(
        {
            "output_config": str(output_config_path),
            "latest_index_path": str(latest_index_path),
            "sample_pool_table_url": table_info["open_url"],
            "created_table": table_info["created_table"],
            "created_fields": table_info["created_fields"],
            "skipped_fields": table_info["skipped_fields"],
        }
    )
    return summary
