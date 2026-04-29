#!/usr/bin/env python3
"""Create or reuse a Feishu market-direction-card table in the same bitable app."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests


ROOT = Path(__file__).resolve().parents[1]
SYNC_SKILL_DIR = ROOT.parent / "script-run-manager-sync"
sys.path.insert(0, str(SYNC_SKILL_DIR))

from core.bitable import (  # type: ignore  # noqa: E402
    FeishuAPIError,
    FeishuBitableClient,
    get_tenant_access_token,
    resolve_wiki_bitable_app_token,
)
from core.feishu_url_parser import parse_feishu_bitable_url  # type: ignore  # noqa: E402


TEXT = {"type": 1, "ui_type": "Text"}
NUMBER = {"type": 2, "ui_type": "Number"}
SINGLE_SELECT = {"type": 3, "ui_type": "SingleSelect"}
MULTI_SELECT = {"type": 4, "ui_type": "MultiSelect"}
DATETIME = {"type": 5, "ui_type": "DateTime"}
CHECKBOX = {"type": 7, "ui_type": "Checkbox"}

DEFAULT_COUNTRIES = ["VN", "TH", "MY", "PH", "ID", "SG", "US", "other"]
DEFAULT_CATEGORIES = ["hair_accessory", "light_tops", "other"]
DEFAULT_LEVELS = ["high", "medium", "low"]
DEFAULT_DIRECTION_FAMILIES = ["审美风格型", "日常场景型", "功能结果型", "形态专用型", "other"]
DEFAULT_DIRECTION_TIERS = ["priority", "balanced", "crowded", "low_sample"]
DEFAULT_ROUTE_PREFERENCES = ["replica_ok", "original_preferred", "neutral"]
DEFAULT_PRICE_BANDS = ["0-50", "50-100", "100-200", "200-500", "500-1000", "1000+", "unknown"]


def single_select(options: List[str]) -> Dict[str, Any]:
    return {
        "type": SINGLE_SELECT["type"],
        "ui_type": SINGLE_SELECT["ui_type"],
        "property": {"options": _build_options(options)},
    }


def multi_select(options: List[str]) -> Dict[str, Any]:
    return {
        "type": MULTI_SELECT["type"],
        "ui_type": MULTI_SELECT["ui_type"],
        "property": {"options": _build_options(options)},
    }


def _build_options(options: List[str]) -> List[Dict[str, Any]]:
    results = []
    seen = set()
    for index, item in enumerate(options):
        name = str(item or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        results.append({"name": name, "color": index % 54})
    return results


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_market_direction_fields() -> Dict[str, Any]:
    return _load_json(ROOT / "configs" / "market_insight_output_templates" / "market_direction_card_table_fields.json")


def _load_style_options() -> List[str]:
    taxonomy_dir = ROOT / "configs" / "market_insight_taxonomies"
    values: List[str] = []
    for file_path in sorted(taxonomy_dir.glob("*_v1.json")):
        payload = _load_json(file_path)
        values.extend(payload.get("style_tag_main") or [])
    return _dedupe(values)


def _load_multi_options(field_key: str) -> List[str]:
    taxonomy_dir = ROOT / "configs" / "market_insight_taxonomies"
    values: List[str] = []
    for file_path in sorted(taxonomy_dir.glob("*_v1.json")):
        payload = _load_json(file_path)
        values.extend(payload.get(field_key) or [])
    return _dedupe(values)


def _dedupe(values: List[str]) -> List[str]:
    results = []
    seen = set()
    for item in values:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        results.append(text)
    return results


def build_field_specs() -> List[Dict[str, Any]]:
    template = _load_market_direction_fields()
    style_options = _load_style_options()
    motive_options = _load_multi_options("value_points")
    element_options = _load_multi_options("element_tags")
    scene_options = _load_multi_options("scene_tags")

    spec_map: Dict[str, Dict[str, Any]] = {
        "方向ID": {"name": "方向ID", **TEXT},
        "方向规范Key": {"name": "方向规范Key", **TEXT},
        "方向实例ID": {"name": "方向实例ID", **TEXT},
        "批次日期": {"name": "批次日期", **DATETIME},
        "国家": {"name": "国家", **single_select(DEFAULT_COUNTRIES)},
        "类目": {"name": "类目", **single_select(DEFAULT_CATEGORIES)},
        "方向名称": {"name": "方向名称", **TEXT},
        "主风格": {"name": "主风格", **single_select(style_options)},
        "方向大类": {"name": "方向大类", **single_select(DEFAULT_DIRECTION_FAMILIES)},
        "方向层级": {"name": "方向层级", **single_select(DEFAULT_DIRECTION_TIERS)},
        "产品形态/结果": {"name": "产品形态/结果", **TEXT},
        "主要承载形态": {"name": "主要承载形态", **TEXT},
        "形态分布": {"name": "形态分布", **TEXT},
        "形态分布_商品数": {"name": "形态分布_商品数", **TEXT},
        "形态分布_销量": {"name": "形态分布_销量", **TEXT},
        "核心购买动机": {"name": "核心购买动机", **multi_select(motive_options)},
        "核心元素": {"name": "核心元素", **multi_select(element_options)},
        "核心场景": {"name": "核心场景", **multi_select(scene_options)},
        "目标价格带": {"name": "目标价格带", **multi_select(DEFAULT_PRICE_BANDS)},
        "热度等级": {"name": "热度等级", **single_select(DEFAULT_LEVELS)},
        "拥挤度等级": {"name": "拥挤度等级", **single_select(DEFAULT_LEVELS)},
        "优先级": {"name": "优先级", **single_select(DEFAULT_LEVELS)},
        "方向商品数": {"name": "方向商品数", **NUMBER},
        "方向7日销量中位数": {"name": "方向7日销量中位数", **NUMBER},
        "平均视频密度": {"name": "平均视频密度", **NUMBER},
        "平均达人密度": {"name": "平均达人密度", **NUMBER},
        "默认内容路线偏好": {"name": "默认内容路线偏好", **single_select(DEFAULT_ROUTE_PREFERENCES)},
        "任务类型": {"name": "任务类型", **TEXT},
        "目标任务池": {"name": "目标任务池", **TEXT},
        "任务Brief来源": {"name": "任务Brief来源", **TEXT},
        "任务Brief置信度": {"name": "任务Brief置信度", **TEXT},
        "选品任务要求": {"name": "选品任务要求", **TEXT},
        "代表商品ID": {"name": "代表商品ID", **TEXT},
        "代表商品名称": {"name": "代表商品名称", **TEXT},
        "选品建议": {"name": "选品建议", **TEXT},
        "避坑提示": {"name": "避坑提示", **TEXT},
        "置信度": {"name": "置信度", **NUMBER},
        "商品数": {"name": "商品数", **NUMBER},
        "平均热度分": {"name": "平均热度分", **NUMBER},
        "平均拥挤度分": {"name": "平均拥挤度分", **NUMBER},
        "方向Key": {"name": "方向Key", **TEXT},
        "是否最新批次": {"name": "是否最新批次", **CHECKBOX},
    }
    ordered_specs: List[Dict[str, Any]] = []
    for item in template.get("fields") or []:
        field_name = str(item.get("field_name") or "").strip()
        if field_name and field_name in spec_map:
            ordered_specs.append(spec_map[field_name])
    return ordered_specs


def resolve_app_token(feishu_url: str) -> tuple[str, Optional[str], str]:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise ValueError(f"无法解析飞书 URL: {feishu_url}")
    app_token = info.app_token
    if "/wiki/" in info.original_url:
        try:
            app_token = resolve_wiki_bitable_app_token(info.app_token)
        except FeishuAPIError:
            app_token = _resolve_embedded_bitable_app_token(info.app_token, info.table_id) or info.app_token
    return app_token, info.view_id, info.original_url


def _resolve_embedded_bitable_app_token(wiki_token: str, table_id: str) -> str:
    access_token = get_tenant_access_token()
    headers = {"Authorization": f"Bearer {access_token}"}
    node_resp = requests.get(
        "https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node",
        headers=headers,
        params={"token": wiki_token},
        timeout=30,
    )
    node_resp.raise_for_status()
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
    metainfo_resp.raise_for_status()
    metainfo_payload = metainfo_resp.json()
    for sheet in metainfo_payload.get("data", {}).get("sheets", []):
        block_info = sheet.get("blockInfo") or {}
        block_token = str(block_info.get("blockToken") or "").strip()
        if block_token.endswith(f"_{table_id}"):
            return block_token.rsplit("_", 1)[0]
    return ""


def _headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {get_tenant_access_token()}",
        "Content-Type": "application/json",
    }


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
    existing_names = {item.field_name for item in client.list_fields()}
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
    query = urlencode(params, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, query, parsed.fragment))


def create_or_reuse_market_direction_table(feishu_url: str, table_name: str) -> Dict[str, Any]:
    app_token, _, original_url = resolve_app_token(feishu_url)
    existing_tables = list_tables(app_token)

    target_table = None
    for item in existing_tables:
        if str(item.get("name") or "").strip() == table_name:
            target_table = item
            break

    created_table = False
    if target_table:
        table_id = str(target_table.get("table_id") or "").strip()
    else:
        table_id = create_table(app_token, table_name)
        created_table = True

    if not table_id:
        raise FeishuAPIError("未能获取目标 table_id")

    client = FeishuBitableClient(app_token=app_token, table_id=table_id)
    field_result = ensure_fields(client, build_field_specs())
    views = list_views(app_token, table_id)
    first_view_id = str((views[0] or {}).get("view_id") or "").strip() if views else ""

    return {
        "app_token": app_token,
        "table_name": table_name,
        "table_id": table_id,
        "created_table": created_table,
        "created_fields": field_result["created"],
        "skipped_fields": field_result["skipped"],
        "view_id": first_view_id,
        "open_url": build_open_url(original_url, table_id=table_id, view_id=first_view_id),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Create or reuse a Feishu market-direction-card table.")
    parser.add_argument("--feishu-url", required=True)
    parser.add_argument("--table-name", default="市场方向卡表")
    args = parser.parse_args()

    result = create_or_reuse_market_direction_table(
        feishu_url=args.feishu_url,
        table_name=args.table_name,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
