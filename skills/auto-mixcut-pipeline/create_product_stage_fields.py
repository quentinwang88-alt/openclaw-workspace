#!/usr/bin/env python3
"""
飞书多维表格字段创建脚本｜店铺产品阶段管理表

创建产品阶段管理所需的 12 个字段和 4 个视图。
Usage:
  python3 create_product_stage_fields.py [--dry-run]
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import requests


class FeishuAPIError(Exception):
    pass


# ── Auth & Config ───────────────────────────────────────────────────

def _load_openclaw_config() -> Dict[str, Any]:
    config_file = Path.home() / ".openclaw" / "openclaw.json"
    with config_file.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def get_tenant_access_token() -> str:
    config = _load_openclaw_config()
    app_id = config["channels"]["feishu"]["appId"]
    app_secret = config["channels"]["feishu"]["appSecret"]
    for attempt in range(4):
        try:
            response = requests.post(
                "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
                json={"app_id": app_id, "app_secret": app_secret},
                timeout=30,
            )
            result = response.json()
            if result.get("code") != 0:
                raise FeishuAPIError(f"获取 access_token 失败: {result.get('msg')}")
            return result["tenant_access_token"]
        except (requests.RequestException, ValueError, FeishuAPIError) as exc:
            if attempt < 3:
                time.sleep(2 ** attempt)
            else:
                raise FeishuAPIError(f"获取 access_token 最终失败: {exc}")


def resolve_wiki_bitable_app_token(wiki_token: str) -> str:
    for attempt in range(4):
        try:
            response = requests.get(
                "https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node",
                headers={"Authorization": f"Bearer {get_tenant_access_token()}"},
                params={"token": wiki_token},
                timeout=30,
            )
            result = response.json()
            if result.get("code") != 0:
                raise FeishuAPIError(f"解析 wiki token 失败: {result.get('msg')}")
            node = result.get("data", {}).get("node", {})
            obj_type = node.get("obj_type", "")
            if obj_type != "bitable":
                raise FeishuAPIError(f"不是 bitable 节点: {obj_type}")
            obj_token = str(node.get("obj_token", "") or "").strip()
            if not obj_token:
                raise FeishuAPIError("未返回底层 bitable obj_token")
            return obj_token
        except (requests.RequestException, ValueError, FeishuAPIError) as exc:
            if attempt < 3:
                time.sleep(2 ** attempt)
            else:
                raise FeishuAPIError(f"解析 wiki token 最终失败: {exc}")


# ── API Client ──────────────────────────────────────────────────────

class BitableClient:
    def __init__(self, app_token: str, table_id: str):
        self.app_token = app_token
        self.table_id = table_id
        self._access_token: Optional[str] = None
        self._expires_at: float = 0.0

    def _get_access_token(self) -> str:
        if self._access_token and time.time() < self._expires_at:
            return self._access_token
        self._access_token = get_tenant_access_token()
        self._expires_at = time.time() + 6900
        return self._access_token

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "application/json",
        }

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        for attempt in range(3):
            try:
                response = requests.request(method, url, timeout=30, **kwargs)
                response.raise_for_status()
                return response
            except requests.RequestException as exc:
                if attempt < 2:
                    time.sleep(2 ** attempt)
                else:
                    raise FeishuAPIError(f"请求失败: {exc}")

    # ── Fields ──────────────────────────────────────────────────

    def list_fields(self) -> List[Dict[str, Any]]:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields"
        response = self._request("GET", url, headers=self._headers(), params={"page_size": 500})
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"获取字段列表失败: {result.get('msg')}")
        return result.get("data", {}).get("items", [])

    def create_field(self, field_name: str, field_type: int, ui_type: str = "Text",
                     property: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields"
        payload: Dict[str, Any] = {"field_name": field_name, "type": field_type, "ui_type": ui_type}
        if property is not None:
            payload["property"] = property
        response = self._request("POST", url, headers=self._headers(), json=payload)
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"创建字段 '{field_name}' 失败: {result.get('msg')}")
        return result.get("data", {}).get("field", {})

    def update_field(self, field_id: str, field_name: str, field_type: int,
                     ui_type: str, property: Optional[Dict[str, Any]] = None) -> None:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields/{field_id}"
        payload: Dict[str, Any] = {"field_name": field_name, "type": field_type, "ui_type": ui_type}
        if property is not None:
            payload["property"] = property
        response = self._request("PUT", url, headers=self._headers(), json=payload)
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"更新字段 '{field_name}' 失败: {result.get('msg')}")

    # ── Views ───────────────────────────────────────────────────

    def list_views(self) -> List[Dict[str, Any]]:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/views"
        response = self._request("GET", url, headers=self._headers(), params={"page_size": 200})
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"获取视图列表失败: {result.get('msg')}")
        return result.get("data", {}).get("items", [])

    def create_view(self, view_name: str) -> Dict[str, Any]:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/views"
        payload = {"view_name": view_name, "view_type": "grid"}
        response = self._request("POST", url, headers=self._headers(), json=payload)
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"创建视图 '{view_name}' 失败: {result.get('msg')}")
        return result.get("data", {}).get("view", {})

    def update_view(self, view_id: str, view_name: str,
                    filter_info: Optional[Dict] = None,
                    group_info: Optional[Dict] = None) -> None:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/views/{view_id}"
        payload: Dict[str, Any] = {"view_name": view_name}
        if filter_info is not None:
            payload["filter_info"] = filter_info
        if group_info is not None:
            payload["group_info"] = group_info
        response = self._request("PATCH", url, headers=self._headers(), json=payload)
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"更新视图 '{view_name}' 失败: {result.get('msg')}")

    def delete_view(self, view_id: str) -> None:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/views/{view_id}"
        response = self._request("DELETE", url, headers=self._headers())
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"删除视图失败: {result.get('msg')}")


# ── Field Type Helpers ──────────────────────────────────────────────

TEXT = {"type": 1, "ui_type": "Text"}
NUMBER = {"type": 2, "ui_type": "Number"}
SINGLE_SELECT = {"type": 3, "ui_type": "SingleSelect"}
DATETIME = {"type": 5, "ui_type": "DateTime"}
USER_FIELD = {"type": 11, "ui_type": "User"}


def single_select(options: List[str]) -> Dict[str, Any]:
    return {
        "type": 3,
        "ui_type": "SingleSelect",
        "property": {"options": [{"name": item, "color": idx % 54} for idx, item in enumerate(options)]},
    }


# ── Field Definitions ───────────────────────────────────────────────

STAGE_OPTIONS = ["测品阶段", "打品阶段", "起量阶段", "维护阶段", "淘汰暂停"]
CREATOR_OPTIONS = ["暂不做达人", "达人池记录", "建联中", "已寄样", "已发布", "已出单", "暂停跟进"]
PAGE_STATUS_OPTIONS = ["未检查", "基础合格", "已优化", "需要重做"]
INVENTORY_OPTIONS = ["正常", "偏少", "断货风险", "库存压力", "暂不备货"]
WEEKLY_ACTION_OPTIONS = [
    "继续测品", "加码打品", "进入起量", "转入维护", "暂停淘汰",
    "优化商品页", "跟进达人", "补充库存", "等待数据",
]

FIELDS = [
    {"name": "产品名称", **TEXT},
    {"name": "当前阶段", **single_select(STAGE_OPTIONS)},
    {"name": "阶段开始日期", **DATETIME},
    {"name": "近7天订单", **NUMBER},
    {"name": "近7天视频数", **NUMBER},
    {"name": "有效视频角度", **TEXT},
    {"name": "达人进度", **single_select(CREATOR_OPTIONS)},
    {"name": "商品页状态", **single_select(PAGE_STATUS_OPTIONS)},
    {"name": "库存状态", **single_select(INVENTORY_OPTIONS)},
    {"name": "本周动作", **single_select(WEEKLY_ACTION_OPTIONS)},
    {"name": "负责人", **USER_FIELD},
    {"name": "备注", **TEXT},
]

# ── View Definitions ────────────────────────────────────────────────

VIEWS = [
    {
        "name": "全部产品视图",
        "filter_info": None,
        "group_info": {"groups": [{"field_name": "当前阶段", "descending": False}]},
    },
    {
        "name": "本周重点视图",
        "filter_info": {
            "conjunction": "and",
            "conditions": [
                {"field_name": "当前阶段", "operator": "isNot", "value": ["淘汰暂停"]},
            ],
        },
        "group_info": {"groups": [{"field_name": "本周动作", "descending": False}]},
    },
    {
        "name": "打品·起量视图",
        "filter_info": {
            "conjunction": "or",
            "conditions": [
                {"field_name": "当前阶段", "operator": "is", "value": ["打品阶段"]},
                {"field_name": "当前阶段", "operator": "is", "value": ["起量阶段"]},
            ],
        },
        "group_info": None,
    },
    {
        "name": "淘汰暂停视图",
        "filter_info": {
            "conjunction": "and",
            "conditions": [
                {"field_name": "当前阶段", "operator": "is", "value": ["淘汰暂停"]},
            ],
        },
        "group_info": None,
    },
]


# ── Parsing ─────────────────────────────────────────────────────────

def parse_url(feishu_url: str) -> tuple:
    cleaned = urlparse(feishu_url)
    params = parse_qs(cleaned.query)
    table_id = params.get("table", [""])[0]
    view_id = params.get("view", [""])[0] or None

    if "/wiki/" in cleaned.path:
        parts = cleaned.path.split("/wiki/")
        wiki_token = parts[1] if len(parts) > 1 else ""
        app_token = resolve_wiki_bitable_app_token(wiki_token)
    elif "/base/" in cleaned.path:
        parts = cleaned.path.split("/base/")
        app_token = parts[1] if len(parts) > 1 else ""
    else:
        raise FeishuAPIError(f"无法解析 URL: {feishu_url}")
    return app_token, table_id, view_id


# ── Ensure Logic ────────────────────────────────────────────────────

def option_names(property_value: Any) -> List[str]:
    if not isinstance(property_value, dict):
        return []
    options = property_value.get("options") or []
    return [str(item.get("name") or "").strip() for item in options if isinstance(item, dict) and item.get("name")]


def ensure_fields(client: BitableClient, dry_run: bool = False) -> Dict[str, Any]:
    existing_fields = client.list_fields()
    existing_names = {f["field_name"] for f in existing_fields}
    existing_by_name = {f["field_name"]: f for f in existing_fields}

    created = []
    skipped = []
    updated = []
    failed = []

    for spec in FIELDS:
        name = spec["name"]
        if name in existing_names:
            existing = existing_by_name[name]
            current_options = option_names(existing.get("property"))
            wanted_options = option_names(spec.get("property"))
            if wanted_options and set(wanted_options) != set(current_options):
                if not dry_run:
                    try:
                        client.update_field(
                            existing["field_id"], name,
                            spec["type"], spec["ui_type"], spec.get("property")
                        )
                        updated.append(name)
                    except Exception as e:
                        failed.append({"field": name, "error": str(e)})
                else:
                    updated.append(name)
            skipped.append(name)
            continue

        if dry_run:
            created.append(name)
            continue

        try:
            client.create_field(name, spec["type"], spec["ui_type"], spec.get("property"))
            created.append(name)
        except Exception as e:
            # Fallback: User field (type 11) → Text field (type 1)
            if spec["type"] == 11:
                try:
                    client.create_field(name, 1, "Text")
                    created.append(f"{name} (降级为文本字段)")
                except Exception as e2:
                    failed.append({"field": name, "error": f"人员字段/文本字段均失败: {e2}"})
            else:
                failed.append({"field": name, "error": str(e)})

    return {"created": created, "skipped": skipped, "updated": updated, "failed": failed}


def ensure_views(client: BitableClient, dry_run: bool = False) -> Dict[str, Any]:
    existing_views = client.list_views()
    existing_by_name = {v.get("view_name", ""): v for v in existing_views}

    created = []
    updated = []
    skipped = []
    failed = []

    for spec in VIEWS:
        name = spec["name"]
        if name in existing_by_name:
            view = existing_by_name[name]
            if not dry_run:
                try:
                    client.update_view(view["view_id"], name, spec.get("filter_info"), spec.get("group_info"))
                    updated.append(name)
                except Exception as e:
                    failed.append({"view": name, "error": str(e)})
            else:
                updated.append(name)
            skipped.append(name)
            continue

        if dry_run:
            created.append(name)
            continue

        try:
            new_view = client.create_view(name)
            view_id = new_view.get("view_id", "")
            if view_id and (spec.get("filter_info") or spec.get("group_info")):
                client.update_view(view_id, name, spec.get("filter_info"), spec.get("group_info"))
            created.append(name)
        except Exception as e:
            failed.append({"view": name, "error": str(e)})

    return {"created": created, "updated": updated, "skipped": skipped, "failed": failed}


# ── Main ────────────────────────────────────────────────────────────

TABLE_URL = "https://gcngopvfvo0q.feishu.cn/wiki/MTarwItbtivjEwknMYycLTAQnC2?table=tblXL2X8G5J529hc&view=vewa25skoN"


def main():
    dry_run = "--dry-run" in sys.argv

    print("=" * 60)
    print("店铺产品阶段管理表｜字段与视图创建")
    print(f"模式: {'DRY RUN (仅预览，不修改表格)' if dry_run else '正式执行'}")
    print("=" * 60)
    sys.stdout.flush()

    # 1. Parse URL
    print("\n[1/4] 解析表格地址...")
    app_token, table_id, view_id = parse_url(TABLE_URL)
    print(f"  app_token: {app_token}")
    print(f"  table_id:  {table_id}")
    print(f"  view_id:   {view_id or '(无)'}")

    client = BitableClient(app_token, table_id)

    # 2. Show existing fields
    print("\n[2/4] 已有字段...")
    existing_fields = client.list_fields()
    print(f"  共 {len(existing_fields)} 个字段:")
    for f in existing_fields:
        p = f.get("property", {})
        opts = option_names(p)
        opt_hint = f" 选项={opts}" if opts else ""
        print(f"    - {f['field_name']} (type={f.get('type')}, ui_type={f.get('ui_type')}){opt_hint}")

    # 3. Ensure fields
    print("\n[3/4] 确保字段存在...")
    field_result = ensure_fields(client, dry_run=dry_run)
    print(f"  新增: {field_result['created']}")
    print(f"  跳过: {field_result['skipped']}")
    print(f"  更新: {field_result['updated']}")
    if field_result["failed"]:
        print(f"  ❌ 失败: {field_result['failed']}")

    # 4. Ensure views
    print("\n[4/4] 确保视图存在...")
    view_result = ensure_views(client, dry_run=dry_run)
    print(f"  新增: {view_result['created']}")
    print(f"  更新: {view_result['updated']}")
    print(f"  跳过: {view_result['skipped']}")
    if view_result["failed"]:
        print(f"  ❌ 失败: {view_result['failed']}")

    print("\n" + "=" * 60)
    has_failures = bool(field_result["failed"] or view_result["failed"])
    if dry_run:
        print("✅ 预览完成。去掉 --dry-run 参数正式执行。")
    elif has_failures:
        print("⚠️ 部分操作失败，请检查上方失败详情。")
    else:
        print("✅ 所有字段和视图已就绪！")
    print(f"表格地址: {TABLE_URL}")
    print("=" * 60)

    return 1 if has_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
