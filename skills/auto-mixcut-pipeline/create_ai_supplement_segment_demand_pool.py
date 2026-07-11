#!/usr/bin/env python3
"""
Create the lightweight Feishu Bitable schema for AI supplement segment demand.

This table is intentionally a "segment request menu", not a video-structure
template. Short-video operators choose role/type/intent; OpenClaw later turns
validated rows into Prompt Package work orders.

Usage:
  python3 skills/auto-mixcut-pipeline/create_ai_supplement_segment_demand_pool.py --url <base-or-wiki-url> --dry-run
  python3 skills/auto-mixcut-pipeline/create_ai_supplement_segment_demand_pool.py --url <base-or-wiki-url>
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import parse_qs, urlparse

import requests


DEFAULT_TABLE_URL = "https://gcngopvfvo0q.feishu.cn/base/LpE8bp6rXa9g6Us1SThcFWfOnzd?table=tblTMWgpeTHSCvJY"


class FeishuAPIError(Exception):
    pass


def _load_openclaw_config() -> Dict[str, Any]:
    config_file = Path.home() / ".openclaw" / "openclaw.json"
    with config_file.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def get_tenant_access_token() -> str:
    config = _load_openclaw_config()
    feishu = config.get("channels", {}).get("feishu", {})
    app_id = feishu.get("appId")
    app_secret = feishu.get("appSecret")
    if not app_id or not app_secret:
        accounts = feishu.get("accounts", {})
        first = next(iter(accounts.values()), {})
        app_id = first.get("appId")
        app_secret = first.get("appSecret")
    if not app_id or not app_secret:
        raise FeishuAPIError("openclaw.json 缺少 Feishu appId/appSecret")

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
                time.sleep(2**attempt)
            else:
                raise FeishuAPIError(f"获取 access_token 最终失败: {exc}") from exc


def resolve_wiki_bitable_app_token(wiki_token: str) -> str:
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
    if node.get("obj_type") != "bitable":
        raise FeishuAPIError(f"wiki 节点不是 bitable: {node.get('obj_type')}")
    return str(node.get("obj_token") or "")


def parse_url(feishu_url: str) -> tuple[str, str, Optional[str]]:
    parsed = urlparse(feishu_url)
    params = parse_qs(parsed.query)
    table_id = params.get("table", [""])[0]
    view_id = params.get("view", [""])[0] or None
    if not table_id:
        raise FeishuAPIError("URL 缺少 table 参数")
    if "/wiki/" in parsed.path:
        wiki_token = parsed.path.split("/wiki/", 1)[1]
        app_token = resolve_wiki_bitable_app_token(wiki_token)
    elif "/base/" in parsed.path:
        app_token = parsed.path.split("/base/", 1)[1]
    else:
        raise FeishuAPIError(f"无法解析飞书多维表格 URL: {feishu_url}")
    if not app_token:
        raise FeishuAPIError("无法解析 app_token")
    return app_token, table_id, view_id


class BitableClient:
    def __init__(self, app_token: str, table_id: str):
        self.app_token = app_token
        self.table_id = table_id
        self._token: Optional[str] = None
        self._expires_at = 0.0

    def _headers(self) -> Dict[str, str]:
        if not self._token or time.time() >= self._expires_at:
            self._token = get_tenant_access_token()
            self._expires_at = time.time() + 6900
        return {"Authorization": f"Bearer {self._token}", "Content-Type": "application/json"}

    def _request(self, method: str, path: str, **kwargs: Any) -> Dict[str, Any]:
        url = f"https://open.feishu.cn/open-apis{path}"
        response = requests.request(method, url, headers=self._headers(), timeout=30, **kwargs)
        try:
            result = response.json()
        except ValueError:
            result = {"code": response.status_code, "msg": response.text[:500]}
        if response.status_code >= 400 or result.get("code") not in (0, None):
            raise FeishuAPIError(f"HTTP {response.status_code}, code={result.get('code')}, msg={result.get('msg')}")
        return result

    def list_fields(self) -> List[Dict[str, Any]]:
        result = self._request(
            "GET",
            f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields",
            params={"page_size": 500},
        )
        return result.get("data", {}).get("items", [])

    def create_field(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        payload = {"field_name": spec["name"], "type": spec["type"], "ui_type": spec["ui_type"]}
        if spec.get("property") is not None:
            payload["property"] = spec["property"]
        result = self._request(
            "POST",
            f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields",
            json=payload,
        )
        return result.get("data", {}).get("field", {})

    def update_field(self, field_id: str, spec: Dict[str, Any]) -> None:
        payload = {"field_name": spec["name"], "type": spec["type"], "ui_type": spec["ui_type"]}
        if spec.get("property") is not None:
            payload["property"] = spec["property"]
        self._request(
            "PUT",
            f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields/{field_id}",
            json=payload,
        )

    def delete_field(self, field_id: str) -> None:
        self._request(
            "DELETE",
            f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields/{field_id}",
        )

    def list_views(self) -> List[Dict[str, Any]]:
        result = self._request(
            "GET",
            f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/views",
            params={"page_size": 200},
        )
        return result.get("data", {}).get("items", [])

    def create_view(self, name: str) -> Dict[str, Any]:
        result = self._request(
            "POST",
            f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/views",
            json={"view_name": name, "view_type": "grid"},
        )
        return result.get("data", {}).get("view", {})

    def delete_view(self, view_id: str) -> None:
        self._request(
            "DELETE",
            f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/views/{view_id}",
        )

    def update_view(self, view_id: str, spec: Dict[str, Any]) -> None:
        payload: Dict[str, Any] = {"view_name": spec["name"]}
        property_payload: Dict[str, Any] = {}
        if spec.get("property") is not None:
            property_payload.update(spec["property"])
        if spec.get("filter_info") is not None:
            property_payload["filter_info"] = spec["filter_info"]
        if spec.get("hidden_fields") is not None:
            property_payload["hidden_fields"] = spec["hidden_fields"]
        if spec.get("group_info") is not None:
            property_payload["group_info"] = spec["group_info"]
        if property_payload:
            payload["property"] = property_payload
        self._request(
            "PATCH",
            f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/views/{view_id}",
            json=payload,
        )


TEXT = {"type": 1, "ui_type": "Text"}
NUMBER = {"type": 2, "ui_type": "Number"}
CHECKBOX = {"type": 7, "ui_type": "Checkbox"}
USER = {"type": 11, "ui_type": "User"}
URL = {"type": 15, "ui_type": "Url"}
AUTO_NUMBER = {"type": 1005, "ui_type": "AutoNumber"}


def options(names: Iterable[str]) -> List[Dict[str, Any]]:
    return [{"name": name, "color": index % 54} for index, name in enumerate(names)]


def single(names: Iterable[str]) -> Dict[str, Any]:
    return {"type": 3, "ui_type": "SingleSelect", "property": {"options": options(names)}}


BUSINESS_LINES = ["泰国女装", "泰国围巾", "泰国冬装", "墨西哥接发", "墨西哥假发", "巴西内容测试", "其他"]
DEMAND_STATUS = ["待填写", "待确认", "待生成", "生成中", "已回流", "待质检", "可用", "不可用", "已使用", "已暂停"]
ROLES = ["首镜", "细节", "结果", "场景"]
SEGMENT_TYPES = [
    "结果先给",
    "前后反差",
    "痛点开场",
    "动作进入",
    "质感近景",
    "功能细节",
    "上身效果",
    "佩戴效果",
    "多角度展示",
    "镜前整理",
    "出门场景",
    "氛围过渡",
]
REFERENCE_OPTIONS = ["不需要参考", "建议参考", "必须参考"]
PRIORITIES = ["高", "中", "低"]
QC_RESULTS = ["待质检", "可用", "需要修改", "不可用"]


PRIMARY_FIELD = {"name": "产品", **TEXT}

FIELDS: List[Dict[str, Any]] = [
    {"name": "需求编号", **AUTO_NUMBER},
    {"name": "业务线", **single(BUSINESS_LINES)},
    {"name": "负责人", **USER},
    {"name": "需求状态", **single(DEMAND_STATUS)},
    {"name": "素材角色", **single(ROLES)},
    {"name": "片段类型", **single(SEGMENT_TYPES)},
    {"name": "内容意图", **TEXT},
    {"name": "必须出现", **TEXT},
    {"name": "禁止出现", **TEXT},
    {"name": "参考要求", **single(REFERENCE_OPTIONS)},
    {"name": "参考素材", **URL},
    {"name": "生成数量", **NUMBER},
    {"name": "优先级", **single(PRIORITIES)},
    {"name": "是否可提单", **CHECKBOX},
    {"name": "备注", **TEXT},
    {"name": "Prompt包编号", **TEXT},
    {"name": "回流素材", **URL},
    {"name": "质检结果", **single(QC_RESULTS)},
]

FIELD_ALIASES = {
    "业务线": ["所属业务线"],
    "参考要求": ["是否必须参考实拍"],
    "参考素材": ["参考素材链接"],
    "回流素材": ["回流链接"],
}


OPERATOR_FIELDS = [
    "产品",
    "业务线",
    "素材角色",
    "片段类型",
    "内容意图",
    "必须出现",
    "禁止出现",
    "参考要求",
    "参考素材",
    "生成数量",
    "优先级",
    "是否可提单",
    "备注",
]


def condition(field_name: str, operator: str, value: List[str]) -> Dict[str, Any]:
    return {"field_name": field_name, "operator": operator, "value": value}


VIEWS: List[Dict[str, Any]] = [
    {
        "name": "运营填写",
        "filter_info": {"conjunction": "and", "conditions": [condition("需求状态", "is", ["待填写"])]},
        "visible_fields_note": OPERATOR_FIELDS,
    },
    {
        "name": "待确认",
        "filter_info": {"conjunction": "and", "conditions": [condition("需求状态", "is", ["待确认"])]},
    },
    {
        "name": "待生成",
        "filter_info": {
            "conjunction": "and",
            "conditions": [
                condition("是否可提单", "is", ["true"]),
                condition("需求状态", "is", ["待生成"]),
            ],
        },
    },
    {
        "name": "已回流待质检",
        "filter_info": {
            "conjunction": "or",
            "conditions": [
                condition("需求状态", "is", ["已回流"]),
                condition("需求状态", "is", ["待质检"]),
            ],
        },
    },
    {
        "name": "可用素材",
        "filter_info": {"conjunction": "and", "conditions": [condition("质检结果", "is", ["可用"])]},
    },
]


def option_names(field_property: Any) -> List[str]:
    if not isinstance(field_property, dict):
        return []
    return [str(item.get("name", "")).strip() for item in field_property.get("options", []) if item.get("name")]


def field_map(fields: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {str(field.get("field_name") or ""): field for field in fields}


def option_id(field: Dict[str, Any], name: str) -> str:
    for option in (field.get("property") or {}).get("options") or []:
        if option.get("name") == name:
            return str(option.get("id") or "")
    raise FeishuAPIError(f"字段 {field.get('field_name')} 缺少选项: {name}")


def encode_filter_value(field: Dict[str, Any], values: List[str]) -> str:
    field_type = int(field.get("type") or 0)
    if field_type in {3, 4}:
        return json.dumps([option_id(field, value) for value in values], ensure_ascii=False)
    if field_type == 7:
        parsed = []
        for value in values:
            parsed.append(str(value).lower() in {"true", "1", "yes", "已勾选"})
        return json.dumps(parsed, ensure_ascii=False)
    return json.dumps(values, ensure_ascii=False)


def resolve_view_spec(spec: Dict[str, Any], fields: List[Dict[str, Any]]) -> Dict[str, Any]:
    resolved = dict(spec)
    fields_by_name = field_map(fields)
    filter_info = spec.get("filter_info")
    if filter_info:
        conditions = []
        for item in filter_info.get("conditions", []):
            field = fields_by_name.get(item["field_name"])
            if not field:
                raise FeishuAPIError(f"视图 {spec['name']} 缺少筛选字段: {item['field_name']}")
            conditions.append(
                {
                    "field_id": field["field_id"],
                    "operator": item.get("operator", "is"),
                    "value": encode_filter_value(field, item.get("value", [])),
                }
            )
        resolved["filter_info"] = {
            "conjunction": filter_info.get("conjunction", "and"),
            "conditions": conditions,
        }
    if spec.get("visible_fields_note"):
        visible = set(spec["visible_fields_note"])
        hidden = [
            field["field_id"]
            for field in fields
            if field.get("field_name") not in visible
        ]
        resolved["hidden_fields"] = hidden
    return resolved


def find_primary_field(fields: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for field in fields:
        if field.get("is_primary"):
            return field
    return fields[0] if fields else None


def ensure_primary_product_field(client: BitableClient, dry_run: bool) -> Dict[str, Any]:
    fields = client.list_fields()
    primary = find_primary_field(fields)
    if not primary:
        return {"updated": [], "skipped": [], "failed": [{"field": "产品", "error": "未找到主字段"}]}
    if primary.get("field_name") == "产品":
        return {"updated": [], "skipped": ["产品"], "failed": []}
    if dry_run:
        return {"updated": [f"{primary.get('field_name')} -> 产品"], "skipped": [], "failed": []}
    try:
        client.update_field(str(primary["field_id"]), PRIMARY_FIELD)
        return {"updated": [f"{primary.get('field_name')} -> 产品"], "skipped": [], "failed": []}
    except Exception as exc:  # noqa: BLE001
        return {"updated": [], "skipped": [], "failed": [{"field": "产品", "error": str(exc)}]}


def ensure_fields(client: BitableClient, dry_run: bool) -> Dict[str, Any]:
    existing = client.list_fields()
    by_name = {field["field_name"]: field for field in existing}
    result = {"created": [], "updated": [], "skipped": [], "failed": []}
    for spec in FIELDS:
        name = spec["name"]
        current = by_name.get(name)
        if not current:
            for alias in FIELD_ALIASES.get(name, []):
                alias_field = by_name.get(alias)
                if alias_field:
                    current = alias_field
                    break
        if current:
            current_options = set(option_names(current.get("property")))
            wanted_options = set(option_names(spec.get("property")))
            needs_update = current.get("field_name") != name or (wanted_options and current_options != wanted_options)
            if needs_update:
                if dry_run:
                    result["updated"].append(f"{current.get('field_name')} -> {name}")
                else:
                    try:
                        client.update_field(current["field_id"], spec)
                        result["updated"].append(f"{current.get('field_name')} -> {name}")
                        refreshed = client.list_fields()
                        by_name = {field["field_name"]: field for field in refreshed}
                    except Exception as exc:  # noqa: BLE001
                        result["failed"].append({"field": name, "error": str(exc)})
            else:
                result["skipped"].append(name)
            continue
        if dry_run:
            result["created"].append(name)
            continue
        try:
            client.create_field(spec)
            result["created"].append(name)
            refreshed = client.list_fields()
            by_name = {field["field_name"]: field for field in refreshed}
        except Exception as exc:  # noqa: BLE001
            if spec["type"] in (11, 1005):
                fallback = {"name": name, **TEXT}
                try:
                    client.create_field(fallback)
                    result["created"].append(f"{name} (降级为文本)")
                    refreshed = client.list_fields()
                    by_name = {field["field_name"]: field for field in refreshed}
                except Exception as fallback_exc:  # noqa: BLE001
                    result["failed"].append({"field": name, "error": f"{exc}; fallback: {fallback_exc}"})
            else:
                result["failed"].append({"field": name, "error": str(exc)})
    return result


def cleanup_duplicate_fields(client: BitableClient, dry_run: bool, delete_extra: bool = False) -> Dict[str, Any]:
    wanted_names = {"产品", *(field["name"] for field in FIELDS)}
    fields = client.list_fields()
    seen: set[str] = set()
    deleted: List[str] = []
    failed: List[Dict[str, str]] = []
    for field in fields:
        name = str(field.get("field_name") or "")
        is_extra = name not in wanted_names
        if not is_extra and name not in seen:
            seen.add(name)
            continue
        if is_extra and not delete_extra:
            continue
        label = f"{name} ({field.get('field_id')})"
        if dry_run:
            deleted.append(label)
            continue
        try:
            client.delete_field(str(field["field_id"]))
            deleted.append(label)
        except Exception as exc:  # noqa: BLE001
            failed.append({"field": label, "error": str(exc)})
    return {"deleted": deleted, "failed": failed}


def cleanup_extra_views(client: BitableClient, dry_run: bool, delete_extra: bool = False) -> Dict[str, Any]:
    if not delete_extra:
        return {"deleted": [], "failed": []}
    wanted = {view["name"] for view in VIEWS}
    deleted: List[str] = []
    failed: List[Dict[str, str]] = []
    for view in client.list_views():
        name = str(view.get("view_name") or "")
        if name in wanted or name == "表格":
            continue
        label = f"{name} ({view.get('view_id')})"
        if dry_run:
            deleted.append(label)
            continue
        try:
            client.delete_view(str(view["view_id"]))
            deleted.append(label)
        except Exception as exc:  # noqa: BLE001
            failed.append({"view": label, "error": str(exc)})
    return {"deleted": deleted, "failed": failed}


def ensure_views(client: BitableClient, dry_run: bool) -> Dict[str, Any]:
    existing = client.list_views()
    by_name = {view.get("view_name", ""): view for view in existing}
    fields = client.list_fields()
    result = {"created": [], "updated": [], "skipped": [], "failed": []}
    for spec in VIEWS:
        name = spec["name"]
        current = by_name.get(name)
        try:
            resolved_spec = resolve_view_spec(spec, fields)
        except Exception as exc:  # noqa: BLE001
            result["failed"].append({"view": name, "error": str(exc)})
            continue
        if dry_run:
            result["updated" if current else "created"].append(name)
            continue
        try:
            if current:
                client.update_view(current["view_id"], resolved_spec)
                result["updated"].append(name)
            else:
                view = client.create_view(name)
                view_id = view.get("view_id")
                if view_id and (
                    resolved_spec.get("filter_info")
                    or resolved_spec.get("group_info")
                    or resolved_spec.get("hidden_fields")
                ):
                    client.update_view(view_id, resolved_spec)
                result["created"].append(name)
        except Exception as exc:  # noqa: BLE001
            result["failed"].append({"view": name, "error": str(exc)})
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=DEFAULT_TABLE_URL)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--delete-extra-fields", action="store_true")
    parser.add_argument("--delete-extra-views", action="store_true")
    args = parser.parse_args()

    app_token, table_id, view_id = parse_url(args.url)
    client = BitableClient(app_token, table_id)

    print("AI补素材片段需求池轻量版｜字段与视图创建")
    print(f"模式: {'DRY RUN' if args.dry_run else '正式执行'}")
    print(f"app_token: {app_token}")
    print(f"table_id: {table_id}")
    print(f"view_id: {view_id or '(无)'}")

    primary_result = ensure_primary_product_field(client, args.dry_run)
    print("\n主字段结果:")
    print(json.dumps(primary_result, ensure_ascii=False, indent=2))

    field_result = ensure_fields(client, args.dry_run)
    print("\n字段结果:")
    print(json.dumps(field_result, ensure_ascii=False, indent=2))

    duplicate_result = cleanup_duplicate_fields(client, args.dry_run, delete_extra=args.delete_extra_fields)
    print("\n重复字段清理:")
    print(json.dumps(duplicate_result, ensure_ascii=False, indent=2))

    view_result = ensure_views(client, args.dry_run)
    print("\n视图结果:")
    print(json.dumps(view_result, ensure_ascii=False, indent=2))

    extra_view_result = cleanup_extra_views(client, args.dry_run, delete_extra=args.delete_extra_views)
    print("\n多余视图清理:")
    print(json.dumps(extra_view_result, ensure_ascii=False, indent=2))

    final_fields = client.list_fields()
    print(f"\n字段总数: {len(final_fields)}")
    print(f"运营填写视图建议展示字段: {len(OPERATOR_FIELDS)} 个 - {', '.join(OPERATOR_FIELDS)}")

    failed = bool(
        primary_result["failed"]
        or field_result["failed"]
        or duplicate_result["failed"]
        or view_result["failed"]
        or extra_view_result["failed"]
    )
    print("\n完成" if not failed else "\n部分失败")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
