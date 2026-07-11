#!/usr/bin/env python3
"""Create Feishu Bitable schema for 本周重点产品池.

This is a product-input table for weekly content topic meetings. It creates
17 Chinese fields and 5 operational views, intentionally avoiding script,
storyboard, and video-structure fields.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import parse_qs, urlparse

import requests


DEFAULT_TABLE_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/TO99wSeyVi9sUOkrCKZctHNknEc"
    "?table=tbljhxtIrgSgqrga&view=vewfEnNVzw"
)

TABLE_NAME = "本周重点产品池"
TABLE_DESCRIPTION = """店铺运营每周最多提交3-5个重点产品，不要提交过多。

每个产品只需要说清楚四件事：

1. 为什么这个产品本周值得做；
2. 当前最大问题是什么；
3. 用户为什么会买；
4. 视频里必须展示什么。

店铺运营不需要填写视频脚本、分镜、短视频结构，这些由短视频运营在选题会前后准备。"""


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

    response = requests.post(
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        json={"app_id": app_id, "app_secret": app_secret},
        timeout=30,
    )
    result = response.json()
    if result.get("code") != 0:
        raise FeishuAPIError(f"获取 access_token 失败: {result.get('msg')}")
    return result["tenant_access_token"]


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
    app_token = str(node.get("obj_token") or "").strip()
    if not app_token:
        raise FeishuAPIError("wiki 节点没有返回 bitable obj_token")
    return app_token


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
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    def _request(self, method: str, path: str, **kwargs: Any) -> Dict[str, Any]:
        response = requests.request(
            method,
            f"https://open.feishu.cn/open-apis{path}",
            headers=self._headers(),
            timeout=30,
            **kwargs,
        )
        try:
            result = response.json()
        except ValueError:
            result = {"code": response.status_code, "msg": response.text[:500]}
        if response.status_code >= 400 or result.get("code") not in (0, None):
            raise FeishuAPIError(
                f"{method} {path} failed: HTTP {response.status_code}, "
                f"code={result.get('code')}, msg={result.get('msg')}"
            )
        return result

    def list_fields(self) -> List[Dict[str, Any]]:
        result = self._request(
            "GET",
            f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields",
            params={"page_size": 500},
        )
        return result.get("data", {}).get("items", [])

    def create_field(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        payload = {
            "field_name": spec["name"],
            "type": spec["type"],
            "ui_type": spec["ui_type"],
        }
        if spec.get("property") is not None:
            payload["property"] = spec["property"]
        result = self._request(
            "POST",
            f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields",
            json=payload,
        )
        return result.get("data", {}).get("field", {})

    def update_field(self, field_id: str, spec: Dict[str, Any]) -> None:
        payload = {
            "field_name": spec["name"],
            "type": spec["type"],
            "ui_type": spec["ui_type"],
        }
        if spec.get("property") is not None:
            payload["property"] = spec["property"]
        self._request(
            "PUT",
            f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields/{field_id}",
            json=payload,
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

    def update_view(self, view_id: str, spec: Dict[str, Any]) -> None:
        payload: Dict[str, Any] = {"view_name": spec["name"]}
        prop: Dict[str, Any] = {}
        for key in ("filter_info", "hidden_fields", "group_info", "sort_info"):
            if spec.get(key) is not None:
                prop[key] = spec[key]
        if prop:
            payload["property"] = prop
        self._request(
            "PATCH",
            f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/views/{view_id}",
            json=payload,
        )

    def update_table(self, payload: Dict[str, Any]) -> None:
        self._request(
            "PATCH",
            f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}",
            json=payload,
        )


TEXT = {"type": 1, "ui_type": "Text"}
DATE = {"type": 5, "ui_type": "DateTime"}
SINGLE = {"type": 3, "ui_type": "SingleSelect"}
MULTI = {"type": 4, "ui_type": "MultiSelect"}
USER = {"type": 11, "ui_type": "User"}
ATTACHMENT = {"type": 17, "ui_type": "Attachment"}
AUTO_NUMBER = {"type": 1005, "ui_type": "AutoNumber"}


def options(names: Iterable[str]) -> List[Dict[str, Any]]:
    return [{"name": name, "color": index % 54} for index, name in enumerate(names)]


def single(names: Iterable[str]) -> Dict[str, Any]:
    return {**SINGLE, "property": {"options": options(names)}}


def multi(names: Iterable[str]) -> Dict[str, Any]:
    return {**MULTI, "property": {"options": options(names)}}


FIELD_SPECS: List[Dict[str, Any]] = [
    {"name": "记录编号", **AUTO_NUMBER},
    {"name": "周期", **DATE},
    {
        "name": "业务线",
        **single([
            "泰国女装",
            "泰国围巾",
            "泰国冬装",
            "越南发饰",
            "马来配饰",
            "墨西哥接发",
            "墨西哥假发",
            "巴西内容测试",
            "其他",
        ]),
    },
    {"name": "产品ID", **TEXT},
    {"name": "产品名称", **TEXT},
    {"name": "产品图片", **ATTACHMENT},
    {"name": "本周定位", **single(["主推", "测试", "补内容", "放大", "清库存", "观察"])},
    {"name": "优先级", **single(["P0本周必须做", "P1优先做", "P2有空再做"])},
    {
        "name": "为什么选它",
        **multi([
            "连续出单",
            "库存充足",
            "利润较好",
            "季节合适",
            "有爆款潜力",
            "需要补内容",
            "需要测款",
            "需要清库存",
            "老板指定",
        ]),
    },
    {
        "name": "当前问题",
        **multi([
            "缺曝光",
            "点击低",
            "转化弱",
            "素材不足",
            "视频少",
            "有单但没放大",
            "需要重新测试",
            "暂不清楚",
        ]),
    },
    {"name": "核心卖点", **TEXT},
    {"name": "适合人群/场景", **TEXT},
    {"name": "必须展示", **TEXT},
    {
        "name": "现有素材情况",
        **multi([
            "素材充足",
            "有部分实拍",
            "只有商品图",
            "有达人素材",
            "有直播切片",
            "素材不足",
            "暂不清楚",
        ]),
    },
    {"name": "提交人", **USER},
    {
        "name": "处理状态",
        **single([
            "草稿",
            "已提交选题会",
            "已采纳",
            "未采纳",
            "待短视频运营准备",
            "制作中",
            "已出样片",
            "已发布",
            "已暂停",
        ]),
    },
    {"name": "备注", **TEXT},
]


VIEW_SPECS: List[Dict[str, Any]] = [
    {
        "name": "店铺运营填写",
        "filter": ("or", [("处理状态", "is", ["草稿"]), ("处理状态", "is", ["已提交选题会"])]),
        "visible": [
            "周期",
            "业务线",
            "产品ID",
            "产品名称",
            "产品图片",
            "本周定位",
            "优先级",
            "为什么选它",
            "当前问题",
            "核心卖点",
            "适合人群/场景",
            "必须展示",
            "现有素材情况",
            "提交人",
            "处理状态",
            "备注",
        ],
    },
    {
        "name": "本周选题会",
        "filter": ("and", [("处理状态", "is", ["已提交选题会"])]),
        "sort": [("优先级", False)],
        "group": [("业务线", False)],
    },
    {
        "name": "短视频运营接收",
        "filter": ("or", [("处理状态", "is", ["已采纳"]), ("处理状态", "is", ["待短视频运营准备"])]),
        "visible": [
            "业务线",
            "产品名称",
            "产品图片",
            "本周定位",
            "优先级",
            "核心卖点",
            "适合人群/场景",
            "必须展示",
            "现有素材情况",
            "备注",
        ],
    },
    {
        "name": "已发布/已完成",
        "filter": ("and", [("处理状态", "is", ["已发布"])]),
    },
    {
        "name": "暂停/未采纳",
        "filter": ("or", [("处理状态", "is", ["未采纳"]), ("处理状态", "is", ["已暂停"])]),
    },
]


def option_names(field_property: Any) -> List[str]:
    if not isinstance(field_property, dict):
        return []
    return [str(item.get("name", "")).strip() for item in field_property.get("options", []) if item.get("name")]


def find_primary_field(fields: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for field in fields:
        if field.get("is_primary"):
            return field
    return fields[0] if fields else None


def ensure_table_metadata(client: BitableClient, dry_run: bool) -> Dict[str, Any]:
    result: Dict[str, Any] = {"updated": [], "failed": []}
    if dry_run:
        result["updated"].append("表格名称/说明")
        return result
    try:
        client.update_table({"name": TABLE_NAME})
        result["updated"].append("表格名称")
    except Exception as exc:  # noqa: BLE001
        result["failed"].append({"target": "表格名称", "error": str(exc)})
    try:
        client.update_table({"description": TABLE_DESCRIPTION})
        result["updated"].append("表格说明")
    except Exception as exc:  # noqa: BLE001
        result["failed"].append({"target": "表格说明", "error": str(exc)})
    return result


def ensure_fields(client: BitableClient, dry_run: bool) -> Dict[str, Any]:
    result: Dict[str, Any] = {"created": [], "updated": [], "skipped": [], "failed": []}
    fields = client.list_fields()
    primary = find_primary_field(fields)
    if primary and primary.get("field_name") != "产品名称":
        if dry_run:
            result["updated"].append(f"{primary.get('field_name')} -> 产品名称")
        else:
            try:
                client.update_field(str(primary["field_id"]), {"name": "产品名称", **TEXT})
                result["updated"].append(f"{primary.get('field_name')} -> 产品名称")
                fields = client.list_fields()
            except Exception as exc:  # noqa: BLE001
                result["failed"].append({"field": "产品名称", "error": str(exc)})

    by_name = {field.get("field_name"): field for field in fields}
    for spec in FIELD_SPECS:
        current = by_name.get(spec["name"])
        if current:
            current_options = set(option_names(current.get("property")))
            wanted_options = set(option_names(spec.get("property")))
            needs_update = current.get("type") != spec["type"] or (
                bool(wanted_options) and current_options != wanted_options
            )
            if needs_update:
                if dry_run:
                    result["updated"].append(spec["name"])
                else:
                    try:
                        client.update_field(str(current["field_id"]), spec)
                        result["updated"].append(spec["name"])
                        fields = client.list_fields()
                        by_name = {field.get("field_name"): field for field in fields}
                    except Exception as exc:  # noqa: BLE001
                        result["failed"].append({"field": spec["name"], "error": str(exc)})
            else:
                result["skipped"].append(spec["name"])
            continue
        if dry_run:
            result["created"].append(spec["name"])
            continue
        try:
            client.create_field(spec)
            result["created"].append(spec["name"])
            fields = client.list_fields()
            by_name = {field.get("field_name"): field for field in fields}
        except Exception as exc:  # noqa: BLE001
            result["failed"].append({"field": spec["name"], "error": str(exc)})
    return result


def option_id(field: Dict[str, Any], name: str) -> str:
    for option in (field.get("property") or {}).get("options") or []:
        if option.get("name") == name:
            return str(option.get("id") or "")
    raise FeishuAPIError(f"字段 {field.get('field_name')} 缺少选项: {name}")


def encode_filter_value(field: Dict[str, Any], values: List[str]) -> str:
    if int(field.get("type") or 0) in {3, 4}:
        return json.dumps([option_id(field, value) for value in values], ensure_ascii=False)
    return json.dumps(values, ensure_ascii=False)


def resolve_view_spec(spec: Dict[str, Any], fields: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_name = {str(field.get("field_name") or ""): field for field in fields}
    resolved = dict(spec)
    conjunction, conditions = spec["filter"]
    resolved["filter_info"] = {
        "conjunction": conjunction,
        "conditions": [
            {
                "field_id": by_name[field_name]["field_id"],
                "operator": operator,
                "value": encode_filter_value(by_name[field_name], values),
            }
            for field_name, operator, values in conditions
        ],
    }
    if spec.get("visible"):
        visible = set(spec["visible"])
        resolved["hidden_fields"] = [
            field["field_id"]
            for field in fields
            if field.get("field_name") not in visible
        ]
    if spec.get("sort"):
        resolved["sort_info"] = {
            "sorts": [
                {
                    "field_id": by_name[field_name]["field_id"],
                    "desc": descending,
                }
                for field_name, descending in spec["sort"]
            ]
        }
    if spec.get("group"):
        resolved["group_info"] = {
            "groups": [
                {
                    "field_id": by_name[field_name]["field_id"],
                    "desc": descending,
                }
                for field_name, descending in spec["group"]
            ]
        }
    return resolved


def ensure_views(client: BitableClient, dry_run: bool) -> Dict[str, Any]:
    result: Dict[str, Any] = {"created": [], "updated": [], "skipped": [], "failed": []}
    fields = client.list_fields()
    views = client.list_views()
    by_name = {view.get("view_name"): view for view in views}
    default_view = next((view for view in views if view.get("view_name") == "表格"), None)

    for spec in VIEW_SPECS:
        name = spec["name"]
        current = by_name.get(name)
        if not current and name == "店铺运营填写" and default_view:
            current = default_view
        try:
            resolved = resolve_view_spec(spec, fields)
        except Exception as exc:  # noqa: BLE001
            result["failed"].append({"view": name, "error": str(exc)})
            continue
        if dry_run:
            result["updated" if current else "created"].append(name)
            continue
        try:
            if current:
                client.update_view(str(current["view_id"]), resolved)
                result["updated"].append(name)
            else:
                view = client.create_view(name)
                client.update_view(str(view["view_id"]), resolved)
                result["created"].append(name)
            views = client.list_views()
            by_name = {view.get("view_name"): view for view in views}
            default_view = next((view for view in views if view.get("view_name") == "表格"), None)
        except Exception as exc:  # noqa: BLE001
            result["failed"].append({"view": name, "error": str(exc)})
    return result


def validate(client: BitableClient) -> Dict[str, Any]:
    fields = client.list_fields()
    views = client.list_views()
    field_names = [field.get("field_name") for field in fields]
    view_names = [view.get("view_name") for view in views]
    return {
        "field_count": len(fields),
        "fields": field_names,
        "view_count": len(views),
        "views": view_names,
        "required_fields_missing": [spec["name"] for spec in FIELD_SPECS if spec["name"] not in field_names],
        "required_views_missing": [spec["name"] for spec in VIEW_SPECS if spec["name"] not in view_names],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=DEFAULT_TABLE_URL)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    app_token, table_id, view_id = parse_url(args.url)
    client = BitableClient(app_token, table_id)

    print("本周重点产品池｜字段与视图创建")
    print(f"模式: {'DRY RUN' if args.dry_run else '正式执行'}")
    print(f"app_token: {app_token}")
    print(f"table_id: {table_id}")
    print(f"view_id: {view_id or '(无)'}")

    metadata_result = ensure_table_metadata(client, args.dry_run)
    print("\n表格信息:")
    print(json.dumps(metadata_result, ensure_ascii=False, indent=2))

    field_result = ensure_fields(client, args.dry_run)
    print("\n字段结果:")
    print(json.dumps(field_result, ensure_ascii=False, indent=2))

    view_result = ensure_views(client, args.dry_run)
    print("\n视图结果:")
    print(json.dumps(view_result, ensure_ascii=False, indent=2))

    final = validate(client)
    print("\n验收快照:")
    print(json.dumps(final, ensure_ascii=False, indent=2))

    failed = bool(
        metadata_result["failed"]
        or field_result["failed"]
        or view_result["failed"]
        or final["required_fields_missing"]
        or final["required_views_missing"]
        or final["field_count"] > 17
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
