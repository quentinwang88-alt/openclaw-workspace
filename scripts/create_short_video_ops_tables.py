#!/usr/bin/env python3
"""Create lightweight Feishu Bitable schemas for short-video operations.

Targets:
- 当地爆款视频拆解表
- 短视频选题会前准备表

The script is idempotent for fields/views and can also sync accepted records
from 本周重点产品池 into 短视频选题会前准备表.
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import requests


HOT_VIDEO_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/TeFtw7rd8iIUpDkgd1GcjCrVnr9"
    "?table=tblBTrJQ4wdrvYnM&view=vew8EEtjEz"
)
TOPIC_PREP_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/Y2A6w9uEWi1qGfkhzJjceC8gnfc"
    "?table=tbl0zopE9XeH8MCF&view=vewaFaabC6"
)
WEEKLY_POOL_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/TO99wSeyVi9sUOkrCKZctHNknEc"
    "?table=tbljhxtIrgSgqrga&view=vewfEnNVzw"
)

TZ = ZoneInfo("Asia/Shanghai")


class FeishuAPIError(Exception):
    pass


def _load_openclaw_config() -> Dict[str, Any]:
    with (Path.home() / ".openclaw" / "openclaw.json").open("r", encoding="utf-8") as handle:
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
        last_error: Optional[str] = None
        for attempt in range(4):
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
            if response.status_code < 400 and result.get("code") in (0, None):
                return result
            code = result.get("code")
            last_error = (
                f"{method} {path} failed: HTTP {response.status_code}, "
                f"code={code}, msg={result.get('msg')}"
            )
            if code in {1254290, 1254291}:
                time.sleep(1.2 * (attempt + 1))
                continue
            break
        raise FeishuAPIError(last_error or f"{method} {path} failed")

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

    def delete_view(self, view_id: str) -> None:
        self._request(
            "DELETE",
            f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/views/{view_id}",
        )

    def update_table(self, payload: Dict[str, Any]) -> None:
        self._request("PATCH", f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}", json=payload)

    def list_records(self, page_size: int = 500) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        page_token: Optional[str] = None
        while True:
            params: Dict[str, Any] = {"page_size": page_size}
            if page_token:
                params["page_token"] = page_token
            result = self._request(
                "GET",
                f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records",
                params=params,
            )
            data = result.get("data", {})
            records.extend(data.get("items", []))
            if not data.get("has_more"):
                break
            page_token = data.get("page_token")
        return records

    def create_record(self, fields: Dict[str, Any]) -> Dict[str, Any]:
        result = self._request(
            "POST",
            f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records",
            json={"fields": fields},
        )
        return result.get("data", {}).get("record", {})


TEXT = {"type": 1, "ui_type": "Text"}
NUMBER = {"type": 2, "ui_type": "Number"}
DATE = {"type": 5, "ui_type": "DateTime", "property": {"date_formatter": "yyyy/MM/dd"}}
SINGLE = {"type": 3, "ui_type": "SingleSelect"}
MULTI = {"type": 4, "ui_type": "MultiSelect"}
USER = {"type": 11, "ui_type": "User"}
URL = {"type": 15, "ui_type": "Url"}
AUTO_NUMBER = {"type": 1005, "ui_type": "AutoNumber"}


def options(names: Iterable[str]) -> List[Dict[str, Any]]:
    return [{"name": name, "color": index % 54} for index, name in enumerate(names)]


def single(names: Iterable[str]) -> Dict[str, Any]:
    return {**SINGLE, "property": {"options": options(names)}}


def multi(names: Iterable[str]) -> Dict[str, Any]:
    return {**MULTI, "property": {"options": options(names)}}


HOT_VIDEO_FIELDS: List[Dict[str, Any]] = [
    {"name": "记录编号", **AUTO_NUMBER},
    {"name": "拆解日期", **DATE},
    {"name": "负责人", **USER},
    {"name": "市场", **single(["泰国", "墨西哥", "巴西", "越南", "马来", "其他"])},
    {"name": "类目", **single(["女装", "围巾", "冬装", "假发", "接发", "发饰", "首饰", "其他"])},
    {"name": "视频链接", **URL},
    {
        "name": "爆的原因初判",
        **multi(["内容好", "产品强", "价格低", "达人强", "账号基础", "擦边吸引", "场景代入强", "评论互动强", "不确定"]),
    },
    {"name": "首3秒怎么抓人", **TEXT},
    {"name": "产品怎么出现", **single(["开头出现", "中段出现", "结尾出现", "全程展示", "只轻微露出", "没有明显产品"])},
    {"name": "核心画面/动作", **TEXT},
    {"name": "用户为什么想买", **TEXT},
    {"name": "可借鉴点", **TEXT},
    {"name": "不能学/风险点", **TEXT},
    {"name": "可转成素材片段", **multi(["首镜", "细节", "结果", "场景", "结尾", "暂不确定"])},
    {"name": "是否值得复刻", **single(["值得复刻", "可参考但不直接复刻", "暂不确定", "不建议复刻"])},
]


TOPIC_PREP_FIELDS: List[Dict[str, Any]] = [
    {"name": "记录编号", **AUTO_NUMBER},
    {"name": "周期", **DATE},
    {"name": "产品", **TEXT},
    {
        "name": "业务线",
        **single(["泰国女装", "泰国围巾", "泰国冬装", "越南发饰", "马来配饰", "墨西哥接发", "墨西哥假发", "巴西内容测试", "其他"]),
    },
    {
        "name": "产品阶段",
        **single(["新品待测", "测品中", "初步出单", "打品中", "起量中", "稳定售卖", "衰退清库存", "暂停淘汰", "暂不确定"]),
    },
    {"name": "本周建议方向", **single(["主做", "少量测试", "先补素材", "暂缓"])},
    {"name": "想验证的视频方向", **TEXT},
    {"name": "首3秒想法", **TEXT},
    {"name": "主要展示方式", **multi(["实拍", "AI补片段", "混剪", "达人素材", "直播切片", "商品图辅助"])},
    {"name": "必须实拍素材", **TEXT},
    {
        "name": "AI可补素材",
        **multi(["结果先给", "前后反差", "痛点开场", "动作进入", "质感近景", "功能细节", "上身效果", "佩戴效果", "多角度展示", "镜前整理", "出门场景", "氛围过渡"]),
    },
    {"name": "参考视频", **URL},
    {"name": "预计样片数量", **NUMBER},
    {"name": "主要风险/疑问", **TEXT},
    {"name": "负责人", **USER},
    {"name": "选题会结论", **single(["采纳", "修改后采纳", "暂缓", "不做"])},
]


def monday_ms(now: Optional[datetime] = None) -> int:
    now = now or datetime.now(TZ)
    monday = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    return int(monday.timestamp() * 1000)


def next_monday_ms(now: Optional[datetime] = None) -> int:
    return monday_ms(now) + 7 * 24 * 60 * 60 * 1000


def option_names(field_property: Any) -> List[str]:
    if not isinstance(field_property, dict):
        return []
    return [str(item.get("name", "")).strip() for item in field_property.get("options", []) if item.get("name")]


def find_primary_field(fields: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for field in fields:
        if field.get("is_primary"):
            return field
    return fields[0] if fields else None


def field_needs_update(current: Dict[str, Any], spec: Dict[str, Any]) -> bool:
    current_options = set(option_names(current.get("property")))
    wanted_options = set(option_names(spec.get("property")))
    return current.get("type") != spec["type"] or (bool(wanted_options) and current_options != wanted_options)


def dedupe_fields(client: BitableClient, field_names: Iterable[str], dry_run: bool) -> Dict[str, Any]:
    result: Dict[str, Any] = {"deleted": [], "failed": []}
    wanted = set(field_names)
    fields = client.list_fields()
    seen: set[str] = set()
    for field in fields:
        name = str(field.get("field_name") or "")
        if name not in wanted:
            continue
        if name not in seen:
            seen.add(name)
            continue
        if field.get("is_primary"):
            continue
        field_id = str(field.get("field_id") or "")
        if dry_run:
            result["deleted"].append(f"{name}:{field_id}")
            continue
        try:
            client.delete_field(field_id)
            result["deleted"].append(f"{name}:{field_id}")
            time.sleep(0.8)
        except Exception as exc:  # noqa: BLE001
            result["failed"].append({"field": name, "field_id": field_id, "error": str(exc)})
    return result


def ensure_table_metadata(client: BitableClient, table_name: str, dry_run: bool) -> Dict[str, Any]:
    result: Dict[str, Any] = {"updated": [], "failed": []}
    if dry_run:
        result["updated"].append("表格名称")
        return result
    try:
        client.update_table({"name": table_name})
        result["updated"].append("表格名称")
    except Exception as exc:  # noqa: BLE001
        result["failed"].append({"target": "表格名称", "error": str(exc)})
    return result


def ensure_fields(
    client: BitableClient,
    field_specs: List[Dict[str, Any]],
    primary_name: str,
    primary_spec: Dict[str, Any],
    dry_run: bool,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {"created": [], "updated": [], "skipped": [], "failed": [], "fallbacks": []}
    dedupe_result = dedupe_fields(client, [spec["name"] for spec in field_specs], dry_run)
    result["dedupe"] = dedupe_result
    if dedupe_result["failed"]:
        result["failed"].extend(dedupe_result["failed"])
    fields = client.list_fields()
    primary = find_primary_field(fields)
    if primary and (primary.get("field_name") != primary_name or primary.get("type") != primary_spec["type"]):
        if dry_run:
            result["updated"].append(f"{primary.get('field_name')} -> {primary_name}")
        else:
            try:
                client.update_field(str(primary["field_id"]), {"name": primary_name, **primary_spec})
                result["updated"].append(f"{primary.get('field_name')} -> {primary_name}")
                fields = client.list_fields()
            except Exception as exc:  # noqa: BLE001
                fallback = {"name": primary_name, **TEXT}
                try:
                    client.update_field(str(primary["field_id"]), fallback)
                    result["updated"].append(f"{primary.get('field_name')} -> {primary_name}")
                    result["fallbacks"].append(f"{primary_name} 使用文本字段，原因: {exc}")
                    fields = client.list_fields()
                except Exception as fallback_exc:  # noqa: BLE001
                    result["failed"].append({"field": primary_name, "error": str(fallback_exc)})

    by_name = {field.get("field_name"): field for field in fields}
    for spec in field_specs:
        current = by_name.get(spec["name"])
        if current:
            if field_needs_update(current, spec):
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
            time.sleep(1.3)
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


def encode_filter_value(field: Dict[str, Any], values: List[Any]) -> str:
    if int(field.get("type") or 0) in {3, 4}:
        return json.dumps([option_id(field, str(value)) for value in values], ensure_ascii=False)
    return json.dumps(values, ensure_ascii=False)


def condition(by_name: Dict[str, Dict[str, Any]], field_name: str, operator: str, values: Optional[List[Any]] = None) -> Dict[str, Any]:
    field = by_name[field_name]
    item = {"field_id": field["field_id"], "operator": operator}
    if values is not None:
        item["value"] = encode_filter_value(field, values)
    return item


def resolve_view_spec(spec: Dict[str, Any], fields: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_name = {str(field.get("field_name") or ""): field for field in fields}
    resolved = dict(spec)
    if spec.get("filter"):
        conjunction, conditions = spec["filter"]
        resolved["filter_info"] = {
            "conjunction": conjunction,
            "conditions": [condition(by_name, field_name, operator, values) for field_name, operator, values in conditions],
        }
    if spec.get("visible"):
        visible = set(spec["visible"])
        resolved["hidden_fields"] = [field["field_id"] for field in fields if field.get("field_name") not in visible]
    if spec.get("sort"):
        resolved["sort_info"] = {
            "sorts": [{"field_id": by_name[field_name]["field_id"], "desc": descending} for field_name, descending in spec["sort"]]
        }
    if spec.get("group"):
        resolved["group_info"] = {
            "groups": [{"field_id": by_name[field_name]["field_id"], "desc": descending} for field_name, descending in spec["group"]]
        }
    return resolved


def hot_video_views() -> List[Dict[str, Any]]:
    return [
        {"name": "运营填写"},
        {
            "name": "本周重点拆解",
            "filter": ("and", [("拆解日期", "isGreaterEqual", [monday_ms()]), ("拆解日期", "isLess", [next_monday_ms()])]),
        },
        {
            "name": "值得复刻",
            "filter": ("or", [("是否值得复刻", "is", ["值得复刻"]), ("是否值得复刻", "is", ["可参考但不直接复刻"])]),
        },
        {"name": "按市场分组", "group": [("市场", False)]},
    ]


def topic_prep_views() -> List[Dict[str, Any]]:
    visible = [
        "周期",
        "产品",
        "业务线",
        "产品阶段",
        "本周建议方向",
        "想验证的视频方向",
        "首3秒想法",
        "主要展示方式",
        "必须实拍素材",
        "AI可补素材",
        "参考视频",
        "预计样片数量",
        "主要风险/疑问",
        "负责人",
        "选题会结论",
    ]
    return [
        {"name": "选题会前填写", "filter": ("or", [("选题会结论", "isEmpty", None), ("选题会结论", "is", ["暂缓"])]), "visible": visible},
        {
            "name": "本周选题会",
            "filter": ("and", [("周期", "is", [monday_ms()])]),
            "group": [("业务线", False)],
            "sort": [("本周建议方向", False)],
        },
        {"name": "已采纳待制作", "filter": ("or", [("选题会结论", "is", ["采纳"]), ("选题会结论", "is", ["修改后采纳"])]),},
        {"name": "暂缓/不做", "filter": ("or", [("选题会结论", "is", ["暂缓"]), ("选题会结论", "is", ["不做"])]),},
    ]


def ensure_views(client: BitableClient, view_specs: List[Dict[str, Any]], dry_run: bool) -> Dict[str, Any]:
    result: Dict[str, Any] = {"created": [], "updated": [], "failed": []}
    fields = client.list_fields()
    views = client.list_views()
    by_name = {view.get("view_name"): view for view in views}
    default_view = next((view for view in views if view.get("view_name") == "表格"), None)
    for index, spec in enumerate(view_specs):
        name = spec["name"]
        current = by_name.get(name)
        if not current and index == 0 and default_view:
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
                try:
                    client.update_view(str(current["view_id"]), resolved)
                except Exception:
                    client.update_view(str(current["view_id"]), {"name": name})
                    result.setdefault("fallbacks", []).append(f"{name}: 仅更新视图名称，筛选/分组需前端确认")
                result["updated"].append(name)
            else:
                view = client.create_view(name)
                try:
                    client.update_view(str(view["view_id"]), resolved)
                except Exception:
                    client.update_view(str(view["view_id"]), {"name": name})
                    result.setdefault("fallbacks", []).append(f"{name}: 仅创建视图名称，筛选/分组需前端确认")
                result["created"].append(name)
            time.sleep(0.3)
            views = client.list_views()
            by_name = {view.get("view_name"): view for view in views}
            default_view = next((view for view in views if view.get("view_name") == "表格"), None)
        except Exception as exc:  # noqa: BLE001
            result["failed"].append({"view": name, "error": str(exc)})
    return result


def validate(client: BitableClient, required_fields: List[str], required_views: List[str]) -> Dict[str, Any]:
    fields = client.list_fields()
    views = client.list_views()
    by_name = {field.get("field_name"): field for field in fields}
    field_names = list(by_name)
    view_names = [view.get("view_name") for view in views]
    type_checks = {
        "想验证的视频方向": by_name.get("想验证的视频方向", {}).get("type"),
        "AI可补素材": by_name.get("AI可补素材", {}).get("type"),
        "视频链接": by_name.get("视频链接", {}).get("type"),
        "参考视频": by_name.get("参考视频", {}).get("type"),
    }
    return {
        "field_count": len(fields),
        "fields": field_names,
        "view_count": len(views),
        "views": view_names,
        "required_fields_missing": [name for name in required_fields if name not in field_names],
        "required_views_missing": [name for name in required_views if name not in view_names],
        "type_checks": {key: value for key, value in type_checks.items() if value is not None},
    }


def normalize_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        parts: List[str] = []
        for item in value:
            if isinstance(item, dict):
                parts.append(str(item.get("text") or item.get("name") or item.get("link") or ""))
            else:
                parts.append(str(item))
        return " ".join(part for part in parts if part).strip()
    if isinstance(value, dict):
        return str(value.get("text") or value.get("name") or value.get("link") or "").strip()
    return str(value).strip()


def sync_weekly_pool_to_topic_prep(dry_run: bool) -> Dict[str, Any]:
    pool_app, pool_table, _ = parse_url(WEEKLY_POOL_URL)
    prep_app, prep_table, _ = parse_url(TOPIC_PREP_URL)
    pool = BitableClient(pool_app, pool_table)
    prep = BitableClient(prep_app, prep_table)
    source_records = pool.list_records()
    target_records = prep.list_records()
    target_keys = {
        (normalize_cell(record.get("fields", {}).get("周期")), normalize_cell(record.get("fields", {}).get("产品")))
        for record in target_records
    }
    created: List[str] = []
    skipped: List[str] = []
    for record in source_records:
        fields = record.get("fields", {})
        status = normalize_cell(fields.get("处理状态"))
        if status not in {"已采纳", "待短视频运营准备"}:
            skipped.append(f"{record.get('record_id')}: 状态={status or '空'}")
            continue
        product = normalize_cell(fields.get("产品名称")) or normalize_cell(fields.get("产品ID"))
        if not product:
            skipped.append(f"{record.get('record_id')}: 产品为空")
            continue
        cycle = fields.get("周期") or monday_ms()
        key = (normalize_cell(cycle), product)
        if key in target_keys:
            skipped.append(f"{record.get('record_id')}: 已存在 {product}")
            continue
        patch: Dict[str, Any] = {
            "周期": cycle,
            "产品": product,
        }
        business_line = normalize_cell(fields.get("业务线"))
        if business_line:
            patch["业务线"] = business_line
        product_stage = normalize_cell(fields.get("产品阶段"))
        if product_stage:
            patch["产品阶段"] = product_stage
        if not dry_run:
            prep.create_record(patch)
            time.sleep(0.3)
        target_keys.add(key)
        created.append(product)
    return {"created_count": len(created), "created": created, "skipped_count": len(skipped), "skipped": skipped[:20]}


@dataclass
class TableJob:
    label: str
    url: str
    table_name: str
    fields: List[Dict[str, Any]]
    primary_name: str
    primary_spec: Dict[str, Any]
    views: List[Dict[str, Any]]


def run_table_job(job: TableJob, dry_run: bool) -> Dict[str, Any]:
    app_token, table_id, view_id = parse_url(job.url)
    client = BitableClient(app_token, table_id)
    metadata_result = ensure_table_metadata(client, job.table_name, dry_run)
    field_result = ensure_fields(client, job.fields, job.primary_name, job.primary_spec, dry_run)
    view_result = ensure_views(client, job.views, dry_run)
    final = validate(client, [spec["name"] for spec in job.fields], [spec["name"] for spec in job.views])
    return {
        "label": job.label,
        "mode": "DRY RUN" if dry_run else "正式执行",
        "app_token": app_token,
        "table_id": table_id,
        "input_view_id": view_id,
        "metadata": metadata_result,
        "fields": field_result,
        "views": view_result,
        "validation": final,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sync-only", action="store_true", help="只从本周重点产品池同步选题准备记录")
    parser.add_argument("--skip-sync", action="store_true", help="建表后不执行同步")
    args = parser.parse_args()

    if args.sync_only:
        print(json.dumps({"sync": sync_weekly_pool_to_topic_prep(args.dry_run)}, ensure_ascii=False, indent=2))
        return 0

    jobs = [
        TableJob(
            label="当地爆款视频拆解表",
            url=HOT_VIDEO_URL,
            table_name="当地爆款视频拆解表",
            fields=HOT_VIDEO_FIELDS,
            primary_name="视频链接",
            primary_spec=URL,
            views=hot_video_views(),
        ),
        TableJob(
            label="短视频选题会前准备表",
            url=TOPIC_PREP_URL,
            table_name="短视频选题会前准备表",
            fields=TOPIC_PREP_FIELDS,
            primary_name="产品",
            primary_spec=TEXT,
            views=topic_prep_views(),
        ),
    ]

    output: Dict[str, Any] = {"tables": []}
    failed = False
    for job in jobs:
        result = run_table_job(job, args.dry_run)
        output["tables"].append(result)
        failed = failed or bool(
            result["metadata"]["failed"]
            or result["fields"]["failed"]
            or result["views"]["failed"]
            or result["validation"]["required_fields_missing"]
            or result["validation"]["required_views_missing"]
        )
    if not args.skip_sync:
        output["sync"] = sync_weekly_pool_to_topic_prep(args.dry_run)
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
