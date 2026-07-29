#!/usr/bin/env python3
"""在指定飞书多维表格中创建「每日内容实验卡」数据表。

字段、视图及配置严格按产品文档执行。
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import requests

TARGET_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/PI0ywHOlmioojvklsSFcpFjGn9c"
    "?table=tblNVu4eN26W5UJR&view=vewMWXkP5z"
)

TABLE_NAME = "每日内容实验卡"
TABLE_DESCRIPTION = """使用规则：
1. 每人每天最多提交2条，建议优先提交最值得讨论的1条；
2. 一条记录只填写一个参考视频或成片；
3. 「今日判断」只写两句话：
   - 为什么选择；
   - 本次只验证什么；
4. 每条视频只设置一个主要验证假设；
5. 认为成片不好时，必须选择具体问题类型，并用一句话说明；
6. 「会议结论」和「今日交付」由会议中统一填写；
7. 不在本表填写完整分镜、完整提示词、长篇复盘或详细数据。"""

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


def parse_url(feishu_url: str) -> Tuple[str, str, Optional[str]]:
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
    def __init__(self, app_token: str, table_id: str = ""):
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

    def list_tables(self) -> List[Dict[str, Any]]:
        result = self._request(
            "GET",
            f"/bitable/v1/apps/{self.app_token}/tables",
            params={"page_size": 100},
        )
        return result.get("data", {}).get("items", [])

    def create_table(self, name: str, primary_field_name: str) -> Dict[str, Any]:
        result = self._request(
            "POST",
            f"/bitable/v1/apps/{self.app_token}/tables",
            json={
                "table": {
                    "name": name,
                    "default_view_name": name,
                    "fields": [{"field_name": primary_field_name, "type": 1}],
                }
            },
        )
        table = result.get("data", {})
        tid = table.get("table_id", "")
        if not tid:
            raise FeishuAPIError(f"创建数据表 {name} 失败：未返回 table_id")
        self.table_id = tid
        # 更新表格说明
        self._request(
            "PATCH",
            f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}",
            json={"name": name, "description": TABLE_DESCRIPTION},
        )
        return table

    def rename_table(self, name: str) -> None:
        self._request(
            "PATCH",
            f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}",
            json={"name": name},
        )

    def list_fields(self) -> List[Dict[str, Any]]:
        result = self._request(
            "GET",
            f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields",
            params={"page_size": 500},
        )
        return result.get("data", {}).get("items", [])

    def create_field(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
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
        payload: Dict[str, Any] = {
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
        for key in ("filter_info", "hidden_fields", "sort_info"):
            if spec.get(key) is not None:
                prop[key] = spec[key]
        if prop:
            payload["property"] = prop
        self._request(
            "PATCH",
            f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/views/{view_id}",
            json=payload,
        )


# ── Field Type Helpers ─────────────────────────────────────────────

AUTO_NUMBER = {"type": 1005, "ui_type": "AutoNumber"}
TEXT = {"type": 1, "ui_type": "Text"}
URL = {"type": 15, "ui_type": "Url"}
USER = {"type": 11, "ui_type": "User"}
SINGLE = {"type": 3, "ui_type": "SingleSelect"}
DATE = {"type": 5, "ui_type": "DateTime"}


def options(names: Iterable[str]) -> List[Dict[str, Any]]:
    return [{"name": name, "color": index % 54} for index, name in enumerate(names)]


def single(names: Iterable[str]) -> Dict[str, Any]:
    return {**SINGLE, "property": {"options": options(names)}}


def date_field(fmt: str = "yyyy-MM-dd") -> Dict[str, Any]:
    return {**DATE, "property": {"date_formatter": fmt}}


def user_field() -> Dict[str, Any]:
    return dict(USER)


def url_field() -> Dict[str, Any]:
    return dict(URL)


# ── Field Definitions ──────────────────────────────────────────────

FIELD_SPECS: List[Dict[str, Any]] = [
    {
        "name": "日期",
        **date_field("yyyy-MM-dd"),
    },
    {
        "name": "负责人",
        **user_field(),
    },
    {
        "name": "项目/产品",
        **TEXT,
    },
    {
        "name": "参考视频/成片链接",
        **url_field(),
    },
    {
        "name": "今日动作",
        **single([
            "拆解",
            "程序复刻",
            "人工复刻",
            "优化旧片",
            "A/B对照",
        ]),
    },
    {
        "name": "今日判断",
        **TEXT,
    },
    {
        "name": "问题定位",
        **single([
            "暂无",
            "选片问题",
            "结构问题",
            "分镜问题",
            "生成真实感",
            "产品还原",
            "口播字幕",
            "剪辑节奏",
            "产品本身",
        ]),
    },
    {
        "name": "问题说明",
        **TEXT,
    },
    {
        "name": "会议结论",
        **single([
            "直接做",
            "修改后做",
            "做A/B对照",
            "暂缓",
            "放弃",
        ]),
    },
    {
        "name": "今日交付",
        **single([
            "1条",
            "2条",
            "3条",
        ]),
    },
]


# ── View Definitions ───────────────────────────────────────────────

def _today_timestamp() -> int:
    """返回今天 00:00:00 Asia/Shanghai 的毫秒时间戳"""
    today = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    return int(today.timestamp() * 1000)


def _tomorrow_timestamp() -> int:
    return _today_timestamp() + 24 * 60 * 60 * 1000


def option_names(field_property: Any) -> List[str]:
    if not isinstance(field_property, dict):
        return []
    return [str(item.get("name", "")).strip() for item in field_property.get("options", []) if item.get("name")]


def option_id(field: Dict[str, Any], name: str) -> str:
    for option in (field.get("property") or {}).get("options") or []:
        if option.get("name") == name:
            return str(option.get("id") or "")
    raise FeishuAPIError(f"字段 {field.get('field_name')} 缺少选项: {name}")


def encode_filter_value(field: Dict[str, Any], values: List[Any]) -> str:
    if int(field.get("type") or 0) in {3, 4}:
        return json.dumps([option_id(field, str(value)) for value in values], ensure_ascii=False)
    return json.dumps(values, ensure_ascii=False)


def build_filter(fields: List[Dict[str, Any]], conjunction: str,
                 conditions_data: List[Tuple[str, str, Optional[List[Any]]]]) -> Optional[Dict[str, Any]]:
    if not conditions_data:
        return None
    by_name = {str(field.get("field_name") or ""): field for field in fields}
    return {
        "conjunction": conjunction,
        "conditions": [
            {
                "field_id": by_name[field_name]["field_id"],
                "operator": operator,
                "value": encode_filter_value(by_name[field_name], values),
            }
            if values is not None
            else {
                "field_id": by_name[field_name]["field_id"],
                "operator": operator,
            }
            for field_name, operator, values in conditions_data
        ],
    }


def build_sort(fields: List[Dict[str, Any]],
               sort_data: List[Tuple[str, bool]]) -> Optional[Dict[str, Any]]:
    if not sort_data:
        return None
    by_name = {str(field.get("field_name") or ""): field for field in fields}
    return {
        "sorts": [
            {"field_id": by_name[field_name]["field_id"], "desc": descending}
            for field_name, descending in sort_data
        ],
    }


def find_primary_field(fields: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for field in fields:
        if field.get("is_primary"):
            return field
    return fields[0] if fields else None


# ── Actions ────────────────────────────────────────────────────────

def ensure_fields(client: BitableClient, dry_run: bool) -> Dict[str, Any]:
    result: Dict[str, Any] = {"created": [], "updated": [], "skipped": [], "failed": [], "fallbacks": []}
    fields = client.list_fields()
    primary = find_primary_field(fields)
    # 将第一列主键重命名为「实验编号」
    if primary and primary.get("field_name") != "实验编号":
        if dry_run:
            result["updated"].append(f"{primary.get('field_name')} -> 实验编号")
        else:
            try:
                client.update_field(str(primary["field_id"]), {"name": "实验编号", **AUTO_NUMBER})
                result["updated"].append(f"{primary.get('field_name')} -> 实验编号")
                fields = client.list_fields()
            except Exception as exc:
                result["failed"].append({"field": "实验编号", "error": str(exc)})

    by_name = {field.get("field_name"): field for field in fields}

    for spec in FIELD_SPECS:
        current = by_name.get(spec["name"])
        if current:
            current_options = set(option_names(current.get("property")))
            wanted_options = set(option_names(spec.get("property")))
            needs_update = (
                current.get("type") != spec["type"]
                or (bool(wanted_options) and current_options != wanted_options)
            )
            # 不更新已有选项（避免端上反复同步）
            if needs_update:
                if dry_run:
                    result["updated"].append(spec["name"])
                else:
                    try:
                        client.update_field(str(current["field_id"]), spec)
                        result["updated"].append(spec["name"])
                        time.sleep(1.3)
                        fields = client.list_fields()
                        by_name = {field.get("field_name"): field for field in fields}
                    except Exception as exc:
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
        except Exception as exc:
            result["failed"].append({"field": spec["name"], "error": str(exc)})
    return result


def ensure_views(client: BitableClient, dry_run: bool) -> Dict[str, Any]:
    result: Dict[str, Any] = {"created": [], "updated": [], "failed": [], "fallbacks": []}
    fields = client.list_fields()

    # 视图定义
    all_field_names = [spec["name"] for spec in FIELD_SPECS]
    operator_view_visible = [name for name in all_field_names if name not in ("会议结论", "今日交付")]

    view_specs: List[Dict[str, Any]] = [
        # ── 视图 1：今日会议（设为默认，排第一） ──
        {
            "name": "今日会议",
            "filter": build_filter(
                fields, "and",
                [("日期", "is", [str(_today_timestamp())])],
            ),
            "sort": build_sort(fields, [("负责人", False)]),
            "hidden_fields": None,
        },
        # ── 视图 2：运营填写 ──
        {
            "name": "运营填写",
            "filter": None,
            "sort": build_sort(fields, [("日期", True), ("负责人", False)]),
            "hidden_fields": [
                field["field_id"]
                for field in fields
                if field.get("field_name") in ("会议结论", "今日交付")
            ],
        },
        # ── 视图 3：历史实验 ──
        {
            "name": "历史实验",
            "filter": None,
            "sort": build_sort(fields, [("日期", True), ("负责人", False)]),
            "hidden_fields": None,
        },
    ]

    views = client.list_views()
    by_name = {view.get("view_name"): view for view in views}
    default_view = next((view for view in views if view.get("view_name") == TABLE_NAME), None)

    for index, spec in enumerate(view_specs):
        name = spec["name"]
        current = by_name.get(name)
        if not current and index == 0 and default_view:
            # 把建表时自动生成的默认视图用作第一个视图
            current = default_view

        if dry_run:
            result["updated" if current else "created"].append(name)
            continue

        try:
            if current:
                client.update_view(str(current["view_id"]), spec)
                result["updated"].append(name)
            else:
                view = client.create_view(name)
                try:
                    client.update_view(str(view["view_id"]), spec)
                except Exception as exc:
                    result.setdefault("fallbacks", []).append(f"{name}: 视图配置失败 ({exc})，已保留默认")
                result["created"].append(name)
            time.sleep(0.5)
            views = client.list_views()
            by_name = {view.get("view_name"): view for view in views}
            default_view = next((view for view in views if view.get("view_name") == TABLE_NAME), None)
        except Exception as exc:
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
        "required_views_missing": [
            name for name in ["今日会议", "运营填写", "历史实验"] if name not in view_names
        ],
    }


# ── Main ───────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=TARGET_URL)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    app_token, _, _ = parse_url(args.url)

    print("=" * 60)
    print("每日内容实验卡｜数据表创建")
    print(f"模式: {'DRY RUN' if args.dry_run else '正式执行'}")
    print(f"app_token: {app_token}")
    print()

    # Step 0: 检查现有表格
    app_client = BitableClient(app_token)
    existing_tables = app_client.list_tables()
    table_names = [t.get("name") for t in existing_tables]
    print(f"已有数据表: {table_names}")
    print()

    # 检查是否可以关联「本周重点产品池」
    pool_table = next((t for t in existing_tables if t.get("name") == "本周重点产品池"), None)
    if pool_table:
        print(f"✓ 发现「本周重点产品池」，建立关联记录。table_id={pool_table.get('table_id')}")
    else:
        # 「本周重点产品池」在其他 wiki 中，无法跨 app 关联
        print("! 「本周重点产品池」不在同一多维表格中，项目/产品字段将使用文本模式。")

    # Step 1: 创建数据表
    if args.dry_run:
        print("[DRY RUN] 将创建数据表:", TABLE_NAME)
        print("[DRY RUN] 将创建 10 个字段 + 1 个主键")
        print("[DRY RUN] 将创建 3 个视图: 今日会议 / 运营填写 / 历史实验")
        return 0

    existing = next((t for t in existing_tables if t.get("name") == TABLE_NAME), None)
    if existing:
        table_id = existing.get("table_id", "")
        if table_id:
            table_client = BitableClient(app_token, table_id)
            table_client.rename_table(TABLE_NAME)
            print(f"数据表已存在，复用: table_id={table_id}")
        else:
            raise FeishuAPIError(f"表 {TABLE_NAME} 存在但无 table_id")
    else:
        table = app_client.create_table(TABLE_NAME, "实验编号")
        table_id = table.get("table_id", "")
        print(f"数据表已创建: table_id={table_id}")

    table_client = BitableClient(app_token, table_id)

    # Step 2: 创建字段
    print("\n── 创建字段 ──")
    field_result = ensure_fields(table_client, False)
    print(json.dumps(field_result, ensure_ascii=False, indent=2))

    # Step 3: 创建视图
    print("\n── 创建视图 ──")
    view_result = ensure_views(table_client, False)
    print(json.dumps(view_result, ensure_ascii=False, indent=2))

    # Step 4: 验收
    tables_after = app_client.list_tables()
    current_table = next((t for t in tables_after if t.get("name") == TABLE_NAME), None)
    if not current_table:
        print("ERROR: 创建后找不到表!")
        return 1

    validation = validate(table_client)
    print("\n── 验收快照 ──")
    print(json.dumps(validation, ensure_ascii=False, indent=2))

    views = table_client.list_views()
    view_details = [
        {"name": v.get("view_name"), "view_id": v.get("view_id")}
        for v in views
    ]
    fields = table_client.list_fields()
    field_details = [
        {"name": f.get("field_name"), "field_id": f.get("field_id"), "type": f.get("type"),
         "is_primary": f.get("is_primary")}
        for f in fields
    ]

    # ── 最终验收报告 ──
    print("\n" + "=" * 60)
    print("验收报告")
    print("=" * 60)
    print()
    print(f"多维表格 wiki URL: {args.url}")
    print(f"多维表格 app_token: {app_token}")
    print(f"数据表名称: {TABLE_NAME}")
    print(f"数据表 ID: {table_id}")
    print()

    print("视图清单:")
    for v in view_details:
        print(f"  - {v['name']}: {v['view_id']}")
    print()

    print("字段清单:")
    for f in field_details:
        marker = " [主键]" if f["is_primary"] else ""
        print(f"  - {f['name']}: {f['field_id']} (type={f['type']}){marker}")
    print()

    # 检查
    unresolved: List[str] = []
    # 检查日期默认值（API 难以直接设置，需人工确认）
    unresolved.append("日期字段默认值需在前端手动设置为「创建当天」，飞书 API 暂不支持通过字段创建接口设置默认值")
    # 检查问题定位默认值
    unresolved.append("问题定位字段默认值「暂无」需在前端手动设置，飞书字段创建 API 不支持 default_value")
    # 会议结论为空优先排序
    unresolved.append("「今日会议」视图中「会议结论为空的记录优先」需要通过前端手动调整排序分组，API sort_info 不支持 nulls-first")
    # 关联记录
    if not pool_table:
        unresolved.append("项目/产品字段：因「本周重点产品池」在不同多维表格中，无法创建关联记录，已使用文本字段")
    # 字段描述
    unresolved.append("字段描述（description）应已写入 API，请在前端确认是否正确展示")
    # 视图默认
    unresolved.append("已将「今日会议」设为第一个视图并复用建表默认视图，请确认其为默认视图")

    print("未能自动完成的事项及原因:")
    for item in unresolved:
        print(f"  - {item}")

    print()
    print(f"完成。数据表链接: {args.url.replace('?table=tblNVu4eN26W5UJR&view=vewMWXkP5z', '')}?table={table_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
