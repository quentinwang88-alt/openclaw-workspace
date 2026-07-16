from __future__ import annotations

import importlib.util
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SKILLS_ROOT = Path(__file__).resolve().parents[3]
SHARED_BITABLE_PATH = SKILLS_ROOT / "script-run-manager-sync" / "core" / "bitable.py"
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "feishu_tables.json"


def _load_shared_bitable_module():
    module_name = "_light_tryon_shared_bitable"
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, SHARED_BITABLE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载共享飞书客户端: {SHARED_BITABLE_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_shared = _load_shared_bitable_module()
FeishuAPIError = _shared.FeishuAPIError
TableField = _shared.TableField
TableRecord = _shared.TableRecord
resolve_wiki_bitable_app_token = _shared.resolve_wiki_bitable_app_token


@dataclass(frozen=True)
class TableEndpoint:
    role: str
    url: str
    app_token: str
    table_id: str
    view_id: str


def parse_feishu_url(role: str, url: str) -> TableEndpoint:
    wiki_match = re.search(r"/wiki/([^?/#]+)", url)
    base_match = re.search(r"/base/([^?/#]+)", url)
    table_match = re.search(r"[?&]table=([^&]+)", url)
    view_match = re.search(r"[?&]view=([^&]+)", url)
    if not table_match or not (wiki_match or base_match):
        raise ValueError(f"无法解析飞书表链接: {url}")
    raw_token = (wiki_match or base_match).group(1)
    app_token = resolve_wiki_bitable_app_token(raw_token) if wiki_match else raw_token
    return TableEndpoint(
        role=role,
        url=url,
        app_token=app_token,
        table_id=table_match.group(1),
        view_id=view_match.group(1) if view_match else "",
    )


def load_feishu_config(path: str | Path | None = None) -> dict[str, Any]:
    config_path = Path(path or os.environ.get("FEISHU_LIGHT_VIDEO_CONFIG") or DEFAULT_CONFIG_PATH).expanduser().resolve()
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    payload["_config_path"] = str(config_path)
    env_enabled = os.environ.get("LIGHT_VIDEO_FEISHU_SYNC_ENABLED")
    if env_enabled is not None:
        payload["sync_enabled"] = env_enabled.strip().lower() in {"1", "true", "yes", "on"}
    env_review = os.environ.get("LIGHT_VIDEO_FEISHU_REVIEW_ENABLED")
    if env_review is not None:
        payload["review_enabled"] = env_review.strip().lower() in {"1", "true", "yes", "on"}
    env_names = {
        "persona": "FEISHU_PERSONA_URL",
        "scene": "FEISHU_SCENE_URL",
        "action": "FEISHU_ACTION_URL",
        "shot_plan": "FEISHU_SHOT_PLAN_URL",
        "styling": "FEISHU_STYLING_URL",
        "subtitle": "FEISHU_SUBTITLE_URL",
        "review": "FEISHU_VIDEO_REVIEW_URL",
        "visual_plan": "FEISHU_VISUAL_PLAN_URL",
    }
    for role, env_name in env_names.items():
        if os.environ.get(env_name):
            payload.setdefault("tables", {}).setdefault(role, {})["url"] = os.environ[env_name]
    if os.environ.get("FEISHU_PRODUCT_WORKBENCH_URL"):
        payload.setdefault("product_workbench", {})["url"] = os.environ["FEISHU_PRODUCT_WORKBENCH_URL"]
    if os.environ.get("FEISHU_ORIGINAL_SCRIPT_URL"):
        payload.setdefault("source_script", {})["url"] = os.environ["FEISHU_ORIGINAL_SCRIPT_URL"]
    if os.environ.get("FEISHU_RUN_MANAGER_URL"):
        payload.setdefault("run_manager", {})["url"] = os.environ["FEISHU_RUN_MANAGER_URL"]
    return payload


def resolve_endpoints(config: dict[str, Any]) -> dict[str, TableEndpoint]:
    endpoints: dict[str, TableEndpoint] = {}
    for role, item in (config.get("tables") or {}).items():
        url = str((item or {}).get("url") or "").strip()
        if url:
            endpoints[role] = parse_feishu_url(role, url)
    return endpoints


def resolve_source_endpoint(config: dict[str, Any]) -> TableEndpoint | None:
    url = str((config.get("source_script") or {}).get("url") or "").strip()
    return parse_feishu_url("source_script", url) if url else None


def resolve_run_manager_endpoint(config: dict[str, Any]) -> TableEndpoint | None:
    url = str((config.get("run_manager") or {}).get("url") or "").strip()
    return parse_feishu_url("run_manager", url) if url else None


class LightTryonFeishuClient(_shared.FeishuBitableClient):
    def batch_delete_records(self, record_ids: list[str]) -> int:
        ids = [str(item).strip() for item in record_ids if str(item).strip()]
        deleted = 0
        for start in range(0, len(ids), 500):
            batch = ids[start:start + 500]
            url = (
                f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/"
                f"{self.table_id}/records/batch_delete"
            )
            response = self._request("POST", url, headers=self._headers(), json={"records": batch})
            result = response.json()
            if result.get("code") != 0:
                raise FeishuAPIError(f"批量删除记录失败: {result.get('msg')}")
            deleted += len(batch)
        return deleted

    def list_tables(self) -> list[dict[str, Any]]:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables"
        response = self._request("GET", url, headers=self._headers(), params={"page_size": 100})
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"获取数据表列表失败: {result.get('msg')}")
        return result.get("data", {}).get("items", [])

    def create_table(self, name: str, default_view_name: str = "表格", primary_field_name: str = "视觉方案ID") -> dict[str, Any]:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables"
        response = self._request(
            "POST",
            url,
            headers=self._headers(),
            json={
                "table": {
                    "name": name,
                    "default_view_name": default_view_name,
                    "fields": [{"field_name": primary_field_name, "type": 1}],
                }
            },
        )
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"创建数据表 {name} 失败: {result.get('msg')}")
        return dict(result.get("data", {}).get("table") or result.get("data") or {})

    def rename_table(self, name: str) -> None:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}"
        response = self._request("PATCH", url, headers=self._headers(), json={"name": name})
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"重命名数据表失败: {result.get('msg')}")

    def update_field(self, field_id: str, spec: dict[str, Any]) -> None:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields/{field_id}"
        payload = {
            "field_name": spec["name"],
            "type": int(spec["type"]),
            "ui_type": str(spec["ui_type"]),
        }
        if spec.get("property") is not None:
            payload["property"] = spec["property"]
        response = self._request("PUT", url, headers=self._headers(), json=payload)
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"更新字段 {spec['name']} 失败: {result.get('msg')}")

    def list_views(self) -> list[dict[str, Any]]:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/views"
        response = self._request("GET", url, headers=self._headers(), params={"page_size": 200})
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"获取视图失败: {result.get('msg')}")
        return result.get("data", {}).get("items", [])

    def create_view(self, name: str) -> str:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/views"
        response = self._request("POST", url, headers=self._headers(), json={"view_name": name, "view_type": "grid"})
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"创建视图 {name} 失败: {result.get('msg')}")
        return str(result.get("data", {}).get("view", {}).get("view_id") or result.get("data", {}).get("view_id") or "")

    def rename_view(self, view_id: str, name: str) -> None:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/views/{view_id}"
        response = self._request("PATCH", url, headers=self._headers(), json={"view_name": name})
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"重命名视图 {name} 失败: {result.get('msg')}")


def make_client(endpoint: TableEndpoint) -> LightTryonFeishuClient:
    return LightTryonFeishuClient(endpoint.app_token, endpoint.table_id)
