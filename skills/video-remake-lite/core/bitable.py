#!/usr/bin/env python3
"""
飞书多维表格读写与字段解析。
"""

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


FIELD_ALIASES = {
    "status": ["状态", "任务状态", "执行状态", "处理状态", "status"],
    "video": ["视频", "视频素材", "原视频", "源视频", "视频链接", "素材视频", "source_video"],
    "content_branch": ["内容分支", "content_branch"],
    "target_country": ["目标国家", "国家", "投放国家", "target_country"],
    "target_language": ["目标语言", "语言", "target_language"],
    "product_type": ["商品类型", "产品类型", "品类", "商品品类", "product_type"],
    "remake_mode": ["复刻模式", "模式", "replicate_mode", "remake_mode"],
    "opening_strategy_primary": ["开头策略主选", "主开头策略", "opening_strategy_primary"],
    "opening_strategy_backup": ["开头策略备选", "备选开头策略", "opening_strategy_backup"],
    "opening_strategy_reason": ["开头策略原因", "开头策略说明", "opening_strategy_reason"],
    "opening_strategy_risk_note": ["开头策略风险", "开头策略风险提示", "opening_strategy_risk_note"],
    "opening_style_tone": ["开头语气", "开头风格语气", "opening_style_tone"],
    "delivery_mode_primary": ["表达载体主选", "主表达载体模式", "delivery_mode_primary"],
    "delivery_mode_backup": ["表达载体备选", "备选表达载体模式", "delivery_mode_backup"],
    "delivery_mode_reason": ["表达载体原因", "表达载体说明", "delivery_mode_reason"],
    "delivery_mode_risk_note": ["表达载体风险", "表达载体风险提示", "delivery_mode_risk_note"],
    "script_breakdown": ["脚本拆解", "脚本拆解结果", "analysis_result"],
    "remake_card": ["复刻卡", "replicate_card"],
    "remade_script": ["复刻后的脚本", "localized_script"],
    "final_prompt": ["最终复刻视频提示词", "final_execution_prompt"],
    "error_message": ["错误信息", "失败原因", "报错信息", "run_log", "运行日志"],
}


@dataclass
class RemakeRecord:
    """养号复刻任务记录。"""

    record_id: str
    fields: Dict[str, Any]


def _load_openclaw_config() -> Dict[str, Any]:
    config_file = Path.home() / ".openclaw" / "openclaw.json"
    with open(config_file, "r", encoding="utf-8") as handle:
        return json.load(handle)


def get_tenant_access_token() -> str:
    """获取飞书 tenant_access_token。"""
    config = _load_openclaw_config()
    app_id = config["channels"]["feishu"]["appId"]
    app_secret = config["channels"]["feishu"]["appSecret"]

    response = requests.post(
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        json={"app_id": app_id, "app_secret": app_secret},
        timeout=30,
    )
    result = response.json()
    if result.get("code") != 0:
        raise Exception(f"获取飞书 access_token 失败: {result.get('msg')}")
    return result["tenant_access_token"]


def resolve_wiki_bitable_app_token(wiki_token: str) -> str:
    """
    将 wiki 链接中的 node token 解析成底层 bitable app token。

    读取通常可直接用 wiki token，但写入往往需要底层 obj_token。
    """
    headers = {"Authorization": f"Bearer {get_tenant_access_token()}"}
    response = requests.get(
        "https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node",
        headers=headers,
        params={"token": wiki_token},
        timeout=30,
    )
    result = response.json()
    if result.get("code") != 0:
        raise Exception(f"解析 wiki token 失败: {result.get('msg')}")

    node = result.get("data", {}).get("node", {})
    if node.get("obj_type") != "bitable":
        raise Exception(f"当前 wiki 节点不是 bitable: {node.get('obj_type')}")

    obj_token = node.get("obj_token")
    if not obj_token:
        raise Exception("wiki 节点未返回底层 bitable obj_token")
    return obj_token


class FeishuBitableClient:
    """飞书多维表格客户端。"""

    def __init__(self, app_token: str, table_id: str):
        self.app_token = app_token
        self.table_id = table_id
        self.access_token: Optional[str] = None
        self.token_expires_at: float = 0

    def _get_access_token(self) -> str:
        if self.access_token and time.time() < self.token_expires_at:
            return self.access_token

        config = _load_openclaw_config()
        self.access_token = get_tenant_access_token()
        self.token_expires_at = time.time() + config.get("feishu", {}).get("tokenExpire", 7200) - 300
        return self.access_token

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "application/json",
        }

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        对飞书 API 请求做轻量重试，避免偶发 SSL EOF / 网络抖动中断整条流水线。
        """
        last_error: Optional[Exception] = None
        for attempt in range(3):
            try:
                response = requests.request(method, url, timeout=30, **kwargs)
                return response
            except requests.exceptions.RequestException as exc:
                last_error = exc
                if attempt < 2:
                    wait_time = 2 ** attempt
                    print(f"    ⚠️ 飞书 API 请求异常，{wait_time} 秒后重试...")
                    time.sleep(wait_time)
        raise Exception(f"飞书 API 请求失败: {last_error}")

    def list_field_names(self) -> List[str]:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields"
        params = {"page_size": 500}
        response = self._request("GET", url, headers=self._headers(), params=params)
        result = response.json()
        if result.get("code") != 0:
            raise Exception(f"获取字段定义失败: {result.get('msg')}")
        items = result.get("data", {}).get("items", [])
        return [item.get("field_name", "") for item in items if item.get("field_name")]

    def list_records(self, page_size: int = 100) -> List[RemakeRecord]:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
        params = {"page_size": page_size}
        page_token = None
        has_more = True
        records: List[RemakeRecord] = []

        while has_more:
            if page_token:
                params["page_token"] = page_token
            response = self._request("GET", url, headers=self._headers(), params=params)
            result = response.json()
            if result.get("code") != 0:
                raise Exception(f"读取记录失败: {result.get('msg')}")

            data = result.get("data", {})
            for item in data.get("items", []):
                records.append(
                    RemakeRecord(
                        record_id=item["record_id"],
                        fields=item.get("fields", {}),
                    )
                )
            has_more = data.get("has_more", False)
            page_token = data.get("page_token")

        return records

    def update_record_fields(self, record_id: str, fields: Dict[str, Any]) -> bool:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/{record_id}"
        payload = {"fields": fields}
        response = self._request("PUT", url, headers=self._headers(), json=payload)
        result = response.json()
        if result.get("code") != 0:
            raise Exception(f"更新记录失败: {result.get('msg')}")
        return True

    def get_tmp_download_url(self, file_token: str) -> str:
        url = "https://open.feishu.cn/open-apis/drive/v1/medias/batch_get_tmp_download_url"
        params = {"file_tokens": file_token}
        response = self._request("GET", url, headers=self._headers(), params=params)
        result = response.json()
        if result.get("code") != 0:
            raise Exception(f"获取视频临时下载链接失败: {result.get('msg')}")
        urls = result.get("data", {}).get("tmp_download_urls", [])
        if not urls:
            raise Exception("飞书未返回临时下载链接")
        return urls[0]["tmp_download_url"]


def resolve_field_mapping(field_names: List[str]) -> Dict[str, Optional[str]]:
    """将逻辑字段映射到表内真实字段名。"""
    mapping: Dict[str, Optional[str]] = {}
    for logical_name, aliases in FIELD_ALIASES.items():
        resolved = None
        for alias in aliases:
            if alias in field_names:
                resolved = alias
                break
        mapping[logical_name] = resolved
    return mapping


def normalize_cell_value(value: Any) -> str:
    """将飞书单元格值转为可读文本。"""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        for key in ("text", "name", "url", "link"):
            cell = value.get(key)
            if isinstance(cell, str) and cell.strip():
                return cell.strip()
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        parts = []
        for item in value:
            text = normalize_cell_value(item)
            if text:
                parts.append(text)
        return " / ".join(parts)
    return str(value)


def resolve_video_url(client: FeishuBitableClient, raw_value: Any) -> str:
    """从飞书单元格中提取一个可传给模型的视频 URL。"""
    if isinstance(raw_value, list):
        for item in raw_value:
            file_token = item.get("file_token") if isinstance(item, dict) else None
            if file_token:
                return client.get_tmp_download_url(file_token)
        for item in raw_value:
            try:
                return resolve_video_url(client, item)
            except Exception:
                continue

    if isinstance(raw_value, str):
        value = raw_value.strip()
        if value.startswith("http://") or value.startswith("https://"):
            return value

    if isinstance(raw_value, dict):
        file_token = raw_value.get("file_token")
        if file_token:
            return client.get_tmp_download_url(file_token)
        for key in ("url", "link", "text"):
            value = raw_value.get(key)
            if isinstance(value, str) and value.startswith(("http://", "https://")):
                return value

    raise Exception("未能从视频字段中解析出可用的视频地址")
