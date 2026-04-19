#!/usr/bin/env python3
"""
飞书多维表格读写、附件下载与字段映射。
"""

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


FIELD_ALIASES = {
    "status": ["任务状态", "状态", "执行状态", "处理状态"],
    "task_no": ["任务编号", "任务ID", "任务序号", "编号"],
    "product_images": ["产品图片", "商品图片", "图片", "主图", "商品主图"],
    "product_code": ["产品编码", "商品编码", "SKU编码", "SKU", "产品Code", "Product Code"],
    "product_id": ["商品ID", "商品Id", "产品ID", "GeeLark商品ID", "GeeLark Product ID", "产品编码", "商品编码", "SKU编码", "SKU", "产品Code", "Product Code"],
    "parent_slot_1": ["所属母版1"],
    "parent_slot_2": ["所属母版2"],
    "parent_slot_3": ["所属母版3"],
    "parent_slot_4": ["所属母版4"],
    "top_category": ["一级类目", "大类目", "一级品类", "主类目"],
    "target_country": ["目标国家", "国家", "投放国家"],
    "target_language": ["目标语言", "语言"],
    "product_type": ["产品类型", "商品类型", "品类", "产品品类"],
    "product_selling_note": ["产品卖点说明"],
    "input_hash": ["输入哈希"],
    "last_run_at": ["最近执行时间", "最近运行时间"],
    "error_message": ["错误信息", "失败原因", "报错信息"],
    "execution_log": ["执行日志", "运行日志"],
    "stage_durations": ["阶段耗时", "耗时统计"],
    "anchor_card_json": ["锚点卡_JSON", "锚点卡JSON"],
    "opening_strategy_json": ["首镜策略_JSON", "首镜策略JSON", "首镜吸引策略_JSON", "首镜吸引策略JSON"],
    "styling_plan_json": ["穿搭方案_JSON", "穿搭方案JSON", "人物穿搭情绪包_JSON", "人物穿搭情绪强化包_JSON", "人物_穿搭_情绪强化包_JSON"],
    "three_strategies_json": ["三套策略_JSON", "三套策略JSON", "四套策略_JSON", "四套策略JSON"],
    "final_s1_json": ["Final_S1_JSON"],
    "final_s2_json": ["Final_S2_JSON"],
    "final_s3_json": ["Final_S3_JSON"],
    "final_s4_json": ["Final_S4_JSON"],
    "exp_s1_json": ["EXP_S1_JSON"],
    "exp_s2_json": ["EXP_S2_JSON"],
    "exp_s3_json": ["EXP_S3_JSON"],
    "exp_s4_json": ["EXP_S4_JSON"],
    "script_s1_json": ["脚本_S1_JSON", "脚本S1_JSON", "脚本1_JSON", "脚本1原始JSON"],
    "script_s2_json": ["脚本_S2_JSON", "脚本S2_JSON", "脚本2_JSON", "脚本2原始JSON"],
    "script_s3_json": ["脚本_S3_JSON", "脚本S3_JSON", "脚本3_JSON", "脚本3原始JSON"],
    "script_s4_json": ["脚本_S4_JSON", "脚本S4_JSON", "脚本4_JSON", "脚本4原始JSON"],
    "review_s1_json": ["脚本_S1_质检_JSON", "脚本S1质检_JSON", "脚本1质检_JSON"],
    "review_s2_json": ["脚本_S2_质检_JSON", "脚本S2质检_JSON", "脚本2质检_JSON"],
    "review_s3_json": ["脚本_S3_质检_JSON", "脚本S3质检_JSON", "脚本3质检_JSON"],
    "review_s4_json": ["脚本_S4_质检_JSON", "脚本S4质检_JSON", "脚本4质检_JSON"],
    "script_s1": ["脚本_S1", "脚本S1", "脚本方向一"],
    "script_s2": ["脚本_S2", "脚本S2", "脚本方向二"],
    "script_s3": ["脚本_S3", "脚本S3", "脚本方向三"],
    "script_s4": ["脚本_S4", "脚本S4", "脚本方向四"],
    "video_prompt_s1_json": ["视频提示词_S1_JSON", "最终视频提示词_S1_JSON", "视频S1_JSON"],
    "video_prompt_s2_json": ["视频提示词_S2_JSON", "最终视频提示词_S2_JSON", "视频S2_JSON"],
    "video_prompt_s3_json": ["视频提示词_S3_JSON", "最终视频提示词_S3_JSON", "视频S3_JSON"],
    "video_prompt_s4_json": ["视频提示词_S4_JSON", "最终视频提示词_S4_JSON", "视频S4_JSON"],
    "video_prompt_s1": ["视频提示词_S1", "最终视频提示词_S1", "视频S1"],
    "video_prompt_s2": ["视频提示词_S2", "最终视频提示词_S2", "视频S2"],
    "video_prompt_s3": ["视频提示词_S3", "最终视频提示词_S3", "视频S3"],
    "video_prompt_s4": ["视频提示词_S4", "最终视频提示词_S4", "视频S4"],
    "variant_s1_json": ["变体_S1_JSON", "变体S1_JSON", "脚本1变体_JSON", "脚本1变体JSON"],
    "variant_s2_json": ["变体_S2_JSON", "变体S2_JSON", "脚本2变体_JSON", "脚本2变体JSON"],
    "variant_s3_json": ["变体_S3_JSON", "变体S3_JSON", "脚本3变体_JSON", "脚本3变体JSON"],
    "variant_s4_json": ["变体_S4_JSON", "变体S4_JSON", "脚本4变体_JSON", "脚本4变体JSON"],
    "script_1_variant_1": ["脚本1变体1", "脚本1 变体1", "脚本1-变体1"],
    "script_1_variant_2": ["脚本1变体2", "脚本1 变体2", "脚本1-变体2"],
    "script_1_variant_3": ["脚本1变体3", "脚本1 变体3", "脚本1-变体3"],
    "script_1_variant_4": ["脚本1变体4", "脚本1 变体4", "脚本1-变体4"],
    "script_1_variant_5": ["脚本1变体5", "脚本1 变体5", "脚本1-变体5"],
    "script_2_variant_1": ["脚本2变体1", "脚本2 变体1", "脚本2-变体1"],
    "script_2_variant_2": ["脚本2变体2", "脚本2 变体2", "脚本2-变体2"],
    "script_2_variant_3": ["脚本2变体3", "脚本2 变体3", "脚本2-变体3"],
    "script_2_variant_4": ["脚本2变体4", "脚本2 变体4", "脚本2-变体4"],
    "script_2_variant_5": ["脚本2变体5", "脚本2 变体5", "脚本2-变体5"],
    "script_3_variant_1": ["脚本3变体1", "脚本3 变体1", "脚本3-变体1"],
    "script_3_variant_2": ["脚本3变体2", "脚本3 变体2", "脚本3-变体2"],
    "script_3_variant_3": ["脚本3变体3", "脚本3 变体3", "脚本3-变体3"],
    "script_3_variant_4": ["脚本3变体4", "脚本3 变体4", "脚本3-变体4"],
    "script_3_variant_5": ["脚本3变体5", "脚本3 变体5", "脚本3-变体5"],
    "script_4_variant_1": ["脚本4变体1", "脚本4 变体1", "脚本4-变体1"],
    "script_4_variant_2": ["脚本4变体2", "脚本4 变体2", "脚本4-变体2"],
    "script_4_variant_3": ["脚本4变体3", "脚本4 变体3", "脚本4-变体3"],
    "script_4_variant_4": ["脚本4变体4", "脚本4 变体4", "脚本4-变体4"],
    "script_4_variant_5": ["脚本4变体5", "脚本4 变体5", "脚本4-变体5"],
    "output_summary": ["输出摘要", "摘要"],
}


class FeishuAPIError(Exception):
    """飞书 API 异常。"""


@dataclass
class TaskRecord:
    record_id: str
    fields: Dict[str, Any]


def _load_openclaw_config() -> Dict[str, Any]:
    config_file = Path.home() / ".openclaw" / "openclaw.json"
    with open(config_file, "r", encoding="utf-8") as handle:
        return json.load(handle)


def get_tenant_access_token() -> str:
    config = _load_openclaw_config()
    app_id = config["channels"]["feishu"]["appId"]
    app_secret = config["channels"]["feishu"]["appSecret"]

    last_error: Optional[Exception] = None
    for attempt in range(4):
        try:
            response = requests.post(
                "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
                json={"app_id": app_id, "app_secret": app_secret},
                timeout=30,
            )
            result = response.json()
            if result.get("code") != 0:
                raise FeishuAPIError(f"获取飞书 access_token 失败: {result.get('msg')}")
            return result["tenant_access_token"]
        except (requests.exceptions.RequestException, ValueError, FeishuAPIError) as exc:
            last_error = exc
            if attempt < 3:
                wait_time = 2 ** attempt
                print(f"    ⚠️ 获取飞书 access_token 失败，{wait_time} 秒后重试...")
                time.sleep(wait_time)
    raise FeishuAPIError(f"获取飞书 access_token 最终失败: {last_error}")


def resolve_wiki_bitable_app_token(wiki_token: str) -> str:
    last_error: Optional[Exception] = None
    for attempt in range(4):
        try:
            headers = {"Authorization": f"Bearer {get_tenant_access_token()}"}
            response = requests.get(
                "https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node",
                headers=headers,
                params={"token": wiki_token},
                timeout=30,
            )
            result = response.json()
            if result.get("code") != 0:
                raise FeishuAPIError(f"解析 wiki token 失败: {result.get('msg')}")

            node = result.get("data", {}).get("node", {})
            if node.get("obj_type") != "bitable":
                raise FeishuAPIError(f"当前 wiki 节点不是 bitable: {node.get('obj_type')}")

            obj_token = node.get("obj_token")
            if not obj_token:
                raise FeishuAPIError("wiki 节点未返回底层 bitable obj_token")
            return obj_token
        except (requests.exceptions.RequestException, ValueError, FeishuAPIError) as exc:
            last_error = exc
            if attempt < 3:
                wait_time = 2 ** attempt
                print(f"    ⚠️ 解析 wiki token 失败，{wait_time} 秒后重试...")
                time.sleep(wait_time)
    raise FeishuAPIError(f"解析 wiki token 最终失败: {last_error}")


class FeishuBitableClient:
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

    @staticmethod
    def _looks_like_invalid_access_token(response: requests.Response) -> bool:
        try:
            result = response.json()
        except ValueError:
            return False

        if result.get("code") == 0:
            return False
        message = str(result.get("msg", "") or "")
        code = str(result.get("code", "") or "")
        lowered = message.lower()
        return (
            "invalid access token" in lowered
            or "invalid tenant access token" in lowered
            or code == "99991663"
        )

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        last_error: Optional[Exception] = None
        for attempt in range(5):
            try:
                response = requests.request(method, url, timeout=30, **kwargs)
                if self._looks_like_invalid_access_token(response):
                    self.access_token = None
                    self.token_expires_at = 0
                    if attempt < 4:
                        refreshed_kwargs = dict(kwargs)
                        headers = dict(refreshed_kwargs.get("headers") or {})
                        headers["Authorization"] = f"Bearer {self._get_access_token()}"
                        refreshed_kwargs["headers"] = headers
                        kwargs = refreshed_kwargs
                        wait_time = 1
                        print("    ⚠️ 飞书 access token 失效，自动刷新后重试...")
                        time.sleep(wait_time)
                        continue
                    raise FeishuAPIError("飞书 access token 已失效，刷新后仍不可用")
                if response.status_code >= 500:
                    raise FeishuAPIError(f"飞书服务异常: {response.status_code} - {response.text[:300]}")
                return response
            except (requests.exceptions.RequestException, FeishuAPIError) as exc:
                last_error = exc
                if attempt < 4:
                    wait_time = 2 ** attempt
                    print(f"    ⚠️ 飞书 API 请求异常，{wait_time} 秒后重试...")
                    time.sleep(wait_time)
        raise FeishuAPIError(f"飞书 API 请求失败: {last_error}")

    def list_field_names(self) -> List[str]:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields"
        response = self._request("GET", url, headers=self._headers(), params={"page_size": 500})
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"获取字段定义失败: {result.get('msg')}")
        return [item["field_name"] for item in result.get("data", {}).get("items", []) if item.get("field_name")]

    def list_records(self, page_size: int = 100) -> List[TaskRecord]:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
        page_token = None
        has_more = True
        records: List[TaskRecord] = []

        while has_more:
            params: Dict[str, Any] = {"page_size": page_size}
            if page_token:
                params["page_token"] = page_token
            response = self._request("GET", url, headers=self._headers(), params=params)
            result = response.json()
            if result.get("code") != 0:
                raise FeishuAPIError(f"读取记录失败: {result.get('msg')}")

            data = result.get("data", {})
            for item in data.get("items", []):
                records.append(TaskRecord(record_id=item["record_id"], fields=item.get("fields", {})))

            has_more = data.get("has_more", False)
            page_token = data.get("page_token")

        return records

    def update_record_fields(self, record_id: str, fields: Dict[str, Any]) -> None:
        if not fields:
            return
        url = (
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
            f"{self.app_token}/tables/{self.table_id}/records/{record_id}"
        )
        response = self._request("PUT", url, headers=self._headers(), json={"fields": fields})
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"更新记录失败: {result.get('msg')}")

    def get_tmp_download_url(self, file_token: str) -> str:
        url = "https://open.feishu.cn/open-apis/drive/v1/medias/batch_get_tmp_download_url"
        response = self._request(
            "GET",
            url,
            headers=self._headers(),
            params={"file_tokens": file_token},
        )
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"获取临时下载链接失败: {result.get('msg')}")
        urls = result.get("data", {}).get("tmp_download_urls", [])
        if not urls:
            raise FeishuAPIError("飞书未返回临时下载链接")
        return urls[0]["tmp_download_url"]

    def download_attachment(self, attachment: Dict[str, Any], output_dir: Path) -> Path:
        file_token = attachment.get("file_token")
        if not file_token:
            raise FeishuAPIError("附件缺少 file_token")

        tmp_url = self.get_tmp_download_url(file_token)
        last_error: Optional[Exception] = None
        response = None
        for attempt in range(4):
            try:
                response = requests.get(tmp_url, timeout=60)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as exc:
                last_error = exc
                if attempt < 3:
                    wait_time = 2 ** attempt
                    print(f"    ⚠️ 附件下载异常，{wait_time} 秒后重试...")
                    time.sleep(wait_time)
                    continue
                raise FeishuAPIError(f"附件下载失败: {last_error}")
        if response is None:
            raise FeishuAPIError(f"附件下载失败: {last_error}")

        output_dir.mkdir(parents=True, exist_ok=True)
        file_name = attachment.get("name") or f"{file_token}.bin"
        target = output_dir / file_name
        with open(target, "wb") as handle:
            handle.write(response.content)
        return target


def resolve_field_mapping(field_names: List[str]) -> Dict[str, Optional[str]]:
    mapping: Dict[str, Optional[str]] = {}
    for logical_name, aliases in FIELD_ALIASES.items():
        mapping[logical_name] = next((alias for alias in aliases if alias in field_names), None)
    return mapping


def normalize_cell_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        for key in ("text", "name", "url", "link"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        texts = [normalize_cell_value(item) for item in value]
        return " / ".join(item for item in texts if item)
    return str(value)


def extract_attachments(raw_value: Any) -> List[Dict[str, Any]]:
    attachments: List[Dict[str, Any]] = []
    if isinstance(raw_value, list):
        for item in raw_value:
            if isinstance(item, dict) and item.get("file_token"):
                attachments.append(item)
    elif isinstance(raw_value, dict) and raw_value.get("file_token"):
        attachments.append(raw_value)
    return attachments


def build_update_payload(mapping: Dict[str, Optional[str]], values: Dict[str, Any]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    for logical_name, value in values.items():
        field_name = mapping.get(logical_name)
        if field_name and value is not None:
            payload[field_name] = value
    return payload
