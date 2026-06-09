from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


WORKSPACE = Path("/Users/likeu3/.openclaw/workspace")
BITABLE_PATH = WORKSPACE / "skills" / "script-run-manager-sync"
if str(BITABLE_PATH) not in sys.path:
    sys.path.insert(0, str(BITABLE_PATH))

from core.bitable import FeishuBitableClient  # type: ignore  # noqa: E402


@dataclass(frozen=True)
class FeishuTable:
    app_token: str
    table_id: str


TABLES: Dict[str, FeishuTable] = {
    "商品内容任务表": FeishuTable(app_token="X7s0bxMAsacG6FsZyFuc725On5e", table_id="tblIy2XkKc2144Pm"),
    "商品锚点卡确认队列": FeishuTable(app_token="IHhGbKws0akToRsmEfRc2vPvnsb", table_id="tbl2QRHwF7g9CmaF"),
    "商品素材上传表": FeishuTable(app_token="TAHCb5acva0osEsP8bOcWabIndd", table_id="tblBj3UCaBRicSKS"),
    "人工复核队列表": FeishuTable(app_token="RZ0Mbf7lxaSkr3sstQHc62Whn0b", table_id="tblEqdnodcuJsrut"),
    "成片质检表": FeishuTable(app_token="B7Nbb4khnakuIGsTlRacjMi9nzf", table_id="tbl43RnlocUGin5a"),
}


class AutoMixcutFeishuClient:
    def __init__(self, table_name: str):
        table = TABLES[table_name]
        self.client = FeishuBitableClient(app_token=table.app_token, table_id=table.table_id)

    def create_record(self, fields: Dict[str, Any]) -> str:
        url = (
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
            f"{self.client.app_token}/tables/{self.client.table_id}/records"
        )
        response = self.client._request("POST", url, headers=self.client._headers(), json={"fields": compact_fields(fields)})
        result = response.json()
        if result.get("code") != 0:
            raise RuntimeError(f"创建飞书记录失败: {result.get('msg')}")
        record_id = result.get("data", {}).get("record", {}).get("record_id") or result.get("data", {}).get("record_id")
        if not record_id:
            raise RuntimeError(f"飞书未返回 record_id: {result}")
        return record_id

    def update_record(self, record_id: str, fields: Dict[str, Any]) -> None:
        self.client.update_record_fields(record_id, compact_fields(fields))

    def get_record(self, record_id: str) -> Dict[str, Any]:
        url = (
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
            f"{self.client.app_token}/tables/{self.client.table_id}/records/{record_id}"
        )
        response = self.client._request("GET", url, headers=self.client._headers())
        result = response.json()
        if result.get("code") != 0:
            raise RuntimeError(f"读取飞书记录失败: {result.get('msg')}")
        record = result.get("data", {}).get("record") or {}
        return record.get("fields") or {}

    def list_records(self, limit: Optional[int] = None) -> List[Any]:
        return self.client.list_records(limit=limit)

    def download_attachment_bytes(self, attachment: Dict[str, Any]):
        return self.client.download_attachment_bytes(attachment)


def compact_fields(fields: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in fields.items() if value not in (None, "", [], {})}


def url_cell(url: Optional[str], text: str) -> Optional[Dict[str, str]]:
    if not url:
        return None
    if not url.startswith(("http://", "https://")):
        return None
    return {"link": url, "text": text, "type": "url"}


def datetime_cell(value: Any | None) -> Optional[int]:
    if not value:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return int(dt.timestamp() * 1000)
