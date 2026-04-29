#!/usr/bin/env python3
"""更新 Accio 群配置，并探测机器人自动 @ 所需权限。"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import requests


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.feishu import FeishuAPIError, FeishuIMClient, TableRecord  # noqa: E402
from app.pipeline import CONFIG_FIELDS, _build_bitable_client, _list_table_records, _record_fields  # noqa: E402
from app.config import get_settings  # noqa: E402
from app.utils import safe_text  # noqa: E402


def _find_config_record(
    records: list[TableRecord],
    config_id: str,
    country: str,
    category: str,
) -> Optional[TableRecord]:
    for record in records:
        fields = _record_fields(record)
        current_config_id = safe_text(fields.get(CONFIG_FIELDS["config_id"]))
        current_country = safe_text(fields.get(CONFIG_FIELDS["country"]))
        current_category = safe_text(fields.get(CONFIG_FIELDS["category"]))
        if config_id and current_config_id == config_id:
            return record
        if not config_id and current_country == country and current_category == category:
            return record
    return None


def _probe_chat_members(client: FeishuIMClient, chat_id: str, bot_name: str) -> Dict[str, Any]:
    url = "{base}/im/v1/chats/{chat_id}/members".format(base=client.BASE_URL, chat_id=chat_id)
    try:
        response = requests.get(url, headers=client._headers(), params={"page_size": 50}, timeout=30)
        payload = response.json()
    except Exception as exc:  # pragma: no cover - network/runtime path
        return {"ok": False, "error": str(exc)}
    result = {
        "http_status": response.status_code,
        "ok": payload.get("code") == 0,
        "code": payload.get("code"),
        "msg": payload.get("msg"),
    }
    items = payload.get("data", {}).get("items", [])
    result["member_count"] = len(items)
    result["members"] = [
        {
            "name": safe_text(item.get("name")) or safe_text(item.get("display_name")),
            "member_id": safe_text(item.get("member_id")),
            "open_id": safe_text(item.get("member_id")) if safe_text(item.get("member_id")).startswith("ou_") else "",
            "member_type": safe_text(item.get("member_type")),
            "tenant_key": safe_text(item.get("tenant_key")),
        }
        for item in items
    ]
    matched = None
    for item in items:
        name = safe_text(item.get("name")) or safe_text(item.get("display_name"))
        if name == bot_name:
            matched = item
            break
    if matched:
        result["matched_member"] = matched
    return result


def _probe_chat_messages(client: FeishuIMClient, chat_id: str) -> Dict[str, Any]:
    url = "{base}/im/v1/messages".format(base=client.BASE_URL)
    try:
        response = requests.get(
            url,
            headers=client._headers(),
            params={
                "container_id_type": "chat",
                "container_id": chat_id,
                "sort_type": "ByCreateTimeDesc",
                "page_size": 10,
            },
            timeout=30,
        )
        payload = response.json()
    except Exception as exc:  # pragma: no cover - network/runtime path
        return {"ok": False, "error": str(exc)}
    result = {
        "http_status": response.status_code,
        "ok": payload.get("code") == 0,
        "code": payload.get("code"),
        "msg": payload.get("msg"),
    }
    result["items"] = payload.get("data", {}).get("items", [])[:5]
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="配置 Accio 群并探测权限")
    parser.add_argument("--chat-id", required=True, help="Accio 目标群 chat_id")
    parser.add_argument("--country", default="VN", help="配置表中的国家")
    parser.add_argument("--category", default="时尚配件", help="配置表中的类目")
    parser.add_argument("--config-id", default="", help="直接按 config_id 匹配")
    parser.add_argument("--bot-name", default="ACCIO选品专员", help="群内机器人显示名")
    args = parser.parse_args()

    settings = get_settings()
    if not settings.config_table_url:
        raise RuntimeError("未配置参数配置表 URL")

    config_client = _build_bitable_client(settings.config_table_url)
    records = _list_table_records(config_client, settings.feishu_read_page_size)
    target = _find_config_record(records, args.config_id.strip(), args.country.strip(), args.category.strip())
    if not target:
        raise RuntimeError("未找到匹配配置: config_id={config_id}, country={country}, category={category}".format(
            config_id=args.config_id.strip() or "-",
            country=args.country.strip(),
            category=args.category.strip(),
        ))

    config_client.update_record_fields(
        target.record_id,
        {
            CONFIG_FIELDS["accio_chat_id"]: args.chat_id.strip(),
        },
    )

    im_client = FeishuIMClient()
    member_probe = _probe_chat_members(im_client, args.chat_id.strip(), args.bot_name.strip())
    message_probe = _probe_chat_messages(im_client, args.chat_id.strip())
    print(
        json.dumps(
            {
                "updated_record_id": target.record_id,
                "chat_id": args.chat_id.strip(),
                "bot_name": args.bot_name.strip(),
                "member_probe": member_probe,
                "message_probe": message_probe,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    try:
        main()
    except FeishuAPIError as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        raise
