#!/usr/bin/env python3
"""Reconcile local account settings with NeoBund's live account capabilities."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

from app.db import AutoPublishDB
from app.neobund_publish import NeoBundPublishAdapter


def _records(payload: Any) -> list[Dict[str, Any]]:
    values = payload.get("records", []) if isinstance(payload, dict) else []
    return [item for item in values if isinstance(item, dict)]


def _account_keys(item: Dict[str, Any]) -> set[str]:
    return {
        str(item.get(key) or "").strip().lower()
        for key in (
            "authId",
            "id",
            "username",
            "creatorUsername",
            "creatorNickname",
            "nickname",
            "remark",
        )
        if str(item.get(key) or "").strip()
    }


def _build_index(records: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for item in records:
        for key in _account_keys(item):
            index.setdefault(key, item)
    return index


def _mapped_account_keys(adapter: NeoBundPublishAdapter, account_id: str) -> set[str]:
    keys = {str(account_id or "").strip().lower()}
    mapped: Any = adapter.account_id_map.get(str(account_id or "").strip())
    if isinstance(mapped, dict):
        mapped = mapped.get("neobund_auth_id") or mapped.get("authId") or mapped.get("id")
    if str(mapped or "").strip():
        keys.add(str(mapped).strip().lower())
    return {item for item in keys if item}


def _find_account(index: Dict[str, Dict[str, Any]], keys: Iterable[str]) -> Optional[Dict[str, Any]]:
    for key in keys:
        item = index.get(str(key or "").strip().lower())
        if item is not None:
            return item
    return None


def _auth_id(item: Optional[Dict[str, Any]]) -> str:
    if not item:
        return ""
    return str(item.get("authId") or item.get("id") or "").strip()


# ---------------------------------------------------------------- CreatOK


def _creatok_connection_keys(item: Dict[str, Any]) -> set[str]:
    """连接项通常含 uid / 显示名；宽容解析避免硬猜单一字段名。"""
    keys: set[str] = set()
    for key in (
        "uid", "connection_uid", "connectionId", "id",
        "account_id", "accountId",
        "username", "display_name", "displayName", "name",
        "account_name", "accountName", "nickname",
    ):
        value = str(item.get(key) or "").strip()
        if value:
            keys.add(value.lower())
    return keys


def _creatok_capability_names(item: Dict[str, Any]) -> set[str]:
    raw = item.get("capabilities") or item.get("capability") or item.get("available_capabilities") or []
    names: set[str] = set()
    if isinstance(raw, str):
        raw = [raw]
    if isinstance(raw, dict):
        raw = list(raw.keys())
    for name in raw:
        text = str(name or "").strip().lower()
        if text:
            names.add(text)
    for key, flag in (
        ("publish_content_video", "content_video_capable"),
        ("publish_content_photo", "content_photo_capable"),
    ):
        if isinstance(item.get(flag), bool) and item[flag]:
            names.add(key)
        elif str(item.get(flag) or "").strip().lower() in {"1", "true", "yes"}:
            names.add(key)
    return names


def _creatok_delivery_mode(item: Dict[str, Any], capabilities: set[str]) -> str:
    """投递模式只对 Content Posting 连接有意义；Shop 账号返回空串。"""
    platform = str(item.get("platform") or "").strip().lower()
    if "shop" in platform:
        return ""
    for name in capabilities:
        if "direct_post" in name:
            return "direct_post"
        if "inbox" in name:
            return "inbox"
    for key in ("delivery_mode", "deliveryMode", "post_mode"):
        value = str(item.get(key) or "").strip().lower()
        if "inbox" in value:
            return "inbox"
        if "direct" in value or "post" in value:
            return "direct_post"
    return "direct_post"


def _has_capability(capabilities: Iterable[str], *needles: str) -> bool:
    return any(needle in name for name in capabilities for needle in needles)


def _accounts_for_channel(db: AutoPublishDB, channel: str) -> list[Any]:
    """Include dual-channel bindings even when the legacy default differs."""
    account_ids = {
        str(binding["account_id"] or "").strip()
        for binding in db.list_account_channel_bindings()
        if str(binding["publish_channel"] or "").strip() == channel
    }
    accounts = {str(row["account_id"] or "").strip(): row
                for row in db.list_account_configs(publish_channel=channel)}
    for account_id in account_ids:
        row = db.get_account_config(account_id)
        if row is not None:
            accounts[account_id] = row
    return [accounts[key] for key in sorted(accounts)]


def reconcile_creatok_account_capabilities(
    db: AutoPublishDB,
    adapter: Any,
) -> Dict[str, Any]:
    """同步 CreatOK 连接发现结果到 account_configs（只读，不创建发布任务）。

    连接字段结构以真实账号连接样例校准前使用宽容解析；失败只记录错误，
    不降级已有 ok 状态。
    """
    accounts = _accounts_for_channel(db, "CreatOK")
    account_ids = [str(row["account_id"] or "").strip() for row in accounts]
    if not accounts:
        return {"checked": 0, "connections": 0, "matched": 0, "items": []}

    try:
        connections = adapter.list_connections()
    except Exception as exc:
        for account_id in account_ids:
            db.mark_account_provider_error(account_id, str(exc))
        return {"checked": 0, "failed": 1, "error": str(exc), "cached_accounts": len(accounts)}

    connection_by_uid: Dict[str, Dict[str, Any]] = {}
    for connection in connections:
        uid = str(
            connection.get("uid") or connection.get("connection_uid")
            or connection.get("connectionId") or connection.get("id") or ""
        ).strip()
        if uid:
            connection_by_uid[uid] = connection

    items = []
    matched = 0
    for account in accounts:
        account_id = str(account["account_id"] or "").strip()
        existing_uid = str(account["provider_connection_uid"] or "").strip()
        connection = connection_by_uid.get(existing_uid)
        if connection is None:
            keys = _creatok_connection_keys({**dict(account), "account_id": account_id})
            for candidate in connections:
                if keys & _creatok_connection_keys(candidate):
                    connection = candidate
                    break
        if connection is None:
            db.mark_account_provider_error(account_id, "CreatOK 未发现该账号的 TikTok 连接")
            items.append({"account_id": account_id, "matched": False, "missing": True})
            continue

        matched += 1
        uid = str(
            connection.get("uid") or connection.get("connection_uid")
            or connection.get("connectionId") or connection.get("id") or ""
        ).strip()
        capabilities = _creatok_capability_names(connection)
        health = str(connection.get("health") or connection.get("status") or "ok").strip()
        timezone = str(
            connection.get("timezone") or connection.get("account_timezone")
            or connection.get("accountTimezone") or account["account_timezone"] or ""
        ).strip()
        db.update_account_provider_info(
            account_id,
            connection_uid=uid,
            timezone=timezone,
            content_video=_has_capability(capabilities, "publish_content_video", "content_video"),
            content_photo=_has_capability(capabilities, "publish_content_photo", "content_photo"),
            shop_video=_has_capability(capabilities, "publish_shoppable_video", "shoppable_video", "publish_shop_video"),
            shop_photo=_has_capability(capabilities, "publish_shoppable_photo", "shoppable_photo", "publish_shop_photo"),
            direct_post="direct_post" in _creatok_delivery_mode(connection, capabilities),
            delivery_mode=_creatok_delivery_mode(connection, capabilities),
            health=health,
            error="",
            profile_id=str(account["publish_profile_id"] or "").strip(),
        )
        items.append({"account_id": account_id, "account_name": str(account["account_name"] or ""), "matched": True, "connection_uid": uid})

    return {
        "checked": len(items),
        "connections": len(connections),
        "matched": matched,
        "missing": sum(1 for item in items if item.get("missing")),
        "items": items,
    }


def reconcile_neobund_account_capabilities(
    db: AutoPublishDB,
    adapter: NeoBundPublishAdapter,
) -> Dict[str, Any]:
    accounts = _accounts_for_channel(db, "NeoBund")
    account_ids = [str(row["account_id"] or "").strip() for row in accounts]
    if not accounts:
        return {
            "checked": 0,
            "organic_capable": 0,
            "shoppable_capable": 0,
            "missing_remote": 0,
            "nurture_mismatch": 0,
            "items": [],
        }

    try:
        organic_index = _build_index(_records(adapter.client.list_tiktok_accounts(current=1, size=200)))
        shoppable_index = _build_index(_records(adapter.client.list_creator_accounts(current=1, size=200)))
    except Exception as exc:
        db.mark_account_capability_error(account_ids, str(exc))
        return {
            "checked": 0,
            "failed": 1,
            "error": str(exc),
            "cached_accounts": len(accounts),
        }

    updates = []
    items = []
    for account in accounts:
        account_id = str(account["account_id"] or "").strip()
        keys = _mapped_account_keys(adapter, account_id)
        account_name = str(account["account_name"] or "").strip().lower()
        if account_name:
            keys.add(account_name)
        organic = _find_account(organic_index, keys)
        shoppable = _find_account(shoppable_index, keys)
        organic_capable = organic is not None and str(organic.get("quotaStatus") or "1").strip() == "1"
        shoppable_capable = shoppable is not None and str(shoppable.get("quotaStatus") or "1").strip() == "1"
        updates.append(
            {
                "account_id": account_id,
                "organic_capable": organic_capable,
                "shoppable_capable": shoppable_capable,
                "organic_auth_id": _auth_id(organic),
                "shoppable_auth_id": _auth_id(shoppable),
            }
        )
        nurture_enabled = bool(int(account["nurture_enabled"] or 0))
        items.append(
            {
                "account_id": account_id,
                "account_name": str(account["account_name"] or ""),
                "organic_capable": organic_capable,
                "shoppable_capable": shoppable_capable,
                "missing_remote": not organic_capable and not shoppable_capable,
                "nurture_mismatch": nurture_enabled and not organic_capable,
            }
        )

    db.update_account_capabilities(updates)
    return {
        "checked": len(items),
        "organic_capable": sum(1 for item in items if item["organic_capable"]),
        "shoppable_capable": sum(1 for item in items if item["shoppable_capable"]),
        "missing_remote": sum(1 for item in items if item["missing_remote"]),
        "nurture_mismatch": sum(1 for item in items if item["nurture_mismatch"]),
        "items": items,
    }
