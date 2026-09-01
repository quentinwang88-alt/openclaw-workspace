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


def reconcile_neobund_account_capabilities(
    db: AutoPublishDB,
    adapter: NeoBundPublishAdapter,
) -> Dict[str, Any]:
    accounts = db.list_account_configs(publish_channel="NeoBund")
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
