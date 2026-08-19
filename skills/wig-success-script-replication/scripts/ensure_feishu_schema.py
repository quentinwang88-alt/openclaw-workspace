#!/usr/bin/env python3
"""Plan or apply the non-destructive V1 Lite schema in one Feishu Base.

Dry-run is the default. Applying requires the exact SHA-256 emitted by a recent
dry-run on the same Base. This tool never deletes a table, field, view, or row.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, urlparse

import requests


SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from field_specs import (  # noqa: E402
    DEFAULT_PRIMARY_TABLE_ID,
    DEFAULT_UNIFIED_BASE_URL,
    TABLE_BY_KEY,
    TABLE_SPECS,
)


API_ROOT = "https://open.feishu.cn/open-apis"
DEFAULT_RECEIPT = Path("/tmp/wig-success-script-replication-schema-plan.json")
RECEIPT_MAX_AGE_SECONDS = 2 * 60 * 60


class SchemaError(RuntimeError):
    pass


def _wiki_token(url: str) -> str:
    path = urlparse(url).path.rstrip("/")
    token = path.rsplit("/", 1)[-1]
    if not token:
        raise SchemaError("cannot parse wiki token from --base-url")
    return token


def _table_id_from_url(url: str) -> str | None:
    values = parse_qs(urlparse(url).query).get("table") or []
    return values[0] if values else None


def _load_credentials() -> tuple[str, str]:
    config_path = Path(os.environ.get("OPENCLAW_CONFIG", "~/.openclaw/openclaw.json")).expanduser()
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
        channel = config["channels"]["feishu"]
        return str(channel["appId"]), str(channel["appSecret"])
    except (OSError, KeyError, TypeError, ValueError) as exc:
        raise SchemaError(f"cannot load Feishu credentials from {config_path}: {exc}") from exc


@dataclass
class FeishuAdmin:
    app_id: str
    app_secret: str
    access_token: str | None = None

    def token(self) -> str:
        if self.access_token:
            return self.access_token
        response = requests.post(
            f"{API_ROOT}/auth/v3/tenant_access_token/internal",
            json={"app_id": self.app_id, "app_secret": self.app_secret},
            timeout=30,
        )
        payload = response.json()
        if response.status_code >= 400 or payload.get("code") not in (0, "0"):
            raise SchemaError(f"Feishu authentication failed: {payload.get('msg') or response.status_code}")
        self.access_token = str(payload["tenant_access_token"])
        return self.access_token

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        response = requests.request(
            method,
            f"{API_ROOT}{path}",
            headers={"Authorization": f"Bearer {self.token()}", "Content-Type": "application/json"},
            params=params,
            json=body,
            timeout=30,
        )
        try:
            payload = response.json()
        except ValueError as exc:
            raise SchemaError(f"Feishu returned non-JSON response ({response.status_code})") from exc
        if response.status_code >= 400 or payload.get("code") not in (0, "0", None):
            raise SchemaError(
                f"Feishu {method} {path} failed: {payload.get('code')} {payload.get('msg')}"
            )
        return payload.get("data", {}) or {}

    def resolve_app_token(self, wiki_url: str) -> str:
        data = self.request(
            "GET",
            "/wiki/v2/spaces/get_node",
            params={"token": _wiki_token(wiki_url)},
        )
        node = data.get("node") or {}
        if node.get("obj_type") != "bitable" or not node.get("obj_token"):
            raise SchemaError("the wiki node is not a Bitable")
        return str(node["obj_token"])

    def list_all(self, path: str, *, page_size: int = 500) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        page_token: str | None = None
        while True:
            params: dict[str, Any] = {"page_size": page_size}
            if page_token:
                params["page_token"] = page_token
            data = self.request("GET", path, params=params)
            items.extend(data.get("items") or [])
            if not data.get("has_more"):
                return items
            page_token = data.get("page_token")


def load_snapshot(admin: FeishuAdmin, app_token: str) -> dict[str, Any]:
    tables = admin.list_all(f"/bitable/v1/apps/{app_token}/tables", page_size=100)
    snapshot: dict[str, Any] = {"tables": []}
    for table in tables:
        table_id = str(table.get("table_id") or "")
        if not table_id:
            continue
        fields = admin.list_all(
            f"/bitable/v1/apps/{app_token}/tables/{table_id}/fields",
            page_size=500,
        )
        views = admin.list_all(
            f"/bitable/v1/apps/{app_token}/tables/{table_id}/views",
            page_size=100,
        )
        snapshot["tables"].append(
            {
                "table_id": table_id,
                "name": str(table.get("name") or ""),
                "fields": fields,
                "views": views,
            }
        )
    return snapshot


def offline_snapshot(primary_table_id: str) -> dict[str, Any]:
    return {
        "tables": [
            {
                "table_id": primary_table_id,
                "name": "<existing-primary-table>",
                "fields": [{"field_id": "<primary-field>", "field_name": "文本", "type": 1, "ui_type": "Text"}],
                "views": [{"view_id": "<default-view>", "view_name": "表格", "view_type": "grid"}],
            }
        ]
    }


def _option_names(property_value: Any) -> list[str]:
    if not isinstance(property_value, dict):
        return []
    return [
        str(item.get("name"))
        for item in property_value.get("options") or []
        if isinstance(item, dict) and item.get("name")
    ]


def _description_text(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("text") or "")
    return str(value or "")


def _writable_property(field_type: int, value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    if field_type == 18:
        return {
            "table_id": value.get("table_id"),
            "multiple": bool(value.get("multiple", False)),
        }
    return value


def _table_map(snapshot: dict[str, Any], primary_table_id: str) -> dict[str, dict[str, Any]]:
    tables = snapshot.get("tables") or []
    result: dict[str, dict[str, Any]] = {}
    mother_spec = TABLE_BY_KEY["mother"]
    primary = next((item for item in tables if item.get("table_id") == primary_table_id), None)
    if primary is None:
        primary = next((item for item in tables if item.get("name") == mother_spec["name"]), None)
    if primary is None:
        raise SchemaError(
            f"primary table {primary_table_id} is not present in this Base; refusing to guess"
        )
    result["mother"] = primary
    for spec in TABLE_SPECS:
        if spec["key"] == "mother":
            continue
        found = next((item for item in tables if item.get("name") == spec["name"]), None)
        if found:
            result[spec["key"]] = found
    return result


def build_plan(snapshot: dict[str, Any], primary_table_id: str) -> list[dict[str, Any]]:
    current = _table_map(snapshot, primary_table_id)
    actions: list[dict[str, Any]] = []

    for spec in TABLE_SPECS:
        table = current.get(spec["key"])
        if table is None:
            actions.append(
                {
                    "op": "create-table",
                    "table_key": spec["key"],
                    "name": spec["name"],
                    "primary": spec["primary"],
                    "view": spec["view"],
                }
            )
            continue
        if table.get("name") != spec["name"]:
            actions.append(
                {
                    "op": "rename-table",
                    "table_key": spec["key"],
                    "table_id": table["table_id"],
                    "from": table.get("name"),
                    "to": spec["name"],
                }
            )

    for spec in TABLE_SPECS:
        table = current.get(spec["key"])
        existing_fields = list((table or {}).get("fields") or [])
        fields_by_name = {str(item.get("field_name")): item for item in existing_fields}
        primary_spec = spec["fields"][0]
        if table and primary_spec["name"] not in fields_by_name:
            if not existing_fields:
                raise SchemaError(f"{spec['name']} has no primary field")
            existing_primary = existing_fields[0]
            if int(existing_primary.get("type") or 0) != 1:
                raise SchemaError(f"{spec['name']} primary field is not text; manual migration required")
            actions.append(
                {
                    "op": "rename-field",
                    "table_key": spec["key"],
                    "table_id": table["table_id"],
                    "field_id": existing_primary.get("field_id"),
                    "from": existing_primary.get("field_name"),
                    "to": primary_spec["name"],
                    "type": int(existing_primary.get("type") or primary_spec["type"]),
                    "ui_type": str(existing_primary.get("ui_type") or primary_spec["ui_type"]),
                }
            )
            fields_by_name[primary_spec["name"]] = existing_primary

        for field_spec in spec["fields"]:
            existing = fields_by_name.get(field_spec["name"])
            if existing is None:
                # The primary field is created together with a missing table.
                if table is None and field_spec.get("primary"):
                    continue
                actions.append(
                    {
                        "op": "create-field",
                        "table_key": spec["key"],
                        "table_id": (table or {}).get("table_id"),
                        "field": {
                            key: value for key, value in field_spec.items()
                            if key not in {"primary", "prune_options"}
                        },
                    }
                )
                continue
            if int(existing.get("type") or 0) != int(field_spec["type"]):
                raise SchemaError(
                    f"field type drift: {spec['name']}.{field_spec['name']} is "
                    f"{existing.get('type')}, expected {field_spec['type']}; no automatic conversion"
                )
            wanted_description = field_spec.get("description")
            if wanted_description and _description_text(existing.get("description")) != _description_text(wanted_description):
                actions.append(
                    {
                        "op": "update-field-description",
                        "table_key": spec["key"],
                        "table_id": table["table_id"],
                        "field_id": existing.get("field_id"),
                        "field_name": field_spec["name"],
                        "type": int(existing.get("type") or field_spec["type"]),
                        "ui_type": str(existing.get("ui_type") or field_spec["ui_type"]),
                        "property": _writable_property(
                            int(existing.get("type") or field_spec["type"]),
                            existing.get("property"),
                        ),
                        "description": wanted_description,
                    }
                )
            if field_spec["type"] == 3:
                wanted = _option_names(field_spec.get("property"))
                present = _option_names(existing.get("property"))
                missing = [name for name in wanted if name not in present]
                if field_spec.get("prune_options") and present != wanted:
                    actions.append(
                        {
                            "op": "replace-select-options",
                            "table_key": spec["key"],
                            "table_id": table["table_id"],
                            "field_id": existing.get("field_id"),
                            "field_name": field_spec["name"],
                            "type": field_spec["type"],
                            "ui_type": field_spec["ui_type"],
                            "options": wanted,
                            "from_options": present,
                        }
                    )
                elif missing:
                    actions.append(
                        {
                            "op": "extend-select-options",
                            "table_key": spec["key"],
                            "table_id": table["table_id"],
                            "field_id": existing.get("field_id"),
                            "field_name": field_spec["name"],
                            "type": field_spec["type"],
                            "ui_type": field_spec["ui_type"],
                            "options": present + missing,
                        }
                    )
            if field_spec["type"] == 18:
                relation_key = field_spec["relation"]
                relation_table = current.get(relation_key)
                actual_target = (existing.get("property") or {}).get("table_id")
                if relation_table and actual_target and actual_target != relation_table["table_id"]:
                    raise SchemaError(
                        f"relation drift: {spec['name']}.{field_spec['name']} targets another table"
                    )

        if table:
            view_names = [str(item.get("view_name") or item.get("name") or "") for item in table.get("views") or []]
            if spec["view"] not in view_names:
                views = table.get("views") or []
                if views:
                    actions.append(
                        {
                            "op": "rename-view",
                            "table_key": spec["key"],
                            "table_id": table["table_id"],
                            "view_id": views[0].get("view_id"),
                            "from": views[0].get("view_name") or views[0].get("name"),
                            "to": spec["view"],
                        }
                    )
                else:
                    actions.append(
                        {
                            "op": "create-view",
                            "table_key": spec["key"],
                            "table_id": table["table_id"],
                            "name": spec["view"],
                        }
                    )
    return actions


def plan_hash(app_token: str, actions: list[dict[str, Any]]) -> str:
    canonical = json.dumps(
        {"app_token": app_token, "actions": actions},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _write_receipt(path: Path, app_token: str, digest: str) -> None:
    path.write_text(
        json.dumps(
            {"app_token": app_token, "plan_hash": digest, "created_at": int(time.time())},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _verify_receipt(path: Path, app_token: str, digest: str) -> None:
    try:
        receipt = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SchemaError("no valid dry-run receipt; run --dry-run first") from exc
    age = int(time.time()) - int(receipt.get("created_at") or 0)
    if age < 0 or age > RECEIPT_MAX_AGE_SECONDS:
        raise SchemaError("dry-run receipt expired; run --dry-run again")
    if receipt.get("app_token") != app_token or receipt.get("plan_hash") != digest:
        raise SchemaError("current plan differs from the approved dry-run")


def _extract_created_table_id(data: dict[str, Any]) -> str:
    table = data.get("table") or data
    table_id = table.get("table_id") if isinstance(table, dict) else None
    if not table_id:
        raise SchemaError("Feishu did not return a table_id after create-table")
    return str(table_id)


def apply_plan(
    admin: FeishuAdmin,
    app_token: str,
    actions: list[dict[str, Any]],
    snapshot: dict[str, Any],
    primary_table_id: str,
) -> None:
    table_ids = {
        key: value["table_id"] for key, value in _table_map(snapshot, primary_table_id).items()
    }
    for action in actions:
        op = action["op"]
        key = action["table_key"]
        if op == "create-table":
            data = admin.request(
                "POST",
                f"/bitable/v1/apps/{app_token}/tables",
                body={
                    "table": {
                        "name": action["name"],
                        "default_view_name": action["view"],
                        "fields": [
                            {"field_name": action["primary"], "type": 1, "ui_type": "Text"}
                        ],
                    }
                },
            )
            table_ids[key] = _extract_created_table_id(data)
            print(f"applied create-table: {action['name']}")
            continue

        table_id = table_ids.get(key) or action.get("table_id")
        if not table_id:
            raise SchemaError(f"cannot resolve table id for {key}")
        table_path = f"/bitable/v1/apps/{app_token}/tables/{table_id}"
        if op == "rename-table":
            admin.request("PATCH", table_path, body={"name": action["to"]})
        elif op == "rename-field":
            admin.request(
                "PUT",
                f"{table_path}/fields/{action['field_id']}",
                body={
                    "field_name": action["to"],
                    "type": action["type"],
                    "ui_type": action["ui_type"],
                },
            )
        elif op == "create-field":
            field = dict(action["field"])
            relation_key = field.pop("relation", None)
            multiple = bool(field.pop("multiple", False))
            if relation_key:
                target = table_ids.get(relation_key)
                if not target:
                    raise SchemaError(f"cannot resolve relation target {relation_key}")
                field["property"] = {"table_id": target, "multiple": multiple}
            field["field_name"] = field.pop("name")
            admin.request("POST", f"{table_path}/fields", body=field)
        elif op == "extend-select-options":
            admin.request(
                "PUT",
                f"{table_path}/fields/{action['field_id']}",
                body={
                    "field_name": action["field_name"],
                    "type": action["type"],
                    "ui_type": action["ui_type"],
                    "property": {"options": [{"name": name} for name in action["options"]]},
                },
            )
        elif op == "replace-select-options":
            admin.request(
                "PUT",
                f"{table_path}/fields/{action['field_id']}",
                body={
                    "field_name": action["field_name"],
                    "type": action["type"],
                    "ui_type": action["ui_type"],
                    "property": {"options": [{"name": name} for name in action["options"]]},
                },
            )
        elif op == "update-field-description":
            body = {
                "field_name": action["field_name"],
                "type": action["type"],
                "ui_type": action["ui_type"],
                "description": action["description"],
            }
            if action.get("property") is not None:
                body["property"] = action["property"]
            admin.request(
                "PUT",
                f"{table_path}/fields/{action['field_id']}",
                body=body,
            )
        elif op == "rename-view":
            admin.request(
                "PATCH",
                f"{table_path}/views/{action['view_id']}",
                body={"view_name": action["to"]},
            )
        elif op == "create-view":
            admin.request(
                "POST",
                f"{table_path}/views",
                body={"view_name": action["name"], "view_type": "grid"},
            )
        else:
            raise SchemaError(f"unsupported plan operation: {op}")
        print(f"applied {op}: {key}")


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ensure the Lite wig replication Feishu schema")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="print the plan; this is the default")
    mode.add_argument("--apply", action="store_true", help="apply an already dry-run plan")
    parser.add_argument("--confirm-plan", help="exact SHA-256 from the preceding dry-run")
    parser.add_argument("--base-url", default=DEFAULT_UNIFIED_BASE_URL)
    parser.add_argument("--app-token", help="skip wiki resolution and use this Bitable app token")
    parser.add_argument("--primary-table-id", default=None)
    parser.add_argument("--receipt", type=Path, default=DEFAULT_RECEIPT)
    parser.add_argument("--offline", action="store_true", help="render desired changes without network")
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    if args.apply and args.offline:
        print("error: --apply cannot be used with --offline")
        return 2
    if args.apply and not args.confirm_plan:
        print("error: --apply requires --confirm-plan from a prior dry-run")
        return 2
    if not args.apply and args.confirm_plan:
        print("error: --confirm-plan is only valid with --apply")
        return 2

    primary_table_id = (
        args.primary_table_id
        or _table_id_from_url(args.base_url)
        or DEFAULT_PRIMARY_TABLE_ID
    )
    try:
        if args.offline:
            app_token = args.app_token or f"wiki:{_wiki_token(args.base_url)}"
            admin = None
            snapshot = offline_snapshot(primary_table_id)
        else:
            app_id, app_secret = _load_credentials()
            admin = FeishuAdmin(app_id, app_secret)
            app_token = args.app_token or admin.resolve_app_token(args.base_url)
            snapshot = load_snapshot(admin, app_token)
        actions = build_plan(snapshot, primary_table_id)
        digest = plan_hash(app_token, actions)
        print(json.dumps({"mode": "apply" if args.apply else "dry-run", "plan_hash": digest, "action_count": len(actions), "actions": actions}, ensure_ascii=False, indent=2))
        if not args.apply:
            _write_receipt(args.receipt, app_token, digest)
            print(f"dry-run receipt: {args.receipt}")
            return 0
        if args.confirm_plan != digest:
            raise SchemaError("--confirm-plan does not match the current plan")
        _verify_receipt(args.receipt, app_token, digest)
        if admin is None:
            raise SchemaError("internal error: apply requires a live Feishu client")
        apply_plan(admin, app_token, actions, snapshot, primary_table_id)
        print(f"schema applied successfully; operations={len(actions)}")
        return 0
    except (SchemaError, requests.RequestException) as exc:
        print(f"error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
