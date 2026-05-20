#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

APP_TOKEN = "VL5MbJj7RaWe4UsmaOAcXKCznoe"
TABLE_ID = "tbl1IrGRZ4fTuZAi"
DB_PATH = "/Users/likeu3/Desktop/skills/workspace-archive-20260419-131447/skills/hermes-product-analysis/artifacts/agent_runtime.sqlite3"
BASE = "https://open.feishu.cn/open-apis"

QUERY = """
 SELECT p.* FROM product_actual_performance p
 INNER JOIN (
 SELECT product_id, MAX(perf_id) AS max_id
 FROM product_actual_performance GROUP BY product_id
 ) m ON p.perf_id = m.max_id
 WHERE p.outcome IN ('待测', '测款中')
"""

FIELD_NAMES = [
    "product_id", "product_name", "market_id", "category_id", "snapshot_date",
    "schema_version", "product_potential_score", "execution_ready_score",
    "v3_5_shadow_score", "total_score", "final_action", "final_task_pool",
    "need_accio_lookup", "accio_lookup_priority", "selection_decision",
    "test_started_at", "week_sales_volume", "week_roi", "fail_reason", "outcome",
    "outcome_manual_override", "outcome_filled_at", "notes",
]

SINGLE_SELECT_ALLOWED = {
    "final_action": {"select", "manual_review", "head_reference", "observe"},
    "accio_lookup_priority": {"P0", "P1", "P2", ""},
    "selection_decision": {"已选", "已弃", "待定"},
    "fail_reason": {"内容不行", "货不对版", "定价错了", "流量没起来", "其他/不清楚"},
    "outcome": {"待测", "测款中", "爆款", "稳赚", "虚爆", "平品", "扑街", "跳过"},
}

NULLABLE = {
    "v3_5_shadow_score", "accio_lookup_priority", "selection_decision", "test_started_at",
    "week_sales_volume", "week_roi", "fail_reason", "outcome_manual_override",
    "outcome_filled_at", "notes",
}


def load_config() -> Dict[str, Any]:
    path = Path.home() / ".openclaw" / "openclaw.json"
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def token() -> str:
    cfg = load_config()
    fs = cfg["channels"]["feishu"]
    r = requests.post(f"{BASE}/auth/v3/tenant_access_token/internal", json={"app_id": fs["appId"], "app_secret": fs["appSecret"]}, timeout=30)
    data = r.json()
    if data.get("code") != 0:
        raise RuntimeError(f"token failed: {data}")
    return data["tenant_access_token"]


def request(method: str, path: str, access_token: str, **kwargs: Any) -> Dict[str, Any]:
    url = f"{BASE}{path}"
    headers = kwargs.pop("headers", {})
    headers.update({"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"})
    last = None
    for attempt in range(5):
        try:
            r = requests.request(method, url, headers=headers, timeout=60, **kwargs)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", "2"))
                time.sleep(wait)
                continue
            data = r.json()
            if data.get("code") == 0:
                return data
            # retry transient-ish errors
            last = data
            if attempt < 4 and data.get("code") in {99991663, 99991400, 1254290}:
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError(f"Feishu API failed {method} {path}: {data}")
        except requests.RequestException as e:
            last = repr(e)
            if attempt < 4:
                time.sleep(2 ** attempt)
                continue
            raise
    raise RuntimeError(f"Feishu API failed after retries: {last}")


def list_all_records(access_token: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    page_token = None
    while True:
        params = {"page_size": 500}
        if page_token:
            params["page_token"] = page_token
        data = request("GET", f"/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", access_token, params=params)
        d = data.get("data", {})
        items.extend(d.get("items", []))
        if not d.get("has_more"):
            break
        page_token = d.get("page_token")
    return items


def date_to_ms(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        # If source is unix seconds, convert to ms; if already ms, leave.
        return int(value * 1000) if value < 10_000_000_000 else int(value)
    s = str(value).strip()
    if not s:
        return None
    # YYYY-MM-DD -> local midnight (Asia/Shanghai) as epoch ms
    if len(s) == 10:
        y, m, d = map(int, s.split("-"))
        tz = dt.timezone(dt.timedelta(hours=8))
        return int(dt.datetime(y, m, d, tzinfo=tz).timestamp() * 1000)
    # Try ISO datetime, assume Asia/Shanghai if naive.
    x = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    if x.tzinfo is None:
        x = x.replace(tzinfo=dt.timezone(dt.timedelta(hours=8)))
    return int(x.timestamp() * 1000)


def unix_seconds_to_ms(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    return date_to_ms(value)


def clean_number(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    return float(value)


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value)
    if s == "" and value in (None, ""):
        return ""
    return s


def row_to_fields(row: sqlite3.Row) -> Dict[str, Any]:
    fields: Dict[str, Any] = {}
    for name in FIELD_NAMES:
        v = row[name] if name in row.keys() else None
        if name in {"product_potential_score", "execution_ready_score", "v3_5_shadow_score", "total_score", "week_sales_volume", "week_roi"}:
            val = clean_number(v)
        elif name == "need_accio_lookup":
            val = bool(int(v or 0))
        elif name == "test_started_at":
            val = date_to_ms(v)
        elif name == "outcome_filled_at":
            # DB field documented as unix seconds; convert to ms.
            val = unix_seconds_to_ms(v)
        elif name in SINGLE_SELECT_ALLOWED:
            raw = "" if v is None else str(v).strip()
            if raw.lower() in {"", "none", "null"} and name in NULLABLE:
                val = None
            elif raw in SINGLE_SELECT_ALLOWED[name]:
                val = raw
            else:
                raise ValueError(f"Invalid select value for {name}: {raw!r} product_id={row['product_id']}")
        else:
            val = clean_text(v)
        if val is None and name not in NULLABLE:
            # Required-ish text/select fields: keep empty string instead of null.
            val = ""
        # For nullable fields, send null to clear old values.
        fields[name] = val
    return fields


def read_source_rows() -> List[sqlite3.Row]:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    try:
        return con.execute(QUERY).fetchall()
    finally:
        con.close()


def chunks(xs: List[Any], n: int = 500):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    access_token = token()
    existing = list_all_records(access_token)
    product_to_record: Dict[str, str] = {}
    for rec in existing:
        fields = rec.get("fields") or {}
        pid = fields.get("product_id")
        if isinstance(pid, list):
            pid = "".join(str(x.get("text", "")) if isinstance(x, dict) else str(x) for x in pid)
        if pid not in (None, ""):
            product_to_record[str(pid)] = rec["record_id"]

    rows = read_source_rows()
    creates: List[Dict[str, Any]] = []
    updates: List[Dict[str, Any]] = []
    for row in rows:
        fields = row_to_fields(row)
        pid = str(row["product_id"])
        if pid in product_to_record:
            updates.append({"record_id": product_to_record[pid], "fields": fields})
        else:
            creates.append({"fields": fields})

    print(json.dumps({
        "source_rows": len(rows),
        "existing_records": len(existing),
        "mapped_product_ids": len(product_to_record),
        "to_create": len(creates),
        "to_update": len(updates),
        "sample_product_ids": [str(r["product_id"]) for r in rows[:5]],
    }, ensure_ascii=False, indent=2))

    if args.dry_run:
        return 0

    created = updated = 0
    for batch in chunks(updates, 500):
        request("POST", f"/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/batch_update", access_token, json={"records": batch})
        updated += len(batch)
        print(f"updated {updated}/{len(updates)}")
    for batch in chunks(creates, 500):
        request("POST", f"/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/batch_create", access_token, json={"records": batch})
        created += len(batch)
        print(f"created {created}/{len(creates)}")

    after = list_all_records(access_token)
    after_map = {}
    for rec in after:
        fields = rec.get("fields") or {}
        pid = fields.get("product_id")
        if isinstance(pid, list):
            pid = "".join(str(x.get("text", "")) if isinstance(x, dict) else str(x) for x in pid)
        if pid not in (None, ""):
            after_map[str(pid)] = rec["record_id"]
    missing = [str(r["product_id"]) for r in rows if str(r["product_id"]) not in after_map]
    print(json.dumps({"created": created, "updated": updated, "after_records": len(after), "after_mapped_product_ids": len(after_map), "missing_after": missing[:10], "missing_count": len(missing)}, ensure_ascii=False, indent=2))
    if missing:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
