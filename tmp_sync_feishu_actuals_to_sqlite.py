#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import shutil
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests

APP_TOKEN = "VL5MbJj7RaWe4UsmaOAcXKCznoe"
TABLE_ID = "tbl1IrGRZ4fTuZAi"
DB_PATH = Path("/Users/likeu3/Desktop/skills/workspace-archive-20260419-131447/skills/hermes-product-analysis/artifacts/agent_runtime.sqlite3")
BASE = "https://open.feishu.cn/open-apis"
TZ = ZoneInfo("Asia/Shanghai")
TODAY = dt.datetime.now(TZ).date()
FINAL_OUTCOMES = {"爆款", "稳赚", "虚爆", "平品", "扑街", "跳过"}
FEISHU_OUTCOME_OPTIONS = {"待测", "测款中", "爆款", "稳赚", "虚爆", "平品", "扑街", "跳过"}
SYNC_FIELDS = [
    "week_sales_volume",
    "week_roi",
    "test_started_at",
    "fail_reason",
    "selection_decision",
    "outcome_manual_override",
    "notes",
]


def load_config() -> Dict[str, Any]:
    with (Path.home() / ".openclaw" / "openclaw.json").open("r", encoding="utf-8") as f:
        return json.load(f)


def tenant_token() -> str:
    fs = load_config()["channels"]["feishu"]
    r = requests.post(
        f"{BASE}/auth/v3/tenant_access_token/internal",
        json={"app_id": fs["appId"], "app_secret": fs["appSecret"]},
        timeout=30,
    )
    data = r.json()
    if data.get("code") != 0:
        raise RuntimeError(f"获取飞书 token 失败: {data}")
    return data["tenant_access_token"]


def feishu_request(method: str, path: str, token: str, **kwargs: Any) -> Dict[str, Any]:
    url = f"{BASE}{path}"
    headers = kwargs.pop("headers", {})
    headers.update({"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    last: Any = None
    for attempt in range(5):
        try:
            r = requests.request(method, url, headers=headers, timeout=60, **kwargs)
            if r.status_code == 429:
                time.sleep(int(r.headers.get("Retry-After", "2")))
                continue
            data = r.json()
            if data.get("code") == 0:
                return data
            last = data
            if attempt < 4 and data.get("code") in {99991663, 99991400, 1254290}:
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError(f"飞书 API 失败 {method} {path}: {data}")
        except requests.RequestException as exc:
            last = repr(exc)
            if attempt < 4:
                time.sleep(2 ** attempt)
                continue
            raise
    raise RuntimeError(f"飞书 API 多次重试失败: {last}")


def list_all_records(token: str) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    while True:
        params = {"page_size": 500}
        if page_token:
            params["page_token"] = page_token
        data = feishu_request("GET", f"/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", token, params=params)
        body = data.get("data", {})
        records.extend(body.get("items", []))
        if not body.get("has_more"):
            break
        page_token = body.get("page_token")
    return records


def is_empty(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str):
        return v.strip() == ""
    if isinstance(v, list):
        return len(v) == 0 or all(is_empty(x) for x in v)
    return False


def text_value(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, list):
        parts: List[str] = []
        for x in v:
            if isinstance(x, dict):
                parts.append(str(x.get("text", "")))
            else:
                parts.append(str(x))
        return "".join(parts).strip()
    if isinstance(v, dict):
        return str(v.get("text", v.get("name", ""))).strip()
    return str(v).strip()


def number_value(v: Any) -> Optional[float]:
    if is_empty(v):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = text_value(v).replace(",", "")
    if not s:
        return None
    return float(s)


def date_yyyy_mm_dd(v: Any) -> Optional[str]:
    if is_empty(v):
        return None
    if isinstance(v, (int, float)):
        x = int(v)
        # Feishu DateTime normally returns milliseconds. Be tolerant of seconds.
        if x < 10_000_000_000:
            x *= 1000
        return dt.datetime.fromtimestamp(x / 1000, TZ).date().isoformat()
    s = text_value(v)
    if not s:
        return None
    if s.isdigit():
        return date_yyyy_mm_dd(int(s))
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(TZ).date().isoformat() if "T" in s else s[:10]


def latest_perf_id(con: sqlite3.Connection, product_id: str) -> Optional[int]:
    row = con.execute(
        "SELECT perf_id FROM product_actual_performance WHERE product_id = ? ORDER BY perf_id DESC LIMIT 1",
        (product_id,),
    ).fetchone()
    return int(row[0]) if row else None


def normalize_record_fields(fields: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    product_id = text_value(fields.get("product_id"))
    vals: Dict[str, Any] = {}
    for name in SYNC_FIELDS:
        raw = fields.get(name)
        if is_empty(raw):
            continue
        if name in {"week_sales_volume", "week_roi"}:
            vals[name] = number_value(raw)
        elif name == "test_started_at":
            vals[name] = date_yyyy_mm_dd(raw)
        else:
            vals[name] = text_value(raw)
    return product_id, vals


def derive_outcome(row: sqlite3.Row) -> str:
    override = (row["outcome_manual_override"] or "").strip()
    if override:
        return override
    sales = row["week_sales_volume"]
    roi = row["week_roi"]
    if sales is None and roi is None:
        started = (row["test_started_at"] or "").strip()
        if not started:
            return "待测"
        try:
            started_date = dt.date.fromisoformat(started[:10])
            return "测款中" if (TODAY - started_date).days < 14 else "待测"
        except Exception:
            return "待测"
    # Rules reference roi for all sales cases. If roi is missing, treat as 0 so low/failed branches still work.
    s = float(sales or 0)
    r = float(roi or 0)
    if s > 500 and r >= 3:
        return "爆款"
    if s > 500 and r < 3:
        return "虚爆"
    if s >= 5 and r >= 3:
        return "稳赚"
    if s >= 5 and r < 3:
        return "平品"
    if s < 5:
        return "扑街"
    return "待测"


def chunks(xs: List[Any], n: int = 500) -> Iterable[List[Any]]:
    for i in range(0, len(xs), n):
        yield xs[i:i+n]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    token = tenant_token()
    records = list_all_records(token)
    product_to_record: Dict[str, str] = {}
    normalized: List[Tuple[str, str, Dict[str, Any]]] = []
    skipped_empty_pid = 0
    for rec in records:
        pid, vals = normalize_record_fields(rec.get("fields") or {})
        if not pid:
            skipped_empty_pid += 1
            continue
        product_to_record[pid] = rec["record_id"]
        normalized.append((rec["record_id"], pid, vals))

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    now = int(time.time())
    sync_updates: List[Dict[str, Any]] = []
    missing_products: List[str] = []

    try:
        if not args.dry_run:
            backup = DB_PATH.with_suffix(DB_PATH.suffix + f".bak_{dt.datetime.now(TZ).strftime('%Y%m%d_%H%M%S')}")
            shutil.copy2(DB_PATH, backup)
            print(f"backup={backup}")
            con.execute("BEGIN")

        for _record_id, pid, vals in normalized:
            if not vals:
                continue
            perf_id = latest_perf_id(con, pid)
            if perf_id is None:
                missing_products.append(pid)
                continue
            row = con.execute(
                "SELECT week_sales_volume, week_roi, test_started_at, fail_reason, selection_decision, outcome_manual_override, notes FROM product_actual_performance WHERE perf_id = ?",
                (perf_id,),
            ).fetchone()
            changed = {k: v for k, v in vals.items() if row[k] != v}
            if not changed:
                continue
            sync_updates.append({"perf_id": perf_id, "product_id": pid, "fields": changed})
            if not args.dry_run:
                cols = list(changed.keys()) + ["updated_at"]
                params = [changed[k] for k in changed] + [now, perf_id]
                sql = "UPDATE product_actual_performance SET " + ", ".join(f"{c} = ?" for c in cols) + " WHERE perf_id = ?"
                con.execute(sql, params)

        outcome_changes: List[Dict[str, Any]] = []
        rows = con.execute(
            "SELECT * FROM product_actual_performance WHERE outcome NOT IN ('爆款','稳赚','虚爆','平品','扑街','跳过')"
        ).fetchall()
        for row in rows:
            new_outcome = derive_outcome(row)
            old_outcome = row["outcome"]
            if new_outcome != old_outcome:
                change = {
                    "perf_id": int(row["perf_id"]),
                    "product_id": row["product_id"],
                    "old_outcome": old_outcome,
                    "new_outcome": new_outcome,
                }
                outcome_changes.append(change)
                if not args.dry_run:
                    con.execute(
                        "UPDATE product_actual_performance SET outcome = ?, outcome_filled_at = ?, updated_at = ? WHERE perf_id = ?",
                        (new_outcome, now, now, int(row["perf_id"])),
                    )

        if not args.dry_run:
            con.commit()

    except Exception:
        if not args.dry_run:
            con.rollback()
        raise
    finally:
        con.close()

    # Push changed outcomes for rows represented in Feishu. If multiple changed rows share a product_id, keep latest perf_id.
    latest_changes: Dict[str, Dict[str, Any]] = {}
    for ch in outcome_changes:
        pid = ch["product_id"]
        if pid not in latest_changes or ch["perf_id"] > latest_changes[pid]["perf_id"]:
            latest_changes[pid] = ch

    feishu_updates: List[Dict[str, Any]] = []
    invalid_outcomes: List[Dict[str, Any]] = []
    for pid, ch in latest_changes.items():
        rid = product_to_record.get(pid)
        if not rid:
            continue
        outcome = ch["new_outcome"]
        if outcome not in FEISHU_OUTCOME_OPTIONS:
            invalid_outcomes.append(ch)
            continue
        feishu_updates.append({"record_id": rid, "fields": {"outcome": outcome}})

    print(json.dumps({
        "today": TODAY.isoformat(),
        "feishu_records": len(records),
        "feishu_records_with_product_id": len(product_to_record),
        "skipped_empty_product_id": skipped_empty_pid,
        "sqlite_field_update_rows": len(sync_updates),
        "sqlite_missing_product_ids": len(missing_products),
        "sqlite_outcome_changes": len(outcome_changes),
        "feishu_outcome_updates": len(feishu_updates),
        "invalid_outcome_overrides_not_pushed": len(invalid_outcomes),
        "sample_field_updates": sync_updates[:5],
        "sample_outcome_changes": outcome_changes[:10],
        "missing_product_ids_sample": missing_products[:10],
        "invalid_outcome_sample": invalid_outcomes[:10],
    }, ensure_ascii=False, indent=2))

    if args.dry_run:
        return 0

    pushed = 0
    for batch in chunks(feishu_updates, 500):
        feishu_request(
            "POST",
            f"/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/batch_update",
            token,
            json={"records": batch},
        )
        pushed += len(batch)
        print(f"pushed_outcome {pushed}/{len(feishu_updates)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
