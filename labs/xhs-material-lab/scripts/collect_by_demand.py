#!/usr/bin/env python3
"""按需求清单采集（消费收敛方案 §7）：消费 export_material_gaps.py 的输出。

流程：读需求 JSON → 对每个族按查询词调 xiaohongshu-mcp search_feeds
（filters 来自 query_families.json）→ 复用 search_to_library 的入库逻辑
（note_id 去重、只收图文）→ 记 query_hits 关系（命中 query 集合与时间，
不因二次命中丢来源）。与生产独立：只写实验室库，不触飞书/OPV。

用法（实验室目录）：
  python3 scripts/collect_by_demand.py --demand /tmp/material_demand.json [--limit-per-query 20]
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from search_to_library import import_feeds, parse_cn_count

LAB_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(LAB_ROOT))
DEFAULT_DB = LAB_ROOT / "var" / "material_library.sqlite3"
DEFAULT_FAMILIES = LAB_ROOT / "config" / "query_families.json"
MCP_BASE = "http://127.0.0.1:18060/mcp/v1/search_feeds"

QUERY_HITS_SCHEMA = """
CREATE TABLE IF NOT EXISTS search_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    demand_key TEXT NOT NULL,
    query TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'running',
    started_at TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at TEXT,
    result_count INTEGER,
    error TEXT
);
CREATE INDEX IF NOT EXISTS idx_search_attempts ON search_attempts (demand_key, query);
CREATE TABLE IF NOT EXISTS query_hits (
    note_id TEXT NOT NULL,
    family TEXT NOT NULL,
    query TEXT NOT NULL,
    demand_key TEXT NOT NULL DEFAULT '',
    hit_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (note_id, family, query)
);
"""


def mcp_search(query: str, filters: dict, timeout: int = 60):
    payload = json.dumps({
        "jsonrpc": "2.0", "id": int(time.time()), "method": "tools/call",
        "params": {"name": "search_feeds", "arguments": {
            "keyword": query, "filters": filters}}},
        ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        MCP_BASE, data=payload, headers={
            "Content-Type": "application/json",
            # 服务端要求 Accept 同时声明 JSON 与 SSE（缺一即 400）
            "Accept": "application/json, text/event-stream",
        })
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = json.loads(resp.read().decode("utf-8"))
    text = raw["result"]["content"][0]["text"]
    return json.loads(text).get("feeds") or []


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--demand", required=True)
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--families", default=str(DEFAULT_FAMILIES))
    ap.add_argument("--limit-per-query", type=int, default=20)
    ap.add_argument("--out-root", default=str(LAB_ROOT / "out" / "search"))
    args = ap.parse_args()

    demand = json.loads(Path(args.demand).read_text(encoding="utf-8"))
    families_cfg = json.loads(Path(args.families).read_text(encoding="utf-8"))
    filters = families_cfg.get("search_filters") or {}
    labels = {f["family_id"]: f.get("label") or f["family_id"]
              for f in families_cfg.get("families") or []}

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    conn.executescript(QUERY_HITS_SCHEMA)
    # 迁移：既有库补 demand_key 列（幂等）
    cols = {r[1] for r in conn.execute("PRAGMA table_info(query_hits)")}
    if "demand_key" not in cols:
        conn.execute(
            "ALTER TABLE query_hits ADD COLUMN demand_key TEXT NOT NULL DEFAULT ''")
        conn.commit()
    out_dir = Path(args.out_root)
    out_dir.mkdir(parents=True, exist_ok=True)

    for item in demand.get("demands") or []:
        family = str(item.get("family") or "")
        if not family:
            continue
        demand_key = str(item.get("demand_key") or f"family:{family}")
        # P3.2（§3.2）：冷却读搜索尝试记录（零命中也冷却），不只看命中
        recent = conn.execute(
            "SELECT COUNT(*) FROM search_attempts WHERE demand_key=?"
            " AND state IN ('done','error') AND started_at >= datetime('now','-1 day')"
            " AND query IN (" + ",".join("?" for _ in (item.get("queries") or [])) + ")",
            [demand_key] + [str(q) for q in (item.get("queries") or [])]).fetchone()[0]
        if recent:
            print(f"  [cooldown] {demand_key} 24h 内已尝试，跳过")
            continue
        print(f"== {family}（{item.get('reason')}）可用存量 {item.get('usable_now')}")
        for query in item.get("queries") or []:
            cursor = conn.execute(
                "INSERT INTO search_attempts (demand_key, query, state)"
                " VALUES (?,?,'running')", (demand_key, query))
            conn.commit()
            attempt_id = cursor.lastrowid
            try:
                feeds = mcp_search(query, filters)
            except Exception as exc:  # noqa: BLE001 - 单查询失败不阻塞族
                conn.execute(
                    "UPDATE search_attempts SET state='error', finished_at="
                    "datetime('now'), error=? WHERE id=?",
                    (str(exc)[:120], attempt_id))
                conn.commit()
                print(f"  [skip] {query}: {exc}")
                continue
            conn.execute(
                "UPDATE search_attempts SET state='done', finished_at="
                "datetime('now'), result_count=? WHERE id=?",
                (len(feeds), attempt_id))
            conn.commit()
            raw_path = out_dir / f"{family}_{query.replace('/', '_')}.json"
            raw_path.write_text(json.dumps(feeds, ensure_ascii=False), encoding="utf-8")
            theme = labels.get(family, family)
            added, dup, skipped = import_feeds(
                conn, feeds, theme, args.limit_per_query)
            for feed in feeds[: args.limit_per_query]:
                note_id = feed.get("id")
                if note_id:
                    conn.execute(
                        "INSERT OR IGNORE INTO query_hits"
                        " (note_id, family, query, demand_key)"
                        " VALUES (?,?,?,?)", (note_id, family, query, demand_key))
            conn.commit()
            print(f"  {query}: 新增 {added}，重复 {dup}，跳过视频 {skipped}")
    # 消费侧以 mode=ro 读库；WAL 模式下 ro 打不开——写完切回 DELETE 日志
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.execute("PRAGMA journal_mode=delete")
    conn.close()


if __name__ == "__main__":
    main()
