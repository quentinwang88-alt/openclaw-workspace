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

LAB_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(LAB_ROOT))
DEFAULT_DB = LAB_ROOT / "var" / "material_library.sqlite3"
DEFAULT_FAMILIES = LAB_ROOT / "config" / "query_families.json"
MCP_BASE = "http://127.0.0.1:18060/mcp/v1/search_feeds"

QUERY_HITS_SCHEMA = """
CREATE TABLE IF NOT EXISTS query_hits (
    note_id TEXT NOT NULL,
    family TEXT NOT NULL,
    query TEXT NOT NULL,
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


def import_feeds(conn: sqlite3.Connection, feeds, theme: str, limit: int):
    """与 search_to_library.py 同构的入库（note_id 去重、跳过视频）。"""
    added = dup = skipped = 0
    for feed in feeds[:limit]:
        note_id = feed.get("id")
        token = feed.get("xsecToken")
        card = feed.get("noteCard") or {}
        if card.get("type") == "video":
            skipped += 1
            continue
        if not note_id or not token:
            continue
        user = card.get("user") or {}
        interact = card.get("interactInfo") or {}
        like = _parse_count(interact.get("likedCount"))
        collected = _parse_count(interact.get("collectedCount"))
        url = f"https://www.xiaohongshu.com/explore/{note_id}?xsec_token={token}"
        cur = conn.execute(
            "INSERT INTO notes (note_id, xsec_token, source_url, theme, origin,"
            " title, author_id, author_nickname, like_count, collected_count,"
            " status, fetch_status) VALUES (?,?,?,?, 'search', ?,?,?,?,?,"
            " 'pending_review', 'pending') ON CONFLICT(note_id) DO NOTHING",
            (note_id, token, url, theme, card.get("displayTitle"),
             user.get("userId"), user.get("nickname"), like, collected))
        if cur.rowcount:
            added += 1
        else:
            dup += 1
    return added, dup, skipped


def _parse_count(value):
    import re
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    s = str(value).strip()
    m = re.match(r"^([\d.]+)万$", s)
    if m:
        return int(float(m.group(1)) * 10000)
    m = re.match(r"^(\d+)", s)
    return int(m.group(1)) if m else None


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
    out_dir = Path(args.out_root)
    out_dir.mkdir(parents=True, exist_ok=True)

    for item in demand.get("demands") or []:
        family = str(item.get("family") or "")
        if not family:
            continue
        print(f"== {family}（{item.get('reason')}）可用存量 {item.get('usable_now')}")
        for query in item.get("queries") or []:
            try:
                feeds = mcp_search(query, filters)
            except Exception as exc:  # noqa: BLE001 - 单查询失败不阻塞族
                print(f"  [skip] {query}: {exc}")
                continue
            raw_path = out_dir / f"{family}_{query.replace('/', '_')}.json"
            raw_path.write_text(json.dumps(feeds, ensure_ascii=False), encoding="utf-8")
            theme = labels.get(family, family)
            added, dup, skipped = import_feeds(
                conn, feeds, theme, args.limit_per_query)
            for feed in feeds[: args.limit_per_query]:
                note_id = feed.get("id")
                if note_id:
                    conn.execute(
                        "INSERT OR IGNORE INTO query_hits (note_id, family, query)"
                        " VALUES (?,?,?)", (note_id, family, query))
            conn.commit()
            print(f"  {query}: 新增 {added}，重复 {dup}，跳过视频 {skipped}")
    # 消费侧以 mode=ro 读库；WAL 模式下 ro 打不开——写完切回 DELETE 日志
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.execute("PRAGMA journal_mode=delete")
    conn.close()


if __name__ == "__main__":
    main()
