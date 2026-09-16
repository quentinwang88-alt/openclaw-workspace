#!/usr/bin/env python3
"""把 xiaohongshu-mcp 搜索结果（out/search/*.json）导入素材库，origin=search。

用法（实验室目录下）：
  python3 scripts/search_to_library.py out/search/travel.json 旅游冬装 [--limit 20]

筛选纪律：只入库图文候选；每主题上限默认 20；跨主题重复按 note_id 自动去重。
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from pathlib import Path

LAB_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = LAB_ROOT / "var" / "material_library.sqlite3"


def parse_cn_count(v) -> int | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return int(v)
    s = str(v).strip()
    m = re.match(r"^([\d.]+)万$", s)
    if m:
        return int(float(m.group(1)) * 10000)
    m = re.match(r"^(\d+)", s)
    return int(m.group(1)) if m else None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("json_file")
    ap.add_argument("theme")
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--limit", type=int, default=20)
    args = ap.parse_args()

    raw = json.loads(Path(args.json_file).read_text(encoding="utf-8"))
    # 兼容两种包装：直接 feeds，或 MCP result.content[0].text 里嵌 JSON
    feeds = raw.get("feeds")
    if feeds is None:
        text = raw["result"]["content"][0]["text"]
        feeds = json.loads(text)["feeds"]

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    added = dup = skipped_video = 0
    for feed in feeds[: args.limit]:
        note_id = feed.get("id")
        token = feed.get("xsecToken")
        card = feed.get("noteCard") or {}
        if card.get("type") == "video":
            skipped_video += 1
            continue
        if not note_id or not token:
            continue
        user = card.get("user") or {}
        interact = card.get("interactInfo") or {}
        url = f"https://www.xiaohongshu.com/explore/{note_id}?xsec_token={token}"
        cur = conn.execute(
            "INSERT INTO notes (note_id, xsec_token, source_url, theme, origin,"
            " title, author_id, author_nickname, like_count, collected_count,"
            " status, fetch_status) VALUES (?,?,?,?, 'search', ?,?,?,?,?,"
            " 'pending_review', 'pending') ON CONFLICT(note_id) DO NOTHING",
            (note_id, token, url, args.theme,
             card.get("displayTitle"), user.get("userId"), user.get("nickname"),
             parse_cn_count(interact.get("likedCount")),
             parse_cn_count(interact.get("collectedCount"))),
        )
        if cur.rowcount:
            added += 1
        else:
            dup += 1
    conn.commit()
    print(f"[{args.theme}] 新增 {added}，跨主题重复 {dup}，跳过视频 {skipped_video}")


if __name__ == "__main__":
    main()
