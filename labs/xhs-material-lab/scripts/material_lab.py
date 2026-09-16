#!/usr/bin/env python3
"""小红书素材实验室 · 本地素材库小工具（仅标准库，python3.9+ 可跑）。

用法（均在 labs/xhs-material-lab/ 下执行）：
  python3 scripts/material_lab.py init
  python3 scripts/material_lab.py add-links --theme "旅游冬装" --file links.txt
  python3 scripts/material_lab.py add-links --theme "显高搭配" <链接1> <链接2>
  python3 scripts/material_lab.py status

只操作本实验室自己的 sqlite（var/material_library.sqlite3），
不触任何飞书表、RDS、OPV 产物目录。
"""
from __future__ import annotations

import argparse
import re
import sqlite3
import sys
import urllib.error
import urllib.request
from pathlib import Path

LAB_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = LAB_ROOT / "var" / "material_library.sqlite3"
SCHEMA = LAB_ROOT / "schema.sql"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
      " (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36")

NOTE_ID_RE = re.compile(r"[0-9a-fA-F]{24}")
NOTE_LINK_RE = re.compile(r"xiaohongshu\.com/(?:explore|discovery/item)/([0-9a-fA-F]{24})")
PROFILE_LINK_RE = re.compile(r"xiaohongshu\.com/user/profile/")
SHORT_RE = re.compile(r"https?://(www\.)?xhslink\.com/\S+")


def parse_link(url: str) -> dict:
    """从粘贴的分享链接提取 note_id 与 xsec_token；短链先原样入库。"""
    item = {"note_id": None, "xsec_token": None, "source_url": url.strip()}
    if SHORT_RE.search(url):
        return item
    ids = NOTE_ID_RE.findall(url)
    if ids:
        # explore/<id>、discovery/item/<id>、user/profile/<uid>/<nid> —— 笔记 ID 恒为最后一个
        item["note_id"] = ids[-1].lower()
    m = re.search(r"[?&]xsec_token=([^&\s]+)", url)
    if m:
        item["xsec_token"] = m.group(1)
    return item


def open_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def cmd_init(args) -> None:
    conn = open_db(Path(args.db))
    conn.executescript(SCHEMA.read_text(encoding="utf-8"))
    conn.commit()
    print(f"素材库已初始化：{args.db}")


def cmd_add_links(args) -> None:
    links = list(args.links)
    if args.file:
        text = Path(args.file).read_text(encoding="utf-8")
        links += [ln.strip() for ln in re.split(r"[\s,;]+", text) if ln.strip()]
    if not links:
        sys.exit("没有可入库的链接（用位置参数或 --file 提供）")

    conn = open_db(Path(args.db))
    added = dup = 0
    for url in links:
        item = parse_link(url)
        cur = conn.execute(
            "INSERT INTO notes (note_id, xsec_token, source_url, theme, origin)"
            " VALUES (?, ?, ?, ?, 'manual')"
            " ON CONFLICT(note_id) DO NOTHING" if item["note_id"] else
            "INSERT INTO notes (note_id, xsec_token, source_url, theme, origin)"
            " VALUES (?, ?, ?, ?, 'manual')",
            (item["note_id"], item["xsec_token"], item["source_url"], args.theme),
        )
        if cur.rowcount:
            added += 1
        else:
            dup += 1
    conn.commit()
    print(f"新增 {added} 条，重复跳过 {dup} 条，主题：{args.theme or '（未指定）'}")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _expand_short_link(url: str, max_hops: int = 5) -> str:
    """手动逐跳跟随重定向；到达笔记页/作者主页即停，避免被落地页二次 302 掩盖真实目标。"""
    opener = urllib.request.build_opener(_NoRedirect)
    cur = url
    for _ in range(max_hops):
        req = urllib.request.Request(cur, headers={"User-Agent": UA})
        try:
            with opener.open(req, timeout=15) as resp:
                return resp.geturl()
        except urllib.error.HTTPError as e:
            loc = e.headers.get("Location")
            if not loc:
                raise
            cur = urllib.request.urljoin(cur, loc)
            if NOTE_LINK_RE.search(cur) or PROFILE_LINK_RE.search(cur):
                return cur
    return cur


def cmd_resolve_links(args) -> None:
    """展开 xhslink 短链：笔记链→回填 note_id/新 token 并复位待抓；主页分享/死链→记失败原因。"""
    conn = open_db(Path(args.db))
    rows = conn.execute(
        "SELECT id, source_url FROM notes WHERE source_url LIKE '%xhslink.com%'"
        " AND fetch_status IN ('pending','failed')"
    ).fetchall()
    if not rows:
        print("没有待解析的短链。")
        return
    for row in rows:
        try:
            final = _expand_short_link(row["source_url"])
        except Exception as exc:
            conn.execute(
                "UPDATE notes SET fetch_status='failed', fetch_error=?,"
                " updated_at=datetime('now') WHERE id=?",
                (f"短链展开失败: {exc}", row["id"]),
            )
            print(f"  ✗ {row['source_url']} → 展开异常 {exc}")
            continue
        m = NOTE_LINK_RE.search(final)
        if m:
            token = None
            tm = re.search(r"[?&]xsec_token=([^&\s]+)", final)
            if tm:
                token = tm.group(1)
            conn.execute(
                "UPDATE notes SET note_id=?, xsec_token=COALESCE(?, xsec_token),"
                " source_url=?, fetch_status='pending', fetch_error=NULL,"
                " updated_at=datetime('now') WHERE id=?",
                (m.group(1).lower(), token, final, row["id"]),
            )
            print(f"  ✓ {row['source_url']} → 笔记 {m.group(1)}")
        elif PROFILE_LINK_RE.search(final):
            conn.execute(
                "UPDATE notes SET fetch_status='failed', fetch_error='作者主页分享链（非单篇笔记），"
                "试点不收', updated_at=datetime('now') WHERE id=?",
                (row["id"],),
            )
            print(f"  △ {row['source_url']} → 作者主页分享，不收")
        else:
            conn.execute(
                "UPDATE notes SET fetch_status='failed', fetch_error='短链已失效（跳回主页）',"
                " updated_at=datetime('now') WHERE id=?",
                (row["id"],),
            )
            print(f"  ✗ {row['source_url']} → 短链失效")
    conn.commit()


def cmd_status(args) -> None:
    conn = open_db(Path(args.db))
    print("== 笔记状态 ==")
    for row in conn.execute(
        "SELECT status, fetch_status, COUNT(*) n FROM notes GROUP BY status, fetch_status"
    ):
        print(f"  {row['status']:<16} fetch={row['fetch_status']:<8} {row['n']}")
    print("== 图片 ==")
    for row in conn.execute("SELECT status, COUNT(*) n FROM note_images GROUP BY status"):
        print(f"  {row['status']:<10} {row['n']}")
    fails = conn.execute(
        "SELECT note_id, fetch_error FROM notes WHERE fetch_status='failed' ORDER BY id DESC LIMIT 5"
    ).fetchall()
    if fails:
        print("== 最近失败（最多 5 条）==")
        for f in fails:
            print(f"  {f['note_id'] or '（短链/未解析）'}: {f['fetch_error']}")
    runs = conn.execute("SELECT COUNT(*) n FROM fetch_runs").fetchone()["n"]
    print(f"== 抓取批次：{runs} 次 ==")


def main() -> None:
    ap = argparse.ArgumentParser(description="小红书素材实验室素材库工具")
    ap.add_argument("--db", default=str(DEFAULT_DB))
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init").set_defaults(func=cmd_init)
    p_add = sub.add_parser("add-links")
    p_add.add_argument("--file", help="链接清单文件（空白/逗号分隔）")
    p_add.add_argument("--theme", default="")
    p_add.add_argument("links", nargs="*")
    p_add.set_defaults(func=cmd_add_links)
    sub.add_parser("resolve-links").set_defaults(func=cmd_resolve_links)
    sub.add_parser("status").set_defaults(func=cmd_status)
    args = ap.parse_args()
    if args.cmd != "init" and not Path(args.db).exists():
        sys.exit(f"素材库不存在，先执行 init：{args.db}")
    args.func(args)


if __name__ == "__main__":
    main()
