#!/usr/bin/env python3
"""试点抓取骨架：链接 → XHS-Downloader 下载 → 写入本地素材库。

【骨架说明】首次真实运行时需要按实际返回结构微调：
  - xhs.extract 的返回字典键名（raw 数据先整体存档到 out/<note_id>/raw.json 再解析）
  - 下载文件名到图片顺序 seq 的映射（当前按文件名排序）

运行方式（用 XHS-Downloader 自带的独立虚拟环境，不动其它任何环境）：
  cd labs/xhs-material-lab
  uv run --project third_party/XHS-Downloader python scripts/fetch_notes.py --limit 5

安全规则：
  - 每次运行条数有上限（默认 5），小批量手动触发，无定时、无自动重试；
  - 连续 3 条失败即中止整批（疑似登录态/风控问题），打印人工处理提示；
  - 只写实验室本地 sqlite 与 out/，不触飞书、RDS、OPV 产物目录。
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
import sqlite3
import sys
from pathlib import Path

LAB_ROOT = Path(__file__).resolve().parent.parent
PROJECT_DIR = LAB_ROOT / "third_party" / "XHS-Downloader"
DEFAULT_DB = LAB_ROOT / "var" / "material_library.sqlite3"
OUT_ROOT = LAB_ROOT / "out"
sys.path.insert(0, str(PROJECT_DIR))

from source import XHS  # noqa: E402  (依赖实验室独立 venv)


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_cn_count(v) -> int | None:
    """点赞数实测可能是 '1.5万' 这类字符串。"""
    if isinstance(v, (int, float)):
        return int(v)
    if isinstance(v, str):
        m = re.match(r"^([\d.]+)万$", v.strip())
        if m:
            return int(float(m.group(1)) * 10000)
        m = re.match(r"^(\d+)", v.strip())
        if m:
            return int(m.group(1))
    return None


def discover_note_files(note_dir: Path):
    """按文件名里的数字序号还原组图顺序（不能按字典序，_10 会排到 _2 前面）。"""
    items = []
    if note_dir.exists():
        for p in sorted(note_dir.iterdir()):
            if p.suffix.lower() not in {".jpg", ".jpeg", ".png", ".webp"}:
                continue
            m = re.search(r"_(\d+)$", p.stem)
            items.append((int(m.group(1)) if m else 0, p))
    items.sort(key=lambda t: t[0])
    return items


async def fetch_one(xhs: XHS, db: sqlite3.Connection, row: sqlite3.Row) -> bool:
    note_pk, url = row["id"], row["source_url"]
    data = await xhs.extract(url, True)  # 失败时返回空字典
    if isinstance(data, list):           # 实测某些跳过场景返回 [{}] 或 []
        data = data[0] if data else {}
    if not data:
        db.execute(
            "UPDATE notes SET fetch_status='failed', fetch_error='extract 返回空"
            "（token 失效、app_share 上下文 token 或内容不可访问）',"
            " updated_at=datetime('now') WHERE id=?",
            (note_pk,),
        )
        return False

    note_id = str(row["note_id"] or data.get("作品ID") or f"pk{note_pk}")
    note_dir = OUT_ROOT / "download" / note_id
    note_dir.mkdir(parents=True, exist_ok=True)
    raw_path = note_dir / "raw.json"
    raw_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    files = discover_note_files(note_dir)
    expected = len(data.get("下载地址") or [])
    images_ok = images_dup = 0
    for seq, path in files:
        digest = sha256_of(path)
        dup = db.execute(
            "SELECT 1 FROM note_images WHERE sha256=?", (digest,)
        ).fetchone()
        if dup:
            images_dup += 1
            continue
        db.execute(
            "INSERT OR REPLACE INTO note_images"
            " (note_pk, seq, file_path, sha256, status) VALUES (?,?,?,?,'ok')",
            (note_pk, seq, str(path.relative_to(OUT_ROOT)), digest),
        )
        images_ok += 1

    if not files:
        status, err = "partial", (
            f"作品类型={data.get('作品类型')}（视频作品或下载被跳过，试点只收图文）"
        )
    elif expected and len(files) < expected:
        status, err = "partial", f"图片不完整：磁盘 {len(files)}/{expected}"
    else:
        status, err = "fetched", None

    db.execute(
        "UPDATE notes SET fetch_status=?, image_count=?, fetch_error=?,"
        " raw_json_path=?, title=?, description=?, author_id=?, author_nickname=?,"
        " like_count=?, collected_count=?, published_at=?,"
        " updated_at=datetime('now') WHERE id=?",
        (status, len(files), err, str(raw_path),
         data.get("作品标题"), data.get("作品描述"),
         data.get("作者ID"), data.get("作者昵称"),
         parse_cn_count(data.get("点赞数量")), parse_cn_count(data.get("收藏数量")),
         data.get("发布时间"), note_pk),
    )
    db.execute(
        "INSERT INTO fetch_runs (tool, mode, notes_ok, images_ok, images_failed, log)"
        " VALUES ('xhs-downloader', 'link_intake', 1, ?, 0, ?)",
        (images_ok, f"note={note_id} files={len(files)} expected={expected}"
                    f" images={images_ok} dup={images_dup}"),
    )
    print(f"  ✓ note={note_id} 图片 {len(files)}/{expected or '?'} 张"
          f"（新入库 {images_ok}，重复跳过 {images_dup}）")
    return True


async def run(args) -> int:
    db = sqlite3.connect(args.db)
    db.row_factory = sqlite3.Row
    db.executescript((LAB_ROOT / "schema.sql").read_text(encoding="utf-8"))
    rows = db.execute(
        "SELECT * FROM notes WHERE fetch_status='pending' AND source_url IS NOT NULL"
        " ORDER BY id LIMIT ?",
        (args.limit,),
    ).fetchall()
    if not rows:
        print("没有待抓取的链接。")
        return 0

    ok = fail = consecutive_fail = 0
    async with XHS(
        work_path=str(OUT_ROOT),
        folder_name="download",
        name_format="作品ID",
        image_format="JPEG",
        image_download=True,
        video_download=False,          # 穿搭素材只要图
        video_cover_download=False,
        live_download=False,
        download_record=True,          # 工具自带下载去重（ExploreID.db）
        folder_mode=True,
        record_data=False,
    ) as xhs:
        for row in rows:
            print(f"抓取 {row['note_id'] or row['source_url']} ...")
            try:
                if await fetch_one(xhs, db, row):
                    ok += 1
                    consecutive_fail = 0
                else:
                    fail += 1
                    consecutive_fail += 1
            except Exception as exc:  # 单条失败不断批，但记录并计数
                fail += 1
                consecutive_fail += 1
                db.execute(
                    "UPDATE notes SET fetch_status='failed', fetch_error=?,"
                    " updated_at=datetime('now') WHERE id=?",
                    (f"{type(exc).__name__}: {exc}", row["id"]),
                )
                print(f"  ✗ 异常：{exc}")
            db.commit()
            if consecutive_fail >= 3:
                print("\n⚠️ 连续 3 条失败，按暂停规则中止本批。")
                print("   请人工检查登录态/网络/链接有效性（不要直接自动重试），排除后再跑。")
                break

    print(f"\n本批完成：成功 {ok}，失败 {fail}，共 {len(rows)} 条。")
    print("复核：python3 scripts/material_lab.py status")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--limit", type=int, default=5, help="单批最多抓取条数")
    args = ap.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
