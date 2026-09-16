#!/usr/bin/env python3
"""把素材库待审候选同步到飞书审核表（用户新建的独立表，非生产表）。

流程：确保字段 → 幂等写入 pending_review 候选 → 已抓取笔记补首图封面 → 回读校验。
幂等：按「笔记ID」字段去重，重复运行不产生重复行。
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

LAB_ROOT = Path(__file__).resolve().parent.parent
WORKSPACE = LAB_ROOT.parent.parent
sys.path.insert(0, str(WORKSPACE / "skills" / "script-run-manager-sync" / "core"))

from bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # noqa: E402

WIKI_TOKEN = "TV1fwhpj7il85VkfBkZciwVBn4g"
TABLE_ID = "tblrb90WgyvV64tL"
DB = LAB_ROOT / "var" / "material_library.sqlite3"
OUT = LAB_ROOT / "out"

TEXT, NUMBER, SINGLE_SELECT, URL, ATTACHMENT = 1, 2, 3, 15, 17

FIELDS = [
    ("素材标题", TEXT, "Text", None),
    ("笔记ID", TEXT, "Text", None),
    ("主题", SINGLE_SELECT, "SingleSelect", {"options": [
        {"name": "旅游冬装"}, {"name": "显高搭配"}, {"name": "轻上装"}, {"name": "链接收录"},
    ]}),
    ("审核状态", SINGLE_SELECT, "SingleSelect", {"options": [
        {"name": "待审核"}, {"name": "已选用"}, {"name": "已淘汰"},
    ]}),
    ("抓取状态", SINGLE_SELECT, "SingleSelect", {"options": [
        {"name": "待抓取"}, {"name": "已抓取"}, {"name": "部分"}, {"name": "失败"},
    ]}),
    ("作者", TEXT, "Text", None),
    ("点赞", NUMBER, "Number", {"formatter": "0"}),
    ("收藏", NUMBER, "Number", {"formatter": "0"}),
    ("笔记链接", URL, "Url", None),
    ("素材图片", ATTACHMENT, "Attachment", None),
    ("备注", TEXT, "Text", None),
]


def ensure_fields(client: FeishuBitableClient) -> None:
    existing = {f.field_name: f for f in client.list_fields()}
    for name, ftype, ui, prop in FIELDS:
        if name in existing:
            continue
        # 主字段（表里第一个 text 字段，通常叫「文本」）改名复用，避免出现两个标题列
        primary = next((f for f in existing.values()
                        if f.field_type == TEXT and f.field_name in ("文本", "标题", "Title")), None)
        if ftype == TEXT and primary is not None and name == "素材标题":
            client.update_field(primary.field_id, field_name=name,
                                field_type=primary.field_type)
            existing[name] = primary
            print(f"字段：主字段「{primary.field_name}」改名 →「{name}」")
            continue
        client.create_field(name, ftype, ui, prop)
        print(f"字段：新建「{name}」")


def load_candidates() -> list[sqlite3.Row]:
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn.execute(
        "SELECT id, note_id, source_url, theme, title, author_nickname,"
        " like_count, collected_count, fetch_status, image_count"
        " FROM notes WHERE status='pending_review' AND origin='search' ORDER BY id"
    ).fetchall()


def main() -> None:
    client = FeishuBitableClient(resolve_wiki_bitable_app_token(WIKI_TOKEN), TABLE_ID)
    ensure_fields(client)

    candidates = load_candidates()
    print(f"待审候选：{len(candidates)} 条")

    existing_records = client.list_records(page_size=500)
    by_note_id = {}
    for rec in existing_records:
        v = rec.fields.get("笔记ID")
        if isinstance(v, list):
            v = v[0].get("text") if v else None
        if v:
            by_note_id[str(v)] = rec.record_id

    to_create = [c for c in candidates if c["note_id"] not in by_note_id]
    print(f"表内已有 {len(by_note_id)} 条，本次新建 {len(to_create)} 条")

    conn = sqlite3.connect(DB)
    for start in range(0, len(to_create), 20):
        chunk = to_create[start:start + 20]
        records = [{"fields": rec} for rec in (
            {k: v for k, v in rec.items() if v is not None} for rec in ({
                "素材标题": c["title"] or f"（无标题）{c['note_id']}",
                "笔记ID": c["note_id"],
                "主题": c["theme"],
                "审核状态": "待审核",
                "抓取状态": {"pending": "待抓取", "fetched": "已抓取",
                            "partial": "部分", "failed": "失败"}.get(c["fetch_status"], "待抓取"),
                "作者": c["author_nickname"] or "",
                "点赞": c["like_count"],
                "收藏": c["collected_count"],
                "笔记链接": {"text": "打开笔记", "link": c["source_url"]},
            } for c in chunk)
        )]
        record_ids = client.batch_create_records(records)
        for c, rid in zip(chunk, record_ids):
            conn.execute("UPDATE notes SET feishu_record_id=? WHERE id=?", (rid, c["id"]))
        conn.commit()
        print(f"  写入 {len(record_ids)} 条")

    # 已抓取的补首图封面
    conn.row_factory = sqlite3.Row
    for c in to_create:
        if c["fetch_status"] != "fetched":
            continue
        cover = conn.execute(
            "SELECT file_path FROM note_images WHERE note_pk=? AND seq=1", (c["id"],)
        ).fetchone()
        if not cover:
            continue
        path = OUT / cover["file_path"]
        if not path.exists():
            continue
        rid = conn.execute(
            "SELECT feishu_record_id FROM notes WHERE id=?", (c["id"],)
        ).fetchone()[0]
        if not rid:
            continue
        att = client.upload_attachment(
            path.read_bytes(), path.name, "image/jpeg", path.stat().st_size)
        client.update_record_fields(rid, {"素材图片": [att]})
        print(f"  封面：{c['note_id']} ← {path.name}")

    # 回读校验
    final = client.list_records(page_size=500)
    with_cover = sum(1 for r in final if r.fields.get("素材图片"))
    with_status = sum(1 for r in final if r.fields.get("审核状态"))
    print(f"校验：表内 {len(final)} 行，审核状态已填 {with_status}，带封面 {with_cover}")


if __name__ == "__main__":
    main()
