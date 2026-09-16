#!/usr/bin/env python3
"""补跑「完整文案（中文）」回写。

场景：成片已完成但翻译当时失败（字段是占位提示），或历史已完成行需要补
中文。按最终成片包 copy 的指纹读缓存/重译，只写飞书展示字段——不改图片、
不改任务状态、不入队、不发布。

用法：
    python3 scripts/backfill_copy_zh.py --record-id <飞书行ID> [--dry-run]
    python3 scripts/backfill_copy_zh.py --all-pending [--limit 20]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
BITABLE_SKILL = WORKSPACE_ROOT / "skills" / "script-run-manager-sync"
for value in (str(WORKSPACE_ROOT), str(BITABLE_SKILL), str(PACKAGE_ROOT)):
    if value not in sys.path:
        sys.path.insert(0, value)

from workspace_support import load_repo_env  # noqa: E402

load_repo_env()

from core.bitable import (  # noqa: E402
    FeishuBitableClient, resolve_wiki_bitable_app_token,
)
from repositories.rds_repository import RdsRepository  # noqa: E402
from services.copy_translation import (  # noqa: E402
    CopyTranslationError, CopyTranslationService, compose_full_copy_zh,
)
from services.feishu_workflow import FIELD_FULL_COPY_ZH  # noqa: E402


def translate_row(repo, client, record_id: str, root: Path, *, dry_run: bool) -> str:
    tasks = repo.list_tasks_by_source_prefix("feishu_opv_photo", record_id) \
        if hasattr(repo, "list_tasks_by_source_prefix") else []
    if not tasks:
        # 直接按 feishu_record_id 查（含 1:N source_record_id 后缀）。
        tasks = repo._fetch_all(
            "SELECT task_id FROM opv_content_task WHERE feishu_record_id = %s "
            "ORDER BY task_id", (record_id,))
    if not tasks:
        return f"{record_id}: 找不到任务"
    service = CopyTranslationService(root=root)
    parts = []
    total = len(tasks)
    for index, task_row in enumerate(tasks, 1):
        task = repo.get_task(task_row["task_id"] if isinstance(task_row, dict) else task_row.task_id)
        package = (repo.get_content_package(task.content_package_id)
                   if task.content_package_id else None)
        copy_block = dict((getattr(package, "photo_manifest_json", None) or {}).get("copy") or {})
        if not copy_block:
            continue
        try:
            translation = service.translate(copy_block, source_locale=task.target_locale)
        except CopyTranslationError as exc:
            return f"{record_id}: 第 {index} 套翻译失败：{exc}"
        parts.append(compose_full_copy_zh(translation, set_index=index, total_sets=total))
    if not parts:
        return f"{record_id}: 没有可翻译的成片文案"
    if dry_run:
        return f"{record_id} (dry-run):\n" + "\n\n".join(parts)
    fields = client.get_record(record_id).fields
    client.update_record_fields(record_id, {FIELD_FULL_COPY_ZH: "\n\n".join(parts)})
    return f"{record_id}: 已回写 {len(parts)} 套中文文案"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wiki-token", default="TR10wxEXHiCYIhk8clActVdenpc")
    parser.add_argument("--table-id", default="tblj3x846gU3rshB")
    parser.add_argument("--record-id", default="")
    parser.add_argument("--all-pending", action="store_true",
                        help="扫描最近已完成、字段为空或为占位的行")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    client = FeishuBitableClient(resolve_wiki_bitable_app_token(args.wiki_token), args.table_id)
    repo = RdsRepository.from_env()
    root = Path.home() / ".openclaw/shared/data/organic_photo_video"
    if args.record_id:
        print(translate_row(repo, client, args.record_id, root, dry_run=args.dry_run))
        return 0
    if not args.all_pending:
        parser.error("需要 --record-id 或 --all-pending")
    handled = 0
    for record in client.list_records(page_size=500):
        current = str(record.fields.get(FIELD_FULL_COPY_ZH) or "")
        progress = str((record.fields.get("进度") or ""))
        if progress != "已完成" or (current and not current.startswith("（中文翻译待补跑")):
            continue
        print(translate_row(repo, client, record.record_id, root, dry_run=args.dry_run))
        handled += 1
        if handled >= args.limit:
            break
    print(f"处理行数：{handled}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
