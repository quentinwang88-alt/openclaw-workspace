#!/usr/bin/env python3
"""Back up and clear product-scoped original-script generation history.

This deliberately does not delete operation task rows, central selling points,
outfit/scene/structure assets, product anchors, or downstream video run tasks.
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List


SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.storage import default_db_path  # noqa: E402
from scripts.run_feishu_operation_tasks import DEFAULT_SCRIPT_URL, _client  # noqa: E402


PRODUCT_CODE_RE = re.compile(r"^\d{8,30}$")
DEFAULT_BACKUP_ROOT = (
    Path.home() / ".openclaw" / "shared" / "data" / "original_script_cleanup_backups"
)


def _codes(values: Iterable[str]) -> List[str]:
    result = sorted({str(value or "").strip() for value in values})
    if not result or any(not PRODUCT_CODE_RE.fullmatch(value) for value in result):
        raise ValueError("必须提供一个或多个 8 至 30 位数字产品编码")
    return result


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value, ensure_ascii=False)
        return value
    except TypeError:
        return str(value)


def _local_counts(db_path: Path, codes: List[str]) -> Dict[str, Any]:
    placeholders = ",".join(["?"] * len(codes))
    result: Dict[str, Any] = {"database": str(db_path)}
    with sqlite3.connect(str(db_path), timeout=30) as conn:
        tables = {
            str(row[0])
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        queries = {
            "batches": (
                "original_content_batch",
                f"SELECT COUNT(*) FROM original_content_batch WHERE product_code IN ({placeholders})",
            ),
            "items": (
                "original_content_item",
                f"SELECT COUNT(*) FROM original_content_item WHERE product_code IN ({placeholders})",
            ),
            "creative_patterns": (
                "creative_pattern_usage",
                f"SELECT COUNT(*) FROM creative_pattern_usage WHERE product_code IN ({placeholders})",
            ),
        }
        for key, (table, sql) in queries.items():
            result[key] = (
                int(conn.execute(sql, tuple(codes)).fetchone()[0] or 0)
                if table in tables
                else 0
            )
    return result


def _backup_sqlite(source_path: Path, destination_path: Path) -> None:
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(source_path), timeout=30) as source, sqlite3.connect(
        str(destination_path), timeout=30
    ) as destination:
        source.backup(destination)


def _delete_local_history(db_path: Path, codes: List[str]) -> Dict[str, int]:
    placeholders = ",".join(["?"] * len(codes))
    deleted = {"batches": 0, "items": 0, "creative_patterns": 0}
    with sqlite3.connect(str(db_path), timeout=30) as conn:
        tables = {
            str(row[0])
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        conn.execute("BEGIN IMMEDIATE")
        if "original_content_item" in tables:
            cursor = conn.execute(
                f"DELETE FROM original_content_item WHERE product_code IN ({placeholders})",
                tuple(codes),
            )
            deleted["items"] = int(cursor.rowcount or 0)
        if "original_content_batch" in tables:
            cursor = conn.execute(
                f"DELETE FROM original_content_batch WHERE product_code IN ({placeholders})",
                tuple(codes),
            )
            deleted["batches"] = int(cursor.rowcount or 0)
        if "creative_pattern_usage" in tables:
            cursor = conn.execute(
                f"DELETE FROM creative_pattern_usage WHERE product_code IN ({placeholders})",
                tuple(codes),
            )
            deleted["creative_patterns"] = int(cursor.rowcount or 0)
        conn.commit()
    return deleted


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="备份并清理指定产品的原创脚本与规划历史"
    )
    parser.add_argument("--product-code", action="append", required=True)
    parser.add_argument("--script-url", default=DEFAULT_SCRIPT_URL)
    parser.add_argument("--db-path", default=str(default_db_path()))
    parser.add_argument("--backup-root", default=str(DEFAULT_BACKUP_ROOT))
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)

    codes = _codes(args.product_code)
    db_path = Path(args.db_path).expanduser().resolve()
    client = _client(args.script_url)
    records = [
        record
        for record in client.list_records(page_size=100)
        if str(record.fields.get("产品编码") or "").strip() in codes
    ]
    by_product = Counter(
        str(record.fields.get("产品编码") or "").strip() for record in records
    )
    downstream = [
        record for record in records
        if str(record.fields.get("运行任务ID") or "").strip()
    ]
    before = {
        "product_codes": codes,
        "feishu_records": len(records),
        "feishu_by_product": dict(by_product),
        "downstream_run_task_references": len(downstream),
        "local": _local_counts(db_path, codes),
    }
    print(json.dumps({"mode": "APPLY" if args.apply else "DRY_RUN", "before": before}, ensure_ascii=False, indent=2))
    if not args.apply:
        return 0

    stamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    backup_dir = Path(args.backup_root).expanduser().resolve() / stamp
    backup_dir.mkdir(parents=True, exist_ok=False)
    feishu_backup = [
        {
            "record_id": record.record_id,
            "fields": {key: _json_safe(value) for key, value in record.fields.items()},
        }
        for record in records
    ]
    (backup_dir / "feishu_production_scripts.json").write_text(
        json.dumps(feishu_backup, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    _backup_sqlite(db_path, backup_dir / db_path.name)
    (backup_dir / "manifest_before.json").write_text(
        json.dumps(before, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    feishu_deleted = client.batch_delete_records(
        [record.record_id for record in records]
    )
    local_deleted = _delete_local_history(db_path, codes)
    result = {
        "backup_dir": str(backup_dir),
        "feishu_deleted": feishu_deleted,
        "local_deleted": local_deleted,
        "downstream_run_tasks_untouched": len(downstream),
        "operation_tasks_untouched": True,
        "central_selling_points_untouched": True,
        "product_anchors_untouched": True,
        "shared_structure_scene_outfit_assets_untouched": True,
    }
    (backup_dir / "cleanup_result.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps({"result": result}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
