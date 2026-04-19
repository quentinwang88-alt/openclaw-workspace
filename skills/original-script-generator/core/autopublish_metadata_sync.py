#!/usr/bin/env python3
"""把已生成完成的原创脚本记录同步到自动发布主数据库。"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from core.bitable import TaskRecord


def _load_module(module_path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载模块: {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _auto_publish_skill_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "short-video-auto-publisher"


def sync_record_to_auto_publish_db(
    *,
    client: Any,
    record_id: str,
    metadata_db_path: Optional[str] = None,
) -> int:
    skill_dir = _auto_publish_skill_dir()
    app_dir = skill_dir / "app"
    if str(skill_dir) not in sys.path:
        sys.path.insert(0, str(skill_dir))
    metadata_module = _load_module(app_dir / "metadata.py", "short_video_auto_publisher_app_metadata")
    db_module = _load_module(app_dir / "db.py", "short_video_auto_publisher_app_db")

    field_names = client.list_field_names()
    mapping: Dict[str, Optional[str]] = metadata_module.resolve_field_mapping(field_names, metadata_module.SOURCE_FIELD_ALIASES)
    records = client.list_records(page_size=100)
    target_record = next((record for record in records if record.record_id == record_id), None)
    if target_record is None:
        raise RuntimeError(f"飞书记录不存在: {record_id}")

    title_generator = metadata_module.HeuristicTitleGenerator()
    metadata_items = metadata_module.build_script_metadata_records(
        [TaskRecord(record_id=target_record.record_id, fields=target_record.fields)],
        mapping,
        title_generator=title_generator,
        record_id=record_id,
    )
    db_path = Path(metadata_db_path) if metadata_db_path else db_module.default_db_path()
    db = db_module.AutoPublishDB(db_path)
    merged_items = []
    for item in metadata_items:
        existing = db.get_script_metadata(item.script_id)
        if existing and str(existing["short_video_title"] or "").strip():
            merged_items.append(
                metadata_module.ScriptMetadata(
                    **{
                        **item.__dict__,
                        "short_video_title": str(existing["short_video_title"] or "").strip(),
                        "title_source": str(existing["title_source"] or "").strip() or item.title_source,
                    }
                )
            )
            continue
        merged_items.append(item)
    return db.upsert_script_metadata(merged_items)


def default_metadata_db_path() -> Path:
    override = os.environ.get("SHORT_VIDEO_AUTO_PUBLISH_DB_PATH")
    if override:
        return Path(override)
    return Path.home() / ".openclaw" / "shared" / "data" / "short_video_auto_publish.sqlite3"
