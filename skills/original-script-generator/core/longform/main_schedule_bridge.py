"""Register a completed long-form original in the existing organic scheduler."""
from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, Mapping


WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
PUBLISHER_ROOT = WORKSPACE_ROOT / "skills" / "short-video-auto-publisher"
if str(PUBLISHER_ROOT) not in sys.path:
    sys.path.insert(0, str(PUBLISHER_ROOT))

from app.db import AutoPublishDB  # noqa: E402
from app.models import ScriptMetadata  # noqa: E402


class LongformScheduleBridgeError(RuntimeError):
    pass


def _text(value: Any) -> str:
    return str(value or "").strip()


def _publish_title(fields: Mapping[str, Any]) -> str:
    # The target-language voiceover is already frozen production copy and is
    # safer than inventing another localized caption at this boundary.
    source = _text(fields.get("口播_目标语言")) or _text(fields.get("脚本标题"))
    source = re.sub(r"\s+", " ", source)
    if not source:
        raise LongformScheduleBridgeError("缺少目标语言口播，不能自动形成发布标题")
    sentence = re.split(r"(?<=[.!?。！？])\s*", source, maxsplit=1)[0]
    return (sentence or source)[:150].strip()


def enqueue_longform_final(
    *,
    record_id: str,
    job_id: str,
    final_video_path: str | Path,
    fields: Mapping[str, Any],
    db: AutoPublishDB | None = None,
) -> Dict[str, str]:
    video = Path(final_video_path).expanduser().resolve()
    if not video.is_file():
        raise LongformScheduleBridgeError(f"长视频成片不存在：{video}")
    store_id = _text(fields.get("店铺ID"))
    country = _text(fields.get("目标国家")).upper()
    if not store_id or not country:
        raise LongformScheduleBridgeError("自动排班需要店铺ID和目标国家")

    product_code = _text(fields.get("产品编码"))
    canonical_key = f"longform:{job_id}"
    title = _publish_title(fields)
    digest = hashlib.sha256(video.read_bytes()).hexdigest()
    context = {
        "schema_version": "longform-main-publish-v1",
        "audio_mode": "embedded_voiceover_platform_bgm_low",
        "video_duration_ms": int(float(fields.get("视频时长") or 25) * 1000),
        "source_product_id": product_code,
        "bgm_mood_hints": ["fashion", "lifestyle"],
        "bgm_rhythm_preference": "medium",
        "feishu_record_id": record_id,
        "longform_job_id": job_id,
    }
    database = db or AutoPublishDB()
    database.upsert_script_metadata([ScriptMetadata(
        canonical_script_key=canonical_key,
        script_id=job_id,
        source_record_id=record_id,
        script_slot=f"LONGFORM:{job_id}",
        task_no=job_id,
        store_id=store_id,
        product_id="",
        parent_slot="LONGFORM",
        direction_label=_text(fields.get("结构家族")) or "原创长视频",
        variant_strength="成片",
        target_country=country,
        product_type=_text(fields.get("产品类型")) or "apparel",
        content_family_key=f"longform-product:{product_code or job_id}",
        script_text=json.dumps(context, ensure_ascii=False, sort_keys=True),
        short_video_title=title,
        title_source="target_voiceover",
        script_source="原创长视频",
        publish_purpose="养号",
        cart_enabled="否",
        content_branch="非商品展示型",
        audio_mode=context["audio_mode"],
    )])
    database.upsert_video_asset(
        canonical_script_key=canonical_key,
        script_id=job_id,
        run_manager_record_id=record_id,
        video_source_type="longform_original_final",
        video_source_value=str(video),
        local_file_path=str(video),
        download_status="下载成功",
        run_video_status="已完成",
        publish_status="待排期",
    )
    return {
        "canonical_script_key": canonical_key,
        "store_id": store_id,
        "status": "待排期",
        "sha256": digest,
    }
