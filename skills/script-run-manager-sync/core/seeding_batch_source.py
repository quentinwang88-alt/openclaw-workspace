"""Fail-closed one-row-one-script adapter for the organic-seeding workbench."""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Sequence

from core.bitable import TableRecord
from core.sync import (
    SCRIPT_TYPE_SEEDING,
    ScriptSyncTask,
    extract_attachments,
    normalize_checkbox,
    normalize_text,
    normalize_video_duration,
    resolve_field_mapping,
    validate_seeding_publish_guard,
)


SEEDING_SOURCE_FIELD_ALIASES: Dict[str, List[str]] = {
    "script_id": ["种草脚本ID", "脚本ID"],
    "product_code": ["产品编码", "产品ID"],
    "product_images": ["产品图片", "参考图"],
    "first_frame": ["首帧图", "统一首帧（系统）"],
    "store_id": ["店铺ID"],
    "item_index": ["序号", "批次序号"],
    "script_title": ["脚本标题"],
    "product_type": ["产品类型"],
    "target_language": ["目标语言"],
    "business_category": ["一级类目"],
    "video_duration": ["时长", "视频时长"],
    "video_prompt": ["视频提示词", "视频生成提示词", "短视频提示词"],
    "voiceover": ["口播", "目标语言口播"],
    "chinese_voiceover": ["中文口播", "口播中文对照"],
    "sync_enabled": ["进入生产"],
    "publish_policy": ["发布策略"],
    "sync_status": ["同步结果"],
    "sync_time": ["同步时间"],
    "processing_status": ["处理状态"],
    "run_task_id": ["运行任务ID"],
}


def _preserve_source_voiceover_payload(
    *,
    target_text: str,
    chinese_translation: str,
    target_language: str,
    duration: int,
    item_index: int = 1,
) -> tuple[str, str]:
    audio_profiles = (
        ("organic_crisp_reveal", ["daily_clean", "crisp_minimal", "light_reveal"]),
        ("organic_light_tension", ["light_tension", "soft_feminine", "clean_pulse"]),
        ("organic_editorial_stance", ["editorial_clean", "warm_minimal", "subtle_pulse"]),
        ("organic_clean_explainer", ["clean_explainer", "daily_clean", "light_texture"]),
        ("organic_forward_motion", ["forward_motion", "fresh_outdoor", "light_rhythm"]),
    )
    selector_profile, moods = audio_profiles[(max(1, int(item_index)) - 1) % len(audio_profiles)]
    contract = {
        "schema_version": "voiceover-expression-contract-v2",
        "source_kind": "organic_seeding_approved_script",
        "target_language": target_language,
        "speech_policy": {
            "generic_cta_required": False,
            "preserve_source_wording": True,
            "rewrite_allowed": False,
        },
        "forbidden_leaps": [
            "不得改写、扩写或重新生成已经人工勾选进入生产的种草口播",
        ],
    }
    plan = {
        "schema_version": "voiceover-execution-plan-v1",
        "mode": "PRESERVE_SOURCE_COPY",
        "reuse_source_copy": True,
        "copy_authority": "SOURCE_APPROVED_REUSE",
        "source_kind": "organic_seeding_approved_script",
        "target_text": target_text,
        "chinese_translation": chinese_translation,
        "lines": [
            {
                "start_ms": 0,
                "end_ms": int(max(1, duration) * 1000),
                "voiceover_text_target_language": target_text,
                "voiceover_text_zh": chinese_translation,
            }
        ],
        "audio_policy": {
            "source_audio": "duck",
            "voiceover": "tts_from_preserved_source_copy",
            "bgm": {
                "mode": "REQUIRED_BED",
                "selector_profile": selector_profile,
                "mood": moods,
                "energy": "low_to_medium",
                "energy_envelope": [
                    {"phase": "OPENING", "energy": "medium", "purpose": "前1秒建立轻量听觉入口"},
                    {"phase": "VOICEOVER_BED", "energy": "low", "purpose": "口播期间自动压低"},
                    {"phase": "PAYOFF", "energy": "medium", "purpose": "揭示或观点落点轻微回升"},
                ],
                "opening_accent": "subtle_percussion_or_environment_timbre_change",
                "forbidden_styles": ["commercial_whoosh", "hard_sell_transition", "festival_edm_drop"],
                "vocal_type": "instrumental",
                "duck_under_voiceover": True,
            },
        },
    }
    return (
        json.dumps(contract, ensure_ascii=False, separators=(",", ":")),
        json.dumps(plan, ensure_ascii=False, separators=(",", ":")),
    )


def resolve_seeding_batch_field_mapping(field_names: Sequence[str]) -> Dict[str, Optional[str]]:
    return resolve_field_mapping(field_names, SEEDING_SOURCE_FIELD_ALIASES)


def build_seeding_sync_tasks(
    records: Sequence[TableRecord],
    mapping: Dict[str, Optional[str]],
    *,
    product_code: Optional[str] = None,
    record_id: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[ScriptSyncTask]:
    tasks: List[ScriptSyncTask] = []
    for record in records:
        if record_id and record.record_id != record_id:
            continue
        fields = record.fields
        enabled = normalize_checkbox(fields.get(mapping.get("sync_enabled"))) if mapping.get("sync_enabled") else False
        if not enabled:
            continue
        if not mapping.get("publish_policy"):
            raise ValueError("SEEDING_PUBLISH_POLICY_UNAVAILABLE:publish_policy")
        if normalize_text(fields.get(mapping["publish_policy"])) != "种草不挂车":
            raise ValueError("SEEDING_PUBLISH_POLICY_INVALID")
        code = normalize_text(fields.get(mapping.get("product_code"))) if mapping.get("product_code") else ""
        if product_code and code != product_code:
            continue
        script_id = normalize_text(fields.get(mapping.get("script_id"))) if mapping.get("script_id") else ""
        prompt = normalize_text(fields.get(mapping.get("video_prompt"))) if mapping.get("video_prompt") else ""
        voiceover = normalize_text(fields.get(mapping.get("voiceover"))) if mapping.get("voiceover") else ""
        chinese_voiceover = normalize_text(fields.get(mapping.get("chinese_voiceover"))) if mapping.get("chinese_voiceover") else ""
        if not code or not script_id or not prompt or not voiceover:
            raise ValueError("SEEDING_SCRIPT_HANDOFF_INCOMPLETE")
        target_language = normalize_text(fields.get(mapping.get("target_language"))) if mapping.get("target_language") else ""
        video_duration = normalize_video_duration(
            fields.get(mapping.get("video_duration")) if mapping.get("video_duration") else None
        )
        index_text = normalize_text(fields.get(mapping.get("item_index"))) if mapping.get("item_index") else ""
        try:
            item_index = int(float(index_text))
        except (TypeError, ValueError):
            item_index = len(tasks) + 1
        voiceover_contract, voiceover_plan = _preserve_source_voiceover_payload(
            target_text=voiceover,
            chinese_translation=chinese_voiceover,
            target_language=target_language,
            duration=video_duration,
            item_index=item_index,
        )
        first_frame_images = (
            extract_attachments(fields.get(mapping.get("first_frame")))
            if mapping.get("first_frame") else []
        )
        product_images = (
            extract_attachments(fields.get(mapping.get("product_images")))
            if mapping.get("product_images") else []
        )
        task = ScriptSyncTask(
            source_record_id=record.record_id,
            product_code=code,
            script_slot=f"D{item_index:02d}",
            task_name=f"{code}.{script_id}",
            prompt_text=prompt,
            reference_images=first_frame_images or product_images,
            internal_script_key=f"seeding:{record.record_id}:{script_id}",
            product_type=normalize_text(fields.get(mapping.get("product_type"))) if mapping.get("product_type") else "",
            target_language=target_language,
            business_category=normalize_text(fields.get(mapping.get("business_category"))) if mapping.get("business_category") else "",
            script_id=script_id,
            short_video_title=normalize_text(fields.get(mapping.get("script_title"))) if mapping.get("script_title") else "",
            store_id=normalize_text(fields.get(mapping.get("store_id"))) if mapping.get("store_id") else "",
            # Product identity stays in product_code/canonical_product_id for
            # analytics.  The shoppable product-id field is intentionally empty.
            product_id="",
            parent_slot=f"SEED{item_index:02d}",
            direction_label="种草内容",
            variant_strength="母版",
            script_source=SCRIPT_TYPE_SEEDING,
            source_script_type=SCRIPT_TYPE_SEEDING,
            publish_purpose="种草",
            cart_enabled="否",
            content_branch="SEEDING_ORGANIC",
            video_duration=video_duration,
            voiceover_expression_contract=voiceover_contract,
            voiceover_execution_plan=voiceover_plan,
            first_frame_strategy=("GENERATED_FIRST_FRAME" if first_frame_images else "PRODUCT_REFERENCE"),
            voiceover_requested=True,
            voiceover_status="待处理",
        )
        validate_seeding_publish_guard(task)
        tasks.append(task)
        if limit is not None and len(tasks) >= limit:
            break
    return tasks
