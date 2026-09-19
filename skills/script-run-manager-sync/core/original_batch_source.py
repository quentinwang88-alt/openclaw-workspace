"""One-row-one-script source adapter for the original production workbench."""
from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional, Sequence

from core.bitable import TableRecord
from core.sync import (
    ScriptSyncTask,
    extract_attachments,
    normalize_checkbox,
    normalize_text,
    normalize_video_duration,
    resolve_field_mapping,
)
from core.production_route import ProductionRoute, classify_production_route


ORIGINAL_BATCH_SOURCE_FIELD_ALIASES: Dict[str, List[str]] = {
    "script_id": ["脚本ID"],
    "product_code": ["产品编码", "产品ID"],
    "product_images": ["产品图片", "参考图"],
    "store_id": ["店铺ID"],
    "batch_id": ["批次ID"],
    "batch_item_id": ["批次ItemID"],
    "item_index": ["批次序号"],
    "script_title": ["脚本标题"],
    "publish_title": ["发布标题", "短视频标题"],
    "product_type": ["产品类型"],
    "target_language": ["目标语言"],
    "business_category": ["一级类目"],
    "video_duration": ["视频时长"],
    "video_format": ["视频形态（系统）", "视频形态"],
    "complete_script": ["完整生产脚本"],
    # The production runner must consume the short-video generation prompt,
    # never the human-readable complete script.  Prefer the new explicit
    # field name while retaining the historical name during migration.
    "video_prompt": ["短视频提示词", "视频生成提示词"],
    "sync_enabled": ["进入生产"],
    "sync_status": ["同步结果"],
    "sync_time": ["同步时间"],
    "processing_status": ["处理状态"],
    "run_task_id": ["运行任务ID"],
    "persona_id": ["人物模板ID（系统）", "人物模板ID"],
    "persona_contract": ["人物模板合同_JSON（系统）", "人物模板合同"],
    "first_frame_strategy": ["视觉参考模式（系统）", "视觉参考模式", "首帧策略"],
    "first_frame_requested": ["生成首帧（需勾选）", "生成首帧"],
    "composite_first_frame": ["统一首帧（系统）", "统一首帧", "AI统一首帧"],
    "first_frame_status": ["首帧准备状态（系统）", "首帧准备状态", "首帧状态"],
    "publish_purpose": ["发布用途"],
    "cart_enabled": ["是否挂车"],
    "content_branch": ["内容分支"],
    "script_source": ["脚本来源"],
    "person_images": ["人物参考图（系统）"],
    "source_voiceover": ["口播_目标语言"],
    "source_voiceover_zh": ["口播_中文", "口播_中文对照"],
    "target_country": ["目标国家"],
}

POOL_SOURCES = {"成功脚本复刻", "原创生成", "视频复刻", "人工编写"}


def _necklace_handoff_reason(*, script_id: str, prompt: str) -> str:
    """Section 8's necklace-only gate: a per-row reason, or ``""``.

    The final consumer has to verify a NECKLACE_MIXED_V1 row against the frozen
    source rather than against an editable table marker.  A row that is not
    identified as necklace V1 returns ``""``.

    The import is local and this is the *only* guard: the gate itself never
    raises and never touches another category, so a broken gate cannot become a
    new preflight failure for earrings, wristwear, rings, hair accessories or
    women's wear.
    """

    try:
        from core.necklace_handoff import check_necklace_handoff
    except Exception:  # noqa: BLE001 - an absent gate must not block other categories
        return ""
    return check_necklace_handoff(script_id=script_id, prompt=prompt) or ""


def _pool_policy(fields: Dict[str, Any], mapping: Dict[str, Optional[str]], script_id: str) -> dict:
    def value(key: str) -> str:
        return normalize_text(fields.get(mapping.get(key))) if mapping.get(key) else ""

    source = value("script_source")
    if not source or source == "原创脚本":
        if script_id.startswith("wsr_"):
            raise ValueError("SCRIPT_POOL_SOURCE_MISSING:成功脚本复刻必须标明脚本来源")
        return {"script_pool_entry": False, "script_source": "原创脚本",
                "source_script_type": "原创脚本", "publish_purpose": value("publish_purpose") or "带货",
                "cart_enabled": value("cart_enabled") or "是", "content_branch": value("content_branch") or "DIRECT_RESPONSE"}
    if source not in POOL_SOURCES:
        raise ValueError(f"SCRIPT_POOL_SOURCE_UNREGISTERED:{source}")
    if source == "成功脚本复刻" and not script_id.startswith("wsr_"):
        raise ValueError("SCRIPT_POOL_SOURCE_ID_MISMATCH:成功脚本复刻需使用wsr_脚本ID")
    for key in ("publish_purpose", "cart_enabled"):
        if not mapping.get(key):
            raise ValueError(f"SCRIPT_POOL_FIELD_MISSING:{key}")
    purpose = value("publish_purpose")
    if purpose not in {"带货", "养号", "种草"}:
        raise ValueError("SCRIPT_POOL_PURPOSE_INVALID:请选择带货、养号或种草")
    cart = value("cart_enabled") or ("是" if purpose == "带货" else "否")
    if cart not in {"是", "否"}:
        raise ValueError("SCRIPT_POOL_CART_INVALID:是否挂车必须是或否")
    branch = value("content_branch") or {"带货": "DIRECT_RESPONSE", "养号": "NURTURE", "种草": "SEEDING_ORGANIC"}[purpose]
    if branch == "SEEDING_ORGANIC" and (purpose != "种草" or cart != "否"):
        raise ValueError("SEEDING_CART_GUARD_MISSING:种草专线保持不挂车")
    return {"script_pool_entry": True, "script_source": source,
            "source_script_type": "短视频复刻脚本" if source in {"成功脚本复刻", "视频复刻"} else "原创脚本",
            "source_remake_record_id": script_id if source in {"成功脚本复刻", "视频复刻"} else "",
            "publish_purpose": purpose, "cart_enabled": cart, "content_branch": branch}


def _pool_voiceover(fields: Dict[str, Any], mapping: Dict[str, Optional[str]], duration: int) -> dict:
    text = normalize_text(fields.get(mapping.get("source_voiceover"))) if mapping.get("source_voiceover") else ""
    if text in {"无", "无口播", "无旁白", "纯音乐"}:
        text = ""
    if not text:
        return {"source_voiceover_managed": True, "voiceover_requested": False}
    translation = normalize_text(fields.get(mapping.get("source_voiceover_zh"))) if mapping.get("source_voiceover_zh") else ""
    contract = {"schema_version": "voiceover-expression-contract-v2", "source_kind": "script_pool_approved_script",
                "speech_policy": {"preserve_source_wording": True, "rewrite_allowed": False, "generic_cta_required": False}}
    plan = {"schema_version": "voiceover-execution-plan-v1", "mode": "PRESERVE_SOURCE_COPY",
            "source_kind": "script_pool_approved_script", "reuse_source_copy": True,
            "copy_authority": "SOURCE_APPROVED_REUSE", "target_text": text, "chinese_translation": translation,
            "lines": [{"start_ms": 0, "end_ms": duration * 1000,
                       "voiceover_text_target_language": text, "voiceover_text_zh": translation}]}
    return {"source_voiceover_managed": True, "voiceover_requested": True, "voiceover_status": "待处理",
            "voiceover_expression_contract": json.dumps(contract, ensure_ascii=False),
            "voiceover_execution_plan": json.dumps(plan, ensure_ascii=False)}


def resolve_original_batch_field_mapping(field_names: Sequence[str]) -> Dict[str, Optional[str]]:
    return resolve_field_mapping(field_names, ORIGINAL_BATCH_SOURCE_FIELD_ALIASES)


def _reference_handoff(
    fields: Dict[str, Any], mapping: Dict[str, Optional[str]], *, script_pool_entry: bool = False
) -> tuple[List[Dict[str, Any]], str]:
    product_images = (
        extract_attachments(fields.get(mapping.get("product_images")))
        if mapping.get("product_images") else []
    )
    composite = (
        extract_attachments(fields.get(mapping.get("composite_first_frame")))
        if mapping.get("composite_first_frame") else []
    )
    strategy = (
        normalize_text(fields.get(mapping.get("first_frame_strategy")))
        if mapping.get("first_frame_strategy") else ""
    )
    status = (
        normalize_text(fields.get(mapping.get("first_frame_status"))).upper()
        if mapping.get("first_frame_status") else ""
    )
    requested = (
        normalize_checkbox(fields.get(mapping.get("first_frame_requested")))
        if mapping.get("first_frame_requested") else False
    )
    ready = bool(composite) and status in {
        "已确认", "CONFIRMED", "READY", "已就绪", "缓存复用"
    }
    # The operator owns this decision.  A REQUIRED/PREFERRED strategy is
    # informative until the user explicitly selects first-frame generation.
    if not requested:
        person_images = extract_attachments(fields.get(mapping.get("person_images"))) if script_pool_entry and mapping.get("person_images") else []
        return person_images + product_images, ""
    if ready:
        return composite, ""
    return product_images, (
        "WAITING_USER_SELECTED_FIRST_FRAME:本条已勾选生成首帧，但统一首帧尚未就绪；"
        "请先运行首帧任务或取消生成首帧勾选。"
    )


def build_original_batch_sync_tasks(
    records: Sequence[TableRecord],
    mapping: Dict[str, Optional[str]],
    *,
    product_code: Optional[str] = None,
    record_id: Optional[str] = None,
    limit: Optional[int] = None,
    errors: Optional[Dict[str, str]] = None,
) -> List[ScriptSyncTask]:
    tasks: List[ScriptSyncTask] = []
    for record in records:
        if record_id and record.record_id != record_id:
            continue
        fields = record.fields
        # Every shared-pool row has exactly one production owner.  In particular,
        # a long remake must not fall through merely because its historical
        # 视频形态 field is blank.
        route = classify_production_route(fields, mapping)
        if route.route != ProductionRoute.SHORT_VIDEO_RUN_MANAGER:
            continue
        enabled = normalize_checkbox(fields.get(mapping.get("sync_enabled"))) if mapping.get("sync_enabled") else False
        if not enabled:
            continue
        code = normalize_text(fields.get(mapping.get("product_code"))) if mapping.get("product_code") else ""
        if product_code and code != product_code:
            continue
        script_id = normalize_text(fields.get(mapping.get("script_id"))) if mapping.get("script_id") else ""
        prompt = normalize_text(fields.get(mapping.get("video_prompt"))) if mapping.get("video_prompt") else ""
        # Deliberately do not fall back to 完整生产脚本.  A missing video
        # prompt must keep the row out of production rather than send the
        # wrong content to the video model.
        try:
            if not script_id or not prompt:
                raise ValueError("SCRIPT_POOL_HANDOFF_INCOMPLETE:缺少脚本ID或短视频提示词，不能回退完整生产脚本")
            policy = _pool_policy(fields, mapping, script_id)
            if not code and not policy["script_pool_entry"]:
                raise ValueError("ORIGINAL_PRODUCT_CODE_MISSING:历史原创记录缺少产品编码")
            # The final necklace check runs last of the preflight checks, so a
            # row that is incomplete for an older reason still reports that
            # reason.  Refusing here means: no target task, the source checkbox
            # is left alone, and nothing is regenerated -- fixing the script and
            # re-exporting lets the row continue normally.
            necklace_reason = _necklace_handoff_reason(script_id=script_id, prompt=prompt)
            if necklace_reason:
                raise ValueError(necklace_reason)
        except ValueError as exc:
            if errors is None:
                raise
            errors[record.record_id] = str(exc)
            continue
        index_text = normalize_text(fields.get(mapping.get("item_index"))) if mapping.get("item_index") else ""
        try:
            item_index = int(float(index_text))
        except (TypeError, ValueError):
            item_index = len(tasks) + 1
        slot = f"D{item_index:02d}"
        batch_item_id = normalize_text(fields.get(mapping.get("batch_item_id"))) if mapping.get("batch_item_id") else ""
        title = normalize_text(fields.get(mapping.get("script_title"))) if mapping.get("script_title") else ""
        reference_images, reference_preparation_error = _reference_handoff(
            fields, mapping, script_pool_entry=policy["script_pool_entry"]
        )
        duration = normalize_video_duration(fields.get(mapping.get("video_duration")) if mapping.get("video_duration") else None)
        persona_contract = normalize_text(fields.get(mapping.get("persona_contract"))) if mapping.get("persona_contract") else ""
        if policy["script_pool_entry"] and reference_images:
            # These one-based indices describe the exact outgoing attachment order.
            try:
                contract = json.loads(persona_contract) if persona_contract else {}
                if not isinstance(contract, dict):
                    contract = {"source_contract": contract}
            except ValueError:
                contract = {"source_contract": persona_contract}
            composite_used = normalize_checkbox(fields.get(mapping.get("first_frame_requested"))) and not reference_preparation_error
            person_count = len(extract_attachments(fields.get(mapping.get("person_images")))) if mapping.get("person_images") else 0
            contract["reference_assets"] = [
                {"index": i + 1, "role": "composite_first_frame" if composite_used else ("person_identity" if i < person_count else "product")}
                for i in range(len(reference_images))
            ]
            contract["reference_handoff_policy"] = "ordered_assets_all_required; never_drop_person_or_product_silently"
            contract["source_reference_fingerprint"] = hashlib.sha256(
                json.dumps([image["file_token"] for image in reference_images]).encode()
            ).hexdigest()
            persona_contract = json.dumps(contract, ensure_ascii=False)
        voiceover = _pool_voiceover(fields, mapping, duration) if policy["script_pool_entry"] and policy["script_source"] != "原创生成" else {}
        tasks.append(
            ScriptSyncTask(
                source_record_id=record.record_id,
                product_code=code,
                script_slot=slot,
                task_name=f"{code}.{script_id}" if code else script_id,
                prompt_text=prompt,
                reference_images=reference_images,
                internal_script_key=script_id if policy["script_pool_entry"] else batch_item_id or f"{record.record_id}:{script_id}",
                product_type=normalize_text(fields.get(mapping.get("product_type"))) if mapping.get("product_type") else "",
                target_language=normalize_text(fields.get(mapping.get("target_language"))) if mapping.get("target_language") else "",
                business_category=normalize_text(fields.get(mapping.get("business_category"))) if mapping.get("business_category") else "",
                script_id=script_id,
                short_video_title=(normalize_text(fields.get(mapping.get("publish_title"))) if mapping.get("publish_title") else "") if policy["script_pool_entry"] else title,
                store_id=normalize_text(fields.get(mapping.get("store_id"))) if mapping.get("store_id") else "",
                # An internal SKU is not a platform listing. New pool sources
                # resolve platform bindings later; legacy originals stay compatible.
                product_id="" if policy["script_pool_entry"] else code,
                parent_slot=slot,
                direction_label=title,
                variant_strength="母版",
                **policy,
                **voiceover,
                video_duration=duration,
                target_country=normalize_text(fields.get(mapping.get("target_country"))) if mapping.get("target_country") else "",
                persona_id=normalize_text(fields.get(mapping.get("persona_id"))) if mapping.get("persona_id") else "",
                persona_contract=persona_contract,
                first_frame_strategy=normalize_text(fields.get(mapping.get("first_frame_strategy"))) if mapping.get("first_frame_strategy") else "",
                reference_preparation_error=reference_preparation_error,
            )
        )
        if limit is not None and len(tasks) >= limit:
            break
    return tasks
