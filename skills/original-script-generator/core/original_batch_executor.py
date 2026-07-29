"""V1 原创批次执行器 — 计划生成 + 单项执行 + resume"""
from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from core.original_batch_models import (
    BatchRecord,
    BatchRequest,
    PlanItem,
    generate_batch_id,
    generate_batch_item_id,
    generate_request_id,
    build_allocation_signature,
    build_input_snapshot,
    build_data_snapshot_hash,
)
from core.original_batch_storage import POLICY_VERSION
from core.original_batch_storage import BatchStorage
from core.original_batch_allocator import allocate_batch_items, build_content_bundle_candidates


STAGE_CHECKPOINT_SCHEMA_VERSION = "original-batch-stage-checkpoint-v1"
VISUAL_PROJECTION_CHECKPOINT_VERSION = "event-projection-v2"
VOICEOVER_CHECKPOINT_VERSION = "central-complete-voiceover-v5-audience-relation"
BLUEPRINT_PRIMARY_TRANSIENT_ATTEMPTS = max(
    1, int(os.environ.get("ORIGINAL_SCRIPT_BLUEPRINT_PRIMARY_TRANSIENT_ATTEMPTS", "2"))
)
BLUEPRINT_FALLBACK_MODEL = str(
    os.environ.get("ORIGINAL_SCRIPT_BLUEPRINT_FALLBACK_MODEL", "gpt-5.6-terra") or ""
).strip()


def _text(value: Any) -> str:
    return str(value or "").strip()


def _stable_id(prefix: str, material: Any) -> str:
    raw = json.dumps(material, ensure_ascii=False, sort_keys=True, default=str)
    return prefix + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20].upper()


def _stable_hash(material: Any) -> str:
    raw = json.dumps(material, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _is_transient_model_error(exc: Exception) -> bool:
    """Only transport/capacity failures may be retried or routed to Terra."""
    text = str(exc).lower()
    markers = (
        "overloaded", "server error", "internal server", "error occurred while processing",
        "503", "502", "500", "429", "rate limit", "too many requests",
        "timeout", "timed out", "connection reset", "connection error",
        "connection aborted", "connection closed", "peer closed",
        "incomplete chunked read", "incomplete message", "remote protocol error",
        "temporarily unavailable",
    )
    return any(marker in text for marker in markers)


def _transient_retry_delay_seconds(retry_index: int) -> int:
    # Short bounded backoff: the batch is serial, so this avoids retry storms
    # without turning one item into a long blocking loop.
    return (3, 8)[min(max(retry_index, 0), 1)]


def _generate_simplified_visual_script_with_fallback(
    *,
    prompt: str,
    primary_model: str,
    reasoning_effort: str,
    max_tokens: int,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Use Sol/high first; only transient exhaustion falls back to Terra/high.

    The function intentionally owns no semantic retry.  The exact same frozen
    prompt is reused, and invalid/unsafe/model-schema responses still surface
    as ordinary item failures rather than silently changing routes.
    """
    from core.llm_client import OriginalScriptLLMClient

    primary = _text(primary_model)
    fallback = _text(BLUEPRINT_FALLBACK_MODEL)
    candidates: List[Tuple[str, int]] = [(primary, BLUEPRINT_PRIMARY_TRANSIENT_ATTEMPTS)]
    if fallback and fallback != primary:
        candidates.append((fallback, 1))

    failures: List[str] = []
    for model_index, (model, attempts) in enumerate(candidates):
        client = OriginalScriptLLMClient(
            route="primary",
            primary_model=model,
            primary_reasoning_effort=reasoning_effort,
            timeout=300,
            max_retries=0,
        )
        for attempt_index in range(attempts):
            try:
                raw = client.call_json(
                    prompt,
                    max_tokens=max_tokens,
                    max_attempts=1,
                    repair_json_on_failure=False,
                )
                return raw, {
                    "model": model,
                    "reasoning_effort": reasoning_effort,
                    "configured_primary_model": primary,
                    "fallback_used": model != primary,
                    "transient_attempt_count": len(failures) + 1,
                }
            except Exception as exc:
                failures.append(f"{model}: {str(exc)[:300]}")
                if not _is_transient_model_error(exc):
                    raise
                has_same_model_retry = attempt_index + 1 < attempts
                if has_same_model_retry:
                    time.sleep(_transient_retry_delay_seconds(attempt_index))
                    continue
                break

        # The next route is a fallback, not an immediate duplicate request.
        if model_index + 1 < len(candidates):
            time.sleep(_transient_retry_delay_seconds(1))

    raise RuntimeError(
        "完整视觉脚本模型在 Sol 重试及 Terra 兜底后仍失败：" + " | ".join(failures)
    )


def _normalize_script_mode(value: str = "") -> str:
    from core.simplified_complete_script import (
        SCRIPT_MODE_LEGACY,
        SCRIPT_MODE_SIMPLIFIED,
    )
    mode = _text(value or os.environ.get("ORIGINAL_BATCH_SCRIPT_MODE") or SCRIPT_MODE_LEGACY).lower()
    if mode not in {SCRIPT_MODE_LEGACY, SCRIPT_MODE_SIMPLIFIED}:
        raise ValueError(f"不支持的原创批次脚本模式：{mode}")
    return mode


def _checkpoint_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _checkpoint_identity(
    item: PlanItem,
    blueprint_provenance: Dict[str, Any],
) -> Dict[str, Any]:
    try:
        frozen = json.loads(item.frozen_direction_package_json or "{}")
    except Exception:
        frozen = item.frozen_direction_package_json or ""
    return {
        "item_snapshot_hash": item.item_snapshot_hash,
        "frozen_direction_hash": _stable_hash(frozen),
        "blueprint_provenance": {
            key: _text(blueprint_provenance.get(key))
            for key in (
                "stage", "route", "model", "reasoning_effort",
                "creative_seed_schema_version",
            )
        },
    }


def _load_stage_checkpoint(
    item: PlanItem,
    blueprint_provenance: Dict[str, Any],
) -> Tuple[Dict[str, Any], bool]:
    """Return a matching checkpoint, or a clean one when its inputs changed."""
    expected = _checkpoint_identity(item, blueprint_provenance)
    try:
        existing = json.loads(item.stage_checkpoint_json or "{}")
    except Exception:
        existing = {}
    if (
        isinstance(existing, dict)
        and existing.get("schema_version") == STAGE_CHECKPOINT_SCHEMA_VERSION
        and existing.get("identity") == expected
        and isinstance(existing.get("stages"), dict)
    ):
        return existing, True
    return {
        "schema_version": STAGE_CHECKPOINT_SCHEMA_VERSION,
        "identity": expected,
        "stages": {},
        "created_at": _checkpoint_now(),
        "updated_at": _checkpoint_now(),
    }, False


def _persist_stage_checkpoint(
    storage: BatchStorage,
    item: PlanItem,
    checkpoint: Dict[str, Any],
) -> None:
    checkpoint["updated_at"] = _checkpoint_now()
    storage.update_item_checkpoint(item.batch_item_id, checkpoint)
    item.stage_checkpoint_json = json.dumps(
        checkpoint, ensure_ascii=False, sort_keys=True, default=str
    )


def _record_checkpoint_error(
    storage: BatchStorage,
    item: PlanItem,
    checkpoint: Dict[str, Any],
    stage_name: str,
    error: Exception,
) -> None:
    stage = checkpoint.setdefault("stages", {}).setdefault(stage_name, {})
    stage.update({
        "status": "ERROR",
        "error": str(error)[:1000],
        "updated_at": _checkpoint_now(),
    })
    _persist_stage_checkpoint(storage, item, checkpoint)


def validate_batch_script_integrity(
    script: Dict[str, Any],
    *,
    direction: Dict[str, Any],
) -> Dict[str, Any]:
    """Three production integrity gates; style remains model-owned."""

    issues: List[str] = []
    blueprint = script.get("creative_blueprint") if isinstance(script.get("creative_blueprint"), dict) else {}
    creative = script.get("creative_diversity_contract") if isinstance(script.get("creative_diversity_contract"), dict) else {}
    production = script.get("production_design") if isinstance(script.get("production_design"), dict) else {}
    if not blueprint or not creative:
        issues.append("脚本没有携带冻结的完整蓝图和创意合同")

    presentation = _text(production.get("presentation_mode")).upper()
    character = production.get("character_setting") if isinstance(production.get("character_setting"), dict) else {}
    scene = production.get("scene_setting") if isinstance(production.get("scene_setting"), dict) else {}
    outfit = production.get("outfit_setting") if isinstance(production.get("outfit_setting"), dict) else {}
    performance = production.get("performance_setting") if isinstance(production.get("performance_setting"), dict) else {}
    event = production.get("event_setting") if isinstance(production.get("event_setting"), dict) else {}
    if not all(_text(scene.get(key)) for key in ("location", "lighting", "background")):
        issues.append("制作设定缺少场景、光线或背景")
    if not _text(event.get("natural_event")):
        issues.append("制作设定缺少可执行的连续生活事件")
    if presentation == "PERSON_ON_CAMERA":
        if not all(
            _text(value)
            for value in (
                character.get("identity"),
                character.get("appearance") or character.get("hair_makeup"),
                character.get("speaking_personality"),
                outfit.get("styling"),
                performance.get("behavior_motivation"),
            )
        ):
            issues.append("人物方向缺少人物、外形、穿搭、说话人格或行为动机")
    elif presentation in {"STATIC_PRODUCT", "HANDS_ONLY"}:
        if character.get("on_camera") is True:
            issues.append(f"{presentation}方向不应把人物设为出镜")
    else:
        issues.append(f"未知制作承载方式：{presentation or '空'}")

    bundle = direction.get("content_bundle_brief") if isinstance(direction.get("content_bundle_brief"), dict) else {}
    authority_text = json.dumps(bundle, ensure_ascii=False)
    voice_text = _text(script.get("continuous_voiceover", {}).get("chinese_translation"))
    unsupported_effect_terms = (
        "显腿长", "腿更长", "拉长腿", "显瘦", "显高", "塑形",
        "修饰身材", "保暖", "舒适", "不挑人", "百搭",
    )
    for term in unsupported_effect_terms:
        if term in voice_text and term not in authority_text:
            issues.append(f"口播出现未授权效果词：{term}")
    return {
        "valid": not issues,
        "issues": issues,
        "policy_version": "original-batch-integrity-v1",
    }


# ── Product context loader ─────────────────────────────────────────────


def load_product_context(
    product_code: str,
    *,
    target_country: str = "",
    target_language: str = "",
    voiceover_root: str = "",
) -> Dict[str, Any]:
    """Load the authoritative product context from the most recent successful run."""
    from core.storage import PipelineStorage
    storage = PipelineStorage()

    runs = storage.query_runs_by_product_code(product_code, limit=200)
    if not runs:
        raise RuntimeError(f"找不到产品 {product_code} 的原创脚本历史运行记录")

    for row in runs:
        record_id = str(row["record_id"] or "")
        if record_id.startswith("stage0-reality:"):
            continue

        anchor = storage.get_latest_stage_output_json(record_id, "anchor_card", product_code)
        route = storage.get_latest_stage_output_json(record_id, "structure_route", product_code)
        if anchor and route:
            strategy = storage.get_latest_stage_output_json(record_id, "strategy_cards", product_code) or {}

            country = target_country or _text(anchor.get("target_country") or route.get("request", {}).get("target_country", "泰国"))
            language = target_language or _text(anchor.get("target_language") or route.get("request", {}).get("target_language", "泰语"))
            product_type = _text(anchor.get("product_type") or route.get("request", {}).get("product_type", "外套"))
            top_category = _text(anchor.get("top_category") or route.get("request", {}).get("category", "女装"))

            # The central engine is the governance authority for reviewed
            # selling claims.  Legacy strategy cards remain a fallback, not a
            # competing source of truth.  This read is done once per batch and
            # the resulting catalog is frozen in the input snapshot.
            from core.product_selling_argument_adapter import (
                load_verified_selling_point_catalog,
            )

            central_snapshot = load_verified_selling_point_catalog(
                product_code,
                voiceover_root=voiceover_root,
            )
            central_catalog = list(central_snapshot.get("catalog") or [])
            legacy_catalog = list(strategy.get("selling_point_catalog", []) or [])
            combined_catalog = [*central_catalog, *legacy_catalog]

            return {
                "source_run_id": row["run_id"],
                "source_record_id": record_id,
                "input_hash": _text(row["input_hash"]),
                "product_code": product_code,
                "target_country": country,
                "target_language": language,
                "product_type": product_type,
                "top_category": top_category,
                "anchor_card": anchor,
                "structure_route": route,
                "selling_point_catalog": combined_catalog,
                "selling_point_catalog_snapshot": central_snapshot,
                "selling_point_catalog_sources": {
                    "central_verified_count": len(central_catalog),
                    "legacy_strategy_count": len(legacy_catalog),
                },
                "product_selling_note": _text(anchor.get("product_selling_note") or strategy.get("product_selling_note", "")),
            }

    raise RuntimeError(f"产品 {product_code} 只有 stage0 测试记录，没有正式生产 run，无法确定产品权威上下文")


def _allowed_structure_carriers(
    selling_point_catalog: List[Dict[str, Any]],
) -> List[str]:
    """Narrow the pool only when every explicit value needs a wearer.

    Missing metadata stays flexible.  This is a cheap pre-filter that avoids
    spending one of four direction slots on a carrier none of the batch's
    operator-approved arguments can use.
    """
    dependencies = [
        _text(item.get("visual_dependency")).upper()
        for item in selling_point_catalog
        if isinstance(item, dict)
        and _text(item.get("visual_dependency"))
        and _text(item.get("argument_kind")).upper() in {"", "SELLING_ARGUMENT"}
    ]
    if dependencies and all(value == "WEARER_REQUIRED" for value in dependencies):
        return ["WEARER_ACTIVE", "MIXED"]
    return ["HAND_ONLY", "STATIC_PRODUCT", "MIXED", "WEARER_ACTIVE"]


def _load_active_hook_ids(voiceover_root: str = "", db_path: str = "") -> List[str]:
    try:
        from core.reality_voiceover_bridge import load_active_voiceover_hooks
        hooks = load_active_voiceover_hooks(voiceover_root or None, db_path=db_path or None)
        return [_text(h.get("hook_id")) for h in hooks if h.get("hook_id")]
    except Exception:
        return ["AUDIENCE_NEED_CALLOUT", "DISCOVERY_RESULT_PROMISE", "DETAIL_SURPRISE", "GENERAL_PRODUCT_SHARE"]


def _reserve_batch_creative_usage(
    *,
    item: PlanItem,
    batch: BatchRecord,
    source_run_id: int,
    creative_storage: Any,
) -> None:
    """Reserve an already-selected creative combination for future batches.

    This is bookkeeping only.  A reservation never changes the frozen creative
    choice and a ledger outage must not discard an otherwise valid plan.
    """

    try:
        frozen = json.loads(item.frozen_direction_package_json or "{}")
        contract = (
            frozen.get("creative_diversity_contract")
            if isinstance(frozen.get("creative_diversity_contract"), dict)
            else {}
        )
        if not contract:
            return
        from core.complete_script_v3 import creative_usage_row

        direction = {
            "direction_assignment_id": item.direction_assignment_id,
            "content_bundle_brief": frozen.get("content_bundle_brief") or {},
        }
        row = creative_usage_row(
            contract=contract,
            product_code=item.product_code,
            direction=direction,
            source_run_id=source_run_id,
        )
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        row["metadata"] = {
            **metadata,
            "batch_id": batch.batch_id,
            "batch_item_id": item.batch_item_id,
        }
        usage_id = creative_storage.reserve_creative_pattern(row)
        frozen["creative_usage_id"] = usage_id
        item.frozen_direction_package_json = json.dumps(
            frozen, ensure_ascii=False, sort_keys=True, default=str
        )
    except Exception as exc:
        print(f"  ⚠️ 创意历史预留失败，仍保留本次冻结计划：{str(exc)[:180]}")


def _update_batch_creative_usage(item: PlanItem, status: str) -> None:
    """Best-effort lifecycle update for a batch-owned creative reservation."""

    try:
        frozen = json.loads(item.frozen_direction_package_json or "{}")
        usage_id = _text(frozen.get("creative_usage_id"))
        if not usage_id:
            return
        from core.storage import PipelineStorage

        PipelineStorage(database_url="sqlite").update_creative_pattern_status(
            usage_id, status
        )
    except Exception as exc:
        print(f"  ⚠️ 创意历史状态更新失败：{str(exc)[:180]}")


# ── Plan-only execution ────────────────────────────────────────────────


def run_plan_only(
    request: BatchRequest,
    *,
    output_dir: str = "",
    voiceover_root: str = "",
    voiceover_db_path: str = "",
) -> Tuple[BatchRecord, List[PlanItem], Dict[str, Any]]:
    """Plan a batch without calling any generative models. Idempotent: same request returns cached plan."""
    storage = BatchStorage()
    storage.ensure_schema()

    request_id = request.request_id or generate_request_id(
        request.product_code,
        request.random_seed,
        request.test_phase,
        request.script_mode,
    )
    # Idempotency is policy-scoped.  A V1 plan does not contain the full
    # creative package and must never be silently reused by the V2 executor.
    existing = storage.get_batch_by_request_id(request_id)
    if existing:
        items = storage.get_items(existing.batch_id)
        if (
            existing.policy_version == POLICY_VERSION
            and items
            and all(item.frozen_direction_package_json for item in items)
        ):
            summary = json.loads(existing.allocation_summary_json) if existing.allocation_summary_json else {}
            return existing, items, summary
        request_id = _stable_id(
            "OP_ORIGINAL_",
            {"supersedes_request_id": request_id, "policy_version": POLICY_VERSION},
        )
        upgraded = storage.get_batch_by_request_id(request_id)
        if upgraded:
            items = storage.get_items(upgraded.batch_id)
            summary = json.loads(upgraded.allocation_summary_json) if upgraded.allocation_summary_json else {}
            return upgraded, items, summary

    # Load product context
    ctx = load_product_context(
        request.product_code,
        target_country=request.target_country,
        target_language=request.target_language,
        voiceover_root=voiceover_root,
    )
    from core.storage import PipelineStorage

    creative_storage: Any = None
    recent_creative_usage: List[Dict[str, Any]] = []
    try:
        # Product/structure source data may be configured on RDS, while the
        # creative ledger is deliberately local workflow state.  The RDS
        # schema does not own `creative_pattern_usage`.
        creative_storage = PipelineStorage(database_url="sqlite")
        recent_creative_usage = creative_storage.list_recent_creative_patterns(
            country=ctx["target_country"],
            category=ctx["top_category"],
            limit=120,
        )
    except Exception as exc:
        # Creative history changes selection quality only.  A temporary
        # history-store outage must not make a deterministic plan impossible.
        print(f"  ⚠️ 创意历史暂不可读，本次按空历史规划：{str(exc)[:180]}")

    # The simplified path reselects from the full versioned structure pool for
    # every batch.  Legacy production keeps consuming its historical route so
    # this change cannot silently alter old output.
    planning_selection = ctx["structure_route"]
    recent_cluster_usage: Dict[str, int] = {}
    if request.script_mode == "simplified_v1":
        from core.structure_router_adapter import select_original_structure_directions

        try:
            recent_cluster_usage = storage.get_recent_cluster_usage(
                request.product_code,
                limit_batches=8,
            )
        except Exception as exc:
            print(f"  ⚠️ 结构轮换历史暂不可读，本次按空历史选择：{str(exc)[:180]}")
        planning_selection = select_original_structure_directions(
            context={
                "product_code": request.product_code,
                "target_country": ctx["target_country"],
                "top_category": ctx["top_category"],
                "product_type": ctx["product_type"],
            },
            anchor_card=ctx["anchor_card"],
            record_id=f"{ctx['source_record_id']}:batch:{request_id}",
            input_hash=f"{ctx.get('input_hash', '')}:{POLICY_VERSION}",
            direction_count=min(request.requested_count, 4),
            random_seed=request.random_seed,
            allowed_carriers=_allowed_structure_carriers(
                ctx["selling_point_catalog"]
            ),
            recent_cluster_usage=recent_cluster_usage,
        )

    # Build direction packages
    from core.reality_reference import build_reality_direction_packages
    packages = build_reality_direction_packages(
        planning_selection,
        anchor_card=ctx["anchor_card"],
        product_type=ctx["product_type"],
        top_category=ctx["top_category"],
        direction_limit=min(request.requested_count, 4),
        recent_execution_card_ids=[],
        recent_source_video_ids=[],
        selling_point_catalog=ctx["selling_point_catalog"],
        product_selling_note=ctx["product_selling_note"],
        allow_structure_only=request.script_mode == "simplified_v1",
    )
    directions = packages.get("directions", [])

    active_hooks = _load_active_hook_ids(
        voiceover_root=voiceover_root,
        db_path=voiceover_db_path,
    )

    # Allocate
    items, alloc_summary = allocate_batch_items(
        product_code=request.product_code,
        requested_count=request.requested_count,
        directions=directions,
        anchor_card=ctx["anchor_card"],
        active_hook_ids=active_hooks,
        creative_policy_version="creative-diversity-v1",
        random_seed=request.random_seed,
        recent_creative_usage=recent_creative_usage,
        selling_point_catalog=ctx["selling_point_catalog"],
        product_selling_note=ctx["product_selling_note"],
        product_type=ctx["product_type"],
        top_category=ctx["top_category"],
    )

    # Persist batch
    input_snapshot = build_input_snapshot(
        ctx, planning_selection, active_hooks,
        "creative-diversity-v1", POLICY_VERSION,
    )
    input_snapshot["script_mode"] = request.script_mode
    input_snapshot["recent_cluster_usage"] = recent_cluster_usage
    data_hash = build_data_snapshot_hash(input_snapshot)

    batch = BatchRecord(
        batch_id=generate_batch_id(
            request_id, request.product_code, ctx.get("input_hash", ""),
            data_hash, POLICY_VERSION, request.random_seed,
        ),
        request_id=request_id,
        product_code=request.product_code,
        requested_count=request.requested_count,
        test_phase=request.test_phase,
        execution_mode="PLAN_ONLY",
        policy_version=POLICY_VERSION,
        random_seed=request.random_seed,
        data_snapshot_hash=data_hash,
        input_snapshot_json=json.dumps(input_snapshot, ensure_ascii=False, default=str),
        planned_count=len(items),
        status="PLANNED" if len(items) >= request.requested_count else "PARTIAL_PLANNED",
        allocation_summary_json=json.dumps(alloc_summary, ensure_ascii=False, default=str),
        target_country=ctx["target_country"],
        target_language=ctx["target_language"],
        top_category=ctx["top_category"],
        product_type=ctx["product_type"],
        source_record_id=ctx["source_record_id"],
    )

    storage.create_batch(batch)

    # Persist items (with real batch ID)
    for item in items:
        item.batch_id = batch.batch_id
        item.batch_item_id = generate_batch_item_id(batch.batch_id, item.item_index, item.allocation_signature)
        try:
            source_run_id = int(ctx.get("source_run_id") or 0)
        except (TypeError, ValueError):
            source_run_id = 0
        if creative_storage is not None:
            _reserve_batch_creative_usage(
                item=item,
                batch=batch,
                source_run_id=source_run_id,
                creative_storage=creative_storage,
            )
        storage.insert_item(item)

    return batch, items, alloc_summary


# ── Script-only execution ──────────────────────────────────────────────


def run_script_only(
    batch_id: str,
    *,
    resume: bool = True,
    limit: int = 0,
    plan_only: bool = False,
    voiceover_root: str = "",
    voiceover_db_path: str = "",
    voiceover_model_command: str = "",
    voiceover_qc_model_command: str = "",
    blueprint_model: str = "gpt-5.6-sol",
    blueprint_reasoning: str = "high",
    script_mode: str = "",
    delay_between_items: int = 0,
) -> Tuple[BatchRecord, List[PlanItem]]:
    """Execute frozen batch items that are PLANNED or SCRIPT_FAILED."""
    storage = BatchStorage()
    storage.ensure_schema()
    selected_script_mode = _normalize_script_mode(script_mode)

    batch = storage.get_batch(batch_id)
    if not batch:
        raise RuntimeError(f"批次不存在: {batch_id}")

    if plan_only:
        return batch, storage.get_items(batch_id)

    items = storage.get_items(batch_id)
    executable = [it for it in items if it.status in {"PLANNED", "SCRIPT_FAILED"}] if resume else [it for it in items if it.status == "PLANNED"]
    if limit and limit > 0:
        executable = executable[:limit]
    already_ready = [it for it in items if it.status == "SCRIPT_READY"]

    if already_ready and resume:
        print(f"  ♻️ 已完成的 {len(already_ready)} 项跳过")

    completed = 0
    failed = 0
    for item in executable:
        print(f"\n  🧩 处理 {item.batch_item_id} [{item.item_index}/{batch.requested_count}] | {item.item_role} | {item.requested_hook_id}")
        try:
            result = _execute_single_item(
                item=item,
                batch=batch,
                storage=storage,
                voiceover_root=voiceover_root,
                voiceover_db_path=voiceover_db_path,
                voiceover_model_command=voiceover_model_command,
                voiceover_qc_model_command=voiceover_qc_model_command,
                blueprint_model=blueprint_model,
                blueprint_reasoning=blueprint_reasoning,
                script_mode=selected_script_mode,
            )
            if result.get("status") == "SUCCESS":
                storage.update_item_status(
                    item.batch_item_id, "SCRIPT_READY",
                    error_code="",
                    error_message="",
                    actual_hook_id=result.get("actual_hook_id", ""),
                    consumer_run_id=result.get("consumer_run_id", ""),
                    script_id=result.get("script_id", ""),
                    content_id=result.get("content_id", ""),
                    video_prompt_id=result.get("video_prompt_id", ""),
                    structure_binding_id=result.get("structure_binding_id", ""),
                    attempt_count=item.attempt_count + 1,
                    result_json=json.dumps(result, ensure_ascii=False, default=str),
                )
                _update_batch_creative_usage(item, "MACHINE_SCREENED")
                completed += 1
            else:
                storage.update_item_status(
                    item.batch_item_id, "SCRIPT_FAILED",
                    error_code=result.get("error_code", "SCRIPT_FAILED"),
                    error_message=result.get("error_message", ""),
                    attempt_count=item.attempt_count + 1,
                    result_json=json.dumps(result, ensure_ascii=False, default=str),
                )
                failed += 1
        except Exception as exc:
            storage.update_item_status(
                item.batch_item_id, "SCRIPT_FAILED",
                error_code="RUNTIME_ERROR",
                error_message=str(exc),
                attempt_count=item.attempt_count + 1,
            )
            failed += 1

        if delay_between_items > 0 and (completed + failed) < len(executable):
            import time as _time
            _time.sleep(delay_between_items)

    # Update batch totals against the number that was actually planned.  A
    # content-capacity shortfall is already recorded by PLAN_ONLY and must not
    # leave an otherwise complete partial batch stuck in DISPATCHING forever.
    latest_items = storage.get_items(batch_id)
    ready = sum(1 for item in latest_items if item.status == "SCRIPT_READY")
    failed_total = sum(1 for item in latest_items if item.status == "SCRIPT_FAILED")
    pending_total = sum(
        1 for item in latest_items if item.status in {"PLANNED", "SCRIPT_RUNNING"}
    )
    if batch.planned_count > 0 and ready >= batch.planned_count:
        batch_status = "SCRIPT_READY"
    elif failed_total > 0 and pending_total == 0:
        batch_status = "PARTIAL_FAILED" if ready > 0 else "FAILED"
    else:
        batch_status = "DISPATCHING"
    storage.update_batch_status(
        batch_id,
        status=batch_status,
        ready_count=ready,
        failed_count=failed_total,
    )
    if batch_status in {"FAILED", "PARTIAL_FAILED"}:
        for failed_item in latest_items:
            if failed_item.status == "SCRIPT_FAILED":
                _update_batch_creative_usage(failed_item, "RELEASED")

    return storage.get_batch(batch_id), storage.get_items(batch_id)


def _execute_simplified_single_item(
    *,
    item: PlanItem,
    batch: BatchRecord,
    storage: BatchStorage,
    voiceover_root: str,
    voiceover_db_path: str,
    voiceover_model_command: str,
    blueprint_model: str,
    blueprint_reasoning: str,
) -> Dict[str, Any]:
    """Execute the thin one-pass visual script + central voiceover path."""

    from core.complete_voiceover_direct import run_central_complete_voiceover
    from core.llm_client import OriginalScriptLLMClient
    from core.simplified_complete_script import (
        CREATIVE_SEED_SCHEMA_VERSION,
        SCRIPT_MODE_SIMPLIFIED,
        assemble_simplified_complete_script,
        build_simplified_creative_seed,
        build_simplified_script_prompt,
        build_simplified_voiceover_inputs,
        normalize_simplified_visual_script,
        validate_simplified_complete_script,
        validate_simplified_visual_script,
    )
    from core.structure_router_adapter import bind_structure_application

    if not item.frozen_direction_package_json:
        raise RuntimeError(
            "FROZEN_DIRECTION_MISSING_REPLAN_REQUIRED: 批次没有完整冻结方向包"
        )
    frozen = json.loads(item.frozen_direction_package_json)
    if frozen.get("schema_version") != "original-frozen-direction-package-v1":
        raise RuntimeError("冻结方向包版本不受支持")
    contract = frozen.get("structure_contract") if isinstance(frozen.get("structure_contract"), dict) else {}
    bundle = frozen.get("content_bundle_brief") if isinstance(frozen.get("content_bundle_brief"), dict) else {}
    creative = frozen.get("creative_diversity_contract") if isinstance(frozen.get("creative_diversity_contract"), dict) else {}
    if not contract or not bundle:
        raise RuntimeError("简化脚本路径缺少冻结的结构或内容事实")

    seed = frozen.get("simplified_creative_seed") if isinstance(frozen.get("simplified_creative_seed"), dict) else {}
    # A normal batch freezes the full creative seed during PLAN_ONLY.  Do not
    # re-read RDS during SCRIPT_ONLY merely to reconstruct data we already
    # pinned: it adds a network dependency without changing the script input.
    # Older batches that lack the seed retain the backward-compatible reload.
    ctx: Dict[str, Any] = {}
    if not seed:
        ctx = load_product_context(
            item.product_code,
            target_country=batch.target_country,
            target_language=batch.target_language,
        )
        seed = build_simplified_creative_seed(
            anchor_card=ctx.get("anchor_card") or {},
            structure_contract=contract,
            content_bundle=bundle,
            creative_contract=creative,
            execution_reference=frozen.get("execution_reference") or {},
            requested_hook_id=item.requested_hook_id,
            content_angle_key=item.content_angle_key,
            product_type=batch.product_type or ctx.get("product_type", ""),
            top_category=batch.top_category or ctx.get("top_category", ""),
        )

    provenance = {
        "stage": "simplified_complete_visual_script",
        "route": "primary",
        "model": blueprint_model,
        "reasoning_effort": blueprint_reasoning,
        "script_mode": SCRIPT_MODE_SIMPLIFIED,
        "creative_seed_schema_version": CREATIVE_SEED_SCHEMA_VERSION,
    }
    checkpoint, checkpoint_matched = _load_stage_checkpoint(item, provenance)
    stage_cache = {
        "checkpoint": "HIT" if checkpoint_matched else "MISS",
        "complete_visual_script": "MISS",
        "voiceover": "MISS",
        "assembly": "MISS",
    }

    # 1. One model call owns the full visual script.  Code does not rewrite
    # character, outfit, scene, emotion, event or storyboard semantics.
    visual_stage = checkpoint.setdefault("stages", {}).setdefault(
        "complete_visual_script", {}
    )
    generation_provenance = dict(provenance)
    visual_script = visual_stage.get("normalized")
    if isinstance(visual_script, dict) and visual_script:
        stage_cache["complete_visual_script"] = "HIT"
    else:
        raw = visual_stage.get("raw")
        if not isinstance(raw, dict) or not raw:
            try:
                raw, route_provenance = _generate_simplified_visual_script_with_fallback(
                    prompt=build_simplified_script_prompt(
                        seed,
                        target_country=batch.target_country or ctx.get("target_country", "泰国"),
                        target_language=batch.target_language or ctx.get("target_language", "泰语"),
                        duration_seconds=15,
                    ),
                    primary_model=blueprint_model,
                    reasoning_effort=blueprint_reasoning,
                    max_tokens=7000,
                )
                generation_provenance.update(route_provenance)
            except Exception as exc:
                _record_checkpoint_error(
                    storage, item, checkpoint, "complete_visual_script", exc
                )
                raise
            visual_stage.update({
                "status": "GENERATED",
                "raw": raw,
                "updated_at": _checkpoint_now(),
            })
            _persist_stage_checkpoint(storage, item, checkpoint)
        visual_script = normalize_simplified_visual_script(
            raw, seed, generation_provenance=generation_provenance
        )
        visual_stage.update({
            "status": "NORMALIZED",
            "normalized": visual_script,
            "updated_at": _checkpoint_now(),
        })
        _persist_stage_checkpoint(storage, item, checkpoint)

    visual_check = validate_simplified_visual_script(visual_script, seed)
    visual_stage.update({
        "status": "VALIDATED" if visual_check["valid"] else "INVALID",
        "validation": visual_check,
        "updated_at": _checkpoint_now(),
    })
    _persist_stage_checkpoint(storage, item, checkpoint)
    if not visual_check["valid"]:
        raise ValueError("简化完整视觉脚本校验失败：" + "；".join(visual_check["issues"][:10]))

    # 2. The existing central voiceover engine keeps hook language and speech
    # style authority; the visual script only supplies truthful context.
    direction, visual_plan = build_simplified_voiceover_inputs(
        visual_script, seed, frozen
    )
    voiceover_surface_contract = (
        seed.get("voiceover_surface_contract")
        if isinstance(seed.get("voiceover_surface_contract"), dict)
        else {}
    )
    relationship_device = _text(
        voiceover_surface_contract.get("relationship_device")
    )
    voice_dependency_hash = _stable_hash({
        "checkpoint_version": VOICEOVER_CHECKPOINT_VERSION,
        "simplified_script_id": visual_script.get("simplified_script_id"),
        "voiceover_direction": direction,
        "voiceover_visual_plan": visual_plan,
        "requested_hook_id": item.requested_hook_id,
        "voiceover_surface_contract": voiceover_surface_contract,
        "model_command_hash": _stable_hash(voiceover_model_command),
    })
    voice_stage = checkpoint.setdefault("stages", {}).setdefault("voiceover", {})
    voiceover = voice_stage.get("plan")
    if (
        isinstance(voiceover, dict)
        and voiceover
        and voice_stage.get("dependency_hash") == voice_dependency_hash
    ):
        stage_cache["voiceover"] = "HIT"
    else:
        try:
            voiceover = run_central_complete_voiceover(
                product_code=item.product_code,
                target_country=batch.target_country or ctx.get("target_country", ""),
                target_language=batch.target_language or ctx.get("target_language", ""),
                top_category=batch.top_category or ctx.get("top_category", ""),
                product_type=batch.product_type or ctx.get("product_type", ""),
                direction=direction,
                visual_plan=visual_plan,
                voiceover_root=voiceover_root,
                voiceover_db_path=voiceover_db_path,
                model_command=voiceover_model_command,
                candidate_hook_id=item.requested_hook_id,
                relationship_device=relationship_device,
            )
        except Exception as exc:
            voice_stage["dependency_hash"] = voice_dependency_hash
            _record_checkpoint_error(storage, item, checkpoint, "voiceover", exc)
            raise
        voice_stage.update({
            "status": "VALIDATED",
            "dependency_hash": voice_dependency_hash,
            "plan": voiceover,
            "updated_at": _checkpoint_now(),
        })
        _persist_stage_checkpoint(storage, item, checkpoint)

    # 3. Assembly is deterministic and may not redesign the model output.
    assembly_dependency_hash = _stable_hash({
        "visual_script_id": visual_script.get("simplified_script_id"),
        "voiceover": voiceover,
    })
    assembly_stage = checkpoint.setdefault("stages", {}).setdefault("assembly", {})
    script = assembly_stage.get("script")
    if (
        isinstance(script, dict)
        and script
        and assembly_stage.get("dependency_hash") == assembly_dependency_hash
    ):
        stage_cache["assembly"] = "HIT"
    else:
        script = assemble_simplified_complete_script(visual_script, seed, voiceover)
        assembly_stage.update({
            "status": "GENERATED",
            "dependency_hash": assembly_dependency_hash,
            "script": script,
            "updated_at": _checkpoint_now(),
        })
        _persist_stage_checkpoint(storage, item, checkpoint)
    assembly_check = validate_simplified_complete_script(script)
    assembly_stage.update({
        "status": "VALIDATED" if assembly_check["valid"] else "INVALID",
        "validation": assembly_check,
        "updated_at": _checkpoint_now(),
    })
    _persist_stage_checkpoint(storage, item, checkpoint)
    if not assembly_check["valid"]:
        raise ValueError("简化完整脚本装配失败：" + "；".join(assembly_check["issues"]))

    consumer_run_id = _stable_id(
        "RUN_", {"batch_item_id": item.batch_item_id, "ts": time.time_ns()}
    )
    script_id = _stable_id("SCRIPT_", script)
    content_id = _stable_id("CONTENT_", bundle)
    video_prompt_id = _stable_id("VP_", script.get("video_generation_brief", {}))
    binding_id = ""
    try:
        binding_id = bind_structure_application(
            contract=contract,
            consumer_run_id=consumer_run_id,
            record_id=batch.source_record_id or item.batch_item_id,
            product_code=item.product_code,
            application_stage="ORIGINAL_BATCH_SIMPLIFIED_SCRIPT_READY",
            script_id=script_id,
            content_id=content_id,
            video_prompt_id=video_prompt_id,
            metadata={
                "batch_id": item.batch_id,
                "batch_item_id": item.batch_item_id,
                "script_mode": SCRIPT_MODE_SIMPLIFIED,
                "content_angle_key": item.content_angle_key,
                "requested_hook_id": item.requested_hook_id,
                "actual_hook_id": voiceover.get("hook_id", item.requested_hook_id),
                "preferred_presentation": seed.get("creative_direction", {}).get("preferred_presentation"),
            },
        ) or ""
    except Exception:
        pass

    return {
        "status": "SUCCESS",
        "script_mode": SCRIPT_MODE_SIMPLIFIED,
        "consumer_run_id": consumer_run_id,
        "script_id": script_id,
        "content_id": content_id,
        "video_prompt_id": video_prompt_id,
        "actual_hook_id": voiceover.get("hook_id", item.requested_hook_id),
        "structure_binding_id": binding_id,
        "frozen_direction_package_schema_version": frozen.get("schema_version"),
        "creative_seed_id": seed.get("creative_seed_id"),
        "stage_cache": stage_cache,
        "validation": {
            "visual_script": visual_check,
            "assembly": assembly_check,
        },
        "script": script,
    }


def _execute_single_item(
    item: PlanItem,
    batch: BatchRecord,
    storage: BatchStorage,
    voiceover_root: str = "",
    voiceover_db_path: str = "",
    voiceover_model_command: str = "",
    voiceover_qc_model_command: str = "",
    blueprint_model: str = "gpt-5.6-sol",
    blueprint_reasoning: str = "high",
    script_mode: str = "",
) -> Dict[str, Any]:
    """Execute one frozen batch item through the reality-reference pipeline."""
    selected_script_mode = _normalize_script_mode(script_mode)
    from core.simplified_complete_script import SCRIPT_MODE_SIMPLIFIED
    if selected_script_mode == SCRIPT_MODE_SIMPLIFIED:
        return _execute_simplified_single_item(
            item=item,
            batch=batch,
            storage=storage,
            voiceover_root=voiceover_root,
            voiceover_db_path=voiceover_db_path,
            voiceover_model_command=voiceover_model_command,
            blueprint_model=blueprint_model,
            blueprint_reasoning=blueprint_reasoning,
        )
    from core.llm_client import OriginalScriptLLMClient
    from core.complete_script_v3 import validate_complete_blueprint, validate_complete_script, video_prompt_projection
    from core.reality_reference import (
        assemble_reality_script,
        project_event_blueprint_to_visual_plan,
        validate_visual_adaptation,
        validate_voiceover_visual_grounding,
    )
    from core.complete_voiceover_direct import run_central_complete_voiceover
    from core.structure_execution_compiler import compile_structure_execution_plan
    from core.structure_router_adapter import bind_structure_application
    from core.reality_reference_prompts import build_complete_script_blueprint_prompt
    from scripts.run_reality_reference_stage0 import (
        _blueprint_model_provenance,
        _normalize_blueprint,
    )

    if not item.frozen_direction_package_json:
        raise RuntimeError(
            "FROZEN_DIRECTION_MISSING_REPLAN_REQUIRED: 旧批次没有完整方向包，请重新执行PLAN_ONLY"
        )
    frozen = json.loads(item.frozen_direction_package_json)
    if frozen.get("schema_version") != "original-frozen-direction-package-v1":
        raise RuntimeError("冻结方向包版本不受支持")
    contract = frozen.get("structure_contract") if isinstance(frozen.get("structure_contract"), dict) else {}
    execution_ref = frozen.get("execution_reference") if isinstance(frozen.get("execution_reference"), dict) else {}
    bundle = frozen.get("content_bundle_brief") if isinstance(frozen.get("content_bundle_brief"), dict) else {}
    creative = frozen.get("creative_diversity_contract") if isinstance(frozen.get("creative_diversity_contract"), dict) else {}
    if not contract or not execution_ref or not bundle or not creative:
        raise RuntimeError("冻结方向包缺少结构、执行卡、内容包或完整创意合同")

    # Load product context first
    ctx = load_product_context(
        item.product_code,
        target_country=batch.target_country,
        target_language=batch.target_language,
    )

    # Compile execution plan from contract
    anchor = ctx.get("anchor_card", {})
    cat_contract = anchor.get("category_execution_contract", {}) or {}
    exec_plan = frozen.get("structure_execution_plan") if isinstance(frozen.get("structure_execution_plan"), dict) else {}
    if not exec_plan.get("shot_plan"):
        exec_plan = compile_structure_execution_plan(contract, cat_contract)

    direction = {
        "output_slot": item.compatibility_slot,
        "direction_assignment_id": item.direction_assignment_id,
        "selection_run_id": item.selection_run_id,
        "cluster_id": item.cluster_id,
        "cluster_version": item.cluster_version,
        "evidence_tier": item.evidence_tier,
        "structure_contract": contract,
        "structure_execution_plan": exec_plan,
        "execution_reference": execution_ref,
        "content_bundle_brief": bundle,
        "p2_lite": frozen.get("p2_lite") if isinstance(frozen.get("p2_lite"), dict) and frozen.get("p2_lite") else {
            "p2_lite_schema_version": "p2-lite-compat-v2",
            "primary_observation": _text(bundle.get("content_mainline", "")),
            "primary_proof": "UNAVAILABLE",
            "secondary_fact": "UNAVAILABLE",
            "camera_reason": "UNAVAILABLE",
            "single_proof_rule": False,
        },
        "creative_diversity_contract": creative,
    }

    llm = OriginalScriptLLMClient(
        route="primary",
        primary_model=blueprint_model,
        primary_reasoning_effort=blueprint_reasoning,
        timeout=300,
        max_retries=0,
    )
    consumer_run_id = _stable_id("RUN_", {"batch_item_id": item.batch_item_id, "ts": time.time_ns()})
    blueprint_provenance = _blueprint_model_provenance(llm)
    checkpoint, checkpoint_matched = _load_stage_checkpoint(
        item, blueprint_provenance
    )
    stage_cache = {
        "checkpoint": "HIT" if checkpoint_matched else "MISS",
        "blueprint": "MISS",
        "visual": "MISS",
        "voiceover": "MISS",
    }

    # 1. Creative blueprint
    blueprint_stage = checkpoint.setdefault("stages", {}).setdefault("blueprint", {})
    blueprint = blueprint_stage.get("normalized")
    if isinstance(blueprint, dict) and blueprint:
        stage_cache["blueprint"] = "HIT_NORMALIZED"
    else:
        blueprint_raw = blueprint_stage.get("raw")
        if isinstance(blueprint_raw, dict) and blueprint_raw:
            stage_cache["blueprint"] = "HIT_RAW"
        else:
            try:
                blueprint_raw = llm.call_json(
                    build_complete_script_blueprint_prompt(
                        target_country=batch.target_country or "泰国",
                        product_type=batch.product_type or "外套",
                        direction=direction,
                    ),
                    max_tokens=6200,
                    max_attempts=1,
                    repair_json_on_failure=False,
                )
            except Exception as exc:
                _record_checkpoint_error(
                    storage, item, checkpoint, "blueprint", exc
                )
                raise
            blueprint_stage.update({
                "status": "GENERATED",
                "raw": blueprint_raw,
                "updated_at": _checkpoint_now(),
            })
            _persist_stage_checkpoint(storage, item, checkpoint)
        blueprint = _normalize_blueprint(
            blueprint_raw,
            creative,
            generation_provenance=blueprint_provenance,
        )
        blueprint_stage.update({
            "status": "NORMALIZED",
            "normalized": blueprint,
            "updated_at": _checkpoint_now(),
        })
        _persist_stage_checkpoint(storage, item, checkpoint)

    blueprint_check = validate_complete_blueprint(blueprint, creative)
    blueprint_stage.update({
        "status": "VALIDATED" if blueprint_check["valid"] else "INVALID",
        "validation": blueprint_check,
        "updated_at": _checkpoint_now(),
    })
    _persist_stage_checkpoint(storage, item, checkpoint)
    if not blueprint_check["valid"]:
        raise ValueError("完整蓝图校验失败：" + "；".join(blueprint_check["issues"][:10]))
    direction["creative_blueprint"] = blueprint
    direction["video_prompt_blueprint"] = video_prompt_projection(blueprint)

    # 2. Visual plan
    visual_dependency_hash = _stable_hash({
        "projection_version": VISUAL_PROJECTION_CHECKPOINT_VERSION,
        "blueprint_id": blueprint.get("creative_blueprint_id"),
        "execution_plan": direction.get("structure_execution_plan", {}),
        "execution_reference": execution_ref,
    })
    visual_stage = checkpoint.setdefault("stages", {}).setdefault("visual", {})
    visual_plan = visual_stage.get("plan")
    if (
        isinstance(visual_plan, dict)
        and visual_plan
        and visual_stage.get("dependency_hash") == visual_dependency_hash
    ):
        stage_cache["visual"] = "HIT"
    else:
        visual_plan = project_event_blueprint_to_visual_plan(direction=direction)
        visual_stage.update({
            "status": "GENERATED",
            "dependency_hash": visual_dependency_hash,
            "plan": visual_plan,
            "updated_at": _checkpoint_now(),
        })
        _persist_stage_checkpoint(storage, item, checkpoint)
    visual_check = validate_visual_adaptation(
        visual_plan,
        execution_plan=direction.get("structure_execution_plan", {}),
        execution_reference=execution_ref,
        content_bundle_brief=bundle,
        creative_blueprint=blueprint,
        creative_diversity_contract=creative,
    )
    visual_stage.update({
        "status": "VALIDATED" if visual_check["valid"] else "INVALID",
        "validation": visual_check,
        "updated_at": _checkpoint_now(),
    })
    _persist_stage_checkpoint(storage, item, checkpoint)
    if not visual_check["valid"]:
        raise ValueError("确定性视觉装配失败：" + "；".join(visual_check["issues"][:10]))

    # 3. Voiceover
    voiceover_dependency_hash = _stable_hash({
        "checkpoint_version": VOICEOVER_CHECKPOINT_VERSION,
        "visual_plan": visual_plan,
        "requested_hook_id": item.requested_hook_id,
        "content_bundle": bundle,
        "model_command_hash": _stable_hash(voiceover_model_command),
        "voiceover_qc_model_command_hash": _stable_hash(voiceover_qc_model_command),
    })
    voiceover_stage = checkpoint.setdefault("stages", {}).setdefault("voiceover", {})
    voiceover = voiceover_stage.get("plan")
    if (
        isinstance(voiceover, dict)
        and voiceover
        and voiceover_stage.get("dependency_hash") == voiceover_dependency_hash
    ):
        stage_cache["voiceover"] = "HIT"
    else:
        try:
            voiceover = run_central_complete_voiceover(
                product_code=item.product_code,
                target_country=batch.target_country,
                target_language=batch.target_language,
                top_category=batch.top_category,
                product_type=batch.product_type,
                direction=direction,
                visual_plan=visual_plan,
                voiceover_root=voiceover_root,
                voiceover_db_path=voiceover_db_path,
                model_command=voiceover_model_command,
                candidate_hook_id=item.requested_hook_id,
            )
        except Exception as exc:
            voiceover_stage["dependency_hash"] = voiceover_dependency_hash
            _record_checkpoint_error(
                storage, item, checkpoint, "voiceover", exc
            )
            raise
        voiceover_stage.update({
            "status": "VALIDATED",
            "dependency_hash": voiceover_dependency_hash,
            "plan": voiceover,
            "updated_at": _checkpoint_now(),
        })
        _persist_stage_checkpoint(storage, item, checkpoint)
    actual_hook = voiceover.get("hook_id", item.requested_hook_id)

    # 4. Assemble script
    script = assemble_reality_script(direction=direction, visual_plan=visual_plan, voiceover_plan=voiceover)
    complete_check = validate_complete_script(script)
    integrity_check = validate_batch_script_integrity(script, direction=direction)
    if not integrity_check["valid"]:
        raise ValueError("批次完整性校验失败：" + "；".join(integrity_check["issues"][:10]))
    first_shot = visual_plan.get("shots", [{}])[0]
    grounding_check = validate_voiceover_visual_grounding(
        voiceover,
        primary_observation=_text(direction.get("p2_lite", {}).get("primary_observation")),
        first_shot_content=" ".join(
            filter(
                None,
                [
                    _text(first_shot.get("shot_content")),
                    _text(first_shot.get("observable_action")),
                ],
            )
        ),
    )
    script["batch_execution_validation"] = {
        "blueprint": blueprint_check,
        "visual": visual_check,
        "complete_script": complete_check,
        "integrity": integrity_check,
        "voiceover_visual_grounding": grounding_check,
    }

    # 5. Bind structure
    binding_id = ""
    try:
        binding_id = bind_structure_application(
            contract=contract,
            consumer_run_id=consumer_run_id,
            record_id=batch.source_record_id or item.batch_item_id,
            product_code=item.product_code,
            application_stage="ORIGINAL_BATCH_SCRIPT_READY",
            script_id=_stable_id("SCRIPT_", script),
            content_id=_stable_id("CONTENT_", bundle),
            video_prompt_id=_stable_id("VP_", script.get("storyboard", [])),
            metadata={
                "batch_id": item.batch_id,
                "batch_item_id": item.batch_item_id,
                "test_phase": batch.test_phase,
                "content_angle_key": item.content_angle_key,
                "audience_tension_status": item.audience_tension_status,
                "requested_hook_id": item.requested_hook_id,
                "actual_hook_id": actual_hook,
                "visual_signature": creative.get("visual_signature", ""),
            },
        ) or ""
    except Exception:
        pass

    return {
        "status": "SUCCESS",
        "consumer_run_id": consumer_run_id,
        "script_id": _stable_id("SCRIPT_", script),
        "content_id": _stable_id("CONTENT_", bundle),
        "video_prompt_id": _stable_id("VP_", script.get("storyboard", [])),
        "actual_hook_id": actual_hook,
        "structure_binding_id": binding_id,
        "frozen_direction_package_schema_version": frozen.get("schema_version"),
        "stage_cache": stage_cache,
        "script": script,
    }
