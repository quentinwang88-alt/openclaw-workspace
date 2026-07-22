from __future__ import annotations

from typing import Any, Iterable

from .content_strategy import build_strategy_pool, sample_execution_variants
from .database import LightTryonDB
from .supplement_shots import compile_supplement_prompt, plan_supplement_shots
from .voiceover_adapter import build_voiceover_request, run_existing_voiceover_flow
from .voiceover_engine_bridge import run_voiceover_engine_variant


def plan_enhanced_variants(
    db: LightTryonDB,
    product_id: str,
    hooks: Iterable[dict[str, Any]],
    *,
    count: int,
    selling_points: Iterable[Any] | None = None,
    available_evidence: Iterable[str] | None = None,
    target_duration_seconds: int = 22,
    plan_version: str = "narrative-v1",
    random_seed: str | int | None = None,
    historical_usage: dict[str, Any] | None = None,
    production_batch_id: str = "",
) -> dict[str, Any]:
    product = db.get_product(product_id)
    if not product:
        raise KeyError(f"找不到商品: {product_id}")
    batch = db.get_or_create_content_batch(
        product_id,
        f"narrative:{plan_version}",
        canonical_product_id=str(product.get("source_product_code") or product_id),
    )
    resolved_batch_id = str(production_batch_id or batch["batch_id"])
    points = list(selling_points) if selling_points is not None else list(product.get("core_selling_points") or [])
    strategies = build_strategy_pool(
        product_id,
        hooks,
        points,
        available_evidence=available_evidence,
        plan_version=plan_version,
    )
    for strategy in strategies:
        db.upsert_content_strategy(strategy)
    variants = sample_execution_variants(
        strategies,
        count,
        product_id=product_id,
        target_duration_seconds=target_duration_seconds,
        plan_version=plan_version,
        random_seed=random_seed,
        historical_usage=historical_usage,
        production_batch_id=resolved_batch_id,
    )
    result = db.create_narrative_variants(variants)
    return {
        "product_id": product_id,
        "plan_version": plan_version,
        "strategy_count": len(strategies),
        "variant_count": len(variants),
        "historical_usage_count": int((historical_usage or {}).get("recent_count") or 0),
        "production_batch_id": resolved_batch_id,
        **result,
        "strategy_usage": _strategy_usage(variants),
        "variants": [row.to_dict() for row in variants],
    }


def generate_variant_voiceover(
    db: LightTryonDB,
    variant_id: str,
    command: str = "",
    *,
    available_evidence: Iterable[str] | None = None,
    timeout: int = 600,
    timeline: dict[str, Any] | None = None,
    voiceover_root: str | None = None,
    voiceover_db: str | None = None,
    model_command: str = "",
    qc_model_command: str = "",
) -> dict[str, Any]:
    variant = db.get_narrative_variant(variant_id)
    if not variant:
        raise KeyError(f"找不到增强型内容变体: {variant_id}")
    strategy = db.get_content_strategy(variant["strategy_group_id"])
    product = db.get_product(variant["product_id"])
    if not strategy or not product:
        raise ValueError("增强型内容变体缺少商品或内容策略")
    request = build_voiceover_request(product, strategy, variant, available_evidence=available_evidence)
    db.update_narrative_variant(
        variant_id,
        workflow_state="generating_voiceover",
        voiceover_status="generating",
        voiceover_request=request,
        last_error="",
    )
    try:
        if command:
            response = run_existing_voiceover_flow(command, request, timeout=timeout)
        else:
            if not timeline:
                raise ValueError("直接复用现有口播系统时必须提供实际画面时间轴")
            response = run_voiceover_engine_variant(
                product,
                strategy,
                variant,
                timeline,
                root=voiceover_root,
                db_path=voiceover_db,
                model_command=model_command,
                qc_model_command=qc_model_command,
            )
        updated = db.update_narrative_variant(
            variant_id,
            workflow_state="waiting_tts",
            voiceover_status="completed",
            voiceover_response=response,
            beat_plan=response["beats"],
            last_error="",
        )
        return {"variant_id": variant_id, "status": "completed", "variant": updated}
    except Exception as exc:
        db.update_narrative_variant(
            variant_id,
            workflow_state="voiceover_failed",
            voiceover_status="failed",
            voiceover_request=request,
            last_error=str(exc)[:2000],
        )
        raise


def plan_variant_supplements(
    db: LightTryonDB,
    variant_id: str,
    *,
    context: dict[str, Any],
    reference_assets: Iterable[str],
    max_generated_shots: int = 2,
) -> dict[str, Any]:
    variant = db.get_narrative_variant(variant_id)
    if not variant:
        raise KeyError(f"找不到增强型内容变体: {variant_id}")
    beats = variant.get("beat_plan") or []
    if not beats:
        raise ValueError("必须先完成口播并生成 Beat 计划")
    assets = db.list_media_assets(variant["product_id"])
    planned = plan_supplement_shots(
        variant_id,
        beats,
        assets,
        reference_assets=reference_assets,
        max_generated_shots=max_generated_shots,
    )
    for shot in planned:
        prompt = compile_supplement_prompt(shot, context)
        db.upsert_supplement_shot({**shot, "prompt_payload": prompt})
    state = "waiting_supplement_assets" if planned else "ready_to_mix"
    db.update_narrative_variant(variant_id, workflow_state=state)
    return {
        "variant_id": variant_id,
        "workflow_state": state,
        "planned_count": len(planned),
        "shots": db.list_supplement_shots(variant_id),
    }


def _strategy_usage(variants: Iterable[Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for variant in variants:
        strategy_id = str(getattr(variant, "strategy_group_id", "") or "")
        counts[strategy_id] = counts.get(strategy_id, 0) + 1
    return counts
