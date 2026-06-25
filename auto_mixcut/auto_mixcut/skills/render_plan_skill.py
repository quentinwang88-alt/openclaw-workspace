from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json
import os
from typing import Any

import yaml

from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result

from .context import SkillContext
from .hard_subtitle_policy import (
    REPAIRABLE_BOTTOM_CAPTION,
    classify_text_overlay,
    has_text_overlay_risk,
    is_repairable_bottom_caption,
    is_unusable_hard_subtitle,
)
from .usage_counter_skill import (
    FAILED_OUTPUT_SEGMENT_STATUSES,
    ads_fast_strict_outputs_enabled,
    is_good_rendered_output_strict,
)


DEFAULT_TEMPLATE_SPECS = [
    {"template_id": "GENERAL_BALANCED_15S", "duration_ms": 15000, "slots": [{"role": role, "duration_ms": 3000} for role in ["hero", "detail", "result", "scene", "ending"]]},
    {"template_id": "CLEAN_PRODUCT_PROOF_15S", "duration_ms": 15000, "slots": [{"role": role, "duration_ms": 3000} for role in ["hero", "detail", "hero", "scene", "ending"]]},
    {"template_id": "DETAIL_TEXTURE_PROOF_15S", "duration_ms": 15000, "slots": [{"role": role, "duration_ms": 3000} for role in ["hero", "detail", "detail", "result", "ending"]]},
    {"template_id": "TRYON_RESULT_FAST_15S", "duration_ms": 15000, "slots": [{"role": role, "duration_ms": 3000} for role in ["result", "hero", "detail", "result", "ending"]]},
]
TRUST_RANK = {"low": 1, "medium": 2, "high": 3}
CORE_ROLES = {"hero", "detail", "result"}
REAL_SOURCE_TYPES = {"authorized_creator", "self_shot", "original_script", "creator_original"}
FALLBACK_TEMPLATE_ID = "GENERAL_BALANCED_15S"
MIN_RENDER_PLAN_DURATION_MS = 12000
MAX_SEGMENT_REUSE_PER_BATCH = 2
MAX_SUPPORT_SEGMENT_REUSE_PER_BATCH = 3
MAX_FIRST_SEGMENT_REUSE_PER_BATCH = 1
MAX_FIRST_ASSET_REUSE_PER_BATCH = 2
MAX_TEMPLATE_REUSE_PER_BATCH = 2
FILL_MODE_SEGMENT_REUSE_PER_BATCH = 3
FILL_MODE_SUPPORT_SEGMENT_REUSE_PER_BATCH = 3
FILL_MODE_FIRST_SEGMENT_REUSE_PER_BATCH = 2
FILL_MODE_FIRST_ASSET_REUSE_PER_BATCH = 4
FILL_MODE_TEMPLATE_REUSE_PER_BATCH = 3
FINAL_FILL_FIRST_SEGMENT_REUSE_PER_BATCH = 3
FINAL_FILL_FIRST_ASSET_REUSE_PER_BATCH = 5
FINAL_FILL_TEMPLATE_REUSE_PER_BATCH = 4


@dataclass(frozen=True)
class TemplateSpec:
    template_id: str
    duration_ms: int
    slots: list[dict[str, Any]]
    default_moods: list[str]
    suitable_categories: list[str]
    template_objective: str
    pacing: str
    required_roles: list[str]
    risk_policy: dict[str, Any]
    source_policy: dict[str, Any]
    bgm_profile: dict[str, Any]
    # 投流模板专属选片策略（改动3 预埋）。种草模板无此键 → 空 dict → 行为不变。
    # 消费方：_build_template_constraints → constraints → _segment_score / _filter_constraints。
    selection_policy: dict[str, Any] = field(default_factory=dict)


class RenderPlanSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def create_plans(self, product_id: str, count: int | None = None, fill_gap_only: bool = True, template_id: str | None = None) -> Result:
        task = _task(self.ctx, product_id)
        allowed = int((task or {}).get("allowed_variant_count") or 0)
        target_total = min(count or allowed, allowed)
        existing_outputs = _usable_existing_outputs(self.ctx, product_id) if fill_gap_only else []
        existing_count = len(existing_outputs)
        total = max(0, target_total - existing_count) if fill_gap_only else target_total
        if target_total <= 0:
            return Result.fail("MATERIAL_NOT_READY", "allowed_variant_count is zero", {"product_id": product_id})
        if total <= 0:
            requested_total = int((task or {}).get("requested_variant_count") or target_total)
            capped_before_requested_target = fill_gap_only and requested_total > existing_count and target_total <= existing_count
            self.ctx.repo.update(
                "content_tasks",
                "product_id",
                product_id,
                {
                    "task_status": "RENDER_PLAN_SKIPPED_ALLOWED_CAP" if capped_before_requested_target else "RENDER_PLAN_SKIPPED_ALREADY_FILLED",
                    "allowed_variant_count": target_total,
                    "actual_variant_count": existing_count,
                    "blocked_reason": (
                        f"补差额: 目标={requested_total}; 已有效={existing_count}; 当前允许={target_total}; 等待AI补素材释放更多容量"
                        if capped_before_requested_target
                        else ""
                    ),
                    "failure_reason": "",
                },
            )
            from .feishu_review_skill import sync_product_task_best_effort

            task_sync = sync_product_task_best_effort(self.ctx, product_id)
            return Result.ok({
                "batch_id": "",
                "render_plan_ids": [],
                "skipped_render_plan_ids": [],
                "fill_gap_only": fill_gap_only,
                "target_variant_count": target_total,
                "requested_variant_count": requested_total,
                "existing_usable_outputs": existing_count,
                "fill_gap_count": 0,
                "task_sync": task_sync,
            })
        active_batch = _active_planning_batch(self.ctx, product_id)
        if fill_gap_only and active_batch:
            self.ctx.repo.update(
                "content_tasks",
                "product_id",
                product_id,
                {
                    "task_status": "RENDER_PLAN_SKIPPED_ACTIVE_BATCH",
                    "actual_variant_count": existing_count,
                    "blocked_reason": f"已有未完成混剪批次 {active_batch.get('batch_id')}，等待其渲染/同步完成或先 abort 冗余批次",
                    "failure_reason": "",
                },
            )
            from .feishu_review_skill import sync_product_task_best_effort

            task_sync = sync_product_task_best_effort(self.ctx, product_id)
            return Result.ok({
                "batch_id": "",
                "render_plan_ids": [],
                "skipped_render_plan_ids": [],
                "fill_gap_only": fill_gap_only,
                "target_variant_count": target_total,
                "existing_usable_outputs": existing_count,
                "fill_gap_count": 0,
                "skipped": True,
                "reason": "active_planning_batch_exists",
                "active_batch_id": active_batch.get("batch_id"),
                "task_sync": task_sync,
            })
        pending_output_batch = _active_pending_output_batch(self.ctx, product_id)
        if fill_gap_only and pending_output_batch:
            self.ctx.repo.update(
                "content_tasks",
                "product_id",
                product_id,
                {
                    "task_status": "RENDER_PLAN_SKIPPED_PENDING_OUTPUT_QC",
                    "actual_variant_count": existing_count,
                    "blocked_reason": f"已有未完成质检/同步的成片批次 {pending_output_batch.get('batch_id')}，等待 quality/sync 完成后再补差额",
                    "failure_reason": "",
                },
            )
            from .feishu_review_skill import sync_product_task_best_effort

            task_sync = sync_product_task_best_effort(self.ctx, product_id)
            return Result.ok({
                "batch_id": "",
                "render_plan_ids": [],
                "skipped_render_plan_ids": [],
                "fill_gap_only": fill_gap_only,
                "target_variant_count": target_total,
                "existing_usable_outputs": existing_count,
                "fill_gap_count": 0,
                "skipped": True,
                "reason": "pending_output_qc_exists",
                "active_batch_id": pending_output_batch.get("batch_id"),
                "task_sync": task_sync,
            })
        templates = _load_templates(self.ctx)
        # 投流模板（fallback_only）指定时单独加载
        if template_id:
            forced = _load_template_by_id(self.ctx, template_id)
            if forced:
                templates = [forced]
            else:
                return Result.fail("TEMPLATE_NOT_FOUND", f"template {template_id} not found", {"template_id": template_id})
        product = self.ctx.repo.get("products", "product_id", product_id) or {}
        batch_id = new_id("BATCH")
        self.ctx.repo.upsert(
            "mixcut_batches",
            "batch_id",
            {
                "batch_id": batch_id,
                "product_id": product_id,
                "task_id": (task or {}).get("task_id"),
                "requested_count": target_total,
                "allowed_count": total,
                "rendered_count": 0,
                "batch_status": "planning",
                "material_tier": (task or {}).get("material_tier"),
                "template_pool_json": [t.template_id for t in templates],
                "experiment_batch": "fill_gap" if fill_gap_only and existing_count else "full_batch",
            },
        )
        plans = []
        skipped = []
        batch_state = {"segments": set(), "segment_counts": {}, "core_segment_counts": {}, "assets": {}, "first_assets": set(), "first_asset_counts": {}, "first_segment_counts": {}, "template_counts": {}}
        variant_start = existing_count + 1 if fill_gap_only else 1
        for offset in range(total):
            variant = variant_start + offset
            excluded_templates: set[str] = set()
            choice: dict[str, Any] = {}
            template: TemplateSpec | None = None
            selected: Result | None = None
            last_skip: dict[str, Any] = {}
            while True:
                choice = _choose_template(self.ctx, product, templates, batch_state, variant, excluded_templates)
                if not choice.get("template"):
                    if _advance_reuse_mode(batch_state, variant):
                        excluded_templates = set()
                        last_skip = {}
                        continue
                    break
                template = choice["template"]
                # 改动3：从模板 selection_policy 组装 constraints（种草模板返回空 dict，零侵入）。
                # 改动1：投流模板的 hook_weight_scale 经此注入 _segment_score。
                template_constraints = _build_template_constraints(template)
                selected = _select_segments(self.ctx, product_id, template.slots, batch_state=batch_state, variant_no=variant, template=template, constraints=template_constraints)
                if selected.success:
                    break
                if selected.error and selected.error.code == "SKIPPED_LOW_QUALITY":
                    last_skip = {"template": template, "choice": choice, "detail": selected.error.detail}
                    excluded_templates.add(template.template_id)
                    continue
                return selected
            if not selected or not selected.success or not template:
                if last_skip:
                    skipped_plan_id = new_id("PLAN")
                    skipped_template = last_skip["template"]
                    row = {
                        "render_plan_id": skipped_plan_id,
                        "batch_id": batch_id,
                        "product_id": product_id,
                        "variant_no": variant,
                        "template_id": skipped_template.template_id,
                        "planned_duration_ms": 0,
                        "plan_json": {
                            "segments": [],
                            "template": _template_plan_json(skipped_template),
                            "template_selection": last_skip["choice"]["debug"],
                            "reuse_mode": batch_state.get("reuse_mode", "strict"),
                            "skip_reason": last_skip["detail"],
                        },
                        "quality_gate_status": "skipped_low_quality",
                        "render_status": "skipped_low_quality",
                    }
                    self.ctx.repo.upsert("render_plans", "render_plan_id", row)
                    _sync_material_supplement_queue(self.ctx, row, last_skip["detail"])
                    skipped.append(skipped_plan_id)
                break
            _record_template_choice(template, batch_state)
            plan_id = new_id("PLAN")
            cursor = 0
            slots = []
            for slot_index, item in enumerate(selected.data, start=1):
                dur = int(item.get("duration_ms") or 3000)
                slots.append({
                    "slot": slot_index,
                    "role": item["role"],
                    "segment_type": item.get("segment_type"),
                    "hook_intent": item.get("hook_intent"),
                    "ai_gen_grade": item.get("ai_gen_grade"),
                    "segment_id": item["segment_id"],
                    "asset_id": item["asset_id"],
                    "source_type": item.get("source_type"),
                    "source_identity": item.get("source_identity"),
                    "prompt_package_id": item.get("prompt_package_id"),
                    "asset_scene_tag": item.get("asset_scene_tag"),
                    "asset_slot_role": item.get("asset_slot_role"),
                    "asset_ai_gen_grade": item.get("asset_ai_gen_grade"),
                    "asset_hook_intent": item.get("asset_hook_intent"),
                    "selection_score": item.get("selection_score"),
                    "selection_reason": item.get("selection_reason"),
                    "start_ms_in_output": cursor,
                    "end_ms_in_output": cursor + dur,
                    "subtitle_cleanup": item.get("subtitle_cleanup") or {"action": "none"},
                })
                cursor += dur
            row = {
                "render_plan_id": plan_id,
                "batch_id": batch_id,
                "product_id": product_id,
                "variant_no": variant,
                "template_id": template.template_id,
                "planned_duration_ms": cursor,
                "plan_json": {
                    "segments": slots,
                    "template": _template_plan_json(template),
                    "template_selection": choice["debug"],
                    "reuse_mode": batch_state.get("reuse_mode", "strict"),
                },
                "quality_gate_status": "pending",
                "render_status": "planned",
            }
            self.ctx.repo.upsert("render_plans", "render_plan_id", row)
            _record_selection(self.ctx, selected.data, batch_state)
            plans.append(plan_id)
        # render_plans 全部创建完后，把 batch 状态从 planning 推进到 planned，
        # 否则 render_batch / _top_up 会认为批次还在 planning 而跳过渲染。
        self.ctx.repo.update(
            "mixcut_batches",
            "batch_id",
            batch_id,
            {
                "batch_status": "planned",
                "planned_count": len(plans),
            },
        )
        self.ctx.repo.update(
            "content_tasks",
            "product_id",
            product_id,
            {
                "task_status": "RENDER_PLAN_CREATED",
                "allowed_variant_count": target_total,
                "actual_variant_count": existing_count,
                "blocked_reason": _top_up_summary(target_total, existing_count, len(plans), len(skipped), (task or {}).get("blocked_reason")),
            },
        )
        from .feishu_review_skill import sync_product_task_best_effort

        task_sync = sync_product_task_best_effort(self.ctx, product_id)
        return Result.ok({
            "batch_id": batch_id,
            "render_plan_ids": plans,
            "skipped_render_plan_ids": skipped,
            "fill_gap_only": fill_gap_only,
            "target_variant_count": target_total,
            "existing_usable_outputs": existing_count,
            "fill_gap_count": total,
            "task_sync": task_sync,
        })

    def create_remix_plan(self, source_output: dict, remix_plan: dict) -> Result:
        product_id = source_output["product_id"]
        template = _choose_remix_template(self.ctx, source_output, remix_plan)
        slots = _remix_slots(template, remix_plan)
        batch_id = new_id("BATCH")
        self.ctx.repo.upsert(
            "mixcut_batches",
            "batch_id",
            {
                "batch_id": batch_id,
                "product_id": product_id,
                "task_id": "",
                "requested_count": 1,
                "allowed_count": 1,
                "rendered_count": 0,
                "batch_status": "planning",
                "material_tier": "remix",
                "template_pool_json": [template.template_id],
                "experiment_batch": f"remix:{source_output['output_id']}",
            },
        )
        selected = _select_segments(self.ctx, product_id, slots, variant_no=1, constraints=remix_plan.get("constraints") or {})
        if not selected.success:
            return selected
        plan_id = new_id("PLAN")
        cursor = 0
        selected_slots = []
        for slot_index, item in enumerate(selected.data, start=1):
            dur = int(item.get("duration_ms") or 3000)
            selected_slots.append({
                "slot": slot_index,
                "role": item["role"],
                "segment_type": item.get("segment_type"),
                "hook_intent": item.get("hook_intent"),
                "ai_gen_grade": item.get("ai_gen_grade"),
                "segment_id": item["segment_id"],
                "asset_id": item["asset_id"],
                "start_ms_in_output": cursor,
                "end_ms_in_output": cursor + dur,
                "subtitle_cleanup": item.get("subtitle_cleanup") or {"action": "none"},
            })
            cursor += dur
        row = {
            "render_plan_id": plan_id,
            "batch_id": batch_id,
            "product_id": product_id,
            "variant_no": int(source_output.get("variant_no") or 1),
            "template_id": template.template_id,
            "planned_duration_ms": cursor,
            "plan_json": {
                "segments": selected_slots,
                "template": {"template_id": template.template_id, "default_moods": template.default_moods},
                "remix": remix_plan,
                "source_output_id": source_output["output_id"],
            },
            "quality_gate_status": "pending",
            "render_status": "planned",
        }
        self.ctx.repo.upsert("render_plans", "render_plan_id", row)
        _record_selection(self.ctx, selected.data, {"segments": set(), "assets": {}, "first_assets": set(), "first_asset_counts": {}, "first_segment_counts": {}})
        return Result.ok({"batch_id": batch_id, "render_plan_id": plan_id})


def estimate_render_plan_capacity(ctx: SkillContext, product_id: str, count: int, allow_fill_mode: bool = False) -> dict[str, Any]:
    if count <= 0:
        return {"planned_count": 0, "skipped_count": 0, "template_counts": {}, "segment_counts": {}}
    templates = _load_templates(ctx)
    product = ctx.repo.get("products", "product_id", product_id) or {}
    segments = ctx.repo.list_where("segments", "product_id=?", (product_id,))
    segments = _render_eligible_segments(_enrich_segments_for_selection(ctx, segments))
    first_slot_candidates = sum(1 for segment in segments if "hero" in (segment.get("effective_roles_json") or []))
    batch_state = {"segments": set(), "segment_counts": {}, "core_segment_counts": {}, "assets": {}, "first_assets": set(), "first_asset_counts": {}, "first_segment_counts": {}, "template_counts": {}}
    batch_state["_selection_segments"] = segments
    planned = 0
    skipped = 0
    for variant in range(1, count + 1):
        excluded_templates: set[str] = set()
        selected: Result | None = None
        template: TemplateSpec | None = None
        last_skip = None
        while True:
            choice = _choose_template(ctx, product, templates, batch_state, variant, excluded_templates)
            if not choice.get("template"):
                if allow_fill_mode and _advance_reuse_mode(batch_state, variant):
                    excluded_templates = set()
                    last_skip = None
                    continue
                break
            template = choice["template"]
            selected = _select_segments(ctx, product_id, template.slots, batch_state=batch_state, variant_no=variant, template=template, constraints=_build_template_constraints(template))
            if selected.success:
                break
            if selected.error and selected.error.code == "SKIPPED_LOW_QUALITY":
                last_skip = selected.error.detail
                excluded_templates.add(template.template_id)
                continue
            last_skip = selected.error.detail if selected.error else {}
            break
        if not selected or not selected.success or not template:
            if last_skip is not None:
                skipped += 1
            break
        _record_template_choice(template, batch_state)
        _record_selection_state_only(selected.data, batch_state)
        planned += 1
    return {
        "planned_count": planned,
        "skipped_count": skipped,
        "template_counts": dict(batch_state.get("template_counts") or {}),
        "segment_counts": dict(batch_state.get("segment_counts") or {}),
        "reuse_mode": batch_state.get("reuse_mode", "strict"),
        "fill_mode_activated_at_variant": batch_state.get("fill_mode_activated_at_variant"),
        "final_fill_mode_activated_at_variant": batch_state.get("final_fill_mode_activated_at_variant"),
        "first_slot_candidates": first_slot_candidates,
        "first_slot_capacity": first_slot_candidates * FILL_MODE_FIRST_SEGMENT_REUSE_PER_BATCH,
    }


def _usable_existing_outputs(ctx: SkillContext, product_id: str) -> list[dict[str, Any]]:
    outputs = ctx.repo.list_where("outputs", "product_id=? ORDER BY created_at, id", (product_id,))
    return [
        output
        for output in outputs
        if is_good_rendered_output_strict(ctx, output, strict_segments=ads_fast_strict_outputs_enabled())
    ]


def _active_planning_batch(ctx: SkillContext, product_id: str) -> dict[str, Any] | None:
    batches = ctx.repo.list_where(
        "mixcut_batches",
        "product_id=? AND batch_status='planning' ORDER BY id DESC",
        (product_id,),
    )
    for batch in batches:
        batch_id = str(batch.get("batch_id") or "")
        if _batch_has_active_work(ctx, batch_id):
            return batch
        if _is_stale_batch(batch):
            ctx.repo.update(
                "mixcut_batches",
                "batch_id",
                batch_id,
                {
                    "batch_status": "aborted_stale_planning",
                    "updated_at": datetime.utcnow().isoformat(timespec="seconds"),
                },
            )
            continue
        return batch
    return None


def _active_pending_output_batch(ctx: SkillContext, product_id: str) -> dict[str, Any] | None:
    outputs = ctx.repo.list_where(
        "outputs",
        "product_id=? AND render_status='rendered' AND (machine_quality_status IS NULL OR machine_quality_status='pending') ORDER BY id DESC LIMIT 1",
        (product_id,),
    )
    for output in outputs:
        if str(output.get("human_quality_status") or "").lower() in {"rejected", "aborted"}:
            continue
        batch_id = str(output.get("batch_id") or "")
        if _is_aborted_batch(ctx, batch_id):
            continue
        if _output_plan_is_aborted(ctx, str(output.get("output_id") or "")):
            continue
        return {"batch_id": batch_id} if batch_id else {"batch_id": ""}
    return None


def _batch_has_active_work(ctx: SkillContext, batch_id: str) -> bool:
    if not batch_id:
        return False
    plans = ctx.repo.list_where("render_plans", "batch_id=?", (batch_id,))
    outputs = ctx.repo.list_where("outputs", "batch_id=?", (batch_id,))
    if not plans and not outputs:
        return False
    terminal = {"rendered", "aborted", "failed", "skipped", "done"}
    for plan in plans:
        status = str(plan.get("render_status") or plan.get("quality_gate_status") or plan.get("plan_status") or "").strip().lower()
        if status and status not in terminal:
            return True
        if not status:
            return True
    for output in outputs:
        machine_status = str(output.get("machine_quality_status") or "").strip().lower()
        human_status = str(output.get("human_quality_status") or "").strip().lower()
        if human_status in {"rejected", "aborted"}:
            continue
        if machine_status in {"", "pending"}:
            return True
    return False


def _is_stale_batch(batch: dict[str, Any]) -> bool:
    created_at = _as_datetime(batch.get("created_at"))
    if not created_at:
        return False
    try:
        stale_hours = max(1, int(os.environ.get("AUTO_MIXCUT_STALE_PLANNING_BATCH_HOURS", "2") or "2"))
    except ValueError:
        stale_hours = 2
    return datetime.utcnow() - created_at > timedelta(hours=stale_hours)


def _is_aborted_batch(ctx: SkillContext, batch_id: str) -> bool:
    if not batch_id:
        return False
    batch = ctx.repo.get("mixcut_batches", "batch_id", batch_id) or {}
    return str(batch.get("batch_status") or "").startswith("aborted")


def _output_plan_is_aborted(ctx: SkillContext, output_id: str) -> bool:
    if not output_id:
        return False
    plans = ctx.repo.list_where("render_plans", "output_id=?", (output_id,))
    return bool(plans) and all(str(plan.get("render_status") or "").lower() == "aborted" for plan in plans)


def _as_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return datetime.fromisoformat(value.strip().replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            return None
    return None


def _top_up_summary(target_total: int, existing_count: int, planned_count: int, skipped_count: int, previous_reason: object = "") -> str:
    summary = f"补差额: 目标={target_total}; 已有效={existing_count}; 本轮计划={planned_count}; 跳过={skipped_count}"
    previous = str(previous_reason or "").strip()
    if "AI补素材" in previous and existing_count + planned_count < target_total:
        return summary + "; " + previous
    return summary


def _select_segments(ctx: SkillContext, product_id: str, slots, batch_state: dict | None = None, variant_no: int = 1, constraints: dict | None = None, template: TemplateSpec | None = None) -> Result:
    constraints = constraints or {}
    selected = []
    state = batch_state or {"segments": set(), "assets": {}, "first_assets": set()}
    segments = state.get("_selection_segments")
    if segments is None:
        segments = ctx.repo.list_where("segments", "product_id=?", (product_id,))
        segments = _enrich_segments_for_selection(ctx, segments)
        segments = _render_eligible_segments(segments)
        state["_selection_segments"] = segments
    for slot in slots:
        role = slot["role"]
        slot_index = len(selected) + 1
        pool = [s for s in segments if role in (s.get("effective_roles_json") or [])]
        if not pool and role == "hero":
            pool = _hero_fallback_pool(segments)
        pool = _filter_product_mismatch_suspects(pool)
        pool = _filter_ai_core_prompt_packages(ctx, product_id, pool, state, role, slot_index)
        pool = _filter_constraints(ctx, pool, constraints, slot_index=slot_index)
        filtered = _filter_slot_pool(pool, slot)
        if filtered:
            pool = filtered
        if not pool:
            return Result.fail(
                "RENDER_PLAN_FAILED",
                f"no segment available for role {role}",
                {"role": role, "reason": "candidate_pool_empty_after_safety_filters"},
            )
        clean_or_repairable_pool = [s for s in pool if not _has_unusable_subtitle_risk(s)]
        if not clean_or_repairable_pool:
            return Result.fail("RENDER_PLAN_FAILED", f"no clean segment available for role {role}", {"role": role, "reason": "hard subtitle unusable"})
        pool = clean_or_repairable_pool
        if slot_index <= 3:
            no_subtitle_pool = [s for s in pool if not _needs_subtitle_crop(s)]
            if no_subtitle_pool:
                pool = no_subtitle_pool
        if slot_index == 1 and template:
            first_slot_pool = [s for s in pool if _passes_first_slot_floor(ctx, s, template.risk_policy)[0]]
            if not first_slot_pool:
                failures = [_first_slot_failure(ctx, s, template.risk_policy) for s in pool]
                return Result.fail(
                    "SKIPPED_LOW_QUALITY",
                    "no first-slot segment passes hard quality floor",
                    {"status": "skipped_low_quality", "template_id": template.template_id, "role": role, "first_slot_failures": failures},
                )
            pool = first_slot_pool
        selected_segment_ids = {item["segment_id"] for item in selected}
        non_duplicate_pool = [s for s in pool if s["segment_id"] not in selected_segment_ids]
        if non_duplicate_pool:
            pool = non_duplicate_pool
        else:
            return Result.fail("SKIPPED_LOW_QUALITY", "within-output segment reuse cap exhausted", {"role": role, "slot_index": slot_index})
        capped_pool = _filter_batch_reuse_caps(pool, state, role, slot_index)
        if capped_pool:
            pool = capped_pool
        else:
            return Result.fail(
                "SKIPPED_LOW_QUALITY",
                "batch reuse caps exhausted for slot",
                {
                    "role": role,
                    "slot_index": slot_index,
                    "reuse_mode": state.get("reuse_mode", "strict"),
                    "segment_reuse_cap": _segment_reuse_cap_for_slot(state, role, slot_index),
                    "first_segment_reuse_cap": _first_segment_reuse_cap(state),
                    "first_asset_reuse_cap": _first_asset_reuse_cap(state),
                },
            )
        if slot_index == 1 and template:
            preferred_first_pool = _prefer_first_slot_pool(ctx, pool)
            if preferred_first_pool:
                pool = preferred_first_pool
            unused_first_asset_pool = [s for s in pool if s.get("asset_id") not in state.get("first_assets", set())]
            if unused_first_asset_pool:
                pool = unused_first_asset_pool
        unique_source_locked = False
        unique_source_pool, unique_source_locked = _filter_min_unique_source_assets(pool, selected, slots, slot_index, constraints)
        if unique_source_pool:
            pool = unique_source_pool
        if slot_index > 1 and role in {"detail", "result"}:
            non_hero_core_pool = [s for s in pool if "hero" not in (s.get("effective_roles_json") or [])]
            if non_hero_core_pool and not unique_source_locked:
                pool = non_hero_core_pool
        unused_asset_pool = _filter_unused_asset(pool, selected)
        if unused_asset_pool:
            pool = unused_asset_pool
        non_consecutive_asset_pool = _filter_non_consecutive_asset(pool, selected)
        if non_consecutive_asset_pool:
            pool = non_consecutive_asset_pool
        core_asset_pool = _filter_core_asset_reuse(pool, selected, role)
        if core_asset_pool:
            pool = core_asset_pool
        sufficient_duration_pool = _filter_sufficient_duration(pool, slot)
        if sufficient_duration_pool:
            pool = sufficient_duration_pool
        if not pool:
            return Result.fail("RENDER_PLAN_FAILED", f"no segment available for role {role}", {"role": role})
        scored = sorted(
            ((_segment_score(ctx, item, selected, state, slot, slot_index, variant_no, constraints), item) for item in pool),
            key=lambda item: (item[0], item[1]["segment_id"]),
            reverse=True,
        )
        best_score, choice = scored[0]
        selected.append({
            "role": role,
            "duration_ms": _slot_duration_ms(slot, choice),
            "_source_duration_ms": _segment_duration_ms(choice, int(slot.get("duration_ms") or 3000)),
            "segment_type": slot.get("segment_type"),
            "hook_intent": slot.get("hook_intent"),
            "ai_gen_grade": slot.get("ai_gen_grade"),
            "segment_id": choice["segment_id"],
            "asset_id": choice["asset_id"],
            "source_type": choice.get("source_type"),
            "source_trust_level": choice.get("source_trust_level"),
            "source_identity": choice.get("source_identity"),
            "prompt_package_id": _ai_prompt_package_id(choice),
            "asset_scene_tag": choice.get("scene_tag"),
            "asset_slot_role": choice.get("slot_role"),
            "asset_ai_gen_grade": choice.get("ai_gen_grade"),
            "asset_hook_intent": choice.get("hook_intent"),
            "selection_score": round(float(best_score), 3),
            "selection_reason": _selection_reason(ctx, choice, state, slot, slot_index, best_score),
            "subtitle_cleanup": _subtitle_cleanup_plan(choice),
        })
    _rebalance_selected_durations(selected, _min_plan_duration_ms(template, slots))
    planned_duration = sum(int(item.get("duration_ms") or 0) for item in selected)
    min_duration = _min_plan_duration_ms(template, slots)
    if planned_duration < min_duration:
        return Result.fail(
            "SKIPPED_LOW_QUALITY",
            "selected segment durations are below minimum render duration",
            {"status": "skipped_low_quality", "planned_duration_ms": planned_duration, "min_duration_ms": min_duration},
        )
    unique_target = _min_unique_source_asset_target(constraints, slots)
    if unique_target:
        unique_assets = {str(item.get("asset_id") or "") for item in selected if item.get("asset_id")}
        if len(unique_assets) < unique_target:
            return Result.fail(
                "SKIPPED_LOW_QUALITY",
                "selected source asset diversity is below template minimum",
                {
                    "status": "skipped_low_quality",
                    "unique_source_assets": len(unique_assets),
                    "min_unique_source_assets": unique_target,
                },
            )
    return Result.ok(selected)


def _filter_min_unique_source_assets(
    pool: list[dict],
    selected: list[dict],
    slots: list[dict],
    slot_index: int,
    constraints: dict,
) -> tuple[list[dict], bool]:
    """Prefer a new asset while a template-level unique-source floor is still reachable."""
    unique_target = _min_unique_source_asset_target(constraints, slots)
    if unique_target <= 0:
        return pool, False
    selected_assets = {str(item.get("asset_id") or "") for item in selected if item.get("asset_id")}
    if len(selected_assets) >= unique_target:
        return pool, False
    remaining_after_current = max(0, len(slots) - slot_index)
    # If we can still hit the target, force this slot to contribute a fresh asset.
    if len(selected_assets) + 1 + remaining_after_current < unique_target:
        return pool, False
    fresh_pool = [s for s in pool if str(s.get("asset_id") or "") not in selected_assets]
    return (fresh_pool, True) if fresh_pool else (pool, False)


def _min_unique_source_asset_target(constraints: dict, slots: list[dict]) -> int:
    try:
        target = int((constraints or {}).get("min_unique_source_assets") or 0)
    except (TypeError, ValueError):
        target = 0
    return min(max(target, 0), len(slots))


def _hero_fallback_pool(segments: list[dict]) -> list[dict]:
    return [
        s
        for s in segments
        if {"result", "detail"}.intersection(s.get("effective_roles_json") or [])
    ]


def _filter_product_mismatch_suspects(pool: list[dict]) -> list[dict]:
    return [s for s in pool if not _is_product_mismatch_suspect(s)]


def _render_eligible_segments(segments: list[dict]) -> list[dict]:
    return [segment for segment in segments if _is_render_eligible_segment(segment)]


def _is_render_eligible_segment(segment: dict) -> bool:
    status = str(segment.get("segment_status") or "").strip()
    return status not in FAILED_OUTPUT_SEGMENT_STATUSES


def _is_product_mismatch_suspect(segment: dict) -> bool:
    value = segment.get("product_mismatch_suspect")
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "是"}


def _filter_ai_core_prompt_packages(
    ctx: SkillContext,
    product_id: str,
    pool: list[dict],
    state: dict,
    role: str,
    slot_index: int,
) -> list[dict]:
    if role not in CORE_ROLES and slot_index != 1:
        return pool
    valid_ids = _valid_prompt_package_ids_for_selection(ctx, product_id, state)
    filtered = []
    for segment in pool:
        if str(segment.get("source_type") or "") != "ai_generated":
            filtered.append(segment)
            continue
        prompt_id = _ai_prompt_package_id(segment)
        if prompt_id and prompt_id in valid_ids:
            filtered.append(segment)
    return filtered


def _valid_prompt_package_ids_for_selection(ctx: SkillContext, product_id: str, state: dict) -> set[str]:
    cache_key = f"_valid_prompt_package_ids:{product_id}"
    if cache_key in state:
        return set(state.get(cache_key) or set())
    valid_statuses = {"imported", "consumed", "downloaded", "uploaded", "已回流", "质检通过", "已消费"}
    invalid_statuses = {"created", "submitted", "generating", "failed", "待提单", "已提单", "生成中", "失败"}
    valid: set[str] = set()
    try:
        rows = ctx.repo.list_where("segment_prompt_packages", "product_id=?", (product_id,))
    except Exception:
        rows = []
    for row in rows:
        prompt_id = str(row.get("segment_prompt_id") or "").strip()
        if not prompt_id:
            continue
        status = str(row.get("package_status") or row.get("result_sync_status") or "").strip()
        if status in invalid_statuses and not (row.get("generated_asset_id") or row.get("generated_segment_id")):
            continue
        if status in valid_statuses or row.get("generated_asset_id") or row.get("generated_segment_id"):
            valid.add(prompt_id)
    state[cache_key] = valid
    return valid


def _ai_prompt_package_id(segment: dict) -> str:
    if str(segment.get("source_type") or "") != "ai_generated":
        return ""
    return str(segment.get("prompt_package_id") or "").strip()


def _slot_duration_ms(slot: dict, segment: dict) -> int:
    target = int(slot.get("duration_ms") or 3000)
    actual = _segment_duration_ms(segment, target)
    if actual <= 0:
        return target
    return max(800, min(target, actual))


def _segment_duration_ms(segment: dict, fallback: int) -> int:
    try:
        actual = int(segment.get("duration_ms") or fallback)
    except (TypeError, ValueError):
        actual = fallback
    return actual if actual > 0 else fallback


def _min_plan_duration_ms(template: TemplateSpec | None, slots: list[dict]) -> int:
    target = int((template.duration_ms if template else 0) or sum(int(slot.get("duration_ms") or 3000) for slot in slots))
    return min(MIN_RENDER_PLAN_DURATION_MS, max(500, target))


def _rebalance_selected_durations(selected: list[dict], min_duration_ms: int) -> None:
    missing = min_duration_ms - sum(int(item.get("duration_ms") or 0) for item in selected)
    if missing <= 0:
        return
    for item in sorted(selected, key=lambda row: 0 if row.get("role") in {"detail", "scene", "ending"} else 1):
        capacity = int(item.get("_source_duration_ms") or 0) - int(item.get("duration_ms") or 0)
        if capacity <= 0:
            continue
        extra = min(capacity, missing)
        item["duration_ms"] = int(item.get("duration_ms") or 0) + extra
        missing -= extra
        if missing <= 0:
            return


def _segment_score(ctx: SkillContext, segment: dict, selected: list[dict], batch_state: dict, slot: dict, slot_index: int, variant_no: int, constraints: dict | None = None) -> float:
    constraints = constraints or {}
    selected_segments = {item["segment_id"] for item in selected}
    selected_asset_counts = {}
    for item in selected:
        selected_asset_counts[item["asset_id"]] = selected_asset_counts.get(item["asset_id"], 0) + 1
    if segment["segment_id"] in selected_segments:
        return -1000

    score = 0.0
    if segment.get("source_trust_level") == "high":
        score += 25
    if segment.get("product_binding_type") == "exact_sku":
        score += 20
    if segment.get("product_match_status") in {"trusted_by_source", "anchor_pass"}:
        score += 20
    source_type = str(segment.get("source_type") or "")
    is_real_source = source_type in REAL_SOURCE_TYPES
    if is_real_source:
        score += 35
        if not any(str(item.get("source_type") or "") in REAL_SOURCE_TYPES for item in selected):
            score += 25
    elif source_type == "ai_generated" and any(str(item.get("source_type") or "") == "ai_generated" for item in selected):
        score -= 8
    if slot_index == 1 and {"hero", "result", "detail"}.intersection(segment.get("effective_roles_json") or []):
        score += 20
    if segment.get("source_type") in (slot.get("preferred_source_types") or []):
        score += 18
    slot_segment_type = str(slot.get("segment_type") or "").strip()
    segment_type = str(segment.get("segment_type") or segment.get("scene_tag") or "").strip()
    if slot_segment_type and segment_type == slot_segment_type:
        score += 18
    is_prompt_package_ai = source_type == "ai_generated" and bool(_ai_prompt_package_id(segment))
    if is_prompt_package_ai:
        score += 16
        if batch_state.get("reuse_mode") in {"fill_target", "final_fill"}:
            score += 18
    if slot.get("preferred_source_trust") and segment.get("source_trust_level") == slot.get("preferred_source_trust"):
        score += 12
    if slot.get("preferred_binding") and segment.get("product_binding_type") == slot.get("preferred_binding"):
        score += 10
    target_duration = int(slot.get("duration_ms") or 3000)
    actual_duration = _segment_duration_ms(segment, target_duration)
    if actual_duration < target_duration:
        score -= min(80, (target_duration - actual_duration) / 20)

    asset_id = segment["asset_id"]
    current_asset_count = selected_asset_counts.get(asset_id, 0)
    score -= current_asset_count * 40
    if current_asset_count >= 2:
        score -= 120
    if segment["segment_id"] in batch_state.get("segments", set()):
        score -= 180
    score -= int(batch_state.get("assets", {}).get(asset_id, 0)) * 85
    if slot_index == 1 and asset_id in batch_state.get("first_assets", set()):
        score -= 180
    real_usage = int(segment.get("used_in_outputs_count") or 0)
    rejected_usage = int(segment.get("used_in_rejected_outputs_count") or 0)
    planned_usage = max(0, int(segment.get("usage_count") or 0) - real_usage)
    score -= real_usage * 8
    score -= max(0, rejected_usage - 2) * 1.5
    score -= planned_usage * 1.5
    if slot_index <= 3:
        text_risk = _text_overlay_risk(ctx, segment)
        if text_risk == REPAIRABLE_BOTTOM_CAPTION:
            score -= 18
        elif text_risk == "foreign_language_caption":
            score -= 70
        elif text_risk == "large_obstructive_text":
            score -= 100
        elif text_risk == "platform_ui_or_watermark":
            score -= 140
        elif text_risk == "safe_product_label":
            score -= 5
    if constraints.get("prefer_source_trust") and segment.get("source_trust_level") == constraints.get("prefer_source_trust"):
        score += 18
    if constraints.get("require_product_visibility") == "high" and _latest_tag_value(ctx, segment, "product_visibility") == "high":
        score += 20
    risk_level = _latest_tag_value(ctx, segment, "risk_level") or segment.get("risk_level")
    if risk_level == "medium":
        score -= 15
    elif risk_level == "high":
        score -= 60
    if _asset_has_watermark(ctx, segment):
        score -= 45 if slot_index <= 3 else 20
    if _has_subtitle_risk(ctx, segment):
        score -= 30 if slot_index <= 3 else 12
    if slot_index <= 3 and segment.get("source_trust_level") == "low":
        score -= 35

    # —— 钩子强度加权（改动1）。全部数值走 config，不硬编码。——
    # 现状根因：hook_strength 已在打标产出，但 _segment_score 不消费它，
    # 强钩子片会被信任分稍低或被用过的普通片挤掉。这里让强钩子在选美阶段主动胜出。
    # 种草模板 constraints 为空 → hook_weight_scale 默认 1.0，两线同受益（方案 A）。
    # 投流模板经 _build_template_constraints 注入 hook_weight_scale=2.0 进一步放大。
    # 回归开关：config hook_score_weights strong/medium 置 0 即退化为改动前行为。
    hook_value = _latest_tag_value(ctx, segment, "hook_strength")
    hook_key = "none" if hook_value is None else str(hook_value)
    hook_weights = _hook_score_weights(ctx)
    hook_base = float(hook_weights.get(hook_key, 0.0) or 0.0)
    if slot_index == 1:
        hook_base *= float(hook_weights.get("first_slot_multiplier", 1.0) or 1.0)
    hook_base *= float(constraints.get("hook_weight_scale", 1.0) or 1.0)
    score += hook_base

    # Deterministic spread so equal candidates do not always pick the same early id.
    score += _stable_spread(segment["segment_id"], variant_no, slot_index)
    return score


def _selection_reason(ctx: SkillContext, segment: dict, batch_state: dict, slot: dict, slot_index: int, score: float) -> dict[str, Any]:
    segment_id = str(segment.get("segment_id") or "")
    asset_id = str(segment.get("asset_id") or "")
    prompt_package_id = _ai_prompt_package_id(segment)
    slot_segment_type = str(slot.get("segment_type") or "")
    segment_type = str(segment.get("segment_type") or segment.get("scene_tag") or "")
    overlay = classify_text_overlay(_overlay_tag(segment))
    return {
        "score": round(float(score), 3),
        "slot_role": slot.get("role"),
        "slot_segment_type": slot_segment_type,
        "matched_segment_type": bool(slot_segment_type and slot_segment_type == segment_type),
        "source_type": segment.get("source_type"),
        "source_trust_level": segment.get("source_trust_level"),
        "product_match_status": segment.get("product_match_status"),
        "product_binding_type": segment.get("product_binding_type"),
        "prompt_package_id": prompt_package_id,
        "is_prompt_package_ai": bool(prompt_package_id and segment.get("source_type") == "ai_generated"),
        "segment_type": segment_type,
        "effective_roles": segment.get("effective_roles_json") or [],
        "reuse_mode": batch_state.get("reuse_mode", "strict"),
        "batch_segment_count": int((batch_state.get("segment_counts") or {}).get(segment_id, 0)),
        "batch_asset_count": int((batch_state.get("assets") or {}).get(asset_id, 0)),
        "first_segment_count": int((batch_state.get("first_segment_counts") or {}).get(segment_id, 0)),
        "text_overlay_risk": overlay.get("risk"),
        "subtitle_cleanup": _subtitle_cleanup_plan(segment),
        "why": _selection_why(segment, slot_segment_type, segment_type, prompt_package_id, overlay),
    }


def _selection_why(segment: dict, slot_segment_type: str, segment_type: str, prompt_package_id: str, overlay: dict[str, str]) -> list[str]:
    reasons = []
    if segment.get("source_type") in REAL_SOURCE_TYPES:
        reasons.append("trusted_real_source")
    if segment.get("source_type") == "ai_generated":
        reasons.append("ai_generated_asset")
    if prompt_package_id:
        reasons.append("ai_prompt_package_identity")
    if slot_segment_type and slot_segment_type == segment_type:
        reasons.append("segment_type_match")
    if segment.get("product_match_status") in {"trusted_by_source", "anchor_pass"}:
        reasons.append("product_match_trusted")
    if overlay.get("risk") in {"none", "safe_product_label"}:
        reasons.append("no_text_overlay_risk")
    elif overlay.get("risk") == REPAIRABLE_BOTTOM_CAPTION:
        reasons.append("repairable_bottom_caption")
    return reasons


def _filter_constraints(ctx: SkillContext, pool: list[dict], constraints: dict, slot_index: int) -> list[dict]:
    candidates = list(pool)
    excluded_segments = set(constraints.get("exclude_segments") or [])
    if excluded_segments:
        filtered = [s for s in candidates if s["segment_id"] not in excluded_segments]
        if filtered:
            candidates = filtered
    excluded_assets = set(constraints.get("exclude_assets") or [])
    if excluded_assets:
        filtered = [s for s in candidates if s.get("asset_id") not in excluded_assets]
        if filtered:
            candidates = filtered
    if constraints.get("require_no_watermark"):
        filtered = [s for s in candidates if "has watermark" not in str(s.get("effective_roles_reason") or "")]
        if filtered:
            candidates = filtered
    matches = set(constraints.get("require_product_match") or [])
    if matches:
        filtered = [s for s in candidates if s.get("product_match_status") in matches]
        if filtered:
            candidates = filtered
    if constraints.get("require_product_visibility") == "high":
        filtered = [s for s in candidates if _latest_tag_value(ctx, s, "product_visibility") == "high"]
        if filtered:
            candidates = filtered
    if constraints.get("max_risk_level") == "low":
        filtered = [s for s in candidates if _latest_tag_value(ctx, s, "risk_level") == "low"]
        if filtered:
            candidates = filtered
    if slot_index == 1 and constraints.get("prefer_roles"):
        preferred = set(constraints.get("prefer_roles") or [])
        filtered = [s for s in candidates if preferred.intersection(s.get("effective_roles_json") or [])]
        if filtered:
            candidates = filtered
    # 改动3A：投流模板首镜必须有视觉钩子（hook_visual_type ≠ none/null）。
    # 全被过滤则返回空池 → _select_segments 返回 SKIPPED_LOW_QUALITY → 触发补钩子链路。
    # 禁静默 fallback：不偷偷降级选无钩子片。
    if slot_index == 1 and constraints.get("require_hook_visual_first_slot"):
        candidates = [s for s in candidates
                      if (_latest_tag_value(ctx, s, "hook_visual_type") or "none") != "none"]
    return candidates


def _enrich_segments_for_selection(ctx: SkillContext, segments: list[dict]) -> list[dict]:
    enriched = []
    tag_keys = {
        "risk_level",
        "product_visibility",
        "hook_strength",
        "hook_visual_type",
        "confidence",
        "mixcut_usability",
        "needs_human_review",
        "text_overlay_risk",
        "text_language",
        "text_overlay_reason",
        "primary_shot_role",
        "secondary_roles_json",
        "reason",
    }
    segment_ids = [str(segment.get("segment_id") or "") for segment in segments if segment.get("segment_id")]
    latest_tags = _latest_tags_for_segments(ctx, segment_ids)
    asset_ids = sorted({str(segment.get("asset_id") or "") for segment in segments if segment.get("asset_id")})
    assets = _assets_by_id(ctx, asset_ids)
    for segment in segments:
        item = dict(segment)
        tag = latest_tags.get(str(segment.get("segment_id") or "")) or {}
        item["_latest_tag_loaded"] = True
        for key in tag_keys:
            if key in tag:
                item[key] = tag.get(key)
        if tag.get("reason"):
            item["tag_reason"] = tag.get("reason")
        asset_id = str(segment.get("asset_id") or "")
        if asset_id:
            asset = assets.get(asset_id) or {}
            for key in ["source_identity", "scene_tag", "generation_type", "prompt_package_id", "slot_role", "ai_gen_grade", "hook_intent"]:
                if asset.get(key) and not item.get(key):
                    item[key] = asset.get(key)
            if asset.get("scene_tag") and not item.get("segment_type"):
                item["segment_type"] = asset.get("scene_tag")
            if asset.get("has_watermark") is not None:
                item["has_watermark"] = asset.get("has_watermark")
            if asset.get("watermark_reason"):
                item["watermark_reason"] = asset.get("watermark_reason")
        enriched.append(item)
    return enriched


def _latest_tags_for_segments(ctx: SkillContext, segment_ids: list[str]) -> dict[str, dict]:
    if not segment_ids:
        return {}
    placeholders = ",".join(["?"] * len(segment_ids))
    rows = ctx.repo.list_where(
        "segment_tags",
        f"segment_id IN ({placeholders}) ORDER BY segment_id, id DESC",
        tuple(segment_ids),
    )
    latest: dict[str, dict] = {}
    for row in rows:
        segment_id = str(row.get("segment_id") or "")
        if segment_id and segment_id not in latest:
            latest[segment_id] = row
    return latest


def _assets_by_id(ctx: SkillContext, asset_ids: list[str]) -> dict[str, dict]:
    if not asset_ids:
        return {}
    placeholders = ",".join(["?"] * len(asset_ids))
    rows = ctx.repo.list_where("assets", f"asset_id IN ({placeholders})", tuple(asset_ids))
    return {str(row.get("asset_id") or ""): row for row in rows if row.get("asset_id")}


def _choose_template(ctx: SkillContext, product: dict, templates: list[TemplateSpec], batch_state: dict, variant: int, excluded_templates: set[str] | None = None) -> dict[str, Any]:
    category = str(product.get("category") or "generic_fashion")
    category_keys = _category_keys(category)
    aliases = _category_aliases(category)
    scored = []
    template_counts = batch_state.setdefault("template_counts", {})
    excluded_templates = excluded_templates or set()
    category_filtered = 0
    for index, template in enumerate(templates):
        if template.template_id in excluded_templates:
            continue
        if int(template_counts.get(template.template_id, 0)) >= _template_reuse_cap(batch_state):
            continue
        score = _template_category_score(category, aliases, template)
        if score <= 0:
            category_filtered += 1
            continue
        if template.template_id.startswith("AI_"):
            score += 12
        if variant > 1 and int(template_counts.get(template.template_id, 0)) > 0:
            score -= 35
        if template.duration_ms in batch_state.get("template_durations", []):
            score -= 10
        score += _template_stable_spread(template.template_id, variant, index)
        scored.append((score, -index, template))
    scored.sort(reverse=True)
    if not scored:
        return {
            "template": None,
            "debug": {
                "strategy": "category_template_score_rotation",
                "category": category,
                "category_keys": sorted(category_keys),
                "score": 0,
                "skip_reason": "no_category_matching_template" if category_filtered else "template_reuse_cap_exhausted",
                "reuse_mode": batch_state.get("reuse_mode", "strict"),
            },
        }
    template = scored[0][2]
    return {
        "template": template,
        "debug": {
            "strategy": "category_template_score_rotation",
            "category": category,
            "category_keys": sorted(category_keys),
            "score": scored[0][0] if scored else 0,
            "reuse_mode": batch_state.get("reuse_mode", "strict"),
        },
    }


def _advance_reuse_mode(batch_state: dict, variant: int) -> bool:
    mode = batch_state.get("reuse_mode", "strict")
    if mode == "strict":
        batch_state["reuse_mode"] = "fill_target"
        batch_state["fill_mode_activated_at_variant"] = variant
        return True
    if mode == "fill_target":
        batch_state["reuse_mode"] = "final_fill"
        batch_state["final_fill_mode_activated_at_variant"] = variant
        return True
    return False


def _template_reuse_cap(batch_state: dict) -> int:
    if batch_state.get("reuse_mode") == "final_fill":
        return FINAL_FILL_TEMPLATE_REUSE_PER_BATCH
    if batch_state.get("reuse_mode") == "fill_target":
        return FILL_MODE_TEMPLATE_REUSE_PER_BATCH
    return MAX_TEMPLATE_REUSE_PER_BATCH


def _record_template_choice(template: TemplateSpec, batch_state: dict) -> None:
    template_counts = batch_state.setdefault("template_counts", {})
    template_counts[template.template_id] = int(template_counts.get(template.template_id, 0)) + 1
    batch_state.setdefault("template_durations", []).append(template.duration_ms)


def _template_plan_json(template: TemplateSpec) -> dict[str, Any]:
    return {
        "template_id": template.template_id,
        "default_moods": template.default_moods,
        "suitable_categories": template.suitable_categories,
        "template_objective": template.template_objective,
        "pacing": template.pacing,
        "required_roles": template.required_roles,
        "risk_policy": template.risk_policy,
        "source_policy": template.source_policy,
        "bgm_profile": template.bgm_profile,
    }


def _category_keys(category: str) -> set[str]:
    aliases = {
        "womens_outerwear": {"womens_outerwear", "womens_top", "generic_fashion"},
        "womens_top": {"womens_outerwear", "womens_top", "generic_fashion"},
        "scarves_hats": {"scarves_hats", "scarf_hat", "scarves", "generic_fashion"},
        "scarf_hat": {"scarves_hats", "scarf_hat", "scarves", "generic_fashion"},
        "scarves": {"scarves_hats", "scarf_hat", "scarves", "generic_fashion"},
        "hair_accessories": {"hair_accessories", "generic_fashion"},
        "earrings": {"earrings", "generic_fashion"},
        "generic_fashion": {"generic_fashion"},
    }
    return aliases.get(category, {category, "generic_fashion"})


def _category_aliases(category: str) -> set[str]:
    aliases = {
        "womens_outerwear": {"womens_top"},
        "womens_top": {"womens_outerwear"},
        "scarves_hats": {"scarf_hat", "scarves"},
        "scarf_hat": {"scarves_hats", "scarves"},
        "scarves": {"scarves_hats", "scarf_hat"},
    }
    return aliases.get(category, set())


def _template_category_score(category: str, aliases: set[str], template: TemplateSpec) -> float:
    categories = set(template.suitable_categories or [])
    if category in categories and "generic_fashion" not in categories:
        return 120.0
    if category in categories:
        return 95.0
    if aliases.intersection(categories):
        return 70.0
    if "generic_fashion" in categories:
        return 40.0
    return 0.0


def _template_stable_spread(template_id: str, variant: int, index: int) -> float:
    seed = f"{template_id}:{variant}:{index}"
    return (sum(ord(ch) for ch in seed) % 19) / 100


def _passes_first_slot_floor(ctx: SkillContext, segment: dict, risk_policy: dict[str, Any]) -> tuple[bool, str]:
    if risk_policy.get("require_no_watermark_for_first_slot") and _asset_has_watermark(ctx, segment):
        return False, "first slot asset has watermark"
    if risk_policy.get("avoid_subtitle_risk_in_first_slot") and _has_subtitle_risk(ctx, segment):
        return False, "first slot has subtitle risk"
    risk = _latest_tag_value(ctx, segment, "risk_level") or segment.get("risk_level")
    ai_anchor_trusted = segment.get("source_type") == "ai_generated" and segment.get("anchor_match_level") == "strict_pass"
    trusted_real_first = _trusted_real_first_segment(ctx, segment)
    if risk != "low" and not ai_anchor_trusted and not trusted_real_first:
        return False, "first slot risk is not low or trusted"
    visibility = _latest_tag_value(ctx, segment, "product_visibility")
    if visibility == "low":
        return False, "first slot product visibility low"
    if (
        segment.get("product_match_status") not in {"trusted_by_source", "anchor_pass"}
        and not ai_anchor_trusted
        and not _low_trust_first_slot_review_candidate(ctx, segment)
    ):
        return False, "first slot product match is not trusted"
    return True, ""


def _trusted_real_first_segment(ctx: SkillContext, segment: dict) -> bool:
    source_type = str(segment.get("source_type") or "")
    trust = str(segment.get("source_trust_level") or "")
    binding = str(segment.get("product_binding_type") or "")
    match = str(segment.get("product_match_status") or "")
    if source_type not in REAL_SOURCE_TYPES:
        return False
    if trust not in {"high", "medium"} or binding != "exact_sku" or match not in {"trusted_by_source", "anchor_pass"}:
        return False
    if str(_latest_tag_value(ctx, segment, "product_visibility") or "") != "high":
        return False
    if str(_latest_tag_value(ctx, segment, "confidence") or "") not in {"high", "medium"}:
        return False
    if str(_latest_tag_value(ctx, segment, "risk_level") or "") != "medium":
        return False
    reason = str(_latest_tag_value(ctx, segment, "reason") or "")
    soft_tokens = ["锚点未知", "锚点不确定", "锚点缺失", "商品锚点", "商品信息缺失", "需核对", "需复核", "需确认", "人工确认", "人工核实"]
    hard_tokens = ["水印", "平台", "账号", "logo", "Logo", "错款", "错品类", "竞品", "SKU一致性", "漂移", "无关元素", "品牌包", "遮挡严重"]
    return any(token in reason for token in soft_tokens) and not any(token in reason for token in hard_tokens)


def _low_trust_first_slot_review_candidate(ctx: SkillContext, segment: dict) -> bool:
    source_type = str(segment.get("source_type") or "")
    trust = str(segment.get("source_trust_level") or "")
    binding = str(segment.get("product_binding_type") or "")
    match = str(segment.get("product_match_status") or "")
    if source_type not in {"douyin_repost", "competitor"}:
        return False
    if trust != "low" or binding not in {"exact_sku", "same_style"}:
        return False
    if match not in {"", "uncertain", "trusted_by_source", "anchor_pass"}:
        return False
    if "hero" not in (segment.get("effective_roles_json") or []):
        return False
    return (
        str(_latest_tag_value(ctx, segment, "primary_shot_role") or "") == "hero"
        and str(_latest_tag_value(ctx, segment, "product_visibility") or "") == "high"
        and str(_latest_tag_value(ctx, segment, "confidence") or "") == "high"
        and str(_latest_tag_value(ctx, segment, "risk_level") or "") == "low"
        and str(_latest_tag_value(ctx, segment, "mixcut_usability") or "") == "yes"
        and not _has_subtitle_risk(ctx, segment)
        and not _asset_has_watermark(ctx, segment)
    )


def _prefer_first_slot_pool(ctx: SkillContext, pool: list[dict]) -> list[dict]:
    safe = [
        s
        for s in pool
        if _latest_tag_value(ctx, s, "product_visibility") == "high"
        and _latest_tag_value(ctx, s, "hook_strength") in {"strong", "medium"}
        and _latest_tag_value(ctx, s, "mixcut_usability") == "yes"
        and _latest_tag_value(ctx, s, "risk_level") == "low"
        and not bool(_latest_tag_value(ctx, s, "needs_human_review"))
    ]
    return safe or pool


def _first_slot_failure(ctx: SkillContext, segment: dict, risk_policy: dict[str, Any]) -> dict[str, Any]:
    passed, reason = _passes_first_slot_floor(ctx, segment, risk_policy)
    return {"segment_id": segment.get("segment_id"), "asset_id": segment.get("asset_id"), "passed": passed, "reason": reason}


def _asset_has_watermark(ctx: SkillContext, segment: dict) -> bool:
    asset = None
    value = segment.get("has_watermark")
    if value is None and segment.get("asset_id"):
        asset = ctx.repo.get("assets", "asset_id", segment.get("asset_id"))
        value = (asset or {}).get("has_watermark")
    normalized = str(value).strip().lower()
    if normalized in {"yes", "true", "1"}:
        return True
    if normalized in {"no", "false", "0", "processed", "skipped", "pending"}:
        return False
    reason = f"{segment.get('effective_roles_reason') or ''} {segment.get('watermark_reason') or ''} {(asset or {}).get('watermark_reason') or ''}".lower()
    return "watermark" in reason or "水印" in reason


def _has_subtitle_risk(ctx: SkillContext, segment: dict) -> bool:
    return has_text_overlay_risk(_overlay_tag(segment))


def _has_unusable_subtitle_risk(segment: dict) -> bool:
    return is_unusable_hard_subtitle(_overlay_tag(segment))


def _needs_subtitle_crop(segment: dict) -> bool:
    return is_repairable_bottom_caption(_overlay_tag(segment))


def _subtitle_cleanup_plan(segment: dict) -> dict[str, Any]:
    if _needs_subtitle_crop(segment):
        return {"action": "bottom_crop", "zoom": 1.12, "reason": "repairable bottom hard subtitle"}
    return {"action": "none"}


def _sync_material_supplement_queue(ctx: SkillContext, row: dict, detail: dict) -> None:
    # 改动3B：把缺口画像写进 payload_json，供 HookSupplementSkill 驱动即梦补钩子提示词。
    # detail 是选片失败时的 error.detail（含 role/first_slot_failures 等），这里提取缺口信息。
    # 现有消费方不读 payload_json，零侵入；新消费方（HookSupplementSkill）读它。
    product_id = row.get("product_id") or ""
    task = _task(ctx, product_id) if product_id else {}
    payload = {
        "product_id": product_id,
        "category": task.get("category") or "",
        "missing_role": detail.get("role") if isinstance(detail, dict) else "",
        "expected_hook_visual_type": "any",
        "expected_segment_type": "home_lifestyle",
        "shortfall": 1,
        "source": "render_plan_skipped_low_quality",
        "render_plan_id": row.get("render_plan_id") or "",
    }
    ctx.repo.upsert(
        "feishu_sync_records",
        "sync_id",
        {
            "sync_id": new_id("FS"),
            "object_type": "material_supplement",
            "object_id": row.get("render_plan_id"),
            "feishu_table": "素材补充队列",
            "feishu_record_id": new_id("FSREC"),
            "sync_status": "pending",
            "cleanup_status": "pending",
            "payload_json": json.dumps(payload, ensure_ascii=False),
        },
    )


def precheck_hook_coverage(ctx: SkillContext, product_id: str, required_count: int = 1) -> dict[str, Any]:
    """预检：该 product 库里有没有足够的钩子型首镜素材（改动3B）。

    不进主流水线，由补钩子入口（如 CLI create-ad-task 或手动）显式调用。
    返回 {"ok": bool, "gap": {...}}：
      - ok=True：库里有 >= required_count 个强钩子首镜候选，无需补。
      - ok=False：缺口，gap 含 missing_role/expected_hook_visual_type/shortfall，供补充队列消费。

    判定标准：同时满足
      1. effective_roles 含 hero（能当首镜）
      2. hook_strength ∈ {strong, medium}（钩子够强，与 _prefer_first_slot_pool 一致）
      3. hook_visual_type ≠ none（有视觉钩子，改动2 字段；老素材 NULL 视为无钩子）
      4. 无水印、risk=low 或受信任（沿用 _passes_first_slot_floor 口径）
    """
    segments = ctx.repo.list_where("segments", "product_id=?", (product_id,))
    if not segments:
        return {"ok": False, "gap": {"product_id": product_id, "missing_role": "hero", "expected_hook_visual_type": "any", "expected_segment_type": "home_lifestyle", "shortfall": required_count}}
    enriched = _render_eligible_segments(_enrich_segments_for_selection(ctx, segments))
    candidates = []
    for seg in enriched:
        roles = seg.get("effective_roles_json") or []
        if "hero" not in roles:
            continue
        if _asset_has_watermark(ctx, seg):
            continue
        hook_strength = _latest_tag_value(ctx, seg, "hook_strength")
        if hook_strength not in {"strong", "medium"}:
            continue
        hook_visual = _latest_tag_value(ctx, seg, "hook_visual_type")
        if not hook_visual or hook_visual == "none":
            continue
        risk = _latest_tag_value(ctx, seg, "risk_level") or seg.get("risk_level")
        ai_anchor_trusted = seg.get("source_type") == "ai_generated" and seg.get("anchor_match_level") == "strict_pass"
        if risk not in {"low", None} and not ai_anchor_trusted and not _trusted_real_first_segment(ctx, seg):
            continue
        candidates.append(seg)
    shortfall = max(0, required_count - len(candidates))
    if shortfall > 0:
        return {"ok": False, "gap": {"product_id": product_id, "missing_role": "hero", "expected_hook_visual_type": "any", "expected_segment_type": "home_lifestyle", "shortfall": shortfall, "current_candidates": len(candidates)}}
    return {"ok": True, "gap": {}, "current_candidates": len(candidates)}


def _latest_tag_value(ctx: SkillContext, segment: dict, key: str):
    if key in segment:
        return segment.get(key)
    if segment.get("_latest_tag_loaded"):
        return None
    rows = ctx.repo.list_where("segment_tags", "segment_id=? ORDER BY id DESC LIMIT 1", (segment["segment_id"],))
    return rows[0].get(key) if rows else None


def _load_render_scoring_config(ctx: SkillContext) -> dict[str, Any]:
    path = ctx.settings.root_dir / "config" / "render_scoring.yaml"
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _hook_score_weights(ctx: SkillContext) -> dict[str, float]:
    """钩子强度加分权重（改动1）。全部走 config，不硬编码。

    回归开关：config 里 strong/medium 置 0 即退化为改动前行为。
    """
    cfg = _load_render_scoring_config(ctx)
    return dict(cfg.get("hook_score_weights") or {})


def _build_template_constraints(template: TemplateSpec) -> dict[str, Any]:
    """从模板 selection_policy 组装选片 constraints（改动3 预埋，改动1 消费 hook_weight_scale）。

    零侵入：种草模板 selection_policy 为空 dict → 返回空 dict →
    _select_segments 收到空 constraints，与改动前（传 None→{}）完全等价。
    投流模板经此把 hook_weight_scale / require_hook_visual_first_slot 注入选片。
    """
    sp = getattr(template, "selection_policy", {}) or {}
    constraints: dict[str, Any] = {}
    if sp.get("hook_weight_scale") is not None:
        constraints["hook_weight_scale"] = sp["hook_weight_scale"]
    if sp.get("require_hook_visual_first_slot"):
        constraints["require_hook_visual_first_slot"] = True
    if sp.get("min_unique_source_assets") is not None:
        constraints["min_unique_source_assets"] = sp["min_unique_source_assets"]
    return constraints


def _text_overlay_risk(ctx: SkillContext, segment: dict) -> str:
    return classify_text_overlay(_overlay_tag(segment))["risk"]


def _overlay_tag(segment: dict) -> dict:
    reason = " ".join(
        str(segment.get(key) or "")
        for key in ("text_overlay_reason", "tag_reason", "effective_roles_reason", "product_match_reason")
    )
    return {
        "text_overlay_risk": segment.get("text_overlay_risk"),
        "text_language": segment.get("text_language"),
        "text_overlay_reason": segment.get("text_overlay_reason"),
        "reason": reason,
    }


def _filter_non_consecutive_asset(pool: list[dict], selected: list[dict]) -> list[dict]:
    if not selected:
        return pool
    previous_asset = selected[-1].get("asset_id")
    return [s for s in pool if s.get("asset_id") != previous_asset]


def _filter_unused_asset(pool: list[dict], selected: list[dict]) -> list[dict]:
    if not selected:
        return pool
    used_assets = {item.get("asset_id") for item in selected}
    return [s for s in pool if s.get("asset_id") not in used_assets]


def _filter_core_asset_reuse(pool: list[dict], selected: list[dict], role: str) -> list[dict]:
    if role not in CORE_ROLES:
        return pool
    used_core_assets = {
        item.get("asset_id")
        for item in selected
        if item.get("role") in CORE_ROLES
    }
    return [s for s in pool if s.get("asset_id") not in used_core_assets]


def _filter_sufficient_duration(pool: list[dict], slot: dict) -> list[dict]:
    target = int(slot.get("duration_ms") or 3000)
    return [
        s
        for s in pool
        if _segment_duration_ms(s, target) >= target
        or (str(s.get("source_type") or "") in REAL_SOURCE_TYPES and _segment_duration_ms(s, target) >= max(2500, int(target * 0.7)))
    ]


def _filter_slot_pool(pool: list[dict], slot: dict) -> list[dict]:
    candidates = list(pool)
    segment_type = str(slot.get("segment_type") or "").strip()
    if segment_type:
        matched = [
            s
            for s in candidates
            if str(s.get("segment_type") or s.get("scene_tag") or "").strip() == segment_type
        ]
        if matched:
            candidates = matched
    min_trust = slot.get("min_source_trust")
    if min_trust:
        threshold = TRUST_RANK.get(str(min_trust), 0)
        trusted = [s for s in candidates if TRUST_RANK.get(str(s.get("source_trust_level") or ""), 0) >= threshold]
        if trusted:
            candidates = trusted
    disallowed = set(slot.get("disallowed_source_types") or [])
    if disallowed:
        allowed = [s for s in candidates if s.get("source_type") not in disallowed]
        if allowed:
            candidates = allowed
    return candidates


def _filter_batch_reuse_caps(pool: list[dict], state: dict, role: str, slot_index: int) -> list[dict]:
    segment_counts = state.get("segment_counts") or {}
    core_counts = state.get("core_segment_counts") or {}
    first_counts = state.get("first_segment_counts") or {}
    first_asset_counts = state.get("first_asset_counts") or {}
    filtered = []
    reuse_cap = _segment_reuse_cap_for_slot(state, role, slot_index)
    core_cap = _core_segment_reuse_cap(state)
    first_segment_cap = _first_segment_reuse_cap(state)
    first_asset_cap = _first_asset_reuse_cap(state)
    for segment in pool:
        segment_id = str(segment.get("segment_id") or "")
        asset_id = str(segment.get("asset_id") or "")
        if int(segment_counts.get(segment_id, 0)) >= reuse_cap:
            continue
        if int(core_counts.get(segment_id, 0)) >= core_cap:
            continue
        if (slot_index == 1 or role == "hero") and int(first_counts.get(segment_id, 0)) >= first_segment_cap:
            continue
        if slot_index == 1 and int(first_asset_counts.get(asset_id, 0)) >= first_asset_cap:
            continue
        filtered.append(segment)
    return filtered


def _segment_reuse_cap_for_slot(state: dict, role: str, slot_index: int) -> int:
    if state.get("reuse_mode") in {"fill_target", "final_fill"}:
        if slot_index == 1 or role in CORE_ROLES:
            return FILL_MODE_SEGMENT_REUSE_PER_BATCH
        return FILL_MODE_SUPPORT_SEGMENT_REUSE_PER_BATCH
    if slot_index == 1 or role in CORE_ROLES:
        return MAX_SEGMENT_REUSE_PER_BATCH
    return MAX_SUPPORT_SEGMENT_REUSE_PER_BATCH


def _core_segment_reuse_cap(state: dict) -> int:
    if state.get("reuse_mode") in {"fill_target", "final_fill"}:
        return FILL_MODE_SEGMENT_REUSE_PER_BATCH
    return MAX_SEGMENT_REUSE_PER_BATCH


def _first_segment_reuse_cap(state: dict) -> int:
    if state.get("reuse_mode") == "final_fill":
        return FINAL_FILL_FIRST_SEGMENT_REUSE_PER_BATCH
    if state.get("reuse_mode") == "fill_target":
        return FILL_MODE_FIRST_SEGMENT_REUSE_PER_BATCH
    return MAX_FIRST_SEGMENT_REUSE_PER_BATCH


def _first_asset_reuse_cap(state: dict) -> int:
    if state.get("reuse_mode") == "final_fill":
        return FINAL_FILL_FIRST_ASSET_REUSE_PER_BATCH
    if state.get("reuse_mode") == "fill_target":
        return FILL_MODE_FIRST_ASSET_REUSE_PER_BATCH
    return MAX_FIRST_ASSET_REUSE_PER_BATCH


def _load_templates(ctx: SkillContext) -> list[TemplateSpec]:
    path = ctx.settings.root_dir / "config" / "templates.yaml"
    specs = DEFAULT_TEMPLATE_SPECS
    if path.exists():
        with open(path, "r", encoding="utf-8") as fh:
            loaded = yaml.safe_load(fh) or {}
        specs = loaded.get("templates") or DEFAULT_TEMPLATE_SPECS
    templates = []
    for spec in specs:
        if spec.get("fallback_only"):
            continue
        slots = spec.get("slots")
        if not slots:
            slots = [{"role": role, "duration_ms": 3000} for role in spec.get("roles") or []]
        slots = _normalize_slots(slots, int(spec.get("duration_ms") or 15000))
        if not slots:
            continue
        templates.append(_template_from_spec(spec, slots))
    return templates or _load_default_templates()


def _load_template_by_id(ctx: SkillContext, template_id: str) -> TemplateSpec | None:
    """按 ID 加载单个模板（包括 fallback_only 的投流模板）。"""
    path = ctx.settings.root_dir / "config" / "templates.yaml"
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as fh:
        loaded = yaml.safe_load(fh) or {}
    for spec in loaded.get("templates") or []:
        if spec.get("template_id") != template_id:
            continue
        slots = spec.get("slots")
        if not slots:
            slots = [{"role": role, "duration_ms": 3000} for role in spec.get("roles") or []]
        slots = _normalize_slots(slots, int(spec.get("duration_ms") or 15000))
        if not slots:
            return None
        return _template_from_spec(spec, slots)
    return None


def _load_default_templates() -> list[TemplateSpec]:
    return [
        _template_from_spec(spec, list(spec["slots"]))
        for spec in DEFAULT_TEMPLATE_SPECS
    ]


def _template_from_spec(spec: dict[str, Any], slots: list[dict[str, Any]]) -> TemplateSpec:
    return TemplateSpec(
        template_id=spec["template_id"],
        duration_ms=sum(int(s["duration_ms"]) for s in slots),
        slots=slots,
        default_moods=list(spec.get("default_moods") or []),
        suitable_categories=list(spec.get("suitable_categories") or []),
        template_objective=str(spec.get("template_objective") or "balanced"),
        pacing=str(spec.get("pacing") or "balanced"),
        required_roles=list(spec.get("required_roles") or []),
        risk_policy=dict(spec.get("risk_policy") or {}),
        source_policy=dict(spec.get("source_policy") or {}),
        bgm_profile=dict(spec.get("bgm_profile") or {}),
        selection_policy=dict(spec.get("selection_policy") or {}),
    )


def _choose_remix_template(ctx: SkillContext, source_output: dict, remix_plan: dict) -> TemplateSpec:
    templates = _load_templates(ctx)
    constraints = remix_plan.get("constraints") or {}
    preferred = constraints.get("prefer_templates") or []
    for template_id in [*preferred, source_output.get("template_id")]:
        if not template_id:
            continue
        match = next((t for t in templates if t.template_id == template_id), None)
        if match:
            return match
    return templates[0]


def _remix_slots(template: TemplateSpec, remix_plan: dict) -> list[dict[str, Any]]:
    slots = [dict(slot) for slot in template.slots]
    constraints = remix_plan.get("constraints") or {}
    preferred_roles = list(constraints.get("prefer_roles") or [])
    if slots and preferred_roles:
        original_first_role = slots[0].get("role")
        preferred_first = preferred_roles[0]
        slots[0]["role"] = preferred_first
        existing_roles = [slot.get("role") for slot in slots[1:]]
        if original_first_role and original_first_role not in existing_roles and original_first_role != slots[0].get("role") and len(slots) > 1:
            replacement_index = next((idx for idx, slot in enumerate(slots[1:], start=1) if slot.get("role") == preferred_first), 1)
            slots[replacement_index]["role"] = original_first_role
    if constraints.get("prefer_source_trust") == "high":
        for slot in slots[:3]:
            slot.setdefault("preferred_source_trust", "high")
    return slots


def _normalize_slots(slots: list[dict], duration_ms: int) -> list[dict]:
    normalized = []
    for slot in slots:
        if not slot.get("role"):
            continue
        item = dict(slot)
        item["duration_ms"] = int(item.get("duration_ms") or 3000)
        normalized.append(item)
    total = sum(int(item["duration_ms"]) for item in normalized)
    if normalized and total != duration_ms:
        normalized[-1]["duration_ms"] = max(500, int(normalized[-1]["duration_ms"]) + duration_ms - total)
    return normalized


def _stable_spread(segment_id: str, variant_no: int, slot_index: int) -> float:
    seed = f"{segment_id}:{variant_no}:{slot_index}"
    return (sum(ord(ch) for ch in seed) % 17) / 100


def _record_selection(ctx: SkillContext, selected: list[dict], batch_state: dict) -> None:
    _record_selection_state_only(selected, batch_state)
    for index, item in enumerate(selected, start=1):
        segment = ctx.repo.get("segments", "segment_id", item["segment_id"]) or {}
        ctx.repo.update("segments", "segment_id", item["segment_id"], {"usage_count": int(segment.get("usage_count") or 0) + 1})


def _record_selection_state_only(selected: list[dict], batch_state: dict) -> None:
    for index, item in enumerate(selected, start=1):
        batch_state.setdefault("segments", set()).add(item["segment_id"])
        segment_counts = batch_state.setdefault("segment_counts", {})
        segment_counts[item["segment_id"]] = int(segment_counts.get(item["segment_id"], 0)) + 1
        if item.get("role") in CORE_ROLES:
            core_counts = batch_state.setdefault("core_segment_counts", {})
            core_counts[item["segment_id"]] = int(core_counts.get(item["segment_id"], 0)) + 1
        assets = batch_state.setdefault("assets", {})
        assets[item["asset_id"]] = int(assets.get(item["asset_id"], 0)) + 1
        if index == 1:
            batch_state.setdefault("first_assets", set()).add(item["asset_id"])
            first_counts = batch_state.setdefault("first_segment_counts", {})
            first_counts[item["segment_id"]] = int(first_counts.get(item["segment_id"], 0)) + 1
            first_asset_counts = batch_state.setdefault("first_asset_counts", {})
            first_asset_counts[item["asset_id"]] = int(first_asset_counts.get(item["asset_id"], 0)) + 1


def _task(ctx: SkillContext, product_id: str):
    tasks = ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC", (product_id,))
    return tasks[0] if tasks else None
