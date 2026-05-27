from __future__ import annotations

from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result

from .context import SkillContext


TEMPLATES = [
    ("GENERAL_BALANCED_15S", ["hero", "detail", "result", "scene", "ending"]),
    ("RESULT_FIRST_15S", ["result", "detail", "result", "scene", "ending"]),
    ("DETAIL_HOOK_15S", ["detail", "hero", "result", "scene", "ending"]),
]


class RenderPlanSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def create_plans(self, product_id: str, count: int | None = None) -> Result:
        task = _task(self.ctx, product_id)
        allowed = int((task or {}).get("allowed_variant_count") or 0)
        total = min(count or allowed, allowed)
        if total <= 0:
            return Result.fail("MATERIAL_NOT_READY", "allowed_variant_count is zero", {"product_id": product_id})
        batch_id = new_id("BATCH")
        self.ctx.repo.upsert(
            "mixcut_batches",
            "batch_id",
            {"batch_id": batch_id, "product_id": product_id, "task_id": (task or {}).get("task_id"), "requested_count": count or allowed, "allowed_count": allowed, "rendered_count": 0, "batch_status": "planning", "material_tier": (task or {}).get("material_tier"), "template_pool_json": [t[0] for t in TEMPLATES]},
        )
        plans = []
        for variant in range(1, total + 1):
            template_id, roles, selected = _select_template(self.ctx, product_id, variant)
            if not selected.success:
                return selected
            plan_id = new_id("PLAN")
            cursor = 0
            slots = []
            for slot_index, item in enumerate(selected.data, start=1):
                dur = 3000 if slot_index <= 4 else 3000
                slots.append({"slot": slot_index, "role": item["role"], "segment_id": item["segment_id"], "asset_id": item["asset_id"], "start_ms_in_output": cursor, "end_ms_in_output": cursor + dur})
                cursor += dur
            row = {"render_plan_id": plan_id, "batch_id": batch_id, "product_id": product_id, "variant_no": variant, "template_id": template_id, "planned_duration_ms": 15000, "plan_json": {"segments": slots}, "quality_gate_status": "pending", "render_status": "planned"}
            self.ctx.repo.upsert("render_plans", "render_plan_id", row)
            for item in selected.data:
                segment = self.ctx.repo.get("segments", "segment_id", item["segment_id"]) or {}
                self.ctx.repo.update("segments", "segment_id", item["segment_id"], {"usage_count": int(segment.get("usage_count") or 0) + 1})
            plans.append(plan_id)
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"task_status": "RENDER_PLAN_CREATED"})
        return Result.ok({"batch_id": batch_id, "render_plan_ids": plans})


def _select_template(ctx: SkillContext, product_id: str, variant: int) -> tuple[str, list[str], Result]:
    last_error = None
    for step in range(len(TEMPLATES)):
        template_id, roles = TEMPLATES[(variant - 1 + step) % len(TEMPLATES)]
        selected = _select_segments(ctx, product_id, roles, offset=variant - 1)
        if selected.success:
            return template_id, roles, selected
        last_error = selected
    return TEMPLATES[(variant - 1) % len(TEMPLATES)][0], TEMPLATES[(variant - 1) % len(TEMPLATES)][1], last_error or Result.fail("RENDER_PLAN_FAILED", "no template available")


def _select_segments(ctx: SkillContext, product_id: str, roles, offset: int = 0) -> Result:
    segments = ctx.repo.list_where("segments", "product_id=?", (product_id,))
    bundles = [_bundle(ctx, s) for s in segments]
    selected = []
    used_assets = set()
    target_unique_assets = min(3, len({s["asset_id"] for s in segments}))
    for slot_index, role in enumerate(roles, start=1):
        pool = [b for b in bundles if role in (b["segment"].get("effective_roles_json") or []) and b["segment"]["segment_id"] not in {item["segment_id"] for item in selected}]
        soft_count = sum(1 for item in selected if item.get("soft_local_subtitle"))
        if soft_count >= 2:
            clean_pool = [b for b in pool if not _is_soft_local_subtitle(b)]
            if clean_pool:
                pool = clean_pool
        if role in {"hero", "detail", "result"}:
            clean_core_pool = [b for b in pool if not _is_soft_local_subtitle(b)]
            if clean_core_pool:
                pool = clean_core_pool
        if slot_index == 1:
            first_pool = [b for b in pool if _first_slot_ok(b)]
            if not first_pool:
                return Result.fail("RENDER_PLAN_FAILED", "no first-slot-safe segment available", {"role": role})
            pool = first_pool
        if not pool:
            return Result.fail("RENDER_PLAN_FAILED", f"no segment available for role {role}", {"role": role})
        if len(used_assets) < target_unique_assets:
            diverse_pool = [b for b in pool if b["segment"]["asset_id"] not in used_assets]
            if diverse_pool:
                pool = diverse_pool
        if selected:
            non_consecutive = [b for b in pool if b["segment"]["asset_id"] != selected[-1]["asset_id"]]
            if non_consecutive:
                pool = non_consecutive
        pool = sorted(pool, key=lambda b: _score_bundle(b, role, slot_index, used_assets), reverse=True)
        choice = pool[offset % len(pool)]
        segment = choice["segment"]
        selected.append({"role": role, "segment_id": segment["segment_id"], "asset_id": segment["asset_id"], "soft_local_subtitle": _is_soft_local_subtitle(choice)})
        used_assets.add(segment["asset_id"])
    if len(used_assets) < target_unique_assets:
        return Result.fail("RENDER_PLAN_FAILED", "unable to satisfy minimum unique source assets", {"unique_assets": len(used_assets), "target_unique_assets": target_unique_assets})
    return Result.ok(selected)


def _task(ctx: SkillContext, product_id: str):
    tasks = ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC", (product_id,))
    return tasks[0] if tasks else None


def _bundle(ctx: SkillContext, segment: dict) -> dict:
    tags = ctx.repo.list_where("segment_tags", "segment_id=? ORDER BY id DESC", (segment["segment_id"],))
    return {"segment": segment, "tag": tags[0] if tags else {}}


def _first_slot_ok(bundle: dict) -> bool:
    tag = bundle["tag"]
    segment = bundle["segment"]
    return (
        tag.get("product_visibility") == "high"
        and tag.get("risk_level") == "low"
        and tag.get("hook_strength") in {"strong", "medium"}
        and tag.get("confidence") != "low"
        and segment.get("product_match_status") in {"trusted_by_source", "anchor_pass"}
        and bool(set(segment.get("effective_roles_json") or []).intersection({"hero", "result", "detail"}))
    )


def _score_bundle(bundle: dict, role: str, slot_index: int, used_assets: set[str]) -> tuple:
    segment = bundle["segment"]
    tag = bundle["tag"]
    primary_match = 1 if tag.get("primary_shot_role") == role else 0
    high_visibility = 1 if tag.get("product_visibility") == "high" else 0
    strong_hook = 1 if tag.get("hook_strength") == "strong" else 0
    medium_hook = 1 if tag.get("hook_strength") == "medium" else 0
    fresh_asset = 1 if segment["asset_id"] not in used_assets else 0
    clean_bonus = 1 if not _is_soft_local_subtitle(bundle) else 0
    low_usage = -(segment.get("usage_count") or 0)
    first_slot_bonus = 3 if slot_index == 1 and _first_slot_ok(bundle) else 0
    return (
        first_slot_bonus,
        clean_bonus,
        fresh_asset,
        primary_match,
        high_visibility,
        strong_hook,
        medium_hook,
        low_usage,
        segment["segment_id"],
    )


def _is_soft_local_subtitle(bundle: dict) -> bool:
    return bundle["segment"].get("effective_roles_reason") == "soft local-language subtitle issue"
