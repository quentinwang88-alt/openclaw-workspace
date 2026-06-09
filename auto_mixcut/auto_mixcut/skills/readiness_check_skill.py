from __future__ import annotations

import math

from auto_mixcut.core.result import Result

from .context import SkillContext
from .feishu_review_skill import sync_product_task_best_effort
from .render_plan_skill import estimate_render_plan_capacity

MAX_SEGMENT_REUSE_PER_BATCH = 2
MAX_SUPPORT_SEGMENT_REUSE_PER_BATCH = 3
MAX_FIRST_SLOT_REUSE_PER_BATCH = 2
MAX_TEMPLATE_REUSE_PER_BATCH = 2
AVG_SEGMENTS_PER_OUTPUT = 5
MAX_HEAVY_RENDER_PLAN_ESTIMATE_SEGMENTS = 80


class ReadinessCheckSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def check_product(self, product_id: str, requested_count: int | None = None) -> Result:
        task = _task(self.ctx, product_id)
        requested = requested_count if requested_count is not None else int((task or {}).get("requested_variant_count") or 0)
        segments = self.ctx.repo.list_where("segments", "product_id=?", (product_id,))
        counts = {"hero": 0, "detail": 0, "result": 0, "scene": 0, "ending": 0}
        total_usable = 0
        for seg in segments:
            roles = seg.get("effective_roles_json") or []
            if roles:
                total_usable += 1
            for role in counts:
                if role in roles:
                    counts[role] += 1
        tier, max_variants, gaps = _tier(counts, total_usable)
        product = self.ctx.repo.get("products", "product_id", product_id) or {}
        diversity = _diversity_capacity(self.ctx, product, segments)
        role_allowed = min(requested or max_variants, max_variants)
        recommended = min(role_allowed, diversity["recommended_capacity"])
        no_ai_max = min(role_allowed, diversity["no_ai_max_capacity"])
        plan_estimate = _calibrate_render_plan_capacity(self.ctx, product_id, segments, no_ai_max)
        actual_plannable = int(plan_estimate.get("planned_count") or 0) if plan_estimate else no_ai_max
        strict_allowed = min(no_ai_max, actual_plannable)
        unique_first_slot_allowed = min(role_allowed, int(diversity.get("first_slot_candidates") or 0))
        allowed = min(role_allowed, unique_first_slot_allowed) if strict_allowed > 0 else 0
        if role_allowed > recommended:
            gaps.extend(_diversity_gap_messages(diversity, role_allowed, recommended, no_ai_max))
        if role_allowed > unique_first_slot_allowed:
            first_shortfall = role_allowed - unique_first_slot_allowed
            gaps.append(
                "first slot uniqueness limited: "
                f"requested={role_allowed}, first_slot_candidates={diversity.get('first_slot_candidates')}, "
                f"allowed_without_repeated_opening={unique_first_slot_allowed}"
            )
            gaps.append(_first_slot_ai_supplement_message(first_shortfall))
        if plan_estimate and role_allowed > strict_allowed:
            plan_shortfall = role_allowed - strict_allowed
            gaps.append(
                "strict render plan capacity limited: "
                f"requested={role_allowed}, formula_no_ai={no_ai_max}, strict_plannable={strict_allowed}, "
                f"skipped={plan_estimate.get('skipped_count')}; render-plan will use controlled fill mode"
            )
            gaps.append(_render_plan_ai_supplement_message(diversity, plan_shortfall))
        material_status = "ready" if allowed > 0 else "not_ready"
        self.ctx.repo.update(
            "content_tasks",
            "product_id",
            product_id,
            _readiness_task_patch(
                tier=tier,
                material_status=material_status,
                allowed=allowed,
                gaps=gaps,
                first_slot_remaining=max(0, int(diversity.get("first_slot_capacity") or 0) - _actual_good_outputs(self.ctx, product_id)),
                current_bottleneck=_current_bottleneck(diversity, gaps),
            ),
        )
        task_sync = sync_product_task_best_effort(self.ctx, product_id)
        return Result.ok({
            "product_id": product_id,
            "tier": tier,
            "allowed_variant_count": allowed,
            "strict_plannable_variant_count": strict_allowed,
            "unique_first_slot_allowed_variant_count": unique_first_slot_allowed,
            "recommended_variant_count": recommended,
            "no_ai_max_variant_count": no_ai_max,
            "role_allowed_variant_count": role_allowed,
            "counts": counts,
            "total_usable": total_usable,
            "diversity_capacity": diversity,
            "render_plan_capacity_estimate": plan_estimate,
            "gaps": gaps,
            "task_sync": task_sync,
        })


def _readiness_task_patch(tier: str, material_status: str, allowed: int, gaps: list[str], first_slot_remaining: int = 0, current_bottleneck: str = "") -> dict:
    patch = {
        "material_tier": tier,
        "material_status": material_status,
        "allowed_variant_count": allowed,
        "first_slot_remaining_capacity": first_slot_remaining,
        "current_bottleneck": current_bottleneck,
        "blocked_reason": "; ".join(gaps),
        "task_status": "READINESS_CHECKED",
    }
    if allowed > 0:
        patch["failure_reason"] = ""
    return patch


def _task(ctx: SkillContext, product_id: str):
    tasks = ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC", (product_id,))
    return tasks[0] if tasks else None


def _tier(counts, total):
    if counts["hero"] >= 3 and counts["detail"] >= 3 and counts["result"] >= 3 and counts["scene"] >= 1 and total >= 12:
        return "tier_3_full", 10, []
    if counts["hero"] >= 2 and counts["detail"] >= 2 and counts["result"] >= 2 and counts["scene"] >= 1 and total >= 7:
        return "tier_2_standard", 5, []
    if counts["hero"] >= 1 and counts["detail"] >= 2 and counts["result"] >= 1 and counts["scene"] >= 2 and counts["ending"] >= 1 and total >= 6:
        return "tier_1_reuse_heavy", 5, []
    gaps = []
    if counts["hero"] < 1:
        gaps.append("need at least 1 hero-capable segment")
    if counts["detail"] < 1:
        gaps.append("need at least 1 detail-capable segment")
    if counts["result"] < 1:
        gaps.append("need at least 1 result-capable segment")
    if total < 4:
        gaps.append("need at least 4 usable segments")
    if not gaps:
        return "tier_1_minimum", 2, []
    return "tier_0_not_ready", 0, gaps


def _diversity_capacity(ctx: SkillContext, product: dict, segments: list[dict]) -> dict:
    usable = [seg for seg in segments if seg.get("effective_roles_json")]
    first_slot = [seg for seg in usable if "hero" in (seg.get("effective_roles_json") or [])]
    unique_assets = {str(seg.get("asset_id") or "") for seg in usable if seg.get("asset_id")}
    conservative_segment_capacity = math.floor(len(usable) * MAX_SEGMENT_REUSE_PER_BATCH / AVG_SEGMENTS_PER_OUTPUT)
    relaxed_segment_capacity = math.floor(len(usable) * MAX_SUPPORT_SEGMENT_REUSE_PER_BATCH / AVG_SEGMENTS_PER_OUTPUT)
    first_slot_capacity = len(first_slot) * MAX_FIRST_SLOT_REUSE_PER_BATCH
    template_capacity = _template_capacity(ctx, product)
    recommended_capacity = max(0, min(conservative_segment_capacity, first_slot_capacity, template_capacity))
    no_ai_max_capacity = max(0, min(relaxed_segment_capacity, first_slot_capacity, template_capacity))
    bottlenecks = []
    if recommended_capacity == conservative_segment_capacity:
        bottlenecks.append("unique_segment_capacity_conservative")
    if no_ai_max_capacity == relaxed_segment_capacity:
        bottlenecks.append("unique_segment_capacity_light_reuse")
    if no_ai_max_capacity == first_slot_capacity:
        bottlenecks.append("first_slot_capacity")
    if no_ai_max_capacity == template_capacity:
        bottlenecks.append("template_capacity")
    return {
        "capacity": no_ai_max_capacity,
        "recommended_capacity": recommended_capacity,
        "no_ai_max_capacity": no_ai_max_capacity,
        "unique_usable_segments": len(usable),
        "unique_assets": len(unique_assets),
        "first_slot_candidates": len(first_slot),
        "first_slot_capacity": first_slot_capacity,
        "segment_reuse_cap": MAX_SEGMENT_REUSE_PER_BATCH,
        "support_segment_reuse_cap": MAX_SUPPORT_SEGMENT_REUSE_PER_BATCH,
        "first_slot_reuse_cap": MAX_FIRST_SLOT_REUSE_PER_BATCH,
        "template_reuse_cap": MAX_TEMPLATE_REUSE_PER_BATCH,
        "avg_segments_per_output": AVG_SEGMENTS_PER_OUTPUT,
        "template_capacity": template_capacity,
        "bottlenecks": bottlenecks,
    }


def _calibrate_render_plan_capacity(ctx: SkillContext, product_id: str, segments: list[dict], count: int) -> dict | None:
    if count <= 0 or not _has_segment_tags(ctx, segments):
        return None
    if len(segments) > MAX_HEAVY_RENDER_PLAN_ESTIMATE_SEGMENTS:
        return {
            "planned_count": count,
            "skipped_count": 0,
            "estimate_mode": "lightweight_skipped_heavy_render_plan",
            "segment_count": len(segments),
            "reason": f"segment_count>{MAX_HEAVY_RENDER_PLAN_ESTIMATE_SEGMENTS}",
        }
    try:
        return estimate_render_plan_capacity(ctx, product_id, count)
    except Exception as exc:
        return {"planned_count": count, "skipped_count": 0, "error": str(exc)}


def _actual_good_outputs(ctx: SkillContext, product_id: str) -> int:
    from .usage_counter_skill import is_good_rendered_output

    return sum(1 for output in ctx.repo.list_where("outputs", "product_id=?", (product_id,)) if is_good_rendered_output(output))


def _current_bottleneck(diversity: dict, gaps: list[str]) -> str:
    text = "；".join(str(item) for item in gaps)
    if "first slot" in text or "hero首镜" in text or "first_slot_capacity" in set(diversity.get("bottlenecks") or []):
        return "首镜容量不足"
    if "strict render plan" in text:
        return "模板/去重约束不足"
    if "diversity capacity" in text:
        return "素材多样性不足"
    if "need at least" in text:
        return "基础角色素材不足"
    return ""


def _has_segment_tags(ctx: SkillContext, segments: list[dict]) -> bool:
    ids = [str(seg.get("segment_id") or "") for seg in segments if seg.get("segment_id")]
    if not ids:
        return False
    placeholders = ",".join(["?"] * len(ids))
    rows = ctx.repo.list_where("segment_tags", f"segment_id IN ({placeholders}) LIMIT 1", tuple(ids))
    return bool(rows)


def _diversity_gap_messages(diversity: dict, role_allowed: int, recommended: int, no_ai_max: int) -> list[str]:
    shortfall = max(0, role_allowed - no_ai_max)
    messages = [
        (
            "diversity capacity limited: "
            f"role_allowed={role_allowed}, recommended={recommended}, no_ai_max={no_ai_max}, "
            f"unique_segments={diversity.get('unique_usable_segments')}, "
            f"first_slot_candidates={diversity.get('first_slot_candidates')}, "
            f"support_reuse_cap={diversity.get('support_segment_reuse_cap')}"
        )
    ]
    if shortfall > 0:
        need = min(max(shortfall, 1), 6)
        hero = max(1, min(2, need))
        detail = max(1, min(2, need))
        result = max(1, min(2, need))
        scene = max(1, min(2, need - 1)) if need >= 4 else 1
        messages.append(f"AI补素材: hero首镜{hero}; detail细节{detail}; result上身{result}; scene场景{scene}")
    return messages


def _render_plan_ai_supplement_message(diversity: dict, shortfall: int) -> str:
    need = min(max(shortfall, 1), 6)
    bottlenecks = set(diversity.get("bottlenecks") or [])
    if "first_slot_capacity" in bottlenecks:
        hero = max(1, min(3, need))
        detail = 1 if need >= 3 else 0
        result = 1 if need >= 4 else 0
    else:
        hero = max(1, min(2, need))
        detail = max(1, min(2, need - 1)) if need >= 2 else 0
        result = 1 if need >= 3 else 0
    parts = [f"hero首镜{hero}"]
    if detail:
        parts.append(f"detail细节{detail}")
    if result:
        parts.append(f"result上身{result}")
    return "AI补素材: " + "; ".join(parts)


def _first_slot_ai_supplement_message(shortfall: int) -> str:
    hero = min(max(int(shortfall or 0), 1), 6)
    return f"AI补素材: hero首镜{hero}"


def _template_capacity(ctx: SkillContext, product: dict) -> int:
    try:
        import yaml

        path = ctx.settings.root_dir / "config" / "templates.yaml"
        data = yaml.safe_load(path.read_text(encoding="utf-8")) if path.exists() else {}
        templates = data.get("templates") or []
    except Exception:
        return 999
    category = str(product.get("category") or "")
    aliases = _category_keys(category)
    count = 0
    for template in templates:
        if template.get("fallback_only"):
            continue
        categories = {str(item) for item in (template.get("suitable_categories") or [])}
        if not categories or aliases.intersection(categories):
            count += 1
    return max(0, count * MAX_TEMPLATE_REUSE_PER_BATCH) if count else 999


def _category_keys(category: str) -> set[str]:
    aliases = {
        "womens_outerwear": {"womens_outerwear", "womens_top", "womens_tops", "generic_fashion"},
        "womens_top": {"womens_outerwear", "womens_top", "womens_tops", "generic_fashion"},
        "womens_tops": {"womens_outerwear", "womens_top", "womens_tops", "generic_fashion"},
        "scarves_hats": {"scarves_hats", "scarf_hat", "scarves", "generic_fashion"},
        "scarf_hat": {"scarves_hats", "scarf_hat", "scarves", "generic_fashion"},
        "scarves": {"scarves_hats", "scarf_hat", "scarves", "generic_fashion"},
        "hair_accessories": {"hair_accessories", "generic_fashion"},
        "earrings": {"earrings", "generic_fashion"},
        "generic_fashion": {"generic_fashion"},
    }
    return aliases.get(category, {category, "generic_fashion"})
