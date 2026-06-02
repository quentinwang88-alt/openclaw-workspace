from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import yaml

from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result

from .context import SkillContext


DEFAULT_TEMPLATE_SPECS = [
    {"template_id": "GENERAL_BALANCED_15S", "duration_ms": 15000, "slots": [{"role": role, "duration_ms": 3000} for role in ["hero", "detail", "result", "scene", "ending"]]},
    {"template_id": "CLEAN_PRODUCT_PROOF_15S", "duration_ms": 15000, "slots": [{"role": role, "duration_ms": 3000} for role in ["hero", "detail", "hero", "scene", "ending"]]},
    {"template_id": "DETAIL_TEXTURE_PROOF_15S", "duration_ms": 15000, "slots": [{"role": role, "duration_ms": 3000} for role in ["hero", "detail", "detail", "result", "ending"]]},
    {"template_id": "TRYON_RESULT_FAST_15S", "duration_ms": 15000, "slots": [{"role": role, "duration_ms": 3000} for role in ["result", "hero", "detail", "result", "ending"]]},
]
TRUST_RANK = {"low": 1, "medium": 2, "high": 3}
CORE_ROLES = {"hero", "detail", "result"}
FALLBACK_TEMPLATE_ID = "GENERAL_BALANCED_15S"


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


class RenderPlanSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def create_plans(self, product_id: str, count: int | None = None) -> Result:
        task = _task(self.ctx, product_id)
        allowed = int((task or {}).get("allowed_variant_count") or 0)
        total = min(count or allowed, allowed)
        if total <= 0:
            return Result.fail("MATERIAL_NOT_READY", "allowed_variant_count is zero", {"product_id": product_id})
        templates = _load_templates(self.ctx)
        product = self.ctx.repo.get("products", "product_id", product_id) or {}
        batch_id = new_id("BATCH")
        self.ctx.repo.upsert(
            "mixcut_batches",
            "batch_id",
            {"batch_id": batch_id, "product_id": product_id, "task_id": (task or {}).get("task_id"), "requested_count": count or allowed, "allowed_count": allowed, "rendered_count": 0, "batch_status": "planning", "material_tier": (task or {}).get("material_tier"), "template_pool_json": [t.template_id for t in templates]},
        )
        plans = []
        skipped = []
        batch_state = {"segments": set(), "assets": {}, "first_assets": set()}
        for variant in range(1, total + 1):
            choice = _choose_template(self.ctx, product, templates, batch_state, variant)
            template = choice["template"]
            selected = _select_segments(self.ctx, product_id, template.slots, batch_state=batch_state, variant_no=variant, template=template)
            if not selected.success:
                if selected.error and selected.error.code == "SKIPPED_LOW_QUALITY":
                    plan_id = new_id("PLAN")
                    row = {
                        "render_plan_id": plan_id,
                        "batch_id": batch_id,
                        "product_id": product_id,
                        "variant_no": variant,
                        "template_id": template.template_id,
                        "planned_duration_ms": 0,
                        "plan_json": {"segments": [], "template": _template_plan_json(template), "template_selection": choice["debug"], "skip_reason": selected.error.detail},
                        "quality_gate_status": "skipped_low_quality",
                        "render_status": "skipped_low_quality",
                    }
                    self.ctx.repo.upsert("render_plans", "render_plan_id", row)
                    _sync_material_supplement_queue(self.ctx, row, selected.error.detail)
                    skipped.append(plan_id)
                    continue
                return selected
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
                    "start_ms_in_output": cursor,
                    "end_ms_in_output": cursor + dur,
                })
                cursor += dur
            row = {
                "render_plan_id": plan_id,
                "batch_id": batch_id,
                "product_id": product_id,
                "variant_no": variant,
                "template_id": template.template_id,
                "planned_duration_ms": cursor,
                "plan_json": {"segments": slots, "template": _template_plan_json(template), "template_selection": choice["debug"]},
                "quality_gate_status": "pending",
                "render_status": "planned",
            }
            self.ctx.repo.upsert("render_plans", "render_plan_id", row)
            _record_selection(self.ctx, selected.data, batch_state)
            plans.append(plan_id)
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"task_status": "RENDER_PLAN_CREATED"})
        return Result.ok({"batch_id": batch_id, "render_plan_ids": plans, "skipped_render_plan_ids": skipped})

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
        _record_selection(self.ctx, selected.data, {"segments": set(), "assets": {}, "first_assets": set()})
        return Result.ok({"batch_id": batch_id, "render_plan_id": plan_id})


def _select_segments(ctx: SkillContext, product_id: str, slots, batch_state: dict | None = None, variant_no: int = 1, constraints: dict | None = None, template: TemplateSpec | None = None) -> Result:
    segments = ctx.repo.list_where("segments", "product_id=?", (product_id,))
    constraints = constraints or {}
    selected = []
    state = batch_state or {"segments": set(), "assets": {}, "first_assets": set()}
    for slot in slots:
        role = slot["role"]
        pool = [s for s in segments if role in (s.get("effective_roles_json") or [])]
        if not pool and role == "hero":
            pool = _hero_fallback_pool(segments)
        pool = _filter_constraints(ctx, pool, constraints, slot_index=len(selected) + 1)
        filtered = _filter_slot_pool(pool, slot)
        if filtered:
            pool = filtered
        slot_index = len(selected) + 1
        if slot_index == 1 and template:
            first_slot_pool = [s for s in pool if _passes_first_slot_floor(ctx, s, template.risk_policy)[0]]
            if not first_slot_pool:
                failures = [_first_slot_failure(ctx, s, template.risk_policy) for s in pool]
                return Result.fail(
                    "SKIPPED_LOW_QUALITY",
                    "no first-slot segment passes hard quality floor",
                    {"status": "skipped_low_quality", "template_id": template.template_id, "role": role, "first_slot_failures": failures},
                )
            pool = _prefer_first_slot_pool(ctx, first_slot_pool)
            unused_first_asset_pool = [s for s in pool if s.get("asset_id") not in state.get("first_assets", set())]
            if unused_first_asset_pool:
                pool = unused_first_asset_pool
        selected_segment_ids = {item["segment_id"] for item in selected}
        non_duplicate_pool = [s for s in pool if s["segment_id"] not in selected_segment_ids]
        if non_duplicate_pool:
            pool = non_duplicate_pool
        non_consecutive_asset_pool = _filter_non_consecutive_asset(pool, selected)
        if non_consecutive_asset_pool:
            pool = non_consecutive_asset_pool
        core_asset_pool = _filter_core_asset_reuse(pool, selected, role)
        if core_asset_pool:
            pool = core_asset_pool
        if not pool:
            return Result.fail("RENDER_PLAN_FAILED", f"no segment available for role {role}", {"role": role})
        scored = sorted(
            ((_segment_score(ctx, item, selected, state, slot, slot_index, variant_no, constraints), item) for item in pool),
            key=lambda item: (item[0], item[1]["segment_id"]),
            reverse=True,
        )
        choice = scored[0][1]
        selected.append({
            "role": role,
            "duration_ms": slot.get("duration_ms") or 3000,
            "segment_type": slot.get("segment_type"),
            "hook_intent": slot.get("hook_intent"),
            "ai_gen_grade": slot.get("ai_gen_grade"),
            "segment_id": choice["segment_id"],
            "asset_id": choice["asset_id"],
        })
    return Result.ok(selected)


def _hero_fallback_pool(segments: list[dict]) -> list[dict]:
    return [
        s
        for s in segments
        if {"result", "detail"}.intersection(s.get("effective_roles_json") or [])
    ]


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
    if slot_index == 1 and {"hero", "result", "detail"}.intersection(segment.get("effective_roles_json") or []):
        score += 20
    if segment.get("source_type") in (slot.get("preferred_source_types") or []):
        score += 18
    if slot.get("preferred_source_trust") and segment.get("source_trust_level") == slot.get("preferred_source_trust"):
        score += 12
    if slot.get("preferred_binding") and segment.get("product_binding_type") == slot.get("preferred_binding"):
        score += 10

    asset_id = segment["asset_id"]
    current_asset_count = selected_asset_counts.get(asset_id, 0)
    score -= current_asset_count * 40
    if current_asset_count >= 2:
        score -= 120
    if segment["segment_id"] in batch_state.get("segments", set()):
        score -= 80
    score -= int(batch_state.get("assets", {}).get(asset_id, 0)) * 30
    if slot_index == 1 and asset_id in batch_state.get("first_assets", set()):
        score -= 180
    score -= int(segment.get("usage_count") or 0) * 5
    if slot_index <= 3:
        text_risk = _text_overlay_risk(ctx, segment)
        if text_risk == "foreign_language_caption":
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

    # Deterministic spread so equal candidates do not always pick the same early id.
    score += _stable_spread(segment["segment_id"], variant_no, slot_index)
    return score


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
    return candidates


def _choose_template(ctx: SkillContext, product: dict, templates: list[TemplateSpec], batch_state: dict, variant: int) -> dict[str, Any]:
    legacy_order = ["GENERAL_BALANCED_15S", "RESULT_FIRST_15S", "DETAIL_HOOK_15S", "CLEAN_PRODUCT_PROOF_15S"]
    ordered = [next((t for t in templates if t.template_id == template_id), None) for template_id in legacy_order]
    ordered = [t for t in ordered if t is not None]
    if variant <= len(ordered):
        template = ordered[variant - 1]
        batch_state.setdefault("templates", set()).add(template.template_id)
        return {"template": template, "debug": {"strategy": "legacy_stable_opening_order", "variant": variant}}
    category = str(product.get("category") or "generic_fashion")
    category_keys = _category_keys(category)
    scored = []
    for index, template in enumerate(templates):
        score = 0
        if category_keys.intersection(template.suitable_categories) or "generic_fashion" in template.suitable_categories:
            score += 30
        if variant > 1 and template.template_id in batch_state.get("templates", set()):
            score -= 6
        score += (variant + index) % 5
        scored.append((score, -index, template))
    scored.sort(reverse=True)
    template = scored[0][2] if scored else templates[(variant - 1) % len(templates)]
    batch_state.setdefault("templates", set()).add(template.template_id)
    return {"template": template, "debug": {"strategy": "template_v2_rule_score", "category": category, "score": scored[0][0] if scored else 0}}


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


def _passes_first_slot_floor(ctx: SkillContext, segment: dict, risk_policy: dict[str, Any]) -> tuple[bool, str]:
    if risk_policy.get("require_no_watermark_for_first_slot") and _asset_has_watermark(ctx, segment):
        return False, "first slot asset has watermark"
    if risk_policy.get("avoid_subtitle_risk_in_first_slot") and _has_subtitle_risk(ctx, segment):
        return False, "first slot has subtitle risk"
    risk = _latest_tag_value(ctx, segment, "risk_level") or segment.get("risk_level")
    if risk == "high":
        return False, "first slot risk level high"
    visibility = _latest_tag_value(ctx, segment, "product_visibility")
    if visibility == "low":
        return False, "first slot product visibility low"
    return True, ""


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
    asset = ctx.repo.get("assets", "asset_id", segment.get("asset_id")) if segment.get("asset_id") else None
    value = (asset or {}).get("has_watermark") or segment.get("has_watermark")
    normalized = str(value).strip().lower()
    if normalized in {"yes", "true", "1"}:
        return True
    if normalized in {"no", "false", "0", "processed", "skipped", "pending"}:
        return False
    reason = f"{segment.get('effective_roles_reason') or ''} {(asset or {}).get('watermark_reason') or ''}".lower()
    return "watermark" in reason or "水印" in reason


def _has_subtitle_risk(ctx: SkillContext, segment: dict) -> bool:
    risk = _text_overlay_risk(ctx, segment)
    if risk in {"foreign_language_caption", "platform_ui_or_watermark", "large_obstructive_text"}:
        return True
    text = f"{segment.get('effective_roles_reason') or ''} {segment.get('product_match_reason') or ''}".lower()
    return any(token in text for token in ["subtitle", "caption", "字幕", "文字遮挡"])


def _sync_material_supplement_queue(ctx: SkillContext, row: dict, detail: dict) -> None:
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
        },
    )


def _latest_tag_value(ctx: SkillContext, segment: dict, key: str):
    if key in segment:
        return segment.get(key)
    rows = ctx.repo.list_where("segment_tags", "segment_id=? ORDER BY id DESC LIMIT 1", (segment["segment_id"],))
    return rows[0].get(key) if rows else None


def _text_overlay_risk(ctx: SkillContext, segment: dict) -> str:
    value = _latest_tag_value(ctx, segment, "text_overlay_risk")
    return str(value or "none")


def _filter_non_consecutive_asset(pool: list[dict], selected: list[dict]) -> list[dict]:
    if not selected:
        return pool
    previous_asset = selected[-1].get("asset_id")
    return [s for s in pool if s.get("asset_id") != previous_asset]


def _filter_core_asset_reuse(pool: list[dict], selected: list[dict], role: str) -> list[dict]:
    if role not in CORE_ROLES:
        return pool
    used_core_assets = {
        item.get("asset_id")
        for item in selected
        if item.get("role") in CORE_ROLES
    }
    return [s for s in pool if s.get("asset_id") not in used_core_assets]


def _filter_slot_pool(pool: list[dict], slot: dict) -> list[dict]:
    candidates = list(pool)
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
    for index, item in enumerate(selected, start=1):
        batch_state.setdefault("segments", set()).add(item["segment_id"])
        assets = batch_state.setdefault("assets", {})
        assets[item["asset_id"]] = int(assets.get(item["asset_id"], 0)) + 1
        if index == 1:
            batch_state.setdefault("first_assets", set()).add(item["asset_id"])
        segment = ctx.repo.get("segments", "segment_id", item["segment_id"]) or {}
        ctx.repo.update("segments", "segment_id", item["segment_id"], {"usage_count": int(segment.get("usage_count") or 0) + 1})


def _task(ctx: SkillContext, product_id: str):
    tasks = ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC", (product_id,))
    return tasks[0] if tasks else None
