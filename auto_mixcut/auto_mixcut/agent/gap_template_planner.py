from __future__ import annotations

import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from auto_mixcut.core.result import Result
from auto_mixcut.skills.context import SkillContext


VALID_ROLES = {"hero", "detail", "result", "scene", "ending"}
VALID_GRADES = {"A", "B", "C"}
VALID_SEGMENT_TYPES = {
    "product_display",
    "handheld_product",
    "detail_atmosphere",
    "tryon_result",
    "mirror_routine",
    "home_lifestyle",
    "before_go_out",
    "seasonal_scene",
    "product_still",
    "unboxing",
    "flatlay",
}
PRODUCT_ONLY_SEGMENT_TYPES = {"product_still", "unboxing", "flatlay"}


@dataclass
class PlannerConfig:
    data: dict[str, Any]

    @property
    def root(self) -> dict[str, Any]:
        return self.data.get("gap_and_template") or {}

    def get(self, *path: str, default: Any = None) -> Any:
        node: Any = self.root
        for key in path:
            if not isinstance(node, dict):
                return default
            node = node.get(key)
        return default if node is None else node


class GapTemplatePlanner:
    """Plans template rotation, slot demand, and AI generation gaps.

    This module is intentionally upstream-only: it returns generation tasks and
    clip orders without invoking the prompt factory or changing render flow.
    """

    def __init__(self, ctx: SkillContext):
        self.ctx = ctx
        self.config = PlannerConfig(_load_yaml(ctx.settings.root_dir / "config" / "gap_and_template.yaml"))
        self.templates = _load_templates(ctx.settings.root_dir / "config" / "templates.yaml")

    def plan_round(
        self,
        round_goal: dict[str, Any],
        asset_pool: list[dict[str, Any]] | None = None,
        diversity_budgets: dict[str, Any] | None = None,
        template_history: dict[str, list[str]] | None = None,
        phash_clustered_products: set[str] | None = None,
        trusted_real_anchor_counts: dict[str, int] | None = None,
    ) -> Result:
        asset_pool = asset_pool or []
        diversity_budgets = diversity_budgets or {}
        template_history = template_history or {}
        phash_clustered_products = phash_clustered_products or set()
        trusted_real_anchor_counts = trusted_real_anchor_counts or {}

        result = {
            "round_id": str(round_goal.get("round_id") or ""),
            "template_plans": [],
            "demand": [],
            "gaps": [],
            "generation_tasks": [],
            "clip_orders": [],
            "warnings": [],
            "cost": {},
        }
        for target in round_goal.get("targets") or []:
            product_id = str(target.get("product_id") or "").strip()
            category = str(target.get("category") or "").strip()
            target_clips = _to_int(target.get("target_clips"), 0)
            if not product_id or not category or target_clips <= 0:
                result["warnings"].append({"product_id": product_id, "type": "invalid_target", "target": target})
                continue

            candidates_result = self._candidate_templates(category)
            if not candidates_result.success:
                return candidates_result
            candidates = candidates_result.data["templates"]
            result["warnings"].extend({"product_id": product_id, **warning} for warning in candidates_result.data.get("warnings", []))

            template_plan = self._plan_template_distribution(product_id, target_clips, candidates, template_history.get(product_id, []))
            template_plan = self._enforce_rotation(product_id, template_plan, candidates, template_history.get(product_id, []), result["warnings"])
            result["template_plans"].append({"product_id": product_id, "template_plan": template_plan})

            if self._template_exhausted(product_id, candidates, template_history.get(product_id, []), phash_clustered_products):
                result["warnings"].append({"product_id": product_id, "type": "template_exhausted"})

            demand = self._expand_demand(product_id, template_plan)
            result["demand"].append({"product_id": product_id, "demand": demand})

            budget = _budget_for_product(diversity_budgets, product_id)
            gaps = self._calculate_gaps(product_id, category, demand, asset_pool, budget, trusted_real_anchor_counts.get(product_id, 0), result["warnings"])
            result["gaps"].append({"product_id": product_id, "gaps": gaps})

            result["generation_tasks"].extend(self._generation_tasks(product_id, category, template_plan, gaps))
            result["clip_orders"].extend(_clip_orders(result["round_id"], product_id, template_plan))

        result["cost"] = self._estimate_and_gate_cost(result)
        return Result.ok(result)

    def validate_templates(self) -> Result:
        errors = []
        warnings = []
        for template in self.templates:
            if template.get("fallback_only"):
                continue
            slots = template.get("slots")
            if not slots:
                continue
            is_ai_template = str(template.get("template_id") or "").startswith("AI_")
            if not template.get("required_roles"):
                warnings.append({"template_id": template.get("template_id"), "warning": "required_roles_missing"})
            for required in ["suitable_categories", "hook_profile", "risk_policy", "source_policy", "bgm_profile"]:
                if required not in template:
                    target = errors if is_ai_template and required in {"suitable_categories", "risk_policy"} else warnings
                    target.append({"template_id": template.get("template_id"), "error" if target is errors else "warning": f"{required}_missing"})
            for idx, slot in enumerate(slots):
                role = slot.get("role")
                grade = slot.get("ai_gen_grade")
                if role not in VALID_ROLES:
                    errors.append({"template_id": template.get("template_id"), "slot": idx, "error": "invalid_role"})
                if is_ai_template and _to_int(slot.get("duration_ms"), 0) < 4000:
                    errors.append({"template_id": template.get("template_id"), "slot": idx, "error": "duration_under_4000"})
                expected_grade = self._role_to_grade(role)
                if grade and expected_grade and grade != expected_grade:
                    warnings.append({"template_id": template.get("template_id"), "slot": idx, "warning": "role_grade_mismatch", "expected": expected_grade, "actual": grade})
                segment_type = slot.get("segment_type")
                if is_ai_template and segment_type and segment_type not in VALID_SEGMENT_TYPES:
                    errors.append({"template_id": template.get("template_id"), "slot": idx, "error": "invalid_segment_type"})
                elif is_ai_template and not segment_type:
                    warnings.append({"template_id": template.get("template_id"), "slot": idx, "warning": "segment_type_missing_defaulted_by_role"})
                for required in ["hook_intent", "ai_gen_grade", "ai_fill_min_grade", "person_framing", "preferred_source_trust"]:
                    if required not in slot:
                        target = errors if is_ai_template and required in {"hook_intent", "ai_gen_grade", "ai_fill_min_grade", "person_framing"} else warnings
                        target.append({"template_id": template.get("template_id"), "slot": idx, "error" if target is errors else "warning": f"{required}_missing"})
                if idx == 0:
                    if slot.get("preferred_binding") != "exact_sku":
                        errors.append({"template_id": template.get("template_id"), "slot": idx, "error": "first_slot_preferred_binding_must_exact_sku"})
                    first_policy = template.get("risk_policy") or {}
                    if not first_policy.get("require_no_watermark_for_first_slot"):
                        errors.append({"template_id": template.get("template_id"), "slot": idx, "error": "first_slot_no_watermark_policy_missing"})
        return Result.fail("TEMPLATE_VALIDATION_FAILED", "template compliance validation failed", {"errors": errors, "warnings": warnings}) if errors else Result.ok({"warnings": warnings})

    def _candidate_templates(self, category: str) -> Result:
        candidates = []
        warnings = []
        category_keys = _category_keys(category)
        for template in self.templates:
            if template.get("fallback_only"):
                continue
            suitable = template.get("suitable_categories")
            if suitable:
                if not category_keys.intersection(str(item) for item in suitable):
                    continue
            else:
                warnings.append({"type": "template_missing_suitable_categories_treated_as_universal", "template_id": template.get("template_id")})
            candidates.append(template)

        hard_min = _to_int(self.config.get("template_candidates", "hard_min_candidates", default=2), 2)
        recommended = _to_int(self.config.get("template_candidates", "recommended_min_candidates", default=3), 3)
        if len(candidates) < hard_min:
            return Result.fail("need_more_templates", "candidate templates are below hard minimum", {"category": category, "candidate_count": len(candidates), "hard_min": hard_min})
        if len(candidates) < recommended:
            warnings.append({"type": "template_rotation_low_margin", "candidate_count": len(candidates), "recommended_min": recommended})
        return Result.ok({"templates": candidates, "warnings": warnings})

    def _plan_template_distribution(self, product_id: str, target_clips: int, candidates: list[dict[str, Any]], history: list[str]) -> list[dict[str, Any]]:
        counts = {str(t.get("template_id")): target_clips // len(candidates) for t in candidates}
        remainder = target_clips % len(candidates)
        history_counts = Counter(history)
        order = sorted((str(t.get("template_id")) for t in candidates), key=lambda tid: (history_counts[tid], tid))
        for tid in order[:remainder]:
            counts[tid] += 1
        return [{"template_id": tid, "clip_count": count} for tid, count in counts.items() if count > 0]

    def _enforce_rotation(self, product_id: str, plan: list[dict[str, Any]], candidates: list[dict[str, Any]], history: list[str], warnings: list[dict[str, Any]]) -> list[dict[str, Any]]:
        window = _to_int(self.config.get("template_rotation", "recent_window_clips", default=8), 8)
        max_ratio = float(self.config.get("template_rotation", "single_template_max_ratio", default=0.5))
        candidate_ids = [str(t.get("template_id")) for t in candidates]
        counts = {str(item["template_id"]): _to_int(item.get("clip_count"), 0) for item in plan}
        recent = history[-window:]
        history_counts = Counter(recent)
        total = len(recent) + sum(counts.values())
        cap = math.floor(total * max_ratio)
        changed = False
        for tid in list(counts):
            while history_counts[tid] + counts[tid] > cap and counts[tid] > 0:
                receiver = min(candidate_ids, key=lambda other: (history_counts[other] + counts.get(other, 0), other))
                if receiver == tid:
                    break
                counts[tid] -= 1
                counts[receiver] = counts.get(receiver, 0) + 1
                changed = True
        if changed:
            warnings.append({"product_id": product_id, "type": "template_rotation_rebalanced"})
        return [{"template_id": tid, "clip_count": count} for tid, count in counts.items() if count > 0]

    def _template_exhausted(self, product_id: str, candidates: list[dict[str, Any]], history: list[str], phash_clustered_products: set[str]) -> bool:
        if product_id not in phash_clustered_products or not candidates:
            return False
        counts = Counter(history)
        return all(counts[str(template.get("template_id"))] >= 2 for template in candidates)

    def _expand_demand(self, product_id: str, template_plan: list[dict[str, Any]]) -> list[dict[str, Any]]:
        demand: dict[tuple[str, str], int] = defaultdict(int)
        by_id = {str(template.get("template_id")): template for template in self.templates}
        for plan_item in template_plan:
            template = by_id.get(str(plan_item.get("template_id"))) or {}
            for slot in _template_slots(template, self.config):
                role = slot["role"]
                grade = slot.get("ai_gen_grade") or self._role_to_grade(role)
                demand[(role, grade)] += _to_int(plan_item.get("clip_count"), 0)
        return [{"slot_role": role, "grade": grade, "count": count} for (role, grade), count in sorted(demand.items())]

    def _calculate_gaps(
        self,
        product_id: str,
        category: str,
        demand: list[dict[str, Any]],
        asset_pool: list[dict[str, Any]],
        diversity_budget: dict[str, Any],
        trusted_real_anchor_count: int,
        warnings: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        max_reuse = _to_int(self.config.get("reuse", "max_reuse_per_asset", default=3), 3)
        ai_ratio_cap = float(diversity_budget.get("ai_ratio_cap", 0.5))
        total_demand = sum(_to_int(item.get("count"), 0) for item in demand)
        ai_budget_total = round(total_demand * ai_ratio_cap)
        ai_used_this_round = 0
        assets_by_cell = _index_available_assets(product_id, asset_pool, max_reuse)
        gaps = []
        for cell in demand:
            role = str(cell.get("slot_role"))
            grade = str(cell.get("grade"))
            count = _to_int(cell.get("count"), 0)
            real_assets = assets_by_cell.get((role, grade, "real"), [])
            ai_assets = assets_by_cell.get((role, grade, "ai"), [])
            real_available = sum(asset["remaining"] for asset in real_assets)
            ai_available = sum(asset["remaining"] for asset in ai_assets)

            use_real = min(count, real_available)
            remain = count - use_real
            ai_budget = max(0, ai_budget_total - ai_used_this_round)
            use_ai_existing = min(remain, ai_available, ai_budget)
            remain -= use_ai_existing
            ai_budget -= use_ai_existing
            ai_used_this_round += use_ai_existing

            ai_gen_gap = min(remain, ai_budget)
            if _is_retire_blocked(diversity_budget, trusted_real_anchor_count):
                ai_gen_gap = 0
                if remain > 0:
                    warnings.append({"product_id": product_id, "type": "retire_candidate", "reason": "diversity_exhausted_real_shortage"})
            remain -= ai_gen_gap
            ai_used_this_round += ai_gen_gap

            shortfall = max(0, remain)
            if shortfall and role == "hero" and grade == "A":
                warnings.append({"product_id": product_id, "type": "hero_a_real_shortage_reduce_clips", "shortfall": shortfall})
            elif shortfall:
                warnings.append({"product_id": product_id, "type": "slot_shortfall_after_ai_budget", "slot_role": role, "grade": grade, "shortfall": shortfall})
            gaps.append({
                "slot_role": role,
                "grade": grade,
                "demand": count,
                "real_available": real_available,
                "use_real": use_real,
                "ai_available": ai_available,
                "use_ai_existing": use_ai_existing,
                "ai_gen_gap": ai_gen_gap,
                "shortfall": shortfall,
                "exhausted_assets": [asset["asset_id"] for asset in _assets_for_product(product_id, asset_pool) if _to_int(asset.get("times_used"), 0) >= max_reuse],
            })
        return gaps

    def _generation_tasks(self, product_id: str, category: str, template_plan: list[dict[str, Any]], gaps: list[dict[str, Any]]) -> list[dict[str, Any]]:
        by_role = {(gap["slot_role"], gap["grade"]): _to_int(gap.get("ai_gen_gap"), 0) for gap in gaps}
        slot_candidates: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        tasks = []
        by_id = {str(template.get("template_id")): template for template in self.templates}
        for plan_item in template_plan:
            template = by_id.get(str(plan_item.get("template_id"))) or {}
            for _ in range(_to_int(plan_item.get("clip_count"), 0)):
                for slot in _template_slots(template, self.config):
                    role = slot["role"]
                    grade = slot.get("ai_gen_grade") or self._role_to_grade(role)
                    slot_candidates[(role, grade)].append({"template_id": template.get("template_id"), **slot})
        for (role, grade), needed in by_role.items():
            for _ in range(needed):
                slot = slot_candidates[(role, grade)].pop(0) if slot_candidates.get((role, grade)) else self._default_slot(role, grade)
                tasks.append(_generation_task_from_slot(product_id, category, slot, self.config))
        return tasks

    def _estimate_and_gate_cost(self, result: dict[str, Any]) -> dict[str, Any]:
        prices = self.config.get("cost", "unit_price_grade", default={}) or {}
        redundancy = float(self.config.get("cost", "rerender_redundancy", default=1.3))
        cap = float(self.config.get("cost", "round_budget_cap", default=0) or 0)
        generation_cost = sum(float(prices.get(task.get("ai_gen_grade"), 0) or 0) for task in result.get("generation_tasks") or [])
        estimated = generation_cost * redundancy
        gated = {"estimated_cost": estimated, "generation_cost": generation_cost, "rerender_redundancy": redundancy, "budget_cap": cap, "over_budget": bool(cap and estimated > cap)}
        if gated["over_budget"]:
            result["warnings"].append({"type": "round_cost_over_budget", "estimated_cost": estimated, "budget_cap": cap, "action_order": ["increase_reuse", "cut_c_grade_generation", "reduce_target_clips"]})
        return gated

    def _role_to_grade(self, role: str) -> str:
        return str(self.config.get("role_to_grade", role, default="C"))

    def _default_slot(self, role: str, grade: str) -> dict[str, Any]:
        return {
            "template_id": "",
            "role": role,
            "ai_gen_grade": grade,
            "segment_type": self.config.get("role_to_segment_type", role, default="home_lifestyle"),
            "hook_intent": self.config.get("role_to_hook_intent", role, default="atmosphere"),
            "person_framing": "ai_local" if grade in {"A", "B"} else "real_preferred",
        }


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}
    except FileNotFoundError:
        return {}


def _load_templates(path: Path) -> list[dict[str, Any]]:
    data = _load_yaml(path)
    return [dict(item or {}) for item in data.get("templates") or []]


def _template_slots(template: dict[str, Any], config: PlannerConfig) -> list[dict[str, Any]]:
    slots = template.get("slots")
    if slots:
        return _apply_product_only_quota([dict(slot or {}) for slot in slots], config, str(template.get("template_id") or ""))
    roles = template.get("roles") or template.get("required_roles") or []
    normalized = []
    for idx, role in enumerate(roles):
        if role not in VALID_ROLES:
            continue
        grade = str(config.get("role_to_grade", role, default="C"))
        normalized.append({
            "role": role,
            "duration_ms": 4000,
            "ai_gen_grade": grade,
            "segment_type": config.get("role_to_segment_type", role, default="home_lifestyle"),
            "hook_intent": config.get("role_to_hook_intent", role, default="atmosphere"),
            "person_framing": "ai_local" if grade in {"A", "B"} else "real_preferred",
        })
    return _apply_product_only_quota(normalized, config, str(template.get("template_id") or ""))


def _apply_product_only_quota(slots: list[dict[str, Any]], config: PlannerConfig, template_id: str = "") -> list[dict[str, Any]]:
    quota = config.get("product_only_quota", default={}) or {}
    ratio_cap = float(quota.get("product_only_ratio_cap", 0.40) or 0.40)
    preferred_roles = set(quota.get("preferred_roles_for_product_only") or ["detail", "hero"])
    forbid_roles = set(quota.get("forbid_product_only_roles") or ["result"])
    ai_slots_all = [
        idx for idx, slot in enumerate(slots)
        if str(slot.get("person_framing") or "ai_local") in {"ai_local", "ai_full_face", "product_only"}
    ]
    convertible_slots = [idx for idx in ai_slots_all if str(slots[idx].get("role") or "") not in forbid_roles]
    if not ai_slots_all or not convertible_slots:
        return slots
    max_product_only = int(len(ai_slots_all) * ratio_cap)
    if max_product_only <= 0:
        return slots
    minimum = 1 if len(ai_slots_all) >= 3 else 0
    existing = [idx for idx in convertible_slots if str(slots[idx].get("person_framing") or "") == "product_only"]
    target = min(max_product_only, max(minimum, len(existing)))
    if len(existing) >= target:
        return slots
    candidates = [
        idx for idx in convertible_slots
        if idx not in existing
        and str(slots[idx].get("role") or "") in preferred_roles
    ]
    if not candidates and minimum:
        candidates = [idx for idx in convertible_slots if idx not in existing]
    needed = max(0, target - len(existing))
    for offset, idx in enumerate(candidates[:needed]):
        slot = slots[idx]
        slot["person_framing"] = "product_only"
        slot["segment_type"] = _product_only_segment_type_for_slot(slot, template_id, offset)
        slot.setdefault("hook_intent", "material_closeup" if slot.get("role") == "detail" else "product_clarity")
    return slots


def _product_only_segment_type_for_slot(slot: dict[str, Any], template_id: str, offset: int) -> str:
    role = str(slot.get("role") or "")
    seed = sum(ord(ch) for ch in f"{template_id}:{role}:{slot.get('hook_intent')}:{offset}")
    if role == "hero":
        return ["unboxing", "product_still"][seed % 2]
    if role == "detail":
        return ["product_still", "flatlay"][seed % 2]
    return ["product_still", "flatlay", "unboxing"][seed % 3]


def _index_available_assets(product_id: str, assets: list[dict[str, Any]], max_reuse: int) -> dict[tuple[str, str, str], list[dict[str, Any]]]:
    indexed: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for asset in _assets_for_product(product_id, assets):
        if asset.get("usable_grade") == "reject":
            continue
        remaining = max_reuse - _to_int(asset.get("times_used"), 0)
        if remaining <= 0:
            continue
        source_type = str(asset.get("source_type") or "real")
        roles = _asset_roles(asset)
        grades = _asset_grades(asset)
        enriched = {**asset, "remaining": remaining}
        for role in roles:
            for grade in grades:
                indexed[(role, grade, source_type)].append(enriched)
    for key in indexed:
        indexed[key].sort(key=lambda asset: _asset_priority(asset))
    return indexed


def _asset_priority(asset: dict[str, Any]) -> float:
    base = float(asset.get("base_priority", 0) or 0)
    return base - float(asset.get("times_used", 0) or 0)


def _assets_for_product(product_id: str, assets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [asset for asset in assets if str(asset.get("product_id") or "") == product_id]


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


def _asset_roles(asset: dict[str, Any]) -> list[str]:
    roles = asset.get("slot_roles") or asset.get("roles")
    if isinstance(roles, list):
        return [role for role in roles if role in VALID_ROLES]
    role = asset.get("slot_role")
    return [role] if role in VALID_ROLES else list(VALID_ROLES)


def _asset_grades(asset: dict[str, Any]) -> list[str]:
    grade = asset.get("ai_gen_grade") or asset.get("grade")
    if grade in VALID_GRADES:
        return [grade]
    usable = asset.get("usable_grade")
    if usable == "usable_core":
        return ["A", "B", "C"]
    if usable == "usable_scene":
        return ["B", "C"]
    return ["A", "B", "C"]


def _budget_for_product(budgets: dict[str, Any], product_id: str) -> dict[str, Any]:
    if product_id in budgets and isinstance(budgets[product_id], dict):
        return budgets[product_id]
    if budgets.get("product_id") == product_id:
        return budgets
    return {"product_id": product_id, "ai_ratio_cap": 0.5}


def _is_retire_blocked(diversity_budget: dict[str, Any], trusted_real_anchor_count: int) -> bool:
    return str(diversity_budget.get("status") or "") == "mature" and float(diversity_budget.get("ai_ratio_cap", 1) or 1) <= 0.30 and trusted_real_anchor_count < 1


def _clip_orders(round_id: str, product_id: str, template_plan: list[dict[str, Any]]) -> list[dict[str, Any]]:
    orders = []
    counter = 1
    for item in template_plan:
        for _ in range(_to_int(item.get("clip_count"), 0)):
            orders.append({"clip_id": f"{product_id}-{counter:03d}", "round_id": round_id, "product_id": product_id, "template_id": item.get("template_id")})
            counter += 1
    return orders


def _generation_task_from_slot(product_id: str, category: str, slot: dict[str, Any], config: PlannerConfig) -> dict[str, Any]:
    role = str(slot.get("role") or "")
    grade = str(slot.get("ai_gen_grade") or config.get("role_to_grade", role, default="C"))
    return {
        "product_id": product_id,
        "category": category,
        "template_id": slot.get("template_id") or "",
        "slot_role": role,
        "ai_gen_grade": grade,
        "segment_type": slot.get("segment_type") or config.get("role_to_segment_type", role, default="home_lifestyle"),
        "hook_intent": slot.get("hook_intent") or config.get("role_to_hook_intent", role, default="atmosphere"),
        "person_framing": slot.get("person_framing") or ("ai_local" if grade in {"A", "B"} else "real_preferred"),
        "duration_sec": 4,
    }


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
