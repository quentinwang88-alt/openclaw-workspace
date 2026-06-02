from __future__ import annotations

from auto_mixcut.agent.gap_template_planner import GapTemplatePlanner, PlannerConfig


def _planner(templates: list[dict]) -> GapTemplatePlanner:
    planner = object.__new__(GapTemplatePlanner)
    planner.templates = templates
    planner.config = PlannerConfig(
        {
            "gap_and_template": {
                "template_rotation": {"recent_window_clips": 8, "single_template_max_ratio": 0.5},
                "template_candidates": {"recommended_min_candidates": 3, "hard_min_candidates": 2},
                "role_to_grade": {"hero": "A", "result": "A", "detail": "B", "scene": "C", "ending": "C"},
                "role_to_segment_type": {
                    "hero": "product_display",
                    "result": "before_go_out",
                    "detail": "product_display",
                    "scene": "seasonal_scene",
                    "ending": "home_lifestyle",
                },
                "role_to_hook_intent": {
                    "hero": "product_clarity",
                    "result": "tryon_result",
                    "detail": "material_closeup",
                    "scene": "atmosphere",
                    "ending": "atmosphere",
                },
                "reuse": {"max_reuse_per_asset": 3, "reuse_penalty_weight": 1.0, "within_clip_reuse_max": 1},
                "cost": {"unit_price_grade": {"A": 0, "B": 0, "C": 0}, "rerender_redundancy": 1.3, "round_budget_cap": 0},
            }
        }
    )
    return planner


def _template(template_id: str, roles: list[str] | None = None, fallback_only: bool = False) -> dict:
    return {"template_id": template_id, "roles": roles or ["hero", "detail", "result", "scene", "ending"], "fallback_only": fallback_only}


def test_template_distribution_balances_remainder_by_history() -> None:
    planner = _planner([_template("T1"), _template("T2"), _template("T3"), _template("T4"), _template("COLD", fallback_only=True)])
    result = planner.plan_round(
        {"round_id": "R1", "targets": [{"product_id": "P1", "category": "womens_outerwear", "target_clips": 10}]},
        template_history={"P1": ["T1", "T1", "T2"]},
    )
    assert result.success
    plan = {item["template_id"]: item["clip_count"] for item in result.data["template_plans"][0]["template_plan"]}
    assert sorted(plan.values()) == [2, 2, 3, 3]
    assert plan["T3"] == 3
    assert plan["T4"] == 3
    assert "COLD" not in plan


def test_candidate_hard_min_blocks() -> None:
    planner = _planner([_template("T1"), _template("COLD", fallback_only=True)])
    result = planner.plan_round({"round_id": "R1", "targets": [{"product_id": "P1", "category": "womens_outerwear", "target_clips": 1}]})
    assert not result.success
    assert result.error.code == "need_more_templates"


def test_gap_uses_remaining_reuse_not_asset_count_times_cap() -> None:
    planner = _planner([_template("T1", ["hero"]), _template("T2", ["hero"])])
    assets = [
        {"asset_id": "X", "product_id": "P1", "slot_role": "hero", "source_type": "real", "usable_grade": "usable_core", "times_used": 2},
        {"asset_id": "Y", "product_id": "P1", "slot_role": "hero", "source_type": "real", "usable_grade": "usable_core", "times_used": 0},
        {"asset_id": "Z", "product_id": "P1", "slot_role": "hero", "source_type": "real", "usable_grade": "usable_core", "times_used": 3},
    ]
    result = planner.plan_round(
        {"round_id": "R1", "targets": [{"product_id": "P1", "category": "womens_outerwear", "target_clips": 10}]},
        asset_pool=assets,
        diversity_budgets={"P1": {"product_id": "P1", "ai_ratio_cap": 0.5}},
    )
    assert result.success
    hero_gap = result.data["gaps"][0]["gaps"][0]
    assert hero_gap["real_available"] == 4
    assert "Z" in hero_gap["exhausted_assets"]


def test_generation_tasks_equal_ai_gen_gap_and_respect_ai_cap() -> None:
    planner = _planner([_template("T1"), _template("T2"), _template("T3")])
    result = planner.plan_round(
        {"round_id": "R1", "targets": [{"product_id": "P1", "category": "womens_outerwear", "target_clips": 10}]},
        diversity_budgets={"P1": {"product_id": "P1", "ai_ratio_cap": 0.5}},
    )
    assert result.success
    gap_sum = sum(item["ai_gen_gap"] for item in result.data["gaps"][0]["gaps"])
    total_demand = sum(item["count"] for item in result.data["demand"][0]["demand"])
    assert len(result.data["generation_tasks"]) == gap_sum
    assert gap_sum <= round(total_demand * 0.5)
