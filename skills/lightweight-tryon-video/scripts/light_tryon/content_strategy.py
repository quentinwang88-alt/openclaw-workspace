from __future__ import annotations

import random
from dataclasses import replace
from typing import Any, Iterable

from .models import ContentStrategy, NarrativeVariant
from .utils import normalized_list, safe_slug, stable_hash


DEFAULT_PLAN_VERSION = "narrative-v1"


def _number(value: Any, default: float = 1.0) -> float:
    try:
        return max(0.01, float(value))
    except (TypeError, ValueError):
        return default


def _selling_points(values: Iterable[Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in values:
        if isinstance(item, dict):
            name = str(item.get("selling_point") or item.get("name") or item.get("text") or "").strip()
            priority = _number(item.get("priority") or item.get("weight"), 1.0)
            evidence = normalized_list(item.get("required_evidence") or item.get("evidence_tags"))
        else:
            name = str(item or "").strip()
            priority = 1.0
            evidence = []
        if not name or name in seen:
            continue
        seen.add(name)
        result.append({"name": name, "priority": priority, "required_evidence": evidence})
    return result


def _hook_id(hook: dict[str, Any]) -> str:
    return str(hook.get("hook_id") or hook.get("template_id") or hook.get("id") or "").strip()


def _infer_visual_focus(selling_point: str) -> str:
    text = selling_point.lower()
    if any(token in text for token in ("颜色", "色调", "colour", "color", "多色")):
        return "color"
    if any(token in text for token in (
        "面料", "袖口", "领口", "门襟", "口袋", "细节", "材质", "拉链", "按扣", "纽扣",
        "铆钉", "走线", "明线", "调节扣", "内衬", "内里",
    )):
        return "detail"
    if any(token in text for token in ("通勤", "日常", "场景", "出门", "空调房")):
        return "scenario"
    return "fit"


def build_strategy_pool(
    product_id: str,
    hooks: Iterable[dict[str, Any]],
    selling_points: Iterable[Any],
    *,
    available_evidence: Iterable[str] | None = None,
    plan_version: str = DEFAULT_PLAN_VERSION,
) -> list[ContentStrategy]:
    """Build eligible hook/selling-point strategies without usage-count caps.

    Hook records come from the existing hook library. No hook wording is
    synthesized here. Missing evidence lowers the weight because an enhanced
    video may still fill the gap with one supplemental shot.
    """

    product_id = str(product_id or "").strip()
    if not product_id:
        raise ValueError("product_id 不能为空")
    hook_rows = [dict(row) for row in hooks if _hook_id(dict(row))]
    points = _selling_points(selling_points)
    if not hook_rows:
        raise ValueError("钩子候选不能为空；必须来自现有钩子库")
    if not points:
        raise ValueError("商品至少需要一个可验证卖点")
    evidence_set = set(normalized_list(available_evidence))
    strategies: list[ContentStrategy] = []
    for hook in hook_rows:
        hook_id = _hook_id(hook)
        compatible = set(normalized_list(
            hook.get("compatible_selling_points") or hook.get("applicable_selling_points")
        ))
        hook_evidence = normalized_list(hook.get("required_evidence") or hook.get("evidence_tags"))
        for point_index, point in enumerate(points):
            primary = point["name"]
            if compatible and primary not in compatible:
                continue
            inferred_focus = _infer_visual_focus(primary)
            allowed_focuses = set(normalized_list(hook.get("allowed_visual_focuses")))
            if allowed_focuses and inferred_focus not in allowed_focuses:
                continue
            required = list(dict.fromkeys([*hook_evidence, *point["required_evidence"]]))
            evidence_coverage = 1.0 if set(required).issubset(evidence_set) else 0.68
            compatibility = _number(hook.get("product_match_score") or hook.get("compatibility"), 1.0)
            hook_priority = _number(hook.get("priority") or hook.get("weight"), 1.0)
            weight = round(hook_priority * point["priority"] * compatibility * evidence_coverage, 6)
            secondary = [row["name"] for row in points if row["name"] != primary][:2]
            visual_focus = str(hook.get("visual_focus") or inferred_focus).strip()
            strategy_id = "STR_" + stable_hash(product_id, plan_version, hook_id, primary, length=16)
            strategies.append(ContentStrategy(
                strategy_group_id=strategy_id,
                product_id=product_id,
                hook_id=hook_id,
                hook_name=str(hook.get("hook_name") or hook.get("template_name") or hook_id).strip(),
                hook_type=str(hook.get("hook_type") or hook.get("type") or "").strip(),
                primary_selling_point=primary,
                secondary_selling_points=secondary,
                visual_focus=visual_focus,
                required_evidence=required,
                selection_weight=weight,
                plan_version=plan_version,
                source_payload={"hook": hook, "selling_point": point, "evidence_coverage": evidence_coverage},
            ))
    if not strategies:
        raise ValueError("钩子库与商品卖点没有形成可用组合")
    return sorted(strategies, key=lambda row: (-row.selection_weight, row.strategy_group_id))


def _history_count(values: dict[str, Any], key: str) -> int:
    if key in values:
        return int(values.get(key) or 0)
    compact = "".join(str(key or "").lower().split())
    if not compact:
        return 0
    matches = [
        int(count or 0)
        for name, count in values.items()
        if compact in "".join(str(name or "").lower().split())
        or "".join(str(name or "").lower().split()) in compact
    ]
    return max(matches, default=0)


def _memory_weight(strategy: ContentStrategy, historical_usage: dict[str, Any] | None) -> float:
    history = historical_usage or {}
    hook_count = int((history.get("hook_counts") or {}).get(strategy.hook_id) or 0)
    point_count = _history_count(
        history.get("selling_point_counts") or {}, strategy.primary_selling_point
    )
    pair_key = f"{strategy.hook_id}\u241f{strategy.primary_selling_point}"
    pair_count = _history_count(history.get("pair_counts") or {}, pair_key)
    # This is a ranking penalty, never a hard ban: recurring product facts remain
    # usable, while an unused angle naturally rises to the front of the batch.
    return 1.0 / (1.0 + 0.30 * hook_count + 0.25 * point_count + 0.55 * pair_count)


def _coverage_order(
    strategies: list[ContentStrategy],
    limit: int,
    historical_usage: dict[str, Any] | None = None,
) -> list[ContentStrategy]:
    selected: list[ContentStrategy] = []
    remaining = list(strategies)
    used_hooks: set[str] = set()
    used_points: set[str] = set()
    while remaining and len(selected) < limit:
        best = max(
            remaining,
            key=lambda row: (
                row.selection_weight
                * _memory_weight(row, historical_usage)
                * (1.55 if row.hook_id not in used_hooks else 1.0)
                * (1.45 if row.primary_selling_point not in used_points else 1.0),
                row.strategy_group_id,
            ),
        )
        selected.append(best)
        remaining.remove(best)
        used_hooks.add(best.hook_id)
        used_points.add(best.primary_selling_point)
    return selected


def sample_execution_variants(
    strategies: list[ContentStrategy],
    count: int,
    *,
    product_id: str,
    target_duration_seconds: int = 22,
    plan_version: str = DEFAULT_PLAN_VERSION,
    random_seed: str | int | None = None,
    coverage_ratio: float = 0.35,
    historical_usage: dict[str, Any] | None = None,
    production_batch_id: str = "",
) -> list[NarrativeVariant]:
    """Coverage-first sampling with product history and delayed replacement.

    A hook or selling point can be reused, but every eligible hook×selling-point
    strategy gets a chance before an exact strategy is repeated.
    """

    if count < 1 or count > 100:
        raise ValueError("增强型视频数量必须在 1-100 之间")
    if not 18 <= int(target_duration_seconds) <= 24:
        raise ValueError("口播增强型视频时长必须在 18-24 秒之间")
    if not strategies:
        raise ValueError("内容策略池不能为空")
    seed_value = random_seed if random_seed is not None else stable_hash(product_id, plan_version, count)
    rng = random.Random(str(seed_value))
    coverage_count = min(count, len(strategies), max(1, round(count * max(0.0, min(1.0, coverage_ratio)))))
    chosen = _coverage_order(strategies, coverage_count, historical_usage)
    remaining = [row for row in strategies if row not in chosen]
    while len(chosen) < count and remaining:
        weights = [
            max(0.01, row.selection_weight * _memory_weight(row, historical_usage))
            for row in remaining
        ]
        selected = rng.choices(remaining, weights=weights, k=1)[0]
        chosen.append(selected)
        remaining.remove(selected)
    while len(chosen) < count:
        weights = [
            max(0.01, row.selection_weight * _memory_weight(row, historical_usage))
            for row in strategies
        ]
        chosen.append(rng.choices(strategies, weights=weights, k=1)[0])
    rng.shuffle(chosen)

    variants: list[NarrativeVariant] = []
    slug = safe_slug(product_id)
    for index, strategy in enumerate(chosen, start=1):
        execution_seed = stable_hash(product_id, plan_version, index, strategy.strategy_group_id, seed_value, length=16)
        variant_id = f"NAR_{slug}_{index:03d}_{execution_seed[:8]}"
        variants.append(NarrativeVariant(
            variant_id=variant_id,
            strategy_group_id=strategy.strategy_group_id,
            product_id=product_id,
            format_type="enhanced_18_24",
            target_duration_seconds=int(target_duration_seconds),
            variant_no=index,
            execution_seed=execution_seed,
            plan_version=plan_version,
            workflow_state="waiting_assets",
            production_batch_id=str(production_batch_id or ""),
        ))
    return variants


def with_adjusted_weight(strategy: ContentStrategy, multiplier: float) -> ContentStrategy:
    """Apply later performance feedback without mutating the hook library row."""

    return replace(strategy, selection_weight=round(strategy.selection_weight * max(0.01, multiplier), 6))
