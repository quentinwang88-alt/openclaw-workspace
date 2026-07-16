from __future__ import annotations

import math
from collections import Counter
from typing import Any, Iterable

from .database import LightTryonDB
from .models import PlannedJob
from .shot_profiles import FULL_FIXED, UPPER_FIXED, UPPER_PUSH_IN, UPPER_THREE_QUARTER, shot_profile_sequence
from .utils import category_matches, normalized_list, safe_slug, stable_hash


RISK_ORDER = {"low": 0, "medium": 1, "high": 2}
UPPER_GARMENT_CATEGORIES = {"top", "tshirt", "tank_top", "knit_top", "shirt", "outerwear", "上装", "上衣", "外套", "t恤", "背心", "吊带", "针织", "针织衫", "衬衫"}


def _filter_pool(
    rows: list[dict[str, Any]], id_key: str, preferred: Iterable[str], *, label: str
) -> list[dict[str, Any]]:
    wanted = normalized_list(preferred)
    if not wanted:
        return rows
    by_id = {row[id_key]: row for row in rows}
    missing = [item for item in wanted if item not in by_id]
    if missing:
        raise ValueError(f"指定的{label}不存在或未启用: {', '.join(missing)}")
    return [by_id[item] for item in wanted]


def weighted_scene_sequence(scenes: list[dict[str, Any]], count: int) -> list[dict[str, Any]]:
    """按最大余数法分配环境；镜头景别由独立 shot profile 决定。"""
    if not scenes:
        raise ValueError("没有可用场景模板")
    if count <= 0:
        return []
    total_weight = sum(max(0, int(row.get("usage_ratio") or 0)) for row in scenes)
    if total_weight <= 0:
        total_weight = len(scenes)
        weights = [1 for _ in scenes]
    else:
        weights = [max(0, int(row.get("usage_ratio") or 0)) for row in scenes]
    quotas = [count * weight / total_weight for weight in weights]
    allocations = [math.floor(value) for value in quotas]
    remaining = count - sum(allocations)
    remainder_order = sorted(
        range(len(scenes)),
        key=lambda index: (quotas[index] - allocations[index], int(scenes[index].get("priority") or 0), -index),
        reverse=True,
    )
    for index in remainder_order[:remaining]:
        allocations[index] += 1

    positive_indexes = [index for index, weight in enumerate(weights) if weight > 0]
    if count >= len(positive_indexes):
        for empty_index in [index for index in positive_indexes if allocations[index] == 0]:
            donor_index = max(
                (index for index in positive_indexes if allocations[index] > 1),
                key=lambda index: (allocations[index], weights[index], int(scenes[index].get("priority") or 0)),
            )
            allocations[donor_index] -= 1
            allocations[empty_index] += 1

    # 主场景先出现；非主场景尽量放在后半程，方便前几条保持稳定。
    result: list[dict[str, Any]] = []
    remaining_counts = allocations[:]
    while len(result) < count:
        progress = len(result) / count
        best_index = max(
            (index for index, value in enumerate(remaining_counts) if value > 0),
            key=lambda index: (
                remaining_counts[index] / max(1, allocations[index]),
                int(scenes[index].get("usage_ratio") or 0) if progress < 0.5 else -len(result),
                int(scenes[index].get("priority") or 0),
            ),
        )
        result.append(scenes[best_index])
        remaining_counts[best_index] -= 1
    # 多环境时保持高权重环境优先，便于首条稳定。
    if count == 4 and len(scenes) >= 3:
        result.sort(key=lambda row: (-int(row.get("usage_ratio") or 0), -int(row.get("priority") or 0)))
    return result


def _scene_sequence_for_product(
    scenes: list[dict[str, Any]], category: str, count: int
) -> list[dict[str, Any]]:
    applicable = [row for row in scenes if category_matches(category, row.get("applicable_categories"))]
    if not applicable:
        applicable = scenes
    return weighted_scene_sequence(applicable, count)


def _select_action(
    scene: dict[str, Any],
    shot_profile_id: str,
    actions: list[dict[str, Any]],
    category: str,
    used: Counter[str],
) -> dict[str, Any]:
    applicable = [row for row in actions if category_matches(category, row.get("applicable_categories"))]
    if not applicable:
        raise ValueError(f"没有适用于类目 {category} 的启用动作模板")
    scene_id = str(scene.get("scene_id") or "")
    compatible_profiles = {shot_profile_id}
    if shot_profile_id == UPPER_THREE_QUARTER:
        compatible_profiles.add(UPPER_FIXED)
    compatible = []
    for row in applicable:
        scene_scope = set(normalized_list(row.get("applicable_scenes")))
        shot_scope = set(normalized_list(row.get("applicable_shot_profiles")))
        if scene_scope and scene_id not in scene_scope:
            continue
        if shot_scope and not (compatible_profiles & shot_scope):
            continue
        compatible.append(row)
    if not compatible:
        raise ValueError(f"类目 {category}、场景 {scene_id}、镜头 {shot_profile_id} 没有兼容的动作模板")
    return min(
        compatible,
        key=lambda row: (
            0 if normalized_list(row.get("applicable_shot_profiles")) or normalized_list(row.get("applicable_scenes")) else 1,
            used[row["action_id"]],
            -int(row.get("priority") or 0),
            RISK_ORDER.get(str(row.get("risk_level")), 9),
            row["action_id"],
        ),
    )


def _select_shot_plan(db: LightTryonDB, product: dict[str, Any], category: str) -> dict[str, Any] | None:
    requested = str(product.get("shot_plan_id") or "").strip()
    rows = db.list_templates("shot_plan")
    if requested:
        matched = [row for row in rows if row["shot_plan_id"] == requested and row.get("status") in {"enabled", "testing"}]
        if not matched:
            raise ValueError(f"指定的镜头方案不存在、已停用或不可测试: {requested}")
        if not category_matches(category, matched[0].get("applicable_categories")):
            raise ValueError(f"镜头方案 {requested} 不适用于类目 {category}")
        return matched[0]
    enabled = [
        row for row in rows
        if row.get("status") == "enabled" and category_matches(category, row.get("applicable_categories"))
    ]
    return enabled[0] if enabled else None


def _action_pool(db: LightTryonDB, requested: Iterable[str]) -> list[dict[str, Any]]:
    wanted = normalized_list(requested)
    if not wanted:
        return db.list_templates("action", "enabled")
    rows = [row for row in db.list_templates("action") if row.get("status") in {"enabled", "testing"}]
    return _filter_pool(rows, "action_id", wanted, label="动作")


def _select_styling(styles: list[dict[str, Any]], category: str, used: Counter[str]) -> dict[str, Any]:
    applicable = [row for row in styles if category_matches(category, row.get("applicable_product_type"))]
    if not applicable:
        # 对无法识别的上装类优先日常牛仔裤；完整套装/连衣裙不得被拆错。
        fallback_id = "STYLE_006" if str(category).lower() in {"set", "matching_set", "dress", "jumpsuit", "套装", "连衣裙", "连体裤"} else "STYLE_002"
        applicable = [row for row in styles if row["styling_id"] == fallback_id] or styles
    order = {row["styling_id"]: index for index, row in enumerate(styles)}
    return min(applicable, key=lambda row: (used[row["styling_id"]], order.get(row["styling_id"], 999), row["styling_id"]))


def _select_subtitle(
    subtitles: list[dict[str, Any]],
    market: str,
    language: str,
    category: str,
    angles: list[str],
    used: Counter[str],
) -> dict[str, Any]:
    applicable = [row for row in subtitles if category_matches(category, row.get("applicable_category"))]
    angle_rank: dict[str, int] = {}
    if angles:
        angle_set = {item.lower() for item in angles}
        for row in applicable:
            row_id = row["subtitle_id"].lower()
            style = str(row.get("subtitle_style") or "").lower()
            ranks = [index for index, angle in enumerate(angles) if angle.lower() in {row_id, style}]
            if ranks:
                angle_rank[row["subtitle_id"]] = min(ranks)
        preferred = [
            row for row in applicable
            if row["subtitle_id"].lower() in angle_set or str(row.get("subtitle_style") or "").lower() in angle_set
        ]
        if preferred:
            applicable = preferred
    exact = [
        row for row in applicable
        if str(row.get("language") or "").lower() == language.lower()
        and str(row.get("market") or "").upper() in {market.upper(), "GLOBAL", "ALL"}
    ]
    language_only = [row for row in applicable if str(row.get("language") or "").lower() == language.lower()]
    candidates = exact or language_only
    if not candidates:
        raise ValueError(
            f"没有适用于 market={market}, language={language}, category={category} 的字幕模板；"
            "请先增加对应语言模板，系统不会把中文字幕静默用于其他市场。"
        )
    return min(candidates, key=lambda row: (used[row["subtitle_id"]], angle_rank.get(row["subtitle_id"], 999), row["subtitle_id"]))


def plan_product(
    db: LightTryonDB,
    product_id: str,
    *,
    count: int | None = None,
    plan_version: str = "v1",
    scene_ids: Iterable[str] | None = None,
    styling_ids: Iterable[str] | None = None,
) -> list[PlannedJob]:
    product = db.get_product(product_id)
    if not product:
        raise KeyError(f"找不到商品: {product_id}")
    if not bool(product.get("enable_light_video", 1)):
        raise ValueError(f"商品 {product_id} 未启用轻量视频")
    target_count = int(count or product.get("target_publish_count") or 4)
    if target_count < 1 or target_count > 100:
        raise ValueError("任务数量必须在 1-100 之间")

    scenes = _filter_pool(db.list_templates("scene", "enabled"), "scene_id", scene_ids if scene_ids is not None else product.get("recommended_scene_pool") or [], label="场景环境")
    actions = _action_pool(db, product.get("recommended_action_pool") or [])
    styles = _filter_pool(db.list_templates("styling", "enabled"), "styling_id", styling_ids if styling_ids is not None else product.get("recommended_styling_pool") or [], label="搭配")
    subtitles = db.list_templates("subtitle", "enabled")
    personas = db.list_templates("persona", "enabled")
    durations = db.list_templates("duration", "enabled")
    if not actions or not styles or not subtitles or not personas or not durations:
        raise ValueError("模板底座不完整：动作、搭配、字幕、人设、时长都必须至少启用一条")

    category = str(product.get("category") or "top")
    shot_plan = _select_shot_plan(db, product, category)
    scene_sequence = _scene_sequence_for_product(scenes, category, target_count)
    shot_sequence = shot_profile_sequence(category, target_count, shot_plan)
    action_usage: Counter[str] = Counter()
    styling_usage: Counter[str] = Counter()
    subtitle_usage: Counter[str] = Counter()
    default_persona_id = str(product.get("default_persona_id") or "").strip()
    if default_persona_id:
        matched = [row for row in personas if row["persona_id"] == default_persona_id]
        if not matched:
            raise ValueError(f"商品默认视觉身份不可用或未启用: {default_persona_id}")
        persona = matched[0]
    else:
        persona = sorted(personas, key=lambda row: (-int(row.get("priority") or 0), row["persona_id"]))[0]
    durations = sorted(durations, key=lambda row: (int(row.get("seconds") or 0), row["duration_id"]))
    result: list[PlannedJob] = []
    priority_base = {"high": 300, "medium": 200, "low": 100}.get(str(product.get("generation_priority") or "medium").lower(), 200)

    for index, (scene, shot_profile_id) in enumerate(zip(scene_sequence, shot_sequence), start=1):
        action = _select_action(scene, shot_profile_id, actions, category, action_usage)
        styling = _select_styling(styles, category, styling_usage)
        subtitle = _select_subtitle(
            subtitles,
            str(product.get("market") or "TH"),
            str(product.get("language") or "th"),
            category,
            normalized_list(product.get("subtitle_angle_pool")),
            subtitle_usage,
        )
        duration = int(durations[(index - 1) % len(durations)]["seconds"])
        action_usage[action["action_id"]] += 1
        styling_usage[styling["styling_id"]] += 1
        subtitle_usage[subtitle["subtitle_id"]] += 1
        identity = stable_hash(
            product_id,
            plan_version,
            index,
            scene["scene_id"],
            shot_profile_id,
            action["action_id"],
            styling["styling_id"],
            subtitle["subtitle_id"],
            duration,
            length=8,
        )
        job_id = f"LTV_{safe_slug(product_id)}_{index:02d}_{identity}"
        result.append(
            PlannedJob(
                job_id=job_id,
                product_id=product_id,
                market=str(product.get("market") or "TH"),
                language=str(product.get("language") or "th"),
                persona_id=persona["persona_id"],
                scene_id=scene["scene_id"],
                shot_profile_id=shot_profile_id,
                shot_plan_id=str((shot_plan or {}).get("shot_plan_id") or ""),
                action_id=action["action_id"],
                styling_id=styling["styling_id"],
                subtitle_id=subtitle["subtitle_id"],
                duration_seconds=duration,
                variant_no=index,
                publish_priority=priority_base + max(1, target_count - index + 1),
                plan_version=plan_version,
                account_id=str(product.get("account_id") or ""),
            )
        )
    return result
