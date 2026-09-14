"""Preset-bound native-photo batch planning and cheap pre-generation checks."""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from domain.photo_contracts import normalize_publish_copy
from services.photo_flow_registry import (
    PhotoFlowRegistryError, get_photo_flow_handler, is_layered_progression_flow,
    is_thermal_transition_flow, resolve_required_roles, role_marker,
    validate_ordered_roles,
)


class PhotoContentPlanError(ValueError):
    pass


POLICY_DIR = (
    Path(__file__).resolve().parents[1]
    / "config" / "photo_planning_policies"
)

# Recipe → planning policy registry. Adding a preset means adding its policy
# file here; nothing else in the workflow needs a recipe-id branch.
RECIPE_POLICY_FILES = {
    "PHOTO_TH_PICK_YOUR_LOOK_V3": "TH_PICK_YOUR_LOOK_V2.json",
    "PHOTO_TH_TRAVEL_OUTFIT_V2": "TH_TRAVEL_OUTFIT_V1.json",
    # 2026-09-13: PHOTO_TH_TEMPERATURE_DRESSING_V2 (layering_two_step) was
    # retired — it never reached RDS, and its slot is taken by the daily
    # thermal-transition line.  The 0-15°C job moves to the travel line.
    "PHOTO_TH_THERMAL_TRANSITION_V1": "TH_THERMAL_TRANSITION_V1.json",
    # 2026-09-13 (VN scarf Phase 2): the country-agnostic travel template.  It
    # reuses the exact TH families and executable rules, but every publish label
    # comes from a Locale Pack, so one recipe can serve TH and VN.  Registered
    # here so the canary is plannable; V2 keeps the live route until Phase 4.
    "PHOTO_TRAVEL_OUTFIT_V3": "TRAVEL_OUTFIT_V3.json",
    # 2026-09-13 (VN scarf Phase 4): the daily matching line.  Spec §5.2 names it
    # ``MATCHING_CHOICE_V2``, but that recipe never existed anywhere in the repo;
    # per the pre-kickoff alignment note §5 the country-agnostic version is
    # shipped as PHOTO_MATCHING_CHOICE_V3 next to the travel V3 skeleton.  It is
    # a draft canary: no production preset routes to it until acceptance.
    "PHOTO_MATCHING_CHOICE_V3": "MATCHING_CHOICE_V3.json",
}


def _fingerprint(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest()


def recipe_has_planning_policy(recipe_id: str) -> bool:
    return str(recipe_id) in RECIPE_POLICY_FILES


def get_planning_flow(recipe_id: str) -> str:
    try:
        policy = load_planning_policy(recipe_id)
    except PhotoContentPlanError:
        return ""
    return str(policy.get("planning_flow") or "reference_contract_v1")


def load_planning_policy(recipe_id: str) -> dict[str, Any]:
    filename = RECIPE_POLICY_FILES.get(str(recipe_id))
    if not filename:
        raise PhotoContentPlanError(f"生产预设尚未接入规划规则：{recipe_id}")
    payload = json.loads((POLICY_DIR / filename).read_text(encoding="utf-8"))
    if payload.get("schema_version") == "opv-photo-planning-policy-v2":
        base_path = POLICY_DIR / str(payload.get("base_policy") or "")
        base = json.loads(base_path.read_text(encoding="utf-8"))
        if base.get("schema_version") != "opv-photo-planning-policy-v1":
            raise PhotoContentPlanError("规划策略基础规则版本无效")
        payload = {**base, **payload, "families": base["families"],
                   "recipe_ids": base["recipe_ids"],
                   "supported_reference_modes": base["supported_reference_modes"],
                   "supported_theme_keys": base["supported_theme_keys"],
                   "minimum_cross_post_axis_difference": base["minimum_cross_post_axis_difference"],
                   "theme_family_order": base["theme_family_order"]}
    elif payload.get("schema_version") != "opv-photo-planning-policy-v1":
        raise PhotoContentPlanError("规划策略版本无效")
    return payload


def load_th_choice_policy() -> dict[str, Any]:
    return load_planning_policy("PHOTO_TH_PICK_YOUR_LOOK_V3")


def _family_score(family_id: str, profile: Mapping[str, Any], policy: Mapping[str, Any]) -> int:
    compatibility = dict((policy.get("family_compatibility") or {}).get(family_id) or {})
    weights = dict(policy.get("scoring_weights") or {})
    palette = set(str(value) for value in profile.get("palette") or [])
    family_palette = set(str(value) for value in compatibility.get("palette") or [])
    style = set(str(value) for value in profile.get("style_tags") or [])
    family_style = set(str(value) for value in compatibility.get("style_tags") or [])
    score = 0
    if palette and family_palette:
        score += round(int(weights.get("palette") or 35) * len(palette & family_palette) / len(palette))
    if str(profile.get("season") or "") in compatibility.get("seasons", []):
        score += int(weights.get("season_material") or 25)
    if style and family_style:
        score += round(int(weights.get("style") or 20) * len(style & family_style) / len(style))
    if str(profile.get("temperature") or "") == str(compatibility.get("temperature") or ""):
        score += 10
    # Every existing family satisfies the four-complete-look garment contract.
    score += int(weights.get("garment") or 10)
    score += int(weights.get("presentation") or 10)
    conflicts = dict(policy.get("hard_conflicts") or {})
    if (profile.get("temperature") == "warm"
            and family_id in set(conflicts.get("warm_reference") or [])):
        return -1000
    return score


def _select_families(*, order: Sequence[str], count: int,
                     style_profile: Mapping[str, Any] | None,
                     policy: Mapping[str, Any]) -> list[str]:
    if not style_profile:
        return list(order[:count])
    ranked = sorted(
        enumerate(order), key=lambda item: (-_family_score(item[1], style_profile, policy), item[0])
    )
    selected = [family_id for _position, family_id in ranked if _family_score(family_id, style_profile, policy) >= 0]
    if len(selected) < count:
        raise PhotoContentPlanError("参考图与现有穿搭方案不相容，无法满足本次生成篇数")
    return selected[:count]


def _complete_look_plan(index: int, theme: Mapping[str, Any], policy: Mapping[str, Any],
                        locale_pack: Mapping[str, Any] | None = None) -> dict[str, Any]:
    # ``locale_pack=None`` keeps the legacy inline Thai table, so the TH V2 plan
    # stays byte-identical.  A bound Locale Pack owns the same four variants and
    # the shared CTA, which is what stops a VN COMPLETE_LOOK plan from leaking
    # Thai copy (spec §8.4 "越南语无泰语泄漏").
    neutral_copy = (
        ("วันนี้เลือกชุดไหนดี", "A B C หรือ D?", "วันนี้คุณชอบลุค A B C หรือ D มากที่สุด?"),
        ("4 ลุค เลือกหนึ่งชุด", "เลือกหนึ่งลุค\nA B C หรือ D?", "ถ้าเลือกได้หนึ่งลุค คุณจะเลือก A B C หรือ D?"),
        ("ลุคไหนตรงใจคุณ", "ลุคไหนตรงใจ?\nA B C หรือ D", "ใน 4 ลุคนี้ ชุดไหนตรงใจคุณที่สุด?"),
        ("ช่วยเลือกหนึ่งลุค", "ช่วยเลือกหน่อย\nA B C หรือ D?", "ช่วยเลือกหน่อย วันนี้ควรเป็นลุค A B C หรือ D?"),
    )
    default_cta = "คุณชอบลุคไหน?"
    if locale_pack is None:
        title, cover, caption = neutral_copy[(index - 1) % len(neutral_copy)]
    else:
        from services.photo_locale import complete_look_copy, locale_pack_labels
        variants = complete_look_copy(locale_pack)
        entry = variants[(index - 1) % len(variants)]
        title, cover, caption = entry["title"], entry["cover"], entry["caption"]
        default_cta = str(
            locale_pack_labels(locale_pack, "generic").get("cta") or ""
        )
    return {
        "schema_version": "opv-photo-content-plan-item-v1",
        "index": index, "policy_id": policy["policy_id"],
        "policy_version": policy["policy_version"],
        "family_id": f"complete_look_set_{index}",
        "variation_id": f"complete_look_set_{index}",
        "theme_key": str(theme.get("theme_key") or ""),
        "angle_zh": f"完整穿搭素材第 {index} 组",
        "scene_zh": "沿用上传素材", "palette_zh": "沿用上传素材",
        "background_color": "", "background_prompt": "",
        "style_modifier": "忠实使用上传的四套完整穿搭，不补充图片无法证明的效果",
        "looks": [],
        "copy": {
            "title": title, "cover": cover, "caption": caption,
            "cta": str(theme.get("cta") or default_cta),
        },
        "difference_axes": {"asset_group": index},
    }


def _family_plan(
    *, index: int, family: Mapping[str, Any], theme: Mapping[str, Any],
    policy: Mapping[str, Any], reference_mode: str,
    style_profile: Mapping[str, Any] | None = None,
    locale_pack: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    looks = copy.deepcopy(list(family["looks"]))
    if reference_mode == "PRODUCT":
        for look in looks:
            look["outerwear"] = "目标商品外套；严格保持商品参考图中的款式、颜色、材质和结构"
            look["outerwear_type"] = "target_product"
    # Publish copy is locale-owned.  ``locale_pack=None`` reads the legacy inline
    # ``title_th``/``cover_th``/``caption_th``/``display_label`` fields, so the TH
    # V2 plan is byte-identical; a bound Locale Pack supplies the same strings for
    # the country-agnostic recipe v2 while the policy keeps the garment rules.
    from services.photo_locale import family_copy as resolve_family_copy
    locale_copy = resolve_family_copy(family, locale_pack=locale_pack)
    if locale_pack is not None:
        for look in looks:
            label = str(locale_copy["look_labels"].get(str(look.get("role") or "")) or "")
            if label:
                look["display_label"] = label
    outerwear_types = sorted({str(item.get("outerwear_type") or "") for item in looks})
    bottom_types = sorted({str(item.get("bottom_type") or "") for item in looks})
    return {
        "schema_version": "opv-photo-content-plan-item-v2",
        "index": index, "policy_id": policy["policy_id"],
        "policy_version": policy["policy_version"],
        "family_id": family["family_id"], "variation_id": family["family_id"],
        "theme_key": str(theme.get("theme_key") or ""), "angle_zh": family["angle_zh"],
        "scene_zh": family["scene_zh"], "palette_zh": family["palette_zh"],
        "background_color": family["background_color"],
        "background_prompt": family["background_prompt"],
        "style_modifier": (
            f"本篇严格使用{family['palette_zh']}；A/B/C/D 必须执行各自冻结单品，"
            "并保持参考图的展示方式、色温、光线和背景质感"
            if style_profile else
            f"{str(theme.get('visual_brief') or '')}；本篇严格使用{family['palette_zh']}；"
            "A/B/C/D 必须执行各自冻结单品，不得回退成其他篇的服装"
        ),
        "presentation_type": str((style_profile or {}).get("presentation_type") or "MODEL_FULL_BODY"),
        "style_profile": dict(style_profile or {}),
        "looks": looks,
        "copy": {
            "title": locale_copy["title"], "cover": locale_copy["cover"],
            "caption": locale_copy["caption"], "cta": str(theme.get("cta") or ""),
        },
        "difference_axes": {
            "family": family["family_id"], "palette": family["palette_zh"],
            "outerwear_types": outerwear_types, "bottom_types": bottom_types,
        },
    }


def _neutral_four_choice_copy(
    locale_pack: Mapping[str, Any], *, index: int,
) -> dict[str, str]:
    """Locale-owned publish copy for a **model-planned** four-choice batch.

    ``_vision_plan`` takes its four looks from the vision model (the model is the
    one that actually reads the reference images), but the publish text must not
    come from it: ``PhotoReferenceVisionService._analysis_prompt`` explicitly asks
    for **Thai** copy and ``_normalize_contract`` rejects a set without it, so a
    VN run would otherwise publish Thai.  A model-planned batch has no policy
    ``family_id`` to key ``family_copy`` on, so the locale pack's four neutral
    variants (the same ones ``COMPLETE_LOOK`` uses) are the right owner here.
    """
    from services.photo_locale import complete_look_copy, locale_pack_labels
    variants = list(complete_look_copy(locale_pack))
    if not variants:
        raise PhotoContentPlanError("locale pack 未提供四选一通用文案")
    entry = dict(variants[(index - 1) % len(variants)])
    labels = locale_pack_labels(locale_pack, "generic")
    return {
        "title": str(entry.get("title") or labels.get("title") or ""),
        "cover": str(entry.get("cover") or labels.get("cover") or ""),
        "caption": str(entry.get("caption") or ""),
        "cta": str(labels.get("cta") or ""),
    }


def _vision_plan(
    *, index: int, recommendation: Mapping[str, Any], theme: Mapping[str, Any],
    policy: Mapping[str, Any], style_profile: Mapping[str, Any],
    locale_pack: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Convert a model-reviewed semantic recommendation into the frozen plan."""
    theme = dict(theme or {})
    value = copy.deepcopy(dict(recommendation))
    looks = list(value.get("looks") or [])
    if locale_pack is not None:
        from services.photo_locale import locale_pack_labels
        # 模型给的 look 标签是泰语（prompt 明写「简短自然泰语标签」），绑定了
        # Locale Pack 时换成该语言的通用标签，避免越南语稿件夹泰语标签（2026-09-14）。
        look_label = str(
            locale_pack_labels(locale_pack, "generic").get("look_label") or "Look {letter}"
        )
        for position, look in enumerate(looks):
            look["display_label"] = look_label.format(
                letter=role_marker(str(look.get("role") or ""), position)
            )
    outerwear_types = sorted({str(item.get("outerwear_type") or item.get("outerwear") or "") for item in looks})
    bottom_types = sorted({str(item.get("bottom_type") or item.get("bottom") or "") for item in looks})
    aggregate = dict(style_profile.get("aggregate") or {})
    return {
        "schema_version": "opv-photo-content-plan-item-v3",
        "index": index, "policy_id": policy["policy_id"],
        "policy_version": policy["policy_version"],
        "family_id": f"vision_dynamic_{index}",
        "variation_id": f"vision_dynamic_{index}",
        "theme_key": str(theme.get("theme_key") or ""),
        "angle_zh": str(value.get("content_angle_zh") or f"参考风格穿搭第 {index} 组"),
        "scene_zh": str(value.get("scene_zh") or "沿用参考图场景语义"),
        "palette_zh": str(value.get("palette_zh") or "、".join(style_profile.get("palette") or [])),
        "background_color": "",
        "background_prompt": str(value.get("background_prompt") or aggregate.get("background") or ""),
        "style_modifier": str(value.get("style_modifier") or "忠实呈现参考图的风格、场景、配色和服装语言"),
        "presentation_type": str(style_profile.get("presentation_type") or "MODEL_FULL_BODY"),
        "style_profile": dict(style_profile),
        "looks": looks,
        # 发布文案归 Locale Pack（见 ``_neutral_four_choice_copy``）：视觉模型只负责
        # 四套 look，文案不能让模型写——它的 prompt 明确要求泰语（2026-09-14）。
        "copy": (
            _neutral_four_choice_copy(locale_pack, index=index)
            if locale_pack is not None
            else dict(value.get("copy") or {})
        ),
        "difference_axes": {
            "family": f"vision_dynamic_{index}",
            "palette": str(value.get("palette_zh") or ""),
            "outerwear_types": outerwear_types, "bottom_types": bottom_types,
            "look_set": _fingerprint([{
                key: look.get(key) for key in ("outerwear", "top_inner", "bottom", "shoes")
            } for look in looks]),
        },
    }


def validate_batch_plan(plan: Mapping[str, Any]) -> None:
    items = list(plan.get("items") or [])
    if len(items) != int(plan.get("count") or 0) or not items:
        raise PhotoContentPlanError("批次内容计划数量不完整")
    reference_mode = str(plan.get("reference_mode") or "")
    style_profile = dict(plan.get("style_profile") or {})
    planning_flow = str(
        plan.get("planning_flow") or style_profile.get("planning_flow") or ""
    )
    try:
        required_roles = resolve_required_roles(
            planning_flow=planning_flow,
            required_roles=plan.get("required_roles") or (),
        )
    except PhotoFlowRegistryError as exc:
        raise PhotoContentPlanError(str(exc)) from exc
    seen_families, seen_looks = set(), set()
    for expected, item in enumerate(items, 1):
        if item.get("index") != expected or not item.get("angle_zh") or not item.get("copy"):
            raise PhotoContentPlanError("批次内容计划条目不完整")
        if reference_mode == "COMPLETE_LOOK":
            continue
        looks = list(item.get("looks") or [])
        try:
            validate_ordered_roles(
                looks, planning_flow=planning_flow, required_roles=required_roles,
            )
        except PhotoFlowRegistryError as exc:
            raise PhotoContentPlanError(f"第 {expected} 篇{exc}") from exc
        travel_moments = [str(look.get("travel_moment") or "") for look in looks]
        if looks[0].get("travel_moment") is not None:
            if any(not moment for moment in travel_moments):
                raise PhotoContentPlanError(f"第 {expected} 篇存在缺少旅行场景的 Look")
            # 主题联动分支允许四页共用同一 travel_moment（scene_prompt 区分）。
            if (not plan.get("allow_repeated_travel_moments")
                    and len(set(travel_moments)) != len(travel_moments)):
                raise PhotoContentPlanError(f"第 {expected} 篇旅行场景重复，四页必须对应不同场景")
        local = set()
        for look in looks:
            signature = _fingerprint({
                key: look.get(key) for key in
                ("outerwear", "top_inner", "bottom", "shoes")
            })
            if signature in local:
                raise PhotoContentPlanError(f"第 {expected} 篇存在重复 Look")
            local.add(signature)
        family = str(item.get("family_id") or "")
        if reference_mode == "STYLE" and style_profile:
            if item.get("presentation_type") != style_profile.get("presentation_type"):
                raise PhotoContentPlanError("内容方案的展示方式与参考图冲突")
            expected_temperature = str(style_profile.get("temperature") or "")
            family_palette = str(item.get("palette_zh") or "")
            if expected_temperature == "warm" and family in {"monochrome", "sporty_layer"}:
                raise PhotoContentPlanError(
                    f"参考图为暖色方向，但第 {expected} 篇选择了不相容方案 {family_palette}"
                )
        if family in seen_families:
            raise PhotoContentPlanError("批次重复使用同一穿搭方案族")
        seen_families.add(family)
        overlap = seen_looks.intersection(local)
        if overlap:
            raise PhotoContentPlanError("不同篇之间存在完全相同的具体穿搭")
        seen_looks.update(local)
    for left_index, left in enumerate(items):
        for right in items[left_index + 1:]:
            if reference_mode == "COMPLETE_LOOK":
                continue
            a, b = left.get("difference_axes") or {}, right.get("difference_axes") or {}
            difference_count = sum(a.get(key) != b.get(key) for key in (
                "family", "palette", "outerwear_types", "bottom_types", "look_set"
            ))
            if difference_count < int(plan.get("minimum_cross_post_axis_difference") or 2):
                raise PhotoContentPlanError("不同篇的有效差异维度不足")


def _travel_template_copy(
    *, record_id: str, index: int, templates: Sequence[Mapping[str, Any]],
    travel_contract: Mapping[str, Any], variables: Mapping[str, Any],
    locale_pack: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Travel copy comes only from audited templates with variable filling;
    the vision model never writes free-form Thai in phase 1."""
    from services.photo_copy import fill_travel_copy_tokens
    if not templates:
        raise PhotoContentPlanError("旅行预设缺少已审核文案模板")
    start = int.from_bytes(hashlib.sha256(
        f"{record_id}:travel_copy:{index}".encode("utf-8")
    ).digest()[:4], "big") % len(templates)
    variant = templates[start]
    copy_block = dict(variant.get("copy") or {})

    def fill(value):
        if isinstance(value, str):
            return fill_travel_copy_tokens(
                value, travel_contract=travel_contract, variables=variables,
                locale_pack=locale_pack)
        if isinstance(value, list):
            return [fill(item) for item in value]
        return value

    slides = [fill(item) for item in copy_block.get("slide_texts") or []]
    return {
        "title": fill(copy_block.get("title") or ""),
        "cover": slides[0] if slides else "",
        "caption": fill(copy_block.get("caption") or ""),
        "cta": str(slides[-1]).split("\n")[-1] if slides else "",
        "hashtags": list(copy_block.get("hashtags") or []),
        "slide_texts": slides,
        "language_review_status": str(copy_block.get("language_review_status") or "DRAFT"),
    }


def plan_th_choice_batch(
    *, record_id: str, recipe_id: str, theme: Mapping[str, Any],
    reference_mode: str, count: int, style_profile: Mapping[str, Any] | None = None,
    travel_contract: Mapping[str, Any] = None,
    copy_templates: Sequence[Mapping[str, Any]] = None,
    required_roles: Sequence[str] = (),
    recipe_spec: Mapping[str, Any] = None,
    variables: Mapping[str, Any] = None,
    locale_pack: Mapping[str, Any] = None,
) -> dict[str, Any]:
    policy = load_planning_policy(recipe_id)
    if recipe_id not in policy["recipe_ids"]:
        raise PhotoContentPlanError(f"生产预设尚未接入规划规则：{recipe_id}")
    if reference_mode not in policy["supported_reference_modes"]:
        raise PhotoContentPlanError(
            f"该生产预设不支持参考模式：{reference_mode or '未选择'}"
            f"（支持：{'、'.join(policy['supported_reference_modes'])}）"
        )
    theme_key = str((theme or {}).get("theme_key") or "")
    if theme_key not in policy["supported_theme_keys"]:
        # 国家无关配方（声明了 ``locale_copy_packs`` ⇒ 调用方会传入 ``locale_pack``）
        # 的发布文案与家族文案都归 Locale Pack 所有，主题只影响家族排序；而方案
        # §5.4 的运营输入只有「参考图类型 / 参考图 / 产品编码」，没有「图文主题」。
        # 因此未选主题（theme_key 为空）不得拦在这里（2026-09-14）。
        if not (locale_pack is not None and not theme_key):
            raise PhotoContentPlanError(f"该生产预设尚未支持主题：{theme_key or '自动'}")
    travel_topic = dict((style_profile or {}).get("travel_topic") or {})
    topic_linked = False
    planning_flow = str(
        (style_profile or {}).get("planning_flow")
        or policy.get("planning_flow")
        or "reference_contract_v1"
    )
    try:
        flow_handler = get_photo_flow_handler(planning_flow)
        frozen_required_roles = flow_handler.resolve_source_roles(required_roles)
    except PhotoFlowRegistryError as exc:
        raise PhotoContentPlanError(str(exc)) from exc
    if count < 1 or count > 9:
        raise PhotoContentPlanError("生成篇数必须是 1 到 9")
    if is_layered_progression_flow(planning_flow):
        if count != 1:
            raise PhotoContentPlanError("分层图文首版每条记录只生成一篇")
        if is_thermal_transition_flow(planning_flow):
            from services.photo_thermal_transition_flow import (
                PhotoThermalTransitionFlowError,
                build_thermal_transition_content_plan,
            )
            try:
                return build_thermal_transition_content_plan(
                    record_id=record_id, recipe_id=recipe_id,
                    recipe_spec=dict(recipe_spec or {}), policy=policy,
                    theme=theme, reference_mode=reference_mode,
                    variables=dict(variables or {}),
                    copy_templates=copy_templates or (),
                )
            except PhotoThermalTransitionFlowError as exc:
                raise PhotoContentPlanError(str(exc)) from exc
        from services.photo_layering_flow import (
            PhotoLayeringFlowError, build_layering_content_plan,
        )
        try:
            return build_layering_content_plan(
                record_id=record_id, recipe_id=recipe_id,
                recipe_spec=dict(recipe_spec or {}), policy=policy,
                theme=theme, reference_mode=reference_mode,
                variables=dict(variables or {}),
                copy_templates=copy_templates or (),
            )
        except PhotoLayeringFlowError as exc:
            raise PhotoContentPlanError(str(exc)) from exc
    if reference_mode == "COMPLETE_LOOK":
        items = [
            _complete_look_plan(index, theme, policy, locale_pack=locale_pack)
            for index in range(1, count + 1)
        ]
    elif (reference_mode == "STYLE" and style_profile
          and style_profile.get("analysis_method") == "doubao_seed_2_1"):
        recommendations = list(style_profile.get("recommended_sets") or [])
        if len(recommendations) < count:
            raise PhotoContentPlanError("视觉合同没有提供足够的穿搭方案")
        # A generic vision contract may still be consumed by the travel
        # recipe (legacy/test fixtures).  Travel-specific copy is activated
        # only when the upstream visual plan explicitly declares that flow.
        # The lookup is registry-driven while preserving that old behavior.
        try:
            profile_flow_handler = get_photo_flow_handler(
                style_profile.get("planning_flow") or ""
            )
        except PhotoFlowRegistryError as exc:
            raise PhotoContentPlanError(str(exc)) from exc
        travel_flow = profile_flow_handler.travel_semantics
        travel_topic = dict(style_profile.get("travel_topic") or {})
        topic_linked = bool(travel_flow and travel_topic.get("theme_type"))
        if travel_flow and not topic_linked and (
                not travel_contract or not copy_templates):
            raise PhotoContentPlanError("旅行两步规划缺少 travel_contract 或审核文案模板")
        items = []
        for index, value in enumerate(recommendations[:count], 1):
            item = _vision_plan(index=index, recommendation=value, theme=theme,
                                policy=policy, style_profile=style_profile,
                                # 作用域（2026-09-14）：旅行流程的文案与 look 标签由旅行
                                # 合同 / 主题体系拥有（且 TH 旅行线已在生产交付），不得被
                                # 这里的 Locale Pack 覆盖。因此仅**非旅行**的 STYLE 线
                                # （如 VN 围巾搭配线，planning_flow=reference_contract_v1）
                                # 才接手 Locale Pack 的文案与标签。
                                locale_pack=None if travel_flow else locale_pack)
            if travel_flow and topic_linked:
                # 主题联动分支：规划响应同时产出选题与发布文案，直接冻结。
                # 模型自由撰写的 title/caption 必须在这里就夹进发布契约——这是
                # 机器文案的唯一入口。不加这一步，超限要等到封版校验（发生在
                # 付费素材生成之后）才暴露，等于白烧一轮生图。人写的审核模板
                # 走 template_fill 分支，保持严格校验不被夹。
                model_copy = normalize_publish_copy(dict(value.get("copy") or {}))
                item["copy"] = {
                    "copy_policy_version": 2,
                    "place_localized": str(model_copy.get("place_localized") or ""),
                    "title": str(model_copy.get("title") or ""),
                    "caption": str(model_copy.get("caption") or ""),
                    "hashtags": [str(v) for v in model_copy.get("hashtags") or []],
                    "slide_texts": [str(v) for v in model_copy.get("slide_texts") or []],
                    "language_review_status": "DRAFT_TRAVEL_TOPIC",
                }
                item["copy_source"] = "travel_topic_model"
                item["topic_zh"] = str(value.get("topic_zh") or "")
                item["template_review_status"] = "DRAFT_TRAVEL_TOPIC"
            elif travel_flow:
                template_copy = _travel_template_copy(
                    record_id=record_id, index=index, templates=copy_templates,
                    travel_contract=travel_contract,
                    variables=style_profile.get("travel_variables") or {},
                    locale_pack=locale_pack,
                )
                item["copy"] = template_copy
                item["copy_source"] = "template_fill"
                item["template_review_status"] = str(
                    template_copy.get("language_review_status") or "DRAFT")
            items.append(item)
    else:
        by_id = {item["family_id"]: item for item in policy["families"]}
        order = list(policy["theme_family_order"].get(theme_key) or [])
        if not order:
            # 未选主题（theme_key 为空）且本次没走到视觉模型方案分支时，方案族顺序
            # 无从确定。给明确错误，而不是让它以 KeyError 的形式暴露（2026-09-14）。
            raise PhotoContentPlanError(
                "该生产预设未选图文主题，且本次参考模式没有提供可用的穿搭方案"
            )
        if reference_mode == "STYLE" and style_profile:
            selected_ids = _select_families(
                order=order, count=count, style_profile=style_profile, policy=policy,
            )
        else:
            start = int.from_bytes(hashlib.sha256(
                f"{record_id}:{recipe_id}:{theme_key}:{reference_mode}".encode("utf-8")
            ).digest()[:4], "big") % len(order)
            selected_ids = [order[(start + offset) % len(order)] for offset in range(count)]
        selected = [by_id[family_id] for family_id in selected_ids]
        items = [
            _family_plan(index=index, family=family, theme=theme, policy=policy,
                         reference_mode=reference_mode, style_profile=style_profile,
                         locale_pack=locale_pack)
            for index, family in enumerate(selected, 1)
        ]
    for item in items:
        item.setdefault("planning_flow", planning_flow)
        item.setdefault("required_roles", list(frozen_required_roles))
    plan = {
        "schema_version": "opv-photo-content-plan-v1",
        "policy_id": policy["policy_id"], "policy_version": policy["policy_version"],
        "record_id": record_id, "recipe_id": recipe_id,
        "theme_key": theme_key, "reference_mode": reference_mode, "count": count,
        "planning_flow": planning_flow,
        "required_roles": list(frozen_required_roles),
        "travel_theme_type": travel_topic.get("theme_type") or "",
        "travel_place": str(travel_topic.get("place") or ""),
        "allow_repeated_travel_moments": topic_linked,
        "style_profile": dict(style_profile or {}),
        "minimum_cross_post_axis_difference": policy["minimum_cross_post_axis_difference"],
        "items": items,
    }
    validate_batch_plan(plan)
    plan["plan_sha256"] = _fingerprint(plan)
    return plan


def summarize_batch_plan(plan: Mapping[str, Any]) -> str:
    lines = []
    style_profile = dict(plan.get("style_profile") or {})
    reference_parts = []
    for key, label in (("environment_reference", "环境"),
                       ("outfit_reference", "穿搭"),
                       ("visual_style_reference", "画面风格")):
        indices = list((style_profile.get(key) or {}).get("indices") or [])
        if indices:
            reference_parts.append(f"{label}=图{'、'.join(str(v) for v in indices)}")
    if reference_parts:
        lines.append("参考用途：" + "；".join(reference_parts))
    theme_type = str(plan.get("travel_theme_type") or "")
    place = str(plan.get("travel_place") or "")
    if theme_type:
        header = f"主题联动：{theme_type}｜地点：{place or '未指定（参考图目的地氛围）'}"
        lines.append(header)
    for item in plan.get("items") or []:
        looks = list(item.get("looks") or [])
        look_text = "；".join(
            f"{role_marker(look.get('role'), index)}="
            f"{look.get('outerwear')} + {look.get('bottom')}"
            for index, look in enumerate(looks)
        ) if looks else "按上传的冻结角色使用完整穿搭"
        topic = str(item.get("topic_zh") or "")
        topic_text = f"｜选题：{topic}" if topic else ""
        cover_role = str((item.get("cover_selection") or {}).get("role") or "")
        cover_text = f"｜封面：{role_marker(cover_role, 0)}" if cover_role else ""
        lines.append(
            f"{item['index']}. {item['angle_zh']}｜配色：{item['palette_zh']}"
            f"{topic_text}{cover_text}｜{look_text}"
        )
    return "\n".join(lines)


class PhotoContentPlanStore:
    """Freeze a paid-generation plan before the first image model call.

    The production scanner runs on one primary machine today, so an atomic
    local manifest is the smallest reliable persistence layer.  The exact
    plan is also copied into the later RDS production-batch manifest.
    """

    def __init__(self, root: Path):
        self.root = Path(root)

    def load_or_create(
        self, *, record_id: str, input_contract: Mapping[str, Any],
        create: Any,
    ) -> dict[str, Any]:
        folder = self.root / "content_plans" / self._safe(record_id)
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / "plan.json"
        input_sha256 = _fingerprint(dict(input_contract))
        if path.is_file():
            payload = json.loads(path.read_text(encoding="utf-8"))
            if payload.get("input_sha256") != input_sha256:
                raise PhotoContentPlanError(
                    "该任务的主题、预设、参考模式或生成数量已变化；请新建任务，避免混用旧内容计划"
                )
            plan = dict(payload.get("plan") or {})
            validate_batch_plan(plan)
            if plan.get("plan_sha256") != _fingerprint({
                key: value for key, value in plan.items() if key != "plan_sha256"
            }):
                raise PhotoContentPlanError("冻结内容计划校验失败")
            return plan
        plan = dict(create())
        validate_batch_plan(plan)
        payload = {
            "schema_version": "opv-photo-content-plan-store-v1",
            "record_id": record_id,
            "input_sha256": input_sha256,
            "input_contract": dict(input_contract),
            "plan": plan,
        }
        temporary = path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        temporary.replace(path)
        return plan

    @staticmethod
    def _safe(value: str) -> str:
        return "".join(
            ch if ch.isalnum() or ch in "-_" else "_" for ch in str(value)
        )[:120]
