"""Simplified complete-script path for original batch production.

This module intentionally keeps the path thin:

1. freeze product truth + one creative direction;
2. ask one model for the complete visual script;
3. reuse the central voiceover engine;
4. mount the result without another semantic rewrite.

The legacy reality-reference path remains available and unchanged.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import copy
from typing import Any, Dict, Iterable, List, Tuple


SCRIPT_MODE_LEGACY = "legacy_v2"
SCRIPT_MODE_SIMPLIFIED = "simplified_v1"
CREATIVE_SEED_SCHEMA_VERSION = "simplified-creative-seed-v25-apparel-action-soft-match"
VISUAL_SCRIPT_SCHEMA_VERSION = "simplified-complete-visual-script-v11-structure-visible-clips"
VALIDATION_POLICY_VERSION = "simplified-minimum-gates-v5-action-soft-signal"
VIDEO_BRIEF_SCHEMA_VERSION = "production-video-brief-v11-semantic-context"
VIDEO_RENDER_PROFILE = "UGC_NATIVE_V2_MULTICLIP"

CAPTURE_MODE_CREATOR_SELF_SHOT = "CREATOR_SELF_SHOT"
CAPTURE_MODE_HANDS_PRODUCT_SHARE = "HANDS_PRODUCT_SHARE"
CAPTURE_MODE_STATIC_PRODUCT_RECORD = "STATIC_PRODUCT_RECORD"

CAPTURE_RHYTHM_PROFILE_ENV = "ORIGINAL_SCRIPT_CAPTURE_RHYTHM_PROFILE"
CAPTURE_RHYTHM_MULTICLIP = "NATIVE_MULTI_CLIP_V1"
CAPTURE_RHYTHM_LEGACY = "LEGACY_ONE_TAKE"
CAPTURE_RHYTHM_SCHEMA_VERSION = "capture-rhythm-contract-v5-structure-visible-clips"
SHOT_RICHNESS_POLICY_VERSION = "original-15s-shot-richness-v3-information-gain-review"
CREATOR_RECORDING_PROFILE_VERSION = "creator-recording-profile-v2-subtractive"
CREATOR_RECORDING_MODE_DIRECT = "CREATOR_DIRECT_SHARE"
CREATOR_RECORDING_MODE_OBSERVATION = "LIFE_EVENT_OBSERVATION"
CAPTURE_PRESET_WORN_DIRECT_SHARE = "WORN_DIRECT_SHARE"
CAPTURE_PRESET_PRODUCT_FIRST_THEN_WORN = "PRODUCT_FIRST_THEN_WORN"


def normalize_creator_capture_preset(profile: Dict[str, Any] | None) -> str:
    """Map old recording grammars onto the two small current presets."""

    value = _text(
        (profile or {}).get("capture_preset")
        or (profile or {}).get("capture_grammar")
    ).upper()
    if value in {
        CAPTURE_PRESET_PRODUCT_FIRST_THEN_WORN,
        "PRODUCT_FIRST_TO_WEARER_SHARE",
    }:
        return CAPTURE_PRESET_PRODUCT_FIRST_THEN_WORN
    return CAPTURE_PRESET_WORN_DIRECT_SHARE


def build_creator_recording_profile(
    *,
    top_category: str,
    product_type: str,
    content_carrier: str,
) -> Dict[str, Any]:
    """Return the small, internal recording-intent contract for stage 0.

    V2 is deliberately narrow: only women's-apparel directions that can be
    carried by a wearer opt into direct creator sharing.  Other categories keep
    the existing observation path, so this experiment cannot silently change
    accessories, hands-only or static-product production.
    """

    category = _text(top_category).lower()
    carrier = _text(content_carrier).upper()
    apparel_scope = category in {"女装", "women apparel", "womenswear"}
    wearer_scope = carrier in {"WEARER_ACTIVE", "MIXED"}
    enabled = apparel_scope and wearer_scope
    if not enabled:
        return {
            "schema_version": CREATOR_RECORDING_PROFILE_VERSION,
            "enabled": False,
            "recording_mode": CREATOR_RECORDING_MODE_OBSERVATION,
            "scope_reason": "PHASE1_WOMENSWEAR_WEARER_ONLY",
        }

    capture_preset = (
        CAPTURE_PRESET_PRODUCT_FIRST_THEN_WORN
        if carrier == "MIXED"
        else CAPTURE_PRESET_WORN_DIRECT_SHARE
    )
    return {
        "schema_version": CREATOR_RECORDING_PROFILE_VERSION,
        "enabled": True,
        "recording_mode": CREATOR_RECORDING_MODE_DIRECT,
        "capture_preset": capture_preset,
        # Keep the old key as a compatibility projection.  New code consumes
        # capture_preset; old stored readers still receive one stable string.
        "capture_grammar": capture_preset,
        "viewer_relationship": "DIRECT_FRIEND_SHARE",
        "visible_clip_range": [3, 4],
        "visible_clip_target": 3,
        "camera_setup_budget": 2,
        "life_event_required": False,
        "physical_continuity": "WEAR_STATE_MONOTONIC",
        "product_type": _text(product_type),
    }


def _text(value: Any) -> str:
    return str(value or "").strip()


def _stable_id(prefix: str, material: Any) -> str:
    from core.outfit_template_provider import without_outfit_display_metadata

    material = without_outfit_display_metadata(material)
    raw = json.dumps(material, ensure_ascii=False, sort_keys=True, default=str)
    return prefix + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20].upper()


def _dedupe_text(values: Iterable[Any], limit: int = 8) -> List[str]:
    result: List[str] = []
    for value in values:
        text = _text(value)
        if text and text.upper() != "UNAVAILABLE" and text not in result:
            result.append(text)
        if len(result) >= limit:
            break
    return result


def _expand_list_to_count(values: Iterable[Any], target_count: int) -> List[str]:
    """Resize ordered guidance without inventing a new semantic function."""

    source = [_text(value) for value in values if _text(value)]
    if not source or target_count <= 0:
        return []
    if target_count == 1:
        return [source[0]]
    if len(source) == target_count:
        return source
    return [
        source[round(index * (len(source) - 1) / max(1, target_count - 1))]
        for index in range(target_count)
    ]


def _capture_scene_is_public(scene_context: Dict[str, Any] | None) -> bool:
    material = json.dumps(scene_context or {}, ensure_ascii=False).lower()
    public_terms = (
        "商场", "展览", "展厅", "画廊", "书店", "大堂", "门厅",
        "电梯厅", "写字楼", "走廊", "连廊", "街边", "车站", "机场",
        "休息区", "公共", "mall", "gallery", "bookstore", "lobby",
        "elevator", "office lobby", "corridor", "public",
    )
    private_terms = (
        "公寓", "卧室", "客厅", "玄关", "家中", "化妆台", "home",
        "apartment", "bedroom", "living room",
    )
    return any(term in material for term in public_terms) and not any(
        term in material for term in private_terms
    )


def _retrieved_execution_card(
    retrieval_reference: Dict[str, Any] | None,
) -> Dict[str, Any]:
    reference = retrieval_reference or {}
    primary = reference.get("primary_real_case")
    primary = primary if isinstance(primary, dict) else {}
    card = primary.get("execution_card") or primary.get("reference_execution_spine")
    return dict(card) if isinstance(card, dict) else {}


def _expand_structure_roles(beats: Iterable[Any], unit_count: int) -> List[str]:
    """Project the routed macro structure onto visible clips without inventing beats.

    Extra clips repeat an existing proof/use beat; they never add a generic
    ``USE`` or ``ENDING`` that was absent from the selected structure.  This
    keeps four visible clips useful without flattening every family into the
    same four-part grammar.
    """

    roles = [_text(value).upper() for value in beats if _text(value)]
    if not roles:
        roles = ["HOOK", "PROOF"]
    target = max(1, int(unit_count or 1))
    if len(roles) > target:
        # The normal 15-second pool currently routes at most five macro beats.
        # If an older contract exceeds the visible-clip budget, retain both
        # ends and record the adjacent middle beats together rather than
        # silently replacing them with a different function.
        while len(roles) > target:
            merge_index = max(1, len(roles) - 2)
            roles[merge_index - 1:merge_index + 1] = [
                f"{roles[merge_index - 1]}+{roles[merge_index]}"
            ]
        return roles
    while len(roles) < target:
        proof_index = next(
            (index for index, role in enumerate(roles) if "PROOF" in role),
            -1,
        )
        if proof_index < 0:
            proof_index = next(
                (index for index, role in enumerate(roles) if "USE" in role),
                len(roles) - 1,
            )
        roles.insert(proof_index + 1, roles[proof_index])
    return roles


def _visible_change_jobs(structure_roles: Iterable[Any]) -> List[str]:
    """Give each cut one perceptible viewing job, not another action checklist."""

    occurrences: Dict[str, int] = {}
    jobs: List[str] = []
    for index, raw_role in enumerate(structure_roles):
        role = _text(raw_role).upper() or "MOMENT"
        occurrences[role] = occurrences.get(role, 0) + 1
        occurrence = occurrences[role]
        if index == 0 or "HOOK" in role or "ATTENTION" in role:
            job = (
                "RESULT_OR_ENTRY_ESTABLISHMENT"
                if occurrence == 1
                else f"DISTINCT_HOOK_INFORMATION_{occurrence}"
            )
        elif "PROOF" in role:
            job = (
                "PRIMARY_PRODUCT_EVIDENCE"
                if occurrence == 1
                else f"DISTINCT_PRODUCT_EVIDENCE_{occurrence}"
            )
        elif "USE" in role:
            job = (
                "OBSERVABLE_ACTION_OR_STATE_PROGRESS"
                if occurrence == 1
                else f"DISTINCT_ACTION_OR_STATE_PROGRESS_{occurrence}"
            )
        elif "ENDING" in role or "CLOSE" in role:
            job = "PRODUCT_RETURN_OR_EVENT_RESOLUTION"
        elif "CONTEXT" in role or "SCENE" in role:
            job = (
                "PRODUCT_IN_LIFE_CONTEXT"
                if occurrence == 1
                else f"DISTINCT_LIFE_CONTEXT_{occurrence}"
            )
        else:
            job = f"DISTINCT_RELATION_OR_STATE_{index + 1}"
        jobs.append(job)
    return jobs


def build_capture_rhythm_contract(
    *,
    capture_mode: str,
    macro_structure: Iterable[Any],
    scene_context: Dict[str, Any] | None = None,
    retrieval_reference: Dict[str, Any] | None = None,
    creator_recording_profile: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Compile one small public capture contract without another model call.

    Structure beats keep narrative authority.  This contract separately owns
    the number of edited clips and the number of physically plausible phone
    setups.  A public scene can therefore keep one fixed phone position plus a
    handheld cutaway without collapsing the finished video back to two clips.
    """

    configured = _text(os.environ.get(CAPTURE_RHYTHM_PROFILE_ENV)).upper()
    if configured in {"LEGACY", "LEGACY_ONE_TAKE", "ONE_TAKE"}:
        profile = CAPTURE_RHYTHM_LEGACY
    else:
        profile = CAPTURE_RHYTHM_MULTICLIP
    mode = _text(capture_mode).upper() or CAPTURE_MODE_CREATOR_SELF_SHOT
    beats = [_text(value).upper() for value in macro_structure if _text(value)]
    execution_card = _retrieved_execution_card(retrieval_reference)
    recording_profile = (
        dict(creator_recording_profile)
        if isinstance(creator_recording_profile, dict)
        else {}
    )
    direct_share = bool(recording_profile.get("enabled")) and _text(
        recording_profile.get("recording_mode")
    ).upper() == CREATOR_RECORDING_MODE_DIRECT
    public_scene = _capture_scene_is_public(scene_context)
    if profile == CAPTURE_RHYTHM_LEGACY:
        target_units = 1
        minimum_units = 1
        preferred_units = 1
        maximum_units = 1
        grammar = "CONTINUOUS_SINGLE_PHONE_VIEW"
    else:
        # Three clips are sufficient when each one contributes real content;
        # a fourth is retained only when the blueprint contains another useful
        # proof/use relation.  Shot count is not a quota for filler endings.
        minimum_units = 3
        if direct_share:
            preferred_units = 3
            maximum_units = 4
        else:
            # Keep the established generic/category paths unchanged.  The
            # 3-or-4 relaxation belongs only to the new women's-apparel direct
            # sharing path and must not flatten existing five-beat structures.
            preferred_units = (
                4 if mode == CAPTURE_MODE_CREATOR_SELF_SHOT else 3
            )
            maximum_units = 5 if mode == CAPTURE_MODE_CREATOR_SELF_SHOT else 4
        # The routed macro structure may legitimately contain more than the
        # default four beats.  Keep it when it still fits the five-clip
        # 15-second budget; shorter structures gain extra clips by repeating
        # an existing proof/use function, not by inventing a new beat.
        target_units = min(
            maximum_units,
            max(preferred_units, min(len(beats), maximum_units)),
        )
        if direct_share:
            planned_direct_clips = int(
                recording_profile.get("planned_visible_clip_count") or 0
            )
            if 3 <= planned_direct_clips <= 5:
                target_units = planned_direct_clips
                preferred_units = planned_direct_clips
        grammar = (
            normalize_creator_capture_preset(recording_profile)
            if direct_share
            else "ROUTED_STRUCTURE_VISIBLE_CLIPS"
        )
    setup_mode = (
        "CONTINUOUS_SINGLE_PHONE_VIEW"
        if profile == CAPTURE_RHYTHM_LEGACY
        else "ONE_PUBLIC_PHONE_POSITION_PLUS_HANDHELD_CUTAWAY"
        if mode == CAPTURE_MODE_CREATOR_SELF_SHOT and public_scene
        else "FIXED_PHONE_MULTI_CLIP"
        if mode == CAPTURE_MODE_CREATOR_SELF_SHOT
        else "PRODUCT_RECORDING_MULTI_CLIP"
    )
    camera_setup_count = (
        1
        if profile == CAPTURE_RHYTHM_LEGACY
        else 2
        if public_scene or mode != CAPTURE_MODE_CREATOR_SELF_SHOT
        else 2
    )
    structure_unit_roles = _expand_structure_roles(beats, target_units)
    # Direct-share clips already carry concrete model-authored visuals.  Do
    # not add a second per-cut "new information" job on top of them.
    visible_change_jobs = (
        [] if direct_share else _visible_change_jobs(structure_unit_roles)
    )
    observed_shot_count = int(execution_card.get("shot_count") or 0)
    observed_parts = [
        _text(value).upper()
        for value in execution_card.get("available_parts") or []
        if _text(value)
    ]
    real_execution_supported = bool(
        observed_shot_count >= 3 and observed_parts
    )
    contract = {
        "schema_version": CAPTURE_RHYTHM_SCHEMA_VERSION,
        "profile": profile,
        "capture_unit_count": target_units,
        "capture_grammar": grammar,
        "capture_emphasis": (
            "USE_OR_STATE_PROGRESS"
            if any(value in {"USE", "USE_PROCESS", "SCENE_USE"} for value in beats)
            else "DETAIL_RELATION"
            if any("DETAIL" in value for value in beats)
            else "RESULT_AND_EVIDENCE"
        ),
        "edit_style": (
            "CONTINUOUS_RECORDING"
            if profile == CAPTURE_RHYTHM_LEGACY
            else "NATIVE_HARD_CUT"
        ),
        "continuity_lock": [
            "SAME_CREATOR",
            "SAME_PRODUCT",
            "SAME_OUTFIT",
            "SAME_LOCATION",
            "SAME_TIME",
            "SAME_DEVICE",
        ],
        "commercial_camera_forbidden": True,
        "hard_required": False,
        "fallback_policy": "DETERMINISTIC_GROUP_NO_RETRY",
        "capture_setup_mode": setup_mode,
        "camera_setup_count": camera_setup_count,
        "macro_structure": beats,
        "structure_unit_roles": structure_unit_roles,
        "observable_change_jobs": visible_change_jobs,
        "structure_authority": (
            "ROUTED_MACRO_STRUCTURE_OWNS_VIEWING_ORDER; "
            "CAPTURE_EXECUTION_MAY_CHANGE_FRAMING_BUT_MUST_NOT_ADD_OR_REMOVE_BEATS"
        ),
        "shot_richness_contract": {
            "policy_version": SHOT_RICHNESS_POLICY_VERSION,
            "minimum_visible_clips": minimum_units,
            "preferred_visible_clips": preferred_units,
            "maximum_visible_clips": maximum_units,
            "planned_visible_clips": target_units,
            "camera_setup_budget": camera_setup_count,
            "required_function_coverage": list(
                dict.fromkeys(structure_unit_roles)
            ),
            "single_take_allowed": profile == CAPTURE_RHYTHM_LEGACY,
            "difference_policy": (
                "NO_EXACT_DUPLICATE;LATE_INFORMATION_GAIN_SOFT_REVIEW"
                if direct_share else "ONE_VISIBLE_DIFFERENCE_PER_CUT"
            ),
        },
        "derivation_source": (
            "REAL_EXECUTION_CARD"
            if real_execution_supported else "GENERIC_FALLBACK"
        ),
    }
    if recording_profile:
        contract["creator_recording_profile"] = recording_profile
    if direct_share:
        contract["capture_intent"] = (
            "同一创作者在同一地点用自己的手机分段直接分享商品。"
        )
        contract["physical_continuity"] = (
            "人物完成穿戴后保持穿戴；后续细节在身上展示，不重新脱下或平铺。"
        )
    if (
        setup_mode == "ONE_PUBLIC_PHONE_POSITION_PLUS_HANDHELD_CUTAWAY"
        and not direct_share
    ):
        middle_guidance = []
        for middle_index in range(max(0, target_units - 2)):
            middle_guidance.append(
                "由创作者手持同一部手机补录商品、穿着或人物关系近景"
                if middle_index == max(0, target_units - 3)
                else "沿用同一固定手机布置，换一个明确内容时刻录制不同的证明或使用关系"
            )
        contract["framing_guidance_by_unit"] = [
            "在一个自然可解释的位置固定手机，独立录制商品结果或核心分享开场",
            *middle_guidance,
            "回到固定位置或手持关系中的当前结构末段，独立录制而不擅自补结束Beat",
        ][:target_units]
    if execution_card:
        available_parts = observed_parts
        contract["retrieved_execution_shape"] = {
            "execution_card_id": _text(
                execution_card.get("execution_card_id")
                or execution_card.get("reference_spine_id")
            ),
            "observed_shot_count": observed_shot_count,
            "available_parts": available_parts,
            "evidence_status": (
                "SUPPORTED" if real_execution_supported else "INSUFFICIENT"
            ),
            "authority_boundary": (
                "只借鉴真实案例已有的拍摄段数量与功能顺序；"
                "不复制来源商品、人物、穿搭或宣称。"
            ),
        }
        # The real case contributes an execution example, never a replacement
        # structure.  Keeping this sequence separate prevents a reference card
        # with an ending from adding ENDING to a routed USE_PROCESS family.
        contract["reference_function_sequence"] = (
            list(available_parts) if real_execution_supported else []
        )
    return contract


def review_compiled_capture_unit_information_gain(
    capture_units: Iterable[Any],
    creator_recording_profile: Dict[str, Any] | None,
) -> Dict[str, Any]:
    """Review the final compiled clips, not an upstream creative promise.

    This diagnostic intentionally runs after deterministic grouping and any
    physical-state projection. It is non-blocking and cannot trigger a model
    retry or rewrite; its only job is to expose when late direct-share clips
    have collapsed back into the same stationary recording relationship.
    """

    profile = creator_recording_profile or {}
    preset = normalize_creator_capture_preset(profile)
    if not profile.get("enabled") or preset != CAPTURE_PRESET_WORN_DIRECT_SHARE:
        return {
            "policy_version": "compiled-information-gain-review-v1",
            "source": "COMPILED_CAPTURE_UNITS",
            "status": "NOT_APPLICABLE",
            "is_blocking": False,
            "low_information_gain_pairs": [],
        }
    units = [item for item in capture_units if isinstance(item, dict)]

    def relation_class(unit: Dict[str, Any]) -> str:
        material = unit.get("visible_signature_material")
        material = material if isinstance(material, dict) else {}
        text = " ".join(
            _text(value)
            for value in (
                material.get("visual"),
                material.get("action"),
                material.get("framing"),
                unit.get("framing_guidance"),
            )
        )
        if re.search(r"镜面|镜子", text):
            return "MIRROR"
        if re.search(r"坐下|坐在|靠坐|座位", text):
            return "SEATED"
        if re.search(
            r"(?:人物|她|创作者|模特).{0,12}(?:走向|走到|走入|走进|行走|步行|穿过|进入|离开)|"
            r"(?:走向|走到|走入|走进|行走|步行|穿过).{0,12}(?:门|窗|座位|走廊|场景|镜头|电梯)",
            text,
        ):
            return "MOVING_IN_SCENE"
        if re.search(r"手持自拍|自拍手机|拿着手机|举着手机", text):
            return "HANDHELD_SELFIE"
        if re.search(r"固定手机|固定机位|正对手机|面对手机", text):
            return "FIXED_PHONE_SHARE"
        if re.search(r"站立|站着|站定|自然站姿|原地", text):
            return "STANDING_SHARE"
        return "UNSPECIFIED_STATIONARY"

    low_pairs: List[Dict[str, Any]] = []
    stationary_relations = {
        "FIXED_PHONE_SHARE",
        "STANDING_SHARE",
        "UNSPECIFIED_STATIONARY",
    }
    # Opening -> proof may naturally keep one setup. The information-gain
    # promise concerns clip 2 -> clip 3 and any later transition.
    for previous_index in range(1, max(1, len(units) - 1)):
        current_index = previous_index + 1
        if current_index >= len(units):
            break
        previous_relation = relation_class(units[previous_index])
        current_relation = relation_class(units[current_index])
        if (
            previous_relation in stationary_relations
            and current_relation in stationary_relations
        ):
            low_pairs.append(
                {
                    "capture_unit_pair": [previous_index + 1, current_index + 1],
                    "reason": "SAME_STATIONARY_SHARE_RELATION_IN_FINAL_UNITS",
                    "previous_relation": previous_relation,
                    "current_relation": current_relation,
                }
            )
    return {
        "policy_version": "compiled-information-gain-review-v1",
        "source": "COMPILED_CAPTURE_UNITS",
        "status": "LOW_INFORMATION_GAIN" if low_pairs else "SUFFICIENT",
        "is_blocking": False,
        "low_information_gain_pairs": low_pairs,
        "note": "最终拍摄单元诊断；不触发阻断、重试、修订或新增模型调用。",
    }


def compile_capture_units(
    storyboard: Iterable[Any],
    contract: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Attach authoritative capture-unit ids and return compact unit metadata.

    The model may suggest ids, but deterministic grouping prevents prompt drift
    from sending a script back into a repair loop.  Four-to-six structural
    passages become three-to-five visible clips while camera setup count stays
    a separate feasibility budget.
    """

    shots = [dict(value) for value in storyboard if isinstance(value, dict)]
    if not shots:
        return [], []
    profile = _text(contract.get("profile")).upper()
    requested = int(contract.get("capture_unit_count") or 1)
    unit_count = 1 if profile == CAPTURE_RHYTHM_LEGACY else max(2, requested)
    unit_count = min(unit_count, len(shots))

    grouping_source = "DETERMINISTIC_FALLBACK"
    suggested_sizes: List[int] = []
    if unit_count > 1:
        # The visual author sees the actual action and camera prose, so its
        # proposed boundaries are semantically better than blind equal-sized
        # partitions.  Preserve them only when they form exactly the frozen
        # number of contiguous, correctly numbered units.  Any malformed or
        # interleaved suggestion still falls back to deterministic grouping.
        expected_unit = 1
        current_size = 0
        suggestion_valid = True
        for shot_index, shot in enumerate(shots):
            unit_id = _text(shot.get("capture_unit_id")).upper()
            matched = re.fullmatch(r"CU_(\d{2})", unit_id)
            if not matched:
                suggestion_valid = False
                break
            number = int(matched.group(1))
            if shot_index == 0:
                if number != 1:
                    suggestion_valid = False
                    break
                current_size = 1
                continue
            if number == expected_unit:
                current_size += 1
                if shot.get("starts_new_take") is True:
                    suggestion_valid = False
                    break
                continue
            if number == expected_unit + 1:
                if shot.get("starts_new_take") is False:
                    suggestion_valid = False
                    break
                suggested_sizes.append(current_size)
                expected_unit = number
                current_size = 1
                continue
            suggestion_valid = False
            break
        if suggestion_valid and current_size:
            suggested_sizes.append(current_size)
        if (
            suggestion_valid
            and len(suggested_sizes) == unit_count
            and sum(suggested_sizes) == len(shots)
            and all(size > 0 for size in suggested_sizes)
        ):
            grouping_source = "MODEL_BOUNDARIES_VALIDATED"

    if grouping_source == "MODEL_BOUNDARIES_VALIDATED":
        group_sizes = suggested_sizes
    elif unit_count == 1:
        group_sizes = [len(shots)]
    elif unit_count == 2:
        first_size = max(1, len(shots) // 2)
        group_sizes = [first_size, len(shots) - first_size]
    else:
        # Keep opening and ending independently visible.  Only the middle
        # structural passages are balanced; this prevents a five/six-shot
        # script from being swallowed into two long passages.
        if unit_count == len(shots):
            group_sizes = [1] * unit_count
        elif unit_count >= 3 and len(shots) >= unit_count:
            middle_shots = len(shots) - 2
            middle_units = unit_count - 2
            base_size, extra = divmod(middle_shots, middle_units)
            middle_sizes = [
                base_size + (1 if index < extra else 0)
                for index in range(middle_units)
            ]
            group_sizes = [1, *middle_sizes, 1]
        else:
            base_size, extra = divmod(len(shots), unit_count)
            group_sizes = [
                base_size + (1 if index < extra else 0)
                for index in range(unit_count)
            ]

    roles_by_count = {
        1: ["CONTINUOUS_SHARE"],
        2: ["OPENING", "PROOF_AND_CONTEXT"],
        3: ["OPENING", "CORE_PROOF_OR_USE", "CONTEXT_END"],
        4: [
            "OPENING",
            "CORE_PROOF",
            "USE_OR_RELATION",
            "CONTEXT_OR_PRODUCT_RETURN",
        ],
        5: [
            "OPENING",
            "PROOF_1",
            "PROOF_OR_USE_2",
            "CONTEXT_RELATION",
            "PRODUCT_RETURN",
        ],
    }
    framing_by_count = {
        1: ["沿用同一普通手机关系"],
        2: [
            "独立录制抓人结果景或商品主体景",
            "同一地点重新放置手机，录制证明、使用关系或生活收束，景别与上一段有可感知差异",
        ],
        3: [
            "独立录制抓人结果景或商品主体景",
            "同一地点重新放置手机，录制商品证明或使用关系，景别与上一段有可感知差异",
            "同一地点补录人物、商品与生活场景关系，完成自然收束",
        ],
        4: [
            "独立录制抓人结果景或商品主体景",
            "录制与开场有明确景别差异的商品证明或可见细节",
            "录制人物、商品与使用或穿搭关系的自然变化",
            "独立补录商品结果或生活关系收束，不退回远景弱化商品",
        ],
        5: [
            "独立录制抓人结果景或商品主体景",
            "补录第一处核心证明，景别与开场不同",
            "补录第二处兼容证明或使用关系，不增加动作清单",
            "录制人物、商品与生活场景的自然关系",
            "独立回到商品清晰结果，自然完成收束",
        ],
    }
    roles = roles_by_count[len(group_sizes)]
    framing = framing_by_count[len(group_sizes)]
    structure_authoritative = bool(
        contract.get("structure_unit_roles") or contract.get("macro_structure")
    )
    structure_roles = [
        _text(value).upper()
        for value in contract.get("structure_unit_roles") or []
        if _text(value)
    ]
    if len(structure_roles) != len(group_sizes) and structure_authoritative:
        structure_roles = _expand_structure_roles(
            contract.get("macro_structure") or [],
            len(group_sizes),
        )
    change_jobs = [
        _text(value).upper()
        for value in contract.get("observable_change_jobs") or []
        if _text(value)
    ]
    recording_profile = (
        contract.get("creator_recording_profile")
        if isinstance(contract.get("creator_recording_profile"), dict)
        else {}
    )
    direct_share = bool(recording_profile.get("enabled")) and _text(
        recording_profile.get("recording_mode")
    ).upper() == CREATOR_RECORDING_MODE_DIRECT
    if not direct_share and len(change_jobs) != len(group_sizes) and structure_roles:
        change_jobs = _visible_change_jobs(structure_roles)
    if not direct_share and len(change_jobs) != len(group_sizes):
        change_jobs = [f"DISTINCT_VISIBLE_INFORMATION_{index + 1}" for index in range(len(group_sizes))]
    frozen_roles = [
        _text(value).upper()
        for value in contract.get("unit_roles") or []
        if _text(value)
    ]
    frozen_framing = [
        _text(value)
        for value in contract.get("framing_guidance_by_unit") or []
        if _text(value)
    ]
    # Category execution roles may own the physical display relation (for
    # example PRODUCT_REACQUISITION for a hair accessory), while
    # structure_roles independently retain the routed viewing order.
    if len(frozen_roles) == len(group_sizes):
        roles = frozen_roles
    elif len(structure_roles) == len(group_sizes):
        roles = structure_roles
    if len(frozen_framing) == len(group_sizes):
        framing = frozen_framing
    units: List[Dict[str, Any]] = []
    annotated: List[Dict[str, Any]] = []
    cursor = 0
    for unit_index, size in enumerate(group_sizes, 1):
        unit_id = f"CU_{unit_index:02d}"
        group = shots[cursor:cursor + size]
        unit_structure_role = (
            structure_roles[unit_index - 1]
            if len(structure_roles) == len(group_sizes)
            else _text(group[0].get("narrative_role")).upper()
            if group
            else roles[unit_index - 1]
        )
        shot_numbers = []
        narrative_roles = []
        for local_index, shot in enumerate(group):
            item = dict(shot)
            source_narrative_role = _text(item.get("narrative_role")).upper()
            structure_role = unit_structure_role or source_narrative_role or roles[unit_index - 1]
            if structure_authoritative:
                if source_narrative_role and source_narrative_role != structure_role:
                    item["source_narrative_role"] = source_narrative_role
                item["narrative_role"] = structure_role
            item["structure_role"] = structure_role
            if len(change_jobs) == len(group_sizes):
                item["observable_change_job"] = change_jobs[unit_index - 1]
            else:
                item.pop("observable_change_job", None)
            item["capture_unit_id"] = unit_id
            item["starts_new_take"] = local_index == 0
            item["edit_before"] = (
                "START"
                if unit_index == 1 and local_index == 0
                else "DIRECT_CUT"
                if local_index == 0
                else "CONTINUE"
            )
            item["capture_unit_role"] = roles[unit_index - 1]
            annotated.append(item)
            shot_numbers.append(int(item.get("shot_no") or cursor + local_index + 1))
            role = _text(item.get("narrative_role")).upper()
            if role and role not in narrative_roles:
                narrative_roles.append(role)
        visible_signature_material = {
            "framing": _dedupe_text(
                item.get("framing") or item.get("style_note") for item in group
            ),
            "carrier": _dedupe_text(item.get("carrier_mode") for item in group),
            "visual": _dedupe_text(
                item.get("shot_content") or item.get("visual_content") for item in group
            ),
            "action": _dedupe_text(
                item.get("observable_action") or item.get("character_action") for item in group
            ),
            "proof_job": (
                change_jobs[unit_index - 1]
                if len(change_jobs) == len(group_sizes)
                else ""
            ),
        }
        units.append(
            {
                "capture_unit_id": unit_id,
                "order": unit_index,
                "shot_numbers": shot_numbers,
                "narrative_roles": narrative_roles,
                "structure_role": unit_structure_role,
                "unit_role": roles[unit_index - 1],
                "observable_change_job": (
                    change_jobs[unit_index - 1]
                    if len(change_jobs) == len(group_sizes)
                    else ""
                ),
                "framing_guidance": framing[unit_index - 1],
                "edit_before": "START" if unit_index == 1 else "DIRECT_CUT",
                "grouping_source": grouping_source,
                "visible_signature": _stable_id(
                    "VSG_", visible_signature_material
                ),
                "visible_signature_material": visible_signature_material,
            }
        )
        cursor += size
    final_information_gain_review = review_compiled_capture_unit_information_gain(
        units,
        recording_profile,
    )
    contract["final_information_gain_review"] = final_information_gain_review
    richness = dict(contract.get("shot_richness_contract") or {})
    if richness:
        planned = int(
            richness.get("planned_visible_clips")
            or contract.get("capture_unit_count")
            or len(units)
        )
        minimum = int(richness.get("minimum_visible_clips") or 1)
        richness["compiled_visible_clips"] = len(units)
        richness["storyboard_segment_count"] = len(shots)
        richness["maximum_storyboard_segments_per_clip"] = max(
            (len(unit.get("shot_numbers") or []) for unit in units),
            default=0,
        )
        richness["compiled_function_sequence"] = [
            _text(unit.get("structure_role")).upper() for unit in units
        ]
        collapsed_compiled = []
        for role in richness["compiled_function_sequence"]:
            for part in role.split("+"):
                if not collapsed_compiled or collapsed_compiled[-1] != part:
                    collapsed_compiled.append(part)
        routed_macro = [
            _text(value).upper()
            for value in contract.get("macro_structure") or []
            if _text(value)
        ]
        richness["structure_preservation_status"] = (
            "PRESERVED"
            if not routed_macro or collapsed_compiled == routed_macro
            else "MISMATCH"
        )
        richness["observable_change_jobs"] = [
            _text(unit.get("observable_change_job")).upper() for unit in units
        ]
        signature_count = len(
            {
                _text(unit.get("visible_signature"))
                for unit in units
                if _text(unit.get("visible_signature"))
            }
        )
        richness["distinct_visible_signature_count"] = signature_count
        richness["visible_signature_status"] = (
            "DISTINCT"
            if signature_count == len(units)
            else "DUPLICATE_CLIP_DESIGN_WARNING"
        )
        clip_count_preserved = (
            len(units) == min(planned, len(shots)) and len(units) >= minimum
        )
        richness["preservation_status"] = (
            "PRESERVED"
            if clip_count_preserved
            and richness["structure_preservation_status"] == "PRESERVED"
            else "STRUCTURE_MISMATCH"
            if clip_count_preserved
            else "DEGRADED_STORYBOARD_TOO_SHORT"
        )
        richness["final_information_gain_review"] = dict(
            final_information_gain_review
        )
        contract["shot_richness_contract"] = richness
    return annotated, units


def _anchor_segments(value: Any) -> List[str]:
    """Split an approved compound anchor without authorizing loose substrings."""

    text = _text(value)
    if not text:
        return []
    return _dedupe_text(re.split(r"[，,；;。]", text), limit=12)


def _anchor_is_authorized(candidate: Any, approved_anchors: Iterable[str]) -> bool:
    """Accept an exact anchor or exact clauses from one approved compound anchor."""

    candidate_text = _text(candidate)
    if not candidate_text:
        return False
    candidate_segments = set(_anchor_segments(candidate_text))
    for approved in approved_anchors:
        approved_text = _text(approved)
        if candidate_text == approved_text:
            return True
        approved_segments = set(_anchor_segments(approved_text))
        if candidate_segments and candidate_segments.issubset(approved_segments):
            return True
    return False


def _anchor_texts(anchor_card: Dict[str, Any], key: str) -> List[str]:
    values: List[str] = []
    for item in anchor_card.get(key) or []:
        if isinstance(item, dict):
            values.append(
                item.get("anchor")
                or item.get("anchor_text")
                or item.get("name")
                or item.get("value")
            )
        else:
            values.append(item)
    return _dedupe_text(values)


_BUTTON_COUNT_TOKEN = r"(?:\d+|[一二三四五六七八九十两]+)"
_SCARF_CANONICAL_TYPES = {"scarf", "winter_scarf", "silk_scarf", "headscarf"}
_WRIST_ACCESSORY_TYPES = {"bracelet", "bangle", "slim_bangle"}
_HAIR_ACCESSORY_TYPES = {
    "hair_accessory_generic", "claw_clip", "hair_clip", "headband",
    "scrunchie", "hair_tie", "ribbon", "hair_pin",
}
_OTHER_ACCESSORY_TYPES = {"earring", "ring", "necklace", "choker"}
_COLOR_TERMS = (
    "黑", "白", "灰", "米", "棕", "咖", "红", "粉", "橙", "黄", "绿", "蓝",
    "紫", "金", "银", "卡其", "驼色", "藏青", "酒红", "抹茶", "奶油",
)


def _visible_button_count(evidence_text: str) -> str:
    """Return only a count explicitly tied to a visible/front button anchor."""

    patterns = (
        rf"(?:左侧|右侧)[^，；。]{{0,16}}?可见[^，；。]{{0,8}}?({_BUTTON_COUNT_TOKEN})\s*颗?[^，；。]{{0,10}}?(?:扣子|纽扣|按扣|圆扣|纹理扣|扣)",
        rf"(?:正面|前襟)[^，；。]{{0,14}}?({_BUTTON_COUNT_TOKEN})\s*颗?[^，；。]{{0,10}}?(?:扣子|纽扣|按扣|圆扣|纹理扣|扣)",
    )
    for pattern in patterns:
        match = re.search(pattern, evidence_text)
        if match:
            return _text(match.group(1))
    return ""


def _visible_closure_contract(evidence_text: str) -> Dict[str, Any]:
    """Compile only visually explicit single-row closure semantics.

    A hidden counterpart is a closure mechanism, not a second visible button
    row.  Keeping that distinction structured prevents later video-prompt
    prose from accidentally literalising hidden snaps as double-breasted
    buttons.  Unknown layouts stay unavailable rather than being inferred.
    """

    if not evidence_text or "双排" in evidence_text:
        return {"status": "UNAVAILABLE"}
    hidden_counterpart = "暗扣" in evidence_text and any(
        token in evidence_text for token in ("可见", "隐藏", "不外露")
    )
    single_front_signal = any(
        token in evidence_text for token in ("单排", "单列", "竖向前襟", "竖直前襟")
    ) or hidden_counterpart
    if not single_front_signal:
        return {"status": "UNAVAILABLE"}
    count = _visible_button_count(evidence_text)
    visible_description = (
        f"前襟只允许一列{count}颗可见扣子"
        if count
        else "前襟只允许一列可见扣子"
    )
    hidden_description = (
        "另一侧暗扣属于隐藏闭合件，不得显示为第二列可见纽扣"
        if hidden_counterpart
        else ""
    )
    return {
        "status": "AVAILABLE",
        "layout": "SINGLE_VISIBLE_VERTICAL_ROW",
        "visible_button_count": count,
        "hidden_counterpart": hidden_counterpart,
        "visible_description": visible_description,
        "hidden_description": hidden_description,
        "authority": "APPROVED_ANCHOR",
    }


def _matching_anchor_segments(
    anchors: Iterable[str],
    tokens: Iterable[str],
    *,
    limit: int = 4,
) -> List[str]:
    return _dedupe_text(
        (
            segment
            for anchor in anchors
            for segment in _anchor_segments(anchor)
            if any(token in segment for token in tokens)
        ),
        limit=limit,
    )


def _scarf_identity_contract(
    product_truth: Dict[str, Any],
    *,
    anchors: Iterable[str],
) -> Dict[str, Any]:
    """Group only explicitly approved scarf evidence into a compact card."""

    anchor_list = list(anchors)
    shape_anchors = _matching_anchor_segments(
        anchor_list,
        ("方形", "方巾", "正方", "长条", "长方", "矩形", "三角"),
    )
    color_anchors = _matching_anchor_segments(anchor_list, _COLOR_TERMS)
    pattern_anchors = _matching_anchor_segments(
        anchor_list,
        ("图案", "印花", "花纹", "格纹", "条纹", "波点", "纯色", "撞色", "渐变"),
    )
    edge_anchors = _matching_anchor_segments(
        anchor_list,
        ("流苏", "包边", "边框", "卷边", "毛边", "锁边", "滚边"),
    )
    logo_text_anchors = _matching_anchor_segments(
        anchor_list,
        ("logo", "LOGO", "Logo", "文字", "字母", "品牌标识"),
    )
    canonical_type = _text(product_truth.get("canonical_product_type"))
    wearing_zone = {
        "winter_scarf": "NECK_SHOULDER",
        "silk_scarf": "NECK_UPPER_BODY",
        "headscarf": "HEAD_HAIR",
        "scarf": "NECK_SHOULDER",
    }.get(canonical_type, "UNAVAILABLE")
    return {
        "status": "AVAILABLE",
        "canonical_product_type": canonical_type,
        "shape_anchors": shape_anchors or ["UNAVAILABLE"],
        "color_anchors": color_anchors or ["UNAVAILABLE"],
        "pattern_anchors": pattern_anchors or ["UNAVAILABLE"],
        "edge_anchors": edge_anchors or ["UNAVAILABLE"],
        "logo_text_anchors": logo_text_anchors or ["UNAVAILABLE"],
        "placement_authority": wearing_zone,
        "material_authority": "UNAVAILABLE_UNLESS_APPROVED_ANCHOR",
        "authority": "APPROVED_ANCHORS_AND_REFERENCE_IMAGE",
    }


def build_product_identity_lock(product_truth: Dict[str, Any]) -> Dict[str, Any]:
    """Compile a compact, deterministic product-identity hand-off.

    The lock deliberately uses only already-approved identity/detail anchors.
    It does not call a model and does not invent missing garment properties.
    Reference-image authority covers attributes that are visually obvious but
    not safely expressible as new structured facts.
    """

    identity_anchors = _dedupe_text(product_truth.get("identity_anchors") or [], limit=8)
    visible_details = _dedupe_text(product_truth.get("visible_detail_anchors") or [], limit=6)
    product_identity = _text(product_truth.get("product_identity"))
    # Structured identity anchors are the physical authority.  The display
    # label may contain operator positioning (for example ``通勤`` or
    # ``咖啡店出片``); mixing that prose into the visual identity lock lets a
    # marketing context silently become a compulsory scene.  Keep the label
    # only as a last-resort fallback when no approved physical anchor exists.
    must_preserve = _dedupe_text(
        identity_anchors or ([product_identity] if product_identity else []),
        limit=8,
    )
    evidence_text = "；".join([*must_preserve, *visible_details])
    canonical_type = _text(product_truth.get("canonical_product_type"))
    raw_quantity_contract = (
        product_truth.get("display_quantity_contract")
        if isinstance(product_truth.get("display_quantity_contract"), dict)
        else {}
    )
    required_quantity_text = _text(
        raw_quantity_contract.get("required_display_count")
    )
    quantity_authorized = (
        canonical_type in _WRIST_ACCESSORY_TYPES
        and _text(raw_quantity_contract.get("status")) == "AUTHORIZED"
        and _text(raw_quantity_contract.get("mode")) == "SAME_SKU_STACK"
        and required_quantity_text in {"2", "3"}
    )
    if canonical_type in _SCARF_CANONICAL_TYPES:
        scarf_contract = _scarf_identity_contract(
            product_truth,
            anchors=[*must_preserve, *visible_details],
        )
        must_not_change = [
            "禁止把商品替换成相似款或根据常见款式重新设计",
            "禁止改变参考图和已批准锚点中的颜色、图案布局、边框、流苏或整体形状",
            "禁止把方形商品改成长条形，或把长条形商品改成方形",
            "禁止新增参考图和已批准锚点中没有的Logo、文字或装饰",
        ]
        if canonical_type == "silk_scarf":
            must_not_change.append(
                "产品类型‘丝巾’不授权真丝、桑蚕丝、冰凉或亲肤材质表现"
            )
        if canonical_type == "headscarf":
            must_not_change.append(
                "禁止根据商品外观新增宗教、民族或文化身份"
            )
        return {
            "reference_image_is_authority": True,
            "priority": "HIGHEST",
            "must_preserve": must_preserve,
            "critical_visible_details": visible_details,
            "must_not_change": _dedupe_text(must_not_change, limit=7),
            "visible_closure_contract": {"status": "NOT_APPLICABLE"},
            "scarf_identity_contract": scarf_contract,
            "compiler_version": "product-identity-lock-v3-scarf",
        }
    if canonical_type in {
        *_WRIST_ACCESSORY_TYPES,
        *_HAIR_ACCESSORY_TYPES,
        *_OTHER_ACCESSORY_TYPES,
    }:
        must_not_change = [
            "禁止把商品替换成相似款或根据常见配饰重新设计",
            "禁止改变参考图和已批准锚点中的配饰类型、整体形状、颜色、相对尺寸或装饰布局",
            "禁止新增参考图和已批准锚点中没有的宝石、链条、吊坠、文字或Logo",
        ]
        if canonical_type in _WRIST_ACCESSORY_TYPES:
            if quantity_authorized:
                required_count = int(required_quantity_text)
                must_not_change.extend([
                    "禁止把已确认的手链、手镯或细手圈改成另一类腕饰；禁止改变环体或链体的相对宽窄、开口",
                    f"本条已由运营卖点明确授权同款叠戴，目标手腕必须始终佩戴{required_count}只同款商品；不得中途增加、减少、摘下或改到另一只手",
                    "除这组已授权同款叠戴外，不得生成其他腕饰、手表或背景中的重复商品",
                ])
            else:
                must_not_change.extend([
                    "禁止把已确认的手链、手镯或细手圈改成另一类腕饰；禁止改变环体或链体的相对宽窄、开口和佩戴数量",
                    "商品只能出现在手腕位置；不得改成项圈、颈饰、戒指或佩戴在另一只手上的重复商品",
                    "未经运营数量授权，禁止新增第二只同款商品或自行设计叠戴",
                ])
        elif canonical_type in _HAIR_ACCESSORY_TYPES:
            must_not_change.extend([
                "禁止把已确认的抓夹、发夹、发圈、发带或发针改成另一种发饰；齿梳、夹体、装饰布局和相对大小必须服从参考图",
                "禁止在头发另一侧、手中或背景中生成第二个同款商品；不得新增参考图没有的蝴蝶结、花朵、珍珠或宝石",
            ])
        elif canonical_type == "earring":
            must_not_change.append(
                "耳饰的单双关系、连接结构、部件顺序和相对长度只服从已批准锚点与参考图"
            )
        return {
            "reference_image_is_authority": True,
            "priority": "HIGHEST",
            "must_preserve": must_preserve,
            "critical_visible_details": visible_details,
            "must_not_change": _dedupe_text(must_not_change, limit=7),
            "visible_closure_contract": {"status": "NOT_APPLICABLE"},
            "display_quantity_contract": (
                dict(raw_quantity_contract) if quantity_authorized else {
                    "status": "SINGLE_PRODUCT_DEFAULT",
                    "required_display_count": 1,
                    "authority": "DEFAULT_IDENTITY_SAFETY",
                }
            ),
            "compiler_version": "product-identity-lock-v4-explicit-quantity",
        }
    closure_contract = _visible_closure_contract(evidence_text)
    if closure_contract.get("status") == "AVAILABLE":
        must_preserve = _dedupe_text(
            [
                *must_preserve,
                closure_contract.get("visible_description"),
                closure_contract.get("hidden_description"),
            ],
            limit=10,
        )
    must_not_change = [
        "禁止把商品替换成相似款或根据常见款式重新设计",
        "禁止改变参考图中的颜色、版型、衣长、领型、前襟和袖口结构",
    ]

    has_button = any(token in evidence_text for token in ("扣子", "纽扣", "按扣", "圆扣", "纹理扣"))
    explicit_button_count = bool(
        re.search(
            r"(?:\d+|[一二三四五六七八九十两]+)[^，；。]{0,12}(?:扣子|纽扣|按扣|圆扣|纹理扣|扣)",
            evidence_text,
        )
    )
    if has_button:
        must_not_change.append("扣子数量、位置、排列方式和间距必须与参考图一致")
        if explicit_button_count:
            must_not_change.append("禁止增加、减少或改写任何已经明确数量的扣子")
        if closure_contract.get("status") == "AVAILABLE":
            must_not_change.append("禁止将参考图中的前襟扣子改成双排扣")
            must_not_change.append("禁止左右对称生成两列可见纽扣")
            if closure_contract.get("hidden_counterpart"):
                must_not_change.append("禁止把隐藏暗扣画成外露纽扣")

    if "拉链" in evidence_text:
        must_not_change.append("禁止把参考图中的拉链替换成纽扣、暗扣或其他闭合结构")

    return {
        "reference_image_is_authority": True,
        "priority": "HIGHEST",
        "must_preserve": must_preserve,
        "critical_visible_details": visible_details,
        "must_not_change": _dedupe_text(must_not_change, limit=9),
        "visible_closure_contract": closure_contract,
        "compiler_version": "product-identity-lock-v2",
    }


def _macro_structure(contract: Dict[str, Any]) -> List[str]:
    hard = contract.get("hard_constraints") if isinstance(contract.get("hard_constraints"), dict) else {}
    sequence = hard.get("beat_sequence") or contract.get("beat_sequence") or []
    result = [
        _text(item) for item in sequence[:8]
        if _text(item) and _text(item).upper() != "UNAVAILABLE"
    ] if isinstance(sequence, list) else []
    if result:
        return result
    identity = contract.get("direction_identity") if isinstance(contract.get("direction_identity"), dict) else {}
    family = _text(identity.get("macro_family_key") or hard.get("macro_family_key"))
    return [part.strip() for part in family.split(">") if part.strip()] or ["HOOK", "PROOF"]


def _is_apparel(product_type: str, top_category: str, anchor_card: Dict[str, Any]) -> bool:
    material = " ".join(
        [
            _text(product_type),
            _text(top_category),
            _text(anchor_card.get("product_type")),
            _text(anchor_card.get("top_category")),
            json.dumps(anchor_card.get("category_execution_contract") or {}, ensure_ascii=False),
        ]
    ).lower()
    return any(
        token in material
        for token in (
            "女装", "男装", "服装", "外套", "上衣", "裙", "裤", "衬衫",
            "夹克", "针织", "apparel", "jacket", "shirt", "dress", "coat",
        )
    )


def _opening_visual_job(
    product_truth: Dict[str, Any],
    *,
    presentation: str,
    requested_hook_id: str,
) -> Dict[str, Any]:
    """Compile one soft first-three-second viewing job from the frozen value."""

    argument = (
        product_truth.get("selling_argument")
        if isinstance(product_truth.get("selling_argument"), dict)
        else {}
    )
    mode = _text(presentation).upper()
    hook_id = _text(requested_hook_id).upper()
    claim_type = _text(argument.get("claim_type")).lower()
    if mode == "STATIC_PRODUCT":
        job = "PRODUCT_FIRST"
    elif mode == "HANDS_ONLY":
        job = "SHOW_DETAIL"
    elif claim_type == "visual_result":
        job = "SHOW_RESULT"
    elif hook_id == "DETAIL_SURPRISE":
        job = "SHOW_DETAIL"
    else:
        job = "SHOW_RESULT"

    guidance = {
        "SHOW_RESULT": "前三秒优先让穿着后的整体结果、轮廓或比例清楚可见",
        "SHOW_DETAIL": "前三秒优先让一个已确认且与本条价值相关的可见细节清楚可见",
        "SHOW_USE_SCENE": "前三秒优先让本条卖点对应的使用场景与完整商品关系成立",
        "PRODUCT_FIRST": "前三秒可以先看清静置商品本身，再按结构进入后续画面",
    }[job]
    return {
        "job": job,
        "first_window_seconds": [0, 3],
        "guidance_zh": guidance,
        "authority": "SOFT_CREATIVE_GUIDANCE",
        "must_not_force": ["前后对比", "身体缺点特写", "情绪反转表演", "额外剧情动作"],
    }


def _preferred_presentation(
    *,
    source_carrier: str,
    bundle: Dict[str, Any],
    product_type: str,
    top_category: str,
    anchor_card: Dict[str, Any],
) -> Tuple[str, str]:
    carrier = _text(source_carrier).upper()
    if carrier in {"WEARER_ACTIVE", "MIXED", "PERSON_ON_CAMERA"}:
        return "PERSON_ON_CAMERA", "沿用计划阶段冻结的真人承载"
    if carrier in {"HAND_ONLY", "HANDS_ONLY"}:
        return "HANDS_ONLY", "沿用计划阶段冻结的手部承载"
    if carrier == "STATIC_PRODUCT":
        return "STATIC_PRODUCT", "沿用计划阶段冻结的静物承载"
    return "PERSON_ON_CAMERA", "结构承载未知，服饰原创沿用既有真人兜底"


def _capture_mode_for_presentation(presentation: str) -> str:
    """Resolve who owns the camera without changing the routed carrier."""

    return {
        "PERSON_ON_CAMERA": CAPTURE_MODE_CREATOR_SELF_SHOT,
        "HANDS_ONLY": CAPTURE_MODE_HANDS_PRODUCT_SHARE,
        "STATIC_PRODUCT": CAPTURE_MODE_STATIC_PRODUCT_RECORD,
    }.get(_text(presentation).upper(), CAPTURE_MODE_CREATOR_SELF_SHOT)


def _project_mixed_template_shot_contract(
    seed: Dict[str, Any],
    storyboard: Any,
    capture_units: Any,
    target: Dict[str, Any],
) -> None:
    """Stamp the frozen per-shot carrier back onto the compiled script.

    ``compile_capture_units`` rebuilds the clip list from the structure contract,
    which knows nothing about the authored mixed-display template.  Without this
    projection the per-shot carrier would survive planning and then silently
    disappear here.  Only runs when a template contract is actually present.

    A compiled clip list that does not line up with the frozen template is
    recorded as a hard error rather than skipped: the shots then have no
    authorised carrier, framing or timeline, and letting the script continue
    would ship an unauthored montage.  ``validate_simplified_visual_script``
    consumes those codes.
    """

    extension = (
        seed.get("category_execution_extension")
        if isinstance(seed.get("category_execution_extension"), dict)
        else {}
    )
    contract = extension.get("mixed_template_contract") if extension else None
    if not isinstance(contract, dict) or not contract:
        return
    try:
        from core.accessory_mixed_templates import (
            NO_FACE_POLICY,
            PROJECTION_ERRORS_KEY,
            PROJECTION_STATUS_MISMATCH,
            PROJECTION_STATUS_SKIPPED,
            project_mixed_template_onto_units,
            scrub_no_face_prose_in_place,
        )
    except Exception:  # noqa: BLE001 - keep legacy paths untouched
        return
    projection = project_mixed_template_onto_units(capture_units, storyboard, contract)
    status = _text(projection.get("status"))
    if status == PROJECTION_STATUS_SKIPPED:
        return
    if status == PROJECTION_STATUS_MISMATCH:
        # Hard stop: record the codes for the script validator and claim nothing.
        # ``mixed_template_shot_projection`` is deliberately *not* written, so no
        # downstream reader can mistake this for an applied contract.
        target[PROJECTION_ERRORS_KEY] = list(projection.get("errors") or [])
        target["mixed_template_contract"] = dict(contract)
        return
    target["mixed_template_contract"] = dict(contract)
    target["mixed_template_shot_projection"] = projection
    if _text(contract.get("face_policy")).upper() == NO_FACE_POLICY:
        # Second boundary for the NO_FACE contract.  The projection above cleaned
        # the fields the adapter owns, but a script is also *model-authored
        # prose*: ``production_design.scene.subject_position`` and
        # ``storyboard[].camera`` are written by the generator, never seen by any
        # adapter, and copied verbatim into the video prompt.  Left alone, a
        # model renders ``CREATOR_SELF_SHOT`` as "人物在同一自拍范围内轻微调整位置"
        # and the face comes back in through the capture mode.
        #
        # Must run *here* and not later: ``video_generation_brief`` picks up
        # ``production_design`` / ``storyboard`` by reference (see its
        # construction below), so cleaning a copy afterwards would not reach it.
        scrub_no_face_prose_in_place(target.get("production_design"))
        scrub_no_face_prose_in_place(target.get("storyboard"))


def _visual_selling_argument_view(raw_argument: Dict[str, Any]) -> Dict[str, Any]:
    """Build the only selling-argument view exposed to the visual model.

    Operator wording remains in the frozen content bundle for the central
    voiceover.  The visual generator receives IDs, governance semantics and a
    normalized creative value only, so source rhetoric cannot turn into a
    character biography or scene premise.
    """

    operator_wording_present = bool(_text(raw_argument.get("operator_expression")))
    creative_value = _text(raw_argument.get("creative_core_value"))
    if not creative_value and not operator_wording_present:
        # Formal legacy strategies were already written as creative briefs and
        # do not carry a raw operator sentence.
        creative_value = _text(raw_argument.get("core_value"))
    return {
        "argument_id": _text(raw_argument.get("argument_id")),
        "source_argument_id": _text(raw_argument.get("source_argument_id")),
        "source_claim_ids": list(raw_argument.get("source_claim_ids") or []),
        "status": _text(raw_argument.get("status")),
        "mapping_status": _text(raw_argument.get("mapping_status")),
        "claim_type": _text(raw_argument.get("claim_type")),
        "claim_theme": _text(raw_argument.get("claim_theme")),
        "allowed_strength": _text(raw_argument.get("allowed_strength")),
        "visual_dependency": _text(raw_argument.get("visual_dependency")) or "FLEXIBLE",
        "compatible_carriers": list(raw_argument.get("compatible_carriers") or []),
        "proof_subject": _text(raw_argument.get("proof_subject")) or "GENERAL_EXPRESSION",
        "concept_ids": list(raw_argument.get("concept_ids") or []),
        "argument_theme": _text(raw_argument.get("argument_theme")),
        "primary_demonstration_mode": _text(
            raw_argument.get("primary_demonstration_mode")
        ),
        "supported_demonstration_modes": list(
            raw_argument.get("supported_demonstration_modes") or []
        ),
        "evidence_mode": _text(raw_argument.get("evidence_mode")),
        "preferred_action_mode": _text(raw_argument.get("preferred_action_mode")),
        "proof_action_intent": _text(raw_argument.get("proof_action_intent")),
        "required_proof_relation": _text(
            raw_argument.get("required_proof_relation")
        ),
        "display_quantity_contract": dict(
            raw_argument.get("display_quantity_contract") or {}
        ),
        "demonstration_policy": _text(raw_argument.get("demonstration_policy")),
        "creative_core_value": creative_value,
        # Compatibility alias for the prompt.  It is normalized creative
        # semantics, never the reviewed operator sentence.
        "core_value": creative_value,
        "core_proof_claim_keys": list(raw_argument.get("core_proof_claim_keys") or []),
        "optional_visual_claim_keys": list(raw_argument.get("optional_visual_claim_keys") or []),
        "expression_boundary": "RAW_OPERATOR_WORDING_RESERVED_FOR_CENTRAL_VOICEOVER",
    }


_WEARER_TERMS = (
    "上身", "试穿", "穿搭", "搭配", "腰线", "身材", "版型", "比例",
    "穿起来", "佩戴效果", "wearer", "try-on", "styling",
)


def _claim_needs_wearer(value: Any) -> bool:
    text = _text(value).lower()
    return any(term in text for term in _WEARER_TERMS)


def _reference_is_compatible(presentation: str, reference_carrier: str) -> bool:
    carrier = _text(reference_carrier).upper()
    allowed = {
        "PERSON_ON_CAMERA": {"WEARER_ACTIVE", "MIXED", "PERSON_ON_CAMERA"},
        "STATIC_PRODUCT": {"STATIC_PRODUCT"},
        "HANDS_ONLY": {"HAND_ONLY", "HANDS_ONLY", "MIXED"},
    }
    return carrier in allowed.get(presentation, set())


def _split_action_grammar(value: Any) -> List[str]:
    return _dedupe_text(
        re.split(r"\s*(?:→|->|—>|＞|>)\s*", _text(value)),
        limit=5,
    )


def _apparel_anchor_action_candidates(anchor_card: Dict[str, Any]) -> List[str]:
    """Reuse already-approved apparel shot/action text as the action source.

    Product anchor cards already contain executable display anchors, operation
    anchors and safe shot templates.  The old apparel fallback ignored all of
    them and promoted the scene template's ``action_grammar`` into the full
    action authority.  Keep the existing ``action_design`` surface, but feed
    it the product-safe material that is already available instead of adding
    another contract or validator.
    """

    contract = (
        anchor_card.get("category_execution_contract")
        if isinstance(anchor_card.get("category_execution_contract"), dict)
        else {}
    )
    display_family = _text(contract.get("display_family")).lower()
    if display_family != "apparel":
        return []

    candidates: List[str] = []
    for item in anchor_card.get("display_anchors") or []:
        if not isinstance(item, dict):
            continue
        anchor = _text(item.get("anchor"))
        shot = _text(item.get("recommended_shot_type"))
        # A bare product noun (for example "five front buttons") is an
        # identity anchor, not an executable action.  Consume display anchors
        # only when the anchor card already includes an approved shot form.
        value = "；".join(part for part in (anchor, shot) if part) if shot else ""
        if value:
            candidates.append(value)
    action_markers = (
        "可", "轻", "扶", "插", "整理", "转", "走", "展示", "拿", "放",
        "adjust", "turn", "walk", "hold", "show",
    )
    candidates.extend(
        _text(item) for item in (anchor_card.get("operation_anchors") or [])
        if _text(item)
        and any(marker in _text(item).lower() for marker in action_markers)
    )
    candidates.extend(
        _text(item) for item in (contract.get("safe_shot_templates") or [])
        if _text(item)
    )
    return _dedupe_text(candidates, limit=10)


_APPAREL_ACTION_INTENT_FAMILIES = (
    (
        "NECK_CLOSURE",
        ("高领", "立领", "领口", "脖子", "门襟", "前襟", "拉链", "拉起", "敞开", "防风"),
        ("高领", "立领", "领口", "脖子", "门襟", "前襟", "拉链", "整理", "轻扶"),
    ),
    (
        "LAYERING_SILHOUETTE",
        ("宽松", "臃肿", "叠穿", "内搭", "加衣服", "卫衣", "针织", "版型"),
        ("轮廓", "衣身", "侧面", "侧前", "背面", "袖筒", "下摆", "转身", "走动", "慢走"),
    ),
    (
        "BODY_PROPORTION",
        ("短款", "小个子", "身高", "腿长", "腿部", "腰线", "比例", "利落"),
        ("短款", "下摆", "全身", "半身", "腰线", "比例", "侧前", "转身"),
    ),
)


def _select_apparel_anchor_action(
    candidates: List[str],
    *,
    semantic_context: Dict[str, Any] | None,
    fallback_material: Dict[str, Any],
) -> Tuple[str, bool]:
    """Soft-rank existing approved actions by the selected selling point.

    This is deliberately not another action policy.  The candidate set still
    comes exclusively from the product anchor card; semantic relevance only
    decides which already-approved candidate is consumed.  When no useful
    relation is found, selection falls back to the previous deterministic
    hash so broad lifestyle arguments keep their original behaviour.
    """

    if not candidates:
        return "", False
    context = semantic_context if isinstance(semantic_context, dict) else {}
    semantic_text = "；".join(
        _dedupe_text(
            [
                context.get("core_buying_reason"),
                context.get("source_argument_text"),
                context.get("creative_core_value"),
                context.get("claim_theme"),
                context.get("argument_theme"),
                context.get("proof_action_intent"),
                *(context.get("core_proof_texts") or []),
            ],
            limit=12,
        )
    ).lower()
    core_proof_texts = [
        _text(value).lower()
        for value in (context.get("core_proof_texts") or [])
        if _text(value)
    ]

    scored: List[Tuple[int, str]] = []
    for candidate in candidates:
        candidate_text = _text(candidate).lower()
        score = 0
        # Exact reuse of the already-selected proof atom is the strongest and
        # most general relation available in the current contracts.
        for proof_text in core_proof_texts:
            if proof_text and (
                proof_text in candidate_text or candidate_text in proof_text
            ):
                score += 20
        # These three broad apparel relations cover closure, silhouette and
        # proportion without inventing product-specific actions.
        for _, source_terms, candidate_terms in _APPAREL_ACTION_INTENT_FAMILIES:
            if any(term in semantic_text for term in source_terms):
                score += 6 * sum(
                    1 for term in candidate_terms if term in candidate_text
                )
        scored.append((score, candidate))

    best_score = max(score for score, _ in scored)
    pool = [candidate for score, candidate in scored if score == best_score]
    material = dict(fallback_material)
    if best_score > 0:
        material["semantic_best_score"] = best_score
        material["semantic_pool"] = pool
    digest = hashlib.sha256(
        json.dumps(material, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).digest()
    return pool[digest[0] % len(pool)], best_score > 0


def _retrieval_execution_metadata(
    retrieval_reference_contract: Dict[str, Any] | None,
) -> Dict[str, Any]:
    """Return execution-only metadata from the already-selected real case."""

    contract = (
        retrieval_reference_contract
        if isinstance(retrieval_reference_contract, dict)
        else {}
    )
    if _text(contract.get("status")).upper() != "AVAILABLE":
        return {}
    primary = contract.get("primary_execution_card") or contract.get("primary_case")
    primary = primary if isinstance(primary, dict) else {}
    card = primary.get("execution_card") or primary.get("reference_execution_spine")
    card = card if isinstance(card, dict) else {}
    if not card:
        return {}
    return {
        "execution_card_id": _text(
            card.get("execution_card_id") or card.get("reference_spine_id")
        ),
        "physical_action_type": _text(card.get("physical_action_type")).upper(),
        "shot_count": int(card.get("shot_count") or 0),
        "available_parts": [
            _text(value).lower() for value in (card.get("available_parts") or [])
            if _text(value)
        ],
        "rhythm_logic": _text(card.get("rhythm_logic")),
    }


def _compile_action_design(
    *,
    creative_contract: Dict[str, Any],
    structure_contract: Dict[str, Any],
    presentation: str,
    carrier_execution: Dict[str, Any],
    proof_subject: str,
    preferred_action_mode: str = "",
    action_semantic_context: Dict[str, Any] | None = None,
    anchor_card: Dict[str, Any] | None = None,
    retrieval_reference_contract: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Freeze one executable action spine without adding another model stage.

    Category adapters own safe product interactions.  The creative contract's
    existing action grammar remains a supporting real-life movement instead
    of being expanded into a second checklist.  The choice is deterministic,
    so retries and snapshot replays keep the same action.
    """

    grammar = _split_action_grammar(creative_contract.get("action_grammar"))
    opening = _text(creative_contract.get("opening_action"))
    capabilities = [
        dict(item)
        for item in carrier_execution.get("interaction_capabilities") or []
        if isinstance(item, dict) and _text(item.get("interaction_id"))
    ]
    macro = _macro_structure(structure_contract)
    proof = _text(proof_subject).upper()
    anchor_candidates = (
        _apparel_anchor_action_candidates(dict(anchor_card or {}))
        if presentation == "PERSON_ON_CAMERA"
        else []
    )
    retrieval_meta = _retrieval_execution_metadata(
        retrieval_reference_contract
    )
    selected: Dict[str, Any] = {}
    if capabilities:
        preferred_mode = _text(
            preferred_action_mode
            or carrier_execution.get("preferred_action_mode")
        ).upper()
        if not preferred_mode and "USE_PROCESS" in macro:
            preferred_mode = "SIMPLE_WEAR_PROCESS"
        elif not preferred_mode and (proof == "PRODUCT_DETAIL" or any(
            term in "；".join(grammar)
            for term in ("细节", "局部", "图案", "边缘", "拿近", "近看")
        )):
            preferred_mode = "DETAIL_SHOW"
        preferred = [
            item
            for item in capabilities
            if _text(item.get("primary_action_mode")).upper() == preferred_mode
        ]
        pool = preferred or capabilities
        material = {
            "opening": opening,
            "grammar": grammar,
            "macro": macro,
            "proof": proof,
            "presentation": presentation,
            "capabilities": [item.get("interaction_id") for item in pool],
        }
        digest = hashlib.sha256(
            json.dumps(material, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).digest()
        continuous_motion_pool = (
            len(pool) > 1
            and all(
                _text(item.get("motion_scope")).upper()
                == "ONE_CONTINUOUS_CHANGE"
                for item in pool
            )
        )
        if continuous_motion_pool and _text(creative_contract.get("contract_id")):
            # The direction contract is already frozen and unique per item.
            # Use a separate digest byte so small-accessory motion families
            # rotate across directions without a state table or quota rule.
            rotation_digest = hashlib.sha256(
                _text(creative_contract.get("contract_id")).encode("utf-8")
            ).digest()
            selected = dict(pool[rotation_digest[1] % len(pool)])
        else:
            selected = dict(pool[digest[0] % len(pool)])

    if selected:
        supporting = "→".join(grammar)
        selected_schema_version = (
            _text(selected.get("schema_version"))
            or "action-design-v1"
        )
        selected.update(
            {
                "schema_version": selected_schema_version,
                "supporting_scene_action": supporting,
                "source": "CATEGORY_CAPABILITY",
                "selection_policy": (
                    "action-variety-v2-direction-rotation"
                    if continuous_motion_pool
                    else "action-variety-v1"
                ),
                "hard_required": False,
            }
        )
    elif anchor_candidates:
        # For person-led apparel, the product anchor card owns the safe
        # product-facing action.  The real case contributes measured rhythm
        # and clip availability; the scene combination remains context only.
        # This replaces the old scene-template action chain rather than adding
        # a second proof-action layer.
        material = {
            "contract_id": _text(creative_contract.get("contract_id")),
            "scene_motif": _text(creative_contract.get("scene_motif")),
            "proof": proof,
            "macro": macro,
            "execution_card_id": _text(retrieval_meta.get("execution_card_id")),
            "candidates": anchor_candidates,
        }
        core, semantic_match = _select_apparel_anchor_action(
            anchor_candidates,
            semantic_context=action_semantic_context,
            fallback_material=material,
        )
        scene_motif = _text(creative_contract.get("scene_motif"))
        selected = {
            "schema_version": "action-design-v2-existing-authority-rewire",
            "interaction_id": "APPAREL_PRODUCT_ANCHOR_ACTION",
            "primary_action_mode": "LIFESTYLE_USE",
            "start_state": (
                f"商品已经穿好，人物处于{scene_motif}的当前生活时刻"
                if scene_motif
                else "商品已经穿好，人物处于当前真实生活时刻"
            ),
            "core_action": core,
            "end_state": "保持同一人物、商品和场景连续，商品仍清楚可见，自然结束这一段分享",
            "supporting_scene_action": "",
            "risk_tier": "LOW",
            "action_keywords": _dedupe_text([core], limit=4),
            "source": (
                "PRODUCT_ANCHOR_WITH_REAL_EXECUTION"
                if retrieval_meta else "PRODUCT_ANCHOR"
            ),
            "selection_policy": (
                "existing-action-design-v3-selling-point-soft-match"
                if semantic_match
                else "existing-action-design-v2-anchor-consumption"
            ),
            "execution_reference": retrieval_meta,
            "scene_action_authority": "CONTEXT_ONLY",
            "hard_required": False,
        }
    else:
        # Apparel and legacy categories continue to use the already allocated
        # creative grammar.  This adds structure, not a new action invention.
        start = grammar[0] if grammar else opening
        core = grammar[1] if len(grammar) > 1 else (opening or start)
        end = grammar[-1] if len(grammar) > 1 else "保持商品清楚可见，自然结束这一小段分享"
        selected = {
            "schema_version": "action-design-v1",
            "interaction_id": "CREATIVE_ACTION_GRAMMAR",
            "primary_action_mode": (
                "HANDHELD_PRODUCT"
                if presentation in {"HANDS_ONLY", "STATIC_PRODUCT"}
                else "LIFESTYLE_USE"
            ),
            "start_state": start or "商品和当前人物或展示面关系已经建立",
            "core_action": core or "保持同一生活时刻，自然展示商品",
            "end_state": end,
            "supporting_scene_action": "",
            "risk_tier": "LOW",
            "action_keywords": _dedupe_text([*grammar, opening], limit=4),
            "source": "CREATIVE_ACTION_GRAMMAR",
            "selection_policy": "action-variety-v1",
            "hard_required": False,
        }
    selected["action_signature"] = _stable_id(
        "ACT_",
        {
            "interaction_id": selected.get("interaction_id"),
            "start": selected.get("start_state"),
            "core": selected.get("core_action"),
            "end": selected.get("end_state"),
            "supporting": selected.get("supporting_scene_action"),
        },
    )
    selected["claim_action_contract"] = {
        "policy_version": "small-accessory-claim-action-v1",
        "proof_action_intent": _text(
            carrier_execution.get("proof_action_intent")
        ),
        "requested_action_mode": _text(
            preferred_action_mode or carrier_execution.get("preferred_action_mode")
        ).upper(),
        "selected_action_mode": _text(
            selected.get("primary_action_mode")
        ).upper(),
        "required_proof_relation": _text(
            carrier_execution.get("required_proof_relation")
        ),
        "compatibility": (
            "MATCHED"
            if not _text(preferred_action_mode or carrier_execution.get("preferred_action_mode"))
            or _text(selected.get("primary_action_mode")).upper()
            == _text(preferred_action_mode or carrier_execution.get("preferred_action_mode")).upper()
            else "SOFT_FALLBACK"
        ),
        "hard_required": False,
        "may_trigger_retry": False,
    }
    return selected


def build_simplified_creative_seed(
    *,
    anchor_card: Dict[str, Any],
    structure_contract: Dict[str, Any],
    content_bundle: Dict[str, Any],
    creative_contract: Dict[str, Any],
    execution_reference: Dict[str, Any],
    requested_hook_id: str,
    content_angle_key: str,
    relationship_device: str = "",
    product_type: str = "",
    top_category: str = "",
    retrieval_reference_contract: Dict[str, Any] | None = None,
    category_execution_extension: Dict[str, Any] | None = None,
    creator_recording_profile: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Freeze only the information needed by the simplified generator."""

    hard = structure_contract.get("hard_constraints") if isinstance(structure_contract.get("hard_constraints"), dict) else {}
    source_carrier = _text(hard.get("content_carrier") or execution_reference.get("content_carrier"))
    presentation, presentation_reason = _preferred_presentation(
        source_carrier=source_carrier,
        bundle=content_bundle,
        product_type=product_type,
        top_category=top_category,
        anchor_card=anchor_card,
    )
    capture_mode = _capture_mode_for_presentation(presentation)
    if category_execution_extension is None:
        from core.category_execution import compile_category_execution_extension

        category_execution_extension = compile_category_execution_extension(
            product_type=product_type,
            top_category=top_category,
            anchor_card=anchor_card,
        )
    else:
        category_execution_extension = dict(category_execution_extension or {})
    carrier_specific_execution: Dict[str, Any] = {}
    if category_execution_extension:
        from core.category_execution import resolve_category_carrier_execution

        carrier_specific_execution = resolve_category_carrier_execution(
            category_execution_extension,
            presentation_mode=presentation,
        )
        # Freeze the part evidence next to the frozen contract.  This is the one
        # place that holds both the contract and the approved anchors, and the
        # answer has to be shared by the blueprint guidance and the final prompt
        # renderer -- deriving it twice is how the two can drift apart.  A
        # no-op without a mixed contract, so no other path changes.
        from core.accessory_mixed_templates import (
            anchor_evidence_texts,
            attach_mixed_part_evidence,
        )

        attach_mixed_part_evidence(
            category_execution_extension,
            anchor_texts=anchor_evidence_texts(anchor_card),
        )
    claim_atoms = [
        {
            "claim_key": _text(item.get("claim_key")),
            "fact_text": _text(item.get("fact_text")),
            "role": _text(item.get("role")) or "visual_proof",
            "semantic_group": _text(item.get("semantic_group")),
        }
        for item in content_bundle.get("claim_atoms") or []
        if isinstance(item, dict) and _text(item.get("claim_key")) and _text(item.get("fact_text"))
    ]
    if presentation == "STATIC_PRODUCT":
        static_claims = [
            item for item in claim_atoms if not _claim_needs_wearer(item.get("fact_text"))
        ]
        if static_claims:
            claim_atoms = static_claims
    identity_anchors = _anchor_texts(anchor_card, "hard_anchors")
    visible_anchors = _dedupe_text(
        [
            *_anchor_texts(anchor_card, "display_anchors"),
            *[item.get("fact_text") for item in claim_atoms],
        ],
        limit=10,
    )
    forbidden = _dedupe_text(
        [
            *(anchor_card.get("distortion_alerts") or []),
            *(content_bundle.get("forbidden_inferences") or []),
            *(execution_reference.get("do_not_invent") or []),
        ],
        limit=12,
    )
    ref_carrier = _text(execution_reference.get("content_carrier") or source_carrier)
    explicit_reference_status = _text(execution_reference.get("reference_status"))
    has_observed_reference = (
        explicit_reference_status != "STRUCTURE_ONLY"
        and (
            explicit_reference_status == "VIDEO_REFERENCED"
            or bool(_text(execution_reference.get("execution_card_id")))
            or bool(execution_reference.get("action_spine"))
            or bool(execution_reference.get("camera_grammar"))
            # Legacy frozen packages predate reference_status.  Preserve their
            # prior carrier-only behaviour.
            or not explicit_reference_status
        )
    )
    compatible = has_observed_reference and _reference_is_compatible(
        presentation, ref_carrier
    )
    optional_reference = {
        "status": (
            "AVAILABLE"
            if compatible
            else "UNAVAILABLE_STRUCTURE_ONLY"
            if _text(execution_reference.get("reference_status")) == "STRUCTURE_ONLY"
            else "SKIPPED_INCOMPATIBLE"
        ),
        "content_carrier": ref_carrier,
        "action_spine": execution_reference.get("action_spine") or execution_reference.get("action_sequence") or [],
        "camera_grammar": execution_reference.get("camera_grammar") or execution_reference.get("camera_sequence") or [],
        "visual_hook_type": _text(execution_reference.get("visual_hook_type")),
        "usage_boundary": "只借鉴镜头节奏与承载关系，不复制原视频动作，不为卖点编触发事件",
    }
    if not compatible:
        optional_reference["action_spine"] = []
        optional_reference["camera_grammar"] = []
        optional_reference["visual_hook_type"] = ""

    raw_value = content_bundle.get("value_proposition") or {}
    raw_argument = (
        content_bundle.get("selling_argument")
        if isinstance(content_bundle.get("selling_argument"), dict)
        else {}
    )
    compatible_carriers = {
        _text(value).upper() for value in raw_argument.get("compatible_carriers") or []
    }
    safe_argument = _visual_selling_argument_view(raw_argument)
    argument_is_available = _text(raw_argument.get("status")) == "AVAILABLE"
    presentation_carriers = {
        "PERSON_ON_CAMERA": {"WEARER_ACTIVE", "MIXED", "PERSON_ON_CAMERA"},
        "STATIC_PRODUCT": {"STATIC_PRODUCT", "MIXED"},
        "HANDS_ONLY": {"HAND_ONLY", "HANDS_ONLY", "MIXED"},
    }.get(presentation, {presentation})
    argument_compatible = not compatible_carriers or bool(
        presentation_carriers & compatible_carriers
    )
    # This should already be MATCHED because the allocator filtered explicit
    # mismatches.  Keeping the status in the frozen seed makes old snapshots
    # diagnosable without letting the visual model change carrier authority.
    if argument_is_available:
        safe_argument["carrier_match_status"] = (
            "MATCHED" if argument_compatible else "UNMATCHED"
        )
    mainline = (
        _text(safe_argument.get("creative_core_value"))
        if argument_is_available
        else _text(raw_value.get("text")) if isinstance(raw_value, dict) else ""
    )
    safe_value = {
        "status": "AVAILABLE" if argument_is_available else _text(raw_value.get("status")),
        "text": mainline,
        "authority": "NORMALIZED_CREATIVE_SEMANTICS" if mainline else "UNAVAILABLE_TO_VISUAL_MODEL",
        "allowed_strength": _text(safe_argument.get("allowed_strength")),
    }
    if not mainline:
        # A visual fact may guide what is shown, but it must not silently
        # replace an operator-maintained selling point as the content thesis.
        mainline = ""
    raw_scene_reference = (
        creative_contract.get("scene_reference_contract")
        if isinstance(creative_contract.get("scene_reference_contract"), dict)
        else {}
    )
    raw_execution_card = (
        raw_scene_reference.get("scene_execution_card")
        if isinstance(raw_scene_reference.get("scene_execution_card"), dict)
        else {}
    )
    raw_space = (
        raw_execution_card.get("space")
        if isinstance(raw_execution_card.get("space"), dict)
        else {}
    )
    raw_scene_recipe = (
        raw_execution_card.get("visual_scene_recipe")
        if isinstance(raw_execution_card.get("visual_scene_recipe"), dict)
        else {}
    )
    raw_scene_request = (
        raw_execution_card.get("scene_request")
        if isinstance(raw_execution_card.get("scene_request"), dict)
        else raw_scene_reference.get("scene_request")
        if isinstance(raw_scene_reference.get("scene_request"), dict)
        else {}
    )
    # The matrix is a scene-realism hint, not a second content source.  Keep
    # only the small execution card needed to make a real phone-recordable
    # micro-space. Source ids, counts, raw source prompts, and product facts
    # never reach the visual model.
    scene_reference = {
        "status": _text(raw_scene_reference.get("status")) or "UNAVAILABLE",
        "selection_mode": _text(raw_scene_reference.get("selection_mode")) or "NO_EFFECT",
        "scene_family_key": _text(raw_scene_reference.get("scene_family_key")),
        "prototype_name": _text(raw_scene_reference.get("prototype_name")),
        "realism_anchors": _dedupe_text(
            raw_scene_reference.get("approved_realism_anchors") or [], limit=2
        ),
        "instruction": (
            "仅作场景真实感提示；不新增人物动作、剧情、卖点或商品事实。"
            if _text(raw_execution_card.get("status")) == "AVAILABLE"
            else "不可用时忽略，不改变原有创意方向。"
        ),
        "scene_request": {
            key: _text(raw_scene_request.get(key))
            for key in (
                "canonical_product_type",
                "presentation_mode",
                "scene_intent",
                "time_light_need",
                "capture_mode",
                "country",
            )
        },
        "execution_card": {
            "status": _text(raw_execution_card.get("status")) or "UNAVAILABLE",
            "source_quality": _text(raw_execution_card.get("source_quality")),
            "prototype_name": _text(raw_execution_card.get("prototype_name")),
            "space": {
                key: _text(raw_space.get(key))
                for key in ("location", "subspace", "phone_placement", "subject_position", "background_depth")
            },
            "background_anchors": _dedupe_text(raw_execution_card.get("background_anchors") or [], limit=2),
            # Operational labels such as WORK_BREAK stay available for
            # provenance/routing but are not presented as visual aesthetics.
            "situation_tags": _dedupe_text(raw_execution_card.get("situation_tags") or [], limit=2),
            "aesthetic_anchors": _dedupe_text(raw_execution_card.get("aesthetic_anchors") or [], limit=2),
            "visual_scene_recipe": {
                key: _text(raw_scene_recipe.get(key))
                for key in (
                    "space_relationship",
                    "material_palette",
                    "lighting_texture",
                    "lived_in_detail",
                )
            },
            "lived_in_trace": _text(raw_execution_card.get("lived_in_trace")),
            "lighting": _text(raw_execution_card.get("lighting")),
            "avoid_overdesign": _text(raw_execution_card.get("avoid_overdesign")),
            "coherence_key": _text(raw_execution_card.get("coherence_key")),
            "instruction": "只补足同一场景来源中的空间、真实感与审美锚点，不改变商品事实、卖点主线、人物动作或结构。",
        },
    }
    persona_selection_contract = copy.deepcopy(
        creative_contract.get("persona_selection_contract") or {}
    )
    runtime_persona_role = _text(creative_contract.get("persona_role"))
    if (
        _text(persona_selection_contract.get("availability")) == "AVAILABLE"
        and runtime_persona_role
    ):
        projection = dict(
            persona_selection_contract.get("script_projection") or {}
        )
        persona_selection_contract["template_identity_text"] = _text(
            projection.get("identity")
        )
        projection["identity"] = runtime_persona_role
        persona_selection_contract["script_projection"] = projection
        persona_selection_contract["runtime_role_contract"] = {
            "current_role": runtime_persona_role,
            "authority": "FROZEN_CREATIVE_CONTEXT",
            "template_still_owns": [
                "appearance", "hair_makeup", "body_proportion", "reference_assets"
            ],
            "instruction": (
                "人物模板决定长相与整体气质；当前角色只描述这条视频里她正在做什么，"
                "不得把模板中的固定职业或旧场景带入本条视频。"
            ),
        }

    seed = {
        "schema_version": CREATIVE_SEED_SCHEMA_VERSION,
        "product_truth": {
            "product_identity": _text(
                anchor_card.get("product_name")
                or product_type
            ),
            "identity_anchors": identity_anchors,
            "visible_detail_anchors": visible_anchors,
            "approved_claims": claim_atoms,
            "value_proposition": safe_value,
            "selling_argument": safe_argument,
            "selling_argument_lineage": dict(
                content_bundle.get("selling_argument_lineage") or {}
            ),
            "display_quantity_contract": dict(
                safe_argument.get("display_quantity_contract") or {}
            ),
            "core_proof_claim_keys": list(safe_argument.get("core_proof_claim_keys") or []),
            "optional_visual_claim_keys": list(safe_argument.get("optional_visual_claim_keys") or []),
            "content_mode": (
                "SELLING_ARGUMENT"
                if _text(safe_argument.get("status")) == "AVAILABLE"
                else "FACTUAL_OBSERVATION"
            ),
            "content_mainline": mainline,
            "forbidden_inferences": forbidden,
        },
        # One lossless semantic spine is shared by visual planning and the
        # central voiceover.  Compact legacy summaries remain available for
        # compatibility, but they no longer own the meaning of this item.
        "semantic_spine_contract": dict(
            content_bundle.get("semantic_spine_contract") or {}
        ),
        "context_bridge_contract": dict(
            content_bundle.get("context_bridge_contract") or {}
        ),
        "creative_direction": {
            "content_angle_key": _text(content_angle_key),
            "requested_hook_id": _text(requested_hook_id),
            "macro_structure": _macro_structure(structure_contract),
            "preferred_presentation": presentation,
            "presentation_reason": presentation_reason,
            "capture_mode": capture_mode,
            "source_structure_carrier": source_carrier,
            "continuity_hint": _text(hard.get("continuity_mode")),
        },
        # This is a frozen surface preference for the central voiceover
        # engine.  It changes only the speaker-to-viewer relationship, never
        # product truth, visual design, validation, or retry behaviour.
        "voiceover_surface_contract": {
            "relationship_device": _text(relationship_device) or "HOOK_DECIDES",
            "speaker_position": (
                "CREATOR_TO_CAMERA"
                if capture_mode == CAPTURE_MODE_CREATOR_SELF_SHOT
                else "VOICEOVER_OVER_PRODUCT"
            ),
            "policy_version": "audience-relation-rotation-v2-capture-aware",
            "hard_required": False,
        },
        "diversity_context": {
            "preferred_persona_role": _text(creative_contract.get("persona_role")),
            "preferred_scene_motif": _text(creative_contract.get("scene_motif")),
            "preferred_surface_profile": dict(creative_contract.get("surface_profile") or {}),
            "outfit_selection_contract": dict(
                creative_contract.get("outfit_selection_contract") or {}
            ),
            "outfit_scene_affinity_contract": dict(
                creative_contract.get("outfit_scene_affinity_contract") or {}
            ),
            "persona_selection_contract": persona_selection_contract,
            "outfit_persona_affinity_contract": dict(
                creative_contract.get("outfit_persona_affinity_contract") or {}
            ),
            "avoid_patterns": _dedupe_text(
                [
                    *(creative_contract.get("anti_template_rules") or []),
                    *(creative_contract.get("forbidden_recent_patterns") or []),
                ],
                limit=10,
            ),
            "instruction": "这些只用于促成人物与场景差异，不要求设计剧情或卖点触发动作",
            "scene_reference": scene_reference,
        },
        "optional_visual_inspiration": optional_reference,
    }
    from core.multidim_reference_adapter import (
        model_visible_reference_projection,
    )

    seed["retrieval_reference"] = model_visible_reference_projection(
        retrieval_reference_contract
    )
    semantic_spine = (
        content_bundle.get("semantic_spine_contract")
        if isinstance(content_bundle.get("semantic_spine_contract"), dict)
        else {}
    )
    semantic_source = (
        semantic_spine.get("source_argument")
        if isinstance(semantic_spine.get("source_argument"), dict)
        else {}
    )
    semantic_thesis = (
        semantic_spine.get("script_thesis")
        if isinstance(semantic_spine.get("script_thesis"), dict)
        else {}
    )
    core_proof_keys = {
        _text(value)
        for value in (safe_argument.get("core_proof_claim_keys") or [])
        if _text(value)
    }
    action_semantic_context = {
        "core_buying_reason": _text(semantic_thesis.get("core_buying_reason")),
        "source_argument_text": _text(semantic_source.get("raw_text")),
        "creative_core_value": _text(safe_argument.get("creative_core_value")),
        "claim_theme": _text(safe_argument.get("claim_theme")),
        "argument_theme": _text(safe_argument.get("argument_theme")),
        "proof_action_intent": _text(safe_argument.get("proof_action_intent")),
        "core_proof_texts": [
            _text(item.get("fact_text"))
            for item in claim_atoms
            if _text(item.get("claim_key")) in core_proof_keys
        ],
    }
    action_design = _compile_action_design(
        creative_contract=creative_contract,
        structure_contract=structure_contract,
        presentation=presentation,
        carrier_execution=carrier_specific_execution,
        proof_subject=_text(safe_argument.get("proof_subject")),
        preferred_action_mode=_text(safe_argument.get("preferred_action_mode")),
        action_semantic_context=action_semantic_context,
        anchor_card=anchor_card,
        retrieval_reference_contract=retrieval_reference_contract,
    )
    seed["action_design"] = action_design
    seed["creative_direction"]["primary_action_mode"] = _text(
        action_design.get("primary_action_mode")
    )
    seed["creative_direction"]["action_signature"] = _text(
        action_design.get("action_signature")
    )
    seed["creative_direction"]["opening_visual_job"] = _opening_visual_job(
        seed["product_truth"],
        presentation=presentation,
        requested_hook_id=requested_hook_id,
    )
    capture_rhythm_contract = build_capture_rhythm_contract(
        capture_mode=capture_mode,
        macro_structure=seed["creative_direction"].get("macro_structure") or [],
        scene_context={
            "preferred_scene_motif": creative_contract.get("scene_motif"),
            "scene_reference": scene_reference,
        },
        retrieval_reference=seed.get("retrieval_reference")
        if isinstance(seed.get("retrieval_reference"), dict) else {},
        creator_recording_profile=creator_recording_profile,
    )
    if category_execution_extension:
        from core.category_execution import (
            project_category_capture_rhythm_contract,
        )

        # The category filming projection must consume the final frozen action
        # design. Supplying only the earlier carrier preference can make a
        # SIMPLE_WEAR_PROCESS blueprint disagree with result-only framing.
        carrier_specific_execution = dict(carrier_specific_execution)
        carrier_specific_execution["selected_action_design"] = dict(action_design)
        capture_rhythm_contract = project_category_capture_rhythm_contract(
            category_execution_extension,
            carrier_execution=carrier_specific_execution,
            capture_contract=capture_rhythm_contract,
        )
    retrieved_dimensions = (
        seed.get("retrieval_reference", {})
        .get("primary_real_case", {})
        .get("dimension_references", {})
    )
    retrieved_rhythm = (
        retrieved_dimensions.get("rhythm")
        if isinstance(retrieved_dimensions, dict)
        and isinstance(retrieved_dimensions.get("rhythm"), dict)
        else {}
    )
    if retrieved_rhythm and not bool(retrieved_rhythm.get("is_generic_rhythm")):
        capture_rhythm_contract["retrieved_rhythm_reference"] = {
            "production_family_id": _text(
                retrieved_rhythm.get("production_family_id")
            ),
            "production_family_name": _text(
                retrieved_rhythm.get("production_family_name")
            ),
            "production_brief": _text(
                retrieved_rhythm.get("production_brief")
            ),
            "authority_boundary": (
                "只控制既有结构内的拍摄单元速度和切换感觉，"
                "不得改变 macro_structure、商品动作或口播。"
            ),
        }
    seed["capture_rhythm_contract"] = capture_rhythm_contract
    seed["creative_direction"]["capture_rhythm_profile"] = _text(
        capture_rhythm_contract.get("profile")
    )
    from core.product_type_resolution import normalize_product_type

    canonical_product_type = normalize_product_type(
        product_type,
        top_category,
    ).canonical_type
    # Category adapters remain optional business logic.  The thin visual
    # contract below is shared by wearable categories and never adds a model
    # call, retry, or hard gate.
    if category_execution_extension:
        profile = (
            category_execution_extension.get("profile")
            if isinstance(category_execution_extension.get("profile"), dict)
            else {}
        )
        canonical_product_type = (
            _text(profile.get("product_subtype")) or canonical_product_type
        )
        seed["category_execution_extension"] = category_execution_extension
        seed["carrier_specific_execution"] = carrier_specific_execution
    if canonical_product_type:
        seed["product_truth"]["canonical_product_type"] = canonical_product_type
    from core.visual_execution_contract import build_visual_execution_contract
    from core.accessory_mixed_templates import frozen_mixed_contract

    # Review #7: the blueprint quotes the frozen environment recipe, so the
    # visual contract has to be built from the same frozen value.  Passing
    # nothing here made the function fall back to the default recipe, leaving one
    # model input with two different surfaces and light directions -- and the
    # first frame and the production prompt both inherit whichever one wins.
    seed_mixed_contract = frozen_mixed_contract(category_execution_extension)

    apparel_anchor_action = _text(action_design.get("interaction_id")) == (
        "APPAREL_PRODUCT_ANCHOR_ACTION"
    )
    visual_execution_contract = build_visual_execution_contract(
        canonical_product_type=canonical_product_type,
        presentation_mode=presentation,
        capture_mode=capture_mode,
        outfit_contract=seed["diversity_context"].get("outfit_selection_contract"),
        scene_reference=scene_reference,
        product_truth=seed["product_truth"],
        opening_visual_job=seed["creative_direction"].get("opening_visual_job"),
        action_design=action_design,
        # Once the existing action_design has consumed approved apparel
        # anchors, the hard-coded creative combination is only a scene/moment
        # selector.  Do not re-inject its old bag/standing/detail chain through
        # the visual contract and silently restore the former authority.
        suggested_opening_action=(
            "" if apparel_anchor_action
            else _text(creative_contract.get("opening_action"))
        ),
        suggested_event_flow=(
            "" if apparel_anchor_action
            else _text(creative_contract.get("action_grammar"))
        ),
        accessory_environment_recipe_id=_text(
            seed_mixed_contract.get("environment_recipe_id")
        ),
        accessory_frozen_contract=seed_mixed_contract,
    )
    if visual_execution_contract:
        seed["visual_execution_contract"] = visual_execution_contract
    seed["creative_seed_id"] = _stable_id("SCS_", seed)
    return seed


def build_simplified_script_prompt(
    seed: Dict[str, Any],
    *,
    target_country: str,
    target_language: str,
    duration_seconds: float,
) -> str:
    capture_mode = _text(
        seed.get("creative_direction", {}).get("capture_mode")
    ) or CAPTURE_MODE_CREATOR_SELF_SHOT
    schema = {
        "schema_version": VISUAL_SCRIPT_SCHEMA_VERSION,
        "script_concept": {
            "one_sentence_idea": "一句话创意",
            "viewer_need": "观众需求或观看理由",
            "hook_intent": "开头如何制造具体关注",
            "macro_structure": ["HOOK", "PROOF"],
        },
        "production_design": {
            "presentation_mode": "PERSON_ON_CAMERA|STATIC_PRODUCT|HANDS_ONLY",
            "capture_mode": (
                "CREATOR_SELF_SHOT|HANDS_PRODUCT_SHARE|STATIC_PRODUCT_RECORD"
            ),
            "character": {
                "persona_id": "必须继承冻结人物模板ID；不可用时留空",
                "identity": "人物身份；无人物则写不适用",
                "appearance": "年龄感、气质与可见外形",
                "hair_makeup": "发型妆容",
                "speaking_personality": "说话人格",
            },
            "outfit": {
                "base_outfit": "除目标商品外的完整基础穿搭",
                "product_role": "目标商品在造型中的位置",
                "accessories": "必要配饰，没有则写无",
            },
            "scene": {
                "location": "具体地点",
                "moment": "具体生活时刻",
                "lighting": "光线",
                "background": "背景陈设",
                "phone_placement": "第一段或主要片段的基础手机位置；后续拍摄单元允许在同一小片区域重新放置",
                "subject_position": "人物或商品在这一小段空间中的位置",
                "background_depth": "前景与背景保留的普通生活层次",
                "lived_in_trace": "一个自然出现的使用痕迹或随身物品；没有则写无",
            },
            "emotion": {
                "starting_state": "开场自然状态",
                "natural_change": "动作带来的轻微变化",
                "ending_state": "结尾状态",
            },
            "life_event": {
                "continuous_event": "这一条视频发生的同一个生活时刻",
                "starting_context": "开场时人物或商品正在什么状态",
                "action_progression": "按冻结action_design完成开始→核心动作→完成状态；允许1至2次自然过渡，不要求每镜变化",
                "ending_context": "同一时刻如何自然结束",
            },
            "action_execution": {
                "primary_action_mode": "必须等于冻结action_design.primary_action_mode",
                "start_state": "必须等于冻结action_design.start_state",
                "core_action": "必须等于冻结action_design.core_action",
                "end_state": "必须等于冻结action_design.end_state",
                "supporting_scene_action": "有则自然衔接；不得扩写成另一套动作清单",
            },
        },
        "product_usage": {
            "identity_anchors_preserved": ["必须原样来自授权锚点"],
            "selling_points_used": ["仅填写本条画面实际采用的approved_claims.claim_key；允许只选一部分"],
        },
        "storyboard": [
            {
                "shot_no": 1,
                "time_range": "0.0-2.0s",
                "visual_content": "包含人物/商品/场景关系的完整可见画面",
                "character_action": "具体可执行动作",
                "natural_emotion": "可见但不过度表演的情绪",
                "camera": "景别、机位、运动",
                "capture_unit_id": "按capture_rhythm_contract分配，如CU_01；模型建议会由代码确定性归并",
                "starts_new_take": True,
                "product_anchors_visible": ["来自授权锚点"],
                "supported_claim_keys": ["当前镜头实际支持的claim_key"],
                "narrative_role": "必须按capture_rhythm_contract.structure_unit_roles的顺序填写，不得自行补USE或ENDING",
            }
        ],
        "voiceover_context": {
            "viewer_relationship": "与观众的关系",
            "speaking_intent": "为什么此刻开口",
            "desired_tone": "自然口语语气",
        },
        "reference_realization": {
            "status": "APPLIED|PARTIAL|NOT_USED|UNAVAILABLE",
            "adopted_parts": ["opening|proof|use_process|ending"],
            "adaptation_notes": "简要说明借鉴了哪些镜头功能；没有使用则说明不兼容点",
        },
    }
    category_extension = (
        seed.get("category_execution_extension")
        if isinstance(seed.get("category_execution_extension"), dict)
        else {}
    )
    carrier_execution = (
        seed.get("carrier_specific_execution")
        if isinstance(seed.get("carrier_specific_execution"), dict)
        else {}
    )
    category_guidance = ""
    if category_extension:
        from core.category_execution import build_category_blueprint_guidance

        category_guidance = build_category_blueprint_guidance(
            category_extension,
            carrier_execution=carrier_execution,
        )
        schema["production_design"]["accessory_execution"] = {
            "wearing_zone": "必须等于冻结配饰执行档案",
            "required_view": "必须等于当前承载下的冻结结果视角",
            "interaction_boundary": ["只继承输入，不新增规则"],
            "identity_priority": ["只继承输入，不推断未知商品属性"],
        }
    category_guidance_block = (
        f"\n\n类目执行补充：\n{category_guidance}\n"
        if category_guidance
        else "\n"
    )
    capture_contract = (
        seed.get("capture_rhythm_contract")
        if isinstance(seed.get("capture_rhythm_contract"), dict)
        else build_capture_rhythm_contract(
            capture_mode=capture_mode,
            macro_structure=seed.get("creative_direction", {}).get("macro_structure") or [],
        )
    )
    if _text(capture_contract.get("profile")) == CAPTURE_RHYTHM_LEGACY:
        capture_guidance = (
            "本条使用旧版连续录制关系：保持一个主要手机视角和一个连续时刻。"
        )
    else:
        setup_mode = _text(capture_contract.get("capture_setup_mode")).upper()
        setup_guidance = (
            "公共场景使用一至两个自然可解释的手机位置，不要求固定、手持或镜面比例；"
            "不要反复架设和搬动无人值守手机。"
            if setup_mode == "ONE_PUBLIC_PHONE_POSITION_PLUS_HANDHELD_CUTAWAY"
            else "不同片段可以在同一小片区域自然补录。"
        )
        capture_guidance = (
            f"本条使用{_text(capture_contract.get('profile'))}：同一创作者或商品、同一地点、"
            f"同一时刻和同一部手机，分别录制{int(capture_contract.get('capture_unit_count') or 2)}段简短素材，"
            f"片段间普通直接剪切。{setup_guidance}"
            "人物、商品、穿搭、光线和生活状态保持连续；不是同一素材的数字裁切。"
        )
    model_visible_seed = _model_visible_creative_seed(seed)
    return f"""你是使用手机创作内容的短视频分享者与完整脚本作者。请为{target_country}市场生成一条约{duration_seconds:g}秒的原创商品短视频视觉脚本。

这不是分层规划题。请一次写出能够直接拍摄/生成的完整内容：人物、外形、穿搭、场景、自然状态和4至6个结构时间段必须同时成立。storyboard 继续表达结构内容段，并按 capture_rhythm_contract 编译成3至5个真实可见剪辑片段；成片片段数与手机布置数是两个概念，同一自然手机布置可以录制不同内容时刻。相同 capture_unit_id 内连续录制，不同 capture_unit_id 之间是直接剪切后的另一段手机素材。

拍摄关系：
{capture_guidance}{category_guidance_block}
核心原则：
1. 商品事实只能来自 product_truth；不知道的内容不补写，绝不虚构功效、材质、颜色或使用结果。
2. semantic_spine_contract.script_thesis 是本条内容语义权威，原始人工卖点保存在 source_argument，主情境与核心购买理由不得被场景、人物、穿搭或真实案例改写。context_bridge_contract 只决定整片画面与口播如何处于同一个消费世界，不要求逐句逐镜对齐。product_truth.content_mainline、selling_argument.creative_core_value 与 core_result_moment 都只是旧字段兼容投影；存在语义主干时不得用这些摘要覆盖它。approved_claims 只用作画面证据，禁止把第一个扣子、口袋或袖型细节改写成全片主题。content_mode=FACTUAL_OBSERVATION 时围绕可见事实做观察，不伪造用户痛点或产品收益。
3. creative_direction.macro_structure 是观看顺序权威，不规定统一镜头模板；capture_rhythm_contract.structure_unit_roles 是它展开到可见片段后的唯一Beat顺序。每个capture_unit按同序角色填写；为了增加片段可以重复已有PROOF/USE，但禁止补入原结构没有的USE、ENDING或其他Beat。类目执行的商品近景/动态/回收关系只改变拍摄方式，不得覆盖structure_unit_roles。requested_hook_id 只描述口播意图，本步骤不写{target_language}口播。
4. presentation_mode 必须等于 preferred_presentation，capture_mode 必须等于 creative_direction.capture_mode。PERSON_ON_CAMERA 必须写完整人物、穿搭、场景和自然状态；CREATOR_SELF_SHOT 中人物是正在对自己的手机镜头说话的创作者，不是被摄影团队拍摄的沉默模特。STATIC_PRODUCT 不虚构出镜人物或商品情绪；HANDS_ONLY 只允许手部进入画面。
5. action_design只在类目确实需要商品互动时提供一次简单动作；没有必要时人物可以只是面对自己的手机分享。不同片段不要求分别增加动作、情绪或生活事件，只要不是同一素材重复裁切即可。不要制造遮挡后揭示、通知弹出、道具机关或“恰好发现”等剧情。
6. 商品锚点与 claim_key 必须逐字从输入中选择。approved_claims 是可选事实池，不是拍摄清单：只选择当前结构自然需要的少量事实，未选事实无需安排镜头。被写入 selling_points_used 或 supported_claim_keys 的事实必须来自池内；同一事实只需全片有一处自然可见，不要求逐项触摸、指向或分配独立动作。本步骤不得决定中央口播最终选择哪些事实，也不得按口播逐句设计镜头。
7. 人物和场景要具体但克制，情绪是自然的小变化，不写广告演员式惊讶。CREATOR_SELF_SHOT 的场景只是分享发生的普通背景，不得扩写成走廊、电梯、室内外连续调度。context_bridge_contract.scene_relation=SUPPORTS 时让画面自然支持主情境；NEUTRAL 时只做不冲突的商品展示，不擅自增加另一种用途；CONFLICTS 不应进入新规划。diversity_context.scene_reference.scene_request 只说明当前商品展示所需的场景语义；其中 time_light_need=DAYLIGHT 时保持同一地点的白天自然光，不能改成夜间氛围。execution_card 若为 AVAILABLE，优先把它的 space 翻译为场景字段：写清手机放在哪里、人物与手机的自然相对位置、背景的前后层次，并自然保留至多两项 background_anchors 和一个 lived_in_trace。位置只用“靠近、旁边、前后、同一小片区域”等相对关系；输入没有实测值时，不写米、厘米、精确距离或精确机位高度。场景卡不是拍摄任务清单，道具不得变成必须触摸或使用的动作；照样只使用现场已有自然光或普通室内光。execution_card 不可用时按原有创意方向完成。
8. diversity_context.outfit_selection_contract 是本条生成前已经选定的穿搭合同。outfit_recipe 是本条唯一配方，其中非空的连体单品，或非空的上装与下装，以及鞋包和辅助配饰应被完整写入 production_design.outfit.base_outfit，不得重新选择、拆分或替换其中任一单品；one_piece 非空时必须按一件连体服装执行，top / bottom 应为空，不得把连衣裙或连体裤改写成上下装。target_role 只说明目标商品在整套造型中的角色，visibility_zones 用于保持商品可见。source_type=LIGHTWEIGHT_TEMPLATE 时，只执行合同里已经标准化的结构化字段和 base_outfit_direction；不得猜测或索取模板标题、正文、prompt_core、notes。source_type=INTERNAL_PROFILE 时，结合 silhouette_key、style_family、style_intensity、outfit_recipe 和 base_outfit_direction形成可感知的完整轮廓；outer_layer_direction / neckline_direction / hair_direction / palette_relation / visibility_requirement 只作柔性设计参考，不增加独立动作或质检门槛。允许因真实场景做轻微自然调整，但不要仅换颜色后重新回到近期相同的“基础上衣＋长裤”组合。preferred_surface_profile 只作旧字段兼容。全片保持一个连续、普通的生活时刻，但允许分成多个手机拍摄单元；不要默认写成“靠近镜头→退后展示→整理衣服→微笑收尾”的固定动作链。
8.1 diversity_context.persona_selection_contract 若 availability=AVAILABLE，人物长相、年龄感、体型、发型妆容和参考资产由 persona_id、identity_lock 与参考资产共同拥有权威；script_projection.identity 已是为当前脚本冻结的临时角色，必须使用它，禁止把 template_identity_text 中的旧职业或旧场景带回当前视频。production_design.character 逐项继承 script_projection。商品参考图只决定商品，不得继承其中模特的脸、年龄、体型、发型、滤镜、姿态或背景。模型只可按当前生活时刻调整自然表情、视线和小动作，不得重新设计人物。若合同为 UNAVAILABLE/NOT_APPLICABLE，沿用普通创作者设计且不伪装成已使用人物库。
10. 类目执行补充中的 interaction_boundary 是全片一次生效的物理边界。storyboard 只写这一镜实际发生的正向动作，不要在每个镜头反复写“不得、禁止、不缠绕、不重新系”等负向规则。若冻结了 selected_action_design，按它执行一个核心商品互动；不得再把 optional_simple_interactions 当作候选清单逐项加入。若冻结了 primary_demonstration_mode，本条只表现这一种用法；supported_demonstration_modes 只是授权范围，不是镜头清单。
10.1 product_truth.display_quantity_contract 只有 status=AUTHORIZED 时才允许出现多只同款商品。此时 required_display_count 是全片唯一数量权威：这组同款不是“竞争性配饰”，人物穿搭与每个拍摄单元都必须保持该数量，不得从单戴切成叠戴、边拍边增加或中途摘下。字段为空时继续默认只出现一只目标商品，禁止模型自行复制。
11. 若输入含 visual_execution_contract，只执行已经选定的人物、穿搭、场景、清楚曝光和商品分离关系。精致感来自真实造型和空间，不来自磨皮、影棚布光或电影运镜；这些视觉信息不得扩写成新的动作、剧情或首镜表演任务。
11.1 retrieval_reference.status=AVAILABLE 时，primary_real_case.execution_card 是同一条真实视频反推得到的执行示范。opening / proof / use_process / ending 只能按各自镜头功能使用；status=UNAVAILABLE 的段落必须保持缺失，不得从其他镜头补造。优先自然借鉴其动作顺序、景别变化、拍摄单元关系与节奏，再用当前商品、卖点、人物、穿搭和场景重新设计；来源具体动作不兼容时，要为同一功能换成当前商品能够成立的可见动作、观察角度或事件状态，不能删完具体执行后只剩“正面站着说→更近一点站着说”。supporting_real_case 只用于补充同功能段的另一种执行可能。五维标签已锁在 same_video_dimension_bundle 中，禁止拆开重组。口播聚类池不会进入本视觉步骤，只在后续中央口播中使用。不得照抄来源商品外观、人物身份、穿搭、品牌文字、价格、具体宣称、CTA原句或不兼容场景。只有实际转化为本条可见变化的功能段才写入reference_realization.adopted_parts；只继承opening/proof/use_process/ending名称不算采用。该字段只用于观察，不影响脚本通过、修订或重试。authority_boundary 必须遵守：当前 product_truth、selling_argument、人物模板、穿搭模板、已选场景、macro_structure 和中央口播仍拥有最终权威。
12. 只返回一个JSON对象，不要Markdown，不要解释。字段齐全，结构如下：
{json.dumps(schema, ensure_ascii=False, indent=2)}

冻结输入：
{json.dumps(model_visible_seed, ensure_ascii=False, indent=2)}
"""


def _model_visible_creative_seed(seed: Dict[str, Any]) -> Dict[str, Any]:
    """Hide outfit audit alternatives after one concrete recipe is frozen.

    The full seed stays persisted for lineage.  The blueprint model only needs
    the selected recipe; showing both ``吊带`` and the audited source
    ``吊带或短袖`` re-opened a decision that code had already made.
    """

    # Frozen seeds normally come back from SQLite as JSON strings, but a
    # provider may still attach datetime metadata during an in-process run.
    # Preserve that metadata as text at the model boundary rather than
    # failing before the visual-script call.
    visible = json.loads(json.dumps(seed, ensure_ascii=False, default=str))
    diversity = (
        visible.get("diversity_context")
        if isinstance(visible.get("diversity_context"), dict)
        else {}
    )
    contract = (
        diversity.get("outfit_selection_contract")
        if isinstance(diversity.get("outfit_selection_contract"), dict)
        else {}
    )
    # The outfit/scene score is routing lineage, not creative material.  Keep
    # it in the persisted seed and final brief, but do not ask the model to
    # interpret an internal ranking decision it cannot improve.
    diversity.pop("outfit_scene_affinity_contract", None)
    creative_direction = (
        visible.get("creative_direction")
        if isinstance(visible.get("creative_direction"), dict)
        else {}
    )
    # Opening jobs and per-cut change jobs remain persisted for reporting, but
    # no longer compete with structure, category action and the actual clip
    # prose inside the visual model prompt.
    creative_direction.pop("opening_visual_job", None)
    visual_contract = (
        visible.get("visual_execution_contract")
        if isinstance(visible.get("visual_execution_contract"), dict)
        else {}
    )
    visual_saliency = (
        visual_contract.get("visual_saliency")
        if isinstance(visual_contract.get("visual_saliency"), dict)
        else {}
    )
    # ``opening_focus`` and ``opening_scene_projection`` are both derived from
    # the persisted opening job.  Sending either one would silently restore the
    # same instruction under another name, so keep them for lineage only.
    visual_saliency.pop("opening_focus", None)
    visual_contract.pop("opening_scene_projection", None)
    capture_contract = (
        visible.get("capture_rhythm_contract")
        if isinstance(visible.get("capture_rhythm_contract"), dict)
        else {}
    )
    capture_contract.pop("observable_change_jobs", None)
    richness = (
        capture_contract.get("shot_richness_contract")
        if isinstance(capture_contract.get("shot_richness_contract"), dict)
        else {}
    )
    for key in (
        "visible_change_axes",
        "minimum_changed_axes_per_cut",
        "change_policy",
        "priority_order",
    ):
        richness.pop(key, None)
    recipe = contract.get("outfit_recipe") if isinstance(contract.get("outfit_recipe"), dict) else {}
    if any(_text(value) for value in recipe.values()):
        projection = _outfit_prompt_projection(contract)
        for key in (
            "source_outfit_recipe",
            "source_accessory_items",
            "inner_type",
            "inner_color",
            "bottom_type",
            "bottom_color",
            "bottom_fit",
            "accessory_level",
            "footwear_visibility",
            "applicable_product_codes",
            "applicable_product_type",
            "ignored_unstructured_fields",
            "structured_fields_used",
            "normalization_warnings",
            "template_display_name",
        ):
            contract.pop(key, None)
        # Keep one compact direction for prose continuity, but make it the
        # deterministic projection of the recipe instead of the raw options.
        contract["base_outfit_direction"] = _text(
            projection.get("frozen_outfit")
        )
        legacy = diversity.get("preferred_surface_profile")
        if isinstance(legacy, dict):
            legacy["base_outfit_direction"] = contract["base_outfit_direction"]
    return visible


def normalize_simplified_visual_script(
    raw: Dict[str, Any],
    seed: Dict[str, Any],
    *,
    generation_provenance: Dict[str, Any],
) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    script = dict(raw)
    script["schema_version"] = VISUAL_SCRIPT_SCHEMA_VERSION
    script["creative_seed_id"] = _text(seed.get("creative_seed_id"))
    script["allocated_direction"] = dict(seed.get("creative_direction") or {})
    production = (
        dict(script.get("production_design"))
        if isinstance(script.get("production_design"), dict)
        else {}
    )
    # Capture ownership is frozen before generation.  Missing or drifted model
    # output is normalized rather than sent through another repair loop.
    production["capture_mode"] = _text(
        seed.get("creative_direction", {}).get("capture_mode")
    ) or _capture_mode_for_presentation(production.get("presentation_mode"))
    persona_contract = (
        dict((seed.get("diversity_context") or {}).get("persona_selection_contract") or {})
        if isinstance(seed.get("diversity_context"), dict)
        else {}
    )
    outfit_persona_affinity = (
        dict(
            (seed.get("diversity_context") or {}).get(
                "outfit_persona_affinity_contract"
            ) or {}
        )
        if isinstance(seed.get("diversity_context"), dict)
        else {}
    )
    if _text(persona_contract.get("availability")) == "AVAILABLE":
        character = (
            dict(production.get("character") or {})
            if isinstance(production.get("character"), dict)
            else {}
        )
        projection = (
            persona_contract.get("script_projection")
            if isinstance(persona_contract.get("script_projection"), dict)
            else {}
        )
        character["persona_id"] = _text(persona_contract.get("persona_id"))
        for key in ("identity", "appearance", "hair_makeup", "speaking_personality"):
            if _text(projection.get(key)):
                character[key] = _text(projection.get(key))
        production["character"] = character
        production["persona_selection_contract"] = persona_contract
        script["persona_selection_contract"] = persona_contract
    if outfit_persona_affinity:
        production["outfit_persona_affinity_contract"] = (
            outfit_persona_affinity
        )
        script["outfit_persona_affinity_contract"] = (
            outfit_persona_affinity
        )
    frozen_outfit_contract = (
        dict((seed.get("diversity_context") or {}).get("outfit_selection_contract") or {})
        if isinstance(seed.get("diversity_context"), dict)
        else {}
    )
    if frozen_outfit_contract and _text(production.get("presentation_mode")).upper() != "STATIC_PRODUCT":
        from core.outfit_selection import upgrade_outfit_structure_contract

        frozen_outfit_contract = upgrade_outfit_structure_contract(
            frozen_outfit_contract
        )
        frozen_outfit_projection = _outfit_prompt_projection(
            frozen_outfit_contract
        )
        frozen_outfit_text = _text(
            frozen_outfit_projection.get("frozen_outfit")
        )
        if frozen_outfit_text:
            production_outfit = (
                dict(production.get("outfit") or {})
                if isinstance(production.get("outfit"), dict)
                else {}
            )
            production_outfit["base_outfit"] = frozen_outfit_text
            accessory_items = list(
                frozen_outfit_contract.get("accessory_items") or []
            )
            if accessory_items:
                production_outfit["accessories"] = "；".join(
                    _text(item) for item in accessory_items if _text(item)
                )
            production["outfit"] = production_outfit
    action_design = (
        dict(seed.get("action_design") or {})
        if isinstance(seed.get("action_design"), dict)
        else {}
    )
    if action_design:
        production["action_execution"] = dict(action_design)
    category_extension = (
        seed.get("category_execution_extension")
        if isinstance(seed.get("category_execution_extension"), dict)
        else {}
    )
    if category_extension:
        from core.category_execution import resolve_category_carrier_execution

        carrier_execution = (
            dict(seed.get("carrier_specific_execution") or {})
            if isinstance(seed.get("carrier_specific_execution"), dict)
            else {}
        )
        if not carrier_execution:
            carrier_execution = resolve_category_carrier_execution(
                category_extension,
                presentation_mode=_text(production.get("presentation_mode"))
                or _text(seed.get("creative_direction", {}).get("preferred_presentation")),
            )
        # The adapter owns physical execution.  Model output cannot redefine
        # the wearing zone or interaction boundary.
        production["accessory_execution"] = carrier_execution
        script["category_execution_extension"] = dict(category_extension)
    scene = dict(production.get("scene") or {}) if isinstance(production.get("scene"), dict) else {}
    # Scene references contain no measured geometry.  Strip model-invented
    # precision deterministically instead of adding another validation/retry.
    for field in ("phone_placement", "subject_position", "background_depth"):
        value = _text(scene.get(field))
        value = re.sub(
            r"(?<![A-Za-z0-9])\d+(?:\.\d+)?\s*(?:米|厘米|公分|cm|m)(?![A-Za-z])",
            "自然距离",
            value,
            flags=re.IGNORECASE,
        )
        scene[field] = value
    if scene:
        production["scene"] = scene
    visual_contract = (
        dict(seed.get("visual_execution_contract") or {})
        if isinstance(seed.get("visual_execution_contract"), dict)
        else {}
    )
    if visual_contract:
        life_event = (
            dict(production.get("life_event") or {})
            if isinstance(production.get("life_event"), dict)
            else {}
        )
        if not _text(life_event.get("continuous_event")):
            concept = script.get("script_concept") if isinstance(script.get("script_concept"), dict) else {}
            emotion = production.get("emotion") if isinstance(production.get("emotion"), dict) else {}
            event_contract = (
                visual_contract.get("event_progression")
                if isinstance(visual_contract.get("event_progression"), dict)
                else {}
            )
            life_event = {
                "continuous_event": _text(
                    event_contract.get("suggested_event_flow")
                    or event_contract.get("suggested_opening_action")
                    or concept.get("one_sentence_idea")
                ),
                "starting_context": _text(scene.get("moment")),
                "action_progression": (
                    "→".join(
                        _dedupe_text(
                            [
                                action_design.get("start_state"),
                                action_design.get("core_action"),
                                action_design.get("end_state"),
                            ],
                            limit=3,
                        )
                    )
                    if action_design
                    else "保持同一真实分享状态"
                ),
                "ending_context": _text(emotion.get("ending_state")),
                "source": "DETERMINISTIC_CONTEXT_FALLBACK",
            }
        production["life_event"] = life_event
        script["visual_execution_contract"] = visual_contract
    script["production_design"] = production
    if visual_contract:
        from core.visual_execution_contract import build_visual_execution_diagnostics

        script["visual_execution_diagnostics"] = build_visual_execution_diagnostics(
            contract=visual_contract,
            production_design=production,
        )
    capture_rhythm_contract = (
        dict(seed.get("capture_rhythm_contract") or {})
        if isinstance(seed.get("capture_rhythm_contract"), dict)
        else build_capture_rhythm_contract(
            capture_mode=_text(production.get("capture_mode")),
            macro_structure=(seed.get("creative_direction") or {}).get("macro_structure") or [],
        )
    )
    compiled_storyboard, capture_units = compile_capture_units(
        script.get("storyboard") or [],
        capture_rhythm_contract,
    )
    script["storyboard"] = compiled_storyboard
    script["capture_rhythm_contract"] = capture_rhythm_contract
    script["capture_units"] = capture_units
    _project_mixed_template_shot_contract(seed, compiled_storyboard, capture_units, script)
    retrieval_reference = (
        seed.get("retrieval_reference")
        if isinstance(seed.get("retrieval_reference"), dict)
        else {}
    )
    raw_realization = (
        script.get("reference_realization")
        if isinstance(script.get("reference_realization"), dict)
        else {}
    )
    allowed_parts = {"opening", "proof", "use_process", "ending"}
    declared_adopted_parts = [
        _text(value).lower()
        for value in raw_realization.get("adopted_parts") or []
        if _text(value).lower() in allowed_parts
    ]
    declared_adopted_parts = list(dict.fromkeys(declared_adopted_parts))
    execution_card = _retrieved_execution_card(retrieval_reference)
    available_reference_parts = {
        _text(value).lower()
        for value in execution_card.get("available_parts") or []
        if _text(value)
    }
    compiled_roles = {
        _text(item.get("structure_role")).upper()
        for item in capture_units
        if isinstance(item, dict) and _text(item.get("structure_role"))
    }
    part_is_present = {
        "opening": any("HOOK" in role or "OPENING" in role for role in compiled_roles),
        "proof": any("PROOF" in role for role in compiled_roles),
        "use_process": any("USE" in role for role in compiled_roles),
        "ending": any("ENDING" in role or "CLOSE" in role for role in compiled_roles),
    }
    adopted_parts = [
        part for part in declared_adopted_parts
        if (not available_reference_parts or part in available_reference_parts)
        and part_is_present.get(part, False)
    ]
    excluded_declared_parts = [
        part for part in declared_adopted_parts if part not in adopted_parts
    ]
    if _text(retrieval_reference.get("status")) != "AVAILABLE":
        realization_status = "UNAVAILABLE"
        adopted_parts = []
    elif adopted_parts:
        realization_status = "APPLIED" if len(adopted_parts) >= 2 else "PARTIAL"
    else:
        requested_status = _text(raw_realization.get("status")).upper()
        realization_status = (
            "NOT_USED" if requested_status == "NOT_USED" else "NOT_REPORTED"
        )
    script["reference_realization"] = {
        "status": realization_status,
        "execution_card_id": _text(
            retrieval_reference.get("execution_card_id")
            or retrieval_reference.get("reference_spine_id")
        ),
        "reference_spine_id": _text(
            retrieval_reference.get("execution_card_id")
            or retrieval_reference.get("reference_spine_id")
        ),
        "adopted_parts": adopted_parts,
        "declared_adopted_parts": declared_adopted_parts,
        "excluded_declared_parts": excluded_declared_parts,
        "adaptation_notes": _text(
            raw_realization.get("adaptation_notes")
        )[:400],
        "signal_type": "MODEL_DECLARED_SOFT_OBSERVATION",
        "realization_level": "FUNCTION_SEQUENCE_ONLY",
        "hard_required": False,
    }
    script["generation_provenance"] = dict(generation_provenance)
    script["simplified_script_id"] = _stable_id("SSV_", script)
    return script


def _required_texts(mapping: Dict[str, Any], keys: Iterable[str]) -> bool:
    return all(_text(mapping.get(key)) for key in keys)


def validate_simplified_visual_script(
    script: Dict[str, Any], seed: Dict[str, Any]
) -> Dict[str, Any]:
    """Only three hard dimensions: usability, truth, and carrier."""

    issues: List[str] = []
    warnings: List[str] = []
    concept = script.get("script_concept") if isinstance(script.get("script_concept"), dict) else {}
    production = script.get("production_design") if isinstance(script.get("production_design"), dict) else {}
    usage = script.get("product_usage") if isinstance(script.get("product_usage"), dict) else {}
    shots = [item for item in script.get("storyboard") or [] if isinstance(item, dict)]
    voice = script.get("voiceover_context") if isinstance(script.get("voiceover_context"), dict) else {}

    # 0. Frozen mixed-display contract consistency.
    # A mixed script whose compiled clips do not line up with its frozen template
    # has no authorised per-shot carrier and two disagreeing timelines.  Re-check
    # here (not only at the projection site) so the verdict is derived from the
    # *actual* compiled script that would be handed downstream.
    try:
        from core.accessory_mixed_templates import (
            PROJECTION_ERRORS_KEY,
            validate_mixed_shot_projection,
        )

        recorded = [
            _text(value)
            for value in (script.get(PROJECTION_ERRORS_KEY) or [])
            if _text(value)
        ]
        if recorded:
            issues.append("混合模板合同不一致：" + ",".join(recorded))
        mixed_contract = script.get("mixed_template_contract")
        if isinstance(mixed_contract, dict) and mixed_contract:
            for code in validate_mixed_shot_projection(
                script.get("capture_units"), mixed_contract
            ):
                issues.append("混合模板镜头不一致：" + _text(code))
    except Exception:  # noqa: BLE001 - legacy scripts have no mixed contract
        pass

    # 1. Output usability.
    if not _required_texts(concept, ("one_sentence_idea", "viewer_need", "hook_intent")):
        issues.append("输出不可用：缺少完整脚本概念")
    if not 4 <= len(shots) <= 6:
        issues.append("输出不可用：分镜必须为4至6个对象")
    for index, shot in enumerate(shots, 1):
        if not _required_texts(
            shot,
            ("time_range", "visual_content", "character_action", "natural_emotion", "camera", "narrative_role"),
        ):
            issues.append(f"输出不可用：第{index}镜字段不完整")
    if not _required_texts(voice, ("viewer_relationship", "speaking_intent", "desired_tone")):
        issues.append("输出不可用：缺少中央口播所需的人物语境")

    truth = seed.get("product_truth") if isinstance(seed.get("product_truth"), dict) else {}
    approved_claims = {
        _text(item.get("claim_key")): _text(item.get("fact_text"))
        for item in truth.get("approved_claims") or []
        if isinstance(item, dict) and _text(item.get("claim_key"))
    }
    approved_anchors = set(
        _dedupe_text(
            [*(truth.get("identity_anchors") or []), *(truth.get("visible_detail_anchors") or [])],
            limit=30,
        )
    )
    used_claims = _dedupe_text(usage.get("selling_points_used") or [], limit=12)
    preserved_identity = _dedupe_text(
        usage.get("identity_anchors_preserved") or [], limit=12
    )
    unknown_identity = [
        anchor for anchor in preserved_identity
        if not _anchor_is_authorized(anchor, approved_anchors)
    ]
    if unknown_identity:
        issues.append("商品事实冲突：脚本声明未授权身份锚点=" + ",".join(unknown_identity))
    required_identity = _dedupe_text(truth.get("identity_anchors") or [], limit=12)
    if required_identity and not any(anchor in preserved_identity for anchor in required_identity):
        issues.append("输出不可用：没有保留至少一个商品身份锚点")
    unknown_claims = [key for key in used_claims if key not in approved_claims]
    if unknown_claims:
        issues.append("商品事实冲突：出现未授权claim_key=" + ",".join(sorted(set(unknown_claims))))
    for shot in shots:
        unknown = [
            _text(item)
            for item in shot.get("product_anchors_visible") or []
            if _text(item) and not _anchor_is_authorized(item, approved_anchors)
        ]
        if unknown:
            issues.append("商品事实冲突：镜头使用未授权锚点=" + ",".join(unknown))
        unknown_shot_claims = [
            _text(item)
            for item in shot.get("supported_claim_keys") or []
            if _text(item) and _text(item) not in approved_claims
        ]
        if unknown_shot_claims:
            issues.append("商品事实冲突：镜头声明未授权claim_key=" + ",".join(unknown_shot_claims))
    evidence_keys = {
        _text(item)
        for shot in shots
        for item in (shot.get("supported_claim_keys") or [])
        if _text(item)
    }
    missing_used_evidence = [key for key in used_claims if key not in evidence_keys]
    if missing_used_evidence:
        warnings.append(
            "脚本声明使用但未单独标注画面证据的事实="
            + ",".join(missing_used_evidence)
        )
    visible_in_story = {
        _text(item)
        for shot in shots
        for item in (shot.get("product_anchors_visible") or [])
        if _text(item)
    }
    if approved_anchors and not visible_in_story.intersection(approved_anchors):
        issues.append("输出不可用：分镜没有明确呈现任何授权商品锚点")

    category_extension = (
        seed.get("category_execution_extension")
        if isinstance(seed.get("category_execution_extension"), dict)
        else {}
    )
    if category_extension:
        from core.category_execution import validate_category_execution_identity

        issues.extend(
            validate_category_execution_identity(
                category_extension,
                script=script,
            )
        )

    semantic_spine = (
        seed.get("semantic_spine_contract")
        if isinstance(seed.get("semantic_spine_contract"), dict)
        else {}
    )
    source_argument = (
        semantic_spine.get("source_argument")
        if isinstance(semantic_spine.get("source_argument"), dict)
        else {}
    )
    script_thesis = (
        semantic_spine.get("script_thesis")
        if isinstance(semantic_spine.get("script_thesis"), dict)
        else {}
    )
    # The reviewed operator argument is already the content authority for this
    # item.  Product anchors still own visible identity and proof facts, but an
    # effect word explicitly present in the selected Feishu selling point must
    # not be rejected merely because it is absent from the visual anchor card.
    # Keep the scope narrow: only the frozen source argument and its semantic
    # thesis are added; unrelated product/market context cannot authorize a
    # new benefit.
    authorized_text = json.dumps(
        {
            "product_truth": truth,
            "operator_source_argument": source_argument.get("raw_text"),
            "operator_core_buying_reason": script_thesis.get(
                "core_buying_reason"
            ),
        },
        ensure_ascii=False,
    )
    # Effect terms are product-truth checks, so only inspect fields whose job is
    # to describe the target product or its result.  Character actions, scene
    # prose and emotion can legitimately contain words such as “舒适位置”
    # without claiming that the product itself is comfortable.
    effect_scope = {
        "script_concept": {
            key: concept.get(key)
            for key in ("one_sentence_idea", "viewer_need", "hook_intent")
        },
        "product_role": (
            production.get("outfit", {}).get("product_role")
            if isinstance(production.get("outfit"), dict)
            else ""
        ),
        "product_usage": usage,
        "storyboard": [
            {
                "visual_content": shot.get("visual_content"),
                "product_anchors_visible": shot.get("product_anchors_visible"),
            }
            for shot in shots
        ],
    }
    effect_text = json.dumps(effect_scope, ensure_ascii=False)
    # “舒适坐姿/位置/状态” describes the creator, not a product benefit.
    # Remove only these unambiguous human-state phrases; product comfort
    # statements such as “穿着舒适” remain governed by authorised truth.
    effect_text = re.sub(r"舒适(?:的)?(?:坐姿|位置|状态|姿势|地坐下)", "", effect_text)
    for term in ("显腿长", "腿更长", "显瘦", "显高", "塑形", "保暖", "舒适", "不挑人", "百搭"):
        if term in effect_text and term not in authorized_text:
            issues.append(f"商品事实冲突：出现未授权效果词={term}")

    # 2. Carrier conflict and completeness within that carrier.
    preferred = _text(seed.get("creative_direction", {}).get("preferred_presentation")).upper()
    actual = _text(production.get("presentation_mode")).upper()
    if actual != preferred:
        issues.append(f"承载冲突：要求{preferred or '未知'}，实际{actual or '空'}")
    expected_capture = _text(
        seed.get("creative_direction", {}).get("capture_mode")
    ).upper()
    actual_capture = _text(production.get("capture_mode")).upper()
    if expected_capture and actual_capture and actual_capture != expected_capture:
        issues.append(
            f"拍摄关系冲突：要求{expected_capture}，实际{actual_capture or '空'}"
        )
    elif expected_capture and not actual_capture:
        warnings.append("模型未返回拍摄关系，归一化阶段将使用冻结capture_mode")
    character = production.get("character") if isinstance(production.get("character"), dict) else {}
    outfit = production.get("outfit") if isinstance(production.get("outfit"), dict) else {}
    scene = production.get("scene") if isinstance(production.get("scene"), dict) else {}
    emotion = production.get("emotion") if isinstance(production.get("emotion"), dict) else {}
    if not _required_texts(scene, ("location", "moment", "lighting", "background")):
        issues.append("输出不可用：场景设定不完整")
    if preferred == "PERSON_ON_CAMERA":
        if not _required_texts(character, ("identity", "appearance", "hair_makeup", "speaking_personality")):
            issues.append("输出不可用：真人方向的人物设定不完整")
        if not _required_texts(outfit, ("base_outfit", "product_role", "accessories")):
            issues.append("输出不可用：真人方向的穿搭设定不完整")
        if not _required_texts(emotion, ("starting_state", "natural_change", "ending_state")):
            issues.append("输出不可用：真人方向的情绪变化不完整")
    elif preferred == "STATIC_PRODUCT":
        # Product anchors can legitimately contain phrases such as “上身效果”.
        # They describe the target product, not necessarily an on-camera person.
        # Judge the actual production fields and executable actions instead of
        # keyword-scanning the whole JSON document.
        action_text = " ".join(
            _text(shot.get("character_action")) + " " + _text(shot.get("visual_content"))
            for shot in shots
        )
        # Do not use single-character pronouns here: “他” also appears inside
        # ordinary static wording such as “其他填充物”.  Match executable
        # performer actions instead, and ignore explicit negative phrases.
        normalized_action_text = action_text
        for negative in (
            "无人物出镜", "人物不出镜", "无人物动作", "无模特",
            "没有人物试穿", "无人试穿", "无真人试穿", "不含人物试穿",
            "不安排人物试穿", "没有人物穿着", "无人穿着", "无真人穿着",
            "没有模特动作", "无模特动作", "不含模特动作",
        ):
            normalized_action_text = normalized_action_text.replace(negative, "")
        performer_terms = (
            "穿上", "试穿", "转身展示", "人物出镜", "模特出镜",
            "她穿", "她走", "她站", "她转", "他穿", "他走", "他站", "他转",
            "人物走", "人物站",
        )
        has_on_camera_action = any(term in normalized_action_text for term in performer_terms)
        character_text = " ".join(_text(value) for value in character.values())
        has_character_design = bool(character_text) and not any(
            marker in character_text for marker in ("不适用", "无人物", "不出镜")
        )
        if has_on_camera_action or has_character_design:
            issues.append("承载冲突：静物方向包含真人上身或模特动作")

    signatures: List[str] = []
    for shot in shots:
        signature = _text(shot.get("visual_content")) + "|" + _text(shot.get("character_action"))
        if signature in signatures:
            warnings.append("分镜存在重复画面动作，但不阻断")
        signatures.append(signature)
    action_design = (
        seed.get("action_design")
        if isinstance(seed.get("action_design"), dict)
        else {}
    )
    action_keywords = _dedupe_text(
        action_design.get("action_keywords") or [], limit=8
    )
    authored_actions = "；".join(
        _text(shot.get("character_action")) + "；" + _text(shot.get("visual_content"))
        for shot in shots
    )
    if action_keywords and not any(
        keyword in authored_actions for keyword in action_keywords if len(keyword) >= 2
    ):
        warnings.append(
            "动作主线未明显落到分镜，可人工复核但不阻断="
            + _text(action_design.get("interaction_id"))
        )
    return {
        "valid": not issues,
        "issues": list(dict.fromkeys(issues)),
        "warnings": list(dict.fromkeys(warnings)),
        "policy_version": VALIDATION_POLICY_VERSION,
    }


def build_simplified_voiceover_inputs(
    script: Dict[str, Any], seed: Dict[str, Any], frozen: Dict[str, Any]
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    production = script.get("production_design") or {}
    character = production.get("character") or {}
    scene = production.get("scene") or {}
    emotion = production.get("emotion") or {}
    life_event = production.get("life_event") or {}
    visual_execution = (
        seed.get("visual_execution_contract")
        if isinstance(seed.get("visual_execution_contract"), dict)
        else {}
    )
    # V2 lets every wearable category pass the already-frozen life moment as
    # optional rhetoric context.  It remains CREATIVE_DESIGN rather than claim
    # evidence, so a location/event can frame the speaker but cannot prove a
    # product benefit.  The single flag restores the old scarf-only behavior.
    context_v2_enabled = _text(
        os.environ.get("CENTRAL_VOICEOVER_CONTEXT_V2_ENABLED", "1")
    ).lower() not in {"0", "false", "off", "no"}
    visual_execution_enabled = context_v2_enabled or (
        _text(visual_execution.get("feature_scope")) == "SCARF_ACCESSORY_GREY"
    )
    voice = script.get("voiceover_context") or {}
    bundle = dict(frozen.get("content_bundle_brief") or {})
    evidence_keys = {
        _text(key)
        for item in script.get("storyboard") or []
        if isinstance(item, dict)
        for key in item.get("supported_claim_keys") or []
        if _text(key)
    }
    bundle["claim_atoms"] = [
        item for item in bundle.get("claim_atoms") or []
        if isinstance(item, dict) and _text(item.get("claim_key")) in evidence_keys
    ]
    product_truth = seed.get("product_truth") if isinstance(seed.get("product_truth"), dict) else {}
    # The visual seed intentionally contains a sanitized argument view.  The
    # central voiceover must keep consuming the full frozen content bundle,
    # including the reviewed operator expression and original content
    # mainline.  Only the actually visible claim atoms are narrowed here.
    shots: List[Dict[str, Any]] = []
    shot_plan: List[Dict[str, Any]] = []
    presentation = _text(production.get("presentation_mode")).upper()
    # Film-level fallback.  ``MIXED`` has no single carrier, so this mapping
    # deliberately does not resolve it: a MIXED film must answer per shot.
    carrier = {
        "PERSON_ON_CAMERA": "WEARER_ACTIVE",
        "STATIC_PRODUCT": "STATIC_PRODUCT",
        "HANDS_ONLY": "HAND_ONLY",
    }.get(presentation, presentation)
    for index, item in enumerate(script.get("storyboard") or [], 1):
        if not isinstance(item, dict):
            continue
        supported = [
            _text(key) for key in item.get("supported_claim_keys") or []
            if _text(key) in evidence_keys
        ]
        # Review #8: the frozen per-shot carrier and continuity group win.  The
        # montage deliberately cuts between worn / hand-only / static shots, so
        # collapsing all four onto the film's ``MIXED`` label (or onto a single
        # "EVENT_1") told the central voiceover to write for four identical worn
        # shots -- weakening exactly the tie between wording and what is on
        # screen.  Legacy scripts carry neither field and keep the old fallback.
        shot_carrier = _text(item.get("carrier_mode")).upper() or carrier
        continuity = _text(item.get("continuity_group")) or "EVENT_1"
        shots.append({
            "shot_no": int(item.get("shot_no") or index),
            "duration": _text(item.get("time_range")),
            "shot_content": _text(item.get("visual_content")),
            "observable_action": _text(item.get("character_action")),
            "natural_emotion": _text(item.get("natural_emotion")),
            "framing": _text(item.get("camera")),
            "product_visibility": list(item.get("product_anchors_visible") or []),
            "supported_claim_keys": supported,
            "carrier_mode": shot_carrier,
            "structure_beat": _text(item.get("narrative_role")),
            "audio_hard_constraint": "NONE",
            "audio_preference": "VOICEOVER_ALLOWED",
        })
        shot_plan.append({
            "shot_no": int(item.get("shot_no") or index),
            "structure_beat": _text(item.get("narrative_role")),
            "carrier_mode": shot_carrier,
            "continuity_group": continuity,
        })
    creative_blueprint = {
        "creative_thesis": _text(script.get("script_concept", {}).get("one_sentence_idea")),
        "creator_motivation": _text(voice.get("speaking_intent")),
        "voiceover_grounding_mode": "CONTENT_FIRST_WHOLE_VIDEO",
        "viewer_relationship": _text(voice.get("viewer_relationship")),
        "persona": {
            "identity": _text(character.get("identity")),
            "appearance": _text(character.get("appearance")),
            "hair_makeup": _text(character.get("hair_makeup")),
            "speaking_personality": _text(character.get("speaking_personality")),
        },
        "scene": {
            "location": _text(scene.get("location")),
            "moment": _text(scene.get("moment")),
            "lighting": _text(scene.get("lighting")),
            "background": _text(scene.get("background")),
        },
        "event_design": {
            "natural_event": (
                _text(life_event.get("continuous_event"))
                if visual_execution_enabled
                else ""
            ),
            "core_result_moment": _text(product_truth.get("content_mainline")),
            "starting_state": _text(
                life_event.get("starting_context") or emotion.get("starting_state")
            ),
            "ending_state": _text(
                life_event.get("ending_context") or emotion.get("ending_state")
            ),
        },
        "retention_hook": {
            "opening_event": "",
            "delayed_answer": "",
        },
        "voice_identity": {
            "tone": _text(voice.get("desired_tone")),
            "relationship_mode": _text(voice.get("viewer_relationship")),
            "particle_density": "NATURAL_1_TO_3",
            "sales_pressure": "LOW",
            "forbidden_tone": ["主播叫卖", "参数清单", "广告腔"],
        },
        "capture_context": {
            "capture_mode": _text(production.get("capture_mode")),
            "speaker_position": _text(
                seed.get("voiceover_surface_contract", {}).get("speaker_position")
            ),
            "relationship_to_lens": (
                "创作者本人面对自己的手机镜头向观众分享"
                if _text(production.get("capture_mode"))
                == CAPTURE_MODE_CREATOR_SELF_SHOT
                else "口播覆盖商品展示画面"
            ),
        },
    }
    reference = dict(frozen.get("execution_reference") or {})
    if _text(seed.get("optional_visual_inspiration", {}).get("status")) != "AVAILABLE":
        reference = {}
    direction = {
        "structure_contract": frozen.get("structure_contract") or {},
        "structure_execution_plan": {
            "macro_family_key": ">".join(seed.get("creative_direction", {}).get("macro_structure") or []),
            "shot_plan": shot_plan,
        },
        "execution_reference": reference,
        "content_bundle_brief": bundle,
        "p2_lite": frozen.get("p2_lite") or {},
        "creative_diversity_contract": frozen.get("creative_diversity_contract") or {},
        "creative_blueprint": creative_blueprint,
    }
    retrieval_contract = (
        frozen.get("retrieval_reference_contract")
        if isinstance(frozen.get("retrieval_reference_contract"), dict)
        else {}
    )
    if retrieval_contract:
        direction["retrieval_reference_contract"] = {
            "status": _text(retrieval_contract.get("status")),
            "speech_hook_pool": list(
                retrieval_contract.get("speech_hook_pool") or []
            )[:4],
            "active_runs": dict(retrieval_contract.get("active_runs") or {}),
            "data_snapshot_hash": _text(
                retrieval_contract.get("data_snapshot_hash")
            ),
        }
    category_extension = (
        seed.get("category_execution_extension")
        if isinstance(seed.get("category_execution_extension"), dict)
        else {}
    )
    if category_extension:
        # The central engine remains the sole dialogue author.  It receives the
        # frozen category authority as context, without a new prompt stage or
        # a second set of copywriting rules.
        direction["category_execution_extension"] = dict(category_extension)
        direction["carrier_specific_execution"] = dict(
            seed.get("carrier_specific_execution") or {}
        )
    visual_plan = {
        "schema_version": "simplified-voiceover-visual-plan-v1",
        "shots": shots,
        "production_design": production,
    }
    return direction, visual_plan


def assemble_simplified_complete_script(
    visual_script: Dict[str, Any],
    seed: Dict[str, Any],
    voiceover_plan: Dict[str, Any],
) -> Dict[str, Any]:
    from core.semantic_spine import semantic_trace

    result = dict(visual_script)
    production_design = (
        result.get("production_design")
        if isinstance(result.get("production_design"), dict)
        else {}
    )
    capture_rhythm_contract = (
        dict(seed.get("capture_rhythm_contract") or {})
        if isinstance(seed.get("capture_rhythm_contract"), dict)
        else build_capture_rhythm_contract(
            capture_mode=_text(production_design.get("capture_mode")),
            macro_structure=(seed.get("creative_direction") or {}).get("macro_structure") or [],
        )
    )
    compiled_storyboard, capture_units = compile_capture_units(
        result.get("storyboard") or [],
        capture_rhythm_contract,
    )
    result["storyboard"] = compiled_storyboard
    result["capture_rhythm_contract"] = capture_rhythm_contract
    result["capture_units"] = capture_units
    _project_mixed_template_shot_contract(seed, compiled_storyboard, capture_units, result)
    product_truth = seed.get("product_truth") if isinstance(seed.get("product_truth"), dict) else {}
    lines = [item for item in voiceover_plan.get("lines") or [] if isinstance(item, dict)]
    target = " ".join(_text(item.get("voiceover_text_target_language")) for item in lines).strip()
    translation = " ".join(_text(item.get("voiceover_text_zh")) for item in lines).strip()
    result["continuous_voiceover"] = {
        "hook_id": _text(voiceover_plan.get("hook_id")),
        "target_language": _text(voiceover_plan.get("target_language")),
        "target_language_key": _text(voiceover_plan.get("target_language_key")),
        "language_validation": _text(voiceover_plan.get("language_validation")),
        "target_text": target,
        "chinese_translation": translation,
        "selected_claim_ids": list(voiceover_plan.get("selected_claim_ids") or []),
        "selected_selling_argument_id": _text(
            voiceover_plan.get("selected_selling_argument_id")
        ),
        "selling_argument_realization": _text(
            voiceover_plan.get("selling_argument_realization")
        ),
        "selling_argument_realization_zh": _text(
            voiceover_plan.get("selling_argument_realization_zh")
        ),
        # Preserve central-engine context diagnostics in the final script.
        # Without these fields, an authorised travel or audience situation
        # may be used successfully but become unobservable after assembly.
        "used_context_anchor": _text(
            voiceover_plan.get("used_context_anchor")
        ),
        "context_consumption_status": _text(
            voiceover_plan.get("context_consumption_status")
        ) or "UNAVAILABLE",
        "voiceover_context_mode": _text(
            voiceover_plan.get("voiceover_context_mode")
        ) or "UNAVAILABLE",
        "generation_mode": "CENTRAL_VOICEOVER_COMPLETE_UTTERANCE",
    }
    semantic_spine = dict(seed.get("semantic_spine_contract") or {})
    context_bridge = dict(seed.get("context_bridge_contract") or {})
    result["semantic_spine_contract"] = semantic_spine
    result["context_bridge_contract"] = context_bridge
    result["semantic_trace"] = semantic_trace(
        semantic_spine,
        context_bridge,
        voiceover_context_mode=_text(
            result["continuous_voiceover"].get("voiceover_context_mode")
        ),
    )
    brief_product_truth = {
        "product_identity": _text(product_truth.get("product_identity")),
        "identity_anchors": product_truth.get("identity_anchors") or [],
        "visible_detail_anchors": product_truth.get("visible_detail_anchors") or [],
    }
    if _text(product_truth.get("canonical_product_type")):
        brief_product_truth["canonical_product_type"] = _text(
            product_truth.get("canonical_product_type")
        )
    if isinstance(product_truth.get("display_quantity_contract"), dict):
        brief_product_truth["display_quantity_contract"] = dict(
            product_truth.get("display_quantity_contract") or {}
        )
    outfit_contract = dict(
        (seed.get("diversity_context") or {}).get("outfit_selection_contract")
        or {}
    )
    outfit_prompt_projection = _outfit_prompt_projection(outfit_contract)
    video_generation_brief = {
        "schema_version": VIDEO_BRIEF_SCHEMA_VERSION,
        "render_profile": VIDEO_RENDER_PROFILE,
        "capture_mode": _text(
            (result.get("production_design") or {}).get("capture_mode")
        ),
        "production_design": result.get("production_design") or {},
        "storyboard": result.get("storyboard") or [],
        "capture_rhythm_contract": capture_rhythm_contract,
        "capture_units": capture_units,
        "final_information_gain_review": dict(
            capture_rhythm_contract.get("final_information_gain_review") or {}
        ),
        "product_truth": brief_product_truth,
        "product_identity_lock": build_product_identity_lock(brief_product_truth),
        "action_design": dict(seed.get("action_design") or {}),
        "outfit_selection_contract": outfit_contract,
        "outfit_prompt_projection": outfit_prompt_projection,
        "persona_selection_contract": dict(
            (seed.get("diversity_context") or {}).get("persona_selection_contract")
            or {}
        ),
        "outfit_persona_affinity_contract": dict(
            (seed.get("diversity_context") or {}).get(
                "outfit_persona_affinity_contract"
            ) or {}
        ),
        "outfit_scene_affinity_contract": dict(
            (seed.get("diversity_context") or {}).get(
                "outfit_scene_affinity_contract"
            ) or {}
        ),
        "voiceover": result["continuous_voiceover"],
        "semantic_context": {
            "primary_narrative_context": _text(
                (semantic_spine.get("script_thesis") or {}).get(
                    "primary_narrative_context"
                )
            ),
            "core_buying_reason": _text(
                (semantic_spine.get("script_thesis") or {}).get(
                    "core_buying_reason"
                )
            ),
            "scene_relation": dict(context_bridge.get("scene_relation") or {}),
            "bridge_mode": _text(
                context_bridge.get("voiceover_context_mode")
                or context_bridge.get("bridge_mode")
            ),
            "instruction": (
                "整片保持与主消费情境同一语义世界；不要求逐句对应镜头，"
                "也不得让背景场景改写商品的核心购买理由。"
            ),
        },
        "instruction": (
            "保持同一人物、商品、穿搭、地点、时刻和生活事件；按拍摄节奏合同分别录制3至5段普通手机素材并直接剪切。"
            "优先级依次为商品与物理连续性、实际镜头推进、卖点关系和原生拍摄可行性；发生冲突时先简化场景与表演，不得退回一镜到底"
            if _text(capture_rhythm_contract.get("profile")) == CAPTURE_RHYTHM_MULTICLIP
            else "保持同一人物、商品、穿搭、场景和连续事件；商品一致性优先于场景美感和镜头效果"
        ),
    }
    visual_contract = (
        dict(seed.get("visual_execution_contract") or {})
        if isinstance(seed.get("visual_execution_contract"), dict)
        else {}
    )
    if visual_contract:
        from core.visual_execution_contract import finalize_visual_execution_contract

        visual_contract = finalize_visual_execution_contract(
            visual_contract,
            production_scene=(result.get("production_design") or {}).get("scene") or {},
            presentation_mode=_text(
                (result.get("production_design") or {}).get("presentation_mode")
            ),
        )
        result["visual_execution_contract"] = visual_contract
        video_generation_brief["visual_execution_contract"] = visual_contract
        video_generation_brief["instruction"] = (
            "保持同一人物、商品、穿搭、地点、时刻和生活事件；商品严格按参考图保持一致，"
            "按拍摄节奏合同分别录制普通手机素材并直接剪切，同时保留人物造型与真实场景的完整表达"
        )
        video_generation_brief["visual_execution_diagnostics"] = dict(
            result.get("visual_execution_diagnostics") or {}
        )
    if _text(video_generation_brief["persona_selection_contract"].get("availability")) == "AVAILABLE":
        video_generation_brief["instruction"] = (
            "保持冻结人物模板、商品、穿搭、地点、时刻和连续事件；按拍摄节奏合同分别录制普通手机素材并直接剪切；人物身份只取人物模板参考，"
            "商品外观只取商品参考，商品图中的模特、滤镜、姿态和背景均无人物权威"
        )
    category_extension = (
        seed.get("category_execution_extension")
        if isinstance(seed.get("category_execution_extension"), dict)
        else {}
    )
    if category_extension:
        from core.category_execution import (
            build_category_video_brief,
            resolve_category_carrier_execution,
        )

        carrier_execution = (
            (result.get("production_design") or {}).get("accessory_execution")
            if isinstance(
                (result.get("production_design") or {}).get("accessory_execution"),
                dict,
            )
            else {}
        )
        if not carrier_execution:
            carrier_execution = resolve_category_carrier_execution(
                category_extension,
                presentation_mode=_text(
                    (result.get("production_design") or {}).get("presentation_mode")
                ),
            )
        category_video_brief = build_category_video_brief(
            category_extension,
            carrier_execution=carrier_execution,
        )
        video_generation_brief["category_execution_extension"] = dict(
            category_extension
        )
        video_generation_brief["accessory_execution_brief"] = category_video_brief
    result["video_generation_brief"] = video_generation_brief
    result["assembly_provenance"] = {
        "script_mode": SCRIPT_MODE_SIMPLIFIED,
        "downstream_visual_rewritten": False,
        "voiceover_engine": "central-complete-voiceover",
        "selling_argument_id": _text(
            (product_truth.get("selling_argument") or {}).get("argument_id")
            if isinstance(product_truth.get("selling_argument"), dict)
            else ""
        ),
        "selling_argument_source_claim_ids": list(
            (product_truth.get("selling_argument") or {}).get("source_claim_ids") or []
            if isinstance(product_truth.get("selling_argument"), dict)
            else []
        ),
        "selling_argument_source_argument_id": _text(
            (product_truth.get("selling_argument") or {}).get("source_argument_id")
            if isinstance(product_truth.get("selling_argument"), dict)
            else ""
        ),
        "selling_argument_lineage_status": _text(
            (product_truth.get("selling_argument_lineage") or {}).get("status")
            if isinstance(product_truth.get("selling_argument_lineage"), dict)
            else ""
        ),
    }
    result["complete_script_id"] = _stable_id("SCSCRIPT_", result)
    return result


def _outfit_prompt_projection(contract: Dict[str, Any]) -> Dict[str, str]:
    """Compile only prompt-visible styling details from the frozen contract.

    The projection is deliberately compact and soft.  It prevents the final
    prompt from losing an already selected top/bottom/hair/palette decision,
    without creating another validation layer or repeating internal metadata.
    """

    if not isinstance(contract, dict) or not contract:
        return {}
    recipe = contract.get("outfit_recipe")
    recipe = recipe if isinstance(recipe, dict) else {}
    labels = {
        "one_piece": "连体单品",
        "top": "上装",
        "bottom": "下装",
        "footwear": "鞋履",
        "bag": "包袋",
        "other_accessories": "辅助配饰",
    }
    def prompt_text(value: Any) -> str:
        text = _text(value)
        if text.upper() in {"UNAVAILABLE", "NOT_APPLICABLE"} or text in {
            "不适用", "不由穿搭模板决定",
        }:
            return ""
        if text.count("（") > text.count("）"):
            text = text.replace("（", "，")
        return text

    recipe_parts = [
        f"{labels[key]}：{prompt_text(recipe.get(key))}"
        for key in labels
        if prompt_text(recipe.get(key))
    ]
    if prompt_text(contract.get("inner_requirements")):
        recipe_parts.append(
            f"内搭要求：{prompt_text(contract.get('inner_requirements'))}"
        )
    if not recipe_parts and _text(contract.get("base_outfit_direction")):
        recipe_parts = [_text(contract.get("base_outfit_direction"))]
    grooming_parts = [
        prompt_text(contract.get("hair_direction")),
        prompt_text(contract.get("neckline_direction")),
        prompt_text(contract.get("outer_layer_direction")),
    ]
    style_family = _text(contract.get("style_family"))
    style_direction = (
        f"整体风格：{style_family}"
        if style_family
        and style_family != "STRUCTURED_TEMPLATE"
        and not re.fullmatch(r"[A-Z0-9_]+", style_family)
        else ""
    )
    finish_parts = [
        style_direction,
        prompt_text(contract.get("palette_relation")),
        prompt_text(contract.get("visibility_requirement")),
        prompt_text(contract.get("finish_direction")),
    ]
    return {
        "frozen_outfit": "；".join(item for item in recipe_parts if item),
        "hair_neckline_outer": "；".join(item for item in grooming_parts if item),
        "palette_visibility_finish": "；".join(item for item in finish_parts if item),
        "authority": "FROZEN_SOFT_STYLING",
    }


def validate_simplified_complete_script(script: Dict[str, Any]) -> Dict[str, Any]:
    voice = script.get("continuous_voiceover") if isinstance(script.get("continuous_voiceover"), dict) else {}
    issues = []
    if not _text(voice.get("target_text")) or not _text(voice.get("chinese_translation")):
        issues.append("中央口播没有成功装配到完整脚本")
    video_brief = script.get("video_generation_brief")
    if not isinstance(video_brief, dict):
        issues.append("完整脚本缺少视频生成简报")
    elif _text(video_brief.get("schema_version")) in {
        VIDEO_BRIEF_SCHEMA_VERSION,
        "production-video-brief-v2-ugc-native",
    }:
        identity_lock = (
            video_brief.get("product_identity_lock")
            if isinstance(video_brief.get("product_identity_lock"), dict)
            else {}
        )
        if not identity_lock.get("reference_image_is_authority"):
            issues.append("视频生成简报没有声明参考图商品权威")
        if not _dedupe_text(identity_lock.get("must_preserve") or []):
            issues.append("视频生成简报缺少商品身份锁")
        if not _dedupe_text(identity_lock.get("must_not_change") or []):
            issues.append("视频生成简报缺少商品负向约束")
        category_extension = (
            video_brief.get("category_execution_extension")
            if isinstance(video_brief.get("category_execution_extension"), dict)
            else {}
        )
        if category_extension:
            from core.category_execution import validate_category_execution_identity

            issues.extend(
                validate_category_execution_identity(
                    category_extension,
                    script=script,
                )
            )
    return {
        "valid": not issues,
        "issues": issues,
        "policy_version": "simplified-complete-assembly-v2-product-lock",
    }
