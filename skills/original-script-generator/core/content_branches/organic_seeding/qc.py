"""Deterministic fail-closed QC for organicness and experience authority."""
from __future__ import annotations

import json
import math
import re
from typing import Any, Dict, Iterable, List, Sequence, Set

from ..language_metrics import contains_cjk, contains_thai_script, estimate_spoken_duration_seconds
from ..language_validation import target_language_surface_error
from .contracts import OrganicSeedThemeContract


COMMERCE_TERMS = (
    "购买", "下单", "链接", "购物车", "小黄车", "优惠", "折扣", "价格", "库存", "抢购",
    "buy now", "shop now", "add to cart", "link in bio", "discount", "promo code",
    "กดสั่ง", "สั่งซื้อ", "ตะกร้า", "โปรโมชั่น", "ส่วนลด",
)
UNCONFIRMED_HISTORY_TERMS = (
    "用了很久", "一直在用", "每次出门", "每次旅行", "已经回购", "又回购", "朋友都问",
    "很多人问", "收到很多夸", "wear it every", "use it every", "repurchase", "bought again",
    "ใช้มานาน", "ใช้ตลอด", "ซื้อซ้ำ", "เพื่อนถาม",
)

_LOCAL_NEGATION_MARKERS = (
    "不", "无", "未", "禁止", "避免", "勿", "ไม่", "ห้าม", "ไม่มี",
    "no ", "not ", "without ", "avoid ", "never ", "forbid",
)
_PROHIBITION_DIRECTIVE_RE = re.compile(
    r"(?:不得|禁止|不要|避免|不能|不可|勿|ห้าม|ไม่ให้|do\s+not|don't|must\s+not|avoid)"
    r".{0,12}(?:出现|包含|展示|添加|提及|使用|输出|include|show|display|mention|use|แสดง|มี)",
    re.IGNORECASE,
)
_HARD_BOUNDARY_RE = re.compile(r"[。！？!?;；\n]|(?:但是|但|不过|然而|\bbut\b|แต่)", re.IGNORECASE)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _commerce_flags(payload: Dict[str, Any]) -> Dict[str, bool]:
    raw = payload.get("commerce_elements") if isinstance(payload.get("commerce_elements"), dict) else {}
    truthy = {"1", "true", "yes", "y", "是", "有"}
    return {
        key: raw.get(key) is True or _text(raw.get(key)).lower() in truthy
        for key in ("price", "promotion", "purchase_cta", "cart_reference")
    }


def _fact_refs(payload: Dict[str, Any]) -> Set[str]:
    refs: Set[str] = set()
    for value in payload.get("used_fact_refs") or []:
        if _text(value):
            refs.add(_text(value))
    for unit in payload.get("capture_units") or []:
        if not isinstance(unit, dict):
            continue
        refs.update(_text(value) for value in unit.get("fact_refs") or [] if _text(value))
    return refs


def _evidence_refs(payload: Dict[str, Any]) -> Set[str]:
    refs: Set[str] = set()
    for unit in payload.get("capture_units") or []:
        if isinstance(unit, dict):
            refs.update(_text(value) for value in unit.get("evidence_refs") or [] if _text(value))
    return refs


def _visual_natural_language(visual_blueprint: Dict[str, Any]) -> str:
    values: List[str] = []
    for key in ("script_title",):
        values.append(_text(visual_blueprint.get(key)))
    for section_key in ("creative_design", "opening_design"):
        section = visual_blueprint.get(section_key) or {}
        if isinstance(section, dict):
            values.extend(_text(value) for value in section.values())
    for unit in visual_blueprint.get("capture_units") or []:
        if not isinstance(unit, dict):
            continue
        for key in ("shot", "camera_action", "subject_action", "product_evidence", "information_gain"):
            values.append(_text(unit.get(key)))
    return " ".join(value for value in values if value)


def evaluate_organic_visual(
    *,
    theme: OrganicSeedThemeContract,
    visual_blueprint: Dict[str, Any],
    product_truth: Dict[str, Any],
    target_language: str = "",
    duration_seconds: float = 15,
) -> Dict[str, Any]:
    """Fail early before voice generation when the visual contract is invalid."""
    hard_errors: List[str] = []
    warnings: List[str] = []
    units = [item for item in visual_blueprint.get("capture_units") or [] if isinstance(item, dict)]
    if not units:
        hard_errors.append("ORGANIC_VISUAL_OUTPUT_UNAVAILABLE")
    if len(units) > 4:
        hard_errors.append("ORGANIC_VISIBLE_CLIP_COUNT_EXCEEDS_LIMIT")
    elif len(units) < 2:
        warnings.append("ORGANIC_VISIBLE_CLIP_COUNT_LOW")

    natural_language = _visual_natural_language(visual_blueprint)
    if contains_thai_script(natural_language):
        hard_errors.append("ORGANIC_VISUAL_INTERNAL_LANGUAGE_NOT_CHINESE")
    elif natural_language and not contains_cjk(natural_language):
        warnings.append("ORGANIC_VISUAL_INTERNAL_LANGUAGE_NEEDS_REVIEW")

    actionable_commerce, ignored_commerce = _commerce_hits(visual_blueprint, {})
    if actionable_commerce or any(_commerce_flags(visual_blueprint).values()):
        hard_errors.append("ORGANIC_COMMERCE_LANGUAGE_FORBIDDEN")

    allowed_fact_refs = set(theme.allowed_fact_refs)
    unexpected_facts = sorted(_fact_refs(visual_blueprint) - allowed_fact_refs)
    if unexpected_facts:
        hard_errors.append("ORGANIC_FACT_REFERENCE_OUTSIDE_AUTHORITY")
    allowed_evidence_refs = {
        _text(item.get("anchor_id"))
        for item in product_truth.get("visual_anchors") or []
        if isinstance(item, dict) and _text(item.get("anchor_id"))
    }
    unexpected_evidence = sorted(_evidence_refs(visual_blueprint) - allowed_evidence_refs)
    if unexpected_evidence:
        hard_errors.append("ORGANIC_VISUAL_EVIDENCE_REFERENCE_OUTSIDE_AUTHORITY")

    total_duration = sum(
        float(item.get("duration_seconds") or 0)
        for item in units
        if isinstance(item.get("duration_seconds"), (int, float))
    )
    if total_duration and duration_seconds and not 0.8 * duration_seconds <= total_duration <= 1.2 * duration_seconds:
        hard_errors.append("ORGANIC_CAPTURE_DURATION_OUTSIDE_BUDGET")

    valid_jobs = {"LIVED_MOMENT", "RELATION", "ATMOSPHERE", "PRODUCT_DETAIL"}
    valid_focus = {"BACKGROUND", "SECONDARY", "PRIMARY"}
    valid_interactions = {"NONE", "NATURAL_USE", "DEMONSTRATION"}
    jobs = [_text(item.get("narrative_job")).upper() for item in units]
    focuses = [_text(item.get("product_focus")).upper() for item in units]
    interactions = [_text(item.get("product_interaction")).upper() for item in units]
    if any(value not in valid_jobs for value in jobs) or any(
        value not in valid_focus for value in focuses
    ) or any(value not in valid_interactions for value in interactions):
        hard_errors.append("ORGANIC_VISUAL_ROLE_FIELDS_UNAVAILABLE")

    role = _text(theme.product_role).upper()
    lifestyle_count = sum(job in {"LIVED_MOMENT", "RELATION", "ATMOSPHERE"} for job in jobs)
    lifestyle_ratio = {"HERO": 0.5, "SUPPORTING": 2 / 3, "INCIDENTAL": 0.75}.get(role, 2 / 3)
    minimum_lifestyle = math.ceil(len(units) * lifestyle_ratio) if units else 0
    if lifestyle_count < minimum_lifestyle:
        hard_errors.append("ORGANIC_LIFESTYLE_RATIO_TOO_LOW")

    role_limits = {
        "HERO": {"primary": 2, "demonstration": 1},
        "SUPPORTING": {"primary": 1, "demonstration": 0},
        "INCIDENTAL": {"primary": 0, "demonstration": 0},
    }
    limits = role_limits.get(role, role_limits["SUPPORTING"])
    if focuses.count("PRIMARY") > limits["primary"]:
        hard_errors.append("ORGANIC_PRODUCT_FOCUS_EXCEEDS_ROLE")
    if interactions.count("DEMONSTRATION") > limits["demonstration"]:
        hard_errors.append("ORGANIC_PRODUCT_DEMONSTRATION_FORBIDDEN")
    if theme.objective in {"STYLE_MEMORY", "SCENE_ASSOCIATION"} and "PRODUCT_DETAIL" in jobs:
        hard_errors.append("ORGANIC_DETAIL_SHOT_CONFLICTS_WITH_OBJECTIVE")

    unit_blobs = [
        " ".join(
            _text(item.get(key))
            for key in ("shot", "subject_action", "product_evidence", "information_gain")
        )
        for item in units
    ]
    off_body_markers = ("挂在", "挂墙", "衣架", "平铺", "放在桌", "放在椅")
    on_body_markers = ("穿着", "上身", "已穿", "佩戴", "戴着")
    first_off_body = next(
        (index for index, blob in enumerate(unit_blobs) if any(marker in blob for marker in off_body_markers)),
        None,
    )
    if first_off_body is not None and any(
        any(marker in blob for marker in on_body_markers)
        for blob in unit_blobs[first_off_body + 1:]
    ):
        hard_errors.append("ORGANIC_WEAR_STATE_DISCONTINUITY")

    evidence_units = sum(
        bool(_text(item.get("product_evidence")) or item.get("fact_refs") or item.get("evidence_refs"))
        for item in units
    )
    if len(units) >= 2 and (
        jobs.count("PRODUCT_DETAIL") >= 2
        or interactions.count("DEMONSTRATION") >= 2
        or (evidence_units == len(units) and focuses.count("PRIMARY") >= 2)
    ):
        hard_errors.append("ORGANIC_PRODUCT_EVIDENCE_CHECKLIST_PATTERN")

    moving_markers = ("推近", "拉远", "横移", "环绕", "跟拍", "push", "dolly", "tracking", "pan")
    for item in units:
        camera_blob = " ".join(
            _text(item.get(key)) for key in ("camera_relation", "camera_action", "shot")
        ).lower()
        if theme.capture_mode == "ONE_FIXED_PHONE" and any(marker in camera_blob for marker in moving_markers):
            hard_errors.append("ORGANIC_CAPTURE_MODE_CONFLICT")
        coverage = _text(item.get("body_coverage")).upper()
        visible_zones = {_text(value).upper() for value in item.get("visible_zones") or []}
        if coverage in {"WAIST_TO_KNEE", "LOWER_BODY", "KNEE_TO_FOOT"} and visible_zones.intersection(
            {"COLLAR", "SLEEVE", "SHOULDER", "NECKLINE"}
        ):
            hard_errors.append("ORGANIC_SHOT_EVIDENCE_VISIBILITY_CONFLICT")

    hard_errors = sorted(set(hard_errors))
    warnings = sorted(set(warnings))
    return {
        "schema_version": "organic-visual-pre-qc-v2",
        "passed": not hard_errors,
        "hard_errors": hard_errors,
        "warnings": warnings,
        "matched_commerce_hits": actionable_commerce,
        "ignored_negated_commerce_hits": ignored_commerce,
        "unexpected_fact_refs": unexpected_facts,
        "unexpected_evidence_refs": unexpected_evidence,
        "execution_grade": "A" if not hard_errors and not warnings else ("B" if not hard_errors else "BLOCKED"),
    }


def _publishable_text_fields(
    visual_blueprint: Dict[str, Any], voiceover: Dict[str, Any]
) -> Iterable[tuple[str, str]]:
    """Yield only audience-visible or video-executable text, never internal metadata."""
    yield "visual.script_title", _text(visual_blueprint.get("script_title"))
    for key in ("subject", "visible_action"):
        opening = visual_blueprint.get("opening_design") or {}
        yield f"visual.opening_design.{key}", _text(opening.get(key))
    for index, unit in enumerate(visual_blueprint.get("capture_units") or []):
        if not isinstance(unit, dict):
            continue
        for key in ("shot", "camera_action", "subject_action", "action", "product_evidence"):
            yield f"visual.capture_units[{index}].{key}", _text(unit.get(key))
    yield "visual.video_generation_prompt", _text(visual_blueprint.get("video_generation_prompt"))
    yield "voiceover.target_text", _text(voiceover.get("target_text"))
    yield "voiceover.chinese_translation", _text(voiceover.get("chinese_translation"))


def _is_negated(text: str, start: int) -> bool:
    prefix = text[:start]
    boundary = list(_HARD_BOUNDARY_RE.finditer(prefix))
    segment = prefix[boundary[-1].end():] if boundary else prefix
    if _PROHIBITION_DIRECTIVE_RE.search(segment):
        return True
    local = re.split(r"[,，、:]", segment)[-1].lower()[-18:]
    return any(marker.lower() in local for marker in _LOCAL_NEGATION_MARKERS)


def _commerce_hits(
    visual_blueprint: Dict[str, Any], voiceover: Dict[str, Any]
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    actionable: List[Dict[str, Any]] = []
    ignored: List[Dict[str, Any]] = []
    for field_path, raw in _publishable_text_fields(visual_blueprint, voiceover):
        lowered = raw.lower()
        for term in COMMERCE_TERMS:
            needle = term.lower()
            cursor = 0
            while needle and (start := lowered.find(needle, cursor)) >= 0:
                end = start + len(needle)
                hit = {
                    "field_path": field_path,
                    "term": term,
                    "context": raw[max(0, start - 24): min(len(raw), end + 24)],
                    "negated": _is_negated(raw, start),
                }
                (ignored if hit["negated"] else actionable).append(hit)
                cursor = end
    return actionable, ignored


def evaluate_organic_script(
    *,
    theme: OrganicSeedThemeContract,
    visual_blueprint: Dict[str, Any],
    voiceover: Dict[str, Any],
    target_language: str = "",
    video_execution_brief: Dict[str, Any] | None = None,
    duration_seconds: float = 15,
    visual_pre_qc: Dict[str, Any] | None = None,
    story_spine: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    hard_errors: List[str] = []
    warnings: List[str] = []
    if not isinstance(visual_blueprint, dict) or not visual_blueprint.get("capture_units"):
        hard_errors.append("ORGANIC_VISUAL_OUTPUT_UNAVAILABLE")
    if not _text(voiceover.get("target_text")) or not _text(voiceover.get("chinese_translation")):
        hard_errors.append("ORGANIC_VOICEOVER_OUTPUT_UNAVAILABLE")
    language_error = target_language_surface_error(
        _text(voiceover.get("target_text")), target_language
    )
    if language_error:
        hard_errors.append(language_error)
    measured_duration = estimate_spoken_duration_seconds(
        _text(voiceover.get("target_text")), target_language
    )
    story = dict(story_spine or {})
    density_min = round(
        float(duration_seconds or 15) * (0.70 if _text(story.get("core_value")) else 0.47), 1
    )
    density_max = round(float(duration_seconds or 15) * 0.98, 1)
    if measured_duration and duration_seconds:
        if measured_duration > duration_seconds * 1.2:
            hard_errors.append("ORGANIC_VOICEOVER_DURATION_EXCEEDS_VIDEO")
        elif measured_duration > density_max:
            warnings.append("ORGANIC_VOICEOVER_DURATION_TIGHT")
        elif measured_duration < float(duration_seconds or 15) * 0.55:
            warnings.append("ORGANIC_VOICEOVER_LIGHT")
    progression = voiceover.get("content_progression") or {}
    progression_keys = (
        "situation_or_tension",
        "personal_judgment",
        "visible_evidence",
        "takeaway_or_discussion",
    )
    progression_count = sum(bool(_text(progression.get(key))) for key in progression_keys)
    if any(_commerce_flags(visual_blueprint).values()) or any(_commerce_flags(voiceover).values()):
        hard_errors.append("ORGANIC_COMMERCE_ELEMENT_FORBIDDEN")

    commerce_hits, ignored_commerce_hits = _commerce_hits(visual_blueprint, voiceover)
    matched_commerce = sorted({str(hit["term"]) for hit in commerce_hits})
    if commerce_hits:
        hard_errors.append("ORGANIC_COMMERCE_LANGUAGE_FORBIDDEN")

    combined = json.dumps(
        {"visual": visual_blueprint, "voiceover": voiceover},
        ensure_ascii=False,
        default=str,
    ).lower()

    experience_claims = [
        _text(value) for value in voiceover.get("experience_claims") or [] if _text(value)
    ]
    matched_history = sorted(term for term in UNCONFIRMED_HISTORY_TERMS if term.lower() in combined)
    if theme.experience_authority == "NONE" and (experience_claims or matched_history):
        hard_errors.append("ORGANIC_UNAUTHORIZED_EXPERIENCE_CLAIM")
    if theme.experience_authority == "OPERATOR_CONFIRMED_HISTORY":
        confirmed = " ".join(theme.confirmed_experience_facts)
        unsupported = [claim for claim in experience_claims if claim not in confirmed]
        if unsupported:
            hard_errors.append("ORGANIC_EXPERIENCE_CLAIM_OUTSIDE_OPERATOR_AUTHORITY")

    allowed_refs = set(theme.allowed_fact_refs)
    observed_refs = _fact_refs(visual_blueprint) | _fact_refs(voiceover)
    unexpected_refs = sorted(observed_refs - allowed_refs)
    if unexpected_refs:
        hard_errors.append("ORGANIC_FACT_REFERENCE_OUTSIDE_AUTHORITY")
    core_claim_ref = _text(story.get("core_claim_ref"))
    if core_claim_ref and core_claim_ref not in _fact_refs(voiceover):
        warnings.append("ORGANIC_CORE_VALUE_REF_NOT_DECLARED")
    if not _text(voiceover.get("viewer_payoff_realization")):
        warnings.append("ORGANIC_VIEWER_PAYOFF_NOT_EXPLICIT")
    first_unit = (visual_blueprint.get("capture_units") or [{}])[0]
    opening_blob = json.dumps(
        {"opening": visual_blueprint.get("opening_design"), "first_unit": first_unit},
        ensure_ascii=False,
    )
    product_code_like = any(term in opening_blob.lower() for term in ("购买", "推荐购买", "must buy", "ของมันต้องมี"))
    if product_code_like:
        warnings.append("ORGANIC_OPENING_FEELS_PRODUCT_LED")

    units = [item for item in visual_blueprint.get("capture_units") or [] if isinstance(item, dict)]
    if len(units) > 4:
        warnings.append("ORGANIC_VISIBLE_CLIP_COUNT_HIGH")
    total_duration = sum(
        float(item.get("duration_seconds") or 0)
        for item in units
        if isinstance(item.get("duration_seconds"), (int, float))
    )
    if total_duration and duration_seconds and not 0.8 * duration_seconds <= total_duration <= 1.2 * duration_seconds:
        warnings.append("ORGANIC_CAPTURE_DURATION_NEEDS_REVIEW")
    action_markers = (
        "环绕", "横移", "侧移", "推近", "拉远", "跟拍", "走", "转身", "回头",
        "整理", "抬手", "摆弄", "orbit", "pan", "push", "walk", "turn",
    )
    for item in units:
        action_blob = " ".join(
            _text(item.get(key)) for key in ("shot", "camera_action", "subject_action", "action")
        ).lower()
        if len({marker for marker in action_markers if marker in action_blob}) >= 4:
            warnings.append("ORGANIC_CAPTURE_UNIT_ACTION_OVERLOADED")
            break
    role_realization = visual_blueprint.get("product_role_realization") or {}
    if _text(role_realization.get("role")).upper() not in {"", theme.product_role}:
        warnings.append("ORGANIC_PRODUCT_ROLE_REALIZATION_MISMATCH")
    evidence_indices = [
        index
        for index, item in enumerate(units, 1)
        if any(_text(value) for value in item.get("fact_refs") or [])
    ]
    if theme.product_role == "INCIDENTAL" and len(evidence_indices) >= max(2, len(units) // 2 + 1):
        warnings.append("ORGANIC_PRODUCT_ROLE_REALIZATION_MISMATCH")
    if evidence_indices:
        first_evidence = evidence_indices[0]
        if theme.product_prominence == "EARLY" and first_evidence > 1:
            warnings.append("ORGANIC_PRODUCT_PROMINENCE_REALIZATION_MISMATCH")
        elif theme.product_prominence == "LATE" and first_evidence <= max(1, len(units) // 2):
            warnings.append("ORGANIC_PRODUCT_PROMINENCE_REALIZATION_MISMATCH")
    if video_execution_brief is not None:
        if not isinstance(video_execution_brief, dict) or not video_execution_brief.get("capture_units"):
            hard_errors.append("ORGANIC_VIDEO_EXECUTION_BRIEF_UNAVAILABLE")
        elif not (video_execution_brief.get("reference_authority") or {}).get("reference_does_not_control"):
            warnings.append("ORGANIC_REFERENCE_AUTHORITY_NOT_EXPLICIT")
    if visual_pre_qc is not None:
        hard_errors.extend(visual_pre_qc.get("hard_errors") or [])
        warnings.extend(visual_pre_qc.get("warnings") or [])

    adness_score = min(100, len(matched_commerce) * 30 + len(matched_history) * 15 + (20 if product_code_like else 0))
    hard_errors = sorted(set(hard_errors))
    warnings = sorted(set(warnings))
    execution_warning_count = len(
        [
            item for item in warnings
            if item.startswith("ORGANIC_CAPTURE_") or item.startswith("ORGANIC_VISIBLE_")
        ]
    )
    execution_grade = "A" if not execution_warning_count else ("B" if execution_warning_count == 1 else "C")
    pre_execution_grade = _text((visual_pre_qc or {}).get("execution_grade")).upper()
    if execution_grade == "A" and pre_execution_grade == "B":
        execution_grade = "B"
    fact_blocked = any(
        "FACT_" in item or "EVIDENCE_" in item or "VISUAL_OUTPUT" in item
        for item in hard_errors
    )
    commerce_blocked = any("COMMERCE" in item for item in hard_errors)
    experience_blocked = any("EXPERIENCE" in item for item in hard_errors)
    language_blocked = bool(language_error) or any("LANGUAGE_" in item for item in hard_errors)
    handoff_ready = bool(_text(voiceover.get("target_text")) and _text(voiceover.get("chinese_translation")))
    duration_in_density_range = bool(
        measured_duration and density_min <= measured_duration <= density_max
    )
    core_value_realized = bool(
        _text(voiceover.get("core_value_realization"))
        or (core_claim_ref and core_claim_ref in _fact_refs(voiceover))
    )
    if core_value_realized and duration_in_density_range:
        voiceover_density_grade = "A"
    elif core_value_realized and measured_duration >= duration_seconds * 0.55:
        voiceover_density_grade = "B"
    else:
        voiceover_density_grade = "C"
    # Compatibility-only numeric field.  Human review and Feishu projection use
    # the explicit dimensions below instead of presenting this as a quality truth.
    content_value_score = max(0, 80 - len(warnings) * 5 - len(hard_errors) * 20)
    return {
        "schema_version": "organic-branch-qc-v2",
        "passed": not hard_errors,
        "machine_screening": "PASS" if not hard_errors else "BLOCKED",
        "hard_errors": hard_errors,
        "warnings": warnings,
        "adness_score": adness_score,
        "content_value_score": content_value_score,
        "legacy_score_only": True,
        "quality_dimensions": {
            "fact_integrity": "BLOCKED" if fact_blocked else "PASS",
            "commerce_safety": "BLOCKED" if commerce_blocked else "PASS",
            "experience_authority": "BLOCKED" if experience_blocked else "PASS",
            "execution_grade": execution_grade,
            "distinctness_grade": "PENDING_BATCH_REVIEW",
            "creative_quality": "PENDING_HUMAN_REVIEW",
            "voiceover_density": voiceover_density_grade,
            "language_review": "BLOCKED" if language_blocked else "MACHINE_SCREENED_NATIVE_REVIEW_PENDING",
            "handoff_readiness": "READY" if handoff_ready and not hard_errors else "BLOCKED",
        },
        "fake_experience_check": "BLOCKED" if experience_blocked else "PASS",
        "matched_commerce_terms": matched_commerce,
        "matched_commerce_hits": commerce_hits,
        "ignored_negated_commerce_hits": ignored_commerce_hits,
        "matched_unconfirmed_history_terms": matched_history,
        "measured_voiceover_duration_seconds": measured_duration,
        "voiceover_target_duration_seconds": [density_min, density_max],
        "voiceover_progression_count": progression_count,
        "core_claim_ref": core_claim_ref,
        "core_value_realized": core_value_realized,
    }


def _character_bigrams(value: Any) -> Set[str]:
    text = re.sub(r"\s+", "", _text(value).lower())
    return {text[index:index + 2] for index in range(max(0, len(text) - 1))}


def _text_similarity(left: Any, right: Any) -> float:
    a = _character_bigrams(left)
    b = _character_bigrams(right)
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def evaluate_organic_batch(results: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Evaluate cross-script diversity as a review signal, never a hard block."""

    count = len(results)
    if not count:
        return {
            "schema_version": "organic-batch-qc-v2",
            "grade": "NOT_AVAILABLE",
            "warnings": ["ORGANIC_BATCH_HAS_NO_READY_SCRIPT"],
        }
    angles = [_text((item.get("seed_theme") or {}).get("angle_family")) for item in results]
    signatures = [_text((item.get("seed_theme") or {}).get("creative_signature")) for item in results]
    rhetorical_families = [_text((item.get("seed_theme") or {}).get("rhetorical_family")) for item in results]
    hook_mechanisms = [_text((item.get("seed_theme") or {}).get("hook_mechanism")) for item in results]
    topic_families = [_text((item.get("topic_contract") or {}).get("topic_family")) for item in results]
    human_triggers = [_text((item.get("story_spine") or {}).get("human_trigger")) for item in results]
    viewer_needs = [_text((item.get("story_spine") or {}).get("viewer_relevance")) for item in results]
    core_claim_refs = [_text((item.get("story_spine") or {}).get("core_claim_ref")) for item in results]
    closing_modes = [_text((item.get("seed_theme") or {}).get("closing_mode")) for item in results]
    scenes = [
        _text(((item.get("visual_blueprint") or {}).get("creative_design") or {}).get("scene"))
        for item in results
    ]
    translations = [_text((item.get("voiceover") or {}).get("chinese_translation")) for item in results]
    high_similarity_pairs: List[List[int]] = []
    for left in range(count):
        for right in range(left + 1, count):
            if _text_similarity(translations[left], translations[right]) >= 0.72:
                high_similarity_pairs.append([left + 1, right + 1])
    warnings: List[str] = []
    unique_angle_ratio = len({value for value in angles if value}) / count
    unique_signature_ratio = len({value for value in signatures if value}) / count
    unique_rhetorical_ratio = len({value for value in rhetorical_families if value}) / count
    unique_hook_ratio = len({value for value in hook_mechanisms if value}) / count
    populated_topic_families = [value for value in topic_families if value]
    unique_topic_ratio = (
        len(set(populated_topic_families)) / count if populated_topic_families else 0.0
    )
    unique_human_trigger_ratio = len({value for value in human_triggers if value}) / count
    unique_viewer_need_ratio = len({value for value in viewer_needs if value}) / count
    unique_core_claim_ratio = len({value for value in core_claim_refs if value}) / count
    populated_scenes = [value for value in scenes if value]
    max_scene_repeat = max((populated_scenes.count(value) for value in set(populated_scenes)), default=0)
    if unique_signature_ratio < 1:
        warnings.append("ORGANIC_BATCH_CREATIVE_SIGNATURE_REPEAT")
    if unique_angle_ratio < 0.6:
        warnings.append("ORGANIC_BATCH_ANGLE_REPEAT")
    if count >= 3 and max_scene_repeat > 2:
        warnings.append("ORGANIC_BATCH_SCENE_REPEAT")
    if high_similarity_pairs:
        warnings.append("ORGANIC_BATCH_VOICEOVER_SIMILARITY_HIGH")
    if count >= 3 and unique_rhetorical_ratio < 0.6:
        warnings.append("ORGANIC_BATCH_RHETORICAL_FAMILY_REPEAT")
    if count >= 3 and unique_hook_ratio < 0.6:
        warnings.append("ORGANIC_BATCH_HOOK_MECHANISM_REPEAT")
    if populated_topic_families and count >= 3 and unique_topic_ratio < 0.6:
        warnings.append("ORGANIC_BATCH_TOPIC_FAMILY_REPEAT")
    if count >= 4 and len({value for value in closing_modes if value}) < 2:
        warnings.append("ORGANIC_BATCH_CLOSING_MODE_REPEAT")
    if any(human_triggers) and count >= 3 and unique_human_trigger_ratio < 0.6:
        warnings.append("ORGANIC_BATCH_HUMAN_TRIGGER_REPEAT")
    if any(viewer_needs) and count >= 3 and unique_viewer_need_ratio < 0.6:
        warnings.append("ORGANIC_BATCH_VIEWER_NEED_REPEAT")
    if count == 1:
        grade = "NOT_APPLICABLE"
    elif not warnings and unique_angle_ratio >= 0.8 and unique_rhetorical_ratio >= 0.8:
        grade = "A"
    elif unique_angle_ratio >= 0.6 and unique_rhetorical_ratio >= 0.6 and len(warnings) <= 2:
        grade = "B"
    else:
        grade = "C"
    return {
        "schema_version": "organic-batch-qc-v2",
        "grade": grade,
        "warnings": warnings,
        "unique_angle_ratio": round(unique_angle_ratio, 3),
        "unique_signature_ratio": round(unique_signature_ratio, 3),
        "unique_rhetorical_ratio": round(unique_rhetorical_ratio, 3),
        "unique_hook_ratio": round(unique_hook_ratio, 3),
        "unique_topic_ratio": round(unique_topic_ratio, 3),
        "unique_human_trigger_ratio": round(unique_human_trigger_ratio, 3),
        "unique_viewer_need_ratio": round(unique_viewer_need_ratio, 3),
        "unique_core_claim_ratio": round(unique_core_claim_ratio, 3),
        "unique_closing_modes": len({value for value in closing_modes if value}),
        "max_scene_repeat": max_scene_repeat,
        "high_similarity_pairs": high_similarity_pairs,
    }
