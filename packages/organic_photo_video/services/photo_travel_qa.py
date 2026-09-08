"""Per-page travel semantic QA: scene/outfit/footwear alignment with targeted repair.

The model only classifies what it sees; every pass/fail verdict is re-derived
here so regression cases (airport title over a cafe background, an "evening
stroll" shot in hard daylight, four pages sharing one scene, stiletto heels on
a walking scene) cannot slip through even if the model answers sloppily.

Strictness contract (2026-09-07 optimization round): nothing defaults to
"passed". A QA response with a missing or mistyped field raises
TravelSemanticQAError (QA_SCHEMA_INCOMPLETE semantics); the caller retries the
model once and stops while preserving the raw payload on a second failure.
"""
from __future__ import annotations

from typing import Any, Mapping, Sequence


TRAVEL_QA_SCHEMA = "opv-photo-travel-semantic-qa-v1"

FAILURE_SCENE_MISMATCH = "SCENE_MISMATCH"
FAILURE_INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
FAILURE_OUTFIT_MISMATCH = "OUTFIT_MISMATCH"
FAILURE_WEATHER_MISMATCH = "WEATHER_MISMATCH"
FAILURE_MOBILITY_MISMATCH = "MOBILITY_MISMATCH"
FAILURE_PERSON_DISASTER = "PERSON_DISASTER"
FAILURE_UNKNOWN_SCENE = "UNKNOWN_SCENE"
QA_SCHEMA_INCOMPLETE = "QA_SCHEMA_INCOMPLETE"

FOOTWEAR_TYPES = (
    "SNEAKER", "LOAFER", "FLAT", "MARY_JANE", "LOW_BOOT",
    "LOW_HEEL", "HIGH_HEEL", "STILETTO", "SANDAL",
)


class TravelSemanticQAError(RuntimeError):
    """Structural QA failure; the raw model response stays with the caller."""


def moment_labels(look_plans: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    """role -> travel_moment expected on the page."""
    return {str(look.get("role") or ""): str(look.get("travel_moment") or "")
            for look in look_plans}


def moment_rules_from_contract(travel_contract: Mapping[str, Any]) -> dict[str, dict]:
    rules = {}
    for item in (travel_contract or {}).get("moments") or []:
        rules[str(item.get("key") or "")] = {
            "allowed": [str(value) for value in item.get("allowed_footwear_types") or []],
            "forbidden": [str(value) for value in item.get("forbidden_footwear_types") or []],
        }
    return rules


def _require_bool(page: Mapping[str, Any], field: str, role: str) -> bool:
    value = page.get(field)
    if not isinstance(value, bool):
        raise TravelSemanticQAError(
            f"QA_SCHEMA_INCOMPLETE：{role} 页缺少布尔字段 {field}（QA 结构不完整）"
        )
    return value


def _require_scene_evidence(page: Mapping[str, Any], role: str) -> list[str]:
    evidence = page.get("scene_evidence")
    if (not isinstance(evidence, list)
            or not evidence
            or any(not isinstance(item, str) or not item.strip() for item in evidence)):
        raise TravelSemanticQAError(
            f"QA_SCHEMA_INCOMPLETE：{role} 页 scene_evidence 必须是至少一条非空字符串的数组"
        )
    return [str(item).strip() for item in evidence]


def _require_observed_footwear(page: Mapping[str, Any], role: str,
                               footwear_types: Sequence[str]) -> str:
    observed = str(page.get("observed_footwear_type") or "").strip().upper()
    allowed = set(footwear_types) if footwear_types else set(FOOTWEAR_TYPES)
    if observed not in allowed:
        raise TravelSemanticQAError(
            f"QA_SCHEMA_INCOMPLETE：{role} 页 observed_footwear_type 缺失或不在枚举内："
            f"{observed or '缺失'}"
        )
    return observed


def normalize_travel_qa(
    raw: Mapping[str, Any], *, look_plans: Sequence[Mapping[str, Any]],
    moment_rules: Mapping[str, Mapping] = None,
    footwear_types: Sequence[str] = (),
) -> dict[str, Any]:
    """Convert a model verdict into per-role QA results with deterministic
    failure codes and repair instructions. Nothing defaults to passed."""
    if not isinstance(raw, Mapping) or not isinstance(raw.get("pages"), list):
        raise TravelSemanticQAError("QA_SCHEMA_INCOMPLETE：旅行语义质检没有返回逐页结果")
    expected = moment_labels(look_plans)
    plans_by_role = {str(look.get("role") or ""): look for look in look_plans}
    page_by_role: dict[str, Mapping[str, Any]] = {}
    for page in raw["pages"]:
        if isinstance(page, Mapping) and str(page.get("role") or "") in expected:
            page_by_role[str(page["role"])] = page
    if set(page_by_role) != set(expected):
        missing = sorted(set(expected) - set(page_by_role))
        raise TravelSemanticQAError(
            f"QA_SCHEMA_INCOMPLETE：旅行语义质检没有覆盖全部 A/B/C/D 页面（缺 {missing}）"
        )
    style_uniform = raw.get("style_uniform")
    if not isinstance(style_uniform, bool):
        raise TravelSemanticQAError("QA_SCHEMA_INCOMPLETE：style_uniform 必须是布尔值")

    rules = dict(moment_rules or {})
    observed_moments: dict[str, str] = {}
    results = []
    for role, moment in expected.items():
        page = page_by_role[role]
        plan = plans_by_role[role]
        observed = str(page.get("observed_moment") or "").strip()
        evidence = _require_scene_evidence(page, role)
        outfit_ok = _require_bool(page, "outfit_matches", role)
        weather_ok = _require_bool(page, "weather_matches", role)
        mobility_claim = _require_bool(page, "mobility_matches", role)
        observed_footwear = _require_observed_footwear(page, role, footwear_types)
        observed_moments[role] = observed
        # 人物观察（2026-09-08）：灾难级旗标参与失败判定；相似表情/头姿/视线
        # 只记录为 warning，不阻塞。轻微歪头不作为失败。
        flags = page.get("person_flags") if isinstance(page.get("person_flags"), Mapping) else {}
        person_deformity = bool(flags.get("face_or_limb_deformity"))
        person_tilt = bool(flags.get("obvious_unnatural_tilt"))

        scene_ok = observed == moment and bool(evidence)
        # 改造二：程序按计划与观察判定步行适配，不只相信模型布尔值。
        rule = rules.get(moment) or {}
        forbidden = set(rule.get("forbidden") or [])
        allowed = set(rule.get("allowed") or [])
        mobility_ok = (
            mobility_claim
            and observed_footwear not in forbidden
            and (not allowed or observed_footwear in allowed)
        )

        failure_code = ""
        if not observed or observed.lower() == "unknown":
            failure_code = FAILURE_UNKNOWN_SCENE
        elif observed != moment:
            failure_code = FAILURE_SCENE_MISMATCH
        elif scene_ok is False:
            failure_code = FAILURE_INSUFFICIENT_EVIDENCE
        elif not outfit_ok:
            failure_code = FAILURE_OUTFIT_MISMATCH
        elif not weather_ok:
            failure_code = FAILURE_WEATHER_MISMATCH
        elif not mobility_ok:
            failure_code = FAILURE_MOBILITY_MISMATCH
        elif person_deformity:
            failure_code = FAILURE_PERSON_DISASTER
        elif person_tilt:
            failure_code = FAILURE_PERSON_DISASTER

        repair = str(page.get("repair_instruction") or "").strip()
        if failure_code == FAILURE_SCENE_MISMATCH and not repair:
            repair = (
                f"当前图片场景是 {observed}，目标是 {moment}；"
                f"请重做为：{plan.get('scene_prompt') or moment}"
            )
        if failure_code == FAILURE_MOBILITY_MISMATCH and not repair:
            plan_footwear = str(plan.get("footwear_type") or "")
            repair = (
                f"当前画面鞋履为 {observed_footwear}，不符合 {moment} 场景的步行要求；"
                f"请改穿与计划一致的可行走鞋型（{plan_footwear or '舒适鞋履'}）"
            )
        if failure_code == FAILURE_PERSON_DISASTER and not repair:
            reason = ("明显脸部或肢体畸形" if person_deformity
                      else "明显不自然的头部倾斜")
            repair = f"{reason}；保持人物身份与穿搭，重新生成本张并修正人物表现"
        results.append({
            "role": role,
            "passed": failure_code == "",
            "failure_code": failure_code,
            "expected": moment,
            "observed": observed,
            "scene_evidence": evidence,
            "observed_footwear_type": observed_footwear,
            "planned_footwear_type": str(plan.get("footwear_type") or ""),
            "mobility_matches": mobility_ok,
            "outfit_matches": outfit_ok,
            "weather_matches": weather_ok,
            "person_flags": {
                "face_or_limb_deformity": person_deformity,
                "obvious_unnatural_tilt": person_tilt,
            },
            "repair_instruction": repair if failure_code else "",
        })

    # Deterministic regression guard: one shared scene cannot honestly cover
    # four distinct travel moments, whatever the model claims per page.
    distinct_observed = {value for value in observed_moments.values() if value}
    distinct_expected = set(expected.values())
    if len(distinct_expected) > 1 and len(distinct_observed) == 1:
        for item in results:
            item["passed"] = False
            if not item["failure_code"]:
                item["failure_code"] = FAILURE_SCENE_MISMATCH
                item["repair_instruction"] = (
                    f"四页场景完全相同（{next(iter(distinct_observed))}），与计划的多个旅行场景不符；"
                    "请按各自 scene_prompt 重做"
                )

    passed = all(item["passed"] for item in results) and style_uniform
    # 相似笑容/头姿/视线（>=2 页）：写 warning，继续成片与发布。
    person_warnings = []
    expression_pages = _pages_with_flag(raw, "similar_fixed_smile")
    if len(expression_pages) >= 2:
        person_warnings.append({
            "code": "SIMILAR_EXPRESSION",
            "message": f"多页出现相似笑容或表情（{', '.join(expression_pages)}）",
        })
    gaze_pages = _pages_with_flag(raw, "similar_gaze")
    if len(gaze_pages) >= 2:
        person_warnings.append({
            "code": "SIMILAR_GAZE",
            "message": f"多页视线方向相似（{', '.join(gaze_pages)}）",
        })
    tilt_pages = _pages_with_flag(raw, "similar_head_pose")
    if len(tilt_pages) >= 2:
        person_warnings.append({
            "code": "SIMILAR_HEAD_POSE",
            "message": f"多页头姿相似（{', '.join(tilt_pages)}）",
        })
    return {
        "schema_version": TRAVEL_QA_SCHEMA,
        "passed": passed,
        "style_uniform": style_uniform,
        "roles": results,
        "person_warnings": person_warnings,
        "notes": str(raw.get("notes") or ""),
    }


def _pages_with_flag(raw: Mapping[str, Any], key: str) -> list[str]:
    pages = raw.get("pages") if isinstance(raw, Mapping) else None
    found: list[str] = []
    for page in pages or []:
        if not isinstance(page, Mapping):
            continue
        flags = page.get("person_flags")
        flags = flags if isinstance(flags, Mapping) else {}
        if flags.get(key):
            found.append(str(page.get("role") or ""))
    return found


def failed_roles_from_travel_qa(qa: Mapping[str, Any],
                                role_order: Sequence[str]) -> list[str]:
    """Roles to regenerate: per-role failures, or the whole group when the
    shared visual style broke."""
    failed = [str(item.get("role") or "") for item in qa.get("roles") or []
              if item.get("passed") is False]
    if qa.get("style_uniform") is False:
        return [role for role in role_order]
    return [role for role in role_order if role in failed]


def travel_qa_as_alignment(qa: Mapping[str, Any]) -> dict[str, Any]:
    """Project semantic QA onto the supply manifest group_alignment shape so
    repair history and evidence stay in one place."""
    return {
        "schema_version": "opv-photo-reference-alignment-v1",
        "scope": "TRAVEL_SEMANTIC_QA",
        "passed": bool(qa.get("passed")),
        "scores": {},
        "reason_codes": sorted({
            str(item.get("failure_code")) for item in qa.get("roles") or []
            if item.get("failure_code")
        }),
        "notes": str(qa.get("notes") or ""),
        "role_findings": [
            {"role": item.get("role"), "passed": item.get("passed"),
             "issues": [str(item.get("failure_code") or "")]}
            for item in qa.get("roles") or []
        ],
        "travel_qa": qa,
    }
