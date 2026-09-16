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

import os
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

#: Category-declared product fields whose ``false`` is a deterministic failure.
#:
#: Review fix (2026-09-13), P0-3.  ``SCARF_QA_FIELDS`` was a declarable contract
#: with no runtime consumer, so a missing / obscured / restructured scarf was
#: only ever visible through the generic ``product_matches`` boolean — while the
#: prompt actively told the model that a missing 配饰 counts as MINOR.  Every
#: field listed here blocks instead, and never degrades to a warning.
PRODUCT_QA_FAILURE_FIELDS = {
    "product_present": FAILURE_OUTFIT_MISMATCH,
    "visibility_sufficient": FAILURE_OUTFIT_MISMATCH,
    "dominant_color_matches": FAILURE_OUTFIT_MISMATCH,
    "pattern_family_matches": FAILURE_OUTFIT_MISMATCH,
    "edge_or_fringe_matches": FAILURE_OUTFIT_MISMATCH,
    "length_volume_plausible": FAILURE_OUTFIT_MISMATCH,
    "face_unobscured": FAILURE_PERSON_DISASTER,
}

#: Repair wording per blocked product field, so a targeted regeneration states
#: which property of the product actually broke.
PRODUCT_QA_REPAIR_ZH = {
    "product_present": "指定商品在画面中缺失，必须重新生成并保留该商品",
    "visibility_sufficient": "指定商品被遮挡或占比过小，必须重新生成并让商品清晰可见",
    "dominant_color_matches": "指定商品主色与商品参考图不一致，必须按参考图恢复主色",
    "pattern_family_matches": "指定商品的图案家族与参考图不符，必须按参考图恢复图案家族",
    "edge_or_fringe_matches": "指定商品的边缘或流苏结构与参考图不符，必须按参考图恢复",
    "length_volume_plausible": "指定商品的长度与体积感与参考图明显不符，必须按参考图恢复",
    "face_unobscured": "指定商品遮挡了人物面部，必须重新生成并露出面部",
}


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
    has_product: bool = False, travel_place: str = "", level: str = None,
    product_qa_fields: Sequence[str] = (),
    fixed_background: bool = False,
) -> dict[str, Any]:
    """Convert a model verdict into per-role QA results with deterministic
    failure codes and repair instructions. Nothing defaults to passed.

    ``product_qa_fields`` carries the Category Adapter's declared per-product
    checks (``SCARF_QA_FIELDS`` for a scarf).  Empty — womenswear's value — keeps
    the historical generic-``product_matches``-only behaviour exactly.
    """
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

    qa_level = (level or os.environ.get("OPV_PHOTO_QA_LEVEL", "standard")).strip().lower()
    strict = qa_level == "strict"
    rules = dict(moment_rules or {})
    observed_moments: dict[str, str] = {}
    results = []
    quality_warnings: list[dict[str, str]] = []
    for role, moment in expected.items():
        page = page_by_role[role]
        plan = plans_by_role[role]
        observed = str(page.get("observed_moment") or "").strip()
        evidence = _require_scene_evidence(page, role)
        outfit_ok = _require_bool(page, "outfit_matches", role)
        weather_ok = _require_bool(page, "weather_matches", role)
        mobility_claim = _require_bool(page, "mobility_matches", role)
        product_ok = _require_bool(page, "product_matches", role) if has_product else True
        # 类目声明的逐项商品质检（review 修复 P0-3）：任一为 false 即确定性
        # 失败，不因 outfit_severity 降级。字段由适配器声明，womenswear 为空。
        product_qa: dict[str, bool] = {}
        product_qa_failure = ""
        product_qa_repair = ""
        if has_product and product_qa_fields:
            # 只取「声明了失败语义」的字段：适配器的 qa_fields 里还含 role /
            # product_matches / repair_instruction 这类非布尔或已判定字段。
            for field, code in PRODUCT_QA_FAILURE_FIELDS.items():
                if field not in product_qa_fields:
                    continue
                value = _require_bool(page, field, role)
                product_qa[field] = value
                if not value and not product_qa_failure:
                    product_qa_failure = code
                    product_qa_repair = PRODUCT_QA_REPAIR_ZH.get(field, "")
        observed_footwear = _require_observed_footwear(page, role, footwear_types)
        observed_moments[role] = observed
        # 人物观察（2026-09-08）：灾难级旗标参与失败判定；相似表情/头姿/视线
        # 只记录为 warning，不阻塞。轻微歪头不作为失败。
        flags = page.get("person_flags") if isinstance(page.get("person_flags"), Mapping) else {}
        person_deformity = bool(flags.get("face_or_limb_deformity"))
        person_tilt = bool(flags.get("obvious_unnatural_tilt"))

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
        elif observed != moment and not fixed_background:
            # 固定背景（Phase 3）：moment 只是鞋履/步行规则的规划语境，画面
            # 不呈现对应场所，observed 与 moment 不一致不算缺陷——但不能因此
            # 结束检查链：商品、穿搭、天气、人物等核心检查必须继续执行
            # （2026-09-15 七样审查：豁免分支曾短路后续全部 elif）。
            failure_code = FAILURE_SCENE_MISMATCH
        elif not evidence:
            # 证据检查与 moment 解耦：固定背景只要求有画面证据，不要求
            # 场所匹配（非固定路径在此处 observed==moment 已由上一分支保证，
            # 行为逐字等价于旧 scene_ok 判定）。
            failure_code = FAILURE_INSUFFICIENT_EVIDENCE
        elif not product_ok:
            failure_code = FAILURE_OUTFIT_MISMATCH
        elif product_qa_failure:
            failure_code = product_qa_failure
        elif not outfit_ok and (
                strict or str(page.get("outfit_severity") or "MINOR").upper() == "MAJOR"):
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
        if not outfit_ok and not failure_code:
            quality_warnings.append({
                "role": role, "code": "MINOR_OUTFIT_VARIATION",
                "message": repair or "穿搭细节与计划存在轻微差异",
            })
        if not weather_ok and not failure_code:
            quality_warnings.append({
                "role": role, "code": "WEATHER_NOTE",
                "message": repair or "光线或温度感与计划存在轻微差异",
            })
        if not mobility_ok and not failure_code:
            quality_warnings.append({
                "role": role, "code": "FOOTWEAR_NOTE",
                "message": repair or "鞋履与场景步行强度存在轻微偏差",
            })
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
            # Only claim a person-cause when a person flag actually fired: a
            # ``PERSON_DISASTER`` raised by the category's own product check
            # (a scarf covering the face) must not be reported as a head tilt.
            reason = "明显脸部或肢体畸形" if person_deformity else (
                "明显不自然的头部倾斜" if person_tilt else "")
            if reason:
                repair = f"{reason}；保持人物身份与穿搭，重新生成本张并修正人物表现"
        if product_qa_failure and not repair:
            # The category-declared product repair answers for its own failure.
            repair = product_qa_repair or (
                "指定商品未通过逐项质检；请按商品参考图重新生成并保留商品身份"
            )
        result = {
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
            "product_matches": product_ok if has_product else None,
            "weather_matches": weather_ok,
            "person_flags": {
                "face_or_limb_deformity": person_deformity,
                "obvious_unnatural_tilt": person_tilt,
            },
            "repair_instruction": repair if failure_code else "",
        }
        # 只在适配器声明了商品质检字段时附带逐项结果，womenswear 的输出形状
        # 与历史逐字一致。
        if product_qa_fields:
            result["product_qa"] = product_qa
        results.append(result)

    # Deterministic regression guard: one shared scene cannot honestly cover
    # four distinct travel moments, whatever the model claims per page.
    # 固定背景（Phase 3）豁免：四页共享同一背景是任务设计，不是缺陷；地点/
    # 温度只是内容语境，画面不呈现目的地也不算冲突。
    distinct_observed = {value for value in observed_moments.values() if value}
    distinct_expected = set(expected.values())
    if (not fixed_background
            and len(distinct_expected) > 1 and len(distinct_observed) == 1):
        for item in results:
            item["passed"] = False
            if not item["failure_code"]:
                item["failure_code"] = FAILURE_SCENE_MISMATCH
                item["repair_instruction"] = (
                    f"四页场景完全相同（{next(iter(distinct_observed))}），与计划的多个旅行场景不符；"
                    "请按各自 scene_prompt 重做"
                )

    destination_conflict = bool(raw.get("destination_conflict")) and not fixed_background
    if destination_conflict:
        for item in results:
            if item["passed"]:
                item["passed"] = False
                item["failure_code"] = FAILURE_SCENE_MISMATCH
                item["repair_instruction"] = (
                    f"画面与指定地点 {travel_place or '旅行目的地'} 存在明显地理冲突；"
                    "按指定地点重做场景"
                )
    passed = all(item["passed"] for item in results) and style_uniform
    raw_cover = raw.get("cover_recommendation") or {}
    cover_role = str(
        raw_cover.get("role") if isinstance(raw_cover, Mapping) else ""
    ).strip()
    if cover_role not in expected:
        cover_role = ""
    cover_recommendation = {
        "role": cover_role,
        "reason_zh": str(
            raw_cover.get("reason_zh") if isinstance(raw_cover, Mapping) else ""
        ).strip(),
    }
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
        "qa_level": "strict" if strict else "standard",
        "style_uniform": style_uniform,
        "destination_conflict": destination_conflict,
        "destination_evidence": [str(v) for v in raw.get("destination_evidence") or []],
        "roles": results,
        "quality_warnings": quality_warnings,
        "person_warnings": person_warnings,
        "cover_recommendation": cover_recommendation,
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
        "quality_warnings": list(qa.get("quality_warnings") or [])
        + list(qa.get("person_warnings") or []),
        "cover_recommendation": dict(qa.get("cover_recommendation") or {}),
        "travel_qa": qa,
    }


# ---------------------------------------------------------------------------
# 旅行目的地统一解析（自动供稿 Phase 3）
# ---------------------------------------------------------------------------

#: 常见旅行地点 → 国家 的最小映射（仅做一致性校验，不做通用 NER）。
KNOWN_PLACE_COUNTRY = {
    "东京": "日本", "大阪": "日本", "京都": "日本", "札幌": "日本", "福冈": "日本",
    "名古屋": "日本", "奈良": "日本", "冲绳": "日本",
    "曼谷": "泰国", "清迈": "泰国", "普吉": "泰国", "苏梅": "泰国", "芭提雅": "泰国",
    "巴黎": "法国", "伦敦": "英国", "首尔": "韩国", "新加坡": "新加坡",
    "吉隆坡": "马来西亚", "巴厘岛": "印度尼西亚", "胡志明": "越南", "河内": "越南",
}


class TravelDestinationError(ValueError):
    """旅行国家与旅行地点冲突——规划前必须修正，不允许静默二选一。"""


def resolve_travel_destination(*, country: str, place: str) -> dict:
    """把「旅行国家 + 旅行地点」合并为一个有效目的地对象。

    - 发布市场（如泰国）与旅行目的地（如日本）是两个概念，国家字段只指
      旅行目的地；
    - 地点命中已知映射且与国家冲突 → 抛错（调用方转为工作流错误）；
    - 两者都空 → 空对象：生成不得凭空标注真实国家/景点。
    """
    country = str(country or "").strip()
    place = str(place or "").strip()
    if place:
        for known_place, known_country in KNOWN_PLACE_COUNTRY.items():
            if known_place in place:
                if country and country != known_country:
                    raise TravelDestinationError(
                        f"旅行地点「{place}」属于{known_country}，与旅行国家"
                        f"「{country}」冲突；请修正后再执行")
                country = country or known_country
                break
    return {"country": country, "place": place}
