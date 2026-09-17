"""每条片子的一条主线：规划期冻结，观察任务与中央口播共用同一份（方案 C2）。

方案原文（节选）：

> C2：每条一条明确主线。复用 ``semantic_spine``，冻结观众问题、核心价值、事实依据、
> 可见回答、表达边界；主线同时进入观察任务与中央口播。各模块任务：佩戴／造型开场
> 直接展示主线相关结果；手持看清商品本体轮廓／排列／已确认结构；静物集中看一个需
> 稳定画面的细节；佩戴关系解释比例搭配关系。首尾可同一佩戴状态但要有细节与整体
> 关系的区别，**不把重复裁切自动算作第二个观察**。

设计取舍：

* **不改 ``semantic_spine`` 本身**。它已经承载了 ``script_thesis`` 等字段，这个模块只把
  它、所选卖点、C1 的事实记录和混合模板的 ``capture_units`` 合成**一份**可读合同，
  数据来源逐项写进 ``source_refs``。
* **不新增主题体系**。主线只是"这一条片子要说什么"，不引入新的 theme/candidate_role。
* **五个字段一个来源，缺了就记 ``input_gap``**，不拿内部 ID（``ARGUMENT_OPERATOR_*``）
  冒充文案 —— 这是 R4 的口子，``is_readable_theme_proposition`` 直接复用。
* **重复裁切不算第二个观察**。每镜算出 ``distinct_observation_key``；同一个 key 的第二次
  出现标 ``counts_as_new_observation=False`` 并归到同一组，所以"同一机位再裁一次"不会
  被当成两处观察。
* import 无副作用、不碰数据库；合同缺失时下游行为与现在完全一致。
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

MAINLINE_CONTRACT_ENV = "ORIGINAL_SCRIPT_MIXED_MAINLINE_CONTRACT_ENABLED"
MAINLINE_CONTRACT_SCHEMA = "mixed-mainline-contract-v1"

#: 方案 C2 点名的五个冻结字段，少一个就是 INCOMPLETE。
MAINLINE_FIELDS: Tuple[str, ...] = (
    "audience_question",
    "core_value",
    "fact_basis",
    "visible_answer",
    "expression_boundary",
)

MAINLINE_STATUS_FROZEN = "FROZEN"
MAINLINE_STATUS_INCOMPLETE = "INCOMPLETE"

#: 模块 → 这一镜在主线上承担什么（方案的"各模块任务"，措辞尽量贴原文）。
MODULE_MAINLINE_TASK_ZH: Dict[str, str] = {
    "WORN_DETAIL": "直接展示主线相关的结果（佩戴／造型开场）",
    "WORN_RELATION": "解释佩戴关系与比例搭配关系",
    "HANDHELD_PRODUCT": "看清商品本体轮廓、排列与已确认结构",
    "STATIC_PRODUCT": "集中看一个需要稳定画面的细节",
}

#: 模块 → 主线环节。首镜承载核心价值的可见结果，末镜只解释关系与比例。
MODULE_MAINLINE_LINK: Dict[str, str] = {
    "WORN_DETAIL": "CORE_VALUE_RESULT",
    "WORN_RELATION": "RELATION_AND_PROPORTION",
    "HANDHELD_PRODUCT": "PRODUCT_BODY_AND_STRUCTURE",
    "STATIC_PRODUCT": "STABLE_DETAIL_ONLY",
}

#: 承担"主线可见回答"的模块优先级：佩戴开场优先，其次手持，最后静物。
_VISIBLE_ANSWER_ORDER: Tuple[str, ...] = (
    "WORN_DETAIL",
    "HANDHELD_PRODUCT",
    "STATIC_PRODUCT",
    "WORN_RELATION",
)

#: C2 只要求"一条主线"，所以口播的主价值只允许一个。
MAIN_VALUE_LIMIT = 1

#: 使用场景词 → 冻结场景族（``outfit_scene_affinity_contract.selected_scene_family``）。
#:
#: 场景族只有 6 个（居家日常／咖啡品质室内／街头外出／镜前试穿／办公通勤／乘车等候），
#: 所以这里**只放能逐字自圆其说的对应**。旅行／度假／海边／运动／婚礼／伴娘 六个词
#: 故意不入表：硬塞"婚礼 → CAFE_DINING"会编出一个画面里不存在的假对应。
SCENE_KEEPER_FAMILIES: Dict[str, str] = {
    "通勤": "OFFICE_WORKBREAK",
    "上班": "OFFICE_WORKBREAK",
    "日常": "HOME_ROUTINE",
    "约会": "CAFE_DINING",
    "聚会": "CAFE_DINING",
    "拍照": "VANITY_TRYON",
    "逛街": "STREET_OUTING",
    "出街": "STREET_OUTING",
}

#: 并列项的切分符。运营原文顿号与逗号两种都用。
_KEEPER_SEPARATORS = re.compile(r"[、，,；;]")

#: "处处适用"式的概括表达。删掉具体场景后，这类短语**本身就是并列承诺**，
#: 留着等于换一种说法继续承诺"多个场景"。实测戒指那条删完场景只剩下
#: "不挑场合" —— 比原文更概括，属于语义反转，必须一起删。
_GENERIC_STACKING_TERMS: Tuple[str, ...] = (
    "不挑场合",
    "百搭",
    "多种",
    "各种",
    "任何场合",
    "都能戴",
    "都合适",
    "都适合",
    "通用",
)

_TRUE_TOKENS = {"1", "true", "yes", "on"}


def mixed_mainline_enabled() -> bool:
    """Whether planning should freeze a mainline contract at all. Default off."""

    return _text(os.environ.get(MAINLINE_CONTRACT_ENV)).lower() in _TRUE_TOKENS


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (list, tuple, set)):
        return " ".join(_text(item) for item in value if _text(item))
    return str(value).strip()


def _readable(value: Any) -> str:
    """Text a reviewer could read back, or "" if it is an internal identifier.

    ``is_readable_theme_proposition`` is the existing gate; reusing it keeps one
    definition of "readable" instead of a second regex.
    """

    text = _text(value)
    if not text:
        return ""
    try:
        from core.accessory_mixed_templates import is_readable_theme_proposition

        return text if is_readable_theme_proposition(text) else ""
    except Exception:  # noqa: BLE001 - a missing helper must not break planning
        return "" if text.upper().startswith("ARGUMENT_OPERATOR_") else text


def _first_readable(*candidates: Any) -> Tuple[str, str]:
    """The first readable candidate and which field it came from."""

    for index, value in enumerate(candidates):
        text = _readable(value)
        if text:
            return text, str(index)
    return "", ""


def _observed_part_or_relation(unit: Mapping[str, Any]) -> str:
    """What this shot asks the viewer to look at —— 一处还是换个机位而已。

    The key is built from the *view* (scope + body zone + module), not from the
    shot's prose.  Re-framing the same view is therefore the same observation,
    which is exactly what "不把重复裁切自动算作第二个观察" asks for.
    """

    parts = [
        _text(unit.get("view_scope")).upper(),
        _text(unit.get("body_zone")).upper(),
        _text(unit.get("module")).upper(),
    ]
    return "|".join(part for part in parts if part)


def _observation_tasks(units: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    tasks: List[Dict[str, Any]] = []
    seen: Dict[str, str] = {}
    for index, unit in enumerate(units, start=1):
        if not isinstance(unit, Mapping):
            continue
        module = _text(unit.get("module")).upper()
        key = _observed_part_or_relation(unit)
        unit_id = _text(unit.get("unit_id"))
        first_owner = seen.get(key, "")
        is_new = not first_owner
        if is_new and key:
            seen[key] = unit_id
        tasks.append(
            {
                "unit_id": unit_id,
                "unit_index": int(unit.get("unit_index") or index),
                "module": module,
                "module_label": _text(unit.get("module_label")),
                "view_scope": _text(unit.get("view_scope")),
                "view_label": _text(unit.get("view_label")),
                "product_state": _text(unit.get("product_state")),
                "observation_job": _text(unit.get("observation_job")),
                "distinct_observation_key": key,
                "counts_as_new_observation": is_new,
                "same_observation_as": "" if is_new else first_owner,
                "mainline_link": MODULE_MAINLINE_LINK.get(module, "SUPPORTING"),
                "task_zh": MODULE_MAINLINE_TASK_ZH.get(module, ""),
            }
        )
    return {
        "tasks": tasks,
        "distinct_keys": list(seen),
        "distinct_observation_count": len(seen),
        "repeated_units": [
            task["unit_id"]
            for task in tasks
            if not task["counts_as_new_observation"]
        ],
    }


def build_mixed_mainline_contract(
    *,
    selling_argument: Optional[Mapping[str, Any]] = None,
    fact_evidence: Optional[Mapping[str, Any]] = None,
    semantic_spine: Optional[Mapping[str, Any]] = None,
    mixed_contract: Optional[Mapping[str, Any]] = None,
    audience_tension_text: str = "",
    requested_hook_id: str = "",
    scene_family: str = "",
) -> Dict[str, Any]:
    """Freeze one mainline for one film: 观众问题／核心价值／事实依据／可见回答／表达边界。"""

    argument = selling_argument if isinstance(selling_argument, Mapping) else {}
    facts = fact_evidence if isinstance(fact_evidence, Mapping) else {}
    spine = semantic_spine if isinstance(semantic_spine, Mapping) else {}
    contract = mixed_contract if isinstance(mixed_contract, Mapping) else {}
    thesis = spine.get("script_thesis") if isinstance(spine.get("script_thesis"), Mapping) else {}
    adaptation = facts.get("adaptation") if isinstance(facts.get("adaptation"), Mapping) else {}
    fact_type = facts.get("fact_type") if isinstance(facts.get("fact_type"), Mapping) else {}

    gaps: List[str] = []

    # ── 观众问题 ────────────────────────────────────────────────────────
    audience_question, audience_index = _first_readable(
        thesis.get("primary_narrative_context"),
        argument.get("audience_situation"),
        argument.get("target_need"),
        audience_tension_text,
    )
    audience_refs = (
        "semantic_spine.script_thesis.primary_narrative_context",
        "selling_argument.audience_situation",
        "selling_argument.target_need",
        "content_bundle_brief.audience_tension_text",
    )
    if not audience_question:
        gaps.append("AUDIENCE_QUESTION_UNAVAILABLE")

    # ── 核心价值（一个，不并列） ────────────────────────────────────────
    core_value, core_index = _first_readable(
        thesis.get("core_buying_reason"),
        thesis.get("selected_source_span"),
        argument.get("creative_core_value"),
        argument.get("core_value"),
    )
    core_refs = (
        "semantic_spine.script_thesis.core_buying_reason",
        "semantic_spine.script_thesis.selected_source_span",
        "selling_argument.creative_core_value",
        "selling_argument.core_value",
    )
    if not core_value:
        gaps.append("CORE_VALUE_UNAVAILABLE")

    # ── 事实依据（原文 + 来源 + 类型 + 结论，全部来自 C1 的记录） ─────────
    source_text = _text(facts.get("source_text"))
    fact_basis = {
        "source_text": source_text,
        "source_field": _text(facts.get("source_text_field")),
        "source": dict(facts.get("source") or {}),
        "fact_type": _text(fact_type.get("tier")),
        "fact_type_label_zh": _text(fact_type.get("tier_label_zh")),
        "evidence_requirement": _text(fact_type.get("requires")),
        "verdict": _text(adaptation.get("verdict")),
        "product_image_version_status": _text(
            (facts.get("product_image_version") or {}).get("status")
        )
        if isinstance(facts.get("product_image_version"), Mapping)
        else "",
    }
    if not source_text or not fact_basis["fact_type"]:
        gaps.append("FACT_BASIS_UNAVAILABLE")

    # ── 表达边界（C1 的口径 + 强度 + 只允许一条主线） ─────────────────────
    expression_boundary = {
        "allowed_wording": _text(adaptation.get("allowed_wording")),
        "forbidden_wording": [
            _text(item) for item in (adaptation.get("forbidden_wording") or []) if _text(item)
        ],
        # C3 的第三类核对要用它：没有真实体验授权时，成稿里出现"试了一天""随手一夹"
        # 这类措辞就是伪装为实测。
        "experience_authority": _text(adaptation.get("experience_authority")),
        "allowed_strength": _text(argument.get("allowed_strength")),
        "max_main_value_count": MAIN_VALUE_LIMIT,
        "conflicts": list(adaptation.get("conflicts") or []),
    }
    if not expression_boundary["allowed_wording"] and not source_text:
        gaps.append("EXPRESSION_BOUNDARY_UNAVAILABLE")

    # ── 核心价值不得违反自己的表达边界 ──────────────────────────────────
    # 核心价值取自运营的卖点原文，而原文本身可能带着本合同的禁词（实测：
    # "双层纱质蝴蝶造型，超唯美" 的 forbidden_wording 就是 ["纱"]）。原样交给
    # 中央口播，模型就会写出刚刚被口径拒掉的材质断言。``core_value`` 保留原文供
    # 审计，``core_value_safe`` 才是口播可以说的那一句（只删不换）。
    from core.selling_fact_evidence import strip_forbidden_wording

    core_value_source_text = core_value
    constrained_forbidden: List[str] = []
    core_value_safe = core_value
    if core_value and expression_boundary["forbidden_wording"]:
        constrained_forbidden = [
            term
            for term in expression_boundary["forbidden_wording"]
            if term and term in core_value
        ]
        if constrained_forbidden:
            core_value_safe = strip_forbidden_wording(core_value, constrained_forbidden)
            if not core_value_safe:
                gaps.append("CORE_VALUE_WORDING_UNAVAILABLE")
    expression_boundary["core_value_wording_constrained"] = bool(constrained_forbidden)
    expression_boundary["core_value_forbidden_matched"] = constrained_forbidden

    # ── 多场景封顶：素材必须跟着收窄，光发约束没用 ────────────────────────
    # 上面只删"禁词"，而多场景封顶的 ``forbidden_wording`` 恒为空（见
    # ``selling_fact_evidence``：单个场景词是对的，错的只是并列堆叠）→
    # ``core_value_safe`` 与原文**逐字相同**。于是模型一边收到"只说一个…不并列多个"，
    # 一边收到并列三个场景的素材：约束与素材自相矛盾，它当然照素材写。
    # 实测手镯／戒指两条真实稿就是这么把三个场景并列说出来的。
    scene_conflicts = [
        item
        for item in expression_boundary["conflicts"]
        if isinstance(item, Mapping)
        and _text(item.get("kind")).upper() == "MULTI_SCENARIO_UNAUTHORIZED"
    ]
    stacked_scenes: List[str] = [
        _text(scene)
        for conflict in scene_conflicts
        for scene in (conflict.get("stacked_scenes") or [])
        if _text(scene)
    ]
    if stacked_scenes:
        keeper, keeper_unresolved = resolve_scene_keeper(stacked_scenes, scene_family)
        expression_boundary["scene_family"] = _text(scene_family)
        expression_boundary["keeper"] = keeper
        expression_boundary["keeper_unresolved"] = keeper_unresolved
        expression_boundary["keeper_rule"] = (
            "按冻结场景族从并列场景里挑一个"
            if keeper
            else "冻结场景族不可用，不指认具体场景"
        )
        expression_boundary["core_value_narrowed_from"] = core_value
        core_value_safe = narrow_core_value_to_keeper(
            core_value_safe, stacked_scenes, keeper
        )
        expression_boundary["core_value_narrowed_to"] = core_value_safe
        expression_boundary["core_value_wording_constrained"] = (
            core_value_safe != core_value
        )

    # ── 观察任务与可见回答 ──────────────────────────────────────────────
    units = [unit for unit in (contract.get("capture_units") or []) if isinstance(unit, Mapping)]
    observations = _observation_tasks(units)
    tasks_by_module = {
        _text(task["module"]): task for task in observations["tasks"]
    }
    visible_task: Dict[str, Any] = {}
    for module in _VISIBLE_ANSWER_ORDER:
        if module in tasks_by_module:
            visible_task = tasks_by_module[module]
            break
    visible_answer = {
        "shot_ref": _text(visible_task.get("unit_id")),
        "module": _text(visible_task.get("module")),
        "module_label": _text(visible_task.get("module_label")),
        "observation_job": _text(visible_task.get("observation_job")),
        # 主线 + 这一镜要看的东西 = 观众实际能看到的那一句话。这里必须用可说
        # 口径：用原文的话，刚被表达边界收窄掉的措辞会从这条通路重新流回下游。
        "text": _join_parts(core_value_safe, _text(visible_task.get("observation_job"))),
        "source_ref": "capture_units[*].observation_job",
    }
    if not visible_answer["shot_ref"]:
        gaps.append("VISIBLE_ANSWER_UNAVAILABLE")

    status = MAINLINE_STATUS_FROZEN if not gaps else MAINLINE_STATUS_INCOMPLETE
    return {
        "schema_version": MAINLINE_CONTRACT_SCHEMA,
        "status": status,
        "audience_question": audience_question,
        "core_value": core_value,
        #: The operator's own sentence, kept for audit even when it had to be
        #: narrowed; ``core_value_safe`` is what the voiceover may actually say.
        "core_value_source_text": core_value_source_text,
        "core_value_safe": core_value_safe,
        "fact_basis": fact_basis,
        "visible_answer": visible_answer,
        "expression_boundary": expression_boundary,
        "observation_tasks": observations["tasks"],
        "distinct_observation_count": observations["distinct_observation_count"],
        "repeated_observation_units": observations["repeated_units"],
        "template_id": _text(contract.get("template_id")),
        "execution_profile": _text(contract.get("execution_profile")),
        "total_duration_seconds": contract.get("total_duration_seconds"),
        "requested_hook_id": _text(requested_hook_id),
        "input_gap": gaps,
        "source_refs": {
            "audience_question": audience_refs[int(audience_index)] if audience_index else "",
            "core_value": core_refs[int(core_index)] if core_index else "",
            "fact_basis": "selling_argument.fact_evidence",
            "visible_answer": "capture_units[*].observation_job",
            "expression_boundary": "selling_argument.fact_evidence.adaptation",
        },
    }


def resolve_scene_keeper(
    stacked_scenes: Sequence[str],
    scene_family: str,
) -> Tuple[str, bool]:
    """从并列的场景里挑出与冻结场景族兼容的那一个。

    冲突里只写了 ``resolution: "KEEP_ONE"`` 和 ``allowed_scenarios: 1`` —— **没说留哪个**。
    所以这里按冻结场景族反查 ``SCENE_KEEPER_FAMILIES``。

    返回 ``(keeper, unresolved)``。冻结场景族缺失、或不在映射里时返回 ``("", True)``，
    调用方据此改为**不指认任何场景** —— 实测戒指那条的冻结场景族是 ``GENERIC_INDOOR``
    （场地族兜底值，``match_status=FALLBACK``），随便挑一个去说会和画面打架。
    """

    family = _text(scene_family).upper()
    if not family:
        return "", True
    for scene in stacked_scenes:
        text = _text(scene)
        if text and SCENE_KEEPER_FAMILIES.get(text) == family:
            return text, False
    return "", True


def narrow_core_value_to_keeper(
    core_value: str,
    stacked_scenes: Sequence[str],
    keeper: str,
) -> str:
    """把并列的场景列举收窄到只剩 keeper。**只删不换。**

    逐**片段**删，不逐词删：运营原文的并列单位是"走亲访友、参加婚礼、日常配搭"
    这样的短语，逐词删只会留下"、、都是可以的"这种残句。含非 keeper 场景词的片段
    整段丢弃，不含任何场景词的片段（"都是可以的"）保留。

    ⚠️ 已知边界：只认 ``stacked_scenes`` 里的词。词表外的场景表达（实测"走亲访友"
    不在 ``selling_fact_evidence._SCENE_TERMS`` 里）删不掉，会留在收窄结果里。
    合同会记 ``core_value_narrowed_to``，让这一点可被复核，而不是被当成功。
    """

    text = _text(core_value)
    if not text:
        return text
    drop = [
        _text(item)
        for item in stacked_scenes
        if _text(item) and _text(item) != _text(keeper)
    ]
    if not drop:
        return text
    kept: List[str] = []
    for part in _KEEPER_SEPARATORS.split(text):
        segment = part.strip()
        if not segment:
            continue
        # "处处适用"式的概括句一并删掉：specific 场景都删了，留着它等于换个
        # 说法继续承诺"多个场景"。实测戒指那条删完场景只剩"不挑场合"。
        if any(term in segment for term in _GENERIC_STACKING_TERMS):
            continue
        carries_dropped = any(term in segment for term in drop)
        carries_keeper = bool(keeper) and keeper in segment
        if carries_dropped and not carries_keeper:
            continue
        kept.append(segment)
    # **允许返回空**：宁可让"核心购买理由"缺席（下游 ``_join_parts`` 只用剩下的
    # 观察任务），也不能留一句概括承诺或残句去误导模型。
    return "、".join(kept).strip()


def _join_parts(*parts: str) -> str:
    return " → ".join(part for part in (_text(item) for item in parts) if part)


def observation_tasks(contract: Optional[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """The per-shot observation tasks, or [] when no mainline was frozen."""

    if not isinstance(contract, Mapping):
        return []
    return [dict(task) for task in (contract.get("observation_tasks") or []) if isinstance(task, Mapping)]


def repeated_observations(contract: Optional[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """Units that re-frame an observation another unit already owns.

    ``counts_as_new_observation`` is False for these, so a reviewer (or a later
    stage) can tell "第二次观察" from "同一处再看一遍".
    """

    grouped: Dict[str, List[str]] = {}
    for task in observation_tasks(contract):
        key = _text(task.get("distinct_observation_key"))
        if not key:
            continue
        grouped.setdefault(key, []).append(_text(task.get("unit_id")))
    return [
        {"distinct_observation_key": key, "unit_ids": ids}
        for key, ids in grouped.items()
        if len(ids) > 1
    ]


def distinct_observation_count(contract: Optional[Mapping[str, Any]]) -> int:
    if isinstance(contract, Mapping) and contract.get("distinct_observation_count"):
        return int(contract["distinct_observation_count"])
    return len(
        {
            _text(task.get("distinct_observation_key"))
            for task in observation_tasks(contract)
            if _text(task.get("distinct_observation_key"))
        }
    )


def rebuild_mainline_for_frozen(
    frozen_package: Optional[Mapping[str, Any]] = None,
    *,
    fact_evidence: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Reconstruct the mainline a frozen package *would* have carried.

    Plans frozen before this contract existed have no ``mixed_mainline_contract``.
    This rebuilds one from the package's own data so a reviewer can compare an
    old film against its mainline **without regenerating anything**.  It is an
    audit helper: nothing calls it during a plan, so an old package keeps its
    exact previous behaviour.

    ``fact_evidence`` may be supplied to fill the C1 half when the package
    predates that record too; otherwise the verdict fields stay empty and the
    contract reports ``FACT_BASIS_UNAVAILABLE`` instead of inventing one.
    """

    frozen = frozen_package if isinstance(frozen_package, Mapping) else {}
    if not frozen:
        return {}
    bundle = (
        frozen.get("content_bundle_brief")
        if isinstance(frozen.get("content_bundle_brief"), Mapping)
        else {}
    )
    extension = (
        frozen.get("category_execution_extension")
        if isinstance(frozen.get("category_execution_extension"), Mapping)
        else {}
    )
    argument = bundle.get("selling_argument") if isinstance(bundle.get("selling_argument"), Mapping) else {}
    return build_mixed_mainline_contract(
        selling_argument=argument,
        fact_evidence=fact_evidence
        or (argument.get("fact_evidence") if isinstance(argument.get("fact_evidence"), Mapping) else None),
        semantic_spine=(
            frozen.get("semantic_spine_contract")
            if isinstance(frozen.get("semantic_spine_contract"), Mapping)
            else bundle.get("semantic_spine_contract")
        ),
        mixed_contract=(
            extension.get("mixed_template_contract")
            if isinstance(extension.get("mixed_template_contract"), Mapping)
            else {}
        ),
        audience_tension_text=bundle.get("audience_tension_text", ""),
        requested_hook_id=frozen.get("requested_hook_id", ""),
    )


def validate_mainline_contract(contract: Optional[Mapping[str, Any]]) -> List[str]:
    """Errors that mean the frozen mainline cannot be trusted downstream."""

    if not isinstance(contract, Mapping) or not contract:
        return ["MAINLINE_CONTRACT_MISSING"]
    errors: List[str] = []
    for field in MAINLINE_FIELDS:
        value = contract.get(field)
        if value in (None, "", {}, []):
            errors.append(f"MAINLINE_FIELD_MISSING:{field}")
    for field in ("audience_question", "core_value"):
        text = _text(contract.get(field))
        if text and not _readable(text):
            # An internal identifier is not a mainline a reviewer can read back.
            errors.append(f"MAINLINE_FIELD_NOT_READABLE:{field}")
    visible = contract.get("visible_answer") if isinstance(contract.get("visible_answer"), Mapping) else {}
    if visible and not _text(visible.get("shot_ref")):
        errors.append("MAINLINE_VISIBLE_ANSWER_WITHOUT_SHOT")
    if not observation_tasks(contract):
        errors.append("MAINLINE_OBSERVATION_TASKS_EMPTY")
    return errors
