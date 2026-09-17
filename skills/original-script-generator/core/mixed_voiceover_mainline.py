"""口播核对与减法：三类核对、载荷白名单、表达边界、语言标签（方案 C3）。

方案原文（节选）：

> C3 口播写作做减法：中央口播只接收目标语言、一个主价值、必要事实、实际可见回答、
> 可选语境及体验边界，不投喂全部候选卖点／动作库／工程错误码；不逐镜报幕、不强制
> 填满 15 秒、不默认都写"我刚发现"。成稿核对分三类：可见结果落在具体镜头；规格事实
> 核对来源但不强求每句镜头证明；建议与审美不伪装为实测。……语言校验、母语自然度、
> 实际音频时长分开报告，**交付文档语言标签动态读取 ``target_language``**。

三件事，各自独立可测：

1. ``review_voiceover_against_mainline`` —— 把成稿按三类分桶核对。它只**报告**，
   不改稿；需要改时走既有的那一次定向修订。
2. ``voiceover_payload_reduction`` / ``reduce_voiceover_payload`` —— 载荷白名单。
   计划里已经只送单一卖点，这个函数把"不许送"的键显式列出来并在出现时剔除，
   所以"没有投喂全部候选"是可验证的，而不是靠复述。
3. ``compile_expression_boundary_layer`` / ``sanitize_payload_forbidden_wording`` /
   ``check_voiceover_target_against_boundary`` —— 表达边界的三段落地。
   真实批次暴露过：主线把核心价值清洗成"双层蝴蝶造型"，同一条片子的口播却说出
   "双层**纱质**蝴蝶造型"（目标语言 ``dáng bướm bằng voan hai lớp``）。清洗只落在
   ``content_mainline`` 一个字段，其余十余处正面授权字段仍是原文，模型照抄授权字段
   是合理行为 —— **禁止的措辞从来没有变成过约束**。三段各自补一个口子：
   清洗（默认清洗全部可讲字段）、禁止层（显式告诉模型不许说什么）、成品兜底
   （真写出来了要能被检出，而不是指望它没写）。
4. ``resolve_language_label`` —— 交付文档的语种标签**只**认 ``target_language``，
   绝不拿目标语言正文（``target_text``）或某个默认语种顶上。

import 无副作用、不碰数据库。没有主线合同时，核对函数返回 ``NOT_APPLICABLE``。
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from core.mixed_mainline_contract import observation_tasks
from core.selling_fact_evidence import (
    EXPERIENCE_NONE,
    TIER_APPEARANCE,
    TIER_AESTHETIC,
    TIER_EXPERIENCE,
    TIER_PERFORMANCE,
    TIER_STYLING,
    detect_unsourced_experience,
    strip_forbidden_wording,
)

CHECK_NOT_APPLICABLE = "NOT_APPLICABLE"
CHECK_PASS = "PASS"
CHECK_ATTENTION = "NEEDS_ATTENTION"
CHECK_FAIL = "FAIL"

#: 中央口播**不许**接收的块。方案点名"全部候选卖点／动作库／工程错误码"。
FORBIDDEN_PAYLOAD_KEYS: Tuple[str, ...] = (
    "selling_point_catalog",
    "selling_point_catalog_snapshot",
    "candidate_arguments",
    "all_candidates",
    "argument_candidates",
    "action_library",
    "action_catalog",
    "direction_assignments",
    "error_code",
    "error_codes",
    "errors",
    "planning_rejections",
    "traceback",
)

#: 必须带上的五类输入：目标语言、一个主价值、必要事实、可见回答、可选语境与体验边界。
#: 目标语言不在本 contract 的键里 —— 它由口播入口的调用参数 ``target_language``
#: （见 ``reality_voiceover_bridge.run_central_complete_voiceover``）与
#: ``voiceover_context_contract`` 携带。把它列进来只会让每一次检查都报一条永远
#: 修不掉的"缺件"，那是误导，不是严格。
REQUIRED_PAYLOAD_KEYS: Tuple[str, ...] = (
    "content_mainline",
    "mixed_mainline_contract",
)

#: 规格事实档：核对来源，但不强求每句话都有镜头证明。
_SPEC_TIERS: Tuple[str, ...] = (TIER_PERFORMANCE, TIER_EXPERIENCE)
#: 建议与审美档：不得伪装为实测。
_SUGGESTION_TIERS: Tuple[str, ...] = (TIER_STYLING, TIER_AESTHETIC, TIER_APPEARANCE)


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (list, tuple, set)):
        return " ".join(_text(item) for item in value if _text(item))
    return str(value).strip()


def _voice(script: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    data = script if isinstance(script, Mapping) else {}
    voice = data.get("continuous_voiceover")
    if not isinstance(voice, Mapping):
        voice = data.get("voiceover") if isinstance(data.get("voiceover"), Mapping) else {}
    return dict(voice or {})


def _storyboard(script: Optional[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    data = script if isinstance(script, Mapping) else {}
    shots = data.get("storyboard")
    if not isinstance(shots, Sequence) or isinstance(shots, (str, bytes)):
        shots = []
    return [dict(shot) for shot in shots if isinstance(shot, Mapping)]


def review_voiceover_against_mainline(
    script: Optional[Mapping[str, Any]],
    mainline: Optional[Mapping[str, Any]],
    *,
    bound_contract: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """按方案的三类口径核对成稿口播。

    * 可见结果：主线那句可见回答必须落在**具体镜头**上（``capture_unit_id``）。
    * 规格事实：核对来源，不要求每句都有镜头证明 —— 所以这里只报"来源记录了没有"。
    * 建议与审美：不得伪装为实测，用与规划期同一份体验词表检出。
    """

    contract = mainline if isinstance(mainline, Mapping) else {}
    if not contract:
        return {
            "schema_version": "mixed-voiceover-mainline-check-v1",
            "status": CHECK_NOT_APPLICABLE,
            "reason": "NO_MAINLINE_CONTRACT",
            "buckets": {},
        }
    voice = _voice(script)
    shots = _storyboard(script)
    shot_units = {
        _text(shot.get("capture_unit_id")) for shot in shots if _text(shot.get("capture_unit_id"))
    }
    visible = contract.get("visible_answer") if isinstance(contract.get("visible_answer"), Mapping) else {}
    shot_ref = _text(visible.get("shot_ref"))
    fact_type = (
        contract.get("fact_basis") if isinstance(contract.get("fact_basis"), Mapping) else {}
    )
    boundary = (
        contract.get("expression_boundary")
        if isinstance(contract.get("expression_boundary"), Mapping)
        else {}
    )

    # ── 可见结果 ────────────────────────────────────────────────────────
    if not shot_units:
        visible_status = CHECK_FAIL
        visible_detail = "成稿没有任何带 capture_unit_id 的镜头，可见结果无从落地"
    elif shot_ref and shot_ref not in shot_units:
        visible_status = CHECK_FAIL
        visible_detail = f"主线的可见回答指到 {shot_ref}，成稿里没有这一镜"
    else:
        visible_status = CHECK_PASS
        visible_detail = f"可见回答落在 {shot_ref or '（已确认镜头集）'}"
    visible_bucket = {
        "requirement": "可见结果落在具体镜头",
        "status": visible_status,
        "detail": visible_detail,
        "shot_ref": shot_ref,
        "storyboard_units": sorted(shot_units),
    }

    # ── 规格事实 ────────────────────────────────────────────────────────
    tier = _text(fact_type.get("fact_type"))
    verdict = _text(fact_type.get("verdict"))
    image_status = _text(fact_type.get("product_image_version_status"))
    if tier in _SPEC_TIERS:
        # 性能与体验需要来源；验不过就是"待补来源"，不是"改稿"。
        if verdict == "NEEDS_SOURCE":
            spec_status = CHECK_ATTENTION
            spec_detail = "该主张需要来源，当前只有运营意图；口播不得把它写成实测"
        else:
            spec_status = CHECK_PASS
            spec_detail = "该主张的来源已按规划期结论记录"
    elif tier == TIER_APPEARANCE:
        spec_status = CHECK_PASS if image_status == "AVAILABLE" else CHECK_ATTENTION
        spec_detail = (
            "外观事实有可核对的商品图片版本"
            if image_status == "AVAILABLE"
            else "外观／材质断言没有可核对的商品图片版本，只能保留观感口径"
        )
    else:
        spec_status = CHECK_PASS
        spec_detail = "该主张不依赖规格来源"
    spec_bucket = {
        "requirement": "规格事实核对来源，不要求每句都有镜头证明",
        "status": spec_status,
        "detail": spec_detail,
        "fact_tier": tier,
        "verdict": verdict,
        "product_image_version_status": image_status,
        "shot_proof_required": False,
    }

    # ── 建议与审美 ──────────────────────────────────────────────────────
    spoken = _text(voice.get("target_text")) + " " + _text(voice.get("chinese_translation"))
    experience = detect_unsourced_experience(
        spoken, experience_authority=_text(boundary.get("experience_authority")) or EXPERIENCE_NONE
    )
    allowed = _text(boundary.get("allowed_wording"))
    ceilings = [
        _text(item.get("kind"))
        for item in (boundary.get("conflicts") or [])
        if isinstance(item, Mapping) and _text(item.get("kind"))
    ]
    stacking_note = ""
    if tier in _SUGGESTION_TIERS and ceilings:
        stacking_note = "本条口播还受口径上限约束：" + "、".join(ceilings)
    suggestion_status = CHECK_ATTENTION if experience["requires_source"] else CHECK_PASS
    suggestion_bucket = {
        "requirement": "建议与审美不伪装为实测",
        "status": suggestion_status,
        "detail": (
            "口播出现未授权的过程／体验措辞"
            if experience["requires_source"]
            else "口播没有未授权的过程／体验措辞"
        ),
        "unsourced_experience_wording": experience["matched_wording"],
        "allowed_wording": allowed,
        "wording_ceilings": ceilings,
        "stacking_note": stacking_note,
    }

    buckets = {
        "visible_result": visible_bucket,
        "spec_fact": spec_bucket,
        "suggestion_and_aesthetic": suggestion_bucket,
    }
    order = (CHECK_FAIL, CHECK_ATTENTION, CHECK_PASS)
    status = CHECK_PASS
    for key in order:
        if any(bucket["status"] == key for bucket in buckets.values()):
            status = key
            break
    return {
        "schema_version": "mixed-voiceover-mainline-check-v1",
        "status": status,
        "reason": "",
        "mainline_core_value": _text(contract.get("core_value")),
        "distinct_observation_count": _distinct(contract),
        "buckets": buckets,
    }


def _distinct(contract: Mapping[str, Any]) -> int:
    keys = {
        _text(task.get("distinct_observation_key"))
        for task in observation_tasks(contract)
        if _text(task.get("distinct_observation_key"))
    }
    return len(keys)


def voiceover_payload_reduction(payload: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """What the central voiceover is allowed to receive, and what was kept out.

    Verifiable rather than asserted: the forbidden keys are listed, so a report can
    say "these are absent" instead of "we did not send all candidates".
    """

    data = payload if isinstance(payload, Mapping) else {}
    present_forbidden = [key for key in FORBIDDEN_PAYLOAD_KEYS if key in data]
    return {
        "schema_version": "mixed-voiceover-payload-reduction-v1",
        "keys_sent": sorted(str(key) for key in data),
        "forbidden_keys_present": present_forbidden,
        "reduction_ok": not present_forbidden,
        "required_keys_present": [key for key in REQUIRED_PAYLOAD_KEYS if key in data],
        "required_keys_missing": [key for key in REQUIRED_PAYLOAD_KEYS if key not in data],
    }


def reduce_voiceover_payload(payload: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """Drop the forbidden blocks.  No-op when none are present."""

    data = dict(payload) if isinstance(payload, Mapping) else {}
    for key in FORBIDDEN_PAYLOAD_KEYS:
        data.pop(key, None)
    return data


# ── 表达边界：清洗 + 禁止层 + 成品兜底 ───────────────────────────────────
#
# 分工（三条各管一段，缺一段就有对应的漏法）：
#
#   sanitize_payload_forbidden_wording  把"可讲素材"里的禁词删掉（不让它进模型）
#   compile_expression_boundary_layer   把边界变成显式禁止层（模型知道不许说什么）
#   check_voiceover_target_against_boundary  成品检（真说了要能被查出来）
#
# 只做前两段的话，模型仍可能从别处推断出被禁断言；只做第三段的话，是在等它出错。
# 三段一起，"没有把材质说成事实"才是可验证的结论而不是期望。

#: 载荷里承载显式禁止层的键。它整体是**约束**，不是可讲素材。
BOUNDARY_LAYER_KEY = "voiceover_expression_boundary"
BOUNDARY_LAYER_SCHEMA = "voiceover-expression-boundary-v1"

#: 封顶类约束的字段口径：kind -> (原文里的并列项字段, 允许条数字段)。
#: 这类约束**不**走 ``forbidden_wording``（单个场景词是对的，错的只是并列），
#: 所以成品侧必须**另有一条**检测，不能靠禁词扫描顺带覆盖。
_CEILING_RULES: Dict[str, Tuple[str, str]] = {
    "MULTI_SCENARIO_UNAUTHORIZED": ("stacked_scenes", "allowed_scenarios"),
    "MULTI_LOOK_UNAUTHORIZED": ("stacked_looks", "allowed_looks"),
}

#: 清洗豁免的命名空间：这些键下面放的就是"不许说什么"本身。
#: 把它们清掉会让约束失去内容 —— 那等于没有约束。
_BOUNDARY_NAMESPACES: Tuple[str, ...] = (
    "expression_boundary",
    "voiceover_expression_boundary",
    "forbidden_claims",
    "forbidden_leaps",
    "forbidden_tone",
    "forbidden_wording",
    "wording_ceilings",
    "conflicts",
    "allowed_wording",
    "allowed_strength",
    "hard_blocks",
    "constraint",
)


def _is_boundary_namespace(key: Any) -> bool:
    """Whether a payload key holds constraints rather than speakable material."""

    lowered = _text(key).lower()
    if not lowered:
        return False
    if "forbidden" in lowered or "boundary" in lowered:
        return True
    return lowered in _BOUNDARY_NAMESPACES


def compile_expression_boundary_layer(
    mainline: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    """把主线的表达边界编译成口播载荷里的显式禁止层。"""

    contract = mainline if isinstance(mainline, Mapping) else {}
    boundary = (
        contract.get("expression_boundary")
        if isinstance(contract.get("expression_boundary"), Mapping)
        else {}
    )
    forbidden = [
        _text(item) for item in (boundary.get("forbidden_wording") or []) if _text(item)
    ]
    ceilings = []
    for item in boundary.get("conflicts") or []:
        if not isinstance(item, Mapping):
            continue
        kind = _text(item.get("kind"))
        # 上限字段由 kind 决定：多场景看 ``allowed_scenarios``，多造型看 ``allowed_looks``。
        # 先前这里写死读 ``allowed_looks``，于是 MULTI_SCENARIO_UNAUTHORIZED 那条的
        # 上限永远编译成 None —— 模型只拿到一个类型名，不知道"最多几个"。
        _, limit_field = _CEILING_RULES.get(kind.upper(), ("", "allowed_looks"))
        ceilings.append(
            {
                "kind": kind,
                "policy": _text(item.get("policy")),
                "resolution": _text(item.get("resolution")),
                # 限值字段名一并下发，模型无需猜字段含义。
                "limit_field": limit_field,
                "max_allowed": item.get(limit_field),
                # 两个原始字段都带上：kind 之外的消费者仍可读到旧键。
                "allowed_scenarios": item.get("allowed_scenarios"),
                "allowed_looks": item.get("allowed_looks"),
            }
        )
    return {
        "schema_version": BOUNDARY_LAYER_SCHEMA,
        "forbidden_wording": forbidden,
        # 规则陈述里只用"造型、层次、排列"这类安全的外观词 —— 指引文案本身
        # 不能引入会被下游门禁拦下的类目术语。
        "forbidden_wording_rule": (
            "下列措辞在本稿的任何语言（目标语言、中译，以及它们的直译或同义改写）中"
            "都不允许出现，也不得作为可讲素材；需要描述商品时只描述可以看到的造型、"
            "层次、排列与比例。"
        )
        if forbidden
        else "",
        "allowed_wording": _text(boundary.get("allowed_wording")),
        "allowed_strength": _text(boundary.get("allowed_strength")),
        "max_main_value_count": int(boundary.get("max_main_value_count") or 1),
        "experience_authority": _text(boundary.get("experience_authority")),
        "wording_ceilings": ceilings,
        "rule_scope": "ANY_LANGUAGE_INCLUDING_TARGET_TEXT",
        "is_constraint_not_material": True,
    }


def sanitize_payload_forbidden_wording(
    payload: Optional[Mapping[str, Any]],
    terms: Sequence[str],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Remove banned wording from the *speakable* part of a payload.

    Fail-closed on purpose: every string is cleaned **unless** it lives under a
    boundary namespace.  The earlier version cleaned ``content_mainline`` only,
    which is exactly why thirteen other fields handed the model the banned
    material claim verbatim -- a whitelist of "fields worth cleaning" silently
    rots as soon as a field is added.  Only deletion, never substitution, so a
    cleaned field cannot assert anything the source did not.
    """

    banned = [_text(item) for item in terms if _text(item)]
    report: Dict[str, Any] = {
        "terms": banned,
        "scanned_strings": 0,
        "cleaned_paths": [],
        "cleaned_field_count": 0,
        # The pre-cleaning value of every field that was touched.  Some of those
        # fields (``mixed_mainline_contract.core_value``) exist to keep the
        # operator's own sentence auditable; deleting the banned wording out of
        # what the model receives is right, losing the original would not be.
        "cleaned_originals": {},
    }
    data = dict(payload) if isinstance(payload, Mapping) else {}
    if not banned:
        return data, report

    def walk(node: Any, path: List[str]) -> Any:
        if isinstance(node, Mapping):
            out: Dict[Any, Any] = {}
            for key, value in node.items():
                if _is_boundary_namespace(key):
                    out[key] = value
                else:
                    out[key] = walk(value, path + [str(key)])
            return out
        if isinstance(node, (list, tuple)):
            return [walk(item, path + [str(index)]) for index, item in enumerate(node)]
        if isinstance(node, str):
            report["scanned_strings"] += 1
            cleaned = strip_forbidden_wording(node, banned)
            if cleaned != node:
                joined = ".".join(path)
                report["cleaned_paths"].append(joined)
                report["cleaned_originals"][joined] = node
            return cleaned
        return node

    cleaned = walk(data, [])
    report["cleaned_paths"] = sorted(set(report["cleaned_paths"]))
    report["cleaned_field_count"] = len(report["cleaned_paths"])
    return cleaned, report


def declared_constraints(layer: Optional[Mapping[str, Any]]) -> List[str]:
    """这一层里**真的约束了模型**的键。

    空边界不产生任何约束，于是"没有边界的包逐字不变"这条保证仍然成立 ——
    变的只有那些**确实声明了东西**的包，而那正是先前被漏掉的部分。
    """

    data = layer if isinstance(layer, Mapping) else {}
    declared: List[str] = []
    if [item for item in (data.get("forbidden_wording") or []) if _text(item)]:
        declared.append("forbidden_wording")
    if _text(data.get("allowed_wording")):
        declared.append("allowed_wording")
    if [
        item for item in (data.get("wording_ceilings") or []) if isinstance(item, Mapping)
    ]:
        declared.append("wording_ceilings")
    if _text(data.get("allowed_strength")):
        declared.append("allowed_strength")
    return declared


def apply_expression_boundary_layer(
    payload: Optional[Mapping[str, Any]],
    mainline: Optional[Mapping[str, Any]],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """One place where the boundary is applied: clean, then declare.

    Returns the payload unchanged (byte for byte) whenever the mainline bans
    nothing, so packages without a boundary keep today's behaviour exactly.
    """

    contract = mainline if isinstance(mainline, Mapping) else {}
    boundary = (
        contract.get("expression_boundary")
        if isinstance(contract.get("expression_boundary"), Mapping)
        else {}
    )
    forbidden = [
        _text(item) for item in (boundary.get("forbidden_wording") or []) if _text(item)
    ]
    layer = compile_expression_boundary_layer(contract)
    declared = declared_constraints(layer)
    data = dict(payload) if isinstance(payload, Mapping) else {}
    report: Dict[str, Any] = {
        "schema_version": "voiceover-boundary-application-v2",
        # ``applied`` 只回答"清洗发生了没有"。它**不等于**"约束发出去了没有"，
        # 先前把两者当同一件事，恰恰让只走 allowed_wording 的封顶约束全部收不到。
        "applied": bool(forbidden),
        "terms": forbidden,
        "cleaned_paths": [],
        "cleaned_field_count": 0,
        "cleaned_originals": {},
        # ``layer_present`` 才回答"这家商品的约束有没有下发给模型"。
        "boundary_layer_present": bool(declared),
        "layer_present": bool(declared),
        "declared_constraints": declared,
    }
    if forbidden:
        data, clean_report = sanitize_payload_forbidden_wording(data, forbidden)
        report["cleaned_paths"] = clean_report["cleaned_paths"]
        report["cleaned_field_count"] = clean_report["cleaned_field_count"]
        report["scanned_strings"] = clean_report["scanned_strings"]
        report["cleaned_originals"] = clean_report["cleaned_originals"]
    # 声明与清洗**解耦**。封顶类约束（多场景、多造型）刻意不写进
    # ``forbidden_wording`` —— 见 ``selling_fact_evidence`` 的说明：单个场景词本身
    # 是对的，错的只是并列堆叠，词级禁用会把合法搭配一起误杀。于是这类约束**只**
    # 存在于 ``allowed_wording`` / ``wording_ceilings`` 里。若把下发条件绑死在
    # "有没有禁词"上，它们就永远收不到：实测手镯／戒指两条真实稿因此把三个场景
    # 并列说了出来，而 ``forbidden_wording`` 为空、下游一切检查都报 PASS。
    if declared:
        data[BOUNDARY_LAYER_KEY] = layer
    return data, report


def speakable_forbidden_hits(
    payload: Optional[Mapping[str, Any]],
    terms: Sequence[str],
) -> List[str]:
    """Dotted paths of *speakable* fields that still carry banned wording.

    Boundary namespaces are skipped on purpose -- that is where a banned term
    is supposed to appear (``forbidden_wording`` exists to name what must not
    be said).  A hit anywhere else means the model would receive the banned
    claim as material it may speak.  Returning paths lets a report *list* the
    remaining leaks instead of asserting "we cleaned it".
    """

    banned = [_text(item) for item in terms if _text(item)]
    if not banned or not isinstance(payload, Mapping):
        return []

    hits: List[str] = []

    def walk(node: Any, path: List[str]) -> None:
        if isinstance(node, Mapping):
            for key, value in node.items():
                if _is_boundary_namespace(key):
                    continue
                walk(value, path + [str(key)])
            return
        if isinstance(node, (list, tuple)):
            for index, value in enumerate(node):
                walk(value, path + [str(index)])
            return
        if isinstance(node, str) and any(term in node for term in banned):
            hits.append(".".join(path))

    walk(payload, [])
    return sorted(set(hits))


def check_voiceover_target_against_boundary(
    voice: Optional[Mapping[str, Any]] = None,
    *,
    mainline: Optional[Mapping[str, Any]] = None,
    lines: Optional[Sequence[Mapping[str, Any]]] = None,
) -> Dict[str, Any]:
    """成品兜底：口播真的写出来之后，查它有没有把被禁的断言说回来。

    判定依据是**中译侧**。这不是取巧：``chinese_translation`` 是口播引擎自报的、
    与目标语言正文语义等价的那句话，实测目标语言 ``dáng bướm bằng voan hai lớp``
    的中译就是"双层纱质蝴蝶造型" —— 禁词在中文侧命中。反过来，靠目标语言字符串
    匹配需要一份跨语言词表，凭印象造词表只会漏检并制造"已覆盖"的假象，所以这里
    如实标注检测范围与残余风险，不谎报全覆盖。
    """

    contract = mainline if isinstance(mainline, Mapping) else {}
    boundary = (
        contract.get("expression_boundary")
        if isinstance(contract.get("expression_boundary"), Mapping)
        else {}
    )
    forbidden = [
        _text(item) for item in (boundary.get("forbidden_wording") or []) if _text(item)
    ]
    ceilings = [
        item for item in (boundary.get("conflicts") or []) if isinstance(item, Mapping)
    ]
    data = voice if isinstance(voice, Mapping) else {}
    base: Dict[str, Any] = {
        "schema_version": "mixed-voiceover-boundary-check-v2",
        "forbidden_wording": forbidden,
        "violations": [],
        "ceiling_checks": [],
        "ceiling_violations": [],
        "detection_scope": ["chinese_translation", "target_text_shared_script"],
        "blocking_basis": "chinese_translation",
        "target_language_detection": "SHARED_SCRIPT_TERMS_ONLY",
        "residual_risk": (
            "目标语言等价词无词表，只有当它与中文禁词同形（如共用汉字）时才可能直接命中；"
            "若目标语言正文未如实反映在中译里，本条仍可能漏检。"
        ),
    }
    if not forbidden and not ceilings:
        return {**base, "status": CHECK_NOT_APPLICABLE, "reason": "NO_FORBIDDEN_WORDING"}
    if not forbidden and not _text(data.get("chinese_translation")):
        return {**base, "status": CHECK_NOT_APPLICABLE, "reason": "NO_CHINESE_TRANSLATION"}

    violations: List[Dict[str, Any]] = []

    def scan(text: Any, scope: str, index: int) -> None:
        haystack = _text(text)
        if not haystack:
            return
        for term in forbidden:
            if term and term in haystack:
                violations.append(
                    {
                        "scope": scope,
                        "index": index,
                        "term": term,
                        "excerpt": haystack[:120],
                    }
                )

    scan(data.get("chinese_translation"), "chinese_translation", 0)
    scan(data.get("target_text"), "target_text_shared_script", 0)
    for index, line in enumerate(lines or [], 1):
        if not isinstance(line, Mapping):
            continue
        scan(line.get("voiceover_text_zh"), "chinese_translation", index)
        scan(line.get("voiceover_text_target_language"), "target_text_shared_script", index)

    # 封顶类约束的单列判定。判据仍取**中译侧**：``stacked_scenes`` /
    # ``stacked_looks`` 是中文场景／造型词（如 日常／约会／上班），只有中文侧能
    # 同形命中；目标语言侧无词表，如实不报，不制造"已覆盖"的假象。
    ceiling_checks: List[Dict[str, Any]] = []
    ceiling_violations: List[Dict[str, Any]] = []
    zh_lines: List[Tuple[str, int, str]] = [("chinese_translation", 0, _text(data.get("chinese_translation")))]
    for index, line in enumerate(lines or [], 1):
        if isinstance(line, Mapping):
            zh_lines.append(("chinese_translation", index, _text(line.get("voiceover_text_zh"))))
    for item in ceilings:
        rule = _CEILING_RULES.get(_text(item.get("kind")).upper())
        if not rule:
            continue
        if _text(item.get("resolution")).upper() != "KEEP_ONE":
            continue
        stacked_field, allowed_field = rule
        stacked = [_text(term) for term in (item.get(stacked_field) or []) if _text(term)]
        if not stacked:
            continue
        allowed = int(item.get(allowed_field) or 1)
        for scope, index, haystack in zh_lines:
            if not haystack:
                continue
            spoken = [term for term in stacked if term in haystack]
            record: Dict[str, Any] = {
                "kind": _text(item.get("kind")),
                "scope": scope,
                "index": index,
                "stacked_terms": stacked,
                "spoken_terms": spoken,
                "allowed": allowed,
                "spoken_count": len(spoken),
                "status": CHECK_FAIL if len(spoken) > allowed else CHECK_PASS,
            }
            ceiling_checks.append(record)
            if len(spoken) > allowed:
                ceiling_violations.append({**record, "excerpt": haystack[:120]})

    return {
        **base,
        "status": CHECK_FAIL if (violations or ceiling_violations) else CHECK_PASS,
        "reason": (
            "FORBIDDEN_WORDING_SPOKEN"
            if violations
            else "CEILING_EXCEEDED"
            if ceiling_violations
            else ""
        ),
        "violations": violations,
        "violation_count": len(violations),
        "ceiling_checks": ceiling_checks,
        "ceiling_violations": ceiling_violations,
    }


def resolve_language_label(
    script: Optional[Mapping[str, Any]] = None,
    *,
    batch: Optional[Mapping[str, Any]] = None,
    voice: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """The language label a delivered document may print.

    R3 of the review: the label was printed from ``target_text`` (which is the
    *utterance in* the target language) with a fallback to a default language.
    Both are wrong: a Vietnamese script was labelled with something that is not
    its language.  The order here is ``voice.target_language`` ->
    ``batch.target_language`` -> nothing, and the utterance is never used as a
    label.  When the two disagree the mismatch is reported instead of hidden.
    """

    resolved = dict(voice) if isinstance(voice, Mapping) else _voice(script)
    row = batch if isinstance(batch, Mapping) else {}
    script_language = _text(resolved.get("target_language"))
    batch_language = _text(row.get("target_language"))
    if script_language:
        label, source = script_language, "script.continuous_voiceover.target_language"
    elif batch_language:
        label, source = batch_language, "batch.target_language"
    else:
        label, source = "", "UNAVAILABLE"
    return {
        "schema_version": "mixed-language-label-v1",
        "label": label,
        "label_source": source,
        "utterance": _text(resolved.get("target_text")),
        "utterance_is_not_a_label": True,
        "mismatch": bool(
            script_language and batch_language and script_language != batch_language
        ),
        "script_language": script_language,
        "batch_language": batch_language,
    }
