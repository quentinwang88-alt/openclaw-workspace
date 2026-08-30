"""Organic theme allocation and branch-neutral visual-intent compilation."""
from __future__ import annotations

from typing import Any, Dict, List, Sequence

from ..contracts import SEEDING_ORGANIC, VisualIntentContract, stable_id
from ..product_evidence import visual_anchor_ids
from .contracts import OrganicSeedThemeContract, SEEDING_OBJECTIVES


DEFAULT_OBJECTIVE_ROTATION = (
    "STYLE_MEMORY",
    "SCENE_ASSOCIATION",
    "CHOICE_EDUCATION",
    "DETAIL_APPRECIATION",
    "PERSONAL_POSITION",
    "DISCUSSION",
)
DEFAULT_ROLE_ROTATION = ("SUPPORTING", "HERO", "SUPPORTING", "HERO", "INCIDENTAL", "SUPPORTING")
DEFAULT_PROMINENCE_ROTATION = ("MID", "LATE", "MID", "EARLY", "LATE", "MID")
RHETORICAL_ROTATION = (
    ("LIVED_DISCOVERY", "LIVED_MOMENT", "NATURAL_END", "ONE_FIXED_PHONE"),
    ("VISUAL_CONTRAST", "CONTRAST_FIRST", "OPEN_DISCUSSION", "ONE_FIXED_PHONE"),
    ("DETAIL_REVEAL", "DETAIL_CURIOSITY", "NATURAL_END", "FIXED_PLUS_HANDHELD"),
    ("CHOICE_RULE", "VIEWER_VALUE_FIRST", "NATURAL_END", "ONE_FIXED_PHONE"),
    ("MOVEMENT_OBSERVATION", "MOTION_FIRST", "OPEN_DISCUSSION", "FOLLOW_BROLL"),
    ("PERSONAL_POSITION", "POSITION_FIRST", "NATURAL_END", "ONE_FIXED_PHONE"),
)

ANGLE_FAMILY_ROTATION = {
    "STYLE_MEMORY": (
        ("COLOR_RELATION", "整套造型中的颜色关系", "颜色与完整造型的关系"),
        ("SILHOUETTE_PROPORTION", "轮廓和上下装比例", "商品轮廓与整套比例的关系"),
        ("DETAIL_RHYTHM", "可见细节形成的视觉节奏", "一个已授权可见细节"),
        ("LAYERING_RELATION", "商品与周围造型的层次关系", "商品加入整体造型后的层次"),
        ("MOVEMENT_IMPRESSION", "自然走动时留下的整体印象", "动态中的轮廓与可见结构"),
        ("PERSONAL_SELECTION", "个人判断造型是否完整的标准", "个人选择标准与商品事实的关系"),
    ),
    "SCENE_ASSOCIATION": (
        ("MOMENT_FIT", "一个生活时刻中的自然使用关系", "商品与当前生活时刻的关系"),
        ("OUTFIT_CONTEXT", "场景与完整造型的呼应", "商品、穿搭与场景的关系"),
        ("MOVEMENT_CONTEXT", "人在场景中移动时的可见状态", "动态场景中的商品状态"),
        ("QUIET_DETAIL", "安静停留时看到的细节", "近距离可见事实"),
        ("TRANSITION_MOMENT", "出门或到达时的过渡瞬间", "过渡动作中的商品关系"),
        ("PERSONAL_RITUAL", "一个轻量日常习惯中的搭配选择", "个人选择与商品事实"),
    ),
    "CHOICE_EDUCATION": (
        ("COLOR_CHECK", "从颜色关系建立选择标准", "可见颜色关系"),
        ("SHAPE_CHECK", "从轮廓关系建立选择标准", "可见轮廓关系"),
        ("DETAIL_CHECK", "从一个结构细节建立选择标准", "一个已授权细节"),
        ("OUTFIT_CHECK", "从完整搭配关系建立选择标准", "商品与穿搭关系"),
        ("MOTION_CHECK", "从动态状态建立选择标准", "动态中的可见状态"),
        ("PERSONAL_TRADEOFF", "说明个人最在意的取舍", "个人判断与客观事实"),
    ),
    "DETAIL_APPRECIATION": (
        ("SURFACE_DETAIL", "表面细节", "表面可见事实"),
        ("EDGE_DETAIL", "边缘或收口细节", "边缘可见事实"),
        ("CLOSURE_DETAIL", "扣件或闭合关系", "闭合结构事实"),
        ("SILHOUETTE_DETAIL", "整体轮廓细节", "轮廓事实"),
        ("MOVING_DETAIL", "动作中才看清的细节", "动态可见事实"),
        ("DETAIL_COMBINATION", "两个相邻细节的组合关系", "同主题可见事实"),
    ),
    "PERSONAL_POSITION": (
        ("COLOR_PREFERENCE", "个人颜色偏好", "颜色事实"),
        ("SHAPE_PREFERENCE", "个人轮廓偏好", "轮廓事实"),
        ("DETAIL_PREFERENCE", "个人细节偏好", "结构细节事实"),
        ("STYLING_PREFERENCE", "个人搭配偏好", "商品与穿搭关系"),
        ("SCENE_PREFERENCE", "个人场景选择", "商品与生活场景关系"),
        ("TRADEOFF_POSITION", "个人取舍标准", "客观事实与个人判断"),
    ),
    "DISCUSSION": (
        ("COLOR_CHOICE", "颜色取向讨论", "颜色关系"),
        ("SHAPE_CHOICE", "轮廓取向讨论", "轮廓关系"),
        ("DETAIL_CHOICE", "细节取向讨论", "细节关系"),
        ("STYLING_CHOICE", "搭配方式讨论", "商品与穿搭关系"),
        ("SCENE_CHOICE", "使用场景讨论", "商品与场景关系"),
        ("OVERALL_OR_DETAIL", "整体与细节优先级讨论", "整体与细节关系"),
    ),
}

SCENE_ROTATION = (
    ("HOME_PREP", "居家出门前的自然整理时刻", "FINISHING_TOUCH"),
    ("MIRROR_CHECK", "安静室内的镜前观察时刻", "MIRROR_GLANCE"),
    ("TRANSIT_PAUSE", "通勤过渡空间里的短暂停留", "ARRIVAL_PAUSE"),
    ("WORK_BREAK", "白天室内休息时的随手观察", "SEATED_CHECK"),
    ("WEEKEND_WALK", "周末外出途中自然走动的片刻", "NATURAL_WALK"),
    ("QUIET_INDOOR", "简洁室内靠窗位置的细节观察", "DETAIL_LOOK"),
)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _strings(value: Any) -> List[str]:
    if isinstance(value, str):
        return [part.strip() for part in value.replace("\r", "\n").split("\n") if part.strip()]
    if isinstance(value, Sequence):
        return [_text(item) for item in value if _text(item)]
    return []


def normalize_product_truth(product_context: Dict[str, Any]) -> Dict[str, Any]:
    facts = []
    for index, raw in enumerate(product_context.get("facts") or [], 1):
        if isinstance(raw, dict):
            text = _text(raw.get("text") or raw.get("fact_text"))
            fact_id = _text(raw.get("fact_id") or raw.get("claim_key")) or f"FACT_{index}"
        else:
            text = _text(raw)
            fact_id = f"FACT_{index}"
        if text:
            fact = {"fact_id": fact_id, "text": text}
            if isinstance(raw, dict):
                for key in (
                    "source", "claim_source_id", "claim_type", "claim_theme",
                    "allowed_strength", "evidence_requirement", "verification_status",
                    "organic_roles",
                ):
                    if raw.get(key) not in (None, "", []):
                        fact[key] = raw.get(key)
            facts.append(fact)
    return {
        "product_code": _text(product_context.get("product_code")),
        "top_category": _text(product_context.get("top_category")),
        "product_type": _text(product_context.get("product_type")),
        "target_country": _text(product_context.get("target_country")),
        "target_language": _text(product_context.get("target_language")),
        "facts": facts,
        "identity_anchors": _strings(product_context.get("identity_anchors")),
        "negative_constraints": _strings(product_context.get("negative_constraints")),
        "visual_anchors": [
            dict(item) for item in product_context.get("visual_anchors") or []
            if isinstance(item, dict)
        ],
        "governed_claim_snapshot": dict(
            product_context.get("governed_claim_snapshot") or {}
        ),
        "organic_claim_catalog": [
            dict(item) for item in product_context.get("organic_claim_catalog") or []
            if isinstance(item, dict)
        ],
    }


def _default_theme_values(
    objective: str, truth: Dict[str, Any], *, angle_label: str, proof_focus: str
) -> Dict[str, str]:
    product_type = truth.get("product_type") or "这个单品"
    fact_text = (truth.get("facts") or [{}])[0].get("text") or f"{product_type}的可见设计"
    values = {
        "STYLE_MEMORY": (f"从{angle_label}看懂{product_type}怎样留下造型记忆", angle_label, f"围绕{proof_focus}观察{fact_text}"),
        "SCENE_ASSOCIATION": (f"获得一个可直接参考的{product_type}生活场景搭配", "场景联想", f"让{product_type}自然参与一个真实生活时刻"),
        "CHOICE_EDUCATION": (f"学会从一个可见细节判断{product_type}是否适合自己的风格", "选择标准", f"只依据{fact_text}形成个人选择标准"),
        "DETAIL_APPRECIATION": (f"注意到{product_type}一个容易错过的视觉细节", "细节印象", f"用近景观察{fact_text}，不扩写功效"),
        "PERSONAL_POSITION": (f"听到一个有边界的个人审美判断，而不是购买建议", "个人立场", f"围绕{fact_text}表达偏好，明确只是个人选择"),
        "DISCUSSION": (f"获得一个关于{product_type}搭配取向的讨论题", "讨论问题", f"由{fact_text}引出开放问题，不引导购买"),
    }
    payoff, residue, connection = values[objective]
    return {"viewer_payoff": payoff, "memory_residue": residue, "product_connection": connection}


def allocate_seed_themes(
    product_context: Dict[str, Any],
    *,
    count: int,
    theme_inputs: Sequence[Dict[str, Any]] = (),
    story_spines: Sequence[Dict[str, Any]] = (),
    default_lived_context: str = "一个自然、可拍摄的日常生活时刻",
) -> List[OrganicSeedThemeContract]:
    truth = normalize_product_truth(product_context)
    if not truth["product_code"]:
        raise ValueError("PRODUCT_CODE_REQUIRED")
    if not truth["facts"] and not truth["identity_anchors"]:
        raise ValueError("PRODUCT_TRUTH_REQUIRED")
    requested = max(1, min(20, int(count)))
    inputs = [dict(item) for item in theme_inputs if isinstance(item, dict)]
    stories = [dict(item) for item in story_spines if isinstance(item, dict)]
    output: List[OrganicSeedThemeContract] = []
    all_fact_ids = tuple(item["fact_id"] for item in truth["facts"])
    all_anchor_ids = visual_anchor_ids(truth)
    for index in range(requested):
        supplied = inputs[index % len(inputs)] if inputs else {}
        story = stories[index] if index < len(stories) else {}
        objective = _text(supplied.get("objective")).upper() or DEFAULT_OBJECTIVE_ROTATION[index % len(DEFAULT_OBJECTIVE_ROTATION)]
        if objective not in SEEDING_OBJECTIVES:
            raise ValueError(f"INVALID_SEEDING_OBJECTIVE:{objective}")
        angle_family, angle_label, proof_focus = ANGLE_FAMILY_ROTATION[objective][
            index % len(ANGLE_FAMILY_ROTATION[objective])
        ]
        if story:
            angle_family = _text(story.get("story_family")).upper() or angle_family
            angle_label = _text(story.get("viewer_relevance")) or angle_label
            proof_focus = _text(story.get("core_value")) or proof_focus
        scene_family, scene_description, action_family = SCENE_ROTATION[index % len(SCENE_ROTATION)]
        rhetorical_family, hook_mechanism, closing_mode, capture_mode = RHETORICAL_ROTATION[
            index % len(RHETORICAL_ROTATION)
        ]
        defaults = _default_theme_values(
            objective, truth, angle_label=angle_label, proof_focus=proof_focus
        )
        authority = _text(supplied.get("experience_authority")).upper() or "NONE"
        confirmed = tuple(_strings(supplied.get("confirmed_experience_facts")))
        operator_requirement = _text(supplied.get("lived_context"))
        lived_context = _text(story.get("lived_context")) or scene_description
        creative_signature = stable_id(
            "SEED_CREATIVE_",
            {
                "product_code": truth["product_code"],
                "objective": objective,
                "angle_family": angle_family,
                "scene_family": scene_family,
                "action_family": action_family,
                "story_id": _text(story.get("story_id")),
            },
        )
        material = {
            "branch": SEEDING_ORGANIC,
            "product_code": truth["product_code"],
            "index": index + 1,
            "objective": objective,
            "viewer_payoff": _text(story.get("viewer_relevance")) or _text(supplied.get("viewer_payoff")) or defaults["viewer_payoff"],
            "lived_context": lived_context or default_lived_context,
            "taste_judgment": _text(supplied.get("taste_judgment")) or "只表达个人审美与观察，不替观众下购买结论",
            "creative_signature": creative_signature,
        }
        theme_id = stable_id("SEED_THEME_", material)
        output.append(
            OrganicSeedThemeContract(
                theme_id=theme_id,
                objective=objective,
                viewer_payoff=material["viewer_payoff"],
                lived_context=material["lived_context"],
                taste_judgment=material["taste_judgment"],
                product_role=_text(supplied.get("product_role")).upper() or DEFAULT_ROLE_ROTATION[index % len(DEFAULT_ROLE_ROTATION)],
                product_prominence=_text(supplied.get("product_prominence")).upper() or DEFAULT_PROMINENCE_ROTATION[index % len(DEFAULT_PROMINENCE_ROTATION)],
                memory_residue=_text(story.get("affinity_residue")) or _text(supplied.get("memory_residue")) or defaults["memory_residue"],
                product_connection=_text(story.get("visible_turn")) or _text(supplied.get("product_connection")) or defaults["product_connection"],
                allowed_fact_refs=(
                    tuple(_strings(story.get("allowed_fact_refs")))
                    or tuple(_strings(supplied.get("allowed_fact_refs")))
                    or all_fact_ids
                ),
                experience_authority=authority,
                confirmed_experience_facts=confirmed,
                forbidden_expressions=tuple(truth["negative_constraints"] + _strings(supplied.get("forbidden_expressions"))),
                interaction_ending_allowed=bool(supplied.get("interaction_ending_allowed", True)),
                viewer_value_type=_text(supplied.get("viewer_value_type")).upper() or objective,
                rhetorical_family=_text(supplied.get("rhetorical_family")).upper() or rhetorical_family,
                hook_mechanism=_text(supplied.get("hook_mechanism")).upper() or hook_mechanism,
                closing_mode=_text(supplied.get("closing_mode")).upper() or closing_mode,
                capture_mode=_text(supplied.get("capture_mode")).upper() or capture_mode,
                angle_family=_text(supplied.get("angle_family")).upper() or angle_family,
                scene_family=_text(supplied.get("scene_family")).upper() or scene_family,
                action_family=_text(supplied.get("action_family")).upper() or action_family,
                opening_relation=_text(supplied.get("opening_relation")).upper() or "LIVED_MOMENT_FIRST",
                proof_focus=_text(supplied.get("proof_focus")) or proof_focus,
                creative_signature=creative_signature,
                operator_content_requirement=operator_requirement,
                proof_anchor_refs=tuple(_strings(supplied.get("proof_anchor_refs"))) or all_anchor_ids,
            )
        )
    return output


def compile_visual_intent(
    theme: OrganicSeedThemeContract,
    *,
    carrier_requirements: Sequence[str] = (),
    structure_compatibility: Sequence[str] = (),
    topic_contract: Dict[str, Any] | None = None,
    retention_contract: Dict[str, Any] | None = None,
    story_spine: Dict[str, Any] | None = None,
) -> VisualIntentContract:
    topic = dict(topic_contract or {})
    retention = dict(retention_contract or {})
    story = dict(story_spine or {})
    return VisualIntentContract.create(
        branch_key=SEEDING_ORGANIC,
        intent_id=stable_id(
            "SEED_INTENT_",
            {
                "theme": theme.to_dict(),
                "topic_contract": topic,
                "retention_contract": retention,
                "story_spine": story,
            },
        ),
        audience_job=theme.viewer_payoff,
        product_role=theme.product_role,
        product_prominence=theme.product_prominence,
        opening_subject="生活时刻或观众收益，不以商品推销结论开场",
        carrier_requirements=carrier_requirements,
        allowed_fact_refs=theme.allowed_fact_refs,
        forbidden_inferences=theme.forbidden_expressions,
        experience_authority=theme.experience_authority,
        structure_compatibility=structure_compatibility,
        branch_payload={
            "theme_id": theme.theme_id,
            "objective": theme.objective,
            "lived_context": theme.lived_context,
            "taste_judgment": theme.taste_judgment,
            "memory_residue": theme.memory_residue,
            "product_connection": theme.product_connection,
            "confirmed_experience_facts": list(theme.confirmed_experience_facts),
            "interaction_ending_allowed": theme.interaction_ending_allowed,
            "viewer_value_type": theme.viewer_value_type,
            "rhetorical_family": theme.rhetorical_family,
            "hook_mechanism": theme.hook_mechanism,
            "closing_mode": theme.closing_mode,
            "capture_mode": theme.capture_mode,
            "proof_anchor_refs": list(theme.proof_anchor_refs),
            "topic_contract": topic,
            "retention_contract": retention,
            "story_spine": story,
            "creative_diversity": {
                "angle_family": theme.angle_family,
                "scene_family": theme.scene_family,
                "action_family": theme.action_family,
                "opening_relation": theme.opening_relation,
                "proof_focus": theme.proof_focus,
                "creative_signature": theme.creative_signature,
                "operator_content_requirement": theme.operator_content_requirement,
            },
        },
    )
