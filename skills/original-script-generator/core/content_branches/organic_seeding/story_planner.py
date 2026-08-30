"""Candidate-pool planning for product-grounded organic stories."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Mapping, Sequence

from ..contracts import stable_id


def _text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


@dataclass(frozen=True)
class OrganicContextSnapshot:
    snapshot_id: str
    campaign_goal: str
    operator_context: str
    product_code: str
    product_type: str
    target_country: str
    target_language: str
    experience_authority: str
    confirmed_experience_facts: tuple[str, ...]
    claim_snapshot_status: str
    claim_snapshot_at: str
    claim_catalog: tuple[Dict[str, Any], ...]
    product_facts: tuple[Dict[str, Any], ...]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OrganicStorySpine:
    story_id: str
    story_family: str
    campaign_goal: str
    human_trigger: str
    viewer_relevance: str
    creator_motive: str
    lived_context: str
    core_claim_ref: str
    core_value: str
    support_fact_ref: str
    support_fact: str
    visible_turn: str
    personal_realization: str
    affinity_residue: str
    discussion_tension: str
    open_loop: str
    allowed_fact_refs: tuple[str, ...]
    evidence_mode: str
    allowed_strength: str
    candidate_score: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def build_organic_context_snapshot(
    product_truth: Mapping[str, Any], theme_inputs: Sequence[Mapping[str, Any]] = ()
) -> OrganicContextSnapshot:
    primary = next((dict(item) for item in theme_inputs if isinstance(item, Mapping)), {})
    snapshot = dict(product_truth.get("governed_claim_snapshot") or {})
    catalogue = tuple(
        dict(item) for item in product_truth.get("organic_claim_catalog") or []
        if isinstance(item, Mapping)
    )
    facts = tuple(
        dict(item) for item in product_truth.get("facts") or []
        if isinstance(item, Mapping)
    )
    material = {
        "product_code": _text(product_truth.get("product_code")),
        "campaign_goal": _text(primary.get("objective")) or "STYLE_MEMORY",
        "operator_context": _text(primary.get("lived_context")),
        "experience_authority": _text(primary.get("experience_authority")).upper() or "NONE",
        "claim_snapshot_at": _text(snapshot.get("snapshot_at")),
        "claim_ids": [_text(item.get("claim_id")) for item in catalogue],
    }
    return OrganicContextSnapshot(
        snapshot_id=stable_id("ORGANIC_CONTEXT_", material),
        campaign_goal=material["campaign_goal"],
        operator_context=material["operator_context"],
        product_code=material["product_code"],
        product_type=_text(product_truth.get("product_type")) or "这个单品",
        target_country=_text(product_truth.get("target_country")),
        target_language=_text(product_truth.get("target_language")),
        experience_authority=material["experience_authority"],
        confirmed_experience_facts=tuple(
            _text(item) for item in primary.get("confirmed_experience_facts") or [] if _text(item)
        ),
        claim_snapshot_status=_text(snapshot.get("status")) or "NOT_CONFIGURED",
        claim_snapshot_at=material["claim_snapshot_at"],
        claim_catalog=catalogue,
        product_facts=facts,
    )


def _claim_story_material(claim: Mapping[str, Any], product_type: str, variant: int) -> Dict[str, str]:
    value = _text(claim.get("canonical_claim") or claim.get("text"))
    claim_type = _text(claim.get("claim_type")).lower()
    theme = _text(claim.get("claim_theme")).lower()
    if claim_type == "visual_result":
        if variant % 2 == 0:
            return {
                "story_family": "RELATABLE_DECISION",
                "human_trigger": f"喜欢{product_type}的轮廓，但担心上身后的整体比例不够利落",
                "viewer_relevance": f"正在比较{product_type}上身比例的人",
                "creator_motive": "画面里的整体结果回答了原本的顾虑，因此值得分享这次判断",
                "visible_turn": f"从完整上身关系中看见“{value}”",
                "personal_realization": "把注意力从单品本身转到穿上后的整体关系",
                "affinity_residue": "留下对利落比例的明确偏好",
                "discussion_tension": "选这类单品时，更在意单品轮廓还是上身后的整体比例",
                "open_loop": "先给出上身结果，但暂不说明真正改变判断的是哪里",
            }
        return {
            "story_family": "VISIBLE_CHOICE_RULE",
            "human_trigger": f"只看商品近景时，很难判断{product_type}是否真的适合自己的比例",
            "viewer_relevance": f"需要一个可直接复用的{product_type}观察顺序的人",
            "creator_motive": "当前画面出现了可复核的比例关系，可以把个人观察方法说清楚",
            "visible_turn": f"先看整体，再确认“{value}”是否在画面中成立",
            "personal_realization": "形成先看整体关系、再看局部细节的个人选择顺序",
            "affinity_residue": "记住的是判断方法，而不是一串商品形容词",
            "discussion_tension": "第一眼会先检查整体比例，还是先看商品细节",
            "open_loop": "先说观察顺序，稍后才给出这次画面的判断",
        }
    if claim_type == "feature" and theme == "fit":
        is_cropped = any(marker in value for marker in ("短款", "衣长", "腰线"))
        if is_cropped and variant % 2 == 0:
            return {
                "story_family": "PROPORTION_PIVOT",
                "human_trigger": f"喜欢短外套的轻快感，但担心蓬松轮廓把视觉重心往下压",
                "viewer_relevance": f"在意短外套腰线落点和整身比例的人",
                "creator_motive": "当前画面能直接看见衣长落点，因此可以解释这次比例判断从哪里改变",
                "visible_turn": f"完整上身时确认“{value}”",
                "personal_realization": "判断短外套时，衣长落点比孤立的商品近景更能改变整体印象",
                "affinity_residue": "留下轻快、重心清楚的短款印象",
                "discussion_tension": "选短外套时，会先看衣长落点还是先看蓬松轮廓",
                "open_loop": "先给出完整比例结果，中段再让观众找到腰线落点",
            }
        if is_cropped:
            return {
                "story_family": "DETAIL_TO_WHOLE",
                "human_trigger": "衣长只是一个小细节，但它可能决定整套造型的视觉重心",
                "viewer_relevance": "不想只听短款标签、希望看到真实上身关系的人",
                "creator_motive": "画面可以把腰线落点与完整比例放在同一次观察里",
                "visible_turn": f"从“{value}”过渡到完整人物与商品关系",
                "personal_realization": "真正影响判断的不是短款这个词，而是衣长在身上的具体落点",
                "affinity_residue": "记住一个能被画面复核的比例细节",
                "discussion_tension": "同样是短款，更在意衣长落点还是整体蓬松感",
                "open_loop": "先看整体结果，稍后才指出真正起作用的衣长位置",
            }
        if variant % 2 == 0:
            return {
                "story_family": "FIT_TRADEOFF",
                "human_trigger": "想要宽松轮廓的松弛感，又不想整套造型只剩体积感",
                "viewer_relevance": f"在宽松感和利落感之间做取舍的{product_type}观众",
                "creator_motive": "这个结构事实在整体画面中形成了具体取舍，不需要再靠空泛夸赞",
                "visible_turn": f"让“{value}”进入完整造型关系中被看见",
                "personal_realization": "接受一种轮廓之前，先确认它有没有破坏整套造型的重心",
                "affinity_residue": "留下有松弛感但仍有边界的印象",
                "discussion_tension": "更愿意保留宽松感，还是优先让轮廓显得干净",
                "open_loop": "先呈现取舍结果，暂时不说这次为什么没有显得拖沓",
            }
        return {
            "story_family": "SILHOUETTE_BOUNDARY",
            "human_trigger": "宽松不等于越大越好，真正难的是让轮廓有松弛感也有边界",
            "viewer_relevance": f"喜欢宽松外套、但不希望它抢走整套比例的人",
            "creator_motive": "当前画面能让版型事实和整体结果同时出现，适合讲一次发现过程",
            "visible_turn": f"从“{value}”过渡到完整轮廓与人物关系",
            "personal_realization": "真正留下印象的不是标签，而是这个事实进入整体后的结果",
            "affinity_residue": "留下一个具体、可复看的版型记忆点",
            "discussion_tension": "宽松外套更重要的是松弛感，还是轮廓边界",
            "open_loop": "先给近景或局部关系，中段再回答它为什么影响整体",
        }
    return {
        "story_family": "LIVED_DISCOVERY" if variant % 2 == 0 else "PERSONAL_STANDARD",
        "human_trigger": f"第一眼注意到{product_type}，但还缺一个具体理由决定是否记住它",
        "viewer_relevance": f"希望从真实画面而不是商品清单理解{product_type}的人",
        "creator_motive": "当前可见事实足以形成一次有边界的个人发现",
        "visible_turn": f"在生活画面里自然看见“{value}”",
        "personal_realization": "从泛泛喜欢转成一个有依据的个人判断",
        "affinity_residue": "留下对这个具体细节的轻偏好",
        "discussion_tension": "更容易被整体气质还是这个具体细节打动",
        "open_loop": "先给出个人判断，中段再让观众看见判断依据",
    }


def _claim_score(claim: Mapping[str, Any]) -> float:
    score = 50.0
    if _text(claim.get("verification_status")).upper() == "VERIFIED":
        score += 25
    score += {"core": 12, "normal": 6, "optional": 2}.get(
        _text(claim.get("operator_priority")).lower(), 0
    )
    if _text(claim.get("evidence_requirement")).lower() == "video_positive":
        score += 8
    if "VISIBLE_REASON" in (claim.get("organic_roles") or []):
        score += 5
    return score


def _fallback_claims(context: OrganicContextSnapshot) -> List[Dict[str, Any]]:
    eligible = [
        dict(item) for item in context.claim_catalog
        if item.get("organic_eligibility") == "ELIGIBLE"
    ]
    if eligible:
        return eligible
    return [
        {
            "claim_id": _text(item.get("fact_id")),
            "canonical_claim": _text(item.get("text")),
            "claim_type": _text(item.get("claim_type")) or "feature",
            "claim_theme": _text(item.get("claim_theme")),
            "verification_status": _text(item.get("verification_status")) or "LOCAL_FACT",
            "evidence_requirement": _text(item.get("evidence_requirement")) or "source_only",
            "allowed_strength": _text(item.get("allowed_strength")) or "factual",
            "operator_priority": "normal",
            "organic_roles": item.get("organic_roles") or ["VALUE_TRIGGER"],
        }
        for item in context.product_facts
        if _text(item.get("fact_id")) and _text(item.get("text"))
    ]


def plan_organic_stories(
    context: OrganicContextSnapshot, *, count: int, pool_size: int | None = None
) -> Dict[str, Any]:
    requested = max(1, min(20, int(count)))
    claims = _fallback_claims(context)
    if not claims:
        return {
            "schema_version": "organic-story-plan-v1",
            "context_snapshot": context.to_dict(),
            "candidate_pool": [],
            "selected": [],
            "status": "NO_PRODUCT_VALUE_AVAILABLE",
        }
    target_pool = max(requested * 2, 8)
    target_pool = max(requested, min(12, pool_size or target_pool))
    candidates: List[Dict[str, Any]] = []
    for sequence in range(target_pool):
        claim = claims[sequence % len(claims)]
        variant = sequence // len(claims)
        material = _claim_story_material(claim, context.product_type, variant)
        support = next(
            (
                item for item in claims
                if _text(item.get("claim_id")) != _text(claim.get("claim_id"))
                and _text(item.get("claim_source_id"))
                and _text(item.get("claim_source_id")) == _text(claim.get("claim_source_id"))
            ),
            {},
        )
        core_ref = _text(claim.get("claim_id"))
        support_ref = _text(support.get("claim_id"))
        allowed_refs = tuple(value for value in (core_ref, support_ref) if value)
        lived_context = (
            f"围绕运营给定语境“{context.operator_context}”发生的一次当前选择"
            if context.operator_context
            else "一个能自然发生当前判断、且商品已经处于正常使用状态的生活片刻"
        )
        candidate_material = {
            "context_snapshot_id": context.snapshot_id,
            "core_claim_ref": core_ref,
            "story_family": material["story_family"],
            "variant": variant,
        }
        candidates.append(
            {
                "story_id": stable_id("ORGANIC_STORY_", candidate_material),
                "story_family": material["story_family"],
                "campaign_goal": context.campaign_goal,
                "human_trigger": material["human_trigger"],
                "viewer_relevance": material["viewer_relevance"],
                "creator_motive": material["creator_motive"],
                "lived_context": lived_context,
                "core_claim_ref": core_ref,
                "core_value": _text(claim.get("canonical_claim")),
                "support_fact_ref": support_ref,
                "support_fact": _text(support.get("canonical_claim")),
                "visible_turn": material["visible_turn"],
                "personal_realization": material["personal_realization"],
                "affinity_residue": material["affinity_residue"],
                "discussion_tension": material["discussion_tension"],
                "open_loop": material["open_loop"],
                "allowed_fact_refs": allowed_refs,
                "evidence_mode": _text(claim.get("evidence_requirement")) or "source_only",
                "allowed_strength": _text(claim.get("allowed_strength")) or "factual",
                "candidate_score": _claim_score(claim) - variant * 2,
            }
        )

    selected: List[Dict[str, Any]] = []
    remaining = list(candidates)
    claim_use: Dict[str, int] = {}
    family_use: Dict[str, int] = {}
    while remaining and len(selected) < requested:
        best = max(
            remaining,
            key=lambda item: (
                float(item["candidate_score"])
                - claim_use.get(item["core_claim_ref"], 0) * 18
                - family_use.get(item["story_family"], 0) * 10,
                item["story_id"],
            ),
        )
        remaining.remove(best)
        selected.append(best)
        claim_use[best["core_claim_ref"]] = claim_use.get(best["core_claim_ref"], 0) + 1
        family_use[best["story_family"]] = family_use.get(best["story_family"], 0) + 1
    spines = [OrganicStorySpine(**item).to_dict() for item in selected]
    return {
        "schema_version": "organic-story-plan-v1",
        "context_snapshot": context.to_dict(),
        "candidate_pool": candidates,
        "selected": spines,
        "status": "READY" if spines else "NO_STORY_SELECTED",
        "selection_policy": "SOFT_SCORE_WITH_SEMANTIC_DIVERSITY",
    }
