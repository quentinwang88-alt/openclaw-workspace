"""Reality-reference assets and stage-0 original-script orchestration.

The module is deliberately read-only for ``sd_*``.  It converts an already
observed video profile into a compact execution card, selects one card for a
routed direction, and validates the visual-first artifacts used by the
experimental original-script branch.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import unquote, urlparse

from core.structure_execution_compiler import compile_structure_execution_plan
from core.complete_script_v3 import assign_audio_actual


EXECUTION_CARD_SCHEMA_VERSION = "reality-execution-card-v1"
REALITY_POLICY_VERSION = "reality-reference-policy-v2"
AUTHENTICITY_POLICY_VERSION = "original-authenticity-qc-v18-light"
CONTENT_BUNDLE_SCHEMA_VERSION = "content-bundle-brief-v9-selling-point-authority"


def env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "1" if default else "0") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def reality_reference_enabled() -> bool:
    return env_flag("ORIGINAL_SCRIPT_REALITY_REFERENCE_ENABLED", False)


def reality_reference_strict() -> bool:
    return env_flag("ORIGINAL_SCRIPT_REALITY_REFERENCE_STRICT", True)


def voiceover_after_visual_enabled() -> bool:
    return env_flag("ORIGINAL_SCRIPT_VOICEOVER_AFTER_VISUAL", True)


def authenticity_qc_enabled() -> bool:
    return env_flag("ORIGINAL_SCRIPT_AUTHENTICITY_QC_ENABLED", True)


def _json_value(value: Any, default: Any) -> Any:
    if isinstance(value, (list, dict)):
        return value
    if value in (None, ""):
        return default
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return default
    return parsed


def _text(value: Any) -> str:
    return str(value or "").strip()


def _list(value: Any) -> List[Any]:
    parsed = _json_value(value, [])
    return parsed if isinstance(parsed, list) else []


def _string_list(value: Any) -> List[str]:
    return [_text(item) for item in _list(value) if _text(item)]


def _stable_id(prefix: str, value: Any) -> str:
    material = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return prefix + hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]


def _sequence_similarity(left: Sequence[str], right: Sequence[str]) -> float:
    a = [_text(item).upper() for item in left if _text(item)]
    b = [_text(item).upper() for item in right if _text(item)]
    if not a or not b:
        return 0.0
    rows = len(a) + 1
    cols = len(b) + 1
    distance = [[0] * cols for _ in range(rows)]
    for i in range(rows):
        distance[i][0] = i
    for j in range(cols):
        distance[0][j] = j
    for i in range(1, rows):
        for j in range(1, cols):
            cost = 0 if a[i - 1] == b[j - 1] else 1
            distance[i][j] = min(
                distance[i - 1][j] + 1,
                distance[i][j - 1] + 1,
                distance[i - 1][j - 1] + cost,
            )
    return max(0.0, 1.0 - distance[-1][-1] / max(len(a), len(b)))


@dataclass
class ExecutionCard:
    execution_card_id: str
    execution_card_schema_version: str
    source_profile_id: str
    source_video_id: str
    source_asset_id: str
    cluster_run_id: str
    cluster_id: int
    cluster_version: str
    evidence_tier: str
    independence_level: str
    extractor_version: str
    duration_sec: Optional[float]
    measured_shot_count: Optional[int]
    beat_sequence: List[str]
    content_carrier: str
    continuity_mode: str
    proof_mechanisms: List[str]
    source_categories: List[str]
    visual_hook_type: str
    camera_grammar: List[str]
    behavior_chain: List[str]
    generation_blockers: List[str]
    shot_execution_spine: List[Dict[str, Any]]
    audio_behavior: Dict[str, Any]
    availability: Dict[str, Any]
    adaptable_fields: List[str]
    protected_execution_fields: List[str]
    unknown_fields: List[str]
    extraction_confidence: float
    source_summary: str = ""
    selection_score: float = 0.0
    selection_reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class ExecutionReferenceRepository:
    """Read observed members that support a routed structure direction."""

    def __init__(self, database_url: str = ""):
        self.database_url = (
            database_url
            or os.environ.get("STRUCTURE_ROUTER_DATABASE_URL")
            or os.environ.get("LIKEU_AI_DATABASE_URL")
            or ""
        ).strip()

    def _connect(self):
        if not self.database_url:
            raise RuntimeError("缺少 STRUCTURE_ROUTER_DATABASE_URL / LIKEU_AI_DATABASE_URL")
        try:
            import pymysql
        except ImportError as exc:
            raise RuntimeError("真实执行参考检索需要 PyMySQL") from exc
        parsed = urlparse(self.database_url)
        return pymysql.connect(
            host=parsed.hostname,
            port=parsed.port or 3306,
            user=unquote(parsed.username or ""),
            password=unquote(parsed.password or ""),
            database=parsed.path.lstrip("/"),
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
            connect_timeout=20,
            read_timeout=60,
        )

    def load_cards_for_assignment(self, assignment: Dict[str, Any], limit: int = 80) -> List[ExecutionCard]:
        source_kind = _text(assignment.get("source_kind")).upper()
        source_run_id = _text(assignment.get("source_run_id"))
        cluster_id = int(assignment.get("cluster_id") or 0)
        cluster_version = _text(assignment.get("cluster_version")) or "UNAVAILABLE"
        if not source_run_id:
            return []

        if source_kind == "DERIVED_VIDEO_CLUSTER":
            query = """
                SELECT
                    a.cluster_run_id AS support_cluster_run_id,
                    a.cluster_id AS support_cluster_id,
                    a.assignment_confidence,
                    p.*,
                    COALESCE(p.source_cat1, e.cat1) AS support_source_cat1,
                    COALESCE(p.source_cat2, e.cat2) AS support_source_cat2,
                    e.availability_status,
                    e.local_path,
                    e.url
                FROM sd_cluster_assignment a
                JOIN sd_structure_profile p ON p.profile_id = a.profile_id
                LEFT JOIN sd_evaluation_asset e ON e.asset_id = p.asset_id
                WHERE a.cluster_run_id = %s
                  AND a.cluster_id = %s
                  AND COALESCE(a.is_noise, 0) = 0
                  AND p.profile_type = 'VIDEO_INDEPENDENT'
                ORDER BY p.extraction_confidence DESC, a.assignment_confidence DESC, p.profile_id
                LIMIT %s
            """
        else:
            # Historical prototypes are PROMPT_ONLY, but many of their video ids
            # also have a separately extracted VIDEO_INDEPENDENT profile.  Only
            # that independent profile is allowed to become an execution card.
            query = """
                SELECT
                    pa.cluster_run_id AS support_cluster_run_id,
                    pa.cluster_id AS support_cluster_id,
                    pa.assignment_confidence,
                    vp.*,
                    COALESCE(vp.source_cat1, pp.source_cat1, e.cat1) AS support_source_cat1,
                    COALESCE(vp.source_cat2, pp.source_cat2, e.cat2) AS support_source_cat2,
                    e.availability_status,
                    e.local_path,
                    e.url
                FROM sd_cluster_assignment pa
                JOIN sd_structure_profile pp ON pp.profile_id = pa.profile_id
                JOIN sd_structure_profile vp
                  ON vp.video_id = pa.video_id
                 AND vp.profile_type = 'VIDEO_INDEPENDENT'
                LEFT JOIN sd_evaluation_asset e ON e.asset_id = vp.asset_id
                WHERE pa.cluster_run_id = %s
                  AND pa.cluster_id = %s
                  AND COALESCE(pa.is_noise, 0) = 0
                ORDER BY vp.extraction_confidence DESC, pa.assignment_confidence DESC, vp.profile_id
                LIMIT %s
            """

        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (source_run_id, cluster_id, max(1, int(limit))))
                rows = cursor.fetchall()
        result: List[ExecutionCard] = []
        seen_profiles = set()
        for row in rows:
            profile_id = _text(row.get("profile_id"))
            if not profile_id or profile_id in seen_profiles:
                continue
            seen_profiles.add(profile_id)
            card = compile_execution_card(
                row,
                cluster_run_id=source_run_id,
                cluster_id=cluster_id,
                cluster_version=cluster_version,
            )
            if card:
                result.append(card)
        return result


def compile_execution_card(
    row: Dict[str, Any],
    *,
    cluster_run_id: str,
    cluster_id: int,
    cluster_version: str,
) -> Optional[ExecutionCard]:
    if _text(row.get("profile_type")).upper() != "VIDEO_INDEPENDENT":
        return None
    semantic_beats = [item for item in _list(row.get("semantic_beats")) if isinstance(item, dict)]
    if not semantic_beats:
        return None
    measured_shots = [item for item in _list(row.get("measured_shots")) if isinstance(item, dict)]
    camera_grammar = _string_list(row.get("camera_grammar"))
    action_chain = _string_list(row.get("action_chain"))
    beat_sequence = _string_list(row.get("coarse_beat_sequence"))
    if not beat_sequence:
        beat_sequence = [
            _text(item.get("coarse_beat")).upper()
            for item in semantic_beats
            if _text(item.get("coarse_beat"))
        ]
    spine: List[Dict[str, Any]] = []
    for index, beat in enumerate(semantic_beats, 1):
        action = _text(beat.get("visual_action"))
        if not action:
            continue
        camera = camera_grammar[min(index - 1, len(camera_grammar) - 1)] if camera_grammar else "UNAVAILABLE"
        spine.append(
            {
                "order": index,
                "time_range": f"{beat.get('start_sec', 'UNAVAILABLE')}-{beat.get('end_sec', 'UNAVAILABLE')}s",
                "structure_beat": _text(beat.get("coarse_beat")).upper() or "UNAVAILABLE",
                "camera_grammar": camera,
                "observable_action": action,
                "product_state": _text(beat.get("product_state")) or "UNAVAILABLE",
                "confidence": float(beat.get("confidence") or 0.0),
                "must_preserve": [
                    value
                    for value in (
                        f"镜头语法={camera}" if camera != "UNAVAILABLE" else "",
                        f"商品可见状态={_text(beat.get('product_state'))}" if _text(beat.get("product_state")) else "",
                        "动作顺序不得倒置",
                    )
                    if value
                ],
            }
        )
    if not spine:
        return None
    unknown_fields: List[str] = []
    if not camera_grammar:
        unknown_fields.append("camera_grammar")
    for field_name in ("location", "lighting", "creator_identity", "exact_styling"):
        unknown_fields.append(field_name)
    audio_mode = _text(row.get("audio_mode")) or "UNAVAILABLE"
    evidence_tier = "VIDEO_OBSERVED"
    independence = _text(row.get("independence_level")) or "UNAVAILABLE"
    card_material = {
        "profile_id": row.get("profile_id"),
        "extractor_version": row.get("extractor_version"),
        "cluster_run_id": cluster_run_id,
        "cluster_id": cluster_id,
        "cluster_version": cluster_version,
        "schema": EXECUTION_CARD_SCHEMA_VERSION,
    }
    duration = row.get("duration_sec")
    try:
        duration_value = float(duration) if duration is not None else None
    except (TypeError, ValueError):
        duration_value = None
    shot_count = row.get("measured_shot_count")
    try:
        shot_count_value = int(shot_count) if shot_count is not None else None
    except (TypeError, ValueError):
        shot_count_value = len(measured_shots) or None
    availability_status = _text(row.get("availability_status")) or "UNAVAILABLE"
    return ExecutionCard(
        execution_card_id=_stable_id("EXEC_", card_material),
        execution_card_schema_version=EXECUTION_CARD_SCHEMA_VERSION,
        source_profile_id=_text(row.get("profile_id")),
        source_video_id=_text(row.get("video_id")),
        source_asset_id=_text(row.get("asset_id")),
        cluster_run_id=cluster_run_id,
        cluster_id=cluster_id,
        cluster_version=cluster_version,
        evidence_tier=evidence_tier,
        independence_level=independence,
        extractor_version=_text(row.get("extractor_version")) or "UNAVAILABLE",
        duration_sec=duration_value,
        measured_shot_count=shot_count_value,
        beat_sequence=beat_sequence,
        content_carrier=_text(row.get("content_carrier")).upper() or "UNAVAILABLE",
        continuity_mode=_text(row.get("continuity_mode")).upper() or "UNAVAILABLE",
        proof_mechanisms=_string_list(row.get("proof_mechanisms")),
        source_categories=[
            value
            for value in (
                _text(row.get("source_cat1")),
                _text(row.get("source_cat2")),
                _text(row.get("support_source_cat1")),
                _text(row.get("support_source_cat2")),
            )
            if value
        ],
        visual_hook_type=_text(row.get("visual_hook_type")).upper() or "UNAVAILABLE",
        camera_grammar=camera_grammar,
        behavior_chain=action_chain,
        generation_blockers=_string_list(row.get("generation_blockers")),
        shot_execution_spine=spine,
        audio_behavior={
            "source_audio_mode": audio_mode,
            "spoken_moments": "UNAVAILABLE",
            "silent_moments": "UNAVAILABLE",
        },
        availability={
            "status": availability_status,
            "has_cached_file": availability_status == "AVAILABLE_CACHED",
            # Kept for audit only; callers must not assume a foreign-machine path exists.
            "source_url": _text(row.get("url")),
        },
        adaptable_fields=["product_identity", "verified_product_facts", "target_language", "voiceover"],
        protected_execution_fields=[
            "beat_order",
            "camera_grammar",
            "observable_action_order",
            "product_visibility_curve",
            "carrier_mode",
        ],
        unknown_fields=unknown_fields,
        extraction_confidence=float(row.get("extraction_confidence") or 0.0),
        source_summary=_text(row.get("extraction_notes")),
    )


def _product_family(product_type: str, top_category: str = "") -> str:
    value = f"{product_type} {top_category}".lower()
    if any(
        token in value
        for token in (
            "围巾", "丝巾", "披肩", "帽", "耳环", "耳饰", "耳线", "项链", "项圈",
            "包", "墨镜", "太阳镜", "眼镜", "scarf", "hat", "earring", "necklace", "bag",
        )
    ):
        return "WORN_ACCESSORY"
    if any(token in value for token in ("外套", "上装", "裙", "裤", "衣", "女装", "apparel")):
        return "APPAREL"
    if any(token in value for token in ("发", "耳", "项链", "手链", "戒", "配饰", "accessor")):
        return "ACCESSORY"
    return "GENERAL"


def _physical_compatibility(card: ExecutionCard, product_type: str, top_category: str = "") -> float:
    family = _product_family(product_type, top_category)
    carrier = card.content_carrier
    if family == "APPAREL":
        return {"WEARER_ACTIVE": 1.0, "MIXED": 0.85, "HAND_ONLY": 0.45, "STATIC_PRODUCT": 0.35}.get(carrier, 0.25)
    if family == "WORN_ACCESSORY":
        return {"MIXED": 1.0, "WEARER_ACTIVE": 0.95, "HAND_ONLY": 0.7, "STATIC_PRODUCT": 0.5}.get(carrier, 0.25)
    if family == "ACCESSORY":
        return {"HAND_ONLY": 1.0, "MIXED": 0.9, "STATIC_PRODUCT": 0.8, "WEARER_ACTIVE": 0.7}.get(carrier, 0.25)
    return 0.7 if carrier != "UNAVAILABLE" else 0.3


def _execution_action_compatible(card: ExecutionCard, product_type: str, top_category: str = "") -> bool:
    family = _product_family(product_type, top_category)
    action_text = " ".join(
        [
            *card.behavior_chain,
            *(
                _text(item.get("observable_action"))
                for item in card.shot_execution_spine
                if isinstance(item, dict)
            ),
        ]
    ).lower()
    categories = " ".join(card.source_categories).lower()
    if family == "APPAREL":
        accessory_only = (
            "发饰",
            "发夹",
            "hair clip",
            "hairclip",
            "hair accessory",
            "hair",
            "star clip",
            "clips",
            "ponytail",
            "adjusts hair",
            "wear the hair",
            "耳朵",
            "耳钉",
            "耳饰",
            "头发",
            "发型",
            "蓬松",
            "ear stud",
            "earring",
            "securing hair",
            "applied to her hair",
            "戴耳环",
            "耳环",
            "necklace",
            "项链",
        )
        if any(token in action_text for token in accessory_only):
            return False
        if any(token in categories for token in ("配饰", "发饰", "发夹", "耳饰", "耳环", "项链", "accessor")):
            return False
        if any(token in categories for token in ("发饰", "发夹", "hair accessory")) and not any(
            token in action_text for token in ("衣", "服装", "上身", "sleeve", "outfit", "wearing")
        ):
            return False
    if family in {"ACCESSORY", "WORN_ACCESSORY"}:
        clothing_only = ("穿外套", "穿裤", "穿裙", "put on jacket", "put on trousers")
        if any(token in action_text for token in clothing_only):
            return False
    return True


def score_execution_card(
    card: ExecutionCard,
    *,
    assignment: Dict[str, Any],
    product_type: str,
    top_category: str = "",
    recent_execution_card_ids: Optional[Iterable[str]] = None,
    recent_source_video_ids: Optional[Iterable[str]] = None,
) -> Tuple[float, List[str]]:
    contract = assignment.get("structure_contract") if isinstance(assignment.get("structure_contract"), dict) else {}
    hard = contract.get("hard_constraints") if isinstance(contract.get("hard_constraints"), dict) else {}
    expected_beats = hard.get("beat_sequence") if isinstance(hard.get("beat_sequence"), list) else []
    expected_carrier = _text(hard.get("content_carrier")).upper()
    expected_continuity = _text(hard.get("continuity_mode")).upper()
    sequence_score = _sequence_similarity(expected_beats, card.beat_sequence)
    carrier_score = 1.0 if expected_carrier and card.content_carrier == expected_carrier else 0.45
    continuity_score = 1.0 if expected_continuity and card.continuity_mode == expected_continuity else 0.55
    structure_score = 0.5 * sequence_score + 0.3 * carrier_score + 0.2 * continuity_score
    completeness = min(1.0, 0.25 + 0.15 * len(card.shot_execution_spine) + 0.08 * len(card.camera_grammar) + 0.05 * len(card.behavior_chain))
    physical = _physical_compatibility(card, product_type, top_category)
    # The router has already applied the product-flow capability contract.  A
    # member that exactly matches that routed carrier must not lose merely
    # because the broad category prior prefers another common carrier.
    if expected_carrier and card.content_carrier == expected_carrier:
        physical = max(physical, 0.8)
    feasible = 1.0
    blockers = {item.upper() for item in card.unknown_fields}
    if "CAMERA_GRAMMAR" in blockers:
        feasible -= 0.25
    action_text = " ".join(
        _text(item.get("observable_action"))
        for item in card.shot_execution_spine
        if isinstance(item, dict)
    ).lower()
    if any(token in action_text for token in ("朋友", "合影", "多人", "friend", "group", "two people")):
        feasible = min(feasible, 0.25)
    if any(str(item or "").upper() in {"MULTIPLE_PEOPLE", "COMPLEX_HUMAN_INTERACTION"} for item in card.generation_blockers):
        feasible = min(feasible, 0.25)
    freshness = 1.0 if card.availability.get("has_cached_file") else 0.65
    novelty = (
        0.1
        if card.execution_card_id in set(recent_execution_card_ids or [])
        or card.source_video_id in set(recent_source_video_ids or [])
        else 1.0
    )
    score = 100.0 * (
        0.25 * physical
        + 0.25 * completeness
        + 0.20 * structure_score
        + 0.15 * feasible
        + 0.10 * novelty
        + 0.05 * freshness
    )
    reasons = [
        f"商品交互兼容={physical:.2f}",
        f"执行信息完整={completeness:.2f}",
        f"结构匹配={structure_score:.2f}",
        f"近期新颖度={novelty:.2f}",
        f"来源可用性={freshness:.2f}",
    ]
    return round(score, 4), reasons


def select_execution_reference(
    assignment: Dict[str, Any],
    cards: Sequence[ExecutionCard],
    *,
    product_type: str,
    top_category: str = "",
    recent_execution_card_ids: Optional[Iterable[str]] = None,
    recent_source_video_ids: Optional[Iterable[str]] = None,
    strict: bool = True,
) -> Dict[str, Any]:
    ranked: List[ExecutionCard] = []
    recent_videos = set(recent_source_video_ids or [])
    for source in cards:
        if not _execution_action_compatible(source, product_type, top_category):
            continue
        if strict and source.source_video_id and source.source_video_id in recent_videos:
            continue
        card = ExecutionCard(**source.to_dict())
        score, reasons = score_execution_card(
            card,
            assignment=assignment,
            product_type=product_type,
            top_category=top_category,
            recent_execution_card_ids=recent_execution_card_ids,
            recent_source_video_ids=recent_source_video_ids,
        )
        card.selection_score = score
        card.selection_reasons = reasons
        ranked.append(card)
    ranked.sort(key=lambda item: (-item.selection_score, -item.extraction_confidence, item.execution_card_id))
    minimum = 68.0 if strict else 55.0
    selected = ranked[0] if ranked and ranked[0].selection_score >= minimum else None
    return {
        "status": "SELECTED" if selected else "REFERENCE_INSUFFICIENT",
        "policy_version": REALITY_POLICY_VERSION,
        "direction_assignment_id": assignment.get("direction_assignment_id", ""),
        "output_slot": assignment.get("output_slot", ""),
        "candidate_count": len(ranked),
        "minimum_score": minimum,
        "selected_card": selected.to_dict() if selected else None,
        "ranked_candidates": [item.to_dict() for item in ranked[:5]],
        "rejection_reason": "没有具备独立视频观察且达到执行完整度门槛的成员" if not selected else "",
    }


def build_p2_lite(
    anchor_card: Dict[str, Any],
    reference: Dict[str, Any],
    *,
    excluded_observations: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    display_anchors = [item for item in anchor_card.get("display_anchors", []) if isinstance(item, dict)]
    hard_anchors = [item for item in anchor_card.get("hard_anchors", []) if isinstance(item, dict)]
    candidates = display_anchors or hard_anchors
    excluded = {_text(item) for item in (excluded_observations or []) if _text(item)}
    mechanisms = {_text(item).upper() for item in reference.get("proof_mechanisms", [])}
    carrier = _text(reference.get("content_carrier")).upper()

    def anchor_score(item: Dict[str, Any], index: int) -> float:
        text = f"{_text(item.get('anchor'))} {_text(item.get('why_must_show'))}".lower()
        score = 10.0 - index
        if _text(item.get("anchor")) in excluded:
            score -= 100.0
        if "DETAIL_MACRO" in mechanisms and any(token in text for token in ("细节", "扣", "口袋", "袖", "领", "纹", "结构")):
            score += 20.0
        if ("NATURAL_USE" in mechanisms or carrier == "WEARER_ACTIVE") and any(
            token in text for token in ("上身", "比例", "腰线", "版型", "穿", "落点")
        ):
            score += 16.0
        if carrier in {"HAND_ONLY", "STATIC_PRODUCT"} and any(token in text for token in ("细节", "扣", "口袋", "袖", "领", "结构")):
            score += 12.0
        return score

    ranked = sorted(enumerate(candidates), key=lambda pair: (-anchor_score(pair[1], pair[0]), pair[0]))
    primary = ranked[0][1] if ranked else {}
    secondary = next(
        (item for _, item in ranked[1:] if _text(item.get("anchor")) not in excluded),
        {},
    )
    spine = reference.get("shot_execution_spine") if isinstance(reference.get("shot_execution_spine"), list) else []
    first_action = _text(spine[0].get("observable_action")) if spine and isinstance(spine[0], dict) else "UNAVAILABLE"
    proof_action = next(
        (
            _text(item.get("observable_action"))
            for item in spine
            if isinstance(item, dict) and _text(item.get("structure_beat")).upper() == "PROOF"
        ),
        first_action,
    )
    primary_observation = _text(primary.get("anchor")) or _text(anchor_card.get("product_positioning_one_liner"))
    primary_proof = (
        f"在“{proof_action}”这一真实动作关系中，让“{primary_observation}”成为唯一主要证明"
        if proof_action and proof_action != "UNAVAILABLE" and primary_observation
        else _text(primary.get("why_must_show"))
    )
    return {
        "p2_lite_schema_version": "p2-lite-v1",
        "primary_observation": primary_observation or "UNAVAILABLE",
        "primary_proof": primary_proof or "UNAVAILABLE",
        "secondary_fact": _text(secondary.get("anchor")) or "UNAVAILABLE",
        "camera_reason": first_action,
        "single_proof_rule": True,
        "source_anchor_index": 0 if primary else None,
    }


def _claim_semantic_group(text: str) -> str:
    value = _text(text).lower()
    groups = (
        ("fit_proportion", ("高腰", "腰线", "短款", "比例", "显腿", "腿部线条")),
        ("silhouette", ("宽松", "不贴", "轮廓", "包容", "廓形")),
        ("detail_structure", ("口袋", "按扣", "纽扣", "罗纹", "拉链", "领口", "袖口", "收口")),
        ("color", ("颜色", "米白", "白色", "黑色", "粉色", "色调")),
        ("material", ("面料", "纹理", "哑光", "垂坠", "弹力", "柔软")),
        ("use_scene", ("通勤", "日常", "旅行", "场景", "搭配", "上班")),
    )
    for group, tokens in groups:
        if any(token in value for token in tokens):
            return group
    return "other:" + hashlib.sha256(value.encode("utf-8")).hexdigest()[:8]


def _argument_proof_groups(value: Dict[str, Any]) -> List[str]:
    """Return the concrete visual groups that can honestly prove one value.

    This is intentionally a small, deterministic bridge rather than another
    model judgement.  A value proposition is allowed to guide the story, but
    the spoken proof must still be a visible product fact.  Unknown values
    fall back to the primary observation instead of inventing a relationship.
    """

    text = " ".join(
        _text(value.get(key)).lower()
        for key in ("text", "proof_thesis", "decision_thesis", "claim_theme")
    )
    groups: List[str] = []
    if any(token in text for token in ("腰线", "比例", "短款", "衣长", "轮廓", "身形")):
        groups.extend(["fit_proportion", "silhouette"])
    if any(token in text for token in ("细节", "结构", "扣", "拉链", "领", "袖", "口袋")):
        groups.append("detail_structure")
    if any(token in text for token in ("面料", "质感", "垂坠", "柔软", "纹理")):
        groups.append("material")
    if any(token in text for token in ("颜色", "色调", "配色")):
        groups.append("color")
    if any(token in text for token in ("日常", "通勤", "搭配", "出门", "场景")):
        groups.append("use_scene")
    return list(dict.fromkeys(groups))


def _assign_argument_proof_chain(
    atoms: List[Dict[str, Any]], value: Dict[str, Any], *, argument_available: bool
) -> Tuple[List[Dict[str, Any]], List[str], List[str], str]:
    """Assign exactly one spoken core proof and visual-only supporting facts.

    A 15-second script may show several details.  It should not automatically
    *say* every visible detail.  The first compatible atom becomes the core
    proof; the remaining atoms preserve storyboard richness as optional visual
    evidence.  Factual-only directions keep one core observation as well.
    """

    copied = [dict(atom) for atom in atoms]
    if not copied:
        return copied, [], [], ("UNPROVEN" if argument_available else "NOT_APPLICABLE")
    preferred_groups = _argument_proof_groups(value) if argument_available else []
    if argument_available:
        chosen_index = next(
            (
                index
                for index, atom in enumerate(copied)
                if _text(atom.get("semantic_group")) in preferred_groups
            ),
            None,
        )
        # An authorised value is not automatically a usable 15-second
        # argument.  When no visible fact directly supports it, preserve the
        # value for lineage but do not silently promote the first button,
        # pocket or other detail into "proof".
        if chosen_index is None:
            optional_keys: List[str] = []
            for atom in copied:
                key = _text(atom.get("claim_key"))
                atom["role"] = "optional_visual"
                atom["proof_scope"] = "VISUAL_ONLY"
                if key:
                    optional_keys.append(key)
            return copied, [], optional_keys, "UNPROVEN"
        readiness = "READY"
    else:
        chosen_index = 0
        readiness = "NOT_APPLICABLE"
    core_key = _text(copied[chosen_index].get("claim_key"))
    optional_keys: List[str] = []
    for index, atom in enumerate(copied):
        key = _text(atom.get("claim_key"))
        if index == chosen_index:
            atom["role"] = "core_proof"
            atom["proof_scope"] = "SPOKEN_CORE"
        else:
            atom["role"] = "optional_visual"
            atom["proof_scope"] = "VISUAL_ONLY"
            if key:
                optional_keys.append(key)
    return copied, ([core_key] if core_key else []), optional_keys, readiness


def _normalize_anchor_fact_for_product(text: str, product_type: str) -> str:
    """Prevent a visual proportion phrase from becoming a false product spec."""

    value = _text(text)
    product = _text(product_type).lower()
    upper_body = any(
        token in product
        for token in ("外套", "上衣", "夹克", "针织", "开衫", "衬衫", "coat", "jacket", "top")
    )
    if upper_body and "高腰" in value:
        if "短款" in value:
            return "短款衣长"
        return value.replace("高腰", "腰线附近")
    return value


def _hook_candidates_for_bundle(reference: Dict[str, Any], claim_atoms: List[Dict[str, Any]]) -> List[str]:
    """Choose speech hooks from the content thesis, not the source shot grammar.

    ``reference`` remains in the signature for backward-compatible callers,
    but DETAIL_MACRO / visual_hook_type / carrier are no longer allowed to
    promote a speech hook.  Otherwise an optional button close-up can turn a
    waist-proportion argument into a generic "look at this detail" voiceover.
    """

    del reference
    primary_atoms = [
        item
        for item in claim_atoms
        if _text(item.get("role")) in {"core_proof", "core_result"}
    ] or claim_atoms[:1]
    groups = {_text(item.get("semantic_group")) for item in primary_atoms}
    candidates: List[str] = []
    if "detail_structure" in groups:
        candidates.append("DETAIL_SURPRISE")
    if groups.intersection({"fit_proportion", "silhouette", "color", "material"}):
        candidates.extend(["VISUAL_RESULT_DIRECT", "DISCOVERY_RESULT_PROMISE"])
    candidates.extend(["GENERAL_PRODUCT_SHARE", "USER_ADVOCACY_STANCE"])
    return list(dict.fromkeys(candidates))


def _is_generation_risk_text(value: str) -> bool:
    """Keep production-generation risks out of consumer-facing hook tension."""

    text = _text(value).lower()
    return any(
        token in text
        for token in (
            "误生成", "生成成", "视频生成", "镜头", "画面", "模型", "提示词",
            "展示衣长", "看不清", "必须展示",
        )
    )


def _is_selling_argument_candidate(item: Dict[str, Any]) -> bool:
    """Return whether a catalog item is allowed to become the content thesis."""

    kind = _text(item.get("argument_kind")).upper()
    if kind:
        return kind == "SELLING_ARGUMENT"
    # Existing formal strategy cards predate the central catalog and are
    # already value-level editorial assets.  Keep them as a backward-compatible
    # fallback.  Raw feature/evidence records must explicitly opt in.
    return bool(_text(item.get("primary_selling_point") or item.get("selling_point")))


def _argument_is_compatible_with_reference(
    item: Dict[str, Any], reference_carrier: str
) -> bool:
    compatible = item.get("compatible_carriers")
    if not isinstance(compatible, list) or not compatible:
        return True
    return reference_carrier in {_text(value).upper() for value in compatible}


def _select_value_proposition(
    anchor_card: Dict[str, Any],
    reference: Dict[str, Any],
    *,
    selling_point_catalog: Optional[Iterable[Dict[str, Any]]] = None,
    product_selling_note: str = "",
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Select one governed value without mistaking a visual fact for a benefit.

    Existing formal strategy briefs are the strongest available value-level
    source in stage 0.  Anchor-card candidates remain a factual fallback.  A
    raw operator note is retained as provenance/context; it is never expanded
    into a stronger promise here.
    """

    reference_carrier = _text(reference.get("content_carrier")).upper()
    mechanisms = {_text(item).upper() for item in reference.get("proof_mechanisms", [])}
    candidates: List[Tuple[float, Dict[str, Any]]] = []
    for index, item in enumerate(selling_point_catalog or []):
        if not isinstance(item, dict):
            continue
        if not _is_selling_argument_candidate(item):
            continue
        carrier_compatible = _argument_is_compatible_with_reference(
            item, reference_carrier
        )
        text = _text(item.get("primary_selling_point") or item.get("selling_point"))
        if not text:
            continue
        role = _text(item.get("script_role"))
        searchable = " ".join(
            _text(item.get(key))
            for key in (
                "primary_selling_point",
                "dominant_user_question",
                "proof_thesis",
                "decision_thesis",
                "primary_focus",
            )
        )
        score = 100.0 - index
        if _text(item.get("source")) == "CENTRAL_VOICEOVER_VERIFIED_CLAIM":
            score += 60.0
        if _text(item.get("claim_type")).lower() == "benefit":
            score += 16.0
        # Carrier compatibility is a creative preference, not permission to
        # speak an already VERIFIED selling point.  Static or hand-led product
        # visuals may still carry an authorised use-case voiceover; they are
        # simply ranked below a naturally matching wearer execution.
        score += 18.0 if carrier_compatible else -18.0
        if reference_carrier in {"WEARER_ACTIVE", "MIXED"}:
            if role == "aura_enhancement":
                score += 40.0
            elif role == "result_delivery":
                score += 34.0
            if any(token in searchable for token in ("穿搭", "上身", "出门", "比例", "叠穿")):
                score += 18.0
        if "DETAIL_MACRO" in mechanisms:
            if role == "cognitive_reframing":
                score += 40.0
            elif role == "risk_resolution":
                score += 34.0
            if any(token in searchable for token in ("细节", "扣子", "口袋", "结构")):
                score += 18.0
        candidates.append((score, item))

    if candidates:
        selected = max(candidates, key=lambda pair: pair[0])[1]
        text = _text(selected.get("primary_selling_point") or selected.get("selling_point"))
        value = {
            "value_id": _text(selected.get("value_id")) or _stable_id(
                "VAL_", {"text": text, "source": "source_script_brief_final_strategy"}
            ),
            "text": text,
            "source": _text(selected.get("source")) or "source_script_brief_final_strategy",
            "authority": "SOURCE_AUTHORIZED",
            "allowed_strength": _text(selected.get("allowed_strength")) or "SOFT_BENEFIT",
            "status": "AVAILABLE",
            "proof_thesis": _text(selected.get("proof_thesis")),
            "decision_thesis": _text(selected.get("decision_thesis")),
            "manual_note": _text(product_selling_note),
            "source_claim_ids": list(selected.get("source_claim_ids") or []),
            "visual_dependency": _text(selected.get("visual_dependency")) or "FLEXIBLE",
            "compatible_carriers": list(selected.get("compatible_carriers") or []),
            "claim_type": _text(selected.get("claim_type")),
            "verification_status": _text(selected.get("verification_status")),
            "evidence_requirement": _text(selected.get("evidence_requirement")),
            "operator_priority": _text(selected.get("operator_priority")),
        }
        tension_text = _text(selected.get("dominant_user_question"))
        # A prior fallback accidentally promoted video-generation warnings
        # ("otherwise it may render as a long coat") into user pain.  Such
        # risks remain available to the visual layer but cannot become hooks.
        if _is_generation_risk_text(tension_text):
            tension_text = ""
        tension = {
            "text": tension_text,
            "source_value_id": value["value_id"],
            "usage_scope": "HOOK_ONLY",
            "status": "AVAILABLE" if tension_text else "UNAVAILABLE",
        }
        return value, tension

    anchor_candidates = [
        item
        for item in anchor_card.get("candidate_primary_selling_points", [])
        if isinstance(item, dict) and _text(item.get("selling_point"))
    ]
    if anchor_candidates:
        selected = anchor_candidates[0]
        text = _text(selected.get("selling_point"))
        value = {
            "value_id": _stable_id("VAL_", {"text": text, "source": "candidate_primary_selling_points"}),
            "text": text,
            "source": "candidate_primary_selling_points",
            "authority": "SOURCE_AUTHORIZED",
            "allowed_strength": "FACTUAL",
            "status": "SELLING_ARGUMENT_UNAVAILABLE",
            "proof_thesis": _text(selected.get("how_to_show")),
            "decision_thesis": "",
            "manual_note": _text(product_selling_note),
        }
        tension_text = _text(selected.get("risk_if_missed"))
        if _is_generation_risk_text(tension_text):
            tension_text = ""
        tension = {
            "text": tension_text,
            "source_value_id": value["value_id"],
            "usage_scope": "HOOK_ONLY",
            "status": "AVAILABLE" if tension_text else "UNAVAILABLE",
        }
        return value, tension

    return (
        {
            "value_id": "",
            "text": "",
            "source": "UNAVAILABLE",
            "authority": "UNAVAILABLE",
            "allowed_strength": "NONE",
            "status": "SELLING_ARGUMENT_UNAVAILABLE",
            "proof_thesis": "",
            "decision_thesis": "",
            "manual_note": _text(product_selling_note),
        },
        {
            "text": "",
            "source_value_id": "",
            "usage_scope": "HOOK_ONLY",
            "status": "UNAVAILABLE",
        },
    )


def build_content_bundle_brief(
    anchor_card: Dict[str, Any],
    reference: Dict[str, Any],
    *,
    product_type: str = "",
    excluded_observations: Optional[Iterable[str]] = None,
    max_claim_atoms: int = 3,
    selling_point_catalog: Optional[Iterable[Dict[str, Any]]] = None,
    product_selling_note: str = "",
) -> Dict[str, Any]:
    """Build one coherent 15s mainline with multiple governed fact atoms."""

    legacy = build_p2_lite(
        anchor_card,
        reference,
        excluded_observations=excluded_observations,
    )
    display = [item for item in anchor_card.get("display_anchors", []) if isinstance(item, dict)]
    hard = [item for item in anchor_card.get("hard_anchors", []) if isinstance(item, dict)]
    ordered: List[Tuple[str, Dict[str, Any], str]] = []
    primary_text = _normalize_anchor_fact_for_product(
        _text(legacy.get("primary_observation")), product_type
    )
    if primary_text and primary_text != "UNAVAILABLE":
        ordered.append((primary_text, {}, "primary_observation"))
    for source_name, rows in (("display_anchor", display), ("hard_anchor", hard)):
        for item in rows:
            text = _normalize_anchor_fact_for_product(_text(item.get("anchor")), product_type)
            if text:
                ordered.append((text, item, source_name))

    excluded = {_text(item) for item in (excluded_observations or []) if _text(item)}
    atoms: List[Dict[str, Any]] = []
    seen_text: set[str] = set()
    seen_groups: set[str] = set()
    for text, item, source_name in ordered:
        if text in seen_text or (text in excluded and text != primary_text):
            continue
        group = _claim_semantic_group(text)
        # One atom from the same semantic chain is enough. For example high-rise,
        # waist definition and leg-length visual remain one fit argument.
        if group in seen_groups:
            continue
        seen_text.add(text)
        seen_groups.add(group)
        role = "core_result" if not atoms else ("visual_proof" if len(atoms) == 1 else "supporting_value")
        fact_key = _stable_id("CLM_", {"text": text, "group": group})
        atoms.append(
            {
                "claim_key": fact_key,
                "fact_text": text,
                "semantic_group": group,
                "role": role,
                "source": source_name,
                "source_note": _text(item.get("why_must_show") or item.get("visual_evidence")),
                "requires_visual_support": True,
            }
        )

    if not atoms and primary_text:
        atoms.append(
            {
                "claim_key": _stable_id("CLM_", primary_text),
                "fact_text": primary_text,
                "semantic_group": _claim_semantic_group(primary_text),
                "role": "core_result",
                "source": "primary_observation",
                "source_note": "",
                "requires_visual_support": True,
            }
        )
    value_proposition, audience_tension = _select_value_proposition(
        anchor_card,
        reference,
        selling_point_catalog=selling_point_catalog,
        product_selling_note=product_selling_note,
    )
    selling_argument_available = _text(value_proposition.get("status")) == "AVAILABLE"
    # Determine the value before cutting the compact fact set.  Otherwise an
    # early detail anchor can crowd out the only fact that actually proves a
    # selected result (for example, a zipper detail displacing cropped length
    # for a waist-proportion argument).
    atom_limit = max(2, min(3, int(max_claim_atoms)))
    preferred_groups = set(_argument_proof_groups(value_proposition)) if selling_argument_available else set()
    if preferred_groups:
        atoms = sorted(
            atoms,
            key=lambda atom: (
                0 if _text(atom.get("semantic_group")) in preferred_groups else 1,
                0 if _text(atom.get("source")) == "primary_observation" else 1,
            ),
        )
    atoms = atoms[:atom_limit]
    atoms, core_proof_claim_keys, optional_visual_claim_keys, argument_readiness = _assign_argument_proof_chain(
        atoms,
        value_proposition,
        argument_available=selling_argument_available,
    )
    proof_match_status = (
        "MATCHED"
        if argument_readiness == "READY"
        else "UNMATCHED"
        if selling_argument_available
        else "NOT_APPLICABLE"
    )
    # The governed selling-point library decides whether a value may be used.
    # Visual semantic matching only helps choose supporting details; it must
    # not veto or downgrade a VERIFIED argument.
    argument_ready = selling_argument_available
    effective_content_mode = "SELLING_ARGUMENT" if selling_argument_available else "FACTUAL_OBSERVATION"
    original_15s_eligible = bool(selling_argument_available)
    downgrade_reason = (
        ""
        if argument_ready
        else (
            "SELLING_ARGUMENT_UNAVAILABLE"
        )
    )
    selling_argument = {
        "argument_id": _text(value_proposition.get("value_id")) if selling_argument_available else "",
        "status": "AVAILABLE" if selling_argument_available else "UNAVAILABLE",
        "source": (_text(value_proposition.get("source")) or "UNAVAILABLE") if selling_argument_available else "UNAVAILABLE",
        "source_claim_ids": list(value_proposition.get("source_claim_ids") or []) if selling_argument_available else [],
        "core_value": _text(value_proposition.get("text")) if selling_argument_available else "",
        "target_need": _text(audience_tension.get("text")) if selling_argument_available else "",
        "proof_thesis": _text(value_proposition.get("proof_thesis")) if selling_argument_available else "",
        "decision_thesis": _text(value_proposition.get("decision_thesis")) if selling_argument_available else "",
        "allowed_strength": (_text(value_proposition.get("allowed_strength")) or "NONE") if selling_argument_available else "NONE",
        "verification_status": _text(value_proposition.get("verification_status")) if selling_argument_available else "",
        "evidence_requirement": _text(value_proposition.get("evidence_requirement")) if selling_argument_available else "",
        "operator_priority": _text(value_proposition.get("operator_priority")) if selling_argument_available else "",
        "visual_dependency": _text(value_proposition.get("visual_dependency")) if selling_argument_available else "FLEXIBLE",
        "compatible_carriers": list(value_proposition.get("compatible_carriers") or []) if selling_argument_available else [],
        # These keys rank the most direct proof for backward-compatible
        # consumers.  They no longer grant the visual layer authority to decide
        # what the central voiceover must say.
        "core_proof_claim_keys": core_proof_claim_keys,
        "optional_visual_claim_keys": optional_visual_claim_keys,
        "script_readiness": "READY" if selling_argument_available else "NOT_APPLICABLE",
        "proof_match_status": proof_match_status,
        # Compatibility alias for old callers.  New consumers must use the
        # explicit split above rather than treating all visual facts as copy.
        "proof_claim_keys": core_proof_claim_keys,
    }
    preferred_hook_angles = _hook_candidates_for_bundle(reference, atoms)
    if argument_ready and audience_tension.get("status") == "AVAILABLE":
        preferred_hook_angles = list(
            dict.fromkeys(
                [
                    "PAIN_REFRAME",
                    "AUDIENCE_NEED_CALLOUT",
                    "DISCOVERY_RESULT_PROMISE",
                    *preferred_hook_angles,
                ]
            )
        )
    else:
        # An attractive value is not permission to fabricate a shopper need.
        # General friend-like sharing remains available, but a need/pain hook
        # requires an approved target_need from the source strategy.
        preferred_hook_angles = [
            item
            for item in preferred_hook_angles
            if item not in {"PAIN_REFRAME", "AUDIENCE_NEED_CALLOUT"}
        ]
    bundle_id = _stable_id(
        "CBR_",
        {
            "execution_card_id": reference.get("execution_card_id"),
            "schema": CONTENT_BUNDLE_SCHEMA_VERSION,
            "selling_argument": selling_argument,
            "audience_tension": audience_tension,
            "claims": [item.get("claim_key") for item in atoms],
            "hook_angles": preferred_hook_angles,
        },
    )
    return {
        "content_bundle_schema_version": CONTENT_BUNDLE_SCHEMA_VERSION,
        "content_bundle_id": bundle_id,
        "content_mainline": _text(value_proposition.get("text")) if argument_ready else (primary_text or _text(anchor_card.get("product_positioning_one_liner")) or "UNAVAILABLE"),
        "content_mode": effective_content_mode,
        "argument_readiness": "READY" if selling_argument_available else "NOT_APPLICABLE",
        "proof_match_status": proof_match_status,
        "original_15s_eligible": original_15s_eligible,
        "downgrade_reason": downgrade_reason,
        "recommended_flow": "ORIGINAL_15S" if original_15s_eligible else "LIGHT_VIDEO_OR_MIXCUT",
        "value_proposition": value_proposition,
        "audience_tension": audience_tension,
        "selling_argument": selling_argument,
        "proof_atoms": atoms,
        "claim_atoms": atoms,
        "claim_atom_count": len(atoms),
        "max_claim_atoms": max(2, min(3, int(max_claim_atoms))),
        "max_themes": 2,
        "primary_proof": legacy.get("primary_proof", "UNAVAILABLE"),
        "camera_reason": legacy.get("camera_reason", "UNAVAILABLE"),
        "preferred_hook_angles": preferred_hook_angles,
        "eligible_hook_ids": preferred_hook_angles,
        # Pick the content-level hook before the blueprint is written.  The
        # central voiceover engine still owns the final wording, but visual
        # design and speech now start from the same attention intent.
        "primary_hook_id": preferred_hook_angles[0] if preferred_hook_angles else "",
        "speech_policy": {
            "target_semantic_beats": 3,
            "target_speech_seconds": [11.0, 15.0],
            "allow_silent_tail": False,
            "force_audience_address": False,
            "prefer_audience_or_need_callout": True,
            "whole_video_evidence_is_sufficient": True,
        },
        "single_proof_rule": False,
    }


def build_reality_direction_packages(
    selection: Dict[str, Any],
    *,
    anchor_card: Dict[str, Any],
    product_type: str,
    top_category: str = "",
    direction_limit: int = 2,
    repository: Optional[ExecutionReferenceRepository] = None,
    strict: bool = True,
    recent_execution_card_ids: Optional[Iterable[str]] = None,
    recent_source_video_ids: Optional[Iterable[str]] = None,
    selling_point_catalog: Optional[Iterable[Dict[str, Any]]] = None,
    product_selling_note: str = "",
    allow_structure_only: bool = False,
) -> Dict[str, Any]:
    repository = repository or ExecutionReferenceRepository()
    packages: List[Dict[str, Any]] = []
    unavailable: List[Dict[str, Any]] = []
    used_observations: List[str] = []
    recent_ids = list(recent_execution_card_ids or [])
    recent_video_ids = list(recent_source_video_ids or [])
    for assignment in selection.get("assignments", []) or []:
        if not isinstance(assignment, dict):
            continue
        cards = repository.load_cards_for_assignment(assignment)
        reference_result = select_execution_reference(
            assignment,
            cards,
            product_type=product_type,
            top_category=top_category,
            recent_execution_card_ids=recent_ids,
            recent_source_video_ids=recent_video_ids,
            strict=strict,
        )
        contract = assignment.get("structure_contract") if isinstance(assignment.get("structure_contract"), dict) else {}
        structure_source_mode = "VIDEO_REFERENCED"
        if reference_result["status"] == "SELECTED":
            selected_card = dict(reference_result["selected_card"])
            selected_card["reference_status"] = "VIDEO_REFERENCED"
        elif allow_structure_only:
            # A prompt-derived cluster may control only the fields already
            # present in its versioned structure contract.  It is deliberately
            # not promoted into a fake execution card.
            hard = (
                contract.get("hard_constraints")
                if isinstance(contract.get("hard_constraints"), dict)
                else {}
            )
            selected_card = {
                "reference_status": "STRUCTURE_ONLY",
                "execution_card_id": "",
                "source_video_id": "",
                "content_carrier": _text(hard.get("content_carrier")),
                "action_spine": [],
                "camera_grammar": [],
                "shot_execution_spine": [],
                "visual_hook_type": "",
                "do_not_invent": [
                    "不得把结构聚类原型补写成源视频动作或机位证据"
                ],
            }
            structure_source_mode = "STRUCTURE_ONLY"
            reference_result = {
                **reference_result,
                "status": "STRUCTURE_ONLY",
                "selected_card": selected_card,
                "rejection_reason": _text(
                    reference_result.get("rejection_reason")
                ),
            }
        else:
            unavailable.append(reference_result)
            continue
        category_contract = (
            anchor_card.get("category_execution_contract")
            if isinstance(anchor_card.get("category_execution_contract"), dict)
            else {}
        )
        execution_plan = compile_structure_execution_plan(contract, category_contract)
        if not execution_plan or execution_plan.get("blocking_conflicts"):
            reference_result["status"] = "REFERENCE_INSUFFICIENT"
            reference_result["rejection_reason"] = "结构合同无法编译成当前原创流程可执行计划"
            unavailable.append(reference_result)
            continue
        # The selected reference is a source example; the route contract is
        # the actual production carrier.  Value compatibility must be judged
        # against the latter, otherwise a WEARER benefit can leak into an S4
        # static-product direction merely because its reference happened to
        # feature a person.
        bundle_reference = dict(selected_card)
        bundle_reference["content_carrier"] = _text(
            (contract.get("hard_constraints") or {}).get("content_carrier")
            or selected_card.get("content_carrier")
        )
        content_bundle = build_content_bundle_brief(
            anchor_card,
            bundle_reference,
            product_type=product_type,
            excluded_observations=used_observations,
            selling_point_catalog=selling_point_catalog,
            product_selling_note=product_selling_note,
        )
        primary_atom = next(
            (item for item in content_bundle.get("claim_atoms", []) if item.get("role") == "core_result"),
            {},
        )
        primary_text = _text(primary_atom.get("fact_text")) or _text(content_bundle.get("content_mainline"))
        p2_lite = {
            "p2_lite_schema_version": "p2-lite-compat-v2",
            "primary_observation": primary_text or "UNAVAILABLE",
            "primary_proof": content_bundle.get("primary_proof", "UNAVAILABLE"),
            "secondary_fact": _text(
                next(
                    (
                        item.get("fact_text")
                        for item in content_bundle.get("claim_atoms", [])[1:]
                        if _text(item.get("fact_text"))
                    ),
                    "UNAVAILABLE",
                )
            ),
            "camera_reason": content_bundle.get("camera_reason", "UNAVAILABLE"),
            "single_proof_rule": False,
        }
        used_observations.append(primary_text)
        if _text(selected_card.get("execution_card_id")):
            recent_ids.append(_text(selected_card.get("execution_card_id")))
        if _text(selected_card.get("source_video_id")):
            recent_video_ids.append(_text(selected_card.get("source_video_id")))
        packages.append(
            {
                "output_slot": assignment.get("output_slot", ""),
                "direction_assignment_id": assignment.get("direction_assignment_id", ""),
                "selection_run_id": selection.get("selection_run_id", ""),
                "cluster_id": assignment.get("cluster_id"),
                "cluster_version": assignment.get("cluster_version", ""),
                "evidence_tier": assignment.get("evidence_tier", ""),
                "structure_contract": contract,
                "structure_execution_plan": execution_plan,
                "execution_reference": selected_card,
                "structure_source_mode": structure_source_mode,
                "reference_selection": reference_result,
                "p2_lite": p2_lite,
                "content_bundle_brief": content_bundle,
            }
        )
        if len(packages) >= max(1, min(4, int(direction_limit))):
            break
    return {
        "reality_reference_policy_version": REALITY_POLICY_VERSION,
        "requested_count": max(1, min(4, int(direction_limit))),
        "selected_count": len(packages),
        "status": "READY" if packages else "REFERENCE_INSUFFICIENT",
        "directions": packages,
        "unavailable_directions": unavailable,
    }


ABSTRACT_AI_TERMS = (
    "轻判断",
    "轻满意",
    "轻安心",
    "情绪推进",
    "情绪弧",
    "决策信号",
    "氛围推进",
    "完成度",
    "人物状态",
    "镜头任务",
)
META_INSTRUCTION_TERMS = (
    "提前给出",
    "在12秒前",
    "完成决策",
    "卖点成立",
    "用户记住",
    "当前主proof",
    "secondary focus",
    "editorial_purpose",
)
OLD_ACTION_PATTERNS = {
    "mirror": re.compile(r"镜前|全身镜|看镜子"),
    "step_back": re.compile(r"半步后退|后退半步"),
    "look_down": re.compile(r"低头.*腰线|低头.*衣摆"),
    "turn": re.compile(r"转身|侧身|转回正面|轻转"),
}


def validate_visual_adaptation(
    payload: Dict[str, Any],
    *,
    execution_plan: Dict[str, Any],
    execution_reference: Dict[str, Any],
    content_bundle_brief: Optional[Dict[str, Any]] = None,
    creative_blueprint: Optional[Dict[str, Any]] = None,
    creative_diversity_contract: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    issues: List[str] = []
    shots = payload.get("shots") if isinstance(payload, dict) and isinstance(payload.get("shots"), list) else []
    plan_shots = [item for item in execution_plan.get("shot_plan", []) if isinstance(item, dict)]
    if len(shots) != len(plan_shots):
        issues.append(f"镜头数应为{len(plan_shots)}，实际为{len(shots)}")
    expected_card_id = _text(execution_reference.get("execution_card_id"))
    if _text(payload.get("execution_card_id")) != expected_card_id:
        issues.append("execution_card_id 未准确继承真实执行卡")
    bundle = content_bundle_brief if isinstance(content_bundle_brief, dict) else {}
    expected_bundle_id = _text(bundle.get("content_bundle_id"))
    if expected_bundle_id and _text(payload.get("content_bundle_id")) != expected_bundle_id:
        issues.append("content_bundle_id 未准确继承内容论证包")
    blueprint = creative_blueprint if isinstance(creative_blueprint, dict) else {}
    diversity = creative_diversity_contract if isinstance(creative_diversity_contract, dict) else {}
    if blueprint:
        if _text(payload.get("creative_blueprint_id")) != _text(blueprint.get("creative_blueprint_id")):
            issues.append("creative_blueprint_id 未准确继承完整脚本蓝图")
        if _text(payload.get("creative_design_authority")) != "CREATIVE_DESIGN":
            issues.append("视觉方案没有标记CREATIVE_DESIGN生产设计权威")
    if diversity and _text(payload.get("creative_diversity_contract_id")) != _text(diversity.get("contract_id")):
        issues.append("creative_diversity_contract_id 未准确继承")
    expected_presentation = _text(diversity.get("required_presentation_mode"))
    if expected_presentation and _text(blueprint.get("presentation_mode")) != expected_presentation:
        issues.append("完整蓝图的presentation_mode与承载合同不一致")
    expected_claim_keys = {
        _text(item.get("claim_key"))
        for item in bundle.get("claim_atoms", [])
        if isinstance(item, dict) and _text(item.get("claim_key"))
    }
    covered_claim_keys: set[str] = set()
    reference_orders = {
        int(item.get("order"))
        for item in execution_reference.get("shot_execution_spine", [])
        if isinstance(item, dict) and str(item.get("order") or "").isdigit()
    }
    inherited_order_stream: List[int] = []
    all_text_parts: List[str] = []
    hard_audio_shots: List[int] = []
    for index, plan_shot in enumerate(plan_shots):
        if index >= len(shots) or not isinstance(shots[index], dict):
            issues.append(f"缺少镜头{index + 1}")
            continue
        shot = shots[index]
        for field_name in ("shot_content", "observable_action", "product_visibility", "framing"):
            if not _text(shot.get(field_name)):
                issues.append(f"镜头{index + 1}缺少{field_name}")
        if blueprint:
            for field_name in (
                "setting_continuity",
                "action_motivation",
                "gaze_and_reaction",
                "audio_hard_constraint",
                "audio_preference",
            ):
                if not _text(shot.get(field_name)):
                    issues.append(f"镜头{index + 1}缺少{field_name}")
            if _text(shot.get("audio_hard_constraint")) not in {
                "NONE",
                "MUST_BE_SILENT",
                "MUST_KEEP_NATURAL_SOUND",
            }:
                issues.append(f"镜头{index + 1}.audio_hard_constraint无效")
            elif _text(shot.get("audio_hard_constraint")) != "NONE":
                hard_audio_shots.append(index + 1)
            if _text(shot.get("audio_preference")) not in {
                "VOICEOVER_PREFERRED",
                "SILENCE_PREFERRED",
                "AMBIENT_PREFERRED",
            }:
                issues.append(f"镜头{index + 1}.audio_preference无效")
        for field_name in ("structure_beat", "carrier_mode", "continuity_group", "opening_mechanism"):
            expected = _text(plan_shot.get(field_name))
            actual = _text(shot.get(field_name))
            if actual != expected:
                issues.append(f"镜头{index + 1}.{field_name}应为{expected or '空'}，实际为{actual or '空'}")
        raw_orders = shot.get("reference_spine_orders")
        if not isinstance(raw_orders, list) or not raw_orders:
            issues.append(f"镜头{index + 1}缺少reference_spine_orders")
        else:
            normalized_orders: List[int] = []
            for value in raw_orders:
                try:
                    order = int(value)
                except (TypeError, ValueError):
                    issues.append(f"镜头{index + 1}包含无效reference_spine_order")
                    continue
                if order not in reference_orders:
                    issues.append(f"镜头{index + 1}引用不存在的执行卡动作节点{order}")
                    continue
                normalized_orders.append(order)
            inherited_order_stream.extend(normalized_orders)
        if expected_claim_keys:
            raw_claim_keys = shot.get("supported_claim_keys")
            if not isinstance(raw_claim_keys, list):
                issues.append(f"镜头{index + 1}缺少supported_claim_keys")
            else:
                normalized_claim_keys = {_text(value) for value in raw_claim_keys if _text(value)}
                unknown = normalized_claim_keys - expected_claim_keys
                if unknown:
                    issues.append(f"镜头{index + 1}引用未知卖点键：{','.join(sorted(unknown))}")
                covered_claim_keys.update(normalized_claim_keys & expected_claim_keys)
        if _text(plan_shot.get("carrier_mode")) == "HAND_ONLY":
            visible = f"{_text(shot.get('shot_content'))} {_text(shot.get('observable_action'))}"
            if re.search(r"人物|模特|达人|全身|半身|脸|抬眼|走|转身", visible):
                issues.append(f"镜头{index + 1}违反HAND_ONLY承载")
        if _text(plan_shot.get("carrier_mode")) == "STATIC_PRODUCT":
            visible = f"{_text(shot.get('shot_content'))} {_text(shot.get('observable_action'))}"
            if re.search(r"人物|模特|达人|她|他|手部|双手|拿起|穿着|走向|开门", visible):
                issues.append(f"镜头{index + 1}违反STATIC_PRODUCT承载")
        all_text_parts.extend([_text(shot.get("shot_content")), _text(shot.get("observable_action"))])
    max_hard_audio = max(1, len(shots) // 3) if shots else 0
    if len(hard_audio_shots) > max_hard_audio:
        issues.append(
            "audio_hard_constraint使用过多：只应保护确有声音叙事必要的少数镜头，"
            f"实际{hard_audio_shots}，最多{max_hard_audio}个"
        )
    visible_text = " ".join(all_text_parts)
    for term in ABSTRACT_AI_TERMS + META_INSTRUCTION_TERMS:
        if term.lower() in visible_text.lower():
            issues.append(f"画面字段包含不可见控制词：{term}")
    if inherited_order_stream and inherited_order_stream != sorted(inherited_order_stream):
        issues.append("执行卡动作节点顺序被倒置")
    if reference_orders and not reference_orders.issubset(set(inherited_order_stream)):
        issues.append("视觉方案未覆盖执行卡的全部动作节点")
    missing_claim_keys = expected_claim_keys - covered_claim_keys
    if missing_claim_keys:
        issues.append(f"视觉方案没有为全部卖点提供画面支持：{','.join(sorted(missing_claim_keys))}")
    return {
        "policy_version": AUTHENTICITY_POLICY_VERSION,
        "valid": not issues,
        "issues": issues,
        "observed_shot_count": len(shots),
        "covered_claim_keys": sorted(covered_claim_keys),
    }


def project_event_blueprint_to_visual_plan(
    *,
    direction: Dict[str, Any],
) -> Dict[str, Any]:
    """Project one coherent event into legacy shot slots without another LLM.

    The model owns three macro passages.  Code owns slot count, structure
    fields, source-order lineage and factual coverage.  Repeated slots remain
    continuations of the same passage rather than new product demonstrations.
    """

    blueprint = (
        direction.get("creative_blueprint")
        if isinstance(direction.get("creative_blueprint"), dict)
        else {}
    )
    passages = [
        dict(item)
        for item in blueprint.get("macro_visual_passages", [])
        if isinstance(item, dict)
    ]
    if len(passages) != 3:
        raise ValueError("事件蓝图必须包含3段macro_visual_passages")
    execution_plan = (
        direction.get("structure_execution_plan")
        if isinstance(direction.get("structure_execution_plan"), dict)
        else {}
    )
    plan_shots = [
        item for item in execution_plan.get("shot_plan", []) if isinstance(item, dict)
    ]
    if not plan_shots:
        raise ValueError("结构执行计划没有可投影的shot_plan")
    reference = (
        direction.get("execution_reference")
        if isinstance(direction.get("execution_reference"), dict)
        else {}
    )
    source_orders = sorted(
        {
            int(item.get("order"))
            for item in reference.get("shot_execution_spine", [])
            if isinstance(item, dict) and str(item.get("order") or "").isdigit()
        }
    )
    if not source_orders:
        raise ValueError("真实执行卡没有可用的shot_execution_spine顺序")

    bundle = (
        direction.get("content_bundle_brief")
        if isinstance(direction.get("content_bundle_brief"), dict)
        else {}
    )
    claim_atoms = [
        item for item in bundle.get("claim_atoms", []) if isinstance(item, dict)
    ]
    claim_text = {
        _text(item.get("claim_key")): _text(item.get("fact_text"))
        for item in claim_atoms
        if _text(item.get("claim_key")) and _text(item.get("fact_text"))
    }
    expected_claim_keys = set(claim_text)
    for passage in passages:
        passage["supported_claim_keys"] = [
            key
            for key in (_text(item) for item in passage.get("supported_claim_keys", []))
            if key in expected_claim_keys
        ]
    covered = {
        key for passage in passages for key in passage.get("supported_claim_keys", [])
    }
    missing = [key for key in claim_text if key not in covered]
    if missing:
        proof_passage = passages[1]
        proof_passage["supported_claim_keys"] = list(
            dict.fromkeys([*proof_passage.get("supported_claim_keys", []), *missing])
        )
        visible_facts = "、".join(claim_text[key] for key in missing)
        carrier = _text(
            direction.get("creative_diversity_contract", {}).get("required_carrier")
        ).upper()
        carrier_subject = (
            "同一静物承载" if carrier == "STATIC_PRODUCT"
            else "同一手部操作" if carrier == "HAND_ONLY"
            else "同一连续画面"
        )
        proof_passage["visible_process"] = (
            _text(proof_passage.get("visible_process"))
            + f"；{carrier_subject}中自然可见{visible_facts}，不为这些细节增加逐项展示动作"
        ).strip("；")

    scene = blueprint.get("scene") if isinstance(blueprint.get("scene"), dict) else {}
    event = (
        blueprint.get("event_design")
        if isinstance(blueprint.get("event_design"), dict)
        else {}
    )
    shots: List[Dict[str, Any]] = []
    claim_coverage: Dict[str, List[int]] = {key: [] for key in claim_text}
    slot_count = len(plan_shots)
    for index, plan_shot in enumerate(plan_shots):
        passage_index = round(index * (len(passages) - 1) / max(1, slot_count - 1))
        passage = passages[passage_index]
        source_index = round(index * (len(source_orders) - 1) / max(1, slot_count - 1))
        order = source_orders[source_index]
        supported = list(passage.get("supported_claim_keys") or [])
        shot_no = index + 1
        for key in supported:
            claim_coverage.setdefault(key, []).append(shot_no)
        anchor_text = "；".join(claim_text[key] for key in supported if key in claim_text)
        shots.append(
            {
                "shot_no": shot_no,
                "duration": _text(plan_shot.get("time_range")),
                "shot_content": _text(passage.get("visible_process")),
                "observable_action": _text(passage.get("observable_action")),
                "product_visibility": _text(passage.get("product_visibility")) or "PARTIAL",
                "framing": _text(passage.get("camera_observation")),
                "anchor_reference": anchor_text or "UNAVAILABLE",
                "supported_claim_keys": supported,
                "reference_spine_orders": [order],
                "editorial_purpose": _text(passage.get("narrative_role")),
                "setting_continuity": "；".join(
                    item
                    for item in (
                        _text(scene.get("location")),
                        _text(scene.get("moment")),
                        _text(scene.get("lighting")),
                    )
                    if item
                ),
                "action_motivation": _text(event.get("natural_event")),
                "gaze_and_reaction": "NATURAL_UNDIRECTED",
                "audio_hard_constraint": "NONE",
                "audio_preference": "VOICEOVER_PREFERRED",
                "structure_beat": _text(plan_shot.get("structure_beat")),
                "carrier_mode": _text(plan_shot.get("carrier_mode")),
                "continuity_group": _text(plan_shot.get("continuity_group")),
                "opening_mechanism": _text(plan_shot.get("opening_mechanism")),
            }
        )
    return {
        "visual_plan_schema_version": "event-blueprint-projection-v1",
        "source": "DETERMINISTIC_EVENT_PROJECTION",
        "execution_card_id": _text(reference.get("execution_card_id")),
        "content_bundle_id": _text(bundle.get("content_bundle_id")),
        "creative_blueprint_id": _text(blueprint.get("creative_blueprint_id")),
        "creative_diversity_contract_id": _text(
            direction.get("creative_diversity_contract", {}).get("contract_id")
        ),
        "creative_design_authority": "CREATIVE_DESIGN",
        "primary_observation": _text(event.get("core_result_moment")),
        "macro_visual_passages": passages,
        "shots": shots,
        "claim_coverage_summary": claim_coverage,
        "reference_preservation_note": "按真实执行卡order单调投影，不增加逐卖点动作",
        "unknowns_preserved": list(reference.get("unknown_fields") or []),
    }


def validate_voiceover_plan(payload: Dict[str, Any], shot_count: int) -> Dict[str, Any]:
    issues: List[str] = []
    lines = payload.get("lines") if isinstance(payload, dict) and isinstance(payload.get("lines"), list) else []
    silent = payload.get("silent_shots") if isinstance(payload, dict) and isinstance(payload.get("silent_shots"), list) else []
    seen = set()
    covered_shots = set()
    for item in lines:
        if not isinstance(item, dict):
            issues.append("口播lines包含非对象项")
            continue
        try:
            shot_no = int(item.get("shot_no"))
        except (TypeError, ValueError):
            issues.append("口播行缺少有效shot_no")
            continue
        if shot_no < 1 or shot_no > shot_count:
            issues.append(f"口播shot_no越界：{shot_no}")
        if shot_no in seen:
            issues.append(f"镜头{shot_no}存在重复口播")
        seen.add(shot_no)
        try:
            end_shot_no = int(item.get("end_shot_no") or shot_no)
        except (TypeError, ValueError):
            end_shot_no = shot_no
            issues.append(f"镜头{shot_no}缺少有效end_shot_no")
        if end_shot_no < shot_no or end_shot_no > shot_count:
            issues.append(f"口播end_shot_no越界：{end_shot_no}")
        else:
            covered_shots.update(range(shot_no, end_shot_no + 1))
        if not _text(item.get("voiceover_text_target_language")):
            issues.append(f"镜头{shot_no}缺少目标语言口播")
        if not _text(item.get("voiceover_text_zh")):
            issues.append(f"镜头{shot_no}缺少中文对照")
    expression = payload.get("expression_contract") if isinstance(payload.get("expression_contract"), dict) else {}
    minimum_silence_ms = int(
        payload.get("minimum_silence_window_ms")
        if payload.get("minimum_silence_window_ms") is not None
        else expression.get("audio_policy", {}).get("minimum_natural_sound_window_ms", 0)
    )
    silent_windows = [
        item
        for item in payload.get("silent_windows", [])
        if isinstance(item, dict)
        and int(item.get("duration_ms") or 0) >= minimum_silence_ms
    ]
    schema_version = _text(payload.get("voiceover_plan_schema_version"))
    if schema_version in {
        "visual-first-voiceover-v2",
        "visual-first-voiceover-v3",
        "visual-first-voiceover-v4",
    }:
        copy_plan = payload.get("copy_plan") if isinstance(payload.get("copy_plan"), dict) else {}
        if expression.get("schema_version") != "voiceover-expression-contract-v2":
            issues.append("V2口播缺少中央表达合同")
        hook = expression.get("hook_contract") if isinstance(expression.get("hook_contract"), dict) else {}
        if not _text(hook.get("core_intent")) or not hook.get("minimal_structure"):
            issues.append("V2口播没有消费完整钩子意图")
        if copy_plan.get("schema_version") != "voiceover-expression-plan-v2":
            issues.append("V2口播缺少可执行表达计划")
        if not _text(copy_plan.get("content_mainline")):
            issues.append("V2表达计划缺少内容主线")
        plan_beats = [item for item in copy_plan.get("beats", []) if isinstance(item, dict)]
        if not plan_beats or any(
            not _text(item.get("speech_intent_zh")) or not _text(item.get("speech_act"))
            for item in plan_beats
        ):
            issues.append("V2表达计划缺少逐段说话动作")
        if schema_version in {"visual-first-voiceover-v3", "visual-first-voiceover-v4"}:
            surface = expression.get("hook_surface_contract") if isinstance(expression.get("hook_surface_contract"), dict) else {}
            creative_voice = expression.get("creative_voice_context") if isinstance(expression.get("creative_voice_context"), dict) else {}
            audio_policy = expression.get("audio_policy") if isinstance(expression.get("audio_policy"), dict) else {}
            if not _text(surface.get("attention_move")) or not surface.get("surface_options"):
                issues.append("V3口播没有消费话术钩子表层合同")
            if not _text(creative_voice.get("speaker_identity")) or not _text(creative_voice.get("viewer_relationship")):
                issues.append("V3口播缺少人物身份或观众关系")
            if audio_policy.get("authority_order") != [
                "audio_hard_constraint",
                "central_voiceover_semantic_segments",
                "audio_preference",
            ]:
                issues.append("V3口播音频权威顺序不正确")
    return {
        "valid": not issues,
        "issues": issues,
        "spoken_shots": sorted(covered_shots),
        "silent_shots": silent,
        "silent_windows": silent_windows,
        "minimum_silence_window_ms": minimum_silence_ms,
    }


def validate_voiceover_visual_grounding(
    payload: Dict[str, Any],
    *,
    primary_observation: str,
    first_shot_content: str,
) -> Dict[str, Any]:
    issues: List[str] = []
    lines = [item for item in payload.get("lines", []) if isinstance(item, dict)]
    first = min(lines, key=lambda item: int(item.get("shot_no") or 999)) if lines else {}
    first_zh = _text(first.get("voiceover_text_zh"))
    if not first_zh:
        issues.append("缺少首句中文对照，无法检查视觉落地")
    # The opening may lead with either the direction's primary observation or
    # another fact genuinely visible in shot 1. Generic words remain excluded,
    # so "版型很清楚" cannot pass merely because the shot contains a product.
    source_chunks = re.findall(
        r"[\u4e00-\u9fff]{2,}",
        " ".join((_text(primary_observation), _text(first_shot_content))),
    )
    meaningful_bigrams = {
        chunk[index : index + 2]
        for chunk in source_chunks
        for index in range(max(0, len(chunk) - 1))
        if chunk[index : index + 2]
        not in {"画面", "商品", "人物", "上身", "结果", "可见", "清楚", "版型", "效果"}
    }
    if first_zh and meaningful_bigrams and not any(token in first_zh for token in meaningful_bigrams):
        issues.append("首句没有落到当前首镜或primary_observation的具体视觉事实")
    normalized_first = re.sub(r"[^\u4e00-\u9fffA-Za-z0-9]", "", first_zh)
    generic_phrases = (
        "姐妹们先看",
        "先看镜头效果",
        "版型真的很清楚",
        "真的很好搭",
        "这个一定要看",
        "喜欢这个效果",
    )
    for phrase in generic_phrases:
        if phrase in normalized_first:
            issues.append(f"首句命中通用电商话术：{phrase}")
    return {"valid": not issues, "issues": issues, "first_line_zh": first_zh}


def authenticity_review(script_json: Dict[str, Any]) -> Dict[str, Any]:
    storyboard = script_json.get("storyboard") if isinstance(script_json.get("storyboard"), list) else []
    visible_text = " ".join(
        " ".join(
            _text(item.get(key))
            for key in ("shot_content", "observable_action", "person_action", "voiceover_text_zh")
        )
        for item in storyboard
        if isinstance(item, dict)
    )
    issues: List[Dict[str, Any]] = []
    for term in ABSTRACT_AI_TERMS:
        if term in visible_text:
            issues.append({"code": "ABSTRACT_AI_TERM", "term": term, "blocking": True})
    for term in META_INSTRUCTION_TERMS:
        if term.lower() in visible_text.lower():
            issues.append({"code": "META_INSTRUCTION_LEAK", "term": term, "blocking": True})
    old_chain_hits = [name for name, pattern in OLD_ACTION_PATTERNS.items() if pattern.search(visible_text)]
    if len(old_chain_hits) >= 4:
        issues.append({"code": "LEGACY_ACTION_CHAIN", "hits": old_chain_hits, "blocking": True})
    spoken_count = sum(
        1
        for item in storyboard
        if isinstance(item, dict)
        and (
            _text(item.get("audio_actual"))
            in {"VOICEOVER", "VOICEOVER_CONTINUATION", "VOICEOVER_WITH_NATURAL_SOUND"}
            or (
                not _text(item.get("audio_actual"))
                and _text(item.get("voiceover_text_target_language"))
            )
        )
    )
    audio_plan = script_json.get("audio_plan") if isinstance(script_json.get("audio_plan"), dict) else {}
    minimum_silence_ms = int(audio_plan.get("minimum_silence_window_ms") or 0)
    silent_windows = [
        item
        for item in audio_plan.get("silent_windows", [])
        if isinstance(item, dict)
        and int(item.get("duration_ms") or 0) >= minimum_silence_ms
    ]
    # `anchor_reference` is lineage metadata, not a semantic proof-topic label.
    # Claim density and non-redundancy are governed by content_bundle_brief and
    # the central voiceover allocator; this review only checks visible-script
    # authenticity hazards.
    blocking = [item for item in issues if item.get("blocking")]
    return {
        "authenticity_policy_version": AUTHENTICITY_POLICY_VERSION,
        "result": "PASS" if not blocking else "FAIL",
        "issues": issues,
        "legacy_action_chain_hits": old_chain_hits,
        "spoken_shot_count": spoken_count,
        "silent_shot_count": max(0, len(storyboard) - spoken_count),
        "silent_window_count": len(silent_windows),
        "max_silent_window_ms": max(
            (int(item.get("duration_ms") or 0) for item in silent_windows),
            default=0,
        ),
    }


def assemble_reality_script(
    *,
    direction: Dict[str, Any],
    visual_plan: Dict[str, Any],
    voiceover_plan: Dict[str, Any],
) -> Dict[str, Any]:
    execution_plan = direction["structure_execution_plan"]
    p2_lite = direction["p2_lite"]
    content_bundle = (
        direction.get("content_bundle_brief")
        if isinstance(direction.get("content_bundle_brief"), dict)
        else {}
    )
    claim_atoms = [
        item for item in content_bundle.get("claim_atoms", []) if isinstance(item, dict)
    ]
    reference = direction["execution_reference"]
    plan_shots = [item for item in execution_plan.get("shot_plan", []) if isinstance(item, dict)]
    visual_shots = [item for item in visual_plan.get("shots", []) if isinstance(item, dict)]
    voice_lines = {
        int(item.get("shot_no")): item
        for item in voiceover_plan.get("lines", [])
        if isinstance(item, dict) and str(item.get("shot_no") or "").isdigit()
    }
    voiceover_covered_shots = {
        covered_shot
        for item in voiceover_plan.get("lines", [])
        if isinstance(item, dict) and str(item.get("shot_no") or "").isdigit()
        for covered_shot in range(
            int(item.get("shot_no")),
            int(item.get("end_shot_no") or item.get("shot_no")) + 1,
        )
    }
    storyboard: List[Dict[str, Any]] = []
    skeleton: List[Dict[str, Any]] = []
    for index, plan_shot in enumerate(plan_shots, 1):
        visual = visual_shots[index - 1] if index <= len(visual_shots) else {}
        voice = voice_lines.get(index, {})
        structural = {
            "structure_beat": plan_shot.get("structure_beat", ""),
            "carrier_mode": plan_shot.get("carrier_mode", ""),
            "continuity_group": plan_shot.get("continuity_group", ""),
            "opening_mechanism": plan_shot.get("opening_mechanism", ""),
        }
        skeleton.append(
            {
                "shot_index": index,
                "time_range": plan_shot.get("time_range", ""),
                "role": plan_shot.get("spoken_task_hint", "proof"),
                "shot_purpose": plan_shot.get("visual_task", ""),
                "proof_path": "REALITY_CONTENT_BUNDLE",
                **structural,
            }
        )
        storyboard.append(
            {
                "shot_no": index,
                "duration": plan_shot.get("time_range", ""),
                "shot_content": _text(visual.get("shot_content")),
                "observable_action": _text(visual.get("observable_action")),
                "shot_purpose": _text(visual.get("editorial_purpose")),
                "subtitle_text_target_language": "",
                "subtitle_text_zh": "",
                "voiceover_text_target_language": _text(voice.get("voiceover_text_target_language")),
                "voiceover_text_zh": _text(voice.get("voiceover_text_zh")),
                "spoken_line_task": _text(voice.get("spoken_line_task")) or "none",
                "person_action": _text(visual.get("observable_action")),
                "performance": {
                    "action_motivation": _text(visual.get("action_motivation")),
                    "gaze_and_reaction": _text(visual.get("gaze_and_reaction")),
                },
                "framing": _text(visual.get("framing")),
                "style_note": _text(visual.get("framing")),
                "anchor_reference": _text(visual.get("anchor_reference")) or _text(p2_lite.get("primary_observation")),
                "reference_spine_orders": visual.get("reference_spine_orders", []),
                "task_type": _text(plan_shot.get("spoken_task_hint")) or "proof",
                "product_visibility": _text(visual.get("product_visibility")),
                "supported_claim_keys": list(visual.get("supported_claim_keys") or []),
                "setting_continuity": _text(visual.get("setting_continuity")),
                "action_motivation": _text(visual.get("action_motivation")),
                "gaze_and_reaction": _text(visual.get("gaze_and_reaction")),
                "audio_hard_constraint": _text(visual.get("audio_hard_constraint")) or "NONE",
                "audio_preference": _text(visual.get("audio_preference")) or "SILENCE_PREFERRED",
                **structural,
            }
        )
    voiceover_start_shots = {
        int(item.get("shot_no"))
        for item in voiceover_plan.get("lines", [])
        if isinstance(item, dict) and str(item.get("shot_no") or "").isdigit()
    }
    storyboard = assign_audio_actual(storyboard, voiceover_covered_shots)
    for shot in storyboard:
        shot_no = int(shot.get("shot_no") or 0)
        if (
            shot_no in voiceover_covered_shots
            and shot_no not in voiceover_start_shots
            and _text(shot.get("audio_actual")) == "VOICEOVER"
        ):
            shot["audio_actual"] = "VOICEOVER_CONTINUATION"
    blueprint = (
        direction.get("creative_blueprint")
        if isinstance(direction.get("creative_blueprint"), dict)
        else {}
    )
    diversity = (
        direction.get("creative_diversity_contract")
        if isinstance(direction.get("creative_diversity_contract"), dict)
        else {}
    )
    persona = blueprint.get("persona") if isinstance(blueprint.get("persona"), dict) else {}
    scene = blueprint.get("scene") if isinstance(blueprint.get("scene"), dict) else {}
    performance_flow = (
        blueprint.get("performance_flow")
        if isinstance(blueprint.get("performance_flow"), dict)
        else {}
    )
    event_design = (
        blueprint.get("event_design")
        if isinstance(blueprint.get("event_design"), dict)
        else {}
    )
    macro_visual_passages = [
        dict(item)
        for item in blueprint.get("macro_visual_passages", [])
        if isinstance(item, dict)
    ]
    retention_hook = (
        blueprint.get("retention_hook")
        if isinstance(blueprint.get("retention_hook"), dict)
        else {}
    )
    carrier_modes = {
        _text(item.get("carrier_mode")).upper() for item in plan_shots if isinstance(item, dict)
    }
    person_on_camera = bool(carrier_modes & {"WEARER_ACTIVE", "MIXED"})
    hands_only = bool(carrier_modes) and carrier_modes <= {"HAND_ONLY"}
    if person_on_camera:
        presentation_mode = "PERSON_ON_CAMERA"
        character_note = "人物按设定出镜并承担动作关系"
    elif hands_only:
        presentation_mode = "HANDS_ONLY"
        character_note = "仅手部进入画面，人物脸部和整体穿搭不出镜"
    else:
        presentation_mode = "STATIC_PRODUCT"
        character_note = "本方向由静物承载，人物不出镜"
    visual_language = (
        blueprint.get("visual_language")
        if isinstance(blueprint.get("visual_language"), dict)
        else {}
    )
    core_atom = next(
        (item for item in claim_atoms if _text(item.get("role")) == "core_result"),
        claim_atoms[0] if claim_atoms else {},
    )
    core_claim_key = _text(core_atom.get("claim_key"))
    passive_facts = [
        _text(item.get("fact_text"))
        for item in claim_atoms
        if _text(item.get("fact_text"))
        and _text(item.get("claim_key")) != core_claim_key
    ]
    visible_anchors = list(
        dict.fromkeys(
            _text(item.get("anchor_reference"))
            for item in visual_shots
            if _text(item.get("anchor_reference"))
            and _text(item.get("anchor_reference")) != "UNAVAILABLE"
        )
    )[:3]
    proof_passage = next(
        (
            item
            for item in macro_visual_passages
            if _text(item.get("narrative_role")) == "EVENT_PROOF"
        ),
        macro_visual_passages[1] if len(macro_visual_passages) > 1 else {},
    )
    proof_action = _text(proof_passage.get("observable_action")).rstrip("。！？!?；;，, ")
    core_result_text = (
        _text(core_atom.get("fact_text")) or "核心商品结果"
    ).rstrip("。！？!?；;，, ")
    render_focus = (
        "开头准备动作保持简短，尽快进入"
        f"{core_result_text}；"
        f"主要观看时间留给{proof_action or '生活事件中的核心过程'}，"
        "若有辅助物，只服务生活动作，不单独展示。"
    )
    video_generation_brief = {
        "schema_version": "reality-video-generation-brief-v3-compact",
        "source": "DETERMINISTIC_COMPACT_PROJECTION",
        "usage": "VIDEO_MODEL_PRIMARY_INPUT",
        "presentation_mode": presentation_mode,
        "carrier_integrity": {
            "expected_carrier": _text(diversity.get("required_carrier")).upper() or "UNAVAILABLE",
            "expected_presentation_mode": _text(diversity.get("required_presentation_mode")) or "UNAVAILABLE",
            "actual_presentation_mode": presentation_mode,
            "result": "PASS" if (
                not _text(diversity.get("required_presentation_mode"))
                or _text(diversity.get("required_presentation_mode")) == presentation_mode
            ) else "FAIL",
        },
        "character": {
            "identity": _text(persona.get("identity")),
            "appearance": _text(persona.get("appearance")),
            "hair_makeup": _text(persona.get("hair_makeup")),
        },
        "scene": {
            "location": _text(scene.get("location")),
            "moment": _text(scene.get("moment")),
            "lighting": _text(scene.get("lighting")),
            "background": _text(scene.get("background")),
        },
        "outfit": _text(persona.get("styling")),
        "opening_observation": _text(event_design.get("start_state"))
        or _text(retention_hook.get("opening_event")),
        "natural_behavior_mainline": _text(event_design.get("natural_event"))
        or _text(performance_flow.get("behavior_motivation")),
        "core_result_moment": _text(event_design.get("core_result_moment")),
        "ending_state": _text(event_design.get("end_state"))
        or _text(performance_flow.get("ending_state")),
        "macro_visual_passages": macro_visual_passages,
        "camera_guidance": list(
            dict.fromkeys(
                _text(item.get("camera_observation"))
                for item in macro_visual_passages
                if _text(item.get("camera_observation"))
            )
        )
        or [
            _text(visual_language.get("camera_behavior")),
            _text(visual_language.get("framing_bias")),
        ],
        "core_result_to_prove": _text(core_atom.get("fact_text")),
        "passive_visible_facts": passive_facts,
        "visible_product_anchors": visible_anchors,
        "render_focus": render_focus,
        "continuous_voiceover": " ".join(
            _text(item.get("voiceover_text_target_language"))
            for item in storyboard
            if _text(item.get("voiceover_text_target_language"))
        ),
        "rendering_boundary": [
            "人物完成自己在该场景原本要做的事情，镜头只负责旁观记录",
            "商品细节保持清楚可见，但不得让人物逐项指向、触摸或核对",
        ],
        "internal_structure_note": (
            f"后台保留{len(storyboard)}个结构槽位用于血缘和时间映射；"
            "视频生成只执行上面的3段生活事件，不把槽位改写成独立表演任务"
        ),
    }
    script = {
        "reality_reference_schema_version": (
            "original-reality-complete-script-v22-event" if blueprint else "original-reality-script-v2"
        ),
        "proof_path": "REALITY_CONTENT_BUNDLE",
        "creative_diversity_contract": diversity,
        "creative_blueprint": blueprint,
        "production_design": {
            "presentation_mode": presentation_mode,
            "character_setting": {
                "on_camera": person_on_camera,
                "note": character_note,
                "identity": _text(persona.get("identity")),
                "age_presence": _text(persona.get("age_presence")),
                "appearance": _text(persona.get("appearance")),
                "hair_makeup": _text(persona.get("hair_makeup")),
                "speaking_personality": _text(persona.get("speaking_personality")),
                "performance_intensity": _text(persona.get("performance_intensity")),
            },
            "scene_setting": {
                "location": _text(scene.get("location")),
                "moment": _text(scene.get("moment")),
                "lighting": _text(scene.get("lighting")),
                "background": _text(scene.get("background")),
                "camera_setup": _text(scene.get("camera_setup")),
            },
            "outfit_setting": {
                "styling": _text(persona.get("styling")),
                "visibility_note": (
                    "完整穿搭应在人物镜头中可识别"
                    if person_on_camera
                    else "本方向不以人物完整穿搭作为画面承载"
                ),
            },
            "performance_setting": {
                "entry_state": _text(performance_flow.get("entry_state")),
                "behavior_motivation": _text(performance_flow.get("behavior_motivation")),
                "reaction_points": list(performance_flow.get("reaction_points") or []),
                "ending_state": _text(performance_flow.get("ending_state")),
            },
            "event_setting": {
                "event_motif": _text(event_design.get("event_motif")),
                "start_state": _text(event_design.get("start_state")),
                "natural_event": _text(event_design.get("natural_event")),
                "core_result_moment": _text(event_design.get("core_result_moment")),
                "end_state": _text(event_design.get("end_state")),
            },
            "retention_setting": {
                "opening_event": _text(retention_hook.get("opening_event")),
                "delayed_answer": _text(retention_hook.get("delayed_answer")),
                "payoff_time": _text(retention_hook.get("payoff_time")),
            },
        },
        "continuous_voiceover": {
            "target_language": " ".join(
                _text(item.get("voiceover_text_target_language"))
                for item in storyboard
                if _text(item.get("voiceover_text_target_language"))
            ),
            "chinese_translation": " ".join(
                _text(item.get("voiceover_text_zh"))
                for item in storyboard
                if _text(item.get("voiceover_text_zh"))
            ),
        },
        "video_generation_brief": video_generation_brief,
        # This is the canonical hand-off to the finished-video voiceover
        # worker.  Preserve the approved words and visual anchors instead of
        # making a downstream worker reconstruct a generic copy from a prompt.
        "voiceover_execution_plan": {
            "schema_version": "voiceover-execution-plan-v1",
            "mode": "REUSE_APPROVED_COPY",
            "source_kind": (
                "central_creative_full_script"
                if _text(voiceover_plan.get("copy_generation_mode")) == "CREATIVE_FULL_SCRIPT"
                else "original_visual_first"
            ),
            "copy_generation_mode": _text(voiceover_plan.get("copy_generation_mode")),
            "candidate_id": _text(voiceover_plan.get("candidate_id")),
            "downstream_rewritten": bool(
                voiceover_plan.get("engine_provenance", {}).get("downstream_rewritten", False)
            ),
            "expression_contract": dict(voiceover_plan.get("expression_contract") or {}),
            "copy_plan": dict(voiceover_plan.get("copy_plan") or {}),
            "target_text": " ".join(
                _text(item.get("voiceover_text_target_language"))
                for item in voiceover_plan.get("lines", [])
                if isinstance(item, dict) and _text(item.get("voiceover_text_target_language"))
            ),
            "chinese_translation": " ".join(
                _text(item.get("voiceover_text_zh"))
                for item in voiceover_plan.get("lines", [])
                if isinstance(item, dict) and _text(item.get("voiceover_text_zh"))
            ),
            "hook_id": _text(voiceover_plan.get("hook_id")),
            "claim_ids": list(voiceover_plan.get("selected_claim_ids") or []),
            "lines": [
                dict(item) for item in voiceover_plan.get("lines", []) if isinstance(item, dict)
            ],
            "silent_windows": [
                dict(item) for item in voiceover_plan.get("silent_windows", []) if isinstance(item, dict)
            ],
        },
        "audio_plan": {
            "authority_order": [
                "audio_hard_constraint",
                "central_voiceover_semantic_segments",
                "audio_preference",
            ],
            "bgm_style": _text(blueprint.get("audio_direction", {}).get("bgm_style")),
            "environment_sound": _text(blueprint.get("audio_direction", {}).get("environment_sound")),
            "silent_windows": list(voiceover_plan.get("silent_windows") or []),
            "minimum_silence_window_ms": int(
                voiceover_plan.get("minimum_silence_window_ms") or 0
            ),
            "total_duration_ms": int(voiceover_plan.get("total_duration_ms") or 15000),
        },
        "script_positioning": {
            "script_title": _text(content_bundle.get("content_mainline"))
            or _text(p2_lite.get("primary_observation")),
            "direction_type": _text(execution_plan.get("macro_family_key")),
            "core_primary_selling_point": _text(p2_lite.get("primary_observation")),
            "supporting_selling_points": [
                _text(item.get("fact_text")) for item in claim_atoms[1:] if _text(item.get("fact_text"))
            ],
        },
        "shot_skeleton": skeleton,
        "storyboard": storyboard,
        "structure_execution_plan": execution_plan,
        "reality_reference_provenance": {
            "direction_assignment_id": direction.get("direction_assignment_id", ""),
            "selection_run_id": direction.get("selection_run_id", ""),
            "cluster_id": direction.get("cluster_id"),
            "cluster_version": direction.get("cluster_version", ""),
            "execution_card_id": reference.get("execution_card_id", ""),
            "execution_card_schema_version": reference.get("execution_card_schema_version", ""),
            "source_profile_id": reference.get("source_profile_id", ""),
            "content_bundle_id": content_bundle.get("content_bundle_id", ""),
            "creative_diversity_contract_id": diversity.get("contract_id", ""),
            "creative_blueprint_id": blueprint.get("creative_blueprint_id", ""),
            "authenticity_policy_version": AUTHENTICITY_POLICY_VERSION,
        },
        "execution_constraints": {
            "single_proof_rule": False,
            "content_mainline": content_bundle.get("content_mainline", ""),
            "claim_atoms": claim_atoms,
            "max_claim_atoms": content_bundle.get("max_claim_atoms", 3),
            "max_themes": content_bundle.get("max_themes", 2),
            "reference_unknown_fields": reference.get("unknown_fields", []),
            "do_not_invent": reference.get("unknown_fields", []),
        },
    }
    script["authenticity_review"] = authenticity_review(script)
    return script
