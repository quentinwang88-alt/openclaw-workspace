"""Read-only, case-first retrieval over the structure-discovery corpus.

This adapter deliberately does not concatenate independently selected cluster
labels.  A real source video is the atomic reference: its structure, scene,
persona-presentation, rhythm and visual-hook assignments travel together with
one pinned reconstruction asset.  The original workflow keeps authority over
product truth, selling arguments, persona identity, outfit and spoken hooks.

RDS is read only during PLAN_ONLY.  The selected contract is frozen into the
batch item, so SCRIPT_ONLY never needs to re-read discovery data.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple
from urllib.parse import unquote, urlparse

from core.product_type_resolution import normalize_product_type


REFERENCE_SCHEMA_VERSION = "retrieval-reference-contract-v4-execution-shape"
REFERENCE_POLICY_VERSION = "execution-case-retrieval-v5-shot-richness"

# Run ids are deliberately not listed here.  sd_dimension_release is the only
# authority for active discovery data; keeping a fallback list would silently
# resurrect prompt_only_full after a cache/process restart.
_REQUIRED_ACTIVE_DIMENSIONS = (
    "structure",
    "scene",
    "rhythm",
    "persona_presentation",
    "visual_hook",
    "script_execution",
    "speech_hook",
)

_PROTOTYPE_TABLES = {
    "structure": "sd_cluster_prototype",
    "scene": "sd_scene_prototype",
    "persona_presentation": "sd_persona_presentation_prototype",
    "rhythm": "sd_rhythm_prototype",
    "visual_hook": "sd_visual_hook_prototype",
}

_JSON_FIELDS = {
    "representative_cases",
    "representative_case_ids",
    "canonical_joint_signature",
    "rhythm_contract",
    "measured_signal",
    "distinctive_anchors",
}


def _language_key(value: Any) -> str:
    text = _text(value).casefold()
    aliases = {
        "th": "th", "thai": "th", "泰语": "th", "泰文": "th",
        "vi": "vi", "vietnamese": "vi", "越南语": "vi", "越南文": "vi",
        "ms": "ms", "malay": "ms", "马来语": "ms", "马来文": "ms",
        "es": "es", "spanish": "es", "西班牙语": "es",
    }
    return aliases.get(text, text)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _id_text(value: Any) -> str:
    """Stringify numeric identifiers without erasing the valid cluster 0."""

    return "" if value is None else str(value).strip()


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _json(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str) or not value.strip():
        return default
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return default
    return parsed if isinstance(parsed, type(default)) else default


def _stable_hash(value: Any, length: int = 24) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


def _active_dimension_releases(cursor: Any) -> Dict[str, Dict[str, Any]]:
    cursor.execute(
        """
        SELECT release_id, dimension_type, run_id, schema_version,
               extractor_version, feature_schema_version, prototype_version,
               schema_status, published_at, metadata_json
        FROM sd_dimension_release
        WHERE is_active=1
        ORDER BY published_at DESC, updated_at DESC
        """
    )
    releases: Dict[str, Dict[str, Any]] = {}
    for raw in cursor.fetchall():
        row = dict(raw)
        dimension = _text(row.get("dimension_type")).lower()
        if dimension and dimension not in releases:
            row["metadata_json"] = _json(row.get("metadata_json"), {})
            releases[dimension] = row
    return releases


def _execution_part(value: Any, part: str) -> Dict[str, Any]:
    row = dict(value) if isinstance(value, Mapping) else {}
    status = _text(row.get("status")).upper() or "UNAVAILABLE"
    if status != "AVAILABLE":
        return {"status": "UNAVAILABLE", "part": part}
    shots = _compact_storyboard(row.get("shots"), limit=6)
    # execution_card_json already assigns the functional segment.  Do not
    # infer a missing segment from another shot merely to make a full card.
    return {
        "status": "AVAILABLE",
        "part": part,
        "shot_indexes": [
            _int(item) for item in row.get("shot_indexes") or []
            if _int(item) > 0
        ],
        "source_logic": _compact_text(
            row.get("source_logic")
            or row.get("action_logic")
            or row.get("camera_logic"),
            360,
        ),
        "shots": shots,
    }


def _normalize_execution_card(feature: Mapping[str, Any]) -> Dict[str, Any]:
    raw = _json(feature.get("execution_card_json"), {})
    parts = {
        name: _execution_part(raw.get(name), name)
        for name in ("opening", "proof", "use_process", "ending")
    }
    available = [
        name for name, value in parts.items()
        if value.get("status") == "AVAILABLE"
    ]
    evidence_tier = _text(feature.get("evidence_tier")) or (
        "VIDEO_INDEPENDENT"
        if bool(_int(feature.get("raw_video_verified")))
        else "VIDEO_DERIVED_SCRIPT"
    )
    video_id = _text(feature.get("video_id"))
    card = {
        "schema_version": "script-execution-card-v2",
        "execution_card_id": "EXEC_" + _stable_hash({
            "video_id": video_id,
            "source_asset_id": feature.get("source_asset_id"),
            "source_asset_version": feature.get("source_asset_version"),
            "storyboard_fingerprint": feature.get("storyboard_fingerprint"),
            "parts": parts,
        }, 20).upper(),
        "video_id": video_id,
        "content_carrier": _text(
            raw.get("content_carrier") or feature.get("content_carrier")
        ),
        "physical_action_type": _text(
            raw.get("physical_action_type")
            or feature.get("physical_action_type")
        ),
        "duration_sec": _float(raw.get("duration_sec"), _float(feature.get("duration_sec"))),
        "shot_count": _int(raw.get("shot_count"), _int(feature.get("shot_count"))),
        "coarse_beat_sequence": _json(
            raw.get("coarse_beat_sequence")
            or feature.get("coarse_beat_sequence"), []
        ),
        "scene_logic": _compact_text(raw.get("scene_logic"), 420),
        "action_logic": _compact_text(raw.get("action_logic"), 420),
        "camera_logic": _compact_text(raw.get("camera_logic"), 420),
        "rhythm_logic": _compact_text(raw.get("rhythm_logic"), 360),
        "expression_logic": _compact_text(raw.get("expression_logic"), 320),
        "parts": parts,
        "available_parts": available,
        "_meta": {
            "evidence_tier": evidence_tier,
            "semantic_source": _text(feature.get("semantic_source")),
            "raw_video_verified": bool(_int(feature.get("raw_video_verified"))),
            "source_asset_id": _text(feature.get("source_asset_id")),
            "source_asset_version": _int(feature.get("source_asset_version")),
            "storyboard_fingerprint": _text(feature.get("storyboard_fingerprint")),
        },
    }
    return card


def _card_completeness(card: Mapping[str, Any]) -> Tuple[int, int]:
    available = len(card.get("available_parts") or [])
    shot_count = _int(card.get("shot_count"))
    return available, min(shot_count, 8)


def _requested_action_tokens(
    creative_contract: Mapping[str, Any],
    content_bundle: Mapping[str, Any],
    category_execution_extension: Optional[Mapping[str, Any]] = None,
) -> List[str]:
    extension = category_execution_extension or {}
    candidates: List[Any] = [
        creative_contract.get("action_design"),
        creative_contract.get("action_grammar"),
        creative_contract.get("opening_action"),
        extension.get("action_design"),
        extension.get("selected_action_design"),
        extension.get("primary_demonstration_mode"),
        (extension.get("profile") or {}).get("primary_demonstration_mode")
        if isinstance(extension.get("profile"), Mapping) else "",
        (content_bundle.get("selling_argument") or {}).get("primary_demonstration_mode")
        if isinstance(content_bundle.get("selling_argument"), Mapping) else "",
    ]
    tokens: List[str] = []
    semantic_aliases = {
        "穿": "WEAR",
        "试穿": "TRY_ON",
        "佩戴": "WEAR",
        "戴好": "RESULT_SHOW",
        "调整": "ADJUST",
        "整理": "ADJUST",
        "系": "ADJUST",
        "拿": "HOLD",
        "手持": "HOLD",
        "展示": "DETAIL_SHOW",
        "细节": "DETAIL_SHOW",
        "走": "WALK",
        "行走": "WALK",
        "转身": "TURN",
        "静置": "STATIC",
        "平铺": "STATIC",
        "悬挂": "STATIC",
    }
    for value in candidates:
        if isinstance(value, Mapping):
            values = list(value.values())
        elif isinstance(value, list):
            values = value
        else:
            values = [value]
        for item in values:
            text = _text(item).upper()
            for token in re.split(r"[^A-Z0-9_]+", text):
                if token and token not in tokens:
                    tokens.append(token)
            for phrase, token in semantic_aliases.items():
                if phrase in text and token not in tokens:
                    tokens.append(token)
    return tokens


def _action_compatibility(action: Any, requested_tokens: Sequence[str]) -> Tuple[int, str]:
    observed = _text(action).upper()
    if not requested_tokens:
        return 1, "NO_ACTION_PREFERENCE"
    aliases = {
        "WEAR": {"WEAR", "TRY_ON", "PUT_ON", "NECK_WORN", "HEAD_WORN", "HAIR_TIE"},
        "ADJUST": {"ADJUST", "SIMPLE_ADJUST", "RESULT_SHOW", "DETAIL_SHOW"},
        "HOLD": {"HOLD", "HANDHELD_PRODUCT", "DETAIL_SHOW"},
        "STATIC": {"STATIC", "DETAIL_SHOW", "RESULT_SHOW"},
    }
    requested = set(requested_tokens)
    if observed and observed in requested:
        return 3, "EXACT_ACTION"
    if observed and any(observed in values and requested.intersection(values) for values in aliases.values()):
        return 2, "COMPATIBLE_ACTION"
    if not observed:
        return 1, "ACTION_UNKNOWN"
    return 0, "ACTION_MISMATCH"


def _proof_execution_alignment(
    card: Mapping[str, Any], intent: Mapping[str, Any]
) -> Dict[str, Any]:
    """Match selling-point proof needs to observed execution-card functions."""

    available = {
        _text(value).lower() for value in (card.get("available_parts") or [])
        if _text(value)
    }
    preferred = [
        _text(value).lower() for value in (intent.get("preferred_parts") or [])
        if _text(value)
    ]
    required = [
        _text(value).lower() for value in (intent.get("required_any_parts") or [])
        if _text(value)
    ]
    preferred_matches = [value for value in preferred if value in available]
    required_match = not required or any(value in available for value in required)
    return {
        "proof_subject": _text(intent.get("proof_subject")) or "GENERAL_EXPRESSION",
        "available_parts": sorted(available),
        "preferred_parts": preferred,
        "preferred_part_matches": preferred_matches,
        "required_any_parts": required,
        "required_part_match": required_match,
        "hard_required_part_match": bool(intent.get("hard_required_part_match")),
    }


def _speech_pool_from_rows(
    prototypes: Sequence[Mapping[str, Any]],
    profiles: Sequence[Mapping[str, Any]],
    *, target_language: str, top_category: str, product_type: str,
) -> List[Dict[str, Any]]:
    language = _language_key(target_language)
    requested = normalize_product_type(product_type, top_category)
    profiles_by_cluster: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in profiles:
        profiles_by_cluster[_id_text(row.get("cluster_id"))].append(row)
    result: List[Tuple[Tuple[int, int, int, str], Dict[str, Any]]] = []
    for proto in prototypes:
        cluster_id = _id_text(proto.get("cluster_id"))
        members = profiles_by_cluster.get(cluster_id, [])
        same_language = [
            row for row in members if _language_key(row.get("language")) == language
        ]
        exact_category = []
        for row in same_language:
            canonical_observed = _text(row.get("canonical_cat2")).casefold()
            observed = normalize_product_type(
                _text(row.get("cat2") or row.get("canonical_cat2")),
                _text(row.get("cat1") or row.get("canonical_cat1")),
            )
            if (
                canonical_observed == _text(requested.canonical_type).casefold()
                or (
                    requested.recognized_by_registry
                    and observed.recognized_by_registry
                    and requested.canonical_type == observed.canonical_type
                )
            ):
                exact_category.append(row)
        chosen = exact_category or same_language
        examples = []
        for row in chosen[:2]:
            text = _compact_text(row.get("transcript_normalized"), 700)
            if text:
                examples.append({
                    "video_id": _text(row.get("video_id")),
                    "language": _language_key(row.get("language")),
                    "rhetorical_example": text,
                })
        if not examples:
            continue
        item = {
            "cluster_id": cluster_id,
            "prototype_name": _text(proto.get("prototype_name")),
            "opening_move": _text(proto.get("opening_move")),
            "relation_mode": _text(proto.get("relation_mode")),
            "argument_order": _text(proto.get("argument_order")),
            "ending_pattern": _text(proto.get("ending_pattern")),
            "member_count": _int(proto.get("member_count")),
            "independent_creator_count": _int(proto.get("independent_creator_count")),
            "target_language_examples": examples,
            "authority": "RHETORIC_ONLY",
            "forbidden_inheritance": [
                "source_product_fact", "source_claim", "brand", "price", "cta_wording"
            ],
        }
        rank = (
            1 if exact_category else 0,
            len(same_language),
            _int(proto.get("independent_creator_count")),
            cluster_id,
        )
        result.append((rank, item))
    result.sort(key=lambda item: (-item[0][0], -item[0][1], -item[0][2], item[0][3]))
    return [item for _, item in result[:4]]


def multidim_reference_enabled() -> bool:
    return _text(
        os.environ.get("ORIGINAL_SCRIPT_MULTIDIM_REFERENCE_ENABLED", "1")
    ).lower() not in {"0", "false", "no", "off"}


def _connect(database_url: str):
    try:
        import pymysql
    except ImportError as exc:  # pragma: no cover - installation boundary
        raise RuntimeError("多维真实案例检索需要 PyMySQL") from exc
    parsed = urlparse(database_url)
    return pymysql.connect(
        host=parsed.hostname,
        port=parsed.port or 3306,
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        database=parsed.path.lstrip("/"),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        connect_timeout=10,
        read_timeout=45,
    )


def _direction_provenance(direction: Mapping[str, Any]) -> Dict[str, Any]:
    contract = direction.get("structure_contract")
    contract = contract if isinstance(contract, Mapping) else {}
    provenance = contract.get("provenance")
    provenance = provenance if isinstance(provenance, Mapping) else {}
    return {
        "direction_assignment_id": _text(
            direction.get("direction_assignment_id")
            or provenance.get("direction_assignment_id")
        ),
        "source_run_id": _text(
            direction.get("source_run_id") or provenance.get("source_run_id")
        ),
        "cluster_id": _id_text(
            direction.get("cluster_id")
            if direction.get("cluster_id") is not None
            else provenance.get("cluster_id")
        ),
        "cluster_version": _text(
            direction.get("cluster_version") or provenance.get("cluster_version")
        ),
    }


def _direction_macro_family(direction: Mapping[str, Any]) -> str:
    """Return the routed macro family without inventing a new structure."""

    contract = direction.get("structure_contract")
    contract = contract if isinstance(contract, Mapping) else {}
    hard = contract.get("hard_constraints")
    hard = hard if isinstance(hard, Mapping) else {}
    identity = contract.get("direction_identity")
    identity = identity if isinstance(identity, Mapping) else {}
    family = _text(
        identity.get("macro_family_key")
        or hard.get("macro_family_key")
        or direction.get("macro_family_key")
    )
    if family:
        return ">".join(part.strip().upper() for part in family.split(">") if part.strip())
    sequence = hard.get("beat_sequence") or contract.get("beat_sequence") or []
    if isinstance(sequence, list):
        return ">".join(
            _text(part).upper() for part in sequence if _text(part)
        )
    return ""


def _country_key(value: Any) -> str:
    text = _text(value).casefold()
    aliases = {
        "th": "TH", "thai": "TH", "thailand": "TH", "泰国": "TH",
        "vn": "VN", "vietnam": "VN", "越南": "VN",
        "my": "MY", "malaysia": "MY", "马来西亚": "MY",
        "mx": "MX", "mexico": "MX", "墨西哥": "MX",
    }
    return aliases.get(text, text.upper())


def _product_match_score(
    case: Mapping[str, Any], *, top_category: str, product_type: str
) -> Tuple[float, str]:
    requested = normalize_product_type(product_type, top_category)
    observed = normalize_product_type(
        _text(case.get("cat2")), _text(case.get("cat1"))
    )
    if requested.recognized_by_registry and observed.recognized_by_registry:
        if requested.canonical_type == observed.canonical_type:
            return 36.0, "EXACT_PRODUCT_TYPE"
        if requested.canonical_family == observed.canonical_family:
            return 18.0, "SAME_PRODUCT_FAMILY"
        return -18.0, "KNOWN_DIFFERENT_FAMILY"
    requested_category = _text(top_category).casefold()
    observed_category = _text(case.get("cat1")).casefold()
    if requested_category and observed_category and (
        requested_category in observed_category
        or observed_category in requested_category
    ):
        return 10.0, "SAME_TOP_CATEGORY"
    return 0.0, "CATEGORY_UNKNOWN"


def _carrier_compatible(presentation: str, carrier: str) -> bool:
    allowed = {
        "PERSON_ON_CAMERA": {
            "WEARER_ACTIVE", "PERSON_ON_CAMERA", "MIXED", "PERSON_LED"
        },
        "STATIC_PRODUCT": {"STATIC_PRODUCT", "HAND_ONLY", "HANDS_ONLY", "MIXED"},
        "HANDS_ONLY": {"HAND_ONLY", "HANDS_ONLY", "STATIC_PRODUCT", "MIXED"},
    }
    normalized = _text(presentation).upper()
    observed = _text(carrier).upper()
    return not observed or observed in allowed.get(normalized, {normalized})


def _compact_text(value: Any, limit: int = 260) -> str:
    text = " ".join(_text(value).split())
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def _sanitize_scene_anchor(value: Any) -> str:
    """Keep observed aesthetics while removing brittle pseudo-precision."""

    text = _compact_text(value, 180)
    text = re.sub(
        r"(?<!\d)\d{3,5}(?:\s*[-~至到]\s*\d{3,5})?\s*K(?![A-Za-z])",
        "",
        text,
        flags=re.I,
    )
    text = re.sub(
        r"(?<![A-Za-z0-9])\d+(?:\.\d+)?\s*(?:米|厘米|公分|cm|m)(?![A-Za-z])",
        "自然距离",
        text,
        flags=re.I,
    )
    return " ".join(text.split()).strip("，；、 ")


def _compact_storyboard(value: Any, *, limit: int = 6) -> List[Dict[str, str]]:
    storyboard = _json(value, [])
    result: List[Dict[str, str]] = []
    for index, shot in enumerate(storyboard[:limit], start=1):
        if not isinstance(shot, Mapping):
            continue
        compact = {
            "shot_index": _int(shot.get("shot_index"), index),
            "title": _compact_text(shot.get("title"), 80),
            "duration": _compact_text(
                shot.get("duration") or shot.get("duration_sec"), 48
            ),
            "visual_content": _compact_text(
                shot.get("visual_content") or shot.get("visual"), 320
            ),
            "camera_and_editing": _compact_text(
                shot.get("camera_and_editing")
                or shot.get("camera")
                or shot.get("shot_type"),
                240,
            ),
            "core_function": _compact_text(
                shot.get("core_function") or shot.get("purpose"), 180
            ),
        }
        if any(value for key, value in compact.items() if key != "shot_index"):
            result.append(compact)
    return result


def _carrier_family(value: Any) -> str:
    text = _text(value).upper()
    if text in {"WEARER_ACTIVE", "PERSON_ON_CAMERA", "PERSON_LED"}:
        return "PERSON_ON_CAMERA"
    if text in {"HAND_ONLY", "HANDS_ONLY"}:
        return "HANDS_ONLY"
    if text == "STATIC_PRODUCT":
        return "STATIC_PRODUCT"
    if text == "MIXED":
        return "MIXED"
    return "UNKNOWN"


def _infer_storyboard_carrier(storyboard: Sequence[Mapping[str, Any]]) -> str:
    """Infer only clear carrier evidence from the observed storyboard text.

    This is deliberately small: it catches obvious metadata/storyboard
    contradictions without attempting a new semantic classifier.
    """

    text = " ".join(
        _text(shot.get(key))
        for shot in storyboard
        if isinstance(shot, Mapping)
        for key in ("title", "visual_content", "core_function")
    )
    person_terms = (
        "人物", "模特", "女生", "女性", "创作者", "试穿", "穿上", "上身",
        "全身", "半身", "侧身", "转身", "走动", "佩戴完成",
    )
    hand_terms = (
        "手部", "双手", "单手", "手持", "手拿", "拿起", "指尖", "掌心",
    )
    static_terms = (
        "静物", "平铺", "悬挂", "挂在", "展示卡", "桌面陈列", "商品卡",
        "无人出镜", "无人物", "仅商品",
    )
    person_score = sum(1 for term in person_terms if term in text)
    hand_score = sum(1 for term in hand_terms if term in text)
    static_score = sum(1 for term in static_terms if term in text)
    if person_score >= 2 or any(term in text for term in ("试穿", "穿上", "人物走动")):
        return "PERSON_ON_CAMERA"
    if hand_score >= 2 and person_score == 0:
        return "HANDS_ONLY"
    if static_score >= 2 and person_score == 0:
        return "STATIC_PRODUCT"
    return "UNKNOWN"


def _source_carrier_conflict(metadata_carrier: Any, storyboard_carrier: Any) -> bool:
    metadata = _carrier_family(metadata_carrier)
    observed = _carrier_family(storyboard_carrier)
    if "UNKNOWN" in {metadata, observed} or "MIXED" in {metadata, observed}:
        return False
    if {metadata, observed} <= {"STATIC_PRODUCT", "HANDS_ONLY"}:
        return False
    return metadata != observed


def _case_eligibility(
    *, category_status: str, requested_presentation: str,
    metadata_carrier: str, storyboard_carrier: str,
) -> Tuple[str, List[str]]:
    """Return ELIGIBLE / SUPPORT_ONLY / QUARANTINED without a score matrix."""

    reasons: List[str] = []
    if category_status == "KNOWN_DIFFERENT_FAMILY":
        reasons.append("KNOWN_DIFFERENT_PRODUCT_FAMILY")
    if _source_carrier_conflict(metadata_carrier, storyboard_carrier):
        reasons.append("SOURCE_METADATA_STORYBOARD_CARRIER_CONFLICT")
    if storyboard_carrier != "UNKNOWN" and not _carrier_compatible(
        requested_presentation, storyboard_carrier
    ):
        reasons.append("STORYBOARD_CARRIER_INCOMPATIBLE_WITH_DIRECTION")
    if reasons:
        return "QUARANTINED", reasons
    if category_status in {"EXACT_PRODUCT_TYPE", "SAME_PRODUCT_FAMILY"}:
        return "ELIGIBLE", []
    return "SUPPORT_ONLY", ["CATEGORY_NOT_STRONG_ENOUGH_FOR_PRIMARY"]


def _spine_part(shot: Optional[Mapping[str, Any]], part: str) -> Dict[str, Any]:
    if not isinstance(shot, Mapping):
        return {"status": "UNAVAILABLE", "part": part}
    return {
        "status": "AVAILABLE",
        "part": part,
        "source_shot_index": _int(shot.get("shot_index")),
        "shot_function": _compact_text(
            shot.get("core_function") or shot.get("title"), 140
        ),
        "visual_action": _compact_text(shot.get("visual_content"), 240),
        "framing_and_transition": _compact_text(
            shot.get("camera_and_editing"), 180
        ),
    }


def _reference_execution_spine(
    storyboard: Sequence[Mapping[str, Any]], *, video_id: str
) -> Dict[str, Any]:
    shots = [dict(value) for value in storyboard if isinstance(value, Mapping)]
    opening = shots[0] if shots else None
    proof = shots[len(shots) // 2] if len(shots) >= 2 else None
    ending = shots[-1] if len(shots) >= 3 else None
    parts = {
        "opening": _spine_part(opening, "opening"),
        "proof": _spine_part(proof, "proof"),
        "ending": _spine_part(ending, "ending"),
    }
    available = [
        key for key, value in parts.items() if value.get("status") == "AVAILABLE"
    ]
    material = {"video_id": video_id, "parts": parts}
    return {
        "schema_version": "reference-execution-spine-v1",
        "reference_spine_id": "RSP_" + _stable_hash(material, 20).upper(),
        "parts": parts,
        "available_parts": available,
        "soft_adoption_target": "优先自然借鉴其中至少两段；不兼容时可少用或不用",
    }


def _representative_video_ids(prototype: Mapping[str, Any]) -> List[str]:
    raw = (
        prototype.get("representative_cases")
        or prototype.get("representative_case_ids")
        or []
    )
    values = _json(raw, [])
    result: List[str] = []
    for value in values:
        video_id = _text(value.get("video_id") if isinstance(value, Mapping) else value)
        if video_id and video_id not in result:
            result.append(video_id)
    return result


def _prototype_projection(
    dimension: str, prototype: Optional[Mapping[str, Any]]
) -> Dict[str, Any]:
    row = dict(prototype or {})
    if not row:
        return {"status": "UNAVAILABLE", "dimension_type": dimension}
    base = {
        "status": "AVAILABLE",
        "dimension_type": dimension,
        "run_id": _text(row.get("run_id")),
        "cluster_id": _id_text(row.get("cluster_id")),
        "schema_status": _text(row.get("schema_status")),
        "member_count": _int(row.get("member_count")),
        "cluster_status": _text(row.get("cluster_status")),
        "measured_signal": _json(row.get("measured_signal"), {}),
    }
    if dimension == "scene":
        base.update({
            "name": _text(row.get("scene_name")),
            "summary": _compact_text(row.get("scene_description"), 300),
            "scene_executability": _text(row.get("scene_executability")),
            "distinctive_anchors": _json(row.get("distinctive_anchors"), {}),
        })
    elif dimension == "persona_presentation":
        base.update({
            "name": _text(row.get("prototype_name")),
            "summary": _compact_text(row.get("prototype_summary"), 300),
            "canonical_joint_signature": _json(
                row.get("canonical_joint_signature"), {}
            ),
        })
    elif dimension == "rhythm":
        base.update({
            "name": _text(row.get("rhythm_name")),
            "summary": _compact_text(row.get("rhythm_description"), 300),
            "production_family_id": _text(row.get("production_family_id")),
            "production_family_name": _text(row.get("production_family_name")),
            "production_brief": _compact_text(row.get("production_brief"), 260),
            "is_generic_rhythm": bool(_int(row.get("is_generic_rhythm"))),
            "rhythm_contract": _json(row.get("rhythm_contract"), {}),
        })
    elif dimension == "visual_hook":
        signature = _json(row.get("canonical_joint_signature"), {})
        base.update({
            "name": _text(row.get("prototype_name")),
            "summary": _compact_text(row.get("prototype_summary"), 300),
            "canonical_joint_signature": signature,
            "entry_subject": _text(
                signature.get("entry_subject")
                or (signature.get("signature") or {}).get("entry_subject")
            ),
        })
    else:
        base.update({
            "name": _text(row.get("macro_structure_name")),
            "summary": _compact_text(row.get("structure_description"), 300),
        })
    return base


def _pair_key(
    left_type: str, left_cluster: Any, right_type: str, right_cluster: Any
) -> Tuple[str, str, str, str]:
    left = (_text(left_type), _id_text(left_cluster))
    right = (_text(right_type), _id_text(right_cluster))
    return (*left, *right) if left <= right else (*right, *left)


def _cooccurrence_bonus(
    assignments: Mapping[str, Mapping[str, Any]],
    cooccurrence: Mapping[Tuple[str, str, str, str], Mapping[str, Any]],
) -> Tuple[float, List[Dict[str, Any]]]:
    evidence: List[Dict[str, Any]] = []
    dimensions = [
        value for value in ("structure", "scene", "persona_presentation", "rhythm", "visual_hook")
        if isinstance(assignments.get(value), Mapping)
    ]
    score = 0.0
    for index, left_type in enumerate(dimensions):
        for right_type in dimensions[index + 1 :]:
            left_cluster = assignments[left_type].get("cluster_id")
            right_cluster = assignments[right_type].get("cluster_id")
            row = cooccurrence.get(
                _pair_key(left_type, left_cluster, right_type, right_cluster)
            )
            if not isinstance(row, Mapping):
                continue
            count = _int(row.get("video_count"))
            lift = max(0.0, _float(row.get("lift")))
            if count < 3:
                continue
            contribution = min(2.0, math.log1p(count) / 3.0) * min(1.5, lift)
            score += contribution
            evidence.append({
                "pair_kind": _text(row.get("pair_kind")),
                "video_count": count,
                "independent_creator_count": _int(
                    row.get("independent_creator_count")
                ),
                "conditional_share": round(_float(row.get("conditional_share")), 4),
                "lift": round(lift, 4),
            })
    evidence.sort(key=lambda item: (-item["video_count"], item["pair_kind"]))
    return min(score, 10.0), evidence[:6]


def _base_candidate_score(
    candidate: Mapping[str, Any], *, top_category: str, product_type: str,
    target_country: str, representative_video_ids: Iterable[str]
) -> Tuple[float, str]:
    case = candidate.get("case") if isinstance(candidate.get("case"), Mapping) else {}
    category_score, category_status = _product_match_score(
        case, top_category=top_category, product_type=product_type
    )
    score = category_score
    if _country_key(case.get("country")) == _country_key(target_country):
        score += 5.0
    if bool(candidate.get("structure_measured_available")):
        score += 4.0
    if _text(candidate.get("video_id")) in set(representative_video_ids):
        score += 8.0
    score += max(0.0, min(1.0, _float(case.get("perf_score")))) * 5.0
    score += max(0.0, min(1.0, _float(case.get("extraction_confidence")))) * 2.0
    return score, category_status


def _unavailable_context(
    direction: Mapping[str, Any], reason: str, *, runs: Optional[Mapping[str, str]] = None
) -> Dict[str, Any]:
    provenance = _direction_provenance(direction)
    return {
        "schema_version": REFERENCE_SCHEMA_VERSION,
        "status": "UNAVAILABLE",
        "reason": reason,
        "policy_version": REFERENCE_POLICY_VERSION,
        "direction_assignment_id": provenance["direction_assignment_id"],
        "structure_run_id": provenance["source_run_id"],
        "structure_cluster_id": provenance["cluster_id"],
        "requested_structure_family": _direction_macro_family(direction),
        "active_runs": dict(runs or {}),
        "candidates": [],
        "prototypes": {},
        "cooccurrence": {},
        "data_snapshot_hash": _stable_hash({"reason": reason, **provenance}),
    }


def _latest_assets(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        video_id = _text(row.get("source_video_id"))
        if not video_id or not _compact_storyboard(row.get("storyboard_json")):
            continue
        current = result.get(video_id)
        rank = (
            _int(row.get("is_frozen")),
            _int(row.get("asset_version"), _int(row.get("version"))),
            _int(row.get("version")),
            _text(row.get("updated_at")),
        )
        current_rank = (
            _int(current.get("is_frozen")),
            _int(current.get("asset_version"), _int(current.get("version"))),
            _int(current.get("version")),
            _text(current.get("updated_at")),
        ) if current else (-1, -1, -1, "")
        if rank > current_rank:
            result[video_id] = row
    return result


def _load_v3_reference_contexts(
    cursor: Any,
    directions: Sequence[Mapping[str, Any]],
    *, target_country: str, target_language: str,
    top_category: str, product_type: str,
) -> Dict[str, Dict[str, Any]]:
    releases = _active_dimension_releases(cursor)
    missing = [name for name in _REQUIRED_ACTIVE_DIMENSIONS if name not in releases]
    provenances = [_direction_provenance(direction) for direction in directions]
    if missing:
        return {
            value["direction_assignment_id"]: _unavailable_context(
                direction, "ACTIVE_RELEASE_MISSING:" + ",".join(missing),
                runs={key: _text(row.get("run_id")) for key, row in releases.items()},
            )
            for direction, value in zip(directions, provenances)
            if value["direction_assignment_id"]
        }

    active_runs = {
        dimension: _text(row.get("run_id"))
        for dimension, row in releases.items()
    }
    schema_versions = {
        dimension: _text(row.get("schema_version"))
        for dimension, row in releases.items()
    }
    requested = normalize_product_type(product_type, top_category)
    # Keep the SQL gate broad enough for registered same-family fallbacks;
    # final exact/same-family eligibility is computed deterministically below.
    cursor.execute(
        """
        SELECT f.*,
               c.case_id, c.evidence_tier AS case_evidence_tier,
               c.semantic_source AS case_semantic_source,
               c.raw_video_verified AS case_raw_video_verified,
               c.extraction_confidence, c.content_carrier AS case_content_carrier,
               c.physical_action_type AS case_physical_action_type,
               c.creator_id AS case_creator_id, c.perf_score AS case_perf_score,
               c.country_code AS case_country_code,
               t.template_group_id
        FROM sd_video_script_feature_v2 f
        LEFT JOIN sd_representative_case c
          ON c.video_id=f.video_id AND c.case_schema_version='case-v2'
        LEFT JOIN sd_video_template_group t ON t.video_id=f.video_id
        WHERE f.cat2=%s OR f.cat1=%s OR UPPER(f.product_family)=%s
        ORDER BY f.video_id
        """,
        (
            _text(product_type),
            _text(top_category),
            _text(requested.canonical_family).upper(),
        ),
    )
    feature_rows = [dict(row) for row in cursor.fetchall()]
    if not feature_rows:
        cursor.execute(
            """
            SELECT f.*,
                   c.case_id, c.evidence_tier AS case_evidence_tier,
                   c.semantic_source AS case_semantic_source,
                   c.raw_video_verified AS case_raw_video_verified,
                   c.extraction_confidence, c.content_carrier AS case_content_carrier,
                   c.physical_action_type AS case_physical_action_type,
                   c.creator_id AS case_creator_id, c.perf_score AS case_perf_score,
                   c.country_code AS case_country_code,
                   t.template_group_id
            FROM sd_video_script_feature_v2 f
            LEFT JOIN sd_representative_case c
              ON c.video_id=f.video_id AND c.case_schema_version='case-v2'
            LEFT JOIN sd_video_template_group t ON t.video_id=f.video_id
            WHERE f.cat1=%s
            ORDER BY f.video_id
            """,
            (_text(top_category),),
        )
        feature_rows = [dict(row) for row in cursor.fetchall()]

    video_ids = sorted({_text(row.get("video_id")) for row in feature_rows if _text(row.get("video_id"))})
    assignments_by_video: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
    for offset in range(0, len(video_ids), 500):
        chunk = video_ids[offset : offset + 500]
        if not chunk:
            continue
        placeholders = ",".join(["%s"] * len(chunk))
        cursor.execute(
            f"""
            SELECT video_id, dimension_type, run_id, cluster_id,
                   schema_status, measured_available
            FROM sd_video_feature_assignment
            WHERE video_id IN ({placeholders})
              AND dimension_type IN ('structure','scene','rhythm','persona_presentation','visual_hook')
            """,
            chunk,
        )
        for row in cursor.fetchall():
            dimension = _text(row.get("dimension_type"))
            if _text(row.get("run_id")) != active_runs.get(dimension):
                continue
            assignments_by_video[_text(row.get("video_id"))][dimension] = dict(row)

    speech_run = active_runs["speech_hook"]
    cursor.execute(
        "SELECT * FROM sd_speech_hook_prototype WHERE run_id=%s",
        (speech_run,),
    )
    speech_prototypes = [dict(row) for row in cursor.fetchall()]
    cursor.execute(
        """
        SELECT p.*, c.cluster_id
        FROM sd_speech_hook_profile p
        JOIN sd_speech_hook_cluster c
          ON c.run_id=p.run_id AND c.video_id=p.video_id
        WHERE p.run_id=%s AND p.hook_usable=1 AND COALESCE(c.is_noise,0)=0
        ORDER BY p.video_id
        """,
        (speech_run,),
    )
    speech_profiles = [dict(row) for row in cursor.fetchall()]
    speech_pool = _speech_pool_from_rows(
        speech_prototypes,
        speech_profiles,
        target_language=target_language,
        top_category=top_category,
        product_type=product_type,
    )

    candidates: List[Dict[str, Any]] = []
    for row in feature_rows:
        evidence_tier = _text(row.get("case_evidence_tier")) or (
            "VIDEO_INDEPENDENT"
            if bool(_int(row.get("raw_video_verified")))
            else "VIDEO_DERIVED_SCRIPT"
        )
        semantic_source = _text(
            row.get("case_semantic_source") or row.get("semantic_source")
        )
        raw_verified = bool(_int(
            row.get("case_raw_video_verified")
            if row.get("case_raw_video_verified") is not None
            else row.get("raw_video_verified")
        ))
        feature = dict(row)
        feature.update({
            "evidence_tier": evidence_tier,
            "semantic_source": semantic_source,
            "raw_video_verified": raw_verified,
        })
        execution_card = _normalize_execution_card(feature)
        if not execution_card.get("available_parts"):
            continue
        case = {
            "cat1": _text(row.get("cat1")),
            "cat2": _text(row.get("cat2")),
            "canonical_cat1": _text(row.get("canonical_cat1")),
            "canonical_cat2": _text(row.get("canonical_cat2")),
            "product_family": _text(row.get("product_family")),
            "country": _text(row.get("case_country_code") or row.get("country_code")),
            "country_code": _text(row.get("case_country_code") or row.get("country_code")),
            "content_carrier": _text(row.get("case_content_carrier") or row.get("content_carrier")),
            "physical_action_type": _text(row.get("case_physical_action_type") or row.get("physical_action_type")),
            "creator_id": _text(row.get("case_creator_id") or row.get("creator_id")),
            "template_group_id": _text(row.get("template_group_id")),
            "perf_score": _float(row.get("case_perf_score"), _float(row.get("perf_score"))),
            "extraction_confidence": _float(row.get("extraction_confidence")),
            "evidence_tier": evidence_tier,
            "semantic_source": semantic_source,
            "raw_video_verified": raw_verified,
            "profile_type": evidence_tier,
        }
        category_score, category_status = _product_match_score(
            case, top_category=top_category, product_type=product_type
        )
        assignments = assignments_by_video.get(_text(row.get("video_id")), {})
        if not {
            "structure", "scene", "rhythm", "persona_presentation", "visual_hook"
        }.issubset(assignments):
            # A partially assigned row cannot be presented as one observed
            # five-dimensional video pattern.
            continue
        candidates.append({
            "video_id": _text(row.get("video_id")),
            "feature": feature,
            "case": case,
            "assignments": assignments,
            "execution_card": execution_card,
            "category_match_status": category_status,
            "base_score": category_score,
            "structure_measured_available": True,
            "asset": {
                "asset_id": _text(row.get("source_asset_id")),
                "asset_version": _int(row.get("source_asset_version")),
                "review_status": "VIDEO_RECONSTRUCTION_READY",
                "storyboard_json": row.get("shot_list"),
                "source_structure_summary": row.get("source_skeleton_json"),
                "source_style_summary": row.get("scene_json"),
                "entry_signature": row.get("visual_hook_json"),
            },
            # The five dimensions are one atomic observation from this video.
            "same_video_dimension_bundle": {
                "video_id": _text(row.get("video_id")),
                "structure_family": _text(row.get("structure_family")),
                "scene_family": _text(row.get("scene_family")),
                "rhythm_family": _text(row.get("rhythm_family")),
                "persona_family": _text(row.get("persona_family")),
                "visual_hook_family": _text(row.get("visual_hook_family")),
                "assignments": {
                    key: {
                        "run_id": _text(value.get("run_id")),
                        "cluster_id": _id_text(value.get("cluster_id")),
                    }
                    for key, value in assignments.items()
                },
            },
        })

    snapshot_material = {
        "policy_version": REFERENCE_POLICY_VERSION,
        "active_releases": {
            key: {
                "release_id": value.get("release_id"),
                "run_id": value.get("run_id"),
                "schema_version": value.get("schema_version"),
                "published_at": value.get("published_at"),
            }
            for key, value in releases.items()
        },
        "candidate_assets": [
            {
                "video_id": item.get("video_id"),
                "asset_id": item.get("asset", {}).get("asset_id"),
                "asset_version": item.get("asset", {}).get("asset_version"),
                "storyboard_fingerprint": item.get("feature", {}).get("storyboard_fingerprint"),
                "dimension_bundle": item.get("same_video_dimension_bundle"),
            }
            for item in candidates
        ],
        "speech_hook_pool": speech_pool,
    }
    snapshot_hash = _stable_hash(snapshot_material)
    contexts: Dict[str, Dict[str, Any]] = {}
    for direction, provenance in zip(directions, provenances):
        direction_id = provenance["direction_assignment_id"]
        if not direction_id:
            continue
        contexts[direction_id] = {
            "schema_version": REFERENCE_SCHEMA_VERSION,
            "status": "AVAILABLE" if candidates else "UNAVAILABLE",
            "reason": "SCRIPT_EXECUTION_CARD_CANDIDATES" if candidates else "NO_EXECUTABLE_CARD_CANDIDATE",
            "policy_version": REFERENCE_POLICY_VERSION,
            "direction_assignment_id": direction_id,
            "structure_run_id": provenance["source_run_id"],
            "structure_cluster_id": provenance["cluster_id"],
            "structure_cluster_version": provenance["cluster_version"],
            "requested_structure_family": _direction_macro_family(direction),
            "target_country": target_country,
            "target_language": target_language,
            "active_runs": active_runs,
            "active_schema_versions": schema_versions,
            "active_releases": releases,
            "cross_run_resolution": (
                "ACTIVE_STRUCTURE_RELEASE"
                if provenance["source_run_id"] == active_runs.get("structure")
                else "ACTIVE_EXECUTION_RETRIEVAL_WITH_LEGACY_DIRECTION"
            ),
            "candidates": candidates,
            "speech_hook_pool": speech_pool,
            "data_snapshot_hash": snapshot_hash,
        }
    return contexts


def load_multidim_reference_contexts(
    directions: Sequence[Mapping[str, Any]],
    *,
    target_country: str = "",
    target_language: str = "",
    top_category: str = "",
    product_type: str = "",
    database_url: str = "",
) -> Dict[str, Dict[str, Any]]:
    """Load compact candidate contexts for deterministic PLAN_ONLY selection.

    Any failure is returned as an explicit UNAVAILABLE context.  It never
    blocks allocation or silently invents missing reference dimensions.
    """

    provenances = [_direction_provenance(direction) for direction in directions]
    valid = [value for value in provenances if value["direction_assignment_id"]]
    if not multidim_reference_enabled():
        return {
            value["direction_assignment_id"]: _unavailable_context(
                direction, "FEATURE_DISABLED"
            )
            for direction, value in zip(directions, provenances)
            if value["direction_assignment_id"]
        }
    database_url = _text(
        database_url
        or os.environ.get("STRUCTURE_ROUTER_DATABASE_URL")
        or os.environ.get("LIKEU_AI_DATABASE_URL")
    )
    if not database_url:
        return {
            value["direction_assignment_id"]: _unavailable_context(
                direction, "DATABASE_UNAVAILABLE"
            )
            for direction, value in zip(directions, provenances)
            if value["direction_assignment_id"]
        }
    contexts: Dict[str, Dict[str, Any]] = {}
    try:
        with _connect(database_url) as conn:
            with conn.cursor() as cursor:
                return _load_v3_reference_contexts(
                    cursor,
                    directions,
                    target_country=target_country,
                    target_language=target_language,
                    top_category=top_category,
                    product_type=product_type,
                )
                # Kept below for one release as dead source-history only; v3
                # never falls through to the PROMPT_ONLY/table-registry path.
                cursor.execute(
                    """
                    SELECT dimension_type, run_id, schema_status, COUNT(*) AS row_count
                    FROM sd_video_feature_assignment
                    GROUP BY dimension_type, run_id, schema_status
                    """
                )
                available_runs = list(cursor.fetchall())
                runs = dict(_DEFAULT_RUNS)
                schema_versions: Dict[str, str] = {}
                cursor.execute(
                    """
                    SELECT table_name, dimension_type, active_run_id,
                           active_schema_version, schema_status
                    FROM sd_table_registry
                    WHERE active_run_id IS NOT NULL AND active_run_id <> ''
                    """
                )
                registry_rows = list(cursor.fetchall())
                registered_active_runs = {
                    _text(row.get("dimension_type")).lower(): _text(
                        row.get("active_run_id")
                    )
                    for row in registry_rows
                    if _text(row.get("schema_status")).upper() != "DEPRECATED"
                }
                registered_schema_versions = {
                    _text(row.get("dimension_type")).lower(): _text(
                        row.get("active_schema_version")
                    )
                    for row in registry_rows
                    if _text(row.get("schema_status")).upper() != "DEPRECATED"
                }
                for dimension in runs:
                    choices = [
                        row for row in available_runs
                        if _text(row.get("dimension_type")) == dimension
                        and _text(row.get("schema_status")).upper() != "DEPRECATED"
                    ]
                    registry_run = registered_active_runs.get(dimension, "")
                    if registry_run and any(
                        _text(row.get("run_id")) == registry_run for row in choices
                    ):
                        runs[dimension] = registry_run
                        schema_versions[dimension] = registered_schema_versions.get(
                            dimension, ""
                        )
                    configured = [
                        row for row in choices
                        if _text(row.get("run_id")) == runs[dimension]
                    ]
                    if not configured and choices:
                        choices.sort(
                            key=lambda row: (-_int(row.get("row_count")), _text(row.get("run_id")))
                        )
                        runs[dimension] = _text(choices[0].get("run_id"))

                # Load the small prototype catalog once.  Deprecated runs are
                # excluded by the chosen active run IDs.
                prototypes: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
                for dimension, table in _PROTOTYPE_TABLES.items():
                    cursor.execute(
                        f"SELECT * FROM {table} WHERE run_id=%s",
                        (runs[dimension],),
                    )
                    for row in cursor.fetchall():
                        parsed = dict(row)
                        for field in _JSON_FIELDS:
                            if field in parsed:
                                default = [] if field in {"representative_cases", "representative_case_ids"} else {}
                                parsed[field] = _json(parsed.get(field), default)
                        prototypes[dimension][_id_text(parsed.get("cluster_id"))] = parsed

                cursor.execute(
                    """
                    SELECT * FROM sd_feature_cooccurrence
                    WHERE video_count >= 3
                    """
                )
                cooccurrence: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
                for row in cursor.fetchall():
                    cooccurrence[
                        _pair_key(
                            row.get("left_feature_type"), row.get("left_cluster_id"),
                            row.get("right_feature_type"), row.get("right_cluster_id"),
                        )
                    ] = dict(row)

                # Structure membership is the first retrieval gate.
                structure_rows: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
                for provenance in valid:
                    cursor.execute(
                        """
                        SELECT video_id, run_id, cluster_id, schema_status,
                               measured_available
                        FROM sd_video_feature_assignment
                        WHERE dimension_type='structure' AND run_id=%s AND cluster_id=%s
                        """,
                        (provenance["source_run_id"], provenance["cluster_id"]),
                    )
                    structure_rows[(provenance["source_run_id"], provenance["cluster_id"])] = [
                        dict(row) for row in cursor.fetchall()
                    ]

                all_video_ids = sorted({
                    _text(row.get("video_id"))
                    for rows in structure_rows.values() for row in rows
                    if _text(row.get("video_id"))
                })
                cases: Dict[str, Dict[str, Any]] = {}
                assignments_by_video: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
                if all_video_ids:
                    for offset in range(0, len(all_video_ids), 500):
                        chunk = all_video_ids[offset : offset + 500]
                        placeholders = ",".join(["%s"] * len(chunk))
                        cursor.execute(
                            f"SELECT * FROM sd_representative_case WHERE video_id IN ({placeholders})",
                            chunk,
                        )
                        for row in cursor.fetchall():
                            cases[_text(row.get("video_id"))] = dict(row)
                        cursor.execute(
                            f"""
                            SELECT * FROM sd_video_feature_assignment
                            WHERE video_id IN ({placeholders})
                              AND dimension_type IN ('structure','scene','persona_presentation','rhythm','visual_hook','category','country')
                            """,
                            chunk,
                        )
                        for row in cursor.fetchall():
                            dimension = _text(row.get("dimension_type"))
                            if _text(row.get("run_id")) != runs.get(dimension):
                                continue
                            assignments_by_video[_text(row.get("video_id"))][dimension] = dict(row)

                # Pre-rank before loading large storyboard columns.
                selected_video_ids: set[str] = set()
                preselected_by_direction: Dict[str, List[Dict[str, Any]]] = {}
                direction_map = {
                    value["direction_assignment_id"]: value for value in valid
                }
                for direction_id, provenance in direction_map.items():
                    structure_prototype = prototypes.get("structure", {}).get(
                        provenance["cluster_id"], {}
                    )
                    representative_ids = _representative_video_ids(structure_prototype)
                    ranked: List[Dict[str, Any]] = []
                    for structure_row in structure_rows.get(
                        (provenance["source_run_id"], provenance["cluster_id"]), []
                    ):
                        video_id = _text(structure_row.get("video_id"))
                        candidate = {
                            "video_id": video_id,
                            "structure_measured_available": bool(
                                _int(structure_row.get("measured_available"))
                            ),
                            "case": cases.get(video_id, {}),
                            "assignments": assignments_by_video.get(video_id, {}),
                        }
                        score, category_status = _base_candidate_score(
                            candidate,
                            top_category=top_category,
                            product_type=product_type,
                            target_country=target_country,
                            representative_video_ids=representative_ids,
                        )
                        candidate["base_score"] = score
                        candidate["category_match_status"] = category_status
                        ranked.append(candidate)
                    ranked.sort(
                        key=lambda item: (-_float(item.get("base_score")), _text(item.get("video_id")))
                    )
                    # Keep enough breadth for scene/carrier matching but do
                    # not load the whole corpus's long storyboard payload.
                    selected = ranked[:80]
                    selected_ids = {_text(item.get("video_id")) for item in selected}
                    for video_id in representative_ids:
                        if video_id not in selected_ids:
                            found = next(
                                (item for item in ranked if item.get("video_id") == video_id), None
                            )
                            if found:
                                selected.append(found)
                    preselected_by_direction[direction_id] = selected
                    selected_video_ids.update(
                        _text(item.get("video_id")) for item in selected
                    )

                asset_rows: List[Dict[str, Any]] = []
                selected_list = sorted(value for value in selected_video_ids if value)
                for offset in range(0, len(selected_list), 300):
                    chunk = selected_list[offset : offset + 300]
                    placeholders = ",".join(["%s"] * len(chunk))
                    cursor.execute(
                        f"""
                        SELECT asset_id, source_video_id, version, asset_version,
                               is_frozen, review_status, updated_at,
                               storyboard_json, source_structure_summary,
                               source_style_summary, entry_signature
                        FROM video_reconstruction_asset
                        WHERE source_video_id IN ({placeholders})
                          AND storyboard_json IS NOT NULL
                          AND storyboard_json NOT IN ('', '[]', '{{}}')
                        """,
                        chunk,
                    )
                    asset_rows.extend(dict(row) for row in cursor.fetchall())
                assets = _latest_assets(asset_rows)

                for direction, provenance in zip(directions, provenances):
                    direction_id = provenance["direction_assignment_id"]
                    if not direction_id:
                        continue
                    candidates: List[Dict[str, Any]] = []
                    for raw in preselected_by_direction.get(direction_id, []):
                        video_id = _text(raw.get("video_id"))
                        asset = assets.get(video_id)
                        if not asset:
                            continue
                        assignments = raw.get("assignments") or {}
                        candidate_prototypes = {
                            dimension: prototypes.get(dimension, {}).get(
                                _id_text(assignments.get(dimension, {}).get("cluster_id")), {}
                            )
                            for dimension in (
                                "structure", "scene", "persona_presentation", "rhythm", "visual_hook"
                            )
                        }
                        candidates.append({
                            **raw,
                            "asset": asset,
                            "prototype_projections": {
                                dimension: _prototype_projection(dimension, value)
                                for dimension, value in candidate_prototypes.items()
                            },
                        })
                    snapshot_material = {
                        "policy_version": REFERENCE_POLICY_VERSION,
                        "runs": runs,
                        "schema_versions": schema_versions,
                        "structure_run_id": provenance["source_run_id"],
                        "structure_cluster_id": provenance["cluster_id"],
                        "candidate_assets": [
                            {
                                "video_id": item.get("video_id"),
                                "asset_id": item.get("asset", {}).get("asset_id"),
                                "asset_version": item.get("asset", {}).get("asset_version"),
                                "asset_updated_at": item.get("asset", {}).get("updated_at"),
                                "storyboard_hash": _stable_hash(
                                    item.get("asset", {}).get("storyboard_json", ""), 16
                                ),
                                "assignments": {
                                    key: value.get("cluster_id")
                                    for key, value in (item.get("assignments") or {}).items()
                                },
                            }
                            for item in candidates
                        ],
                    }
                    contexts[direction_id] = {
                        "schema_version": REFERENCE_SCHEMA_VERSION,
                        "status": "AVAILABLE" if candidates else "UNAVAILABLE",
                        "reason": "CASE_FIRST_CANDIDATES" if candidates else "NO_PINNED_STORYBOARD_CANDIDATE",
                        "policy_version": REFERENCE_POLICY_VERSION,
                        "direction_assignment_id": direction_id,
                        "structure_run_id": provenance["source_run_id"],
                        "structure_cluster_id": provenance["cluster_id"],
                        "structure_cluster_version": provenance["cluster_version"],
                        "cross_run_resolution": (
                            "DIRECT_ACTIVE_STRUCTURE_RUN"
                            if provenance["source_run_id"] == runs["structure"]
                            else "SOURCE_MEMBERSHIP_WITH_ACTIVE_AUX_DIMENSIONS"
                        ),
                        "active_runs": runs,
                        "active_schema_versions": schema_versions,
                        "candidates": candidates,
                        "prototypes": prototypes,
                        "cooccurrence": cooccurrence,
                        "data_snapshot_hash": _stable_hash(snapshot_material),
                    }
        return contexts
    except Exception as exc:
        reason = f"RDS_READ_FAILED:{type(exc).__name__}"
        return {
            value["direction_assignment_id"]: _unavailable_context(
                direction, reason
            )
            for direction, value in zip(directions, provenances)
            if value["direction_assignment_id"]
        }


def _scene_cluster_preferences(creative_contract: Mapping[str, Any]) -> List[str]:
    scene = creative_contract.get("scene_reference_contract")
    scene = scene if isinstance(scene, Mapping) else {}
    values = [scene.get("primary_scene_cluster_id")]
    values.extend(scene.get("supporting_scene_cluster_ids") or [])
    result: List[str] = []
    for value in values:
        text = _id_text(value)
        if text and text not in result:
            result.append(text)
    return result


def _candidate_contract(
    candidate: Mapping[str, Any], *, score: float, category_status: str,
    cooccurrence_evidence: Sequence[Mapping[str, Any]], scene_match: bool,
    role: str, eligibility_status: str, storyboard_carrier: str,
    eligibility_reasons: Sequence[str], support_storyboard_limit: int = 6,
) -> Dict[str, Any]:
    asset = candidate.get("asset") if isinstance(candidate.get("asset"), Mapping) else {}
    case = candidate.get("case") if isinstance(candidate.get("case"), Mapping) else {}
    assignments = candidate.get("assignments") if isinstance(candidate.get("assignments"), Mapping) else {}
    projections = candidate.get("prototype_projections") if isinstance(candidate.get("prototype_projections"), Mapping) else {}
    storyboard = _compact_storyboard(
        asset.get("storyboard_json"), limit=support_storyboard_limit
    )
    execution_card = (
        dict(candidate.get("execution_card"))
        if isinstance(candidate.get("execution_card"), Mapping)
        else _reference_execution_spine(
            storyboard, video_id=_text(candidate.get("video_id"))
        )
    )
    evidence_tier = _text(case.get("evidence_tier")) or _text(
        (execution_card.get("_meta") or {}).get("evidence_tier")
    )
    return {
        "role": role,
        "video_id": _text(candidate.get("video_id")),
        "asset_id": _text(asset.get("asset_id")),
        "asset_version": _int(asset.get("asset_version"), _int(asset.get("version"))),
        "asset_snapshot_hash": _stable_hash({
            "asset_id": asset.get("asset_id"),
            "asset_version": asset.get("asset_version") or asset.get("version"),
            "updated_at": asset.get("updated_at"),
            "storyboard": asset.get("storyboard_json"),
        }),
        "review_status": _text(asset.get("review_status")),
        "selection_score": round(score, 4),
        "category_match_status": category_status,
        "eligibility_status": eligibility_status,
        "eligibility_reasons": list(eligibility_reasons),
        "storyboard_observed_carrier": storyboard_carrier,
        "source_carrier_consistency": (
            "CONFLICT"
            if _source_carrier_conflict(
                case.get("content_carrier"), storyboard_carrier
            )
            else "CONSISTENT_OR_UNKNOWN"
        ),
        "scene_match": bool(scene_match),
        "evidence_tier": evidence_tier,
        "profile_type": _text(case.get("profile_type")),
        "has_measured_structure": bool(candidate.get("structure_measured_available")),
        "semantic_source": _text(case.get("semantic_source")),
        "raw_video_verified": bool(case.get("raw_video_verified")),
        "creator_id": _text(case.get("creator_id")),
        "template_group_id": _text(case.get("template_group_id")),
        "_meta": {
            "evidence_tier": evidence_tier,
            "semantic_source": _text(case.get("semantic_source")),
            "raw_video_verified": bool(case.get("raw_video_verified")),
        },
        "source_metadata": {
            "cat1": _text(case.get("cat1")),
            "cat2": _text(case.get("cat2")),
            "country": _text(case.get("country")),
            "content_carrier": _text(case.get("content_carrier")),
            "continuity_mode": _text(case.get("continuity_mode")),
            "perf_score": _float(case.get("perf_score")),
        },
        "source_summary": {
            "structure": _compact_text(asset.get("source_structure_summary"), 420),
            "style": _compact_text(asset.get("source_style_summary"), 320),
            "entry": _compact_text(asset.get("entry_signature"), 260),
            "opening_logic": _compact_text(case.get("opening_logic"), 300),
            "camera_logic": _compact_text(case.get("camera_logic"), 300),
            "action_logic": _compact_text(case.get("action_logic"), 300),
            "rhythm_logic": _compact_text(case.get("rhythm_logic"), 260),
        },
        "storyboard": storyboard,
        "execution_card": execution_card,
        # Compatibility alias for reports and old completed-batch readers.
        "reference_execution_spine": execution_card,
        "same_video_dimension_bundle": dict(
            candidate.get("same_video_dimension_bundle") or {}
        ),
        "feature_assignments": {
            dimension: {
                "run_id": _text(value.get("run_id")),
                "cluster_id": _id_text(value.get("cluster_id")),
                "schema_status": _text(value.get("schema_status")),
                "measured_available": bool(_int(value.get("measured_available"))),
            }
            for dimension, value in assignments.items()
            if isinstance(value, Mapping)
        },
        "dimension_references": {
            dimension: dict(value)
            for dimension, value in projections.items()
            if isinstance(value, Mapping) and value.get("status") == "AVAILABLE"
        },
        "cooccurrence_evidence": [dict(value) for value in cooccurrence_evidence],
    }


def select_retrieval_reference_contract(
    context: Optional[Mapping[str, Any]],
    *,
    creative_contract: Mapping[str, Any],
    content_bundle: Mapping[str, Any],
    requested_hook_id: str,
    used_video_ids: Optional[MutableMapping[str, int]] = None,
    category_execution_extension: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Select one primary real case and at most one supporting case.

    The function is deterministic for the same frozen context and usage
    counters.  It contains no model call and no hard production gate.
    """

    context = context if isinstance(context, Mapping) else {}
    base = {
        "schema_version": REFERENCE_SCHEMA_VERSION,
        "policy_version": REFERENCE_POLICY_VERSION,
        "status": "UNAVAILABLE",
        "selection_mode": "NO_EFFECT",
        "direction_assignment_id": _text(context.get("direction_assignment_id")),
        "structure_run_id": _text(context.get("structure_run_id")),
        "structure_cluster_id": _id_text(context.get("structure_cluster_id")),
        "active_runs": dict(context.get("active_runs") or {}),
        "active_schema_versions": dict(
            context.get("active_schema_versions") or {}
        ),
        "active_releases": {
            key: {
                "release_id": _text(value.get("release_id")),
                "run_id": _text(value.get("run_id")),
                "schema_version": _text(value.get("schema_version")),
                "published_at": _text(value.get("published_at")),
            }
            for key, value in (context.get("active_releases") or {}).items()
            if isinstance(value, Mapping)
        },
        "cross_run_resolution": _text(context.get("cross_run_resolution")),
        "data_snapshot_hash": _text(context.get("data_snapshot_hash")),
        "authority_boundary": {
            "structure": "HARD_STRUCTURE_AUTHORITY",
            "scene": "SCENE_REALISM_AUTHORITY_IF_MATCHED",
            "rhythm": "CAPTURE_RHYTHM_AUTHORITY_WITHIN_STRUCTURE",
            "persona_presentation": "RETRIEVAL_AID_ONLY",
            "visual_hook": "RETRIEVAL_AID_ONLY",
            "persona_identity": "CURRENT_PERSONA_TEMPLATE_AUTHORITY",
            "outfit": "CURRENT_OUTFIT_TEMPLATE_AUTHORITY",
            "spoken_hook": "CENTRAL_VOICEOVER_AUTHORITY",
            "speech_hook_pool": "RHETORIC_AND_TARGET_LANGUAGE_EXAMPLE_ONLY",
            "selling_argument": "CURRENT_ORIGINAL_SELLING_ARGUMENT_AUTHORITY",
            "product_truth": "CURRENT_PRODUCT_ANCHOR_AUTHORITY",
        },
        "usage_boundary": (
            "借鉴真实案例的镜头功能、景别推进、动作连续性和节奏；"
            "不得复制来源商品、人物身份、穿搭、品牌、口播或场景中与本条冻结设计冲突的细节。"
        ),
        "primary_case": {},
        "supporting_case": {},
        "primary_execution_card": {},
        "supplemental_execution_card": {},
        "execution_candidate_pool": [],
        "speech_hook_pool": list(context.get("speech_hook_pool") or []),
        "candidate_diagnostics": {},
        "fallback_reason": _text(context.get("reason")) or "CONTEXT_UNAVAILABLE",
    }
    if _text(context.get("status")) != "AVAILABLE":
        return base
    candidates = [
        value for value in (context.get("candidates") or [])
        if isinstance(value, Mapping)
    ]
    if not candidates:
        base["fallback_reason"] = "NO_CANDIDATES"
        return base

    presentation = _text(
        creative_contract.get("presentation_mode")
        or (creative_contract.get("surface_profile") or {}).get("presentation_mode")
    ).upper()
    if not presentation:
        carrier = _text(
            creative_contract.get("content_carrier")
            or creative_contract.get("carrier_mode")
        ).upper()
        presentation = (
            "STATIC_PRODUCT" if carrier == "STATIC_PRODUCT"
            else "HANDS_ONLY" if carrier in {"HAND_ONLY", "HANDS_ONLY"}
            else "PERSON_ON_CAMERA"
        )
    scene_preferences = _scene_cluster_preferences(creative_contract)
    requested_actions = _requested_action_tokens(
        creative_contract,
        content_bundle,
        category_execution_extension,
    )
    proof_intent = (
        content_bundle.get("proof_execution_intent")
        if isinstance(content_bundle.get("proof_execution_intent"), Mapping)
        else {}
    )
    selling_actions = [
        _text(value).upper()
        for value in (proof_intent.get("preferred_action_tokens") or [])
        if _text(value)
    ]
    used_video_ids = used_video_ids if used_video_ids is not None else Counter()
    requested_structure_family = ">".join(
        part.strip().upper()
        for part in _text(context.get("requested_structure_family")).split(">")
        if part.strip()
    )
    cooccurrence = context.get("cooccurrence")
    cooccurrence = cooccurrence if isinstance(cooccurrence, Mapping) else {}
    ranked: List[Tuple[float, str, Dict[str, Any]]] = []
    eligibility_counts: Counter = Counter()
    quarantine_reasons: Counter = Counter()
    evidence_counts: Counter = Counter()
    for candidate in candidates:
        video_id = _text(candidate.get("video_id"))
        case = candidate.get("case") if isinstance(candidate.get("case"), Mapping) else {}
        assignments = candidate.get("assignments") if isinstance(candidate.get("assignments"), Mapping) else {}
        asset = candidate.get("asset") if isinstance(candidate.get("asset"), Mapping) else {}
        compact_storyboard = _compact_storyboard(asset.get("storyboard_json"))
        storyboard_carrier = _infer_storyboard_carrier(compact_storyboard)
        metadata_carrier = _text(case.get("content_carrier"))
        execution_card = (
            candidate.get("execution_card")
            if isinstance(candidate.get("execution_card"), Mapping)
            else {}
        )
        action_score, action_status = _action_compatibility(
            case.get("physical_action_type")
            or execution_card.get("physical_action_type"),
            requested_actions,
        )
        selling_action_score, selling_action_status = _action_compatibility(
            case.get("physical_action_type")
            or execution_card.get("physical_action_type"),
            selling_actions,
        )
        available_parts, card_shots = _card_completeness(execution_card)
        observed_shots = max(card_shots, len(compact_storyboard))
        bundle = (
            candidate.get("same_video_dimension_bundle")
            if isinstance(candidate.get("same_video_dimension_bundle"), Mapping)
            else {}
        )
        rhythm_family = _text(bundle.get("rhythm_family")).upper()
        if not rhythm_family:
            rhythm_projection = (
                (candidate.get("prototype_projections") or {}).get("rhythm")
                if isinstance(candidate.get("prototype_projections"), Mapping)
                else {}
            )
            rhythm_projection = (
                rhythm_projection if isinstance(rhythm_projection, Mapping) else {}
            )
            rhythm_family = _text(
                rhythm_projection.get("production_family_id")
                or rhythm_projection.get("production_family_name")
            ).upper()
        single_take_case = (
            observed_shots <= 1 or "SINGLE_TAKE" in rhythm_family
        )
        shot_richness_eligible = observed_shots >= 3 and not single_take_case
        target_duration = _float(
            creative_contract.get("duration_seconds")
            or context.get("duration_seconds"),
            15.0,
        )
        observed_duration = _float(execution_card.get("duration_sec"))
        duration_fit = 0.0
        if observed_duration > 0 and target_duration > 0:
            relative_gap = abs(observed_duration - target_duration) / target_duration
            duration_fit = max(-1.0, 1.0 - relative_gap)
        proof_alignment = _proof_execution_alignment(
            execution_card, proof_intent
        )
        evidence_tier = _text(case.get("evidence_tier"))
        evidence_counts[evidence_tier or "UNKNOWN"] += 1
        carrier_match = _carrier_compatible(presentation, metadata_carrier)
        eligibility_status, eligibility_reasons = _case_eligibility(
            category_status=_text(candidate.get("category_match_status")),
            requested_presentation=presentation,
            metadata_carrier=metadata_carrier,
            storyboard_carrier=storyboard_carrier,
        )
        active_structure_run = _text(
            (context.get("active_runs") or {}).get("structure")
            if isinstance(context.get("active_runs"), Mapping) else ""
        )
        requested_structure_run = _text(context.get("structure_run_id"))
        requested_structure_cluster = _id_text(context.get("structure_cluster_id"))
        observed_structure_cluster = _id_text(
            (assignments.get("structure") or {}).get("cluster_id")
            if isinstance(assignments.get("structure"), Mapping) else ""
        )
        if (
            active_structure_run
            and requested_structure_run == active_structure_run
            and requested_structure_cluster
            and observed_structure_cluster != requested_structure_cluster
        ):
            eligibility_status = "QUARANTINED"
            eligibility_reasons = [
                *eligibility_reasons,
                "ACTIVE_STRUCTURE_CLUSTER_MISMATCH",
            ]
        eligibility_counts[eligibility_status] += 1
        quarantine_reasons.update(eligibility_reasons)
        scene_cluster = _id_text((assignments.get("scene") or {}).get("cluster_id"))
        projections = candidate.get("prototype_projections") or {}
        scene_ref = projections.get("scene") or {}
        scene_reference_usable = (
            _text(scene_ref.get("schema_status")).upper() == "AUTHORITY"
            and _text(scene_ref.get("scene_executability")).upper() == "AVAILABLE"
        )
        scene_match = bool(
            scene_reference_usable
            and scene_preferences
            and scene_cluster in scene_preferences
        )
        category_rank = {
            "EXACT_PRODUCT_TYPE": 3,
            "SAME_PRODUCT_FAMILY": 2,
            "SAME_TOP_CATEGORY": 1,
        }.get(_text(candidate.get("category_match_status")), 0)
        # Lexicographic business precedence expressed as separated score
        # bands: subtype > carrier/proof compatibility > visible clip
        # progression and duration fit > completeness > dedupe > raw
        # verification > performance.  raw verification is never a gate.
        score = category_rank * 100000.0
        score += 20000.0 if carrier_match else -20000.0
        # The operator-selected selling argument owns the proof action.  The
        # creative action remains useful, but cannot outrank a case that
        # actually demonstrates the selling point's required function.
        score += selling_action_score * 6000.0
        score += action_score * 1800.0
        if _country_key(case.get("country")) == _country_key(
            creative_contract.get("target_country")
            or context.get("target_country")
        ):
            score += 1200.0
        score += 7000.0 if shot_richness_eligible else -25000.0
        score += min(observed_shots, 5) * 700.0
        score += duration_fit * 1500.0
        score += available_parts * 700.0
        score += len(proof_alignment["preferred_part_matches"]) * 2600.0
        if proof_alignment["required_any_parts"]:
            score += 5200.0 if proof_alignment["required_part_match"] else -5200.0
        if scene_preferences:
            score += 120.0 if scene_match else 0.0
        cooc_bonus, cooc_evidence = _cooccurrence_bonus(assignments, cooccurrence)
        score += cooc_bonus
        creator_id = _text(case.get("creator_id"))
        template_group_id = _text(case.get("template_group_id"))
        score -= 9000.0 * _int(used_video_ids.get(video_id, 0))
        score -= 1800.0 * _int(used_video_ids.get("creator:" + creator_id, 0)) if creator_id else 0.0
        score -= 1600.0 * _int(used_video_ids.get("template:" + template_group_id, 0)) if template_group_id else 0.0
        if bool(case.get("raw_video_verified")):
            score += 200.0
        score += max(0.0, min(1.0, _float(case.get("perf_score")))) * 100.0
        score += max(0.0, min(1.0, _float(case.get("extraction_confidence")))) * 20.0
        # Prefer an observed opening and non-generic rhythm, but never make
        # either mandatory.
        visual_ref = projections.get("visual_hook") or {}
        rhythm_ref = projections.get("rhythm") or {}
        if _text(visual_ref.get("entry_subject")):
            score += 2.0
        if rhythm_ref and not bool(rhythm_ref.get("is_generic_rhythm")):
            score += 2.0
        ranked.append((
            score,
            video_id,
            {
                "candidate": candidate,
                "carrier_match": carrier_match,
                "scene_match": scene_match,
                "cooccurrence_evidence": cooc_evidence,
                "eligibility_status": eligibility_status,
                "eligibility_reasons": eligibility_reasons,
                "storyboard_carrier": storyboard_carrier,
                "action_status": action_status,
                "action_score": action_score,
                "selling_action_status": selling_action_status,
                "selling_action_score": selling_action_score,
                "proof_execution_match": proof_alignment,
                "card_completeness": available_parts,
                "observed_shot_count": observed_shots,
                "rhythm_family": rhythm_family,
                "single_take_case": single_take_case,
                "shot_richness_eligible": shot_richness_eligible,
                "duration_fit": round(duration_fit, 4),
                "structure_family": ">".join(
                    part.strip().upper()
                    for part in _text(
                        (candidate.get("same_video_dimension_bundle") or {}).get(
                            "structure_family"
                        )
                        if isinstance(
                            candidate.get("same_video_dimension_bundle"), Mapping
                        ) else ""
                    ).split(">")
                    if part.strip()
                ),
            },
        ))
    ranked.sort(key=lambda item: (-item[0], item[1]))
    base["candidate_diagnostics"] = {
        "total": len(ranked),
        "eligibility_distribution": dict(eligibility_counts),
        "reason_distribution": dict(quarantine_reasons),
        "evidence_distribution": dict(evidence_counts),
        "shot_richness_distribution": dict(Counter(
            "ELIGIBLE"
            if item[2].get("shot_richness_eligible")
            else "SINGLE_TAKE_OR_LT3_SHOTS"
            for item in ranked
        )),
        "requested_action_tokens": requested_actions,
        "selling_action_tokens": selling_actions,
        "proof_execution_intent": dict(proof_intent),
    }
    primary_pool = [
        item for item in ranked
        if item[2]["carrier_match"]
        and item[2]["shot_richness_eligible"]
        and item[2]["eligibility_status"] == "ELIGIBLE"
        and (
            not item[2]["proof_execution_match"]["hard_required_part_match"]
            or item[2]["proof_execution_match"]["required_part_match"]
        )
    ]
    family_fallback_used = False
    if not primary_pool:
        # A routed cluster can be newer or finer-grained than the execution
        # corpus.  When that leaves the exact cluster empty, a real video from
        # the same macro family is a useful *support-level* execution example.
        # The whole five-dimensional bundle still comes from one video; only
        # the exact-cluster requirement is relaxed and disclosed.
        primary_pool = [
            item for item in ranked
            if item[2]["carrier_match"]
            and item[2]["shot_richness_eligible"]
            and requested_structure_family
            and item[2].get("structure_family") == requested_structure_family
            and _text(item[2]["candidate"].get("category_match_status"))
            in {"EXACT_PRODUCT_TYPE", "SAME_PRODUCT_FAMILY"}
            and set(item[2].get("eligibility_reasons") or [])
            == {"ACTIVE_STRUCTURE_CLUSTER_MISMATCH"}
            and (
                not item[2]["proof_execution_match"]["hard_required_part_match"]
                or item[2]["proof_execution_match"]["required_part_match"]
            )
        ]
        family_fallback_used = bool(primary_pool)
        if not primary_pool:
            eligible_without_proof = any(
                item[2]["carrier_match"]
                and item[2]["eligibility_status"] == "ELIGIBLE"
                for item in ranked
            )
            compatible_but_too_simple = any(
                item[2]["carrier_match"]
                and not item[2]["shot_richness_eligible"]
                and item[2]["eligibility_status"] == "ELIGIBLE"
                for item in ranked
            )
            base["fallback_reason"] = (
                "NO_EXECUTION_CASE_FOR_PROOF_INTENT"
                if proof_intent.get("hard_required_part_match")
                and eligible_without_proof
                else "NO_SHOT_RICH_EXECUTION_CASE"
                if compatible_but_too_simple
                else "NO_ELIGIBLE_PRIMARY_CASE"
            )
            return base
    primary_rank = primary_pool[0]
    primary_score, primary_video_id, primary_meta = primary_rank
    primary = _candidate_contract(
        primary_meta["candidate"],
        score=primary_score,
        category_status=_text(primary_meta["candidate"].get("category_match_status")),
        cooccurrence_evidence=primary_meta["cooccurrence_evidence"],
        scene_match=primary_meta["scene_match"],
        role="PRIMARY_REAL_CASE",
        eligibility_status=(
            "MACRO_FAMILY_SUPPORT"
            if family_fallback_used else primary_meta["eligibility_status"]
        ),
        storyboard_carrier=primary_meta["storyboard_carrier"],
        eligibility_reasons=primary_meta["eligibility_reasons"],
    )
    if not primary.get("storyboard"):
        base["fallback_reason"] = "PRIMARY_STORYBOARD_EMPTY"
        return base
    primary["proof_execution_match"] = dict(
        primary_meta["proof_execution_match"]
    )
    primary["shot_richness_match"] = {
        "status": "ELIGIBLE",
        "observed_shot_count": int(primary_meta.get("observed_shot_count") or 0),
        "rhythm_family": _text(primary_meta.get("rhythm_family")),
        "duration_fit": primary_meta.get("duration_fit"),
        "policy_version": "original-15s-shot-richness-v1",
    }

    support: Dict[str, Any] = {}
    primary_category_status = _text(
        primary_meta["candidate"].get("category_match_status")
    )
    support_pool = [
        item for item in ranked
        if item[2]["carrier_match"]
        and item[2]["shot_richness_eligible"]
        and item[2]["eligibility_status"] in {"ELIGIBLE", "SUPPORT_ONLY"}
    ]
    primary_creator = _text(primary_meta["candidate"].get("case", {}).get("creator_id"))
    primary_template = _text(primary_meta["candidate"].get("case", {}).get("template_group_id"))
    distinct_support_pool = [
        item for item in support_pool
        if item[1] != primary_video_id
        and (
            not primary_creator
            or _text(item[2]["candidate"].get("case", {}).get("creator_id")) != primary_creator
        )
        and (
            not primary_template
            or _text(item[2]["candidate"].get("case", {}).get("template_group_id")) != primary_template
        )
    ]
    for score, video_id, meta in (distinct_support_pool or support_pool):
        if video_id == primary_video_id or not meta["carrier_match"]:
            continue
        if (
            meta["eligibility_status"] == "ELIGIBLE"
            and _text(meta["candidate"].get("category_match_status"))
            != primary_category_status
        ):
            continue
        support = _candidate_contract(
            meta["candidate"],
            score=score,
            category_status=_text(meta["candidate"].get("category_match_status")),
            cooccurrence_evidence=meta["cooccurrence_evidence"],
            scene_match=meta["scene_match"],
            role="SUPPORTING_REAL_CASE",
            eligibility_status=meta["eligibility_status"],
            storyboard_carrier=meta["storyboard_carrier"],
            eligibility_reasons=meta["eligibility_reasons"],
            support_storyboard_limit=2,
        )
        support["proof_execution_match"] = dict(meta["proof_execution_match"])
        support["shot_richness_match"] = {
            "status": "ELIGIBLE",
            "observed_shot_count": int(meta.get("observed_shot_count") or 0),
            "rhythm_family": _text(meta.get("rhythm_family")),
            "duration_fit": meta.get("duration_fit"),
            "policy_version": "original-15s-shot-richness-v1",
        }
        break

    used_video_ids[primary_video_id] = _int(used_video_ids.get(primary_video_id, 0)) + 1
    if primary_creator:
        key = "creator:" + primary_creator
        used_video_ids[key] = _int(used_video_ids.get(key, 0)) + 1
    if primary_template:
        key = "template:" + primary_template
        used_video_ids[key] = _int(used_video_ids.get(key, 0)) + 1

    candidate_pool: List[Dict[str, Any]] = []
    for score, _, meta in primary_pool[:8]:
        candidate_contract = _candidate_contract(
            meta["candidate"],
            score=score,
            category_status=_text(meta["candidate"].get("category_match_status")),
            cooccurrence_evidence=meta["cooccurrence_evidence"],
            scene_match=meta["scene_match"],
            role="EXECUTION_CANDIDATE",
            eligibility_status=meta["eligibility_status"],
            storyboard_carrier=meta["storyboard_carrier"],
            eligibility_reasons=meta["eligibility_reasons"],
            support_storyboard_limit=3,
        )
        candidate_contract["proof_execution_match"] = dict(
            meta["proof_execution_match"]
        )
        candidate_pool.append(candidate_contract)
    return {
        **base,
        "status": "AVAILABLE",
        "selection_mode": (
            "SCRIPT_EXECUTION_CARD_SAME_VIDEO_MACRO_FAMILY_FALLBACK"
            if family_fallback_used
            else "SCRIPT_EXECUTION_CARD_SAME_VIDEO_DIMENSIONS"
        ),
        "requested_spoken_hook_id": _text(requested_hook_id),
        "content_bundle_id": _text(content_bundle.get("content_bundle_id")),
        "proof_execution_intent": dict(proof_intent),
        "primary_case": primary,
        "supporting_case": support,
        "primary_execution_card": primary,
        "supplemental_execution_card": support,
        "execution_candidate_pool": candidate_pool,
        "speech_hook_pool": list(context.get("speech_hook_pool") or []),
        "scene_alignment_status": (
            "MATCHED_SELECTED_SCENE" if primary.get("scene_match")
            else "STRUCTURE_CARRIER_MATCH_ONLY"
        ),
        "fallback_reason": (
            "EXACT_CLUSTER_EMPTY_MACRO_FAMILY_SUPPORT"
            if family_fallback_used else ""
        ),
        "contract_hash": _stable_hash({
            "snapshot": context.get("data_snapshot_hash"),
            "primary": {
                "video_id": primary.get("video_id"),
                "asset_id": primary.get("asset_id"),
                "asset_version": primary.get("asset_version"),
            },
            "support": {
                "video_id": support.get("video_id"),
                "asset_id": support.get("asset_id"),
                "asset_version": support.get("asset_version"),
            },
            "proof_execution_intent": proof_intent,
            "primary_proof_execution_match": primary.get(
                "proof_execution_match"
            ),
        }),
    }


def model_visible_reference_projection(
    contract: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Return the compact few-shot payload intended for the blueprint model."""

    contract = contract if isinstance(contract, Mapping) else {}
    if _text(contract.get("status")) != "AVAILABLE":
        return {
            "status": "UNAVAILABLE",
            "fallback_reason": _text(contract.get("fallback_reason")),
        }
    primary = contract.get("primary_execution_card") or contract.get("primary_case")
    primary = primary if isinstance(primary, Mapping) else {}
    support = contract.get("supplemental_execution_card") or contract.get("supporting_case")
    support = support if isinstance(support, Mapping) else {}
    scene_reference: Dict[str, Any] = {}
    if _text(contract.get("scene_alignment_status")) == "MATCHED_SELECTED_SCENE":
        dimensions = primary.get("dimension_references")
        dimensions = dimensions if isinstance(dimensions, Mapping) else {}
        raw_scene = dimensions.get("scene")
        raw_scene = raw_scene if isinstance(raw_scene, Mapping) else {}
        if (
            _text(raw_scene.get("schema_status")).upper() == "AUTHORITY"
            and _text(raw_scene.get("scene_executability")).upper() == "AVAILABLE"
        ):
            anchors = raw_scene.get("distinctive_anchors")
            anchors = anchors if isinstance(anchors, Mapping) else {}
            cleaned_anchors: Dict[str, Any] = {}
            for key, raw in anchors.items():
                if key == "generic_fillers":
                    continue
                values = raw if isinstance(raw, list) else [raw]
                cleaned = [_sanitize_scene_anchor(item) for item in values[:2]]
                cleaned = [item for item in cleaned if item]
                if cleaned:
                    cleaned_anchors[key] = cleaned
            scene_reference = {
                "name": _text(raw_scene.get("name")),
                "summary": _compact_text(raw_scene.get("summary"), 220),
                "distinctive_anchors": cleaned_anchors,
            }
    primary_spine = (
        dict(primary.get("execution_card") or primary.get("reference_execution_spine") or {})
        if isinstance(primary.get("execution_card") or primary.get("reference_execution_spine"), Mapping)
        else {}
    )
    support_spine = (
        dict(support.get("execution_card") or support.get("reference_execution_spine") or {})
        if isinstance(support.get("execution_card") or support.get("reference_execution_spine"), Mapping)
        else {}
    )
    primary_payload = {
        "_meta": dict(primary.get("_meta") or {}),
        "same_video_dimension_bundle": dict(
            primary.get("same_video_dimension_bundle") or {}
        ),
        "matched_scene_realism": scene_reference,
    }
    if _text(primary_spine.get("execution_card_id")):
        primary_payload["execution_card"] = primary_spine
    else:
        primary_payload["reference_execution_spine"] = primary_spine
    support_payload = {"_meta": dict(support.get("_meta") or {})}
    if support:
        if _text(support_spine.get("execution_card_id")):
            support_payload["execution_card"] = support_spine
        else:
            support_payload["opening_inspiration"] = (
                (support_spine.get("parts") or {}).get("opening", {})
                if isinstance(support_spine.get("parts"), Mapping)
                else {}
            )
    return {
        "status": "AVAILABLE",
        "selection_mode": _text(contract.get("selection_mode")),
        "usage_boundary": _text(contract.get("usage_boundary")),
        "authority_boundary": dict(contract.get("authority_boundary") or {}),
        "scene_alignment_status": _text(contract.get("scene_alignment_status")),
        "execution_card_id": _text(
            primary_spine.get("execution_card_id")
            or primary_spine.get("reference_spine_id")
        ),
        "primary_real_case": primary_payload,
        "supporting_real_case": support_payload if support else {},
    }


def legacy_execution_reference_projection(
    contract: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Project the selected real case onto the established execution surface.

    New production reads the richer retrieval contract.  Older downstream
    code still reads ``execution_reference``.  Keeping this deterministic
    projection prevents a successfully retrieved real case from becoming an
    empty STRUCTURE_ONLY reference later in the same frozen batch.
    """

    contract = contract if isinstance(contract, Mapping) else {}
    if _text(contract.get("status")) != "AVAILABLE":
        return {}
    primary = contract.get("primary_execution_card") or contract.get("primary_case")
    primary = primary if isinstance(primary, Mapping) else {}
    card = primary.get("execution_card") or primary.get("reference_execution_spine")
    card = card if isinstance(card, Mapping) else {}
    parts = card.get("parts") if isinstance(card.get("parts"), Mapping) else {}
    ordered = [
        parts.get(name) for name in ("opening", "proof", "use_process", "ending")
        if isinstance(parts.get(name), Mapping)
        and _text(parts.get(name, {}).get("status")) == "AVAILABLE"
    ]
    source = primary.get("source_metadata")
    source = source if isinstance(source, Mapping) else {}
    return {
        "reference_status": "VIDEO_REFERENCED",
        "execution_card_id": _text(
            card.get("execution_card_id") or card.get("reference_spine_id")
        ),
        "content_carrier": _text(
            card.get("content_carrier") or source.get("content_carrier")
        ),
        "action_spine": [
            _text(item.get("visual_action")) for item in ordered
            if _text(item.get("visual_action"))
        ],
        "camera_grammar": [
            _text(item.get("framing_and_transition")) for item in ordered
            if _text(item.get("framing_and_transition"))
        ],
        "visual_hook_type": _text(
            (parts.get("opening") or {}).get("shot_function")
            if isinstance(parts.get("opening"), Mapping) else ""
        ),
        "do_not_invent": [],
        "retrieval_selection_mode": _text(contract.get("selection_mode")),
        "_meta": {
            **dict(primary.get("_meta") or {}),
            "video_id": _text(primary.get("video_id")),
            "asset_id": _text(primary.get("asset_id")),
            "asset_version": _int(primary.get("asset_version")),
            "eligibility_status": _text(primary.get("eligibility_status")),
        },
    }
