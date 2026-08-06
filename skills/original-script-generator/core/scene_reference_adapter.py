"""Thin, read-only scene reference adapter for original-script planning.

The scene corpus is intentionally advisory.  It can enrich an already valid
creative candidate, but it may never authorize product claims, replace a
structure carrier, block a plan, or make an additional model call.
"""
from __future__ import annotations

import json
import math
import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence
from urllib.parse import unquote, urlparse


SCENE_REFERENCE_SCHEMA_VERSION = "scene-reference-contract-v2"
SCENE_REFERENCE_POLICY_VERSION = "scene-routing-policy-v1"
_SKILL_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_POLICY_PATH = _SKILL_ROOT / "config" / "scene_routing_policy_v1.json"


def _text(value: Any) -> str:
    return str(value or "").strip()


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


def _list(value: Any) -> List[Any]:
    """Accept the JSON columns returned by MySQL without leaking raw data."""

    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _mapping(value: Any) -> Dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _unique_texts(values: Iterable[Any], *, limit: int) -> List[str]:
    result: List[str] = []
    for value in values:
        text = _text(value)
        if text and text not in result:
            result.append(text)
        if len(result) >= limit:
            break
    return result


def _same_country(left: Any, right: Any) -> bool:
    a, b = _text(left).lower(), _text(right).lower()
    if not a or not b:
        return False
    thai = {"th", "thai", "thailand", "泰国"}
    if a in thai and b in thai:
        return True
    return a == b


def _category_matches(observation: Mapping[str, Any], category: Any) -> bool:
    target = _text(category).lower()
    if not target:
        return False
    observed = " ".join(
        _text(observation.get(key)).lower() for key in ("cat1", "cat2")
    )
    return target in observed or observed in target


def scene_reference_enabled() -> bool:
    return _text(os.environ.get("ORIGINAL_SCRIPT_SCENE_REFERENCE_ENABLED")).lower() in {
        "1", "true", "yes", "on",
    }


@lru_cache(maxsize=2)
def load_scene_policy(policy_path: str = "") -> Dict[str, Any]:
    """Load the small, versioned application map without touching RDS."""

    path = Path(policy_path) if policy_path else _DEFAULT_POLICY_PATH
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(parsed, dict):
        return {}
    return parsed


def scene_family_for_motif(scene_motif: Any) -> str:
    """Map the *fixed internal candidate pool* to an operational family.

    This never parses seller wording or source prompts.  It only labels the
    finite, curated scene strings already emitted by ``complete_script_v3``.
    """

    value = _text(scene_motif).lower()
    mappings = (
        ("OFFICE_WORKBREAK", ("办公室", "写字楼", "电梯", "办公")),
        ("CAR_TRANSIT", ("车道", "等车", "车内", "搭车")),
        # Exit corridors and public walkways behave like outing/transit
        # spaces, even when they belong to a bookstore, mall, or exhibition.
        # Check this before the broad venue family so strong STREET_OUTING
        # evidence can match the existing curated candidate pool.
        ("STREET_OUTING", ("街", "户外", "出口", "连廊", "走廊", "门廊")),
        ("CAFE_DINING", ("咖啡", "餐桌", "酒店", "书店", "展览", "商场")),
        ("MIRROR_FITTING", ("镜",)),
        ("VANITY_TRYON", ("梳妆",)),
        ("UNBOXING", ("包装", "开箱", "到货")),
        ("STATIC_DISPLAY", ("挂架", "衣架", "收纳架", "工作台", "台面", "置物架")),
        ("PLAIN_DISPLAY", ("单色", "浅色墙", "白墙", "金属墙")),
        ("HOME_ROUTINE", ("公寓", "客厅", "玄关", "窗边", "门口")),
    )
    for family, tokens in mappings:
        if any(token in value for token in tokens):
            return family
    return "GENERIC_INDOOR"


def _direction_provenance(direction: Mapping[str, Any]) -> Dict[str, Any]:
    contract = direction.get("structure_contract")
    contract = contract if isinstance(contract, Mapping) else {}
    provenance = contract.get("provenance")
    provenance = provenance if isinstance(provenance, Mapping) else {}
    return {
        "direction_assignment_id": _text(
            direction.get("direction_assignment_id") or provenance.get("direction_assignment_id")
        ),
        "source_run_id": _text(direction.get("source_run_id") or provenance.get("source_run_id")),
        "source_kind": _text(direction.get("source_kind") or provenance.get("source_kind")),
        "structure_cluster": _int(direction.get("cluster_id", provenance.get("cluster_id"))),
        "macro_family_key": _text(direction.get("macro_family_key") or contract.get("macro_family_key")),
    }


def _unavailable_context(direction: Mapping[str, Any], reason: str) -> Dict[str, Any]:
    provenance = _direction_provenance(direction)
    return {
        "status": "UNAVAILABLE",
        "reason": reason,
        "scene_run_id": "",
        "source_run_id": provenance["source_run_id"],
        "structure_cluster": provenance["structure_cluster"],
        "families": {},
    }


def _connect(database_url: str):
    try:
        import pymysql
    except ImportError as exc:  # pragma: no cover - installation boundary
        raise RuntimeError("场景参考读取需要 PyMySQL") from exc
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
        read_timeout=30,
    )


def _routing_bonus(*, support_count: int, lift: float, routing_status: str) -> int:
    """Positive-only, capped preference so generic high-volume scenes do not win.

    One or two examples are provenance only.  A scene receives a bonus only
    after it has at least three examples *and* appears more often with the
    selected structure than it does globally.
    """

    if routing_status != "ACTIVE" or support_count < 3 or lift <= 1.0:
        return 0
    support_floor = 4 if support_count < 5 else 8
    lift_extra = min(4, max(0, round(math.log(lift, 2) * 2)))
    return min(12, support_floor + lift_extra)


def _beat_key(value: Any) -> str:
    parsed = _list(value)
    if parsed:
        return ">".join(_text(item).upper() for item in parsed if _text(item))
    return _text(value).replace(" ", "").upper()


def _scene_layout_for_motif(scene_motif: Any, presentation: Any) -> Dict[str, str]:
    """Turn an already-selected motif into a phone-recordable micro-space.

    These are deliberately *creative execution defaults*, not facts claimed
    about a source video.  The scene corpus supplies the family/prototype;
    this small map prevents the model from turning it into a hotel lobby or a
    blank studio while keeping the existing candidate pool authoritative.
    """

    motif = _text(scene_motif)
    lowered = motif.lower()
    if any(token in motif for token in ("玄关", "门口", "快递柜")):
        return {
            "subspace": "入口一侧的窄墙面与鞋柜附近，不延伸到其他房间",
            "phone_placement": "手机放在鞋柜或矮柜边，保持一个固定日常视角",
            "subject_position": "人物在手机前的自然交谈范围内，完整上身或全身自然进入画面",
            "background_depth": "前景保留少量入口物件，背景只留门、墙面或收纳边缘",
        }
    if any(token in motif for token in ("办公室", "写字楼", "电梯", "办公")):
        return {
            "subspace": "靠墙的衣帽区、休息角或电梯厅一侧，不占用正式办公区",
            "phone_placement": "手机放在矮柜、置物台或靠墙支撑处，保持普通同事视角",
            "subject_position": "人物在墙面前或入口旁自然停留，不做走秀式来回",
            "background_depth": "前景保留包或文件等一件日常物品，背景保留局部收纳或动线",
        }
    if any(token in motif for token in ("咖啡", "书店", "商场", "展览", "酒店")):
        return {
            "subspace": "靠窗座位、过道边或入口一侧的普通停留位置，不布置专门拍摄角",
            "phone_placement": "手机靠在桌边、窗台或随身包旁，维持随手记录的高度",
            "subject_position": "人物在手机前自然停留，以正常交谈距离为主，不摆拍走秀",
            "background_depth": "保留局部桌面、座位或过道纵深，避免整面精致装潢成为主体",
        }
    if any(token in motif for token in ("客厅", "公寓", "窗边", "收纳架", "衣帽")):
        return {
            "subspace": "窗边、边柜或收纳架附近的一小段真实居住区域",
            "phone_placement": "手机放在边柜、书架或稳定桌面上，使用固定日常高度",
            "subject_position": "人物在窗边与手机同一小片区域内自然站立或短暂移动",
            "background_depth": "前景留少量家具边缘，背景保留墙面、窗帘或收纳的一部分",
        }
    return {
        "subspace": "日常空间靠墙的一小段可见区域，不扩展为布景",
        "phone_placement": "手机放在稳定平面或随手支撑处，保持固定手机视角",
        "subject_position": "人物与手机保持自然交谈距离，不做摄影棚式走位",
        "background_depth": "保留近处一件生活物品和远处普通环境，避免空白影棚",
    }


def _global_family_records(
    matrix_rows: Sequence[Mapping[str, Any]],
    *,
    clusters: Mapping[str, Any],
    families_policy: Mapping[str, Any],
    scene_run_id: str,
    video_observations: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Build scene-family prototype fallbacks without structure affinity."""

    grouped: Dict[str, List[Mapping[str, Any]]] = {}
    for row in matrix_rows:
        family = _text(clusters.get(str(_int(row.get("scene_cluster"))))) or "UNKNOWN_GENERIC"
        grouped.setdefault(family, []).append(row)
    result: Dict[str, Dict[str, Any]] = {}
    for family, members in grouped.items():
        ranked = sorted(members, key=lambda row: (-_int(row.get("count")), _int(row.get("scene_cluster"))))
        primary = ranked[0]
        representative_ids = _unique_texts(
            (
                video_id
                for row in ranked
                for video_id in _list(row.get("sample_video_ids"))
            ),
            limit=3,
        )
        family_policy = families_policy.get(family, {}) if isinstance(families_policy, Mapping) else {}
        result[family] = {
            "scene_family_key": family,
            "scene_run_id": scene_run_id,
            "routing_status": "SOFT_ONLY",
            "support_level": "GLOBAL_PROTOTYPE",
            "support_count": sum(max(0, _int(row.get("count"))) for row in members),
            "matrix_bonus": 0,
            "supporting_scene_cluster_ids": [_int(row.get("scene_cluster")) for row in ranked],
            "primary_scene_cluster_id": _int(primary.get("scene_cluster")),
            "prototype_name": _text(primary.get("scene_name")),
            "dominant_location": _text(primary.get("dominant_location")),
            "dominant_ambience": _text(primary.get("dominant_ambience")),
            "top_props": _list(primary.get("top_props")),
            "realism_anchor_pool": _list(primary.get("realism_anchor_pool")),
            "lighting_distribution": _mapping(primary.get("lighting_distribution")),
            "representative_video_ids": representative_ids,
            "representative_scene_observations": [
                dict(video_observations[video_id])
                for video_id in representative_ids
                if isinstance(video_observations.get(video_id), Mapping)
            ],
            "approved_realism_anchors": list(family_policy.get("approved_realism_anchors") or [])[:2],
        }
    return result


def _curated_scene_execution_card(
    family: str, *, scene_motif: str, presentation: str
) -> Dict[str, Any]:
    return {
        "schema_version": "scene-execution-card-v1",
        "status": "AVAILABLE",
        "source_quality": "CURATED_MOTIF_FALLBACK",
        "scene_family_key": family,
        "prototype_name": "",
        "space": {
            "location": _text(scene_motif) or "普通日常室内的一小段可见区域",
            **_scene_layout_for_motif(scene_motif, presentation),
        },
        "background_anchors": [],
        "lived_in_trace": "保留一件自然出现的随身物品或使用痕迹",
        "lighting": "现场已有自然光或普通室内光",
        "avoid_overdesign": "保留普通手机记录感；不添加商业布光、品牌陈列、花束、样板间式整洁或过度虚化。",
        "provenance": {
            "scene_run_id": "",
            "primary_scene_cluster_id": 0,
            "support_level": "CURATED_FALLBACK",
            "support_count": 0,
            "target_observation_used": False,
        },
    }


def _prototype_anchors(record: Mapping[str, Any], family_policy: Mapping[str, Any]) -> List[str]:
    """Select at most two non-product background cues from a scene prototype."""

    rejected = ("首饰", "耳环", "发夹", "发饰", "玩偶", "商品", "服装", "衣服", "鞋")
    values: List[Any] = []
    for item in _list(record.get("top_props")):
        if isinstance(item, Mapping):
            values.append(item.get("name") or item.get("value") or item.get("prop"))
        else:
            values.append(item)
    values.extend(family_policy.get("approved_realism_anchors") or [])
    return _unique_texts(
        (value for value in values if not any(token in _text(value) for token in rejected)),
        limit=2,
    )


def _prototype_lighting(record: Mapping[str, Any]) -> str:
    distribution = _mapping(record.get("lighting_distribution"))
    ranked = sorted(
        ((_text(key), _int(value)) for key, value in distribution.items()),
        key=lambda item: (-item[1], item[0]),
    )
    for lighting, _count in ranked:
        if lighting and lighting.upper() not in {"UNKNOWN", "UNAVAILABLE", "OTHER"}:
            return lighting
    return "现场已有自然光与普通室内光的真实混合"


def _select_scene_observation(
    record: Mapping[str, Any], *, country: Any, category: Any
) -> Dict[str, Any]:
    """Prefer one observed tag in the target market/category, when present.

    The prototype remains the fallback because historical samples can be
    cross-category.  This is a soft precision improvement, never a gate.
    """

    observations = [
        item for item in record.get("representative_scene_observations") or []
        if isinstance(item, Mapping)
    ]
    if not observations:
        return {}
    ranked = sorted(
        observations,
        key=lambda item: (
            -int(_same_country(item.get("country"), country)),
            -int(_category_matches(item, category)),
            -_float(item.get("confidence")),
        ),
    )
    best = dict(ranked[0])
    if _same_country(best.get("country"), country) and _category_matches(best, category):
        return best
    return {}


def _build_scene_execution_card(
    record: Mapping[str, Any],
    family_policy: Mapping[str, Any],
    *,
    scene_motif: Any,
    presentation: Any,
    country: Any = "",
    category: Any = "",
) -> Dict[str, Any]:
    """A small advisory card consumed by the visual-script and video prompt.

    It is intentionally available for a weak/soft matrix match too: it never
    changes scoring, authorizes a claim, or blocks production.  Evidence level
    remains explicit so later routing can prefer stronger source observations.
    """

    prototype_name = _text(record.get("prototype_name"))
    observation = _select_scene_observation(record, country=country, category=category)
    raw_traces = _list(observation.get("realism_anchors")) or _list(record.get("realism_anchor_pool"))
    trace_values = []
    for item in raw_traces:
        if isinstance(item, Mapping):
            trace_values.append(item.get("text") or item.get("anchor") or item.get("value"))
        else:
            trace_values.append(item)
    trace = _unique_texts(trace_values, limit=1)
    return {
        "schema_version": "scene-execution-card-v1",
        "status": "AVAILABLE",
        "source_quality": (
            "TARGET_MARKET_CATEGORY_SCENE_TAG"
            if observation
            else "SCENE_FAMILY_PROTOTYPE"
            if _text(record.get("support_level")) == "GLOBAL_PROTOTYPE"
            else "STRUCTURE_SCENE_PROTOTYPE"
        ),
        "scene_family_key": _text(record.get("scene_family_key")),
        "prototype_name": prototype_name,
        "space": {
            "location": _text(scene_motif) or _text(record.get("dominant_location")) or prototype_name,
            **_scene_layout_for_motif(scene_motif, presentation),
        },
        "background_anchors": _unique_texts(
            [*_list(observation.get("props_normalized")), *_prototype_anchors(record, family_policy)],
            limit=2,
        ),
        "lived_in_trace": trace[0] if trace else "保留一件自然出现的随身物品或使用痕迹",
        "lighting": _text(observation.get("lighting"))
        if _text(observation.get("lighting")).upper() not in {"", "UNKNOWN", "UNAVAILABLE", "OTHER"}
        else _prototype_lighting(record),
        "avoid_overdesign": "保留普通手机记录感；不添加商业布光、品牌陈列、花束、样板间式整洁或过度虚化。",
        "provenance": {
            "scene_run_id": _text(record.get("scene_run_id")),
            "primary_scene_cluster_id": _int(record.get("primary_scene_cluster_id")),
            "support_level": _text(record.get("support_level")),
            "support_count": _int(record.get("support_count")),
            "target_observation_used": bool(observation),
        },
    }


def build_contexts_from_matrix_rows(
    directions: Sequence[Mapping[str, Any]],
    matrix_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
    video_observations: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Pure aggregation used by both RDS loading and unit tests."""

    clusters = policy.get("clusters") if isinstance(policy.get("clusters"), Mapping) else {}
    families_policy = policy.get("families") if isinstance(policy.get("families"), Mapping) else {}
    video_observations = video_observations or {}
    scene_run_id = _text(policy.get("scene_run_id"))
    global_families = _global_family_records(
        matrix_rows,
        clusters=clusters,
        families_policy=families_policy,
        scene_run_id=scene_run_id,
        video_observations=video_observations,
    )
    grouped: Dict[tuple, List[Mapping[str, Any]]] = {}
    run_totals: Dict[str, int] = {}
    run_family_totals: Dict[tuple, int] = {}
    for row in matrix_rows:
        structure_run = _text(row.get("structure_run"))
        structure_cluster = _int(row.get("structure_cluster"))
        scene_cluster = _int(row.get("scene_cluster"))
        count = max(0, _int(row.get("count")))
        family = _text(clusters.get(str(scene_cluster))) or "UNKNOWN_GENERIC"
        grouped.setdefault((structure_run, structure_cluster), []).append(row)
        run_totals[structure_run] = run_totals.get(structure_run, 0) + count
        run_family_totals[(structure_run, family)] = (
            run_family_totals.get((structure_run, family), 0) + count
        )

    contexts: Dict[str, Dict[str, Any]] = {}
    family_count = max(1, len(families_policy))
    for direction in directions:
        provenance = _direction_provenance(direction)
        direction_id = provenance["direction_assignment_id"]
        if not direction_id:
            continue
        source_run = provenance["source_run_id"]
        structure_cluster = provenance["structure_cluster"]
        rows = grouped.get((source_run, structure_cluster), [])
        fallback_mode = ""
        effective_source_run = source_run
        # v2_final directions are intentionally derived/read-only and do not
        # yet own their own structure×scene matrix.  A same-beat fallback is
        # safe for scene texture (not for scoring): it never changes the
        # selected structure and is marked as lower-quality provenance.
        macro_key = provenance["macro_family_key"]
        if not rows and macro_key:
            fallback_rows = [
                row for row in matrix_rows
                if _beat_key(row.get("dominant_beat_sequence")) == macro_key
            ]
            if fallback_rows:
                rows = fallback_rows
                effective_source_run = _text(rows[0].get("structure_run"))
                fallback_mode = "MACRO_FAMILY_FALLBACK"
        if not rows:
            contexts[direction_id] = _unavailable_context(direction, "NO_STRUCTURE_SCENE_MATRIX")
            contexts[direction_id]["global_families"] = global_families
            continue
        structure_total = sum(max(0, _int(row.get("count"))) for row in rows)
        by_family: Dict[str, List[Mapping[str, Any]]] = {}
        for row in rows:
            family = _text(clusters.get(str(_int(row.get("scene_cluster"))))) or "UNKNOWN_GENERIC"
            by_family.setdefault(family, []).append(row)
        family_contexts: Dict[str, Dict[str, Any]] = {}
        for family, members in by_family.items():
            member_count = sum(max(0, _int(row.get("count"))) for row in members)
            global_count = run_family_totals.get((effective_source_run, family), 0)
            global_total = run_totals.get(effective_source_run, 0)
            conditional = (member_count + 1.0) / (structure_total + family_count)
            baseline = (global_count + 1.0) / (global_total + family_count)
            lift = conditional / baseline if baseline else 1.0
            family_policy = families_policy.get(family, {})
            routing_status = _text(family_policy.get("routing_status")) or "SOFT_ONLY"
            ranked = sorted(members, key=lambda row: (-_int(row.get("count")), _int(row.get("scene_cluster"))))
            representative_ids: List[str] = []
            for item in ranked:
                raw_ids = item.get("sample_video_ids") or []
                if isinstance(raw_ids, str):
                    try:
                        raw_ids = json.loads(raw_ids)
                    except (TypeError, ValueError, json.JSONDecodeError):
                        raw_ids = []
                for video_id in raw_ids if isinstance(raw_ids, list) else []:
                    text = _text(video_id)
                    if text and text not in representative_ids:
                        representative_ids.append(text)
                    if len(representative_ids) >= 3:
                        break
                if len(representative_ids) >= 3:
                    break
            primary = ranked[0]
            support_level = "STRONG" if member_count >= 5 else "MEDIUM" if member_count >= 3 else "WEAK"
            family_contexts[family] = {
                "scene_family_key": family,
                "scene_run_id": scene_run_id,
                "routing_status": routing_status,
                "support_level": support_level,
                "support_count": member_count,
                "structure_total": structure_total,
                "conditional_share": round(member_count / structure_total, 4) if structure_total else 0.0,
                "lift": round(lift, 4),
                "matrix_bonus": 0 if fallback_mode else _routing_bonus(
                    support_count=member_count,
                    lift=lift,
                    routing_status=routing_status,
                ),
                "supporting_scene_cluster_ids": [
                    _int(item.get("scene_cluster")) for item in ranked
                ],
                "primary_scene_cluster_id": _int(primary.get("scene_cluster")),
                "prototype_name": _text(primary.get("scene_name")),
                "dominant_location": _text(primary.get("dominant_location")),
                "dominant_ambience": _text(primary.get("dominant_ambience")),
                "top_props": _list(primary.get("top_props")),
                "realism_anchor_pool": _list(primary.get("realism_anchor_pool")),
                "lighting_distribution": _mapping(primary.get("lighting_distribution")),
                "representative_video_ids": representative_ids,
                "representative_scene_observations": [
                    dict(video_observations[video_id])
                    for video_id in representative_ids
                    if isinstance(video_observations.get(video_id), Mapping)
                ],
                "approved_realism_anchors": list(
                    family_policy.get("approved_realism_anchors") or []
                )[:2],
                "affinity_tags": list(family_policy.get("affinity_tags") or []),
            }
        contexts[direction_id] = {
            "status": "AVAILABLE",
            "reason": "READ_ONLY_SOFT_REFERENCE",
            "scene_run_id": scene_run_id,
            "policy_version": _text(policy.get("policy_version")) or SCENE_REFERENCE_POLICY_VERSION,
            "source_run_id": source_run,
            "matrix_source_run_id": effective_source_run,
            "structure_cluster": structure_cluster,
            "fallback_mode": fallback_mode,
            "families": family_contexts,
            "global_families": global_families,
        }
    return contexts


def load_scene_reference_contexts(
    directions: Sequence[Mapping[str, Any]],
    *,
    database_url: str = "",
    policy_path: str = "",
) -> Dict[str, Dict[str, Any]]:
    """Best-effort RDS read. Any failure leaves normal planning untouched."""

    if not scene_reference_enabled():
        return {}
    policy = load_scene_policy(policy_path)
    if not policy:
        return {
            _direction_provenance(direction)["direction_assignment_id"]: _unavailable_context(direction, "POLICY_UNAVAILABLE")
            for direction in directions
            if _direction_provenance(direction)["direction_assignment_id"]
        }
    database_url = _text(database_url or os.environ.get("STRUCTURE_ROUTER_DATABASE_URL") or os.environ.get("LIKEU_AI_DATABASE_URL"))
    if not database_url:
        return {
            _direction_provenance(direction)["direction_assignment_id"]: _unavailable_context(direction, "DATABASE_UNAVAILABLE")
            for direction in directions
            if _direction_provenance(direction)["direction_assignment_id"]
        }
    try:
        with _connect(database_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT m.structure_run, m.structure_cluster, m.scene_cluster, m.count,
                           m.sample_video_ids, p.scene_name, p.dominant_location,
                           p.dominant_ambience, p.top_props, p.realism_anchor_pool,
                           p.lighting_distribution, sp.dominant_beat_sequence
                    FROM sd_structure_scene_matrix m
                    LEFT JOIN sd_scene_prototype p
                      ON p.run_id=m.scene_run AND p.cluster_id=m.scene_cluster
                    LEFT JOIN sd_cluster_prototype sp
                      ON sp.run_id=m.structure_run AND sp.cluster_id=m.structure_cluster
                    WHERE m.scene_run=%s
                    """,
                    (_text(policy.get("scene_run_id")) or "scene_v2",),
                )
                rows = cursor.fetchall()
                sample_ids = _unique_texts(
                    (
                        video_id
                        for row in rows
                        for video_id in _list(row.get("sample_video_ids"))
                    ),
                    limit=5000,
                )
                observations: Dict[str, Dict[str, Any]] = {}
                if sample_ids:
                    placeholders = ",".join(["%s"] * len(sample_ids))
                    cursor.execute(
                        f"""
                        SELECT a.video_id, a.country, a.cat1, a.cat2,
                               t.location, t.lighting, t.ambience,
                               t.props_normalized, t.realism_anchors, t.confidence
                        FROM sd_evaluation_asset a
                        INNER JOIN sd_scene_tag t ON t.video_id=a.video_id
                        WHERE a.video_id IN ({placeholders})
                        """,
                        sample_ids,
                    )
                    observations = {
                        _text(row.get("video_id")): dict(row)
                        for row in cursor.fetchall()
                        if _text(row.get("video_id"))
                    }
        return build_contexts_from_matrix_rows(directions, rows, policy, observations)
    except Exception:
        return {
            _direction_provenance(direction)["direction_assignment_id"]: _unavailable_context(direction, "RDS_READ_FAILED")
            for direction in directions
            if _direction_provenance(direction)["direction_assignment_id"]
        }


def scene_reference_contract_for_family(
    context: Optional[Mapping[str, Any]],
    scene_family_key: str,
    *,
    scene_motif: str = "",
    presentation: str = "",
    country: str = "",
    category: str = "",
) -> Dict[str, Any]:
    """Return a compact, frozen contract; unavailable data never becomes a gate."""

    context = context if isinstance(context, Mapping) else {}
    family = _text(scene_family_key) or "GENERIC_INDOOR"
    record = context.get("families", {}).get(family) if isinstance(context.get("families"), Mapping) else None
    fallback_level = ""
    if not isinstance(record, Mapping):
        record = (
            context.get("global_families", {}).get(family)
            if isinstance(context.get("global_families"), Mapping)
            else None
        )
        if isinstance(record, Mapping):
            fallback_level = "SCENE_FAMILY_PROTOTYPE_FALLBACK"
    if not isinstance(record, Mapping):
        return {
            "schema_version": SCENE_REFERENCE_SCHEMA_VERSION,
            "status": "SOFT_ONLY",
            "selection_mode": "CURATED_MOTIF_FALLBACK",
            "scene_family_key": family,
            "reason": _text(context.get("reason")) or "NO_FAMILY_MATCH",
            "approved_realism_anchors": [],
            "matrix_bonus": 0,
            "scene_execution_card": _curated_scene_execution_card(
                family, scene_motif=scene_motif, presentation=presentation
            ),
        }
    status = _text(record.get("routing_status"))
    matrix_bonus = _int(record.get("matrix_bonus"))
    fallback_mode = _text(context.get("fallback_mode"))
    # An ACTIVE family is only a taxonomy permission.  The concrete
    # structure-family pair must also pass the support/lift gate before any
    # realism anchor reaches generation.  This keeps one-off PROMPT_ONLY
    # associations as provenance instead of creative guidance.
    eligible_for_prompt = status == "ACTIVE" and matrix_bonus > 0
    contract = {
        "schema_version": SCENE_REFERENCE_SCHEMA_VERSION,
        "status": "AVAILABLE" if eligible_for_prompt else "SOFT_ONLY",
        "selection_mode": fallback_level or fallback_mode or "EXACT_STRUCTURE_SCENE",
        "policy_version": _text(context.get("policy_version")) or SCENE_REFERENCE_POLICY_VERSION,
        "scene_run_id": _text(context.get("scene_run_id")),
        "source_run_id": _text(context.get("source_run_id")),
        "matrix_source_run_id": _text(context.get("matrix_source_run_id")),
        "structure_cluster": _int(context.get("structure_cluster")),
        "scene_family_key": family,
        "routing_status": status or "SOFT_ONLY",
        "support_level": _text(record.get("support_level")),
        "support_count": _int(record.get("support_count")),
        "conditional_share": _float(record.get("conditional_share")),
        "lift": _float(record.get("lift")),
        "matrix_bonus": 0 if fallback_level else matrix_bonus,
        "supporting_scene_cluster_ids": list(record.get("supporting_scene_cluster_ids") or []),
        "primary_scene_cluster_id": _int(record.get("primary_scene_cluster_id")),
        "prototype_name": _text(record.get("prototype_name")),
        "representative_video_ids": list(record.get("representative_video_ids") or [])[:3],
        "approved_realism_anchors": list(record.get("approved_realism_anchors") or [])[:2]
        if eligible_for_prompt
        else [],
    }
    # The old compact realism anchors remain gated by support/lift.  The
    # execution card is different: it is advisory scene-layout context from a
    # named prototype and is safe even when the pair is only SOFT_ONLY.
    family_policy = {}
    # ``approved_realism_anchors`` has already been copied into the record;
    # preserve it as the policy source for the card without exposing matrix
    # data to model prompts.
    if isinstance(record.get("approved_realism_anchors"), list):
        family_policy["approved_realism_anchors"] = record.get("approved_realism_anchors")
    contract["scene_execution_card"] = _build_scene_execution_card(
        record,
        family_policy,
        scene_motif=scene_motif,
        presentation=presentation,
        country=country,
        category=category,
    )
    return contract
