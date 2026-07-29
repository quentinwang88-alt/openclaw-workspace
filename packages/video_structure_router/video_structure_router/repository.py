"""Read-only repository for structure discovery tables.

This module deliberately exposes no mutation method for ``sd_*`` tables.
"""

from __future__ import annotations

import json
import hashlib
import os
import statistics
from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import unquote, urlparse

from .models import StructureCandidate


def _json_value(value: Any, default: Any) -> Any:
    if value is None or value == "":
        return default
    if isinstance(value, (list, dict)):
        return value
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return default
    return parsed


def _distribution_values(value: Any) -> List[str]:
    rows = _json_value(value, [])
    if not isinstance(rows, list):
        return []
    result: List[str] = []
    for row in rows:
        if isinstance(row, dict):
            text = str(row.get("value") or "").strip()
        else:
            text = str(row or "").strip()
        if text and text not in result:
            result.append(text)
    return result


def _dominant_distribution_value(value: Any) -> str:
    values = _distribution_values(value)
    return values[0] if values else ""


def _mode(values: Iterable[str]) -> str:
    cleaned = [str(value or "").strip() for value in values if str(value or "").strip()]
    if not cleaned:
        return ""
    counts = Counter(cleaned)
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]


def _flatten_json_lists(values: Iterable[Any]) -> List[str]:
    result: List[str] = []
    for value in values:
        parsed = _json_value(value, [])
        if not isinstance(parsed, list):
            continue
        for item in parsed:
            text = str(item or "").strip()
            if text and text not in result:
                result.append(text)
    return result


def evidence_tier_for(
    cluster_status: str,
    profile_types: Iterable[str],
    independence_levels: Iterable[str],
) -> str:
    """Versioned pure mapping from discovery evidence to routing evidence."""
    status = str(cluster_status or "").strip().upper()
    types = {str(item or "").strip().upper() for item in profile_types}
    independence = {str(item or "").strip().upper() for item in independence_levels}
    if "HUMAN_GOLD" in types or status == "HUMAN_VALIDATED":
        return "HUMAN_VALIDATED"
    if status == "STABLE" and "VIDEO_INDEPENDENT" in types and "FULL" in independence:
        return "STABLE"
    if "VIDEO_INDEPENDENT" in types and "FULL" in independence:
        return "VIDEO_SUPPORTED"
    if "VIDEO_INDEPENDENT" in types:
        return "VIDEO_SUPPORTED_PARTIAL"
    return "BOOTSTRAP"


class RDSStructureRepository:
    """Read the current structure assets from the configured RDS."""

    def __init__(self, database_url: str = "", video_run_id: str = ""):
        self.database_url = (
            database_url
            or os.environ.get("STRUCTURE_ROUTER_DATABASE_URL")
            or os.environ.get("LIKEU_AI_DATABASE_URL")
            or ""
        ).strip()
        self.video_run_id = (
            video_run_id
            or os.environ.get("STRUCTURE_ROUTER_VIDEO_RUN_ID")
            or "v2_final"
        ).strip()

    def _connect(self):
        if not self.database_url:
            raise RuntimeError("缺少 STRUCTURE_ROUTER_DATABASE_URL / LIKEU_AI_DATABASE_URL")
        try:
            import pymysql
        except ImportError as exc:
            raise RuntimeError("RDS 结构路由需要 PyMySQL") from exc
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

    def load_candidates(self) -> List[StructureCandidate]:
        with self._connect() as conn:
            prompt_candidates = self._load_prototype_candidates(conn)
            video_candidates = self._load_video_candidates(conn)
        return prompt_candidates + video_candidates

    def _load_prototype_candidates(self, conn: Any) -> List[StructureCandidate]:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM sd_cluster_prototype
                ORDER BY run_id, cluster_id, prototype_id
                """
            )
            rows = cursor.fetchall()

        candidates: List[StructureCandidate] = []
        for row in rows:
            beat_sequence = [str(item) for item in _json_value(row.get("dominant_beat_sequence"), [])]
            candidates.append(
                StructureCandidate(
                    candidate_key=f"prototype:{row['prototype_id']}",
                    source_kind="CLUSTER_PROTOTYPE",
                    source_run_id=str(row.get("run_id") or ""),
                    cluster_id=int(row.get("cluster_id") or 0),
                    cluster_version=str(row.get("cluster_version") or "v1"),
                    prototype_id=str(row.get("prototype_id") or ""),
                    cluster_status=str(row.get("cluster_status") or "BOOTSTRAP_CANDIDATE"),
                    evidence_tier=evidence_tier_for(
                        str(row.get("cluster_status") or "BOOTSTRAP_CANDIDATE"),
                        ["PROMPT_ONLY"],
                        ["NONE"],
                    ),
                    macro_structure_name=str(row.get("macro_structure_name") or "候选结构"),
                    structure_description=str(row.get("structure_description") or ""),
                    beat_sequence=beat_sequence,
                    required_beats=[str(item) for item in _json_value(row.get("required_beats"), beat_sequence)],
                    optional_beats=[str(item) for item in _json_value(row.get("optional_beats"), [])],
                    content_carrier=_dominant_distribution_value(row.get("carrier_distribution")),
                    continuity_mode=_dominant_distribution_value(row.get("continuity_distribution")),
                    cut_density=_dominant_distribution_value(row.get("cut_density_distribution")),
                    visual_hook_type=_dominant_distribution_value(row.get("hook_distribution")),
                    proof_mechanisms=[],
                    ending_pattern=_dominant_distribution_value(row.get("ending_distribution")),
                    # PROMPT_ONLY 中出现的镜头数/时间戳不具备视频实测权威性。
                    shot_count_min=None,
                    shot_count_max=None,
                    shot_count_median=None,
                    duration_median=None,
                    member_count=int(row.get("member_count") or 0),
                    distinct_videos=int(row.get("distinct_videos") or 0),
                    cohesion=float(row.get("cohesion") or 0.0),
                    extraction_confidence=0.0,
                    categories=_distribution_values(row.get("category_distribution")),
                    countries=_distribution_values(row.get("country_distribution")),
                    variation_axes=[str(item) for item in _json_value(row.get("variation_axes"), [])],
                    representative_cases=[item for item in _json_value(row.get("representative_cases"), []) if isinstance(item, dict)],
                    extractor_versions=[],
                    feature_schema_versions=[],
                    compatibility_matrix_versions=[],
                    profile_types=["PROMPT_ONLY"],
                    independence_levels=["NONE"],
                    metadata={
                        "dominant_seq_pct": row.get("dominant_seq_pct"),
                        "carrier_distribution": _json_value(row.get("carrier_distribution"), []),
                        "continuity_distribution": _json_value(row.get("continuity_distribution"), []),
                        "cut_density_distribution": _json_value(row.get("cut_density_distribution"), []),
                        "hook_distribution": _json_value(row.get("hook_distribution"), []),
                    },
                )
            )
        return candidates

    def _load_video_candidates(self, conn: Any) -> List[StructureCandidate]:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    a.cluster_run_id,
                    a.cluster_id,
                    a.assignment_confidence,
                    p.*,
                    e.country AS evaluation_country,
                    e.cat1 AS evaluation_cat1,
                    e.cat2 AS evaluation_cat2
                FROM sd_cluster_assignment a
                JOIN sd_structure_profile p ON p.profile_id = a.profile_id
                LEFT JOIN sd_evaluation_asset e ON e.asset_id = p.asset_id
                WHERE a.cluster_run_id = %s
                  AND COALESCE(a.is_noise, 0) = 0
                  AND p.profile_type = 'VIDEO_INDEPENDENT'
                ORDER BY a.cluster_id, p.profile_id
                """,
                (self.video_run_id,),
            )
            rows = cursor.fetchall()

        groups: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        for row in rows:
            groups[int(row.get("cluster_id") or 0)].append(row)

        candidates: List[StructureCandidate] = []
        for cluster_id, members in sorted(groups.items()):
            shot_counts = [int(row["measured_shot_count"]) for row in members if row.get("measured_shot_count") is not None]
            durations = [float(row["duration_sec"]) for row in members if row.get("duration_sec") is not None]
            beat_sequences = [tuple(_json_value(row.get("coarse_beat_sequence"), [])) for row in members]
            beat_sequences = [seq for seq in beat_sequences if seq]
            dominant_seq = list(Counter(beat_sequences).most_common(1)[0][0]) if beat_sequences else []
            confidence_values = [float(row.get("extraction_confidence") or 0.0) for row in members]
            categories = []
            countries = []
            for row in members:
                for value in (row.get("source_cat1"), row.get("source_cat2"), row.get("evaluation_cat1"), row.get("evaluation_cat2")):
                    text = str(value or "").strip()
                    if text and text not in categories:
                        categories.append(text)
                for value in (row.get("source_country"), row.get("evaluation_country")):
                    text = str(value or "").strip()
                    if text and text not in countries:
                        countries.append(text)
            carrier = _mode(row.get("content_carrier") for row in members)
            continuity = _mode(row.get("continuity_mode") for row in members)
            cut_density = _mode(row.get("cut_density") for row in members)
            hook = _mode(row.get("visual_hook_type") for row in members)
            ending = _mode(row.get("ending_pattern") for row in members)
            proof = _flatten_json_lists(row.get("proof_mechanisms") for row in members)
            version_material = "|".join(
                sorted(
                    {
                        ":".join(
                            (
                                str(row.get("extractor_version") or ""),
                                str(row.get("feature_schema_version") or ""),
                                str(row.get("compatibility_matrix_version") or ""),
                            )
                        )
                        for row in members
                    }
                )
            )
            derived_version = hashlib.sha256(version_material.encode("utf-8")).hexdigest()[:10]
            candidates.append(
                StructureCandidate(
                    candidate_key=f"derived-video:{self.video_run_id}:{cluster_id}",
                    source_kind="DERIVED_VIDEO_CLUSTER",
                    source_run_id=self.video_run_id,
                    cluster_id=cluster_id,
                    cluster_version=f"derived-{derived_version}",
                    prototype_id="",
                    cluster_status="VIDEO_CANDIDATE",
                    evidence_tier=evidence_tier_for(
                        "VIDEO_CANDIDATE",
                        ["VIDEO_INDEPENDENT"],
                        [str(row.get("independence_level") or "") for row in members],
                    ),
                    macro_structure_name=f"视频实测结构簇 {cluster_id}",
                    structure_description=(
                        f"{'>'.join(dominant_seq) or '未知叙事序列'}；"
                        f"{carrier or '未知承载'} / {continuity or '未知连续性'} / {cut_density or '未知切镜'}"
                    ),
                    beat_sequence=dominant_seq,
                    required_beats=dominant_seq,
                    optional_beats=[],
                    content_carrier=carrier,
                    continuity_mode=continuity,
                    cut_density=cut_density,
                    visual_hook_type=hook,
                    proof_mechanisms=proof,
                    ending_pattern=ending,
                    shot_count_min=min(shot_counts) if shot_counts else None,
                    shot_count_max=max(shot_counts) if shot_counts else None,
                    shot_count_median=float(statistics.median(shot_counts)) if shot_counts else None,
                    duration_median=float(statistics.median(durations)) if durations else None,
                    member_count=len(members),
                    distinct_videos=len({str(row.get("video_id") or "") for row in members}),
                    cohesion=0.0,
                    extraction_confidence=(sum(confidence_values) / len(confidence_values)) if confidence_values else 0.0,
                    categories=categories,
                    countries=countries,
                    variation_axes=[
                        f"承载方式={carrier or 'UNAVAILABLE'}",
                        f"连续性={continuity or 'UNAVAILABLE'}",
                        f"实测镜头中位数={statistics.median(shot_counts) if shot_counts else 'UNAVAILABLE'}",
                    ],
                    representative_cases=[
                        {
                            "video_id": str(row.get("video_id") or ""),
                            "profile_id": str(row.get("profile_id") or ""),
                            "shot_count": row.get("measured_shot_count"),
                        }
                        for row in sorted(
                            members,
                            key=lambda item: -float(item.get("extraction_confidence") or 0.0),
                        )[:3]
                    ],
                    extractor_versions=sorted({str(row.get("extractor_version") or "") for row in members if row.get("extractor_version")}),
                    feature_schema_versions=sorted({str(row.get("feature_schema_version") or "") for row in members if row.get("feature_schema_version")}),
                    compatibility_matrix_versions=sorted({str(row.get("compatibility_matrix_version") or "") for row in members if row.get("compatibility_matrix_version")}),
                    profile_types=["VIDEO_INDEPENDENT"],
                    independence_levels=sorted({str(row.get("independence_level") or "") for row in members if row.get("independence_level")}),
                    metadata={"derived_read_only": True},
                )
            )
        return candidates
