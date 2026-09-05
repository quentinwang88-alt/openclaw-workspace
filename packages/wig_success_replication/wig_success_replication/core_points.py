"""Explicit mother requirements and bounded, non-semantic omission checks."""
from __future__ import annotations

import re
from typing import Any


def mother_core_points(contract: Any) -> list[dict[str, str]]:
    if contract is None:
        return []
    if isinstance(contract, dict):
        value = contract
    else:
        value = contract.model_dump(mode="json")
    if value.get("execution_summary"):
        return [dict(point) for point in value["execution_summary"]["core_mechanisms"]]
    if value.get("core_points"):
        return [dict(point) for point in value["core_points"]]
    mechanism = value.get("core_mechanism") or {}
    keys = ("hook_engine", "reveal_engine", "proof_engine", "conversion_engine")
    return [{"point_id": f"legacy_{index}", "requirement": str(mechanism[key]),
             "evidence_source": "legacy_derived"}
            for index, key in enumerate(keys, 1) if mechanism.get(key)]


def _explicitly_removes_subject(text: str, subject: str) -> bool:
    # Match a complete, unqualified instruction, not a prefix of
    # “不出现人物参考图里的头发” or a valid Before-only staging instruction.
    boundary = r"[。.!！？?；;，,、\n]"
    pattern = (
        rf"(?:^|{boundary})\s*"
        r"(?:(?:全片|全程|画面中|整个视频|视频中)\s*)?"
        rf"(?:不出现|不展示|无|没有)(?:任何)?{re.escape(subject)}"
        rf"\s*(?=$|{boundary})"
    )
    return re.search(pattern, text) is not None


def obvious_omissions(full_prompt: str, contract: Any) -> list[str]:
    """Reject provable contradictions/omissions, never certify visual semantics.

    Keyword presence only means this narrow static check found no omission.
    It does not prove execution, similarity, identity or successful replication.
    """
    points = mother_core_points(contract)
    if not points:
        return []
    requirements = " ".join(point["requirement"] for point in points)
    issues = []
    if "假发" in requirements and _explicitly_removes_subject(full_prompt, "假发"):
        issues.append("mother_core_contradiction:wig_removed")
    if any(word in requirements for word in ("人物", "女生", "同一人")) and _explicitly_removes_subject(full_prompt, "人物"):
        issues.append("mother_core_contradiction:person_removed")
    # No action-word-presence gate: valid synonyms and mothers without a lens
    # transition must not acquire a paint/occlusion choreography by heuristic.
    # Mechanism fidelity is a compilation concern and optional visual review;
    # this function only catches the explicit subject contradictions above.
    return issues
