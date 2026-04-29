#!/usr/bin/env python3
"""Field-name matching helpers."""

from __future__ import annotations

import re
from typing import Iterable, Optional, Sequence


def normalize_name(value: str) -> str:
    return re.sub(r"[\s_\-()（）【】\[\]]+", "", str(value or "").strip().lower())


def resolve_field_name(
    field_names: Sequence[str],
    requested_name: Optional[str] = None,
    fallback_candidates: Optional[Iterable[str]] = None,
) -> Optional[str]:
    if requested_name:
        direct_match = _match_name(field_names, requested_name)
        if direct_match:
            return direct_match

    for candidate in fallback_candidates or ():
        matched = _match_name(field_names, candidate)
        if matched:
            return matched

    if requested_name:
        target = normalize_name(requested_name)
        contains_matches = [
            field_name
            for field_name in field_names
            if target and (target in normalize_name(field_name) or normalize_name(field_name) in target)
        ]
        if len(contains_matches) == 1:
            return contains_matches[0]

    return None


def _match_name(field_names: Sequence[str], candidate: str) -> Optional[str]:
    normalized_candidate = normalize_name(candidate)
    if not normalized_candidate:
        return None
    for field_name in field_names:
        if normalize_name(field_name) == normalized_candidate:
            return field_name
    return None
