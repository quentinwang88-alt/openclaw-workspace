#!/usr/bin/env python3
"""Small JSON parsing helpers."""

from __future__ import annotations

import json
import re
from typing import Any


def parse_json_object(text: str) -> Any:
    raw = str(text or "").strip()
    if not raw:
        raise ValueError("empty JSON text")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    fenced = re.search(r"```(?:json)?\s*(.*?)```", raw, flags=re.DOTALL | re.IGNORECASE)
    if fenced:
        return json.loads(fenced.group(1).strip())

    start_candidates = [idx for idx in (raw.find("{"), raw.find("[")) if idx >= 0]
    if not start_candidates:
        raise ValueError("no JSON object or array found")
    start = min(start_candidates)
    end = max(raw.rfind("}"), raw.rfind("]"))
    if end <= start:
        raise ValueError("truncated JSON text")
    return json.loads(raw[start : end + 1])


def dumps_pretty(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2)
