"""Resolve four-choice asset labels before a request or plan is frozen."""
from __future__ import annotations

import copy
from typing import Any, Mapping, Sequence

from domain.photo_contracts import LABEL_PLACEHOLDER, validate_copy


def resolve_photo_copy(copy_block: Mapping[str, Any], *, assets: Sequence[Mapping[str, Any]],
                       locale: str, extra_tokens: Mapping[str, str] = None,
                       expected_slide_count: int = 5) -> dict[str, Any]:
    labels = {}
    for letter in "abcd":
        matches = [asset for asset in assets if asset.get("role") == f"look_{letter}"]
        if len(matches) > 1:
            raise ValueError(f"ambiguous display_label role look_{letter}")
        raw = matches[0].get("display_label") if matches else None
        if isinstance(raw, Mapping):
            raw = raw.get(locale) or raw.get(locale.split("-")[0])
        label = raw.strip() if isinstance(raw, str) else ""
        labels[letter] = label or (f"ลุค {letter.upper()}" if locale.lower().startswith("th") else f"Look {letter.upper()}")

    tokens = {str(key): str(value) for key, value in dict(extra_tokens or {}).items() if str(key)}

    def replace(value):
        if isinstance(value, str):
            replaced = LABEL_PLACEHOLDER.sub(lambda match: labels[match.group(1)], value)
            for key, token_value in tokens.items():
                replaced = replaced.replace("{" + key + "}", token_value)
            return replaced
        if isinstance(value, Mapping):
            return {key: replace(item) for key, item in value.items()}
        if isinstance(value, list):
            return [replace(item) for item in value]
        return copy.deepcopy(value)

    resolved = replace(copy_block)
    errors = validate_copy(resolved, expected_slide_count=expected_slide_count)
    if errors:
        raise ValueError("; ".join(errors))
    return resolved


def fill_travel_copy_tokens(text: str, *, travel_contract: Mapping[str, Any],
                            variables: Mapping[str, Any]) -> str:
    """Fill audited template tokens ({destination}/{temperature}) from contract labels."""
    destination = str((travel_contract.get("destination_labels_th") or {}).get(
        str(variables.get("destination") or ""), ""))
    temperature = str((travel_contract.get("temperature_labels_th") or {}).get(
        str(variables.get("temperature_band") or ""), ""))
    filled = str(text or "")
    if "{destination}" in filled and not destination:
        raise ValueError("旅行文案模板缺少目的地泰语标签")
    if "{temperature}" in filled and not temperature:
        raise ValueError("旅行文案模板缺少温度泰语标签")
    return filled.replace("{destination}", destination).replace("{temperature}", temperature)
