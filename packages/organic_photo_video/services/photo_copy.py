"""Resolve four-choice asset labels before a request or plan is frozen."""
from __future__ import annotations

import copy
import re
from typing import Any, Mapping, Sequence

from domain.photo_contracts import LABEL_PLACEHOLDER, validate_copy


CONTRACT_TOKEN = re.compile(r"(?<!\{)\{([a-z][a-z0-9_]*)\}(?!\})")


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
                            variables: Mapping[str, Any],
                            locale_pack: Mapping[str, Any] | None = None) -> str:
    """Fill audited template tokens ({destination}/{temperature}) from contract labels.

    ``locale_pack=None`` keeps the legacy inline TH contract labels (byte-identical
    to the pre-Phase-2 baseline); a bound Locale Pack supplies them instead, so the
    country-agnostic recipe v2 never reads a ``_th`` field.
    """
    from services.photo_locale import travel_copy_tokens
    token_values = travel_copy_tokens(
        variables, travel_contract=travel_contract, locale_pack=locale_pack,
    )
    return fill_contract_copy_tokens(
        text,
        token_values=token_values,
        contract_label="旅行",
    )


def _thermal_transition_copy_tokens(
    contract: Mapping[str, Any], variables: Mapping[str, Any],
) -> dict[str, str]:
    """Resolve the daily hot→cold transition labels from its own contract."""
    transition_key = str(variables.get("transition_key") or "")
    entry = next(
        (dict(item) for item in contract.get("transitions") or []
         if str(item.get("key") or "") == transition_key),
        None,
    )
    if entry is None:
        raise ValueError(f"冷热切换合同不支持场景 {transition_key or '未填写'}")
    sensitivity_key = str(variables.get("thermal_sensitivity") or "")
    sensitivity = dict((contract.get("sensitivity_rules") or {}).get(sensitivity_key) or {})
    dress_code_key = str(variables.get("dress_code") or "")
    dress_code = dict((contract.get("dress_code_rules") or {}).get(dress_code_key) or {})
    return {
        "transition_label": str(entry.get("label_th") or ""),
        "sensitivity_label": str(sensitivity.get("label_th") or sensitivity_key),
        "dress_code_label": str(dress_code.get("label_th") or dress_code_key),
    }


def fill_contract_copy_tokens(
    text: str, *, token_values: Mapping[str, Any], contract_label: str = "图文",
) -> str:
    """Fill audited contract tokens and reject unresolved values.

    The caller owns the mapping from business variables to localized labels;
    this helper deliberately stays neutral between travel and layering flows.
    """
    filled = str(text or "")
    values = {str(key): str(value) for key, value in token_values.items()
              if value not in (None, "")}
    for token in sorted(set(CONTRACT_TOKEN.findall(filled))):
        if token not in values:
            raise ValueError(f"{contract_label}文案模板缺少 {token} 的已审核填充值")
        filled = filled.replace("{" + token + "}", values[token])
    return filled


def contract_copy_tokens(recipe_spec: Mapping[str, Any], variables: Mapping[str, Any],
                         locale_pack: Mapping[str, Any] | None = None) -> dict[str, str]:
    """Resolve localized tokens from a Recipe-owned executable contract."""
    travel = dict(recipe_spec.get("travel_contract") or {})
    if travel:
        from services.photo_locale import travel_copy_tokens
        return travel_copy_tokens(
            variables, travel_contract=travel, locale_pack=locale_pack,
        )
    transition = dict(recipe_spec.get("thermal_transition_contract") or {})
    if transition:
        return _thermal_transition_copy_tokens(transition, variables)
    layering = dict(recipe_spec.get("layering_contract") or {})
    if not layering:
        return {}
    band_key = str(variables.get("band_key") or variables.get("temperature_band") or "")
    band = next(
        (dict(item) for item in layering.get("bands") or []
         if str(item.get("key") or "") == band_key),
        {},
    )
    if not band:
        raise ValueError(f"温度穿搭合同不支持温度档 {band_key or '未填写'}")
    sensitivity_key = str(variables.get("thermal_sensitivity") or "")
    sensitivity = dict((layering.get("sensitivity_rules") or {}).get(sensitivity_key) or {})
    scene_key = str(variables.get("scene") or "")
    scene = next(
        (dict(item) for item in layering.get("scene_modifiers") or []
         if str(item.get("key") or "") == scene_key),
        {},
    )
    bounds = dict(band.get("final_visible_layer_bounds") or {})
    lower, upper = int(bounds.get("min") or 0), int(bounds.get("max") or 0)
    choice = str(sensitivity.get("layer_choice") or "DEFAULT")
    layer_count = upper if choice == "UPPER_BOUND" else lower
    if choice == "DEFAULT" and lower and upper:
        layer_count = (lower + upper) // 2
    return {
        "temperature": str(band.get("label_th") or ""),
        "layer_count": str(layer_count or ""),
        "sensitivity_label": str(sensitivity.get("label_th") or sensitivity_key),
        "scene_label": str(scene.get("label_th") or scene_key),
    }
