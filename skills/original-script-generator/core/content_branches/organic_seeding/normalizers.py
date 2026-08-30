"""Normalize model output before branch QC or deterministic rendering."""
from __future__ import annotations

from typing import Any, Dict

from ..language_metrics import estimate_spoken_duration_seconds


NARRATIVE_JOBS = {"LIVED_MOMENT", "RELATION", "ATMOSPHERE", "PRODUCT_DETAIL"}
PRODUCT_FOCUS_LEVELS = {"BACKGROUND", "SECONDARY", "PRIMARY"}
PRODUCT_INTERACTIONS = {"NONE", "NATURAL_USE", "DEMONSTRATION"}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _strip_role_prefix(value: Any) -> str:
    text = _text(value)
    for prefix in ("人物动作：", "人物：", "人物人物", "人物"):
        if text.startswith(prefix):
            return text[len(prefix):].strip(" ：:")
    return text


def normalize_visual_blueprint(payload: Any) -> Dict[str, Any]:
    raw = dict(payload) if isinstance(payload, dict) else {}
    units = []
    for index, item in enumerate(raw.get("capture_units") or [], 1):
        if not isinstance(item, dict):
            continue
        unit = dict(item)
        unit["unit_id"] = _text(unit.get("unit_id")) or f"C{index}"
        unit["subject_action"] = _strip_role_prefix(
            unit.get("subject_action") or unit.get("action")
        )
        unit.pop("action", None)
        unit["shot_size"] = _text(unit.get("shot_size")) or "UNSPECIFIED"
        unit["body_coverage"] = _text(unit.get("body_coverage")) or "UNSPECIFIED"
        unit["camera_relation"] = _text(unit.get("camera_relation")) or "UNSPECIFIED"
        unit["setup_id"] = _text(unit.get("setup_id")) or "SETUP_1"
        unit["narrative_job"] = _text(unit.get("narrative_job")).upper()
        unit["product_focus"] = _text(unit.get("product_focus")).upper()
        unit["product_interaction"] = _text(unit.get("product_interaction")).upper()
        if unit["narrative_job"] not in NARRATIVE_JOBS:
            unit["narrative_job"] = "UNSPECIFIED"
        if unit["product_focus"] not in PRODUCT_FOCUS_LEVELS:
            unit["product_focus"] = "UNSPECIFIED"
        if unit["product_interaction"] not in PRODUCT_INTERACTIONS:
            unit["product_interaction"] = "UNSPECIFIED"
        for key in ("authorized_props", "visible_zones", "evidence_refs", "fact_refs"):
            unit[key] = [
                _text(value) for value in unit.get(key) or [] if _text(value)
            ]
        units.append(unit)
    raw["schema_version"] = "organic-visual-blueprint-v4"
    raw["capture_units"] = units
    raw.pop("video_generation_prompt", None)
    return raw


def normalize_voiceover(
    payload: Any, *, target_language: str, duration_seconds: float
) -> Dict[str, Any]:
    raw = dict(payload) if isinstance(payload, dict) else {}
    target_text = _text(raw.get("target_text"))
    actual_duration = estimate_spoken_duration_seconds(target_text, target_language)
    raw["schema_version"] = "organic-voiceover-v4-native-spoken-brief"
    raw["target_text"] = target_text
    raw["chinese_translation"] = _text(raw.get("chinese_translation"))
    raw["model_estimated_duration_seconds"] = raw.get("estimated_duration_seconds")
    raw["estimated_duration_seconds"] = actual_duration
    raw["duration_budget_seconds"] = float(duration_seconds)
    raw["used_fact_refs"] = [
        _text(value) for value in raw.get("used_fact_refs") or [] if _text(value)
    ]
    raw["experience_claims"] = [
        _text(value) for value in raw.get("experience_claims") or [] if _text(value)
    ]
    progression = raw.get("content_progression")
    raw["content_progression"] = dict(progression) if isinstance(progression, dict) else {}
    return raw
