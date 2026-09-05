"""Deterministic lightweight text overlays for image-story videos."""

from __future__ import annotations

from copy import deepcopy
import re
from typing import Any, Dict, Mapping


OVERLAY_PROFILE_LIGHT_V1 = "OVERLAY_LIGHT_V1"

TH_RECIPE_COPY: Dict[str, Dict[int, Dict[str, str]]] = {
    "RECIPE_PAIN_POINT_SOLUTION_V1": {
        1: {
            "template_id": "HOOK_TITLE_V1",
            "text": "ตัวเล็กแต่งยังไงให้ดูสูง?",
        },
        2: {"template_id": "STATE_TAG_V1", "text": "ก่อน: สัดส่วนดูตัน"},
        3: {"template_id": "STATE_TAG_V1", "text": "หลัง: เอวสูงช่วยได้"},
        5: {
            "template_id": "END_QUESTION_V1",
            "text": "ลุคนี้ช่วยให้ดูขายาวขึ้น",
        },
    },
    "RECIPE_SCENE_SOLUTION_V1": {
        1: {"template_id": "HOOK_TITLE_V1", "text": "ไอเดียแต่งตัวในวันนี้"},
        3: {
            "template_id": "STATE_TAG_V1",
            "text": "รายละเอียดของลุคนี้",
        },
        5: {"template_id": "END_QUESTION_V1", "text": "เรียบง่าย แต่ดูดี"},
    },
    "RECIPE_VISUAL_TRANSFORM_V1": {
        1: {
            "template_id": "HOOK_TITLE_V1",
            "text": "ไอเดียแมตช์เสื้อตัวเดิม",
        },
        2: {"template_id": "STATE_TAG_V1", "text": "LOOK 1"},
        3: {"template_id": "STATE_TAG_V1", "text": "LOOK 2"},
        5: {"template_id": "END_QUESTION_V1", "text": "ชอบลุคไหนมากกว่า?"},
    },
    "RECIPE_OUTFIT_BREAKDOWN_V1": {},
    "RECIPE_MULTI_LOOK_V1": {},
}


class TextOverlayError(ValueError):
    pass


def _garment_label(state: Mapping[str, Any]) -> str:
    """Conservative garment vocabulary, not free-form translation or guessed colour."""
    inner = str(state.get("top_inner") or "").lower()
    bottom = state.get("bottom") or {}
    bottom_text = str(bottom.get("type") or "") if isinstance(bottom, dict) else str(bottom)
    bottom_text = bottom_text.lower()
    # Explicit dress takes precedence over 'no separate bottom' placeholders.
    dress_tokens = ("连衣裙", "เดรส", "dress")
    if any(token in inner or token in bottom_text for token in dress_tokens):
        return "เดรส"
    skirt = any(token in bottom_text for token in ("裙", "skirt", "กระโปรง"))
    pants = any(token in bottom_text for token in ("裤", "pants", "trousers", "jeans", "shorts", "กางเกง"))
    if skirt and not pants:
        return "กระโปรง"
    if pants and not skirt:
        return "กางเกง"
    return ""


def _state_copy(plan: Mapping[str, Any], shot: Mapping[str, Any], recipe_id: str, text: str) -> str:
    states = plan.get("outfit_states") or {}
    if not states:
        return text
    slot = int(shot.get("slot_index") or 0)
    if recipe_id == "RECIPE_PAIN_POINT_SOLUTION_V1":
        # Wearing-mode changes do not prove the person became taller/thinner.
        return {1: "ปรับวิธีใส่ ด้วยเสื้อผ้าชุดเดิม", 2: "วิธีใส่แบบแรก",
                3: "ลองปรับวิธีใส่", 5: "ชอบวิธีใส่แบบไหน?"}.get(slot, text)
    if recipe_id != "RECIPE_VISUAL_TRANSFORM_V1":
        return text
    base, final = states.get("BASE") or {}, states.get("FINAL") or {}
    from services.styling_normalizer import outfit_fingerprint
    distinct = bool(base and final) and not final.get("same_look", False) and (
        outfit_fingerprint(base, include_accessories=False) != outfit_fingerprint(final, include_accessories=False)
    )
    if slot == 1:
        return "เสื้อตัวเดิม แมตช์ได้ 2 ลุค" if distinct else "รายละเอียดของลุคนี้"
    if slot == 5:
        return "ชอบลุคไหนมากกว่า?" if distinct else "ลุคนี้เป็นสไตล์คุณไหม?"
    state_ref = str(shot.get("outfit_state_ref") or "")
    state = states.get(state_ref) or {}
    if not distinct:
        return "รายละเอียดของลุคนี้"
    if not state:
        return "ไอเดียการแต่งตัว"
    label = "LOOK 1" if state_ref == "BASE" else "LOOK 2"
    garment = _garment_label(state)
    return f"{label} · {garment}" if garment else label


def apply_overlay_profile(
    plan: Mapping[str, Any],
    *,
    recipe_id: str,
    locale: str,
    profile_id: str,
) -> Dict[str, Any]:
    if profile_id != OVERLAY_PROFILE_LIGHT_V1:
        raise TextOverlayError(f"unknown overlay profile: {profile_id}")
    if locale != "th-TH":
        raise TextOverlayError(
            f"{profile_id} currently has no approved copy for locale {locale!r}"
        )
    slot_copy = TH_RECIPE_COPY.get(recipe_id)
    if slot_copy is None:
        raise TextOverlayError(f"no overlay copy for recipe {recipe_id!r}")
    output = deepcopy(dict(plan))
    output["overlay_profile_id"] = profile_id
    for shot in output.get("shots") or []:
        slot = int(shot.get("slot_index") or 0)
        item = slot_copy.get(slot)
        if recipe_id == "RECIPE_MULTI_LOOK_V1":
            count = len(output.get("shots") or [])
            state = (output.get("outfit_states") or {}).get(shot.get("outfit_state_ref")) or {}
            label = _garment_label(state)
            text = f"LOOK {slot}" + (f" · {label}" if label else "")
            template = "STATE_TAG_V1"
            if slot == 1:
                text = f"ไอเท็มเดิม แมตช์ได้ {count} ลุค" if count > 1 else "ไอเดียแต่งตัวในวันนี้"
                template = "HOOK_TITLE_V1"
            elif slot == count:
                text = f"LOOK {slot} · ชอบลุคไหน?"
                template = "END_QUESTION_V1"
            item = {"text": text, "template_id": template}
        if item is None:
            shot["overlay_text"] = ""
            shot["overlay_spec"] = {"enabled": False}
            continue
        text = _state_copy(output, shot, recipe_id, item["text"])
        validate_overlay_text(text, locale=locale)
        shot["overlay_text"] = text
        shot["overlay_spec"] = {
            "enabled": True,
            "template_id": item["template_id"],
            "locale": locale,
            "safe_area": "tiktok_1080x1920_v1",
            "layout_policy": "reserved_header_v2",
        }
    return output


def validate_overlay_text(text: str, *, locale: str) -> None:
    value = str(text or "").strip()
    if not value:
        raise TextOverlayError("overlay text is empty")
    if "\n" in value or "\r" in value:
        raise TextOverlayError("overlay text must be a single logical line")
    if re.search(r"[\u4e00-\u9fff]", value):
        raise TextOverlayError("overlay text contains CJK characters")
    limit = 42 if locale == "th-TH" else 48
    if len(value) > limit:
        raise TextOverlayError(
            f"overlay text is too long for {locale}: {len(value)} > {limit}"
        )
