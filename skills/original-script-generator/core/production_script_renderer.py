"""Render one simplified batch item for the Feishu production workbench.

The batch database remains authoritative.  This module deliberately emits two
different projections:

* a human-readable complete script for review in Feishu;
* a compact production prompt consumed by the video run manager.

Internal lineage is returned as scalar metadata and is never expanded into the
video prompt.
"""
from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, Iterable

from core.accessory_mixed_templates import (
    ACCESSORY_MIXED_TEMPLATE_PROFILE,
    MIXED_TEMPLATE_CONTRACT_KEY,
    audit_mixed_final_execution,
    frozen_mixed_contract,
    mixed_execution_objects,
    module_framing_legend_lines,
    worn_body_framing,
)
from core.category_execution import (
    compile_category_execution_extension,
    resolve_category_carrier_execution,
)
from core.simplified_complete_script import (
    CAPTURE_RHYTHM_LEGACY,
    CAPTURE_RHYTHM_MULTICLIP,
    CAPTURE_RHYTHM_PROFILE_ENV,
    CAPTURE_MODE_CREATOR_SELF_SHOT,
    build_capture_rhythm_contract,
    build_product_identity_lock,
    compile_capture_units,
    normalize_creator_capture_preset,
)
from core.video_prompt_compaction import (
    CompactionReport,
    compact_video_prompt_report,
)


VIDEO_PROMPT_PROFILE_ENV = "ORIGINAL_SCRIPT_VIDEO_PROMPT_PROFILE"
UGC_NATIVE_PROFILE = "ugc_native_v1"

# Bumped whenever this renderer changes what a delivered prompt may contain.
# The final execution audit binds its verdict to this string plus the prompt
# hash, so a re-render that produces a different prompt is a different object
# and has to be re-checked rather than inheriting the old PASS.
#: v5：【整片语义主线】的两行改为各自判定 —— 表达口径收窄后只剩一行时不再整块丢弃。
SCRIPT_RENDERER_VERSION = "production-script-renderer-v5-speakable-semantic-mainline"
DELIVERY_SNAPSHOT_SCHEMA_VERSION = "necklace-delivery-snapshot-v1"
DELIVERY_SNAPSHOT_FIELD = "delivery_snapshot"

# Persisted shape of the execution audit.  Versioned separately from the
# renderer: the audit schema can gain fields without implying the prompt changed
# (and therefore without invalidating an existing ``PASS``).
RENDER_VALIDATION_SCHEMA_VERSION = "mixed-render-validation-v1"
RENDER_VALIDATION_FIELD = "render_validation"

LEGACY_PROFILE = "legacy"
STAGE0_VIDEO_PROMPT_PROFILE = "stage0-ugc-compact-v2-capture-fidelity"
UGC_NATIVE_POSITIVE = (
    "普通用户使用手机竖屏随手记录，使用现场已有自然光或普通室内光；"
    "机位简单，允许轻微手持感、轻微构图不完美和真实环境层次，"
    "人物皮肤、衣物和背景保留自然质感"
)
UGC_NATIVE_NEGATIVE = (
    "不要广告片、时尚大片、影棚布光、电影运镜、强景深虚化、精修磨皮、"
    "稳定器滑轨感、完美商品陈列和过度干净的布景"
)
CREATOR_SELF_SHOT_POSITIVE = (
    "创作者本人使用手机前置镜头或放在身边的一台手机录制，主要看向镜头直接分享；"
    "全片保持一个主要手机视角、一个地点和一个连续时刻，像个人账号的一次真实录制；"
    "允许短暂退后展示穿着结果，最多插入一次商品细节补拍，随后回到同一手机视角"
)
CREATOR_SELF_SHOT_NEGATIVE = (
    "不要广告片、摄影团队、第三人跟拍、正反打、多机位覆盖、影棚布光、精修磨皮、"
    "稳定器推拉横移、商业景深、广告定格、模特走位和跨房间调度"
)
CREATOR_MULTICLIP_POSITIVE = (
    "创作者本人在同一地点、同一时刻使用同一部手机分别录制3至5段简短素材；"
    "片段间使用普通直接剪切或自然跳剪，不同片段允许在同一小片区域重新放置手机、"
    "改变人物与手机距离或补录商品细节；成片片段数不等于手机布置数，"
    "人物、商品、穿搭、光线和生活状态保持连续"
)
CREATOR_MULTICLIP_NEGATIVE = (
    "不要摄影团队多机位覆盖、第三人跟拍、正反打、稳定器推拉横移、跨房间调度、"
    "商业景深或广告定格；不要只靠数字裁切、连续变焦或人物在一个长镜头里反复走近走远来假装切镜"
)


def _text(value: Any, fallback: str = "UNAVAILABLE") -> str:
    text = str(value or "").strip()
    return text or fallback


def _dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list:
    return value if isinstance(value, list) else []


def _json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _outfit_scene_match_label(contract: Dict[str, Any]) -> str:
    return {
        "MATCHED": "已匹配",
        "NO_PREFERENCE": "未配置偏好",
        "FALLBACK": "已回退",
        "NOT_APPLICABLE": "不适用",
    }.get(_text(contract.get("match_status"), "").upper(), "不适用")


def _actual_outfit_accessories(
    outfit_contract: Dict[str, Any], production_outfit: Dict[str, Any]
) -> str:
    items = [
        _text(item, "") for item in _list(outfit_contract.get("accessory_items"))
        if _text(item, "")
    ]
    if items:
        return "；".join(items)
    recipe = _dict(outfit_contract.get("outfit_recipe"))
    return _text(
        recipe.get("other_accessories")
        or production_outfit.get("accessories"),
        "不适用",
    )


def _join(values: Iterable[Any]) -> str:
    cleaned = [str(value).strip() for value in values if str(value or "").strip()]
    return "；".join(cleaned) or "UNAVAILABLE"


def _video_prompt_profile(brief: Dict[str, Any]) -> str:
    configured = str(os.environ.get(VIDEO_PROMPT_PROFILE_ENV) or "").strip().lower()
    if configured:
        return LEGACY_PROFILE if configured == LEGACY_PROFILE else UGC_NATIVE_PROFILE
    embedded = str(brief.get("render_profile") or "").strip().lower()
    return LEGACY_PROFILE if embedded == LEGACY_PROFILE else UGC_NATIVE_PROFILE


def _naturalize_text(value: Any) -> str:
    text = _text(value, "")
    replacements = (
        ("背景柔和虚化", "背景保留现场环境"),
        ("背景轻微虚化", "背景保留现场环境"),
        ("背景虚化", "背景保留现场环境"),
        ("远处虚化", "远处环境自然可见"),
        ("后方虚化", "后方环境自然可见"),
        ("轻微虚化", "自然景深"),
        ("柔和虚化", "自然景深"),
        ("以固定近景带到", "近距离看到"),
        ("固定近景", "近景"),
        ("画面整洁", "保留真实生活环境"),
        ("干净墙面", "普通墙面"),
    )
    for old, new in replacements:
        text = text.replace(old, new)
    text = text.replace("虚化", "自然可见")
    text = text.replace("保留真实生活环境但保留", "保留")
    return text


def _compact_native_description(
    value: Any,
    *,
    max_segments: int = 2,
    max_chars: int = 110,
) -> str:
    text = _naturalize_text(value)
    if not text:
        return ""
    segments = [part.strip() for part in re.split(r"[，；。]", text) if part.strip()]
    compact = "，".join(segments[:max_segments]) if segments else text
    if len(compact) <= max_chars:
        return compact
    return compact[:max_chars].rstrip("，；。 ")


def _closure_summary(identity_lock: Dict[str, Any]) -> str:
    contract = _dict(identity_lock.get("visible_closure_contract"))
    if contract.get("status") != "AVAILABLE":
        return ""
    parts = [
        _text(contract.get("visible_description"), ""),
        _text(contract.get("hidden_description"), ""),
    ]
    return "，".join(part for part in parts if part)


def _video_safe_closure_text(value: Any, identity_lock: Dict[str, Any]) -> str:
    """Remove prose that can literalise hidden snaps as a visible second row."""

    text = _naturalize_text(value)
    contract = _dict(identity_lock.get("visible_closure_contract"))
    if contract.get("status") != "AVAILABLE" or not contract.get("hidden_counterpart"):
        return text
    summary = _closure_summary(identity_lock)
    if not summary:
        return text
    text = re.sub(r"左侧.{0,80}?暗扣", summary, text, count=1)
    text = text.replace("按扣与暗扣结构", "单列可见圆扣与隐藏闭合结构")
    text = text.replace("两侧扣位", "前襟单列可见扣位")
    return text


def _compact_camera(value: Any) -> str:
    text = _text(value, "")
    if not text:
        return ""
    tokens = []
    for token in (
        "正上方", "俯拍", "正面", "侧前方", "侧面", "平视",
        "全身", "中远景", "中景", "近景", "特写",
    ):
        if token in text and token not in tokens:
            tokens.append(token)
        if len(tokens) >= 3:
            break
    return " / ".join(tokens)


def _capture_mode(brief: Dict[str, Any], production: Dict[str, Any]) -> str:
    return _text(
        brief.get("capture_mode") or production.get("capture_mode"), ""
    ).upper()


def _preserve_category_capture_projection(
    rebuilt: Dict[str, Any], embedded: Dict[str, Any]
) -> Dict[str, Any]:
    """Keep an already-frozen accessory projection during public rebuilds."""

    if not isinstance(embedded.get("category_rollout_contract"), dict):
        return rebuilt
    rebuilt_count = int(rebuilt.get("capture_unit_count") or 0)
    category_roles = _list(embedded.get("category_unit_roles"))
    unit_roles = _list(embedded.get("unit_roles"))
    framing = _list(embedded.get("framing_guidance_by_unit"))
    if (
        rebuilt_count < 1
        or len(category_roles) != rebuilt_count
        or len(unit_roles) != rebuilt_count
        or len(framing) != rebuilt_count
    ):
        # A stale category projection must not claim to be active after the
        # public compiler has selected a different visible-clip count.
        return rebuilt
    result = dict(rebuilt)
    for key in (
        "category_projection",
        "category_unit_roles",
        "unit_roles",
        "framing_guidance_by_unit",
        "category_rollout_contract",
    ):
        value = embedded.get(key)
        if isinstance(value, dict):
            result[key] = dict(value)
        elif isinstance(value, list):
            result[key] = list(value)
        elif value not in (None, ""):
            result[key] = value
    rebuilt_richness = _dict(result.get("shot_richness_contract"))
    embedded_richness = _dict(embedded.get("shot_richness_contract"))
    for key, value in embedded_richness.items():
        if key.startswith("category_"):
            rebuilt_richness[key] = value
    result["shot_richness_contract"] = rebuilt_richness
    return result


def _capture_rhythm_contract(
    brief: Dict[str, Any],
    script: Dict[str, Any],
    *,
    capture_mode: str,
    storyboard: list,
) -> Dict[str, Any]:
    embedded = _dict(brief.get("capture_rhythm_contract")) or _dict(
        script.get("capture_rhythm_contract")
    )
    configured = _text(os.environ.get(CAPTURE_RHYTHM_PROFILE_ENV), "").upper()
    if configured in {"LEGACY", "LEGACY_ONE_TAKE", "ONE_TAKE"}:
        return build_capture_rhythm_contract(
            capture_mode=capture_mode,
            macro_structure=[_text(_dict(value).get("narrative_role"), "") for value in storyboard],
        )
    if configured in {
        "NATIVE_MULTI_CLIP_V1",
        "NATIVE_MULTICLIP_V1",
        "MULTICLIP",
        "MULTI_CLIP",
    }:
        with_profile = build_capture_rhythm_contract(
            capture_mode=capture_mode,
            macro_structure=[_text(_dict(value).get("narrative_role"), "") for value in storyboard],
            scene_context=_dict(
                _dict(brief.get("production_design")).get("scene")
            ),
        )
        with_profile["profile"] = CAPTURE_RHYTHM_MULTICLIP
        return _preserve_category_capture_projection(with_profile, embedded)
    allocated_direction = _dict(script.get("allocated_direction"))
    routed_macro = _list(allocated_direction.get("macro_structure"))
    if not routed_macro:
        routed_macro = _list(embedded.get("macro_structure"))
    if not routed_macro:
        routed_macro = [
            _text(_dict(value).get("narrative_role"), "")
            for value in storyboard
        ]
    # V3 encoded the fatal two/three-clip cap. V4 fixed the count but still
    # flattened different routed structures into one generic four-part
    # sequence. Upgrade both at render time; product, scene, voiceover and
    # frozen actions remain untouched.
    if (
        _text(embedded.get("schema_version"), "")
        in {
            "capture-rhythm-contract-v3-scene-feasible-reference",
            "capture-rhythm-contract-v4-shot-richness",
        }
        and _text(embedded.get("profile"), "").upper()
        == CAPTURE_RHYTHM_MULTICLIP
    ):
        upgraded = build_capture_rhythm_contract(
            capture_mode=capture_mode,
            macro_structure=routed_macro,
            scene_context=_dict(
                _dict(brief.get("production_design")).get("scene")
            ),
        )
        if isinstance(embedded.get("retrieved_execution_shape"), dict):
            upgraded["retrieved_execution_shape"] = dict(
                embedded.get("retrieved_execution_shape") or {}
            )
        upgraded["derivation_source"] = (
            "READ_TIME_STRUCTURE_VISIBLE_CLIP_UPGRADE"
        )
        return upgraded
    # Other old stored scripts remain compatible unless the environment
    # explicitly enables the current profile.
    if embedded:
        return embedded
    legacy = build_capture_rhythm_contract(
        capture_mode=capture_mode,
        macro_structure=[_text(_dict(value).get("narrative_role"), "") for value in storyboard],
    )
    legacy["profile"] = CAPTURE_RHYTHM_LEGACY
    legacy["capture_unit_count"] = 1
    legacy["capture_grammar"] = "CONTINUOUS_SINGLE_PHONE_VIEW"
    legacy["edit_style"] = "CONTINUOUS_RECORDING"
    return legacy


def _creator_moment_text(value: Any, *, max_chars: int) -> str:
    """Remove director transitions while preserving the executable action."""

    text = _naturalize_text(value)
    replacements = (
        ("切至", ""),
        ("切到", ""),
        ("切回", ""),
        ("镜头位于", "手机保持在"),
        ("固定中长景", ""),
        ("固定中景", ""),
        ("固定近景", ""),
        ("斜侧中长景", ""),
        ("侧前方约四十五度", ""),
        ("浅景深", "自然手机景深"),
    )
    for old, new in replacements:
        text = text.replace(old, new)
    return _compact_native_description(text, max_segments=2, max_chars=max_chars)


def _creator_content_moments(storyboard: list) -> list:
    """Keep routed beats but compile them as moments in one recording.

    Consecutive duplicate beats are merged so a proof-heavy structure does not
    turn back into a five-camera coverage plan in the final video prompt.
    """

    moments = []
    for raw in storyboard:
        shot = _dict(raw)
        role = _text(shot.get("narrative_role"), "MOMENT")
        current = {
            "time_range": _text(shot.get("time_range"), ""),
            "narrative_role": role,
            "visual_content": _creator_moment_text(
                shot.get("visual_content"), max_chars=100
            ),
            "character_action": _creator_moment_text(
                shot.get("character_action"), max_chars=64
            ),
            "product_anchors_visible": _list(
                shot.get("product_anchors_visible")
            ),
        }
        if moments and moments[-1]["narrative_role"] == role:
            previous = moments[-1]
            previous["time_range"] = "{} → {}".format(
                previous["time_range"], current["time_range"]
            ).strip(" →")
            previous["visual_content"] = _compact_native_description(
                "；".join(
                    value
                    for value in (
                        previous["visual_content"], current["visual_content"]
                    )
                    if value
                ),
                max_segments=3,
                max_chars=130,
            )
            previous["character_action"] = _compact_native_description(
                "；".join(
                    value
                    for value in (
                        previous["character_action"], current["character_action"]
                    )
                    if value
                ),
                max_segments=3,
                max_chars=86,
            )
            previous["product_anchors_visible"] = list(
                dict.fromkeys(
                    [
                        *previous["product_anchors_visible"],
                        *current["product_anchors_visible"],
                    ]
                )
            )
            continue
        moments.append(current)
    return moments


def _multiclip_text(value: Any, *, max_chars: int) -> str:
    """Remove legacy one-take language when a stored passage is recompiled."""

    text = _naturalize_text(value)
    replacements = (
        ("同一固定手机画面", "本段独立手机画面"),
        ("同一原始手机画面", "本段独立手机画面"),
        ("本段独立手机画面先裁到", "本段独立录制为"),
        ("本段独立手机画面裁到", "本段独立录制为"),
        ("同一画面裁切自然放宽到", "本段独立录制为"),
        ("画面裁切自然放宽到", "本段独立录制为"),
        ("裁切自然放宽到", "改用"),
        ("同一固定手机", "同一部手机"),
        ("同一固定前置镜头", "同一部手机前置镜头"),
        ("沿用同一固定", "同一地点重新录制的"),
        ("手机位置和透视完全不变", "手机在同一地点重新放置"),
        ("仅在同一原始画面内", "在本段素材中"),
        ("无机位切换和摄影机运动", "使用普通静止手机构图"),
        ("无机位切换", ""),
        ("不切换机位", ""),
        ("手机不动、无变焦", "使用普通静止手机构图"),
        ("手机不移动", "使用普通静止手机构图"),
        ("裁切", "构图"),
    )
    for old, new in replacements:
        text = text.replace(old, new)
    return _compact_native_description(text, max_segments=3, max_chars=max_chars)


_MIXED_BANNED_SUMMARY_TERMS = (
    "镜面", "镜中", "正脸", "全脸", "半脸", "侧脸", "头肩", "自拍",
)


def _mixed_banned_framing_terms(contract: Dict[str, Any]) -> list:
    """Framing concepts this frozen montage bans, in summary form.

    Used to filter the older whole-film accessory prose: a sentence that tells a
    no-face film to prefer ``镜面反射`` is not a stale wording problem, it is a
    direct contradiction of the contract the same prompt states two lines above.
    """

    out: list = []
    for unit in _list(_dict(contract).get("capture_units")):
        for item in _list(_dict(unit).get("forbidden_framing")):
            text = _text(item)
            for term in _MIXED_BANNED_SUMMARY_TERMS:
                if term in text and term not in out:
                    out.append(term)
    return out


def _mixed_capture_unit_passages(
    storyboard: list,
    units: list,
    *,
    mixed_contract: Dict[str, Any],
) -> list:
    """Per-shot passages for a mixed montage, taken from the execution objects.

    Nothing here reads ``unit_role``.  That is the fix: the old arc could only
    reach the prompt through the role lookup, so removing the lookup removes the
    whole class of "handheld shot told to shift its weight" defects rather than
    one sentence of it.
    """

    by_id: Dict[str, list] = {}
    for raw in storyboard:
        shot = _dict(raw)
        by_id.setdefault(_text(shot.get("capture_unit_id"), "CU_01"), []).append(shot)
    compiled_by_id: Dict[str, Dict[str, Any]] = {}
    for raw in units or []:
        unit = _dict(raw)
        key = _text(unit.get("capture_unit_id"))
        if key and key not in compiled_by_id:
            compiled_by_id[key] = unit

    out: list = []
    for obj in mixed_execution_objects(mixed_contract, storyboard):
        unit_id = _text(obj.get("shot_id")) or "CU_01"
        group = by_id.get(unit_id) or []
        compiled = compiled_by_id.get(unit_id) or {}
        if not group and not compiled:
            continue
        visuals = [
            _video_safe_closure_text(shot.get("visual_content"), {})
            for shot in group
            if _text(shot.get("visual_content"), "")
        ]
        anchors: list = []
        for shot in group:
            for anchor in _list(shot.get("product_anchors_visible")):
                if anchor not in anchors:
                    anchors.append(anchor)
        first_range = _text(group[0].get("time_range"), "") if group else ""
        last_range = _text(group[-1].get("time_range"), "") if group else ""
        structure_role = _text(
            compiled.get("structure_role")
            or (group[0].get("structure_role") if group else "")
            or (group[0].get("narrative_role") if group else ""),
            "",
        ).upper()
        out.append(
            {
                **compiled,
                "capture_unit_id": unit_id,
                "time_range": (
                    first_range
                    if not last_range or first_range == last_range
                    else f"{first_range} → {last_range}"
                ),
                # The approved script's own visible event and action.  The
                # frozen contract no longer overwrites them: its ending
                # guidance used to replace the last shot's picture with worn
                # vocabulary ("补录后脑发饰区域") even when the montage ended on
                # a relation shot with its own, better detail.
                "visual_content": _multiclip_text("；".join(visuals), max_chars=150),
                "character_action": _multiclip_text(
                    _text(obj.get("action")), max_chars=95
                ),
                "framing_guidance": _text(obj.get("camera_guidance"), ""),
                # ``_text`` defaults to the literal "UNAVAILABLE"; a shot that
                # simply has no gaze instruction must render *no line*, not a
                # line that says "UNAVAILABLE" -- the compaction pass would then
                # report the placeholder as a lost per-shot constraint.
                "gaze_target": _text(obj.get("gaze_target"), ""),
                "micro_reaction": _text(obj.get("micro_reaction"), ""),
                "natural_reaction": _text(obj.get("micro_reaction"), ""),
                "module": _text(obj.get("module"), ""),
                "module_label": _text(obj.get("module_label"), ""),
                "execution_role": _text(obj.get("execution_role"), ""),
                "execution_version": _text(obj.get("execution_version"), ""),
                "action_source": _text(obj.get("action_source"), ""),
                "mixed_execution_applied": True,
                "structure_role": structure_role,
                "observable_change_job": _text(
                    compiled.get("observable_change_job")
                    or (group[0].get("observable_change_job") if group else ""),
                    "",
                ).upper(),
                "product_anchors_visible": anchors,
                "compiled_clip_passage": True,
            }
        )
    return out


def _capture_unit_passages(
    storyboard: list,
    units: list,
    *,
    accessory_brief: Dict[str, Any] | None = None,
    action_design: Dict[str, Any] | None = None,
    mixed_contract: Dict[str, Any] | None = None,
) -> list:
    accessory_brief = _dict(accessory_brief)
    action_design = _dict(action_design)
    # ── 混合模板：每镜从自己的执行对象派生 ────────────────────────────
    # R1: the old role arc assigns ``unit_role`` by *position*
    # (PRODUCT_RESULT_CLOSE -> NATURAL_MOTION_RELATION -> ... -> PRODUCT_
    # REACQUISITION) and then hands out worn body actions from it.  A montage
    # whose second shot is a handheld close-up and third is a still life has
    # nothing to do with that arc, so it received "the person shifts weight in
    # the neck-and-shoulder relation" and "record a continuous upper body" --
    # in direct violation of those units' own ``forbidden_framing``.
    #
    # In mixed mode the per-shot execution object is the only authority: the
    # frozen unit owns the boundaries, the approved storyboard owns the content.
    if _dict(mixed_contract).get("execution_profile") == ACCESSORY_MIXED_TEMPLATE_PROFILE:
        return _mixed_capture_unit_passages(
            storyboard, units, mixed_contract=_dict(mixed_contract)
        )
    prominence = _dict(accessory_brief.get("product_prominence_contract"))
    # Current category projection supplies the unit-level motion language.  A
    # frozen action design may override individual fields, but must not erase a
    # newer projection field merely because its older performance arc exists.
    performance_by_role: Dict[str, Dict[str, Any]] = {}
    for performance_arc in (
        _list(prominence.get("performance_arc")),
        _list(action_design.get("performance_arc")),
    ):
        for item in performance_arc:
            if not isinstance(item, dict):
                continue
            role = _text(item.get("unit_role"), "").upper()
            if role:
                performance_by_role[role] = {
                    **performance_by_role.get(role, {}),
                    **item,
                }
    terminal = _dict(prominence.get("terminal_visibility"))
    small_accessory_performance = (
        _text(prominence.get("sequence_policy"), "").upper()
        == "PRODUCT_OPENING_TO_MOTION_TO_PRODUCT_RETURN"
    )
    by_id: Dict[str, list] = {}
    for raw in storyboard:
        shot = _dict(raw)
        by_id.setdefault(_text(shot.get("capture_unit_id"), "CU_01"), []).append(shot)
    result = []
    for raw_unit in units:
        unit = _dict(raw_unit)
        unit_id = _text(unit.get("capture_unit_id"), "CU_01")
        group = by_id.get(unit_id) or []
        if not group:
            continue
        visuals = [
            _video_safe_closure_text(shot.get("visual_content"), {})
            for shot in group
            if _text(shot.get("visual_content"), "")
        ]
        actions = [
            _video_safe_closure_text(shot.get("character_action"), {})
            for shot in group
            if _text(shot.get("character_action"), "")
        ]
        reactions = (
            [
                _video_safe_closure_text(shot.get("natural_emotion"), {})
                for shot in group
                if _text(shot.get("natural_emotion"), "")
            ]
            if small_accessory_performance
            else []
        )
        anchors = []
        for shot in group:
            for anchor in _list(shot.get("product_anchors_visible")):
                if anchor not in anchors:
                    anchors.append(anchor)
        first_range = _text(group[0].get("time_range"), "")
        last_range = _text(group[-1].get("time_range"), "")
        unit_role = _text(unit.get("unit_role"), "").upper()
        performance = performance_by_role.get(unit_role, {})
        passage = {
            **unit,
            "time_range": (
                first_range
                if not last_range or first_range == last_range
                else f"{first_range} → {last_range}"
            ),
            "visual_content": _multiclip_text("；".join(visuals), max_chars=150),
            "character_action": _multiclip_text("；".join(actions), max_chars=95),
            "natural_reaction": _multiclip_text(
                "；".join(reactions), max_chars=70
            ),
            "gaze_target": _text(performance.get("gaze_target"), ""),
            "micro_reaction": _text(performance.get("micro_reaction"), ""),
            "structure_role": _text(
                unit.get("structure_role")
                or group[0].get("structure_role")
                or group[0].get("narrative_role"),
                "",
            ).upper(),
            "observable_change_job": _text(
                unit.get("observable_change_job")
                or group[0].get("observable_change_job"),
                "",
            ).upper(),
            "product_anchors_visible": anchors,
            # The passage was already compacted once at the capture-unit
            # boundary.  The final renderer must not truncate it a second time
            # and erase the latter action/state change.
            "compiled_clip_passage": True,
        }
        if small_accessory_performance:
            movement_guidance = _text(
                performance.get("movement_guidance"), ""
            )
            core_action = _text(action_design.get("core_action"), "")
            if unit_role == "NATURAL_MOTION_RELATION" and core_action:
                # The frozen action is the authority for the middle unit.  Do
                # not let a static first storyboard shot or text compaction
                # replace it with "stand and talk".
                passage["character_action"] = core_action
                passage["core_action_projection_applied"] = True
            elif movement_guidance:
                passage["character_action"] = movement_guidance
                passage["active_boundary_projection_applied"] = True
        if unit_role == "PRODUCT_REACQUISITION" and terminal:
            ending_guidance = _text(terminal.get("ending_guidance"), "")
            if ending_guidance:
                passage["visual_content"] = ending_guidance
            if not passage.get("active_boundary_projection_applied"):
                passage["character_action"] = (
                    _text(action_design.get("end_state"), "")
                    or "保持已经完成的佩戴结果，在同一地点自然结束分享"
                )
            passage["terminal_projection_applied"] = True
        result.append(passage)
    return result


def _unique_stage0_texts(
    values: Iterable[Any],
    *,
    identity_lock: Dict[str, Any] | None = None,
) -> list[str]:
    """Keep full executable stage-0 prose while removing exact duplicates.

    A stage-0 storyboard may project one real clip onto two internal structure
    slots.  The video prompt should not repeat those slots, but it must retain
    the complete framing, phone relationship and visible action of the real
    clip.  This helper deliberately has no punctuation or character budget.
    """

    lock = _dict(identity_lock)
    output: list[str] = []
    for value in values:
        text = _video_safe_closure_text(value, lock)
        if text and text not in output:
            output.append(text)
    return output


def _stage0_time_range(group: list[Dict[str, Any]], visual_text: str) -> str:
    """Prefer the authored clip time and fall back to projected slot ranges."""

    match = re.search(
        r"(?P<start>\d+(?:\.\d+)?)\s*(?:至|[-–—~～])\s*"
        r"(?P<end>\d+(?:\.\d+)?)\s*(?:秒|s\b)",
        visual_text,
        flags=re.IGNORECASE,
    )
    if match:
        return f"{float(match.group('start')):g}-{float(match.group('end')):g}s"
    ranges = _unique_stage0_texts(
        shot.get("time_range") or shot.get("duration") for shot in group
    )
    if not ranges:
        return ""
    spans = []
    for value in ranges:
        span = re.search(
            r"(?P<start>\d+(?:\.\d+)?)\s*(?:至|[-–—~～])\s*"
            r"(?P<end>\d+(?:\.\d+)?)\s*(?:秒|s\b)?",
            value,
            flags=re.IGNORECASE,
        )
        if span:
            spans.append((float(span.group("start")), float(span.group("end"))))
    if spans:
        return f"{min(start for start, _ in spans):g}-{max(end for _, end in spans):g}s"
    return ranges[0] if len(ranges) == 1 else " → ".join((ranges[0], ranges[-1]))


def _stage0_capture_passages(script: Dict[str, Any]) -> list[Dict[str, Any]]:
    """Compile one prompt passage per real stage-0 capture unit.

    ``storyboard`` remains the structure/lineage projection.  ``capture_units``
    is the actual edit authority.  When a one-to-one macro visual passage is
    available, it is the most faithful source for visible process and camera
    observation; the projected storyboard still supplies the frozen framing,
    phone relationship, gaze and product anchors.
    """

    brief = _dict(script.get("video_generation_brief"))
    storyboard = [
        dict(item)
        for item in (_list(brief.get("storyboard")) or _list(script.get("storyboard")))
        if isinstance(item, dict)
    ]
    units = [
        dict(item)
        for item in (_list(brief.get("capture_units")) or _list(script.get("capture_units")))
        if isinstance(item, dict)
    ]
    if not units and storyboard:
        seen: list[str] = []
        for index, shot in enumerate(storyboard, 1):
            unit_id = _text(shot.get("capture_unit_id"), f"CU_{index:02d}")
            if unit_id in seen:
                continue
            seen.append(unit_id)
            numbers = [
                int(item.get("shot_no") or item_index)
                for item_index, item in enumerate(storyboard, 1)
                if _text(item.get("capture_unit_id"), f"CU_{item_index:02d}")
                == unit_id
            ]
            units.append(
                {
                    "capture_unit_id": unit_id,
                    "shot_numbers": numbers,
                    "structure_role": _text(shot.get("structure_role") or shot.get("narrative_role"), "MOMENT"),
                    "framing_guidance": _text(shot.get("framing"), ""),
                }
            )

    blueprint = _dict(script.get("creative_blueprint"))
    macro_passages = _list(brief.get("macro_visual_passages")) or _list(
        blueprint.get("macro_visual_passages")
    )
    macro_passages = [dict(item) for item in macro_passages if isinstance(item, dict)]
    macros_align = len(macro_passages) == len(units)
    identity_lock = _dict(brief.get("product_identity_lock"))

    passages: list[Dict[str, Any]] = []
    for index, unit in enumerate(units, 1):
        unit_id = _text(unit.get("capture_unit_id"), f"CU_{index:02d}")
        shot_numbers = {
            int(value)
            for value in _list(unit.get("shot_numbers"))
            if str(value).isdigit()
        }
        group = [
            shot
            for position, shot in enumerate(storyboard, 1)
            if (
                (shot_numbers and int(shot.get("shot_no") or position) in shot_numbers)
                or (not shot_numbers and _text(shot.get("capture_unit_id"), "") == unit_id)
            )
        ]
        macro = macro_passages[index - 1] if macros_align else {}

        group_visuals = _unique_stage0_texts(
            (shot.get("shot_content") or shot.get("visual_content") for shot in group),
            identity_lock=identity_lock,
        )
        group_actions = _unique_stage0_texts(
            (shot.get("observable_action") or shot.get("character_action") or shot.get("person_action") for shot in group),
            identity_lock=identity_lock,
        )
        macro_visual = _video_safe_closure_text(macro.get("visible_process"), identity_lock)
        macro_action = _video_safe_closure_text(macro.get("observable_action"), identity_lock)
        visual_text = macro_visual or "；".join(group_visuals)
        action_text = macro_action or "；".join(group_actions)

        framing = _unique_stage0_texts(
            [
                *(shot.get("framing") or shot.get("style_note") for shot in group),
                macro.get("camera_observation"),
            ],
            identity_lock=identity_lock,
        )
        recording_relation = _unique_stage0_texts(
            (shot.get("recording_relation") for shot in group),
            identity_lock=identity_lock,
        )
        if not framing:
            framing = _unique_stage0_texts(
                [unit.get("framing_guidance")], identity_lock=identity_lock
            )
        gaze_and_reaction = _unique_stage0_texts(
            (
                shot.get("gaze_and_reaction")
                or _dict(shot.get("performance")).get("gaze_and_reaction")
                for shot in group
            ),
            identity_lock=identity_lock,
        )
        anchors: list[str] = []
        for shot in group:
            for value in [
                *_list(shot.get("product_anchors_visible")),
                shot.get("anchor_reference"),
            ]:
                text = _video_safe_closure_text(value, identity_lock)
                if text and text not in anchors:
                    anchors.append(text)

        passages.append(
            {
                "capture_unit_id": unit_id,
                "time_range": _stage0_time_range(group, visual_text),
                "structure_role": _text(
                    unit.get("structure_role")
                    or unit.get("unit_role")
                    or (group[0].get("structure_role") if group else ""),
                    "MOMENT",
                ),
                "visual_content": visual_text,
                "character_action": action_text,
                "framing": "；".join(framing),
                "recording_relation": "；".join(recording_relation),
                "gaze_and_reaction": "；".join(gaze_and_reaction),
                "product_anchors": anchors,
            }
        )
    return passages


def render_stage0_video_generation_prompt(
    *,
    script: Dict[str, Any],
    duration_seconds: float = 15,
) -> str:
    """Render the compact but clip-faithful stage-0 production handoff.

    This is a deterministic projection only.  It does not call a model, does
    not expose claim/QC lineage, and does not turn internal structure slots
    into fake edits.
    """

    script = _dict(script)
    brief = _dict(script.get("video_generation_brief"))
    production = _dict(brief.get("production_design")) or _dict(
        script.get("production_design")
    )
    character = _dict(brief.get("character")) or _dict(
        production.get("character_setting") or production.get("character")
    )
    scene = _dict(brief.get("scene")) or _dict(
        production.get("scene_setting") or production.get("scene")
    )
    outfit_setting = _dict(production.get("outfit_setting") or production.get("outfit"))
    outfit = _text(brief.get("outfit") or outfit_setting.get("styling") or outfit_setting.get("base_outfit"), "")
    identity_lock = _dict(brief.get("product_identity_lock"))
    must_preserve = _unique_stage0_texts(
        _list(identity_lock.get("must_preserve")), identity_lock=identity_lock
    )[:3]
    must_not_change = _unique_stage0_texts(
        _list(identity_lock.get("must_not_change")), identity_lock=identity_lock
    )[:3]
    voice = _dict(brief.get("voiceover")) or _dict(script.get("continuous_voiceover"))
    semantic_context = _dict(brief.get("semantic_context"))
    recording_context = _dict(brief.get("recording_context"))
    capture_rhythm = _dict(brief.get("capture_rhythm_contract")) or _dict(
        script.get("capture_rhythm_contract")
    )
    passages = _stage0_capture_passages(script)
    if not passages:
        raise ValueError("阶段0视频提示词缺少可执行拍摄片段")

    lines = [
        "【视频任务】",
        f"普通个人账号手机竖屏短视频，时长{float(duration_seconds or 15):g}秒。",
        "",
        "【商品身份锁｜最高优先级】",
        "商品外观以参考图为唯一准则；商品一致性优先于人物表演、场景氛围和镜头效果。",
    ]
    # 两行各自独立判定：合同把并列场景收窄后，"主消费情境"可能被清空而"核心购买理由"
    # 仍有内容（反之亦然）。用前者当整块的门，会把后者连带丢掉。
    narrative_context = _text(semantic_context.get("primary_narrative_context"), "")
    core_buying_reason = _text(semantic_context.get("core_buying_reason"), "")
    if narrative_context or core_buying_reason:
        lines.extend(["", "【整片语义主线｜不做逐句逐镜绑定】"])
        if narrative_context:
            lines.append("主消费情境：" + narrative_context)
        if core_buying_reason:
            lines.append("核心购买理由：" + core_buying_reason)
        lines.append(
            "画面、人物和口播保持在同一个消费世界；不要求每句口播由当前镜头证明，"
            "也不得让背景地点改写这条主线。"
        )
    if must_preserve:
        lines.append("必须保持：" + _join(must_preserve))
    if must_not_change:
        lines.extend(["", "【商品负向约束】", _join(must_not_change)])

    lines.extend(
        [
            "",
            "【人物、穿搭与场景】",
            "人物：" + "；".join(
                value
                for value in (
                    _text(character.get("identity"), ""),
                    _text(character.get("appearance"), ""),
                    _text(character.get("hair_makeup"), ""),
                )
                if value
            ),
            f"穿搭：{outfit}",
            "场景：" + "；".join(
                value
                for value in (
                    _text(scene.get("location"), ""),
                    _text(scene.get("moment"), ""),
                    _text(scene.get("lighting"), ""),
                    _text(scene.get("background"), ""),
                )
                if value
            ),
            *(
                ["分享动机：" + _text(recording_context.get("recording_motivation"), "")]
                if _text(recording_context.get("recording_motivation"), "")
                else []
            ),
            "",
            "【拍摄与剪辑】",
            (
                f"同一创作者、商品、穿搭、地点和时刻，用同一部普通手机分别录制{len(passages)}段素材；"
                "片段间使用普通直接剪切，不是一个长镜头里的数字裁切、连续变焦或人物反复走近走远。"
            ),
            (
                f"手机布置预算：{int(capture_rhythm.get('camera_setup_count') or 2)}种；"
                "同一布置可以录制不同内容时刻，成片片段数不等于手机布置数。"
            ),
        ]
    )

    for index, passage in enumerate(passages, 1):
        if index > 1:
            lines.extend(["", "【直接剪切｜开始另一段独立手机素材】"])
        title_parts = [f"拍摄片段{index:02d}"]
        if _text(passage.get("time_range"), ""):
            title_parts.append(_text(passage.get("time_range"), ""))
        title_parts.append(_text(passage.get("structure_role"), "MOMENT"))
        lines.extend(
            [
                "",
                "【" + "｜".join(title_parts) + "】",
                f"画面事件：{_text(passage.get('visual_content'), '')}",
                f"人物动作：{_text(passage.get('character_action'), '')}",
            ]
        )
        if _text(passage.get("framing"), ""):
            lines.append("本段手机构图：" + _text(passage.get("framing"), ""))
        if _text(passage.get("recording_relation"), ""):
            lines.append("拍摄关系：" + _text(passage.get("recording_relation"), ""))
        if _text(passage.get("gaze_and_reaction"), ""):
            lines.append("视线与自然状态：" + _text(passage.get("gaze_and_reaction"), ""))
        anchors = _list(passage.get("product_anchors"))
        if anchors:
            lines.append("本段商品焦点：" + _join(anchors))

    # 交付文档的语种标签只认 target_language。``target_text`` 是目标语言**正文**，
    # 拿它当语种标签会把越南语脚本标成别的（Review R3）。
    from core.mixed_voiceover_mainline import resolve_language_label

    _language_label = resolve_language_label(voice=voice)

    lines.extend(
        [
            "",
            "【连续口播｜必须原样使用目标语言】",
            _language_label["label"] or _text(voice.get("target_text"), ""),
            "",
            "【统一执行优先级】",
            (
                "第一优先保持商品身份、穿戴状态和人物肢体连续；第二优先完整执行以上独立可见片段并真实直接剪切；"
                "第三优先执行具体景别与商品观察关系。发生冲突时先简化背景陈设和人物表演，不得合并片段或退回一镜到底。"
            ),
        ]
    )
    return "\n".join(lines).strip()


def _upgrade_small_accessory_brief_for_render(
    *,
    category_extension: Dict[str, Any],
    accessory_brief: Dict[str, Any],
    production: Dict[str, Any],
    item: Any,
) -> Dict[str, Any]:
    """Read current soft motion projection for old ready scripts.

    The stored action remains frozen.  Only the read-time prominence contract
    is upgraded so an already-ready prompt can regain a product-visible ending
    without rerunning the blueprint or voiceover.
    """

    result = dict(accessory_brief or {})
    if not category_extension:
        return result
    carrier = resolve_category_carrier_execution(
        category_extension,
        presentation_mode=_text(
            production.get("presentation_mode")
            or getattr(item, "carrier_mode", ""),
            "",
        ),
    )
    current_prominence = _dict(carrier.get("product_prominence_contract"))
    # Very old accessory profiles (v1/v2) predate product_prominence entirely.
    # Recompile only the current registered category semantics for rendering;
    # do not change the frozen structure, action, selling point or voiceover.
    if (
        _text(current_prominence.get("sequence_policy"), "").upper()
        != "PRODUCT_OPENING_TO_MOTION_TO_PRODUCT_RETURN"
    ):
        type_source = _dict(category_extension.get("product_type_source"))
        product_type = _text(
            type_source.get("display_type")
            or type_source.get("canonical_type"),
            "",
        )
        rebuilt_extension = (
            compile_category_execution_extension(
                product_type=product_type,
                top_category="配饰",
                anchor_card={},
                enabled=True,
            )
            if product_type
            else {}
        )
        if rebuilt_extension:
            rebuilt_carrier = resolve_category_carrier_execution(
                rebuilt_extension,
                presentation_mode=_text(
                    production.get("presentation_mode")
                    or getattr(item, "carrier_mode", ""),
                    "",
                ),
            )
            current_prominence = _dict(
                rebuilt_carrier.get("product_prominence_contract")
            )
    if (
        _text(current_prominence.get("sequence_policy"), "").upper()
        != "PRODUCT_OPENING_TO_MOTION_TO_PRODUCT_RETURN"
    ):
        return result
    stored_prominence = _dict(result.get("product_prominence_contract"))
    result["product_prominence_contract"] = {
        **stored_prominence,
        **current_prominence,
    }
    if not result.get("schema_version") or result.get("schema_version") == "accessory-video-handoff-v4-small-prominence":
        result["schema_version"] = "accessory-video-handoff-v5-small-motion-return"
    return result


def _apply_small_accessory_capture_projection(
    units: list,
    *,
    accessory_brief: Dict[str, Any],
) -> list:
    prominence = _dict(
        _dict(accessory_brief).get("product_prominence_contract")
    )
    if (
        _text(prominence.get("sequence_policy"), "").upper()
        != "PRODUCT_OPENING_TO_MOTION_TO_PRODUCT_RETURN"
        or len(units) < 2
    ):
        return units
    terminal = _dict(prominence.get("terminal_visibility"))
    if len(units) == 2:
        roles = ["PRODUCT_RESULT_CLOSE", "PRODUCT_REACQUISITION"]
    else:
        # V4 commonly produces four visible clips.  Repeating
        # NATURAL_MOTION_RELATION for every middle clip would project the same
        # frozen core_action twice and recreate a stiff repeated head turn.
        # Only one clip owns the core motion; later middle clips retain their
        # own storyboard detail/context event.
        middle_roles = ["NATURAL_MOTION_RELATION"]
        if len(units) >= 4:
            middle_roles.append("PRODUCT_DETAIL_RELATION")
        if len(units) >= 5:
            middle_roles.extend(
                ["CONTEXT_RELATION"] * (len(units) - 4)
            )
        roles = [
            "PRODUCT_RESULT_CLOSE",
            *middle_roles,
            "PRODUCT_REACQUISITION",
        ]
    result = []
    for index, raw in enumerate(units):
        unit = dict(raw)
        unit["unit_role"] = roles[index]
        if index == 0:
            unit["framing_guidance"] = (
                "独立录制商品已经佩戴完成的结果近景，让小商品第一眼清楚可辨"
            )
        elif index == len(units) - 1:
            unit["framing_guidance"] = (
                _text(terminal.get("ending_guidance"), "")
                or "同一地点补录商品结果近景，自然完成收束"
            )
        elif unit["unit_role"] == "NATURAL_MOTION_RELATION":
            unit["framing_guidance"] = (
                "同一地点重新放置手机，录制一次连续的上半身或拍摄关系变化，"
                "不用重复摆头支撑整段"
            )
        elif unit["unit_role"] == "PRODUCT_DETAIL_RELATION":
            unit["framing_guidance"] = (
                "同一地点补录商品佩戴细节或与人物的清晰位置关系；"
                "沿用本段原有可见事件，不重复上一段核心动作"
            )
        else:
            unit["framing_guidance"] = (
                "同一地点补录商品与当前生活状态的自然关系；"
                "商品仍清楚可辨，不重复核心动作或退到远景"
            )
        unit["category_projection"] = "SMALL_ACCESSORY_MOTION_RETURN_V2"
        result.append(unit)
    return result


def load_item_result(item: Any) -> Dict[str, Any]:
    raw = getattr(item, "result_json", "") or ""
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def load_content_bundle(item: Any) -> Dict[str, Any]:
    raw = getattr(item, "content_bundle_json", "") or ""
    try:
        value = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _selling_argument(item: Any) -> Dict[str, Any]:
    bundle = load_content_bundle(item)
    return _dict(bundle.get("selling_argument"))


def _requires_semantic_display(argument: Dict[str, Any]) -> bool:
    """Whether reviewed operator wording must never enter an operator-facing view.

    Raw wording remains part of the immutable content bundle for lineage only.
    In particular, it must not leak back into Feishu titles or "核心卖点" after
    the central voiceover has already rewritten it respectfully.
    """

    return bool(argument.get("respectful_reframe_required")) or _text(
        argument.get("expression_policy"), ""
    ) == "SEMANTIC_AUTHORITY_NOT_VERBATIM"


def core_selling_point(item: Any, script: Dict[str, Any] | None = None) -> str:
    """Return the safe operator-facing value summary for an exported script.

    `selling_argument_realization_zh` is authored by the central voiceover
    engine.  It is intentionally a short Chinese summary, not a translation
    of the reviewed source wording.  Legacy rows without that field degrade
    to a neutral marker rather than exposing prohibited raw rhetoric.
    """

    bundle = load_content_bundle(item)
    argument = _selling_argument(item)
    script = script or _dict(load_item_result(item).get("script"))
    voice = _dict(script.get("continuous_voiceover"))
    safe_summary = _text(voice.get("selling_argument_realization_zh"), "")
    if safe_summary:
        return safe_summary
    if _requires_semantic_display(argument):
        return "已按自然口播表达"
    proposition = _dict(bundle.get("value_proposition"))
    return _text(
        argument.get("core_value")
        or proposition.get("text")
        or bundle.get("content_mainline")
    )


def build_script_title(item: Any, script: Dict[str, Any]) -> str:
    production = _dict(script.get("production_design"))
    character = _dict(production.get("character"))
    scene = _dict(production.get("scene"))
    person = _text(character.get("identity"), "")
    location = _text(scene.get("location"), "")
    short_scene = location.split("的")[-1] if location else person
    core = core_selling_point(item, script)
    # A legacy sensitive record may not yet carry its safe summary.  Do not
    # turn the neutral migration marker into a noisy title.
    if core == "已按自然口播表达":
        core = ""
    return "｜".join(part for part in (short_scene or person, core) if part) or _text(
        getattr(item, "batch_item_id", "")
    )


def _frozen_mainline_for_item(item: Any) -> Dict[str, Any]:
    """The frozen mainline this delivered script has to be reviewed against.

    Absent for packages predating C2 (and for non-accessory lines), in which
    case the review reports ``NOT_APPLICABLE`` and the delivery text is
    unchanged -- byte for byte -- from what it was before.
    """

    result = load_item_result(item)
    script = _dict(result.get("script"))
    brief = _dict(script.get("video_generation_brief")) or script
    for holder in (
        _dict(brief.get("category_execution_extension")),
        script,
        result,
        _json_dict(getattr(item, "frozen_direction_package_json", "")),
    ):
        contract = holder.get("mixed_mainline_contract")
        if isinstance(contract, dict) and contract:
            return contract
    return {}


def review_delivered_voiceover(script: Dict[str, Any], mainline: Dict[str, Any]) -> Dict[str, Any]:
    """成稿口播的两项核对合成一份结论：三类核对 + 口径兜底。

    Both are *reports*.  Neither rewrites the script: the review's own design
    note says the fix, when one is needed, goes through the single targeted
    revision instead.  Wiring them into delivery is what makes 方案 C3's
    "成稿核对分三类" a produced artefact instead of a test-only helper.
    """

    from core.mixed_voiceover_mainline import (
        CHECK_ATTENTION,
        CHECK_FAIL,
        CHECK_NOT_APPLICABLE,
        CHECK_PASS,
        check_voiceover_target_against_boundary,
        review_voiceover_against_mainline,
    )

    contract = _dict(mainline)
    voice = _dict(_dict(script).get("continuous_voiceover"))
    review = review_voiceover_against_mainline(script, contract)
    boundary = check_voiceover_target_against_boundary(voice=voice, mainline=contract)
    reported = [
        item
        for item in (review.get("status"), boundary.get("status"))
        if item and item != CHECK_NOT_APPLICABLE
    ]
    status = CHECK_NOT_APPLICABLE
    for candidate in (CHECK_FAIL, CHECK_ATTENTION, CHECK_PASS):
        if candidate in reported:
            status = candidate
            break
    return {
        "schema_version": "delivered-voiceover-review-v1",
        "status": status,
        "mainline_review": review,
        "boundary_check": boundary,
    }


def voiceover_review_delivery_lines(review: Dict[str, Any]) -> list:
    """Human-readable summary lines.  None at all without a mainline contract."""

    from core.mixed_voiceover_mainline import CHECK_ATTENTION, CHECK_FAIL, CHECK_NOT_APPLICABLE

    data = _dict(review)
    if data.get("status") in ("", CHECK_NOT_APPLICABLE):
        return []
    boundary = _dict(data.get("boundary_check"))
    mainline_review = _dict(data.get("mainline_review"))
    if boundary.get("status") == CHECK_FAIL:
        hits = "、".join(
            f"{_text(item.get('term'))}（{_text(item.get('scope'))}）"
            for item in (boundary.get("violations") or [])
            if isinstance(item, dict)
        )
        lines = [f"口播口径核对：未通过 —— 成稿说出被禁措辞：{hits or '见机读结论'}"]
    else:
        lines = ["口播口径核对：通过（成稿未出现被禁措辞）"]
    buckets = _dict(mainline_review.get("buckets"))
    details = [
        f"{_text(_dict(bucket).get('requirement'))}：{_text(_dict(bucket).get('detail'))}"
        for bucket in (buckets.get("visible_result"), buckets.get("spec_fact"), buckets.get("suggestion_and_aesthetic"))
        if _dict(bucket)
    ]
    if buckets:
        verdict = "通过" if mainline_review.get("status") == "PASS" else "需注意"
        lines.append(f"主线三类核对：{verdict}｜" + "；".join(details))
    elif mainline_review.get("status") == CHECK_ATTENTION:
        lines.append(f"主线三类核对：需注意 —— {_text(mainline_review.get('reason'))}")
    return lines


def render_complete_production_script(
    *,
    item: Any,
    duration_seconds: float,
) -> str:
    result = load_item_result(item)
    script = _dict(result.get("script"))
    concept = _dict(script.get("script_concept"))
    production = _dict(script.get("production_design"))
    video_brief = _dict(script.get("video_generation_brief"))
    character = _dict(production.get("character"))
    outfit = _dict(production.get("outfit"))
    scene = _dict(production.get("scene"))
    emotion = _dict(production.get("emotion"))
    product_usage = _dict(script.get("product_usage"))
    voice = _dict(script.get("continuous_voiceover"))
    persona_contract = _dict(video_brief.get("persona_selection_contract")) or _dict(
        production.get("persona_selection_contract")
    )
    outfit_contract = _dict(video_brief.get("outfit_selection_contract"))
    outfit_persona_affinity = _dict(
        video_brief.get("outfit_persona_affinity_contract")
    ) or _dict(production.get("outfit_persona_affinity_contract"))
    storyboard = _list(script.get("storyboard"))
    capture_rhythm = _dict(script.get("capture_rhythm_contract")) or _dict(
        _dict(script.get("video_generation_brief")).get("capture_rhythm_contract")
    )
    frozen_package = _json_dict(
        getattr(item, "frozen_direction_package_json", "")
    )
    outfit_scene_affinity = (
        _dict(video_brief.get("outfit_scene_affinity_contract"))
        or _dict(production.get("outfit_scene_affinity_contract"))
        or _dict(script.get("outfit_scene_affinity_contract"))
        or _dict(frozen_package.get("outfit_scene_affinity_contract"))
    )
    capture_units = _list(script.get("capture_units")) or _list(
        _dict(script.get("video_generation_brief")).get("capture_units")
    )

    # 语种标签单一来源：只认 target_language，目标语言正文单独一行（Review R3）。
    from core.mixed_voiceover_mainline import resolve_language_label

    _language_label = resolve_language_label(voice=voice, batch=frozen_package)
    # 成稿口播的两项核对（三类核对 + 口径兜底）。没有冻结主线时不产出任何行，
    # 所以旧包与其它类目的交付文本逐字不变。
    _voiceover_review = review_delivered_voiceover(
        script, _frozen_mainline_for_item(item)
    )

    lines = [
        "【脚本标题】",
        build_script_title(item, script),
        "",
        "【基本信息】",
        f"时长：{float(duration_seconds or 15):g}秒",
        f"结构：{_text(getattr(item, 'macro_family_key', ''))}",
        f"承载方式：{_text(production.get('presentation_mode') or getattr(item, 'carrier_mode', ''))}",
        f"拍摄关系：{_text(production.get('capture_mode'))}",
        f"拍摄节奏：{_text(capture_rhythm.get('profile'))}",
        f"真实拍摄片段：{len(capture_units) if capture_units else 1}段",
        f"核心卖点：{core_selling_point(item, script)}",
        f"钩子类型：{_text(voice.get('hook_id') or getattr(item, 'actual_hook_id', '') or getattr(item, 'requested_hook_id', ''))}",
        f"人物模板：{_text(persona_contract.get('persona_name') or persona_contract.get('persona_id'))}",
        f"穿搭模板：{_text(outfit_contract.get('template_display_name') or outfit_contract.get('template_id') or outfit_contract.get('silhouette_key'))}",
        f"穿搭×场景：{_outfit_scene_match_label(outfit_scene_affinity)}",
        f"人物×穿搭：{_text(outfit_persona_affinity.get('match_status'))}",
        "",
        "【人物设定】",
        f"身份：{_text(character.get('identity'))}",
        f"外貌：{_text(character.get('appearance'))}",
        f"妆发：{_text(character.get('hair_makeup'))}",
        f"表达状态：{_text(character.get('speaking_personality'))}",
        "",
        "【完整穿搭】",
        f"基础穿搭：{_text(outfit.get('base_outfit'))}",
        f"商品角色：{_text(outfit.get('product_role'))}",
        f"配饰道具：{_actual_outfit_accessories(outfit_contract, outfit)}",
        "",
        "【场景设定】",
        f"地点：{_text(scene.get('location'))}",
        f"时刻：{_text(scene.get('moment'))}",
        f"光线：{_text(scene.get('lighting'))}",
        f"背景：{_text(scene.get('background'))}",
        f"手机位置：{_text(scene.get('phone_placement'))}",
        f"人物/商品位置：{_text(scene.get('subject_position'))}",
        f"背景层次：{_text(scene.get('background_depth'))}",
        f"生活痕迹：{_text(scene.get('lived_in_trace'))}",
        "",
        "【人物状态】",
        f"开始：{_text(emotion.get('starting_state'))}",
        f"变化：{_text(emotion.get('natural_change'))}",
        f"结束：{_text(emotion.get('ending_state'))}",
        "",
        "【商品身份锚点】",
        _join(_list(product_usage.get("identity_anchors_preserved"))),
        "",
        "【连续口播】",
        f"目标语言：{_language_label['label'] or '目标语言（未记录）'}",
        f"目标语言正文：{_text(voice.get('target_text'))}",
        f"中文：{_text(voice.get('chinese_translation'))}",
        *voiceover_review_delivery_lines(_voiceover_review),
    ]

    for index, raw_shot in enumerate(storyboard, 1):
        shot = _dict(raw_shot)
        lines.extend(
            [
                "",
                f"【分镜{int(shot.get('shot_no') or index):02d}｜{_text(shot.get('time_range'))}｜{_text(shot.get('narrative_role'))}｜{_text(shot.get('capture_unit_id'))}】",
                f"剪辑关系：{_text(shot.get('edit_before'))}",
                f"画面：{_text(shot.get('visual_content'))}",
                f"动作：{_text(shot.get('character_action'))}",
                f"人物状态：{_text(shot.get('natural_emotion'))}",
                f"机位：{_text(shot.get('camera'))}",
                f"商品锚点：{_join(_list(shot.get('product_anchors_visible')))}",
            ]
        )

    lines.extend(
        [
            "",
            "【统一执行】",
            _text(_dict(script.get("video_generation_brief")).get("instruction"), "保持同一人物、穿搭、商品、场景与连续事件；连续口播不要求逐句绑定单一镜头。"),
        ]
    )
    return "\n".join(lines).strip()


def _render_legacy_video_generation_prompt(*, item: Any, duration_seconds: float) -> str:
    result = load_item_result(item)
    script = _dict(result.get("script"))
    brief = _dict(script.get("video_generation_brief")) or script
    production = _dict(brief.get("production_design"))
    character = _dict(production.get("character"))
    outfit = _dict(production.get("outfit"))
    scene = _dict(production.get("scene"))
    emotion = _dict(production.get("emotion"))
    truth = _dict(brief.get("product_truth"))
    voice = _dict(brief.get("voiceover")) or _dict(script.get("continuous_voiceover"))
    semantic_context = _dict(brief.get("semantic_context"))
    storyboard = _list(brief.get("storyboard")) or _list(script.get("storyboard"))

    lines = [
        "【视频任务】",
        f"竖屏短视频，时长{float(duration_seconds or 15):g}秒。",
        "",
        "【人物与穿搭】",
        f"出镜方式：{_text(production.get('presentation_mode'))}",
        f"人物：{_text(character.get('identity'))}；{_text(character.get('appearance'))}",
        f"妆发：{_text(character.get('hair_makeup'))}",
        f"基础穿搭：{_text(outfit.get('base_outfit'))}",
        f"商品角色：{_text(outfit.get('product_role'))}",
        f"配饰道具：{_text(outfit.get('accessories'))}",
        "",
        "【场景】",
        f"地点：{_text(scene.get('location'))}",
        f"时刻与光线：{_text(scene.get('moment'))}；{_text(scene.get('lighting'))}",
        f"背景：{_text(scene.get('background'))}",
        f"人物状态：{_text(emotion.get('starting_state'))} → {_text(emotion.get('natural_change'))} → {_text(emotion.get('ending_state'))}",
        "",
        "【商品必须保持】",
        _join(_list(truth.get("identity_anchors")) or _list(_dict(script.get("product_usage")).get("identity_anchors_preserved"))),
    ]

    for index, raw_shot in enumerate(storyboard, 1):
        shot = _dict(raw_shot)
        lines.extend(
            [
                "",
                f"【镜头{int(shot.get('shot_no') or index):02d}｜{_text(shot.get('time_range'))}】",
                f"画面：{_text(shot.get('visual_content'))}",
                f"动作：{_text(shot.get('character_action'))}",
                f"自然状态：{_text(shot.get('natural_emotion'))}",
                f"机位：{_text(shot.get('camera'))}",
                f"商品可见：{_join(_list(shot.get('product_anchors_visible')))}",
            ]
        )

    lines.extend(
        [
            "",
            "【连续口播｜必须原样使用目标语言】",
            _text(voice.get("target_text")),
            "",
            "【统一执行】",
            _text(brief.get("instruction"), "保持同一人物、穿搭、商品、场景与连续事件；不要重新设计语义。"),
        ]
    )
    return "\n".join(lines).strip()


def _is_face_free_contract(category_extension: Dict[str, Any]) -> bool:
    """True when the frozen mixed accessory template forbids showing the face."""

    contract = _dict(category_extension.get(MIXED_TEMPLATE_CONTRACT_KEY))
    return bool(
        _text(contract.get("execution_profile"), "")
        == ACCESSORY_MIXED_TEMPLATE_PROFILE
        and _text(contract.get("face_policy"), "").upper() == "NO_FACE"
    )


def _face_free_constraint_lines(category_extension: Dict[str, Any]) -> list:
    """Hard no-face block for the authored mixed accessory template.

    The template's central guarantee is that no shot ever shows the face.  The
    per-shot framing prose already implies it, but nothing in the prompt ever
    *states* it, so a video model is free to fall back to the accessory genre's
    habitual wearer close-up.  Emitting the constraint explicitly closes that
    gap.

    Only the worn modules carry a body-in-frame vocabulary.  A handheld shot is
    hand-and-product and a static shot is product-and-surface, and both ban
    things that are *not* faces (a static shot may not show a hand at all), so
    those bans are stated on their own instead of being folded into "人物只能以
    这些局部入画" -- which is how a bracelet ended up described in ear words.

    Returns ``[]`` for every other profile, so nothing else changes.
    """

    contract = _dict(category_extension.get(MIXED_TEMPLATE_CONTRACT_KEY))
    if not _is_face_free_contract(category_extension):
        return []
    forbidden: list = []
    for unit in contract.get("capture_units") or []:
        if not isinstance(unit, dict):
            continue
        if _text(unit.get("module"), "") not in {"WORN_DETAIL", "WORN_RELATION"}:
            continue
        for value in unit.get("forbidden_framing") or []:
            text = _text(value, "")
            if text and text not in forbidden:
                forbidden.append(text)

    module_bans: list = []
    for unit in contract.get("capture_units") or []:
        if not isinstance(unit, dict):
            continue
        if _text(unit.get("module"), "") not in {
            "HANDHELD_PRODUCT",
            "STATIC_PRODUCT",
        }:
            continue
        for value in unit.get("forbidden_framing") or []:
            text = _text(value, "")
            if text and text not in forbidden and text not in module_bans:
                module_bans.append(text)

    lines = [
        "",
        "【全片不露脸｜硬约束】",
        "任何一镜都不得出现"
        + "、".join(
            forbidden or ["眼睛", "鼻子", "嘴部", "正面全脸", "镜面反射露脸"]
        )
        + "（镜面里的反射同样算露脸）。",
    ]
    allowed = worn_body_framing(contract)
    if allowed:
        lines.append("佩戴类镜头只能以这些局部入画：" + "、".join(allowed) + "。")
    if module_bans:
        lines.append(
            "手持与静物镜头另有模块禁令，不得违反：" + "、".join(module_bans) + "。"
        )
    lines.append(
        "不得把任何一镜改成以人脸为主体的正面构图；"
        "人物入画时只保留上述局部身体关系，商品始终是画面主体。"
    )
    return lines


def _render_ugc_native_video_generation_prompt(*, item: Any, duration_seconds: float) -> str:
    result = load_item_result(item)
    script = _dict(result.get("script"))
    brief = _dict(script.get("video_generation_brief")) or script
    production = _dict(brief.get("production_design")) or _dict(script.get("production_design"))
    character = _dict(production.get("character"))
    outfit = _dict(production.get("outfit"))
    scene = _dict(production.get("scene"))
    life_event = _dict(production.get("life_event"))
    recording_profile = _dict(brief.get("creator_recording_profile")) or _dict(
        production.get("creator_recording_profile")
    )
    recording_context = _dict(brief.get("recording_context")) or _dict(
        production.get("recording_context")
    )
    direct_creator_share = bool(recording_profile.get("enabled")) and _text(
        recording_profile.get("recording_mode"), ""
    ).upper() == "CREATOR_DIRECT_SHARE"
    action_design = _dict(brief.get("action_design")) or _dict(
        production.get("action_execution")
    )
    truth = _dict(brief.get("product_truth"))
    if not truth:
        truth = {
            "identity_anchors": _list(_dict(script.get("product_usage")).get("identity_anchors_preserved")),
            "visible_detail_anchors": [],
        }
    identity_lock = _dict(brief.get("product_identity_lock"))
    category_extension = _dict(brief.get("category_execution_extension"))
    face_free = _is_face_free_contract(category_extension)
    # The frozen mixed montage, when this item is one.  Read once and threaded
    # through: every downstream line that used to be written from the small-
    # accessory role arc has to consult it instead.
    mixed_contract = frozen_mixed_contract(category_extension)
    mixed_mode = (
        _text(mixed_contract.get("execution_profile"))
        == ACCESSORY_MIXED_TEMPLATE_PROFILE
    )
    accessory_brief = _upgrade_small_accessory_brief_for_render(
        category_extension=category_extension,
        accessory_brief=_dict(brief.get("accessory_execution_brief")),
        production=production,
        item=item,
    )
    persona_contract = _dict(brief.get("persona_selection_contract")) or _dict(
        production.get("persona_selection_contract")
    )
    visual_execution = _dict(brief.get("visual_execution_contract"))
    visual_schema = _text(visual_execution.get("schema_version"), "")
    visual_finish_enabled = visual_schema in {
        "visual-execution-contract-v2",
        "visual-execution-contract-v3-saliency",
        "visual-execution-contract-v4-wearable-saliency",
    }
    styling_context = _dict(visual_execution.get("styling_context"))
    visual_scene_context = _dict(visual_execution.get("scene_context"))
    visual_saliency = _dict(visual_execution.get("visual_saliency"))
    outfit_prompt_projection = _dict(brief.get("outfit_prompt_projection"))
    opening_scene_projection = _dict(
        visual_execution.get("opening_scene_projection")
    )
    category_profile = _dict(category_extension.get("profile"))
    # Historical accessory briefs may predate canonical_product_type even
    # though their category extension already knows the registered subtype.
    # Enrich a local copy only; rendering must not mutate stored batch data.
    identity_truth = dict(truth)
    canonical_type = _text(identity_truth.get("canonical_product_type"), "")
    if not canonical_type:
        canonical_type = _text(category_profile.get("product_subtype"), "")
        if canonical_type:
            identity_truth["canonical_product_type"] = canonical_type
    # Rendering is deterministic.  Compare against the compiler's current
    # version instead of treating garment V2 as universally current: scarves
    # use a separate V3 lock and old V2 prompts must be upgraded on read.
    rebuilt_lock = build_product_identity_lock(identity_truth)
    if (
        rebuilt_lock.get("must_preserve")
        and identity_lock.get("compiler_version")
        != rebuilt_lock.get("compiler_version")
    ):
        identity_lock = rebuilt_lock
    must_preserve = _list(identity_lock.get("must_preserve"))
    if not must_preserve:
        must_preserve = _list(truth.get("identity_anchors"))
    must_preserve = [
        _video_safe_closure_text(value, identity_lock)
        for value in must_preserve
    ]
    critical_details = _list(identity_lock.get("critical_visible_details")) or _list(
        truth.get("visible_detail_anchors")
    )
    critical_details = [
        _video_safe_closure_text(value, identity_lock)
        for value in critical_details
    ]
    must_not_change = _list(identity_lock.get("must_not_change"))
    voice = _dict(brief.get("voiceover")) or _dict(script.get("continuous_voiceover"))
    semantic_context = _dict(brief.get("semantic_context"))
    storyboard = _list(brief.get("storyboard")) or _list(script.get("storyboard"))
    capture_mode = _capture_mode(brief, production)
    capture_rhythm = _capture_rhythm_contract(
        brief,
        script,
        capture_mode=capture_mode,
        storyboard=storyboard,
    )
    storyboard, compiled_capture_units = compile_capture_units(
        storyboard,
        capture_rhythm,
    )
    capture_units = _list(brief.get("capture_units"))
    if _text(capture_rhythm.get("profile"), "").upper() == CAPTURE_RHYTHM_MULTICLIP:
        # Unit ids are deterministic authority.  Stored metadata may be stale
        # after a profile change, so rebuild it from the current storyboard.
        capture_units = compiled_capture_units
        if not isinstance(
            capture_rhythm.get("category_rollout_contract"), dict
        ):
            # Backward compatibility for old SCRIPT_READY rows. New rows have
            # already frozen the same motion relation before blueprint and
            # must not be projected a second time during rendering.
            capture_units = _apply_small_accessory_capture_projection(
                capture_units,
                accessory_brief=accessory_brief,
            )
    multiclip_enabled = (
        _text(capture_rhythm.get("profile"), "").upper()
        == CAPTURE_RHYTHM_MULTICLIP
    )
    concept = _dict(script.get("script_concept"))
    event_text = _text(
        recording_context.get("recording_motivation")
        if direct_creator_share
        else life_event.get("continuous_event")
        or life_event.get("motivation")
        or concept.get("one_sentence_idea"),
        "",
    )

    lines = [
        "【视频任务】",
        f"竖屏手机短视频，时长{float(duration_seconds or 15):g}秒。",
        "",
        "【商品身份锁｜最高优先级】",
        (
            "商品外观以参考图为唯一准则；严格保持商品身份与关键细节，"
            "人物造型、真实场景和手机原生质感同时完整成立。"
            if visual_finish_enabled
            else "商品外观以参考图为唯一准则；商品一致性优先于人物美感、场景氛围和镜头效果。"
        ),
    ]
    # 同 §整片语义主线：两行各自判定，缩窄后只剩余一行时不能整块丢掉。
    narrative_context = _text(semantic_context.get("primary_narrative_context"), "")
    core_buying_reason = _text(semantic_context.get("core_buying_reason"), "")
    if narrative_context or core_buying_reason:
        lines.extend(["", "【整片语义主线｜不做逐句逐镜绑定】"])
        if narrative_context:
            lines.append("主消费情境：" + narrative_context)
        if core_buying_reason:
            lines.append("核心购买理由：" + core_buying_reason)
        lines.append(
            "画面、人物和口播保持在同一个消费世界；不要求每句口播由当前镜头证明，"
            "也不得让背景场景改写商品的核心购买理由。"
        )
    if visual_saliency:
        lines.append(
            "参考图只负责商品颜色、图案、形状和结构；参考图的整体曝光、滤镜、"
            "背景色调和其中人物均不是本条画面风格权威。"
        )
    if must_preserve:
        lines.append(f"必须保持：{_join(must_preserve)}")
    if critical_details:
        lines.append(f"关键可见细节：{_join(critical_details[:4])}")
    if must_not_change:
        lines.extend(["", "【商品负向约束】", _join(must_not_change)])
    face_free_lines = _face_free_constraint_lines(category_extension)
    if face_free_lines:
        lines.extend(face_free_lines)
    if visual_saliency:
        exposure = _dict(visual_saliency.get("exposure"))
        separation = _dict(visual_saliency.get("separation"))
        opening_focus = _dict(visual_saliency.get("opening_focus"))
        saliency_lines = ["", "【画面优先级｜明亮原生与商品分离】"]
        if _text(exposure.get("guidance"), ""):
            saliency_lines.append("曝光：" + _text(exposure.get("guidance"), ""))
        if _text(separation.get("outfit_guidance"), ""):
            saliency_lines.append(
                "商品与穿搭：" + _text(separation.get("outfit_guidance"), "")
            )
        if _text(separation.get("background_guidance"), ""):
            saliency_lines.append(
                "商品与背景：" + _text(separation.get("background_guidance"), "")
            )
        if not direct_creator_share:
            opening_parts = [
                _text(opening_focus.get("guidance"), ""),
                _text(opening_focus.get("natural_change"), ""),
            ]
            opening_text = "".join(part for part in opening_parts if part)
            if opening_text:
                saliency_lines.append("首镜：" + opening_text)
        lines.extend(saliency_lines)
    if opening_scene_projection and not direct_creator_share:
        projection_lines = ["", "【首帧/第一拍摄单元场景投影｜不改后续场景】"]
        location_identity = _text(
            opening_scene_projection.get("location_identity"), ""
        )
        if location_identity:
            projection_lines.append("地点身份：" + location_identity)
        for label, key in (
            ("背景锚点", "opening_background_anchor"),
            ("主体背后", "subject_backdrop_guidance"),
            ("密集元素位置", "dense_elements_placement"),
            ("生活痕迹", "lived_in_trace_guidance"),
        ):
            value = _text(opening_scene_projection.get(key), "")
            if value:
                projection_lines.append(f"{label}：{value}")
        projection_lines.append(
            "只约束首帧和第一段构图；后续片段仍按完整冻结场景执行。"
        )
        lines.extend(projection_lines)
    if _text(persona_contract.get("availability")) == "AVAILABLE":
        persona_projection = _dict(persona_contract.get("script_projection"))
        lines.extend(
            [
                "",
                "【人物身份锁｜与商品参考分权】",
                f"人物模板ID：{_text(persona_contract.get('persona_id'))}",
                (
                    "人物身份、年龄感、体型、发型和自然肤质以人物模板参考为准；"
                    "商品参考图只决定商品外观，其中模特、滤镜、姿态和背景均无人物权威。"
                ),
                f"人物设定：{_text(persona_projection.get('identity'))}；{_text(persona_projection.get('appearance'))}",
                (
                    "人物比例："
                    + _text(_dict(persona_contract.get("identity_lock")).get("body_proportion_text"))
                    if _text(_dict(persona_contract.get("identity_lock")).get("body_proportion_text"))
                    else "人物比例：服从人物模板参考，不从商品参考图复制"
                ),
                f"人物妆发：{_text(persona_projection.get('hair_makeup'))}",
                (
                    "只允许随当前生活时刻产生自然表情、视线和小动作；"
                    "不得重新设计脸、年龄、体型、发型或妆容等级。"
                ),
                f"参考策略：{_text(persona_contract.get('reference_strategy'))}",
            ]
        )

    if accessory_brief:
        accessory_lines = ["", "【配饰佩戴与展示关系】"]
        product_relation = _text(accessory_brief.get("product_relation"), "")
        required_result = _text(
            accessory_brief.get("required_visible_result"), ""
        )
        interaction_limit = _list(accessory_brief.get("interaction_limit"))
        optional_interactions = _list(
            accessory_brief.get("optional_simple_interactions")
        )
        selected_action = _dict(accessory_brief.get("selected_action_design"))
        wear_state = _dict(accessory_brief.get("wear_state_contract"))
        hand_guard = _dict(accessory_brief.get("hand_anatomy_guard"))
        product_prominence = _dict(
            accessory_brief.get("product_prominence_contract")
        )
        if not selected_action and not optional_interactions and category_extension:
            # Deterministic read-time compatibility for ready scripts created
            # before optional lightweight interactions were added.  This does
            # not rewrite the script or invent an action sequence; it only
            # exposes the registered category adapter's current soft options.
            carrier_execution = resolve_category_carrier_execution(
                category_extension,
                presentation_mode=_text(
                    production.get("presentation_mode")
                    or getattr(item, "carrier_mode", ""),
                    "",
                ),
            )
            optional_interactions = _list(
                carrier_execution.get("optional_simple_interactions")
            )
        identity_focus = _list(accessory_brief.get("identity_focus"))
        identity_authority_guidance = _text(
            accessory_brief.get("identity_authority_guidance"), ""
        )
        claim_boundary = _text(accessory_brief.get("claim_boundary"), "")
        accessory_capture_relationship = _text(
            accessory_brief.get("capture_relationship"), ""
        )
        if product_relation and _text(wear_state.get("initial_state")) != "IN_PROGRESS":
            accessory_lines.append(f"商品关系：{product_relation}")
        if _text(wear_state.get("continuity_rule_zh"), ""):
            accessory_lines.append(
                "佩戴连续性：" + _text(wear_state.get("continuity_rule_zh"), "")
            )
        if required_result:
            accessory_lines.append(f"必要结果：{required_result}")
        if product_prominence and not mixed_mode:
            prominence_parts = [
                _text(product_prominence.get("opening_guidance"), ""),
                _text(product_prominence.get("context_guidance"), ""),
            ]
            terminal = _dict(product_prominence.get("terminal_visibility"))
            if _text(terminal.get("ending_guidance"), ""):
                prominence_parts.append(
                    _text(terminal.get("ending_guidance"), "")
                )
            prominence_text = "；".join(
                part for part in prominence_parts if part
            )
            if prominence_text:
                accessory_lines.append(
                    "商品观察尺度："
                    + prominence_text
                    + "。只调整兼容内容段的景别，不改变结构、佩戴状态或动作主线。"
                )
        if mixed_mode:
            # The prominence contract above describes a *worn-only* film ("首个
            # 核心展示段优先使用后脑发饰区域近景", "镜面必须裁到颈肩范围") and
            # contradicts a montage that opens on a still life and bans every
            # mirror. The frozen per-module range is the same information,
            # stated for the film that will actually be shot.
            accessory_lines.extend(module_framing_legend_lines(mixed_contract))
        if interaction_limit:
            accessory_lines.append(f"交互边界：{_join(interaction_limit)}")
        if selected_action:
            accessory_lines.append(
                "按下方每段自己的执行行执行；整片只锁商品身份、穿戴连续与直接剪切"
                if mixed_mode
                else "核心互动已冻结，按下方“本条动作主线”执行一次并服从交互边界"
            )
        elif optional_interactions:
            accessory_lines.append(
                "可选轻互动：最多自然采用一个，也可以不用；"
                + _join(optional_interactions)
            )
        if identity_focus:
            accessory_lines.append(f"身份重点：{_join(identity_focus)}")
        if identity_authority_guidance:
            accessory_lines.append(f"身份授权：{identity_authority_guidance}")
        if claim_boundary:
            accessory_lines.append(f"表达边界：{claim_boundary}")
        if accessory_capture_relationship:
            # 混合模式：这一行是旧的"后脑优先用镜面反射"口径，与本片自己的
            # NO_FACE 禁令（镜面反射露脸）直接冲突。按句删掉点名的取景方式，
            # 保留仍然成立的那半句（"不要求人物正对前置镜头"）。
            text = accessory_capture_relationship
            if mixed_mode:
                banned = _mixed_banned_framing_terms(mixed_contract)
                text = "；".join(
                    clause
                    for clause in text.split("；")
                    if clause.strip()
                    and not any(term in clause for term in banned)
                )
            if _text(text):
                accessory_lines.append(f"配饰拍摄位置关系：{text}")
        if _text(hand_guard.get("guidance_zh"), ""):
            accessory_lines.append(
                "手部结构：" + _text(hand_guard.get("guidance_zh"), "")
            )
        lines.extend(accessory_lines)

    if action_design and not direct_creator_share and not mixed_mode:
        action_lines = [
            "",
            "【本条动作主线｜只执行这一条】",
            f"动作类型：{_text(action_design.get('primary_action_mode'))}",
            f"开始状态：{_text(action_design.get('start_state'))}",
            f"核心动作：{_text(action_design.get('core_action'))}",
            f"完成状态：{_text(action_design.get('end_state'))}",
        ]
        if _text(action_design.get("motion_scope"), "") == "ONE_CONTINUOUS_CHANGE":
            action_lines.append(
                "动态重点：开头在轻微自然变化中看清商品，中段执行上述核心动作，"
                "结尾在小幅变化或重新构图中回到商品，只在最后一瞬自然收住。"
            )
        if _text(action_design.get("supporting_scene_action"), ""):
            if (
                _text(
                    _dict(
                        accessory_brief.get("product_prominence_contract")
                    ).get("sequence_policy"),
                    "",
                ).upper()
                == "PRODUCT_OPENING_TO_MOTION_TO_PRODUCT_RETURN"
            ):
                action_lines.append(
                    "辅助生活衔接：只保留同一地点内的自然状态；最后一段不离场，"
                    "结尾服从商品回收近景。"
                )
            else:
                action_lines.append(
                    "辅助生活衔接："
                    + _text(action_design.get("supporting_scene_action"), "")
                    + "；只作自然衔接，不与核心动作叠成动作清单。"
                )
        lines.extend(action_lines)

    if mixed_mode:
        # 一次拍摄里只有一条旧"核心动作"，而混合片段的第 2/3 段是手持与静物。
        # 把那条动作整片派发，等于让静物镜去演佩戴动作。所以混合模式不发布
        # 整片核心动作：每段只服从自己那一行。
        mixed_roles = [
            _text(_dict(unit).get("module"))
            for unit in _list(mixed_contract.get("capture_units"))
        ]
        lines.extend(
            [
                "",
                "【本条动作主线｜按每段自己的模块执行】",
                (
                    "整片只有一条主线：同一商品、同一创作者、同一地点，"
                    f"分成{len([role for role in mixed_roles if role])}段直接剪切。"
                ),
                (
                    "每段的动作、构图、商品状态和身体范围由该段自己那一行给出；"
                    "不得把某一段的动作套用到其他段，也不得为凑连续性补拍摘戴过程。"
                ),
                (
                    "跨段是普通直接剪切，可以从已经拿稳切到已经静置、已经佩戴；"
                    "不要求拍摄摘下、戴上、扣合或夹取的中间过程。"
                ),
            ]
        )

    if direct_creator_share:
        preset = normalize_creator_capture_preset(recording_profile)
        continuity_text = (
            "首段可以让商品单独出现；人物穿上后始终保持穿着，后续细节都在身上拍。"
            if preset == "PRODUCT_FIRST_THEN_WORN"
            else "人物从开头已经穿好商品并全程保持穿着；不重复穿脱。"
        )
        capture_lines = [
            "",
            "【拍摄方式｜达人直接分享】",
            (
                f"同一创作者在同一地点用自己的手机分{len(capture_units)}段直接分享；"
                "片段间普通直接剪切，可自然使用固定、手持或镜面关系，不规定比例。"
            ),
            continuity_text,
            "不要生活小剧场、逐项检查或广告式走位。",
        ]
    else:
        capture_lines = [
            "",
            (
                "【拍摄方式｜UGC_NATIVE_V2_MULTICLIP】"
                if multiclip_enabled
                else "【拍摄方式｜UGC_NATIVE_V1】"
            ),
            UGC_NATIVE_POSITIVE,
        ]
    if multiclip_enabled and not direct_creator_share:
        capture_lines.extend(
            [
                "",
                "【拍摄节奏｜NATIVE_MULTI_CLIP_V1】",
                CREATOR_MULTICLIP_POSITIVE,
                (
                    f"实际剪辑目标：{len(capture_units)}个独立可见片段；"
                    f"手机布置预算：{int(capture_rhythm.get('camera_setup_count') or 2)}种。"
                    "同一布置可以录制不同内容时刻，但成片必须发生真实直接剪切。"
                ),
                f"拍摄边界：{CREATOR_MULTICLIP_NEGATIVE}",
            ]
        )
    elif capture_mode == CAPTURE_MODE_CREATOR_SELF_SHOT and not direct_creator_share:
        capture_lines.extend(
            [
                "",
                "【拍摄关系｜CREATOR_SELF_SHOT】",
                CREATOR_SELF_SHOT_POSITIVE,
                f"拍摄关系边界：{CREATOR_SELF_SHOT_NEGATIVE}",
            ]
        )
    elif not direct_creator_share:
        capture_lines.append(f"风格负向：{UGC_NATIVE_NEGATIVE}")
    # The face-free framing vocabulary is read from the frozen contract rather
    # than written out as ear wording: this sentence used to tell a bracelet to
    # put "耳侧、耳廓、颈侧" in frame.
    face_free_framing = "、".join(
        worn_body_framing(_dict(category_extension.get(MIXED_TEMPLATE_CONTRACT_KEY)))
    )
    lines.extend(
        [
            *capture_lines,
            "",
            "【人物、穿搭与生活场景】",
            (
                f"出镜方式：{_text(production.get('presentation_mode'))}"
                "（本片全片不露脸，人物只以"
                + (face_free_framing or "本镜自己的局部身体关系")
                + "等局部入画）"
                if face_free
                else f"出镜方式：{_text(production.get('presentation_mode'))}"
            ),
            (
                f"拍摄关系：{capture_mode or '普通手机商品记录'}"
                "（同一个人用同一部手机分多段录制后直接剪切成片；任何一镜都不以人脸为主体）"
                if face_free
                else f"拍摄关系：{capture_mode or '普通手机商品记录'}"
            ),
            f"人物：{_text(character.get('identity'))}；{_text(character.get('appearance'))}",
            f"妆发：{_text(character.get('hair_makeup'))}",
            f"基础穿搭：{_text(outfit.get('base_outfit'))}",
            *(
                [
                    "冻结穿搭配方："
                    + _text(outfit_prompt_projection.get("frozen_outfit"), "")
                    + "；这是本条唯一穿搭配方，不得改款或另选单品。"
                ]
                if _text(production.get("presentation_mode"), "").upper() != "STATIC_PRODUCT"
                and _text(outfit_prompt_projection.get("frozen_outfit"), "")
                else []
            ),
            f"商品角色：{_video_safe_closure_text(outfit.get('product_role'), identity_lock)}",
            f"配饰道具：{_text(outfit.get('accessories'))}",
            f"地点与时刻：{_text(scene.get('location'))}；{_text(scene.get('moment'))}",
            f"光线：{_compact_native_description(scene.get('lighting'), max_segments=2, max_chars=90) or '只使用现场已有自然光或普通室内光，不重新设计商业布光。'}",
            f"{'基础手机位置' if multiclip_enabled else '手机位置'}：{_compact_native_description(scene.get('phone_placement'), max_segments=2, max_chars=100) or ('第一段使用普通手机构图，后续片段可在同一小片区域重新放置' if multiclip_enabled else '保持一个固定的普通手机视角')}",
            f"人物/商品位置：{_compact_native_description(scene.get('subject_position'), max_segments=2, max_chars=100)}",
            f"背景层次：{_compact_native_description(scene.get('background_depth'), max_segments=2, max_chars=100)}",
            f"生活痕迹：{_compact_native_description(scene.get('lived_in_trace'), max_segments=1, max_chars=60) or '保留一处自然使用痕迹'}",
            f"现场背景：{_compact_native_description(scene.get('background'), max_segments=3, max_chars=160) or '保留真实生活环境'}",
        ]
    )
    if visual_finish_enabled:
        aesthetic = _list(visual_scene_context.get("aesthetic_anchors"))
        scene_recipe = _dict(visual_scene_context.get("visual_scene_recipe"))
        visual_finish_lines = [
            "",
            "【视觉完成度｜NATIVE_STYLED】",
            (
                "保持真实个人账号的手机拍摄质感，但人物不是临时套上素衣的展示模特："
                "妆发、穿搭轮廓与配色已经自然完成；精致感来自真实造型和空间本身，"
                "不是磨皮、影棚布光或电影运镜。"
            ),
        ]
        if _text(styling_context.get("finish_direction"), ""):
            visual_finish_lines.append(
                f"造型完成度：{_text(styling_context.get('finish_direction'), '')}"
            )
        if _text(styling_context.get("supporting_elements"), ""):
            visual_finish_lines.append(
                f"辅助元素：{_text(styling_context.get('supporting_elements'), '')}"
            )
        if _text(styling_context.get("grooming_direction"), ""):
            visual_finish_lines.append(
                f"人物妆发：{_text(styling_context.get('grooming_direction'), '')}"
            )
        if aesthetic:
            visual_finish_lines.append(f"场景审美锚点：{_join(aesthetic)}")
        recipe_parts = []
        recipe_labels = (
            ("空间", "space_relationship"),
            ("材质与色调", "material_palette"),
            ("光线质感", "lighting_texture"),
            ("生活痕迹", "lived_in_detail"),
        )
        for label, key in recipe_labels:
            value = _compact_native_description(
                scene_recipe.get(key), max_segments=2, max_chars=90
            )
            if value:
                recipe_parts.append(f"{label}：{value}")
        if recipe_parts:
            visual_finish_lines.append("场景来源配方（软参考）：" + "；".join(recipe_parts))
        if _text(visual_scene_context.get("instruction"), ""):
            visual_finish_lines.append(
                f"场景关系：{_text(visual_scene_context.get('instruction'), '')}"
            )
        lines.extend(visual_finish_lines)
    if event_text and not direct_creator_share and (capture_mode != CAPTURE_MODE_CREATOR_SELF_SHOT or visual_finish_enabled):
        lines.append(
            f"连续生活事件：{_compact_native_description(event_text, max_segments=2, max_chars=90)}"
        )

    if multiclip_enabled and not direct_creator_share:
        public_setup = (
            _text(capture_rhythm.get("capture_setup_mode"), "").upper()
            == "ONE_PUBLIC_PHONE_POSITION_PLUS_HANDHELD_CUTAWAY"
        )
        # "手持自拍" describes the legacy wearer-on-camera cutaway.  A face-free
        # contract must ask for a plain handheld pick-up shot instead.
        cutaway_shot = (
            "手持补录或商品切片"
            if _is_face_free_contract(category_extension)
            else "手持自拍或商品切片"
        )
        lines.extend(
            [
                (
                    f"拍摄单元：以下{len(capture_units)}段是分别录制的普通手机素材，片段间直接剪切；"
                    "不是一个长镜头里的数字裁切、连续变焦或人物反复走近走远。"
                ),
                (
                    "公共场景拍摄关系：只使用一个自然可解释的固定手机位置，再补一段"
                    + cutaway_shot
                    + "；这两种布置可录制多个不同内容时刻，不得因此合并成两个长镜头；"
                    "不要在公共空间反复架设、搬动无人值守手机。"
                    if public_setup else
                    "连续性：只锁同一人物、商品、穿搭、地点、时刻、手机和生活状态；不锁死手机位置与景别。"
                ),
            ]
        )
    elif capture_mode == CAPTURE_MODE_CREATOR_SELF_SHOT and not direct_creator_share:
        lines.extend(
            [
                "分享方式：创作者主要看向自己的手机镜头说话；以下结构只控制内容推进，不代表切换摄影机位。",
                "画面变化：按本条动作主线完成一次连续变化；允许同一动作跨两个内容段，不要把每个结构段拍成独立广告镜头。",
            ]
        )

    prompt_storyboard = (
        _capture_unit_passages(
            storyboard,
            capture_units,
            accessory_brief=accessory_brief,
            action_design=action_design,
            mixed_contract=mixed_contract,
        )
        if multiclip_enabled
        else _creator_content_moments(storyboard)
        if capture_mode == CAPTURE_MODE_CREATOR_SELF_SHOT
        else storyboard
    )
    for index, raw_shot in enumerate(prompt_storyboard, 1):
        shot = _dict(raw_shot)
        compiled_clip_passage = bool(shot.get("compiled_clip_passage"))
        character_action = _video_safe_closure_text(
            shot.get("character_action"), identity_lock
        )
        if (
            shot.get("core_action_projection_applied")
            or shot.get("active_boundary_projection_applied")
        ):
            # These strings are already deterministic category projections.
            # A second punctuation-based compaction would remove the latter
            # half of the movement and recreate a static opening or ending.
            rendered_character_action = _naturalize_text(character_action)[:180]
        elif compiled_clip_passage:
            rendered_character_action = _naturalize_text(character_action)[:140]
        else:
            rendered_character_action = _compact_native_description(
                character_action, max_segments=2, max_chars=70
            )
        safe_visual = _video_safe_closure_text(
            shot.get("visual_content"), identity_lock
        )
        rendered_visual = (
            _naturalize_text(safe_visual)[:210]
            if compiled_clip_passage
            else _compact_native_description(
                safe_visual, max_segments=2, max_chars=110
            )
        )
        camera = (
            _text(shot.get("framing_guidance"), "")
            if multiclip_enabled
            else ""
            if capture_mode == CAPTURE_MODE_CREATOR_SELF_SHOT
            else _compact_camera(shot.get("camera"))
        )
        if multiclip_enabled and index > 1:
            lines.extend(["", "【直接剪切｜开始另一段独立手机素材】"])
        lines.extend(
            [
                "",
                f"【{'拍摄片段' if multiclip_enabled else '连续内容段' if capture_mode == CAPTURE_MODE_CREATOR_SELF_SHOT else '片段'}{index:02d}｜{_text(shot.get('time_range'))}｜{_text(shot.get('structure_role') or shot.get('narrative_role') or shot.get('unit_role'))}】",
                f"画面事件：{rendered_visual}",
                f"人物动作：{rendered_character_action}",
            ]
        )
        if (
            multiclip_enabled
            and not direct_creator_share
            and _text(shot.get("observable_change_job"), "")
        ):
            lines.append(
                "本段相对上一段的新信息："
                + _text(shot.get("observable_change_job"), "")
            )
        physical_unit_role = _text(shot.get("unit_role"), "")
        structure_role = _text(shot.get("structure_role"), "")
        # 「角色说明」在混合模式下由本镜模块派生。旧 unit_role 是按位置索引的
        # 内部叙事标签，把它写进提示词等于把"这是中段所以要演佩戴动作"的旧弧
        # 又请回现场。审计时仍可从 ``legacy_role`` 读到它。
        execution_role = _text(shot.get("execution_role"), "")
        if execution_role:
            lines.append(f"商品执行关系：{execution_role}")
        elif (
            multiclip_enabled
            and physical_unit_role
            and structure_role
            and physical_unit_role != structure_role
        ):
            lines.append(f"商品执行关系：{physical_unit_role}")
        gaze_target = _text(shot.get("gaze_target"), "")
        natural_reaction = _text(
            shot.get("micro_reaction") or shot.get("natural_reaction"), ""
        )
        if gaze_target and not direct_creator_share:
            lines.append(f"视线关系：{gaze_target}")
        if natural_reaction and not direct_creator_share:
            lines.append(f"自然反应：{natural_reaction}")
        if camera:
            lines.append(f"{'本段手机构图' if multiclip_enabled else '手机机位'}：{camera}")
        anchors = _list(shot.get("product_anchors_visible"))
        if anchors:
            safe_anchors = [
                _video_safe_closure_text(anchor, identity_lock)
                for anchor in anchors
            ]
            lines.append(f"商品必须可见：{_join(safe_anchors)}")

    lines.extend(
        [
            "",
            "【连续口播｜必须原样使用目标语言】",
            _text(voice.get("target_text")),
            "",
            "【统一执行】",
            (
                "商品身份和穿戴连续优先；按以上片段直接剪切，其余动作与表情自然即可。"
                if direct_creator_share
                else "第一优先保持商品身份、佩戴状态、人物肢体和穿搭连续；第二优先完整执行以上独立可见片段并真实直接剪切；第三优先卖点关系与原生手机可行性。发生冲突时先简化场景陈设和人物表演，不得合并片段或退回一镜到底。"
                if multiclip_enabled
                else "保持同一创作者、商品、穿搭、场景和手机视角；结构只改变分享内容，不建立摄影团队。商品一致性优先于场景美感和镜头效果。"
                if capture_mode == CAPTURE_MODE_CREATOR_SELF_SHOT
                else "保持同一人物、商品、穿搭、场景和连续事件；商品一致性优先于场景美感和镜头效果。"
            ),
        ]
    )
    return "\n".join(lines).strip()


def render_video_generation_prompt(*, item: Any, duration_seconds: float) -> str:
    """Rendered video prompt.  Byte-identical to before the compaction report."""
    return render_video_generation_prompt_report(
        item=item, duration_seconds=duration_seconds
    ).text


def _frozen_mixed_contract_for_item(item: Any) -> Dict[str, Any]:
    """The frozen montage a delivered prompt has to be audited against."""

    result = load_item_result(item)
    script = _dict(result.get("script"))
    brief = _dict(script.get("video_generation_brief")) or script
    for holder in (
        _dict(brief.get("category_execution_extension")),
        script,
        result,
        _json_dict(getattr(item, "frozen_direction_package_json", "")),
    ):
        contract = frozen_mixed_contract(holder)
        if contract:
            return contract
    return {}


def _with_necklace_execution_audit(
    validation: Dict[str, Any],
    text: str,
    contract: Dict[str, Any],
    *,
    renderer_version: str,
) -> Dict[str, Any]:
    """Fold the necklace V1 audit into the shared execution audit.

    Section 8's necklace assertions are checked here -- on the *same delivered
    string*, at the same stage, as the shared audit.  A non-necklace contract
    returns the shared report *unchanged*, so no other category's validation
    payload (or its ``prompt_hash``) moves at all.

    Never raises: an audit is bookkeeping, and bookkeeping must not turn a
    finished film into a failure.
    """

    try:
        from core.necklace_mixed_profile import (
            AUDIT_NOT_APPLICABLE,
            audit_necklace_final_prompt,
            merge_prompt_audits,
        )

        necklace = audit_necklace_final_prompt(
            text, contract, renderer_version=renderer_version
        )
    except Exception:  # noqa: BLE001 - bookkeeping must not fail an item
        return validation
    if _text(_dict(necklace).get("status")) == AUDIT_NOT_APPLICABLE:
        return validation
    return merge_prompt_audits(validation, necklace)


def render_video_generation_prompt_checked(
    *, item: Any, duration_seconds: float
) -> Dict[str, Any]:
    """The final prompt *plus* the execution audit that binds it.

    The audit runs on the text that is actually delivered — after splicing and
    after compaction — because R1 lived in exactly that last step and every
    earlier check ran on the frozen contract instead.

    ``render_validation`` is bound to the delivered text through ``prompt_hash``
    and ``renderer_version``.  A re-render that changes the prompt is a
    different object and has to be re-checked; it cannot inherit an earlier
    ``PASS``.
    """

    result = load_item_result(item)
    script = _dict(result.get("script"))
    brief = _dict(script.get("video_generation_brief")) or script
    if _video_prompt_profile(brief) == LEGACY_PROFILE:
        text = _render_legacy_video_generation_prompt(
            item=item, duration_seconds=duration_seconds
        )
        report = _unchanged_report(text)
        pre = text
    else:
        pre = _render_ugc_native_video_generation_prompt(
            item=item, duration_seconds=duration_seconds
        )
        report = compact_video_prompt_report(pre)
        text = report.text

    contract = _frozen_mixed_contract_for_item(item)
    storyboard = _list(brief.get("storyboard")) or _list(script.get("storyboard"))
    validation = audit_mixed_final_execution(
        text,
        contract,
        storyboard=storyboard,
        renderer_version=SCRIPT_RENDERER_VERSION,
        pre_compaction_prompt=pre,
    )
    # Section 8: the necklace promises are audited on the delivered text as well,
    # in the same place and on the same string.  A non-necklace contract leaves
    # the shared report untouched.
    validation = _with_necklace_execution_audit(
        validation, text, contract, renderer_version=SCRIPT_RENDERER_VERSION
    )
    return {"text": text, "report": report, "render_validation": validation}


class _ResultProxy:
    """A view of ``item`` whose ``result_json`` is a not-yet-persisted result.

    The audit has to run against the script that is *about to be stored*.  At
    that moment ``item.result_json`` still holds the previous state (usually the
    planning-time payload), so rendering from ``item`` directly would audit an
    empty shell and hand back a ``PASS`` that says nothing about the delivered
    text.  Only ``result_json`` is overridden; every other attribute is passed
    through, so no easily-stale field list has to be maintained here.
    """

    def __init__(self, item: Any, result: Dict[str, Any]) -> None:
        object.__setattr__(self, "_item", item)
        object.__setattr__(
            self,
            "_result_json",
            json.dumps(result, ensure_ascii=False, default=str),
        )

    def __getattr__(self, name: str) -> Any:
        if name == "result_json":
            return object.__getattribute__(self, "_result_json")
        return getattr(object.__getattribute__(self, "_item"), name)


def render_validation_blocks_delivery(validation: Any) -> bool:
    """``True`` iff the audit found a deterministic execution conflict.

    Only ``FAIL`` blocks.  ``PASS`` and ``NOT_APPLICABLE`` (legacy / non-montage)
    do not, and neither does ``AUDIT_ERROR``: our own bookkeeping must never be
    the reason real content stops shipping.

    A blocked item is deliberately **not** ``SCRIPT_FAILED``.  A render conflict
    is reproducible offline and fixable by re-rendering, while
    ``PLANNED``/``SCRIPT_FAILED`` are exactly the statuses ``resume`` retries —
    so routing it through failure would buy a paid model call that cannot change
    a deterministic template projection.
    """

    return _text(_dict(validation).get("status")) == "FAIL"


def render_validation_block_reason(validation: Any, *, limit: int = 6) -> str:
    """One operator-facing line: which shot contradicted which boundary."""

    validation = _dict(validation)
    if not render_validation_blocks_delivery(validation):
        return ""
    issues = _list(validation.get("issues"))
    parts = []
    for issue in issues[:limit]:
        issue = _dict(issue)
        parts.append(
            f"镜{_text(issue.get('shot_id'), '?')}"
            f"[{_text(issue.get('module'), '?')}]"
            f"{_text(issue.get('code'))}"
            f"（来源 {_text(issue.get('source'), '?')}）"
        )
    suffix = f"；共 {len(issues)} 项" if len(issues) > len(parts) else ""
    return "最终提示词与冻结合同冲突，未进入视频提交：" + "、".join(parts) + suffix


def attach_render_validation(
    *, item: Any, result: Dict[str, Any], duration_seconds: float
) -> Dict[str, Any]:
    """Audit the prompt of a result that is about to be persisted.

    Returns ``{}`` for anything that is not a frozen accessory montage, so the
    legacy result payload stays byte-identical and pays nothing for this step.

    Never raises.  A finished script must not become a failure because
    bookkeeping broke; that case is recorded as ``AUDIT_ERROR`` instead, which
    does not block delivery and is counted separately in the batch report.
    """

    if not isinstance(result, dict) or _text(result.get("status")) != "SUCCESS":
        return {}
    proxy = _ResultProxy(item, result)
    try:
        if not _frozen_mixed_contract_for_item(proxy):
            return {}
    except Exception:  # noqa: BLE001 - detection must not fail an item
        return {}
    try:
        checked = render_video_generation_prompt_checked(
            item=proxy, duration_seconds=float(duration_seconds or 15)
        )
    except Exception as exc:  # noqa: BLE001 - bookkeeping must not fail an item
        return {
            RENDER_VALIDATION_FIELD: {
                "version": RENDER_VALIDATION_SCHEMA_VERSION,
                "status": "AUDIT_ERROR",
                "reason": f"{type(exc).__name__}: {exc}"[:300],
                "issues": [],
                "checked_shots": 0,
                "prompt_hash": "",
                "renderer_version": SCRIPT_RENDERER_VERSION,
            }
        }
    validation = dict(checked.get("render_validation") or {})
    report = checked.get("report")
    validation["compaction_ok"] = bool(getattr(report, "ok", True))
    validation["over_limit"] = bool(getattr(report, "over_limit", False))
    validation["prompt_chars"] = len(checked.get("text") or "")
    return {RENDER_VALIDATION_FIELD: validation}



def render_video_generation_prompt_report(
    *, item: Any, duration_seconds: float
) -> CompactionReport:
    """Same prompt, plus whether compaction met the budget.

    Compaction never truncates, so an over-limit contract can only be
    *reported*.  ``report.ok`` is ``False`` if any protected constraint was
    dropped or migrated between shots (Review #9); ``report.over_limit`` is
    ``True`` when explanatory filler could not cover the gap.
    """
    result = load_item_result(item)
    script = _dict(result.get("script"))
    brief = _dict(script.get("video_generation_brief")) or script
    if _video_prompt_profile(brief) == LEGACY_PROFILE:
        return _unchanged_report(
            _render_legacy_video_generation_prompt(
                item=item,
                duration_seconds=duration_seconds,
            )
        )
    # The UGC-native prompt concatenates several projections of the same frozen
    # contract and can run past the video stage's character budget.  The
    # compaction pass only touches prompts that exceed the budget, and only
    # removes text that is duplicated elsewhere in the prompt.
    return compact_video_prompt_report(
        _render_ugc_native_video_generation_prompt(
            item=item,
            duration_seconds=duration_seconds,
        )
    )


def _unchanged_report(text: str) -> CompactionReport:
    """A report for a path that has its own budget handling (legacy)."""
    return CompactionReport(
        text=text,
        original_chars=len(text),
        limit=0,
        applied=False,
        over_limit=False,
    )


def _source_content_hash(item: Any) -> str:
    """Digest of the inputs this render consumed, and nothing else.

    Covers the script body/brief that was rendered and the frozen direction
    package it was rendered from.  Deliberately excludes the projection, the
    snapshot and every timestamp: this exists so a consumer can tell "the same
    frozen source" from "somebody moved the source under this delivery".
    """

    import hashlib

    result = load_item_result(item)
    payload = {
        "script": _dict(result.get("script")),
        "frozen_direction_package": _json_dict(
            getattr(item, "frozen_direction_package_json", "")
        ),
    }
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def build_delivery_snapshot(
    *, projection: Dict[str, Any], item: Any, created_at: str = ""
) -> Dict[str, Any]:
    """The text this export actually published, bound to the audit of that text.

    F4 (review 2026-09-20): the export re-renders the prompt and audits it, puts
    the fresh verdict in the 飞书 columns -- and left ``result_json`` untouched.
    The consumer therefore read the *generation-time* verdict forever, while the
    table showed the current one, so a re-export after a renderer fix
    (``v1 FAIL`` -> ``v2 PASS`` is the real case) could never terminate.

    ``{}`` for anything that is not ``NECKLACE_MIXED_V1``: this round adds the
    field for one profile only, so every other category's stored payload stays
    byte-identical.  The snapshot is built from the *same* checked projection
    that produces the 飞书 fields, so the text in the table and the text in the
    snapshot cannot drift apart.
    """

    from core.necklace_mixed_profile import NECKLACE_MIXED_V1_PROFILE

    contract = _frozen_mixed_contract_for_item(item)
    if _text(contract.get("feature_profile"), "") != NECKLACE_MIXED_V1_PROFILE:
        return {}
    block = _dict(contract.get("necklace_contract"))
    return {
        "schema_version": DELIVERY_SNAPSHOT_SCHEMA_VERSION,
        "batch_item_id": _text(getattr(item, "batch_item_id", ""), ""),
        # The *internal* id, which is what the frozen table's own column holds;
        # ``projection["script_id"]`` is the public one the table column shows.
        "internal_script_id": _text(getattr(item, "script_id", ""), ""),
        "complete_script_id": _text(projection.get("script_id"), ""),
        "feature_profile": NECKLACE_MIXED_V1_PROFILE,
        "frozen_contract_hash": _text(block.get("profile_config_hash"), ""),
        "source_content_hash": _source_content_hash(item),
        "prompt_text": _text(projection.get("video_prompt"), ""),
        "render_validation": _dict(projection.get(RENDER_VALIDATION_FIELD)),
        "created_at": created_at or _snapshot_now(),
    }


def _snapshot_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def build_production_projection(*, batch: Any, item: Any) -> Dict[str, Any]:
    result = load_item_result(item)
    script = _dict(result.get("script"))
    production = _dict(script.get("production_design"))
    video_brief = _dict(script.get("video_generation_brief"))
    storyboard = _list(video_brief.get("storyboard")) or _list(
        script.get("storyboard")
    )
    capture_mode = _capture_mode(video_brief, production)
    # Diagnostics must describe the contract that the final renderer actually
    # executes.  Stored V3 scripts are upgraded to V4 at read time; reading the
    # embedded contract directly made reports claim 2/3 clips while the final
    # prompt correctly rendered four.
    capture_rhythm = _capture_rhythm_contract(
        video_brief,
        script,
        capture_mode=capture_mode,
        storyboard=storyboard,
    )
    _, effective_capture_units = compile_capture_units(
        storyboard,
        capture_rhythm,
    )
    shot_richness = _dict(capture_rhythm.get("shot_richness_contract"))
    action_design = _dict(video_brief.get("action_design")) or _dict(
        production.get("action_execution")
    )
    visual_signature = _text(getattr(item, "visual_signature", ""))
    action_signature = _text(action_design.get("action_signature"), "")
    effective_creative_signature = "|".join(
        value for value in (visual_signature, action_signature) if value
    )
    scene = _dict(production.get("scene"))
    voice = _dict(script.get("continuous_voiceover"))
    provenance = _dict(script.get("generation_provenance"))
    persona_contract = _dict(video_brief.get("persona_selection_contract")) or _dict(
        production.get("persona_selection_contract")
    )
    outfit_contract = _dict(video_brief.get("outfit_selection_contract"))
    outfit_persona_affinity = _dict(
        video_brief.get("outfit_persona_affinity_contract")
    ) or _dict(production.get("outfit_persona_affinity_contract"))
    frozen_package = _json_dict(
        getattr(item, "frozen_direction_package_json", "")
    )
    outfit_scene_affinity = (
        _dict(video_brief.get("outfit_scene_affinity_contract"))
        or _dict(production.get("outfit_scene_affinity_contract"))
        or _dict(script.get("outfit_scene_affinity_contract"))
        or _dict(frozen_package.get("outfit_scene_affinity_contract"))
    )
    complete_script_id = _text(
        script.get("complete_script_id")
        or result.get("script_id")
        or getattr(item, "script_id", "")
        or getattr(item, "batch_item_id", "")
    )
    # The delivered prompt and its execution audit must be produced by one call:
    # the audit is bound to the exact text through ``prompt_hash`` and
    # ``renderer_version``, so a re-render can never inherit an earlier PASS.
    checked = render_video_generation_prompt_checked(
        item=item,
        duration_seconds=float(getattr(batch, "duration_seconds", 15) or 15),
    )
    render_validation = dict(checked.get("render_validation") or {})
    _compaction_report = checked.get("report")
    render_validation["compaction_ok"] = bool(
        getattr(_compaction_report, "ok", True)
    )
    render_validation["over_limit"] = bool(
        getattr(_compaction_report, "over_limit", False)
    )
    render_validation["prompt_chars"] = len(checked.get("text") or "")
    # 成稿口播核对进投影：三类核对 + 口径兜底，状态与逐项证据都留在结构化字段里。
    voiceover_review = review_delivered_voiceover(script, _frozen_mainline_for_item(item))
    return {
        "script_id": complete_script_id,
        "batch_id": _text(getattr(batch, "batch_id", "")),
        "batch_item_id": _text(getattr(item, "batch_item_id", "")),
        "item_index": int(getattr(item, "item_index", 0) or 0),
        "product_code": _text(getattr(item, "product_code", "") or getattr(batch, "product_code", "")),
        "target_country": _text(getattr(batch, "target_country", "")),
        "target_language": _text(getattr(batch, "target_language", "")),
        "top_category": _text(getattr(batch, "top_category", "")),
        "product_type": _text(getattr(batch, "product_type", "")),
        "duration_seconds": float(getattr(batch, "duration_seconds", 15) or 15),
        "script_title": build_script_title(item, script),
        "core_selling_point": core_selling_point(item, script),
        "hook_type": _text(voice.get("hook_id") or getattr(item, "actual_hook_id", "") or getattr(item, "requested_hook_id", "")),
        "macro_family": _text(getattr(item, "macro_family_key", "")),
        "carrier_mode": _text(production.get("presentation_mode") or getattr(item, "carrier_mode", "")),
        "creative_signature": effective_creative_signature,
        "scene_summary": _text(scene.get("location")),
        "target_voiceover": _text(voice.get("target_text")),
        "chinese_voiceover": _text(voice.get("chinese_translation")),
        "voiceover_review": voiceover_review,
        "voiceover_review_status": _text(voiceover_review.get("status"), ""),
        "complete_script": render_complete_production_script(
            item=item, duration_seconds=getattr(batch, "duration_seconds", 15)
        ),
        "video_prompt": _text(checked.get("text")),
        "render_validation": render_validation,
        "cluster_id": getattr(item, "cluster_id", None),
        "cluster_version": _text(getattr(item, "cluster_version", ""), ""),
        "selection_run_id": _text(getattr(item, "selection_run_id", ""), ""),
        "direction_assignment_id": _text(getattr(item, "direction_assignment_id", ""), ""),
        "content_bundle_id": _text(getattr(item, "content_bundle_id", ""), ""),
        "creative_contract_id": _text(getattr(item, "creative_contract_id", ""), ""),
        "persona_id": _text(persona_contract.get("persona_id"), ""),
        "persona_name": _text(persona_contract.get("persona_name"), ""),
        "persona_contract_json": (
            json.dumps(persona_contract, ensure_ascii=False, sort_keys=True, default=str)
            if persona_contract else ""
        ),
        "outfit_template_id": _text(
            outfit_contract.get("template_id")
            or outfit_contract.get("silhouette_key"),
            "",
        ),
        "outfit_template_name": _text(
            outfit_contract.get("template_display_name")
            or outfit_contract.get("template_id")
            or outfit_contract.get("silhouette_key"),
            "",
        ),
        "outfit_accessories": _actual_outfit_accessories(
            outfit_contract, _dict(production.get("outfit"))
        ),
        "outfit_scene_match": _outfit_scene_match_label(
            outfit_scene_affinity
        ),
        "outfit_scene_contract_json": (
            json.dumps(
                outfit_scene_affinity,
                ensure_ascii=False,
                sort_keys=True,
                default=str,
            )
            if outfit_scene_affinity else ""
        ),
        "outfit_persona_match": _text(
            outfit_persona_affinity.get("match_status"), ""
        ),
        "outfit_persona_contract_json": (
            json.dumps(
                outfit_persona_affinity,
                ensure_ascii=False,
                sort_keys=True,
                default=str,
            )
            if outfit_persona_affinity else ""
        ),
        "reference_strategy": _text(
            persona_contract.get("reference_strategy") or "DIRECT_PRODUCT_REFERENCE"
        ),
        "input_snapshot_hash": _text(getattr(item, "item_snapshot_hash", ""), ""),
        "model_version": "/".join(
            value for value in (
                _text(provenance.get("model"), ""),
                _text(provenance.get("reasoning_effort"), ""),
            ) if value
        ),
        "capture_rhythm_schema": _text(
            capture_rhythm.get("schema_version"), ""
        ),
        "visible_clip_count": int(
            len(effective_capture_units)
            or shot_richness.get("compiled_visible_clips")
            or capture_rhythm.get("capture_unit_count")
            or 0
        ),
        "camera_setup_count": int(
            capture_rhythm.get("camera_setup_count")
            or shot_richness.get("camera_setup_budget")
            or 0
        ),
        "shot_richness_status": _text(
            shot_richness.get("preservation_status"), ""
        ),
        "structure_preservation_status": _text(
            shot_richness.get("structure_preservation_status"), ""
        ),
        "compiled_function_sequence": list(
            shot_richness.get("compiled_function_sequence") or []
        ),
        "script_type": "原创脚本",
        "processing_status": "待审核",
    }
