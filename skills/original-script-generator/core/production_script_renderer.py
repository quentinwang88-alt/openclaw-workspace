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

from core.simplified_complete_script import (
    CAPTURE_MODE_CREATOR_SELF_SHOT,
    build_product_identity_lock,
)


VIDEO_PROMPT_PROFILE_ENV = "ORIGINAL_SCRIPT_VIDEO_PROMPT_PROFILE"
UGC_NATIVE_PROFILE = "ugc_native_v1"
LEGACY_PROFILE = "legacy"
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


def _text(value: Any, fallback: str = "UNAVAILABLE") -> str:
    text = str(value or "").strip()
    return text or fallback


def _dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list:
    return value if isinstance(value, list) else []


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


def render_complete_production_script(
    *,
    item: Any,
    duration_seconds: float,
) -> str:
    result = load_item_result(item)
    script = _dict(result.get("script"))
    concept = _dict(script.get("script_concept"))
    production = _dict(script.get("production_design"))
    character = _dict(production.get("character"))
    outfit = _dict(production.get("outfit"))
    scene = _dict(production.get("scene"))
    emotion = _dict(production.get("emotion"))
    product_usage = _dict(script.get("product_usage"))
    voice = _dict(script.get("continuous_voiceover"))
    storyboard = _list(script.get("storyboard"))

    lines = [
        "【脚本标题】",
        build_script_title(item, script),
        "",
        "【基本信息】",
        f"时长：{float(duration_seconds or 15):g}秒",
        f"结构：{_text(getattr(item, 'macro_family_key', ''))}",
        f"承载方式：{_text(production.get('presentation_mode') or getattr(item, 'carrier_mode', ''))}",
        f"拍摄关系：{_text(production.get('capture_mode'))}",
        f"核心卖点：{core_selling_point(item, script)}",
        f"钩子类型：{_text(voice.get('hook_id') or getattr(item, 'actual_hook_id', '') or getattr(item, 'requested_hook_id', ''))}",
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
        f"配饰道具：{_text(outfit.get('accessories'))}",
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
        f"目标语言：{_text(voice.get('target_text'))}",
        f"中文：{_text(voice.get('chinese_translation'))}",
    ]

    for index, raw_shot in enumerate(storyboard, 1):
        shot = _dict(raw_shot)
        lines.extend(
            [
                "",
                f"【分镜{int(shot.get('shot_no') or index):02d}｜{_text(shot.get('time_range'))}｜{_text(shot.get('narrative_role'))}】",
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


def _render_ugc_native_video_generation_prompt(*, item: Any, duration_seconds: float) -> str:
    result = load_item_result(item)
    script = _dict(result.get("script"))
    brief = _dict(script.get("video_generation_brief")) or script
    production = _dict(brief.get("production_design")) or _dict(script.get("production_design"))
    character = _dict(production.get("character"))
    outfit = _dict(production.get("outfit"))
    scene = _dict(production.get("scene"))
    life_event = _dict(production.get("life_event"))
    truth = _dict(brief.get("product_truth"))
    if not truth:
        truth = {
            "identity_anchors": _list(_dict(script.get("product_usage")).get("identity_anchors_preserved")),
            "visible_detail_anchors": [],
        }
    identity_lock = _dict(brief.get("product_identity_lock"))
    # Rendering is deterministic, so old stored briefs can safely receive the
    # latest closure wording without re-running the blueprint or voiceover.
    if identity_lock.get("compiler_version") != "product-identity-lock-v2":
        rebuilt_lock = build_product_identity_lock(truth)
        if rebuilt_lock.get("must_preserve"):
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
    storyboard = _list(brief.get("storyboard")) or _list(script.get("storyboard"))
    capture_mode = _capture_mode(brief, production)
    concept = _dict(script.get("script_concept"))
    event_text = _text(
        life_event.get("continuous_event")
        or life_event.get("motivation")
        or concept.get("one_sentence_idea"),
        "",
    )

    lines = [
        "【视频任务】",
        f"竖屏手机短视频，时长{float(duration_seconds or 15):g}秒。",
        "",
        "【商品身份锁｜最高优先级】",
        "商品外观以参考图为唯一准则；商品一致性优先于人物美感、场景氛围和镜头效果。",
    ]
    if must_preserve:
        lines.append(f"必须保持：{_join(must_preserve)}")
    if critical_details:
        lines.append(f"关键可见细节：{_join(critical_details[:4])}")
    if must_not_change:
        lines.extend(["", "【商品负向约束】", _join(must_not_change)])

    accessory_brief = _dict(brief.get("accessory_execution_brief"))
    if accessory_brief:
        accessory_lines = ["", "【配饰佩戴与展示关系】"]
        product_relation = _text(accessory_brief.get("product_relation"), "")
        required_result = _text(
            accessory_brief.get("required_visible_result"), ""
        )
        interaction_limit = _list(accessory_brief.get("interaction_limit"))
        identity_focus = _list(accessory_brief.get("identity_focus"))
        identity_authority_guidance = _text(
            accessory_brief.get("identity_authority_guidance"), ""
        )
        claim_boundary = _text(accessory_brief.get("claim_boundary"), "")
        if product_relation:
            accessory_lines.append(f"商品关系：{product_relation}")
        if required_result:
            accessory_lines.append(f"必要结果：{required_result}")
        if interaction_limit:
            accessory_lines.append(f"交互边界：{_join(interaction_limit)}")
        if identity_focus:
            accessory_lines.append(f"身份重点：{_join(identity_focus)}")
        if identity_authority_guidance:
            accessory_lines.append(f"身份授权：{identity_authority_guidance}")
        if claim_boundary:
            accessory_lines.append(f"表达边界：{claim_boundary}")
        lines.extend(accessory_lines)

    capture_lines = [
        "",
        "【拍摄方式｜UGC_NATIVE_V1】",
        UGC_NATIVE_POSITIVE,
    ]
    if capture_mode == CAPTURE_MODE_CREATOR_SELF_SHOT:
        capture_lines.extend(
            [
                "",
                "【拍摄关系｜CREATOR_SELF_SHOT】",
                CREATOR_SELF_SHOT_POSITIVE,
                f"拍摄关系边界：{CREATOR_SELF_SHOT_NEGATIVE}",
            ]
        )
    else:
        capture_lines.append(f"风格负向：{UGC_NATIVE_NEGATIVE}")
    lines.extend(
        [
            *capture_lines,
            "",
            "【人物、穿搭与生活场景】",
            f"出镜方式：{_text(production.get('presentation_mode'))}",
            f"拍摄关系：{capture_mode or '普通手机商品记录'}",
            f"人物：{_text(character.get('identity'))}；{_text(character.get('appearance'))}",
            f"妆发：{_text(character.get('hair_makeup'))}",
            f"基础穿搭：{_text(outfit.get('base_outfit'))}",
            f"商品角色：{_video_safe_closure_text(outfit.get('product_role'), identity_lock)}",
            f"配饰道具：{_text(outfit.get('accessories'))}",
            f"地点与时刻：{_text(scene.get('location'))}；{_text(scene.get('moment'))}",
            f"光线：{_compact_native_description(scene.get('lighting'), max_segments=2, max_chars=90) or '只使用现场已有自然光或普通室内光，不重新设计商业布光。'}",
            f"手机位置：{_compact_native_description(scene.get('phone_placement'), max_segments=2, max_chars=100) or '保持一个固定的普通手机视角'}",
            f"人物/商品位置：{_compact_native_description(scene.get('subject_position'), max_segments=2, max_chars=100)}",
            f"背景层次：{_compact_native_description(scene.get('background_depth'), max_segments=2, max_chars=100)}",
            f"生活痕迹：{_compact_native_description(scene.get('lived_in_trace'), max_segments=1, max_chars=60) or '保留一处自然使用痕迹'}",
            f"现场背景：{_compact_native_description(scene.get('background'), max_segments=3, max_chars=160) or '保留真实生活环境'}",
        ]
    )
    if event_text and capture_mode != CAPTURE_MODE_CREATOR_SELF_SHOT:
        lines.append(
            f"连续生活事件：{_compact_native_description(event_text, max_segments=2, max_chars=90)}"
        )

    if capture_mode == CAPTURE_MODE_CREATOR_SELF_SHOT:
        lines.extend(
            [
                "分享方式：创作者主要看向自己的手机镜头说话；以下结构只控制内容推进，不代表切换摄影机位。",
                "画面变化：可在同一手机前短暂退后、转身或靠近展示一个细节；不要把每个结构段拍成独立广告镜头。",
            ]
        )

    prompt_storyboard = (
        _creator_content_moments(storyboard)
        if capture_mode == CAPTURE_MODE_CREATOR_SELF_SHOT
        else storyboard
    )
    for index, raw_shot in enumerate(prompt_storyboard, 1):
        shot = _dict(raw_shot)
        camera = (
            ""
            if capture_mode == CAPTURE_MODE_CREATOR_SELF_SHOT
            else _compact_camera(shot.get("camera"))
        )
        lines.extend(
            [
                "",
                f"【{'连续内容段' if capture_mode == CAPTURE_MODE_CREATOR_SELF_SHOT else '片段'}{index:02d}｜{_text(shot.get('time_range'))}｜{_text(shot.get('narrative_role'))}】",
                f"画面事件：{_compact_native_description(_video_safe_closure_text(shot.get('visual_content'), identity_lock), max_segments=2, max_chars=110)}",
                f"人物动作：{_compact_native_description(_video_safe_closure_text(shot.get('character_action'), identity_lock), max_segments=2, max_chars=70)}",
            ]
        )
        if camera:
            lines.append(f"手机机位：{camera}")
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
                "保持同一创作者、商品、穿搭、场景和手机视角；结构只改变分享内容，不建立摄影团队。商品一致性优先于场景美感和镜头效果。"
                if capture_mode == CAPTURE_MODE_CREATOR_SELF_SHOT
                else "保持同一人物、商品、穿搭、场景和连续事件；商品一致性优先于场景美感和镜头效果。"
            ),
        ]
    )
    return "\n".join(lines).strip()


def render_video_generation_prompt(*, item: Any, duration_seconds: float) -> str:
    result = load_item_result(item)
    script = _dict(result.get("script"))
    brief = _dict(script.get("video_generation_brief")) or script
    if _video_prompt_profile(brief) == LEGACY_PROFILE:
        return _render_legacy_video_generation_prompt(
            item=item,
            duration_seconds=duration_seconds,
        )
    return _render_ugc_native_video_generation_prompt(
        item=item,
        duration_seconds=duration_seconds,
    )


def build_production_projection(*, batch: Any, item: Any) -> Dict[str, Any]:
    result = load_item_result(item)
    script = _dict(result.get("script"))
    production = _dict(script.get("production_design"))
    scene = _dict(production.get("scene"))
    voice = _dict(script.get("continuous_voiceover"))
    provenance = _dict(script.get("generation_provenance"))
    complete_script_id = _text(
        script.get("complete_script_id")
        or result.get("script_id")
        or getattr(item, "script_id", "")
        or getattr(item, "batch_item_id", "")
    )
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
        "creative_signature": _text(getattr(item, "visual_signature", "")),
        "scene_summary": _text(scene.get("location")),
        "target_voiceover": _text(voice.get("target_text")),
        "chinese_voiceover": _text(voice.get("chinese_translation")),
        "complete_script": render_complete_production_script(
            item=item, duration_seconds=getattr(batch, "duration_seconds", 15)
        ),
        "video_prompt": render_video_generation_prompt(
            item=item, duration_seconds=getattr(batch, "duration_seconds", 15)
        ),
        "cluster_id": getattr(item, "cluster_id", None),
        "cluster_version": _text(getattr(item, "cluster_version", ""), ""),
        "selection_run_id": _text(getattr(item, "selection_run_id", ""), ""),
        "direction_assignment_id": _text(getattr(item, "direction_assignment_id", ""), ""),
        "content_bundle_id": _text(getattr(item, "content_bundle_id", ""), ""),
        "creative_contract_id": _text(getattr(item, "creative_contract_id", ""), ""),
        "input_snapshot_hash": _text(getattr(item, "item_snapshot_hash", ""), ""),
        "model_version": "/".join(
            value for value in (
                _text(provenance.get("model"), ""),
                _text(provenance.get("reasoning_effort"), ""),
            ) if value
        ),
        "script_type": "原创脚本",
        "processing_status": "待审核",
    }
