"""Branch-neutral video execution brief and deterministic prompt projection.

Business branches decide what the video is about.  This module only owns the
small, physical handoff understood by a video generator: reference authority,
product identity, continuity, visible clips, audio routing and negative guards.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Sequence


VIDEO_EXECUTION_SCHEMA = "video-execution-brief-v3-render-profile"
PRODUCT_PROOF_PROFILE = "PRODUCT_PROOF"
ORGANIC_LIFESTYLE_PROFILE = "ORGANIC_LIFESTYLE"


def _text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _unique_text(values: Iterable[Any], *, limit: int) -> List[str]:
    output: List[str] = []
    seen = set()
    for value in values:
        text = _text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        output.append(text)
        if len(output) >= limit:
            break
    return output


def _fact_texts(product_truth: Dict[str, Any]) -> List[str]:
    output: List[str] = []
    for item in product_truth.get("facts") or []:
        if isinstance(item, dict):
            text = _text(item.get("text") or item.get("fact_text"))
        else:
            text = _text(item)
        if text:
            output.append(text)
    return output


def _visual_anchor_texts(product_truth: Dict[str, Any]) -> List[str]:
    return [
        _text(item.get("description"))
        for item in product_truth.get("visual_anchors") or []
        if isinstance(item, dict) and _text(item.get("description"))
    ]


def _without_role_prefix(value: Any) -> str:
    text = _text(value)
    for prefix in ("人物动作：", "人物人物", "人物：", "人物", "创作者动作：", "创作设计："):
        if text.startswith(prefix):
            return text[len(prefix):].lstrip(" ：:")
    return text


def _without_design_prefix(value: Any) -> str:
    text = _text(value)
    for prefix in ("创作设计：", "创作设计:"):
        if text.startswith(prefix):
            return text[len(prefix):].strip()
    return text


def _compact_capture_units(units: Sequence[Any]) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    for index, raw in enumerate(units, 1):
        if not isinstance(raw, dict):
            continue
        subject_action = _text(raw.get("subject_action") or raw.get("action"))
        output.append(
            {
                "unit_id": _text(raw.get("unit_id")) or f"C{index}",
                "duration_seconds": raw.get("duration_seconds"),
                "shot": _text(raw.get("shot")) or "普通手机自然景别",
                "camera_action": _text(raw.get("camera_action")) or "固定机位",
                "subject_action": _without_role_prefix(subject_action) or "保持自然生活状态",
                "product_evidence": _text(raw.get("product_evidence")),
                "narrative_job": _text(raw.get("narrative_job")).upper(),
                "product_focus": _text(raw.get("product_focus")).upper(),
                "product_interaction": _text(raw.get("product_interaction")).upper(),
                "shot_size": _text(raw.get("shot_size")),
                "body_coverage": _text(raw.get("body_coverage")),
                "camera_relation": _text(raw.get("camera_relation")),
                "setup_id": _text(raw.get("setup_id")) or f"SETUP_{index}",
                "authorized_props": _unique_text(raw.get("authorized_props") or [], limit=3),
                "visible_zones": _unique_text(raw.get("visible_zones") or [], limit=8),
                "evidence_refs": [
                    _text(value) for value in raw.get("evidence_refs") or [] if _text(value)
                ],
                "fact_refs": [
                    _text(value) for value in raw.get("fact_refs") or [] if _text(value)
                ],
            }
        )
    return output


def compile_video_execution_brief(
    *,
    product_truth: Dict[str, Any],
    visual_blueprint: Dict[str, Any],
    duration_seconds: float,
    audio_mode: str = "VOICEOVER_ADDED_IN_POST",
    render_profile: str = PRODUCT_PROOF_PROFILE,
    attention_arc: Dict[str, Any] | None = None,
    audio_arc: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Compile a clean execution package without any branch policy prose."""

    identity_anchors = _unique_text(
        list(product_truth.get("identity_anchors") or [])
        + _visual_anchor_texts(product_truth)
        + _fact_texts(product_truth),
        limit=3,
    )
    if not identity_anchors:
        identity_anchors = ["商品颜色、轮廓和可见结构严格以商品参考图为准"]

    negative_defaults = (
        "不改变商品颜色、轮廓、可见结构或数量",
        "不复制商品参考图中的人物、穿搭、背景或姿势",
        "不新增Logo、文字、装饰或合同外配件",
    )
    negative_constraints = _unique_text(
        list(product_truth.get("negative_constraints") or []) + list(negative_defaults),
        limit=4,
    )
    creative = visual_blueprint.get("creative_design")
    if not isinstance(creative, dict):
        creative = {}
    units = _compact_capture_units(visual_blueprint.get("capture_units") or [])
    authorized_props = _unique_text(
        [prop for unit in units for prop in unit.get("authorized_props") or []], limit=6
    )
    brief = {
        "schema_version": VIDEO_EXECUTION_SCHEMA,
        "duration_seconds": float(duration_seconds),
        "reference_authority": {
            "product_reference_controls": "只控制目标商品的外观与可见结构",
            "reference_does_not_control": "人物、脸、身形、妆发、穿搭、场景、姿势和构图",
        },
        "product_identity_lock": identity_anchors,
        "continuity": {
            "creator": _without_design_prefix(creative.get("creator")) or "同一位自然生活方式创作者",
            "scene": _without_design_prefix(creative.get("scene")) or "同一处自然生活场景",
            "lived_moment": _without_design_prefix(creative.get("lived_moment")) or "同一个连续生活时刻",
            "edit_mode": "片段间直接切镜；人物、商品、穿搭、地点和光线保持连续",
        },
        "capture_units": units,
        "render_profile": _text(render_profile).upper() or PRODUCT_PROOF_PROFILE,
        "authorized_props": authorized_props,
        "audio_mode": audio_mode,
        "negative_constraints": negative_constraints,
    }
    # Optional, branch-authored execution hints.  The shared layer transports
    # them without deciding the creative topic or retention policy.
    if isinstance(attention_arc, dict) and attention_arc:
        brief["attention_arc"] = dict(attention_arc)
    if isinstance(audio_arc, dict) and audio_arc:
        brief["audio_arc"] = dict(audio_arc)
    return brief


def render_video_execution_prompt(brief: Dict[str, Any]) -> str:
    """Render a stable, scannable prompt from a structured execution brief."""

    if _text(brief.get("render_profile")).upper() == ORGANIC_LIFESTYLE_PROFILE:
        return _render_organic_lifestyle_prompt(brief)

    authority = brief.get("reference_authority") or {}
    continuity = brief.get("continuity") or {}
    lines = ["【商品锁】"]
    lines.extend(f"- {_text(value)}" for value in brief.get("product_identity_lock") or [])
    lines.extend(
        [
            "【参考图权威】",
            f"- 参考图{_text(authority.get('product_reference_controls'))}；不控制{_text(authority.get('reference_does_not_control'))}。",
            "【全片连续性】",
            f"- 人物：{_text(continuity.get('creator'))}。",
            f"- 场景：{_text(continuity.get('scene'))}。",
        ]
    )
    if _text(continuity.get("lived_moment")) != _text(continuity.get("scene")):
        lines.append(f"- 连续动作：{_text(continuity.get('lived_moment'))}。")
    lines.append(f"- 剪辑：{_text(continuity.get('edit_mode'))}。")
    for index, unit in enumerate(brief.get("capture_units") or [], 1):
        if not isinstance(unit, dict):
            continue
        duration = unit.get("duration_seconds")
        duration_text = f"｜{duration}s" if duration not in (None, "") else ""
        lines.extend(
            [
                f"【片段{index}{duration_text}】",
                f"- {_text(unit.get('shot'))}；{_text(unit.get('camera_action'))}；"
                f"人物动作：{_without_role_prefix(unit.get('subject_action'))}；"
                f"商品可见结果：{_text(unit.get('product_evidence')) or '自然清楚可见'}。",
            ]
        )
    if brief.get("authorized_props"):
        lines.extend(
            ["【已授权场景道具】", f"- {'、'.join(_unique_text(brief.get('authorized_props') or [], limit=6))}"]
        )
    lines.extend(
        [
            "【声音】",
            "- 画面阶段不生成口播或字幕；正式目标语言口播由后期独立配音，保留轻环境声。",
            "【禁止】",
        ]
    )
    lines.extend(f"- {_text(value)}" for value in brief.get("negative_constraints") or [])
    return "\n".join(line for line in lines if line.strip()).strip()


def _render_organic_lifestyle_prompt(brief: Dict[str, Any]) -> str:
    """Render organic footage as lived content, not a product-proof checklist."""

    authority = brief.get("reference_authority") or {}
    continuity = brief.get("continuity") or {}
    attention = brief.get("attention_arc") or {}
    audio = brief.get("audio_arc") or {}
    lines = [
        "【生活时刻】",
        f"- {_text(continuity.get('lived_moment')) or '同一个自然发生的生活时刻'}。",
        "【人物与场景】",
        f"- 人物：{_text(continuity.get('creator'))}。",
        f"- 场景：{_text(continuity.get('scene'))}。",
        f"- 参考图{_text(authority.get('product_reference_controls'))}；不控制{_text(authority.get('reference_does_not_control'))}。",
    ]
    if attention:
        payoff_window = attention.get("payoff_window_seconds") or []
        payoff_window_text = ""
        if isinstance(payoff_window, (list, tuple)) and len(payoff_window) >= 2:
            payoff_window_text = f"；约{payoff_window[0]}-{payoff_window[1]}秒完成回答"
        lines.extend(
            [
                "【前3秒留存】",
                f"- 首帧：{_text(attention.get('first_frame_job'))}。",
                f"- 前0.8秒：{_text(attention.get('first_800ms_visual_move'))}。",
                f"- 前3秒：{_text(attention.get('first_3s_open_loop'))}{payoff_window_text}。",
            ]
        )
    lines.append("【自然画面推进】")
    for index, unit in enumerate(brief.get("capture_units") or [], 1):
        if not isinstance(unit, dict):
            continue
        duration = unit.get("duration_seconds")
        duration_text = f"，约{duration}s" if duration not in (None, "") else ""
        focus = _text(unit.get("product_focus")).upper()
        focus_text = {
            "BACKGROUND": "商品自然处于背景关系",
            "SECONDARY": "商品作为次要视觉关系自然可见",
            "PRIMARY": "商品可短暂成为画面主体",
        }.get(focus, "商品自然融入人物与场景")
        lines.append(
            f"- 片段{index}{duration_text}：{_text(unit.get('shot'))}；"
            f"{_text(unit.get('camera_action'))}；"
            f"{_without_role_prefix(unit.get('subject_action'))}；{focus_text}。"
        )
    lines.extend(
        [
            "【商品连续性约束】",
            *[f"- {_text(value)}" for value in brief.get("product_identity_lock") or []],
            f"- {_text(continuity.get('edit_mode'))}。",
            "【声音后期交接】",
            (
                "- 画面阶段不生成口播、字幕或画内音乐；后期加入正式目标语言口播，"
                f"BGM按开场{_text(audio.get('opening_energy')) or 'medium'}→口播底床"
                f"{_text(audio.get('voiceover_bed_energy')) or 'low'}→揭示点"
                f"{_text(audio.get('payoff_energy')) or 'medium'}推进，并保留轻环境声。"
            ),
            f"- 开头听觉动作：{_text(audio.get('opening_accent')) or '轻量音色变化，不使用商业转场音'}。",
            "【禁止】",
            "- 禁止为了展示商品而逐项触摸、整理、指向、拉扯或切连续细节特写。",
            "- 禁止把生活片段拍成商品功能演示、卖点核对或陈列广告。",
            *[f"- {_text(value)}" for value in brief.get("negative_constraints") or []],
        ]
    )
    return "\n".join(line for line in lines if line.strip()).strip()
