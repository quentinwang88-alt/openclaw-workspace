#!/usr/bin/env python3
"""
脚本 JSON 渲染与摘要生成。
"""

import re
from typing import Any, Dict, List

from core.json_parser import _normalize_video_prompt_payload


FINAL_VIDEO_PROMPT_PREFERRED_CHARS = 1800
FINAL_VIDEO_PROMPT_MAX_CHARS = 2000


def _stringify_lines(items: List[str]) -> str:
    return "\n".join(item for item in items if item)


def _render_dict_items(items: Any, primary_keys: List[str]) -> List[str]:
    if not isinstance(items, list):
        return []
    lines: List[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        values = [str(item.get(key, "") or "").strip() for key in primary_keys]
        text = " | ".join(value for value in values if value)
        if text:
            lines.append(f"- {text}")
    return lines


def _render_positioning(positioning: Any) -> str:
    if isinstance(positioning, dict):
        return _stringify_lines(
            [
                f"- 脚本标题：{positioning.get('script_title', '')}",
                f"- 方向类型：{positioning.get('direction_type', '')}",
                f"- 核心主打点：{positioning.get('core_primary_selling_point', '')}",
            ]
        )
    return str(positioning or "")


def _compact_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[；;]{2,}", "；", text)
    text = re.sub(r"[，,]{2,}", "，", text)
    return text.strip(" \n\t；;，,。")


def _semantic_segments(value: Any) -> List[str]:
    text = _compact_text(value)
    if not text:
        return []
    raw_segments = re.split(r"[；;。\n]+", text)
    segments: List[str] = []
    seen = set()
    for raw in raw_segments:
        segment = _compact_text(raw)
        if not segment:
            continue
        key = re.sub(r"[\s；;，,。:：、\-]", "", segment)
        if not key or key in seen:
            continue
        seen.add(key)
        segments.append(segment)
    return segments


def _truncate_text(value: Any, max_chars: int) -> str:
    text = _compact_text(value)
    if not text or max_chars <= 0:
        return ""
    if len(text) <= max_chars:
        return text
    if max_chars <= 1:
        return text[:max_chars]
    return text[: max_chars - 1].rstrip(" ，；;、,:：") + "…"


def _join_limited_segments(value: Any, max_segments: int, max_chars: int) -> str:
    if max_chars <= 0 or max_segments <= 0:
        return ""
    segments = _semantic_segments(value)[:max_segments]
    if not segments:
        return ""
    return _truncate_text("；".join(segments), max_chars)


def _merge_brief_parts(*parts: Any, separator: str = "；") -> str:
    merged: List[str] = []
    seen = set()
    for part in parts:
        for segment in _semantic_segments(part):
            key = re.sub(r"[\s；;，,。:：、\-]", "", segment)
            if not key or key in seen:
                continue
            seen.add(key)
            merged.append(segment)
    return separator.join(merged)


def _compress_descriptor(value: Any, max_segments: int, max_chars: int) -> str:
    return _join_limited_segments(value, max_segments=max_segments, max_chars=max_chars)


def _compress_voiceover(value: Any, max_chars: int) -> str:
    text = _compact_text(value)
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    return _truncate_text(text, max_chars)


def _compress_style_note(style_note: Any, boundary_text: str, shot_content: str, person_action: str, max_chars: int) -> str:
    note = _join_limited_segments(style_note, max_segments=2, max_chars=max_chars)
    if not note:
        return ""
    note_key = re.sub(r"[\s；;，,。:：、\-]", "", note)
    for other in (boundary_text, shot_content, person_action):
        other_key = re.sub(r"[\s；;，,。:：、\-]", "", _compact_text(other))
        if note_key and other_key and note_key in other_key:
            return ""
    return note


def _compress_boundary(value: Any, max_segments: int, max_chars: int) -> str:
    return _join_limited_segments(value, max_segments=max_segments, max_chars=max_chars)


def _compress_setup(value: Any, max_segments: int, max_chars: int) -> str:
    return _join_limited_segments(value, max_segments=max_segments, max_chars=max_chars)


def _parse_duration_seconds(value: Any) -> float:
    text = str(value or "").strip()
    match = re.search(r"(\d+(?:\.\d+)?)", text)
    if not match:
        return 0.0
    try:
        return float(match.group(1))
    except ValueError:
        return 0.0


def _format_duration(seconds: float) -> str:
    if seconds <= 0:
        return ""
    if abs(seconds - round(seconds)) < 1e-6:
        return f"{int(round(seconds))}s"
    return f"{seconds:.1f}".rstrip("0").rstrip(".") + "s"


def _merge_proof_shots(shots: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if len(shots) <= 4:
        return shots
    merged = [dict(item) for item in shots]
    index = 0
    while len(merged) > 4 and index < len(merged) - 1:
        current = merged[index]
        nxt = merged[index + 1]
        current_task = str(current.get("spoken_line_task", "") or "").strip()
        next_task = str(nxt.get("spoken_line_task", "") or "").strip()
        if current_task not in {"proof", "none"} or next_task not in {"proof", "none"}:
            index += 1
            continue
        combined_seconds = _parse_duration_seconds(current.get("duration")) + _parse_duration_seconds(nxt.get("duration"))
        merged[index] = {
            **current,
            "duration": _format_duration(combined_seconds) or _truncate_text(
                _merge_brief_parts(current.get("duration"), nxt.get("duration"), separator="+"),
                10,
            ),
            "shot_content": _compress_descriptor(
                _merge_brief_parts(current.get("shot_content"), nxt.get("shot_content")),
                max_segments=3,
                max_chars=40,
            ),
            "voiceover_text_target_language": _compress_voiceover(
                _merge_brief_parts(
                    current.get("voiceover_text_target_language"),
                    nxt.get("voiceover_text_target_language"),
                    separator=" / ",
                ),
                max_chars=30,
            ),
            "voiceover_text_zh": _compress_voiceover(
                _merge_brief_parts(current.get("voiceover_text_zh"), nxt.get("voiceover_text_zh"), separator=" / "),
                max_chars=18,
            ),
            "spoken_line_task": "proof",
            "person_action": _compress_descriptor(
                _merge_brief_parts(current.get("person_action"), nxt.get("person_action")),
                max_segments=2,
                max_chars=18,
            ),
            "style_note": "",
        }
        del merged[index + 1]
    for shot_no, shot in enumerate(merged, 1):
        shot["shot_no"] = shot_no
    return merged


def _render_final_video_prompt_core(prompt_json: Dict[str, Any]) -> str:
    prompt = _normalize_video_prompt_payload(prompt_json)
    shots = prompt.get("shot_execution", []) or []
    sections: List[str] = []

    video_setup = _compact_text(prompt.get("video_setup", ""))
    if video_setup:
        sections.append(f"整体：{video_setup}")

    shot_blocks = []
    for shot in shots:
        shot_no = shot.get("shot_no", "")
        duration = str(shot.get("duration", "") or "").strip()
        parts = [
            f"镜头{shot_no}（{duration}）" if duration else f"镜头{shot_no}",
            f"内容：{shot.get('shot_content', '')}",
            f"动作：{shot.get('person_action', '')}",
        ]
        voiceover = (
            str(shot.get("voiceover_text_target_language", "") or "").strip()
            or str(shot.get("voiceover_text_zh", "") or "").strip()
        )
        if voiceover:
            parts.append(f"口播：{voiceover}")
        spoken_line_task = str(shot.get("spoken_line_task", "") or "").strip()
        if spoken_line_task:
            parts.append(f"任务：{spoken_line_task}")
        style_note = str(shot.get("style_note", "") or "").strip()
        if style_note:
            parts.append(f"提醒：{style_note}")
        shot_blocks.append("\n".join(parts))

    if shot_blocks:
        sections.append(_stringify_lines(shot_blocks))

    execution_boundary = _compact_text(prompt.get("execution_boundary", ""))
    if execution_boundary:
        sections.append(f"统一约束：{execution_boundary}")

    return "\n\n".join(section for section in sections if section).strip()


def _compress_video_prompt_pass(prompt_json: Dict[str, Any], second_pass: bool = False) -> Dict[str, Any]:
    prompt = _normalize_video_prompt_payload(prompt_json)
    compressed = dict(prompt)
    compressed["video_setup"] = _compress_setup(
        prompt.get("video_setup", ""),
        max_segments=3 if second_pass else 5,
        max_chars=90 if second_pass else 140,
    )
    compressed["execution_boundary"] = _compress_boundary(
        prompt.get("execution_boundary", ""),
        max_segments=3 if second_pass else 5,
        max_chars=90 if second_pass else 180,
    )

    shot_execution: List[Dict[str, Any]] = []
    boundary_text = compressed["execution_boundary"]
    seen_style_notes = set()
    for shot in prompt.get("shot_execution", []) or []:
        shot_content = _compress_descriptor(
            shot.get("shot_content", ""),
            max_segments=2 if second_pass else 3,
            max_chars=34 if second_pass else 52,
        )
        person_action = _compress_descriptor(
            shot.get("person_action", ""),
            max_segments=1 if second_pass else 2,
            max_chars=16 if second_pass else 28,
        )
        style_note = (
            ""
            if second_pass
            else _compress_style_note(
                shot.get("style_note", ""),
                boundary_text,
                shot_content,
                person_action,
                max_chars=14,
            )
        )
        style_key = re.sub(r"[\s；;，,。:：、\-]", "", style_note)
        if style_key and style_key in seen_style_notes:
            style_note = ""
        elif style_key:
            seen_style_notes.add(style_key)
        shot_execution.append(
            {
                **shot,
                "duration": _compact_text(shot.get("duration", "")),
                "shot_content": shot_content,
                "voiceover_text_target_language": _compress_voiceover(
                    shot.get("voiceover_text_target_language", ""),
                    max_chars=28 if second_pass else 42,
                ),
                "voiceover_text_zh": ""
                if second_pass
                else _compress_voiceover(shot.get("voiceover_text_zh", ""), max_chars=20),
                "spoken_line_task": _compact_text(shot.get("spoken_line_task", "")),
                "person_action": person_action,
                "style_note": style_note,
            }
        )
    compressed["shot_execution"] = shot_execution
    return compressed


def _hard_trim_video_prompt(prompt_json: Dict[str, Any], hard_max_chars: int) -> Dict[str, Any]:
    prompt = _normalize_video_prompt_payload(prompt_json)
    trimmed = dict(prompt)
    shots = _merge_proof_shots(prompt.get("shot_execution", []) or [])
    aggressively_trimmed: List[Dict[str, Any]] = []
    for shot in shots:
        aggressively_trimmed.append(
            {
                **shot,
                "shot_content": _compress_descriptor(shot.get("shot_content", ""), max_segments=2, max_chars=24),
                "voiceover_text_target_language": _compress_voiceover(
                    shot.get("voiceover_text_target_language", ""), max_chars=20
                ),
                "voiceover_text_zh": "",
                "person_action": _compress_descriptor(shot.get("person_action", ""), max_segments=1, max_chars=12),
                "style_note": "",
            }
        )
    trimmed["shot_execution"] = aggressively_trimmed
    trimmed["video_setup"] = _compress_setup(prompt.get("video_setup", ""), max_segments=2, max_chars=60)
    trimmed["execution_boundary"] = _compress_boundary(prompt.get("execution_boundary", ""), max_segments=2, max_chars=60)

    rendered = _render_final_video_prompt_core(trimmed)
    if len(rendered) <= hard_max_chars:
        return trimmed

    trimmed["video_setup"] = _truncate_text(trimmed.get("video_setup", ""), 40)
    trimmed["execution_boundary"] = _truncate_text(trimmed.get("execution_boundary", ""), 40)
    final_shots: List[Dict[str, Any]] = []
    for shot in trimmed.get("shot_execution", []) or []:
        final_shots.append(
            {
                **shot,
                "shot_content": _truncate_text(shot.get("shot_content", ""), 18),
                "voiceover_text_target_language": _truncate_text(shot.get("voiceover_text_target_language", ""), 14),
                "person_action": _truncate_text(shot.get("person_action", ""), 10),
            }
        )
    trimmed["shot_execution"] = final_shots
    return trimmed


def compress_final_video_prompt_payload(
    prompt_json: Dict[str, Any],
    preferred_max_chars: int = FINAL_VIDEO_PROMPT_PREFERRED_CHARS,
    hard_max_chars: int = FINAL_VIDEO_PROMPT_MAX_CHARS,
) -> Dict[str, Any]:
    compressed = _compress_video_prompt_pass(prompt_json, second_pass=False)
    rendered = _render_final_video_prompt_core(compressed)
    if len(rendered) <= preferred_max_chars:
        return compressed
    if len(rendered) <= hard_max_chars:
        return compressed

    compressed = _compress_video_prompt_pass(compressed, second_pass=True)
    rendered = _render_final_video_prompt_core(compressed)
    if len(rendered) <= hard_max_chars:
        return compressed

    compressed = _hard_trim_video_prompt(compressed, hard_max_chars=hard_max_chars)
    rendered = _render_final_video_prompt_core(compressed)
    if len(rendered) <= hard_max_chars:
        return compressed
    return compressed


def render_anchor_card(anchor_card: Dict[str, Any]) -> str:
    hard_anchors = anchor_card.get("hard_anchors", []) or []
    display_anchors = anchor_card.get("display_anchors", []) or []
    distortion_alerts = anchor_card.get("distortion_alerts", []) or []
    candidate_selling_points = anchor_card.get("candidate_primary_selling_points", []) or []
    parameter_anchors = anchor_card.get("parameter_anchors", []) or []
    structure_anchors = anchor_card.get("structure_anchors", []) or []
    operation_anchors = anchor_card.get("operation_anchors", []) or []
    fixation_result_anchors = anchor_card.get("fixation_result_anchors", []) or []
    before_after_result_anchors = anchor_card.get("before_after_result_anchors", []) or []
    scene_usage_anchors = anchor_card.get("scene_usage_anchors", []) or []

    sections = [
            "【产品锚点卡】\n"
            + _stringify_lines(
                [
                    f"- 产品定位：{anchor_card.get('product_positioning_one_liner', '')}",
                ]
                + _render_dict_items(hard_anchors, ["anchor", "reason_not_changeable", "confidence"])
            ),
            "【展示锚点】\n" + _stringify_lines(_render_dict_items(display_anchors, ["anchor", "why_must_show", "recommended_shot_type"])),
            "【候选主卖点】\n" + _stringify_lines(_render_dict_items(candidate_selling_points, ["selling_point", "how_to_show"])),
            "【失真警报】\n" + _stringify_lines([f"- {item}" for item in distortion_alerts if item]),
        ]
    if parameter_anchors:
        sections.insert(
            3,
            "【参数锚点】\n"
            + _stringify_lines(
                _render_dict_items(parameter_anchors, ["parameter_name", "parameter_value", "why_must_preserve"])
            ),
        )
    if any([structure_anchors, operation_anchors, fixation_result_anchors, before_after_result_anchors, scene_usage_anchors]):
        sections.append(
            "【发饰专用锚点】\n"
            + _stringify_lines(
                [
                    f"- 结构锚点：{'；'.join(structure_anchors)}",
                    f"- 操作锚点：{'；'.join(operation_anchors)}",
                    f"- 固定结果锚点：{'；'.join(fixation_result_anchors)}",
                    f"- 前后变化锚点：{'；'.join(before_after_result_anchors)}",
                    f"- 使用场景锚点：{'；'.join(scene_usage_anchors)}",
                ]
            )
        )

    return "\n\n".join(sections).strip()


def render_strategy_card(strategy: Dict[str, Any]) -> str:
    return "\n".join(
        [
            f"【{strategy.get('strategy_id', '')} 内容强策略卡】",
            f"- 方向名称：{strategy.get('strategy_name', '')}",
            f"- 主卖点：{strategy.get('primary_selling_point', '')}",
            f"- 用户主问题：{strategy.get('dominant_user_question', '')}",
            f"- Proof 论点：{strategy.get('proof_thesis', '')}",
            f"- Decision 论点：{strategy.get('decision_thesis', '')}",
            f"- 开场模式：{strategy.get('opening_mode', '')}",
            f"- 选中首镜策略：{strategy.get('selected_opening_strategy_name', '')}",
            f"- 开场首镜：{strategy.get('opening_first_shot', '')}",
            f"- 证明模式：{strategy.get('proof_mode', '')}",
            f"- 证明方法：{strategy.get('selling_point_proof_method', '') or strategy.get('core_proof_method', '')}",
            f"- 结尾模式：{strategy.get('ending_mode', '')}",
            f"- 场景子空间：{strategy.get('scene_subspace', '')}",
            f"- 场景建议：{strategy.get('scene_suggestion', '')}",
            f"- 场景功能：{strategy.get('scene_function', '')}",
            f"- 视觉进入方式：{strategy.get('visual_entry_mode', '')}",
            f"- 节奏：{strategy.get('rhythm_signature', '')}",
            f"- 人设状态：{strategy.get('persona_state', '')}",
            f"- 人物建议：{strategy.get('persona_state_suggestion', '')}",
            f"- 穿搭完成度：{strategy.get('styling_completion_tag', '')}",
            f"- 人物视觉气质：{strategy.get('persona_visual_tone', '')}",
            f"- 穿搭关键锚点：{strategy.get('styling_key_anchor', '')}",
            f"- 情绪推进轨迹：{strategy.get('emotion_arc_tag', '')}",
            f"- 动作进入方式：{strategy.get('action_entry_mode', '')}",
            f"- 穿搭底盘逻辑：{strategy.get('styling_base_logic', '')}",
            f"- 穿搭底盘限制：{'；'.join(strategy.get('styling_base_constraints', []) or [])}",
            f"- 开头情绪：{strategy.get('opening_emotion', '')}",
            f"- 中段情绪：{strategy.get('middle_emotion', '')}",
            f"- 结尾情绪：{strategy.get('ending_emotion', '')}",
            f"- 口播方式：{strategy.get('voiceover_style', '')}",
            f"- 商品主次关系：{strategy.get('product_dominance_rule', '')}",
            f"- 风险提醒：{strategy.get('risk_note', '')}",
        ]
    ).strip()


def render_strategy_progress_preview(
    anchor_card: Dict[str, Any],
    strategy: Dict[str, Any],
    include_anchor_card: bool = False,
) -> str:
    parts = ["【中间结果回写｜正式脚本尚未生成】"]
    if include_anchor_card:
        parts.append(render_anchor_card(anchor_card))
    parts.append(render_strategy_card(strategy))
    return "\n\n".join(part for part in parts if part).strip()


def render_internal_script(script_json: Dict[str, Any]) -> str:
    storyboard = script_json.get("storyboard", []) or []
    constraints = script_json.get("execution_constraints", {}) or {}
    opening_design = script_json.get("opening_design", {}) or {}
    content_id = str(script_json.get("content_id", "") or "").strip()

    storyboard_lines = []
    for item in storyboard:
        shot_no = item.get("shot_no", "")
        subtitle_target = str(item.get("subtitle_text_target_language", "") or "").strip()
        subtitle_zh = str(item.get("subtitle_text_zh", "") or "").strip()
        voiceover_target = str(item.get("voiceover_text_target_language", "") or "").strip()
        voiceover_zh = str(item.get("voiceover_text_zh", "") or "").strip()
        parts = [
            f"镜头{shot_no}：",
            f"- 时长：{item.get('duration', '')}",
            f"- 镜头内容：{item.get('shot_content', '')}",
            f"- 镜头目的：{item.get('shot_purpose', '')}",
            f"- 人物动作：{item.get('person_action', '')}",
            f"- 口播任务：{item.get('spoken_line_task', '')}",
            f"- 风格提醒：{item.get('style_note', '')}",
            f"- 镜头任务：{item.get('task_type', '')}",
        ]
        if subtitle_target:
            parts.append(f"- 字幕（当地语言）：{subtitle_target}")
        if subtitle_zh:
            parts.append(f"- 字幕（中文）：{subtitle_zh}")
        if voiceover_target:
            parts.append(f"- 口播（当地语言）：{voiceover_target}")
        if voiceover_zh:
            parts.append(f"- 口播（中文）：{voiceover_zh}")
        storyboard_lines.append("\n".join(part for part in parts if part))

    constraint_lines = [
        f"画面风格约束：{constraints.get('visual_style', '')}",
        f"人物约束：{constraints.get('person_constraints', '')}",
        f"穿搭约束：{constraints.get('styling_constraints', '')}",
        f"气质完成度约束：{constraints.get('tone_completion_constraints', '')}",
        f"场景约束：{constraints.get('scene_constraints', '')}",
        f"情绪推进约束：{constraints.get('emotion_progression_constraints', '')}",
        f"镜头执行重点：{constraints.get('camera_focus', '')}",
        f"商品优先原则：{constraints.get('product_priority_principle', '')}",
        f"真实性执行原则：{constraints.get('realism_principle', '')}",
    ]

    return "\n\n".join(
        [
            f"【脚本ID】\n- {content_id}" if content_id else "",
            "【开头设计】\n"
            + _stringify_lines(
                [
                    f"- 首镜模式：{opening_design.get('opening_mode', '')}",
                    f"- 首镜画面：{opening_design.get('first_frame', '')}",
                    f"- 开头表达切入口：{opening_design.get('expression_entry', '')}",
                    f"- 开头第一句类型：{opening_design.get('first_line_type', '')}",
                ]
            ),
            f"【分镜】\n{_stringify_lines(storyboard_lines)}",
            f"【执行约束】\n{_stringify_lines(constraint_lines)}",
        ]
    ).strip()


def render_script_v2(script_json: Dict[str, Any]) -> str:
    return render_internal_script(script_json)


def render_variant_script(variant: Dict[str, Any]) -> str:
    final_prompt = variant.get("final_video_script_prompt", {}) or {}
    video_setup = final_prompt.get("video_setup", {}) or {}
    shots = final_prompt.get("shot_execution", []) or []
    style_boundaries = final_prompt.get("style_boundaries", []) or []
    content_id = str(variant.get("content_id", "") or final_prompt.get("content_id", "") or "").strip()

    shot_lines = []
    for item in shots:
        shot_no = item.get("shot_no", "")
        parts = [
            f"镜头{shot_no}：",
            f"- 时长：{item.get('duration', '')}",
            f"- 画面内容：{item.get('visual', '')}",
            f"- 人物动作：{item.get('person_action', '')}",
            f"- 商品展示重点：{item.get('product_focus', '')}",
        ]
        voiceover = str(item.get("voiceover", "") or "").strip()
        if voiceover:
            parts.append(f"- 口播：{voiceover}")
        shot_lines.append("\n".join(parts))

    return "\n\n".join(
        [
            f"【脚本ID】\n- {content_id}" if content_id else "",
            "【视频整体设定】\n"
            + _stringify_lines(
                [
                    f"- 视频主题：{video_setup.get('video_theme', '')}",
                    f"- 商品主角设定：{video_setup.get('product_focus', '')}",
                    f"- 人物呈现：{video_setup.get('person_final', '')}",
                    f"- 穿搭呈现：{video_setup.get('outfit_final', '')}",
                    f"- 场景呈现：{video_setup.get('scene_final', '')}",
                    f"- 情绪呈现：{video_setup.get('emotion_final', '')}",
                    f"- 整体风格：{video_setup.get('overall_style', '')}",
                ]
            ),
            f"【分镜执行】\n{_stringify_lines(shot_lines)}",
            "【统一风格边界】\n"
            + _stringify_lines([f"- {item}" for item in style_boundaries if item]),
        ]
    ).strip()


def render_final_video_prompt(prompt_json: Dict[str, Any]) -> str:
    prompt = compress_final_video_prompt_payload(prompt_json)
    return _render_final_video_prompt_core(prompt)


def render_video_prompt(prompt_json: Dict[str, Any]) -> str:
    return render_final_video_prompt(prompt_json)


def render_failed_script(rendered_script: str, review_json: Dict[str, Any]) -> str:
    return rendered_script.strip()


def render_skipped_video_prompt(reason: str) -> str:
    return "\n".join(
        [
            "【最终视频提示词未生成】",
            f"- 原因：{reason}",
            "- 说明：该方向脚本未通过质检，因此只保留最后一版脚本与质检结果，不生成最终视频提示词。",
        ]
    ).strip()


def render_script(script_json: Dict[str, Any]) -> str:
    return render_internal_script(script_json)


def build_summary(
    anchor_card: Dict[str, Any],
    final_s1: Dict[str, Any],
    final_s2: Dict[str, Any],
    final_s3: Dict[str, Any],
    final_s4: Dict[str, Any],
) -> str:
    product_positioning = str(anchor_card.get("product_positioning_one_liner", "") or "").strip()
    if not product_positioning:
        hard_anchor_items = anchor_card.get("hard_anchors", []) or []
        if isinstance(hard_anchor_items, list):
            product_positioning = " / ".join(
                str(item.get("anchor", "") or "").strip()
                for item in hard_anchor_items[:3]
                if isinstance(item, dict) and str(item.get("anchor", "") or "").strip()
            )

    strategy_lines = []
    for strategy in [final_s1, final_s2, final_s3, final_s4]:
        strategy_lines.append(
            " / ".join(
                [
                    str(strategy.get("strategy_id") or strategy.get("final_strategy_id") or "").strip(),
                    str(strategy.get("strategy_name") or "").strip(),
                    str(strategy.get("primary_selling_point") or "").strip(),
                ]
            ).strip(" /")
        )

    return "\n".join(
        [
            f"产品锚点：{product_positioning}",
            "四套策略：",
            *(f"- {line}" for line in strategy_lines if line),
        ]
    ).strip()
