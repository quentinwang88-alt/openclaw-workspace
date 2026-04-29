#!/usr/bin/env python3
"""
脚本 JSON 渲染与摘要生成。
"""

import logging
import re
from typing import Any, Dict, List

from core.json_parser import _normalize_video_prompt_payload


FINAL_VIDEO_PROMPT_PREFERRED_CHARS = 1800
FINAL_VIDEO_PROMPT_MAX_CHARS = 2000
SEEDANCE_SCRIPT_PREFERRED_CHARS = 1900
SEEDANCE_SCRIPT_MAX_CHARS = 2000


logger = logging.getLogger(__name__)


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


def _is_anchor_priority_segment(segment: str) -> bool:
    text = _compact_text(segment)
    return text.startswith("商品锚点：") or text.startswith("锚点执行：") or text.startswith("锚点：")


def _join_limited_segments(value: Any, max_segments: int, max_chars: int) -> str:
    if max_chars <= 0 or max_segments <= 0:
        return ""
    all_segments = _semantic_segments(value)
    priority_segments = [segment for segment in all_segments if _is_anchor_priority_segment(segment)]
    normal_segments = [segment for segment in all_segments if not _is_anchor_priority_segment(segment)]
    segments = (priority_segments + normal_segments)[:max_segments]
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
    raw_note = _compact_text(style_note)
    if _is_anchor_priority_segment(raw_note):
        return _truncate_text(raw_note, max(max_chars, 18))
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


def _dedupe_preserve(items: List[str]) -> List[str]:
    deduped: List[str] = []
    seen = set()
    for item in items:
        text = _compact_text(item)
        key = re.sub(r"[\s；;，,。:：、\-｜|]", "", text)
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(text)
    return deduped


def _split_text_units(value: Any, split_commas: bool = False) -> List[str]:
    text = _compact_text(value)
    if not text:
        return []
    parts = re.split(r"[；;。\n]+", text)
    units: List[str] = []
    for part in parts:
        part = _compact_text(part)
        if not part:
            continue
        subparts = re.split(r"[，,]+", part) if split_commas else [part]
        for subpart in subparts:
            unit = _compact_text(subpart)
            if unit:
                units.append(unit)
    return _dedupe_preserve(units)


def _extract_negative_clauses(value: Any) -> List[str]:
    if isinstance(value, list):
        negatives: List[str] = []
        for item in value:
            negatives.extend(_extract_negative_clauses(item))
        return _dedupe_preserve(negatives)
    text = _compact_text(value)
    if not text:
        return []
    negatives = re.findall(r"(?:不要让|不要把|不要|不让|禁止|避免|不可|不能|不用|不做|不得|不允许|勿)[^；;。]+", text)
    if not negatives:
        for segment in _semantic_segments(value):
            if any(token in segment for token in ("不要", "不让", "禁止", "避免", "不可", "不能", "不用", "不做", "不得", "不允许", "勿")):
                negatives.append(segment)
    return _dedupe_preserve(negatives)


def _clean_negative_clause(value: str) -> str:
    text = _compact_text(value)
    text = re.sub(r"^(不要让|不要把|不要|不让|禁止|避免|不可|不能|不用|不做|不得|不允许|勿)", "", text)
    return text.strip(" ，；;。") or _compact_text(value)


def _normalize_age_text(value: str) -> str:
    text = _compact_text(value)
    text = text.replace("20+到30岁", "20-30岁")
    text = text.replace("20+至30岁", "20-30岁")
    return text


def _constraint_semantic_key(value: str) -> str:
    text = _compact_text(value)
    if not text:
        return ""
    groups = [
        (("喧宾夺主", "抢商品", "抢过商品", "压过耳饰", "压过商品", "不抢商品", "强过耳饰", "强过商品"), "人物抢戏"),
        (("夸张笑容", "大动作"), "夸张表演"),
        (("多余晃动",), "多余晃动"),
        (("过曝", "高光"), "局部过曝"),
        (("流苏", "一团"), "流苏粘连"),
        (("流苏", "粘连"), "流苏粘连"),
        (("耳部", "遮挡"), "耳部遮挡"),
        (("耳部", "遮住"), "耳部遮挡"),
        (("耳垂", "遮挡"), "耳部遮挡"),
        (("强滤镜", "闪烁", "转场", "特效"), "过强特效"),
        (("氛围镜", "静物镜", "佩戴结果"), "缺少佩戴结果"),
        (("首镜", "下垂部分"), "首镜缺完整结构"),
        (("包装主导", "无关环境"), "无关环境包装"),
        (("强网感推荐",), "强网感推荐"),
    ]
    for keywords, key in groups:
        if all(keyword in text for keyword in keywords):
            return key
        if len(keywords) == 1 and keywords[0] in text:
            return key
    return re.sub(r"[\s；;，,。:：、\-｜|]", "", text)


def _harden_negative_clause(value: str) -> str:
    text = _compact_text(value)
    if not text:
        return ""
    if any(keyword in text for keyword in ("喧宾夺主", "抢商品", "抢过商品", "压过耳饰", "压过商品", "强过耳饰", "强过商品")):
        if any(keyword in text for keyword in ("耳饰", "耳环", "耳钉", "耳坠", "耳夹")):
            return "人物不得抢过耳饰"
        return "人物不得抢过商品"
    if "夸张笑容" in text or "大动作" in text:
        return "禁止夸张笑容和大动作"
    if "多余晃动" in text:
        return "禁止多余晃动"
    if ("过曝" in text or "高光" in text) and any(keyword in text for keyword in ("透明心形", "双翼", "心形")):
        return "透明心形和浅色双翼禁止过曝"
    if "流苏" in text and any(keyword in text for keyword in ("一团", "粘连", "成束")):
        return "多股流苏必须保持分丝，禁止粘连成一束"
    if any(keyword in text for keyword in ("耳部", "耳垂", "小环连接处")) and any(
        keyword in text for keyword in ("遮挡", "遮住", "无遮挡")
    ):
        return "禁止遮挡耳部、耳垂与小环连接处"
    if any(keyword in text for keyword in ("强滤镜", "强闪烁", "特效转场", "夸张转场")):
        return "禁止强滤镜、夸张闪烁和特效转场"
    if any(keyword in text for keyword in ("氛围镜", "静物镜")) and "佩戴结果" in text:
        return "禁止只拍氛围或静物而缺少佩戴结果"
    if "首镜" in text and "下垂部分" in text:
        return "首镜不得只拍下垂部分，必须先交代完整结构"
    if "包装主导" in text or "无关环境" in text:
        return "禁止无关环境和包装主导画面"
    if "强网感推荐" in text:
        return "禁止强网感推荐"
    cleaned = _clean_negative_clause(text)
    if text.startswith(("禁止", "不得", "不允许")):
        return text
    return f"禁止{cleaned}"


def _extract_scene_label(visual_style: str, scene_constraints: str) -> str:
    merged = _compact_text(f"{visual_style} {scene_constraints}")
    patterns = [
        (r"家中窗边", "家中窗边"),
        (r"玄关镜前", "玄关镜前"),
        (r"衣柜区", "衣柜区"),
        (r"穿衣区", "穿衣区"),
        (r"梳妆台", "梳妆台"),
        (r"桌边窗前", "桌边窗前"),
        (r"窗边", "窗边"),
        (r"镜前", "镜前"),
    ]
    for pattern, label in patterns:
        if re.search(pattern, merged):
            return label
    source = _compact_text((visual_style or scene_constraints).split("，")[0])
    source = re.sub(r"^(限定在|固定在|场景固定在)", "", source)
    source = re.sub(r"(自然软光|自然光|柔光|奶油白.*|浅米背景.*)$", "", source)
    source = re.sub(r"(近距离分享场景|真实生活空间|场景)$", "", source)
    return _compact_text(source)


def _extract_lighting_label(visual_style: str, scene_constraints: str) -> str:
    merged = _compact_text(f"{visual_style} {scene_constraints}")
    patterns = ["自然软光", "窗边自然光", "自然光", "柔光", "暖光", "冷光", "日光"]
    for pattern in patterns:
        if pattern in merged:
            return pattern
    return ""


def _extract_background_label(visual_style: str) -> str:
    units = _split_text_units(visual_style, split_commas=True)
    for unit in units:
        if "背景" in unit:
            return unit
    return ""


def _extract_persona_label(person_constraints: str, tone_constraints: str) -> str:
    units = _split_text_units(person_constraints, split_commas=True)
    persona_parts: List[str] = []
    for unit in units:
        if any(keyword in unit for keyword in ("耳部", "耳垂", "发丝", "无遮挡", "露出", "比例", "不抢商品")):
            continue
        persona_parts.append(_normalize_age_text(unit))
    tone_map = ["轻分享型", "轻判断型", "问题解决型", "轻发现型"]
    tone_label = ""
    for item in tone_map:
        if item in tone_constraints:
            tone_label = f"{item}状态"
            break
    if tone_label and tone_label not in persona_parts:
        persona_parts.append(tone_label)
    return ",".join(_dedupe_preserve(persona_parts[:4]))


def _extract_wardrobe_label(styling_constraints: str) -> str:
    text = _compact_text(styling_constraints)
    if not text:
        return ""
    colors = []
    for color in ["奶白", "浅米", "雾粉", "浅灰蓝", "奶油白", "浅灰", "米白", "浅粉"]:
        if color in text and color not in colors and not any(color in existing for existing in colors):
            colors.append(color)
    garment = "低饱和纯色上衣" if "上衣" in text else "低饱和纯色穿搭"
    if "纯色" not in text and "低饱和" not in text:
        garment = _compress_descriptor(text, max_segments=1, max_chars=20)
    result = garment
    if colors:
        result = f"{garment}({ '/'.join(colors[:5]) })"
    if "领口干净利落" in text:
        result = f"{result},领口干净利落"
    return result


def _extract_hair_label(*values: Any) -> str:
    merged = _compact_text(" ".join(str(value or "") for value in values))
    match = re.search(r"(发丝[^，；;。]*耳后)", merged)
    if match:
        text = match.group(1).replace("必须", "").replace("已", "")
        return _compact_text(text)
    for token in ("低马尾", "半扎", "盘发", "耳后"):
        if token in merged:
            return token if token != "耳后" else "发丝拨到耳后"
    return ""


def _extract_critical_constraint(*values: Any) -> str:
    merged = _compact_text(" ".join(str(value or "") for value in values))
    match = re.search(r"(至少一侧[^，；;。]*(?:露出|无遮挡))", merged)
    if match:
        return _compact_text(match.group(1)).replace("必须", "")
    if "耳部" in merged and "无遮挡" in merged:
        return "耳部关键结构无遮挡"
    return ""


def _extract_exclusion_label(*values: Any) -> str:
    merged = _compact_text(" ".join(str(value or "") for value in values))
    match = re.search(r"(除当前[^，；;。]*首饰)", merged)
    if match:
        text = match.group(1).replace("不再", "不").replace("叠加", "叠加")
        return _compact_text(text)
    if "无其他首饰" in merged:
        return "无其他首饰"
    return ""


def _extract_material_tone(*values: Any) -> str:
    merged = _compact_text("；".join(str(value or "") for value in values))
    if not merged:
        return ""
    if "金色" in merged:
        if "橘红" in merged:
            return "金色保持真实暖调不偏橘红"
        return "金色保持真实暖调"
    if "银色" in merged:
        return "银色保持真实冷调"
    units = _split_text_units(merged)
    for unit in units:
        if any(keyword in unit for keyword in ("材质", "色调", "透明", "珠尾")) and not any(
            keyword in unit for keyword in ("窗边", "摆动", "镜头", "耳部")
        ):
            return unit
    return ""


def _extract_known_structure_chain(*values: Any) -> str:
    merged = _compact_text(" ".join(str(value or "") for value in values))
    known = [
        ("小环", "小环"),
        ("透明心形", "透明心形"),
        ("浅色双翼", "浅色双翼"),
        ("双翼", "浅色双翼"),
        ("多股流苏", "多股流苏"),
        ("流苏", "多股流苏"),
        ("透明珠尾", "透明珠尾"),
        ("透明珠", "透明珠尾"),
        ("珠尾", "透明珠尾"),
    ]
    hits: List[str] = []
    for token, label in known:
        if token in merged and label not in hits:
            hits.append(label)
    if len(hits) >= 2:
        return "→".join(hits)
    return ""


def _collect_negative_keys_from_rendered_text(text: str) -> List[str]:
    keys: List[str] = []
    for clause in _extract_negative_clauses(text):
        key = _constraint_semantic_key(_harden_negative_clause(clause))
        if key:
            keys.append(key)
    for unit in _split_text_units(text):
        if any(keyword in unit for keyword in ("禁止", "不得", "不做", "不允许", "不能")):
            key = _constraint_semantic_key(unit)
            if key:
                keys.append(key)
    return _dedupe_preserve(keys)


def _extract_progression(value: Any) -> str:
    text = _compact_text(value)
    if not text:
        return ""
    text = re.sub(r"^(严格执行|执行|保持|维持)", "", text)
    match = re.search(r"([^；;。:：]{1,12}\s*→\s*[^；;。:：]{1,12}(?:\s*→\s*[^；;。:：]{1,12}){1,3})", text)
    if match:
        return _compact_text(match.group(1)).replace(" ", "")
    return _compress_descriptor(text, max_segments=2, max_chars=40)


def _infer_product_category(*values: Any) -> str:
    text = _compact_text(" ".join(str(value or "") for value in values))
    patterns = [
        (r"耳线|耳环|耳饰|耳钉|耳夹|耳坠", "耳饰"),
        (r"发夹|抓夹|鲨鱼夹|香蕉夹", "发夹"),
        (r"发饰|发箍|发圈|头箍|头绳|发带", "发饰"),
        (r"项链|吊坠", "项链"),
        (r"戒指", "戒指"),
        (r"手链|手环|手镯|手串", "手饰"),
        (r"包包|手提包|斜挎包|单肩包|托特包", "包包"),
    ]
    for pattern, label in patterns:
        if re.search(pattern, text):
            return label
    return "商品"


def _infer_quantity(*values: Any) -> str:
    text = _compact_text(" ".join(str(value or "") for value in values))
    if any(token in text for token in ("一对", "双只", "两只", "双耳", "一双")):
        return "一对"
    if any(token in text for token in ("单只", "一只")):
        return "单只"
    return ""


def _extract_structure_chain(*values: Any) -> str:
    candidates: List[str] = []
    for value in values:
        text = _compact_text(value)
        if not text:
            continue
        direct = re.findall(r"([^；;。]{1,18}(?:→[^；;。]{1,18}){1,6})", text)
        candidates.extend(direct)

        ordered = re.findall(r"按([^。；;]{2,50})的顺序", text)
        candidates.extend(ordered)

    for candidate in sorted(candidates, key=len, reverse=True):
        candidate = re.sub(r"^.*?按", "", candidate)
        candidate = re.sub(r"^(hook段|proof段|decision段)", "", candidate)
        candidate = re.sub(r"的顺序(?:读取|展示|呈现)?$", "", candidate)
        normalized = re.sub(r"再到|再往下|往下|然后|以及|并且|并", "→", candidate)
        normalized = re.sub(r"[、，,/和]\s*", "→", normalized)
        tokens = [
            _compact_text(token).strip("- ")
            for token in normalized.split("→")
            if _compact_text(token).strip("- ")
        ]
        tokens = _dedupe_preserve(tokens)
        if len(tokens) >= 2:
            return "→".join(tokens[:6])
    fallback = _compress_descriptor(_merge_brief_parts(*values), max_segments=2, max_chars=48)
    return fallback or "结构需连续可读"


def _format_seedance_duration(value: Any) -> str:
    seconds = _parse_duration_seconds(value)
    if seconds > 0:
        rounded = max(1, int(seconds + 0.5))
        return f"{rounded}s"
    return _truncate_text(value, 6) or "3s"


def _extract_action_keywords(storyboard: List[Dict[str, Any]]) -> List[str]:
    merged = " ".join(_compact_text(item.get("person_action", "")) for item in storyboard)
    keyword_specs = [
        ("轻转头", ["转头", "侧头"]),
        ("微点头", ["点头"]),
        ("平静观察", ["观察", "确认", "看"]),
        ("稳定持物", ["提起", "持", "拿", "托"]),
        ("轻微摆动", ["摆动", "晃动"]),
        ("稳定构图微动态", ["停留", "静置", "静止", "定格", "停住"]),
    ]
    hits: List[str] = []
    for label, tokens in keyword_specs:
        if any(token in merged for token in tokens):
            hits.append(label)
    return _dedupe_preserve(hits)


def _build_overall_line(constraints: Dict[str, Any], limits: Dict[str, int]) -> str:
    visual_style = _compact_text(constraints.get("visual_style", ""))
    scene_constraints = _compact_text(constraints.get("scene_constraints", ""))
    person_constraints = _compact_text(constraints.get("person_constraints", ""))
    styling_constraints = _compact_text(constraints.get("styling_constraints", ""))
    tone_constraints = _compact_text(constraints.get("tone_completion_constraints", ""))

    head = [
        "15秒",
        _truncate_text(_extract_scene_label(visual_style, scene_constraints), limits["scene"]),
        _truncate_text(_extract_lighting_label(visual_style, scene_constraints), limits["visual"]),
        _truncate_text(_extract_background_label(visual_style), limits["visual"]),
        _truncate_text(_extract_persona_label(person_constraints, tone_constraints), limits["person"]),
    ]
    tail = [
        _truncate_text(_extract_wardrobe_label(styling_constraints), limits["styling"]),
        _truncate_text(_extract_hair_label(person_constraints, styling_constraints), limits["tone"]),
        _truncate_text(_extract_critical_constraint(person_constraints, styling_constraints), limits["tone"]),
        _truncate_text(_extract_exclusion_label(person_constraints, styling_constraints), limits["tone"]),
    ]
    return "|".join(part for part in head if part) + (
        ";" + ";".join(part for part in tail if part) if any(tail) else ""
    )


def _build_product_line(script_json: Dict[str, Any], constraints: Dict[str, Any], limits: Dict[str, int]) -> str:
    storyboard = script_json.get("storyboard", []) or []
    first_shot = storyboard[0] if storyboard else {}
    category = _infer_product_category(
        constraints.get("product_priority_principle", ""),
        constraints.get("camera_focus", ""),
        first_shot.get("shot_content", ""),
    )
    quantity = _infer_quantity(first_shot.get("shot_content", ""), constraints.get("camera_focus", ""))
    structure = _extract_known_structure_chain(
        constraints.get("camera_focus", ""),
        constraints.get("product_priority_principle", ""),
        constraints.get("realism_principle", ""),
        " ".join(str(item.get("shot_content", "") or "") for item in storyboard),
    ) or _extract_structure_chain(
        constraints.get("camera_focus", ""),
        constraints.get("product_priority_principle", ""),
        first_shot.get("anchor_reference", ""),
        first_shot.get("shot_content", ""),
    )
    material_tone = _truncate_text(
        _extract_material_tone(constraints.get("realism_principle", ""), constraints.get("product_priority_principle", "")),
        limits["realism"],
    )
    prefix = f"{category}{quantity}" if quantity else category
    parts = [f"{prefix}:{_truncate_text(structure, limits['structure'])}"]
    if material_tone:
        parts.append(material_tone)
    parts.append("结构从上到下必须连续可读")
    return ";".join(part for part in parts if part)


def _build_shot_requirement(item: Dict[str, Any], boundary_text: str, limits: Dict[str, int]) -> str:
    style_note = _compress_style_note(
        item.get("style_note", ""),
        boundary_text,
        _compact_text(item.get("shot_content", "")),
        _compact_text(item.get("person_action", "")),
        max_chars=limits["requirement"],
    )
    if style_note:
        positive_units = [unit for unit in _split_text_units(style_note, split_commas=True) if not _extract_negative_clauses(unit)]
        style_note = _truncate_text("，".join(_dedupe_preserve(positive_units)), limits["requirement"])
    parts: List[str] = []
    if style_note:
        parts.append(style_note)
    local_negatives: List[str] = []
    for source in (item.get("style_note", ""), item.get("person_action", ""), item.get("shot_content", "")):
        for clause in _extract_negative_clauses(source):
            local_negatives.append(_harden_negative_clause(clause))
    for clause in _dedupe_preserve(local_negatives):
        key = _constraint_semantic_key(clause)
        if key == "人物抢戏":
            continue
        parts.append(clause)
    if not parts:
        fallback = _compress_descriptor(item.get("anchor_reference", ""), max_segments=1, max_chars=limits["requirement"])
        parts.append(fallback or "无")
    return _truncate_text("；".join(_dedupe_preserve(parts)), limits["requirement"] * 2) or "无"


def _build_emotion_line(script_json: Dict[str, Any], constraints: Dict[str, Any]) -> str:
    storyboard = script_json.get("storyboard", []) or []
    progression = _extract_progression(constraints.get("emotion_progression_constraints", "")) or "情绪平稳推进"
    allowed_actions = _extract_action_keywords(storyboard)
    disallowed: List[str] = []
    for item in _extract_negative_clauses(constraints.get("emotion_progression_constraints", "")):
        key = _constraint_semantic_key(item)
        if key == "夸张表演":
            disallowed.append("夸张笑容和大动作")
        elif key == "人物抢戏":
            disallowed.append("人物抢过耳饰")
    parts = [progression]
    if allowed_actions:
        parts.append(f"只做{'/'.join(allowed_actions[:4])}")
    if disallowed:
        parts.append(f"不做{'/'.join(_dedupe_preserve(disallowed)[:4])}")
    return ";".join(parts)


def _build_rhythm_line(storyboard: List[Dict[str, Any]]) -> str:
    cursor = 0.0
    hook_end = 0.0
    proof_start = None
    proof_end = None
    decision_start = None
    for item in storyboard:
        duration = _parse_duration_seconds(item.get("duration"))
        task_text = str(item.get("spoken_line_task", "") or item.get("task_type", "")).strip().lower()
        start = cursor
        end = cursor + duration
        if "hook" in task_text and end > hook_end:
            hook_end = end
        if "proof" in task_text:
            proof_start = start if proof_start is None else min(proof_start, start)
            proof_end = end if proof_end is None else max(proof_end, end)
        if "decision" in task_text and decision_start is None:
            decision_start = start
        cursor = end

    parts: List[str] = []
    if hook_end > 0:
        parts.append(f"hook在前{max(1, int(hook_end + 0.5))}秒内完成")
    if proof_start is not None and proof_end is not None:
        start_no = max(1, int(proof_start + 0.5))
        end_no = max(start_no, int(proof_end + 0.5))
        parts.append(f"proof镜头在第{start_no}-{end_no}秒之间展开")
    if decision_start is not None:
        parts.append(f"decision信号在第{max(1, int(decision_start + 0.5))}秒前出现")
    return ";".join(parts) or "按15秒单条节奏推进"


def _build_audio_line(script_json: Dict[str, Any], max_chars: int = 180) -> str:
    audio_layer = script_json.get("audio_layer") if isinstance(script_json.get("audio_layer"), dict) else {}
    if not audio_layer:
        return ""
    bgm_style = _compact_text(audio_layer.get("bgm_style", ""))
    bgm_energy = _compact_text(audio_layer.get("bgm_energy", ""))
    voiceover_priority = _compact_text(audio_layer.get("voiceover_priority", ""))
    cues = audio_layer.get("sfx_cues") if isinstance(audio_layer.get("sfx_cues"), list) else []
    cue_parts: List[str] = []
    for cue in cues[:3]:
        if not isinstance(cue, dict):
            continue
        cue_text = _merge_brief_parts(
            cue.get("time_range", ""),
            cue.get("sfx_type", ""),
            cue.get("purpose", ""),
            cue.get("volume_note", ""),
        )
        if cue_text:
            cue_parts.append(cue_text)
    negatives = audio_layer.get("audio_negative_constraints")
    negative_text = ""
    if isinstance(negatives, list):
        negative_text = " / ".join(_compact_text(item) for item in negatives[:3] if _compact_text(item))
    line = _merge_brief_parts(
        f"BGM:{bgm_style or '低存在感日常'}",
        f"能量:{bgm_energy or 'low'}",
        f"口播优先:{voiceover_priority or 'high'}",
        f"SFX:{'；'.join(cue_parts)}" if cue_parts else "",
        f"混音:{_compact_text(audio_layer.get('mix_note', ''))}" if _compact_text(audio_layer.get("mix_note", "")) else "",
        f"禁:{negative_text}" if negative_text else "",
    )
    return _truncate_text(line, max_chars)


def _build_forbidden_line(
    script_json: Dict[str, Any],
    constraints: Dict[str, Any],
    used_negative_keys: List[str],
    max_chars: int,
) -> str:
    negatives: List[str] = []
    negatives.extend(_extract_negative_clauses(script_json.get("negative_constraints", [])))
    for key, value in constraints.items():
        if key in {"visual_style", "person_constraints", "styling_constraints"}:
            negatives.extend(_extract_negative_clauses(value))
            continue
        if key in {"tone_completion_constraints", "scene_constraints", "emotion_progression_constraints", "realism_principle"}:
            negatives.extend(_extract_negative_clauses(value))

    rendered: List[str] = []
    seen_keys = set(used_negative_keys)
    for clause in negatives:
        hardened = _harden_negative_clause(clause)
        key = _constraint_semantic_key(hardened)
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        rendered.append(hardened)
    forbidden = "；".join(_dedupe_preserve(rendered))
    return _truncate_text(forbidden, max_chars) or "无"


def _infer_variant_shot_task(index: int, total: int) -> str:
    if index <= 1:
        return "hook"
    if index >= total:
        return "decision"
    return "proof"


def _variant_boundary_to_negative(boundary: str) -> str:
    text = _compact_text(boundary)
    if not text:
        return ""
    if _extract_negative_clauses(text):
        return text
    if "原生自然" in text:
        return "禁止强滤镜、夸张闪烁和特效转场"
    if "商品必须是主角" in text or ("商品" in text and "主角" in text):
        return "禁止人物抢过商品"
    if any(keyword in text for keyword in ("耳部完整露出", "耳部无遮挡", "耳部露出")):
        return "禁止遮挡耳部、耳垂与小环连接处"
    if "佩戴结果" in text:
        return "禁止只拍氛围或静物而缺少佩戴结果"
    if "家中自然分享" in text or "家中场景" in text:
        return "禁止脱离家中自然分享语境"
    return ""


def _build_variant_negative_constraints(style_boundaries: List[Any]) -> List[str]:
    negatives: List[str] = []
    for item in style_boundaries:
        text = _compact_text(item)
        if not text:
            continue
        extracted = _extract_negative_clauses(text)
        if extracted:
            negatives.extend(extracted)
            continue
        mapped = _variant_boundary_to_negative(text)
        if mapped:
            negatives.append(mapped)
    return _dedupe_preserve(negatives)


def _variant_to_seedance_script_json(variant: Dict[str, Any]) -> Dict[str, Any]:
    final_prompt = variant.get("final_video_script_prompt", {}) or {}
    video_setup = final_prompt.get("video_setup", {}) or {}
    shots = final_prompt.get("shot_execution", []) or []
    style_boundaries = final_prompt.get("style_boundaries", []) or []
    content_id = str(variant.get("content_id", "") or final_prompt.get("content_id", "") or "").strip()

    storyboard: List[Dict[str, Any]] = []
    total_shots = len(shots)
    merged_product_focus = _merge_brief_parts(
        video_setup.get("product_focus", ""),
        *[shot.get("product_focus", "") for shot in shots if isinstance(shot, dict)],
    )
    merged_style_boundaries = _merge_brief_parts(*style_boundaries)
    for index, shot in enumerate(shots, 1):
        if not isinstance(shot, dict):
            continue
        product_focus = _compact_text(shot.get("product_focus", ""))
        storyboard.append(
            {
                "shot_no": int(shot.get("shot_no", index) or index),
                "duration": _compact_text(shot.get("duration", "")),
                "shot_content": _compact_text(shot.get("visual", "")),
                "person_action": _compact_text(shot.get("person_action", "")),
                "voiceover_text_target_language": _compact_text(shot.get("voiceover", "")),
                "spoken_line_task": _infer_variant_shot_task(index, total_shots),
                "style_note": product_focus,
                "anchor_reference": product_focus,
            }
        )

    execution_constraints = {
        "visual_style": _merge_brief_parts(video_setup.get("overall_style", ""), video_setup.get("video_theme", "")),
        "person_constraints": _compact_text(video_setup.get("person_final", "")),
        "styling_constraints": _compact_text(video_setup.get("outfit_final", "")),
        "tone_completion_constraints": _compact_text(video_setup.get("emotion_final", "")),
        "scene_constraints": _compact_text(video_setup.get("scene_final", "")),
        "emotion_progression_constraints": _compact_text(video_setup.get("emotion_final", "")),
        "camera_focus": merged_product_focus,
        "product_priority_principle": merged_product_focus,
        "realism_principle": _merge_brief_parts(video_setup.get("overall_style", ""), merged_style_boundaries),
    }

    return {
        "content_id": content_id,
        "storyboard": storyboard,
        "execution_constraints": execution_constraints,
        "negative_constraints": _build_variant_negative_constraints(style_boundaries),
    }


def _render_seedance_script_pass(script_json: Dict[str, Any], limits: Dict[str, int]) -> str:
    storyboard = script_json.get("storyboard", []) or []
    constraints = script_json.get("execution_constraints", {}) or {}
    content_id = _compact_text(script_json.get("content_id", ""))
    boundary_text = _merge_brief_parts(
        constraints.get("visual_style", ""),
        constraints.get("person_constraints", ""),
        constraints.get("styling_constraints", ""),
        constraints.get("realism_principle", ""),
    )

    shot_blocks: List[str] = []
    shot_requirements: List[str] = []
    for idx, item in enumerate(storyboard, 1):
        task = _compact_text(item.get("spoken_line_task", "") or item.get("task_type", "")) or "none"
        requirement = _build_shot_requirement(item, boundary_text, limits)
        shot_requirements.append(requirement)
        shot_blocks.append(
            "\n".join(
                [
                    f"【镜头{idx}|{_format_seedance_duration(item.get('duration', ''))}|{task}】",
                    f"画面:{_compress_descriptor(item.get('shot_content', ''), max_segments=2, max_chars=limits['shot_content']) or '无'}",
                    f"动作:{_compress_descriptor(item.get('person_action', ''), max_segments=1, max_chars=limits['shot_action']) or '无'}",
                    f"要求:{requirement}",
                    f"口播:{_compress_voiceover(item.get('voiceover_text_target_language', ''), max_chars=limits['voiceover']) or '无'}",
                ]
            )
        )

    overall_line = _build_overall_line(constraints, limits)
    if content_id:
        overall_line = f"脚本ID:{content_id};{overall_line}" if overall_line else f"脚本ID:{content_id}"

    emotion_line = _build_emotion_line(script_json, constraints)
    used_negative_keys = _collect_negative_keys_from_rendered_text(emotion_line)
    for requirement in shot_requirements:
        used_negative_keys.extend(_collect_negative_keys_from_rendered_text(requirement))

    audio_line = _build_audio_line(script_json)
    sections = [
        f"【整体】{overall_line}",
        f"【商品】{_build_product_line(script_json, constraints, limits)}",
        "\n\n".join(shot_blocks),
        f"【情绪】{emotion_line}",
        f"【节奏】{_build_rhythm_line(storyboard)}",
        f"【音频】{audio_line}" if audio_line else "",
        f"【禁止】{_build_forbidden_line(script_json, constraints, _dedupe_preserve(used_negative_keys), limits['forbidden'])}",
    ]
    return "\n\n".join(section for section in sections if section).strip()


def _render_seedance_script(script_json: Dict[str, Any]) -> str:
    passes = [
        {
            "scene": 56,
            "visual": 56,
            "person": 72,
            "styling": 64,
            "tone": 42,
            "structure": 56,
            "realism": 72,
            "shot_content": 84,
            "shot_action": 42,
            "requirement": 42,
            "voiceover": 90,
            "forbidden": 220,
        },
        {
            "scene": 44,
            "visual": 44,
            "person": 58,
            "styling": 50,
            "tone": 32,
            "structure": 46,
            "realism": 56,
            "shot_content": 68,
            "shot_action": 32,
            "requirement": 28,
            "voiceover": 78,
            "forbidden": 180,
        },
        {
            "scene": 36,
            "visual": 36,
            "person": 46,
            "styling": 40,
            "tone": 26,
            "structure": 38,
            "realism": 42,
            "shot_content": 56,
            "shot_action": 24,
            "requirement": 18,
            "voiceover": 68,
            "forbidden": 150,
        },
    ]

    rendered = ""
    for limits in passes:
        rendered = _render_seedance_script_pass(script_json, limits)
        if len(rendered) <= SEEDANCE_SCRIPT_PREFERRED_CHARS:
            return rendered
    if len(rendered) > SEEDANCE_SCRIPT_MAX_CHARS:
        raise ValueError(f"render_script output too long: {len(rendered)} > {SEEDANCE_SCRIPT_MAX_CHARS}")
    logger.warning("render_script output exceeded preferred chars: %s", len(rendered))
    return rendered


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
        raw_style_note = shot.get("style_note", "")
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
            _truncate_text(raw_style_note, 18)
            if second_pass and _is_anchor_priority_segment(_compact_text(raw_style_note))
            else ""
            if second_pass
            else _compress_style_note(
                raw_style_note,
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
    key_visual_constraints = anchor_card.get("key_visual_constraints", []) or []
    distortion_alerts = anchor_card.get("distortion_alerts", []) or []
    candidate_selling_points = anchor_card.get("candidate_primary_selling_points", []) or []
    parameter_anchors = anchor_card.get("parameter_anchors", []) or []
    structure_anchors = anchor_card.get("structure_anchors", []) or []
    operation_anchors = anchor_card.get("operation_anchors", []) or []
    fixation_result_anchors = anchor_card.get("fixation_result_anchors", []) or []
    before_after_result_anchors = anchor_card.get("before_after_result_anchors", []) or []
    scene_usage_anchors = anchor_card.get("scene_usage_anchors", []) or []
    hair_profile_lines = [
        f"- 发饰子类型：{anchor_card.get('hair_accessory_subtype', '')}",
        f"- 佩戴区域：{anchor_card.get('placement_zone', '')}",
        f"- 固定范围：{anchor_card.get('hold_scope', '')}",
        f"- 佩戴方向：{anchor_card.get('orientation', '')}",
        f"- 主要结果：{anchor_card.get('primary_result', '')}",
    ]

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
    if key_visual_constraints:
        sections.insert(
            2,
            "【关键视觉防错锚点】\n"
            + _stringify_lines(_render_dict_items(key_visual_constraints, ["constraint", "confidence", "basis"])),
        )
    if parameter_anchors:
        sections.insert(
            3,
            "【参数锚点】\n"
            + _stringify_lines(
                _render_dict_items(parameter_anchors, ["parameter_name", "parameter_value", "why_must_preserve"])
            ),
        )
    if any([structure_anchors, operation_anchors, fixation_result_anchors, before_after_result_anchors, scene_usage_anchors]) or any(
        str(anchor_card.get(key, "") or "").strip()
        for key in ("hair_accessory_subtype", "placement_zone", "hold_scope", "orientation", "primary_result")
    ):
        sections.append(
            "【发饰专用锚点】\n"
            + _stringify_lines(
                hair_profile_lines
                + [
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
            f"- AI可拍风险：{item.get('ai_shot_risk', 'low')}",
            f"- 替代镜头模板：{item.get('replacement_template_id', '')}",
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
    audio_layer = script_json.get("audio_layer") if isinstance(script_json.get("audio_layer"), dict) else {}
    sfx_lines = []
    for cue in (audio_layer.get("sfx_cues") if isinstance(audio_layer.get("sfx_cues"), list) else [])[:3]:
        if isinstance(cue, dict):
            sfx_lines.append(
                f"- {cue.get('time_range', '')} / {cue.get('sfx_type', '')} / {cue.get('purpose', '')} / {cue.get('volume_note', '')}"
            )
    audio_lines = [
        f"BGM风格：{audio_layer.get('bgm_style', '')}",
        f"BGM能量：{audio_layer.get('bgm_energy', '')}",
        f"口播优先级：{audio_layer.get('voiceover_priority', '')}",
        f"混音备注：{audio_layer.get('mix_note', '')}",
        "SFX提示：\n" + _stringify_lines(sfx_lines),
        "音频负向约束：" + "；".join(str(item or "") for item in (audio_layer.get("audio_negative_constraints") or []) if str(item or "").strip()),
    ] if audio_layer else []

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
            f"【音频层】\n{_stringify_lines(audio_lines)}" if audio_lines else "",
        ]
    ).strip()


def render_script_v2(script_json: Dict[str, Any]) -> str:
    return _render_seedance_script(script_json)


def render_variant_script(variant: Dict[str, Any]) -> str:
    return _render_seedance_script(_variant_to_seedance_script_json(variant))


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
    return _render_seedance_script(script_json)


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
