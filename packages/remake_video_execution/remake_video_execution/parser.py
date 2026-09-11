from __future__ import annotations

import re
from typing import Iterable

from .contracts import ExecutionPlan, Issue, Shot, SourceSnapshot


TIMED_HEADING = re.compile(
    r"【\s*(?P<start>\d+(?:\.\d+)?)\s*[-—–至~～]\s*(?P<end>\d+(?:\.\d+)?)\s*秒\s*】"
)
SPOKEN = re.compile(r"(?:口播/短句|口播|对白)\s*[：:]\s*(.+)")
SCREEN = re.compile(r"(?:屏幕文字|字幕|评论气泡)\s*[：:]\s*(.+)")
TRAILING_HEADING = re.compile(
    r"\n(?=【(?!\s*\d+(?:\.\d+)?\s*[-—–至~～]).+?】)"
)


def _extract(pattern: re.Pattern[str], text: str) -> list[str]:
    return [match.group(1).strip() for match in pattern.finditer(text) if match.group(1).strip()]


def _timed_shots(prompt: str) -> tuple[list[Shot], str]:
    matches = list(TIMED_HEADING.finditer(prompt))
    shots: list[Shot] = []
    trailing_requirements = ""
    for index, match in enumerate(matches):
        body_start = match.end()
        body_end = matches[index + 1].start() if index + 1 < len(matches) else len(prompt)
        if index + 1 == len(matches):
            tail = prompt[body_start:body_end]
            trailing = TRAILING_HEADING.search(tail)
            if trailing:
                split_at = body_start + trailing.start() + 1
                body_end = split_at
                trailing_requirements = prompt[split_at:].strip()
        body = prompt[body_start:body_end].strip()
        start_ms = round(float(match.group("start")) * 1000)
        end_ms = round(float(match.group("end")) * 1000)
        shots.append(Shot(
            shot_id=f"SHOT_{index + 1:02d}", start_ms=start_ms, end_ms=end_ms,
            body=body, source_span=match.group(0),
            spoken_lines=_extract(SPOKEN, body), screen_texts=_extract(SCREEN, body),
        ))
    return shots, trailing_requirements


def _structured_shots(source: SourceSnapshot) -> list[Shot]:
    structured = source.structured_source or {}
    rows = structured.get("shots") or structured.get("timeline") or structured.get("capture_units") or []
    if not isinstance(rows, list):
        return []
    result: list[Shot] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            return []
        start = row.get("start_ms")
        end = row.get("end_ms")
        if start is None and row.get("start_seconds") is not None:
            start = round(float(row["start_seconds"]) * 1000)
        if end is None and row.get("end_seconds") is not None:
            end = round(float(row["end_seconds"]) * 1000)
        try:
            start_ms, end_ms = int(start), int(end)
        except (TypeError, ValueError):
            return []
        body = str(
            row.get("body") or row.get("visual_content") or row.get("action") or row.get("prompt") or ""
        ).strip()
        spoken = row.get("spoken_lines") or row.get("voiceover") or []
        screen = row.get("screen_texts") or row.get("screen_text") or []
        if isinstance(spoken, str):
            spoken = [spoken]
        if isinstance(screen, str):
            screen = [screen]
        result.append(Shot(
            shot_id=str(row.get("shot_id") or row.get("capture_unit_id") or f"SHOT_{index + 1:02d}"),
            start_ms=start_ms, end_ms=end_ms, body=body,
            source_span=f"structured_source[{index}]",
            scene_id=str(row.get("scene_id") or ""),
            spoken_lines=[str(item).strip() for item in spoken if str(item).strip()],
            screen_texts=[str(item).strip() for item in screen if str(item).strip()],
        ))
    return result


def _audio_mode(source: SourceSnapshot, shots: Iterable[Shot]) -> str:
    prompt = source.raw_prompt
    if source.source_voiceover or any(shot.spoken_lines for shot in shots):
        return "SOURCE_COPY_TTS"
    if re.search(r"不安排(?:人声)?口播|无(?:明确|可辨识|清晰)?(?:人声)?口播|无可辨识(?:口播)?台词|纯画面|画面叙事为主", prompt):
        return "NO_VOICEOVER"
    if re.search(r"对口型|人物对白|多人对白", prompt):
        return "DIALOGUE_REQUIRES_PROVIDER"
    return "UNSPECIFIED"


def parse_source(source: SourceSnapshot) -> ExecutionPlan:
    prompt = source.raw_prompt
    shots, trailing_requirements = _timed_shots(prompt)
    if not shots:
        shots = _structured_shots(source)
    issues: list[Issue] = []
    if not prompt:
        issues.append(Issue("SOURCE_PROMPT_MISSING", "BLOCK", "缺少视频生成提示词"))
    if not shots and prompt:
        issues.append(Issue(
            "TIMELINE_UNSTRUCTURED", "BLOCK",
            "提示词没有可确定提取的时间轴，需要结构化来源或受限解析模型",
        ))
    if shots:
        if shots[0].start_ms != 0:
            issues.append(Issue("TIMELINE_START_GAP", "BLOCK", "时间轴没有从0秒开始", shots[0].source_span))
        for previous, current in zip(shots, shots[1:]):
            if current.start_ms != previous.end_ms:
                issues.append(Issue(
                    "TIMELINE_GAP_OR_OVERLAP", "BLOCK",
                    f"{previous.shot_id} 与 {current.shot_id} 时间不连续",
                    f"{previous.source_span} -> {current.source_span}",
                ))
        if source.duration_ms and shots[-1].end_ms != source.duration_ms:
            issues.append(Issue(
                "DURATION_MISMATCH", "BLOCK",
                f"字段时长 {source.duration_ms / 1000:g}s 与时间轴结尾 {shots[-1].end_ms / 1000:g}s 不一致",
                shots[-1].source_span,
            ))
    if not source.reference_manifest:
        issues.append(Issue(
            "REFERENCE_ASSETS_MISSING", "BLOCK",
            "总库行没有可冻结的统一首帧、人物图、产品图或参考图",
            affected_stage="PREFLIGHT",
        ))
    if not source.publish_purpose:
        issues.append(Issue("PUBLISH_PURPOSE_MISSING", "BLOCK", "发布用途为空", affected_stage="HANDOFF"))
    if not source.cart_enabled:
        issues.append(Issue("CART_SETTING_MISSING", "BLOCK", "是否挂车为空", affected_stage="HANDOFF"))
    visible_texts = [text for shot in shots for text in shot.screen_texts]
    if source.target_language in {"泰语", "Thai", "th", "TH"}:
        for text in visible_texts:
            if re.search(r"[\u4e00-\u9fff]", text):
                issues.append(Issue(
                    "VISIBLE_TEXT_LANGUAGE_MISMATCH", "BLOCK",
                    f"泰语任务包含中文观众可见文字：{text}",
                    affected_stage="SUBTITLE",
                ))
    return ExecutionPlan(
        source=source,
        duration_ms=source.duration_ms or (shots[-1].end_ms if shots else 0),
        aspect_ratio="9:16",
        requested_resolution="",
        shots=shots,
        global_requirements="\n\n".join(
            part for part in (prompt[: matches_start(prompt)].strip(), trailing_requirements) if part
        ),
        audio_mode=_audio_mode(source, shots),
        issues=issues,
    )


def matches_start(prompt: str) -> int:
    match = TIMED_HEADING.search(prompt)
    return match.start() if match else len(prompt)
