from __future__ import annotations

import re

from .contracts import ExecutionPlan, Shot


INLINE_TIME_RANGE = re.compile(
    r"(?P<start>\d+(?:\.\d+)?)\s*[-—–至~～]\s*(?P<end>\d+(?:\.\d+)?)\s*(?P<unit>s|秒)"
)


def _localize_body_timeline(shot: Shot, segment_start_ms: int, segment_end_ms: int) -> str:
    """Translate source-clock detail bullets to the segment clock.

    Untimed prose is preserved verbatim. A timed detail line outside a split segment is
    omitted so a long source shot does not ask every generated segment to repeat all
    of its actions.
    """
    rendered: list[str] = []
    for line in shot.body.splitlines():
        match = INLINE_TIME_RANGE.search(line)
        if not match:
            rendered.append(line)
            continue
        source_start = round(float(match.group("start")) * 1000)
        source_end = round(float(match.group("end")) * 1000)
        if source_start < shot.start_ms or source_end > shot.end_ms or source_end <= source_start:
            rendered.append(line)
            continue
        overlap_start = max(source_start, segment_start_ms)
        overlap_end = min(source_end, segment_end_ms)
        if overlap_end <= overlap_start:
            continue
        local_start = (overlap_start - segment_start_ms) / 1000
        local_end = (overlap_end - segment_start_ms) / 1000
        replacement = f"{local_start:g}-{local_end:g}{match.group('unit')}"
        rendered.append(line[:match.start()] + replacement + line[match.end():])
    return "\n".join(rendered).strip()


def render_segment_prompt(plan: ExecutionPlan, shots: list[Shot], start_ms: int, end_ms: int) -> str:
    lines = [
        f"生成一段 {(end_ms - start_ms) / 1000:g} 秒、9:16竖屏视频。",
        "本段来自一条完整复刻视频；只执行下列原稿内容，不增加剧情、卖点、台词、字幕或CTA。",
    ]
    if plan.global_requirements.strip():
        lines.extend(["", "【整片冻结要求】", plan.global_requirements.strip()])
    lines.extend(["", "【本段时间轴】"])
    for shot in shots:
        local_start = max(shot.start_ms, start_ms) - start_ms
        local_end = min(shot.end_ms, end_ms) - start_ms
        lines.append(f"【{local_start / 1000:g}-{local_end / 1000:g}秒｜{shot.shot_id}】")
        lines.append(_localize_body_timeline(shot, start_ms, end_ms))
    if plan.audio_mode in {"NO_VOICEOVER", "SOURCE_COPY_TTS"}:
        lines.extend(["", "本段视频画面不要自行生成可辨识人声；声音按全片冻结音频计划后期处理。"])
    return "\n".join(lines).strip()
