"""Compaction pass for the rendered video generation prompt.

Why this module exists
----------------------
``production_script_renderer`` builds the UGC-native prompt by concatenating
several *deterministic projections* of the same frozen contract.  Those
projections overlap on purpose -- the per-shot anchor list restates the
identity lock, the scene block restates the opening-frame projection, every
shot repeats the whole action spine -- which is good for robustness and bad
for a downstream model that rejects over-long prompts.  The legacy prompt
profile already enforces a budget (``FINAL_VIDEO_PROMPT_MAX_CHARS``); the
UGC-native profile did not, so a mixed-accessory script could render past
4000 characters and fail at the video stage.

The pass is deliberately written as a *lossless* compaction: every rule
removes text that is still present elsewhere in the same prompt, or text that
carries no instruction for a video model at all.  It never paraphrases the
category contracts it does not own.

Rules, applied in order
-----------------------
L1   drop placeholder values (``UNAVAILABLE`` / ``不适用`` / ``无``)
L2   drop a line whose value already appeared verbatim; lift the per-shot
     product-anchor lines into a single ``每段商品必须可见`` line **only when
     every shot asks for the same thing**
L2b  drop the in-shot camera line when it only restates that shot's own
     visible event
L3   shorten fixed template boilerplate (equivalent wording, same meaning)
L4   drop clauses that already appeared in a shot-level field
L5   drop protocol tokens that carry no instruction for a video model

Per-shot anchors must not be lifted across shots (Review #9)
-----------------------------------------------------------
``商品必须可见`` is emitted once per shot, and different shots legitimately name
different observation jobs: shot 1 may ask for 整体轮廓 while shot 2 asks for
背部耳针连接处.  The original implementation lifted the *first* line to a
film-level ``每段商品必须可见`` rule and deleted the rest, which did two wrong
things at once -- it asserted shot 1's requirement onto shots 2..N, and it
silently deleted their own.  The lift is now legal only when every shot's
value is identical after normalisation; otherwise every shot keeps its own
line, in its own block.

Hard guarantees (a compaction that breaks any of these is a defect):

* ``【商品负向约束】`` and ``【全片不露脸｜硬约束】`` are never touched -- the
  face-free section has to keep naming what it forbids.
* A line carrying a negative constraint (``不得`` / ``禁止`` / ``不出现``) is
  never dropped, so no guard can be silently removed.
* Shot-level dimensions, the action spine (``动作类型``/``开始状态``/``核心动作``),
  the carrier (``出镜方式``/``承载方式``), the crop scale (``商品观察尺度``), the
  identity lock (``必须保持``/``关键可见细节``) and the cut/continuity rules
  (``【直接剪切…】`` header, ``连续性``) are never weakened by de-duplication.
* Shot headers (``【拍摄片段01｜0.0-4.0s｜HOOK】``) carry the timeline and are
  byte-stable: they are never dropped and never rewritten to a shorter label.
* If the budget cannot be met, the compacted text is returned as-is and the
  report says ``over_limit``.  We do not truncate: a truncated contract is
  worse than an over-long one, and we do not silently drop a shot.
* :func:`audit_shot_protection` re-checks, per shot, that every protected
  constraint still belongs to *that* shot after compaction.  Checking that a
  keyword still occurs somewhere in the text is not enough -- a constraint
  that migrated from shot 1 to shot 4 would pass that weaker check.

Only prompts that exceed the budget are rewritten, so short prompts (every
other category in practice) are byte-identical to the previous behaviour.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

VIDEO_PROMPT_CHAR_LIMIT_ENV = "ORIGINAL_SCRIPT_VIDEO_PROMPT_CHAR_LIMIT"
DEFAULT_VIDEO_PROMPT_CHAR_LIMIT = 4000

# A value that means "the renderer had nothing to say here".
PLACEHOLDER_VALUES: Tuple[str, ...] = (
    "UNAVAILABLE",
    "N/A",
    "NA",
    "NONE",
    "不适用",
    "无",
    "-",
    "candidate-specific",
)

# Whole sections that must survive compaction byte for byte.
PROTECTED_SECTIONS: Tuple[str, ...] = ("【商品负向约束】", "【全片不露脸｜硬约束】")

# A line containing any of these is a guard, not filler.
KEEP_MARKERS: Tuple[str, ...] = ("不得", "禁止", "不出现", "不算", "画外")

# Protocol tokens whose value is an internal enum.  A video model cannot act
# on ``PRIMARY_PRODUCT_EVIDENCE``; the observable instruction is the shot's
# own 画面事件, which stays.
PROTOCOL_LABELS: Tuple[str, ...] = ("本段相对上一段的新信息", "参考策略")

# ------------------------------------------------ shot blocks (Review #9)
# ``【拍摄片段01｜0.0-4.0s｜HOOK】`` and its siblings.  The header carries the
# timeline, so it is byte-stable: never dropped, never shortened.
SHOT_HEADER_RE = re.compile(r"^【[^】]*?\d{1,2}\s*｜[^】]*】$")

SHOT_ANCHOR_LABEL = "商品必须可见"
GLOBAL_ANCHOR_LABEL = "每段商品必须可见"

# Labels whose value belongs to one shot and must never be promoted to a
# film-level rule or migrated to another shot.
PER_SHOT_PROTECTED_LABELS: Tuple[str, ...] = (
    "画面事件",
    "人物动作",
    "商品必须可见",
    "商品执行关系",
    "视线关系",
    "自然反应",
)

# ``本段手机构图`` is deliberately absent from the audited set: L2b may delete
# it when it merely restates *the same shot's* visible event, which moves
# nothing anywhere.
PER_SHOT_AUDIT_LABELS: Tuple[str, ...] = PER_SHOT_PROTECTED_LABELS

# Per-shot dimensions and the frozen authoring spine.  These are never dropped
# as duplicates and never have clauses stripped.
SHOT_CORE_LABELS: Tuple[str, ...] = (
    "画面事件",
    "人物动作",
    "本段手机构图",
    "视线关系",
    "自然反应",
    "商品必须可见",
    "每段商品必须可见",
    "本段相对上一段的新信息",
    "商品执行关系",
    "出镜方式",
    "承载方式",
    "商品观察尺度",
    "动作类型",
    "开始状态",
    "核心动作",
    "结束状态",
    "必须保持",
    "关键可见细节",
)

# Only descriptive/scene lines give way to clause-level de-duplication.
# Carrier, crop, cut/continuity and the action spine are *not* explanatory
# prose -- they are constraints -- so they are deliberately absent here.
L4_ELIGIBLE_LABELS: Set[str] = {
    "地点身份",
    "地点与时刻",
    "光线",
    "基础手机位置",
    "手机位置",
    "人物/商品位置",
    "背景层次",
    "生活痕迹",
    "现场背景",
    "背景锚点",
    "主体背后",
    "密集元素位置",
    "必要结果",
    "交互边界",
    "身份重点",
    "配饰道具",
    "商品角色",
}

# L3: fixed boilerplate, equivalent wording.  Key may be a whole line or a
# substring; an empty replacement deletes the line.
LINE_SHORTENINGS: Dict[str, str] = {
    "核心互动已冻结，按下方“本条动作主线”执行一次并服从交互边界": "按下方“本条动作主线”执行一次",
    "表达边界：允许展示并表达已经佩戴后的关系；不要求完整佩戴过程": "",
    "参考图只负责商品颜色、图案、形状和结构；参考图的整体曝光、滤镜、背景色调和其中人物均不是本条画面风格权威。": (
        "参考图只定商品颜色、图案、形状与结构；其曝光、滤镜、背景色调与人物都不是本条风格权威。"
    ),
    "人物身份、年龄感、体型、发型和自然肤质以人物模板参考为准；商品参考图只决定商品外观，其中模特、滤镜、姿态和背景均无人物权威。": (
        "人物身份、年龄感、体型、发型与肤质以人物模板参考为准；商品参考图不提供人物权威。"
    ),
    "普通用户使用手机竖屏随手记录，使用现场已有自然光或普通室内光；机位简单，允许轻微手持感、轻微构图不完美和真实环境层次，人物皮肤、衣物和背景保留自然质感": (
        "普通用户用手机竖屏随手记录，只用现场已有自然光；机位简单，容许轻微手持感与构图不完美，"
        "人物皮肤、衣物和背景保留自然质感。"
    ),
    "创作者本人在同一地点、同一时刻使用同一部手机分别录制3至5段简短素材；片段间使用普通直接剪切或自然跳剪，不同片段允许在同一小片区域重新放置手机、改变人物与手机距离或补录商品细节；成片片段数不等于手机布置数，人物、商品、穿搭、光线和生活状态保持连续": (
        "创作者本人用同一部手机在同一地点分3至5段录制；片段间普通直接剪切或自然跳剪，"
        "可在同一小片区域重新放置手机或补录细节；成片片段数不等于手机布置数，"
        "人物、商品、穿搭、光线与生活状态保持连续"
    ),
    "身份重点：单只或成对关系；佩戴连接结构；部件数量与排列顺序；相对长度和比例": (
        "身份重点：佩戴连接结构、部件数量与顺序、相对长度比例"
    ),
    "手部结构：画面中最多出现同一人物自然生长的两只手；双手参与时左右手各自完成一个连续角色，不出现第三只手、助手手臂、重复手掌或额外手指结构": (
        "手部结构：最多出现同一人物两只自然手，不出现第三只手、助手手臂或额外手指"
    ),
    "第一优先保持商品身份、佩戴状态、人物肢体和穿搭连续；第二优先完整执行以上独立可见片段并真实直接剪切；第三优先卖点关系与原生手机可行性。发生冲突时先简化场景陈设和人物表演，不得合并片段或退回一镜到底。": (
        "第一优先商品身份、佩戴状态与人物穿搭连续；第二优先完整执行以上片段并真实直接剪切；"
        "第三优先卖点关系与手机可行性。冲突时先简化场景与表演，不得合并片段或退回一镜到底。"
    ),
    "。只调整兼容内容段的景别，不改变结构、佩戴状态或动作主线。": (
        "；只调景别，不改结构、佩戴状态或动作主线。"
    ),
    "只允许随当前生活时刻产生自然表情、视线和小动作；不得重新设计脸、年龄、体型、发型或妆容等级。": (
        "只允许随当下产生自然表情、视线与小动作；不得重新设计脸、年龄、体型、发型或妆造。"
    ),
    "动态重点：开头在轻微自然变化中看清商品，中段执行上述核心动作，结尾在小幅变化或重新构图中回到商品，只在最后一瞬自然收住。": (
        "动态重点：开头在轻微变化中看清商品，中段执行核心动作，结尾回到商品并在最后一瞬收住。"
    ),
    "辅助生活衔接：只保留同一地点内的自然状态；最后一段不离场，结尾服从商品回收近景。": (
        "辅助衔接：只保留同一地点内的自然状态，最后一段不离场。"
    ),
}

# L3 for lines that carry a variable part.
REGEX_SHORTENINGS: Tuple[Tuple[Any, str], ...] = (
    (re.compile(r"^拍摄单元：以下\d+段是分别录制的普通手机素材.*$"), ""),
    (re.compile(r"^人物比例：(UNAVAILABLE|服从人物模板参考.*)$"), ""),
    (re.compile(r"^【直接剪切｜开始另一段独立手机素材】$"), "【直接剪切】"),
)


def video_prompt_char_limit() -> int:
    """Return the configured character budget for a rendered video prompt.

    ``0`` (or a negative / unparsable value) means "compaction disabled".
    """
    raw = os.environ.get(VIDEO_PROMPT_CHAR_LIMIT_ENV)
    if raw is None or not str(raw).strip():
        return DEFAULT_VIDEO_PROMPT_CHAR_LIMIT
    text = str(raw).strip().lower()
    if text in {"off", "false", "no", "disable", "disabled"}:
        return 0
    try:
        limit = int(text)
    except ValueError:
        return DEFAULT_VIDEO_PROMPT_CHAR_LIMIT
    return limit


def _label_of(line: str) -> str:
    if "：" not in line:
        return ""
    return line.partition("：")[0].strip()


def _value_of(line: str) -> str:
    if "：" not in line:
        return ""
    return line.partition("：")[2].strip()


def _clauses(value: str) -> List[str]:
    return [part for part in re.split(r"(?<=[；。])", value) if part.strip()]


# Whitespace and punctuation carry no meaning when asking "is this the same
# rule?".  Two shots that both say ``整体轮廓`` and ``整体轮廓。`` agree.
_CONSTRAINT_NOISE_RE = re.compile(r"[\s、，。；：（）()\[\]【】·\-—]+")


def _normalize_constraint(value: str) -> str:
    """Punctuation/whitespace-insensitive form of a constraint value."""
    return _CONSTRAINT_NOISE_RE.sub("", str(value or ""))


def _shot_block_index(lines: Sequence[str]) -> List[int]:
    """Which shot block each line belongs to; ``-1`` before the first header."""
    owner: List[int] = []
    current = -1
    for line in lines:
        if SHOT_HEADER_RE.match(line.strip()):
            current += 1
        owner.append(current)
    return owner


def _shot_blocks(lines: Sequence[str]) -> Dict[int, List[str]]:
    blocks: Dict[int, List[str]] = {}
    for index, line in zip(_shot_block_index(lines), lines):
        blocks.setdefault(index, []).append(line)
    return blocks


def audit_shot_protection(
    before: str,
    after: str,
    *,
    lifted: Sequence[str] = (),
) -> Tuple[str, ...]:
    """Per-shot ownership audit of protected constraints (Review #9).

    For every shot, each protected constraint present *before* must still be
    present in that same shot's block *after*, and must not have moved into
    another shot's block.  A whole-document keyword search cannot catch a
    constraint that migrated from shot 1 to shot 4.

    ``lifted`` names per-shot labels that were legitimately promoted to a
    film-level rule (only legal when every shot agreed).
    """
    lifted_set = {str(label) for label in lifted}
    before_blocks = _shot_blocks(str(before or "").split("\n"))
    after_blocks = _shot_blocks(str(after or "").split("\n"))

    problems: List[str] = []
    for shot in sorted(index for index in before_blocks if index >= 0):
        if shot not in after_blocks:
            problems.append(f"片段{shot + 1:02d}在压缩后整体消失")
            continue
        for label in PER_SHOT_AUDIT_LABELS:
            if label in lifted_set:
                continue
            wanted = {
                _normalize_constraint(_value_of(line))
                for line in before_blocks[shot]
                if _label_of(line) == label and _value_of(line)
            }
            if not wanted:
                continue
            here = {
                _normalize_constraint(_value_of(line))
                for line in after_blocks[shot]
                if _label_of(line) == label and _value_of(line)
            }
            for value in sorted(wanted - here):
                moved_to = [
                    other + 1
                    for other, lines in after_blocks.items()
                    if other != shot
                    and any(
                        _label_of(line) == label
                        and _normalize_constraint(_value_of(line)) == value
                        for line in lines
                    )
                ]
                if moved_to:
                    problems.append(
                        f"片段{shot + 1:02d}的「{label}」约束被挪到了片段"
                        + "、".join(f"{target:02d}" for target in moved_to)
                        + f"：{value}"
                    )
                else:
                    problems.append(
                        f"片段{shot + 1:02d}的「{label}」约束丢失：{value}"
                    )
    return tuple(problems)


@dataclass(frozen=True)
class CompactionReport:
    """Outcome of one compaction pass.

    ``text`` is what callers send downstream.  The remaining fields exist so an
    over-limit contract is *visible* rather than silent: we never truncate, so
    the only honest thing to do is report it.
    """

    text: str
    original_chars: int
    limit: int
    applied: bool
    over_limit: bool
    protected_dropped: Tuple[str, ...] = ()
    shot_migrations: Tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        """No protected constraint was dropped or migrated."""
        return not self.protected_dropped and not self.shot_migrations


def _section_flags(lines: List[str]) -> List[bool]:
    flags: List[bool] = []
    guard = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("【") and stripped.endswith("】"):
            guard = stripped in PROTECTED_SECTIONS
        flags.append(guard)
    return flags


def _fold_blank(lines: List[str]) -> str:
    out: List[str] = []
    for line in lines:
        if not line.strip() and out and not out[-1].strip():
            continue
        out.append(line)
    while out and not out[0].strip():
        out.pop(0)
    while out and not out[-1].strip():
        out.pop()
    return "\n".join(out)


def compact_video_prompt(prompt: str, limit: Optional[int] = None) -> str:
    """Return ``prompt`` compacted to ``limit`` characters.

    ``limit`` defaults to :func:`video_prompt_char_limit`.  A prompt that
    already fits is returned unchanged.

    Callers that need to know whether the result is still over budget, or
    whether any protected constraint moved, should use
    :func:`compact_video_prompt_report` instead -- the string alone cannot
    express that.
    """
    return compact_video_prompt_report(prompt, limit=limit).text


def compact_video_prompt_report(
    prompt: str, limit: Optional[int] = None
) -> CompactionReport:
    """Compact ``prompt`` and report what the pass did and could not do.

    Never truncates.  If the compaction rules cannot bring the text under
    ``limit`` without dropping a constraint, the text is returned over budget
    and ``over_limit`` is ``True``; the per-shot ownership audit is run
    regardless, so a silent shot-level loss cannot pass unnoticed.
    """
    if limit is None:
        limit = video_prompt_char_limit()
    text = str(prompt or "")
    resolved_limit = int(limit) if limit is not None else 0
    if limit is None or resolved_limit <= 0 or len(text) <= resolved_limit:
        return CompactionReport(
            text=text,
            original_chars=len(text),
            limit=resolved_limit,
            applied=False,
            over_limit=False,
        )

    compacted, lifted = _compact_lines(text)
    audit = audit_shot_protection(text, compacted, lifted=lifted)
    return CompactionReport(
        text=compacted,
        original_chars=len(text),
        limit=resolved_limit,
        applied=compacted != text,
        over_limit=len(compacted) > resolved_limit,
        protected_dropped=audit_protected_text(text, compacted),
        shot_migrations=audit,
    )


def audit_protected_text(before: str, after: str) -> Tuple[str, ...]:
    """Text the compaction is never allowed to drop or rewrite.

    Covers the shot headers (timeline + cut relation: a header must never be
    shortened to a bare label) and the whole protected sections, which must
    survive byte for byte.
    """
    before_lines = before.split("\n")
    after_lines = {line.strip() for line in after.split("\n")}
    after_text = "\n".join(after.split("\n"))
    losses: List[str] = []

    for line in before_lines:
        stripped = line.strip()
        if SHOT_HEADER_RE.match(stripped) and stripped not in after_lines:
            losses.append(f"镜头标题/时间轴被丢弃或改写：{stripped}")

    in_guard = False
    for line in before_lines:
        stripped = line.strip()
        if stripped.startswith("【") and stripped.endswith("】"):
            in_guard = stripped in PROTECTED_SECTIONS
            continue
        if in_guard and stripped and stripped not in after_text:
            losses.append(f"受保护段落内容丢失：{stripped}")
    return tuple(losses)


def _compact_lines(text: str) -> Tuple[str, Tuple[str, ...]]:
    """Apply L1-L5.  Returns the compacted text and the labels genuinely lifted."""
    lines = text.split("\n")
    guard = _section_flags(lines)

    # ---------- L1 placeholder values + L3 fixed boilerplate ----------
    step1: List[str] = []
    for line, protected in zip(lines, guard):
        if not line.strip():
            step1.append(line)
            continue
        new_line = line
        if not protected:
            for pattern, dst in REGEX_SHORTENINGS:
                if pattern.match(line.strip()):
                    new_line = dst
                    break
            if new_line.strip() and "：" in new_line:
                value = _value_of(new_line)
                if value in PLACEHOLDER_VALUES or value.startswith("不适用"):
                    continue
            for src, dst in LINE_SHORTENINGS.items():
                if src and src in new_line:
                    new_line = new_line.replace(src, dst)
                    break
        if not new_line.strip():
            continue
        step1.append(new_line)

    # ---------- L2 verbatim duplicate lines + per-shot anchors ----------
    # A per-shot product-visibility line may only be lifted to a film-level
    # ``每段商品必须可见`` rule when *every* shot asks for the same thing.
    # Otherwise lifting it would assert shot 1's requirement onto the other
    # shots and delete their own (Review #9).
    anchor_positions = [
        position
        for position, line in enumerate(step1)
        if _label_of(line) == SHOT_ANCHOR_LABEL and _value_of(line)
    ]
    anchor_values = {
        _normalize_constraint(_value_of(step1[position]))
        for position in anchor_positions
    }
    lift_anchors = len(anchor_values) == 1

    step2: List[str] = []
    seen: Set[str] = set()
    skipped = set(anchor_positions[1:]) if lift_anchors else set()
    anchor_renamed = False
    for position, line in enumerate(step1):
        if position in skipped:
            continue
        line = line.rstrip()
        label = _label_of(line)
        if position in anchor_positions:
            # Only a genuine lift renames the line, and only for the first
            # occurrence.  When the shots disagree every line keeps its own
            # label -- renaming all of them would assert each shot's
            # requirement onto every other shot.
            if lift_anchors and position == anchor_positions[0]:
                line = GLOBAL_ANCHOR_LABEL + "：" + _value_of(line)
                anchor_renamed = True
            step2.append(line)
            continue
        if (
            label
            and label not in SHOT_CORE_LABELS
            and not any(marker in line for marker in KEEP_MARKERS)
        ):
            value = _value_of(line)
            if value and len(value) >= 6 and value in seen:
                continue
            if value:
                seen.add(value)
        step2.append(line)

    # ---------- L2b in-shot camera line repeating its own visible event ----------
    step2b: List[str] = []
    last_visual = ""
    for line in step2:
        label = _label_of(line)
        if line.strip().startswith("【"):
            last_visual = ""
        if label == "画面事件":
            last_visual = _value_of(line)
        elif label == "本段手机构图" and last_visual and _value_of(line) == last_visual:
            continue
        step2b.append(line)

    # ---------- L4 clause-level de-duplication ----------
    authority: Set[str] = set()
    for line in step2b:
        label, _, value = line.partition("：")
        if label.strip() in SHOT_CORE_LABELS and not any(
            marker in line for marker in KEEP_MARKERS
        ):
            for part in _clauses(value):
                key = part.strip().rstrip("；。")
                if len(key) >= 12:
                    authority.add(key)

    step4: List[str] = []
    for line in step2b:
        label = _label_of(line)
        if (
            label not in L4_ELIGIBLE_LABELS
            or any(marker in line for marker in KEEP_MARKERS)
            or len(line) < 25
        ):
            step4.append(line)
            continue
        value = line.partition("：")[2]
        fresh: List[str] = []
        for part in _clauses(value):
            key = part.strip().rstrip("；。")
            if len(key) < 12:
                fresh.append(part)
                continue
            if key in authority:
                continue
            authority.add(key)
            fresh.append(part)
        rebuilt = "".join(fresh).strip("；")
        if not rebuilt:
            continue
        step4.append(f"{label}：{rebuilt}")

    # ---------- L5 protocol tokens ----------
    step5 = [line for line in step4 if _label_of(line) not in PROTOCOL_LABELS]

    lifted: Tuple[str, ...] = (SHOT_ANCHOR_LABEL,) if anchor_renamed else ()
    return _fold_blank(step5), lifted
