#!/usr/bin/env python3
"""Codex/OpenAI gpt-5.5 client for video-remake-lite.

The Codex Responses API does not accept mp4/video input directly. The client
therefore downloads the Feishu video, samples ordered keyframes, and sends
those frames to the local openai-codex OAuth endpoint with model gpt-5.5.
"""

from __future__ import annotations

import base64
import json
import math
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import httpx
import imageio.v2 as imageio
import requests
from PIL import Image, ImageChops, ImageStat

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from workspace_support import load_repo_env

load_repo_env()

CODEX_BASE_URL = os.environ.get(
    "VIDEO_REMAKE_CODEX_BASE_URL",
    "https://chatgpt.com/backend-api/codex",
).rstrip("/")
CODEX_MODEL = os.environ.get("VIDEO_REMAKE_CODEX_MODEL", "gpt-5.5")
FRAME_COUNT_OVERRIDE = int(os.environ.get("VIDEO_REMAKE_FRAME_COUNT", "0") or "0")
MIN_FRAME_COUNT = int(os.environ.get("VIDEO_REMAKE_MIN_FRAME_COUNT", "16") or "16")
MAX_FRAME_COUNT = int(os.environ.get("VIDEO_REMAKE_MAX_FRAME_COUNT", "32") or "32")
FRAME_MAX_SIZE = int(os.environ.get("VIDEO_REMAKE_FRAME_MAX_SIZE", "768") or "768")
FRAME_QUALITY = int(os.environ.get("VIDEO_REMAKE_FRAME_QUALITY", "88") or "88")
WORK_DIR = Path(os.environ.get("VIDEO_REMAKE_WORK_DIR", "/tmp/video_remake_codex_gpt55"))
SUPPORTED_DURATIONS = tuple(
    sorted(
        {
            int(item.strip())
            for item in os.environ.get("VIDEO_REMAKE_SUPPORTED_DURATIONS", "6,8,10,15").split(",")
            if item.strip().isdigit() and int(item.strip()) > 0
        }
    )
) or (6, 8, 10, 15)
HAN_RE = re.compile(r"[\u3400-\u9fff]")
SPOKEN_TEXT_LABEL_RE = re.compile(
    r"(?im)^(?P<label>.*(?:字幕|旁白|口播|台词|屏幕文字|显示文字|画面文字|on-screen text|subtitle|voiceover|spoken line).{0,30}?[:：])(?P<value>.*)$"
)
SPOKEN_TEXT_FIELD_RE = re.compile(
    r"(?i)(?:[\u3400-\u9fff]{0,16})?"
    r"(?:字幕|旁白|口播|台词|屏幕文字|显示文字|画面文字|on-screen text|subtitle|voiceover|spoken line)"
    r"(?:[/／、和与](?:字幕|旁白|口播|台词|屏幕文字|显示文字|画面文字|on-screen text|subtitle|voiceover|spoken line))*"
    r"[^\n:：|。；;，,\"“”']{0,20}[:：]"
)
NO_SPOKEN_TEXT_VALUES = {
    "",
    "无",
    "无字幕",
    "无口播",
    "无旁白",
    "无屏幕文字",
    "无显示文字",
    "无画面文字",
    "无字幕/无口播",
    "无字幕/无旁白",
    "无字幕，无口播",
    "无字幕，无旁白",
    "none",
    "no",
}
INSTRUCTION_CHINESE_SEGMENT_RE = re.compile(
    r"[（(][^）)]*(?:执行说明|不显示|不可显示|不可发声|不要显示|不要发声|不朗读|不出现在画面)[^）)]*[）)]"
)
INSTRUCTION_CHINESE_CLAUSE_RE = re.compile(
    r"(?:^|[。；;，,、])[^。；;，,、]*(?:不显示|必须|禁止|不展示|不要|不得|不可|只显示|只展示|显示|出现|持续|核心|说明|执行|保留|使用|目标语言|其他语言文字|账号ID|画面|发声|朗读|无字幕|无口播|无旁白|全片|前\d*秒|尾页|允许|仅允许|仅限|顶部|中间|下方|右下角|按钮|搜索框|搜索词|搜索界面|界面文字|平台标识|真实平台|BGM|尾音|淡出|停留|结束|可放|装饰|图标|字体|字号|描边|颜色|白色|小字|居中|偏下|遮脸|简洁|中文|水印|logo|字幕样式|样式说明|样式如|注释|前半段|后半段|固定镜头|对焦|切镜|本片|上半段|下半段|建议|完全|镜头描述)[^。；;，,、]*"
)
SCREEN_TEXT_LABEL_RE = re.compile(
    r"(?:按钮|搜索框|下方|上方|中间|顶部|底部|左侧|右侧|界面|页面|标题|说明|提示|文案)?文字\s*[:：]"
)
QUOTED_VISIBLE_TEXT_RE = re.compile(r"[\"“”']([^\"“”']+)[\"“”']")
VISIBLE_TEXT_CONTENT_MARKER_RE = re.compile(
    r"(?:内容只能是|内容仅限|字幕内容(?:只能是|仅限)?|实际字幕(?:为|是)?|实际显示文字只能是|显示文字只能是)\s*[:：]\s*(?P<text>.*)"
)


@dataclass(frozen=True)
class VideoDurationDecision:
    original_duration: float
    strategy: str
    target_duration: int
    reason: str = ""

    @property
    def is_suitable(self) -> bool:
        return self.target_duration > 0 and self.strategy != "不建议复刻"

    def render(self) -> str:
        lines = [
            "零、时长决策",
            f"- 原视频实际时长（秒）：{self.original_duration:.1f}",
            f"- 复刻策略：{self.strategy}",
            f"- 复刻目标时长（秒）：{self.target_duration}",
        ]
        if self.reason:
            lines.append(f"- 原因：{self.reason}")
        return "\n".join(lines)


@dataclass(frozen=True)
class VideoRemakeGenerationResult:
    outputs: Dict[str, str]
    duration_decision: VideoDurationDecision
    reference_frames: List[Path]


def decide_video_duration(duration: float) -> VideoDurationDecision:
    """Make duration routing deterministic and aligned with downstream presets."""
    actual = max(float(duration or 0), 0.0)
    if actual <= 0:
        return VideoDurationDecision(
            original_duration=actual,
            strategy="不建议复刻",
            target_duration=0,
            reason="无法读取有效视频时长。",
        )
    min_supported_duration = min(SUPPORTED_DURATIONS)
    if actual < min_supported_duration:
        return VideoDurationDecision(
            original_duration=actual,
            strategy="短片延展复刻",
            target_duration=min_supported_duration,
            reason=(
                f"原视频短于下游最短{min_supported_duration}秒档，保留原主动作，"
                "只用自然起势、微停顿和动作回收补足时长，禁止循环、慢放或复制帧。"
            ),
        )
    if actual > 25:
        return VideoDurationDecision(
            original_duration=actual,
            strategy="不建议复刻",
            target_duration=0,
            reason="原视频超过25秒，下游最长15秒，直接压缩会破坏动作和情绪节奏。",
        )
    if actual > 15:
        return VideoDurationDecision(
            original_duration=actual,
            strategy="选段压缩复刻",
            target_duration=15,
            reason="只保留一个连续高光动作段，不把多个段落强塞进15秒。",
        )

    target = min(SUPPORTED_DURATIONS, key=lambda item: (abs(item - actual), item))
    reason = ""
    if abs(target - actual) > 0.25:
        reason = f"下游使用离原时长最近的受支持档位{target}秒，脚本必须按该档位重新对齐。"
    return VideoDurationDecision(
        original_duration=actual,
        strategy="原长复刻",
        target_duration=target,
        reason=reason,
    )


class VideoRemakeLLMClient:
    """Use local Codex/OpenAI gpt-5.5 for video remake generation."""

    def __init__(
        self,
        api_url: str = CODEX_BASE_URL,
        api_key: str = "",
        model: str = CODEX_MODEL,
        timeout: int = 240,
        max_retries: int = 2,
    ):
        self.api_url = api_url.rstrip("/")
        self.api_key = api_key or self._resolve_codex_token()
        self.model = model
        self.timeout = timeout
        self.max_retries = max_retries
        self.work_dir = WORK_DIR

    def generate_four_fields(
        self,
        *,
        video_url: str,
        context: Dict[str, str],
        task_label: str,
    ) -> VideoRemakeGenerationResult:
        """Generate the four fields through a staged, temporally grounded flow."""
        _video_path, duration, _fps, frames = self._sample_video_frames(video_url, task_label)
        decision = decide_video_duration(duration)
        reference_frames = self._select_reference_frame_paths(frames)
        if not decision.is_suitable:
            return VideoRemakeGenerationResult(
                outputs={"脚本拆解": decision.render()},
                duration_decision=decision,
                reference_frames=reference_frames,
            )

        print(f"    1/3 时序事实拆解（自适应关键帧 {len(frames)} 张）...")
        analysis = self._responses_text(
            prompt=self._build_temporal_analysis_prompt(
                context=context,
                task_label=task_label,
                decision=decision,
                frames=frames,
            ),
            frames=frames,
            reasoning_effort="medium",
        ).strip()
        script_breakdown = f"{decision.render()}\n\n{analysis}".strip()

        print("    2/3 生成高保真复刻卡...")
        remake_card = self._responses_text(
            prompt=self._build_remake_card_prompt(
                context=context,
                decision=decision,
                script_breakdown=script_breakdown,
            ),
            frames=[],
            reasoning_effort="medium",
        ).strip()

        print("    3/3 收束可执行脚本和视频提示词...")
        raw_execution = self._responses_text(
            prompt=self._build_execution_prompt(
                context=context,
                decision=decision,
                remake_card=remake_card,
            ),
            frames=[],
            reasoning_effort="low",
        )
        execution = self._extract_json(raw_execution)
        remade_script = str(execution.get("复刻后的脚本") or "").strip()
        final_prompt = str(execution.get("最终复刻视频提示词") or "").strip()
        if not remade_script or not final_prompt:
            raise RuntimeError("Codex gpt-5.5 最终阶段缺少复刻后的脚本或最终复刻视频提示词")

        final_prompt = self._ensure_execution_contract(
            final_prompt,
            decision=decision,
        )
        final_prompt = self._ensure_spoken_text_no_chinese(
            final_prompt,
            context,
        )
        outputs = {
            "脚本拆解": script_breakdown,
            "复刻卡": remake_card,
            "复刻后的脚本": remade_script,
            "最终复刻视频提示词": final_prompt,
        }
        return VideoRemakeGenerationResult(
            outputs=outputs,
            duration_decision=decision,
            reference_frames=reference_frames,
        )

    def chat_with_video(self, video_url: str, prompt: str, max_tokens: int = 2500) -> str:
        """Compatibility API: run a prompt against sampled video keyframes."""
        _video_path, _duration, _fps, frames = self._sample_video_frames(video_url, "compat")
        return self._responses_text(prompt=prompt, frames=frames)

    def chat_text(self, prompt: str, max_tokens: int = 2500) -> str:
        """Compatibility API: run a text-only prompt through Codex gpt-5.5."""
        return self._responses_text(prompt=prompt, frames=[])

    @staticmethod
    def _build_temporal_analysis_prompt(
        *,
        context: Dict[str, str],
        task_label: str,
        decision: VideoDurationDecision,
        frames: List[Tuple[Path, float]],
    ) -> str:
        frame_times = "、".join(f"{timestamp:.1f}s" for _path, timestamp in frames)
        return f"""
你是养号短视频的时序复刻分析师。下面的图片不是静态素材合集，而是从同一条原视频中按场景变化和动作峰值自适应抽取的连续时序帧。

【程序已锁定的时长】
{decision.render()}
- 关键帧时间：{frame_times}
- 任务编号：{task_label}
- 内容分支：{context.get('content_branch_label') or '非商品展示型'}
- 目标国家：{context.get('target_country') or '未提供'}
- 目标语言：{context.get('target_language') or '未提供'}
- 商品类型：{context.get('product_type') or '未提供'}

只输出“脚本拆解”正文，不要 JSON，不要重新决定时长。必须按时间顺序写清：
1. 镜头/场景边界和每段对应原视频时间。
2. 每个主动作的起始姿态、触发、动作峰值、结束/回收姿态；同时写手、头、视线和身体重心的变化。
3. 人物表情曲线：中性基线、细微触发、峰值、回落；区分嘴角、眼神、眉部，不要只写“开心/微笑”。
4. 镜头运动、主体距离和构图变化。
5. 必须保留的生活化不完美：眨眼、呼吸、短暂停顿、视线先行、左右不完全对称、手部轻微迟疑。
6. 选段压缩时，只选一个连续高光动作段，明确原视频起止时间，不拼接互不连续的动作。
7. 只能根据画面判断；看不到音轨时明确写“音频待后期匹配”，不要编造歌词、台词或精确 BGM 卡点。
8. 非商品展示型不得改成带货视频。
""".strip()

    @staticmethod
    def _build_remake_card_prompt(
        *,
        context: Dict[str, str],
        decision: VideoDurationDecision,
        script_breakdown: str,
    ) -> str:
        return f"""
你是养号短视频复刻导演。根据下面的事实拆解，输出唯一一份“复刻卡”，不要 JSON，不要提供备选方向。

【硬设置】
- 目标时长固定为 {decision.target_duration} 秒，不得再改。
- 目标语言：{context.get('target_language') or '目标语言'}。
- 内容分支：{context.get('content_branch_label') or '非商品展示型'}。
- 画面高保真优先于文案创新，只做轻微本地化和防判重。

【脚本拆解】
{script_breakdown}

复刻卡必须包含：
1. 选用的连续原视频区间和镜头数量；目标时长不超过8秒时1-2镜头，其余最多3镜头。
2. 每镜头只允许一个主动作，写清“起始姿态 -> 触发 -> 峰值 -> 回收”，不要堆叠多个动作指令。
3. 每镜头写清视线、头部、双手、身体重心和动作速度曲线。
4. 表情用过程描述：中性基线 -> 非对称微反应 -> 峰值 -> 自然回落；保留眨眼、呼吸和微停顿。
5. 指明对应原视频时间和最接近的参考帧时间。
6. 声音只写表达模式和后期节奏建议；无法从画面确认的内容不得编造。
7. 防判重只能改变背景小物、轻微机位和字幕措辞，不能改变主动作和情绪转折。
""".strip()

    @staticmethod
    def _build_execution_prompt(
        *,
        context: Dict[str, str],
        decision: VideoDurationDecision,
        remake_card: str,
    ) -> str:
        return f"""
你是短视频生成模型的执行提示词编辑。根据脚本拆解和复刻卡，生成唯一可执行版本。

只返回合法 JSON，不要 Markdown，不要额外解释：
{{
  "复刻后的脚本": "完整可执行分镜脚本字符串",
  "最终复刻视频提示词": "给视频生成模型直接消费的精简提示词字符串"
}}

【硬约束】
- 总时长必须精确写成 {decision.target_duration} 秒；各镜头时长之和也必须是 {decision.target_duration} 秒。
- 最多3个镜头；每镜头只执行一个主动作，不在同一时间要求人物完成多个动作。
- 每镜头必须写：时间范围、对应原视频时间、起始姿态、动作弧线、结束姿态、视线/头部/手部/重心、表情曲线、镜头运动。
- 表演必须包含自然微动作：动作前短暂停顿、视线先于头部移动、一次自然眨眼或呼吸、左右轻微不对称、动作完成后的回收。
- 禁止固定笑脸、全程盯镜头、匀速机械手势、突然启动/停止、过度对称表情、僵直站姿、无动机摆拍。
- 最终提示词保持紧凑，先写总设置，再按镜头写执行，最后写负面限制；不要复述分析过程。
- 画面说明可用中文，但实际显示或朗读的字幕、旁白、台词、屏幕文字只能使用 {context.get('target_language') or '目标语言'}，不能出现中文。
- 无法确认原音频时写“音频待后期匹配”，不要编造歌词或精确卡点。
- 非商品展示型不得增加商品卖点、购买理由或转化收口。

【复刻卡】
{remake_card}
""".strip()

    @staticmethod
    def _ensure_execution_contract(
        final_prompt: str,
        *,
        decision: VideoDurationDecision,
    ) -> str:
        text = str(final_prompt or "").strip()
        duration_header = f"【生成硬设置】\n总时长：{decision.target_duration}秒。"
        naturalness = (
            "自然表演：每镜头一个主动作；动作前有微停顿，视线先于头部，保留自然眨眼/呼吸、"
            "轻微不对称和动作回收。"
        )
        negative = (
            "禁止固定笑脸、全程盯镜头、匀速机械手势、突然启动或停止、过度对称表情、"
            "僵直站姿、无动机摆拍。"
        )
        if not re.search(rf"总时长\s*[:：]\s*{decision.target_duration}(?:\.0)?\s*秒", text):
            text = f"{duration_header}\n{text}"
        if not all(keyword in text for keyword in ("微停顿", "视线", "眨眼")):
            text = f"{text}\n\n【自然表演约束】\n{naturalness}"
        if "固定笑脸" not in text or "机械手势" not in text:
            text = f"{text}\n\n【表演负面限制】\n{negative}"
        return text.strip()

    @staticmethod
    def _select_reference_frame_paths(frames: List[Tuple[Path, float]]) -> List[Path]:
        if not frames:
            return []
        indexes = sorted({0, len(frames) // 2, len(frames) - 1})
        return [frames[index][0] for index in indexes]

    def build_reference_image_spec(
        self,
        *,
        context: Dict[str, str],
        remake_card: str,
        final_prompt: str,
        reference_requirements: str,
        modification_notes: str,
        labeled_images: List[Tuple[Path, str]],
        input_mode: str = "综合参考",
        character_appearance_anchor: str = "",
        character_identity_setting: str = "",
    ) -> Dict[str, str]:
        image_roles = "\n".join(
            f"- 图片 {index}：{label}"
            for index, (_path, label) in enumerate(labeled_images, 1)
        ) or "- 无用户图片；只根据脚本和原视频时序帧生成"
        prompt = f"""
你是短视频首镜视觉导演。请把复刻脚本、用户参考图和用户要求整理成一张可供 gpt-image-2 直接生成的“0秒首镜执行图”。

【图片角色】
{image_roles}

【素材模式】
{input_mode}

【原视频人物文字锚点】
{character_appearance_anchor or '未提供'}

【用户指定人物族群与地域设定】
{character_identity_setting or '未提供；不得自行推断或补充'}

【用户参考要求，最高优先级】
{reference_requirements or '未提供'}

【本轮修改意见】
{modification_notes or '无'}

【目标国家/语言】
{context.get('target_country') or '未提供'} / {context.get('target_language') or '未提供'}

【复刻卡】
{remake_card}

【最终视频提示词】
{final_prompt}

只返回合法 JSON：
{{
  "视觉锁定卡": "人物外观指纹、服装结构指纹、场景、构图、光线、首镜动作起点、必须保留、允许调整、禁止出现",
  "AI参考图提示词": "给 gpt-image-2 的完整中文图片生成提示词",
  "参考冲突": "无，或明确写出无法同时满足的冲突"
}}

硬规则：
1. 生成的是单张铺满完整9:16画布的真实手机短视频首帧，不是拼贴图、分镜板、海报或商品广告；禁止上下或左右黑边、白边、横屏信箱式留白。
2. 视觉锁定卡必须单列“人物外观指纹”：优先逐项沿用“原视频人物文字锚点”，写明可观察的肤色、脸型、眉眼特征、鼻唇气质、发色、发长、分缝、发丝质地、体型比例、可见配饰和初始坐姿；禁止只写“年轻女性/男性”，也不能用泛化面孔替换锚点中明确的脸部几何。
3. 人物图只锁人物身份、脸型/五官气质、肤色、发型、体型和可见配饰；服装图只锁服装；场景图只锁环境和光线；原视频帧只锁生活感、构图和动作起点。
4. 若有服装参考图，视觉锁定卡必须单列“服装结构指纹”，逐项写明：领型与领口边饰、门襟/扣件、肩袖结构、袖口边饰、侧片/拼接的位置和形状、图案/刺绣/条纹的位置、主辅色比例、版型和面料。不得把它概括为“某色球衣”。
5. AI参考图提示词必须逐项复述人物外观指纹和服装结构指纹，并明确“服装参考图优先于原视频帧”；服装参考图中原本可见的徽章、号码、品牌标记或文字属于视觉结构，应保留其位置、比例、色块和装饰效果；不得凭空新增参考图之外的水印、UI、品牌或文字。
6. 首帧必须是动作开始前或刚开始的瞬间，不能是动作完成后的摆拍结果。
7. 保留自然表情、轻微不对称、呼吸感和生活化姿态，禁止固定笑脸、僵直站姿和广告模特感。
8. 不出现字幕、水印、UI、画中画或多余人物；但服装参考图上原本可见的徽章、号码、品牌标记或文字不算额外画面元素，必须按参考图保留在服装上。
9. 用户文字与图片明显冲突且无法按优先级解决时，不要猜；在“参考冲突”中说明。若已按用户文字或本轮修改意见完成取舍，`参考冲突`必须返回“无”。
10. AI参考图提示词要明确每张输入图负责什么，禁止把人物、服装和场景属性串用。
11. 当素材模式为“脚本+服装参考图”时，Image 2 的人物和场景只能根据最终视频提示词、复刻卡和“原视频人物文字锚点”生成；不得把原视频时序帧、人物参考图、场景参考图或上一版AI图作为图片输入，或借用这些图片以外的额外脸部信息。输入服装图只用于服装结构和配色。
12. 如果“用户指定人物族群与地域设定”非空，必须把它作为明确的生成约束写入人物外观指纹和AI参考图提示词；不得用与该设定明显不符的泛化人物替代。该设定来自用户，不要自行推断或改写。
""".strip()
        raw = self._responses_text(
            prompt=prompt,
            frames=[],
            labeled_images=labeled_images,
            reasoning_effort="medium",
        )
        data = self._extract_json(raw)
        lock_card = str(data.get("视觉锁定卡") or "").strip()
        image_prompt = str(data.get("AI参考图提示词") or "").strip()
        conflict = str(data.get("参考冲突") or "").strip()
        if not lock_card or not image_prompt:
            raise RuntimeError("视觉锁定卡或AI参考图提示词缺失")
        return {
            "视觉锁定卡": lock_card,
            "AI参考图提示词": image_prompt,
            "参考冲突": conflict,
        }

    def extract_character_appearance_anchor(
        self,
        source_images: List[Tuple[Path, str]],
    ) -> str:
        if not source_images:
            return ""
        prompt = """
你是短视频人物形象分析师。根据同一原视频的对照帧，提炼可直接写进图像生成提示词的人物外观文字锚点。

只返回合法 JSON：
{"人物形象锚定":"..."}

要求：
1. 只描述画面可观察的外观，不判断或命名人物的种族、国籍、地域或身份。
2. 必须覆盖：肤色明暗与冷暖、脸型纵横比例和下颌线、眉形/浓淡/弧度、眼型/眼距/眼睑观感、鼻梁和鼻尖的相对形态、唇形与嘴角、发色/长度/分缝/卷直/碎发、体型比例、可见配饰、初始姿态。
3. 写清“不可漂移”的几何关系，例如面部偏长或偏圆、眉眼距离、鼻梁高低、嘴唇厚薄、发量与卷度；不要使用空泛的“好看、年轻、有气质”。
4. 这是文字锚点，后续 Image 2 不会收到这些原视频图片；措辞要足够具体，避免模型生成完全不同的泛化面孔。
""".strip()
        raw = self._responses_text(
            prompt=prompt,
            frames=[],
            labeled_images=source_images[:2],
            reasoning_effort="medium",
        )
        data = self._extract_json(raw)
        anchor = str(data.get("人物形象锚定") or "").strip()
        if not anchor:
            raise RuntimeError("人物形象锚定缺失")
        return anchor

    def evaluate_reference_image(
        self,
        *,
        visual_lock_card: str,
        generated_image: Path,
        labeled_reference_images: List[Tuple[Path, str]],
        character_identity_setting: str = "",
    ) -> Dict[str, Any]:
        qa_images = [(generated_image, "待质检的AI首镜执行图")]
        qa_images.extend(labeled_reference_images[:5])
        prompt = f"""
你是短视频首镜参考图质检员。对比AI首镜图、参考图和视觉锁定卡，判断它是否适合直接作为图片转视频的第0秒输入。

【视觉锁定卡】
{visual_lock_card}

【用户指定人物族群与地域设定】
{character_identity_setting or '未提供'}

只返回合法 JSON：
{{
  "结论": "通过或不通过",
  "总分": 0到100的整数,
  "人物一致性": "简述",
  "服装一致性": "简述",
  "场景调性": "简述",
  "首镜可执行性": "简述",
  "硬性问题": ["问题1"],
  "重做指令": "不通过时给gpt-image-2的精确修正指令"
}}

硬性不通过条件：人物或服装明显错位；用户指定人物族群与地域设定非空但生成结果与该设定明显不符；人物面部有重影、双重眉眼、叠加鼻嘴、模糊涂抹、五官错位或明显畸形；服装被简化为泛化款式，或遗漏服装参考图中清晰可见的领型、门襟/扣件、袖口边饰、侧片/拼接、图案/刺绣位置等结构指纹；动作已经完成而没有后续动作空间；画面是拼贴/海报；出现参考图之外的水印、UI、品牌或文字；上下或左右有大面积黑边/白边/横屏信箱式留白；多余人物；广告棚拍感明显。服装参考图本来就有的徽章、号码、品牌标记或文字，不能单独作为不通过理由。总分低于75也判为不通过。
""".strip()
        raw = self._responses_text(
            prompt=prompt,
            frames=[],
            labeled_images=qa_images,
            reasoning_effort="low",
        )
        data = self._extract_json(raw)
        try:
            score = int(float(data.get("总分") or 0))
        except Exception:
            score = 0
        conclusion = str(data.get("结论") or "").strip()
        hard_issues = data.get("硬性问题")
        if not isinstance(hard_issues, list):
            hard_issues = [str(hard_issues)] if hard_issues else []
        passed = conclusion == "通过" and score >= 75 and not [item for item in hard_issues if str(item).strip()]
        data["总分"] = score
        data["通过"] = passed
        data["硬性问题"] = hard_issues
        return data

    def _responses_text(
        self,
        *,
        prompt: str,
        frames: Iterable[Tuple[Path, float]],
        labeled_images: Optional[Iterable[Tuple[Path, str]]] = None,
        reasoning_effort: str = "medium",
    ) -> str:
        content: List[Dict[str, Any]] = [{"type": "input_text", "text": prompt}]
        for index, (path, timestamp) in enumerate(frames, 1):
            content.append({"type": "input_text", "text": f"关键帧 {index}，约 {timestamp:.1f} 秒："})
            content.append({"type": "input_image", "image_url": self._image_data_url(path)})
        for index, (path, label) in enumerate(labeled_images or [], 1):
            content.append({"type": "input_text", "text": f"参考图片 {index}：{label}"})
            content.append({"type": "input_image", "image_url": self._image_data_url(path)})

        body: Dict[str, Any] = {
            "model": self.model,
            "instructions": (
                "You are a precise short-video remake workflow assistant. "
                "Return only valid JSON when requested."
            ),
            "input": [{"role": "user", "content": content}],
            "reasoning": {"effort": reasoning_effort},
            "stream": True,
            "store": False,
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                parts: List[str] = []
                with httpx.Client(timeout=self.timeout) as client:
                    with client.stream(
                        "POST",
                        f"{self.api_url}/responses",
                        json=body,
                        headers=headers,
                    ) as response:
                        if response.status_code != 200:
                            error = response.read().decode("utf-8", errors="replace")
                            raise RuntimeError(
                                f"Codex gpt-5.5 返回 {response.status_code}: {error[:1200]}"
                            )
                        for line in response.iter_lines():
                            if not line or not line.startswith("data: "):
                                continue
                            data = line[6:]
                            if data.strip() == "[DONE]":
                                break
                            try:
                                event = json.loads(data)
                            except Exception:
                                continue
                            if event.get("type") == "response.output_text.delta":
                                parts.append(event.get("delta") or "")
                output = "".join(parts).strip()
                if not output:
                    raise RuntimeError("Codex gpt-5.5 未返回文本")
                return output
            except Exception as exc:
                last_error = exc
                if attempt < self.max_retries:
                    wait_time = 2 ** attempt
                    print(
                        f"    ⚠️ Codex gpt-5.5 调用异常，{wait_time} 秒后重试 "
                        f"({attempt + 1}/{self.max_retries})..."
                    )
                    time.sleep(wait_time)
        raise RuntimeError(f"Codex gpt-5.5 调用最终失败: {last_error}")

    def _spoken_text_chinese_lines(self, text: str) -> List[str]:
        """Find likely subtitle/voiceover values that contain Chinese."""
        bad_lines: List[str] = []
        for line in str(text or "").splitlines():
            stripped_line = line.strip()
            line_quoted_values = QUOTED_VISIBLE_TEXT_RE.findall(stripped_line)
            if (
                re.search(r"(?:要求|样式|规则)", stripped_line)
                and line_quoted_values
                and not HAN_RE.search("".join(line_quoted_values))
            ):
                continue
            style_content_marker = VISIBLE_TEXT_CONTENT_MARKER_RE.search(stripped_line)
            if (
                "字幕样式" in stripped_line
                and style_content_marker
                and not HAN_RE.search(style_content_marker.group("text"))
            ):
                continue
            matches = list(SPOKEN_TEXT_FIELD_RE.finditer(stripped_line))
            if not matches:
                continue
            for index, match in enumerate(matches):
                end = (
                    len(line)
                    if "要求" in match.group(0)
                    else matches[index + 1].start() if index + 1 < len(matches) else len(line)
                )
                value = line[match.end():end].strip().strip("` ")
                if " | " in value:
                    value = value.split(" | ", 1)[0].strip()
                normalized = re.sub(r"\s+", "", value).lower()
                if normalized in NO_SPOKEN_TEXT_VALUES:
                    continue
                no_spoken_prefixes = ("无字幕", "无口播", "无旁白", "无屏幕文字", "无显示文字", "无画面文字")
                if any(normalized.startswith(p) for p in no_spoken_prefixes):
                    continue
                if "样式" in match.group(0):
                    # '字幕样式：' / '字幕样式' with no quoted visible text or all
                    # quoted values are non-Chinese → pure styling instruction, skip.
                    # Exception: if the value contains a visible-content marker
                    # (内容只能是/内容仅限/实际显示文字只能是) it may still carry
                    # unquoted Chinese display text → continue validation.
                    if not VISIBLE_TEXT_CONTENT_MARKER_RE.search(value):
                        style_quoted_values = QUOTED_VISIBLE_TEXT_RE.findall(value)
                        if not style_quoted_values or not HAN_RE.search("".join(style_quoted_values)):
                            continue
                quoted_visible_values = QUOTED_VISIBLE_TEXT_RE.findall(value)
                if quoted_visible_values:
                    if HAN_RE.search("".join(quoted_visible_values)):
                        bad_lines.append(line)
                        break
                    if re.search(r"(?i)(?:屏幕文字|显示文字|画面文字|on-screen text)", match.group(0)):
                        # For explicit on-screen text fields, quoted text is the
                        # renderable payload. Chinese around it is often timing,
                        # action, or emotion direction for the video model.
                        continue
                    current_clause_prefix = re.split(r"[。；;，,、]", line[: match.start()])[-1]
                    if "要求" in match.group(0) or "要求" in current_clause_prefix:
                        continue
                    spoken_value = QUOTED_VISIBLE_TEXT_RE.sub("", value)
                else:
                    spoken_value = value
                first_sentence_value = re.split(r"[。；;]", spoken_value, 1)[0].strip()
                if first_sentence_value and not HAN_RE.search(first_sentence_value):
                    # Compact final prompts often write the visible subtitle first,
                    # then continue with Chinese audio/shot/negative instructions
                    # on the same line. Only the first sentence is renderable text.
                    spoken_value = first_sentence_value
                spoken_value = INSTRUCTION_CHINESE_SEGMENT_RE.sub("", spoken_value)
                content_marker = VISIBLE_TEXT_CONTENT_MARKER_RE.search(spoken_value)
                if content_marker and "样式" in match.group(0):
                    spoken_value = content_marker.group("text")
                spoken_value = INSTRUCTION_CHINESE_CLAUSE_RE.sub("", spoken_value)
                spoken_value = SCREEN_TEXT_LABEL_RE.sub("", spoken_value)
                normalized_spoken_value = re.sub(r"[\s。；;，,、.]+", "", spoken_value).lower()
                if normalized_spoken_value in NO_SPOKEN_TEXT_VALUES:
                    continue
                if HAN_RE.search(spoken_value):
                    bad_lines.append(line)
                    break
        return bad_lines

    def _ensure_spoken_text_no_chinese(self, final_prompt: str, context: Dict[str, str]) -> str:
        """Keep subtitle/voiceover/on-screen text values out of Chinese."""
        text = str(final_prompt or "").strip()
        bad_lines = self._spoken_text_chinese_lines(text)
        if not bad_lines:
            return text

        repair_prompt = f"""
请修复下面这段视频生成提示词。

规则：
- 只返回修复后的完整提示词，不要 JSON，不要 Markdown 代码块，不要解释。
- 保留中文执行说明、镜头说明、场景说明、动作说明、BGM/节奏说明、负面限制词。
- 只修复会被视频里显示或朗读的内容：字幕、旁白、口播、台词、屏幕文字、显示文字、画面文字、on-screen text、subtitle、voiceover、spoken line。
- 这些会显示/朗读的内容必须改成 {context.get('target_language') or '目标语言'}，不能出现中文。
- 如果某个镜头没有字幕/口播，可以写“无字幕/无口播”，这是执行说明，不是画面文字。
- 保留原本时长、镜头顺序、复刻意图和养号非广告约束。

需要修复的可显示/可朗读中文行：
{chr(10).join(bad_lines)}

原提示词：
{text}
""".strip()
        rewritten = self._responses_text(prompt=repair_prompt, frames=[]).strip()
        remaining_bad_lines = self._spoken_text_chinese_lines(rewritten)
        if remaining_bad_lines:
            raise RuntimeError(
                "最终复刻视频提示词的口播/字幕仍包含中文，已拒绝写回: "
                + " | ".join(remaining_bad_lines[:5])
            )
        return rewritten

    @staticmethod
    def _target_sample_count(duration: float, frame_count: int) -> int:
        if FRAME_COUNT_OVERRIDE > 0:
            desired = FRAME_COUNT_OVERRIDE
        else:
            desired = max(MIN_FRAME_COUNT, min(MAX_FRAME_COUNT, int(math.ceil(duration * 2))))
        return min(max(desired, 1), max(frame_count, 1))

    @staticmethod
    def _select_adaptive_indexes(
        frame_count: int,
        sample_count: int,
        motion_scores: List[Tuple[int, float]],
    ) -> List[int]:
        """Blend timeline anchors with scene/motion peaks while preserving order."""
        frame_count = max(int(frame_count), 1)
        sample_count = min(max(int(sample_count), 1), frame_count)
        anchor_count = min(sample_count, max(4, sample_count // 3))
        selected = {
            int(i * (frame_count - 1) / max(anchor_count - 1, 1))
            for i in range(anchor_count)
        }
        min_gap = max(1, frame_count // max(sample_count * 3, 1))
        for frame_index, _score in sorted(motion_scores, key=lambda item: item[1], reverse=True):
            if len(selected) >= sample_count:
                break
            if all(abs(frame_index - existing) >= min_gap for existing in selected):
                selected.add(max(0, min(int(frame_index), frame_count - 1)))

        if len(selected) < sample_count:
            for i in range(sample_count * 3):
                frame_index = int(i * (frame_count - 1) / max(sample_count * 3 - 1, 1))
                if frame_index not in selected:
                    selected.add(frame_index)
                if len(selected) >= sample_count:
                    break
        return sorted(selected)[:sample_count]

    @staticmethod
    def _read_frame_with_fallback(reader: Any, frame_index: int) -> Tuple[Any, int]:
        for fallback_index in range(frame_index, max(frame_index - 10, -1), -1):
            try:
                return reader.get_data(fallback_index), fallback_index
            except Exception:
                continue
        raise RuntimeError(f"无法读取视频帧: {frame_index}")

    def _scan_motion_scores(
        self,
        reader: Any,
        frame_count: int,
        sample_count: int,
    ) -> List[Tuple[int, float]]:
        candidate_count = min(frame_count, max(48, min(sample_count * 4, 128)))
        candidate_indexes = sorted(
            {
                int(i * (frame_count - 1) / max(candidate_count - 1, 1))
                for i in range(candidate_count)
            }
        )
        scores: List[Tuple[int, float]] = []
        previous: Optional[Image.Image] = None
        for frame_index in candidate_indexes:
            try:
                frame_data, actual_index = self._read_frame_with_fallback(reader, frame_index)
            except Exception:
                continue
            current = Image.fromarray(frame_data).convert("L").resize((64, 64))
            score = 0.0
            if previous is not None:
                difference = ImageChops.difference(current, previous)
                stats = ImageStat.Stat(difference)
                score = float(stats.mean[0]) + float(stats.stddev[0]) * 0.35
            scores.append((actual_index, score))
            previous = current
        return scores

    def _sample_video_frames(self, video_url: str, task_label: str) -> Tuple[Path, float, float, List[Tuple[Path, float]]]:
        task_dir = self.work_dir / re.sub(r"[^A-Za-z0-9_.-]+", "_", str(task_label or "task"))
        task_dir.mkdir(parents=True, exist_ok=True)
        video_path = task_dir / "source.mp4"
        if not video_path.exists() or video_path.stat().st_size == 0:
            response = requests.get(video_url, timeout=90)
            response.raise_for_status()
            video_path.write_bytes(response.content)

        reader = imageio.get_reader(str(video_path), "ffmpeg")
        try:
            meta = reader.get_meta_data()
            fps = float(meta.get("fps") or 0)
            duration = float(meta.get("duration") or 0)
            try:
                frame_count = int(reader.count_frames())
            except Exception:
                frame_count = int(duration * fps) if duration and fps else 1
            frame_count = max(frame_count, 1)

            sample_count = self._target_sample_count(duration, frame_count)
            motion_scores = self._scan_motion_scores(reader, frame_count, sample_count)
            indexes = self._select_adaptive_indexes(frame_count, sample_count, motion_scores)
            frames: List[Tuple[Path, float]] = []
            for index, frame_index in enumerate(indexes, 1):
                frame_data, actual_frame_index = self._read_frame_with_fallback(reader, frame_index)
                image = Image.fromarray(frame_data).convert("RGB")
                image.thumbnail((FRAME_MAX_SIZE, FRAME_MAX_SIZE))
                path = task_dir / f"frame_{index:02d}.jpg"
                image.save(path, quality=FRAME_QUALITY)
                timestamp = actual_frame_index / fps if fps else 0
                frames.append((path, timestamp))
            return video_path, duration, fps, frames
        finally:
            reader.close()

    @staticmethod
    def _image_data_url(path: Path) -> str:
        encoded = base64.b64encode(path.read_bytes()).decode("utf-8")
        return f"data:image/jpeg;base64,{encoded}"

    @staticmethod
    def _extract_json(text: str) -> Dict[str, Any]:
        raw = text.strip()
        fence = chr(96) * 3
        if raw.startswith(fence):
            raw = re.sub(r"^" + fence + r"(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*" + fence + r"$", "", raw)
        try:
            return json.loads(raw)
        except Exception:
            match = re.search(r"\{[\s\S]*\}", raw)
            if match:
                return json.loads(match.group(0))
            raise

    @staticmethod
    def _resolve_codex_token() -> str:
        paths = [
            Path.home() / ".openclaw" / "agents" / "main" / "agent" / "auth-profiles.json",
            Path.home() / ".codex" / "auth.json",
        ]
        for path in paths:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue

            profiles = payload.get("profiles") if isinstance(payload, dict) else None
            if isinstance(profiles, dict):
                for key in ("openai-codex:default", "openai-codex:quentinwang88@gmail.com"):
                    profile = profiles.get(key) or {}
                    access = str(profile.get("access") or "").strip()
                    if access:
                        return access

            tokens = payload.get("tokens") if isinstance(payload, dict) else None
            if isinstance(tokens, dict):
                access = str(tokens.get("access_token") or "").strip()
                if access:
                    return access

        raise RuntimeError(
            "未找到本机 openai-codex OAuth access token；请先确认 Codex/OpenAI 登录态可用。"
        )

    @staticmethod
    def _build_four_field_prompt(context: Dict[str, str], task_label: str, duration: float, frame_count: int) -> str:
        return f"""
你是 video-remake-lite skill 的执行模型。必须使用本机 Codex/OpenAI gpt-5.5 的判断口径，基于短视频关键帧完成养号视频高保真轻量复刻。

【养号高保真轻量复刻总控】
当前任务不是原创短视频生成，也不是带货视频复刻，而是养号视频高保真轻量复刻。
优先保留原视频最高光的钩子、动作、情绪、节奏和内容骨架；只做必要的轻微本地化与防判重改写。
不主动加入商品、不主动讲卖点、不主动做转化；非商品展示型不允许强行植入商品、卖点或购买理由。
表达载体优先继承原视频：如果原视频是 BGM+字幕，复刻视频也优先采用 BGM+字幕；不要为了讲清楚而强行加口播。
所有字段都可以用中文写执行说明、镜头说明、场景、动作、情绪、BGM、节奏和负面限制词；但会被视频画面显示或朗读的字幕、旁白、口播、台词、屏幕文字必须使用目标语言，不能出现中文。

【素材说明】
- 我提供的是同一条原视频按时间顺序抽出的关键帧，不是静态图片合集。
- 原视频时长约 {duration:.1f} 秒；关键帧数量 {frame_count} 张。
- 如果无法从关键帧精确判断 BGM/口播，请不要编造具体歌词；可基于画面节奏写“同节奏/同情绪 BGM 建议”，并标注不可精确识别。
- 内容分支：{context.get('content_branch_label') or '非商品展示型'}
- 目标国家：{context.get('target_country') or '未提供'}
- 目标语言：{context.get('target_language') or '未提供'}
- 商品类型：{context.get('product_type') or '未提供'}
- 店铺ID：{context.get('store_id') or '未提供'}
- 任务编号：{task_label}

【输出任务】
请一次性生成并只返回合法 JSON，不要 Markdown，不要代码块，不要额外解释。
JSON 必须包含以下 4 个字符串字段：
1. "脚本拆解"
2. "复刻卡"
3. "复刻后的脚本"
4. "最终复刻视频提示词"

【四列内容要求】
- 脚本拆解：包含时长决策、复刻适配判断、原视频高光DNA、人物动作/情绪/节奏、必须保留项、允许轻改项、防跑偏提醒。
- 复刻卡：给出轻微本地化复刻方案；只能轻改，不能变成原创，不能带货化。
- 复刻后的脚本：给出唯一版本的可执行复刻脚本/分镜脚本。若有字幕/旁白/口播/台词/屏幕文字，实际内容必须使用目标语言；中文只能用于内部说明、中文含义、执行提醒，不能写进会被展示或朗读的内容里。
- 最终复刻视频提示词：给视频生成模型直接消费的最终提示词。必须包含镜头顺序、人物动作、情绪目标、字幕/旁白、BGM/节奏要求、卡点动作、负面限制词；不能输出多个方向。提示词本身可以用中文写执行说明，但所有会被视频显示或朗读的文字必须使用目标语言，不能出现中文。

【口播/字幕/屏幕文字语言硬规则】
1. “字幕/旁白”“口播”“台词”“屏幕文字”“显示文字”“画面文字”“on-screen text”“subtitle”“voiceover”“spoken line”等字段里的实际内容，必须只使用目标语言。
2. 上述可显示/可朗读字段禁止出现中文，禁止中外文混写。
3. 中文翻译或中文含义只能放在“中文含义（不可发声/不可显示）”“执行提醒”“说明”等字段，不能放进字幕/旁白/口播字段。
4. 如果某个镜头没有字幕或口播，字段值写“无字幕/无口播”，并明确这是执行说明，不是要显示在画面里的文字。
5. 负面限制词必须包含：不要出现中文口播、不要出现中文字幕、不要出现中文屏幕文字、不要把中文说明当成画面文字。

【硬性限制】
- 非商品展示型不强制商品出现，不写商品卖点、价格、下单引导。
- 不要把原视频改成广告片、带货片、教程片或大幅原创。
- 防判重只能轻微改：背景/角度/小动作/字幕措辞可轻改，核心高光和主节奏不能改。
- 若口播/字幕/屏幕文字字段里出现中文即视为失败；生成前必须自检并改成目标语言。
- 返回 JSON 字符串里的换行请用 \\n。
""".strip()
