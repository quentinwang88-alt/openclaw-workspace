#!/usr/bin/env python3
"""Codex/OpenAI gpt-5.5 client for video-remake-lite.

The Codex Responses API does not accept mp4/video input directly. The client
therefore downloads the Feishu video, samples ordered keyframes, and sends
those frames to the local openai-codex OAuth endpoint with model gpt-5.5.
"""

from __future__ import annotations

import base64
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import httpx
import imageio.v2 as imageio
import requests
from PIL import Image

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
FRAME_COUNT = int(os.environ.get("VIDEO_REMAKE_FRAME_COUNT", "8") or "8")
FRAME_MAX_SIZE = int(os.environ.get("VIDEO_REMAKE_FRAME_MAX_SIZE", "768") or "768")
FRAME_QUALITY = int(os.environ.get("VIDEO_REMAKE_FRAME_QUALITY", "88") or "88")
WORK_DIR = Path(os.environ.get("VIDEO_REMAKE_WORK_DIR", "/tmp/video_remake_codex_gpt55"))
HAN_RE = re.compile(r"[\u3400-\u9fff]")
SPOKEN_TEXT_LABEL_RE = re.compile(
    r"(?im)^(?P<label>.*(?:字幕|旁白|口播|台词|屏幕文字|显示文字|画面文字|on-screen text|subtitle|voiceover|spoken line).{0,30}?[:：])(?P<value>.*)$"
)
NO_SPOKEN_TEXT_VALUES = {
    "",
    "无",
    "无字幕",
    "无口播",
    "无旁白",
    "无字幕/无口播",
    "无字幕/无旁白",
    "无字幕，无口播",
    "无字幕，无旁白",
    "none",
    "no",
}


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
    ) -> Dict[str, str]:
        """Generate the four Feishu output fields in one gpt-5.5 call."""
        _video_path, duration, _fps, frames = self._sample_video_frames(video_url, task_label)
        prompt = self._build_four_field_prompt(context, task_label, duration, len(frames))
        raw = self._responses_text(prompt=prompt, frames=frames)
        data = self._extract_json(raw)
        required = ["脚本拆解", "复刻卡", "复刻后的脚本", "最终复刻视频提示词"]
        missing = [key for key in required if not str(data.get(key) or "").strip()]
        if missing:
            raise RuntimeError("Codex gpt-5.5 输出缺少字段: " + ", ".join(missing))
        data["最终复刻视频提示词"] = self._ensure_spoken_text_no_chinese(
            str(data["最终复刻视频提示词"]),
            context,
        )
        return {key: str(data[key]).strip() for key in required}

    def chat_with_video(self, video_url: str, prompt: str, max_tokens: int = 2500) -> str:
        """Compatibility API: run a prompt against sampled video keyframes."""
        _video_path, _duration, _fps, frames = self._sample_video_frames(video_url, "compat")
        return self._responses_text(prompt=prompt, frames=frames)

    def chat_text(self, prompt: str, max_tokens: int = 2500) -> str:
        """Compatibility API: run a text-only prompt through Codex gpt-5.5."""
        return self._responses_text(prompt=prompt, frames=[])

    def _responses_text(self, *, prompt: str, frames: Iterable[Tuple[Path, float]]) -> str:
        content: List[Dict[str, Any]] = [{"type": "input_text", "text": prompt}]
        for index, (path, timestamp) in enumerate(frames, 1):
            content.append({"type": "input_text", "text": f"关键帧 {index}，约 {timestamp:.1f} 秒："})
            content.append({"type": "input_image", "image_url": self._image_data_url(path)})

        body: Dict[str, Any] = {
            "model": self.model,
            "instructions": (
                "You are a precise short-video remake workflow assistant. "
                "Return only valid JSON when requested."
            ),
            "input": [{"role": "user", "content": content}],
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
            match = SPOKEN_TEXT_LABEL_RE.match(line.strip())
            if not match:
                continue
            value = match.group("value").strip().strip("\"'` ")
            normalized = re.sub(r"\s+", "", value).lower()
            if normalized in NO_SPOKEN_TEXT_VALUES:
                continue
            if HAN_RE.search(value):
                bad_lines.append(line)
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

    def _sample_video_frames(self, video_url: str, task_label: str) -> Tuple[Path, float, float, List[Tuple[Path, float]]]:
        task_dir = self.work_dir / re.sub(r"[^A-Za-z0-9_.-]+", "_", str(task_label or "task"))
        task_dir.mkdir(parents=True, exist_ok=True)
        video_path = task_dir / "source.mp4"
        if not video_path.exists() or video_path.stat().st_size == 0:
            video_path.write_bytes(requests.get(video_url, timeout=90).content)

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

            sample_count = min(max(FRAME_COUNT, 1), frame_count)
            indexes = sorted(
                set(
                    int(i * (frame_count - 1) / max(sample_count - 1, 1))
                    for i in range(sample_count)
                )
            )
            frames: List[Tuple[Path, float]] = []
            for index, frame_index in enumerate(indexes, 1):
                image = Image.fromarray(reader.get_data(frame_index)).convert("RGB")
                image.thumbnail((FRAME_MAX_SIZE, FRAME_MAX_SIZE))
                path = task_dir / f"frame_{index:02d}.jpg"
                image.save(path, quality=FRAME_QUALITY)
                timestamp = frame_index / fps if fps else 0
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
