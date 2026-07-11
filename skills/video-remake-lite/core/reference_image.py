#!/usr/bin/env python3
"""Image 2 visual approval layer for video-remake-lite."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from PIL import Image, ImageStat

from core.bitable import FeishuBitableClient, normalize_cell_value
from core.llm_client import VideoRemakeLLMClient


APPROVAL_AUTO = "自动预览"
APPROVAL_MANUAL = "人工确认"
APPROVAL_STRICT = "严格确认"

STATUS_GENERATING = "生成中"
STATUS_QA = "质检中"
STATUS_PENDING_APPROVAL = "待确认"
STATUS_CONFIRMED = "已确认"
STATUS_REWORK = "需重做"
STATUS_FAILED = "失败"
STATUS_CONFLICT = "参考冲突"

ACTION_CONFIRM = "确认"
ACTION_REGENERATE = "重新生成"
VIDEO_VISUAL_LOCK_HEADER = "【人物与造型硬锁（参考图优先）】"
INPUT_MODE_COMPREHENSIVE = "综合参考"
INPUT_MODE_SCRIPT_CLOTHING = "脚本+服装参考图"
INPUT_MODE_SCRIPT_ONLY = "仅脚本"
REFERENCE_INPUT_MODES = {
    INPUT_MODE_COMPREHENSIVE,
    INPUT_MODE_SCRIPT_CLOTHING,
    INPUT_MODE_SCRIPT_ONLY,
}


@dataclass(frozen=True)
class ReferenceImageResult:
    attachment: Optional[Dict[str, Any]]
    visual_lock_card: str
    image_prompt: str
    status: str
    approval_mode: str
    version: int
    qa_result: str
    input_fingerprint: str
    error_message: str = ""
    character_appearance_anchor: str = ""

    @property
    def sync_ready(self) -> bool:
        return self.status == STATUS_CONFIRMED and bool(self.attachment)


def extract_attachments(raw_value: Any) -> List[Dict[str, Any]]:
    if isinstance(raw_value, dict) and raw_value.get("file_token"):
        return [raw_value]
    if isinstance(raw_value, list):
        return [item for item in raw_value if isinstance(item, dict) and item.get("file_token")]
    return []


def build_reference_input_fingerprint(
    fields: Dict[str, Any],
    mapping: Dict[str, Optional[str]],
    outputs: Dict[str, str],
) -> str:
    payload: Dict[str, Any] = {
        "requirements": normalize_cell_value(fields.get(mapping.get("reference_requirements"))),
        "input_mode": normalize_cell_value(fields.get(mapping.get("reference_input_mode"))),
        "character_appearance_anchor": normalize_cell_value(fields.get(mapping.get("character_appearance_anchor"))),
        "character_identity_setting": normalize_cell_value(fields.get(mapping.get("character_identity_setting"))),
        "remake_card": outputs.get("复刻卡", ""),
        "final_prompt": outputs.get("最终复刻视频提示词", ""),
        "attachments": {},
    }
    for logical_name in (
        "person_reference_images",
        "clothing_reference_images",
        "scene_reference_images",
        "reference_images",
    ):
        field_name = mapping.get(logical_name)
        payload["attachments"][logical_name] = [
            str(item.get("file_token") or "")
            for item in extract_attachments(fields.get(field_name))
        ] if field_name else []
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def apply_visual_lock_to_video_prompt(final_prompt: str, visual_lock_card: str) -> str:
    """Carry the approved visual identity into the downstream video prompt."""
    prompt = str(final_prompt or "").strip()
    lock_card = str(visual_lock_card or "").strip()
    if not lock_card:
        return prompt
    lock_card = (
        lock_card.replace("均可去文字化和去品牌化", "均应遵照服装参考图保留视觉结构")
        .replace("可去文字化和去品牌化", "应遵照服装参考图保留视觉结构")
        .replace("可去标识化", "应保留参考图已有的视觉标记")
    )

    existing_start = prompt.find(VIDEO_VISUAL_LOCK_HEADER)
    if existing_start >= 0:
        next_section = prompt.find("\n【", existing_start + len(VIDEO_VISUAL_LOCK_HEADER))
        prompt = prompt[:existing_start] if next_section < 0 else prompt[:existing_start] + prompt[next_section + 1:]

    # These phrases come from horizontal source footage and must never override
    # the vertical reference image that will be sent to the video generator.
    prompt = prompt.replace("上下黑边，", "").replace("左右黑边，", "").replace("横向居中，", "")
    lock_block = f"""{VIDEO_VISUAL_LOCK_HEADER}
以下来自用户参考图和已质检的 AI 首镜图，优先级高于后文任何泛化人物、服装或构图描述：
{lock_card}

执行硬规则：人物外观、发型、体型、服装结构和配色必须逐项遵照上述锁定卡，不得把具体服装简化成泛泛的“球衣/红绿上衣”。服装必须保留领型、门襟/扣件、袖口与边饰、侧片/拼接形状、图案或刺绣位置、版型与面料质感；用户服装参考图里已有的徽章、号码、品牌标记或文字属于服装视觉结构的一部分，应保留其位置、比例和视觉效果，不得凭空新增参考图之外的水印、UI、品牌或文字。画面必须铺满完整9:16竖版画布，禁止任何上下或左右黑边、白边及横屏留白。"""
    return f"{lock_block}\n\n{prompt}".strip()


class ReferenceImageWorkflow:
    def __init__(
        self,
        *,
        client: FeishuBitableClient,
        llm_client: VideoRemakeLLMClient,
        mapping: Dict[str, Optional[str]],
    ) -> None:
        self.client = client
        self.llm_client = llm_client
        self.mapping = mapping
        self.work_dir = Path(
            os.environ.get(
                "VIDEO_REMAKE_REFERENCE_WORK_DIR",
                "/Users/likeu3/.openclaw/shared/data/video-remake-lite/reference-images",
            )
        )
        self.image_skill = Path(
            os.environ.get(
                "VIDEO_REMAKE_OPENAI_IMAGE_SKILL",
                "/Users/likeu3/.openclaw/workspace/skills/openai-image/run_pipeline.py",
            )
        )
        self.max_auto_retries = max(
            0,
            int(os.environ.get("VIDEO_REMAKE_REFERENCE_MAX_RETRIES", "1") or "1"),
        )

    def has_user_inputs(self, fields: Dict[str, Any]) -> bool:
        if normalize_cell_value(fields.get(self.mapping.get("reference_requirements"))):
            return True
        return any(
            extract_attachments(fields.get(self.mapping.get(logical_name)))
            for logical_name in (
                "person_reference_images",
                "clothing_reference_images",
                "scene_reference_images",
            )
            if self.mapping.get(logical_name)
        )

    def resolve_input_mode(self, fields: Dict[str, Any]) -> str:
        configured = normalize_cell_value(fields.get(self.mapping.get("reference_input_mode")))
        return configured if configured in REFERENCE_INPUT_MODES else INPUT_MODE_COMPREHENSIVE

    def resolve_approval_mode(self, fields: Dict[str, Any], *, force_manual: bool = False) -> str:
        configured = normalize_cell_value(fields.get(self.mapping.get("reference_approval_mode")))
        if configured not in {APPROVAL_AUTO, APPROVAL_MANUAL, APPROVAL_STRICT}:
            configured = APPROVAL_AUTO
        if force_manual or self.has_user_inputs(fields):
            return APPROVAL_MANUAL if configured == APPROVAL_AUTO else configured
        return configured

    def generate(
        self,
        *,
        fields: Dict[str, Any],
        outputs: Dict[str, str],
        context: Dict[str, str],
        source_frame_paths: Sequence[Path],
        task_label: str,
        version: int,
        force_manual: bool = False,
    ) -> ReferenceImageResult:
        version = max(int(version or 1), 1)
        task_dir = self.work_dir / self._safe_name(task_label) / f"v{version}"
        task_dir.mkdir(parents=True, exist_ok=True)
        requirements = normalize_cell_value(fields.get(self.mapping.get("reference_requirements")))
        modification_notes = normalize_cell_value(fields.get(self.mapping.get("reference_revision_notes")))
        approval_mode = self.resolve_approval_mode(fields, force_manual=force_manual)
        input_mode = self.resolve_input_mode(fields)
        fingerprint = build_reference_input_fingerprint(fields, self.mapping, outputs)

        labeled_images = self._materialize_reference_inputs(
            fields=fields,
            task_dir=task_dir,
            source_frame_paths=source_frame_paths,
            include_previous_ai=force_manual,
            input_mode=input_mode,
        )
        anchor_images = self._materialize_character_anchor_inputs(
            fields=fields,
            task_dir=task_dir,
            source_frame_paths=source_frame_paths,
        )
        character_anchor = self.llm_client.extract_character_appearance_anchor(anchor_images)
        character_identity_setting = normalize_cell_value(
            fields.get(self.mapping.get("character_identity_setting"))
        )
        qa_reference_images = [*labeled_images, *anchor_images]
        spec = self.llm_client.build_reference_image_spec(
            context=context,
            remake_card=outputs.get("复刻卡", ""),
            final_prompt=outputs.get("最终复刻视频提示词", ""),
            reference_requirements=requirements,
            modification_notes=modification_notes,
            labeled_images=labeled_images,
            input_mode=input_mode,
            character_appearance_anchor=character_anchor,
            character_identity_setting=character_identity_setting,
        )
        conflict = spec.get("参考冲突", "").strip()
        if self._has_unresolved_conflict(conflict):
            return ReferenceImageResult(
                attachment=None,
                visual_lock_card=spec["视觉锁定卡"],
                image_prompt=spec["AI参考图提示词"],
                status=STATUS_CONFLICT,
                approval_mode=approval_mode,
                version=version,
                qa_result=conflict,
                input_fingerprint=fingerprint,
                error_message=conflict,
                character_appearance_anchor=character_anchor,
            )

        image_prompt = spec["AI参考图提示词"]
        generated_path = self._run_image2(
            task_label=task_label,
            version=version,
            prompt=image_prompt,
            reference_paths=[path for path, _label in labeled_images],
            output_dir=task_dir,
        )
        qa = self.llm_client.evaluate_reference_image(
            visual_lock_card=spec["视觉锁定卡"],
            generated_image=generated_path,
            labeled_reference_images=qa_reference_images,
            character_identity_setting=character_identity_setting,
        )
        qa = self._apply_canvas_quality_guard(generated_path, qa)

        for retry_index in range(self.max_auto_retries):
            if qa.get("通过"):
                break
            retry_instruction = str(qa.get("重做指令") or "").strip()
            if not retry_instruction:
                break
            retry_prompt = f"{image_prompt}\n\n【自动质检修正】\n{retry_instruction}"
            retry_reference_paths = [path for path, _label in labeled_images]
            if input_mode == INPUT_MODE_COMPREHENSIVE:
                retry_reference_paths = [generated_path, *retry_reference_paths]
            generated_path = self._run_image2(
                task_label=f"{task_label}_retry{retry_index + 1}",
                version=version,
                prompt=retry_prompt,
                reference_paths=retry_reference_paths,
                output_dir=task_dir,
            )
            image_prompt = retry_prompt
            qa = self.llm_client.evaluate_reference_image(
                visual_lock_card=spec["视觉锁定卡"],
                generated_image=generated_path,
                labeled_reference_images=qa_reference_images,
                character_identity_setting=character_identity_setting,
            )
            qa = self._apply_canvas_quality_guard(generated_path, qa)

        attachment = self._upload_generated_image(generated_path, task_label=task_label, version=version)
        qa_text = json.dumps(qa, ensure_ascii=False, indent=2)
        if not qa.get("通过"):
            status = STATUS_REWORK
        elif approval_mode in {APPROVAL_MANUAL, APPROVAL_STRICT}:
            status = STATUS_PENDING_APPROVAL
        else:
            status = STATUS_CONFIRMED
        return ReferenceImageResult(
            attachment=attachment,
            visual_lock_card=spec["视觉锁定卡"],
            image_prompt=image_prompt,
            status=status,
            approval_mode=approval_mode,
            version=version,
            qa_result=qa_text,
            input_fingerprint=fingerprint,
            character_appearance_anchor=character_anchor,
        )

    def _materialize_character_anchor_inputs(
        self,
        *,
        fields: Dict[str, Any],
        task_dir: Path,
        source_frame_paths: Sequence[Path],
    ) -> List[Tuple[Path, str]]:
        local_frames = [Path(path) for path in source_frame_paths if Path(path).exists()]
        if local_frames:
            return [
                (path, f"原视频人物外观对照帧{index}，只用于生成文字锚点和质检，不输入Image2")
                for index, path in enumerate(local_frames[:2], 1)
            ]
        if self.mapping.get("reference_images"):
            return self._download_field_attachments(
                fields.get(self.mapping["reference_images"]),
                task_dir,
                "character_source",
                "原视频人物外观对照帧，只用于生成文字锚点和质检，不输入Image2",
                limit=2,
            )
        return []

    def _materialize_reference_inputs(
        self,
        *,
        fields: Dict[str, Any],
        task_dir: Path,
        source_frame_paths: Sequence[Path],
        include_previous_ai: bool,
        input_mode: str,
    ) -> List[Tuple[Path, str]]:
        labeled: List[Tuple[Path, str]] = []
        if (
            input_mode == INPUT_MODE_COMPREHENSIVE
            and include_previous_ai
            and self.mapping.get("ai_reference_image")
        ):
            labeled.extend(
                self._download_field_attachments(
                    fields.get(self.mapping["ai_reference_image"]),
                    task_dir,
                    "previous_ai",
                    "上一版AI首镜图，仅保留正确部分并按修改意见调整",
                    limit=1,
                )
            )
        role_specs = [
            ("person_reference_images", "person", "人物参考图，只锁身份、发型和体型", 2),
            ("clothing_reference_images", "clothing", "服装参考图，只锁款式、颜色和材质", 2),
            ("scene_reference_images", "scene", "场景参考图，只锁环境、构图和光线", 1),
        ]
        if input_mode == INPUT_MODE_SCRIPT_CLOTHING:
            role_specs = [
                ("clothing_reference_images", "clothing", "服装参考图，只锁款式、颜色和材质", 2),
            ]
        elif input_mode == INPUT_MODE_SCRIPT_ONLY:
            role_specs = []
        for logical_name, prefix, label, limit in role_specs:
            field_name = self.mapping.get(logical_name)
            if field_name:
                labeled.extend(
                    self._download_field_attachments(
                        fields.get(field_name), task_dir, prefix, label, limit=limit
                    )
                )

        local_source_frames = [Path(path) for path in source_frame_paths if Path(path).exists()]
        if input_mode != INPUT_MODE_COMPREHENSIVE:
            return labeled[:7]
        if not local_source_frames and self.mapping.get("reference_images"):
            labeled.extend(
                self._download_field_attachments(
                    fields.get(self.mapping["reference_images"]),
                    task_dir,
                    "source",
                    "原视频时序帧，只锁生活感、构图和动作起点",
                    limit=2,
                )
            )
        else:
            for index, path in enumerate(local_source_frames[:2], 1):
                labeled.append((path, f"原视频时序帧{index}，只锁生活感、构图和动作起点"))
        return labeled[:7]

    def _download_field_attachments(
        self,
        raw_value: Any,
        task_dir: Path,
        prefix: str,
        label: str,
        *,
        limit: int,
    ) -> List[Tuple[Path, str]]:
        downloaded: List[Tuple[Path, str]] = []
        for index, attachment in enumerate(extract_attachments(raw_value)[:limit], 1):
            content, file_name, _content_type, _size = self.client.download_attachment_bytes(attachment)
            suffix = Path(file_name).suffix.lower()
            if suffix not in {".png", ".jpg", ".jpeg", ".webp"}:
                suffix = ".jpg"
            path = task_dir / f"{prefix}_{index:02d}{suffix}"
            path.write_bytes(content)
            downloaded.append((path, f"{label}（第{index}张）"))
        return downloaded

    def _run_image2(
        self,
        *,
        task_label: str,
        version: int,
        prompt: str,
        reference_paths: Sequence[Path],
        output_dir: Path,
    ) -> Path:
        if not self.image_skill.exists():
            raise FileNotFoundError(f"openai-image入口不存在: {self.image_skill}")
        task_id = f"video_remake_{self._safe_name(task_label)}_v{version}"
        input_path = output_dir / f"{task_id}_input.json"
        payload = {
            "task_id": task_id,
            "task_type": "video_remake_reference",
            "target_field": "AI视频参考图",
            "mode": "edit" if reference_paths else "generate",
            "prompt": prompt,
            "input_image_path": str(reference_paths[0]) if reference_paths else "",
            "input_image_paths": [str(path) for path in reference_paths[:7]],
            "size": "1024x1536",
            "quality": os.environ.get("VIDEO_REMAKE_REFERENCE_IMAGE_QUALITY", "high"),
            "output_format": "png",
            "output_dir": str(output_dir),
            "n": 1,
        }
        input_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        env = dict(os.environ)
        env["OPENAI_IMAGE_API_MODE"] = "codex"
        env["OPENAI_IMAGE_TIMEOUT"] = os.environ.get("VIDEO_REMAKE_REFERENCE_IMAGE_TIMEOUT", "420")
        proxy = os.environ.get("VIDEO_REMAKE_IMAGE_PROXY_URL", "http://127.0.0.1:18080").strip()
        if proxy:
            env["HTTP_PROXY"] = proxy
            env["HTTPS_PROXY"] = proxy
            env["http_proxy"] = proxy
            env["https_proxy"] = proxy
        completed = subprocess.run(
            [sys.executable, str(self.image_skill), "--input", str(input_path)],
            capture_output=True,
            text=True,
            timeout=int(env["OPENAI_IMAGE_TIMEOUT"]) + 60,
            env=env,
        )
        result: Optional[Dict[str, Any]] = None
        stdout = completed.stdout.strip()
        try:
            parsed = json.loads(stdout)
            if isinstance(parsed, dict):
                result = parsed
        except json.JSONDecodeError:
            # The shared image skill may emit progress logs before its final JSON.
            decoder = json.JSONDecoder()
            for index, char in enumerate(stdout):
                if char != "{":
                    continue
                try:
                    parsed, _end = decoder.raw_decode(stdout, index)
                except json.JSONDecodeError:
                    continue
                if isinstance(parsed, dict) and (
                    "status" in parsed or "output_image_paths" in parsed
                ):
                    result = parsed
        if result is None:
            raise RuntimeError(
                f"Image 2返回无法解析: {completed.stdout[-500:]} {completed.stderr[-500:]}"
            )
        if completed.returncode != 0 or result.get("status") != "success":
            raise RuntimeError(str(result.get("error_message") or completed.stderr[-1000:] or "Image 2生成失败"))
        paths = [Path(path) for path in result.get("output_image_paths") or []]
        if not paths or not paths[0].exists():
            raise RuntimeError("Image 2未返回有效图片文件")
        return paths[0]

    def _upload_generated_image(self, path: Path, *, task_label: str, version: int) -> Dict[str, Any]:
        content = path.read_bytes()
        return self.client.upload_attachment(
            content=content,
            file_name=f"{self._safe_name(task_label)}_ai_video_reference_v{version}.png",
            content_type="image/png",
            size=len(content),
        )

    @staticmethod
    def _apply_canvas_quality_guard(image_path: Path, qa: Dict[str, Any]) -> Dict[str, Any]:
        """Reject obvious letterboxing that a video model would preserve."""
        try:
            with Image.open(image_path) as image:
                image = image.convert("RGB")
                width, height = image.size
                band_height = max(1, int(height * 0.12))
                top = image.crop((0, 0, width, band_height))
                bottom = image.crop((0, height - band_height, width, height))
                if not (
                    ReferenceImageWorkflow._is_near_black_uniform(top)
                    and ReferenceImageWorkflow._is_near_black_uniform(bottom)
                ):
                    return qa
        except Exception:
            return qa

        guarded = dict(qa)
        issues = list(guarded.get("硬性问题") or [])
        issue = "画面上下存在大面积信箱黑边，无法作为铺满9:16的视频首镜输入"
        if issue not in issues:
            issues.append(issue)
        guarded["硬性问题"] = issues
        guarded["结论"] = "不通过"
        guarded["总分"] = min(int(guarded.get("总分") or 0), 60)
        guarded["通过"] = False
        instruction = str(guarded.get("重做指令") or "").strip()
        correction = "画面必须铺满完整9:16竖版画布，移除所有上下或左右黑边、白边和横屏信箱式留白。"
        guarded["重做指令"] = f"{instruction}\n{correction}".strip()
        return guarded

    @staticmethod
    def _is_near_black_uniform(image: Image.Image) -> bool:
        stat = ImageStat.Stat(image)
        means = stat.mean
        extrema = stat.extrema
        return max(means) < 12 and max(high - low for low, high in extrema) < 30

    @staticmethod
    def _has_unresolved_conflict(conflict: str) -> bool:
        normalized = str(conflict or "").strip()
        if not normalized or normalized.lower() == "none" or normalized.startswith("无"):
            return False
        unresolved_markers = ("无法同时", "无法满足", "不可兼顾", "无法执行")
        if any(marker in normalized for marker in unresolved_markers):
            return True
        resolved_markers = ("已按", "已根据", "已依照", "可按", "可同时")
        resolution_actions = ("执行", "处理", "调整", "优先", "满足")
        if any(marker in normalized for marker in resolved_markers) and any(
            action in normalized for action in resolution_actions
        ):
            return False
        return True

    @staticmethod
    def _safe_name(value: str) -> str:
        return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "task")).strip("._") or "task"
