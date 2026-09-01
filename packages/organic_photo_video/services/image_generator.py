"""Image generation adapter (Stage C) for OPV.

Wraps the existing ``skills/openai-image`` service (gpt-image-2, codex OAuth
path) so OPV never builds a second model-auth stack (MODEL_HANDOFF 4.3).

Prompt composition is deterministic: product identity lock + persona lock +
frozen look recipe + scene + per-slot intent. The generator never invents
product structure beyond the reference images, and every shot carries the
product/persona/look/scene refs from the plan for cross-shot consistency.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
OPENAI_IMAGE_SKILL_DIR = WORKSPACE_ROOT / "skills" / "openai-image"

PROVIDER_NAME = "openai-image"
MODEL_NAME = "gpt-image-2"
OPV_SIZE = "1024x1536"  # 9:16 vertical
OPV_QUALITY = "high"
OPV_OUTPUT_FORMAT = "png"


@dataclass
class ShotGenerationRequest:
    task_id: str
    slot_index: int
    slot_role: str
    shot_version: int
    plan_shot: Dict[str, Any]
    product: Dict[str, Any]
    persona_snapshot: Dict[str, Any]
    look_snapshot: Dict[str, Any]
    scene_snapshot: Dict[str, Any]
    output_dir: str
    continuity_reference_images: List[str] = field(default_factory=list)
    product_facts: Dict[str, Any] = field(default_factory=dict)
    outfit_state: Dict[str, Any] = field(default_factory=dict)
    recipe_execution: Dict[str, Any] = field(default_factory=dict)
    camera_hint: str = ""
    reference_roles: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GenerationOutcome:
    ok: bool
    image_path: Optional[str] = None
    provider: str = PROVIDER_NAME
    model: str = MODEL_NAME
    request_id: str = ""
    width: Optional[int] = None
    height: Optional[int] = None
    error: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)


def read_image_dimensions(path: str) -> Optional[tuple]:
    """Return (width, height) for PNG/JPEG without external dependencies."""
    try:
        with open(path, "rb") as handle:
            head = handle.read(32)
    except OSError:
        return None
    if head[:8] == b"\x89PNG\r\n\x1a\n" and len(head) >= 24:
        width, height = struct.unpack(">II", head[16:24])
        return int(width), int(height)
    try:
        with open(path, "rb") as handle:
            data = handle.read()
    except OSError:
        return None
    if data[:2] != b"\xff\xd8":  # not JPEG
        return None
    index = 2
    while index + 9 < len(data):
        if data[index] != 0xFF:
            index += 1
            continue
        marker = data[index + 1]
        if 0xC0 <= marker <= 0xCF and marker not in (0xC4, 0xC8, 0xCC):
            height, width = struct.unpack(">HH", data[index + 5 : index + 9])
            return int(width), int(height)
        if marker in (0xD8, 0x01) or 0xD0 <= marker <= 0xD7:
            index += 2
            continue
        length = struct.unpack(">H", data[index + 2 : index + 4])[0]
        index += 2 + length
    return None


_SLOT_FRAMING = {
    "hero": "正面平视构图（膝上或近全身），人物为绝对主体，商品完整占据主体区域，首屏吸引力优先",
    "full_look": "全身构图，清楚展示上下装关系、腰线位置和身材比例",
    "lifestyle": "自然生活动作瞬间（与场景匹配，如行走、推行李），商品在画面中仍清楚可辨",
    "detail": "近距离局部特写，聚焦商品结构与面料质感，手机近距离拍摄，轻微自然手持感",
    "second_angle": "第二机位角度（侧后 45 度回眸或另一站位），与前面几张是同一人物、同一穿搭、同一场景光线",
}


def compose_shot_prompt(request: ShotGenerationRequest) -> str:
    """Deterministic prompt for one shot; all anchors come from the plan."""
    product = request.product
    persona = request.persona_snapshot
    look = request.look_snapshot
    scene = request.scene_snapshot
    shot = request.plan_shot
    product_facts = request.product_facts or {}
    outfit_state = request.outfit_state or {}
    recipe_execution = request.recipe_execution or {}

    product_name = str(
        product.get("product_name") or product.get("product_id") or "目标商品"
    )
    lines: List[str] = [
        "生成一张竖屏 9:16 真实手机感穿搭照片（TikTok 自然流内容，不是电商棚拍）。",
        "",
        "【商品身份锁】",
        f"目标商品：{product_name}；颜色、图案、材质观感、形状和结构只按商品参考图。",
        "保持参考图中的颜色、版型、衣长、领型、前襟和袖口结构；禁止替换成相似款或重新设计。",
        "忽略商品参考图中的模特、脸、妆发、姿态、滤镜与背景。",
        "",
        "【人物身份锁】",
    ]
    facts = product_facts.get("facts") or {}
    known_facts = [
        f"{key}={value}"
        for key, value in facts.items()
        if value not in (None, "", "unknown")
    ]
    if known_facts:
        lines.append("已确认产品事实：" + "；".join(known_facts))
    persona_desc = str(persona.get("prompt_core") or persona.get("name") or "年轻女性")
    lines.append(f"人物模板：{persona.get('name') or 'persona'}；{persona_desc}")
    lines.append("自然肤色与真实皮肤纹理；自然淡妆；面部不要碎发遮眼。")
    lines.append("比例：自然成年女性比例，头身比约 1:7.2；不要放大头部，不要缩短躯干或四肢。")
    lines.append("人物外貌只按人物模板生成，不得从商品参考图复制模特的脸或姿势。")
    lines.extend(
        [
            "",
            "【冻结穿搭】",
        ]
    )
    look_recipe = look.get("recipe") or {}
    if look_recipe and not outfit_state:
        label_map = {
            "top_inner": "内搭",
            "target_outer": "目标商品",
            "bottom": "下装",
            "footwear": "鞋履",
            "accessories": "配饰",
            "overall_style": "整体风格",
        }
        for key, label in label_map.items():
            if look_recipe.get(key):
                lines.append(f"{label}：{look_recipe[key]}")
    if outfit_state:
        lines.append(
            f"当前穿搭状态：{shot.get('outfit_state_ref') or 'FINAL'}"
            f"（{outfit_state.get('state_purpose') or ''}）"
        )
        for key, label in (
            ("bottom", "下装"),
            ("shoes", "鞋履"),
            ("bag", "包"),
            ("accessories", "配饰"),
            ("style_direction", "风格方向"),
        ):
            value = outfit_state.get(key)
            if isinstance(value, dict):
                value = " / ".join(str(v) for v in value.values() if v)
            if value:
                lines.append(f"{label}：{value}")
    if look.get("prompt_core"):
        lines.append(f"穿搭要求：{look['prompt_core']}")
    if recipe_execution.get("transform_mode") == "controlled_outfit_change":
        allowed = outfit_state.get("change_permissions") or []
        lines.append(
            "目标商品本身必须完全不变；补充单品只按当前 outfit state 执行。"
            f"本状态允许变化项：{','.join(allowed) if allowed else '无'}。"
        )
    else:
        lines.append("按冻结配方穿搭；连体单品不得拆分；不得增减单品。")
    lines.extend(["", "【场景】"])
    lines.append(str(scene.get("prompt_core") or scene.get("name") or "明亮自然的生活场景"))
    if scene.get("prompt_negative"):
        lines.append(f"场景负向：{scene['prompt_negative']}")
    lines.extend(
        [
            "",
            "【本张画面意图】",
            f"槽位 {shot.get('slot_index')}（{shot.get('slot_role')}）：{shot.get('purpose', '')}",
            _SLOT_FRAMING.get(request.slot_role, _SLOT_FRAMING["hero"]),
        ]
    )
    camera_hint = request.camera_hint or shot.get("camera_hint") or ""
    if camera_hint:
        lines.append(f"镜头提示：{camera_hint}")
    if request.continuity_reference_images:
        if recipe_execution.get("transform_mode") == "controlled_outfit_change":
            lines.append(
                "连续性参考图只用于锁定同一人物、同一目标商品和光线；"
                "不得照抄参考图的补充穿搭，必须执行当前 outfit state。"
            )
        else:
            lines.append("连续性参考图用于锁定人物、商品、穿搭和光线。")
    if shot.get("overlay_text"):
        lines.append(f"画面短句参考（不要把文字画进图片）：{shot['overlay_text']}")
    lines.extend(
        [
            "",
            "【画面风格】",
            "普通创作者自己用手机竖屏拍摄的原生记录感；自然光、白平衡自然；保留真实皮肤、发丝和衣物纹理。",
            "首先像真实生活记录，其次才是好看；不要棚拍、广告大片、电影灯光、磨皮塑料脸、夸张网红姿势。",
            "",
            "【通用负向要求】",
            "不要文字、字幕、水印、Logo 杜撰；不要尺寸标注；不要多余人物或多余肢体；不要畸形手指。",
            (
                "保持跨图一致：同一人物、同一商品、同一场景光线；"
                "穿搭按各镜头 outfit state 执行。"
                if recipe_execution.get("transform_mode") == "controlled_outfit_change"
                else "保持跨图一致：同一人物、同一商品、同一套穿搭、同一场景光线。"
            ),
            "只输出一张完整画面。",
        ]
    )
    return "\n".join(lines)




# Module-level cache: once imported, the skill's service class survives
# later sys.modules hijacks (e.g. creator-crm's own "core"/"app" packages).
_SKILL_SERVICE_CACHE: Dict[str, Any] = {}


def _load_skill_service():
    import sys

    if _SKILL_SERVICE_CACHE.get("factory") is None:
        if str(OPENAI_IMAGE_SKILL_DIR) not in sys.path:
            sys.path.insert(0, str(OPENAI_IMAGE_SKILL_DIR))
        try:
            from app.config import get_settings
            from core.image_service import ImageService
        except ModuleNotFoundError:
            # Another package claimed the "core"/"app" root names; purge and
            # re-import with our skill dir first on the path.
            for root in ("core", "app"):
                for name in [
                    m
                    for m in list(sys.modules)
                    if m == root or m.startswith(root + ".")
                ]:
                    del sys.modules[name]
            from app.config import get_settings
            from core.image_service import ImageService
        _SKILL_SERVICE_CACHE["factory"] = lambda: ImageService(
            settings=get_settings()
        )
    return _SKILL_SERVICE_CACHE["factory"]

class OpenAIImageGenerator:
    """Adapter over skills/openai-image (lazy import; auth via codex OAuth)."""

    def __init__(self, service=None, size: str = OPV_SIZE, quality: str = OPV_QUALITY):
        self._service = service
        self._size = size
        self._quality = quality

    def _load_service(self):
        if self._service is None:
            self._service = _load_skill_service()()
        return self._service

    def generate_shot(self, request: ShotGenerationRequest) -> GenerationOutcome:
        try:
            service = self._load_service()
            from core.schemas import ImageTaskRequest

            reference_paths = self._reference_paths(request)
            image_request = ImageTaskRequest.from_dict(
                {
                    "task_id": (
                        f"{request.task_id}_P{request.slot_index}_v{request.shot_version}"
                    ),
                    "task_type": "opv_still_shot",
                    "target_field": f"slot_{request.slot_index}",
                    "mode": "edit" if reference_paths else "generate",
                    "prompt": compose_shot_prompt(request),
                    "input_image_paths": reference_paths,
                    "size": self._size,
                    "quality": self._quality,
                    "output_format": OPV_OUTPUT_FORMAT,
                    "output_dir": request.output_dir,
                    "n": 1,
                    "metadata": {
                        "reference_order": [
                            "PRODUCT_IDENTITY",
                            "PERSONA_IDENTITY",
                            "RECIPE_CONTINUITY_ANCHOR",
                        ],
                        "reference_roles": request.reference_roles,
                        "outfit_state_ref": request.plan_shot.get(
                            "outfit_state_ref"
                        ),
                        "opv_task_id": request.task_id,
                        "slot_index": request.slot_index,
                        "shot_version": request.shot_version,
                    },
                }
            )
            result = service.process_task(image_request)
        except Exception as exc:  # noqa: BLE001 - adapter boundary
            return GenerationOutcome(ok=False, error=f"{type(exc).__name__}: {exc}")

        status = str(getattr(result, "status", "") or "")
        paths = list(getattr(result, "output_image_paths", []) or [])
        error_message = str(getattr(result, "error_message", "") or "")
        request_id = str(
            getattr(result, "task_id", "") or f"{request.task_id}_P{request.slot_index}"
        )
        if status != "success" or not paths:
            return GenerationOutcome(
                ok=False,
                request_id=request_id,
                error=error_message or f"generator status {status!r}",
                raw={"status": status},
            )
        image_path = paths[0]
        dimensions = read_image_dimensions(image_path)
        size_ok = check_portrait_916(dimensions)
        return GenerationOutcome(
            ok=size_ok,
            image_path=image_path,
            request_id=request_id,
            width=dimensions[0] if dimensions else None,
            height=dimensions[1] if dimensions else None,
            error="" if size_ok else f"image is not 9:16 portrait: {dimensions}",
            raw={"status": status},
        )

    @staticmethod
    def _reference_paths(request: ShotGenerationRequest) -> List[str]:
        """Local reference files only.

        Asset rows may store Feishu-hosted references (dicts with file_token /
        tmp_url); those are skipped because Feishu API quota is restricted and
        downloads are not allowed here. Mirroring persona reference images to
        local disk is an ops todo before the account goes active.
        """
        paths: List[str] = []
        for source in (
            request.product.get("reference_images", []) or [],
            request.persona_snapshot.get("local_reference_images", [])
            or request.persona_snapshot.get("reference_images", [])
            or [],
            request.continuity_reference_images or [],
        ):
            for entry in source:
                if isinstance(entry, str) and Path(entry).exists():
                    paths.append(entry)
                elif isinstance(entry, dict):
                    candidate = str(entry.get("local_path") or "").strip()
                    if candidate and Path(candidate).exists():
                        paths.append(candidate)
        return paths


# The codex path may return 9:16 at arbitrary pixel sizes (e.g. 941x1672);
# Phase 2 rendering normalizes to 1080x1920, so QC enforces ratio + minimum
# width instead of exact pixels.
MIN_WIDTH_PX = 896
RATIO_TOLERANCE = 0.02


def check_portrait_916(dimensions: Optional[tuple]) -> bool:
    if not dimensions:
        return False
    width, height = dimensions
    if width < MIN_WIDTH_PX:
        return False
    return abs(width / height - 9 / 16) <= RATIO_TOLERANCE


# tiny sys.path helpers so unit tests never import the real skill
def sys_path() -> List[str]:
    import sys

    return sys.path


def add_to_sys_path(path: str) -> None:
    import sys

    sys.path.insert(0, path)
