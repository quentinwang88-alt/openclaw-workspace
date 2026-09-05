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

from services.product_reference_resolver import (
    has_detail_reference,
    select_product_references_for_slot,
)

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
    "lifestyle": "与当前主题和场景明确匹配的自然动作瞬间，商品在画面中仍清楚可辨；不得自行添加未规划道具",
    "detail": "腰部至头部或商品局部近景，聚焦已确认的商品结构、衣长、腰线或材质观感；不得使用全身构图",
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
    presentation = dict(recipe_execution.get("presentation_profile") or {})
    pure_color = str(presentation.get("background_mode") or "") == "solid_color"
    background_color = str(presentation.get("background_color") or "#F6F5F2")

    product_label = {
        "outerwear": "目标外套",
        "dress": "目标连衣裙",
        "top": "目标上装",
        "bottom": "目标下装",
    }.get(str(product.get("category") or "").strip().lower(), "目标商品")
    opening = (
        "生成一张竖屏 9:16 的真实穿搭创作者照片：无缝纯色背景、自然时装感，"
        "不是电商商品图、证件照或刻板影棚照。"
        if pure_color
        else "生成一张竖屏 9:16 真实手机感穿搭照片（TikTok 自然流内容，不是电商棚拍）。"
    )
    lines: List[str] = [
        opening,
        "",
        "【商品身份锁】",
        f"目标商品：{product_label}。商品参考图是颜色、图案、材质观感、形状和结构的最高视觉事实。",
        "商品名称、Look 名称、穿搭模板或其文案不得覆盖商品参考图中的颜色和材质。",
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
    if pure_color:
        lines.append(
            "比例："
            + str(presentation.get("proportion_direction") or "自然修长时装比例，约7.6-8头身；不得机械拉伸")
            + "。相机保持足够距离，避免广角夸大脚部或俯拍压缩腿部。"
        )
    else:
        lines.append("比例：自然成年女性比例，头身比约 1:7.2；不要放大头部，不要缩短躯干或四肢。")
    lines.append("人物外貌只按人物模板生成，不得从商品参考图复制模特的脸或姿势。")
    composition = shot.get("composition_contract") or {}
    framing = _SLOT_FRAMING.get(request.slot_role, _SLOT_FRAMING["hero"])
    if shot.get("shot_grammar") not in (None, "", "LEGACY") and composition.get(
        "instruction"
    ):
        framing = str(composition["instruction"])
    if request.slot_role == "detail" and not has_detail_reference(product):
        framing = (
            "商品参考包没有真实细节图，本张必须改为腰部至头部的安全近景；"
            "只呈现参考图明确可见的颜色、廓形和材质观感，"
            "禁止出现完整腿部、鞋子和大面积地面；"
            "禁止编造纽扣、口袋、缝线、纹理或装饰细节"
        )
    if pure_color and composition.get("instruction"):
        framing = str(composition["instruction"])
    if pure_color and composition.get("framing") == "full_body":
        framing += f" 人物高度约为画面的{presentation.get('full_body_occupancy') or '85-92%'}；头顶和鞋底均完整可见。"
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
            ("top_inner", "内搭"),
            ("onepiece", "连体单品（保持一件完整单品）"),
            ("bottom", "下装"),
            ("shoes", "鞋履"),
            ("bag", "包"),
            ("accessories", "配饰"),
            ("socks", "袜子或腿套"),
            ("style_direction", "风格方向"),
            ("target_wear_mode", "目标商品穿法"),
            ("tucking", "内搭塞衣角方式"),
        ):
            value = outfit_state.get(key)
            if isinstance(value, dict):
                value = " / ".join(str(v) for v in value.values() if v)
            if value:
                lines.append(f"{label}：{value}")
    # prompt_core and target_outer may contain legacy target-garment colors.
    # Structured companion items above are safe; the target garment always
    # comes from the selected product reference pack.
    if recipe_execution.get("transform_mode") == "controlled_outfit_change":
        allowed = outfit_state.get("change_permissions") or []
        lines.append(
            "目标商品本身必须完全不变；补充单品只按当前 outfit state 执行。"
            + (f"本状态额外允许的穿法调整：{','.join(allowed)}。" if allowed else
               "不允许当前冻结搭配之外的自由变化；当前搭配与连续性参考不同时，按本镜搭配替换配套单品。")
        )
    else:
        lines.append("按冻结配方穿搭；连体单品不得拆分；不得增减单品。")
    lines.extend(["", "【背景与场景】"])
    if pure_color:
        lines.append(
            f"背景必须是完整、均匀的纯色 {background_color}；无墙面纹理、地板、窗户、家具、"
            "街景、机场、咖啡店、道具或其他环境元素。允许人物脚下极轻微接触阴影，但不得形成地面。"
        )
        lines.append("场合只作为规划中的穿搭偏好，不得把它画成实际背景；动作只使用本镜头已规划的站姿。")
    else:
        lines.append(str(scene.get("prompt_core") or scene.get("name") or "明亮自然的生活场景"))
        if scene.get("selected_zone"):
            lines.append(
                f"本组固定拍摄区域：{scene['selected_zone']}；"
                "只改变人物在该区域内的机位和动作，不得切换到其他场景。"
            )
        if scene.get("prompt_negative"):
            lines.append(f"场景负向：{scene['prompt_negative']}")
    if (
        recipe_execution.get("content_goal") in {"outfit_breakdown", "multi_look"}
        and request.slot_index == int(recipe_execution.get("anchor_slot") or 2)
    ):
        lines.append(
            "本张同时是首图人物锚点源：必须正面完整全身、人物轮廓无遮挡、手脚不出画；"
            + (
                "人物在画面中占约85%-92%高度，头顶和鞋底留出呼吸空间；后续只允许从本图提取同一人物。"
                if pure_color
                else "保持当前已选场景，不得改成白墙或摄影棚。后续只允许从本图提取同一人物。"
            )
        )
    lines.extend(
        [
            "",
            "【本张画面意图】",
            f"槽位 {shot.get('slot_index')}（{shot.get('slot_role')}）：{shot.get('purpose', '')}",
            framing,
        ]
    )
    camera_hint = request.camera_hint or shot.get("camera_hint") or ""
    if camera_hint:
        lines.append(f"镜头提示：{camera_hint}")
    if composition:
        lines.append("构图合同（必须执行）：" + str(composition.get("instruction") or ""))
        lines.append(
            "构图标识："
            f"framing={composition.get('framing') or ''}；"
            f"camera={composition.get('camera_angle') or ''}；"
            f"pose={composition.get('pose') or ''}"
        )
        forbidden = list(composition.get("forbidden") or [])
        if forbidden:
            lines.append("本张明确禁止：" + "、".join(str(value) for value in forbidden))
        if composition.get("prop_policy") == "only_from_scene_or_purpose":
            lines.append("人物动作和道具只能来自当前场景、本镜头目的或镜头提示，不得自行添加任何无关道具。")
        elif composition.get("prop_policy") == "no_new_props":
            lines.append("本张不得新增任何道具。")
    if request.continuity_reference_images:
        if recipe_execution.get("transform_mode") == "controlled_outfit_change":
            lines.append(
                "连续性参考图只用于锁定同一人物、同一目标商品和光线；"
                "不得照抄参考图的补充穿搭，必须执行当前 outfit state。"
            )
        else:
            lines.append("连续性参考图用于锁定人物、商品、穿搭和光线。")
        if recipe_execution.get("content_goal") == "multi_look":
            lines.append("本系列的新鲜感来自当前页不同的穿搭，不靠夸张换机位；保持接近的全身占比、背景与腰位平视，只小幅自然换姿。")
        else:
            lines.append(
                "连续性参考图不得用于照抄上一张的景别、构图、站姿、手势或机位；"
                "本张必须执行自己的构图合同，形成肉眼可见的镜头差异。"
            )
    if shot.get("overlay_text"):
        lines.append(f"画面短句参考（不要把文字画进图片）：{shot['overlay_text']}")
    lines.extend(
        [
            "",
            "【画面风格】",
            (
                "纯色背景下的自然穿搭创作者写真；光线柔和均匀，保留真实皮肤、发丝和衣物纹理。"
                if pure_color
                else "普通创作者自己用手机竖屏拍摄的原生记录感；自然光、白平衡自然；保留真实皮肤、发丝和衣物纹理。"
            ),
            (
                "不要商品白底图、广告大片、电影灯光、磨皮塑料脸、夸张网红姿势或背景渐变。"
                if pure_color
                else "首先像真实生活记录，其次才是好看；不要棚拍、广告大片、电影灯光、磨皮塑料脸、夸张网红姿势。"
            ),
            "",
            "【通用负向要求】",
            "不要文字、字幕、水印、Logo 杜撰；不要尺寸标注；不要多余人物或多余肢体；不要畸形手指。",
            (
                "保持跨图一致：同一人物、同一商品、同一背景色与光线；"
                "穿搭按各镜头 outfit state 执行。"
                if pure_color and recipe_execution.get("transform_mode") == "controlled_outfit_change"
                else (
                    "保持跨图一致：同一人物、同一商品、同一背景色与光线、同一套穿搭。"
                    if pure_color
                    else (
                        "保持跨图一致：同一人物、同一商品、同一场景光线；穿搭按各镜头 outfit state 执行。"
                        if recipe_execution.get("transform_mode") == "controlled_outfit_change"
                        else "保持跨图一致：同一人物、同一商品、同一套穿搭、同一场景光线。"
                    )
                )
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
        item_refs = (request.outfit_state.get("item_reference_images")
                     or request.outfit_state.get("item_refs") or request.look_snapshot.get("item_refs") or {})
        item_entries = list(item_refs.values()) if isinstance(item_refs, dict) else list(item_refs or [])
        for source in (
            select_product_references_for_slot(
                request.product, request.slot_role
            ),
            request.persona_snapshot.get("local_reference_images", [])
            or request.persona_snapshot.get("reference_images", [])
            or [],
            item_entries,
            request.continuity_reference_images or [],
        ):
            for entry in source:
                if isinstance(entry, str) and Path(entry).exists():
                    paths.append(entry)
                elif isinstance(entry, dict):
                    candidate = str(entry.get("local_path") or entry.get("path") or entry.get("cutout_image_path") or "").strip()
                    if candidate and Path(candidate).exists():
                        paths.append(candidate)
        return list(dict.fromkeys(paths))


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
