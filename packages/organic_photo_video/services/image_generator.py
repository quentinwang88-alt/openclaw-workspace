"""Image generation adapter (Stage C) for OPV.

Wraps the existing ``skills/openai-image`` service (gpt-image-2, codex OAuth
path) so OPV never builds a second model-auth stack (MODEL_HANDOFF 4.3).

Prompt composition is deterministic: product identity lock + persona lock +
frozen look recipe + scene + per-slot intent. The generator never invents
product structure beyond the reference images, and every shot carries the
product/persona/look/scene refs from the plan for cross-shot consistency.
"""

from __future__ import annotations

import json
import os
import struct
import subprocess
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from services.persona_pack import select_identity_references
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


def human_presentation_contract_lines(policy: Dict[str, Any]) -> List[str]:
    """Executable human-photography contract text (positive phrasing only)."""
    head = dict(policy.get("head") or {})
    face = dict(policy.get("face") or {})
    body = dict(policy.get("body") or {})
    identity = dict(policy.get("identity") or {})
    candid = dict(policy.get("candid") or {})
    lines: List[str] = []
    if head.get("require_level_neck") or head.get("forbid_intentional_side_tilt"):
        lines.append("头颈保持自然直立，不向左右肩膀倾斜；肩线放松自然下压。")
    if body.get("require_believable_weight_distribution"):
        lines.append("动作必须有明确重心，承重腿、迈步腿、髋部和手臂协调受力。")
    if body.get("forbid_mannequin_arms"):
        lines.append("手臂有自然弯曲和动态，不紧贴身体像人台。")
    if face.get("require_real_skin_texture"):
        lines.append(
            "真实皮肤保留毛孔、细小纹理和自然不对称；"
            "避免玻璃眼、塑料皮肤和固定上扬的嘴角。"
        )
    if candid.get("low_camera_awareness"):
        lines.append(
            "像同行朋友用手机随手抓拍的瞬间，不是刻意摆拍：机位可轻微偏离正面"
            "（侧机位或略低角度），构图不必完全居中，允许轻微不对称。"
        )
    if candid.get("allow_environment_in_frame"):
        lines.append(
            "背景保留真实生活内容：行人、船只或街道元素可以自然入画（可轻微虚化），"
            "前景栏杆或植物可以形成自然遮挡；不要影棚式的干净空旷。"
        )
    if candid.get("allow_natural_imperfection") or candid.get("hair_wind_movement"):
        lines.append(
            "允许自然的不完美：碎发和发丝被风吹动、衣物自然褶皱和垂坠、"
            "表情有微小动态；不要过度整洁的目录感。"
        )
    if candid.get("polished_daily_styling"):
        lines.append("自然精致的日常穿搭照片：人物妆发整洁，发型经过简单整理，允许轻薄妆容。")
    if candid.get("relaxed_spirited_expression"):
        lines.append("表情放松，眼神有精神，可有轻微自然笑意；不僵硬、不刻意。")
    if candid.get("clear_face_exposure_soft_shadows"):
        lines.append(
            "面部曝光清楚、阴影柔和，光线符合现场环境；"
            "避免背光脸黑、灰暗面色或疲惫感。"
        )
    if candid.get("skin_fairness"):
        lines.append(
            "肤色在人物参考图基础上自然提亮为冷白透亮，像真实的好皮肤；"
            "保持毛孔和质感，不要苍白假白或滤镜白。"
        )
    if candid.get("eyes_keep_reference_size"):
        lines.append(
            "眼睛保持人物参考图的自然大小和形状（杏仁眼、不放大不改眼型），"
            "眼神放松，不刻意睁大。"
        )
    if identity.get("forbid_copying_reference_pose"):
        lines.append(
            "保持人物参考中的身份特征。当前画面的表情、视线与动作按场景自然展开，"
            "不照搬参考照片的头部倾斜和固定笑容。"
        )
    return lines


def color_grading_contract_lines(plan: Dict[str, Any], *, include_skin: bool,
                                 palette_hex: Any = None) -> List[str]:
    """Group-unified color contract so every image shares one grade."""
    lines = [
        f"全组统一色调：色温基准 {plan.get('temperature') or '自然'}；"
        f"饱和度 {plan.get('saturation') or '适中'}；对比度 {plan.get('contrast') or '柔和'}。",
    ]
    note = str(plan.get("tone_note_zh") or "")
    if note:
        lines.append(f"调色说明：{note}")
    if include_skin:
        lines.append(
            "人物肤色以人物参考图为唯一权威标准（"
            + str(plan.get("skin_tone_anchor") or "四张完全一致")
            + "）；禁止随场景光线改变肤色深浅和色相。"
        )
    palette = [str(value).upper() for value in (palette_hex or [])
               if str(value).startswith("#")]
    if palette:
        lines.append("本套穿搭主色（画面配色须与这些主色呼应）：" + "、".join(palette) + "。")
    return lines


def pose_contract_text(pose_contract: Dict[str, Any]) -> str:
    """Render a structured pose contract as executable Chinese direction.

    Pseudo-precise camera numbers were removed 2026-09-07 (unexecutable by
    image models); only intent-level direction survives.
    """
    parts = []
    if pose_contract.get("pose_family"):
        parts.append(f"动作族 {pose_contract['pose_family']}")
    for key, label in (("action_zh", "动作"), ("gesture_detail_zh", "手势细节"),
                       ("gaze_zh", "视线"), ("head_zh", "头位"), ("body_zh", "身体"),
                       ("camera_zh", "机位")):
        if pose_contract.get(key):
            parts.append(f"{label}：{pose_contract[key]}")
    return "；".join(parts) + "。"


def reference_usage_contract_lines(request: Any) -> List[str]:
    """Describe the final reference order after the same de-dup used by generation."""
    roles = dict(request.reference_roles or {})
    if not roles:
        return []

    def local_paths(values: Any) -> List[str]:
        output = []
        for entry in values or []:
            raw = (entry if isinstance(entry, str) else
                   entry.get("local_path") or entry.get("path") if isinstance(entry, dict)
                   else "")
            if raw and Path(str(raw)).is_file():
                output.append(str(Path(str(raw))))
        return output

    item_refs = (request.outfit_state.get("item_reference_images")
                 or request.outfit_state.get("item_refs")
                 or request.look_snapshot.get("item_refs") or {})
    item_entries = list(item_refs.values()) if isinstance(item_refs, dict) else list(item_refs or [])
    persona_entries = (
        select_identity_references(request.persona_snapshot)
        if request.persona_snapshot.get("reference_items")
        else request.persona_snapshot.get("local_reference_images", [])
        or request.persona_snapshot.get("reference_images", []) or []
    )
    ordered = []
    for group in (
        select_product_references_for_slot(request.product, request.slot_role),
        persona_entries, item_entries, request.continuity_reference_images or [],
    ):
        for path in local_paths(group):
            if path not in ordered:
                ordered.append(path)

    role_sets = {
        key: set(local_paths(value if isinstance(value, list) else [value]))
        for key, value in roles.items()
    }
    labels = {
        "product_identity_images": "指定商品身份",
        "persona_identity_images": "人物身份",
        "environment_reference_images": "环境",
        "visual_style_reference_images": "画面风格",
        "outfit_reference_images": "穿搭比例/层次/配色关系",
        "style_reference_images": "风格参考",
    }
    lines = []
    for index, path in enumerate(ordered, 1):
        matched = [label for key, label in labels.items() if path in role_sets.get(key, set())]
        if not matched:
            continue
        lines.append(f"输入图片{index}用途：{'、'.join(dict.fromkeys(matched))}。")
    return lines


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
    presentation_type = str(presentation.get("presentation_type") or "MODEL_FULL_BODY")
    flat_lay = presentation_type == "FLAT_LAY"
    pure_color = str(presentation.get("background_mode") or "") == "solid_color"
    style_reference = str(recipe_execution.get("reference_mode") or "").upper() == "STYLE"
    background_color = str(presentation.get("background_color") or "#F6F5F2")

    product_label = {
        "outerwear": "目标外套",
        "dress": "目标连衣裙",
        "top": "目标上装",
        "bottom": "目标下装",
    }.get(str(product.get("category") or "").strip().lower(), "目标商品")
    opening = (
        "生成一张竖屏 9:16 的真实服装平铺搭配照片：正上方俯拍，像当地穿搭创作者自行整理拍摄的图文素材。"
        if flat_lay else
        "生成一张竖屏 9:16 的真实穿搭创作者照片：无缝纯色背景、自然时装感，"
        "不是电商商品图、证件照或刻板影棚照。"
        if pure_color
        else "生成一张竖屏 9:16 真实手机感穿搭照片（TikTok 自然流内容，不是电商棚拍）。"
    )
    lines: List[str] = [opening, ""]
    if style_reference:
        theme = dict(recipe_execution.get("theme_brief") or {})
        reference_profile = dict(presentation.get("reference_style_profile") or {})
        aggregate = dict(reference_profile.get("aggregate") or {})
        outfit_reference = dict(reference_profile.get("outfit_reference") or {})
        environment_reference = dict(reference_profile.get("environment_reference") or {})
        visual_style_reference = dict(reference_profile.get("visual_style_reference") or {})
        lines.extend([
            "【风格参考合同】",
            "每张参考图只按已标注用途工作：穿搭图提供版型比例、层次、配色关系和穿法；环境图提供场所与背景；画面风格图提供光线、色调和构图。",
            "穿搭参考允许借鉴完整搭配关系但不要求同款；不得复制人物身份、品牌 Logo、文字或截图界面。环境图中的服装不得控制穿搭，穿搭图的背景不得控制场景。",
            f"本篇主题：{theme.get('label_zh') or '穿搭灵感'}；{theme.get('visual_brief') or ''}",
            f"环境参考：{json.dumps(environment_reference, ensure_ascii=False)}",
            f"穿搭参考：{json.dumps(outfit_reference, ensure_ascii=False)}",
            f"画面风格参考：{json.dumps(visual_style_reference, ensure_ascii=False)}",
            "当前页执行冻结穿搭；四页保持可比较的区别，不为凑差异拆散协调搭配。",
            "",
            "【平铺展示合同】" if flat_lay else "【人物身份锁】",
        ])
        if product:
            lines.extend([
                "",
                "【指定商品身份锁】",
                f"目标商品：{product_label}。商品参考图是该商品颜色、图案、材质观感、形状和结构的最高视觉事实。",
                "穿搭灵感只能调整配套单品、搭配比例和穿法，不能替换或重新设计指定商品。",
                "忽略商品图中的模特、姿态、滤镜与背景。",
            ])
        usage_lines = reference_usage_contract_lines(request)
        if usage_lines:
            lines.extend(["", "【输入图片用途】", *usage_lines])
    else:
        lines.extend([
            "【商品身份锁】",
            f"目标商品：{product_label}。商品参考图是颜色、图案、材质观感、形状和结构的最高视觉事实。",
            "商品名称、Look 名称、穿搭模板或其文案不得覆盖商品参考图中的颜色和材质。",
            "保持参考图中的颜色、版型、衣长、领型、前襟和袖口结构；禁止替换成相似款或重新设计。",
            "忽略商品参考图中的模特、脸、妆发、姿态、滤镜与背景。",
            "",
            "【人物身份锁】",
        ])
    facts = product_facts.get("facts") or {}
    known_facts = [
        f"{key}={value}"
        for key, value in facts.items()
        if value not in (None, "", "unknown")
    ]
    if known_facts:
        lines.append("已确认产品事实：" + "；".join(known_facts))
    if flat_lay:
        lines.extend([
            "画面中绝对不能出现人物、人脸、手、脚、人体局部、人台、衣架或自拍设备。",
            "每件衣物自然平铺并组成一套能看懂的完整搭配；保持真实布料褶皱、厚度和接触阴影。",
        ])
    else:
        persona_desc = str(persona.get("prompt_core") or persona.get("name") or "年轻女性")
        lines.append(f"人物模板：{persona.get('name') or 'persona'}；{persona_desc}")
        lines.append("自然肤色与真实皮肤纹理；自然淡妆；面部不要碎发遮眼。")
    if pure_color and not flat_lay:
        lines.append(
            "比例："
            + str(presentation.get("proportion_direction") or "自然修长时装比例，约7.6-8头身；不得机械拉伸")
            + "。相机保持足够距离，避免广角夸大脚部或俯拍压缩腿部。"
        )
    elif not flat_lay:
        lines.append("比例：自然成年女性比例，头身比约 1:7.2；不要放大头部，不要缩短躯干或四肢。")
    if not flat_lay:
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
    color_plan = request.recipe_execution.get("color_grading_plan") or {}
    if color_plan:
        lines.extend(["", "【全局色彩合同】"])
        lines.extend(color_grading_contract_lines(
            color_plan, include_skin=not flat_lay,
            palette_hex=(request.look_snapshot.get("recipe") or {}).get("palette_hex"),
        ))
    human_contract = request.recipe_execution.get("human_presentation_contract") or {}
    if human_contract and not flat_lay:
        lines.extend(["", "【人物摄影合同】"])
        lines.extend(human_presentation_contract_lines(human_contract))
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
            ("outerwear", "外套"),
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
    if style_reference:
        lines.append(
            "严格执行本页冻结穿搭；借鉴选中穿搭参考的比例、层次、配色关系和穿法，"
            "不要求同款。" + ("指定商品必须保持不变。" if product else "")
        )
    elif recipe_execution.get("transform_mode") == "controlled_outfit_change":
        allowed = outfit_state.get("change_permissions") or []
        lines.append(
            "目标商品本身必须完全不变；补充单品只按当前 outfit state 执行。"
            + (f"本状态额外允许的穿法调整：{','.join(allowed)}。" if allowed else
               "不允许当前冻结搭配之外的自由变化；当前搭配与连续性参考不同时，按本镜搭配替换配套单品。")
        )
    else:
        lines.append("按冻结配方穿搭；连体单品不得拆分；不得增减单品。")
    lines.extend(["", "【背景与场景】"])
    if flat_lay:
        lines.append(
            "沿用参考图的桌面或墙面质感、暖色温、定向光和自然阴影；背景保持干净，"
            "不要摄影棚无缝纸、纯色抠图、电商白底或多余装饰。"
        )
    elif pure_color:
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
        pose_contract = composition.get("pose_contract") or {}
        if pose_contract:
            lines.append("动作合同：" + pose_contract_text(pose_contract))
        forbidden = list(composition.get("forbidden") or [])
        if forbidden:
            lines.append("本张明确禁止：" + "、".join(str(value) for value in forbidden))
        if composition.get("prop_policy") == "only_from_scene_or_purpose":
            lines.append("人物动作和道具只能来自当前场景、本镜头目的或镜头提示，不得自行添加任何无关道具。")
        elif composition.get("prop_policy") == "no_new_props":
            lines.append("本张不得新增任何道具。")
    if request.continuity_reference_images:
        if style_reference:
            lines.append(
                "连续性输入用于保持参考图的平铺构图、背景质感、色温和光线；不得出现人物，"
                "也不得复制参考图的品牌或具体单品。"
                if flat_lay else
                "连续性输入同时包含原始风格参考和可能存在的首张生成人物锚点；"
                "保持同一人物、同一类场景语义和光线，但不得复制上一页服装或姿势。"
            )
        elif recipe_execution.get("transform_mode") == "controlled_outfit_change":
            lines.append(
                "连续性参考图只用于锁定同一人物、同一目标商品和光线；"
                "不得照抄参考图的补充穿搭，必须执行当前 outfit state。"
            )
        else:
            lines.append("连续性参考图用于锁定人物、商品、穿搭和光线。")
        if recipe_execution.get("content_goal") == "multi_look" and not flat_lay:
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
                "真实服装平铺摄影；参考图式暖色自然光和有触感的背景，保留布料纹理与自然阴影。"
                if flat_lay else
                "纯色背景下的自然穿搭创作者写真；光线柔和均匀，保留真实皮肤、发丝和衣物纹理。"
                if pure_color
                else "普通创作者自己用手机竖屏拍摄的原生记录感；自然光、白平衡自然；保留真实皮肤、发丝和衣物纹理。"
            ),
            (
                "不要人物、电商白底、悬浮商品、广告大片、过度整齐的目录排版或虚假 Logo。"
                if flat_lay else
                "不要商品白底图、广告大片、电影灯光、磨皮塑料脸、夸张网红姿势或背景渐变。"
                if pure_color
                else "首先像真实生活记录，其次才是好看；不要棚拍、广告大片、电影灯光、磨皮塑料脸、夸张网红姿势。"
            ),
            "",
            "【通用负向要求】",
            "不要文字、字幕、水印、Logo 杜撰；不要尺寸标注；不要多余人物或多余肢体；不要畸形手指。",
            (
                "保持跨图一致：同一平铺构图、背景质感、色温和光线；每页严格执行不同的冻结穿搭。"
                if style_reference and flat_lay
                else "保持跨图一致：同一人物、同一类场景语义与光线；每页严格执行不同的冻结穿搭。"
                if style_reference
                else (
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
        # Role-typed persona pack items come first (face evidence, then body);
        # legacy snapshots fall back to their untyped reference lists.
        persona_entries: List[Any] = (
            select_identity_references(request.persona_snapshot)
            if request.persona_snapshot.get("reference_items")
            else request.persona_snapshot.get("local_reference_images", [])
            or request.persona_snapshot.get("reference_images", [])
            or []
        )
        for source in (
            select_product_references_for_slot(
                request.product, request.slot_role
            ),
            persona_entries,
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


CREATOK_PROVIDER_NAME = "creatok"
CREATOK_DEFAULT_MODEL = "gpt-image-2-official"
CREATOK_DEFAULT_RESOLUTION = "1K"
CREATOK_DEFAULT_QUALITY = "low"
CREATOK_DEFAULT_ASPECT_RATIO = "9:16"
CREATOK_DEFAULT_POLL_TIMEOUT = 420
CREATOK_MAX_REFERENCE_IMAGES = 16


def _env(key: str, default: str) -> str:
    value = str(os.environ.get(key) or "").strip()
    return value or default


class CreatokImageGenerator:
    """Adapter over the ``creatok`` CLI (thin client, auth via CREATOK_API_KEY).

    The CLI submits, polls, and writes ``result.json`` under ``--out``; the
    generated image itself must be downloaded from the returned URL. Any
    failure surfaces as ``GenerationOutcome(ok=False)`` so the fallback
    channel can take over — this class never raises across the boundary.
    """

    def __init__(
        self,
        model: str = "",
        resolution: str = "",
        quality: Optional[str] = None,
        aspect_ratio: str = "",
        binary: str = "",
        poll_timeout: int = 0,
    ):
        self.model = model or _env("OPV_CREATOK_IMAGE_MODEL", CREATOK_DEFAULT_MODEL)
        self.resolution = resolution or _env(
            "OPV_CREATOK_IMAGE_RESOLUTION", CREATOK_DEFAULT_RESOLUTION
        )
        # Empty env value means "omit quality" (models without quality tiers).
        self.quality = (
            quality if quality is not None
            else os.environ.get("OPV_CREATOK_IMAGE_QUALITY", CREATOK_DEFAULT_QUALITY)
        )
        self.aspect_ratio = aspect_ratio or _env(
            "OPV_CREATOK_IMAGE_ASPECT_RATIO", CREATOK_DEFAULT_ASPECT_RATIO
        )
        self.binary = binary or _env("OPV_CREATOK_BIN", "creatok")
        self.poll_timeout = poll_timeout or int(
            _env("OPV_CREATOK_POLL_TIMEOUT", str(CREATOK_DEFAULT_POLL_TIMEOUT))
        )

    def _options(self) -> Dict[str, Any]:
        options: Dict[str, Any] = {
            "model": self.model,
            "resolution": self.resolution,
            "n": 1,
            "aspect_ratio": self.aspect_ratio,
        }
        if self.quality:
            options["quality"] = self.quality
        return options

    def generate_shot(self, request: ShotGenerationRequest) -> GenerationOutcome:
        stem = f"{request.task_id}_P{request.slot_index}_v{request.shot_version}"
        outcome_id = f"creatok:{stem}"
        if not os.environ.get("CREATOK_API_KEY"):
            return GenerationOutcome(
                ok=False, provider=CREATOK_PROVIDER_NAME, model=self.model,
                request_id=outcome_id, error="CREATOK_API_KEY not configured",
            )
        output_dir = Path(request.output_dir)
        run_dir = output_dir / f"creatok_{stem}"
        try:
            run_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            return GenerationOutcome(
                ok=False, provider=CREATOK_PROVIDER_NAME, model=self.model,
                request_id=outcome_id, error=f"output dir unavailable: {exc}",
            )
        reference_paths = OpenAIImageGenerator._reference_paths(request)
        command = [
            self.binary, "image", "generate",
            "--prompt", compose_shot_prompt(request),
            "--options", json.dumps(self._options()),
            "--out", str(run_dir),
            "--timeout", str(self.poll_timeout),
        ]
        if reference_paths:
            command.extend([
                "--ref", ",".join(reference_paths[:CREATOK_MAX_REFERENCE_IMAGES]),
            ])
        try:
            completed = self._spawn_with_retry(command)
        except (subprocess.TimeoutExpired, OSError) as exc:
            return GenerationOutcome(
                ok=False, provider=CREATOK_PROVIDER_NAME, model=self.model,
                request_id=outcome_id, error=f"creatok cli failed: {exc}",
            )
        envelope = self._parse_envelope(completed.stdout)
        (run_dir / "envelope.json").write_text(
            json.dumps(envelope, ensure_ascii=False, indent=1) or completed.stdout,
            encoding="utf-8",
        )
        if not envelope.get("ok"):
            error = (envelope.get("error") or {})
            return GenerationOutcome(
                ok=False, provider=CREATOK_PROVIDER_NAME, model=self.model,
                request_id=str(envelope.get("task_id") or outcome_id),
                error=(
                    f"creatok {error.get('kind', 'unknown')}: "
                    f"{error.get('message') or completed.stderr[-400:]}"
                ),
                raw={"envelope": envelope},
            )
        images = list(
            (((envelope.get("data") or {}).get("result") or {}).get("images") or [])
        )
        if not images or not images[0].get("url"):
            return GenerationOutcome(
                ok=False, provider=CREATOK_PROVIDER_NAME, model=self.model,
                request_id=str(envelope.get("task_id") or outcome_id),
                error="creatok envelope has no image url",
                raw={"envelope": envelope},
            )
        image_path = self._download(
            str(images[0]["url"]), output_dir / f"{stem}{self._suffix(images[0]['url'])}"
        )
        if not image_path:
            return GenerationOutcome(
                ok=False, provider=CREATOK_PROVIDER_NAME, model=self.model,
                request_id=str(envelope.get("task_id") or outcome_id),
                error="creatok image download failed",
                raw={"envelope": envelope},
            )
        dimensions = read_image_dimensions(str(image_path))
        size_ok = check_portrait_916(dimensions)
        return GenerationOutcome(
            ok=size_ok,
            image_path=str(image_path),
            provider=CREATOK_PROVIDER_NAME,
            model=self.model,
            request_id=str(envelope.get("task_id") or outcome_id),
            width=dimensions[0] if dimensions else None,
            height=dimensions[1] if dimensions else None,
            error="" if size_ok else f"image is not 9:16 portrait: {dimensions}",
            raw={"envelope": envelope, "reference_count": len(reference_paths)},
        )

    def _spawn_with_retry(self, command: List[str]):
        """Spawn the CLI; a missing binary is retried — npm global updates
        briefly swap the symlink target (twice seen in production)."""
        import time

        last_error: Optional[BaseException] = None
        for attempt in range(3):
            try:
                return subprocess.run(
                    command, capture_output=True, text=True,
                    timeout=self.poll_timeout + 90,
                )
            except FileNotFoundError as exc:
                last_error = exc
                time.sleep(3)
        raise last_error

    @staticmethod
    def _parse_envelope(stdout: str) -> Dict[str, Any]:
        try:
            envelope = json.loads(stdout)
            return envelope if isinstance(envelope, dict) else {"ok": False}
        except json.JSONDecodeError:
            return {"ok": False, "error": {"kind": "invalid", "message": "non-JSON cli output"}}

    @staticmethod
    def _suffix(url: str) -> str:
        suffix = Path(url.split("?", 1)[0]).suffix.lower()
        return suffix if suffix in {".png", ".jpg", ".jpeg", ".webp"} else ".png"

    @staticmethod
    def _download(url: str, target: Path) -> Optional[Path]:
        try:
            with urllib.request.urlopen(url, timeout=120) as response:  # noqa: S310
                target.write_bytes(response.read())
            return target
        except OSError:
            return None


class FallbackShotGenerator:
    """Try the primary channel first; on any failure hand off to fallback."""

    def __init__(self, primary, fallback):
        self.primary = primary
        self.fallback = fallback

    def generate_shot(self, request: ShotGenerationRequest) -> GenerationOutcome:
        primary_error = ""
        try:
            outcome = self.primary.generate_shot(request)
            if outcome is not None and outcome.ok:
                outcome.raw = {**(outcome.raw or {}), "channel": "primary"}
                return outcome
            primary_error = outcome.error if outcome is not None else "no outcome"
        except Exception as exc:  # noqa: BLE001 - channel boundary
            primary_error = f"{type(exc).__name__}: {exc}"
        try:
            outcome = self.fallback.generate_shot(request)
        except Exception as exc:  # noqa: BLE001 - channel boundary
            return GenerationOutcome(
                ok=False, provider="fallback", model="",
                error=f"primary: {primary_error}; fallback: {type(exc).__name__}: {exc}",
            )
        if outcome is None:
            return GenerationOutcome(
                ok=False, provider="fallback", model="",
                error=f"primary: {primary_error}; fallback returned nothing",
            )
        if not outcome.ok:
            outcome.error = f"primary: {primary_error}; fallback: {outcome.error}"
        outcome.raw = {**(outcome.raw or {}), "channel": "fallback", "primary_error": primary_error}
        return outcome


def build_default_photo_generator():
    """Production default: CreatOK primary, codex openai-image fallback.

    ``OPV_PHOTO_CHANNEL`` overrides: ``openai-image`` pins the legacy codex
    channel, ``creatok`` drops the fallback. Defaults to ``creatok_fallback``.
    """
    channel = _env("OPV_PHOTO_CHANNEL", "creatok_fallback").lower()
    if channel == "openai-image":
        return OpenAIImageGenerator()
    if channel == "creatok":
        return CreatokImageGenerator()
    return FallbackShotGenerator(CreatokImageGenerator(), OpenAIImageGenerator())
