from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Iterable

from .utils import normalized_list, stable_hash

if TYPE_CHECKING:
    from .database import LightTryonDB


SUPPLEMENT_PROMPT_VERSION = "supplement-shot-v5-target-framing"
MAX_GENERATED_SHOTS_PER_VARIANT = 3
CONTEXT_DEPENDENT_PHRASES = ("与主场景", "主场景相同", "同一套", "同一个女生", "原展示区")


SHOT_TEMPLATES: dict[str, dict[str, Any]] = {
    "main_wear_upper": {
        "duration_seconds": 8,
        "framing": "从头部拍到大腿上段，上衣完整可见，下装只露出腰头、胯部和一小段裤腿",
        "action_timeline": "0-1秒稳定正面站立；1-4秒身体缓慢转动约15度；4-6秒自然回到接近正面；6-8秒稳定定格",
        "focus": "上衣领口、肩线、袖型、门襟、衣身余量和完整下摆",
        "fallback_strategy": "crop_existing_main_clip",
    },
    "fit_turn": {
        "duration_seconds": 6,
        "framing": "上半身至大腿上段构图，上衣是画面主体，下装只露出一截",
        "action_timeline": "0-1秒稳定正面站立；1-4秒缓慢转向一侧约15度并停留；4-6秒回到接近正面",
        "focus": "肩线、袖型、侧面余量和上身比例",
        "fallback_strategy": "crop_existing_main_clip",
    },
    "detail_sleeve": {
        "duration_seconds": 6,
        "framing": "肩部至胯部中近景，袖口和衣身占据主要区域",
        "action_timeline": "0-1秒双手自然停留；1-4秒一只手沿另一侧袖口边缘轻触一次；4-6秒自然放回并稳定停顿",
        "focus": "袖型、袖口宽度、面料颜色和真实表面纹理",
        "fallback_strategy": "use_verified_product_still",
    },
    "detail_neckline": {
        "duration_seconds": 6,
        "framing": "头部或肩部至腰胯的上半身中近景，领口、门襟和内搭露出区域清楚",
        "action_timeline": "0-1秒稳定站立；1-4秒空闲手轻轻整理一次领口边缘；4-6秒手自然放下并定格",
        "focus": "领口、门襟、扣合状态、内搭类型和内搭露出面积",
        "fallback_strategy": "use_verified_product_still",
    },
    "detail_closure": {
        "duration_seconds": 5,
        "framing": "肩部至腰部中近景，正面门襟、拉链、按扣或口袋结构占画面主要区域",
        "action_timeline": "0-1秒稳定正面；1-3.5秒空闲手沿门襟走势轻触一次，不改变拉链和按扣的开合状态；3.5-5秒停止动作并保持结构清楚",
        "focus": "正面拉链、按扣、门襟或口袋等已经由商品图确认的开合结构",
        "fallback_strategy": "use_verified_product_still",
    },
    "detail_waistline": {
        "duration_seconds": 5,
        "framing": "胸部至大腿上段构图，完整露出衣摆、裤腰和一小段裤腿",
        "action_timeline": "0-1秒稳定正面；1-3秒身体轻转约10度，让衣摆和裤腰比例清楚；3-5秒回到正面并停顿",
        "focus": "上衣实际衣长、衣摆位置以及与裤腰形成的穿搭比例",
        "fallback_strategy": "crop_existing_main_clip",
    },
    "color_upper": {
        "duration_seconds": 5,
        "framing": "肩部至大腿上段构图，上衣在均匀浅暖白光线下占据画面主体",
        "action_timeline": "0-1秒稳定；1-3秒缓慢转动约10度让正面和轻侧面依次受光；3-5秒稳定停顿",
        "focus": "参考图已经确认的商品颜色，以及亮而不偏黄的真实受光效果",
        "fallback_strategy": "use_verified_product_still",
    },
    "detail_fabric": {
        "duration_seconds": 6,
        "framing": "肩部至胯部中近景，商品面料保持清晰，不使用微距畸变",
        "action_timeline": "0-1秒稳定；1-4秒手掌轻轻抚过衣身表面一次；4-6秒停止动作并保持",
        "focus": "已经由商品图确认的面料纹理，不演示未经证实的弹力或功能",
        "fallback_strategy": "use_verified_product_still",
    },
    "wear_hold_color": {
        "duration_seconds": 8,
        "framing": "头部至大腿上段构图，身上主色上衣完整可见，手持副色位于身体一侧且不遮挡领口",
        "action_timeline": "0-1秒稳定站立；1-5秒将副色上衣轻轻抬到腰胸之间并停留；5-8秒保持稳定",
        "focus": "身上主色和手中副色款式相同、颜色不同，二者不得交换或融合",
        "fallback_strategy": "verified_color_still_carousel",
    },
    "scenario_pose": {
        "duration_seconds": 6,
        "framing": "头部至大腿上段，上半身商品清楚，下装只露出一部分",
        "action_timeline": "0-2秒自然站立；2-4秒空闲手轻轻整理衣摆一次；4-6秒以准备出门的自然状态定格",
        "focus": "真实日常穿着感，商品始终是画面主体",
        "fallback_strategy": "reuse_main_wear_hold",
    },
}


SHOT_ACTION_VARIANTS: dict[str, list[dict[str, str]]] = {
    "main_wear_upper": [
        {"variant_id": "front_to_quarter", "action_timeline": "0-1秒正面稳定；1-4秒缓慢转向约15度；4-6秒回正；6-8秒定格"},
        {"variant_id": "hem_adjust", "action_timeline": "0-1秒正面稳定；1-4秒空闲手整理一次衣摆；4-6秒手自然放下；6-8秒定格"},
        {"variant_id": "half_step", "action_timeline": "0-1秒稳定；1-3秒向前移动半步；3-5秒停留展示；5-8秒自然回位并定格"},
    ],
    "fit_turn": [
        {"variant_id": "left_quarter", "action_timeline": "0-1秒正面稳定；1-4秒缓慢向左转约20度并停留；4-6秒回到正面；6-8秒定格"},
        {"variant_id": "right_quarter", "action_timeline": "0-1秒正面稳定；1-4秒缓慢向右转约20度并停留；4-6秒回到正面；6-8秒定格"},
        {"variant_id": "side_hold", "action_timeline": "0-1秒正面稳定；1-3秒转向轻侧面；3-6秒保持侧面展示肩线和衣身余量；6-8秒自然回正"},
    ],
    "scenario_pose": [
        {"variant_id": "ready_to_go", "action_timeline": "0-2秒自然站立；2-4秒整理一次衣摆；4-8秒以准备出门的状态定格"},
        {"variant_id": "phone_check", "action_timeline": "0-2秒稳定站立；2-4秒拿手机的手轻微调整高度但仍完整遮脸；4-8秒自然定格"},
        {"variant_id": "weight_shift", "action_timeline": "0-2秒正面站立；2-5秒重心自然移向一侧并轻微侧身；5-8秒保持生活化姿态"},
    ],
    "detail_closure": [
        {"variant_id": "trace_closure", "action_timeline": "0-1秒正面稳定；1-4秒空闲手沿门襟结构轻触一次；4-8秒放下手并定格"},
        {"variant_id": "closure_hold", "action_timeline": "0-2秒正面稳定；2-5秒手停在拉链或按扣附近但不改变开合；5-8秒手自然离开并定格"},
    ],
    "detail_fabric": [
        {"variant_id": "single_sweep", "action_timeline": "0-1秒稳定；1-4秒手掌轻抚衣身表面一次；4-8秒停止动作并定格"},
        {"variant_id": "side_light_turn", "action_timeline": "0-2秒稳定；2-5秒身体轻转约10度让斜侧光扫过面料；5-8秒保持受光状态"},
    ],
    "detail_neckline": [
        {"variant_id": "collar_touch", "action_timeline": "0-1秒稳定；1-4秒空闲手整理一次领口边缘；4-8秒放下手并定格"},
        {"variant_id": "collar_quarter", "action_timeline": "0-2秒正面稳定；2-5秒轻转约10度展示领口侧面轮廓；5-8秒回正并定格"},
    ],
    "detail_waistline": [
        {"variant_id": "waist_quarter", "action_timeline": "0-1秒正面稳定；1-4秒轻转约10度展示衣摆与裤腰比例；4-8秒回正并定格"},
        {"variant_id": "hem_release", "action_timeline": "0-2秒稳定；2-4秒手指轻放衣摆边缘后立即松开；4-8秒保持衣摆自然垂落"},
    ],
    "detail_sleeve": [
        {"variant_id": "cuff_touch", "action_timeline": "0-1秒稳定；1-4秒一只手轻触另一侧袖口一次；4-8秒手自然放下并定格"},
        {"variant_id": "sleeve_extend", "action_timeline": "0-2秒双手自然下垂；2-5秒一侧手臂轻微向外展开展示袖型；5-8秒恢复自然位置"},
    ],
}


ROLE_CAPTURE_GATES: dict[str, str] = {
    "main_wear_upper": (
        "上衣从领口到完整下摆必须连续清晰可见至少5秒，并占画面高度60%以上；"
        "手和手机不得遮挡肩线、门襟或下摆"
    ),
    "detail_neckline": (
        "使用颈部至胸口的局部近景，领口与上段门襟合计占画面40%以上；"
        "该区域必须连续清晰展示至少5秒，脸和手机不得进入主体区域"
    ),
    "detail_waistline": (
        "使用胸口至大腿中段的局部近景，完整衣摆与裤腰位于画面中央并占画面40%以上；"
        "衣摆动作完成后必须连续稳定展示至少5秒"
    ),
}


ROLE_SELLING_POINT_KEYWORDS: dict[str, tuple[str, ...]] = {
    "detail_closure": ("拉链", "按扣", "门襟", "口袋", "铆钉"),
    "detail_neckline": ("领口", "领型", "立领", "按扣"),
    "detail_waistline": ("短款", "衣长", "腰线", "衣摆", "下摆", "比例"),
    "detail_fabric": ("面料", "纹理", "哑光", "皮质", "质感"),
    "detail_sleeve": ("袖型", "袖口", "袖子", "铆钉"),
    "fit_turn": ("版型", "剪裁", "宽松", "肩线", "衣长", "比例"),
    "scenario_pose": ("百搭", "休闲", "简约", "风格", "内搭"),
    "main_wear_upper": ("版型", "衣长", "领口", "袖型", "面料"),
    "color_upper": ("颜色", "色调", "受光"),
    "wear_hold_color": ("颜色", "色调"),
}


def _trim_sentence(value: Any) -> str:
    return str(value or "").strip().rstrip("。；; ")


def _focused_selling_points(role: str, selling_points: list[str], limit: int = 4) -> list[str]:
    points = [_trim_sentence(item) for item in selling_points if _trim_sentence(item)]
    keywords = ROLE_SELLING_POINT_KEYWORDS.get(role, ())
    selected = [point for point in points if any(keyword in point for keyword in keywords)]
    color = next((point for point in points if any(keyword in point for keyword in ("颜色", "色调", "米杏", "纯白"))), "")
    if color and color not in selected:
        selected.insert(0, color)
    if not selected:
        selected = points[:limit]
    return selected[:limit]


def _timeline_for_duration(template: dict[str, Any], duration_seconds: int) -> str:
    timeline = _trim_sentence(template.get("action_timeline"))
    base_duration = int(template.get("duration_seconds") or duration_seconds)
    if duration_seconds > base_duration:
        timeline += (
            f"；{base_duration}-{duration_seconds}秒保持最终稳定姿态，"
            "人物、商品和手部不漂移，不重复动作"
        )
    return timeline


def _resolved_innerwear(visual: dict[str, Any], *, has_visual_reference: bool) -> str:
    inner_type = _trim_sentence(visual.get("resolved_inner_type"))
    inner_color = _trim_sentence(visual.get("resolved_inner_color"))
    if has_visual_reference and (not inner_type or "或" in inner_type or "/" in inner_type):
        return "内搭类型、颜色、领口和露出面积严格跟随产品穿搭图，不得随机切换。"
    if inner_type or inner_color:
        return f"内搭锁定为{inner_color or '参考图颜色'}{inner_type or '简洁内搭'}，领口和露出面积保持不变。"
    return "内搭严格跟随产品穿搭图，不得自行新增或替换。" if has_visual_reference else ""


def _visual_locked_styling(value: str, *, has_visual_reference: bool) -> str:
    if not has_visual_reference:
        return value
    result = re.sub(r"颜色选[^；。]+之一", "颜色严格跟随第1张产品穿搭图", value)
    return _trim_sentence(result)


ROLE_DEFAULTS = {
    "hook": ["main_wear_upper"],
    "fit": ["fit_turn"],
    "proof": ["main_wear_upper"],
    "detail": ["detail_sleeve"],
    "color": ["wear_hold_color"],
    "scenario": ["scenario_pose"],
    "decision": ["main_wear_upper"],
    "cta": ["main_wear_upper"],
}

RICHNESS_ROLE_SEQUENCE = (
    "detail_closure",
    "detail_neckline",
    "detail_waistline",
    "detail_fabric",
    "detail_sleeve",
    "fit_turn",
    "scenario_pose",
)


def asset_shot_roles(asset: dict[str, Any]) -> set[str]:
    tags = asset.get("observed_tags") if isinstance(asset.get("observed_tags"), dict) else {}
    roles = set(normalized_list(tags.get("shot_roles") or tags.get("supported_roles")))
    roles.update(normalized_list(tags.get("secondary_roles")))
    primary = str(tags.get("primary_shot_role") or "").strip()
    if primary:
        roles.add(primary)
    for segment in tags.get("segments") or []:
        if not isinstance(segment, dict):
            continue
        roles.update(segment_shot_roles(segment))
    return roles


def segment_shot_roles(segment: dict[str, Any]) -> set[str]:
    """Resolve only the roles supported by one observed segment."""

    primary = str(segment.get("primary_shot_role") or "").strip()
    roles = {primary, *normalized_list(segment.get("secondary_roles"))}
    if primary in {"hero", "result", "ending"}:
        roles.add("main_wear_upper")
    reason = str(segment.get("reason") or "")
    if primary == "detail" and any(term in reason for term in ("拉链", "按扣", "门襟", "口袋")):
        roles.add("detail_closure")
    if primary == "detail" and any(term in reason for term in ("领口", "立领", "领型")):
        roles.add("detail_neckline")
    if any(term in reason for term in ("短款", "衣摆", "腰线", "裤腰")):
        roles.update({"detail_waistline", "fit_turn"})
    if any(term in reason for term in ("颜色", "米白", "米杏", "色调")):
        roles.add("color_upper")
    return {role for role in roles if role}


def find_missing_shots(beats: list[dict[str, Any]], assets: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    available: set[str] = set()
    for asset in assets:
        if str(asset.get("asset_status") or "") not in {"ready", "usable", "tagged"}:
            continue
        available.update(asset_shot_roles(asset))
    missing: list[dict[str, Any]] = []
    for index, beat in enumerate(beats, start=1):
        required = normalized_list(beat.get("required_shot_roles"))
        if not required:
            required = ROLE_DEFAULTS.get(str(beat.get("role") or "proof"), ["main_wear_upper"])
        if available.intersection(required):
            continue
        missing.append({
            "beat_id": str(beat.get("beat_id") or f"B{index}"),
            "beat_role": str(beat.get("role") or "proof"),
            "speech_text": str(beat.get("speech_text") or ""),
            "shot_role": required[0],
            "priority": str(beat.get("priority") or "required"),
            "required_evidence": normalized_list(beat.get("required_evidence")),
        })
    return missing


def plan_supplement_shots(
    variant_id: str,
    beats: list[dict[str, Any]],
    assets: Iterable[dict[str, Any]],
    *,
    reference_assets: Iterable[str],
    max_generated_shots: int = MAX_GENERATED_SHOTS_PER_VARIANT,
) -> list[dict[str, Any]]:
    missing = find_missing_shots(beats, assets)
    required = [item for item in missing if item["priority"] == "required"]
    optional = [item for item in missing if item["priority"] != "required"]
    selected = (required + optional)[: max(0, int(max_generated_shots))]
    result: list[dict[str, Any]] = []
    for item in selected:
        role = item["shot_role"] if item["shot_role"] in SHOT_TEMPLATES else "main_wear_upper"
        template = SHOT_TEMPLATES[role]
        shot_id = "SUP_" + stable_hash(variant_id, item["beat_id"], role, length=18)
        result.append({
            "shot_id": shot_id,
            "variant_id": variant_id,
            "beat_id": item["beat_id"],
            "shot_role": role,
            "duration_seconds": template["duration_seconds"],
            "priority": item["priority"],
            "status": "planned",
            "reference_assets": normalized_list(reference_assets),
            "expected_tags": {
                "shot_roles": [role],
                "beat_role": item["beat_role"],
                "required_evidence": item["required_evidence"],
                "speech_text": item["speech_text"],
            },
            "fallback_strategy": template["fallback_strategy"],
            "max_attempts": 2,
        })
    return result


def compile_supplement_prompt(shot: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    role = str(shot.get("shot_role") or "")
    if role not in SHOT_TEMPLATES:
        raise ValueError(f"不支持的补充镜头角色: {role}")
    template = SHOT_TEMPLATES[role]
    expected_tags = shot.get("expected_tags") if isinstance(shot.get("expected_tags"), dict) else {}
    action_variant_id = str(expected_tags.get("action_variant") or "").strip()
    action_variant = next(
        (row for row in SHOT_ACTION_VARIANTS.get(role, []) if row.get("variant_id") == action_variant_id),
        {},
    )
    product = context.get("product") or {}
    scene = context.get("scene") or {}
    persona = context.get("persona") or {}
    styling = context.get("styling") or {}
    visual = context.get("visual_plan") or {}
    scene_text = _trim_sentence(context.get("scene_description") or scene.get("prompt_core"))
    if not scene_text:
        raise ValueError("补充镜头缺少完整场景描述")
    if any(phrase in scene_text for phrase in CONTEXT_DEPENDENT_PHRASES):
        raise ValueError("补充镜头场景描述含跨任务指代，必须改成完整独立描述")
    persona_text = _trim_sentence(re.sub(r"^同一个", "一名", str(persona.get("prompt_core") or "").strip()))
    references = normalized_list(shot.get("reference_assets"))
    styling_text = _visual_locked_styling(
        _trim_sentence(context.get("styling_description") or styling.get("prompt_core")),
        has_visual_reference=bool(references),
    )
    inner = _resolved_innerwear(visual, has_visual_reference=bool(references))
    outerwear_state = str(visual.get("resolved_outerwear_state") or "").strip()
    if outerwear_state:
        inner += f"外套开合状态：{outerwear_state}。"
    reference_rule = (
        "第1张产品穿搭图是人物、商品颜色、内搭、下装、配饰、场景和整体视觉的唯一动画基准；"
        "其余原始商品图只用于核对商品结构与颜色，不得覆盖第1张图中的实际穿搭。"
        if references else
        "没有可用视觉基准图，禁止推测商品结构；看不清的细节保持简洁。"
    )
    multi_color_lock = ""
    if role == "wear_hold_color":
        main_color = str(context.get("main_color") or visual.get("product_color") or "参考图中的主色")
        secondary_color = str(context.get("secondary_color") or "已提供副色商品图中的颜色")
        multi_color_lock = (
            f"人物身上始终穿{main_color}，手中始终展示{secondary_color}；"
            "两件衣服不得交换、融合、变色或突然消失，不得出现第三种颜色。"
        )
    product_name = str(product.get("product_name") or product.get("product_title") or "参考图中的主商品上衣").strip()
    if re.fullmatch(r"\d{8,}", product_name) or product_name.startswith("OSG_"):
        product_name = "参考图中的主商品上衣"
    selling_points = normalized_list(product.get("core_selling_points"))
    focused_points = _focused_selling_points(role, selling_points)
    duration_seconds = int(shot.get("duration_seconds") or template["duration_seconds"])
    timeline_template = {
        **template,
        **({"action_timeline": action_variant["action_timeline"], "duration_seconds": duration_seconds} if action_variant else {}),
    }
    timeline = _timeline_for_duration(timeline_template, duration_seconds)
    avoid_actions = normalized_list(expected_tags.get("avoid_action_variants"))
    diversity_rule = (
        f"本镜头动作变体为{action_variant_id}；不得复刻这些已有动作版本：{'、'.join(avoid_actions)}。"
        if action_variant_id and avoid_actions
        else (f"本镜头动作变体固定为{action_variant_id}。" if action_variant_id else "")
    )
    capture_gate = ROLE_CAPTURE_GATES.get(role, "")
    positive = "\n".join([
        f"【内容ID】{shot.get('shot_id')}",
        f"{duration_seconds}秒竖屏9:16，单镜头，固定镜面试穿机位，镜头和人物距离保持不变。",
        f"参考图约束：{reference_rule}",
        f"场景：{scene_text}",
        f"人物：{persona_text}",
        (
            f"商品：本条唯一主商品是{product_name}。严格还原颜色、领口、肩线、袖型、门襟、口袋、"
            "衣长、面料表面和整体版型；看不清的细节不得虚构。"
            + (f"本镜头只重点表现已经确认的相关卖点：{'；'.join(focused_points)}。" if focused_points else "")
        ),
        (
            f"穿搭：{styling_text}。{inner}"
            + ("下装颜色、版型和配饰严格跟随第1张产品穿搭图，不随机替换。" if references else "")
        ),
        f"构图：{template['framing']}。重点展示：{template['focus']}。",
        f"镜头验收硬指标：{capture_gate}。" if capture_gate else "",
        f"动作：{timeline}。动作只进行一次，起势和结束均保持稳定。{diversity_rule}",
        f"颜色锁定：{multi_color_lock}" if multi_color_lock else "",
        (
            "硬性要求：人物身份、商品、穿搭、房间结构、机位和浅暖白明亮光线全程不变；"
            "画面不能偏黄，使用正面柔光配合轻微斜侧光；画面包含头部时手机始终完整遮脸。"
            "不切镜、不推拉、不变焦，不生成字幕、贴纸、Logo或水印，品牌信息只由后期添加。"
        ),
    ]).strip()
    negative = (
        "不要商品变色、结构漂移、衣服融入身体、面料液化、内搭变化、外套开合变化；"
        "不要多余手指、手穿透衣服、身体重影、人物消失或变脸；"
        "不要切镜、推拉、变焦、摇移、广角畸变、循环动作、夸张走秀；"
        "不要过曝、死白高光、橙黄色滤镜、硬侧光阴影；"
        "不要字幕、文字、价格、促销标签、Logo或水印。"
    )
    payload = {
        "schema_version": "1.0",
        "prompt_version": SUPPLEMENT_PROMPT_VERSION,
        "positive_prompt": positive,
        "negative_prompt": negative,
        "reference_images": references,
        "generation": {
            "duration_seconds": duration_seconds,
            "ratio": "9:16",
            "camera": "fixed",
            "clean_plate": True,
        },
        "expected_tags": expected_tags,
    }
    payload["prompt_fingerprint"] = stable_hash(payload, length=24)
    return payload


def plan_product_richness_pool(
    db: "LightTryonDB",
    product_id: str,
    *,
    roles: Iterable[str] | None = None,
    plan_version: str = "narrative-voiceover-bridge-v2",
) -> dict[str, Any]:
    """Create one reusable evidence clip per role and distribute ownership across variants.

    The generated media assets remain product-scoped, so every long-form variant may use
    every successful richness shot even though the generation record has one variant FK.
    """
    product = db.get_product(product_id)
    if not product:
        raise KeyError(f"找不到商品: {product_id}")
    variants = db.list_narrative_variants(product_id, plan_version=plan_version)
    if not variants:
        raise ValueError("请先规划口播增强型变体")
    plans = db.list_visual_plans(product_id=product_id, plan_status="active")
    plans = [item for item in plans if str(item.get("outfit_image_path") or "").strip()]
    if not plans:
        raise ValueError("没有已确认的产品穿搭图，无法建立专用镜头池")
    visual = plans[-1]
    scene = db.get_template("scene", str(visual.get("scene_id") or "")) or {}
    styling = db.get_template("styling", str(visual.get("styling_id") or "")) or {}
    persona = db.get_template("persona", str(visual.get("persona_id") or "")) or {}
    context = {
        "product": product,
        "scene": scene,
        "persona": persona,
        "styling": styling,
        "visual_plan": visual,
        "scene_description": str(scene.get("prompt_core") or "").strip(),
        "styling_description": str(styling.get("prompt_core") or "").strip(),
    }
    selected_roles = normalized_list(roles) or list(RICHNESS_ROLE_SEQUENCE)
    unsupported = [role for role in selected_roles if role not in SHOT_TEMPLATES]
    if unsupported:
        raise ValueError(f"不支持的丰富镜头角色: {', '.join(unsupported)}")
    references = [
        str(visual.get("outfit_image_path") or ""),
        *[str(path) for path in (product.get("product_images") or [])[:2]],
    ]
    references = [path for path in references if path]
    created = 0
    items: list[dict[str, Any]] = []
    existing = {shot["shot_role"]: shot for shot in db.list_product_supplement_shots(product_id)}
    for index, role in enumerate(selected_roles, start=1):
        if role in existing:
            current = existing[role]
            refreshed_prompt = compile_supplement_prompt(current, context)
            refreshed_prompt["generation"]["duration_seconds"] = 8
            refreshed = db.upsert_supplement_shot({**current, "prompt_payload": refreshed_prompt})
            items.append({**refreshed, "reused": True})
            continue
        owner = variants[(index - 1) % len(variants)]
        template = SHOT_TEMPLATES[role]
        shot_id = "SUP_" + stable_hash(product_id, plan_version, "richness", role, length=18)
        shot = {
            "shot_id": shot_id,
            "variant_id": owner["variant_id"],
            "beat_id": f"RICH_{index:02d}",
            "shot_role": role,
            "duration_seconds": 8,
            "priority": "richness",
            "status": "planned",
            "reference_assets": references,
            "expected_tags": {
                "shot_roles": [role],
                "beat_role": "richness_pool",
                "shared_product_pool": True,
            },
            "fallback_strategy": template["fallback_strategy"],
            "max_attempts": 2,
        }
        prompt = compile_supplement_prompt(shot, context)
        prompt["generation"]["duration_seconds"] = 8
        saved = db.upsert_supplement_shot({**shot, "prompt_payload": prompt})
        items.append(saved)
        created += 1
    return {
        "product_id": product_id,
        "plan_version": plan_version,
        "requested_roles": selected_roles,
        "created": created,
        "reused": len(items) - created,
        "items": items,
    }


def plan_product_diversity_pool(
    db: "LightTryonDB",
    product_id: str,
    *,
    target_count: int | None = None,
    plan_version: str = "narrative-voiceover-bridge-v2",
    forced_role_actions: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Plan only the missing action/framing variants needed by target output count."""

    from .diversity import assess_product_asset_capacity

    product = db.get_product(product_id)
    if not product:
        raise KeyError(f"找不到商品: {product_id}")
    variants = db.list_narrative_variants(product_id, plan_version=plan_version)
    if not variants:
        raise ValueError("请先规划口播增强型变体")
    target = max(1, int(target_count or len(variants)))
    capacity = assess_product_asset_capacity(
        db, product_id, target_count=target, plan_version=plan_version,
    )
    plans = [
        item for item in db.list_visual_plans(product_id=product_id, plan_status="active")
        if str(item.get("outfit_image_path") or "").strip()
    ]
    if not plans:
        raise ValueError("没有已确认的产品穿搭图，无法建立多样性镜头池")
    visual = plans[-1]
    scene = db.get_template("scene", str(visual.get("scene_id") or "")) or {}
    styling = db.get_template("styling", str(visual.get("styling_id") or "")) or {}
    persona = db.get_template("persona", str(visual.get("persona_id") or "")) or {}
    context = {
        "product": product,
        "scene": scene,
        "persona": persona,
        "styling": styling,
        "visual_plan": visual,
        "scene_description": str(scene.get("prompt_core") or "").strip(),
        "styling_description": str(styling.get("prompt_core") or "").strip(),
    }
    references = [
        str(visual.get("outfit_image_path") or ""),
        *[str(path) for path in (product.get("product_images") or [])[:2]],
    ]
    references = [path for path in references if path]
    existing_shots = db.list_product_supplement_shots(product_id)
    in_flight_statuses = {"planned", "queued", "generating", "received"}
    in_flight_by_role: dict[str, int] = {}
    used_variants_by_role: dict[str, list[str]] = {}
    for shot in existing_shots:
        role = str(shot.get("shot_role") or "")
        expected = shot.get("expected_tags") if isinstance(shot.get("expected_tags"), dict) else {}
        action_variant = str(expected.get("action_variant") or "")
        if not action_variant and shot.get("output_asset_id") and SHOT_ACTION_VARIANTS.get(role):
            # Legacy richness shots used the first/base motion.  Reserve that
            # variant so new diversity tasks choose a genuinely different arc.
            action_variant = str(SHOT_ACTION_VARIANTS[role][0].get("variant_id") or "")
        if action_variant:
            used_variants_by_role.setdefault(role, []).append(action_variant)
        if str(shot.get("status") or "") in in_flight_statuses and not shot.get("output_asset_id"):
            in_flight_by_role[role] = in_flight_by_role.get(role, 0) + 1
    created = 0
    reused = 0
    items: list[dict[str, Any]] = []
    owner_cursor = 0
    for role in ("main_wear_upper", *RICHNESS_ROLE_SEQUENCE):
        deficit = int((capacity.get("role_deficits") or {}).get(role) or 0)
        to_plan = max(0, deficit - int(in_flight_by_role.get(role, 0)))
        variants_for_role = SHOT_ACTION_VARIANTS.get(role) or [{"variant_id": "base"}]
        used = list(dict.fromkeys(used_variants_by_role.get(role, [])))
        available = [row for row in variants_for_role if str(row.get("variant_id") or "") not in used]
        for index in range(to_plan):
            action = (available or variants_for_role)[index % len(available or variants_for_role)]
            action_id = str(action.get("variant_id") or f"variant_{index + 1}")
            owner = variants[owner_cursor % len(variants)]
            owner_cursor += 1
            beat_id = f"DIV_{role}_{action_id}".upper()
            shot_id = "SUP_" + stable_hash(product_id, plan_version, "diversity", role, action_id, length=18)
            current = db.get_supplement_shot(shot_id)
            if current:
                reused += 1
                items.append({**current, "reused": True})
                continue
            template = SHOT_TEMPLATES[role]
            shot = {
                "shot_id": shot_id,
                "variant_id": owner["variant_id"],
                "beat_id": beat_id,
                "shot_role": role,
                "duration_seconds": 8,
                "priority": "diversity",
                "status": "planned",
                "reference_assets": references,
                "expected_tags": {
                    "shot_roles": [role],
                    "beat_role": "diversity_pool",
                    "shared_product_pool": True,
                    "action_variant": action_id,
                    "avoid_action_variants": used,
                    "target_output_count": target,
                },
                "fallback_strategy": template["fallback_strategy"],
                "max_attempts": 2,
            }
            prompt = compile_supplement_prompt(shot, context)
            saved = db.upsert_supplement_shot({**shot, "prompt_payload": prompt})
            items.append(saved)
            used.append(action_id)
            created += 1
    forced = forced_role_actions or {}
    for role, action_id in forced.items():
        if role not in SHOT_TEMPLATES:
            raise ValueError(f"不支持的强制多样性镜头角色: {role}")
        action = next(
            (row for row in SHOT_ACTION_VARIANTS.get(role, []) if row.get("variant_id") == action_id),
            None,
        )
        if not action:
            raise ValueError(f"镜头角色 {role} 不支持动作版本: {action_id}")
        shot_id = "SUP_" + stable_hash(product_id, plan_version, "diversity", role, action_id, length=18)
        current = db.get_supplement_shot(shot_id)
        if current:
            reused += 1
            if not any(str(item.get("shot_id") or "") == shot_id for item in items):
                items.append({**current, "reused": True})
            continue
        owner = variants[owner_cursor % len(variants)]
        owner_cursor += 1
        used = list(dict.fromkeys(used_variants_by_role.get(role, [])))
        shot = {
            "shot_id": shot_id,
            "variant_id": owner["variant_id"],
            "beat_id": f"DIV_{role}_{action_id}".upper(),
            "shot_role": role,
            "duration_seconds": 8,
            "priority": "diversity",
            "status": "planned",
            "reference_assets": references,
            "expected_tags": {
                "shot_roles": [role],
                "beat_role": "diversity_pool",
                "shared_product_pool": True,
                "action_variant": action_id,
                "avoid_action_variants": used,
                "target_output_count": target,
                "forced_by_diversity_qc": True,
            },
            "fallback_strategy": SHOT_TEMPLATES[role]["fallback_strategy"],
            "max_attempts": 2,
        }
        prompt = compile_supplement_prompt(shot, context)
        saved = db.upsert_supplement_shot({**shot, "prompt_payload": prompt})
        items.append(saved)
        used_variants_by_role.setdefault(role, []).append(action_id)
        created += 1
    return {
        "product_id": product_id,
        "plan_version": plan_version,
        "target_count": target,
        "capacity_before": capacity,
        "created": created,
        "reused": reused,
        "pending_by_role": in_flight_by_role,
        "forced_role_actions": forced,
        "items": items,
    }
