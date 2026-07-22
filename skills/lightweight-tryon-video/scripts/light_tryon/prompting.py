from __future__ import annotations

import re
from typing import Any

from .utils import normalized_list, stable_hash
from .shot_profiles import get_shot_profile


PROMPT_BUILDER_VERSION = "tryon-prompt-v2.4.1"

UPPER_GARMENT_CATEGORIES = {"top", "tshirt", "tank_top", "knit_top", "shirt", "outerwear", "上装", "上衣", "外套", "t恤", "背心", "吊带", "针织", "针织衫", "衬衫"}
PUSH_CONFLICT_PHRASES = ("不要镜头推进", "不要推拉", "不推拉", "镜头固定", "固定机位")


CONTEXT_DEPENDENT_SCENE_PHRASES = (
    "与主场景", "主场景相同", "同一套", "同一个女生", "同一人物展示区", "主账号视觉",
)

SELF_CONTAINED_SCENES = {
    "half_body_detail": (
        "真实居家的现代简约卧室，暖白色墙面、浅米色光滑瓷砖地面；左后方是一张低矮整洁的床，"
        "使用浅灰色床品，右后方是带竖向褶皱的整面灰色落地窗帘，右侧后方有小型开放式置物架，"
        "天花板有柔和暖白色灯带。竖屏9:16，胸口高度正面平视固定机位，半身或大半身构图；"
        "画面清楚保留灰色窗帘、暖白墙面和低矮床头中的至少两个背景元素，重点展示上衣领口、"
        "肩线、袖口和面料细节。光线明亮柔和、暖中性，真实居家、非商业棚拍。"
    ),
    "variation_full_body": (
        "真实居家的现代简约卧室窗边区域，暖白色墙面、浅米色光滑瓷砖地面；人物站在带竖向褶皱的"
        "灰色落地窗帘旁，左后方可见一张使用米白色床品的低矮床铺，右侧后方可见小型开放式置物架，"
        "天花板有柔和暖白色灯带。竖屏9:16，胸口高度正面平视固定机位，全身构图。"
        "光线明亮柔和、暖中性，真实生活化、非商业棚拍。"
    ),
}


def _self_contained_scene(scene: dict[str, Any]) -> str:
    text = str(scene.get("prompt_core") or "").strip()
    if not any(phrase in text for phrase in CONTEXT_DEPENDENT_SCENE_PHRASES):
        return text
    replacement = SELF_CONTAINED_SCENES.get(str(scene.get("scene_type") or ""))
    if not replacement:
        raise ValueError(f"场景 {scene.get('scene_id')} 的 Prompt 含跨任务指代，必须改为完整场景描述")
    return replacement


def _self_contained_persona(persona: dict[str, Any]) -> str:
    text = str(persona.get("prompt_core") or "").strip()
    return re.sub(r"^同一个年轻成年女生", "一名年轻成年女生", text)


def _self_contained_action(action: dict[str, Any], scene: dict[str, Any]) -> str:
    text = str(action.get("prompt_core") or "").strip()
    if str(action.get("action_type") or "") == "half_step_forward" or action.get("action_id") == "ACT_006":
        start_position = (
            "画面中部偏灰色窗帘一侧"
            if str(scene.get("scene_type") or "") == "variation_full_body"
            else "画面中央"
        )
        return (
            f"人物全身站在{start_position}，缓慢向镜头方向移动半步，在头顶到双脚仍完整入镜的位置停顿约1秒，"
            f"再缓慢退回{start_position}并自然定格；镜头固定，手机始终完整遮脸。"
        )
    return text.replace("回到原展示区", "回到画面中央固定展示位置")


def _compact_segment(value: Any, limit: int) -> str:
    """Keep operator-facing prompts readable without leaking the audit payload."""
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    pieces = [part.strip() for part in re.split(r"(?<=[。！？.!?])", text) if part.strip()]
    kept: list[str] = []
    size = 0
    for piece in pieces:
        if kept and size + len(piece) > limit:
            break
        if not kept and len(piece) > limit:
            return piece[: limit - 1].rstrip("，,；; ") + "。"
        kept.append(piece)
        size += len(piece)
    return "".join(kept) or text[: limit - 1].rstrip("，,；; ") + "。"


SCENE_DISPLAY_VALUES = {
    "indoor_tryon_room": "室内试穿空间",
    "bedroom": "卧室",
    "modern_cafe": "现代咖啡店",
    "warm_white": "暖白色",
    "light_beige": "浅米色",
    "chest_level": "胸口高度",
    "eye_level": "眼平高度",
    "front_flat": "正面平视",
    "bright_not_overexposed": "明亮但不过曝",
    "warm_neutral": "暖中性",
    "neutral_warm_no_yellow": "浅暖白不偏黄",
    "oblique_soft_with_fill": "45度斜侧柔光加正面补光",
    "glossy_tile": "浅米色光滑瓷砖",
    "left_back": "左后方",
    "right_back": "右后方",
    "right_side_back": "右后方",
    "gray": "灰色",
    "dark_blue_gray": "深蓝灰",
    "warm_white_strip": "暖白灯带",
}


def _compact_scene_context(scene: dict[str, Any], resolved_text: str) -> str:
    """Turn the resolved structured scene snapshot into concise model-ready prose."""
    source = scene.get("source_payload") if isinstance(scene.get("source_payload"), dict) else {}

    def value(key: str) -> str:
        raw = source.get(key)
        if raw in (None, "", [], {}):
            raw = scene.get(key)
        text = str(raw or "").strip()
        return SCENE_DISPLAY_VALUES.get(text, text)

    name = value("scene_name") or value("room_type")
    wall = value("wall_color")
    floor = value("floor_type") or value("floor_color")
    bed_position = value("bed_position")
    bed_sheet = value("bed_sheet_color")
    curtain_position = value("curtain_position")
    curtain_color = value("curtain_color")
    shelf_position = value("shelf_position")
    ceiling_light = value("ceiling_light_type")
    camera_height = value("camera_height")
    camera_angle = value("camera_angle")
    lighting_level = value("lighting_level")
    lighting_tone = value("lighting_tone")

    details: list[str] = []
    if wall or floor:
        details.append("、".join(item for item in (f"{wall}墙面" if wall else "", floor) if item))
    if bed_position:
        details.append(f"{bed_position}低矮床" + (f"配{bed_sheet}床品" if bed_sheet else ""))
    if curtain_position:
        color = curtain_color.rstrip("色") + "色" if curtain_color else ""
        details.append(f"{curtain_position}{color}落地窗帘（竖褶）")
    if shelf_position:
        details.append("不出现置物架" if shelf_position in {"不出现", "none", "无"} else f"置物架位于{shelf_position}")
    if ceiling_light:
        details.append(ceiling_light)
    if lighting_level or lighting_tone:
        details.append("、".join(item for item in (lighting_level, lighting_tone + "光" if lighting_tone else "") if item))
    if camera_height or camera_angle:
        details.append("".join(item for item in (camera_height, camera_angle) if item))
    if not details:
        return _compact_segment(resolved_text, 145)
    return f"{name}：" + "；".join(details)


def _dedupe_negative_parts(values: list[str]) -> str:
    seen: set[str] = set()
    parts: list[str] = []
    for value in values:
        for part in re.split(r"[。！？.!?]+", str(value or "")):
            normalized = re.sub(r"\s+", " ", part).strip(" ,，;；")
            key = normalized.lower()
            if normalized and key not in seen:
                seen.add(key)
                parts.append(normalized)
    return "。".join(parts) + ("。" if parts else "")


def _camera_safe_negative(value: Any, camera_motion: str) -> str:
    if camera_motion != "push_in":
        return str(value or "")
    parts = re.split(r"[。！？.!?]+", str(value or ""))
    return "。".join(part for part in parts if part.strip() and not any(phrase in part for phrase in PUSH_CONFLICT_PHRASES))


def _template_snapshot(kind: str, template: dict[str, Any]) -> dict[str, Any]:
    id_key = f"{kind}_id"
    return {
        "id": template.get(id_key),
        "version": template.get("config_version") or "V1",
        "prompt_core": template.get("prompt_core") or "",
        "prompt_negative": template.get("prompt_negative") or "",
        "feishu_record_id": template.get("feishu_record_id") or "",
    }


BRAND_SERIES_BY_CATEGORY = {
    "outerwear": "Everyday Outerwear",
    "top": "Everyday Tops",
    "tshirt": "Everyday Tops",
    "tank_top": "Easy Layering",
    "knit_top": "Soft Knit Edit",
    "shirt": "Daily Shirt Edit",
    "dress": "Everyday Dress",
    "skirt": "Everyday Skirt",
    "pants": "Everyday Pants",
}


def build_brand_plan(persona: dict[str, Any], product: dict[str, Any]) -> dict[str, Any]:
    enabled = str(persona.get("brand_overlay_enabled") or "disabled").strip().lower() == "enabled"
    logos = normalized_list(persona.get("brand_logo_images"))
    display_name = str(persona.get("brand_display_name") or "").strip()
    category = str(product.get("category") or "").strip().lower()
    series_title = str(persona.get("brand_default_series_title") or "").strip()
    if not series_title:
        series_title = BRAND_SERIES_BY_CATEGORY.get(category, "Everyday Edit")
    active = enabled and bool(logos or display_name)
    return {
        "enabled": active,
        "configured": enabled,
        "logo_images": logos,
        "display_name": display_name,
        "series_title": series_title,
        "style_preset": str(persona.get("brand_style_preset") or "cream_serif"),
        "primary_color": str(persona.get("brand_primary_color") or "cream_white"),
        "position": "center",
        "start_seconds": 0.0,
        "display_seconds": 0.8,
        "fade_in_seconds": 0.0,
        "fade_out_seconds": 0.18,
        "max_width_ratio": 0.40,
        "center_y_ratio": 0.50,
        "process_mark": "none",
        "render_mode": "post_production_overlay",
        "cover_from_first_frame": True,
        "dynamic_subtitles_enabled": False,
        "disabled_reason": "" if active else ("missing_logo_or_display_name" if enabled else "not_enabled"),
    }


def _subtitle_plan(subtitle: dict[str, Any], duration: int, *, brand_intro_seconds: float = 0.0) -> dict[str, Any]:
    opening = str(subtitle.get("opening_text") or "").strip()
    middle = str(subtitle.get("middle_text") or "").strip()
    ending = str(subtitle.get("ending_text") or "").strip()
    cues: list[dict[str, Any]] = []
    if opening:
        opening_start = max(0.35, float(brand_intro_seconds) + 0.15)
        cues.append({"start": round(opening_start, 2), "end": min(3.2, duration * 0.36), "text": opening})
    if middle:
        cues.append({"start": duration * 0.38, "end": duration * 0.64, "text": middle})
    if ending:
        cues.append({"start": max(duration * 0.66, duration - 3.2), "end": duration - 0.35, "text": ending})
    return {
        "subtitle_id": subtitle["subtitle_id"],
        "language": subtitle.get("language"),
        "style": subtitle.get("subtitle_style"),
        "tone": subtitle.get("tone"),
        "char_limit": int(subtitle.get("char_limit") or 30),
        "render_mode": "post_production_burn_in",
        "cues": cues,
    }


def build_prompt(context: dict[str, Any]) -> dict[str, Any]:
    job = context["job"]
    product = context["product"]
    scene = context["scene"]
    persona = context["persona"]
    styling = context["styling"]
    action = context["action"]
    subtitle = context["subtitle"]
    shot_profile = get_shot_profile(job.get("shot_profile_id"), scene)
    duration = int(job["duration_seconds"])
    visual_plan = context.get("visual_plan") or {}
    outfit_request = visual_plan.get("outfit_request_payload") if isinstance(visual_plan.get("outfit_request_payload"), dict) else {}
    resolved_context = outfit_request.get("resolved_context") if isinstance(outfit_request.get("resolved_context"), dict) else {}
    confirmed_outfit = str(visual_plan.get("outfit_image_path") or visual_plan.get("outfit_image_url") or "").strip()
    images = [confirmed_outfit] if confirmed_outfit else normalized_list(product.get("product_images"))
    selling_points = normalized_list(product.get("core_selling_points"))
    product_name = str(product.get("product_name") or product.get("product_title") or job["product_id"]).strip()
    product_title = str(product.get("product_title") or product_name).strip()
    camera_motion = str(shot_profile.get("camera_motion") or "fixed").strip()
    is_push_in = camera_motion == "push_in"
    product_category = str(product.get("category") or "").strip().lower()
    is_upper_garment = product_category in UPPER_GARMENT_CATEGORIES
    is_outerwear = product_category in {"outerwear", "外套"}
    lighting_level = str(scene.get("lighting_level") or "bright_not_overexposed")
    lighting_tone = str(scene.get("lighting_tone") or scene.get("lighting_temp") or "warm_neutral")

    environment_segment = str(resolved_context.get("scene") or _self_contained_scene(scene)).strip()
    environment_display = str(resolved_context.get("scene_display") or "").strip()
    shot_segment = str(shot_profile.get("prompt_core") or "").strip()
    scene_segment = f"{(environment_display or _compact_scene_context(scene, environment_segment)).rstrip('。')}。{shot_segment}"
    persona_segment = str(resolved_context.get("persona") or _self_contained_persona(persona)).strip()
    product_segment = (
        f"本条唯一主商品是：{product_name}。商品标题信息：{product_title}。"
        + (f"只强调已提供的明确卖点：{'；'.join(selling_points)}。" if selling_points else "不要自行补充未提供的材质、功能或结构卖点。")
        + ("必须以已确认的产品穿搭图作为唯一视觉基准，保持人物、场景、商品和整套搭配不变；" if confirmed_outfit else "必须严格依据商品参考图保持颜色、领口、袖型、门襟、口袋、长度、面料表面和整体版型；")
        + "看不清的细节宁可保持简洁，不得虚构。"
    )
    styling_segment = str(resolved_context.get("styling") or styling.get("prompt_core") or "").strip()
    inner_layer_segment = str(resolved_context.get("inner_layer") or "").strip()
    if inner_layer_segment:
        styling_segment = f"{styling_segment.rstrip('。')}；{inner_layer_segment}。"
    action_segment = _self_contained_action(action, scene)
    if is_upper_garment:
        opening_segment = (
            "开场0-1秒：先保持与参考图一致的头顶至大腿中段上装展示构图，人物稳定正面站立，"
            "手机完整遮脸，上装领口、肩线、袖型、门襟、口袋和完整下摆清楚可见；"
            "下装只露腰头、胯部和大腿上半截。"
        )
    else:
        opening_segment = "开场0-1秒：人物在固定展示区域稳定站立，商品完整清楚可见。"
    camera_segment = (
        "单镜头，镜头沿正面光轴极慢、匀速、平稳推近，不变焦、不摇移、不改变拍摄角度；"
        "从大半身缓慢收至上半身，商品领口、肩线、袖型、门襟和完整下摆始终可见。"
        if is_push_in else
        "单镜头、固定第三人称机位，镜头位置、焦段和构图全程不动。"
    )
    lighting_segment = (
        f"光线配置为 {lighting_level}/{lighting_tone}：高亮柔光、暖中性肤色，主体和商品清晰通透；"
        "高光有细节、白色不溢出，禁止过曝、偏色和商品颜色漂移。"
    )
    consistency_segment = (
        f"{camera_segment}竖屏 9:16，视频精确目标时长 {duration} 秒。"
        f"景别为 {shot_profile.get('shot_type')}，人物始终位于固定展示区域。"
        f"{lighting_segment}{str(scene.get('consistency_prompt') or '').strip()}"
        f"{str(shot_profile.get('consistency_prompt') or '').strip()}"
        "从第一帧到最后一帧保持同一个成年女生、同一个房间结构、同一光线、同一件商品和同一套搭配；动作只有一个主动作，包含自然起势、展示峰值、短暂停留和回收。"
        "手机必须始终完整遮住整张脸。输出无文字、无贴纸、无水印的干净原片，品牌信息只由后期精确叠加。"
    )
    negatives = [
        _camera_safe_negative(scene.get("prompt_negative"), camera_motion),
        _camera_safe_negative(shot_profile.get("prompt_negative"), camera_motion),
        str(persona.get("prompt_negative") or ""),
        _camera_safe_negative(action.get("prompt_negative"), camera_motion),
        "不要商品变色、结构漂移、衣服融入身体、面料液化、手脚畸形、额外手指、身体重影、人物突然消失或变脸。",
        ("不要突然加速、后退、横移、摇移、变焦、切镜、广角畸变或裁掉上装下摆。" if is_push_in else
         "不要镜头推进、缩放、摇移、切镜、广角畸变、慢动作、循环动作、夸张表演、走出画面。"),
        "不要过曝、死白高光、灰雾、偏色、肤色失真或商品颜色变化。",
        "不要橙黄色滤镜、黄昏感、暖黄偏色、硬侧光阴影或人物一侧明显过暗。",
        "不要让绿植、落地灯或其他装饰进入人物轮廓、遮挡服装或成为画面视觉主体。",
        "不要在生成画面中出现任何文字、字幕、价格、促销标签、logo 或水印。",
    ]
    negative_prompt = _dedupe_negative_parts(negatives)
    critical_scene_lock = ""
    if visual_plan.get("resolved_background_type") or visual_plan.get("resolved_edge_decor"):
        critical_scene_lock = (
            "主背景、边缘装饰及其位置、主光方向全程锁定首帧，不得新增、移除或移动家具装饰；"
            "保持浅暖白、明亮不偏黄，不出现硬侧阴影或人物一侧过暗；"
        )
    outerwear_lock = (
        "外套及内搭全程锁定首帧状态，不得改变外套开合、内搭款式、领口、颜色或露出面积；"
        if is_outerwear else ""
    )
    critical_constraints = f"{critical_scene_lock}{outerwear_lock}"
    template_snapshots = {
        "persona": _template_snapshot("persona", persona),
        "scene": _template_snapshot("scene", scene),
        "shot_profile": {
            "id": shot_profile["shot_profile_id"],
            "version": "SHOT_V1",
            "prompt_core": shot_profile.get("prompt_core") or "",
            "prompt_negative": shot_profile.get("prompt_negative") or "",
            "feishu_record_id": "",
        },
        "action": _template_snapshot("action", action),
        "styling": _template_snapshot("styling", styling),
        "subtitle": _template_snapshot("subtitle", subtitle),
    }
    if context.get("shot_plan"):
        template_snapshots["shot_plan"] = _template_snapshot("shot_plan", context["shot_plan"])
    template_versions = {kind: snapshot["version"] for kind, snapshot in template_snapshots.items()}
    segments = {
        "environment": environment_segment,
        "shot": shot_segment,
        "opening": opening_segment,
        "persona": persona_segment,
        "product": product_segment,
        "styling": styling_segment,
        "action": action_segment,
        "consistency": consistency_segment,
        "critical_constraints": critical_constraints,
    }
    # The full structured segments and template snapshots remain in the payload for
    # reproducibility. Only this compact, model-ready text is shown to operators and
    # exported to the generation queue.
    full_prompt = (
        f"【内容ID】{job['job_id']}\n"
        f"{duration}秒竖屏9:16，{('单镜头极慢平稳推近' if is_push_in else '单镜头固定机位')}。\n"
        f"场景：{_compact_segment(scene_segment, 190)}\n"
        f"人物：{_compact_segment(persona_segment, 115)}\n"
        "商品：严格按参考图还原颜色、领口、袖型、门襟、口袋、长度、面料表面和整体版型；"
        "看不清的细节保持简洁，不得虚构。\n"
        f"搭配：{_compact_segment(styling_segment, 150)}\n"
        f"首屏：{_compact_segment(opening_segment, 120)}\n"
        f"动作：{_compact_segment(action_segment, 120)}\n"
        + ("硬性要求：高亮柔光但不过曝，商品原色和完整上装结构始终清楚；" if is_upper_garment else
           "硬性要求：高亮柔光但不过曝，商品原色和完整商品结构始终清楚；")
        + "从第一帧到最后一帧，人物外观、服装款式、房间布局和光线不得变化；手机始终完整遮脸；"
        + critical_constraints
        + ("只允许上述极慢推近，不后退、不横移、不变焦、不切镜、不变形；" if is_push_in else "镜头全程固定，不推拉、不切镜、不变形；")
        + "不出现字幕、贴纸、Logo或水印，品牌字标与服装类目如需使用只由后期添加。"
    )
    brand_plan = build_brand_plan(persona, product)
    subtitle_plan = _subtitle_plan(
        subtitle,
        duration,
        brand_intro_seconds=float(brand_plan["display_seconds"]) if brand_plan["enabled"] else 0.0,
    )
    if brand_plan["enabled"] and not brand_plan.get("dynamic_subtitles_enabled", False):
        subtitle_plan = {
            **subtitle_plan,
            "render_mode": "disabled",
            "disabled_reason": "brand_cover_only",
            "cues": [],
        }
    payload = {
        "schema_version": "1.0.0",
        "prompt_builder_version": PROMPT_BUILDER_VERSION,
        "content_id": job["job_id"],
        "segments": segments,
        "positive_prompt": full_prompt,
        "display_prompt": full_prompt,
        "negative_prompt": negative_prompt,
        "reference_images": images,
        "reference_roles": [
            {"url": url, "role": "confirmed_outfit_truth" if confirmed_outfit else "strict_product_truth", "priority": index + 1}
            for index, url in enumerate(images)
        ],
        "generation": {
            "mode": "image_to_video" if images else "text_to_video",
            "model": str(job.get("generation_model") or "Seedance 2.0"),
            "ratio": "9:16",
            "resolution": "720P",
            "duration_seconds": duration,
            "camera": camera_motion,
            "clean_plate": True,
        },
        "subtitle_plan": subtitle_plan,
        "brand_plan": brand_plan,
        "template_versions": template_versions,
        "template_snapshots": template_snapshots,
        "qc_expectations": {
            "shot_profile_id": shot_profile["shot_profile_id"],
            "shot_type": shot_profile.get("shot_type"),
            "required_anchors": scene.get("required_anchors") or [],
            "minimum_anchor_count": 2,
            "phone_covers_face": True,
            "duration_min": 8.0,
            "duration_max": 10.0,
            "aspect_ratio": "9:16",
            "camera_motion": camera_motion,
            "upper_garment_fully_visible": is_upper_garment,
            "brightness_style": "high_key_soft",
            "brightness_adequate": True,
            "no_overexposure": True,
            "product_color_preserved": True,
            "resolved_background_type": visual_plan.get("resolved_background_type") or "",
            "resolved_edge_decor": visual_plan.get("resolved_edge_decor") or "",
            "resolved_key_light_direction": visual_plan.get("resolved_key_light_direction") or "",
            "background_simple": True,
            "decor_not_overlap_garment": True,
            "no_yellow_cast": True,
            "fabric_texture_visible": True,
        },
    }
    payload["prompt_fingerprint"] = stable_hash(payload, length=16)
    return payload


def build_jimeng_record(context: dict[str, Any], prompt_payload: dict[str, Any]) -> dict[str, Any]:
    job = context["job"]
    product = context["product"]
    generation = prompt_payload["generation"]
    business_product_id = str(product.get("source_product_code") or job["product_id"]).strip()
    return {
        "任务名": job["job_id"],
        "内容ID": job["job_id"],
        "商品ID": business_product_id,
        "状态": "待处理",
        "提示词": prompt_payload["positive_prompt"],
        "参考图": prompt_payload.get("reference_images") or [],
        "免参考图": not bool(prompt_payload.get("reference_images")),
        "生成次数": 1,
        "模型": generation["model"],
        "视频比例": generation["ratio"],
        "视频时长": generation["duration_seconds"],
        "分辨率": generation["resolution"],
        "场景ID": job["scene_id"],
        "镜头策略": job.get("shot_profile_id") or "",
        "动作ID": job["action_id"],
        "搭配ID": job["styling_id"],
        "字幕ID": job["subtitle_id"],
        "人设ID": job["persona_id"],
        "变体编号": job["variant_no"],
        "Prompt版本": prompt_payload["prompt_builder_version"],
        "字幕计划": prompt_payload["subtitle_plan"],
        "商品名称": product.get("product_name"),
    }
