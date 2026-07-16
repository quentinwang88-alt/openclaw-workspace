from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path
from typing import Any, Iterable

from .database import LightTryonDB
from .models import PlannedJob
from .planner import plan_product
from .prompting import PROMPT_BUILDER_VERSION, build_brand_plan, build_prompt
from .utils import category_matches, normalized_list, now_iso, safe_slug, stable_hash


OUTFIT_BUILDER_VERSION = "outfit-image-v1.8.0"
MAX_VISUAL_PLANS = 6
UPPER_GARMENT_CATEGORIES = {"top", "tshirt", "tank_top", "knit_top", "shirt", "outerwear"}
BRAND_ONLY_PERSONA_FIELDS = {
    "brand_overlay_enabled", "brand_logo_images", "brand_display_name", "brand_style_preset",
    "brand_primary_color", "brand_default_series_title",
}
LEGACY_NAME_TO_ID = {
    "现代简约卧室": "SCENE_A_001",
    "室内INS奶油风": "SCENE_A_001",
    "白色高腰阔腿裤": "STYLE_001",
    "经典蓝色直筒牛仔裤": "STYLE_002",
    "白色高腰短裤": "STYLE_003",
    "纯色休闲短裤": "STYLE_004",
    "简洁半裙": "STYLE_005",
    "保持商品原套装": "STYLE_006",
}

DEFAULT_BOTTOM_COLOR_POOLS = {
    "直筒牛仔裤": ["蓝色牛仔"],
    "straight_jeans": ["蓝色牛仔"],
    "白色短裤": ["白色"],
    "white_shorts": ["白色"],
    "同色套装下装": ["同色系"],
    "matching_set_bottom": ["同色系"],
}


def _persona_visual_source_hash(persona: dict[str, Any]) -> str:
    source = persona.get("source_payload")
    if isinstance(source, dict) and source:
        visual_source = {key: value for key, value in source.items() if key not in BRAND_ONLY_PERSONA_FIELDS}
        return stable_hash(visual_source, length=24)
    return str(persona.get("source_hash") or "")
DEFAULT_NEUTRAL_BOTTOM_COLORS = ["黑色", "浅灰", "米色"]
BOTTOM_TYPE_DISPLAY = {
    "wide_leg_pants": "高腰阔腿裤",
    "straight_jeans": "直筒牛仔裤",
    "white_shorts": "白色短裤",
    "casual_shorts": "休闲短裤",
    "midi_skirt": "半裙",
    "matching_set_bottom": "同色套装下装",
}
SCENE_VALUE_DISPLAY = {
    "indoor_tryon_room": "室内试穿空间",
    "bedroom": "卧室",
    "bedroom_corner": "卧室角落",
    "window_side_room": "窗边房间",
    "modern_cafe": "现代咖啡店",
    "living_room": "客厅",
    "warm_white": "暖白色",
    "white": "纯白色",
    "light_beige": "浅米色",
    "low_minimal": "低矮简约床",
    "small_open": "小型开放式置物架",
    "chest_level": "胸口高度",
    "waist_level": "腰部高度",
    "eye_level": "眼平高度",
    "front_flat": "正面平视",
    "slight_high": "轻微俯拍",
    "slight_low": "轻微仰拍",
    "center_display_zone": "中央展示区",
    "center": "正中",
    "small": "小范围",
    "very_small": "极小范围",
    "soft_bright_natural": "高亮柔和自然光",
    "high_key_soft_natural": "高亮柔和自然光",
    "oblique_soft_with_fill": "45度斜侧柔光加正面补光",
    "bright_not_overexposed": "明亮但不过曝",
    "warm_neutral": "暖中性色调",
    "neutral_warm_no_yellow": "浅暖白不偏黄",
}
SCENE_STRUCTURED_FIELDS = (
    ("room_type", "房间类型"),
    ("wall_color", "墙面颜色"),
    ("floor_type", "地面材质"),
    ("floor_color", "地面颜色"),
    ("ceiling_type", "天花板"),
    ("ceiling_light_type", "顶部灯光"),
    ("bed_position", "床的位置"),
    ("bed_style", "床的样式"),
    ("bed_sheet_color", "床品颜色"),
    ("curtain_position", "窗帘位置"),
    ("curtain_color", "窗帘颜色"),
    ("shelf_position", "置物架位置"),
    ("shelf_style", "置物架样式"),
    ("scene_style", "场景视觉风格"),
    ("background_cleanliness", "背景简洁度"),
    ("camera_height", "机位高度"),
    ("camera_angle", "机位角度"),
    ("subject_position", "人物展示区"),
    ("movement_boundary", "人物运动范围"),
    ("lighting_style", "光线风格"),
    ("lighting_level", "光线强度"),
    ("lighting_tone", "光线色调"),
)

LEGACY_ROOM_DETAIL_FIELDS = {
    "bed_position", "bed_style", "bed_sheet_color", "curtain_position", "curtain_color", "shelf_position", "shelf_style",
}


def _asset_fingerprint(value: Any) -> str:
    items: list[str] = []
    for raw in value if isinstance(value, list) else normalized_list(value):
        if isinstance(raw, dict):
            items.append(stable_hash(raw, length=24))
            continue
        text = str(raw or "").strip()
        path = Path(text).expanduser()
        if text and path.is_file():
            digest = hashlib.sha256()
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            items.append(digest.hexdigest())
        elif text:
            items.append(stable_hash(text, length=24))
    return stable_hash(items, length=24)


def _validate_unique_names(rows: list[dict[str, Any]], kind: str) -> None:
    name_key = f"{kind}_name"
    seen: set[str] = set()
    duplicates: set[str] = set()
    for row in rows:
        name = str(row.get(name_key) or "").strip()
        if not name:
            raise ValueError(f"启用的{'场景' if kind == 'scene' else '搭配'}模板存在空名称")
        if name in seen:
            duplicates.add(name)
        seen.add(name)
    if duplicates:
        raise ValueError(f"启用的{'场景' if kind == 'scene' else '搭配'}模板名称重复: {', '.join(sorted(duplicates))}")


def _styling_applicable(row: dict[str, Any], product: dict[str, Any]) -> bool:
    category = str(product.get("category") or "top")
    if not category_matches(category, row.get("applicable_product_type")):
        return False
    fit_requirements = {item.lower() for item in normalized_list(row.get("product_fit"))}
    if fit_requirements and "不限" not in fit_requirements and "*" not in fit_requirements:
        product_text = " ".join(
            str(product.get(key) or "") for key in ("sub_category", "product_title", "product_name", "notes")
        ).lower()
        known_fit = {item for item in fit_requirements if item and item in product_text}
        # 商品没有结构化版型时不做臆测；有明确版型文本时才执行约束。
        explicit_fit_words = {"修身", "合体", "短款", "宽松", "slim", "loose", "cropped"}
        if any(word in product_text for word in explicit_fit_words) and not known_fit:
            return False
    forbidden = normalized_list(row.get("forbidden_pairings"))
    product_text = " ".join(
        str(product.get(key) or "") for key in ("sub_category", "product_title", "product_name", "notes")
    ).lower()
    if any(rule.lower() in product_text for rule in forbidden if len(rule.strip()) >= 2):
        return False
    return True


def _auto_row(rows: list[dict[str, Any]], kind: str, product: dict[str, Any]) -> dict[str, Any]:
    if kind == "scene":
        applicable = [row for row in rows if category_matches(str(product.get("category") or "top"), row.get("applicable_categories"))] or rows
        return min(
            applicable,
            key=lambda row: (-int(row.get("priority") or 0), -int(row.get("usage_ratio") or 0), str(row["scene_id"])),
        )
    applicable = [row for row in rows if _styling_applicable(row, product)]
    if not applicable:
        raise ValueError("没有符合商品类型、版型和禁止搭配规则的启用搭配模板")
    return min(applicable, key=lambda row: (-int(row.get("priority") or 0), str(row["styling_id"])))


def resolve_template_selection(
    db: LightTryonDB,
    kind: str,
    raw_values: Any,
    product: dict[str, Any],
) -> list[dict[str, Any]]:
    if kind not in {"scene", "styling"}:
        raise ValueError("视觉方案只支持 scene / styling 选择")
    rows = db.list_templates(kind, "enabled")
    if not rows:
        raise ValueError(f"没有启用的{'场景' if kind == 'scene' else '搭配'}模板")
    _validate_unique_names(rows, kind)
    values = normalized_list(raw_values)
    auto = not values or values == ["自动选择"]
    if "自动选择" in values and len(values) > 1:
        raise ValueError("“自动选择”不能和具体模板同时选择")
    if auto:
        return [_auto_row(rows, kind, product)]
    id_key = f"{kind}_id"
    name_key = f"{kind}_name"
    by_id = {str(row[id_key]): row for row in rows}
    by_name = {str(row[name_key]): row for row in rows}
    resolved: list[dict[str, Any]] = []
    for value in values:
        row = by_id.get(value) or by_id.get(LEGACY_NAME_TO_ID.get(value, "")) or by_name.get(value)
        if not row:
            raise ValueError(f"不支持或未启用的{'场景' if kind == 'scene' else '搭配'}: {value}")
        resolved.append(row)
    return resolved


def _select_persona(db: LightTryonDB, product: dict[str, Any]) -> dict[str, Any]:
    rows = db.list_templates("persona", "enabled")
    if not rows:
        raise ValueError("没有启用的视觉身份")
    persona_id = str(product.get("default_persona_id") or "").strip()
    if persona_id:
        match = next((row for row in rows if row["persona_id"] == persona_id), None)
        if not match:
            raise ValueError(f"商品默认视觉身份不可用或未启用: {persona_id}")
        return match
    return min(rows, key=lambda row: (-int(row.get("priority") or 0), str(row["persona_id"])))


def _resolve_styling_variant(
    product: dict[str, Any],
    scene: dict[str, Any],
    styling: dict[str, Any],
    persona: dict[str, Any],
    *,
    source_record_id: str,
    product_asset_fingerprint: str,
) -> tuple[str, str]:
    """Resolve one stable, automatic bottom color and the cumulative fit constraints."""
    colors = normalized_list(styling.get("bottom_color"))
    if not colors:
        colors = DEFAULT_BOTTOM_COLOR_POOLS.get(
            str(styling.get("bottom_type") or "").strip(),
            DEFAULT_NEUTRAL_BOTTOM_COLORS,
        )
    seed = stable_hash(
        source_record_id or product.get("product_id") or product.get("source_product_code"),
        product_asset_fingerprint,
        scene.get("scene_id"),
        styling.get("styling_id"),
        persona.get("persona_id"),
        "bottom-color-v1",
        length=16,
    )
    selected_color = colors[int(seed, 16) % len(colors)]
    resolved_fit = "、".join(normalized_list(styling.get("bottom_fit")))
    return selected_color, resolved_fit


def _resolve_scene_variant(
    product: dict[str, Any],
    scene: dict[str, Any],
    styling: dict[str, Any],
    persona: dict[str, Any],
    *,
    source_record_id: str,
    product_asset_fingerprint: str,
    variant_index: int = 0,
) -> tuple[str, str, str]:
    """Resolve one stable background, edge decoration and key-light direction."""
    if not any((
        normalized_list(scene.get("background_type_pool")),
        normalized_list(scene.get("edge_decor_pool")),
        str(scene.get("scene_style") or "").strip(),
        str(scene.get("key_light_direction") or "").strip(),
    )):
        return "", "", ""
    seed = stable_hash(
        source_record_id or product.get("product_id"), product_asset_fingerprint,
        scene.get("scene_id"), styling.get("styling_id"), persona.get("persona_id"),
        "indoor-scene-variant-v1", length=16,
    )
    number = int(seed, 16)
    scene_refs = normalized_list(scene.get("reference_images"))
    backgrounds = normalized_list(scene.get("background_type_pool")) or ["极简暖白墙"]
    background = "场景参考图中的浅色主背景" if scene_refs else backgrounds[number % len(backgrounds)]

    decor_count = str(scene.get("decor_count") or "1件").strip()
    decor_pool = [item for item in normalized_list(scene.get("edge_decor_pool")) if item != "不放装饰"]
    decor = "不放装饰"
    if decor_count != "不放" and decor_pool:
        count = 1
        if decor_count == "最多2件" and len(decor_pool) > 1:
            count = 1 + ((number >> 5) % 2)
        start = (number >> 9) % len(decor_pool)
        selected = [decor_pool[(start + offset) % len(decor_pool)] for offset in range(count)]
        configured_position = str(scene.get("decor_position") or "系统自动边缘").strip()
        if configured_position == "系统自动边缘":
            positions = (
                ["左侧画面边缘", "右侧背景转角"]
                if (number >> 13) % 2 == 0 else ["右侧画面边缘", "左侧背景转角"]
            )
        else:
            positions = [{
                "左边缘": "左侧画面边缘",
                "右边缘": "右侧画面边缘",
                "背景转角": "背景转角",
            }.get(configured_position, configured_position)]
        descriptions: list[str] = []
        for index, item in enumerate(selected):
            position = positions[index % len(positions)]
            measure = "一盏" if "灯" in item else "一盆"
            descriptions.append(f"{position}{measure}{item}")
        decor = "、".join(descriptions)

    configured_light = str(scene.get("key_light_direction") or "系统左右轮换").strip()
    light = (
        ("左前方45°" if int(variant_index) % 2 == 0 else "右前方45°")
        if configured_light == "系统左右轮换" else configured_light
    )
    return background, decor, light


def _resolve_inner_layer(
    product: dict[str, Any],
    scene: dict[str, Any],
    styling: dict[str, Any],
    persona: dict[str, Any],
    *,
    source_record_id: str,
    product_asset_fingerprint: str,
) -> tuple[str, str, str]:
    if str(product.get("category") or "").strip().lower() != "outerwear":
        return "", "", ""
    custom_type = str(styling.get("inner_type") or "").strip()
    custom_color = str(styling.get("inner_color") or "").strip()
    product_text = " ".join(
        str(product.get(key) or "") for key in ("product_name", "product_title", "sub_category", "notes")
    ).lower()
    light_signals = ("白", "米", "奶油", "燕麦", "浅", "white", "cream", "beige", "light")
    dark_signals = ("黑", "深", "藏青", "black", "dark", "navy", "charcoal")
    if any(signal in product_text for signal in light_signals):
        colors = ["黑色", "暖灰色"]
    elif any(signal in product_text for signal in dark_signals):
        colors = ["白色", "燕麦色", "浅灰色"]
    else:
        colors = ["黑色", "暖灰色"]
    seed = stable_hash(
        source_record_id or product.get("product_id"),
        product_asset_fingerprint,
        scene.get("scene_id"),
        styling.get("styling_id"),
        persona.get("persona_id"),
        "outerwear-inner-v1",
        length=16,
    )
    color = custom_color or colors[int(seed, 16) % len(colors)]
    inner_type = custom_type or "简洁修身纯色圆领打底衫"
    return inner_type, color, "锁定首帧开合状态，视频中不得拉开、拉合或增减扣件"


def _display_value(value: Any) -> str:
    return SCENE_VALUE_DISPLAY.get(str(value or "").strip(), str(value or "").strip())


def _build_scene_prompt(
    scene: dict[str, Any],
    *,
    resolved_background_type: str = "",
    resolved_edge_decor: str = "",
    resolved_key_light_direction: str = "",
) -> str:
    """Prefer the latest Feishu structured facts over a potentially stale prose field."""
    source_payload = scene.get("source_payload") if isinstance(scene.get("source_payload"), dict) else {}
    facts: list[str] = []
    has_general_background = bool(normalized_list(scene.get("background_type_pool")))
    for backend, label in SCENE_STRUCTURED_FIELDS:
        if has_general_background and backend in LEGACY_ROOM_DETAIL_FIELDS:
            continue
        value = source_payload.get(backend)
        if value in (None, "", [], {}):
            value = scene.get(backend)
        if value in (None, "", [], {}):
            continue
        facts.append(f"{label}：{_display_value(value)}")
    if resolved_background_type:
        facts.append(f"实际主背景：{resolved_background_type}")
    if resolved_edge_decor:
        facts.append(f"实际边缘装饰：{resolved_edge_decor}")
    if resolved_key_light_direction:
        facts.append(f"实际主光方向：{resolved_key_light_direction}大面积斜侧柔光，正面少量柔和补光")
    if not facts:
        return str(scene.get("prompt_core") or scene.get("scene_name") or "").strip().rstrip("。")
    result = (
        f"{scene.get('scene_name') or '目标场景'}。以下结构化场景事实为最高基准，"
        f"其他文字如有冲突一律忽略：{'；'.join(facts)}。整体真实生活化、非商业棚拍"
    )
    consistency = str(scene.get("consistency_prompt") or "").strip().rstrip("。")
    negative = str(scene.get("prompt_negative") or "").strip().rstrip("。")
    if consistency:
        result += f"。一致性要求：{consistency}"
    if negative:
        result += f"。禁止项：{negative}"
    return result


def _build_scene_display(
    scene: dict[str, Any],
    *,
    resolved_background_type: str,
    resolved_edge_decor: str,
    resolved_key_light_direction: str,
) -> str:
    if not resolved_background_type:
        return ""
    name = str(scene.get("scene_name") or "室内场景").strip()
    style = str(scene.get("scene_style") or "INS奶油风").strip()
    headline = name if style and style in name else "，".join(item for item in (name, style) if item)
    cleanliness = str(scene.get("background_cleanliness") or "极简").strip()
    decor = (
        "不放置额外装饰"
        if resolved_edge_decor == "不放装饰"
        else f"{resolved_edge_decor}，装饰仅位于画面边缘且不遮挡服装轮廓"
    )
    return (
        f"{headline}，{cleanliness}干净背景；主背景采用{resolved_background_type}；{decor}；"
        f"{resolved_key_light_direction}大面积斜侧柔光，正面少量柔和补光；"
        "整体浅暖白、明亮通透但不偏黄，服装受光均匀，面料纹理和商品原色清楚"
    )


def _framing_contract(product: dict[str, Any], styling: dict[str, Any]) -> tuple[str, str, str]:
    if str(product.get("category") or "").strip().lower() in UPPER_GARMENT_CATEGORIES:
        return (
            "upper_body_focus_with_partial_bottom",
            "以上装为绝对视觉主体，人物从头顶展示到大腿中段附近；上装占人物展示区域约60%，"
            "完整露出领口、肩线、袖子、门襟、口袋和下摆。下装只露出腰头、胯部和大腿上半截，"
            "用于说明搭配，不展示完整裤腿、脚部或鞋子。",
            "upper_garment_dominant_partial_bottom_visible",
        )
    visibility = str(styling.get("footwear_visibility") or "").strip()
    if visibility in {"not_required", "不要求入镜", "不入镜"}:
        return (
            "head_to_trouser_hem",
            "人物从头部到裤脚完整展示，画面下沿裁在裤脚或脚踝附近，鞋子不得入镜；"
            "不要为了隐藏鞋子而裁掉下装的主要版型。",
            "outfit_visible_to_trouser_hem",
        )
    if visibility in {"required", "必须入镜"}:
        return "full_body_with_shoes", "人物全身和鞋子完整入镜。", "full_body_with_shoes_visible"
    return "full_body", "人物全身构图，鞋子可自然入镜但不是展示重点。", "full_body_visible"


def _build_styling_prompt(
    styling: dict[str, Any],
    *,
    bottom_type: str,
    resolved_bottom_color: str,
    resolved_bottom_fit: str,
) -> str:
    parts = [f"商品上装搭配一件无图案的{resolved_bottom_color}{bottom_type}"]
    if resolved_bottom_fit:
        parts.append(f"下装同时满足{resolved_bottom_fit}版型")
    vibe = "、".join(normalized_list(styling.get("vibe_tag")))
    if vibe:
        parts.append(f"整体风格为{vibe}")
    accessory = str(styling.get("accessory_level") or "").strip()
    accessory_display = {"none": "不加配饰", "minimal": "配饰极少", "normal": "配饰不过度抢眼"}.get(accessory, accessory)
    if accessory_display:
        parts.append(accessory_display)
    parts.append("搭配只辅助展示比例，不改变、不遮挡主商品结构")
    return "；".join(parts)


def build_outfit_request(
    product: dict[str, Any],
    scene: dict[str, Any],
    styling: dict[str, Any],
    persona: dict[str, Any],
    *,
    resolved_bottom_color: str = "",
    resolved_bottom_fit: str = "",
    resolved_inner_type: str = "",
    resolved_inner_color: str = "",
    resolved_outerwear_state: str = "",
    inner_requirements: str = "",
    resolved_background_type: str = "",
    resolved_edge_decor: str = "",
    resolved_key_light_direction: str = "",
    allow_scene_text_fallback: bool = True,
) -> dict[str, Any]:
    product_refs = normalized_list(product.get("product_images"))
    scene_refs = scene.get("reference_images") if isinstance(scene.get("reference_images"), list) else normalized_list(scene.get("reference_images"))
    persona_refs = persona.get("reference_images") if isinstance(persona.get("reference_images"), list) else normalized_list(persona.get("reference_images"))
    if not product_refs:
        raise ValueError("生成产品穿搭图需要商品参考图")
    bottom_type_id = str(styling.get("bottom_type") or "").strip()
    bottom_type = BOTTOM_TYPE_DISPLAY.get(bottom_type_id, bottom_type_id)
    framing, framing_rule, framing_qc = _framing_contract(product, styling)
    scene_prompt = _build_scene_prompt(
        scene,
        resolved_background_type=resolved_background_type,
        resolved_edge_decor=resolved_edge_decor,
        resolved_key_light_direction=resolved_key_light_direction,
    )
    scene_display = _build_scene_display(
        scene,
        resolved_background_type=resolved_background_type,
        resolved_edge_decor=resolved_edge_decor,
        resolved_key_light_direction=resolved_key_light_direction,
    )
    persona_prompt = str(persona.get("prompt_core") or persona.get("persona_name") or "").strip().rstrip("。")
    styling_prompt = _build_styling_prompt(
        styling,
        bottom_type=bottom_type,
        resolved_bottom_color=resolved_bottom_color,
        resolved_bottom_fit=resolved_bottom_fit,
    )
    resolved_rules = (
        f"本视觉方案的下装颜色已由系统确定为“{resolved_bottom_color}”，只能使用该颜色，"
        "禁止改用候选池中的其他颜色。"
    )
    if bottom_type:
        resolved_rules += f"下装类型固定为“{bottom_type}”。"
    if resolved_bottom_fit:
        resolved_rules += (
            f"下装版型同时满足“{resolved_bottom_fit}”；这些标签是同一件下装的累计要求，"
            "不是多个可选款。"
        )
    inner_rules = ""
    if resolved_inner_type and resolved_inner_color:
        inner_rules = (
            f"外套内搭固定为“{resolved_inner_color}{resolved_inner_type}”，修身、无图案、无Logo、无帽、无高领，"
            "不得遮挡外套领口、门襟、口袋或下摆。"
        )
    if resolved_outerwear_state:
        inner_rules += f"外套开合规则：{resolved_outerwear_state}。"
    custom_inner_requirements = (
        str(inner_requirements or "").strip().rstrip("。")
        if resolved_inner_type and resolved_inner_color else ""
    )
    if custom_inner_requirements:
        inner_rules += f"内搭补充要求：{custom_inner_requirements}。"
    prompt = (
        f"生成一张真实女装试穿首帧，竖屏9:16。{framing_rule}"
        f"场景必须遵循：{scene_prompt}。"
        f"人物必须遵循：{persona_prompt}。"
        f"搭配必须遵循：{styling_prompt}。"
        f"{resolved_rules}"
        f"{inner_rules}"
        "商品以商品参考图为最高事实基准，颜色、领口、袖型、长度、面料表面和版型不得改变；"
        "手机完整遮脸，光线明亮通透但不过曝，不出现文字、字幕、贴纸、Logo或水印。"
    )
    references: list[dict[str, Any]] = []
    for role, values in (("product_truth", product_refs), ("persona_identity", persona_refs), ("scene_truth", scene_refs)):
        for index, value in enumerate(values, start=1):
            references.append({"role": role, "value": value, "priority": index})
    return {
        "protocol_version": "1.0",
        "builder_version": OUTFIT_BUILDER_VERSION,
        "mode": "product_outfit_image",
        "ratio": "9:16",
        "framing": framing,
        "prompt": prompt,
        "references": references,
        "resolved_styling": {
            "bottom_type": bottom_type,
            "bottom_type_id": bottom_type_id,
            "bottom_color": resolved_bottom_color,
            "bottom_fit": normalized_list(resolved_bottom_fit.replace("、", ",")),
            "inner_type": resolved_inner_type,
            "inner_color": resolved_inner_color,
            "outerwear_state": resolved_outerwear_state,
            "inner_requirements": custom_inner_requirements,
        },
        "resolved_context": {
            "scene": scene_prompt,
            "scene_display": scene_display,
            "persona": persona_prompt,
            "styling": styling_prompt,
            "inner_layer": inner_rules.rstrip("。"),
        },
        "scene_reference_mode": "image" if scene_refs else "text_fallback",
        "qc_requirements": [
            "product_fidelity", "styling_match", "scene_match", "persona_match", "phone_covers_face",
            framing_qc, "bright_color_accurate", "no_text_logo_watermark",
            "background_matches_resolved_type", "edge_decor_does_not_overlap_garment",
            "oblique_soft_light_preserves_fabric_texture", "bright_neutral_warm_without_yellow_cast",
        ],
    }


def orchestrate_visual_plans(
    db: LightTryonDB,
    product_id: str,
    *,
    source_record_id: str,
    scene_values: Any,
    styling_values: Any,
    per_plan_video_count: int,
    allow_scene_text_fallback: bool = True,
) -> list[dict[str, Any]]:
    if int(per_plan_video_count) not in {0, 1, 5}:
        raise ValueError("每方案视频数量只能是 0、1、5")
    product = db.get_product(product_id)
    if not product:
        raise KeyError(f"找不到商品: {product_id}")
    scenes = resolve_template_selection(db, "scene", scene_values, product)
    styles = resolve_template_selection(db, "styling", styling_values, product)
    total = len(scenes) * len(styles)
    if total > MAX_VISUAL_PLANS:
        raise ValueError(f"本次组合会生成 {total} 个视觉方案，超过上限 {MAX_VISUAL_PLANS}；请减少场景或搭配选择")
    persona = _select_persona(db, product)
    product_asset_fingerprint = _asset_fingerprint(product.get("product_images"))
    active: list[dict[str, Any]] = []
    for scene in scenes:
        for styling_index, styling in enumerate(styles):
            resolved_bottom_color, resolved_bottom_fit = _resolve_styling_variant(
                product,
                scene,
                styling,
                persona,
                source_record_id=source_record_id,
                product_asset_fingerprint=product_asset_fingerprint,
            )
            resolved_background_type, resolved_edge_decor, resolved_key_light_direction = _resolve_scene_variant(
                product,
                scene,
                styling,
                persona,
                source_record_id=source_record_id,
                product_asset_fingerprint=product_asset_fingerprint,
                variant_index=styling_index,
            )
            resolved_inner_type, resolved_inner_color, resolved_outerwear_state = _resolve_inner_layer(
                product,
                scene,
                styling,
                persona,
                source_record_id=source_record_id,
                product_asset_fingerprint=product_asset_fingerprint,
            )
            request = build_outfit_request(
                product, scene, styling, persona,
                resolved_bottom_color=resolved_bottom_color,
                resolved_bottom_fit=resolved_bottom_fit,
                resolved_inner_type=resolved_inner_type,
                resolved_inner_color=resolved_inner_color,
                resolved_outerwear_state=resolved_outerwear_state,
                inner_requirements=str(styling.get("inner_requirements") or ""),
                resolved_background_type=resolved_background_type,
                resolved_edge_decor=resolved_edge_decor,
                resolved_key_light_direction=resolved_key_light_direction,
                allow_scene_text_fallback=allow_scene_text_fallback,
            )
            fingerprint = stable_hash(
                product_asset_fingerprint,
                product.get("shot_plan_id") or "AUTO",
                sorted(normalized_list(product.get("recommended_action_pool"))),
                scene["scene_id"], scene.get("config_version"), scene.get("source_hash"), _asset_fingerprint(scene.get("reference_images")),
                styling["styling_id"], styling.get("config_version"), styling.get("source_hash"),
                resolved_bottom_color, resolved_bottom_fit,
                resolved_inner_type, resolved_inner_color, resolved_outerwear_state,
                resolved_background_type, resolved_edge_decor, resolved_key_light_direction,
                persona["persona_id"], persona.get("config_version"), _persona_visual_source_hash(persona),
                OUTFIT_BUILDER_VERSION,
                length=24,
            )
            base = safe_slug(source_record_id or product_id, 32)
            visual_plan_id = f"LTVP_{base}_{safe_slug(scene['scene_id'], 20)}_{safe_slug(styling['styling_id'], 20)}_{fingerprint[:8]}"
            existing = db.get_visual_plan(visual_plan_id)
            row = db.upsert_visual_plan({
                "visual_plan_id": visual_plan_id,
                "source_record_id": source_record_id,
                "product_id": product_id,
                "product_code": product.get("source_product_code") or product_id,
                "product_name": product.get("product_name") or "",
                "product_images": product.get("product_images") or [],
                "scene_id": scene["scene_id"],
                "scene_name": scene.get("scene_name") or scene["scene_id"],
                "styling_id": styling["styling_id"],
                "styling_name": styling.get("styling_name") or styling["styling_id"],
                "resolved_bottom_color": resolved_bottom_color,
                "resolved_bottom_fit": resolved_bottom_fit,
                "resolved_inner_type": resolved_inner_type,
                "resolved_inner_color": resolved_inner_color,
                "resolved_outerwear_state": resolved_outerwear_state,
                "resolved_background_type": resolved_background_type,
                "resolved_edge_decor": resolved_edge_decor,
                "resolved_key_light_direction": resolved_key_light_direction,
                "persona_id": persona["persona_id"],
                "per_plan_video_count": int(per_plan_video_count),
                "plan_version": int((existing or {}).get("plan_version") or 1),
                "plan_fingerprint": fingerprint,
                "plan_status": "active" if per_plan_video_count else "disabled",
                "outfit_image_status": (existing or {}).get("outfit_image_status") or "pending",
                "outfit_request_payload": request,
                "error_message": "",
                "superseded_at": "",
            })
            active.append(row)
    db.supersede_visual_plans(source_record_id, [row["visual_plan_id"] for row in active])
    return active


def confirm_outfit_image(
    db: LightTryonDB,
    visual_plan_id: str,
    *,
    image_path: str = "",
    image_url: str = "",
    image_version: str = "",
    feedback: str = "",
    qc_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return db.set_visual_plan_outfit(
        visual_plan_id,
        status="confirmed",
        image_path=image_path,
        image_url=image_url,
        image_version=image_version or f"confirmed-{stable_hash(image_path, image_url, length=10)}",
        feedback=feedback,
        qc_result=qc_result,
    )


def run_outfit_generation_worker(
    db: LightTryonDB,
    command: str,
    *,
    visual_plan_ids: Iterable[str] | None = None,
    limit: int = 1,
    timeout: int = 900,
    provider: str = "command",
    allow_scene_text_fallback: bool = False,
) -> dict[str, Any]:
    from .workers import run_json_command

    selected = {str(item) for item in (visual_plan_ids or []) if str(item)}
    plans = [
        row for row in db.list_visual_plans(plan_status="active")
        if row.get("outfit_image_status") in {"pending", "regenerate", "failed"}
        and (not selected or row["visual_plan_id"] in selected)
    ][: int(limit)]
    result: dict[str, Any] = {
        "claimed": len(plans), "auto_confirmed": 0, "created_jobs": 0, "failed": 0, "items": [],
    }
    for plan in plans:
        plan_id = plan["visual_plan_id"]
        request = plan.get("outfit_request_payload") or {}
        try:
            db.set_visual_plan_outfit(plan_id, status="generating")
            response = run_json_command(command, {"visual_plan": plan, "request": request}, timeout=timeout)
            image_path = str(response.get("output_image_path") or "").strip()
            image_url = str(response.get("output_image_url") or "").strip()
            if image_path and not Path(image_path).expanduser().is_file():
                raise ValueError(f"首帧 worker 返回的图片不存在: {image_path}")
            if not image_path and not image_url:
                raise ValueError("首帧 worker 必须返回 output_image_path 或 output_image_url")
            db.record_visual_plan_attempt(
                plan_id, provider=provider, status="success", request_payload=request, response_payload=response,
            )
            db.set_visual_plan_outfit(
                plan_id,
                status="confirmed",
                image_path=str(Path(image_path).expanduser().resolve()) if image_path else "",
                image_url=image_url,
                image_version=str(response.get("image_version") or stable_hash(response, length=12)),
                feedback="系统自动放行；图片仅作视频视觉参考与事后抽检，不再等待人工确认。",
                qc_result={"review_mode": "post_generation_audit", "auto_released": True},
            )
            job_result = create_confirmed_video_jobs(db, plan_id)
            result["auto_confirmed"] += 1
            result["created_jobs"] += int(job_result.get("created") or 0)
            result["items"].append({
                "visual_plan_id": plan_id,
                "status": "confirmed",
                "created_jobs": int(job_result.get("created") or 0),
            })
        except Exception as exc:
            db.record_visual_plan_attempt(
                plan_id, provider=provider, status="failed", request_payload=request, error=str(exc),
            )
            db.set_visual_plan_outfit(plan_id, status="failed", error=str(exc))
            result["failed"] += 1
            result["items"].append({"visual_plan_id": plan_id, "status": "failed", "error": str(exc)})
    return result


def create_confirmed_video_jobs(db: LightTryonDB, visual_plan_id: str) -> dict[str, Any]:
    visual = db.get_visual_plan(visual_plan_id)
    if not visual:
        raise KeyError(f"找不到视觉方案: {visual_plan_id}")
    if visual.get("plan_status") != "active":
        raise ValueError(f"视觉方案 {visual_plan_id} 不是启用状态")
    if visual.get("outfit_image_status") != "confirmed":
        raise ValueError(f"视觉方案 {visual_plan_id} 的产品穿搭图尚未确认，不能创建视频任务")
    count = int(visual.get("per_plan_video_count") or 0)
    if count not in {1, 5}:
        return {"visual_plan_id": visual_plan_id, "created": 0, "existing": 0, "job_ids": []}
    plan_version = f"visual-{visual_plan_id}-v{int(visual.get('plan_version') or 1)}"
    planned = plan_product(
        db,
        visual["product_id"],
        count=count,
        plan_version=plan_version,
        scene_ids=[visual["scene_id"]],
        styling_ids=[visual["styling_id"]],
    )
    image_path = str(visual.get("outfit_image_path") or "")
    image_url = str(visual.get("outfit_image_url") or "")
    jobs: list[PlannedJob] = [
        replace(
            job,
            source_script_record_id=str(visual.get("source_record_id") or ""),
            visual_plan_id=visual_plan_id,
            outfit_image_path=image_path,
            outfit_image_url=image_url,
            outfit_image_version=str(visual.get("outfit_image_version") or ""),
            legacy_job=False,
        )
        for job in planned
    ]
    result = db.create_jobs(jobs)
    stored = [job for job in db.list_jobs(product_id=visual["product_id"]) if job.get("visual_plan_id") == visual_plan_id]
    for job in stored:
        context = db.get_job_context(job["job_id"])
        current_brand_plan = build_brand_plan(context["persona"], context["product"])
        if (
            job.get("prompt_version") != PROMPT_BUILDER_VERSION
            or not job.get("prompt_payload")
            or (job.get("prompt_payload") or {}).get("brand_plan") != current_brand_plan
        ):
            prompt = build_prompt(context)
            db.update_prompt(job["job_id"], prompt, PROMPT_BUILDER_VERSION)
    stored = [job for job in db.list_jobs(product_id=visual["product_id"]) if job.get("visual_plan_id") == visual_plan_id]
    job_ids = [job["job_id"] for job in stored]
    db.upsert_visual_plan({**visual, "job_ids": job_ids, "updated_at": now_iso()})
    return {"visual_plan_id": visual_plan_id, **result, "job_ids": job_ids}
