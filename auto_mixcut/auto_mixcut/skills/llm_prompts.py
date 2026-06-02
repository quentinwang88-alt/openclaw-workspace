from __future__ import annotations

import json
from typing import Any, Dict, List

from .context import SkillContext


def product_anchor_prompt(product_id: str, product_name: str, category: str, market: str) -> str:
    return f"""
请为以下商品生成混剪用的商品锚点卡 JSON。

商品信息：
- 商品ID：{product_id}
- 商品名称：{product_name}
- 市场：{market}
- 类目：{category}

请只返回 JSON，不要 markdown，不要解释：
{{
  "category": "{category}",
  "product_subtype": "{product_name}",
  "core_visual_points": ["商品核心视觉点1", "商品核心视觉点2", "..."],
  "must_not_change_points": ["混剪中绝对不能改变的识别点"],
  "forbidden_mismatch": ["禁止出现的错配项"],
  "strict_roles": ["hero", "detail", "result"],
  "allowed_scene_usage": true,
  "confidence": "high|medium|low",
  "reason": "中文简短说明"
}}

锚点卡要求：
- core_visual_points：3-5 个，商品最关键的视觉识别点（形状、材质、颜色、关键装饰）
- must_not_change_points：2-4 个，混剪中绝对不能变的识别特征
- forbidden_mismatch：2-5 个，绝对不能出现的错误匹配（其他类目商品、无关配饰等）
- strict_roles：该商品允许承担的核心混剪角色（可选 hero/detail/result）
""".strip()


def segment_tagging_prompt(product: dict, asset: dict, segment: dict) -> str:
    anchor = product.get("product_anchor_json") or {}
    return f"""
请根据连续抽帧判断这个 TikTok Shop 商品短视频片段的混剪用途。

商品信息：
- 商品ID：{product.get('product_id')}
- 商品名称：{product.get('product_name')}
- 市场：{product.get('market')}
- 类目：{product.get('category')}
- 商品锚点：{json.dumps(anchor, ensure_ascii=False)}

素材信息：
- source_type：{asset.get('source_type')}
- source_trust_level：{asset.get('source_trust_level')}
- product_binding_type：{asset.get('product_binding_type')}
- segment_id：{segment.get('segment_id')}

请只返回 JSON，不要 markdown，不要解释。字段和值必须严格使用下面枚举：
{{
  "primary_shot_role": "hero|detail|result|scene|ending|unusable",
  "secondary_roles": ["hero|detail|result|scene|ending"],
  "product_visibility": "high|medium|low",
  "hook_strength": "strong|medium|weak",
  "mixcut_usability": "yes|needs_processing|no",
  "risk_level": "low|medium|high",
  "confidence": "high|medium|low",
  "needs_human_review": true|false,
  "reason": "中文，简短说明判断依据"
}}

判断标准：
- hero：商品主体清楚、首屏能吸引人，适合开头。
- detail：商品材质、结构、局部细节清楚。
- result：佩戴/使用后效果清楚。
- scene：氛围、生活方式、背景场景，商品可以不是强主体。
- ending：适合收尾、定格、轻氛围。
- unusable：黑屏、严重模糊、商品不可见、明显错品、风险内容、水印/UI遮挡严重。
- 如果商品与锚点不确定、AI生成漂移、画面含平台水印/账号UI/明显搬运痕迹，应提高 risk_level 或 needs_human_review。
""".strip()


def consistency_prompt() -> str:
    return """
请检查这些连续帧中的商品是否跨帧保持一致，重点看商品形状、结构、关键装饰、数量、材质是否漂移。
只返回 JSON：
{
  "frame_consistency_score": 0-100,
  "frame_consistency_status": "pass|uncertain|fail",
  "frame_consistency_reason": "中文简短原因"
}
""".strip()


def watermark_prompt() -> str:
    return "请判断图片是否包含 TikTok/Douyin logo、平台 UI、用户ID、账号名或明显水印。只返回 JSON：{\"has_watermark\":\"yes|no|unknown\",\"confidence\":\"high|medium|low\",\"watermark_type\":\"TikTok|Douyin|platform_ui|user_id|other|none\",\"reason\":\"中文简短原因\"}"


def ai_anchor_check_prompt(product: dict, segment: dict) -> str:
    anchor = product.get("product_anchor_json") or {}
    return f"""
请根据连续抽帧，判断这个 AI 生成的商品片段是否可以用于混剪。

商品信息：
- 商品ID：{product.get('product_id')}
- 商品名称：{product.get('product_name')}
- 类目：{product.get('category')}

商品锚点：
{json.dumps(anchor, ensure_ascii=False)}

片段信息：
- 片段类型：{segment.get('segment_type') or 'unknown'}
- source_type：{segment.get('source_type')}

请只返回 JSON，不要 markdown，不要解释：
{{
  "anchor_match_level": "strict_pass|soft_pass|uncertain|fail",
  "product_category_correct": true|false,
  "core_visual_points_status": {{}},
  "forbidden_mismatch_detected": true|false,
  "forbidden_mismatch_reason": "如有就不匹配，简述原因；否则null",
  "distortion_risk": "low|medium|high",
  "allowed_core_roles": ["hero|detail|result"],
  "allowed_soft_roles": ["scene|ending"],
  "needs_human_review": true|false,
  "reason": "中文简短说明判断依据"
}}

判定标准：
- strict_pass：商品类别正确，关键识别点清楚，不违反 forbidden_mismatch，跨帧一致，可承担 hero/detail/result。
- soft_pass：商品方向大体正确，但细节不足以承担强商品展示，只能用于 scene/ending。
- uncertain：模型不确定，普通商品默认降级 scene/ending，高优先级商品需人工复核。
- fail：商品明显错了（类目错误、结构错误、核心视觉点消失），不能进入混剪。

注意：AI 生成素材容易在细节处失真，请重点关注商品形状、结构、关键装饰是否漂移。
""".strip()


def segment_prompt_refinement_prompt(anchor_json: str, segment_type: str, segment_type_cn: str, category: str) -> str:
    return f"""你是一个 TikTok Shop 商品视频 prompt 提炼助手。
请从商品锚点中提取与「{segment_type_cn}」({segment_type}) 片段类型最相关的视觉信息，生成精炼的视频生成 prompt 组件。

商品类目：{category}

商品锚点：
{anchor_json}

请只返回 JSON，不要 markdown，不要解释：
{{
  "visual_description": "一段 3 秒竖屏视频的英文描述，聚焦该片段类型需要的画面内容。如果此片段类型需要在画面中展示商品，必须包含商品关键视觉特征。120 词以内。",
  "key_anchor_points": ["3-5 个用于该片段的关键锚点要求，中文简短描述"],
  "scene_description": "该片段类型的场景和光线描述，英文。30 词以内。",
  "forbidden_items": ["必须禁止出现的元素，如字幕、水印、logo、广告感等，中文"]
}}

提炼原则：
- 如果片段类型是 product_display / handheld_product / detail_atmosphere / tryon_result：必须强调商品核心视觉点（结构、材质、颜色、关键装饰）
- 如果片段类型是 mirror_routine / home_lifestyle / before_go_out / seasonal_scene：强调氛围和生活感，商品可以自然出现但不强制
- 必须根据 category_execution_contract 中的 forbidden_actions 给出禁止项
- visual_description 必须包含 TikTok UGC 风格、9:16 竖屏、单镜头、2-5 秒这些硬约束
""".strip()


def normalize_segment_tag(data: Any) -> Dict[str, Any]:
    if not isinstance(data, dict):
        raise ValueError("segment tag response is not object")
    roles = {"hero", "detail", "result", "scene", "ending", "unusable"}
    vis = {"high", "medium", "low"}
    hooks = {"strong", "medium", "weak"}
    usability = {"yes", "needs_processing", "no"}
    risk = {"low", "medium", "high"}
    conf = {"high", "medium", "low"}
    primary = _enum(data.get("primary_shot_role"), roles, "unusable")
    secondary = [_enum(v, roles - {"unusable"}, "") for v in (data.get("secondary_roles") or [])]
    secondary = [v for v in secondary if v]
    return {
        "primary_shot_role": primary,
        "secondary_roles": secondary[:3],
        "product_visibility": _enum(data.get("product_visibility"), vis, "low"),
        "hook_strength": _enum(data.get("hook_strength"), hooks, "weak"),
        "mixcut_usability": _enum(data.get("mixcut_usability"), usability, "needs_processing"),
        "risk_level": _enum(data.get("risk_level"), risk, "medium"),
        "confidence": _enum(data.get("confidence"), conf, "low"),
        "needs_human_review": bool(data.get("needs_human_review")),
        "reason": str(data.get("reason") or "").strip()[:500],
    }


def normalize_consistency(data: Any) -> Dict[str, Any]:
    if not isinstance(data, dict):
        raise ValueError("consistency response is not object")
    score = float(data.get("frame_consistency_score") or 0)
    status = _enum(data.get("frame_consistency_status"), {"pass", "uncertain", "fail"}, "uncertain")
    return {"frame_consistency_score": max(0, min(100, score)), "frame_consistency_status": status, "frame_consistency_reason": str(data.get("frame_consistency_reason") or "")[:500]}


def normalize_anchor_check(data: Any) -> Dict[str, Any]:
    if not isinstance(data, dict):
        raise ValueError("anchor check response is not object")
    levels = {"strict_pass", "soft_pass", "uncertain", "fail"}
    roles = {"hero", "detail", "result"}
    soft_roles = {"scene", "ending"}
    core = data.get("allowed_core_roles") or []
    soft = data.get("allowed_soft_roles") or []
    return {
        "anchor_match_level": _enum(data.get("anchor_match_level"), levels, "uncertain"),
        "product_category_correct": bool(data.get("product_category_correct")),
        "core_visual_points_status": data.get("core_visual_points_status") or {},
        "forbidden_mismatch_detected": bool(data.get("forbidden_mismatch_detected")),
        "forbidden_mismatch_reason": str(data.get("forbidden_mismatch_reason") or "") if data.get("forbidden_mismatch_reason") else None,
        "distortion_risk": _enum(data.get("distortion_risk"), {"low", "medium", "high"}, "medium"),
        "allowed_core_roles": [r for r in core if r in roles],
        "allowed_soft_roles": [r for r in soft if r in soft_roles],
        "needs_human_review": bool(data.get("needs_human_review")),
        "reason": str(data.get("reason") or "").strip()[:500],
    }


def normalize_prompt_refinement(data: Any) -> Dict[str, Any]:
    if not isinstance(data, dict):
        raise ValueError("prompt refinement response is not object")
    return {
        "visual_description": str(data.get("visual_description") or "").strip()[:800],
        "key_anchor_points": (data.get("key_anchor_points") or [])[:5],
        "scene_description": str(data.get("scene_description") or "").strip()[:200],
        "forbidden_items": (data.get("forbidden_items") or [])[:10],
    }


def normalize_product_anchor(data: Any, category: str, product_name: str) -> Dict[str, Any]:
    if not isinstance(data, dict):
        raise ValueError("product anchor response is not object")
    return {
        "category": str(data.get("category") or category),
        "product_subtype": str(data.get("product_subtype") or product_name),
        "core_visual_points": (data.get("core_visual_points") or [])[:5],
        "must_not_change_points": (data.get("must_not_change_points") or [])[:4],
        "forbidden_mismatch": (data.get("forbidden_mismatch") or [])[:5],
        "strict_roles": (data.get("strict_roles") or ["hero", "detail", "result"])[:4],
        "allowed_scene_usage": bool(data.get("allowed_scene_usage", True)),
        "confidence": _enum(data.get("confidence"), {"high", "medium", "low"}, "medium"),
        "drafted_by": "llm_router",
        "reason": str(data.get("reason") or "").strip()[:500],
    }


BGM_ALLOWED_LABELS = {
    "mood_tags": [
        "cute_light", "daily_clean", "soft_feminine", "fashion_chic",
        "premium_clean", "warm_cozy", "winter_soft", "fresh_summer",
        "calm_lifestyle", "energetic", "minimal_clean",
    ],
    "energy_level": ["low", "medium", "high"],
    "vocal_type": ["instrumental", "light_vocal", "vocal", "unknown"],
    "category_tags": [
        "hair_accessories", "earrings", "womens_top", "womens_outerwear",
        "scarf_hat", "scarves_hats", "scarves", "generic_fashion",
    ],
    "template_tags": [
        "GENERAL_BALANCED_15S", "RESULT_FIRST_15S", "DETAIL_HOOK_15S", "CLEAN_PRODUCT_PROOF_15S",
        "AI_PRODUCT_FIRST_20S", "AI_LIFESTYLE_16S", "AI_DETAIL_PROOF_24S",
        "AI_HAIR_CONTRAST_REVEAL_20S", "AI_HAIR_LAZY_UPGRADE_16S", "AI_HAIR_LUCK_MOOD_20S",
        "AI_SCARF_PRODUCT_FIRST_20S", "AI_SCARF_LIFESTYLE_16S",
        "AI_EARRING_DETAIL_16S", "AI_EARRING_LIFESTYLE_20S",
        "AI_OUTERWEAR_PRODUCT_FIRST_20S", "AI_OUTERWEAR_LIFESTYLE_20S",
    ],
}


def bgm_tagging_prompt(payload: dict) -> str:
    track_name = payload.get("track_name") or "Unknown"
    artist_name = payload.get("artist_name") or "Unknown"
    source_platform = payload.get("source_platform") or "unknown"
    download_version = payload.get("download_version") or ""
    existing_human_tags = payload.get("existing_human_tags") or {}
    allowed_labels = payload.get("allowed_labels") or BGM_ALLOWED_LABELS
    import json

    return f"""你是一个 TikTok Shop 短视频混剪 BGM 标签助手。
请仔细听这段音频，根据实际听感判断该曲目的标签。

参考信息（不一定准确，以实际听感为准）：
- 曲名：{track_name}
- Artist：{artist_name}
- 平台：{source_platform}
- 下载版本：{download_version}
- 现有标签：{json.dumps(existing_human_tags or {}, ensure_ascii=False)}

允许的标签值：
{json.dumps(allowed_labels, ensure_ascii=False, indent=2)}

规则：
1. 只能从 allowed_labels 中选择标签。
2. mood_tags 最多 3 个，category_tags 最多 3 个，template_tags 最多 3 个。
3. 根据实际听感判断 vocal_type（instrumental / light_vocal / vocal / unknown）。
4. 能量高（鼓点强、节奏快、明显律动）→ high，中等（有节奏但不激烈）→ medium，舒缓（钢琴、环境音、lofi）→ low。
5. 不确定类目时至少返回 generic_fashion。
6. 不允许推断授权是否合法。
7. 普通 2 分钟左右 BGM 建议 default_volume=0.2，淡入 500ms，淡出 800ms。
8. 如果音频不完整或无法判断，tag_confidence 返回 low。

只输出 JSON，不要 markdown，不要解释：
{{
  "ai_suggested_tags": {{
    "mood_tags": ["daily_clean"],
    "energy_level": "medium",
    "vocal_type": "instrumental",
    "category_tags": ["generic_fashion"],
    "template_tags": []
  }},
  "mix_suggestions": {{
    "recommended_start_sec": 12,
    "default_volume": 0.2,
    "fade_in_ms": 500,
    "fade_out_ms": 800,
    "suitable_for_intro": true,
    "loop_friendly": false,
    "voiceover_friendly": true
  }},
  "tag_confidence": "high",
  "tag_review_required": false,
  "tag_diff_json": {{}},
  "reason": "基于实际音频听感判断"
}}""".strip()


def normalize_bgm_tag(data: Any) -> dict:
    if not isinstance(data, dict):
        raise ValueError("bgm tag response is not object")

    labels = BGM_ALLOWED_LABELS

    suggested = data.get("ai_suggested_tags") or {}
    mood_tags = [t for t in (suggested.get("mood_tags") or []) if t in labels["mood_tags"]][:3]
    energy = _enum(suggested.get("energy_level"), set(labels["energy_level"]), "medium")
    vocal = _enum(suggested.get("vocal_type"), set(labels["vocal_type"]), "unknown")
    category_tags = [t for t in (suggested.get("category_tags") or []) if t in labels["category_tags"]][:3]
    template_tags = [t for t in (suggested.get("template_tags") or []) if t in labels["template_tags"]][:3]

    mix = data.get("mix_suggestions") or {}

    return {
        "ai_suggested_tags": {
            "mood_tags": mood_tags or ["daily_clean"],
            "energy_level": energy,
            "vocal_type": vocal,
            "category_tags": category_tags or ["generic_fashion"],
            "template_tags": template_tags,
        },
        "mix_suggestions": {
            "recommended_start_sec": float(mix.get("recommended_start_sec", 12)),
            "default_volume": float(mix.get("default_volume", 0.2)),
            "fade_in_ms": int(mix.get("fade_in_ms", 500)),
            "fade_out_ms": int(mix.get("fade_out_ms", 800)),
            "suitable_for_intro": bool(mix.get("suitable_for_intro", True)),
            "loop_friendly": bool(mix.get("loop_friendly", False)),
            "voiceover_friendly": bool(mix.get("voiceover_friendly", True)),
        },
        "tag_confidence": _enum(data.get("tag_confidence"), {"high", "medium", "low"}, "low"),
        "tag_review_required": bool(data.get("tag_review_required", False)),
        "tag_diff_json": data.get("tag_diff_json") or {},
        "reason": str(data.get("reason") or "").strip()[:500],
    }


def _default_bgm_tag() -> dict:
    return {
        "ai_suggested_tags": {
            "mood_tags": ["daily_clean"],
            "energy_level": "medium",
            "vocal_type": "unknown",
            "category_tags": ["generic_fashion"],
            "template_tags": [],
        },
        "mix_suggestions": {
            "recommended_start_sec": 0,
            "default_volume": 0.2,
            "fade_in_ms": 500,
            "fade_out_ms": 800,
            "suitable_for_intro": True,
            "loop_friendly": False,
            "voiceover_friendly": True,
        },
        "tag_confidence": "low",
        "tag_review_required": False,
        "tag_diff_json": {},
        "reason": "default fallback: no data",
    }


def _enum(value: Any, allowed: set[str], default: str) -> str:
    text = str(value or "").strip()
    return text if text in allowed else default
