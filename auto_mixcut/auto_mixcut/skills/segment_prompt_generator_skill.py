from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from auto_mixcut.core.result import Result

from .ai_segment_factory_config import AISegmentFactoryConfig, SegmentTypeRule, get_config
from .context import SkillContext


SEGMENT_TYPE_CN: Dict[str, str] = {
    "product_display": "商品桌面展示",
    "handheld_product": "手拿商品",
    "detail_atmosphere": "细节氛围",
    "tryon_result": "佩戴/上身效果",
    "mirror_routine": "镜前整理",
    "home_lifestyle": "居家生活场景",
    "before_go_out": "出门前场景",
    "seasonal_scene": "季节/场景氛围",
    "product_still": "纯物静物",
    "unboxing": "拆包装",
    "flatlay": "平铺摆拍",
}


class SegmentPromptGeneratorSkill:
    def __init__(self, ctx: SkillContext, config: Optional[AISegmentFactoryConfig] = None):
        self.ctx = ctx
        self.config = config or get_config()

    def generate(self, product_id: str, segment_type: str, scene_preference: str = "", style_preference: str = "", character_requirement: str = "") -> Result:
        rule = self.config.get_segment_type_rule(segment_type)
        if not rule.possible_roles:
            return Result.fail("UNKNOWN_SEGMENT_TYPE", f"unknown segment type: {segment_type}", {"product_id": product_id, "segment_type": segment_type})

        product = self.ctx.repo.get("products", "product_id", product_id)
        if not product:
            return Result.fail("PRODUCT_NOT_FOUND", "product not found", {"product_id": product_id})

        anchor = product.get("product_anchor_json") or {}
        anchor_data = anchor if isinstance(anchor, dict) else {}
        category = product.get("category", "")
        anchor_version = product.get("anchor_version", "")

        scene = scene_preference or rule.prompt_scene_default
        action = rule.prompt_action_default
        style = style_preference or "真实日常"
        if segment_type in {"product_still", "flatlay"}:
            character = character_requirement or "无人物，无人手，只拍产品、包装或陈列"
        elif segment_type == "unboxing":
            character = character_requirement or "只允许手部开箱，无面部无身体"
        else:
            character = character_requirement or "年轻女性，真实日常风格，可侧脸或不露脸"
        anchor_text = json.dumps(anchor_data, ensure_ascii=False) if anchor_data else ""

        use_refinement = not self.ctx.settings.mock_llm and anchor_data and anchor_version.startswith("p1_")

        if use_refinement:
            from .llm_router_skill import LLMRouterSkill
            router = LLMRouterSkill(self.ctx)
            ref_res = router.call(
                "segment_prompt_refinement",
                {
                    "anchor_json": anchor_text,
                    "segment_type": segment_type,
                    "segment_type_cn": SEGMENT_TYPE_CN.get(segment_type, segment_type),
                    "category": category,
                    "prompt_version": "v1.0",
                },
                product_id=product_id,
            )
            if ref_res.success:
                refined = ref_res.data.get("response", {})
                prompt = _build_refined_prompt(
                    rule=rule, segment_type=segment_type,
                    visual_description=str(refined.get("visual_description") or ""),
                    key_anchor_points=refined.get("key_anchor_points") or [],
                    scene_description=str(refined.get("scene_description") or ""),
                    forbidden_items=refined.get("forbidden_items") or [],
                    character=character, action=action, style=style,
                )
                return Result.ok({"product_id": product_id, "segment_type": segment_type, "prompt": prompt, "refined_by_llm": True, "rule": {"risk_level": rule.risk_level, "default_roles": rule.default_roles, "core_allowed": str(rule.core_allowed)}})

        prompt = _build_structured_prompt(
            rule=rule, segment_type=segment_type,
            category=category, anchor_data=anchor_data, anchor_text=anchor_text,
            scene=scene, action=action, style=style, character=character,
            category_forbidden=self.config.get_category_forbidden(category),
        )

        return Result.ok({"product_id": product_id, "segment_type": segment_type, "prompt": prompt, "refined_by_llm": False, "rule": {"risk_level": rule.risk_level, "default_roles": rule.default_roles, "core_allowed": str(rule.core_allowed)}})


def _build_structured_prompt(
    rule: SegmentTypeRule, segment_type: str,
    category: str, anchor_data: Dict[str, Any], anchor_text: str,
    scene: str, action: str, style: str, character: str,
    category_forbidden: List[str],
) -> str:
    global_rules = get_config().global_rules
    output_rules = global_rules.get("output", {})
    global_avoid = output_rules.get("avoid", [])

    critical_points = anchor_data.get("critical_points") or anchor_data.get("hard_anchors") or []
    high_points = anchor_data.get("high_importance_points") or anchor_data.get("display_anchors") or []
    product_forbidden = anchor_data.get("forbidden_mismatch") or []

    product_forbidden_texts: List[str] = []
    for pf in product_forbidden:
        if isinstance(pf, str):
            product_forbidden_texts.append(pf)
        elif isinstance(pf, dict):
            product_forbidden_texts.append(pf.get("anchor", pf.get("description", str(pf))))
    product_forbidden_merged = list(dict.fromkeys(product_forbidden_texts))[:5]

    all_forbidden = list(dict.fromkeys(
        [f"禁止{a}" for a in global_avoid]
        + ["禁止字幕", "禁止文字", "禁止logo", "禁止水印"]
        + [f"禁止{f}" for f in category_forbidden]
        + [f"禁止{f}" for f in product_forbidden_merged]
    ))[:10]

    critical_lines = "\n".join([f"- {_anchor_text(c)}" for c in (critical_points[:3] if critical_points else [])]) or "- 商品主体形状和关键结构必须可见"
    high_lines = "\n".join([f"- {_anchor_text(h)}" for h in (high_points[:3] if high_points else [])]) or "- 材质和颜色可见"

    return f"""生成一个 {output_rules.get('duration_seconds', [2,5])[0]}-{output_rules.get('duration_seconds', [2,5])[1]} 秒{output_rules.get('aspect_ratio', '9:16')}竖屏短视频片段。

片段目标：
用于 TikTok Shop 商品混剪素材，片段类型为 {SEGMENT_TYPE_CN.get(segment_type, segment_type)}，{rule.prompt_description}
风险等级：{rule.risk_level} | 镜位：{', '.join(rule.default_roles)}

关键锚点（必须保留）：
{critical_lines}

重要锚点（尽量保留）：
{high_lines}

场景：
{scene}

动作：
{action}

人物：
{character}

风格：
{style}，{output_rules.get('style', 'natural TikTok UGC')}风格。

禁止：
{', '.join(all_forbidden)}。

输出：
单镜头，{output_rules.get('duration_seconds', [2,5])[0]}-{output_rules.get('duration_seconds', [2,5])[1]} 秒，{output_rules.get('aspect_ratio', '9:16')}，{output_rules.get('style', 'natural TikTok UGC')}风格，无字幕无BGM。"""


def _build_refined_prompt(
    rule: SegmentTypeRule, segment_type: str,
    visual_description: str, key_anchor_points: List[str],
    scene_description: str, forbidden_items: List[str],
    character: str, action: str, style: str,
) -> str:
    anchor_bullets = "\n".join([f"- {p}" for p in key_anchor_points]) if key_anchor_points else ""
    forbidden_text = ", ".join(forbidden_items) if forbidden_items else "禁止字幕, 禁止水印, 禁止logo, 禁止广告感"

    return f"""生成一个 2-3 秒竖屏短视频片段。

画面：
{visual_description}

场景：
{scene_description}

商品关键锚点：
{anchor_bullets}

动作：
{action}

人物：
{character}

风格：
{style}，TikTok UGC 风格。

禁止：
{forbidden_text}。

输出：
单镜头，2-5 秒，9:16 竖屏，真实自然，无字幕无 BGM。"""


def _anchor_text(item: Any) -> str:
    if isinstance(item, dict):
        return item.get("anchor", item.get("description", str(item)))
    return str(item)
