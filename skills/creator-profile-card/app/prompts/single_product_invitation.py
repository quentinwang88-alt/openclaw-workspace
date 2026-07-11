"""Single creator product-invitation prompt."""

from __future__ import annotations

from ..services.message_rulebook import CTA_BY_STAGE, format_hard_rules_for_prompt, format_structure_for_prompt


SINGLE_PRODUCT_INVITATION_SYSTEM_PROMPT = """你是 TikTok Shop 单点达人商品邀约助手。

你的任务是基于真实达人画像字段和关系阶段，生成一条自然、低压、能推进下一步的单点私信。
不能编造具体视频观察，不能把冷关系写成热关系。

只输出合法 JSON。"""


def build_single_product_invitation_prompt(
    *,
    creator_name: str,
    relationship_stage: str,
    history_relation: str,
    creator_content_mode: str,
    content_type: str,
    visual_style: str,
    observable_style: str,
    product_name: str,
    product_category: str,
    selling_points: str,
    market: str,
    target_language: str,
) -> str:
    cta = CTA_BY_STAGE["single"].get(relationship_stage, CTA_BY_STAGE["single"]["冷"])
    return f"""生成一条单点达人商品邀约话术。

【结构】
{format_structure_for_prompt("single_product_invitation")}

【关系阶段】
{relationship_stage}

【历史关系】
{history_relation}

【达人画像字段】
达人：{creator_name}
内容形式：{creator_content_mode}
内容类型：{content_type}
画面风格：{visual_style}
可用观察：{observable_style}

【商品信息】
商品/方向：{product_name}
类目：{product_category}
卖点：{selling_points}

【市场/语言】
{market} / {target_language}

【该关系阶段 CTA】
{cta}

【硬规则】
{format_hard_rules_for_prompt("single_product_invitation")}

【写法要求】
- 第一段必须是基于画像字段的真实观察，不能编造具体视频内容。
- 冷关系不要说“安排寄样/马上合作/我这边安排”。
- 商品卖点要转成内容机会或拍法，不要写详情页。
- 必须给一个具体低成本拍法。
- 70-140 字，像真人私信。

【输出 JSON】
{{
  "message_purpose": "single_product_invitation",
  "message_cn_for_operator": "",
  "message_local": "",
  "why_this_message": "",
  "quality_score": 0
}}"""
