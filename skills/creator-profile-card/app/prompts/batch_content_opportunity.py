"""Batch content-opportunity outreach prompt."""

from __future__ import annotations

from ..services.message_rulebook import format_hard_rules_for_prompt, format_structure_for_prompt


BATCH_CONTENT_OPPORTUNITY_SYSTEM_PROMPT = """你是 TikTok Shop 批量冷启动建联话术助手。

你的任务不是单点邀约，不要假装了解具体达人。
目标是让陌生/冷达人在 3 秒内理解：这是一个低成本、可快速测试、可能能补内容的合作方向。

只输出合法 JSON。"""


def build_batch_content_opportunity_prompt(
    *,
    product_direction: str,
    product_category: str,
    selling_points: str,
    market: str,
    target_language: str,
    local_hook: str = "",
    local_cta: str = "",
) -> str:
    return f"""生成一套批量冷启动建联话术。

【结构】
{format_structure_for_prompt("batch_content_opportunity")}

注意：这是软结构，不要求四段作文。必须有「内容机会钩子」和「轻 CTA」；低成本场景、产品方向可以合并精简。

【产品方向】
{product_direction}

【产品类目】
{product_category}

【图片推导卖点】
{selling_points}

【市场/语言】
{market} / {target_language}

【本地语言固定防线】
钩子句：{local_hook or "无"}
CTA 句：{local_cta or "无"}
如果提供了钩子句和 CTA 句，message_local 必须优先使用它们，不要自由改写成强推表达。

【硬规则】
{format_hard_rules_for_prompt("batch_content_opportunity")}

【写法要求】
- 先讲内容机会，不要先讲产品卖点。
- 批量场景短 > 全，中文 60-130 字。
- 不要出现达人名、@名字、Hi/Hello/哈喽开头。
- 不要说“适合你/适合你账号/我看了你的视频/你的风格”。
- 不要说“我这边安排/马上寄样/马上合作”。
- 多品时优先用类目统称，不要逐个硬翻复杂商品名。
- 卖点只保留 1-2 个，并转成“容易展示/适合内容呈现”的表达。
- CTA 目标是让达人回复“可以看看”，不是立刻推进合作。

【输出 JSON】
{{
  "message_purpose": "batch_content_opportunity",
  "message_cn_for_operator": "",
  "message_local": "",
  "why_this_message": "",
  "quality_score": 0
}}"""
