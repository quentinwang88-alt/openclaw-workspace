"""voc-insight LLM 表达润色 prompt 模板。

按 usecase 分系统提示；润色对象 = 一个确定性 insight + 它的证据样本。
LLM 只做表达润色，不许改 evidence_refs / confidence / usecase / decision 这些规则字段。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

# ── 系统提示（按 usecase 切语气）──────────────────────────────
SYSTEM_PROMPTS: Dict[str, str] = {
    "ads_mixcut": (
        "你是跨境电商投流视频证明动作表达顾问。把给定 VOC 洞察润色成能被短视频镜头证明的动作钩子。"
        "注意：usage_lane、proof_archetype、video_fit_score、required_beats 由规则层控制，你只做表达润色。"
        "标题中文必须指向可拍结果，本地语言口播像真人买家说话，"
        "广告 hook 给 2-3 条本地语言短句，每条都要暗含场景、前后对比或可见动作。"
        "不要把发货快、价格、数量这类不可拍信息包装成视频主钩子；只润色表达，不要 invent 新卖点。"
    ),
    "creator_brief": (
        "你是达人建联话术专家。把给定的 VOC 洞察润色成适合发给达人的 brief 卖点。"
        "标题中文要清楚好记，本地语言口播要让达人照着念自然，hook 给 2-3 条达人可直接用的开场白。"
        "只润色表达，不要编造新卖点。"
    ),
    "content_copy": (
        "你是电商内容文案专家。把给定的 VOC 洞察润色成商品详情/短视频文案可用的卖点表达。"
        "标题中文要顺，本地语言要像真实买家评价的口吻，hook 给 2-3 条可做视频字幕的短句。"
        "只润色表达，不要编造新卖点。"
    ),
    "selection": (
        "你是选品分析师。把给定的 VOC 洞察润色成给选品决策看的简洁卖点说明。"
        "标题中文要准、不夸大，本地语言保留原始买家口吻，hook 给 1-2 条点明核心价值的短句。"
        "只润色表达，不要编造新卖点。"
    ),
    "general": (
        "你是跨境电商 VOC 洞察文案专家。把给定的 VOC 洞察做表达润色。"
        "标题中文精炼，本地语言自然，hook 给 2 条短句。只润色表达，不要编造新卖点。"
    ),
}

# ── 用户 prompt 模板 ──────────────────────────────────────────
USER_PROMPT_TEMPLATE = """请基于以下 VOC 洞察和真实买家评价样本，做表达润色。

【洞察】
- insight_id: {insight_id}
- insight_type: {insight_type}
- 当前中文标题: {title_zh}
- 当前本地口播: {local_voice}
- 信号标签: {signal_tags}
- 置信度: {confidence}
- 覆盖商品数: {product_count}
- 证据数: {evidence_count}
- 风险备注: {risk_notes}
- 视频使用层: {usage_lane}
- 视频适配分: {video_fit_score}
- 可拍证明点: {visual_proof_zh}
- 必须出现动作: {required_action_zh}

【真实买家评价样本（本地语言）】
{evidence_examples}

【目标市场语言】{market_lang}

请只输出一个 JSON，结构如下，不要输出任何其它内容：
```json
{{
  "title_zh": "润色后的中文标题（≤18字，精炼有钩子，不夸大）",
  "local_voice": "润色后的本地语言口播（1句，≤25字符，像真人买家说话）",
  "hooks": ["本地语言广告/达人 hook 1（≤40字符）", "hook 2", "hook 3"],
  "reason_zh": "1-2 句中文说明，解释为什么这个卖点成立、给运营/达人/投流同学看"
}}
```

规则：
1. title_zh 必须和原洞察同义，只改表达，不许换意思。
2. local_voice 必须是 {market_lang} 自然口语，优先参考 evidence_examples 里的真实买家用词。
3. hooks 每条独立可用，不要互相重复；带场景/对比/结果钩子更佳。
4. 如果 usage_lane 不是 video_hook/video_support，hooks 给运营备注语气，不要当视频主钩子吹。
5. 如果 insight_type 是 fulfillment_issue 或 pain_point，hooks 给"避坑提示"语气，不要当卖点吹。
6. 如果 risk_notes 含 value_quantity_requires_set_or_multipack，title_zh 和 hooks 必须体现"组合装/多件装"前提。
7. ads_mixcut 的 hooks 不能只写抽象形容词，必须能落成画面，例如"戴上后转头看变化"、"碎发一夹变利落"。
8. reason_zh 要点出证据强度（如"35 个商品 122 条 VOC 提到"），不要空泛。
9. 禁止新增证据未支持的功能承诺：不要写"不疼/不痛/不伤发/全天不掉/一整天/防滑"，泰语不要写"ไม่เจ็บ/ไม่ปวด/ไม่ดึงผม/ผมไม่เสีย/ทั้งวัน/ตลอดวัน"。
"""


def build_polish_prompt(insight: Dict[str, Any], market_lang: str) -> str:
    """组装用户 prompt。"""
    examples = insight.get("evidence_examples") or []
    if not examples:
        # 从 evidence_refs 凑数
        refs = insight.get("evidence_refs") or []
        examples = refs[:3]
    examples_text = "\n".join("- {}".format(e) for e in examples[:5]) or "- (无样本)"
    return USER_PROMPT_TEMPLATE.format(
        insight_id=insight.get("insight_id", ""),
        insight_type=insight.get("insight_type", ""),
        title_zh=insight.get("title_zh", ""),
        local_voice=insight.get("local_voice", ""),
        signal_tags=", ".join(insight.get("signal_tags") or []),
        confidence=insight.get("confidence", ""),
        product_count=insight.get("product_count", 0),
        evidence_count=insight.get("evidence_count", 0),
        risk_notes=", ".join(insight.get("risk_notes") or []) or "无",
        usage_lane=insight.get("usage_lane", ""),
        video_fit_score=insight.get("video_fit_score", ""),
        visual_proof_zh=insight.get("visual_proof_zh", ""),
        required_action_zh=insight.get("required_action_zh", ""),
        evidence_examples=examples_text,
        market_lang=market_lang,
    )


def system_prompt_for(usecase: str) -> str:
    return SYSTEM_PROMPTS.get(usecase, SYSTEM_PROMPTS["general"])


# 市场代码 → 本地语言名（给 prompt 用）
MARKET_LANG: Dict[str, str] = {
    "TH": "泰语",
    "VN": "越南语",
    "MY": "马来语",
    "ID": "印尼语",
    "PH": "菲律宾语/英语",
    "SG": "英语/马来语",
}


def market_lang_name(market: str) -> str:
    return MARKET_LANG.get((market or "").upper(), "本地语言")
