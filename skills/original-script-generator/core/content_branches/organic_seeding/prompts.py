"""Prompts owned by SEEDING_ORGANIC. No direct-response prompt branching."""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from ..contracts import VisualIntentContract
from .contracts import OrganicSeedThemeContract


def build_organic_visual_prompt(
    *, product_truth: Dict[str, Any], intent: VisualIntentContract, duration_seconds: float,
    topic_contract: Optional[Dict[str, Any]] = None,
    retention_contract: Optional[Dict[str, Any]] = None,
    story_spine: Optional[Dict[str, Any]] = None,
    commerce_repair_hits: Optional[List[Dict[str, Any]]] = None,
    quality_repair_reasons: Optional[List[str]] = None,
) -> str:
    payload = {
        "product_truth": product_truth,
        "visual_intent": intent.to_dict(),
        "topic_contract": dict(topic_contract or {}),
        "retention_contract": dict(retention_contract or {}),
        "story_spine": dict(story_spine or {}),
        "duration_seconds": duration_seconds,
    }
    return f"""你是生活方式短视频的视觉策划。生成种草类、不挂车、非强销售的可执行视觉脚本。

业务边界：
- 观看价值优先，商品只是内容中的一个角色；不得把任务改写成带货广告。
- 不得输出交易信息、促销信息、销售行动号召或平台导流元素；不要在输出中复述这条禁令。
- 只能使用 product_truth 中的事实。创作的场景、人物和审美判断必须标为创作设计，不能伪装成商品事实。
- 开场先给生活时刻、审美问题或观看收益；按 product_prominence 决定商品何时成为主体。
- 画面必须符合商品物理形态、佩戴状态、数量连续性和同一人物手部关系。
- 只设计画面，不写口播；口播由独立模块生成。
- 除固定英文枚举和 ID 外，script_title、creative_design、opening_design、capture_units 的自然语言字段必须全部使用简体中文；禁止输出泰语视觉说明。
- 输入商品图片只控制目标商品外观与可见结构；不得复制图片中的人物、脸、身形、妆发、裤装、背景、姿势或构图。
    - story_spine 是本条内容的因果主线权威；人物动机、观众处境、核心价值和可见转折必须属于同一件事。
    - topic_contract 只是 story_spine 的注意力表面投影；不得为了执行话题家族而覆盖真实人群处境和核心产品价值。
    - visual_intent.branch_payload.creative_diversity 只提供可执行场景与动作参考，不得覆盖 story_spine。
- 必须执行 rhetorical_family / hook_mechanism / closing_mode / capture_mode，不得把所有方向都写成同一种生活旁白结构。
- topic_contract 是本条内容命题权威；开场、画面推进和中段揭示必须共同完成同一个 topic_thesis，不能只把 topic_family 当标签。
- retention_contract 是前3秒权威：第一段从动作中点、构图反差或结果状态开始，第一段建议1.2至2.2秒；不得先用5秒远景建立环境。
- 当前目标时长设计3至4个可见片段；15秒优先4段。每个后续片段必须带来新的构图、人物—商品关系或观看信息，不得为了凑段数重复站立、观察或走路。
- 每段只允许一个主要人物动作和一个简单镜头动作；禁止把环绕、走路、转身、整理衣物和手部细节堆在同一段。
- capture_mode=ONE_FIXED_PHONE 时所有片段只能使用固定手机和普通直接切镜；FOLLOW_BROLL 才允许跟拍。
- ONE_FIXED_PHONE 仍要通过真实重录形成景别或人物关系变化，禁止连续三段相同景别、相同位置和相同观看关系。
- 每段必须声明 narrative_job=LIVED_MOMENT/RELATION/ATMOSPHERE/PRODUCT_DETAIL、product_focus=BACKGROUND/SECONDARY/PRIMARY、product_interaction=NONE/NATURAL_USE/DEMONSTRATION。
- 至少三分之二片段必须承担生活、人物关系或氛围，不能承担商品细节核对；product_evidence 是可选字段，不得为了填字段而给每段安排商品证明。
- 商品角色约束：HERO 最多2段 PRIMARY、最多1段 DEMONSTRATION；SUPPORTING 最多1段 PRIMARY且禁止 DEMONSTRATION；INCIDENTAL 禁止 PRIMARY和DEMONSTRATION。
- STYLE_MEMORY 或 SCENE_ASSOCIATION 目标禁止 PRODUCT_DETAIL。禁止为了展示商品而触摸、整理、指向或拉扯；自然穿着与真实使用不算演示。
- 穿戴类商品默认从首段就处于已经自然穿着的状态，并在全片保持同一穿戴状态；禁止先挂墙、平铺或手持，下一段无过程突然变成已经穿上。
- 只有确实承担商品事实或视觉锚点的片段才填写 product_evidence，并用 evidence_refs/fact_refs 引用权威；其他生活片段保持空值。
- 每段明确 authorized_props；未列入其中的道具不得进入画面。

输入合同：
{json.dumps(payload, ensure_ascii=False, indent=2, default=str)}

只返回一个 JSON 对象：
{{
  "schema_version": "organic-visual-blueprint-v4",
  "script_title": "自然、非商业化的标题",
  "creative_design": {{"creator": "人物或不适用", "scene": "具体可拍地点", "lived_moment": "一件自然发生的事"}},
  "opening_design": {{"subject": "开场主体", "visible_action": "动作中点、构图反差或结果先行", "viewer_value": "开场给观众的价值", "first_frame_tension": "首帧中等待被解释的关系", "first_3s_open_loop": "前3秒留下的问题"}},
  "capture_units": [
    {{"unit_id": "C1", "duration_seconds": 1.8, "narrative_job": "LIVED_MOMENT/RELATION/ATMOSPHERE/PRODUCT_DETAIL", "product_focus": "BACKGROUND/SECONDARY/PRIMARY", "product_interaction": "NONE/NATURAL_USE/DEMONSTRATION", "shot_size": "FULL/MEDIUM/CLOSE", "body_coverage": "HEAD_TO_KNEE/HEAD_TO_HIP/SHOULDER_TO_HIP/DETAIL_ONLY", "camera_relation": "FIXED_TRIPOD/HANDHELD_SELFIE/FOLLOW_BROLL", "setup_id": "SETUP_1", "shot": "简短中文景别与手机关系", "camera_action": "固定或一种简单运镜", "subject_action": "一个可执行人物动作", "authorized_props": [], "visible_zones": ["FULL_BODY/FRONT/BACK/SIDE/COLLAR/FRONT_CENTER/SLEEVE/HEM"], "product_evidence": "可选；仅商品证明段填写", "evidence_refs": [], "fact_refs": [], "information_gain": "本段相对上一段新增的观看信息"}}
  ],
  "product_role_realization": {{"role": "HERO/SUPPORTING/INCIDENTAL", "first_prominent_unit": "C2", "memory_residue": "最终留下的印象"}},
  "commerce_elements": {{"price": false, "promotion": false, "purchase_cta": false, "cart_reference": false}}
}}
不要输出 video_generation_prompt；最终视频提示词由代码从 capture_units 确定性渲染。
capture_units 为3至4段、最多4段，总时长接近目标时长；第一段短而有动作，其余时间按内容信息分配。{_repair_suffix(commerce_repair_hits)}{_quality_repair_suffix(quality_repair_reasons)}"""


def build_organic_voiceover_prompt(
    *,
    product_truth: Dict[str, Any],
    theme: OrganicSeedThemeContract,
    visual_blueprint: Dict[str, Any],
    target_language: str,
    duration_seconds: float,
    topic_contract: Optional[Dict[str, Any]] = None,
    retention_contract: Optional[Dict[str, Any]] = None,
    story_spine: Optional[Dict[str, Any]] = None,
    commerce_repair_hits: Optional[List[Dict[str, Any]]] = None,
    quality_repair_reasons: Optional[List[str]] = None,
) -> str:
    story = dict(story_spine or {})
    voice_min_seconds = max(4.0, round(float(duration_seconds) * (0.70 if story.get("core_value") else 0.47), 1))
    voice_max_seconds = max(voice_min_seconds + 0.5, round(float(duration_seconds) * 0.96, 1))
    fact_index = {
        str(item.get("fact_id") or "").strip(): item
        for item in product_truth.get("facts") or []
        if isinstance(item, dict) and str(item.get("fact_id") or "").strip()
    }
    core_ref = str(story.get("core_claim_ref") or "").strip()
    support_ref = str(story.get("support_fact_ref") or "").strip()
    payload = {
        "spoken_brief_schema": "organic-spoken-brief-v1",
        "target_market": {
            "country": product_truth.get("target_country"),
            "language": target_language,
            "product_type": product_truth.get("product_type"),
        },
        "audience_or_need": story.get("viewer_relevance") or theme.viewer_payoff,
        "human_trigger": story.get("human_trigger"),
        "reason_to_speak_now": story.get("creator_motive"),
        "one_lived_context": story.get("lived_context") or theme.lived_context,
        "one_core_value": fact_index.get(core_ref) or {
            "fact_id": core_ref, "text": story.get("core_value")
        },
        "optional_visible_support": fact_index.get(support_ref) or (
            {"fact_id": support_ref, "text": story.get("support_fact")} if support_ref else {}
        ),
        "visible_turn": story.get("visible_turn"),
        "personal_realization": story.get("personal_realization"),
        "affinity_residue": story.get("affinity_residue"),
        "discussion_tension": story.get("discussion_tension"),
        "hook_intent": {
            "open_loop": story.get("open_loop") or (topic_contract or {}).get("open_loop"),
            "attention_mechanism": (topic_contract or {}).get("attention_mechanism"),
        },
        "experience_authority": theme.experience_authority,
        "confirmed_experience_facts": list(theme.confirmed_experience_facts),
        "soft_spoken_seconds": [voice_min_seconds, voice_max_seconds],
    }
    return f"""你是短视频原生口播作者。写一段种草类、不挂车、没有购买指令的自然分享口播。

硬边界：
- 目标是让观众记住一种风格、场景、选择标准、细节、个人立场或讨论问题，不是完成销售论证。
- 禁止交易信息、促销信息、销售行动号召和平台导流；不要在口播或中文对照中复述这条禁令。
- experience_authority=NONE 时，不得声称长期使用、回购、每次都用、朋友都问、收到很多夸赞等历史经历。
- CURRENT_OBSERVATION 只允许说当下可直接观察到的内容；OPERATOR_CONFIRMED_HISTORY 也只能使用 confirmed_experience_facts 原文授权的经历。
- 商品事实只能来自精简说话简报中的 one_core_value 与 optional_visible_support；个人审美必须写成个人判断，不得升级成普遍功效。
- 口播实际使用商品事实时，used_fact_refs 必须逐字复制对应 fact_id；没有使用事实则返回空数组。
- 不逐镜解说，不念参数清单。自然收尾或开放讨论即可。
- 把整段写成一个人因为某个具体触发而产生的一条连续想法。可以省略场景、支持事实或提问，只有核心价值必须让听众听懂。
- 第一段尽快给出具体顾虑、判断或结果；不要先机械报时间地点，也不要套“今天给大家分享”的栏目开场。
- one_lived_context 只是可选说话位置，不是商品功效证据；画面不需要逐句解释。
- 不要求把处境、判断、依据、结论写成四句或逐项填满。允许自然停顿、轻微省略和带偏好的口语跳跃。
- optional_visible_support 最多使用一次；如果加入后像商品说明书，宁可不用。禁止拼入第二个独立价值主题。
- soft_spoken_seconds={voice_min_seconds}-{voice_max_seconds}秒只用于控制可容纳性，不是最低填充率；内容说完后给BGM、环境声和画面留白，禁止用形容词或重复结论补时长。
- 直接用{target_language}形成原生口语，中文对照只能在目标语言完成后忠实翻译，不能先写中文小作文再逐句翻译。

输入合同：
{json.dumps(payload, ensure_ascii=False, indent=2, default=str)}

只返回一个 JSON 对象：
{{
  "schema_version": "organic-voiceover-v4-native-spoken-brief",
  "target_text": "完整{target_language}口播",
  "chinese_translation": "忠实中文对照",
  "estimated_duration_seconds": {(voice_min_seconds + voice_max_seconds) / 2:.1f},
  "hook_surface": "开场修辞意图",
  "speaker_motive_realization": "实际采用的说话动机；仅供审核，不必逐字出现在口播",
  "viewer_relevance_realization": "这段话具体与谁有关；仅供审核",
  "core_value_realization": "实际表达的唯一核心价值",
  "affinity_residue": "听完留下的偏好、心动点或记忆，不写购买指令",
  "topic_realization": "这段口播实际表达的中心思想",
  "open_loop_realization": "前3秒留下的具体问题",
  "payoff_realization": "中段如何回答开场问题",
  "content_progression": {{}},
  "closing_mode": "NATURAL_END/OPEN_DISCUSSION",
  "used_fact_refs": [],
  "experience_claims": [],
  "viewer_payoff_realization": "观众最终得到什么",
  "commerce_elements": {{"price": false, "promotion": false, "purchase_cta": false, "cart_reference": false}}
}}{_repair_suffix(commerce_repair_hits)}{_quality_repair_suffix(quality_repair_reasons)}"""


def _repair_suffix(hits: Optional[List[Dict[str, Any]]]) -> str:
    if not hits:
        return ""
    compact = [
        {"field_path": hit.get("field_path"), "term": hit.get("term"), "context": hit.get("context")}
        for hit in hits
    ]
    return f"""

这是一次且仅一次的商业表达修订。上一稿命中：
{json.dumps(compact, ensure_ascii=False, indent=2)}
只删除或改写上述真实商业表达，保持原有事实、人物、场景、结构、目标语言和观看价值不变。
不要解释修改过程，也不要在任何输出字段中复述命中词或禁令。"""


def _quality_repair_suffix(reasons: Optional[List[str]]) -> str:
    compact = [str(reason or "").strip() for reason in reasons or [] if str(reason or "").strip()]
    if not compact:
        return ""
    directives = []
    if "ORGANIC_VOICEOVER_DURATION_EXCEEDS_VIDEO" in compact or "ORGANIC_VOICEOVER_DURATION_TIGHT" in compact:
        directives.append("口播实测过满：压缩重复修饰和同义句，保留唯一核心价值与自然说话动机。")
    repair_directives = "\n".join(f"- {item}" for item in directives)
    return f"""

这是一次且仅一次的定向质量修订。上一稿需要修正：
{json.dumps(compact, ensure_ascii=False, indent=2)}
{repair_directives}
只修正这些问题，保持冻结的内容角度、商品证据、人物、场景、修辞家族和目标语言不变。不要解释修订过程。"""
