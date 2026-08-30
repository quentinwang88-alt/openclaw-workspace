"""Topic contracts owned exclusively by the organic-seeding branch.

The shared kernel must not decide what is interesting.  This module freezes one
clear, non-commercial audience tension before visuals and voiceover are written,
so both surfaces execute the same idea instead of merely sharing a loose theme.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Sequence

from ..contracts import stable_id
from .contracts import OrganicSeedThemeContract


TOPIC_FAMILY_ROTATION = (
    "VISUAL_SURPRISE",
    "RELATABLE_TENSION",
    "COUNTERINTUITIVE_POSITION",
    "CHOICE_RULE",
    "MOTION_REVEAL",
)


@dataclass(frozen=True)
class OrganicTopicContract:
    contract_id: str
    topic_family: str
    topic_thesis: str
    audience_tension: str
    personal_stance: str
    open_loop: str
    payoff: str
    comment_trigger: str
    attention_mechanism: str
    first_frame_strategy: str
    audio_profile: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _topic_material(
    family: str, *, product_type: str, proof_focus: str, angle_label: str
) -> Dict[str, str]:
    product = product_type or "这个单品"
    focus = proof_focus or angle_label or "完整造型关系"
    if family == "VISUAL_SURPRISE":
        return {
            "topic_thesis": f"先让观众判断{product}最先留下的印象，再揭示真正起作用的是{focus}",
            "audience_tension": "第一眼判断与停留后观察到的重点可能不同",
            "personal_stance": "创作者只说明自己最后记住的视觉关系，不替观众下结论",
            "open_loop": "开场先给结果或异常构图，不立刻解释原因",
            "payoff": f"在中段用可见画面回答为什么会注意到{focus}",
            "comment_trigger": "如果只能保留一个记忆点，会选整体关系还是这个局部关系",
            "attention_mechanism": "RESULT_BEFORE_REASON",
            "first_frame_strategy": "用前景、镜面或临界动作形成未解释的视觉关系",
            "audio_profile": "CRISP_REVEAL",
        }
    if family == "RELATABLE_TENSION":
        return {
            "topic_thesis": f"围绕{product}进入整套造型后是融入还是抢走注意力，展开一次真实选择",
            "audience_tension": "想让单品被看见，又不想整套造型只剩商品展示",
            "personal_stance": "用个人取舍回答，不把审美偏好写成普遍规则",
            "open_loop": "开场直接提出两难，但暂不选边",
            "payoff": f"通过{focus}给出本次造型里的具体选择",
            "comment_trigger": "让观众对同一个具体取舍选边，而不是泛泛提问",
            "attention_mechanism": "RELATABLE_DILEMMA",
            "first_frame_strategy": "冻结在人物即将作出选择的临界瞬间",
            "audio_profile": "LIGHT_TENSION",
        }
    if family == "COUNTERINTUITIVE_POSITION":
        return {
            "topic_thesis": f"提出一个有边界的个人立场：更稳妥的选择不一定比{focus}更容易留下印象",
            "audience_tension": "安全搭配与有记忆点的搭配之间存在个人取舍",
            "personal_stance": "第一人称表达改变或偏好，禁止升级为商品功效",
            "open_loop": "先说与常见安全选择不同的决定，再展示理由",
            "payoff": f"中段用{focus}说明这次为什么保留当前选择",
            "comment_trigger": "邀请观众判断会保留当前选择还是换回更稳妥的方案",
            "attention_mechanism": "POSITION_FIRST",
            "first_frame_strategy": "人物已经做出选择，但画面仍保留另一个可能性的暗示",
            "audio_profile": "EDITORIAL_STANCE",
        }
    if family == "CHOICE_RULE":
        return {
            "topic_thesis": f"给出一个可复用但属于个人的方法：先看{focus}，再看{product}本身",
            "audience_tension": "只盯单品细节，可能看不清它和整套造型的关系",
            "personal_stance": "把规则明确限定为创作者自己的观察顺序",
            "open_loop": "开场先说观察顺序，不马上展示最终判断",
            "payoff": f"用一个明确画面变化完成对{focus}的判断",
            "comment_trigger": "询问观众第一眼会先检查哪一个具体关系",
            "attention_mechanism": "USEFUL_RULE_FIRST",
            "first_frame_strategy": "使用能够同时看到整体与关键关系的分层构图",
            "audio_profile": "CLEAN_EXPLAINER",
        }
    return {
        "topic_thesis": f"利用静止与自然移动的差异，揭示{product}在{focus}上的第二层印象",
        "audience_tension": "静止画面中的判断，可能在人物移动后发生变化",
        "personal_stance": "只描述本次可见变化和个人注意结果",
        "open_loop": "开场先给动作变化，不马上解释观众应该看哪里",
        "payoff": f"在移动后的清楚画面中揭示{focus}",
        "comment_trigger": "让观众判断静止状态和动态状态哪一个更容易记住",
        "attention_mechanism": "MOTION_THEN_REVEAL",
        "first_frame_strategy": "冻结在人物刚进入亮部、转向或停步的动作中点",
        "audio_profile": "FORWARD_MOTION",
    }


def allocate_topic_contracts(
    themes: Sequence[OrganicSeedThemeContract], product_truth: Dict[str, Any],
    story_spines: Sequence[Dict[str, Any]] = (),
) -> List[OrganicTopicContract]:
    """Project a story into an attention contract.

    The legacy family rotation remains as a fallback when no story planner is
    present.  With a story spine, the human trigger and visible payoff own the
    topic; the family is only a descriptive label.
    """

    product_type = _text(product_truth.get("product_type")) or "这个单品"
    output: List[OrganicTopicContract] = []
    for index, theme in enumerate(themes):
        story = (
            dict(story_spines[index])
            if index < len(story_spines) and isinstance(story_spines[index], dict)
            else {}
        )
        if story:
            family = _text(story.get("story_family")).upper() or "PRODUCT_GROUNDED_STORY"
            material = {
                "topic_thesis": _text(story.get("personal_realization")) or theme.viewer_payoff,
                "audience_tension": _text(story.get("human_trigger")),
                "personal_stance": _text(story.get("creator_motive")),
                "open_loop": _text(story.get("open_loop")),
                "payoff": _text(story.get("visible_turn")),
                "comment_trigger": _text(story.get("discussion_tension")),
                "attention_mechanism": {
                    "RELATABLE_DECISION": "RELATABLE_DILEMMA",
                    "VISIBLE_CHOICE_RULE": "USEFUL_RULE_FIRST",
                    "FIT_TRADEOFF": "RESULT_BEFORE_REASON",
                    "DETAIL_TO_WHOLE": "DETAIL_THEN_WHOLE",
                    "PROPORTION_PIVOT": "RESULT_BEFORE_REASON",
                    "SILHOUETTE_BOUNDARY": "POSITION_FIRST",
                    "PERSONAL_STANDARD": "POSITION_FIRST",
                }.get(family, "LIVED_DISCOVERY"),
                "first_frame_strategy": (
                    "从已经发生一半的选择、比较或上身结果开始，"
                    "让观众先看到问题，稍后再解释判断依据"
                ),
                "audio_profile": {
                    "RELATABLE_DECISION": "LIGHT_TENSION",
                    "VISIBLE_CHOICE_RULE": "CLEAN_EXPLAINER",
                    "FIT_TRADEOFF": "CRISP_REVEAL",
                    "DETAIL_TO_WHOLE": "CRISP_REVEAL",
                    "PROPORTION_PIVOT": "CRISP_REVEAL",
                    "SILHOUETTE_BOUNDARY": "EDITORIAL_STANCE",
                    "PERSONAL_STANDARD": "EDITORIAL_STANCE",
                }.get(family, "FORWARD_MOTION"),
            }
        else:
            family = TOPIC_FAMILY_ROTATION[index % len(TOPIC_FAMILY_ROTATION)]
            material = _topic_material(
                family,
                product_type=product_type,
                proof_focus=_text(theme.proof_focus),
                angle_label=_text(theme.memory_residue),
            )
        contract_id = stable_id(
            "SEED_TOPIC_",
            {
                "theme_id": theme.theme_id,
                "topic_family": family,
                "topic_thesis": material["topic_thesis"],
            },
        )
        output.append(
            OrganicTopicContract(
                contract_id=contract_id,
                topic_family=family,
                **material,
            )
        )
    return output
