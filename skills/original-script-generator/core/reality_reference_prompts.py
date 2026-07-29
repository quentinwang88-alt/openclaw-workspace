"""Prompts for the visual-first reality-reference experiment."""

from __future__ import annotations

import json
from typing import Any, Dict


def _compact(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def build_complete_script_blueprint_prompt(
    *,
    target_country: str,
    product_type: str,
    direction: Dict[str, Any],
) -> str:
    bundle = direction.get("content_bundle_brief") if isinstance(direction.get("content_bundle_brief"), dict) else {}
    execution_plan = direction.get("structure_execution_plan") if isinstance(direction.get("structure_execution_plan"), dict) else {}
    reference = direction.get("execution_reference") if isinstance(direction.get("execution_reference"), dict) else {}
    diversity = direction.get("creative_diversity_contract") if isinstance(direction.get("creative_diversity_contract"), dict) else {}
    carrier = _compact({
        "required_carrier": diversity.get("required_carrier", "UNAVAILABLE"),
        "required_presentation_mode": diversity.get("required_presentation_mode", "UNAVAILABLE"),
        "on_camera_policy": diversity.get("on_camera_policy", "STRUCTURE_PLAN_GOVERNS"),
    })
    direction_package = {
        "creative_thesis": bundle.get("content_mainline", ""),
        "primary_hook_id": bundle.get("primary_hook_id", ""),
        "content_bundle": bundle,
        "structure_execution_plan": execution_plan,
        "execution_reference_summary": {
            "content_carrier": reference.get("content_carrier", ""),
            "visual_hook_type": reference.get("visual_hook_type", ""),
            "behavior_chain": reference.get("behavior_chain", []),
            "shot_execution_spine": reference.get("shot_execution_spine", []),
            "unknown_fields": reference.get("unknown_fields", []),
        },
    }
    return f"""你是原创短视频的事件蓝图编剧。你不写逐镜分镜和口播，而是把已分配的创意坐标补全成一个具体、连贯、可拍的15秒生活事件，并输出三段宏观画面过程。

【目标国家】{target_country}
【产品类型】{product_type}

【方向包】
{_compact(direction_package)}

【程序分配的创意多样性合同，硬约束】
{_compact(diversity)}

【承载方式硬约束】
{carrier}

【职责边界】
1. 源视频未知的人物、地点、灯光继续保持未知；你输出的人物场景属于CREATIVE_DESIGN生产设计，不得声称来自源视频观察。
2. 必须准确继承diversity contract的viewer_relationship、scene_motif和opening_action。forbidden_recent_patterns禁止的是上一轮已失败的完整组合，不是永久封禁“卧室、镜子、转身”等单个元素；本轮不得未经合同分配自行退回该失败组合。
3. 不写戏剧化剧情。WEARER_ACTIVE/MIXED时，creator_motivation解释为什么这个人在场景里顺手记录，event_design让人物完成一件本来就要做的普通事情；STATIC_PRODUCT时，三段是同一静物承载上的连续观察，人物和手均不出现；HAND_ONLY时，只能由手和商品完成连续操作，人物整体不出现。
   creative_product_profile只提供轻量类目适配：WORN_APPAREL看穿着中的整体关系；WORN_ACCESSORY从已经佩戴好的状态进入，让配饰与身体或整套穿搭的关系自然可见；HAND_STATIC_ACCESSORY按结构承载做手部或静物观察。它不能覆盖required_carrier，也不要求增加专门表演动作。
4. 不输出“轻判断、轻满意、小惊喜、情绪推进、决策信号”等抽象标签。人物状态必须写成可见行为或具体说话方式。
5. persona和scene必须具体，但不能增加未经商品事实授权的功效、材质、价格、销量或使用结果。
6. 说话人格要适合泰国短视频自然分享，避免主播式催单；语气词密度写成具体原则，不直接写完整口播。
7. reaction_points只在场景自然产生反应时写0至1个；允许空数组。不要为了证明人物有情绪而强制笑、点头、挑眉、突然停住或看镜头。
8. anti_template_rules必须包含程序合同的forbidden_recent_patterns，不再补充大段新限制。
9. primary_hook_id是本方向已经选定的口播注意力意图。retention_hook从合同允许的自然起始状态开始：人物事件、手部操作或静物观察；不要求突然停住、抬眼或表演戏剧性反应。
10. performance_flow不是商品检查清单。behavior_motivation只写event_design.natural_event的简洁投影，不得另外创造袖口观察、扣位观察、口袋检查、侧身确认等商品展示链。
11. macro_visual_passages固定3段：进入、展开与核心结果、自然结束。三段必须是同一件连续过程：人物事件、手部操作或静物观察，不能按商品部位拆段；结尾不得为了展示而额外摆姿势或停一拍，除非停下本来就是该过程的一部分。
12. content_bundle中的2至3个claim_atom必须在三段整体范围内全部进入supported_claim_keys，但单个过渡段或结束段允许空数组。同一段可以同时支持多个claim；除核心结果外，其他事实只要自然可见，不得为它们设计专门动作。
13. observable_action只写合同允许的可见动作。visible_process写承载方式、场景、穿搭（若人物出镜）和商品在该过程中的可见状态；camera_observation只写旁观机位。人物无需表演情绪或对镜头反应。
14. 三段画面必须继承shot_execution_spine的动作关系；不用输出逐镜reference order，程序会按源动作顺序确定性投影。

只输出合法JSON对象：
{{
  "schema_version":"complete-script-blueprint-v4-carrier",
  "authority":"CREATIVE_DESIGN",
  "diversity_contract_id":"",
  "presentation_mode":"PERSON_ON_CAMERA|HANDS_ONLY|STATIC_PRODUCT|MIXED",
  "creative_thesis":"",
  "creator_motivation":"",
  "viewer_relationship":"",
  "retention_hook":{{
    "opening_event":"0至1.5秒内摄像机可见的具体事件",
    "delayed_answer":"暂时不完全展示、到后续才看清的具体答案或结果",
    "payoff_time":"3-5s"
  }},
  "persona":{{
    "identity":"",
    "age_presence":"",
    "appearance":"",
    "hair_makeup":"",
    "styling":"",
    "speaking_personality":"",
    "performance_intensity":""
  }},
  "scene":{{
    "location":"",
    "moment":"",
    "lighting":"",
    "background":"",
    "camera_setup":"",
    "why_this_scene":""
  }},
  "performance_flow":{{
    "entry_state":"",
    "behavior_motivation":"必须逐字包含程序分配的opening_action，并只概括合同允许的同一连续过程",
    "reaction_points":[],
    "ending_state":"事件自然完成；不为展示商品额外停住"
  }},
  "event_design":{{
    "event_motif":"一句话说明合同允许的连续过程",
    "start_state":"过程开始时人物/手部/静物承载和商品的具体状态",
    "natural_event":"必须逐字包含程序分配的opening_action；随后继续同一过程",
    "core_result_moment":"过程中自然看清核心商品结果的时刻",
    "end_state":"过程结束后的具体状态，不为展示商品额外停住"
  }},
  "macro_visual_passages":[
    {{
      "passage_no":1,
      "narrative_role":"EVENT_ENTRY",
      "visible_process":"人物、场景、穿搭和商品的可见过程",
      "observable_action":"一项正在发生的生活动作",
      "camera_observation":"旁观式机位和景别",
      "product_visibility":"FULL|PARTIAL|OCCLUDED|NONE",
      "supported_claim_keys":[]
    }},
    {{
      "passage_no":2,
      "narrative_role":"EVENT_PROOF",
      "visible_process":"同一事件继续，核心结果自然出现",
      "observable_action":"延续上一段的生活动作",
      "camera_observation":"旁观式机位和景别",
      "product_visibility":"FULL|PARTIAL|OCCLUDED|NONE",
      "supported_claim_keys":["CLM_xxx","CLM_xxx"]
    }},
    {{
      "passage_no":3,
      "narrative_role":"EVENT_END",
      "visible_process":"事件自然完成后的状态",
      "observable_action":"连续完成同一件生活事件，不增加展示性停顿",
      "camera_observation":"旁观式机位和景别",
      "product_visibility":"FULL|PARTIAL|OCCLUDED|NONE",
      "supported_claim_keys":[]
    }}
  ],
  "visual_language":{{
    "image_texture":"",
    "camera_behavior":"",
    "framing_bias":"",
    "editing_rhythm":"",
    "anti_template_rules":[""]
  }},
  "voice_identity":{{
    "tone":"",
    "relationship_mode":"",
    "particle_density":"",
    "sales_pressure":"",
    "forbidden_tone":[""]
  }},
  "audio_direction":{{
    "bgm_style":"",
    "environment_sound":"",
    "voiceover_priority":""
  }}
}}"""


def build_visual_adaptation_prompt(
    *,
    target_country: str,
    product_type: str,
    anchor_card: Dict[str, Any],
    direction: Dict[str, Any],
) -> str:
    anchor_subset = {
        "hard_anchors": anchor_card.get("hard_anchors", []),
        "display_anchors": anchor_card.get("display_anchors", []),
        "key_visual_constraints": anchor_card.get("key_visual_constraints", []),
        "operation_anchors": anchor_card.get("operation_anchors", []),
        "category_execution_contract": anchor_card.get("category_execution_contract", {}),
    }
    return f"""你是原创短视频的视觉执行适配器。你的工作不是从零创作，而是把一个真实视频的执行关系换成当前商品。

【目标】
先生成完全无口播的15秒画面方案。结构合同决定叙事顺序；真实执行卡决定具体拍法；商品锚点只决定可验证事实。

目标国家：{target_country}
产品类型：{product_type}

【结构执行计划：宏观顺序硬约束，微观镜头只作执行槽位】
{_compact(direction.get('structure_execution_plan', {}))}

【真实执行卡】
{_compact(direction.get('execution_reference', {}))}

【内容论证包：一个主线、多个相关卖点】
{_compact(direction.get('content_bundle_brief', {}))}

【程序分配的创意多样性合同】
{_compact(direction.get('creative_diversity_contract', {}))}

【完整脚本蓝图：明确标记的生产创意设计】
{_compact(direction.get('creative_blueprint', {}))}

【允许进入视频提示词的蓝图投影】
{_compact(direction.get('video_prompt_blueprint', {}))}

【商品事实锚点】
{_compact(anchor_subset)}

【执行纪律】
1. 为兼容现有生产接口，shots数量仍与structure_execution_plan.shot_plan一致，并继承structure_beat、carrier_mode、continuity_group、opening_mechanism和时间段。但这些是执行槽位，不代表“一卖点一镜头”；相邻槽位可以是同一动作的延续、视角变化或自然过渡。
2. 每镜只写摄像机能看到的事实；shot_content 与 observable_action 禁止出现“轻判断、轻满意、轻安心、情绪推进、决策信号、完成度”等抽象词。
3. 真实执行卡中的 observable_action、camera_grammar、商品可见度关系是具体拍法来源；允许把原商品替换为当前商品，但不得改成通用镜前脚本。
   每个新镜头必须用 reference_spine_orders 标明继承了执行卡 shot_execution_spine 的哪些 order；扩镜允许重复同一order，但整体顺序不得倒置。
4. 商品锚点中的scene_suggestions、persona_suggestions、safe_shot_templates不是场景来源。人物、场景、灯光和表演只能来自完整脚本蓝图的VIDEO_PROMPT投影，并标记为CREATIVE_DESIGN。
5. 围绕content_bundle_brief.content_mainline展开，直接继承creative_blueprint.performance_flow.behavior_motivation作为全片唯一人物行为主线。只有role=core_result的核心结果需要通过这段行为主动证明；其余claim_atom只需在整片中清楚可见，不得为扣子、口袋、袖型等细节分别安排指向、触摸、逐项核对动作。一个镜头可以同时支持多个supported_claim_keys；不支持卖点的过渡镜写空数组。
6. 不要求每镜商品完整、居中、清楚；允许过渡镜头、局部可见和静默镜头。
7. HAND_ONLY 只能出现手和商品，不能出现人物整体、脸、全身、半身、走路、转身或目光。
8. execution_reference.unknown_fields继续保持源观察未知，不得把蓝图设计写回执行卡；但允许使用蓝图中明确声明的CREATIVE_DESIGN人物、地点、灯光和穿搭完成生产设定。
9. editorial_purpose 是后台元数据，不能写进 shot_content 或 observable_action。
10. 不写口播、字幕、购买结论、CTA或策略解释。
11. 必须继承creative_blueprint_id和creative_diversity_contract_id；每镜补充setting_continuity、action_motivation、gaze_and_reaction、audio_hard_constraint和audio_preference。这些字段用于内部连贯与审核，不代表要交给视频模型逐项表演。
12. action_motivation只说明本镜如何延续同一件生活动作，不得混入shot_content。首镜应从自然动作中途开始，避免静态站桩，但不制造额外戏剧动作。gaze_and_reaction没有自然反应时统一写“NATURAL_UNDIRECTED”，不强制看镜头或改变表情。
13. audio_hard_constraint只允许NONE、MUST_BE_SILENT、MUST_KEEP_NATURAL_SOUND；默认必须是NONE。只有具体扣合声、撕拉声或必须完整保留的环境动作才可设硬约束，6镜中最多2镜，禁止因为蓝图提到环境声就给全部镜头加硬约束。audio_preference只允许VOICEOVER_PREFERRED、SILENCE_PREFERRED、AMBIENT_PREFERRED。

只输出一个合法JSON对象，不要markdown，不要解释：
{{
  "visual_plan_schema_version":"visual-adaptation-v2",
  "execution_card_id":"",
  "content_bundle_id":"",
  "creative_blueprint_id":"",
  "creative_diversity_contract_id":"",
  "creative_design_authority":"CREATIVE_DESIGN",
  "primary_observation":"",
  "shots":[
    {{
      "shot_no":1,
      "duration":"0-2.5s",
      "shot_content":"只写可见画面",
      "observable_action":"只写可见动作",
      "product_visibility":"FULL|PARTIAL|OCCLUDED|NONE",
      "framing":"具体机位或景别；未知写UNAVAILABLE",
      "anchor_reference":"本镜实际用到的一个商品事实；不用则写UNAVAILABLE",
      "supported_claim_keys":["CLM_xxx"],
      "reference_spine_orders":[1],
      "editorial_purpose":"后台用途说明",
      "setting_continuity":"继承蓝图中的具体场景状态",
      "action_motivation":"后台说明为什么做这个动作",
      "gaze_and_reaction":"只写自然发生的可见目光或反应；没有则写NATURAL_UNDIRECTED",
      "audio_hard_constraint":"NONE",
      "audio_preference":"VOICEOVER_PREFERRED",
      "structure_beat":"HOOK",
      "carrier_mode":"HAND_ONLY|STATIC_PRODUCT|MIXED|WEARER_ACTIVE",
      "continuity_group":"",
      "opening_mechanism":""
    }}
  ],
  "claim_coverage_summary":{{"CLM_xxx":[1,3]}},
  "reference_preservation_note":"",
  "unknowns_preserved":[]
}}"""
