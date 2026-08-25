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
    recording_profile = direction.get("creator_recording_profile") if isinstance(direction.get("creator_recording_profile"), dict) else {}
    direct_share = bool(recording_profile.get("enabled"))
    capture_preset = str(
        recording_profile.get("capture_preset")
        or recording_profile.get("capture_grammar")
        or ""
    ).strip().upper()
    worn_direct_share = direct_share and capture_preset == "WORN_DIRECT_SHARE"
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
        "creator_recording_profile": recording_profile,
    }
    recording_directive = (
        """【本方向拍摄模式：CREATOR_DIRECT_SHARE】
1. 同一创作者在同一地点，用自己的手机分3至4段直接分享商品；不要求生活剧情、自拍比例或情绪表演。
2. WORN_DIRECT_SHARE从开头已经穿好并保持穿着；PRODUCT_FIRST_THEN_WORN只允许首段商品单独出现，人物穿上后不再脱下、平铺或重新穿戴。
3. 每段是独立录制后直接剪切的真实手机素材，不是同一素材的时间切片或数字缩放。具体固定、手持或镜面关系由当前画面自然决定，不按比例验收。
4. 不为证明卖点设计逐项检查、反复系扣解扣、表演式转身或生活小剧场。人物可以面对自己的手机自然分享；若内容自然需要，允许一次不做作的人物状态或人物与场景关系变化。
"""
        if direct_share
        else "【本方向沿用兼容模式：LIFE_EVENT_OBSERVATION】\n继续按下方生活过程纪律输出；recording_context和clip_design可留空。\n"
    )
    responsibility_directive = (
        """【职责边界｜达人直分享减法版】
1. 商品事实、结构承载、人物、穿搭和场景继续准确继承；源视频未知信息不冒充观察事实。
2. viewer_relationship和scene_motif必须继承。opening_action只作轻量创意参考，不要求逐字执行或围绕它编排一件生活事件。
3. creator_motivation只写为什么愿意分享；event_design、performance_flow和macro_visual_passages仅保留兼容投影，不得扩写成生活剧情、情绪曲线或商品检查链。
4. clip_design是画面权威。通常输出3段；只有确有另一项新证明、使用关系或观看信息时才输出第4段。不得为了凑数补拿包、伸手停录、无信息地回到商品或远景收尾。
5. 人物状态只写自然清醒、像给朋友分享；不设计表演式的强笑、点头、挑眉、停顿、转身或逐项指向，但允许一次自然发生且确实改变观看关系的轻微状态变化。
6. content_bundle中的卖点只需在整条视频范围内获得可见支持，不要求每个细节对应专门动作。
7. 结构Beat只控制观看顺序；真实执行卡只提供镜头语法参考，不复制来源商品、人物、场景或宣称。
8. 不写完整口播。人物、穿搭、场景和商品细节保持具体，减少的是行为控制而不是制作信息。
9. 结构Beat仍是权威：不得为了增加片段而补入原结构没有的USE或ENDING；新片段只能展开已有HOOK/PROOF/USE/ENDING功能。"""
        if direct_share
        else """【职责边界】
1. 源视频未知的人物、地点、灯光继续保持未知；你输出的人物场景属于CREATIVE_DESIGN生产设计，不得声称来自源视频观察。
2. 必须准确继承diversity contract的viewer_relationship、scene_motif和opening_action。forbidden_recent_patterns禁止的是上一轮已失败的完整组合，不是永久封禁单个元素；本轮不得未经合同分配自行退回该失败组合。
3. 不写戏剧化剧情。WEARER_ACTIVE/MIXED只解释为什么顺手记录普通事情；STATIC_PRODUCT时人物和手均不出现；HAND_ONLY时人物整体不出现。
4. 不输出抽象情绪标签。reaction_points允许空数组，不为了证明人物有情绪而强制笑、点头、挑眉、突然停住或看镜头。
5. persona和scene必须具体，但不能增加未经商品事实授权的功效、材质、价格、销量或使用结果。
6. retention_hook从合同允许的自然状态开始，不要求突然停住、抬眼或表演戏剧性反应。
7. performance_flow不是商品检查清单；macro_visual_passages固定3段并保持同一连续过程。
8. content_bundle中的卖点在三段整体范围内获得支持；其他事实自然可见，不为它们设计专门动作。
9. observable_action只写合同允许的可见动作；三段继承真实执行卡的动作关系。"""
    )
    behavior_motivation_schema = (
        "只写主动分享当前商品，不扩写生活事件"
        if direct_share
        else "必须逐字包含程序分配的opening_action，并只概括同一连续过程"
    )
    natural_event_schema = (
        "主动分享当前商品；不要求生活事件或动作链"
        if direct_share
        else "必须逐字包含程序分配的opening_action；随后继续同一过程"
    )
    clip_information_gain_directive = (
        """【WORN_DIRECT_SHARE三段信息分工】
1. 第1段负责整体结果或自然钩子；第2段负责一个在身细节或证明；第3段必须提供一种新的“人物—商品—场景”观看关系。
2. 第3段的新关系可来自人物状态、人物在场景中的位置、手机与人物的关系，或商品与完整穿搭的观察关系；只选一种自然可拍的变化，不需要动作清单。
3. 以下不算新关系：中景改成稍宽中景；同一位置继续站着说话；把第1段整体效果换句话再写一次；对同一素材做数字裁切或缩放。
4. 不强制走路、转身、拿包或整理衣服。若没有自然变化，应重新设计第三段的观看关系，而不是补一个表演动作。
"""
        if worn_direct_share
        else ""
    )
    macro_action_entry = (
        "可为空；仅作兼容投影，不为凑动作补写生活行为"
        if direct_share
        else "一项正在发生的生活动作"
    )
    macro_action_proof = (
        "可为空；仅作兼容投影，可保持自然说话状态"
        if direct_share
        else "延续上一段的生活动作"
    )
    macro_action_end = (
        "可为空；仅作兼容投影，不补表演式收尾"
        if direct_share
        else "连续完成同一件生活事件，不增加展示性停顿"
    )
    return f"""你是原创短视频的完整拍摄蓝图编剧。你不写口播，而是把已分配的创意坐标补全成一个具体、连贯、可拍的15秒手机短视频。

【目标国家】{target_country}
【产品类型】{product_type}

【方向包】
{_compact(direction_package)}

【程序分配的创意多样性合同，硬约束】
{_compact(diversity)}

【承载方式硬约束】
{carrier}

{recording_directive}

{responsibility_directive}

{clip_information_gain_directive}

只输出合法JSON对象：
{{
  "schema_version":"complete-script-blueprint-v4-carrier",
  "authority":"CREATIVE_DESIGN",
  "diversity_contract_id":"",
  "presentation_mode":"PERSON_ON_CAMERA|HANDS_ONLY|STATIC_PRODUCT|MIXED",
  "creative_thesis":"",
  "creator_motivation":"",
  "viewer_relationship":"",
  "recording_context":{{
    "recording_mode":"CREATOR_DIRECT_SHARE|LIFE_EVENT_OBSERVATION",
    "recording_motivation":"为什么此刻主动录这条分享",
    "camera_relationship":"自拍|镜面自拍|固定手机|商品先出镜后切人物",
    "viewer_awareness":"创作者知道正在对观众拍摄"
  }},
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
    "behavior_motivation":"{behavior_motivation_schema}",
    "reaction_points":[],
    "ending_state":"事件自然完成；不为展示商品额外停住"
  }},
  "event_design":{{
    "event_motif":"一句话说明合同允许的连续过程",
    "start_state":"过程开始时人物/手部/静物承载和商品的具体状态",
    "natural_event":"{natural_event_schema}",
    "core_result_moment":"过程中自然看清核心商品结果的时刻",
    "end_state":"过程结束后的具体状态，不为展示商品额外停住"
  }},
  "clip_design":[
    {{
      "clip_no":1,
      "clip_job":"本段给观众的新信息",
      "recording_relation":"当前自然手机关系；可为空，不按自拍或镜面比例验收",
      "framing":"具体景别和观看关系",
      "visible_process":"本段能直接拍到的具体画面",
      "observable_action":"可为空；不为凑动作补写表演",
      "product_visibility":"FULL|PARTIAL|OCCLUDED",
      "supported_claim_keys":["CLM_xxx"]
    }},
    {{
      "clip_no":2,
      "clip_job":"与上一段不同的新信息",
      "recording_relation":"当前自然手机关系；可为空",
      "framing":"与上一段有可感知差异的景别和观看关系",
      "visible_process":"本段能直接拍到的具体画面",
      "observable_action":"可为空；允许只是自然说话状态",
      "product_visibility":"FULL|PARTIAL|OCCLUDED",
      "supported_claim_keys":["CLM_xxx"]
    }},
    {{
      "clip_no":3,
      "clip_job":"商品与人物或穿搭关系",
      "recording_relation":"当前自然手机关系；可为空",
      "framing":"与前两段有可感知差异的景别和观看关系",
      "visible_process":"本段能直接拍到的具体画面",
      "observable_action":"可为空；允许只是自然说话状态",
      "product_visibility":"FULL|PARTIAL|OCCLUDED",
      "supported_claim_keys":["CLM_xxx"]
    }}
  ],
  "macro_visual_passages":[
    {{
      "passage_no":1,
      "narrative_role":"EVENT_ENTRY",
      "visible_process":"人物、场景、穿搭和商品的可见过程",
      "observable_action":"{macro_action_entry}",
      "camera_observation":"旁观式机位和景别",
      "product_visibility":"FULL|PARTIAL|OCCLUDED|NONE",
      "supported_claim_keys":[]
    }},
    {{
      "passage_no":2,
      "narrative_role":"EVENT_PROOF",
      "visible_process":"同一事件继续，核心结果自然出现",
      "observable_action":"{macro_action_proof}",
      "camera_observation":"旁观式机位和景别",
      "product_visibility":"FULL|PARTIAL|OCCLUDED|NONE",
      "supported_claim_keys":["CLM_xxx","CLM_xxx"]
    }},
    {{
      "passage_no":3,
      "narrative_role":"EVENT_END",
      "visible_process":"事件自然完成后的状态",
      "observable_action":"{macro_action_end}",
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
