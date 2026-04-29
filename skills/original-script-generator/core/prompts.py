#!/usr/bin/env python3
"""
原创脚本生成提示词构建。
"""

import json
from typing import Any, Dict, List, Optional


def _compact_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def _fill_template(template: str, values: Dict[str, Any]) -> str:
    filled = template
    for key, value in values.items():
        filled = filled.replace(f"{{{{{key}}}}}", str(value))
    return filled


def _optional_note_text(value: str) -> str:
    return value.strip() if isinstance(value, str) and value.strip() else "(空)"


ACCOUNT_STYLE_BOUNDARY = "真实、轻精致、自然、可挂主页、不硬广、有审美完成度、商品必须是主角"
AI_VIDEO_RHYTHM_RULE = """【AI 视频节奏执行规则】
- 禁止使用“停住 / 停半拍 / 定格 / 静止 / 停留1秒 / 最后1秒轻停 / 站定不动 / 保持不动 / 完全静止”等停顿、冻结、卡帧类表达；
- 不要要求“最后停 0.5 秒 / 1 秒”，收尾必须保持自然微动态；
- 如果需要让商品被看清，改用“镜头保持稳定、构图保持清楚、人物有极轻微目光移动、头部仅做 1–3 度微动、耳饰保持清晰可见、商品在连续轻微动态中被看清”等表达；
- 所有分镜动作都应是稳定构图下的轻微连续动态，避免让 AI 视频模型理解成画面冻结。"""
AUDIO_LAYER_RULE = """【音频层设计】
- 请在正式脚本输出中新增 audio_layer 字段，用于后期剪辑或自动化音频处理；
- audio_layer 只作为后期增强层，不改变画面脚本主结构；
- BGM 必须低存在感，不盖过口播；bgm_energy 只允许 low / medium，默认 low；
- 每条视频最多设计 1–3 个关键 SFX，不要每个镜头都加；
- 发饰类音频只做轻辅助：开头可按首镜类型选择 subtle_pop / soft_ting / clean_tick，但不要强行每条都加；
- 发饰类只有画面明确出现夹口闭合 / 夹住头发 / 开合动作时，才可使用 soft_click / clean_clip_click / light_snap；没有明确夹合动作时，不要强行使用 soft_click；
- 发饰类头发轻动、轻侧头、结果镜头可使用 very_light_hair_rustle / soft_brush / subtle_room_tone，服务固定感和操作真实感；
- 发饰类 BGM 默认轻快、清爽、生活化，不压口播、不过强鼓点、不做强情绪 EDM；mix_note 必须提醒 SFX 不盖过口播，ASMR 类音效只做点缀，画面动作弱时 SFX 也要克制；
- 耳饰类只允许非常轻的 light_sparkle / soft_chime / subtle_room_tone 点缀，避免廉价闪光感；
- 女装类可使用 fabric_swipe / light_transition / subtle_room_tone，服务换镜和穿搭结果；
- general_accessory 默认少用音效，只允许 light_tap / subtle_chime / soft_sparkle；
- 所有 SFX 必须与画面动作匹配，不能为了热闹乱加；
- voiceover_priority 固定为 high，BGM 与 SFX 不得盖过口播；
- audio_negative_constraints 默认包含：不要夸张闪光音、不要游戏音效、不要过强鼓点、不要盖住口播、不要廉价 bling 效果、不要恐怖 / 悬疑 / 过度戏剧感。"""
CONTROL_LAYER_LANGUAGE_RULE = (
    "统一规则：本流程中，所有策略说明、角色分类、质检说明、执行约束、字段描述等控制性文本必须使用中文；"
    "字幕、口播、本地化表达等面向最终用户的内容层文本使用 target_language；"
    "工程枚举值可保留英文 key，但面向模型的自然语言解释必须中文。"
)
TYPE_GUARD_FAMILY_LABELS = {
    "apparel": "服装",
    "jewelry": "首饰",
    "hair_accessory": "发饰",
    "accessory": "配饰",
    "unknown": "未知",
}

PRODUCT_SELLING_NOTE_RULES = """“产品卖点说明”使用规则：
- 若 product_selling_note 为空，则禁止仅凭图片主动推断设计来源、寓意、宗教、民俗或功效含义
- 若 product_selling_note 为空，只允许围绕外观结构、佩戴/上身结果、风格气质、用户可感知价值来写
- 若 product_selling_note 不为空，可把其中内容当作卖点背景、设计灵感、轻寓意、送礼表达或表达限制的优先参考
- 涉及寓意时，只能写为设计灵感 / 好意头 / 轻寓意 / 祝福感，禁止升级为招财、转运、保平安、开运、灵验、带来结果等强承诺
- 若 product_selling_note 与商品实物明显冲突，以商品实物外观为准，不得硬写不成立的信息"""

PRODUCT_PARAMETER_INFO_RULES = """“产品参数信息”使用规则：
- 若 product_parameter_info 为空，则只能保留图片里直接可见、可读、可确认的参数事实
- 若 product_parameter_info 不为空，可把它视为人工确认的参数事实来源，优先写入 parameter_anchors
- 不得把 product_parameter_info 改写成更强结论，不得脑补图片里看不见的功效、材质背书或品牌承诺
- 若 product_parameter_info 与图片明显冲突，优先保留更稳妥、可确认的表达，不得硬写冲突事实"""

OPENING_FIXED_POOL = """opening_mode：
- 轻顾虑冲突型
- 轻判断型
- 结果先给型
- 高惊艳首镜型

proof_mode：
- 细节证明型
- 结果证明型
- 顾虑化解型
- 搭配成立型

ending_mode：
- 适合谁收尾
- 结果感收尾
- 场景代入收尾
- 顾虑化解收尾
- 轻安利收尾

scene_subspace：
- H1 窗边自然光
- H2 镜前 / 玄关镜前
- H3 梳妆台 / 桌边
- H4 床边 / 坐姿分享
- H5 衣柜 / 穿衣区

visual_entry_mode：
- V1 局部质感压镜型
- V2 上脸 / 上身结果先给型
- V3 动作进入型
- V4 高惊艳首镜型

persona_state：
- R1 轻分享型
- R2 小惊喜型
- R3 轻判断型
- R4 轻冷静型

action_entry_mode：
- A1 手部进入
- A2 转头 / 侧脸进入
- A3 佩戴动作进入
- A4 镜前整理进入
- A5 半步后退 / 整体结果进入

styling_completion_tag：
- 安静通勤感
- 干净日常感
- 柔和精致感
- 轻约会感
- 轻冷淡感

persona_visual_tone：
- 克制顺眼型
- 轻判断型
- 轻分享型
- 小惊喜型

styling_key_anchor（只允许输出 1 个）：
- 领口干净
- 肩线利落
- 面料柔和但不过软塌
- 头部区域清爽
- 耳侧区域无遮挡
- 发型边界清楚
- 上身轮廓干净
- 配色低对比但不发灰

emotion_arc_tag：
- 轻疑问 → 轻确认 → 轻安心
- 轻顾虑 → 轻被说服 → 轻满意
- 平静观察 → 小惊喜 → 满意确认
- 轻判断 → 轻发现 → 轻认同
- 平静进入 → 轻结果成立 → 轻推荐"""

PERSONA_LIGHT_CONTROL_RULES = """【新增规则：人物穿搭与情绪轻优化】
1. 人物穿搭不能只停留在“不要出错”，还要给出一个明确的完成度方向；
2. 必须为当前视频选择一个 styling_completion_tag；
3. 必须为当前人物选择一个 persona_visual_tone；
4. 必须选择一个 styling_key_anchor，作为这条视频里最关键的穿搭视觉锚点；
5. 必须选择一个 emotion_arc_tag，作为整条视频的人物轻情绪推进轨迹；
6. styling_completion_tag 决定穿搭整体感觉；
7. persona_visual_tone 决定人物视觉气质；
8. styling_key_anchor 决定这条视频里最关键的穿搭视觉点；
9. emotion_arc_tag 决定人物情绪如何从开头自然走到结尾；
10. 所有这些设定都必须服从商品优先原则，不得抢商品。"""

PERSONA_LIGHT_CONTROL_POOL = """styling_completion_tag：
- 安静通勤感
- 干净日常感
- 柔和精致感
- 轻约会感
- 轻冷淡感

persona_visual_tone：
- 克制顺眼型
- 轻判断型
- 轻分享型
- 小惊喜型

styling_key_anchor（只允许输出 1 个）：
- 领口干净
- 肩线利落
- 面料柔和但不过软塌
- 头部区域清爽
- 耳侧区域无遮挡
- 发型边界清楚
- 上身轮廓干净
- 配色低对比但不发灰

emotion_arc_tag：
- 轻疑问 → 轻确认 → 轻安心
- 轻顾虑 → 轻被说服 → 轻满意
- 平静观察 → 小惊喜 → 满意确认
- 轻判断 → 轻发现 → 轻认同
- 平静进入 → 轻结果成立 → 轻推荐"""

PERSONA_LIGHT_CONTROL_DEFAULTS = """默认分配建议：

S1 强停留原生型：
- styling_completion_tag：干净日常感 / 安静通勤感
- persona_visual_tone：轻分享型 / 克制顺眼型
- styling_key_anchor：头部区域清爽 / 领口干净
- emotion_arc_tag：轻疑问 → 轻确认 → 轻安心

S2 平衡型：
- styling_completion_tag：柔和精致感 / 安静通勤感
- persona_visual_tone：克制顺眼型 / 轻判断型
- styling_key_anchor：领口干净 / 上身轮廓干净
- emotion_arc_tag：平静观察 → 小惊喜 → 满意确认

S3 强购买承接型：
- styling_completion_tag：安静通勤感 / 轻冷淡感
- persona_visual_tone：轻判断型 / 克制顺眼型
- styling_key_anchor：耳侧区域无遮挡 / 肩线利落 / 发型边界清楚
- emotion_arc_tag：轻顾虑 → 轻被说服 → 轻满意

S4 高惊艳首镜型：
- styling_completion_tag：柔和精致感 / 轻约会感
- persona_visual_tone：小惊喜型 / 轻判断型
- styling_key_anchor：头部区域清爽 / 配色低对比但不发灰
- emotion_arc_tag：平静进入 → 轻结果成立 → 轻推荐"""

HAIR_ACCESSORY_KEYWORDS = {
    "发饰",
    "发夹",
    "抓夹",
    "边夹",
    "刘海夹",
    "香蕉夹",
    "竖夹",
    "鲨鱼夹",
    "发箍",
    "发圈",
    "发带",
    "发绳",
    "头绳",
    "头箍",
    "盘发",
}

EAR_ACCESSORY_KEYWORDS = {"耳线", "耳环", "耳饰", "耳钉", "耳夹", "耳坠"}
HAIR_CLIP_KEYWORDS = {"发夹", "抓夹", "边夹", "刘海夹", "香蕉夹", "竖夹", "鲨鱼夹"}
JEWELRY_ACCESSORY_KEYWORDS = {
    "配饰",
    "饰品",
    "首饰",
    "手圈",
    "手环",
    "手链",
    "手镯",
    "手串",
    "戒指",
    "项链",
    "吊坠",
    "脚链",
    "胸针",
    "bracelet",
    "bangle",
    "cuff",
    "ring",
    "necklace",
    "pendant",
    "anklet",
    "brooch",
}

PROMPT_PRODUCT_TYPE_GUARD = """你是产品类型视觉识别与冲突分析助手。

你的任务是：先只根据图片判断这个商品在视觉上最像什么类型，再输出标准化的图片侧类型判断结果。

注意：
1. 这一步先做图片视觉判断，不要被表格产品类型带偏；
2. 最终仲裁由代码侧完成；
3. 只能基于图片可见信息判断；
4. 如果图片信息不足，可以输出 unknown 并降低 confidence。

输入信息：
- table_product_type: {{table_product_type}}
- business_category: {{business_category}}

请输出合法 JSON：
{
  "vision_family": "apparel|jewelry|hair_accessory|accessory|unknown",
  "vision_slot": "body|upper_body|lower_body|full_body|wrist|neck|ear|finger|hair|unknown",
  "vision_type": "",
  "vision_confidence": 0.0,
  "visible_evidence": [""],
  "risk_note": ""
}

规则：
- vision_type 只写图片最像的类型短词，例如：女装、轻上装、上衣、外套、连衣裙、下装、项链、项圈、手链、手镯、细手圈、戒指、耳饰、抓夹、发夹、发箍、发圈、扎发绳、发带、发簪
- 如果无法判断到具体类型，可保留空字符串，但 family/slot 应尽量判断
- vision_confidence 必须输出 0 到 1 之间的小数
- visible_evidence 写 2-4 条图片可见依据
- 不要输出 markdown，不要输出解释文字。"""


def _normalized_product_family(product_type: str) -> str:
    text = str(product_type or "").strip()
    lowered = text.lower()
    if any(keyword in text for keyword in HAIR_ACCESSORY_KEYWORDS):
        return "发饰"
    if any(keyword in text for keyword in EAR_ACCESSORY_KEYWORDS):
        return "耳饰"
    if any(keyword in text for keyword in JEWELRY_ACCESSORY_KEYWORDS if keyword.isascii() is False):
        return "首饰"
    if any(keyword in lowered for keyword in JEWELRY_ACCESSORY_KEYWORDS if keyword.isascii()):
        return "首饰"
    return "服装"


def _product_family_from_type_guard(product_type: str, type_guard_json: Optional[Dict[str, Any]] = None) -> str:
    if isinstance(type_guard_json, dict):
        family = str(type_guard_json.get("canonical_family", "") or "").strip()
        mapped = TYPE_GUARD_FAMILY_LABELS.get(family)
        if mapped:
            return mapped
    return _normalized_product_family(product_type)


def _build_type_guard_block(type_guard_json: Optional[Dict[str, Any]] = None) -> str:
    if not isinstance(type_guard_json, dict) or not type_guard_json:
        return ""

    prompt_contract = str(type_guard_json.get("prompt_contract", "") or "").strip()
    raw_product_type = str(type_guard_json.get("raw_product_type", "") or "").strip()
    display_type = str(type_guard_json.get("display_type", "") or "").strip()
    canonical_family = TYPE_GUARD_FAMILY_LABELS.get(
        str(type_guard_json.get("canonical_family", "") or "").strip(),
        str(type_guard_json.get("canonical_family", "") or "").strip(),
    )
    canonical_slot = str(type_guard_json.get("canonical_slot", "") or "").strip()
    conflict_level = str(type_guard_json.get("conflict_level", "") or "").strip()
    conflict_reason = str(type_guard_json.get("conflict_reason", "") or "").strip()
    vision_family = str(type_guard_json.get("vision_family", "") or "").strip()
    vision_slot = str(type_guard_json.get("vision_slot", "") or "").strip()
    vision_type = str(type_guard_json.get("vision_type", "") or "").strip()
    review_required = bool(type_guard_json.get("review_required"))
    visible_evidence = type_guard_json.get("visible_evidence") if isinstance(type_guard_json.get("visible_evidence"), list) else []
    evidence_text = "；".join(str(item or "").strip() for item in visible_evidence if str(item or "").strip())
    confidence_value = type_guard_json.get("vision_confidence")
    confidence_text = ""
    if isinstance(confidence_value, (int, float)):
        confidence_text = f"{float(confidence_value):.2f}"
    elif str(confidence_value or "").strip():
        confidence_text = str(confidence_value).strip()

    header_lines = [
        "【产品类型总控约束】",
        f"- 表格原始产品类型：{raw_product_type or '未填写'}",
        f"- 最终采用类型：{display_type or raw_product_type or '未填写'}",
        f"- 标准族类：{canonical_family or '未知'}",
        f"- 标准佩戴/使用部位：{canonical_slot or 'unknown'}",
        f"- 冲突等级：{conflict_level or 'none'}",
        f"- 视觉识别：{vision_type or 'unknown'} / {vision_family or 'unknown'} / {vision_slot or 'unknown'}",
        f"- 视觉置信度：{confidence_text or 'unknown'}",
        f"- 是否需要人工复核：{'是' if review_required else '否'}",
        f"- 冲突原因：{conflict_reason or '无'}",
    ]
    if evidence_text:
        header_lines.append(f"- 图片侧依据：{evidence_text}")

    footer_lines = [
        "执行要求：",
        "1. 后续所有阶段都必须优先遵循表格产品类型和最终标准佩戴/使用部位，不得被图片角度带偏。",
        "2. 如果图片视觉与表格类型冲突，允许记录冲突，但不允许擅自把商品改写成其他部位使用的品类。",
        "3. 如果图片存在白底、无尺度参照、单张易误判等情况，必须继续优先遵循最终产品类型。",
        "4. 输出文本如果出现与最终类型冲突的禁词或错误部位，视为类型跑偏。",
    ]
    parts = ["\n".join(header_lines)]
    if prompt_contract:
        parts.append("类型契约：\n" + prompt_contract)
    parts.append("\n".join(footer_lines))
    return "\n\n".join(part for part in parts if part).strip()


def _append_type_guard_block(prompt: str, type_guard_json: Optional[Dict[str, Any]] = None) -> str:
    prompt = f"{prompt}\n\n{CONTROL_LAYER_LANGUAGE_RULE}"
    block = _build_type_guard_block(type_guard_json)
    if not block:
        return prompt
    return f"{prompt}\n\n{block}"


def _is_hair_accessory(product_type: str) -> bool:
    return _normalized_product_family(product_type) == "发饰"


def _is_hair_clip(product_type: str) -> bool:
    text = str(product_type or "").strip()
    return any(keyword in text for keyword in HAIR_CLIP_KEYWORDS)


def _hair_accessory_anchor_rules(product_type: str) -> str:
    if not _is_hair_accessory(product_type):
        return ""
    return """【发饰类特别要求】
如果 product_type 属于发饰类，请额外输出：
1. hair_accessory_subtype：small_side_clip / claw_clip / headband / hair_tie / hair_band / styling_tool / other_hair_accessory
2. placement_zone：face_side / back_head / top_head / low_ponytail / half_up / bun_area / full_head / unknown
3. hold_scope：flyaway_hair / small_hair_section / half_hair / low_ponytail / bun / decorative_only / unknown
4. orientation：horizontal_clip / vertical_clip / wrap_around / tie_up / insert_fix / wear_on_head / unknown
5. primary_result：cleaner_hairline / stronger_hold / more_complete_hairstyle / faster_hair_fix / decorative_focus / softer_face_shape / more_volume / unknown

判断原则：
- 只根据图片和商品类型做轻量推断，不要求精确；
- 不确定可输出 unknown，不要为了完整性硬猜；
- 这些字段只用于后续脚本方向选择，不是商品真实参数；
- 不要把所有发饰都默认判成 small_side_clip 或 claw_clip。

同时继续输出：
- structure_anchors：例如抓夹/边夹/发箍/发圈/发带/盘发工具结构
- operation_anchors：例如从哪里夹、是否单手可操作、是否适合半扎/盘发/整理碎发
- fixation_result_anchors：例如夹上后稳不稳、能否收住头发、是否容易松散
- before_after_result_anchors：例如脸边更干净、后脑更利落、整体更完整
- scene_usage_anchors：例如通勤、上学、居家、洗脸护肤、快速出门等

发饰脚本优先证明：
- 上头前后变化
- 操作门槛
- 固定结果
- 发型完成度
- 使用场景"""


def _hair_accessory_opening_rules(product_type: str) -> str:
    if not _is_hair_accessory(product_type):
        return ""
    return """【发饰类特别要求】
发饰类 Hook 优先来自：
- 发型问题切入
- 快速变化切入
- 操作门槛切入
- 结果先给切入

不要只拍发饰单体特写，必须让用户尽快看到“夹上 / 戴上 / 用上之后的变化”。"""


def _hair_accessory_persona_rules(product_type: str) -> str:
    if not _is_hair_accessory(product_type):
        return ""
    return """【发饰类特别要求】
1. hairstyle_rule 必须服务于发饰展示，不允许头发本身复杂到掩盖发饰作用；
2. clothing_rule 更克制，避免头部区域被耳饰、帽子、大围巾等抢注意力；
3. emotion_progression 更适合：
   - 开头：轻困扰 / 轻嫌麻烦 / 轻疑问
   - 中段：轻确认 / 轻惊喜 / 轻觉得顺手
   - 结尾：轻满意 / 轻安心 / 轻推荐
4. movement_style 优先是：
   - 对镜整理头发
   - 出门前快速处理
   - 顺手夹一下确认效果"""


def _hair_accessory_strategy_rules(product_type: str) -> str:
    if not _is_hair_accessory(product_type):
        return ""
    return """【发饰类特别规则】
如果 display_family = hair_accessory，则四条脚本的 primary_focus 不得写死为“小发夹 / 小抓夹 / 脸侧碎发 / 一小束头发 / 半扎发型”。
必须先读取 anchor_card_json 中的：
- hair_accessory_subtype
- placement_zone
- hold_scope
- orientation
- primary_result

再根据这些字段动态生成四条脚本的主表达重点；不确定字段为 unknown 时，也要选择更稳妥的“发饰子类型待确认”表达，不要硬套小发夹逻辑。

四个 script_role 的发饰类通用定义：
- cognitive_reframing：纠正对该发饰的常见误判，必须跟具体子类型有关，不要泛泛写“不是普通发饰”。
- result_delivery：直接把戴上 / 夹上 / 扎上之后的发型结果给出来，不要把结构拆解当主线。
- risk_resolution：解决该子类型最常见的使用顾虑，例如夹不住、扎不稳、勒头、显幼、不好上手等，decision 信号要前移。
- aura_enhancement：强调整体发型更完整、更有完成度，适合出门前 / 镜前确认 / 日常整理好后的情境。

发饰类四条策略必须拉开 primary_focus，不能四条都同时讲“夹得住 + 夹好后好看 + 结构清楚 + 结果完整”这一整套组合。"""


def _hair_accessory_expression_rules(product_type: str) -> str:
    if not _is_hair_accessory(product_type):
        return ""
    return """【发饰类特别规则】
发饰类表达必须围绕：
- 变化
- 操作
- 固定结果
- 场景可用性

不要把发饰写成单纯审美展示物。"""


def _hair_accessory_script_rules(product_type: str) -> str:
    if not _is_hair_accessory(product_type):
        return ""
    lines = [
        "生成发饰类脚本前必须先读取 script_brief.hair_accessory_profile 中的 hair_accessory_subtype / placement_zone / hold_scope / orientation / primary_result。",
        "禁止忽略这些字段，直接套用“小发夹 / 小抓夹 / 脸侧碎发整理 / 一小束头发 / 半扎发型”模板。",
        "每条脚本只能有 1 个 primary_focus；proof 段镜头停留中心和口播中心必须围绕 primary_focus，secondary_focus 最多 1 个，可为空。",
        "除 primary_focus / secondary_focus 外，其他卖点只能背景露出，不得成为独立 proof 镜头、镜头目的中心或独立口播中心。",
        "发饰类四条脚本不得都讲同一件事，必须按照 script_role 拉开主线。",
        "优先使用低风险镜头：已佩戴 / 已夹好 / 已扎好状态、轻微侧头 / 回头、结果镜头、固定关系镜头、发饰静物特写、开合 / 结构短特写。",
        "避免长时间夹发过程、手/头发/发饰长时间纠缠同框、反复调整发饰位置、大幅甩头测试稳固、复杂盘发过程、持续用手整理头发再露出发饰。",
        "每组 4 条脚本中，最多 1 条可使用“夹 / 扎 / 戴的过程”作为主要 proof；其余脚本优先用“结果成立 + 固定关系 + 轻微动态”完成 proof。",
        "只要能用状态镜头表达，就不要用过程镜头。",
        "固定关系不能一律理解成“夹住一小束头发”：small_side_clip 是夹住脸侧碎发 / 小束头发；claw_clip 是夹住半头 / 后脑发束 / 低盘区域；headband 是戴在头顶 / 发际线附近的压发与装饰关系；hair_tie 是扎住马尾 / 低扎 / 发束；hair_band 是环绕头部或局部发束；styling_tool 是帮助形成盘发 / 固定结构。",
        "首镜优先展示使用后的发型结果，前 3 秒内看到发饰已经在头发上、发型状态更完整 / 更整齐 / 更有装饰感；若子类型不是夹类，应改成戴好 / 扎好 / 使用后结果。",
        "必须明确佩戴方向、佩戴区域和作用范围，且要与 hair_accessory_profile 匹配。",
        "不允许只拍发饰单体特写。",
        "不允许没有操作、没有变化、没有使用结果的空展示。",
        "proof 至少证明以下两项中的两项：使用后的发型变化、发饰与头发的固定/收束/佩戴关系、日常场景下的完成度。",
        "固定证明不要靠大动作或甩头，优先使用轻微转头、轻微低头、镜前确认、侧后方已夹好结果。",
        "字幕一镜一句，短句优先，不要解释太长。",
        "decision 优先收住：适合谁、日常会不会真用到、是不是买来不会闲置。",
        "必须输出 rhythm_checkpoints，且使用机器可检查格式：hook_complete_by=3s，core_proof_start_between=4-8s，decision_signal_by=12s，risk_resolution_decision_by=9s_or_not_applicable。",
        "不要输出“proof 在第1-15秒展开”“decision 在第14秒前出现”这类宽泛描述。",
    ]
    if _is_hair_clip(product_type):
        lines.extend(
            [
                "抓夹/边夹类允许出现快速整理、出门前、到达后镜前补夹、摘盔后恢复等场景，但头盔元素不要喧宾夺主。",
                "发夹类首镜不要只拍材质或装饰，要尽快把头发变化和固定效果接实。",
            ]
        )
    return "\n".join(f"- {line}" for line in lines)


def _hair_accessory_qc_rules(product_type: str) -> str:
    if not _is_hair_accessory(product_type):
        return ""
    return """发饰类轻质检：
- 是否错误地把当前发饰写成“小发夹 / 小抓夹 / 一小束头发 / 脸侧碎发整理”模板
- 是否已正确读取并使用 hair_accessory_subtype / placement_zone / hold_scope / orientation / primary_result
- 四条脚本的 primary_focus 是否明显不同
- 当前脚本的 proof 是否围绕 primary_focus
- 是否过度依赖夹 / 扎 / 戴的过程镜头
- 是否出现长时间夹发 / 扎发、手/头发/发饰纠缠、复杂整理动作、大幅甩头等高风险动作
- audio_layer 是否在没有明确夹合动作时误用 soft_click
- rhythm_checkpoints 是否为机器可检查格式
- 是否只展示发饰好看，而没有展示发型变化或固定 / 收束 / 佩戴关系

处理原则：能最小修正就最小修正；轻微问题不阻断；只有明显跑偏时进入 major issue。"""


def _hair_accessory_variant_rules(product_type: str) -> str:
    if not _is_hair_accessory(product_type):
        return ""
    return """【发饰类特别规则】
如果 product_type 属于发饰类：
1. 变体不允许都只在开头变化；
2. 必须适度测试：
   - 操作呈现方式
   - 固定效果的 proof 方式
   - 发型变化的呈现顺序
3. 不允许 5 条都还是“单体展示 + 一句泛安利”。"""


PROMPT_P1 = """你是一个资深的跨境电商短视频内容分析专家、商品视觉锚点分析专家。

你的任务不是写脚本，而是基于输入的商品图片与最少背景信息，生成一份严格结构化的产品锚点卡，供后续短视频策略与脚本生成使用。

你必须遵守以下规则：
1. 只能基于图片中可见信息和输入字段进行判断；
2. 不得编造价格、材质成分、功能参数、品牌背书、尺寸信息；
3. 对无法从图片确认的内容，直接省略，不要猜测成事实；
4. 输出必须是合法 JSON，不要输出 markdown，不要输出解释文字。

输入信息：
- target_country: {{target_country}}
- target_language: {{target_language}}
- product_type: {{product_type}}
- product_selling_note: {{product_selling_note}}
- product_parameter_info: {{product_parameter_info}}

请结合图片输出 JSON，结构如下：
{
  "product_positioning_one_liner": "",
  "hard_anchors": [
    {
      "anchor": "",
      "reason_not_changeable": "",
      "confidence": "high|medium|low"
    }
  ],
  "display_anchors": [
    {
      "anchor": "",
      "why_must_show": "",
      "recommended_shot_type": ""
    }
  ],
  "key_visual_constraints": [
    {
      "constraint": "",
      "confidence": "high|medium|low",
      "basis": ""
    }
  ],
  "hair_accessory_subtype": "",
  "placement_zone": "",
  "hold_scope": "",
  "orientation": "",
  "primary_result": "",
  "distortion_alerts": [""],
  "candidate_primary_selling_points": [
    {
      "selling_point": "",
      "how_to_tell": "",
      "how_to_show": "",
      "risk_if_missed": ""
    }
  ],
  "persona_suggestions": [
    {
      "persona": "",
      "why_fit": ""
    }
  ],
  "scene_suggestions": [
    {
      "scene": "",
      "why_fit": "",
      "not_recommended_scene": ""
    }
  ],
  "camera_mandates": [
    {
      "stage": "opening|middle|ending",
      "must_do": ""
    }
  ],
  "parameter_anchors": [
    {
      "parameter_name": "",
      "parameter_value": "",
      "why_must_preserve": "",
      "execution_note": "",
      "confidence": "high|medium|low"
    }
  ],
  "structure_anchors": [""],
  "operation_anchors": [""],
  "fixation_result_anchors": [""],
  "before_after_result_anchors": [""],
  "scene_usage_anchors": [""]
}

【类目规则】
- 服装类：重点锁定版型、领口、肩线、长度、面料视觉感、上身结果
- 耳饰类：重点锁定耳钩/耳针结构、上耳垂坠比例、局部质感、脸侧结果
- 发饰类：重点锁定结构、操作方式、固定方式、上头前后变化、使用场景
- 首饰类：重点锁定佩戴位置、圈口/直径/宽度/厚薄/克重等可见参数、局部质感、上手结果

{{hair_accessory_rules}}

【发饰轻量字段规则】
- 只有 product_type 属于发饰类时，才需要认真填写 hair_accessory_subtype / placement_zone / hold_scope / orientation / primary_result；
- 非发饰类可以统一输出空字符串；
- 发饰类字段只服务于脚本方向，不是商品真实参数；能判断就判断，不确定允许输出 unknown；
- 不要为了完整性硬猜，不要把所有发饰默认判成 small_side_clip 或 claw_clip。

【关键视觉防错锚点规则】
- key_visual_constraints 不是抽取真实商品参数，而是少量生成“防止 AI 视频还原错误”的关键视觉约束
- 每个商品最多输出 0–5 条；如果图片信息不足，可输出空数组 []
- 只写图片中可观察或可合理推测的信息，且必须与视频还原错误强相关
- 优先写相对长度、落点、方向、佩戴方式、体量级别、版型比例
- 不写材质、重量、品牌、功效、精确规格
- confidence 只能是 high / medium / low；high 与 medium 会进入后续脚本强执行，low 只作为参考
- 轻量模板示例：
  - 耳饰：短垂比例、末端落点、贴耳主体、避免长流苏
  - 发饰：横夹/竖夹、佩戴方向、侧边固定/后脑盘发
  - 项链：锁骨链/中长链、吊坠落点、领口关系
  - 手链：贴腕/轻松动、手腕局部结果
  - 戒指：单指主戴、上手比例、美甲不抢镜
  - 女装上装：衣长不要被拉长、版型不要变厚重、肩线/领口不要生成错、轻上装不要生成成厚外套或长外套

【参数锚点规则】
- 如果图片或图中文字里明确出现尺寸、直径、厚度、宽度、克重、数量、材质名、颜色名等可确认参数，请写入 parameter_anchors
- parameter_anchors 只允许收录图片中直接可见、可读、可确认的参数，不得猜测
- 如果 product_parameter_info 不为空，可将其中人工确认的参数事实补入 parameter_anchors，并优先保持这些事实不被后续阶段改写
- 如果图片里没有明确参数，请输出空数组 []
- parameter_anchors 用于后续脚本和质检保持参数事实，不代表每个参数都必须口播

{{product_selling_note_rules}}"""


PROMPT_P2 = """你是一个资深的短视频首镜设计专家、服装/耳饰/发饰/首饰视觉抓停留专家，尤其擅长为 TK 冷启动内容设计“前3秒更有原生抓力”的首镜方案。

你的任务是：基于商品锚点卡，为这个商品设计一组真正有“首屏吸引力”的首镜方案。

【输入信息】
- 商品类目：{{product_family}}
- 商品锚点卡：{{anchor_card_json}}
- 目标国家：{{target_country}}
- 账号调性：真实、轻精致、可挂主页、不硬广、有审美完成度
- 当前目标：更适合 TK 冷启动，强化前3秒抓力

【任务要求】
请输出 5 个首镜吸引策略，且尽量拉开差异。

至少覆盖：
1. 局部质感先行
2. 结果先给
3. 动作进入
4. 问题切入
5. 高惊艳首镜

首镜表达切入口优先从以下类型中选择：
1. 轻吐槽式
2. 判断反差式
3. 顾虑冲突式
4. 结果先行式
5. 误区纠正式

如果 target_country 属于东南亚市场，则场景建议优先推荐达人家中自然分享场景。

【类目提醒】
- 服装类：优先上身效果、搭配完成度、比例结果
- 耳饰类：优先局部质感、上耳结果、脸侧提升
- 发饰类：优先上头前后变化、操作过程、快速整理结果、固定效果
- 首饰类：优先上手结果、佩戴位置关系、圈口/粗细/体量感、局部质感和参数可信度

【每个策略输出】
1. strategy_name
2. angle_bucket
3. opening_mode_candidate
4. visual_entry_mode_candidate
5. first_frame_visual
6. shot_size
7. action_design
8. first_product_focus
9. native_expression_entry
10. opening_first_line_type
11. suggested_short_line
12. style_note
13. risk_note

【特别约束】
1. 首镜必须优先解决“画面吸引力 + 表达抓力”，而不是先解释商品；
2. 开头第一句不要只是平铺描述商品或场景；
3. 优先让第一句自带一个真实顾虑、轻矛盾、判断、反差，或让人想听后半句的口子；
4. 不要让人物、背景、动作喧宾夺主，必须让商品成为视觉主角；
5. 避免以下弱开头作为优先方案：
   - 我最近会穿这种……
   - 这件很适合……
   - 今天想分享……
   - 这种看起来……
   - 同样是……这种会……

{{hair_accessory_rules}}

输出必须是合法 JSON，不要输出 markdown，不要输出解释文字。
输出结构：
{
  "opening_strategies": [
    {
      "strategy_name": "",
      "angle_bucket": "",
      "opening_mode_candidate": "",
      "visual_entry_mode_candidate": "",
      "first_frame_visual": "",
      "shot_size": "",
      "action_design": "",
      "first_product_focus": "",
      "native_expression_entry": "",
      "opening_first_line_type": "",
      "suggested_short_line": "",
      "style_note": "",
      "risk_note": ""
    }
  ]
}"""


PROMPT_P3 = """你是一个资深的短视频人物设定顾问、造型顾问、情绪推进设计顾问。

你的任务不是写脚本，而是为当前商品生成一份《人物 / 穿搭 / 情绪强化包》。

【输入信息】
- 商品类目：{{product_family}}
- 商品锚点卡：{{anchor_card_json}}
- 目标国家：{{target_country}}
- 账号调性：真实、轻精致、自然、不硬广

【输出要求】
请输出结构化结果，包含：
1. persona_state
2. appearance_anchor
3. attractiveness_boundary
4. hairstyle_rule
5. makeup_rule
6. clothing_rule
7. accessory_rule
8. emotion_progression
9. movement_style
10. styling_completion_tag
11. persona_visual_tone
12. styling_key_anchor
13. emotion_arc_tag
14. anti_template_warnings

【硬约束】
1. 人物必须真实、顺眼，但不能过度网红脸、过度精修感、过度漂亮到喧宾夺主；
2. 耳饰类必须至少一侧耳朵完整露出；
3. 发饰类必须优先保证头发可看清、发型变化可判断；
4. 配饰类默认穿低饱和纯色上衣，不叠加抢眼配饰；
5. 女装类必须保证搭配完整、比例成立；
6. 情绪必须有推进；
7. 不要全程同一种轻笑模板；
8. 不要主播感，不要测评腔，不要过度表演。
9. 不要让人物最终都收敛成“温柔、轻笑、顺手分享”的单一模板；
10. 穿搭表述不能只写负向约束，还要体现一个正向完成度方向；
11. 情绪推进不能只停留在“自然一点”，而要让模型知道开头、中段、结尾分别处于什么轻状态。

{{light_control_rules}}

【人物穿搭与情绪轻控制字段池】
{{light_control_pool}}

{{hair_accessory_rules}}

【persona_state 建议池】
- R1 轻分享型
- R2 小惊喜型
- R3 轻判断型
- R4 轻冷静型

【输出格式要求】
1. 必须输出单个合法 JSON 对象；
2. persona_state 必须是单个字符串，不要输出数组或对象；
3. styling_completion_tag / persona_visual_tone / styling_key_anchor / emotion_arc_tag 都必须是单个字符串；
4. anti_template_warnings 必须是字符串数组，建议 3-5 条；
5. 不要额外包一层 result / data / persona_pack。

输出结构：
{
  "persona_state": "",
  "appearance_anchor": "",
  "attractiveness_boundary": "",
  "hairstyle_rule": "",
  "makeup_rule": "",
  "clothing_rule": "",
  "accessory_rule": "",
  "emotion_progression": "",
  "movement_style": "",
  "styling_completion_tag": "",
  "persona_visual_tone": "",
  "styling_key_anchor": "",
  "emotion_arc_tag": "",
  "anti_template_warnings": [""]
}

输出必须是合法 JSON，不要输出 markdown，不要输出解释文字。"""


PROMPT_P4 = """你是一个资深的短视频内容策略专家，擅长为跨境电商服装/耳饰/发饰/首饰商品设计原创短视频打法。

你的任务不是直接写脚本，而是先完成《内容策略匹配卡》生成。

【输入信息】
- 商品类目：{{product_family}}
- 商品锚点卡：{{anchor_card_json}}
- 首镜吸引策略：{{opening_strategies_json}}
- 人物 / 穿搭 / 情绪强化包：{{persona_style_emotion_pack_json}}
- 目标国家：{{target_country}}
- 视频时长：15秒
- 账号调性：{{account_style_boundary}}

【任务要求】
请输出 4 套差异明确的《内容策略匹配卡》：
1. S1 强停留原生型
2. S2 平衡型
3. S3 强购买承接型
4. S4 高惊艳首镜型

每套必须输出：
- strategy_id
- final_strategy_id
- strategy_name
- script_role
- primary_focus
- secondary_focus
- primary_selling_point
- dominant_user_question
- proof_thesis
- decision_thesis
- main_attention_mechanism
- main_shooting_method
- aux_shooting_method
- selected_opening_strategy_name
- selling_point_proof_method
- purchase_bridge_method
- scene_suggestion
- scene_subspace
- scene_function
- persona_state_suggestion
- persona_state
- persona_presence_role
- persona_polish_level
- opening_mode
- opening_strategy
- opening_first_line_type
- opening_first_shot
- visual_entry_mode
- proof_mode
- core_proof_method
- ending_mode
- decision_style
- rhythm_signature
- action_entry_mode
- styling_completion_tag
- persona_visual_tone
- styling_key_anchor
- emotion_arc_tag
- styling_base_logic
- styling_base_constraints
- opening_emotion
- middle_emotion
- ending_emotion
- voiceover_style
- product_dominance_rule
- realism_principles
- forbidden_patterns
- risk_note

【固定池】
{{fixed_pool}}

【特别约束】
1. 4 套策略必须真正拉开差异；
2. dominant_user_question 不允许只是同一句换说法；
3. proof_thesis 不允许只是同一购买理由的轻改写；
4. decision_thesis 不允许只是同一收尾判断的轻改写；
5. 4 条 script_role 必须完整覆盖以下 4 种固定角色，各用一次：cognitive_reframing / result_delivery / risk_resolution / aura_enhancement；
6. 每条都必须明确 primary_focus；secondary_focus 可以为空字符串，但不能与 primary_focus 相同；
7. 若 secondary_focus 为空，该策略的 proof 设计必须更集中服务 primary_focus，不要横向扩卖点；
8. opening_mode / proof_mode / ending_mode 都必须服从 script_role；
9. 不允许 4 套都使用同一种 opening_mode；
10. 不允许 4 套都使用同一种 ending_mode；
11. 至少出现 3 种不同的 proof_mode；
12. 至少出现 3 种不同的 visual_entry_mode；
13. 至少 2 种不同的 persona_state；
14. 至少 3 种不同的 action_entry_mode；
15. 4 条脚本的 rhythm_signature 不允许完全相同；
16. S4 与 S1 的差异不能只是“更好看一点”；
17. 如果 target_country 属于东南亚市场：
    - 至少 2 套以家中自然分享场景为主
    - 家中场景至少覆盖 2 种 scene_subspace
    - 不允许 4 条都落在同一个窗边 / 同一面镜子 / 同一机位逻辑里
    - S4 仍应优先保留家中自然分享语境
18. 不允许 4 条全部使用同一个 styling_completion_tag；
19. 至少出现 2 种不同的 styling_completion_tag；
20. 不允许 4 条全部使用同一个 persona_visual_tone；
21. 至少出现 2 种不同的 persona_visual_tone；
22. 不允许 4 条全部使用同一个 emotion_arc_tag；
23. 至少出现 2 种不同的 emotion_arc_tag；
24. styling_key_anchor 允许少量重复，但不要 4 条都完全一样；
25. 这些差异必须是轻差异，不得把人物写成明显不同的人设；
26. 差异服务于内容感知分化，不服务于抢商品注意力。

【script_role 顶层分类规则】
- script_role 是整条 15 秒视频的顶层职责，opening_mode 只是前 3 秒执行方式；
- cognitive_reframing：必须明确指出一个常见误判，并通过画面或口播完成“不是 X，而是 Y”的纠偏；
- result_delivery：必须先让戴上 / 穿上 / 夹上后的结果成立，不能只停留在结构说明；
- risk_resolution：必须明确提出一个风险，并在中段完成化解；
- aura_enhancement：必须包含整体感 / 氛围感 / 造型完成度镜头，不能只讲局部功能。

【primary_focus / secondary_focus 规则】
- primary_focus 是这条 15 秒视频唯一必须讲清的一件事，同时承担主购买理由与主证明逻辑；
- secondary_focus 是可选辅助证明点，最多 1 个；
- 复杂结构商品默认只允许 1 个主 proof 主题 + 1 个辅助主题；
- 其他结构细节只能背景性露出，不得成为独立 proof 镜头主题。

{{light_control_defaults}}

{{hair_accessory_rules}}

{{product_selling_note_rules}}

输出必须是合法 JSON，不要输出 markdown，不要输出解释文字。
输出结构：
{
  "strategies": [
    {
      "strategy_id": "S1|S2|S3|S4",
      "final_strategy_id": "Final_S1|Final_S2|Final_S3|Final_S4",
      "strategy_name": "",
      "script_role": "cognitive_reframing|result_delivery|risk_resolution|aura_enhancement",
      "primary_focus": "",
      "secondary_focus": "",
      "primary_selling_point": "",
      "dominant_user_question": "",
      "proof_thesis": "",
      "decision_thesis": "",
      "main_attention_mechanism": "",
      "main_shooting_method": "",
      "aux_shooting_method": "",
      "selected_opening_strategy_name": "",
      "selling_point_proof_method": "",
      "purchase_bridge_method": "",
      "scene_suggestion": "",
      "scene_subspace": "",
      "scene_function": "",
      "persona_state_suggestion": "",
      "persona_state": "",
      "persona_presence_role": "",
      "persona_polish_level": "",
      "opening_mode": "",
      "opening_strategy": "",
      "opening_first_line_type": "",
      "opening_first_shot": "",
      "visual_entry_mode": "",
      "proof_mode": "",
      "core_proof_method": "",
      "ending_mode": "",
      "decision_style": "",
      "rhythm_signature": "",
      "action_entry_mode": "",
      "styling_completion_tag": "",
      "persona_visual_tone": "",
      "styling_key_anchor": "",
      "emotion_arc_tag": "",
      "styling_base_logic": "",
      "styling_base_constraints": [""],
      "opening_emotion": "",
      "middle_emotion": "",
      "ending_emotion": "",
      "voiceover_style": "",
      "product_dominance_rule": "",
      "realism_principles": [""],
      "forbidden_patterns": [""],
      "risk_note": ""
    }
  ]
}

{{repair_block}}"""


PROMPT_P5 = """你是一个资深的短视频内容策略总控专家。

你的任务不是重新生成策略，而是检查并定稿已有的 4 套策略。

【输入信息】
- product_type: {{product_type}}
- target_country: {{target_country}}
- target_language: {{target_language}}
- anchor_card_json: {{anchor_card_json}}
- opening_strategies_json: {{opening_strategies_json}}
- persona_style_emotion_pack_json: {{persona_style_emotion_pack_json}}
- strategies_json: {{strategies_json}}

【核心任务】
1. 检查 S1 / S2 / S3 / S4 是否真正有差异；
2. 对过于相似的地方做轻度修正；
3. 输出 4 个标准化的已定稿策略包。

【语义级检查重点】
1. dominant_user_question 不得两两只是换说法
2. proof_thesis 不得两两只是同一购买理由的改写
3. decision_thesis 不得两两只是同一收尾判断的改写
4. 用户看完后最可能记住的购买理由，4 条之间必须尽量拉开
5. 4 条 script_role 必须完整覆盖 cognitive_reframing / result_delivery / risk_resolution / aura_enhancement
6. 每条都必须有 primary_focus；secondary_focus 可以为空，但不能与 primary_focus 相同
7. script_role 必须先成立，再选择 opening_mode / proof_mode / ending_mode；opening_mode 不能替代 script_role

【差异检查规则】
1. 不允许 4 套都使用同一种 opening_mode；
2. 不允许 4 套都使用同一种 ending_mode；
3. 至少出现 3 种不同的 proof_mode；
4. 至少出现 3 种不同的 visual_entry_mode；
5. 至少 2 种不同的 persona_state；
6. 至少 3 种不同的 action_entry_mode；
7. 4 条脚本的 rhythm_signature 不允许完全相同；
8. S4 与 S1 的差异不能只是“更好看一点”；
9. S4 必须在首镜目标、首镜画面组织、开头进入方式、后续承接方式上与 S1/S2/S3 明显不同。
10. 若 secondary_focus 为空，该策略的 proof 设计必须更集中服务 primary_focus。
11. 至少 2 种不同的 styling_completion_tag；
12. 至少 2 种不同的 persona_visual_tone；
13. 至少 2 种不同的 emotion_arc_tag；
14. styling_key_anchor 不允许 4 条完全相同；
15. 若 opening/proof/ending 已有差异，但人物视觉感、穿搭完成感、情绪轨迹仍然像同一条模板，应视为感知近似度仍偏高，需要轻修正。
16. P5 必须检查每条策略的 script_role 是否成立：cognitive_reframing 要有误判纠偏，result_delivery 要有结果先给，risk_resolution 要有风险与化解，aura_enhancement 要有整体气质提升。
17. P5 必须检查 primary_focus 是否足够聚焦；secondary_focus 只能作为 1 个辅助证明点，不得横向扩成第二条主线。

【东南亚规则】
如果 target_country 属于东南亚市场，请额外检查：
1. 至少 2 套以家中自然分享场景为主；
2. 家中场景至少覆盖 2 种 scene_subspace；
3. 不允许 4 条都落在同一个窗边 / 同一面镜子 / 同一机位逻辑里；
4. 若近似度过高，优先修正顺序：
   - ending_mode
   - proof_mode
   - visual_entry_mode
   - scene_subspace
   - persona_state / action_entry_mode
5. 若前述修正后仍然近似，可轻修正：
   - styling_completion_tag
   - persona_visual_tone
   - emotion_arc_tag
   - styling_key_anchor

{{hair_accessory_rules}}

输出结构：
{
  "difference_check": "",
  "strategies": [
    {
      "strategy_id": "S1|S2|S3|S4",
      "final_strategy_id": "Final_S1|Final_S2|Final_S3|Final_S4",
      "strategy_name": "",
      "script_role": "cognitive_reframing|result_delivery|risk_resolution|aura_enhancement",
      "primary_focus": "",
      "secondary_focus": "",
      "primary_selling_point": "",
      "dominant_user_question": "",
      "proof_thesis": "",
      "decision_thesis": "",
      "main_attention_mechanism": "",
      "main_shooting_method": "",
      "aux_shooting_method": "",
      "selected_opening_strategy_name": "",
      "selling_point_proof_method": "",
      "purchase_bridge_method": "",
      "scene_suggestion": "",
      "scene_subspace": "",
      "scene_function": "",
      "persona_state_suggestion": "",
      "persona_state": "",
      "persona_presence_role": "",
      "persona_polish_level": "",
      "opening_mode": "",
      "opening_strategy": "",
      "opening_first_line_type": "",
      "opening_first_shot": "",
      "visual_entry_mode": "",
      "proof_mode": "",
      "core_proof_method": "",
      "ending_mode": "",
      "decision_style": "",
      "rhythm_signature": "",
      "action_entry_mode": "",
      "styling_completion_tag": "",
      "persona_visual_tone": "",
      "styling_key_anchor": "",
      "emotion_arc_tag": "",
      "styling_base_logic": "",
      "styling_base_constraints": [""],
      "opening_emotion": "",
      "middle_emotion": "",
      "ending_emotion": "",
      "voiceover_style": "",
      "product_dominance_rule": "",
      "realism_principles": [""],
      "forbidden_patterns": [""],
      "risk_note": ""
    }
  ]
}

输出必须是合法 JSON，不要输出 markdown，不要输出长篇解释。"""


PROMPT_P6 = """你是一个资深的短视频表达策略专家、内容血肉扩充专家。

你的任务是：基于已经定稿的策略包，输出表达扩充计划。

【输入信息】
- 商品类目：{{product_family}}
- 商品锚点卡：{{anchor_card_json}}
- 已定稿策略包：{{final_strategy_json}}
- 人物 / 穿搭 / 情绪强化包：{{persona_style_emotion_pack_json}}
- 目标国家：{{target_country}}
- 视频时长：15秒
- 账号调性：{{account_style_boundary}}

【输出字段】
1. exp_id
2. main_expression_pattern
3. aux_expression_pattern
4. native_expression_entry
5. opening_expression_task
6. middle_expression_task
7. ending_expression_task
8. human_touch_focus_point
9. most_likely_empty_point
10. expression_weight_control

【特别规则】
1. 表达必须更有原生感，不要像“正式介绍商品”；
2. 不能改变主卖点；
3. 不能让表达层喧宾夺主；
4. 对于 S4：
   - 表达层不要抢首镜画面；
   - 开头允许先画面打人，再让语言快速进入；
   - 中段必须更快把首镜接实，避免空钩子；
5. 如果 target_country 属于东南亚市场，请优先保留家中自然分享语境；

{{hair_accessory_rules}}

【输出格式要求】
1. 必须输出单个合法 JSON 对象；
2. 每个字段都必须是字符串，不要输出数组或对象；
3. exp_id 建议写成 `EXP_S1 / EXP_S2 / EXP_S3 / EXP_S4` 之一；
4. 不要额外包一层 result / data / expression_plan。

输出结构：
{
  "exp_id": "",
  "main_expression_pattern": "",
  "aux_expression_pattern": "",
  "native_expression_entry": "",
  "opening_expression_task": "",
  "middle_expression_task": "",
  "ending_expression_task": "",
  "human_touch_focus_point": "",
  "most_likely_empty_point": "",
  "expression_weight_control": ""
}

输出必须是合法 JSON，不要输出 markdown，不要输出解释文字。"""


PROMPT_P7 = """你是一个资深的跨境电商短视频脚本策划专家、AI视频生成提示词专家。

你的任务是：基于 script_brief 输出一条可直接用于短视频生成的 15 秒原创脚本。

【输入信息】
- script_brief: {{script_brief_json}}
- target_country: {{target_country}}
- target_language: {{target_language}}
- product_type: {{product_type}}
- 视频时长：15秒

【核心要求】
1. 前3秒必须先解决“为什么值得继续看”；
2. 第一眼必须让用户明确看到商品；
3. 全片必须覆盖 hook / proof / decision；
4. 不强制使用“严格三句三任务”；
5. 口播允许 2–4 句短句灵活完成任务覆盖；
6. 默认 4–6 个镜头；
7. 单镜头默认 1–3 秒，尽量不超过 4 秒；
8. proof 可以由 2 个连续短镜头共同完成；
9. decision 必须是轻决策收束，不是默认催单；
10. 内部说明中文；字幕默认不输出，`subtitle_text_target_language` 和 `subtitle_text_zh` 默认留空字符串；口播只输出目标语言，`voiceover_text_zh` 默认留空字符串；
11. 你必须执行 script_brief，不要重新发明主卖点或方向；
12. script_brief.focus_control 中的 script_role 必须在镜头推进里成立，opening / proof / ending 都要服从它；
13. proof 镜头的视觉焦点和口播中心只能服务于 primary_focus 与 secondary_focus；
14. 如果 secondary_focus 为空，至少 2 个 proof 镜头要持续服务 primary_focus，不要横向扩卖点；
15. hook 必须在前 3 秒内完成，至少一个核心 proof 起始点要落在 4–8 秒区间，decision 信号必须在 12 秒前出现；
16. 若 script_role = risk_resolution，decision 信号必须在 9 秒前出现；
17. 脚本必须严格继承 styling_completion_tag / persona_visual_tone / styling_key_anchor / emotion_arc_tag；
18. 穿搭表述不能只写“低饱和纯色、不抢商品”，还要体现当前的 styling_completion_tag；
19. 人物状态不能只写“自然、轻笑、顺手分享”，还要体现当前的 persona_visual_tone；
20. styling_key_anchor 必须落实到执行约束里，说明哪一个穿搭视觉点最关键；
21. emotion_arc_tag 必须落实到人物动作与表情推进中；
22. script_brief.ai_shot_risk_profile 中的 forbidden / high_risk 要主动规避；若当前镜头容易踩雷，优先改用 replacement_templates；
23. 上述字段都只负责轻度拉开气质，不允许压过商品展示。
24. 如果 script_brief.parameter_anchors 非空，脚本必须保持这些参数事实，不得改写、偷换或与镜头设计冲突；
25. parameter_anchors 不要求逐条口播，但关键参数至少要做到“画面可见、表达不冲突、结果不被写歪”。
26. 如果 script_brief.key_visual_constraints 非空，P7 必须严格遵守这些关键视觉防错锚点，不得把相对长度、落点、方向、佩戴方式、体量级别、版型比例写反或写变形。
27. key_visual_constraints 是视频还原防错约束，不是新增卖点，不要求逐条口播，但必须在镜头内容、人物动作、执行约束中不冲突。
28. 不要输出大段“为什么这样设计”的解释。

【script_role 执行规则】
- 你必须先根据 script_role 判断整条脚本职责，而不是只看 opening_mode；
- cognitive_reframing：不是 X，而是 Y，必须明确指出一个常见误判并完成纠偏；
- result_delivery：戴上 / 穿上 / 夹上之后，结果已经成立，必须包含成品直出或结果先给镜头；
- risk_resolution：即便在 X 情况下也 OK，必须明确提出一个风险并在中段完成化解；
- aura_enhancement：用了之后整体更有感觉，必须包含整体感 / 氛围感 / 造型完成度镜头；
- opening / proof / decision 都必须服务于 script_role。

【primary_focus 执行规则】
- proof 段的镜头停留中心和口播中心必须围绕 primary_focus 展开；
- secondary_focus 最多只能作为一个辅助主题；
- 如果 secondary_focus 为空，primary_focus 对应的 proof 镜头不少于 2 个；
- 其他结构细节只能作为背景性露出，不得成为独立 proof 镜头主题；
- 对复杂结构商品，默认只允许 1 个主 proof 主题 + 1 个辅助主题。

【AI 可拍性规则】
- 每个 storyboard 镜头必须输出 ai_shot_risk 与 replacement_template_id；
- 不得生成 ai_shot_risk = forbidden 的镜头；
- 命中 high_risk 时，优先使用 script_brief.ai_shot_risk_profile.replacement_templates 替代，并填写 replacement_template_id；
- 耳饰类特别禁止：戴耳环动作、手触耳饰、撩 / 顺 / 整理耳侧发丝的过程帧、大幅转头、剧烈流苏摆动；
- 发饰类特别禁止：把完整夹发过程作为核心 proof、手指/头发/发夹三者长时间纠缠同框、大幅甩头测试固定效果、复杂盘发过程。

【15 秒硬节点规则】
- hook 必须在前 3 秒内完成；
- 至少一个核心 proof 的起始点必须在 4–8 秒区间；
- decision 信号必须在 12 秒前出现；
- 如果 script_role = risk_resolution，decision 信号必须在总时长 60% 节点前出现，即 15 秒视频要在 9 秒前出现；
- 时间类规则冲突时，取更早者；该规则不适用于结构类、镜头类、字段类规则。

{{ai_video_rhythm_rule}}

{{hair_accessory_rules}}

{{audio_layer_rule}}

【输出结构】
{
  "script_positioning": {
    "script_title": "",
    "direction_type": "",
    "core_primary_selling_point": ""
  },
  "opening_design": {
    "opening_mode": "",
    "first_frame": "",
    "expression_entry": "",
    "first_line_type": ""
  },
  "full_15s_flow": [
    {
      "stage": "opening|middle|ending",
      "time_range": "",
      "task": "hook|proof|decision|proof+decision",
      "summary": ""
    }
  ],
  "storyboard": [
    {
      "shot_no": 1,
      "duration": "",
      "shot_content": "",
      "shot_purpose": "",
      "subtitle_text_target_language": "",
      "subtitle_text_zh": "",
      "voiceover_text_target_language": "",
      "voiceover_text_zh": "",
      "spoken_line_task": "hook|proof|decision|proof+decision|none",
      "person_action": "",
      "style_note": "",
      "anchor_reference": "",
      "task_type": "attention|proof|bridge",
      "ai_shot_risk": "low|medium|high|forbidden",
      "replacement_template_id": ""
    }
  ],
  "execution_constraints": {
    "visual_style": "",
    "person_constraints": "",
    "styling_constraints": "",
    "tone_completion_constraints": "",
    "scene_constraints": "",
    "emotion_progression_constraints": "",
    "camera_focus": "",
    "product_priority_principle": "",
    "realism_principle": ""
  },
  "rhythm_checkpoints": {
    "hook_complete_by": "3s",
    "core_proof_start_between": "4-8s",
    "decision_signal_by": "12s",
    "risk_resolution_decision_by": "9s_or_not_applicable"
  },
  "audio_layer": {
    "bgm_style": "",
    "bgm_energy": "low|medium",
    "sfx_cues": [
      {
        "time_range": "",
        "sfx_type": "",
        "purpose": "",
        "volume_note": ""
      }
    ],
    "voiceover_priority": "high",
    "mix_note": "",
    "audio_negative_constraints": []
  },
  "negative_constraints": [""]
}

输出必须是合法 JSON，不要输出 markdown，不要输出解释文字。"""


PROMPT_Q1 = """你是一个资深的短视频脚本质检与修正专家。

你的任务是对已经生成的正式脚本做轻质检，并在必要时做最小修正。

【输入信息】
- 商品类目：{{product_family}}
- 商品锚点卡：{{anchor_card_json}}
- 已定稿策略包：{{final_strategy_json}}
- 表达扩充计划：{{expression_plan_json}}
- 人物 / 穿搭 / 情绪强化包：{{persona_style_emotion_pack_json}}
- 当前正式脚本：{{script_json}}
- 目标国家：{{target_country}}
- 视频时长：15秒

【质检目标】
检查以下问题：
1. 是否满足 4–6 镜头
2. 单镜头是否大多在 1–3 秒，是否有明显过长镜头
3. 是否覆盖 hook / proof / decision
4. 中段是否真的承担 proof，而不是只在描述氛围
5. 结尾是否是轻决策收束，而不是默认下单引导
6. 首镜是否足够清楚地让人知道卖什么
7. S4 是否有“首镜强但中段空”的问题
8. script_role 是否先成立，再延续到 proof / ending
9. primary_focus / secondary_focus 是否收得住，proof 是否没有横向扩卖点
10. hook 是否在前 3 秒内完成
11. 是否至少有一个核心 proof 起始点落在 4–8 秒区间
12. decision 信号是否在 12 秒前出现；若 script_role = risk_resolution，是否在 9 秒前出现
13. 人物状态是否符合 persona_state
14. 情绪推进是否成立
15. 穿着是否符合要求，是否抢商品
16. 配饰类是否完整露耳、无遮挡关键结构
17. 是否存在明显模板化表达
18. 当前脚本的人物视觉感是否符合 persona_visual_tone
19. 当前脚本的穿搭描述是否只停留在“避免错误”，还是已经体现 styling_completion_tag
20. 当前脚本是否明确落实了 styling_key_anchor
21. 当前脚本的情绪推进是否符合 emotion_arc_tag
22. 是否存在“人物看起来仍像同一批模板人、同一类安全穿搭、同一种平情绪轨迹”的问题
23. 如果商品锚点卡里存在 parameter_anchors，当前脚本是否错误改写、忽略关键参数，或让参数事实与画面/口播冲突
24. 是否踩中了常见 AI 高风险拍法，如空镜炫技、人物压过商品、首镜强但中段空、只拍氛围不进入 proof
25. 最终脚本和后续视频提示词是否存在“停住 / 停半拍 / 定格 / 静止 / 停留1秒 / 最后1秒轻停 / 站定不动 / 保持不动 / 完全静止 / 最后停0.5秒或1秒”等停顿、冻结、卡帧类表达；如有，必须改成“稳定构图 + 极轻微连续动态”表达
26. 如果商品锚点卡里存在 key_visual_constraints，当前脚本是否违反了其中 high / medium 约束，例如把短垂比例写成长流苏、把末端落点写错、把佩戴方向/位置写反、把轻上装写成厚外套或长外套
27. 是否出现 ai_shot_risk = forbidden 的镜头；如出现，进入 major_issues，并优先用 replacement_templates 做最小修正
28. 是否出现 high_risk 镜头；如出现，默认进入 minor_issues，并建议替换
29. 如果商品类目为发饰，是否满足：前 3 秒看到夹好结果、明确横夹/竖夹/侧边/后脑/半扎等佩戴关系、不过度依赖夹发过程、证明固定效果、字幕不过长、不是只展示发饰好看
30. audio_layer 是否存在明显问题：BGM 可能盖过口播、SFX 过多、SFX 与画面动作不匹配、发饰类遗漏关键固定音效机会、耳饰类使用过度闪光音效、女装类转场音效过重、general_accessory 音效过密导致廉价模板感

{{hair_accessory_rules}}

{{ai_video_rhythm_rule}}

{{audio_layer_rule}}

【修正规则】
1. 这是轻质检，不是强阻断精品审稿；
2. 只有重大问题才明显修；
3. 轻微问题记 minor_issues，不阻断；
4. 修正必须遵守“最小改动原则”；
5. repaired_script 必须返回修正后的完整脚本 JSON；若无需修正，则返回原脚本。
6. 人物视觉感 / 穿搭完成度 / 情绪推进这些项当前阶段默认作为 warning，不阻断流程；
7. 只有在人物明显抢商品、穿搭明显跑偏、完全无气质差异可感知、styling_key_anchor 完全没有执行、emotion_arc_tag 完全没有执行导致整条视频情绪像死水时，才提升为 major issue。
8. parameter_anchors 当前默认也是轻质检项；只有出现明显参数改写、关键参数被反向表达、或脚本内容与可见参数事实直接冲突时，才提升为 major issue。
9. audio_layer 问题默认记为 minor_issues，不阻断流程；只做最小修正，例如减少 SFX、降低 BGM 存在感、替换廉价音效。

输出结构：
{
  "pass": true,
  "major_issues": [""],
  "minor_issues": [""],
  "repair_actions": [""],
  "repaired_script": {}
}

输出必须是合法 JSON，不要输出 markdown，不要输出解释文字。"""


PROMPT_P7_VIDEO = """你是最终视频提示词生成器。

你的任务是基于“已经通过质检的脚本”，生成一版干净、紧凑、适合视频生成模型使用的最终视频提示词。

你的职责只有一个：干净转写。
你必须忠实执行脚本，不得重新分析脚本，不得补充策略解释，不得做自检、自证、质检说明。

输入信息：
- target_country: {{target_country}}
- target_language: {{target_language}}
- product_type: {{product_type}}
- account_style_boundary: {{account_style_boundary}}
- anchor_card_json: {{anchor_card_json}}
- final_strategy_json: {{final_strategy_json}}
- script_json: {{script_json}}

要求：
- 删除所有自检、自证、质检、解释性内容
- 不要输出“为什么这样设计”
- 除口播字段外，其它所有字段一律使用中文描述
- 最终喂视频模型的分镜层不要输出 `shot_purpose`、`anchor_reference`、`task_type`、字幕字段
- 分镜层只保留：镜头编号、时长、镜头内容、人物动作、口播、口播任务、必要风格提醒
- 最终渲染给视频模型的文本必须尽量控制在 1800 字符以内，硬上限 2000 字符；因此字段内容必须短、准、可执行
- `voiceover_text_zh` 默认留空字符串，除非调用方后续明确要求保留中文对照
- `style_note` 只保留该镜头独有的提醒；如果某条提醒已经在全局执行边界里出现，不要在每个镜头重复写
- 把重复的风格提醒尽量上提到 `execution_boundary`
- `video_setup` 和 `execution_boundary` 都要用短句，不要写成长段方法论或重复限制
- `video_setup` 里必须显式保留 1-3 个最关键的商品锚点，优先写成短句，例如“商品锚点：xxx / xxx”，不要只保留空泛风格词
- `execution_boundary` 里必须显式写出锚点执行要求，例如“至少 1 镜清楚交代 xxx”，不要只写泛化拍摄原则
- `shot_execution` 里至少有 1 个镜头要直接服务于最关键商品锚点，不能所有镜头都只剩泛化氛围描述
- 如果 `anchor_card_json.parameter_anchors` 非空，不得在最终视频提示词里改写这些参数事实；关键参数可在 `video_setup` 或相关镜头里轻量保留
- 如果 `anchor_card_json.key_visual_constraints` 存在 high / medium 约束，最终视频提示词必须遵守并轻量保留，不得把关键视觉比例、落点、方向、佩戴方式、体量级别、版型比例写错
- 输出结构收敛为：视频整体设定 / 分镜执行 / 统一执行边界

{{ai_video_rhythm_rule}}

请输出 JSON，包含：
1. video_setup
2. shot_execution
3. execution_boundary

JSON 结构如下：
{
  "video_setup": "时长 / 场景 / 人物状态 / 穿搭底盘边界 / 商品呈现重点 / 整体风格",
  "shot_execution": [
    {
      "shot_no": 1,
      "duration": "",
      "shot_content": "",
      "voiceover_text_target_language": "",
      "voiceover_text_zh": "",
      "spoken_line_task": "hook|proof|decision|proof+decision|none",
      "person_action": "",
      "style_note": ""
    }
  ],
  "execution_boundary": ""
}"""


PROMPT_P8 = """你是一个资深的短视频脚本变体设计专家。

你的任务是：基于一条已经成立的正式脚本，生成 {{variant_count}} 个轻变体版本。

【目标】
测试：
1. 哪种前3秒更容易让用户继续看
2. 哪种中段更能把开头接实
3. 哪种结尾更自然地收住购买意向

【输入信息】
- target_country: {{target_country}}
- target_language: {{target_language}}
- product_type: {{product_type}}
- anchor_card_json: {{anchor_card_json}}
- final_strategy_json: {{final_strategy_json}}
- expression_plan_json: {{expression_plan_json}}
- persona_style_emotion_pack_json: {{persona_style_emotion_pack_json}}
- original_script_json: {{original_script_json}}
- source_script_id: {{source_script_id}}
- source_strategy_id: {{source_strategy_id}}
- canonical_strategy_id: {{canonical_strategy_id}}
- direction_allowed_pool_json: {{direction_allowed_pool_json}}
- person_variant_layer_json: {{person_variant_layer_json}}
- outfit_variant_layer_json: {{outfit_variant_layer_json}}
- scene_variant_layer_json: {{scene_variant_layer_json}}
- emotion_variant_layer_json: {{emotion_variant_layer_json}}
- variant_plan_json: {{variant_plan_json}}
- debug_mode: {{debug_mode}}

本次只输出以下 variant_id：
- {{variant_ids}}

【必须保留不变的部分】
1. 商品本体及关键锚点；
2. 原脚本唯一主卖点；
3. 原策略包核心任务；
4. 原表达扩充计划主线方向；
5. 商品优先原则；
6. 账号调性：真实、轻精致、自然、不硬广、可挂主页；
7. 如果 target_country 属于东南亚市场，家中自然分享场景高权重原则不能被破坏。

【必须满足】
1. 每条变体都必须覆盖 hook / proof / decision；
2. 每条变体默认使用 4–6 个镜头推进；
3. 单镜头默认 1–3 秒，尽量不超过 4 秒；
4. 中段不能只写情绪陪衬；
5. 结尾不能只写泛安利；
6. 不允许所有变体只是开头不同，中后段逻辑完全一样；
7. S4 即使做变体，也不得滑向广告片；首镜后必须尽快进入 proof，不得空钩子。
8. `source_strategy_id` 必须严格等于输入里的 `source_strategy_id`；`strategy_id` 必须严格等于输入里的 `canonical_strategy_id`。
9. 不要把 `strategy_id` 写成 `source_strategy_id`，不要写成 `Final_S1/Final_S2/Final_S3/Final_S4`，也不要写成 `S1_V1/S4_V2` 这类变体化命名。
10. 如果 target_country 属于东南亚市场，`final_video_script_prompt.video_setup.scene_final` 必须明确写出家中自然分享子场景，例如“家中穿衣区/镜前”“家中衣柜/穿衣区”“家中梳妆台/桌边”“家中窗边自然光”“家中床边/坐姿分享”“家中玄关镜前”，不要只写“生活化真实场景”“真实场景”“日常场景”这类泛标签。
11. 如果 target_country 属于东南亚市场，优先让 V1/V2/V3 落在明确的家中自然分享子场景，并通过不同子场景拉开差异。

{{ai_video_rhythm_rule}}

{{hair_accessory_rules}}

{{product_selling_note_rules}}

输出必须是合法 JSON，不要输出 markdown，不要输出解释文字。
输出 JSON 结构如下：
{
  "variant_count": {{variant_count}},
  "variants": [
    {
      "variant_id": "V1|V2|V3|V4|V5",
      "variant_no": 1,
      "variant_strength": "light|medium|heavy",
      "variant_focus": "opening|proof|ending|scene|rhythm|persona|action|outfit|emotion",
      "source_script_id": "",
      "source_strategy_id": "",
      "strategy_id": "",
      "strategy_name": "",
      "primary_selling_point": "",
      "final_video_script_prompt": {
        "video_setup": {
          "video_theme": "",
          "product_focus": "",
          "person_final": "",
          "outfit_final": "",
          "scene_final": "",
          "emotion_final": "",
          "overall_style": ""
        },
        "shot_execution": [
          {
            "shot_no": 1,
            "duration": "",
            "visual": "",
            "person_action": "",
            "product_focus": "",
            "voiceover": ""
          }
        ],
        "style_boundaries": [""]
      },
      "internal_variant_state": {
        "variant_name": "",
        "main_adjustment": "",
        "test_goal": "",
        "variant_change_summary": "",
        "inherited_core_items": [""],
        "changed_structure_fields": [""],
        "changed_feeling_layers": ["person|outfit|scene|emotion"],
        "main_change": "",
        "secondary_change": "",
        "difference_summary": "",
        "coverage": ["hook", "proof", "decision"],
        "proof_blueprint": [
          {
            "anchor": "",
            "action": "",
            "visible_result": "",
            "concern_relieved": ""
          }
        ],
        "person_variant_layer": {
          "person_identity_base": "",
          "person_style_base": "",
          "appearance_boundary": "",
          "body_presentation_boundary": "",
          "camera_relationship": ""
        },
        "outfit_variant_layer": {
          "outfit_core_formula": "",
          "product_role_in_outfit": "",
          "silhouette_boundary": "",
          "pairing_boundary": "",
          "color_mood_boundary": ""
        },
        "scene_variant_layer": {
          "scene_domain_base": "",
          "scene_subspace": "",
          "scene_function_moment": "",
          "light_boundary": "",
          "prop_boundary": ""
        },
        "emotion_variant_layer": {
          "emotion_base": "",
          "emotion_curve": "",
          "emotion_intensity_boundary": "",
          "delivery_boundary": ""
        },
        "consistency_checks": {
          "person_manifestation": "",
          "outfit_manifestation": "",
          "scene_manifestation": "",
          "emotion_manifestation": ""
        }
      }
    }
  ]
}

{{repair_block}}"""


def build_anchor_card_prompt(
    target_country: str,
    target_language: str,
    product_type: str,
    product_selling_note: str = "",
    product_parameter_info: str = "",
    hair_clip_mode: bool = False,
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> str:
    del hair_clip_mode
    prompt = _fill_template(
        PROMPT_P1,
        {
            "target_country": target_country,
            "target_language": target_language,
            "product_type": product_type,
            "product_selling_note": _optional_note_text(product_selling_note),
            "product_parameter_info": _optional_note_text(product_parameter_info),
            "hair_accessory_rules": _hair_accessory_anchor_rules(product_type),
            "product_selling_note_rules": PRODUCT_SELLING_NOTE_RULES,
        },
    )
    return _append_type_guard_block(prompt, type_guard_json)


def build_product_type_guard_prompt(
    table_product_type: str,
    business_category: str = "",
) -> str:
    return _fill_template(
        PROMPT_PRODUCT_TYPE_GUARD,
        {
            "table_product_type": table_product_type or "(空)",
            "business_category": business_category or "(空)",
        },
    )


def build_opening_strategy_prompt(
    target_country: str,
    target_language: str,
    product_type: str,
    anchor_card_json: Dict[str, Any],
    product_selling_note: str = "",
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> str:
    del product_selling_note
    prompt = _fill_template(
        PROMPT_P2,
        {
            "target_country": target_country,
            "target_language": target_language,
            "product_family": _product_family_from_type_guard(product_type, type_guard_json),
            "anchor_card_json": _compact_json(anchor_card_json),
            "hair_accessory_rules": _hair_accessory_opening_rules(product_type),
        },
    )
    return _append_type_guard_block(prompt, type_guard_json)


def build_styling_plan_prompt(
    target_country: str,
    target_language: str,
    product_type: str,
    anchor_card_json: Dict[str, Any],
    product_selling_note: str = "",
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> str:
    del target_language, product_selling_note
    prompt = _fill_template(
        PROMPT_P3,
        {
            "target_country": target_country,
            "product_family": _product_family_from_type_guard(product_type, type_guard_json),
            "anchor_card_json": _compact_json(anchor_card_json),
            "light_control_rules": PERSONA_LIGHT_CONTROL_RULES,
            "light_control_pool": PERSONA_LIGHT_CONTROL_POOL,
            "hair_accessory_rules": _hair_accessory_persona_rules(product_type),
        },
    )
    return _append_type_guard_block(prompt, type_guard_json)


def build_strategy_prompt(
    target_country: str,
    target_language: str,
    product_type: str,
    anchor_card_json: Dict[str, Any],
    opening_strategies_json: Dict[str, Any],
    persona_style_emotion_pack_json: Dict[str, Any],
    product_selling_note: str = "",
    repair_instruction: str = "",
    hair_accessory_mode: bool = False,
    hair_clip_mode: bool = False,
    clip_expression_mode: str = "",
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> str:
    del hair_accessory_mode, hair_clip_mode, clip_expression_mode
    repair_block = f"\n附加修正要求：\n{repair_instruction.strip()}\n" if repair_instruction.strip() else ""
    prompt = _fill_template(
        PROMPT_P4,
        {
            "target_country": target_country,
            "target_language": target_language,
            "product_family": _product_family_from_type_guard(product_type, type_guard_json),
            "anchor_card_json": _compact_json(anchor_card_json),
            "opening_strategies_json": _compact_json(opening_strategies_json),
            "persona_style_emotion_pack_json": _compact_json(persona_style_emotion_pack_json),
            "account_style_boundary": ACCOUNT_STYLE_BOUNDARY,
            "fixed_pool": OPENING_FIXED_POOL,
            "light_control_defaults": PERSONA_LIGHT_CONTROL_DEFAULTS,
            "hair_accessory_rules": _hair_accessory_strategy_rules(product_type),
            "product_selling_note_rules": PRODUCT_SELLING_NOTE_RULES,
            "repair_block": repair_block,
        },
    )
    return _append_type_guard_block(prompt, type_guard_json)


def build_final_strategy_prompt(
    target_country: str,
    target_language: str,
    product_type: str,
    anchor_card_json: Dict[str, Any],
    product_selling_note: str = "",
    opening_strategies_json: Optional[Dict[str, Any]] = None,
    styling_plans_json: Optional[Dict[str, Any]] = None,
    strategies_json: Optional[Dict[str, Any]] = None,
    repair_instruction: str = "",
    hair_accessory_mode: bool = False,
    hair_clip_mode: bool = False,
    clip_expression_mode: str = "",
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> str:
    del product_selling_note, hair_accessory_mode, hair_clip_mode, clip_expression_mode
    prompt = _fill_template(
        PROMPT_P5,
        {
            "product_type": product_type,
            "target_country": target_country,
            "target_language": target_language,
            "anchor_card_json": _compact_json(anchor_card_json),
            "opening_strategies_json": _compact_json(opening_strategies_json or {}),
            "persona_style_emotion_pack_json": _compact_json(styling_plans_json or {}),
            "strategies_json": _compact_json(strategies_json or {}),
            "hair_accessory_rules": _hair_accessory_strategy_rules(product_type),
        }
    )
    prompt = _append_type_guard_block(prompt, type_guard_json)
    return prompt + (f"\n\n附加修正要求：\n{repair_instruction.strip()}" if repair_instruction.strip() else "")


def build_expression_plan_prompt(
    target_country: str,
    target_language: str,
    product_type: str,
    anchor_card_json: Dict[str, Any],
    final_strategy_json: Dict[str, Any],
    product_selling_note: str = "",
    persona_style_emotion_pack_json: Optional[Dict[str, Any]] = None,
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> str:
    del target_language, product_selling_note
    prompt = _fill_template(
        PROMPT_P6,
        {
            "target_country": target_country,
            "product_family": _product_family_from_type_guard(product_type, type_guard_json),
            "anchor_card_json": _compact_json(anchor_card_json),
            "final_strategy_json": _compact_json(final_strategy_json),
            "persona_style_emotion_pack_json": _compact_json(persona_style_emotion_pack_json or {}),
            "account_style_boundary": ACCOUNT_STYLE_BOUNDARY,
            "hair_accessory_rules": _hair_accessory_expression_rules(product_type),
        },
    )
    return _append_type_guard_block(prompt, type_guard_json)


def build_script_prompt(
    target_country: str,
    target_language: str,
    product_type: str,
    script_brief_json: Dict[str, Any],
    product_selling_note: str = "",
    existing_script_jsons: Optional[Dict[str, Dict[str, Any]]] = None,
    current_script_json: Optional[Dict[str, Any]] = None,
    repair_instruction: str = "",
    hair_accessory_mode: bool = False,
    hair_clip_mode: bool = False,
    clip_expression_mode: str = "",
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> str:
    del product_selling_note, existing_script_jsons, hair_accessory_mode, hair_clip_mode, clip_expression_mode
    prompt = _fill_template(
        PROMPT_P7,
        {
            "target_country": target_country,
            "target_language": target_language,
            "product_type": product_type,
            "script_brief_json": _compact_json(script_brief_json),
            "hair_accessory_rules": _hair_accessory_script_rules(product_type),
            "ai_video_rhythm_rule": AI_VIDEO_RHYTHM_RULE,
            "audio_layer_rule": AUDIO_LAYER_RULE,
        },
    )
    prompt = _append_type_guard_block(prompt, type_guard_json)
    extras: List[str] = []
    if isinstance(current_script_json, dict) and current_script_json:
        extras.append(
            "当前待修脚本 JSON：\n"
            f"{_compact_json(current_script_json)}\n"
            "这是一轮基于现有脚本的定向修订，请优先在当前脚本上做必要重排和修正，不要重新发明主卖点。"
        )
    if repair_instruction.strip():
        extras.append(f"附加修正要求：\n{repair_instruction.strip()}")
    if extras:
        prompt = f"{prompt}\n\n" + "\n\n".join(extras)
    return prompt


def build_script_review_prompt(
    target_country: str,
    product_type: str,
    anchor_card_json: Dict[str, Any],
    final_strategy_json: Dict[str, Any],
    expression_plan_json: Dict[str, Any],
    persona_style_emotion_pack_json: Dict[str, Any],
    script_json: Dict[str, Any],
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> str:
    prompt = _fill_template(
        PROMPT_Q1,
        {
            "target_country": target_country,
            "product_family": _product_family_from_type_guard(product_type, type_guard_json),
            "anchor_card_json": _compact_json(anchor_card_json),
            "final_strategy_json": _compact_json(final_strategy_json),
            "expression_plan_json": _compact_json(expression_plan_json),
            "persona_style_emotion_pack_json": _compact_json(persona_style_emotion_pack_json),
            "script_json": _compact_json(script_json),
            "hair_accessory_rules": _hair_accessory_qc_rules(product_type),
            "ai_video_rhythm_rule": AI_VIDEO_RHYTHM_RULE,
            "audio_layer_rule": AUDIO_LAYER_RULE,
        },
    )
    return _append_type_guard_block(prompt, type_guard_json)


def build_script_revision_prompt(
    anchor_card_json: Dict[str, Any],
    final_strategy_json: Dict[str, Any],
    expression_plan_json: Dict[str, Any],
    script_json: Dict[str, Any],
    review_json: Dict[str, Any],
) -> str:
    return _compact_json(
        {
            "anchor_card_json": anchor_card_json,
            "final_strategy_json": final_strategy_json,
            "expression_plan_json": expression_plan_json,
            "script_json": script_json,
            "review_json": review_json,
        }
    )


def build_final_video_prompt_prompt(
    target_country: str,
    target_language: str,
    product_type: str,
    anchor_card_json: Dict[str, Any],
    final_strategy_json: Dict[str, Any],
    script_json: Dict[str, Any],
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> str:
    prompt = _fill_template(
        PROMPT_P7_VIDEO,
        {
            "target_country": target_country,
            "target_language": target_language,
            "product_type": product_type,
            "account_style_boundary": ACCOUNT_STYLE_BOUNDARY,
            "anchor_card_json": _compact_json(anchor_card_json),
            "final_strategy_json": _compact_json(final_strategy_json),
            "script_json": _compact_json(script_json),
            "ai_video_rhythm_rule": AI_VIDEO_RHYTHM_RULE,
        },
    )
    return _append_type_guard_block(prompt, type_guard_json)


def build_variant_prompt(
    target_country: str,
    target_language: str,
    product_type: str,
    anchor_card_json: Dict[str, Any],
    final_strategy_json: Dict[str, Any],
    expression_plan_json: Dict[str, Any],
    original_script_json: Dict[str, Any],
    source_script_id: str,
    source_strategy_id: str,
    direction_allowed_pool_json: Dict[str, Any],
    person_variant_layer_json: Dict[str, Any],
    outfit_variant_layer_json: Dict[str, Any],
    scene_variant_layer_json: Dict[str, Any],
    emotion_variant_layer_json: Dict[str, Any],
    variant_plan_json: List[Dict[str, Any]],
    debug_mode: bool = True,
    product_selling_note: str = "",
    repair_instruction: str = "",
    variant_ids: Optional[List[str]] = None,
    persona_style_emotion_pack_json: Optional[Dict[str, Any]] = None,
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> str:
    repair_block = f"\n附加修正要求：\n{repair_instruction.strip()}\n" if repair_instruction.strip() else ""
    resolved_variant_ids = variant_ids or ["V1", "V2", "V3", "V4", "V5"]
    prompt = _fill_template(
        PROMPT_P8,
        {
            "variant_count": len(resolved_variant_ids),
            "target_country": target_country,
            "target_language": target_language,
            "product_type": product_type,
            "anchor_card_json": _compact_json(anchor_card_json),
            "final_strategy_json": _compact_json(final_strategy_json),
            "expression_plan_json": _compact_json(expression_plan_json),
            "persona_style_emotion_pack_json": _compact_json(persona_style_emotion_pack_json or {}),
            "original_script_json": _compact_json(original_script_json),
            "source_script_id": source_script_id,
            "source_strategy_id": source_strategy_id,
            "canonical_strategy_id": str(final_strategy_json.get("strategy_id", "") or "").strip(),
            "direction_allowed_pool_json": _compact_json(direction_allowed_pool_json),
            "person_variant_layer_json": _compact_json(person_variant_layer_json),
            "outfit_variant_layer_json": _compact_json(outfit_variant_layer_json),
            "scene_variant_layer_json": _compact_json(scene_variant_layer_json),
            "emotion_variant_layer_json": _compact_json(emotion_variant_layer_json),
            "variant_plan_json": _compact_json(variant_plan_json),
            "debug_mode": "true" if debug_mode else "false",
            "variant_ids": ", ".join(resolved_variant_ids),
            "hair_accessory_rules": _hair_accessory_variant_rules(product_type),
            "product_selling_note_rules": PRODUCT_SELLING_NOTE_RULES,
            "repair_block": repair_block,
            "ai_video_rhythm_rule": AI_VIDEO_RHYTHM_RULE,
        },
    )
    return _append_type_guard_block(prompt, type_guard_json)


def build_p2_opening_prompt(
    target_country: str,
    target_language: str,
    product_type: str,
    anchor_card_json: Dict[str, Any],
    product_selling_note: str = "",
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> str:
    return build_opening_strategy_prompt(
        target_country=target_country,
        target_language=target_language,
        product_type=product_type,
        anchor_card_json=anchor_card_json,
        product_selling_note=product_selling_note,
        type_guard_json=type_guard_json,
    )


def build_p6_expression_plan_prompt(
    target_country: str,
    target_language: str,
    product_type: str,
    anchor_card_json: Dict[str, Any],
    final_s1_json: Dict[str, Any],
    product_selling_note: str = "",
    final_s2_json: Optional[Dict[str, Any]] = None,
    final_s3_json: Optional[Dict[str, Any]] = None,
    final_s4_json: Optional[Dict[str, Any]] = None,
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> str:
    del final_s2_json, final_s3_json, final_s4_json
    return build_expression_plan_prompt(
        target_country=target_country,
        target_language=target_language,
        product_type=product_type,
        anchor_card_json=anchor_card_json,
        final_strategy_json=final_s1_json,
        product_selling_note=product_selling_note,
        type_guard_json=type_guard_json,
    )


def build_p8_variant_prompt(
    target_country: str,
    target_language: str,
    product_type: str,
    anchor_card_json: Dict[str, Any],
    final_strategy_json: Dict[str, Any],
    expression_plan_json: Dict[str, Any],
    original_script_json: Dict[str, Any],
    source_script_id: str,
    source_strategy_id: str,
    direction_allowed_pool_json: Dict[str, Any],
    person_variant_layer_json: Dict[str, Any],
    outfit_variant_layer_json: Dict[str, Any],
    scene_variant_layer_json: Dict[str, Any],
    emotion_variant_layer_json: Dict[str, Any],
    variant_plan_json: List[Dict[str, Any]],
    debug_mode: bool = True,
    product_selling_note: str = "",
    repair_instruction: str = "",
    variant_ids: Optional[List[str]] = None,
    persona_style_emotion_pack_json: Optional[Dict[str, Any]] = None,
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> str:
    return build_variant_prompt(
        target_country=target_country,
        target_language=target_language,
        product_type=product_type,
        anchor_card_json=anchor_card_json,
        final_strategy_json=final_strategy_json,
        expression_plan_json=expression_plan_json,
        original_script_json=original_script_json,
        source_script_id=source_script_id,
        source_strategy_id=source_strategy_id,
        direction_allowed_pool_json=direction_allowed_pool_json,
        person_variant_layer_json=person_variant_layer_json,
        outfit_variant_layer_json=outfit_variant_layer_json,
        scene_variant_layer_json=scene_variant_layer_json,
        emotion_variant_layer_json=emotion_variant_layer_json,
        variant_plan_json=variant_plan_json,
        debug_mode=debug_mode,
        product_selling_note=product_selling_note,
        repair_instruction=repair_instruction,
        variant_ids=variant_ids,
        persona_style_emotion_pack_json=persona_style_emotion_pack_json,
        type_guard_json=type_guard_json,
    )
