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
1. structure_anchors：例如抓夹/边夹/发箍/发圈/发带/盘发工具结构
2. operation_anchors：例如从哪里夹、是否单手可操作、是否适合半扎/盘发/整理碎发
3. fixation_result_anchors：例如夹上后稳不稳、能否收住头发、是否容易松散
4. before_after_result_anchors：例如脸边更干净、后脑更利落、整体更完整
5. scene_usage_anchors：例如通勤、上学、居家、洗脸护肤、快速出门等

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
发饰类 dominant_user_question 优先来自：
- 会不会不好用
- 会不会不会夹
- 会不会不稳
- 会不会太夸张
- 会不会只是好看但不实用
- 能不能快速整理头发

发饰类 proof_thesis 优先证明：
- 操作门槛低
- 固定效果稳
- 发型变化明显
- 脸边 / 后脑 / 整体更整齐
- 日常场景真能用

发饰类 decision_thesis 优先收住：
- 不太会弄头发的人更适合
- 想让头发别那么乱但又不想弄太复杂的，这种更适合
- 属于很容易留在日常反复用的那种
- 不是看着好看、自己根本不会用的发饰
- 快速出门 / 上班 / 上学很实用"""


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
        "如果 product_type 属于发饰类，则必须优先展示上头前后变化、操作过程、固定结果、使用场景。",
        "不允许只拍发饰单体特写。",
        "不允许没有操作、没有变化、没有使用结果的空展示。",
        "proof 必须优先回答：好不好操作、固不固定得住、夹上后变化成什么样。",
        "decision 优先收住：适合谁、日常会不会真用到、是不是买来不会闲置。",
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
    return """发饰类是否真正体现：
- 操作过程
- 变化结果
- 固定效果

若三者明显缺失，应视为重大问题。"""


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
26. 不要输出大段“为什么这样设计”的解释。

{{hair_accessory_rules}}

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
      "task_type": "attention|proof|bridge"
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

{{hair_accessory_rules}}

【修正规则】
1. 这是轻质检，不是强阻断精品审稿；
2. 只有重大问题才明显修；
3. 轻微问题记 minor_issues，不阻断；
4. 修正必须遵守“最小改动原则”；
5. repaired_script 必须返回修正后的完整脚本 JSON；若无需修正，则返回原脚本。
6. 人物视觉感 / 穿搭完成度 / 情绪推进这些项当前阶段默认作为 warning，不阻断流程；
7. 只有在人物明显抢商品、穿搭明显跑偏、完全无气质差异可感知、styling_key_anchor 完全没有执行、emotion_arc_tag 完全没有执行导致整条视频情绪像死水时，才提升为 major issue。
8. parameter_anchors 当前默认也是轻质检项；只有出现明显参数改写、关键参数被反向表达、或脚本内容与可见参数事实直接冲突时，才提升为 major issue。

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
- 如果 `anchor_card_json.parameter_anchors` 非空，不得在最终视频提示词里改写这些参数事实；关键参数可在 `video_setup` 或相关镜头里轻量保留
- 输出结构收敛为：视频整体设定 / 分镜执行 / 统一执行边界

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
1. 哪种前3秒更容易停住用户
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
