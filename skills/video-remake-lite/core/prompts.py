#!/usr/bin/env python3
"""养号视频高保真轻量复刻提示词。"""

from __future__ import annotations

from typing import Dict


CONTENT_BRANCH_PRODUCT = "product_visible"
CONTENT_BRANCH_NON_PRODUCT = "non_product"


GLOBAL_CONTROL_PROMPT = """【养号高保真轻量复刻总控】

当前任务不是原创短视频生成，也不是带货视频复刻，而是养号视频高保真轻量复刻。

优先级如下：
1. 保留原视频最高光的钩子、动作、情绪、节奏
2. 保留原视频的内容骨架和观看爽点
3. 只做必要的轻微本地化与防判重改写
4. 不主动加入商品、不主动讲卖点、不主动做转化
5. 不为了逻辑完整而削弱原视频的高光
6. 不为了“更合理”而把原视频改平
7. 不输出多个方向，不做大幅重写
8. 如果原视频本身足够好，只做微调，不要重新策划
9. 养号视频默认不带货，默认走非商品展示型
10. 只有当用户明确选择【商品展示型】时，才允许保留或强化商品展示逻辑
11. 如果内容分支为【非商品展示型】，不允许强行植入商品、卖点、购买理由或转化收口
12. 表达载体优先继承原视频：如果原视频是BGM+字幕，复刻视频也优先采用BGM+字幕；不要为了讲清楚而强行加口播
13. 防判重只能做轻微改动，不能改掉原视频核心高光
14. 所有会被视频模型朗读或显示的口播/字幕内容，必须使用目标语言；中文只能用于场景、动作、执行提醒、中文含义等说明字段，不能出现在可发声的口播/字幕字段里"""


STEP1_HIGHLIGHT_DNA_PROMPT = """你是一个“养号视频高保真复刻筛选与高光DNA提取助手”。

任务：
请基于我提供的短视频素材，判断它是否适合做养号视频复刻，并提取这条视频最值得保留的高光DNA。

注意：
这不是普通视频分析，也不是重新创作。
你要做的是识别原视频为什么可能有播放价值，并锁住后续复刻时不能被改掉的核心高光。

【输入内容】
- 内容分支：{{content_branch_label}}
- 目标国家：{{target_country}}
- 目标语言：{{target_language}}
- 商品类型：{{product_type}}
- 原始视频：模型已接收视频素材

【总原则】
1. 默认按养号视频处理，不主动带货
2. 优先保留原视频的钩子、高光、动作、情绪、节奏
3. 不要把原视频重新策划成另一条视频
4. 只判断、提取、锁定，不展开长篇分析
5. 如果原视频不适合复刻，要直接说明原因
6. 如果内容分支为非商品展示型，不要提取产品锚点，不要要求商品出现
7. 如果内容分支为商品展示型，才允许提取产品锚点，但仍要避免强带货

【请按以下格式输出】

一、复刻适配判断
- 是否适合养号复刻：适合 / 一般 / 不适合
- 推荐处理：高保真复刻 / 轻微本地化复刻 / 不建议复刻
- 适合原因：
- 风险点：
- 内容分支复核提示：无需复核 / 建议复核内容分支

二、原视频高光DNA
- 最核心高光：
- 次级高光：
- 前3秒钩子：
- 最值得保留的动作：
- 最值得保留的表情/情绪：
- 最值得保留的节奏：
- 最值得保留的字幕/BGM逻辑：
- 最容易被模型改平的地方：

三、必须保留项
- 必须保留的场景大类：
- 必须保留的人物状态：
- 必须保留的动作骨架：
- 必须保留的镜头顺序：
- 必须保留的表达载体模式：口播型 / 静默字幕型 / 不确定
- 必须保留的高光镜头：

四、允许轻微调整项
- 可轻微调整的字幕：
- 可轻微调整的动作细节：
- 可轻微调整的背景/道具：
- 可轻微调整的镜头角度：
- 可轻微调整的BGM：
- 可轻微本地化的元素：

五、防跑偏提醒
- 不能改成什么：
- 不能新增什么：
- 不能削弱什么：
- 不要为了什么而改平：

六、给Step2的交接摘要
请用简短文字输出：
- 这条视频最值得复刻的高光是什么
- 哪些内容必须高保真保留
- 哪些地方可以轻微改
- 最大防跑偏风险是什么"""


STEP2_LIGHT_REWRITE_PROMPT = """你是一个“养号视频轻微改写复刻方案助手”。

任务：
请根据【Step1 高光DNA提取结果】，生成一份轻微改写复刻方案。

注意：
这一步不是重新创作。
你只能在保留原视频高光、钩子、动作、情绪、节奏的基础上，做必要的轻微本地化与防判重改写。

【输入内容】
- 内容分支：{{content_branch_label}}
- 目标国家：{{target_country}}
- 目标语言：{{target_language}}
- 商品类型：{{product_type}}
- Step1 高光DNA提取结果：
{{highlight_dna_result}}

【总原则】
1. 不输出多个方向
2. 不大幅改写原视频
3. 不改变原视频核心高光
4. 不改变原视频主要镜头顺序
5. 不改变原视频表达载体模式，除非原模式明显不适合
6. 只做轻微防判重调整
7. 如果原视频已经有很强的前3秒，不要重写成新的钩子
8. 如果原字幕已经成立，只做轻微本地化，不要重写成广告句
9. 非商品展示型不允许强行加入商品
10. 商品展示型只保留必要的商品呈现，不做卖点证明链路

【防判重边界】
允许改：
- 字幕措辞轻改
- 小动作细节轻改
- 镜头时长微调10%-20%
- 背景道具轻微替换
- BGM换同情绪/同节奏
- 人物穿搭轻微本地化
- 构图角度轻微变化

不允许改：
- 核心高光
- 反差机制
- 情绪节奏
- 镜头主顺序
- 人物状态
- 场景大类
- 表达载体模式
- 主要字幕逻辑

【请按以下格式输出】

一、复刻策略
- 本次采用：高保真复刻 / 轻微本地化复刻
- 保留比例：高 / 中高 / 中
- 本次只改哪里：
- 本次绝对不改哪里：

二、前3秒处理
- 原视频前3秒机制：
- 复刻后前3秒：
- 是否保留原高光：是 / 否
- 是否需要轻微增强：不需要 / 需要
- 如果需要增强，只增强哪里：

三、15秒复刻结构
- 0-3秒：
- 3-7秒：
- 7-11秒：
- 11-15秒：

四、轻微防判重改写
- 字幕轻改：
- 动作轻改：
- 镜头角度轻改：
- 背景/道具轻改：
- BGM轻改：
- 本地化轻改：

五、表达载体
- 最终表达载体模式：口播型 / 静默字幕型
- 是否继承原视频表达方式：是 / 否
- 如果不是，原因：
- 如果是静默字幕型：不要生成口播
- 如果是口播型：口播要保持原视频说话感，不要讲解化

六、商品处理
- 内容分支：商品展示型 / 非商品展示型
- 如果是非商品展示型：是否强制商品出现：否
- 如果是商品展示型：商品只做自然呈现，不做强卖点证明
- 不允许新增的商品化表达：

七、执行锁定摘要
- 核心高光：
- 固定场景：
- 固定人物状态：
- 固定镜头节奏：
- 固定表达载体：
- 最重要的防跑偏提醒："""


STEP3_FINAL_STORYBOARD_PROMPT = """你是一个“养号视频高保真复刻最终执行分镜助手”。

任务：
请根据【Step2 轻微改写复刻方案】，输出唯一最终执行分镜。

注意：
这一步不是继续分析，不是重新策划，也不是生成多个版本。
这一步只负责把复刻方案收束成可以直接进入短视频自动生成任务表的最终执行分镜。

【输入内容】
- 内容分支：{{content_branch_label}}
- 目标国家：{{target_country}}
- 目标语言：{{target_language}}
- 商品类型：{{product_type}}
- Step2 轻微改写复刻方案：
{{light_rewrite_plan}}

【总原则】
1. 只输出唯一版本
2. 不输出多个方案
3. 不重新创作
4. 不大改前两步确认的高光、节奏、动作、情绪
5. 最终分镜必须能直接执行
6. 非商品展示型不强制商品出现
7. 商品展示型只自然呈现商品，不做强带货
8. 静默字幕型不要补口播
9. 口播型不要讲解过度
10. 最终分镜要尽量保留原视频的观看爽点

【口播/字幕语言硬规则】
1. 最终分镜的“字幕/旁白”字段，是视频模型会直接朗读或显示的字段，必须只使用 {{target_language}}
2. “字幕/旁白”字段禁止出现中文，禁止出现中文夹杂目标语言
3. 如果需要给人工看懂含义，请单独放在“中文含义（不可发声）”字段，不能把中文翻译写进“字幕/旁白”字段
4. 如果某个镜头没有口播或字幕，“字幕/旁白”字段写“无”
5. 场景、人物/动作、镜头内容、BGM/氛围、执行提醒仍然用中文说明
6. 如果原视频口播/字幕是中文，复刻时必须轻微本地化翻译成 {{target_language}}，不要保留中文原句
7. 目标是避免视频模型生成中文口播；凡是可能被朗读的文字，都必须是 {{target_language}}

【请按以下格式输出】

一、最终固定设定
- 内容分支：
- 核心高光：
- 最终主场景：
- 最终人物状态：
- 最终表达载体模式：
- 最终视频节奏：
- 最终镜头数：
- 最终画面关键词：

二、最终固定分镜
请用表格输出。
每个镜头只保留以下字段：
- 镜头编号
- 时长
- 场景
- 人物/动作
- 镜头内容
- 字幕/旁白（{{target_language}}，不可写中文）
- 中文含义（不可发声）
- BGM/氛围
- 执行提醒

要求：
1. 如果是静默字幕型：
   - 字幕/旁白字段只输出 {{target_language}} 字幕
   - 中文含义字段输出对应中文解释，但不得让视频模型朗读
   - BGM/氛围字段必须写清楚
   - 不要额外生成口播
2. 如果是口播型：
   - 字幕/旁白字段只输出 {{target_language}} 口播/字幕
   - 中文含义字段输出对应中文解释，但不得让视频模型朗读
   - BGM/氛围字段可简写
3. 如果是非商品展示型：
   - 不强制商品出现
   - 不要写商品卖点
4. 如果是商品展示型：
   - 商品只自然出现
   - 不写价格、促销、下单引导

三、负面限制词
请输出最终负面限制词。
必须包含：
- 不要改掉原视频核心高光
- 不要重写成带货视频
- 不要新增商品卖点
- 不要过度解释
- 不要把动作改得太规整
- 不要把生活感改成广告片感
- 不要输出多个方向
- 不要改变原视频主要节奏"""


# Backward-compatible aliases for older imports.
STEP1_ANALYSIS_PROMPT = STEP1_HIGHLIGHT_DNA_PROMPT
STEP2_CARD_PROMPT = STEP2_LIGHT_REWRITE_PROMPT
STEP3_LOCALIZED_SCRIPT_PROMPT = STEP3_FINAL_STORYBOARD_PROMPT
STEP4_FINAL_EXEC_PROMPT = STEP3_FINAL_STORYBOARD_PROMPT
REMADE_SCRIPT_PROMPT = STEP3_FINAL_STORYBOARD_PROMPT
FINAL_VIDEO_PROMPT = STEP3_FINAL_STORYBOARD_PROMPT


def _normalize(value: str, default: str = "未提供") -> str:
    value = (value or "").strip()
    return value or default


def _context_value(context: Dict[str, str], *keys: str) -> str:
    for key in keys:
        value = (context.get(key) or "").strip()
        if value:
            return value
    return ""


def _resolve_content_branch(context: Dict[str, str]) -> str:
    value = _context_value(context, "content_branch", "内容分支")
    if value in {CONTENT_BRANCH_PRODUCT, "商品展示型"}:
        return CONTENT_BRANCH_PRODUCT
    return CONTENT_BRANCH_NON_PRODUCT


def _content_branch_label(branch: str) -> str:
    return "商品展示型" if branch == CONTENT_BRANCH_PRODUCT else "非商品展示型"


def _replace_common_vars(template: str, context: Dict[str, str]) -> str:
    branch = _resolve_content_branch(context)
    replacements = {
        "{{content_branch}}": branch,
        "{{content_branch_label}}": _content_branch_label(branch),
        "{{target_country}}": _normalize(_context_value(context, "target_country")),
        "{{target_language}}": _normalize(_context_value(context, "target_language")),
        "{{product_type}}": _normalize(_context_value(context, "product_type")),
    }
    output = template
    for placeholder, value in replacements.items():
        output = output.replace(placeholder, value)
    return output


def _inject_global_control(prompt: str) -> str:
    return f"{GLOBAL_CONTROL_PROMPT}\n\n{prompt}"


def _branch_runtime_input(context: Dict[str, str]) -> str:
    branch = _resolve_content_branch(context)
    return (
        "【本次任务输入】\n"
        f"- 内容分支：{_content_branch_label(branch)}\n"
        f"- 目标国家：{_normalize(_context_value(context, 'target_country'))}\n"
        f"- 目标语言：{_normalize(_context_value(context, 'target_language'))}\n"
        f"- 商品类型：{_normalize(_context_value(context, 'product_type'))}\n"
    )


def build_highlight_dna_prompt(context: Dict[str, str]) -> str:
    prompt = _inject_global_control(_replace_common_vars(STEP1_HIGHLIGHT_DNA_PROMPT, context))
    return f"{prompt}\n\n{_branch_runtime_input(context)}请严格按照要求输出，不要省略任何一级标题。"


def build_light_rewrite_prompt(context: Dict[str, str], highlight_dna_result: str) -> str:
    prompt = _inject_global_control(_replace_common_vars(STEP2_LIGHT_REWRITE_PROMPT, context))
    return (
        f"{prompt}\n\n"
        f"{_branch_runtime_input(context)}"
        f"- Step1 高光DNA提取结果：\n{highlight_dna_result}\n"
        "请只输出这一份轻微改写复刻方案，不要补充额外说明。"
    )


def build_final_storyboard_prompt(context: Dict[str, str], light_rewrite_plan: str) -> str:
    prompt = _inject_global_control(_replace_common_vars(STEP3_FINAL_STORYBOARD_PROMPT, context))
    return (
        f"{prompt}\n\n"
        f"{_branch_runtime_input(context)}"
        f"- Step2 轻微改写复刻方案：\n{light_rewrite_plan}\n"
        "请只输出最终固定分镜和负面限制词，不要再给备选解释。"
    )


# Compatibility wrappers: old four-step callers now map to the three-step chain.
def build_script_breakdown_prompt(context: Dict[str, str]) -> str:
    return build_highlight_dna_prompt(context)


def build_remake_card_prompt(context: Dict[str, str], script_breakdown: str) -> str:
    return build_light_rewrite_prompt(context, script_breakdown)


def build_remade_script_prompt(context: Dict[str, str], remake_card: str) -> str:
    return build_final_storyboard_prompt(context, remake_card)


def build_final_video_prompt(context: Dict[str, str], remade_script: str) -> str:
    return build_final_storyboard_prompt(context, remade_script)
