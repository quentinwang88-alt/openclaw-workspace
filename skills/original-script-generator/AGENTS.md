# Original Script Generator Agent Guide

## Scope
- This repo evolves the current pipeline; do not redesign the system from scratch.
- Keep the current stages:
  - `P1` 产品锚点卡
  - `P2` 内容强策略卡
  - `P3` 正式脚本生成
  - `P4` 独立脚本质检
  - `P5` 脚本修订
  - `P6` 最终视频提示词生成
  - `P7` 轻变体生成（可选）

## Core Principle
- Prioritize `原生感成立 + 避免明显广告化`.
- Do not optimize for “强压广告感” as the main goal.
- The content should feel like real sharing, not a product manual.

## Direction Rule
- Preserve the 4-direction system: `S1 / S2 / S3 / S4`.
- Do not make人物 / 穿搭 / 场景 / 情绪 into a universal template.
- Direction-specific tendencies must stay visible:
  - `S1`: more casual, more life-like, more spontaneous
  - `S2`: more stable, balanced, clearly成立但不过度销售
  - `S3`: more decisive and clear, but still native not salesy
  - `S4`: stronger first-frame result feel, but not influencer-commercial

## Strategy Card Requirements
- `P2` must keep or generate these fields:
  - `persona_presence_role`
  - `persona_polish_level`
  - `styling_base_logic`
  - `styling_base_constraints`
  - `scene_function`
  - `opening_emotion`
  - `middle_emotion`
  - `ending_emotion`
  - `product_dominance_rule`
- `scene_function` must explain why the scene helps hook / proof / result / daily believability.
- `styling_base_logic` must explain how the base look helps users see, judge, and believe the product.
- Emotions must be a light flow across beginning / middle / ending, not just an ending garnish.
- Prefer 镜头可感知状态 over abstract persona labels.
- Each strategy should include at least one proof direction that resolves a concern, not just praises the product.
- `scene_function` and `styling_base_logic` should answer “why this for this content, not something else”.
- Only one high-design element should be strongly emphasized in a given script direction; the rest should stay ordinary.

## Script Requirements
- In `P3`, key shots must show:
  - 人物状态
  - 穿搭底盘作用
  - 场景功能
  - 当前情绪
- Key constraints:
  - first shot must include both action and state
  - at least one proof shot must explain how styling helps the product land
  - middle proof should carry `middle_emotion`, not pure cold info
  - ending must be the natural close of previous state, not only a selling sentence
  - at least one proof shot should clearly show a concern being relieved
  - emotion should stay light and continuous, not become a fully designed emotional arc
  - avoid stacking multiple high-design atmosphere elements in one script
- Avoid:
  - persona tags only (`自然 / 松弛 / 微笑`) without shot function
  - styling labels only (`基础白T`) without role
  - scene names without function
  - emotion only appearing at the ending

## QC Requirements
- Keep QC lightweight, but it should catch:
  - action without state in the first shot
  - proof shots without styling logic
  - proof with praise only and no concern relief
  - scene as location tag only
  - middle section with only cold info and no felt emotion
  - over-designed emotion flow
  - multiple high-design elements causing ad-like risk
  - person / styling / scene / emotion stealing focus from the product
  - direction drift caused by the 4 directions becoming too similar in人物/底盘/场景功能/情绪流动

## Video Prompt Requirements
- Final video prompt must stay compact.
- Only lightly preserve:
  - 人物状态边界
  - 穿搭底盘边界
  - 商品主次关系
  - 原生感边界
- Do not re-expand into strategy explanation.

## Engineering Rule
- When updating prompts, also update:
  - `core/json_parser.py`
  - `core/script_renderer.py`
  - any related validation or recovery logic
- Keep backward-compatible fallbacks where practical, but bias new generations toward the upgraded schema.
