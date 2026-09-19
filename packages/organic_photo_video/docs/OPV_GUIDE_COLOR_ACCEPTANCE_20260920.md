# 攻略／配色教程真实样片验收（GC-V2 闭环）

日期：2026-09-20 凌晨。基线提交：`ec6d193`（F1-F5 初版）→ `68f6a17`（教程真实入口全链）→ `cac93d0`（固定背景收尾）。对应方案 `OPV_GUIDE_COLOR_CLOSURE_PLAN_20260919.md` §9.2。

## 结论

**两条新教程主题端到端成片验收通过**，Phase B-D 按方案完成定义闭合。评审（OPV_GUIDE_COLOR_REVIEW_20260919）指出的「讲解规划→逐页图片→逐页文案→排版未闭环」在真实入口上已接通；但初版 F1-F5 修复存在五处只在真实跑测中暴露的断链（见下），本轮全部修复并钉上入口级回归。

## 验收样本（方案 §9.2）

| 任务 | 记录 | 账号/主题 | 交付 |
| --- | --- | --- | --- |
| 新任务一：旅行攻略＋东京＋指定商品 | `recvvGl06csZ3L` | tocrystal66，主题来源＝账号默认（旅行·穿搭攻略），表达＝实用指南，目的地＝东京（任务字段） | 已完成，4 页成片 |
| 新任务二：配色教程＋同商品＋纯色背景 | `recvvGl1qtGMOz` | wn0didnad6，主题来源＝任务选择（配色教程），视觉预设＝纯色搭配解析（VP_SOLID_COLOR_V1，fixed） | 已完成，4 页成片（断点续跑复用素材） |

两篇使用同一指定商品 `1737141103233042426`（THFZ01，参考包 `opv_prp_2575e7885718042f7475`）。

### 对照验收（遮字看图／去图看字／两篇不同问题）

- 只看图上文字能说出「学会了什么」：任务一页面文字为「薄内搭易穿脱」「裤脚与鞋面衔接」「裙摆接靴口减少截断」等方法标题＋一句解释；任务二为「浅色衔接降低色差」「炭灰明暗对比让棕色突出」「棕鞋呼应夹克连蓝裙」。
- 遮住文字看图：任务一东京老城街景下同一棕色短外套的多种搭配部位变化可见；任务二纯色棚拍四组配色组合（浅蓝外套×酒红/炭灰/奶白/棕）颜色关系可读。
- 两篇回答不同问题：东京旅行怎么穿轻盈 vs 红棕夹克怎么配色不沉闷。
- 无「选择 A/B/C/D」投票文案；无凭空温度声明（任务二 caption 的 15–22°C 来自预设温度档，属允许范围）。

### 交付物位置

- 冻结旅行计划（模型输入合同＋叙事）：`~/.openclaw/shared/data/organic_photo_video/reference_contracts/<record>/travel_plan.json`（input_contract 含 travel_topic.theme_key、fixed_background、narrative_plan）。
- 内容计划：`content_plans/<record>/plan.json`（allow_repeated_travel_moments=true、copy_policy_version=2）。
- 成片：飞书行「预览/成片」4 张；本地 `photo_packages/opv_task_*/final/0*.jpg`。

## 真实跑测暴露并修复的断链（五处＋两处收尾）

1. `build_travel_topic` 不带 `theme_key` → 规划端教程判定（guide 规则块/narrative_plan/文案分支）全失效，整链回落投票分支。
2. `_travel_plan_prompt` 的文案合同与 JSON 示例按 `theme_type` 门控且示例写死 5 条 A/B/C/D → 教程主题不被要求产文案、模型跟示例走投票格式（任务二降级根因）。
3. `topic_linked`（内容计划层）只认 `theme_type` → 纯色教程四页同 travel_moment 被「场景重复」拒绝（固定背景/同场景稳定是教程设计要求）。
4. `build_theme_copy` 只放行 5 条 slide_texts → 教程 4 页文案在冻结请求时被丢弃、回退「A·造型」投票组装（成片仍带投票文案的最后一公里）。
5. 视觉预设预校验先于单封面折叠按卡页数断言 → 卡 5 页×文案 4 条判不匹配（新增 `allow_four_slide_copy` 折叠路径）。
6. 组语义 QA 在固定背景下把 `observed_moment=unknown` 判硬失败（`photo_travel_qa`）。
7. 结构化排版底部条带按 font.size 估算块高，低估泰文声调墨迹，末行贴边被裁（`photo_structured_layout` 按实测墨迹上移）。

所有修复均为 additive：旧六主题/旧流程的冻结契约、提示词与渲染逐字不变（有回归断言钉住）。

## 测试

`python3 -m unittest discover -s tests`：**1556 项全过**（新增 `GuideTopicEntryTest` 13 项入口级回归：build_travel_topic→prompt 合同/schema→normalize 4 页与降级无投票→plan 冻结 v2 文案与同场景放行→build_theme_copy 4 条→单封面折叠；固定背景 unknown 场景 2 项；底部墨迹 1 项）。

## 作废与成本

- `recvvG6uwiNHGX`（任务一①）、`recvvG6vQk0gAq`（任务二①③）、`recvvGa7eKW0HK`（任务一②）、`recvvGhv2Khj1i`（任务一④）均已在飞书行备注「作废勿发布」；任务一②/任务一④/任务二① 批次已取消留档。
- 付费生成约 16 张（含 3 轮被断链/竞争作废的成片）；任务二④ 断点续跑零重生完成收尾。

## 已知限制

- 「内容方案摘要」的「用户问题/比较依据」行仍取自配方投票内容卡模板（展示层）；成片文字与中文回写已是教程文案。后续可让 content_card 问题行读 narrative_plan.question_zh。
- 跨代码版本的调度器进程可能与手动执行竞争冻结（本轮任务一④的不匹配即源于此）；教程主题上线后建议一次只跑一个入口。
- GitHub 代理本轮不可达：`68f6a17`、`cac93d0` 及更早 `ec6d193` 共 3 个提交待网络恢复后补推。
