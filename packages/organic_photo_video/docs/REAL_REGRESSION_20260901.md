# OPV 三配方生产回归记录

> 执行日期：2026-09-01
>
> 测试产品：1737141103233042426（浅蓝色短款蓬松外套）
> 安全边界：未人工批准、未渲染视频、未提交 NeoBund、未发布。

## 1. 生产升级

- RDS 只读检查：内容包重复 task_id 为 0。
- 已应用迁移 003：新增 `opv_content_shot.outfit_state_ref`。
- 已应用迁移 004：新增 `opv_content_package.task_id` 唯一索引。
- 已幂等同步 1 Market Pack、9 Theme、1 Render Preset、3 Recipe、1 Render Profile、1 Quality Profile。
- 迁移后复核：字段、唯一索引、迁移台账全部存在。

## 2. 三配方结果

| 配方 | 任务 | Hook | 生成 | 当前状态 |
|---|---|---|---|---|
| 痛点解决 × 小个子显高 | `opv_task_20260901_137467ccc379` | pain_point | P1-P5 成功 | image_review / package qa_review |
| 场景解决 × 咖啡约会 | `opv_task_20260901_fb88d718fa58` | scene_problem | P1-P5 成功 | image_review / package qa_review |
| 视觉变化 × 一衣多穿 | `opv_task_20260901_09316bec8325` | before_after | P5 首先生成，随后 P1-P4 成功 | image_review / package qa_review |

三个内容包均写入按 P1-P5 排序的最新 Shot ID；未出现迁移、唯一约束、人物模板、Profile 或锚点错误。

## 3. 画面审核

共同优点：

- 三个人物模板区分明确，同组人物身份整体稳定。
- 浅蓝外套是每张画面的主体，色彩、短款轮廓和蓬松感基本保持。
- 咖啡场景、室内场景和 P5-first 锚点均按计划执行。
- 原生手机内容感比广告棚拍更接近养号内容。

发现的系统问题：

1. 痛点配方的 BASE 与 FINAL 画面穿搭几乎没有差异。
2. 视觉变化配方虽然正确执行 BASE / ALT_1 / FINAL 状态和 P5-first，但画面仍全部采用白色针织裙，Before/After 不成立。
3. 场景配方 P3 出现旅行箱，与咖啡约会语义存在轻微偏移。

根因不是 Outfit State 未落库，而是：Outfit Planner 未让选中 Look 覆盖通用规则；图片提示词随后又追加固定 Look 文案，覆盖了 BASE / ALT 状态。

## 4. 现场修复

- Outfit Plan 现在以选中 Look recipe 作为 FINAL 状态来源。
- 增加 `top_inner` 状态字段，BASE 可以与 FINAL 形成真实内搭/下装差异。
- 有 Outfit State 时不再追加会覆盖状态的固定 Look prompt。
- 产品和人物仍冻结；只允许互补单品按状态变化。
- QA 支持复用已落库且维度合同一致的单图结果，真实脚本调用间隔 3 秒，避免断点续跑从 P1 重复消耗。
- 全量测试：187 / 187 通过。

## 5. 视觉 QA 路由情况

- 痛点任务：首个 QA 请求遭遇 Doubao 429，未生成有效结果。
- 场景任务：P1、P2 已通过；P3 开始连续超时。断点续跑已验证会复用 P1/P2。
- 备用 Gemini 路由返回 404；当前 Creator CRM 视觉路由不适合继续批量重试。
- 三条任务保持 `image_review`，没有绕过 QA，也没有人工伪造通过结果。

## 6. 结论

RDS、Profile、Hook、内容包、P5 锚点和 Outfit State 数据链已验证可用；前三套样本不应批准或渲染，因为真实回归发现 Outfit State 的视觉执行被固定 Look 覆盖。

根因修复后已额外运行一条最小验证任务：

- 任务：`opv_task_20260901_0957b8765974`
- 配方：视觉变化 × 一衣多穿
- 顺序：P5 → P1 → P2 → P3 → P4
- 结果：P1/P3/P4/P5 为白色针织裙 FINAL/ALT 造型，P2 BASE 明确变为白色直筒裤；目标浅蓝外套和人物身份保持一致。

因此 Outfit State 的视觉执行修复已通过真实生成验证。该任务仍停在 `image_review`，视觉 QA 应在路由恢复或更换可用模型后从断点续跑。

## 7. QA 策略调整

根据本轮真实使用效果，外部视觉模型 QA 改为默认关闭、按需启用：

- 生产默认保留图片尺寸、文件完整性等媒体 QC。
- 五图完成后直接进入人工整组审核。
- `OPV_TH_TEST_001.visual_qa_required=false`，人工批准不再等待模型 QA。
- `run_generation_task.py` 默认不调用外部 QA；只有显式传入 `--run-visual-qa` 才执行。
- QA 服务与断点能力保留，用于高风险商品、疑难人物一致性或专项抽检。

## 8. 轻文字层 V2

- 新增 `OVERLAY_LIGHT_V1`，包含首镜 Hook、状态标签、收尾互动三种组件。
- 文字通过 FFmpeg/libass + Thonburi 字体在视频层渲染，不修改原图。
- 泰语文案按 Recipe 固定映射；P4 产品细节图保持无文字。
- 增加 CJK 泄漏、空文字、单行长度、未知 Locale 和未知模板校验。
- 三条无文字 V1 均保留，并成功生成 10 秒文字版 V2；三条媒体 QC 全部通过。
- 抽帧检查确认泰文无缺字、文字未越界，P1/P2/P3/P5 时序正确。
