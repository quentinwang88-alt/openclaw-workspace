# 主要图文模板优化——交付报告

日期：2026-09-20。基线 `1385111` → 本轮 `26c5d23`（修复一）→ B/C（修复二三）→ D（修复四）→ 色卡发布语言修正。方案：`OPV_KEY_TEMPLATE_OPTIMIZATION_PLAN_20260920.md`；依据评审：`OPV_ROW373_COLOR_TUTORIAL_REVIEW_20260920.md`、`OPV_KEY_TEMPLATE_REVIEW_20260920.md`。

## 结论

四个修复全部按方案落地并真实样片验证：**围巾类目契约贯通（无商品编码）**、**教程结构成为渲染输入（页级 text/色卡/画面证据贯穿到渲染器 v2）**、**两种信息布局（配色侧栏 70/30＋攻略底部解释区）零生图重排验收通过**、**温度主题条件指南真实样片逐页答条件**。1572 项测试全过（新增 21 项入口级回归）。

## 修复一：无商品编码的围巾类目（26c5d23）

- `photo_category_registry`：新增 `required_visible_items/labels`（类目存在契约，与商品身份分工）；`resolve_task_category_adapter` 集中解析（商品＞任务类目＞None，矛盾付费前报错）。
- `feishu_workflow`：类目上下文解析一次并注入 `task_category_key`（仅当解析结果不同于旧回退时注入——女装/假发/指定商品冻结契约逐字不变）。
- 规划：类目存在块（四 look 必带 accessories 围巾设定、优先同一条围巾变围法）＋ normalize accessories 必填（走既有 revise 链）。
- 供给/生图：`outfit_state` 带自由围巾；无商品时打【类目存在锁】不打商品身份锁；`has_product` 统一按 product_id 判定（修注入后真值误判）。
- QA：自由围巾用 `required_item_visible` 轻量观察（`FAILURE_REQUIRED_ITEM_MISSING` 定向重生，用既有一次修复预算）。

**真实样片**：VN 围巾旅行预设当前 status=disabled（VN 文案未母语审校），按方案 §10 不擅自启用——链路由 8 项新单测钉住（registry 解析优先级/冲突、提示类目块、normalize 必填、QA 缺失判败、供给回退、生图存在锁），**待运营启用 VN 线后补跑真实样片**。

## 修复二：教程结构成为渲染输入

- 规划合同新增 `pages[]`：`kicker/headline/body`（发布语言）、`color_chips[{name_zh,hex,role}]`（配色页）、`key_point_zh/visual_basis_zh`（中文审计＋画面证据），与 slide_texts/look 一一对应。
- normalize 每篇独立构建 narrative v2（`layout_hint=color_sidebar/explain_bottom`），模型缺结构从 slide 回装（降级不失败）；`post.copy` 冻结 pages。
- 贯穿链：计划条目 `narrative`＋`copy.pages` → `build_theme_copy` 透传 → workflow 把页级 text/色卡挂进折叠后内容卡并重冻指纹 → planner slides 携带 `page_text/color_chips`。
- **visual_basis 编译进生图请求**：supply 按页（look 角色）把画面证据写进 `outfit_state.style_direction`（讲内搭露内搭、讲鞋口呈现裤脚衔接）。
- 旧任务无 pages 键→字符串 overlay 路径逐字不变；中文回写走 slide 投影（与页级 text 同源由合同保证）。

## 修复三：两种信息布局（structured_v2 渲染器）

- **配色页**：主图 70% 宽 contain（头顶到脚完整）＋右侧配色侧栏：headline/body＋程序绘制示意色块（泰语角色标签＋hex，色块是搭配示意不伪装实测色值；name_zh 只进 manifest 审计）。
- **攻略/温度页**：主图 74% 高 contain＋底部独立解释区（细分隔线非整块黑底），图文不重叠。
- 四宫格文字 fit 收窄到实际预留区（17% ≈ 20% 减边距，替代默认 34%）。

**零生图重排验收（§9.3-1）**：374（recvvIvN6J9Yvl）配色源图 ×4 重排为色卡侧栏；371（recvvGl06csZ3L）攻略源图 ×4 重排为底部解释区。视觉验证：主图完整、色卡与衣服颜色对应可读、图文分离不遮证据、泰文无乱码无裁切。样张：`/tmp/gc_relayout/`（color374_p1-4 / guide371_p1-4 + report）；重排脚本 `scripts/relayout_guide_samples_20260920.py`。

## 修复四：温度条件指南＋展示轴＋摘要

- **温度主题无显式表达→默认实用指南**：A-C 逐页回答穿脱/覆盖/层次条件，末页总结非投票；温度数字只在运营显式温度档出现，缺省只写相对条件。
- **真实样片**（`recvvJiswTWVUO`，首尔＋同一件外套＋无表达配置账号）：标题「首尔早晚冷走路又热，外套怎么穿才方便脱？」；四页＝暖时单穿薄内搭／走热脱外套（画面外套敞开露条纹内搭＝证据）／咖啡馆脱外层剩薄开衫／傍晚风凉扣上＋收藏提示。无裤裙投票、无编造温度数字。中文回写每页对应条件。已完成，确认发布未勾选。
- 展示型主题联动：选择轴取决于四套实际差异（颜色环境/轮廓/用途/场景），确实比较裤裙才用裤裙问题。
- 教程行摘要显示「本篇问题＋本篇方法」（theme_brief.topic_zh 教程主题也带键），不再显示配方投票卡的用户问题/比较依据。

## 测试

`python3 -m unittest discover -s tests`：**1572 全过**（新增：围巾类目 8 项、教程结构贯穿/渲染分区 5 项、温度条件指南 3 项、教程摘要 1 项等）。评审基线 142 项六组模块回归在内通过。

## 已知限制与边界

1. **无编码围巾真实样片**：VN 预设 disabled（方案 §10 不擅自启用）；待运营启用后补跑（链路已单测钉住）。
2. 中文回写走 slide 投影而非逐页结构直译（同源性由「headline+body 与 slide_text 一致」合同保证）；后续可让翻译直接吃 pages。
3. 色卡 hex 为模型给的示意值，非实测色；不做像素级色彩管理。
4. MX 标签避让头顶（评审§7 低优先）本轮未动；旧穿搭拆解/冷热切换按方案保留专用流程。
5. 新结构只对新任务生效；历史冻结任务保持旧快照，不自动重排。

## 提交

`26c5d23` 修复一 → `~` 修复二+三（结构贯穿+渲染 v2）→ `~` 修复四 → `~` 色卡发布语言修正＋重排脚本（含两份评审文档入库）。
