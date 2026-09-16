# 外部参考执行路径修复：实测验证交付（2026-09-16）

对应修复：提交 `e3c7c5f 系列`（外部参考执行路径修复，Phase 1-3）+ `0d66f56`（目的地行字段）。
验证日期：2026-09-16 晚｜验证人：开发经理｜账号：tocrystal66（泰国女装1）

## 一、验证矩阵（方案 §八 实测要求）

| 场景 | 行 record_id | 结果 | 成片 SHA 对拍原图¹ |
|---|---|---|---|
| S1 原问题参考 + outfit_only + 日本 | recvvnY8ybyhR3 | ✅ 已完成（4 张） | 4/4 不同 |
| S2 同一参考 + visual_only + 纯色背景 | recvvnYaYt8d6E | ✅ 已完成（4 张） | 4/4 不同 |
| S3 narrative_only（不发原图） | recvvnYdhVnmHo | ⚠️ 需处理（见 §四） | 无成片（0 参考图 ✓） |
| S4 指定本店商品（重跑） | recvvo3fqTOLN0 | ✅ 已完成（4 张） | 4/4 不同 |

¹ 对拍基准＝参考笔记 6950fcbc 全部 17 页原图 SHA256。修复前的问题行
recvvniddNdPpv/recvvnyHOLaxQ2 的中间素材与原图 SHA **完全一致**（原图直用）；
修复后全部成片 SHA 均不同，原图直用通道已封死。

## 二、各场景证据

### S1 outfit_only + 日本（recvvnY8ybyhR3）

- **执行合同**：adoption=outfit_only；pages=[(2,outfit_detail),(3,outfit_detail),(4,outfit_detail),(5,outfit_detail)]（每页带 sha256）；destination={country:日本}；policy_version=external-reference-exec-v1；指纹 1b9360f9…。
- **行字段**：参考图类型=风格参考；参考图（可选）4 张（仅选材指定页，非前 N 张）；旅行国家=日本。
- **内容方案摘要**（可追溯段原文）：「旅行目的地 日本（来源：任务字段）；外部参考 采用 outfit_only｜主参考 6950fcbc…｜策略 external-reference-exec-v1」。
- **内容计划**：「一件外套在日本秋日城市旅行中的复用价值」——目的地进入规划。
- **成片判定**（Doubao 对拍参考原图）：场景=日本古街五重塔/町屋/日式咖啡店（scene_is_japanese_urban=true，copied_reference_scene=none）；搭配「仅借鉴大地色穿搭思路，款式细节差异化，属于借鉴而非照搬」→ **pass**。
- 结论：**outfit_only 语义三层生效**（规划新场景 / 只发搭配细节页 / 提示词带目的地约束）。

### S2 visual_only + 纯色背景（recvvnYaYt8d6E）

- 合同 pages=[(1,visual_tone)]（封面色调页）；行视觉预设=纯色搭配解析（fixed，来源：任务选择）。
- 成片判定：背景=浅米色纯色（solid_background=true）→ **pass**。
- 固定背景 + 日本目的地共存：背景保持纯色，目的地体现在内容规划中（方案 §五.1 执行规则验证）。

### S3 narrative_only（recvvnYdhVnmHo）

- 行参考图数量=**0**（选材页引用被强制清空，未发送任何第三方原图）✓。
- 进度=需处理，备注=「NEEDS_CONTENT: 请求 1 篇，但仅找到 0 份未占用的有效内容」。
- **定性：安全失败，非接线泄漏**。当前执行链的 STYLE/PRODUCT 模式都需要
  ≥1 张参考图或产品编码才能生成；narrative_only 的零图入口（纯文本驱动的
  穿搭生成）本轮未建——这是**已知边界**，不是回退（没有偷偷把原图当输入）。

### S4 指定本店商品（recvvo3fqTOLN0，重跑）

- 首跑 recvvnYfFYMhCT 因验证脚本把商品模式写成中文值（"使用指定商品"≠归一化
  值 "specified"）实际未带商品，已作废保留现场；重跑用归一化值。
- **合同 product 字段（真实商品资料，RDS 解析器）**：{"code":"1737141103233042426",
  "name":"浅蓝色短款蓬松外套","category":"outerwear","variant":"light_blue"}。
- 内容计划围绕真实商品展开（「同一件短款蓬松外套…四选一」）；商品为权威、
  参考只提供配套（方案 §五.5 验证）。
- 选材阶段 ProductBrief 携带真实品类（outerwear）参与候选匹配，不再 (code,"")。

## 三、修复前后行为对照

| 断点（修复前） | 修复后行为 | 证据 |
|---|---|---|
| 参考写「完整穿搭素材」→ COMPLETE_LOOK 无条件原图直用 | 外部参考写「参考图+风格参考」，走 STYLE 规划/生成 | S1-S4 行字段 |
| 附件数=角色数 → 自动判断误入 COMPLETE_LOOK | 显式「风格参考」胜出（单测 StyleRoutingTest） | 测试 |
| 外部行无合同直接执行 | 付费前反查合同，缺合同/授权异常/COMPLETE_LOOK/产品不一致即中止 | assert_external_supply_wiring + 单测 |
| adoption 无消费方 | 进合同+内容要求+摘要展示；narrative_only 零图；outfit_only 只发细节页 | S1/S3 |
| 旅行国家字段被无视 | 统一解析（冲突报错）、travel_topic/theme_brief 冻结、摘要展示采用值与来源 | S1/S2/S4 摘要 |
| 选题取 narrowed[0] 与图片串配 | 选题/摘要/图片全部来自 selection.main_note_id | 单测 test_topic_comes_from_selection_main |
| 固定前 N 张上传 | 页级选材（note+seq+purpose+sha256 进合同） | S1 4页/S2 1页/S4 2页 |
| ProductBrief(code,"") | RDS 真实品类/名称/variant；资料缺失明确暂停 | S4 合同 |

## 四、已知边界与遗留

1. **narrative_only 无零图生成入口**：需「纯文本驱动穿搭生成」的新入口（或与
   指定商品组合使用）。本轮按方案安全阻断（不泄漏原图），未新建入口。
2. **泰文/非拉丁文字 QA 调整**（方案 §六）按执行顺序留在最后，本轮未动；
   现有技术检查行为保持。
3. 实测选材结果由脚本固定（monkeypatch select_reference），生产自动模式仍由
   Doubao 终选 adoption/pages——两侧行为差异仅在于"谁做决定"，执行链相同。
4. 验证过程产生的作废行（保留现场，均已 需处理+执行=False）：
   首版脚本 daily_limit 误设导致的 slot4-9 六行、旧 S1 重复行 recvvnXxHZbNMu、
   S4 首跑 recvvnYfFYMhCT。两条原始问题行 recvvniddNdPpv/recvvnyHOLaxQ2
   继续作为回归样例保留。
5. 本轮全部验证行均为**测试任务**，勾选发布才会进入发布队列（当前全部未勾选）。

## 五、测试与提交

- 单元测试：packages/organic_photo_video 全量 **1480 passed + 290 subtests**（含新增
  test_external_supply_contract.py 22 项：合同持久化/幂等恢复/指纹维度/目的地冲突/
  付费前门禁/显式风格参考胜出/页级校验/narrative_only 清空/商品缺失暂停）。
- 提交：外部参考执行路径修复（9 文件，+992/-33）＋ 目的地行字段补充（0d66f56）。
- 回归样例：两条问题行 + 四场景行，全部保留在任务表。
