# OPV 自动图文供稿：问题发现与代码现状简报（供 Codex 分析）

日期：2026-09-16 ｜ 写入方：开发经理 ｜ 状态：问题已定位，修复方向未定，等待分析结论

## 一、背景与系统现状

「账号策略驱动的自动图文供稿」Phase 1+2 已完成并在生产跑通（提交链
`e1a3e04` → `65bc839` → `d149ff2` → `2777695` → `b9fc815`，均在本地 main 未推送）。
数据流：

```
小红书素材（labs/xhs-material-lab 采集库, SQLite 只读）
  → material_source.py 读取适配（ro 模式）
  → material_analysis.py Doubao 视觉分析（缓存于 material_consumer.sqlite3）
  → material_adapter.py 程序初筛 narrow_candidates + Doubao 终选 select_reference
  → auto_photo_supply.py 写飞书任务表行（与运营手动建行同构）
  → 现有扫描器（cron 2min）→ feishu_workflow → photo_content_planner
  → image_generator（Doubao 视觉/图生图）→ 成片回写任务表
```

关键文件（packages/organic_photo_video/）：

- `services/material_source.py`：MaterialSource 只读适配（mode=ro URI）、
  MaterialPackage、version_fingerprint（note_id+图片 sha256 序列）。
- `services/material_analysis.py`：`ANALYSIS_VERSION="material-analysis-v2"`（v2 见 §三），
  MaterialAnalyzer 批量 6 图/批调 Doubao（`_DoubaoVisionClient`），批内页码全局重定位
  `_rebase_page_roles`，失败 24h 冷却；MaterialLedger 台账（analysis 缓存按
  version_fingerprint×model×ANALYSIS_VERSION 键、call_log、supply_slots
  UNIQUE(account×date×slot)、analysis_failures）。
- `services/material_adapter.py`：narrow_candidates 程序初筛打分（结构 +3、品类 +2.5/
  -0.5、人工已选 +1.5、页角色 +0.5、互动弱信号 ≤+1、近期使用 -3.0；审美硬门槛见 §三）；
  select_reference 读纯文字摘要由 Doubao 终选，产出 SelectionResult(main/supplements/
  **adoption**/rationale)，adoption ∈ overall|outfit_only|visual_only|narrative_only。
- `services/auto_photo_supply.py`：幂等双层（台账 supply_slots + 飞书「来源标记」列
  `auto_supply|date|account` 对账，备注前缀兜底）；limit=daily_limit（默认 1）；
  建行字段：预设/执行=True/数量=1/目标账号(handle)/来源标记/备注/主题（定位优先时）。
- `services/publish_account_profile.py`：photo_supply_policy 为条件键（未配置=字节级
  不变），`build_default_resolver()`（连发布侧 SQLite）vs 裸 resolver（仅种子文件）。
- 脚本：`scripts/run_auto_photo_supply.py`（默认 dry-run，--apply 需 OPV_PHOTO_VISION_*）、
  `run_material_analysis.py`、`preview_material_candidates.py`。
- 生成侧（本轮问题核心区）：`services/photo_content_planner.py`、
  `services/feishu_workflow.py`、`services/photo_reference.py`
  （`REFERENCE_MODE_COMPLETE_LOOK="COMPLETE_LOOK"`）。

素材库：labs/xhs-material-lab/var/material_library.sqlite3，~102 篇完整素材；
采集纪律=主题搜索（search_feeds，filters: sort_by=最多点赞/note_type=图文/
publish_time=半年内）；匿名信息流素材质量差已弃用。授权口径 reference_only。

## 二、事件时间线

1. **首跑**（2026-09-16，行 `recvvniddNdPpv`，slot1，参考 6a6f7d6c）：成片被用户判
   「太丑」。根因：参考是匿名信息流的「公厕镜面自拍求助帖」（杭州地铁公厕、手机挡脸、
   腿被裁），生成忠实继承素人自拍/隔玻璃游客照质感。两层放水：分析 consumable 只查
   「穿搭可辨认」不查审美；终选只做语义匹配。首跑目检只查技术项（水印/文字/畸变），
   未审美把关。
2. **修复**（提交 `b9fc815`，已生效）：见 §三。
3. **素材池换血**：改最多点赞搜索拉 37 篇（旅游穿搭 20 + 秋冬穿搭 17），37/37 下载成功，
   44 篇 v2 分析完成（1 篇 JSON 解析失败进冷却）。可消费池 4→29 篇（质量分布
   poor:7 / normal:5 / good:33，门槛拦掉 2 篇镜面自拍）。
4. **重跑**：账号 daily_limit=1 不够用，一次性 runner 在**内存**覆盖为 2（未动持久配置），
   slot2 建行 `recvvnyHOLaxQ2`，新参考 6950fcbc（「秋冬旅行穿搭来啦～6套look日常保暖
   又出片」，高赞图文），adoption=overall，8 分钟生成 4 张成片。
5. **新问题暴露**（本轮核心，见 §四）：审美大幅提升，但用户核查发现
   ①成片疑似「直接搬小红书图」；②行里「旅行国家：日本」设定被全程无视，成片背景
   是中国川西。

## 三、已完成的修复（b9fc815，测试 24 绿）

- lab 库两篇自拍素材标 rejected（6a6f7d6c…、6a754beb…），永不再选。
- 分析 prompt v2：新增 `shoot_style`（mirror_selfie/casual_phone_selfie/street_snap/
  studio/indoor/outdoor_other/mixed）与 `photography_quality`（poor/normal/good）；
  consumable 判定加入审美标准（poor 一律不可消费）；ANALYSIS_VERSION 升 v2 使旧缓存
  全量作废重析。
- narrow_candidates 硬门槛：`photography_quality=="poor"` 或 shoot_style ∈
  {mirror_selfie, casual_phone_selfie} 直接排除；good 加 0.5。
- 终选 prompt：写死「绝对不选自拍/随手拍/poor，优先全身完整、光线干净的博主级出片」，
  候选摘要新增拍摄方式/质量字段。

## 四、未解决问题（交 Codex 分析的核心）

### P1（最严重）：成片是对参考图的近 1:1 复刻，「生成」沦为「重绘翻拍」

证据（32×32 灰度归一化互相关，uv run --with pillow,numpy）：

- final_02 ↔ 参考第2页 **0.653**；final_03 ↔ 第3页 **0.745**；final_04 ↔ 第4页 **0.749**；
  封面 ↔ 参考封面 0.495。**页序一一同对应**。
- 人眼比对：同场景（雪山湖泊/雪人熊/寺庙金顶）、同姿势、同构图、同款穿搭，仅人脸与
  细节纹理为模型重绘。

机制（已定位）：

- `services/photo_content_planner.py:404`（complete_look_set 家族 plan）硬编码：
  `scene_zh: "沿用上传素材"`、`palette_zh: "沿用上传素材"`、
  `style_modifier: "忠实使用上传的四套完整穿搭，不补充图片无法证明的效果"`。
- `services/feishu_workflow.py:1955`：COMPLETE_LOOK reference_mode 兜底分支同样硬编码
  `scene_zh/palette_zh = "沿用上传素材"`、`style_modifier: "不得改写上传的完整穿搭事实"`。
- 即：「参考优先」（用户方案 §六）在生成层被执行成**场景级复刻**，image_generator 以
  参考页为底图 i2i。
- **adoption 参数是断的**：SelectionResult.adoption 只存在于素材层三个文件
  （material_adapter/auto_photo_supply/material_analysis），photo_content_planner /
  feishu_workflow / photo_reference **没有任何消费方**。首跑 adoption=outfit_only
  同样复刻了自拍场景，证明该字段不影响生成行为，纯装饰。

风险定性：

1. 版权：高度衍生近似复制（构图/场景/服装/姿势全保留），原作者投诉几乎必成立；
2. 平台：小红书原创检测/查重风险；
3. 与「旅行国家」设定正面冲突（P2）；
4. 本质矛盾：真人博主实拍图做参考 + 场景级沿用 = 只能换脸，谈不上原创生成。

**当前处置建议：行 recvvnyHOLaxQ2 与 recvvniddNdPpv 均暂停发布。**

### P2：「旅行国家：日本」设定被全链无视

行 recvvnyHOLaxQ2 字段携带 `旅行国家: 日本`（表格创建默认值/定位链写入），但：

1. **选材无地理维度**：narrow_candidates 打分因子不含地点；素材搜索关键词
   （旅游穿搭/秋冬穿搭）未按国家定向；
2. **分析不提取地点**：v2 prompt 输出 photography/background 但无结构化地点特征，
   无法支撑地理匹配；
3. **生成显式沿用参考场景**：内容方案摘要原文「场景/配色：沿用上传素材」+「不生成
   具体地点」，参考实拍地（中国川西：雪山/草甸/藏寺/雪原）被直接搬进成片，
   与日本设定矛盾。

即：设定存在于行字段，但选材、分析、生成三层均未消费。

### 附：次要发现

- Doubao 审美判图会把**非拉丁设计文字误报为乱码水印**（本轮泰文 B/C/D 页角标签、
  封面 "ABC หรือ D?" 被判 garbled）；需先转写核实再定罪。若把视觉判图接进自动化
  质检，要处理该误报模式。
- 分析批处理 1 篇 JSON 解析失败（69b41348…，finish_reason=stop 但内容截断），
  现有 24h 冷却机制可兜底，重试策略是否需按错误类型区分待评估。

## 五、候选修复方向（未经确认，供分析评估）

- **L1 生成层**：将「场景：沿用上传素材」改为「场景按旅行国家重新生成（日本→街景/
  城市/自然），参考仅借鉴搭配、配色、氛围，禁止复刻具体场景与构图」；同时打通
  adoption 语义（outfit_only/visual_only 应真实改变借鉴范围）与 i2i 强度/模式。
  注意：该硬编码是「参考优先」方案的执行体现，改动需产品层确认；脱离参考后画面
  质量与一致性可能波动，需重跑验证。
- **L2 选材层**：分析 v3 增加结构化地点/场景特征（雪山/寺庙/海滩/日式街景…），
  选材按旅行国家过滤或降权冲突场景。图像地点识别有局限，可辅以笔记标题/描述文本。
- **L3 素材层（最治本）**：搜索关键词按旅行国家定向（如「日本旅行穿搭」），让参考
  本身摄于目标国家。注意：即便如此，P1 不改的话仍是场景级复刻，版权风险不变。

## 六、复现与验证材料

- 成片：任务表（wiki `TR10wxEXHiCYIhk8clActVdenpc` / table `tblj3x846gU3rshB`）
  行 `recvvnyHOLaxQ2`（新，4 成片+6 参考附件）；对照参考原图：
  `labs/xhs-material-lab/out/download/6950fcbc000000001e00c110/`（17 图 + raw.json）。
- 相似度脚本：/tmp/similarity.py（32×32 灰度 NCC；结论见 §四 P1）。
- 单元测试：`cd packages/organic_photo_video && uv run --python 3.12 --with pytest
  --with pillow --with requests python -m pytest tests/test_material_phase1.py
  tests/test_auto_photo_supply.py`（当前 24 passed）。
- 台账：`packages/organic_photo_video/var/material_consumer.sqlite3`
  （supply_slots：tocrystal66/2026-09-16 slot1=recvvniddNdPpv、slot2=recvvnyHOLaxQ2）。
- 视觉模型环境：`~/.openclaw/workspace/.env.local` 的 OPV_PHOTO_VISION_MODEL/_API_URL/
  _API_KEY（勿外泄，勿打印值）。

## 七、约束（分析时不可违反）

- 素材授权=reference_only：带水印原图/轻改图永远不进发布链；
- 飞书 select 选项只加不删（删除会清历史单元格）；
- 生产飞书表写入只允许既有正规链路；release_gate 反 AI 审核人机制不可绕过；
- 新增定时任务/改生产配置需用户确认；
- Python 3.9 兼容（OPV 包源码需 `from __future__ import annotations`）。

## 八、待 Codex 输出

1. P1 的修复设计：生成指令层怎么改（含 adoption 打通、i2i 模式/强度）、对现有
   complete_look 家族与四选一互动格式的影响面评估；
2. P2 的分层方案取舍（L1/L2/L3 的优先级与最小可用组合）；
3. 版权风险定级与「参考图使用边界」建议（什么样的借鉴程度是安全的）；
4. 对「审美质检自动化」的建议（含非拉丁文字误报的处理）。
