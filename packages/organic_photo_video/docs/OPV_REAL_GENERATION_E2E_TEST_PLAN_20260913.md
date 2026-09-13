# OPV 真实生图 · 端到端整体流程测试方案

- 日期：2026-09-13
- 仓库：`/Users/likeu3/.openclaw/workspace`，目标包 `packages/organic_photo_video`
- 上游：`tmp/oneroute_accept/report.md`（2026-09-13 13:11，**通道层**验收）
- 本方案定位：**从「只验通道」升级到「验整体流程」**，与上游互补、不重复
- 执行前置：本方案需经确认后才执行（见 §7）

---

## 1. 为什么现在做这一轮

| 轮次 | 范围 | 用到的东西 | 缺口 |
|---|---|---|---|
| 13:11 通道层验收 | 只验 1route 通道本身（连通/模型阶梯/兜底/粘性/9:16 闸门） | 真实行真实图 + `PhotoStyleReferenceSupplyService.prepare()` 的真实请求 | **没走生产主链**：不建 RDS 任务、不建冻结批次、不回写飞书、不落成片 |
| **本轮（端到端）** | 飞书行 → 规划 → 素材 → **真实生图** → 技术质检 → 套版成片 → 回写 | 生产同款入口 `run_feishu_tasks.py --record-id` + `build_default_photo_generator()` | 见 §6 未覆盖项 |

一句话：通道层证明了「模型能出图」，本轮要证明「**一行数据能从进来到出片，全链不靠假实现**」。

---

## 2. 目标 / 非目标

### 2.1 目标（每条都要有可核验证据）

| # | 目标 | 证据形态 |
|---|---|---|
| G1 | 真实调用生图模型，落在 `1route` | 该行 request/attempts 记录：`channel=1route`、`model=gpt-image-2.5-sunburst` |
| G2 | 走生产同款入口，不手搓夹具 | 命令为 `scripts/run_feishu_tasks.py --record-id <rid>`；数据来自线上真实行 |
| G3 | 冻结批次幂等：同一行重复跑不重复建批次 | `opv_production_batch` 按 `source_record_id` 只有 1 条；重跑命中 `batch` 复用 |
| G4 | 行级状态机按预期流转 | `进度` 空 → `生成中` → `待审核`（技术完成）；`执行` 复位 false |
| G5 | 成片真实落库并回写 | `预览/成片` 附件数 = 页数；RDS `content_shot` 有记录且 `image_url` 指向真实文件 |
| G6 | 失败可观测 | 若失败：`进度=需处理/失败可重试` + `图文生成失败的原因` 非空，**不得静默** |
| G7 | 不污染存量生产行 | 除新建的测试行外，全表 `进度` 分布与执行前一致 |

### 2.2 非目标（本轮明确不做）

1. **不做故障注入**（通道层已验证兜底/粘性/熔断；故障注入另开一轮）。
2. **不做视觉质检**：`OPV_PHOTO_VISION_PROVIDER` 属外部模型，未获明确授权；保持 `NOT_EXECUTED`，质量结论仅来自 9:16 技术闸门 + 人工目检。
3. **不发布**：`确认发布=false`、`审核=无需审核`；不触发排期/发布链路。
4. **不碰存量非空进度的行**：只新增 1 行测试行。
5. **不改任何配方/策略/账号状态**；只读用。

---

## 3. 环境勘察结论（已执行，全部只读）

### 3.1 生图通道（`services/image_generator.py::build_default_photo_generator`）

| 项 | 值 |
|---|---|
| 通道链 | `1route > codex > creatok`（`OPV_PHOTO_CHANNEL`） |
| 主模型 | `gpt-image-2.5-sunburst`（`OPV_ONEROUTE_IMAGE_MODEL`） |
| 备选阶梯 | `gpt-image-2` |
| 端点 | `https://image-api.1route.dev`，edit 模式 `multipart`，超时 300s |
| 熔断 | 阈值 3 / 冷却 300s，组内粘性开 |
| 凭据 | `OPV_ONEROUTE_API_KEY` 已配置（len=67，值未打印） |
| 视觉 | `Doubao-Seed-2.1-turbo`（`.env.local`），本轮不调用 |
| 健康度 | 今天 13:11 实测：4/4 页一次产出、零降级、全过 9:16（940–941×1672） |

### 3.2 RDS

- 连通：`mysql+pymysql://***@rm-bp1jo43oygziz3xv1fo.mysql.rds.aliyuncs.com:3306/likeu_ai_database`
- 已播种配方 17 条。**可用（V2 系列）**：`PHOTO_TH_TRAVEL_OUTFIT_V2`、`PHOTO_TH_PICK_YOUR_LOOK_V3`、`PHOTO_TH_TEMPERATURE_DRESSING_V1`、`PHOTO_MX_*`
- **未播种**：`PHOTO_TRAVEL_OUTFIT_V3`、`PHOTO_MATCHING_CHOICE_V3`、`PHOTO_TH_THERMAL_TRANSITION_V1`
  → 走生产扫描器必须先在 RDS 存在；未播种的线**本轮不可用**（见 §7 选项 B）
- 账号：`OPV_TH_TEST_001`（TH / testing）、`OPV_MX_PHOTO_001`（MX / testing）
- 素材集：仅 `womenswear`/TH —— `TH_WOMENSWEAR_CHOICE`（enabled，多版本）、`TH_WOMENSWEAR_LAYERED`（disabled）。**无 scarf/accessories 素材集**

### 3.3 飞书表（`tblj3x846gU3rshB`，app_token `HOhEbiAXeamZnasVRKzcttVlnce`，179 行）

- `进度` 分布：已完成 66 / 空 36 / 已发布 35 / 需处理 22 / 失败可重试 9 / 提交中 4 / 待审核 3 / 已排期 3 / 发布失败 1
- `生产预设` 分布：`图文｜TH｜旅行穿搭` 144、`图文｜TH｜四选一穿搭` 19、其它 16
- `参考图（可选）` 张数：0 张 40 / 1 张 19 / **2 张 94** / 3 张 21 / **4 张 5**
- 现有 3 条惰性测试标记行（`执行=false`、无 `进度`），**本轮新建第 4 条，不复用它们的语义**：
  - `recvv3PO6r6391` OPV表格冒烟｜A线·冷热切换
  - `recvv3PP03XQct` OPV表格冒烟｜B线·旅行温度
  - `recvv4FbzGCTca` OPV通道验收｜1route 生图通道

### 3.4 配方 → 规划流

| 配方 | policy | planning_flow | 支持模式 | 本轮可用 |
|---|---|---|---|---|
| `PHOTO_TH_TRAVEL_OUTFIT_V2` | `TH_TRAVEL_OUTFIT_V1.json` | `travel_two_step` | `STYLE` / `COMPLETE_LOOK` | ✅ RDS 已播种 |
| `PHOTO_TH_THERMAL_TRANSITION_V1` | `TH_THERMAL_TRANSITION_V1.json` | `thermal_transition_two_step` | `COMPLETE_LOOK`（`canary_only=true`） | ❌ RDS 未播种 |
| `PHOTO_TH_PICK_YOUR_LOOK_V3` | `TH_PICK_YOUR_LOOK_V2.json` | 继承 V2 基础策略 | — | ✅ RDS 已播种 |

---

## 4. 测试数据（从表格复制，不手写夹具）

**取材行：`recvuDYB1RsfGV`**（也是今天通道层验收用的同一行）

| 字段 | 值 | 为什么合适 |
|---|---|---|
| 生产预设 | `图文｜TH｜旅行穿搭` | 对应已播种的 `PHOTO_TH_TRAVEL_OUTFIT_V2` |
| 图文主题 | `旅行·环境协调` | 有 `travel_theme_type`，走旅行主题分支 |
| 参考图类型 | `风格参考` | → `REFERENCE_MODE_STYLE`，最少只需 1 张 |
| 参考图 | **4 张，1440×1920 真实图** | 素材充足，不触发「素材不足」分支 |
| 旅行国家 / 地点 | 韩国 / 景福宫 | 文案 token（`{destination}`）可用 |
| 进度 | **空** | 未被任何流程占用 |

**复制方式（新增 1 行，不修改原行）：**
1. 新建行，字段取自 `recvuDYB1RsfGV`：`生产预设`、`图文主题`、`参考图类型`、`参考图`（4 张，按序）、`旅行国家`、`旅行地点（可选）`、`生成篇数=1`
2. `备注` 前缀写标记：`OPV端到端生图测试｜取材行 recvuDYB1RsfGV｜`（幂等锚点，重跑先按此前缀找回并复用，不重复建行）
3. 清掉建表默认值 `旅行国家`（先写再清，避免默认「日本」污染）；写入正确值
4. `执行=false`、`审核=无需审核`、`确认发布=false`、**不写 `进度`** → 建完先用 dry-run 扫描器自证「扫不到」
5. 确认无误后，把这一行的 `执行` 置 `true`，作为**唯一**被放行的行

> 铁律：**只有这一行会被置 `执行=true`**。执行期间若发现其它行 `执行=true`，立即中止。

---

## 5. 用例分层（L0 → L4）

### L0 凭据与通道自证（零副作用，付费但极小）
```bash
/usr/bin/python3 scripts/probe_oneroute_channel.py
```
- 期望：`exit 0`；t2i 与 edit 两条路径都出图且过 9:16
- 意图：先把「通道此刻可用」钉死；否则后面失败分不清是流程问题还是余额/接口问题
- 不做：不写 RDS、不碰飞书

### L1 离线规划（不调模型）
- 用取材行字段跑一次规划，冻结出 `content_plan`（N 页 + 文案）
- 期望：页数与 `PHOTO_CHOICE_CARD_V2` 版式匹配；文案为泰语、无空字段
- 意图：把「规划错误」和「生图错误」分开

### L2 真实端到端单行（核心）
```bash
# 2.1 先只读自证：只应捞到 1 行
/usr/bin/python3 scripts/run_feishu_tasks.py --dry-run --record-id <NEW_RID>

# 2.2 真跑（唯一一次写操作 + 唯一一次付费生图）
/usr/bin/python3 scripts/run_feishu_tasks.py --record-id <NEW_RID>
```
- 期望：`errors: []`；`scanned=1`；`eligible=1`；`processed[0].action == "generate"`
- 观察：`进度` 流转 `生成中` → `待审核`；`预览/成片` 出现附件

### L3 落库与回写核验（只读）
| 核验 | 手段 | 期望 |
|---|---|---|
| 批次唯一 | `opv_production_batch` where `source_record_id=<NEW_RID>` | 恰好 1 条；`batch_status` 非 `cancelled` |
| 任务数 | `content_task` / `list_tasks_by_source_prefix` | 与 `生成篇数` 一致 |
| 出图记录 | 该 task 的 shots | 每页 1 条，`shot_status` 成功，`image_url` 文件存在 |
| 成片 | 行内 `预览/成片` 附件 | 附件数 = 页数；`PIL.Image.verify()` 可解码 |
| 通道落点 | 生成器 attempt 记录 | 全部 `1route` / `gpt-image-2.5-sunburst`（或有显式降级轨迹） |

### L4 幂等与隔离（只读）
| 核验 | 手段 | 期望 |
|---|---|---|
| 幂等 | 再次 `--dry-run --record-id <NEW_RID>` | 不再 `eligible`（`进度=待审核`、`执行=false`） |
| 批次不重复 | 再查 `opv_production_batch` | 仍为 1 条 |
| 全表不污染 | 重扫全表 `进度` 分布 | 与 §3.3 基线**逐项一致**（除新增测试行外） |
| 无 VN/生产侧副作用 | `git status` + 配置未改 | 无配方/账号/预设改动 |

### L5 反向控制（可选，本轮默认不做）
- 构造应被拦的输入（缺素材 / 主题与 flow 不符 / 预设无账号），证明闸门**会拦**并给出可读原因
- 理由：本轮定位是「正向打通」，反向已在通道层与单测覆盖；要做需另开一轮并单独确认

---

## 6. 通过判据（全部满足才算通过）

1. L0 `exit 0`，两路径出图过 9:16；
2. L2 `errors: []` 且该行走到 `待审核`；
3. L3 五项核验全绿，成片可解码、页数正确；
4. L4 幂等成立、全表进度分布零漂移；
5. L1 规划产物与最终成片页数一致（无「规划 5 页只出 3 页」这类静默截断）；
6. 全程**未发布**、未改配置、未调用视觉模型（或已显式授权）。

**部分通过（需明确记录）**：成片产出但通道发生降级（如落 `codex`）——流程通过，但要在报告里标出通道归属与降级轨迹。

**失败**：任何一步 `errors` 非空且未自愈；或出现静默截断/静默降级。

---

## 7. 风险、边界与回滚

| 风险 | 缓解 |
|---|---|
| 真实付费生图 | 只放行 1 行、`生成篇数=1`；L0 先探活避免带病重试 |
| 误改存量生产行 | 只置 1 行为 `执行=true`；执行前打印全部 `执行=true` 的行，多于 1 行即中止 |
| 生产 RDS 被写脏 | 只写该行对应的 task/batch/shot；用 `source_record_id=<NEW_RID>` 可精确定位；不删改他人数据 |
| 扫描器并发 | `_RecordFlock` 按 record 加锁；确认无其它进程在跑扫描器 |
| 机器角色 | `THIS_MACHINE_ROLE=main`，故**绝不做全表扫描**，一律 `--record-id` |
| 回滚 | 测试行 `执行=false` + `进度` 清空（或置 `需处理`）+ 备注写结论；RDS 侧保留审计不删；**不发布即无外部影响** |

**必须显式说明的边界（写进最终报告）**：
- 本轮的成片是**真实生图**产物（区别于表内 ASSET_REUSE 套版冒烟）；
- 仍未执行视觉质检 → 质量结论**不构成生产级质检**；
- 未做故障注入 → 兜底/熔断只有单测与通道层证据，本轮无新证据；
- 参考图若含第三方平台水印（小红书等）→ 不得作为发布素材。

---

## 8. 待确认事项

| # | 事项 | 选项 | 我的倾向 |
|---|---|---|---|
| Q1 | 跑哪条线 | A. `PHOTO_TH_TRAVEL_OUTFIT_V2`（TH 旅行，RDS 已播种，取材行 4 张真实图）<br>B. `PHOTO_TH_PICK_YOUR_LOOK_V3`（TH 四选一，RDS 已播种）<br>C. VN 围巾 V3（需先播种 RDS + 建素材集，属生产数据变更，且 Phase 4 §7.1 两项未解） | **A**：最接近真实生产形态，零额外前置变更 |
| Q2 | 写到哪 | A. 新建 1 行测试行（推荐，惰性+标记）<br>B. 复用已有标记行 `recvv4FbzGCTca` | **A2**：新建独立行，语义不与通道验收混淆 |
| Q3 | 跑多深 | A. L0+L1 只做自证<br>B. L0→L4 完整端到端（含真实生图与回写）<br>C. L0→L5 再加故障注入 | **B** |
| Q4 | 是否调用视觉质检 | A. 不调（`NOT_EXECUTED`）<br>B. 调 `OPV_PHOTO_VISION_PROVIDER`（外部模型，额外费用） | **A**（未获授权前不调） |

---

## 9. 执行后产出物

- `tmp/real_gen_e2e/report.md`（人读）+ `result.json`（结构化）
- `tmp/real_gen_e2e/frames/`（回读的成片）
- `tmp/real_gen_e2e/row_before_after.json`（该行字段前后快照）
- 表格内该测试行的 `备注`：标记 + 通道落点 + 结论 + 非发布声明
- 需要时在 `docs/` 落一份交付文档

---

**状态：方案待确认（§8 四个问题）。确认后按 L0 → L4 顺序执行，每层先只读自证再写。**
