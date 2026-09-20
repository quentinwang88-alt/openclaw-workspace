# 项链 V1 修复包 A/B/C 验收记录 · 2026-09-20

方案：`docs/NECKLACE_V1_NEXT_FIX_PLAN_20260920.md`（本轮只修 F1–F4，不重构内容生成系统）。

## 结论

阶段 1（三个修复包离线通过）**已完成**：F1 公开身份门禁、F4 交付快照与恢复、F2 冻结视觉合同投影、F3 数量证据极性全部在真实形态 fixture 上复现并消除；项链开关仍为默认关闭；本轮未调用任何生成模型、未写飞书、未写生产库。

阶段 2／3／4 **未执行**，需要运营输入与授权，见文末"未覆盖项"。因此本记录只宣布**离线脚本与门禁层验收通过**，不宣布成片效果。按方案 §6：没有成片时只宣布脚本验收通过。

## 证据分级（必须分清）

本记录全部属于 **①离线纯函数／临时库 mock**：

| 类别 | 本轮是否发生 | 说明 |
| --- | --- | --- |
| 离线纯函数 / 临时 SQLite mock | 是 | A/B/C 三包各自的证据脚本（`run_6/necklace_v1_c4_evidence/`、`…_c5_evidence/`）+ 全量隔离回归（最多 94 个测试文件） |
| 真实模型调用 | 否 | 未调用；F2/F3 的 payload 是本地拼装，不是模型返回 |
| 真实外部写入（飞书 / RDS / 生产库） | 否 | 飞书客户端为内存假表；三个生产库的指纹见"回归"节（其中一个期间被并发飞书跑批改过，已取证归属） |
| 人工审阅成片 | 否 | 未生成成片 |
| 发布表现 | 否 | — |

唯一的真实输入是**真实冻结合同与真实导出产物**（取自隔离生产副本），模型与外部系统均为替身。

**证据文件位置**：本记录中所有 `run_6/...` 路径都相对于 WorkBuddy 工作区 `/Users/likeu3/WorkBuddy/2026-09-14-22-43-35/`，**不在本 git 仓库内**——运行产物不写入源码树。仓库内可复现的部分是测试与夹具：`skills/original-script-generator/tests/`（含 `fixtures/`）与 `skills/script-run-manager-sync/tests/`。要复跑证据脚本，需先按运行台约定指向隔离副本。

## 提交与改动文件清单

| 提交 | 内容 | 文件（`git show --stat`） |
| --- | --- | --- |
| `80ff506` | A：公开身份解析 + 交付快照 + 恢复（F1、F4） | 15 文件，+2264/−185 |
| `450e84e` | B：视觉合同只读自身冻结快照（F2） | 6 文件，+1588/−4 |
| `94ab262` | C：数量按局部断言与极性判断（F3） | 2 文件，+1073/−98 |

按文件展开：

- **A（15 文件）** — 消费者侧 `skills/script-run-manager-sync/core/necklace_handoff.py`、`core/original_batch_source.py`；生成侧 `skills/original-script-generator/core/original_batch_storage.py`、`core/production_script_feishu.py`、`core/production_script_renderer.py`；5 个调用点各改 1–4 行（`scripts/backfill_scarf_batch_handoff.py`、`scripts/export_local_batch_to_feishu.py`、`scripts/refresh_scene_recipe_batch.py`、`scripts/replace_feishu_production_scripts.py`、`scripts/run_feishu_operation_tasks.py`）；测试 `script-run-manager-sync/tests/test_necklace_handoff.py`、`original-script-generator/tests/test_delivery_snapshot.py`（新）、`tests/test_production_script_feishu.py`；文档 `docs/NECKLACE_MIXED_V1_REVIEW_20260920.md`、`docs/NECKLACE_V1_NEXT_FIX_PLAN_20260920.md`。
- **B（6 文件）** — `core/visual_execution_contract.py`、`core/accessory_mixed_templates.py`、`core/simplified_complete_script.py`、`tests/test_necklace_frozen_visual_projection.py`（新）、`tests/fixtures/necklace_v1_frozen_contract.json`（新）、`tests/fixtures/accessory_frozen_contracts.json`（新）。
- **C（2 文件）** — `core/necklace_mixed_profile.py`、`tests/test_necklace_count_evidence.py`（新）。

开发前基线 HEAD `1385111`。用户未提交的 `tests/test_necklace_mixed_integration.py` 全程未纳入任何提交、未被覆盖。

## F1–F4 前后复现结果

### F1 · 公开脚本 ID 门禁（修复前：恒放行）

真实样本：批次 `OCB_ED41AFA5023A5568A58E` / `OCI_8762196B817007C9B9F0`，内部 ID `SCRIPT_8BCEF97D70205D7F6540`，导出公开 ID `SCSCRIPT_40284D07CBCADD28F3F2`，镜块标题 `HOOK / PROOF / PROOF / ENDING`。

| 输入 | 修复前 | 修复后 |
| --- | --- | --- |
| 公开 ID + 真实叙事标题 | `tasks=1`（**未做任何检查**就放行） | `tasks=1`，逐字校验通过 |
| 同 ID，正文改成"正脸出镜展示耳侧耳环" | `tasks=1`（放行） | `tasks=0` + `NECKLACE_HANDOFF_PROMPT_CHANGED` |
| 删除全部镜块标题 | 跳过校验、放行 | 拦截，且**身份已解析**（不是"没找到就跳过"） |
| 改写镜块标题措辞 | 跳过校验、放行 | 拦截，同上 |
| 三种定位符（内部 / 公开 / 批次 Item） | 只有内部 ID 有效 | 三种裁决一致 |

### F4 · 重导后旧审计无法恢复（修复前：要求重导→重导后仍旧审计）

真实样例行 `result_json` 里存的是 `necklace-final-prompt-audit-v1 / FAIL / 5 issues`，而导出渲染出的新审计只进飞书字段、不回写行内 → 消费者永远读到旧 FAIL。

| | 修复前 | 修复后 |
| --- | --- | --- |
| 行内 `result_json` 审计 | `v1 / FAIL / 5 issues` | **仍是 `v1 / FAIL / 5 issues`（原样留档）** |
| `delivery_snapshot` | 不存在 | 存在，`necklace-delivery-snapshot-v1` |
| 快照内共享审计 | — | `mixed-execution-audit-v1 / PASS`，4 镜全锚定 |
| 快照内项链局部队 | — | `necklace-final-prompt-audit-v2 / PASS`，`profile_config_hash=d405e0c3…` |
| 快照 `prompt_text` 与飞书提示词字段 | — | **逐字一致**（实测两者字符串相等，3627 字符；同时记在快照 `render_validation.prompt_chars`） |
| 同步门禁 | 读旧 v1/FAIL → 要求重导 → 循环 | 重导前 `SNAPSHOT_MISSING` 拒绝 → 重导后 1 个目标任务 |
| 跨轮缓存 | 可能命中旧缓存形成永久拒绝 | 每轮 `reset_necklace_handoff_cache()`，第二轮正常 |

### F2 · 项链视觉合同丢失冻结配置（修复前：recipe 有 id、无文字；取景为空）

真实历史产物 `run_6/necklace_v1_live/out/dump_68A58E/item1_result.json`：

| 字段 | 修复前 | 修复后 |
| --- | --- | --- |
| `lighting_recipe.recipe_id` | `NMX_WARM_NEUTRAL_WINDOW_V1` | 同 |
| `lighting_recipe.label` | `""` | `暖中性侧窗光` |
| `lighting_recipe.environment` | `""` | 冻结原文（单一方向暖中性侧窗光…） |
| `lighting_recipe.goal` | `""` | 冻结原文（金属不过曝…） |
| `framing_zone.allowed_framing` | `[]` | 4 项，**只来自佩戴镜 CU_01/CU_02** |
| `framing_zone.forbidden_framing` | `[]` | 5 项 |
| `authorities.framing_zone` | `CATEGORY_EXTENSION` | `FROZEN_PER_SHOT` |
| 手持／静物镜 | — | 各自保留自己的边界，颈部要求未被提升到全部镜头 |

根因（两处都读"今天的配置"）：项链配方 id 不在共享 `environment_recipes`（`get_environment_recipe` 抛 `ValueError`）；共享 `category_rules` 无 `NECK`；且 `resolve_mixed_zone("necklace","")` 返回 `(None,"womenwear")`，冻结的 `category_zone` 是唯一能说出 `NECK` 的来源。

### F3 · 数量证据（修复前：否定／未知被读成肯定）

方案 §4 的最小回归表逐行结果（下表按 §4 原表 8 行呈现，第 8 行的四种非法字面量压缩成一格；证据文件把非法字面量拆成 4 行、并多出 2 条"危险方向"用例，合计 13 行）：

| 输入 | 旧 层/坠 | 旧 V1 | 新 层/坠 | 新 V1 | 期望 |
| --- | --- | --- | --- | --- | --- |
| 单层链条，只有一个吊坠 | 1/1 | 通过 | 1/1 | 通过 | 通过 |
| 单层链条带吊坠，不能认定单吊坠 | 1/1 | **通过（错）** | 1/UNKNOWN | 拒绝 | 拒绝 |
| 不是双层，是单层链条，只有一个吊坠 | 2/1 | **拒绝（错）** | 1/1 | 通过 | 通过 |
| 并非单层，是双层链，只有一个吊坠 | 2/1 | 拒绝 | 2/1 | 拒绝 | 不适用 |
| 有链条和吊坠，数量看不清 | –/– | 拒绝 | –/– | 拒绝 | 拒绝 |
| 字段为 1、另一可靠锚点明确双层 | 1/1 | **通过（错）** | CONFLICT/1 | 拒绝 | 拒绝 |
| 商品盘成两圈，但未确认层数 | –/– | 拒绝 | –/– | 拒绝 | 拒绝 |
| 数量 `true` / `1.8` / `0` / 负数 | 1 / 1 / – / – | **true、1.8 通过（错）** | –/– | 拒绝 | 拒绝 |

13 行对照中 **6 行修复前后结论不同**，说明这条对照有区分力、不是恒真。这 6 行分别是：上表第 2 行、第 3 行、第 6 行；第 8 行内的 `true` 与 `1.8`（旧实现都放行）；以及上表未单列的 `不能确认是不是单层`（旧读成层数 1 并放行，新为 `layer_count:UNKNOWN` 拒绝）。

修法：每个数量形成 `value / status / source_ref / source_text / polarity` 局部证据；结构化数字严格正整数（`bool` 在 Python 里就是 `int`，`int(1.8)` 会截断，两者都拒绝）；文本按局部断言解析，否定／不确定标记必须紧邻所修饰的词；冲突保留 `CONFLICT`；缺来源数字标 `UNSOURCED`，不再借用 `STRUCTURE_COUNTS`。

## 旧类目对照

| 对照面 | 方法 | 结果 |
| --- | --- | --- |
| 四类配饰视觉合同投影（B） | 五类真实冻结合同分别喂 HEAD 版模块与工作区版模块，比 sha256 | `EAR/WRIST/FINGER/HAIR` **逐字节相同**；仅 `NECK` 变化（多出 `frozen_projection` 一个键） |
| 四类配饰"冻结 == 不冻结"（B） | 同一份真实合同，传 / 不传 `frozen_contract` | 四类全部相同——权威仍在共享配置，冻结对它们是空操作 |
| 耳饰同步行（A） | 与"未加门禁"版本逐字比较导出行 | 逐字一致 |
| 已识别旧项链行（A） | 同上 | 逐字一致 |
| 旧 `counts` 整数形状（C） | `{"layer_count":1,"pendant_count":1}` | 仍可读、仍通过；来源标为 `UNSOURCED` 而非伪造 |
| 共享模板定义（A/B） | 编译后比对定义与 A/B/C 轮转 | 未被改动 |

## 公开 ID 端到端测试

`run_6/necklace_v1_c4_evidence/n2_sync_gate_e2e.py`，真实导出产物 + 真实同步适配器 `build_original_batch_sync_tasks`，**14/14 通过**（B、C 之后复跑仍 14/14）：

1. 公开 ID + 真实叙事标题 → 1 个目标任务
2. 同 ID 改成正脸耳环文本 → 0 个 + `PROMPT_CHANGED`
3. 删除全部镜块标题 → 已解析身份 + 逐字校验拦截
4. 改写镜块标题措辞 → 同上
5. 三种定位符同一裁决
6. 旧 v1/FAIL → 重导 → 恢复（重导前拒绝、重导后 1 个任务）
7. 快照未落库 → 0 个 + 快照缺失说明
8. 快照已存但表内文本旧 → 0 个 + 文本不一致
9. 跨轮缓存不残留（第一轮拒绝、第二轮通过）
10. 来源异常三类 → 三类都拒绝
11. 错误码 `NECKLACE_HANDOFF_UNVERIFIED` / `_SOURCE_UNAVAILABLE` / `_SOURCE_AMBIGUOUS`
12. 耳饰行与未加门禁逐字一致
13. 已识别旧项链行为不变
14. 定位层四态 `FOUND / NOT_FOUND / SOURCE_UNAVAILABLE / AMBIGUOUS`

## 错误恢复测试

| 触发 | 期望 | 结果 | 出处 |
| --- | --- | --- | --- |
| 导出时无 `storage` | 不写飞书，`snapshot_blocked=1` | 通过 | 证据脚本 `n1_export_snapshot.py` case `no_storage_blocks_sheet` |
| 飞书写入失败 | 拦截，报 `PROMPT_CHANGED` | 通过（`RuntimeError: 模拟飞书写入失败`） | 同上，case `sheet_write_fails` |
| 冻结来源行不存在 | `NOT_FOUND`（不是"我们的问题"） | 通过 | 证据脚本 `n2_sync_gate_e2e.py` 检查 9/9b |
| 冻结来源打不开 | `SOURCE_UNAVAILABLE`（不是对该行的裁决） | 通过 | 同上 |
| 冻结来源多条冲突 | `AMBIGUOUS` | 通过 | 同上 |
| 声称是项链但读不到身份 | 暂缓（`UNVERIFIED`），声明永不作为通过依据 | 通过 | 同上 |
| 无项链信号且读不到 | 放行（不误伤其他类目） | 通过 | 同上，检查 10a |
| 并发写者抢先提交 | CAS 失败 → 拒绝对 `result_json` 的合并 | 通过（在读与写之间插入真实竞争） | 仓库内 `tests/test_delivery_snapshot.py::test_a_concurrent_writer_turns_the_merge_into_a_refusal` |

## 版本与开关状态

- 项链开关 `ORIGINAL_SCRIPT_NECKLACE_MIXED_V1_ENABLED`：**默认关闭**（`os.environ.get(..., "0")`），当前环境未设置。
- 配饰混合开关 `ORIGINAL_SCRIPT_ACCESSORY_MIXED_TEMPLATE_V1_ENABLED`：同样默认关闭，未变更。
- 交付快照 schema：`necklace-delivery-snapshot-v1`。
- 项链审计：`necklace-final-prompt-audit-v2`；共享审计：`mixed-execution-audit-v1`。
- 视觉投影新增：`frozen_projection`（`NECKLACE_MIXED_V1`，状态 `COMPLETE` / `INCOMPLETE`）。
- `profile_config_hash`：`d405e0c3933770bfe13f9f91b94a6b3b19b87987684ca48f53077a3675da14be`。
- 未改动：其他类目模板、配方轮转、口播规则、模型、缓存版本、恢复状态机、发布排期；未新建数据库或飞书表。

## 回归

- 全量隔离回归跑两次（分别覆盖 B 后与 C 后）：
  - B 后 **93/93 文件通过**（生成侧 80 + 消费者侧 13）；
  - C 后 **94/94 文件通过**（生成侧 81 + 消费者侧 13）。
  - 两次运行的生成侧与消费者侧非零退出文件数均为 0。C 后比 B 后多的那 1 个生成侧文件就是 C 新增的 `test_necklace_count_evidence.py`。
- 项链相关：`test_necklace_handoff`(63)、`test_delivery_snapshot`(19)、`test_necklace_count_evidence`(52)、`test_necklace_frozen_visual_projection`(41)、`test_necklace_mixed_integration`(60)、`test_necklace_mixed_profile`(49)、`test_necklace_profile_isolation`(23)、`test_accessory_mixed_integration`(89)、`test_accessory_mixed_templates`(78)、`test_category_execution_adapter`(34)、`test_visual_execution_contract`(8)、`test_production_script_renderer`(41)、`test_simplified_complete_script`(58)、`test_production_script_feishu`(14)、`test_video_prompt_compaction`(42)。以上条数逐项取自 C 后回归日志，非估算。
- 生产库指纹：
  - `longform_original_video.sqlite3` = `6e150904fab2fa1dc172941e72de9876`、`short_video_auto_publish.sqlite3` = `2a2ff0f5a93447e3e00a5ecb849fcc69` —— 全程稳定，两次回归窗口内均报 `[未变]`。
  - `original_script_generator.sqlite3` —— 本轮**开始时**的基线是 `e3191fcffcad8ed1cb9bcaf5d7a5d262`，且 B 后回归窗口内报 `[未变]`。但它在 B 后与 C 后两次回归**之间**变成了 `16bfb598cadf9d0d74c6ff9be30707c7`；事后取证确认是并发的一次飞书真实跑批写的（见下节），不是本轮的测试。C 后回归以 `16bfb59…` 为窗口基线，同样报 `[未变]`。**本记录发布时的当前值是 `16bfb598cadf9d0d74c6ff9be30707c7`**，引用时请用这个值，不要把 `e3191f…` 当成"现在还是它"。
- 回归日志：`full_regression_after_b.log`、`full_regression_after_c.log`（在 `run_6/necklace_v1_c5_evidence/out/`）。

### 运行台的一处误判（已修，供下轮注意）

C 包第一次全量回归报"隔离失败"：生产库 `original_script_generator.sqlite3` 在运行窗口内变化。核查后**不是我们的测试写的**，而是一次同时进行的**飞书真实跑批**：新批次 `OCB_F865585281E69A3EBE22`（`request_id=OP_FEISHU_39F81AFAE5FDCE358F23`、`workflow_type=ORIGINAL_SCRIPT`、`execution_mode=PLAN_ONLY`、`product_type=耳饰`、`target_country=越南`、请求 5 条计划 3 条）在 10:17:28 → 10:17:34 一次性写完即停。生产库里 197 个批次有 103 个来自 `OP_FEISHU_`，这是常规生产入口。

守卫漏检的原因：空闲探测只采样 4 秒，对**偶发**写入方天然漏检；事后 0/6/12 秒确认时写入方已经停止，于是外部写入被误判成我们的越界。已给 `wp0/run_isolated_tests.sh` 加上**归属取证**：一旦发现变化，就把最近写入批次／行的 `request_id`、`product_type`、`target_country`、时间戳打出来（读只读副本，不触碰原库）。指纹只能发现"有人写了"，不能说明"是谁写的"——这一步把两者分开。

## 真实样本与未覆盖项

已使用的真实样本（均取自隔离生产副本，**只读**）：

- 项链冻结合同：`OCI_8762196B817007C9B9F0` / `SCRIPT_8BCEF97D70205D7F6540`。
- 四类配饰冻结合同：`OCI_64E9A8D9D59CD7E3C7DE`(EAR)、`OCI_3BC3BFC65FF9F59D7783`(WRIST)、`OCI_26411410890208D0ABA7`(FINGER)、`OCI_D1B5DDF3D17F7ABC57AC`(HAIR)。
- 夹具出处记录在 `tests/fixtures/accessory_frozen_contracts.json` 的 `_comment` / `_provenance`；抽取脚本 `run_6/necklace_v1_c5_evidence/extract_fixtures.py`。

未覆盖项（明确报告，不降低标准）：

1. **阶段 2 未做**：需要一款真实资料完整的单层单吊坠项链（当前商品图 + 可追溯数量 + 有效卖点）。现有 H 项链 fixture 的人工数量与 TESTSET 卖点只能作测试，不能复制进生产冒充运营确认。原始商品被 `layer_count` / `pendant_count` UNKNOWN 拦下，这正是 F3 修好后应有的行为。
2. **阶段 3 未做**：未通过现有入口生成新脚本，未调真实模型，未做真实导出 → 同步（飞书为内存假表）。
3. **阶段 4 未做**：未生成成片，未人工看链条连续性、吊坠结构与比例、掌心接触、肤色／商品色、镜间光影、实际语音容量。
4. **F2 验证的是文字合同投影**，不能据此宣称成片没有色偏或穿模。
5. 冻结合同里 `zone_label` 缺失，投影记为 `notes` 而非 `gaps`——它只是展示标签，`no-face` 判据看 `execution_profile` + `face_policy`。这一点已在测试中固定，但尚未在真实成片上观察。
6. **创作脚本提示词里仍有内部标识**（**本次之前就存在**，非本轮引入）：`build_simplified_script_prompt` 会把 `_model_visible_creative_seed(seed)` 的 JSON 内嵌进提示词，而 `_model_visible_creative_seed` 只清理 `visual_execution_contract` / `diversity_context` / `creative_direction` / `capture_rhythm_contract` 下的若干键，**没有清理 `category_execution_extension.mixed_template_contract`**。实测（`run_6/necklace_v1_c5_evidence/verify_internal_ids_in_creative_prompt.py` → `out/internal_ids_in_creative_prompt.json`，脚本只读仓库模块、不连库）：

   | 文本 | 长度 | 含 `profile_config_hash` | 含完整 hash `d405e0c3…` | 含 `NMX_WARM_NEUTRAL_WINDOW_V1` |
   | --- | --- | --- | --- | --- |
   | 创作脚本提示词（内嵌 seed JSON） | 38,888 | 是 | 是 | 是 |
   | 最终成片提示词（导出字段，`render_validation.prompt_chars`） | 3,627 | 否 | 否 | 否 |

   同一脚本另行确认：模型可见 seed 里 `visual_execution_contract` **不含** `frozen_projection`（B 包的 pop 生效），但 `category_execution_extension.mixed_template_contract` 整体保留（12 个键，含 `environment_recipe_id`、`capture_units` 等）。是否收窄这条通道**未决**；本轮按"只修 F1–F4、不动其他"的范围没有改它，以免影响已在跑的类目。

