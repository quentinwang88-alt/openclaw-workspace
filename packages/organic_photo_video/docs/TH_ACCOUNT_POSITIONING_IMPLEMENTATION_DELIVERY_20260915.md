# 账号定位贯穿图文生产与发布——实施交付报告

日期：2026-09-15。本文记录本轮实际完成的代码、配置、migration 与测试结果，对应需求为《账号定位进入生成与发布》（用户 2026-09-15 指令稿），方案背景见 `docs/TH_TWO_ACCOUNT_CONTENT_POSITIONING_HANDOFF_20260915.md`。

代码目录：

- 图文包：`packages/organic_photo_video`（下称 OPV）
- 发布器：`skills/short-video-auto-publisher`（下称发布器）

---

## 一、本轮交付的三件事与落点

| 目标 | 实现 |
|---|---|
| 规整账号默认配置与本篇任务配置 | 账号管理表（经发布器同步）为唯一运营来源；OPV `photo_content_profile` 规范化对象 + 仓库种子兜底；任务表新增「目标账号（可选）」单选 |
| 配置贯穿规划、文案、图片和排版 | 本篇配置快照（目标账号＋主题来源＋表达模式＋定位＋视觉基准＋指纹）冻结进 request.theme_brief；主题回退、表达模式、color_grading_plan 进入完整生成链 |
| 指定账号的内容进入该账号的发布流程 | `target_publish_account_id` 从任务快照 → ContentTask 列 → release manifest → 主队列 script_metadata 列 → 领取隔离 → 提交前双重校验 |

**不新增**：新后台、推荐系统、账号等级、额外人工审核步骤、新的生产预设、平行主题目录、第二套主题下拉。运营日常仍是：**选择账号 → 主题留空继承或选择本篇主题 → 填商品与具体要求 → 执行 → 确认发布**。

## 二、概念规整结果

| 概念 | 落点 |
|---|---|
| 账号定位（给谁看、提供什么价值） | 账号表「账号定位」列 → profile.positioning → 旅行规划提示词【账号长期定位】块 |
| 默认主题 | 账号表「默认主题」列 → profile.default_theme → `resolve_photo_theme` 回退（任务显式选择优先） |
| 本篇主题 | 复用任务表现有「图文主题」单选；新增原生图文「一衣多穿」选项 |
| 内容表达 | 账号表「内容表达」（搭配灵感/实用指南）→ profile.expression_mode；任务「内容要求」中显式写「实用指南/搭配灵感」可覆盖 |
| 视觉风格 | 账号表「视频风格/视觉风格」列（复用现有列）→ profile.visual_baseline → 参考分析 + 旅行规划 + color_grading_plan |
| 风格图片 | 账号表「风格图片」附件 → 只保存 file_token/name 指纹（**生成接入未实现，见 §八**） |
| 图文领取范围 | 账号表「图文领取范围」（沿用店铺池/仅本账号任务）→ account_configs.photo_claim_scope |
| 生产预设/Recipe | 复用现有 `feishu_production_presets.json` 与 planning policies，程序内部解析，未新增平行入口 |
| 商品与参考图 | 沿用任务表现有产品编码/参考图/参考图类型入口 |

主题优先级（按项解析，已实现并测试）：**本篇主题 = 任务选择 → 账号默认 → 原有预设行为**；内容表达 = 任务要求显式覆盖 → 账号默认 → 原有表达（旧行为逐字不变）。改主题不清空账号风格，不回写账号默认值。

「一衣多穿」路由隔离：原生图文「一衣多穿」= 旅行线同商品多搭配（theme_key 仍为 COOL_WEATHER_TRAVEL，带 `native_multiway` 身份标记，**必须**提供产品编码或商品参考，否则生成前报错）；legacy 图片视频一衣多穿继续走 `TH｜一衣多穿｜轻文字` 预设与 `THEME_TH_ONE_PIECE_MULTIWAY_V1`，互不路由。生成差异全部由主题/类目/表达/视觉配置驱动，**代码中不存在按账号名称的生成分支**。

## 三、运营操作说明

### 3.1 账号管理表（唯一维护入口）

下次 `run-all`/`sync-accounts` 运行时会自动补建以下列（幂等；已存在同名列则复用）：

| 列 | 说明 |
|---|---|
| 账号定位 | 自由文本：给谁看、持续提供什么价值 |
| 默认主题 | 填任务表「图文主题」的合法选项名（如 凉爽旅行 / 旅行·打卡穿搭 / 一衣多穿） |
| 内容表达 | 单选：搭配灵感 / 实用指南（留空＝默认旧行为） |
| 视频风格（或新建「视觉风格」） | 自由文本：摄影、色调、构图基准；对图文摄影同样生效 |
| 风格图片 | 附件（本轮只存指纹，见 §八） |
| 图文领取范围 | 单选：沿用店铺池（默认）/ 仅本账号任务 |

同一账号多通道行共享定位（同步聚合取第一个非空值）；列留空不会清空已在 SQLite 缓存的配置（同步用 COALESCE 空值保护）。

### 3.2 图文任务表

- 新增单选「**目标账号（可选）**」（已在线上表创建，选项 tocrystal66 / wn0didnad6；可在飞书手动补充，生成侧严格校验）。
- 留空 → 店铺公共池旧行为；选择 → 店铺/市场从账号推导，填错店铺或市场不一致会在生成前报错（不自动借用其他店铺）。
- 主题留空 → 继承账号默认主题；显式选择 → 本篇覆盖。
- 执行后「内容方案摘要」首行回显：目标账号、店铺、主题（来源：任务选择/账号默认/原预设）、表达、视觉基准。
- 确认发布时：行内「店铺」必须等于目标账号所属店铺；行内目标账号与任务冻结值不一致（如事后改行）会报错，转账号走修订/重排。

### 3.3 表达覆盖

任务「内容要求（可选）」里包含「实用指南」或「搭配灵感」字样时覆盖账号默认表达（来源记为 task）。

## 四、代码改动清单

### OPV（packages/organic_photo_video）

| 文件 | 改动 |
|---|---|
| `services/publish_account_profile.py`（新增） | photo_content_profile 规范化、表达/领取范围枚举、指纹、`PublishAccountProfileResolver`（账号表缓存优先，仓库种子 `config/publish_accounts/*.json` 兜底） |
| `config/publish_accounts/tocrystal66.json` / `wn0didnad6.json`（新增） | 两个试点 TH 账号种子配置（含身份核实状态注记） |
| `services/feishu_workflow.py` | `FIELD_TARGET_ACCOUNT`；`_resolve_target_account`（显式填写但解析失败必报错）；账号默认主题回退＋theme_source；市场/店铺一致性校验；一衣多穿需商品校验；表达解析（任务覆盖账号）；account_brief 冻结进 request.theme_brief 与 `target_publish_account_id`；旅行链注入（定位/表达/视觉基准参数）；`build_theme_copy(expression_mode=)`；摘要回显；确认发布目标/店铺校验；MX 假发线与视频线填目标账号显式报错 |
| `services/photo_theme.py` | 「一衣多穿」注册（native_multiway）；`_display_slide_texts`/`build_theme_copy` 表达模式感知（指南模式不裁「名称 — 理由」，默认模式逐字不变；指南模式补收藏/提问 CTA 标记） |
| `services/photo_reference_vision.py` | `analyze_reference`/`plan_travel_content` 增加 account_positioning / expression_mode / account_visual_baseline（仅非空进 input_contract 与提示词，旧任务 hash 逐字不变）；旅行规划输出顶层 `color_grading_plan`（复用 `_normalize_color_grading_plan`）；`build_travel_style_profile` 携带 color_grading_plan 进入 style_profile → 供给 → image_generator【全局色彩合同】（既有通道，无需改 supply/generator）；实用指南/搭配灵感文案规则块（仅主题联动分支） |
| `services/task_intake.py` | TaskRequest.target_publish_account_id；intake_context 冻结 |
| `domain/models.py` | ContentTask.target_publish_account_id（to_row/from_row） |
| `migrations/010_target_publish_account.sql`（新增） | `opv_content_task` 加列 + 索引（**已应用**，见 §五） |
| `services/release_gate.py` | freeze_photo_release 输出 target_publish_account_id（additive，旧 manifest 不变）；`_assert_target_account_consistency`：manifest ↔ 队列 context ↔ 实际领取账号三方一致；validate_upload/validate_photo_upload 增加 `account_id` 参数 |
| `services/main_schedule_bridge.py` | enqueue_task 校验目标账号存在且店铺一致（不一致保留待处理不换号）；context + script_metadata 携带目标账号；get_task_state 回读目标账号与实际账号 |
| `scripts/ensure_feishu_task_table.py` | 任务表字段定义新增「目标账号（可选）」（选项来自仓库种子，确定性可回归） |

### 发布器（skills/short-video-auto-publisher）

| 文件 | 改动 |
|---|---|
| `app/models.py` | ScriptMetadata/AccountConfig/PublishCandidate 新字段 |
| `app/db.py` | `script_metadata.target_publish_account_id`、`account_configs.photo_content_profile_json / photo_claim_scope` 列（建表 + `_ensure_column` 幂等迁移，生产库已生效）；upsert 读写；`list_ready_candidates` 透出目标账号（列值优先，context JSON 兜底）；账号配置 upsert 空值 COALESCE 保护 |
| `app/scheduler.py` | ACCOUNT_FIELD_ALIASES 新列（定位/默认主题/内容表达/视觉风格（复用「视频风格」列名）/风格图片/图文领取范围）；`sync_accounts` 聚合 photo_content_profile + 领取范围（多通道共享）；`filter_candidates_for_account` 双向领取隔离；槽位 pending 原因说明等待目标账号；候选选择循环内二次目标校验；submission_context 冻结目标账号；`_validate_opv_upload(account_id=)` 提交前核对冻结清单目标 + 队列列与 context 互证 |
| `run_pipeline.py` | `ensure_account_nurture_fields` 补建账号表新列（幂等，Text/单选回退） |

### 领取隔离规则（发布器，只影响候选过滤，不改渠道/带货/配额规则）

1. 候选带 `target_publish_account_id` → 只能被该账号领取；目标账号无槽位/暂停时保持待排期并显示原因，不转号。
2. 账号 `photo_claim_scope=own_tasks_only` → 不领取未绑定的原生图文公共池候选（视频与未配置账号不受影响）。
3. 提交前：领取账号 ≠ 冻结目标 → 阻止提交；队列列与 context 目标不一致 → 阻止提交。
4. 旧账号默认 store_pool；旧任务无目标 → 全部旧行为。

## 五、数据与 migration（已执行的部署动作）

| 动作 | 状态 |
|---|---|
| RDS `migrations/010_target_publish_account.sql` | ✅ 已应用（`apply_rds_migration.py --apply`，sha256=fe53d8de…，additive：加列 + 索引，旧行 NULL=店铺池） |
| 发布器 SQLite `account_configs.photo_content_profile_json / photo_claim_scope`、`script_metadata.target_publish_account_id` | ✅ 已通过幂等 `_ensure_column` 语义加列并核验 |
| 试点账号初始配置（tocrystal66 / wn0didnad6）：内容定位缓存 + `photo_claim_scope=own_tasks_only` | ✅ 已写入生产 SQLite（内容与仓库种子一致） |
| 飞书图文任务表「目标账号（可选）」单选（选项：tocrystal66、wn0didnad6） | ✅ 已创建并回读校验（只创建该字段，未触发其他字段变更） |
| 飞书账号管理表新列 | ⏳ 由下次 `sync-accounts`/`run-all` 自动补建（幂等） |

**身份核实状态**（来自生产库只读核对）：`tocrystal66`（泰国女装1）与 `wn0didnad6`（泰国女装2）均为 THFZ01、状态可用、CreatOK 通道、具备 content_photo 能力，各有独立 CreatOK 连接（conn_tc_3QnZ… / conn_tc_1oVv…）。**「likeU shop 显示名 ↔ tocrystal66」已由用户于 2026-09-15 确认**；两个试点账号身份均已核实，可直接用于样片。

## 六、兼容与恢复边界（实现并测试）

- 未选目标账号的行：不新增任何冻结键，input_contract / 提示词 / 冻结请求逐字不变（1408 项 OPV 回归全绿即为证据）。
- 续跑/重拍：批次 manifest 冻结后复用，`validate_frozen_request` 重验含目标字段的指纹；账号配置后续修改不影响已冻结任务（快照内含 profile_fingerprint）。
- 目标账号加入已有旧行＝新任务配置（合理重规划），不是静默续用。
- 文案修订路径（adopt_repaired_copy / allow_copy_only_rebaseline）未触碰；表达模式只影响文案时仍走既有「只换文案复用源图」通道。
- 视频线、MX 假发、VN 围巾、legacy 一衣多穿入口行为不变（误填目标账号会显式报错而非静默忽略）。
- 商品编码仍只表示内容参考商品，不改变 cart_enabled / 挂车规则。

## 七、测试结果

| 套件 | 结果 |
|---|---|
| OPV 全量 `python3 -m unittest discover -s tests` | **1408 通过 / 0 失败**（含新增 `test_publish_account_profile.py` 16 项、`test_target_account_workflow.py` 5 项新用例） |
| 发布器全量 `python3 -m unittest discover -s tests` | **348 通过 / 0 失败**（含新增 `test_target_account_claim.py` 12 项） |

新增用例覆盖的验收点：

1. 同店 B 账号不能领取 A 绑定候选；A（仅本账号任务）不领未绑定公共图文；A 绑定候选 A 可领且 submission_context/落库账号正确；store_pool 账号仍领未绑定内容。
2. 提交前：领取账号≠冻结目标阻止；队列列与 context 改绑阻止。
3. 账号默认主题回退（theme_source=account_default）与任务显式覆盖（theme_source=task）；快照冻结 target/theme_brief.account/指纹；TaskRequest 携带目标账号；摘要回显。
4. 未配置账号、市场不一致、店铺不一致 → 生成前显式报错且不产生冻结批次。
5. 实用指南表达从规划提示词 → `_display_slide_texts`（理由行不被裁）→ `build_theme_copy` 全链保留；默认模式旧输出逐字不变。
6. color_grading_plan 经旅行 normalizer 进入 plan 与 style_profile；未配置时结构逐字不变。
7. ContentTask 目标账号列 roundtrip；manifest 无目标的旧任务按旧语义放行。
8. sync_accounts 从账号表列构建 profile/领取范围；空值不清空已有缓存（COALESCE 保护）。
9. 一衣多穿主题注册、别名、native_multiway 身份；缺商品报错文案指向正确入口。

## 八、未实现项与边界（如实清单）

1. **账号风格图片未接入生成**：账号表「风格图片」附件已保存 file_token/name 指纹并随 profile 冻结，但参考分析/旅行规划尚未消费该图片（试点账号当前以文字视觉基准运行；接入时需处理附件下载 staging 与参考集扩展）。
2. **真实样片未生成**：§九验收要求的四篇 TH 样片（A 两篇：默认主题继承＋一衣多穿覆盖；B 两篇：旅行指南＋明确温度条件）待运营建行执行；本轮只交付代码与离线验证，未调用付费模型。
3. **队列归属真实证据待样片**：隔离逻辑有自动化测试与空队列干净基线（THFZ01 当前待排期图文候选为 0），真实「内容只进入该账号」的证据需样片入队后由 `get_task_state` / publish_slots 记录回读。
4. **月份/温度语义（工作包 D）未单独实现**：本轮沿用现有温度档机制（15/10/5/0°C 档＋文章级温度上下文），未新增月份解析或天气事实校验；账号表文案例句不会自动变成任务事实（定位文本只作规划基准注入）。
5. **旅行色彩 QA 保持观察级**：未放开 strict、未新增色温阈值或自动重生循环（按方案 C-6/C-7 边界）；跨两篇样片观察后再议。
7. 飞书账号管理表新列由下次同步任务补建；如需立即生效可手动运行 `run_pipeline.py sync-accounts`。

## 九、回退方式

- 停止新定位任务：任务行清空「目标账号」即可回到店铺公共池（已冻结任务不受影响）。
- 代码回退：两侧均为 additive 改动，revert 提交不影响旧数据；RDS/SQLite 新列可保留（NULL/默认值＝旧行为）。
- 试点账号领取范围改回：账号表「图文领取范围」选「沿用店铺池」同步，或直接 UPDATE `account_configs.photo_claim_scope`。
- 已入队/已排期内容不因回退自动改号或重发。

## 十、验收清单状态（对照方案 §九）

| 验收项 | 状态 |
|---|---|
| 账号 A 一篇继承默认主题、一篇显式一衣多穿 | 代码就绪＋单测覆盖；真实样片待执行 |
| 账号 B 两篇旅行指南（地点/问题/逐页建议/跨篇风格） | 代码就绪（指南表达全链）；真实样片待执行 |
| 同店两账号不互领 | ✅ 自动化测试（`test_target_account_claim`） |
| 改账号配置不影响旧任务续跑 | ✅ 冻结快照设计＋回归（1408 全绿含续跑用例） |
| 文案修订复用图片不重复生图/入队 | ✅ 未触碰既有通道＋回归 |
| 商品任务同时用环境/穿搭/风格参考 | ✅ 未触碰参考用途机制＋回归 |
| 未配置账号的 TH 现有任务兼容 | ✅ 1408 回归（契约逐字不变有专项断言） |
| MX 假发 / VN 围巾 / legacy 一衣多穿 / 视频发布兼容 | ✅ 回归＋误填目标账号显式报错用例 |
| 近期修复（素材防覆盖、定向重拍、本地化、付费图复用） | ✅ 全量回归覆盖 |

---

## 十一、附：生产预设/图文主题选项精简（同日追加，用户批准后执行）

**线上终态**（已回读核验）：生产预设 19→12 项；图文主题 15→9 项；参考图类型 5→4 项；全表 0 行持有越界值。

| 动作 | 内容 |
|---|---|
| 补选项 | 图文主题追加「一衣多穿」（此前代码就绪但线上下拉缺项） |
| 删垃圾项（零行引用） | 图文主题：日本/白川乡/IMG_8173.JPG/IMG_8172.JPG；生产预设：一条超长误粘文本、一条误填的「旅行·打卡穿搭」；参考图类型：日本 |
| 孤儿行修复 | 10 行（盘点时 6 行 + 执行中发现新填 4 行）「图文｜TH｜温度穿搭」（配置中已无此预设，全部从未执行）统一改为「图文｜TH｜旅行穿搭」，其余填写（主题/地点/目标账号）原样保留 |
| 删停用项 | 生产预设：温度穿搭、穿搭拆解首图、图文小个子显高、MX 假发前后对比/脸型匹配/场景发型（配置同步 `status: disabled` + `entry_group: trial`，0 行引用）；图文主题：秋季穿搭/日常通勤/咖啡约会 |
| 防回加 | ensure 脚本主题播种排除「冷热切换/温度穿搭」（分层线专用，分层预设全部停用）；预设播种只下发 active 预设（既有机制） |

**保留未动**：legacy 视频 7 个选项（有历史行；删除会清空历史行显示——已在临时表实证该行为）、VN 围巾 2 个（配置已停用，历史行保留显示）、六类旅行主题全集（含 0 行的「旅行·配色参考」，六类完整性契约）。

**依赖发现**：TH｜随机养号组合（保留启用）经 `tasks_from` 指向 TH｜五套穿搭｜拆解首图，后者因此保持启用，两个选项都保留。

**恢复方式**：配置把对应预设 `status` 改回 `active`（`entry_group` 相应改回）并重跑 `scripts/ensure_feishu_task_table.py`，或直接在飞书手动加回选项；被隐藏主题的解析能力全程未变。

**验证**：OPV 全量 1408 通过（含更新后的 shipped-catalog 断言：启用中原生图文预设=TH 旅行/TH 四选一/MX 四选一发型；入口清单文档已重新生成）。

---

## 十二、附：视觉预设与完整中文文案轮（同日追加）

**本轮完成 Phase 1 + Phase 2（全量 1416 测试通过）；Phase 3/4/5 未实现，见文末。**

### Phase 1：基础修复（已完成）

| 项 | 实现 |
|---|---|
| 完整文案（中文） | 新增飞书 Text 列「完整文案（中文）」；`services/copy_translation.py` 按最终成片包 copy（title/caption/hashtags/slide_texts）整包翻译，按文案指纹缓存（`<staging>/copy_translations/<fingerprint>.json`），修订文案自动重译；翻译失败降级为占位提示，不影响成片/发布/图片指纹；`scripts/backfill_copy_zh.py --record-id/--all-pending` 独立补跑；多套按【第 N 套】分段 |
| 文案合同冲突 | 旅行提示词的主题联动文案合同改为按表达模式输出**单一规则源**（per_page_rule/final_page_rule/caption_rule 三套一致规则），删除后置「实用指南文案要求」覆盖块 |
| CTA 传递 | `_display_slide_texts` 唯一来源优先级：模型末页自带 CTA（第二行整行或首个 CTA 标记后文本，原样保留）→ planned copy.cta 字段 → 主题兜底；修复「模型写收藏被改成 A/B/C/D 投票」的丢失路径 |

### Phase 2：视觉预设（已完成）

- `services/visual_preset.py` + `config/visual_presets/`：三个版本化预设（清爽室内穿搭 VP_CLEAN_INDOOR_V1 / 纯色搭配解析 VP_SOLID_COLOR_V1 / 旅行场景穿搭 VP_TRAVEL_SCENE_V1），各自声明 background.mode（fixed/scene）、固定场景基准、摄影基准、排版引用、缺图回退。
- 账号 profile 新增 `default_visual_preset`（进指纹）；两个试点账号已配置「旅行场景穿搭」（种子 + SQLite 缓存同步）。
- 任务表新增「视觉预设（可选）」单选（已建线上字段，3 选项）；优先级：本篇选择 → 账号默认 → 入口默认（product_supply=纯色，style_plan=旅行场景，其余=清爽室内；**入口默认只对账号绑定行生效**，无绑定旧行冻结请求逐字不变）。
- 快照：preset_id/name/version/background_mode/source/fingerprint 冻结进 request.theme_brief.visual_preset；摘要首行回显「视觉预设 ××（fixed/scene，来源：…）」。

### Phase 3 固定背景执行（同日第二段追加，已完成）

视觉预设 `background.mode=fixed` 现在真正进入生成链（全量 1418 测试通过）：

- **规划合同**：`plan_travel_content(fixed_background=...)` → 提示词新增【固定背景合同（优先于场景规则）】：四套 Look 共享预设声明的固定场景，scene_prompt 只变姿势/机位/景别/道具，不引入街道地标；travel_moment 仍按枚举选择（供鞋履步行规则）但画面不要求呈现对应场所；地点/温度是内容语境不是画面要求；无真实地标参考不得虚构地标。input_contract 仅固定背景任务新增 `fixed_background` 键（旧任务契约逐字不变）。
- **快照贯通**：`theme_brief.visual_preset` 快照写入 `style_profile.visual_preset`，供给与 QA 据此识别背景方式。
- **QA 豁免**（`normalize_travel_qa(fixed_background=True)`）：①逐页 observed_moment ≠ 计划 moment 不再判 SCENE_MISMATCH（moment 只是规划语境）；②四页同场景护栏豁免（共享背景是设计）；③destination_conflict 不再一票否决（纯色/室内背景不呈现目的地不算冲突）。QA 提示词补【固定背景说明】，模型按画面活动判 moment、纯色背景一律不报地点冲突。
- 参考图不隐式改变背景方式：任务上传的 OUTFIT/风格图在 fixed 模式下只借鉴搭配与摄影，不改变固定背景（提示词合同明确）。

### 仍未实现（下一轮继续）

1. **Phase 3 新版结构化排版器**：`photo_package.py` 的主标题/说明/地点/CTA 分层排版（左对齐、渐变底、避开脸部）未开发——现状仍是单字符串 overlay_text + 圆角底色条；预设 → layout 模板覆盖接线（`visual_preset.layout.template_id` 覆盖配方默认模板）也未接。设计落点：`normalize_photo_template` v3 + `_draw_overlay` 旁新增结构化绘制 + 三处 expected_texts 扁平化。
2. **Phase 4 场景复用**：素材库背景参考标记（用途/目的地/场景类型/季节）与自动选择未开发；账号风格图片附件仍未接入参考分析（只存指纹）。
3. **Phase 5 小样**：固定室内一衣多穿、纯色温度指南两类小样现在已可跑（选视觉预设「清爽室内穿搭/纯色搭配解析」+ 执行）；素材库场景小样待 Phase 4。

### 本轮代码落点

`services/copy_translation.py`（新增）、`scripts/backfill_copy_zh.py`（新增）、`services/visual_preset.py`（新增）、`config/visual_presets/*`（新增 3 个）、`services/feishu_workflow.py`（FIELD_FULL_COPY_ZH/FIELD_VISUAL_PRESET、成片后翻译回写、视觉预设解析与快照、摘要回显）、`services/photo_theme.py`（CTA 唯一来源）、`services/photo_reference_vision.py`（表达规则并入单一文案合同）、`services/publish_account_profile.py`（default_visual_preset）、`services/photo_travel_qa.py`+`services/photo_reference_vision.py`+`services/photo_style_reference_supply.py`（固定背景合同与 QA 豁免）、`scripts/ensure_feishu_task_table.py`（两个新字段声明）、种子与 SQLite 试点配置更新、测试 `tests/test_copy_and_visual_preset.py`（7 项）+ `tests/test_photo_travel_semantics.py::FixedBackgroundGuardTest`（2 项）+ `test_target_account_workflow`（+2 项）+ 既有断言按新契约更新。全量 1418 项通过。
