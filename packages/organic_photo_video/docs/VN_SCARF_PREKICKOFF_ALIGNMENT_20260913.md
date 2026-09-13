# VN 围巾跨市场复用 — 开工前对齐清单

日期：2026-09-13
状态：待拍板（未改任何业务代码）
对应规格：`docs/VN_SCARF_CROSS_MARKET_IMPLEMENTATION_SPEC_20260913.md`
适用项目：`packages/organic_photo_video`
上游交付：`docs/VN_SCARF_PHASE0_BASELINE_20260913.md`（Phase 0 已完成）

---

## 0. 为什么先出这份清单

Phase 1 本身（抽 `PhotoReferenceContext` + Category Adapter、TH V2 零行为变化）设计上安全。
问题在于它的**起点不干净，且"零行为变化"目前无法被独立复现**。

本清单把 5 项待裁定问题按严重度排序，每项给出：现状证据（含 `文件:行`）→ 规格原文 → 冲突点 → 影响哪个 Phase → 建议方案与备选。

核对方式：全部**只读**。使用了 `git worktree add --detach` 在 `/tmp` 建临时 checkout 验证 HEAD，
验证后已 `git worktree remove` 清理；主工作树 84 条脏改动与 938 条绿测**未受任何影响**（复验通过）。

---

## 1. 【阻断级】绿测基线无法从 HEAD 复现

### 现状证据

在隔离 checkout 上跑 HEAD（`6b6abac`）自身：

| 环境 | 结果 |
|---|---|
| 主工作树（当前） | 938 tests **OK** |
| 隔离 checkout 的 HEAD | 825 tests → **failures=3, errors=27（共 30）** |
| 补齐 `shared/data` 后重跑 HEAD | 825 tests → failures=3, errors=27 中 **19 条是 `FileNotFoundError`** |

剩余 `FileNotFoundError` 指向：

```
packages/organic_photo_video/config/profiles/IMAGE_STORY_VIDEO_V1.json
packages/organic_photo_video/config/profiles/QUALITY_OUTFIT_BREAKDOWN_V2.json
```

### 根因：三类**运行必需**的资产被 `.gitignore` 排除

| 资产 | 忽略规则 | 后果 |
|---|---|---|
| `packages/organic_photo_video/config/profiles/*.json` | **`.gitignore:13` `**/config/profiles/*.json`** | 渲染档案/质量档案**从未提交过**（`git log --all` 对该路径为空）。其中 `QUALITY_STANDARD_V1.json` 正是 `PHOTO_TH_TRAVEL_OUTFIT_V2.json` 的 `quality_profile_id` 所指 |
| `shared/data/organic_photo_video/asset_sets/**` | `.gitignore:181` `shared/data/` | 测试素材（`look_a.png` 等）不在版本控制内 |
| `packages/organic_photo_video/var/**` | `.gitignore:42` `var/` | 运行态数据 |

`.gitignore:1-19` 的分节标题是「环境变量与敏感配置」——第 13 行本意是忽略**别的**包（全仓另有
`skills/inventory-query/config/profiles`）的敏感配置，规则写宽了，把 OPV 包的 **shipped config** 一起吞了。
引入提交：`9a0f11f`（2026-04-19 「Clean workspace backup residue and temp artifacts」）。

**判定它属于 shipped config 而非环境资产**：`tests/test_config_loader.py:26
test_bundle_counts_match_phase0_plan` 对 `render_profiles == 2`、`quality_profiles == 3` 做**精确计数断言**，
且 `config/profiles/` 会被 `config/loader.py` 的 `load_render_profiles/load_quality_profiles` 校验加载。
被测试精确断言的东西不应该是不入库的环境数据。

### 冲突点

- 规格 §7 Phase 0 要求"跑现有测试"建立基线——但**基线不可移植**。
- 规格 §7 要求"每个 Phase 独立提交"、§12 要求"回滚只需禁用 preset/route"——**回滚安全网目前是假的**：
  任何"切到 HEAD 再验证"的动作都会因为缺 `config/profiles/` 而大面积失败，无法区分"回滚生效"还是"环境缺失"。
- 规格 §9 发布门禁假定配置可追溯——但配方依赖的 profile 没有版本历史。

### 建议方案

**A（推荐）**：把 `packages/organic_photo_video/config/profiles/*.json` 纳入版本控制。
最小改动是给 `.gitignore` 加否定规则或收窄第 13 行，例如
`!packages/organic_photo_video/config/profiles/*.json`（或把 `**/config/profiles/*.json`
改为只匹配确实敏感的路径）。然后确认 `shared/data/`、`var/` 是否也应有最小 fixtures 入库。

**B**：保留现状，但在 `docs/` 里写一份「运行态资产清单」，规定所有验证必须在本工作树执行、
禁止用隔离 checkout 做回滚验证。

**C**：什么都不做——**不推荐**，Phase 1 之后每次 golden 失败都要先排除环境噪声。

### 需你拍板

1. `config/profiles/*.json` 是有意不入库（环境资产），还是被误伤（应入库）？
2. `shared/data/` 的测试素材是否需要最小 fixture 集入库，以便"干净 checkout 可复现绿测"？

---

## 2. 【① 提交检查点】84 条脏改动如何落定

### 现状证据

| 项 | 值 |
|---|---|
| HEAD | `6b6abac02ba143ae9b037ad48f6a06e88032092f` |
| 脏条目 | **84**（OPV 包内 58） |
| 分布 | `packages/organic_photo_video` 58 / `skills/original-script-generator` 10 / `packages/remake_video_execution` 6 / 其他 10 |

OPV 包内 **9 个新增 service + 10 个新增 test 从未提交**，而
`services/photo_flow_registry.py` 被 `services/photo_content_planner.py:10` import——
两边都是本次未提交改动，因此**部分提交会造出一个不自洽的 HEAD**。

Phase 1/2 目标文件**全部已有重叠改动**：

| 文件 | 现有 diff |
|---|---|
| `services/photo_reference_vision.py` | +378 / -26 |
| `services/feishu_workflow.py` | +478 / -46 |
| `services/photo_package.py` | +198 / -2 |
| `services/photo_style_reference_supply.py` | +101 / -15 |
| `services/photo_copy.py` | +95 / -5 |
| `services/photo_content_planner.py` | +91 / -7 |

### 冲突点

规格 §14 要求"遇到目标文件已有重叠修改时，在现状上做最小增量改造"；§7 要求"每个 Phase 独立提交"。
但**核心 services 文件同时承载了「热转线」和「旅行体感线」两条线的改动**，
所以"按业务域分组提交"在文件粒度上**做不到干净**。

### 建议方案

**A（推荐）**：先解决 §1（profiles 是否入库），再落**一个**「OPV photo 域基线」提交，
把包内 58 条一起纳入，提交信息里如实写明它同时含热转线与体感线。理由：这是唯一能产出
**自洽 HEAD** 的低成本方式；Phase 边界从 Phase 1 开始才需要严格。
（另 26 条非 OPV 域改动建议单独提交或暂时不动，见 §6 表。）

**B**：用 `git add -p` 做 hunk 级切分，分「热转线」「体感线」两个提交。边界干净，但
`photo_reference_vision.py` 单文件 378 行改动跨两线，切分成本高且有误切风险。

**C**：暂不提交，仅在文档中冻结快照，继续推进。——**不推荐**，golden 失败时无法归因。

### 需你拍板

选 A / B / C。若选 A，我需要你确认「一次提交是否接受把两条线混在一起」。

---

## 3. 【② category 目录与 schema 冲突】

### 现状证据

- `config/categories/` **已被占用**：`WOMENSWEAR_V1.json`、`WIG_V1.json`，schema 为
  `opv-category-profile-v1`（字段：`interest_drivers / visual_dimensions / asset_requirements / content_rules`）。
- `config/loader.py:47` `CATEGORY_PROFILE_SCHEMA = "opv-category-profile-v1"`
- `config/loader.py:76` 校验时**硬比较** schema_version；`:182-184` `load_category_file`；
  `:188-189` `load_categories` 走**全目录 glob `*.json`**
- `config/loader.py:422` `load_categories(config_dir / "categories")`
- `tests/test_config_loader.py:27` **硬编码** `len(bundle.categories) == 2`
- `scripts/preflight_native_photo.py:48` 用该目录构建 `category_key` 集合

### 规格原文

§3.3 要求新增 `config/categories/SCARF_V1.json`，且 `schema_version = "opv-category-profile-v2"`，
字段为 `capabilities / accepted_product_categories / main_product_slot / product_label_zh /
required_product_roles / identity_attributes / product_reference_priority_by_slot`。

### 冲突点

同一目录放进 v2 文件后，`load_category_file` 的 glob 会读到它并按 **v1** 校验 → 直接抛
`ConfigLoadError`，连带 `preflight_native_photo.py` 整体报错。
规格 §6.2 只提到"`domain/contracts.py` 增加 category profile v2 校验"，
**漏了 `config/loader.py` 的 glob、schema 常量与 `test_config_loader` 的计数断言**。

另外：现有 v1 profile **不含任何商品槽位/参考图优先级字段**，所以 §3.3 说的
"WOMENSWEAR adapter 必须复刻当前逻辑"不可能指这个文件——实际要复刻的是
`services/photo_reference_vision.py:1632` 的
`target_fields = {"outerwear": ("outerwear","外套"), ..., "dress": ("outerwear","连衣裙")}`
（按 `category` 分支）。规格这处判断**是对的**。

### 建议方案

**A（推荐）**：沿用仓库**已有的同目录多 schema 先例**——
`config/loader.py:94 validate_photo_layout` 已经同时处理 `opv-photo-layout-v2` 与 v1
（在 `config/layouts/` 同一目录内按 `schema_version` 分派）。

按同样模式做：

1. 加常量 `CATEGORY_PROFILE_SCHEMA_V2 = "opv-category-profile-v2"`；
2. `validate_category_profile` 顶部按 `schema_version` 分派到 v2 校验分支（v1 路径逐字不动）；
3. `load_category_file` 返回类型仍是 `Dict`，无需改结构；
4. `tests/test_config_loader.py:27` 计数 2 → 3（**有意更新**，与上一轮 `content_recipes 19→18` 同类）。

**B**：把 Category Adapter 放到新目录 `config/category_adapters/`，与 v1 content profile 物理隔离。
更干净，但偏离规格 §3.3 的路径，需要规格作者确认。

### 需你拍板

选 A（沿用先例、同目录共存）还是 B（新目录）？

---

## 4. 【③ Market Pack 是 config → RDS 管道，不只是配置文件】

### 现状证据

- `config/loader.py:362 load_market_packs()` 加载 `config/market_packs/*.json`（`opv-market-pack-v1`）
- `scripts/seed_reference_data.py`：**默认 dry-run**，`--apply` 才 upsert：
  - `market_packs / themes / render_presets / **content_recipes** / render_profiles / quality_profiles`
  - 走 `RdsRepository`（`repositories/rds_repository.py:188 upsert_market_pack`、`:1662 upsert_content_recipe`）
  - **不 seed `categories`** —— 类目档案是纯 config，无 DB 步骤
- 生产侧读 DB：`task_intake.py` / `content_planner.py` / `photo_planner.py` / `preflight.py`
  全部走 `repository.get_market_pack(...)`
- 已有 `MP_TH_DEFAULT_v1.json`（`target_country=TH`、`fallback_allowed=false`）、`MP_MX_DEFAULT_v1.json`；
  账号 `config/accounts/*.json` 已绑 `default_market_pack_id`
- **`LIKEU_AI_DATABASE_URL` 已在当前环境中设置** → `RdsRepository.from_env()` 可用 →
  `--apply` 是**可真实落库**的
- `tests/test_config_loader.py:28` 断言 `len(bundle.market_packs) == 2`

### 冲突点

规格 §3.4 / §6.1 把 `config/market_packs/MP_VN_DEFAULT_v1.json` 描述为"新增文件"，
但真正生效还需要一次 `--apply`（= **RDS 写入**），而 §12 又说"数据库变更只能是可回滚的增量"、
"真实飞书写入…必须分别经过对应阶段门禁"。规格**没写这一步，也没写门禁**。

连带影响：Phase 2 的 `PHOTO_TRAVEL_OUTFIT_V3.json` 也是 `content_recipes` 之一 →
**同样需要一次 seed 才能在 production 生效**。

### 建议方案

把 seed 定义为一个**显式门禁步骤**，并遵守：

1. Phase 2/4 的 seed **只在**用户逐次批准后执行；
2. 先 `--apply` 之前必跑 dry-run，把 `bundle.summary_lines()` 的计数清单交给你确认；
3. 只 upsert **新增 id**（`MP_VN_DEFAULT_v1`、`PHOTO_TRAVEL_OUTFIT_V3`），
   不传 `--deprecate-preset`，不触碰 TH/MX 既有行；
4. 跑完**回读**：确认 `MP_TH_DEFAULT_v1`、既有 recipe 的 `status/version` 未变；
5. 离线开发与测试（Phase 1–4 的 canary）**完全不需要** seed，不受阻塞。

### 需你拍板

1. 是否允许在 Phase 2 / Phase 4 各执行**一次** `seed_reference_data.py --apply`（RDS 增量 upsert）？
2. 是否要求我先给你 dry-run 计数清单，你点头后再 apply？

---

## 5. 【④ `MATCHING_CHOICE_V2` 全仓不存在】

### 现状证据

- `grep -rl "MATCHING_CHOICE_V2" config/ services/` → **零引用**
- 现有 18 个配方分两个家族：
  - **native_photo 家族**（有 `media_kind` + `category_key`）：`PHOTO_MX_*`(5) / `PHOTO_TH_*`(8)
  - **旧 board 家族**（无 `media_kind`/`category_key`）：`RECIPE_MULTI_LOOK_V1`、`RECIPE_OUTFIT_BREAKDOWN_V1`、
    `RECIPE_PAIN_POINT_SOLUTION_V1`、`RECIPE_SCENE_SOLUTION_V1`、`RECIPE_VISUAL_TRANSFORM_V1`
- `tests/test_config_loader.py:34` 断言 `len(bundle.content_recipes) == 18`

最接近"围巾日常搭配（同一条围巾搭四套 Look / 配色选择）"的现成骨架是
`PHOTO_TH_PICK_YOUR_LOOK_V3`（`content_goal=pick_your_look`，native_photo，womenswear，
已接入 `RECIPE_POLICY_FILES` 与 planning policy）。

### 冲突点

规格 §5.2「围巾日常搭配」线写作 `MATCHING_CHOICE_V2 × SCARF_V1 × MP_VN_DEFAULT_V1 × LOCALE_VI_VN_V1`，
但该配方在两个家族里**都不存在**，也没有命名来源。Phase 4 该线缺依赖，
且 `content_recipes` 计数会因此再 +1（18 → 19 → 20）。

### 建议方案

**A（推荐）**：由我在 Phase 4 定义它，落在 **native_photo 家族**，复刻
`PHOTO_TH_PICK_YOUR_LOOK_V3` 骨架 + Phase 2 的 recipe v2 参数化，命名建议
`PHOTO_MATCHING_CHOICE_V3`（与 V3 通用骨架对齐），而非沿用规格里的 `MATCHING_CHOICE_V2`
（名字暗示旧 `RECIPE_*` 家族，易误导）。

**B**：严格沿用规格命名 `MATCHING_CHOICE_V2`。

**C**：规格作者另有出处，请补充该配方的来源文件/设计稿。

### 需你拍板

选 A / B / C（若 C，请给出出处）。

---

## 6. 只读核对汇总

| 规格假设 | 现状核对 | 结论 |
|---|---|---|
| `services/photo_reference_context.py` | 不存在 | ✅ 需新建，规格准确 |
| `services/photo_category_registry.py` | 不存在 | ✅ 需新建，规格准确 |
| `config/categories/SCARF_V1.json` | 目录已存在且有 2 个 **v1** 档案 | ⚠️ schema 冲突，见 §3 |
| `config/market_packs/MP_VN_DEFAULT_v1.json` | 目录已存在，2 个包已被 seed 进 RDS | ⚠️ 需 RDS 门禁，见 §4 |
| `config/locales/` | 不存在 | ✅ 需新建，规格准确 |
| `config/destinations/` | 不存在 | ✅ 需新建，规格准确 |
| `config/recipes/PHOTO_TRAVEL_OUTFIT_V3.json` | 不存在（现有 18 个配方） | ✅ 需新建；计数 18→19 |
| `config/photo_planning_policies/TRAVEL_OUTFIT_V3.json` | 不存在（现有 3 个 policy） | ✅ 需新建 |
| `MATCHING_CHOICE_V2` | **全仓零引用** | ❌ 缺依赖，见 §5 |
| `photo_reference_vision.py:1632 target_fields` 硬编码 | 确认存在 | ✅ 规格判断正确 |
| Phase 0 基线 | 已完成，938 OK | ✅ 已交付 |

**非 OPV 域的 26 条脏改动**（`skills/original-script-generator` 10、`packages/remake_video_execution` 6、
`skills/short-video-auto-publisher` 3、`skills/script-run-manager-sync` 3、`runtime` 2、`DREAMS.md` 1、
一个名为 `-` 的误建文件 1）与本规格无关，建议**单独处理，不在本次范围**。

---

## 7. 建议的开工顺序

```
① 拍板 §1（profiles 是否入库）      ← 决定基线能否复现，优先
② 拍板 §2 提交方案 → 落自洽 HEAD
③ 拍板 §3 / §4 / §5 口径
④ 进 Phase 1（PhotoReferenceContext + Category Adapter，零行为变化）
```

§3/§4/§5 的裁定结果可先记录，**不阻塞 Phase 1**（Phase 1 只动参考解析与商品槽位调用，
不新增配方、不新增类目档案、不写 RDS）。但 §1 与 §2 建议**先解**，
否则 Phase 1 的"零行为变化"证明缺一个可信的对照基准。

---

## 8. 本次未做的事

- 未修改任何业务代码、配置或测试（本清单本身除外）。
- 未提交、未 reset、未 checkout、未 stash、未删除任何文件。
- 未连接/写入 RDS，未调用飞书，未发布，未真实生图。
- 临时 checkout 已在验证后清理（`git worktree remove`），主工作树复验 352 photo 域测试 OK。
