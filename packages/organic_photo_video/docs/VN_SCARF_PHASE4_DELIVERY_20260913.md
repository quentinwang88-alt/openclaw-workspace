# VN 围巾跨市场 · Phase 4 交付：VN 激活配置

- 规格：`docs/VN_SCARF_CROSS_MARKET_IMPLEMENTATION_SPEC_20260913.md` §7 Phase 4
- 日期：2026-09-13
- 基线提交：`e07b2b2`（Phase 3 完成态：SCARF 类目适配器 + accessories 商品槽位）
- 本阶段边界：**只做「越南激活配置 + 三种参考模式的离线 canary」**。不新增服务分支、不改共享运行时行为、不启用任何 VN 生产预设或发布路由、不真实生图、不写飞书/RDS、不发布。
- 本阶段结论：**VN 围巾「离线规划能力」已具备**；VN 仍**不可发布**（见 §7）。

---

## 1. 修改文件清单

### 1.1 新增（12 个）

| 文件 | 行数 | 作用 |
|---|---:|---|
| `config/market_packs/MP_VN_DEFAULT_v1.json` | 105 | VN Market Pack（`opv-market-pack-v1`，`status: draft`）：`target_country=VN`、`target_locale=vi-VN`、`fallback_allowed=false`、围巾可见性规则、vi-VN copy 规则（`NATIVE_APPROVED` 要求）、**目的地权重**、两条内容线配比（`scarf_matching:5 / scarf_travel:7`）、旅行主题优先级、本地发布时段、禁用项（含「单条围巾不保证保暖」）。 |
| `config/locales/LOCALE_VI_VN_V1.json` | 254 | 越南语 Locale Pack（`opv-photo-locale-pack-v1`，`status: draft`，`fallback_allowed=false`）：`labels`（travel_moments 7 / destinations 6 / temperature_bands 5 / generic 4）、`family_copy` **17 组**（旅行线 8 + 搭配线 9）、`complete_look_copy` 4 组。 |
| `config/copy_packs/VN_TRAVEL_OUTFIT_V1.tsv` | 5 | 旅行线 4 条 vi-VN 模板（`recipe_id=PHOTO_TRAVEL_OUTFIT_V3` / `profile_id=travel_scene_four_looks`），全部 `language_review_status=DRAFT`。 |
| `config/copy_packs/VN_SCARF_MATCHING_V1.tsv` | 5 | 搭配线 4 条 vi-VN 模板（`recipe_id=PHOTO_MATCHING_CHOICE_V3` / `profile_id=scarf_four_looks`），全部 `DRAFT`。 |
| `config/recipes/PHOTO_MATCHING_CHOICE_V3.json` | 222 | 围巾**日常搭配线**配方（`opv-content-recipe-v2`，`status: draft`）：`content_goal=pick_your_look`、`market_policy=MARKET_PACK_REQUIRED`、`locale_copy_packs={"vi-VN":"VN_SCARF_MATCHING_V1"}`、`asset_set_keys=["VN_SCARF_CHOICE"]`。**不含** `markets`/`category_key`/`locale`。 |
| `config/photo_planning_policies/MATCHING_CHOICE_V3.json` | 484 | 搭配线规划策略（`opv-photo-planning-policy-v1`，`policy_id=MATCHING_CHOICE_V3`）：9 个形态族，`planning_flow=reference_contract_v1`；已剥离全部 `title_th/cover_th/caption_th/display_label`。 |
| `config/accounts/OPV_VN_TEST_001.json` | 68 | VN 试点账号（`opv-account-profile-v1`）：`target_country=VN`、`default_locale=vi-VN`、`timezone=Asia/Ho_Chi_Minh`、`status=paused`、`persona_ref_id=TH_APPAREL_REAL_03_001`、`allowed_persona_refs` 3 个、`publishing_enabled=false`。 |
| `tests/photo_vn_canary_fixture.py` | 452 | 三份离线 canary manifest 的**确定性生成器**（纯函数，无 IO/网络/飞书/RDS/生图）；同时被再生成测试与 `python` 直接执行调用。 |
| `tests/test_photo_vn_activation.py` | 689 | **55 条** VN 激活测试：配置加载、vi-VN fail-loud、零泰语泄漏、TH↔VN 只差 market/locale、生产门禁、canary 再生成、无 VN 服务分支。 |
| `tests/fixtures/PHASE4_CANARY_VN_SCARF_STYLE.json` | 227 | canary 1：VN + scarf + STYLE（无商品）。 |
| `tests/fixtures/PHASE4_CANARY_VN_SCARF_STYLE_PRODUCT.json` | 341 | canary 2：VN + scarf + STYLE + PRODUCT（首尔；同一围巾 A-D + 围巾 QA 合同）。 |
| `tests/fixtures/PHASE4_CANARY_VN_SCARF_COMPLETE_LOOK.json` | 150 | canary 3：VN + scarf + COMPLETE_LOOK（4 张上传图，不生图）。 |

### 1.2 修改（11 个，+254 / −24）

| 文件 | 变更 |
|---|---|
| `config/feishu_production_presets.json` | 新增 **2 个 disabled VN 预设**：「图文｜VN｜围巾旅行」（`PHOTO_TRAVEL_OUTFIT_V3`，`native_photo_style_plan_v1`）与「图文｜VN｜围巾搭配四选一」（`PHOTO_MATCHING_CHOICE_V3`，`native_photo_product_supply_v1`），均 `status=disabled` + `disabled_reason`。 |
| `config/locales/LOCALE_TH_TH_V1.json` | 新增 `complete_look_copy`（4 条，**逐字**等于规划器内联泰语常量）+ 说明字段；其余不变。目的是让 TH 包与 VN 包同构（COMPLETE_LOOK 文案也由 Locale Pack 拥有）。 |
| `config/recipes/PHOTO_TRAVEL_OUTFIT_V3.json` | `locale_copy_packs` 由 `{th-TH: …}` 扩为 `{th-TH: …, vi-VN: "VN_TRAVEL_OUTFIT_V1"}`（+1 行）。 |
| `domain/contracts.py` | `validate_locale_pack_payload` 增加 `complete_look_copy` 校验（存在时须为非空列表，每项 `title/cover/caption` 非空串）。**未放宽任何旧合同**。 |
| `services/photo_locale.py` | 新增 `complete_look_copy(locale_pack)` 解析器并登记 `__all__`；缺字段/结构非法时抛 `PhotoLocaleError`。 |
| `services/photo_content_planner.py` | `_complete_look_plan(...)` 增加 `locale_pack=None` 形参：`None` → 保持原内联泰语常量（**TH 路径零变化**）；绑定包 → 读 `complete_look_copy` + Locale Pack 的通用 CTA。`plan_th_choice_batch` 调用点透传 `locale_pack`；`RECIPE_POLICY_FILES` 登记 `PHOTO_MATCHING_CHOICE_V3 → MATCHING_CHOICE_V3.json`。 |
| `tests/test_config_loader.py` | 计数更新：market_packs 2→3、content_recipes 19→20、locale_packs 1→2、photos 14→15；draft canary 集合加 `PHOTO_MATCHING_CHOICE_V3`；v2 `planning_flow` 改为按 recipe 分别断言。 |
| `tests/test_feishu_workflow.py` | native_photo 预设 8→10；`category_key` 集合加 `scarf`；`expected_routes` 加两个 VN 预设。 |
| `tests/test_photo_locale.py` | `locale_packs` 列表加 `LOCALE_VI_VN_V1`；**新增** `CompleteLookLegacyEquivalenceTest`（3 条）：锁定 `locale_pack=None` 的内联泰语 4 条文案与其下标轮换、主题 CTA 优先级，并证明 TH 包逐字复刻同一表。 |
| `tests/test_photo_request_factory.py` | recipes 14→15、`recipe_count` 14→15、`profile_count` 30→31、`canary_market_unbound` 改为 `["PHOTO_MATCHING_CHOICE_V3","PHOTO_TRAVEL_OUTFIT_V3"]`。 |
| `tests/test_photo_travel_v3_equivalence.py` | `locale_copy_packs` 断言改为 th-TH + vi-VN；`copy_variants_by_locale` 断言改为 `["th-TH","vi-VN"]` 并显式锁定 `copy_variants` 仍为 TH 集。 |

**未触碰 / 未提交（属其它进程的工作区改动）**：`services/image_generator.py`（`billing` 失败分类 + 模型阶梯短路）、`services/photo_wig_supply.py`（OneRoute 环境键）、`tests/test_image_generator_channels.py`、`docs/TH_PHOTO_REFERENCE_MODULE_HANDOFF_20260907.md`、`scripts/run_feishu_scanner_locked.sh`，以及 `packages/remake_video_execution`、`skills/*` 等非 OPV 域改动。

---

## 2. 新增/变更合同

**未新增任何 schema**；只扩 1 处 + 用既有 schema 承载新配置。

1. **`opv-photo-locale-pack-v1`（扩展）**：新增可选键 `complete_look_copy`——存在时须为非空列表，每项为 `{title, cover, caption}` 三个非空串。缺省时不校验（TH 包在 Phase 4 之前没有该键，行为不变）。该键承载的正是 Phase 4 之前硬编码在 `_complete_look_plan` 里的 COMPLETE_LOOK 中性文案。
2. **其余合同零变更**：`opv-market-pack-v1`、`opv-account-profile-v1`、`opv-content-recipe-v2`、`opv-photo-recipe-v2`、`opv-photo-planning-policy-v1`、`opv-photo-execution-context-v1`、`opv-photo-destination-catalog-v1`、`opv-category-profile-v2` 的字段、校验路径与报错文案均未改动。

新增配置**全部走既有合同**：Market Pack、Locale Pack、Copy Pack（TSV）、Recipe v2、Planning Policy v1、Account Profile v1 —— 这正是规格 §11「新增一个市场只需添加 Market、Locale、账号、目的地权重和 preset 配置，不改核心 Python」的验证点。

---

## 3. 向后兼容说明

- **TH V2 / TH V3 逐字不变**：`_complete_look_plan` 的新形参默认 `None`，走原内联泰语表；新增的 `default_cta` 变量其值等于原字面量 `"คุณชอบลุคไหน?"`，`theme.get("cta") or default_cta` 与原 `theme.get("cta") or "คุณชอบลุคไหน?"` 等价。`CompleteLookLegacyEquivalenceTest` 逐字锁定该路径。
- **TH Locale Pack 只增不改**：新键 `complete_look_copy` 与 `_complete_look_copy_note`，既有 `labels` / `family_copy` / `status` 一字未改；`test_photo_locale.LegacyEquivalenceTest` 继续证明 TH 包 = V1 内联表。
- **V3 只加了一条 locale → copy pack 映射**：`copy_variants`（默认集）仍取排序后第一个 locale（`th-TH`），`test_photo_travel_v3_equivalence` 显式锁定，Phase 2 的 TH 等价合同未破。
- **`locale_pack=None` 仍是默认**：所有既有调用方（飞书工作流、request factory、其它计划入口）不传该参数，因此行为与 Phase 3 完全一致。
- **未动共享运行时行为**：`photo_locale` 仍是纯函数模块（无 IO / 无 `from services.`）；规划器未新增任何国家字面量分支（`NoVnServiceBranchTest` 断言 `photo_content_planner.py` 中不存在 `"VN"`/`"TH"` 字面量）。
- **清单计数类基线按预期更新**（market_packs 2→3、content_recipes 19→20、locale_packs 1→2、photos 14→15、presets 8→10、recipes 14→15、profiles 30→31），均已在测试内注明日期与原因。

---

## 4. 执行过的测试命令与完整结果

```bash
cd /Users/likeu3/.openclaw/workspace/packages/organic_photo_video

PYTHONPATH=.:tests /usr/bin/python3 -m unittest tests.test_photo_vn_activation
PYTHONPATH=.:tests /usr/bin/python3 -m unittest tests.test_photo_locale tests.test_config_loader \
  tests.test_feishu_workflow tests.test_photo_request_factory tests.test_photo_travel_v3_equivalence
PYTHONPATH=.:tests /usr/bin/python3 -m unittest \
  tests.test_photo_reference_vision tests.test_photo_content_planner \
  tests.test_photo_theme_reference tests.test_photo_asset_supply \
  tests.test_photo_persona_pack tests.test_product_reference_resolver \
  tests.test_photo_travel_semantics tests.test_photo_package \
  tests.test_photo_feishu_batch tests.test_feishu_workflow
PYTHONPATH=.:tests /usr/bin/python3 -m unittest discover -s tests -p 'test_photo_*.py'
PYTHONPATH=.:tests /usr/bin/python3 -m unittest discover -s tests -p 'test_*.py'
/usr/bin/python3 scripts/preflight_native_photo.py
git diff --check
```

| 命令 | 结果 |
|---|---|
| `tests.test_photo_vn_activation` | **55 OK**（本阶段新增） |
| `test_photo_locale` + `test_config_loader` + `test_feishu_workflow` + `test_photo_request_factory` + `test_photo_travel_v3_equivalence` | **OK**（含 `test_photo_locale` 增至 26 条） |
| 规格 §14.2 点名的 10 个照域模块 | **OK** |
| photo 域全量 | **550 OK**（Phase 3 后 492 + 58 新增） |
| 全量回归（工作区） | **1168 OK**（Phase 3 后 1109 + 58 + 其它进程 1 条口径差异） |
| `scripts/preflight_native_photo.py` | `exit 0`、`errors: []`、`recipes: 15 / profiles: 31`、`canary_market_unbound: ["PHOTO_MATCHING_CHOICE_V3","PHOTO_TRAVEL_OUTFIT_V3"]` |
| `git diff --check` | 无输出 |

### 4.1 隔离验证（两棵干净树）

`git archive` 导出 HEAD，`git write-tree` 叠加**仅本阶段 23 个文件**（不含其它进程的 `image_generator` billing hunk 等），并挂入机器相关依赖（`.env` / `.env.local`、`shared/data`、兄弟 skill 的 `var/` 运行库）：

| 树 | 结果 |
|---|---|
| 纯 HEAD（`e07b2b2`，Phase 3 完成态） | **1108 OK** |
| HEAD + 仅本阶段改动（`git write-tree` = `c0e1453`） | **1166 OK** |

**1166 − 1108 = 58 = 本阶段新增测试数（55 激活 + 3 互补等价）。既有 1108 条一条未改变结果。**

> 首次隔离运行曾出现 3 条 `test_photo_wig_choice` 失败（`unable to open database file`），经排查是漏挂 `shared/data` 与兄弟 skill 的 `var/` 运行库所致；补齐后**两棵树同为全绿**，且该失败在纯 HEAD 树上同样出现，与 Phase 4 无关。

### 4.2 离线探针（规划侧实际输出）

三份 canary manifest 的 `content_plan` 即探针输出（§6）。要点：

- **STYLE**：`Đi Seoul mặc gì khi 0-5°C?` / caption `Thời tiết Seoul khoảng 0-5°C. Xem 4 look rồi chọn giúp mình A B C hay D nhé.`，look 角色 `look_a..look_d` 齐备。
- **STYLE + PRODUCT**：`content_plan` 与 STYLE **逐字节相同**（商品不经内容计划进入），差异仅 `context.product` 与 `product_application`。
- **COMPLETE_LOOK**：item schema 为 `opv-photo-content-plan-item-v1`，`looks: []`，`Hôm nay chọn set nào?`。
- 三份 manifest 全文 **零泰语**（`[฀-๿]` 正则扫描）。

### 4.3 新测试的「咬得住」自检（9 / 9 捕获）

| 变异 | 是否被捕获 |
|---|---|
| VN Locale Pack 删除一个搭配线形态族 | ✅ |
| `complete_look_copy[0].title` 置空 | ✅ |
| 往 VN Copy Pack 注入一个泰语词 | ✅ |
| canary fixture 与配置漂移 | ✅ |
| 把一个 VN 预设改为 `active` | ✅ |
| 新增一条 VN 发布路由 | ✅ |
| 目的地权重加入词表外的键 | ✅ |
| VN 账号 `publishing_enabled` 置 true | ✅ |
| TH 包 `complete_look_copy` 与内联表不一致 | ✅ |

（变异脚本在验证后逐文件还原，并 `cmp` 复核全部恢复。）

---

## 5. 未执行的真实调用

本阶段**未**执行，且未以任何方式触发：

- **真实生图 / 视觉 QA canary**：规格 §8.4 的 6 张对照图 **未生成**（Phase 4 只交付「离线 manifest」，§14.2）。
- **真实飞书写入 / 任务创建 / RDS 播种**：未做。`MP_VN_DEFAULT_v1`、`PHOTO_TRAVEL_OUTFIT_V3`、`PHOTO_MATCHING_CHOICE_V3` 均**未** `seed_reference_data.py --apply`（按开工前对齐清单 §4：离线开发与测试不需要 seed）。
- **真实商品图包同步**：未做（未拉取任何商品数据）。
- **真实人物包解析**：仅**只读**核验（见 §5.1），未写入、未同步。
- **TikTok / CreatOK 发布**：未做。VN 账号仍 `status=paused`、`publishing_enabled=false`，无 VN 发布路由。

### 5.1 人物包验收证据（只读）

对 VN 账号声明的 3 个候选（公共人物模板库 `skills/lightweight-tryon-video/var/light_tryon.sqlite3:persona_templates`，`mode=ro`）逐一跑 `build_persona_pack` + `evaluate_persona_pack`：

| persona_ref_id | status | 参考图 | 角色 | ready |
|---|---|---|---|---|
| `TH_APPAREL_REAL_03_001`（账号默认） | enabled | 4 | FACE_FRONT_NEUTRAL / FACE_THREE_QUARTER / BODY_FULL_NEUTRAL / BODY_FULL_MOTION | ✅ |
| `TH_APPAREL_REAL_02_001` | testing | 4 | FACE_FRONT_NEUTRAL / FACE_AUXILIARY / FACE_THREE_QUARTER / BODY_FULL_NEUTRAL | ✅ |
| `TH_APPAREL_REAL_01_001` | testing | 4 | FACE_FRONT_NEUTRAL / FACE_THREE_QUARTER / BODY_FULL_NEUTRAL / BODY_FULL_MOTION | ✅ |

三者 `approved_count=4`、脸 + 全身证据齐备、`issues=[]` → 满足规格 §5.5「每个人物包必须通过现有 `evaluate_persona_pack()`」。**人的可用性（是否适合 VN 围巾账号定位）仍需运营审核**，故账号保持 `paused`。

---

## 6. canary 产物路径与 manifest

| 产物 | 路径 |
|---|---|
| canary 1｜STYLE（无商品） | `tests/fixtures/PHASE4_CANARY_VN_SCARF_STYLE.json` |
| canary 2｜STYLE + PRODUCT（首尔） | `tests/fixtures/PHASE4_CANARY_VN_SCARF_STYLE_PRODUCT.json` |
| canary 3｜COMPLETE_LOOK | `tests/fixtures/PHASE4_CANARY_VN_SCARF_COMPLETE_LOOK.json` |
| 生成器（可重放） | `tests/photo_vn_canary_fixture.py`（`PYTHONPATH=.:tests /usr/bin/python3 tests/photo_vn_canary_fixture.py` 重新写出三份） |

每份 manifest 的 schema 为 `opv-photo-canary-manifest-v1`，含四块：

1. **`context`** —— 冻结的 resolved execution snapshot（规格 §4）：recipe / category(scarf→accessories) / market(VN) / locale(vi-VN) / destination / reference / product / persona，全部带 id + version（+ 参考图哈希）。
2. **`content_plan`** —— 国家无关规划器绑定 vi-VN 后的批次计划（含 `plan_sha256`）：形态族、A-D 四 Look 的旅行场景与冻结单品、发布文案（`title/cover/caption/cta`）、五页 `slide_texts`。
3. **`account_binding`** —— VN 账号绑定（`OPV_VN_TEST_001`，`status=paused`、`publishing_enabled=false`、3 个候选人物、默认 Market/Locale/RenderPreset）。
4. **`unresolved`** —— **已声明但尚未交付**的绑定清单（本 manifest 不代表可发布）：
   - `asset_set_keys`：仍为 `TH_WOMENSWEAR_CHOICE`（见 §7.1 第 1 条）；
   - `copy_pack_language_review_status`：`DRAFT`（需 `NATIVE_APPROVED`）；
   - `market_pack_status`：`draft`（需 `active`）；
   - `account_status`：`paused`（需 `active`）。

canary 2 额外包含 **`product_application`**（同一围巾写入 `look_a..look_d` 的 `accessories` 槽位，且不改动外层四个服装槽位）与 **`product_qa_contract`**（围巾 10 字段 QA 合同，逐字对应规格 §5.6），因此规格 §8.3「V3 × scarf × VN × STYLE × 有商品 → 同一围巾进入 A-D」在离线层面**有可复核产物**。

**Phase 4 的临时任务验证（规格 §7 Phase 4 第 5 条）**：`VnOfflinePlanningTest` + `VnCanaryManifestTest` 用同一套 `PHOTO_TRAVEL_OUTFIT_V3 + SCARF_V1 + MP_VN_DEFAULT_V1 + LOCALE_VI_VN_V1` 绑定，跑通 STYLE / STYLE+PRODUCT / COMPLETE_LOOK 三种模式；搭配线 `PHOTO_MATCHING_CHOICE_V3` 另在 `VnOfflinePlanningTest` 内跑通 STYLE / COMPLETE_LOOK。

---

## 7. 已知限制与下一阶段阻塞项

### 7.1 本阶段**有意未做**的两项（Phase 3 交付文档曾列为「Phase 4 处理/必须解决」）

规格 §7 Phase 4 的 5 条任务**不含**这两项；且规格 §7 明令「每个 Phase 独立提交；**不得把抽象重构、VN 文案和生产路由混在同一提交**」——两者都属于抽象/运行时改造，混入本提交会违反该条。故本阶段**按现状保留并显式上报**，请裁决归属：

1. **素材集仍绑泰国（`asset_set_keys: ["TH_WOMENSWEAR_CHOICE"]`）**。
   - 不能在本阶段改：`test_photo_travel_v3_equivalence.test_asset_and_variable_contract_match` 断言 **V3 的 `asset_set_keys` 必须等于 V2**（Phase 2 冻结的 TH 等价合同）；改动它会直接打破 Phase 2 的零行为变化证明。
   - 影响面：**真实出图**（Phase 5）。离线 canary 不受影响——preflight 对无市场绑定的 canary recipe 走 `CANARY_MARKET_UNBOUND` 分支，根本不解析素材集。
   - 搭配线 `PHOTO_MATCHING_CHOICE_V3` 已改为声明 `VN_SCARF_CHOICE`（**当前不存在**）。这是**故意**的：未来启用时 preflight 会落到 `NEEDS_ASSET`（响亮失败），而不是静默复用泰国素材集。
   - **建议**：Phase 5 引入「市场/国家无关素材集键」或 market→asset_set 映射，并把 V2/V3 的等价断言改为「仅 TH 绑定时等价」。
2. **目的地词表仍是 TH/V2 的 6 键**（`variables_schema.destination = generic_cool_city/seoul/tokyo/osaka/shanghai/beijing`，Phase 3 §7.1 第 4 条）。
   - 本阶段的目的地权重与 VN 包的目的地标签**严格限定在这 6 个可选键内**（`VnMarketPackTest` 断言权重 ⊆ 词表枚举、权重键集 == VN 包标签键集），因此 `hanoi`/`sa_pa`/`sapporo`/`harbin` 等**当前不可选**。
   - 影响面：规格 §5.1「越南国内…旅行」与 §10「越南国内凉爽旅行 2 篇」、§8.4 canary 3「Sapporo snow」需要该词表扩展（词表在**共享** recipe 里，扩键会同时改变 TH 的声明式 schema）。
   - **建议**：Phase 5 做 catalog-id 化（把 `destination` 的枚举切到 `EAST_ASIA_COOL_V1` 的 `destination_id`），并同步补齐 TH 包标签，避免 TH 侧静默取到空串。

### 7.2 本阶段已知限制（有意为之 / 需知悉）

1. **VN 文案未母语审校**：两个 Copy Pack 与 Locale Pack 均 `DRAFT`。已用正则扫描确认**零泰语泄漏**，但**不等于**母语自然度通过；规格 §9 要求上线前全部 `NATIVE_APPROVED`。
2. **围巾在 `PRODUCT`-only 参考模式下槽位不适用**：共享规划器 `_family_plan` 在 `reference_mode == "PRODUCT"` 时把 `outerwear` 改写为「目标商品外套…」——这是为服装类设计的既有通用行为，对 `main_product_slot=accessories` 的围巾不成立。围巾的商品注入请走规格 §5.4 B 的 **STYLE + product**（本阶段 canary 2 与 `apply_target_product_to_look` 均按此验证）。`MATCHING_CHOICE_V3` 的策略沿用了所复刻骨架（`TH_PICK_YOUR_LOOK_V1`）的 `[STYLE, PRODUCT, COMPLETE_LOOK]`，未收窄；该模式在围巾线上实际不可用，需 Phase 5 做「槽位感知的商品注入」或收窄模式声明。
3. **预留但未实现的键**：`PHOTO_TRAVEL_OUTFIT_V3` 的 `execution_profiles[0].variables` 仍含 `destination/temperature_band/season/style/body_profile/travel_goal` 等枚举默认值，Phase 2 遗留，本阶段未动。
4. **`MATCHING_CHOICE_V3` 的策略文件未做 v2 基础策略继承**：它是完整 v1 policy（484 行），与 `TH_PICK_YOUR_LOOK_V2` 的 `base_policy` 继承写法不同。功能等价，但形态不统一；如需统一可在后续阶段改造。

### 7.3 进入 Phase 5 的前提

- 用户验收本 Phase。
- Phase 5（发布与首批内容）需要先解决 §7.1 的两项（素材集解绑、目的地 catalog-id 化），以及 §7.2 第 1 条（越南语母语审校 → `NATIVE_APPROVED`）。
- 规格 §9 发布门禁还要求：Market Pack 转 `active`、账号转 `active`、至少一个 VN 账号验证 `content_photo_capable=true`、CreatOK/TikTok Content Posting 连接健康、单篇真实发布验证通过。
- 首次真实生图/写入前需按开工前对齐清单 §4 决定是否执行 `seed_reference_data.py --apply`（**本阶段未执行**）。

### 7.4 需要用户裁决的事项

1. **`MATCHING_CHOICE_V2` → `PHOTO_MATCHING_CHOICE_V3` 的命名**：规格 §5.2 写作 `MATCHING_CHOICE_V2`，但该配方全仓零引用（开工前对齐清单 §5 已列为待拍板）。本阶段依据你的选择新建国家无关的 `PHOTO_MATCHING_CHOICE_V3`（落 native_photo 家族，复刻 `PHOTO_TH_PICK_YOUR_LOOK_V3` 骨架 + Phase 2 的 v2 参数化）。已写入该预设的 `disabled_reason` 与 `RECIPE_POLICY_FILES` 注释。如规格作者另有出处，请补充。
2. **§7.1 两项的归属**（Phase 4 补做 / 归入 Phase 5）——理由见上，我倾向归入 Phase 5 以便保持「本 Phase 只含配置与离线产物」。
3. **工作区其它进程的未提交改动**（`image_generator.py` billing 分类、`photo_wig_supply.py` OneRoute 环境键、`test_image_generator_channels.py`、`docs/TH_PHOTO_REFERENCE_MODULE_HANDOFF_20260907.md`、`scripts/run_feishu_scanner_locked.sh`，以及 `packages/remake_video_execution`、`skills/*` 等）**未包含在本次提交**，是否单独提交由你决定。
4. 仍有两个疑似垃圾文件未处理（Phase 2 起遗留）：仓库根目录名为 `-` 的文件、`skills/short-video-auto-publisher/SHORT_VIDEO_AUTO_PUBLISH_DB_PATH`。

---

## 暂停等待验收

Phase 4 结束。按规格 §14「默认只执行一个 Phase，验收后再进入下一 Phase」，此处停下等待验收；下一阶段为 **Phase 5：发布与首批内容**（VN 原生图文能力验证 / 发布路由 / 单篇低风险发布 / 人工检查 / 首批 12 篇）。
