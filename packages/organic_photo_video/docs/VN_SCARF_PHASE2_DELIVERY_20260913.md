# VN 围巾跨市场 · Phase 2 交付：通用旅行模板 V3

- 规格：`docs/VN_SCARF_CROSS_MARKET_IMPLEMENTATION_SPEC_20260913.md` §7 Phase 2
- 日期：2026-09-13
- 基线提交：`d341c1e`（Phase 1 完成态）
- 本阶段边界：**只做"国家无关的旅行模板 + 离线等价证据"**。旧 TH V2 保持 active 与原路由，V3 仅 canary（`status: draft`，未进任何生产 preset）。

---

## 1. 修改文件清单

### 1.1 新增（10 个）

| 文件 | 行数 | 作用 |
|---|---:|---|
| `services/photo_locale.py` | 199 | Locale Pack / Destination Catalog 的纯解析层（`travel_moment_labels`、`destination_labels`、`temperature_labels`、`family_copy`、`travel_copy_tokens`、`snow_scene_allowed`、`destinations_for_country`）。`locale_pack=None` 语义=读旧内联 `*_th` 表。 |
| `services/photo_execution_context.py` | 133 | §4 冻结执行上下文的装配器 `build_execution_context()` + `assert_unchanged()`。 |
| `config/locales/LOCALE_TH_TH_V1.json` | 131 | 首个 Locale Pack（th-TH）。逐字复刻 V2 recipe 的 `label_th`/`destination_labels_th`/`temperature_labels_th` 与 V2 policy 的 `title_th`/`cover_th`/`caption_th`/`looks[].display_label`。 |
| `config/destinations/EAST_ASIA_COOL_V1.json` | 201 | Destination Catalog V1：VN 4 / KR 2 / JP 3 / CN 3 = 12 个目的地，只存语义事实（气候族、温度档、季节、旅行时刻、snow policy），**不存发布语言**。 |
| `config/recipes/PHOTO_TRAVEL_OUTFIT_V3.json` | 404 | 国家无关 recipe v2（从 V2 派生，结构字段逐字一致）。 |
| `config/photo_planning_policies/TRAVEL_OUTFIT_V3.json` | 432 | 通用规划 policy：保留全部服装规则与 family 结构，仅移除 locale 文案。 |
| `config/copy_packs/TH_TRAVEL_OUTFIT_V3.tsv` | 5 | th-TH 文案包（4 条），行内容与 V2 逐字一致，仅 `recipe_id` 改为 V3。规格 §3.2 的 `locale_copy_packs` 已指名此包 id，故必须存在。 |
| `tests/test_photo_locale.py` | 266 | 23 条：旧表↔pack 等价、fail-loud、snow policy、目录语言禁令、模块纯净性。 |
| `tests/test_photo_travel_v3_equivalence.py` | 488 | 30 条：V3 结构等价、TH+womenswear 绑定等价、V2 未回归、执行上下文 fixture 复算。 |
| `tests/fixtures/PHASE2_EXECUTION_CONTEXT_TH_WOMENSWEAR_V3.json` | 45 | 规格 §14.2 要求的 resolved execution context fixture（由测试每次复算比对）。 |

### 1.2 修改（8 个，+477 / −41）

| 文件 | 变更 |
|---|---|
| `domain/contracts.py` | 新增 4 个 schema 常量与校验器：`validate_locale_pack_payload`、`validate_destination_catalog_payload`、`validate_photo_recipe_v2_spec_payload`、`validate_photo_execution_context_payload`；`validate_content_recipe_payload` 增加 v2 分派。 |
| `config/loader.py` | 新增 `LOCALE_DIR`/`DESTINATION_DIR` 与 4 个 loader；`SeedBundle` 增加 `locale_packs`/`destination_catalogs`；`load_content_recipe_file` 支持 v2 的 `locale_copy_packs` 解析。 |
| `scripts/preflight_native_photo.py` | v2 感知：能力改为对照类目注册表校验；无市场绑定的 canary 归入新桶 `canary_market_unbound`，不污染 `needs_asset`。 |
| `services/photo_content_planner.py` | 注册 `PHOTO_TRAVEL_OUTFIT_V3 → TRAVEL_OUTFIT_V3.json`；`_family_plan` / `_travel_template_copy` / `plan_th_choice_batch` 新增 `locale_pack=None` 透传。 |
| `services/photo_copy.py` | `fill_travel_copy_tokens` / `contract_copy_tokens` 的标签改由 locale 层解析，新增 `locale_pack=None`。 |
| `services/photo_reference_vision.py` | `_normalize_travel_plan` 与 `_travel_plan_prompt` 的 moment 标签改由 locale 层解析，新增 `locale_pack=None`。 |
| `tests/test_config_loader.py` | 清单计数 18→19、照片 recipe 13→14、状态例外加 V3(draft)、v2 schema 断言。 |
| `tests/test_photo_request_factory.py` | 清单 13→14、preflight `recipe_count` 13→14 / `profile_count` 29→30、断言 V3 进入 `canary_market_unbound` 而非 `needs_asset`。 |

**未触碰**：`services/image_generator.py`、`tests/test_image_generator_channels.py`（另一进程正在改）、`services/photo_wig_supply.py`、`scripts/run_feishu_scanner_locked.sh`、`docs/TH_PHOTO_REFERENCE_MODULE_HANDOFF_20260907.md`。

---

## 2. 新增/变更合同

新增 4 个合同（均为新增，未放宽任何旧合同）：

1. `opv-photo-recipe-v2`（recipe_spec）：要求 `market_policy`（`MARKET_PACK_REQUIRED`/`MARKET_OPTIONAL`）、`required_category_capabilities`、`locale_copy_packs`、`planning_flow`；**禁止**出现 `markets`/`category_key`/`locale`；`travel_contract` 禁止 `label_th`/`destination_labels_th`/`temperature_labels_th`；`copy_style` 禁止固定 `locale`。
2. `opv-photo-locale-pack-v1`：`labels.{travel_moments,destinations,temperature_bands,generic}` 非空字符串；`fallback_allowed` **必须为 false**（缺语言必须 fail loudly，不得静默回退）；可选 `family_copy`（家族级发布文案）。
3. `opv-photo-destination-catalog-v1`：目的地条目禁含 `label/labels/locale/name_localized/copy`；`snow_scene_policy` 为 `FORBIDDEN/OPTIONAL_NOT_DEFAULT/DEFAULT`，且 `DEFAULT` 仅允许 climate_family 含 `snow` 的目的地。
4. `opv-photo-execution-context-v1`（§4 冻结快照）：recipe/category/market/locale/reference/persona 为必需块，destination/product 可选。

顶层 `schema_version` 增加 `opv-content-recipe-v2`；`opv-content-recipe-v1` 与 `opv-photo-recipe-v1` 的校验路径与报错文案未改动。

---

## 3. 向后兼容说明

- **v1 合同零放宽**：`validate_photo_recipe_spec_payload`、`validate_content_recipe_payload` 的 v1 分支逻辑未改，仅在顶层 schema 判定与 recipe_spec 分派处新增一支。
- **v1 loader 行为不变**：`load_content_recipe_file` 对 v1 recipe 仍走原 `copy_pack_id → TSV` 路径，代码顺序与结果一致。
- **V2 链路标签逐字保留**：`photo_locale` 的 `locale_pack=None` 分支返回的字典与旧内联表逐字相同（由 23 条等价测试与 30 条 V3 测试锁定）。
- **`_family_plan` 一处语义微差**：旧代码 `family["title_th"]` 在字段缺失时抛 `KeyError`，现经 `family_copy()` 返回 `""`。仅影响"policy 漏字段"的错误场景，正常配置下输出完全一致（`test_family_plan_is_identical_under_the_th_binding` 逐 family 比对 `copy`/`looks` 全等）。
- **旧冻结 task/manifest 可继续恢复**：未新增/改名字段，未改状态机。
- **清单计数类基线测试按预期更新**：新增 1 个 shipped recipe 会改变 recipe 总数（18→19、照片 13→14、preflight 13→14 / 29→30），已在测试内注明日期与原因。

---

## 4. 执行过的测试命令与完整结果

```bash
cd /Users/likeu3/.openclaw/workspace/packages/organic_photo_video

# 新增测试
PYTHONPATH=.:tests /usr/bin/python3 -m unittest tests.test_photo_locale
PYTHONPATH=.:tests /usr/bin/python3 -m unittest tests.test_photo_travel_v3_equivalence

# 最高验证（§14.2）
PYTHONPATH=.:tests /usr/bin/python3 -m unittest discover -s tests -p 'test_photo_*.py'
PYTHONPATH=.:tests /usr/bin/python3 -m unittest discover -s tests -p 'test_*.py'
git diff --check
```

| 命令 | 结果 |
|---|---|
| `tests.test_photo_locale` | **23 OK** |
| `tests.test_photo_travel_v3_equivalence` | **30 OK** |
| photo 域全量 | **457 OK**（基线 404 + 53 新增） |
| 全量回归 | **1074 OK**（基线 1021 + 53 新增） |
| `git diff --check` | 无输出（无空白错误） |

### 4.1 隔离验证（证明 V2 零行为变化）

从 `git archive HEAD` 导出干净树，只叠加本阶段改动（**不含**并发进程的 `image_generator.py` / `test_image_generator_channels.py`），并挂入机器相关依赖（`.env.local`、asset_sets、light_tryon.sqlite3）：

| 树 | 结果 |
|---|---|
| 纯 HEAD（`d341c1e`） | **1020 OK ×2** |
| HEAD + 仅本阶段改动 | **1068 OK ×3** |

**1068 − 1020 = 48 = 本阶段新增测试数（23+30 中的前 48 条；fixture 用例在其后补加，见下）。**
差值全部来自新增测试，既有 1020 条测试一条未变 → V2 链路行为未变。

> 说明：上表隔离树建于 fixture 用例补加之前。补加后工作区实测为全量 1074 / photo 域 457，与"HEAD 1020 + 53"一致。

### 4.2 等价证据要点（最强的几条）

- `test_family_plan_is_identical_under_the_th_binding`：对 8 个 family 逐个执行 `_family_plan`，V2(policy内联泰语) 与 V3(+TH Locale Pack) 的 `copy`、`looks`、`angle_zh`、`scene_zh`、`palette_zh`、`background_*`、`difference_axes` **完全相等**。
- `test_copy_tokens_resolve_identically_without_th_tables`：V3 合同已无任何 `_th` 表，但 `{destination}`/`{temperature}` 解析结果与 V2 逐字相同。
- `test_v3_copy_pack_reproduces_the_th_templates`：V3 的 4 条 th-TH 文案模板与 V2 的 4 条逐字相同。
- `test_th_pack_covers_every_v3_moment_and_family`：7 个 moment、8 个 family 的标题/封面/caption/4 个 look 标签在 pack 中全部非空。
- `test_cool_cities_never_default_to_snow`：TOKYO/SHANGHAI 即使显式请求也不生成雪景；SAPPORO/HARBIN/GANGWON 默认可雪。

---

## 5. 未执行的真实调用

本阶段**未**执行，且未以任何方式触发：

- 真实生图 / 视觉 canary：§8.4 的 6 张对照图（VN+scarf、Seoul、Sapporo snow、COMPLETE_LOOK、TH+scarf、TH+womenswear-V3 对 V2）**未生成**。验收依赖人工看图，需真实模型与素材。
- 真实飞书写入、任务创建、发布：未做。
- TikTok / CreatOK 发布能力验证：未做（本阶段不涉及）。
- 真实人物包解析：fixture 中 persona 为占位标识，未绑定任何真实人物包。
- 真实商品图包快照：fixture 为 STYLE 无商品；同商品跨市场快照对照属 Phase 3。

---

## 6. canary 产物路径与 manifest

| 产物 | 路径 |
|---|---|
| canary recipe | `config/recipes/PHOTO_TRAVEL_OUTFIT_V3.json`（`status: draft`，未进 `feishu_production_presets.json`） |
| canary policy | `config/photo_planning_policies/TRAVEL_OUTFIT_V3.json` |
| Locale Pack | `config/locales/LOCALE_TH_TH_V1.json` |
| Destination Catalog | `config/destinations/EAST_ASIA_COOL_V1.json` |
| th-TH 文案包 | `config/copy_packs/TH_TRAVEL_OUTFIT_V3.tsv` |
| **resolved execution context fixture** | `tests/fixtures/PHASE2_EXECUTION_CONTEXT_TH_WOMENSWEAR_V3.json` |

fixture 内容（V3 + TH + womenswear + LOCALE_TH_TH_V1 + SEOUL_WINTER + STYLE）：`recipe{id,version,planning_flow}` / `category{womenswear,WOMENSWEAR_V1,v1,outerwear}` / `market{TH,MP_TH_DEFAULT_V1,v1}` / `locale{th-TH,LOCALE_TH_TH_V1,v1,TH_TRAVEL_OUTFIT_V3}` / `destination{SEOUL_WINTER,KR,Seoul}` / `reference{STYLE,fingerprint,1 hash}` / `persona{占位}`。persona 标识与哈希均为 fixture 占位值，已在文件内 `_fixture_note` 注明。

V2 生产路由未变更：`PHOTO_TH_TRAVEL_OUTFIT_V2` 仍 active，preset 仍指向它。

---

## 7. 已知限制与下一阶段阻塞项

### 7.1 本阶段已知限制（有意为之）

1. **V3 未接入生产路由**。`services/feishu_workflow.py` / `photo_request_factory.py` / `photo_package.py` 中 `PHOTO_TH_TRAVEL` 前缀判断**未改**——规格 §7 Phase 2 第 5 条明确要求"旧 V2 保持原路由"。
2. **vision 的 locale 参数尚未被生产调用方透传**。`_normalize_travel_plan` / `_travel_plan_prompt` 已改为经 locale 层读标签并接受 `locale_pack`，但上游未传（`None` → 与 V2 完全一致）。真正的绑定在 Phase 4 解出 execution context 后完成。
3. **`variables_schema.destination` 仍沿用 V2 的键命名**（`generic_cool_city`/`seoul`/`tokyo`…），尚未改为 Destination Catalog 的 `destination_id` 命名空间。当前 catalog 已通过 `snow_scene_allowed` / `destinations_for_country` 与 execution context fixture 生效，但"目的地枚举改为 catalog id"属 Phase 4。
4. **`asset_set_keys: ["TH_WOMENSWEAR_CHOICE"]` 仍绑定泰国素材集**。这是 VN 落地的**硬阻塞**：Phase 4 必须提供 VN 素材集或市场无关素材集，否则 VN 无法出图。
5. **未做视觉 canary**（见 §5）。"生成端完成"≠"可发布"。
6. **`_family_plan` 缺字段由 KeyError 变为空串**（见 §3）。

### 7.2 进入 Phase 3 的前提

- 用户验收本 Phase。
- Phase 3（SCARF 类目）需要：`config/categories/SCARF_V1.json`、`SCARF_V1` adapter（含 `accessories` 商品槽位）、`scarf` 类目别名、`product_owner` 语义扩展（目前 `product_own_category_by_slot={"outerwear":("outerwear",)}`，需新增 `accessories` 归属判定）。Phase 2 已把类目侧接口（`apply_target_product_to_look`/`product_owns_slot`/`role_priority_for_slot`）备好，但**Phase 3 不在本阶段范围内**。
- Phase 3 起还需输出 `TH + scarf` 与 `VN + scarf` 的同商品图包快照对照（规格 §14.2）。

### 7.3 需要用户裁决的事项

- 工作区仍有**另一进程**在改 `services/image_generator.py` 与 `tests/test_image_generator_channels.py`（本阶段最后一次写入 12:06，期间无进程在跑）。本阶段提交**只包含上表 18 个文件**，不包含这两个；如需一并入库，请另行确认。
- 另有两个疑似垃圾文件（根目录 `-`、`skills/short-video-auto-publisher/SHORT_VIDEO_AUTO_PUBLISH_DB_PATH`）仍未处理。
