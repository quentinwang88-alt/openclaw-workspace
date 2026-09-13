# VN 围巾跨市场 · Phase 3 交付：SCARF 类目适配器

- 规格：`docs/VN_SCARF_CROSS_MARKET_IMPLEMENTATION_SPEC_20260913.md` §7 Phase 3
- 日期：2026-09-13
- 基线提交：`2201e48`（Phase 2 完成态：通用旅行模板 V3）
- 本阶段边界：**只做"围巾类目 + accessories 槽位 + 在线解码证据"**。不新增市场、不写越南语文案、不改生产路由；V3 仍为 `status: draft` canary，未进任何 preset。

---

## 1. 修改文件清单

### 1.1 新增（2 个）

| 文件 | 行数 | 作用 |
|---|---:|---|
| `config/categories/SCARF_V1.json` | 45 | 首个 `opv-category-profile-v2` 类目适配器配置：`category_key=scarf`、`main_product_slot=accessories`、`capabilities=[wearable_styling, travel_look, product_embedding]`、`product_label_zh=目标围巾`、5 个身份属性、3 条按角色分派的参考图优先级。**不含** `markets`/`locale`/文案。 |
| `tests/test_photo_scarf_category.py` | 469 | 35 条：配置↔适配器交叉合同、槽位解析、accessories 写入、参考图优先级、围巾 QA 合同、别名、TH+SCARF 解耦、womenswear 未受影响。 |

### 1.2 修改（9 个，+333 / −54）

| 文件 | 变更 |
|---|---|
| `domain/contracts.py` | 新增 `CATEGORY_PROFILE_V2_SCHEMA_VERSION`、`PHOTO_OUTFIT_SLOTS`、`PRODUCT_REFERENCE_ROLES`、`PRODUCT_REFERENCE_SLOTS`、`CATEGORY_FORBIDDEN_KEYS` 与 `validate_photo_category_profile_v2_payload`。v1 校验器一字未改。 |
| `config/loader.py` | 新增 `CATEGORY_PROFILE_V2_SCHEMA`；`validate_category_profile` 按 `schema_version` 分派 → v2 走 `domain.contracts`，其余走原 v1 规则。 |
| `services/photo_category_registry.py` | 新增 `SCARF_V1` 适配器、`SCARF_QA_FIELDS`、`SCARF_QA_RULES` 并登记进 `_ADAPTERS`；`PhotoCategoryAdapter` 新增 5 个可选字段（`must_keep`/`forbidden`/`qa_fields`/`qa_rules`/`presence_lock_lines`），默认值=既有 womenswear 文案；`build_product_qa_contract` 改为适配器驱动并输出 `qa_fields`/`qa_rules`。 |
| `services/photo_style_reference_supply.py` | `_product_targets_slot` 改为按商品类目取适配器（未认领类目回退 `WOMENSWEAR_V1`）；新增 `_product_adapter`/`_target_slot_override`；`outfit_state` 由内联字面量提为变量，并在适配器 `main_product_slot` 非 `outerwear` 时写入该槽位。 |
| `services/product_reference_resolver.py` | `select_product_references_for_slot` 改为按商品类目取适配器（缺类目回退 `WOMENSWEAR_V1`），即 Phase 1 留下的"等第二个类目上线再改"TODO。 |
| `services/operation_product_pack_source.py` | `PRODUCT_CATEGORY_ALIASES` 新增 `围巾/披肩/丝巾/脖套 → scarf`；同步机制未动。 |
| `services/image_generator.py` | 商品显示名/适配器一次解析；新增适配器驱动的 `presence_lock_lines` 注入（womenswear 为空 → 提示词不变）。 |
| `tests/test_config_loader.py` | 类目计数 2→3、集合加 `scarf`；`test_photo_categories_are_config_only_and_active` 按 v1/v2 两种 profile 形态分别断言。 |
| `tests/test_photo_category_registry.py` | `registered_category_keys` 断言加 `scarf`；`test_unknown_category_raises` 的守卫 token 从 `scarf` 换成真正未注册的 `shoes`。 |

**未触碰 / 未提交**：`tests/test_image_generator_channels.py`（另一进程的通道改动，本阶段不含）；`scripts/preflight_native_photo.py`、`services/photo_locale.py`、`services/photo_copy.py`、`services/photo_content_planner.py`、`services/photo_reference_vision.py`（Phase 2 成果，本阶段未改）。

> **`image_generator.py` 的 hunk 隔离**：该文件工作区同时含另一进程的 `billing` 失败分类改动（`FATAL_ERROR_KINDS` 增 `billing`、`_ERROR_KIND_MARKERS` 增 billing 条目）。本阶段提交**只含围巾相关的 3 个 hunk**，billing 改动保留在工作区未暂存（见 §7.3）。

---

## 2. 新增/变更合同

新增 1 个合同 + 1 处合同扩展，**未放宽任何旧合同**：

1. **`opv-category-profile-v2`（新增）**：要求 `category_key`/`category_name`/`status`/`main_product_slot`/`product_label_zh` 为非空串；`capabilities`/`accepted_product_categories`/`required_product_roles`/`identity_attributes` 为非空字符串列表；`product_reference_priority_by_slot` 为"故事板角色 → 非空参考角色列表"的映射，且角色名只能取自 `PHOTO_REFERENCE_SLOTS` / `PRODUCT_REFERENCE_ROLES`；`main_product_slot` 只能取自 `PHOTO_OUTFIT_SLOTS`（8 个渲染槽位）；**禁止**出现 `markets`/`locale`/`labels`/`label_th`/`copy`（类目不得自绑市场或发布语言）。
2. **`opv-photo-product-qa-v1`（扩展）**：新增 `qa_fields`、`qa_rules` 两个键，且 `must_keep`/`forbidden` 从硬编码改为适配器声明。未声明 `qa_fields` 的类目（womenswear）得到空列表，键集与语义向后兼容。该合同目前**尚无生产消费者**（Phase 1 起即如此），本阶段只把它做成类目可声明、可测试的形态。

`opv-category-profile-v1` 与其余所有 v1/v2 合同的校验路径、报错文案、可接受输入均未改动。

---

## 3. 向后兼容说明

- **v1 类目配置零影响**：`WOMENSWEAR_V1.json` / `WIG_V1.json` 的字节、校验路径与结果不变；`validate_category_profile` 仅在顶部多一次 `schema_version` 等值判断。
- **womenswear 适配器零影响**：`must_keep`/`forbidden` 的默认值逐字等于原硬编码列表；`qa_fields`/`qa_rules`/`presence_lock_lines` 默认空元组。`test_womenswear_contract_is_unchanged` 与 `test_none_of_the_adapter_declared_extras_leak_into_womenswear` 锁定这一点。
- **未认领类目回退**：`product_reference_resolver` 与 `photo_style_reference_supply` 对 `wig`/空类目回退 `WOMENSWEAR_V1`，而非改用共享默认序 —— 只有回退才能保证这些商品的取图顺序与提示词与迁移前逐字一致（Phase 1 注释里点名的风险）。
- **`image_generator` 提示词零影响**：womenswear/wig 的合成提示词与 Phase 2 时逐字相同（`test_scarf_presence_lines_come_only_from_the_adapter` 用"停用适配器查找"的方式隔离出适配器的唯一贡献量）。
- **清单计数类基线按预期更新**：类目数 2→3（`test_config_loader.py`），已在测试内注明日期与原因。

---

## 4. 执行过的测试命令与完整结果

```bash
cd /Users/likeu3/.openclaw/workspace/packages/organic_photo_video

PYTHONPATH=.:tests /usr/bin/python3 -m unittest tests.test_photo_scarf_category
PYTHONPATH=.:tests /usr/bin/python3 -m unittest tests.test_photo_category_registry tests.test_config_loader tests.test_photo_request_factory
# 规格 §14.2 点名的 10 个照域模块
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
| `tests.test_photo_scarf_category` | **35 OK** |
| `tests.test_photo_category_registry` + `test_config_loader` + `test_photo_request_factory` | **74 OK** |
| 规格点名的 10 个照域模块 | **188 OK** |
| photo 域全量 | **492 OK**（Phase 2 后 457 + 35 新增） |
| 全量回归（工作区） | **1109 OK**（Phase 2 后 1074 + 35 新增） |
| `scripts/preflight_native_photo.py` | `exit 0`，`errors: []`，`recipes: 14 / profiles: 30`，`canary_market_unbound: ["PHOTO_TRAVEL_OUTFIT_V3"]`（与 Phase 2 完全一致） |
| `git diff --check` | 无输出 |

### 4.1 隔离验证（两棵干净树）

从 `git archive` 导出，只叠加本阶段改动（**不含**并发进程的 `image_generator` billing hunk 与 `test_image_generator_channels.py`），并挂入机器相关依赖（`.env.local`、asset_sets、light_tryon.sqlite3）：

| 树 | 结果 |
|---|---|
| 纯 HEAD（`2201e48`，Phase 2 完成态） | **1073 OK** |
| HEAD + 仅本阶段改动（`git write-tree` = `70be7d4`） | **1108 OK** |

**1108 − 1073 = 35 = 本阶段新增测试数。** 既有 1073 条一条未变。

### 4.2 逐面字节比对的等价证据（最强）

在两棵树里跑同一支探针，把 TH V2 会看到的**全部**接口面序列化后逐行比对：商品显示名（11 个 token）、类目→槽位映射（11 个 token）、6 个角色的参考图优先级、`product_owns_slot` 的 2×4 组合、`apply_target_product_to_look` 的 10 个类目、STYLE 供应侧类目判定（12 个输入）、`select_product_references_for_slot` 的 6 个角色、`failed_roles_from_travel_qa` 投影，以及 3 段合成提示词（含 STYLE 分支）。

**结果：全文差异只有 1 处新增行** —— 商品类目为 `scarf` 时 `apply_target_product_to_look` 多写出 `"accessories": "指定商品围巾（以商品参考图为准）"`。

```
@@ -99,6 +99,7 @@
    "top_inner": "白衬衫"
   },
   "9": {
+   "accessories": "指定商品围巾（以商品参考图为准）",
    "bottom": "直筒牛仔",
```

即：womenswear / wig / 空类目 / 大小写与空格变体的所有输出、所有参考图顺序、所有提示词**逐字未变**；唯一的 delta 就是 Phase 3 要新增的围巾槽位行为。

### 4.3 新测试的金丝雀自检（证明测试"咬得住"）

对适配器逐项注入变异，确认对应断言会失败：

| 变异 | 是否被捕获 |
|---|---|
| `main_product_slot` 改回 `outerwear` | ✅ |
| 槽位映射改为 `("outerwear","围巾")` | ✅ |
| `hero` 参考序改回含 back 的旧序 | ✅ |
| `qa_fields` 清空 | ✅ |
| `presence_lock_lines` 清空 | ✅ |
| 身份属性换回服装词（`pattern`/`collar` 等） | ✅ |
| `capabilities` 收窄 | ✅ |

---

## 5. 未执行的真实调用

本阶段**未**执行，且未以任何方式触发：

- 真实生图 / 视觉 QA canary：§8.4 的对照图 **未生成**。
- 真实飞书写入、任务创建、RDS 播种：未做。
- 真实商品图包同步（`operation_product_pack_source` 的别名只加映射，未拉取数据）。
- 真实人物包解析、发布（TikTok / CreatOK）：未做。
- TH+SCARF 验证为**离线绑定**：只在测试内用 `build_execution_context` 装配 TH 市场上下文并比对类目层输出，未出图。

---

## 6. canary 产物路径与 manifest

| 产物 | 路径 |
|---|---|
| SCARF 类目适配器配置 | `config/categories/SCARF_V1.json`（`opv-category-profile-v2`，`status: active`） |
| SCARF 适配器实现 | `services/photo_category_registry.py` → `SCARF_V1` |
| 围巾 QA 合同 | `SCARF_QA_FIELDS`（10 字段，含 `product_present`/`pattern_family_matches`/`edge_or_fringe_matches`/`length_volume_plausible`/`face_unobscured`）+ `SCARF_QA_RULES` |
| 别名 | `services/operation_product_pack_source.py`：`围巾/披肩/丝巾/脖套 → scarf` |

**Phase 3 的临时任务验证（规格 §7 Phase 3 第 4 条）**：`ThScarfDecouplingTest` 用同一 `PHOTO_TRAVEL_OUTFIT_V3 + SCARF_V1 + SEOUL_WINTER + STYLE` 绑定，分别装配 TH（`MP_TH_DEFAULT_V1` / `th-TH`）与 VN（`MP_VN_DEFAULT_V1` / `vi-VN`）上下文，断言：

- 两个上下文的差异键集 **恰为** `{market, locale}`（recipe / category / destination / reference / product / persona 全等）；
- 两市场下 `apply_target_product_to_look` 写出的槽位文案与 `build_product_qa_contract` 输出**完全相同**；
- `SCARF_V1` 的 dataclass 字段中不存在任何 `markets`/`country`/`locale` 字段；
- V3 的 `required_category_capabilities` 被 `SCARF_V1.capabilities` 完整覆盖（即 preflight 的能力门禁通过）。

→ 类目层不含国家信息，**类目与国家解耦**已由测试证明。

生产路由未变更：`PHOTO_TRAVEL_OUTFIT_V3` 仍 `draft`，仍不在 `config/feishu_production_presets.json` 中；TH V2 仍走原路由。

---

## 7. 已知限制与下一阶段阻塞项

### 7.1 本阶段已知限制（有意为之）

1. **围巾 QA 目前只是"可声明合同"，尚未接入运行时质检修环**。`build_product_qa_contract` 已能输出 10 个围巾 QA 字段与确定性规则，但 `photo_travel_qa.normalize_travel_qa` 的逐页输出仍只有通用 `product_matches`，`_travel_qa_prompt` 的逐页 JSON 也未要求这 10 个字段。要真正生效需同时改提示词与规范化器（含 `TRAVEL_QA_SCHEMA`），属 Phase 4 的"离线 canary"工作。
2. **`SCARF_V1` 声明 `status: active`**（规格 §3.3 原文如此），但类目可被生产使用的前提是"有 recipe 通过 preset 绑定它"——目前没有任何 preset 引用 `scarf`，故实际不可达。
3. **V3 仍为 canary**（`draft`，无市场绑定）。
4. **`variables_schema.destination` 仍用 V2 键命名**，未切到 catalog id。Phase 2 遗留，Phase 4 处理。
5. **`asset_set_keys: ["TH_WOMENSWEAR_CHOICE"]` 仍绑定泰国素材集** —— VN 出图的硬阻塞，Phase 4 必须解决。
6. **vision 的 `locale_pack` 生产调用方未透传**。Phase 2 遗留。

### 7.2 进入 Phase 4 的前提

- 用户验收本 Phase。
- Phase 4（VN 激活配置）需要：`config/market_packs/MP_VN_DEFAULT_v1.json`、`config/locales/LOCALE_VI_VN_V1.json`、`config/copy_packs/VN_TRAVEL_OUTFIT_V1.tsv` 与 `VN_SCARF_MATCHING_V1.tsv`、目的地权重、2-3 个人物包验收、disabled VN 预设，以及三类离线 canary（STYLE / STYLE+PRODUCT / COMPLETE_LOOK）。
- Phase 4 还需**解绑泰国素材集**（§7.1 第 5 条），否则 VN 无法出图。
- 规格 §5.2 点名的 `MATCHING_CHOICE_V2` 在全仓**零引用**（Phase 0 已记录），Phase 4 前需确认它是否真实存在或改用现有配方。

### 7.3 需要用户裁决的事项

- **`image_generator.py` 的 billing 改动归属**：另一进程给 `FATAL_ERROR_KINDS` 与 `_ERROR_KIND_MARKERS` 增加了 `billing` 分类（把 `403 INSUFFICIENT_BALANCE` 从 auth 中拆出）。该改动仍在工作区未暂存，**未包含在本次提交**。是否单独提交由用户决定。
- **`tests/test_image_generator_channels.py`** 同为另一进程的改动，未提交。
- 另有两个疑似垃圾文件（根目录 `-`、`skills/short-video-auto-publisher/SHORT_VIDEO_AUTO_PUBLISH_DB_PATH`）仍未处理（Phase 2 起遗留）。

---

## 暂停等待验收

Phase 3 结束。按规格 §14「默认只执行一个 Phase，验收后再进入下一 Phase」，此处停下等待验收；下一阶段为 **Phase 4：VN 激活配置**（Market Pack / 越南语 Locale+Copy Pack / 目的地权重 / 人物包验收 / disabled VN 预设 / 三类离线 canary）。
