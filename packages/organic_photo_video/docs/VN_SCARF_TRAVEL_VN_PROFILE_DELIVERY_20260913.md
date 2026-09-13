# VN 围巾跨市场｜旅行线 VN execution profile 交付（Phase 5 首项）

- **日期**：2026-09-13
- **范围**：`packages/organic_photo_video/`
- **前置**：`495872e`（搭配线真实生图端到端验收）；`df6d651`（Review 3×P0 整改）
- **本轮授权**：用户对我上一轮结尾提议「推进旅行线的 VN execution profile」回复「好的，继续」
- **结论**：旅行线 `PHOTO_TRAVEL_OUTFIT_V3` 新增**第二个 execution profile**，原生绑定
  `VN_SCARF_CHOICE`；VN 旅行请求不再 `NEEDS_ASSET`。TH 行为**零变化**（全量 1192 → 1200，
  增量恰为新增测试）。

---

## 1. 问题：旅行线为何必然 `NEEDS_ASSET`

`PHOTO_TRAVEL_OUTFIT_V3` 是 Phase 2 从 TH V2 派生的**国家无关**配方：`locale_copy_packs`
同时挂了 `th-TH` 与 `vi-VN`，所以**文案**能按市场取语言。但它的 `execution_profiles[0]`
的 `asset_set_keys` 仍是 `["TH_WOMENSWEAR_CHOICE"]`（TH 素材集）。

而 `AssetSetService.candidates()` 按 `category_key` + `market` **双重过滤**：TH 素材集是
`womenswear/TH`，对 `scarf/VN` 请求恒为 0 候选 → 工厂抛 `NEEDS_ASSET`。

好消息（上一轮已证）：这是**响亮失败**，不会静默复用泰国素材。坏消息：VN 旅行线**根本出不了图**。

---

## 2. 设计：为什么是「追加第二个 profile」而不是改 `profile[0]`

`tests/test_photo_travel_v3_equivalence` 把 **Phase 2 的零行为变化**钉成了合同：

- `test_execution_profile_variables_match` 断言 `v3["execution_profiles"][0]` 的
  `profile_id` / `variables` / **`asset_set_keys`** 与 V2 `[0]` **逐字相等**。

所以**不能**改 `profile[0].asset_set_keys`（会破合同），也**不能**给它加第二个键
（同上）。可选方案有三：

| 方案 | 结果 |
|---|---|
| 改 `profile[0]` 的 asset_set_keys | ✗ 破 Phase 2 等价合同 |
| 新增 per-market 字段（如 `asset_set_keys_by_market`） | 可行，但要动 recipe-v2 合同 + 工厂，新增配置概念 |
| **追加 `execution_profiles[1]`** | ✓ **等价测试只比 `[0]`**（见 §2.1），不动合同、不动工厂、不动 contracts |

选第三条。它也不是权宜之计——**多 profile 本就是本仓的既有范式**：`PHOTO_TH_TRAVEL_OUTFIT_V1`
有 3 个 profile（seoul_cafe / tokyo_walk / osaka_airport），`PHOTO_MX_*` 系列亦然。
profile 就是「同一配方的不同执行变体」，VN 变体正是其中一种。

### 2.1 为什么追加到 `[1]` 不会影响 TH

工厂 `PhotoRequestFactory.build_batch()`（`services/photo_request_factory.py:268`）**遍历全部
profile**，为每个 profile 解析它声明能服务的候选，再按 rank 择优。因此：

- **TH 请求**：`profile[0]`（TH 键）命中 TH 素材集；`profile[1]`（VN 键）在 TH 市场下 0 候选，
  不参与 → TH 仍只解析 `TH_WOMENSWEAR_CHOICE`。
- **VN 请求**：`profile[0]` 0 候选，`profile[1]` 命中 `VN_SCARF_CHOICE` → 原生解析，**无需 override**。

`profile[1]` 的 `variables` 与 `profile[0]` **完全一致**，所以旅行计划（destination / 温度档 /
季节 / 风格）不发生任何漂移——两个 profile 的差别**只有素材绑定**。这一点被新测试
`test_profiles_share_variables_so_the_travel_plan_is_identical` 与
`test_the_two_profiles_share_the_same_vietnamese_copy` 钉住（后者是防漂移哨兵）。

---

## 3. 改动清单

| 文件 | 变更 | 说明 |
|---|---|---|
| `config/recipes/PHOTO_TRAVEL_OUTFIT_V3.json` | +16 行 | **纯追加** `execution_profiles[1]`（`travel_scene_four_looks_vn`，`asset_set_keys=["VN_SCARF_CHOICE"]`）；`profile[0]` 一个字节未动 |
| `config/copy_packs/VN_TRAVEL_OUTFIT_V1.tsv` | +4 行 | 新 profile 的 4 条 `vi-VN` 文案 |
| `config/loader.py` | +11/−1 | **放行「profile 只服务部分 locale」**：某 locale 无该 profile 行时跳过而非报错；默认 locale 取第一个**有文案**的（见 §3.1） |
| `tests/test_photo_travel_vn_profile.py` | 新增 | 7 条：两个 profile 的绑定/变量/文案一致性 + VN 原生解析 + TH 不越界 + 跨市场不泄漏 |
| `tests/test_photo_v3_production_path.py` | +28/−6 | 旅行线用例**去掉 override**（此前靠 override 才冻结），改证「原生解析」 |
| `tests/test_photo_request_factory.py` | +5/−1 | `profile_count` 计数断言 31 → 32（如实 +1 个 profile） |

### 3.1 loader 改动的必要性

v2 配方加载时，`load_content_recipe_file` 会为**每个** profile、按**每个**声明 locale 去
copy pack 取行，且 `load_photo_copy_pack` 在「该 profile 一行都没有」时**抛错**
（`loader.py:375`）。新 profile 只服务 `vi-VN`，于是取 `th-TH` 包时会抛错，整份配方加载失败。

改法（最小且可证零影响）：

1. `load_photo_copy_pack(..., allow_empty=False)` 新增可选参数，默认**保持原抛错语义**；
2. v2 循环里传 `allow_empty=True`，该 locale 无行则 `continue`（不写入 `copy_variants_by_locale`）；
3. `default_locale = sorted(variants_by_locale)[0]` 因此天然落在**有文案**的 locale 上。

**零行为变化**：既有配方里，每个 profile 在其声明的**每个** locale 都有行（`TRAVEL_V3.profile[0]`
有 `th-TH`+`vi-VN`；`MATCHING_V3.profile[0]` 只有 `vi-VN`）→ 新的 `continue` 分支**永不触发**，
`default_locale` 与改前一致。`test_resolved_recipe_carries_one_copy_set_per_locale` 原样通过。

---

## 4. 关键发现：RDS 里的配方仍是**旧版**，激活需重新播种

干跑时发现：工厂读的是 **RDS** 的配方，而 RDS 里 `PHOTO_TRAVEL_OUTFIT_V3` 还是 Phase 4
（上一轮）播种的**旧版**——只含 `profile[0]`。也就是说：

> **本轮改的是 `config/`；要让 VN 旅行线真正在 RDS 生产链上生效，必须重新播种配方**
> （`scripts/seed_reference_data.py --apply`，配方仍为 `draft`，属激活步骤、非本轮范围）。

这不是缺陷，而是本仓一贯的「config 骨架 ≠ 运行时可路由」。已写入 §7 后续项。

---

## 5. 验证证据

### 5.1 回归（零行为变化）

用「备份 → 回退到 HEAD → 跑 → 还原（md5 全等）」的方式在同一工作区测两遍：

| 树 | 结果 |
|---|---|
| HEAD（本轮改动全部回退） | **1192 OK** |
| 工作区（含本轮改动） | **1200 OK** |

**增量 = 8 = 本轮新增测试数**（7 条新模块 + 1 条新用例）→ 既有行为逐条不变。

> 全量命令：`PYTHONPATH="$PWD:$PWD/tests" /usr/bin/python3 -m unittest <87 个 tests.test_* 模块>`
> （本仓无 pytest；必须 `/usr/bin/python3`，托管 3.13 缺 `pymysql`/`PIL`）

### 5.2 preflight

`python3 -m scripts.preflight_native_photo` → `exit 0`、`errors: []`、`external_writes: 0`、
`recipe_count: 15`、`profile_count: 32`（原 31，+1 为新增 profile）、
`canary_market_unbound: ["PHOTO_MATCHING_CHOICE_V3","PHOTO_TRAVEL_OUTFIT_V3"]`（不变）。

### 5.3 真实 RDS 干跑（`tmp/vn_real_gen/dryrun_travel_vn_profile.py`）

配方取 config（含新 profile）、素材集取**真实 RDS**（`ASSET_VN_SCARF_CHOICE_V1`），走真 `build_batch`：

| 线 | 选中 profile | asset_set_key | asset_set_id | 泰文 |
|---|---|---|---|---|
| 围巾旅行 | `travel_scene_four_looks_vn` | `VN_SCARF_CHOICE` | `ASSET_VN_SCARF_CHOICE_V1` | 无 |
| 围巾搭配 | `scarf_four_looks` | `VN_SCARF_CHOICE` | `ASSET_VN_SCARF_CHOICE_V1` | 无 |

旅行线冻结标题：`Diện gì để chụp ảnh ở Thành phố se lạnh?`（越南语，`{destination}` 已由
Locale Pack 解析为「Thành phố se lạnh」）。

---

## 6. 与既有合同的关系

- `test_photo_travel_v3_equivalence`（Phase 2 TH 等价）**未改动即通过**——`profile[0]` 逐字未变。
- `test_photo_vn_activation` 原样通过。
- 唯一改动的既有断言是 `profile_count`（31 → 32），属**如实计数**，已在注释标注本轮来源。

---

## 7. 仍未做 / 后续

| 项 | 说明 |
|---|---|
| **重新播种配方到 RDS** | 激活前置：`seed_reference_data.py --apply` 让 RDS 拿到 `profile[1]`（仍 `draft`、预设仍 `disabled`） |
| **旅行线真实生图端到端** | 本轮只证到「冻结解析」（干跑）；真跑需按 `opv-real-gen-e2e-verification` 的分层流程 + 武装闸门（付费、写生产数据，需单独授权） |
| 母语终审 | VN 文案仍 `DRAFT` |
| VN 本地人设 / TikTok 发布能力 | 仍复用泰语库人设；发布能力未确认 |
| P1-4 / P1-5 / P2-7 | Execution Context / Destination Catalog 未进运行时；缺 `披肩围巾` 别名 |

---

## 8. 文件清单

**入库（本次独立提交）**
- `config/recipes/PHOTO_TRAVEL_OUTFIT_V3.json`（M）
- `config/copy_packs/VN_TRAVEL_OUTFIT_V1.tsv`（M）
- `config/loader.py`（M）
- `tests/test_photo_travel_vn_profile.py`（新增）
- `tests/test_photo_v3_production_path.py`（M）
- `tests/test_photo_request_factory.py`（M）
- 本交付文档（新增）

**不入库（gitignore）**
- `tmp/vn_real_gen/dryrun_travel_vn_profile.py`（干跑脚本）
