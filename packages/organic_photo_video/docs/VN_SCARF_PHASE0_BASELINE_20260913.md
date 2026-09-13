# VN 围巾跨市场复用 — Phase 0 基线交付

日期：2026-09-13
状态：已完成，待验收
对应规格：`docs/VN_SCARF_CROSS_MARKET_IMPLEMENTATION_SPEC_20260913.md` §7 Phase 0
适用项目：`packages/organic_photo_video`

---

## 0. 本阶段范围

按规格 §14「默认只执行一个 Phase」的口径，本轮**只做 Phase 0：基线与防回归**。

明确未做（留给后续 Phase，且均需单独门禁）：

- 未新增 `PhotoReferenceContext`、Category Adapter、Recipe v2、Locale Pack、Market Pack、Destination Catalog。
- 未改动任何既有业务代码、配置或飞书 schema。
- 未真实生图、未调用外部视觉模型、未写 RDS、未发布、未修改飞书生产表。
- 未 `git reset` / `checkout` / `stash` / 清理任何未跟踪文件。

Phase 0 的两项交付物（规格 §7）：**基线测试记录** + **TH V2 关键输出的 golden/contract 测试**。

---

## 1. 工作区快照（逐字保留，未做任何回退）

| 项 | 值 |
|---|---|
| HEAD | `6b6abac02ba143ae9b037ad48f6a06e88032092f` |
| 分支 | `main` |
| dirty 条目 | 43 modified + 39 untracked = **82** |
| 快照文件 | `tmp/phase0_vn_scarf/git_status_short.txt` |

脏工作区来自上一轮 TH 热转 / 旅行体感改造（`PHOTO_TH_THERMAL_TRANSITION_V1`、`TRAVEL_OUTFIT_V2` 体感增强等），其中即包含规格要求保留的 `config/recipes/PHOTO_TH_TRAVEL_OUTFIT_V2.json`、`services/photo_*`、`scripts/*` 等。本阶段对这批改动**只读**，未合并、未覆盖、未回退。

---

## 2. 修改文件清单

本阶段仅新增，未修改任何既有文件：

| 文件 | 类型 | 说明 |
|---|---|---|
| `tests/test_photo_travel_v2_golden_contract.py` | 新增 | TH V2 golden/contract 防回归测试，19 条 |
| `docs/VN_SCARF_PHASE0_BASELINE_20260913.md` | 新增 | 本交付文档 |
| `tmp/phase0_vn_scarf/*.txt` | 新增（已被 `.gitignore` 的 `tmp/` 覆盖） | 测试原始输出与工作区快照 |

规格文档 `docs/VN_SCARF_CROSS_MARKET_IMPLEMENTATION_SPEC_20260913.md` 在本阶段开始前已存在于工作区，且与运营提供的内容逐字一致（33400 字节），无需重新落库。

---

## 3. 新增合同说明（冻结测试，非业务合同变更）

本次**没有业务合同变化**。新增的是一个"快照契约"测试文件，把 TH V2 当前可观察行为钉死，供后续 Phase 的公共能力抽取做零行为变化的证明。

`tests/test_photo_travel_v2_golden_contract.py` 共 4 个 TestCase / 19 条：

### 3.1 `TravelV2RecipeConfigGoldenTest`（7 条）

冻结 `config/recipes/PHOTO_TH_TRAVEL_OUTFIT_V2.json`：

- **身份**：`schema_version=opv-content-recipe-v1`、`recipe_version=7`、`anchor_slot=2`、`shot_count=5`、`status=active`。
- **页面结构**：5 个 slot 的 `role / layout_variant / source_roles`，其中 **P1 = `travel_cover` 且 source 恒为 `look_a`**，P2–P5 = `look_a/b/c/d` 顺序。
- **content_card**：5 页 `index / layout / source_roles` 同上。
- **旅行合同**：7 个 moment 的 key 顺序、`moments_per_post=4`、`look_required_fields`。
- **咖啡店场景鞋履规则**：`allowed=[]`、`forbidden=["STILETTO"]`——防止重构把空允许表"顺手补全"而改变 QA。
- **跨合同一致性**：`variables_schema.destination` 枚举 == `destination_labels_th` 键集；`temperature_band` 枚举 == `temperature_labels_th` 键集（Phase 2 换 Locale Pack 时的护栏）。
- **recipe_spec**：`category_key=womenswear`、`markets=["TH"]`、`template_id=PHOTO_TRAVEL_CARD_V3`、`template_version=3`、`supported_presentations`、`asset_requirements.required_roles`、`copy_style.locale=th-TH`。

### 3.2 `TravelV2PolicyGoldenTest`（3 条）

冻结 `config/photo_planning_policies/TH_TRAVEL_OUTFIT_V1.json` 与注册表：

- `recipe_has_planning_policy("PHOTO_TH_TRAVEL_OUTFIT_V2")` 为真。
- `policy_id=TH_TRAVEL_OUTFIT_V1`、`policy_version=3`、`recipe_ids`、`supported_reference_modes=["STYLE","COMPLETE_LOOK"]`、`planning_flow=travel_two_step`、`minimum_cross_post_axis_difference=2`。
- 8 个旅行 family 的顺序，以及 `COOL_WEATHER_TRAVEL` 的家族顺序；每个主题顺序引用的 family 必须存在于 `families`。

### 3.3 `TravelV2ReferenceModeContractTest`（5 条）

冻结 Phase 1 将要包进 `PhotoReferenceContext` 的参考解析契约（`services/photo_reference.resolve_reference_mode`）：

- 运营枚举 `("自动判断","风格参考","商品参考","完整穿搭")`。
- 显式映射：`风格参考→STYLE`、`商品参考→PRODUCT`、`完整穿搭→COMPLETE_LOOK`。
- AUTO 判定优先级：`有 product_id → PRODUCT` > `附件数 == 角色数×篇数 → COMPLETE_LOOK` > `其余 → STYLE`。
- `完整穿搭` 附件数不足时报错；多篇时按 `角色数×篇数` 校验。
- 空输入与未知类型报错。

### 3.4 `TravelV2PlanGoldenTest`（4 条）

在无 vision profile 时，family 轮换是 `record_id:recipe_id:theme_key:reference_mode` 的纯哈希，因此整份计划可复现，适合钉死：

- 冻结 `record_id="rec-golden-th-v2"` + 主题「凉爽旅行」(`COOL_WEATHER_TRAVEL`) + `STYLE` + `count=3` 的：
  - family 顺序 `("shopping_mall","airport_transit","city_walk")`；
  - 三篇 cover 文案原文；
  - `planning_flow`、`required_roles`。
- 每篇 4 个 look 的角色顺序恒为 `look_a..look_d`，且冻结服装字段（`display_label/outerwear/top_inner/bottom/shoes/outerwear_type/bottom_type`）齐全。
- **pinned 计划摘要** `plan_sha256 = 0acb4cb3e3bb255e9a7848710967341a94e235783af78008ea457563bbc5f1ca`，并要求两次调用可复现。
- V2 仍拒绝 `PRODUCT` 参考模式。

> 说明：`travel_moment / scene_prompt / weather_logic / footwear_type` 由下游旅行流程注入，不在批次计划阶段产出，故不在本文件的计划断言中，而是在 3.1 的合同字段里冻结。

### 3.5 金丝雀自检（证明摘要确实"咬得住"）

只读方式（monkeypatch，未改盘）验证 pinned 摘要对漂移敏感：

```
baseline sha: 0acb4cb3e3bb255e9a7848710967341a94e235783af78008ea457563bbc5f1ca
mutate shoes:     sha_changed=True
mutate outerwear: sha_changed=True
canary OK: pinned digest detects garment drift
```

即：改动任一 Look 的服装字段，golden 立即失败。

---

## 4. 向后兼容说明

- 零业务行为变化：本阶段未触碰 `services/`、`config/`、`scripts/` 任何字节。
- 测试总数 919 → **938**（+19 新 golden），photo 域 333 → **352**。
- 新增测试全部只读现有配置与公共函数，不写外部系统、不联网。

---

## 5. 执行过的测试命令与完整结果

全部以 `/usr/bin/python3` 运行（项目使用标准库 `unittest`，本机无 `pytest` 属环境事实，非代码失败）。

```bash
cd /Users/likeu3/.openclaw/workspace/packages/organic_photo_video

# 5.1 规格 §14.1 指定的 10 个照域模块
PYTHONPATH=.:tests /usr/bin/python3 -m unittest \
  tests.test_photo_reference_vision tests.test_photo_content_planner \
  tests.test_photo_theme_reference tests.test_photo_asset_supply \
  tests.test_photo_persona_pack tests.test_product_reference_resolver \
  tests.test_photo_travel_semantics tests.test_photo_package \
  tests.test_photo_feishu_batch tests.test_feishu_workflow
# → Ran 188 tests ... OK   (tmp/phase0_vn_scarf/focused_10modules.txt)

# 5.2 §14.2 photo 域
PYTHONPATH=.:tests /usr/bin/python3 -m unittest discover -s tests -p 'test_photo_*.py'
# → Ran 333 tests ... OK   (新增前)  tmp/phase0_vn_scarf/photo_domain.txt
# → Ran 352 tests ... OK   (新增后)  tmp/phase0_vn_scarf/photo_domain_after.txt

# 5.3 §14.2 全量回归
PYTHONPATH=.:tests /usr/bin/python3 -m unittest discover -s tests -p 'test_*.py'
# → Ran 919 tests ... OK   (新增前)  tmp/phase0_vn_scarf/full_regression.txt
# → Ran 938 tests ... OK   (新增后)  tmp/phase0_vn_scarf/full_regression_after.txt

# 5.4 新增 golden 单跑
PYTHONPATH=.:tests /usr/bin/python3 -m unittest tests.test_photo_travel_v2_golden_contract
# → Ran 19 tests ... OK

# 5.5 空白检查
git diff --check
# → 无输出（干净）  tmp/phase0_vn_scarf/git_diff_check_after.txt
```

**结论：919 基线全绿，+19 新 golden 后 938 全绿；`git diff --check` 干净。**

---

## 6. 未执行的真实调用说明

| 真实调用 | 状态 |
|---|---|
| 真实生图 / 图片模型 | 未执行 |
| 外部视觉质检（`OPV_PHOTO_VISION_PROVIDER`） | 未执行（未授权） |
| RDS 写入 | 未执行 |
| 飞书 schema 修改 | 未执行（未新增字段/表） |
| CreatOK / TikTok 发布 | 未执行 |
| 任何网络请求 | 未执行 |

---

## 7. canary 产物

- **本阶段无 canary 产物**：规格 §14.2 只要求 Phase 2 起输出 resolved execution context fixture、Phase 3 起输出 TH/VN 同商品快照对照、Phase 4 起输出三种参考模式离线 manifest。
- Phase 0 产物为：工作区快照 `tmp/phase0_vn_scarf/git_status_short.txt`、测试原始日志 `tmp/phase0_vn_scarf/*.txt`（均已被 `.gitignore` 的 `tmp/` 覆盖，不入库）。

---

## 8. 已知限制与下一阶段阻塞项

| 项 | 状态 | 影响 |
|---|---|---|
| VN 账号原生图文能力 | 未确认（本地 VN 账号暂停/未验证 `content_photo_capable`） | 阻塞 Phase 5 发布；不阻塞 Phase 1–4 开发 |
| 越南语文案 | 未 `NATIVE_APPROVED` | 阻塞正式发布门禁；Phase 4 可先 DRAFT canary |
| 遗留死代码 `services/feishu_workflow.resolve_temperature_variables()` | 仍在（上一轮已记录于 §8） | 与本次 Phase 无关；清理需改约 40 行 + 外部 schema 同步，需单独立项 |
| `PHOTO_TH_TRAVEL_OUTFIT_V2` 仍为 `active` 且复用 `TH_TRAVEL_OUTFIT_V1` 策略 | 设计现状 | Phase 2 新增 V3 时须保持 V2 路由/文案/顺序不变（本 golden 已就位把关） |
| 本 golden 依赖哈希确定性 | 已验证可复现 | 若后续有意变更 V2 合同，必须走评审并显式更新 pinned 摘要，不得静默 re-baseline |

---

## 9. 下一步

Phase 0 交付完成，**暂停等待验收**。

验收通过后进入 Phase 1：新增 `PhotoReferenceContext`（`services/photo_reference_context.py`）+ Category Adapter registry（先只注册 WOMENSWEAR 兼容实现），把 TH V2 的参考解析与商品槽位调用迁到公共入口，并证明 **TH V2 测试输出与迁移前一致**（本 golden 文件即为该证明的判定器，交付物为"零业务行为变化"）。
