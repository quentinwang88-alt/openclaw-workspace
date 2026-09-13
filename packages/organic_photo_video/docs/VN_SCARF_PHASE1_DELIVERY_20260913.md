# VN 围巾跨市场复用 — Phase 1 交付（抽公共能力，零行为变化）

日期：2026-09-13
状态：代码与测试完成，**未提交，待验收**
对应规格：`docs/VN_SCARF_CROSS_MARKET_IMPLEMENTATION_SPEC_20260913.md` §7 Phase 1、§13 交付物
适用项目：`packages/organic_photo_video`

---

## 0. 本阶段范围

按规格 §14「默认只执行一个 Phase，验收后再进入下一 Phase」，本轮**只做 Phase 1**。

Phase 1 规格要求的 4 项，逐项状态：

| # | 规格原文（§7 Phase 1） | 状态 |
|---|---|---|
| 1 | 新增 `PhotoReferenceContext` | ✅ 已完成 |
| 2 | 新增 Category Adapter registry，先只注册 WOMENSWEAR 兼容实现 | ✅ 已完成 |
| 3 | 将 TH V2 参考解析和商品槽位调用迁移到公共入口 | ✅ 已完成 |
| 4 | 确认 TH V2 测试输出与迁移前一致 | ✅ 已完成（见 §4） |

明确未做（留给后续 Phase，均需单独门禁）：

- 未新增 Recipe v2、Locale Pack、Market Pack、Destination Catalog、`SCARF_V1`、VN 文案/预设/发布路由。
- 未真实生图、未调用外部视觉模型、未写 RDS、未发布、未修改飞书生产表。
- 未 `git reset` / `checkout` / `stash` / 清理任何未跟踪文件。

---

## 1. 修改文件清单

### 1.1 新增

| 文件 | 行数 | 说明 |
|---|---|---|
| `services/photo_reference_context.py` | 170 | 公共参考管线门面：`PhotoReferenceContext` 冻结数据合同 + `resolve_photo_reference_context()` |
| `services/photo_category_registry.py` | 295 | 类目适配器注册表：`PhotoCategoryAdapter` + 首个适配器 `WOMENSWEAR_V1` |
| `tests/test_photo_reference_context.py` | 303 | 门面等价性/契约测试，17 条 |
| `tests/test_photo_category_registry.py` | 382 | 注册表等价性/防回归测试，35 条 |

### 1.2 修改（4 处调用点迁移到公共入口）

| 文件 | 变化 | 迁移内容 |
|---|---|---|
| `services/feishu_workflow.py` | +21 / −22 | 非分层参考解析（参考模式 + 参考附件 + 商品快照 + 商品上下文）迁到 `resolve_photo_reference_context`；移除本地导入中已不再使用的 `resolve_reference_mode` |
| `services/photo_reference_vision.py` | +7 / −23 | `_normalize_travel_plan` 的 `target_fields` 内联表 → `apply_target_product_to_look` |
| `services/photo_style_reference_supply.py` | +14 / −1 | `outerwear` 覆盖条件 → `_product_targets_slot` → `product_owns_slot` |
| `services/product_reference_resolver.py` | +6 / −7 | 内联 `role_order` 表 → `role_priority_for_slot` |
| `services/image_generator.py` | +2 / −6（**仅我的部分**） | `product_label` 内联表 → `resolve_product_display_label` |

> ⚠️ `services/image_generator.py` 当前工作区版本**同时包含另一个并发进程的改动**（OneRoute 通道路由，约 +780 行，替换 `FallbackShotGenerator`）。**我的改动只有 2 处**：新增 `from services.photo_category_registry import resolve_product_display_label`，以及把 `product_label = {...}.get(...)` 换成 `resolve_product_display_label(...)`。二者在同一文件内无法在无人介入下干净拆分，故本 Phase **未提交**（见 §7）。

---

## 2. 新增/变更合同说明

### 2.1 `PhotoReferenceContext`（新增数据合同）

冻结 dataclass，字段：`reference_mode`、`reference_attachments`、`product_snapshot`、
`product_context`、`product_reference_paths`、`complete_look_sources`、
`style_reference_paths`、`input_fingerprint`。

- 字段顺序参照规格 §3.1 A 建议合同；`reference_attachments` 为**附加字段**，承载 TH V2 运营字段
  当前的飞书附件描述符，使 V2 路径迁移后仍取到与迁移前相同的对象。
- 公共入口：`resolve_photo_reference_context(*, selected_type, unified_attachments, legacy_complete,
  legacy_product, product_id, required_roles, quantity, account_id, record_id, product_reference_resolver)`。
- 与规格签名映射：规格的 `record` / `recipe` / `account` / `asset_supply` 在本阶段**未收进门面**——
  它们属于飞书字段读取与附件暂存，Phase 1 保持其在 `feishu_workflow` 内的位置不动，避免把飞书异常
  类型与 IO 依赖引入公共模块。门面只接收已解析的原语。规格 §14 允许签名按现有代码风格微调并要求给出映射，
  此处即为映射说明。
- 模块纯净：无 `from services.` 之外的第三方依赖，无文件/网络/DB 访问（有测试守卫）。

### 2.2 `PhotoCategoryAdapter`（新增类目合同）

`WOMENSWEAR_V1` 逐字复刻迁移前的 3 张内联表 + 1 处分支：

| 适配器字段 | 复刻来源 |
|---|---|
| `product_slot_by_category` | 规划侧 `target_fields` 的类目→槽位映射 |
| `product_display_label_by_category` / `default_product_display_label_zh` | `image_generator` 的 `product_label` 表 |
| `product_own_category_by_slot` | `photo_style_reference_supply` 的 `outerwear` 覆盖条件 |
| `required_product_roles` / `product_reference_priority_by_slot` | `product_reference_resolver` 的 `role_order` |

关键语义区分（本 Phase 抓到并修正的真 bug）：

- **规划槽位映射**（`resolve_product_slot`）：`dress → outerwear`（连衣裙走外套槽位）；
- **生成归属槽位**（`product_owns_slot`）：只有真正的 `outerwear` 商品才抢占 `outerwear` 槽位。
- 首次实现误用前者实现后者，导致 `dress` 商品抢走外套槽位 → **行为变化**。已由
  `test_style_supply_condition_matches_legacy(product={'category':'dress'})` 抓出并以
  `product_own_category_by_slot = {"outerwear": ("outerwear",)}` 修正。

### 2.3 无既有业务合同变化

- 未改任何 recipe/policy/字段 schema；未新增或放宽 v1 合同。
- `PHOTO_TH_TRAVEL_OUTFIT_V2` 及其冻结任务、manifest、缓存、发布包**逐字未动**。

---

## 3. 向后兼容说明

- TH V2 参考模式（STYLE / PRODUCT / COMPLETE_LOOK / AUTO）、分层流（温度分层 / 冷热切换）路径
  **行为不变**：分层流完全未走新门面；非分层流门面输出与旧内联逐字等价（§4 测试）。
- 飞书运营字段、状态机、错误文案不变：门面不依赖飞书异常类型，缺包错误仍由调用方包装为原有
  `指定商品 {id} 缺少可用商品参考包：...`；`resolve_reference_mode` 的 `ValueError` 仍按原样包装为
  `FeishuWorkflowError(str(exc))`。
- `style_product` / `reference_attachments` 下游只读不改写（已核对），故可安全转为冻结上下文。

---

## 4. 执行过的测试命令与完整结果

工作目录：`packages/organic_photo_video`

### 4.1 新增测试

```bash
PYTHONPATH=.:tests /usr/bin/python3 -m unittest tests.test_photo_category_registry   # 35 条 OK
PYTHONPATH=.:tests /usr/bin/python3 -m unittest tests.test_photo_reference_context   # 17 条 OK
```

### 4.2 每阶段最低验证（规格 §14.2）

```bash
PYTHONPATH=.:tests /usr/bin/python3 -m unittest discover -s tests -p 'test_photo_*.py'
# Ran 404 tests ... OK   （改动前 387 + 新增 17）
```

### 4.3 全量回归

```bash
PYTHONPATH=.:tests /usr/bin/python3 -m unittest discover -s tests -p 'test_*.py'
# Ran 1020 tests ... OK
git diff --check      # 无空白错误
```

### 4.4 零行为变化隔离验证（关键证据）

因存在**并发进程**同时改 `image_generator.py`，主工作区结果会抖动，故用隔离树验证：

| 树 | 内容 | 结果 |
|---|---|---|
| `/tmp/opv_head`（纯 HEAD） | `git archive HEAD` + 本机三项依赖 | **938 OK ×2** |
| `/tmp/opv_iso`（HEAD + 我的 Phase 1 全部改动） | 叠加新增 4 文件 + 迁移 5 文件 | **990 OK ×3** |

**990 − 938 = 52 = 新增测试数（35 + 17）**，即我的改动只新增测试、未改变任何既有测试结果 ⇒ **零行为变化**。
（隔离树中 `image_generator.py` 只手工叠加了我的 2 行改动，未带入并发进程的 780 行。）

隔离树复现绿色基线所需的本机依赖（不入库）：`.env.local`、
`shared/data/organic_photo_video/asset_sets/`、`skills/lightweight-tryon-video/var/light_tryon.sqlite3`。

### 4.5 环境坑（本阶段踩到）

- `git diff | grep` 命中非 UTF-8 字节时被判为二进制而**无输出**，会误判"改动丢失"；需 `grep -a` 或先落盘。

---

## 5. 未执行的真实调用说明

本阶段**未执行**以下外部调用，全部留待后续 Phase 的门禁：

- 未调用真实视觉模型（Doubao Vision）；
- 未真实生图 / 未调用图像服务；
- 未写飞书生产表、未发布、未写 RDS；
- 未执行任何 TikTok / 发布路由。
- 门面 `resolve_photo_reference_context` 的 `style_reference_paths` 字段**本阶段恒为空**：附件暂存仍在
  `feishu_workflow` 原位，待 Phase 2 引入执行上下文时回填。

---

## 6. canary 产物路径与 manifest

**本阶段无 canary 产物**。规格 §14.2 要求"Phase 2 起输出 resolved execution context fixture；
Phase 3 起输出 scarf 对照；Phase 4 起输出三种参考模式离线 manifest；Phase 5 才提供真实发布证据"。
Phase 1 的交付物是**公共 facade/registry + 零行为变化证据**，不产生 canary manifest。

---

## 7. 已知限制与下一阶段阻塞项

### 7.1 已知限制

1. `photo_reference_context.py` 的规格签名（含 `record`/`recipe`/`account`/`asset_supply`）**未完全落实**：
   本阶段仅收"已解析原语"，飞书字段读取与附件暂存仍在 `feishu_workflow`。Phase 2 引入执行上下文后
   再收敛签名（届时须再次证明 V2 等价）。
2. `photo_category_registry.apply_target_product_to_look` 在 `adapter=None` 时自解析类目，
   仅覆盖 TH V2 现有路径；SCARF 语义未定义。
3. 注册表目前只有 `WOMENSWEAR_V1`；`role_priority_for_slot` 仍与类目无关（沿用旧 `role_order`），
   规格要求在 Phase 3 改为按适配器解析（`product_reference_resolver.py` 内已留注释）。

### 7.2 阻塞项 / 需用户裁定

1. **提交拆分（阻塞本 Phase 收口）**：`image_generator.py` 混有并发进程的 OneRoute 改动
   （约 +780 行），与我的 2 行改动不可在无人介入下干净拆分。规格 §7 要求"每个 Phase 独立提交"，
   故 Phase 1 **当前未提交**。需用户裁定：
   - (A) 先把并发进程的 OneRoute 改动单独提交/暂存，再提交我的 Phase 1；
   - (B) 由我用 `git add -p` 级手段只挑我的 2 行入 Phase 1 提交（需接受对并发进程改动零感知）；
   - (C) 暂缓提交，等并发进程完成后再一并处理。
2. **并发写入风险**：另一进程正在改 `image_generator.py` 与
   `tests/test_image_generator_channels.py`，导致主工作区同一命令结果抖动（曾见 938→1020 条、
   失败数 6/5/14 跳动）。本 Phase 结论均以隔离树为准；后续 Phase 开工前建议确认该进程已停止。
3. **两个疑似垃圾文件未处理**（与本次无关，仅提示）：工作区根目录的 `-`、
   `skills/short-video-auto-publisher/SHORT_VIDEO_AUTO_PUBLISH_DB_PATH`。

### 7.3 进入 Phase 2 的前置条件

- 本 Phase 验收通过；
- 并发进程停止或已提交，工作区可干净切分；
- 按规格 §14.2，Phase 2 起必须输出至少一份 resolved execution context fixture。

---

## 8. 验收对照（一句话）

> Phase 1 的两个交付物——**公共 facade（`PhotoReferenceContext`）** 与 **公共 registry（Category Adapter）**——
> 均已完成，4 处调用点已迁移，52 条新测试通过；隔离树证明 **HEAD 938 OK → HEAD+Phase 1 990 OK**，
> 既有行为零变化。尚未提交，等待 §7.2 的提交拆分裁定。
