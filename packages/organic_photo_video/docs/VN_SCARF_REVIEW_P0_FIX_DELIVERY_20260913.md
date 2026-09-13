# VN 围巾跨市场｜Review 不通过整改交付（3×P0 + 生产链集成测试）

- **日期**：2026-09-13
- **范围**：`packages/organic_photo_video/`
- **输入**：外部评审报告《结论：Review 不通过》（3×P0 / 3×P1 / 1×P2），结论为「已完成 Phase 1–4 的离线骨架与 canary，但未达 Definition of Done，不能启用 VN 围巾生产」
- **本轮授权范围**：只修 3 个 P0 + 补 1 条生产链集成测试；V3 的「类目/市场校验语义」按「能力契约（capability contract）」改判
- **不在本轮范围**：P1-4（Execution Context 未进入生产冻结）、P1-5（Destination Catalog 未进入运行时）、P2-7（缺 `披肩围巾` 别名）——见 §6
- **结论**：3 个 P0 全部修复并有可复现证据；新增 24 条集成/回归测试并全绿；VN **仍不可上线**，门禁见 §7

---

## 1. 评审结论核对结果

先逐条核对评审事实（本轮开工前完成），结论是**评审主体成立**，其中 2 处表述需更正：

| 评审原文 | 核对结果 |
|---|---|
| 「V2 Recipe 按设计不包含 `category_key`/`markets`」 | **表述反了**。`opv-photo-recipe-v1`（TH V2）**有** `category_key`/`markets`；`opv-photo-recipe-v2`（V3）**才去掉**它们，改为 `required_category_capabilities` + `market_policy` + `locale_copy_packs`。评审的结论（旧消费方按字面字段比较 → v2 配方永远进不了链路）成立。 |
| 「当前围巾别名只有『披肩』」 | **不准确**。已存在 4 个别名（`围巾`/`披肩`/`丝巾`/`脖套`），缺的是 `披肩围巾` 这一条；P2-7 因此仍成立但优先级最低。 |
| P0-1 / P0-2 / P0-3 其余描述 | 逐条读码确认成立（详见 §2–§4）。 |
| P0-2「有现成入口」判断 | 成立：`task.target_locale` / `task.market_pack_id` 已由 `task_intake.py` 落库，`photo_planner.py` 也已用 `task.target_locale`；缺的是**消费端**按语言取文案与取标签。 |

---

## 2. P0-1｜通用 Recipe V3 无法进入真实生产链

**症状**：`PhotoRequestFactory.build_batch()` 与 `PhotoReusePlannerService.plan_task()` 都用 `recipe_spec["category_key"] != category_key` 和 `market not in recipe_spec["markets"]` 做校验。v2 配方刻意不带这两个字段，于是**任何** v2 配方在冻结/规划阶段即被拒；Phase 2–4 的 canary 全部绕过了这条链路（直接调 `plan_th_choice_batch`），所以缺陷一直不可见。

**修复（改判为能力契约）**：新增 `services/photo_recipe_contract.py`，把「配方能否服务某类目/某市场」收敛到一处，v1/v2 分流：

- `category_binding_errors()`：v1 保留字面 `category_key` 相等（逐字不变）；v2 要求适配器 `capabilities ⊇ required_category_capabilities`——类目**名**可以在市场间不同，能力必须齐备。
- `market_binding_errors()`：v1 保留字面 `markets` 列表；v2 由 `market_policy` 决定。`MARKET_PACK_REQUIRED` 要求存在**active** 的 Market Pack，且其 `target_country`/`target_locale` 与请求一致；包可由调用方传入或按账号 `default_market_pack_id` 解析。既无包又无账号 → 直接拒绝（「无法校验的市场声明」正是该校验要防的东西）。
- `product_mode_errors()`：两套 schema 共用。

两个消费方改为调用同一组函数：

| 文件 | 位置 | 说明 |
|---|---|---|
| `services/photo_recipe_contract.py` | 新增（190 行） | 唯一真源，不读文件/环境/网络 |
| `services/photo_request_factory.py` | `build_batch` L232–242 | 字面比较 → `category_binding_errors` + `market_binding_errors` + `product_mode_errors` |
| `services/photo_request_factory.py` | `validate_frozen_request` L54–79 | 冻结件复校同源；v2 额外要求 `market`/`locale` 非空 |
| `services/photo_planner.py` | L80–103 | 同上；v2 额外强制「任务必须先有 Market Pack」 |

**零行为变化**：v1 分支逐字保留原判断；TH V2 的冻结件与旧实现一致（`test_photo_request_factory` 17/17、`test_photo_travel_v2_golden_contract` 19/19 未改动即通过）。

---

## 3. P0-2｜Locale Pack 只活在测试里，真实 VN 链路仍出泰语

**症状（两半条链）**：

1. `config/loader.py` 为 v2 配方写入 `copy_variants_by_locale`，同时把**排序第一**的语言（有 `th-TH` 时恒为泰语）写进 `copy_variants` 作为兼容字段。冻结侧直接读 `copy_variants`，于是 VN 任务冻结的是**泰语文案**。
2. 旅游线的 `{destination}`/`{temperature}` 审计 token 由 `contract_copy_tokens()` 从**内联泰语表**（`destination_labels_th`/`temperature_labels_th`）解析；V3 配方已删除这两张表，于是真实 VN 链路把两个 token 解析成**空串**，冻结出 `"Gợi ý phối đồ du lịch "` 这种无目的地的标题——泰语泄漏检查查不出来。

**修复**：

| 文件 | 位置 | 变更 |
|---|---|---|
| `config/loader.py` | `resolve_locale_pack()` L219–236 | 按语言解析 Locale Pack；解析不到返回 `None`（由调用方决定是否致命），不做任何回退 |
| `services/photo_request_factory.py` | `_localized_variants()` L170 | 冻结文案按请求语言取 `copy_variants_by_locale[locale]`；该语言不存在则**报错**，不再静默取泰语 |
| `services/photo_request_factory.py` | `_bound_locale_pack()` L144、L246、L319 | 仅当配方声明 `locale_copy_packs`（v2）时绑定 Locale Pack，并把包传给 `contract_copy_tokens(locale_pack=…)`——补齐第 2 半条链；缺包直接报错，避免「空目的地」这类隐性降级 |
| `services/photo_locale.py` | `travel_copy_template()` L163 | 降级文案模板由 Locale Pack 拥有（generic 标签 + 轮换 `complete_look_copy`）；`locale_pack=None` 返回 `None`，调用方保留内联泰语模板 |
| `services/photo_reference_vision.py` | `_travel_copy_template()` L319、降级分支 L1691–1724 | 文案纯度按**绑定的语言**判定（未绑定仍为 `th-TH`）；降级模板按语言生成 |
| `services/photo_style_reference_supply.py` | L208 / L526 | 生成请求里的语言标记由 `"th-TH"` 写死改为随任务的 `locale`（默认值保持 `th-TH`） |
| `services/feishu_workflow.py` | L1178–1191 | 生产入口按配方声明解析 Locale Pack；仅 v2 生效，v1 不解析 |

**顺带修掉的同类问题（评审 P1-6，同一函数内）**：`_require_travel_copy_release_allowed()` 用 `recipe_id.startswith("PHOTO_TH_TRAVEL")` 识别旅行线，国家无关化之后 **VN 旅行配方会被发布门禁静默跳过**。改为模块级谓词 `is_topic_travel_recipe(recipe_id, repository)`（`feishu_workflow.py` L420）：历史前缀仍直接命中，其余按配方自身的 `travel_contract` / `planning_flow` 判定。同函数内 `copy_locale_issues(copy_block, "th-TH")` 也改为 `task.target_locale`，报错文案由「泰语文案」泛化为「发布文案」（L2697–2730）。

---

## 4. P0-3｜围巾 QA 只声明不接线

**症状**：`SCARF_QA_FIELDS` / `SCARF_QA_RULES` / `SCARF_V1` 在 Phase 3 已声明，但**没有任何运行时消费者**。质检提示词未要求逐项判定，`normalize_travel_qa()` 也只看通用 `product_matches`——而提示词明确写着「配饰缺失属 MINOR」，所以「指定围巾缺失/被遮挡/被换结构/遮脸」会被放行。

**修复**：

| 文件 | 位置 | 变更 |
|---|---|---|
| `services/photo_travel_qa.py` | L42–62 | 新增 `PRODUCT_QA_FAILURE_FIELDS`（7 个字段 → 失败码）与 `PRODUCT_QA_REPAIR_ZH`（逐字段修复话术） |
| `services/photo_travel_qa.py` | L163–178 | `normalize_travel_qa(..., product_qa_fields=())`：有商品且类目声明了字段时逐项 `_require_bool`，**任一 false 即确定性失败**，不因 `outfit_severity=MINOR` 降级；字段缺失仍是 `QA_SCHEMA_INCOMPLETE`（不默认通过） |
| `services/photo_travel_qa.py` | L205–208 / L252–258 | 失败码注入 `elif product_qa_failure`；修复文案优先取类目话术 |
| `services/photo_reference_vision.py` | L919–928 | `review_travel_pages()` 按**商品自身类目**解析适配器（`adapter_for_product_category`）取 `qa_fields`/`qa_rules`，并把 `main_product_slot` 补进逐页 outfit 字段 |
| `services/photo_reference_vision.py` | `_travel_qa_prompt` L1984–2037 | 依据类目声明生成【指定商品逐项判定】块并注入 JSON 示例字段；`qa_fields` 为空（womenswear）时两块都退化为空串，提示词与历史逐字一致 |

**自查发现并修掉的第 4 个缺口**：当失败来自 `face_unobscured=false` 时，`failure_code` 是 `PERSON_DISASTER`，而原先的 `PERSON_DISASTER` 分支会用「明显不自然的头部倾斜」这条与人无关的话术覆盖掉围巾修复话术（两个人形旗标都为 false 时尤其明显）。已改为该分支只在人形旗标确实为真时才发声（`photo_travel_qa.py` L248–258）——womenswear 路径下 `product_qa_failure` 恒为空，输出逐字不变。

---

## 5. 生产链集成测试（评审明确要求的验收门）

新增 `tests/test_photo_v3_production_path.py`（**24 条**），真正调用 `PhotoRequestFactory.build_batch()` 与 `PhotoReusePlannerService.plan_task()`，不做任何「手工挑语言/手工绕链路」：

| 测试类 | 覆盖 |
|---|---|
| `VnScarfProductionPathTest`（12） | VN + scarf + STYLE+PRODUCT 冻结→规划全链：v2 配方凭**能力契约**冻结（`category_key`/`markets` 确实不存在）；能力超集接受、未注册类目与能力不足的适配器拒绝；冻结文案为越南语且**无泰文/无 CJK**；绑定的不是泰语文案包；不支持语言报错；产品快照进入 `accessories` 槽；`plan_task` 产出 5 页计划、文案无泰文、任务转 `planned` 并建版本；Market Pack 回退 draft 时**规划器自行拦截**；出厂 draft 包拦截整条链 |
| `VnTravelV3FactoryTokenTest`（1） | 旅游线经真实工厂的 `{destination}`/`{temperature}` **解析为越南语标签**（修复前为空串），且无残留占位符、无泰文 |
| `ScarfQaContractRuntimeTest`（7） | 围巾 QA 契约在运行时生效：提示词携带逐项字段（womenswear 不带）；全 true 通过并回报 7 个逐项布尔；`product_present=false` → `OUTFIT_MISMATCH` 且修复话术点明缺失、进入定向重生（`failed_roles_from_travel_qa`）；`face_unobscured=false` → `PERSON_DISASTER` 且话术点明面部；`edge_or_fringe_matches=false` 不因 MINOR 放行；**同一响应在 womenswear（无字段）下仍通过**（证明契约确实改变了行为）；字段缺失 → 结构不完整而非通过 |
| `VnDegradedCopyTemplateTest`（3） | 模型文案不合格时的降级模板为越南语、4 轮换互不相同、无包时逐字保留内联泰语模板 |
| `LegacyV1FactoryPathUnchangedTest`（2） | 新分支对**全部** v1 配方不可达；v1 token 解析在传入 `locale_pack=None` 前后完全一致且仍取内联泰语表 |

---

## 6. 本轮未处理（评审剩余项）

| 编号 | 内容 | 状态与理由 |
|---|---|---|
| P1-4 | Execution Context 未进入生产冻结 | 未动。当前只在 canary 中使用；进入生产冻结需先确定冻结列/版本语义，属独立改造 |
| P1-5 | Destination Catalog 未进入运行时 | 未动。`destination_weights` 已配置但运行时仍走 recipe 内联 `destination` 枚举，切换需迁移变量词表（Phase 3 §7.1 第 4 项已标注留在 Phase 5） |
| P2-7 | 缺 `披肩围巾` 别名 | 未动。属于可选别名扩充，不影响能力契约 |

---

## 7. 验证证据

**测试**（`PYTHONPATH=.:tests /usr/bin/python3 -m unittest …`）：

| 口径 | 结果 |
|---|---|
| 全量（`test_*.py`） | **1192 tests, OK** |
| photo 域（`test_photo*.py`） | **574 tests, OK** |
| 本轮新增 | 24 tests, OK |

**零行为变化（HEAD 隔离树对比）**：`git archive HEAD` 展开到独立目录，补齐同级上下文（`workspace_support.py`、`skills/`、`shared/`）后运行：

- HEAD 基线与「HEAD + 本轮修复」逐文件对比，13 个历史敏感文件（TH V2 金标准契约 / V3 等价 / 请求工厂 / 规划器 / 旅行语义 / 围巾类目 / 内容安全 / 主题参考 / Locale / 参考上下文 / 内容规划 / VN 激活 / MX 假发）**通过数与状态完全一致**。
- 计数对账：`1192 = 1166（HEAD 基线）＋ 2（工作区既有的、与本轮无关的 `test_image_generator_channels` 新增）＋ 24（本轮新增）`。
- 隔离树中 `test_photo_labels` 有 2 条因**目录搬迁**找不到 `shared/…asset_sets/*.png` 而失败，在真实工作区 7/7 通过，与本轮改动无关。

**预检**：`scripts/preflight_native_photo.py` → `exit=0`，`errors=[]`、`external_writes=0`、`recipe_count=15`、`profile_count=31`、`canary_market_unbound=["PHOTO_MATCHING_CHOICE_V3","PHOTO_TRAVEL_OUTFIT_V3"]`（两条 V3 仍为 draft/未绑定市场，与修复前一致）。

---

## 8. VN 上线门禁（本轮之后仍然关闭）

修复使 VN 链路**在能力上可跑通**，但以下绑定仍未交付，VN 不得启用：

1. `MP_VN_DEFAULT_V1` 出厂 `status=draft`；代码层已把非 active 包的冻结与规划都变成硬失败（本轮新增断言）。
2. `OPV_VN_TEST_001` 出厂 `status=paused`、`publishing_enabled=false`；`main_publish_routes.json` 无 VN 路由。
3. 越南语文案包 `language_review_status=DRAFT`，正式发布前需母语审校升至 `NATIVE_APPROVED`。
4. **缺少 VN 围巾素材集**：`PHOTO_TRAVEL_OUTFIT_V3` 的 `asset_set_keys` 仍指 TH 键（Phase 4 canary 已把它列为 unresolved）；本条集成测试用显式 override 钉住 VN 素材集来验证 token 链路，生产化需把 VN 素材集写进配方。
5. P1-4 / P1-5 未做。

---

## 9. 交付物清单

| 文件 | 类型 |
|---|---|
| `services/photo_recipe_contract.py` | 新增（共享配方绑定契约） |
| `tests/test_photo_v3_production_path.py` | 新增（24 条生产链集成测试） |
| `services/photo_request_factory.py` | 修改（P0-1 绑定 + P0-2 文案/标签按语言） |
| `services/photo_planner.py` | 修改（P0-1 绑定 + Market Pack 前置校验） |
| `services/photo_locale.py` | 修改（P0-2 语言自有的降级模板） |
| `services/photo_reference_vision.py` | 修改（P0-2 文案纯度/降级 + P0-3 质检接线） |
| `services/photo_travel_qa.py` | 修改（P0-3 逐项商品质检失败语义） |
| `services/photo_style_reference_supply.py` | 修改（P0-2 语言标记随任务） |
| `services/feishu_workflow.py` | 修改（P0-2 生产入口绑定 + P1-6 旅行线谓词） |
| `config/loader.py` | 修改（`resolve_locale_pack`） |
