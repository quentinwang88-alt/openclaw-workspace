# 配饰原创混合模板：阶段 B 交付说明（按类目 × 镜头模块 × 实际结构生成拍摄规则）

日期：2026-09-15
依据：`ACCESSORY_MIXED_TEMPLATE_IMPLEMENTATION_REVIEW_20260915.md`（Review #5、#6）
任务书：《配饰原创混合模板：下一轮具体修复任务书》§三 阶段 B
前置：`ACCESSORY_MIXED_TEMPLATE_REPAIR_STAGE_A_20260915.md`（阶段 A 已完成）
开发源：`/Users/likeu3/.openclaw/workspace/skills/original-script-generator`

本文件只描述**阶段 B**。阶段 C–E 尚未实施。

---

## 一、本阶段修了什么

### Review #5 — 非耳饰类目与静物镜头收到错误的身体裁切规则

**修复前**：四镜共用一个「身体区域」列表。手链的静物镜会继承腕部入画要求；
共用文案一律用耳部措辞写，于是手镯被告知「用耳侧、耳廓、耳垂、颈侧描述」，
静物镜被告知要出现身体局部。

**修复后**：取景拆成**两层**，各自只回答自己那一个问题。

| 层 | 位置 | 回答的问题 | 谁读它 |
|---|---|---|---|
| 模块层 | `module_framing_rules`（config） | 这一镜用**哪一类画面范围** | 四镜全读 |
| 类目层 | `category_rules[zone].module_framing` | 佩戴部位**具体在哪** | 只有两个佩戴镜读 |

- `HANDHELD_PRODUCT`：`view_scope=HAND_AND_PRODUCT`、`view_label=手与商品`、
  `framing_source=MODULE`。允许「手指与商品本体的承托关系 / 手部少量入画，商品是
  清楚主体 / 商品正面与侧面轮廓」；禁止「脸与头部入画 / 正面全脸 / 镜面反射露脸 /
  佩戴部位入画 / 全身穿搭展示」。**不继承**耳部、手腕、后脑、发束任何入画要求。
- `STATIC_PRODUCT`：`view_scope=PRODUCT_AND_SURFACE`、`view_label=商品与台面`、
  `framing_source=MODULE`。禁止「人物、脸与身体入画 / 手或手臂入画 /
  佩戴部位入画 / 镜面反射露脸」——静物镜连手都不许有。
- `WORN_DETAIL` / `WORN_RELATION`：`framing_source=CATEGORY_ZONE`，按类目分流：
  耳饰 = 耳廓与耳垂近景、耳侧与耳垂落点；腕饰 = 同一手腕近景、少量前臂/袖口；
  戒指 = 同一只手与同一根手指、戒面与戒圈；发饰 = 后脑与侧后方、固定发束与发饰。
  每类目同时带自己的 `forbidden_framing`（耳部排除眼鼻嘴、腕部排除脸与头部、
  手指排除整手全身关系、头发排除正面全脸与眼鼻嘴）。

**唯一投影出口**：`project_module_framing(canonical_type, module)`。蓝图指引
（`render_mixed_blueprint_guidance`）与渲染器（`production_script_renderer`）都不再
自己拼裁切句，一律读同一函数；`renderer` 里「出镜方式」行原来写死
「耳侧、耳廓、颈侧」，改为 `worn_body_framing(contract)` 动态投影。
`_face_free_constraint_lines()` 重写为三段：佩戴镜允许范围（并集）、佩戴镜禁令、
**手持/静物镜另有模块禁令**。

**数量规则同步拆分**：原来一条 `quantity_rule` 兼管两件事，导致成对耳饰被要求
「一个近景内出现两只」。现拆为两行——
- `授权数量与身份`：来自 `quantity_rule`，画面里可见多少都不改变它；
- `本镜可见数量（不改变授权数量）`：来自 `visible_quantity_rule`，
  只说明本镜裁切实际可见的部分，「不要求近景内出现全部件数」。

类目的 `visible_quantity_rule` 比模块默认更具体，`project_module_framing` 里
**更具体者胜**。

### Review #6 — 手镯被要求展示不存在的链节、吊坠和搭扣

**修复前**：腕部只有一条规则，实心手镯和细链手链拿到同一句动作，于是手镯被告知
「展示链节与吊坠垂落」、「呈现链节走向与搭扣外观」；发饰规则还写「不得生成参考图
未提供的背面结构」，把结构判断塞在类目层。

**修复后**：新增**物理子类型层** `physical_subtype_rules`，与
`canonical_type_to_zone` 的 13 个 canonical type 完全对齐（已用脚本校验：缺少/多余
子类型规则 = 无）。每条声明 `family` / `observation_focus` / `fallback_observation` /
`structure_facts` / `action_by_module` / `distinct_jobs` / `optional_actions`。

关键分组：

| 分组 | canonical type | 关键事实 |
|---|---|---|
| 柔性腕链 | `bracelet` | `has_chain=VERIFIED` |
| 刚性腕圈 | `bangle`, `slim_bangle` | `has_chain=ABSENT`、`has_rigid_ring=VERIFIED` |
| 耳饰 | `earring` | 耳针 / 耳夹均 UNKNOWN |
| 戒指 | `ring` | `has_rigid_ring=VERIFIED` |
| 抓夹 | `claw_clip` | `has_teeth_or_jaw=VERIFIED`、`has_hinge_or_spring=VERIFIED` |
| 发簪 | `hair_pin` | `has_pin_shaft=VERIFIED` |
| 发圈 | `scrunchie`, `hair_tie` | `has_elastic_band=VERIFIED` |
| 发箍 | `headband` | `has_band_arch=VERIFIED` |
| 缎带 | `ribbon` | `has_ribbon_bow=VERIFIED` |
| 未知结构 | `hair_accessory_generic` | `structure_facts={}` = 全 UNKNOWN |

### 三态证据机制（UNKNOWN 不当作 ABSENT）

- 三态常量：`VERIFIED` / `ABSENT` / `UNKNOWN`。
- `part_evidence_terms`：13 个部件的 `part_label` + `positive_terms` +
  `negative_terms`。**否定词优先扫描**，否则「无吊坠」会因为含「吊坠」被判为 VERIFIED。
- `resolve_part_evidence(part_key, structure_facts=…, anchor_texts=…)`：
  注册表有意见就以注册表为准；否则扫锚点；都无 → `UNKNOWN`。
- `part-gated actions`：合同声明 `part_gated_actions`，只在对应部件被证据确认
  `VERIFIED` 时才由 `resolve_part_gated_actions` 发放。`UNKNOWN` 与 `ABSENT` 一律
  不发放，回落到子类型基础动作（基础动作从不点名该部件）。
- **证据在 seed 边界冻结一次**：`build_simplified_creative_seed()` 调
  `attach_mixed_part_evidence()`，把结果写进 extension 的 `part_evidence` 键，
  蓝图指引与渲染器共用同一份，不再各自重算。
- **锚点权威**：`anchor_evidence_texts()` 只读 `hard_anchors`，**不读
  `display_anchors`**。后者可能是模型自造的展示建议（如「手持展示吊坠」），
  把展示想法当商品事实正是要防的伪造。
- **编译期硬错误**：`validate_mixed_template_contract()` 末尾新增
  `ERR_MIXED_PART_CONTRADICTION` —— 若动作里出现本品类已 `ABSENT` 的部件词，
  直接报错，不再靠人工复查。

### 未确认结构一律禁止展示

`withheld` 原来只统计「被证据驳回的 gated 候选」，于是**没有声明 optional action 的
子类型**（通用发饰，或夹体/铰链均为 UNKNOWN 的发夹）不会收到任何指令，等于默认
允许模型自由发挥背面结构。现改为：

```
withheld  = 未发放的 gated 候选数
unconfirmed = 声明了 UNKNOWN 部件，或 structure_facts 为空（整体未确认）
fallback 且 (withheld > 0 或 unconfirmed) → 输出「未确认部件不要展示：<fallback>」
```

同时把类目层最后一处结构写死清掉：HAIR 的 `quantity_rule` 由「不得生成参考图未
提供的**背面结构**」改为「授权锚点未确认的结构一律不生成，只呈现画面里实际可见的
部分」——禁令保留，但不再由类目层断言某个具体部件。

**新守卫用例抓出的真实泄漏（5 处，已全部修正）**：

| 位置 | 修复前 | 修复后 |
|---|---|---|
| `WRIST.base_action` | 展示**链节与吊坠**垂落 | 展示腕饰本体与手腕的贴合关系 |
| `WRIST.quantity_rule` | **细链**手链与**刚性手镯**不得互相冒充 | 同一类目下不同物理形态的商品不得互相冒充 |
| `WRIST.action_by_module.WORN_DETAIL` | 展示**链节与吊坠**垂落状态 | 展示腕饰本体与手腕的贴合关系 |
| `WRIST.action_by_module.HANDHELD_PRODUCT` | 展示**链节**走向与**搭扣**外观 | 展示本体造型与表面轮廓 |
| `WRIST.action_by_module.STATIC_PRODUCT` | 呈现**链节**垂落与金属反射层次 | 呈现本体轮廓与金属反射层次 |

`forbidden_actions` 有意不在守卫范围内：「扣细搭扣」是**禁令**而非「存在搭扣」的
**断言**，不构成结构伪造。

### 一处语义收紧：无法作答的投影返回空

`project_module_framing` 现在在两种情况下返回 `{}`：模块未知，**以及模块为
`CATEGORY_ZONE` 源但类目未知**。后者原会返回一份「允许范围为空的投影」，
消费者会把「没有规则」当成「什么都不许」，属于静默默认值；返回 `{}` 强制调用方
显式决定。`compile_mixed_template_contract` 在更早处已对不支持的品类抛错，
故本收紧不影响任何生产路径。

---

## 二、改动文件

| 文件 | 行数 | 改动 |
|---|---|---|
| `config/accessory_mixed_templates.json` | 846 | + `_module_framing_note` / `module_framing_rules`(4) / `_physical_subtype_note` / `part_evidence_terms`(13) / `physical_subtype_rules`(13)；4 个类目 + `module_framing`(2 行) + `visible_quantity_rule`；WRIST 与 HAIR 类目文案去结构写死 |
| `core/accessory_mixed_templates.py` | 1801 | + `physical_subtype_rule` / `subtype_structure_facts` / `project_module_framing` / `module_framing_projection` / `worn_body_framing` / `module_framing_legend_lines` / `resolve_part_evidence(_map)` / `resolve_part_gated_actions` / `anchor_evidence_texts` / `attach_mixed_part_evidence`；重写 `compile_mixed_template_contract` 与 `render_mixed_blueprint_guidance`；`validate_mixed_template_contract` 新增部件矛盾硬错误 |
| `core/category_execution/accessory.py` | 2124 | `_mixed_template_blueprint_guidance` 读取冻结的 `part_evidence` 并透传给蓝图指引 |
| `core/production_script_renderer.py` | 2289 | `_face_free_constraint_lines` 重写（佩戴禁令 / 佩戴允许 / 模块禁令三段）；「出镜方式」行改用 `worn_body_framing` |
| `core/simplified_complete_script.py` | 3816 | `build_simplified_creative_seed` 在 extension 边界调用 `attach_mixed_part_evidence` 冻结证据 |
| `tests/test_accessory_mixed_templates.py` | 1019 | + `ShippedModuleFramingIsCompleteTest` / `ShippedSubtypeRulesAreCompleteTest` / `ModuleProjectionReadsBackConsistentlyTest` / `PairedEarringCropRuleTest`（+16 项） |
| `tests/test_accessory_mixed_integration.py` | 1084 | + `ModuleFramingPerShotTest`(T05) / `PartEvidenceGatingTest`(T06) / `RendererFramingPerCategoryTest`（+2 项未确认结构守卫） |

未改动：`core/original_batch_allocator.py`、`core/original_batch_executor.py`、
`core/first_frame_contract.py`、`core/visual_execution_contract.py`、
`core/video_prompt_compaction.py`（分别属于阶段 E / C / D）。

---

## 三、回归用例与实际结果

| 用例 | 覆盖 | 关键断言 | 结果 |
|---|---|---|---|
| T05 | Review #5 | 每镜 `view_scope`/`view_label`/`state_boundary` 齐全；手持镜与静物镜 `body_zone=""` 且不继承佩戴部位；佩戴镜读本类目区域；每镜允许范围与禁用范围**不重叠**；类目取景用语必须与本类目词表有 2-gram 交集（防跨区域词泄漏） | ✅ |
| T06 | Review #6 | 13 个 canonical type 全有子类型规则；`structure_facts` 只用三态；动作不得点名本子类型已 ABSENT 的部件；`bracelet` 有锚点→发放吊坠动作、`has_clasp=ABSENT`→不发放；无锚点→全部 UNKNOWN→0 发放；未确认结构必有禁令行 | ✅ |
| T07 | Review #6 | 「未确认部件不要展示」在**没有 gated 动作**时也必须发出；指引里不得有任何「要求展示未确认背面」的指令（禁令行除外）；类目层文案不得写死任何结构部件词 | ✅ |
| T18(部分) | 任务书 §三-A.1 | 长视频作用域 → `LONGFORM` 且拒绝（阶段 A 已覆盖） | ✅ |

**完成标准验收探针**（`wp0/stage_b_probe.py`，只读，隔离环境，13 类商品 × 4 镜）：

```
标准 1：四类目 × 三种承载无自相矛盾裁切          PASS（13 品 × 4 镜 × 3 断言）
标准 2：实心手镯不出现链节/吊坠/搭扣动作          PASS（含反向对照：手链确实提到链节）
标准 3：结构未知的商品不被要求展示未知背面        PASS（200+ 行指引逐行扫描）
------------------------------------------------------------
合计 197 项断言，0 失败
```

**全量相关回归（隔离环境，临时数据目录）**：

```
test_accessory_mixed_templates.py      PASS  66   (原 50 + 阶段B 16)
test_accessory_mixed_integration.py    PASS  56   (阶段A 36 + 阶段B 20)
test_category_execution_adapter.py     PASS  34
test_production_script_renderer.py     PASS  27
test_simplified_complete_script.py     PASS  53
test_visual_execution_contract.py      PASS   8
test_first_frame_contract.py           PASS  12
test_original_batch_allocator.py       PASS  47
test_original_batch_executor.py        PASS  16
test_original_batch_report.py          PASS   8
test_video_prompt_compaction.py        PASS  21
------------------------------------------------
合计 348 项，0 失败；三个生产库 md5 逐字未变
  original_script_generator.sqlite3     e0024aa776bad3020379702d4f4dff46  [未变]
  longform_original_video.sqlite3       6e150904fab2fa1dc172941e72de9876  [未变]
  short_video_auto_publish.sqlite3      50529ae02f733c99cad6caf116bfb5b1  [未变]
```

---

## 四、未完成 / 下一步

阶段 B 的完成标准（任务书）："四类目 × 三种承载无自相矛盾裁切；实心手镯不出现
链节/吊坠动作；结构未知的商品不被要求展示未知背面" → **三条均已由探针逐条机器化
验证并通过**。

后续阶段（尚未实施）：

- **C** Review #4、#7、#8：首帧 / 光线 / 中央口播统一读取冻结合同
- **D** Review #9：最终提示词压缩不得丢镜头约束
- **E** Review #3：按**最终镜头动作**做批次与历史差异判断

注意：阶段 E 依赖阶段 B。差异签名要按「最终镜头动作」计算，若 B 未修完，
就是在给废弃的旧动作（"展示链节与吊坠垂落"）算签名。实施顺序不可调换。
