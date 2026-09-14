# VN 围巾线「参考图」失效复核（2026-09-14）

- 复核对象：生产预设 `图文｜VN｜围巾搭配四选一` → 配方 `PHOTO_MATCHING_CHOICE_V3`
  （profile `scarf_four_looks`），飞书行 `recvv6G1bQ9aTs`
- 仓库：`/Users/likeu3/.openclaw/workspace`，包 `packages/organic_photo_video`
- 触发：用户指出「越南围巾线参考图根本没起到作用」
- 结论：**用户判断成立，而且比"没起作用"更严重 —— 参考图连流水线都没进。**

---

## 1. 一句话结论

这行原本的配置是 **`参考图类型=风格参考` + 3 张真实参考图（6.6/5.9/5.7 MB）**。
该组合在搭配线上会**在开工前直接报错**；我们上一轮把它改成「0 张参考图 + `素材状态=已匹配可用素材`」
才跑通 —— 而那个改法**同时关掉了参考图唯一可能生效的通道**。

交付的那一行，4 张成片素材与素材集 `ASSET_VN_SCARF_CHOICE_V1` 声明的 4 张 look
**逐字节相同（sha256 全等）**，证明出图 100% 来自素材集复用，参考图贡献为**零**。

作为对照，同一天的 TH 旅行行 `recvv6HdBYmQpk` 用 3 张参考图驱动了 **4 张真实生图**
（`1route / gpt-image-2.5-sunburst`）。同一条产线，参考图在旅行线是活的，在围巾搭配线上是死的。

> **措辞更正（2026-09-14 二次核查，推翻本文初稿的 §7.4 结论）**：
> 这**不是**「方案要求 STYLE、代码没实现」，也**不是**「声明越界、没有落点」。
> `services/feishu_workflow.py:1596-1695` 有一段**完整的 STYLE 落点**（真调
> `PhotoStyleReferenceSupplyService.prepare(reference_paths=...)` 用参考图生 look），
> 且它与旅行线**共用同一段代码**，不依赖 `travel_two_step`。
> 准确的表述是：**能力在，入口被两道「必须选主题」的硬拦锁住**（`:1363` 与 `:1598`，
> 同一个报错文案各写一遍）—— 而方案 §5.4 的运营输入**只有三个字段
> （参考图类型 / 参考图 / 产品编码），根本没有「图文主题」**。
> ⇒ 偏差在**「多要求了一个方案没有的字段」**，不是「少写了一整条生产线」。
> 详见 §2.3 与 §7.4；裁决结果见 **§6.0**，W1 改了什么与验收结果见 **§6.1 / §6.3**（**已实施**）。
> 另有一条与方案 §5.2/§5.4/§10 **直接冲突**的商品模式问题（§7.2 第 2 行）。

---

## 2. 事实核验（逐项可复现）

### 2.1 行的原始配置（`tmp/real_gen_e2e_3rows/vn_row_backup.json`）

| 字段 | 值 |
|---|---|
| `参考图类型` | **`风格参考`** |
| `参考图（可选）` | **3 张**：`WS45bvaHTouJ2ixCe0tcnHSMn2g`(6.6MB)、`RROjbKneWo5gHbxQHuycmcYrnpe`(5.9MB)、`XwEMb7lfsoHHoTxEQGpcKbIVnNb`(5.7MB) |
| `素材状态` | 空 |
| `图文主题` | 空 |

> 注意：`参考图类型` 被**显式**选成「风格参考」，不是「自动判断」。这不是误操作，是明确意图。

### 2.2 参考模式解析（实测 `resolve_reference_mode()`）

按该配方 `asset_requirements.required_roles = [look_a..look_d]`（4 个角色）、`生成篇数=1`：

| 输入 | 解析结果 |
|---|---|
| **`风格参考` + 3 张（原配置）** | `STYLE` |
| `自动判断` + 3 张 | `STYLE` |
| `自动判断` + 4 张 | `COMPLETE_LOOK` |
| `完整穿搭` + 3 张 | **抛错**：完整穿搭模式生成 1 篇需要按每篇 look_a/look_b/look_c/look_d 上传 4 张图片 |
| `完整穿搭` + 4 张 | `COMPLETE_LOOK` |

即：原配置落在 `STYLE`。

### 2.3 为什么 `STYLE` 在搭配线上直接死：两道「必须选主题」，而方案没有主题字段

**第一道 —— 规划前（`services/feishu_workflow.py:1363`）**

```python
if reference_mode == REFERENCE_MODE_STYLE and recipe_for_input:
    if theme is None:
        raise FeishuWorkflowError("风格参考模式需要选择图文主题")
```

**第二道 —— 素材供给前（同文件 `:1596`，同一个报错文案又写了一遍）**

```python
if (reference_mode == REFERENCE_MODE_STYLE and recipe_for_input
        and asset_status not in {"已确认，正在生成", "已匹配可用素材"}):
    if theme is None:
        raise FeishuWorkflowError("风格参考模式需要选择图文主题")
```

行上 `图文主题` 为空 → 第一道就抛错。**这就是上一轮那条报错的出处，参考图一张都没用上。**

**关键事实（本文初稿漏掉的一条）**：第二道的**下面**就是 STYLE 的完整落点 ——

```python
paths = style_reference_paths
...
supply_service = PhotoStyleReferenceSupplyService(
    generator=self.generator, root=staging_root, vision_service=self.photo_reference_vision,
)
prepared = supply_service.prepare(
    record_id=item_id, reference_paths=paths, theme=theme,
    account=account, persona=persona, variation=variation, ...
)
```

它是**真把参考图喂进生成**的（不是只做文本分析），而且**不判断 `travel_two_step`**，
因此旅行线与搭配线**共用这一条通路**。⇒ STYLE 对搭配线**不是没有落点，是落点被 theme 拦在门外**。

**而方案 §5.4 的运营输入只有三个字段**（原文引用见 §7.1）：
`参考图类型 = 风格参考` / `参考图 = 1-3 张` / `产品编码 = 空`（A）或 `= 围巾编码`（B）。
**没有「图文主题」这一项。** 所以「必须选主题」是**实现比方案多要求了一个字段**。

### 2.4 交付后的状态：参考子系统被整体绕过

`services/photo_reference_context.py:91` —— **只有附件非空才解析模式**：

```python
if unified_attachments:                      # 附件为空时整段跳过
    reference_attachments = list(unified_attachments)
    reference_mode = resolve_reference_mode(
        selected_type=selected_type,
        attachments=reference_attachments,
        ...
    )
```

于是 0 张附件 ⇒ `reference_mode = ""`（空串）。而下游三个「真正会用到参考图」的供给分支
**都以非空 `reference_mode` 为前提**：

| 行 | 分支 | 条件 |
|---|---|---|
| `:1488` | COMPLETE_LOOK 合成 | `reference_mode == COMPLETE_LOOK` |
| `:1596` | **STYLE 风格参考供给（唯一用参考图生 look 的分支）** | `reference_mode == STYLE` |
| `:1718` | PRODUCT 商品供给 | `reference_mode == PRODUCT` |

空串时三条全不命中 ⇒ 不 stage 参考图、不调风格分析、不做内容规划，
直接走到 `:1829 build_batch` 用**素材集**出图。这解释了本轮 VN 行 **0 付费生图成本**。

### 2.5 产物证据：参考流水线一次都没跑

同一行 id 在四个参考相关目录下的存在性：

| 目录 | `recvv6G1bQ9aTs`（VN 围巾） | `recvv6HdBYmQpk`（TH 旅行，对照） |
|---|---|---|
| `style_references/<rid>/` | **无** | 有（3 张 staged 参考图 `01_*.jpg`…） |
| `reference_contracts/<rid>/` | **无** | 有（`reference_analysis.json` + `travel_plan.json`） |
| `style_reference_supply/<rid>_item_1/` | **无** | 有（`status=complete`，4 张 look，`1route/gpt-image-2.5-sunburst`） |
| `content_plans/<rid>/` | **无** | 有 |

### 2.6 出图来源：逐字节等于素材集（决定性证据）

`photo_packages/opv_task_20260913_936bcb19f694/shots/*_reuse.png` 的 sha256，
与 `config/asset_sets/ASSET_VN_SCARF_CHOICE_V1.json` 声明的 4 张 look 哈希**全部相等**：

| 镜头 | sha256 | 对应素材 |
|---|---|---|
| `..._P1_v1_reuse.png` | `5961805ae7803bf00a528d68dbdb283efbec03addfb17e4488a27162ec1f2556` | `vn_scarf_a` |
| `..._P2_v1_reuse.png` | `944dc80067437ece390abb503697a5074e72b96e217855a2640bcb4ecddbdf7e` | `vn_scarf_b` |
| `..._P3_v1_reuse.png` | `f66e0bedfdbff3d16f331d908b24499c6f9330f91410a29b6a73f4771157f6c2` | `vn_scarf_c` |
| `..._P4_v1_reuse.png` | `e48274d417d3c9076b2171e1cc92207ec7984c8be88dd77d74fbf505a3666f03` | `vn_scarf_d` |

文件名后缀 `_reuse.png` 亦与 `generation_provider=asset-reuse` 口径一致（SKILL §5.1）。

---

## 3. 根因：`素材状态` 这个字段一身二职

这是本轮最值得记的一条，它不是配置笔误，是**闸门语义冲突**。

**身份一 —— 搭配线的入场券。** `services/feishu_workflow.py:1299`

```python
if (requires_product_supply and not reference_attachments and not product_id
        and asset_status not in {"已确认，正在生成", "已匹配可用素材"}):
    raise FeishuWorkflowError("该图文预设需要填写产品编码或上传参考图")
```

带 `outfit_supply` 的配方（搭配线就是）必须三选一：产品编码 / 参考图 / **`素材状态 ∈ {已确认，正在生成, 已匹配可用素材}`**。

**身份二 —— 流水线自己的「资产已就绪，别重做」标记。** `:1967`

```python
self._write_fields(record.record_id, {
    FIELD_EXECUTE: False, FIELD_PROGRESS: PROGRESS_RUNNING,
    FIELD_REVIEW: REVIEW_PENDING, FIELD_NOTES: "", FIELD_PHOTO_SUMMARY: summary,
    FIELD_PHOTO_ASSET_STATUS: "已匹配可用素材",
})
```

而且参考供给分支都**显式排除**它：

| 行 | 条件 |
|---|---|
| `:1505` | `asset_status not in {"已确认，正在生成", "已匹配可用素材"}` |
| `:1596` | 同上 |

**⇒ 结论**：为了让搭配线过 `:1299` 而填 `素材状态=已匹配可用素材`，
等于同时告诉下游「素材已备好，不要再 stage / 不要再生成」——
**用参考图那一项去过闸门，反而会关掉参考图自己的通道**。
正确用法是「用参考图就去过参考图那一项」，而不是填这个标记；但字段说明完全没提示这一点，
是个很容易踩的坑。

---

## 4. 判定矩阵：VN 围巾线四种配置各会发生什么

| # | 配置 | 结果 | 参考图是否起作用 |
|---|---|---|---|
| 1 | **`风格参考` + 3 张 + 无主题**（原配置） | 开工前抛错「风格参考模式需要选择图文主题」 | ❌ 一张没用上 |
| 2 | 0 张 + `素材状态=已匹配可用素材`（**本轮交付**） | 纯素材集复用；`reference_mode=""`，参考子系统整体绕过 | ❌ 完全不参与 |
| 3 | `风格参考`/`自动` + 3 张 + **选主题** | 走 STYLE：真用参考图 + 人物模板生 4 张 look（付费） | ⚠️ 图能生效，**但文案有 locale 风险**（见下） |
| 4 | **`完整穿搭`/`自动` + 恰好 4 张 + 无主题 + 无素材状态标记** | 4 张参考图**直接当 4 个 look**（`:1505` stage 后 qualify） | ✅ 生效，且文案走 VN 文案包 |

### 关于 #3 的文案风险（代码事实）

- `plan_th_choice_batch` 在 `:1456` 被调用时**没有传 `locale_pack`**（该参数默认 `None`）。
- `_family_plan`（`photo_content_planner.py:192/221`）在 `locale_pack=None` 时读的是家族的
  **内联 `title_th/cover_th/caption_th`**（`photo_locale.py:111-120`）。而 VN 搭配线的 policy
  **已把这些字段剥离**（见该 policy 的 `_locale_note`）⇒ 拿到的是**空串**，**不是泰语**。
  空串随后被 `build_theme_copy`（`feishu_workflow.py:1849-1852`）的 `or theme["title"]` 兜成**泰语**；
  同时 look 标签硬取 `th-TH`（`photo_theme.py:279`）、`cta` 取 `theme["cta"]`
  （`photo_theme._PROFILES` 四个可选主题的 cta **全是泰语**）。
- 若走 `_vision_plan` 分支（`analysis_method == "doubao_seed_2_1"`），文案改为视觉模型自由撰写，
  但 `PhotoReferenceVisionService.analyze()` 调用处（`:1416`）**同样没有语言参数** → VN 文案无保证。

⇒ 也就是说 **#3 这条唯一能让「风格参考」生效的路，会牺牲越南语文案**，与
`MATCHING_CHOICE_V3.json` 里 `_locale_note` 自己写的「越南语无泰语泄漏」目标相悖。

---

## 5. 影响面

- **直接**：VN 围巾搭配线（`PHOTO_MATCHING_CHOICE_V3`）——「风格参考」这一选项对它是死路。
- **同类**：任何 `outfit_supply` + `NO_PRODUCT` + `reference_contract_v1` 的搭配线共用这条逻辑
  （现有配置里只有这一条，但它是模板）。
- **不受影响**：旅行线（`travel_two_step`，走 `:1378` 的旅行分支）参考图工作正常，TH#1 已验证。
- **既有交付未受影响**：本轮 VN 行的 5 页成片、RDS 落库、飞书回写均正常；
  素材集复用的 4 张 look 本身是过验素材。问题在「参考图白配了」，不在成片质量。

---

## 6. 裁决结果与落点清单（2026-09-14 用户已裁，**未实施**）

### 6.0 五个裁决

| # | 问题 | 裁决 | 含义 |
|---|---|---|---|
| 1 | 方案 §5.4 的 A/B/C 覆盖哪条线 | **两条线都算** | 「风格参考」是搭配线的**承诺能力** ⇒ 按方案**去掉「必须选主题」**，属**补实现**而非现场打补丁 |
| 2 | 商品模式与方案的冲突 | **按方案走：放行商品** | 搭配线要能排「风格参考 + 围巾商品编码」（§5.4 B，标为 V1 推荐） |
| 3 | 已跑的那一行 | **保留交付** | 不动已交付行（5 页成片 / RDS / 飞书回写均已验）；改动只影响**后续**运行 |
| 4 | 无主题时 4 套 look 从哪来 | **由视觉模型照参考图产出** | 沿用**已有**通路（`analyze()` → `recommended_sets` → 规划器 `:451` → `_vision_plan`），不新写规划流 |
| 5 | 商品改造是否本轮一起做 | **分开做** | 本轮只交付「参考图生效」，商品语义（§6.2 #2）单独排期 |

> 「`风格参考` 对该线是隐藏还是保留」这个连带问题**自动消解**：按裁决它应当**可用**，所以保留，且必须真能跑通。

### 6.1 W1 **已实施**：让参考图在搭配线真正生效

**设计（按裁决 4）**：STYLE + 无主题时，4 套 look 由**视觉模型照参考图产出** ——
沿用**已有**通路 `analyze()` → `recommended_sets` → 规划器 STYLE 分支 → `_vision_plan`；
**发布文案与 look 标签改由 Locale Pack 提供**。

| # | 位置 | 改动 |
|---|---|---|
| 1 | `feishu_workflow.py`（`_generate_native_photo`，`planning_flow` 之后） | 新增 `theme_optional`（国家无关＝已绑定 Locale Pack，且**非**旅行、**非**温度分层/冷热切换）、`theme_supplied`、`effective_theme` 三个变量 |
| 2 | `:1363-1365` / `:1598-1599`（**同一个报错文案的两道**守卫） | 条件由 `theme is None` 改为 `effective_theme is None` |
| 3 | `:1429` | 内容规划门槛 `theme is not None` → `effective_theme is not None`；`input_contract["theme_key"]` 改安全取值 |
| 4 | `:1456` 调用 `plan_th_choice_batch` | **补传 `locale_pack`**（原漏传 ⇒ 文案空串） |
| 5 | `:1418` / `:1657` / `:1693` | `analyze()` / `prepare()` / 素材 metadata 改用 `effective_theme` |
| 6 | `:1849` | `if theme:` → `if theme_supplied:` ⇒ 未选主题时**不再**用主题文案整段覆盖 `request["copy"]` |
| 7 | `photo_content_planner.py`（`plan_th_choice_batch`） | 已绑定 Locale Pack 且 `theme_key` 为空时**放行**主题白名单 |
| 8 | `_vision_plan` | 新增 `locale_pack` 入参；**looks 保留模型产出**，`copy` 改取 `complete_look_copy[0]` + `labels.generic.cta`，look 标签改用 `labels.generic.look_label`；**仅对非旅行流程生效** |
| 9 | `_family_plan` / `theme_family_order` | `theme[...]` 直接下标改安全取值；无主题又落到家族路径时给**明确错误**，而不是让它以 KeyError 暴露 |

新增 `_neutral_four_choice_copy()`：模型方案没有 policy `family_id` 可挂文案，
故复用 Locale Pack 为 `COMPLETE_LOOK` 备的四组中性文案。

**为什么文案不能交给模型**：`PhotoReferenceVisionService._analysis_prompt` 明写要
「泰语标题 / 两行以内泰语封面文案 / 自然泰语短文案 / 泰语互动句」，
且 `_normalize_contract`（`:2089-2090`）缺任一字段直接抛「第 N 篇缺少泰语文案」。
⇒ 「模型编 look + 模型写文案」在越南语线上**必然出泰语**，所以必须把文案拆出来。

**作用域（零漂移的关键）**：规划器里的 Locale Pack 覆盖**只对非旅行流程生效**
（`locale_pack=None if travel_flow else locale_pack`）。因为 `PHOTO_TRAVEL_OUTFIT_V3`
**也**声明了 `locale_copy_packs`（`th-TH` + `vi-VN`）—— 若不限定，TH 旅行线的 look 标签会被改。
首次实现漏了这个限定，正是它让 3 个 `VN_SCARF_*` canary 的 `plan_sha256` 变了。

### 6.2 W2 落点清单：搭配线放行商品

| # | 文件 | 现状 | 待办 |
|---|---|---|---|
| 1 | `config/recipes/PHOTO_MATCHING_CHOICE_V3.json` | `recipe_spec.product_modes = ["NO_PRODUCT"]` | 加入 `PRODUCT`。`photo_recipe_contract.py:182` 拿它做白名单，不改则预设 `default_product_mode=PRODUCT` 会被拒 |
| 2 | `photo_content_planner.py:183-186`（`_family_plan` 的 `PRODUCT` 分支） | 把商品当**外套替换**（`"目标商品外套；严格保持商品参考图…"`） | 围巾是 `accessories`（方案 §4 `category.main_product_slot = "accessories"`）。按 §5.2「一篇只允许一个 `product_id + variant_key`」与 §5.3「指定商品时围巾必须出现在 look_a..look_d、不新增独立商品详情页」，应改为**围巾出现在 A/B/C/D 四套里**，而不是替换外套 |

> W2 #2 是**语义设计**（不是配置开关），建议与 W1 **分开排期**，
> 否则会把「参考图生效」这个可独立验收的目标拖进一个大改动里。

### 6.3 验收结果（W1）

| 验收项 | 结果 |
|---|---|
| 全量回归 | `Ran 1220 tests` — **0 fail / 0 error**（原 1215 + 本轮新增 5） |
| 零漂移 | 首版未限定作用域时**确实出现 3 个 canary 失败**；限定为"非旅行流程"后全部恢复，**旅行线与 canary 黄金基线一个字节都没改** |
| 新增字段级测试 | `tests/test_photo_vn_activation.py::VnMatchingLineAcceptsSpecOperatorInputTest`（5 条）：无主题 + 模型给方案可规划、文案越南语、模型泰语标签被换掉、无 Locale Pack 时仍拦截、主题缺失落到家族路径时给明确错误 |
| L1 干跑（**不付费**） | `tmp/real_gen_e2e_3rows/l1_vn_matching_no_theme.py`，**11/11 通过**（明细见下） |
| 真实端到端 | **未跑**（会付费生图）。建议 W1 得到确认后再另排一行真跑 |

L1 干跑明细（11 项全过）：

- `theme_key=""`；4 套 look 来自视觉合同（`外套 A/B/C/D`）；`family_id=vision_dynamic_1`；
- 发布文案：`title=Hôm nay chọn set nào?` / `cover=A B C hay D?` /
  `caption=Hôm nay bạn thích look A B C hay D nhất?` / `cta=Bạn thích look nào?`，**无泰语**；
- look 标签由模型的泰语 `ลุค A` 被替换为 `Look A`；
- `prepare()` 在**中性主题**下跑通，产出 **4 个生成请求**，参考图数 **3/4/4/4**
  ⇒ 参考图真的进了生成，不只是文本分析；
- `provider` 全为 `stub` ⇒ **零付费**；视觉模型调用记录 `executed=False`。

> 注：`prepare()` 第二次运行会因 `supply_manifest` 已 complete 而走「续跑复用」直接返回
> 上次的 sources（这是它**应有**的行为）。自证脚本因此每次先清空自己建的暂存目录，
> 否则会看到 0 个生成请求而误判「参考图没进生成」。

### 6.4 留档：裁决前的候选（原文）

- **A. 对齐声明**（把 policy 的 `supported_reference_modes` 收紧）—— **已被裁决否定**：
  用户裁「两条线都算」，说明 STYLE 是该线的承诺能力，不该从白名单里摘掉。
- **B. 让风格参考真正可用** —— **本轮采纳方向**，落点见 §6.1。
- **C. 只记录不修** —— 已否定。
- **D. 商品模式对齐方案** —— 已采纳，落点见 §6.2。

---

## 7. 方案原文对照：「原始方案支持风格参考」指的是哪条线

用户指出「原始方案里是支持风格参考的」。**这句话成立，但要精确定位** ——
方案里 STYLE 的落点是**旅行线**；而这一行跑的是**搭配线**。

### 7.1 方案怎么说的（原文引用）

| 出处 | 原文要点 | 指向 |
|---|---|---|
| §1.1 必须实现 | 「围巾使用参考图的方式与当前泰国旅行线一致」 | 参照物是旅行线 |
| §2.1 参考图 | 「`自动判断 / 风格参考 / 商品参考 / 完整穿搭` 现有字段值兼容」；「显式 `STYLE + product_id` 时，风格图仅作为灵感，商品图包作为商品身份权威」 | 通用兼容性要求 |
| §5.2 两条内容线 | 围巾日常搭配 → `MATCHING_CHOICE_V2 × SCARF_V1`；围巾旅行穿搭 → `PHOTO_TRAVEL_OUTFIT_V3 × SCARF_V1 × EAST_ASIA_COOL_V1` | 两条线**分开定义** |
| §5.4 运营输入 A/B/C | A 纯风格（`风格参考` + **1-3 张** + 产品编码空）/ B `风格参考 + 产品编码`（标为「V1 推荐」）/ C `完整穿搭` + 4 张；执行优先级写「商品图包 > **冻结旅行计划** > 风格参考 > 模型自由发挥」 | **原文一个字都没提「图文主题」**；但正文是旅行线语义 ⇒ 曾有范围歧义。**用户已裁：覆盖两条线（§6.0）** |
| §8.3 组合测试 | 7 行组合里 **6 行是 `V3`**；scarf 的三行是 `V3 \| scarf \| VN \| STYLE`（无商品 / 有商品 / COMPLETE_LOOK） | 验收矩阵**只有旅行线**，无搭配线 |
| §8.4 视觉 canary | 6 张对照图全部是 V3 场景（Seoul / Sapporo / 同一商品图包跨 TH-VN） | 同上 |
| §14.2 | 「Phase 4 起输出三种参考模式的离线 manifest」 | 未指明哪条线 |

**Phase 4 实际交付的三个 canary fixture，`context.recipe.id` 全部是
`PHOTO_TRAVEL_OUTFIT_V3`、`theme_key` 全部是 `COOL_WEATHER_TRAVEL`**
（`tests/fixtures/PHASE4_CANARY_VN_SCARF_STYLE{,_PRODUCT}.json` 与 `_COMPLETE_LOOK.json`，已逐份读取核对）。
⇒ 无论是方案还是 Phase 4 产物，「围巾 + 风格参考」的落点**都在旅行线**；
搭配线在参考模式验收里**一行都没有**。

### 7.2 「支持」从哪来：STYLE 的落点**在**，但入口多了一道方案没有的门

Phase 4 新建了搭配线配方及其 policy（提交 `828fd02`）。三种参考模式的情况**并不相同**：

| # | 声明（配置侧） | 落点（代码侧，已核） | 后果 |
|---|---|---|---|
| 1 | `config/photo_planning_policies/MATCHING_CHOICE_V3.json`：`supported_reference_modes: ["STYLE","PRODUCT","COMPLETE_LOOK"]` | **STYLE 有完整落点** —— `feishu_workflow.py:1596-1695` 真调 `PhotoStyleReferenceSupplyService.prepare(reference_paths=...)` 用参考图生 look，**且不判断 `travel_two_step`**（与旅行线共用同一段代码）。但入口被 `:1363` / `:1598` 两道「必须选主题」挡住；planner 侧 `:398-400` 也恒需 `theme_key ∈ supported_theme_keys` | **不是漏实现、也不是没有落点**：落点在，多出来的那一关（「必须选主题」）方案 §5.4 里**没有这个字段** ⇒ 运营按 §5.4 A 填的组合（风格参考 + 无主题）**必挡** —— 就是那行报错 |
| 2 | 同一 policy 声明支持 `PRODUCT` | 配方 `PHOTO_MATCHING_CHOICE_V3.json`：`recipe_spec.product_modes = ["NO_PRODUCT"]`；预设 `图文｜VN｜围巾搭配四选一` 也是 `default_product_mode=NO_PRODUCT` | 与方案 §5.2「商品模式下一篇只允许一个 `product_id + variant_key`」、§5.4 B（V1 推荐）、§10「5 篇中 2 篇带真实产品」**直接相反** |
| 3 | 方案 §5.4 A 期望「纯风格」可跑 | 搭配线的参考供给还额外被 `素材状态` 的入场券语义关掉（§3 全文） | 即便前两关都过，参考图仍不会进流水线 |

### 7.3 为什么 1215 条测试全绿还漏掉了（"这么大问题"的真正原因）

三层，缺任一层都兜不住：

1. **离线单测把前置条件悄悄补上了。**
   `tests/test_photo_vn_activation.py:84-103 _plan()` **无条件** `theme=_theme(locale_pack)`，
   `_theme()`（`:76-81`）返回硬编码合法主题。于是
   `test_matching_line_plans_in_both_visual_modes`（`:432`，STYLE / COMPLETE_LOOK 各跑一次）**必过** ——
   它验的是「给定主题与 locale 时的规划产物」，**不是**「运营按 §5.4 填字段能不能开工」。
   Phase 4 交付文档据此写下「搭配线 `PHOTO_MATCHING_CHOICE_V3` 另在 `VnOfflinePlanningTest`
   内跑通 STYLE / COMPLETE_LOOK」：**该句在规划器层面为真、在运营字段层面为假。**
2. **那条守卫零覆盖。** `grep -rn "需要选择图文主题" tests/` → **零命中**。
3. **阶段边界把真实验证推后，且没有"按线"要求。**
   Phase 4 明确「不真实生图 / 不写飞书 / 不写 RDS / 不启用任何 VN 生产预设」；
   §14.2 把真实可见产物推到 Phase 5；§9 发布门禁只要求「**单篇**真实发布验证通过」，
   **没有要求每条内容线各跑一次真实端到端**。

⇒ 这类纯声明式缺陷从 Phase 4（2026-09-13）一直活到上一次**第一次真跑 VN 行**才暴露，
中间 1215 条测试全绿。

### 7.4 结论（二次更正，2026-09-14）

- **不是**「方案要求 STYLE，代码没实现」。
- **也不是**本文初稿写的「声明越界、没有落点」—— 那是初稿漏读了 `:1596` 之后那段代码，措辞过重，此处更正。
- **准确结论是四条**：
  1. **STYLE 对搭配线的落点存在**（`feishu_workflow.py:1596-1695`，与旅行线共用同一段代码），且**真把参考图喂进生成**，不是只做文本分析。
  2. 它**多要求了一个方案 §5.4 里没有的字段「图文主题」**（两道守卫 `:1363` / `:1598`），
     使 `风格参考 + 无主题` 这条**方案 A 的原样填法**必挡 —— 这就是运营撞上的那条报错。
  3. `plan_th_choice_batch` 的调用处（`:1456`）**漏传 `locale_pack`**：于是 `_family_plan`
     走 `locale_pack=None` 分支读**已被剥离**的 `title_th/cover_th/caption_th` ⇒ **文案为空串**；
     若有主题，`build_theme_copy`（`:1849`）又整段覆盖，回落到 `theme["title"]`/`theme["cta"]` 与
     `th-TH` look 标签 ⇒ **泰语文案**。两条路都保不住越南语。
  4. 全程**没有任何验收项**要求「运营真实字段 × 每条线 × 每种参考模式」各撞一次；
     `:1363` 那条守卫在 `tests/` 里**零覆盖**（`grep -rn "需要选择图文主题" tests/` → 零命中），
     所以 1215 条测试全绿也兜不住。
- 顺带查实的同类问题：`PRODUCT`（policy 声明支持 / 配方 `NO_PRODUCT`），
  与方案 §5.2、§5.4 B、§10 **直接冲突**，比参考图问题更硬。用户已裁决「按方案走：放行商品」（落点见 §6.2）。

---

## 8. 边界声明

- 本文所有行数据、哈希、目录存在性均为**只读核验**，未修改任何生产行 / 生产库 / 配置。
- #3、#4 两条路径为**代码推演**，本轮**未实跑**（#3 会产生付费生图）。
  若需坐实，建议先用 `PhotoStyleReferenceSupplyService.prepare()` + StubGenerator 做 L1 干跑（不联网、不付费）。
- 未做视觉质检；上文对「参考图是否生效」的判断依据是**流水线参与度与产物哈希**，不是画面美感评估。
- **§7 追加核查全部为只读**：代码 / 配置 / `git log -S` 提交史核验，未运行 planner，
  未改任何配置或生产数据。
- **仍未坐实的一项**：搭配线一旦补上主题，文案是否真会回落到泰语。
  已核到的代码点是 `feishu_workflow.py:1844-1852`（有 theme 时**整段替换** `request["copy"]`）、
  `photo_theme.py:279`（look 标签硬取 `th-TH`）、`:310-313`（兜底 `thai_hook`/`thai_caption`）；
  是否触发取决于规划产出的 `slide_texts` 是否恰为 5 条（`photo_theme.py:295`）。
  建议用 StubGenerator 做 L1 干跑坐实，本文不下结论。
- **§2.3 / §6 / §7.2 / §7.4 的二次核查同为只读**：读 `feishu_workflow.py` `:1340-1700`、
  `photo_content_planner.py` `:44-560`、`photo_locale.py` `:95-150`、`config/loader.py` `:219-236`、
  方案原文 §5.2-§5.5 与 §4，以及 `config/locales/LOCALE_VI_VN_V1.json`、两个配方/policy JSON。
  **未运行 planner、未改任何配置或生产数据。**
- **§6.1 的 W1 已实施并验收**（改 `feishu_workflow.py` / `photo_content_planner.py`，新增 5 条字段级测试，
  全量回归 `Ran 1220 tests — OK`，L1 干跑 11/11）；**§6.2 的 W2（放行商品）按裁决 5 仍待排期，未实施**。
  真实端到端（付费生图）本轮**未跑**。
- 二次核查推翻的是**本文初稿的 §7.4 结论**（"声明越界、没有落点"）；§2.1-§2.6 的产物与哈希证据、
  §3 的字段双重语义、§4 判定矩阵、§7.3 的测试盲区**均不变**。
