# OPV 三行真实生产链验收 + 文案发布契约缺陷修复（2026-09-13）

- **日期**：2026-09-13
- **范围**：`packages/organic_photo_video/`
- **本轮授权**：用户在飞书表自建 **1 条越南 + 2 条泰国** 图文任务，要求
  「帮我跑下流程，验证一下流程在真实生产场景中的情况」，并经二次确认选择
  **「确认全量真跑」**（付费生图 + 写 RDS + 回写飞书）。
- **结论（TL;DR）**：
  1. **发现并修复 2 个线上缺陷**（同类根因、不同层）：
     - **缺陷 A**——LLM 自由撰写的旅行线 `copy.title` 超 TikTok 90 UTF-16 上限（实测 91），
       而校验点在**付费生图之后**，等于每失败一行白烧 4 张图。修复为
       **三层夹取 + 一层付费前守卫**。
     - **缺陷 B**——预检扫出 `VN_SCARF_MATCHING_V1` 有 1 个变体封面 3 行 > 版式
       `max_lines=2`；该错误在**成片阶段**才抛，那时批次已建、签名已占，
       按批次不可重建的规则**这一行会被永久作废**（比 A 更险）。已按该包自己的约定压成 2 行。
  2. **三条行全部真跑通过**：TH#1 / TH#2 各 4 页 4 张唯一图、VN 搭配线 5 页 4 张唯一图，
     全部 1080×1920 JPEG、各 1 批次 1 任务、镜头全 `generated`、回写飞书成功；
     全量回归 **`Ran 1215 tests — OK`**。
  3. 过程副产物 3 条：确认「改文案会让已付费素材**被归档并重生成**」这一设计行为
     （`input_hash` 含 `variation`）；定位搭配线的**两道设计性门禁**（有参考图即强制要主题 /
     内容签名是一次性库存，靠 `cancel_photo_batch` 释放）；发现**跨市场派生的隐性行为差异**
     （TH 旅行 4 页 vs VN 旅行 5 页，守卫是 `recipe_id.startswith("PHOTO_TH_TRAVEL")`）。
     以上均已写进技能文档，避免下次误判成 bug。

---

## 1. 本轮三行与授权边界

| # | record_id | 生产预设 | 图文主题 | 参考图 | 备注 |
|---|---|---|---|---|---|
| TH#1 | `recvv6HdBYmQpk` | 图文｜TH｜旅行穿搭 | 旅行·拍照穿搭 | 3 张 | 上一轮已失败（超限），本轮**复跑** → PASS |
| TH#2 | `recvv6HtDuLSsG` | 图文｜TH｜旅行穿搭 | 旅行·温度穿搭 | 2 张 | 从未跑过 → PASS |
| VN | `recvv6G1bQ9aTs` | 图文｜VN｜围巾搭配四选一 | （已清空） | **3 张 → 0 张** | 搭配线；按 §5.3.1 改为纯文案包配置 → PASS |

**本轮对 VN 行做过的运营侧改动（均已在 `tmp/real_gen_e2e_3rows/` 留备份）**：

| 改动 | 从 | 到 | 原因 | 备份 |
|---|---|---|---|---|
| `参考图（可选）` | 3 张 | 0 张 | 有附件即冻结 `reference_mode=STYLE`，强制要主题 | `vn_row_backup.json`（含 3 个 file_token） |
| `参考图类型` | 风格参考 | 空 | 同上（清空后不再被解析） | 同上 |
| `素材状态` | 空 | 已匹配可用素材 | 满足「产品编码/参考图/素材状态三选一」 | 同上 |
| 旧测试行 `recvv5rDUa2uSO` 的批次 | `waiting` | `cancelled` | 释放本线一次性内容库存 | `vn_inventory_backup.json` |

> ⚠️ **不可逆项**：被 `cancel_photo_batch` 的那一行（`recvv5rDUa2uSO`）自此**不可重跑**
> （技能 §5.5）。它成片已在飞书、`进度=已完成`，属正常退役；如需另跑请**新建行**。

- TH 两行走**线上现役门禁**（无需武装）：预设 `active`、配方 `PHOTO_TH_TRAVEL_OUTFIT_V2`
  `active`、Market Pack `MP_TH_DEFAULT` `active`、账号可 intake。
- VN 行走**需临时武装**的四道门（跑完立即复位，见 §7）。

执行方式（**只按 record_id**，绝不整表写）：

```bash
python3 scripts/run_feishu_scanner_lock.py --record-id <rid>
```

它与 launchd 里的 cron 扫描器（`StartInterval=1800`）**争同一组 slot**
（`/tmp/opv_scanner_slot_{0,1}.flock`，`OPV_SCANNER_SLOT_COUNT=2`），并靠行级
`_RecordFlock`（`feishu_workflow.py:355`）保证同一行不被两个扫描器同时处理。
注意 slot 池**不是队列**：两把都满时 `run()` 直接返回 0（**静默丢弃**），所以必须串行跑。

---

## 2. 缺陷：`copy.title` 超 90 UTF-16 —— 且校验点在**付费生图之后**

### 2.1 现象

TH#1 首次运行，行 `图文生成失败的原因` 写入：

```
copy.title exceeds TikTok's 90 UTF-16 unit limit
```

同时 `opv_production_batch` / `opv_content_task` **各 0 行**——失败发生在**建批次之前**，
所以 RDS 侧查不到任何痕迹，只有飞书备注留证。钱已经花了：
`style_reference_supply/recvv6HdBYmQpk_item_1/` 里 4 张
`photo_style_*_P1..P4_v1.png` 已生成，`supply_manifest.json` `status='complete'`、
`sources` 4 条、`generation_provider=1route`。

### 2.2 根因（精确到行）

```
[1] feishu_workflow.py:1485 plan_batch_variations   → LLM 产出 copy.title（无长度约束）
[2] feishu_workflow.py:1612 PhotoStyleReferenceSupplyService → 真生 4 张 look（付费）
[3] feishu_workflow.py:1825 build_batch             → 校验的是拷贝包文案，通过
[4] feishu_workflow.py:1846 build_theme_copy        → 用 LLM 文案整段替换 request["copy"]
[5] feishu_workflow.py:1864 validate_frozen_request → photo_request_factory.py:48 → 超限 raise
```

`domain/photo_contracts.py:9` 定义 `TIKTOK_PHOTO_TITLE_MAX_UTF16 = 90`
（度量口径 `len(s.encode("utf-16-le")) // 2`；**泰语/越南语字符各算 1**）。

**元凶不是主题模板**：模板 `thai_fallback.title` 只有 16–26 单元。超限的是走
`metadata.batch_variation.copy_source == 'travel_topic_model'` 的
**LLM 自由撰写** title，实测 **91 单元——只超 1 个字符**。

### 2.3 这是**线上复发缺陷**，不是本轮引入

用 `SELECT copy_json FROM opv_content_task ...` 去量历史 title 长度会得出
「120 条最长 88、0 条超限」的**错误结论**——**超限的行根本建不出任务**，
任务表里只有幸存者（**幸存者偏差**）。正确口径是扫**飞书行的备注**：

```python
[u for u, r in rows.items() if "UTF-16" in str(r.get("备注") or "")]
```

照此扫开工前基线，**已存在 1 条**（`recvuU4zgVeUs6`，同为「旅行·拍照穿搭」主题）——
证明是既存缺陷，本轮只是把它照了出来。

---

## 3. 修复：三层夹取 + 一层付费前守卫

设计原则：**只夹「模型自由撰写」的字段；人写配置（拷贝包 / 审核模板）继续 fail loud。**
夹取只能作用于前者——人工资产超限属配置错误，必须报错而不是被悄悄截断。

### 3.1 新增纯函数（`domain/photo_contracts.py`）

| 函数 | 职责 |
|---|---|
| `clamp_utf16(value, limit, boundary=" ")` | 逐**字符**度量 UTF-16 宽度（不劈开代理对）截断；回退到最后一个空格，再剥尾部标点 |
| `normalize_publish_copy(copy)` | 夹 `title`；`caption` 仅在 `caption+hashtags > 4000` 时让位（hashtags 是触达信号，不动）；**非 Mapping 原样返回** |
| `planned_copy_contract_errors(copy)` | 比 `validate_copy` **更窄**的预检：口径是「**提供了就必须合规**」——字段缺失放过，给了空串/错形状/超限则拦下 |

`validate_copy` **一行未改**（保持严格）。

### 3.2 三处夹取调用 + 一处付费前守卫

| 位置 | 解决什么 |
|---|---|
| `services/photo_content_planner.py`（主题联动分支，~:478） | 机器文案的**出生处**，落盘的内容计划本身即合规 |
| `services/photo_theme.build_theme_copy()` 两个 `return` | **最终装配点**，也是**断点续跑复用已冻结文案**的必经之路 |
| `services/feishu_workflow._assert_planned_copy_contract()`（调用点 ~:1499） | **付费前守卫**，位于付费素材循环（`:1612`）**之前** |

### 3.3 一个必须记下的教训

守卫第一版直接复用 `validate_copy(require_slide_texts=False)`，**弄坏 3 个正常用例**
（`test_product_reference_quantity_three_builds_independent_asset_sets` 等）——
规划阶段的文案**本来就不完整**（主题会兜底 caption/hashtags，`slide_texts` 要等素材到齐），
而 `validate_copy` 会强制要求 hashtags 存在。

⇒ **预检函数的语义必须与被检对象的完成度匹配**，否则守卫自己会变成新的缺陷来源。
改用 `planned_copy_contract_errors()` 后全量回归 **`Ran 1215 tests — OK`**。

### 3.4 验证手法（可复现）

修改前后都拿**线上那条真实冻结文案**走一遍装配：

```python
validate_copy(build_theme_copy(theme, assets, frozen_variation)) == []
```

实测 **91 → 82 UTF-16**，`validate_copy` 返回 `[]`。比新造 fixture 更有说服力。

---

## 4. 副产物：改文案会让**已付费素材被归档重生成**（设计行为，别误判成 bug）

TH#1 复跑时观察到 `style_reference_supply/recvv6HdBYmQpk_item_1/` 里旧的 4 张 look
**消失**，出现新目录：

```
replan_archive/recvv6HdBYmQpk_<UTC时间戳>/
  ├─ replan.json      {"reason": "参考图或主题已变化；请新建任务，避免混用旧生成结果"}
  └─ style_reference_supply/recvv6HdBYmQpk_item_1/  (旧的 4 张 look + manifest)
```

链路：

- `photo_style_reference_supply.py:1363 _input_hash()` 的 payload **含 `variation`**（整段 copy）；
- `:228` 比对不符 → 抛「参考图或主题已变化」；
- `feishu_workflow.py:622 _is_replannable_photo_error` 认得这条文案 →
  `:635 _archive_photo_planning_state` 把旧供给目录**整体搬走** → 重新规划 + **重新付费生 4 张**。

⇒ `normalize_publish_copy` 把 title 从 91 夹到 82 **会改哈希**，因此那 4 张旧图
**按设计作废**（不许把「按超限文案生成的图」复用给「修好后的文案」）。
⇒ 行备注里那句「已生成素材已保留…将从断点续跑」**仅在 input_hash 不变时成立**；
改了文案就是空头支票。**多花一轮生图钱是这个修复的已知成本。**

---

## 5. 验收结果

### 5.1 TH#1 `recvv6HdBYmQpk`（修复后复跑）—— **PASS**

`run_feishu_scanner_lock.py --record-id recvv6HdBYmQpk` 返回：

```json
{"scanned": 1, "eligible": 1,
 "processed": [{"record_id": "recvv6HdBYmQpk", "action": "generate_native_photo",
                "task_ids": ["opv_task_20260913_673c2bf8e124"],
                "photo_count": 4, "failures": []}],
 "errors": [], "feishu_request_count": 17, "feishu_retry_count": 0}
```

执行时序（60s 轮询实测）：

| 时刻 | 进度 | supply looks | manifest |
|---|---|---|---|
| 22:48 | 素材生成 1/4 | 1 | incomplete |
| 22:53 | 素材生成 2/4 | 2 | incomplete |
| 22:57 | 素材生成 3/4 | 3 | incomplete |
| 23:03 | 素材生成 4/4 | 4 | incomplete |
| 23:03 | 质检中 | 4 | **complete** |
| 23:04 | **已完成** | 4 | complete |

落库核验（技能 §6 清单）：

| 项 | 实测 | 判定 |
|---|---|---|
| `opv_production_batch` | **1 条** `opv_batch_5eaab897670e1a6988be4457db4fea56`，`batch_status=waiting`，`lock_version=1` | ✓ |
| `opv_content_task` | **1 条** `opv_task_20260913_673c2bf8e124`，`task_status=photo_packaging`，`requested_shot_count=4` | ✓ |
| `opv_content_shot` | **4 条全部** `shot_status=generated` | ✓ |
| 磁盘成片 | `01..04.jpg` 全部 **1080×1920 JPEG** | ✓ |
| 行内 `预览/成片` | **4 个附件** | ✓ |
| 真实生图通道 | `supply_manifest.sources[0..3]`：`generation_provider=1route`、`generation_model=gpt-image-2.5-sunburst` | ✓ |
| 行终态 | `进度=已完成`、`执行=False`（流水线自动落闸） | ✓ |
| **`copy.title` 超限错误** | **不再出现**（`failures: []`） | ✓ |

> 行备注（流水线原文）：`已完成 1/1 篇原生图文，共 4 张；技术检查已通过；勾选确认发布后冻结当前成品并进入发布队列。`
>
> **页数 vs 唯一图数**：旅行线走 `travel_two_step`，经 `apply_travel_single_cover`
> 折叠 Look A 的重复详情页 → **5 页 / 4 张唯一图**。这里 `photo_count=4` + 4 条 shot
> 是**正确**形态，不是漏页。

### 5.2 TH#2 `recvv6HtDuLSsG`（全新行，首次跑）—— **PASS**

`run_feishu_scanner_lock.py --record-id recvv6HtDuLSsG` 返回
`photo_count=4, failures=[], errors=[]`。

**这条是修复的「非续跑」证据**：全新行从 `规划中` 起步，规划产出的 LLM title 为
**69 UTF-16**（`content_plans/recvv6HtDuLSsG/plan.json`），`planned_copy_contract_errors() == []`，
全程 `图文生成失败的原因` 为空。执行时序：

| 时刻 | 进度 | looks |
|---|---|---|
| 23:05 | （空） | 0 |
| 23:06 | 规划中 | 0 |
| 23:07 | 准备素材 | 0 |
| 23:12 | 素材生成 1/4 | 1 |
| 23:18 | 素材生成 2/4 | 2 |
| 23:22 | 素材生成 3/4 | 3 |
| 23:29 | 素材生成 4/4 | 4 |
| 23:29 | 质检中 → **已完成** | 4 |

落库核验：**1 批次** `opv_batch_113720798fcd757a387515842bf65807` (`waiting`) +
**1 任务** `opv_task_20260913_77da26f6befd` (`requested_shot_count=4`) +
**4 镜头全部 generated** + **4 张 1080×1920 JPEG** + 行内 4 附件 + `进度=已完成`，
供给侧 `4 × 1route/gpt-image-2.5-sunburst`。

### 5.3 VN `recvv6G1bQ9aTs`（围巾搭配四选一）—— **PASS（第 3 次尝试）**

#### 5.3.1 前两次失败：两道**设计性**门禁（不是缺陷）

| # | 报错 | 真因 |
|---|---|---|
| 1 | `风格参考模式需要选择图文主题` | 行上有 3 张参考图 → `resolve_reference_inputs()` 冻结 `reference_mode=STYLE` → `feishu_workflow.py:1363` 强制要主题。而主题已被清空（为了走越南语文案包）。 |
| 2 | `NEEDS_CONTENT: …仅找到 0 份未占用的有效内容` | `content_signature = hash(logic_key, 素材哈希)` 是**一次性库存**；上一轮的测试行 `recvv5rDUa2uSO` 持有同签名的未取消批次（`opv_batch_cc165f164834557263f032292811de87`）。 |

**关键机制（已写进技能 §5.9）**：`resolve_reference_inputs()` **只在有附件时才解析参考模式**——
0 附件时 `reference_mode=""`，STYLE 分支整体跳过。所以搭配线要「走文案包」的正确配置是
**清空参考图 + `素材状态=已匹配可用素材`**（上一轮验证通过的测试行 `recvv5rDUa2uSO` 正是如此），
而不是填主题（填主题会让 `build_theme_copy` 整段覆盖要验的文案包文案）。

**释放库存**用的是官方语义 API：`cancel_photo_batch("recvv5rDUa2uSO")`
（"Release unpublished photo inventory while retaining the audit row"）。
守卫检查通过（该任务无 `released_revision_id`、无 `opv_publish_record`）；
执行后 `list_photo_content_signatures()` **148 → 147，恰好释放 1 个**。
**代价**：该测试行自此不可重跑（技能 §5.5）——但它成片已在飞书、`进度=已完成`，属正常退役。

#### 5.3.2 第 3 次结果（`photo_count=5, failures=[], errors=[]`）

| 项 | 实测 |
|---|---|
| `opv_production_batch` | **1 条** `opv_batch_38552ede20ee6d97b4707a9a3b164974`，`waiting`，`lock_version=1` |
| `opv_content_task` | **1 条** `opv_task_20260913_936bcb19f694`，`requested_shot_count=5` |
| `opv_content_shot` | **4 条全部 generated**（4 个唯一 Look） |
| 成片 | **5 张 1080×1920 JPEG**（5 页） |
| 行内 `预览/成片` | **5 个附件**，`进度=已完成`，`执行=False` |
| 付费生图 | **无**（纯 `ASSET_REUSE`；`style_reference_supply/` 目录不存在）→ 本轮 VN 行 **0 生图花费** |
| 文案变体 | `scarf_one_four_ways_v1`（封面 `Một chiếc khăn, bốn cách phối\nA B C hay D?`，2 行） |

对比上一轮同线（`recvv5rDUa2uSO`）：`req=5 / unique=4 / pages=5` —— **形态完全一致**。

### 5.4 副发现：跨市场派生的隐性行为差异（TH 4 页 vs VN 5 页）

三条线实测页数/唯一图数：

| 线 | 配方 | `requested_shot_count`(页) | `opv_content_shot`(唯一图) | `final/*.jpg`(页) |
|---|---|---|---|---|
| TH#1 / TH#2 | `PHOTO_TH_TRAVEL_OUTFIT_V2` | **4** | 4 | **4** |
| 上轮 VN 旅行 | `PHOTO_TRAVEL_OUTFIT_V3` | 5 | 4 | 5 |
| VN 搭配 | `PHOTO_MATCHING_CHOICE_V3` | 5 | 4 | 5 |

根因在 `services/photo_request_factory.py:88`：

```python
def apply_travel_single_cover(request):
    """Make Look A the cover and remove its duplicate detail page."""
    ...
    if not str(result.get("recipe_id") or "").startswith("PHOTO_TH_TRAVEL"):
        return result
```

守卫是 **TH 专用字符串前缀**，不是「配方是否具备旅行单图封面能力」。所以国家无关的
`PHOTO_TRAVEL_OUTFIT_V3`（VN 在用）**拿不到这个折叠**，输出比 TH 多一页
（封面四宫格之后又出现一张单独的 Look A 详情页）。

**判定：本轮不修，仅登记。** 理由：这是 Phase 2 派生时就存在的行为，已随上一轮 VN 旅行交付
固化（文档亦按 5 页记录）；改动会变更一条已交付线的输出合同，应由业务决定 V3 是否要与 TH V2
严格等价。**已写进技能 §6.1**，避免下次把 5 页误判成漏页/多页 bug。

> 附带修正：技能原写「搭配线是 5 页 / 5 张唯一图」**是错的**——`shot` 表按**唯一 Look** 计数。
> 判「有没有漏页」看 `final/*.jpg` 数量，不要看 shot 条数。

### 5.5 全表零漂移

`verify_run.py` 对比**开工前基线**（`baseline.json`，22:02 采集，186 行）：

```
总行数 基线=186 现=186
漂移 = {"": [38, 37], "已完成": [69, 72], "待执行": [2, 0]}
```

`已完成 +3`（TH#1 / TH#2 / VN 三行）、`待执行 -2`、空进度 `-1` —— **恰好是三条目标行自己的迁移**，
行数不变 ⇒ **其余 183 行零改动**。

---

## 6. 预检又抓到**第二个同类缺陷**：VN 围巾搭配文案封面 3 行

TH#1 通过后、跑 VN 前，用只读预检脚本把**整个文案包**逐变体过了一遍
（`tmp/real_gen_e2e_3rows/prepack.py`，口径与 `photo_package.py:_draw_overlay` 一致）：

```
### VN_SCARF_MATCHING_V1 × PHOTO_CHOICE_CARD_V2 (max_lines=2, cover_index=1)
[OK  ] scarf_one_four_ways_v1  cover_lines=2 title=29u desc=111u
[OK  ] scarf_color_for_set_v1  cover_lines=2 title=32u desc=109u
[OK  ] scarf_coat_pairing_v1   cover_lines=2 title=31u desc=124u
[FAIL] scarf_hanoi_cool_v1     cover_lines=3 title=35u desc=113u
        cover = 'Hà Nội trở lạnh\nPhối khăn thế nào?\nA B C hay D?'
```

### 6.1 为什么这条比 §2 的 title 缺陷更险

`max_lines` **只在成片阶段**被检查（`photo_package.py:237`），而那时：

1. 批次**已建**、内容签名**已占**；
2. 按技能 §5.5，`batch_id = "opv_batch_" + canonical_hash(record_id)[:32]`、且
   `claim_batch_run` 的谓词含 `batch_status<>'cancelled'` ——**批次一旦失败/取消，
   同一行永远跑不动**。

⇒ 命中这个变体（约 1/4 概率，变体由 `fingerprint(...)` 伪随机选取）不只是「这一跑失败」，
而是**这一行被永久作废**。上一轮的 VN 搭配线验收恰好抽中安全变体，所以地雷一直没炸。

### 6.2 修复

`config/copy_packs/VN_SCARF_MATCHING_V1.tsv` 的 `scarf_hanoi_cool_v1` 封面
3 行压成 2 行，**向该包自己的既有约定看齐**（其余 3 个变体都是
`标题\nA B C hay D?`，且该行的 `title` 字段**本来就写着**
`Hà Nội trở lạnh, phối khăn thế nào?`）：

```
- Hà Nội trở lạnh\nPhối khăn thế nào?\nA B C hay D?
+ Hà Nội trở lạnh, phối khăn thế nào?\nA B C hay D?
```

- 只动 `slide_1` 一个单元格，**不新造句子**（复用该行的 title 原文）。
- `language_review_status=DRAFT` **保持不动**（没有顺手升级成「已审校」）。
- 无 golden 夹具引用该包（已 grep 确认），故无需重建 fixture。
- 复检：`0 / 4 variants violate the compose/publish contract`。

### 6.3 同类缺陷已第三次出现，值得上收为机器守卫

| 轮次 | 位置 | 形态 |
|---|---|---|
| 上轮 | `VN_TRAVEL_OUTFIT_V1` | 3 个变体封面 3 行 |
| 本轮 §2 | LLM 旅行文案 `copy.title` | 91 vs 90 UTF-16 |
| 本轮 §6 | `VN_SCARF_MATCHING_V1` | 1 个变体封面 3 行 |

**建议（未在本轮实施）**：把「封面行数 ≤ layout `max_lines`」与「title/caption 发布契约」
合并成**配置装载期**的校验（`config/loader.py` 已能同时看到 copy pack 与 layout），
让这类错误在**播种/加载**时就炸，而不是等到成片——那时代价已经是「一行作废」。

---

## 7. VN 四道门禁的武装与复位

| # | 门禁 | 对象 | 武装 | 复位 |
|---|---|---|---|---|
| 1 | 生产预设 `status` | `图文｜VN｜围巾搭配四选一` | `active` | `disabled` |
| 2 | 配方 `status` | `PHOTO_MATCHING_CHOICE_V3` | `active` | `draft` |
| 3 | Market Pack `status` | `MP_VN_DEFAULT_V1`（`pack_key=MP_VN_DEFAULT`） | `active` | `draft` |
| 4 | 账号 `status` | `OPV_VN_TEST_001` | `testing` | `paused` |

- 素材集 `ASSET_VN_SCARF_CHOICE_V1` 保持 `enabled`（**它不是路由闸门**，只做候选过滤）。
- 搭配线额外条件（`feishu_workflow.py:1302`「需要产品编码或参考图」）由行上 **产品编码**满足。
  行上原有 **3 张参考图**已在 `vn_fix_row.py` 中**清空**（备份见 `vn_row_backup.json`）：
  有附件会走 `resolve_reference_mode()` → STYLE 分支强制要「图文主题」，而搭配线用的是
  `MATCHING_CHOICE` 分支，两者互斥 —— 这是搭配线**第一个必然撞上的门禁**（详见 SKILL §5.9.1）。
- 预设的武装采用**外科式字符串替换**（只改 `status` 值），避免 `json.dumps` 重写产生大量
  空白 diff；复位脚本从 `arm_state.json` 读回原值并**直读 RDS 复核**。

---

## 8. 交付物与边界声明

- **新增代码**：`domain/photo_contracts.py`（+3 函数）、`services/photo_content_planner.py`、
  `services/photo_theme.py`、`services/feishu_workflow.py`（+1 方法 + 1 调用点）。
- **新增测试**：`tests/test_photo_labels.py`（`PublishCopyContractTest`，9 例）、
  `tests/test_travel_topic_linkage.py`（2 例）、`tests/test_feishu_workflow.py`
  （`PlannedCopyContractTest`，3 例）。
- **回归**：`Ran 1215 tests — OK`。
- **边界**：
  - 成片是**真实生图**产物（非 `run_feishu_table_smoke.py` 的 ASSET_REUSE 套版）。
  - 本轮的**视觉质检**未跑完整 `OPV_PHOTO_VISION_PROVIDER` 流程 → 质量结论**不构成生产级质检**；
    但 STYLE 模式生产主链**自身**确有调用外部视觉模型（风格分析 / 内容规划，
    `services/photo_reference_vision.py`），这一路是流水线必需步骤，**不是**质检。
  - 未做故障注入 → 兜底/熔断只有单测与通道层证据。
- **残留**：上一轮作废行 `recvv5RcJIxHpB` 与其孤儿 RDS 行**未删**，留档待用户处置。
