# 配饰原创混合模板 · 阶段 E 交付说明

**日期**：2026-09-15
**范围**：任务书阶段 E —— Review #3（按最终镜头做批次与历史差异判断）
**开发源**：`/Users/likeu3/.openclaw/workspace/skills/original-script-generator`
**主要文件**：`core/accessory_mixed_templates.py`、`core/original_batch_allocator.py`、`core/original_batch_executor.py`
**探针**：`wp0/stage_e_probe.py`（60 项断言，0 失败）
**离线整链路验收**：`wp0/stage_e_e2e.py`（334 项断言，0 失败；12 份核心样本 + 10 个物理子类型边界样本）
**回归**：T14–T18 已落为 17 个用例；隔离运行器 11 个文件 **382 tests 全 PASS**
**生产库**：三个库 md5 逐字未变（`e0024aa7…` / `6e150904…` / `ba2cb606…`）

---

## 0. 一句话结论

阶段 D 修的是"合同最后一跳不被压缩改写"；阶段 E 修的是**去重这件事本身算错了轴**。

修复前：两条候选的**四镜最终约束完全相同**，只要序号/旧场景签名不同，就会被当成两条独立脚本交付。
Review #3 的现场是"请求 4 条、交付 4 条，其中第 1 条与第 4 条成片会一模一样"。

修复后：同一现场**只交付 3 条**，第 4 条被 `EXACT_DUPLICATE` 剔除，并留下可核查的 `difference_report`
（`nearest_script_id=DA_E#01`、`difference_dimensions=['FINAL_SHOTS_IDENTICAL','SURFACE_IDENTICAL']`），
批次报告给出 `mixed_shortage_reason=DIFFERENCE_INSUFFICIENT`。

---

## 1. Review #3 的根因：两条轴线被混用

批次里本来就有一条差异轴线：

```python
creative_diversity_contract.visual_signature
  = persona_role|scene_motif|opening_action|action_grammar|outfit_signature|persona_id
```

它是**场景/人物轴**，服务于所有类目。而饰品混合模板的重复是这样产生的：

```python
preferred = select_template_id(item_index - 1)          # 3 个模板按 item index 轮换
recipe_id = select_environment_recipe_id(item_index - 1) # 3 个环境配方按 item index 轮换
```

每 3 条回到同一个（模板 × 配方）组合。第 4 条与第 1 条因此**共用同一套四镜**，
但 `scene_motif` 等场景标签仍在轮换 —— 于是旧轴报"不同"，蒙太奇却完全相同。

**关键判断：这不是"旧签名不好"，而是"用错了轴"。** 旧轴继续管它本来管的类目与场景，
新增一条**增量轴**专门衡量"观众最终看到的四个镜头"。

---

## 2. 修复内容

### 2.1 三个签名 + 版本号（`core/accessory_mixed_templates.py`）

冻结合同时一并冻结签名（`compile_mixed_template_contract` 尾部写入 `final_shot_signature`）：

| 签名 | 内容 | 是否参与去重判定 |
|---|---|---|
| `semantic` | 主题命题、观众问题、批准 claim、证据引用、`observation_focus`、`structure_facts` | ✅ 区分"新主题" |
| `visual` | 每镜 module / view_scope / 观察重点 / 动作 / 动作边界 / 允许与禁止裁切 / 佩戴关系 / 首镜任务 + 已验证部件 + template_id + face_policy | ✅ 判定"同一套镜头" |
| `surface` | 环境配方、光线、音频路由、时长、剪切策略 | ⚠️ 只用于排期/轮换，**不得单独作为新主题的依据** |
| `signature_version` | `mixed-signature-v1` | ✅ 防止新旧签名混用 |

两条**故意不参与** digest 的输入，正是 Review #3 的漏洞来源：

1. **`theme_id` 与 thesis 文案**：按 item 轮换、可同义改写。写进 digest 就等于"换个说法就算新内容"。
   （仍在报告里保留为 `theme_label` / `proposition`。）
2. **每镜的 `evidence_refs`（claim id）**：那是"这一镜用来证明什么"，属语义内容。
   把它放进视觉签名，会让"论证改写了、画面没变"看起来像"画面变了" —— 是同一个缺陷的镜像。
   已验证**部件**改在合同层进入 `visual.verified_parts`。

判据实测（探针 + e2e 双重验证）：

```
theme_id 改成 TH_99、thesis 换一种说法 → visual digest 不变  ✅
claim 集合改变                          → visual digest 不变  ✅
```

### 2.2 ABSENT 与 UNKNOWN 必须保持区分

`verified_parts` 里 `ABSENT` 与 `UNKNOWN` 都保留：

- "链节已确认不存在" → 不许拍出链节；
- "链节从未确认" → 不能推断它不存在。

两者是不同的拍摄指令，塌成同一个签名就等于让一条拍摄约束凭空消失。

### 2.3 裁决表（与任务书逐条对应）

| 情形 | 裁决 | 计入独立内容 |
|---|---|---|
| 最终镜头相同、表层也相同 | `EXACT_DUPLICATE` | ❌ |
| 最终镜头相同、只有环境/光线不同 | `SURFACE_ONLY` | ❌ |
| 同主题、有证据支撑的新观察重点 | `EXECUTION_VARIANT` | ✅（显式标记变体） |
| 同主题、相同观察重点不同镜头顺序 | `EXECUTION_VARIANT` | ✅ |
| 主题问题有实质变化且安排了对应镜头 | `DISTINCT_THEME` | ✅ |
| 新增观察点不在作者写的词表里 | `NEEDS_REVIEW` | ❌（**不宣称已去重**） |

`NEEDS_REVIEW` 进 `MIXED_BLOCKING_VERDICTS`（与 `EXACT_DUPLICATE` / `SURFACE_ONLY` 同级）是刻意的：
任务书原文"语义是否相同无法可靠判断 → 不宣称已去重通过"。**无法验证的差异不是差异。**

多重引用取**严重度最高**者（`_MIXED_VERDICT_SEVERITY`），所以本批已收的同款蒙太奇不会被历史的宽松裁决放行。

### 2.4 候选筛选顺序（把任务书顺序落到代码）

```
生成候选 → 编译完整混合合同 → 本批/历史比较 → 接受与预留 → 生成正文 → 用实际正文/最终镜头再核对一次
```

- **接受前**：`_build_mixed_template_injection` 先试 index 驱动的模板；被判 `counts_as_independent=False`
  就换下一个模板；三个模板都被否 → 返回 `rejected`（**不降级、不合成同义脚本**）。
  这样"计划相同"不会被当成"计划不同"而放行。
- **接受后**：`reserved_mixed_references` 记录**这一条真正拥有的最终镜头**，同批后续条目与它比较。
- **生成正文后**：`original_batch_executor._execute_simplified_single_item` 再核对一次，
  结果**并列记录**在 `script["mixed_final_shot_recheck"]`（不覆盖计划期裁决）——
  "计划为变体、生成为重复"这个不一致正是审核者需要的信号。
  另含 `montage_collapsed` 检测：四镜 `rendered_event` 全同 → `NEEDS_REVIEW` + `RENDERED_SHOTS_COLLAPSED`。

### 2.5 数量报告口径 + 两个新发现（本轮修复）

摘要新增字段：

```
mixed_distinct_theme_count / mixed_execution_variant_count
mixed_duplicate_rejected_count / mixed_insufficient_evidence_count
mixed_usable_count（= 独立主题 + 执行变体）
mixed_history_coverage{scope_status, history_compared, history_incomplete, history_rows_seen}
mixed_shortage_reason = DIFFERENCE_INSUFFICIENT | CONTENT_CAPACITY
```

写用例时探针抓出**两个真实缺陷**，本轮一并修掉：

1. **同一候选被重复累计。** 一轮候选被否后 `mother_bundle_indices` 不会更新，
   下一轮会把**同一个候选**再放上桌、再否一次，于是 `deferred_content` 与"重复剔除数"双份计数
   （实测 6 条记录里只有 4 条是不同候选）。
   新增 `_dedup_mixed_rejections`：按
   `(方向, 槽位, 内容包, 降级原因, 裁决, 比较依据)` 去重，计数从去重后的记录重算。
2. **零覆盖可能被误读成"已比对、未发现重复"。** 女装批次的三个计数也是 0，
   读者无法区分"这个机制不适用"和"比对了但没发现重复"。
   新增 `scope_status`：`IN_SCOPE` / `OUT_OF_SCOPE`（开关关或非四类目）/ `SCOPE_UNSUPPORTED`
   （长视频借用规划、复刻、resume）。

### 2.6 复用现有持久化，不新建库

- 预留沿用现有台账 `creative_pattern_usage`，只在 `metadata` 里加 `mixed_signature`（与旧场景签名**并列**）。
- `mixed_reference_signature` 同时接受 `metadata`（内存 dict）与 `metadata_json`（`SELECT *` 返回的字符串列）
  两种形态 —— 这条不兼容曾让台账回传静默失败（探针第 7 节抓出）。
- **缺最终镜头的历史只计数、不引用**（`HISTORY_INCOMPLETE` / `HISTORY_SIGNATURE_VERSION_MISMATCH`）：
  绝不用旧 `scene_id` 伪造最终镜头签名，也绝不为历史补签名重跑模型。

---

## 3. T14–T18 用例（17 个，全部通过）

| 用例 | 落点 | 断言要点 |
|---|---|---|
| **T14** `test_a_repeated_montage_is_refused_and_reported` | `test_accessory_mixed_integration.py` | 请求 4 → 交付 3；1 条 `EXACT_DUPLICATE`；`FINAL_SHOTS_IDENTICAL` + `nearest_script_id`；`DIFFERENCE_INSUFFICIENT` |
| **T14** `test_the_legacy_scene_axis_cannot_mask_a_repeated_montage` | 同上 | 两条历史旧场景签名不同 → 引用签名相同 → 仍 `EXACT_DUPLICATE` |
| **T14** `test_a_rewritten_theme_label_does_not_move_the_final_shots` | 同上 | 改写 theme_id/thesis → visual digest 不变 |
| **T15** `test_environment_change_alone_is_not_independent_content` | 同上 | 三种模板都被历史占用 → 只换台面 → 0 产出、`SURFACE_ONLY`、`counts_as_independent=False` |
| **T15** `test_a_new_evidenced_observation_point_is_labelled_an_execution_variant` | 同上 | `EXECUTION_VARIANT` + `NEW_OBSERVATION_POINT`；**新观察点确实落在 `capture_units` 上** |
| **T15** `test_an_invented_observation_point_is_not_accepted` | 同上 | 凭空发明的观察点 → `NEEDS_REVIEW` + `UNVERIFIED_NEW_OBSERVATION`，不计独立内容 |
| **T16** `test_a_shortage_is_reported_instead_of_synthesised` | `test_original_batch_allocator.py` | 请求 20 → 少交付；`usable == planned`；四镜 digest 两两不同；缺口原因明确 |
| **T16** `test_rejected_candidates_are_counted_once_each` | 同上 | 剔除记录互不重复；计数 == 记录数；重试次数 ≤ 5（不无限重试）；每条带 `REPLAN_MIXED_THEME` |
| **T17** `test_a_history_row_alone_blocks_the_same_montage` | 同上 | 整条历史已占用 → 0 产出；`nearest_script_id == HIST_AMX_A_WORN_FIRST`；`history_compared == 3` |
| **T17** `test_incomplete_history_is_counted_but_never_used_as_a_reference` | 同上 | `history_compared=0 / incomplete=1 / seen=1`，且不阻断新内容 |
| **T17** `test_the_same_candidate_reserved_twice_is_still_one_duplicate` | 同上 | 历史条目翻倍，剔除数**不翻倍**；仍判重复 |
| **T17** `test_concurrent_reservations_keep_every_signature_intact` | 同上 | 4 线程并发写台账：无丢行、无损坏、逐行能取回完整签名 |
| **T17** `test_resume_reuses_the_settled_batch_and_its_reservation` | 同上 | resume 同请求：同批次、同条目、台账行数不变、**规划函数只被调用 1 次**；预留行带最终镜头签名 |
| **T18** `test_remake_and_resume_scopes_are_refused` | `test_accessory_mixed_integration.py` | `REMAKE` / `is_new_plan=False` / 30 秒 → `scope_rejected`，无合同无报错 |
| **T18** `test_womenswear_never_builds_an_accessory_contract` | 同上 | 女装返回 `{}`（全局开关不得越权） |
| **T18** `test_longform_and_womenswear_plans_stay_legacy` | 同上 | 长视频借用规划/女装 → 无合同，`scope_status` 分别为 `SCOPE_UNSUPPORTED` / `OUT_OF_SCOPE` |
| **T18** `test_an_eligible_short_original_still_gets_the_contract` | 同上 | 合法短原创仍拿到合同 + 比较报告 |

**测试隔离加固**：`test_original_batch_allocator.py` 新增 `setUpModule`，
把 `OPENCLAW_SHARED_DATA_DIR` 等全部本地库指向临时目录并**移除 RDS URL 环境变量**。
原 `test_plan_only_idempotent` 会经 `run_plan_only` 落到默认库路径（裸跑时即生产库），
该守卫把整个文件变成"裸跑也安全"。

---

## 4. 离线整链路验收（12 份样本 + 边界样本）

`wp0/stage_e_e2e.py`：四类目 × A/B/C = 12 份，从冻结合同一路走到**最终视频提示词**，
全程不调用模型、不写生产库。

链路：`compile_mixed_template_contract` → `_project_mixed_template_shot_contract`
→ `render_first_frame_prompt` / `render_mixed_blueprint_guidance` / `render_video_generation_prompt`。

按任务书 §212 的要求，**只查最终交接引用**（不查中间 seed）：

| 检查项 | 结果 |
|---|---|
| 交接容器 `video_generation_brief` 携带冻结混合合同 + 可比较签名 | 12/12 ✅ |
| 最终视频提示词携带商品身份参考（身份锁 + 必须保持锚点 + 关键可见细节） | 12/12 ✅ |
| 合同自带 `product_identity_ref` | 12/12 ✅ |
| 首帧事实取第一镜模块、首帧提示词携带第一镜允许裁切与身份锚点、无"半脸" | 12/12 ✅ |
| 最终交接的 4 个单位逐镜携带 module / view_scope / 观察重点（取自冻结合同） | 12/12 ✅ |
| 佩戴镜头的局部裁切清单、手持与静物镜头的模块禁令进入最终提示词 | 12/12 ✅ |
| 蓝图提示词逐条携带逐镜模块说明与观察重点 | 12/12 ✅ |
| 时间轴 15 秒、连续无空洞、投影状态 `APPLIED`、四个稳定镜头 ID | 12/12 ✅ |
| 同一冻结合同两次渲染逐字一致（离线、无模型调用） | 12/12 ✅ |
| 12 份样本的最终镜头签名两两不同 | ✅ |
| 10 个物理子类型边界样本（手链/手镯/细手镯/鲨鱼夹/抓夹/发箍/皮筋/发圈/发带/发簪/发饰） | 全部可编译、每镜有裁切 ✅ |

合计 **334 项断言，0 失败**。

---

## 5. 本阶段发现的、不在阶段 E 范围内的问题（已记录，未擅自修改）

这两条都由验收脚本以实证方式记下，修与不修需要你决定 —— 它们都落在**共享组件**上，
按任务书"只改本任务相关代码"的边界，本轮只做记录。

### 5.1 共享注册表的配饰兜底把发饰解析成手镯（影响 6 个常见词）

`core/product_type_resolution.py:136-150`：

```python
if business_family in {"jewelry", "accessory"}:
    return "bangle"      # 任何未登记的配饰词都落到 bangle
```

实测（`resolve_mixed_zone(raw, "配饰")`）：

```
头绳 → WRIST/bangle      发绳 → WRIST/bangle      发卡 → WRIST/bangle
一字夹 → WRIST/bangle    鸭嘴夹 → WRIST/bangle    束发带 → WRIST/bangle
皮筋 → HAIR/hair_tie ✅  发圈 → HAIR/scrunchie ✅  发箍 → HAIR/headband ✅
```

后果：**产品类型字段写"发卡"的商品会拿到手腕类目的裁切、部件词表与模块禁令**，
即"发饰被当成手镯拍"。`resolve_mixed_zone` 的 docstring 说"未知产品仍被拒绝而不是猜测"，
这对**直接传注册名**的路径成立，但对**运营商原始词**不成立。
建议：补别名，或让未登记的配饰词**明确拒绝**（`resolve_mixed_zone` 返回 `None`）而不是兜底成 `bangle`。

### 5.2 分镜时间轴与冻结时间轴没有一致性校验

冻结合同的时间轴在 `capture_units`（0-3/3-6/6-10/10-15），
而最终视频提示词的连续内容段表头取自分镜自己的 `time_range`，两者之间**没有任何校验**。

实证：故意把分镜第 3/4 镜写成 `6-9秒` / `9-12秒`（合同是 `6-10秒` / `10-15秒`），
投影仍然报 `APPLIED`，最终提示词表头照抄 `…→9-12秒`，与合同的 15 秒自相矛盾。
建议：投影或校验处把分镜时间轴对齐冻结时间轴，或直接拒绝不一致。

---

## 6. 本阶段留下的关键事实

1. **两条差异轴线并存，不得互相替代。** `creative_diversity_contract.visual_signature`（场景/人物轴）
   对**所有类目**继续生效；`final_shot_signature`（最终镜头轴）是**增量**轴，只管混合模板。
2. **`surface` 签名只用于排期/轮换。** 只看环境/光线不同 → `SURFACE_ONLY`，不计独立内容。
   想让它变成新内容，必须换出真正不同的镜头。
3. **"无法验证"按"不通过"处理。** `NEEDS_REVIEW` 属阻断裁决，且会进 `mixed_insufficient_evidence_count`。
4. **历史不完整只计数不引用。** 缺最终镜头的历史行**不能**被当成"没找到重复"，
   也不能用旧 `scene_id` 补签名（那是伪造引用）。
5. **同一候选多轮重试只算一次。** 剔除记录按候选身份去重后再计数（`_dedup_mixed_rejections`）。
6. **`scope_status` 必须和计数一起读。** 女装批次的三零不等于"去重通过"。
7. **生成后核对是"并列记录"而非"覆盖"。** `mixed_difference_report`（计划期）与
   `mixed_final_shot_recheck`（生成期）同时保留，两者不一致时以生成期为准做人工复核。
8. **本机制降低自有内容重复，不是平台判重保证。** 报告只能宣称"在本批与现有历史范围内未发现同款蒙太奇"。
9. **解释器**：`/Users/likeu3/.workbuddy/binaries/python/envs/default/bin/python`。
10. **zsh 下 bash `grep` 会静默返回空**，检索请用 Grep 工具。

---

## 7. 与后续阶段的关系

阶段 A→E 已全部完成，A–E 的验收口径合并起来是一条完整链路：

| 阶段 | 修的缺陷 | 一句话 |
|---|---|---|
| A | Review #1/#2 | 合同失败必须停，不得带着废合同继续 |
| B | Review #5/#6 | 合同内容不得自相矛盾（裁切 × 模块、动作 × 三态证据） |
| C | Review #4/#7/#8 | 下游不得绕过冻结合同重新判断（首帧/光线/口播） |
| D | Review #9 | 压缩不得改写逐镜约束 |
| **E** | **Review #3** | **去重必须按最终镜头算，不得按旧场景签名算** |

E 的产物（`final_shot_signature`）是"下一批不再重复上一批"的唯一依据，
因此它必须同时满足两件事：**冻结**（随合同冻结，resume 与压缩都不改动它）与
**可回传**（写进历史台账，下一批能读到）。两者都已由测试与探针各自独立验证。
