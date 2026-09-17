# 配饰与发饰优化方案 · P0 实施与验收（I1–I6）

- 日期：2026-09-17
- 范围：**P0 全部（I1–I6）** + 上线入口启用（用户裁决：随 P0 一起在配置里启用）
- 依据：`docs/ACCESSORY_IMPLEMENTATION_AND_SCRIPT_QUALITY_REVIEW_20260917.md`（缺陷权威定义，只读）
- 本轮**未触发生成、未发布、未覆盖既有脚本**；全部结论来自离线断言与真实商品 plan-only

---

## 0. 一句话结论

Review 列出的 6 个缺陷（I1–I6）**全部在代码中定位、修复、并各有断言级证据**；
新模式从「生产中一次都没真正走通」变为「官方入口自动启用、真实商品新规划逐条携带合法合同」。

---

## 1. 逐条修复（行号级证据 + 断言）

| 编号 | 缺陷 | 根因落点 | 修法 | 证据 |
| --- | --- | --- | --- | --- |
| **I4** | 15 秒冻结合同可渲染成 12 秒时间轴 | `accessory_mixed_templates` 投影时不写 `time_range`；`production_script_renderer` 用分镜自带时间覆盖 | 投影把**冻结时间轴单向派生**进 storyboard（`format_mixed_shot_time_range` + `TIMELINE_PROJECTION_VERSION`）；派生不出即 `PROJECTION_STATUS_MISMATCH` | `p0_probe` I4 三断言：逐镜 `0-3s/3-6s/6-10s/10-15s`，最终提示词不再出现 `9-12s` |
| **I5** | 未确认结构被授权为可展示部件 | `resolve_part_evidence` 用有限否定词 + 正词子串判定 | 判定顺序改为 **结构注册表 → 该部件的不确定措辞 → 否定 → 正词 → UNKNOWN**；不确定措辞成为单一来源（`config` 的 `evidence_uncertainty_terms` + 代码默认值） | I5 四断言：`搭扣结构无法确认`/`看不清背面搭扣`/`未提供背面搭扣图`/`无法确认是否为搭扣` 全 → `UNKNOWN`；真事实仍 `VERIFIED`；结构化 `ABSENT` 仍 `ABSENT` |
| **I6** | 仅第一镜有锚点时被提升为全片要求 | `video_prompt_compaction` 只看"出现过的锚点值是否唯一" | 改为**按分镜块**校验：声明集合必须覆盖全部有该字段的分镜块才允许提升 | I6 四断言：单镜不提升且保留分镜级整行；四镜一致仍提升；不一致不提升 |
| **I2** | 不同商品互相耗尽 A/B/C 模板 | 历史参考转换未按 `product_code` 过滤 | `_mixed_history_references(rows, product_code)` 只收本商品；跨商品与不完整**分开计数**并进批次报告 | I2 六断言：别款商品历史 3 条 → 可比 0、`skipped={"incomplete":0,"other_product":3}`；新商品仍规划出 2 条；**同商品同组合历史仍硬去重**（计划 0、剔除 4） |
| **I3** | 成稿复核混入计划差异，且无跨批正文比较 | 成稿 digest 含 `template_id` 与计划观察任务；只传同批兄弟；历史只存计划签名 | ①成稿 digest **只由实际 `visual_content` 决定**，计划字段移入 `visual.planned`；②`signature_kind` 一致性守卫；③台账分 `mixed_signature`/`mixed_rendered_signature` 两键并回写成稿签名；④成稿复核引用 = 同批已生成兄弟 + 同商品历史成稿 | I3 七断言（详见 §2） |
| **I1** | 生成后判重复仍作为 READY 交付 | 重检结果只写进 script，状态机按 `SUCCESS` 直接标 READY | 新增条目状态 `SCRIPT_DUPLICATE`；`_script_item_outcome()` 据裁决分流；批次统计把重复算作"未就绪"；报告暴露重复数与原因 | I1 十二断言（详见 §3） |

---

## 2. I3 细节：计划签名 / 成稿签名彻底分离

**病状（Review 原文）**：两份四段 `visual_content` 逐字相同、只关联不同 A/B 模板，仍返回
`EXECUTION_VARIANT`、`counts_as_independent=true`。

**修法**
1. `mixed_signature_from_script()` 的 digest 载荷改为 `{"signature_kind": "RENDERED", "shots": [逐镜实际画面]}`；
   计划字段（`template_id`/`face_policy`/`verified_parts`/逐镜要求）移入 `visual.planned`，**不参与 digest**。
2. `visual.signature_kind` = `PLANNED` / `RENDERED`；`compare_mixed_signatures()` 先比 kind，
   不一致直接 `NEEDS_REVIEW` + `SIGNATURE_KIND_MISMATCH`（缺失按 `PLANNED` 兼容历史台账，
   否则整本台账会变成"不可比"）。
3. 台账两个键：`mixed_signature`（计划，`_reserve_batch_creative_usage` 规划时写）、
   `mixed_rendered_signature`（成稿，`_persist_mixed_rendered_signature` 生成后合并写，**不改生命周期状态**）。
   新增只读方法 `PipelineStorage.get_creative_pattern()` 支持"读-改-写"metadata。
4. 成稿复核引用（`_mixed_render_references`）= 同批**已生成**兄弟 + 同商品**历史成稿**；
   只有冻结计划的兄弟按 `source=FROZEN` **显式计数**跳过（`references_skipped_not_rendered`），
   不让"计划差异"去否决一份成稿。

**证据（`wp0/p0_probe.py` I3 节 7/7）**
- 同正文、A/B 模板不同 → 成稿 digest 相同（`5b5168a70d99`）；
- 成稿镜头字段只有 `['is_opening','position','rendered_event']`（计划字段不得渗入）；
- 同光影 + 同正文 → `EXACT_DUPLICATE`、`counts_as_independent=false`；
- 计划模板/光影不同也不得把逐字相同的正文判为独立（`SURFACE_ONLY`、`visual_identical=true`）；
- 计划签名 vs 成稿签名直接比 → `NEEDS_REVIEW` + `SIGNATURE_KIND_MISMATCH`；
- 只有计划的兄弟 → 可比 0 条、跳过 1 条；
- 成稿签名落台账（同键、状态不变）→ 下一批读得到 → `comparison_scope=RENDERED_BATCH_AND_HISTORY`、`history_rendered_compared=1`。

---

## 3. I1 细节：重复不得成为"独立可用交付"

**病状（Review 原文）**：离线 mock 真实 `run_script_only`，结果含
`review_status=EXACT_DUPLICATE, counts_as_independent=false`，条目仍 `SCRIPT_RUNNING → SCRIPT_READY`；
而 `SCRIPT_READY` 正是**首帧任务 / 生产脚本表 / 生产交接**的唯一入口。

**修法**
- 新增条目状态 `SCRIPT_DUPLICATE`：不进 `{PLANNED, SCRIPT_FAILED}`，所以 `resume` 不会反复付费重生同一份重复。
- `_script_item_outcome(result)`：`EXACT_DUPLICATE`/`SURFACE_ONLY` → `SCRIPT_DUPLICATE` +
  `MIXED_DUPLICATE_CANDIDATE` + 人读原因（含与谁重复）；`NEEDS_REVIEW`/`montage_collapsed` → **仍 READY**
  但 `requires_review=true`（评审要求"不确定的继续人工复核，不扩大成无界模型重写"）。
- `_refresh_batch_totals` 把 `SCRIPT_DUPLICATE` 计入"未就绪"，否则全重复批次会报成全绿。
- 报告：`duplicate_count` 顶层字段 + 逐条 `mixed_delivery`；Markdown 头部增
  "其中成稿重复（不计入独立可用）：N"，条目表增"成稿复核 / 复核说明"两行。
- 重复条目的台账预留**不提升**为 `MACHINE_SCREENED`（什么都没被接受），但成稿签名已回写 → 下一批继续避重。

**证据（`p0_probe` I1 节 12/12 + `tests/test_accessory_mixed_integration.py::MixedDeliveryStatusTest` 8 例）**
包括：真实 `run_script_only`（mock 存储与执行）后状态为 `SCRIPT_DUPLICATE` 且**不含** `SCRIPT_READY`、
`script_id` 仍保留（可复核而非消失）、整批重复时批次状态为 `FAILED`、生成失败仍走 `SCRIPT_FAILED`、
无重检结果的普通条目仍 `SCRIPT_READY`（旧行为零变化）。

---

## 4. 上线入口（用户裁决）

`scripts/run_feishu_operation_tasks.py::_enable_production_category_extensions()` 增加
`os.environ.setdefault(ACCESSORY_MIXED_TEMPLATE_ENV, "1")` —— 与既有
`ORIGINAL_SCRIPT_ACCESSORY_PROFILE_ENABLED` 同一处，`openclaw_original_script_task.py` 作为子进程入口一并覆盖。

**为什么这样启用是安全的**：适用范围由**代码内两道闸门**决定，不由开关决定
（`mixed_scope_decision`：分支/时长/新规划/script_mode；`resolve_mixed_zone`：物理子类型）。

**入口验证实测（`wp0/p33_entry_verify.py`，25/25，不调模型、不写飞书/生产库）**
- 入口调用前默认关；入口调用后自启用（且仍可被外部显式覆盖）；
- 拒绝：长视频 / 非新规划 / `legacy_v2` / 时长 30s / 女装 / 空类目 / 围巾；
- 放行：15 秒饰品短视频；`耳饰→EAR/earring`、`抓夹→HAIR/claw_clip`（已是 canonical 不再送归一）；
- 真实商品 `1731025835147691611` 请求 3 条 → **计划 3/3**，模板 A/B/C 各一、光影三配方各一、
  逐镜 `NO_FACE`、合同全部通过 `validate_mixed_template_contract`、
  **冻结晶种内也含合同**、`mixed_usable_count == len(items)`。

---

## 5. 回归与隔离证据

| 运行 | 结果 |
| --- | --- |
| `wp0/run_isolated_tests.sh`（71 个测试文件全量） | **全部 PASS，非零退出 0** |
| `wp0/run_all_probes.sh`（A–E 阶段 + P0） | 6 探针全 PASS（stage_e 61、stage_e_e2e 334 断言） |
| `wp0/p0_probe.py` | 40 项断言，失败 0 |
| `wp0/p33_entry_verify.py` | 25 项断言，失败 0 |
| 生产库 md5 | `original_script_generator` / `longform_original_video` / `short_video_auto_publish` 运行前后逐字节一致（`short_video_auto_publish` 有常驻外部写入方，仅在单次运行内比对） |

**本轮踩到并修掉的夹具陷阱**：`_mixed_history_references` 改为按 `product_code` 过滤后，
所有**手工造的台账夹具**缺该字段 → 被记成"历史不完整" → 引用为空 → **去重静默失效**（红线变绿灯）。
受影响并已修：`tests/test_accessory_mixed_integration.py::_mixed_history_row`、`wp0/stage_e_probe.py`（夹具 + 新返回结构两处断言）。
→ 结论：改公共函数签名时，**手造夹具必须当成生产数据一起更新**。

---

## 6. 本轮明确未做

1. **未触发生成/发布**：方案 §12.2 的 12 条新稿（6 款 × 2 条）与 §12.3 的 4 条媒体验收仍待跑。
2. 新状态的**跨模块可见性**：`SCRIPT_DUPLICATE` 目前只在本地批次库与批次报告中可见；
   飞书运营表若要单独展示该状态，需要下一轮明确列映射（本轮靠"只同步 `SCRIPT_READY`"实现排除）。
3. 报告缺陷的另一半：无混合复核时 `mixed_final_shot_recheck` 仍写空 `{}`，
   与"不适用"不可区分（本轮只让**重复条目**必带原因码）。
4. 段间"素材级"差异、口播投影（方案 P2/P3）未开始。

---

## 7. 改动文件清单（本模块）

| 文件 | 变更 |
| --- | --- |
| `core/accessory_mixed_templates.py` | I3 签名分离 + kind 守卫 + 台账双键引用 + `mixed_delivery_decision`；I4 时间轴派生；I5 不确定措辞 |
| `core/original_batch_allocator.py` | I2 按商品过滤历史（返回 `(references, skipped)`） |
| `core/original_batch_executor.py` | I1 `_script_item_outcome` + 状态机 + 批次统计；I3 `_mixed_render_references` / `_persist_mixed_rendered_signature` |
| `core/original_batch_models.py` | 新增条目状态 `SCRIPT_DUPLICATE` |
| `core/storage.py` | 新增只读 `get_creative_pattern()` |
| `core/video_prompt_compaction.py` | I6 按分镜块校验锚点提升 |
| `config/accessory_mixed_templates.json` | 新增 `evidence_uncertainty_terms` |
| `scripts/run_original_batch.py` | 报告 `duplicate_count` + 逐条 `mixed_delivery` + Markdown 复核行 |
| `scripts/run_feishu_operation_tasks.py` | 官方入口启用混合模板开关 |
| `tests/test_accessory_mixed_integration.py` | I3/I1 新测试（15 例）+ 夹具补 `product_code` |
| `tests/test_original_batch_allocator.py` | I2 新测试 + 夹具补 `product_code` |

未提交：以上改动**均未提交**（工作区正被并发推进，按约定不擅自提交/不 reset）。
