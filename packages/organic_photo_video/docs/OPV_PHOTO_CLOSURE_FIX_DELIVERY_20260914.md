# 图文生产收尾修复交付：素材入库、商品质检归因、旧计划兼容

日期：2026-09-14（夜）。对应方案：`docs/PHOTO_PRODUCTION_CLOSURE_FIX_PLAN_20260914.md`。
本文件是**实施结果与证据**，不是方案。方案正文保持收到时的原样，仅在头部补了一行状态。

---

## 1. 结论与提交版本

三个修复（A 素材入库防覆盖、B 核心商品定向重拍、C 旧计划语言窄兼容）**已实施并提交**：

| 项 | 值 |
| --- | --- |
| 本轮提交 | `70d82bc` — `fix(opv): 素材登记防覆盖、核心商品定向重拍、旧计划语言窄兼容` |
| 该提交之前 | `84a4b20`（HEAD 从 `84a4b20` → `70d82bc`） |
| 未推送 | 本轮新增 2 个提交（代码 `70d82bc` + 本文档），连同此前未推送的 10 个共 **12 个**未推 `origin/main`（远端仍停在 `3026091`）。本轮**未**推送 |
| 工作区 | 其余脏改动属其他会话（`remake_video_execution`、`original-script-generator`、`short-video-auto-publisher/app/creatok_publish.py`、根 `DREAMS.md` 等），**未触碰、未提交** |
| 沿用而非重建的既有修改 | `1d9b55e`（素材集键槽位守卫）、`2f53ead`（发布文案与档位随市场走）。`services/photo_locale.py` 的归属与 `select_execution_profile` 收敛保持不变，本轮**没有**第二份语言模块 |
| 本轮回归 | `1339 项 OK`（改前基线 `1312`；新增 27 条） |

方案 §一 的现状描述已过时（当时 HEAD `1d9b55e`、locale/profile 修复尚未提交）；本文件按实际状态写。

---

## 2. 开工前核对的现状（方案 §五.1）

- 本机与线上确有并发在跑：`opv_production_batch` 里 `waiting` 批次最近更新到 `2026-09-14 22:10`（`recvvcD2x9br89`），另有 `recvvuC0BJL8Nxa`(21:19)、`recvvuC5tEMsNj7`(21:17)、`recvvcgz2Kk41n`(21:04) 等。飞书扫描器由 launchd 每 30 分钟跑一次。**本轮一律未触碰这些行**，只做只读查询与临时目录里的离线验证。
- 三条相关行的任务态（只读）：`recvvcD2x9br89` / `recvvcgz2Kk41n` 均为 `photo_packaging`（按项目口径即**正常终态**）；`recvvcgvz0LsIh`（VN 围巾旅行 A 行）为 `draft`，**无批次** —— 即 §五.5 待做的那一行。

---

## 3. 最终配置状态（方案 §五.3：明确口径，不混用临时启用态）

本轮结束时是 **开发默认／安全关闭态**，不是审查期间用过的临时启用态：

| 门禁 | 现状 |
| --- | --- |
| 预设 `图文｜VN｜围巾旅行` | `disabled` |
| 预设 `图文｜VN｜围巾搭配四选一` | `disabled` |
| 配方 `PHOTO_TRAVEL_OUTFIT_V3` / `PHOTO_MATCHING_CHOICE_V3` | `draft` |
| Market Pack `MP_VN_DEFAULT_V1` | `draft` |
| 账号 `OPV_VN_TEST_001` | `paused` |
| 素材集（VN） | `ASSET_VN_SCARF_*` 全部 `enabled`（含 A 行已付费四张的 `…UPLOAD_fc774d7b7d52a969`） |
| 自检脚本判定 | `NOT ARMED` |

命令与结果：

```bash
/usr/bin/python3 tmp/real_gen_e2e_3rows/check_gates_3c.py      # -> NOT ARMED
/usr/bin/python3 scripts/preflight_native_photo.py --verify-files
# -> {"check":"config","external_writes":0,"errors":[],"ready":false,…,"file_checks":true} 退出码 0
```

`ready:false` 是**预期**：VN 两条线的预设未 enabled、V3 配方仍有 `CANARY_MARKET_UNBOUND`。这是「配置可解析、无错误」与「生产就绪」两个口径的差别，本轮**没有**为了跑绿在正式机上切换生产配置（方案 §五.4），也未删除任何停用断言。

---

## 4. 修复 A：素材入库不得覆盖历史行

### 4.1 落地

- `opv_asset_set` 有两个唯一键：`PRIMARY KEY (asset_set_id)` 与
  `UNIQUE KEY uq_opv_asset_set_version (asset_set_key, asset_set_version)`。通用 `_upsert`
  的 `ON DUPLICATE KEY UPDATE` 会命**任一**键 ⇒ 版本撞号时静默改写历史行（保留其原
  `asset_set_id`），本次的 content-addressed id 从未落地。`_upsert` 语义**未改**，素材改走
  专用方法：
  - `insert_asset_set`：普通 `INSERT`，冲突即回滚，绝不覆盖任何既有行；
  - 同 `asset_set_id` 重试：内容一致（key/category/market/tags/manifest）则幂等返回库中实际
    记录；内容不同则**拒绝原地改写**并报错；
  - 不同 id 争用同 `(key, version)`：按索引口径取版本（`next_asset_set_version` =
    `SELECT MAX(asset_set_version) WHERE asset_set_key=%s`，**不分状态/市场/类别**），重试上限
    `ASSET_SET_REGISTRATION_ATTEMPTS = 3`，超限保留 stage 与已生成图片并报「素材登记冲突，可重试登记」，
    **不触发生图**；
  - 状态更新只走 `update_asset_set_status`，只改 `status` 一列（`rowcount == 0` 抛 `StaleStatusError`）。
- `AssetSetService.save` 写入后回读不到记录即抛 `AssetSetError`，**不再**回落到从未持久化的内存对象。
- `photo_asset_supply.qualify` 的版本分配优先走 `next_asset_set_version`（索引口径），无该方法时退回旧算法；结尾回读自证改为只校验「本次 id 真的落了行且落在预期 key 上」。

**保留现有唯一索引**，未把 `status` 加进唯一键，未删除任何停用行来腾版本，未引入分布式锁。

### 4.2 验收对照（方案 §二 的 5 条）

| 验收 | 证据 |
| --- | --- |
| 1 同 key 最高版本已 disabled，新增仍取更高版本，旧行全字段不变 | `tests/test_rds_repository.py::test_disabled_row_still_owns_its_slot_and_is_never_modified` |
| 2 同 key 已有其他 category/market 版本，新插入不覆盖它 | `…::test_another_markets_row_is_not_overwritten_on_a_version_guess` |
| 3 两个不同素材争用同版本：各自存在或一方可重试失败，不互相覆盖 | `…::test_two_assets_racing_for_one_version_both_survive` |
| 4 同素材重复登记返回同一实际 id/version，无重复记录 | `…::test_same_id_retry_reuses_the_stored_version`、`…::test_same_id_with_different_content_refuses_in_place_edit` |
| 5 登记失败重试不调用图片生成器 | `tests/test_photo_profile_market_binding.py::test_write_that_cannot_be_read_back_fails_loudly_instead_of_pinning_a_ghost_id`（带下载计数，断言未增） |
| 重试有界、拒绝路径不留痕 | `…::test_retries_are_bounded_and_change_nothing`（断言插入次数 = 3、版本严格递增、表快照不变）、`…::test_status_retirement_touches_only_the_status_column` |

测试替身 `FakeAssetSetStore` **同时实现两个唯一键**（重复 id 或重复 (key,version) 都抛
`IntegrityError(1062)` 且不动表），`UPDATE … SET status` 按 MySQL 语义返回 **changed** rowcount，
非素材专用 SQL 直接 `AssertionError`。生产库不是冲突测试场。

### 4.3 A3 定向核查：保持停用，不重建（附理由）

只读核查脚本 `tmp/real_gen_e2e_3rows/probe_asset_set_overwrite_a3.py`，证据快照
`tmp/real_gen_e2e_3rows/asset_set_overwrite_evidence_a3.json`（全表 184 行）。

覆盖铁证 —— 两行的**创建时间与更新时间差了 9 天**，且行的 `market/category` 已被改成 VN/scarf，而
`asset_set_id` 里的市场段仍是 TH：

| 列 | `ASSET_TH_WOMENSWEAR_CHOICE_V1` | `ASSET_TH_WOMENSWEAR_CHOICE_V2` |
| --- | --- | --- |
| `asset_set_key / version` | `TH_WOMENSWEAR_CHOICE / 1` | `TH_WOMENSWEAR_CHOICE / 2` |
| `created_at` | `2026-09-05 14:10:00` | `2026-09-05 14:33:50` |
| `updated_at` | `2026-09-14 20:50:44` | `2026-09-14 20:46:29` |
| `market / category_key` | `VN / scarf` | `VN / scarf` |
| `id 里的市场段` | `TH` | `TH` |
| `status` | `disabled` | `disabled` |
| `reviewer` | `system_style_reference_generation` | `system_style_reference_generation` |
| `source_hashes` | 与本轮 VN 四张付费 look 的素材集 `ASSET_VN_SCARF_UPLOAD_fc774d7b7d52a969` **完全相同** | 同左 |

**未重建、未启用**，理由（方案 §二「缺少证据时保持停用并记录原因」）：

- 种子文件 `config/asset_sets/ASSET_TH_WOMENSWEAR_CHOICE_V1.json` / `V2.json` **不带
  `content_approval` 块**（`grep -c sha256` = 0），而 `import_photo_asset_set.normalize()`
  不合成该块 ⇒ 库里原行的 `content_approval.attributes` **无法确定性恢复**。按「不能用猜测内容
  恢复或启用」，保持 `disabled`。
- 未删除旧 ID、未批量改写历史任务 pin、未重导 TH/MX 配置。
- 引用关系（快照已存）：`…_V2` 被 **2 个冻结批次** pin（`opv_batch_8a5eb8b5debba39839302a11ca16ff99`
  / `recvul2HddIgFq`、`opv_batch_e311dbae24855c6b1fb1a5dfb95470b0` / `recvulMq5VuFh0`，均 09-05、
  `waiting`）；`…_V1` 无 pin。
- 盘上旧字节完好、未被本轮触碰：`shared/data/organic_photo_video/asset_sets/th_choice/look_a..e.png`（2026-07）。
- 本轮需要登记的 VN 已生成图片走的是修好的插入路径（新素材集 `ASSET_VN_SCARF_UPLOAD_*`），无需回填这两条 TH 行。

---

## 5. 修复 B：核心商品失败必须进入定向重拍

### 5.1 落地

- `photo_reference_vision.review_alignment` / `_normalize_role_findings`：逐 look 新增结构化布尔
  `core_product_mismatch`，**仅在本篇指定商品时为真**（`product_specified=bool(product_context)`）；
  旧响应缺该键 ⇒ 归一为 `False` ⇒ 老路径逐字不变。提示词在【核心商品】段要求模型显式给出该键，
  并明确「围法、褶皱、佩戴位置、细微纹理差异只写 notes，该键必须为 false」。
- `photo_style_reference_supply._failed_roles`：该键为真时**绕过中文关键词白名单**直接判失败；
  硬门禁集合由 `missing` 改名 `hard` 并并入 `core_product_mismatch`（模型整体 `passed` 不得覆盖逐
  Look 的明确硬失败）。**没有**把所有 `passed=false` 升级为硬失败。
- 组级修复循环：**空归因短路**放在 `overrun` 检查**之前** —— 没有可修复角色时不再「对原图再查一次」
  并把那记成完成一轮（旧行为会烧掉 `MAX_GROUP_REPAIR_ATTEMPTS=1` 后 `group_failed`），而是直接给
  可读错误并保留 QA 证据与全部成片。

### 5.2 验收对照（方案 §三 的 4 条）

| 验收 | 证据（`tests/test_photo_theme_reference.py`） |
| --- | --- |
| 1 判 B 商品不符 ⇒ 只重生 B 一次，A/C/D 字节不变；第二次通过则完成 | `test_core_product_failure_repairs_only_the_blamed_look`：视觉调用 scope 序列 `FIRST_LOOK_A → FULL_LOOK_GROUP → FULL_LOOK_GROUP`；生成请求只多 `(slot_index=2, shot_version=2)`；四张路径 `look-1_v1 / look-2_v2 / look-3_v1 / look-4_v1`；`attempt_history[0].retired` 只有 `look_b` |
| 2 修复仍失败时按既有上限结束，不新增 QA 轮次 | `test_failed_product_repair_stops_at_the_existing_attempt_cap`：报「重做次数已用尽」、`group_repair_attempts == 1`、模型调用 3 次、生成 5 次 |
| 3 未指定商品时颜色微差/围法/褶皱/普通配饰差异不触发新硬门槛 | `test_without_a_product_the_same_chinese_issues_stay_lenient`（去掉旗标 ⇒ 不触发任何重拍）；视觉层 `test_core_product_flag_is_forced_off_without_a_designated_product` |
| 4 上轮三个中文复现描述不再导致空转；判据是结构化信号 | `test_failure_without_a_repairable_role_does_not_count_a_phantom_round`：报「无法归因到任何单张」、**2 次**模型调用（不是 3）、**4 次**生成（不是 5）、`group_repair_attempts == 0`、`attempt_history == []` |
| 旧响应缺字段保持旧处理路径 | `test_legacy_response_without_the_field_keeps_the_old_path`（视觉层） |

---

## 6. 修复 C：语言兼容与文案修复不得连带重生源图

### 6.1 落地（两趟：先检测，再修复）

**C1 契约键窄兼容**（`photo_content_planner.load_or_create`）：

- `ADDITIVE_CONTRACT_KEYS = ("publish_locale",)`，且仅当配方声明了 `locale_copy_packs` 时才传入
  ⇒ **v1 泰语配方（契约逐字不含该键）完全不受影响**（已在 RDS 核对：`PHOTO_TH_TRAVEL_OUTFIT_V2`
  的 `locale_copy_packs` 为 `null`）。
- 判定只用 `additive_only_contract_change`：**移除该键后其余契约逐字一致**才算「只是新增了键」，
  否则保持原「主题、预设、参考模式或生成数量已变化」硬失败。
- 语言自证 `copy_language_matches_locale` 返回 `True / False / None`，**`None` 绝不当作「是」**：
  一致 ⇒ 只补记该键并重存 hash（计划本体逐字不动）；不一致 ⇒ `copy_language`；证不出 ⇒ `unverified`。
- **语言自证取样覆盖 look 标签**：`plan_copy_texts` 现在也读每张 look 的 `display_label`
  —— 它是下发到上片的文字（`photo_locale.family_copy` 的 `look_labels`，由 `services/photo_copy.py`
  消费，**不进图片生成提示词**），所以「copy 已换成越南语但标签还是泰语」必须被判成语言不符。
- 两种 `LegacyPlanLocaleUpgrade` 消息**都不在** `_is_replannable_photo_error` 白名单里 ⇒
  **不可能**静默走到归档付费素材。

**C2 只换文案字段**：

- 检测趟**不写任何东西**；工作流先把旧计划**复制**一份到 `replan_archive/<rec>_<ts>/`
  （新增 `_archive_frozen_plan_copy`，与「搬走」的 `_archive_photo_planning_state` 区分开：
  `content_plan` 与 `style_reference_supply` **原地保留**），再走第二趟
  `_load_content_plan(repair_copy=_build_content_plan)`。
- `adopt_repaired_copy(stored_plan, rebuilt_plan)` 用当前 Locale Pack 重建计划，然后**只采纳**
  文案字段（`copy` / `copy_source` / `template_review_status` 与每张 look 的 `display_label`），
  并逐条断言**除这些字段外（含计划顶层）逐字相同**；任何画面字段（`looks` 的穿搭/场景、
  `scene_zh`、`palette_zh`、`background_prompt`、顶层 `travel_place` 等）不一致即抛
  `reason="visual_changed"`，**什么都不写**。这正是不做「整份重规划后假定新计划与旧图仍相符」。
- 供给侧：`prepare(..., copy_repaired_from=<旧 plan 条目>)` → `allow_copy_only_rebaseline`
  要求 complete + 角色齐备 + 画面投影相同 + **文案投影确实变了** + `assert_identity_unchanged`
  （参考图哈希/主题/各角色穿搭规格签名/人物包）全过，才重写 `input_hash` 并在清单写
  `copy_rebaseline` 留痕。
- **未**全局删除 hash 中的字段、**未**无条件重设基线、**未**放宽整个指纹检查、**未**全量批改历史
  `plan.json`。

### 6.2 两个关键事实（决定了实现形态，都实测过）

1. **`supply_manifest.json` 从不保存 `variation`**（只存 `theme_brief`=`theme`、`sources[]`、
   `style_reference_paths`、`input_hash`）。所以「只改了文案」**只能由调用方**同时给出新旧两版
   计划条目来证明；任何「从清单里读旧 variation 再比」的写法必然恒判失败（本实现第一版就踩了这个坑，
   由 `test_copy_only_change_reuses_paid_looks_without_generating` 抓出）。
2. **「生成当时那版 variation」有现成真记录**：`style_reference_supply/<item>/manifest.json` 的
   `metadata.batch_variation`（管线自己写的）。实测它与归档旧计划条目**逐字相同**（`cover_selection`
   除外 —— 它是 `prepare` 之后补写的），这既验证了「用旧计划条目当对照物」的正确性，也让「生成当年
   的 input_hash」可以被**真实证据**算出来，用于离线复现。

### 6.3 验收对照（方案 §四 C 的 5 条）

| 验收 | 证据 |
| --- | --- |
| 1 同语言旧计划 + 只新增 publish_locale：图片路径/hash 不变、生成器零调用、可继续冻结排版 | `tests/test_photo_content_planner.py::test_same_language_plan_is_reused_after_backfilling_the_new_key`；**真实数据**见 §7 的 C1 脚本 |
| 2 旧 VN 泰文文案：保留原四张付费图、最终越南语通过文案校验、生成器零调用 | `tests/test_photo_theme_reference.py::test_copy_only_change_reuses_paid_looks_without_generating`（`generated_this_run == 0` + 生成器请求数不变 + 四张路径/sha256 全同 + `copy_rebaseline` 留痕）；**真实数据**见 §7 的 C2 脚本 |
| 3 真实改变场景或商品不被吞掉 | 计划层：`test_copy_repair_refuses_when_the_rebuild_moves_a_visual_field`、`test_copy_repair_refuses_a_rebuild_with_a_different_item_count`、`test_adopt_repaired_copy_refuses_a_moved_top_level_field`；契约层：`test_a_real_input_change_is_never_swallowed_by_the_compatibility`（商品/篇数变了连窄兼容都不进）；供给层：`test_copy_only_rebaseline_cannot_smuggle_in_a_real_visual_change`、`test_look_label_change_alone_also_reuses_the_paid_looks`（标签之外再动画面即拦） |
| 4 已冻结批次按原路径恢复，TH 成熟输出不变 | 全量 1339 项 OK（含全部 TH 旅行/搭配/发型 golden 与 `test_photo_copy_locale_binding.py`）；`PHOTO_TH_TRAVEL_OUTFIT_V2` 契约不含 `publish_locale` ⇒ 根本不进窄兼容分支 |
| 5 失败期间仍保留可恢复的计划、素材和修订记录 | 检测趟/拒绝路径一律不写盘（用例断言磁盘快照不变）；修复趟把旧计划**副本**存入 `replan_archive/`；供给清单只重写 `input_hash` 与 `copy_rebaseline`，图片字节与 `sources[]` 不动 |
| 安全分支：证不出语言 ⇒ 保留旧计划与素材，不猜 | `test_unprovable_language_keeps_the_old_plan_instead_of_guessing` |
| 安全默认：没有旧计划条目作对照 ⇒ 拒绝复用 | `test_a_copy_change_without_the_old_plan_item_is_refused` |

---

## 7. 真实数据离线证明（命令与实测输出）

两个脚本都在包内 `tmp/`（**gitignored**），**只写临时目录**，不写生产文件、不调模型、不写 RDS。

```bash
cd packages/organic_photo_video
PYTHONPATH=.:tests /usr/bin/python3 tmp/real_gen_e2e_3rows/probe_additive_locale_c1.py
PYTHONPATH=.:tests /usr/bin/python3 tmp/real_gen_e2e_3rows/probe_copy_only_repair_c2.py
PYTHONPATH=.:tests /usr/bin/python3 tmp/real_gen_e2e_3rows/probe_copy_only_repair_e2e_c2.py
PYTHONPATH=.:tests /usr/bin/python3 tmp/real_gen_e2e_3rows/preflight_a_row_hash.py   # §7.4
PYTHONPATH=.:tests /usr/bin/python3 tmp/real_gen_e2e_3rows/prove_a_row_zero_gen.py  # §7.4
```

**C1（真实行 `recvvcgz2Kk41n`，PHOTO_MATCHING_CHOICE_V3，契约缺 `publish_locale`、文案已是越南语）**
—— 第一趟：计划逐字不变、契约补记该键、`input_sha256` 由 `9d92934d…` → `4603e422…`、再跑一次直接命中；
负控制：把 `publish_locale` 塞成 `th-TH` ⇒ 抛 `reason='unverified'` 且磁盘一字未动（证明它真的在自证
语言，而不是听信调用方）。

**C2（真实行 `recvvcgvz0LsIh`，VN 围巾旅行，旧泰语计划 + 已付费四张 look）**
用归档旧计划 + 真实素材/参考合同在临时根里跑四趟：

| 趟 | 动作 | 实测 |
| --- | --- | --- |
| 1 | 检测 | 抛 `reason='copy_language'`，磁盘一字未动 |
| 2 | 就地只换文案 | 泰文字符数 `208 → 0`；`copy.title` 由 `ไอเดียแต่งตัวเที่ยว…` → `Gợi ý phối đồ du lịch…`；除文案外的条目字段与顶层**逐字相同**；look 的穿搭/场景逐字相同 |
| 3 | 供给层复用 | `generated_this_run == 0`，四张 sha256 与原来全同，哨兵生成器**零调用**（该行基线上一轮已被脚本重定，故走快路径） |
| 4 | 还原「生成当时」基线再跑 | 用 `metadata.batch_variation` 算出当年的 `input_hash`（`acffabdf…`）装进**临时**清单 ⇒ 重定基线为 `4f6af55e…`（**与线上清单现值完全一致**）、`copy_rebaseline.reason='copy_only_language_fix'`、`generated_this_run == 0`、图片字节不变 |

第 4 趟的价值：既覆盖了 `allow_copy_only_rebaseline` 全路径，又证明**新代码路径复现了上一轮手工脚本
的结果**（同一个 hash），而不是另生一套口径。

### 7.4 方案 §五.5：旅行 A 行的四张已付费图 → 越南语成片（已达成，附证据）

```bash
PYTHONPATH=.:tests /usr/bin/python3 tmp/real_gen_e2e_3rows/preflight_a_row_hash.py
PYTHONPATH=.:tests /usr/bin/python3 tmp/real_gen_e2e_3rows/prove_a_row_zero_gen.py
```

**结论：该目标已由 A2 行达成；A 行本身按代码口径只读，不应重跑。** 依据如下。

1. **四张已付费 look 完好。** `style_reference_supply/recvvcgvz0LsIh_item_1/` 的
   `P1–P4_v1.png`（mtime 20:30–20:34）实测 sha256 与清单 `sources[]` **逐一相同**：
   `5c2609c0…` / `8e05a92a…` / `ea363794…` / `d15115bb…`。

2. **它们已经被“复用”成了越南语成片（零付费生成）。** A2 行 `recvvcD2x9br89`
   （同一预设 `图文｜VN｜围巾旅行`、同一主题 `凉爽旅行`）：
   · 进度 = **已完成**，`预览/成片` = **5 个附件**，全部 `1080×1920`；
   · 任务 `opv_task_20260914_6e72edc87635` = `photo_packaging`，批次
     `opv_batch_cccb19dcb6cbda76626ccde979b331cd` = `waiting`（22:09 冻结）；
   · 四个镜头 `generation_provider='asset-reuse'` + `generation_model='file-copy-v1'`，
     `image_sha256` = 上面那四个值 ⇒ **纯文件复用，无一次付费生成**；
   · 该批次的四个 look 绑定的是**同一个** `asset_set_id`
     `ASSET_VN_SCARF_UPLOAD_cf7e3d7614e0392b`(v4)，即 A 行那四张图；
   · 冻结文案为**纯越南语**（`title=Gợi ý phối đồ du lịch Thành phố se lạnh`、`caption`、
     `hashtags=['#phoidodulich','#khanquang','#OOTD']`、`slide_texts`）：**泰文字符 0**、
     无 `{{ }}` 占位符、`locale/market = vi-VN/VN`。

3. **A 行不能重跑，这是代码明文规定的“出路”，不是缺省行为。**
   `services/feishu_workflow.py:1321`：

   ```python
   if batch and batch.batch_status == "cancelled":
       raise FeishuWorkflowError("该图文批次已取消；请新增一行重新发起，历史记录保持只读")
   ```

   A 行的批次 `opv_batch_2e8ac79d473ede2ad833dc47bbd57fcb` 正是 `cancelled`
   （21:47 作废；其冻结文案还是修复前的泰语 `ไอเดียแต่งตัวเที่ยวThành phố se lạnh`）。
   方案 §五.5 明写「**不删除批次强行回到首跑**」⇒ 不重置也不删除该批次。

4. **在稳定版本（HEAD）上，A 行素材段实测仍返回 `generated_this_run: 0`。**
   `prove_a_row_zero_gen.py` 把 A 行素材段原样拷进临时数据根，用 HEAD 的真实
   `PhotoStyleReferenceSupplyService.prepare()` 真跑（哨兵生成器被调用即抛）：

   ```
   generated_this_run = 0    生成器调用次数 = 0    generated_count = 4
   look_a/b/c/d sha256 与清单一致 = True（全部）
   临时副本内素材字节与生产一致 = True
   生产侧数据在本次证明前后字节不变 = True        RESULT: PASS
   ```

   即：`prior.input_hash == 现算 input_hash`（同为 `4f6af55e45d5604e…`）、清单 `complete`
   且四角色齐备 ⇒ 走 `prepare` 的 early-return 分支。这条既是 §五.5 的前置证明，也是
   **修复 C 在稳定版本上的真实数据验收**（"旧计划语言升级和文案修正不重新调用图片生成器"）。

5. **不新建行**：`style_reference_supply` 以**记录号**为目录键，新建行的素材段没有这份缓存，
   会真调图片生成器 = 付费；且与 A2 的成片（同文案、同四张图）重复。故按方案 §五.5 的
   「先检查修订路径」执行，不新造一行。A 行保持**永久只读留档**（不删历史批次/任务/图片）。

---

## 8. 未做 / 未验收（如实列出，不用模拟测试代替）

| 项 | 状态 | 说明 |
| --- | --- | --- |
| 方案 §五.5：复用 A 行（`recvvcgvz0LsIh`）现有四张图片完成越南语成片 | **已达成** | 见 §7.4：四张图完好，已由 A2 行 `recvvcD2x9br89` 以 `asset-reuse/file-copy-v1` **零付费**复用成 5 页越南语成片（进度=已完成）；A 行因批次 `cancelled` 被 `feishu_workflow.py:1321` 判为只读，按方案**不删批次强行首跑** |
| 方案 §五.6：选一条围巾四选一三篇任务验证恢复 | **未做（受阻）** | 该验证需要在**指定商品**下真实重拍 = 付费调用，且需真实同款商品资料；方案 §五.6 本身写明「未备齐时不阻塞已验证的自由搭配能力，但不能宣称商品模式已验收」。**待授权 + 待商品资料** |
| 商品模式（指定围巾）真实验收 | **未验收** | 本轮 B 的验收全部是流程/视觉层断言；真实同款商品资料未备齐，**不宣称商品模式已验收** |
| 真实发布到 VNPS01 | **未做** | 按方案 §五.7，不因修复完成自动发布；沿用既有确认发布授权与渠道 |
| A3 的 TH 行恢复 | **不做** | 缺可靠旧快照，保持 `disabled` 并记录原因（§4.3） |

已知的保守行为（会在真实运行中以可读错误暴露，而不是静默复用）：

- 语言修复时若「按当前语言重建」连**计划顶层字段**都变了（例如配方档位/变量在两次运行间变化），
  会以 `reason="visual_changed"` **拒绝修复**并保留旧计划与素材，要求人工确认后新建任务。这是刻意的
  失败关闭方向（宁可人工介入，不可拿旧图配新计划）。
- 供给层 `allow_copy_only_rebaseline` 要求调用方给出旧计划条目；缺该证据时一律拒绝复用（保守默认）。

---

## 9. 回归三件套（方案 §五）

```bash
PYTHONPATH=.:tests /usr/bin/python3 -m unittest discover -s tests -p 'test_*.py'
# Ran 1339 tests ... OK        （基线 1312 + 新增 27）
/usr/bin/python3 scripts/preflight_native_photo.py --verify-files
# {"check":"config","external_writes":0,"errors":[],"ready":false,…,"file_checks":true}  退出码 0
git diff --check
# 无输出
```

按方案要求，不为追求数量重复运行相同测试。预检只证明其报告范围内的配置，不代替真实出片和发布证据。

---

## 10. 下一步（需明确授权后才动生产）

1. ~~**§五.5**：`recvvcgvz0LsIh` 走一次续跑以复用四张已付费图完成越南语成片~~ ⇒ **已达成，见 §7.4**：
   目标由 A2 行以零付费复用完成；A 行按代码口径保持只读（批次已取消），**不删批次强行首跑**。
2. **§五.6**（需授权 + 需商品资料）：一条围巾四选一三篇任务的恢复验证。需要真实同款商品资料，
   且真实重拍会产生生图费用；未备齐前保持「未验收」，不宣称商品模式已验收。
3. A 行与四选一都通过后，再按既有授权决定是否确认发布到 `VNPS01`（§五.7：不自动发布）。
