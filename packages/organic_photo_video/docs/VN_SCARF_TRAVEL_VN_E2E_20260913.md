# VN 围巾跨市场｜旅行线真实生产链端到端验收（Phase 5 第二项）

- **日期**：2026-09-13
- **范围**：`packages/organic_photo_video/`
- **前置**：`e6278a0`（旅行线 VN execution profile）；`495872e`（搭配线端到端验收）；`df6d651`（Review 3×P0 整改）
- **本轮授权**：用户对我上一轮结尾提议「让旅行线在 VN 真正可跑」回复 **「现在就跑」**
- **结论**：旅行线在 VN 用**真实素材集 + 真实套版 + 真实回写**一次跑通：5 页 / 4 张唯一图，
  1080×1920 JPEG，镜头 `qa=passed`，飞书回写 5 张成片、`进度=已完成`。
  过程中发现并修复 **1 个真实缺陷**（VN 旅行文案封面 3 行超出 `max_lines=2`）与
  **1 个运行态死锁**（批次取消后同行无法重跑）。

---

## 1. 本轮跑的到底是什么

不是干跑，是按 `opv-real-gen-e2e-verification` 分层流程走的**真实生产链**：

```
武装四道闸门 → 惰性测试行 → run_feishu_scanner_lock.py --record-id <rid>
   ├─ 建 RDS 生产批次（冻结清单 + 占用内容签名）
   ├─ 建内容任务 + 镜头（复用 VN_SCARF_CHOICE 真实素材）
   ├─ 套版合成 1080×1920 JPEG
   └─ 回写飞书（预览/成片 + 进度 + 备注）
→ 核验 → 复位四道闸门 → 收尾测试行
```

四道闸门（全部可逆，见 §5）：

| 闸门 | 判据（代码位置） | 本轮取值 | 复位后 |
|---|---|---|---|
| 生产预设 `status` | `ProductionPresetCatalog._require_enabled`（`feishu_workflow.py:472`，只读 `status`） | `active` | `disabled` |
| 配方 `status` | `photo_request_factory.py:226` | `active` | `draft` |
| Market Pack `status` | `preflight.py:69` | `active` | `draft` |
| 账号 `status` | `task_intake.py:115`（`ALLOWED_ACCOUNT_STATUSES=("active","testing")`） | `testing` | `paused` |

> 闸门里的「素材集 `enabled`」**不是**路由闸门（素材集只做候选过滤，不做准入），故不计入四道。

---

## 2. 发现的真实缺陷：VN 旅行文案封面 3 行

### 2.1 现象

首次运行在**封版（compose）阶段**抛错：

```
PhotoPackageError: overlay text exceeds max_lines=2
```

（`services/photo_package.py:238`）

### 2.2 根因

被判定的文案变体是 `travel_photo_spot_v1`，其冻结进批次 manifest 的封面是 **3 行**：

```
Chụp ảnh ở Thành phố se lạnh
Look nào lên ảnh đẹp nhất?
Chọn A B C hay D
```

而旅行线封面用的版式 `PHOTO_TRAVEL_CARD_V3` 写死 `max_lines=2`
（`split_last_line_to_bottom=true`，即第 2 行会被挪到底部条）。3 行 → 直接抛错。

**为什么此前从未暴露**：`execution_profiles[0]` 是 TH 键（`asset_set_keys=["TH_WOMENSWEAR_CHOICE"]`），
所以这 4 条 vi-VN 文案在 Phase 5 之前**从未被任何真实请求选中过**；profile[1] 一上线就命中了它。
换句话说：这不是新引入的 bug，是**一直被遮住的既存文案缺陷**，被本轮真实链路照出来了。

### 2.3 修法（用户裁定：「改文案（推荐）」）

把 3 行压成 2 行，**与本仓既有范式一致**——同一作者的 `travel_which_look_v1`
本来就是 2 行（`标题` + 共享 CTA `Chọn A B C hay D`），且所有 `CHOICE_GRID` 版式都是 `max_lines=2`。

改法：**第 1 行用该变体自己的 `title`，第 2 行用共享 CTA**，两个 profile
（`travel_scene_four_looks` 与 `travel_scene_four_looks_vn`）同步，共 **6 行**（3 变体 × 2 profile）：

| copy_id | 修复前（3 行） | 修复后（2 行） |
|---|---|---|
| `travel_weather_pick_v1` | 原 3 行 | `Đi {destination} mặc gì khi {temperature}?` / `Chọn A B C hay D` |
| `travel_moment_pick_v1` | 原 3 行 | `4 look cho lịch trình ở {destination}` / `Chọn A B C hay D` |
| `travel_photo_spot_v1` | 原 3 行 | `Diện gì để chụp ảnh ở {destination}?` / `Chọn A B C hay D` |

`travel_which_look_v1` 本就是 2 行，**一个字节未动**。

> 注意：TSV 里的换行是**字面 `\n` 转义**（不是真换行），由 `config.loader` 还原。
> 因此 `grep`/`wc` 看到的「1 行」是格式假象，必须看解析后的结果。

### 2.4 修复的可证性

`tmp/vn_real_gen/dryrun_travel_copy.py`（只读干跑，走真 `PhotoRequestFactory`）覆盖全部 4 个变体：

```
[OK ] travel_moment_pick_v1    lines=2  '4 look cho lịch trình ở Thành phố se lạnh\nChọn A B C hay D'
[OK ] travel_which_look_v1     lines=2  'Gợi ý phối đồ du lịch Thành phố se lạnh\nChọn A B C hay D'
[OK ] travel_photo_spot_v1     lines=2  'Diện gì để chụp ảnh ở Thành phố se lạnh?\nChọn A B C hay D'
[OK ] travel_weather_pick_v1   lines=2  'Đi Thành phố se lạnh mặc gì khi 15-22°C?\nChọn A B C hay D'
→ 全部变体封面 ≤ 2 行；max_lines=2 不再被触发
```

**连带改动**：两条 golden canary 夹具
（`PHASE4_CANARY_VN_SCARF_STYLE{,_PRODUCT}.json`）的 `content_plan` 含文案，必须重建以匹配
（`python3 tests/photo_vn_canary_fixture.py`），否则 `test_shipped_fixtures_match_a_fresh_rebuild` 失败。

---

## 3. 发现的运行态死锁：批次取消后，同一行永远无法重跑

### 3.1 现象

第二轮同一行重跑，扫描器返回：

```json
{"scanned":1,"eligible":1,
 "processed":[{"record_id":"recvv5RcJIxHpB","action":"leased_skip",
   "reason":"该批次仍有运行租约，请勿重复执行"}],"errors":[]}
```

### 3.2 根因（三件事叠加）

1. **批次 id 由行 id 决定**：`feishu_workflow.py:1023` 用
   `batch_id = "opv_batch_" + canonical_hash(record.record_id)[:32]` → **同一行恒映射同一批次**。
2. **`claim_batch_run` 拒绝已取消批次**（`repositories/rds_repository.py:766`）：

   ```sql
   UPDATE opv_production_batch SET run_owner=...,lease_until=...
   WHERE batch_id=%s AND batch_status<>'cancelled'
     AND (run_owner IS NULL OR lease_until<UTC_TIMESTAMP(6))
   ```

   批次一旦 `cancelled`，该谓词**恒不成立** → `BatchLease` 构造抛
   `BatchLeaseBusy("该批次仍有运行租约，请勿重复执行")` → 记 `leased_skip`。
   **注意报错文案有误导性**：此时 `run_owner`/`lease_until` 其实都已经是 `NULL`（取消时已清空），
   真正的原因是 `batch_status='cancelled'`。
3. **孤儿任务会被复用**：`feishu_workflow.py:1046` 用
   `source_record_id == "{record_id}:{index}:{recipe_id}:1"` 匹配已有任务并复用其**冻结计划**，
   即使批次重建，也会拿到上一轮的旧计划（含 3 行文案）。

⇒ **结论：一条行在批次被取消后是不可恢复的，只能换行重跑。** 这是本轮最有价值的运维发现
（`scripts/cancel_native_photo_batch.py` 的文档只说「保留批次行供审计」，未说明这一后果）。

### 3.3 处理

**不删除任何 RDS 行**（删除是外部不可逆动作，交由用户决定）。改为：

1. 首轮行 `recvv5RcJIxHpB` 标注为**已作废**（`执行=false`，备注写明原因），保留审计；
2. 新建行 `recvv5V0wlPuqm`（`create_vn_travel_row.py --new`）走**冷启动**路径 → 新批次、新任务。

因首轮批次已 `cancelled`（内容签名已释放），新批次可正常占用同一签名（见 §4.3）。

---

## 4. 验收证据

### 4.1 运行输出（真实链路，非干跑）

```json
{"scanned":1,"eligible":1,
 "processed":[{"record_id":"recvv5V0wlPuqm","action":"generate_native_photo",
   "task_ids":["opv_task_20260913_107111854f53"],"photo_count":5,"failures":[]}],
 "errors":[],"feishu_request_count":8,"feishu_retry_count":0}
```

### 4.2 解析绑定（Phase 5 的核心主张，在**真实链路**中被证实）

冻结进批次 manifest 的请求：

| 字段 | 值 |
|---|---|
| `recipe_id` | `PHOTO_TRAVEL_OUTFIT_V3` |
| `profile_id` | **`travel_scene_four_looks_vn`**（`execution_profiles[1]`） |
| `profile_label` | 旅行场景：四套完整 Look 选一（越南市场） |
| `asset_set_key` | **`VN_SCARF_CHOICE`** |
| `asset_set_id` | `ASSET_VN_SCARF_CHOICE_V1` |
| `market` / `locale` | `VN` / `vi-VN` |
| `copy_variant_id` | `travel_moment_pick_v1`（已修复的 2 行变体） |
| `asset_snapshot` | 8 条 |

即：`profile[1]` **原生**把 VN 请求绑到 VN 素材集，**无需任何 override**。

### 4.3 批次 / 任务 / 镜头

| 项 | 值 |
|---|---|
| 批次 | `opv_batch_cfdd12dedc918757958be596044c23df`，`status=waiting`，`expected_count=1`，退租后 `run_owner/lease_until=NULL` |
| 内容签名 | `f92ac4cc…b5e5`（与首轮相同——**签名不随文案变化**，故取消后释放/重占行为一致） |
| 任务 | `opv_task_20260913_107111854f53`，`requested_shot_count=5` |
| 镜头 | **4 条**，`slot 1..4` = `look_a/b/c/d`，全部 `shot_status=generated`、`qa_status=passed`，4 个 sha256 **互不相同** |
| 素材来源 | 文件名带 `_v1_reuse.png` → 走 **ASSET_REUSE**（复用真实素材，未调付费生成） |

**5 页 / 4 张唯一图**：`apply_travel_single_cover`（`feishu_workflow.py:1868`）把 Look A 的
重复详情页折叠，故页数 5、唯一图 4。这正是旅行线的预期形态。

### 4.4 成片（字节级）

`shared/data/organic_photo_video/photo_packages/opv_task_20260913_107111854f53/opv_rev_20260913_cc21ac3e4526/final/`

| 文件 | 尺寸 | 格式 |
|---|---|---|
| `01.jpg` … `05.jpg` | 每张 **1080×1920** | JPEG |

人工目检：封面 `01.jpg` 的 2 行文案**完整无溢出**；`05.jpg` 的
`split_last_line_to_bottom` 生效（第 2 行「Look nào hợp lịch trình của bạn?」落在底部条）。

### 4.5 飞书写回

| 字段 | 值 |
|---|---|
| `进度` | **已完成** |
| `预览/成片` | **5 个附件**：`01.jpg`…`05.jpg` |
| `备注` | 「已完成 1/1 篇原生图文，共 5 张；技术检查已通过…」 |
| `执行` / `确认发布` | `false` / `false`（不得发布） |
| `图文生成失败的原因` | 空 |

### 4.6 零漂移

| 项 | 运行前 | 运行后 |
|---|---|---|
| 全表记录数 | 183 | 183（无增删） |
| 进度直方图 | 已完成 68 / None 38 | **已完成 69 / None 37** |
| 其他行 | — | **无一变化**（仅目标行 `None → 已完成`） |
| `执行=true` 行 | 2 行（既有） | 2 行（同前，目标行跑完自动复位） |

### 4.7 回归（零行为变化）

全量单测：`Ran 1200 tests in 77.126s — OK`（与文案修复前**同为 1200**，无回归）。

> 命令：`/usr/bin/python3 -m unittest discover -s tests -p 'test_*.py'`
> （本仓无 pytest；必须用 `/usr/bin/python3`——托管 3.13 缺 `pymysql`/`PIL`）

---

## 5. 闸门复位与残留

### 5.1 复位

- `preset` → `disabled`；`Market Pack` → `draft`；`recipe` → `draft`；`account` → `paused`。
- `config/feishu_production_presets.json` **字节级还原**（`git diff` 为空；本轮武装用的是
  「定向 1 词替换」而非 `json.dumps` 重排，故无需 `git checkout` 兜底）。

### 5.2 有意保留的 RDS 残留（非缺陷，已登记）

| 残留 | 说明 |
|---|---|
| 批次 `opv_batch_75c45778ff2bb12c0aa0b6745682a3a7`（`cancelled`） | 首轮批次，`inventory` 已释放；保留供审计 |
| 任务 `opv_task_20260913_22d603eda6ae`（`image_review`）+ 1 包 + 1 修订 + 4 镜头 | 首轮孤儿产物；**未删除**（避免在生产库做不可逆删除） |
| RDS 配方 `PHOTO_TRAVEL_OUTFIT_V3` 已含 `profile[1]` | 重新播种的**有意**结果（config↔RDS 一致）；`status` 仍 `draft`，**不可路由**，故对生产零影响 |

### 5.3 播种爆炸半径（已证）

`tmp/vn_real_gen/snapshot_seed.py` 对 8 个实体族（共 203 实体）做前后快照 diff：
改动**恰好 1 个实体**（`PHOTO_TRAVEL_OUTFIT_V3` 的 profile 数 1→2）；账号 / 素材集 / 主题 / Pack 全部未动。

---

## 6. 仍未做 / 后续（Phase 5 剩余）

| 项 | 说明 |
|---|---|
| **VN 文案母语终审** | 全部 `language_review_status=DRAFT`；**成片不得作为发布素材**（本轮已写入备注声明） |
| VN 本地人设 | 仍复用泰语库人设 |
| TikTok 原生图文发布能力 | 仍未确认 → 预设保持 `disabled` 的三大理由之一 |
| 视觉质检 | 未执行（未调用外部视觉模型），只在备注里如实声明 |
| 批次取消后的「换行重跑」 | 本轮用新行绕过；是否要在 `cancel_native_photo_batch.py` 里显式提示此后果，待定 |
| 删除首轮作废行 / 孤儿 RDS 产物 | **故意留给用户决定**（外部不可逆动作） |

---

## 7. 文件清单

**入库（本次独立提交）**
- `config/copy_packs/VN_TRAVEL_OUTFIT_V1.tsv`（M，6 行：3 变体 × 2 profile）
- `tests/fixtures/PHASE4_CANARY_VN_SCARF_STYLE.json`（M，重建）
- `tests/fixtures/PHASE4_CANARY_VN_SCARF_STYLE_PRODUCT.json`（M，重建）
- 本交付文档（新增）

**不入库（`tmp/` 已 gitignore）**
- `tmp/vn_real_gen/`：本轮全部工具与证据（`fix_vn_travel_copy.py`、`dryrun_travel_copy.py`、
  `create_vn_travel_row.py`、`arm_vn_travel_row.py`、`arm_travel.py`、`arm_account.py`、
  `snapshot_seed.py`、`verify_travel_run.py`、`finalize_vn_travel_row.py`、
  `obsolete_vn_travel_row.py`、`travel_run2.log`、`travel_baseline3.json`、`verify_travel_run.json` 等）
