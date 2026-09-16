# 复刻长视频统一接入原创长视频 Plan C 媒体生产 — 交接报告

改造范围：`/Users/likeu3/.openclaw/workspace`（安装镜像 `/Users/likeu3/.codex/skills` 未改动，`git status` 已核对）。
本次**未提交 Git**，工作树保留全部改动供人工审阅。

结论：复刻长视频已不再是独立媒体生成系统。复刻稿写入统一总库后，走与原创完全相同的运营入口、
状态推进、Plan C 媒体生成、续跑、成片校验与飞书回写；唯一保真的差异是**复刻稿不被重新创作或改写**。

---

## 一、修改文件

### 新增（4 个源文件 + 5 个测试文件）

| 文件 | 职责 |
| --- | --- |
| `packages/remake_video_execution/remake_video_execution/plan_c_handoff.py` | 复刻编译器 → 共享 Plan C 执行器的**唯一接缝**。`build_plan_c_handoff()` / `build_remake_keyframe_package()` / `strip_visible_text_lines()` |
| `skills/original-script-generator/core/longform/subtitles.py` | 字幕后处理：消费 `plan.postprocess_contract.subtitles`，在源时间轴烧录冻结可见文字 |
| `packages/remake_video_execution/tests/test_plan_c_handoff.py` | 适配层 13 项 |
| `skills/original-script-generator/tests/test_longform_audio_modes.py` | 音频状态机 10 项（含真实静音 AAC 成片） |
| `skills/original-script-generator/tests/test_longform_feishu_production.py` | 统一生产器 12 项（真实 SQLite + 真实 ffmpeg 成片） |
| `skills/original-script-generator/tests/test_longform_subtitles.py` | 字幕后处理 13 项（含像素级"文字真的画上去了"验证） |
| `skills/original-script-generator/tests/test_longform_storage_migration.py` | 旧库原地升级 6 项 |

### 修改（9 个源文件 + 3 个测试文件 + 1 份说明）

| 文件 | 改动 |
| --- | --- |
| `core/longform/storage.py` | 来源身份列迁移 + `remake_job_id()` + `find_job_by_source()` + 部分唯一索引 |
| `core/longform/audio.py` | 新增 `finalize_silent()`（静音 AAC 轨，可复用已批准 BGM） |
| `core/longform/voiceover.py` | 新增 `measure_frozen_source_voiceover_with_edge()` + `SOURCE_VOICEOVER_OVERFLOW` |
| `core/longform/workflow.py` | 音频三模式状态机、付费前 `BLOCKED_AUDIO_MODE` 阻断、冻结原口播 TTS 分支、静音成片分支、字幕后处理接入 |
| `scripts/run_feishu_longform_production_tasks.py` | 双来源候选筛选、现场建 job、回写 job ID、源稿变更阻断 |
| `packages/remake_video_execution/.../planner.py` | `plan_c_segment_count()` + `plan_segments_for_plan_c()`（整秒预算 14/14/13） |
| `packages/remake_video_execution/scripts/run_real_flow.py` | 停用生产职责：删除商品硬编码纠偏与 41 秒硬编码 |
| `skills/script-run-manager-sync/scripts/openclaw_original_batch_sync.py` | 入口文案 → `1/2 统一长视频生产（原创 + 复刻）` |
| `packages/remake_video_execution/README.md` | 正式入口、调试职责、适配层、字幕后处理四节 |
| `skills/script-run-manager-sync/tests/test_production_route.py` | 路由 10 项（原 4 项 + 6 项边界扫描） |
| `skills/script-run-manager-sync/tests/test_openclaw_original_batch_sync.py` | 入口 13 项（原 7 项 + 6 项统一入口） |
| `core/longform/workflow.py` 附带修复 | `BLOCKED_AUDIO_MODE` 事件原先在 `_persist_report()` **之后**才追加，导致报告里查不到阻断原因；已调整为与 `EDGE_TTS_PREFLIGHT_FAILED` 一致（先记录再落报告） |

---

## 二、数据迁移

`longform_job` 表**只做 `ALTER TABLE ADD COLUMN`**，不 DROP、不重建、不回填：

```
source_kind            TEXT NOT NULL DEFAULT 'ORIGINAL_GENERATED'
source_record_id       TEXT NOT NULL DEFAULT ''
source_script_id       TEXT NOT NULL DEFAULT ''
source_revision_hash   TEXT NOT NULL DEFAULT ''
```

```
CREATE UNIQUE INDEX IF NOT EXISTS idx_longform_job_source_identity
ON longform_job(source_kind, source_record_id, source_revision_hash)
WHERE source_record_id <> '' AND source_revision_hash <> ''
```

- 历史原创行读回即为 `ORIGINAL_GENERATED`，来源 ID 为空，**继续照常续跑**；部分索引的 `WHERE` 让大量空身份的历史行不冲突。
- `remake_job_id(record_id, source_revision_hash)` → `LFR_` + sha256 前 20 位；同一冻结稿永远同一 job，改稿产生新 job 而不是覆盖旧 job。
- 旧独立库 `/Users/likeu3/.openclaw/shared/data/remake_video_execution.sqlite3` **未迁移、未触碰**。若其中已有远端 task ID，必须先人工审计。
- 未新增审核表，未新增业务字段到飞书表（沿用既有 `PRODUCTION_SCRIPT_FIELDS`）。

---

## 三、路由变化

`REMAKE_SEGMENTED` 的语义收敛为「**复刻专用编译入口**」（仍指向统一生产器，不是独立媒体生成系统）：

| 行形态 | 路由 | 执行 |
| --- | --- | --- |
| 来源=视频复刻/成功脚本复刻 且 时长>15s | `REMAKE_SEGMENTED` | 现场编译 Plan C job → 与原创共用全部下游 |
| 来源=复刻 且 显式标记长视频（即使 ≤15s） | `REMAKE_SEGMENTED` | 同上 |
| 来源=原创生成 且 视频形态=长视频 | `ORIGINAL_LONGFORM` | 复用生成器已建的 job |
| 其余（含 10s/15s 复刻） | `SHORT_VIDEO_RUN_MANAGER` | 不进长视频生产器 |
| `视频形态` 误填「短视频」但时长>15s 的复刻 | `REMAKE_SEGMENTED` + `conflict` 提示 | **不**被短 worker 抢走 |

统一入口保留，未扩大自动发布权限：

```bash
python3 .../script-run-manager-sync/scripts/openclaw_original_batch_sync.py check --limit 20
python3 .../script-run-manager-sync/scripts/openclaw_original_batch_sync.py sync  --limit 20
```

`check` 不带任何付费参数；`sync` 才展开 `--allow-real-submit --allow-external-tts`；
`2/2 15秒短视频同步` 仍单独运行，白名单校验（`record_id` / `product_code` / `limit`）两条分支共用。

**发布边界未变**：本次只统一「生成成片」。`确认发布` 仍需人工勾选，未扩大任何自动发布权限。

---

## 四、状态合同

### 声音模式（付费前阻断的第一道闸）

| mode | 行为 |
| --- | --- |
| `PRESERVE_SOURCE_COPY` | 冻结原口播：真实 Edge TTS 只**测量**，`rewrite_allowed=false`、`revision_limit=0`，`target_text_sha256` 逐字节不变；只加速不放慢，溢出报 `SOURCE_VOICEOVER_OVERFLOW` |
| `NO_VOICEOVER` | 完全不调 TTS；成片仍写入真实静音 AAC 轨，校验与发布兼容 |
| `UNSPECIFIED` / `DIALOGUE_REQUIRES_PROVIDER` | `status=BLOCKED_AUDIO_MODE`，**H3 提交前**阻断（不构造网关请求、不 preflight、不 submit、不 query、不 download） |
| 无声明（原创） | 历史行为不变，允许中央口播模型做**一次**定向修订 |

### 关键帧 / 边界

- `SEG_01/02/03` → `A/B/C`；K0 = `MASTER_OPENING`；
- `CONTINUOUS` → 上一段计划尾帧 `K{n}_PLANNED` + 实际尾帧 `K{n-1}_ACTUAL`，`generation_mode=first_last`；
- `CUT` → 独立 `S{X}_ENTRY`，`generation_mode=first_frame`；
- 段数只支持 2–3 段（约 16–45s），段时长必须落在 4–15s 整数窗口；否则 `REMAKE_SEGMENT_COUNT_UNSUPPORTED` / `REMAKE_SEGMENT_DURATION_UNSUPPORTED` **阻断而非截断**。

### 幂等与安全

- job 材料含 `record_id + source_revision_hash`；重复保存同一身份只 1 行；
- 已存在远端 task ID / READY 片段的行改稿 → `SOURCE_CHANGED_AFTER_SUBMIT` 阻断，不覆盖不重复付费；
- 未付费的旧 revision 可安全重编（仅修指针）；
- dry-run 全程只读：不建字段、不下载附件、不写 SQLite、不建 job、不写飞书。

### 字幕后处理

适配层输出 `postprocess_contract.subtitles`（`PRESERVE_SOURCE_TIMELINE` + cue），
执行器在**合并后、混音前**按源时间轴烧录（ASS + libass）。cue 文本逐字保留，不翻译不改写；
泰语目标解析泰文字体，缺字体直接报错而**不是**画出方框；原创计划无此合同 → 成片与改造前一致。

---

## 五、测试结果

全部离线，使用 `/usr/bin/python3`（3.9.6，含 requests/httpx）。命令从各 skill 目录内执行。

```
# 1) 长视频全部 tests/test_longform_*.py（16 个模块）
Ran 143 tests ... OK

# 2) 生产脚本 / 批处理（feishu、renderer、openclaw task、batch executor/allocator/report）
Ran 106 tests ... OK

# 3) 复刻包全部（含适配层 13 项）
Ran  48 tests ... OK

# 4) 路由 + 统一入口 + 其余 run-manager
Ran 122 tests ... OK
```

合计 **419 项通过，0 失败**。其中本次新增/扩展 **66 项**：
适配层 13、音频状态机 10、统一生产器 12、字幕后处理 13、旧库迁移 6、路由边界 +6、统一入口 +6。

真实依赖已覆盖，非纯 mock：
- 真实 SQLite：旧库原地升级、幂等保存、部分唯一索引约束；
- 真实 ffmpeg：静音 AAC 成片（校验音频流存在）、字幕烧录（黑底帧峰值亮度 `>128` vs 源帧 `<16`，证明字形真的画上去了，且窗口外不画）；
- 真实飞书接口（只读）：`check --limit 20` 返回 0，读源表 169 条、长视频候选 0（当前无行勾选"进入生产"），`1/2` / `2/2` 文案正确，全程无写入。

---

## 六、遗留限制（务必人工确认）

1. **未做任何真实付费验证**：未调用真实 H3、未调用真实 Edge TTS 落盘口播、未真实写飞书。全部为离线 + mock。
2. **未生成真实样片**。适配层只在单测中验证合同，真机表现（尤其泰文口播时长实测）需按第七节跑一遍。
3. **旧复刻独立库未迁移**，也刻意不做自动迁移（按任务书要求）。若其内已有远端 task ID，必须先人工审计并回收，否则存在重复付费风险。
4. **`run_real_flow.py` 已降级为调试脚本**。任何人若仍按旧文档执行它，不构成生产入口；其 BGM/字幕/时间窗必须显式传入。
5. **字幕后处理仅覆盖 `PRESERVE_SOURCE_TIMELINE`**。源稿若声明了 `字幕` 但没有 `屏幕文字/字幕：` 结构化前缀，cue 为空 → 成片无字幕，需人工确认是否补结构化提示。
6. **字幕字体依赖 macOS 系统字体**（Thonburi / Arial Unicode）。换机部署需确认这些路径存在，否则会在烧录前直接报错（这是刻意设计，避免出现方框）。
7. **单段 ≤15 秒上限**由 Plan C H3 能力决定；41 秒稿按整秒预算切成 14/14/13，与源稿秒点不必逐帧对齐（边界按镜头边界吸附）。
8. 工作区另有**大量与本改造无关的未提交改动**（`organic_photo_video`、`short-video-auto-publisher`、`DREAMS.md` 等），本次未触碰、未清理、未提交。

---

## 七、真实样片验证步骤（建议按序执行）

> 前置：确认 `METASO_MINIMAX_API_KEY` 等凭证可用；在**测试商品**上验证，避免污染生产。

1. **选一条已冻结的复刻稿**（16–45s，总库「脚本来源=视频复刻」，`产品图片` 有值，`发布用途`/`是否挂车` 非空，声音模式明确）。
2. **只读预演**：
   ```bash
   python3 /Users/likeu3/.openclaw/workspace/skills/script-run-manager-sync/scripts/openclaw_original_batch_sync.py check --limit 20
   ```
   预期：该行出现在「1/2 统一长视频生产（原创 + 复刻）」列表，标注 `REMAKE_SEGMENTED` 与 `(待创建)`。
3. **确认阻断项**：若该行声音模式未声明，会以 `AUDIO_MODE_UNRESOLVED` 拦截在付费之前 —— 这正是预期行为，应先补源稿而不是绕过。
4. **勾选「进入生产」后跑单条 sync**（先单条，勿批量）：
   ```bash
   python3 .../openclaw_original_batch_sync.py sync --record-id <recId> --limit 1
   ```
5. **核对产物**：`~/.openclaw/shared/data/longform_original_video/<LFR_xxx>/`
   - `merged_silent.mp4` → `merged_captioned.mp4`（若源稿含可见文字）→ `final_video.mp4`；
   - `subtitles.json`（字幕清单）、`finalization.json`（混音/静音清单）；
   - `execution_report.json` 中应能看到 `SUBTITLES_BURNED`、`FINAL_READY`（或 `FINAL_READY_SILENT`）事件。
6. **人工验收四个点**：① 画面是否与复刻稿逐镜一致且**未新增剧情/卖点/台词**；② 观众可见文字是否为源稿原文字（泰文不缺失、不变形）；③ 口播是否为**原稿原文**（不得被改写，可与源稿逐字比对）；④ 段间衔接是否有硬切错位。
7. **飞书行核验**：`长视频执行状态（系统）=已完成`、`长视频成片（系统）` 有附件、`进入生产` 已取消勾选、`长视频错误信息（系统）` 为空。
8. **远端续跑**：若第 4 步返回 `WAITING_REMOTE`，勾选保持，再跑同一命令即可续跑（不应重复提交）。
9. 验收通过再决定是否提交 Git。

---

生成时间：2026-09-12
