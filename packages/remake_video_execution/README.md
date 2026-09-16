# Remake Video Execution

复刻提示词的内容保真分段执行域。它把统一脚本总库中的最终人工稿冻结为权威输入，解析源时间轴，按生成渠道的单段能力编排片段，并为后续 H3、桥帧、音频、字幕和回写保存独立状态。

当前已接通：

- 带秒点提示词的确定性解析；
- 镜头覆盖检查和不超过 15 秒的分段计划；
- 无口播、源稿旁白和未指定声音的区分；
- 参考素材、用途、挂车和可见文字语言的预检；
- 独立 SQLite 任务、片段、提交尝试和回写队列；
- Plan C H3 网关薄适配与真实提交显式授权；
- 共享总库长复刻与普通单段 worker 的路由隔离。

只读预览：

```bash
PYTHONPATH=/Users/likeu3/.openclaw/workspace/packages/remake_video_execution \
python3 -m remake_video_execution.cli check \
  --input /absolute/path/to/records.json \
  --output-dir /absolute/path/to/preview \
  --source-kind 视频复刻
```

冻结计划到独立数据库：

```bash
PYTHONPATH=/Users/likeu3/.openclaw/workspace/packages/remake_video_execution \
python3 -m remake_video_execution.cli plan \
  --input /absolute/path/to/records.json \
  --output-dir /absolute/path/to/plan \
  --source-kind 视频复刻
```

当前不自动提交真实视频。实际素材缺失或能力检查未通过时保持阻断；后续执行器必须消费冻结计划，不能重新理解或改写源稿。

## 正式生产入口（唯一）

复刻长视频**不再**使用本包的 `scripts/run_real_flow.py` 作为生产入口。正式运营流程与原创长视频完全统一：

```text
复刻任务生成脚本 → 写入“原创视频生产脚本”总库 → 运营审核 → 勾选“进入生产”
→ openclaw_original_batch_sync.py
→ 统一长视频生产器识别 REMAKE_SEGMENTED
→ plan_c_handoff.py 把冻结复刻稿编译成 Plan C job（A/B/C）
→ 与原创共用的 K0/进入帧、H3、远端续跑、TTS/静音轨、合并、成片校验、飞书回写
```

正式命令仍只有：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/script-run-manager-sync/scripts/openclaw_original_batch_sync.py check --limit 20
python3 /Users/likeu3/.openclaw/workspace/skills/script-run-manager-sync/scripts/openclaw_original_batch_sync.py sync --limit 20
```

### 只保留调试职责的脚本

以下两个脚本保留为本地调试工具，不承担生产职责：

- `scripts/run_real_flow.py`：已删除特定商品硬编码纠偏与 41 秒硬编码；BGM、字幕文本与字幕时间窗都必须显式传入；
- `skills/script-run-manager-sync/scripts/run_remake_segmented_preview.py`：只做本地只读预览，不写飞书、不生成成片。

旧独立库 `/Users/likeu3/.openclaw/shared/data/remake_video_execution.sqlite3` **不会**自动迁移。若其中已有远端 task ID，必须先人工审计，禁止当作新 job 重复提交。

## 复刻稿 → Plan C 适配层

`remake_video_execution/plan_c_handoff.py` 是复刻编译器与共享 Plan C 媒体执行器之间唯一的接缝：

- 冻结人工复刻稿，确定性解析时间轴，按 15 秒上限切成 2-3 段（≤30 秒为 A/B，31-45 秒为 A/B/C）；
- `SEG_01/02/03` 统一映射为 Plan C 的 `A/B/C`；
- `incoming_boundary=CONTINUOUS` → `boundary_mode=CONTINUOUS`（上一段计划尾帧 + 实际尾帧续接）；
- `incoming_boundary=CUT` → `boundary_mode=DISCONTINUOUS_CUT`（生成独立进入帧）；
- 生成与原创兼容的关键帧包（K0、`K{n}_PLANNED`、`S{X}_ENTRY`）；
- 声音模式继承 `PRESERVE_SOURCE_COPY / NO_VOICEOVER / UNSPECIFIED`，`rewrite_allowed=false`；
- 观众可见文字从视频提示词中剔除，改由后期字幕合同渲染。

它**不**调用原创的 `validate_master_contract()` / `compile_longform_plan()`：复刻稿不需要卖点、capture unit 或唯一 Hook 创作规则。

### 字幕后处理（由共享执行器消费）

适配层输出 `plan.postprocess_contract.subtitles`（`mode=PRESERVE_SOURCE_TIMELINE` + 逐条 cue）。
共享执行器 `core/longform/subtitles.py` 在合并后、混音/静音轨前把冻结原稿的可见文字按
**源时间轴**烧录进成片（ASS + libass，避免 drawtext 转义破坏泰文/中文）：

- cue 文本逐字保留，不翻译、不改写、不重排；
- 泰语目标解析泰文字体（Thonburi / Arial Unicode），缺字体直接报错而不是画出方框；
- 原创计划没有该合同，因此不烧字幕，成片与改造前一致；
- 产物 `merged_captioned.mp4` + `subtitles.json` 清单，可幂等复用。

离线贯通测试可用 Mock H3 验证请求编译、状态登记和幂等重跑，不产生费用：

```bash
PYTHONPATH=/Users/likeu3/.openclaw/workspace/packages/remake_video_execution \
python3 packages/remake_video_execution/scripts/run_mock_flow.py \
  --plan /absolute/path/to/plan.json \
  --start-frame /absolute/path/to/local-test-frame.png \
  --output-dir /absolute/path/to/test-run
```

## 统一商品参考图入口

新计划默认 `--reference-source auto`，复用 OPV 的
`FeishuOperationProductPackSource.list_candidates(product_id)` 只读发现短视频运营任务表图片。
每个运营记录是一组；按图片内容集合去重，保留图组内图片顺序。不会调用 OPV
`resolve_snapshot`，不会导入、建表或更新 OPV/AMC 数据库，也不会回写飞书。
附件仅下载到本地缓存；不可读取、空下载、组不完整或缓存内容改变均报错。

选择顺序：源绑定的 `reference_selection.group_id`（或原始输入 `参考图组ID` /
`structured_source.reference_group_id`）优先，其次 `--reference-group-id`；默认对内容去重，并按飞书系统 `last_modified_time` 选最新组。
图组ID格式为 `operation:<运营表record_id>`；AMC显式组为 `amc:<reference_image_pack_id>`。
通过 `automatic_fields=true` 读取系统创建/修改时间；不同内容存在多组时，必须全部具有可靠修改时间，且最新时点只对应一种内容。缺时间或最新时点不同内容并列返回 `REFERENCE_GROUP_AMBIGUOUS`；不使用扫描顺序、record_id、最近执行时间或缓存mtime排序。系统修改时间是整行时间，可能因状态更新变化，不代表图片字段独立修改时间。选中时间、字段、策略与候选数量会冻结到 `selection_basis`。
只有运营查询成功、确实没有候选且没有指定运营组时，auto才回退AMC最新active包。
查询/下载失败、指定组失效、多组歧义不会触发回退。
`--reference-source operation` 禁止AMC回退；`--reference-source amc` 或原有
`--reference-image-pack-id` 明确使用AMC，但不能覆盖源任务已绑定的组。

“最新”是选定运营组当前附件内容，以 `content_version_hash` 标识，不虚构数字版本。
AMC仍保留其包ID和数字版本。新计划冻结provider、group_id、源记录、内容集合指纹、
有序图片SHA256和全部图片的本地副本。普通运行恢复冻结清单，不再查询当前组；改图只作用于
重新规划的新revision。历史AMC计划与原有单图本地清单仍可恢复，不自动换图；历史单图没有
保存hash时只能检查文件存在，无法追溯证明其原始字节一致性。

规划示例（只生成本地计划，可能只读查询飞书并下载图片）：

```bash
python3 -m remake_video_execution.cli plan --input /absolute/input.json \
  --output-dir /absolute/new-plan --product-id 1734199053312362490 \
  --reference-group-id operation:SOURCE_RECORD_ID
```

真实执行入口 `scripts/run_real_flow.py` 接受同样的来源与选组参数；依然需要独立明确的视频生成授权。
只执行本包`check`不会查询参考图服务。

2026-09-09只读验证：该产品运营表有13条含图记录，按内容去重为3组。获取系统时间后，默认选中
`operation:recvtgh7WvfAhS` 的2图组：创建2026-08-25 07:54:12、修改2026-08-25 18:33:45（北京时间）。
此前AMC查到的V1三图包属于不同来源。当前记录内容不能用于重建历史附件修改过程。
实测报告：`/Users/likeu3/.openclaw/workspace/runtime/remake-reference-audit-20260909/latest_selection_report.json`。
