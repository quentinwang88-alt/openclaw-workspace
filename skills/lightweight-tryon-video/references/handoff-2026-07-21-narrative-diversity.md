# 口播增强型轻视频后续测试与优化交接文档

> 2026-07-22 收口说明：本商品的单商品测试已经结束，不再执行本文第 6、7 节中的继续补拍或 8 条重剪建议。最终状态与后续规则以 `references/closeout-2026-07-22-narrative-diversity.md` 为准。

更新日期：2026-07-21
项目目录：`/Users/likeu3/.openclaw/workspace/skills/lightweight-tryon-video`
主数据库：`/Users/likeu3/.openclaw/workspace/skills/lightweight-tryon-video/var/light_tryon.sqlite3`

## 1. 本轮目标与结论

本轮围绕女装口播增强型轻视频完成了第一轮真实全流程测试，并针对“口播够长但画面重复度过高”的问题实现了批次级优化。

目前系统同时保留两条互不替代的生产线：

1. `video_jobs`：8–10 秒基础轻视频，先生成视频，再决定是否配口播或进入其它后处理。
2. `narrative_variants`：18–24 秒口播增强型视频，复用现有钩子库、口播文案、轻视频素材和智能混剪打标能力。

口播增强线当前遵循：

- 不修改现有口播系统的写作风格和正式文案。
- 钩子仍来自现有 ACTIVE 钩子库。
- 允许重要钩子、卖点和策略在 20–50 条测试中重复，不设僵硬次数上限。
- 18 秒以上只生成一条连续 TTS，不逐句拼接，避免中途断音。
- 画面按真实 TTS 时间轴匹配钩子和卖点证据镜头。
- 有口播的最终视频默认不配 BGM，保证泰语口播清晰。
- 使用原始脚本表商品参考图做视觉对照，不要求智能混剪商品锚点。
- 本项目只与短视频自动运行管理表同步任务和结果，不自行打开即梦页面、冒充设备或人员认领任务。

## 2. 当前测试产品

| 项目 | 当前值 |
|---|---|
| 外部商品编码 | `1736444730937804794` |
| 内部商品 ID | `OSG_recvoW7oTWkQtl` |
| 已建立增强型变体 | 8 条 |
| 变体状态 | 8 条均为 `final_qc` |
| 口播状态 | 8 条均为 `completed` |
| 当前成片目录 | `var/narrative_delivery/final` |
| 音频策略 | 连续口播、无 BGM |

8 条当前成片为：

```text
NAR_OSG_recvoW7oTWkQtl_001_031527bc.mp4
NAR_OSG_recvoW7oTWkQtl_002_41ab3778.mp4
NAR_OSG_recvoW7oTWkQtl_003_4f314f1e.mp4
NAR_OSG_recvoW7oTWkQtl_004_108db26e.mp4
NAR_OSG_recvoW7oTWkQtl_005_626c6890.mp4
NAR_OSG_recvoW7oTWkQtl_006_fa1a339a.mp4
NAR_OSG_recvoW7oTWkQtl_007_d1f2eaff.mp4
NAR_OSG_recvoW7oTWkQtl_008_1441a916.mp4
```

这些文件属于优化前的第一轮成片，可作为重复度对照基线；尚未用新增的差异化素材重新剪辑。

## 3. 第一轮测试暴露的问题

不是单纯为了测试而临时硬凑。旧编排逻辑在素材不足时，正常批量生产也可能出现同样的问题：

- 多条视频大量复用同一首镜。
- 同一组素材只改变截取时长，整体观感仍然相同。
- 多条视频使用完全一致的镜头排列。
- 两个不同的补镜头任务曾回传同一个视频文件。
- 系统过去只检查单条视频是否有可用镜头，没有在生成 8 条前评估整批素材容量。

当前 8 条旧成片的批次多样性质检结果：

| 指标 | 结果 |
|---|---:|
| 平均共享画面时长比例 | 83.29% |
| 最大共享画面时长比例 | 99.86% |
| 同一首镜最大使用次数 | 7 |
| 完全相同的镜头序列 | 3 组 |
| 多样性得分 | 25.45 / 100 |

当前素材容量评估：

- 已就绪的独立素材组：10 个。
- 由于不同镜头角色覆盖不均，稳定支撑约 6 条差异化长视频。
- 若要稳定生成 8 条，仍缺 3 个不同镜头。
- 缺口角色：`scenario_pose`、`detail_fabric`、`detail_sleeve` 各 1 个。

## 4. 已完成的优化开发

### 4.1 素材去重与画面指纹

文件：`scripts/light_tryon/asset_ingestion.py`

- 原有 SHA-256 完全重复检查继续保留。
- 新增 4 帧视频 dHash 画面指纹。
- 可识别文件编码不同、但画面高度相似的素材。
- 重复组信息写入资产 `qc_result`，编排时按重复组而不是单纯按文件 ID 计算。
- 当前产品已处理 19 个素材文件，19 个均完成画面指纹；另有一组完全重复回传已由文件哈希识别。

### 4.2 批次素材容量评估

文件：`scripts/light_tryon/diversity.py`

- 根据目标视频数计算各镜头角色的推荐配额。
- 输出已有角色数量、缺口、稳定容量、可扩展容量和风险等级。
- 不再只看素材总数，避免“文件很多，但关键镜头只有一个”的虚假充足。

### 4.3 差异化补镜头动作

文件：`scripts/light_tryon/supplement_shots.py`

- 补镜头提示词版本升级为 `supplement-shot-v4-diversity-variants`。
- 每个角色有多个明确的动作版本，例如左右转身、衣摆整理、袖口触碰、手臂伸展、侧光面料展示、查看手机等。
- Prompt 会写明本次动作，并明确禁止复刻已有动作版本。
- 旧任务没有 `action_variant` 时，会被视为已经使用基础动作，新任务从第二种动作开始选，避免表面上新建任务、实际上动作没变。
- 规划幂等：重复运行不会重复创建同一个缺口任务。

### 4.4 重复回传检测与重试

文件：`scripts/light_tryon/narrative_run_manager.py`

- 回流视频如果与另一个补镜头任务文件完全相同，会标记为 `duplicate_return`。
- 不把重复视频当作新的可用镜头。
- 自动规划另一个动作版本作为重试任务。
- `superseded`、`duplicate_return`、`received`、`failed` 状态不会再次被误同步到运行管理表。

### 4.5 跨视频编排轮换

文件：`scripts/light_tryon/assembly_planner.py`

- 单条视频内部不重复使用同一素材或同一重复组。
- 同一产品的多条视频会读取整批历史使用次数。
- 证据匹配仍是第一优先级；在证据合格的候选中，优先选择全批使用较少的素材。
- 对重复首镜、重复镜头序列前缀和全批高频素材增加惩罚。
- 新版粗剪计划：`narrative-roughcut-v2-diversity`。
- 新版口播剪辑计划：`narrative-voiceover-cut-v3-diversity`。

### 4.6 批次多样性质检

文件：`scripts/light_tryon/diversity.py`

质检检查：

- 任意两条视频共享画面时长比例。
- 同一个首镜在批次中的复用次数。
- 是否存在完全相同的素材排列顺序。
- 综合多样性得分与警告。

当不足 2 条已规划视频时返回 `not_evaluated`，不会错误显示 100 分通过。

### 4.7 命令入口

文件：`scripts/light_tryon/cli.py`

新增：

```bash
python3 scripts/run_pipeline.py --db var/light_tryon.sqlite3 asset fingerprint \
  --product-id OSG_recvoW7oTWkQtl

python3 scripts/run_pipeline.py --db var/light_tryon.sqlite3 narrative capacity \
  --product-id OSG_recvoW7oTWkQtl --target-count 8

python3 scripts/run_pipeline.py --db var/light_tryon.sqlite3 narrative plan-diversity-pool \
  --product-id OSG_recvoW7oTWkQtl --target-count 8

python3 scripts/run_pipeline.py --db var/light_tryon.sqlite3 narrative diversity-qc \
  --product-id OSG_recvoW7oTWkQtl
```

`narrative sync-supplements` 默认会先做容量检查和缺口规划；传 `--no-auto-diversity` 才关闭。

## 5. 当前尚未提交的 3 个补素材任务

以下任务已经在本地数据库建立为 `planned`，但没有同步到短视频自动运行管理表，也没有产生外部生成费用：

| 任务 ID | 镜头角色 | 动作版本 | 目标效果 |
|---|---|---|---|
| `SUP_73af6a49da693996c3` | `detail_fabric` | `side_light_turn` | 侧光下轻转上半身，突出面料纹理 |
| `SUP_a0855fd22eaf94a172` | `detail_sleeve` | `sleeve_extend` | 自然伸展手臂，展示袖型和活动感 |
| `SUP_ba7c3f094831699d64` | `scenario_pose` | `phone_check` | 室内场景中短暂查看手机，增加生活感 |

曾经创建、随后在同步前废弃的基础动作任务：

```text
SUP_eecfcaac6e0dc45c61  detail_fabric  single_sweep  superseded
SUP_08f1c6c5114838e426  detail_sleeve  cuff_touch   superseded
SUP_0f9aa68cc412cd632c  scenario_pose  ready_to_go  superseded
```

这 3 条废弃任务不得提交。

## 6. 下一会话推荐测试顺序

### 第一步：只读确认状态

先运行容量、任务状态和 dry-run，不修改飞书、不正式提单：

```bash
cd /Users/likeu3/.openclaw/workspace/skills/lightweight-tryon-video

python3 scripts/run_pipeline.py --db var/light_tryon.sqlite3 narrative capacity \
  --product-id OSG_recvoW7oTWkQtl --target-count 8

python3 scripts/run_pipeline.py --db var/light_tryon.sqlite3 narrative sync-supplements \
  --product-id OSG_recvoW7oTWkQtl --target-count 8 \
  --channel 即梦 --model 'Seedance 2.0 VIP' --dry-run
```

验收点：dry-run 应只出现上述 3 个 `planned` 任务，不应包含 `superseded` 或已经 `received` 的任务。

### 第二步：经用户确认后提交 3 个补镜头

真实同步会创建外部生成任务并可能产生费用，必须取得用户明确确认后再去掉 `--dry-run`：

```bash
python3 scripts/run_pipeline.py --db var/light_tryon.sqlite3 narrative sync-supplements \
  --product-id OSG_recvoW7oTWkQtl --target-count 8 \
  --channel 即梦 --model 'Seedance 2.0 VIP'
```

只同步任务到运行管理表。不要自行打开即梦页面或主动认领生成任务。

### 第三步：从运行管理表回流已完成视频

不需要等待所有任务全部完成；可以周期性回流已经上传的结果：

```bash
python3 scripts/run_pipeline.py --db var/light_tryon.sqlite3 narrative pull-supplements \
  --product-id OSG_recvoW7oTWkQtl
```

如果返回的是完全重复文件，系统会标记 `duplicate_return` 并规划不同动作重试。不要把重复素材人工改成 `received`。

### 第四步：去重与真实打标

回流后先计算画面指纹：

```bash
python3 scripts/run_pipeline.py --db var/light_tryon.sqlite3 asset fingerprint \
  --product-id OSG_recvoW7oTWkQtl
```

再复用智能混剪的视觉打标，优先使用原始商品参考图模式：

```bash
python3 scripts/run_pipeline.py --db var/light_tryon.sqlite3 asset tag \
  --product-id OSG_recvoW7oTWkQtl --anchor-mode source_reference --limit 10
```

`expected_tags` 只能表示生成意图，不能直接作为口播证据。只有真实视觉打标成功的 `observed_tags` 可以进入正式重新编排。`asset tag-supplements` 仅作为视觉服务不可用时、且结构检查已通过的契约回退方案。

### 第五步：再次检查容量

```bash
python3 scripts/run_pipeline.py --db var/light_tryon.sqlite3 narrative capacity \
  --product-id OSG_recvoW7oTWkQtl --target-count 8
```

推荐进入重剪前达到：

- `stable_capacity >= 8`
- `missing_shot_count = 0`
- 没有未处理的完全重复或近似重复回传

### 第六步：重新编排并混合连续口播

对 8 个 `NAR_...` 变体依次重新运行：

```bash
python3 scripts/run_pipeline.py --db var/light_tryon.sqlite3 narrative assemble \
  --variant-id <NAR_ID>

python3 scripts/run_pipeline.py --db var/light_tryon.sqlite3 narrative mix \
  --variant-id <NAR_ID>
```

因为已有 TTS 时间轴，`assemble` 会进入新版口播画面重排，不会重新写口播。`mix` 继续使用连续口播且不添加 BGM。

### 第七步：批次质检

```bash
python3 scripts/run_pipeline.py --db var/light_tryon.sqlite3 narrative diversity-qc \
  --product-id OSG_recvoW7oTWkQtl
```

本轮建议验收标准：

- 完全相同镜头序列：0。
- 同一首镜使用次数：不超过 2。
- 任意两条视频共享画面时长：尽量不超过 60%。
- 多样性得分：建议至少 70。
- 钩子与主卖点仍命中对应证据镜头。
- 口播全程连续、无明显停顿、无 BGM 干扰。

如果卖点证据准确，但共享画面仍高于 60%，不要通过随机打乱掩盖问题；应再补 2–3 个真实差异镜头。

## 7. 后续优化优先级

### P0：完成当前 3 个镜头的真实回传与 8 条重剪

这是验证本次代码是否真正解决问题的必要步骤。没有新素材时，只修改编排算法无法从根本上降低共享画面比例。

### P1：近似重复也自动触发重试

当前完全相同文件会在回流阶段自动重试；近似重复画面会在运行 `asset fingerprint` 后被合并并从编排中降权，但尚未自动创建另一个动作重试。建议下一步把近似重复判定接到重试规划器。

### P1：建立“批次合格后再交付”门禁

目前 `diversity-qc` 会评分和告警，但还没有强制阻止低分批次进入交付目录。建议配置可调门槛：首镜复用、最大共享比例、重复序列数和最低得分。

### P2：增加第二种主体展示素材

若当前 3 个补镜头返回后仍不够丰富，优先补：

1. `main_wear_upper`：整理衣摆或向前半步，不重复基础正面站姿。
2. `fit_turn`：与现有方向相反的 20–30 度转身。
3. `scenario_pose`：重心切换或准备出门动作。

这三类镜头比继续增加纯细节特写更能改善整条视频的体感差异。

### P2：用第二个商品做泛化测试

当前测试集中在一个外套类商品。当前产品达到验收标准后，再选一个非外套上装或多色商品，验证：

- 不同类目镜头角色配额是否合理。
- 内搭、闭合方式和袖型证据是否正确。
- 多色商品在有可靠参考图时是否能使用颜色展示镜头。
- 容量评估是否会错误要求无关镜头。

### P3：稳定后再增加自动巡检

在单商品与第二商品测试通过前，不建议立即开启无人值守批量提单。稳定后可以增加 1–2 小时一次的低频任务：

```text
仅同步 planned 补镜头
→ 仅回流运行管理表已上传结果
→ 指纹与真实打标
→ 容量检查
→ 达标后进入待重剪状态
```

自动巡检只能同步与回流数据，不应自行操作生成网站、修改执行归属或冒充认领人。

## 8. 测试与代码状态

2026-07-21 已执行：

```bash
python3 -m compileall -q scripts/light_tryon
PYTHONPATH=scripts python3 -m unittest discover -s tests
```

结果：86 项测试全部通过。

本目录存在用户已有修改和大量尚未纳入 Git 的项目文件。后续会话必须：

- 不使用 `git reset --hard`、`git clean` 或覆盖式检出。
- 不删除旧成片和数据库历史。
- 修改前只查看与当前任务有关的文件差异。
- 真实提交生成任务前先 dry-run，并取得用户确认。

本轮核心文件：

```text
scripts/light_tryon/diversity.py
scripts/light_tryon/asset_ingestion.py
scripts/light_tryon/supplement_shots.py
scripts/light_tryon/narrative_run_manager.py
scripts/light_tryon/assembly_planner.py
scripts/light_tryon/cli.py
tests/test_narrative_enhancement.py
```

长期设计说明：

- `references/voiceover-enhanced-video-v1.md`
- `references/integrations.md`
- `references/likeu-brand-identity.md`

## 9. 可直接复制到新会话的接手提示

```text
请接手轻量试穿视频的后续流程测试与优化。

项目目录：
/Users/likeu3/.openclaw/workspace/skills/lightweight-tryon-video

请先完整阅读：
references/handoff-2026-07-21-narrative-diversity.md
references/voiceover-enhanced-video-v1.md
references/integrations.md

测试产品：外部商品编码 1736444730937804794，内部 product_id 为 OSG_recvoW7oTWkQtl。

先只读检查数据库状态、素材容量和 3 个 planned 补镜头任务，然后执行 sync-supplements --dry-run。不要未经我确认正式提交 Seedance 任务，不要自行打开即梦页面或冒充设备/人员认领任务。

当前目标是：让 3 个不同动作的补镜头回传并真实打标后，重新编排已有 8 条连续泰语口播视频；口播视频不配 BGM。最终要求重复镜头序列为 0、同一首镜不超过 2 次、共享画面显著下降，同时保持钩子/卖点与证据画面同步。
```
