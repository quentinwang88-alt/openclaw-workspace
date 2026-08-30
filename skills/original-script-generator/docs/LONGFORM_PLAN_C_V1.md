# 20–45 秒原创视频可变分段 Plan C（隔离旁路 V3）

该旁路不会被现有 15 秒原创任务导入，也不读写现有飞书生产表。第一阶段只使用本地 JSON、独立 SQLite 和独立素材目录。

## 数据与入口

- CLI：`scripts/run_longform_original.py`
- 状态库：`~/.openclaw/shared/data/longform_original_video.sqlite3`
- 素材目录：`~/.openclaw/shared/data/longform_original_video/<job_id>/`
- H3 网关：`skills/jimeng-video-generator/platforms/minimax-h3/local-job-runner.js`

主合同允许 20–45 秒、一个语义主线、一个开场钩子、1–4 个有角色分工的有效卖点和最多两个场景块。20–30 秒自动切为两段，31–45 秒自动切为三段；每段最长 15 秒且使用整数时长。20 秒为 `10+10`，21–24 秒自动均衡。V6 每段通常保留三个真正可执行的 `OPENING / CORE / PAYOFF` 观看任务，只有确有新证明或新使用关系时才在主合同保留第4个审计单元；H3 不再承担“四个同权镜头必须全部出现”的任务。使用 `plan --use-model` 时复用当前原创模型线路；不带该参数时，可对人工/测试合同做纯确定性编译。

`plan --source-script-id` 会只读中央运营确认卖点库，生成 `longform_argument_bundle`。20–24 秒建议自然使用 1–2 个价值，25–30 秒建议 2–3 个，40–45 秒建议 3–4 个；它们分别承担主结果、商品原因、使用价值或风格回报，禁止机械列举。该容量是软目标，来源不足时不虚构、不阻断。

场景默认仍为单场景。显式 `--scene-mode auto` 会先从已批准卖点和 `product-market-context` 编译 `scene_progression_contract`：存在明确使用情境时优先两个场景，没有时保留单场景；`--scene-mode multi` 表达双场景偏好但仍允许在不自然时退回。场景退回只进入 `visual_progression_report`，不增加模型重试。`LONGFORM_MULTISCENE_V1_ENABLED=1` 只改变 CLI 未指定时的默认值，不影响 15 秒流程。

## 执行顺序

1. `plan`：生成 A/B 或 A/B/C 合同，并把当前可用的商品、人物及批准首帧参考复制到 job 的 `frozen_references/`，记录 SHA256。历史作业只有合成首帧时诚实标记其来源，不把它当商品原图。
2. `voiceover`：一次生成完整 20–45 秒连续思想及 A/B/C 语义段；后续段不得重新开钩子。
3. 根据边界合同准备帧。真实连续动作才生成 K1/K2_PLANNED；跨场景为下一段生成 `SCENE_ENTRY`，同场景切到新商品观察关系生成 `SETUP_ENTRY`，均不要求中途人工确认。
4. `run-to-final --allow-external-tts` 在任何新的付费 H3 提交前先逐段 Edge 实测。目标覆盖本段90%-96%，86%-100%可接受；只对明显过短/过长的段定向修订一次，保留更优版本后继续。
5. 执行器按 `segments[]` 顺序提交。只有 `CONTINUOUS` 边界才从上段最后0.8秒自动选实际尾帧；`DISCONTINUOUS_CUT` 直接硬切到已登记的片段进入帧。
6. `merge` 对2至3段统一规格、去除源音轨并直接切镜。
7. 最终混音直接复用预检音频，不重复调用 TTS，也不用负速率拖长；默认叠加本地已批准的低存在感 BGM，缺失只软降级。`LONGFORM_BGM_PATH` 可覆盖默认 CC0 素材。

## 固化后的统一续跑

关键帧由现有图片能力生成后，可只运行一次：

```bash
python3 scripts/run_longform_original.py run-to-final \
  --job-id <JOB_ID> \
  --start-frame /absolute/path/K0.png \
  --end-frame /absolute/path/K1_PLANNED.png \
  --allow-real-submit \
  --allow-external-tts \
  --summary-only
```

三段任务在上述参数外增加：

```bash
  --planned-frame K2=/absolute/path/K2_PLANNED.png
```

跨场景任务不需要对应的 `Kx_PLANNED`。可以提供已经生成的进入帧：

```bash
  --scene-entry B=/absolute/path/scene_b.png
```

也可在登记阶段自动生成（以 K0 为同人物、同商品、同穿搭的合成参考）：

```bash
  --auto-generate-scene-entry
```

统一入口按 SQLite 状态从最近断点继续，绝不重复提交已有 H3 `taskId`。如果 K0/K1 已登记，可以省略两个图片参数。默认从 A 的末尾候选中按清晰度、曝光和低运动量自动选取 K1_ACTUAL；需要人工确认时增加 `--manual-bridge`，流程会停在 `WAITING_BRIDGE_REVIEW`。

只检查环境，不提交、不调用 TTS：

```bash
python3 scripts/run_longform_original.py preflight --allow-real-submit
```

缺少付费提交所需凭据时，预检会输出问题清单并以退出码 `2` 结束，便于 OpenClaw 或其他自动化在提交前直接停止。

凭据只从当前进程环境或 macOS `launchctl` 环境读取：

```bash
launchctl setenv METASO_MINIMAX_API_KEY '<key>'
```

不得从历史会话、飞书、JSON、代码或日志恢复 Key。

单独补跑最终口播和混音：

```bash
python3 scripts/run_longform_original.py finalize \
  --job-id <JOB_ID> --allow-external-tts --summary-only
```

每次统一续跑都会更新：

- 数据库 `execution_report_json`
- 素材目录 `<job_id>/execution_report.json`
- 最终状态 `FINAL_READY`
- 最终路径 `final_video_path`
- 最终审阅包 `<job_id>/text_review/`

## V6.1 商品权威、动作与音频复用

- `frozen_references/manifest.json` 会记录 `reference_mode`。存在商品原图时，K0 使用商品原图控制商品结构、人物图控制身份，不再让历史合成首帧覆盖商品；缺少商品原图时自动进入 `COMPOSITE_FALLBACK`，只作构图参考并继续批量，不停下来要求人工确认。
- `visible_closure_contract` 同时进入关键帧、H3 和中央口播。已核实的闭合件使用对应词汇，未知闭合方式只使用中性“合上/敞开/调整前襟”语义，不建立商品专属黑名单。
- A/B/C 的三个执行单元使用“商品部位×动作关系×景别”做全片软去重；只有存在合适候选时才换选，候选不足仅记录 `CROSS_SEGMENT_ACTION_SIGNATURE_REUSED`，不重试、不阻断。
- Edge TTS 预检通过后的音频是冻结生产资产。最终混音在文本哈希、音色和文件一致时直接复用同一路径及预检语速，不做第二次语速计算。
- 20–22 秒任务会产生 10–11 秒片段；口播目标会为片段开口延迟预留尾部空间。若冻结音频已经接近片段上限，最终混音只缩短该段开口延迟，不重新加速口播。

## 安全边界

- H3 真提交必须显式携带 `--allow-real-submit`。
- 外部 TTS 必须显式携带 `--allow-external-tts`。
- 创建请求网络结果不确定时记录 `SUBMIT_UNCERTAIN`，不自动重试，避免重复计费。
- 只有 `CONTINUOUS` 边界使用已登记的计划尾帧，下一片段使用上一片段成片抽取的实际尾帧；跨场景 `SCENE_ENTRY` 和同场景新机位 `SETUP_ENTRY` 都直接使用独立片段进入帧。
- V1 不做转场、自动调色、字幕和第二尾帧，不为增加镜头机械扩充动作。
- 当前不注册 OpenClaw 自然语言入口、不接飞书；文本和视频小样通过后再建设运营入口。
- 统一续跑只编排现有隔离状态；默认不生成图片，只有显式 `--auto-generate-scene-entry` 时才为需要独立进入的片段生成 `SCENE_ENTRY/SETUP_ENTRY`。它永远不隐式进入 15 秒主流程。

## V2 投喂与提交纪律

- 主合同仍保留完整人物、穿搭、场景、语义和拍摄单元，供复现与人工审查；H3 只消费紧凑执行投影，不消费内部解释、评分、authority 或完整审计 JSON。
- K0 为 `OPENING`。连续边界的 K1/K2_PLANNED 为 `BRIDGE`；非连续边界的片段进入帧以 K0 为主要人物与构图血缘，并在可用时附加冻结商品原图和人物参考，只切换本段场景或手机观察关系。
- 所有片段的紧凑提示词顶部统一声明“延续同一商品，不重新设计、增加或删除可见结构”。这是一条通用连续性指令，不扩展成按扣、拉链等类目规则库。
- A 段最后约 0.5 秒尽量让商品自然清楚、不过度遮挡；这是软偏好。自动桥接选择不做语义识别，不因商品可读性未知而阻断、人工确认或自动重跑。
- 冻结穿搭的颜色和单品高于视觉显著性建议；需要商品分离时只调整背景、人物站位和现场光线。
- `h3-prepare/h3-submit` 禁止通过命令行临时覆盖关键帧。所有片段只读取数据库登记的开始帧和计划结束帧。
- 提交幂等指纹由图片内容 SHA256、提示词、模式、时长、比例和分辨率共同构成。创建结果不确定时仍保持 `SUBMIT_UNCERTAIN`，不自动重试。
- H3 离线回归统一运行 `npm run test:h3`，不使用依赖 Chrome 的旧包级 `npm test` 作为本旁路验收。
