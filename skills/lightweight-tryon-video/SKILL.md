---
name: lightweight-tryon-video
description: >-
  Build and operate the AI lightweight fashion try-on video template line: manage fixed
  scene/action/styling/subtitle/persona templates, import apparel SKUs, deterministically
  create product visual plans and gated 8–10 second video jobs, assemble strict product-faithful prompts, export
  Jimeng-compatible task records, run command-based generation workers, burn in localized
  subtitles, perform structural and visual QC, prepare manual review, and plan 18–24 second
  voiceover-enhanced variants without replacing the existing hook or voiceover writing flow. Use for requests
  mentioning AI 轻量试穿视频、固定女生/房间/机位、模板化换装、自然流铺量、试穿任务单、
  8–10 秒女装视频、Scene A/B/C、ACT/STYLE/SUB 模板，or maintaining this production line
  in OpenClaw/Codex.
---

# AI 轻量试穿视频模板系统

使用独立模板线生产稳定的 8–10 秒女装试穿视频；增强型入口只负责内容组合、Beat、补镜头和素材打标编排，不替代现有钩子库或口播写作流程。

## 运行边界

- 固定 V1 主人设、房间体系、9:16 机位、手机完整遮脸和小幅动作。
- 将商品真实性置于自由创意之前；不清楚的商品结构不得补写或生成。
- 让视频模型生成无文字干净原片；使用结构化 `subtitle_plan` 后期烧录字幕。
- 将本地 SQLite 作为本 Skill 的任务真相源；用唯一 `visual_plan_id`、`job_id` 和 `attempt_no` 保留幂等与重试历史。
- 原始脚本入口必须先建立“商品 × 场景 × 搭配 × 人设”的产品视觉方案；产品穿搭图确认前禁止创建或认领视频任务。
- 只在用户明确要求真实生成、目标表配置完成且主电脑生产依赖正常时，调用现有 `jimeng-video-generator`。默认仅编排、预览或导出任务包。
- 不复制现有浏览器自动化。将任务导出给 `jimeng-video-generator`，让它负责认领、提交、资产匹配、下载和回写。

## 核心入口

使用：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py <command>
```

默认数据库在 `skills/lightweight-tryon-video/var/light_tryon.sqlite3`。可在所有命令前传 `--db <path>`，或设置 `LIGHT_TRYON_DB`。

## 标准工作流

### 1. 初始化模板底座

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py init
```

这会建立核心表（含 `product_visual_plans`、`visual_plan_attempts`），并幂等写入 2 个启用环境（现代简约卧室、明亮现代咖啡店）、4 个停用的历史兼容场景、4 个镜头方案、9 个动作、6 个搭配、多语言字幕、1 个主人设和 8/10 秒时长模板。运营只维护“在哪里拍”的环境；底层镜头定义保持系统内置，1/5 条任务的镜头排列由飞书“镜头方案库”维护。

### 2. 录入商品

优先从 JSON/JSONL 导入：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py product add \
  --file skills/lightweight-tryon-video/assets/product.example.json
```

也可直接传参数。商品至少需要 `product_id` 和 `product_name`。正式任务应提供商品参考图；没有参考图时任务包会标记 `免参考图=true`，必须人工评估跑款风险。

### 3. 产品视觉方案与穿搭图门禁

原始脚本表可多选场景和搭配，系统做笛卡尔积，最多 6 个视觉方案。“自动选择”只解析成 1 个模板，且不能与具体选项共存。每个视觉方案只有一张当前有效的产品穿搭图。

下装颜色不需要在原始脚本表逐项选择。系统会在每个视觉方案中自动抽取一个颜色并写入“实际下装颜色”；同一方案重跑以及生成 1 条或 5 条视频时保持不变。搭配模板的“下装颜色”只作为可选候选池，留空时按下装类型使用系统默认色池。多个“下装版型”标签是同一件下装的累计要求，不会随机组合或拆成多个方案。

查询方案、通过外部 JSON worker 生成穿搭图、人工确认并创建视频任务：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py visual-plan list --source-record-id <record_id>
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py visual-plan generate-outfit --command '<图片 worker>' --limit 1
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py visual-plan confirm-outfit --visual-plan-id <ID> --image-path /absolute/outfit.jpg
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py visual-plan create-jobs --visual-plan-id <ID>
```

场景参考图为选填：有图时作为空间布局基准，无图时使用完整场景模板文字生成。室内奶油风场景可维护背景候选池、边缘装饰候选池和主光方向；系统为每个视觉方案稳定解析一种主背景、0–2件边缘装饰和一个45度斜侧主光方向，不会把所有候选堆在同一画面，并保存实际背景、装饰和主光方向快照。上装类商品的穿搭图默认以上半身为主体，只露出下装腰头到大腿中段的一截。搭配模板可选维护 `内搭类型`、`内搭颜色` 和 `内搭补充要求`；外套类优先采用模板值，任一字段留空时由系统自动补齐默认圆领内搭或稳定颜色，并把实际内搭类型、颜色和外套开合规则保存为只读方案快照。图片生成成功后自动标记为已确认并创建视频任务，不再等待人工图片复核；飞书图片用于事后抽检和问题追溯。

### 4. 编排任务并构建 Prompt

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py plan --product-id <商品ID>
```

`plan` 是保留的本地兼容入口。原始脚本生产入口由已确认视觉方案创建任务。外套类每方案生成 5 条时，镜头严格为 `3 条高亮柔光上半身固定 + 1 条头顶至大腿中段固定 + 1 条极慢平稳推近`，不再使用全身镜头；每方案生成 1 条时只用上半身固定镜头。其它上装、T恤、背心、针织和衬衫仍使用 `2 条全身固定 + 2 条高亮柔光上半身固定 + 1 条极慢平稳推近`。5 条任务共用同一视觉方案的一张已确认产品穿搭图，视频 Prompt 不再直接混用商品图、人物图和场景图；每条外套视频在开场 0–1 秒明确锁定上装重点构图，并沿用方案中的内搭和外套开合状态。

检查任务：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py job list --product-id <商品ID> --full
```

### 5. 接入短视频自动运行管理表

复核台选择 `生成渠道`、`生成模型` 和 `视频时长` 后，使用统一运行管理表排队，不再维护第二套轻视频生成表。默认 `生成渠道=不生成`；只有显式选择“自动 / 即梦 / iMini”才入队。运行管理表的 `参考图` 使用原始脚本表“产品图片”，保持附件顺序；iMini 直接使用第一张作为首帧。模型、8/10 秒和 720P 都是硬约束。

基础轻视频入队时固定写 `脚本类型=轻视频脚本`。历史兼容入口手工创建的补充镜头仍写 `脚本类型=轻视频补素材脚本`，但口播重剪不再因普通 Beat 缺口自动创建补素材任务。该字段只表示业务来源，不替代生成平台 `渠道`。

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu ensure-run-manager-schema --dry-run
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu ensure-run-manager-schema
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu sync-run-manager --job-id <JOB_ID>
```

命令会完成配置回读、幂等入队、生成结果转存到 `初始成片`、LikeU 首屏/类目后期，并上传 `最终视频`。普通无口播任务可继续执行 BGM 混音；运行表中勾选了 `是否配口播` 或已有 `口播状态` 的任务会被自动排除在 BGM 阶段之外。口播链路固定为 `口播混音 → LikeU 首屏/类目后期 → 终检 → 回写运行表口播成片与轻视频复核表最终视频`，首屏渲染只重编码画面并复制口播音轨，不叠加 BGM。勾选 `重新提交生成` 时仅在同任务没有运行中实例时重置队列；同步成功后自动取消勾选。

### 6. 导出即梦任务包（兼容入口）

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py export-jimeng \
  --product-id <商品ID> \
  --output /tmp/light-tryon-jobs.jsonl
```

每条记录都含 `【内容ID】<job_id>`、精简且自包含的模型可用提示词、参考图、8/10 秒、9:16、Seedance 2.0 和模板元数据。每条提示词必须独立写全场景布局、人物外观、机位、光线、商品、搭配与动作，不得使用“与主场景相同”“同一套”“同一个女生”“原展示区”等依赖其它任务上下文的指代。完整结构化模板快照只保留在 SQLite 审计载荷中；飞书复核台的“完整Prompt”只展示可直接复制的正文，不展示 JSON、转义符、本地路径或模板快照。按 [integrations.md](references/integrations.md) 将记录写入独立的即梦任务表；不要写进原创脚本表。

### 7. 调用生成适配器（本地兼容入口）

通用命令适配器从 stdin 接收 JSON，并在 stdout 返回：

```json
{"output_video_path":"/absolute/video.mp4","output_cover_path":"/absolute/cover.jpg"}
```

运行：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py generate \
  --command '<安全的外部 worker 命令>' --limit 1
```

单条失败不会阻塞后续任务。第一次失败进入 `retrying`，达到 `--max-attempts` 后进入 `failed`。使用 `job reset --job-id ...` 人工恢复。

### 8. 字幕与品牌后期

生成任务保存原片到 `raw_video_path`。使用 FFmpeg 按字幕计划烧录：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py subtitle-render \
  --job-id <JOB_ID> --font-name 'Arial Unicode MS'
```

选择覆盖目标语言完整字符集的字体。命令默认将烧录后的文件设为 `output_video_path`，并把 QC 重置为 `pending`。

账号视觉身份启用品牌叠加后，推荐使用统一后处理命令。系统从第 0 帧立即叠加居中偏下的品牌 Logo/字标及服装类目标题，约 0.8 秒后淡出，同时输出带同款品牌信息的封面；默认不再烧录后续评价字幕。视频模型仍生成无文字、无 Logo 的干净原片：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py postprocess-render \
  --job-id <JOB_ID> --font-name 'Arial Unicode MS'
```

也可只执行品牌层：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py brand-render --job-id <JOB_ID>
```

品牌配置维护在“账号视觉身份表”：`品牌叠加状态`、`店铺Logo`、`品牌展示名称`、`品牌视觉预设`、`品牌主色`、`默认系列名称`。Logo 建议使用透明 PNG；未上传 Logo 时可使用品牌展示名称生成稳定字标。品牌启用后只使用首帧品牌字标与服装类目标题，不再追加动态评价字幕。

LikeU 的正式字标资产、标准色、首屏比例、安全留白和禁用规则见 [LikeU 品牌视觉与视频应用规范](references/likeu-brand-identity.md)。生产配置应上传透明 PNG，不要使用带底色的预览 JPG。

日常运营可直接在“轻量视频任务复核台”的 `初始成片` 附件字段上传视频。处理器会下载原片，按该任务保存的 `subtitle_plan` 和 `brand_plan` 依次叠加字幕与 LikeU 品牌首屏，再将 MP4 上传到同一条记录的 `最终视频` 附件字段：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu process-review-videos
```

可用 `--job-id <JOB_ID>` 只处理指定任务，或用 `--limit 1` 小批验证。相同初始附件、字幕计划和品牌计划只处理一次；替换初始成片或调整字幕/品牌配置后会自动重新处理。`feishu sync-all` 也会执行这一步。

需要将现有 auto_mixcut BGM 库用于轻量试穿最终成片时，运行：

```bash
python3 skills/lightweight-tryon-video/scripts/apply_review_bgm.py
```

该命令复用 auto_mixcut 的国家优先、标签评分、同批曲目去重、推荐起点、响度标准化和音量策略；默认按 `TH + womens_outerwear + daily_clean + medium + instrumental优先` 选择，并用 BGM 替换原音轨后覆盖复核台 `最终视频`。

不要对口播成片运行该命令。`feishu sync-run-manager` 会按 `是否配口播` / `口播状态` 自动跳过口播任务；需要全批禁用 BGM 时继续使用 `--no-bgm`。

### 9. QC 与人工复核

仅结构初检：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py qc --limit 20
```

结构初检检查视频流、8–10 秒和 9:16。未运行视觉 QC 时必须进入 `manual_review`，不得自动判通过。

视觉 QC 命令从 stdin 接收任务、视频、场景、动作和预期，stdout 返回 [data-model.md](references/data-model.md) 中的评分 JSON：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py qc \
  --vision-command '<视觉 QC worker 命令>' --limit 20
```

视觉结果必须明确返回：镜头运动是否与 `fixed/push_in` 预期一致、上装是否完整可见（上装类任务）、亮度是否合格、是否无过曝、商品颜色是否保持。缺少任一必填证据不能自动通过；明确失败则直接判失败。

导出本地复核页并写人工结论：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py review-export --output /tmp/light-tryon-review.html
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py review-set --job-id <JOB_ID> --decision passed --note '人工确认可发布'
```

### 10. 飞书运营配置与复核台

7 张表的链接集中配置在 `config/feishu_tables.json`，新增的第 7 张是“产品视觉方案表”。也可用环境变量覆盖。首次接入按顺序执行：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu ensure-schema --dry-run
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu ensure-schema
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu init-records
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu pull-templates
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu push-visual-plans
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu pull-visual-plans
```

只更新部分模板表时重复传入 `--role`，例如 `feishu init-records --role scene --role action`，避免覆盖其它运营配置。

日常同步：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu pull-templates
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu push-review
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu pull-review
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu process-review-videos
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu sync-run-manager
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu pull-source
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu sync-all
```

原始脚本汇总表通过 `每方案视频数量` 触发，选项为 `不生成 / 每方案 1 个 / 每方案 5 个`；`轻量视频场景`、`轻量视频搭配` 和选填的 `轻量视频动作` 使用名称选择，`轻量视频镜头方案` 使用单选名称，后端统一解析稳定 ID。镜头和动作留空或选择“自动选择”时由类目自动匹配；测试状态的动作/镜头方案只有被明确选择时才参与。另有 `预计视觉方案数`、`预计视频总数`、`视觉方案ID`。首次接入先执行：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu ensure-source-schema --dry-run
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu ensure-source-schema
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu pull-source --dry-run
```

生成数量为空时完全忽略；`不生成` 将当前方案标记为被替代但不删除历史任务；每方案从 1 改成 5 只补齐 4 条，从 5 改回 1 不删历史。修改场景或搭配会产生新的活动视觉方案，移除的组合标记为 `superseded`。产品穿搭图未确认时任务数必须为 0。不存在或停用的模板、重复/空模板名称、超过 6 个组合都会明确报错。

所有核心同步都支持按业务 ID 或 ISO 8601 修改时间局部重跑。例如：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu pull-templates --role action --business-id ACT_006
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu push-review --job-id <JOB_ID>
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu pull-review --changed-after 2026-07-13T09:00:00+08:00
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu pull-source --record-id <飞书record_id>
```

飞书维护人设、账号品牌视觉、场景环境、镜头方案、动作、搭配、字幕和产品视觉方案复核；SQLite 仍是商品、方案、任务、生成、后处理、QC 的执行真相源。账号品牌 Logo 会在模板同步时缓存到本地，后期精确叠加，不交给视频模型生成。模板同步后会在同一轮自动刷新原始脚本表的场景、搭配、动作和镜头方案名称选项。选填的场景参考图会缓存到本地并进入穿搭图请求；为空时使用场景文字。下装颜色由系统自动解析，不增加原始脚本操作字段；产品视觉方案表中的“实际下装颜色/实际下装版型”仅用于查看结果。新增场景只需在场景环境库增加一条环境记录，不要为全身、半身、推近重复建场景。

复核台生成配置只由运营维护，普通系统回推不会覆盖已选渠道、模型、时长或重跑勾选。`生成渠道=不生成` 永远不入队；运行管理表记录通过 `脚本ID=视频任务ID` 幂等关联，结果先回到 `初始成片`，再进入最终后期。

## 口播增强型视频（18–24 秒）

数据库 V3.1 包含内容策略、增强型变体、公共素材、补充镜头和自动生产批次表。原有 `video_jobs` 仍严格保持 8–10 秒，不改变“先有视频、再配口播”的生产顺序。

系统直接读取当前口播系统数据库中的 `ACTIVE` 钩子和同一商品的共享内容台账，建立策略池并进行“历史降权 + 本批覆盖 + 唯一组合用完后再重复”。不需要运营导出或维护第二份钩子 JSON；重要事实仍可复用，但近期高频钩子、卖点和钩子×卖点组合会自动降权。比较型钩子会先校验多色等事实前提，避免只凭文案模板硬套：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py narrative plan \
  --product-id <商品ID> --count 20 --duration 22 \
  --evidence 主穿搭图 --evidence 上半身试穿
```

8–10 秒基础视频、18–24 秒增强视频和生成后复刻视频共用中央商品内容台账；不会在智能混剪里另建文案规则。外部商品编码是稳定主身份，`OSG_...` 等内部 ID 自动登记为别名；幂等重跑沿用原批次，新 `plan_version` 自动分配下一批次。查看某商品最近使用的格式、批次、钩子和卖点：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py narrative content-memory \
  --product-id <商品ID>
```

完整跨批次规则见 [商品级口播内容记忆](references/product-voiceover-content-memory-v1.md)。

先将已生成的基础轻视频登记并复用智能混剪完成真实打标。`expected_tags` 只是生成意图，不能作为口播证据或剪辑依据。历史任务可批量补登记。增强型轻视频默认使用原始脚本表的商品参考图直连视觉打标，不要求建立智能混剪商品锚点；参考图会与视频抽帧共同交给视觉模型比对，但不会把商品状态伪造成 `confirmed`，也不会认领智能混剪任务。仍可显式切回旧锚点模式：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py asset backfill \
  --product-id <商品ID>
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py asset tag \
  --product-id <商品ID> --anchor-mode source_reference --limit 10
```

需要使用旧锚点门禁时传 `--anchor-mode confirmed_anchor`。任何视觉调用失败、空角色标签或不可用片段都必须重试或进入人工复核，禁止用计划标签兜底。

有足够的 `observed_tags` 后，先按实际可见镜头生成 18–24 秒无声视觉粗剪和客观时间轴：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py narrative assemble \
  --variant-id <NAR_ID>
```

正式口播直接调用当前口播流程；适配器只传钩子 ID、主辅卖点、目标时长和粗剪证据，不添加另一套写作风格，也不允许下游改写：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py narrative voiceover \
  --variant-id <NAR_ID>
```

完成口播 Beat 后，系统只把首镜和一个主卖点 Beat 设为硬匹配；普通 Beat 与结尾使用同产品素材软匹配，不再逐句要求专属证据。若首镜或核心卖点确实缺少证据，只阻塞当前口播变体，不自动补素材。以下命令保留为运营明确决定补拍时的手工兼容入口：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py narrative plan-supplements \
  --variant-id <NAR_ID> --context-file /path/context.json \
  --reference /path/confirmed-outfit.jpg
```

口播确认后用当前 TTS 适配器一次性合成完整口播；18 秒以上视频禁止逐句或分块拼接音频。增强型泰语默认使用自然偏慢的 `-18%` 语速，以减少 22 秒成片尾部空白；运营仍可显式覆盖。系统读取 TTS 服务返回的词级时间边界，把每条口播定位到真实语音区间，并据此重新选择和排列证据镜头。若真实 TTS 超出视频时长，任务回到现有口播流程精简，不通过强制倍速或下游改写硬塞。最后只将连续口播音轨混入重排后的画面；V1 不随机叠加模型 BGM，避免口播被盖住：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py narrative tts \
  --variant-id <NAR_ID> --provider edge --voice-id th-TH-PremwadeeNeural
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py narrative assemble \
  --variant-id <NAR_ID>
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py narrative mix \
  --variant-id <NAR_ID>
```

默认顺序是：`飞书选择数量 → 策略变体 → 基础素材真实打标 → 视觉粗剪 → 现有口播 → 单次连续 TTS → 按真实台词时间重排画面 → 混音 → 终检`。普通 Beat 缺少专属画面不再扩展流程；只有首镜或核心卖点缺少必要证据时，当前变体停止渲染并提示人工选择换卖点、换素材或放弃该变体。

完整数据契约、状态流转和降级规则见 [口播增强型轻视频开发说明](references/voiceover-enhanced-video-v1.md)。

## 模板管理

查询、启停、更新和删除模板：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py template list --kind scene
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py template list --kind shot_plan
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py template status --kind action --template-id ACT_006 --status disabled
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py template upsert --kind subtitle --json-file /path/template.json
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py template delete --kind subtitle --template-id <ID>
```

优先使用 `disabled`，不要删除已经被任务引用的模板；数据库外键会阻止破坏历史任务。

镜头方案在飞书“镜头方案库”维护：`生成1条时镜头` 决定单条任务，`第1条镜头` 至 `第5条镜头` 保留顺序和重复值。动作模板的场景与镜头条件均为可选；填写后必须同时匹配，留空表示不限制。`使用优先级` 数字越大越优先，`测试` 状态只在原始脚本表明确选择该名称时生效。

## 验收与排障

运行全部单测：

```bash
python3 -m unittest discover -s skills/lightweight-tryon-video/tests -v
```

查看生产统计：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py stats
```

遇到问题时依次检查：商品语言是否有字幕模板、任务是否已构建 Prompt、生成状态是否卡在 `generating`、输出文件是否真实存在、字幕字体是否覆盖目标语言、QC 是否缺少视觉结果。

## 参考

- 读取 [data-model.md](references/data-model.md) 了解表结构、状态机、worker 和 QC JSON 契约。
- 读取 [integrations.md](references/integrations.md) 了解飞书字段映射、即梦接入和生产风险。
- 运营新增或调整环境时读取 [environment-maintenance.md](references/environment-maintenance.md)，一条环境只维护一次。
- 默认模板唯一来源是 [default_templates.json](assets/default_templates.json)。修改后重新运行 `init` 幂等更新。
