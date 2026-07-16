# 飞书与即梦集成

## 目录

1. 边界
2. 飞书 Base 设计
3. video_jobs 兼容字段
4. 接入现有即梦 worker
5. 生产检查

## 1. 边界

本 Skill 负责模板、商品、编排、Prompt、字幕计划、生成状态和 QC。现有 `skills/jimeng-video-generator` 负责浏览器登录态、任务认领、真实提交、资产匹配、下载和飞书附件回写。

不要从本 Skill 复制 `feishu-direct-monitor.js`、`trace-state.js` 或 `result-uploader.js`。浏览器自动化是生产机能力，应通过独立配置复用。

## 2. 飞书 Base 设计

当前生产配置使用 6 个原有独立 Base，并新增“镜头方案库”和“产品视觉方案表”：账号视觉身份表、场景环境库、动作模板库、镜头方案库、搭配模板库、字幕模板库、轻量视频任务复核台、产品视觉方案表。跨 Base 字段均保存稳定文本业务 ID；SQLite 内部继续保留真正的外键关系。

不要新增商品主表。现有商品工作台链接尚未配置时，`config/feishu_tables.json` 的 `product_workbench.url` 必须保持空值，程序不得猜表或写入。SQLite 是商品、任务、生成和 QC 的执行真相源；前五张飞书表是运营模板配置主源，复核台是人工结论主源。

表链接放在 `config/feishu_tables.json`，不要写死在同步函数。支持以下覆盖变量：

- `FEISHU_LIGHT_VIDEO_CONFIG`
- `LIGHT_VIDEO_FEISHU_SYNC_ENABLED`
- `LIGHT_VIDEO_FEISHU_REVIEW_ENABLED`
- `FEISHU_PERSONA_URL` / `FEISHU_SCENE_URL` / `FEISHU_ACTION_URL`
- `FEISHU_STYLING_URL` / `FEISHU_SUBTITLE_URL` / `FEISHU_VIDEO_REVIEW_URL`
- `FEISHU_VISUAL_PLAN_URL`
- `FEISHU_PRODUCT_WORKBENCH_URL`
- `FEISHU_ORIGINAL_SCRIPT_URL`
- `FEISHU_RUN_MANAGER_URL`

原始脚本汇总表新增/升级 10 个轻量字段：`每方案视频数量`、多选 `轻量视频场景`、多选 `轻量视频搭配`、`预计视觉方案数`、`预计视频总数`、`视觉方案ID`、`轻量视频状态`、`轻量视频任务ID`、`轻量视频错误信息`、`轻量视频最近触发时间`。原创脚本字段不参与轻量认领。旧 `是否跑轻模型` 或 `轻量视频生成数量` 会原位迁移为 `每方案视频数量`，不新增重复入口。

产品视觉方案表一行对应一个组合。运营可直接在 `产品穿搭图` 附件字段上传并预览图片，再将 `穿搭图状态` 改为“已确认”或“重新生成”；同步层会缓存附件，在“已确认”时执行门禁并补建视频任务。模板同步会同步刷新源表场景/搭配名称下拉；空名称或重名直接失败。

同步命令、初始化顺序、局部重跑示例和定时任务入口见 `SKILL.md` 与 `config/cron.example`。同步日志写入 SQLite 的 `feishu_sync_runs`、`feishu_sync_items`；人工复核幂等事件写入 `manual_review_events`。

轻量视频任务复核台由运营维护 `生成渠道`、`生成模型`、`视频时长` 和 `重新提交生成`。渠道支持“不生成 / 自动 / 即梦 / iMini”，模型支持 `Seedance 2.0 / Seedance 2.0 VIP`，时长只接受 8 或 10。系统字段 `运行表记录ID`、`队列同步状态`、`队列错误信息` 和 `最新追踪ID` 不由运营填写。

## 3. video_jobs 兼容字段

若让现有即梦 worker直接读取任务表，至少包含：

| 飞书字段 | 来源/默认 |
|---|---|
| 任务名 | `job_id` |
| 内容ID | `job_id` |
| 商品ID | `product_id` |
| 状态 | `待处理` |
| 提示词 | `prompt_payload.positive_prompt` |
| 参考图 | 原始脚本表“产品图片”，保持附件顺序，第一张作为主图/首帧 |
| 免参考图 | 没有参考图时为真 |
| 生成次数 | 固定 1 |
| 模型 | 复核台精确选择的 `Seedance 2.0` 或 `Seedance 2.0 VIP` |
| 视频比例 | `9:16` |
| 视频时长 | 8 或 10 |
| 场景ID / 镜头策略 / 动作ID / 搭配ID / 字幕ID / 人设ID | 任务组合 |
| 变体编号 | `variant_no` |
| Prompt版本 | Prompt builder 版本 |
| 执行归属 | 由现有 worker 写入 |
| 已提交次数 | 由现有 worker 写入 |
| 最新追踪ID | 由现有 worker 写入 |
| 结果回传状态 | 由现有 worker 写入 |
| 生成视频 / 生成视频文件名 | 由 uploader 写入 |
| 提交时间 / 完成时间 / 错误信息 | 由现有 worker 写入 |

`export-jimeng` 生成 JSONL 任务包。附件 URL 写入飞书前需要按现有 `FeishuBitableClient` 的附件上传流程转成 token；不要把本地路径直接写成附件字段。

## 4. 接入现有运行管理表与生成 worker

轻量任务不再新建生成队列表；统一写入现有“短视频自动脚本运行管理表”，字段映射沿用 `jimeng-video-generator/feishu-direct.json`：

- `脚本ID` 固定写轻视频 `job_id`，`任务来源=轻量试穿视频`，用于幂等和结果回流。
- `首帧策略=直接使用原始脚本参考图`；iMini 使用原始脚本图片的第一张，不得再生成一张首帧。
- 每条任务显式写模型，禁止静默改成 Fast 或把 VIP 降级为普通版。
- `defaultRatio` 设为 `9:16`。
- 表内每条任务显式写 8 或 10 秒，不依赖旧配置的 4 秒默认值。
- 每条任务显式写 `720P`，不依赖 iMini 的 480P 默认值。
- 内容 ID 认领必须启用；Prompt 已内嵌 `【内容ID】- job_id`。
- 双机共表时保留 `执行归属` 字段和现有认领规则。

执行入口：

```bash
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu ensure-run-manager-schema --dry-run
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu ensure-run-manager-schema
python3 skills/lightweight-tryon-video/scripts/run_pipeline.py feishu sync-run-manager
```

`sync-run-manager` 先回读运营配置，再按 `job_id + Prompt + 模型 + 时长 + 渠道 + 原始脚本参考图` 形成来源指纹并幂等入队；已有运行中任务时阻塞重复提交。生成视频上传到运行管理表后，命令把附件转存到复核台 `初始成片`，接着执行 LikeU 首屏/类目后期和 BGM，最终写入 `最终视频`。可用 `--no-postprocess` 或 `--no-bgm` 调试单个阶段。

真实提单前先用 worker 的 dry-run 查看任务，再进行小批量提交。不要在开发机、未登录 Chrome 或未确认额度时启动正式提单。

## 5. 生产检查

1. 商品参考图是否真实可访问并保持商品结构。
2. 目标语言是否有对应字幕模板。
3. 每条任务是否为 8/10 秒、9:16、单次生成。
4. Prompt 是否包含唯一内容 ID。
5. 每个视觉方案是否只有一张已确认产品穿搭图；视频任务不得把人物、场景和商品图混成一组让模型自行猜。
6. 产品穿搭图请求是否包含商品图、人物信息和搭配文本；场景参考图为选填，为空时确认完整场景文字已进入请求。
7. 资产是否先回流到 `初始成片`，再执行 LikeU 首屏、类目和 BGM 写入 `最终视频`。
8. 成片是否完成结构 QC、视觉 QC 或人工复核。
9. 只有 `qc_status=passed` 的成片进入发布准备。
