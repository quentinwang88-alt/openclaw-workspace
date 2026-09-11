# CreatOK 渠道接入记录

> 更新时间：2026-09-05
> 现状：**代码基础已就绪（迁移/模型/适配器/路由/测试），真实发布被 envelope 门禁拦住，等待 API Key 与官方样例接入后开通。**

## CLI 版本与安装

- 包：`@creatok/cli`（npm 全局），安装命令 `npm install -g @creatok/cli@0.13.0`
- 当前锁定版本：**v0.13.0**（stable，commit `1c7cc30`，skills_hash `26e5d84a4964`）
- 升级前先 `creatok doctor` 看 `update` 块；升级用 `creatok update --apply`，失败回退按官方指引手动装
- CLI 要求 Node 18+；本机 Node v24.14.0
- 官方内嵌 skill：`creatok-publish-tiktok`（v1.0.6），已随 CLI 安装到 `~/.claude/skills/`
- 内嵌 skill 会漂移：`creatok doctor` 的 `skills_drift` 为 true 时跑 `creatok skills install`

## API Key 配置

- 生成地址：https://creatok.ai/app/workspace/api-keys
- 官方变量名：`CREATOK_API_KEY`（stable channel；preview 用 `CREATOK_API_KEY_PREVIEW`，两者独立）
- 本项目支持别名环境变量（如 `CREATOK_API_KEY_MAIN`）——`publisher_profiles.api_key_env_name` 存名字，
  适配器把别名值映射到标准变量名注入子进程，**Key 本体绝不落库/进 argv/进日志**
- 校验：`creatok doctor` 的 `data.api_key_configured` 必须为 true
- 本机配置（2026-09-05）：已写入 `~/.zshrc`（机器级，不进 git）；非交互 shell 需要显式 export

## 账号发现现状（2026-09-05，真实连接）

当前 workspace 仅 1 个连接：

- `yoursbeauty.my`（platform=**tiktok_shop**，platform_account_id=7525721272178066433）
- connection_uid：`conn_ts_4zDg4wT0Wd9L82ysizOb66`，health=ready
- capabilities：`publish_shoppable_video`、`add_showcase_products`、`remove_showcase_products`
- **不含 `publish_content_video`/`publish_content_photo`（普通 Content Posting 能力）→ 首期非带货无可用连接**
- **不含 `publish_shoppable_photo`（Shop 图文）→ 第二期 Shop 图文能力未开通**
- `creator-info` 对该连接返回 404（`publish_connection_not_found`）——该命令仅服务 Content Posting
- 结论：当前可用的唯一真实能力是 **Shop 带货视频**；首期目标（非带货视频/图文）需先在
  CreatOK workspace（creatok.ai）新增普通 TikTok 账号连接

能力名与本地字段映射（已按真实连接校准）：

| 远端 capabilities 子串 | 本地列 |
|---|---|
| publish_content_video / content_video | content_video_capable |
| publish_content_photo / content_photo | content_photo_capable |
| publish_shoppable_video / shoppable_video / publish_shop_video | shop_video_capable |
| publish_shoppable_photo / shoppable_photo / publish_shop_photo | shop_photo_capable |
| direct_post（Content Posting 才判定） | direct_post_capable / delivery_mode |

## 已确认的 CLI 命令（v0.13.0）

```text
creatok doctor                                  健康检查（api_key_configured / skills_drift）
creatok publish connections [--capability] [--platform] [--operation-id]
creatok publish capabilities --connection <uid> [--operation-id]
creatok publish creator-info --connection <uid> [--operation-id]
creatok publish products --connection <uid> [--keyword] [--operation-id]
creatok publish music --connection <uid> (--keyword|--url) [--operation-id]
creatok publish prepare-video --connection <uid> --file <asset.json> [--json] [--operation-id]
creatok publish submit --file <envelope.json> | --json <envelope>  [--operation-id]
creatok publish status --job-id <job_id> | --idempotency-key <key>  [--operation-id]
creatok assets create --type video|image --file <path>
creatok capabilities（远程能力：模型/默认值/限制）
creatok version | update | jobs | logs
```

要点（来自官方 skill 文档）：

- 每个内容项生成一个 `operation_id`，发现/准备/提交/状态全部复用
- 本地媒体必须先 `assets create` 成正式 Asset，提交时用最终 `object_key`
- Shop Video 还需 `prepare-video` 转 `file_id`；`upload_session` 不可暴露/修改
- envelope 是 **snake_case** 的最终 JSON，经 `submit --file` 提交
- 超时/结果未知后**禁止**重新生成 key 提交，先 `status --idempotency-key <原key>` 恢复
- `error.result_unknown=true` 或 `data.job.result_unknown=true` → 只能对账，不得自动重发
- 明确失败后可重发（需重新确认 + 新 key）
- 错误链路保留 `request_id`、`error.reason`、`error.stage`

## 状态映射（app/creatok_publish.py `parse_task_status`）

| 远端状态 | 本地状态 | result |
|---|---|---|
| queued/scheduled/processing | pending | 已排期 |
| waiting_confirmation/inbox | pending_confirmation | 待账号确认 |
| succeeded/published | success | 发布成功 |
| failed 且 result_unknown=false | failed | 发布失败 |
| result_unknown=true | unknown | 结果未知，继续对账 |
| canceled | terminated | 已取消 |

## 幂等与安全（已实现）

- 任务 ID：`creatok:<job_id>`，经 `task_prefix_adapters` 路由（禁串渠道）
- `build_submission_context(request)` 在 reserve 前把 `provider/operation_id/idempotency_key/
  content_type/commerce_type/timezone/media_sha256/connection_uid` 冻结进 `submission_context_json`
- `idempotency_key` 为确定性哈希（account_id+script_id+publish_at+timezone+类型+媒体指纹），
  崩溃后重建一致；`reconcile_scheduled_task` 只用冻结的 key 只读查询
- 超时/`result_unknown` → 抛 `CreatOKResultUnknownError`（submission_ambiguous，禁止重发）
- API Key 只经子进程环境变量；CLI 错误消息经 `_sanitize_error_text` 脱敏
- `terminate_task` 实现为抛错：CLI v0.13.0 无公开取消命令，不得宣称远端已取消

## Envelope 契约（2026-09-05 正式账号采集，CLI v0.13.0）

**tiktok_shop_video**（唯一实测成功契约）——fixture 已固化在
`tests/fixtures/creatok_envelopes/`：

```json
{
  "kind": "tiktok_shop_video",
  "connection_uid": "<from publish connections>",
  "idempotency_key": "<stable sha256>",
  "timezone": "GMT+8",
  "media": {"file_id": "<prepare-video 返回>", "file_kind": "video_file"},
  "provider_options": {
    "product_id": "...", "product_title": "...", "title": "..."
  }
}
```

实测发现（确定性结论，来源于真实 submit 的 pydantic 校验反馈）：

- `kind` 枚举：**`tiktok_shop_video` 有效**（`shop_video`/`shoppable_video`/`video` 等无效）
- `timezone` 仅支持 **`GMT+8`/`EST`/`PST`/`BRT`/`CST`**（非 IANA！发布前需要 IANA → GMT+ 转换，泰国 GMT+7 不在枚举内）
- `media.file_id` = `prepare-video` 返回的 file_id；provider_options 必须含 product_id/product_title/title
- **无显式排期字段**：顶层与 provider_options 里 `schedule_time`/`publish_at`/`scheduled_at`…
  全部 `unrecognized_keys`。提交后 job.schedule_time 为 null，发布时点由服务端决定——
  **⚠️ 未确认"提交后是立即发布还是默认排队"之前不得在真实账号上发起可过 precheck 的提交**
- 创建响应：`data.created` + `data.job`（job_id 为数字，含 state/raw_status/result_unknown/
  fail_reason/diagnostic/schedule_time/platform_content_urls…）
- status 查询：`data.job` 与创建响应同构；`status --job-id` 与 `status --idempotency-key` 均已实测
- 失败的 definite 语义：`result_unknown=false` + `retryable=false` + `recovery_action`；
  Provider precheck 拒绝样例（precheck_failed / "Video id is invalid" / code 170001040）已归档 fixture

**Organic Video / Organic Photo / Shop Photo 三种契约仍未采集**——当前 workspace 无
Content Posting 连接（需求方：在 creatok.ai 添加普通 TikTok 账号），Shop Photo 能力未开通。

## 有机契约（2026-09-05 tamarawoo 采集，CLI v0.13.0）

连接 `conn_tc_3cWjrL84udzEWMsixHyspt`（tiktok_content_posting，显示名 tamarawoo，
**creator-info 真实用户名 tamarawoo8363**，privacy 3/4 档，视频上限 600 秒）。
capabilities：`publish_content_video` + `publish_content_photo`。

**tiktok_content_video**（fixture 已固化；2026-09-05 实测端到端成功——真实 job 355065）：

```json
{
  "kind": "tiktok_content_video",
  "connection_uid": "conn_tc_...",
  "idempotency_key": "...",
  "media": {"object_key": "uploads/videos/<sid>/<name>.mp4", "file_name": "xxx.mp4"},
  "provider_options": {
    "post_mode": "DIRECT_POST",
    "privacy_level": "PUBLIC_TO_EVERYONE",
    "disable_comment": false, "disable_duet": false, "disable_stitch": false,
    "brand_content_toggle": false, "brand_organic_toggle": false
  },
  "schedule": {"at": 1788681600000, "timezone": "GMT+8"}
}
```

- 实测 `post_mode` 变体**仅 `DIRECT_POST`**（inbox/其他均 invalid_union）
- **排期字段为 `schedule` 对象**：`at`（毫秒，绝对 UTC 时间戳，由账号 IANA 时区换算）、`timezone`
  （仅 `GMT+8`/`EST`/`PST`/`BRT`/`CST`；亚洲站统一映射 GMT+8，实际时刻由 `at` 决定，
  **泰国/越南 GMT+7 同样可用**——实证 09-06 15:00 Bangkok → at=1788681600000 精确）
- **`media.object_key` 用 assets create 返回的裸对象键（uploads/...）**；assets list 的签名
  `media_url` 只是下载视图，用于提交会报 `asset_not_accessible`（422，实测）
- 窗口：`at` 必须 >= 当前 +60 秒（服务器校验"Schedule time must be at least 60 seconds in the future"）

**2026-09-05 端到端实证**（泰国养号视频-2 → tamarawoo，opv 素材）：
- submit 成功：job_id=355065，state=`scheduled`，raw_status=`SCHEDULED`，排期 09-06 15:00 Bangkok
- 本地闭环：slot=已排期 + `creatok:355065` + video_assets 已排期
- 状态可回读：`status --job-id 355065` 正常（scheduled）
- 早前一次提交被标记"结果不明"（submit 报错且未建 job）——对账 `status --idempotency-key`
  返回 notfound（definite），防重复机制按设计工作

**2026-09-06 发布后画面异常复盘**：job 355065 在 CreatOK 返回
`PUBLISH_COMPLETE`，但 TikTok 端显示花屏。排班引用的本地 MP4、CreatOK 中四个上传副本
均为相同 SHA256，全部可完整解码，排期 epoch 也准确对应 Bangkok 15:00。因此异常位于
CreatOK 向 TikTok 交付或 TikTok 二次转码阶段。此后 Organic Video 在上传前固定生成
H.264 High L4 / yuv420p / CFR 30 / AAC 44.1kHz / faststart 兼容副本；无音轨源补静音
AAC。`assets create` 后必须从签名 URL 回读整文件并核对字节哈希，未通过不提交。
submission context 同时保存原始/兼容文件哈希、asset UID、object key、最终 envelope、
operation ID、request ID 与 provider submission ID，便于区分本地、上传和平台转码故障。

**tiktok_content_photo**（schema 与真实定时任务均已验证）：

```json
{
  "kind": "tiktok_content_photo",
  "connection_uid": "...", "idempotency_key": "...",
  "media": {"object_keys": ["uploads/images/...", "..."]},
  "provider_options": {
    "post_mode": "DIRECT_POST", "privacy_level": "PUBLIC_TO_EVERYONE",
    "disable_comment": false, "auto_add_music": true,
    "brand_content_toggle": false, "brand_organic_toggle": false,
    "title": "短标题", "description": "正文 #标签"
  }
}
```

**资产对象键**：Content Posting 图文使用 `assets create` 返回的最终
`data.asset.object_key`（`uploads/...`），不能把临时上传键或签名下载 URL 写入 envelope。
Shop `prepare-video` 继续使用它自己的正式 file 引用流程。

**普通图文音乐边界**：TikTok Content Posting 只支持 `auto_add_music`，由 TikTok
自动选择推荐音乐，不能指定 `music_id`。本系统把它记录为 `platform_auto`，不显示为
“已选中具体 BGM”。CreatOK Shop 连接的曲库检索属于另一条发布能力。

**普通短视频音乐**：CreatOK Content Posting Video 当前不接受平台 `music_id`。
系统复用 OPV/NeoBund 的内容画像、时长、节奏评分和账号三日去重策略，但曲目来源限定为
`auto_mixcut/assets/bgm/LICENSES.csv` 中明确允许商业使用且本地文件存在的音频。选中后先
生成带 BGM 的独立成片，再执行 H.264/AAC 兼容转码与上传。`bgm_json.mode=local_mix`
同时记录授权来源、选曲分数、混音起点、音量和输出哈希；由于音轨已经嵌入文件，发布后
无需等待平台音乐回读。

上述普通短视频本地混音路径已于 2026-09-06 暂停，`CREATOK_LOCAL_BGM_ACTIVE=false`。
生产默认行为是：CreatOK 普通短视频不自动配本地 BGM；原生多图继续设置
`auto_add_music=true`，由 TikTok 自动推荐音乐。图文生产不再为了配乐转成短视频。

**AI 标识边界**：CLI v0.13.0 的 `tiktok_content_photo` schema 会拒绝顶层及
`provider_options.is_aigc`。本地会冻结 `ai_generated` 和
`provider_ai_label_status=unsupported_by_creatok_v0.13.0`，但在 CreatOK 提供正式字段前
不能声称该标识已透传到 TikTok。

**Shop Video**（上一节）补充：`prepare-video` 的 asset-json 也用签名 media_url；
无排期字段（发布时点服务端决定，未经官方确认前不真实提交）。

## 排班窗口（实现约束）

- Organic：60 秒 ~ 7 天；Shop：60 秒 ~ 30 天；超窗口保留"待排期"，不标失败
- 时区用账号 IANA 时区（Asia/Bangkok 等），不用本机时区；提交时映射为 CreatOK GMT+ 枚举
- CreatOK Content Posting 图文已接入 `run_pipeline`；排期前校验标题 90、正文 4000
  UTF-16 单位，并把文案与音乐模式计入幂等键。

## 数据库（已迁移，幂等）

- 新表 `publisher_profiles`：profile_id、provider、workspace_name、api_key_env_name、
  cli_path、enabled、last_health_check_at、last_error
- `account_configs` 新增列：publish_profile_id、provider_connection_uid、account_timezone、
  content_video_capable、content_photo_capable、shop_video_capable、shop_photo_capable、
  direct_post_capable、delivery_mode、provider_health、provider_checked_at、provider_error
- 列同步方法：`update_account_provider_info` / `mark_account_provider_error` / `get_creatok_connection_cache`
- profile 方法：`upsert_publisher_profile` / `get_publisher_profile` / `list_publisher_profiles`

## 部分实现文件

- `app/creatok_publish.py`：CreatOKCLI（subprocess shell=False）+ CreatOKPublishAdapter
- `app/models.py`：`PublishRequest`（video|photo × organic|shop，多 media_paths）
- `app/publishers.py`：`BasePublishAdapter.create_publish_task`（默认转发单视频旧接口）+
  `RoutedPublishAdapter.create_publish_task`（CreatOK 渠道路由）+ 渠道归一化加 creatok
- `app/capabilities.py`：`reconcile_creatok_account_capabilities`（连接发现回写，宽容解析待校准）
- `app/scheduler.py`：提交点按渠道分流；reserve 前冻结 CreatOK 身份
- `run_pipeline.py`：auto 模式注册 CreatOK 路由；`sync-account-capabilities` 默认 auto 同时同步两渠道
- 配置：config JSON 可写 `creatok_cli_path`、`creatok_api_key_env_name`

## 待办

1. ✅ `CREATOK_API_KEY` 已配置（~/.zshrc，doctor/reachable 通过）
2. ✅ 账号连接发现 + 能力回写（只读，代码经真实连接端到端验证；本地库无 CreatOK 账号配置，等飞书账号表 + 同步）
3. ✅ Shop Video 官方 envelope 契约已采集并固化 fixture；**排期语义未确认，真实提交保持关闭**
4. ⏳ 老板在 creatok.ai 添加普通 TikTok Content Posting 连接 → 采集 Organic Video/Photo 契约 → 首期非带货
5. ⏳ 向 CreatOK 确认 Shop 发布排期语义（提交后立即发布 or 服务端排队）
6. 全契约接通后：单账号 real test → 7 天并跑 + 50 条无重复对账后批量迁移
- 时区用账号 IANA 时区（Asia/Bangkok 等），不用本机时区
