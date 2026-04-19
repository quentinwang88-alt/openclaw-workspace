---
name: short-video-auto-publisher
description: |
  短视频自动发布 skill。当用户说“跑一遍短视频自动发布”“执行短视频自动发布”“视频自动发布”“自动发布短视频”“GeeLark排期发布”时，优先匹配此 skill。
  基于现有飞书生产脚本表、短视频自动脚本运行管理表和账号表，
  同步脚本主数据、回收并下载已生成视频、按账号固定发布时间槽做未来 24 小时增量补排，
  再调用 GeeLark 创建定时发布任务并同步发布结果。
  适用于“跑一遍短视频自动发布”“同步视频并排期”“刷新标题并纠正当地语言标题”“查询 GeeLark 发布结果”等请求。
  不用于达人视频宫格图、达人打标、Kalodata 拉数、建联话术生成。
---

# Short Video Auto Publisher

## 强匹配提示

以下说法应优先命中这个 skill，而不是 `creator-crm`：

- 跑一遍短视频自动发布
- 执行短视频自动发布
- 视频自动发布
- 自动发布短视频
- GeeLark 自动发布
- 同步视频并排期
- 查询短视频发布结果

以下请求不属于这个 skill：

- 达人视频宫格图
- 达人打标
- Kalodata 拉数
- 建联话术生成

## 核心能力

这个 skill 会：

1. 从生产脚本宽表拆分脚本主数据并落本地数据库
2. 为每条脚本生成结构化 `脚本ID`
3. 为每条脚本生成短视频标题，并尽量保证使用目标国家当地语言
4. 从短视频脚本运行表回收 `脚本ID + 视频`
5. 下载视频到本地目录，写入自动发布数据库
6. 同步账号配置，并按账号固定发布时间生成未来 24 小时槽位
7. 根据两条硬规则做增量补排
8. 调 GeeLark 创建定时发布任务
9. 查询 GeeLark 任务状态并回写数据库

## 默认入口

一键主流程：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/short-video-auto-publisher/run_pipeline.py run-all \
  --publish-mode geelark
```

常用分步命令：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/short-video-auto-publisher/run_pipeline.py sync-script-db
python3 /Users/likeu3/.openclaw/workspace/skills/short-video-auto-publisher/run_pipeline.py sync-accounts
python3 /Users/likeu3/.openclaw/workspace/skills/short-video-auto-publisher/run_pipeline.py sync-videos
python3 /Users/likeu3/.openclaw/workspace/skills/short-video-auto-publisher/run_pipeline.py schedule --publish-mode geelark
python3 /Users/likeu3/.openclaw/workspace/skills/short-video-auto-publisher/run_pipeline.py sync-results --publish-mode geelark
```

## 执行约束

- 当用户说“跑一遍自动发布”“执行真实发布”“同步可发布视频”时，默认执行**全量真实流程**
- 不要自行添加 `--limit`
- 只有当用户明确说“抽样测试”“先跑几条”“只看前 N 条”时，才允许加 `--limit`
- OpenClaw 内优先直接执行绝对路径命令，不先搜索 token、不先 dry-run，除非用户明确要求

标题纠正：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/short-video-auto-publisher/run_pipeline.py refresh-titles \
  --title-mode fallback \
  --llm-route backup \
  --batch-size 10
```

## 默认数据源

- 生产脚本表：内置默认 URL
- 短视频脚本运行表：内置默认 URL
- 账号表：内置默认 URL
- 本地数据库：`/Users/likeu3/.openclaw/shared/data/short_video_auto_publish.sqlite3`
- 本地视频目录：`/Users/likeu3/.openclaw/shared/data/short_video_auto_publish_videos`

通常不需要额外传飞书 URL，除非临时切表。

## 发布规则

当前只保留两条硬规则：

- 同账号同产品 72 小时不重复
- 同店铺同内容家族 48 小时不重复

调度方式：

- 固定发布时间槽
- 未来 24 小时窗口
- 每次只补未排期槽位

## 标题规则

- 标题尽量使用 `目标国家` 对应的当地语言
- 默认会清理 `视频标题：`、`标题：`、`视频主题：` 等前缀
- 如果 LLM 首次返回错语种，会自动换路重试
- 如果多条线路仍然不合格，宁可留空，也不写坏标题

## GeeLark

当前默认对接：

- 上传地址：`/open/v1/upload/getUrl`
- 创建任务：`/open/v1/task/add`
- 查询任务：`/open/v1/task/query`

使用时至少需要：

- `--publish-mode geelark`
- `--publish-api-token`

如果命令里没有显式传 `--publish-api-token`，默认还会自动按以下顺序查找：

1. 环境变量：
   - `SHORT_VIDEO_AUTO_PUBLISH_API_TOKEN`
   - `GEELARK_BEARER_TOKEN`
   - `GEELARK_API_TOKEN`
2. 本地配置文件：
   - `/Users/likeu3/.openclaw/shared/data/short_video_auto_publisher_config.json`

因此在 OpenClaw 中执行本 skill 时，不需要再额外四处搜索 GeeLark token；默认配置存在时可直接执行。

## 推荐触发说法

这些说法都适合触发这个 skill：

- “用 short-video-auto-publisher 跑一遍自动发布”
- “执行短视频自动发布全流程”
- “同步视频并补排 GeeLark”
- “刷新短视频标题，改成当地语言”
- “查询短视频发布结果”
