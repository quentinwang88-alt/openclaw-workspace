---
name: duan-video-auto-publish
description: |
  短视频自动发布别名 skill。用户说“跑一遍短视频自动发布”“执行短视频自动发布”“GeeLark 自动发布”“同步视频并排期”“查询短视频发布结果”时，可优先命中此 skill。
  这是一个窄入口，直接执行 short-video-auto-publisher 的主流程，不用于达人视频宫格图、Kalodata 拉数、脚本生成或脚本同步。
---

# 短视频自动发布别名入口

这个 skill 只做一件事：

- 把“短视频自动发布”这类自然语言请求，稳定引导到真正的发布主流程

## 适用说法

- 跑一遍短视频自动发布
- 执行短视频自动发布
- GeeLark 自动发布
- 同步视频并排期
- 查询短视频发布结果

## 不适用说法

- 达人视频宫格图
- Kalodata 拉数
- 原创脚本生成
- 同步脚本到运行表

## 默认执行

优先直接执行：

```bash
python3 /Users/likeu3/Desktop/skills/workspace/skills/short-video-auto-publisher/run_pipeline.py run-all \
  --publish-mode geelark
```

如果没有显式传 token，主流程也会自动从
`/Users/likeu3/.openclaw/shared/data/short_video_auto_publisher_config.json`
读取 GeeLark token。

默认执行真实全量流程时：

- 不要自行添加 `--limit`
- 不要先跑 dry-run
- 只有用户明确要求抽样或测试时，才允许加 `--limit` 或切 `dry-run`

如果用户只提到某个单步，则调用对应子命令：

- `sync-accounts`
- `sync-videos`
- `schedule`
- `sync-results`
- `refresh-titles`

## 说明

- 真实发布逻辑仍然在 `short-video-auto-publisher`
- 这个 skill 的作用只是让 OpenClaw 更容易第一次就找对入口
