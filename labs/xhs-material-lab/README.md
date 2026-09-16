# 小红书穿搭素材实验室（xhs-material-lab）

技术预研与试点的隔离沙盒。**与图文模块（packages/organic_photo_video）零耦合**：
不注册任何定时任务、不写飞书、不写 RDS、不进 `~/.openclaw/shared/data/` 产物树、
不碰妙手 9333 与任何现有浏览器登录态。方案全文见
`docs/XHS_OUTFIT_MATERIAL_INTAKE_PRE_RESEARCH_20260915.md`。

## 目录

```
labs/xhs-material-lab/
├── README.md                 ← 本文件（试点操作手册）
├── schema.sql                ← 本地素材库结构（sqlite）
├── scripts/
│   ├── material_lab.py       ← 建库 / 链接入库 / 状态盘点（纯标准库）
│   └── fetch_notes.py        ← 试点抓取骨架（调 XHS-Downloader，见文件头说明）
├── third_party/XHS-Downloader/   ← 源码 + 独立 uv venv（gitignored）
├── var/                      ← 素材库 sqlite（gitignored，运行后生成）
└── out/                      ← 下载产物（gitignored）
```

## 已完成的验证（2026-09-15）

- XHS-Downloader 已浅克隆 + `uv sync --no-dev` 装好独立环境（Python 3.12.13）。
- 冒烟通过：`from source import XHS` 库导入 OK；`main.py --help` CLI 正常。
- **未做**真实抓取（不登录、不请求小红书）——按计划留给 20 条链接试点。

## 试点 A：按链接入库（先做这个）

1. 把运营挑好的 ≤20 条笔记分享链接存成 `pilot_links.txt`（一行一条，保留完整链接含 xsec_token）。
2. 建库 + 入库：
   ```bash
   cd labs/xhs-material-lab
   python3 scripts/material_lab.py init
   python3 scripts/material_lab.py add-links --theme "旅游冬装" --file pilot_links.txt
   ```
3. 小批量抓取（每批 ≤5 条，手动触发）：
   ```bash
   uv run --project third_party/XHS-Downloader python scripts/fetch_notes.py --limit 5
   ```
   首跑需要按 raw.json 实际结构微调字段映射（fetch_notes.py 头部有标注）。
4. 盘点：`python3 scripts/material_lab.py status`

记录四个指标：完整获取率、图片完整率、单篇耗时、人工介入次数（fetch_runs 表）。

## 试点 B：按主题自动搜索（试点 A 跑顺后再做）

- 工具：xiaohongshu-mcp（Apache-2.0，有 darwin-arm64 release 二进制，首跑自动下载约 150MB 无头浏览器）。
- 安装与登录（一次性，扫码）：从 GitHub Releases 下载 `xiaohongshu-login-darwin-arm64` 与
  `xiaohongshu-mcp-darwin-arm64` 放入 `third_party/`，登录态存实验室自己的 `data/` 目录。
- **纪律：只用读取类接口**（`search_feeds` / `get_feed_detail` / `user_profile`）。
  该工具同时带发布/点赞/收藏/评论接口，**一律禁用**。
- 用**专用小号**，不用日常账号（同一账号不允许多个网页端登录；引流/搬运是官方重点打击对象）。
- 每主题 ≤20 篇候选，只产出候选清单（标题/作者/链接/互动量），审核后把链接回流试点 A 入库。

## 红线（本实验室自守）

1. 素材默认 `authorization=reference_only`，仅供提取搭配/场景/选题参考；
   带水印的第三方图**永不**进入 OPV 发布链（OPV 既有合规红线）。
2. 登录失效/出现验证提示 → 停止并人工处理，禁止自动重试循环。
3. 不注册 cron / LaunchAgent；一切运行都是手动小批量。
4. 将来若要接 OPV，只允许「只读供给适配器 + 默认关闭的实验开关」单向接入（方案 §7）。
