---
name: openai-image
description: |
  Generate or edit images with OpenAI Image API via Codex Responses path
  (chatgpt.com/backend-api/codex) or legacy OpenAI Images API.
  Uses the openai-codex OAuth token (same as OpenClaw's built-in image_generate).
  Supports SOCKS5 proxy for network-restricted environments.
  Trigger for requests such as "跑一下 gpt-image-2 文生图",
  "用 prompt 生成头像图", "编辑这张商品图背景",
  "用 codex 路径生成图片", "通过 codex 接口生成图片".
---

# OpenAI Image

这个 skill 是 OpenClaw 里的独立图片处理入口，支持两种 API 模式：

- **codex**（默认）：通过 `chatgpt.com/backend-api/codex` 的 Responses API 调用图片生成，复用 openai-codex OAuth 认证
- **openai**（旧版）：通过标准 OpenAI Images API（`api.openai.com/v1`）调用，需要 `OPENAI_API_KEY`

当前支持的操作：

- `mode=generate`：文生图
- `mode=edit`：基于本地图片按 prompt 编辑

## 认证机制

### codex 模式（默认）

和 OpenClaw 内置 `image_generate` 工具走同一条路径：复用 openai-codex OAuth 登录。

OpenClaw 的 openai 插件在检测到没有普通 OpenAI API key 但存在 openai-codex OAuth 时，会自动走 Codex 路径生成图片。本 skill 的 codex 模式与之一致。

access token 自动从以下位置获取：

1. `OPENCLAW_AGENT_AUTH_PROFILE_PATH` 中的 `openai-codex:default` profile
2. `~/.codex/auth.json` 中的 Codex CLI token
3. `~/.hermes/auth.json` 中的 openai-codex provider token

### openai 模式（旧版）

使用 `OPENAI_API_KEY` 和 `OPENAI_BASE_URL` 环境变量。

## Codex API 要求

Codex Responses API 有以下硬性要求：

- **必须设置 `stream: true`**：API 强制要求流式响应（SSE），不支持同步请求
- **必须包含 `instructions` 字段**：缺少会返回 400 错误
- **图像模型使用 `gpt-image-2`**：和 OpenClaw 内置 image_generate 工具一致，不要用聊天模型（如 gpt-5.5）

## SOCKS5 代理

如果本机网络需要 SOCKS5 代理才能访问外部 API，设置环境变量：

```bash
ALL_PROXY=socks5://127.0.0.1:1080
# 或
SOCKS_PROXY=socks5://127.0.0.1:1080
```

需要安装 `httpx-socks`：

```bash
pip install httpx-socks
```

也支持 `HTTP_PROXY` / `HTTPS_PROXY` 环境变量作为备选。

## 环境变量

```bash
# API 模式选择
OPENAI_IMAGE_API_MODE=codex          # codex 或 openai，默认 codex

# Codex 模式配置
OPENAI_CODEX_BASE_URL=https://chatgpt.com/backend-api/codex
OPENAI_CODEX_IMAGE_MODEL=gpt-image-2  # 图像模型，默认 gpt-image-2（与 OpenClaw 内置一致）
OPENAI_CODEX_INSTRUCTIONS=You are an image generation assistant.  # 系统指令
# ORIGINAL_SCRIPT_PRIMARY_LLM_API_KEY=  # 手动指定 codex access token（一般不需要，自动读取 OAuth）

# 旧版 OpenAI 模式配置
OPENAI_API_KEY=
OPENAI_BASE_URL=https://api.openai.com/v1
OPENAI_IMAGE_MODEL=gpt-image-2

# 代理配置
ALL_PROXY=socks5://127.0.0.1:1080
# 或
HTTP_PROXY=http://127.0.0.1:7890
HTTPS_PROXY=http://127.0.0.1:7890

# 通用配置
OPENAI_IMAGE_OUTPUT_DIR=/Users/likeu3/.openclaw/workspace/runtime/image_outputs
OPENAI_IMAGE_DEFAULT_SIZE=1024x1024
OPENAI_IMAGE_DEFAULT_QUALITY=medium
OPENAI_IMAGE_DEFAULT_FORMAT=png
OPENAI_IMAGE_TIMEOUT=120
OPENAI_IMAGE_MAX_RETRIES=3
```

## 输入格式

输入是一个 JSON 文件，至少包含：

- `task_id`
- `mode`
- `prompt`

编辑模式额外需要：

- `input_image_path`

保留但暂不参与业务逻辑的字段：

- `task_type`
- `target_field`

可参考：

- `/Users/likeu3/.openclaw/workspace/skills/openai-image/examples/sample_generate.json`
- `/Users/likeu3/.openclaw/workspace/skills/openai-image/examples/sample_edit.json`

## 运行方式

文生图（codex 模式，默认）：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/openai-image/run_pipeline.py \
  --input /Users/likeu3/.openclaw/workspace/skills/openai-image/examples/sample_generate.json
```

文生图（旧版 OpenAI 模式）：

```bash
OPENAI_IMAGE_API_MODE=openai python3 /Users/likeu3/.openclaw/workspace/skills/openai-image/run_pipeline.py \
  --input /Users/likeu3/.openclaw/workspace/skills/openai-image/examples/sample_generate.json
```

图编辑：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/openai-image/run_pipeline.py \
  --input /Users/likeu3/.openclaw/workspace/skills/openai-image/examples/sample_edit.json
```

## 运行规则

- 默认模式：`codex`（通过 chatgpt.com/backend-api/codex，复用 openai-codex OAuth）
- 默认图像模型：`gpt-image-2`（与 OpenClaw 内置 image_generate 工具一致）
- 默认输出格式：`png`
- 默认尺寸：`1024x1024`
- 默认质量：`medium`
- Codex 请求强制使用 SSE 流式响应，自动从 `response.completed` 事件提取图片
- API 失败时会按 `1s / 2s / 4s` 做简单重试
- 成功时自动创建输出目录并落盘图片
- 失败时不会直接崩溃，而是返回结构化 JSON

## 注意事项

- Codex 模式需要先登录 OpenClaw / Hermes 账号以获取 openai-codex OAuth token
- `edit` 模式下的 `input_image_path` 必须是本地真实路径
- 如果传了 `mask_image_path`，该路径也必须存在
- 当前只支持 `generate` / `edit` 两种模式
- 使用 SOCKS5 代理需安装 `httpx-socks`
- Codex 图像模型用 `gpt-image-2`，不要用聊天模型（gpt-5.5 等），后者不支持图片生成
