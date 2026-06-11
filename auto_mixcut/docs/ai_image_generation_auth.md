# AI Image Generation Auth

`auto_mixcut` 的 AI 补素材链路会依赖外部生成能力。当前视频生成主要走即梦/imini 提单与回流，但参考图准备、首帧/图像生成验证、后续 image2 能力扩展会经过 OpenClaw 的 `openai-codex` / `gpt-image-2` 通道。

这份文档记录“Codex 账号切换后，如何保障 auto_mixcut AI 补素材链路不被 OpenClaw image2 授权和代理问题打断”。

## 需求背景

业务目标：

- 商品缺 hero/detail/result 等关键素材时，`auto_mixcut` 能自动创建 AI 补素材任务包。
- AI 任务需要稳定读取 OSS 商品图包、生成/提交素材，并把回流视频继续纳入切片、打标、有效角色、render plan。
- OpenClaw/Codex 账号切换后，不应该让 AI 补素材支线卡在 image2/OAuth/代理配置上。

典型问题：

- `HTTP 401 token_invalidated`：OpenClaw 仍在使用旧 Codex OAuth。
- `unsupported_region`：OAuth token exchange 没走代理。
- `ETIMEDOUT ...:443`：image2 请求直连外网，没走本机代理。
- `SsrFBlockedError`：显式本机代理被 OpenClaw SSRF 保护拦截。
- `HTTP 429 usage_limit_reached`：授权和网络已通，但当前 Codex 账号 image2 额度不足。

## 当前方案

OpenClaw 侧保留一个当前账号 profile：

```text
openai-codex:codex-current
```

并将 `openai-codex` provider 的优先级指向它：

```bash
openclaw models auth order get --provider openai-codex
```

期望输出包含：

```text
Order override: openai-codex:codex-current
```

OpenClaw 配置中，`models.providers.openai-codex.request` 需要显式代理：

```json
{
  "allowPrivateNetwork": true,
  "proxy": {
    "mode": "explicit-proxy",
    "url": "http://127.0.0.1:18080"
  }
}
```

这样即使 shell 进程没有继承 `HTTP_PROXY/HTTPS_PROXY`，OpenClaw image2 也不会直连超时。

## 业务边界

这套配置只用于 OpenClaw 的 `openai-codex` / image2 通道。

不要把它误用到 `auto_mixcut` 主打标链路：

- AI 打标、质检等主 LLM 调用由 `llm_router_skill` 控制。
- Doubao/Volcano ARK 打标不需要走 `chatgpt.com/backend-api/codex`。
- 如果 shell 里残留了错误的 `HTTP_PROXY/HTTPS_PROXY`，反而可能干扰 Doubao/ARK 调用。

## 开发落点

已新增本机 Codex skill：

```text
/Users/likeu3/.codex/skills/openclaw-codex-account-switch/SKILL.md
```

用途：

- 诊断 OpenClaw 当前 `openai-codex` profile 和 order。
- 检查 image2 常见错误。
- 修复显式代理配置。
- 给出 image2 验证命令。

诊断脚本：

```bash
/Users/likeu3/.codex/skills/openclaw-codex-account-switch/scripts/check_openclaw_codex.sh
```

修复 OpenClaw 显式代理配置：

```bash
/Users/likeu3/.codex/skills/openclaw-codex-account-switch/scripts/check_openclaw_codex.sh --fix-proxy-config
```

脚本默认不打印 OAuth token。

## 运维流程

Codex 账号切换后，先执行：

```bash
openclaw models auth list
openclaw models auth order get --provider openai-codex
/Users/likeu3/.codex/skills/openclaw-codex-account-switch/scripts/check_openclaw_codex.sh
```

如果 `openai-codex:codex-current` 不是当前默认账号，重新登录：

```bash
openclaw models auth login --provider openai-codex --set-default
```

如果普通登录遇到 callback/manual input/unsupported region 问题，使用 `openclaw-codex-account-switch` skill 中记录的代理 OAuth helper 方案处理。

改完 OpenClaw 配置后重启 gateway：

```bash
launchctl bootout gui/501 /Users/likeu3/Library/LaunchAgents/ai.openclaw.gateway.plist
launchctl bootstrap gui/501 /Users/likeu3/Library/LaunchAgents/ai.openclaw.gateway.plist
```

验证 image2：

```bash
openclaw infer image generate --model openai/gpt-image-2 \
  --prompt "A simple clean product photo of one gold earring on a white background, no text." \
  --size 1024x1024 \
  --output /private/tmp/openclaw-image2-probe.png \
  --json \
  --timeout-ms 180000
```

判断标准：

- 生成图片文件：通道完全正常。
- 返回 `HTTP 429 usage_limit_reached`：OAuth 和代理已正常，当前账号额度不足。
- 返回 `401/token_invalidated`：需要重新绑定当前 Codex 账号。
- 返回 `ETIMEDOUT`：请求仍在直连，检查显式代理配置。
- 返回 `SsrFBlockedError`：检查 `allowPrivateNetwork: true`。

## 和 auto_mixcut 流程的关系

当 `auto_mixcut` 发现素材容量不足时，正常链路应为：

```text
readiness / guard
  -> 判断缺口
  -> 创建 AI 补素材 prompt package
  -> 即梦/imini 提单
  -> 回流 AI 视频
  -> 导入 RDS/OSS
  -> probe / segment / frames / AI tagging / QC / anchor check
  -> effective_roles
  -> top-up render plan
  -> render / Feishu sync
```

OpenClaw image2 授权不是 render 主链路的业务判断条件，但它会影响 AI 补素材支线的生成能力和后续扩展。因此它应被视作 AI 补素材基础设施健康检查的一部分。

## 后续建议

- 在 auto_mixcut guard 中增加“AI 生成通道健康检查”摘要字段，只记录状态，不阻断非 AI 补素材主流程。
- 当缺口需要 AI 补素材但 image2/OpenClaw 生成通道异常时，把原因写入 `content_tasks.last_error` 或 guard detail。
- 将 `HTTP 429 usage_limit_reached` 识别为“额度不足”，避免误判为授权失败。
- 后续如果 image2 不再参与补素材，可把本检查降级为 OpenClaw 运维项，不进入 auto_mixcut 阻断条件。
