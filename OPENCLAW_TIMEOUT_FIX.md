# OpenClaw 超时问题解决方案

## 问题
对话时提示 "LLM request timed out"

## 诊断结果
- ✓ API 连通性正常
- ⚠️  响应时间较慢（Claude: 4.5秒, Gemini: 11.6秒）
- 使用中转服务：ai678.top 和 yunwu.ai

## 解决方案

### 方案 1: 优先使用更快的 API

当前配置使用 Claude API 作为主要模型，这是正确的选择（响应更快）。

如果仍然超时，可以在 [`openclaw.json`](../openclaw.json:70) 中确认：
```json
{
  "agents": {
    "defaults": {
      "model": {
        "primary": "claude-api/claude-sonnet-4-6"  // 使用更快的 Claude
      }
    }
  }
}
```

### 方案 2: 增加超时时间（推荐）

OpenClaw 可能需要配置更长的超时时间。查看是否有以下配置选项：

```json
{
  "models": {
    "providers": {
      "claude-api": {
        "timeout": 60000,  // 增加到 60 秒（毫秒）
        "baseUrl": "https://www.ai678.top/v1",
        "apiKey": "sk-K6t5ejHgkCSAf7r3"
      }
    }
  }
}
```

### 方案 3: 更换更快的中转服务

如果可能，考虑更换响应更快的中转服务：

**推荐的中转服务**（需要自行注册）：
- https://api.openai-proxy.com
- https://api.chatanywhere.com.cn
- https://api.openai-sb.com

### 方案 4: 使用官方 API（最佳但需要科学上网）

如果有条件，直接使用官方 API：
```json
{
  "models": {
    "providers": {
      "claude-api": {
        "baseUrl": "https://api.anthropic.com",
        "apiKey": "你的官方 API Key"
      }
    }
  }
}
```

### 方案 5: 检查飞书集成配置

飞书集成可能有额外的超时限制。检查飞书插件配置：

```bash
# 查看飞书插件配置
cat /Users/likeu3/.openclaw/extensions/feishu/config.json
```

## 临时解决方案

如果经常超时，可以：

1. **使用更短的提示词** - 减少 token 数量
2. **分步骤提问** - 不要一次问太复杂的问题
3. **重试** - 超时后重新发送消息

## 验证修复

修改配置后，重启 OpenClaw：
```bash
# 重启 OpenClaw 服务
pkill -f openclaw
openclaw start
```

然后测试简单对话：
```
你好
```

如果能正常响应，说明问题解决。

## 监控响应时间

运行测试脚本监控 API 响应时间：
```bash
python3 test_api_connectivity.py
```

如果响应时间持续 >30 秒，建议更换中转服务。

## 需要帮助？

如果问题持续，提供以下信息：
1. OpenClaw 版本：`openclaw --version`
2. 错误日志：`~/.openclaw/logs/`
3. API 响应时间：运行 `test_api_connectivity.py` 的结果
