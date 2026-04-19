# 飞书文件消息处理问题分析

## 问题描述

用户在飞书群聊中发送 Excel 文件，AI 助手无法直接读取文件内容，只能显示 `[File: filename]`。

## 技术分析

### 当前流程

1. **飞书插件接收消息** (`bot.ts`)
   - `parseMessageContent` 将文件消息转换为 `[File: filename]` 文本
   - `resolveFeishuMediaList` 下载文件到本地临时目录
   - `buildAgentMediaPayload` 构建媒体 payload（包含 `MediaPath` 和 `MediaPaths`）

2. **媒体 Payload 结构**
   ```javascript
   {
     MediaPath: "/path/to/file.xlsx",
     MediaPaths: ["/path/to/file.xlsx"],
     MediaType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
   }
   ```

3. **Agent 接收处理**
   - Agent 收到 `MediaPath` 等字段
   - 但 Agent 不会自动读取文件内容
   - 只显示 `[File: filename]` 文本

### 根本原因

OpenClaw 的媒体附件系统主要设计用于：
- 图片（自动添加到消息上下文）
- 音频（自动转录）
- 视频

对于 Excel/CSV 等数据文件，系统会下载但不会自动解析内容。

## 解决方案

### 方案1：Agent 自动读取（需要 OpenClaw 核心更新）

在 Agent 处理入站消息时，检测 `MediaPath` 字段，自动读取文件内容：

```typescript
// 在消息处理流程中添加
if (ctx.MediaPath && isExcelFile(ctx.MediaPath)) {
  const content = await readExcelFile(ctx.MediaPath);
  messageBody += `\n\n[File Content]:\n${content}`;
}
```

**优点：**
- 对所有 Agent 生效
- 用户体验一致

**缺点：**
- 需要修改 OpenClaw 核心代码
- 需要考虑文件大小限制

### 方案2：Skill 层处理（当前可行）

创建一个专门的 Skill 来处理文件消息：

```python
# skills/file_handler/handle_feishu_file.py
import pandas as pd

def handle_feishu_file(file_path: str) -> str:
    """读取飞书文件并返回内容摘要"""
    if file_path.endswith(('.xlsx', '.xls')):
        df = pd.read_excel(file_path)
        return f"表格包含 {len(df)} 行，{len(df.columns)} 列\n列名: {list(df.columns)}"
    elif file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
        return f"CSV 包含 {len(df)} 行，{len(df.columns)} 列"
    else:
        return f"不支持的文件类型: {file_path}"
```

**优点：**
- 无需修改 OpenClaw 核心
- 可灵活定制处理逻辑

**缺点：**
- 每个 Agent 需要单独配置
- 需要用户明确调用

### 方案3：即时变通方案（推荐现在使用）

**当前立即可用的方法：**

1. **飞书多维表格链接**（推荐）
   - 把 Excel 数据导入飞书多维表格
   - 分享表格链接给 AI
   - AI 可以通过 `feishu_bitable_get_meta` 等工具读取

2. **上传到 Workspace**
   - 把 Excel 文件放到 `~/.openclaw/workspace/`
   - 告诉 AI 文件路径
   - AI 使用 `read` 工具读取

3. **粘贴数据**
   - 直接复制 Excel 数据粘贴到聊天中
   - AI 可以直接处理

## 建议

**短期（现在）：**
使用方案3的变通方法，通过飞书多维表格或上传文件到 workspace。

**中期：**
实现方案2的 Skill，让 AI 可以主动读取文件附件。

**长期：**
向 OpenClaw 提交 PR，在核心层支持自动读取数据文件。

## 相关代码位置

- 飞书插件: `/usr/local/lib/node_modules/openclaw/extensions/feishu/src/bot.ts`
- 媒体处理: `/usr/local/lib/node_modules/openclaw/extensions/feishu/src/media.ts`
- 媒体 Payload: `/usr/local/lib/node_modules/openclaw/dist/plugin-sdk/agent-media-payload-B_CWE0i5.js`
- 附件标准化: `/usr/local/lib/node_modules/openclaw/dist/model-selection-46xMp11W.js`
