# File Handler Skill

自动处理飞书群聊中的文件附件，支持 Excel、CSV 等数据文件。

## 功能

- 自动检测飞书文件消息
- 读取 Excel/CSV 文件内容
- 提取数据摘要和列信息
- 支持数据分析任务

## 使用方法

当用户在飞书群聊发送文件时，AI 会自动：
1. 检测文件类型
2. 读取文件内容
3. 提取数据摘要
4. 根据上下文决定如何响应

## 支持格式

- `.xlsx`, `.xls` - Excel 文件
- `.csv` - CSV 文件
- `.txt`, `.md` - 文本文件
- `.json` - JSON 文件

## 配置

在 `config.json` 中配置：

```json
{
  "auto_read": true,
  "max_file_size_mb": 30,
  "supported_formats": [".xlsx", ".csv", ".txt", ".json"]
}
```
