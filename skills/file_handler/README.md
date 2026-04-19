# File Handler Skill - 飞书文件自动处理

## 🎯 功能

自动读取飞书群聊中发送的文件（Excel、CSV、文本、JSON），无需手动上传。

## 📁 文件结构

```
skills/file_handler/
├── SKILL.md              # Skill 定义文档
├── README.md             # 使用说明
├── config.json           # 配置文件
├── tools.json            # OpenClaw 工具配置
├── file_reader.py        # 核心文件读取模块
├── handle_feishu_file.py # 飞书文件处理模块
├── auto_file_handler.py  # 自动文件检测器
└── read_feishu_file.sh   # Shell 工具脚本
```

## 🚀 使用方法

### 方法1：自动检测（推荐）

当用户在飞书群聊发送文件后，AI 会自动检测到文件并读取：

```python
# AI 自动执行
python3 ~/.openclaw/workspace/skills/file_handler/auto_file_handler.py
```

### 方法2：等待新文件

如果知道用户即将发送文件，可以等待：

```python
python3 ~/.openclaw/workspace/skills/file_handler/auto_file_handler.py --wait --timeout 60
```

### 方法3：指定文件路径

如果知道文件路径，直接读取：

```python
python3 ~/.openclaw/workspace/skills/file_handler/auto_file_handler.py --file /path/to/file.xlsx
```

## 📋 支持的文件格式

| 格式 | 扩展名 | 说明 |
|------|--------|------|
| Excel | `.xlsx`, `.xls` | 完整读取，显示行数、列数、预览 |
| CSV | `.csv` | 完整读取，显示行数、列数、预览 |
| JSON | `.json` | 解析并格式化显示 |
| 文本 | `.txt`, `.md` | 读取前5000字符 |

## 🔧 配置

编辑 `config.json`：

```json
{
  "auto_read": true,           // 是否自动读取
  "max_file_size_mb": 30,      // 最大文件大小
  "supported_formats": [".xlsx", ".csv", ".txt", ".json"],
  "watch_dirs": [              // 监视的目录
    "/tmp/openclaw",
    "~/.openclaw/tmp",
    "/tmp"
  ]
}
```

## 🧪 测试

```bash
# 进入 Skill 目录
cd ~/.openclaw/workspace/skills/file_handler

# 运行测试
python3 file_reader.py test_data.xlsx

# 或运行自动检测
python3 auto_file_handler.py
```

## 💡 使用场景

### 场景1：数据分析

用户在飞书发送 Excel 文件 → AI 自动读取 → 进行数据分析

```
用户: [发送 sales_data.xlsx]
AI: 检测到文件 sales_data.xlsx
    - 行数: 1000
    - 列数: 8
    - 列名: 日期, 产品, 销量, 价格, ...
    
    需要我进行什么分析？
```

### 场景2：数据对比

用户发送两个文件 → AI 读取并对比

```
用户: [发送 data_v1.xlsx 和 data_v2.xlsx]
AI: 已读取两个文件，开始对比分析...
```

## ⚠️ 注意事项

1. **文件大小限制**：默认 30MB，超大文件可能读取失败
2. **临时文件**：飞书文件下载到临时目录，可能被清理
3. **文件时效**：只检测 1 小时内的新文件
4. **依赖要求**：需要 Python3 + pandas + openpyxl

## 🔧 安装依赖

```bash
pip3 install pandas openpyxl
```

## 📝 更新日志

### v1.0.0 (2024-03-16)
- ✅ 初始版本
- ✅ 支持 Excel、CSV、JSON、文本文件
- ✅ 自动检测飞书文件
- ✅ 格式化输出文件摘要

## 🤝 贡献

欢迎提交 Issue 和 PR！
