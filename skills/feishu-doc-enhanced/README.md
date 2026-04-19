# Feishu Doc Enhanced - 飞书文档增强版

## 🎯 解决的问题

飞书 doc API 的 `write` 操作在内容过长时（约 >5000 字符或复杂格式）可能失败，导致文档创建后内容为空。

## ✅ 解决方案

实现智能分批写入机制：
1. **自动检测内容长度**
2. **智能分段**：按段落/章节分割，保持内容完整性
3. **分批追加**：使用 `append` 操作分批写入
4. **错误重试**：单段失败可重试，不影响其他段落

## 📦 文件说明

```
feishu-doc-enhanced/
├── SKILL.md                    # Skill 说明文档
├── feishu_doc_enhanced.py      # 核心实现代码
├── INTEGRATION.md              # 集成指南
└── README.md                   # 本文件
```

## 🚀 快速开始

### 基础使用

```python
from feishu_doc_enhanced import FeishuDocEnhanced

# 初始化
client = FeishuDocEnhanced(batch_size=3000)

# 写入长内容
result = client.write_enhanced(
    doc_token="ABC123def",
    content="# 很长的Markdown内容..."
)

print(f"写入完成: {result['success']}, 共 {result['segments']} 段")
```

### 创建并写入

```python
# 一键创建文档并写入长内容
result = client.create_and_write(
    title="分析报告",
    content="# 很长的内容...",
    folder_token="fldcnXXX"
)
```

## 📊 效果对比

| 特性 | 原生 write | write_enhanced |
|------|-----------|----------------|
| 内容长度限制 | ~5000 字符 | 无限制 |
| 长内容处理 | 可能失败 | 自动分批 |
| 格式保持 | 完整 | 完整 |
| 错误恢复 | 无 | 单段重试 |
| 进度反馈 | 无 | 分段状态 |

## 🔧 核心特性

### 1. 智能分段
- 优先按 `##` 标题分割
- 其次按 `###` 标题分割
- 再次按空行（段落）分割
- 最后强制截断（保持句子完整）

### 2. 错误重试
- 单段失败自动重试（默认 3 次）
- 连续失败记录但不中断
- 返回每段的写入状态

### 3. 格式保持
- Markdown 标题层级完整
- 列表结构不破坏
- 代码块保持完整

## 📖 详细文档

- [SKILL.md](SKILL.md) - Skill 说明和使用方法
- [INTEGRATION.md](INTEGRATION.md) - 与现有 Skill 集成指南
- [feishu_doc_enhanced.py](feishu_doc_enhanced.py) - 源代码和注释

## 🔗 集成建议

### data_analysis Skill
修改 `run_analysis_v5.py`，替换飞书导出部分：

```python
from feishu_doc_enhanced import FeishuDocEnhanced

client = FeishuDocEnhanced()
result = client.write_enhanced(doc_token, markdown_content)
```

### product_testing Skill
修改 `data_assembler.py`，在生成报告后使用：

```python
from feishu_doc_enhanced import FeishuDocEnhanced

client = FeishuDocEnhanced()
result = client.create_and_write(title, report)
```

## ⚙️ 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `batch_size` | 3000 | 每批最大字符数 |
| `max_retries` | 3 | 单段最大重试次数 |

### batch_size 建议

| 内容类型 | 建议值 |
|---------|--------|
| 纯文本 | 5000 |
| 含列表 | 3000 |
| 含代码块 | 2000 |
| 含表格 | 1500 |

## 📝 示例场景

### 场景1：长报告写入
```python
report = generate_long_report()  # 2万字
result = client.write_enhanced(doc_token, report)
# 自动分为 7 段写入
```

### 场景2：批量文档创建
```python
for store in stores:
    report = analyze_store(store)
    client.create_and_write(
        title=f"{store}分析报告",
        content=report
    )
```

### 场景3：错误恢复
```python
result = client.write_enhanced(doc_token, content)
if not result["success"]:
    # 部分成功，重试失败段落
    for status in result["segment_status"]:
        if not status["success"]:
            retry_segment(status["index"])
```

## 🧪 测试

```bash
python feishu_doc_enhanced.py
```

运行内置的测试用例。

## 📈 性能

- **短内容**（<3000 字符）：1 次 API 调用
- **中等内容**（3000-10000 字符）：3-5 次 API 调用
- **长内容**（>10000 字符）：自动分段，每段 1 次调用

## 🤝 贡献

欢迎提交 Issue 和 PR！

## 👨‍💻 作者

**开发经理**  
**最后更新**: 2026-03-19

## 📄 License

MIT License