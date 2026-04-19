# Feishu Doc Enhanced - 集成指南

## 快速开始

### 1. 安装

将 `feishu_doc_enhanced.py` 复制到你的项目目录：

```bash
cp feishu_doc_enhanced.py /path/to/your/project/
```

### 2. 基础使用

```python
from feishu_doc_enhanced import FeishuDocEnhanced

# 初始化
client = FeishuDocEnhanced(batch_size=3000, max_retries=3)

# 写入长内容
result = client.write_enhanced(
    doc_token="ABC123def",
    content="# 很长的Markdown内容..."
)

print(f"写入完成: {result['success']}, 共 {result['segments']} 段")
```

### 3. 创建并写入

```python
# 一键创建文档并写入长内容
result = client.create_and_write(
    title="分析报告",
    content="# 很长的内容...",
    folder_token="fldcnXXX"
)
```

---

## 与 data_analysis Skill 集成

### 修改 run_analysis_v5.py

找到导出到飞书文档的代码段，替换为增强版：

```python
# 原代码（可能失败）
# feishu_doc(action="write", doc_token=doc_token, content=markdown_content)

# 新代码（自动分批）
from feishu_doc_enhanced import FeishuDocEnhanced

client = FeishuDocEnhanced(batch_size=3000)
result = client.write_enhanced(
    doc_token=doc_token,
    content=markdown_content
)

if result["success"]:
    print(f"✅ 飞书文档导出成功，共 {result['segments']} 段")
else:
    print(f"❌ 导出失败: {result.get('error', 'unknown')}")
```

---

## 与 product_testing Skill 集成

### 修改 data_assembler.py

在生成报告后使用增强版写入：

```python
# 生成报告内容
report = generate_report(products)

# 使用增强版写入
from feishu_doc_enhanced import FeishuDocEnhanced

client = FeishuDocEnhanced()
result = client.create_and_write(
    title="测品分析报告",
    content=report,
    folder_token=folder_token
)
```

---

## 参数配置

### batch_size 选择建议

| 内容类型 | 建议 batch_size | 说明 |
|---------|----------------|------|
| 纯文本 | 5000 | 简单格式，可更大 |
| 含列表 | 3000 | 标准设置 |
| 含代码块 | 2000 | 复杂格式，保守设置 |
| 含表格 | 1500 | 表格解析复杂，需更小 |

### max_retries 设置

- **开发环境**: 3 次（默认）
- **生产环境**: 5 次（网络不稳定时）
- **关键报告**: 10 次（必须成功时）

---

## 错误处理

### 完整错误处理示例

```python
from feishu_doc_enhanced import FeishuDocEnhanced

client = FeishuDocEnhanced()
result = client.write_enhanced(
    doc_token="ABC123def",
    content=long_content
)

if result["success"]:
    print(f"✅ 写入成功")
    print(f"   总段数: {result['segments']}")
    print(f"   成功段: {result['success_segments']}")
    print(f"   失败段: {result['failed_segments']}")
    
    # 检查每段状态
    for status in result["segment_status"]:
        if not status["success"]:
            print(f"   ⚠️ 第 {status['index']} 段失败")
else:
    print(f"❌ 写入失败: {result.get('error', 'unknown')}")
    
    # 部分成功的情况
    if result.get("segment_status"):
        success_count = sum(1 for s in result["segment_status"] if s["success"])
        print(f"   部分成功: {success_count}/{result['segments']} 段")
```

---

## 性能优化

### 大批量写入优化

```python
# 对于超大批量内容（>10万字），建议：

client = FeishuDocEnhanced(
    batch_size=5000,  # 增大批次
    max_retries=5     # 增加重试
)

# 或者拆分为多个文档
sections = split_report_into_sections(report)
for i, section in enumerate(sections):
    client.create_and_write(
        title=f"报告-第{i+1}部分",
        content=section
    )
```

### 异步写入（高级）

```python
import asyncio

async def write_async(client, doc_token, content):
    return await asyncio.to_thread(
        client.write_enhanced,
        doc_token,
        content
    )

# 并行写入多个文档
results = await asyncio.gather(*[
    write_async(client, token, content)
    for token, content in documents
])
```

---

## 测试

### 单元测试

```python
import unittest
from feishu_doc_enhanced import FeishuDocEnhanced

class TestFeishuDocEnhanced(unittest.TestCase):
    def setUp(self):
        self.client = FeishuDocEnhanced(batch_size=100)
    
    def test_split_short_content(self):
        content = "# 标题\n\n短内容"
        segments = self.client._split_content(content, 100)
        self.assertEqual(len(segments), 1)
    
    def test_split_long_content(self):
        content = "# 标题\n\n" + "A" * 500
        segments = self.client._split_content(content, 100)
        self.assertGreater(len(segments), 1)
    
    def test_split_by_headers(self):
        content = "# H1\n\n内容\n\n## H2\n\n内容"
        segments = self.client._split_content(content, 50)
        self.assertGreaterEqual(len(segments), 1)

if __name__ == "__main__":
    unittest.main()
```

---

## 常见问题

### Q1: 分批写入后格式错乱？

A: 确保在标题处分割，避免在段落中间截断。默认按 `##` 标题分割，保持格式完整。

### Q2: 表格内容被截断？

A: 表格建议单独处理，或使用更小的 `batch_size`（1500 以下）。

### Q3: 写入速度太慢？

A: 增大 `batch_size` 到 5000，或减少 `max_retries`。

### Q4: 部分段落失败？

A: 检查返回的 `segment_status`，失败的段落可以单独重试。

---

## 更新日志

### v1.0 (2026-03-19)
- 初始版本
- 支持自动分批写入
- 支持按标题智能分割
- 支持错误重试

### 计划功能
- [ ] 支持图片自动分批上传
- [ ] 支持表格单独处理
- [ ] 支持进度回调
- [ ] 支持并发写入

---

## 贡献

欢迎提交 Issue 和 PR！

**作者**: 开发经理  
**最后更新**: 2026-03-19