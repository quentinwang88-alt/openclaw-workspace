# Feishu Document Enhanced Skill

## 优化方案：长内容自动分批写入

### 问题描述

飞书 doc API 的 `write` 操作在内容过长时（约 >5000 字符或复杂格式）可能失败，导致文档创建后内容为空。

### 解决方案

实现智能分批写入机制：
1. **自动检测内容长度**
2. **智能分段**：按段落/章节分割，保持内容完整性
3. **分批追加**：使用 `append` 操作分批写入
4. **错误重试**：单段失败可重试，不影响其他段落

### 使用方法

#### 方式一：使用增强版 write（自动分批）

```json
{
  "action": "write_enhanced",
  "doc_token": "ABC123def",
  "content": "# 很长的Markdown内容...",
  "batch_size": 3000,
  "auto_create": true
}
```

参数说明：
- `batch_size`: 每批最大字符数（默认 3000）
- `auto_create`: 文档不存在时自动创建（默认 true）

#### 方式二：创建并写入（一键完成）

```json
{
  "action": "create_and_write",
  "title": "文档标题",
  "content": "# 很长的Markdown内容...",
  "folder_token": "fldcnXXX",
  "batch_size": 3000
}
```

### 实现逻辑

```python
def write_enhanced(doc_token, content, batch_size=3000):
    """
    智能分批写入长内容
    """
    # 1. 清理内容
    content = clean_markdown(content)
    
    # 2. 智能分段（按标题层级分割）
    segments = split_by_headers(content, batch_size)
    
    # 3. 第一批写入（清空原有内容）
    feishu_doc.write(doc_token, segments[0])
    
    # 4. 后续批次追加
    for segment in segments[1:]:
        feishu_doc.append(doc_token, segment)
    
    return {"success": True, "segments": len(segments)}
```

### 分段策略

1. **按标题分割**：优先在 ##、### 等标题处分割
2. **按段落分割**：其次在空行处分割
3. **强制截断**：超过 batch_size 时强制截断

### 错误处理

- 单段写入失败：自动重试 3 次
- 连续失败：记录错误位置，继续写入后续段落
- 完整报告：返回每段的写入状态

### 与原生 API 对比

| 特性 | 原生 write | write_enhanced |
|------|-----------|----------------|
| 内容长度限制 | ~5000 字符 | 无限制 |
| 长内容处理 | 可能失败 | 自动分批 |
| 格式保持 | 完整 | 完整 |
| 错误恢复 | 无 | 单段重试 |
| 进度反馈 | 无 | 分段状态 |

### 使用建议

1. **短内容**（<3000 字符）：使用原生 `write`
2. **长内容**（>3000 字符）：使用 `write_enhanced`
3. **不确定长度**：统一使用 `write_enhanced`，自动处理

### 示例代码

```python
# 场景：写入长报告
content = """
# 泰国女装1店分析报告

## 一、整体销售情况
...（很长内容）

## 二、高潜机会品
...（很长内容）

## 三、执行计划
...（很长内容）
"""

# 使用增强版写入
result = feishu_doc.write_enhanced(
    doc_token="ABC123def",
    content=content,
    batch_size=3000
)

print(f"写入完成，共 {result['segments']} 段")
```

### 注意事项

1. 分批写入会增加 API 调用次数
2. 极端长内容（>10万字）建议拆分为多个文档
3. 表格、图片等特殊元素建议单独处理

---

**版本**: V1.0  
**作者**: 开发经理  
**最后更新**: 2026-03-19