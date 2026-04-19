# Feishu 文档操作使用指南

**版本**: V1.0  
**创建日期**: 2026-03-19  
**作者**: 开发经理

---

## ⚠️ 重要发现

### `create` 操作的 `content` 参数无效

**问题描述：**
飞书 doc API 的 `create` 操作在传入 `content` 参数时，**内容会被忽略**，只创建空文档。

**错误用法（内容会丢失）：**
```json
{
  "action": "create",
  "title": "My Document",
  "content": "# Title\n\nThis content will be LOST!"
}
```

**正确用法（两步法）：**
```json
// 步骤1: 创建空文档
{
  "action": "create",
  "title": "My Document"
}
// → 返回 doc_token: "ABC123def"

// 步骤2: 追加内容
{
  "action": "append",
  "doc_token": "ABC123def",
  "content": "# Title\n\nThis content will be SAVED!"
}
```

---

## 完整操作流程

### 创建并写入长文档

```python
import json

# 1. 创建空文档
create_result = feishu_doc(
    action="create",
    title="分析报告",
    folder_token="fldcnXXX"  # 可选
)
doc_token = create_result["document_id"]

# 2. 追加内容（支持长内容）
feishu_doc(
    action="append",
    doc_token=doc_token,
    content="# 标题\n\n很长的 Markdown 内容..."
)

# 3. 获取文档 URL
doc_url = f"https://xxx.feishu.cn/docx/{doc_token}"
```

---

## 各操作说明

### Read Document
```json
{ "action": "read", "doc_token": "ABC123def" }
```
返回：标题、纯文本内容、块统计

### Write Document (Replace All)
```json
{ "action": "write", "doc_token": "ABC123def", "content": "# Title\n\nContent..." }
```
替换整个文档内容。支持：标题、列表、代码块、引用、链接、图片、粗体/斜体/删除线。

**限制：** Markdown 表格不支持。

### Append Content
```json
{ "action": "append", "doc_token": "ABC123def", "content": "Additional content" }
```
在文档末尾追加内容。

### Create Document
```json
{ "action": "create", "title": "New Document" }
```
创建空文档。**注意：`content` 参数会被忽略！**

---

## 常见问题

### Q1: 为什么我的文档创建后内容为空？

**A:** 因为你使用了 `create` 的 `content` 参数。正确做法是：
1. 先用 `create` 创建空文档
2. 再用 `append` 写入内容

### Q2: 如何写入长内容（>5000字符）？

**A:** 使用 `append` 分批写入：
```python
# 分批追加
for segment in content_segments:
    feishu_doc(action="append", doc_token=doc_token, content=segment)
```

### Q3: `write` 和 `append` 有什么区别？

| 操作 | 作用 | 使用场景 |
|------|------|---------|
| `write` | 替换整个文档 | 更新已有文档 |
| `append` | 在末尾追加 | 新建文档写入内容 |

### Q4: 如何创建带内容的文档（一步完成）？

**A:** 目前 API 不支持。必须分两步：
1. `create` 创建空文档
2. `append` 写入内容

---

## 最佳实践

### 1. 创建新文档并写入内容

```python
# 步骤1: 创建
doc = feishu_doc(action="create", title="标题")
token = doc["document_id"]

# 步骤2: 写入
feishu_doc(action="append", doc_token=token, content=content)
```

### 2. 更新已有文档

```python
# 直接替换全部内容
feishu_doc(action="write", doc_token=token, content=new_content)
```

### 3. 在现有文档后追加

```python
# 在末尾追加
feishu_doc(action="append", doc_token=token, content=additional_content)
```

---

## 错误示例 vs 正确示例

### ❌ 错误：内容会丢失

```json
{
  "action": "create",
  "title": "报告",
  "content": "# 标题\n\n很长的内容..."
}
```

结果：创建了空文档，内容丢失。

### ✅ 正确：内容成功写入

```json
// 第一步
{
  "action": "create",
  "title": "报告"
}
// → 返回 doc_token

// 第二步
{
  "action": "append",
  "doc_token": "ABC123def",
  "content": "# 标题\n\n很长的内容..."
}
```

结果：文档创建成功，内容完整写入。

---

## 更新日志

### V1.0 (2026-03-19)
- 初始版本
- 记录 `create` 操作 `content` 参数无效的问题
- 提供正确的两步法操作流程

---

**注意：** 本指南基于实际测试发现，建议在使用 feishu_doc 工具时遵循上述最佳实践。