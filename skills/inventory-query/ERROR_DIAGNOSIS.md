# 库存查询 API 错误诊断报告

## 问题描述

查询 SKU "wj" 时，API 返回错误：
```json
{
  "sku": "wj",
  "error": "API 返回错误（状态码: 2001）",
  "available": 0,
  "timestamp": "2026-03-06T17:49:03.805240"
}
```

## 根本原因

API 返回状态码 `2001`，这表示 **认证失败**。

通过测试发现：
- 所有 SKU 查询都返回相同的 2001 错误
- 包括配置文件中的测试 SKU "BU0010" 也失败
- API 响应中 `data` 字段为 `null`

这说明问题不是 SKU 不存在，而是 **Cookie 已过期或无效**。

## 解决方案

### 1. 更新 Cookie（必需）

Cookie 是 API 认证的关键，需要从浏览器获取最新的 Cookie：

**步骤：**

1. 打开浏览器，访问 https://www.bigseller.pro
2. 登录账号
3. 打开开发者工具（按 F12）
4. 切换到 **Network** 标签
5. 访问库存页面（Inventory）
6. 在 Network 中找到 `pageList.json` 请求
7. 点击该请求，查看 **Request Headers**
8. 复制完整的 `Cookie` 值
9. 更新 [`config/api_config.json`](config/api_config.json:16) 中的 Cookie 字段

**配置文件位置：**
```
skills/inventory-query/config/api_config.json
```

**需要更新的字段：**
```json
{
  "api": {
    "headers": {
      "Cookie": "这里粘贴从浏览器复制的完整 Cookie"
    }
  }
}
```

### 2. 验证修复

更新 Cookie 后，运行测试脚本验证：

```bash
cd skills/inventory-query
python3 test_fuzzy_query.py
```

如果成功，应该看到类似输出：
```
✓ 模糊匹配成功！
  找到 X 个匹配的 SKU:
    1. WJ-XXX-001
    2. WJ-XXX-002
  
  总库存统计:
    可用库存: XXX
    总库存: XXX
    预留: XXX
```

## 已实现的改进

为了更好地支持模糊查询，我已经对代码进行了以下改进：

### 1. 模糊匹配功能

- 新增 `fuzzy` 参数（默认 `True`）
- 模糊匹配：查询 "wj" 会返回所有包含 "wj" 的 SKU
- 精确匹配：设置 `fuzzy=False` 只返回完全匹配的 SKU

### 2. 多结果汇总

当模糊查询匹配到多个 SKU 时，自动汇总：
- `available`: 所有匹配 SKU 的可用库存总和
- `total`: 所有匹配 SKU 的总库存总和
- `reserved`: 所有匹配 SKU 的预留库存总和
- `matched_count`: 匹配到的 SKU 数量
- `matched_skus`: 匹配到的所有 SKU 列表

### 3. 更清晰的错误信息

- 错误信息现在包含状态码：`API 返回错误（状态码: 2001）`
- 更容易诊断问题类型

## 使用示例

### 模糊查询（默认）
```python
from inventory_api import query_inventory

# 查询所有包含 "wj" 的 SKU
results = query_inventory(["wj"], fuzzy=True)

if 'matched_count' in results['wj']:
    print(f"找到 {results['wj']['matched_count']} 个匹配")
    print(f"匹配的 SKU: {results['wj']['matched_skus']}")
    print(f"总可用库存: {results['wj']['available']}")
```

### 精确查询
```python
# 只查询完全匹配 "WJ-001" 的 SKU
results = query_inventory(["WJ-001"], fuzzy=False)
print(f"可用库存: {results['WJ-001']['available']}")
```

## 诊断工具

已创建以下诊断脚本：

1. **[`debug_wj.py`](debug_wj.py)** - 调试特定 SKU 的 API 响应
2. **[`compare_skus.py`](compare_skus.py)** - 对比多个 SKU 的查询结果
3. **[`diagnose_auth`](diagnose_auth)** - 诊断 API 认证问题
4. **[`test_fuzzy_query.py`](test_fuzzy_query.py)** - 测试模糊查询功能

## 下一步

1. **立即行动**：更新 Cookie（按照上面的步骤）
2. **验证**：运行 `test_fuzzy_query.py` 确认修复
3. **正常使用**：Cookie 更新后，所有查询功能将正常工作

## 注意事项

- Cookie 会定期过期，如果再次出现 2001 错误，需要重新获取
- 建议定期（如每周）更新一次 Cookie
- 不要在公共场合分享 Cookie，它包含你的登录凭证
