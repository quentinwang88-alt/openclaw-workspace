# API 配置检查指南

## 问题诊断

当前 API 配置返回固定的 50 条记录，无论查询什么 SKU。这通常意味着：

1. **API 端点不正确** - 可能是列表 API 而不是搜索 API
2. **请求参数不正确** - 参数名称或格式与实际 API 不匹配
3. **缺少必要参数** - 可能需要额外的参数才能触发搜索

## 重新检查步骤

### 1. 在浏览器中执行搜索

1. 打开 Chrome DevTools (F12)
2. 切换到 Network 标签
3. 清空所有请求（点击 🚫 图标）
4. **在网页搜索框中输入一个具体的 SKU**（如 "SL002"）
5. 点击搜索按钮
6. 观察 Network 面板中新出现的请求

### 2. 找到正确的 API 请求

查找返回 JSON 数据的请求，特征：
- Type 列显示为 `fetch` 或 `xhr`
- Response 标签页显示 JSON 格式数据
- 数据中包含你搜索的 SKU 信息

### 3. 记录完整的请求信息

点击该请求，记录以下信息：

#### A. Request URL
```
完整的 URL，例如：
https://www.bigseller.pro/api/v1/inventory/search
或
https://www.bigseller.pro/api/v1/inventory/pageList.json
```

#### B. Request Method
```
GET 或 POST
```

#### C. Request Headers
特别注意：
- `Content-Type`
- `Authorization` 或 `Cookie`
- `Referer`
- `User-Agent`

#### D. Request Payload (最重要！)
在 Payload 标签页查看，例如：
```json
{
  "sku": "SL002",
  "pageNo": 1,
  "pageSize": 50
}
```

或者：
```json
{
  "searchType": "sku",
  "searchContent": "SL002",
  "pageNo": 1,
  "pageSize": 50
}
```

### 4. 对比当前配置

当前配置的 payload：
```json
{
  "pageNo": 1,
  "pageSize": 50,
  "searchType": "skuName",
  "searchContent": "SL002"
}
```

**检查点：**
- [ ] `searchType` 的值是否正确？（可能应该是 "sku" 而不是 "skuName"）
- [ ] `searchContent` 是否是正确的参数名？（可能应该是 "sku" 或 "skuCode"）
- [ ] 是否缺少其他必要参数？（如 `warehouseId`, `countryCode` 等）

### 5. 测试不同的配置

创建一个测试脚本来尝试不同的参数组合：

```python
# 测试不同的 searchType
search_types = ["sku", "skuName", "skuCode", "productSku"]

# 测试不同的参数名
param_names = ["sku", "skuCode", "searchContent", "keyword"]
```

## 常见问题

### Q: 为什么浏览器能搜索到，但 API 调用不行？

**可能原因：**
1. **参数名称错误** - 浏览器使用的参数名与配置不同
2. **参数格式错误** - 可能需要特定的格式（如大小写敏感）
3. **缺少隐藏参数** - 浏览器可能发送了额外的参数（如 userId, token 等）
4. **API 版本不同** - 可能有多个 API 端点，你拦截的不是搜索 API

### Q: 如何确认是否是正确的 API？

**验证方法：**
1. 在浏览器中搜索 "test123"（一个不存在的 SKU）
2. 查看 API 返回的数据
3. 如果返回空结果或错误，说明 API 是有效的
4. 如果仍然返回 50 条固定记录，说明这不是搜索 API

## 下一步

1. 按照上述步骤重新检查浏览器请求
2. 将实际的 Request Payload 发给我
3. 我会帮你更新配置文件

## 临时解决方案

如果确实无法找到正确的搜索 API，可以：
1. 使用当前的 50 条记录进行匹配（已实现）
2. 回到浏览器自动化方案（Selenium/Playwright）
3. 联系系统管理员获取 API 文档
