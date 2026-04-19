# 🔍 BigSeller API 发现 - 手动操作指南

由于需要登录凭证，我无法直接访问 BigSeller 后台。请按照以下步骤手动找到 API：

## 📋 操作步骤

### 第一步：登录并打开开发者工具

1. 使用你的浏览器访问：https://www.bigseller.pro/web/inventory/index.htm
2. 登录你的 BigSeller 账号
3. 按 **F12** 打开开发者工具
4. 切换到 **Network（网络）** 标签
5. 点击过滤器，选择 **Fetch/XHR**
6. 点击 🚫 图标清空现有请求

### 第二步：执行查询操作

1. 在库存管理页面的搜索框中输入一个测试 SKU（例如：`TH-DR-8801`）
2. 点击"查询"或"搜索"按钮
3. 观察 Network 面板中新出现的请求

### 第三步：识别正确的 API

逐个点击请求，查看 **Response（响应）** 标签页，找到返回 JSON 数据的请求。

**✅ 正确的 API 特征：**
```json
{
  "code": 0,
  "data": {
    "sku": "TH-DR-8801",
    "stock": 150,
    "available": 145,
    ...
  },
  "message": "success"
}
```

### 第四步：记录 API 信息

找到正确的 API 后，在 **Headers** 标签页记录：

#### 1. Request URL
```
例如：https://www.bigseller.pro/api/v1/inventory/query
```

#### 2. Request Method
```
POST 或 GET
```

#### 3. Request Headers（重点是鉴权信息）
```
Authorization: Bearer YOUR_TOKEN_HERE（复制你自己的完整 token）
或
Cookie: session_id=xxx; token=yyy
```

#### 4. Request Payload（如果是 POST）
在 **Payload** 标签页查看：
```json
{
  "sku_code": "TH-DR-8801",
  "country": "thailand",
  "warehouse": "default"
}
```

### 第五步：更新配置文件

将上面记录的信息填入配置文件：

```bash
cd /Users/likeu3/.openclaw/workspace/skills/inventory-query
cp config/api_config.example.json config/api_config.json
nano config/api_config.json
```

更新以下字段：

```json
{
  "api": {
    "base_url": "https://www.bigseller.pro",  // ← 从 Request URL 提取
    "method": "POST",  // ← 从 Request Method 获取
    "endpoints": {
      "query": "/api/v1/inventory/query"  // ← 从 Request URL 提取路径部分
    },
    "headers": {
      "Authorization": "Bearer YOUR_TOKEN_HERE",  // ← 从 Request Headers 复制
      "Content-Type": "application/json",
      "Referer": "https://www.bigseller.pro/web/inventory/index.htm"
    },
    "payload_template": {
      "sku_code": "{sku}",  // ← 根据实际 Payload 调整字段名
      "country": "{country}",
      "warehouse": "default"
    },
    "response_format": {
      "status_path": "code",  // ← 根据实际响应格式调整
      "success_value": 0,
      "data_path": "data",
      "fields": {
        "available": "available",  // ← 根据实际字段名调整
        "total": "total",
        "reserved": "reserved",
        "status": "status"
      }
    }
  }
}
```

### 第六步：测试连接

```bash
cd /Users/likeu3/.openclaw/workspace/skills/inventory-query
python test_api.py
```

**预期输出：**
```
============================================================
库存查询 API 连接测试
============================================================

✓ 配置文件加载成功
  API 地址: https://www.bigseller.pro
  查询端点: /api/v1/inventory/query

正在测试查询 SKU: TEST-001

查询结果:
------------------------------------------------------------
✓ 查询成功！
  SKU: TEST-001
  可用库存: 150
  ...
```

## 🎯 常见 API 模式（参考）

BigSeller 可能使用以下 API 模式之一：

### 模式1：标准 RESTful API
```
POST https://www.bigseller.pro/api/v1/inventory/query
Authorization: Bearer xxx
Body: {"sku": "xxx", "country": "thailand"}
```

### 模式2：GraphQL API
```
POST https://www.bigseller.pro/graphql
Body: {"query": "query { inventory(sku: \"xxx\") { available } }"}
```

### 模式3：内部 API
```
POST https://www.bigseller.pro/web/api/inventory/search
Cookie: session_id=xxx
Body: {"skuCode": "xxx"}
```

## 📝 完成后

配置完成并测试成功后，你就可以使用高性能的库存查询了：

```python
from skills.inventory_query.inventory_api import query_inventory

results = query_inventory(["TH-DR-8801", "TH-DR-8802"])
for sku, data in results.items():
    print(f"{sku}: {data['available']} 件")
```

## ⚠️ 注意事项

1. **Token 安全**：不要将包含真实 Token 的配置文件提交到 Git
2. **Token 过期**：Token 可能会过期，需要定期更新
3. **限流保护**：默认启用 0.5秒/次的限流，避免触发 API 限制

## 🆘 需要帮助？

如果遇到问题：
1. 检查 Request URL 是否完整正确
2. 确认 Authorization Token 是否有效
3. 验证 Payload 字段名是否匹配
4. 查看实际的响应数据结构

参考完整文档：[`docs/API_DISCOVERY_GUIDE.md`](docs/API_DISCOVERY_GUIDE.md)
