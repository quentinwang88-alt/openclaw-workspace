# 🔍 API 发现指南 - 找到后台的隐藏 API

## 第一步：使用 Chrome DevTools 找到 API

### 1.1 打开开发者工具

1. 使用 Chrome 浏览器登录后台管理系统
2. 进入"库存查询"或"商品管理"页面
3. 按 **F12** 打开开发者工具（或右键 → 检查）
4. 切换到 **Network（网络）** 面板
5. 点击过滤器，选择 **Fetch/XHR**

### 1.2 执行查询操作

1. 在网页的搜索框输入测试 SKU（例如：`TH-DR-8801`）
2. 点击"查询"或"搜索"按钮
3. 观察 Network 面板中新出现的请求

### 1.3 识别正确的 API

逐个点击请求，查看 **Response（响应）** 标签页：

**✅ 正确的 API 特征：**
```json
{
  "code": 0,
  "data": {
    "sku": "TH-DR-8801",
    "stock": 150,
    "available": 145,
    "reserved": 5,
    "status": "active"
  },
  "message": "success"
}
```

**❌ 错误的请求（忽略）：**
- HTML 页面内容
- CSS/JS 文件
- 图片资源
- 空响应

---

## 第二步：提取请求三要素

找到正确的 API 后，记录以下信息：

### 2.1 Request URL（接口地址）

在 **Headers** 标签页找到：

```
Request URL: https://api.your-backend.com/v1/inventory/search
```

**常见 API 路径模式：**
- `/api/inventory/query`
- `/api/v1/products/search`
- `/web/inventory/getStock`
- `/api/sku/detail`

### 2.2 Request Headers（请求头）

在 **Headers** 标签页的 **Request Headers** 部分，重点关注：

#### 鉴权信息（必需）

**方式1：Bearer Token**
```
Authorization: Bearer YOUR_TOKEN_HERE
```

**方式2：Cookie**
```
Cookie: session_id=abc123xyz; user_token=def456uvw
```

**方式3：自定义 Token**
```
X-Auth-Token: your-token-here
X-API-Key: your-api-key
```

#### 其他常见 Headers
```
Content-Type: application/json
User-Agent: Mozilla/5.0 ...
Referer: https://www.bigseller.pro/web/inventory/index.htm
```

### 2.3 Request Payload（请求参数）

在 **Payload** 或 **Request** 标签页查看：

**方式1：JSON Body（POST）**
```json
{
  "sku_code": "TH-DR-8801",
  "warehouse": "SG_Main",
  "country": "thailand"
}
```

**方式2：Query Parameters（GET）**
```
?sku=TH-DR-8801&warehouse=SG_Main&country=thailand
```

**方式3：Form Data（POST）**
```
sku_code=TH-DR-8801&warehouse=SG_Main
```

---

## 第三步：测试 API 调用

### 3.1 使用 curl 测试

```bash
# GET 请求
curl -X GET "https://api.your-backend.com/v1/inventory/search?sku=TH-DR-8801" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json"

# POST 请求
curl -X POST "https://api.your-backend.com/v1/inventory/search" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sku_code": "TH-DR-8801", "warehouse": "SG_Main"}'
```

### 3.2 使用 Python requests 测试

```python
import requests

# 配置
url = "https://api.your-backend.com/v1/inventory/search"
headers = {
    "Authorization": "Bearer YOUR_TOKEN",
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 ..."
}
payload = {
    "sku_code": "TH-DR-8801",
    "warehouse": "SG_Main"
}

# 发送请求
response = requests.post(url, json=payload, headers=headers)
print(response.status_code)
print(response.json())
```

---

## 📝 实际案例：BigSeller 平台

### 示例 API 结构（需要实际验证）

```python
# 可能的 API 端点
url = "https://www.bigseller.pro/api/inventory/query"

# 可能的请求格式
headers = {
    "Authorization": "Bearer <从浏览器复制>",
    "Content-Type": "application/json",
    "Referer": "https://www.bigseller.pro/web/inventory/index.htm"
}

payload = {
    "sku": "TH-DR-8801",
    "country": "thailand",
    "warehouse_id": "default"
}

response = requests.post(url, json=payload, headers=headers)
```

---

## 🔧 常见问题

### Q1: 找不到 API 请求？

**解决方案：**
1. 清空 Network 面板（点击 🚫 图标）
2. 重新执行查询操作
3. 确保选择了 **Fetch/XHR** 过滤器
4. 检查是否有 **WebSocket** 连接（切换到 WS 标签）

### Q2: API 返回 401 Unauthorized？

**原因：** Token 过期或缺失

**解决方案：**
1. 重新登录后台系统
2. 从浏览器 DevTools 复制最新的 Token
3. 检查 Cookie 是否包含在请求中

### Q3: API 返回 403 Forbidden？

**原因：** 缺少必要的 Headers

**解决方案：**
1. 复制浏览器中的完整 Headers
2. 特别注意 `Referer`、`Origin`、`User-Agent`
3. 检查是否需要 CSRF Token

### Q4: 如何处理动态 Token？

**方案1：定期更新**
```python
def get_fresh_token():
    # 从登录接口获取新 Token
    login_response = requests.post(
        "https://api.your-backend.com/auth/login",
        json={"username": "xxx", "password": "xxx"}
    )
    return login_response.json()["token"]
```

**方案2：使用 Session**
```python
session = requests.Session()
# 先登录
session.post("https://api.your-backend.com/auth/login", ...)
# 后续请求自动携带 Cookie
session.post("https://api.your-backend.com/inventory/query", ...)
```

---

## ✅ 验证清单

在实施 API 调用前，确保：

- [ ] 找到了返回 JSON 数据的 API 端点
- [ ] 记录了完整的 Request URL
- [ ] 复制了所有必要的 Headers（特别是鉴权信息）
- [ ] 理解了 Request Payload 的格式
- [ ] 使用 curl 或 Python 成功测试了 API
- [ ] API 返回的数据包含所需的库存信息
- [ ] 考虑了 Token 过期和刷新机制

---

## 🚀 下一步

完成 API 发现后，使用 [`inventory_manager_api.py`](inventory_manager_api.py) 实现高性能查询。

**预期性能提升：**
- 单次查询：从 5-8秒 → **0.1-0.5秒**
- 10个SKU：从 50-80秒 → **1-5秒**
- 100个SKU：从 500-800秒 → **10-50秒**

**提升幅度：90-95% ⚡**
