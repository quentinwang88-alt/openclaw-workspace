# 库存查询 Skill - README

## 📦 OpenClaw Skill: 高性能库存查询

基于 API 直接调用的库存查询工具，性能比浏览器自动化快 **90-95%**。

**✨ 新功能：自动 Token 刷新** - 无需手动更新过期的 API Token！

**🗂️ 多账号支持** - 可为不同 BigSeller 店铺账号保存独立 profile，避免切号时覆盖仓库配置。

### ⚡ 性能对比

| 方案 | 10个SKU | 100个SKU | 提升 |
|------|---------|----------|------|
| Selenium | 50-80秒 | 500-800秒 | 基准 |
| Playwright | 30-50秒 | 300-500秒 | ↑40% |
| **API 直接调用** | **1-5秒** | **10-50秒** | **↑95%** |

---

## 🚨 Token 过期？自动更新！

### 方案 1：自动监听（推荐）⭐

浏览器刷新页面时自动更新 Token，无需手动操作！

```bash
# 3分钟完成设置
cd skills/inventory-query/browser-extension
python generate_icons.py  # 生成图标

# 安装浏览器扩展（见快速指南）
# 启动自动接收服务
cd ..
python token_manager.py server
```

详见 [`TOKEN_AUTO_UPDATE_QUICKSTART.md`](TOKEN_AUTO_UPDATE_QUICKSTART.md)

### 方案 2：手动刷新

如果遇到 **401006 错误**（Token 已过期），运行：

```bash
cd skills/inventory-query
python token_manager.py refresh
```

详见 [`TOKEN_QUICK_START.md`](TOKEN_QUICK_START.md)

### 方案 3：多账号 profile 管理

如果你会在多个 BigSeller 店铺账号之间切换，建议把每个账号保存成独立 profile：

```bash
cd skills/inventory-query

# 把当前账号保存为一个命名 profile
python profile_manager.py save-active 店铺A

# 基于当前配置克隆一个新 profile 模板给另一个账号（会清空旧认证）
python profile_manager.py create 店铺B --activate

# 查看当前有哪些 profile
python profile_manager.py list

# 手动切换回某个账号配置
python profile_manager.py switch 店铺A
```

推荐流程：

1. 当前正在用的账号先执行 `save-active 店铺A`
2. 新账号先执行 `create 店铺B --activate`
3. 如果店铺 B 的 `warehouseIds` / 国家配置和店铺 A 不同，先调整当前的 `config/api_config.json`
4. 切到 BigSeller 的店铺 B，刷新库存页并点击扩展“立即同步”
5. 如果你第 3 步改了配置，再执行一次 `save-active 店铺B` 把这些设置固化到 profile
6. 以后再切回已登记账号时，系统会优先按 token 中的 uid/puid 自动匹配 profile

---

## 🚀 快速开始

### 1. 安装依赖

```bash
cd skills/inventory-query
pip install -r requirements.txt
```

### 2. 找到 API 端点

按照 [`docs/API_DISCOVERY_GUIDE.md`](docs/API_DISCOVERY_GUIDE.md) 的指导：

1. 打开 Chrome DevTools（F12）
2. 切换到 Network → Fetch/XHR
3. 在网页中搜索一个 SKU
4. 找到返回 JSON 数据的 API 请求
5. 记录 Request URL、Headers 和 Payload

### 3. 配置 API

```bash
# 复制配置示例
cp config/api_config.example.json config/api_config.json

# 编辑配置文件
nano config/api_config.json
```

更新以下关键信息：
- `api.base_url`: API 基础地址
- `api.endpoints.query`: 查询端点路径
- `api.headers.Authorization`: 从浏览器复制的 Token
- `api.payload_template`: 根据实际 API 调整

### 4. 测试连接

```bash
python test_api.py
```

预期输出：
```
============================================================
库存查询 API 连接测试
============================================================

✓ 配置文件加载成功
  API 地址: https://www.bigseller.pro
  查询端点: /api/inventory/query

正在测试查询 SKU: TEST-001

查询结果:
------------------------------------------------------------
✓ 查询成功！
  SKU: TEST-001
  可用库存: 150
  总库存: 200
  预留: 50
  状态: active

============================================================
✓ API 连接测试完成
============================================================
```

---

## 📖 使用方法

### 在 Python 中使用

```python
from skills.inventory_query.inventory_api import query_inventory

# 精确查询单个 SKU
results = query_inventory(["TH-DR-8801"], fuzzy=False)
print(results["TH-DR-8801"]["available"])  # 输出: 150

# 模糊查询（默认）- 查询所有包含 "wj" 的 SKU
results = query_inventory(["wj"], fuzzy=True)
if 'matched_count' in results['wj']:
    print(f"找到 {results['wj']['matched_count']} 个匹配的 SKU")
    print(f"匹配的 SKU: {results['wj']['matched_skus']}")
    print(f"总可用库存: {results['wj']['available']}")

# 查询多个 SKU
results = query_inventory([
    "TH-DR-8801",
    "TH-DR-8802",
    "TH-DR-8803"
])

for sku, data in results.items():
    if 'error' not in data:
        print(f"{sku}: {data['available']} 件")
```

### 在 OpenClaw 中使用

```python
# 在 OpenClaw 的其他 Skill 或脚本中
from skills.inventory_query import query_inventory

# 查询库存
skus = ["TH-DR-8801", "TH-DR-8802"]
results = query_inventory(skus)

# 生成低库存预警
low_stock = []
for sku, data in results.items():
    if 'error' not in data and data['available'] < 50:
        low_stock.append(f"{sku}: 仅剩 {data['available']} 件")

if low_stock:
    print("⚠️ 低库存预警:")
    for item in low_stock:
        print(f"  - {item}")
```

---

## 📁 文件结构

```
skills/inventory-query/
├── SKILL.md                    # Skill 说明文档
├── README.md                   # 本文件
├── inventory_api.py            # API 调用核心代码
├── test_api.py                 # 测试脚本
├── requirements.txt            # Python 依赖
├── config/
│   ├── api_config.json         # API 配置（需创建）
│   └── api_config.example.json # 配置示例
└── docs/
    ├── API_DISCOVERY_GUIDE.md  # API 发现指南
    ├── API_QUICKSTART.md       # 快速开始
    └── PERFORMANCE_COMPARISON.md # 性能对比
```

---

## 🔧 配置说明

### 配置文件结构

```json
{
  "api": {
    "base_url": "https://www.bigseller.pro",
    "method": "POST",
    "endpoints": {
      "query": "/api/inventory/query"
    },
    "headers": {
      "Authorization": "Bearer YOUR_TOKEN_HERE"
    },
    "payload_template": {
      "sku_code": "{sku}",
      "country": "{country}"
    },
    "response_format": {
      "status_path": "code",
      "success_value": 0,
      "data_path": "data",
      "fields": {
        "available": "available"
      }
    }
  },
  "rate_limit": {
    "enabled": true,
    "delay_between_queries": 0.5
  }
}
```

### 关键配置项

- **base_url**: API 基础地址
- **endpoints.query**: 查询端点路径
- **headers.Authorization**: 鉴权 Token（从浏览器复制）
- **payload_template**: 请求参数模板，`{sku}` 和 `{country}` 会被自动替换
- **response_format**: 响应数据格式配置
  - `status_path`: 状态码路径（如 `code` 或 `status.code`）
  - `data_path`: 数据路径（如 `data` 或 `result.items.0`）
  - `fields`: 字段映射

---

## 🔍 故障排查

### API 返回错误（状态码: 2001）

**原因**: Cookie 已过期或无效

**解决方案**:
1. 重新登录后台系统 https://www.bigseller.pro
2. 打开 Chrome DevTools（F12）
3. 切换到 Network 标签
4. 访问库存页面，找到 `pageList.json` 请求
5. 复制 Request Headers 中的完整 Cookie
6. 更新 `config/api_config.json` 中的 Cookie 字段

**详细诊断**: 查看 [`ERROR_DIAGNOSIS.md`](ERROR_DIAGNOSIS.md)

### 401 Unauthorized

**原因**: Token 过期或无效

**解决方案**:
1. 重新登录后台系统
2. 打开 Chrome DevTools
3. 找到最新的 API 请求
4. 复制新的 Authorization header
5. 更新 `config/api_config.json`

### 404 Not Found

**原因**: API 端点地址错误

**解决方案**:
1. 检查 `base_url` 和 `endpoints.query` 配置
2. 确保路径拼接正确
3. 在浏览器 DevTools 中再次确认完整的 Request URL

### 数据解析失败

**原因**: 响应格式配置不正确

**解决方案**:
1. 在浏览器 DevTools 中查看实际的响应数据结构
2. 更新 `response_format` 配置
3. 参考 `config/api_config.example.json` 中的注释

### 模糊查询未找到匹配

**原因**: 查询字符串太短或不存在

**解决方案**:
1. 使用更具体的查询字符串
2. 检查 SKU 是否真的存在于系统中
3. 尝试精确查询：`query_inventory(["完整SKU"], fuzzy=False)`

详细故障排查见：[`docs/API_QUICKSTART.md`](docs/API_QUICKSTART.md)

---

## 📊 性能优化

### 1. 调整限流参数

如果 API 没有严格的限流限制：

```json
{
  "rate_limit": {
    "enabled": true,
    "delay_between_queries": 0.2  // 从 0.5 降低到 0.2
  }
}
```

### 2. 使用批量查询

如果 API 支持批量查询，配置批量端点：

```json
{
  "api": {
    "endpoints": {
      "query": "/api/inventory/query",
      "batch": "/api/inventory/batch"
    }
  }
}
```

---

## 🎯 与原实现对比

### 原实现（Selenium/Playwright）

位置: `/Users/likeu3/Documents/inventory-management/`

**优点**:
- 通用性强，适用于任何网站
- 可以处理复杂的 JavaScript 渲染

**缺点**:
- 速度慢（需要启动浏览器、加载页面）
- 资源占用高（内存、CPU）
- 容易受页面变化影响

### 新实现（API 直接调用）

位置: `skills/inventory-query/`

**优点**:
- ✅ 性能提升 90-95%
- ✅ 资源占用降低 95%
- ✅ 代码简洁，易于维护
- ✅ 不受页面变化影响

**缺点**:
- ⚠️ 需要找到正确的 API 端点
- ⚠️ Token 可能需要定期更新

---

## 📚 相关文档

- [SKILL.md](SKILL.md) - Skill 完整说明
- [API 发现指南](docs/API_DISCOVERY_GUIDE.md) - 如何找到后台 API
- [快速开始](docs/API_QUICKSTART.md) - 5分钟上手指南
- [性能对比](docs/PERFORMANCE_COMPARISON.md) - 详细性能测试报告

---

## 🤝 贡献

如果你发现问题或有改进建议，欢迎提交 Issue 或 Pull Request。

---

## 📝 更新日志

### v1.1.0 (2026-03-06)
- ✨ 新增模糊查询功能（`fuzzy` 参数）
- 📊 支持多结果自动汇总
- 🔍 改进错误信息，包含状态码
- 📝 新增 [`ERROR_DIAGNOSIS.md`](ERROR_DIAGNOSIS.md) 诊断文档
- 🛠️ 新增多个调试工具脚本

### v1.0.0 (2026-03-06)
- ✨ 初始版本
- ⚡ 基于 API 直接调用，性能提升 90-95%
- 🔄 支持自动重试和限流保护
- 📊 支持批量查询
- 📝 完整的文档和配置示例

---

**需要帮助？** 查看 [`docs/API_DISCOVERY_GUIDE.md`](docs/API_DISCOVERY_GUIDE.md) 或运行 `python test_api.py` 进行测试。
