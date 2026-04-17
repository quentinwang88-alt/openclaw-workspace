---
name: inventory-query
description: |
  高性能库存查询工具。支持通过 API 直接调用查询 SKU 库存信息，性能比浏览器自动化快 90-95%。
  适用于需要快速查询大量 SKU 库存的场景。
---

# 库存查询 Skill

基于 API 直接调用的高性能库存查询工具，毫秒级响应。

## 功能

- ⚡ 极速查询：单个 SKU 0.1-0.5秒，10个 SKU 1-5秒
- 📊 批量支持：支持批量查询多个 SKU
- 🔄 自动重试：内置重试机制和限流保护
- 🌍 多国家：支持多国家/地区库存查询
- 💾 结果保存：自动保存查询结果为 JSON

## 使用方法

### 查询单个 SKU

```python
from skills.inventory_query import query_inventory

result = query_inventory(["TH-DR-8801"])
# 返回: {"TH-DR-8801": {"sku": "TH-DR-8801", "available": 150, ...}}
```

### 查询多个 SKU

```python
results = query_inventory([
    "TH-DR-8801",
    "TH-DR-8802",
    "TH-DR-8803"
])
```

### 指定国家

```python
results = query_inventory(["TH-DR-8801"], country="thailand")
```

## 配置

### 第一步：找到 API 端点

1. 打开 Chrome DevTools（F12）
2. 切换到 Network → Fetch/XHR
3. 在网页中搜索一个 SKU
4. 找到返回 JSON 数据的 API 请求
5. 记录 Request URL、Headers 和 Payload

详细步骤见：[`docs/API_DISCOVERY_GUIDE.md`](docs/API_DISCOVERY_GUIDE.md)

### 第二步：配置 API 信息

编辑 [`config/api_config.json`](config/api_config.json)：

```json
{
  "api": {
    "base_url": "https://www.bigseller.pro",
    "endpoints": {
      "query": "/api/inventory/query"
    },
    "headers": {
      "Authorization": "Bearer YOUR_TOKEN_HERE"
    },
    "payload_template": {
      "sku_code": "{sku}",
      "country": "{country}"
    }
  }
}
```

### 第三步：测试连接

```bash
cd skills/inventory-query
python test_api.py

# 或者直接指定 SKU 进行测试
python test_api.py wj MWJ-BP bu0020

# 使用 inventory_api.py 直接查询
python inventory_api.py wj MWJ-BP
```

## 性能对比

| 方案 | 10个SKU | 100个SKU | 提升幅度 |
|------|---------|----------|---------|
| Selenium | 50-80秒 | 500-800秒 | 基准 |
| Playwright | 30-50秒 | 300-500秒 | ↑ 40% |
| **API 直接调用** | **1-5秒** | **10-50秒** | **↑ 95%** |

## 文件结构

```
skills/inventory-query/
├── SKILL.md                    # 本文件
├── inventory_api.py            # API 调用核心代码
├── test_api.py                 # 测试脚本
├── config/
│   ├── api_config.json         # API 配置（需创建）
│   └── api_config.example.json # 配置示例
└── docs/
    ├── API_DISCOVERY_GUIDE.md  # API 发现指南
    ├── API_QUICKSTART.md       # 快速开始
    └── PERFORMANCE_COMPARISON.md # 性能对比
```

## 依赖

```bash
pip install requests
```

## 注意事项

1. **Token 管理**：API Token 可能会过期，需要定期更新
2. **限流保护**：默认启用限流保护（0.5秒/次），可在配置中调整
3. **错误处理**：内置重试机制，自动处理临时性错误
4. **数据准确性**：首次使用时建议对比浏览器查询结果验证准确性
5. **前台名称回退查询**：如果按实际库存编码 `sku` 查不到，系统会自动再按前台展示名称 `title` 查询一次。例如前台显示 `F-002`，实际库存编码可能是 `FSL001`

## 故障排查

### 401 Unauthorized
- Token 过期，需要重新获取
- 从浏览器 DevTools 复制最新的 Authorization header

### 404 Not Found
- API 端点地址错误
- 检查 `base_url` 和 `endpoints.query` 配置

### 数据解析失败
- 响应格式配置不正确
- 查看实际响应数据，调整 `response_format` 配置

详细故障排查见：[`docs/API_QUICKSTART.md`](docs/API_QUICKSTART.md)

## 示例

### 在 OpenClaw 中使用

```python
# 查询库存并生成报告
from skills.inventory_query import query_inventory

skus = ["TH-DR-8801", "TH-DR-8802", "TH-DR-8803"]
results = query_inventory(skus)

# 生成库存报告
low_stock = []
for sku, data in results.items():
    if 'error' not in data and data['available'] < 50:
        low_stock.append(f"{sku}: 仅剩 {data['available']} 件")

if low_stock:
    print("⚠️ 低库存预警:")
    for item in low_stock:
        print(f"  - {item}")
```

### 集成到自动化流程

```python
# 定时检查库存
import schedule
from skills.inventory_query import query_inventory

def check_inventory():
    critical_skus = ["TH-DR-8801", "TH-DR-8802"]
    results = query_inventory(critical_skus)
    
    for sku, data in results.items():
        if data.get('available', 0) < 20:
            # 发送预警通知
            send_alert(f"SKU {sku} 库存不足: {data['available']}")

# 每小时检查一次
schedule.every().hour.do(check_inventory)
```

## 更新日志

### v1.0.0 (2026-03-06)
- ✨ 初始版本
- ⚡ 基于 API 直接调用，性能提升 90-95%
- 🔄 支持自动重试和限流保护
- 📊 支持批量查询
- 📝 完整的文档和配置示例

## 相关资源

- [API 发现指南](docs/API_DISCOVERY_GUIDE.md) - 如何找到后台 API
- [快速开始](docs/API_QUICKSTART.md) - 5分钟上手指南
- [性能对比](docs/PERFORMANCE_COMPARISON.md) - 详细性能测试报告
- [原始实现](/Users/likeu3/Documents/inventory-management/) - Selenium/Playwright 版本（参考）
