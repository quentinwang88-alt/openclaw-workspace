# 库存预警 Skill

自动监控库存预计可售天数，低于阈值时通过飞书发送预警通知，并可自动创建包含详细信息的飞书文档。

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置

```bash
# 复制配置文件
cp config/alert_config.example.json config/alert_config.json

# 编辑配置
# - 设置预警阈值（默认 10 天）
# - 配置飞书 Webhook URL
```

### 3. 测试运行

```bash
# 测试查询（不发送通知）
python3 alert.py --no-notify

# 正式运行（发送通知）
python3 alert.py
```

### 4. 设置定时任务

```bash
# 每天上午 8:30 自动运行
crontab -e

# 添加以下行（注意修改路径）
30 8 * * * cd /Users/likeu3/.openclaw/workspace/skills/inventory-alert && /usr/bin/python3 alert.py
```

## 功能特性

- ✅ 自动查询所有 SKU 库存信息
- ✅ 检测预计可售天数低于阈值的 SKU
- ✅ 显示 SKU 编码、可用库存、预计可售天数、日均销量
- ✅ 通过飞书机器人发送通知（支持个人和群组）
- ✅ 自动创建飞书文档，包含详细预警信息表格
- ✅ 支持定时自动运行
- ✅ 支持手动查询

## 预警信息

### 飞书通知消息

- **SKU 编码**：产品 SKU
- **可用库存**：当前可用库存数量
- **预计可售天数**：按当前销售速度可售天数
- **日均销量**：平均每日销售数量

### 飞书文档（可选）

自动创建的飞书文档包含更详细的信息表格：

- SKU编码
- SKU名称
- SKU图片
- 当前可用库存
- 日均销量
- 预计可售天数
- 建议采购数量（日均销量 × 15天）

详细配置请参考：[飞书文档功能配置指南](FEISHU_DOC_GUIDE.md)

## 配置说明

config/alert_config.json：

```json
{
  "threshold_days": 10,
  "feishu": {
    "enabled": true,
    "user_webhook_url": "https://open.feishu.cn/open-apis/bot/v2/hook/YOUR_USER_WEBHOOK",
    "group_webhook_url": "https://open.feishu.cn/open-apis/bot/v2/hook/YOUR_GROUP_WEBHOOK",
    "app_id": "YOUR_APP_ID",
    "app_secret": "YOUR_APP_SECRET",
    "create_doc": true,
    "doc_folder_token": ""
  }
}
```

**飞书文档功能**（可选）：
- `app_id`: 飞书应用 ID
- `app_secret`: 飞书应用密钥
- `create_doc`: 是否创建飞书文档
- `doc_folder_token`: 文档存放文件夹（可选）

详细配置请参考：[飞书文档功能配置指南](FEISHU_DOC_GUIDE.md)

## 使用示例

### 命令行使用

```bash
# 检查并发送通知
python3 alert.py

# 仅检查不发送通知
python3 alert.py --no-notify
```

### Python 代码调用

```python
from skills.inventory_alert.alert import check_inventory_alerts

# 检查并发送通知
result = check_inventory_alerts()
print(f"发现 {result['alert_count']} 个预警")

# 查看预警详情
for alert in result['alerts']:
    print(f"{alert['sku']}: {alert['purchase_sale_days']} 天")
```

## 输出示例

```
⚠️ 库存预警通知

发现 3 个 SKU 预计可售天数低于 10 天：

📦 SKU: TH-DR-8801
   可用库存: 15
   预计可售: 5 天
   日均销量: 3.00

📦 SKU: bu0020
   可用库存: 28
   预计可售: 7 天
   日均销量: 4.00

📦 SKU: wj001
   可用库存: 45
   预计可售: 9 天
   日均销量: 5.00

⏰ 检查时间: 2026-03-06 08:30:00
```

## 依赖

- inventory-query - 库存查询 API
- requests - HTTP 请求库

## 文档

- [SKILL.md](SKILL.md) - 详细技术文档
- [inventory-query](../inventory-query/README.md) - 库存查询 Skill

## 故障排查

### 无法查询库存

确保 inventory-query 已正确配置：

```bash
cd ../inventory-query
python3 inventory_api.py
```

### 飞书通知失败

1. 检查 Webhook URL 是否正确
2. 检查网络连接
3. 确认机器人未被禁用

### 定时任务未运行

```bash
# 检查 crontab
crontab -l

# 检查 Python 路径
which python3

# 手动测试
cd /Users/likeu3/.openclaw/workspace/skills/inventory-alert
python3 alert.py --no-notify
```

## 注意事项

1. 首次使用建议先用 --no-notify 测试
2. 飞书 Webhook URL 需要保密
3. 定时任务路径需根据实际情况调整
4. 确保 inventory-query 配置正确

## License

MIT
