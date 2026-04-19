# Inventory Alert Skill

库存预警 Skill - 监控库存预计可售天数，低于阈值时自动发送飞书通知

## 功能

- 自动查询所有 SKU 的库存信息
- 检测预计可售天数低于阈值的 SKU
- 通过飞书机器人发送预警通知
- 支持定时自动运行和手动查询

## 依赖

- [`inventory-query`](../inventory-query/SKILL.md) - 库存查询 API
- `requests` - HTTP 请求库

## 配置

### 1. 配置文件

复制示例配置文件：

```bash
cp config/alert_config.example.json config/alert_config.json
```

编辑 [`config/alert_config.json`](config/alert_config.json)：

```json
{
  "threshold_days": 10,
  "feishu": {
    "enabled": true,
    "user_webhook_url": "https://open.feishu.cn/open-apis/bot/v2/hook/YOUR_USER_WEBHOOK",
    "group_webhook_url": "https://open.feishu.cn/open-apis/bot/v2/hook/YOUR_GROUP_WEBHOOK"
  }
}
```

参数说明：
- `threshold_days`: 预警阈值（天数），低于此值将触发预警
- `feishu.enabled`: 是否启用飞书通知
- `feishu.user_webhook_url`: 用户个人飞书机器人 Webhook URL
- `feishu.group_webhook_url`: 群组飞书机器人 Webhook URL

### 2. 获取飞书 Webhook URL

1. 在飞书中创建自定义机器人
2. 获取 Webhook URL
3. 将 URL 填入配置文件

详细步骤参考：[飞书机器人文档](https://open.feishu.cn/document/ukTMukTMukTM/ucTM5YjL3ETO24yNxkjN)

### 3. 设置定时任务

使用 cron 设置每天上午 8:30 自动运行：

```bash
# 编辑 crontab
crontab -e

# 添加以下行（注意修改路径）
30 8 * * * cd /Users/likeu3/.openclaw/workspace/skills/inventory-alert && /usr/bin/python3 alert.py
```

或者使用 OpenClaw 的 cron skill（如果已安装）。

## 使用方法

### 手动运行

```bash
# 检查并发送通知
python3 alert.py

# 仅检查不发送通知
python3 alert.py --no-notify
```

### 在代码中调用

```python
from skills.inventory_alert.alert import check_inventory_alerts

# 检查并发送通知
result = check_inventory_alerts()
print(f"发现 {result['alert_count']} 个预警")

# 仅检查不发送通知
result = check_inventory_alerts(send_notification=False)
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

## 返回数据结构

```python
{
    "timestamp": "2026-03-06T08:30:00",
    "alert_count": 3,
    "alerts": [
        {
            "sku": "TH-DR-8801",
            "available": 15,
            "total": 15,
            "reserved": 0,
            "status": "normal",
            "avg_daily_sales": 3.0,
            "purchase_sale_days": 5,
            "timestamp": "2026-03-06T08:30:00"
        }
    ],
    "message": "...",
    "notifications_sent": ["user", "group"]
}
```

## 注意事项

1. 确保 [`inventory-query`](../inventory-query/SKILL.md) skill 已正确配置
2. 飞书 Webhook URL 需要保密，不要提交到版本控制
3. 定时任务的路径需要根据实际情况调整
4. 建议先使用 `--no-notify` 测试功能是否正常

## 故障排查

### 问题：无法查询库存

检查 [`inventory-query`](../inventory-query/SKILL.md) 配置是否正确：

```bash
cd ../inventory-query
python3 inventory_api.py
```

### 问题：飞书通知发送失败

1. 检查 Webhook URL 是否正确
2. 检查网络连接
3. 查看飞书机器人是否被禁用

### 问题：定时任务未运行

1. 检查 crontab 是否正确设置：`crontab -l`
2. 检查 Python 路径是否正确：`which python3`
3. 查看系统日志：`grep CRON /var/log/syslog`（Linux）或 `log show --predicate 'process == "cron"'`（macOS）

## 相关文档

- [库存查询 Skill](../inventory-query/SKILL.md)
- [飞书机器人文档](https://open.feishu.cn/document/ukTMukTMukTM/ucTM5YjL3ETO24yNxkjN)
