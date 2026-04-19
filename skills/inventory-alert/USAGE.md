# 库存预警使用指南

## 快速配置

### 步骤 1：配置飞书 Webhook

1. 在飞书中创建自定义机器人：
   - 打开飞书群聊
   - 点击右上角 "..." → "设置" → "群机器人"
   - 点击 "添加机器人" → "自定义机器人"
   - 设置机器人名称和描述
   - 复制 Webhook URL

2. 配置文件：

```bash
cd skills/inventory-alert
cp config/alert_config.example.json config/alert_config.json
```

编辑 [`config/alert_config.json`](config/alert_config.json)：

```json
{
  "threshold_days": 10,
  "feishu": {
    "enabled": true,
    "user_webhook_url": "https://open.feishu.cn/open-apis/bot/v2/hook/你的个人机器人webhook",
    "group_webhook_url": "https://open.feishu.cn/open-apis/bot/v2/hook/你的群组机器人webhook"
  }
}
```

### 步骤 2：测试运行

```bash
# 测试查询（不发送通知）
python3 alert.py --no-notify

# 正式运行（发送通知）
python3 alert.py
```

### 步骤 3：设置定时任务

#### 方法 1：使用 crontab（推荐）

```bash
# 编辑 crontab
crontab -e

# 添加以下行（每天上午 8:30 运行）
30 8 * * * cd /Users/likeu3/.openclaw/workspace/skills/inventory-alert && /usr/bin/python3 alert.py

# 保存并退出（按 ESC，输入 :wq，回车）
```

#### 方法 2：使用 OpenClaw cron skill

如果你安装了 OpenClaw 的 cron skill，可以这样配置：

```json
{
  "schedule": "30 8 * * *",
  "command": "cd /Users/likeu3/.openclaw/workspace/skills/inventory-alert && python3 alert.py",
  "description": "库存预警检查"
}
```

## 使用场景

### 场景 1：每日自动检查

设置定时任务后，系统会每天上午 8:30 自动检查库存，如果发现预计可售天数低于 10 天的 SKU，会自动发送飞书通知。

### 场景 2：手动查询

当你想立即查看库存预警情况时：

```bash
cd skills/inventory-alert
python3 alert.py --no-notify
```

### 场景 3：在代码中集成

```python
from skills.inventory_alert.alert import check_inventory_alerts

# 检查库存预警
result = check_inventory_alerts(send_notification=False)

# 处理预警
if result['alert_count'] > 0:
    print(f"⚠️ 发现 {result['alert_count']} 个库存预警")
    for alert in result['alerts']:
        sku = alert['sku']
        days = alert['purchase_sale_days']
        available = alert['available']
        print(f"  {sku}: 剩余 {days} 天，库存 {available}")
```

## 自定义配置

### 调整预警阈值

如果你想在库存剩余 15 天时就收到预警：

```json
{
  "threshold_days": 15,
  ...
}
```

### 只发送给个人

如果你不想发送到群组，只保留个人 webhook：

```json
{
  "threshold_days": 10,
  "feishu": {
    "enabled": true,
    "user_webhook_url": "https://open.feishu.cn/open-apis/bot/v2/hook/你的webhook",
    "group_webhook_url": ""
  }
}
```

### 只发送到群组

如果你只想发送到群组：

```json
{
  "threshold_days": 10,
  "feishu": {
    "enabled": true,
    "user_webhook_url": "",
    "group_webhook_url": "https://open.feishu.cn/open-apis/bot/v2/hook/群组webhook"
  }
}
```

## 预警消息示例

当检测到库存预警时，你会收到类似这样的飞书消息：

```
⚠️ 库存预警通知

发现 3 个 SKU 预计可售天数低于 10 天：

📦 SKU: bgwj003
   可用库存: 9
   预计可售: 1 天
   日均销量: 4.90

📦 SKU: SL002
   可用库存: 3
   预计可售: 1 天
   日均销量: 1.87

📦 SKU: RWJ005
   可用库存: 12
   预计可售: 2 天
   日均销量: 4.90

⏰ 检查时间: 2026-03-06 08:30:00
```

## 常见问题

### Q: 如何修改定时任务的运行时间？

A: 编辑 crontab：

```bash
crontab -e
```

修改时间格式（分 时 日 月 周）：

```
# 每天上午 9:00
0 9 * * *

# 每天上午 8:30 和下午 6:00
30 8,18 * * *

# 每周一上午 8:30
30 8 * * 1
```

### Q: 如何查看定时任务是否正常运行？

A: 查看 crontab 日志：

```bash
# macOS
log show --predicate 'process == "cron"' --last 1d

# Linux
grep CRON /var/log/syslog
```

### Q: 如何临时禁用通知？

A: 方法 1 - 修改配置文件：

```json
{
  "threshold_days": 10,
  "feishu": {
    "enabled": false,
    ...
  }
}
```

方法 2 - 修改 crontab，添加 `--no-notify` 参数：

```
30 8 * * * cd /Users/likeu3/.openclaw/workspace/skills/inventory-alert && /usr/bin/python3 alert.py --no-notify
```

### Q: 如何只查看特定 SKU 的预警？

A: 目前不支持过滤特定 SKU，但你可以修改 [`alert.py`](alert.py) 中的 [`check_alerts()`](alert.py:195) 方法添加过滤逻辑。

## 故障排查

### 问题：收不到飞书通知

1. 检查 Webhook URL 是否正确
2. 测试 Webhook：

```bash
curl -X POST "你的webhook_url" \
  -H "Content-Type: application/json" \
  -d '{"msg_type":"text","content":{"text":"测试消息"}}'
```

3. 检查机器人是否被禁用
4. 查看程序输出是否有错误信息

### 问题：定时任务没有运行

1. 检查 crontab 是否正确设置：

```bash
crontab -l
```

2. 检查 Python 路径：

```bash
which python3
```

3. 手动运行测试：

```bash
cd /Users/likeu3/.openclaw/workspace/skills/inventory-alert
python3 alert.py --no-notify
```

### 问题：查询库存失败

确保 [`inventory-query`](../inventory-query/README.md) skill 配置正确：

```bash
cd ../inventory-query
python3 inventory_api.py
```

## 进阶使用

### 集成到其他系统

你可以通过 HTTP API 或命令行调用库存预警：

```bash
# 获取 JSON 格式的预警数据
python3 -c "
from alert import check_inventory_alerts
import json
result = check_inventory_alerts(send_notification=False)
print(json.dumps(result, indent=2, ensure_ascii=False))
"
```

### 自定义通知格式

修改 [`alert.py`](alert.py) 中的 [`format_alert_message()`](alert.py:220) 方法来自定义通知消息格式。

### 添加其他通知渠道

在 [`InventoryAlert`](alert.py:180) 类中添加新的通知方法，例如：

- 企业微信
- 钉钉
- 邮件
- Slack
- Telegram

## 相关文档

- [README.md](README.md) - 项目概述
- [SKILL.md](SKILL.md) - 技术文档
- [inventory-query](../inventory-query/README.md) - 库存查询 Skill
