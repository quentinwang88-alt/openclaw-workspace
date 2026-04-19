# 库存预警 Skill - 项目总结

## 项目概述

基于 [`inventory-query`](../inventory-query/README.md) skill 实现的库存预警系统，自动监控所有 SKU 的预计可售天数，当低于设定阈值时通过飞书机器人发送预警通知。

## 已实现功能

✅ **核心功能**
- 自动查询所有 SKU 的库存信息
- 提取预计可售天数（`purchaseSaleDays`）和日均销量（`avgDailySales`）
- 检测预计可售天数低于阈值的 SKU
- 按预计可售天数排序，优先显示最紧急的

✅ **通知功能**
- 通过飞书机器人发送预警通知
- 支持同时发送给个人和群组
- 可配置是否启用通知
- 格式化的预警消息，包含 SKU、库存、天数、销量

✅ **定时任务**
- 支持 cron 定时任务（每天上午 8:30）
- 提供 crontab 配置示例
- 可手动运行查询

✅ **灵活配置**
- 可配置预警阈值（默认 10 天）
- 可配置飞书 Webhook URL
- 支持启用/禁用通知
- 提供配置示例文件

## 文件结构

```
skills/inventory-alert/
├── alert.py                          # 核心实现
├── requirements.txt                  # Python 依赖
├── setup.sh                          # 快速设置脚本
├── crontab.example                   # crontab 配置示例
├── README.md                         # 项目说明
├── SKILL.md                          # 技术文档
├── USAGE.md                          # 使用指南
└── config/
    └── alert_config.example.json     # 配置示例
```

## 核心实现

### 1. 扩展库存查询 API

[`InventoryAlertAPI`](alert.py:28) 类扩展了 [`InventoryAPI`](../inventory-query/inventory_api.py:19)，增加了以下功能：

- 提取 `avgDailySales`（日均销量）
- 提取 `purchaseSaleDays`（预计可售天数）
- 实现 [`query_all_skus()`](alert.py:155) 方法查询所有 SKU

### 2. 预警检查

[`InventoryAlert`](alert.py:180) 类实现预警逻辑：

- [`check_alerts()`](alert.py:195)：检查所有 SKU，筛选低于阈值的
- [`format_alert_message()`](alert.py:220)：格式化预警消息
- [`send_feishu_notification()`](alert.py:254)：发送飞书通知
- [`run()`](alert.py:277)：主运行流程

### 3. 命令行接口

支持命令行参数：
- 无参数：检查并发送通知
- `--no-notify`：仅检查不发送通知

### 4. Python API

提供 [`check_inventory_alerts()`](alert.py:302) 函数供其他代码调用。

## 使用方法

### 快速开始

```bash
# 1. 配置
cd skills/inventory-alert
cp config/alert_config.example.json config/alert_config.json
# 编辑 config/alert_config.json，填入飞书 Webhook URL

# 2. 测试
python3 alert.py --no-notify

# 3. 设置定时任务
crontab -e
# 添加：30 8 * * * cd /Users/likeu3/.openclaw/workspace/skills/inventory-alert && /usr/bin/python3 alert.py
```

### 手动查询

```bash
# 查询但不发送通知
python3 alert.py --no-notify

# 查询并发送通知
python3 alert.py
```

### 代码调用

```python
from skills.inventory_alert.alert import check_inventory_alerts

result = check_inventory_alerts(send_notification=False)
print(f"发现 {result['alert_count']} 个预警")
```

## 测试结果

测试运行成功，发现 22 个 SKU 预计可售天数低于 10 天：

```
⚠️ 库存预警通知

发现 22 个 SKU 预计可售天数低于 10 天：

📦 SKU: bgwj003
   可用库存: 9
   预计可售: 1 天
   日均销量: 4.90

📦 SKU: SL002
   可用库存: 3
   预计可售: 1 天
   日均销量: 1.87

...（更多预警）

⏰ 检查时间: 2026-03-06 20:43:47
```

## 配置说明

[`config/alert_config.json`](config/alert_config.example.json)：

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

## 依赖关系

```
inventory-alert
    └── inventory-query
            └── requests
```

## 技术特点

1. **复用现有代码**：继承 [`InventoryAPI`](../inventory-query/inventory_api.py:19)，避免重复实现
2. **灵活配置**：支持多种配置选项
3. **容错处理**：API 调用失败时继续处理其他 SKU
4. **排序优化**：按预计可售天数排序，优先显示紧急情况
5. **多渠道通知**：支持同时发送给个人和群组

## 后续优化建议

1. **通知渠道**：支持更多通知方式（企业微信、钉钉、邮件等）
2. **过滤功能**：支持按 SKU、分类等过滤
3. **历史记录**：记录每次检查结果，便于分析趋势
4. **智能阈值**：根据历史数据动态调整预警阈值
5. **批量操作**：支持批量补货建议
6. **可视化**：生成图表展示库存趋势

## 相关文档

- [README.md](README.md) - 项目说明
- [SKILL.md](SKILL.md) - 技术文档
- [USAGE.md](USAGE.md) - 使用指南
- [inventory-query](../inventory-query/README.md) - 库存查询 Skill

## 维护说明

### 更新配置

修改 [`config/alert_config.json`](config/alert_config.json) 后无需重启，下次运行时自动生效。

### 更新代码

修改 [`alert.py`](alert.py) 后，定时任务会在下次运行时使用新代码。

### 日志查看

程序输出会显示在终端或 cron 日志中：

```bash
# macOS
log show --predicate 'process == "cron"' --last 1d

# Linux
grep CRON /var/log/syslog
```

## License

MIT
