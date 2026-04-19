# Creator Grid Pipeline - 自动化流水线配置

## 定时任务配置（Cron）

### 方式 1: 使用系统 crontab

编辑 crontab：
```bash
crontab -e
```

添加定时任务：
```bash
# 每天凌晨 2 点运行流水线
0 2 * * * cd /Users/likeu3/.openclaw/workspace && /usr/bin/python3 creator_grid_pipeline.py run >> output/pipeline_cron.log 2>&1

# 每 6 小时运行一次
0 */6 * * * cd /Users/likeu3/.openclaw/workspace && /usr/bin/python3 creator_grid_pipeline.py run >> output/pipeline_cron.log 2>&1

# 每周一早上 8 点运行
0 8 * * 1 cd /Users/likeu3/.openclaw/workspace && /usr/bin/python3 creator_grid_pipeline.py run >> output/pipeline_cron.log 2>&1
```

### 方式 2: 使用 launchd (macOS 推荐)

创建 plist 文件：`~/Library/LaunchAgents/com.openclaw.creator-grid-pipeline.plist`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.openclaw.creator-grid-pipeline</string>
    
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/python3</string>
        <string>/Users/likeu3/.openclaw/workspace/creator_grid_pipeline.py</string>
        <string>run</string>
    </array>
    
    <key>WorkingDirectory</key>
    <string>/Users/likeu3/.openclaw/workspace</string>
    
    <key>StandardOutPath</key>
    <string>/Users/likeu3/.openclaw/workspace/output/pipeline_launchd.log</string>
    
    <key>StandardErrorPath</key>
    <string>/Users/likeu3/.openclaw/workspace/output/pipeline_launchd_error.log</string>
    
    <!-- 每天凌晨 2 点运行 -->
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>2</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
    
    <!-- 或者使用间隔运行（每 6 小时） -->
    <!--
    <key>StartInterval</key>
    <integer>21600</integer>
    -->
    
    <key>RunAtLoad</key>
    <false/>
</dict>
</plist>
```

加载定时任务：
```bash
launchctl load ~/Library/LaunchAgents/com.openclaw.creator-grid-pipeline.plist
```

查看状态：
```bash
launchctl list | grep creator-grid-pipeline
```

停止定时任务：
```bash
launchctl unload ~/Library/LaunchAgents/com.openclaw.creator-grid-pipeline.plist
```

### 方式 3: 使用 systemd (Linux)

创建服务文件：`/etc/systemd/system/creator-grid-pipeline.service`

```ini
[Unit]
Description=Creator Grid Pipeline Service
After=network.target

[Service]
Type=simple
User=your_username
WorkingDirectory=/Users/likeu3/.openclaw/workspace
ExecStart=/usr/bin/python3 /Users/likeu3/.openclaw/workspace/creator_grid_pipeline.py run
Restart=on-failure
RestartSec=60

[Install]
WantedBy=multi-user.target
```

创建定时器文件：`/etc/systemd/system/creator-grid-pipeline.timer`

```ini
[Unit]
Description=Creator Grid Pipeline Timer
Requires=creator-grid-pipeline.service

[Timer]
# 每天凌晨 2 点运行
OnCalendar=*-*-* 02:00:00
# 或者每 6 小时运行一次
# OnCalendar=*-*-* 00/6:00:00

Persistent=true

[Install]
WantedBy=timers.target
```

启用定时器：
```bash
sudo systemctl enable creator-grid-pipeline.timer
sudo systemctl start creator-grid-pipeline.timer
```

查看状态：
```bash
sudo systemctl status creator-grid-pipeline.timer
sudo systemctl list-timers
```

## HTTP API 触发

### 启动 HTTP 服务

```bash
# 默认端口 8766
python3 creator_grid_pipeline.py serve

# 自定义端口
python3 creator_grid_pipeline.py serve --port 9000
```

### API 端点

#### 1. 健康检查
```bash
curl http://localhost:8766/health
```

响应：
```json
{
  "status": "ok"
}
```

#### 2. 查询状态
```bash
curl http://localhost:8766/status
```

响应：
```json
{
  "last_run": "2026-03-10T12:00:00",
  "total_processed": 150,
  "total_success": 145,
  "total_failed": 5,
  "recent_runs": [
    {
      "run_id": "20260310_120000",
      "started_at": "2026-03-10T12:00:00",
      "completed_at": "2026-03-10T12:30:00",
      "total": 50,
      "success": 48,
      "failed": 2
    }
  ]
}
```

#### 3. 触发运行
```bash
# 运行所有待处理达人
curl -X POST http://localhost:8766/run

# 限制处理数量（测试用）
curl -X POST http://localhost:8766/run \
  -H "Content-Type: application/json" \
  -d '{"limit": 10}'
```

响应：
```json
{
  "run_id": "20260310_140000",
  "started_at": "2026-03-10T14:00:00",
  "completed_at": "2026-03-10T14:25:00",
  "total": 50,
  "success": 48,
  "failed": 2,
  "results": [...]
}
```

## 命令行触发

### 运行流水线
```bash
# 运行所有待处理达人
python3 creator_grid_pipeline.py run

# 限制处理数量（测试用）
python3 creator_grid_pipeline.py run --limit 10
```

### 查看状态
```bash
python3 creator_grid_pipeline.py status
```

## 监控和日志

### 日志文件
- 主日志：`output/pipeline.log`
- Cron 日志：`output/pipeline_cron.log`
- Launchd 日志：`output/pipeline_launchd.log`

### 查看日志
```bash
# 实时查看日志
tail -f output/pipeline.log

# 查看最近 100 行
tail -n 100 output/pipeline.log

# 搜索错误
grep ERROR output/pipeline.log
```

### 状态文件
- 状态文件：`output/pipeline_state.json`
- 包含历史运行记录和统计信息

## 通知配置（可选）

### 飞书机器人通知

在流水线完成后发送通知到飞书群：

```python
# 在 creator_grid_pipeline.py 中添加
import requests

def send_feishu_notification(run_data: Dict[str, Any]):
    """发送飞书通知"""
    webhook_url = "https://open.feishu.cn/open-apis/bot/v2/hook/YOUR_WEBHOOK_TOKEN"
    
    message = {
        "msg_type": "text",
        "content": {
            "text": f"""Creator Grid Pipeline 运行完成
运行ID: {run_data['run_id']}
成功: {run_data['success']}
失败: {run_data['failed']}
总计: {run_data['total']}"""
        }
    }
    
    requests.post(webhook_url, json=message)
```

### 邮件通知

使用 Python 的 `smtplib` 发送邮件通知。

## 故障排查

### 检查定时任务是否运行
```bash
# crontab
grep CRON /var/log/syslog

# launchd (macOS)
log show --predicate 'process == "launchd"' --last 1h | grep creator-grid

# systemd (Linux)
journalctl -u creator-grid-pipeline.timer
```

### 检查流水线状态
```bash
python3 creator_grid_pipeline.py status
```

### 手动测试
```bash
# 测试处理 1 个达人
python3 creator_grid_pipeline.py run --limit 1
```

## 性能优化

### 并发处理
修改 `creator_grid_pipeline.py` 中的处理逻辑，使用多线程或多进程：

```python
from concurrent.futures import ThreadPoolExecutor

def run_parallel(self, creators: List[Dict], max_workers: int = 5):
    """并发处理达人"""
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_creator, c) for c in creators]
        results = [f.result() for f in futures]
    return results
```

### 批量处理
将达人分批处理，避免一次性处理过多：

```python
def run_in_batches(self, creators: List[Dict], batch_size: int = 50):
    """分批处理达人"""
    for i in range(0, len(creators), batch_size):
        batch = creators[i:i+batch_size]
        self.process_batch(batch)
```

## 安全建议

1. **API 认证**：为 HTTP API 添加认证机制
2. **日志轮转**：定期清理旧日志文件
3. **错误重试**：对失败的任务实现重试机制
4. **资源限制**：限制并发数量，避免资源耗尽

## 相关文件

- [`creator_grid_pipeline.py`](creator_grid_pipeline.py:1) - 流水线主程序
- [`batch_processor_core.py`](batch_processor_core.py:1) - 核心处理逻辑
- [`config.py`](config.py:1) - 配置文件
