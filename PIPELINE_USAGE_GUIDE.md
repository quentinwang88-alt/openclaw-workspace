# Creator Grid Pipeline - 完整使用指南

## 概述

Creator Grid Pipeline 是一个自动化流水线系统，用于批量处理达人视频封面和生成宫图。

### 核心功能

1. **自动读取飞书数据**：从飞书多维表格读取待处理达人列表
2. **获取视频数据**：获取视频封面、播放量、带货金额等数据
3. **生成宫图**：每个达人生成最多 2 张宫图（每张 12 个视频）
4. **上传到飞书**：自动上传生成的宫图到飞书多维表格

### 触发方式

- ✅ **命令行触发**：手动运行
- ✅ **定时触发**：通过 cron/launchd/systemd 定时运行
- ✅ **HTTP API 触发**：通过 HTTP 接口远程触发
- ✅ **OpenClaw 集成**：可被 OpenClaw 直接调用

## 快速开始

### 方式 1: 使用管理工具（推荐）

```bash
# 运行管理工具
./pipeline_manager.sh
```

管理工具提供交互式菜单，包括：
- 运行流水线
- 测试运行
- 查看状态
- 启动 HTTP 服务
- 查看日志
- 设置定时任务
- 停止定时任务

### 方式 2: 直接命令行

```bash
# 运行流水线
python3 creator_grid_pipeline.py run

# 测试运行（处理 5 个达人）
python3 creator_grid_pipeline.py run --limit 5

# 查看状态
python3 creator_grid_pipeline.py status

# 启动 HTTP 服务
python3 creator_grid_pipeline.py serve
```

## 详细使用说明

### 1. 命令行模式

#### 运行流水线
```bash
# 处理所有待处理达人
python3 creator_grid_pipeline.py run

# 限制处理数量（用于测试）
python3 creator_grid_pipeline.py run --limit 10
```

#### 查看状态
```bash
python3 creator_grid_pipeline.py status
```

输出示例：
```
======================================================================
流水线状态
======================================================================
上次运行: 2026-03-10T12:00:00
累计处理: 150
累计成功: 145
累计失败: 5

最近 10 次运行:
  - 20260310_120000: 48/50 成功
  - 20260309_120000: 50/50 成功
  ...
```

### 2. HTTP API 模式

#### 启动服务
```bash
# 默认端口 8766
python3 creator_grid_pipeline.py serve

# 自定义端口
python3 creator_grid_pipeline.py serve --port 9000
```

#### API 端点

**健康检查**
```bash
curl http://localhost:8766/health
```

**查询状态**
```bash
curl http://localhost:8766/status
```

**触发运行**
```bash
# 运行所有待处理达人
curl -X POST http://localhost:8766/run

# 限制处理数量
curl -X POST http://localhost:8766/run \
  -H "Content-Type: application/json" \
  -d '{"limit": 10}'
```

### 3. 定时任务模式

#### macOS (launchd)

1. 使用管理工具设置：
```bash
./pipeline_manager.sh
# 选择 "7. 设置定时任务（launchd - macOS）"
```

2. 或手动创建配置文件：
```bash
# 创建 plist 文件
cat > ~/Library/LaunchAgents/com.openclaw.creator-grid-pipeline.plist << 'EOF'
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
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>2</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
    <key>RunAtLoad</key>
    <false/>
</dict>
</plist>
EOF

# 加载任务
launchctl load ~/Library/LaunchAgents/com.openclaw.creator-grid-pipeline.plist

# 查看状态
launchctl list | grep creator-grid-pipeline

# 停止任务
launchctl unload ~/Library/LaunchAgents/com.openclaw.creator-grid-pipeline.plist
```

#### Linux (crontab)

1. 使用管理工具设置：
```bash
./pipeline_manager.sh
# 选择 "6. 设置定时任务（crontab）"
```

2. 或手动编辑 crontab：
```bash
crontab -e
```

添加以下行：
```bash
# 每天凌晨 2 点运行
0 2 * * * cd /Users/likeu3/.openclaw/workspace && /usr/bin/python3 creator_grid_pipeline.py run >> output/pipeline_cron.log 2>&1

# 每 6 小时运行一次
0 */6 * * * cd /Users/likeu3/.openclaw/workspace && /usr/bin/python3 creator_grid_pipeline.py run >> output/pipeline_cron.log 2>&1
```

## 流水线工作流程

```
┌─────────────────────────────────────────────────────────────┐
│                    流水线启动                                │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  步骤 1: 从飞书多维表格读取待处理达人列表                    │
│  - 筛选条件：视频截图字段为空                                │
│  - 返回：达人列表（record_id, tk_handle, video_ids）        │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  步骤 2: 逐个处理达人                                        │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ 2.1 获取视频数据（播放量、带货金额）                  │  │
│  │     - 数据源：Kalodata API / 其他数据源               │  │
│  └───────────────────────────────────────────────────────┘  │
│                            ↓                                 │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ 2.2 获取视频封面（前 24 个）                          │  │
│  │     - 使用 oEmbed API 获取封面 URL                    │  │
│  │     - 有多少抓多少，最多 24 个                        │  │
│  └───────────────────────────────────────────────────────┘  │
│                            ↓                                 │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ 2.3 生成宫图                                          │  │
│  │     - 宫图 1：前 12 个视频封面                        │  │
│  │     - 宫图 2：第 13-24 个视频封面（如果有）           │  │
│  │     - 封面上叠加播放量和带货金额                      │  │
│  └───────────────────────────────────────────────────────┘  │
│                            ↓                                 │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ 2.4 上传到飞书                                        │  │
│  │     - 上传宫图到飞书多维表格                          │  │
│  │     - 更新记录的视频截图字段                          │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  步骤 3: 汇总结果                                            │
│  - 成功数量                                                  │
│  - 失败数量                                                  │
│  - 保存运行记录                                              │
└─────────────────────────────────────────────────────────────┘
```

## 配置说明

### 核心配置

在 [`config.py`](config.py:1) 中配置：

```python
# 飞书配置
FEISHU_APP_TOKEN = "your_app_token"
FEISHU_TABLE_ID = "your_table_id"

# 封面数量配置
MIN_COVER_COUNT = 12  # 最少需要的封面数
MAX_VIDEOS_PER_CREATOR = 24  # 每个达人最多获取的视频数
```

### 流水线配置

在 [`creator_grid_pipeline.py`](creator_grid_pipeline.py:1) 中配置：

```python
# HTTP 服务端口
PIPELINE_PORT = 8766

# 状态文件路径
STATE_FILE = workspace_root / "output/pipeline_state.json"

# 日志文件路径
LOG_FILE = workspace_root / "output/pipeline.log"
```

## 日志和监控

### 日志文件

- **主日志**：`output/pipeline.log`
- **Cron 日志**：`output/pipeline_cron.log`
- **Launchd 日志**：`output/pipeline_launchd.log`
- **Launchd 错误日志**：`output/pipeline_launchd_error.log`

### 查看日志

```bash
# 实时查看日志
tail -f output/pipeline.log

# 查看最近 100 行
tail -n 100 output/pipeline.log

# 搜索错误
grep ERROR output/pipeline.log

# 搜索成功记录
grep SUCCESS output/pipeline.log
```

### 状态文件

状态文件 `output/pipeline_state.json` 包含：
- 上次运行时间
- 累计处理数量
- 累计成功/失败数量
- 最近 100 次运行记录

## OpenClaw 集成

### 方式 1: 直接调用

在 OpenClaw 中可以直接调用流水线：

```python
import subprocess

# 运行流水线
result = subprocess.run(
    ["python3", "creator_grid_pipeline.py", "run"],
    cwd="/Users/likeu3/.openclaw/workspace",
    capture_output=True,
    text=True
)

print(result.stdout)
```

### 方式 2: HTTP API 调用

```python
import requests

# 触发运行
response = requests.post("http://localhost:8766/run")
result = response.json()

print(f"成功: {result['success']}")
print(f"失败: {result['failed']}")
```

### 方式 3: 作为 OpenClaw 技能

创建 OpenClaw 技能配置：

```yaml
# skills/creator-grid-pipeline/SKILL.md
name: Creator Grid Pipeline
description: 自动化处理达人视频封面和生成宫图
trigger: 
  - "运行宫图流水线"
  - "处理达人视频"
  - "生成宫图"
command: python3 creator_grid_pipeline.py run
```

## 故障排查

### 问题 1: 流水线运行失败

**检查日志**：
```bash
tail -n 100 output/pipeline.log | grep ERROR
```

**常见原因**：
- 飞书 API 认证失败
- 网络连接问题
- 视频封面获取失败

### 问题 2: 定时任务未运行

**检查 crontab**：
```bash
crontab -l | grep creator_grid_pipeline
```

**检查 launchd (macOS)**：
```bash
launchctl list | grep creator-grid-pipeline
log show --predicate 'process == "launchd"' --last 1h | grep creator-grid
```

### 问题 3: HTTP 服务无法访问

**检查服务是否运行**：
```bash
curl http://localhost:8766/health
```

**检查端口占用**：
```bash
lsof -i :8766
```

## 性能优化

### 并发处理

修改 [`creator_grid_pipeline.py`](creator_grid_pipeline.py:1) 添加并发处理：

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

分批处理大量达人：

```python
def run_in_batches(self, creators: List[Dict], batch_size: int = 50):
    """分批处理达人"""
    for i in range(0, len(creators), batch_size):
        batch = creators[i:i+batch_size]
        self.process_batch(batch)
        time.sleep(60)  # 批次间休息
```

## 最佳实践

1. **测试先行**：使用 `--limit` 参数先测试少量达人
2. **定期监控**：定期查看日志和状态
3. **错误处理**：对失败的任务实现重试机制
4. **资源限制**：避免并发数过高导致资源耗尽
5. **日志轮转**：定期清理旧日志文件

## 相关文件

- [`creator_grid_pipeline.py`](creator_grid_pipeline.py:1) - 流水线主程序
- [`pipeline_manager.sh`](pipeline_manager.sh:1) - 管理工具
- [`batch_processor_core.py`](batch_processor_core.py:1) - 核心处理逻辑
- [`PIPELINE_CONFIG.md`](PIPELINE_CONFIG.md:1) - 配置说明
- [`BATCH_PROCESSOR_IMPROVEMENTS.md`](BATCH_PROCESSOR_IMPROVEMENTS.md:1) - 改进说明

## 支持

如有问题，请查看：
1. 日志文件：`output/pipeline.log`
2. 状态文件：`output/pipeline_state.json`
3. 相关文档：`PIPELINE_CONFIG.md`
