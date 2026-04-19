# Creator Grid Pipeline - 快速开始

## 🚀 5 分钟快速上手

### 1. 测试运行

```bash
# 使用管理工具（推荐）
./pipeline_manager.sh

# 或直接命令行
python3 creator_grid_pipeline.py run --limit 5
```

### 2. 查看结果

```bash
# 查看状态
python3 creator_grid_pipeline.py status

# 查看日志
tail -f output/pipeline.log
```

### 3. 设置自动化

```bash
# 使用管理工具设置定时任务
./pipeline_manager.sh
# 选择 "6" 或 "7" 设置定时任务
```

## 📋 完整流程

### 步骤 1: 检查配置

确保 [`config.py`](config.py:1) 中的飞书配置正确：

```python
FEISHU_APP_TOKEN = "your_app_token"
FEISHU_TABLE_ID = "your_table_id"
```

### 步骤 2: 测试运行

```bash
# 测试处理 1 个达人
python3 creator_grid_pipeline.py run --limit 1
```

### 步骤 3: 正式运行

```bash
# 处理所有待处理达人
python3 creator_grid_pipeline.py run
```

### 步骤 4: 设置自动化

选择一种方式：

**方式 A: 定时任务（推荐）**
```bash
./pipeline_manager.sh
# 选择 "6. 设置定时任务（crontab）" 或 "7. 设置定时任务（launchd）"
```

**方式 B: HTTP 服务**
```bash
# 启动服务
python3 creator_grid_pipeline.py serve

# 在另一个终端触发
curl -X POST http://localhost:8766/run
```

## 🎯 使用场景

### 场景 1: 每天自动处理

```bash
# 设置每天凌晨 2 点自动运行
./pipeline_manager.sh
# 选择 "6" 或 "7"，然后选择 "每天凌晨 2 点"
```

### 场景 2: 手动触发

```bash
# 方式 1: 命令行
python3 creator_grid_pipeline.py run

# 方式 2: HTTP API
curl -X POST http://localhost:8766/run
```

### 场景 3: OpenClaw 集成

在 OpenClaw 中直接调用：
```python
import subprocess
subprocess.run(["python3", "creator_grid_pipeline.py", "run"])
```

## 📊 监控和维护

### 查看运行状态

```bash
python3 creator_grid_pipeline.py status
```

### 查看日志

```bash
# 实时查看
tail -f output/pipeline.log

# 查看错误
grep ERROR output/pipeline.log
```

### 查看定时任务

```bash
# crontab
crontab -l | grep creator_grid_pipeline

# launchd (macOS)
launchctl list | grep creator-grid-pipeline
```

## 🔧 常见问题

### Q: 如何停止定时任务？

```bash
./pipeline_manager.sh
# 选择 "8. 停止定时任务"
```

### Q: 如何查看处理结果？

```bash
# 查看状态文件
cat output/pipeline_state.json

# 查看日志
tail -n 100 output/pipeline.log
```

### Q: 如何重新处理失败的达人？

流水线会自动跳过已处理的达人，只处理待处理的达人。如需重新处理，需要在飞书中清空对应记录的视频截图字段。

## 📚 更多文档

- [完整使用指南](PIPELINE_USAGE_GUIDE.md) - 详细的使用说明
- [配置说明](PIPELINE_CONFIG.md) - 定时任务和 API 配置
- [改进说明](BATCH_PROCESSOR_IMPROVEMENTS.md) - 核心功能改进

## 🎉 开始使用

```bash
# 1. 测试运行
python3 creator_grid_pipeline.py run --limit 5

# 2. 查看结果
python3 creator_grid_pipeline.py status

# 3. 设置自动化
./pipeline_manager.sh
```

就这么简单！🚀
