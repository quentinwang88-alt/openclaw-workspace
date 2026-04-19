# OpenClaw 集成指南 - Creator Grid Pipeline

## 概述

本文档说明如何在 OpenClaw 中集成和使用 Creator Grid Pipeline，实现自动化处理飞书表格中的未处理记录。

## 🎯 集成方式

### 方式 1: 简单通知（推荐）

最简单的方式，直接运行通知脚本：

```bash
# 在 OpenClaw 中执行
python3 notify_openclaw.py
```

或者在 OpenClaw 对话中说：
```
"运行宫图流水线"
"处理未处理的达人"
"生成宫图"
```

OpenClaw 会自动执行：
```python
import subprocess
subprocess.run(["python3", "notify_openclaw.py"])
```

### 方式 2: HTTP API 触发

先启动 HTTP 服务（一次性设置）：
```bash
# 在后台启动服务
nohup python3 creator_grid_pipeline.py serve > output/pipeline_http.log 2>&1 &
```

然后在 OpenClaw 中触发：
```python
import requests
response = requests.post("http://localhost:8766/run")
result = response.json()
print(f"成功: {result['success']}, 失败: {result['failed']}")
```

### 方式 3: 定时自动运行

设置定时任务后，无需手动触发，系统会自动运行：

```bash
# 使用管理工具设置
./pipeline_manager.sh
# 选择 "6" 或 "7" 设置定时任务
```

## 📋 使用场景

### 场景 1: 手动触发处理

**用户说**："处理飞书表格中的未处理记录"

**OpenClaw 执行**：
```python
import subprocess
result = subprocess.run(
    ["python3", "notify_openclaw.py"],
    capture_output=True,
    text=True
)
print(result.stdout)
```

### 场景 2: 测试处理少量记录

**用户说**："测试处理 5 个达人"

**OpenClaw 执行**：
```python
import subprocess
result = subprocess.run(
    ["python3", "notify_openclaw.py", "--limit", "5"],
    capture_output=True,
    text=True
)
print(result.stdout)
```

### 场景 3: 查看处理状态

**用户说**："查看宫图流水线状态"

**OpenClaw 执行**：
```python
import subprocess
result = subprocess.run(
    ["python3", "creator_grid_pipeline.py", "status"],
    capture_output=True,
    text=True
)
print(result.stdout)
```

### 场景 4: 查看处理日志

**用户说**："查看最近的处理日志"

**OpenClaw 执行**：
```python
import subprocess
result = subprocess.run(
    ["tail", "-n", "50", "output/pipeline.log"],
    capture_output=True,
    text=True
)
print(result.stdout)
```

## 🔧 OpenClaw 技能配置

创建一个 OpenClaw 技能，让 OpenClaw 能够识别相关命令：

### 创建技能文件

在 `skills/creator-grid-pipeline/` 目录下创建 `SKILL.md`：

```markdown
# Creator Grid Pipeline

## 描述
自动化处理达人视频封面和生成宫图的流水线系统

## 触发词
- 运行宫图流水线
- 处理未处理的达人
- 生成宫图
- 处理飞书表格
- 查看流水线状态
- 查看处理日志

## 命令

### 运行流水线
```bash
python3 notify_openclaw.py
```

### 测试运行
```bash
python3 notify_openclaw.py --limit 5
```

### 查看状态
```bash
python3 creator_grid_pipeline.py status
```

### 查看日志
```bash
tail -n 50 output/pipeline.log
```

## 使用示例

用户: "运行宫图流水线"
OpenClaw: 执行 `python3 notify_openclaw.py`

用户: "测试处理 5 个达人"
OpenClaw: 执行 `python3 notify_openclaw.py --limit 5`

用户: "查看流水线状态"
OpenClaw: 执行 `python3 creator_grid_pipeline.py status`
```

## 🚀 快速命令

### 在 OpenClaw 中可以直接说：

1. **"运行宫图流水线"**
   - 处理所有未处理的达人

2. **"测试处理 5 个达人"**
   - 测试处理少量达人

3. **"查看流水线状态"**
   - 查看运行状态和统计

4. **"查看处理日志"**
   - 查看最近的处理日志

5. **"检查 HTTP 服务状态"**
   - 检查 HTTP 服务是否运行

## 📊 返回结果示例

### 成功运行
```
======================================================================
通知 OpenClaw 处理未处理的记录
======================================================================
时间: 2026-03-10 13:00:00

开始运行流水线...
----------------------------------------------------------------------
[INFO] 从飞书多维表格读取待处理达人...
[INFO] 待处理达人数量: 50

[1/50] 处理达人: example_user
  ⏳ 步骤 1/3: 获取封面（共 24 个）...
  封面获取结果: 24/24
  ⏳ 步骤 2/3: 生成宫图...
  ✅ 宫图 1 已生成
  ✅ 宫图 2 已生成
  ⏳ 步骤 3/3: 上传到飞书...
  ✅ 上传成功

...

======================================================================
流水线运行完成
======================================================================
✅ 成功: 48
❌ 失败: 2
📊 总计: 50
⏱️  耗时: 25.3分钟
```

### 查看状态
```
======================================================================
流水线状态
======================================================================
上次运行: 2026-03-10T13:00:00
累计处理: 150
累计成功: 145
累计失败: 5

最近 10 次运行:
  - 20260310_130000: 48/50 成功
  - 20260309_120000: 50/50 成功
  ...
```

## 🔄 自动化流程

### 完整的自动化流程

```
用户在飞书表格中添加新达人
    ↓
（可选）设置定时任务自动运行
    或
用户通知 OpenClaw："运行宫图流水线"
    ↓
OpenClaw 执行 notify_openclaw.py
    ↓
流水线自动运行
    ├─ 读取飞书待处理达人
    ├─ 获取视频数据
    ├─ 生成宫图
    └─ 上传到飞书
    ↓
OpenClaw 返回处理结果
```

## 💡 最佳实践

### 1. 首次使用
```bash
# 测试运行
python3 notify_openclaw.py --limit 5

# 查看结果
python3 creator_grid_pipeline.py status
```

### 2. 日常使用
在 OpenClaw 中说：
- "运行宫图流水线"（处理所有）
- "查看流水线状态"（查看结果）

### 3. 自动化
设置定时任务，无需手动触发：
```bash
./pipeline_manager.sh
# 选择 "6" 或 "7"
```

## 🔍 故障排查

### 问题 1: OpenClaw 无法执行命令

**解决方案**：
确保脚本有执行权限：
```bash
chmod +x notify_openclaw.py
chmod +x creator_grid_pipeline.py
```

### 问题 2: HTTP 服务未运行

**检查状态**：
```bash
python3 notify_openclaw.py --check
```

**启动服务**：
```bash
python3 creator_grid_pipeline.py serve
```

### 问题 3: 查看详细错误

**查看日志**：
```bash
tail -n 100 output/pipeline.log | grep ERROR
```

## 📚 相关文档

- [`notify_openclaw.py`](notify_openclaw.py:1) - OpenClaw 通知脚本
- [`creator_grid_pipeline.py`](creator_grid_pipeline.py:1) - 流水线主程序
- [`PIPELINE_QUICKSTART.md`](PIPELINE_QUICKSTART.md:1) - 快速开始
- [`PIPELINE_USAGE_GUIDE.md`](PIPELINE_USAGE_GUIDE.md:1) - 完整使用指南

## 🎉 开始使用

在 OpenClaw 中说：
```
"运行宫图流水线"
```

就这么简单！OpenClaw 会自动处理飞书表格中的所有未处理记录。
