# 重要说明 - 请使用标准入口点

## ⚠️ 问题说明

在 `skills/creator-crm/` 目录下有很多旧文件，它们存在以下问题：

1. **只获取 12 张封面**：使用 `video_ids[:12]` 限制
2. **没有视频数据**：不包含播放量和带货金额
3. **没有状态管理**：不更新飞书任务状态
4. **代码重复**：多个文件实现相同功能

这些旧文件会导致 OpenClaw 混乱，因为它可能会读取到错误的代码。

## ✅ 标准入口点

**唯一推荐使用的文件**：

### 1. [`process_grid_task.py`](process_grid_task.py:1) - 主入口点
```bash
python3 process_grid_task.py
```

**功能**：
- ✅ 从飞书读取"待开始"状态的任务
- ✅ 获取前 24 个视频封面（有多少抓多少）
- ✅ 生成两张宫图（每张 12 个）
- ✅ 封面上包含播放量和带货金额
- ✅ 更新飞书状态（已完成/生成失败）

### 2. [`batch_processor_core.py`](batch_processor_core.py:1) - 核心处理逻辑
被 [`process_grid_task.py`](process_grid_task.py:1) 调用，不要直接使用。

## 🗑️ 废弃文件列表

以下文件已废弃，**不要使用**：

### skills/creator-crm/ 目录下的废弃文件

1. `complete_automation_v2.py` - 只获取 12 张封面
2. `full_automation.py` - 只获取 12 张封面
3. `generate_grids_openclaw.py` - 只获取 12 张封面
4. `process_single.py` - 只获取 12 张封面
5. `generate_batch_code.py` - 只获取 12 张封面
6. `background_automation.py` - 只获取 12 张封面
7. `batch_generate_grids.py` - 只获取 12 张封面
8. `run_batch_generation.py` - 只获取 12 张封面
9. `complete_automation.py` - 只获取 12 张封面
10. `auto_process_creators.py` - 只获取 12 张封面
11. `practical_automation.py` - 只获取 12 张封面
12. `full_automation_integrated.py` - 只获取 12 张封面

### skills/creator-crm/core/ 目录下的废弃文件

1. `sub_agents.py` - 第 219 行：`video_ids[:12]`

## 📝 如何使用标准入口点

### 在 OpenClaw 中

**用户说**："处理飞书表格中的待开始任务"

**OpenClaw 执行**：
```python
import subprocess
subprocess.run(["python3", "process_grid_task.py"])
```

### 命令行

```bash
# 处理所有待开始的任务
python3 process_grid_task.py
```

### 工作流程

```
1. 打开飞书多维表格
   ↓
2. 找到状态为"待开始"的任务
   ↓
3. 逐个处理任务
   ├─ 获取前 24 个视频封面
   ├─ 获取播放量和带货金额
   ├─ 生成两张宫图
   └─ 上传到飞书
   ↓
4. 更新任务状态
   ├─ 成功 → "已完成"
   └─ 失败 → "生成失败"（附带错误信息）
```

## 🔧 修复建议

### 方式 1: 重命名废弃文件（推荐）

```bash
cd skills/creator-crm

# 重命名废弃文件，添加 .deprecated 后缀
for file in complete_automation_v2.py full_automation.py generate_grids_openclaw.py process_single.py generate_batch_code.py background_automation.py batch_generate_grids.py run_batch_generation.py complete_automation.py auto_process_creators.py practical_automation.py full_automation_integrated.py; do
    if [ -f "$file" ]; then
        mv "$file" "$file.deprecated"
    fi
done
```

### 方式 2: 移动到废弃目录

```bash
cd skills/creator-crm

# 创建废弃目录
mkdir -p deprecated

# 移动废弃文件
mv complete_automation_v2.py deprecated/
mv full_automation.py deprecated/
mv generate_grids_openclaw.py deprecated/
mv process_single.py deprecated/
mv generate_batch_code.py deprecated/
mv background_automation.py deprecated/
mv batch_generate_grids.py deprecated/
mv run_batch_generation.py deprecated/
mv complete_automation.py deprecated/
mv auto_process_creators.py deprecated/
mv practical_automation.py deprecated/
mv full_automation_integrated.py deprecated/
```

### 方式 3: 添加废弃标记

在每个废弃文件的开头添加：

```python
#!/usr/bin/env python3
"""
⚠️ DEPRECATED - 此文件已废弃

请使用标准入口点: process_grid_task.py

原因:
- 只获取 12 张封面（应该获取 24 张）
- 没有视频数据（播放量、带货金额）
- 没有状态管理（待开始/已完成/生成失败）

详见: DEPRECATED_FILES.md
"""

import sys
print("⚠️ 此文件已废弃，请使用: python3 process_grid_task.py")
sys.exit(1)
```

## 📚 相关文档

- [`process_grid_task.py`](process_grid_task.py:1) - 标准入口点
- [`batch_processor_core.py`](batch_processor_core.py:1) - 核心处理逻辑
- [`BATCH_PROCESSOR_IMPROVEMENTS.md`](BATCH_PROCESSOR_IMPROVEMENTS.md:1) - 改进说明

## 🎯 总结

**只使用这一个文件**：
```bash
python3 process_grid_task.py
```

**不要使用**：
- skills/creator-crm/ 目录下的其他 automation/process/generate 相关文件
- 它们都有问题，会导致 OpenClaw 混乱

**OpenClaw 应该执行**：
```python
import subprocess
subprocess.run(["python3", "process_grid_task.py"])
```

就这么简单！
