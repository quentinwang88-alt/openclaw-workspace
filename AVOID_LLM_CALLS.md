# OpenClaw 与 LLM 使用策略

## 策略更新（2026-03-11）

**新策略**：允许 OpenClaw 在必要时合理使用 LLM 模型辅助执行任务。

## 推荐的触发方式

### 方式 1: 通过 OpenClaw 触发（推荐）

直接告诉 OpenClaw 执行任务，它会：
1. 读取 `SKILL.md` 了解如何执行
2. 运行 `run_pipeline.py` 脚本
3. 在遇到问题时使用 LLM 辅助诊断

```
# 在 OpenClaw 中说：
"处理飞书表格中的待开始达人，生成宫格图"
"运行 creator-crm 流水线，处理前5个达人"
"检查 creator-crm 的执行状态"
```

### 方式 2: 直接命令行运行

```bash
# 处理所有待开始的达人
python3 skills/creator-crm/run_pipeline.py

# 只处理前5个
python3 skills/creator-crm/run_pipeline.py --limit 5

# 测试模式
python3 skills/creator-crm/run_pipeline.py --dry-run --limit 10
```

### 方式 3: 定时任务（完全自动化）

```bash
# 设置定时任务
./pipeline_manager.sh
# 选择 "6" 或 "7" 设置每天自动运行
```

## LLM 辅助的合理使用场景

OpenClaw 可以在以下情况使用 LLM：

1. **理解任务意图**：将自然语言转换为具体操作
2. **错误诊断**：分析错误日志，提供解决方案
3. **Cookie 更新指导**：当 Kalodata Cookie 过期时，指导用户获取新 Cookie
4. **批量策略建议**：根据待处理数量，建议最优的批次大小
5. **状态报告**：将执行结果整理成易读的报告

## 不需要 LLM 的操作

以下操作完全由脚本自动完成，不需要 LLM：
- 从飞书读取待处理达人
- 调用 Kalodata API 获取视频数据
- 下载视频封面图片
- 生成宫格图
- 上传到飞书并更新状态

## 快速参考

```bash
# 通过 OpenClaw（允许 LLM 辅助）
# 直接在 OpenClaw 中描述任务

# 直接运行（不需要 LLM）
python3 skills/creator-crm/run_pipeline.py --limit 5

# 定时任务（完全自动化）
./pipeline_manager.sh
```
