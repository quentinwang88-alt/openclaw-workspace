# 视觉蓝图模型隔离对照

入口：`scripts/test_visual_blueprint_model_ab.py`。仅用于经用户授权的开发实验，不是运营任务入口。

从两条已完成短视频脚本读取冻结种子与口播检查点，以当前正式视觉提示词、归一化、装配和生产提示词渲染器比较 Sol/high 与 Astra/high。两组同输入；不刷新RDS、不改生产数据库、不写飞书、不生成媒体。默认模型不变。

```bash
python3 scripts/test_visual_blueprint_model_ab.py \
  --source-script-id SCRIPT_15868328DA87220DBF4A \
  --source-script-id SCRIPT_3BEE7DBCA04FE4ADA650 \
  --output-dir /Users/likeu3/.openclaw/shared/data/visual_blueprint_ab_20260909 \
  --prepare-only
```

授权模型调用后移除 `--prepare-only`；`--report-only` 只重渲染已有结果。同目录续跑复用已完成模型结果，源ID或受监控代码版本改变须使用新目录。输入快照冻结后不再重读历史任务，保证模型之间输入相同。

使用现有应用CLI，单模型最多两次瞬时错误尝试，每次最多300秒，不跨模型兜底、不做内容修订。所有输出在共享data目录，模型调用不获得图片、不运行首帧/TTS/视频。

先审计种子中的语义主线、动作与穿搭是否适合作为比较基准。旧种子缺字段时只记录局限，不能将局部模型实验报告为当前新规划的端到端验收。5个storyboard时间段与3个capture_units不是同一指标。

2026-09-09首轮：4/4返回并过基础校验，视觉与渲染回归80/80；Astra局部更清楚，但未解决旧冻结动作导致的后段重复，因此没有修改生产默认。完整报告：共享data下 `visual_blueprint_ab_20260909/review.md`。
