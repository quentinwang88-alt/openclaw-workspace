---
name: daily-report-inspection
description: |
  每日运营日报自动巡检。每天 18:20 自动找到当天日报（配饰+女装），解析核心数据，
  按规则检测异常（视频生成积压、发布断档、达人建联断档、样品审批卡点、可用率偏低等），
  输出一份 500-800 字 Markdown 巡检卡片，通过老王助理飞书机器人发送给老板。
  触发词：日报巡检、日报检查、巡检日报、daily report inspection
---

# 每日日报巡检 Skill

**版本**: V1.0
**创建日期**: 2026-05-20
**作者**: 开发经理

## 功能

自动巡检每日运营日报，回答四个问题：
1. 今天哪些事情真的推进了？
2. 今天哪里出现异常？
3. 明天应该盯哪几件事？
4. 哪些问题需要老板介入？

## 使用方法

```bash
# 直接运行巡检（读取当天日报）
cd skills/daily-report-inspection && python3 inspect_report.py

# 指定日期运行
python3 inspect_report.py --date 2026-05-20

# 只生成摘要不发送飞书
python3 inspect_report.py --no-send

# 查看帮助
python3 inspect_report.py --help
```

## 日报来源

飞书知识库：`https://gcngopvfvo0q.feishu.cn/wiki/DR6nwi6auiXqRHkrh9hccLyCnZe`

## 文件结构

```
skills/daily-report-inspection/
├── SKILL.md              # 本文件
├── config.json           # 配置文件（wiki URL, bot 账号, 匹配规则）
├── inspect_report.py     # 主入口脚本
├── feishu_reader.py      # 飞书知识库读取器（定位+读取日报）
├── report_parser.py      # 日报内容解析器（自然语言 -> 结构化字段）
├── inspection_engine.py  # 巡检规则引擎（应用规则，产出异常列表）
├── summary_builder.py    # 巡检卡片生成器（Markdown 输出）
└── feishu_sender.py      # 飞书消息发送器（通过老王助理 bot 推送）
```

## 巡检规则

### 短视频生成
- 今日生成=0 且 明日待生成≥10 → 视频生成积压风险
- 直接可用率<40% → 直接可用率偏低
- 综合可用率<60% → 综合可用率偏低
- 包含类目/锚点/跑偏关键词 → 生成链路跑偏

### 视频发布
- 某店铺发布=0 → 内容发布断档
- failed_count>0 → 发布失败
- 生成数明显大于发布数 → 生成到发布转化卡点

### 达人建联
- 建联=0/未进行 → 达人动作断档
- 建联>500 → 建联口径需确认
- 连续2天断档 → 达人渠道推进不足

### 样品审批
- 有申请但批出=0 → 样品审批转化为0
- 多业务线均批出=0 → 样品审批链路集中卡点

### 选品及上品
- 上品>0 → 供给侧有效推进
- 上品多但视频/发布少 → 上品和内容承接不匹配

### 日报定位
- 精准匹配失败 → 模糊匹配
- 模糊匹配失败 → 最近修改匹配
- 仍未找到 → 缺失提醒

## 配置

编辑 `config.json` 修改日报目录、匹配规则、巡检阈值等。

## 注意事项

- 日报内容保持现有格式，不强制结构化
- 解析采用正则+关键词，允许字段缺失
- 异常判断优先抓经营问题，不做形式化总结
- 输出控制在 500-800 字
