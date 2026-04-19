# Creator Monitoring Assistant V1

## 项目简介

这是一个基于 Python 的达人经营监控助手。系统接收每周导出的达人经营 Excel，完成：

1. 原始数据入库
2. clean 数据标准化
3. 周指标、环比、4 周滚动、生命周期指标计算
4. 规则标签判定
5. 飞书单表同步

数据库是事实源，飞书是查看与协作层。

## 环境变量说明

必填：

- `DATABASE_URL`
- `FEISHU_APP_TOKEN`
- `FEISHU_TABLE_ID`

推荐：

- `FEISHU_APP_ID`
- `FEISHU_APP_SECRET`
- `FEISHU_ENABLE_SYNC=true`
- `FEISHU_WRITE_BATCH_SIZE=100`
- `FEISHU_READ_PAGE_SIZE=200`
- `STORE_DEFAULT=""`
- `MIN_VALID_GMV=100`
- `MIN_VALID_ORDER_COUNT=3`
- `MIN_VALID_CONTENT_ACTION_COUNT=1`
- `MIN_VALID_SAMPLE_COUNT=1`
- `MIN_POSITIVE_EFFICIENCY_PEER_COUNT=5`

示例：

```bash
export DATABASE_URL="postgresql://user:password@localhost:5432/creator_monitoring"
export FEISHU_APP_ID="cli_xxx"
export FEISHU_APP_SECRET="xxx"
export FEISHU_APP_TOKEN="basc_xxx"
export FEISHU_TABLE_ID="tbl_xxx"
```

## 数据库初始化

PostgreSQL:

```bash
psql "$DATABASE_URL" -f scripts/init_db.sql
```

本地轻量验证也支持 SQLite：

```bash
export DATABASE_URL="sqlite:///$(pwd)/data/creator_monitoring.sqlite3"
python3 run_pipeline.py --init-db --stat-week "2026-W13" --source-file-path "/tmp/report.xlsx"
```

## Excel 导入方式

输入 Excel 需至少包含这些列：

- 达人名称
- 联盟归因 GMV
- 退款金额
- 归因订单数
- 联盟归因成交件数
- 已退款的商品件数
- 平均订单金额
- 日均商品成交件数
- 视频数
- 直播数
- 预计佣金
- 已发货样品数

## 周任务运行方式

```bash
python3 run_pipeline.py --stat-week "2026-W13" --source-file-path "/path/to/report.xlsx" --platform "tiktok" --country "th" --store "泰国服装1店"
```

## 飞书配置方法

需为飞书应用开通多维表格相关 scope，至少包括：

- 读取多维表格 / 字段 / 记录
- 新增记录
- 更新记录

同步层通过多维表格开放 API 完成字段读取、记录分页查询、批量新增和批量更新；同一张表始终串行写入。

## 如何初始化飞书单表

创建一张多维表格“达人当前动作表”，字段至少包含：

- `record_key`
- `达人名称`
- `国家`
- `店铺`
- `负责人`
- `当前统计周`
- `本周GMV`
- `上周GMV`
- `GMV环比`
- `本周内容动作数`
- `上周内容动作数`
- `动作数环比`
- `本周单动作GMV`
- `单动作GMV环比`
- `本周退款率`
- `退款率变化`
- `近4周GMV`
- `当前主标签`
- `当前风险标签`
- `优先级`
- `核心原因`
- `本周建议动作`
- `跟进状态`
- `人工备注`
- `最近更新时间`

说明：

- 飞书表一行只代表一个达人当前状态，不再保存 `达人 × 周` 历史明细
- `record_key = creator_key = platform + ":" + country + ":" + store + ":" + normalized_creator_name`
- 系统只更新系统字段，不覆盖 `负责人`、`跟进状态`、`人工备注`
- V1 不自动删除旧达人行，建议默认视图筛选 `当前统计周 = 最新周`

## 如何查看同步结果

飞书里可按 `当前主标签`、`当前风险标签`、`优先级` 和 `当前统计周` 建视图筛选。

## 如何重跑某一周

直接对同一 `stat_week` 重跑：

```bash
python3 run_pipeline.py --stat-week "2026-W13" --source-file-path "/path/to/report.xlsx" --country "th" --store "泰国服装1店"
```

系统会保留原始导入批次历史，但会重建该周的 clean / metrics / monitoring result，并按 `record_key=creator_key` 覆盖飞书系统字段。

## 无效达人过滤

为避免零样本、零动作、零结果的无效数据拉低阈值，系统会先做一层有效达人过滤。满足以下任一条件才会进入标签与飞书同步：

- `GMV >= MIN_VALID_GMV`
- `订单数 >= MIN_VALID_ORDER_COUNT`
- `内容动作数 >= MIN_VALID_CONTENT_ACTION_COUNT`
- `样品数 >= MIN_VALID_SAMPLE_COUNT`

这些阈值都可以通过环境变量调整。

为避免 `0` 值把效率 benchmark 拉低，`单动作 GMV / 单样品 GMV` 的分位数只会在“正样本数量足够”时计算，默认至少需要 5 个正样本；否则该周不会用这一项去放宽 `potential_new` 判定。

## 人工字段说明

下列字段属于人工维护字段，程序不会覆盖：

- `负责人`
- `跟进状态`
- `人工备注`
