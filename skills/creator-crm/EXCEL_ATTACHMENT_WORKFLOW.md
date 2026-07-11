# Creator CRM Excel 附件驱动流程

## 目标

后续 Creator CRM 的发起方式改为：

1. 用户把 Kalodata 榜单 Excel 作为附件发给 OpenClaw。
2. OpenClaw 直接解析 Excel，不再先导入候选飞书表。
3. 每个候选达人先查 RDS 长期达人池，已分析/已维护的达人不重复抓取和打标。
4. 新达人默认只抓取 12 个视频封面，生成 1 张宫格图。
5. 宫格图上传 OSS，过程资产只写入 RDS，不写入飞书候选表。
6. 跑视频评分、达人风格、类目打标和准入轻筛。
7. 最后只把“本批需要建联的达人 + 标准话术 + 运营状态字段”写入最终飞书表。

## 入口脚本

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/creator-crm/scripts/run_excel_outreach_pipeline.py \
  --excel /path/to/kalodata.xlsx \
  --batch-id THFZ010615 \
  --product-name "本轮计划建联产品" \
  --product-category "轻上装" \
  --selling-points "日常好搭、适合低成本短视频展示" \
  --output-feishu-url "https://xxx.feishu.cn/base/xxx?table=xxx" \
  --write-feishu
```

## 常用参数

- `--max-videos 12`：默认只抓 12 个视频封面；如需灰度可设为 15。
- `--max-grids 1`：默认生成 1 张宫格图。
- `--limit 30`：只跑前 30 条，用于小批测试。
- `--offset 500`：从 Excel 的有效候选偏移后开始跑。
- `--include-manual`：最终名单额外包含 `准入决策=人工查看`。
- `--include-oss-links`：最终飞书表额外写 OSS 链接；默认不写过程资产链接。
- `--dry-run`：只解析 Excel，不抓取、不打标、不写飞书。

## 最终飞书表字段

脚本会自动确保输出表存在这些字段：

- 达人识别：`建联批次号`、`达人handle`、`达人名称`、`TikTok链接`、`Kalodata链接`
- 榜单参考：`粉丝数`、`榜单成交金额`
- 准入结果：`视频最终评分`、`主大类`、`主子类`、`达人风格标签`、`准入评分`、`准入决策`、`达人分层`、`适配类目`
- 建联执行：`计划建联产品`、`批量建联话术`、`批量建联话术本地语言`、`建联发送状态`、`达人已回复`、`进入维护状态`
- 系统追踪：`RDS达人UID`、`素材OSS链接`

## 数据沉淀

- RDS `creator_profiles`：达人主档和别名排重。
- RDS `creator_analysis`：视频评分、风格、类目、样本视频和 OSS 宫格引用。
- RDS `creator_assets`：宫格 OSS object key、过期时间、删除状态。
- 飞书最终表：只保留运营要发送消息所需字段。

## OSS 清理

已挂每日 cron：

```cron
30 3 * * * /Users/likeu3/.openclaw/workspace/skills/creator-crm/scripts/run_creator_oss_cleanup_daily.sh
```

清理脚本会根据 RDS `creator_assets.expires_at` 删除过期 OSS 宫格并标记 `deleted`。
