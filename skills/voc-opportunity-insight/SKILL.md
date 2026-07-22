---
name: voc-opportunity-insight
description: 独立分析VOC消费者需求、产品方向、潜在产品机会、规格要求和风险，并将机会卡、信号聚合、规格风险、原子证据同步到指定飞书多维表格，将总览报告发布为飞书知识库文档。用于“分析VOC产品机会”“输出消费者洞察”“把VOC表格写入飞书”“创建VOC报告”“刷新VOC机会池”等请求；不用于ADS卖点、商品选品打分、找货或推荐测品。
---

# VOC Product Opportunity Insight

运行一条与 ADS、选品评分和找货解耦的 VOC 情报链路：

```text
富化VOC -> 原子证据 -> 信号聚合 -> 产品机会卡 -> 规格风险库
       -> 多维表格同步 + 飞书知识库报告
```

## 固定边界

- 只输出消费者需求、产品方向、产品概念、规格和风险参考。
- 商品 ID 仅表示证据来源，不表示商品优先级。
- 不写 `product_potential_score`、商品动作、找货任务或 ADS 表。
- 分析阶段始终 dry-run；当前 `run_voc_opportunity.py --write` 必须保持禁用。
- 飞书同步不得清空目标表；按“唯一键”幂等更新，并优先复用空白行。
- 默认把用户提供的原数据表改为 `01_VOC机会看板` 并只保留机会卡；另建规格、信号、原始证据和后台全量数据表。必须先核验后台全量表数量，再清理主表明细。

## 默认目标

```text
多维表格:
https://gcngopvfvo0q.feishu.cn/wiki/DMIYwxge7iV8sAktvjDcZx84nzQ?table=tbl3wdy3DTHdjHlH&view=vewFYyTCLh

报告父节点:
https://gcngopvfvo0q.feishu.cn/wiki/Qe8rwXS8uiiHsukBGVMcInHBnO2
```

可以通过 CLI 参数覆盖，但不得凭空替换用户明确提供的目标。

## 快速运行

### 已有富化结果JSON

```bash
python3 skills/voc-opportunity-insight/scripts/run_pipeline.py \
  --input-json skills/voc-insight/output/FM_MX_WIGS_COMBINED_20260717_wigs_voc_dryrun.json
```

### 单个RDS批次

数据库访问前清除代理：

```bash
env -u HTTP_PROXY -u HTTPS_PROXY -u http_proxy -u https_proxy \
python3 skills/voc-opportunity-insight/scripts/run_pipeline.py \
  --batch-id VOC_BATCH_ID
```

### 只重新同步已有分析结果

```bash
python3 skills/voc-opportunity-insight/scripts/sync_voc_outputs_to_feishu.py \
  --result-json skills/voc-insight/output/BATCH_voc_opportunity_result.json \
  --report-markdown skills/voc-insight/output/BATCH_voc_opportunity_overview.md
```

## 执行顺序

1. 运行相邻 `skills/voc-insight/scripts/run_voc_opportunity.py`。
2. 确认质量门 `passed=true`、`database_written=false`、禁止字段为0。
3. 读取多维表格字段；只创建缺失字段，不修改已有字段类型。
4. 创建飞书知识库子文档并分批写入报告。
5. 构建四类表格记录：
   - `机会卡`
   - `信号聚合`
   - `规格风险`
   - `原子证据`
6. 按 `唯一键` 更新已有行，优先复用空白行，再批量新增。
7. 中文化产品形态、机会类型、置信度、信号标签、规格类型和证据标签。
8. 配置五张用途数据表，将现有链接对应的数据表设为仅显示机会卡的默认入口。
9. 回读记录并验证四类数据数量、报告链接和唯一键覆盖。

## 失败处理

- 分析质量门失败：停止发布，保留本地产物。
- 飞书登录/权限失败：不得改用浏览器内部写接口硬绕；若仅知识库缺少创建子节点权限，可降级创建独立飞书文档并明确返回警告。
- 字段创建部分失败：停止写记录，输出缺失字段清单。
- 批量写入失败：已成功批次保留；重跑通过唯一键恢复，不产生重复数据。
- 报告创建成功但表格失败：返回报告链接和表格错误，重跑时允许创建新报告版本但表格仍幂等。

## 输出映射

需要修改表格字段或排查同步时，读取 [references/feishu_output_mapping.md](references/feishu_output_mapping.md)。

需要修改分析结构、置信度或机会卡时，读取相邻文件：

```text
../voc-insight/references/voc_opportunity_output_contract_v2.json
../voc-insight/references/voc_analysis_application_refactor_v2.md
```

## 验收

- 本地机会质量门通过。
- 飞书文档可打开且正文非空。
- 多维表格四类数据均存在。
- `唯一键` 无重复。
- 重跑后记录总数不因重复写入增长。
- 表格和报告中不出现具体商品推荐动作、选品分或找货状态。
