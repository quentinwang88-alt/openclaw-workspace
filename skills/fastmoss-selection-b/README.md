# FastMoss 选品方案 B

独立的 B 服务，负责 FastMoss 批次附件中转、SQLite 归档、规则筛选、Accio 协作、Hermes 终判和飞书轻量工作台回写。

## 飞书轻量化原则

- 飞书只做工作台，不做原始快照仓库。
- FastMoss 原始快照、标准化中间字段、规则调试字段、Accio/Hermes 明细都只保留在 SQLite。
- 选品工作台只保留决策必需字段。
- 跟进复盘表只保留轻量结果账本。

## 飞书四张表

### 参数配置表

保留字段：

- `config_id`
- `国家`
- `类目`
- `是否启用`
- `新品天数阈值`
- `总销量下限`
- `总销量上限`
- `新品7天销量下限`
- `老品7天销量下限`
- `老品7天销量占比下限`
- `视频竞争密度上限`
- `达人竞争密度上限`
- `汇率到人民币`
- `平台综合费率`
- `配饰发饰头程运费_rmb`
- `轻上装头程运费_rmb`
- `厚女装头程运费_rmb`
- `Accio目标群ID`
- `是否启用Hermes`
- `规则版本号`
- `备注`

### 批次管理表

保留字段：

- `batch_id`
- `国家`
- `类目`
- `快照时间`
- `原始文件附件`
- `原始文件名`
- `原始记录数`
- `A导入状态`
- `B下载状态`
- `B入库状态`
- `规则筛选状态`
- `Accio状态`
- `Hermes状态`
- `整体状态`
- `错误信息`
- `重试次数`
- `最后更新时间`

### 选品工作台

保留字段：

- `work_id`
- `batch_id`
- `product_id`
- `国家`
- `类目`
- `商品名称`
- `商品图片`
- `TikTok商品落地页地址`
- `上架天数`
- `7天销量`
- `最低价_rmb`
- `最高价_rmb`
- `总销量`
- `7天成交均价_rmb`
- `竞争成熟度`
- `规则总分`
- `入池类型`
- `规则通过原因`
- `推荐采购价_rmb`
- `商品粗毛利率`
- `分销后毛利率`
- `Accio备注`
- `打法建议`
- `Hermes推荐动作`
- `Hermes推荐理由`
- `Hermes风险提醒`
- `人工最终状态`
- `负责人`
- `人工备注`
- `是否进入跟进`

### 跟进复盘表

保留字段：

- `followup_id`
- `来源work_id`
- `商品名称`
- `国家`
- `类目`
- `跟进开始时间`
- `打法`
- `当前状态`
- `7天复盘`
- `30天复盘`
- `最终结论`
- `是否写回经验`
- `复盘备注`

## 命令

```bash
python3 skills/fastmoss-selection-b/run_pipeline.py init-db
python3 skills/fastmoss-selection-b/run_pipeline.py run-once
python3 skills/fastmoss-selection-b/run_pipeline.py collect-accio
python3 skills/fastmoss-selection-b/run_pipeline.py run-hermes
python3 skills/fastmoss-selection-b/run_pipeline.py sync-followup
python3 skills/fastmoss-selection-b/run_pipeline.py cleanup-archives
```

## 环境变量

- `FASTMOSS_B_CONFIG_TABLE_URL`
- `FASTMOSS_B_BATCH_TABLE_URL`
- `FASTMOSS_B_WORKSPACE_TABLE_URL`
- `FASTMOSS_B_FOLLOWUP_TABLE_URL`
- `FASTMOSS_B_HERMES_COMMAND`
- `FASTMOSS_B_DATA_ROOT`
- `FASTMOSS_B_ACCIO_TIMEOUT_HOURS`

`FASTMOSS_B_HERMES_COMMAND` 支持以下占位符：

- `{input}`: `hermes_input.json` 路径
- `{output}`: `hermes_output.json` 路径
- `{batch_id}`: 当前批次 ID
- `{profile}`: Hermes profile 名，默认 `picker`
- `{system_prompt}`: 固定 system prompt 文件路径
- `{user_prompt}`: 动态 user prompt 文件路径
- `{repair_prompt}`: JSON 修复 prompt 文件路径

示例：

```bash
export FASTMOSS_B_HERMES_COMMAND='my-hermes-picker --profile "{profile}" --system "{system_prompt}" --prompt "{user_prompt}" --input "{input}" --output "{output}"'
```

Hermes 归档目录会额外写出：

- `hermes_system_prompt.txt`
- `hermes_user_prompt.txt`
- `hermes_repair_prompt.txt`
- `hermes_output_raw.txt`

## 数据说明

- 原始 3000 条只进本地 SQLite，不回写飞书
- 飞书只承接批次状态、shortlist 决策结果和轻量复盘
- 所有中间产物会落到 `~/.openclaw/shared/data/fastmoss_selection_b/runs/<batch_id>/`
- 毛利测算默认以 `最低价_rmb` 作为售价参考，不再使用 `7天成交均价_rmb`
- 毛利测算默认扣除 `平台综合费率`，并按规则估算头程运费：`配饰/发饰=0.2`、`轻上装=2`、`厚女装=5`
- `商品粗毛利率 = (最低价 - 平台综合费 - 头程运费 - 采购价) / 最低价`
- `分销后毛利率 = (最低价 - 平台综合费 - 头程运费 - 佣金 - 采购价) / 最低价`

## 备注

当前导入器基于常见 FastMoss 列名别名做映射。如果真实样表列名略有出入，只需要在 `app/importer.py` 的 `COLUMN_ALIASES` 里补别名即可。
