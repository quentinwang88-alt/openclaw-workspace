# Creator CRM 轻筛流程改造交接报告

更新时间：2026-06-15

## 一、项目背景

本轮改造围绕 `creator-crm` skill 中的「榜单新达人轻筛」流程展开。原有 `creator-crm` 主要是完整达人画像流程：读取飞书多维表格记录，拉取 Kalodata 达人视频/封面，生成视频宫格截图，调用 LLM 做视频评分、达人风格、带货类目等分析，并回写飞书字段。

用户希望新增一个更轻量的榜单新达人初筛流程，用于在完整达人维护/建联之前，先判断达人是否值得进入后续流程。核心原则是：轻筛只做准入判断，不提前生成关系阶段、建联动作、话术草稿等二阶段 CRM 内容。

## 二、当前轻筛流程定位

轻筛现在定位为第一阶段：

1. 从榜单导入表读取候选达人。
2. 做批次内排重和历史排重。
3. 优先复用表格已有深画像字段。
4. 只输出初筛结果。
5. 不管理达人关系，不生成建联话术，不写入建联执行字段。

轻筛表应该只承担「是否值得进入下一阶段」的问题；达人维护、关系阶段、话术、建联动作等应在另一套达人维护流程和表格中处理。

## 三、核心业务逻辑

### 1. 数据来源

测试使用的飞书表：

`https://gcngopvfvo0q.feishu.cn/base/FdzGbM1b4aXG2zsr6rncFWEln3g?table=tbluJnxKyXquWcEC&view=vewPrkWWaW`

该表已有：

- `视频最终评分`
- `主大类`
- `主子类`
- `视频截图`
- Kalodata 达人链接
- TikTok 账号信息

### 2. 轻筛优先级

当前轻筛优先使用规则路径：

如果记录中已有 `视频最终评分` 且已有 `主大类` 或 `主子类`：

- 不再访问 Kalodata。
- 不再下载/生成封面宫格。
- 不调用 LLM。
- 直接基于已有字段计算准入结果。

如果缺少可复用字段：

- 优先复用飞书已有 `视频截图`。
- 调用 LLM 作为兜底分析。
- 仅在没有截图时才尝试轻量拉取 Kalodata 视频封面。

### 3. 当前准入评分公式

最新规则已改为：

```text
准入评分 = 视频最终评分 * 0.8 + 商品展示迁移性 * 0.2
```

设计思路：

- `视频最终评分` 已经包含内容质量、稳定性、画面、真人/主体展示等因素，因此权重最高。
- `商品展示迁移性` 只判断达人现有类目与当前货盘的承接关系，不应把视频质量一般的达人强行托起来。

### 4. 商品展示迁移性规则

迁移分由已有 `主大类/主子类` 推断：

- 服装、上衣、外套、裙装、裤装、内衣、睡衣、穆斯林服饰等：5
- 配饰、首饰、耳环、项链、戒指、发饰、箱包、鞋等：5
- 美妆、彩妆、香水、护肤、个护、运动服饰：4
- 居家、收纳、家居、运动户外、小家电：3
- 数码、手机、食品、饮料、其它未知：2
- 未命中特征词：3

### 5. 当前准入决策阈值

规则路径的决策逻辑：

- `视频最终评分 < 3.2`：放弃
- `商品展示迁移性 <= 2`：放弃
- `3.2 <= 视频最终评分 < 3.8`：人工查看
- `准入评分 >= 4.4` 且 `视频最终评分 >= 4.1` 且 `迁移性 >= 4`：通过，可单点建联
- `准入评分 >= 4.1` 且 `视频最终评分 >= 3.8` 且 `迁移性 >= 4`：通过，可批量建联
- `准入评分 >= 3.7` 且 `视频最终评分 >= 3.5` 且 `迁移性 >= 3`：人工查看
- 其它：放弃

注意：`可单点建联/可批量建联` 目前只作为内部 `entry_tier` 结果，不再回写飞书表的 `准入分层` 字段，避免混入第二阶段执行字段。

## 四、回写字段范围

当前轻筛只回写：

- `达人擅长内容形式`
- `内容类型`
- `适配类目`
- `准入评分`
- `准入决策`
- `准入原因`
- `准入拦截原因`
- `准入分项`

已明确不回写：

- `历史关系`
- `关系阶段`
- `当前动作`
- `本次话术草稿`
- `处理状态`
- `建联方式`
- `个性化等级`
- `准入分层`

这些字段属于达人维护/建联执行阶段，不应出现在轻筛表里。

## 五、已完成的代码改造

### 1. `run_pipeline.py`

位置：

`/Users/likeu3/.openclaw/workspace/skills/creator-crm/run_pipeline.py`

主要改动：

- 新增 `entry_screen` 运行模式。
- 新增 `--light-entry` 快捷参数。
- 新增 `--max-videos`、`--max-grids` 控制轻量抓取上限。
- 新增 `get_video_final_score()` 读取 `视频最终评分`。
- 新增 `get_existing_category()` 读取 `主大类/主子类`。
- 轻筛候选人 payload 中携带：
  - `video_final_score`
  - `main_category`
  - `sub_category`
  - `video_screenshots`
- 轻筛模式支持批次排重和历史排重。

### 2. `automation_v2.py`

位置：

`/Users/likeu3/.openclaw/workspace/skills/creator-crm/automation_v2.py`

主要改动：

- 新增 `EntryScreenAgent`。
- `AutomationOrchestrator` 支持 `mode="entry_screen"`。
- 新增 `handle_entry_screen()` 任务处理器。
- 如果已有 `视频最终评分 + 主大类/主子类`，直接走规则轻筛，不访问 Kalodata、不调用 LLM。
- 如果缺字段，优先下载飞书已有 `视频截图` 给 LLM 兜底。

### 3. `core/llm_analyzer.py`

位置：

`/Users/likeu3/.openclaw/workspace/skills/creator-crm/core/llm_analyzer.py`

主要改动：

- 新增 `EntryScreenAgent`。
- 规则路径使用 `视频最终评分 * 0.8 + 商品展示迁移性 * 0.2`。
- LLM 兜底路径也同步使用同一套 80/20 口径。
- `FeishuFieldUpdater.update_entry_screen_result()` 只写轻筛字段。
- 通过/人工查看时会清空旧的 `准入拦截原因`，避免重跑后残留上轮放弃原因。

### 4. `core/sub_agents.py`

位置：

`/Users/likeu3/.openclaw/workspace/skills/creator-crm/core/sub_agents.py`

主要改动：

- `GridGeneratorAgent` 支持 `max_grids`。
- 轻筛模式默认只生成最多 1 张宫格图，降低成本。

## 六、飞书表结构清理

本轮曾误把部分二阶段字段加到轻筛表，后续已清理。

已删除字段：

- `历史关系`
- `关系阶段`
- `当前动作`
- `本次话术草稿`
- `处理状态`
- `建联方式`
- `个性化等级`

`准入分层` 当时表里不存在，已跳过；代码也不再回写该字段。

清理后表字段数为 36。

## 七、测试记录

### 1. 单条测试

测试记录示例：

- `monut070`
- 成功复用已有截图/已有字段。
- 轻筛字段可正常回写。

### 2. 50 条测试中止

曾启动 50 条 `已完成` 记录测试，中途根据用户反馈停止。停止前成功约 12 条。问题是当时仍写入了部分二阶段字段，后续已清理字段并改代码停止写入。

### 3. 新规则前 30 条测试

命令：

```bash
python3 -u skills/creator-crm/run_pipeline.py \
  --mode entry_screen \
  --feishu-url 'https://gcngopvfvo0q.feishu.cn/base/FdzGbM1b4aXG2zsr6rncFWEln3g?table=tbluJnxKyXquWcEC&view=vewPrkWWaW' \
  --status 已完成 \
  --limit 30 \
  --workers 2
```

结果：

- 总数：30
- 成功：30
- 失败：0
- 大多数记录走规则路径。
- 少数缺可复用评分/类目的记录走已有截图 + LLM 兜底。

当时旧规则通过率过高：

- 通过：27
- 放弃：3
- 通过率：90%

### 4. 最新 80/20 规则回算

基于上一轮 30 条中的规则路径样本回算，新规则显著降低自动通过率：

- 放弃：3
- 人工查看：12
- 可批量建联：6
- 可单点建联：7

说明：

- 迁移分权重从 35% 降到 20%。
- `视频最终评分 3.2-3.7` 区间不再自动通过，多数进入人工查看。

## 八、当前可用命令

### 1. 跑轻筛

```bash
cd /Users/likeu3/.openclaw/workspace
python3 -u skills/creator-crm/run_pipeline.py \
  --mode entry_screen \
  --feishu-url '飞书表 URL' \
  --status 已完成 \
  --limit 30 \
  --workers 2
```

### 2. 干跑检查

```bash
cd /Users/likeu3/.openclaw/workspace
python3 skills/creator-crm/run_pipeline.py \
  --mode entry_screen \
  --feishu-url '飞书表 URL' \
  --status 已完成 \
  --limit 10 \
  --dry-run
```

### 3. 编译检查

```bash
python3 -m py_compile \
  /Users/likeu3/.openclaw/workspace/skills/creator-crm/core/llm_analyzer.py \
  /Users/likeu3/.openclaw/workspace/skills/creator-crm/run_pipeline.py \
  /Users/likeu3/.openclaw/workspace/skills/creator-crm/automation_v2.py
```

## 九、后续建议

1. 再跑一轮 30 条，观察新规则下实际回写分布是否接近预期。
2. 若仍然偏松，可继续提高普通通过线到 `4.2`，或要求 `视频最终评分 >= 4.0` 才自动通过。
3. 给 `人工查看` 增加更细原因，例如：
   - 视频分中等但类目匹配
   - 类目可迁移但画面质量不足
   - 缺少视频评分，需补跑深画像
4. 长期建议把轻筛结果同步到第二阶段达人维护表时再生成：
   - 关系阶段
   - 当前动作
   - 建联方式
   - 个性化等级
   - 话术草稿
5. 后续如果要大批量跑，建议先 dry-run 看候选量，再以 `--workers 2` 或 `--workers 3` 小批量推进，避免飞书写入频率过高。

## 十、重要注意事项

- 当前轻筛不是完整画像，不应替代原有视频评分和类目打标流程。
- 如果表里没有 `视频最终评分`，轻筛会退化到 LLM 兜底，结果不如规则路径稳定。
- `准入分项` 现在会写类似 `视频质量稳定4.0/迁移5`。
- 若重跑旧记录，代码会清空不再适用的 `准入拦截原因`。
- 当前表中已有历史测试结果，后续分析通过率时要注意区分旧规则和新规则回写时间。
