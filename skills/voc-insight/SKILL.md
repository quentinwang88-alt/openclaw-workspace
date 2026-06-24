# voc-insight

从 RDS 的 Fastmoss VOC 证据层读取数据，生成可复用的 **类目通用痛点**、**商品形态洞察**、**商品级视频证明点 / 文案辅助点**，供内容、短视频、达人、投流混剪、选品多个流程调用。

## 边界

**负责：**
- 读取 VOC 证据（`fastmoss_voc_enriched` / `fastmoss_voc_product_form_summary` 等）
- 判断样本是否够（形态级 / 类目级门槛）
- 按商品形态总结痛点/卖点
- 把多个商品形态的共性升级成类目通用痛点
- 给具体商品推荐可人工确认的 VOC 方向，并判断它适合视频钩子、视频辅助、文案使用还是拒绝使用
- 对 `ads_mixcut` 输出可拍摄证明动作，而不是只输出抽象卖点

**不负责：**
- 不抓网页 / 不修改 Fastmoss 抓取逻辑
- 不渲染视频 / 不直接决定投流上线
- 不绕过人工确认 VOC 方向
- 不把发货快、量多价优、客服/包装等不可视频证明的信息包装成视频主钩子
- 不把低样本形态强行总结成强结论

## 运行前

1. 确认已装驱动：`python3 -m pip install --user pymysql`
2. 确认 DB URL 在环境里：`LIKEU_AI_DATABASE_URL`（或 `VOC_INSIGHT_DATABASE_URL` / `HERMES_AGENT_DATABASE_URL`）
3. **必须清代理**（RDS 不走本地 OSS 代理）：
   ```bash
   env -u HTTP_PROXY -u HTTPS_PROXY -u http_proxy -u https_proxy python3 ...
   ```

## 命令

```bash
# 形态级（单形态 dry-run）
env -u HTTP_PROXY -u HTTPS_PROXY python3 skills/voc-insight/scripts/run_voc_insight.py \
  --batch-id FM_TH_HAIRCLIP_20260622_165607 --scope form --product-form basic_hair_clip --dry-run --pretty

# 类目级（跨形态升级 + 落库）
env -u HTTP_PROXY -u HTTPS_PROXY python3 skills/voc-insight/scripts/run_voc_insight.py \
  --batch-id FM_TH_HAIRCLIP_20260622_165607 --scope category --write

# 商品级（视频证明点 / 文案辅助点 / 风险 / 跳过原因 + 落库到 fastmoss_voc_product_recommendation）
env -u HTTP_PROXY -u HTTPS_PROXY python3 skills/voc-insight/scripts/run_voc_insight.py \
  --batch-id FM_TH_HAIRCLIP_20260622_165607 --scope product \
  --product-id 1729659517276948599 --usecase ads_mixcut --write

# ADS 钩子包（读取商品内容任务表的人工确认字段 + 落库到 voc_ads_hook_package）
env -u HTTP_PROXY -u HTTPS_PROXY python3 skills/voc-insight/scripts/build_ads_hook_package.py \
  --batch-id FM_TH_HAIRCLIP_20260622_165607 \
  --product-id 1735876226192016437 \
  --read-feishu-confirmation --ensure-feishu-fields --write --sync-feishu-status --pretty

# 命令行临时确认（不依赖飞书，适合烟测）
env -u HTTP_PROXY -u HTTPS_PROXY python3 skills/voc-insight/scripts/build_ads_hook_package.py \
  --batch-id FM_TH_HAIRCLIP_20260622_165607 \
  --product-id 1735876226192016437 \
  --confirmed-insight-id "selling_hold_quality x3" --target-hook-count 3 --write --pretty
```

## 参数

| 参数 | 必填 | 说明 |
|------|------|------|
| `--batch-id` | 是 | Fastmoss VOC 批次，如 `FM_TH_HAIRCLIP_20260622_165607` |
| `--market` | 否 | 如 `TH`，缺省自动从 pack/snapshot 推导 |
| `--category-key` | 否 | 如 `hair_clip`，缺省自动推导 |
| `--scope` | 否 | `category`/`form`/`product`，缺省按 `--product-id`/`--product-form` 推断 |
| `--product-form` | 否 | 形态，如 `basic_hair_clip` |
| `--product-id` | 否 | `scope=product` 时必填 |
| `--usecase` | 否 | `ads_mixcut`/`creator_brief`/`content_copy`/`selection` |
| `--write` | 否 | 落库（建表 + 写 artifact + 商品级写 reco 表） |
| `--dry-run` | 否 | 不落库（`--write` 缺省时默认 dry-run） |
| `--sync-feishu-task` | 否 | MVP stub，暂未实现 |
| `--pretty` | 否 | 美化 JSON 输出 |
| `--database-url` | 否 | 覆盖 DB URL |

## ADS 人工确认字段

`build_ads_hook_package.py` 默认不把 VOC 候选当成正式投流包。需要运营在「商品内容任务表」填：

| 字段 | 填法 |
|------|------|
| `VOC人工确认状态` | 填 `已确认` 才能进入 `ready_for_hook_package`；填 `驳回` 会阻断 |
| `VOC人工确认卖点` | 可填 `selling_appearance_cute_color` / `selling_hold_quality`，也可填中文方向；支持 `selling_hold_quality x3` |
| `VOC目标钩子数` | 只确认 1 个卖点时作为该卖点要生成的钩子片段数 |

程序会回写：

- `VOC钩子包状态`
- `VOC钩子包ID`
- `VOC钩子候选数`
- `VOC钩子包摘要`
- `VOC钩子包更新时间`

## 输出去向

- **stdout**：完整 JSON 结果（见 `references/output_contract.md`）
- `--write` 时：
  - `voc_insight_run`：本次运行元数据
  - `voc_form_insight_artifact`：每形态一行，`insight_payload_json` 存该形态全部洞察
  - `voc_category_insight_artifact`：每 run 一行，跨形态升级后的类目通用洞察
  - `fastmoss_voc_product_recommendation`：商品级推荐（`recommendation_id` 后缀 `_voc_det_v1`，与上游 LLM 版 `_ads_voc_reco_v0` 区分，互不覆盖），`recommendation_status=det_generated` 待人工确认
  - `voc_ads_hook_package`：商品级 ADS 钩子候选包；只有 `readiness_status=ready_for_hook_package` 才可进入正式投流混剪链路

## ADS 视频证明点

`ads_mixcut` 不再直接消费“VOC卖点文案”，而是消费以下字段：

- `usage_lane`：`video_hook` / `video_support` / `copy_only` / `reject`
- `video_fit_score`：视频可表达分，ADS 主钩子默认要求 `>=70`
- `visual_proof_zh`：镜头要证明的结果
- `required_action_zh`：必须出现的动作
- `proof_shot_list`：可拆成多个片段的证明镜头
- `forbidden_claims`：禁止表达的越权承诺

例如 `selling_hold_quality` 不会只写“夹得稳”，而会进入脚本为：

> 碎发或局部头发从松散到被夹住后更利落；模特用发夹夹住侧边碎发或局部头发，手离开发夹后轻微转头。

## 文件

```
skills/voc-insight/
  SKILL.md                       本文件
  scripts/run_voc_insight.py     确定性执行入口（含 DDL，--write 自动建表）
  references/schema.md           RDS 表结构 + 新增表 DDL
  references/scoring_rules.md    样本门槛 + 升级规则 + guardrails
  references/output_contract.md  输出 JSON 结构
```

## 设计原则（V1）

- **先 deterministic，不急上 LLM**：证据筛选、样本门槛、适用范围全部由程序规则控制。
- 等输出结构稳定后，再加 LLM 做"表达润色"和"本地语言口播化"，但**规则层不交给 LLM**。
- 所有洞察必须带 `evidence_refs`，低样本只出观察不出结论。

## 验收（MVP 已达成）

- [x] 读取一个 `batch_id`，列出每个 `product_form` 的样本状态
- [x] 生成形态级洞察（含置信度 / usecase 适用范围 / 证据引用）
- [x] 判断哪些洞察可升级成类目通用痛点（跨形态 ≥3、≥10 商品、≥80 VOC、单形态贡献 ≤60%、单商品贡献 ≤30%）
- [x] 给单个商品生成 `primary/secondary 卖点 · risk_guard 风险提示 · skipped 跳过原因`
- [x] 输出带证据引用
- [x] 低样本形态（observe_only）不生成强结论
