# 种草不挂车分支架构

## 边界决定

系统采用一个代码仓库、一个媒体生产平台、两个业务边界：

```text
共享生产内核
├── 产品事实 / 中央治理卖点只读快照 / 商品身份与负向约束
├── 通用模型传输 / JSON / 日志 / 重试
├── VisualIntentContract / 素材与媒体执行
└── 通用物理与语言安全规则
    ├── DIRECT_RESPONSE
    │   ├── 带货卖点投影、购买理由语义主线
    │   ├── 带货钩子、带货口播、带货质检
    │   └── 允许挂车的发布策略
    └── SEEDING_ORGANIC
        ├── OrganicClaimAdapter、故事候选与种草语义主线
        ├── 原生口播、广告感与体验真实性质检
        └── 禁止挂车的发布策略
```

公共层不得出现 `selling_argument`、`core_buying_reason`、`seed_theme` 或 `viewer_payoff`。业务层只通过 `VisualIntentContract` 向视觉生产层交付意图，并通过冻结的 `PublishPolicyContract` 向下游交付发布约束。

## 代码落点

- `core/content_branches/contracts.py`：分支中立合同与版本指纹。
- `core/content_branches/registry.py`：小型分支注册中心。
- `core/content_branches/direct_response/`：现有原创带货线的兼容适配器。
- `core/content_branches/organic_seeding/`：种草规划、视觉提示、口播提示、质检、渲染和独立执行器。
- `core/content_branches/storage.py`：可注入数据库路径的公共运行存储。
- `core/organic_seeding_feishu.py`：种草专属飞书任务表和审核表 Schema。

## 隔离保证

- 两条线的数据库文件、进程锁、脚本 ID、策略版本和提示词版本分开。
- 种草飞书入口没有默认表地址，不能误读原创带货表。
- 种草同步使用独立 source adapter；人工表只维护 `发布策略=种草不挂车`，适配器在内部展开四项发布元数据。策略缺失或冲突时 fail closed。
- 商品编码在种草同步中只进入 `全球产品ID` 供内部分析，可挂车 `产品ID` 留空。
- 发布器再次检查脚本来源、发布用途、内容分支和挂车标记；任一不挂车信号都会清空平台商品 ID。

## 当前执行顺序

```text
种草运营任务
  -> 商品视觉事实与中央卖点快照冻结
  -> OrganicClaimAdapter（一个核心价值 + 可选支持事实）
  -> 8–12 个故事候选软排序
  -> OrganicStorySpine / OrganicSeedThemeContract
  -> VisualIntentContract
  -> 独立视觉蓝图调用
  -> 独立种草口播调用
  -> 事实 / 体验 / 广告感 / 商业元素质检
  -> 种草生产脚本审核表（默认不进入生产）
  -> seeding-batch 同步（fail closed）
  -> 共享视频执行与发布器不挂车保护
```

视觉蓝图和口播是两个独立模型调用，但都消费同一份 `OrganicStorySpine`。种草分支只读中央治理卖点快照，随后由分支独占的 `OrganicClaimAdapter` 清洗为产品事实、证据强度和种草角色；不会调用带货卖点投影、销售论证包或 `creative_full_single_v1`。

中央卖点进入种草分支前必须遵守：共享 canonical claim、来源、证据要求和允许强度；隔离 `primary_selling_point`、销售脚本角色、CTA、价格、购物车和转化评分。每条脚本只开放一个核心价值与至多一个同主题支持事实。

## 精简工作台

- 运营任务表 18 列：只保留商品输入、目标市场、内容要求、体验授权、事实约束和执行结果。
- 生产脚本表 21 列（含首帧图）：只保留审核需要的脚本内容、质检摘要、单一发布策略和生产同步状态。
- `时长` 在任务侧固定为 15 秒；`商品角色 / 露出时机` 由规划器自动轮换。
- 质检细项合并为 `质检结果`，但数据库仍保留结构化评分；表格精简不改变内部审计能力。
- 商业语言质检采用“结构化标记硬阻断 + 发布字段语境扫描 + 一次受控修订”。否定性禁令不会被当成 CTA；失败证据保留字段路径、命中词和上下文。
- 精确续跑只改变本次执行集合，不改变脚本内容身份；已通过槽位无需重复调用模型。
