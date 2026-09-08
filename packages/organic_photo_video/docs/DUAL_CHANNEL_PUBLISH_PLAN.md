# 双通道内容分型发布改动方案（NeoBund + CreatOK）

> 需求：一个账号可同时挂 NeoBund 和 CreatOK 两个发布通道；其中一个通道只发**带货类**短视频/图文，另一个只发**不带货**图文/短视频。
> 状态：方案稿（待老板确认后实施）
> 日期：2026-09-07

## 一、现状盘点

### 1.1 排班与发布链路（skills/short-video-auto-publisher）

```
launchd 每小时 → run_pipeline --run-all（文件锁）
  ├─ 飞书拉候选：脚本表→script_metadata、run manager→video_assets（本地视频）
  ├─ account_configs：每账号单一 publish_channel（GeeLark/NeoBund/手动/暂停）
  ├─ publish_slots：槽位 = (account_id, scheduled_for) 唯一，按账号时段窗口预生成
  ├─ schedule_slots 规则选片：养号配额（nurture_enabled/daily_count/nurture_only）、
  │   同脚本不重复、同商品 24h≤2、内容族 48h 冲突
  ├─ RoutedPublishAdapter 按账号渠道路由 → create_scheduled_task（统一抽象接口）
  └─ sync_publish_results 回查 → 失败分类（撤回重排/禁账号/挂起重试）→ 飞书通知
```

- 内容分型判定已存在：`is_nurture_candidate` / `is_non_shoppable_candidate`（养号/种草/非商品展示标记→organic，其余→shoppable，非带货强制 `product_id=""`）
- NeoBund 账号能力对账：organic/shoppable 各自 quotaStatus
- OPV（图文养号流水线）走独立 RDS 状态机 + 自己的 NeoBund 适配，已验证发图文合成视频

### 1.2 CreatOK 能力（CLI v0.13.0，API key 已在 workspace .env）

实测连接（3 个）：

| connection_uid | 账号 | platform | 能力 |
|---|---|---|---|
| conn_ts_4zDg… | yoursbeauty.my | tiktok_shop | publish_shoppable_video、showcase 管理 |
| conn_tc_3QnZ… | LikeU shop | tiktok_content_posting | publish_content_video、**publish_content_photo** |
| conn_tc_…（tamarawoo） | tamarawoo | tiktok_content_posting | publish_content_video、**publish_content_photo** |

关键能力差异（决定通道分工）：

| 能力 | NeoBund | CreatOK |
|---|---|---|
| 带货短视频 | ✅（authType=1，现役） | ✅ Shop video（挂商品+TikTok 音乐库） |
| 非带货短视频 | ✅（authType=2，OPV 在用） | ✅ Content Posting video |
| **图文（photo mode）** | ❌ 不支持 | ✅ Content Posting photo |
| 挂 TikTok 音乐 | ✅（已抓包验证） | ✅ Shop 走音乐库；Content Posting 音乐能力待验证 |
| 定时发布 | ✅ scheduledReleaseTime | ✅ envelope 支持立即/定时+时区 |
| 防重复 | 列表回查（已验证） | ✅ idempotency_key + status 恢复（官方红线同样禁止盲重提） |
| 接入方式 | HTTP 适配器（已有） | `creatok` CLI 子进程（skill 规定禁止直调 HTTP） |

### 1.3 差距（现状 → 目标）

1. `account_configs.publish_channel` 是**单值**——一账号只能挂一个通道
2. 调度器选片后按账号单渠道创建任务，没有「内容类型 → 通道」的匹配层
3. 没有 CreatOK 适配器（现有：GeeLark/NeoBund/Http/DryRun）
4. 没有账号 ↔ CreatOK connection_uid 的映射
5. 图文（photo mode）类候选在现有 pipeline 中不存在（video_assets 只有视频）

## 二、设计方案

### 2.1 核心模型：账号级「通道绑定」替代单渠道

`account_configs` 新增 `channel_bindings_json`（保留 `publish_channel` 作默认/兼容）：

```json
{
  "channel_bindings": [
    {"channel": "NeoBund", "content_scope": "shoppable"},
    {"channel": "CreatOK", "content_scope": "organic"}
  ]
}
```

- `content_scope`: `shoppable`（带货视频/图文）/ `organic`（非带货视频/图文）
- 同一账号可绑定多个通道，但**同一 content_scope 只允许绑定一个通道**（约束写入配置校验，避免同型内容双通道竞争）
- 飞书账号表加「带货通道/非带货通道」两列（沿用现有 ensure-field 模式同步），CLI `--channel-binding-overrides` 提供运维覆盖

**推荐分工**（基于实测能力）：

| 通道 | content_scope | 理由 |
|---|---|---|
| NeoBund | shoppable | 带货视频链路现役且成熟；配额/能力对账已有 |
| CreatOK | organic | 独有 photo mode 图文能力；Content Posting 即「不带货」原生场景；idempotency 恢复纪律与现有防重红线一致 |

> 若想反过来（CreatOK 发带货、NeoBund 发养号）也支持——配置层面自由，但 CreatOK shop 连接目前只有视频能力且音乐走 TikTok 库而非热榜接口，带货图文仍只能落 CreatOK；方案按「分型」设计而不是写死通道。

### 2.2 数据库改动（SQLite，全部增量）

```sql
-- account_configs
ALTER TABLE account_configs ADD COLUMN channel_bindings_json TEXT NOT NULL DEFAULT '';

-- publish_slots：记录该槽位实际使用的通道（回查/撤回路由用）
ALTER TABLE publish_slots ADD COLUMN publish_channel_used TEXT;

-- 新表：账号 ↔ CreatOK 连接映射（auth 复用 .env 的 CREATOK_API_KEY）
CREATE TABLE IF NOT EXISTS creatok_connections (
    account_id TEXT PRIMARY KEY,
    connection_uid TEXT NOT NULL,
    platform TEXT NOT NULL,            -- tiktok_shop | tiktok_content_posting
    capabilities_json TEXT NOT NULL DEFAULT '[]',
    health TEXT,
    last_checked_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
```

`channel_bindings_json` 为空时回退 `publish_channel` 单渠道行为——**存量账号零感知**。

### 2.3 CreatOKPublishAdapter（新适配器，实现 BasePublishAdapter）

位置：`app/creatok_publish.py`，包装 `creatok` CLI 子进程（遵守 skill「禁止直调 HTTP」红线）：

```python
class CreatOKPublishAdapter(BasePublishAdapter):
    def create_scheduled_task(self, *, account_id, video_path, title,
                              publish_at, script_id, product_id="",
                              product_title="", mark_ai=None,
                              media_type="video",      # video | photo
                              music_selection=None) -> str:
        # 1. account_id → creatok_connections.connection_uid
        # 2. creatok assets create（本地媒体 → 正式 Asset object_key）
        # 3. 分型：
        #    shoppable → prepare-video → products 选择 → (可选 music)
        #                → envelope(publish_kind=shop_video)
        #    organic   → envelope(publish_kind=content_video|content_photo,
        #                privacy/interaction 取 creator-info 约束默认值)
        # 4. 定时字段：envelope 带时区（默认账号市场时区，如 Asia/Bangkok）
        # 5. idempotency_key = "svp-" + script_id（稳定键，失败可恢复）
        # 6. creatok publish submit --file envelope.json
        # 7. 返回 "creatok:<job_id>"；失败读 error.result_unknown：
        #    result_unknown → 抛 RetryableCreateError（挂起重排，绝不重提）
    def query_task_status(self, *, task_id, scheduled_for): ...
    def terminate_task(self, *, task_id): ...   # 撤回（如 CLI 支持 cancel，需验证）
```

关键实现纪律（对齐 CreatOK skill 红线）：

- **idempotency**：每条 envelope 一个稳定 `idempotency_key`；超时/未知结果只允许 `publish status --idempotency-key` 查询恢复，映射到现有「挂起重试」而不是重复提交——与 NeoBund 的空响应回查同构
- **自动确认授权**：CreatOK skill 的 confirmation gate 是人工纪律；自动化接入前需老板批准一张**自动提交白名单**（账号 × publish_kind × 商品范围），适配器只对白名单内 envelope 免确认提交，白名单存本地配置、改动需显式确认
- 音乐（仅 Shop）：`music_selection` 复用 OPV 的 BGM 选歌产物结构，但字段换成 CreatOK 的 `music_id`（TikTok 音乐库）；**organic Content Posting 暂不挂音乐**（能力待验证，先原声）

### 2.4 调度器改造（schedule_slots）

改动集中在「选片 → 建任务」之间插入**通道匹配**：

```
候选判定 content_type = shoppable | organic（现有函数，不改）
  ↓
slot 生成不变（账号 × 时段）
  ↓
选中的候选 × 账号绑定通道求交：
  - candidate.shoppable + binding(NeoBund, shoppable) → NeoBund 适配器
  - candidate.organic   + binding(CreatOK, organic)  → CreatOK 适配器
  - 无匹配绑定 → 该候选跳过（记 blocked_by_rules 原因"无该类型内容的发布通道"）
  ↓
create_scheduled_task(..., 通过 RoutedPublishAdapter 新增的
                      create_scheduled_task_for_content_type  或直接选适配器)
publish_slots.publish_channel_used 记录实际通道
  ↓
sync_publish_results / terminate 按 task_id 前缀路由（"creatok:" → CreatOK 适配器，
现有 task_prefix_adapters 机制直接可用）
```

`RoutedPublishAdapter` 扩展：新增按 (account_id, content_type) 选择适配器的方法；`_normalize_channel` 增加 `CreatOK`。养号配额、去重规则**不变**（它们本来就是按账号计的，天然跨通道生效）。

### 2.5 图文（photo mode）候选支持

分两步：

1. **本方案内（最小）**：`script_metadata` 增加 `media_type` 字段（video|photo），飞书脚本表同步加列；photo 候选只允许路由到 CreatOK binding；photo 的素材走 `video_assets` 同表存本地路径（复用下载/OSS 逻辑）
2. **后续（单独方案）**：OPV 内容包直出 TikTok photo mode 帖（5 图原生图文而非合成视频）——`opv_content_package.selected_image_ids_json` 已具备图片清单，CreatOK `publish_content_photo` 是发布端，中间补一个「内容包 → photo envelope」转换器即可

### 2.6 OPV 的位置

- OPV 现有 NeoBund organic 发布链路不动（BGM 热榜挂载已在 NeoBund 验证）
- `OpvPublishFlow` 的适配器接口与 `BasePublishAdapter.create_scheduled_task` 同构，后续加 CreatOK 绑定即可让 OPV 内容走 Content Posting（无音乐版）——本方案不做，仅保证接口兼容

### 2.7 失败处理与状态回查

| 场景 | 行为 |
|---|---|
| CreatOK 提交超时/result_unknown | 槽位挂起（mark pending reason），`publish status --idempotency-key` 恢复；绝不重提（同 NeoBund 红线） |
| definite failed | 记 failure_detail，槽位取消（同现有非重试错误路径） |
| connection health 非 ready | capabilities 对账时打标，调度跳过该通道并计数（类似 NeoBund quotaStatus 机制） |
| 撤回 terminate | NeoBund 已验证；CreatOK cancel 能力需实测（无则记录「不支持撤回」并在 UI 提示） |

## 三、实施顺序（4 步，每步可独立回滚）

| 步骤 | 内容 | 交付物 | 风险 |
|---|---|---|---|
| 1. 配置与模型 | channel_bindings 列 + 建表 + 飞书列 + 配置校验 + 通道绑定同步 | 迁移脚本 + 单测 | 低（纯增量，空值回退旧行为） |
| 2. CreatOK 适配器 | creatok_publish.py + connection 对账命令（复刻 capabilities.py 模式）+ 自动提交白名单 | 适配器 + 单测（mock CLI） | 中（需实测一条定时任务验证 envelope/时区） |
| 3. 调度接入 | 通道匹配层 + publish_channel_used + task 前缀路由 | schedule_slots 改造 + 单测 | 中（选片规则不变，仅路由变化） |
| 4. 试点上线 | 选 1 个账号（建议 LikeU shop：两边都已连接）双通道配置，观察 3 天 | 试点报告 | 低（单账号，可随时回退单渠道） |

**试点验证项**：同账号两通道同日并发的额度表现、 CreatOK 定时时区正确性（注意 NeoBund 是北京时间解释的教训）、photo mode 首帖、result_unknown 恢复演练。

## 四、需要老板拍板的 4 个决策

1. **通道分工确认**：推荐 NeoBund=带货、CreatOK=不带货（图文/视频）；还是反过来？
2. **自动提交白名单边界**：哪些账号 × 内容类型允许免人工确认自动发？（建议先只开试点账号的 organic 类）
3. **带货图文走哪**：CreatOK Shop 目前只有 video 能力——带货图文暂不可行，是否接受「带货=视频、不带货=视频+图文」的落地形态？
4. **试点账号**：LikeU shop（NeoBund authId 4834 + CreatOK conn_tc_3QnZ…，两边连接都已就绪）是否合适？

## 五、明确不做的事

- 不改养号配额/内容去重规则（跨通道天然共享，按账号计）
- 不动 GeeLark 现役链路（channel_bindings 为空的账号完全走老路）
- 不做 CreatOK HTTP 直连（skill 红线：只能 CLI）
- 不在本方案内实现 OPV → CreatOK 发布与 Content Posting 音乐（后续单独验证）

---

# 实施记录（2026-09-07，用户四决策落地）

用户决策：① 不按渠道硬编码类型，账号选两个渠道时限定互发不同类型（由表格「内容类型限定」列决定，一边留空自动补全，两边冲突/全空则不启用双通道）；② 无独立白名单，授权完全以账号表格设定为准；③ 接受带货=视频、不带货=视频+图文；④ 试点账号 tocrystal66。

实施（增量于并行会话已有的 creatok_publish.py / 能力列 / photo 管道之上）：

1. `account_channel_bindings` 表（account×channel 主键，content_scope + 各通道独立发布时间窗）+ `publish_slots.publish_channel_used`
2. 槽位生成：有绑定的账号按每绑定的时间窗分生成并打通道标记；存量待排期槽位按时间窗回填标记
3. `sync_accounts` 拆绑定：两行同账号 → 两绑定；互补校验（填一边自动补另一边；同型冲突或全空 → 不启用，回落单渠道）
4. 调度：槽位按 `publish_channel_used` 直选适配器（RoutedPublishAdapter._adapter_for_channel）；候选按绑定 scope 过滤（shoppable 槽只选带货候选，organic 槽只选养号/种草候选；photo 候选暂仅 organic）
5. 飞书「内容类型限定」单选列（带货/不带货）自动创建；账号表即授权源（无独立白名单）
6. 测试：新增 tests/test_dual_channel.py（9 用例），全套 316 绿

试点 tocrystal66 已配置并验证：NeoBund=带货（13:25/21:30）、CreatOK=不带货（15:00/22:00，连接 conn_tc_3QnZcdS70fNLC9imLbQmRg / Asia/Bangkok / 直接发布）；绑定与槽位标记均落库，存量槽位已回填。
