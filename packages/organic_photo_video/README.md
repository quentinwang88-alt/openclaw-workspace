# Organic Photo Video

TikTok 图文养号工作流的数据与方案包。旧任务仍可把 5 张图渲染为 MP4；
`native_photo` 任务直接发布有序图片，不经过视频渲染。

新增独立预设 **TH｜五套穿搭｜拆解首图**：P1 拆解 A 套，P2–P5 分别展示其他四套；候选不足时按实际 1–4 套生成，不重复凑数。使用与数据合同见 [多穿实现说明](docs/MULTI_LOOK_IMPLEMENTATION_20260904.md)。现有预设和 BGM/发布流程保持不变。

## 文件

- `docs/PRODUCT_SOLUTION.md`：产品与技术落地方案。
- `docs/DEVELOPMENT_PLAN_V1.md`：整合图组、视频化、NeoBund 热点 BGM、发布和数据闭环的完整开发方案。
- `docs/MODEL_HANDOFF.md`：供后续模型直接接手开发的背景、现状、合同、顺序与禁止事项。
- `docs/TH_PHOTO_REFERENCE_MODULE_HANDOFF_20260907.md`：TH 原生图文风格参考链的当前实现、生产入口、测试、已知缺口与接手任务。
- `migrations/001_create_opv_tables_mysql.sql`：MySQL 8 / RDS 幂等建表脚本。
- `scripts/apply_rds_migration.py`：带 SHA-256 二次确认的显式安装器。
- `domain/statuses.py`：任务/图片/渲染/发布/Outbox 状态机与受控迁移。
- `domain/models.py`：11 张 `opv_` 表的数据模型（UTC 时间、JSON 字段、Decimal/bool 转换）。
- `domain/contracts.py`：`opv-*-v1` 配置与 Plan/Shot/BGM JSON 合同校验（纯标准库）。
- `config/`：版本化 Market Pack、Theme、Render Preset JSON 与账号导入示例。
- `config/loader.py`：配置加载 + 合同校验入口。
- `repositories/rds_repository.py`：RDS 读写层，含幂等任务创建与受控状态迁移。
- `services/task_intake.py`：Stage A 幂等 Task Intake（含 idempotency key 推导合同）。
- `scripts/seed_reference_data.py`：Market Pack / Theme / Render Preset 种子脚本（默认 dry-run）。
- `scripts/import_account_profile.py`：账号资产绑定导入脚本（默认 dry-run，拒绝导入示例文件）。
- `scripts/preflight_task.py`：生成/发布前只读预检（不调用飞书）。
- `services/visual_qa.py`：单图与五图组视觉模型 QA 合同和门控。
- `scripts/run_visual_qa.py`：复用 creator-crm 视觉模型线路执行并落库 QA。
- `config/experiments/TH_BASELINE_6RUN_V1.json`：泰国基线 6 组试跑合同。
- `tests/`：标准库 unittest 套件（`python3 -m unittest discover -s tests`）。

## 建表

先预览迁移摘要：

```bash
python3 scripts/apply_rds_migration.py
```

再使用预览输出的完整 SHA-256 显式应用：

```bash
python3 scripts/apply_rds_migration.py --apply --confirm-sha256 <sha256>
```

脚本读取 `ORGANIC_PHOTO_VIDEO_DATABASE_URL`，未配置时复用 `LIKEU_AI_DATABASE_URL`。不会调用飞书 API，也不会创建种子任务。

## Phase 0 进度（2026-08-30）

已完成：

- 版本化配置：`MP_TH_DEFAULT_V1` 泰国 Market Pack、8 个泰国 Theme、`RP_STILL_VERTICAL_12S_V1` 12.5 秒渲染预设、账号导入合同与示例（示例禁止直接导入）。
- Domain：状态机常量与受控迁移、11 张表数据模型、`opv-*-v1` JSON 合同校验。
- Repository：覆盖全部 11 张表的读写；任务创建依赖 `uq_opv_task_idempotency` 幂等；状态迁移带 SQL 乐观守卫；BGM 仅按 `opv-bgm-v1` 合同存 `platform_metadata_json`。
- Task Intake：显式 key 或“账号+商品+来源+主题+选题+UTC 日期”推导 sha256 幂等键；要求商品参考图、激活的 Market Pack 和可解析的账号绑定。
- 82 个单元测试全部通过；测试不依赖真实 RDS，不调用飞书。

种子与导入（默认 dry-run，确认输出后加 `--apply`）：

```bash
python3 scripts/seed_reference_data.py            # 预览：1 pack + 8 themes + 1 preset
python3 scripts/seed_reference_data.py --apply    # 写入 RDS
python3 scripts/import_account_profile.py <账号.json>   # 校验账号绑定
python3 scripts/import_account_profile.py <账号.json> --apply
```

下一步（Phase 1）：~~Content Planner 输出 `opv-plan-v1`~~（已完成：`services/content_planner.py` + `services/asset_resolver.py` + `THEME_TH_TRAVEL_DEPARTURE_V1`，96 个测试通过，首个真实任务 `opv_task_20260830_a629e64d2dda` 已推进到 planned）、Hero-first 图片生产、单图/图组 QA 与人工审核接口。仍未解决：泰国试点账号、正式 Persona 选择、NeoBund 热点 BGM 真实字段抓取（见 `docs/MODEL_HANDOFF.md` 第 15 节）。

## 能力升级（产品驱动型图文内容）

统一入口：`services/content_story.py` 的 `generate_product_image_story(product_id, market, language, recipe_id, theme_id, variant_count)`。
内容配方（痛点解决/场景方案/视觉变化）、渲染与质检 profile、穿搭规则库见 `config/recipes/`、`config/profiles/`、`config/outfit/`。
首轮三配方验收与完整测试报告见 `docs/CAPABILITY_UPGRADE_SUMMARY_20260831.md`。

当前 V2 主链仅做技术检查：锚点生成并自动选取 → 补齐五图 → 文件解码/尺寸记录/哈希与冻结选择校验 → 渲染 → 成片媒体 QC → 回写成片。
不再调用锚点、单图、组图或成片独立视觉模型，不需要三轮人工放行。
`technical` 检查记录明确标注 `technical_only`、`visual_review_performed=false`，不伪造 human/model 审核通过。
技术完成不代表审美合格，也不代表授权发布；仍需操作者查看预览并明确勾选“确认发布”。
专项视觉诊断保留在 `scripts/run_visual_qa.py` 等独立工具，生产入口的 `--run-visual-qa` 已停用。

## 飞书图文养号生产入口

生产任务表沿用产品编码、生产预设、生成篇数、执行、进度、
预览/成片、确认发布、审核、备注，并为 V2 增加审核方式、审核阶段、重试审核和系统管理的审核批次。
RDS 仍是唯一事实源，飞书记录 ID 只作为外部
幂等键。安装与巡检命令：

```bash
python3 scripts/ensure_feishu_task_table.py
python3 scripts/run_feishu_tasks.py --dry-run
python3 scripts/run_feishu_tasks.py
```

“生成篇数”支持 1–9，历史“生成数量”字段仍可读取；V1 预设仍按原自动生成五图和视频流程执行。
V2 预设统一技术直通；同一商品的多套有效图包会按批次稳定轮换，失败重试不换包。
一行完成后如需再次生产，新增一行并重新填写数量，避免不同批次混组。

新增生产预设 `TH｜穿搭拆解首图｜均衡变体`：继续复用账号当前的 Look
穿搭模板库，P2 先生成并冻结真人、商品和完整穿搭，P1 再以程序化方式合成
“真人上身 + 目标商品 + 搭配单品 + 泰文清单”的首图。首图不调用图片模型
生成文字，不渲染价格、店铺、订单或平台 UI；批量任务会稳定轮换四种版式、
四种文案和冷暖底色。若重做 P2，系统会自动重做整组，避免 P1 与后续图失配。

V2 旧“审核方式”“通过”“重试审核”不再驱动生产；需明确勾选“执行”启动或续跑。
已有 anchor_review/image_generating 等历史任务复用当前 revision 的已生成锚点与选中素材，无需新建任务。
缺少或失败的镜头可在下一次执行补齐；哈希变化、不可解码素材应指定镜头返工。未知在途生成/渲染禁止重复提交。
技术检查失败暂停并保留成果，同批其他任务可继续。已完成任务不会因为重复巡检再次生成或审核。
飞书“审核阶段”显示技术检查/技术完成；新版枚举请运行 `ensure_feishu_task_table.py` 同步，无新增 RDS 迁移。
历史三关操作文档 [飞书 V2 接入说明](docs/FEISHU_WORKFLOW_V2_20260903.md) 仅描述旧审核模式，不再是生产默认。

历史 V1 中已经停在“待审核”的记录现在保持等待，需明确选择“通过”续跑；
V1 新建任务原有的自动生成和渲染行为不变。

### 原生图文 MVP

飞书正式操作只需选择生产预设、生成篇数和图文主题；需要参考时统一上传到
“参考图（可选）”。“参考图类型”可选自动判断、风格参考、商品参考或完整穿搭：
自动判断会把正好 `生成篇数 × 4` 张图视为每篇一组 A/B/C/D 完整穿搭，把带产品编码的输入视为商品参考，
其余输入视为风格参考。无法可靠判断时应显式选择类型。

风格参考只约束色调、层搭和氛围，不把参考图中的衣服当成必须复刻的商品。系统结合
所选主题与账号人物模板为每篇自动生成四套完整 Look，再制作五张 JPEG。生成多篇时，
系统先冻结每篇不同的内容角度、场景和配色，再为每篇建立独立 AssetSet；参考图本身
可以复用，不会被当作已经完成的帖子图片。商品参考模式按
Recipe 的短裤/裙装职责查询该产品已有完整 Look，
按篇分配尚未使用的合格 Look，不足部分再按冻结的差异方案生成新 Look，避免同一组图片只换标题；
不创建 MP4、不选择本地 BGM。“内容方案摘要”由系统回写，展示每篇的方向、场景、
配色、文案版本和素材集版本。运营不需要填写 JSON。

商品参考图是长期复用的商品事实，不按图文任务消耗。每套成功补图立即写入供给清单，
中断重试只补未完成角色。系统自动分配 A/B/C/D，运营审核四套穿搭是否成立。

完整穿搭模式要求按“第 1 篇 A/B/C/D、第 2 篇 A/B/C/D……”顺序上传 `生成篇数 × 4`
张完整造型图；系统在同一次执行中按篇分组、冻结文件、
创建不可变 AssetSet 版本并生成五页。旧“完整穿搭素材（可选）”仅保留历史兼容；
旧“商品参考图（可选）”的数据迁入统一参考图字段后删除，新任务只维护一个参考图入口。
若填写产品编码且库存不足，系统会优先从该产品旧穿搭任务中选取符合角色的已通过
完整造型图，再自动补生成剩余 Look。

原生图文只保留一个人工门槛：预览生成后勾选“确认发布”。该动作同时冻结当前五页、
标题、正文和标签并进入发布队列；系统会自动清除勾选。文件解码、尺寸、哈希和素材
关系仍由技术检查阻断异常结果。该规则仅作用于 native_photo，旧视频流程保持原审核兼容。

Recipe 至少有一份真实内容方案和一套文案即可，不再要求堆满 3 个 profile / 2 套文案。
新请求必须有 `content_card`：中文主题、用户问题、比较依据、五页职责、素材角色及明确的素材缺口。
素材适用确认保存在 AssetSet 的 `content_approval`，绑定每个源文件 SHA256，并列出可证明的服装属性和允许的内容逻辑；
本轮支持“四套不同搭配”“同外套不同下装”“逐层增加上装”等明确关系，不支持的非空规则会拒绝执行。
技术预检通过只证明这些声明与冻结文件一致，不代替内容负责人对素材和文案的判断。

有效库存按 `logic_key + 源图 SHA256 集合` 计算。同图同逻辑换标题、profile 或文案不增加库存。
数量不足时整批冻结前失败，不减量；现有 ProductionBatch 同时保留内容签名，跨批次占用由 MySQL 锁串行检查，
已冻结内容须在原行续跑。同一批次首次冻结后，修改预设、数量、Recipe、版式或素材版本不会改变续跑内容。

历史已验收样板使用 `PHOTO_TH_PICK_YOUR_LOOK_V2`（Recipe version 3）和 Choice 素材 V3；
新任务入口使用 `PHOTO_TH_PICK_YOUR_LOOK_V3`（Recipe version 7）。它在参考图生图前冻结整批具体
A/B/C/D 穿搭、配色、背景与泰文封面；V3 只要求四套 Look 不同，不再全局强制 B/C 同外套。
商品参考模式仍由目标商品事实保证四套外套一致，风格参考模式允许四套外套不同。旧 TH 温度/旅行预设停用，旧 Choice Recipe 退役。
三行旧 TH 验收的九个任务已持久标记 `CONTENT_REJECTED`，包括后来新修订在内，不能通过终审或发布交接；
任何修正须另建任务。TH_LAYERED 旧版本退休，新 V2 仅保留未获内容资格的参考图。

变量按 `variables_schema.type/values` 校验：`enum` 要求类型与值都匹配，`set` 要求无重复
的非空子集，`fixed_set` 要求完整集合。全部变量进入计划快照；只有 `asset_match_keys`
声明的视觉变量参与素材标签匹配，文案/逻辑变量不会凭空成为素材门槛。
素材集通过稳定 `asset_set_key` 查找最新启用版本，然后校验市场、品类、标签、
`asset_requirements.required_roles` 及 `required_pairs`。最新版不合适时不会回退旧版。
冻结批次按角色冻结素材顺序和字节哈希；之后素材版本退休不改变在途任务，文件被改动则停止。

四选一素材可在每个 `look_a`–`look_d` 资产上配置 `display_label: {"th-TH": "泰语名称"}`。
文案中的 `{{label_a}}`–`{{label_d}}` 在请求/计划冻结前按角色替换；缺少泰语名称时使用
`ลุค A/B/C/D`，残留或未知占位符会阻断。新 `PHOTO_CHOICE_CARD_V1` 采用严格可执行布局 v2，仅在封面
四宫格绘制 A/B/C/D 徽标，旧冻结版式保持原样。泰语文案的 `language_review_status`
为 `pending_native_review` 时，内容方案摘要显示“泰语待母语复核”；已经进入版本化生产
文案包的版本使用 `production_copy_pack`，任务侧不再重复增加语言审核步骤。

`PHOTO_TH_PICK_YOUR_LOOK_V3` 的运营文案维护在
`config/copy_packs/TH_PICK_YOUR_LOOK_V3.tsv`。一行是一套标题、正文、标签和五页文字；
Recipe 只引用 `copy_pack_id`，无需在嵌套 JSON 中维护文案。CreatOK 原生图文默认请求
TikTok 自动推荐音乐，系统记录 `platform_auto`，不会伪装成指定曲目选择。

参考图驱动的批量内容规则由开发侧版本化文件
`config/photo_planning_policies/TH_PICK_YOUR_LOOK_V1.json` 管理；运营只选择主题、参考类型、
生成篇数并上传参考图。每篇的实际服装和文案会写入“内容方案摘要”，不要求运营维护 JSON。

生成中某篇失败，保留其他篇成果并显示“需处理”。再次勾选“执行”只补齐冻结批次的缺项。
全部完成后查看五张预览，满意后直接勾选“确认发布”。
终审重试会校验并跳过已 `photo_ready` 的篇；飞书回写失败先重放已提交的投影，
不会重新生成或重复终审。缺少冻结批次的历史图文需先人工核对，禁止按当前预设猜测补单。

配置部署前运行只读预检（不会连接飞书/RDS，也不会发布）：

```bash
python3 scripts/preflight_native_photo.py
python3 scripts/preflight_native_photo.py --verify-files
python3 scripts/preflight_native_photo.py --verify-files --require-ready
```

预检区分配置错误、`NEEDS_ASSET` 和 `NEEDS_CONTENT`。`--verify-files` 额外检查本地字节、资格 hash 和文字字体。
当前只有新 TH 四选一样板 ready；未完成内容卡/源资格的 MX 和小个子方案不自动生产，旧冻结批次按原快照续跑。
`--require-ready` 会因这些缺口返回非零，不能用预检无语法错误冒充全目录内容就绪。
预检只检查配置与文件，不连接 RDS 扣除已冻结内容；实际新请求还会检查未占用库存，样板冻结后同图同逻辑不能再新建一篇。

首次部署需要应用 migration 009 并导入更新后的 Recipe、账号和市场包；仅修改本地 JSON
不会更新 RDS。以下均为显式部署命令，不属于预检：

```bash
python3 scripts/apply_rds_migration.py
python3 scripts/apply_rds_migration.py --apply --confirm-sha256 <预览输出的sha256>
python3 scripts/seed_reference_data.py
python3 scripts/seed_reference_data.py --apply
python3 scripts/ensure_feishu_task_table.py
```

`ensure_feishu_task_table.py` 会补齐预设、图文终审选项、“内容要求（可选）”与“内容方案摘要”，不要求创建
“图文任务JSON”。已有该字段时仍支持兼容覆盖：单篇填写 `{variables, copy, asset_set_id}`
的部分字段；批量填写 `{"items":[...]}` 且数量与生成篇数一致。覆盖也必须通过相同变量、
素材、角色、内容合同和库存校验，首次冻结后不再读取当前覆盖值。

素材集维护见 `config/examples/photo_asset_set_example.json`，先填真实文件及角色/标签再导入：

```bash
python3 scripts/import_photo_asset_set.py <素材集.json>
python3 scripts/import_photo_asset_set.py <素材集.json> --apply
```

高级 CLI 仍可使用完整请求，但新计划同样必须使用具有内容卡/素材资格的新 Recipe；旧示例只用于解释字段。
制作与终审分开执行；人工检查全部最终图片后，可一次终审并交接主队列：

```bash
python3 scripts/run_native_photo_task.py <图文任务.json>
python3 scripts/review_native_photo_package.py --task-id <task_id> --reviewer <运营姓名> --decision passed --enqueue
```

正式发布继续使用现有 `MainScheduleBridge → short-video-auto-publisher/app.scheduler` 主链，
SQLite 是唯一发布队列。TH/MX 的店铺路由配置、真实发布账号/渠道连接、时区、能力与账号状态
仍由现有主发布系统管理；本次没有改变渠道。`scripts/run_publish_scheduler.py` 是已默认停用
的历史独立入口，不要用它启动图文发布。账号流量评分和播放数据回收不在本期范围。

共享商品图包同步（按 SKU/颜色导入全部 active 最新版本）：

```bash
python3 scripts/sync_product_reference_packs.py --product-id <产品编码> --apply
```

若共享表尚无该商品图包，可继续用 `scripts/upsert_product_reference_pack.py`
从运营确认的本地图片建包；系统不从单张图猜测或伪造其他颜色。

素材集内容更新必须使用新 ID/版本；同 ID 只允许状态退休。新角色计划的五页可由四张源图组成：封面直接引用 look_a/b/c/d，详情页按角色引用；single 严格要求一张源图。新布局 v2 只接受导出器实际执行的 render_options，非空未知字段直接拒绝，旧 v1 仅兼容冻结历史。

未发布、未放行且未进入发布记录的图文批次可以通过 repository 的
`cancel_photo_batch(source_record_id)` 标记为 cancelled 并释放内容签名；审计行仍保留。
已放行或已进入发布流程的批次不能释放。取消后原飞书行只读，如需生产请新增一行。
