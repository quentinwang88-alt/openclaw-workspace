# 产品驱动型图文内容：开发与测试总结

> 轮次：2026-08-31（能力升级方案落地 + 三配方真实验收走查）
> 前置文档：`DEV_ROUND_SUMMARY_20260830_31.md`（OPV 基础流水线 Phase 0-3）
> 本轮目标：从「根据主题生成 5 张图」升级为「以产品为锚点，通过内容配方规划钩子、穿搭和叙事，生成可复用的结构化图文内容包」

## 一、能力升级落地内容（第一阶段：规划解耦）

### 1.1 数据库（迁移 002，已应用 RDS）

- `opv_content_task` 新列：recipe_id / recipe_version / content_goal / hook_strategy / outfit_plan_json / storyboard_version / content_package_id / product_facts_json
- `opv_content_shot` 新列：narrative_function / product_focus / overlay_spec_json / transition_hint / continuity_constraints_json
- 新表：`opv_content_recipe`（内容配方）、`opv_render_profile`（渲染 profile）、`opv_quality_profile`（质检 profile）、`opv_content_package`（内容包）
- 安装器增加 `_opv_schema_migrations` 台账：ALTER 不可重放，已应用文件按名跳过

### 1.2 内容配方（config/recipes/，已入 RDS）

| recipe_id | content_goal | 叙事五层 | anchor_slot | 适用 |
|---|---|---|---|---|
| RECIPE_PAIN_POINT_SOLUTION_V1 | 痛点解决 | HOOK=痛点+结果钩子 → CONTEXT=普通穿法问题 → TRANSFORMATION=正确搭配 → PROOF=细节证明 → PAYOFF=完整效果 | P1 | 小个子/梨形/遮肉/腿型/比例 |
| RECIPE_SCENE_SOLUTION_V1 | 场景方案 | HOOK=场景需求 → CONTEXT=基础搭配 → TRANSFORMATION=场景层次 → PROOF=鞋包配饰 → PAYOFF=完整造型 | P1 | 旅行/通勤/约会/咖啡/雨天/校园 |
| RECIPE_VISUAL_TRANSFORM_V1 | 视觉变化 | HOOK=Before/After → CONTEXT=主色基础 → TRANSFORMATION=变化过程 → PROOF=细节统一 → PAYOFF=最终 Look | **P5**（先出最终造型再反向生成） | 同色系/一件多穿/普通到高级 |

配方不绑定账号；账号测试自由组合 Recipe × Theme。

### 1.3 新增服务

- `services/outfit_planner.py`：Product Facts（产品事实锁定；颜色/版型/材质等禁改特征显式列出，未知识别标 unknown 不瞎编）+ 结构化 Outfit Plan（styling_logic 字段回答"为什么这样搭/产品角色/比例/配色/场景适配"）
- `services/content_planner.py` v2：配方驱动规划。叙事弧来自 Recipe、场景与话题来自 Theme、时长按 Render Preset 归一化、转场按 Preset 白名单强制；persona 走账号 allow-list + TASK_FROZEN 锁 + 结构化快照哈希；plan 内含 render_contract（preset_id/目标时长/fps）。无 recipe 的任务走旧主题路径，完全向后兼容
- `services/content_package.py`：内容包生命周期 planning → generating → qa_review → ready → rendered（任意非终态可 invalid）；发布系统只消费 ready/rendered
- `services/hero_first.py` 升级：Anchor-frame-first——anchor_slot 由 Recipe 决定（默认 P1，视觉变化型锚 P5），锚点图先出，锁定人物/产品/穿搭/场景后再生其余 4 张；单张失败只重生成对应 slot
- `services/content_story.py`：统一入口 `generate_product_image_story(product_id, market, language, recipe_id, theme_id, variant_count)`，内部完成 Intake → Product Facts → Outfit Plan → Recipe 规划 → Content Package；带断点续跑语义（已完成阶段自动跳过）
- `repositories`：recipe/profile/package 读写 + task/shot 新列支持

### 1.4 配置实体

- `config/profiles/IMAGE_STORY_VIDEO_V1.json`：9:16 / 10-15s / 30fps / H.264 / 首镜钩子窗口 2s / 允许 cut、遮镜、卡点
- `config/profiles/QUALITY_STANDARD_V1.json`：五维质检维度（产品保真 / 穿搭质量 / 人物一致性 / 叙事质量 / 组图质量），各维度独立记录、独立判定重生成对象，不出单一总分
- `config/outfit/OUTFIT_RULES_TH_V1.json`：按内容键的穿搭基线库（底装/鞋包/配色/风格方向）+ 气候规则 + 产品可见性硬约束

## 二、与并行开发的合并

本轮期间另一会话在同一包内落地了方案第二阶段的部分内容：`services/visual_qa.py`（五维视觉 QA，creator-crm 视觉模型路由）、`services/preflight.py`（首镜预检）、`services/locale_quality.py`（文案本地化质检）、persona allow-list + TASK_FROZEN 锁、render_contract、主题时长按 preset 归一化，以及配套测试规格。

合并要点：

- 双方测试规格全部满足（并行会话的 TDD 规格 + 本方配方驱动架构）
- 账号 `OPV_TH_TEST_001` 的 operating_rules 由并行会话更新：visual_qa_required=true、allowed_persona_refs 换为 SELECTED 系列（原始人设模板库新选 3 个 persona，各 1 张参考图）
- 教训：同一包并行开发冲突成本高，改 content_planner 前必须重读文件；建议后续单线推进或先划分文件所有权

## 三、测试情况

### 3.1 单元测试

174 个用例全部通过（unittest，无需真实 RDS/模型），覆盖：状态机、模型往返、合同校验（含新增的 product-facts/recipe/outfit-plan/content-package/render-profile/quality-profile）、配置加载（3 recipes + 2 profiles 随种子校验）、规划器（配方路径 + 旧主题路径 + 幂等 + allow-list + fastcut 归一化）、内容包生命周期、统一入口（账号匹配/快照必填/幂等）。

### 3.2 三配方真实验收走查（同一产品：1737141103233042426 浅蓝短款蓬松外套）

统一入口调用，TH/th-TH，账号 OPV_TH_TEST_001（@ambalalala2）：

| # | Recipe × Theme | Look | Persona | 5 图 | 视觉 QA | 视频 |
|---|---|---|---|---|---|---|
| 1 | 痛点解决 × 小个子显高 | STYLE_OPV_PUFFER_PETITE_001（高腰阔腿显高） | SELECTED_01 黑发轻精致 | 5/5 通过 | ✅ 86 分 | ✅ 10.00s QC 全过 |
| 2 | 场景方案 × 咖啡约会 | STYLE_OPV_PUFFER_TRAVEL_001（白裤机场） | SELECTED_02 黑发自然 | 5/5 通过 | ✅ 89 分 | ✅ 10.00s QC 全过 |
| 3 | 视觉变化 × 一衣多穿 | STYLE_OPV_PUFFER_MULTIWAY_001（连衣裙多穿） | SELECTED_03 棕发甜感 | 5/5 通过 | ✅ 84 分 | ✅ 10.00s QC 全过 |

- 3 个内容包均到 `rendered`，3 个任务停 `video_review` 待人工终审，**均未发布**
- 视频路径：`~/.openclaw/shared/data/organic_photo_video/<task_id>/renders/<task_id>_v1.mp4`
- 走查中验证了断点续跑：进程被杀、网络超时后重跑均从断点继续，零重复生成、零重复任务

### 3.3 验收标准对照（方案第十五节）

| 标准 | 结果 |
|---|---|
| 1 结构化产品快照 | ✅ product_facts 落库 |
| 2 同一产品多主题穿搭 | ✅ 3 组合实跑 |
| 3 策略/穿搭/分镜明确 | ✅ recipe/outfit_plan/plan_json 分层落库 |
| 4 P1 视觉钩子 | ✅ 配方叙事弧定义（首镜预检 preflight 已有） |
| 5 五图完整叙事 | ✅ 五层叙事弧 + 视觉 QA 叙事维度 |
| 6/7 产品/人物/穿搭一致 | ✅ 视觉 QA 三组合全部通过 |
| 8 内容包间真实差异 | ✅ 不同 look/scene/persona/叙事弧 |
| 9 封面文字独立 | ⏳ 文案方案与 copy_writer 已有；程序化文字渲染在第二阶段 |
| 10 单张失败单独重生成 | ✅ shot_version 机制（已有） |
| 11 图片包独立于视频 | ✅ 内容包 ready 即可用 |
| 12 一包多渲染版本 | ✅ 渲染行版本化（v1→v2 已验证） |
| 13 重复执行不重复创建 | ✅ 幂等键 + 同日重跑实测 |
| 14 全链路追溯 | ✅ task 含 recipe/outfit/product_facts/package，package 含 lineage |

## 四、走查中发现并修复的问题

1. **facade product_id 注入层级错误**：注入到嵌套 product 块而非顶层快照，导致产品事实为空——修复为顶层注入
2. **`update_task_plan` 缺新列**：配方落库时 TypeError——扩展支持全部 8 个新字段
3. **`core` 命名空间互抢**：openai-image 与 creator-crm 都有 `core` 包，后导入者劫持 sys.modules 导致对方 ModuleNotFoundError——双方改为模块级缓存 + purge 自愈重试，首次导入后互不干扰
4. **approve_group 无事务保护**：多步写入（shots→feedback→group_qa→package→task）中途因 RDS 连接超时中断，留下"shots 已 approved 但任务/包未推进"的中间态——本次手动补完；事务性改进列入待办
5. **发现（未修）**：NeoBund `scheduledReleaseTime` 按北京时间解释而非账号本地时间（上一轮已发现，本轮再次确认）

## 五、已知限制与待办

1. **视觉 QA 耗时**：creator-crm doubao 视觉路由单任务 13-22 分钟，是流水线最慢环节；可评估并发单图评审或更快路由
2. **程序化文字渲染**：封面标题仍不烧入图片（文案确认后叠加），叠加器待开发
3. **variation_plan**：多变体变化轴未实现，facade 的 variant_count 目前生成 N 个独立任务
4. **approve_group 事务性**：多步写入需包事务或幂等重入设计
5. **Recipe × 账号绑定矩阵**：账号测试自由组合已可用，运营侧的绑定配置界面待做
6. **Persona 参考图**：SELECTED 系列各仅 1 张，转 active 前需补 3-5 张
7. 三条新视频待老板终审；发布需明确指令（NeoBund 流程已验证）

## 六、当前调用方式

```python
from repositories.rds_repository import RdsRepository
from services.content_story import generate_product_image_story

repo = RdsRepository.from_env()
results = generate_product_image_story(
    repo,
    product_id="1737141103233042426",
    market="TH", language="th-TH",
    recipe_id="RECIPE_SCENE_SOLUTION_V1",
    theme_id="THEME_TH_CAFE_DATE_V1",
    variant_count=1,
    product_snapshot={
        "product_name": "...", "category": "outerwear",
        "reference_images": ["...", "..."],
        "planned_persona_ref": "...", "planned_look_ref": "...", "planned_scene_ref": "...",
    },
    asset_reader=LightTryonAssetReader(),
)
# 后续：HeroFirstProducer.produce → VisualQaService.review_task
#       → VideoRenderFlow.approve_group / render（内容包自动推进）
```

命令行等价物：`scripts/run_generation_task.py`（单任务生成+视觉 QA）、`scripts/confirm_publish_result.py`（发布回查）。
