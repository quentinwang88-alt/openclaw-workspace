# OPV 开发轮次总结（2026-08-30 ~ 2026-08-31）

> 本轮从 Phase 0 代码骨架一直推进到首条内容真实提交 NeoBund 定时发布，并完成快节奏改版。
> 全程 RDS 唯一事实源，零飞书依赖，未输出任何凭据。

## 一、里程碑总览

| 阶段 | 交付 | 状态 |
|---|---|---|
| Phase 0 配置与骨架 | 状态机 / 11 表模型 / 合同校验 / RDS 仓库 / 幂等 Intake / 种子脚本 | ✅ 已入库（1 pack + 9 themes + 1 preset） |
| 全链路测试准备 | 真实商品建档、专属 Look/Scene、测试账号导入、首个真实任务 | ✅ 幂等验证通过 |
| Phase 1 图组生产 | Content Planner（opv-plan-v1）+ Hero-first 生图编排（gpt-image-2） | ✅ 5 张图组过媒体 QC |
| Phase 2 视频化 | FFmpeg 快切渲染器 + 视频 QC + 渲染记录 | ✅ v1 12.5s / v2 10s 两版成片 |
| Phase 3 BGM 与发布 | 选歌策略 v2 + 发布流程（防重提/音频门控）+ NeoBund 真实合同接入 | ✅ 首条已提交定时发布 |
| 快节奏改版 | 10 秒硬切预设 + dance 池选歌 + 撤回换片重提 | ✅ NeoBund 1327852 待发布 |

## 二、当前发布中的内容

- 任务：`opv_task_20260830_a629e64d2dda`（OPV 状态 `publishing`）
- NeoBund 任务：**1327852**，账号 @ambalalala2（authId 5250），**2026-08-31 20:00 曼谷时间定时发布**
- 视频：v2 快切版，1080×1920 / 30fps / H.264 / 10.00 秒 / 5×2s 硬切 / 无音轨（平台挂歌）
- 音乐：《Dance》- DJ BAI（dance 池选中，musicId 6810714911585339394，提交时签名 URL 直接复用）
- 文案：ป้ายยา เจเก็ตตัวนี้ ใส่บินได้ทั้งเที่ยว สบายมาก + 泰语卖点句 + 5 话题标签（方案 TITLE_SCHEME_TH_V1 生成，老板已确认）
- 旧任务 1327683 已撤回（NeoBund status 800），v1 视频与渲染行保留作历史对比
- 结果回查：一次性自动化 21:10（北京）运行 `scripts/confirm_publish_result.py`

## 三、代码资产（packages/organic_photo_video/）

```text
domain/
  statuses.py       任务/Shot/渲染/发布/Outbox 状态机 + 受控迁移
  models.py         11 张 opv_ 表模型（UTC/JSON/Decimal/bool 转换）
  contracts.py      opv-*-v1 合同校验（market-pack/theme/render-preset/
                    account-profile/plan/bgm/title-scheme）
config/
  market_packs/MP_TH_DEFAULT_v1.json        泰国市场规则
  themes/            9 个主题（含快节奏改版后的 travel 主题）
  render_presets/RP_STILL_FASTCUT_10S_V1.json   10 秒硬切版（12S 渐变版已退役）
  copy/TITLE_SCHEME_TH_V1.json              泰语标题方案（钩子公式/词槽/规则）
  accounts/OPV_TH_TEST_001.json             测试账号（绑定 authId 5250）
  loader.py          加载即校验
services/
  task_intake.py     幂等建任务（sha256 幂等键合同）
  content_planner.py opv-plan-v1 规划（Theme 分镜→5 shots）
  asset_resolver.py  轻量试穿资产快照（只读）
  image_generator.py gpt-image-2 适配器 + 确定性 prompt 组装 + 9:16 QC
  hero_first.py      Hero 门控编排（失败恢复/断点续跑/单槽位重试=新版本）
  video_renderer.py  FFmpeg 时间轴（硬切/动效/扩帧保时长）+ ffprobe/blackdetect QC
  video_render_flow.py  Stage D 审核留痕 + 渲染编排（completed 后自动出 v2）
  neobund_music.py   选歌策略 v2（情绪35/热度30/适配20/去重15，3天去重）
  neobund_publisher.py  发布流程（人工 gate/音频门控/防重复提交/实际音乐回读）
  neobund_wiring.py  复用 auto-publisher 本机凭据的接线
scripts/
  seed_reference_data.py / import_account_profile.py   默认 dry-run
  confirm_publish_result.py  发布结果幂等回查
tests/               153 个用例（另有 auto-publisher 133 个）
```

## 四、抓包确认的 NeoBund 真实接口合同（2026-08-31）

**热点音乐列表** `POST /np/shoppable/image/search/music`（client base_url 已含 /np）：
- 请求：`{type:2, keyword, page_size:20, language:"th-TH", region:"TH", page_token?, search_id?}`
- 响应：`{music:[{id,title,author,duration(秒str),cover_thumb,play_url}], next_page_token, has_more:"True"}`
- keyword 是自由搜索词：`hot`=热榜、`dance`=舞曲池均已验证；无 authId，账号范围随登录会话

**Organic commit** `POST /np/shoppable/video/commit`：
- 基础：`authId / authType:2 / videoTitle(=TikTok 文案) / scheduledReleaseTime / attachFileId / isPrecheck / remark / isAigc(整数)` + `postType / brandContentToggle / brandOrganicToggle / disableComment / disableDuet / disableStitch`（旧 payload 缺这些会 400）
- 音乐：`musicId / musicTitle / musicAuthor / musicUrl / musicCoverUrl / musicSoundVolume / videoOriginalSoundVolume`（0-100，默认 50；musicUrl 用列表返回的签名 URL，有效期约 1 天）
- 响应可为空体 → 按 remark/标题+时间回查任务列表，禁止盲目重提（前端也是这么做的）
- 回读：任务列表记录含 `musicId/musicTitle/musicAuthor/status(100=待发布,800=已终止)/countryCode`

## 五、本轮踩坑记录（已修复）

1. Persona 参考图在资产库存的是飞书 file_token（无本地副本）→ 适配器只接受本地存在文件；Persona 参考图本地镜像列入运维待办
2. codex 生图返回 9:16 任意像素（941×1672）→ QC 改为比例容差+最小宽 896px
3. NeoBund 列表路径双写 /np 前缀（404）→ 路径去掉 /np 前缀
4. organic commit 400 → payload 对齐抓包合同（补 postType 等 6 字段、isAigc 用整数）
5. `update_publish_result` 全列更新导致失败路径清空 metadata → 改部分更新（_UNSET 哨兵）
6. TikTok 歌单每次请求换一批 → 选中即持久化签名 URL，提交直接复用
7. macOS TCC 挡 ~/Downloads 读取 → 请用户把文件放进 workspace

## 六、待办与风险

- 今晚发布结果确认（自动化已挂）；确认后拉 TikTok 自然流数据验证内容表现
- `opv_publish_window` 触发器：接入每小时排班轮询（链路已全部验证，纯开发工作）
- 视觉模型自动 QA（qa_json 里 visual_model_qa 为占位）；切点卡拍（BGM 无 BPM 数据，V1 按固定 2s 切）
- 泰国甜妹 Persona 需补 3-5 张参考图并本地镜像后才可转 active
- auto-publisher 的 `isAIGC` 字段名与前端实际 `isAigc` 不一致（OPV 已用 isAigc），建议单独验证修正
- 两个 HAR 文件含登录态，解析完毕建议删除

# 能力升级轮：产品驱动型图文内容（2026-08-31 深夜）

按《产品驱动型图文内容能力优化方案》完成第一阶段「规划解耦」，并与并行会话的
visual_qa / preflight / locale_quality 工作完成合并（165→174 测试全绿）。

## 落地内容

- **迁移 002**（已应用 RDS，带 `_opv_schema_migrations` 台账）：
  - `opv_content_task` 新列：recipe_id / recipe_version / content_goal / hook_strategy / outfit_plan_json / storyboard_version / content_package_id / product_facts_json
  - `opv_content_shot` 新列：narrative_function / product_focus / overlay_spec_json / transition_hint / continuity_constraints_json
  - 新表：`opv_content_recipe` / `opv_render_profile` / `opv_quality_profile` / `opv_content_package`
- **配置**：3 个内容配方（痛点解决 RECIPE_PAIN_POINT_SOLUTION / 场景方案 RECIPE_SCENE_SOLUTION / 视觉变化 RECIPE_VISUAL_TRANSFORM，五层叙事 HOOK→CONTEXT→TRANSFORMATION→PROOF→PAYOFF，anchor_slot 可配）；`IMAGE_STORY_VIDEO_V1` 渲染 profile；`QUALITY_STANDARD_V1` 五维质检 profile；`OUTFIT_RULES_TH_V1` 穿搭规则库
- **服务**：
  - `outfit_planner.py`：Product Facts（产品事实锁定，未知识别显式 unknown）+ 结构化 Outfit Plan（回答为什么这样搭/产品角色/比例/配色/场景）
  - `content_planner.py` v2：配方驱动规划——叙事弧来自 Recipe、场景来自 Theme、时长按 preset 归一化、转场按 preset 强制、persona 走账号 allow-list + TASK_FROZEN 锁 + structured_snapshot_hash、plan 带 render_contract；无 recipe 任务走旧主题路径完全兼容
  - `content_package.py`：内容包生命周期 planning→generating→qa_review→ready→rendered（invalid 兜底），hero_first / render flow 自动推进
  - `hero_first.py`：Anchor-frame-first——Recipe 的 anchor_slot 决定先生成哪张（视觉变化型可锚 P5）
  - `content_story.py`：统一入口 `generate_product_image_story(product_id, market, language, recipe_id, theme_id, variant_count)`（V1 完成 intake+plan+package，生图/渲染走既有服务）
- **合并说明**：与并行会话（visual_qa/preflight/locale_quality/persona-lock/render-contract/时长归一化）完成合并，双方测试规格全部满足

## 与方案的差异说明

- `visual_qa.py` 由并行会话实现（方案写作时视为已存在）——单图五维+组图维度与 QUALITY_STANDARD_V1 对接是下一步
- variation_plan（多变体变化轴）未实现：facade 的 variant_count 目前生成 N 个独立任务
- Recipe 尚未做账号-配方绑定矩阵（账号测试自由组合）
