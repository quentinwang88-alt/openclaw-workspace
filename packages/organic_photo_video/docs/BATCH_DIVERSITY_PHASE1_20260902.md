# OPV 批次多样性第一阶段（2026-09-02）

## 目标与边界

本阶段解决同一飞书记录生成 1-9 条内容时，人物、穿搭、场景与镜头结构重复的问题。

- 不新增飞书字段。
- 不新增 RDS 表或字段。
- 不调用模型做打分、视觉 QA 或相似度判断。
- 不改变历史任务；新任务在创建时把编排结果冻结进现有 `product_snapshot_json` 和 `plan_json`。
- 同一飞书 `record_id` 重试时编排结果稳定；新建记录自动获得新的稳定偏移。

## 执行链路

```text
生产预设 + 生成数量
  -> 稳定轮换商品参考包
  -> BatchDiversityPlanner 批次编排
  -> 冻结 persona/look/silhouette/scene/zone/grammar/recipe/hook
  -> ContentPlanner 把 G1/G2/G3 编译成真实镜头合同
  -> 原有生图与渲染链路
```

## 批次组合合同

完整组合键包含：

```text
reference_pack_id + reference_pack_version
+ persona_ref
+ look_ref + visible_silhouette
+ scene_ref + scene_zone
+ shot_grammar
+ recipe_id + hook_strategy
```

9 条验收批次要求：

- 完整组合键重复数为 0。
- G1/G2/G3 各 3 条。
- 两套商品图包为 5/4 分布。
- 三个人物各约 3 条。
- 可见穿搭轮廓不少于 4 种。
- 场景区域组合不少于 5 种。
- 同主题重复出现时使用不同泰语标题角度，标题不出现纯数字产品编码。

资产不足时不阻断：编排器缩小到真实可用资产池，并明确标记 `fallback_used=true`；组合键如实保留重复，不会用序号伪装成新的视觉组合，也不会虚构新人物、穿搭或场景。

场景遵守主题边界：生产预设已指定 `scene_ref` 时，只在该场景的内部区域轮换；只有预设没有指定场景时，才从账号允许场景中选择，避免把咖啡约会错误分配到机场或卧室。

## 三套镜头语法

- `G1 result_first`：完整穿搭结果开场，再交付比例、生活动作、商品安全细节与第二角度。
- `G2 detail_first`：腰部以上商品轮廓/领口/门襟开场，再切上身版型、全身、生活状态与第二角度。
- `G3 lifestyle_action_first`：自然入镜/拿包/整理衣领开场，再切完整结果、侧向生活构图、安全细节与离场回看。

语法不是标签：每个槽位都会覆盖 `purpose`、`camera_hint` 以及构图合同中的 `framing/camera_angle/pose/instruction`，最终进入生图提示词。

## 商品身份与文案保护

- 运营表图包缺少名称/类别时，优先继承同产品默认或已有权威图包的名称与类别。
- 纯数字商品名不会进入标题和 caption；无权威名称时按类别使用泰语通用称呼。
- `outerwear` 遇到含“连衣裙/เดรส”的旧主题模板时，改用外套“一件多搭”文案。

## 配置与代码

- 配置：`config/batch_diversity/OPV_BATCH_DIVERSITY_V1.json`
- 批次编排：`services/batch_diversity_planner.py`
- 飞书入口接入：`services/feishu_workflow.py`
- 镜头计划与文案保护：`services/content_planner.py`
- 场景区域与构图合同下发：`services/image_generator.py`
- 商品身份继承：`services/product_reference_resolver.py`

## 测试策略

测试只使用内存 fake 和临时文件：不读取正式飞书记录、不写正式 RDS、不执行付费生图。

- `tests/test_batch_diversity_planner.py`：9 条只编排验收、重试稳定、资产不足降级。
- `tests/test_content_planner.py`：G2 首镜真实改为商品近景、场景区域冻结、纯数字/错类文案保护。
- `tests/test_product_reference_resolver.py`：运营图包继承权威商品名称与类别。
