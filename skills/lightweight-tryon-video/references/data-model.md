# 数据模型与协议

## 目录

1. 核心表
2. 状态机
3. 任务幂等
4. 生成 worker 协议
5. 视觉 QC 协议
6. 判定规则

## 1. 核心表

SQLite DDL 的唯一执行来源是 `scripts/light_tryon/database.py`。

### scene_templates

存储“在哪里拍”的环境结构、适用类目、背景锚点、光线、正向 Prompt 和负向 Prompt。室内场景额外保存视觉风格、背景候选池、背景简洁度、边缘装饰候选池、装饰数量/位置和主光方向；编排时稳定解析一种实际背景、实际边缘装饰和实际主光方向。默认启用室内 INS 奶油风与明亮现代咖啡店；历史 A/B/C/D/E 行中只有 A 继续作为主室内环境，B/C/D/E 停用但保留以复现旧任务。镜头由系统内置 `SHOT_FULL_FIXED`、`SHOT_UPPER_FIXED`、`SHOT_UPPER_THREE_QUARTER`、`SHOT_UPPER_PUSH_IN` 四种策略生成。

### action_templates

存储适用类目、适用环境、适用镜头策略、动作步骤、幅度、手部使用、转身程度、风险、优先级和 Prompt。填写的场景与镜头条件必须同时满足；同等条件下先用镜头/场景专用动作，再按本批使用次数、优先级、风险和 ID 排序。`testing` 动作只在商品明确选择时参与。ACT_007–ACT_009 按上半身固定/推近镜头生效，不再绑定卧室 D/E 场景。

### shot_plan_templates

存储镜头方案名称、适用类目、生成 1 条时的镜头、生成 5 条时第 1–5 条的有序镜头以及非标准条数循环。底层四种镜头定义仍由系统维护；运营只调整排列。自动模式按类目和优先级选择已启用方案，`testing` 方案只在原始脚本表明确选择时参与。

### styling_templates

存储适用上装/商品类型、下装、颜色、版型、配饰、风格和 Prompt，并提供选填的内搭类型、内搭颜色、内搭补充要求。外套类优先使用模板内搭值，空字段由系统自动补齐；STYLE_006 对套装、连衣裙和连体款有保护分支，避免凭空添加下装。

### subtitle_templates

存储市场、语言、字幕角度、开/中/结尾文案、字数上限和语气。字幕模板只形成后期计划，不要求视频模型生成文字。V1 预配置中文、泰语和英文；不存在目标语言时编排必须失败，不能静默使用错误语言。

### persona_templates

存储人物外形、手机、脸部可见度和 Prompt，并按账号维护选填的店铺 Logo、品牌展示名、视觉预设、主色和默认系列名称。品牌配置只形成后期叠加计划，不发送给视频生成模型。V1 只选择一个启用人设。

### products_for_light_video

存储商品 ID、市场/语言、类目、标题、参考图、明确卖点、推荐模板池、计划条数和状态。

### product_visual_plans

一行代表一个 `商品 + 场景 + 搭配 + 人设` 组合。保存名称快照与稳定 ID、方案指纹、每方案视频数量、产品穿搭图请求、当前有效穿搭图、复核状态和关联任务 ID。场景方案保存系统解析的实际背景类型、实际边缘装饰和实际主光方向；外套类方案还保存内搭类型、内搭颜色和外套开合规则。一个源记录最多 6 个活动方案；移除组合只标记 `superseded`，不删除历史。

方案指纹包含商品图内容指纹、场景/搭配/人设 ID 与版本、场景参考图指纹和穿搭图 builder 版本。单纯代码升级不会创建视频任务。

### video_jobs

存储商品与五类模板组合、环境、`shot_profile_id`、时长、变体、Prompt 包、生成/QC 状态、原片、后期成片和错误。新任务同时保存 `visual_plan_id`、确认穿搭图路径/URL/版本；旧任务这些字段为空并保留 `legacy_job=1`。生成适配字段包括 `generation_channel`、`generation_model`、`generation_rerun`、`run_manager_record_id`、同步状态/错误、追踪 ID、结果状态和来源指纹。新任务的 `plan_version` 内含视觉方案 ID，因此 `product_id + plan_version + variant_no` 仍可兼容旧库。

### 辅助表

- `duration_templates`：V1 只启用 8 秒和 10 秒。
- `job_attempts`：每次真实生成独立记录 provider、请求、响应、错误和开始/结束时间。
- `visual_plan_attempts`：每次产品穿搭图生成独立记录请求、响应和错误。

## 2. 状态机

生成状态：

```text
pending -> generating -> success
                  |-> retrying -> generating
                  |-> failed
```

产品穿搭图状态（正常生产成功后由系统直接从 `generating` 自动放行到 `confirmed`；`pending_review` 只保留为兼容状态）：

```text
pending -> generating -> pending_review -> confirmed
   ^              |             |-> regenerate
   |              |-> failed --------|
```

只有 `plan_status=active AND outfit_image_status=confirmed` 才能创建、构建和认领绑定的视频任务。

QC 状态：

```text
pending -> passed
       |-> manual_review -> passed|failed
       |-> failed
```

只有 `generation_status=success` 才能进入 QC。后期字幕更新成片后，QC 必须重置为 `pending`。

运行管理表同步状态：

```text
not_submitted -> pending -> queued -> generating -> returned
                                  |-> failed|blocked
```

默认 `generation_channel=no_generate`，不会创建运行任务。来源指纹未变化时跳过；运行中的同 `job_id` 禁止重复提交。只有运行表 `结果回传状态=uploaded` 且存在 `生成视频` 附件时，才将视频转存到复核台 `初始成片` 并置为 `returned`。

## 3. 任务幂等

- `job_id` 包含商品、变体和组合哈希，用于 Prompt 内容 ID 与资产认领。
- 同一 `plan_version` 重跑不重复建任务。
- 策略或模板发生需要重新实验的变化时，显式使用新 `plan_version`。
- 重试生成不新建任务，但递增 `attempt_no` 并保留旧尝试。
- 不覆盖旧 trace、错误或 provider 响应。
- 同一视觉方案从每方案 1 条改成 5 条只新增 V2–V5；从 5 改回 1 不删除任务。
- 同一视觉方案的所有任务只使用该方案的一张已确认产品穿搭图作为下游视觉参考。

## 4. 产品穿搭图 worker 协议

命令从 stdin 读取 `visual_plan` 与 `request`。`request.references` 明确区分 `product_truth`、`persona_identity`、`scene_truth`，搭配使用文本模板，不要求单独搭配参考图。`scene_truth` 为选填；缺少时 `scene_reference_mode=text_fallback`，使用完整场景文字继续执行。

成功 stdout：

```json
{"output_image_path":"/absolute/outfit.jpg","image_version":"optional"}
```

输出成功后自动写入 `confirmed` 并创建视频任务；人工只做飞书事后抽检，不阻断提示词或视频任务生成。

## 5. 视频生成 worker 协议

命令必须从 stdin 读取一个 JSON 对象：

```json
{
  "protocol_version": "1.0",
  "job": {"job_id": "...", "duration_seconds": 8},
  "product": {"product_images": ["..."]},
  "prompt_payload": {"positive_prompt": "..."},
  "jimeng_record": {"状态": "待处理", "提示词": "..."}
}
```

成功时 stdout 只能输出一个 JSON 对象：

```json
{
  "output_video_path": "/absolute/path/result.mp4",
  "output_cover_path": "/absolute/path/cover.jpg",
  "provider_task_id": "optional"
}
```

`output_video_path` 必须存在。非零退出码、无输出、非法 JSON 或不存在的文件都算失败。

## 6. 视觉 QC 协议

视觉 worker 从 stdin 接收视频路径、场景/动作模板、商品和 QC 预期。stdout 返回：

```json
{
  "scene_consistency": 28,
  "person_naturalness": 18,
  "clothing_clarity": 19,
  "action_completeness": 14,
  "realism": 14,
  "phone_covers_face": true,
  "clothing_identifiable": true,
  "no_severe_body_anomaly": true,
  "scene_matches_template": true,
  "action_complete": true,
  "camera_motion_matches": true,
  "upper_garment_fully_visible": true,
  "brightness_adequate": true,
  "no_overexposure": true,
  "product_color_preserved": true,
  "visible_anchor_count": 3,
  "evidence": ["左后床、右后灰帘和右侧置物架稳定可见"]
}
```

评分上限分别为 30/20/20/15/15，总分 100。worker 必须以视频时序判断动作和镜头运动，不能只看单帧。固定镜头必须无推拉摇移；推近镜头只能沿正面光轴极慢平稳推近。上装类任务还必须检查领口、肩线、袖型、门襟和下摆是否持续完整可见。

## 7. 判定规则

- 结构失败：直接 `failed`。
- 未运行视觉 QC：`manual_review`。
- 手机未遮脸、商品不可识别、严重肢体异常、场景不匹配、动作不完整、镜头运动不匹配、亮度不合格、过曝、商品偏色、上装被裁切，或背景锚点少于 2 个：即使总分高也 `failed`。
- 必填视觉布尔证据缺失时进入 `manual_review`，不得仅凭总分自动通过。
- 总分 >= 80：`passed`。
- 总分 60–79：`manual_review`。
- 总分 < 60：`failed`。
- 产品穿搭图 QC 另行检查商品还原、搭配、场景、人设、手机遮脸、上装重点构图、明亮且颜色准确、无文字/Logo/水印；该检查用于事后抽检，不阻断生产。视频 QC 必须以确认穿搭图而非原始商品图作为视觉一致性基准。
- 复核台 `初始成片` 与 `最终视频` 均为附件字段。初始附件、任务字幕计划、品牌计划和后处理版本共同形成幂等指纹；指纹未变化且最终视频仍存在时跳过，任一输入变化时重新渲染并覆盖该记录的最终视频附件。
