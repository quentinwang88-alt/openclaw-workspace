# TH 图文人物真实感与动作质量优化——开发交接方案

更新日期：2026-09-07
执行范围：TH 女装原生图文，先覆盖 `PHOTO_TH_PICK_YOUR_LOOK_V3` 的 `STYLE + SCENE_MODEL` 路径
优先级：P0，优先于继续扩展旅行场景、温度穿搭或其他 Recipe

## 1. 背景

TH 原生图文已经具备以下能力：

- 飞书任务表发起；
- 四选一、旅行穿搭等生产预设；
- 参考图下载与 Doubao Seed 2.1 视觉理解；
- A/B/C/D 穿搭计划冻结；
- 通过现有图片生成通道生成真人穿搭素材；
- 五页原生图文排版；
- 首图和整组视觉检查；
- 失败角色定向重生；
- 进入 CreatOK 原生多图发布链路。

当前技术链路已经可运行，但最新一批四选一穿搭暴露出严重的人物质量问题。服装和背景基本可用，人物却明显降低整组内容的可信度：

- 面部有明显 AI 美颜和娃娃脸感；
- 头部在多张图片中向同一侧倾斜；
- 固定使用相似微笑和正面对镜视线；
- 肩颈、手臂、髋部和双腿缺少自然受力关系；
- 所谓行走或转身只改变腿部位置，上半身仍像人台；
- 四张图片只完成了换装，没有形成四个真实的拍照瞬间。

这已经成为当前图文质量的主要短板。本轮不要继续通过扩展更多服装主题掩盖人物问题。

## 2. 本轮样片与证据

飞书记录：`recvuuHoQdq5Bc`
Recipe：`PHOTO_TH_PICK_YOUR_LOOK_V3` V9
主题：`COOL_WEATHER_TRAVEL`
篇数：3，共生成 12 张 A/B/C/D 素材。

供图 manifest：

```text
~/.openclaw/shared/data/organic_photo_video/style_reference_supply/recvuuHoQdq5Bc_item_1/supply_manifest.json
~/.openclaw/shared/data/organic_photo_video/style_reference_supply/recvuuHoQdq5Bc_item_2/supply_manifest.json
~/.openclaw/shared/data/organic_photo_video/style_reference_supply/recvuuHoQdq5Bc_item_3/supply_manifest.json
```

典型问题图：

```text
~/.openclaw/shared/data/organic_photo_video/style_reference_supply/recvuuHoQdq5Bc_item_3/photo_style_recvuuHoQdq5Bc_item_3_P1_v1_1.png
~/.openclaw/shared/data/organic_photo_video/style_reference_supply/recvuuHoQdq5Bc_item_2/photo_style_recvuuHoQdq5Bc_item_2_P3_v1_1.png
```

当前人物模板：`TH_APPAREL_SELECTED_01_001`。人物参考只有一张近景自拍：

```text
~/.openclaw/workspace/shared/data/persona_templates/TH_APPAREL_SELECTED_01_001/reference_01.png
```

人物模板数据库：

```text
~/.openclaw/workspace/skills/lightweight-tryon-video/var/light_tryon.sqlite3
table: persona_templates
```

该参考图本身存在用户不接受的特征：头部倾斜、脸部过度精致、固定微笑、近景自拍动作。当前 Persona 只有这一张参考图，无法提供中性头位、全身比例和自然动作证据。

## 3. 根因

### 3.1 Persona 源头不合格

当前账号绑定：

```text
config/accounts/OPV_TH_TEST_001.json
persona_ref_id = TH_APPAREL_SELECTED_01_001
```

账号处于 `testing`，只要求一张本地人物参考。单张近景图同时承担脸部身份、发型、表情、头部姿态和人物气质锚定，图片模型会把不需要的歪头和假笑一起继承。

`TH_APPAREL_SELECTED_02_001` 和 `TH_APPAREL_SELECTED_03_001` 同样只有一张 AI 感近景自拍，不能简单切换到 02/03 解决问题。

### 3.2 首张成片继续污染后三张

实现位置：

```text
services/photo_style_reference_supply.py
```

当前流程把第一个已经生成的 Look 作为 `identity_anchor` 加入后续请求。首张一旦出现歪头、固定笑容或僵硬站姿，这些特征会继续传递给 B/C/D。

### 3.3 动作合同太弱

当前 `pose_hints` 主要是：

- 自然正面站立；
- 轻微三分之四侧身；
- 小幅迈步或自然转身；
- 正面放松站立。

这些词没有明确头颈、肩线、重心、手臂、视线和动作时刻。模型通常退化为最安全的“居中站立 + 歪头微笑”。

### 3.4 现有质检没有检查人物表现

实现位置：

```text
services/photo_reference_vision.py::_alignment_prompt
```

当前评分只包含：

- `presentation_alignment`
- `style_alignment`
- `scene_alignment`
- `palette_alignment`
- `look_difference`

没有检查脸部真实感、头部姿态、身体受力、手势、表情和组内动作重复。最新 12 张图因此仍然获得约 88–95 分并全部通过。

### 3.5 负向 Prompt 无法单独解决

`services/image_generator.py` 已有“不要磨皮塑料脸、夸张网红姿势”等描述，但只有负向要求，没有提供可执行的真实人物摄影合同。继续堆叠“不要 AI 感”等抽象词不会稳定改善结果。

## 4. 本轮目标

完成后，一篇四选一图文应满足：

1. 第一眼像真实泰国女生的手机旅行穿搭照片；
2. 人物身份在四张图中可识别为同一人；
3. 不出现明显左右歪头；
4. 肩颈、躯干、髋部和腿部具有可信的受力关系；
5. 四张至少有三种不同动作、两种视线方向；
6. 最多一张使用“正面站立、看镜头、轻微微笑”；
7. 人物不合格时只重生失败角色；
8. 不要求运营每条任务上传人物图或维护 JSON。

## 5. 设计原则

- Persona 是账号或生产预设绑定的系统资产，不是逐任务参考图。
- 飞书里的“参考图”继续只负责风格、场景、服装语言或商品事实，不负责指定人物身份。
- 人物身份与人物动作分离：Persona Pack 决定“是谁”，Pose Contract 决定“怎么拍”。
- 不把任何一张未经人物质检的生成图用作后续身份锚点。
- 模型负责观察，程序根据结构化结果重新计算 pass/fail，不能直接相信模型给出的 `passed`。
- 先只改 TH 四选一的 `STYLE + SCENE_MODEL`，通过真实样片后再向旅行 Recipe 和其他真人图文路径推广。
- 保留旧任务和旧 Recipe 的冻结快照，不覆盖历史 manifest，不做全量重构。

## 6. 具体改造

### 6.1 建立合格 Persona Pack

先建立一个新的 TH 人物包，不要继续把现有三张单一 AI 自拍直接用于生产。

建议新建稳定 ID，例如：

```text
TH_APPAREL_REAL_01_001
```

最少包含 4 张同一人物参考：

| role | 画面要求 | 用途 |
|---|---|---|
| `FACE_FRONT_NEUTRAL` | 正面平视、头颈直立、自然表情 | 脸部身份 |
| `FACE_THREE_QUARTER` | 三分之四侧面、头部不向肩膀倾斜 | 侧面身份 |
| `BODY_FULL_NEUTRAL` | 全身、自然站立、肩髋受力可信 | 身材和比例 |
| `BODY_FULL_MOTION` | 自然行走或场景互动抓拍 | 动作和松弛感 |

`persona_templates.reference_images` 已经是 JSON 数组，可在每个元素中增加兼容字段：

```json
{
  "local_path": "...",
  "name": "...",
  "sha256": "...",
  "role": "FACE_FRONT_NEUTRAL",
  "approved": true
}
```

现有读取端会忽略额外字段，因此第一阶段不需要数据库 migration。

开发要求：

- `services/asset_resolver.py` 保留完整 reference item，不要只返回路径；
- 增加 Persona Pack readiness 校验；
- `SCENE_MODEL` 至少需要 3 张已批准本地参考，且必须同时具有 face 和 full-body 证据；
- 单一近景自拍不能再通过真人场景图的生产预检；
- 将 `OPV_TH_TEST_001` 切换到新 Persona 前，先跑候选图人工验收；
- 不删除旧 Persona，旧冻结任务仍可追溯。

如果没有真实人物照片，可用现有生图能力生成候选 Persona Pack，但候选必须先独立质检并由用户一次性选择。不能由任务扫描器自动生成后直接投入生产。

### 6.2 增加人物摄影合同

建议新增一个开发侧配置：

```text
config/photo_human_presentation/TH_CREATOR_REALISM_V1.json
```

这是系统配置，不新增飞书字段，也不要求运营维护。

最小合同：

```json
{
  "policy_id": "TH_CREATOR_REALISM_V1",
  "head": {
    "require_level_neck": true,
    "forbid_intentional_side_tilt": true
  },
  "face": {
    "require_real_skin_texture": true,
    "forbid_doll_eyes": true,
    "forbid_plastic_skin": true,
    "forbid_fixed_beauty_smile": true
  },
  "body": {
    "require_believable_weight_distribution": true,
    "require_relaxed_shoulders": true,
    "forbid_mannequin_arms": true
  },
  "group": {
    "min_pose_families": 3,
    "min_gaze_directions": 2,
    "max_static_camera_smile": 1
  }
}
```

在 `services/image_generator.py::compose_shot_prompt` 中添加独立的 `【人物摄影合同】`，只在真人模式启用。不要只增加一句“动作自然”，需要明确描述：

```text
头颈保持自然直立，不向左右肩膀倾斜；肩线放松。
动作必须有明确重心，承重腿、迈步腿、髋部和手臂协调。
真实皮肤保留毛孔、细小纹理和自然不对称，避免玻璃眼、塑料皮肤和固定嘴角。
像同行朋友用手机抓拍，不使用证件照式居中站立。
```

同时明确：人物参考只约束身份，不复制人物参考图里的头部角度、表情、手势或姿势。

### 6.3 建立四选一动作合同

在 `services/photo_style_reference_supply.py` 中用结构化 `pose_contract` 替代当前简单 `pose_hints`。

第一版固定四类动作即可：

| role | pose_family | action | gaze | head | body |
|---|---|---|---|---|---|
| `look_a` | `RELAXED_STAND` | 一脚略向前，另一腿承重 | 看镜头附近 | 头颈水平 | 肩膀放松、髋部轻微自然偏移 |
| `look_b` | `WALKING_CANDID` | 行走中间时刻，手臂自然摆动 | 看前方 | 头颈顺着行走方向 | 步幅小、受力明确 |
| `look_c` | `SCENE_INTERACTION` | 整理袖口、拿包或轻触场景已有物体 | 看手部或侧方 | 不歪头 | 上身随动作轻微转动 |
| `look_d` | `TURN_BACK` | 走过镜头后轻微回身 | 看镜头附近 | 转头但不侧倾 | 肩髋角度不同、脚步自然 |

注意：

- 动作道具只能来自计划场景，不能凭空添加咖啡杯、行李箱或包；
- 四个角色不能全部正视镜头；
- `TURN_BACK` 是身体转动，不是把头倒向肩膀；
- 后续可以轮换动作族，本轮不建设复杂动作推荐系统。

需要把以下结构写入 `plan_shot.composition_contract`：

```json
{
  "pose_family": "WALKING_CANDID",
  "action": "自然行走中的中间时刻",
  "gaze": "forward_off_camera",
  "head_posture": "level",
  "body_mechanics": "clear_weight_bearing_leg_and_natural_arm_swing"
}
```

`compose_shot_prompt` 必须把这些字段转换为正向可执行文本。

### 6.4 取消生成图之间的姿势污染

修改 `services/photo_style_reference_supply.py`：

- 每一张都使用同一套已批准 Persona Pack；
- 不再默认把第一张生成结果作为 B/C/D 的 `identity_anchor`；
- 原始风格参考继续提供场景、构图和服装语言；
- Persona Pack 只提供人物身份；
- `pose_contract` 只提供当前页面动作；
- 不把上一张的表情、头部角度、手势和站姿作为连续性要求。

优先测试“固定 Persona Pack，不传前一张生成图”的身份一致性。如果脸部漂移仍明显，再使用规范化的 `FACE_FRONT_NEUTRAL` 作为所有页面的共同身份锚点，不能重新引入某张生成结果作为锚点。

### 6.5 首张人物门禁

当前 Look A 在生成后只做参考风格一致性检查。修改为：

```text
生成 Look A
→ 风格一致性检查
→ 人物表现检查
→ 两者均通过后才能生成 B/C/D
```

如果 Look A 的脸、头部或身体姿态不合格：

- 生成定向修复说明；
- 保持 Persona、场景和穿搭不变；
- 只重生 Look A；
- 最多自动重试一次；
- 第二次仍失败则停止，不允许坏锚点继续污染整组。

### 6.6 新增人物表现质检

建议新增：

```text
services/photo_human_qa.py
```

并在 `services/photo_reference_vision.py` 增加：

```python
review_human_presentation(...)
```

继续复用已经接好的 Doubao Seed 2.1 视觉线路，不新建模型鉴权和调用栈。

模型输出只描述观察事实：

```json
{
  "roles": [
    {
      "role": "look_a",
      "scores": {
        "face_realism": 0,
        "head_posture": 0,
        "body_posture": 0,
        "gesture_naturalness": 0,
        "expression_naturalness": 0,
        "creator_photo_feel": 0
      },
      "observations": {
        "head_tilt": "NONE|MINOR|OBVIOUS",
        "head_tilt_direction": "NONE|LEFT|RIGHT",
        "gaze": "CAMERA|FORWARD|SIDE|DOWN",
        "pose_family": "RELAXED_STAND|WALKING_CANDID|SCENE_INTERACTION|TURN_BACK|STATIC_MANNEQUIN",
        "expression": "NEUTRAL|SOFT_SMILE|CANDID|FIXED_BEAUTY_SMILE",
        "ai_face_signs": []
      },
      "issues": [],
      "repair_instruction": ""
    }
  ]
}
```

程序层重新计算结果，建议第一版阈值：

- `face_realism >= 80`
- `head_posture >= 85`
- `body_posture >= 80`
- `gesture_naturalness >= 75`
- `expression_naturalness >= 75`
- `creator_photo_feel >= 80`

硬失败条件：

- `head_tilt == OBVIOUS`；
- `pose_family == STATIC_MANNEQUIN`；
- `ai_face_signs` 包含 `PLASTIC_SKIN`、`DOLL_EYES` 或 `FACE_GEOMETRY_ARTIFACT`；
- 肢体结构不合理；
- 人物脸部身份明显漂移。

整组失败条件：

- 四张中少于三种 `pose_family`；
- 少于两种视线方向；
- 超过一张为 `RELAXED_STAND + CAMERA + SOFT_SMILE`；
- 两张及以上出现同方向 `MINOR` 歪头；
- 两张及以上使用相同固定表情和近似身体姿势。

模型即使返回 `passed=true`，程序命中上述条件仍必须失败。

### 6.7 定向重生与证据保存

复用当前 `group_repair_attempts`、`attempt_history` 和失败角色重生机制，补充人物失败原因。

例如：

```text
上一版人物头部明显向右肩倾斜，双臂僵直，表情为固定微笑。
保持同一人物身份、穿搭和场景；头颈保持水平，肩膀放松；
改为自然行走抓拍，视线看向前方，手臂随步伐自然摆动。
```

每个 source 在 manifest 中增加：

```json
{
  "human_presentation_qa": {},
  "pose_contract": {},
  "persona_pack_id": "TH_APPAREL_REAL_01_001"
}
```

manifest 顶层增加：

```json
{
  "group_human_presentation_qa": {}
}
```

不修改旧 manifest；读取时字段缺失按旧版本处理。

### 6.8 飞书与运营界面

本轮不新增任何运营必填字段。

运营仍然只维护：

- 生产预设；
- 生成篇数；
- 图文主题；
- 参考图类型；
- 参考图；
- 可选内容要求；
- 执行；
- 确认发布。

人物由账号或生产预设自动绑定。飞书只在现有字段中反馈：

- `素材状态`：例如“人物质检 3/4”；
- `审核阶段`：例如“人物表现质检”；
- `备注`：例如“Look B 头部倾斜，正在定向重生”。

不要让运营填写动作 Prompt、Persona JSON 或人物参考路径。

## 7. 推荐修改文件

必须修改：

```text
services/photo_style_reference_supply.py
services/image_generator.py
services/photo_reference_vision.py
services/asset_resolver.py
services/asset_readiness.py
config/accounts/OPV_TH_TEST_001.json
```

建议新增：

```text
services/photo_human_qa.py
config/photo_human_presentation/TH_CREATOR_REALISM_V1.json
tests/test_photo_human_qa.py
tests/test_photo_persona_pack.py
tests/test_photo_human_production.py
```

可能修改：

```text
services/feishu_workflow.py
tests/test_photo_theme_reference.py
tests/test_photo_reference_vision.py
tests/test_photo_feishu_batch.py
```

不需要修改：

- CreatOK 发布接口；
- 原生多图排版；
- BGM 选择；
- 发布队列表结构；
- Recipe/Template 主体关系；
- 已完成历史任务。

## 8. 开发阶段

### Phase 1：Persona Pack 与预检

工作：

1. 扩展 reference item 元数据兼容读取；
2. 实现 Persona Pack readiness；
3. 建立一套新的 TH Persona Pack；
4. 让单一近景 Persona 在 `SCENE_MODEL` 生产前失败；
5. 保持旧冻结任务兼容。

验收：

- 新 Persona Pack 能返回 face/full-body/motion 参考；
- 缺少 full-body 或只有一张图片时明确报错；
- 不需要 migration；
- 用户只需一次性审核人物包。

### Phase 2：人物摄影与动作合同

工作：

1. 加入人物摄影合同；
2. 结构化 A/B/C/D 动作；
3. Prompt 输出 head/gaze/action/body mechanics；
4. 取消第一张生成图对后三张姿势的默认锚定。

验收：

- 生成 Prompt 中四个角色动作明确不同；
- 四张不会共享同一歪头、微笑和手臂姿势；
- 每张仍保持同一 Persona 和冻结穿搭。

### Phase 3：人物质检与定向修复

工作：

1. 新增 Doubao 人物观察合同；
2. 程序化重新计算 pass/fail；
3. Look A 前置门禁；
4. 四张整组重复度检查；
5. 将失败原因注入重生 Prompt；
6. 将 QA 证据写入 manifest。

验收：

- 当前问题样片不能再全部通过；
- Look A 不合格时不会继续生成 B/C/D；
- 单个角色失败只重生该角色；
- 超过重试次数后保留证据并明确停止。

### Phase 4：真实样片

新建飞书记录，不能覆盖 `recvuuHoQdq5Bc`，因为旧计划和 manifest 已冻结。

测试输入：

```text
生产预设：图文｜TH｜四选一穿搭
生成篇数：1
图文主题：凉爽旅行
参考图类型：风格参考
参考图：复用本轮两张海边旅行参考图
内容要求：自然松弛的旅行抓拍，人物头颈直立，四张动作明显不同
```

只生成一篇 4 张素材和 5 页成片，不勾选确认发布。

## 9. 测试要求

单元测试至少覆盖：

1. 单一近景 Persona 对 `SCENE_MODEL` 不可用；
2. 具有 face/full-body/motion 的 Persona Pack 可用；
3. 旧 reference JSON 没有 `role` 时仍可读取；
4. 四个角色生成不同 pose contract；
5. Prompt 明确禁止复制人物参考姿势；
6. `OBVIOUS` 歪头必定失败；
7. AI 脸硬失败项必定失败；
8. 组内动作不足三类时失败；
9. 两张以上同向轻微歪头时失败；
10. Look A 人物失败后只重生 Look A；
11. group QA 只重生失败角色；
12. manifest 能断点续跑且保留人物 QA 证据；
13. 旧四选一、完整穿搭和历史任务不回归。

建议把本轮观察结果制作成模型输出 fixture，验证确定性判定层；不要在单元测试里真实调用付费模型。

完成后执行：

```bash
python3 -m unittest discover -s tests -p 'test_photo_*.py'
python3 -m unittest discover -s tests
```

## 10. 真实样片验收标准

必须同时满足：

- 4 张均无明显侧向歪头；
- 第一眼没有塑料皮肤、娃娃眼和固定 AI 微笑；
- 至少 3 种动作；
- 至少 2 种视线方向；
- 最多 1 张正面静态看镜头；
- 行走图的肩、髋、承重腿和手臂协调；
- 场景互动来自画面已有元素；
- 服装与冻结计划一致；
- 四张人物身份一致；
- 人物质检结果和 manifest 完整；
- 失败页面能定向重生，已合格图片不重复付费生成。

用户最终只判断两件事：

1. 是否像真实女生自己或朋友拍的穿搭内容；
2. 人物是否具有自然、松弛、有拍照美感的状态。

若这两项未通过，即使自动分数达标，也不能宣布本轮完成。

## 11. 非目标

本轮暂不处理：

- 旅行 Recipe 的目的地/温度/逐页场景语义；
- 泰语文案母版审核；
- MX 假发人物系统；
- 账号初始流量评分；
- 更多视觉 Template；
- 发布和 BGM 链路；
- 复杂动作智能推荐系统。

旅行语义优化可以在人物真实感通过后继续，避免同时改变人物、场景和文案而无法判断哪项改动有效。

## 12. 接手模型执行注意事项

1. 仓库当前存在大量未提交改动，不要执行 `git reset`、覆盖式 checkout 或清理未跟踪文件。
2. 先阅读 `docs/TH_PHOTO_REFERENCE_MODULE_HANDOFF_20260907.md`，理解已完成的参考图与生产链路。
3. 先审查现有实现，再按 Phase 1–3 开发；不要旁建第二套图片生产系统。
4. 复用现有 Doubao 视觉客户端和图片生成适配器，不新写模型鉴权。
5. 不把 API Key、访问令牌或飞书凭证写入代码、日志、fixture 或文档。
6. 在所有自动化测试通过前不要运行真实付费生图。
7. 真实测试必须限制到单条新飞书记录，先 dry-run，再生成一篇。
8. 不自动发布样片，等待用户查看最终 5 页成片。

接手模型的首条执行指令建议为：

```text
请阅读 docs/TH_PHOTO_REFERENCE_MODULE_HANDOFF_20260907.md 和
docs/TH_PERSONA_REALISM_OPTIMIZATION_HANDOFF_20260907.md。
先审查当前代码与样片证据，按后一个文档的 Phase 1–3 完成人物真实感、
动作合同和人物质检改造，运行相关测试并报告改动。不要覆盖现有未提交修改，
不要调用真实付费生图，也不要发布。测试通过后再给出单条真实样片的执行准备情况。
```

---

## 13. 开发执行记录（2026-09-07 Phase 1-3 完成）

### 已落地

**Phase 1：Persona Pack 与预检**
- 新增 `services/persona_pack.py`：`normalize_reference_items`（兼容旧 JSON 无 role）、`build_persona_pack`、`evaluate_persona_pack`（≥3 张已批准本地参考 + face/full-body 证据）、`identity_reference_paths`（face 在前的身份参考顺序）。
- `services/asset_resolver.py::get_persona` 新增 `reference_items`（保留完整 entry：role/approved/sha256），不改变 `structured_snapshot_hash`，旧冻结快照不受影响。
- `services/asset_readiness.py`：plan 带 `presentation_type=SCENE_MODEL` 且有 persona 时执行人物包预检；单一近景自拍无法通过。

**Phase 2：人物摄影合同与动作合同**
- 新增 `config/photo_human_presentation/TH_CREATOR_REALISM_V1.json`（head/face/body/identity/group 全量字段 + max_same_direction_minor_tilt、max_fixed_beauty_smile_roles）。
- `services/image_generator.py`：新增 `human_presentation_contract_lines`（【人物摄影合同】独立段落，仅真人模式经 `recipe_execution.human_presentation_contract` 启用）与 `pose_contract_text`（动作合同正向文本）；`_reference_paths` 优先使用角色化人物包参考。
- `services/photo_style_reference_supply.py`：`POSE_CONTRACTS` 四动作族（RELAXED_STAND/WALKING_CANDID/SCENE_INTERACTION/TURN_BACK）写入 `composition_contract.pose_contract`；人物场景模式取消"首张生成图当身份锚点"，改用人物包 `persona_identity_images`；旅行路径本轮保持原行为。

**Phase 3：人物质检与定向修复**
- 新增 `services/photo_human_qa.py`：观察归一化（score 钳制/枚举校验）+ 程序化判定（6 项阈值、OBVIOUS 歪头/STATIC_MANNEQUIN/AI 脸三项/肢体/身份漂移硬失败、组级动作族≥3/视线≥2/静态微笑≤1/同向 MINOR≤1/固定微笑≤1）；模型 passed 永不被信任（观察 Prompt 也不要求 passed）。
- `services/photo_reference_vision.py::review_human_presentation`：复用 Doubao 线路，身份参考图+生成图一起输入，只输出观察事实。
- supply 流程：Look A 前置门禁（风格检查→人物检查→双重失败即停，只重生 Look A）；整组人物 QA 与风格 QA 联合归因，复用 group_repair_attempts/attempt_history，失败原因注入重生 Prompt；manifest 增加 per-source `pose_contract`/`persona_pack_id`/`human_gate_qa`/`human_presentation_qa` 与顶层 `group_human_presentation_qa`，旧 manifest 缺字段按旧版本读取。
- `services/feishu_workflow.py`：进度事件 `human_qa_started`/`repair_scheduled(reason,notes)` 映射到飞书"人物表现质检中/人物质检修复 N 张"。

### 测试

- 新增 `tests/test_photo_persona_pack.py`（9）、`tests/test_photo_human_qa.py`（20）、`tests/test_photo_human_production.py`（7）；更新 `test_photo_theme_reference.py`（人物包 fixture + 取消锚定断言）、`test_photo_travel_semantics.py`（事件负载断言）。
- `python3 -m unittest discover tests -p 'test_photo_*.py'`：157/157；全量 667/667。
- 未调用任何真实付费生图/视觉模型；未触碰 `recvuuHoQdq5Bc` 冻结数据。

### 待办（需要用户决策/操作）

1. **Persona Pack 本体尚未建立**：`TH_APPAREL_REAL_01_001` 需要真实人物照片（4 角色各一张）或用生图能力产出候选包（候选需独立质检并由用户一次性选定）。付费生图在本轮被禁止，故未生成候选。
2. **账号切换**：`config/accounts/OPV_TH_TEST_001.json` 仍绑定 `TH_APPAREL_SELECTED_01_001`。在人物包通过人工验收并写入 `persona_templates`（reference_images 带角色字段）之前不做切换；切换后新 SCENE_MODEL 任务才会通过预检。注意：人物包预检已生效，旧单人像 persona 的新 SCENE_MODEL 四选一任务会在生产前明确报错拦截（属预期行为）。
3. **真实样片（Phase 4）**：新建飞书记录（凉爽旅行/1 篇/风格参考/复用两张海边参考图/内容要求写明自然抓拍），单条持锁运行，不勾选发布。
