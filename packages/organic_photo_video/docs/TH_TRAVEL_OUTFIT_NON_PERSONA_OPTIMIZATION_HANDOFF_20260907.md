# TH 旅行穿搭模板——非人物问题优化开发方案

更新日期：2026-09-07
目标 Recipe：`PHOTO_TH_TRAVEL_OUTFIT_V2`
本轮明确排除：Persona Pack、AI 脸、头部姿态、人物动作与人物表现 QA；这些能力正在另一条开发线修改。

## 1. 背景与现状

最新真实样片：

- 飞书记录：`recvuve1J8mRyH`
- Recipe：`PHOTO_TH_TRAVEL_OUTFIT_V2`
- 主题：`COOL_WEATHER_TRAVEL`
- 输入：15–22°C 凉爽城市旅行，机场、老城、咖啡店、傍晚散步各一套
- 冻结计划：`~/.openclaw/shared/data/organic_photo_video/content_plans/recvuve1J8mRyH/plan.json`
- 供图证据：`~/.openclaw/shared/data/organic_photo_video/style_reference_supply/recvuve1J8mRyH_item_1/supply_manifest.json`
- 最终成片：`~/.openclaw/shared/data/organic_photo_video/photo_packages/opv_task_20260907_3f78fea60b28/opv_rev_20260907_ae8ab7167c7c/final/`

本轮已经解决上一版的核心场景错位：

- Look A 有机场航站楼和行李箱；
- Look B 有老城建筑、石板街和落叶；
- Look C 有咖啡馆座位、咖啡和蛋糕；
- Look D 有傍晚光线、河边步道和亮起的街灯。

旅行参考理解与内容规划已经拆成两步；每个 Look 已有独立的 `travel_moment`、`scene_prompt`、`weather_logic` 和泰语标签；旅行语义 QA 已能按角色定向重生场景错位图片。这个架构保留，不另建第二套旅行生产流程。

除人物问题外，当前仍有四项需要处理：

1. 四套穿搭只有两套上装骨架，差异不足；
2. 步行场景使用细跟高跟鞋，旅行实用性不成立；
3. 温度合同自相矛盾，并生成无依据的室内精确温度；
4. 旅行 QA 对缺失字段默认通过，且没有检查套版后的最终文字页面。

泰语文案仍为 `DRAFT`，发布门禁会阻止进入正式发布。开发模型不得自行把它改为 `NATIVE_APPROVED`。

## 2. 开发边界

本轮可以修改：

```text
config/recipes/PHOTO_TH_TRAVEL_OUTFIT_V2.json
config/copy_packs/TH_TRAVEL_OUTFIT_V2.tsv
services/photo_reference_vision.py
services/photo_content_planner.py
services/photo_travel_qa.py
services/photo_package.py
services/feishu_workflow.py
tests/test_photo_travel_semantics.py
tests/test_photo_package.py
tests/test_photo_feishu_batch.py
```

本轮不要修改：

```text
services/persona_pack.py
services/photo_human_qa.py
config/photo_human_presentation/
人物模板数据库或 Persona reference_images
人物动作合同和人物修复 Prompt
```

不要修改 CreatOK 发布、BGM、账号评分、MX 假发和其他 Recipe。

## 3. 改造一：加强四套穿搭差异

### 3.1 当前问题

最新计划实际为：

- A/C：同一件奶油黄开衫、同一件吊带、同一双红鞋，只更换裤子和裙子；
- B/D：同一件风衣、同一件衬衫、同一双高跟鞋，只更换裤子和裙子。

当前校验只要求完整签名不完全相同，因此只换一个字段也能通过。

### 3.2 数据与校验

在 `services/photo_reference_vision.py::_normalize_travel_plan` 增加程序化 pairwise 校验。

核心字段：

```python
TRAVEL_LOOK_CORE_FIELDS = ("outerwear", "top_inner", "bottom", "shoes")
TRAVEL_MIN_PAIRWISE_FIELD_DIFF = 2
```

任意两套 Look 至少有两个核心字段不同：

```python
distance = sum(normalize(a[field]) != normalize(b[field]) for field in fields)
if distance < 2:
    errors.append("look_a 与 look_c 仅有 1 个核心单品不同，至少需要 2 个")
```

额外禁止：

- 两套 Look 使用完全相同的 `outerwear + top_inner + shoes`；
- 四套中只有两种上半身组合；
- 通过在描述中加入同义词绕过校验。

第一阶段采用基本字符串归一化即可：去空白、统一大小写、把 `无/none/不穿外套` 归一为 `NONE`。不要引入向量数据库或新的 LLM 调用。

### 3.3 规划 Prompt

在 `_travel_plan_prompt` 明确：

```text
四套 Look 必须是四套肉眼明显不同的完整穿搭。
任意两套在外套、内搭、下装、鞋履四个核心字段中至少有两个不同。
不能用同一件外套、同一件内搭和同一双鞋只替换下装来冒充新 Look。
```

现有一次自动修订逻辑继续复用；第一次输出差异不足时，把具体 pair 错误发回模型重做完整 JSON。

### 3.4 验收

- 本轮 A/C 和 B/D 计划作为 fixture 时必须失败；
- 四套穿搭满足最小两字段差异时通过；
- 因差异不足触发的自动修订最多一次；
- 计划冻结后规则不再重新计算旧任务。

## 4. 改造二：增加旅行场景实用性合同

### 4.1 当前问题

`old_town_walk` 和 `evening_stroll` 都使用尖头细跟高跟鞋，与石板路、碎石路和长时间步行冲突。视觉风格来自参考图，但 Recipe 应当优先保证旅行穿搭可执行。

### 4.2 扩展旅行合同

在 `PHOTO_TH_TRAVEL_OUTFIT_V2.json` 的每个 moment 增加开发侧配置：

```json
{
  "key": "old_town_walk",
  "mobility_level": "HIGH",
  "allowed_footwear_types": ["SNEAKER", "LOAFER", "FLAT", "MARY_JANE", "LOW_BOOT"],
  "forbidden_footwear_types": ["HIGH_HEEL", "STILETTO"]
}
```

建议第一版规则：

| moment | mobility | 禁止 |
|---|---|---|
| `airport_departure` | HIGH | `HIGH_HEEL`, `STILETTO` |
| `old_town_walk` | HIGH | `HIGH_HEEL`, `STILETTO` |
| `cafe_visit` | LOW | `STILETTO`，允许低跟鞋 |
| `evening_stroll` | HIGH | `HIGH_HEEL`, `STILETTO` |
| `shopping_day` | HIGH | `HIGH_HEEL`, `STILETTO` |
| `photo_spot` | MEDIUM | 允许低跟，禁止 `STILETTO` |
| `night_walk` | HIGH | `HIGH_HEEL`, `STILETTO` |

### 4.3 Look 结构

每个旅行 Look 新增必填枚举：

```json
{
  "footwear_type": "SNEAKER|LOAFER|FLAT|MARY_JANE|LOW_BOOT|LOW_HEEL|HIGH_HEEL|STILETTO|SANDAL"
}
```

`_normalize_travel_plan` 必须验证：

- `footwear_type` 在枚举中；
- 符合对应 `travel_moment` 的 allowed/forbidden 规则；
- `shoes` 文字与 `footwear_type` 没有明显矛盾，例如描述出现“细跟高跟鞋”却标记为 `FLAT` 时失败。

文字矛盾检测只覆盖有限的中文硬规则：`细跟/高跟/stiletto`、`运动鞋`、`乐福鞋`、`平底`、`短靴`、`玛丽珍`。不建设通用鞋类 NLP。

### 4.4 规划 Prompt

把 moment 的 mobility 和鞋履约束一起传给模型，并明确：

```text
参考图中的鞋履只能作为审美参考。机场、老城步行、傍晚散步等高步行场景必须优先使用舒适可行走鞋履，不能照搬细跟高跟鞋。
```

### 4.5 QA

旅行原图 QA 新增：

```json
"mobility_matches": true,
"observed_footwear_type": "LOAFER"
```

程序根据计划与观察结果判断，不能只相信模型的 `mobility_matches`。明显出现细跟鞋而计划是高步行场景时，使用 `MOBILITY_MISMATCH` 失败码并只重生该角色。

## 5. 改造三：统一温度表达合同

### 5.1 当前矛盾

Recipe 的 `content_card.comparison_basis_zh` 写着“不宣称温度数值”，但模板标题和正文会使用 `15–22°C`。计划还生成了机场 20°C、室外 16°C、咖啡馆 21°C 等无数据依据的精确值。

### 5.2 统一规则

修改 Recipe 文案为：

```text
允许把执行预设中的温度区间作为整篇穿搭条件展示；不生成具体地点、室内、机舱或某个时段的精确温度，不宣称保暖效果或保证穿着体验。
```

保留文章级：

```text
temperature_band = 15_22c
展示值 = 15–22°C
```

每页 `weather_logic` 只能表达：

- 室内外切换；
- 白天/傍晚温差；
- 防风、方便穿脱、步行舒适等搭配理由；
- 不出现新的数字、温度符号或精确温度。

在 `_normalize_travel_plan` 中拒绝 `weather_logic` 出现：

```regex
\d+\s*(?:°\s*C|℃|摄氏度|度)
```

如果模型第一次产生精确温度，进入现有自动修订；第二次仍出现则停止规划。

### 5.3 文案

泰语标题和 caption 可以使用模板填充后的文章级 `{temperature}`，但每页场景标签不增加新的温度值。

## 6. 改造四：收紧旅行语义 QA

### 6.1 禁止缺省通过

修改 `services/photo_travel_qa.py::normalize_travel_qa`。

当前以下写法必须删除：

```python
page.get("outfit_matches", True)
page.get("title_matches", True)
page.get("weather_matches", True)
raw.get("style_uniform", True)
```

每页必须完整提供：

```json
{
  "role": "look_a",
  "observed_moment": "airport_departure",
  "scene_evidence": ["航站楼落地窗", "登机箱"],
  "outfit_matches": true,
  "weather_matches": true,
  "mobility_matches": true,
  "observed_footwear_type": "FLAT",
  "repair_instruction": ""
}
```

顶层必须提供布尔值：

```json
"style_uniform": true
```

严格规则：

- 任何必需字段缺失或类型错误，整次 QA 返回结构错误并停止；
- `observed_moment` 为空或 `unknown` 必须失败；
- `scene_evidence` 必须是至少一个非空字符串的数组；
- `outfit_matches/weather_matches/mobility_matches/style_uniform` 必须是真正的 bool；
- 保留 scene evidence、观察鞋型和各项布尔值到 normalized result 与 manifest；
- 不再在原始无文字图片阶段检查 `text_clean`；
- 原始素材 QA 不宣称“无文字乱码”。

增加失败码：

```text
QA_SCHEMA_INCOMPLETE
UNKNOWN_SCENE
MOBILITY_MISMATCH
```

结构不完整属于模型 QA 失败，不得把四张图片默认为通过，也不要直接全量重生。先允许重新执行 QA 一次；第二次结构仍不完整才停止并保留原始响应。

### 6.2 QA Prompt

`_travel_qa_prompt` 中：

- `observed_moment` 可以是计划枚举或明确的 `unknown`；
- `scene_evidence` 改为可见证据字符串数组；
- 删除 raw image 的 `title_matches` 和 `text_clean`；
- 增加观察鞋型和步行适配；
- 要求不得仅复述计划，必须说明图片中实际看见的证据。

## 7. 改造五：增加最终套版页面检查

### 7.1 原因

旅行语义 QA 发生在文字套版前。当前 manifest 中 `title` 为空，质检却声称“无文字乱码”，说明最终泰语标题、裁切、遮挡和 CTA 没有被真正检查。

### 7.2 新增接口

在 `services/photo_reference_vision.py` 增加：

```python
review_travel_final_pages(
    image_paths,
    expected_texts,
    role_order,
)
```

输入最终 5 页 JPG：

- 第 1 页：封面；
- 第 2–5 页：A/B/C/D。

输出：

```json
{
  "pages": [
    {
      "index": 1,
      "text_readable": true,
      "text_matches_expected": true,
      "text_clipped": false,
      "text_garbled": false,
      "subject_obscured": false,
      "issues": []
    }
  ]
}
```

程序要求 5 页全部返回，关键字段缺失失败。

### 7.3 接入位置

在 `PhotoPackageExporter.export()` 完成最终 JPG 后、图文包被视为技术完成前执行。可新增独立 `TravelFinalPackageQAService`，由 `NativePhotoProductionFlow.prepare()` 在 exporter 返回后调用。

只对 `PHOTO_TH_TRAVEL*` 启用；Recipe ID 从冻结任务/Recipe snapshot 读取，不能从文件名猜测。

QA 证据写入 `photo_manifest_json`：

```json
"travel_final_page_qa": {}
```

然后重新计算 `package_fingerprint`。不需要数据库 migration。

最终页面检查失败时：

- 不自动重生原始人物图；
- 保留源素材；
- 阻止进入“技术完成”；
- 若是文字裁切/乱码，修复模板或文案后创建 rework revision 重新套版；
- 若是标题与场景不一致，修正文案或冻结计划，不能静默覆盖已冻结结果。

## 8. 泰语文案处理

当前 copy pack 全部为 `DRAFT`。本轮开发模型可以：

- 修正占位符、未解析 token、过长文字和套版问题；
- 输出建议修改稿供母语人员审核；
- 保持发布门禁。

开发模型不能：

- 自行将 `DRAFT` 改成 `NATIVE_APPROVED`；
- 删除发布门禁；
- 因样片看起来可读就宣布泰语已经通过母语审核。

发布前由母语人员审核至少四套模板，确认后再统一改状态。

## 9. 测试

在 `tests/test_photo_travel_semantics.py` 增加：

1. 最新计划 A/C 只差下装时失败；
2. B/D 只差下装时失败；
3. 任意两套至少两字段不同通过；
4. 老城步行使用 `STILETTO` 失败；
5. 傍晚散步使用 `HIGH_HEEL` 失败；
6. 咖啡馆使用 `LOW_HEEL` 通过；
7. `footwear_type` 与中文鞋履描述矛盾时失败；
8. `weather_logic` 包含 20°C/16度/21℃ 时失败；
9. 只有文章级 `temperature_band` 时通过；
10. `observed_moment` 缺失或 unknown 时失败；
11. `scene_evidence` 缺失、空数组或错误类型时失败；
12. 任一布尔字段缺失不能默认通过；
13. `style_uniform` 缺失不能默认通过；
14. QA 证据完整写入 manifest；
15. 场景错位仍只重生失败角色；
16. 最终页面 QA 必须覆盖 5 页；
17. 最终泰语文字裁切、乱码、标题错位能阻止技术完成；
18. 非旅行 Recipe 不触发旅行最终页面 QA。

执行：

```bash
python3 -m unittest tests.test_photo_travel_semantics tests.test_photo_package tests.test_photo_feishu_batch
```

当前全量测试基线暂时存在 Persona Pack 并行开发造成的 4 errors + 2 failures。不要通过删除人物预检或降低人物规则来让测试变绿；本开发线只保证不新增失败。两条开发线合并后必须执行：

```bash
python3 -m unittest discover -s tests
```

全量未通过前不能宣布生产可用。

## 10. 真实样片验收

代码和非付费测试通过后，新建一条飞书任务，不能复用或覆盖 `recvuve1J8mRyH`。

建议输入：

```text
生产预设：图文｜TH｜旅行穿搭
生成篇数：1
图文主题：凉爽旅行
参考图类型：风格参考
内容要求：15–22度凉爽城市旅行，机场、老城、咖啡馆、傍晚散步各一套；步行场景使用舒适鞋履；四套穿搭明显不同
```

非人物部分验收：

- 四个场景均有明确视觉证据；
- 任意两套 Look 至少两个核心单品不同；
- 机场、老城、傍晚没有细跟高跟鞋；
- 只出现文章级 15–22°C，不出现虚构室内精确温度；
- 原图旅行 QA 证据完整；
- 最终 5 页文字清晰、未裁切、未乱码；
- 页面标签与实际场景一致；
- 泰语仍为 DRAFT 时不能确认发布。

不要勾选确认发布。

## 11. 交给开发模型的指令

```text
请阅读 docs/TH_PHOTO_REFERENCE_MODULE_HANDOFF_20260907.md 和
docs/TH_TRAVEL_OUTFIT_NON_PERSONA_OPTIMIZATION_HANDOFF_20260907.md。

只实施后一个文档中的非人物改造：旅行 Look 差异、场景实用鞋履、温度表达合同、
严格旅行语义 QA 和最终套版页面 QA。人物 Persona Pack、AI 脸、头部姿态、动作合同
与人物表现 QA 正在另一条开发线修改，不要改动或回退相关文件逻辑。

先补测试，再修改实现。复用现有旅行两步规划、定向重生、Doubao 视觉客户端、
原生多图 exporter 和发布门禁，不新建第二套生产系统。不要把泰语文案状态自行改成
NATIVE_APPROVED，不调用真实付费生图，不发布。完成后报告修改文件、测试结果、
仍由 Persona 并行开发导致的已知失败，以及单条真实样片的准备情况。
```
