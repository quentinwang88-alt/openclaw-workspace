# 越南围巾图文跨市场复用实施规格

日期：2026-09-13  
状态：待开发  
适用项目：`packages/organic_photo_video`  
目标读者：接手实现、测试、灰度和交付的开发模型

## 0. 执行摘要

本次不是另建一套“越南围巾系统”，而是在现有原生图文工厂上完成一次可复用的解耦：

1. 将泰国旅行线已经存在的参考图处理抽成公共 `Reference Pipeline`；运营字段和使用方式保持不变。
2. 越南围巾继续使用现有 `ProductReferencePack`、`ProductReferenceResolver`、版本、变体、轮换和冻结机制；禁止新增国家专属商品图包。
3. 继续使用现有原创人物模板库与 `Persona Pack`；禁止新增越南专属人物库系统。
4. 将旅行穿搭抽成国家、语言和具体商品类目无关的 `TRAVEL_OUTFIT_V3`。
5. 新增轻量 `SCARF_V1` 类目适配器，只负责商品槽位、参考图取图优先级、展示规则和商品 QA。
6. 新增越南 Market Pack、越南语 Locale Pack、东亚冷凉目的地目录、越南账号绑定和生产预设。
7. 保留 `PHOTO_TH_TRAVEL_OUTFIT_V2` 及其冻结任务不变；新能力走并行的 V3 路径。

最终组合模型：

```text
ReferenceContext + ProductSnapshot + PersonaSnapshot
                         ↓
              Content Flow / Template
                         ↓
                 Category Adapter
                         ↓
       Market + Locale + Destination + Account
                         ↓
            Generation → QA → Package → Publish
```

业务语义：

```text
人物模板决定“谁来穿”
商品图包决定“穿什么”
风格参考决定“怎么搭、怎么拍”
旅行合同决定“在哪里、做什么”
Market/Locale 决定“给谁看、使用什么语言”
```

## 1. 目标与非目标

### 1.1 必须实现

- 同一个旅行内容模板可以运行 `TH + womenswear`、`VN + womenswear`、`VN + scarf`、`TH + scarf`。
- 围巾使用参考图的方式与当前泰国旅行线一致。
- 围巾商品使用现有商品图包机制；相同产品图包可以跨 VN/TH 任务复用。
- 真人生成继续使用现有人物模板库、人物包门禁、账号默认人物和允许人物池。
- 越南用户和旅行目的地国家分开建模，例如：

```text
audience_market     = VN
destination_country = KR
destination_city    = Seoul
```

- 指定围巾在 A/B/C/D 全部页面中存在、可辨认且保持同一产品变体。
- 越南语内容不得回退为泰语；未通过母语审核不得进入正式发布。
- 旧 TH V2 任务、manifest、缓存和发布包继续可读、可重试、可追溯。

### 1.2 明确不做

- 不新增越南商品图包表、目录或数据库模型。
- 不新增围巾参考图字段或围巾专属参考模式。
- 不新增越南人物模板库系统。
- 不修改现有商品图包的 `front/back/side/detail/lifestyle` 角色集合。
- 不在 V1 新建围巾专属旅行规划器、图片生成器或发布器。
- 不在 V1 建设独立“纯商品旅行模式”；商品融入优先沿用“风格参考 + 产品编码”。
- 不在 V1 改造温度分层、冷热切换、MX 假发等其它业务线。
- 不接入实时天气；只使用配置中冻结的宽泛温度档和季节语义。
- 不承诺复杂 Logo、细密花纹或织物纹理的像素级复刻。
- 不直接覆盖或重命名 `PHOTO_TH_TRAVEL_OUTFIT_V2`。

## 2. 当前基线与必须保留的行为

### 2.1 参考图

当前入口：

- `services/photo_reference.py`
- `services/photo_asset_supply.py`
- `services/photo_reference_vision.py`
- `services/photo_style_reference_supply.py`
- `services/feishu_workflow.py`

必须保留：

- `自动判断 / 风格参考 / 商品参考 / 完整穿搭` 现有字段值兼容。
- 显式 `STYLE + product_id` 时，风格图仅作为灵感，商品图包作为商品身份权威。
- 风格图不得写入商品图包。
- 参考原图哈希、视觉分析、旅行计划和输入指纹可追溯。
- 冻结任务的参考图、主题、内容要求或篇数变化时拒绝续跑。
- `COMPLETE_LOOK` 每篇按 A/B/C/D 提供四张图，不重新生成。

### 2.2 商品图包

唯一机制：

- `services/product_reference_resolver.py`
- `services/operation_product_pack_source.py`
- 现有 `ProductReferencePack` 数据模型与 repository 方法

必须保留：

- 一个运营记录对应一个候选图包，不跨记录自动合并。
- 文件可读检查、按字节哈希去重、`asset_fingerprint`。
- `variant_key`、默认图包、图包版本、`ready/limited/blocked/retired`。
- 同批次稳定轮换和历史少用优先。
- 快照冻结 `reference_pack_id/reference_pack_version/variant_key`。
- 文件内容变化时要求重新入库。
- 商品图包本身没有 audience market 字段。

### 2.3 人物模板库

唯一机制：

- 原创人物模板库及 `LightTryonAssetReader`
- `services/persona_pack.py`
- 账号的 `persona_ref_id`
- 账号 `operating_rules.allowed_persona_refs`

必须保留：

- 真人场景人物包至少三张已批准本地参考。
- 必须包含 face 和 full-body 证据。
- 默认生图身份输入使用主脸图和合适的全身图。
- 风格参考人物、商品图模特不能成为生成人物身份。
- 同篇 A/B/C/D 使用同一人物包。
- 单页重生继续使用冻结人物，不能中途换人。
- `FLAT_LAY` 不要求人物模板；`COMPLETE_LOOK` 使用上传素材中的人物，不重新套人。

### 2.4 旅行流程

必须复用：

- `travel_two_step`
- A/B/C/D 有序角色
- 每页独立 `travel_moment/scene_prompt/weather_logic/footwear_type`
- 场景证据、步行强度、鞋履、天气感、人物灾难级问题检查
- 商品不匹配时按角色定向重生
- 断点续跑和历史证据
- P1 使用 Look A 作为封面来源、P2-P5 对应 A-D

## 3. 目标分层

### 3.1 公共资产层

#### A. Reference Pipeline

新增一个薄编排层，不复制底层实现：

```text
services/photo_reference_context.py
```

建议数据合同：

```python
@dataclass(frozen=True)
class PhotoReferenceContext:
    reference_mode: str
    reference_paths: tuple[str, ...]
    style_reference_paths: tuple[str, ...]
    complete_look_sources: tuple[dict, ...]
    product_snapshot: dict
    product_reference_paths: tuple[str, ...]
    product_context: dict
    input_fingerprint: str
```

建议公共入口：

```python
def resolve_photo_reference_context(
    *, record, recipe, account, quantity, required_roles,
    product_reference_resolver, asset_supply,
) -> PhotoReferenceContext:
    ...
```

约束：

- 只编排现有 `resolve_reference_mode()`、附件暂存和 `ProductReferenceResolver`。
- 不在该模块生成文案或执行旅行规划。
- 不将 `STYLE + product_id` 改成新 UI 枚举。
- 首次迁移应确保 TH V2 现有测试输出不变。

#### B. Product Reference Pack

不新增服务。只做两个小扩展：

1. `services/operation_product_pack_source.py` 增加精确别名：

```python
"围巾": "scarf"
"披肩围巾": "scarf"
"scarf": "scarf"
```

2. `services/product_reference_resolver.py` 的通用商品名增加：

```python
"scarf": "目标围巾"
```

禁止给 ProductReferencePack 增加国家字段。商品是否允许在某市场使用，应由任务/账号/产品资格层判断。

#### C. Persona Library / Pack

不新增服务。将人物快照纳入统一执行合同：

```text
persona_ref_id
persona_pack_id
persona_reference_hashes
persona_pack_readiness
```

账号继续负责：

- 默认人物 `persona_ref_id`
- 可轮换人物池 `allowed_persona_refs`

越南围巾只需要在公共库中登记/选择经运营审核的人物，并绑定 VN 账号；不复制人物库技术实现。

### 3.2 公共内容层

新建并行的通用 Recipe schema，旧 v1 保持兼容：

```text
schema_version = opv-photo-recipe-v2
recipe_id      = PHOTO_TRAVEL_OUTFIT_V3
planning_flow  = travel_two_step
```

建议 v2 能力字段：

```json
{
  "market_policy": "MARKET_PACK_REQUIRED",
  "required_category_capabilities": [
    "wearable_styling",
    "travel_look",
    "product_embedding"
  ],
  "locale_copy_packs": {
    "th-TH": "TH_TRAVEL_OUTFIT_V3",
    "vi-VN": "VN_TRAVEL_OUTFIT_V1"
  }
}
```

通用 Recipe 保留：

- `story_structure`
- `travel_contract`
- `required_roles`
- `supported_presentations`
- `planning_flow`
- 页面结构、场景语义和 QA 能力声明

通用 Recipe 删除：

- 固定 `markets: ["TH"]`
- 固定 `category_key: womenswear`
- 固定 `locale: th-TH`
- `label_th`
- `destination_labels_th`
- `temperature_labels_th`
- 泰语固定文案

旧 `opv-content-recipe-v1`/`opv-photo-recipe-v1` 继续正常加载；不要一次性迁移所有 Recipe。

### 3.3 类目适配层

新增：

```text
services/photo_category_registry.py
config/categories/SCARF_V1.json
```

建议 Category Adapter 合同：

```python
@dataclass(frozen=True)
class PhotoCategoryAdapter:
    category_key: str
    capabilities: tuple[str, ...]
    accepted_product_categories: tuple[str, ...]
    main_product_slot: str
    product_label_zh: str
    required_product_roles: tuple[str, ...]
    product_reference_priority_by_slot: dict[str, tuple[str, ...]]
    identity_attributes: tuple[str, ...]
```

公共函数：

```python
def get_photo_category_adapter(category_key: str) -> PhotoCategoryAdapter:
    ...

def apply_target_product_to_look(
    *, adapter, look, product_snapshot,
) -> dict:
    ...

def build_product_qa_contract(*, adapter, product_snapshot) -> dict:
    ...
```

`SCARF_V1` 建议配置：

```json
{
  "schema_version": "opv-category-profile-v2",
  "category_key": "scarf",
  "category_name": "围巾",
  "status": "active",
  "capabilities": [
    "wearable_styling",
    "travel_look",
    "product_embedding"
  ],
  "accepted_product_categories": ["scarf"],
  "main_product_slot": "accessories",
  "product_label_zh": "目标围巾",
  "required_product_roles": ["look_a", "look_b", "look_c", "look_d"],
  "identity_attributes": [
    "dominant_color",
    "pattern_family",
    "material_appearance",
    "edge_or_fringe",
    "length_volume"
  ],
  "product_reference_priority_by_slot": {
    "full_look": ["front", "lifestyle", "detail"],
    "hero": ["front", "lifestyle", "detail"],
    "detail": ["front", "detail"]
  }
}
```

首个 WOMENSWEAR adapter 必须复刻当前逻辑，作为无行为变化的回归基线。之后 `photo_reference_vision.py` 和 `photo_style_reference_supply.py` 不再自行维护 `target_fields`/`outerwear` 分支。

### 3.4 市场、本地化和目的地层

#### Market Pack

新增：

```text
config/market_packs/MP_VN_DEFAULT_v1.json
```

内容包括：

- `target_country = VN`
- `target_locale = vi-VN`
- `timezone = Asia/Ho_Chi_Minh` 或由账号提供
- 越南市场视觉偏好、内容安全、发布时段、内容方向权重
- `fallback_allowed = false`

Market Pack 不包含具体 Recipe 页面结构和商品图包。

#### Locale Pack

新增 loader 与配置目录：

```text
config/locales/LOCALE_TH_TH_V1.json
config/locales/LOCALE_VI_VN_V1.json
```

最小结构：

```json
{
  "schema_version": "opv-photo-locale-pack-v1",
  "locale_pack_id": "LOCALE_VI_VN_V1",
  "locale": "vi-VN",
  "fallback_allowed": false,
  "labels": {
    "travel_moments": {},
    "destinations": {},
    "temperature_bands": {},
    "generic": {}
  }
}
```

具体标题、Caption、Hashtag 和五页文案继续使用现有 TSV copy pack 机制：

```text
config/copy_packs/VN_TRAVEL_OUTFIT_V1.tsv
config/copy_packs/VN_SCARF_MATCHING_V1.tsv
```

正式发布前所有实际使用的越南语模板必须为 `NATIVE_APPROVED`；开发期允许 DRAFT，但发布门禁必须阻断。

#### Destination Catalog

新增：

```text
config/destinations/EAST_ASIA_COOL_V1.json
```

目的地条目不保存发布语言，只保存语义事实：

```json
{
  "destination_id": "SEOUL_WINTER",
  "destination_country": "KR",
  "destination_city": "Seoul",
  "climate_family": "cold_city",
  "temperature_bands": ["minus_10_0c", "0_5c", "5_10c"],
  "allowed_seasons": ["winter"],
  "travel_moments": [
    "airport_departure",
    "city_walk",
    "cafe_visit",
    "shopping_day",
    "evening_stroll"
  ],
  "snow_scene_policy": "OPTIONAL_NOT_DEFAULT"
}
```

V1 目的地范围：

- VN：Hanoi winter、Sa Pa、Ha Giang、Da Lat
- KR：Seoul、Gangwon/Pyeongchang
- JP：Tokyo、Osaka/Kyoto、Sapporo/Hokkaido
- CN：Beijing、Shanghai、Harbin

要求：

- Tokyo/Shanghai 等不得默认生成雪景。
- 雪景仅对明确的 snow destination/profile 开放。
- 不生成当天实时温度；每篇只冻结一个宽泛温度档。

### 3.5 账号与发布层

新增账号配置时继续使用现有 AccountProfile schema：

```text
config/accounts/<VN_SCARF_ACCOUNT>.json
```

必须配置：

- `target_country = VN`
- `default_locale = vi-VN`
- `persona_ref_id`
- `allowed_persona_refs`
- `default_market_pack_id = MP_VN_DEFAULT_V1`
- 原生图文发布账号映射

新增生产预设：

```text
图文｜VN｜围巾旅行
图文｜VN｜围巾搭配四选一
```

预设仅负责绑定：

```text
recipe + category + market + locale + account + routing policy
```

不得在预设里复制旅行合同、围巾 QA 或商品图包逻辑。

当前 `config/main_publish_routes.json` 尚无 VN 路由。发布路由只能在确认一个 VN 账号具备 TikTok Content Posting 原生图文能力后启用；内容生成开发不应被此阻塞。

## 4. 运行时解析与冻结合同

新增统一的 resolved execution snapshot。名称可调整，但字段语义必须保留：

```json
{
  "schema_version": "opv-photo-execution-context-v1",
  "recipe": {
    "id": "PHOTO_TRAVEL_OUTFIT_V3",
    "version": 3,
    "planning_flow": "travel_two_step"
  },
  "category": {
    "key": "scarf",
    "profile_id": "SCARF_V1",
    "profile_version": 1,
    "main_product_slot": "accessories"
  },
  "market": {
    "country": "VN",
    "market_pack_id": "MP_VN_DEFAULT_V1",
    "market_pack_version": 1
  },
  "locale": {
    "locale": "vi-VN",
    "locale_pack_id": "LOCALE_VI_VN_V1",
    "locale_pack_version": 1,
    "copy_pack_id": "VN_TRAVEL_OUTFIT_V1"
  },
  "destination": {
    "destination_id": "SEOUL_WINTER",
    "destination_country": "KR",
    "destination_city": "Seoul"
  },
  "reference": {
    "mode": "STYLE",
    "input_fingerprint": "...",
    "reference_hashes": []
  },
  "product": {
    "product_id": "...",
    "reference_pack_id": "...",
    "reference_pack_version": 1,
    "variant_key": "...",
    "asset_fingerprint": "..."
  },
  "persona": {
    "persona_ref_id": "...",
    "persona_pack_id": "...",
    "reference_hashes": []
  }
}
```

冻结规则：

- task 创建后，所有版本、ID、图片哈希和输入指纹不可静默变化。
- 重试必须使用同一个 execution context。
- 用户改变参考图、产品、人物、地点、主题或篇数时必须新建任务。
- Market/Locale 后续升级不影响已冻结任务。

## 5. 越南围巾业务配置

### 5.1 内容定位

目标：越南年轻女性的围巾搭配灵感，以及越南国内和东亚冷凉目的地旅行穿搭。

内容主角始终是围巾；旅行、通勤、咖啡和拍照是场景。

### 5.2 两条内容线

#### 围巾日常搭配

```text
MATCHING_CHOICE_V2
× SCARF_V1
× MP_VN_DEFAULT_V1
× LOCALE_VI_VN_V1
```

内容方向：

- 同一条围巾搭配四套 Look
- 同一套衣服的围巾配色选择
- 围巾与外套配色
- 河内转凉日常
- 通勤、咖啡和傍晚出行

商品模式下一篇只允许一个 `product_id + variant_key`，不得在 A/B/C/D 自动换色。

#### 围巾旅行穿搭

```text
PHOTO_TRAVEL_OUTFIT_V3
× SCARF_V1
× MP_VN_DEFAULT_V1
× LOCALE_VI_VN_V1
× EAST_ASIA_COOL_V1
```

旅行主题优先级：

1. 配色参考
2. 拍照友好
3. 四套 Look 选择
4. 环境协调
5. 打卡穿搭
6. 温度穿搭（谨慎）

温度只作为整套服装语境，不允许声称单条围巾保证保暖。

### 5.3 页面结构

V1 继续复用 TH 旅行线五页结构：

| 页 | 内容 | 素材来源 |
|---|---|---|
| P1 | 旅行主题封面 | Look A，同源安全裁切 |
| P2 | Look A | look_a |
| P3 | Look B | look_b |
| P4 | Look C | look_c |
| P5 | Look D + CTA | look_d |

围巾适配仅增加：

- P1 允许偏上半身裁切，但不额外生图。
- P2-P5 保持当前旅行完整 Look 结构。
- 指定商品时，围巾必须出现在 look_a..look_d。
- 不新增独立商品详情页；如后续需要电商证明型内容，另立独立 Recipe，不污染旅行 V1。

### 5.4 运营输入

#### A. 纯风格

```text
参考图类型 = 风格参考
参考图       = 1-3 张
产品编码     = 空
```

#### B. 风格 + 自有围巾（V1 推荐）

```text
参考图类型 = 风格参考
参考图       = 1-3 张
产品编码     = 已有有效商品图包的围巾编码
```

执行优先级：

```text
商品图包 > 冻结旅行计划 > 风格参考 > 模型自由发挥
```

#### C. 完整穿搭

```text
参考图类型 = 完整穿搭
参考图       = 每篇 A/B/C/D 共 4 张
```

不重新生成，不自动替换围巾。若上传图未包含自有商品，不得宣称已经融入自有商品。

### 5.5 人物策略

- 从现有公共人物模板库中选择 2-3 个经运营审核、适合 VN 围巾账号定位的人物。
- 每个人物包必须通过现有 `evaluate_persona_pack()`。
- 同篇固定一个人物，不同篇可按账号允许池稳定轮换。
- 风格参考图人物只提供穿搭/环境/摄影信息。
- 商品图模特只提供商品展示信息。
- 围巾可遮挡颈部和部分胸前，不能遮脸。
- 不能为展示围巾改变脸型、异常拉长颈部或造成头肩畸变。

### 5.6 围巾 QA 合同

指定商品时，每页输出至少包含：

```json
{
  "role": "look_a",
  "product_present": true,
  "product_matches": true,
  "visibility_sufficient": true,
  "dominant_color_matches": true,
  "pattern_family_matches": true,
  "edge_or_fringe_matches": true,
  "length_volume_plausible": true,
  "face_unobscured": true,
  "repair_instruction": ""
}
```

确定性判定：

- `product_present=false`：失败。
- `product_matches=false`：失败。
- `visibility_sufficient=false`：失败。
- 主色或图案家族明显错误：失败。
- 边缘/流苏轻微形变：warning；明显消失或换结构：失败。
- 细密纹理微差：warning，不冒充像素级一致。
- 普通非目标配饰缺失仍可为 MINOR。
- 指定围巾不能套用“配饰缺失为 MINOR”的宽松规则。
- 失败继续使用现有 `failed_roles_from_travel_qa()` 定向重生。

## 6. 逐文件改造清单

### 6.1 新增文件

- `services/photo_reference_context.py`
- `services/photo_category_registry.py`
- `config/categories/SCARF_V1.json`
- `config/market_packs/MP_VN_DEFAULT_v1.json`
- `config/locales/LOCALE_TH_TH_V1.json`
- `config/locales/LOCALE_VI_VN_V1.json`
- `config/destinations/EAST_ASIA_COOL_V1.json`
- `config/recipes/PHOTO_TRAVEL_OUTFIT_V3.json`
- `config/photo_planning_policies/TRAVEL_OUTFIT_V3.json`
- `config/copy_packs/VN_TRAVEL_OUTFIT_V1.tsv`
- `config/copy_packs/VN_SCARF_MATCHING_V1.tsv`
- 对应单元测试文件，名称遵循现有 `tests/test_photo_*.py`

### 6.2 修改文件

#### `config/loader.py`

- 支持 `opv-photo-recipe-v2`。
- 新增 locale pack、destination catalog loader 和验证。
- 保持所有 v1 loader 行为不变。

#### `domain/contracts.py` / 相关 photo contracts

- 增加 recipe v2、category profile v2、locale pack、destination catalog、execution context 校验。
- v1 合同不得放宽或移除。

#### `services/photo_content_planner.py`

- 注册 `PHOTO_TRAVEL_OUTFIT_V3` 通用 policy。
- 通用路径不再使用 `plan_th_*` 命名；保留旧函数兼容包装。
- 校验 recipe 所需 category capability。

#### `services/photo_reference_vision.py`

- 通用 V3 从 Locale Pack 读取 moment/destination/temperature 标签。
- 替换 `label_th`、`destination_labels_th`、`temperature_labels_th`。
- 将本地 `target_fields` 映射替换为 Category Adapter。
- 规划、生成和 QA 继续同时看到商品图和风格图。
- 加入围巾 QA 观察字段；旧 V2 QA 输出继续兼容。

#### `services/photo_style_reference_supply.py`

- locale 从 frozen request/execution context 读取，移除通用 V3 路径的固定 `th-TH`。
- 使用 Category Adapter 写入目标商品槽位。
- 使用 Category Adapter 选择商品参考图角色优先级。
- 继续使用现有人物包、身份参考和定向重生逻辑。

#### `services/image_generator.py`

- 商品显示名通过 Category Adapter 解析。
- `scarf` 写入 `outfit_state.accessories`。
- 保留通用商品身份锁；增加围巾不得缺失、不得换款的可验证指令。
- 人物身份锁与商品身份锁继续分离。

#### `services/photo_copy.py`

- 按 locale pack 解析 destination/temperature/moment 标签。
- 通用 V3 禁止 `_th` 字段依赖。

#### `services/photo_theme.py`

- 新 V3 主题使用 locale map/locale pack。
- 不在新路径使用 `thai_fallback`。
- 旧 V2 fallback 保持兼容。

#### `services/photo_request_factory.py`

- 通用 V3 根据 recipe capabilities、category profile、Market Pack 校验兼容性。
- 不再依赖 `PHOTO_TH_TRAVEL` 前缀。

#### `services/photo_package.py`

- 旅行最终 QA 按 `planning_flow=travel_two_step` 或 registry capability 路由。
- 旧 V2 继续可进入同一 QA。

#### `services/feishu_workflow.py`

- 使用 `PhotoReferenceContext`，移除新 V3 分支中重复的参考解析。
- 旅行路由、封面选择、发布门禁按 Flow capability 判断。
- locale QA 使用冻结的 `target_locale`，不写死 `th-TH`。
- 保留现有运营字段和状态机。

#### `services/operation_product_pack_source.py`

- 仅增加 scarf 类目别名，不改变商品图包同步机制。

#### `services/product_reference_resolver.py`

- 仅增加通用名称“目标围巾”；不增加国家逻辑。

#### `services/main_schedule_bridge.py`

- 类目/商品本地称呼从 locale/category 配置读取。
- 不写死泰语商品名。

#### `config/feishu_production_presets.json`

- 首先增加 disabled/canary 的 VN 围巾预设。
- 完成账号、文案、视觉验收后再改 active。

#### `config/main_publish_routes.json`

- 仅在 VN Content Posting 原生图文账号确认后增加 VN 路由。

#### 部署脚本

- 不新增只服务 VN 的长期脚本。
- 将 `scripts/deploy_th_photo_planner.py` 的可复用部分抽成通用 `deploy_photo_planner.py`；保留旧入口作为兼容包装。

## 7. 开发阶段与提交边界

### Phase 0：基线与防回归

1. 记录当前 dirty worktree；不得 reset、checkout 或覆盖用户现有变更。
2. 跑现有旅行、参考图、商品图包、人物包、发布包测试。
3. 为 TH V2 关键输出补 golden/contract 测试。
4. 不做真实生图或发布。

交付：基线测试记录。

### Phase 1：抽公共能力，不改变行为

1. 新增 `PhotoReferenceContext`。
2. 新增 Category Adapter registry，并先只注册 WOMENSWEAR 兼容实现。
3. 将 TH V2 参考解析和商品槽位调用迁移到公共入口。
4. 确认 TH V2 测试输出与迁移前一致。

交付：公共 facade/registry；零业务行为变化。

### Phase 2：通用旅行 V3

1. 增加 recipe v2 合同。
2. 新增 `PHOTO_TRAVEL_OUTFIT_V3` 和通用 policy。
3. 新增 Locale Pack 与 Destination Catalog。
4. 用 TH + WOMENSWEAR 绑定 V3 做离线等价验证。
5. 旧 V2 仍保持 active/原路由，V3 仅 canary。

交付：国家无关旅行模板。

### Phase 3：SCARF 类目

1. 新增 `SCARF_V1`。
2. 增加 scarf 类目别名和通用名称。
3. 加入 `accessories` 商品槽位、参考图优先级和 QA 合同。
4. 先在临时任务中验证 TH + SCARF，证明类目与国家解耦。

交付：一次实现、可跨市场复用的围巾适配器。

### Phase 4：VN 激活配置

1. 新增 VN Market Pack 与越南语 Locale/Copy Pack。
2. 新增目的地权重。
3. 选择并验收 2-3 个人物模板，绑定 VN 测试账号。
4. 新增 disabled VN 生产预设。
5. 生成三类离线 canary：STYLE、STYLE+PRODUCT、COMPLETE_LOOK。

交付：VN 围巾离线生产能力。

### Phase 5：发布与首批内容

1. 验证 VN TikTok Content Posting photo capability。
2. 配置 VN 发布路由。
3. 做单篇低风险发布验证。
4. 人工检查 TikTok 端图片顺序、越南语、封面、商品和人物。
5. 再启用首批 12 篇。

交付：VN 围巾正式可发布。

每个 Phase 独立提交；不得把抽象重构、VN 文案和生产路由混在同一提交。

## 8. 测试矩阵

### 8.1 单元测试

- ReferenceContext：STYLE、STYLE+product_id、COMPLETE_LOOK、AUTO 兼容。
- STYLE 图绝不进入 ProductReferencePack。
- 相同 product_id/variant 在 VN 与 TH 解析出相同商品快照。
- Persona Pack 缺 face/full-body/三张参考时阻断。
- 同篇人物冻结，重生不得换 persona。
- SCARF adapter 将商品写入 `accessories`。
- SCARF adapter 对不精确的 `accessory` 类目拒绝或要求归一为 `scarf`。
- 围巾 full-look 参考图选择优先 `front/lifestyle/detail`。
- Locale Pack 缺 vi-VN 文案时 fail loudly。
- `audience_market` 与 `destination_country` 不得互相覆盖。
- snow policy 禁止 Tokyo/Shanghai 默认生成雪景。

### 8.2 回归测试

- `PHOTO_TH_TRAVEL_OUTFIT_V2` 旧配置可加载。
- 旧 TH V2 STYLE 计划、文案、A-D 顺序、封面选择不变。
- 旧冻结 task/manifest 可以继续恢复。
- 商品图包版本和轮换测试全部通过。
- 人物包和人物身份测试全部通过。
- 旅行语义、目标角色重生和最终页面 QA 全部通过。

### 8.3 组合测试

| Recipe | Category | Market | Reference | Product | 预期 |
|---|---|---|---|---|---|
| TH V2 legacy | womenswear | TH | STYLE | 无 | 不回归 |
| V3 | womenswear | TH | STYLE | 有 | 通用 V3 等价可用 |
| V3 | womenswear | VN | STYLE | 无 | 仅市场/语言变化 |
| V3 | scarf | VN | STYLE | 无 | 纯围巾灵感内容 |
| V3 | scarf | VN | STYLE | 有 | 同一围巾进入 A-D |
| V3 | scarf | VN | COMPLETE_LOOK | 图中已有 | 不生图、直接排版 |
| V3 | scarf | TH | STYLE | 有 | 只增加 TH 激活配置即可运行 |

### 8.4 视觉 canary

至少生成：

1. VN + scarf + STYLE，无商品。
2. VN + scarf + STYLE + PRODUCT，Seoul。
3. VN + scarf + STYLE + PRODUCT，Sapporo snow。
4. VN + scarf + COMPLETE_LOOK。
5. TH + scarf + STYLE + 同一个商品图包。
6. TH + womenswear + V3，用于与 V2 对照。

人工验收：

- 人物身份一致。
- 风格参考人物未被复制。
- 指定围巾 A-D 均存在。
- 围巾主色、图案家族、边缘/流苏、长度体积基本一致。
- 围巾没有遮脸，头发没有完全遮商品。
- 地点、旅行时刻、天气感和鞋履合理。
- 越南语无泰语泄漏、乱码和不自然断行。
- P1 裁切后围巾清晰，P2-P5 保持旅行 Look 可读性。

## 9. 发布门禁

正式启用 VN 预设前必须全部满足：

- VN Market Pack active。
- 越南语 Copy Pack 全部 `NATIVE_APPROVED`。
- VN 账号已绑定通过预检的人物包。
- 至少一个 VN 账号验证 `content_photo_capable=true`。
- CreatOK/TikTok Content Posting 连接健康。
- 单篇真实发布验证通过。
- 发布对象与飞书最终预览、release manifest 一致。
- 没有新增 `PHOTO_VN_*` 服务级 prefix 分支。

当前已知：本地 VN 账号仍处于暂停/未确认原生图文能力状态；开发模型不得把“生成端完成”描述为“已可正式发布”。

## 10. 首批 12 篇配置

| 内容家族 | 数量 | 商品建议 |
|---|---:|---|
| 围巾日常搭配与配色 | 5 | 2 篇带真实产品 |
| 越南国内凉爽旅行 | 2 | 1 篇带真实产品 |
| 韩国旅行 | 2 | 1 篇带真实产品 |
| 日本旅行 | 2 | 可纯风格或 1 篇带商品 |
| 中国旅行 | 1 | 纯风格探索 |

输入模式建议总量：

- STYLE：6 篇
- STYLE + PRODUCT：4 篇
- COMPLETE_LOOK：2 篇

此分配用于验证能力，不是长期固定内容比例。24h/72h 按内容家族、目的地和是否带商品分别观察；纯养号内容不能直接验证 149k-299k VND 的价格接受度。

## 11. Definition of Done

只有同时满足以下条件才算完成：

### 架构完成

- Reference Pipeline、ProductReferencePack、Persona Pack 均只有一套公共实现。
- 通用旅行 V3 路径无固定 TH/vi/th 前缀业务判断。
- 新增一个市场只需添加 Market、Locale、账号、目的地权重和 preset 配置，不改核心 Python。
- SCARF adapter 完成后，新增另一个国家围巾线只需配置，不复制类目代码。
- 所有 resolved context 都冻结版本和图片哈希。
- 旧 TH V2 不回归。

### VN 围巾完成

- STYLE、STYLE+PRODUCT、COMPLETE_LOOK 三种运营方式通过。
- 商品图包与 TH 女装使用同一机制。
- 人物模板库与 TH 使用同一机制。
- 指定围巾在 A-D 中持续存在且可辨认。
- 单页商品/场景失败可定向重生。
- 越南语无泰语泄漏并通过母语审核。
- VN 原生图文真实发布验证通过。

## 12. 回滚与安全要求

- 旧 TH V2 Recipe、policy、copy pack 和 preset 不删除、不覆盖。
- V3 与 VN preset 默认 disabled/canary。
- 数据库变更只能是可回滚的增量字段/表；不得破坏旧行。
- 冻结任务不做批量重写。
- 发布路由最后启用；回滚只需禁用 VN preset/route，不影响生成数据。
- 不使用 `git reset --hard`、不覆盖当前 dirty worktree、不清理未知未跟踪文件。
- 真实生图、外部写入、飞书 schema 修改和 TikTok 发布必须分别经过对应阶段门禁。

## 13. 开发模型交付物

每阶段必须交付：

1. 修改文件清单。
2. 新增/变更合同说明。
3. 向后兼容说明。
4. 执行过的测试命令与完整结果。
5. 未执行的真实调用说明。
6. canary 产物路径与 manifest。
7. 已知限制和下一阶段阻塞项。

不得只交付“代码已完成”结论而没有测试和冻结证据。

## 14. 可直接交给开发模型的启动指令

将下面内容与本规格一起提供给开发模型；默认只执行一个 Phase，验收后再进入下一 Phase：

```text
你需要在 packages/organic_photo_video 中实现
docs/VN_SCARF_CROSS_MARKET_IMPLEMENTATION_SPEC_20260913.md。

先执行 Phase 0；未经上一阶段验收，不得跳到后续阶段，不得提前启用 VN
生产 preset、发布路由、真实飞书写入、真实生图或 TikTok 发布。

工作区已有用户修改。开始前先记录 git status；不得 reset、checkout、stash、
删除或覆盖与本任务无关的修改。遇到目标文件已有重叠修改时，先阅读并在现状上做
最小增量改造，不得恢复旧版本。

实现优先级：
1. 冻结合同、兼容性和行为约束是强制项。
2. 分层边界与能力归属是强制项。
3. 本文建议的类名、函数名可以因现有代码风格微调，但交付时必须给出映射。
4. 不得用 PHOTO_VN_*、SCARF_VN_* 服务分支替代公共抽象。
5. 不得新建第二套参考图、商品图包或人物模板库机制。

每个 Phase 结束后停止，并提交：修改文件、合同变化、兼容说明、测试结果、
未执行的外部调用、风险和下一阶段条件。测试不通过时不得以“与本次无关”直接略过，
必须给出失败用例、复现命令以及确认依据。
```

### 14.1 开工前命令

```bash
cd /Users/likeu3/.openclaw/workspace/packages/organic_photo_video
git status --short
PYTHONPATH=.:tests /usr/bin/python3 -m unittest \
  tests.test_photo_reference_vision \
  tests.test_photo_content_planner \
  tests.test_photo_theme_reference \
  tests.test_photo_asset_supply \
  tests.test_photo_persona_pack \
  tests.test_product_reference_resolver \
  tests.test_photo_travel_semantics \
  tests.test_photo_package \
  tests.test_photo_feishu_batch \
  tests.test_feishu_workflow
```

项目已知使用标准库 `unittest`；不要因本机没有 `pytest` 将环境问题误判为代码失败。

### 14.2 每阶段最低验证

聚焦 photo 域：

```bash
PYTHONPATH=.:tests /usr/bin/python3 -m unittest discover -s tests -p 'test_photo_*.py'
```

全量回归：

```bash
PYTHONPATH=.:tests /usr/bin/python3 -m unittest discover -s tests -p 'test_*.py'
git diff --check
```

Phase 2 起还必须输出至少一份 resolved execution context fixture；Phase 3 起输出
`TH + scarf` 和 `VN + scarf` 的同商品图包快照对照；Phase 4 起输出三种参考模式的
离线 manifest；Phase 5 才允许提供真实发布证据。

### 14.3 必须暂停并上报的情况

- 现有用户修改与目标文件存在无法安全合并的语义冲突。
- 必须做破坏性数据库迁移才能继续。
- 旧 TH V2 golden/contract 测试在公共抽取后发生变化。
- VN 文案未达到 `NATIVE_APPROVED`，却要求启用正式发布。
- 没有验证可用的 VN 原生图文账号，或账号连接/权限预检失败。
- 只能通过复制参考图、商品图包或人物模板库实现才能继续。
