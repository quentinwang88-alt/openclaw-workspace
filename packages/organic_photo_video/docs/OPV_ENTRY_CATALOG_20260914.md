# OPV 入口清单：预设目录与「预设→配方→流程→主题→语言→排版→输出」对照

> 本文件由 `scripts/photo_entry_catalog.py` 生成，**请勿手改**。配置改动后重新生成：
> `PYTHONPATH=. /usr/bin/python3 scripts/photo_entry_catalog.py --output <本文件>`
>
> 用途：排查「这条线归谁管、主题能选什么、文案用哪种语言、出几张什么版式、发到哪家店」。
> 所有取值都从 `config/` 与配方文件里读出来，**不是**第二份执行配置。

## 口径

- `entry_group` 只控制目录与视图：`production`=运营日常入口、`trial`=已配置待验收、
  `legacy`=保留的历史入口。**能否使用仍由预设 `status` 决定**，非 `active` 一律拒绝。
- 「主题范围」＝该配方规划策略的 `supported_theme_keys`；空则说明该配方不接规划策略。
- 「主题缺省」＝`services/feishu_workflow.theme_is_optional`（唯一归属，工作流与本文共用）。
- 「文案来源」：声明了 `locale_copy_packs` 的国家无关配方由语言包提供；v1 配方随
  「图文主题」携带内联泰语。

## 1. 预设目录


| 分组 | 预设 | 状态 | 可用 | 类型 | 市场 | 配方 |
|---|---|---|---|---|---|---|
| production | 图文｜TH｜旅行穿搭 | active | ✅ | native_photo | TH | `PHOTO_TH_TRAVEL_OUTFIT_V2` |
| production | 图文｜TH｜小个子显高 | active | ✅ | native_photo | TH | `PHOTO_TH_PETITE_STYLING_V1` |
| production | 图文｜TH｜四选一穿搭 | active | ✅ | native_photo | TH | `PHOTO_TH_PICK_YOUR_LOOK_V3` |
| production | 图文｜MX｜假发前后对比 | active | ✅ | native_photo | MX | `PHOTO_MX_BEFORE_AFTER_V1` |
| production | 图文｜MX｜四选一发型 | active | ✅ | native_photo | MX | `PHOTO_MX_PICK_YOUR_HAIR_V2` |
| production | 图文｜MX｜脸型匹配发型 | active | ✅ | native_photo | MX | `PHOTO_MX_FACE_SHAPE_MATCH_V1` |
| production | 图文｜MX｜场景发型 | active | ✅ | native_photo | MX | `PHOTO_MX_OCCASION_HAIR_V1` |
| trial | 图文｜TH｜冷热切换 | disabled | ⛔ | native_photo | TH | `PHOTO_TH_THERMAL_TRANSITION_V1` |
| trial | 图文｜VN｜围巾旅行 | disabled | ⛔ | native_photo | VN | `PHOTO_TRAVEL_OUTFIT_V3` |
| trial | 图文｜VN｜围巾搭配四选一 | disabled | ⛔ | native_photo | VN | `PHOTO_MATCHING_CHOICE_V3` |
| legacy | TH｜五套穿搭｜拆解首图 | active | ✅ | — | TH | `RECIPE_MULTI_LOOK_V1` |
| legacy | TH｜小个子显高｜轻文字 | active | ✅ | — | TH | `RECIPE_PAIN_POINT_SOLUTION_V1` |
| legacy | TH｜咖啡约会｜轻文字 | active | ✅ | — | TH | `RECIPE_SCENE_SOLUTION_V1` |
| legacy | TH｜一衣多穿｜轻文字 | active | ✅ | — | TH | `RECIPE_VISUAL_TRANSFORM_V1` |
| legacy | TH｜三配方套装｜轻文字 | active | ✅ | — | TH | `RECIPE_PAIN_POINT_SOLUTION_V1`、`RECIPE_SCENE_SOLUTION_V1`、`RECIPE_VISUAL_TRANSFORM_V1` |
| legacy | TH｜自动匹配穿搭组合｜轻文字 | active | ✅ | — | TH | `RECIPE_PAIN_POINT_SOLUTION_V1`、`RECIPE_SCENE_SOLUTION_V1`、`RECIPE_VISUAL_TRANSFORM_V1` |
| legacy | TH｜穿搭拆解首图｜均衡变体 | active | ✅ | — | TH | `RECIPE_OUTFIT_BREAKDOWN_V1` |
| legacy | TH｜随机养号组合｜轻文字 | active | ✅ | — | — | `（候选项：TH｜五套穿搭｜拆解首图）` |

## 2. 预设 → 配方 → 流程 → 主题 → 语言 → 排版 → 输出

### 图文｜TH｜旅行穿搭

分组 `production`（运营日常入口）；状态 `active`；可用：是；类目 `womenswear`；路由策略 `native_photo_style_plan_v1`

- **任务 1**：账号 `OPV_TH_TEST_001`；市场/语言 `TH`/`th-TH`；钩子 `destination_temperature`
  - 配方：`PHOTO_TH_TRAVEL_OUTFIT_V2`（native_photo，5 页，锚点槽位 2）
  - 规划流程：`travel_two_step`
  - 主题范围：`AUTUMN_OUTFIT`、`COOL_WEATHER_TRAVEL`、`DAILY_COMMUTE`、`CAFE_DATE`
  - 主题缺省：必填
  - 文案来源：v1 内联（随「图文主题」携带 th-TH 文案）
  - 排版：版式 `FULL_BLEED`、`CHOICE_DETAIL`；角色 `travel_cover`、`look_a`、`look_b`、`look_c`、`look_d_with_cta`
  - 输出形式：`native_photo` 5 页；发布店铺（TH）=`THFZ01`

### 图文｜TH｜小个子显高

分组 `production`（运营日常入口）；状态 `active`；可用：是；类目 `womenswear`；路由策略 `native_photo_v1`

- **任务 1**：账号 `OPV_TH_TEST_001`；市场/语言 `TH`/`th-TH`；钩子 `petite_question`
  - 配方：`PHOTO_TH_PETITE_STYLING_V1`（native_photo，5 页，锚点槽位 3）
  - 规划流程：`—`
  - 主题范围：—
  - 主题缺省：必填
  - 文案来源：v1 内联（随「图文主题」携带 th-TH 文案）
  - 排版：版式 `SPLIT_TWO`、`DETAIL`；角色 `hook_comparison`、`baseline`、`improved`、`key_detail`、`cta`
  - 输出形式：`native_photo` 5 页；发布店铺（TH）=`THFZ01`

### 图文｜TH｜四选一穿搭

分组 `production`（运营日常入口）；状态 `active`；可用：是；类目 `womenswear`；路由策略 `native_photo_product_supply_v1`

- **任务 1**：账号 `OPV_TH_TEST_001`；市场/语言 `TH`/`th-TH`；钩子 `pick_one_of_four`
  - 配方：`PHOTO_TH_PICK_YOUR_LOOK_V3`（native_photo，5 页，锚点槽位 2）
  - 规划流程：`reference_contract_v1`
  - 主题范围：`AUTUMN_OUTFIT`、`COOL_WEATHER_TRAVEL`、`DAILY_COMMUTE`、`CAFE_DATE`
  - 主题缺省：必填
  - 文案来源：v1 内联（随「图文主题」携带 th-TH 文案）
  - 排版：版式 `GRID_FOUR`、`CHOICE_DETAIL`；角色 `choice_grid`、`look_a`、`look_b`、`look_c`、`look_d_with_cta`
  - 输出形式：`native_photo` 5 页；发布店铺（TH）=`THFZ01`

### 图文｜MX｜假发前后对比

分组 `production`（运营日常入口）；状态 `active`；可用：是；类目 `wig`；路由策略 `native_photo_v1`

- **任务 1**：账号 `OPV_MX_PHOTO_001`；市场/语言 `MX`/`es-MX`；钩子 `antes_despues`
  - 配方：`PHOTO_MX_BEFORE_AFTER_V1`（native_photo，5 页，锚点槽位 3）
  - 规划流程：`—`
  - 主题范围：—
  - 主题缺省：必填
  - 文案来源：v1 内联（随「图文主题」携带 th-TH 文案）
  - 排版：版式 `SPLIT_TWO`、`DETAIL`；角色 `before_after_hook`、`before`、`after`、`hair_detail`、`cta`
  - 输出形式：`native_photo` 5 页；发布店铺（MX）=`MXJF01`

### 图文｜MX｜四选一发型

分组 `production`（运营日常入口）；状态 `active`；可用：是；类目 `wig`；路由策略 `native_photo_v1`

- **任务 1**：账号 `OPV_MX_PHOTO_001`；市场/语言 `MX`/`es-MX`；钩子 `cual_elegirias`
  - 配方：`PHOTO_MX_PICK_YOUR_HAIR_V2`（native_photo，4 页，锚点槽位 1）
  - 规划流程：`—`
  - 主题范围：—
  - 主题缺省：必填
  - 文案来源：v1 内联（随「图文主题」携带 th-TH 文案）
  - 排版：版式 `HAIR_OPTION`、`HAIR_OPTION_CTA`；角色 `hair_a`、`hair_b`、`hair_c`、`hair_d`
  - 输出形式：`native_photo` 4 页；发布店铺（MX）=`MXJF01`

### 图文｜MX｜脸型匹配发型

分组 `production`（运营日常入口）；状态 `active`；可用：是；类目 `wig`；路由策略 `native_photo_v1`

- **任务 1**：账号 `OPV_MX_PHOTO_001`；市场/语言 `MX`/`es-MX`；钩子 `face_shape_match`
  - 配方：`PHOTO_MX_FACE_SHAPE_MATCH_V1`（native_photo，5 页，锚点槽位 2）
  - 规划流程：`—`
  - 主题范围：—
  - 主题缺省：必填
  - 文案来源：v1 内联（随「图文主题」携带 th-TH 文案）
  - 排版：版式 `GRID_FOUR`、`CHOICE_DETAIL`；角色 `face_shape_hook`、`round_face`、`oval_face`、`square_face`、`long_face_with_cta`
  - 输出形式：`native_photo` 5 页；发布店铺（MX）=`MXJF01`

### 图文｜MX｜场景发型

分组 `production`（运营日常入口）；状态 `active`；可用：是；类目 `wig`；路由策略 `native_photo_v1`

- **任务 1**：账号 `OPV_MX_PHOTO_001`；市场/语言 `MX`/`es-MX`；钩子 `hair_for_each_plan`
  - 配方：`PHOTO_MX_OCCASION_HAIR_V1`（native_photo，5 页，锚点槽位 2）
  - 规划流程：`—`
  - 主题范围：—
  - 主题缺省：必填
  - 文案来源：v1 内联（随「图文主题」携带 th-TH 文案）
  - 排版：版式 `GRID_FOUR`、`CHOICE_DETAIL`；角色 `occasion_hook`、`date_hair`、`party_hair`、`work_hair`、`weekend_hair_with_cta`
  - 输出形式：`native_photo` 5 页；发布店铺（MX）=`MXJF01`

### 图文｜TH｜冷热切换

分组 `trial`（已配置待验收）；状态 `disabled`；可用：否；类目 `womenswear`；路由策略 `native_photo_v1`

- **任务 1**：账号 `OPV_TH_TEST_001`；市场/语言 `TH`/`th-TH`；钩子 `thermal_contrast`
  - 配方：`PHOTO_TH_THERMAL_TRANSITION_V1`（native_photo，5 页，锚点槽位 1）
  - 规划流程：`thermal_transition_two_step`
  - 主题范围：`THERMAL_TRANSITION`
  - 主题缺省：必填
  - 文案来源：v1 内联（随「图文主题」携带 th-TH 文案）
  - 排版：版式 `TRIPTYCH`、`FULL_BLEED`；角色 `hook`、`state_base`、`state_mid`、`state_outer`、`cta`
  - 输出形式：`native_photo` 5 页；发布店铺（TH）=`THFZ01`

### 图文｜VN｜围巾旅行

分组 `trial`（已配置待验收）；状态 `disabled`；可用：否；类目 `scarf`；路由策略 `native_photo_style_plan_v1`

- **任务 1**：账号 `OPV_VN_TEST_001`；市场/语言 `VN`/`vi-VN`；钩子 `destination_temperature`
  - 配方：`PHOTO_TRAVEL_OUTFIT_V3`（native_photo，5 页，锚点槽位 2）
  - 规划流程：`travel_two_step`
  - 主题范围：`AUTUMN_OUTFIT`、`COOL_WEATHER_TRAVEL`、`DAILY_COMMUTE`、`CAFE_DATE`
  - 主题缺省：必填
  - 文案来源：语言包 `th-TH` → `TH_TRAVEL_OUTFIT_V3`；`vi-VN` → `VN_TRAVEL_OUTFIT_V1`
  - 排版：版式 `FULL_BLEED`、`CHOICE_DETAIL`；角色 `travel_cover`、`look_a`、`look_b`、`look_c`、`look_d_with_cta`
  - 输出形式：`native_photo` 5 页；发布店铺（VN）**未配置路由**

### 图文｜VN｜围巾搭配四选一

分组 `trial`（已配置待验收）；状态 `disabled`；可用：否；类目 `scarf`；路由策略 `native_photo_product_supply_v1`

- **任务 1**：账号 `OPV_VN_TEST_001`；市场/语言 `VN`/`vi-VN`；钩子 `pick_one_of_four`
  - 配方：`PHOTO_MATCHING_CHOICE_V3`（native_photo，5 页，锚点槽位 2）
  - 规划流程：`reference_contract_v1`
  - 主题范围：`AUTUMN_OUTFIT`、`COOL_WEATHER_TRAVEL`、`DAILY_COMMUTE`、`CAFE_DATE`
  - 主题缺省：可空（文案已由语言包承担）
  - 文案来源：语言包 `vi-VN` → `VN_SCARF_MATCHING_V1`
  - 排版：版式 `GRID_FOUR`、`CHOICE_DETAIL`；角色 `choice_grid`、`look_a`、`look_b`、`look_c`、`look_d_with_cta`
  - 输出形式：`native_photo` 5 页；发布店铺（VN）**未配置路由**

### TH｜五套穿搭｜拆解首图

分组 `legacy`（保留的历史入口）；状态 `active`；可用：是；类目 `—`；路由策略 `—`

- **任务 1**：账号 `OPV_TH_TEST_001`；市场/语言 `TH`/`th-TH`；钩子 `multi_look`
  - 配方：`RECIPE_MULTI_LOOK_V1`（历史类型，5 页，锚点槽位 1）
  - 预设自带主题：`THEME_TH_OUTFIT_BREAKDOWN_V1`
  - 规划流程：`—`
  - 主题范围：—
  - 主题缺省：必填
  - 文案来源：v1 内联（随「图文主题」携带 th-TH 文案）
  - 排版：版式 —；角色 —
  - 输出形式：`video` 5 页；发布店铺（TH）=`THFZ01`

### TH｜小个子显高｜轻文字

分组 `legacy`（保留的历史入口）；状态 `active`；可用：是；类目 `—`；路由策略 `—`

- **任务 1**：账号 `OPV_TH_TEST_001`；市场/语言 `TH`/`th-TH`；钩子 `pain_point`
  - 配方：`RECIPE_PAIN_POINT_SOLUTION_V1`（历史类型，5 页，锚点槽位 1）
  - 预设自带主题：`THEME_TH_PETITE_PROPORTION_V1`
  - 规划流程：`—`
  - 主题范围：—
  - 主题缺省：必填
  - 文案来源：v1 内联（随「图文主题」携带 th-TH 文案）
  - 排版：版式 —；角色 —
  - 输出形式：`video` 5 页；发布店铺（TH）=`THFZ01`

### TH｜咖啡约会｜轻文字

分组 `legacy`（保留的历史入口）；状态 `active`；可用：是；类目 `—`；路由策略 `—`

- **任务 1**：账号 `OPV_TH_TEST_001`；市场/语言 `TH`/`th-TH`；钩子 `scene_problem`
  - 配方：`RECIPE_SCENE_SOLUTION_V1`（历史类型，5 页，锚点槽位 1）
  - 预设自带主题：`THEME_TH_CAFE_DATE_V1`
  - 规划流程：`—`
  - 主题范围：—
  - 主题缺省：必填
  - 文案来源：v1 内联（随「图文主题」携带 th-TH 文案）
  - 排版：版式 —；角色 —
  - 输出形式：`video` 5 页；发布店铺（TH）=`THFZ01`

### TH｜一衣多穿｜轻文字

分组 `legacy`（保留的历史入口）；状态 `active`；可用：是；类目 `—`；路由策略 `—`

- **任务 1**：账号 `OPV_TH_TEST_001`；市场/语言 `TH`/`th-TH`；钩子 `before_after`
  - 配方：`RECIPE_VISUAL_TRANSFORM_V1`（历史类型，5 页，锚点槽位 5）
  - 预设自带主题：`THEME_TH_ONE_PIECE_MULTIWAY_V1`
  - 规划流程：`—`
  - 主题范围：—
  - 主题缺省：必填
  - 文案来源：v1 内联（随「图文主题」携带 th-TH 文案）
  - 排版：版式 —；角色 —
  - 输出形式：`video` 5 页；发布店铺（TH）=`THFZ01`

### TH｜三配方套装｜轻文字

分组 `legacy`（保留的历史入口）；状态 `active`；可用：是；类目 `—`；路由策略 `—`

- **任务 3**：账号 `OPV_TH_TEST_001`；市场/语言 `TH`/`th-TH`；钩子 `before_after`
  - 配方：`RECIPE_VISUAL_TRANSFORM_V1`（历史类型，5 页，锚点槽位 5）
  - 预设自带主题：`THEME_TH_ONE_PIECE_MULTIWAY_V1`
  - 规划流程：`—`
  - 主题范围：—
  - 主题缺省：必填
  - 文案来源：v1 内联（随「图文主题」携带 th-TH 文案）
  - 排版：版式 —；角色 —
  - 输出形式：`video` 5 页；发布店铺（TH）=`THFZ01`

### TH｜自动匹配穿搭组合｜轻文字

分组 `legacy`（保留的历史入口）；状态 `active`；可用：是；类目 `—`；路由策略 `—`

- **任务 3**：账号 `OPV_TH_TEST_001`；市场/语言 `TH`/`th-TH`；钩子 `before_after`
  - 配方：`RECIPE_VISUAL_TRANSFORM_V1`（历史类型，5 页，锚点槽位 5）
  - 预设自带主题：`THEME_TH_ONE_PIECE_MULTIWAY_V1`
  - 规划流程：`—`
  - 主题范围：—
  - 主题缺省：必填
  - 文案来源：v1 内联（随「图文主题」携带 th-TH 文案）
  - 排版：版式 —；角色 —
  - 输出形式：`video` 5 页；发布店铺（TH）=`THFZ01`

### TH｜穿搭拆解首图｜均衡变体

分组 `legacy`（保留的历史入口）；状态 `active`；可用：是；类目 `—`；路由策略 `—`

- **任务 1**：账号 `OPV_TH_TEST_001`；市场/语言 `TH`/`th-TH`；钩子 `—`
  - 配方：`RECIPE_OUTFIT_BREAKDOWN_V1`（历史类型，5 页，锚点槽位 2）
  - 预设自带主题：`THEME_TH_OUTFIT_BREAKDOWN_V1`
  - 规划流程：`—`
  - 主题范围：—
  - 主题缺省：必填
  - 文案来源：v1 内联（随「图文主题」携带 th-TH 文案）
  - 排版：版式 —；角色 —
  - 输出形式：`video` 5 页；发布店铺（TH）=`THFZ01`

### TH｜随机养号组合｜轻文字

分组 `legacy`（保留的历史入口）；状态 `active`；可用：是；类目 `—`；路由策略 `multi_look_6s_v2`

按 `selection: deterministic_one` 从候选项里选一条：`TH｜五套穿搭｜拆解首图`。

