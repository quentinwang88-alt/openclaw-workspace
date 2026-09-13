# TH 旅行低温增强（PHOTO_TH_TRAVEL_OUTFIT_V2 + thermal_sensitivity）本地操作说明

- 文档日期：2026-09-13
- 目标包：`packages/organic_photo_video`
- 生产入口：飞书预设「图文｜TH｜旅行穿搭」——**未改动**
- 外部写入：**无**（不写飞书 schema、不写 RDS、不发布、不启用预设）

---

## 1. 业务定位

0–15°C 的低温内容**收回现役旅行线**，仍然保持「目的地 + 四套选择」的内容形态：

> 去某地玩，这个温度该穿哪套？A / B / C / D 你选哪套？

与已退役的日常温度分层线（`PHOTO_TH_TEMPERATURE_DRESSING_V2`，2026-09-13 删除）的关系：

- **不复用**其 `t15/t10/t5/t0` 单点低温语义，也**不消费**它的任何资产；
- 旅行线用自己已有的 `temperature_band`（`15_22c / 10_15c / 5_10c / 0_5c`）与目的地档；
- 体感只是**叠加在温度档之上的整篇调整**，不改变「四套选一」的选题结构。

---

## 2. 变更清单

| 文件 | 变更 |
|---|---|
| `config/recipes/PHOTO_TH_TRAVEL_OUTFIT_V2.json` | `recipe_version` 6 → **7**；`variables_schema` 增加可选变量 `thermal_sensitivity` |
| `config/photo_planning_policies/TH_TRAVEL_OUTFIT_V1.json` | `policy_version` 2 → **3** |
| `config/travel_theme_templates.json` | `TEMPERATURE` 主题 `version` 1 → **2**，新增 `thermal_sensitivity_planning`（体感调整指引 + 约束） |
| `services/photo_theme.py` | 旅行主题 profile 暴露 `thermal_sensitivity_planning`（仅 TEMPERATURE 有值） |
| `services/feishu_workflow.py` | 新增 `resolve_travel_thermal_sensitivity()` 与 `build_travel_topic()`（原内联主题简报抽取为可测函数） |
| `services/photo_reference_vision.py` | 旅行规划提示词 `v8` → **`v9`**；TEMPERATURE 主题渲染体感块 |

三个版本号**各自独立**，互不派生：配方版本在配方 JSON、策略版本在策略 JSON、提示词版本是代码常量
（有回归测试钉住这一点，避免再次出现 v8 / recipe 6 口径混用）。

---

## 3. 可选变量与消费边界

```json
"thermal_sensitivity": {
  "type": "enum",
  "values": ["feels_cold", "normal", "feels_warm"],
  "required": false,
  "default": "normal"
}
```

| 规则 | 落地方式 |
|---|---|
| 仅 `travel_theme_type=TEMPERATURE` 消费 | 只有该主题才把体感写进主题简报；其余五个主题的简报与提示词**逐字节不变** |
| 其余五个旅行主题保持原行为 | `build_travel_topic()` 在非 TEMPERATURE 时直接返回原简报（有测试对比同一主题填/不填体感的提示词完全相等） |
| 旧任务缺字段时默认 `normal` | 空列 → 取配方 `variables_schema` 声明的 `default`（单一事实源） |
| 四套 Look 使用相同目的地、温度档和体感 | 体感是**文章级**变量，提示词明确「体感只作用于整篇文章」 |
| A/B/C/D 不能分别代表怕冷/怕热 | 主题模板 `constraints` 明写并进入提示词 |
| 不复用日常冷热切换资产 | 旅行线仍只用自己的 `travel_outfit_choice` 资产标签 |

---

## 4. 体感指引的来源

体感调整方向不写在提示词代码里，而在 `config/travel_theme_templates.json` 的
`TEMPERATURE.thermal_sensitivity_planning`：

```json
{
  "default": "normal",
  "scope": "article",
  "modifiers": {
    "feels_cold": "同温度档下整体偏保暖：…",
    "normal": "按当前温度档常规搭配，不额外增加或减少保暖层级",
    "feels_warm": "同温度档下整体偏轻量：…"
  },
  "constraints": [
    "体感只作用于整篇文章，四套 Look 必须使用同一体感",
    "A/B/C/D 仍比较风格、轮廓与配色，不得分别代表怕冷或怕热",
    "不得出现具体温度数字，温度只以文章级温度档呈现"
  ]
}
```

提示词只在**模板确实提供了对应修正文案**时才渲染体感块，避免出现「声明了体感但没有指引」的静默状态。

---

## 5. 运营操作

- 运营在飞书任务表填 **「体感倾向」** 列（复用已有列，未新增列、未执行 schema 同步）：
  `怕冷` / `正常体感` / `怕热`，也可直接填 `feels_cold` / `normal` / `feels_warm`。
- **留空 = `normal`**，与历史任务行为一致。
- 非法的非空值（如「很怕冷」）**明确报错**，不做隐式降级。
- 「体感倾向」只在图文主题选 **「旅行·温度穿搭」** 时被读取；选其余五个旅行主题时该列被忽略，
  不影响产出的提示词。

---

## 6. 需要注意的缓存影响

提示词版本号会进入旅行规划缓存的 `input_contract`（`travel_plan.json` 的 `input_sha256`），
因此 **v9 生效后，在途任务若已有旧缓存会提示「参考分析或旅行变量已变化；请新建任务」**。
这是版本升级的既有设计（v8 之前同理），处置方式就是**新建任务**，不要手改缓存文件。

---

## 7. 回归

新增 `tests/test_travel_thermal_sensitivity.py`（12 条），含方案点名的 5 条：

```text
test_non_temperature_travel_themes_ignore_thermal_sensitivity
test_legacy_travel_task_defaults_to_normal_sensitivity
test_temperature_travel_applies_sensitivity_modifier
test_all_six_travel_themes_remain_six
test_travel_recipe_version_and_prompt_version_are_independent
```

其余 7 条补充：提示词 v8 行为保持、冻结 profile 未新增键、运营标签与非法值、体感为文章级而非逐套、
只有温度主题暴露体感指引、只有温度主题版本变动、配方枚举与解析器同源。

本次结果：**919 tests / OK**（基线 907，新增 12），`preflight_native_photo` 0 error / 0 external write。

注意：本机托管 Python 3.13 缺 `pymysql`/`PIL`，必须用 `/usr/bin/python3`；本项目未安装 `pytest`。

---

## 8. 未做与边界

- **未**执行飞书 schema 同步、**未**启用或修改任何飞书预设、**未**写 RDS、**未**发布。
- 「体感倾向」列仍然同时是旧温度分层线遗留使用的列（该线已退役，其解析函数仍在代码中但已不可达），
  后续若要收敛，需要连同线上表选项一起处理，属于外部写入，需单独授权。
- 旅行线**始终**从配方 execution profile 冻结变量（`temperature_band` 等），运营列不覆盖它；
  若要按行改温度档，是另一个独立需求。
