# TH 日常冷热切换线（PHOTO_TH_THERMAL_TRANSITION_V1）本地操作说明

- 文档日期：2026-09-12
- 目标包：`packages/organic_photo_video`
- 生产入口：飞书预设「图文｜TH｜冷热切换」——**disabled**
- 外部写入：**无**（不写飞书、不写 RDS、不发布）

---

## 1. 业务定位

同一天从**室外高温**进入 **BTS/商场**再进入**办公室强空调**，一套衣服怎么切换。

| 角色 | 热环境 | 穿搭 | 可见层数 |
|---|---|---|---|
| `base` | 室外炎热 | 透气基础层 | 1 |
| `mid` | BTS/商场偏凉 | 增加可穿脱薄衬衫 | 2 |
| `outer` | 办公室空调强 | 增加轻量有型外搭 | 3 |

与已退役的温度分层线（`PHOTO_TH_TEMPERATURE_DRESSING_V2`，2026-09-13 删除）的关系：

- 继承同一套**结构机制**（代码层仍在）：三态有序层栈、严格包含、基础层/下装/鞋冻结、同人同机位、
  `LAYER_PROGRESSION` 参考用途、`COMPLETE_LOOK` 三张素材、注册表路由。
- **不继承语义**：没有 `t15/t10/t5/t0` 跨档单调性，改为 `thermal_context`
  （`outdoor_hot → transit_cool → office_cold`）顺序校验。
- 旅行·温度穿搭 slot（`travel_theme_templates.json` 的 `TEMPERATURE`）保持不变，
  0–15°C 低温内容归旅行线，本线**不消费**旅行资产。

## 2. 首版开放范围（canary_only）

policy `TH_THERMAL_TRANSITION_V1.json` 设 `canary_only: true`，只实现：

```text
transition_key        = outdoor_bts_office
thermal_sensitivity   = normal
dress_code            = office
style_series          = minimal_city
temperature_label_mode= QUALITATIVE
reference_mode        = COMPLETE_LOOK（STYLE 未开放）
count                 = 1
```

其余组合一律**显式报错**，不做隐式降级：

```text
冷热切换 Phase 1 canary 尚未开放 transition_key=...
冷热切换 Phase 1 canary 仅开放 室外热→BTS→办公室空调／正常体感／办公室着装／minimal_city／QUALITATIVE；以下变量尚未开放：...
```

## 3. 合同与失败码

`thermal_transition_contract`（recipe 内，schema `opv-photo-thermal-transition-contract-v1`）：

- `presentation_order = base → mid → outer`
- `generation_order  = outer → mid → base`（先冻结最全态，再逐层减）
- `progression_relation = STRICT_STACK_INCLUSION`，`same_base_bottom_shoes = true`
- `allowed_item_types` / `forbidden_item_types`（禁用羽绒、厚呢大衣、雪地靴、保暖内衣、短裤、凉鞋）

| 失败码 | 含义 |
|---|---|
| `UNKNOWN_THERMAL_CONTEXT` | 出现未声明的体感枚举 |
| `CONTEXT_ORDER_MISMATCH` | 体感顺序与 `states` 不一致 |
| `TEMPERATURE_VALUE_UNSOURCED` | 文案出现无来源具体温度数字 |
| `BASE_OUTFIT_DRIFT` | 基础层/下装/鞋跨状态漂移 |
| `TRANSITION_NOT_VISIBLE` | 过渡未由新增可穿上身层体现 |
| `ITEM_SLOT_DUPLICATED` | 同一槽位出现多个单品（冻结基础层会变得歧义） |
| `QA_SCHEMA_INCOMPLETE` | 观察 JSON 缺字段/类型错误（含冷热切换专有字段） |

结构类失败码（`LAYER_COUNT_MISMATCH` / `LAYER_ORDER_MISMATCH` /
`LAYER_SOURCE_IDENTITY_MISMATCH` / `LAYER_SOURCE_CAMERA_MISMATCH` 等）
由共享的 `photo_layering_qa.normalize_layering_qa` 产出，冷热切换线**继承**而非重写。

## 4. 页面结构（五页）

| 页 | `source_roles` | 版式 | 泰语场景标签 |
|---|---|---|---|
| 1 | base, mid, outer | `triptych_3` | ข้างนอกร้อน / รถไฟฟ้า/ห้างเย็น / ออฟฟิศแอร์แรง |
| 2 | base | `single` | — |
| 3 | mid | `single` | — |
| 4 | outer | `single` | — |
| 5 | base, mid, outer | `triptych_3` | 同封面 |

`triptych_3`：三栏纵向等宽、固定角色顺序、中心裁切（不拉伸）、
每栏底部安全条显示场景标签；标签超宽直接报错，不溢出到相邻栏或人脸区。

## 5. 本地 canary

```bash
cd packages/organic_photo_video
/usr/bin/python3 scripts/run_thermal_transition_canary.py \
  --base  <室外热的透气基础层实拍> \
  --mid   <增加薄层后的实拍> \
  --outer <增加办公室外搭后的实拍> \
  --output-dir tmp/thermal_transition_canary/run_YYYYMMDD \
  --observation-json <离线观察 JSON>
```

- 不带 `--observation-json` 时会走配置好的视觉模型路由。
- 不带 `--apply` 语义：本脚本**从不**写飞书、RDS 或发布。
- 输出：`content_plan.json`、`thermal_transition_qa.json`、
  `thermal_transition_qa.md`、`page_*.jpg`、`canary_result.json`。
- 三张源图必须内容不同（按 sha256 去重），且**不得**与温度分层线已入库素材同源。

`release_ready` 只有在「QA 通过 **且** 文案为 `NATIVE_APPROVED`」时才为 `true`；
当前泰语文案为 `DRAFT`，因此必然为 `false`。

### 关键：canary 素材要求

- 必须是**新拍**的 base/mid/outer 三态：室外热 → 交通工具 → 办公室。
- 温度分层线已有的薄针织 / 开衫 / 风衣低温素材**不能**当作本线业务正例；
  如需占位，只能作为**结构回归样本**，不得当作已验收内容。
- 人物图**不得**在未获明确授权的情况下发送到外部视觉模型。

## 6. Phase 1 通过条件（需人工完成）

1. 三栏封面 3 秒内能读懂「室外热 → 交通工具 → 办公室冷」；
2. 分层 QA 全通过（`failed_roles` 为空）；
3. 场景顺序正确、文字不遮脸、不裁切、泰文不溢出底板；
4. CTA 具备「怕热还是怕空调」的互动感；
5. 泰语母语审校通过（改为 `NATIVE_APPROVED`，并带审校人/时间/匹配 SHA256）；
6. 仍不写飞书、不写 RDS、不发布。

以上全部满足后，再把飞书预设「图文｜TH｜冷热切换」从 `disabled` 改为启用。

## 7. 旧温度分层 V2 的处置（已执行）

`scripts/deploy_th_photo_planner.py`（**dry-run，只读**）实测：

```text
PHOTO_TH_PICK_YOUR_LOOK_V3        current_version = 9   （已上线，对照：RDS 可达）
PHOTO_TH_TRAVEL_OUTFIT_V2         current_version = 6   （已上线）
PHOTO_TH_TEMPERATURE_DRESSING_V2  current_version = null（RDS 未部署）
PHOTO_TH_THERMAL_TRANSITION_V1    current_version = null（新增，未部署）
```

`PHOTO_TH_TEMPERATURE_DRESSING_V2` **未部署到 RDS**，命中《TH 冷热穿搭双线改造执行方案》§7
的「未部署」分支，因此于 **2026-09-13 执行清理**（删除旧配置、配方总数保持不变）：

已删除（备份在 `/tmp/opv_retire_temperature_layering_v2_20260913/deleted/`）：

```text
config/recipes/PHOTO_TH_TEMPERATURE_DRESSING_V2.json
config/photo_planning_policies/TH_TEMPERATURE_DRESSING_V1.json
config/copy_packs/TH_TEMPERATURE_DRESSING_V2.tsv
config/layouts/PHOTO_TEMPERATURE_LAYERING_V2.json
tests/test_photo_temperature_flow.py
scripts/run_temperature_layering_canary.py
scripts/review_temperature_copy_pack.py
docs/TH_TEMPERATURE_DRESSING_V2_PHASE1A.md
```

已修改：

```text
services/photo_content_planner.py      RECIPE_POLICY_FILES 去掉旧映射
scripts/deploy_th_photo_planner.py     DEPLOYABLE_RECIPES 去掉旧配方
config/feishu_production_presets.json  删除引用旧配方的「图文｜TH｜温度穿搭」预设
tests/...                              计数与引用改指向本线
```

结果与守恒：

```text
content_recipes  19 → 18   （= 旧温度 V2 加入前的配方总数，符合「保持不变」）
board_layouts    12 → 11
native_photo recipes / presets  14 → 13 / 9 → 8
recipe_count / profile_count（preflight）  14 → 13 / 30 → 29
```

保留未动（属**通用基础设施**，本线仍在使用）：

```text
services/photo_layering_contract.py / photo_layering_flow.py
services/photo_layering_qa.py / photo_layering_report.py
services/photo_copy_review.py
```

`RDS 未删除`（本来就没有部署过），旧 `PHOTO_TH_TEMPERATURE_DRESSING_V1`（deprecated 模板）
按方案 §7 未被要求处理，原样保留。

> 遗留待决：`photo_theme.THEME_OPTIONS` 中的「温度穿搭」（→ `TEMPERATURE_DRESSING`）
> 现已无配方消费。保留是因为删除它需要同步飞书线上表选项（本次禁止外部写入）；
> 是否随 B 线一并收敛，待后续确认。

## 8. 回归

```bash
cd packages/organic_photo_video
/usr/bin/python3 -m unittest discover -s tests -p 'test_*.py'
```

本次结果：**907 tests / OK**（A 线落地后为 915；§7 清理退役线时移除了它自带的
8 个用例 `tests/test_photo_temperature_flow.py`，其余全部保留）。

注意：本机托管 Python 3.13 缺 `pymysql`/`PIL`，必须用 `/usr/bin/python3`；
本项目未安装 `pytest`，不要把 `pytest` 缺失误判成测试失败。

新增测试：

- `tests/test_photo_thermal_transition_flow.py`
- `tests/test_photo_thermal_transition_semantics.py`
- `tests/test_photo_thermal_triptych_layout.py`
- `tests/test_thermal_transition_routing.py`
