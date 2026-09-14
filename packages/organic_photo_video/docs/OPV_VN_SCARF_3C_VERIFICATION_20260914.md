# VN 围巾两条线 3C 真实验证（2026-09-14）

对应《图文模块：少量断点修复、入口规整与围巾生产接入》§3C。本轮授权范围：
**两条线各跑 1 篇**，真实生图、真建批次、真回写飞书；商品资料本轮不接入。

## 一、结论速览

| 线 | 预设 | 飞书行 | 结果 | 证据 |
| --- | --- | --- | --- | --- |
| 搭配四选一（非旅行流程） | 图文｜VN｜围巾搭配四选一 | `recvvcgz2Kk41n` | **PASS** | 进度=已完成、出 5 张 1080×1920 成片、批次 `waiting`→`photo_packaging` |
| 围巾旅行（旅行流程） | 图文｜VN｜围巾旅行 | `recvvcgvz0LsIh` | **BLOCKED** | 卡在发布契约：VN 行拿到**泰语文案**（下详 §4 D3） |

两条线都**没有**"配置上线即批量放行"：六道生产门禁在收尾时已全部回到关闭态（§6）。

## 二、C 行验收（端到端 PASS）

- 飞书：`进度=已完成`、`审核=无需审核`、`素材状态=已匹配可用素材`、
  `备注=已完成 1/1 篇原生图文，共 5 张；技术检查已通过`、`预览/成片` 附件 **5** 个。
- RDS：批次 `opv_batch_0973fe080554abd86df3409f7376d204`（`media_kind=native_photo`）；
  内容任务 `opv_task_20260914_fe538d457cbe`（`source_record_id=recvvcgz2Kk41n:1:PHOTO_MATCHING_CHOICE_V3:1`，
  `task_status=photo_packaging`）——按既有口径这就是正常终态。
- 产物：`photo_packages/opv_task_20260914_fe538d457cbe/opv_rev_20260914_f73a96a7e0e1/final/01..05.jpg`
  全部 **1080×1920**；只有 **1** 个 revision 目录 ⇒ 没有重建批次。
- 素材：4 张 look 由 1route（`gpt-image-2.5-sunburst`）生成后作为 4 个镜头（`shots/*_reuse.png`）。
- 内容签名库存 165 → **167**（+2 = 两条线各 1 篇）。

## 三、A 行为什么没出片

A 行**已真实付费生成 4 张 look**（`style_reference_supply/recvvcgvz0LsIh_item_1/`，
`supply_manifest.json status=complete`）并已按正确的键登记成素材集，但**发布契约**拦下：

```
title / caption / hashtags[0..1] / slide_texts[0] / slide_texts[4]
  contains Thai characters for locale vi-VN
```

缓存计划 `content_plans/recvvcgvz0LsIh/plan.json` 里可直接看到泰语模板 + 越南语 token：

```
title  = "ไอเดียแต่งตัวเที่ยวThành phố se lạnh"
copy_source = "template_fill"
```

## 四、本轮查实的三处断点

### D1 —— `(asset_set_key, asset_set_version)` 唯一约束 + 静默改行 ⇒ 悬空 pin（**根因**）

`opv_asset_set` 上 `uq_opv_asset_set_version` 是 **UNIQUE(asset_set_key, asset_set_version)**，
而 `qualify()` 的版本号只统计**该类别+该市场下 enabled 的同键行**：一条**停用**的历史行
同样占着槽位却不在计数里。于是

1. `INSERT ... ON DUPLICATE KEY UPDATE` 改写了那条历史行、**保留它原来的 `asset_set_id`**；
2. 本次算出的 content-addressed id（如 `ASSET_VN_SCARF_UPLOAD_cf7e3d7614e0392b`）**从未落库**；
3. `AssetSetService.save()` 在回读为空时回落返回内存对象，调用方把它当 pin 记下；
4. 冻结阶段按 id 查不到 ⇒ 报 `NEEDS_ASSET`，**而图已经付过费**。

实证：A 行 20:30 那次失败后，`staging/recvvcgvz0LsIh_item_1/manifest.json` 记着
`asset_set_id=ASSET_VN_SCARF_UPLOAD_cf7e3d7614e0392b` 而库里按该 id 查为 `None`；
同一时刻 `ASSET_TH_WOMENSWEAR_CHOICE_V2`（创建于 09-05、本应属于泰国线）的
`manifest_json.assets[*].path` 被改写成了 A 行今天生成的 4 张 VN 图。

### D2 —— 自动风格参考供给按 `profiles[0]` 选档 ⇒ VN 素材集登记进泰国命名空间

`PHOTO_TRAVEL_OUTFIT_V3` 的两个档位**变量完全相同、只有 `asset_set_keys` 不同**
（`TH_WOMENSWEAR_CHOICE` / `VN_SCARF_CHOICE`）。STYLE 分支调 `qualify()` 时
`variation` 不含 `profile_binding`，旧实现回落到 `profiles[0]` = **泰国档**，
于是 VN 请求把素材集写进了泰国的键——正好触发 D1（泰国键 v1/v2 槽位被 09-05 的停用行占着）。

同时 `build_batch` 也按顺序取第一个有候选的档位，导致 VN 请求可能冻结到**泰国档**，
与 `tests/test_photo_v3_production_path.py::test_travel_line_resolves_the_vn_asset_set_with_no_override`
钉死的契约相矛盾。

### D3 —— 旅行流程的文案由主题模板拥有（泰语），Locale Pack 的越语文案被跳过（**未修，待决策**）

`services/photo_content_planner.py`：

```python
locale_pack=None if travel_flow else locale_pack   # 2026-09-14 的作用域决定
...
elif travel_flow:
    template_copy = _travel_template_copy(..., templates=copy_templates, locale_pack=locale_pack)
    item["copy_source"] = "template_fill"
```

`copy_templates` 来自主题（泰语），`locale_pack` 只用来替换 token（所以出现
"泰语句子 + 越南语地名"）。而 `config/locales/LOCALE_VI_VN_V1.json` **已经交付**
`family_copy`，其注释写明"前 8 个家族对应 TRAVEL_OUTFIT_V3 的旅行家族……
泰国语版本逐字保留在 LOCALE_TH_TH_V1.json 中"——即越语旅行文案**已就绪但没接线**。

因此：**VN 旅行线目前不可能通过发布契约**，与是否填旅行地点无关；这条必须修（见 §7）。

## 五、本轮改动

| 文件 | 改动 |
| --- | --- |
| `services/photo_request_factory.py` | `build_batch` 跳过 `markets` 不含本市场（且已声明 `markets`）的档位 |
| `services/photo_asset_supply.py` | ① `qualify` 未指名 profile 时**按市场选档**；② 落库后**回读自证**，(asset_set_key, version) 槽位被占则**响亮失败**（含"已生成素材可复用、不会重复付费"提示） |
| `domain/photo_contracts.py` | `validate_execution_profiles` 校验可选字段 `markets` 的形状 |
| `config/recipes/PHOTO_TRAVEL_OUTFIT_V3.json` | 两个档位分别声明 `markets: ["TH"]` / `["VN"]` |
| `tests/test_photo_profile_market_binding.py` | 新增 7 条：按市场选档、显式 binding 优先、未声明 markets 保持旧行为、槽位被占必须响亮失败、校验器拒绝畸形 `markets` |

**零影响保证**：未声明 `markets` 的档位对所有市场开放；`load_content_recipes()` 全量配方
在 `validate_execution_profiles` 下零错误；全量回归 **1290 OK**（基线 1283 + 新增 7）。

> 上述 1290 OK 是在**门禁关闭**（预设 `disabled`）状态下测的。武装期间
> `test_photo_vn_activation` / `test_photo_entry_catalog` / `test_photo_preflight_presets`
> 等 10 条会因"预设被临时改成 active"而失败——这是武装态的正常现象，与本轮改动无关。

## 六、门禁前后状态

| 门禁 | 武装期间 | 复位后（现状） |
| --- | --- | --- |
| 预设 `图文｜VN｜围巾旅行` / `…搭配四选一` | active | **disabled** |
| 配方 `PHOTO_TRAVEL_OUTFIT_V3` / `PHOTO_MATCHING_CHOICE_V3` | active | **draft** |
| Market Pack `MP_VN_DEFAULT_V1` | active | **draft** |
| 账号 `OPV_VN_TEST_001` | testing | **paused** |
| 素材集 `VN_SCARF_CHOICE` v1/v2 | enabled | enabled（原状） |

新增素材集（跑通期间注册、无 prior，保持现状）：
`…fc774d7b7d52a969`(v3，A 行首跑)、`…cf7e3d7614e0392b`(v4，修复后 A 行复用登记)、
`…92f6a7661c453059`(v5，C 行)。

## 七、数据侧处置

- **两条错登记行已停用（可逆）**：`ASSET_TH_WOMENSWEAR_CHOICE_V1` / `_V2` 的库里行是
  `market='VN'`、`category_key='scarf'`、manifest 指向 VN 围巾图，而同名配置文件
  （git 无改动）声明的是 `market='TH'`/`womenswear`/`disabled`。它们会让 VN 请求
  错误命中泰国档。已只改 `status -> disabled`（快照 `tmp/real_gen_e2e_3rows/neutralize_state_3c.json`），
  **不删行、不删文件**，TH 线路零影响（TH 请求本就过滤 `market='VN'`）。
- A 行保留：4 张已付费 look、批次 `opv_batch_2e8ac79d…`（waiting）、内容任务
  `opv_task_20260914_ff90abf557fa`（draft）。**未删除任何已生成图片/任务/批次**。

## 八、遗留与下一步（需拍板）

1. **D3 必须先修**，否则 VN 旅行线永远出不了片。建议：旅行流程的 `template_fill`
   分支优先取 `locale_pack.family_copy[family_id]`（越南语已交付），TH 侧要求
   `LOCALE_TH_TH_V1.family_copy` 与现主题模板填出的结果**逐字相同**并加断言锁死，
   再改 `locale_pack=None if travel_flow else locale_pack` 的作用域。
2. D1 的根因（版本号只统计 enabled 行）仍在：建议把版本计算改为"同键**所有**状态行的
   最大版本 + 1"，或给该唯一索引加 `status`——本轮只加了"响亮失败"护栏，没有动口径。
3. VN 旅行线的 `photo_locale.travel_copy_template.hashtags` 恒为空列表（既有缺口）。
4. 发布：本轮未点"确认发布"。C 行成片已在 `预览/成片`，具备走既有确认发布的全部条件。
