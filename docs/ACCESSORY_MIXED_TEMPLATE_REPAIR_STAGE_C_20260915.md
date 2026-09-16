# 配饰原创混合模板 · 阶段 C 交付说明

**日期**：2026-09-15
**范围**：任务书阶段 C —— Review #4（首帧读冻结合同）、Review #7（冻结光线配方传入视觉合同）、Review #8（中央口播继承逐镜承载与连续性分组）
**开发源**：`/Users/likeu3/.openclaw/workspace/skills/original-script-generator`
**探针**：`wp0/stage_c_probe.py`（78 项断言，0 失败）
**回归**：T08–T12 已落为用例；阶段 B+C 相关回归 357 项全通过

---

## 0. 一句话结论

阶段 C 三项修复全部完成并机器化验收通过。**共同主题是同一件事：一个任务一旦冻结，它的画面/光线/口播约束就必须来自冻结合同，而不是来自"当前进程的环境变量"。**

阶段 A 修的是"合同失败必须停"，阶段 B 修的是"合同内容本身自相矛盾"。阶段 C 修的是第三类漏洞：**合同是对的，但下游三个消费者各自绕过合同自己重新判断了一遍。**

| 消费者 | 修复前 | 修复后 |
|---|---|---|
| 首帧 | 读环境开关重新决定取景 | 只读 `mixed_template_contract` |
| 视觉合同（光线） | 用默认配方，忽略冻结合同 | 读冻结合同的 `environment_recipe_id` |
| 中央口播 | 整片一个承载 + `EVENT_1` | 逐镜承载 + 逐镜连续性分组 |

---

## 1. Review #4 —— 首帧只读冻结合同

### 1.1 修复前的真实故障

`_mixed_accessory_frame_line` 的签名是 `(*, canonical_type, opening_carrier)`，函数体第一件事是：

```
if not accessory_mixed_template_enabled():
    return ""          # 回落到旧的"半脸耳侧近景"
```

首帧是**异步渲染**的。于是有两条互不相同的错误路径：

1. **重试/异步 worker**：任务在计划阶段被冻结为"不露脸"，但首帧 worker 的环境里开关是关的（或关掉之后重试）→ 函数返回空 → 旧的"半脸耳侧近景"回来了。**一个已经冻结为不露脸的任务，被重新画上了脸。**
2. **反向**：开关打开时，**所有**旧配饰任务（不带任何混合合同）的首帧也开始走新分支。开关本意是"管新计划"，实际却改了历史任务的画面。

### 1.2 修复内容

`core/accessory_mixed_templates.py` 新增：

- `frozen_mixed_contract(container)` —— 从 script / brief / extension 任一层取出冻结的 `mixed_template_contract`；取不到就返回 `{}`。**存在该 dict（而非某个环境变量）才是"这是混合模式任务"的唯一判据。**
- `mixed_first_frame_facts(contract)` —— 冻结合同里**只与首帧有关**的切片。

`core/first_frame_contract.py`：

- `_mixed_accessory_frame_line` 签名从 `(*, canonical_type, opening_carrier)` 改为 `(contract)`。**产品类型不再参与判断** —— 取景权威是冻结合同，不是类型。
- `build_first_frame_contract` 新增 `mixed_template_contract` / `mixed_first_frame_facts` 两个字段，并把 `mixed_first_frame_facts` 并入指纹载荷。

### 1.3 为什么指纹只吃"首镜切片"

这是本阶段唯一一个需要解释的设计取舍。

`mixed_first_frame_facts` 返回：

```
{
  "template_id", "face_policy", "environment_recipe_id",
  "opening_unit": { unit_id, module, view_scope, view_label,
                    carrier_mode, allowed_framing, forbidden_framing }
}
```

**只有第一镜进指纹，后三镜一律不进。**

理由：`asset_fingerprint` / `contract_id` 的用途是**缓存键**。首帧是静态图，只有第一镜能影响它。若把整份合同哈希进去，那么"第三镜的观察任务措辞改了一个词"就会让全人类缓存的、**实际画面完全没变**的首帧图全部失效并重付费生成。

这条取舍已被用例锁死（T10）：改最后一镜 → 指纹不变；改首镜 → 指纹必变。

### 1.4 验收

| 断言 | 结果 |
|---|---|
| 三个模板 × 开关两态：`asset_fingerprint` / `contract_id` / 渲染出的 prompt 全部逐字一致 | ✅ |
| 五个旧类型（earring / bracelet / ring / hair_clip / silk_scarf）无合同时，开关两态指纹与 prompt 逐字一致，且不泄漏任一混合首帧行 | ✅ |
| 无合同 → `_mixed_accessory_frame_line(None)` 与 `({})` 均返回 `""`（两个开关态下） | ✅ |
| 首帧事实集合恰为 4 个模板级字段 + `opening_unit` | ✅ |
| 改后段不动指纹；改首镜必动指纹 | ✅ |

---

## 2. Review #7 —— 冻结光线配方传入视觉合同

### 2.1 修复前的真实故障

蓝图（blueprint guidance）引用的是**冻结合同里的** `environment_recipe_id`；而 `build_accessory_mixed_visual_contract` 只接受一个 `environment_recipe_id` 参数，调用方（`build_simplified_creative_seed`）**什么都没传**，于是函数回落到 `definition.default_environment_recipe`。

后果：**同一个模型输入里，两个表面说着两种光线。**

- 蓝图说：`MATTE_GREY_DETAIL`（哑光灰detail）
- 视觉合同说：`WINDOW_LIGHT_WOOD`（窗光木质，默认值）

首帧和生产提示词都继承"谁赢谁说了算"，因此这不是"两处不一致"这么轻 —— 是**画面到底什么样取决于哪一段文本后写入**。

### 2.2 修复内容

`core/visual_execution_contract.py`：

- `build_accessory_mixed_visual_contract` 新增 `frozen_contract` 参数。存在冻结合同时，**配方优先级为**：显式传入 > 冻结合同 > 类目默认。
- `frozen_contract` 同时承担第二职责：**让任务保持冻结**。原来 `if not accessory_mixed_template_enabled(): return {}` 意味着"开关一关，重跑就回落旧合同"。现在只要冻结合同存在，开关就无权把任务打回旧路径。
- `build_visual_execution_contract` 新增 `accessory_frozen_contract` 参数并透传。

`core/simplified_complete_script.py`：

```
seed_mixed_contract = frozen_mixed_contract(category_execution_extension)
...
accessory_environment_recipe_id=_text(seed_mixed_contract.get("environment_recipe_id")),
accessory_frozen_contract=seed_mixed_contract,
```

### 2.3 验收（**必须走真实 seed 入口**）

任务书明确要求："每一种配方都通过真实 seed 入口验证各阶段一致，不能只直接测试视觉合同构建函数。"

因此 T11 不调用 `build_accessory_mixed_visual_contract`，而是走 `build_simplified_creative_seed` —— 生成器真正消费的那个入口。

| 断言（× 3 配方） | 结果 |
|---|---|
| 冻结合同逐字进入 `seed["category_execution_extension"]` | ✅ |
| `seed["visual_execution_contract"]["feature_scope"] == "ACCESSORY_MIXED_TEMPLATE"` | ✅ |
| `visual["lighting_recipe"]["recipe_id"] == frozen["environment_recipe_id"]` | ✅ |
| `visual["lighting_recipe"]["recipe_id"] == 指定配方`（逐个精确匹配） | ✅ |
| 三配方取值互不相同（未集体塌回默认） | ✅ |
| 无冻结合同 + 开关关闭 → 不产出饰品混合视觉合同 | ✅ |

### 2.4 变异检验（证明用例真有检测力）

把 `accessory_environment_recipe_id` / `accessory_frozen_contract` 临时还原为原来的"什么都不传"，T11 的**三个配方全部失败**（`KeyError: 'visual_execution_contract'` —— 冻结合同丢失 + 开关默认关闭 → 视觉合同退化为 `{}`）。

已还原，`core/simplified_complete_script.py` 现为正确版本。

---

## 3. Review #8 —— 中央口播继承逐镜承载与连续性分组

### 3.1 修复前的真实故障

`build_simplified_voiceover_inputs` 里只有一个**整片级**承载：

```
carrier = {"PERSON_ON_CAMERA": "WEARER_ACTIVE",
           "STATIC_PRODUCT": "STATIC_PRODUCT",
           "HANDS_ONLY": "HAND_ONLY"}.get(presentation, presentation)
```

而模板 C 的四镜承载是 `STATIC_PRODUCT → HAND_ONLY → WEARER_ACTIVE → WEARER_ACTIVE`。

`MIXED` 这个承载**故意没有**映射条目 —— 因为一个混合片断本来就不存在"整片承载"这个概念。于是口播收到的信息被压平成"四镜完全相同"，连续性分组也一律是默认 `EVENT_1`。

后果：口播在为**四个一模一样的佩戴镜**写词，而画面上第一镜根本没有人物、第二镜只有手。**口播与画面的绑定被削弱，而这正是混合模板存在的理由。**

### 3.2 修复内容

`core/simplified_complete_script.py`：

```
shot_carrier = _text(item.get("carrier_mode")).upper() or carrier
continuity   = _text(item.get("continuity_group")) or "EVENT_1"
```

**优先继承冻结的逐镜值，只有旧脚本缺字段时才走整片兜底。** 兜底分支被有意保留，并且整片映射表里**故意不解析 `MIXED`** —— 让一个 MIXED 片断误用整片兜底，比让它显式报错更危险。

`core/accessory_mixed_templates.py`：`project_mixed_template_onto_units` 的 storyboard 镜像新增 `continuity_group`。

### 3.3 一个容易踩错的结构

`shot_plan` 的位置是：

```
direction["structure_execution_plan"]["shot_plan"]
```

**不是** `direction["shot_plan"]`。探针最初按后者读取，得到 `KeyError: 'shot_plan'`。

### 3.4 验收

| 断言 | 结果 |
|---|---|
| 模板 C 冻结承载 = `['STATIC_PRODUCT','HAND_ONLY','WEARER_ACTIVE','WEARER_ACTIVE']` | ✅ |
| 归一化脚本通过 `validate_simplified_visual_script` | ✅ |
| 逐镜合同投影状态 = `APPLIED` | ✅ |
| storyboard 逐镜承载 / 连续性分组 == 冻结值 | ✅ |
| 口播 `shot_plan` 逐镜承载 / 连续性分组 == 冻结值 | ✅ |
| `visual["shots"]` 逐镜承载一致 | ✅ |
| **反向对照**：删掉这两个字段后 → `['WEARER_ACTIVE']×4` + `['EVENT_1']×4` | ✅ |
| 三个模板的逐镜承载与各自冻结合同逐字一致 | ✅ |

反向对照是这条用例的关键：它同时证明了"继承生效"和"兜底仍在"，单看前者无法区分"继承"与"兜底恰好碰对"。

### 3.5 变异检验

把 `build_simplified_voiceover_inputs` 里的继承临时改回整片兜底，T12 失败：

```
- ['WEARER_ACTIVE', 'WEARER_ACTIVE', 'WEARER_ACTIVE', 'WEARER_ACTIVE']
+ ['STATIC_PRODUCT', 'HAND_ONLY', 'WEARER_ACTIVE', 'WEARER_ACTIVE']
```

已还原。

---

## 4. 回归用例落点（T08–T12）

| 用例 | 文件 | 锁定的行为 |
|---|---|---|
| T08 | `tests/test_accessory_mixed_integration.py` | 开关不改变冻结任务的指纹与提示词 |
| T09 | `tests/test_accessory_mixed_integration.py` | 无合同旧任务不受开关影响，旧取景逐字保留 |
| T10 | `tests/test_accessory_mixed_integration.py` | 指纹只含首镜切片（改后段不变、改首镜必变） |
| T11 | `tests/test_simplified_complete_script.py` | 三种配方经真实 seed 入口各阶段一致 |
| T12 | `tests/test_simplified_complete_script.py` | 口播继承逐镜承载与连续性分组 + 旧脚本仍走兜底 |

T11/T12 的新测试类为 `MixedTemplateFrozenContractTest`；T08–T10 的新测试类为 `MixedFirstFrameFingerprintTest`。

---

## 5. 回归结果

### 5.1 阶段 B+C 相关全量（隔离运行器）

```
test_accessory_mixed_templates.py             PASS  Ran 66 tests       OK
test_accessory_mixed_integration.py           PASS  Ran 59 tests       OK
test_category_execution_adapter.py            PASS  Ran 34 tests       OK
test_production_script_renderer.py            PASS  Ran 27 tests       OK
test_simplified_complete_script.py            PASS  Ran 56 tests       OK
test_visual_execution_contract.py             PASS  Ran  8 tests       OK
test_first_frame_contract.py                  PASS  Ran 12 tests       OK
test_original_batch_allocator.py              PASS  Ran 47 tests       OK
test_original_batch_executor.py               PASS  Ran 16 tests       OK
test_original_batch_report.py                 PASS  Ran  8 tests       OK
test_video_prompt_compaction.py               PASS  Ran 21 tests       OK
```

pytest 口径合计 **357 passed / 1386 subtests**（阶段 B 为 348，阶段 C 新增 9）。
unittest 口径合计 **324 tests, 0 failures**。

### 5.2 生产库安全

| 生产库 | 运行前 | 运行后 |
|---|---|---|
| `original_script_generator.sqlite3` | `e0024aa776bad3020379702d4f4dff46` | **未变** |
| `longform_original_video.sqlite3` | `6e150904fab2fa1dc172941e72de9876` | **未变** |
| `short_video_auto_publish.sqlite3` | `c522d01e531ed1cbe1fd209102caf045` | **未变** |

结论：全部生产库未被本次运行触碰。

> 附注：`short_video_auto_publish.sqlite3` 存在**常驻外部写入方**（周期约 10 秒，与本次工作无关）。隔离运行器已能区分"我们写的"与"别人写的"，避免守卫恒红。本次运行恰好三库全部未变。

### 5.3 本轮修改的文件

| 文件 | 改动 |
|---|---|
| `core/accessory_mixed_templates.py` | 新增 `mixed_first_frame_facts` / `frozen_mixed_contract`；storyboard 镜像新增 `continuity_group` |
| `core/first_frame_contract.py` | `_mixed_accessory_frame_line` 改为合同驱动；合同与指纹新增混合字段 |
| `core/visual_execution_contract.py` | 新增 `frozen_contract` / `accessory_frozen_contract` 透传 |
| `core/simplified_complete_script.py` | 种子里传入冻结配方；口播逐镜承载继承 |
| `tests/test_accessory_mixed_integration.py` | `MixedFrameLineGateTest` / `MixedFirstFramePromptTest` 重写；新增 `MixedFirstFrameFingerprintTest` |
| `tests/test_simplified_complete_script.py` | 新增 `MixedTemplateFrozenContractTest` |
| `wp0/run_isolated_tests.sh` | 外部写入方探测；修正一处与实际不符的提示文案 |
| `wp0/stage_c_probe.py` | 新建（78 项断言） |

---

## 6. 阶段 C 留下的关键事实（给后续维护者）

1. **`mixed_first_frame_facts` 只吃首镜，是缓存键设计，不是遗漏** —— 别为了"看起来更完整"把整份合同哈希进去。
2. **`frozen_mixed_contract` 返回值的存在与否，是"是否混合模式任务"的唯一判据**。任何新增的下游消费者都应读它，而不是读 `ORIGINAL_SCRIPT_ACCESSORY_MIXED_TEMPLATE_V1_ENABLED`。该开关只该管**新计划**。
3. **`build_accessory_mixed_visual_contract` 的两个 guard**：`frozen` 存在则开关无权否决；`CATEGORY_ZONE` 源但类目未知时返回 `{}`。
4. **`_preferred_presentation` 里 `MIXED` 故意映射为 `PERSON_ON_CAMERA`** —— 那是整片兜底。`build_simplified_voiceover_inputs` 里的整片映射表**故意不含 `MIXED`**，强迫混合片断逐镜作答。
5. **`shot_plan` 在 `direction["structure_execution_plan"]["shot_plan"]`**，不是 `direction["shot_plan"]`。
6. **`validate_simplified_visual_script` 要求 `production_design.presentation_mode == seed.creative_direction.preferred_presentation`**。混合配饰片段的正确取值是 `PERSON_ON_CAMERA`（整片兜底），不是 `MIXED` —— 写 `MIXED` 会报"承载冲突"。
7. **变异检验是必需的**：T11/T12 都通过临时还原缺陷验证过检测力。只证明"现在通过"不足以说明用例有效。

---

## 7. 下一步

**阶段 D（Review #9）：压缩不丢镜头约束。** 已定位：压缩把第一条"商品必须可见"提升为"每段"，未比较各镜值是否相同。离线复现步骤已备（4251 字符；第一镜"耳饰整体轮廓"、第二镜"背部耳针连接处"）。

**顺序不可调换**：阶段 E（Review #3）的差异签名必须按"最终镜头动作"计算 —— 即必须在阶段 B 完成之后。B、C 均已完成，D 之后即可进入 E。
