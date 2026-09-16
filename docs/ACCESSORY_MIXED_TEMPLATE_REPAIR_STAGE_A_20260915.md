# 配饰原创混合模板：阶段 A 交付说明（合同失败必须停止）

日期：2026-09-15
依据：`ACCESSORY_MIXED_TEMPLATE_IMPLEMENTATION_REVIEW_20260915.md`（Review #1、#2）
任务书：《配饰原创混合模板：下一轮具体修复任务书》§三 阶段 A
开发源：`/Users/likeu3/.openclaw/workspace/skills/original-script-generator`

本文件只描述**阶段 A**。阶段 B–E 尚未实施。

---

## 一、本阶段修了什么

### Review #1 — 四镜头合同与五镜头结构不一致仍通过校验

**修复前**：五节拍结构编译出 5 个 capture unit，与四镜模板对不上；投影函数返回
`MISMATCH` 并**按位置回退**把前 4 个镜头覆盖成模板值，第 5 个镜头没有任何
`module/carrier/face_policy`；正式脚本校验 `valid=true`，两套时间轴并存。

**修复后**（三道闸门）：

1. **结构选择阶段排除**（`original_batch_allocator.py`）
   新增 `_mixed_structure_rejection()`：混合模式生效时，按冻结模板的镜头数检查
   该 direction 的节拍数。节拍多于镜头数的结构**不进入结构池**，而是写入
   `deferred_content` 并带 `MIXED_STRUCTURE_INCOMPATIBLE:beats=5>units=4`。
   短于镜头数的结构保持原有扩展机制（重复已有 PROOF/USE 功能，不新增节拍）。
   → 不是"现场截掉第五镜头"，也不是"强制拼接节拍"。

2. **投影改为显式按 ID 映射 + 硬错误**（`accessory_mixed_templates.py`）
   `project_mixed_template_onto_units()` 重写：
   - 移除按位置静默覆盖的回退；匹配**只按 shot id**
     （`capture_unit_id` 为内部标准，`unit_id` 仅作读取边界别名）
   - 新增 `validate_mixed_shot_projection()`：重复 ID、缺镜、多镜、未知 ID、
     非正时长、承载冲突、产品状态冲突、时间轴重叠/缺口、总长不符 → 全部硬错误
   - 任一错误 → `status=MISMATCH`、`applied_units=0`，**不做部分投影**
   - 新增 `frozen_unit_timeline()`：起止时间由冻结 `duration_seconds` 累加派生，
     输出 `time_range`，成为**唯一**时间轴来源

3. **normalize 后二次校验**
   - `_project_mixed_template_shot_contract()`：`MISMATCH` 时写入
     `mixed_template_shot_projection_errors`，**不写** `mixed_template_shot_projection`
     （避免被误认为已生效），并直接 return。
   - `validate_simplified_visual_script()` 新增第 0 项检查：消费上述错误码，
     并对实际 `capture_units` 与合同**再次**调用 `validate_mixed_shot_projection`。
     → 未通过不得进入口播、首帧或生产。

### Review #2 — 混合合同校验失败仍进入可生成队列

**修复前**：合同失败只写 `mixed_template_contract_status=REJECTED`，随后照常
构建 seed、返回 `PLANNED`，执行器可选中并发起模型调用。

**修复后**（两道闸门）：

1. **规划期**（`original_batch_allocator.py::_make_item`）
   合同编译/校验失败 → **不再返回 `PlanItem`**，并写回 `deferred_content`
   （`downgrade_reason=MIXED_CONTRACT_REJECTED` + `contract_errors`）。
   → 彻底消失的洞：`PLANNED` 项携带 `REJECTED` 合同。

2. **执行期**（`original_batch_executor.py::run_script_only`）
   在 `for` 循环内、`_execute_single_item_with_timeout` **之前**统一前置检查：
   - 声明了混合模式但 `status != FROZEN` → `MIXED_CONTRACT_REJECTED`
   - 声明了但没有合同 → `MIXED_CONTRACT_MISSING`
   - 合同存在但校验失败 → `MIXED_CONTRACT_INVALID:<首个错误码>`
   → 命中即置 `SCRIPT_FAILED` 并 `continue`，**模型调用 0 次**。
   → 无混合声明的历史任务 `declared=False`，原路径不变。

### 任务书 §三-A 步骤 1 — execution scope 隔离

**修复前**：混合模式只由「类目 + 环境变量」决定。

**修复后**：
- 新增 `mixed_scope_decision(scope)`（`accessory_mixed_templates.py`），判定
  `task_branch` / `target_duration_seconds` / `is_new_plan` / `script_mode`。
- 新增 `resolve_planning_execution_scope(request, ctx)`
  （`original_batch_executor.py`）：**长视频 DIRECT_PRODUCT 借用路径**会在
  `product_context` 上打 `longform_prefer_persona_pack` / `longform_outfit_color_matching`
  标志（`scripts/run_feishu_operation_tasks.py:383-384`），据此判为 `LONGFORM` 分支。
  → 该路径的 `duration_seconds=15.0` 与短视频原创**完全一样**，只看时长会误判；
    必须看父任务自身声明。
- `allocate_batch_items()` 新增 `execution_scope` 参数并透传到
  `_build_mixed_template_injection()`；scope 不合格时返回
  `{"scope_rejected": "MIXED_SCOPE_UNSUPPORTED:branch=LONGFORM"}`，
  **不注入合同、不记失败**（这不是混合模式的业务）。
- `scope=None`（直接调用库的旧调用方/既有测试）保持历史行为不变。

### 错误码表（任务书建议码，复用同义码）

| 码 | 含义 |
|---|---|
| `MIXED_SCOPE_UNSUPPORTED` | 真实父任务不是 15 秒短视频原创 |
| `MIXED_STRUCTURE_INCOMPATIBLE` | 节拍数 > 模板镜头数，无合法映射 |
| `MIXED_CONTRACT_MISSING` | 声明了混合模式但没有冻结合同 |
| `MIXED_CONTRACT_REJECTED` | 规划期合同校验失败 |
| `MIXED_UNIT_ID_MISMATCH` | 缺 ID / 重复 ID / 未知 ID / 缺镜 |
| `MIXED_UNIT_COUNT_MISMATCH` | 镜头数与合同不一致 |
| `MIXED_TIMELINE_MISMATCH` | 时间轴缺口/重叠/总长不符 |
| `MIXED_CARRIER_MISMATCH` / `MIXED_STATE_MISMATCH` | 镜头承载 / 商品状态冲突 |
| `MIXED_UNIT_DURATION_INVALID` | 非正时长 |

---

## 二、改动文件

| 文件 | 改动 |
|---|---|
| `core/accessory_mixed_templates.py` | + 执行作用域判定、结构兼容映射、时间轴派生、投影硬校验；重写 `project_mixed_template_onto_units` |
| `core/original_batch_allocator.py` | + `_routed_beats` / `_mixed_structure_rejection`；结构池排除；`_make_item` 合同失败不产出；`execution_scope` 透传 |
| `core/original_batch_executor.py` | + `resolve_planning_execution_scope` / `_declared_mixed_contract_state` / `_frozen_package_of` / `_mixed_preflight_error`；循环内前置检查 |
| `core/simplified_complete_script.py` | 投影 MISMATCH 转硬错误；`validate_simplified_visual_script` 新增合同一致性检查 |
| `tests/test_accessory_mixed_integration.py` | +23 项回归（T01 / T02 / T03 + T18 作用域部分） |

未改动：`config/accessory_mixed_templates.json`、`core/category_execution/accessory.py`、
`core/production_script_renderer.py`、`core/first_frame_contract.py`、
`core/visual_execution_contract.py`、`core/video_prompt_compaction.py`
（分别属于阶段 B / C / D）。

---

## 三、回归用例与实际结果

| 用例 | 覆盖 | 关键断言 | 结果 |
|---|---|---|---|
| T01 | Review #1 | 5 节拍 → `compatible=False`；结构选择阶段带原因排除；4 节拍保留；**编译出 5 镜后 `valid=false` 且不写 applied projection** | ✅ |
| T02 | Review #2 | 无声明放行；`REJECTED` / 缺失 / 16 秒 / 重复 ID / 缺镜 → 全部非空错误码；**跑真实 `run_script_only`，模型调用 0 次且置 `SCRIPT_FAILED`**；合法合同仍到达模型 | ✅ |
| T03 | 容错 | A/B/C 三模板 → 4 个稳定 ID（CU_01–04）、`APPLIED`、时间轴连续且合计 15 秒、三种承载齐全 | ✅ |
| T18(部分) | 任务书 §三-A.1 | 长视频作用域 → `LONGFORM` 且拒绝；短视频 15 秒 → 放行；30 秒 / 续跑 / 非 simplified → 拒绝；**长视频作用域不构建合同** | ✅ |

**修复前失败证据**：T01 的 `test_compiled_five_shot_script_is_not_valid` 与 T02 的
`test_running_a_blocked_item_calls_no_model` 在改动前的实现下分别得到
`valid=true` 与"进入模型调用"，本次改动后由这两条用例守住。

**全量相关回归（隔离环境，临时数据目录）**：

```
test_accessory_mixed_templates.py      PASS  50
test_accessory_mixed_integration.py    PASS  36   (原 13 + 新增 23)
test_category_execution_adapter.py     PASS  34
test_simplified_complete_script.py     PASS  53
test_production_script_renderer.py     PASS  27
test_visual_execution_contract.py      PASS   8
test_first_frame_contract.py           PASS  12
test_original_batch_allocator.py       PASS  47
test_original_batch_executor.py        PASS  16
test_original_batch_report.py          PASS   8
test_video_prompt_compaction.py        PASS  21
------------------------------------------------
合计 312 项，0 失败；三个生产库 md5 逐字未变
```

---

## 四、未完成 / 下一步

阶段 A 的完成标准（任务书）："Review 的五镜头样例明确失败，16 秒非法合同不进入
模型调用，合法四镜头能完成本地完整投影" → **三条均已由回归覆盖并通过**。

后续阶段（尚未实施）：

- **B** Review #5、#6：类目 × 镜头模块裁切规则分层；物理子类型动作与三态证据
- **C** Review #4、#7、#8：首帧 / 光线 / 中央口播统一读取冻结合同
- **D** Review #9：提示词压缩不得丢镜头约束
- **E** Review #3：按最终镜头做批次与历史差异判断

注意：阶段 E 依赖阶段 B（"给废弃的旧动作算签名"必须先修好），
实施顺序不可调换。
