# 混剪系统钩子能力 · 修订版开发方案（v2）

> 本版基于对 `auto_mixcut/` 当前代码的逐行核实重写，修正了原方案（交付版）中失准的代码引用，并重新评估每个改动与现有种草线的冲突点。**核心目标不变：增强钩子识别与利用，几乎不影响原有线。**
>
> 阅读顺序：第 0 节(定调)→ 第 1 节(根因·基于真实代码)→ 第 2-4 节(三个改动)→ 第 5 节(冲突点与零侵入保障)→ 第 6 节(验收)→ 第 7 节(上线)。

---

## 0. 总览与架构定调

### 0.1 一句话定调
只有「一套混剪引擎 + 一个公共素材库 + 一套打标 + 多个投放模板」。投流(AD-FAST)不是独立产线，是线3 混剪引擎的一个模板变体。

### 0.2 三个最小改动

| 改动 | 性质 | 对原线影响 | 章节 |
|---|---|---|---|
| 改动1：`_segment_score` 加 hook_strength 加分 | 纯加分项，零结构改动 | **极低**：老素材 hook_strength 值已存在(null/weak/medium/strong)，加分对种草模板同样生效但权重低，详见 §5.1 | §2 |
| 改动2：segment_tags 加 `hook_visual_type` 列 + 打标多输出该字段 | 只加 nullable 列，不动现有语义 | **极低**：老数据 NULL，现有选片/校验零消费该字段，详见 §5.2 | §3 |
| 改动3：投流模板 `AD_FAST_HOOK_8S` + 补钩子链路 | 加 1 模板 + 1 薄编排 skill + 主路径新增 constraints 组装 | **低但非零**：需在 `create_plans` 主路径新增 constraints 组装(当前不传)，详见 §5.3 | §4 |

### 0.3 串成完整能力

```
公共钩子素材库(segments + segment_tags)
  真实素材(切片打标) + AI补钩子素材(即梦→回流)
        │
        │ 同一套 _segment_score(改动1:钩子加权)
        ├──────────────┬──────────────┐
        ▼              ▼              ▼
   种草模板(养号)   投流模板(改动3)   未来投放人格
   15s / C1-C9     6-10s / 硬底线

打标侧(改动2):每段多打 hook_visual_type(视觉钩子类型)
缺钩子时(改动3):投流预检→补充队列→prompt_factory→即梦出片→回流
卖点(4.11):创建投流任务可选传 selling_points→卖点→视觉转译→融进补钩子提示词
```

### 0.4 系统不产"文案"
auto_mixcut 目前是**纯无声视频混剪 + BGM**。`orchestrator.py:57-118` 主流水线无任何文案/字幕/口播环节。`render_skill.py` 渲染管线**预留了字幕能力**(`_subtitle_plan` + `_drawtext_filter`，`render_skill.py:602-647`)，但当前 `plan_json.subtitles` 恒空，从不烧录。
- 不新建文案层、不烧字幕、不改 render。
- 人工卖点是可选输入，唯一归宿是→喂进补钩子提示词，变成画面。

---

## 1. 根因：钩子为什么弱（基于真实代码）

### 1.1 真实代码事实

`_segment_score` 是选片打分函数，实际定义在 `render_plan_skill.py:712-800`。其全部加分项（实读 `:722-752`）：

| 加分项 | 分值 | 实际行号 |
|---|---|---|
| `source_trust_level == "high"` | +25 | `:722-723` |
| `product_binding_type == "exact_sku"` | +20 | `:724-725` |
| `product_match_status` 可信 | +20 | `:726-727` |
| 可信实拍 `is_real_source` | +35（首位实拍再 +25） | `:731-733` |
| 首镜且 effective_role ∈ {hero,result,detail} | +20 | `:736-737` |
| source_type 命中 slot.preferred_source_types | +18 | `:738-739` |
| segment_type 匹配 slot.segment_type | +18 | `:743-744` |
| AI prompt 包(source_type=ai_generated + 有 identity/scene_tag) | +16（fill 模式再 +18） | `:745-748` |
| preferred_source_trust 命中 | +12 | `:749-750` |
| preferred_binding 命中 | +10 | `:751-752` |
| constraints.require_product_visibility==high 且命中 | +20 | `:788-789` |
| constraints.prefer_source_trust 命中 | +18 | `:786-787` |

**没有任何一项给 hook_strength=strong 加分。** 这点原方案判断正确。

### 1.2 hook_strength 当前的真实消费点（修正原方案错误）

原方案称 hook_strength "在 effective_role_skill.py:93,102,130 判 hero 资格"——**这是错的**。grep 全文：`effective_role_skill.py` **零次**出现 hook_strength。

hook_strength 当前真实消费点只有 3 处：

| 位置 | 用途 | 性质 |
|---|---|---|
| `render_plan_skill.py:1170` `_prefer_first_slot_pool` | 首镜偏好池要求 hook_strength ∈ {strong,medium}；不满足的片不进偏好池，但会 fallback 到全池 | **软偏好**（非硬门槛，有 fallback） |
| `render_plan_skill.py:898` `_enrich_segments_for_selection` | 加载 tag 字段进 enriched segment | 数据加载 |
| `quality_gate_skill.py:57` | 机检：首镜 hook_strength 必须 strong/medium，否则 `first segment hook is weak` fail | **硬门槛**（成片质检阶段） |

### 1.3 修正后的根因

hook_strength 的现状是「**软偏好池 + 机检硬门槛 + 选美零加分**」：
- 选片时 `_prefer_first_slot_pool`(`:1166-1175`) 会先把 strong/medium 的片挑进偏好池优先选，但**偏好池空了就 fallback 全池**(`return safe or pool`)，所以 weak 片照样能进首镜。
- 进了候选池后，`_segment_score` 打分时 hook_strength **完全不参与**，强钩子片只要信任分稍低或被用过(usage_count 惩罚 `:770-773`)，立刻被普通片挤掉。
- 真正的硬卡点在最后的机检 `quality_gate_skill.py:57`——但那是"事后质检"，不是"事前优选"。

**后果**：库里有强钩子片，选片阶段不会被优先选中；机检虽然卡 weak，但只能让片子"勉强过线"，不能让强钩子"被优选"。
**治本** = 改动1：在 `_segment_score` 选美阶段给 hook_strength 加分，让强钩子主动胜出，而不是靠机检兜底。

---

## 2. 改动1：钩子加权（治本，两条线同受益）

### 2.1 文件与函数
`auto_mixcut/auto_mixcut/skills/render_plan_skill.py` → `_segment_score`(`:712-800`)

### 2.2 新增加分逻辑

插入位置：`_segment_score` 末尾，`return score` 之前（当前函数没有显式 return，需确认；实际末尾在 `:800` 附近 `if slot_index <= 3 and source_trust_level == "low": score -= 35` 之后）。

```python
# —— 钩子强度加权（改动1）。全部数值走 config，不硬编码。——
hook = _latest_tag_value(ctx, segment, "hook_strength")
hook_weights = _hook_score_weights(ctx)          # 见 §2.3
base = hook_weights.get(str(hook), 0.0)          # strong/medium/weak/None→0
if slot_index == 1:
    base *= hook_weights.get("first_slot_multiplier", 1.0)
base *= float(constraints.get("hook_weight_scale", 1.0))
score += base
```

**零侵入关键点**：
- `constraints` 已是 `_segment_score` 入参（`:712` 签名 `constraints: dict | None = None`，`:713` `constraints = constraints or {}`），无需改签名。
- `_latest_tag_value` 已存在（`:1232`），读法与 `:1170` 一致。
- `hook_weight_scale` 种草模板不传 → `constraints.get("hook_weight_scale", 1.0)` → 1.0 → 行为不变。

### 2.3 config

新增配置块 `config/render_scoring.yaml`（新建文件，挂载到 `render_plan_skill` 读 config 处；加载方式参照 `segment_prompt_factory_skill.py:733` 的 `_load_factory_config` 模式）：

```yaml
hook_score_weights:
  strong: 40          # 强钩子基础加分（对标 source_trust=high 的 +25，确保首镜能盖过普通片）
  medium: 15
  weak: 0
  none: 0
  first_slot_multiplier: 2.0   # 首镜钩子权重翻倍
```

helper：
```python
def _hook_score_weights(ctx: SkillContext) -> dict:
    cfg = _load_render_scoring_config(ctx) or {}
    return cfg.get("hook_score_weights", {}) or {}
```

**数值标定**（修正原方案过强风险）：
- 现有最高单项加分：可信实拍 `is_real_source +35`。
- strong=40 + 首镜×2 = 80（首镜位），能盖过单 trust 项(+25)但不能盖过可信实拍组合(`+35+25+20+20+20=120`)。
- 非首镜 strong=40，不会无脑碾压 trust/binding 组合(`+20+20+20=60`)，保留多样性。
- **对 AI 钩子素材的特殊影响**（原方案未提）：AI 生成素材拿不到 `is_real_source +35`，只能拿 `+16(prompt_package)`。strong=80 首镜加分会让 AI 强钩子片大幅胜出 AI 普通片——这是投流想要的效果。批次去重账本(`first_assets`，`:766-767`)会阻止同 asset 垄断首镜，无垄断风险。

### 2.4 不动清单（对原线零侵入）
- ✅ 只新增加分项，不改任何现有加分/减分行（`:722-800` 现有逻辑全保留）。
- ✅ 不改 hook_strength 语义、不改 `_prefer_first_slot_pool`(`:1166`)、不改 `_passes_first_slot_floor`(`:1098`)、不改 `quality_gate_skill.py:57`。
- ✅ 老素材 hook_strength=null → `hook_weights.get("None", 0.0)`... 注意：`_latest_tag_value` 返回 None 时 `str(None)="None"`，config 里 `none:0` 键名要写成 `"None"` 或 helper 做归一化。**实现细节**：helper 里 `key = "none" if hook is None else str(hook)`，config 用小写 `none`。
- ✅ 回归开关：config `hook_score_weights` 全置 0 → 一键回退。
- ✅ 种草模板走 `create_plans` 主路径，constraints 为空 dict（见 §5.3），`hook_weight_scale` 取默认 1.0，钩子加权按 config 基础值生效——这是"两条线同受益"的体现，不是"零影响"。**若要种草线完全不受影响**，可把种草模板的 constraints 显式传 `hook_weight_scale: 0.0`（见 §5.3 方案 B）。

---

## 3. 改动2：视觉钩子打标字段（不扩散）

### 3.1 两个钩子字段正交分工
- `hook_strength`(strong/medium/weak) = 钩子"有多强"（已存在，改动1 消费）。
- `hook_visual_type` = 钩子"是什么视觉类型"（本改动新增，改动3 消费）。

### 3.2 视觉钩子枚举（7 类 + none）

| 枚举 | 含义 | 判断要点 |
|---|---|---|
| unboxing | 开箱/拆封 | 手拆包装、取出商品 |
| before_after | 前后对比 | 同画面/相邻镜头使用前 vs 后 |
| effect_reveal | 效果揭示 | 佩戴/使用后效果"亮出来"瞬间 |
| detail_macro | 细节特写 | 材质/做工/局部强特写 |
| action | 上身/佩戴动作 | 戴上/系上/穿上的连续动作 |
| face_emotion | 人脸情绪 | 清晰人脸表情/看镜头 |
| product_reveal | 产品亮相 | 产品首次清晰呈现(无强动作) |
| none | 无明显视觉钩子 | 平稳氛围/背景镜头 |

### 3.3 精确改动点（真实行号）

**(a) 数据库：segment_tags 加 nullable 列**
`migrations/001_sqlite_init.sql` 的 `segment_tags` 表（实际建表语句在 `:205-224` 区间，`hook_strength TEXT,` 在 `:212`）之后加：
```sql
hook_visual_type TEXT,   -- 改动2:视觉钩子类型(nullable)
```
`migrations/001_mysql_init.sql` 对应表同步加。存量库：`ALTER TABLE segment_tags ADD COLUMN hook_visual_type TEXT;`

**(b) 打标 prompt 加字段**
打标 prompt 实际在 `auto_mixcut/auto_mixcut/skills/llm_prompts.py:63`（`"hook_strength": "strong|medium|weak",`），**不是**原方案说的 `llm_router_skill.py:678`。在 `:63` 之后加：
```json
"hook_visual_type": "unboxing|before_after|effect_reveal|detail_macro|action|face_emotion|product_reveal|none",
```
并在 prompt 判断标准段（`llm_prompts.py` 同文件的 secondary_roles 说明之后）加视觉钩子判定说明。

**(c) 解析兜底**
`llm_router_skill.py` 的 `_normalize_segment_tag` 函数（原方案说 `:704`，需在实现时定位真实行号）加枚举集合与返回字段：
```python
hook_visual_types = {"unboxing","before_after","effect_reveal","detail_macro","action","face_emotion","product_reveal","none"}
# 返回 dict 里 hook_strength 之后加：
"hook_visual_type": _enum(data.get("hook_visual_type"), hook_visual_types, "none"),
```

**(d) 写库**
`ai_tagging_skill.py:176`（`"hook_strength": tag["hook_strength"],`）之后加：
```python
"hook_visual_type": tag.get("hook_visual_type", "none"),
```
fallback tag 构造处（`ai_tagging_skill.py:579` 附近的 mock/fallback 路径，`"hook_strength": "strong" if role in {"hero","result"} else "medium",`）也补 `"hook_visual_type": "none"`。

**(e) prompt_version 处理（重大修正）**

**原方案错误**：称"_pending_segments 按 prompt_version 判定是否需重标"。

**真实代码**（`ai_tagging_skill.py:21-65`）：`submit_batch`/`poll_results` 的去重逻辑是 `_latest_tags_by_segment`——**只看该 segment 有没有最新 tag，不看 prompt_version**。已有 tag 的 segment 直接 skip，`force=True` 才强制重标。

**结论**：
- 升 prompt_version（v1.0→v1.2）**不会触发全量重标**（已有 tag 的都被 skip），无烧钱风险。原方案"按需重标"的担心不存在。
- 但反过来，**老素材的 hook_visual_type 也不会自动补上**——它们已被 skip。要给老素材补 hook_visual_type，必须对目标 product 显式调 `poll_results(force=True)`。
- **本方案处理**：默认 `prompt_version` 保持 `v1.0`（`ai_tagging_skill.py:21,42` 默认值），**不升版**。新切片自然走打标拿到 hook_visual_type；老素材在投流预检（改动3）发现缺 hook_visual_type 时，按需对单个 product `force=True` 重标。这样彻底避免任何全量动作。

### 3.4 不动清单
- ✅ 不改 hook_strength 语义、取值、任何消费点。
- ✅ 不改 `_segment_score`(改动1 只读 hook_strength，不读 hook_visual_type)。
- ✅ 不改 `_prefer_first_slot_floor` / `quality_gate_skill.py:57`。
- ✅ 老素材 hook_visual_type=NULL → 现有流程零消费，无感。
- ✅ prompt_version 不升，无重标风暴。

---

## 4. 改动3：投流模板 + 补钩子链路

### 4.1 新增投流模板

`config/templates.yaml`（现有 20 个模板，均 15s 种草向）新增：
```yaml
- template_id: AD_FAST_HOOK_8S
  duration_ms: 8000
  default_moods: [premium_clean, daily_clean]
  description: 投流极简版，钩子怼最前3秒，信息流投放用。
  suitable_categories: [hair_accessories, earrings, womens_top, scarf_hat, generic_fashion]
  template_objective: ad_fast_hook
  pacing: fast
  required_roles: [hero]
  risk_policy:
    require_no_watermark_for_first_slot: true
    avoid_subtitle_risk_in_first_slot: true
    allow_risky_segments_after_slot: 2
    fail_only_on_severe_mismatch: true
  source_policy:
    prefer_high_trust: true
    allow_medium_trust: true
    avoid_low_trust_core_slots: true
  selection_policy:                       # ★投流模板专属
    hook_weight_scale: 2.0
    require_hook_visual_first_slot: true
  bgm_profile:
    moods: [premium_clean, daily_clean]
    energy: high
  slots:
    - {role: hero, duration_ms: 3000, preferred_source_trust: high}
    - {role: result, duration_ms: 3000}
    - {role: detail, duration_ms: 2000, preferred_binding: exact_sku}
```

**冲突点：TemplateSpec 需加字段**。当前 `render_plan_skill.py:51-62` 的 `TemplateSpec` 类只有 `risk_policy`/`source_policy`，**没有 `selection_policy`**。`_load_template`(`:1410` 附近) 也只解析 `risk_policy`/`source_policy`。
- 必须给 `TemplateSpec` 加 `selection_policy: dict = field(default_factory=dict)`。
- `_load_template` 加 `selection_policy=dict(spec.get("selection_policy") or {})`。
- 现有 20 个种草模板没有 `selection_policy` 键 → 解析为空 dict → 默认行为不变。**零侵入**。

### 4.2 钩子权重注入（衔接改动1）

**冲突点：主路径当前不组装 constraints**。

真实代码：
- `create_plans`(`:208`) 调 `_select_segments` 时**不传 constraints**（签名 `:545` 的 constraints 默认 None → `:546` 变 `{}`）。
- 只有 `create_remix_plan`(`:334`) 传 `remix_plan.get("constraints")`。

**修正方案**：在 `create_plans` 调 `_select_segments` 前，从 template 组装 constraints：
```python
# render_plan_skill.py:208 附近，改前：
selected = _select_segments(self.ctx, product_id, template.slots, batch_state=batch_state, variant_no=variant, template=template)
# 改后：
template_constraints = _build_template_constraints(template)  # 新 helper
selected = _select_segments(self.ctx, product_id, template.slots, batch_state=batch_state, variant_no=variant, template=template, constraints=template_constraints)
```

helper：
```python
def _build_template_constraints(template) -> dict:
    sp = getattr(template, "selection_policy", {}) or {}
    constraints = {}
    if sp.get("hook_weight_scale"):
        constraints["hook_weight_scale"] = sp["hook_weight_scale"]
    if sp.get("require_hook_visual_first_slot"):
        constraints["require_hook_visual_first_slot"] = True
    return constraints
```

**零侵入保障**：种草模板 `selection_policy={}` → `_build_template_constraints` 返回 `{}` → `_select_segments` 收到空 constraints → 与改动前完全等价（之前传 None 也是变 `{}`）。`_filter_constraints`(`:856`) 和 `_segment_score`(`:712`) 对空 constraints 的处理已经是现状。

### 4.3 首镜必须有视觉钩子（衔接改动2）

`require_hook_visual_first_slot: true` 时，在 `_filter_constraints`(`:856`) 末尾加过滤：
```python
if slot_index == 1 and constraints.get("require_hook_visual_first_slot"):
    hv = _latest_tag_value(ctx, segment, "hook_visual_type")
    if not hv or hv == "none":
        continue  # 过滤掉无视觉钩子的片
```
**注意**：这是硬过滤。若全池被过滤光，`_select_segments` 会返回 `SKIPPED_LOW_QUALITY`（沿用 `:577-579` 现有机制），触发补充队列（§4.5）。**禁静默 fallback**——不偷偷降级选无钩子片。

### 4.4 触发节点：创建投流任务时主动预检

**冲突点：当前无"创建任务时预检"路径**。

`_sync_material_supplement_queue`(`:1216`) 当前只在 `create_plans` 选片失败后(`:238`)被动触发，`detail` 是 `selected.error.detail`（选片失败的缺口信息）。没有"创建任务时主动预检钩子覆盖"的逻辑。

**新增预检函数**（挂在投流任务创建后，不进主流水线）：
```python
def precheck_hook_coverage(ctx, product_id, template) -> dict:
    """创建投流任务后调用。返回 {ok: bool, gap: dict}。"""
    segments = _enrich_segments_for_selection(ctx, ctx.repo.list_where("segments", "product_id=?", (product_id,)))
    if template.selection_policy.get("require_hook_visual_first_slot"):
        candidates = [s for s in segments
                      if "hero" in (s.get("effective_roles_json") or [])
                      and not _asset_has_watermark(ctx, s)
                      and _passes_first_slot_floor(ctx, s, template.risk_policy)[0]
                      and (_latest_tag_value(ctx, s, "hook_visual_type") or "none") != "none"]
        shortfall = 1 - len(candidates)  # 投流模板首镜至少要 1 条钩子候选
        if shortfall > 0:
            return {"ok": False, "gap": {"missing_role": "hero", "expected_hook_visual_type": "any", "shortfall": shortfall}}
    return {"ok": True, "gap": {}}
```

**调用点**：新增 CLI `create-ad-task`（或复用 `create-task` 加 `--template AD_FAST_HOOK_8S`），创建后立即跑 `precheck_hook_coverage`。不满足则写补充队列（§4.5），任务状态置 `WAITING_HOOK_SUPPLEMENT`，不进 render。

**不进主流水线**：`orchestrator.run_product`(`:36`) 完全不改，种草线不碰预检逻辑。

### 4.5 补充队列扩展

`_sync_material_supplement_queue`(`:1216`) 当前只写固定字段，无缺口画像。扩展为带 `payload_json`：

**冲突点：feishu_sync_records 是否有 payload_json 字段、upsert 是否透传**。需核实 `migrations` 和 `repo.upsert` 实现。若 repo 层有字段白名单，加列不够，还得改 upsert。

**修正方案**：
1. `migrations` 给 `feishu_sync_records` 加 `payload_json TEXT`（两份）。
2. 核实 `rds_repository_skill.py` 的 `upsert` 是否透传任意字段；若有白名单，把 `payload_json` 加入允许集。
3. `_sync_material_supplement_queue` 加 `payload_json` 写入（见原方案 §4.5 代码，保留）。

**零侵入**：现有调用方（`:238`）传的 detail 结构不变，只是多写一个 JSON 字段，老逻辑读不到也不受影响。

### 4.6 接 prompt_factory：新增 HookSupplementSkill

**原方案正确点**：`segment_prompt_factory_skill.build_packages`(`:124`) 真实存在，签名 `build_packages(material_anchor_brief, template_slot, count=None, persist=True)`，可复用。

**新增薄 skill** `auto_mixcut/auto_mixcut/skills/hook_supplement_skill.py`：
```python
class HookSupplementSkill:
    def run(self, product_id: str) -> Result:
        gaps = self.ctx.repo.list_where(
            "feishu_sync_records",
            "object_type='material_supplement' AND sync_status='pending'", ())
        factory = SegmentPromptFactorySkill(self.ctx)
        produced = []
        for g in gaps:
            p = json.loads(g.get("payload_json") or "{}")
            if p.get("product_id") != product_id:
                continue
            brief = self._build_brief(product_id, p)   # 含卖点融合，见 §4.11
            slot = {
                "role": p.get("missing_role", "hero"),
                "segment_type": p.get("expected_segment_type", "home_lifestyle"),
                "duration_ms": 4000,        # ★即梦 AI 片段硬约束=4s
            }
            r = factory.build_packages(brief, slot, count=p.get("shortfall", 3))
            if not r.ok:
                return r
            produced.extend(r.value.get("packages", []))
        return Result.ok({"produced_prompts": len(produced)})
```

### 4.7 prompt_factory 硬约束（已核实存在）

`_validate_package`(`segment_prompt_factory_skill.py:707-718`) 真实强制：

| 约束 | 实际行号 | 说明 |
|---|---|---|
| AI 片段时长必须 = 4s | `:712` `duration_sec != 4` | 即梦单镜头约束 |
| negative 必须含 no cut + no watermark | `:714-717` | 禁切镜、禁水印 |

**修正原方案**：原方案称"外套禁 ai_full_face 在 `:318`"——**不在 `_validate_package` 里**。ai_full_face 判断在 `:836` `_is_full_face_framing` 和 `:890-896` `_person_framing`，由品类执行契约 `category_execution_contract` 控制。补钩子时 brief.category 走该品类契约，自动继承禁用，无需额外代码。

### 4.8 回流
即梦网页版出片 → 导入为新 asset → 走正常 `orchestrator`(probe→segment→tag→effective_role) → `factory.mark_imported`(`:159`，已存在) 回填 → 入公共库。**回流后的 AI 钩子素材两模板共用，无归属**。

### 4.9 完整闭环图（同原方案，略）

### 4.10 不动清单
- ✅ 不新建产线：AD_FAST_HOOK_8S 只是 templates.yaml 一个新模板。
- ✅ `orchestrator.run_product`(`:36-119`) 完全不改——投流任务走单独入口，不进种草流水线。
- ✅ 复用 `_sync_material_supplement_queue` + `segment_prompt_factory_skill` + `mark_imported`。
- ✅ 种草模板 `selection_policy={}` → constraints 空 → 行为不变。

---

## 4.11 卖点与文案钩子的闭环

### 4.11.1 现状铁证（已核实）
auto_mixcut 无文案层。`orchestrator.py:57-118` 主流水线无文案环节。`render_skill.py:602-647` 预留字幕能力但 `plan_json.subtitles` 恒空。`product_anchor_skill.py:41` 的 `core_visual_points` 是视觉锚点(给 AI 画面看)，非营销卖点。

### 4.11.2 定调
卖点的归宿是"变成画面"，不是"变成文案"。不新建文案层、不改 render、不烧字幕。

### 4.11.3 卖点加在哪个节点
创建投流任务时可选传 `selling_points`(字符串数组，可空)，透传进补充队列 `payload_json`，`HookSupplementSkill._build_brief` 融进提示词。

### 4.11.4 卖点融进提示词（复用现有字段，已核实）
`segment_prompt_factory_skill.build_package`(`:44`) 的 brief 已有挂载点：
- `brief.primary_visual_result` → 视觉结果层
- `brief.must_show` → 必现要素
- `template_slot.hook_intent` → 钩子意图

卖点经"中文卖点→英文视觉短语"转译后注入。转译用一次离线 LLM 调用（只在补钩子阶段，不违反"在线组装禁 LLM"）。失败/无卖点 → 走纯视觉兜底，不阻塞。

```python
def _build_brief(self, product_id, gap_payload):
    product = self.ctx.repo.get("products", "product_id", product_id)
    anchor = json.loads(product.get("product_anchor_json") or "{}")
    brief = {
        "product_id": product_id,
        "category": gap_payload.get("category") or product.get("category"),
        "hard_anchors": anchor.get("core_visual_points", []),
        "must_not_show": anchor.get("must_not_change_points", []),
    }
    selling_points = gap_payload.get("selling_points") or []
    if selling_points:
        visual = self._translate_selling_to_visual(selling_points)
        if visual:
            brief["primary_visual_result"] = visual[0]
            brief["must_show"] = visual
    return brief
```

### 4.11.5 三个钩子概念分工（不重叠）
| 概念 | 是什么 | 谁产 | 落点 | 用途 |
|---|---|---|---|---|
| hook_strength(改动1) | 钩子有多强 | 打标 AI | segment_tags | 选片加权 |
| hook_visual_type(改动2) | 钩子是什么视觉类型 | 打标 AI | segment_tags | 投流按类型检索 |
| selling_points(本节) | 人工卖点(可选) | 人+LLM转译 | 投流任务参数→brief | 让 AI 补钩子贴卖点 |

---

## 5. 冲突点汇总与零侵入保障（本版重点）

### 5.1 改动1 对种草线的影响

| 检查项 | 结论 |
|---|---|
| 种草模板会吃到钩子加权吗？ | **会**。`create_plans` 主路径 constraints 为空，`hook_weight_scale=1.0`，strong 片首镜 +80。这是"两条线同受益"的设计意图，**不是 bug**。 |
| 会不会改变种草线现有选片结果？ | **可能微调**。库里有 strong 钩子片的种草任务，首镜更可能选到强钩子片。这正是需求"增强钩子利用"。 |
| 若要种草线完全不变？ | 方案 B（已弃用）：种草模板 yaml 加 `selection_policy: {hook_weight_scale: 0.0}` 显式关闭。需给所有 20 个种草模板加键，工作量大且违背"两条线受益"初衷。 |
| **最终决策** | **采用方案 A**：接受微调，默认两线同受益钩子加权，用 A/B 数据验证。回归开关=config 全置 0。不实施方案 B。 |
| 回归手段 | config `hook_score_weights` 全置 0 一键回退。 |

### 5.2 改动2 对种草线的影响

| 检查项 | 结论 |
|---|---|
| 老素材 hook_visual_type=NULL 影响选片吗？ | **不影响**。改动1 只读 hook_strength，不读 hook_visual_type。hook_visual_type 只在改动3 的投流模板 `require_hook_visual_first_slot` 过滤时用。 |
| prompt_version 不升版有副作用吗？ | **无**。`ai_tagging_skill.py:21,42` 默认 v1.0 保持，新切片打标 prompt 已含 hook_visual_type 字段（改动2 改了 llm_prompts.py），老素材 skip 不重标。 |
| 打标 prompt 加字段会让模型变慢/变贵吗？ | **极小**。多输出一个枚举字段，对 vision LLM 调用几乎无感。 |

### 5.3 改动3 对种草线的影响（核心冲突区）

| 冲突点 | 现状 | 修正方案 | 对种草线影响 |
|---|---|---|---|
| **constraints 主路径不传** | `create_plans:208` 不传 constraints | 新增 `_build_template_constraints(template)`，从 selection_policy 组装 | 种草模板 selection_policy={} → 返回 {} → 等价于改动前(传 None→{})。**零影响** |
| **TemplateSpec 无 selection_policy 字段** | `:51-62` 只有 risk/source_policy | 加 `selection_policy: dict = field(default_factory=dict)` + `_load_template` 解析 | 现有 20 模板无此键 → 空 dict。**零影响** |
| **预检逻辑不存在** | `_sync_material_supplement_queue` 只被动触发 | 新增 `precheck_hook_coverage` + 新 CLI 入口，不进 orchestrator | `run_product` 不改，种草流水线不碰预检。**零影响** |
| **feishu_sync_records 无 payload_json** | `:1216` 只写固定字段 | 加列 + 核实 upsert 透传 | 现有调用方多写一个字段，老逻辑不读。**零影响** |
| **首镜硬过滤 require_hook_visual_first_slot** | `_filter_constraints:856` 无此逻辑 | 加 slot_index==1 过滤 | 只在投流模板 constraints 含此键时生效；种草模板 constraints 空，不过滤。**零影响** |

### 5.4 铁律
1. 不硬编码，全走 config。
2. 不删现有 slot 字段、不动 20 个种草模板现有字段。
3. 不改 hook_strength 语义、不改 `_prefer_first_slot_pool`/`_passes_first_slot_floor`/`quality_gate_skill.py:57`。
4. 取池禁静默 fallback——首镜无钩子候选 → 走补钩子链路，不降级选烂片。
5. 投流模板复用现有混剪引擎(`_select_segments`/`_segment_score`/`render_skill`)，不另起组装器。
6. 补钩子链路复用 `_sync_material_supplement_queue` + `segment_prompt_factory_skill`，不新建产线。
7. `orchestrator.run_product` 完全不改。

---

## 6. 验收清单

### 6.1 改动1（钩子加权）
| # | 用例 | 期望 |
|---|---|---|
| 1-V1 | 同 role 池含 1 strong + N weak，首镜 | strong 片被选中 |
| 1-V2 | strong 片 usage_count 偏高但阈值内 | 首镜仍优先 strong |
| 1-V3 | **20 个种草模板全量重跑** | 与基线一致或仅"更优选强钩子"，无新增 SKIPPED/校验失败 |
| 1-V4 | config strong:0 | 退化为改动前行为 |
| 1-V5 | 投流模板 hook_weight_scale=2.0 vs 种草 1.0 | 投流首镜钩子片选中率显著更高 |
| 1-V6 | **同批次首镜不重复用同一 asset** | `batch_state.first_assets` 去重生效 |

### 6.2 改动2（视觉打标字段）
| # | 用例 | 期望 |
|---|---|---|
| 2-V1 | 新切片打标 | segment_tags 多出 hook_visual_type，值在枚举内 |
| 2-V2 | 模型返回非法值/缺字段 | 解析兜底为 none，打标不失败 |
| 2-V3 | **存量素材不重标(prompt_version 不升)** | hook_visual_type=NULL，种草线/20 模板重跑与基线一致 |
| 2-V4 | 明显开箱片 | hook_visual_type=unboxing |
| 2-V5 | 平稳背景片 | hook_visual_type=none |
| 2-V6 | **对老 product force=True 重标** | hook_visual_type 补上，不影响其他 product |

### 6.3 改动3（投流模板 + 补钩子）
| # | 用例 | 期望 |
|---|---|---|
| 3-V1 | product 库存充足，创建投流任务 | 直接 render，不触发补钩子 |
| 3-V2 | product 缺首镜钩子，创建投流任务 | 补充队列出现带 payload_json 的缺口记录 |
| 3-V3 | 跑 HookSupplementSkill | prompt_package 表新增 N 条 4s/含 no cut+no watermark 的提示词 |
| 3-V4 | 外套品类补钩子 | 走 category_execution_contract，不含 ai_full_face |
| 3-V5 | 回流导入 AI 片 | tag 自动带 hook_visual_type；mark_imported 回填；投流模板能检索到 |
| 3-V6 | 投流模板 vs 种草模板取同 product | 投流首镜钩子片选中率显著更高 |
| 3-V7 | 投流成片时长 | 落 6-10s，首镜 hook_visual_type≠none |
| 3-V8 | **种草任务走 run_product** | 不触发预检、不写补充队列、行为与改动前一致 |

### 6.4 卖点融合
| # | 用例 | 期望 |
|---|---|---|
| 4-V1 | 创建投流任务带 selling_points=["保暖不臃肿"] | payload_json 含 selling_points；补钩子提示词 positive 含转译视觉短语 |
| 4-V2 | 不带 selling_points(如耳环) | 补钩子走纯视觉佩戴效果路径，不报错 |
| 4-V3 | 转译失败/无 LLM | 跳过卖点注入，走纯视觉兜底，不阻塞 |
| 4-V4 | 全程 render | 投流片不含烧录字幕 |

---

## 7. 上线顺序与 P0 验证

### 7.1 开发顺序
1. **改动1**（加权）——独立、治本、风险最低，可先上。**唯一对种草线有微调的改动**，上线后跑 20 模板回归 + A/B 观察一周。
2. **改动2**（打标字段）——独立，为改动3 铺数据。prompt_version 不升，零风险。
3. **改动3**（投流模板 + 补钩子）——依赖改动1、2。工作量比原方案呈现的大（需新增 constraints 组装 + 预检函数 + CLI 入口 + HookSupplementSkill）。

### 7.2 P0 验证（强烈建议先验红利再投改动3）
写改动3 代码前，先手动验证投流红利：
- 手动剪 10 条 4 钩子变体（用现有库存强钩子片），投信息流小预算。
- 对比现有 15s 种草片 CTR/完播。
- 红利为真 → 投改动3；证伪 → 省下整套补钩子链路。

### 7.3 与原方案的差异总结
| 项 | 原方案 | 本版修正 |
|---|---|---|
| 行号引用 | 多处失准 | 全部核实修正 |
| 根因(effective_role 用 hook_strength) | 错误(实际零用) | 修正为 `_prefer_first_slot_pool:1170` + `quality_gate:57` |
| prompt_version 升版触发重标 | 误判机制存在 | 修正：ai_tagging 不看版本，只看有无 tag；本版不升版 |
| constraints 主路径注入 | 假设已组装 | 修正：主路径不传，需新增 `_build_template_constraints` |
| TemplateSpec.selection_policy | 假设已支持 | 修正：需加字段+解析 |
| 预检触发器 | 称"薄触发器" | 修正：当前无此路径，需新建预检函数+CLI，工作量更大 |
| _validate_package 4s/no-cut 约束 | 行号错但存在 | 核实存在于 `:712-717` |
| 外套禁 ai_full_face 位置 | 称在 _validate_package | 修正：在 `:836`/`:890`，由品类契约控制 |
| 改动1 对种草线影响 | 称"零影响" | 修正：种草线会吃到钩子加权(设计意图)，非零但可接受/可关闭 |

---

## 附：待实现时需再次核实的点
1. `_load_render_scoring_config` 的挂载点（render_plan_skill 当前有无读 config 的现成入口，若无则新增）。
2. `rds_repository_skill.upsert` 是否有字段白名单过滤 `payload_json`。
3. `llm_router_skill._normalize_segment_tag` 的真实行号（原方案 :704 需重新定位）。
4. 改动1 插入点的 `return score` 确认（`_segment_score:800` 末尾结构）。
