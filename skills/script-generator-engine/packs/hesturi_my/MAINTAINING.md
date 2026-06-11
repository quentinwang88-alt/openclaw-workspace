# 内头巾配置包 · 维护手册（怎么更新卖点和钩子）

> 目标：让你随时能更新卖点、钩子、场景、真实感词库，**改 YAML 即可，不用动代码**。
> 所有配置都在本目录 `packs/hesturi_my/` 下，纯 YAML，改完引擎自动读取。

---

## 一、最常见操作

### 1. 给已有钩子加/改"证明角度"（最常用）
打开 `pain_hooks.yaml` → 找到对应钩子 → 在 `proof_angles:` 下加一行即可。
> 例：给"不勒"钩子 `H_NO_MARK` 加一个新证法：
```yaml
H_NO_MARK:
  proof_angles:
    - "勒痕角度:戴一整天后摘下,额头无勒痕特写"
    - "余量角度:手指伸进边缘还有空间"
    - "新增:摘下后立刻量体温/碰额头不痛的反应"   # ← 直接加
```
加完，S1-S4 轮换时会自动把新角度纳入。

### 2. 新增一个全新痛点钩子
要动 **2 处**（就这两处）：
1. `pain_hooks.yaml` → `hooks:` 下加一个新钩子块（照现有格式：voc_rank/pain_desc/proof_angles/visual_brief/voice_intent/ai_shot_risk）。
2. `pain_hooks.yaml` → `selling_point_routing.rules:` 下加一条关键词路由，指向新钩子。
> 不用改任何代码。

### 3. 调整"卖点 → 钩子"的匹配关键词
打开 `pain_hooks.yaml` → `selling_point_routing.rules:` → 给对应钩子的 `keywords:` 增删词（支持中文/马来语/英文片段）。
> 例：让"轻便"也路由到透气钩子：
```yaml
- keywords: ["透气","棉质","凉爽","不闷","不薄","cotton","sejuk","breathable","轻便"]
  hook: H_BREATHABLE
```

### 4. 加/改场景
打开 `scene_pool.yaml` → `scenes:` 下加场景块（label/authenticity_boost/good_for_hooks/proof_friendly/visual_brief）。

### 5. 调真实感粗糙度 / 词库
打开 `raw_texture_lexicon.yaml`：
- 改某维度的真实感词 → 改 `dimensions.<维度>.positive / negative`。
- 改某档位的强度 → 改 `authenticity_levels.<档>`（full/mild/off）。
- 换默认档 → 改 `default_level`（如以后高客单品改 light）。

### 6. 加/改出镜人物画像
打开 `performance_profiles.yaml` → `personas:` 下增删。

---

## 二、改完后怎么验证（一条命令）

```bash
cd ~/.openclaw/workspace/skills/script-generator-engine
python3 engine/category_router.py
```
会打印钩子数/场景数/档位，确认你的修改被正确加载、YAML 没写坏。

---

## 三、文件速查表

| 你想改什么 | 改这个文件 | 改哪个字段 |
|---|---|---|
| 钩子的证明角度 | `pain_hooks.yaml` | `hooks.<钩子>.proof_angles` |
| 新增钩子 | `pain_hooks.yaml` | `hooks` + `selling_point_routing.rules`（2处）|
| 卖点匹配词 | `pain_hooks.yaml` | `selling_point_routing.rules.keywords` |
| 画面/口播指令 | `pain_hooks.yaml` | `hooks.<钩子>.visual_brief / voice_intent` |
| 场景 | `scene_pool.yaml` | `scenes` |
| 真实感词库 | `raw_texture_lexicon.yaml` | `dimensions` |
| 真实感档位/默认 | `raw_texture_lexicon.yaml` | `authenticity_levels / default_level` |
| 出镜人物 | `performance_profiles.yaml` | `personas` |
| prompt 措辞 | `prompt_overrides.yaml` | 对应注入块 |
| 默认参数(变体数/去重窗口) | `pack.yaml` | `defaults` |

---

## 四、未来扩展新类目（不属于日常维护，备查）

复制整个 `hesturi_my/` 目录 → 改名如 `earrings_vn/` → 改里面 YAML 内容
→ 在 `engine/category_router.py` 的 `CATEGORY_REGISTRY` 加一行注册。
引擎代码 0 改动，新类目即可跑通。
