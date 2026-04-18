# 产品类型统一处理开发方案（女装 / 轻上装 / 首饰 / 发饰）

## 1. 目标

建立一套可复用的“产品类型归一 + 图片识别仲裁 + 生成约束 + 结果质检”机制，覆盖当前主营品类：

- 女装
- 轻上装
- 首饰
- 发饰

重点解决：

1. 表格填写类型与 AI 视觉识别类型不一致时如何处理
2. 同一业务类型下的多种表述如何兼容，例如：手镯 / 细手圈 / 细手环
3. 让脚本生成阶段优先遵循业务类型，不被单张误导图片带偏
4. 建立可扩展的通用品类框架，后续新增品类时只需补配置，不必重写逻辑

---

## 2. 总体原则

### 2.1 原则一，业务字段优先于视觉字段

- 表格里的 `产品类型` / `业务类型` 是业务主信号
- AI 图片识别是辅助信号，不允许直接覆盖业务字段
- 当两者冲突时，必须进入“仲裁”流程，而不是直接采用视觉结果

### 2.2 原则二，先归一到“标准类型”，再做生成

不要直接把表格原文丢给生成模型。

必须先经过：

- 原始类型保留
- 标准类型归一
- 视觉类型识别
- 冲突等级判定
- 仲裁结果输出
- 基于仲裁结果生成 prompt

### 2.3 原则三，生成阶段必须有硬约束

对于高风险冲突，不能只靠“提醒模型注意”。

必须注入：

- 允许描述的佩戴部位 / 穿着部位
- 禁止出现的错误类型词
- 禁止出现的错误身体部位词
- 禁止出现的错误镜头语言

### 2.4 原则四，生成后仍要做二次质检

即使 prompt 已经约束，也必须对生成结果做：

- 规则检测
- 关键词越界检测
- 必要时的 LLM 二次校验

否则还是会出现“表格正确，成稿跑偏”的问题。

---

## 3. 统一数据模型

建议在现有流程里增加以下标准字段。

```json
{
  "raw_product_type": "细手圈",
  "business_category": "首饰",
  "canonical_family": "jewelry",
  "canonical_slot": "wrist",
  "canonical_type": "slim_bangle",
  "display_type": "细手圈",
  "vision_family": "jewelry",
  "vision_slot": "neck",
  "vision_type": "choker",
  "vision_confidence": 0.78,
  "conflict_level": "high",
  "resolution_policy": "prefer_table",
  "resolved_family": "jewelry",
  "resolved_slot": "wrist",
  "resolved_type": "slim_bangle",
  "prompt_label": "细手圈（手腕佩戴的细款单圈腕饰）",
  "review_required": true
}
```

### 字段解释

- `raw_product_type`: 表格原始填写值，保留原文
- `business_category`: 业务大类，例如女装 / 首饰 / 发饰
- `canonical_family`: 标准一级族类
- `canonical_slot`: 标准身体部位 / 使用部位
- `canonical_type`: 标准二级类型
- `display_type`: 给运营和表格展示的友好名称，优先保留业务原词
- `vision_*`: AI 从图片识别出的结果
- `conflict_level`: 冲突等级
- `resolution_policy`: 仲裁策略
- `resolved_*`: 最终给脚本生成器使用的类型结果
- `prompt_label`: 给生成模型看的最终类型描述
- `review_required`: 是否需要人工复核

---

## 4. 一级类目统一定义

建议统一 4 个主营大类为：

```yaml
families:
  apparel:
    zh: 服饰
  jewelry:
    zh: 首饰
  hair_accessory:
    zh: 发饰
  unknown:
    zh: 未知
```

其中：

- 女装、轻上装 都归到 `apparel`
- 首饰 归到 `jewelry`
- 发饰 归到 `hair_accessory`

---

## 5. 二级类型标准体系

## 5.1 服饰 apparel

```yaml
apparel:
  womenwear:
    slot: body
    aliases: ["女装"]
  light_top:
    slot: upper_body
    aliases: ["轻上装", "上装", "轻薄上装"]
  top:
    slot: upper_body
    aliases: ["上衣", "T恤", "衬衫", "背心", "针织上衣"]
  outerwear:
    slot: upper_body
    aliases: ["外套", "开衫", "夹克"]
  dress:
    slot: full_body
    aliases: ["连衣裙", "裙装"]
  bottom:
    slot: lower_body
    aliases: ["下装", "裤子", "半裙", "短裙"]
```

### 服饰说明

- `女装` 更像业务大类，不建议直接当生成主类型
- 如果表格只填了 `女装`，可作为 `business_category`，但生成时应结合视觉补到 `top / dress / bottom / outerwear`
- `轻上装` 可以直接归一到 `light_top`

## 5.2 首饰 jewelry

```yaml
jewelry:
  necklace:
    slot: neck
    aliases: ["项链"]
  choker:
    slot: neck
    aliases: ["项圈", "颈圈", "choker"]
  bracelet:
    slot: wrist
    aliases: ["手链"]
  bangle:
    slot: wrist
    aliases: ["手镯", "手环"]
  slim_bangle:
    slot: wrist
    aliases: ["细手圈", "细手环", "细手镯", "细手圈手镯", "开口细手镯"]
  ring:
    slot: finger
    aliases: ["戒指"]
  earring:
    slot: ear
    aliases: ["耳饰", "耳环", "耳钉", "耳坠"]
```

### 首饰说明

- `手镯 / 手环 / 细手圈 / 细手环` 都属于 `wrist` 槽位
- 建议把 `细手圈 / 细手环` 统一映射到 `slim_bangle`
- 但 `display_type` 保留业务原词，不强制改写

## 5.3 发饰 hair_accessory

```yaml
hair_accessory:
  claw_clip:
    slot: hair
    aliases: ["抓夹", "发抓", "鲨鱼夹"]
  hair_clip:
    slot: hair
    aliases: ["发夹", "边夹", "顶夹"]
  headband:
    slot: hair
    aliases: ["发箍", "头箍"]
  scrunchie:
    slot: hair
    aliases: ["发圈", "大肠发圈"]
  hair_tie:
    slot: hair
    aliases: ["皮筋", "扎发绳"]
  ribbon:
    slot: hair
    aliases: ["发带", "蝴蝶结发饰", "丝带发饰"]
  hair_pin:
    slot: hair
    aliases: ["发簪", "发针"]
```

---

## 6. 表格类型归一规则

建议实现一个 `normalize_product_type()`。

### 输入

- `business_category`
- `raw_product_type`

### 输出

- `canonical_family`
- `canonical_slot`
- `canonical_type`
- `display_type`

### 核心逻辑

1. 先按 `business_category` 限定候选空间
2. 再在当前 family 内用别名字典匹配 `raw_product_type`
3. 匹配不到时，进入 family 内 fallback
4. 若 family 也为空，则进入全局匹配

### fallback 规则

#### 女装
- 如果只填 `女装`
  - `canonical_family = apparel`
  - `canonical_type = womenwear`
  - 允许视觉继续细分

#### 轻上装
- 统一到 `canonical_type = light_top`
- `slot = upper_body`

#### 首饰且填“细手圈 / 细手环”
- 统一到 `canonical_type = slim_bangle`
- `slot = wrist`
- `display_type` 保留原始填写值

#### 首饰只填“首饰”
- 归到 `canonical_family = jewelry`
- `canonical_type = unknown_jewelry`
- 允许视觉细分，但若细分结果置信度不足，不直接下结论

#### 发饰只填“发饰”
- 归到 `canonical_family = hair_accessory`
- `canonical_type = unknown_hair_accessory`

---

## 7. AI 视觉识别输出要求

不要只让视觉模型输出一个“产品类型”。

必须输出结构化证据：

```json
{
  "family": "jewelry",
  "slot": "neck",
  "type": "choker",
  "confidence": 0.78,
  "evidence": {
    "body_part": "neck",
    "shape": "rigid_single_loop",
    "closure": "open_gap",
    "scale_reference": "none",
    "in_use": false,
    "multi_image_consensus": false
  }
}
```

### 必须抽取的视觉证据

- `body_part`: 佩戴/展示部位，例如 wrist / neck / ear / hair / upper_body
- `shape`: 形态，例如 rigid_loop / soft_chain / fabric_top / claw_shape
- `closure`: 开口/扣头/链条等结构特征
- `scale_reference`: 是否有尺寸参照，例如 wrist / neck / human body / none
- `in_use`: 是否是上身/上手/上发图
- `multi_image_consensus`: 多图是否一致

### 关键要求

**多图聚合时，不同图片权重要不同。**

建议权重：

- 明确佩戴图 / 上身图 / 上手图: 1.0
- 半身或局部但有身体参照图: 0.8
- 单品白底图: 0.4
- 裁切严重、无尺度参照图: 0.2

这条是解决 031 这类问题的核心。

因为：
- 手腕佩戴图应当高权重支持 `wrist`
- 白底无尺度细金圆环图，只能低权重辅助，不应反向推翻业务字段

---

## 8. 冲突等级定义

## 8.1 低冲突 low

表格与 AI：

- family 相同
- slot 相同
- type 轻微不同

例如：
- 表格：细手圈
- AI：手镯

处理：
- 直接通过
- `resolved_type` 优先使用表格归一结果
- AI 仅提供细节补充

## 8.2 中冲突 medium

表格与 AI：

- family 相同
- slot 相同
- type 差异较大

例如：
- 表格：耳钉
- AI：耳坠
- 表格：轻上装
- AI：开衫

处理：
- 优先按表格类型生成
- 同时允许视觉补充版型 / 材质 / 开合方式
- 不强制人工审核
- 但后置质检要更严格

## 8.3 高冲突 high

表格与 AI：

- family 不同，或
- slot 不同

例如：
- 表格：手镯，AI：项圈
- 表格：发夹，AI：耳饰
- 表格：轻上装，AI：连衣裙

处理：
- 不允许直接采用 AI 结果
- `resolved_*` 优先使用表格归一结果
- 注入强负向约束重新生成
- 生成后做二次质检
- 仍冲突则 `review_required = true`

---

## 9. 仲裁策略

建议实现 `resolve_type_conflict()`。

### 伪代码

```python
def resolve_type_conflict(table_type, vision_type):
    if not table_type and vision_type:
        return use_vision_with_flag()

    if table_type.family == vision_type.family and table_type.slot == vision_type.slot:
        if table_type.type == vision_type.type:
            return accept(table_priority=True, review=False, conflict="none")
        else:
            return accept(table_priority=True, review=False, conflict="low_or_medium")

    return accept(
        table_priority=True,
        review=True,
        conflict="high",
        add_hard_constraints=True
    )
```

### 核心决策表

| 场景 | 处理策略 |
|---|---|
| 表格空，AI有结果 | 用 AI，标记 AI补全 |
| 表格和 AI 一致 | 直接通过 |
| 同 family 同 slot，不同 type | 以表格为主，AI补细节 |
| family 或 slot 冲突 | 以表格为主，进入高冲突流程 |

---

## 10. 生成阶段 prompt 契约

不要只给模型一个 `产品类型`。

必须给一组结构化控制项。

### 示例，细手圈

```json
{
  "resolved_family": "jewelry",
  "resolved_slot": "wrist",
  "resolved_type": "slim_bangle",
  "display_type": "细手圈",
  "prompt_label": "细手圈，属于手腕佩戴的细款单圈腕饰",
  "allowed_body_parts": ["wrist", "hand"],
  "forbidden_body_parts": ["neck", "collarbone", "ear", "hair"],
  "forbidden_terms": ["项圈", "颈圈", "choker", "贴颈", "锁骨", "脖子留白"],
  "required_terms": ["手腕", "腕部", "佩戴在手上"]
}
```

### 示例，轻上装

```json
{
  "resolved_family": "apparel",
  "resolved_slot": "upper_body",
  "resolved_type": "light_top",
  "display_type": "轻上装",
  "prompt_label": "轻上装，属于上半身穿着的轻薄上装",
  "allowed_body_parts": ["upper_body", "shoulder", "waist"],
  "forbidden_body_parts": ["wrist", "neck_as_jewelry", "hair"],
  "forbidden_terms": ["手镯", "项圈", "发夹"],
  "required_terms": ["上身", "穿着", "版型", "面料"]
}
```

---

## 11. 各大类的通用负向约束模板

## 11.1 女装 / 轻上装

### 必须强调
- 是穿在上半身 / 全身 / 下半身中的哪一类
- 版型
- 面料
- 穿着方式
- 轮廓和搭配逻辑

### 禁止错写
- 禁止首饰化描述
- 禁止发饰化描述

### 典型禁词
- 手镯
- 项圈
- 耳环
- 发夹

## 11.2 首饰

### 必须强调
- 佩戴部位
- 单圈 / 链条 / 开口 / 吊坠 / 宝石 / 耳垂等结构
- 是否有身体参照

### 禁止错写
- 禁止错误佩戴部位

### 首饰分槽位禁词

#### wrist 腕饰
禁词：
- 项圈
- 颈圈
- choker
- 锁骨
- 贴颈
- 脖子

#### neck 颈饰
禁词：
- 手腕佩戴
- 手镯
- 手环
- 戴在手上

#### ear 耳饰
禁词：
- 手腕
- 颈部
- 头发

## 11.3 发饰

### 必须强调
- 使用部位是头发 / 发顶 / 发尾 / 耳侧头发区
- 固定方式，例如夹、绑、箍、绕

### 禁止错写
- 禁止写成耳饰 / 项链 / 手镯

---

## 12. 后置质检机制

生成完脚本后，必须跑一层 `validate_generated_script()`。

### 12.1 规则质检

按 `resolved_slot` 检查违禁词。

#### 例，wrist
如果脚本中出现：
- 颈部
- 锁骨
- 项圈
- choker
- 贴颈

则直接判定失败。

#### 例，light_top
如果脚本中出现：
- 戴在手上
- 项圈
- 发夹

则失败。

### 12.2 LLM 质检

建议在规则质检后，增加一层轻量二次判断：

输入：
- resolved_type
- resolved_slot
- 原图摘要
- 脚本文本

输出：
- 是否与最终类型一致
- 是否存在错误佩戴部位
- 是否需要回炉重写

### 12.3 回炉策略

- 第一次失败：自动重写一次，注入更强负向约束
- 第二次失败：标记人工审核，不自动写回飞书

---

## 13. 针对“细手圈 / 细手环”的兼容方案

## 13.1 结论

当前建议视为：

- family: `jewelry`
- slot: `wrist`
- canonical_type: `slim_bangle`

### 为什么这样设计

因为这几个词在业务上都属于同一类腕饰，只是叫法不同：

- 手镯
- 手环
- 细手圈
- 细手环
- 开口细手镯

它们不应在流程里被拆成完全不同的品类。

### 兼容策略

- 生成控制层统一按 `wrist accessory` 管
- 文案展示层保留原业务词
- 视觉补充层可补充“单圈 / 开口 / 细款 / 硬挺 / 金属环”等特征
- 绝不允许 AI 把它转写成 `neck` 体系

### 推荐 prompt label

```text
细手圈，属于手腕佩戴的细款单圈腕饰，可描述开口结构、金属细环、腕部佩戴效果，但禁止写成项圈、颈圈或贴颈首饰。
```

---

## 14. 031 类问题的标准处理方式

### 输入
- 表格：手镯 / 细手圈 / 细手环
- 图片1：手腕佩戴图
- 图片2：白底细金圆环图，无尺度参照

### 正确流程

1. 表格归一到 `jewelry > wrist > slim_bangle`
2. 图片1 高权重支持 `wrist`
3. 图片2 低权重可能误判 `neck`
4. 多图聚合后，视觉冲突应被识别，但不应覆盖表格
5. 进入高冲突或中冲突仲裁
6. 生成时强制：
   - must: wrist / 手腕佩戴
   - forbid: neck / choker / collarbone / 贴颈
7. 成稿后跑质检
8. 若仍出现颈饰词，自动回炉或转人工

### 预期结果

不会再出现：
- 表格写手镯
- 脚本全部写项圈

---

## 15. 推荐实现模块拆分

建议拆成 5 个独立模块。

## 15.1 `type_normalizer.py`
负责：
- 表格原始类型归一
- family / slot / type 输出
- alias 配置匹配

## 15.2 `vision_classifier.py`
负责：
- 多图视觉识别
- 输出结构化视觉证据
- 图片级权重聚合

## 15.3 `type_resolver.py`
负责：
- 表格结果与视觉结果仲裁
- 输出 resolved_type 与 conflict_level
- 标记是否人工审核

## 15.4 `prompt_contract_builder.py`
负责：
- 根据 resolved_type 构造 prompt label
- 注入 allowed / forbidden body parts
- 注入 required / forbidden terms

## 15.5 `script_validator.py`
负责：
- 规则质检
- LLM 二次质检
- 回炉判断

---

## 16. 推荐配置结构

建议把规则尽量配置化。

```json
{
  "families": {
    "jewelry": {
      "types": {
        "slim_bangle": {
          "slot": "wrist",
          "aliases": ["细手圈", "细手环", "细手镯", "开口细手镯"],
          "forbidden_terms": ["项圈", "颈圈", "choker", "贴颈", "锁骨"],
          "required_terms": ["手腕", "腕部"]
        }
      }
    }
  }
}
```

这样后续新增：
- 半裙
- 发箍
- 耳夹
- 开衫

都只需要补配置。

---

## 17. 上线顺序建议

## Phase 1，先加最小可用版本

目标：先止血

上线内容：
- 类型归一
- 冲突判级
- 高冲突时优先信表格
- 首饰类佩戴部位禁词校验

优先处理：
- 手镯 / 手环 / 细手圈 / 细手环
- 项链 / 项圈
- 耳饰
- 发饰
- 轻上装

## Phase 2，加多图权重和结构化视觉证据

目标：提升准确率

上线内容：
- 图片级证据提取
- 多图聚合
- 佩戴图高权重
- 白底图低权重

## Phase 3，加生成后 LLM 质检和自动回炉

目标：提升兜底能力

上线内容：
- 二次判断
- 自动重写一次
- 人工复核拦截

---

## 18. 最终开发结论

这套方案的核心不是“让 AI 更聪明”，而是把流程改成：

1. **先有标准类型**
2. **再看视觉证据**
3. **冲突时优先信业务字段**
4. **生成时加硬约束**
5. **生成后再做质检**

对于你当前主营的：

- 女装
- 轻上装
- 首饰
- 发饰

这套结构已经够通用。

特别是：

- `细手圈`
- `细手环`
- `手镯`
- `手环`

可以统一纳入 `jewelry > wrist > slim_bangle / bangle` 体系，既保留业务叫法，又能稳定控制生成结果不跑去颈饰。

---

## 19. 建议下一步

直接按下面顺序开发：

1. 先做 `type_normalizer.py`
2. 再做 `type_resolver.py`
3. 把 `prompt_contract_builder.py` 接到原创脚本生成链路前
4. 最后加 `script_validator.py`

如果要继续，我下一版可以直接补：

- 配置文件 JSON 草案
- Python 伪代码 / 类结构
- 冲突仲裁函数签名
- 首饰 / 服饰 / 发饰的违禁词表
- 接到现有 pipeline 的插入点
