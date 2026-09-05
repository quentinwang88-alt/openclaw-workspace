# 穿搭拆解首图模板 V2

## 生产合同

- 配方：`RECIPE_OUTFIT_BREAKDOWN_V1`
- 主题：`THEME_TH_OUTFIT_BREAKDOWN_V1`
- 飞书预设：`TH｜穿搭拆解首图｜均衡变体`
- 图组仍为 5 张，P1 是首图，P2 是内部生成锚点。
- P2–P5 复用现有 Persona、Look、Scene 解析与图片生成链路；全部使用同一个
  `FINAL` Outfit State。
- P1 使用 `LAYOUT_OUTFIT_BREAKDOWN_V2` 合成 1080×1920 自由拼贴板：左/右侧
  是完整穿搭人物，另一侧独立展示目标商品、内搭和下装。
- 人物抠图直接取 P2 锚点；目标商品以商品参考图为视觉事实；内搭和下装优先
  使用当前 Look 的 `item_refs`。系统把参考图转换为纯绿幕单品图并抠图，不在
  P1 展示空卡片，也不伪造价格、店铺或订单信息。
- 四类素材写入 `plan_json.decomposition_assets`，包含原始路径、来源类型、哈希
  和透明度/主体占比质检结果。四类素材未全部通过时，P1 明确失败。

## Look 数据兼容

既有 Look 的文字配方无需迁移，仍是穿搭语义来源。`item_refs` 存在时优先用于
锁定真实单品；缺失时，根据 P2 已冻结穿搭还原对应单品，不再显示占位卡。
可在 Look 的 `outfit_recipe` 中增量增加：

```json
{
  "top_inner": "灰色连帽针织衫",
  "bottom": "黑色短裙",
  "footwear": "黑色厚底鞋",
  "item_refs": {
    "top_inner": {
      "local_path": "/absolute/path/hoodie.png",
      "label_i18n": {"th-TH": "เสื้อฮู้ดสีเทา"}
    },
    "bottom": {
      "local_path": "/absolute/path/skirt.png",
      "label_i18n": {"th-TH": "กระโปรงสั้นสีดำ"}
    }
  }
}
```

路径应是运营确认的真实单品图。系统会先把参考图转成独立绿幕单品图，再做
透明化和硬质检；不会从购物截图抄写价格、商家或订单信息。

## 变体策略

`BALANCED_4_V1` 以飞书记录 ID 和批次序号稳定选择变量，失败重试保持不变：

- Layout：`LEFT_HERO`、`RIGHT_HERO`、`CENTER_HERO`、`BOTTOM_GRID`
- Copy：穿搭公式、收藏这套、今日穿搭、三件成套
- Palette：暖白、冷白
- 内容层仍沿用当前批量差异化模块轮换 Persona、Look、Scene、Hook 和镜头语法。

视频渲染对 P1 使用 `contain`，防止清单和单品被轻动态裁切；P2–P5 保持
`cover`。单独重做 P1 只重合成首图；重做 P2 会自动重做 P1–P5。
