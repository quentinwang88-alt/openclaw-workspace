# 三阶段方案交付报告——教程断点/素材消费/VN围巾准入

日期：2026-09-21。基线 `39a9353` → `a158ece`(P1) → `a38deb1`(P2) → `1319e3f`(P3) → `9b57726` + wrap 修复（P1 真实跑测收口）。

## 一、实际修改与行为说明

### Phase 1 教程输出断点
1. **统一可见文字来源**（`copy_translation.py`）：`visible_page_text` 按渲染顺序提取 kicker→headline→body→色卡发布语言标签全量，废除 `headline or body`（正文被标题吞掉）。翻译 prompt、翻译缓存指纹、最终页 QA 三处共用同一投影（`pages_to_slide_texts`）。`label_zh` 是中文审计字段：并入指纹（改标签即重译中文）但**不进** slide 投影（Sarabun 不画中文、发布语言纯度校验会拦）。无 pages 的旧任务投影返回 None，走原 slide_texts。
2. **温度路由真实链路**（`photo_content_planner.resolve_guide_execution`）：同时识别 `theme_key=TEMPERATURE` 与 `travel_theme_type=TEMPERATURE`——温度穿搭经 `resolve_photo_theme` 落在 `COOL_WEATHER_TRAVEL+TEMPERATURE`，旧判断漏掉真实形态。真实链路测试 `resolve_photo_theme→build_travel_topic→resolve_guide_execution` 钉住；guide_place_rule 同步修正（有地点即要求 place_localized，样片 recvvPdTajO5T7 失败根因）。
3. **色卡标签**（vision + layout）：规划合同输出 `label`（当地语言，画在色卡旁）与 `label_zh`（中文审计）分离；缺明确单品名给中性类别词。渲染优先 `label`，缺 label 回退中性类别词表（accessories 不再一律「围巾」）。

### Phase 2 素材选择与消费
1. **主题策略表补全**（`material_adapter.py`）：配色教程/旅行·穿搭攻略/旅行·环境协调三条策略（prefer_structures/needs/prefer/fallback），只影响初筛软排序与终选 prompt 文案。
2. **主题关键词软排序**：`THEME_RANK_KEYWORDS`（权重 0.75–1.0，低于近期降权等主信号），命中分析缓存的标题/选题/搭配关系文字加分。离线回放五主题排序体现用途差异（配色教程首选命中「配色」、拍照穿搭首选命中「出片」、攻略首选 same_item_multiway）。
3. **中文单品名标准化匹配**：`CATEGORY_ITEM_ALIASES`（outerwear→外套/大衣/羽绒…，scarf→围巾/丝巾…）+ `category_matches_core_items`，替换旧 `category in core_text`（中文素材极少写英文类目名）。
4. **采用页真实消费**（`auto_photo_supply.py`）：内容要求的逐页讲解改读**终选实际选中页**（selection.pages），不再固定取前四页；指定商品时 reference_basis 不再因摘要含「外套」整页删除（商品身份由商品参考图权威保障）。
5. **去重/补库**（既有机制确认）：recent_note_ids 降权、record_demand 缺口台账、方法词（公式/拆解/教程）已入配色教程/攻略策略——本轮验证无需新代码。

### Phase 3 VN 围巾
1. **跨市场回退消除**（`auto_photo_supply.py`）：参考推导预设按账号市场校验——VN 账号配 TH 预设＝配置错误暂停记录（preset_market_mismatch）；非 TH 市场推导非旅行结构无匹配预设＝暂停记录。绝不把 VN 围巾变成 TH 女装。
2. **两种围巾模式语义统一**：QA 上下文显式保留 task_category_key（复审 F5 已修），本轮补链式断言——`review_travel_pages` 的 has_product 与 `review_alignment` 的 product_specified 都按 product_id 判定，自由围巾走 required_item_visible 存在观察，指定围巾走商品身份逐项检查。
3. **准入核验（只读）**：8 个 VN 账号全部「暂停」；仅越南配饰1 有 CreatOK 连接（provider_health=error、UID 空），其余 GeeLark 无原生图文通道；VN 围巾两预设仍 disabled（越语文案未母语审校）。**发布侧前置未满足，保持 disabled，未擅自启用。**

## 二、离线测试

`python3 -m unittest discover -s tests`：**1581 全过**。本轮新增/修改：真实链路温度路由、F1 证据投影（normalize→真实 profile→plan→供给侧同源）、自由围巾 QA 上下文捕获、可见文字投影、侧栏 wrap 像素级验证。覆盖边界：模型/飞书用假响应；真实渲染由两条生产样片验证。

## 三、真实样片（方案要求四条）

| 样片 | 记录 | 结果 | 所用参考页 |
| --- | --- | --- | --- |
| TH 配色教程 | **recvvPENTdt4Ss** | ✅ 已完成，QA 4 页全过无裁切 | XHS 6a5b8dfb p2/p4（舒服系配色） |
| TH 温度指南 | **recvvPjTwVsQBs** | ✅ 已完成（首尔，逐页答穿脱条件） | 商品参考图（商品锚定） |
| VN 自由围巾旅行 | — | ⛔ 未生成：VN 预设 disabled＋发布通道未通 | 代码链路 45 项单测钉住 |
| VN 指定商品围巾 | — | ⛔ 同上 | 同上 |

修复过程（真实跑测暴露并当轮修复）：
- 样片 recvvPdTajO5T7 失败 → 温度 guide_place_rule 与地点校验自相矛盾（已修）。
- 样片 recvvPdIqkOoWG 失败 → label_zh 污染 slide 投影被 CJK 校验拦截（已修：审计字段只进指纹）。
- 样片 recvvPws6zcH2l QA 拦截 4 页裁切 → `wrap_text` 两处缺陷（行中长 token 不细断；textlength advance 与 bbox 墨迹宽不一致）。修正后本地重演冻结数据右缘全部 ≤1048 安全线，重跑 recvvPENTdt4Ss QA 全过。

## 四、尚未完成的生产准入项

1. VN 发布侧：CreatOK 连接修复/原生图文能力确认/越语文案母语审校/预设启用——均为运营动作，完成后补 VN 两条真实样片。
2. tocrystal66（库存 20）与 wn0didnad6（库存 30）待库存消化到 12 以下自动恢复供给，或人工调高目标库存。

## 五、开关、回退与对旧任务的影响

- 新投影/新标签只对**带 pages 的新冻结任务**生效；旧任务（无 pages）投影返回 None，原 slide_texts 路径逐字不变（有断言钉住）。
- 跨市场暂停只影响「自动化模式≠off 且预设与市场不符」的账号；TH 5 账号现有生产不受影响。
- 回退方式：revert 对应提交即可；已冻结批次按原快照续跑，不受代码回退影响（快照内嵌 layout/copy 全量）。
- 每日 8:30 自动供稿 crontab 已建（`/tmp/opv_auto_supply.log`）；删除该 crontab 行即关闭定时。
