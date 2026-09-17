# 下一轮修复 D 批验收报告（2026-09-18）

基线 `b055c77`（A/B/C 全量 + D 期间顺修 target_inventory 白名单 bug）。
方案：`docs/OPV_CONSUMPTION_FIX_EXECUTION_PLAN_20260917.md`。结论：**D 批通过**。

## 离线回归

全量 **1522 passed + 1 skipped + 290 subtests**（D 基线）；白名单修复后供给测试 35 绿。

## 三条真实终选小样（不 monkeypatch，Doubao 真选）

### D1 参考优先＋无主题＋配色素材（recvvtk8K2ApPL）✅

| 验收点 | 结果 |
|---|---|
| 配色主题产出 | ✅ topic_statement=「3种区别于大地色的秋日穿搭配色方向」；封面「用3种配色公式打造4套造型」 |
| 非默认旅行 | ✅ 预设切「图文｜TH｜四选一穿搭」（通用结构），主题=秋季穿搭（配色映射），主题来源=参考推导 |
| 非自动温度 | ✅ 温度档无/温度带无/文案零温度词 |
| 来源合规 | ✅ 5 成片 SHA 全异原图；参考 3 页（4/5/6）带页级用途 |

### D2 定位优先＋浅蓝外套一衣多穿（recvvtkPnS6cJI）✅

| 验收点 | 结果 |
|---|---|
| 商品一致 | ✅ 封面主外套判浅蓝短款夹克（d2_is_light_blue_jacket=true），单一主单品策略成立 |
| 配套实质变化 | ✅ 四选一「选裤装还是裙装」结构 |
| 文案不残留参考商品 | ✅ 参考原题「灰色连帽卫衣 8 套」→主张泛化「多套」（source_topic 保留原题），文案无灰色卫衣痕迹 |
| 温度 | ✅ 无温度声明（一衣多穿不带温度带） |
| 已知边界 | 预设仍旅行线（一衣多穿结构按 B2 设计保留原路径），封面 kicker「城市漫步之旅」带旅行味——与 T2-3 一致，非回归 |

### D3 明确日本旅行（recvvtkhmCYHD0）✅（含如实记录）

| 验收点 | 结果 |
|---|---|
| 目的地进链路 | ✅ 合同 destination=日本；行字段旅行国家=日本；requirement 含「场景按目的地重新规划」 |
| 画面无地点冲突 | ✅ Doubao 判定：日本传统町屋石板街场景，conflict=none |
| 温度带来源 | ✅ 15-22°C（账号默认主题预设公开含义，brief 记录来源） |
| 如实记录 | 真实终选自由选了「入秋日常穿搭」素材（未限 material_scope），选题与目的地弱关联——素材主张与账号主题的张力属真实终选的自然结果；后续可在 brief 目的地明确时给选材加目的地偏好（改进项非缺陷） |

## 交付物清单（§5 核对）

每行留存：effective_brief（合同列）、原素材（source_topic+主参考 note_id）、
采用计划（adoption+页级用途+sha256）、合同指纹、实际输入图片数（D1/D3 各 3、D2 3）、
最终图片（/tmp/dsamples+飞书附件）与中文文案。**生成 provider/model/request_id：未落盘，
明确记为未知**（当前只能从配置推断 1route/gpt-image-2.5-sunburst）——lineage 落盘仍列
后续项（与自动发布授权同批）。

## 过程发现

- **target_inventory 白名单 bug**（b055c77 修复）：supply_policy 属性按
  `if k in merged` 白名单过滤，default_supply_policy 缺 target_inventory 键导致
  表内/内存设置读出 None——D2 实测 4/8 仍 inventory_full 暴露。
- D2 首跑被库存门正确拦截（pos2 在制 4≥目标 4），A4 门在真实数据生效。

## 未实现/未实测（本方案口径）

- 自动发布策略授权：未实现（方案§1 边界，通过后单独接入）。
- 生成 lineage（provider/model/request_id）落盘：未实现，记未知。
- RDS 子项/revision 级库存去重：飞书行口径已够当前验收，列后续。
