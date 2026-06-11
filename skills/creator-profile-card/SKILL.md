---
name: creator-profile-card
description: |
  达人关系运营闭环 V1.1。基于达人主页截图（头部 + 最新 20 个视频封面拼图），
  由 AI 分析生成画像卡并写入飞书业务表。
  V1.1 整合内容机会卡 + 三种消息目的 + 周度巡检 + 关系状态流转，
  实现"画像打标→分层→周度巡检→判断动作→生成话术→人工审核→发送→记录回复→更新状态"完整闭环。
  适用于盘活店铺历史达人资产、提高建联回复率的场景。
---

# Creator Profile Card V1.1 — 达人关系运营闭环

达人画像卡 + 关系运营 + 周度巡检 + 三类话术生成。

## V1.1 核心改进

从"单次话术生成"升级为**完整的达人关系运营闭环**：

```text
达人画像打标
  ↓
达人分层（A/B/C/D）
  ↓
周度达人池巡检
  ↓
判断本周动作（关系维护/商品邀约/轻跟进/暂缓/放弃/人工查看）
  ↓
生成对应话术（三种 message_purpose 分流）
  ↓
人工审核发送
  ↓
记录达人回复 → 更新关系阶段和下次可联系时间
  ↓
下周继续巡检
```

**三大原则：**
- 不是每周群发，而是每周巡检——只筛出本周值得触达、冷却期已过的达人
- 关系维护和商品邀约必须分开——维护不能推品，邀约必须给内容机会
- AI 只建议，人工最终确认——发送、寄样、选品、放弃都必须人工确认

## 飞书表结构（23 字段）

### 画像字段（9 个，V1.0 保留）

| 字段 | 类型 | 说明 |
|------|------|------|
| 达人链接 | 链接 | TikTok 主页 |
| 历史关系 | 单选 | 出过单/发过视频/申请过样品/聊过未合作/陌生 |
| 活跃度 | 单选 | 高/中/低/停更 |
| 内容类型 | 单选 | 穿搭/妆发/首饰试戴/好物分享/居家生活/口播种草/直播切片/其他 |
| 画面风格 | 单选 | 自拍近景/镜前半身/全身穿搭/桌面展示/家中生活流/户外街拍/直播间感 |
| 适配类目 | 多选(≤2) | 发饰/耳饰/项链/围巾/帽子/轻上装/女装/暂无 |
| 推荐商品/品类 | 文本 | AI 建议 |
| 沟通切入点 | 文本 | 一句中文核心沟通理由 |
| 当前动作 | 单选 | 精准沟通/半自动沟通/暂缓/放弃 |

### 关系运营字段（14 个，V1.1 新增）

| 字段 | 类型 | 说明 |
|------|------|------|
| 达人层级 | 单选 | A/B/C/D 类，决定维护优先级和频率 |
| 关系阶段 | 单选 | 冷/温/热/合作中/冷却/放弃 |
| 上次联系时间 | 日期 | 冷却期计算基础 |
| 上次联系类型 | 单选 | 关系维护/商品邀约/轻跟进/人工消息/无 |
| 上次回复时间 | 日期 | 判断关系温度 |
| 最新回复状态 | 单选 | 未回复/已读未回/普通回复/感兴趣/拒绝/无效回复 |
| 连续未回复次数 | 数字 | 控制降频和放弃 |
| 下次可联系时间 | 日期 | 冷却机制核心 |
| 本周建议动作 | 单选 | 关系维护/商品邀约/轻跟进/暂缓/放弃/人工查看 |
| 本周建议原因 | 文本 | 运营参考 |
| 处理状态 | 单选 | 待处理/已发送/已暂缓/已放弃/需人工查看/已回复 |
| 本次话术草稿 | 长文本 | 运营审核和发送 |
| 选定商品 | 文本 | 商品邀约时必须 |
| 发送结果 | 单选 | 未发送/已发送/修改后发送/不发送 |

## 三种消息目的

### 1. 关系维护 (relationship_maintenance)

非带货、不推品、不索取、不逼回复。用于老达人恢复关系温度。

话术结构：轻关系开场 → 公开内容观察 → 表达后续精准匹配 → 低压力结尾。

评分维度（满分 10 分）：真实内容观察 2 + 无带货目的感 2 + 关系修复自然 2 + 表达后续精准匹配 1.5 + 低压力结尾 1 + 无监控感/无尬夸 1 + 语气自然 0.5。

### 2. 商品邀约 (product_invitation)

必须先生成内容机会卡作为隐藏中间层。话术包含四要素：具体观察 + 具体商品/品类 + 低成本拍摄场景 + 低压力 CTA。

评分维度（满分 10 分）：具体画面观察 2 + 商品具体度 2 + 拍摄场景 2 + 达人收益感 1 + 低压力 CTA 1 + 非模板化 1 + 风险控制 1。

### 3. 轻跟进 (follow_up)

上次商品邀约 3-7 天未回复，轻提醒不催促不施压，给拒绝空间。

风险检查：施压表达、催促表达、追加利益施压、监控感。

## 使用方式

### 初始化飞书表字段

```bash
python3 skills/creator-profile-card/scripts/add_relationship_fields.py
```

### 生成达人画像卡

```bash
python3 skills/creator-profile-card/scripts/generate_profile.py \
  --creator-url "https://www.tiktok.com/@xxx" \
  --market "VN" \
  --history-relation "发过视频" \
  --cover-collage "cover_01_08.jpg" \
  --cover-collage "cover_09_16.jpg" \
  --cover-collage "cover_17_20.jpg" \
  --feishu-table-url "https://gcngopvfvo0q.feishu.cn/base/XXX?table=tblXXX"
```

### 生成私信话术（支持 --message-purpose）

```bash
# 商品邀约
python3 skills/creator-profile-card/scripts/generate_message.py \
  --creator-url "https://www.tiktok.com/@xxx" \
  --market TH --target-language 泰语 \
  --history-relation 发过视频 \
  --product-name "宽松轻薄开衫" --product-category 轻上装 \
  --product-json "product_info.json" \
  --profile-json "profile_result.json" \
  --cover-collage cover_01_08.jpg --cover-collage cover_09_16.jpg

# 关系维护
python3 scripts/generate_message.py \
  --creator-url "https://www.tiktok.com/@xxx" \
  --market TH --target-language 泰语 \
  --history-relation 出过单 \
  --message-purpose relationship_maintenance

# 轻跟进
python3 scripts/generate_message.py \
  --creator-url "https://www.tiktok.com/@xxx" \
  --market TH --target-language 泰语 \
  --message-purpose follow_up
```

### 周度巡检

```bash
# 仅输出（不回写）
python3 skills/creator-profile-card/scripts/weekly_scan.py --dry-run

# 实际执行（回写本周建议动作 + 原因）
python3 skills/creator-profile-card/scripts/weekly_scan.py

# 执行并生成话术草稿
python3 skills/creator-profile-card/scripts/weekly_scan.py --generate-messages
```

## 配置

优先读环境变量，回退到项目默认值：

- `CREATOR_PROFILE_LLM_API_URL` → LLM API 地址
- `CREATOR_PROFILE_LLM_MODEL` → 模型名（默认 `gpt-5.5`，也支持 `Doubao-Seed-2.0-pro`）
- `CREATOR_PROFILE_LLM_API_KEY` → API Key
- `CREATOR_PROFILE_LOG_DB_PATH` → 日志数据库路径

飞书凭证优先读 `~/.openclaw/openclaw.json` 中的 `channels.feishu`。

## 目录结构

```
skills/creator-profile-card/
├── SKILL.md
├── app/
│   ├── config.py
│   ├── prompts/
│   │   ├── __init__.py
│   │   ├── profile_analysis.py              # V1.0 画像分析 Prompt
│   │   ├── content_opportunity.py           # V1.1 内容机会卡 Prompt
│   │   ├── message_generation.py            # V1.1 商品邀约 Prompt（四要素）
│   │   ├── relationship_maintenance.py      # V1.1 关系维护 Prompt
│   │   └── follow_up_message.py            # V1.1 轻跟进 Prompt
│   ├── services/
│   │   ├── llm_client.py                    # LLM 客户端（双模式）
│   │   ├── profile_generator.py            # 画像生成编排
│   │   ├── content_opportunity_generator.py # 内容机会卡生成
│   │   ├── message_generator.py            # 话术生成（三 purpose 分流）
│   │   ├── validator.py                    # 校验（画像 + 质量评分 + 风险检测）
│   │   ├── weekly_decision.py             # 周度动作决策引擎
│   │   └── feishu_writer.py               # 飞书读写
│   └── models/
│       └── __init__.py
├── scripts/
│   ├── add_relationship_fields.py           # 新增 14 个关系运营字段
│   ├── generate_profile.py                  # 生成画像卡 CLI
│   ├── generate_message.py                  # 生成话术 CLI
│   ├── weekly_scan.py                       # 周度巡检 CLI
│   ├── batch_drafts.py                      # 批量话术生成
│   └── run_v11_test.py                      # V1.1 试跑脚本
└── requirements.txt
```

## 操作规则

### 画像校验（V1.0 保留）
- 枚举校验、置信度阈值（≥0.75/0.55-0.74/<0.55）
- 内容类型和画面风格至少引用 3 个封面编号
- 封面 <12 张最高置信度限 0.75，<8 张强制人工复核

### 话术质量（V1.1）
- 商品邀约 <8 分重写，最多 2 次
- 关系维护 <8 分重写，最多 2 次
- 轻跟进通过风险检查（无施压、无催促、无追加利益、无监控感）

### 风险检测（全类型）
- 监控感：我翻了你很多视频、我一直关注你
- 过度承诺：保证出单、一定会火、最高佣金
- 模板句：想和你沟通合作、想邀请你推广
- 关系维护额外：检测是否出现商品词（发饰、开衫、佣金等）

### 冷却机制
- 关系维护后：A 类 21 天 / B 类 35 天 / C 类 60 天
- 商品邀约后：A/B 类 7 天 / C 类 30 天
- 轻跟进后：A 类 30 天，B/C 类不跟进
- 发送后自动回写：上次联系时间、上次联系类型、下次可联系时间、关系阶段=冷却

### 排除规则（不进入本周触达池）
- 关系阶段 = 合作中/放弃
- 达人层级 = D 类
- 活跃度 = 停更
- 下次可联系时间 > 今天
- 连续未回复次数 ≥ 4

## 飞书视图

| 视图 | 筛选条件 | 用途 |
|------|---------|------|
| 本周待处理达人 | 处理状态=待处理, 动作≠暂缓 | 运营每日工作 |
| A 类重点达人 | 层级=A 类, 阶段≠放弃 | 重点维护 |
| 冷却中达人 | 关系阶段=冷却 | 防止重复打扰 |
