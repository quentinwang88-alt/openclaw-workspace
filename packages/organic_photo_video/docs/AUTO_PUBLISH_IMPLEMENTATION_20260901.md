# OPV 自动排班、NeoBund 发布与 BGM 审查

## 运营入口

当前主入口：

- 确认发布：新增复选框；只有成片为“已完成”且被勾选才授权排期。
- 审核：“排期发布”仅保留为历史任务兼容入口。
- 进度：新增“已排期 / 发布中 / 已发布 / 发布失败”。

生成完成不会自动发布；只有勾选“确认发布”，系统才获得发布授权。成功入队后复选框自动清空。

## 排班合同

- RDS `opv_publish_record` 是唯一事实源，新增 `planned_publish_at`、`submitted_at` 和到期索引。
- 账号仍使用 `daily_publish_window_local` 与 `daily_volume.min`。
- 泰国测试账号当前 5 个确定性档位：12:00、12:30、19:45、20:30、21:15（Asia/Bangkok）。
- RDS 保存 UTC；NeoBund 当前按北京时间解释 `scheduledReleaseTime`，提交前自动换算为 Asia/Shanghai 墙钟时间。
- 同账号同档位被占用时自动使用下一档；每天满额后进入下一天。

## 发布 worker

`scripts/run_publish_scheduler.py` 每次执行一个幂等 tick：

1. 找出进入发布前 120 分钟窗口的 ready 记录。
2. 重新读取对应国家和语言的 NeoBund 音乐池。
3. BGM 选择成功后 arm，并只提交一次 NeoBund 定时任务。
4. 已错过最小 15 分钟提交提前量的任务顺延，不提交过去时间。
5. 到发布时间后 5 分钟开始回查；结果不明确只继续回查，绝不重复提交。
6. 将状态和选中 BGM 同步回原飞书记录。

## BGM 审查与修正

保留评分结构：情绪 35% / 热度 30% / 时长适配 20% / 三天去重 15%，同歌三天最多使用 2 次。

本轮修正：

- 选歌从提前排期时推迟到发布前 120 分钟内，避免签名音乐 URL 过期。
- 统一主题词和音乐词的情绪分类，解决 `soft_acoustic`、`upbeat_pop`、`transformation_beat` 无法匹配的问题。
- 搜索池随主题变化：显高使用 hot+pop，咖啡约会使用 hot+lofi+romantic，一衣多穿使用 hot+dance。
- 定向搜索结果保留情绪相关性，但热度权重按 70% 折算，不冒充国家热榜名次。
- 静音图文视频默认音乐音量 70、原声音量 0；禁止本地 BGM 与平台 BGM 双轨叠加。
- 自动 worker 选歌失败时不降级为静音发布。

2026-09-01 真实只读审计：TH/th-TH 接口正常；显高 Top1 为 pop 池，咖啡约会 Top1 为 Relax (Lofi)，一衣多穿 Top1 为 dance 池。审计不会上传视频或创建发布任务。

## 当前启用状态

- RDS migration 005 已应用，readiness 通过。
- 飞书选项已正式补齐并通过二次幂等检查。
- 用户级 LaunchAgent 已启用：飞书扫描间隔 120 秒，发布调度间隔 300 秒。
- worker 只处理带明确 human publish gate 的 RDS 发布记录；无授权记录不会提交。
