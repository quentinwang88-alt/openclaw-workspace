# 脚本总库首帧串联实测（2026-09-03）

## 结果

已按用户勾选完成 3 张首帧及 3 条短视频运行任务。执行使用现有白名单 sync 入口，非手工代写结果。首帧准备及交接约 3 分钟，失败 0。
源表均为“首帧已就绪 / 已送生产”，进入生产勾选已按成功流程取消；生成首帧勾选保留。目标记录均为“待处理 / 养号 / 不挂车 / 10秒”。未改发布排期，未在本次执行中提交视频生成或发布。

| 产品 | 源记录 | 运行记录 |
|---|---|---|
| S260724029604 | recvu7OUUkJvBL | recvu85O6A2qGA |
| S260724029604 | recvu7OWYDZTgZ | recvu862ERdOeM |
| S260724014590 | recvu7P0lRR9dq | recvu86gAO84RY |

每条运行任务携带 7 张图：1 张首帧、4 张人物参考、2 张商品参考；角色序号 1–7 与附件数量一致。目标执行说明覆盖历史图片编号，源正文哈希与执行前完全一致，运行表仍包含完整源正文。

## 调度与 API

原任务 original-production-script-sync-every-6h（72f74158-1cc5-4a12-b441-3d0f9e0e7ddf）保留每六小时频次、命令和通知配置，只将单轮 timeoutSeconds 从 600 调为 19800，以容纳最多 20 条首帧任务。没有新增轮询任务。
首帧使用同步器已经读取的单记录快照，子进程不调用 list_records/get_record。原有长视频兼容入口的读取保留。运行期间用本地 SQLite 查进度，最终只定点读取源/目标各 3 条验收。
同步器本轮记录接口计数 source=16 / target=17 / total=33；此计数不代表包含首帧子进程、认证、wiki解析与字段查询的全部飞书 API 总数。

## 代码与验证

- original-script-generator/core/wsr_first_frame.py：从该行最终视频提示词编译静态 t=0 首帧，不重写故事/人物/口播；支持无商品养号。
- original-script-generator/scripts/run_first_frame_tasks.py：单记录快照协议、共享记录/资产锁、图片和上传 token 持久化恢复；修复阶段0同槽位身份校验及 dry-run 建库问题。
- script-run-manager-sync/core/first_frame_handoff.py 与 run_pipeline.py：同轮首帧→同步、已就绪复用、失败保留勾选、首帧+人物+商品角色交接。
- 98 项同步回归、40 项首帧/长视频兼容回归通过；另有 7 项独立隔离验证通过。
- 两份 SKILL.md 已更新并通过验证；未改其他历史脚本内容或旧复刻产物。

## 本地首帧

- wsr_prompt_e5e9594a5ee0babff0e8b87c: /Users/likeu3/.openclaw/shared/data/original_first_frames/3a1e4b6a1bb3b9c4017e06c05ebbdbccb757f2a3/FFA_3A1E4B6A1BB3B9C4017E_1.png
- wsr_prompt_2b47fc19e589edb6c176f581: /Users/likeu3/.openclaw/shared/data/original_first_frames/2d814ce7b132008de5700186d3a5029c816d46c2/FFA_2D814CE7B132008DE570_1.png
- wsr_prompt_05ab61a9a855030a55596a45: /Users/likeu3/.openclaw/shared/data/original_first_frames/72d437f7fa2a83f9cb568a827da1c761aa5ccf10/FFA_72D437F7FA2A83F9CB56_1.png

## 回滚材料

本轮源码及两个 SQLite 一致性备份、原调度配置、验收 JSON：/private/tmp/script-pool-first-frame.kIiZFF。仅为回滚材料，未执行恢复。

