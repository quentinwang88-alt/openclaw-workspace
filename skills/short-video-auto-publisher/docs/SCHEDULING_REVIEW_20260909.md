# 排班系统 Review（2026-09-09 晚）

> 范围：本轮双通道改造 + 当晚排班故障的完整复盘。目的：区分「已根治 / 已止血 / 未处理」，给出优先级。

## 一、当晚故障完整复盘（因果链）

```
15:09 轮：release 核验瞬时环境错误（RepositoryError，未复现根因）
   → 8 个 OPV 槽位被「永久取消」（verification_error 不在可重试分类）
15:09 轮：opv 图文候选尚未入池（时间差，非故障）
   → CreatOK 槽位「暂未找到候选」
19:14 轮：wn0didnad6 未开养号开关 + organic 候选全被归为 nurture 类
   → organic 通道候选池被清空（结构性 bug）
20:29/20:33 我用 dry-run 调试（两次）
   → DryRunPublishAdapter 不支持 photo：6 槽位误取消 + 10 条候选进入失败冷却
20:35 轮：候选全在冷却 → 全部「暂未找到候选」
20:45 清除误伤冷却 + 重跑真实调度
   → ✅ 4 条 CreatOK 图文任务排上（393821/393824/394064/394068，均 scheduled）
```

**结论：链路本身是通的。当晚不顺 = 1 个真实 bug（养号过滤）+ 1 个瞬时故障（核验）+ 我的调试副作用（dry-run），三者叠加。**

## 二、已根治（有测试保护）

| # | 问题 | 修复 | 保护 |
|---|---|---|---|
| 1 | organic 通道候选被养号开关清空 | scope=organic 时跳过 nurture 过滤 | 322 测试全绿 |
| 2 | 存量槽位无通道标记（路由错配风险） | 槽位生成时按时间窗回填标记 | test_dual_channel |
| 3 | 连接 UID 需手工填写 | creator_username 自动发现 + 回填 | 已实测 wn0didnad6 等 5 账号 |
| 4 | 多账号 CreatOK 配置（时区/模式/UID 缺失） | 已批量补齐 + 对账全 ready | 对账实测 |
| 5 | 双通道槽位生成/绑定/路由 | 绑定表+分窗+通道标记+scope 过滤 | test_dual_channel 9 用例 |

## 三、已止血但未根治（诚实清单）

| # | 问题 | 现状 | 风险 | 建议优先级 |
|---|---|---|---|---|
| 1 | **瞬时错误永久取消槽位**：核验/环境类失败被归为不可重试 → 槽位直接取消，需人工恢复 | 手动恢复了 8 个 | 任何一次网络抖动都可能吞掉未来槽位，且无告警 | **高**：dependency/verification 类错误应挂起重排而非取消 |
| 2 | **DryRunPublishAdapter 不支持 photo**：dry-run 有真实副作用（取消槽位+失败计数） | 手动清理了误伤 | 任何人再用 dry-run 调试都会重演今晚 | **高**：加 photo 支持或让 dry-run 完全只读 |
| 3 | `schedule` 命令默认 dry-run 模式 | 今晚误用两次 | 低（我是唯一操作者）但易踩 | 中：默认 auto 或加警告横幅 |
| 4 | 核验瞬时 RepositoryError 的根因未定位 | 未复现（手动验证通过） | 可能复发 | 中：给核验子进程加 stderr 捕获落日志 |
| 5 | 双行同账号 `publish_channel` 单值列被后写行覆盖 | 已知无害（调度按槽位标记路由） | 仅当账号绑定含 GeeLark 时有路由歧义 | 低：GeeLark+双通道场景出现时再处理 |

## 四、未处理的已知事项

1. **表格脏数据**：一行「账号ID」列填了连接 UID（状态暂停，不产槽位）——待用户删除
2. **3 个新授权账号未配行**：vickieyi990 / kaycen79335 / ronaldrou245（CreatOK 已就绪，表格加行即用）
3. **越南配饰1**：配了 CreatOK 但无连接、状态暂停（不用则忽略）
4. **候选供给依赖并行管道**：organic 图文候选由 OPV photo 管道供给，`opv_publish_window` 触发器仍未建——供给断档时 organic 槽位会空转（安全降级，但产能受限）
5. **NeoBund 带货槽位今晚全空**：THFZ01 带货候选池没货（8 同步/79 跳过）——等飞书补脚本，非系统问题
6. **同分钟唯一键约束**：`UNIQUE(account_id, scheduled_for)` 下两通道若配置同一分钟，后一个槽位静默丢弃——当前各账号时间窗均错开，无实际影响

## 五、当晚净结果

- 4 个账号 × 各 1 条 TikTok 图文（photo mode）已提交：21:30 tamarawoo / 22:00 LikeU shop / 22:30 doristang / 22:30 louisatian
- 明早 `sync_results`（每小时自动）回查实际发布结果即可闭环
- 322 个单元测试全绿

## 六、建议的下一步顺序

1. 【高】错误分类改造（#三-1）+ DryRun photo 支持（#三-2）——这是"排班不顺"的两个结构性根因
2. 【高】明早验证 4 条任务实际发布 + 24h 数据回收路径确认
3. 【中】opv_publish_window 触发器——让 organic 候选供给自动化
4. 【低】表格清理（脏行/新账号行）

---

# 修复执行记录（同日晚，Review 后实施）

## 已完成（325 测试全绿，新增 3 个回归用例）

1. **【高1】提交前失败不再永久取消槽位**（`app/scheduler.py`）
   - `submission_not_sent=True` 的失败分两类：RuntimeError 类（环境/资产准备瞬时问题）→ 挂起等重试；ValueError 类（内容/合同确定性拒绝）→ 挂起换候选重排
   - 永久取消仅保留给"已真实提交后失败"类
   - 回归测试：`PreflightFailureClassificationTest`（复刻 2026-09-09 事故两个场景）

2. **【高2】DryRunPublishAdapter 支持统一发布接口**（`app/publishers.py`）
   - photo/organic 请求不再抛错，返回 dryrun- 假任务 ID → dry-run 调试零副作用
   - 回归测试：`DryRunUnifiedInterfaceTest`

3. **【中】schedule 命令 dry-run 警告**（`run_pipeline.py`）
   - 默认 dry-run 模式启动时打印横幅说明副作用与正确用法

## 修复过程中发现并同步根治的第三个 bug

**能力门控用了账号合并行的单值通道**（`account_can_publish_candidate` 调用处不传通道）：
双通道账号的 `publish_channel` 列随同步顺序翻转，photo 候选的门控此前依赖
"CreatOK 行恰好最后写入"才通过——同步顺序一变 organic 候选会被全量拒绝。
已改为按**槽位实际通道**（`publish_channel_used`）判定。这正是 Review 里
"低优先级 #5"的现实化，已提前根治。

## 测试调试中的经验（已踩坑）

photo 候选的 ready 查询有完整合同链：`opv-photo-release-v1` manifest 指纹自洽 +
script_text 冻结清单一致 + 幻灯片文件真实存在 + `account_can_publish_candidate`
按通道查 content_photo_capable。构造测试夹具必须满足全部条件。
