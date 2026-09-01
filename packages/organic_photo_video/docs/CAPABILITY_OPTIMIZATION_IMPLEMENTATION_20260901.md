# 图文钩子能力：审查优化实施说明

> 日期：2026-09-01
>
> 基线：`CAPABILITY_UPGRADE_SUMMARY_20260831.md`
> 范围：修复整体架构问题并补测试；本轮未连接生产 RDS、未调用模型、未生成或发布内容。

## 一、这次解决的核心问题

1. **配方变化与穿搭一致性冲突**：新增结构化 `outfit_states`（BASE / FINAL / ALT_1），每个分镜通过 `outfit_state_ref` 引用状态。产品与人物始终锁定，只允许配方声明的互补单品变化。
2. **Hook 配置没有真正参与执行**：支持显式 hook 和按 `variant_index` 稳定轮换；同一任务重跑保持幂等，并把最终 hook 写入任务、分镜与内容包追溯信息。
3. **Render / Quality Profile 只配置未执行**：规划时解析并冻结 profile 版本；动效、转场、帧率使用冻结合同，Profile 缺失或停用时立即失败。
4. **人物与账号选择存在隐式首条命中**：账号存在多个候选时必须显式传 `account_id`；人物使用实际 persona 快照，避免退回账号通用描述。
5. **Anchor 与 P1 混用**：首帧生产和单图重生统一遵循 Recipe 的 `anchor_slot`；视觉变化配方可先生成 P5。P1 仍作为发布封面，不与连续性锚点混为一谈。
6. **Content Package 可能记录旧 shot 对象**：生产完成后重新读取最新 shot 版本，再按 P1-P5 顺序写入内容包。
7. **组图审核多步写入不原子**：RDS 增加领域事务，一次提交 shot 选择、反馈、QA、内容包 ready 和任务 rendering；任一步失败全部回滚。
8. **五维 QA 被平均分掩盖**：新任务按 Quality Profile 的维度阈值独立判定，并返回失败后的 `next_actions`；旧任务保留兼容逻辑。
9. **迁移台账 hash 过粗**：新迁移按单文件 SHA-256 校验；历史 001/002 兼容旧 bundle 台账，不把新文件误判为已执行。

## 二、数据结构变更

- `opv_content_shot.outfit_state_ref VARCHAR(32) NULL`
- `opv_content_package.task_id` 增加唯一索引，保证一个任务只有一个内容包
- 迁移文件：
  - `migrations/003_enforce_package_and_outfit_state.sql`
  - `migrations/004_unique_content_package_per_task.sql`

注意：代码已使用 `outfit_state_ref`，因此 **003 必须先于新代码部署**。004 会在历史数据存在重复 `task_id` 时主动失败，不自动删除或合并数据。

上线前只读检查：

```sql
SELECT task_id, COUNT(*) AS package_count
FROM opv_content_package
GROUP BY task_id
HAVING COUNT(*) > 1;
```

查询结果为空后，才可应用 004。

## 三、执行合同

### 规划阶段

- 验证 Recipe × Theme 的适配关系。
- 解析显式或轮换得到的 Hook。
- 冻结 Recipe、Render Profile、Quality Profile 的 ID 与版本。
- 生成 Product Facts、Outfit Plan、Outfit States 和五个分镜引用。

### 图片阶段

- 先生成配方锚点；其余分镜只继承人物、产品与光线等连续性信息。
- 不跨状态复制整套穿搭；只能按 `outfit_state_ref` 执行受控变化。
- 单张重生仍引用配方锚点，而不是硬编码 P1。

### 审核与内容包阶段

- 单图与组图按 Quality Profile 的独立维度阈值判断。
- 人工通过后，RDS 在同一事务内完成全部状态推进。
- 内容包保存 Recipe / Profile / Persona / Look / Product / Outfit 追溯链。

## 四、验证结果

- `python3 -m compileall`：通过
- `python3 -m unittest discover -s tests -v`：**185 / 185 通过**
- 迁移 dry-run：**4 个文件、19 条语句**，未写入数据库
- 根目录测试污染文件 `-`：已清除，并确认不再生成

覆盖重点包括：Hook 轮换、Recipe × Theme 拒绝、账号歧义、Profile 快照、Outfit State、P5 锚点生成与重生、内容包最新 shot ID、独立维度 QA、审核事务提交与回滚、迁移单文件 hash。

## 五、尚未包含的工作

- 未应用生产 RDS 迁移或种子。
- 未做真实模型/RDS/视频渲染回归。
- 未补墨西哥、越南的独立穿搭规则包；当前仍只有泰国规则基线。
- 未增加程序化封面文字渲染，也未修改发布流程。

## 六、建议上线顺序

1. 备份并只读检查内容包重复数据。
2. 应用 003；处理重复数据后应用 004。
3. 部署代码与配置种子。
4. 用一个测试产品各跑一条：痛点、场景、视觉变化配方。
5. 人工核对产品保真、人物一致、BASE/FINAL 状态变化、P5 锚点与视频节奏。
6. 通过后再扩大到多市场；市场扩展优先新增规则包，不复制业务流程。
