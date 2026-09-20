# 项链 V1 实现复核 · 2026-09-20

结论：项链独立配置、4/3/4/4 秒四镜模板、默认关闭与局部接入已经实现；但实际生产门禁未在真实导出形态上生效，视觉执行合同漏接项链配方，数量证据解析存在否定／未知误判。暂不建议开启项链生产。

## 审查范围与证据边界

- 实现提交：79cb465、cce51e6、c5884c2，读取当前工作区，包括用户尚未提交的 test_necklace_mixed_integration.py 修改；未覆盖或修改该文件。
- 开发合同：docs/NECKLACE_MIXED_V1_DEVELOPMENT_CONTRACT_20260919.md。
- 实际模型样本：run_6/necklace_v1_live/out/dump_68A58E，批次 OCB_ED41AFA5023A5568A58E，内部脚本 ID SCRIPT_8BCEF97D70205D7F6540。
- 该样本是真实模型生成，但输入是补充数量锚点与测试卖点的 fixture。原始商品被 layer_count/pendant_count UNKNOWN 拦下；补入“单层金色细链，只挂一枚吊坠”后才跑通。不是未经加工的真实商品生产验收。
- context_fixture.json 明确 product_reference_assets 为空、无本地该版本审阅后商品图；测试卖点标记 TESTSET_ANCHOR_CANDIDATE / NOT_OPERATOR_CONFIRMED。不能据此认定商品已完成图像与卖点确认。
- 未执行视频生成，未审成片、实际配音时长或母语自然度。
- 本次只读检查产物并做离线纯函数／临时数据库 mock 复现；未调用生成模型、未写飞书或生产库、未修改业务源码。

## F1 · P1：真实导出使用公开脚本 ID，门禁未解析到冻结身份即放行

位置：skills/script-run-manager-sync/core/necklace_handoff.py:504–513。

门禁先按内部 script_id 查库，只有文本看起来带项链模块签名时，才尝试公开 ID 兜底。真实导出字段使用公开 ID，而真实渲染的镜头标题使用叙事角色，并不携带预期模块签名。

真实样本：

- 内部 ID：SCRIPT_8BCEF97D70205D7F6540。
- 导出公开 ID：SCSCRIPT_40284D07CBCADD28F3F2。
- 镜头标题：HOOK / PROOF / PROOF / ENDING。
- 绊线期望：WORN_DETAIL / WORN_RELATION / HANDHELD_PRODUCT / STATIC_PRODUCT。

离线端到端复现：将该真实样本放入临时 SQLite，以真实 export_ready_batch 导出（mock 飞书），再将导出字段交给真实同步适配器。结果 tasks=1、errors={}，并非完成了项链检查，而是未找到身份后直接跳过检查。

进一步将提示词改成“人物正脸出镜，展示耳侧耳环”，使用真实公开 ID 仍返回 tasks=1、errors={}；换成内部 ID 才正常拒绝。

这不只是异常时的安全网缺口，当前真实样本正常导出路径也会触发。仅有基于人工模块标题的门禁测试不能证明正式路径有效。

建议：先基于稳定 ID／可信来源解析内部与公开 ID，再判断是否项链。公开 ID 查找不能依赖可编辑提示词的措辞。不要按 4/3/4/4 时长猜类别，因为 AMX_C 具有相同时长。无法确认身份的项链新稿应有明确错误，不得自动当非项链放行。

## F2 · P2：视觉执行合同仍用旧全局配置，项链光影与 NECK 范围为空

位置：skills/original-script-generator/core/visual_execution_contract.py:424–435。

_mixed_visual_contract 已收到项链冻结合同，但仍通过 load_mixed_template_definition() 查 NECK，以及无上下文 get_environment_recipe() 查 NMX 配方。旧全局定义没有这些项，异常被吞掉并生成空内容。

真实样本 visual_execution_contract 中：

```text
lighting_recipe.recipe_id = NMX_WARM_NEUTRAL_WINDOW_V1
lighting_recipe.environment = ""
lighting_recipe.goal = ""
lighting_recipe.label = ""
lighting_recipe.recipe_version = null
framing_zone.zone = NECK
framing_zone.allowed_framing = []
framing_zone.forbidden_framing = []
```

冻结混合合同本身有完整信息，问题在下游投影。其他字段仍携带局部光线及取景要求，因此不是声称模型完全没收到光线；但宣称“冻结配方权威”的视觉合同实际为空，下游各处依据不一致，也无法稳定保障参考风格。

建议：项链分支从冻结快照派生配方与范围，不能回读旧全局池或重建最新配置；非项链维持原路径。补真实冻结合同→视觉合同→模型 payload 的一致性断言，不能只测编译器输出。

## F3 · P2：数量解析不识别否定与不确定，未知可被放行

位置：skills/original-script-generator/core/necklace_mixed_profile.py:563–566。

计数按关键词子串、从大到小匹配，没有判断断言极性与不确定性。通过真实 build_necklace_product_evidence → judge_necklace_eligibility 链路复现：

| 硬锚点文本 | 当前结果 | 应有结果 |
| --- | --- | --- |
| 单层链条带吊坠，不能认定单吊坠 | layer_count=1、pendant_count=1、eligible=true | 吊坠数未知，不通过首期资格 |
| 不是双层，是单层链条，只有一个吊坠 | layer_count=2、eligible=false | 否定双层，正向证据为单层 |

建议：数量优先使用有来源的结构化证据；文本提取按分句识别肯定／否定／未知，矛盾保留冲突，不用最大数字覆盖。为 count 保留具体来源字段或原句，避免统一标 STRUCTURE_COUNTS 掩盖实际依据。

## F4 · P2：重导新版 PASS 未回存，消费者仍读取旧版 FAIL

位置：skills/original-script-generator/core/production_script_feishu.py:627；消费者读取与版本判断在 skills/script-run-manager-sync/core/necklace_handoff.py:385。

真实样本存储审计为 v1 / FAIL，当前渲染为 v2 / PASS。调用真实 export_ready_batch（mock 飞书）返回 created=1、execution_blocked=0，但库内 result_json 完全未变。使用能正确找到身份的内部 ID 再同步，仍报 NECKLACE_HANDOFF_STALE_LOCAL_VERSION 并要求重新导出。

因此修复 F1 后，旧项链稿还可能陷入“要求重导、重导仍旧审计”的循环。建议将最新交付文本、hash、审计版本和结果一致持久化到消费者实际读取的权威位置，或在消费者重审当前交付文本；保留旧审计历史，不让表内新版 PASS 与库内旧 FAIL 成为两个互相矛盾的权威。

## 已确认实现的部分

- NMX 模板和专属光影放在独立配置，未直接追加进旧 A/B/C 池。
- 现有商品类型注册表复用 necklace，没有重复重写类型体系。
- 项链开关默认关闭，未在入口自动开启；首期只针对单层单吊坠。
- 四镜使用现有模块，0–4 / 4–7 / 7–11 / 11–15 秒，静物收尾。
- 当前样本原有存储审计是旧版 FAIL；使用当前 checked renderer 重新渲染，共享审计与项链 v2 审计均 PASS，0 issues。不能把旧文件里的 FAIL 直接当作当前渲染仍失败。
- 局部 checkpoint 材料采用条件添加，没有为了项链全局升级模型或共有缓存版本。
- 开发报告已更正早期不真实镜头标题夹具和若干不成立的通过结论，review 依据当前代码及实际产物，不沿用文档早期总述。

## 脚本与参考视频的差距

实际项链样本展示字母 H 吊坠，四种模块与时长成立，但内容仍偏“下班前顺手记录”。

1. CU_02 动作为收起桌边物品并转向出口。它增加生活叙事，却弱化锁骨／领口的稳定比例展示。首期更适合固定位置，仅保留轻微肩部变化。
2. CU_03 只明确少量手指承托吊坠，没有真正落实参考里的掌心衬底微距。应明确掌心背景、吊坠正面细节和链条自然铺落，避免手指遮挡。
3. 口播三句反复表达“注意到 H／一眼认出 H／喜欢辨识度”，信息重复。建议一条主线接一个实际细节或佩戴比例，留出纯画面时间。
4. 项链配置的暖中性侧窗光方向是对的，但前述空视觉合同要修复。是否接近参考的肤质、金属高光和接触阴影，只能通过成片验证。

这些是脚本可用性与风格建议，不能从尚无成片推断实际链条已经断裂或穿模。

## 验证与下一步

本次独立执行项链资格测试 49 项通过；渲染／交接审查执行 OSG 229 项及同步器 51 项通过；冻结／隔离审查执行 138 项通过。不同测试组可能重叠，不合并为唯一用例数。上述真实产物与边界复现说明测试通过仍未覆盖全部生产形态。

建议先一起修 F1/F4，再修 F2/F3；补公开 ID 的真实导出→同步反例、旧 FAIL 重导后正常恢复、冻结配方投影、数量否定／未知用例。之后使用有核实数量、当前商品图和真实有效卖点的一款项链做小批脚本及成片验收。保持开关默认关闭，现有耳环路径不扩大修改。
