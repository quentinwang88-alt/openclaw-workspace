# OPV 多组试跑准备落地记录（2026-08-31）

## 本轮已落地

- Theme 只保留语义分镜权重；Planner 按账号 Render Preset 归一化最终时长和转场。
- 当前泰国账号统一为 `RP_STILL_FASTCUT_10S_V1`：5×2 秒、硬切；旧 12.5 秒预设在 RDS 标记为 `deprecated`。
- 9 个泰国 Theme 的最终封面短句已清除中泰混写；执行说明独立放入 `overlay_instruction`。
- 泰语标题词槽修正 `หุ้นสวย` 为 `หุ่นสวย`。
- 原创人物模板库明确成为 Persona 权威来源；计划快照记录同步来源、本地参考图和源字段。
- 新增单图 + 五图组视觉 QA 合同与门控，复用 creator-crm 已配置的多图视觉线路。账号已设置 `visual_qa_required=true`，未完成视觉 QA 的新图组不能进入人工整组批准。
- 最新 P1-P5 均为选中版本；旧任务 `opv_task_20260830_a629e64d2dda` 已回填为 5/5 选中。
- 新增只读预检 `scripts/preflight_task.py`，检查 RDS、商品参考图、Persona/Look/Scene、Theme 匹配、时间线、语言纯净度、镜头完整性和历史发布记录。
- 新增 6 组实验合同 `config/experiments/TH_BASELINE_6RUN_V1.json`，所有 run 使用显式且不同的幂等键。

## 已验证的真实预检结果

当前已发布任务：

- 商品参考图：3/3 本地可读。
- Render：10,000ms，`[2000,2000,2000,2000,2000]`，5 个 hard cut。
- Persona：来自原创人物模板库，飞书附件 1 张，但本地可供模型读取的 Persona 参考图为 0。
- Look / Scene：`STYLE_OPV_PUFFER_TRAVEL_001` / `ENV_AIRPORT_DEPART_001` 均可解析。
- 镜头：P1-P5 完整，选中 5/5。
- 历史发布：检测到 NeoBund 1327852 和已终止的 1327683，预检会阻止盲目重提。

历史任务仍保留旧版中泰混写 overlay，这是历史快照，不回写篡改；所有新 Plan 使用清洗后的 Theme。

## 6 组试跑边界

- A1/A2/A3：同商品、同 Persona、同 Look、同机场场景，仅测试生成稳定性。
- B1：卧室收拾行李场景；需要先补与卧室语义匹配的旅行打包 Theme/Storyboard，不能直接套机场分镜。
- B2：第二套 Look；本地候选优先 `STYLE_002`（白色直筒裤，风险最低），备选 `STYLE_004`（黑色开衩裙，差异更大但需重做场景适配）。
- B3：第二商品 + 匹配 Theme；等待商品编码与 3-5 张参考图。

先生成图片与视频母版，完成视觉 QA 和人工评分后只发布排名前 2，不自动批量提交 NeoBund。

## 当前唯一硬数据缺口

`TH_APPAREL_CAFE_001` 没有本地 Persona 参考图。testing 状态允许带警告试跑，但无法公平评价跨组人物一致性；转 active 前至少需要 3 张，建议 3-5 张同一人物、不同角度、自然光、无重滤镜参考图。
