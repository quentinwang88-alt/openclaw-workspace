# 配饰原创混合展示流程：实现 Review

日期：2026-09-15。范围：当前工作区未提交的配饰混合模板及下游集成改动，对照 `ACCESSORY_MIXED_TEMPLATE_BATCH_DEVELOPMENT_HANDOFF_20260914.md`。

结论：模板配置、合同编译和部分下游投影已经实现，但尚不满足首期验收。发现 1 个 P1、8 个 P2。主要缺口是新合同没有成为全流程一致的执行依据，且最终混合镜头没有进入批次差异判断。

这是代码与离线行为 review，不是生成视频的画质验收。未调用付费模型、生成视频、回写飞书或修改生产数据。本次仅新增这份报告，没有修改实现代码。

## 1. [P1] 四镜头合同与五镜头结构不一致仍通过校验

位置：`skills/original-script-generator/core/accessory_mixed_templates.py:834`；`core/simplified_complete_script.py:1375`。

旧结构先编译 capture units，再按序号覆盖新混合模板字段。数量不一致时，投影函数返回 `MISMATCH`，调用方只处理 `SKIPPED`，继续保留结果；正式脚本校验没有消费该错误。

离线复现：对 MIXED 结构指定五个节拍 HOOK / PROOF / PROOF / USE_PROCESS / ENDING，输入五个各 3 秒的有效镜头，冻结四镜头模板 A。normalize 后返回：

```json
{"status":"MISMATCH","errors":["MIXED_TEMPLATE_UNIT_COUNT_MISMATCH:compiled=5 contract=4"],"applied_units":4}
```

`validate_simplified_visual_script` 仍返回 valid=true。第五镜头没有混合模板的 module/carrier/no-face 字段；前四个单元的 duration_seconds 被覆盖为模板时长，原时间区间仍为各 3 秒，形成两套不一致的时间轴。

修复：在结构选择/编译阶段使节拍与冻结模板对齐；投影不完整或时长矛盾必须阻断后续生成，不能只记录状态。

验收：四模板镜头遇到五结构镜头时明确拒绝或先进行有合同依据的结构转换；每个镜头必须具有合法承载、裁切、时长，最终时间轴连续且总长 15 秒。

## 2. [P2] 混合合同校验失败仍进入可生成队列

位置：`skills/original-script-generator/core/original_batch_allocator.py:1416`；执行入口 `core/original_batch_executor.py:978`。

合同失败只写入 `mixed_template_contract_status=REJECTED`，随后照常构建 seed、返回 PLANNED。没有消费 REJECTED 状态的执行拦截。

离线 mock 合同编译结果为 16 秒时长错误，调用真实 `_make_item`，得到：

```json
{"item_status":"PLANNED","mixed_status":"REJECTED","contract_present":false}
```

影响：执行器仍能选中该条目并进入模型调用，产出缺少本次混合合同的内容。

修复与验收：规划失败项应进入明确的失败/延期状态；执行入口再检查一次。注入合同错误时，不得入生成队列、不得触发模型调用。

## 3. [P2] 最终混合镜头没有进入去重，10–20 条差异目标未实现

位置：`skills/original-script-generator/core/original_batch_allocator.py:1542`；`core/accessory_mixed_templates.py:335`；`core/original_batch_executor.py:553`。

模板和环境都按序号同时轮换，各三个选项，每三条回到相同组合。动作和观察重点主要来自类目常量；`difference_report` 初始化为空后没有比较/填充消费者。现有历史预留依旧使用旧 creative_diversity_contract，未使用最终混合合同。

离线调用真实 `_make_item`：商品、方向、主题、claim、hook 相同，序号分别为 1 和 4，旧 visual_signature 不同。第二条传入第一条已用签名，仍得到：

```json
{"both_accepted":true,"allocation_signatures_differ":true,"mixed_contracts_identical":true}
```

影响：系统会把旧场景签名不同、最终混合镜头合同完全相同的候选当成不同内容；不能据此宣称已解决同产品批量重复。

修复：依据最终使用的主题、首镜、观察重点、商品状态和佩戴关系计算差异，与本批和历史比较。不同颜色/顺序只应作为辅助差异；不足时少产出并说明原因。

验收：上述 1/4 候选必须识别为重复；保留的每条候选应有可核查的差异理由。

## 4. [P2] 首帧由当前环境开关决定，冻结的不露脸约束会失效

位置：`skills/original-script-generator/core/first_frame_contract.py:350`。

`_mixed_accessory_frame_line` 根据当前环境变量判断是否输出混合模板首帧规则，没有以任务已冻结的混合合同为依据。同一份冻结 NO_FACE 输入，开关为 1 时输出不露脸规则，为 0 时回到历史耳饰“半脸耳侧近景”。

影响：异步首帧 worker 没有同样的环境变量、或开关关闭后续跑，均可能改变已冻结任务的裁切。反过来，打开开关也会改变没有新合同的旧配饰任务。

修复：开关只决定新规划是否启用；首帧与续跑读取冻结合同和版本。将影响首帧的合同纳入相应缓存/指纹。

验收：同一冻结任务在开关切换前后输出约束一致；无混合合同的旧任务不因启用开关改变。

## 5. [P2] 非耳饰类目与静物镜头收到错误的身体裁切规则

位置：`skills/original-script-generator/core/accessory_mixed_templates.py:293`、`:727`；`core/production_script_renderer.py:1831`。

蓝图指令统一要求“佩戴关系只用耳侧、耳廓、耳垂、颈侧、少量下颌边缘描述”，生产提示词也写死“人物只以耳侧、耳廓、颈侧等局部身体入画”。这些指令对手链、戒指、发饰同样生效，与它们自己的手腕/手指/后脑规则冲突。

此外，合同把同一类目的身体 allowed_framing 复制到所有模块。例如戒指 STATIC_PRODUCT 的 allowed_framing 仍包含“同一只手与同一根手指近景”；耳饰静物仍带耳部范围，无法准确表达独立台面静物。

修复：按“类目 × 镜头模块”产生裁切规则：佩戴用对应身体区域，手持用手与商品关系，静物用商品与台面。通用不露脸段落不应写死耳部。

验收：四类目分别渲染三种承载；每段允许范围与承载一致，非耳饰没有耳部独占指令。

## 6. [P2] 手镯被要求展示不存在的链节、吊坠和搭扣

位置：`skills/original-script-generator/config/accessory_mixed_templates.json:212`；合同动作赋值 `core/accessory_mixed_templates.py:300`。

WRIST 同时覆盖手链和刚性手镯，但动作统一要求展示链节、吊坠垂落及搭扣。编译 product_type=手镯，可直接得到“手指稳定承托腕饰，小幅转动展示链节走向与搭扣外观”。动作编译未依据商品锚点确认这些部件实际存在。

影响：实心手镯会收到要求生成虚构结构的正向指令；“手链与手镯不得互相冒充”的负向规则不能消除该冲突。发饰也需按发夹、发簪、发圈等物理结构检查同类问题。

修复：按物理子类型定义动作与观察重点，再以已验证的锚点裁剪。无法确认的背面/连接结构不进入必拍动作。

验收：无吊坠链、实心手镯、无扣手镯分别只生成实际存在的观察重点。

## 7. [P2] 冻结光线配方未传入视觉合同，同一 seed 出现两套要求

位置：`skills/original-script-generator/core/simplified_complete_script.py:2361`；`core/visual_execution_contract.py:412`。

视觉合同新增了 accessory_environment_recipe_id 参数，但真实 seed 构建调用没有传入。构建函数因此使用默认 WINDOW_LIGHT_WOOD。

离线复现：混合合同选定 MATTE_GREY_DETAIL，生成 seed 后 `visual_execution_contract.lighting_recipe.recipe_id` 仍为 WINDOW_LIGHT_WOOD，而混合蓝图仍包含原来灰色环境配方。

影响：同一模型输入包含冲突的台面、光线要求；环境轮换和单片色调一致性不能按冻结合同保证。

修复：从冻结混合合同传入同一配方，并让蓝图、视觉合同、首帧与生产提示词共用。历史任务使用其原冻结值。

验收：每一种配方都通过真实 seed 入口验证各阶段一致，不能只直接测试视觉合同构建函数。

## 8. [P2] 中央口播输入丢失每镜承载和切镜关系

位置：`skills/original-script-generator/core/simplified_complete_script.py:3262`、`:3284`、`:3292`。

口播适配仍按整片 presentation_mode 计算一次 carrier，再写入全部 shots/shot_plan；连续性也统一写成 EVENT_1，没有读取新增的逐镜头字段。

离线复现：模板 C 的 storyboard 承载为 STATIC_PRODUCT / HAND_ONLY / WEARER_ACTIVE / WEARER_ACTIVE，经过 `build_simplified_voiceover_inputs` 后，两个口播输入结构中的四镜均成为 WEARER_ACTIVE。

影响：新模板虽已表达静物和手持，口播的结构化依据却仍把整片当作佩戴展示，削弱了文案与可见镜头的契合约束。这是本次混合流程未接通的集成点；尚未通过真实模型输出证明具体错词频率。

修复与验收：优先继承冻结逐镜承载及连续性分组，只有旧脚本缺字段时才走整片兜底。模板 C 经口播适配后四镜的承载必须原样保留。

## 9. [P2] 提示词压缩会删除后续镜头独有的商品可见约束

位置：`skills/original-script-generator/core/video_prompt_compaction.py:286`。

压缩把第一条“商品必须可见”提升为“每段商品必须可见”，之后遇到同标签一律删除，没有比较各镜头的值是否相同。

离线复现：4251 字符输入，第一镜要求“耳饰整体轮廓”，第二镜要求“背部耳针连接处”。压缩后第二个观察重点消失，第一个却被提升为每镜必须展示。

影响：混合展示中的结构细节可能在最后送生产前丢失；该压缩入口还适用于其他 UGC 类目，影响范围不限于本次配饰。

修复：只有各镜头完全相同的约束才能提升为全局；独有的商品锚点、动作边界和裁切约束必须留在对应镜头。

验收：相同约束可去重，不同镜头的不同锚点压缩后都存在且归属不变。

## 已执行的验证与边界

在临时数据目录下使用 unittest 执行 7 组现有测试，共 200 项，全部通过：

- test_accessory_mixed_templates.py：50 项。
- test_accessory_mixed_integration.py：13 项。
- test_category_execution_adapter.py：34 项。
- test_simplified_complete_script.py：53 项。
- test_production_script_renderer.py：30 项。
- test_visual_execution_contract.py：8 项。
- test_first_frame_contract.py：12 项。

另做了上述针对性离线构建、normalize/validate、提示词渲染及 mock 复现。协作审查中重复执行的测试没有重复计入 200 项。

现有测试通过说明已经覆盖的接口和字段行为正常，不能替代以上跨阶段合同一致性检查。未验收实际成片的手部形变、商品身份一致性、光影真实度或真实模型文案质量。

## 修复顺序与最终验收

1. 先修复 1、2，保证不完整/非法合同无法流入生成。
2. 再修复 4–8，保证类目、模块、首帧、口播和光线使用同一份冻结语义。
3. 修复 3、9，确保差异判断和生产压缩后的实际镜头一致。
4. 加入覆盖上述复现的集成回归；优先从真实规划入口一路走到最终提示词，而不是只断言新增字段存在。
5. 代码回归后，再用四类目各一款、三种镜头顺序做小批样片验收。批量 10–20 条应检查逐条差异报告；差异不足时允许少产出。

未作为确定缺陷计入的核对项：长视频也复用了部分规划入口，需要追加“15 秒混合模式不注入长视频”的隔离回归；参考图/首帧进入生产的完整交付能力仍需独立验收，不能把未改动的旧同步逻辑当成本次新增回归。
