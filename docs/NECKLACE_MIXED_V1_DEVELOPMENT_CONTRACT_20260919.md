# 项链专属混合展示 V1：开发合同与隔离验收

日期：2026-09-19。状态：开发方案，未实施。本文中的新增模块名、字段与开关是拟议接口，不表示当前已经存在。

## 0. 给开发模型的任务声明

在现有原创脚本系统内，为“单层、单吊坠项链”新增一个默认关闭、显式启用的专属混合展示分支。保持耳饰、腕饰、戒指、发饰、女装、长视频及旧冻结任务的既有行为不变。

首期只做一个 15 秒四镜模板、一套光影、同一套穿搭。不得扩展成配饰系统重构，不得修改默认模型，不得顺带修改上一轮 review 中不属于项链接入的共有问题。

业务参考：`/Users/likeu3/Desktop/罗盘项链视频.mp4`，约 12.6 秒。只借鉴佩戴主体、掌心细节、托盘收尾和光影感觉，不复制它的商品纹样、人物身份或第二套衣服。正式商品事实来自任务自己的商品参考资料。

开发源：`/Users/likeu3/.openclaw/workspace/skills/original-script-generator`。
同步消费者：`/Users/likeu3/.openclaw/workspace/skills/script-run-manager-sync`。
审阅及测试运行产物：`/Users/likeu3/.openclaw/shared/data/reviews/necklace_mixed_v1/`，生产库不得作为测试库。

## 1. 已核实的代码基线

本次检查时，原创脚本相关最新提交为 6843591，之前包括 d78f80c 等表达素材修复。开发开始时重新记录 HEAD、工作区差异和安装路径；若实现已经变化，以实际调用链为准更新落点，但不能扩大需求。

- `config/product_type_config.json` 已有 canonical_type=necklace、family=jewelry、slot=neck；另有 choker。**不需要新增重复项链类型，不改现有类型解析优先级。**
- `config/accessory_mixed_templates.json` 的混合区域只有 EAR/WRIST/FINGER/HAIR，canonical_type_to_zone 尚不含 necklace。
- `core/accessory_mixed_templates.py` 的无参数 list_mixed_templates / mixed_template_ids / select_template_id 使用全局 A/B/C。把项链模板直接追加进这个数组会使其他类目轮转到项链模板，禁止这样做。
- `core/original_batch_allocator.py::_build_mixed_template_injection` 当前使用无类目上下文的模板及配方轮转，需要增加项链的局部选择入口。
- 当前 schema 和四种模块可以表达本方案，无需新增模块枚举或改共享 execution_profile。
- `core/production_script_renderer.py::render_video_generation_prompt_checked` 已支持最终压缩后的执行检查，并绑定 prompt_hash / renderer_version。优先复用。
- 上轮 review 发现最终同步消费者尚有门禁缺口。不得只给导出增加检查就宣称项链可以安全送生产；见第 8 节。

## 2. 不可改动的行为边界

以下是硬约束：

1. 原 A/B/C 模板 ID、顺序、时长、默认光影列表及轮转结果不变。
2. 原四类配饰的类别规则、动作词表、全局不露脸清理、商品事实与去重算法不因本任务改变。
3. 不改女装、长视频、复刻、人工上传配音或发布排期逻辑。
4. 不替换模型、路由、中央口播服务，不建新数据库或飞书表，不增加后台定时任务。
5. 不批量重规划历史任务，不覆盖旧稿，不清空 checkpoint，不批量重置 RUNNING/READY。
6. 不修改共享 schema/profile 常量来标识项链；用附加的 feature_profile 字段。
7. 不全局提升口播或组装缓存版本。项链变化用仅该分支参与的版本字段与 hash。
8. 不用商品名称子串、neck slot 或 jewelry family 直接判定单吊坠项链；choker、胸针、吊坠配件不能误入。
9. 关闭新开关时，不删除配置或破坏已冻结项链稿的解析能力。
10. 不重写整个共享模块。确需改动共享函数，只增加显式上下文的窄分支，旧调用默认仍走原路径。

## 3. 首期产品合同

模板 ID：`NMX_01_WEAR_DETAIL_STATIC`。
feature_profile：`NECKLACE_MIXED_V1`。
feature_version：`1`。
推荐光影 ID：`NMX_WARM_NEUTRAL_WINDOW_V1`，仅在项链配方池内可选。

| 单元 | 时间 | 现有 module | carrier_mode | product_state | 观察任务 |
| --- | --- | --- | --- | --- | --- |
| CU_01 | 0–4s | WORN_DETAIL | WEARER_ACTIVE | ALREADY_WORN | 锁骨、链条弧度和吊坠落点；已经佩戴完成 |
| CU_02 | 4–7s | WORN_RELATION | WEARER_ACTIVE | ALREADY_WORN | 同一领口与项链的整体比例，比首镜略宽 |
| CU_03 | 7–11s | HANDHELD_PRODUCT | HAND_ONLY | HELD | 掌心承托吊坠、链条自然铺落，观察一个有依据的细节 |
| CU_04 | 11–15s | STATIC_PRODUCT | STATIC_PRODUCT | RESTING_ON_SURFACE | 哑光托盘上的吊坠与链条布局，静物收尾 |

face_policy=NO_FACE，global_carrier=MIXED，音频继续使用现有 VOICEOVER_POST。剪辑使用现有 DIRECT_CUT；同镜不生成摘下、戴上、扣合等过程。

V1 两个佩戴镜默认不加入手拉链动作，只保留自然呼吸与轻微肩部变化。轻触链条作为后续有单独样片验收的可选能力，不能本轮为了像参考视频而加入复杂拉动过程。

颈部取景从下巴以下裁切，主要呈现颈部下段、锁骨、领口上方及少量肩部；不要求胸部以上整个人入镜，不显示嘴、鼻、眼或反射脸。没有依据的链长／吊坠尺寸不凭空写厘米值。

光影锁定同一方向的暖中性侧窗光，衣物米白或其他已配置的简洁低饱和颜色；托盘哑光。保留接触阴影、皮肤与织物纹理，抑制金属过曝，不加星芒。暖背景不能改变商品原色。

## 4. 路由、开关与失败行为

新规划开关建议：`ORIGINAL_SCRIPT_NECKLACE_MIXED_V1_ENABLED`，默认 0。此开关只允许项链新规划进入新分支；不能替代现有混合模式总开关和业务作用域。

进入新规划必须同时满足：

- 现有 mixed_scope_decision 已允许的业务分支、simplified_v1、15 秒、新规划条件；
- 原混合模式开关与项链开关均开启；
- 归一类型严格等于 necklace；
- 商品锚点／可靠资料能够确认单层链和单吊坠，且没有明显结构冲突；
- 任务已有可用商品参考和非空有效主线。

单层、单吊坠的资格判定写进现有商品证据／冻结结果，记录来源和 UNKNOWN；不能靠标题“项链”把两者强制设置为 VERIFIED。可见的绕圈摆放也不能自动当成多层项链。

| 输入情况 | 行为 |
| --- | --- |
| 非 necklace | 原路径逐字保持，不受项链开关影响 |
| necklace，开关关闭，无新冻结合同 | 原处理路径不变，不声称新模板生效 |
| necklace，显式启用且符合资格 | 使用 NMX 模板 |
| necklace，显式启用但层数/吊坠数未知或不适配 | 返回可追溯的拒绝/待核实原因，不悄悄回退耳环或女装模板 |
| 已冻 NECKLACE_MIXED_V1，之后关开关 | 仍能读取、审阅及按原合同恢复；不重规划或替换模板 |
| 项链请求拿到 AMX 模板，或耳环请求拿到 NMX 模板 | 明确报模板归属错误 |

必要的新原因码可采用 NECKLACE_SCOPE_UNSUPPORTED、NECKLACE_EVIDENCE_INCOMPLETE、NECKLACE_TEMPLATE_MISMATCH、NECKLACE_MAINLINE_UNAVAILABLE，映射现有拒绝报告。不要为此新建一套批次状态枚举。

## 5. 实现结构与文件修改范围

建议新增两个实现文件：

- `config/necklace_mixed_v1.json`：只存项链模板、NECK 规则、专属光影、观察与动作边界，不复制整份共享配饰配置。
- `core/necklace_mixed_profile.py`：纯函数负责加载项链配置、资格判断、生成局部 profile overlay、冻结附加合同和项链专属校验；import 不访问数据库、网络或环境写入。

配置 overlay 必须深拷贝／独立构建，不得原地修改 load_mixed_template_definition 返回的缓存对象。项链配置不得进入旧的全局模板或全局光影轮转池。

| 文件 | 允许修改 | 不允许修改 |
| --- | --- | --- |
| core/accessory_mixed_templates.py | 显式上下文的项链解析、局部 definition 接入与合同附加校验 | 原 A/B/C 列表、无参默认返回、旧 module 行为、去重裁决重写 |
| core/original_batch_allocator.py | 在 _build_mixed_template_injection 选择项链 profile；将完整合同写入已有冻结位置 | 更改其他类目分配排序、候选数量、自动增加新主题 |
| core/category_execution/accessory.py | 在明确 necklace 分支补佩戴区域及 prominence 等必要适配 | 修改通用颈饰/丝巾或耳饰 profile，批量替换颈部词 |
| core/simplified_complete_script.py | 仅在现有投影确实漏字段时，补项链合同到模型可见 seed/brief | 重写共享写作提示词、改变所有类目的结尾规则 |
| core/original_batch_executor.py | 仅项链 checkpoint 材料增加局部版本/合同 hash；延续现有渲染检查 | 全局 bump 口播/组装版本、清缓存、改恢复状态机 |
| core/production_script_renderer.py | 必要时读取项链附加事实与局部版本；以当前 module 渲染托盘末镜 | 按镜头序号强制所有末镜佩戴、全局改人物动作 |
| core/mixed_mainline_contract.py | 仅有证据证明现有字段不够时，为项链补具体主线/任务投影 | 重写全类目主线、扩张全局关键词与禁词表 |
| 同步器 core/original_batch_source.py 或其窄辅助函数 | 仅项链新 profile 的最终版本与提示词检查，见第 8 节 | 重写全类目同步、修改其它行状态或排期 |

本轮默认不修改 product_type_resolution.py、product_type_config.json、共享 accessory_mixed_templates.json、selling_fact_evidence.py、mixed_voiceover_mainline.py、storage schema、模型配置或发布模块。类型已存在，不能重复添加。确有接入依赖时先在实施记录写明调用证据与最小 diff；不得以“顺便清理”扩大修改。

## 6. 接入 API 与数据合同

建议接口形态如下，名称可按代码风格调整，语义与兼容要求不可改：

```python
resolve_necklace_v1_scope(product_context, execution_scope, *, enabled) -> ScopeDecision
load_necklace_v1_definition() -> Mapping
build_necklace_profile_overlay(product_evidence) -> Mapping
validate_necklace_v1_contract(contract) -> list[str]
```

ScopeDecision 至少包含 applicable、eligible、reason、evidence_refs。非项链 applicable=false；显式选择项链新模式却不合格时 applicable=true、eligible=false，调用方不能把这两者都当“空结果，随便走旧路径”。

共有编译器继续产出现有 mixed_template_contract。优先给内部 definition 消费点增加 keyword-only 的可选上下文，None 时保留原路径；只有项链入口传入 overlay。不要用全局变量／临时修改 CONFIG_PATH／改缓存内容切换配置。

所有读取模板、配方、类目 framing、物理 subtype 的位置必须使用同一上下文，包括 compile_mixed_template_contract、project_module_framing、get_mixed_template、get_environment_recipe 与校验器。不得编译时用项链定义、校验时却回到旧全局定义。

无参 mixed_template_ids()、list_mixed_templates()、select_template_id(index) 与 select_environment_recipe_id(index) 输出仍保持原值。可新增有上下文的内部选择器；项链只有一个模板，因此不参与 A/B/C 轮转。

现有合同额外附加一个命名空间，建议：

```json
{
  "feature_profile": "NECKLACE_MIXED_V1",
  "feature_version": 1,
  "profile_config_hash": "<项链配置快照哈希>",
  "necklace_contract": {
    "subtype": "SINGLE_LAYER_SINGLE_PENDANT",
    "eligibility_evidence_refs": [],
    "chain_identity": {},
    "pendant_identity": {},
    "wearing_relation": {},
    "interaction_mode": "NONE"
  }
}
```

以上仅示意最小结构。链条与吊坠字段复用既有商品身份引用，避免再复制一套会漂移的事实库。额外字段只保存确实被消费者读取的信息。

模板序列、时长、光影完整参数、各镜观察与边界必须落入冻结合同，不能恢复时只拿 template_id 读最新配置。既有 seed / category_execution_extension / video_generation_brief 的投影需要保留该合同；不要增加一份消费者不读取的旁路文件。

核对链路：资格判定 → 规划选择 → 冻结合同 → 模型实际收到的 seed → 成稿及口播 → 最终提示词 → 执行检查 → 实际同步文本。

## 7. 内容与缓存要求

主线首期只用：佩戴落点与比例、吊坠已有纹样／层次、与当前实际领口的搭配。两条稿若仅换暖光或同义词，不扩大独立主题数。

每镜观察任务必须是具体商品信息，不能只写“展示核心卖点”。例如已知吊坠为圆形纹样：首镜看落点，次镜看与领口比例，掌心镜看正面纹样，静物镜看完整轮廓与链条布局。商品没有纹样时不得套入罗盘纹样。

口播仍使用现有中央完整口播流程。没有来源的材质、舒适性、不过敏、不掉色和功能不自动补写；不能将缺失主线强行清空后继续声称内容合格。资料不足时选择已有有效主线，若无则报告缺口。

项链分支的规划、视觉、口播和组装 checkpoint 按依赖纳入 feature_profile、feature_version、冻结合同 hash 及相关局部投影版本。非项链时**不要加空字段**，旧哈希材料保持不变。只改项链视觉动作时不应无理由让旧耳环口播缓存全部失效。

沿用共有 schema 和 SCRIPT_RENDERER_VERSION；如只有项链渲染有变，增加项链局部版本到该分支审计 metadata。若共有行为确实变化，必须作为独立兼容变更说明，不能借本任务全局 bump 掩盖影响。

环境开关控制新规划选择，不作为已冻结内容的可变身份来源。恢复和重导消费冻结快照，不能因当前光影配置更新而静默改变历史稿。

## 8. 最终校验和生产消费者

复用 checked renderer 的压缩后检查，针对项链增加以下断言：四镜顺序和时间正确、NECK 佩戴范围、掌心镜只有手与商品、静物末镜无人且商品静置、商品身份引用一致、无旧耳侧/腕部/发型动作回流。

不能靠字符串出现“头部”就判失败，应区分“不得出现头部”与实际动作要求。代码只能验证脚本约束，链条是否在成片中断裂、穿模仍需成片审阅，报告不得混称已自动验证。

最终同步处对从冻结源识别为 NECKLACE_MIXED_V1 的行核验当前实际提示词及 hash、共有/局部渲染版本。不要只依赖可被删除的表格标记；按 script_id 读取冻结身份或可信来源引用。

当前文本与通过检查的文本不一致、没有新版项链检查、或已知 FAIL 时，返回该行错误，不写目标任务、不取消源勾选、不触发模型重生。修正并重新导出后允许正常继续。其它类目暂不改变既有同步行为；共享门禁缺陷另开修复任务，不能混进项链 PR。

若不完成此项链范围的最终消费者检查，本轮只能标“脚本开发与离线验收完成”，不得开启项链生产试跑。

## 9. 必须交付的测试

先保存当前旧行为，再实现。真实数据库只读；测试用临时目录、隔离存储和 mock 外部调用。

| 测试组 | 必须覆盖 |
| --- | --- |
| 路由 | necklace 单吊坠通过；choker、多层链、无吊坠链、未知层数不误入；开关关/开行为；非15秒及其他分支不进入 |
| 模板隔离 | 开关开/关时，旧 A/B/C ID列表、顺序、配方轮转逐项不变；旧类目不能获取NMX，项链不能混选AMX |
| 配置缓存污染 | 按“耳环→项链→耳环”和“项链→耳环→项链”交错编译，重复结果一致，共享配置不被修改 |
| 合同 | 4/3/4/4秒，四种现有module，单层单吊坠依据可追溯；没有尺寸来源不生成数值；未知扣头不展示 |
| 全链路投影 | 捕获模型实际payload确认合同存在；不是只检查规划侧有字段；最终末镜确实静物，无旧佩戴动作覆盖 |
| 冻结与恢复 | 开关关闭后已冻稿仍可解析；配置更新不静默改老稿；旧非项链dependency hash不变；项链相关合同修改会使相应缓存失效 |
| 内容 | 无效主线不能报完整；罗盘造型不升级功能；掌心与静物不能机械重复同一个观察任务 |
| 渲染 | 压缩前后时间、身份、载体、不露脸一致；失败时错误带shot_id与字段；否定句不误报 |
| 最终交接 | PASS且hash吻合可生成任务；FAIL、改过文本、过期局部版本、缺校验均不能生成目标任务；非项链行为不变 |
| 旧类目回归 | 耳饰、腕饰、戒指、发饰各至少一个固定输入；四条既有review样本可只读用作离线回归；女装和长视频相关现有用例通过 |

旧类目固定输入对照至少覆盖：类型、模板、各镜时长/动作、光影、口播payload、最终提示词和缓存依赖材料。排除真正的时间戳等易变字段后比较；不能把正文、顺序或业务字段作为“噪声”忽略。

现有测试文件按改动面执行；新增建议文件：test_necklace_mixed_profile.py、test_necklace_mixed_integration.py、test_necklace_profile_isolation.py，以及同步器中的项链交接测试。无需为了凑数量重测所有无关模块。

## 10. 提交顺序、上线与回退

提交1：独立配置、纯profile模块、资格与模板隔离测试。新开关默认关闭，无生产行为变化。

提交2：编译/分配/冻结/必要适配投影，补交错调用与旧类目golden对照。仍默认关闭。

提交3：最终渲染、局部缓存版本及项链消费者检查。旧类目对照全部通过，离线交付可复核。

提交4：文档与验收材料。记录确切源代码版本、安装镜像位置、调用入口、开关状态和未覆盖项。不把未执行的真实生成写成通过。

正式启用仅在现有运行入口对明确选择的试跑任务开启项链开关，先一个资料完整的商品、一条稿、一套衣服。不要在 run_feishu_operation_tasks 中把新开关 setdefault 为1，也不要修改全局 .env 或自动任务。

成片首轮检查：链条连续性、吊坠形状、数量与比例、身体接触、商品原色、四镜光影、实际语音容量。通过后再考虑轻触链条和双领口版本。

回退：关闭项链新规划开关；保留合同解析器和审阅能力，不删除冻结数据。若须回退代码，保留可识别新合同的版本或将相关任务明确留待处理，禁止回退后让新合同误走旧路径。

开发交付必须列出：修改文件及理由、未改动边界、关键diff、测试结果、旧类目对照、模型payload捕获、最终四镜提示词、门禁反例、版本与回退步骤。未全部做到，不得仅以“项链生成成功”宣布开发完成。
