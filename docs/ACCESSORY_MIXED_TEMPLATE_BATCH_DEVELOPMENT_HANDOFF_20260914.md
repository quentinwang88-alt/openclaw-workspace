**饰品混合展示与同产品批次差异化：开发交接方案**

交接日期：2026-09-14。本文是交给下一位开发模型的实施规格，包含已核实的现状、用户已明确的产品要求、建议实现与验收标准。本文中的新模式名、字段名、配置文件名均为建议，尚未实现；接手者应优先适配现有接口，不能将建议字段误认为现有能力。

**1. 背景与最终目标。** 用户认为原创视频生成线在女装方向遇到明显瓶颈，当前不继续优化女装，希望在耳饰、手链、戒指、发饰上建立真实、精致、相对固定、可批量使用的视频模板。

用户已进一步明确：一条视频必须同时包含手持、静物和不露脸佩戴效果。三种展示方式是同一视频内部的镜头模块，不能把本项目做成三条互不相关的纯手持、纯静物、纯佩戴生产线。

同一产品期望生成 10–20 条候选原创脚本。单条混合展示解决内部单调；批次内容规划解决不同视频之间的重复。数量是候选目标，不是强制填满的配额。商品事实、可见细节或合适的搭配关系不足时，允许少生成，并说明原因。

最终目标是：同产品、多内容方向、固定混合拍摄模板、有限视觉变量、稳定商品一致性。不得把换音乐、换背景、口播同义改写当作主要原创差异，也不得承诺绕过平台重复判定或保证推荐。

**2. 本次范围与交接状态。** 首期仅为现有 15 秒原创带货脚本路径增加显式启用的饰品模式；复用已有商品事实、卖点、口播、存储、运营任务和生产交接。不要重建整套系统。

| 范围内 | 首期不纳入 |
| --- | --- |
| 耳饰、手链、戒指、发饰，按标准商品类型识别 | 女装优化，围巾/丝巾/头巾改造 |
| 15 秒、三种展示方式、默认四个可见片段 | 20–45 秒长视频 Plan C 改造 |
| 不露脸、局部身体关系、同视频统一光影 | 真人对镜口型、复杂剧情与完整穿戴教程 |
| 同产品 10–20 个候选的内容容量、差异和历史比较 | 平台算法规避、发布频率或账号运营策略 |
| 逐镜执行合同、首帧与视频提示词闭环 | 重做原生图文、种草独立分支、人工上传视频配音 |

本轮只做了源码、文档和本地 SQLite 的只读 review，没有修改生成逻辑，没有写飞书，没有调用生成模型、生成图片或视频。本文是开发计划，不是实现完成报告。

历史脚本样本没有找到可关联的首帧绑定；尚未逐条观看相应成片。因此下面的历史观察是脚本和规划层证据，不能表述为成片缺陷统计或生成成功率。

**3. 项目位置、规则与正式入口。** 先阅读开发源 AGENTS.md、SKILL.md 及现有交接文档，源码与当前数据库状态优先于历史说明。SKILL.md 保留了大量旧流程描述，不能照搬旧 S1–S4 入口开发新任务。

| 内容 | 路径 |
| --- | --- |
| 开发工作区 | /Users/likeu3/.openclaw/workspace |
| 原创脚本开发源 | /Users/likeu3/.openclaw/workspace/skills/original-script-generator |
| 原创模块规则 | /Users/likeu3/.openclaw/workspace/skills/original-script-generator/AGENTS.md |
| 原创模块说明 | /Users/likeu3/.openclaw/workspace/skills/original-script-generator/SKILL.md |
| 安装态/共享镜像 | /Users/likeu3/.codex/skills/original-script-generator |
| 脚本送生产模块 | /Users/likeu3/.openclaw/workspace/skills/script-run-manager-sync |
| 短视频生产模块 | /Users/likeu3/.openclaw/workspace/skills/short-video-auto-publisher |
| 总体交接文档 | /Users/likeu3/.openclaw/workspace/docs/ORIGINAL_CONTENT_SHORT_LONGFORM_HANDOFF_20260909.md |
| 运行数据根 | /Users/likeu3/.openclaw/shared/data |
| 原创实际运行库 | /Users/likeu3/.openclaw/shared/data/original_script_generator.sqlite3 |
| 长视频独立运行库 | /Users/likeu3/.openclaw/shared/data/longform_original_video.sqlite3 |

正式脚本入口：

    python3 /Users/likeu3/.openclaw/workspace/skills/original-script-generator/scripts/openclaw_original_script_task.py <action> [白名单参数]

正式送生产入口：

    python3 /Users/likeu3/.openclaw/workspace/skills/script-run-manager-sync/scripts/openclaw_original_batch_sync.py <check|sync> [白名单参数]

15 秒原创从“短视频运营任务表”读取任务，生成“一行一条”的“原创视频生产脚本”；运营勾选“进入生产”后同步至“短视频自动脚本运行管理表”，由原短视频链负责媒体执行。长视频经同一运营入口分流至独立 Plan C，不进入短视频运行管理表。

run_pipeline.py 与旧一行产品 S1–S4/变体表仅用于历史维护，禁止接入新模式。S1–S4 是兼容槽位，不是本项目要固定的四种内容方向。阶段 0 实验仍保持原隔离边界，不借本项目改成生产入口。

运行数据库、测试生成物、参考图缓存、日志都放共享数据或临时目录，不写入 skill 源码树。代码修改在开发源完成，不直接手改安装镜像；先核对安装态是否为软链接，再按当前部署方式同步。

当前任务只授权输出方案。接手模型可依据后续开发指令实现与离线测试；真实模型调用、飞书写入、媒体提交和发布分别遵循用户届时授权及已有状态规则，不把本文当成批量生产或发布指令。

**4. 已核实的现状。** 当前系统具备可复用基础，问题在于饰品混合展示尚未贯穿各阶段。

已有能力包括商品锚点和数量约束、饰品执行适配器、HAND_ONLY/STATIC_PRODUCT/MIXED/WEARER_ACTIVE 承载、近景显著性、低风险动作、冻结方向包、分阶段检查点、中央口播、创意使用记录，以及首帧资产和生产交接。

BatchRequest.requested_count 当前已限制在 1–20；支持 20 个请求不等于已有足够的差异规划能力。不要为本项目另建一套批次系统。

| 已核实问题 | 代码依据（行号为交接时位置） | 开发含义 |
| --- | --- | --- |
| 耳饰默认半脸、镜面、头肩关系 | [accessory.py:451](/Users/likeu3/.openclaw/workspace/skills/original-script-generator/core/category_execution/accessory.py:451)、[accessory.py:1066](/Users/likeu3/.openclaw/workspace/skills/original-script-generator/core/category_execution/accessory.py:1066) | 新模式必须有独立的不露脸裁切和局部动作 |
| HAND_ONLY 耳饰可落到“展开商品局部”通用候选 | [accessory.py:969](/Users/likeu3/.openclaw/workspace/skills/original-script-generator/core/category_execution/accessory.py:969)、[accessory.py:1349](/Users/likeu3/.openclaw/workspace/skills/original-script-generator/core/category_execution/accessory.py:1349) | 按类目和物理形态选择动作，不能统一展开 |
| 静物有主体近景支持，但通用分镜又写已佩戴、人物关系 | [accessory.py:407](/Users/likeu3/.openclaw/workspace/skills/original-script-generator/core/category_execution/accessory.py:407)、[accessory.py:157](/Users/likeu3/.openclaw/workspace/skills/original-script-generator/core/category_execution/accessory.py:157)、[accessory.py:1688](/Users/likeu3/.openclaw/workspace/skills/original-script-generator/core/category_execution/accessory.py:1688) | 按逐镜承载编译，不能只切顶层枚举 |
| 首帧类目指引和承载规则冲突 | [first_frame_contract.py:422](/Users/likeu3/.openclaw/workspace/skills/original-script-generator/core/first_frame_contract.py:422)、[first_frame_contract.py:459](/Users/likeu3/.openclaw/workspace/skills/original-script-generator/core/first_frame_contract.py:459) | 耳饰静物/手持不能再无条件注入半脸；腕饰静物不能无条件注入前臂 |
| 首帧发饰分支只列 hair_accessory/hairclip，戒指缺少专属分支 | [first_frame_contract.py:424](/Users/likeu3/.openclaw/workspace/skills/original-script-generator/core/first_frame_contract.py:424) | 统一标准类型注册表，避免和 claw_clip/hair_clip 等脱节 |
| 完整视觉合同支持集为服装与围巾，四类饰品返回空合同 | [visual_execution_contract.py:18](/Users/likeu3/.openclaw/workspace/skills/original-script-generator/core/visual_execution_contract.py:18)、[visual_execution_contract.py:391](/Users/likeu3/.openclaw/workspace/skills/original-script-generator/core/visual_execution_contract.py:391) | 补专用光影和局部取景分支；不能只扩支持集后误吃围巾默认规则 |
| 通用蓝图要求自拍创作者讲话、限制普通现场光 | [simplified_complete_script.py:2498](/Users/likeu3/.openclaw/workspace/skills/original-script-generator/core/simplified_complete_script.py:2498) | 新模式不再要求对镜讲话，允许经过设计的自然柔光 |
| 口播投影可能把全片承载写到每镜 | [simplified_complete_script.py:3211](/Users/likeu3/.openclaw/workspace/skills/original-script-generator/core/simplified_complete_script.py:3211) | 保留每镜真实承载和佩戴证据，不把 MIXED 当每镜身份 |
| MIXED 实际映射到 PERSON_ON_CAMERA/CREATOR_SELF_SHOT，storyboard schema 缺逐镜 carrier | [simplified_complete_script.py:1326](/Users/likeu3/.openclaw/workspace/skills/original-script-generator/core/simplified_complete_script.py:1326)、[simplified_complete_script.py:2406](/Users/likeu3/.openclaw/workspace/skills/original-script-generator/core/simplified_complete_script.py:2406) | 全片标签不足以保证逐镜混合展示，schema 与默认拍摄模式需要一起处理 |
| 全片默认佩戴连续性不适合混合展示状态切换 | [accessory.py:1136](/Users/likeu3/.openclaw/workspace/skills/original-script-generator/core/category_execution/accessory.py:1136) | 分离全片商品身份与单镜状态连续性 |

本地历史样本：

| 产品类型/产品编码 | 批次 | 日期 | 抽查观察 |
| --- | --- | --- | --- |
| 耳饰 / 1735648377332729435 | OCB_68D472F735F5AA8E122C | 2026-08-26 | 半脸耳侧→头肩→收文件入包→半脸回收；自然光混普通室内光 |
| 戒指 / 1731490506790504027 | OCB_B3BB8FEBFE4962E267F1 | 2026-08-26 | 拿到脸侧→已戴好手部正侧面→正脸分享 |
| 发夹 / 1735250312327234613 | OCB_B31C2178D1C12549DD8D | 2026-08-26 | 后脑、镜面、拿包出门等关系 |
| 手镯 / 1730689205488485979 | OCB_E734E805E24C48B2E297 | 2026-08-17 | 腕部近景→拿包→中全景穿搭关系 |

查询时，“耳饰”标签 10 个 item、“戒指”标签 15 个 item 均为 WEARER_ACTIVE；另有“耳环”标签及发饰、抓夹、手镯的 MIXED/HAND_ONLY/STATIC 历史。不能据此宣称系统硬编码只支持真人，也不能将手镯样本作为细链手链效果证据。以上是本地历史快照，不代表完整线上生产量。

**5. 产品设计：整片固定模板。** 初版固定三种模块、三套整片顺序，默认四个 capture_units。每条成片都出现手持、静物和佩戴；两段佩戴提供不同观察关系。

| 模板建议 ID | 顺序 | 主要用途 |
| --- | --- | --- |
| AMX_A_WORN_FIRST | 佩戴→手持→静物→佩戴 | 佩戴比例、局部搭配、整体效果 |
| AMX_B_FORM_FIRST | 手持→静物→佩戴近景→佩戴关系 | 先看完整造型再理解佩戴 |
| AMX_C_DETAIL_FIRST | 静物细节→手持整体→佩戴近景→佩戴关系 | 具有清楚、可验证且有辨识度的细节 |

模板 A 起始时长建议 3+3+4+5 秒；其他模板与内容重点允许在总计 15 秒内调整。这里是拍摄模块顺序，不自动等于 HOOK/PROOF/USE/ENDING 的叙事 Beat。末镜佩戴也可以是 PROOF，不得凭模板强加原结构不存在的 ENDING。

静物模块不等于四秒完全无信息变化，可以是克制的同侧机位变化；不绕到没有参考证据的背面。手持默认稳定承托加一次小幅转角。佩戴从已经完成的状态开始，使用局部微转、自然呼吸或小幅头肩变化。

首段与末段佩戴必须有不同任务，例如商品本体→袖口比例，耳垂落点→颈侧领口，戒面→整只手比例，发饰装饰→发束固定关系。不能同一素材重复裁切伪装为两个新镜头。

**6. 类目与光影执行规格。** 细分类目使用现有 normalize_product_type 和类别注册结果，不用各文件各写一套中文关键词。

| 类目 | 允许的局部画面 | 主要动作边界 |
| --- | --- | --- |
| 耳饰 | 耳廓、耳垂、颈侧、少量下颌边缘；眼鼻嘴不入画 | 不穿耳、不扣耳堵、不拨发、不快速甩动；手持不“展开”刚性装饰 |
| 手链 | 同一手腕、少量前臂和袖口；自然链节与吊坠垂落 | 不扣细搭扣、不拉伸、不快速翻腕；细链与刚性手镯分开测试 |
| 戒指 | 同一只手与同一根手指，戒面和戒圈关系清楚 | 不掰开口、不换指、不加无授权叠戴、不快速翻掌 |
| 发饰 | 后脑/侧后方、已完成发型和固定发束 | 不完整盘发、不重新夹发、不转到正脸、不引入镜面露脸 |

商品结构与授权数量保持不变；一对耳饰的耳侧镜头只看到一只，不等于商品数量变成单只。身体左右、佩戴位置和可见数量分别记录，不能用“每镜可见数必须相等”误伤合理裁切。

参考图不足时只展示已知面和已知细节。不从小图生成虚构镶嵌、链节、背扣或毫米级尺寸；没有可核实尺寸时可写相对观察，不宣称精确比例。不能只靠一张图中的材质观感认定真实金属、宝石或功能。

| 光影建议 ID | 全片环境 | 目标 |
| --- | --- | --- |
| WINDOW_LIGHT_WOOD | 浅木台面、米白背景、大面积侧窗柔光、轻微反射补亮 | 清爽自然，保留纹理与接触阴影 |
| WARM_WOOD_NEUTRAL_SUBJECT | 稍深木色、低饱和背景，商品中性柔光，背景略暖 | 温润精致，商品不被整体染黄 |
| MATTE_GREY_DETAIL | 哑光灰背景、柔和侧光、可见金属明暗边缘 | 干净结构和正常反射层次 |

同条视频一套环境配方，光向、白平衡、肤色、主要背景材质保持一致；正常角度变化引起的高光变化允许存在。不磨成塑料皮肤，不增加星芒、虚构钻石火彩或过曝白块。不通过镜头抖动、曝光跳变或低画质制造“真实感”。

新档允许经过设计的自然柔光和柔和反射补亮。不要全局放开女装的照明规则；同时检查 scene_reference_adapter、蓝图、首帧和视频渲染是否仍注入相反风格限制。

穿搭只投影实际入画的袖口、领口、肩部或发型。可以沿用共享人物/穿搭的来源与身份记录，但不强制生成不可见的全身人物剧情，不要求运营新增完整人物设定。

**7. 批次规划与内容容量。** 目标是增加可感知的原创差异，不建立“排列数足够就代表内容足够”的笛卡尔积系统。

规划步骤：

1. 读取当前权威商品锚点、已确认卖点及已冻结/允许使用的市场语境。
2. 在现有内容候选能力上整理主题：观众问题或观察目的、唯一核心价值、相关可见证据、适合的局部佩戴关系。
3. 给每个有效主题分配主稿；有实质视觉差异时才添加同主题实验稿。
4. 联合选择兼容的 MIXED 叙事结构、整片模板、核心细节、首镜和局部搭配。
5. 对同批预留项和同产品历史执行相似性比较；去除完全相同执行，软排序近似内容。
6. 冻结计划，再进入单条蓝图与口播。模型不得在写作时重新选择模板、主题或光影。

例如有五个成立主题时，可以形成五条主稿加五条实验稿，共十个候选。报告必须写“五个主题、十个执行候选”，不能写成十个独立卖点。二十条同样由真实内容容量决定，不规定每产品必须五个主题、每主题必须两条。

差异优先级：内容重点/观众问题 → 首镜观察关系/核心证明/佩戴关系 → 模块权重和节奏 → 环境、音乐与措辞。背景变化、句子同义改写、随机 seed 变化不构成新的主题。

同主题且核心视觉相同、只换背景或措辞的候选标记 NEAR_VARIANT。对完全相同执行不追加新的付费候选；对可用但相近的执行报告差异，不自动反复调用模型润色。候选不足复用现有部分容量结果机制，明确 requested/planned/ready/failed 及未补足原因，不能把“少规划”记成“模型失败”。

建议每条内部保存 parent_theme_id、candidate_role（PRIMARY/EXECUTION_VARIANT）、nearest_item/script_id、difference_summary、difference_dimensions。均属于版本化 JSON 扩展建议，不要求增加运营表字段。

同产品不同语言的同一视觉方案，仍视为同一视觉家族，不能因翻译而重置历史。语义比较可以按目标市场区分，视觉历史至少跨同商品可见；不同商品版本应保存版本关系，不把旧颜色或结构继承到当前 SKU。

复用 creative_pattern_usage.metadata_json、已有批次/条目 JSON 与状态机制，不新建并行的创意历史库。历史窗口做可配置的有界读取并冻结快照；明确 PLANNED 预留、READY、FAILED 的影响，重试同 item 不重复扣容量。不要宣称某个相似度阈值对应平台审核线。

仅靠模板编号哈希不足以判断感知差异。无新增语义模型时，先比较结构化主题、证据 ID、开场构图、核心细节、佩戴区域/搭配关系，输出需人工判断的相近项；不可伪称已做视频感知去重。

**8. 一份冻结合同贯穿下游。** 建议在现有 frozen_direction_package_json 的 category_execution_extension 内增加一个版本化 mixed_template_contract；其他字段保留既有权威，不复制成第二套商品事实数据库。

以下为结构建议，不是当前可直接执行的 JSON：

    mixed_template_contract:
      schema_version: accessory-mixed-template-v1
      source_mode: AUTHORED_TEMPLATE
      execution_profile: ACCESSORY_MIXED_TEMPLATE_V1
      template_id: AMX_A_WORN_FIRST
      template_version: 1
      content_theme:
        theme_id: <真实规划主题ID>
        parent_theme_id: <主题族ID>
        candidate_role: PRIMARY | EXECUTION_VARIANT
        thesis: <本条唯一内容重点>
        approved_claim_refs: [<既有卖点/事实引用>]
        evidence_refs: [<已有商品证据引用>]
      global_carrier: MIXED
      face_policy: NO_FACE
      audio_route: VOICEOVER_POST
      environment_recipe_id: WINDOW_LIGHT_WOOD
      environment_recipe_version: 1
      product_identity_ref: <现有商品锁与图片哈希快照引用>
      local_body_style_ref: <冻结肤色/袖口/发型等局部设定引用>
      capture_units:
        - unit_id: CU_01
          duration_seconds: 3
          module: WORN_DETAIL
          carrier_mode: WEARER_ACTIVE
          structure_role: <已冻结叙事角色>
          framing: <类目局部裁切>
          face_policy: NO_FACE
          observation_job: <本镜观察目的>
          product_state: ALREADY_WORN
          body_zone: <EAR/WRIST/FINGER/HAIR>
          action_ref: <类目安全动作>
          evidence_refs: [<本镜可观察事实>]
          edit_before: START
          continuity_group: CU_01
        - unit_id: CU_02
          duration_seconds: 3
          module: HANDHELD_PRODUCT
          carrier_mode: HAND_ONLY
          product_state: HELD
          edit_before: DIRECT_CUT
          continuity_group: CU_02
        - unit_id: CU_03
          duration_seconds: 4
          module: STATIC_PRODUCT
          carrier_mode: STATIC_PRODUCT
          product_state: RESTING_ON_SURFACE
          edit_before: DIRECT_CUT
          continuity_group: CU_03
        - unit_id: CU_04
          duration_seconds: 5
          module: WORN_RELATION
          carrier_mode: WEARER_ACTIVE
          product_state: ALREADY_WORN
          observation_job: <与CU_01不同的佩戴关系>
          edit_before: DIRECT_CUT
          continuity_group: CU_04
      difference_report:
        nearest_script_id: <可空>
        difference_dimensions: [<实际变化>]
        difference_summary: <与最相近脚本的可见区别>
        review_status: PLANNED_ONLY

后面三个 capture_units 为缩写示意，正式合同同样必须填写 structure_role、framing、observation_job、动作和证据等必要字段。字段应按当前 schema 精简归并；不要为了照搬本文建立重复合同。

全片 MIXED、每镜 carrier、物理状态和 face_policy 是不同维度。腕部/手指局部已佩戴可保留 WEARER_ACTIVE 的证据语义，同时用取景裁切禁止脸；不要把一切不露脸画面都降为“纯手持、不能证明佩戴”。MIXED 能容纳佩戴证明，不代表任意镜头都能证明任意卖点，例如不展示脸部整体不能证明“显脸小”。

全片固定商品身份；每个连续镜头固定物理状态；明确 DIRECT_CUT 后可从佩戴切到手持、静物再切回佩戴。跨镜不生成摘戴过程，镜内不发生瞬间转移。已有“全片不重做佩戴”类提示应按本模式重写语义，不能机械禁止这种展示蒙太奇。

storyboard、capture_units、voiceover shot_plan、video_generation_brief 与首帧都要保留逐镜承载。当前首帧通过开场 storyboard 取 carrier，缺失时不能把全片 MIXED 代替开场状态。同一信息只有一个权威来源，其他表示由代码投影。

特别注意二次编译：assemble_simplified_complete_script 会重建 capture_units，production_script_renderer.py:1434 附近还会从 storyboard 再次编译。新字段只放在临时 capture_units 会被丢弃。要验证序列化、重载、assembly、首帧和二次渲染后都保持相同语义；历史小饰品近景→真人动态→回收投影不能二次覆盖新模板。

**9. 模板来源与结构权威。** 本项目允许人工设计的固定拍摄模板，属于用户本次明确的新需求。应增加有边界的 AUTHORED_TEMPLATE 来源模式，不将其伪装成真实视频观察或绕过所有旧结构规则。

模板选择必须发生在结构冻结之前或与之联合选择。不能先让旧路由锁定 WEARER_ACTIVE/自拍，最后在视频提示词里硬改为手持静物混剪。尤其要检查 run_plan_only 上游在“无真实执行案例”时是否提前退出；只在 allocator 尾部添加模板不能解决入口被拦的问题。

优先复用兼容的 MIXED 结构合同，将四个物理模块映射到合法的 structure_unit_roles。真实执行案例存在时只作为兼容软参考，不覆盖已选模板与当前商品；真实案例不存在时允许新模式使用明确登记的人工模板计划，按模板来源进行完整性校验。

如现有结构合同要求真实视频 ID、reference_spine_orders、cluster_id 等，新增来源分支或兼容适配，不填写假的 video_id、观察顺序或实测标签来骗过校验。模板计划可以有自身真实生成的 selection/direction ID，但不得声称来自 RDS 真实样本。不要修改全局旧来源验证，也不要破坏阶段 0 对真实观察的要求。

已核实 original_batch_executor.py:705 附近按 simplified_v1 开放 allow_structure_only，应优先复用，避免另建路由器。core/structure_execution_compiler.py 已有 MIXED 排程和 apply_structure_execution_plan 写回能力，但排程并非本次 A/B/C。若新模式仍经过该编译器，应输入模板兼容的冻结计划，防止旧排程在下游重新写回 carrier、beat 或 continuity。

保留现有叙事结构权威，整片顺序模板只拥有物理展示编排权威。商品事实和卖点仍来自原权威来源，光影、搭配和场景作为 CREATIVE_DESIGN 保存，不能回写成商品事实或真实观察。

**10. 建议代码改造位置。** 新模式优先作为 simplified_v1 下的 execution_profile，不新增一套脚本引擎。BatchRequest.script_mode 当前只接受 legacy_v2/simplified_v1，不能直接塞新值后被悄悄归一成 legacy_v2。

| 模块 | 改造任务 |
| --- | --- |
| core/original_batch_models.py | 为配置快照/输入哈希提供新档入口；复用现有 1–20 数量能力 |
| core/original_batch_executor.py:run_plan_only | 早期解析模式，接入模板来源和内容容量，避免旧案例门槛提前阻断；复用预留/续跑 |
| core/original_batch_allocator.py:allocate_batch_items/_make_item | 联合选主题、MIXED 结构、模板、光影、局部搭配，冻结差异与来源 |
| core/category_execution/accessory.py | 按每镜承载生成动作、局部取景、物理状态和数量指引；修复刚性耳饰展开等冲突 |
| core/structure_execution_compiler.py | 联合结构与模板的逐镜排程，确保写回操作不覆盖新模板，保留原Beat顺序 |
| core/simplified_complete_script.py | seed、prompt、schema、normalize、capture_units、校验、口播投影、assembly 全链路消费逐镜合同 |
| core/visual_execution_contract.py | 补饰品专用自然精致光影投影，不继承围巾默认 focus |
| core/scene_reference_adapter.py | 新档中保持冻结环境一致，不把普通现场光禁令重新注入 |
| core/first_frame_contract.py | 从实际首镜读取 module/carrier/state/crop；统一标准类目；删除与新档相反的类目指引 |
| core/production_script_renderer.py | 确定性渲染每镜，只输出执行信息；确保无自拍讲话/半脸规则回流 |
| core/json_parser.py、core/script_renderer.py 及相关恢复逻辑 | 按 AGENTS 要求检查 schema 兼容，新字段不丢失；不相关旧路径保持行为 |
| core/storage.py、core/original_batch_storage.py | 复用 metadata_json、方向包和检查点；仅在确有必要时做可回退迁移 |
| scripts/run_original_batch.py:_render_complete_scripts_markdown 及现有报告构建逻辑 | 输出主题/候选数量、主稿/实验稿、差异、容量不足、模板与生成验证状态 |
| script-run-manager-sync | 核对真实首帧/原图选择、15秒分流、口播和提示词是否无损交接；最小修改并保留并发改动 |

可将少量模板定义和纯编译函数独立成 config/accessory_mixed_templates.json、core/accessory_mixed_templates.py 等文件；这些名称为建议，避免将上千行新规则继续堆入 accessory.py。不要建立第二套动作生成模型、卖点引擎、口播引擎或创意数据库。

模板模式需要独立的拍摄指导，允许后置/固定机位局部记录和画外旁白，不再继承 CREATOR_SELF_SHOT 的对镜讲话。若现有 capture_mode 枚举必须新增值，应完整更新 schema、归一化、渲染、缓存和测试，不能仅替换展示文字。

**11. 关键帧和媒体生产分阶段接入。** 当前 15 秒路径不能被假设为已支持四张逐镜首帧或四个独立付费片段。现有单首帧、原图回退、运行管理表和生成后配音必须逐项核查。

第一阶段交付文本、冻结合同、整片首帧与完整视频提示词；另提供四镜头关键帧提示词/审阅清单，明确哪些只是计划、哪些实际生成。通过现有媒体入口测试单次 15 秒请求是否能遵循四镜混合展示。不要将四张图拼成宫格后冒充当前下游支持的逐镜参考。

当前原创参考交接见 script-run-manager-sync/core/original_batch_source.py:120：未勾选生成首帧时沿用商品图，勾选且就绪时返回 composite，未就绪则阻塞。core/first_frame_handoff.py:91 的补充人物/商品参考逻辑明确限定成功脚本复刻，不能假设已覆盖原创。开发必须核对实际视频通道是否支持额外商品图/局部身体参考及其角色，记录实际提交资产；不能只有脚本文字写“原图拥有权威”，实际却只提交一张丢失细节的合成首帧。不能自动替用户勾选生成首帧，也不能声称只有静物首帧便已锁定后段的手部肤色、发型和所有商品细节。

若单次生成经真实验证仍明显跨镜变款、混淆承载或无法遵循顺序，再进入独立片段路线：每镜参考帧→短片段→直接剪辑→整段旁白与音乐→成片检查。该路线需要单独的执行适配与成本核算，不能强行把 15 秒任务送入长视频 Plan C。

独立片段模式若实施，必须具备逐片段任务 ID、幂等指纹、模型/参数/参考图内容哈希、续跑与合并记录；成功片段可复用，提交状态不确定先查远端，不自动重付费。整片仍对应原生产脚本一行，不给运营增加四次确认。四个短片段不等于四条独立脚本或四份不同口播。

图片和视频模型沿用当前已配置线路，不在本项目顺带迁移模型。逐镜关键帧共享商品原图权威与环境设定；合成帧只辅助构图，不能覆盖原始商品结构。运行成本以实际调用与成功片段记录为准。

**12. 音频、缓存和上线边界。** 口播沿用中央服务，唯一核心主题跨镜表达，已批准事实之外不补功效；字幕/口播目标语言与中文对照继续按现有交接。声音默认画外旁白，不需要脸部说话或口型。音乐和音量走当前有授权的后处理，不另建下载流程，也不为每镜独立生成一段互不相干的文案。

新模板、主题、逐镜计划、光影/局部造型版本和参考图内容哈希必须在规划冻结前进入有效输入指纹。只在末端更新渲染器不能让旧缓存冒充新模式。

保持原 request_id、batch/item 身份规则及幂等语义；不能在 API 调用失败后随机改 request_id 或 seed 重开付费任务。执行过的旧批次继续按其冻结合同 resume，不自动套新模板。需要新设计时明确新建或 replan。

视觉与口播继续按依赖分开缓存：视觉变化应使对应首帧/视频失效；纯声音选择不应重做商品画面；主题变化影响语义输入，不能复用旧口播。字段改名和运营展示名称不应无意义触发重生成。开关关闭仅影响新规划，已有新模式冻结批次应可读取、审计与续跑，或明确报告不兼容，不得静默降为旧自拍。

建议新增模式开关 ORIGINAL_SCRIPT_ACCESSORY_MIXED_TEMPLATE_V1_ENABLED，默认关闭，仅对标准类型白名单、15 秒、目标分支的新任务生效。名称为建议。灰度入口和运营选择沿用现有任务能力；必要时只增加一个“拍摄模式”选择，不要求填写逐镜 JSON，不新增运营审核表。

**13. 校验与批次审阅。** 尽量扩展现有验证和报告，物理/事实/交接硬错误与审美、差异提示分开。不要引入多个新质检模型和无限修订循环。

| 类型 | 处理 |
| --- | --- |
| 必要合同缺失、三种模块缺项、时间轴错误、模板与逐镜承载矛盾 | 模型前发现则终止该候选规划并明确错误，不进入付费生成 |
| 未授权事实、错误商品版本/数量、明确露脸动作、错误身体区域 | 接入现有单条校验/修订边界；不能标可交付 |
| 文案相近、细节重复、首镜弱、环境略单调 | 规划软排序或审阅提示，不触发无休止自动修订 |
| 图片/视频里的手指错误、商品变形、真实露脸 | 只有实际媒体检查才能给结论；未检查标 NOT_REVIEWED |
| 成片仅视频流无音频、时长异常、片段缺失 | 实际媒体交付检查失败，不能标成片完成 |

文本中写着“无多指、无露脸”不代表实际画面已通过。不要用正则扫描所有负向规则中的“脸”“手”等词来判违规，要检查执行字段和实际媒体；镜面也属于可见范围。

建议复用现有报告输出：requested_count、planned_count、ready_count、failed_count、theme_count、primary_count、variant_count、capacity_reason、nearest_script_id、difference_summary，以及 keyframe/video 的实际生成与审阅状态。模板图像审阅、成片可用性和发布表现分别记录，不能互相替代。

**14. 开发任务与交付顺序。** 建议分为可独立审阅的提交，不以一次大改覆盖全部路径。

| 工作包 | 交付内容 | 通过标准 |
| --- | --- | --- |
| WP0 基线 | 确认入口、现有差异、测试环境和受影响文件；构造隔离 fixture | 不改生产数据；留下基线与未提交改动说明 |
| WP1 模板合同 | 三种整片模板、四类局部规则、三套光影、逐镜连续性 | 四类均可离线编译合法四镜计划；旧模式不受影响 |
| WP2 批次与来源 | AUTHORED_TEMPLATE 权威分支、主题/实验稿、历史差异、容量报告 | 请求10/20可返回有依据的不同候选或部分容量；无虚假观察来源 |
| WP3 文本闭环 | allocator→seed→blueprint→normalize→口播投影→brief→renderer | 每镜承载、主题、光影和不露脸规则无丢失或矛盾 |
| WP4 首帧与生产交接 | 实际开场首帧、逐镜审阅计划、运行管理表交接、声音路由 | A/B/C首镜正确；15秒不误入Plan C；只同步已授权状态 |
| WP5 实测与评估 | 经授权的小样媒体验证、同产品10条差异审核、成本记录 | 明确实际成功/失败、人工审阅和待验证项，不凭文本宣布质量通过 |
| WP6 条件扩展 | 仅当单次15秒路径实测不足，再实现独立片段生成 | 幂等、续跑、局部重做、合并与音频完整，无重复计费 |

WP1–WP4 构成首期开发交付；WP5 是媒体上线验证，未得到实际结果前应写待验证。WP6 是有证据再执行的扩展，不是本次默认重构目标。不能因为不能提交付费媒体，就把已授权的离线实现也停下。

**15. 测试与验收。** 修改应补有意义的合同和集成回归，不仅检查字段等于实现常量。mock 模型、RDS、飞书、媒体和付费提交，使用临时数据库；任何导入带初始化副作用的模块先检查数据库路径，不能让单测连接正式库。

必须覆盖的验收场景：

1. 耳饰、手链、戒指、发饰 × A/B/C：均含手持/静物/佩戴，四镜总时长15秒，身份和局部状态正确。
2. 首镜静物耳饰没有半脸/人体指令；首镜手持耳饰没有展开局部；首镜佩戴没有眼鼻嘴/对镜说话指令。
3. claw_clip、hair_clip、hair_pin、earring、bracelet、ring 等规范类型通过同一解析；刚性腕饰与细链手链不互相冒充。
4. 合法直接切镜 WORN→HELD→STATIC→WORN 通过；同一连续镜头瞬移/变款被识别；既有女装穿戴连续性不被放松。
5. 一对耳饰可单耳入画，但不能复制成三只；只手持或静物不能被标为佩戴效果证明。
6. 商品细节未知时选择已知视角；不编背扣、真实材质或精确尺寸；缺真正核心证据时报告容量不足。
7. 请求10、20时不靠背景/BGM/seed膨胀独立主题；主稿/实验稿可区分；当前批次与历史完全相同执行不重复入队。
8. 无真实参考视频时新模式合法采用人工模板来源；旧真实观察路径仍保持原证据校验；无假 video_id/cluster_id。
9. 归一化、口播 shot_plan、capture_units、brief、首帧与最终提示词保留相同逐镜承载，不统一覆盖为 MIXED 或 WEARER_ACTIVE。
10. 同请求resume不改冻结计划、不重复预留、不重复调用已完成阶段；模板/参考图内容改变触发正确失效；旧缓存不冒充新版本。
11. 模式关闭、女装、围巾、长视频、种草、旧S1–S4输出路径保持既有行为；默认任务不被全局改为饰品模板。
12. 生产交接保留已确认口播、正确参考资产和15秒分流；未勾选进入生产不执行；实际媒体审核未运行不能显示已通过。

现有测试文件可优先扩展：tests/test_category_execution_adapter.py、test_original_batch_allocator.py、test_original_batch_executor.py、test_original_batch_report.py、test_simplified_complete_script.py、test_structure_execution_compiler.py、test_production_script_renderer.py、test_multidim_reference_adapter.py、test_visual_execution_contract.py、test_first_frame_contract.py、test_first_frame_tasks.py。同步侧优先 test_original_batch_source.py、test_first_frame_handoff.py；若调整额外参考图再覆盖 test_reference_manifest_handoff.py、test_sync.py；若调整分流再覆盖 test_openclaw_original_batch_sync.py、test_production_route.py。接手时核对实际测试命令和依赖，再运行受影响集合；不要为省事直接跑可能访问正式库的全部脚本。

离线测试建议命令（从原创开发源执行；命令未在本次文档任务中运行）：

    python3 -m pytest -q tests/test_category_execution_adapter.py tests/test_simplified_complete_script.py tests/test_structure_execution_compiler.py tests/test_visual_execution_contract.py tests/test_first_frame_contract.py tests/test_production_script_renderer.py tests/test_original_batch_allocator.py tests/test_multidim_reference_adapter.py

修改执行器、缓存或报告后追加对应 test_original_batch_executor.py、test_first_frame_tasks.py、test_original_batch_report.py。同步模块从其自身开发源目录单独执行：

    python3 -m unittest discover -s tests -p 'test_original_batch_source.py'
    python3 -m unittest discover -s tests -p 'test_first_frame_handoff.py'

两个模块均有顶层 core 包，测试分进程运行，避免 Python 导入缓存串包。使用 unittest 时确认没有漏掉 pytest 风格的函数测试，不能把“收集0项”当作通过。

以上是否可完全离线取决于接手时测试内容，应先检查 fixture/mocks；使用项目现有解释器环境，不为了本任务升级依赖。新测试可能单列一个模板合同文件，但不建立第二套测试框架。

媒体验证两轮：第一轮四类各一商品、同模板同环境，每个方案两次，共八条完整视频，先看基本稳定性；第二轮选参考完整且内容容量较高的一件商品，规划十条候选，审阅后仅生成有意义差异的方案。扩二十条由结果决定。测试输入应保存参考图哈希、商品版本、主题、模板、模型参数和随机配置，才可复现；八条样本不能代表长期成功率。

**16. 接手时的工作区注意事项。** 2026-09-14 核对时，下列文件已存在其他任务的未提交改动，本文没有修改它们。列表仅为快照，接手必须重新查看 git status/diff，保留既有工作；不能 reset、覆盖或把并发改动全部提交进本项目。

已修改：

    skills/original-script-generator/core/longform/audio.py
    skills/original-script-generator/core/longform/storage.py
    skills/original-script-generator/core/longform/voiceover.py
    skills/original-script-generator/core/longform/workflow.py
    skills/original-script-generator/scripts/run_feishu_longform_production_tasks.py
    skills/script-run-manager-sync/scripts/openclaw_original_batch_sync.py
    skills/script-run-manager-sync/tests/test_openclaw_original_batch_sync.py
    skills/script-run-manager-sync/tests/test_production_route.py

未跟踪：

    skills/original-script-generator/core/longform/subtitles.py
    skills/original-script-generator/tests/test_longform_audio_modes.py
    skills/original-script-generator/tests/test_longform_feishu_production.py
    skills/original-script-generator/tests/test_longform_storage_migration.py
    skills/original-script-generator/tests/test_longform_subtitles.py

**17. 给接手模型的执行摘要。** 请先核对现有代码，沿上述工作包增量实现，不重新进行已确认的产品方向讨论。用户已确认的是“同条视频混合手持/静物/不露脸佩戴，单品10–20个有实际差异的候选，真实精致的固定模板，暂停女装优化”。首期复用 simplified_v1 和现有15秒生产入口，通过显式饰品模式增加模板来源、逐镜承载和批次主题分配。

优先修复已定位的相反指令，保证一份冻结合同贯穿规划、蓝图、首帧、口播投影和最终提示词。不要通过新增一套庞大的prompt规则、末端全文改写、虚构观察来源或强制凑满数量解决问题。

开发交付应包含：变更文件和理由、版本化模板定义、四类示例与同产品十条批次审阅包、实际运行的离线测试结果、开关与回退方法、幂等/历史缓存说明、真实媒体验证状态、尚未验证的限制。将实现完成、离线通过、真实生成、人工审阅和发布表现分别报告。
