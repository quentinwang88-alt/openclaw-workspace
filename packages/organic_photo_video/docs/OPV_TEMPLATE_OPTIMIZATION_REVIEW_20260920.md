# 模板优化实施复审与下一步建议

日期：2026-09-20。审查提交：26c5d23～273ffd4；仅评审，未改生产代码、配置、飞书记录或发布状态。

## 结论

部分通过。最新配色真实生产样片已使用侧栏色卡，温度样片的穿脱动作与文字也有实质改善。但“教程结构全链贯通、中文与画面完全同源、自由围巾QA闭环”仍未完成，不宜按交付报告的“四个修复全部真实验收”口径扩大新教程／自由围巾自动生产。既有稳定展示流程不需暂停。

下一轮应是小范围断点修复与版面收敛，不需要换模型、增加模板种类、增加运营必填字段或逐篇人工审核。

## 审查证据与范围

- 阅读本轮diff、开发交付报告和真实规划／素材／翻译缓存。
- 配色样片 recvvK1eGxPvNm：逐张查看4张最终图；源图SHA匹配最终任务 opv_task_20260920_94aa4dea31c8 的4张复用素材。
- 温度样片 recvvJiswTWVUO：查看第2、3张最终图；源图SHA匹配任务 opv_task_20260920_e6c8dd28b159 的4张复用素材。
- 查看 /tmp/gc_relayout/guide371_p2.jpg 并审查离线重排脚本。
- 本次独立运行7组相关回归：186 passed、84 subtests passed；12条既有Pillow弃用警告。不以此宣称全仓1574测试均由本次复跑。
- 未重新调用付费生成，未重新读取飞书实时行；记录与成片对应由本地冻结资料和素材哈希交叉验证。无商品围巾真实样片仍未验收。

## 关键发现

### F1：教程画面证据在真实投影中丢失（P1，自动教程扩量前修）

`services/photo_reference_vision.py:1721–1732` 的 build_travel_style_profile 组装 recommended_sets 时未复制 post.narrative。`photo_content_planner.py:860` 从 recommended_sets 读 narrative，`photo_style_reference_supply.py:519` 再从 item.narrative 读 visual_basis。因此虽有上游叙事对象，真实生图的新增“本页讲解画面证据”通道仍是空的。

实数据复现：recvvK1eGxPvNm 的 travel_plan.posts[0] 有 narrative；经过真实 build_travel_style_profile 后 recommended_sets[0] 无 narrative，冻结计划 item 也无。copy.pages 仍能进入渲染，所以“成功出了色卡”不能证明画面证据已接通。原look/scene提示仍可能让模型生成正确画面，不能反过来将每张正确样片当作新增通道已生效。

修复：保留每篇narrative及其source_role，经过真正的profile投影后进入item与供图；与pages只维护一份规范化结构，避免继续多处手动复制。单测必须走 normalize → build_travel_style_profile → plan_th_choice_batch → 实际ShotGenerationRequest，并断言证据出现在请求中。现测试手工recommended_sets=posts绕开了丢字段环节。

### F2：最终排版和中文／文字质检不是同一份文字（P1）

`services/copy_translation.py:23–33` 只取title/caption/hashtags/slide_texts；`photo_package.py:588–589` 最终页面QA也仅取slide_texts。新renderer却读取page_text（来自copy.pages）。代码未将最终pages确定性投影成slide_texts，只在prompt要求二者一致。

真实配色封面的pages.body含“灰粉与浅驼如何衔接奶白外套”的完整解释，slide_texts没有，中文缓存 de4e8a4439203052efc18a523b371744 也未逐页翻出该句。仅改pages.body，现翻译指纹保持不变，缓存不会重译。色卡旁文字同样不在完整中文来源中。

修复：新教程的最终结构化页面文字作为唯一权威；兼容slides、最终文字QA、中文翻译及缓存指纹均从它派生。纳入引子、标题、解释、可见单品／颜色标签；文案修正后同步投影。旧无pages任务仍走原流程。无需另加模型审查轮次。

### F3：温度只改了提示词，未进入教程版式（P2）

`photo_content_planner.py:470–475` 的guide判断仍只有旅行攻略与配色教程。`photo_reference_vision.py:2454` 只对TEMPERATURE且expression为空改逐页规则，仍要求五段封面＋ABCD；显式PRACTICAL_GUIDE走后面的旧分支。

recvvJiswTWVUO的冻结copy没有pages，最新成片仍是旧底部叠字。第2张确实已脱外套拿在臂上，第3张确实在咖啡馆脱下外套，属于内容改善；但不能据此说独立解释区已在温度生产链生效。

修复：集中解析执行类型，温度＋默认实用表达／显式PRACTICAL_GUIDE均进入四页guide结构；显式STYLE_INSPIRATION保留展示。区分temperature_guide与color_tutorial，不能只把枚举塞进集合后让现有else分支误标为color_tutorial。不改独立冷热切换三态流程。验证真实入口后用本次温度源图重新排版即可。

### F4：色卡侧栏有具体文字错误，排版适配不完整（P2）

- `photo_structured_layout.py:369–372` 把accessories硬编码成泰语“围巾”；最新配色第4张实际讲粉色袜子呼应，第三色卡却写围巾。
- 同一映射把所有内搭标成เสื้อใน，这个用词容易落到内衣语义；下装无论裤裙都写“裤子／裙子”，缺乏当前单品的准确描述。
- `:438–440` 的kicker不测宽不换行，只按字符截取。真实封面测得bbox右边界1071，设计安全边界1048，已挤入右侧边距。
- 侧栏chip标签也不换行；`:455–457` 标题高度重复计算，且侧栏缩字循环耗尽后仍继续绘制，没有与底部布局一致的溢出处理。
- 视觉上右侧内容集中顶部、下方大面积空白，HEX代码占用了读者注意力。主体与文案各自成块，暂时更像资料卡而非成熟配色教程。

修复：冻结每块色卡的实际单品名和发布语言标签，例如“粉色袜子”，而不是用accessories推断具体商品。缺具体名时用中性标签，不猜成围巾。HEX保留内部审计，默认不展示；色块配自然颜色名／单品名即可。所有可见文字共用真实bbox、换行和有限缩字逻辑。主问题升为主标题，kicker仅承载短信息；调整现有侧栏对齐／留白，不新增一套模板系统。保留完整人物与鞋脚，先用现有源图重排。

### F5：自由围巾QA上下文仍被白名单删除（P1，围巾扩量前修）

`photo_style_reference_supply.py:45–62` 的 _product_qa_context 只复制商品字段，不包含新增task_category_key。其结果在`:985–996` 传给review_travel_pages。后者虽已支持自由围巾required_item_visible，但实际收不到类目。

离线调用 `_product_qa_context({'task_category_key':'scarf'})` 返回空product_context；接收侧adapter解析为None，required_item_visible没有启用。

修复：显式保留任务类目上下文，仍以product_id判断是否有指定商品。新增从真实supply调用捕获reviewer参数的测试，模拟缺围巾返回false并验证有限修复；不能只直接调用QA helper证明规则存在。随后在隔离验收入口跑一条无编码围巾旅行，不必为了验收批量启用当前disabled预设。

## 已改善与暂不扩展的部分

- 新配色成片已真实采用色卡侧栏，四套有浅色衔接、浅深对比、粉色上衣与袜子呼应等可见关系；第三张蓝色内搭清楚可见，较之前被外套遮住有改善。
- 温度内容已能执行真实穿脱与室内外切换，应该保留，不需要推翻重新规划。
- 本次关键回归未见旧四选一、MX发型、拆解、冷热切换测试退化。这里只是所运行测试范围的结论，不等于所有生产账号已重新验收。
- MX头发标签小修、更多模板样式、人物包重构和模型切换继续后置。

## 验收方式需要纠正

`scripts/relayout_guide_samples_20260920.py` 手填页文案与色卡，并直接调用renderer。它只能验证渲染能力，不能验证生产接线或最终文字同源。guide371_p2样张画面是条纹上衣＋牛仔裤，手填解释却是米白上衣＋米白裙装，不能作为“图文依据一致”验收。

用冻结真实pages与对应source_role调用实际exporter；缺结构时将样张标为版式demo，不宣称全链完成。测试不要再复制生产代码片段来断言该片段正确。

## 最小下一轮

1. 接线修复：F1、F2、F5；顺带统一F3的主题执行类型。新增少量跨模块测试，覆盖真实profile投影、QA参数和中文缓存。
2. 排版修正：F4；同一套配色4张、现有温度4张零生图重排。准确标签、完整中文、无越界优先，视觉比例微调其次。
3. 验收收口：一条新教程真实入口（只在现有源图不够时生图），一条自由围巾隔离样片；检查多篇任务各自叙事而非继承第一篇。无须每个模板全量重跑。

完成标准：实际请求包含本页画面证据；渲染／中文／最终文字QA同源；温度实际走目标布局；围巾缺失观察项在真实调用中存在；色卡标签对应真实单品。完成后再扩大新教程供稿比例。
