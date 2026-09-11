# 长视频主脚本：Astra与商品相关画面（2026-09-10）

仅20–45秒Plan C生效。主脚本默认Astra/high，中央口播保持已有Astra线路，15秒视觉蓝图、图像和H3模型不变。固定作者模型不暗中跨模型回退；如失败，可使用既有入口显式指定Sol或Terra。

主脚本仍复用OriginalScriptLLMClient的既有API/CLI传输；Astra的CLI备用路径复用已用于中央口播的应用内新版codex。其它调用方未传覆盖时仍使用原CLI，未全局更换运行环境。

## 本次做减法的部分

- 不再要求每镜必须提供商品证明。生活连接镜头可如实标记情境承接；中段要让购买理由转化为观众看得见的效果，而非用information_gain给看手机、走路等动作附会意义。
- 身份与单品保持一致不等于全片穿着状态不变。明确直切后按该单元脚本呈现状态；连续边界仍延续实际尾帧，不无故重做已完成动作。
- 细节投影可收紧景别，但不把作者的动作替换为“小幅姿态调整”。首帧与视频提示词采用同一段首单元的画面/动作，独立进入帧不抄K0姿势和敞合状态。
- 不加动作库、不加每段动作配额、不加逐镜口播绑定或自动审稿循环；现有分段、商品身份锁和生产安全检查不变。

## 生效与验证

运行入口包括飞书运营任务、OpenClaw适配器和run_longform_original，默认模型一致。显式`--blueprint-model gpt-5.6-sol`或`gpt-5.6-terra`仍有效。主脚本generation_provenance记录请求模型、推理强度和prompt_version，不接受模型自行声称的血缘。

已完成任务及resume复用已有主脚本，不因升级而自动重写、重生成视频或增加费用。新任务/replan才使用新作者要求；不要用旧稿resume检验新模型效果。

离线回归：`python3 -m unittest tests.test_longform_product_led_writer tests.test_longform_original tests.test_longform_relaxed_visibility tests.test_longform_frame_authority tests.test_longform_feishu_workbench tests.test_openclaw_original_script_task`。

下一轮先同一商品、同一冻结图片/卖点/人物/穿搭生成两条全新文本。不先花视频费用。人工看三点：去掉信息增量解释后中段是否仍看得出商品价值；动作有无从作者到执行/首帧被改写；两条是否仍以看手机、走路和背影为主要差异。达到后再各生成一条视频；模型升级不等于已证明成片改善。
