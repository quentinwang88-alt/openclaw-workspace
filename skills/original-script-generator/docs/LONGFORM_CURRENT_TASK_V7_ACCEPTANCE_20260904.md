# 长视频当前任务与共享资源接线验收

日期：2026-09-04。范围：开发、离线回归、本地穿搭库只读核查。

## 实施结果

- 新运营长视频任务/replan从当前附件取得商品身份，再调用共享PLAN_ONLY。历史短脚本不再自动覆盖商品、穿搭、人物与场景。
- 全批来源在模型前冻结为`batches/<batch_id>/source_plan.json`。来源不足不取模复制；模型失败仍保留来源可续跑。任务输入变化拒绝旧版本续跑；旧job无输入快照时要求replan。
- 当前商品图缓存验证附件身份、文件存在性与SHA256。冻结素材再次检查已分析参考的内容指纹。当前任务有图但没有可冻结商品原图时，在脚本模型前报告输入失败，不悄悄借历史合成图。
- 选中人物参考沿既有下载器缓存，并以PERSONA_REFERENCE传入冻结清单。引用图不传给文本模型，首帧阶段继续按商品/人物分权。
- 穿搭同步与读取清除明确不可见产品ID字符；已有颜色适配字段仅供选择，长视频显式启用。同色/未限定色共同轮换，明确他色排除。完整合同留审计，下游只消费已选穿搭执行投影。
- 本批/历史穿搭用量复用原规划账本，完成只更新原记录。输出显示实际模板、配饰、关联状态及候选/使用报告。
- 进入帧与H3分别使用本段场景，不继承A段卧室背景。长视频近景不再强制全身，后配旁白静口；15秒默认首帧行为保持。
- 中央口播重新解析钩子、批准样本、原生表达及关系语言，修订复用资源快照；保留长视频多卖点，不传整份穿搭审计。
- 有效语音长度去掉可信首尾静音，内部停顿保留；片段边界最多±0.75秒微调，保留一次时长修订。大幅内容不足仍软提示，不声称通过布局就能解决文案不足。

## 验证

原创相关14个测试模块共126项通过，轻视频同步模块33项通过，共159项。包括真实本地ffmpeg静音检测与合成测试；模型、中央资源访问与飞书均mock，无外部生成。

```bash
python3 -m unittest tests.test_longform_original tests.test_longform_feishu_workbench tests.test_longform_current_task_sources tests.test_longform_frame_authority tests.test_longform_outfit_projection tests.test_longform_voice_resources_audio tests.test_first_frame_contract tests.test_outfit_template_provider tests.test_operation_product_bootstrap tests.test_operation_runner_lock tests.test_operation_runner_interrupt tests.test_feishu_operation_replan_request tests.test_production_script_feishu tests.test_openclaw_original_script_task -q
```

在lightweight-tryon-video目录：

```bash
PYTHONPATH=scripts python3 -m unittest tests.test_feishu_sync -q
```

产品1737141103233042426本地共享库只读验证：

- 修复前可读3套；修复后关联8套enabled模板，testing模板未启用。
- 白色兼容池为STYLE_0017、STYLE_001、STYLE_002、STYLE_004；另外4个显式他色模板被排除。
- 一组确定性批内选择为STYLE_0017→STYLE_002→STYLE_001。该结果验证选择器，不代表已生成3条正式脚本。
- 本轮真实旧master/plan只读重编译：B进入帧不再含卧室，保留大堂；K0仍为正确的A地点。

## 部署与限制

`~/.codex/skills/original-script-generator`为workspace开发源软链接，因此代码修改已对安装入口生效，无需复制覆盖。穿搭脏ID通过读取兼容立即生效；下次正式refresh-outfits才清洁写入共享缓存。

本轮没有刷新/修改飞书、没有重写已有脚本/job，没有调用外部模型/TTS/图片/视频服务，也没有提交Git。真实文本与视频观感尚待新任务或replan验证。建议先2–3条白色款新文本，检查商品身份、模板轮换、场景和口播，再选1条跑视频；不增加正式流水线的中途人工确认。
