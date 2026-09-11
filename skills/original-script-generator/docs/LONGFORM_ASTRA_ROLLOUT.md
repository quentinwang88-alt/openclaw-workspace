# 长视频中央口播 Astra 灰度

仅 creative_longform_single_v1 默认使用 gpt-6-astra/high。
沿用中央模型路由：瞬时错误最多两次主模型尝试，再以gpt-5.6-sol/high兜底一次。
认证、模型不支持和JSON/内容错误不触发跨模型降级，不增加文案润色。
15秒、人工配口播及其他合同仍为原Sol/high→Terra/high路线。

长口播使用已安装的 /Applications/ChatGPT.app/Contents/Resources/codex。
原standalone/current（0.144.6）不支持Astra；不得仅改模型名继续用该旧版本。
可用 LONGFORM_VOICEOVER_CODEX_BIN 配置兼容新版路径；不改全局CLI指向。
LONGFORM_VOICEOVER_ASTRA_ENABLED=0 回退原Sol→Terra及原CLI。

_model_provenance保留configured_primary_model、actual_model、fallback_used、reasoning_effort、transient_attempt_count、route_version和codex_bin，随长视频口播持久化，不再丢弃。
旧脚本不会自动失效或重写；新口播调用及后续口播修订使用新路由。
完整RDS参考样本的灰度开关不因模型迁移自动开启；本次测试继续使用已批准冻结完整参考。

验收：中央命令16项、路由隔离2项、长口播投影4项通过。
本地长视频库目前只有产品1737141103233042426，因此尚不能宣称两个其他产品的交叉验证完成。
隔离真实入口测试使用 scripts/test_longform_route_smoke.py，仅写共享测试目录，不更新生产任务、不写飞书或生成媒体。
