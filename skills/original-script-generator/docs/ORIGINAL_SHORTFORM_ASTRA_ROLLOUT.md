# 15秒原创新口播模型升级（2026-09-09）

用户确认升级口播，不升级视觉蓝图。本次仅修改路由、等待预算与口播检查点依赖；不修改写作提示词、钩子/卖点规则、语言适配、结构、人物、穿搭、场景或媒体生成。

## 生效范围

正式 `simplified_v1` 整段口播桥接 `core/complete_voiceover_direct.py` 在中央命令请求顶层携带 `route_scope=original_shortform`；中央路由在该scope和 `creative_full_single_v1` 同时匹配时使用 `gpt-6-astra/high`。路由标记不进入模型可见写作内容。

同一合同的其他调用方没有该标记时仍使用Sol→Terra。人工上传视频配口播、旧分句生成、质检、三候选实验入口不因此升级。女装与配饰等使用该正式整段桥接的类目统一生效，不按类目新增开关。长口播维持已有独立Astra路由；蓝图继续Sol。

## 失败与回退

- 主路由最多两次Astra，仅瞬时错误重试；耗尽后一次Sol/high。
- 身份认证及内容/JSON错误不因本次改动新增跨模型重试。
- 使用已有 `/Applications/ChatGPT.app/Contents/Resources/codex`；旧CLI不支持Astra，因此不沿用旧二进制。
- `ORIGINAL_SHORTFORM_VOICEOVER_CODEX_BIN` 可覆盖短口播CLI。
- `ORIGINAL_SHORTFORM_VOICEOVER_ASTRA_ENABLED=0` 回退短口播原Sol→Terra及原CLI，不影响长口播。
- 原创桥接外层等待600秒，容纳中央3×185秒及退避；原360秒可能在兜底开始前中断。没有新增重试层。

## 缓存与血缘

中央结果的 `_model_provenance` 原样进入口播 `engine_provenance.model`，记录实际模型、是否兜底和路由版本。

仅简化原创的口播依赖哈希加入路由scope、开关与CLI覆盖值。变更或回退只使执行中的口播检查点不再冒充同一路由结果；蓝图与视觉检查点保持独立。已完成脚本不主动重写、不回写飞书；需要新创作时照常新任务或replan。

## 验证边界

新增路由测试验证：scope隔离、独立回退、同提示词两次Astra后Sol、认证错误单次结束、命令分派与实际模型血缘。原创桥接测试验证标记传递、600秒预算和回退缓存变化。没有运行新的付费生成或覆盖已完成脚本；此前Astra口播实验不是本次15秒文案质量验收。
