# 完整口播参考 RDS 灰度

中央引擎 `services/writing_cases.py` 只读 `sd_speech_transcript.quality_details_json.writing_reference`。
本地 `writing_reference_cases.json` 仅留作历史候选记录，不再作为正文来源。

读取要求：READY、FULL、review_method 为 SOURCE_AUDIO_CHECKED 或 HUMAN_APPROVED_TEXT、reviewed_by 非空。
同时验证 transcript_raw 的 source_transcript_hash 与 reference_text 的 reviewed_text_hash。
SOURCE_AUDIO_CHECKED 可以是模型辅助核对，不等同于母语人工批准；不确定片段与核对人保留审计。
国家、类目从最新 sd_speech_hook_profile 获取，不要求该视频仍在 active hook 簇或 hook_usable。
缺元数据、哈希不符和 PENDING_REVIEW 不进入参考；连接失败正常回退旧参考并记录错误类型，不记录连接凭据。

仅显式 LONGFORM_WRITING_CASE_ENABLED=1 时长视频资源解析器才读取。
一个完整样本替换旧表达参考，不叠加样本，不改商品事实、卖点、视觉或15秒路径。

隔离2×2对照入口（不更新生产数据库、不写飞书、不生成媒体）：

```bash
python3 scripts/test_longform_writing_reference_ab.py \
  --job-id LFJ_40AEA8CBCB98646F3D48 \
  --output-dir /Users/likeu3/.openclaw/shared/data/writing_reference_ab_20260909 \
  --prepare-only
```

确认商品数据与完整参考话术均获外部模型发送授权后，去掉 --prepare-only 执行。
已有输入快照和成功候选会复用；更换测试输入或模型命令时使用新输出目录。
同一商品主合同、视觉计划、卖点、钩子和系统提示词生成旧参考2条、完整参考2条。
writer_inputs.json 检查非参考部分一致；模型返回保留实际模型及投影审计。
结果逐条落盘，最后生成乱序 blind_review.md 和单独 answer_key.json。
字符估时非TTS实测，2×2只作探索，不宣称生产质量或泰语母语审核通过。

2026-09-09：读取验证选中7641584617031552277，24项相关单元测试通过。
输入快照准备后，外部发送完整参考话术曾被权限审核拦截。用户补充明确授权后已完成4/4对照文本，均Sol/high，无降级或重试。
结果位于上述输出目录的result.json、blind_review.md、review.md。接入有效，但本次2×2没有观察到明显质量提升，默认开关继续关闭。
