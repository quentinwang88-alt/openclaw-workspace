# 全流程 SKU 颗粒度迁移方案

## 原则

- `sku_code` 是所有新业务输入和任务身份；`sku_id` 是内部不可变关联主键。
- `spu_id` 只用于商品归属和汇总，不参与参考图或生成任务的最终选择。
- 历史任务保留原快照、脚本ID、任务ID、图包ID和资源路径；SKU归属以旁路绑定补充。
- 重试使用旧冻结快照；重新规划生成新revision并强制绑定SKU。

## 历史对象分类

1. 原编码已经是SKU：直接建立确认绑定。
2. 原编码是SPU，但指定范围内只有一个有证据的实际SKU：自动建立范围绑定。
3. 同一SPU存在多个SKU证据或属性不完整：标记待确认，不为新任务静默选择。

自动归属必须同时满足：权威SKU目录存在候选；运营记录或冻结属性与候选兼容；同一市场、店铺及历史范围内无其他SKU证据。使用次数最多、最近一次、图片相同、`DEFAULT`或模型识图均不能单独作为身份依据。

## 最小迁移数据

新增SKU目录、旧身份映射和业务对象绑定：

```text
catalog_sku:
  sku_id, sku_code, spu_id, market, store_id, attributes, status

legacy_identity_mapping:
  source_system, scope, legacy_identity, sku_id,
  state, evidence, rule_version, mapping_version

business_identity_binding:
  source_system, object_type, object_id, sku_id,
  mapping_id, legacy_snapshot_hash, binding_version
```

映射状态：`DISCOVERED → AUTO_CONFIRMED/NEEDS_REVIEW/CONFLICT → APPROVED → SHADOW_VALIDATED → APPLIED → VERIFIED`。源数据变化后转为`STALE`；错误映射追加撤销版本，不删除历史。

## 各类数据处理

- 飞书：保留旧“产品编码”，新增“SKU编码、迁移状态、历史编码”。新任务强制SKU，旧列逐步只读。
- OPV图组：运营表一行仍是一个参考图组，图组绑定SKU；同SKU可以有多个组用于轮换。
- auto_mixcut：`DEFAULT`包先映射到真实SKU，旧包ID不改；新包直接写真实SKU。
- 原创与复刻：新计划将SKU和参考组写入内容hash；旧计划的hash与冻结图包保持不变。
- OSS与缓存：不移动历史对象。资产按SHA256/asset_id统一，旧路径保留为别名和恢复来源。
- 脚本及生成任务：历史script_id、job_id和平台任务ID不做字符串替换，只追加SKU绑定。

同一图片可以被多个SKU引用，尤其是只因尺码不同而共用图片的SKU；共享资产不等于合并SKU身份。

## 上线顺序

1. 建立权威SKU目录，盘点各系统旧编码、冻结属性、图包和图片哈希。
2. 影子计算映射，自动确认无冲突的“历史唯一实际SKU”，输出待确认和冲突清单。
3. 所有读取增加顺序：冻结快照 → 新sku_id → 已批准旧映射 → 阻断新任务。
4. 新任务以SKU为权威双写兼容字段；历史只写旁路binding，不改原快照。
5. 依次灰度切换参考图查询、原创、复刻、OPV图文、发布与统计。
6. 验证稳定后停止旧产品编码的新写入，历史兼容读取长期保留。

## 迁移保护

- 每条迁移项保存源对象`before_hash`；应用前发现变化即标记STALE。
- 迁移脚本幂等，重复执行不得重复建任务、图包或发布记录。
- `NEEDS_REVIEW/CONFLICT`只阻止相关对象的新生产，不影响有完整冻结快照的历史重试。
- SKU更正通过新版本binding撤销和替换，保留完整审计链。
- 验收必须确认历史任务hash、参考图hash、成片路径和平台关联未变化。
