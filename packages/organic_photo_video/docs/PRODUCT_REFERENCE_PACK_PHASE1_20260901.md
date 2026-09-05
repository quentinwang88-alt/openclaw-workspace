# 商品参考包第一阶段实施说明

## 已实现范围

- 飞书图文养号任务仍只维护产品编码、生产预设和执行。
- RDS `opv_product_reference_pack` 保存运营确认的版本化商品图片组。
- 批量生产使用稳定循环轮换；显式指定图包时精确命中；无批次键时再使用唯一默认组。
- 没有本地图片，以及无批次键时配置含糊，都会安全阻断。
- 任务创建时把 pack id、版本、图片指纹、角色和具体本地路径冻结进
  `product_snapshot_json`，历史任务不跟随后续商品资料变更。
- 生图按 hero/full_look/lifestyle/detail/second_angle 路由最多三张商品图；
  缺少真实 detail 图时改用保守中近景提示，禁止编造未证实结构。
- 飞书生产路径不调用视觉模型QA，继续使用技术媒体检查和人工预览审核。

## 来源回退

1. 新建图文任务前，只读扫描短视频运营任务表中相同产品编码的记录；
2. 每条飞书记录的附件保持为独立候选包，落到稳定本地缓存；
3. 按整组图片内容指纹与已有 RDS 包去重后，补入
   `opv_product_reference_pack`，`source_ref` 保留飞书 `record_id`；
4. 有效的 RDS 商品参考包（包括人工包和上一步同步包）参与稳定轮换；
5. 没有 RDS 包时，依次回退原创流程缓存和最近非失败OPV任务快照；
6. 无来源则进入“需处理”。

同步不新增或回写任何飞书字段，不合并不同记录的附件，也不进行视觉评分或
视觉 QA。连续批量序号采用确定性循环，同一批次可以覆盖全部有效图包；已创建
任务仍使用冻结在 `product_snapshot_json` 中的原图包版本。

## 运维命令

- `scripts/upsert_product_reference_pack.py`：预览或写入运营确认的参考包。
- `scripts/inspect_product_reference_resolution.py`：只读检查选包和P1-P5取图。
- `scripts/check_upgrade_readiness.py`：检查迁移006和整体RDS就绪状态。
