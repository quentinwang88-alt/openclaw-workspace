# 商品参考图资产统一方案

## 结论

统一不可变图片资产、参考图组身份、版本和来源血缘；图文、原创视频和复刻视频分别保留自己的选组策略。不得把所有业务统一为“取产品最新 active 包”。

## 当前实现

### 图文（organic_photo_video）

正式飞书任务为 `ProductReferenceResolver` 注入 `FeishuOperationProductPackSource`。来源适配器按产品编码读取短视频运营任务表的所有匹配记录，每条记录的附件独立组成一个候选图组，并缓存到：

`~/.openclaw/shared/data/opv_operation_reference_cache/<product_id>/<record_id>/`

候选组以 `operation_<record_id>` 作为 variant，按图片内容哈希集合去重后写入 `opv_product_reference_pack`。解析时每个 variant 只保留最高可用版本，再根据显式 variant、批次稳定轮换、默认组或唯一组选择。选择结果连同图片路径、SHA256、角色和包版本冻结到 batch manifest 及 content task snapshot，续跑不重新选组。

`resolve_snapshot` 不是纯读取操作：它会同步飞书候选组，并可能从共享缓存或历史任务 bootstrap 后写入 OPV 包表。

### auto_mixcut

以 `market + product_id + sku_id` 为逻辑图组，每次更新生成新版本；同一组合通常只保留一个 active 版本。消费端默认取得版本号最高的 active 包。资产存储在 OSS，并在 `product_reference_images` 中记录 object key、角色、顺序和文件哈希。

### remake_video_execution

当前修复版从 auto_mixcut 只读解析显式包或最高 active 版本，下载全部图片并冻结包 ID、版本和 SHA256。任务续跑只验证冻结内容，不重新选择新包。

## 当前风险

1. OPV 的运营记录组和 auto_mixcut 的 SKU 图包使用不同身份模型，不能直接按包 ID 合并。
2. OPV 运营表候选没有审批状态、国家或 SKU 过滤，同产品不同市场和不同款式可能混组。
3. OPV 的解析接口混合同步、导入和读取，检查操作可能产生数据库写入。
4. OPV 缓存命中不重新验证内容；冻结包图片丢失时可能过滤缺图后继续，造成图包静默缩小。
5. 两边都可能按输入顺序猜图片角色，角色语义不可靠。
6. OPV 已退役或阻断内容仍参与去重，可能阻止同一图组重新导入。
7. auto_mixcut 创建新版本时先归档旧 active 包，后续上传失败会产生暂时无 active 包的窗口。
8. OPV 依赖本地绝对路径，换机器或清理缓存后恢复能力不足。

## 统一目标模型

### 1. 不可变资产层

每张图片只按内容身份保存：

- `asset_id`
- `sha256`
- `oss_object_key`
- `mime_type / width / height`
- `created_at`

本地文件只是可重建缓存，OSS 对象和 SHA256 才是权威。读取缓存时必须复算哈希。

### 2. 参考图组层

图组表达“一套应共同使用的商品参考图”：

- `reference_group_id`
- `product_id`
- `market`
- `sku_id`
- `variant_key`
- `group_kind`：`sku_authority / operation_variant / manual / task_upload`
- `version`
- `status`
- `is_default`
- 有序资产清单及明确角色

运营表的一行是 `operation_variant`，不能被当作 SKU；auto_mixcut 包是 `sku_authority`。同组更新才增加 version，不同运营记录默认是不同组。

### 3. 来源血缘层

内容相同也保留所有来源别名：

- 来源系统、表、记录 ID、附件 token
- 旧 OPV pack ID、旧 auto_mixcut pack ID
- 首次同步和最后确认时间

图片内容去重不能删除来源关系。

### 4. 纯读解析层

拆成两个明确接口：

- `sync_reference_sources(...)`：显式同步并允许写入。
- `list/resolve_reference_groups(...)`：严格只读，返回全部候选、版本和选择理由。

任何 `check`、审计或恢复流程只允许调用纯读接口。

### 5. 任务冻结层

任务计划必须冻结：

- group ID、version、来源 ID
- 每张资产的 asset ID、角色、顺序、SHA256
- 选择策略和选择理由

恢复时任何图片缺失或哈希变化都阻断，不能把缺失图片过滤后继续执行。新版本只影响新计划。

## 各业务选择策略

### 图文

在符合产品、市场、SKU、审批状态的所有 `operation_variant` 和允许的默认组中稳定轮换。轮换结果在批次建立时冻结，确保乱序执行和重试结果一致。

### 原创视频

优先使用任务显式指定组；否则按产品、市场和 SKU 选择默认 `sku_authority` 的最高有效版本。需要多造型实验时，由任务显式启用 variant 轮换。

### 复刻视频

优先使用复刻来源记录绑定的参考组，其次使用任务显式组，再次使用同市场和 SKU 的默认权威组。一个复刻任务及其所有分段必须使用同一冻结组，不进行跨组轮换。

### 人工上传

每次上传形成 `task_upload` 图组并绑定任务。除非用户明确发布为共享组，否则不得覆盖产品默认权威组。

## 分阶段迁移

### 阶段一：消除高风险行为

1. 给 OPV 增加严格纯读 reader，把同步和 bootstrap 从 `resolve_snapshot` 拆出。
2. 运营来源增加市场、SKU、审批状态过滤，并把过滤条件写入解析报告。
3. 缓存命中复算 SHA256；冻结图包缺任一资产即阻断。
4. 无可靠角色时保存 `unknown`，不再按顺序推断 back、lifestyle。
5. auto_mixcut 新包完成上传和入库后，再原子切换 active 版本。

### 阶段二：建立统一目录

1. 新建统一 asset、reference group、group asset、source alias 表及纯读服务。
2. 对 OPV、auto_mixcut 历史包建立映射，保留旧 ID，不修改历史任务。
3. 对同一产品执行影子解析，对比旧选择与新选择的图片集合、角色、版本和理由。

### 阶段三：逐业务切换

1. 先切复刻新任务，因为其策略最明确：绑定单组并冻结。
2. 再切原创视频的 SKU 权威包选择。
3. 最后切图文轮换，保留现有 selection key 和历史使用量算法。
4. 稳定后统一写入入口，旧表降为兼容投影；历史任务继续读取原冻结快照。

## 验收条件

- 给定同一任务输入，多次规划得到相同 group、version 和资产哈希。
- 更新参考组后，旧任务保持旧快照，新任务选择新版本。
- 同产品不同市场、SKU 和运营组不会互相混用。
- 图文多组轮换保持均衡，复刻任务不会在重试时换组。
- 缺图、哈希变化、来源无审批或角色不明均有明确状态与错误原因。
- 审计和查询接口不产生数据库、OSS或飞书写入。
