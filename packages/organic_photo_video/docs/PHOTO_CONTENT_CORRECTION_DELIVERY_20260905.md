A+B 内容纠偏交付记录（2026-09-05）

本轮完成工程修复和一篇待验收样板。技术通过不代表用户内容验收或泰语母语审核通过。

生产收束：
- recvulMq5VuFh0、recvulMq5VlqR1、recvulMq5VoNKA 三行均标记“技术样本 / 内容验收未通过 / 禁止发布”，执行与确认发布关闭。
- 对应九任务均有持久 CONTENT_REJECTED 终审失败记录。Feishu 通过/waived、release_revision 的数据库事务以及发布前 freeze gate 均阻断；按 task 查询所有历史 revision，换修订不能绕过。
- Choice 旧 Recipe、TH 温度和旅行 Recipe 退役；温度/旅行预设禁止新任务。Choice V1/V2 与 Layered V1 素材集停用。旧配置内容未原地覆盖，原图/成品和冻结证据保留。

实现：
- photo_package.py：single 恰好一源；新 PHOTO_CHOICE_CARD_V1 使用严格布局 v2，只接受实际执行的 render_options。旧冻结 v1 仍兼容。
- photo_planner.py / domain/contracts.py：角色是源图片身份，与最终页号分开；4 源可形成四源封面与 4 张详情页，无 cover_seed。
- photo_content.py：冻结中文主题/用户问题/比较依据/逐页职责/角色与缺口；绑定源 SHA256 的适用确认；执行 distinct_looks、同外套不同下装及严格叠穿关系；非空未知规则拒绝。
- photo_request_factory.py：取消 3 profile / 2 copy 数量要求，按 logic_key + 源图 hash 集合计算有效库存；同图换标题不能凑数，缺库存整批冻结前失败，不减量。
- rds_repository.py：复用 ProductionBatch 储存签名，用 MySQL advisory lock 串行检查跨批占用；原行续跑先返回原冻结批次。未新增表或运营 JSON。
- asset_set_service.py：同 ID 素材内容禁止改写，只允许状态变更；内容更新用新 ID/版本。旧冻结快照不受当前配置退休影响，但源字节改变会阻断。

新版本：PHOTO_TH_PICK_YOUR_LOOK_V2 / recipe version 3，ASSET_TH_WOMENSWEAR_CHOICE_V3 / 4 源，PHOTO_CHOICE_CARD_V1 / layout version 1。Layered V2 仅保留未获内容资格的参考图，仍 disabled。

样板：
- 飞书记录：recvumeaKkgo8k
- task：opv_task_20260905_c0bea27f6845
- revision：opv_rev_20260905_9eb2d4dc2eb3
- batch：opv_batch_7c1cce04ec3c6a514bf6c12bc8b09895
- 主题：短外套配短裤还是裙装？四套搭配选一
- 状态：图文终审 / 待审核；5 附件；4 源 / 5 JPEG；终审记录 0；released_revision_id = null；模型调用 0；主资产/排期 0；发布调用 0。
- P1 四套总览；P2 A 棕外套+短裤；P3 B 灰外套+短裤；P4 C 同款灰外套+百褶裙；P5 D 棕外套+黑裙并互动。
- B/C 内搭也改变了，内容卡和发布文案明确这一点，不宣称单变量实验、保暖、城市适用或显高。
- 四源与五张最终页已逐一目视核对。source qualification 由 assistant 视觉核对记录，未伪造人工或母语批准。泰语 pending_native_review，内容 pending_user_review。

最终图片：
- [01.jpg](/Users/likeu3/.openclaw/shared/data/organic_photo_video/photo_packages/TH_TRUTHFUL_CONTENT_SAMPLE_20260905/opv_task_20260905_c0bea27f6845/opv_rev_20260905_9eb2d4dc2eb3/final/01.jpg)
- [02.jpg](/Users/likeu3/.openclaw/shared/data/organic_photo_video/photo_packages/TH_TRUTHFUL_CONTENT_SAMPLE_20260905/opv_task_20260905_c0bea27f6845/opv_rev_20260905_9eb2d4dc2eb3/final/02.jpg)
- [03.jpg](/Users/likeu3/.openclaw/shared/data/organic_photo_video/photo_packages/TH_TRUTHFUL_CONTENT_SAMPLE_20260905/opv_task_20260905_c0bea27f6845/opv_rev_20260905_9eb2d4dc2eb3/final/03.jpg)
- [04.jpg](/Users/likeu3/.openclaw/shared/data/organic_photo_video/photo_packages/TH_TRUTHFUL_CONTENT_SAMPLE_20260905/opv_task_20260905_c0bea27f6845/opv_rev_20260905_9eb2d4dc2eb3/final/04.jpg)
- [05.jpg](/Users/likeu3/.openclaw/shared/data/organic_photo_video/photo_packages/TH_TRUTHFUL_CONTENT_SAMPLE_20260905/opv_task_20260905_c0bea27f6845/opv_rev_20260905_9eb2d4dc2eb3/final/05.jpg)

验证：548 个 OPV 全量 unittest 通过；其中 53 个 photo 聚焦测试通过。新增覆盖多源 single、角色换序、换下装不满足叠穿、同图换标题库存不足、跨批签名占用、同批幂等续跑、跨修订拒绝门禁、旧冻结漂移和从 migration 提取的 VARCHAR(32) 约束边界。git diff --check 通过。

preflight --verify-files 无配置错误，只有新 TH 样板 ready=1；其他尚无源资格/内容卡的 MX/小个子等明确 NEEDS_CONTENT/NEEDS_ASSET。该预检只读配置，不扣除 RDS 已冻结签名；本样板占用唯一有效内容后，新行同图同逻辑应拒绝。

已执行外部写入：seed_reference_data、指定素材版本/状态、三行旧记录及九个失败审核、新样板的一批一任务和五附件。均回读。正式表 schema 同步没有新增或改名字段。部署中一次 JSON 键顺序造成字符串比较误报，改为结构比较后在已成功步骤基础上续跑，无内容覆盖。

未执行：模型生成、终审 passed/waived、确认发布、主发布队列、TikTok 发布。发布 adapter/渠道保持不变，未提交 git commit。

证据：
- /tmp/opv-content-hold-readback.json：三行/九任务内容拒绝及逐项门禁调用。
- /tmp/opv-content-config-readback.json：RDS Recipe 与素材版本逐项回读。
- /tmp/opv-content-feishu-schema-readback.json：正式表字段同步。
- /tmp/opv-truthful-sample-20260905.json：单篇请求/批次/计划/成品/附件/状态与队列回读。
- /tmp/opv-content-final-readback.json：最终旧九任务、三行标记和跨批库存核对。
- /tmp/opv-content-all-final.log、/tmp/opv-content-preflight-final.json：测试与预检。
