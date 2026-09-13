# VN 围巾跨市场｜真实生图端到端验收交付（VN 搭配四选一）

- **日期**：2026-09-13
- **范围**：`packages/organic_photo_video/`
- **前置**：`df6d651`（Review 3×P0 整改 + 24 条生产链集成测试）已合入 `main`
- **本轮授权**：评审 P0 整改后，用户批准「放行到真实生图」→「先造真实 VN 素材集」→「围巾搭配四选一（推荐）」
- **结论**：在**生产 1route 真实生图**通道上，VN 围巾搭配线跑通端到端（1 行 → 1 批 → 1 任务 → 4 shots → 5 页成片 → 飞书写回），字节级零漂移；4 道生产闸门与账号状态已全部复位；VN **仍不可上线**（Phase 5 遗留项见 §9）

---

## 1. 本轮要回答的问题

上一轮（`df6d651`）只证明了「代码层能跑通」（L0–L1 离线 + 集成测试），但**从未在生产运行时上真正生成过一张 VN 图**。本轮要回答的是：

> 一条 VN 围巾任务，能否在 RDS + 飞书 + 生产 1route 通道的真实链路上，从「表里一行」走到「表里五页成片」，且不误用泰国素材、不静默降级？

答案是：**能**（搭配线），但需要现场补齐 RDS 实体（§3）——配置层的 Phase 1–4 骨架**不等于**运行时可路由。

---

## 2. 六道生产门禁（开跑前勘察结论）

开跑前只读侦察（`tmp/vn_real_gen/recon.py`）发现：**RDS 里没有任何 VN 实体**。这意味着「翻 4 个开关就能跑」的假设不成立——**没有开关可翻，只有实体要播种**。真实存在的 6 道门禁：

| # | 门禁 | 判定位置 | 本次状态 |
|---|---|---|---|
| 1 | 预设不能是 `disabled` | `ProductionPresetCatalog._require_enabled`（`feishu_workflow.py:472`） | 两个 VN 预设均 `disabled` → 需临时武装 |
| 2 | 配方 `status` 必须 `active` | `photo_request_factory.py:226`、`photo_planner.py:75` | `PHOTO_MATCHING_CHOICE_V3` = `draft` |
| 3 | Market Pack 必须 `active` | `preflight.py:69` + P0-1 `market_binding_errors` | `MP_VN_DEFAULT_V1` = 不存在 |
| 4 | 账号必须存在于 RDS | `task_intake.py` | `OPV_VN_TEST_001` = 不存在 |
| 5 | 素材集 `enabled` 且类目/市场匹配 | `AssetSetService.candidates` | `VN_SCARF_CHOICE` = 不存在 |
| 6 | 飞书字段选项含该预设 | `catalog.names`（仅 active 预设） | 字段 16 选项无 VN 预设 |

---

## 3. 播种 RDS 实体（配置层 → 运行时）

用仓库自带脚本播种（默认 dry-run，`--apply` 才写），全部可复现：

| 动作 | 命令 | 结果 |
|---|---|---|
| 播种 market_pack / theme / render_preset / **content_recipe** / render_profile / quality_profile | `scripts/seed_reference_data.py --apply` | VN 实体落库；`status` 字段被尊重，**不会复活已废弃配方** |
| 导入 VN 账号档案 | `scripts/import_account_profile.py --apply` | `OPV_VN_TEST_001`（persona `TH_APPAREL_REAL_03_001`） |
| 导入素材集 | `scripts/import_photo_asset_set.py --apply` | `ASSET_VN_SCARF_CHOICE_V1`（重算 sha256） |
| 追加飞书预设选项 | `scripts/ensure_feishu_task_table.py --preset-only "图文｜VN｜围巾搭配四选一"` | `record_writes: 0`（只加选项，不动行） |

**踩坑 1（外键顺序）**：先导账号会报 `(1452, Cannot add or update a child row)`——`opv_account_profile.default_market_pack_id` 有指向 `opv_market_pack` 的外键。必须先播种 Market Pack，再导账号。

**踩坑 2（附带写入）**：`seed_reference_data.py --apply` 顺带把配置层为 `active` 但 RDS 缺失的 `PHOTO_TH_THERMAL_TRANSITION_V1` 也播了。其预设为 `disabled`、不可路由，风险可控，已记录。

---

## 4. 真实 VN 素材集：4 张 look（生产 1route 真实生图）

**为什么必须造**：搭配线要求「四选一」的 4 张真人 look 图，且必须是 VN 类目/市场。原 TH 素材集在 VN 市场**不可被选中**（`AssetSetService.candidates` 的 `category_key` + `market` 双重过滤，见 §6），所以必须新建。

**生成策略**（`tmp/vn_real_gen/generate_looks.py`）：`gpt-image-2.5-sunburst`（`multipart` 编辑模式），采用 **anchor_then_reference**：

1. 文生图产出锚点 `look_a`；
2. 以 `look_a` 作为 `continuity_reference_images`，参考编辑产出 `look_b/c/d`，保证**同一人 / 同一房间 / 同一条围巾**。

**内容规格**（`look_spec.json`）：驼色格纹羊毛围巾为恒定元素，4 套不同的外搭/下装：

| 角色 | 外搭 | 下装 | 画面语言 |
|---|---|---|---|
| A | 米白开衫 | 深蓝牛仔 | Cardigan kem + quần jeans xanh |
| B | 黑色皮夹克 | 黑色 A 字半裙 | Áo da đen + chân váy chữ A |
| C | 燕麦风衣 | 白色阔腿裤 | Áo trench yến mạch + quần ống rộng trắng |
| D | 灰绿卫衣 | 米色工装裤 | Hoodie xanh rêu + quần túi hộp be |

**产物**：4 张 941×1672 PNG（`shared/data/organic_photo_video/asset_sets/vn_scarf_choice/look_{a,b,c,d}.png`，各 ~1.9–2.2MB，`shared/data/` 为 gitignore，与 TH 素材集一致）。四张图均：镜面自拍、手机遮脸、围巾清晰可辨且不遮脸、无文字/水印。

---

## 5. 素材集合同校验（本地）

新增 `config/asset_sets/ASSET_VN_SCARF_CHOICE_V1.json`：`category_key=scarf`、`market=VN`、`status=enabled`，4 条资产带 `vi-VN` 展示名与**真实 sha256**；`content_approval.allowed_logic_keys=[scarf_styling_choice, travel_scene_outfit_choice]`，`attributes` 含 4 组互不相同的 `outerwear_id`/`bottom_id`。

`tmp/vn_real_gen/validate_asset_contract.py` 证明**两条 VN 线**都能通过：`tags_match` ✓、`validate_requirements` ✓、`freeze_content_card` ✓（含 `allowed_logic_keys` 命中、`source_hashes` 一致、4 组外搭/下装互异）。

---

## 6. 端到端真实生图（生产 1route 通道）

**测试行**：飞书惰性测试行 `recvv5rDUa2uSO`（搭配线无需产品/参考图，只需 `素材状态=已匹配可用素材`）。

**链路证据**（`tmp/vn_real_gen/verify_run.json`）：

| 层 | 值 |
|---|---|
| 批次 | `opv_batch_cc165f164834557263f032292811de87`，`waiting`，`expected_count=1`，`media_kind=native_photo` |
| 任务 | `opv_task_20260913_f5dc82221478`，`photo_packaging` |
| 配方 / 类目 / 国家 / 语言 | `PHOTO_MATCHING_CHOICE_V3` / `scarf` / `VN` / `vi-VN` |
| Market Pack | `MP_VN_DEFAULT_V1` |
| 请求出图数 | 5 |
| shots | 4 条，全部 `generated`，`generation_provider=asset-reuse`（复用真实素材集，sha256 与源 PNG **逐字节相同**） |
| 成片 | 5 页 JPEG，`1080×1920`，全部 PIL 可读 |
| 飞书写回 | 进度=已完成，成片=5，执行=false，确认发布=false，无 failure |

**shots 与源图 sha256 对齐**：

| slot | 源 look | sha256 |
|---|---|---|
| 1 | look_a | `5961805ae7803bf00a528d68dbdb283efbec03addfb17e4488a27162ec1f2556` |
| 2 | look_b | `944dc80067437ece390abb503697a5074e72b96e217855a2640bcb4ecddbdf7e` |
| 3 | look_c | `f66e0bedfdbff3d16f331d908b24499c6f9330f91410a29b6a73f4771157f6c2` |
| 4 | look_d | `e48274d417d3c9076b2171e1cc92207ec7984c8be88dd77d74fbf505a3666f03` |

---

## 7. 字节级零漂移核验

`tmp/vn_real_gen/verify_final.py`（`verify_final.json`）：

- `all_attachments_byte_identical: true`——5 张飞书附件与本地最终件**逐字节一致**；
- `zero_drift: true`——飞书全表直方图（加入本行后）与基线快照一致；
- 行终态：进度=已完成，执行=false，确认发布=false，审核=无需审核。

---

## 8. 一条真实缺陷（响亮失败，非静默复用）

同一轮同时验证了**旅行线**：`PHOTO_TRAVEL_OUTFIT_V3` 的配方仍绑定 `TH_WOMENSWEAR_CHOICE`，在 VN 市场返回 **0 候选**，于是抛 **`NEEDS_ASSET`** 响亮失败——**没有**静默复用泰国素材。

这是**期望行为**（宁可响亮失败也不跨市场错用），但也说明：**旅行线要上 VN，必须先给它的配方绑定 VN 素材集或补 VN execution profile**（见 §9）。

---

## 9. 仍未覆盖 / Phase 5 遗留项

| 项 | 说明 |
|---|---|
| **旅行线 VN execution profile** | `PHOTO_TRAVEL_OUTFIT_V3` 仍引用 TH 素材集；VN 旅行线当前必然 `NEEDS_ASSET`。需绑定 VN 素材集或补 execution profile |
| **VN 文案仍为 DRAFT** | 无母语终审（`excluded_claims` 含 `native_language_approved`） |
| **账号人设非本地** | `OPV_VN_TEST_001` 复用泰语库人设 `TH_APPAREL_REAL_03_001`，非 VN 本地人设 |
| **发布能力未确认** | TikTok Content Posting 能力未验证；本轮全程 `确认发布=false`，发布侧闭环 |
| P1-4 / P1-5 / P2-7 | Execution Context 未进生产冻结；Destination Catalog 未进运行时；缺 `披肩围巾` 别名 |

---

## 10. 内容 / 合规标旗

⚠️ 驼色格纹围巾的格纹图案**近似某奢侈品经典格**。**商用前建议更换配色/格型**，以规避商标与外观风险。本轮为内部测试，未对外发布。

---

## 11. 清理与复位（全部已执行）

跑完后逐项复位，并用**直接读 RDS**复核（非仅信任记录文件）：

| 对象 | 复位后 | 复核方式 |
|---|---|---|
| 预设「图文｜VN｜围巾搭配四选一」/「围巾旅行」 | `disabled` | 读 config 文件（与 HEAD 无 diff） |
| `MP_VN_DEFAULT_V1` | `draft` | 直读 `opv_market_pack` ✓ |
| `PHOTO_MATCHING_CHOICE_V3` / `PHOTO_TRAVEL_OUTFIT_V3` | `draft` | 直读 `opv_content_recipe` ✓ |
| `OPV_VN_TEST_001` | `paused` | 直读 `opv_account_profile` ✓ |
| 飞书测试行 `recvv5rDUa2uSO` | 执行=false、确认发布=false、幂等标记已还原 | `finalize_vn_row.py` |
| `config/feishu_production_presets.json` | 与 HEAD 无差异（武装时的空白重排已 `git checkout` 还原） | `git diff --stat` 为空 ✓ |

> 注：素材集 `ASSET_VN_SCARF_CHOICE_V1` 保持 `enabled`——它**不是**路由闸门（路由由预设/配方/pack 决定），保持 enabled 才能被后续 VN 任务复用。

---

## 12. 交付物

**入库（独立提交）**
- `config/asset_sets/ASSET_VN_SCARF_CHOICE_V1.json`（新增）

**不入库（gitignore / 运行时产物）**
- `shared/data/organic_photo_video/asset_sets/vn_scarf_choice/look_{a,b,c,d}.png`（4 张真实素材图）
- `tmp/vn_real_gen/*`（本轮全部可复现脚本与证据 JSON）

**可复现脚本**（`tmp/vn_real_gen/`）：`recon` → `capture_baseline` → `generate_looks` → `build_asset_set` → `validate_asset_contract` → `seed/verify_seed` → `dryrun_candidates` → `arm_gates`/`arm_account` → `create_vn_row` → `arm_vn_row` → `verify_run` → `verify_final` → `restore_gates`/`arm_account --restore` → `finalize_vn_row`
