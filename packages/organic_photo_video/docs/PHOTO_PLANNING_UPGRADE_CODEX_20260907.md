# TH 图文规划层升级与跨图一致性——开发方案

更新日期：2026-09-07
执行范围：`PHOTO_TH_PICK_YOUR_LOOK_V3` 的 `STYLE + SCENE_MODEL` 路径（规划层换模型 + 全局视觉计划 + 跨图一致性校验）
优先级：P0（用户验收两轮样片后仍存在：穿搭不好看、肤色不齐、构图不佳、色调不统一）
前置：人物真实感改造（`TH_PERSONA_REALISM_OPTIMIZATION_HANDOFF_20260907.md`）已完成并生效

## 1. 根因结论（本轮要治什么）

两轮样片验证了一个结构性模式：**有"结构化合同 + 程序化 QA + 定向重生"闭环的属性（歪头/AI 脸/动作重复）已被治好；只靠 prompt 文字软约束的属性（审美/肤色/构图/色调）没有**。具体缺口：

1. **规划模型能力不足**：Doubao Seed 2.1 turbo 是理解型模型，穿搭即兴填空、无审美判断、无全局计划能力。
2. **无全局视觉计划**：四张图独立规划独立生成，肤色（连续属性）和色调（场景光×服装色×调色的乘积）没有任何全组基准，必然漂移。
3. **构图靠形容词**：camera_zh 文本对图像模型构图控制力弱，且无数值化、无 QA。
4. **无跨图一致性校验**：QA 全部逐张打分或对齐参考图，组内互比维度缺失（palette_alignment 查的是"像不像参考图"，方向就错了）。

已验证的技术前提（2026-09-07 探针）：`gpt-5.6-sol` 经 codex Responses API 可用——支持 `input_image` base64 输入、`reasoning.effort=medium` 生效、输出质量高于 Doubao turbo。调用硬约束：`stream=true`、`store=false`、不支持 `max_output_tokens`、无 `response_format=json_object`。

## 2. 本轮目标

完成后，一篇四选一图文应满足：

1. 四张肤色肉眼一致（以 persona 正脸图为全组唯一肤色权威源）；
2. 四张色调统一（全组色温/饱和度/对比度基准一致，look 间只有服装主色差异）；
3. 构图按数值化合同执行（人物占比/焦段/机位角度/地平线），四张机位不同且稳定；
4. 每套穿搭经规划模型审美自评 ≥85 才冻结；
5. 肤色/色调不一致时能程序化发现并定向重生失败页面；
6. 规划层 provider 可配置可回退，manifest 记录实际使用的模型；
7. 不改变生图通道（CreatOK gpt-image-2-official 为主、codex 图像兜底不变）。

## 3. 具体改造

### 3.1 CodexVisionClient（规划层模型升级）

**新增 `services/codex_vision_client.py`**：

```python
class CodexVisionClient:
    def __init__(self, *, api_url, access_token, model, reasoning_effort="medium", timeout=180)
    def chat_with_multiple_images(self, paths, prompt, instructions=None) -> dict
    @staticmethod
    def parse_json_response(response) -> dict   # 与 _DoubaoVisionClient.parse_json_response 行为一致
```

- 端点：`{api_url}/responses`（api_url 复用 `skills/openai-image/app/config.py::resolve_codex_base_url()`，token 复用 `resolve_codex_access_token()`，sys.path 注入方式与 `image_generator._load_skill_service` 相同，不复制 token 逻辑）。
- 请求形态：`instructions`（JSON-only 强约束 + 容错说明）+ `input` 消息数组（每图一个 `{"type":"input_image","image_url":"data:image/jpeg;base64,..."}` + 一个 `input_text`）+ `reasoning={"effort": effort}` + `stream=True, store=False`。
- SSE 解析：逐行取 `data: `，累积 `response.output_text.delta`，`[DONE]` 结束；`response.failed` / 非 200 → 抛 `PhotoReferenceVisionError`（错误信息含 HTTP 状态与 detail）。
- 重试：网络/5xx/429 重试一次（间隔 2s）；4xx 参数错误不重试直接抛。
- 图片输入复用 `PhotoReferenceVisionService._model_images` 的压缩缓存（1600px JPEG q86）——将该方法的产物路径传给 client，client 不自己压图。

**修改 `services/photo_reference_vision.py`**：

- `__init__` 读取（并加入 `_load_local_environment` 白名单）：
  - `OPV_PHOTO_VISION_PROVIDER`：`codex`（默认）｜`doubao`
  - `OPV_PHOTO_VISION_CODEX_MODEL`：默认 `gpt-5.6-sol`
  - `OPV_PHOTO_VISION_CODEX_EFFORT`：默认 `medium`
  - `OPV_PHOTO_VISION_MODEL/API_URL/API_KEY` 保留（Doubao 回退配置）
- `_client()` 按 provider 构建；Doubao key 未配置且 provider=doubao 时维持现有报错。
- **显式回退**：codex 两次重试后仍失败 → 自动用 Doubao 完成本次调用，返回结构附加 `"provider_used": "doubao_fallback"`；不静默——每次 review/analyze 的返回和 manifest 都记录 provider。禁止 Doubao 反向回退 codex（避免额度失控）。
- Doubao 与 codex 的 `chat_with_multiple_images` 签名对齐（`paths, prompt, max_tokens` 忽略 codex 不支持的参数）。

### 3.2 规划产出升级：全局视觉计划（治肤色/色调/构图的规划侧）

**`photo_reference_vision.py::_analysis_prompt` 输出 schema 升级**（新增字段，向后兼容）：

```json
{
  "color_grading_plan": {
    "temperature": "warm_4800k",
    "saturation": "medium_soft",
    "contrast": "gentle",
    "skin_tone_anchor": "冷白透亮带自然血色，以人物参考图为唯一标准",
    "tone_note_zh": "全组统一的调色说明（中文一句）"
  },
  "recommended_sets": [{
    "looks": [{
      "palette_hex": ["#C9B99A", "#F5F1E8", "#8A4A3B"],
      "outfit_aesthetic": {
        "harmony": 88, "layering": 85, "color_balance": 90, "proportion": 87,
        "issues": [], "revise_zh": ""
      }
    }]
  }]
}
```

- 穿搭审美自评规则写进 prompt（rubric：单品协调/层次/配色平衡/显高显瘦，85 分及格线）。
- `_normalize_contract` 校验：hex 格式（`^#[0-9A-Fa-f]{6}$`，每 look 2-4 个）、`outfit_aesthetic` 四项分数钳制 0-100、`color_grading_plan` 四字段非空；缺失 `color_grading_plan` 用保守默认填充（不失败——旧路径兼容）。
- **自评触发重规划**：任一 look 的四项均值 <85 → 拼接 revise_zh 重发规划请求一次（复用 `plan_travel_content` 的两段式纠错模式）；两次仍 <85 → 结构报错（宁可失败不冻结丑穿搭）。
- `_normalize_contract` 返回值透传新字段到 style_profile。

**`services/photo_content_planner.py::_vision_plan`**：把 `color_grading_plan` 与每 look `palette_hex` 冻结进 plan item（`content_plan` 已有冻结/摘要机制，直接扩展字段）。

**`services/photo_style_reference_supply.py`**：

- `prepare` 时从 variation/style_profile 提取 `color_grading_plan`，随 recipe_execution 携带进每个 ShotGenerationRequest（全组同一份）。
- POSE_CONTRACTS 的 `camera_zh` 升级为结构化 `camera_params`（保留 camera_zh 作为自然语言总结）：

```json
{
  "subject_height_ratio": "0.70-0.80",
  "lens": "35mm",
  "camera_angle_deg": 30,
  "camera_position": "front_left",
  "horizon": "lower_third"
}
```

每角色不同数值（look_a 侧 30°/35mm、look_b 侧前 45°/50mm、look_c 斜侧前景/35mm 低机位、look_d 侧后 120°/50mm），pose_contract 保留现有字段。

**`services/image_generator.py::compose_shot_prompt`**：

- 新增【全局色彩合同】段落（仅 color_grading_plan 存在时渲染）：色温/饱和度/对比度基准、肤色权威源声明（"肤色只取人物参考图（正脸），四张完全一致；禁止随场景改变肤色"）、本 look 主色卡 hex。
- `pose_contract_text` 渲染数值化构图参数（"人物高度占画面 70-80%，35mm 视角，机位左前方约 30 度，地平线在下三分线"）。

**manifest**：supply manifest 顶层记录 `color_grading_plan` 与 `vision_provider`（每 source 已有 generation 字段，视觉检查结构里记 provider_used）。

### 3.3 跨图一致性校验（治肤色/色调的校验侧）

**新增 `services/photo_color_consistency.py`（纯程序化，零模型成本）**：

```python
def color_stats(path) -> dict      # RGB 均值、亮度、平均饱和度、色温代理（R/B 比）、直方图裁剪比例
def evaluate_group_consistency(paths, baseline=None) -> dict
# 返回 {"passed": bool, "failed_roles": [...], "metrics": {...}, "issues": [...]}
```

- 判定规则（第一版阈值，样片校准后固化）：
  - 组内色温代理（R/B 比）极差 > 0.18 → 失败；
  - 组内平均饱和度标准差 > 0.06 → 失败；
  - 亮度极差 > 28（0-255）→ 失败；
  - 归因：偏离组内中位数最大的图记为 failed_role。
- `baseline` 可选：color_grading_plan 的文字基准暂不参与数值比对（模型文字→数值映射不稳定），第一版只做组内互比。

**`photo_reference_vision.py::review_group_consistency`（视觉模型组级比对）**：

- 输入：persona 正脸图（肤色权威源）+ 四张生成图；
- 输出 schema（观察型，程序重算 pass/fail，与 human QA 同纪律）：

```json
{"roles": [{"role": "look_a", "skin_tone_match": 92, "color_grading_match": 88,
            "lighting_match": 90, "drift_note_zh": ""}],
 "group_passed_suggestion": true}
```

- 程序化判定：任一 role 任一维度 <85 → 该 role 失败；`group_passed_suggestion` 不被信任。

**supply 流程接入**（`photo_style_reference_supply.py` 整组 QA 段）：

- 顺序：`程序化色调比对`（免费，先跑，失败即短路）→ 风格 QA → 人物 QA → `视觉组级一致性`（仅程序化通过后调用，省视觉费用）。
- 失败并入现有 `failed_roles` 联合归因与 `group_repair_attempts` 定向重生；`attempt_history` 条目增加 `consistency_alignment` 字段；manifest 顶层增加 `group_consistency_qa`。
- 定向重生 prompt 注入一致性修复说明（"本张肤色/色调偏离全组基准，严格对齐人物参考图肤色与全组色温"）。

### 3.4 不改的东西

- 生图通道（CreatOK 主 + codex 兜底）与 `image_generator` 的生成适配器；
- 人物包机制、四动作族合同、手势变体、human QA（已验证有效）；
- 飞书字段（零运营新增）；
- 旧 manifest / 冻结任务（新字段缺失按旧版本处理）；
- 旅行 Recipe 路径（本轮后单独评估平移）。

## 4. 开发阶段

### Phase 1：CodexVisionClient + provider 路由（半天）
1. 新建 `services/codex_vision_client.py`（SSE 解析/重试/错误映射）；
2. `PhotoReferenceVisionService` 双 provider + 显式回退 + manifest 记录；
3. 单测（不真实调用：SSE fixture、4xx 不重试、回退路径、token 缺失）；
4. 真实探针一次（单图 JSON 输出验证，计入本轮允许的真实调用）。

### Phase 2：全局视觉计划 + 数值构图（一天）
1. `_analysis_prompt` schema 升级 + `_normalize_contract` 校验 + 自评重规划；
2. content planner 冻结透传；
3. supply 携带 color_grading_plan、camera_params；
4. `compose_shot_prompt` 渲染色彩合同与数值构图；
5. 更新受影响测试 + 新增 schema/渲染断言测试。

### Phase 3：跨图一致性校验（一天）
1. `photo_color_consistency.py` + 单测（构造偏差图/一致图）；
2. `review_group_consistency` + 程序化判定 + 单测；
3. supply 流程接入（顺序、联合归因、attempt_history、manifest）；
4. 全量回归。

### Phase 4：真实样片（新飞书记录，不覆盖历史）
- 凉爽旅行/1 篇/风格参考/复用两张海边参考图；内容要求同前；
- 验收：四张肤色/色调肉眼统一、构图数值执行、穿搭自评 ≥85、一致性检查全绿、manifest 完整。

## 5. 测试要求

单测至少覆盖：
1. CodexVisionClient：SSE 增量解析、`[DONE]`、非 200 抛错、429/5xx 重试一次、4xx 不重试、JSON 后附带文本容错；
2. provider 路由：默认 codex、显式 doubao、codex 失败回退 Doubao 且记录 provider_used；
3. `_normalize_contract`：hex 校验失败报错、分数钳制、缺 color_grading_plan 走默认、自评 <85 触发一次重规划、两次 <85 报错；
4. `compose_shot_prompt`：色彩合同/肤色权威源/数值构图渲染断言；fl_at_lay 不渲染人物类合同；
5. `color_stats`/`evaluate_group_consistency`：合成偏差图判失败并归因、一致图通过、单图组不判失败；
6. `review_group_consistency`：任一维度 <85 失败、模型 group_passed_suggestion=true 仍可失败；
7. supply 集成：一致性失败并入定向重生、attempt_history 带 consistency_alignment、断点续跑保留证据、旧 manifest 兼容；
8. 全量回归：`python3 -m unittest discover tests`。

禁止在单测中真实调用 codex/Doubao；模型输出一律 fixture。

## 6. 真实样片验收标准

必须同时满足：
- 四张肤色与 persona 正脸图肉眼一致（"只有一张白"不再出现）；
- 四张色温/饱和度统一，仅服装主色不同；
- 构图按数值合同执行（人物占比/机位/地平线可核对）；
- 每套穿搭自评 ≥85 且用户认可成套协调；
- 程序化 + 视觉一致性检查全部通过并留 manifest 证据；
- 失败页面定向重生、已合格图片不重复付费生成；
- 全量测试通过。

用户最终判断两件事：① 四张放在一起像不像同一次出片；② 穿搭/构图是否达到"愿意发布"的水准。任一不满足则继续校准阈值与合同，不宣布完成。

## 7. 风险与边界

1. **codex 速率限制**：规划链路一次任务 6-10 次调用，超限回退 Doubao（manifest 可见）；若频繁回退，评估错峰或降 effort。
2. **规划增强不等于执行增强**：gpt-image-2 对色调/肤色的执行方差不会归零。若样片后 2/4 仍有残留，下一步选项：① 生成后统一调色对齐（Pillow LUT，便宜确定，建议先行）；② 首图过审后作为后续图的色调参考输入（只取色不取姿势的分离锚点）。届时按残留程度决策，不在本轮范围。
3. **数值阈值是初版**：色温/饱和度阈值基于经验值，Phase 4 样片后校准并固化到测试。
4. **不覆盖未提交修改**；仓库当前有大量未提交改动，禁止 reset/checkout 清理。
5. **密钥纪律**：codex token 走既有解析链，不写入代码/日志/fixture/文档；新 env 变量加入 `.env.local` 白名单加载。

## 8. 接手模型执行注意事项

1. 先读 `docs/TH_PERSONA_REALISM_OPTIMIZATION_HANDOFF_20260907.md`（人物体系已生效，本轮不重建）；
2. 按 Phase 1→3 开发，Phase 4 真实样片需单条新飞书记录，先 dry-run；
3. 自动化测试全绿前禁止真实付费调用（codex 探针一次除外）；
4. 不自动发布样片，等待用户看 5 页成片。

首条执行指令建议：

```text
请阅读 docs/PHOTO_PLANNING_UPGRADE_CODEX_20260907.md 和
docs/TH_PERSONA_REALISM_OPTIMIZATION_HANDOFF_20260907.md。
按前一个文档的 Phase 1-3 完成规划层 codex 接入、全局视觉计划和跨图一致性校验，
运行测试并报告改动。不要覆盖未提交修改，除一次 codex 探针外不要真实调用付费模型。
测试通过后再准备 Phase 4 真实样片。
```

---

## 9. 开发执行记录（2026-09-07 完成）

### Phase 1-3 已落地

- **codex_vision_client.py**：SSE 流式/429/5xx 重试一次/4xx 不重试；`stream=true, store=false` 无 max_output_tokens。
- **photo_reference_vision.py**：`OPV_PHOTO_VISION_PROVIDER`（默认 codex）+ 显式 doubao 回退；所有调用统一 `parse_vision_envelope`（JSON 后附文本容错、裸数组兼容）；`_chat(prefer="fast")` QA 走 Doubao。
- **规划升级**：`_analysis_prompt` 产出 `color_grading_plan`（色温/饱和/肤色权威源）+ 每 look `palette_hex` + `outfit_aesthetic` 自评（<85 重规划一次，两次报错）；`camera_params` 数值化构图（占比/焦段/机位角度/地平线）随 pose_contract 冻结；`compose_shot_prompt` 渲染【全局色彩合同】+ 构图数值。
- **photo_color_consistency.py**：程序化组内互比（R/B 比极差>0.18/饱和度σ>0.06/亮度极差>28 拦截，中位数偏离归因）+ `evaluate_visual_consistency`（模型建议不信任）。
- 测试：`tests/test_photo_planning_upgrade.py` 等，photo 域 223 项全绿。

### Phase 4 真实样片（4 次迭代）与三项后续修改

1. `recvuwDUs3FsTz`：排队+一次领取失败+creatok 不在 cron PATH（locked.sh 由并行会话修复 export PATH；本会话加 spawn FileNotFoundError 重试 3 次）；断点续跑完成——暴露**视觉一致性失败未触发修复**的闭环 bug，已修+测试。
2. `recvuwXrW8QPX3`：暴露 **Look A 单张门禁走 Doubao 被"四张动作不同"组级要求误导**——已修：单张门禁（FIRST_LOOK_*）保留 codex + prompt 加 scope 澄清 + 宽容边界条款（单品级细节不作失败理由）。
3. 同片第二次：**2 轮修复用尽**——identity_drift 四张全挂（AI persona+AI 生成链路的能力常态）、肤色微差重生不收敛。结论：QA 标准超出执行能力=付费死循环 → 引出 QA 瘦身。
4. **QA 瘦身**（`OPV_PHOTO_QA_LEVEL`，默认 standard）：只拦硬灾难（OBVIOUS 歪头/STATIC_MANNEQUIN/AI 脸三项/肢体畸形）；identity_drift 只记录；视觉一致性降级为提示（程序化粗粒度检查保留）；MAX_GROUP_REPAIR_ATTEMPTS=1。`strict` 可切回。
5. **性能优化**（单篇 12-15→约 8-11 分钟）：QA 调用 Doubao（5-10s）；B/C/D 与 Look A 门禁预取并行（2 workers，仅无生成图锚点路径；门禁彻底失败时预取结果不入 manifest）；扫描器 4 槽位锁+记录级 flock（`_RecordFlock`，不同记录可并行）。
6. 插单：参考图格式自动转换（`normalize_image_bytes`，WebP/BMP/TIFF/HEIC→JPEG q92+EXIF 摆正，pillow-heif 已装）。
7. 生产验证样片 `recvuxcl7g53XE`：**11 分钟完成**（含 1 轮定向修复），qa_level=standard 生效，四动作族/creatok 通道/视觉一致性全过。

### 遗留与边界

- 肤色统一若仍不达预期：正确路径是**发布前整体调色（LUT）**，不再靠重生循环（两轮已证伪）。
- `test_feishu_v2`/`test_production_batch` 顶层导入式失败的中间态属并行会话在改的领域，未在本轮处理。
- 与并行会话的撞车合并点：`reference_palette`（参考图签名配色）、locked.sh PATH、旅行鞋履校验——均已合并修复。
- 全量测试以 photo 域 223 项 + workflow 19 项为准（本会话改动面）。
