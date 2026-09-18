"""账号自动供稿（Phase 2，方案 §十一）：库存补货、名额预留、幂等建任务。

接入方式（§九）：**写飞书任务表行**——与运营手动建行完全同构，由现有扫描器
（feishu_workflow.scan）正常消费与校验。本模块不直插 RDS 业务表、不复制
workflow、不触碰生成与发布状态机；对既有代码的唯一依赖是任务表字段契约。

幂等两层：
1. 台账 ``supply_slots``（account_id × supply_date × slot 唯一，事务预留）；
2. 每轮先按「来源标记」列对账飞书表已建行（覆盖台账与飞书之间崩溃的窗口），
   slot 编号 ≤ 已建行数的名额直接跳过。标记放专用列而非备注：备注会被
   工作流在生成启动时清空/覆写，来源标记列工作流只读不写。

异常语义（§十四）：素材不足记缺口跳过（不建行）；商品资料/预设缺失记缺口
暂停该账号；单账号异常不阻塞其他账号。

字段契约与 ``services/feishu_workflow.py`` 保持一致（改动需两侧同步）：
产品编码 / 生产预设 / 执行 / 生成篇数 / 图文主题 / 内容要求（可选）/
目标账号（可选） / 完整穿搭素材（可选） / 备注。
"""
from __future__ import annotations

import re

import os
from dataclasses import dataclass, field
from datetime import date as _date
from typing import Any, Dict, List, Optional, Sequence

from services.material_adapter import (
    Candidate,
    ProductBrief,
    SelectionResult,
    narrow_candidates,
    parse_thermal_band,
    select_reference,
    thermal_overlap,
    thermal_window,
    theme_thermal_band,
)
from services.material_analysis import ANALYSIS_VERSION, MaterialLedger
from services.material_source import MaterialPackage, MaterialSource
from services.publish_account_profile import (
    SUPPLY_AUTOMATION_OFF,
    SUPPLY_PRODUCT_MODE_SPECIFIED,
    SUPPLY_STRATEGY_POSITIONING_FIRST,
    PublishAccountBinding,
)
from services.external_supply_contract import (
    SUPPLY_POLICY_VERSION,
    contract_fingerprint,
    default_contract_store_path,
)

# 与 services/feishu_workflow.py 的字段契约同值（模块解耦，改动需两侧同步）；
# 「来源标记」列由 scripts/ensure_feishu_task_table.py 补建——备注会被工作流
# 在生成过程中覆写（feishu_workflow 启动时清空备注），不能承载幂等标记。
FIELD_PRODUCT = "产品编码"
FIELD_PRESET = "生产预设"
FIELD_EXECUTE = "执行"
FIELD_QUANTITY = "生成篇数"
FIELD_CONTENT_THEME = "图文主题"
FIELD_CONTENT_REQUIREMENT = "内容要求（可选）"
FIELD_TARGET_ACCOUNT = "目标账号（可选）"
FIELD_PHOTO_INPUT = "完整穿搭素材（可选）"
FIELD_NOTES = "备注"
FIELD_SOURCE_TAG = "来源标记"
# 外部第三方参考的执行路径（Phase 1 来源分流）：参考图走「参考图（可选）」
# + 显式「风格参考」类型，由现有 STYLE 规划/生成路径消费；
# 「完整穿搭素材」只属于运营人工完整素材，外部 reference_only 永不进入。
FIELD_REFERENCE = "参考图（可选）"
FIELD_REFERENCE_TYPE = "参考图类型"
FIELD_TRAVEL_COUNTRY = "旅行国家"
# 温度档（SingleSelect，选项 15/10/5/0°C 左右）：供给明确知道本篇温度带时
# 落行字段，保证文案声明的温度与选品/选材约束同源。
FIELD_TEMPERATURE_BAND = "温度档"
_TEMPERATURE_BAND_OPTIONS = ("15°C 左右", "10°C 左右", "5°C 左右", "0°C 左右")

MARKER_PREFIX = "auto_supply"
MAX_REFERENCE_IMAGES = 6
DEFAULT_DAILY_LIMIT = 1
DEFAULT_TARGET_INVENTORY = 12


def supply_marker(today: str, account_id: str, slot: int = 0) -> str:
    """来源标记：`auto_supply|日期|账号[|slotN]`。

    旧三段格式仍被对账解析兼容（按前缀计数）；追加的 slot 段用于
    建行后回绑合同的对账定位（方案 A2：绑定失败可恢复）。
    """
    base = f"{MARKER_PREFIX}|{today}|{account_id}"
    return f"{base}|slot{int(slot)}" if slot else base


def parse_marker_slot(marker: str) -> int:
    """从来源标记解析 slot 段；旧格式/无段返回 0。"""
    for part in str(marker or "").split("|"):
        if part.startswith("slot") and part[4:].isdigit():
            return int(part[4:])
    return 0


#: B2 通用结构预设（已验证纯通用：PHOTO_TH_PICK_YOUR_LOOK_V3）
GENERIC_CHOICE_PRESET = "图文｜TH｜四选一穿搭"


def _nontravel_theme(topic: str) -> str:
    """非旅行选题→结构性主题映射（photo_theme._PROFILES 现成项）。

    只决定四页执行结构的文案族，不注入旅行/温度语境；配色/日常类
    默认日常通勤，季节/约会关键词就近映射。
    """
    text = str(topic or "")
    if any(k in text for k in ("秋", "秋冬", "春")):
        return "秋季穿搭"
    if any(k in text for k in ("约会", "咖啡", "甜美", "裙")):
        return "咖啡约会"
    return "日常通勤"


def _product_anchored_statement(topic: str, product_snapshot: Dict[str, Any]) -> str:
    """B2 §4.1：指定商品时，本篇主张围绕本店商品重写。

    参考原题的「颜色+材质+品类」短语（奶油色羊羔毛短外套/灰色卫衣）是
    参考事实，照搬会让规划与生图围绕参考商品展开（样片实测：指定浅蓝
    外套渲染成奶油色）。做法：剥离参考的颜色+材质+长短修饰+品类名词，
    换上本店商品的颜色+品类；场景/搭配/季节等中性词保留。
    无商品事实时仅返回泛化题。
    """
    text = _generalize_reference_topic(topic)
    name = str(product_snapshot.get("product_name") or "")
    category = str(product_snapshot.get("category") or "")
    color = next((token for token in ("浅蓝", "奶白", "米白", "黑色", "白色",
                                      "棕色", "灰色", "焦糖", "驼色", "深蓝",
                                      "藏蓝", "红色", "绿色")
                  if token in name), "")
    if not (name or category):
        return text
    color_re = "浅蓝|奶白|米白|奶油色|奶油|黑色|白色|灰色|驼色|棕色|焦糖|深蓝|藏蓝|红色|绿色"
    garment_re = ("(奶油色|奶白|米白|浅蓝|深蓝|藏蓝|黑色|白色|灰色|驼色|棕色|焦糖|红色|绿色)?"
                  "(羊羔毛|羊毛|蓬松|羽绒|针织|毛呢|灯芯绒|麂皮|皮革|摇粒绒|抓绒|棉)?"
                  "(短|长|中|宽松|oversize|修身)?(款|式)?"
                  "(外套|大衣|开衫|卫衣|毛衣|衬衫|夹克|风衣|羽绒服|棉服|棉衣)")
    text = re.sub(garment_re, "单品", text)
    text = re.sub(f"({color_re})?单品", "单品", text)
    text = re.sub(r"单品{2,}", "单品", text)
    anchor = "".join(x for x in (color, category if category != "outerwear" else "外套")
                     if x) or (name or "本店商品")
    tail = re.sub(r"^单品[^，。；]*?(?=的)", "", text).strip()
    tail = re.sub(r"^的", "", tail).strip() or "多种日常搭配展示"
    return f"以本店{anchor}为核心：{tail}"


def _generalize_reference_topic(topic: str) -> str:
    """把参考原题里依赖其原始组图规模的计数泛化（B2：不能复制「42套」）。

    42套→多套、18图→组图、第2期/9款等同样收敛；参考的商品颜色等
    事实性描述保留（商品一致性由商品快照另行权威约束）。
    """
    import re
    text = str(topic or "")
    text = re.sub(r"\d+\s*套", "多套", text)
    text = re.sub(r"\d+\s*图", "组图", text)
    text = re.sub(r"\d+\s*款", "多款", text)
    text = re.sub(r"\d+\s*个\s*(look|Look|造型)", r"多个\1", text)
    text = re.sub(r"[第]\d+\s*(期|弹|篇)", "", text)
    return text.strip() or text


def _city_country_of(name: str) -> str:
    from services.publish_account_profile import _city_country
    return _city_country(str(name or ""))


def worker_identity() -> str:
    """名额租约的 worker 标识：host#pid#随机短码（日志可对账）。"""
    import socket
    import uuid
    return f"{socket.gethostname()}#{os.getpid()}#{uuid.uuid4().hex[:8]}"


def _notes_text(value: Any) -> str:
    if isinstance(value, list):
        return "".join(
            part.get("text", "") if isinstance(part, dict) else str(part)
            for part in value
        )
    return str(value or "")


@dataclass
class SlotPlan:
    account_id: str
    slot: int
    status: str                 # created / already_created / dry_run / no_material / error
    record_id: Optional[str] = None
    product_code: str = ""
    main_note_id: str = ""
    adoption: str = ""
    detail: str = ""
    contract_id: str = ""
    pages: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class AccountRunResult:
    account_id: str
    account_name: str
    status: str                 # supplied / skipped_off / skipped_no_preset / limit_reached / no_material / error
    detail: str = ""
    slots: List[SlotPlan] = field(default_factory=list)


def pick_product_code(codes: Sequence[str], usage_counts: Dict[str, int]) -> str:
    """简单轮换（§六）：按历史使用次数升序，次数同则按配置顺序。"""
    if not codes:
        return ""
    return min(codes, key=lambda code: (usage_counts.get(code, 0), codes.index(code)))


class AutoPhotoSupply:
    """每轮供稿执行器。dry_run=True 时不做任何写操作与模型调用。"""

    def __init__(
        self,
        *,
        client: Any,                  # 飞书任务表客户端（list_records/batch_create_records/upload_attachment）
        source: MaterialSource,
        ledger: MaterialLedger,
        vision_client: Any = None,    # Doubao 选材调用；dry-run 不需要
        analyzer: Any = None,         # MaterialAnalyzer；apply 时按需补分析
        model: str = "",
        today: Optional[str] = None,
        max_reference_images: int = MAX_REFERENCE_IMAGES,
        contract_store: Any = None,   # ExternalSupplyContractStore；None=同库默认
        product_snapshot_resolver: Any = None,  # callable(code)->snapshot dict
        thermal_band: str = "",       # 显式温度带（如 "15-22°C"）；空=按主题推导
    ):
        self.client = client
        self.source = source
        self.ledger = ledger
        self.vision_client = vision_client
        self.analyzer = analyzer
        self.model = model
        self.today = today or _date.today().isoformat()
        self.max_reference_images = max_reference_images
        self.lease_owner = worker_identity()
        if contract_store is None:
            from services.external_supply_contract import ExternalSupplyContractStore
            contract_store = ExternalSupplyContractStore()
        self.contract_store = contract_store
        self.product_snapshot_resolver = product_snapshot_resolver
        self.thermal_band = parse_thermal_band(thermal_band) if thermal_band else None

    # ---- 主入口 ----
    def run(
        self,
        accounts: Sequence[PublishAccountBinding],
        *,
        apply: bool = True,
    ) -> List[AccountRunResult]:
        rows_by_marker: Dict[str, int] = {}
        inventory_by_handle: Dict[str, int] = {}
        budget: Dict[str, int] = {}
        rows_by_slot: Dict[tuple, List[str]] = {}
        unknown_by_handle: Dict[str, int] = {}
        rows_anyday: Dict[tuple, List[str]] = {}
        if apply:
            (rows_by_marker, inventory_by_handle, rows_by_slot,
             rows_anyday, unknown_by_handle) = self._scan_task_rows()
            budget = self.ledger.daily_call_usage(self.today)
            # 方案 §7.1：跨日对账 submitting 合同——按合同自身日期/账号/slot
            # 找行补绑；查空继续保留未知，不释放重提交、不重建。
            try:
                submissions = self.contract_store.list_pending_submissions()
            except Exception:  # noqa: BLE001
                submissions = []
            for contract in submissions:
                cid = str(contract.get("contract_id") or "")
                date = str(contract.get("supply_date") or "")
                account = str(contract.get("account_id") or "")
                slot_no = int(contract.get("slot") or 0)
                record_ids = rows_anyday.get((date, account, slot_no), [])
                if len(record_ids) == 1:
                    self.contract_store.attach_record(cid, record_ids[0])
                    try:
                        self.ledger.complete_slot(account, date, slot_no,
                                                  record_id=record_ids[0])
                    except Exception:  # noqa: BLE001
                        pass
                elif len(record_ids) > 1:
                    self.ledger.record_gap(
                        scope=f"supply:{account}", reason="marker_ambiguous",
                        detail=f"submitting {cid} 匹配 {len(record_ids)} 行")
                # 查空：保持 submitting（未知不当作未创建）
            # 方案 A2：先对账未完成绑定——行已建但合同未绑（attach 失败/
            # 响应未知）的，唯一匹配补绑；多匹配记异常，不猜不删不重建。
            for (account_id, slot_no), record_ids in sorted(rows_by_slot.items()):
                if len(record_ids) != 1:
                    self.ledger.record_gap(
                        scope=f"supply:{account_id}", reason="marker_ambiguous",
                        detail=f"slot{slot_no} 匹配到 {len(record_ids)} 行："
                               + ",".join(record_ids[:5]))
                    continue
                contract_id = f"{account_id}|{self.today}|{slot_no}"
                try:
                    contract = self.contract_store.get(contract_id)
                except Exception:  # noqa: BLE001 - 读取异常按错误处理不重建
                    self.ledger.record_gap(
                        scope=f"supply:{account_id}", reason="contract_read_error",
                        detail=contract_id)
                    continue
                if (contract is not None and contract.get("status") != "created"
                        and not contract.get("record_id")):
                    self.contract_store.attach_record(contract_id, record_ids[0])
                    try:
                        self.ledger.complete_slot(
                            account_id, self.today, slot_no, record_id=record_ids[0])
                    except Exception:  # noqa: BLE001 - 行存在即占名额，台账缺失可后补
                        pass
        results: List[AccountRunResult] = []
        for binding in accounts:
            try:
                results.append(self._run_account(
                    binding, apply, rows_by_marker, inventory_by_handle, budget,
                    unknown_by_handle))
            except Exception as exc:  # noqa: BLE001 - 单账号异常不阻塞其他账号
                self.ledger.record_gap(
                    scope=f"supply:{binding.account_id}", reason="account_error",
                    detail=str(exc)[:300])
                results.append(AccountRunResult(
                    account_id=binding.account_id,
                    account_name=binding.account_name,
                    status="error", detail=str(exc)[:200]))
        return results

    # ---- 对账：统计飞书表里当日已建的自动行 ----
    # 优先读「来源标记」（工作流不覆写）；兼容旧数据的备注前缀作为兜底。
    #: 库存口径（方案 §9/A4）：生成中＋合格待发＋排队中；已发布/废弃/失败
    #: 不计。动态进度文本（素材生成 3/4 等）由 _inventory_pieces 规范化识别。
    INVENTORY_ACTIVE_STATES = frozenset({
        "待执行", "规划中", "准备素材", "素材生成", "质检中", "生成中",
        "人物表现质检中", "待审核", "待排班", "已排期", "提交中", "发布中",
        "已完成"})
    #: 终态（0 库存）：已发布（含部分发布后的剩余按篇数折算前的整行）、
    #: 废弃/需处理/发布失败
    INVENTORY_ZERO_STATES = frozenset({
        "已发布", "需处理", "发布失败", "已取消", "失败可重试"})

    def _scan_task_rows(self) -> tuple:
        """一次全表扫描同时产出：当日自动行对账 + 各账号待发库存。

        手工及往日任务都计入库存（按目标账号归属）；进度列是文本或
        富文本两种形态都兼容。
        """
        marker_counts: Dict[str, int] = {}
        inventory: Dict[str, int] = {}
        unknown: Dict[str, int] = {}
        rows_by_slot: Dict[tuple, List[str]] = {}          # 当日
        rows_by_slot_anyday: Dict[tuple, List[str]] = {}   # 全日期（跨日对账 §7.1）
        for record in self.client.list_records(page_size=500):
            marker = _notes_text(record.fields.get(FIELD_SOURCE_TAG))
            if not marker:
                notes = _notes_text(record.fields.get(FIELD_NOTES))
                marker = notes if notes.startswith(MARKER_PREFIX) else ""
            if marker.startswith(MARKER_PREFIX):
                parts = marker.split("|")
                if len(parts) >= 3 and parts[0] == MARKER_PREFIX:
                    slot_no = parse_marker_slot(marker)
                    if slot_no:
                        rows_by_slot_anyday.setdefault(
                            (parts[1], parts[2], slot_no), []).append(record.record_id)
                    if parts[1] == self.today:
                        marker_counts[parts[2]] = marker_counts.get(parts[2], 0) + 1
                        if slot_no:
                            rows_by_slot.setdefault((parts[2], slot_no), []).append(
                                record.record_id)
            handle = _notes_text(record.fields.get(FIELD_TARGET_ACCOUNT)).strip()
            progress = _notes_text(record.fields.get("进度")).strip()
            executing = bool(record.fields.get(FIELD_EXECUTE))
            pieces, known = self._inventory_pieces(record.fields, progress, executing)
            if handle and known and pieces > 0:
                inventory[handle] = inventory.get(handle, 0) + pieces
            elif handle and not known:
                unknown[handle] = unknown.get(handle, 0) + 1
        return marker_counts, inventory, rows_by_slot, rows_by_slot_anyday, unknown

    @classmethod
    def _inventory_pieces(cls, fields: Dict[str, Any], progress: str,
                          executing: bool) -> tuple:
        """一行折算库存篇数（方案 A4：4 张图=1 篇，按篇数不按行）。

        返回 (pieces, known)：known=False 表示进度无法可靠分类——
        该账号按“库存未知”处理，不按零计（不新增自动任务）。
        - 已发布/需处理/发布失败 → 0；
        - 进度空但 执行=True → 视为待执行（排队中），计满篇数；
        - 动态文本（素材生成 3/4 / 质检修复等）视为在制，计满篇数；
        - 篇数取「生成篇数」字段（缺省 1）。
        """
        try:
            qty = max(1, int(float(str(fields.get("生成篇数") or 1))))
        except (TypeError, ValueError):
            qty = 1
        if progress in cls.INVENTORY_ZERO_STATES:
            return 0, True
        if progress in cls.INVENTORY_ACTIVE_STATES:
            return qty, True
        import re
        if re.match(r"^(素材生成|文案生成|风格质检修复|人物质检修复)\s*\d+\s*/\s*\d+", progress):
            return qty, True     # 在制动态文本：计满篇数（生成中）
        if not progress:
            return (qty, True) if executing else (0, True)
        return 0, False          # 未识别进度：库存未知，交由上层保守处理

    def _count_existing_auto_rows(self) -> Dict[str, int]:
        return self._scan_task_rows()[0]

    def _run_account(
        self, binding: PublishAccountBinding, apply: bool,
        rows_by_marker: Dict[str, int],
        inventory_by_handle: Optional[Dict[str, int]] = None,
        budget: Optional[Dict[str, int]] = None,
        unknown_by_handle: Optional[Dict[str, int]] = None,
    ) -> AccountRunResult:
        policy = binding.supply_policy
        result = AccountRunResult(
            account_id=binding.account_id, account_name=binding.account_name,
            status="supplied")
        # 方案 §7.2：库存未知只暂停本账号新增，不阻塞其他账号/续跑
        if apply and unknown_by_handle and unknown_by_handle.get(binding.account_id):
            n_unknown = unknown_by_handle[binding.account_id]
            self.ledger.record_gap(
                scope=f"supply:{binding.account_id}",
                reason="inventory_unknown",
                detail=f"{n_unknown} 行进度无法识别，暂停该账号新增")
            result.status = "inventory_unknown"
            result.detail = f"库存未知（{n_unknown} 行进度无法分类）"
            return result
        if policy.get("automation") == SUPPLY_AUTOMATION_OFF:
            result.status = "skipped_off"
            return result
        preset = str(policy.get("preset") or "").strip()
        if not preset:
            self.ledger.record_gap(
                scope=f"supply:{binding.account_id}", reason="no_preset",
                detail="automation 开启但未配置自动供稿预设，暂停该账号")
            result.status = "skipped_no_preset"
            return result

        limit = int(policy.get("daily_limit") or 0) or DEFAULT_DAILY_LIMIT
        created = rows_by_marker.get(binding.account_id, 0)
        if created >= limit:
            result.status = "limit_reached"
            result.detail = f"今日已建 {created}/{limit}"
            return result

        # 调用预算（方案 §9）：分析+终选调用计入当日额度，失败重试也计入
        if apply and budget is not None:
            usage_calls = int(budget.get("calls") or 0)
            usage_images = int(budget.get("images") or 0)
            cap_calls = int(os.environ.get("OPV_SUPPLY_DAILY_CALL_CAP") or 300)
            cap_images = int(os.environ.get("OPV_SUPPLY_DAILY_IMAGE_CAP") or 3000)
            if usage_calls >= cap_calls or usage_images >= cap_images:
                result.status = "budget_exhausted"
                result.detail = (f"当日调用 {usage_calls}/{cap_calls}、"
                                 f"图片 {usage_images}/{cap_images}，预算用尽")
                return result

        # 待发库存（方案 §9）：缺口=目标库存-当前库存，新增量受缺口约束
        if apply and inventory_by_handle is not None:
            target = int(policy.get("target_inventory") or 0) or DEFAULT_TARGET_INVENTORY
            current = inventory_by_handle.get(binding.account_id, 0)
            room = max(0, target - current)
            if room <= 0:
                result.status = "inventory_full"
                result.detail = f"待发库存 {current} ≥ 目标 {target}，本轮不补"
                return result
            limit = min(limit, created + room)

        codes = list(policy.get("product_codes") or [])
        specified = policy.get("product_mode") == SUPPLY_PRODUCT_MODE_SPECIFIED
        scope_themes = [t for t in (policy.get("material_scope") or []) if t] or None
        default_theme = str(binding.profile.get("default_theme") or "").strip()
        positioning_first = policy.get("content_strategy") == SUPPLY_STRATEGY_POSITIONING_FIRST

        for slot in range(created + 1, limit + 1):
            plan = self._run_slot(
                binding=binding, slot=slot, preset=preset, codes=codes,
                specified=specified, scope_themes=scope_themes,
                default_theme=default_theme, positioning_first=positioning_first,
                apply=apply)
            result.slots.append(plan)
            if plan.status == "leased_elsewhere":
                result.status = "leased_elsewhere"
                result.detail = plan.detail
                break
            if plan.status == "no_material":
                result.status = "no_material"
                break
            if plan.status == "error":
                result.status = "error"
                result.detail = plan.detail
                break
            if plan.status == "created":
                rows_by_marker[binding.account_id] = (
                    rows_by_marker.get(binding.account_id, 0) + 1)
        if result.status == "supplied" and not any(
                s.status in {"created", "dry_run"} for s in result.slots):
            result.status = "limit_reached"
        return result

    # ---- 单名额 ----
    def _run_slot(
        self, *, binding: PublishAccountBinding, slot: int, preset: str,
        codes: List[str], specified: bool, scope_themes: Optional[List[str]],
        default_theme: str, positioning_first: bool, apply: bool,
    ) -> SlotPlan:
        plan = SlotPlan(account_id=binding.account_id, slot=slot, status="dry_run")

        if apply:
            # 执行租约（评审 §D：名额唯一键≠锁，双 worker 会重复执行 reserved）。
            # acquired 后所有提前返回路径都会释放租约（finally），created 由
            # complete_slot 清理。
            lease_owner = self.lease_owner
            lease = self.ledger.acquire_slot_lease(
                binding.account_id, self.today, slot, owner=lease_owner)
            if lease == "already_created":
                plan.status = "already_created"
                plan.detail = "台账已完成（对账跳过）"
                return plan
            if lease == "held_by_other":
                plan.status = "leased_elsewhere"
                plan.detail = "名额由其他 worker 持有（租约未过期）"
                return plan
            try:
                return self._run_slot_locked(
                    binding=binding, slot=slot, preset=preset, codes=codes,
                    specified=specified, scope_themes=scope_themes,
                    default_theme=default_theme, positioning_first=positioning_first,
                    lease_owner=lease_owner)
            except Exception:
                self.ledger.release_slot_lease(
                    binding.account_id, self.today, slot, owner=lease_owner)
                raise
        return self._produce_slot_plan(
            binding=binding, slot=slot, preset=preset, codes=codes,
            specified=specified, scope_themes=scope_themes,
            default_theme=default_theme, positioning_first=positioning_first,
            apply=False)

    def _run_slot_locked(
        self, *, binding: PublishAccountBinding, slot: int, preset: str,
        codes: List[str], specified: bool, scope_themes: Optional[List[str]],
        default_theme: str, positioning_first: bool, lease_owner: str,
    ) -> SlotPlan:
        plan = self._produce_slot_plan(
            binding=binding, slot=slot, preset=preset, codes=codes,
            specified=specified, scope_themes=scope_themes,
            default_theme=default_theme, positioning_first=positioning_first,
            apply=True, lease_owner=lease_owner)
        if plan.status != "created":
            # 失败/无素材/错误：释放租约回 reserved，等待下轮重试
            self.ledger.release_slot_lease(
                binding.account_id, self.today, slot, owner=lease_owner)
        return plan

    def _produce_slot_plan(
        self, *, binding: PublishAccountBinding, slot: int, preset: str,
        codes: List[str], specified: bool, scope_themes: Optional[List[str]],
        default_theme: str, positioning_first: bool, apply: bool,
        lease_owner: str = "",
    ) -> SlotPlan:
        plan = SlotPlan(account_id=binding.account_id, slot=slot, status="dry_run")

        # 合同恢复（评审 §D「冻结 A、执行 B」）：未绑定行的 intent 合同优先，
        # 冻结输入（商品/温度带/目的地/主参考/采用/页）全部复用，不重新随机选材。
        frozen: Optional[Dict[str, Any]] = None
        if apply:
            try:
                frozen = self.contract_store.get(
                    f"{binding.account_id}|{self.today}|{slot}")
            except Exception:  # noqa: BLE001 - 合同库异常按无冻结处理，走新建
                frozen = None
            if frozen is not None and frozen.get("status") not in (
                    "intent", "submitting"):
                frozen = None
            if frozen is not None and frozen.get("status") == "submitting":
                # 上次提交结果未知：本轮跨日对账未找到行——保留未知，
                # 不重新选材也不重新提交（方案 §7.1）
                plan.status = "submit_unknown"
                plan.detail = "上次建行结果未知（submitting），等待对账，不重建"
                return plan

        # 本篇温度带：显式参数 > 主题默认（未映射主题=None → 不启用检查）
        band = self.thermal_band or theme_thermal_band(default_theme)
        if frozen is not None:
            frozen_band = parse_thermal_band(frozen.get("temperature_band"))
            if frozen_band:
                band = frozen_band

        # 指定商品（先解析，真实摘要同时供初筛与终选）：
        # - 热学门禁：商品适用窗口与本篇温度带不重叠 → 换下一个轮换编码；
        # - 全部失配 → 记缺口不建行（不静默降级为自由搭配）；
        # - 解析失败同样顺延，全部失败才明确暂停。
        product_code = ""
        product_snapshot: Dict[str, Any] = {}
        product_brief: Optional[ProductBrief] = None
        if frozen is not None:
            frozen_product = dict(frozen.get("product") or {})
            if frozen_product.get("code"):
                product_code = str(frozen_product["code"])
                product_snapshot = {
                    "product_name": frozen_product.get("name") or "",
                    "category": frozen_product.get("category") or "",
                    "variant_key": frozen_product.get("variant") or "",
                }
                pname = str(product_snapshot["product_name"])
                product_brief = ProductBrief(
                    product_code, str(product_snapshot["category"] or ""),
                    form=str(product_snapshot["variant_key"] or ""),
                    key_features=[pname] if pname else [])
        if specified and not product_code:
            usage_counts = self.ledger.product_usage_counts(binding.account_id)
            ordered_codes = sorted(
                codes, key=lambda c: (usage_counts.get(c, 0), codes.index(c)))
            if not ordered_codes:
                self.ledger.record_gap(
                    scope=f"supply:{binding.account_id}", reason="no_product_code",
                    detail="specified 模式但未配置产品编码")
                plan.status = "no_material"
                plan.detail = "商品使用方式=指定但无产品编码"
                return plan
            if not apply:
                product_code = ordered_codes[0]
            elif self.product_snapshot_resolver is None:
                self.ledger.record_gap(
                    scope=f"supply:{binding.account_id}", reason="product_resolver_missing",
                    detail="指定商品模式需要商品解析器（RDS 商品参考包）")
                plan.status = "error"
                plan.detail = "指定商品但无商品解析器，暂停本任务"
                return plan
            else:
                accepted = None
                failures: List[str] = []
                thermal_skips: List[str] = []
                for code in ordered_codes:
                    try:
                        snapshot = dict(self.product_snapshot_resolver(code) or {})
                    except Exception as exc:  # noqa: BLE001 - 单码失败顺延下一码
                        failures.append(f"{code}: {str(exc)[:120]}")
                        continue
                    window = thermal_window(
                        snapshot.get("product_name"), snapshot.get("category"),
                        snapshot.get("variant_key"))
                    if band and window and not thermal_overlap(window, band):
                        thermal_skips.append(
                            f"{code}（适用{window[0]}-{window[1]}°C）")
                        continue
                    accepted = (code, snapshot)
                    break
                if accepted is None:
                    if thermal_skips and not failures:
                        self.ledger.record_gap(
                            scope=f"supply:{binding.account_id}",
                            reason="no_thermal_match",
                            detail=(f"本篇温度带 {band[0]}-{band[1]}°C，全部商品失配："
                                    + "、".join(thermal_skips)))
                        plan.status = "no_material"
                        plan.detail = (f"全部商品与温度带 {band[0]}-{band[1]}°C 失配，"
                                       "本轮不建行（请补充应季商品）")
                        return plan
                    self.ledger.record_gap(
                        scope=f"supply:{binding.account_id}",
                        reason="product_snapshot_failed",
                        detail="；".join(failures + thermal_skips)[:280])
                    plan.status = "error"
                    plan.detail = "商品资料缺失或全部失配，暂停本任务"
                    return plan
                product_code, product_snapshot = accepted
            if product_code and product_snapshot:
                product_name = str(product_snapshot.get("product_name") or "")
                product_brief = ProductBrief(
                    product_code,
                    str(product_snapshot.get("category") or ""),
                    form=str(product_snapshot.get("variant_key") or ""),
                    key_features=[product_name] if product_name else [])
        # effective_brief 前置（方案 B1）：筛候选前解析本篇有效需求，
        # 贯穿初筛/终选/内容计划/合同；选材后仅回填推导项。
        # 温度带只来自明确事实（显式参数/冻结合同/主题预设的公开含义），
        # 来源记录在 band_source——不自动发明温度。
        if self.thermal_band:
            band_source = "显式参数"
        elif frozen is not None and parse_thermal_band(frozen.get("temperature_band")):
            band_source = "冻结合同"
        elif theme_thermal_band(default_theme):
            band_source = f"主题预设（{default_theme}）"
        else:
            band_source = ""
        effective_brief = {
            "account_id": binding.account_id,
            "market": str(binding.target_country or ""),
            "positioning": str(binding.profile.get("positioning") or "")[:120],
            "content_strategy": (
                "positioning_first" if positioning_first else "reference_first"),
            "theme": {"value": default_theme, "source": (
                "账号默认" if default_theme else "待定（参考推导）")},
            "product": {
                "mode": ("specified" if specified else "unspecified"),
                "code": product_code,
                "category": str(product_snapshot.get("category") or ""),
                "name": str(product_snapshot.get("product_name") or ""),
            },
            "visual_preset": str(binding.profile.get("default_visual_preset") or ""),
            "destination": {
                "country": str(binding.profile.get("travel_country") or "").strip(),
                "place": str(binding.profile.get("travel_place") or "").strip(),
            },
            "temperature_band": {
                "value": (f"{band[0]}-{band[1]}°C" if band else ""),
                "source": band_source,
            },
            "output": {"preset": preset, "quantity": 1},
        }
        # B2 §2/§3：账号目的地范围（一次配置；未配置→空列表保持原行为）
        destinations = [
            str(d.get("id") or "")
            for d in (binding.profile.get("travel_destinations") or [])
            if isinstance(d, dict) and d.get("id")
        ]
        narrow_brief = product_brief or (
            ProductBrief(product_code, "") if product_code else None)

        candidates = self._collect_candidates(scope_themes)
        recent = self.ledger.recent_note_ids(account_id=binding.account_id)
        narrowed = self._narrow(
            candidates, default_theme if positioning_first else "",
            narrow_brief, recent, band)
        if not narrowed and apply and self.analyzer is not None:
            # 方案 C4：0 候选先做一次有限补分析（正式入口，限 5 篇），
            # 再判缺口——不能提前 return 把"未分析"当"无素材"。
            self.analyzer.analyze_pending(limit=5)
            candidates = self._collect_candidates(scope_themes)
            narrowed = self._narrow(
                candidates, default_theme if positioning_first else "",
                narrow_brief, recent, band)
        if not narrowed:
            self.ledger.record_gap(
                scope=f"supply:{binding.account_id}", reason="no_material",
                detail="无可用候选（补分析后仍不足或全部被拒）")
            # 方案 C1：缺口即异步采集需求（相同需求在台账合并计数）
            self.ledger.record_demand({
                "account_id": binding.account_id,
                "theme_direction": (
                    default_theme
                    or str(narrow_brief.category if narrow_brief else "")
                    or "自由选题"),
                "purposes": ["outfit", "visual", "narrative"],
                "product_form": str(
                    (product_snapshot or {}).get("category") or ""),
                "destination_country": str(
                    effective_brief.get("destination", {}).get("country") or ""),
                "destination_use": "environment_inspiration",
            })
            plan.status = "no_material"
            plan.detail = "无可用候选素材"
            return plan

        if not apply:
            plan.main_note_id = narrowed[0].note_id
            plan.detail = (f"dry-run：候选 {len(narrowed)}，首选 {narrowed[0].note_id}"
                           f"（不调用模型不建行）")
            return plan

        if self.analyzer is not None and len(narrowed) < 3:
            self.analyzer.analyze_pending(limit=5)
            candidates = self._collect_candidates(scope_themes)
            narrowed = self._narrow(
                candidates, default_theme if positioning_first else "",
                narrow_brief, recent, band)

        if self.vision_client is None:
            plan.status = "error"
            plan.detail = "apply 模式需要 vision_client（选材调用）"
            return plan

        # 长调用（模型终选）前续租，避免选材期间租约过期被接管
        if lease_owner and not self.ledger.renew_slot_lease(
                binding.account_id, self.today, slot, owner=lease_owner):
            plan.status = "leased_elsewhere"
            plan.detail = "租约已丢失（选材前续租失败）"
            return plan

        selection: Optional[Any] = None
        frozen_main = str((frozen or {}).get("main_note_id") or "")
        if frozen is not None and frozen_main and frozen_main in candidates:
            # 恢复路径：主参考/采用/页来自冻结合同，跳过模型终选
            selection = SelectionResult(
                main_note_id=frozen_main,
                supplement_note_ids=[],
                adoption=str(frozen.get("adoption") or "overall"),
                rationale="[合同恢复] 复用冻结输入，不重新选材",
                pages=[{
                    "note_id": frozen_main,
                    "seq": int(page.get("seq") or 0),
                    "purpose": str(page.get("purpose") or ""),
                } for page in (frozen.get("selected_pages") or [])])
        else:
            # 方案 A3：模型发起前原子预留额度；成功/失败/未知都占用
            cap_calls = int(os.environ.get("OPV_SUPPLY_DAILY_CALL_CAP") or 300)
            if not self.ledger.reserve_budget(
                    purpose="selection", cap=cap_calls,
                    note=f"{binding.account_id} slot{slot}"):
                plan.status = "budget_exhausted"
                plan.detail = f"终选额度已满（当日 {cap_calls} 次）"
                return plan
            try:
                selection = select_reference(
                    self.vision_client, narrowed,
                    theme=default_theme if positioning_first else "",
                    product=product_brief,
                    temperature_band=band,
                    effective_brief=effective_brief,
                    allowed_destinations=(
                        destinations if not positioning_first else ()))
                self.ledger.settle_budget(purpose="selection", state="consumed")
            except Exception:
                self.ledger.settle_budget(purpose="selection", state="consumed",
                                          note="调用失败仍计额度")
                raise
            # 终选调用入账（方案 §9 预算口径；恢复路径不调模型不入账）
            self.ledger.log_supply_call(purpose="selection", model=self.model)
        if selection is None:
            self.ledger.record_gap(
                scope=f"supply:{binding.account_id}", reason="no_material",
                detail="模型未选出合适主参考")
            plan.status = "no_material"
            plan.detail = "模型未选出主参考"
            return plan

        package_pair = candidates.get(selection.main_note_id)
        if package_pair is None:
            plan.status = "error"
            plan.detail = "主参考不在候选集合（内部错误）"
            return plan
        package_obj, main_analysis = package_pair[0], package_pair[1]
        if not main_analysis:
            plan.status = "error"
            plan.detail = "主参考缺少分析缓存（内部错误）"
            return plan

        # 选题、来源说明、参考图、摘要全部来自同一选材结果（修复串配：
        # 此前主题取 narrowed[0] 而图片取模型终选，可能不是同一篇）。
        # B2：source_topic 保留参考原题；topic_statement 是适配本篇的主张
        # ——参考专属计数（42套/18图）不得照搬进本篇文案语境。
        source_topic = str(main_analysis.get("note_topic") or "")
        topic = _generalize_reference_topic(source_topic or selection.rationale[:40])
        if product_snapshot:
            # §4.1：指定商品时主张以本店商品事实重写（压制参考品类/颜色词）
            topic = _product_anchored_statement(topic, product_snapshot)
        attachments, selected_pages = self._stage_reference_pages(
            package_obj, selection)

        # 目的地：账号显式配置优先；未配置则留空（执行侧行字段为准）。
        # 恢复路径：冻结合同的目的地优先（保证与已冻结输入一致）。
        destination = {
            "country": str(binding.profile.get("travel_country") or "").strip(),
            "place": str(binding.profile.get("travel_place") or "").strip(),
        }
        if frozen is not None:
            frozen_destination = dict(frozen.get("destination") or {})
            if frozen_destination:
                destination = {
                    "country": str(frozen_destination.get("country") or ""),
                    "place": str(frozen_destination.get("place") or ""),
                }

        # B2 §3：目的地决策——本篇明确(行字段/冻结合同) > 账号范围内选择 >
        # 终选返回(参考优先) > 无。定位优先旅行主题在终选前程序轮换。
        destination_pick = dict(destination)     # 现值=冻结合同或账号 travel_country
        if destination_pick.get("country") and not destination_pick.get("place"):
            # 账号单值可能是城市名（首尔）→ 归一为国家+城市（§2.2 层级兼容）
            raw = str(destination_pick["country"])
            country = _city_country_of(raw)
            if country and raw != country:
                destination_pick = {"country": country, "place": raw}
        destination_source = ("冻结合同" if frozen is not None
                              and destination.get("country") else
                              "账号单值" if destination.get("country") else "")
        if not positioning_first and selection is not None:
            picked = str(getattr(selection, "destination", "") or "")
            if picked:
                destination_pick = {"country": _city_country_of(picked),
                                    "place": picked}
                destination_source = "参考终选"
        travelish_flag = any(k in f"{source_topic}{selection.rationale}"
                             for k in ("旅行", "旅游", "出游")) if selection else False
        # 主题需要地点：定位优先的旅行向默认主题，或参考选题本身旅行向
        theme_needs_place = travelish_flag or (
            positioning_first and "旅行" in str(default_theme or ""))
        if (not destination_pick.get("country") and destinations
                and theme_needs_place):
            # 程序轮换：近期少用优先，同分稳定取首个（§3.2，零模型调用）
            picked = self._rotate_destination(
                binding.account_id, destinations)
            destination_pick = {"country": _city_country_of(picked), "place": picked}
            destination_source = "账号范围轮换"
        destination = destination_pick

        # B2：选题性质决定执行结构——真旅行选题走旅行预设；其余切通用
        # 结构（图文｜TH｜四选一穿搭，已验证纯通用：零 travel 键/choice-card/
        # reference_contract_v1）。非旅行不注入旅行语境，也不声明温度带
        # （B1：不发明温度）。定位优先缺主题同样在此提炼，不再一票报错。
        theme_value = default_theme or ""
        theme_derived_note = ""
        effective_preset = preset
        if not theme_value:
            structure = str(main_analysis.get("set_structure") or "")
            topic_blob = f"{source_topic}{selection.rationale}"
            travelish = any(k in topic_blob for k in ("旅行", "旅游", "出游"))
            if structure == "same_item_multiway" and product_code:
                theme_value = "一衣多穿"
                theme_derived_note = "内容方向以参考素材为基准（同件多搭结构：主题=一衣多穿）｜"
                band = band or theme_thermal_band(theme_value)
            elif travelish:
                theme_value = "凉爽旅行"
                theme_derived_note = "内容方向以参考素材为基准（旅行选题：主题=凉爽旅行）｜"
                band = band or theme_thermal_band(theme_value)
            else:
                # 通用结构：非旅行主题（结构性映射，无旅行/温度注入）。
                # §4.2：本篇明确温度（显式参数/冻结合同）不得被通用结构
                # 清除——只有随主题推导出的温度才随之消失。
                effective_preset = GENERIC_CHOICE_PRESET
                theme_value = _nontravel_theme(source_topic)
                theme_derived_note = (
                    f"内容方向以参考素材为基准（通用结构：主题={theme_value}）｜")
                if band_source not in ("显式参数", "冻结合同"):
                    band = None

        requirement = theme_derived_note + self._content_requirement_text(
            selection=selection, analysis=main_analysis, topic=topic,
            destination=destination, product=product_snapshot,
            temperature_band=band)

        # B1：brief 在筛候选前已组装（见 narrow_brief 前）；此处只回填
        # 选材后才能确定的部分——最终主题、本篇主张、目的地冻结值。
        effective_brief["theme"] = {"value": theme_value, "source": (
            "账号默认" if default_theme else "参考推导")}
        effective_brief["product"] = {
            "mode": "specified" if specified else "unspecified",
            "code": product_code,
            "category": str(product_snapshot.get("category") or ""),
            "name": str(product_snapshot.get("product_name") or ""),
        }
        effective_brief["destination"] = dict(destination)
        if destination_source:
            effective_brief["destination_source"] = destination_source
        effective_brief["temperature_band"] = {
            "value": (f"{band[0]}-{band[1]}°C" if band else ""),
            "source": band_source,
        }
        topic_statement = topic
        effective_brief["source_topic"] = source_topic
        effective_brief["output"] = {"preset": effective_preset, "quantity": 1}

        contract = {
            "account_id": binding.account_id,
            "supply_date": self.today,
            "slot": slot,
            "source_type": "xhs_reference",
            "authorization": "reference_only",
            "adoption": selection.adoption,
            "main_note_id": selection.main_note_id,
            "main_note_title": package_obj.title or topic,
            "selected_pages": selected_pages,
            "product": {
                "code": product_code,
                "name": str(product_snapshot.get("product_name") or ""),
                "category": str(product_snapshot.get("category") or ""),
                "variant": str(product_snapshot.get("variant_key") or ""),
            } if product_code else {},
            "destination": {k: v for k, v in destination.items() if v},
            "temperature_band": (f"{band[0]}-{band[1]}°C" if band else ""),
            "topic_statement": topic_statement,
            "effective_brief": effective_brief,
            "content_requirement": requirement,
            "policy_version": SUPPLY_POLICY_VERSION,
        }
        contract["contract_fingerprint"] = contract_fingerprint(
            adoption=contract["adoption"],
            main_note_id=contract["main_note_id"],
            selected_pages=selected_pages,
            product=contract["product"],
            destination=contract["destination"],
            policy_version=SUPPLY_POLICY_VERSION,
            temperature_band=contract["temperature_band"],
            topic_statement=topic_statement)
        # 建行前先持久化供稿意图（崩溃可恢复）；执行侧付费前凭此合同放行。
        # 冻结合同不可恢复（主参考已不可用等）时用新输入覆盖 intent，
        # 消除「冻结 A、执行 B」。
        if frozen is not None:
            self.contract_store.supersede_intent(
                dict(contract, contract_id=f"{binding.account_id}|{self.today}|{slot}"))
            stored = self.contract_store.get(
                f"{binding.account_id}|{self.today}|{slot}") or contract
        else:
            stored = self.contract_store.persist_intent(contract)
        plan.contract_id = str(stored.get("contract_id")
                               or f"{binding.account_id}|{self.today}|{slot}")
        if stored.get("status") == "created" and stored.get("record_id"):
            plan.status = "already_created"
            plan.record_id = str(stored["record_id"])
            plan.detail = "合同已绑定行（幂等恢复）"
            return plan

        fields: Dict[str, Any] = {
            FIELD_PRESET: effective_preset,
            FIELD_EXECUTE: True,
            FIELD_QUANTITY: 1,
            # 目标账号下拉选项是账号 handle（= account_id，如 tocrystal66），
            # 不是 account_name（如 泰国女装1）——写名字会命中不存在的选项
            FIELD_TARGET_ACCOUNT: binding.account_id or binding.account_name,
            FIELD_SOURCE_TAG: supply_marker(self.today, binding.account_id, slot),
            FIELD_NOTES: (
                f"自动供稿 slot{slot}"
                f"｜主参考 {selection.main_note_id}|采用 {selection.adoption}"
                f"|{selection.rationale[:60]}"),
            # 来源分流：外部 reference_only 参考显式走「风格参考」，由现有
            # STYLE 规划/生成路径消费；绝不写「完整穿搭素材」字段。
            FIELD_REFERENCE_TYPE: "风格参考",
            FIELD_CONTENT_THEME: theme_value,
            FIELD_CONTENT_REQUIREMENT: requirement,
        }
        if product_code:
            fields[FIELD_PRODUCT] = product_code
        if band:
            # 温度档落行字段，与文案声明的温度同源（15-22 → 「15°C 左右」）
            band_option = next(
                (opt for opt in _TEMPERATURE_BAND_OPTIONS
                 if opt.startswith(f"{band[0]}°C")), "")
            if band_option:
                fields[FIELD_TEMPERATURE_BAND] = band_option
        if destination.get("country"):
            # 执行侧从行字段读目的地（本篇明确值）；不覆盖为空值
            fields[FIELD_TRAVEL_COUNTRY] = destination["country"]
        # 注：不写「旅行地点（可选）」——文案模板的 destination 走已审核
        # 枚举标签（locale destinations），中文城市名会令 copy 填充失败；
        # 具体选址由合同/brief/内容要求承载（2026-09-18 样片A/B实测）
        if attachments:
            fields[FIELD_REFERENCE] = attachments

        # 方案 A2：外部写入前复核租约——丢失所有权的 worker 不能再建行
        if lease_owner and not self.ledger.renew_slot_lease(
                binding.account_id, self.today, slot, owner=lease_owner):
            plan.status = "leased_elsewhere"
            plan.detail = "建行前租约复核失败（所有权已丢失）"
            return plan

        # 方案 §7.1：外部请求发送前写提交意图；空响应/超时/中断保持
        # submitting——不释放重提交，由下轮跨日对账补绑或保留未知
        self.contract_store.mark_submitting(plan.contract_id)
        record_ids = self.client.batch_create_records([{"fields": fields}])
        record_id = record_ids[0] if record_ids else ""
        if not record_id:
            plan.status = "submit_unknown"
            plan.detail = "建行响应未知，合同保持 submitting 待对账"
            return plan
        self.contract_store.attach_record(plan.contract_id, record_id)
        self.ledger.complete_slot(
            binding.account_id, self.today, slot,
            record_id=record_id, product_code=product_code,
            main_note_id=selection.main_note_id,
            adoption=selection.adoption,
            note=fields[FIELD_NOTES], owner=lease_owner)
        self.ledger.record_usage(
            note_id=selection.main_note_id, account_id=binding.account_id,
            task_ref=record_id, adoption=selection.adoption)
        plan.status = "created"
        plan.record_id = record_id
        plan.product_code = product_code
        plan.main_note_id = selection.main_note_id
        plan.adoption = selection.adoption
        plan.pages = selected_pages
        return plan

    # ---- 候选收集（包 + 分析缓存配对）与初筛 ----
    def _collect_candidates(
        self, scope_themes: Optional[List[str]]
    ) -> Dict[str, tuple]:
        packages = self.source.list_packages(themes=scope_themes)
        out: Dict[str, tuple] = {}
        for package in packages:
            analysis = self.ledger.get_cached_analysis(
                package.version_fingerprint, self.model, ANALYSIS_VERSION)
            out[package.note_id] = (package, analysis)
        return out

    def _narrow(self, candidates: Dict[str, tuple], theme: str,
                brief: Optional[ProductBrief], recent: set,
                band: Optional[tuple] = None) -> List[Candidate]:
        packages = [pair[0] for pair in candidates.values()]
        analyses = {note_id: pair[1] for note_id, pair in candidates.items() if pair[1]}
        return narrow_candidates(
            packages, analyses, theme=theme, product=brief,
            recent_note_ids=recent, temperature_band=band)

    def _rotate_destination(self, account_id: str, destinations: List[str]) -> str:
        """账号范围内程序轮换（§3.2）：近 14 天合同少用优先，同分稳定取首。"""
        from collections import Counter
        used = Counter()
        try:
            rows = self.ledger._conn.execute(
                "SELECT note FROM supply_slots WHERE account_id=?"
                " AND updated_at >= datetime('now','-14 days')", (account_id,)
            ).fetchall()
            for row in rows:
                text = str(row["note"] or "")
                for d in destinations:
                    if d in text:
                        used[d] += 1
        except Exception:  # noqa: BLE001
            pass
        return min(destinations, key=lambda d: (used.get(d, 0),
                                                 destinations.index(d)))

    # ---- 页级供图（Phase 1/3）：只上传选材结果指定的页面 ----
    def _stage_reference_pages(
        self, package: MaterialPackage, selection: Any,
    ) -> tuple:
        """按选材结果的页引用供图；narrative_only 或无页引用时不发任何原图。

        返回 (attachments, selected_pages)；selected_pages 携带每页 sha256，
        进入执行合同（合同指纹的一部分）。不再固定取前 N 张。
        """
        import hashlib

        adoption = str(selection.adoption or "overall")
        if adoption == "narrative_only":
            return [], []
        by_seq = {img.seq: img for img in package.images}
        attachments: List[Dict[str, Any]] = []
        selected: List[Dict[str, Any]] = []
        for page in (selection.pages or [])[: self.max_reference_images]:
            image = by_seq.get(int(page.get("seq") or 0))
            if image is None or not image.exists:
                continue
            attachments.append(self.client.upload_attachment(
                image.path.read_bytes(), image.path.name, "image/jpeg",
                image.path.stat().st_size))
            selected.append({
                "note_id": selection.main_note_id,
                "seq": image.seq,
                "sha256": hashlib.sha256(image.path.read_bytes()).hexdigest(),
                "purpose": str(page.get("purpose") or ""),
            })
        return attachments, selected

    # ---- 内容要求（Phase 2）：adoption 语义 + 目的地 + 温度带 + 商品约束 ----
    @staticmethod
    def _content_requirement_text(
        *, selection: Any, analysis: Dict[str, Any], topic: str,
        destination: Dict[str, Any], product: Dict[str, Any],
        temperature_band: Optional[tuple] = None,
    ) -> str:
        adoption = str(selection.adoption or "overall")
        lines: List[str] = [f"参考优先选题：{topic}"]
        adoption_rules = {
            "outfit_only": (
                "采用方式 outfit_only：只借鉴参考的搭配关系（单品组合/层次/"
                "比例/配色），人物、场景、构图必须重新创作，禁止复刻参考画面"),
            "visual_only": (
                "采用方式 visual_only：只借鉴参考的色调与摄影气质，人物造型与"
                "场景按本篇主题重新创作"),
            "narrative_only": (
                "采用方式 narrative_only：只借鉴参考的选题与页面结构，全部画面"
                "按本篇主题独立创作（参考结构：" +
                str(analysis.get("set_structure") or "") + "）"),
            "overall": (
                "采用方式 overall：参考仅作综合灵感，重新形成本篇计划，"
                "禁止逐页复现参考组图"),
        }
        lines.append(adoption_rules.get(adoption, adoption_rules["overall"]))
        if adoption in {"outfit_only", "narrative_only", "overall"}:
            relations = str(analysis.get("outfit_relations") or "")
            core = "、".join(
                str((item or {}).get("item") or "")
                for item in (analysis.get("core_items") or [])[:6])
            if core:
                lines.append(f"参考搭配要点（文字化借鉴）：{core}"
                             + (f"；{relations}" if relations else ""))
        country = str(destination.get("country") or "")
        place = str(destination.get("place") or "")
        if country or place:
            dest = "、".join(x for x in (country, place) if x)
            lines.append(f"旅行目的地设定：{dest}；场景按目的地重新规划，"
                         "不照搬参考图拍摄地")
        if temperature_band:
            lines.append(
                f"温度带 {temperature_band[0]}–{temperature_band[1]}°C：主单品须为"
                "该温度带适用品类（如薄外套/开衫/夹克/长袖/衬衫），"
                "禁止羽绒、棉服、蓬松外套等厚重冬装，也不采用夏装")
        if product:
            pname = str(product.get("product_name") or "")
            pcat = str(product.get("category") or "")
            if pname or pcat:
                lines.append(f"本篇商品以本店资料为准：{pcat} {pname}".strip())
        return "｜".join(lines)[:1500]

