"""账号自动供稿（Phase 2，方案 §十一）：库存补货、名额预留、幂等建任务。

接入方式（§九）：**写飞书任务表行**——与运营手动建行完全同构，由现有扫描器
（feishu_workflow.scan）正常消费与校验。本模块不直插 RDS 业务表、不复制
workflow、不触碰生成与发布状态机；对既有代码的唯一依赖是任务表字段契约。

幂等两层：
1. 台账 ``supply_slots``（account_id × supply_date × slot 唯一，事务预留）；
2. 每轮先按「备注 marker」对账飞书表已建行（覆盖台账与飞书之间崩溃的窗口），
   slot 编号 ≤ 已建行数的名额直接跳过。

异常语义（§十四）：素材不足记缺口跳过（不建行）；商品资料/预设缺失记缺口
暂停该账号；单账号异常不阻塞其他账号。

字段契约与 ``services/feishu_workflow.py`` 保持一致（改动需两侧同步）：
产品编码 / 生产预设 / 执行 / 生成篇数 / 图文主题 / 内容要求（可选）/
目标账号（可选） / 完整穿搭素材（可选） / 备注。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date as _date
from typing import Any, Dict, List, Optional, Sequence

from services.material_adapter import (
    Candidate,
    ProductBrief,
    narrow_candidates,
    select_reference,
)
from services.material_analysis import ANALYSIS_VERSION, MaterialLedger
from services.material_source import MaterialPackage, MaterialSource
from services.publish_account_profile import (
    SUPPLY_AUTOMATION_OFF,
    SUPPLY_PRODUCT_MODE_SPECIFIED,
    SUPPLY_STRATEGY_POSITIONING_FIRST,
    PublishAccountBinding,
)

# 与 services/feishu_workflow.py 的字段契约常量同值（模块解耦，改动需两侧同步）
FIELD_PRODUCT = "产品编码"
FIELD_PRESET = "生产预设"
FIELD_EXECUTE = "执行"
FIELD_QUANTITY = "生成篇数"
FIELD_CONTENT_THEME = "图文主题"
FIELD_CONTENT_REQUIREMENT = "内容要求（可选）"
FIELD_TARGET_ACCOUNT = "目标账号（可选）"
FIELD_PHOTO_INPUT = "完整穿搭素材（可选）"
FIELD_NOTES = "备注"

MARKER_PREFIX = "auto_supply"
MAX_REFERENCE_IMAGES = 6
DEFAULT_DAILY_LIMIT = 1


def supply_marker(today: str, account_id: str) -> str:
    return f"{MARKER_PREFIX}|{today}|{account_id}"


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
    status: str                 # created / skipped_limit / dry_run / no_material / error
    record_id: Optional[str] = None
    product_code: str = ""
    main_note_id: str = ""
    adoption: str = ""
    detail: str = ""


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
    ):
        self.client = client
        self.source = source
        self.ledger = ledger
        self.vision_client = vision_client
        self.analyzer = analyzer
        self.model = model
        self.today = today or _date.today().isoformat()
        self.max_reference_images = max_reference_images

    # ---- 主入口 ----
    def run(
        self,
        accounts: Sequence[PublishAccountBinding],
        *,
        apply: bool = True,
    ) -> List[AccountRunResult]:
        rows_by_marker: Dict[str, int] = {}
        if apply:
            rows_by_marker = self._count_existing_auto_rows()
        results: List[AccountRunResult] = []
        for binding in accounts:
            try:
                results.append(self._run_account(binding, apply, rows_by_marker))
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
    def _count_existing_auto_rows(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for record in self.client.list_records(page_size=500):
            notes = _notes_text(record.fields.get(FIELD_NOTES))
            if not notes.startswith(MARKER_PREFIX):
                continue
            parts = notes.split("|")
            if len(parts) >= 3 and parts[0] == MARKER_PREFIX and parts[1] == self.today:
                counts[parts[2]] = counts.get(parts[2], 0) + 1
        return counts

    def _run_account(
        self, binding: PublishAccountBinding, apply: bool,
        rows_by_marker: Dict[str, int],
    ) -> AccountRunResult:
        policy = binding.supply_policy
        result = AccountRunResult(
            account_id=binding.account_id, account_name=binding.account_name,
            status="supplied")
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
            reservation = self.ledger.reserve_slot(
                binding.account_id, self.today, slot)
            if reservation == "created":
                plan.status = "skipped_limit"   # 台账显示已完成（对账兜底）
                plan.detail = "台账已完成（对账跳过）"
                return plan

        product_code = ""
        if specified:
            product_code = pick_product_code(
                codes, self.ledger.product_usage_counts(binding.account_id))
            if not product_code:
                self.ledger.record_gap(
                    scope=f"supply:{binding.account_id}", reason="no_product_code",
                    detail="specified 模式但未配置产品编码")
                plan.status = "no_material"
                plan.detail = "商品使用方式=指定但无产品编码"
                return plan

        candidates = self._collect_candidates(scope_themes)
        recent = self.ledger.recent_note_ids(account_id=binding.account_id)
        narrowed = self._narrow(candidates, default_theme if positioning_first else "",
                                product_code, recent)
        if not narrowed:
            self.ledger.record_gap(
                scope=f"supply:{binding.account_id}", reason="no_material",
                detail="无可用候选（分析缓存不足或全部被拒）")
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
            narrowed = self._narrow(candidates, default_theme if positioning_first else "",
                                    product_code, recent)

        if self.vision_client is None:
            plan.status = "error"
            plan.detail = "apply 模式需要 vision_client（选材调用）"
            return plan
        selection = select_reference(
            self.vision_client, narrowed,
            theme=default_theme if positioning_first else "",
            product=ProductBrief(product_code, "") if product_code else None)
        if selection is None:
            self.ledger.record_gap(
                scope=f"supply:{binding.account_id}", reason="no_material",
                detail="模型未选出合适主参考")
            plan.status = "no_material"
            plan.detail = "模型未选出主参考"
            return plan

        package = candidates.get(selection.main_note_id)
        if package is None:
            plan.status = "error"
            plan.detail = "主参考不在候选集合（内部错误）"
            return plan
        package_obj = package[0]

        attachments = self._upload_references(package_obj)
        fields: Dict[str, Any] = {
            FIELD_PRESET: preset,
            FIELD_EXECUTE: True,
            FIELD_QUANTITY: 1,
            FIELD_TARGET_ACCOUNT: binding.account_name or binding.account_id,
            FIELD_NOTES: (
                f"{supply_marker(self.today, binding.account_id)}|slot{slot}"
                f"|主参考 {selection.main_note_id}|采用 {selection.adoption}"
                f"|{selection.rationale[:60]}"),
        }
        if product_code:
            fields[FIELD_PRODUCT] = product_code
        if positioning_first and default_theme:
            fields[FIELD_CONTENT_THEME] = default_theme
        else:
            topic = str((narrowed[0].analysis or {}).get("note_topic") or "")
            fields[FIELD_CONTENT_REQUIREMENT] = (
                f"参考优先选题：{topic or selection.rationale[:40]}")
        if attachments:
            fields[FIELD_PHOTO_INPUT] = attachments

        record_ids = self.client.batch_create_records([{"fields": fields}])
        record_id = record_ids[0] if record_ids else ""
        self.ledger.complete_slot(
            binding.account_id, self.today, slot,
            record_id=record_id, product_code=product_code,
            main_note_id=selection.main_note_id,
            adoption=selection.adoption,
            note=fields[FIELD_NOTES])
        self.ledger.record_usage(
            note_id=selection.main_note_id, account_id=binding.account_id,
            task_ref=record_id, adoption=selection.adoption)
        plan.status = "created"
        plan.record_id = record_id
        plan.product_code = product_code
        plan.main_note_id = selection.main_note_id
        plan.adoption = selection.adoption
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
                product_code: str, recent: set) -> List[Candidate]:
        packages = [pair[0] for pair in candidates.values()]
        analyses = {note_id: pair[1] for note_id, pair in candidates.items() if pair[1]}
        return narrow_candidates(
            packages, analyses, theme=theme,
            product=ProductBrief(product_code, "") if product_code else None,
            recent_note_ids=recent)

    def _upload_references(self, package: MaterialPackage) -> List[Dict[str, Any]]:
        attachments: List[Dict[str, Any]] = []
        for image in package.images[: self.max_reference_images]:
            if not image.exists:
                continue
            attachments.append(self.client.upload_attachment(
                image.path.read_bytes(), image.path.name, "image/jpeg",
                image.path.stat().st_size))
        return attachments
