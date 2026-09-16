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
    ):
        self.client = client
        self.source = source
        self.ledger = ledger
        self.vision_client = vision_client
        self.analyzer = analyzer
        self.model = model
        self.today = today or _date.today().isoformat()
        self.max_reference_images = max_reference_images
        if contract_store is None:
            from services.external_supply_contract import ExternalSupplyContractStore
            contract_store = ExternalSupplyContractStore()
        self.contract_store = contract_store
        self.product_snapshot_resolver = product_snapshot_resolver

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
    # 优先读「来源标记」（工作流不覆写）；兼容旧数据的备注前缀作为兜底。
    def _count_existing_auto_rows(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for record in self.client.list_records(page_size=500):
            marker = _notes_text(record.fields.get(FIELD_SOURCE_TAG))
            if not marker:
                notes = _notes_text(record.fields.get(FIELD_NOTES))
                marker = notes if notes.startswith(MARKER_PREFIX) else ""
            if not marker.startswith(MARKER_PREFIX):
                continue
            parts = marker.split("|")
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
                plan.status = "already_created"   # 台账已完成（对账兜底）
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

        # 指定商品：先解析真实商品摘要（品类/名称/variant），解析失败明确
        # 暂停本任务——不再以 ProductBrief(code, "") 假装商品匹配已完成。
        product_snapshot: Dict[str, Any] = {}
        product_brief: Optional[ProductBrief] = None
        if product_code:
            if self.product_snapshot_resolver is None:
                self.ledger.record_gap(
                    scope=f"supply:{binding.account_id}", reason="product_resolver_missing",
                    detail="指定商品模式需要商品解析器（RDS 商品参考包）")
                plan.status = "error"
                plan.detail = "指定商品但无商品解析器，暂停本任务"
                return plan
            try:
                product_snapshot = dict(
                    self.product_snapshot_resolver(product_code) or {})
            except Exception as exc:  # noqa: BLE001 - 商品资料缺失＝暂停，不降级为自由搭配
                self.ledger.record_gap(
                    scope=f"supply:{binding.account_id}", reason="product_snapshot_failed",
                    detail=f"{product_code}: {str(exc)[:200]}")
                plan.status = "error"
                plan.detail = f"商品资料缺失（{product_code}），暂停本任务：{str(exc)[:80]}"
                return plan
            product_name = str(product_snapshot.get("product_name") or "")
            product_brief = ProductBrief(
                product_code,
                str(product_snapshot.get("category") or ""),
                form=str(product_snapshot.get("variant_key") or ""),
                key_features=[product_name] if product_name else [])

        selection = select_reference(
            self.vision_client, narrowed,
            theme=default_theme if positioning_first else "",
            product=product_brief)
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
        topic = str(main_analysis.get("note_topic") or selection.rationale[:40])
        attachments, selected_pages = self._stage_reference_pages(
            package_obj, selection)

        # 目的地：账号显式配置优先；未配置则留空（执行侧行字段为准）。
        destination = {
            "country": str(binding.profile.get("travel_country") or "").strip(),
            "place": str(binding.profile.get("travel_place") or "").strip(),
        }

        requirement = self._content_requirement_text(
            selection=selection, analysis=main_analysis, topic=topic,
            destination=destination, product=product_snapshot)
        theme_value = default_theme or ""
        if not theme_value:
            # STYLE 执行路径必须携带图文主题（feishu_workflow 硬校验）；
            # 参考优先但未配置默认主题＝配置缺口，明确报错不静默。
            self.ledger.record_gap(
                scope=f"supply:{binding.account_id}", reason="no_theme",
                detail="外部参考走风格参考路径需要账号默认图文主题")
            plan.status = "error"
            plan.detail = "账号未配置默认图文主题（STYLE 路径必需），暂停本任务"
            return plan

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
            "content_requirement": requirement,
            "policy_version": SUPPLY_POLICY_VERSION,
        }
        contract["contract_fingerprint"] = contract_fingerprint(
            adoption=contract["adoption"],
            main_note_id=contract["main_note_id"],
            selected_pages=selected_pages,
            product=contract["product"],
            destination=contract["destination"],
            policy_version=SUPPLY_POLICY_VERSION)
        # 建行前先持久化供稿意图（崩溃可恢复）；执行侧付费前凭此合同放行。
        stored = self.contract_store.persist_intent(contract)
        plan.contract_id = str(stored.get("contract_id") or "")
        if stored.get("status") == "created" and stored.get("record_id"):
            plan.status = "already_created"
            plan.record_id = str(stored["record_id"])
            plan.detail = "合同已绑定行（幂等恢复）"
            return plan

        fields: Dict[str, Any] = {
            FIELD_PRESET: preset,
            FIELD_EXECUTE: True,
            FIELD_QUANTITY: 1,
            # 目标账号下拉选项是账号 handle（= account_id，如 tocrystal66），
            # 不是 account_name（如 泰国女装1）——写名字会命中不存在的选项
            FIELD_TARGET_ACCOUNT: binding.account_id or binding.account_name,
            FIELD_SOURCE_TAG: supply_marker(self.today, binding.account_id),
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
        if destination.get("country"):
            # 执行侧从行字段读目的地（本篇明确值）；不覆盖为空值
            fields[FIELD_TRAVEL_COUNTRY] = destination["country"]
        if destination.get("place"):
            fields["旅行地点（可选）"] = destination["place"]
        if attachments:
            fields[FIELD_REFERENCE] = attachments

        record_ids = self.client.batch_create_records([{"fields": fields}])
        record_id = record_ids[0] if record_ids else ""
        self.contract_store.attach_record(plan.contract_id, record_id)
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
                product_code: str, recent: set) -> List[Candidate]:
        packages = [pair[0] for pair in candidates.values()]
        analyses = {note_id: pair[1] for note_id, pair in candidates.items() if pair[1]}
        return narrow_candidates(
            packages, analyses, theme=theme,
            product=ProductBrief(product_code, "") if product_code else None,
            recent_note_ids=recent)

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

    # ---- 内容要求（Phase 2）：adoption 语义 + 目的地 + 商品约束进执行提示 ----
    @staticmethod
    def _content_requirement_text(
        *, selection: Any, analysis: Dict[str, Any], topic: str,
        destination: Dict[str, Any], product: Dict[str, Any],
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
        if product:
            pname = str(product.get("product_name") or "")
            pcat = str(product.get("category") or "")
            if pname or pcat:
                lines.append(f"本篇商品以本店资料为准：{pcat} {pname}".strip())
        return "｜".join(lines)[:1500]

