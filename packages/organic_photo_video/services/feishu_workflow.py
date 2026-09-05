"""Compact Feishu workbench adapter for OPV image-story production.

Feishu is the operator surface; RDS remains the source of truth.  The adapter
uses the Feishu record id as the external idempotency namespace and never
publishes content.
"""

from __future__ import annotations

import hashlib
import json
import mimetypes
from dataclasses import asdict, dataclass, replace
from domain.models import ProductionBatch
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from services.asset_resolver import LightTryonAssetReader
from services.batch_diversity_planner import BatchDiversityPlanner
from services.content_story import generate_product_image_story
from services.hero_first import HeroFirstProducer
from services.image_generator import OpenAIImageGenerator
from services.product_reference_resolver import (
    ProductReferenceResolutionError,
    ProductReferenceResolver,
)
from services.video_render_flow import VideoRenderFlow
from services.video_renderer import FFmpegStillRenderer
from services.feishu_v2 import (
    FeishuV2Mixin, FIELD_REVIEW_MODE, FIELD_REVIEW_STAGE, FIELD_RETRY_REVIEW,
    FIELD_REVIEW_TOKEN, MODE_AUTO, MODE_HUMAN,
)
from services.workflow_v2 import workflow_v2_enabled
from services.production_batch import BatchLeaseBusy, ProjectionPendingError


SOURCE_TYPE = "feishu_opv"
FIELD_PRODUCT = "产品编码"
FIELD_PRESET = "生产预设"
FIELD_EXECUTE = "执行"
FIELD_PROGRESS = "进度"
FIELD_OUTPUT = "预览/成片"
FIELD_REVIEW = "审核"
FIELD_NOTES = "备注"
FIELD_QUANTITY = "生成数量"
FIELD_CONFIRM_PUBLISH = "确认发布"

PROGRESS_PENDING = "待执行"
PROGRESS_RUNNING = "生成中"
PROGRESS_REVIEW = "待审核"
PROGRESS_DONE = "已完成"
PROGRESS_ACTION = "需处理"
PROGRESS_QUEUED = "待排班"
PROGRESS_SCHEDULED = "已排期"
PROGRESS_PUBLISHING = "发布中"
PROGRESS_PUBLISHED = "已发布"
PROGRESS_PUBLISH_FAILED = "发布失败"

REVIEW_PENDING = "待审核"
REVIEW_APPROVED = "通过"
REVIEW_NOT_REQUIRED = "无需审核"
REVIEW_REDO_ALL = "整组重做"
REVIEW_SCHEDULE = "排期发布"


class FeishuWorkflowError(RuntimeError):
    pass


def dependent_redo_slots(plan: Dict[str, Any], requested_slots: Iterable[int]) -> List[int]:
    """Expand redo scope when a derived board's continuity anchor changes."""
    requested = sorted({int(value) for value in requested_slots})
    execution = plan.get("recipe_execution") or {}
    anchor = int(plan.get("anchor_slot") or 1)
    if execution.get("content_goal") == "outfit_breakdown" and anchor in requested:
        return list(range(1, 6))
    return requested


def text_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        parts = []
        for item in value:
            if isinstance(item, dict):
                parts.append(str(item.get("text") or item.get("name") or ""))
            else:
                parts.append(str(item))
        return "".join(parts).strip()
    return str(value).strip()


@dataclass(frozen=True)
class PresetTask:
    account_id: str
    market: str
    language: str
    recipe_id: str
    theme_id: str
    hook_strategy: str
    persona_ref: str
    look_ref: str
    scene_ref: str


class ProductionPresetCatalog:
    def __init__(self, path: Optional[Path] = None):
        default = Path(__file__).resolve().parents[1] / "config" / "feishu_production_presets.json"
        self.path = Path(path) if path else default
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        self.overlay_profile_id = str(payload.get("default_overlay_profile_id") or "")
        self._raw = {item["name"]: item for item in payload.get("presets", [])}
        if not self._raw:
            raise FeishuWorkflowError("no Feishu production presets configured")

    @property
    def names(self) -> List[str]:
        return list(self._raw)

    def resolve(self, name: str, record_id: str) -> List[PresetTask]:
        raw = self._raw.get(name)
        if raw is None:
            raise FeishuWorkflowError(f"未知生产预设：{name}")
        if raw.get("selection") == "deterministic_one":
            candidates = list(raw.get("tasks_from") or [])
            if not candidates:
                raise FeishuWorkflowError(f"预设没有候选项：{name}")
            digest = hashlib.sha256(record_id.encode("utf-8")).digest()
            return self.resolve(candidates[digest[0] % len(candidates)], record_id)
        tasks = [PresetTask(**item) for item in raw.get("tasks", [])]
        if not tasks:
            raise FeishuWorkflowError(f"预设没有生产任务：{name}")
        return tasks

    def resolve_batch(
        self, name: str, record_id: str, quantity: int
    ) -> List[PresetTask]:
        """Expand one operator row to exactly ``quantity`` deterministic units."""
        if quantity < 1 or quantity > 9:
            raise FeishuWorkflowError("生成数量必须是 1 到 9")
        raw = self._raw.get(name)
        if raw is None:
            raise FeishuWorkflowError(f"未知生产预设：{name}")
        if raw.get("selection") == "deterministic_one":
            candidates = list(raw.get("tasks_from") or [])
            if not candidates:
                raise FeishuWorkflowError(f"预设没有候选项：{name}")
            result: List[PresetTask] = []
            for index in range(quantity):
                digest = hashlib.sha256(
                    f"{record_id}:{index + 1}".encode("utf-8")
                ).digest()
                selected = candidates[digest[0] % len(candidates)]
                result.append(self.resolve(selected, f"{record_id}:{index + 1}")[0])
            return self._diversify_repeated_recipes(result)
        tasks = [PresetTask(**item) for item in raw.get("tasks", [])]
        if not tasks:
            raise FeishuWorkflowError(f"预设没有生产任务：{name}")
        return self._diversify_repeated_recipes(
            [tasks[index % len(tasks)] for index in range(quantity)]
        )

    def preview(self, name: str, record_id: str, quantity: int) -> Dict[str, Any]:
        """Cheap routing preview; actual library counts are only known after planning."""
        from services.multi_look_planner import MULTI_LOOK_RECIPE_ID
        specs = self.resolve_batch(name, record_id, quantity)
        return {"preset": name, "routing_policy": self._raw[name].get("routing_policy", "explicit_preset"),
                "count_status": "requested_not_yet_resolved",
                "note": "目标套数，不是实际套数；生成前按兼容穿搭库冻结并展示实际页数与时长。",
                "videos": [{"recipe_id": spec.recipe_id,
                            "requested_look_count": 5 if spec.recipe_id == MULTI_LOOK_RECIPE_ID else None,
                            "target_duration_ms": 6000 if spec.recipe_id == MULTI_LOOK_RECIPE_ID else None}
                           for spec in specs]}

    @staticmethod
    def _diversify_repeated_recipes(tasks: List[PresetTask]) -> List[PresetTask]:
        """Keep the preset's first intent; rotate hooks on later repetitions."""
        seen: Dict[str, int] = {}
        result = []
        for task in tasks:
            occurrence = seen.get(task.recipe_id, 0)
            seen[task.recipe_id] = occurrence + 1
            result.append(
                task if occurrence == 0 else replace(task, hook_strategy="")
            )
        return result


def quantity_value(value: Any) -> int:
    if value in (None, ""):
        return 1
    try:
        result = int(float(value))
    except (TypeError, ValueError) as exc:
        raise FeishuWorkflowError("生成数量必须是 1 到 9 的整数") from exc
    if result < 1 or result > 9 or float(value) != result:
        raise FeishuWorkflowError("生成数量必须是 1 到 9 的整数")
    return result


class FeishuTaskWorkflow(FeishuV2Mixin):
    def __init__(
        self,
        repository,
        client,
        *,
        catalog: Optional[ProductionPresetCatalog] = None,
        generator=None,
        renderer=None,
        publish_scheduler=None,
        product_reference_resolver=None,
        visual_qa_adapter=None,
        output_root=None,
        asset_readiness_gate=None,
    ):
        self.repository = repository
        self.client = client
        self.catalog = catalog or ProductionPresetCatalog()
        self.generator = generator or OpenAIImageGenerator()
        self.renderer = renderer or FFmpegStillRenderer()
        self.publish_scheduler = publish_scheduler
        self.visual_qa_adapter = visual_qa_adapter
        self.output_root = output_root
        self.asset_readiness_gate = asset_readiness_gate
        self._run_lease = None
        self.product_reference_resolver = (
            product_reference_resolver or ProductReferenceResolver(repository)
        )

    def scan(
        self,
        *,
        dry_run: bool = False,
        record_id: str = "",
        resume_running: bool = False,
    ) -> Dict[str, Any]:
        records = (
            [self.client.get_record(record_id)]
            if record_id
            else self.client.list_records(page_size=500)
        )
        report = {"scanned": len(records), "eligible": 0, "processed": [], "errors": []}
        for record in records:
            fields = record.fields or {}
            action = self._action(fields)
            if action in {"approve", "retry_review"} and self._v2_tasks(record.record_id):
                # A retained legacy review command is not permission to start
                # fresh media work under the new technical-only workflow.
                action = "generate" if bool(fields.get(FIELD_EXECUTE)) else ""
            if action == "schedule" and not bool(fields.get(FIELD_CONFIRM_PUBLISH)) and self._v2_tasks(record.record_id):
                action = ""  # V2 requires the dedicated explicit publish checkbox.
            if not dry_run and not action:
                try:
                    batch = self._batch(record.record_id)
                    if batch and batch.pending_fields_json:
                        pending = batch.pending_fields_json
                        self.client.update_record_fields(record.record_id, pending)
                        self.repository.acknowledge_batch_projection(batch.batch_id, pending)
                        report["processed"].append({"record_id": record.record_id, "action": "recover_projection"})
                        continue
                    if batch and batch.batch_status == "running" and batch.lease_until and text_value(fields.get(FIELD_PROGRESS)) == PROGRESS_RUNNING:
                        from datetime import datetime
                        if batch.lease_until <= datetime.utcnow():
                            self._write_fields(record.record_id, {FIELD_PROGRESS: PROGRESS_ACTION,
                                FIELD_EXECUTE: False, FIELD_REVIEW: REVIEW_PENDING,
                                FIELD_NOTES: "上次运行租约已过期；在途生成结果未知，未自动重跑。请核对 RDS 素材后显式续跑或返工。"})
                            continue
                except Exception as exc:
                    report["errors"].append({"record_id": record.record_id, "error": f"恢复工作台失败：{exc}"})
                    continue
            if (
                resume_running
                and record_id
                and text_value(fields.get(FIELD_PROGRESS)) == PROGRESS_RUNNING
            ):
                action = "generate"
            if not action:
                if (
                    callable(getattr(self.publish_scheduler, "get_task_state", None))
                    and text_value(fields.get(FIELD_PROGRESS))
                    in {PROGRESS_QUEUED, PROGRESS_SCHEDULED, PROGRESS_PUBLISHING}
                ):
                    synced = self.sync_publication_status(record.record_id, current_fields=fields)
                    if synced.get("updated"):
                        report["processed"].append(synced)
                continue
            report["eligible"] += 1
            if dry_run:
                result = {"record_id": record.record_id, "action": action}
                if action == "generate" and callable(getattr(self.catalog, "preview", None)):
                    try:
                        batch = self._batch(record.record_id)
                        if batch:
                            result["production_preview"] = {"source": "frozen_batch",
                                "note": "续跑沿用冻结清单，不按当前预设重新选配方。",
                                "recipe_ids": [entry["spec"]["recipe_id"] for entry in batch.manifest_json["entries"]]}
                        else:
                            existing = (self._tasks(record.record_id) if callable(getattr(
                                self.repository, "list_tasks_by_source_prefix", None)) else [])
                            if existing:
                                result["production_preview"] = {"source": "existing_tasks",
                                    "note": self._production_plan_note(existing)}
                            else:
                                result["production_preview"] = self.catalog.preview(
                                    text_value(fields.get(FIELD_PRESET)), record.record_id,
                                    quantity_value(fields.get(FIELD_QUANTITY)))
                    except Exception as exc:
                        result["preview_error"] = str(exc)
                report["processed"].append(result)
                continue
            try:
                batch = self._batch(record.record_id)
                if batch:
                    self._claim_batch(batch)
                if action == "generate":
                    result = self._generate(record)
                elif action == "auto_render":
                    result = self._approve_and_render(
                        record, approval_mode="operator_auto"
                    )
                elif action == "approve":
                    result = self._approve_and_render(record)
                elif action == "retry_review":
                    tasks = self._v2_tasks(record.record_id)
                    if not tasks:
                        raise FeishuWorkflowError("重试审核仅适用于 V2 分阶段任务")
                    result = self._advance_v2(record, tasks, automatic=self._review_mode(record) == MODE_AUTO, resume_generation=False)
                elif action == "schedule":
                    result = self._schedule_publish(record)
                else:
                    result = self._redo(record, action)
                report["processed"].append(result)
            except BatchLeaseBusy as exc:
                # A losing worker must not consume commands or overwrite the
                # active owner's workbench projection.
                report["processed"].append({"record_id": record.record_id, "action": "leased_skip", "reason": str(exc)})
            except ProjectionPendingError as exc:
                report["errors"].append({"record_id": record.record_id, "error": str(exc), "projection_pending": True})
            except Exception as exc:  # noqa: BLE001 - isolate workbench rows
                message = str(exc).strip()[:900] or exc.__class__.__name__
                failure_fields = {
                    FIELD_EXECUTE: False,
                    FIELD_PROGRESS: PROGRESS_ACTION,
                    FIELD_NOTES: message,
                    FIELD_RETRY_REVIEW: False,
                    FIELD_REVIEW: REVIEW_PENDING,
                }
                if action == "schedule":
                    failure_fields[FIELD_CONFIRM_PUBLISH] = False
                try:
                    current_batch = self._batch(record.record_id)
                    if self._v2_tasks(record.record_id) or (
                        current_batch and int(current_batch.manifest_json.get("workflow_version") or 1) >= 2
                    ):
                        failure_fields.update({FIELD_REVIEW: REVIEW_NOT_REQUIRED, FIELD_REVIEW_MODE: None})
                    self._write_fields(record.record_id, failure_fields)
                except Exception as projection_error:
                    message += f"；飞书回写失败，RDS 状态保留：{projection_error}"
                report["errors"].append({"record_id": record.record_id, "error": message})
            finally:
                if self._run_lease:
                    try:
                        self._run_lease.close()
                    except Exception as exc:
                        report["errors"].append({"record_id": record.record_id, "error": f"租约释放失败：{exc}"})
                    self._run_lease = None
        return report

    def _batch(self, record_id):
        reader = getattr(self.repository, "get_production_batch", None)
        return reader(record_id) if callable(reader) else None

    def _claim_batch(self, batch):
        if self._run_lease:
            self._run_lease.check()
            return
        from services.production_batch import BatchLease
        self._run_lease = BatchLease(self.repository, batch.batch_id)

    def _write_fields(self, record_id, fields):
        if self._run_lease:
            self._run_lease.check()
        batch = self._batch(record_id)
        if batch:
            self.repository.queue_batch_projection(batch.batch_id, fields)
        try:
            self.client.update_record_fields(record_id, fields)
        except Exception as exc:
            if batch:
                raise ProjectionPendingError(f"RDS 状态已保留，飞书投影等待重放：{exc}") from exc
            raise
        if batch:
            self.repository.acknowledge_batch_projection(batch.batch_id, fields)

    def _assert_batch_complete(self, record_id, tasks):
        batch = self._batch(record_id)
        if batch and (len(tasks) != batch.expected_count or len({t.source_record_id for t in tasks}) != batch.expected_count):
            raise FeishuWorkflowError(f"批次只建成 {len(tasks)}/{batch.expected_count} 条，勾选执行补齐冻结清单后再审核或发布")

    @staticmethod
    def _action(fields: Dict[str, Any]) -> str:
        review = text_value(fields.get(FIELD_REVIEW))
        progress = text_value(fields.get(FIELD_PROGRESS))
        if bool(fields.get(FIELD_CONFIRM_PUBLISH)) and progress == PROGRESS_DONE:
            return "schedule"
        if review == REVIEW_SCHEDULE and progress not in {
            PROGRESS_QUEUED, PROGRESS_SCHEDULED, PROGRESS_PUBLISHING, PROGRESS_PUBLISHED,
        }:
            return "schedule"
        if review == REVIEW_APPROVED and progress != PROGRESS_DONE:
            return "approve"
        if review == REVIEW_REDO_ALL:
            return "redo_all"
        if review == "重做成片":
            return "redo_render"
        if review.startswith("重做P") and review[3:].isdigit():
            return f"redo_{int(review[3:])}"
        if bool(fields.get(FIELD_RETRY_REVIEW)):
            return "retry_review"
        if bool(fields.get(FIELD_EXECUTE)) and progress not in {
            PROGRESS_DONE, PROGRESS_QUEUED, PROGRESS_SCHEDULED, PROGRESS_PUBLISHING,
            PROGRESS_PUBLISHED,
        }:
            return "generate"
        return ""

    def _generate(self, record) -> Dict[str, Any]:
        batch = self._batch(record.record_id)
        existing = self._tasks(record.record_id)
        if existing and batch is None:
            if len(existing) != quantity_value(record.fields.get(FIELD_QUANTITY)):
                raise FeishuWorkflowError("历史批次缺少冻结清单且任务数量不足；请人工核对，不能按当前配置猜测补单")
            if all(workflow_v2_enabled(task) for task in existing):
                return self._advance_v2(record, existing)
            if any(workflow_v2_enabled(task) for task in existing):
                raise FeishuWorkflowError("历史混合 V1/V2 批次需要人工拆分，不能遗漏子任务")
        product_id = batch.manifest_json["product_id"] if batch else text_value(record.fields.get(FIELD_PRODUCT))
        preset_name = batch.manifest_json["preset"] if batch else text_value(record.fields.get(FIELD_PRESET))
        if not product_id:
            raise FeishuWorkflowError("请填写产品编码")
        if not preset_name:
            raise FeishuWorkflowError("请选择生产预设")
        quantity = batch.expected_count if batch else quantity_value(record.fields.get(FIELD_QUANTITY))
        specs = ([PresetTask(**entry["spec"]) for entry in batch.manifest_json["entries"]]
                 if batch else self.catalog.resolve_batch(preset_name, record.record_id, quantity))
        allowed_source_ids = {
            f"{record.record_id}:{index}:{spec.recipe_id}:1"
            for index, spec in enumerate(specs, start=1)
        }
        unexpected = [
            task.source_record_id
            for task in self._tasks(record.record_id)
            if task.source_record_id not in allowed_source_ids
        ]
        if unexpected:
            raise FeishuWorkflowError(
                "该记录已经按其他生产预设建过任务；为避免混组，请新建一行再选择新预设"
            )
        self._write_fields(record.record_id, {
            FIELD_EXECUTE: False,
            FIELD_PROGRESS: PROGRESS_RUNNING,
            FIELD_REVIEW: REVIEW_NOT_REQUIRED,
            FIELD_NOTES: "",
            **({FIELD_REVIEW_MODE: None} if not existing or all(workflow_v2_enabled(task) for task in existing)
               or (batch and int(batch.manifest_json.get("workflow_version") or 1) >= 2) else {}),
        })
        task_ids: List[str] = []
        products: List[Dict[str, Any]] = []
        asset_reader = LightTryonAssetReader()
        if batch is None:
            for index, spec in enumerate(specs, start=1):
                try:
                    products.append(self.product_reference_resolver.resolve_snapshot(
                        product_id, selection_key=f"{record.record_id}:{index}", account_id=spec.account_id,
                    ))
                except ProductReferenceResolutionError as exc:
                    raise FeishuWorkflowError(str(exc)) from exc
            assignments = BatchDiversityPlanner(self.repository, asset_reader).plan(
                record_id=record.record_id, specs=specs, products=products,
            )
            if not existing:
                from services.workflow_v2 import canonical_hash
                batch = self.repository.create_production_batch_idempotent(ProductionBatch(
                    batch_id="opv_batch_" + canonical_hash(record.record_id)[:32],
                    source_record_id=record.record_id, expected_count=quantity,
                    manifest_json={"workflow_version": 2, "product_id": product_id, "preset": preset_name,
                                   "entries": [{"spec": asdict(a.spec), "product": a.product_snapshot} for a in assignments]},
                ))
            else:
                # Historical V1 rows retain their old behavior and do not
                # receive a retroactive V2 plan or batch interpretation.
                entries = [{"spec": asdict(a.spec), "product": a.product_snapshot} for a in assignments]
        if batch:
            self._claim_batch(batch)
            product_id = batch.manifest_json["product_id"]
            entries = batch.manifest_json["entries"]
            if len(entries) != batch.expected_count:
                raise FeishuWorkflowError("冻结批次清单数量损坏，禁止创建或放行")
        products = [entry["product"] for entry in entries]
        producer = HeroFirstProducer(self.repository, self.generator, output_root=self.output_root,
                                     asset_readiness_gate=self.asset_readiness_gate)
        for index, entry in enumerate(entries, start=1):
            if self._run_lease:
                self._run_lease.check()
            spec, product = PresetTask(**entry["spec"]), entry["product"]
            prefix = f"{record.record_id}:{index}:{spec.recipe_id}"
            found = [t for t in self._tasks(record.record_id) if t.source_record_id == prefix + ":1" and t.plan_json and t.task_status != "draft"]
            rows = [{"task_id": t.task_id} for t in found] or generate_product_image_story(
                self.repository,
                product_id=product_id,
                market=spec.market,
                language=spec.language,
                recipe_id=spec.recipe_id,
                account_id=spec.account_id,
                theme_id=spec.theme_id,
                hook_strategy=spec.hook_strategy,
                product_snapshot=dict(product),
                operator="feishu_opv_workbench",
                asset_reader=asset_reader,
                source_type=SOURCE_TYPE,
                source_record_id_prefix=prefix,
                feishu_record_id=record.record_id,
                persona_ref=spec.persona_ref,
                look_ref=spec.look_ref,
                scene_ref=spec.scene_ref,
                variant_index_offset=index - 1,
            )
            for row in rows:
                task_id = row["task_id"]
                task = self.repository.get_task(task_id)
                if batch and not workflow_v2_enabled(task):
                    if task.task_status != "planned" or task.active_revision_id:
                        raise FeishuWorkflowError("冻结 V2 批次中发现已运行的 V1 子任务，需人工核对")
                    self.repository.update_task_plan(task_id, plan_json={**task.plan_json, "workflow_version": 2})
                    from services.workflow_v2 import RevisionService
                    RevisionService(self.repository).ensure_working(self.repository.get_task(task_id))
                    task = self.repository.get_task(task_id)
                task_ids.append(task_id)
        # Show actual frozen counts, not the preset's promised maximum, before
        # any costly image call. This is informational, never a new approval gate.
        self._write_fields(record.record_id, {FIELD_NOTES: self._production_plan_note(
            [self.repository.get_task(task_id) for task_id in task_ids])})
        # Materialize the whole frozen batch before starting costly media.
        for task_id in task_ids:
            if self._run_lease:
                self._run_lease.check()
            task = self.repository.get_task(task_id)
            if workflow_v2_enabled(task):
                continue
            if task and task.task_status in {"planned", "hero_generating", "image_generating", "failed"}:
                producer.produce(task_id)
            self._recover_incomplete_review(task_id, producer)
        # Workflow V2 intentionally stops after the anchor.  It is a normal
        # review state, not a failed auto-render and must never be handed to
        # the group-render loop below.
        v2_tasks = [self.repository.get_task(tid) for tid in task_ids if workflow_v2_enabled(self.repository.get_task(tid))]
        if v2_tasks:
            if len(v2_tasks) != len(task_ids):
                raise FeishuWorkflowError("同一记录不能混合 V1 自动成片和 V2 分阶段审核任务")
            return self._advance_v2(record, v2_tasks, automatic=self._review_mode(record) == MODE_AUTO)
        reference_note = self._reference_notes(products)
        rendered = self._render_tasks(
            task_ids,
            approval_mode="operator_auto",
            notes=f"飞书自动生产；{reference_note}",
        )
        attachments = self._upload_files(rendered, parent_type="bitable_file")
        self._write_fields(record.record_id, {
            FIELD_PROGRESS: PROGRESS_DONE,
            FIELD_OUTPUT: attachments,
            FIELD_REVIEW: REVIEW_NOT_REQUIRED,
            FIELD_NOTES: (
                f"已自动生成 {len(rendered)} 条成片；{reference_note}。"
                "仅执行必要的媒体技术检查，未进行人工审美审核；未发布。"
            ),
        })
        return {
            "record_id": record.record_id,
            "action": "generate_and_render",
            "task_ids": task_ids,
            "videos": rendered,
        }

    @staticmethod
    def _production_plan_note(tasks) -> str:
        entries = []
        for index, task in enumerate(tasks, 1):
            plan = task.plan_json or {}
            shots = plan.get("shots") or []
            sequence = plan.get("outfit_sequence") or []
            # BASE/FINAL may be different wearing states of the same outfit.
            fingerprints = {state.get("outfit_fingerprint") for state in (plan.get("outfit_states") or {}).values()
                            if state.get("outfit_fingerprint")}
            looks = len(sequence) or len(fingerprints) or None
            seconds = sum(int(s.get("duration_ms") or 0) for s in shots) / 1000
            count = f"{looks}套穿搭" if looks is not None else "穿搭套数未标注"
            entries.append(f"第{index}条：{count}/{len(shots)}页/{seconds:g}秒")
        return "生成前冻结计划：" + "；".join(entries) + "。按实际兼容模板编排；数量不足会减少页数，不用同套换角度冒充多套。"

    @staticmethod
    def _reference_note(product: Dict[str, Any]) -> str:
        variant = str(product.get("variant_key") or "default")
        version = int(product.get("reference_pack_version") or 1)
        count = len(product.get("reference_images") or [])
        status = str(product.get("reference_status") or "limited")
        suffix = "，缺少部分角度时细节镜头使用保守模式" if status == "limited" else ""
        return f"商品参考包 {variant}/V{version}，{count} 张图，状态 {status}{suffix}"

    @classmethod
    def _reference_notes(cls, products: List[Dict[str, Any]]) -> str:
        unique: List[Dict[str, Any]] = []
        seen = set()
        for product in products:
            key = (product.get("reference_pack_id"), product.get("reference_pack_version"))
            if key not in seen:
                seen.add(key)
                unique.append(product)
        return "；".join(cls._reference_note(product) for product in unique)

    def _approve_and_render(
        self, record, *, approval_mode: str = "human"
    ) -> Dict[str, Any]:
        tasks = self._tasks(record.record_id)
        if not tasks:
            raise FeishuWorkflowError("找不到该飞书记录对应的 RDS 任务")
        self._assert_batch_complete(record.record_id, tasks)
        if any(workflow_v2_enabled(task) for task in tasks):
            if not all(workflow_v2_enabled(task) for task in tasks):
                raise FeishuWorkflowError("混合工作流版本需分开处理")
            if approval_mode != "human":
                return self._project_v2(record, tasks)
            return self._advance_v2(record, tasks, approve=True)
        self._write_fields(record.record_id, {FIELD_PROGRESS: PROGRESS_RUNNING})
        note = text_value(record.fields.get(FIELD_NOTES))
        render_paths = self._render_tasks(
            [task.task_id for task in tasks],
            approval_mode=approval_mode,
            notes=(
                note or "飞书人工审核通过"
                if approval_mode == "human"
                else "历史待审核任务按新版流程自动续跑"
            ),
        )
        attachments = self._upload_files(render_paths, parent_type="bitable_file")
        auto = approval_mode == "operator_auto"
        self._write_fields(record.record_id, {
            FIELD_PROGRESS: PROGRESS_DONE,
            FIELD_OUTPUT: attachments,
            FIELD_REVIEW: REVIEW_NOT_REQUIRED if auto else REVIEW_APPROVED,
            FIELD_NOTES: (
                f"已完成 {len(render_paths)} 条自动成片；仅执行媒体技术检查；未发布。"
                if auto else f"已完成 {len(render_paths)} 条人工确认成片；未发布。"
            ),
        })
        return {
            "record_id": record.record_id,
            "action": "auto_render" if auto else "approve",
            "videos": render_paths,
        }

    def _render_tasks(
        self,
        task_ids: Iterable[str],
        *,
        approval_mode: str,
        notes: str,
    ) -> List[str]:
        flow = VideoRenderFlow(self.repository, self.renderer)
        render_paths: List[str] = []
        for task_id in task_ids:
            current = self.repository.get_task(task_id)
            if current is None:
                raise FeishuWorkflowError(f"找不到 RDS 任务：{task_id}")
            if current.task_status == "image_review":
                current = flow.approve_group(
                    task_id,
                    reviewer=(
                        "feishu_operator_auto_render"
                        if approval_mode == "operator_auto"
                        else "feishu_human_review"
                    ),
                    notes=notes,
                    approval_mode=approval_mode,
                )
            if current.task_status == "rendering":
                rendered = flow.render(
                    task_id,
                    overlay_profile_id=self.catalog.overlay_profile_id or None,
                )
            elif current.task_status == "video_review":
                rendered = self.repository.latest_render(task_id)
            else:
                raise FeishuWorkflowError(
                    f"任务 {task_id} 当前状态 {current.task_status}，不能生成成片"
                )
            if not rendered or rendered.qc_status != "passed" or not rendered.output_url:
                raise FeishuWorkflowError(f"任务 {task_id} 视频媒体质检未通过")
            render_paths.append(rendered.output_url)
        return render_paths

    def _schedule_publish(self, record) -> Dict[str, Any]:
        if self.publish_scheduler is None:
            raise FeishuWorkflowError("自动排班服务未接入")
        tasks = self._tasks(record.record_id)
        if not tasks:
            raise FeishuWorkflowError("找不到该飞书记录对应的 RDS 任务")
        self._assert_batch_complete(record.record_id, tasks)
        slots = []
        confirmed_by_checkbox = bool(record.fields.get(FIELD_CONFIRM_PUBLISH))
        # Preflight every task before enqueuing any member of this row.
        for task in tasks:
            if workflow_v2_enabled(task) and not self._v2_released(task):
                raise FeishuWorkflowError("当前 V2 版本尚未通过成片终审，不能确认发布")
        enqueue_main = callable(getattr(self.publish_scheduler, "enqueue_task", None))
        for task in tasks:
            if enqueue_main:
                slots.append(
                    self.publish_scheduler.enqueue_task(
                        task.task_id, feishu_record_id=record.record_id
                    )
                )
            else:
                slot = self.publish_scheduler.queue_task(
                    task.task_id,
                    operator=(
                        "feishu_confirm_publish_v2"
                        if confirmed_by_checkbox else "feishu_legacy_review_schedule"
                    ),
                )
                slots.append({
                    "task_id": task.task_id,
                    "local_time": slot.account_local_time.isoformat(timespec="minutes"),
                    "timezone": slot.account_timezone,
                })
        if enqueue_main:
            progress = PROGRESS_QUEUED
            note = (
                f"已进入短视频主排班池，共 {len(slots)} 条。"
                "按店铺养号配额自动分配账号和时间，非带货/不挂车；"
                "在未来 48 小时排期窗口内提前选择当地 NeoBund BGM 并创建定时任务。"
            )
            action = "enqueue_main_schedule"
        else:
            progress = PROGRESS_SCHEDULED
            summary = "；".join(
                f"{item['local_time']} ({item['timezone']})" for item in slots
            )
            note = f"已进入自动发布队列：{summary}。发布前 120 分钟内选择国家热点 BGM。"
            action = "schedule"
        self._write_fields(record.record_id, {
            FIELD_PROGRESS: progress,
            FIELD_CONFIRM_PUBLISH: False,
            FIELD_NOTES: note,
        })
        return {"record_id": record.record_id, "action": action, "slots": slots}

    def sync_publication_status(
        self, record_id: str, *, current_fields: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Project RDS publish truth back to the compact Feishu row."""
        tasks = self._tasks(record_id)
        if not tasks:
            return {"record_id": record_id, "updated": False}
        state_reader = getattr(self.publish_scheduler, "get_task_state", None)
        if callable(state_reader):
            states = [state_reader(task.task_id) for task in tasks]
            progress, note = self._publication_projection(states)
            current = current_fields or self.client.get_record(record_id).fields or {}
            if text_value(current.get(FIELD_PROGRESS)) == progress and text_value(current.get(FIELD_NOTES)) == note:
                return {"record_id": record_id, "updated": False, "progress": progress}
            self._write_fields(record_id, {
                FIELD_PROGRESS: progress,
                FIELD_NOTES: note,
            })
            return {"record_id": record_id, "updated": True, "progress": progress}
        states = [task.task_status for task in tasks]
        records = []
        for task in tasks:
            render = self.repository.get_render(task.selected_render_id or "")
            record = (
                self.repository.get_publish_record_by_render(render.render_id)
                if render else None
            )
            if record:
                records.append(record)
        if all(state == "published" for state in states):
            progress = PROGRESS_PUBLISHED
        elif any(state == "failed" for state in states):
            progress = PROGRESS_PUBLISH_FAILED
        elif any(state == "publishing" for state in states):
            progress = PROGRESS_SCHEDULED
        elif records:
            progress = PROGRESS_SCHEDULED
        else:
            progress = PROGRESS_QUEUED
        details = []
        for publish in records:
            audio = (publish.platform_metadata_json or {}).get("audio") or {}
            selected = audio.get("selected") or {}
            local_time = ((publish.platform_metadata_json or {}).get("schedule") or {}).get(
                "account_local_time"
            )
            item = str(local_time or publish.planned_publish_at or "")
            if selected.get("title"):
                item += f"｜BGM: {selected['title']}"
            if publish.external_post_url:
                item += f"｜{publish.external_post_url}"
            details.append(item)
        self._write_fields(record_id, {
            FIELD_PROGRESS: progress,
            FIELD_NOTES: "；".join(details)[:1800],
        })
        return {"record_id": record_id, "updated": True, "progress": progress}

    @staticmethod
    def _publication_projection(states: List[Dict[str, Any]]) -> tuple[str, str]:
        """Return a truthful batch projection, including partial failures."""
        statuses = [str(item.get("status") or "") for item in states]
        errors = [str(item.get("error_message") or "").strip() for item in states]
        stopped = [
            index for index, error in enumerate(errors)
            if "已停止自动重试" in error or "超过自动重试上限" in error
        ]
        retrying = [index for index, error in enumerate(errors) if "等待自动重试" in error]
        failed = [index for index, status in enumerate(statuses) if status == "发布失败"]
        needs_action = sorted(set(stopped + failed))

        if statuses and all(status == "已发布" for status in statuses):
            progress = PROGRESS_PUBLISHED
        elif needs_action:
            progress = PROGRESS_PUBLISH_FAILED
        elif any(status == "发布中" for status in statuses):
            progress = PROGRESS_PUBLISHING
        elif any(status == "已排期" for status in statuses):
            progress = PROGRESS_SCHEDULED
        else:
            progress = PROGRESS_QUEUED

        counts = {
            "已发布": statuses.count("已发布"),
            "已排期": statuses.count("已排期"),
            "发布中": statuses.count("发布中"),
            "待排班": sum(status in {"", "待排班"} for status in statuses),
            "重试中": len(retrying),
            "需处理": len(needs_action),
        }
        summary = "，".join(
            f"{label}{count}" for label, count in counts.items() if count
        ) or "待排班0"
        details = []
        for index, item in enumerate(states):
            values = [
                str(item.get("task_id") or "").strip() if index in needs_action or index in retrying else "",
                str(item.get("account_name") or "").strip(),
                str(item.get("planned_publish_at") or "").strip(),
                f"BGM: {item.get('bgm_title')}" if item.get("bgm_title") else "",
            ]
            if index in needs_action or index in retrying:
                values.append(errors[index][:180])
            detail = "｜".join(value for value in values if value)
            if detail:
                details.append(detail)
        note = f"批次状态：共{len(states)}，{summary}。"
        if details:
            note += "；".join(details)
        return progress, note[:1800]

    def _redo(self, record, action: str) -> Dict[str, Any]:
        tasks = self._tasks(record.record_id)
        if not tasks:
            raise FeishuWorkflowError("找不到该飞书记录对应的 RDS 任务")
        if any(workflow_v2_enabled(task) for task in tasks):
            if not all(workflow_v2_enabled(task) for task in tasks):
                raise FeishuWorkflowError("混合工作流版本需分开返工")
            return self._redo_v2(record, tasks, action)
        if action == "redo_render":
            raise FeishuWorkflowError("重做成片仅适用于 V2 分阶段流程")
        slots = list(range(1, 6)) if action == "redo_all" else [int(action.split("_")[1])]
        self._write_fields(record.record_id, {FIELD_PROGRESS: PROGRESS_RUNNING})
        producer = HeroFirstProducer(self.repository, self.generator)
        effective_slots = set(slots)
        for task in tasks:
            current = self.repository.get_task(task.task_id) or task
            if current.task_status != "image_review":
                raise FeishuWorkflowError(
                    f"任务 {task.task_id} 已不在图片审核阶段，不能重做图片"
                )
            self.repository.transition_task(task.task_id, "image_review", "image_generating")
            try:
                anchor = int((current.plan_json or {}).get("anchor_slot") or 1)
                # P1 is derived from P2 and P3-P5 share P2 continuity. A new
                # anchor invalidates the complete visible group.
                task_slots = dependent_redo_slots(current.plan_json or {}, slots)
                effective_slots.update(task_slots)
                ordered = sorted(task_slots, key=lambda value: value != anchor)
                for slot in ordered:
                    result = producer.regenerate_slot(task.task_id, slot)
                    if result.status != "generated":
                        raise FeishuWorkflowError(
                            f"任务 {task.task_id} 的 P{slot} 重做失败：{result.error}"
                        )
            finally:
                refreshed = self.repository.get_task(task.task_id)
                if refreshed and refreshed.task_status == "image_generating":
                    self.repository.transition_task(
                        task.task_id, "image_generating", "image_review"
                    )
        attachments = self._upload_previews([task.task_id for task in tasks])
        self._write_fields(record.record_id, {
            FIELD_PROGRESS: PROGRESS_REVIEW,
            FIELD_OUTPUT: attachments,
            FIELD_REVIEW: REVIEW_PENDING,
            FIELD_NOTES: f"已重做 {'P' + str(slots[0]) if len(effective_slots) == 1 else '整组'}，请重新审核。",
        })
        return {
            "record_id": record.record_id,
            "action": action,
            "slots": sorted(effective_slots),
        }

    def _tasks(self, record_id: str):
        return self.repository.list_tasks_by_source_prefix(SOURCE_TYPE, f"{record_id}:")

    def _recover_incomplete_review(self, task_id: str, producer) -> None:
        """Retry only incomplete slots once while keeping the row resumable."""
        task = self.repository.get_task(task_id)
        if not task or task.task_status != "image_review":
            return
        shots = HeroFirstProducer._latest_per_slot(self.repository.list_shots(task_id))
        incomplete = [
            shot.slot_index
            for shot in shots
            if shot.shot_status != "generated" or shot.qa_status != "passed" or not shot.image_url
        ]
        if not incomplete:
            return
        self.repository.transition_task(task_id, "image_review", "image_generating")
        try:
            anchor = int((task.plan_json or {}).get("anchor_slot") or 1)
            for slot in sorted(incomplete, key=lambda value: value != anchor):
                result = producer.regenerate_slot(task_id, slot)
                if result.status != "generated":
                    raise FeishuWorkflowError(
                        f"任务 {task_id} 的 P{slot} 自动补跑失败：{result.error}"
                    )
        finally:
            refreshed = self.repository.get_task(task_id)
            if refreshed and refreshed.task_status == "image_generating":
                self.repository.transition_task(task_id, "image_generating", "image_review")

    def _upload_previews(self, task_ids: Iterable[str]) -> List[Dict[str, str]]:
        paths: List[str] = []
        for task_id in task_ids:
            shots = HeroFirstProducer._latest_per_slot(self.repository.list_shots(task_id))
            if len(shots) != 5 or any(not shot.image_url for shot in shots):
                raise FeishuWorkflowError(f"任务 {task_id} 没有完整的 5 张预览图")
            paths.extend(str(shot.image_url) for shot in shots)
        return self._upload_files(paths, parent_type="bitable_image")

    def _upload_files(self, paths: Iterable[str], *, parent_type: str) -> List[Dict[str, str]]:
        uploaded: List[Dict[str, str]] = []
        for raw in paths:
            path = Path(raw)
            if not path.is_file():
                raise FeishuWorkflowError(f"输出文件不存在：{path}")
            content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
            item = self.client.upload_attachment(
                path.read_bytes(), path.name, content_type, path.stat().st_size,
                parent_type=parent_type,
            )
            uploaded.append({"file_token": item["file_token"]})
        return uploaded
