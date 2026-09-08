"""Compact Feishu workbench adapter for OPV image-story production.

Feishu is the operator surface; RDS remains the source of truth.  The adapter
uses the Feishu record id as the external idempotency namespace and never
publishes content.
"""

from __future__ import annotations

import hashlib
import json
import mimetypes
import os
from dataclasses import asdict, dataclass, replace
from domain.models import ProductionBatch
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from services.asset_resolver import LightTryonAssetReader
from services.batch_diversity_planner import BatchDiversityPlanner
from services.content_story import generate_product_image_story
from services.hero_first import HeroFirstProducer
from services.image_generator import build_default_photo_generator
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
FIELD_QUANTITY = "生成篇数"
FIELD_QUANTITY_LEGACY = "生成数量"
FIELD_CONFIRM_PUBLISH = "确认发布"
FIELD_PHOTO_REQUEST = "图文任务JSON"  # optional legacy override; never required
FIELD_PHOTO_SUMMARY = "内容方案摘要"
FIELD_PHOTO_INPUT = "完整穿搭素材（可选）"
FIELD_PHOTO_INPUT_LEGACY = "图文参考图"
FIELD_PRODUCT_REFERENCE = "商品参考图（可选）"
FIELD_REFERENCE = "参考图（可选）"
FIELD_REFERENCE_TYPE = "参考图类型"
FIELD_CONTENT_THEME = "图文主题"
FIELD_CONTENT_REQUIREMENT = "内容要求（可选）"
FIELD_TRAVEL_PLACE = "旅行地点（可选）"
FIELD_MUSIC_MODE = "配乐方式"
FIELD_PHOTO_ASSET_STATUS = "素材状态"

PROGRESS_PENDING = "待执行"
PROGRESS_RUNNING = "生成中"
PROGRESS_PLANNING = "规划中"
PROGRESS_PREPARING_ASSETS = "准备素材"
PROGRESS_QA = "质检中"
PROGRESS_REPAIR = "待修复"
PROGRESS_RETRYABLE = "失败可重试"
PROGRESS_REVIEW = "待审核"
PROGRESS_DONE = "已完成"
PROGRESS_ACTION = "需处理"
PROGRESS_QUEUED = "待排班"
PROGRESS_SCHEDULED = "已排期"
PROGRESS_PUBLISHING = "发布中"
PROGRESS_PUBLISHED = "已发布"
PROGRESS_PUBLISH_FAILED = "发布失败"

IN_FLIGHT_PROGRESS = {
    PROGRESS_RUNNING, PROGRESS_PLANNING, PROGRESS_PREPARING_ASSETS,
    PROGRESS_QA, PROGRESS_REPAIR,
}

REVIEW_PENDING = "待审核"
REVIEW_APPROVED = "通过"
REVIEW_NOT_REQUIRED = "无需审核"
REVIEW_REDO_ALL = "整组重做"
REVIEW_SCHEDULE = "排期发布"


class FeishuWorkflowError(RuntimeError):
    pass


class _RecordFlock:
    """Non-blocking per-record advisory lock (kernel-owned, death-safe).

    Scanner slots run in parallel; this guarantees two workers never process
    the same row. ``with _RecordFlock(rid) as locked:`` — locked is False when
    another worker owns the record.
    """

    def __init__(self, record_id: str, directory: str = "/tmp"):
        import fcntl

        self._fcntl = fcntl
        self.path = f"{directory}/opv_record_{record_id}.flock"
        self.fd: Optional[int] = None

    def __enter__(self) -> bool:
        self.fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            self._fcntl.flock(self.fd, self._fcntl.LOCK_EX | self._fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(self.fd)
            self.fd = None
        return self.fd is not None

    def __exit__(self, *args) -> bool:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        return False


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


def task_quantity(fields: Mapping[str, Any]) -> int:
    """Prefer the operator-friendly name while keeping historical rows readable."""
    value = fields.get(FIELD_QUANTITY)
    if value in (None, ""):
        value = fields.get(FIELD_QUANTITY_LEGACY)
    return quantity_value(value)


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
        return [name for name, raw in self._raw.items() if raw.get("status", "active") == "active"]

    def _require_enabled(self, name: str) -> None:
        raw = self.metadata(name)
        if raw.get("status", "active") != "active":
            raise FeishuWorkflowError("NEEDS_CONTENT: 该生产预设已停用，缺少通过内容验收的方案：" + name)

    def metadata(self, name: str) -> Dict[str, Any]:
        raw = self._raw.get(name)
        if raw is None:
            raise FeishuWorkflowError(f"未知生产预设：{name}")
        return dict(raw)

    def is_native_photo(self, name: str) -> bool:
        return self.metadata(name).get("media_kind") == "native_photo"

    def resolve(self, name: str, record_id: str) -> List[PresetTask]:
        self._require_enabled(name)
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
        self._require_enabled(name)
        if quantity < 1 or quantity > 9:
            raise FeishuWorkflowError("生成篇数必须是 1 到 9")
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
        raise FeishuWorkflowError("生成篇数必须是 1 到 9 的整数") from exc
    if result < 1 or result > 9 or float(value) != result:
        raise FeishuWorkflowError("生成篇数必须是 1 到 9 的整数")
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
        photo_reference_vision=None,
    ):
        self.repository = repository
        self.client = client
        self.catalog = catalog or ProductionPresetCatalog()
        self.generator = generator or build_default_photo_generator()
        self.renderer = renderer or FFmpegStillRenderer()
        self.publish_scheduler = publish_scheduler
        self.visual_qa_adapter = visual_qa_adapter
        self.output_root = output_root
        self.asset_readiness_gate = asset_readiness_gate
        self.photo_reference_vision = photo_reference_vision
        self._run_lease = None
        self._photo_quality_summaries: Dict[str, str] = {}
        self.product_reference_resolver = (
            product_reference_resolver or ProductReferenceResolver(repository)
        )

    @staticmethod
    def _complete_look_attachments(fields: Dict[str, Any]) -> List[Dict[str, Any]]:
        return list(fields.get(FIELD_PHOTO_INPUT) or fields.get(FIELD_PHOTO_INPUT_LEGACY) or [])

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
            # Complete a committed photo projection before interpreting any
            # retained command. This includes terminal-review writeback failure.
            pending_batch = self._batch(record.record_id) if not dry_run else None
            if (pending_batch and pending_batch.manifest_json.get("media_kind") == "native_photo"
                    and pending_batch.pending_fields_json):
                try:
                    self._claim_batch(pending_batch)
                    pending = pending_batch.pending_fields_json
                    self.client.update_record_fields(record.record_id, pending)
                    self.repository.acknowledge_batch_projection(pending_batch.batch_id, pending)
                    report["processed"].append({"record_id": record.record_id, "action": "recover_projection"})
                except BatchLeaseBusy as exc:
                    report["processed"].append({"record_id": record.record_id, "action": "leased_skip", "reason": str(exc)})
                except Exception as exc:
                    report["errors"].append({"record_id": record.record_id, "error": f"恢复工作台失败：{exc}"})
                finally:
                    if self._run_lease:
                        self._run_lease.close()
                        self._run_lease = None
                continue
            action = self._action(fields)
            v2_tasks = (
                self._v2_tasks(record.record_id)
                if action in {"approve", "retry_review"} else []
            )
            all_photo = bool(v2_tasks) and all(
                str(getattr(task, "media_kind", "video") or "video") == "native_photo"
                for task in v2_tasks
            )
            if (action == "approve" and not v2_tasks
                    and text_value(fields.get(FIELD_PHOTO_ASSET_STATUS)) == "待内容审核"):
                action = "approve_photo_assets"
            if action in {"approve", "retry_review"} and v2_tasks and not (
                action == "approve" and all_photo
            ):
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
                                    task_quantity(fields))
                    except Exception as exc:
                        result["preview_error"] = str(exc)
                report["processed"].append(result)
                continue
            try:
                with _RecordFlock(record.record_id) as record_locked:
                    if not record_locked:
                        # Another scanner slot owns this row; it will project
                        # the final fields when it finishes.
                        report["processed"].append({
                            "record_id": record.record_id,
                            "action": "record_locked_skip",
                        })
                        continue
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
                    elif action == "approve_photo_assets":
                        result = self._approve_staged_photo_assets(record)
                    elif action == "retry_review":
                        tasks = self._v2_tasks(record.record_id)
                        if not tasks:
                            raise FeishuWorkflowError("重试审核仅适用于 V2 分阶段任务")
                        result = self._advance_v2(record, tasks, automatic=self._review_mode(record) == MODE_AUTO, resume_generation=False)
                    elif action == "confirm_publish":
                        result = self._confirm_photo_publish(record)
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
                try:
                    prior_progress = text_value(
                        self.client.get_record(record.record_id).fields.get(FIELD_PROGRESS))
                except Exception:
                    prior_progress = ""
                if action == "generate" and (
                        prior_progress in IN_FLIGHT_PROGRESS
                        or prior_progress.startswith("素材生成 ")):
                    # Paid work already started; resume reuses completed assets
                    # instead of regenerating them, so mark it retryable.
                    failure_fields[FIELD_PROGRESS] = PROGRESS_RETRYABLE
                    failure_fields[FIELD_NOTES] = (
                        message + "；已生成素材已保留，重新勾选执行将从断点续跑，不重复生成已完成角色"
                    )
                if action in {"schedule", "confirm_publish"}:
                    failure_fields[FIELD_CONFIRM_PUBLISH] = False
                try:
                    current_batch = self._batch(record.record_id)
                    if self._v2_tasks(record.record_id) or (
                        current_batch and int(current_batch.manifest_json.get("workflow_version") or 1) >= 2
                    ):
                        is_photo = current_batch and current_batch.manifest_json.get("media_kind") == "native_photo"
                        failure_fields.update({FIELD_REVIEW: REVIEW_NOT_REQUIRED,
                                               FIELD_REVIEW_MODE: None})
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
        if batch and batch.manifest_json.get("media_kind") == "native_photo":
            entries = self._photo_batch_entries(batch)
            if {entry["source_record_id"] for entry in entries} != {task.source_record_id for task in tasks}:
                raise FeishuWorkflowError("图文任务与冻结批次清单不一致，禁止放行")

    @staticmethod
    def _action(fields: Dict[str, Any]) -> str:
        review = text_value(fields.get(FIELD_REVIEW))
        progress = text_value(fields.get(FIELD_PROGRESS))
        if bool(fields.get(FIELD_CONFIRM_PUBLISH)) and progress not in {
            PROGRESS_QUEUED, PROGRESS_SCHEDULED, PROGRESS_PUBLISHING, PROGRESS_PUBLISHED,
        }:
            return "confirm_publish"
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
        from services.release_gate import require_photo_content_allowed
        for task in existing:
            require_photo_content_allowed(self.repository, task)
        if (existing and batch is None
                and all(workflow_v2_enabled(task) for task in existing)
                and all(str(getattr(task, "media_kind", "video") or "video") != "native_photo"
                        for task in existing)):
            if len(existing) != task_quantity(record.fields):
                raise FeishuWorkflowError("历史批次缺少冻结清单且任务数量不足；请人工核对，不能按当前配置猜测补单")
            return self._advance_v2(record, existing)
        preset_name = batch.manifest_json["preset"] if batch else text_value(record.fields.get(FIELD_PRESET))
        if not preset_name:
            raise FeishuWorkflowError("请选择生产预设")
        quantity = batch.expected_count if batch else task_quantity(record.fields)
        if ((batch and batch.manifest_json.get("media_kind") == "native_photo")
                or (batch is None and self.catalog.is_native_photo(preset_name))):
            return self._generate_native_photo(record, preset_name, quantity, existing, batch=batch)
        if existing and batch is None:
            if len(existing) != task_quantity(record.fields):
                raise FeishuWorkflowError("历史批次缺少冻结清单且任务数量不足；请人工核对，不能按当前配置猜测补单")
            if all(workflow_v2_enabled(task) for task in existing):
                return self._advance_v2(record, existing)
            if any(workflow_v2_enabled(task) for task in existing):
                raise FeishuWorkflowError("历史混合 V1/V2 批次需要人工拆分，不能遗漏子任务")
        product_id = batch.manifest_json["product_id"] if batch else text_value(record.fields.get(FIELD_PRODUCT))
        if not product_id:
            raise FeishuWorkflowError("请填写产品编码")
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

    def _generate_native_photo(self, record, preset_name: str, quantity: int, existing, *, batch=None) -> Dict[str, Any]:
        """Freeze the entire row before task creation; retry only its frozen entries."""
        if batch and batch.batch_status == "cancelled":
            raise FeishuWorkflowError("该图文批次已取消；请新增一行重新发起，历史记录保持只读")
        from config.loader import load_board_layouts
        from services.photo_package import NativePhotoProductionFlow
        from services.photo_planner import PhotoReusePlannerService
        from services.photo_request_factory import PhotoRequestFactory, fingerprint, validate_frozen_request
        from services.task_intake import TaskIntakeService, TaskRequest

        style_product: dict[str, Any] = {}

        if batch is None:
            if existing:
                raise FeishuWorkflowError("历史图文缺少冻结批次；请先核对原任务，不能按当前预设自动补单")
            overrides = []
            raw = text_value(record.fields.get(FIELD_PHOTO_REQUEST))
            if raw:
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError as exc:
                    raise FeishuWorkflowError("图文任务JSON不是有效 JSON") from exc
                if not isinstance(payload, dict):
                    raise FeishuWorkflowError("图文任务JSON必须是对象")
                overrides = payload.get("items") if "items" in payload else [payload]
                if (not isinstance(overrides, list) or len(overrides) != quantity
                        or any(not isinstance(item, dict) for item in overrides)):
                    raise FeishuWorkflowError("图文任务JSON.items 数量必须与生成篇数一致")
            specs = self.catalog.resolve_batch(preset_name, record.record_id, quantity)
            if any(not spec.account_id for spec in specs):
                raise FeishuWorkflowError("该图文预设尚未绑定 OPV 生产账号")
            preset = self.catalog.metadata(preset_name)
            product_id = text_value(record.fields.get(FIELD_PRODUCT))
            unified_attachments = list(record.fields.get(FIELD_REFERENCE) or [])
            legacy_complete = self._complete_look_attachments(record.fields)
            legacy_product = list(record.fields.get(FIELD_PRODUCT_REFERENCE) or [])
            recipe_ids = {spec.recipe_id for spec in specs}
            recipe_for_input = (
                self.repository.get_content_recipe(specs[0].recipe_id)
                if len(recipe_ids) == 1 else None
            )
            roles = list((((recipe_for_input.recipe_spec_json or {}).get("asset_requirements") or {}).get("required_roles") or [])) if recipe_for_input else []
            from services.photo_theme import resolve_photo_theme, build_theme_copy
            from services.photo_reference import (
                REFERENCE_MODE_COMPLETE_LOOK, REFERENCE_MODE_PRODUCT, REFERENCE_MODE_STYLE,
                resolve_reference_mode,
            )
            try:
                theme = resolve_photo_theme(text_value(record.fields.get(FIELD_CONTENT_THEME)))
            except ValueError as exc:
                raise FeishuWorkflowError(str(exc)) from exc
            reference_mode = ""
            reference_attachments = []
            if unified_attachments:
                reference_attachments = unified_attachments
                try:
                    reference_mode = resolve_reference_mode(
                        selected_type=text_value(record.fields.get(FIELD_REFERENCE_TYPE)),
                        attachments=reference_attachments, product_id=product_id,
                        required_role_count=len(roles), requested_count=quantity,
                    )
                except ValueError as exc:
                    raise FeishuWorkflowError(str(exc)) from exc
            elif legacy_complete:
                reference_mode, reference_attachments = REFERENCE_MODE_COMPLETE_LOOK, legacy_complete
            elif legacy_product or product_id:
                reference_mode, reference_attachments = REFERENCE_MODE_PRODUCT, legacy_product
            # Explicit STYLE + product code is a supported combination: the
            # product pack remains the identity lock, while uploaded images are
            # inspiration only and must never be written into that pack.
            if reference_mode == REFERENCE_MODE_STYLE and product_id:
                try:
                    style_product = self.product_reference_resolver.resolve_snapshot(
                        product_id, selection_key=record.record_id,
                        account_id=specs[0].account_id,
                    )
                except ProductReferenceResolutionError as exc:
                    raise FeishuWorkflowError(
                        f"指定商品 {product_id} 缺少可用商品参考包：{exc}"
                    ) from exc
            product_context = ({
                key: style_product.get(key)
                for key in ("product_id", "product_name", "category", "variant_key",
                            "reference_pack_id", "reference_pack_version")
                if style_product.get(key) not in (None, "")
            } if style_product else {})
            requires_product_supply = bool(
                recipe_for_input and (recipe_for_input.recipe_spec_json or {}).get("outfit_supply")
            )
            asset_status = text_value(record.fields.get(FIELD_PHOTO_ASSET_STATUS))
            if (requires_product_supply and not reference_attachments and not product_id
                    and asset_status not in {"已确认，正在生成", "已匹配可用素材"}):
                raise FeishuWorkflowError(
                    "该图文预设需要填写产品编码或上传参考图"
                )
            from services.photo_batch_variation import plan_batch_variations
            from services.photo_content_planner import (
                PhotoContentPlanStore, get_planning_flow, plan_th_choice_batch,
                recipe_has_planning_policy,
            )
            variation_theme = theme or {
                "theme_key": "AUTO", "label_zh": "自动差异化穿搭",
                "visual_brief": "保持同一商品或参考风格，变化场景、配色和穿搭组合",
            }
            staging_root = (Path(self.output_root) if self.output_root else
                            Path.home() / ".openclaw/shared/data/organic_photo_video")
            style_reference_paths: list[str] = []
            style_profile: dict[str, Any] = {}
            content_requirement = text_value(record.fields.get(FIELD_CONTENT_REQUIREMENT))
            travel_place = text_value(record.fields.get(FIELD_TRAVEL_PLACE))
            travel_contract: dict[str, Any] = {}
            travel_variables: dict[str, Any] = {}
            travel_copy_templates = None
            travel_topic: dict[str, Any] = {}
            planning_flow = get_planning_flow(recipe_for_input.recipe_id) if recipe_for_input else ""
            if planning_flow == "travel_two_step":
                recipe_spec_input = recipe_for_input.recipe_spec_json or {}
                travel_contract = dict(recipe_spec_input.get("travel_contract") or {})
                profiles_input = list(recipe_spec_input.get("execution_profiles") or [])
                travel_variables = dict((profiles_input[0].get("variables") or {})
                                        if profiles_input else {})
                travel_copy_templates = list(
                    (profiles_input[0].get("copy_variants") or [])
                    if profiles_input else []
                ) or None
                if theme and theme.get("travel_theme_type"):
                    travel_topic = {
                        "theme_type": str(theme.get("travel_theme_type")),
                        "theme_version": int(theme.get("travel_theme_version") or 1),
                        "theme_label_zh": str(theme.get("label_zh") or ""),
                        "planning_focus": str(theme.get("visual_brief") or ""),
                        "topic_patterns": list(theme.get("topic_patterns") or []),
                        "body_copy_focus": str(theme.get("body_copy_focus") or ""),
                        "cta_patterns": list(theme.get("cta_patterns") or []),
                        "place": travel_place,
                        "temperature_band": str(travel_variables.get("temperature_band") or ""),
                        "temperature_context": {
                            "value": str(travel_variables.get("temperature_band") or ""),
                            "source": "execution_profile",
                        },
                        "thai_fallback": {
                            "title": str(theme.get("title") or ""),
                            "cover": str(theme.get("cover") or ""),
                            "caption": str(theme.get("caption") or ""),
                            "hashtags": list(theme.get("hashtags") or []),
                            "cta": str(theme.get("cta") or ""),
                        },
                        "content_requirement": content_requirement,
                    }
            if reference_mode == REFERENCE_MODE_STYLE and recipe_for_input:
                if theme is None:
                    raise FeishuWorkflowError("风格参考模式需要选择图文主题")
                from services.photo_asset_supply import PhotoAssetSupplyService
                from services.photo_reference_vision import PhotoReferenceVisionService
                style_reference_paths = PhotoAssetSupplyService(
                    self.client, root=staging_root,
                ).stage_reference_images(
                    record_id=record.record_id, attachments=reference_attachments,
                    reference_kind="style",
                )
                reference_vision = self.photo_reference_vision or PhotoReferenceVisionService(
                    root=staging_root
                )
                self.photo_reference_vision = reference_vision
                if planning_flow == "travel_two_step":
                    self._write_fields(record.record_id, {FIELD_PROGRESS: PROGRESS_PLANNING})
                    reference_analysis = reference_vision.analyze_reference(
                        record_id=record.record_id, paths=style_reference_paths,
                        theme=theme or variation_theme,
                        category_key=str((recipe_for_input.recipe_spec_json or {}).get("category_key") or ""),
                        content_requirement=content_requirement,
                    )
                    travel_plan = reference_vision.plan_travel_content(
                        record_id=record.record_id, analysis=reference_analysis,
                        travel_contract=travel_contract, variables=travel_variables,
                        content_requirement=content_requirement, count=quantity,
                        travel_topic=travel_topic or None,
                        product_context=product_context,
                    )
                    style_profile = reference_vision.build_travel_style_profile(
                        reference_analysis, travel_plan, count=quantity,
                    )
                    if travel_topic:
                        style_profile["travel_topic"] = dict(travel_topic)
                else:
                    style_profile = reference_vision.analyze(
                        record_id=record.record_id, paths=style_reference_paths,
                        theme=theme or variation_theme,
                        category_key=str((recipe_for_input.recipe_spec_json or {}).get("category_key") or ""),
                        content_requirement=content_requirement, count=quantity,
                        product_context=product_context,
                    )
                if product_context:
                    style_profile["product_context"] = dict(product_context)
            content_plan = None
            if (recipe_for_input
                    and recipe_has_planning_policy(recipe_for_input.recipe_id)
                    and theme is not None
                    and reference_mode in {
                        REFERENCE_MODE_COMPLETE_LOOK, REFERENCE_MODE_PRODUCT,
                        REFERENCE_MODE_STYLE,
                    }):
                input_contract = {
                    "recipe_id": recipe_for_input.recipe_id,
                    "recipe_version": recipe_for_input.recipe_version,
                    "theme_key": theme["theme_key"],
                    "reference_mode": reference_mode,
                    "quantity": quantity,
                    "product_id": product_id,
                    "reference_tokens": [
                        str(item.get("file_token") or item.get("name") or "")
                        for item in reference_attachments if isinstance(item, Mapping)
                    ],
                    "style_profile": style_profile,
                    "content_requirement": content_requirement,
                    "travel_topic": travel_topic or {},
                    "travel_place": travel_place,
                }
                content_plan = PhotoContentPlanStore(staging_root).load_or_create(
                    record_id=record.record_id, input_contract=input_contract,
                    create=lambda: plan_th_choice_batch(
                        record_id=record.record_id,
                        recipe_id=recipe_for_input.recipe_id,
                        theme=theme, reference_mode=reference_mode, count=quantity,
                        style_profile=style_profile,
                        travel_contract=travel_contract or None,
                        copy_templates=travel_copy_templates,
                    ),
                )
                variations = list(content_plan["items"])
            else:
                variations = plan_batch_variations(
                    record_id=record.record_id, theme=variation_theme, count=quantity,
                )
            if reference_mode == REFERENCE_MODE_COMPLETE_LOOK and content_plan is None:
                variations = [
                    {
                        "variation_id": f"complete_look_set_{index}", "index": index,
                        "theme_key": str(variation_theme.get("theme_key") or ""),
                        "angle_zh": f"完整穿搭素材第 {index} 组",
                        "scene_zh": "沿用上传素材", "palette_zh": "沿用上传素材",
                        "style_modifier": "不得改写上传的完整穿搭事实",
                    }
                    for index in range(1, quantity + 1)
                ]
            pinned_asset_set_ids: list[str] = []
            prepared_source_groups: list[list[dict[str, Any]]] = []
            if (reference_mode == REFERENCE_MODE_COMPLETE_LOOK and recipe_for_input
                    and asset_status not in {
                        "已确认，正在生成", "已匹配可用素材",
                    }):
                recipe = recipe_for_input
                from services.photo_asset_supply import PhotoAssetSupplyService
                asset_supply = PhotoAssetSupplyService(self.client, root=staging_root)
                for index in range(quantity):
                    item_id = f"{record.record_id}_item_{index + 1}"
                    begin, end = index * len(roles), (index + 1) * len(roles)
                    staged = asset_supply.stage(
                        record_id=item_id, attachments=reference_attachments[begin:end],
                        required_roles=roles,
                    )
                    prepared_source_groups.append(list(staged["files"]))
                    saved = asset_supply.qualify(
                        record_id=item_id, recipe=recipe, repository=self.repository,
                        reviewer="feishu_operator_execution", reviewer_type="technical",
                        source="feishu_complete_look_input",
                    )
                    pinned_asset_set_ids.append(saved.asset_set_id)
            if (reference_mode == REFERENCE_MODE_STYLE and recipe_for_input
                    and asset_status not in {"已确认，正在生成", "已匹配可用素材"}):
                if theme is None:
                    raise FeishuWorkflowError("风格参考模式需要选择图文主题")
                recipe = recipe_for_input
                from services.photo_asset_supply import PhotoAssetSupplyService
                from services.photo_style_reference_supply import PhotoStyleReferenceSupplyService
                asset_supply = PhotoAssetSupplyService(self.client, root=staging_root)
                paths = style_reference_paths
                account = self.repository.get_account_profile(specs[0].account_id)
                if account is None:
                    raise FeishuWorkflowError("图文生产账号不存在")
                if style_profile.get("presentation_type") == "FLAT_LAY":
                    persona = {}
                else:
                    if not getattr(account, "persona_ref_id", None):
                        raise FeishuWorkflowError("真人参考模式需要图文生产账号绑定人物模板")
                    persona = LightTryonAssetReader().get_persona(account.persona_ref_id)
                for index, variation in enumerate(variations, 1):
                    item_id = f"{record.record_id}_item_{index}"
                    self._write_fields(record.record_id, {
                        FIELD_PROGRESS: PROGRESS_PREPARING_ASSETS,
                    })
                    asset_counter = {"done": 0}
                    total_assets = len(variations) * 4

                    def _asset_progress(event: str, **data: Any) -> None:
                        if event == "asset_generated":
                            asset_counter["done"] += 1
                            self._write_fields(record.record_id, {
                                FIELD_PROGRESS: f"素材生成 {asset_counter['done']}/{total_assets}",
                            })
                        elif event == "qa_started":
                            self._write_fields(record.record_id, {FIELD_PROGRESS: PROGRESS_QA})
                        elif event == "human_qa_started":
                            self._write_fields(record.record_id, {
                                FIELD_PROGRESS: "人物表现质检中",
                                FIELD_REVIEW_STAGE: "人物表现质检",
                            })
                        elif event == "repair_scheduled":
                            note = str(data.get("notes") or "")
                            self._write_fields(record.record_id, {
                                FIELD_PROGRESS: PROGRESS_REPAIR,
                                **({
                                    FIELD_NOTES: (
                                        f"人物质检修复 {len(data.get('roles') or [])} 张：{note}"[:180]
                                        if data.get("reason") == "human_presentation"
                                        else f"风格质检修复 {len(data.get('roles') or [])} 张"[:180]
                                    )
                                } if note or data.get("reason") else {}),
                            })

                    prepared = PhotoStyleReferenceSupplyService(
                        generator=self.generator, root=staging_root,
                        vision_service=self.photo_reference_vision,
                    ).prepare(
                        record_id=item_id, reference_paths=paths, theme=theme,
                        account=account, persona=persona, variation=variation,
                        progress=_asset_progress, product=style_product,
                    )
                    from services.photo_content_check import validate_prepared_sources
                    validate_prepared_sources(variation, prepared["sources"])
                    quality_summary = str(
                        (prepared.get("quality") or {}).get("quality_summary_zh") or ""
                    )
                    if quality_summary:
                        # 非阻塞质量提示：并入最终完成摘要展示，不影响生成与发布。
                        self._photo_quality_summaries[record.record_id] = quality_summary
                    prepared_source_groups.append(list(prepared["sources"]))
                    asset_supply.stage_existing(
                        record_id=item_id, sources=prepared["sources"], required_roles=roles,
                        metadata={"theme_key": theme["theme_key"],
                                  "reference_mode": reference_mode,
                                  "product_id": str(style_product.get("product_id") or ""),
                                  "product_reference_pack_id": str(
                                      style_product.get("reference_pack_id") or ""
                                  ),
                                  "batch_variation": variation},
                    )
                    saved = asset_supply.qualify(
                        record_id=item_id, recipe=recipe, repository=self.repository,
                        reviewer="system_style_reference_generation", reviewer_type="technical",
                        source="feishu_style_reference_generated",
                    )
                    pinned_asset_set_ids.append(saved.asset_set_id)
            if (reference_mode == REFERENCE_MODE_PRODUCT and recipe_for_input
                    and asset_status not in {
                        "已确认，正在生成", "已匹配可用素材",
                    }):
                recipe = recipe_for_input
                if recipe is None or recipe.status != "active":
                    raise FeishuWorkflowError("图文 Recipe 不存在或已停用")
                from services.photo_asset_supply import PhotoAssetSupplyService
                from services.photo_outfit_supply import PhotoOutfitSupplyService
                asset_supply = PhotoAssetSupplyService(self.client, root=staging_root)
                if reference_attachments:
                    references = asset_supply.stage_product_references(
                        record_id=record.record_id, attachments=reference_attachments,
                    )
                    effective_product_id = product_id or f"FEISHU_PHOTO_{record.record_id}"
                    variant_key = f"photo_{record.record_id}"[:96]
                    self.product_reference_resolver.build_pack(
                        product_id=effective_product_id, product_name=effective_product_id,
                        category="outerwear", references=references, variant_key=variant_key,
                        is_default=not bool(product_id), source_type="feishu_photo_product_input",
                        source_ref=record.record_id, persist=True,
                    )
                    product = self.product_reference_resolver.resolve_snapshot(
                        effective_product_id, selection_key=record.record_id,
                        variant_key=variant_key, account_id=specs[0].account_id,
                    )
                    product_id = effective_product_id
                else:
                    product = self.product_reference_resolver.resolve_snapshot(
                        product_id, selection_key=record.record_id,
                        account_id=specs[0].account_id,
                    )
                source_reader = getattr(self.repository, "list_reusable_outfit_sources", None)
                existing_sources = (source_reader(product_id, limit=30)
                                    if callable(source_reader) else [])
                account = self.repository.get_account_profile(specs[0].account_id)
                if account is None:
                    raise FeishuWorkflowError("图文生产账号不存在")
                remaining_existing_sources = list(existing_sources)
                for index, variation in enumerate(variations, 1):
                    item_id = f"{record.record_id}_item_{index}"
                    prepared = PhotoOutfitSupplyService(
                        generator=self.generator, asset_reader=LightTryonAssetReader(), root=staging_root,
                    ).prepare(
                        record_id=item_id, recipe=recipe, product=product,
                        account=account,
                        existing_sources=remaining_existing_sources,
                        variation=variation,
                    )
                    if variation.get("looks"):
                        from services.photo_content_check import validate_prepared_sources
                        validate_prepared_sources(variation, prepared["sources"])
                    prepared_source_groups.append(list(prepared["sources"]))
                    reused_refs = {
                        str(item.get("look_ref") or "")
                        for item in prepared["sources"]
                        if item.get("source_kind") == "existing_outfit"
                    }
                    if reused_refs:
                        remaining_existing_sources = [
                            item for item in remaining_existing_sources
                            if str(item.get("look_ref") or "") not in reused_refs
                        ]
                    item_roles = [item["role"] for item in prepared["role_plan"]]
                    asset_supply.stage_existing(
                        record_id=item_id, sources=prepared["sources"], required_roles=item_roles,
                        metadata={"theme_key": str(variation_theme.get("theme_key") or ""),
                                  "reference_mode": reference_mode,
                                  "batch_variation": variation},
                    )
                    saved = asset_supply.qualify(
                        record_id=item_id, recipe=recipe, repository=self.repository,
                        reviewer="system_product_outfit_generation", reviewer_type="technical",
                        source="feishu_product_outfit_generated",
                    )
                    pinned_asset_set_ids.append(saved.asset_set_id)
            if len(prepared_source_groups) > 1:
                from services.photo_content_check import validate_batch_sources
                validate_batch_sources(prepared_source_groups)
            if pinned_asset_set_ids:
                if len(pinned_asset_set_ids) != quantity:
                    raise FeishuWorkflowError("批次素材集数量与生成篇数不一致")
                if overrides:
                    overrides = [
                        {**dict(item), "asset_set_id": asset_set_id}
                        for item, asset_set_id in zip(overrides, pinned_asset_set_ids)
                    ]
                else:
                    overrides = [
                        {"asset_set_id": asset_set_id} for asset_set_id in pinned_asset_set_ids
                    ]
            try:
                requests = PhotoRequestFactory(self.repository, layouts=load_board_layouts()).build_batch(
                    record_id=record.record_id, specs=specs,
                    category_key=str(preset.get("category_key") or ""),
                    product_mode=str(preset.get("default_product_mode") or "NO_PRODUCT"),
                    overrides=overrides,
                )
                if style_product:
                    for request in requests:
                        request["product_id"] = str(style_product.get("product_id") or product_id)
                        request["product_snapshot"] = dict(style_product)
                        request["request_sha256"] = fingerprint({
                            key: value for key, value in request.items()
                            if key != "request_sha256"
                        })
                        validate_frozen_request(request)
                if theme or reference_mode:
                    for index, request in enumerate(requests):
                        frozen_manifest = request["asset_snapshot"].get("manifest_json") or {}
                        if isinstance(frozen_manifest, str):
                            frozen_manifest = json.loads(frozen_manifest)
                        if theme:
                            request["copy"] = build_theme_copy(
                                theme, frozen_manifest.get("assets") or [], variations[index]
                            )
                        request["theme_brief"] = {
                            **dict(variation_theme), "reference_mode": reference_mode,
                            "reference_count": len(reference_attachments),
                            "batch_variation": variations[index],
                            **({
                                "travel_theme_type": str(theme.get("travel_theme_type") or ""),
                                "travel_theme_version": int(theme.get("travel_theme_version") or 1),
                                "place": str(travel_topic.get("place") or travel_place or ""),
                                "topic_zh": str(variations[index].get("topic_zh") or ""),
                                "temperature_context": dict(travel_topic.get("temperature_context") or {}),
                            } if theme and theme.get("travel_theme_type") else {}),
                        }
                        request["request_sha256"] = fingerprint({
                            key: value for key, value in request.items() if key != "request_sha256"
                        })
                        validate_frozen_request(request)
            except Exception as exc:
                from services.asset_set_service import AssetSetError
                attachments = self._complete_look_attachments(record.fields)
                if not attachments and (isinstance(exc, AssetSetError) or "NEEDS_CONTENT" in str(exc)):
                    product_id = text_value(record.fields.get(FIELD_PRODUCT))
                    source_reader = getattr(self.repository, "list_reusable_outfit_sources", None)
                    recipe = self.repository.get_content_recipe(specs[0].recipe_id) if len(specs) == 1 else None
                    roles = list((((recipe.recipe_spec_json or {}).get("asset_requirements") or {}).get("required_roles") or [])) if recipe else []
                    sources = (source_reader(product_id, limit=max(4, len(roles)))
                               if product_id and roles and callable(source_reader) else [])
                    if len(sources) >= len(roles) and roles:
                        from services.photo_asset_supply import PhotoAssetSupplyService
                        staging_root = (Path(self.output_root) if self.output_root else
                                        Path.home() / ".openclaw/shared/data/organic_photo_video")
                        staged = PhotoAssetSupplyService(self.client, root=staging_root).stage_existing(
                            record_id=record.record_id, sources=sources, required_roles=roles,
                        )
                        summary = (
                            f"已从产品 {product_id} 的旧穿搭任务选出 {len(roles)} 套不同 Look，"
                            "按 A/B/C/D 暂存。请确认图片满足当前主题后，将审核改为“通过”；"
                            "确认前不会入库、生成或发布。"
                        )
                        self._write_fields(record.record_id, {
                            FIELD_EXECUTE: False, FIELD_PROGRESS: PROGRESS_ACTION,
                            FIELD_REVIEW: REVIEW_NOT_REQUIRED, FIELD_REVIEW_STAGE: "素材已准备",
                            FIELD_PHOTO_ASSET_STATUS: "待内容审核", FIELD_PHOTO_SUMMARY: summary,
                            FIELD_NOTES: "来源为旧穿搭任务的已选中、技术通过图片；勾选确认发布后继续生成并入队。",
                        })
                        return {"record_id": record.record_id, "action": "stage_existing_outfit_assets",
                                "staging_manifest": staged["manifest_path"], "photo_count": len(staged["files"])}
                    raise FeishuWorkflowError(
                        f"{exc}；可从现有穿搭模板流程生成完整造型图后，"
                        "在“图文参考图”按 A/B/C/D 顺序上传。系统不会要求运营填写 JSON。"
                    ) from exc
                raise
            manifest = {
                "schema_version": "opv-photo-batch-v1", "workflow_version": 2,
                "media_kind": "native_photo", "preset": preset_name,
                "entries": [{"spec": asdict(spec), "request": request,
                             "source_record_id": f"{record.record_id}:{index}:{spec.recipe_id}:1"}
                            for index, (spec, request) in enumerate(zip(specs, requests), 1)],
            }
            if content_plan is not None:
                manifest["content_plan"] = content_plan
            manifest["manifest_sha256"] = fingerprint(manifest)
            batch = self.repository.create_production_batch_idempotent(ProductionBatch(
                batch_id="opv_batch_" + fingerprint(record.record_id)[:32],
                source_record_id=record.record_id, expected_count=quantity,
                manifest_json=manifest,
            ))
        self._claim_batch(batch)
        entries = self._photo_batch_entries(batch)
        expected_sources = {entry["source_record_id"] for entry in entries}
        if any(task.source_record_id not in expected_sources for task in self._tasks(record.record_id)):
            raise FeishuWorkflowError("该行存在冻结批次以外的任务，禁止混组")
        requests = [entry["request"] for entry in entries]
        summary = PhotoRequestFactory.summary(requests)
        frozen_content_plan = (batch.manifest_json or {}).get("content_plan")
        if frozen_content_plan:
            from services.photo_content_planner import summarize_batch_plan
            summary += "\n具体内容计划：\n" + summarize_batch_plan(frozen_content_plan)
        output_root = (Path(self.output_root) if self.output_root else
                       Path.home() / ".openclaw/shared/data/organic_photo_video/photo_packages")
        producer = HeroFirstProducer(
            self.repository, self.generator, output_root=output_root,
            asset_readiness_gate=self.asset_readiness_gate, technical_only=True,
        )
        self._write_fields(record.record_id, {
            FIELD_EXECUTE: False, FIELD_PROGRESS: PROGRESS_RUNNING,
            FIELD_REVIEW: REVIEW_PENDING, FIELD_NOTES: "", FIELD_PHOTO_SUMMARY: summary,
            FIELD_PHOTO_ASSET_STATUS: "已匹配可用素材",
        })
        task_ids, paths, failures = [], [], []
        for entry in entries:
            if self._run_lease:
                self._run_lease.check()
            item = entry["request"]
            validate_frozen_request(item)
            source_record_id = entry["source_record_id"]
            try:
                found = [task for task in self._tasks(record.record_id) if task.source_record_id == source_record_id]
                if len(found) > 1:
                    raise FeishuWorkflowError("冻结条目存在重复任务，禁止继续")
                task = found[0] if found else TaskIntakeService(self.repository).create_task(TaskRequest(
                    account_id=item["account_id"],
                    product_id=(str(item.get("product_id") or "") or None),
                    product_snapshot=dict(item.get("product_snapshot") or {}),
                    media_kind="native_photo", category_key=item["category_key"],
                    product_mode=item["product_mode"], source_type=SOURCE_TYPE,
                    source_record_id=source_record_id, feishu_record_id=record.record_id,
                    requested_shot_count=5, created_by="feishu_opv_photo", idempotency_key=source_record_id,
                )).task
                if task.target_country != item["market"] or task.target_locale != item["locale"]:
                    raise FeishuWorkflowError("生产账号市场/语言已变化，与冻结批次不一致")
                if task.task_status == "draft":
                    PhotoReusePlannerService(self.repository).plan_task(
                        task.task_id, recipe_id=item["recipe_id"], variables=item["variables"],
                        copy_block=item["copy"], layout=item["layout_snapshot"],
                        asset_set_id=item["asset_set_id"], recipe_snapshot=item["recipe_snapshot"],
                        asset_snapshot=item["asset_snapshot"], execution_profile_id=item["profile_id"],
                        copy_variant_id=item["copy_variant_id"], content_card=item.get("content_card"),
                        theme_brief=item.get("theme_brief"), operator="feishu_opv_photo",
                    )
                flow = NativePhotoProductionFlow(
                    self.repository, producer, output_root=output_root,
                    vision_service=getattr(self, "photo_reference_vision", None),
                )
                result = flow.prepare(task.task_id, template=item["layout_snapshot"])
                manifest = result.get("photo_manifest") or {}
                if len(manifest.get("slides") or []) != 5:
                    raise FeishuWorkflowError(f"图文任务 {task.task_id} 没有完整成品页")
                task_ids.append(task.task_id)
                paths.extend(str(slide["path"]) for slide in manifest["slides"])
            except Exception as exc:
                failures.append(f"{source_record_id}: {exc}")
        attachments = self._upload_files(paths, parent_type="bitable_image") if paths else []
        self._write_fields(record.record_id, {
            FIELD_PROGRESS: PROGRESS_ACTION if failures else PROGRESS_DONE,
            FIELD_OUTPUT: attachments, FIELD_REVIEW: REVIEW_NOT_REQUIRED,
            FIELD_REVIEW_STAGE: "处理中" if failures else "技术完成", FIELD_PHOTO_SUMMARY: summary,
            FIELD_NOTES: (f"已完成 {len(task_ids)}/{batch.expected_count} 篇原生图文，共 {len(paths)} 张；"
                          + ("勾选执行后沿用冻结方案补齐。" + "；".join(failures) if failures
                             else "技术检查已通过；勾选确认发布后冻结当前五页并进入发布队列。")
                          + "｜" + self._photo_quality_summaries.get(record.record_id, "")
                          )[:1500],
        })
        return {"record_id": record.record_id, "action": "generate_native_photo",
                "task_ids": task_ids, "photo_count": len(paths), "failures": failures}

    def _approve_staged_photo_assets(self, record) -> Dict[str, Any]:
        """Turn an ordered upload into reusable inventory, then produce its package."""
        if self._batch(record.record_id) or self._tasks(record.record_id):
            raise FeishuWorkflowError("已有冻结任务时不能改用暂存素材")
        preset_name = text_value(record.fields.get(FIELD_PRESET))
        preset = self.catalog.metadata(preset_name)
        if preset.get("media_kind") != "native_photo":
            raise FeishuWorkflowError("暂存素材审核只适用于原生图文预设")
        if task_quantity(record.fields) != 1:
            raise FeishuWorkflowError("人工上传素材首版一次只审核并生成 1 篇")
        spec = self.catalog.resolve_batch(preset_name, record.record_id, 1)[0]
        recipe = self.repository.get_content_recipe(spec.recipe_id)
        if recipe is None or recipe.status != "active":
            raise FeishuWorkflowError("图文 Recipe 不存在或已停用")
        from services.photo_asset_supply import PhotoAssetSupplyService
        staging_root = (Path(self.output_root) if self.output_root else
                        Path.home() / ".openclaw/shared/data/organic_photo_video")
        saved = PhotoAssetSupplyService(self.client, root=staging_root).qualify(
            record_id=record.record_id, recipe=recipe, repository=self.repository,
        )
        self._write_fields(record.record_id, {
            FIELD_REVIEW: REVIEW_NOT_REQUIRED, FIELD_PHOTO_ASSET_STATUS: "已确认，正在生成",
            FIELD_NOTES: f"已按确认发布冻结素材集 {saved.asset_set_id} V{saved.asset_set_version}；开始生成原生图文。",
        })
        record.fields[FIELD_PHOTO_ASSET_STATUS] = "已确认，正在生成"
        record.fields[FIELD_REVIEW] = REVIEW_PENDING
        # Pin this run to the exact human-confirmed upload without asking the
        # operator to maintain the compatibility JSON field.
        record.fields[FIELD_PHOTO_REQUEST] = json.dumps({"asset_set_id": saved.asset_set_id})
        result = self._generate(record)
        result["qualified_asset_set_id"] = saved.asset_set_id
        return result

    @staticmethod
    def _photo_batch_entries(batch):
        from services.photo_request_factory import fingerprint, validate_frozen_request
        manifest = batch.manifest_json
        unsigned = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
        if (manifest.get("schema_version") != "opv-photo-batch-v1"
                or manifest.get("manifest_sha256") != fingerprint(unsigned)):
            raise FeishuWorkflowError("冻结图文批次指纹损坏，禁止继续")
        entries = manifest.get("entries") or []
        if not entries or len(entries) != batch.expected_count:
            raise FeishuWorkflowError("冻结批次清单数量损坏，禁止创建或放行")
        sources = []
        for index, entry in enumerate(entries, 1):
            request = entry["request"]
            validate_frozen_request(request)
            expected = f"{batch.source_record_id}:{index}:{request['recipe_id']}:1"
            if entry.get("source_record_id") != expected:
                raise FeishuWorkflowError("冻结图文批次条目身份损坏")
            sources.append(expected)
        if len(set(sources)) != batch.expected_count:
            raise FeishuWorkflowError("冻结图文批次包含重复条目")
        return entries

    @staticmethod
    def _production_plan_note(tasks) -> str:
        entries = []
        for index, task in enumerate(tasks, 1):
            plan = task.plan_json or {}
            if str(getattr(task, "media_kind", "video") or "video") == "native_photo":
                entries.append(f"第{index}条：{len(plan.get('slides') or [])}张原生图文/无视频渲染")
                continue
            shots = plan.get("shots") or []
            sequence = plan.get("outfit_sequence") or []
            # BASE/FINAL may be different wearing states of the same outfit.
            fingerprints = {state.get("outfit_fingerprint") for state in (plan.get("outfit_states") or {}).values()
                            if state.get("outfit_fingerprint")}
            looks = len(sequence) or len(fingerprints) or None
            seconds = sum(int(s.get("duration_ms") or 0) for s in shots) / 1000
            count = f"{looks}套穿搭" if looks is not None else "穿搭套数未标注"
            entries.append(f"第{index}条：{count}/{len(shots)}页/{seconds:g}秒")
        if tasks and all(str(getattr(task, "media_kind", "video")) == "native_photo" for task in tasks):
            return "生成前冻结计划：" + "；".join(entries) + "。有效内容库存不足时整批停止，不减少篇数。"
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
        if all(
            str(getattr(task, "media_kind", "video") or "video") == "native_photo"
            for task in tasks
        ):
            return self._approve_photo_packages(record, tasks)
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

    def _require_travel_copy_release_allowed(self, tasks) -> None:
        """Validate the actual frozen travel package selected for publishing.

        Topic-linked travel copy is generated per task, so a static copy-pack
        status cannot prove that the final title and five overlays are safe.
        The publish checkbox binds the operator decision to this exact package;
        this gate checks its topic binding, locale purity and placeholders.
        """
        from domain.photo_contracts import placeholder_errors
        from services.locale_quality import copy_locale_issues

        for task in tasks:
            recipe_id = str(getattr(task, "recipe_id", "") or "")
            if not recipe_id.startswith("PHOTO_TH_TRAVEL"):
                continue
            package = self.repository.get_content_package(
                str(getattr(task, "content_package_id", "") or "")
            )
            revision = self.repository.get_task_revision(
                str(getattr(task, "active_revision_id", "") or "")
            )
            manifest = dict(getattr(package, "photo_manifest_json", None) or {})
            copy_block = dict(manifest.get("copy") or {})
            theme_brief = dict(manifest.get("theme_brief") or {})
            frozen_theme = dict(
                ((revision.plan_snapshot_json or {}).get("plan") or {}).get("theme_brief") or {}
            ) if revision else {}
            slide_texts = list(copy_block.get("slide_texts") or [])
            if package is None or revision is None or len(slide_texts) != 5 or not all(
                str(value or "").strip() for value in slide_texts
            ):
                raise FeishuWorkflowError("旅行图文缺少完整的最终五页文案")
            if (not theme_brief.get("travel_theme_type")
                    or theme_brief.get("travel_theme_type") != frozen_theme.get("travel_theme_type")
                    or theme_brief.get("theme_key") != frozen_theme.get("theme_key")):
                raise FeishuWorkflowError("旅行图文的主题与最终发布包没有正确绑定")
            issues = placeholder_errors(copy_block)
            issues.extend(copy_locale_issues(copy_block, "th-TH"))
            if issues:
                raise FeishuWorkflowError(
                    "旅行图文最终泰语文案未通过发布检查：" + "；".join(issues)
                )

    def _approve_photo_packages(
        self, record, tasks, *, approval_mode: str = "content_review"
    ) -> Dict[str, Any]:
        from services.workflow_v2 import PhotoPackageReviewService
        from services.release_gate import require_photo_content_allowed
        if approval_mode == "publish_confirmation":
            self._require_travel_copy_release_allowed(tasks)
        review_ids = []
        # Do not partially approve a batch that still has missing/failed media.
        for task in tasks:
            require_photo_content_allowed(self.repository, task)
            if task.task_status == "photo_ready":
                if not self._v2_released(task):
                    raise FeishuWorkflowError("已验收图文的冻结 release 已失效")
            elif task.task_status != "photo_packaging":
                raise FeishuWorkflowError("图文批次尚未全部完成，请先勾选执行补齐")
        for task in tasks:
            if self._run_lease:
                self._run_lease.check()
            if task.task_status == "photo_ready":
                continue  # Previous review committed; retry only its projection.
            compact = approval_mode == "publish_confirmation"
            review = PhotoPackageReviewService(self.repository).record(
                task.task_id, decision="passed",
                dimensions=(
                    {"publish_confirmation": True, "technical_package": True}
                    if compact else {
                        "operator_preview": True,
                        "content_alignment": True,
                        "language_confirmed": True,
                    }
                ),
                evidence=(
                    {
                        "authorization": "feishu_confirm_publish_checkbox",
                        "visual_review_performed": False,
                    }
                    if compact else None
                ),
                reviewer_type="human",
                reviewer=(
                    "feishu_publish_confirmation" if compact
                    else "feishu_human_operator"
                ),
            )
            review_ids.append(review.review_id)
        self._write_fields(record.record_id, {
            FIELD_PROGRESS: PROGRESS_DONE,
            FIELD_REVIEW: REVIEW_NOT_REQUIRED if approval_mode == "publish_confirmation" else REVIEW_APPROVED,
            FIELD_REVIEW_STAGE: "技术完成" if approval_mode == "publish_confirmation" else "已验收",
            FIELD_CONFIRM_PUBLISH: approval_mode == "publish_confirmation",
            FIELD_NOTES: (
                f"已按确认发布冻结 {len(tasks)} 篇原生图文，正在进入主排班池。"
                if approval_mode == "publish_confirmation"
                else f"已人工验收 {len(tasks)} 篇原生图文；勾选确认发布后进入主排班池。"
            ),
        })
        return {
            "record_id": record.record_id, "action": "approve_native_photo",
            "task_ids": [task.task_id for task in tasks], "review_ids": review_ids,
        }

    def _confirm_photo_publish(self, record) -> Dict[str, Any]:
        """Use 确认发布 as the only operator gate for native-photo content."""
        preset_name = text_value(record.fields.get(FIELD_PRESET))
        tasks = self._tasks(record.record_id)
        photo_flow = bool(tasks) and all(
            str(getattr(task, "media_kind", "video") or "video") == "native_photo"
            for task in tasks
        )
        if not tasks and preset_name and self.catalog.is_native_photo(preset_name):
            generated = self._generate(record)
            if generated.get("action") in {
                "stage_photo_assets", "stage_existing_outfit_assets",
                "prepare_product_outfit_assets",
            }:
                record.fields[FIELD_PHOTO_ASSET_STATUS] = "待内容审核"
                generated = self._approve_staged_photo_assets(record)
            tasks = self._tasks(record.record_id)
            photo_flow = bool(tasks) and all(
                str(getattr(task, "media_kind", "video") or "video") == "native_photo"
                for task in tasks
            )
        elif not tasks:
            raise FeishuWorkflowError("找不到该飞书记录对应的 RDS 任务")
        if not photo_flow:
            # Existing video behavior remains unchanged.
            if text_value(record.fields.get(FIELD_PROGRESS)) != PROGRESS_DONE:
                raise FeishuWorkflowError("视频尚未完成技术生产，不能确认发布")
            return self._schedule_publish(record)

        self._assert_batch_complete(record.record_id, tasks)
        if any(task.task_status == "photo_packaging" for task in tasks):
            self._approve_photo_packages(
                record, tasks, approval_mode="publish_confirmation"
            )
            tasks = self._tasks(record.record_id)
        if any(task.task_status != "photo_ready" for task in tasks):
            raise FeishuWorkflowError("图文尚未完成技术生产，不能确认发布")
        return self._schedule_publish(record)

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
        photo_only = all(
            str(getattr(task, "media_kind", "video") or "video") == "native_photo"
            for task in tasks
        )
        # Preflight every task before enqueuing any member of this row.
        for task in tasks:
            if workflow_v2_enabled(task) and not self._v2_released(task):
                raise FeishuWorkflowError("当前 V2 版本尚未通过终审，不能确认发布")
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
            note = (f"已进入主排班池，共 {len(slots)} 篇原生图文；"
                    "按店铺养号配额分配具备图文直发能力的账号和时间，不挂商品；"
                    "CreatOK 原生图文由 TikTok 自动添加推荐音乐，不承诺具体曲目。"
                    if photo_only else
                    f"已进入短视频主排班池，共 {len(slots)} 条。"
                    "按店铺养号配额自动分配账号和时间，非带货/不挂车；"
                    "在未来 48 小时排期窗口内提前选择当地 NeoBund BGM 并创建定时任务。")
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
            FIELD_MUSIC_MODE: (
                "TikTok 平台自动推荐" if photo_only and enqueue_main
                else "发布窗口自动选曲"
            ),
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
