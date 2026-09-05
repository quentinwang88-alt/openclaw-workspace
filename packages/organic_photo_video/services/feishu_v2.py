"""Version-aware, one-command/one-human-gate Feishu workbench controller.

This mixin never infers approval from a waiting row. RDS owns the stage and
revision; Feishu carries an explicit decision bound to the displayed token.
"""

from __future__ import annotations

from dataclasses import asdict
import hashlib
from pathlib import Path

from services.workflow_v2 import (
    AnchorReviewService, RenderReviewService, ReworkService,
    RevisionAssetResolver, ScopedReviewService, WorkflowV2Error,
    canonical_hash, frozen_task_view, workflow_v2_enabled,
)


FIELD_REVIEW_MODE = "审核方式"
FIELD_REVIEW_STAGE = "审核阶段"
FIELD_RETRY_REVIEW = "重试审核"
FIELD_REVIEW_TOKEN = "审核批次"
MODE_AUTO = "自动审核"
MODE_HUMAN = "人工确认"
STAGE_LABELS = {"anchor_review": "锚点技术检查", "image_review": "组图技术检查", "video_review": "成片技术检查"}


class FeishuV2Mixin:
    @staticmethod
    def _verify_v2_asset(asset):
        path = Path(str(asset.get("path") or ""))
        if not path.is_file():
            raise WorkflowV2Error(f"审核素材不存在：{asset.get('asset_id', '')}")
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        if digest.hexdigest() != asset.get("sha256"):
            raise WorkflowV2Error(f"审核素材文件已变化：{asset.get('asset_id', '')}；请返工，不能沿用原审核批次")

    @staticmethod
    def _review_failure_details(review):
        codes = getattr(review, "reason_codes_json", None) or getattr(review, "reason_codes", None) or []
        evidence = getattr(review, "evidence_json", None) or {}
        notes = evidence.get("notes") or getattr(review, "notes", "")
        actions = evidence.get("next_actions") or getattr(review, "next_actions", None) or []
        return "；".join(str(value) for value in (", ".join(codes), notes, ", ".join(actions)) if value)[:500]

    def _v2_tasks(self, record_id):
        return [task for task in self._tasks(record_id) if workflow_v2_enabled(task)]

    def _review_mode(self, record):
        # Empty values and legacy 无需审核 are never an implicit approval.
        from services.feishu_workflow import text_value
        return MODE_AUTO if text_value(record.fields.get(FIELD_REVIEW_MODE)) == MODE_AUTO else MODE_HUMAN

    def _v2_anchor_asset(self, task):
        revision = self.repository.get_task_revision(task.active_revision_id)
        if revision is None:
            raise WorkflowV2Error("锚点审核缺少当前版本")
        plan = revision.plan_snapshot_json.get("plan") or {}
        anchor_slot = int(plan.get("anchor_slot") or 1)
        candidates = revision.asset_manifest_json.get("candidates") or {}
        rows = [s for s in self.repository.list_shots(task.task_id)
                if s.slot_index == anchor_slot and s.origin_revision_id == revision.revision_id
                and s.shot_id in candidates and s.shot_status == "generated" and s.image_url]
        if not rows:
            raise WorkflowV2Error("当前版本没有可审核的锚点候选")
        shot = max(rows, key=lambda s: s.shot_version)
        asset = RevisionAssetResolver.candidate(revision, shot.shot_id)
        if asset["sha256"] != shot.image_sha256:
            raise WorkflowV2Error("锚点候选与冻结文件不一致")
        self._verify_v2_asset(asset)
        return asset

    def _v2_released(self, task):
        from services.release_gate import freeze_release, ReleaseGateError
        try:
            freeze_release(self.repository, task, self.repository.get_render(task.selected_render_id or ""))
            return True
        except (ReleaseGateError, OSError):
            return False

    def _v2_snapshot(self, tasks):
        tasks = [self.repository.get_task(task.task_id) for task in tasks]
        awaiting = [t for t in tasks if t and not self._v2_released(t)]
        priority = {"anchor_review": 0, "image_review": 1, "video_review": 2}
        waiting = [t for t in awaiting if t.task_status in priority]
        if not waiting:
            release_token = canonical_hash({"released": [
                {"task": t.task_id, "revision": t.released_revision_id, "render": t.selected_render_id}
                for t in tasks
            ]}) if not awaiting else ""
            return {"stage": "complete" if not awaiting else "working", "targets": [], "paths": [], "token": release_token}
        stage = min((t.task_status for t in waiting), key=priority.get)
        targets = [t for t in waiting if t.task_status == stage]
        paths, payload, issues = [], [], []
        for task in targets:
            revision = self.repository.get_task_revision(task.active_revision_id)
            if stage == "anchor_review":
                try:
                    assets = [self._v2_anchor_asset(task)]
                except WorkflowV2Error as exc:
                    assets = []
                    issues.append(str(exc))
            elif stage == "image_review":
                specs = (revision.plan_snapshot_json.get("plan") or {}).get("shots") or []
                assets = []
                for spec in specs:
                    try:
                        assets.append(RevisionAssetResolver.selected(revision, f"shot:{int(spec['slot_index'])}"))
                    except WorkflowV2Error:
                        issues.append(f"{task.task_id} 缺少 P{spec['slot_index']}，请选择重做P{spec['slot_index']}")
            else:
                render = self.repository.get_render(task.selected_render_id or "")
                if not render or render.origin_revision_id != task.active_revision_id or render.qc_status != "passed":
                    issues.append("当前版本没有媒体质检通过的成片，请选择重做成片")
                    assets = []
                else:
                    assets = [{"asset_id": render.render_id, "sha256": render.output_sha256, "path": render.output_url}]
            verified = []
            for asset in assets:
                try:
                    self._verify_v2_asset(asset)
                    verified.append(asset)
                except WorkflowV2Error as exc:
                    issues.append(str(exc))
            paths.extend(asset["path"] for asset in verified)
            payload.append({"task": task.task_id, "revision": task.active_revision_id,
                            "assets": [{"id": a["asset_id"], "sha256": a["sha256"]} for a in assets]})
        return {"stage": stage, "targets": targets, "paths": paths,
                "issues": issues, "reviewable": not issues,
                "token": canonical_hash({"stage": stage, "targets": payload, "issues": issues})}

    def _project_v2(self, record, tasks, *, error=""):
        from services.feishu_workflow import (
            FIELD_EXECUTE, FIELD_REVIEW, FIELD_PROGRESS, FIELD_OUTPUT, FIELD_NOTES,
            FIELD_CONFIRM_PUBLISH, PROGRESS_ACTION, PROGRESS_REVIEW, PROGRESS_DONE,
            REVIEW_NOT_REQUIRED,
        )
        snapshot = self._v2_snapshot(tasks)
        self._assert_batch_complete(record.record_id, tasks)
        error = error or "；".join(snapshot.get("issues") or [])
        stage = snapshot["stage"]
        fields = {FIELD_EXECUTE: False, FIELD_RETRY_REVIEW: False, FIELD_REVIEW: REVIEW_NOT_REQUIRED,
                  FIELD_CONFIRM_PUBLISH: False, FIELD_REVIEW_TOKEN: snapshot["token"],
                  FIELD_REVIEW_MODE: None}
        paths = snapshot["paths"]
        if stage == "complete":
            paths = [self.repository.get_render(self.repository.get_task(t.task_id).selected_render_id).output_url for t in tasks]
            fields.update({FIELD_PROGRESS: PROGRESS_DONE, FIELD_REVIEW_STAGE: "技术完成",
                           FIELD_NOTES: f"{len(tasks)} 条成片已完成技术检查，可查看预览。"
                                        f"{self._production_plan_note(tasks)}"
                                        "未做独立视觉审核；是否发布请明确勾选确认发布，发布时选择 BGM。"})
        else:
            fields.update({FIELD_PROGRESS: PROGRESS_ACTION,
                           FIELD_REVIEW_STAGE: STAGE_LABELS.get(stage, "处理中"),
                           FIELD_NOTES: error or "主链仅做技术检查，不需要分阶段审核。勾选执行继续生产；已有素材会按冻结版本复用。"})
        if paths:
            fields[FIELD_OUTPUT] = self._upload_files(paths, parent_type="bitable_file" if stage in {"video_review", "complete"} else "bitable_image")
        self._write_fields(record.record_id, fields)
        return {"record_id": record.record_id, "action": "complete" if stage == "complete" else f"await_{stage}",
                "task_ids": [t.task_id for t in tasks], "stage": stage}

    def _v2_continue_generation(self, tasks):
        from services.hero_first import HeroFirstProducer
        from services.video_render_flow import VideoRenderFlow
        from services.technical_production import TechnicalProductionFlow
        producer = HeroFirstProducer(self.repository, self.generator, output_root=self.output_root,
                                     asset_readiness_gate=self.asset_readiness_gate, technical_only=True)
        flow = TechnicalProductionFlow(self.repository, producer,
            VideoRenderFlow(self.repository, self.renderer, output_root=self.output_root))
        errors = []
        for item in tasks:
            if self._run_lease:
                self._run_lease.check()
            try:
                flow.run(item.task_id, overlay_profile_id=self.catalog.overlay_profile_id or None)
            except Exception as exc:
                errors.append(f"{item.task_id}: {exc}")
        if errors:
            raise WorkflowV2Error("；".join(errors))

    def _v2_review_stage(self, snapshot, *, automatic, record):
        raise WorkflowV2Error("分阶段视觉审核已退出生产主链；请使用独立诊断工具")

    def _advance_v2(self, record, tasks, *, approve=False, automatic=False, resume_generation=True):
        from services.feishu_workflow import FIELD_REVIEW, FIELD_EXECUTE, FIELD_PROGRESS, REVIEW_NOT_REQUIRED, PROGRESS_RUNNING, text_value
        snapshot = self._v2_snapshot(tasks)
        self._assert_batch_complete(record.record_id, tasks)
        if snapshot["stage"] == "complete":
            return self._project_v2(record, tasks)
        if approve or not resume_generation:
            return self._project_v2(record, tasks, error="旧审核命令已停用；请勾选执行继续技术检查流程，不会调用独立视觉审核。")
        # Consume the explicit command before side effects. An interrupted
        # scanner cannot interpret the old 通过 as approval for the next gate.
        self._write_fields(record.record_id, {
            FIELD_REVIEW: REVIEW_NOT_REQUIRED, FIELD_REVIEW_MODE: None, FIELD_RETRY_REVIEW: False,
            FIELD_EXECUTE: False, FIELD_PROGRESS: PROGRESS_RUNNING,
        })
        try:
            self._v2_continue_generation(tasks)
        except Exception as exc:
            from services.production_batch import ProjectionPendingError
            if isinstance(exc, ProjectionPendingError):
                raise
            return self._project_v2(record, tasks, error=f"技术生产未完成：{str(exc)[:650]}。勾选执行仅补失败/未开始镜头；已生成素材变化或成片失败请选择对应返工。在途结果未知时禁止重复生成。")
        return self._project_v2(record, tasks)

    def _redo_v2(self, record, tasks, action):
        from services.feishu_workflow import FIELD_REVIEW_TOKEN, FIELD_NOTES, FIELD_REVIEW, REVIEW_NOT_REQUIRED, text_value
        requested = [] if action in {"redo_all", "redo_render"} else [int(action.split("_")[1])]
        snapshot = self._v2_snapshot(tasks)
        if text_value(record.fields.get(FIELD_REVIEW_TOKEN)) != snapshot["token"] or not snapshot["token"]:
            return self._project_v2(record, tasks, error="返工批次已变化，请在刷新后的预览上重新选择返工镜头。")
        # Validate the complete row before mutating any of its tasks.
        for task in tasks:
            if task.task_status not in {"anchor_review", "image_review", "video_review", "rework_pending"}:
                raise WorkflowV2Error("仅允许审核阶段返工；发布中的任务不能改写")
            revision = self.repository.get_task_revision(task.active_revision_id)
            anchor = int((revision.plan_snapshot_json.get("plan") or {}).get("anchor_slot") or 1)
            if task.task_status == "anchor_review" and requested and anchor not in requested:
                raise WorkflowV2Error(f"当前仍在锚点审核，请先确认锚点或重做 P{anchor}，不能直接重做下游镜头")
            state_reader = getattr(self.publish_scheduler, "get_task_state", None)
            state = state_reader(task.task_id) if callable(state_reader) else {}
            if state and state.get("status") in {"待排期", "待排班", "已排期", "发布中", "已发布"}:
                raise WorkflowV2Error("该任务已进入发布队列，请先明确处理排程后再返工")
        self._write_fields(record.record_id, {FIELD_REVIEW: REVIEW_NOT_REQUIRED, FIELD_REVIEW_MODE: None, FIELD_RETRY_REVIEW: False})
        for task in tasks:
            revision = self.repository.get_task_revision(task.active_revision_id)
            plan = revision.plan_snapshot_json.get("plan") or {}
            anchor = int(plan.get("anchor_slot") or 1)
            scope = "render" if action == "redo_render" else ("anchor" if not requested or anchor in requested else "shot")
            if scope == "render" and task.task_status != "video_review":
                raise WorkflowV2Error("重做成片仅适用于成片审核阶段")
            ReworkService(self.repository).begin(
                task.task_id, expected_revision_id=revision.revision_id, expected_lock_version=revision.lock_version,
                scope=scope, slot_indexes=requested if scope == "shot" else (),
                reason=text_value(record.fields.get(FIELD_NOTES)) or f"飞书明确请求 {action}",
                idempotency_key=canonical_hash({"record": record.record_id, "revision": revision.revision_id,
                                                "token": text_value(record.fields.get(FIELD_REVIEW_TOKEN)), "action": action}),
                operator="feishu_explicit_operator_rework",
            )
        return self._advance_v2(record, tasks)
