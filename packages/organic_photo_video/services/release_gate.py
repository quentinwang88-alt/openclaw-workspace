"""Frozen release contract; deliberately stdlib-only for the main publisher."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


class ReleaseGateError(RuntimeError):
    pass


def assert_main_queue_rework_allowed(task_id: str, **kwargs: Any) -> None:
    from services.main_schedule_bridge import assert_main_queue_rework_allowed as guard
    guard(task_id, **kwargs)


def contract_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":")).encode()).hexdigest()


def file_hash(path: str) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


TECHNICAL_REVIEWER = "opv_technical_pipeline"


def technical_evidence_valid(evidence: Any, scope: str) -> bool:
    """Technical evidence is not an aesthetic/model/human approval."""
    if not isinstance(evidence, dict):
        return False
    checks = evidence.get("checks") or {}
    required = {"frozen_inputs", "file_hash", "media_qc"}
    required.add("decoded_images" if scope in {"anchor", "group"} else "render_input_binding")
    return (evidence.get("schema_version") == "opv-technical-check-v1"
            and evidence.get("production_policy") == "technical_only"
            and evidence.get("scope") == scope
            and evidence.get("visual_review_performed") is False
            and all(checks.get(key) is True for key in required))


def trusted_review(review: Any) -> bool:
    kind = str(review.reviewer_type or "")
    name = str(review.reviewer or "").lower()
    if kind == "technical":
        return name == TECHNICAL_REVIEWER and technical_evidence_valid(review.evidence_json, review.scope)
    return kind in {"human", "model"} and not (
        kind == "human" and any(word in name for word in ("codex", "chatgpt", "assistant"))
    )


def expected_render_fingerprint(revision: Any) -> str:
    snapshot = revision.plan_snapshot_json or {}
    plan = snapshot.get("plan") or {}
    from domain.contracts import ContractViolationError, expected_plan_shot_count, is_multi_look_plan
    if is_multi_look_plan(plan):
        try:
            expected_plan_shot_count(plan)
        except ContractViolationError as exc:
            raise ReleaseGateError(str(exc)) from exc
    manifest = revision.asset_manifest_json or {}
    selected, candidates = manifest.get("selected") or {}, manifest.get("candidates") or {}
    selection = []
    for shot in sorted(plan.get("shots") or [], key=lambda item: int(item["slot_index"])):
        slot = int(shot["slot_index"])
        candidate = candidates.get(selected.get(f"shot:{slot}")) or {}
        if not candidate.get("asset_id") or not candidate.get("sha256"):
            raise ReleaseGateError(f"已验收版本缺少冻结镜头 shot:{slot}")
        selection.append({"slot": slot, "shot": candidate["asset_id"], "sha256": candidate["sha256"]})
    if not selection:
        raise ReleaseGateError("已验收版本没有镜头选择清单")
    return contract_hash({"revision": revision.revision_id, "selection": selection,
                          "copy": snapshot.get("copy") if "copy" in snapshot else plan.get("copy") or {},
                          "render_contract": plan.get("render_contract") or {}})


def freeze_release(repository: Any, task: Any, render: Any) -> dict:
    revision_id = str(task.released_revision_id or "")
    if (not revision_id or task.active_revision_id != revision_id or render is None
            or render.origin_revision_id != revision_id
            or task.selected_render_id != render.render_id or not render.publish_ready
            or render.qc_status != "passed" or not render.input_fingerprint):
        raise ReleaseGateError("Workflow V2 没有当前版本的已验收成片")
    revision = repository.get_task_revision(revision_id)
    if (revision is None or revision.task_id != task.task_id
            or revision.revision_status != "released"):
        raise ReleaseGateError("已验收 revision 不存在或归属不匹配")
    if (contract_hash(revision.plan_snapshot_json) != revision.input_snapshot_hash
            or contract_hash((revision.asset_manifest_json or {}).get("selected") or {}) != revision.selection_hash
            or expected_render_fingerprint(revision) != render.input_fingerprint):
        raise ReleaseGateError("成片输入指纹与当前冻结 revision 不一致")
    reviews = repository.list_quality_reviews(revision_id, scope="render", target_id=render.render_id)
    review = next((r for r in reversed(reviews)
                   if r.input_fingerprint == render.input_fingerprint), None)
    if (review is None or review.decision not in {"passed", "waived"}
            or not trusted_review(review)
            or (review.evidence_json or {}).get("render_sha256") != render.output_sha256):
        raise ReleaseGateError("成片检查缺失、过期或检查来源无效")
    if file_hash(render.output_url) != render.output_sha256:
        raise ReleaseGateError("已验收成片文件 SHA256 已变化")
    manifest = {
        "schema_version": "opv-release-v1", "workflow_version": 2,
        "task_id": task.task_id, "revision_id": revision_id,
        "render_id": render.render_id, "review_id": review.review_id,
        "reviewer_type": review.reviewer_type, "reviewer": review.reviewer,
        "review_decision": review.decision,
        "input_fingerprint": render.input_fingerprint,
        "selection_hash": revision.selection_hash,
        "input_snapshot_hash": revision.input_snapshot_hash,
        "video_path": str(Path(render.output_url).resolve()),
        "video_sha256": render.output_sha256,
        "copy": dict(getattr(render, "copy_snapshot_json", {}) or
                     revision.plan_snapshot_json.get("copy") or {}),
    }
    if review.reviewer_type == "technical":
        manifest["production_policy"] = "technical_only"
        manifest["technical_evidence"] = dict(review.evidence_json)
    manifest["manifest_sha256"] = contract_hash(manifest)
    return manifest


def validate_upload(context: dict, *, video_path: str, script_id: str, title: str) -> None:
    """Revalidate the exact release bytes immediately before remote upload.

    Legacy V1 OPV rows remain compatible. V2 metadata must carry a manifest;
    neither a changed path, changed title nor a stale local video is allowed.
    """
    manifest = context.get("release_manifest")
    if (int(context.get("workflow_version") or 1) < 2 and not manifest
            and context.get("schema_version") != "opv-main-publish-v2"):
        return
    if not isinstance(manifest, dict):
        raise ReleaseGateError("Workflow V2 发布缺少冻结 release 清单，需重新验收入队")
    unsigned = {k: v for k, v in manifest.items() if k != "manifest_sha256"}
    technical = (manifest.get("reviewer_type") == "technical"
                 and manifest.get("reviewer") == TECHNICAL_REVIEWER
                 and manifest.get("production_policy") == "technical_only"
                 and technical_evidence_valid(manifest.get("technical_evidence"), "render")
                 and manifest.get("technical_evidence", {}).get("selection_hash") == manifest.get("selection_hash")
                 and manifest.get("technical_evidence", {}).get("input_snapshot_hash") == manifest.get("input_snapshot_hash")
                 and manifest.get("technical_evidence", {}).get("render_sha256") == manifest.get("video_sha256"))
    required = ("task_id", "revision_id", "render_id", "review_id", "input_fingerprint",
                "selection_hash", "input_snapshot_hash", "video_path", "video_sha256")
    if (any(not manifest.get(key) for key in required)
            or manifest.get("manifest_sha256") != contract_hash(unsigned)
            or (manifest.get("reviewer_type") not in {"human", "model"} and not technical)
            or (manifest.get("reviewer_type") == "human" and any(
                word in str(manifest.get("reviewer") or "").lower() for word in ("codex", "chatgpt", "assistant")))
            or manifest.get("review_decision") not in {"passed", "waived"}
            or manifest.get("task_id") != script_id
            or context.get("publish_title") != title):
        raise ReleaseGateError("Workflow V2 release 清单或发布标题不匹配")
    if (str(Path(video_path).resolve()) != manifest["video_path"]
            or file_hash(video_path) != manifest["video_sha256"]):
        raise ReleaseGateError("上传前成片 SHA256/路径与验收版本不一致，已阻止发布")
