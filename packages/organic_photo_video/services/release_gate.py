"""Frozen release contract; deliberately stdlib-only for the main publisher."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


class ReleaseGateError(RuntimeError):
    pass


def require_photo_content_allowed(repository: Any, task: Any) -> None:
    """A rejected task cannot be revived by a later approval or rework."""
    if str(getattr(task, "media_kind", "video")) != "native_photo":
        return
    reader = getattr(repository, "list_content_rejections", None)
    if callable(reader):
        rejected = reader(task.task_id)
    else:
        # Lightweight repositories used by local tests/diagnostics have no SQL
        # projection; inspect their active review history when available.
        reader = getattr(repository, "list_quality_reviews", None)
        reviews = reader(task.active_revision_id, scope="photo_package") if callable(reader) else []
        rejected = [r for r in reviews if "CONTENT_REJECTED" in (r.reason_codes_json or [])]
    if rejected:
        raise ReleaseGateError("CONTENT_REJECTED: 内容验收未通过，禁止该任务放行或发布；需另建修正样板")


def require_photo_language_review(repository: Any, task: Any, package_manifest: dict) -> None:
    """Apply an opt-in, Recipe-owned native-language release requirement.

    Existing photo Recipes intentionally keep their historical human-confirmation
    behavior.  New Recipes can make a frozen copy review status mandatory without
    smuggling that policy into the Feishu checkbox semantics.
    """
    get_recipe = getattr(repository, "get_content_recipe", None)
    if not callable(get_recipe):
        return
    recipe_id = str(getattr(task, "recipe_id", "") or "")
    recipe = get_recipe(recipe_id) if recipe_id else None
    spec = dict(getattr(recipe, "recipe_spec_json", None) or {})
    required = str(
        (spec.get("release_requirements") or {}).get(
            "required_language_review_status"
        ) or ""
    )
    if not required:
        return
    actual = str((package_manifest.get("copy") or {}).get("language_review_status") or "")
    if actual != required:
        raise ReleaseGateError(
            f"LANGUAGE_REVIEW_REQUIRED: 发布要求 {required}，当前为 {actual or '未审校'}"
        )
    if required == "NATIVE_APPROVED":
        frozen_copy = package_manifest.get("copy") or {}
        audit = frozen_copy.get("language_review") or {}
        if (not isinstance(audit, dict)
                or not str(audit.get("reviewed_by") or "").strip()
                or not str(audit.get("reviewed_at") or "").strip()
                or len(str(audit.get("review_sha256") or "")) != 64):
            raise ReleaseGateError(
                "LANGUAGE_REVIEW_REQUIRED: NATIVE_APPROVED 缺少审校人、时间或内容哈希"
            )
        from services.photo_copy_review import frozen_copy_review_sha256
        if str(audit.get("review_sha256") or "") != frozen_copy_review_sha256(frozen_copy):
            raise ReleaseGateError(
                "LANGUAGE_REVIEW_REQUIRED: 审校后的泰语文案已发生变化"
            )


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
    required.add(
        "decoded_images" if scope in {"anchor", "group", "photo_package"}
        else "render_input_binding"
    )
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


def trusted_photo_content_review(review: Any) -> bool:
    """A native-photo release needs one explicit operator authorization.

    Older releases used three separate preview/content/language confirmations.
    The compact flow binds the publish checkbox to the exact package fingerprint
    after technical checks, so that one action can both freeze and authorize it.
    """
    if str(getattr(review, "scope", "")) != "photo_package":
        return False
    if str(getattr(review, "decision", "")) != "passed":
        return False
    if str(getattr(review, "reviewer_type", "")) != "human":
        return False
    if not trusted_review(review):
        return False
    dimensions = dict(getattr(review, "dimensions_json", None) or {})
    legacy_review = all(dimensions.get(key) is True for key in (
        "operator_preview", "content_alignment", "language_confirmed",
    ))
    publish_confirmation = (
        dimensions.get("publish_confirmation") is True
        and dimensions.get("technical_package") is True
    )
    return legacy_review or publish_confirmation


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


def freeze_photo_release(repository: Any, task: Any) -> dict:
    """Rebuild and verify the exact ordered native-photo release contract."""
    require_photo_content_allowed(repository, task)
    revision_id = str(task.released_revision_id or "")
    package_id = str(getattr(task, "content_package_id", "") or "")
    if (str(getattr(task, "media_kind", "video") or "video") != "native_photo"
            or not revision_id or task.active_revision_id != revision_id or not package_id):
        raise ReleaseGateError("Workflow V2 没有当前版本的已验收图文包")
    revision = repository.get_task_revision(revision_id)
    package = repository.get_content_package(package_id)
    if (revision is None or revision.task_id != task.task_id
            or revision.revision_status != "released"
            or package is None or package.task_id != task.task_id
            or package.status != "ready"):
        raise ReleaseGateError("已验收图文 revision 或内容包不存在或归属不匹配")
    if (contract_hash(revision.plan_snapshot_json) != revision.input_snapshot_hash
            or contract_hash((revision.asset_manifest_json or {}).get("selected") or {})
            != revision.selection_hash):
        raise ReleaseGateError("图文输入或选图与当前冻结 revision 不一致")
    package_manifest = dict(getattr(package, "photo_manifest_json", None) or {})
    require_photo_language_review(repository, task, package_manifest)
    slides = list(package_manifest.get("slides") or [])
    if (package_manifest.get("schema_version") != "opv-photo-package-v1"
            or package_manifest.get("media_kind") != "native_photo"
            or package_manifest.get("revision_id") != revision_id
            or package_manifest.get("content_package_id") != package_id
            or not slides):
        raise ReleaseGateError("已验收图文包清单缺失或版本不匹配")

    from services.workflow_v2 import (
        RevisionAssetResolver, photo_package_fingerprint_input,
    )
    release_slides = []
    for expected_index, slide in enumerate(slides, 1):
        asset = RevisionAssetResolver.selected(revision, f"slide:{expected_index}")
        path = str(Path(asset["path"]).resolve())
        if (int(slide.get("index") or 0) != expected_index
                or slide.get("asset_id") != asset["asset_id"]
                or slide.get("sha256") != asset["sha256"]
                or file_hash(path) != asset["sha256"]):
            raise ReleaseGateError(f"已验收图文第 {expected_index} 页文件或顺序已变化")
        release_slides.append({
            "index": expected_index, "asset_id": asset["asset_id"],
            "path": path, "sha256": asset["sha256"],
            "mime_type": str(slide.get("mime_type") or "image/jpeg"),
            "width": int(slide.get("width") or 0),
            "height": int(slide.get("height") or 0),
            "source_asset_ids": list(slide.get("source_asset_ids") or []),
        })
    extra = photo_package_fingerprint_input(package)
    input_fingerprint = RevisionAssetResolver.fingerprint(
        revision, [f"slide:{index}" for index in range(1, len(slides) + 1)],
        extra=extra,
    )
    reviews = repository.list_quality_reviews(
        revision_id, scope="photo_package", target_id=package_id
    )
    review = next(
        (item for item in reversed(reviews) if item.input_fingerprint == input_fingerprint),
        None,
    )
    if review is None or not trusted_photo_content_review(review):
        raise ReleaseGateError("图文包缺少当前版本的人工内容与语言确认")
    manifest = {
        "schema_version": "opv-photo-release-v1", "workflow_version": 2,
        "media_kind": "native_photo", "task_id": task.task_id,
        "revision_id": revision_id, "content_package_id": package_id,
        "review_id": review.review_id, "reviewer_type": review.reviewer_type,
        "reviewer": review.reviewer, "review_decision": review.decision,
        "content_review": dict(review.dimensions_json or {}),
        "input_fingerprint": input_fingerprint,
        "selection_hash": revision.selection_hash,
        "input_snapshot_hash": revision.input_snapshot_hash,
        "template_id": str(package_manifest.get("template_id") or ""),
        "template_version": int(package_manifest.get("template_version") or 0),
        "cover_index": int(package_manifest.get("cover_index") or 1),
        "copy": dict(package_manifest.get("copy") or {}),
        "theme_brief": dict(package_manifest.get("theme_brief") or {}),
        "slides": release_slides,
    }
    # 目标发布账号（additive）：任务冻结了目标账号时进入 manifest 并被
    # manifest_sha256 覆盖；旧任务缺省不新增键，历史清单 hash 逐字不变。
    target_account = str(getattr(task, "target_publish_account_id", "") or "")
    if target_account:
        manifest["target_publish_account_id"] = target_account
    manifest["manifest_sha256"] = contract_hash(manifest)
    return manifest


def _assert_target_account_consistency(manifest: dict, context: dict,
                                       account_id: str = "") -> None:
    """队列目标与冻结清单必须一致；任一侧缺失目标时按旧语义放行。

    防线语义：manifest 冻结了 ``target_publish_account_id`` 时，
    (a) 队列 context 若也带目标，两者必须相同（防入队后被改绑）；
    (b) 调用方传入实际领取账号（发布器提交前核对）时必须相同（防串号）。
    """
    manifest_target = str(manifest.get("target_publish_account_id") or "")
    if not manifest_target:
        return
    context_target = str(context.get("target_publish_account_id") or "")
    if context_target and context_target != manifest_target:
        raise ReleaseGateError("发布队列目标账号与冻结清单不一致，已阻止提交")
    submitting = str(account_id or "").strip()
    if submitting and submitting != manifest_target:
        raise ReleaseGateError(
            f"当前领取账号 {submitting} 不是冻结目标账号 {manifest_target}，已阻止提交"
        )


def validate_upload(context: dict, *, video_path: str, script_id: str, title: str,
                    account_id: str = "") -> None:
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
    _assert_target_account_consistency(manifest, context, account_id)


def validate_photo_upload(
    context: dict, *, media_paths: list[str], script_id: str, title: str,
    account_id: str = "",
) -> None:
    """Validate ordered photo bytes immediately before any remote upload."""
    manifest = context.get("release_manifest")
    if not isinstance(manifest, dict):
        raise ReleaseGateError("Workflow V2 图文发布缺少冻结 release 清单")
    unsigned = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    required = (
        "task_id", "revision_id", "content_package_id", "review_id",
        "input_fingerprint", "selection_hash", "input_snapshot_hash",
        "template_id", "template_version", "slides",
    )
    content_review = dict(manifest.get("content_review") or {})
    if (manifest.get("schema_version") != "opv-photo-release-v1"
            or manifest.get("media_kind") != "native_photo"
            or any(not manifest.get(key) for key in required)
            or manifest.get("manifest_sha256") != contract_hash(unsigned)
            or manifest.get("reviewer_type") != "human"
            or (manifest.get("reviewer_type") == "human" and any(
                word in str(manifest.get("reviewer") or "").lower()
                for word in ("codex", "chatgpt", "assistant")
            ))
            or manifest.get("review_decision") != "passed"
            or not (
                all(content_review.get(key) is True for key in (
                    "operator_preview", "content_alignment", "language_confirmed",
                ))
                or (
                    content_review.get("publish_confirmation") is True
                    and content_review.get("technical_package") is True
                )
            )
            or manifest.get("task_id") != script_id
            or context.get("publish_title") != title):
        raise ReleaseGateError("Workflow V2 图文 release 清单或发布标题不匹配")
    slides = list(manifest.get("slides") or [])
    if len(media_paths) != len(slides):
        raise ReleaseGateError("上传图片数量与验收图文包不一致")
    for index, (provided, slide) in enumerate(zip(media_paths, slides), 1):
        expected_path = str(Path(str(slide.get("path") or "")).resolve())
        if (int(slide.get("index") or 0) != index
                or str(Path(provided).resolve()) != expected_path
                or file_hash(provided) != slide.get("sha256")):
            raise ReleaseGateError(f"上传前第 {index} 张图片或顺序与验收版本不一致")
    _assert_target_account_consistency(manifest, context, account_id)
