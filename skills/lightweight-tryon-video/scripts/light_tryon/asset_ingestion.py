from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .database import LightTryonDB
from .utils import normalized_list, stable_hash
from .workers import run_json_command, validate_video_decode, structural_qc


ASSET_TAG_TAXONOMY_VERSION = "ltv-asset-tags-v1"
AUTO_MIXCUT_TAGGER_VERSION = "auto-mixcut-segment-tagging-v1"
SOURCE_REFERENCE_TAGGER_VERSION = "auto-mixcut-source-reference-tagging-v1"
VISUAL_FINGERPRINT_VERSION = "video-dhash-4frames-v1"
SUPPLEMENT_CONTRACT_EVIDENCE_TAGS = {
    "detail_closure": ["detail_macro", "closure_detail", "zipper_detail", "snap_detail", "pocket_closeup"],
    "detail_neckline": ["detail_macro", "neckline_detail", "collar_detail"],
    "detail_waistline": ["waist_closeup", "cropped_length", "tryon_front"],
    "detail_fabric": ["detail_macro", "fabric_detail"],
    "detail_sleeve": ["detail_macro", "sleeve_detail", "cuff_detail"],
    "fit_turn": ["tryon_front", "side_view", "full_body", "relaxed_fit"],
    "scenario_pose": ["tryon_front", "full_body", "styled_look", "lifestyle", "camera_pose"],
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _video_visual_fingerprint(path: Path, ffmpeg_bin: str = "") -> list[str]:
    binary = ffmpeg_bin or shutil.which("ffmpeg") or str(Path.home() / ".local" / "bin" / "ffmpeg")
    try:
        completed = subprocess.run(
            [
                binary, "-v", "error", "-i", str(path),
                "-vf", "fps=0.5,scale=9:8:flags=area,format=gray",
                "-frames:v", "4", "-f", "rawvideo", "-",
            ],
            capture_output=True, timeout=90, check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return []
    if completed.returncode != 0:
        return []
    frame_size = 9 * 8
    frames = [
        completed.stdout[index:index + frame_size]
        for index in range(0, len(completed.stdout), frame_size)
        if len(completed.stdout[index:index + frame_size]) == frame_size
    ]
    hashes: list[str] = []
    for frame in frames:
        bits = 0
        for row in range(8):
            start = row * 9
            for column in range(8):
                bits = (bits << 1) | int(frame[start + column] > frame[start + column + 1])
        hashes.append(f"{bits:016x}")
    return hashes


def _fingerprint_distance(left: list[str], right: list[str]) -> float:
    if not left or not right:
        return 64.0
    pairs = zip(left[: min(len(left), len(right))], right[: min(len(left), len(right))])
    distances = [bin(int(a, 16) ^ int(b, 16)).count("1") for a, b in pairs]
    return sum(distances) / max(1, len(distances))


def _duplicate_fingerprint_metadata(
    db: LightTryonDB,
    product_id: str,
    fingerprint: list[str],
    *,
    exclude_asset_id: str = "",
) -> dict[str, Any]:
    nearest: tuple[float, dict[str, Any]] | None = None
    for asset in db.list_media_assets(product_id):
        if str(asset.get("asset_id") or "") == exclude_asset_id:
            continue
        qc = asset.get("qc_result") if isinstance(asset.get("qc_result"), dict) else {}
        other = normalized_list(qc.get("visual_fingerprint"))
        if not other:
            continue
        distance = _fingerprint_distance(fingerprint, other)
        if nearest is None or distance < nearest[0]:
            nearest = (distance, asset)
    if nearest is None or nearest[0] > 6.0:
        return {"duplicate_group_id": "", "near_duplicate_of_asset_id": "", "visual_distance": None}
    other = nearest[1]
    other_qc = other.get("qc_result") if isinstance(other.get("qc_result"), dict) else {}
    return {
        "duplicate_group_id": str(other_qc.get("duplicate_group_id") or other.get("asset_id") or ""),
        "near_duplicate_of_asset_id": str(other.get("asset_id") or ""),
        "visual_distance": round(nearest[0], 3),
    }


def register_media_asset(
    db: LightTryonDB,
    product_id: str,
    file_path: str | Path,
    *,
    source_job_id: str = "",
    source_type: str = "ai_generated",
    expected_tags: dict[str, Any] | None = None,
    taxonomy_version: str = ASSET_TAG_TAXONOMY_VERSION,
) -> tuple[dict[str, Any], bool]:
    path = Path(file_path).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"素材文件不存在: {path}")
    file_hash = _sha256(path)
    existing = db.find_media_asset_by_hash(product_id, file_hash, taxonomy_version=taxonomy_version)
    if existing:
        return existing, False
    asset_id = "LTA_" + stable_hash(product_id, file_hash, taxonomy_version, length=20)
    fingerprint = _video_visual_fingerprint(path)
    duplicate_meta = _duplicate_fingerprint_metadata(db, product_id, fingerprint) if fingerprint else {}
    row = db.upsert_media_asset({
        "asset_id": asset_id,
        "product_id": product_id,
        "source_job_id": source_job_id,
        "source_type": source_type,
        "file_path": str(path),
        "file_sha256": file_hash,
        "file_size": path.stat().st_size,
        "expected_tags": expected_tags or {},
        "observed_tags": {},
        "qc_result": {
            "visual_fingerprint_version": VISUAL_FINGERPRINT_VERSION,
            "visual_fingerprint": fingerprint,
            **duplicate_meta,
        },
        "tag_status": "queued",
        "asset_status": "received",
        "tag_taxonomy_version": taxonomy_version,
    })
    return row, True


def backfill_asset_visual_fingerprints(
    db: LightTryonDB,
    *,
    product_id: str,
    ffmpeg_bin: str = "",
) -> dict[str, Any]:
    result: dict[str, Any] = {"processed": 0, "fingerprinted": 0, "near_duplicates": 0, "failed": 0, "items": []}
    for asset in db.list_media_assets(product_id):
        path = Path(str(asset.get("file_path") or "")).expanduser()
        if not path.is_file():
            continue
        result["processed"] += 1
        fingerprint = _video_visual_fingerprint(path, ffmpeg_bin=ffmpeg_bin)
        if not fingerprint:
            result["failed"] += 1
            result["items"].append({"asset_id": asset["asset_id"], "status": "failed"})
            continue
        duplicate_meta = _duplicate_fingerprint_metadata(
            db, product_id, fingerprint, exclude_asset_id=str(asset.get("asset_id") or ""),
        )
        qc = asset.get("qc_result") if isinstance(asset.get("qc_result"), dict) else {}
        updated = db.upsert_media_asset({
            **asset,
            "qc_result": {
                **qc,
                "visual_fingerprint_version": VISUAL_FINGERPRINT_VERSION,
                "visual_fingerprint": fingerprint,
                **duplicate_meta,
            },
        })
        result["fingerprinted"] += 1
        if duplicate_meta.get("near_duplicate_of_asset_id"):
            result["near_duplicates"] += 1
        result["items"].append({
            "asset_id": updated["asset_id"],
            "status": "near_duplicate" if duplicate_meta.get("near_duplicate_of_asset_id") else "unique",
            **duplicate_meta,
        })
    return result


def backfill_generated_job_assets(
    db: LightTryonDB,
    *,
    product_id: str | None = None,
    job_ids: list[str] | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Register existing clean initial videos without re-downloading or rendering."""

    requested = {str(value) for value in (job_ids or []) if str(value)}
    jobs = db.list_jobs(product_id=product_id)
    if requested:
        jobs = [job for job in jobs if str(job.get("job_id") or "") in requested]
    result: dict[str, Any] = {"processed": 0, "created": 0, "existing": 0, "skipped": 0, "items": []}
    for job in jobs[: int(limit) if limit else None]:
        raw_path = Path(str(job.get("raw_video_path") or "")).expanduser()
        output_path = Path(str(job.get("output_video_path") or "")).expanduser()
        path = raw_path if raw_path.is_file() else output_path
        if not path.is_file():
            result["skipped"] += 1
            result["items"].append({"job_id": job.get("job_id"), "status": "skipped", "reason": "video_file_missing"})
            continue
        expected = _job_expected_tags(job)
        asset, created = register_media_asset(
            db,
            str(job.get("product_id") or ""),
            path,
            source_job_id=str(job.get("job_id") or ""),
            source_type="light_tryon_initial_video",
            expected_tags=expected,
        )
        result["processed"] += 1
        result["created" if created else "existing"] += 1
        result["items"].append({
            "job_id": job.get("job_id"),
            "asset_id": asset.get("asset_id"),
            "status": "created" if created else "existing",
            "file_path": str(path.resolve()),
        })
    return result


def _job_expected_tags(job: dict[str, Any]) -> dict[str, Any]:
    profile = str(job.get("shot_profile_id") or "").lower()
    if "detail" in profile or "close" in profile:
        roles = ["detail", "detail_sleeve", "detail_neckline"]
    elif "upper" in profile or "medium" in profile:
        roles = ["hero", "result", "main_wear_upper", "fit_turn"]
    else:
        roles = ["hero", "result", "main_wear_upper"]
    return {
        "shot_roles": roles,
        "scene_id": job.get("scene_id"),
        "action_id": job.get("action_id"),
        "styling_id": job.get("styling_id"),
        "shot_plan_id": job.get("shot_plan_id"),
        "shot_profile_id": job.get("shot_profile_id"),
        "expected_only": True,
    }


def process_pending_asset_tags(
    db: LightTryonDB,
    *,
    product_id: str,
    limit: int = 10,
    tag_command: str = "",
    auto_mixcut_root: str | Path | None = None,
    auto_mixcut_config: str | None = None,
    anchor_mode: str = "source_reference",
    source_job_ids: Iterable[str] | None = None,
) -> dict[str, Any]:
    if anchor_mode not in {"source_reference", "confirmed_anchor"}:
        raise ValueError(f"不支持的素材打标商品依据模式: {anchor_mode}")
    product = db.get_product(product_id) or {}
    reference_images = [
        str(Path(value).expanduser().resolve())
        for value in (product.get("product_images") or [])
        if Path(str(value)).expanduser().is_file()
    ]
    if not tag_command and anchor_mode == "source_reference" and not reference_images:
        raise ValueError("source_reference 模式要求原始脚本商品参考图已缓存到本地")
    requested_source_ids = {str(item).strip() for item in (source_job_ids or []) if str(item).strip()}
    all_assets = db.list_media_assets(product_id)
    recovered_stale = 0
    for row in all_assets:
        if row.get("tag_status") != "tagging" or not _tagging_lease_expired(row):
            continue
        db.upsert_media_asset({
            **row,
            "tag_status": "retrying",
            "asset_status": "received",
            "last_error": "STALE_TAGGING_LEASE_RECOVERED",
        })
        recovered_stale += 1
    assets = [
        row for row in db.list_media_assets(product_id)
        if row.get("tag_status") in {"queued", "retrying"}
        or (
            row.get("tag_status") == "blocked"
            and str(row.get("last_error") or "") == "AUTO_MIXCUT_ANCHOR_REQUIRED"
        )
        or (
            row.get("tag_status") == "completed"
            and row.get("asset_status") == "ready"
            and not _has_valid_observed_segment(row)
        )
    ]
    if requested_source_ids:
        assets = [row for row in assets if str(row.get("source_job_id") or "") in requested_source_ids]
    selected = assets[: max(0, int(limit))]
    result: dict[str, Any] = {
        "processed": 0, "ready": 0, "manual_review": 0, "blocked": 0, "failed": 0,
        "recovered_stale": recovered_stale, "circuit_open": False, "deferred": 0, "items": [],
    }
    provider_failures = 0
    failure_limit = _tag_circuit_failure_limit()
    for index, asset in enumerate(selected):
        try:
            db.upsert_media_asset({**asset, "tag_status": "tagging", "last_error": ""})
            tagged = (
                _tag_with_command(tag_command, asset)
                if tag_command
                else _tag_with_auto_mixcut(
                    asset,
                    auto_mixcut_root=auto_mixcut_root,
                    config_path=auto_mixcut_config,
                    anchor_mode=anchor_mode,
                    product_context=product,
                    reference_images=reference_images,
                )
            )
            status, semantic_qc = _supplement_semantic_contract(asset, tagged)
            if status not in {"ready", "manual_review", "blocked"}:
                status = "manual_review"
            db.upsert_media_asset({
                **asset,
                "observed_tags": tagged.get("observed_tags") or {},
                "qc_result": {
                    **(asset.get("qc_result") if isinstance(asset.get("qc_result"), dict) else {}),
                    **semantic_qc,
                },
                "tag_status": "completed" if status in {"ready", "manual_review"} else "blocked",
                "asset_status": status,
                "auto_mixcut_asset_id": tagged.get("auto_mixcut_asset_id") or "",
                "auto_mixcut_segment_ids": tagged.get("auto_mixcut_segment_ids") or [],
                "tagger_version": tagged.get("tagger_version") or AUTO_MIXCUT_TAGGER_VERSION,
                "last_error": tagged.get("error") or "",
            })
            result[status] += 1
            result["processed"] += 1
            result["items"].append({"asset_id": asset["asset_id"], "status": status})
            provider_failures = 0
        except Exception as exc:
            error = str(exc)[:2000]
            db.upsert_media_asset({**asset, "tag_status": "retrying", "asset_status": "received", "last_error": error})
            result["failed"] += 1
            result["processed"] += 1
            result["items"].append({"asset_id": asset["asset_id"], "status": "failed", "error": str(exc)})
            provider_failures = provider_failures + 1 if _is_visual_provider_failure(error) else 0
            if provider_failures < failure_limit:
                continue
            deferred = selected[index + 1:]
            circuit_error = f"VISUAL_TAGGING_CIRCUIT_OPEN: {error[:500]}"
            for pending in deferred:
                db.upsert_media_asset({
                    **pending,
                    "tag_status": "retrying",
                    "asset_status": "received",
                    "last_error": circuit_error,
                })
                result["items"].append({
                    "asset_id": pending["asset_id"], "status": "deferred", "error": circuit_error,
                })
            result["circuit_open"] = True
            result["circuit_reason"] = error[:500]
            result["deferred"] = len(deferred)
            break
    return result


def _tagging_lease_expired(asset: dict[str, Any]) -> bool:
    raw = str(asset.get("updated_at") or "").strip()
    if not raw:
        return True
    try:
        updated = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
        age_seconds = (datetime.now(timezone.utc) - updated.astimezone(timezone.utc)).total_seconds()
    except ValueError:
        return True
    try:
        lease_seconds = max(60, int(os.environ.get("LIGHT_TRYON_TAG_LEASE_SECONDS", "1800") or "1800"))
    except ValueError:
        lease_seconds = 1800
    return age_seconds >= lease_seconds


def _tag_circuit_failure_limit() -> int:
    try:
        return max(1, int(os.environ.get("LIGHT_TRYON_TAG_CIRCUIT_FAILURES", "2") or "2"))
    except ValueError:
        return 2


def _is_visual_provider_failure(error: str) -> bool:
    normalized = error.lower()
    return any(marker in normalized for marker in (
        "llm_call_exhausted", "all retries and escalations exhausted", "timeout", "timed out",
        "connectionerror", "connecttimeout", "readtimeout", "rate_limit", "429", "503", "504",
    ))


def _supplement_semantic_contract(
    asset: dict[str, Any],
    tagged: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    """Route an off-target generated supplement to review after real visual tagging.

    The smart-mixcut tagger remains the source of observed facts.  This adapter
    only compares those facts with the lightweight-video supplement contract.
    """

    status = str(tagged.get("status") or "manual_review")
    qc_result = dict(tagged.get("qc_result") or {})
    source_job_id = str(asset.get("source_job_id") or "").strip()
    expected = asset.get("expected_tags") if isinstance(asset.get("expected_tags"), dict) else {}
    expected_roles = normalized_list(expected.get("shot_roles"))
    if not source_job_id.startswith("SUP_") or not expected_roles:
        return status, qc_result

    # Imported lazily to keep the ingestion module independent at import time.
    from .supplement_shots import asset_shot_roles

    observed_tags = tagged.get("observed_tags") if isinstance(tagged.get("observed_tags"), dict) else {}
    observed_roles = sorted(asset_shot_roles({"observed_tags": observed_tags}))
    matched_roles = sorted(set(expected_roles).intersection(observed_roles))
    matched = bool(matched_roles)
    contract = {
        "policy_version": "supplement-semantic-contract-v1",
        "expected_roles": expected_roles,
        "observed_roles": observed_roles,
        "matched_roles": matched_roles,
        "matched": matched,
    }
    qc_result["supplement_semantic_contract"] = contract
    if status == "ready" and not matched:
        status = "manual_review"
        qc_result["decision"] = "manual_review"
        reasons = normalized_list(qc_result.get("manual_review_reasons"))
        if "supplement_target_role_not_observed" not in reasons:
            reasons.append("supplement_target_role_not_observed")
        qc_result["manual_review_reasons"] = reasons
    return status, qc_result


def tag_supplement_assets_by_contract(
    db: LightTryonDB,
    *,
    product_id: str,
    source_job_ids: Iterable[str],
    ffmpeg_bin: str = "ffmpeg",
    ffprobe_bin: str = "ffprobe",
) -> dict[str, Any]:
    """Use a generated supplement's explicit role contract after structural QC.

    This is a narrow fallback for first-party generated shots only. It never
    claims visual AI recognition; the provenance remains visible in qc_result.
    """
    requested = {str(item).strip() for item in source_job_ids if str(item).strip()}
    if not requested:
        raise ValueError("至少需要一个补充镜头来源任务 ID")
    assets = [
        row for row in db.list_media_assets(product_id)
        if str(row.get("source_job_id") or "") in requested
    ]
    result: dict[str, Any] = {"processed": 0, "ready": 0, "failed": 0, "items": []}
    for asset in assets:
        expected = asset.get("expected_tags") if isinstance(asset.get("expected_tags"), dict) else {}
        roles = normalized_list(expected.get("shot_roles"))
        if not roles:
            result["failed"] += 1
            result["items"].append({"asset_id": asset["asset_id"], "status": "failed", "error": "missing_expected_role"})
            continue
        try:
            probe = validate_video_decode(
                asset["file_path"], ffmpeg_bin=ffmpeg_bin, ffprobe_bin=ffprobe_bin, expected_duration=8.0,
            )
            structural = structural_qc(probe)
            if not structural["passed"]:
                raise ValueError("结构质检失败: " + ",".join(structural["failures"]))
            source_role = roles[0]
            primary = "detail" if source_role.startswith("detail_") else ("scene" if source_role == "scenario_pose" else "result")
            secondary = list(dict.fromkeys(
                ([source_role, "main_wear_upper", "result"] if primary != "detail" else [source_role])
                + SUPPLEMENT_CONTRACT_EVIDENCE_TAGS.get(source_role, [])
            ))
            duration_ms = int(round(float(probe["duration_seconds"]) * 1000))
            observed = {
                "shot_roles": [primary, *secondary],
                "segments": [{
                    "segment_id": f"CONTRACT_{asset['asset_id']}",
                    "start_ms": 0,
                    "end_ms": duration_ms,
                    "primary_shot_role": primary,
                    "secondary_roles": secondary,
                    "product_visibility": "high",
                    "hook_strength": "medium" if primary != "scene" else "strong",
                    "hook_visual_type": "try_on",
                    "mixcut_usability": "yes",
                    "risk_level": "medium",
                    "confidence": "medium",
                    "needs_human_review": True,
                    "reason": f"按生成任务 {source_role} 的镜头契约入库；已通过时长、竖屏比例与完整解码质检，待成片阶段复核画面内容。",
                }],
            }
            qc = {
                **(asset.get("qc_result") if isinstance(asset.get("qc_result"), dict) else {}),
                "decision": "ready_with_source_contract",
                "tagging_method": "supplement_source_contract_fallback",
                "structural_qc": structural,
                "expected_tags": expected,
                "requires_final_visual_qc": True,
            }
            db.upsert_media_asset({
                **asset, "observed_tags": observed, "qc_result": qc, "tag_status": "completed",
                "asset_status": "ready", "tagger_version": "supplement-source-contract-v1", "last_error": "",
            })
            result["processed"] += 1
            result["ready"] += 1
            result["items"].append({"asset_id": asset["asset_id"], "status": "ready", "source_role": source_role})
        except Exception as exc:
            db.upsert_media_asset({**asset, "tag_status": "retrying", "asset_status": "received", "last_error": str(exc)[:2000]})
            result["processed"] += 1
            result["failed"] += 1
            result["items"].append({"asset_id": asset["asset_id"], "status": "failed", "error": str(exc)})
    return result


def _tag_with_command(command: str, asset: dict[str, Any]) -> dict[str, Any]:
    response = run_json_command(command, {
        "protocol_version": "1.0",
        "asset": asset,
        "expected_tags": asset.get("expected_tags") or {},
        "required_outputs": ["observed_tags", "qc_result", "status"],
    }, timeout=900)
    if not isinstance(response.get("observed_tags"), dict):
        raise ValueError("素材打标 worker 必须返回 observed_tags 对象")
    return response


def _tag_with_auto_mixcut(
    asset: dict[str, Any], *, auto_mixcut_root: str | Path | None = None, config_path: str | None = None,
    anchor_mode: str = "confirmed_anchor", product_context: dict[str, Any] | None = None,
    reference_images: list[str] | None = None,
) -> dict[str, Any]:
    root = Path(auto_mixcut_root or Path(__file__).resolve().parents[4] / "auto_mixcut").expanduser().resolve()
    if not (root / "auto_mixcut").is_dir():
        raise RuntimeError(f"找不到智能混剪项目: {root}")
    # build_context reads .env relative to AUTO_MIXCUT_ROOT. Without this, the
    # lightweight-video process silently opens its own empty SQLite database.
    os.environ.setdefault("AUTO_MIXCUT_ROOT", str(root))
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    from auto_mixcut.core.bootstrap import build_context
    from auto_mixcut.skills.ai_tagging_skill import AITaggingSkill
    from auto_mixcut.skills.frame_sample_skill import FrameSampleSkill
    from auto_mixcut.skills.media_probe_skill import MediaProbeSkill
    from auto_mixcut.skills.oss_storage_skill import OSSStorageSkill
    from auto_mixcut.skills.segment_skill import SegmentSkill
    from auto_mixcut.skills.watermark_detect_skill import WatermarkDetectSkill

    ctx = build_context(config_path)
    product_id = str(asset["product_id"])
    product = ctx.repo.get("products", "product_id", product_id) or {}
    refs = [str(Path(value).expanduser().resolve()) for value in (reference_images or []) if Path(value).is_file()]
    if anchor_mode == "source_reference" and not refs:
        raise ValueError("source_reference 模式缺少有效商品参考图")
    if anchor_mode == "source_reference" and not product:
        source_product = product_context or {}
        created = ctx.repo.upsert("products", "product_id", {
            "product_id": product_id,
            "product_name": source_product.get("product_title") or source_product.get("product_name") or product_id,
            "market": source_product.get("market") or "TH",
            "category": source_product.get("category") or "uncategorized",
            "priority": source_product.get("generation_priority") or "medium",
            "anchor_status": "pending",
            "anchor_version": "",
            "product_status": "source_reference_only",
        })
        if not created.success:
            raise RuntimeError(created.error.message if created.error else "写入 source_reference 商品上下文失败")
        product = ctx.repo.get("products", "product_id", product_id) or {}
    if anchor_mode == "confirmed_anchor" and product.get("anchor_status") != "confirmed":
        return {
            "status": "blocked",
            "error": "AUTO_MIXCUT_ANCHOR_REQUIRED",
            "observed_tags": {},
            "qc_result": {"decision": "blocked", "reason": "智能混剪商品锚点卡尚未确认"},
            "tagger_version": AUTO_MIXCUT_TAGGER_VERSION,
        }
    identity = f"light_tryon:{asset['asset_id']}"
    existing = ctx.repo.list_where("assets", "product_id=? AND source_identity=? ORDER BY id DESC LIMIT 1", (product_id, identity))
    if existing:
        mix_asset_id = existing[0]["asset_id"]
    else:
        uploaded = OSSStorageSkill(ctx).upload_asset(
            product_id,
            str(asset["file_path"]),
            source_type="ai_generated",
            source_trust_level="medium",
            product_binding_type="exact_sku",
            allow_source_reference=anchor_mode == "source_reference",
            source_reference_count=len(refs),
        )
        if not uploaded.success:
            message = uploaded.error.message if uploaded.error else "智能混剪素材上传失败"
            raise RuntimeError(message)
        mix_asset_id = uploaded.data["asset_id"]
        ctx.repo.update("assets", "asset_id", mix_asset_id, {
            "source_identity": identity,
            "generation_job_id": str(asset.get("source_job_id") or ""),
            "generation_type": "light_tryon_supplement",
            "generation_model": "external_video_model",
            "generation_prompt": str(asset.get("expected_tags") or {}),
        })

    # This bridge is invoked once per lightweight-video asset. Keep every
    # preprocessing and tagging operation scoped to that asset; product-wide
    # calls can repeatedly process unrelated auto_mixcut assets and make a
    # small supplement backfill grow into an unbounded batch.
    steps = []
    for name, call in (
        ("probe", lambda: MediaProbeSkill(ctx).probe_asset(mix_asset_id)),
        ("watermark", lambda: WatermarkDetectSkill(ctx).check_asset(mix_asset_id)),
        ("segment", lambda: SegmentSkill(ctx).segment_asset(mix_asset_id)),
    ):
        response = call()
        steps.append({"step": name, **response.to_dict()})
        if not response.success:
            raise RuntimeError(response.error.message if response.error else f"{name} 失败")

    segments = ctx.repo.list_where("segments", "asset_id=?", (mix_asset_id,))
    sampler = FrameSampleSkill(ctx)
    for segment in segments:
        sampled = sampler.sample_segment(segment["segment_id"])
        steps.append({"step": "frames", "segment_id": segment["segment_id"], **sampled.to_dict()})
        if not sampled.success:
            raise RuntimeError(sampled.error.message if sampled.error else "抽帧失败")
    tagger = AITaggingSkill(ctx)
    for index, segment in enumerate(segments):
        existing_tags = ctx.repo.list_where(
            "segment_tags", "segment_id=? ORDER BY id DESC LIMIT 1", (segment["segment_id"],)
        )
        if existing_tags:
            steps.append({"step": "tag", "segment_id": segment["segment_id"], "status": "skipped"})
            continue
        frame_count = len(ctx.repo.list_where("segment_frames", "segment_id=?", (segment["segment_id"],)))
        tagged = tagger._poll_segment(
            product_id,
            segment,
            index,
            "v1.0",
            False,
            {},
            frame_count,
            refs if anchor_mode == "source_reference" else None,
        )
        steps.append({"step": "tag", "segment_id": segment["segment_id"], **tagged.to_dict()})
        if not tagged.success or str((tagged.data or {}).get("status") or "") != "completed":
            message = tagged.error.message if tagged.error else str((tagged.data or {}).get("error") or "素材打标失败")
            raise RuntimeError(message)
    segment_ids = [str(row["segment_id"]) for row in segments]
    observed_segments: list[dict[str, Any]] = []
    for segment in segments:
        tag_rows = ctx.repo.list_where(
            "segment_tags", "segment_id=? ORDER BY id DESC LIMIT 1", (segment["segment_id"],)
        )
        tag = tag_rows[0] if tag_rows else {}
        observed_segments.append({
            "segment_id": segment["segment_id"],
            "start_ms": segment.get("start_ms"),
            "end_ms": segment.get("end_ms"),
            "primary_shot_role": tag.get("primary_shot_role"),
            "secondary_roles": tag.get("secondary_roles_json") or [],
            "product_visibility": tag.get("product_visibility"),
            "hook_strength": tag.get("hook_strength"),
            "hook_visual_type": tag.get("hook_visual_type"),
            "mixcut_usability": tag.get("mixcut_usability"),
            "risk_level": tag.get("risk_level"),
            "confidence": tag.get("confidence"),
            "needs_human_review": bool(tag.get("needs_human_review")),
            "reason": tag.get("reason") or "",
            "text_overlay_risk": tag.get("text_overlay_risk") or "",
            "text_language": tag.get("text_language") or "",
        })
    missing_segment_tags = [row["segment_id"] for row in observed_segments if not row.get("primary_shot_role")]
    if missing_segment_tags:
        raise RuntimeError(f"当前素材有未完成视觉打标的片段: {len(missing_segment_tags)}")
    usable = [
        row for row in observed_segments
        if row.get("primary_shot_role") in {"hero", "detail", "result", "scene", "ending"}
        and row.get("mixcut_usability") in {"yes", "needs_processing"}
    ]
    if not usable:
        raise RuntimeError("素材视觉打标没有返回任何可用的实际角色标签")
    needs_review = any(row.get("needs_human_review") or row.get("risk_level") in {"medium", "high"} for row in observed_segments)
    status = "ready" if usable and not needs_review else "manual_review"
    roles = sorted({
        str(role)
        for row in usable
        for role in [row.get("primary_shot_role"), *(row.get("secondary_roles") or [])]
        if str(role or "").strip()
    })
    return {
        "status": status,
        "auto_mixcut_asset_id": mix_asset_id,
        "auto_mixcut_segment_ids": segment_ids,
        "observed_tags": {"shot_roles": roles, "segments": observed_segments},
        "qc_result": {
            "decision": status,
            "usable_segment_count": len(usable),
            "segment_count": len(observed_segments),
            "expected_tags": asset.get("expected_tags") or {},
            "steps": steps,
        },
        "tagger_version": SOURCE_REFERENCE_TAGGER_VERSION if anchor_mode == "source_reference" else AUTO_MIXCUT_TAGGER_VERSION,
    }


def _has_valid_observed_segment(asset: dict[str, Any]) -> bool:
    observed = asset.get("observed_tags") if isinstance(asset.get("observed_tags"), dict) else {}
    return any(
        isinstance(row, dict)
        and row.get("primary_shot_role") in {"hero", "detail", "result", "scene", "ending"}
        and row.get("mixcut_usability") in {"yes", "needs_processing"}
        for row in (observed.get("segments") or [])
    )
