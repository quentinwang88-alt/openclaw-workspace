#!/usr/bin/env python3
"""Generate user-selected first-frame assets from the production script table."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse


SKILL_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = SKILL_ROOT.parents[1]
OPENAI_IMAGE_RUNNER = WORKSPACE_ROOT / "skills" / "openai-image" / "run_pipeline.py"
DEFAULT_OUTPUT_ROOT = Path.home() / ".openclaw" / "shared" / "data" / "original_first_frames"
DEFAULT_IMAGE_TIMEOUT_SECONDS = 480
DEFAULT_STALE_AFTER_SECONDS = 900
DEFAULT_RUN_MANAGER_URL = "https://gcngopvfvo0q.feishu.cn/base/Bbi4bD4Hxa9cWms2GO2cDZ9wnBc?table=tbljUInlUld4MnOw"
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.bitable import extract_attachments  # noqa: E402
from core.first_frame_contract import (  # noqa: E402
    build_first_frame_contract,
    render_first_frame_prompt,
)
from core.first_frame_storage import FirstFrameStorage  # noqa: E402
from core.wsr_first_frame import build_wsr_first_frame_contract, render_wsr_first_frame_prompt  # noqa: E402
from core.production_script_feishu import PRODUCTION_SCRIPT_FIELD_NAMES  # noqa: E402
from scripts.run_feishu_operation_tasks import (  # noqa: E402
    DEFAULT_SCRIPT_URL,
    _client,
)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _checked(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return _text(value).lower() in {"1", "true", "yes", "checked", "已勾选", "勾选"}


def _load_stage0_script(script_id: str) -> Optional[Dict[str, Any]]:
    """Resolve an explicitly exported Stage0 script from its run artifact.

    Stage0 review rows are intentionally not persisted as formal
    ``original_content_item`` records.  Their public IDs encode the immutable
    Stage0 run and output slot, so the first-frame workbench can safely load
    the already-generated script without re-planning it.
    """

    public_id = _text(script_id)
    match = re.fullmatch(r"SCSCRIPT_STAGE0_(\d+)_(S\d+)", public_id, re.IGNORECASE)
    if not match and not re.fullmatch(r"SCSCRIPT_[0-9A-F]{24}", public_id, re.IGNORECASE):
        return None
    stage0_run_id = match.group(1) if match else ""
    output_slot = match.group(2).upper() if match else ""
    run_root = Path.home() / ".openclaw" / "shared" / "data" / "original_production_runs"
    candidates = sorted(
        run_root.glob("*/stage0_result.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for path in candidates:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, TypeError, json.JSONDecodeError):
            continue
        for product in payload.get("products", []) if isinstance(payload, dict) else []:
            if not isinstance(product, Mapping):
                continue
            if match and _text(product.get("stage0_run_id")) != stage0_run_id:
                continue
            for direction in product.get("directions", []) or []:
                if not isinstance(direction, Mapping):
                    continue
                candidate_slot = _text(direction.get("output_slot")).upper()
                if match:
                    if candidate_slot != output_slot:
                        continue
                else:
                    candidate_script = direction.get("script")
                    candidate_provenance = (
                        candidate_script.get("reality_reference_provenance", {})
                        if isinstance(candidate_script, Mapping)
                        else {}
                    )
                    blueprint_id = _text(
                        candidate_provenance.get("creative_blueprint_id")
                    )
                    batch_id = "STAGE0_" + hashlib.sha256(
                        str(path.resolve()).encode("utf-8")
                    ).hexdigest()[:16].upper()
                    expected_id = "SCSCRIPT_" + hashlib.sha256(
                        json.dumps(
                            {
                                "batch_id": batch_id,
                                "slot": candidate_slot,
                                "blueprint_id": blueprint_id,
                            },
                            ensure_ascii=False,
                            sort_keys=True,
                            default=str,
                        ).encode("utf-8")
                    ).hexdigest()[:24].upper()
                    if expected_id.upper() != public_id.upper():
                        continue
                script = direction.get("script")
                if isinstance(script, dict) and script:
                    return dict(script)
    return None


def _load_script(script_id: str, db_path: Optional[str] = None) -> Dict[str, Any]:
    if _text(script_id).startswith("wsr_"):
        raise ValueError("WSR脚本必须从总库最终提示词读取，禁止查找原创stage0")
    storage = FirstFrameStorage(db_path)
    with sqlite3.connect(str(storage.db_path), timeout=30) as conn:
        row = conn.execute(
            "SELECT result_json FROM original_content_item WHERE script_id=? AND status='SCRIPT_READY' "
            "ORDER BY updated_at DESC LIMIT 1",
            (script_id,),
        ).fetchone()
        if not row:
            # The production workbench exposes ``complete_script_id`` while
            # the batch table's primary ``script_id`` is an internal ID.  New
            # first-frame jobs are initiated from the workbench, so resolve
            # that public ID from the persisted result when necessary.
            candidates = conn.execute(
                "SELECT result_json FROM original_content_item "
                "WHERE status='SCRIPT_READY' AND result_json IS NOT NULL "
                "ORDER BY updated_at DESC"
            ).fetchall()
            for candidate in candidates:
                try:
                    result = json.loads(candidate[0] or "{}")
                except (TypeError, json.JSONDecodeError):
                    continue
                script = result.get("script") if isinstance(result, dict) else None
                if isinstance(script, dict) and _text(script.get("complete_script_id")) == script_id:
                    row = candidate
                    break
    if not row or not row[0]:
        stage0_script = _load_stage0_script(script_id)
        if stage0_script:
            return stage0_script
        raise ValueError(f"找不到 SCRIPT_READY 的本地脚本结果: {script_id}")
    result = json.loads(row[0])
    script = result.get("script") if isinstance(result, dict) else None
    if not isinstance(script, dict):
        raise ValueError(f"本地脚本结果缺少 script: {script_id}")
    return script


def _download_references(
    client: Any, assets: Iterable[Mapping[str, Any]], output_dir: Path,
    *, cache_dir: Optional[Path] = None,
) -> list[str]:
    paths: list[str] = []
    for index, asset in enumerate(assets, start=1):
        if not isinstance(asset, Mapping):
            continue
        local_text = _text(
            asset.get("local_path") or asset.get("cached_path") or asset.get("path")
        )
        local_path = Path(local_text).expanduser().resolve() if local_text else None
        if local_path and local_path.is_file():
            content = local_path.read_bytes()
            suffix = local_path.suffix or ".jpg"
            selected = local_path
            if cache_dir is not None:
                digest = hashlib.sha256(content).hexdigest()
                cache_dir.mkdir(parents=True, exist_ok=True)
                cached = cache_dir / f"ref_{index:02d}_{digest[:10]}{suffix}"
                if not cached.is_file():
                    cached.write_bytes(content)
                selected = cached
            paths.append(str(selected))
            continue
        if not _text(asset.get("file_token")):
            continue
        content, name, content_type, _size = client.download_attachment_bytes(dict(asset))
        suffix = Path(name).suffix or {
            "image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp",
        }.get(content_type, ".jpg")
        path = output_dir / f"ref_{index:02d}{suffix}"
        path.write_bytes(content)
        selected = path
        if cache_dir is not None:
            digest = hashlib.sha256(content).hexdigest()
            cache_dir.mkdir(parents=True, exist_ok=True)
            cached = cache_dir / f"ref_{index:02d}_{digest[:10]}{suffix}"
            if not cached.is_file():
                cached.write_bytes(content)
            selected = cached
        paths.append(str(selected))
    return paths


def _generate_image(
    *,
    prompt: str,
    reference_paths: list[str],
    output_dir: Path,
    asset_id: str,
    timeout_seconds: int = DEFAULT_IMAGE_TIMEOUT_SECONDS,
    reference_roles: Optional[list[str]] = None,
) -> Path:
    if not OPENAI_IMAGE_RUNNER.exists():
        raise FileNotFoundError(f"图像生成入口不存在: {OPENAI_IMAGE_RUNNER}")
    request = {
        "task_id": asset_id,
        "task_type": "original_first_frame",
        "target_field": "统一首帧（系统）",
        "mode": "edit" if reference_paths else "generate",
        "prompt": prompt,
        "input_image_path": reference_paths[0] if reference_paths else "",
        "input_image_paths": reference_paths,
        "size": "1024x1536",
        "quality": "high",
        "output_format": "png",
        "output_dir": str(output_dir),
        "n": 1,
        "metadata": {"reference_order": reference_roles or ["PRODUCT_IDENTITY", "PERSONA_IDENTITY"]},
    }
    request_path = output_dir / f"{asset_id}_request.json"
    request_path.write_text(json.dumps(request, ensure_ascii=False, indent=2), encoding="utf-8")
    try:
        completed = subprocess.run(
            [sys.executable, str(OPENAI_IMAGE_RUNNER), "--input", str(request_path)],
            cwd=str(OPENAI_IMAGE_RUNNER.parent),
            text=True,
            capture_output=True,
            check=False,
            timeout=max(60, int(timeout_seconds)),
        )
    except subprocess.TimeoutExpired as exc:
        detail = _text(exc.stderr) or _text(exc.stdout)
        suffix = f"；最后输出：{detail[-300:]}" if detail else ""
        raise TimeoutError(
            f"首帧模型调用超过 {max(60, int(timeout_seconds))} 秒，子进程已终止{suffix}"
        ) from exc
    stdout = completed.stdout.strip()
    try:
        result = json.loads(stdout[stdout.find("{"):]) if "{" in stdout else {}
    except json.JSONDecodeError:
        result = {}
    paths = result.get("output_image_paths") if isinstance(result, dict) else None
    if completed.returncode != 0 or not isinstance(paths, list) or not paths:
        error = _text(result.get("error_message")) if isinstance(result, dict) else ""
        raise RuntimeError(error or completed.stderr.strip() or stdout[-1000:] or "首帧模型调用失败")
    path = Path(str(paths[0])).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"首帧输出文件不存在: {path}")
    return path


def _upload_generated(client: Any, path: Path) -> Dict[str, Any]:
    content = path.read_bytes()
    return client.upload_attachment(
        content=content,
        file_name=path.name,
        content_type="image/png",
        size=len(content),
    )


class UnsupportedFirstFrame(ValueError):
    pass


@contextmanager
def _first_frame_lock(identity: str):
    lock_dir = DEFAULT_OUTPUT_ROOT / ".locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    path = lock_dir / (hashlib.sha256(identity.encode()).hexdigest() + ".lock")
    with path.open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("FIRST_FRAME_LOCKED:该记录或素材已有首帧任务执行中") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _frame_ready(values: Mapping[str, Any]) -> bool:
    fields = PRODUCTION_SCRIPT_FIELD_NAMES
    return bool(extract_attachments(values.get(fields["composite_first_frame"]))) and _text(
        values.get(fields["first_frame_status"])
    ).upper() in {"已就绪", "缓存复用", "已确认", "READY", "CONFIRMED"}


def _validate_record(record: Any, *, require_production: bool = False) -> str:
    fields = PRODUCTION_SCRIPT_FIELD_NAMES
    values = record.fields
    if not re.fullmatch(r"rec[A-Za-z0-9]+", str(record.record_id)):
        raise UnsupportedFirstFrame("FIRST_FRAME_RECORD_ID_INVALID")
    if not _checked(values.get(fields["first_frame_requested"])):
        raise UnsupportedFirstFrame("FIRST_FRAME_NOT_SELECTED:未勾选生成首帧")
    script_id = _text(values.get(fields["script_id"]))
    source = _text(values.get("脚本来源"))
    is_wsr = script_id.startswith("wsr_")
    if is_wsr != (source == "成功脚本复刻"):
        raise UnsupportedFirstFrame("FIRST_FRAME_SOURCE_ID_MISMATCH")
    if not is_wsr and source not in {"", "原创脚本", "原创生成"}:
        raise UnsupportedFirstFrame(f"FIRST_FRAME_SOURCE_UNSUPPORTED:{source}")
    if require_production and not _checked(values.get(fields["production_enabled"])):
        raise UnsupportedFirstFrame("FIRST_FRAME_PRODUCTION_NOT_SELECTED:未勾选进入生产")
    try:
        duration = float(values.get(fields["duration_seconds"]) or 15)
    except (ValueError, TypeError) as exc:
        raise UnsupportedFirstFrame("FIRST_FRAME_DURATION_INVALID") from exc
    form = _text(values.get(fields["video_format"])).upper()
    if duration <= 0 or duration > 15 or "LONG" in form or "长视频" in form or values.get(fields["longform_job_id"]):
        raise UnsupportedFirstFrame("FIRST_FRAME_LONGFORM_UNSUPPORTED:长视频由独立执行器处理")
    if not script_id:
        raise UnsupportedFirstFrame("FIRST_FRAME_SCRIPT_ID_MISSING")
    return "WSR_SCRIPT_POOL" if is_wsr else "ORIGINAL"


def _asset_snapshot(storage: FirstFrameStorage, fingerprint: str) -> Dict[str, Any]:
    with sqlite3.connect(str(storage.db_path), timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM original_first_frame_asset WHERE asset_fingerprint=?", (fingerprint,)).fetchone()
    return dict(row) if row else {}


def _wsr_verified_references(client: Any, storage: FirstFrameStorage, record: Any) -> list[str]:
    """Resolve current bytes before cache lookup; old ready flags are not proof.

    A prior token's durable, hash-verified bytes may be reused. New tokens are
    downloaded once, allowing identical images re-uploaded under new tokens to
    share the same content fingerprint without trusting names or sizes.
    """
    contract = build_wsr_first_frame_contract(record_id=record.record_id, fields=record.fields)
    assets = list(contract["product_reference_assets"]) + list(contract["persona_reference_assets"])
    previous = {}
    with sqlite3.connect(str(storage.db_path), timeout=30) as conn:
        row = conn.execute("""SELECT a.contract_json FROM original_first_frame_binding b
            JOIN original_first_frame_asset a USING(asset_fingerprint) WHERE b.script_id=?""",
            (contract["script_id"],)).fetchone()
    if row:
        try:
            old = json.loads(row[0] or "{}")
            for spec, cached in zip(old.get("ordered_reference_assets", []), old.get("cached_reference_assets", [])):
                token, path = _text(spec.get("file_token")), Path(cached.get("local_path") or "")
                if token and path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest() == cached.get("sha256"):
                    previous[token] = str(path)
        except (ValueError, TypeError, OSError):
            previous = {}
    resolved = {}
    missing = []
    input_dir = DEFAULT_OUTPUT_ROOT / "input_cache" / hashlib.sha256(contract["script_id"].encode()).hexdigest()[:24]
    input_dir.mkdir(parents=True, exist_ok=True)
    for index, asset in enumerate(assets):
        token = _text(asset.get("file_token"))
        # An explicit local input may change in place; do not mask it with an
        # old token cache. _download_references hashes its current bytes.
        has_local = any(asset.get(key) for key in ("local_path", "cached_path", "path"))
        if token in previous and not has_local:
            resolved[index] = previous[token]
        else:
            missing.append((index, asset))
    downloaded = _download_references(client, [asset for _index, asset in missing], input_dir,
                                     cache_dir=input_dir / "references") if missing else []
    if len(downloaded) != len(missing):
        raise ValueError("FIRST_FRAME_REFERENCE_COUNT_MISMATCH:参考图下载不完整，不允许静默丢图")
    resolved.update({index: path for (index, _asset), path in zip(missing, downloaded)})
    paths = [resolved[index] for index in range(len(assets))]
    if len(paths) != len(assets):
        raise ValueError("FIRST_FRAME_REFERENCE_COUNT_MISMATCH:参考图下载不完整，不允许静默丢图")
    return paths


def _wsr_original_hashes(paths: list[str], contract: dict, fields: Mapping[str, Any]) -> list[str]:
    manifest = fields.get("_wsr_reference_manifest")
    known = []
    if manifest:
        package_root = Path(__file__).resolve().parents[3] / "packages" / "wig_success_replication"
        if str(package_root) not in sys.path:
            sys.path.insert(0, str(package_root))
        from wig_success_replication.reference_manifest import validate_reference_manifest
        known = validate_reference_manifest(manifest)["reference_assets"]
    hashes = []
    for path, reference in zip(paths, contract["ordered_reference_assets"]):
        digest = hashlib.sha256(Path(path).read_bytes()).hexdigest()
        role = "product" if reference["role"] == "PRODUCT_IDENTITY" else "person_identity"
        by_token = next((asset for asset in known if asset.get("file_token") == reference["file_token"]), None)
        if by_token and (by_token.get("role") != role or digest not in {by_token.get("original_sha256"), by_token.get("derived_sha256")}):
            raise ValueError("WSR_FIRST_FRAME_MANIFEST_BYTES_CHANGED")
        previous = by_token or next((asset for asset in known if asset.get("role") == role and digest in {
            asset.get("original_sha256"), asset.get("derived_sha256")}), None)
        hashes.append(previous["original_sha256"] if previous else digest)
    return hashes


def _wsr_execution_frozen(script_id: str) -> bool:
    """Do not refresh a frame once its video/publish execution is frozen."""
    path = Path(os.environ.get("SHORT_VIDEO_AUTO_PUBLISH_DB_PATH") or
                str(Path.home() / ".openclaw/shared/data/short_video_auto_publish.sqlite3"))
    if not path.is_file():
        return False
    with sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True, timeout=10) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "video_assets" in tables and conn.execute("""SELECT 1 FROM video_assets WHERE script_id=?
            AND (COALESCE(publish_task_id,'')<>'' OR publish_status IN ('已排期','已发布','提交中','待对账')) LIMIT 1""",
            (script_id,)).fetchone():
            return True
        if "publish_slots" in tables and conn.execute("""SELECT 1 FROM publish_slots WHERE script_id=?
            AND (COALESCE(publish_task_id,'')<>'' OR schedule_status IN ('已排期','已发布','提交中','待对账')) LIMIT 1""",
            (script_id,)).fetchone():
            return True
    return False


def _validate_wsr_target_execution(record: Any) -> None:
    run_id = _text(record.fields.get(PRODUCTION_SCRIPT_FIELD_NAMES["run_task_id"]))
    snapshot = getattr(record, "execution_target_snapshot", None)
    if not run_id and not snapshot:
        return
    script_id = _text(record.fields.get(PRODUCTION_SCRIPT_FIELD_NAMES["script_id"]))
    if snapshot is None:
        if not re.fullmatch(r"rec[A-Za-z0-9]+", run_id):
            raise UnsupportedFirstFrame("FIRST_FRAME_EXECUTION_UNKNOWN:运行任务ID无法核验，保留原首帧")
        try:
            target = _client(DEFAULT_RUN_MANAGER_URL).get_record(run_id)
            snapshot = {"record_id": target.record_id, "fields": target.fields}
        except Exception as exc:
            raise UnsupportedFirstFrame("FIRST_FRAME_EXECUTION_UNKNOWN:运行任务状态暂不可核验，保留原首帧") from exc
    target_fields = snapshot.get("fields") if isinstance(snapshot, dict) else None
    if not isinstance(target_fields, dict) or (run_id and snapshot.get("record_id") != run_id) or _text(target_fields.get("脚本ID")) != script_id:
        raise UnsupportedFirstFrame("FIRST_FRAME_EXECUTION_UNKNOWN:运行任务身份无法核验，保留原首帧")
    status = _text(target_fields.get("任务状态"))
    task_ids = ("MiniMax任务ID", "H3任务ID", "外部任务ID", "发布任务ID", "任务ID")
    if status not in {"待处理", "待开始", "未开始", "失败", "阻塞"} or any(_text(target_fields.get(key)) for key in task_ids):
        raise UnsupportedFirstFrame("FIRST_FRAME_EXECUTION_FROZEN:运行任务已开始或状态未知，不更新首帧")


def _run_record(client: Any, storage: FirstFrameStorage, record: Any, *, source_url: str,
                force: bool = False, require_production: bool = False,
                db_path: Optional[str] = None,
                image_timeout_seconds: int = DEFAULT_IMAGE_TIMEOUT_SECONDS) -> Dict[str, Any]:
    fields = PRODUCTION_SCRIPT_FIELD_NAMES
    source_kind = _validate_record(record, require_production=require_production)
    script_id = _text(record.fields.get(fields["script_id"]))
    if source_kind == "WSR_SCRIPT_POOL" and _wsr_execution_frozen(script_id):
        raise UnsupportedFirstFrame("FIRST_FRAME_EXECUTION_FROZEN:任务已提交或排期，不更新首帧")
    if source_kind == "WSR_SCRIPT_POOL":
        _validate_wsr_target_execution(record)
    with _first_frame_lock(f"record:{source_url}:{record.record_id}"):
        if source_kind != "WSR_SCRIPT_POOL" and _frame_ready(record.fields) and not force:
            return {"status": "ready", "cached": True, "fields": {
                name: record.fields[name] for name in (fields["composite_first_frame"], fields["first_frame_status"], fields["reference_strategy"])
                if name in record.fields}}
        if source_kind == "WSR_SCRIPT_POOL":
            verified_paths = _wsr_verified_references(client, storage, record)
            provisional = build_wsr_first_frame_contract(record_id=record.record_id, fields=record.fields)
            contract = build_wsr_first_frame_contract(record_id=record.record_id, fields=record.fields,
                reference_sha256=_wsr_original_hashes(verified_paths, provisional, record.fields))
            prompt = render_wsr_first_frame_prompt(contract)
        else:
            script = _load_script(script_id, db_path=db_path)
            contract = build_first_frame_contract(
                script_id=script_id, product_code=_text(record.fields.get(fields["product_code"])),
                product_images=extract_attachments(record.fields.get(fields["product_images"])), script=script)
            if contract.get("availability") != "AVAILABLE":
                raise ValueError(f"FIRST_FRAME_CONTRACT_UNAVAILABLE:{contract.get('availability')}")
            prompt = render_first_frame_prompt(contract)
        fingerprint = _text(contract["asset_fingerprint"])
        asset_id = "FFA_" + fingerprint[:20].upper()
        with _first_frame_lock("asset:" + fingerprint):
            previous = _asset_snapshot(storage, fingerprint)
            metadata = {"model": _text(contract.get("image_model")), "prompt_version": _text(contract.get("prompt_version")),
                        "prompt_text": prompt, "contract_json": json.dumps(contract, ensure_ascii=False, sort_keys=True, default=str)}
            if source_kind == "WSR_SCRIPT_POOL":
                product_count = len(contract["product_reference_assets"])
                contract["cached_reference_assets"] = [{"role": "PRODUCT_IDENTITY" if index < product_count else "PERSONA_IDENTITY",
                    "local_path": path, "sha256": hashlib.sha256(Path(path).read_bytes()).hexdigest()}
                    for index, path in enumerate(verified_paths)]
                metadata["contract_json"] = json.dumps(contract, ensure_ascii=False, sort_keys=True, default=str)
            attachment = {}
            if not force:
                try:
                    attachment = json.loads(previous.get("feishu_attachment_json") or "{}")
                except (ValueError, TypeError):
                    attachment = {}
            cached = bool(attachment.get("file_token"))
            local_path = Path(previous["local_path"]) if not force and previous.get("local_path") else None
            if not cached:
                if not local_path or not local_path.is_file():
                    client.update_record_fields(record.record_id, {fields["first_frame_status"]: "生成中"})
                    storage.upsert_asset(fingerprint=fingerprint, asset_id=asset_id, status="GENERATING", **metadata)
                    storage.bind(script_id=script_id, source_record_id=record.record_id,
                                 fingerprint=fingerprint, asset_id=asset_id, status="GENERATING")
                    asset_dir = DEFAULT_OUTPUT_ROOT / fingerprint
                    asset_dir.mkdir(parents=True, exist_ok=True)
                    assets = list(contract.get("product_reference_assets") or []) + list(contract.get("persona_reference_assets") or [])
                    paths = verified_paths if source_kind == "WSR_SCRIPT_POOL" else _download_references(client, assets, asset_dir, cache_dir=asset_dir / "references")
                    if len(paths) != len(assets):
                        raise ValueError("FIRST_FRAME_REFERENCE_COUNT_MISMATCH:参考图下载不完整，不允许静默丢图")
                    if not paths and source_kind != "WSR_SCRIPT_POOL":
                        raise ValueError("没有可下载的商品/人物参考图")
                    product_count = len(contract.get("product_reference_assets") or [])
                    roles = ["PRODUCT_IDENTITY" if index < product_count else "PERSONA_IDENTITY" for index in range(len(paths))]
                    contract["cached_reference_assets"] = [{"role": role, "local_path": path,
                        "sha256": hashlib.sha256(Path(path).read_bytes()).hexdigest()} for role, path in zip(roles, paths)]
                    metadata["contract_json"] = json.dumps(contract, ensure_ascii=False, sort_keys=True, default=str)
                    # Persistent output dir survives upload/writeback failures.
                    local_path = _generate_image(prompt=prompt, reference_paths=paths, output_dir=asset_dir,
                                                 asset_id=asset_id, timeout_seconds=image_timeout_seconds,
                                                 reference_roles=roles)
                    storage.upsert_asset(fingerprint=fingerprint, asset_id=asset_id, status="IMAGE_READY",
                                         local_path=str(local_path), **metadata)
                else:
                    cached = True
                attachment = _upload_generated(client, local_path)
                # Persist the uploaded token before Feishu row writeback.
                storage.upsert_asset(fingerprint=fingerprint, asset_id=asset_id, status="READY",
                                     local_path=str(local_path), feishu_attachment_json=json.dumps(attachment, ensure_ascii=False), **metadata)
            elif source_kind == "WSR_SCRIPT_POOL":
                storage.upsert_asset(fingerprint=fingerprint, asset_id=asset_id, status="READY",
                    local_path=previous.get("local_path") or "", feishu_attachment_json=json.dumps(attachment, ensure_ascii=False), **metadata)
            patch = {fields["composite_first_frame"]: [attachment],
                     fields["first_frame_status"]: "缓存复用" if cached else "已就绪",
                     fields["reference_strategy"]: _text(contract.get("persona_contract", {}).get("reference_strategy")) or "GENERATED_FIRST_FRAME"}
            storage.bind(script_id=script_id, source_record_id=record.record_id,
                         fingerprint=fingerprint, asset_id=asset_id, status="READY")
            client.update_record_fields(record.record_id, patch)
            return {"status": "ready", "cached": cached, "fields": patch, "asset_id": asset_id, "fingerprint": fingerprint}


def run_record_snapshot(request: Mapping[str, Any], *, db_path: Optional[str] = None,
                        image_timeout_seconds: int = DEFAULT_IMAGE_TIMEOUT_SECONDS) -> Dict[str, Any]:
    protocol = "first-frame-record-v1"
    raw = request.get("record") if isinstance(request, Mapping) else None
    result = {"protocol": protocol, "record_id": _text((raw or {}).get("record_id")) if isinstance(raw, Mapping) else "",
              "script_id": _text((raw.get("fields") or {}).get("脚本ID")) if isinstance(raw, Mapping) and isinstance(raw.get("fields"), Mapping) else "",
              "status": "unsupported", "fields": {}}
    client = None
    record = None
    try:
        if request.get("protocol") != protocol or not isinstance(raw, Mapping) or not isinstance(raw.get("fields"), Mapping):
            raise UnsupportedFirstFrame("FIRST_FRAME_SNAPSHOT_INVALID")
        source_url = _text(request.get("source_url"))
        parsed, expected = urlparse(source_url), urlparse(DEFAULT_SCRIPT_URL)
        if parsed.scheme != "https" or parsed.hostname != expected.hostname or parse_qs(parsed.query).get("table") != parse_qs(expected.query).get("table"):
            raise UnsupportedFirstFrame("FIRST_FRAME_SNAPSHOT_SOURCE_INVALID")
        record = SimpleNamespace(record_id=result["record_id"], fields=dict(raw["fields"]))
        record.execution_target_snapshot = request.get("execution_target_snapshot")
        _validate_record(record, require_production=True)
        if request.get("record_id") and request["record_id"] != record.record_id:
            raise UnsupportedFirstFrame("FIRST_FRAME_SNAPSHOT_ID_MISMATCH")
        if request.get("script_id") and request["script_id"] != result["script_id"]:
            raise UnsupportedFirstFrame("FIRST_FRAME_SNAPSHOT_SCRIPT_MISMATCH")
        if _frame_ready(record.fields) and not result["script_id"].startswith("wsr_"):
            names = PRODUCTION_SCRIPT_FIELD_NAMES
            return {**result, "status": "ready", "cached": True, "fields": {name: record.fields[name] for name in (
                names["composite_first_frame"], names["first_frame_status"], names["reference_strategy"]) if name in record.fields}}
        client = _client(source_url)
        storage = FirstFrameStorage(db_path)
        storage.ensure_schema()
        outcome = _run_record(client, storage, record, source_url=source_url, require_production=True,
                              db_path=db_path, image_timeout_seconds=image_timeout_seconds)
        return {**result, **outcome}
    except UnsupportedFirstFrame as exc:
        return {**result, "error": str(exc)}
    except Exception as exc:
        # Do not erase IMAGE_READY/READY cache or uploaded tokens when only a
        # later transfer fails. Next attempt resumes without image generation.
        # A busy lock belongs to another active attempt; do not mark it failed.
        if client is not None and record is not None and "FIRST_FRAME_LOCKED" not in str(exc):
            try:
                client.update_record_fields(record.record_id, {
                    PRODUCTION_SCRIPT_FIELD_NAMES["first_frame_status"]: "生成失败"})
            except Exception:
                pass
        return {**result, "status": "failed", "error": str(exc)[:1500]}


def run_tasks(
    *,
    source_url: str = DEFAULT_SCRIPT_URL,
    product_code: str = "",
    record_id: str = "",
    limit: int = 5,
    dry_run: bool = False,
    force: bool = False,
    db_path: Optional[str] = None,
    image_timeout_seconds: int = DEFAULT_IMAGE_TIMEOUT_SECONDS,
    stale_after_seconds: int = DEFAULT_STALE_AFTER_SECONDS,
) -> Dict[str, int]:
    client = _client(source_url)
    storage = FirstFrameStorage(db_path)
    if not dry_run:
        storage.ensure_schema()
    fields = PRODUCTION_SCRIPT_FIELD_NAMES
    recovered = [] if dry_run else storage.recover_stale_generating(
        stale_after_seconds=stale_after_seconds
    )
    recovered_record_ids = {
        _text(row.get("source_record_id")) for row in recovered
        if _text(row.get("source_record_id"))
    }
    for recovered_record_id in recovered_record_ids:
        try:
            client.update_record_fields(
                recovered_record_id,
                {fields["first_frame_status"]: "生成失败"},
            )
        except Exception as exc:
            print(
                f"[首帧遗留状态回写失败] {recovered_record_id} | {_text(exc)[:300]}",
                file=sys.stderr,
            )
    records = client.list_records(page_size=500)
    selected = []
    for record in records:
        if record_id and record.record_id != record_id:
            continue
        if product_code and _text(record.fields.get(fields["product_code"])) != product_code:
            continue
        if not _checked(record.fields.get(fields["first_frame_requested"])):
            continue
        if _frame_ready(record.fields) and not force and not (
            _text(record.fields.get(fields["script_id"])).startswith("wsr_")
            and (record_id or _checked(record.fields.get(fields["production_enabled"])))
        ):
            continue
        try:
            _validate_record(record)
        except UnsupportedFirstFrame:
            continue
        selected.append(record)
        if len(selected) >= max(1, limit):
            break

    metrics = {
        "selected": len(selected), "ready": 0, "cached": 0,
        "failed": 0, "skipped": 0,
        "recovered_stale": len(recovered_record_ids),
    }
    for record in selected:
        script_id = _text(record.fields.get(fields["script_id"]))
        product = _text(record.fields.get(fields["product_code"]))
        if dry_run:
            print(f"[预览] {record.record_id} | {product} | {script_id}")
            continue
        try:
            outcome = _run_record(client, storage, record, source_url=source_url, force=force,
                                  db_path=db_path, image_timeout_seconds=image_timeout_seconds)
            metrics["cached" if outcome.get("cached") else "ready"] += 1
        except UnsupportedFirstFrame as exc:
            print(f"[首帧跳过] {record.record_id} | {script_id} | {_text(exc)[:500]}", file=sys.stderr)
            metrics["skipped"] += 1
        except Exception as exc:
            message = _text(exc)[:500]
            print(f"[首帧失败] {record.record_id} | {script_id} | {message}", file=sys.stderr)
            try:
                client.update_record_fields(record.record_id, {fields["first_frame_status"]: "生成失败"})
            except Exception:
                pass
            metrics["failed"] += 1
    return metrics


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="处理原创生产脚本表中用户勾选的首帧任务")
    parser.add_argument("--source-url", default=DEFAULT_SCRIPT_URL)
    selector = parser.add_mutually_exclusive_group()
    selector.add_argument("--product-code", default="")
    selector.add_argument("--record-id", default="")
    selector.add_argument("--record-snapshot", default="", help="单条first-frame-record-v1 JSON快照，不扫描表")
    parser.add_argument("--result-json", default="", help="机器结果JSON路径，快照模式必填")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--db-path")
    parser.add_argument(
        "--image-timeout-seconds", type=int,
        default=int(os.environ.get("ORIGINAL_FIRST_FRAME_IMAGE_TIMEOUT_SECONDS", DEFAULT_IMAGE_TIMEOUT_SECONDS)),
    )
    parser.add_argument(
        "--stale-after-seconds", type=int,
        default=int(os.environ.get("ORIGINAL_FIRST_FRAME_STALE_AFTER_SECONDS", DEFAULT_STALE_AFTER_SECONDS)),
    )
    args = parser.parse_args(argv)
    if args.record_snapshot and not args.result_json:
        parser.error("--record-snapshot requires --result-json")
    if args.record_snapshot and (args.force or args.dry_run):
        parser.error("快照串联不支持force/dry-run；check入口不得启动首帧子进程")
    return args


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    if args.record_snapshot:
        try:
            request = json.loads(Path(args.record_snapshot).read_text(encoding="utf-8"))
            if not isinstance(request, Mapping):
                raise ValueError("record snapshot must be an object")
            result = run_record_snapshot(request, db_path=args.db_path,
                                         image_timeout_seconds=args.image_timeout_seconds)
        except Exception as exc:
            result = {"protocol": "first-frame-record-v1", "record_id": "", "script_id": "",
                      "status": "unsupported", "fields": {}, "error": str(exc)[:1500]}
        result_path = Path(args.result_json).expanduser().resolve()
        result_path.parent.mkdir(parents=True, exist_ok=True)
        result_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result["status"] == "ready" else 1
    metrics = run_tasks(
        source_url=args.source_url,
        product_code=args.product_code,
        record_id=args.record_id,
        limit=args.limit,
        dry_run=args.dry_run,
        force=args.force,
        db_path=args.db_path,
        image_timeout_seconds=args.image_timeout_seconds,
        stale_after_seconds=args.stale_after_seconds,
    )
    print(json.dumps(metrics, ensure_ascii=False, indent=2))
    return 1 if metrics["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
