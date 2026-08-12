#!/usr/bin/env python3
"""Generate user-selected first-frame assets from the production script table."""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional


SKILL_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = SKILL_ROOT.parents[1]
OPENAI_IMAGE_RUNNER = WORKSPACE_ROOT / "skills" / "openai-image" / "run_pipeline.py"
DEFAULT_OUTPUT_ROOT = Path.home() / ".openclaw" / "shared" / "data" / "original_first_frames"
DEFAULT_IMAGE_TIMEOUT_SECONDS = 480
DEFAULT_STALE_AFTER_SECONDS = 900
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.bitable import extract_attachments  # noqa: E402
from core.first_frame_contract import (  # noqa: E402
    build_first_frame_contract,
    render_first_frame_prompt,
)
from core.first_frame_storage import FirstFrameStorage  # noqa: E402
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


def _load_script(script_id: str, db_path: Optional[str] = None) -> Dict[str, Any]:
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
        raise ValueError(f"找不到 SCRIPT_READY 的本地脚本结果: {script_id}")
    result = json.loads(row[0])
    script = result.get("script") if isinstance(result, dict) else None
    if not isinstance(script, dict):
        raise ValueError(f"本地脚本结果缺少 script: {script_id}")
    return script


def _download_references(client: Any, assets: Iterable[Mapping[str, Any]], output_dir: Path) -> list[str]:
    paths: list[str] = []
    for index, asset in enumerate(assets, start=1):
        if not isinstance(asset, Mapping) or not _text(asset.get("file_token")):
            continue
        content, name, content_type, _size = client.download_attachment_bytes(dict(asset))
        suffix = Path(name).suffix or {
            "image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp",
        }.get(content_type, ".jpg")
        path = output_dir / f"ref_{index:02d}{suffix}"
        path.write_bytes(content)
        paths.append(str(path))
    return paths


def _generate_image(
    *,
    prompt: str,
    reference_paths: list[str],
    output_dir: Path,
    asset_id: str,
    timeout_seconds: int = DEFAULT_IMAGE_TIMEOUT_SECONDS,
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
        "metadata": {"reference_order": ["PRODUCT_IDENTITY", "PERSONA_IDENTITY"]},
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
    records = client.list_records(page_size=100)
    selected = []
    for record in records:
        if record_id and record.record_id != record_id:
            continue
        if product_code and _text(record.fields.get(fields["product_code"])) != product_code:
            continue
        if not _checked(record.fields.get(fields["first_frame_requested"])):
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
        fingerprint = ""
        asset_id = ""
        prompt = ""
        contract: Dict[str, Any] = {}
        script_id = _text(record.fields.get(fields["script_id"]))
        product = _text(record.fields.get(fields["product_code"]))
        if dry_run:
            print(f"[预览] {record.record_id} | {product} | {script_id}")
            continue
        if not script_id:
            client.update_record_fields(record.record_id, {fields["first_frame_status"]: "生成失败"})
            metrics["failed"] += 1
            continue
        try:
            script = _load_script(script_id, db_path=db_path)
            product_images = extract_attachments(record.fields.get(fields["product_images"]))
            contract = build_first_frame_contract(
                script_id=script_id,
                product_code=product,
                product_images=product_images,
                script=script,
            )
            availability = _text(contract.get("availability"))
            if availability != "AVAILABLE":
                status = "缺少人物参考图" if availability == "PERSONA_REFERENCE_UNAVAILABLE" else "生成失败"
                client.update_record_fields(record.record_id, {fields["first_frame_status"]: status})
                storage.bind(
                    script_id=script_id, source_record_id=record.record_id,
                    fingerprint=_text(contract.get("asset_fingerprint")),
                    asset_id="", status=availability,
                )
                metrics["failed"] += 1
                continue

            fingerprint = _text(contract.get("asset_fingerprint"))
            cached = None if force else storage.get_ready_asset(fingerprint)
            if cached:
                attachment = json.loads(_text(cached.get("feishu_attachment_json")) or "{}")
                if not isinstance(attachment, dict) or not attachment.get("file_token"):
                    cached = None
            if cached:
                client.update_record_fields(
                    record.record_id,
                    {
                        fields["composite_first_frame"]: [attachment],
                        fields["first_frame_status"]: "缓存复用",
                        fields["reference_strategy"]: _text(
                            contract.get("persona_contract", {}).get("reference_strategy")
                        ) or "GENERATED_FIRST_FRAME",
                    },
                )
                storage.bind(
                    script_id=script_id, source_record_id=record.record_id,
                    fingerprint=fingerprint, asset_id=_text(cached.get("asset_id")),
                    status="READY",
                )
                metrics["cached"] += 1
                continue

            asset_id = "FFA_" + fingerprint[:20].upper()
            prompt = render_first_frame_prompt(contract)
            client.update_record_fields(record.record_id, {fields["first_frame_status"]: "生成中"})
            storage.upsert_asset(
                fingerprint=fingerprint, asset_id=asset_id, status="GENERATING",
                model=_text(contract.get("image_model")),
                prompt_version=_text(contract.get("prompt_version")),
                prompt_text=prompt,
                contract_json=json.dumps(contract, ensure_ascii=False, sort_keys=True, default=str),
            )
            storage.bind(
                script_id=script_id, source_record_id=record.record_id,
                fingerprint=fingerprint, asset_id=asset_id, status="GENERATING",
            )
            asset_dir = DEFAULT_OUTPUT_ROOT / fingerprint
            asset_dir.mkdir(parents=True, exist_ok=True)
            with tempfile.TemporaryDirectory(prefix="original-first-frame-") as tmp:
                tmp_dir = Path(tmp)
                reference_assets = list(contract.get("product_reference_assets") or []) + list(
                    contract.get("persona_reference_assets") or []
                )
                reference_paths = _download_references(client, reference_assets, tmp_dir)
                if not reference_paths:
                    raise ValueError("没有可下载的商品/人物参考图")
                generated = _generate_image(
                    prompt=prompt, reference_paths=reference_paths,
                    output_dir=tmp_dir, asset_id=asset_id,
                    timeout_seconds=image_timeout_seconds,
                )
                permanent = asset_dir / f"{asset_id}.png"
                shutil.copy2(generated, permanent)
            attachment = _upload_generated(client, permanent)
            storage.upsert_asset(
                fingerprint=fingerprint, asset_id=asset_id, status="READY",
                model=_text(contract.get("image_model")),
                prompt_version=_text(contract.get("prompt_version")),
                prompt_text=prompt,
                contract_json=json.dumps(contract, ensure_ascii=False, sort_keys=True, default=str),
                local_path=str(permanent),
                feishu_attachment_json=json.dumps(attachment, ensure_ascii=False, sort_keys=True),
                error_message="",
            )
            storage.bind(
                script_id=script_id, source_record_id=record.record_id,
                fingerprint=fingerprint, asset_id=asset_id, status="READY",
            )
            client.update_record_fields(
                record.record_id,
                {
                    fields["composite_first_frame"]: [attachment],
                    fields["first_frame_status"]: "已就绪",
                    fields["reference_strategy"]: _text(
                        contract.get("persona_contract", {}).get("reference_strategy")
                    ) or "GENERATED_FIRST_FRAME",
                },
            )
            metrics["ready"] += 1
        except Exception as exc:
            message = _text(exc)[:500]
            print(f"[首帧失败] {record.record_id} | {script_id} | {message}", file=sys.stderr)
            client.update_record_fields(
                record.record_id,
                {fields["first_frame_status"]: "生成失败"},
            )
            try:
                if fingerprint and asset_id:
                    storage.upsert_asset(
                        fingerprint=fingerprint, asset_id=asset_id, status="FAILED",
                        model=_text(contract.get("image_model")),
                        prompt_version=_text(contract.get("prompt_version")),
                        prompt_text=prompt,
                        contract_json=json.dumps(contract, ensure_ascii=False, default=str),
                        error_message=message,
                    )
                    storage.bind(
                        script_id=script_id, source_record_id=record.record_id,
                        fingerprint=fingerprint, asset_id=asset_id, status="FAILED",
                    )
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
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
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
