#!/usr/bin/env python3
"""Explicit local revision/comparison, with separately authorized outbox export."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
from typing import Iterable

from _runtime import load_application, print_result
from wig_success_replication.models import MotherContract, MotherReview
from wig_success_replication.repository import MotherVersionRecord, PromptRecord
from wig_success_replication.revision_service import RevisionService, prepare_revision, resolve_revision_source, _validate_completed_artifact

DEFAULT_ROOT = Path.home() / ".openclaw/shared/data/wig_script_revisions"


def prompt_id(value: str) -> str:
    value = value.removeprefix("wsr_")
    if not re.fullmatch(r"(?:prompt|revision)_[A-Za-z0-9_-]{1,80}", value):
        raise argparse.ArgumentTypeError("an explicit prompt_/wsr_prompt_ ID is required")
    return value


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("revise", "compare", "export-revision"))
    parser.add_argument("--prompt-id", type=prompt_id)
    parser.add_argument("--instruction-file", type=Path)
    parser.add_argument("--changes-file", type=Path)
    parser.add_argument("--snapshot", type=Path, help="offline source_prompt+mother JSON; never fabricates a live record")
    parser.add_argument("--artifact", type=Path)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_ROOT)
    choice = parser.add_mutually_exclusive_group()
    choice.add_argument("--apply", action="store_true")
    choice.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _snapshot(value: dict) -> tuple[PromptRecord, MotherVersionRecord]:
    source = PromptRecord(**value["source_prompt"])
    data = value["mother"]
    contract = MotherContract.model_validate(data["contract"])
    mother = MotherVersionRecord(mother_id=data["mother_id"], version=int(data["version"]),
        feishu_record_id=data.get("feishu_record_id", ""), name=data.get("name", "frozen source mother"),
        source_product_id=data.get("source_product_id", ""), source_script=data.get("source_script", ""),
        source_hash=data["source_hash"], draft=contract, contract=contract,
        review=MotherReview.model_validate(data["review"]) if data.get("review") else MotherReview(
            final_contract=contract, issues=[], human_summary=contract.human_summary, review_status="not_run"))
    return source, mother


def execute(args: argparse.Namespace, *, application_factory=load_application) -> dict:
    apply = bool(args.apply)
    if args.action == "export-revision":
        if not args.artifact or any((args.prompt_id, args.instruction_file, args.changes_file, args.snapshot)):
            raise ValueError("export-revision accepts only --artifact and --apply/--dry-run")
        artifact = _read_json(args.artifact)
        _validate_completed_artifact(artifact)
        if not apply:
            return {"status": "preview", "operation": "upsert_script_pool", "revision_id": artifact["request"]["revision_id"],
                    "production_enabled": False, "cumulative_slots_used": 0}
        app = application_factory()
        result = RevisionService.enqueue_export(artifact, app._runner.repository)
        result["delivery"] = app._runner.outbox_service.retry_one(result["outbox_id"])
        result["status"] = result["delivery"].get("status", "writeback_pending")
        result["artifact_ready"] = True
        if result["delivery"].get("failed", 0):
            result["next_action"] = "retry export-revision with the same artifact; do not regenerate"
        return result
    if not args.instruction_file or args.artifact or not (args.prompt_id or args.snapshot):
        raise ValueError("revise/compare requires --instruction-file and --prompt-id or --snapshot")
    instruction = args.instruction_file.read_text(encoding="utf-8").strip()
    changes = _read_json(args.changes_file) if args.changes_file else []
    if not instruction or not isinstance(changes, list):
        raise ValueError("instruction must be nonempty and changes must be a JSON list")
    app = None
    snapshot = _read_json(args.snapshot) if args.snapshot else None
    if snapshot:
        source, mother = _snapshot(snapshot)
        if args.prompt_id and args.prompt_id != source.prompt_id:
            raise ValueError("snapshot does not belong to the requested prompt")
    elif not apply:
        return {"status": "preview", "source_prompt_id": args.prompt_id, "revision_kind": args.action,
                "instruction": instruction, "allowed_changes": changes, "inputs_resolved": False,
                "note": "apply resolves only the exact stored prompt/mother/references; no cumulative generation", "external_writes": False}
    else:
        app = application_factory()
        source = resolve_revision_source(app._runner.repository, args.prompt_id)
        if source is None:
            raise ValueError("source prompt not found; supply an exact immutable source snapshot for an isolated revision")
        mother = app._runner.repository.get_mother_version(source.mother_id, source.mother_version)
        if mother is None:
            raise ValueError("frozen mother version not found; refusing latest-version substitution")
    request = prepare_revision(source=source, mother=mother, instruction=instruction, allowed_changes=changes,
                               kind="comparison" if args.action == "compare" else "revision")
    if not apply:
        return RevisionService(artifact_root=args.output_dir).run(request, dry_run=True)
    app = app or application_factory()
    compiler = lambda **kwargs: app._runner.replication_service._call_compiler(task_type="REPLICATION_COMPILE", **kwargs)
    # Snapshot image URLs are useful for explicit local fixtures; the service
    # verifies all bytes against the frozen manifest before a model request.
    images = snapshot.get("image_urls") if snapshot else None
    if images is None:
        images = lambda: app._runner.client.attachment_data_urls(request["reference_manifest"]["reference_assets"])
    return RevisionService(artifact_root=args.output_dir, compiler=compiler).run(request, image_urls=images)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = execute(args)
        print_result(result)
        return 1 if (result.get("delivery") or {}).get("failed", 0) else 0
    except Exception as exc:
        print_result({"status": "failed", "error": f"{type(exc).__name__}: {exc}"})
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
