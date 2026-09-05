"""Explicit, isolated single-script revisions; never consume production slots.

Revisions live in content-addressed local artifacts. Export queues a complete
new-script payload in the existing outbox, not a row with sequence zero in the
production prompt table (zero is also covered by its unique index).
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict
import base64
import hashlib
import fcntl
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Callable
from types import SimpleNamespace

from .core_points import mother_core_points
from .checkpoint_policy import freeze_checkpoints
from .execution_contract import execution_summary, compilation_provenance
from .deterministic_qa import inspect_compile_output
from .models import ReplicationCompileOutput, VariantPlanItem
from .hashing import deterministic_id, prompt_hash, stable_hash
from .reference_manifest import validate_reference_manifest
from .repository import MotherVersionRecord, PromptRecord, Repository, make_outbox_record

REVISION_VERSION = "wsr-explicit-revision-v1"


effective_checkpoint_policy = freeze_checkpoints


def _context_identity(context: dict) -> str:
    value = deepcopy(context)
    manifest = value.pop("reference_manifest", None)
    if manifest:
        value["reference_content"] = {"manifest_id": manifest["manifest_id"], "derived": [
            {"derived_sha256": asset.get("derived_sha256"), "transform": asset.get("transform")}
            for asset in manifest["reference_assets"]]}
    return stable_hash(value)

def prepare_revision(*, source: PromptRecord, mother: MotherVersionRecord, instruction: str,
                     allowed_changes: list[dict] | None = None, kind: str = "revision") -> dict:
    if kind not in {"revision", "comparison"} or not str(instruction).strip():
        raise ValueError("revision/comparison and a nonempty revision instruction are required")
    if (source.mother_id, source.mother_version) != (mother.mother_id, mother.version):
        raise ValueError("revision must use the original frozen mother version")
    context = deepcopy(source.handoff_context)
    manifest = validate_reference_manifest(context.get("reference_manifest") or {})
    summary = context.get("execution_summary") or execution_summary(mother.contract)
    if hasattr(summary, "model_dump"):
        summary = summary.model_dump(mode="json")
    points = context.get("mother_core_points") or mother_core_points(mother.contract)
    if not summary:
        summary = {"schema_version": "1", "core_mechanisms": points, "baseline_shooting": [],
                   "variable_expression": [], "provenance": "legacy_derived"}
    policy = effective_checkpoint_policy(points, allowed_changes)
    effective_summary = {**deepcopy(summary), "core_mechanisms": policy["effective_checkpoints"]}
    identity = {"version": REVISION_VERSION, "source_prompt_id": source.prompt_id,
        "source_prompt_hash": prompt_hash(source.full_prompt), "mother_id": mother.mother_id,
        "mother_version": mother.version, "mother_source_hash": mother.source_hash,
        "reference_manifest_id": manifest["manifest_id"], "execution_summary": summary,
        "instruction": str(instruction).strip(), "kind": kind, **policy,
        "generation_provenance": compilation_provenance(mother),
        "source_context_hash": _context_identity(context),
        "source_controls": {key: getattr(source, key) for key in ("product_id", "publish_purpose", "cart_enabled", "sequence_no", "variant_type", "replication_mode", "creative_route")},
        "relationship": context.get("relationship") or ("same_product" if source.product_id == mother.source_product_id else "cross_product"),
        "model": "gpt-5.6-sol", "reasoning_effort": "high"}
    request_hash = stable_hash(identity)
    return {"revision_id": deterministic_id("revision", request_hash), "request_hash": request_hash,
            "revision_kind": kind, "revision_instruction": str(instruction).strip(),
            "original_prompt": source.full_prompt, "parent_prompt_id": source.prompt_id,
            "mother_id": mother.mother_id, "mother_version": mother.version,
            "frozen_execution_summary": deepcopy(summary), "effective_execution_summary": effective_summary,
            "reference_manifest": manifest, "source_snapshot": asdict(source), "identity": identity, **policy}


def validate_revision_request(request: dict) -> None:
    policy = effective_checkpoint_policy(request["frozen_mother_core_points"], request["allowed_changes"])
    if any(request.get(key) != value for key, value in policy.items()):
        raise ValueError("revision effective checkpoints were changed without explicit authorization")
    if stable_hash(request["identity"]) != request["request_hash"] or deterministic_id("revision", request["request_hash"]) != request["revision_id"]:
        raise ValueError("revision request identity mismatch")
    manifest = validate_reference_manifest(request["reference_manifest"])
    if manifest["manifest_id"] != request["identity"]["reference_manifest_id"] or prompt_hash(request["original_prompt"]) != request["identity"]["source_prompt_hash"]:
        raise ValueError("revision frozen content mismatch")
    identity, source = request["identity"], request["source_snapshot"]
    expected = {"instruction": request["revision_instruction"], "kind": request["revision_kind"],
                "source_prompt_id": request["parent_prompt_id"], "mother_id": request["mother_id"],
                "mother_version": request["mother_version"], "execution_summary": request["frozen_execution_summary"], **policy}
    if any(identity.get(key) != value for key, value in expected.items()):
        raise ValueError("revision top-level fields differ from frozen identity")
    if request["effective_execution_summary"] != {**deepcopy(request["frozen_execution_summary"]), "core_mechanisms": policy["effective_checkpoints"]}:
        raise ValueError("revision effective summary mismatch")
    if (source["prompt_id"], source["mother_id"], source["mother_version"], source["full_prompt"]) != (
            request["parent_prompt_id"], request["mother_id"], request["mother_version"], request["original_prompt"]):
        raise ValueError("revision source snapshot mismatch")
    if any(source.get(key) != value for key, value in identity["source_controls"].items()):
        raise ValueError("revision source controls mismatch")
    if _context_identity(source["handoff_context"]) != identity["source_context_hash"]:
        raise ValueError("revision source context mismatch")
    if _context_identity({"reference_manifest": request["reference_manifest"]}) != _context_identity({"reference_manifest": source["handoff_context"].get("reference_manifest")}):
        raise ValueError("revision references differ from frozen source")


def _validate_completed_artifact(artifact: dict) -> None:
    validate_revision_request(artifact["request"])
    if artifact.get("status") != "READY" or not artifact.get("revised_prompt"):
        raise ValueError("only a complete isolated revision can be exported")
    if prompt_hash(artifact["revised_prompt"]) != artifact.get("revised_prompt_hash"):
        raise ValueError("revision content changed after completion")
    output = artifact.get("model_output") or {}
    if len(output.get("outputs") or []) != 1 or output["outputs"][0].get("full_prompt") != artifact["revised_prompt"]:
        raise ValueError("revision model output differs from artifact")
    _inspect_output(artifact["request"], output)


def resolve_revision_source(repository: Repository, source_id: str) -> PromptRecord | None:
    """Read an exact production prompt or the immutable exported revision artifact."""
    source = repository.get_prompt(source_id)
    if source is not None:
        return source
    if not source_id.startswith("revision_"):
        return None
    binding = repository.get_script_pool_binding(source_id)
    context = deepcopy((binding.metadata if binding else {}).get("frozen_handoff_context") or {})
    artifact = context.get("revision_artifact")
    if not artifact:
        return None
    _validate_completed_artifact(artifact)
    if artifact["request"]["revision_id"] != source_id:
        raise ValueError("revision binding source identity mismatch")
    source_data = deepcopy(artifact["request"]["source_snapshot"])
    # A revision-of-a-revision starts from that version's effective baseline;
    # the previous version's original policy remains inside its frozen artifact.
    context["mother_core_points"] = deepcopy(artifact["request"]["effective_checkpoints"])
    source_data.update(prompt_id=source_id, full_prompt=artifact["revised_prompt"],
        prompt_hash=artifact["revised_prompt_hash"], sequence_no=0, handoff_context=context,
        creative_signature=deepcopy(artifact["model_output"]["outputs"][0]["creative_signature"]))
    return PromptRecord(**source_data)


def _inspect_output(request: dict, raw: dict) -> list[dict]:
    output = ReplicationCompileOutput.model_validate(raw)
    if any(marker in row.full_prompt for row in output.outputs
           for marker in (request["revision_id"], "effective_checkpoints", "allowed_changes", "revision_context")):
        raise ValueError("revision audit metadata leaked into final prompt")
    payload = RevisionService.compiler_payload(request)
    if (output.batch_id, output.mother_id, output.mother_version, output.product_id, output.relationship) != (
        request["revision_id"], request["mother_id"], request["mother_version"], request["source_snapshot"]["product_id"], request["identity"]["relationship"]):
        raise ValueError("revision output identity mismatch")
    results = inspect_compile_output(output, [VariantPlanItem.model_validate(item) for item in payload["variant_plan"]],
        SimpleNamespace(forbidden_claims=payload["target_product_fact"].get("forbidden_claims") or []), existing_prompts=[], publish_purpose=payload["publish_purpose"],
        mother_contract={"execution_summary": request["effective_execution_summary"]})
    issues = [issue for result in results for issue in result.issues]
    if issues:
        raise ValueError("revision mechanical checks failed: " + ",".join(issues))
    return [{"issues": list(result.issues), "assessment_scope": result.assessment_scope,
             "semantic_status": result.semantic_status} for result in results]


def _atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=str(path.parent), delete=False) as handle:
        json.dump(value, handle, ensure_ascii=False, sort_keys=True, default=str, indent=2)
        temporary = handle.name
    os.replace(temporary, path)


class RevisionService:
    def __init__(self, *, artifact_root: Path, compiler: Callable[..., Any] | None = None):
        self.artifact_root, self.compiler = Path(artifact_root), compiler

    @staticmethod
    def compiler_payload(request: dict) -> dict:
        source = request["source_snapshot"]
        # These are response-schema slots, not new cumulative production slots.
        mode = source.get("replication_mode") if source.get("replication_mode") in {"high_fidelity", "general"} else "high_fidelity"
        route = source.get("creative_route") or ("H1" if mode == "high_fidelity" else "G4")
        return {"batch_id": request["revision_id"], "publish_purpose": source["publish_purpose"],
            "cart_enabled": source["cart_enabled"], "relationship": request["identity"]["relationship"],
            "mother_id": source["mother_id"], "mother_version": source["mother_version"],
            "execution_summary": request["effective_execution_summary"],
            "generation_provenance": request["identity"]["generation_provenance"],
            "reference_manifest": request["reference_manifest"],
            "target_product_fact": deepcopy(source.get("handoff_context", {}).get("target_product_fact") or {
                "product_id": source["product_id"], "facts_authority": "unchanged original prompt and frozen references", "freeze_status": "not_frozen"}),
            "variant_plan": [{"variant_type": source.get("variant_type") or "high_fidelity_h1",
                "sequence_no": max(1, min(int(source.get("sequence_no") or 1), 20)), "replication_mode": mode,
                "creative_route": route, "variant_key": "explicit_revision", "mutation_key": request["revision_id"],
                "change_dimensions": ["baseline"], "core_mutations": ["explicit_operator_revision"],
                "instruction": request["revision_instruction"]}],
            "revision_context": {"source_prompt_id": request["parent_prompt_id"], "original_prompt": request["original_prompt"],
                "allowed_changes": request["allowed_changes"], "mode": request["revision_kind"],
                "effective_execution_summary": request["effective_execution_summary"],
                "revision_instruction": request["revision_instruction"]},
            "attached_image_order": {
                "face_reference_count": sum(a["role"] == "person_identity" for a in request["reference_manifest"]["reference_assets"]),
                "product_image_count": sum(a["role"] == "product" for a in request["reference_manifest"]["reference_assets"]),
                "rule": "人物、商品和可选首帧顺序严格以冻结 reference_manifest 为准；不得猜测固定图号。"},
            "special_requirements": "只修订明确指定的内容，其余沿用原稿；只交付执行脚本，测试ID/允许变化/核查点等审计信息不得写进full_prompt。"}

    def run(self, request: dict, *, image_urls: list[str] | Callable[[], list[str]] | None = None, dry_run: bool = False) -> dict:
        validate_revision_request(request)
        path = self.artifact_root / (request["revision_id"] + ".json")
        if dry_run:
            return {"status": "preview", "artifact_path": str(path), "request": deepcopy(request), "external_writes": False}
        if self.compiler is None:
            raise ValueError("revision compiler is unavailable")
        self.artifact_root.mkdir(parents=True, exist_ok=True)
        with (self.artifact_root / (request["revision_id"] + ".lock")).open("a") as lock:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError("revision already running") from exc
            if path.is_file():
                saved = json.loads(path.read_text(encoding="utf-8"))
                if saved.get("request", {}).get("request_hash") != request["request_hash"]:
                    raise ValueError("revision artifact identity mismatch")
                if saved.get("status") == "READY":
                    _validate_completed_artifact(saved)
                    return {**saved, "cached": True, "artifact_path": str(path)}
                if saved.get("status") == "GENERATING":
                    raise RuntimeError("previous revision call is uncertain; inspect artifact before retrying")
            images = (image_urls() if callable(image_urls) else image_urls) or []
            if len(images) != len(request["reference_manifest"]["reference_assets"]):
                raise ValueError("revision requires every frozen reference, in manifest order")
            for image, asset in zip(images, request["reference_manifest"]["reference_assets"]):
                if not str(image).startswith("data:image/") or ";base64," not in image:
                    raise ValueError("revision compiler images must be frozen image data URLs")
                digest = hashlib.sha256(base64.b64decode(image.split(";base64,", 1)[1], validate=True)).hexdigest()
                if digest != asset.get("derived_sha256"):
                    raise ValueError("revision compiler image bytes differ from frozen manifest")
            artifact = {"schema_version": "1", "status": "GENERATING", "request": deepcopy(request)}
            _atomic_json(path, artifact)
            try:
                output = self.compiler(payload=self.compiler_payload(request), images=images, entity_id=request["revision_id"])
                raw = output.model_dump(mode="json") if hasattr(output, "model_dump") else output
                mechanical_checks = _inspect_output(request, raw)
                if (raw.get("batch_id"), raw.get("mother_id"), raw.get("mother_version"), raw.get("product_id")) != (
                    request["revision_id"], request["mother_id"], request["mother_version"], request["source_snapshot"]["product_id"]):
                    raise ValueError("revision output identity mismatch")
                rows = raw.get("outputs") or []
                if len(rows) != 1 or not str(rows[0].get("full_prompt") or "").strip():
                    raise ValueError("revision must return exactly one nonempty final prompt")
                prompt = str(rows[0]["full_prompt"])
                if any(marker in prompt for marker in (request["revision_id"], "effective_checkpoints", "allowed_changes", "revision_context")):
                    raise ValueError("revision audit metadata leaked into final prompt")
                artifact.update(status="READY", revised_prompt=prompt, revised_prompt_hash=prompt_hash(prompt),
                                model_output=raw, model_qa=rows[0].get("qa") or {}, mechanical_checks=mechanical_checks,
                                semantic_status="not_evaluated")
            except Exception as exc:
                artifact.update(status="FAILED", error=f"{type(exc).__name__}: {exc}")
                _atomic_json(path, artifact)
                raise
            _atomic_json(path, artifact)
            return {**artifact, "cached": False, "artifact_path": str(path)}

    @staticmethod
    def enqueue_export(artifact: dict, repository: Repository) -> dict:
        _validate_completed_artifact(artifact)
        request, source = artifact["request"], artifact["request"]["source_snapshot"]
        validate_revision_request(request)
        if prompt_hash(artifact["revised_prompt"]) != artifact.get("revised_prompt_hash"):
            raise ValueError("revision content changed after completion")
        context = deepcopy(source["handoff_context"])
        context.update(mother_id=request["mother_id"], mother_version=request["mother_version"],
            parent_prompt_id=request["parent_prompt_id"], revision_kind=request["revision_kind"],
            allowed_changes=request["allowed_changes"], effective_checkpoints=request["effective_checkpoints"],
            frozen_mother_core_points=request["frozen_mother_core_points"],
            mother_core_points=request["frozen_mother_core_points"], execution_summary=request["effective_execution_summary"],
            reference_manifest=request["reference_manifest"],
            revision_artifact={key: deepcopy(value) for key, value in artifact.items() if key not in {"cached", "artifact_path"}})
        label = "对照测试" if request["revision_kind"] == "comparison" else "脚本修订"
        payload = {"prompt_id": request["revision_id"], "mother_id": request["mother_id"], "mother_version": request["mother_version"],
            "product_id": source["product_id"], "sequence_no": 0, "replication_mode": source["replication_mode"],
            "creative_route": source["creative_route"], "variant_type": source["variant_type"],
            "change_summary": f"{label}｜{request['parent_prompt_id']}｜{request['revision_instruction']}",
            "full_prompt": artifact["revised_prompt"], "voiceover_text": (artifact["model_output"]["outputs"][0].get("creative_signature") or {}).get("voiceover_text", ""),
            "publish_purpose": source["publish_purpose"], "cart_enabled": source["cart_enabled"],
            "handoff_context": context, "status": "待审核"}
        record = make_outbox_record("revision", request["revision_id"], "upsert_script_pool", payload)
        # Stable aggregate ID also survives cosmetic/token-only transport data.
        record.outbox_id = deterministic_id("outbox", "revision", request["revision_id"], "upsert_script_pool")
        queued = repository.enqueue_outbox(record)
        return {"queued": bool(queued), "outbox_id": record.outbox_id, "revision_id": request["revision_id"], "script_id": "wsr_" + request["revision_id"],
                "production_enabled": False, "cumulative_slots_used": 0}
