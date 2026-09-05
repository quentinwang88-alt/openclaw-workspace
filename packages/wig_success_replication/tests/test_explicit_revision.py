from copy import deepcopy
from dataclasses import asdict, replace
import base64
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

from test_v1_1 import _seed_repository, _compile_output
from wig_success_replication.hashing import prompt_hash
from wig_success_replication.reference_manifest import build_reference_manifest
from wig_success_replication.repository import PromptRecord, ScriptPoolBindingRecord
from wig_success_replication.revision_service import (
    RevisionService, prepare_revision, effective_checkpoint_policy, resolve_revision_source,
)


@pytest.fixture
def frozen():
    repo = _seed_repository(["p"])
    mother = repo.get_mother_version("m", 1)
    content = b"offline-frozen-image-fixture"
    digest = hashlib.sha256(content).hexdigest()
    manifest = build_reference_manifest([{"index": 1, "role": "product", "file_token": "frozen-token",
        "original_sha256": digest, "derived_sha256": digest, "transform": "test-identity"}])
    source = PromptRecord(prompt_id="prompt_original", batch_id="batch_old", mother_id="m", mother_version=1,
        product_id="p", variant_type="high_fidelity_h1", variant_key="old", mutation_key="old",
        change_summary="old", full_prompt="原稿：女主展示前后对比。", prompt_hash=prompt_hash("原稿：女主展示前后对比。"),
        sequence_no=1, replication_mode="high_fidelity", creative_route="H1",
        handoff_context={"reference_manifest": manifest, "mother_core_points": [{"point_id": "core_1",
            "requirement": "保留前后反差", "evidence_source": "frozen_mother"}]})
    repo.save_prompt(source)
    return repo, mother, source, ["data:image/png;base64," + base64.b64encode(content).decode()]


def request_for(frozen, **kwargs):
    return prepare_revision(source=frozen[2], mother=frozen[1], instruction=kwargs.pop("instruction", "把钩子改成提问"), **kwargs)


def compiler(**kwargs):
    return _compile_output(kwargs["payload"])


def test_revision_is_local_cached_and_never_uses_cumulative_slots(tmp_path, frozen):
    calls = []
    def call(**kwargs):
        calls.append(kwargs)
        return compiler(**kwargs)
    service = RevisionService(artifact_root=tmp_path, compiler=call)
    request = request_for(frozen)
    result = service.run(request, image_urls=frozen[3])
    cached = service.run(request, image_urls=lambda: pytest.fail("cached call downloaded media"))
    assert result["status"] == "READY" and cached["cached"]
    assert len(calls) == 1
    assert len(frozen[0].list_prompts("m", 1, "p")) == 1
    assert not frozen[0].pending_outbox(10)
    assert calls[0]["payload"]["reference_manifest"] == request["reference_manifest"]
    assert "mother_contract" not in calls[0]["payload"]
    assert result["semantic_status"] == "not_evaluated"


def test_export_is_new_id_idempotent_and_preserves_original_points(tmp_path, frozen):
    change = [{"point_id": "core_1", "operation": "replace", "replacement_requirement": "改用结果对比", "reason": "对照测试"}]
    service = RevisionService(artifact_root=tmp_path, compiler=compiler)
    request = request_for(frozen, kind="comparison", allowed_changes=change)
    first = service.run(request, image_urls=frozen[3])
    second = service.run(request, image_urls=frozen[3])
    assert service.enqueue_export(first, frozen[0])["queued"]
    assert not service.enqueue_export(second, frozen[0])["queued"]
    rows = frozen[0].pending_outbox(10)
    assert len(rows) == 1
    payload = rows[0].payload
    assert payload["sequence_no"] == 0 and payload["prompt_id"] != frozen[2].prompt_id
    assert "对照测试" in payload["change_summary"]
    context = payload["handoff_context"]
    assert context["mother_core_points"] == request["frozen_mother_core_points"]
    assert context["effective_checkpoints"] != context["mother_core_points"]
    assert "cached" not in context["revision_artifact"]
    assert len(frozen[0].list_prompts("m", 1, "p")) == 1
    assert frozen[0].get_prompt("prompt_original").full_prompt == "原稿：女主展示前后对比。"


@pytest.mark.parametrize("field", ["revision_instruction", "effective_checkpoints", "effective_execution_summary", "source_snapshot", "reference_manifest"])
def test_frozen_request_tampering_rejected(tmp_path, frozen, field):
    request = request_for(frozen)
    if field == "revision_instruction":
        request[field] = "偷偷替换"
    elif field == "effective_checkpoints":
        request[field][0]["requirement"] = "偷偷替换"
    elif field == "effective_execution_summary":
        request[field]["baseline_shooting"] = ["偷偷替换"]
    elif field == "source_snapshot":
        request[field]["product_id"] = "other"
    else:
        request[field]["reference_assets"][0]["derived_sha256"] = "a" * 64
    with pytest.raises(ValueError):
        RevisionService(artifact_root=tmp_path).run(request, dry_run=True)


def test_changed_instruction_and_compiler_provenance_get_new_ids(frozen, monkeypatch):
    first = request_for(frozen)
    assert first["revision_id"] != request_for(frozen, instruction="另一个明确修改")["revision_id"]
    monkeypatch.setattr("wig_success_replication.revision_service.compilation_provenance", lambda mother: {"compile_schema_fingerprint": "new"})
    assert first["revision_id"] != request_for(frozen)["revision_id"]


def test_token_only_rebind_does_not_change_revision_content_id(frozen):
    first = request_for(frozen)
    frozen[2].handoff_context["reference_manifest"]["reference_assets"][0]["file_token"] = "copied-token"
    assert first["revision_id"] == request_for(frozen)["revision_id"]


def test_bad_image_rejected_before_compiler(tmp_path, frozen):
    service = RevisionService(artifact_root=tmp_path, compiler=lambda **kw: pytest.fail("model called"))
    with pytest.raises(ValueError, match="bytes differ"):
        service.run(request_for(frozen), image_urls=["data:image/png;base64,YmFk"])


@pytest.mark.parametrize("bad_text", ["【条件锁，当前禁止执行】", "前两张为人物图", "其余不变", "effective_checkpoints"])
def test_mechanical_errors_do_not_become_ready(tmp_path, frozen, bad_text):
    def bad(**kwargs):
        output = compiler(**kwargs)
        output.outputs[0].full_prompt += bad_text
        return output
    with pytest.raises(ValueError):
        RevisionService(artifact_root=tmp_path, compiler=bad).run(request_for(frozen), image_urls=frozen[3])
    stored = json.loads(next(tmp_path.glob("revision_*.json")).read_text())
    assert stored["status"] == "FAILED"


def test_corrupted_ready_cache_does_not_short_circuit_checks(tmp_path, frozen):
    service = RevisionService(artifact_root=tmp_path, compiler=compiler)
    request = request_for(frozen)
    result = service.run(request, image_urls=frozen[3])
    path = Path(result["artifact_path"])
    corrupt = json.loads(path.read_text())
    corrupt["revised_prompt"] = "篡改"
    path.write_text(json.dumps(corrupt))
    with pytest.raises(ValueError):
        service.run(request, image_urls=frozen[3])


def test_dry_run_has_no_model_and_no_artifact_directory(tmp_path, frozen):
    root = tmp_path / "absent"
    result = RevisionService(artifact_root=root).run(request_for(frozen), dry_run=True)
    assert result["status"] == "preview" and not root.exists()


@pytest.mark.parametrize("replacement", [123, None, " "])
def test_checkpoint_policy_rejects_non_text_replacement(replacement):
    with pytest.raises(ValueError):
        effective_checkpoint_policy([{"point_id": "a", "requirement": "原点"}], [{"point_id": "a", "operation": "replace", "replacement_requirement": replacement, "reason": "测试"}])


def test_revision_of_exported_revision_uses_effective_baseline(tmp_path, frozen):
    service = RevisionService(artifact_root=tmp_path, compiler=compiler)
    request = request_for(frozen, allowed_changes=[{"point_id": "core_1", "operation": "replace", "replacement_requirement": "新测试基准", "reason": "对照"}])
    artifact = service.run(request, image_urls=frozen[3])
    service.enqueue_export(artifact, frozen[0])
    context = frozen[0].pending_outbox(10)[0].payload["handoff_context"]
    frozen[0].save_script_pool_binding(ScriptPoolBindingRecord(request["revision_id"], "recNew", "hash",
        metadata={"frozen_handoff_context": context}))
    source = resolve_revision_source(frozen[0], request["revision_id"])
    assert source.full_prompt == artifact["revised_prompt"]
    next_request = prepare_revision(source=source, mother=frozen[1], instruction="再修开头")
    assert next_request["frozen_mother_core_points"] == request["effective_checkpoints"]
    assert len(frozen[0].list_prompts("m", 1, "p")) == 1


def test_wrong_frozen_mother_version_rejected(frozen):
    with pytest.raises(ValueError, match="frozen mother version"):
        prepare_revision(source=frozen[2], mother=replace(frozen[1], version=2), instruction="改写")


def _cli():
    scripts = Path(__file__).resolve().parents[3] / "skills/wig-success-script-replication/scripts"
    sys.path.insert(0, str(scripts))
    import run_script_revision
    return run_script_revision


def test_cli_default_preview_does_not_construct_live_application(tmp_path):
    cli = _cli()
    instruction = tmp_path / "instruction.txt"
    instruction.write_text("改写钩子")
    args = cli.parse_args(["revise", "--prompt-id", "wsr_prompt_original", "--instruction-file", str(instruction)])
    result = cli.execute(args, application_factory=lambda: pytest.fail("live application created"))
    assert result["status"] == "preview" and not result["inputs_resolved"]


def test_snapshot_dry_run_does_not_fabricate_review(tmp_path, frozen):
    cli = _cli()
    snapshot = {"source_prompt": asdict(frozen[2]), "mother": {"mother_id": "m", "version": 1,
        "source_hash": "h", "contract": frozen[1].contract.model_dump(mode="json")}}
    source, mother = cli._snapshot(snapshot)
    assert mother.review.review_status == "not_run"


def test_whitelist_revision_rejects_cumulative_flags_and_injection():
    _cli()
    import openclaw_wig_replication_task as adapter
    args = adapter.parse_args(["revise", "--prompt-id", "prompt_ok", "--instruction-file", "safe.txt"])
    command = adapter.build_command(args)
    assert command[-1] == "--dry-run"
    args.limit = 10
    with pytest.raises(ValueError):
        adapter.build_command(args)
    args.limit = None
    args.prompt_id = "prompt_ok; touch injected"
    with pytest.raises(ValueError):
        adapter.build_command(args)


def test_frozen_product_fact_is_compiled_and_forbidden_text_rejected(tmp_path, frozen):
    frozen[2].handoff_context["target_product_fact"] = {"product_id": "p", "forbidden_claims": ["garantía infinita"]}
    request = request_for(frozen)
    assert RevisionService.compiler_payload(request)["target_product_fact"] == frozen[2].handoff_context["target_product_fact"]
    def bad(**kwargs):
        output = compiler(**kwargs)
        output.outputs[0].full_prompt += " garantía infinita"
        return output
    with pytest.raises(ValueError, match="forbidden_claim"):
        RevisionService(artifact_root=tmp_path, compiler=bad).run(request, image_urls=frozen[3])


def test_same_product_relationship_is_frozen(frozen):
    frozen[1].source_product_id = "p"
    request = request_for(frozen)
    assert RevisionService.compiler_payload(request)["relationship"] == "same_product"
    frozen[2].handoff_context["relationship"] = "cross_product"
    assert RevisionService.compiler_payload(request_for(frozen))["relationship"] == "cross_product"


def test_cli_export_delivers_only_its_own_outbox(tmp_path, frozen):
    cli = _cli()
    artifact = RevisionService(artifact_root=tmp_path, compiler=compiler).run(request_for(frozen), image_urls=frozen[3])
    delivered = []
    app = SimpleNamespace(_runner=SimpleNamespace(repository=frozen[0], outbox_service=SimpleNamespace(
        retry_one=lambda outbox_id: delivered.append(outbox_id) or {"completed": 1})))
    args = cli.parse_args(["export-revision", "--artifact", artifact["artifact_path"], "--apply"])
    result = cli.execute(args, application_factory=lambda: app)
    assert delivered == [result["outbox_id"]]
    assert frozen[0].get_outbox(result["outbox_id"]).aggregate_id == artifact["request"]["revision_id"]
    assert result["delivery"] == {"completed": 1}


def test_cli_export_preview_never_constructs_application(tmp_path, frozen):
    cli = _cli()
    artifact = RevisionService(artifact_root=tmp_path, compiler=compiler).run(request_for(frozen), image_urls=frozen[3])
    args = cli.parse_args(["export-revision", "--artifact", artifact["artifact_path"]])
    result = cli.execute(args, application_factory=lambda: pytest.fail("live app"))
    assert result["status"] == "preview"
    assert not frozen[0].pending_outbox()


def test_cli_frozen_snapshot_preview_no_model_db_or_writes(tmp_path, frozen):
    cli = _cli()
    path = tmp_path / "snapshot.json"
    path.write_text(json.dumps({"source_prompt": asdict(frozen[2]), "mother": {
        "mother_id": "m", "version": 1, "source_hash": "h", "contract": frozen[1].contract.model_dump(mode="json")}}))
    instruction = tmp_path / "instruction.txt"
    instruction.write_text("修订开头")
    output = tmp_path / "no_output"
    args = cli.parse_args(["compare", "--snapshot", str(path), "--instruction-file", str(instruction), "--output-dir", str(output)])
    result = cli.execute(args, application_factory=lambda: pytest.fail("live app"))
    assert result["status"] == "preview" and not output.exists()


def test_mysql_exact_read_interfaces_are_parameterized():
    from wig_success_replication.repository import MySQLRepository
    repo = MySQLRepository(lambda: pytest.fail("real DB connection"))
    calls = []
    repo._fetchone = lambda sql, params: calls.append((sql, params)) or None
    assert repo.get_prompt("prompt_safe") is None
    assert repo.get_mother_version("mother_safe", 7) is None
    assert repo.get_outbox("outbox_safe") is None
    assert calls[0][1] == ("prompt_safe",)
    assert calls[1][1] == ("mother_safe", 7)
    assert calls[2][1] == ("outbox_safe",)
    assert all("%s" in sql and "LIMIT 1" in sql for sql, _ in calls)


def test_failed_delivery_is_not_generation_failure_and_retries_same_artifact(tmp_path, frozen, monkeypatch):
    cli = _cli()
    calls = []
    artifact = RevisionService(artifact_root=tmp_path, compiler=compiler).run(request_for(frozen), image_urls=frozen[3])
    app = SimpleNamespace(_runner=SimpleNamespace(repository=frozen[0], outbox_service=SimpleNamespace(
        retry_one=lambda outbox_id: calls.append(outbox_id) or {"status": "writeback_pending", "failed": 1, "completed": 0})))
    args = cli.parse_args(["export-revision", "--artifact", artifact["artifact_path"], "--apply"])
    first = cli.execute(args, application_factory=lambda: app)
    second = cli.execute(args, application_factory=lambda: app)
    assert first["status"] == "writeback_pending" and first["artifact_ready"]
    assert first["next_action"].endswith("do not regenerate")
    assert calls == [first["outbox_id"], first["outbox_id"]]
    assert len(frozen[0].pending_outbox()) == 1
    assert not second["queued"]
    monkeypatch.setattr(cli, "execute", lambda args: first)
    printed = []
    monkeypatch.setattr(cli, "print_result", printed.append)
    assert cli.main(["export-revision", "--artifact", artifact["artifact_path"], "--apply"]) == 1
    assert printed[0]["artifact_ready"]
