from dataclasses import replace

import pytest

from test_fixtures import mother_contract, mother_review
from test_v1_1 import _seed_repository, _compile_output, _fact
from test_v1_lite import output_for, fact
from wig_success_replication.core_points import mother_core_points, obvious_omissions
from wig_success_replication.deterministic_qa import inspect_compile_output
from wig_success_replication.execution_contract import execution_summary, load_compile_prompt
from wig_success_replication.hashing import source_hash
from wig_success_replication.models import ExecutionSummary, MotherContract, MotherReview, MotherStatus, VariantPlanItem
from wig_success_replication.mother_service import MotherInput, MotherTemplateService
from wig_success_replication.replication_service import ReplicationBatchService
from wig_success_replication.repository import InMemoryRepository
from wig_success_replication.variant_planner import VariantPlanner


def request(**changes):
    return replace(MotherInput(mother_id="m", feishu_record_id="r", name="mother", source_product_id="source",
        source_script="完整原稿\n保持原始排版", validation_notes="人工测试", source_product_fact=_fact("source"),
        source_image_urls=("https://example.invalid/source.jpg",)), **changes)


class MotherLLM:
    def __init__(self):
        self.calls = []

    def call(self, **kwargs):
        self.calls.append(kwargs)
        payload = kwargs["user_payload"]
        contract = mother_contract("m", payload["mother_version"], "model-placeholder")
        return mother_review(contract) if kwargs["task_type"] == "MOTHER_REVIEW" else contract


def test_default_one_pass_and_explicit_review_are_truthfully_marked():
    repo, llm = InMemoryRepository(), MotherLLM()
    service = MotherTemplateService(repo, llm)
    first = service.process(request())
    assert [call["task_type"] for call in llm.calls] == ["MOTHER_ANALYZE"]
    assert first.record.source_script == request().source_script
    assert first.record.review.review_status == "not_run"
    assert first.record.review.issues == []
    assert "未运行独立审查" in first.record.review.human_summary
    assert MotherReview.model_validate_json(first.record.review.model_dump_json()).review_status == "not_run"
    legacy = mother_review(mother_contract()).model_dump(mode="json")
    legacy.pop("review_status")
    assert MotherReview.model_validate(legacy).review_status == "completed"
    reviewed = service.process(request(), independent_review=True)
    assert [call["task_type"] for call in llm.calls] == ["MOTHER_ANALYZE", "MOTHER_ANALYZE", "MOTHER_REVIEW"]
    assert reviewed.record.review.review_status == "completed"
    assert llm.calls[-1]["image_urls"] is None


def test_business_fingerprint_changes_notes_but_deploy_does_not_reanalyse(monkeypatch):
    import wig_success_replication.mother_service as module
    service, llm = MotherTemplateService(InMemoryRepository(), MotherLLM()), None
    llm = service.llm
    original = request()
    first = service.process(original)
    assert not service.process(replace(original, source_image_urls=("https://example.invalid/new-locator.jpg",))).created
    monkeypatch.setattr(module, "prompt_fingerprint", lambda *args: "new-template-version")
    assert not service.process(original).created
    assert len(llm.calls) == 1
    explicit = service.process(original, refresh=True)
    assert explicit.created and explicit.record.version == 2
    assert explicit.record.contract.processing_provenance.implementation_fingerprint != first.record.contract.processing_provenance.implementation_fingerprint
    changed_notes = service.process(replace(original, human_notes="保留胸前起始"))
    changed_validation = service.process(replace(original, human_notes="保留胸前起始", validation_notes="新增测试依据"))
    assert changed_notes.record.version == 3 and changed_validation.record.version == 4
    assert first.record.source_script == original.source_script
    assert first.record.contract.processing_provenance.input_fingerprint != changed_notes.record.contract.processing_provenance.input_fingerprint


def test_legacy_deploy_with_satisfied_three_slots_does_not_call_any_model():
    repo = _seed_repository(["source"])
    current = repo.latest_mother("m")
    original = request(source_script=current.source_script, validation_notes="")
    current.source_hash = source_hash(original.source_script, {
        "policy_version": "human_selected_claim_transfer_v2",
        "source_product_fact": original.source_product_fact.model_dump(mode="json")})

    class Compiler:
        def call(self, **kwargs):
            return _compile_output(kwargs["user_payload"])

    service = ReplicationBatchService(repo, Compiler())
    assert service.generate(mother_id="m", product_ids=["source"], per_product_count=3).saved_prompts == 3
    before = [(row.prompt_id, row.full_prompt) for row in repo.list_prompts("m", 1, "source")]

    class ForbiddenLLM:
        def call(self, **kwargs):
            raise AssertionError("a code upgrade must not consume a model call")

    reused = MotherTemplateService(repo, ForbiddenLLM()).process(original)
    assert not reused.created and reused.record.version == 1
    assert reused.record.contract.execution_summary is None
    result = ReplicationBatchService(repo, ForbiddenLLM()).generate(
        mother_id="m", product_ids=["source"], per_product_count=3, special_requirements="新拍法只作用于新稿")
    assert result.saved_prompts == 0 and not result.errors
    assert [(row.prompt_id, row.full_prompt) for row in repo.list_prompts("m", 1, "source")] == before


def test_compact_summary_separates_core_from_baseline_without_mutating_legacy():
    old = mother_contract()
    raw = old.model_dump(mode="json")
    legacy = execution_summary(old)
    assert legacy["provenance"] == "legacy_derived"
    assert legacy["legacy_constraints"] == old.global_rules.hard_locks + old.global_rules.forbidden_mutations
    assert old.model_dump(mode="json") == raw
    compact = ExecutionSummary(core_mechanisms=[{"point_id": "occlusion", "requirement": "完整遮挡后才能换发"}],
        baseline_shooting=["双掌从胸前推进"], variable_expression=["允许单掌贴镜头"], provenance="model_extracted")
    current = old.model_copy(update={"execution_summary": compact})
    assert mother_core_points(current)[0]["requirement"] == "完整遮挡后才能换发"
    assert obvious_omissions("单掌直接完全覆盖镜头，然后揭晓假发", current) == []
    assert "胸前" not in str(mother_core_points(current))
    assert MotherContract.model_validate_json(current.model_dump_json()).execution_summary == compact


def test_payload_and_prompt_only_include_relevant_purpose_mode_and_summary():
    repo = _seed_repository(["source"])

    class Compiler:
        calls = []
        def call(self, **kwargs):
            self.calls.append(kwargs)
            return _compile_output(kwargs["user_payload"])

    llm = Compiler()
    result = ReplicationBatchService(repo, llm).generate(mother_id="m", product_ids=["source"],
        per_product_count=1, publish_purpose="养号", handoff_context_by_product={"source": {"mother_name": "test"}})
    assert result.saved_prompts == 1
    payload = llm.calls[0]["user_payload"]
    assert "execution_summary" in payload and "mother_contract" not in payload
    assert "mother_core_points" not in payload  # no repeated parallel lock tree
    prompt = llm.calls[0]["system_prompt"]
    assert "当前用途：养号" in prompt and "当前用途：带货" not in prompt
    assert "当前模式：高保真" in prompt and "当前模式：一般复刻" not in prompt
    saved = repo.list_prompts("m", 1, "source", "养号")[0]
    assert saved.handoff_context["execution_summary"] == payload["execution_summary"]
    assert saved.handoff_context["generation_provenance"] == payload["generation_provenance"]
    general = load_compile_prompt("带货", {"general"})
    assert "当前模式：高保真" not in general and "当前用途：养号" not in general


@pytest.mark.parametrize("purpose", ["带货", "养号"])
def test_no_magic_asset_words_similarity_proof_or_dimension_gate(purpose):
    plan = VariantPlanner().plan("cross_product", 4, existing_sequences=[1, 2, 3], publish_purpose=purpose)
    output = output_for("b", "m", 1, "p", "cross_product", plan)
    output.outputs[0].full_prompt = "场景与人物：女生面对镜头。关键动作与结果：展示新发型。结尾与声音：微笑，无口播。"
    signature = output.outputs[0].creative_signature
    signature.voiceover_text = ""
    signature.proof_actions = []
    signature.proof_order = []
    signature.cta_expression = ""
    signature.core_anchors = ["新发型的视觉回报"]
    signature.changed_dimensions = []
    results = inspect_compile_output(output, plan, fact("p"), publish_purpose=purpose)
    assert results[0].passed and results[0].semantic_status == "not_evaluated"
    signature_roundtrip = type(signature).model_validate_json(signature.model_dump_json())
    assert signature_roundtrip.changed_dimensions == []
    output.outputs[0].full_prompt += "耐热。"
    assert "forbidden_claim:耐热" in inspect_compile_output(output, plan, fact("p", forbidden=["耐热"]))[0].issues


def test_exact_content_duplicate_still_rejected_and_repair_stops_after_one_round():
    plan = VariantPlanner().plan("same_product", 1)
    output = output_for("b", "m", 1, "p", "same_product", plan)
    assert "duplicate_prompt_content" in inspect_compile_output(output, plan, fact("p"), existing_prompts=output.outputs)[0].issues
    repo = _seed_repository(["source"])

    class AlwaysBad:
        calls = []
        def call(self, **kwargs):
            self.calls.append(kwargs)
            payload = kwargs["user_payload"]
            key = payload["variant_plan"][0]
            return _compile_output(payload, bad_keys={(key["variant_key"], key["mutation_key"])})

    llm = AlwaysBad()
    result = ReplicationBatchService(repo, llm).generate(mother_id="m", product_ids=["source"], per_product_count=1)
    assert result.saved_prompts == 0 and result.failed_products == ("source",)
    assert len(llm.calls) == 2
    repair = llm.calls[1]["user_payload"]
    assert repair["failed_items"][0]["full_prompt"] == "缺少资源规则"
    assert repair["qa_failures"][0]["issues"] == ["signature_voiceover_not_in_full_prompt"]


@pytest.mark.parametrize("failure", ["network", "identity"])
def test_repair_failure_preserves_accepted_siblings_and_only_retries_missing_slot(failure):
    repo = _seed_repository(["source"])

    class PartlyBad:
        def __init__(self):
            self.calls = []
        def call(self, **kwargs):
            self.calls.append(kwargs)
            payload = kwargs["user_payload"]
            if len(self.calls) == 1:
                last = payload["variant_plan"][-1]
                return _compile_output(payload, bad_keys={(last["variant_key"], last["mutation_key"])})
            if len(self.calls) == 2:
                if failure == "network":
                    raise TimeoutError("repair transport timeout")
                wrong = _compile_output(payload)
                wrong.mother_id = "wrong-mother"
                return wrong
            return _compile_output(payload)

    llm = PartlyBad()
    service = ReplicationBatchService(repo, llm)
    first = service.generate(mother_id="m", product_ids=["source"], per_product_count=3,
        handoff_context_by_product={"source": {"mother_name": "test"}})
    assert first.saved_prompts == 2 and first.batch.status.value == "partial"
    assert any("repair_failed:" in error for error in first.errors)
    assert [row.sequence_no for row in repo.list_prompts("m", 1, "source")] == [1, 2]
    assert len(repo.pending_outbox()) == 4  # prompt table + script pool for each accepted sibling
    original = {row.prompt_id: row.full_prompt for row in repo.list_prompts("m", 1, "source")}
    resumed = service.generate(mother_id="m", product_ids=["source"], per_product_count=3,
        handoff_context_by_product={"source": {"mother_name": "test"}})
    assert resumed.saved_prompts == 1 and resumed.batch.status.value == "completed"
    assert [item["sequence_no"] for item in llm.calls[-1]["user_payload"]["variant_plan"]] == [3]
    assert len(llm.calls) == 3  # one compile, one failed repair, one missing-slot compile
    assert all(row.full_prompt == original[row.prompt_id]
               for row in repo.list_prompts("m", 1, "source") if row.prompt_id in original)
