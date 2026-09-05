from __future__ import annotations

import json
import threading
import time

import pytest

from test_fixtures import mother_contract, mother_review
from wig_success_replication.model_run_logger import ModelRunLogger
from wig_success_replication.hashing import source_hash
from wig_success_replication.models import (
    Appearance,
    BatchStatus,
    CreativeSignature,
    MotherStatus,
    ProductFactCard,
    ProductFactStatus,
    PromptQA,
    ReplicationCompileOutput,
    ReplicationPrompt,
    VariantPlanItem,
)
from wig_success_replication.mother_service import MotherInput, MotherTemplateService
from wig_success_replication.prompts import load_prompt
from wig_success_replication.repository import (
    InMemoryRepository,
    MotherVersionRecord,
    ProductFactRecord,
)
from wig_success_replication.replication_service import ReplicationBatchService
from wig_success_replication.structured_llm import StructuredModelError, StructuredResponsesClient


def _fact(product_id: str) -> ProductFactCard:
    return ProductFactCard(
        product_id=product_id,
        market=["MX"],
        appearance=Appearance(
            length="long",
            texture="wave",
            color="negro",
            bangs="none",
            layers="normal",
            face_framing="unknown",
        ),
        confirmed_selling_points=["ondas visibles"],
        visual_proof_actions=["girar suavemente"],
        forbidden_claims=["耐热"],
        uncertain_points=[],
        evidence_notes=["imagen"],
        human_confirmed=True,
    )


def _seed_repository(product_ids: list[str]) -> InMemoryRepository:
    repository = InMemoryRepository()
    contract = mother_contract("m", 1, "h")
    mother = MotherVersionRecord(
        mother_id="m",
        version=1,
        feishu_record_id="r",
        name="mother",
        source_product_id="source",
        source_script="script",
        source_hash="h",
        draft=contract,
        review=mother_review(contract),
        contract=contract,
        status=MotherStatus.CONFIRMED,
    )
    repository.save_mother(mother)
    for product_id in product_ids:
        repository.save_product_fact(
            ProductFactRecord(
                product_id,
                1,
                f"hash-{product_id}",
                _fact(product_id),
                "ok",
                ProductFactStatus.CONFIRMED,
            )
        )
    return repository


def _compile_output(payload: dict, *, bad_keys: set[tuple[str, str]] | None = None) -> ReplicationCompileOutput:
    bad_keys = bad_keys or set()
    plan = [VariantPlanItem.model_validate(item) for item in payload["variant_plan"]]
    product_id = payload["target_product_fact"]["product_id"]
    outputs = []
    for item in plan:
        key = (item.variant_key, item.mutation_key)
        base_voiceover = (
            "Amiga mira mi cabello sin volumen y con las puntas apagadas, pero esta peluca cambia todo: "
            "aparece antes del segundo cinco, muestro el contraste, giro y peino para probar el resultado, "
            "toca el carrito para verla."
        )
        voiceover = base_voiceover if item.sequence_no == 1 else (
            "Amiga mira mi cabello con poco volumen y las puntas apagadas, pero esta peluca lo cambia todo: "
            "aparece antes del segundo cinco, enseño el contraste, giro y peino para probar el resultado, "
            "toca el carrito para verla."
        )
        signature = CreativeSignature(
            core_anchors=["conflict", "early_product_reveal", "strong_contrast", "proof", "cta"],
            hook_type=f"hook-{item.sequence_no}",
            opening_action=f"opening-{item.sequence_no}",
            reveal_method=f"reveal-{item.sequence_no}",
            voiceover_text=voiceover,
            timing_pattern=[f"timing-{item.sequence_no}"],
            proof_actions=["girar cabello", "peinar puntas"],
            proof_order=["mostrar frente", "girar cabello", "peinar puntas"],
            cta_expression=f"cta-{item.sequence_no}",
            ending_composition=f"ending-{item.sequence_no}",
            changed_dimensions=item.change_dimensions,
        )
        prompt = "缺少资源规则" if key in bad_keys else (
            "人物参考图只参考脸部，不使用其中的头发。"
            "产品图片定义发色、长度、卷度、刘海和层次。"
            "不上传参考视频。"
            f"{voiceover}。"
            f"独立版本 {product_id} {item.variant_key}。"
        )
        outputs.append(
            ReplicationPrompt(
                variant_type=item.variant_type,
                sequence_no=item.sequence_no,
                replication_mode=item.replication_mode,
                creative_route=item.creative_route,
                variant_key=item.variant_key,
                mutation_key=item.mutation_key,
                change_summary=item.instruction,
                full_prompt=prompt,
                creative_signature=signature,
                qa=PromptQA(passed=True, issues=[]),
            )
        )
    return ReplicationCompileOutput(
        batch_id=payload["batch_id"],
        mother_id="m",
        mother_version=1,
        product_id=product_id,
        relationship=payload["relationship"],
        outputs=outputs,
    )


class _HttpError(RuntimeError):
    def __init__(self, status_code: int):
        self.status_code = status_code
        super().__init__(f"HTTP {status_code}")


@pytest.mark.parametrize("failure", [_HttpError(400), ValueError("schema validation error")])
def test_structured_client_does_not_retry_request_or_schema_failures(failure: Exception):
    class Transport:
        calls = 0

        def create(self, **kwargs):
            self.calls += 1
            raise failure

    transport = Transport()
    client = StructuredResponsesClient(transport, max_retries=1, retry_delay_seconds=0)
    with pytest.raises(StructuredModelError):
        client.call(
            task_type="TEST",
            entity_id="p",
            system_prompt="test",
            user_payload={},
            response_model=ProductFactCard,
            schema_name="product_fact_v1",
        )
    assert transport.calls == 1


def test_structured_client_retries_429_once_and_logs_each_attempt():
    class Transport:
        calls = 0

        def create(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise _HttpError(429)
            return {
                "id": "resp-ok",
                "output_text": json.dumps(_fact("p").model_dump(mode="json"), ensure_ascii=False),
            }

    repository = InMemoryRepository()
    transport = Transport()
    client = StructuredResponsesClient(
        transport,
        logger=ModelRunLogger(repository),
        max_retries=1,
        retry_delay_seconds=0,
    )
    result = client.call(
        task_type="TEST",
        entity_id="p",
        system_prompt="test",
        user_payload={},
        response_model=ProductFactCard,
        schema_name="product_fact_v1",
    )
    assert result.product_id == "p"
    assert transport.calls == 2
    assert [row.status for row in repository.model_runs] == ["retrying", "success"]
    assert [row.retry_count for row in repository.model_runs] == [0, 1]


def test_structured_client_never_exceeds_one_retry():
    class Transport:
        calls = 0

        def create(self, **kwargs):
            self.calls += 1
            raise _HttpError(503)

    transport = Transport()
    client = StructuredResponsesClient(transport, max_retries=1, retry_delay_seconds=0)
    with pytest.raises(StructuredModelError):
        client.call(
            task_type="TEST",
            entity_id="p",
            system_prompt="test",
            user_payload={},
            response_model=ProductFactCard,
            schema_name="product_fact_v1",
        )
    assert transport.calls == 2


def test_mother_review_does_not_resend_product_images():
    contract = mother_contract("m", 1, "ignored")

    class LLM:
        def __init__(self):
            self.calls = []

        def call(self, **kwargs):
            self.calls.append(kwargs)
            return contract if len(self.calls) == 1 else mother_review(contract)

    llm = LLM()
    service = MotherTemplateService(InMemoryRepository(), llm)
    service.process(
        MotherInput(
            mother_id="m",
            feishu_record_id="r",
            name="mother",
            source_product_id="source",
            source_script="完整脚本",
            validation_notes="tested",
            source_product_fact=_fact("source"),
            source_image_urls=("https://example.invalid/wig.jpg",),
        ), independent_review=True
    )
    assert llm.calls[0]["image_urls"] == ["https://example.invalid/wig.jpg"]
    assert llm.calls[1]["image_urls"] is None


def test_explicit_refresh_rebuilds_legacy_mother_using_current_policy():
    repository = _seed_repository(["source"])
    current = repository.latest_mother("m")
    fact = _fact("source")
    current.source_hash = source_hash("script", fact.model_dump(mode="json"))

    class LLM:
        def __init__(self):
            self.calls = []

        def call(self, **kwargs):
            self.calls.append(kwargs)
            contract = mother_contract("m", 2, "ignored")
            return contract if len(self.calls) == 1 else mother_review(contract)

    llm = LLM()
    result = MotherTemplateService(repository, llm).process(MotherInput(
        mother_id="m",
        feishu_record_id="r",
        name="mother",
        source_product_id="source",
        source_script="script",
        validation_notes="tested",
        source_product_fact=fact,
        source_image_urls=("https://example.invalid/wig.jpg",),
    ), refresh=True)
    assert result.created is True
    assert result.record.version == 2
    assert result.record.source_hash != current.source_hash
    assert all("claim_transfer_policy" in call["user_payload"] for call in llm.calls)
    assert len(llm.calls) == 1
    assert result.record.review.review_status == "not_run"


def test_compile_prompt_delegates_claim_transfer_to_human_selection():
    prompt = load_prompt("replication_compile")
    assert "运营选中目标产品即代表声明迁移已获授权" in prompt
    assert "不得重新分析产品匹配度" in prompt
    assert "confirmed_selling_points、visual_proof_actions为空" in prompt
    assert "严禁出现“条件锁”" in prompt


def test_replication_repairs_only_failed_qa_items_once():
    repository = _seed_repository(["source"])

    class LLM:
        def __init__(self):
            self.calls = []

        def call(self, **kwargs):
            self.calls.append(kwargs)
            payload = kwargs["user_payload"]
            plan = [VariantPlanItem.model_validate(item) for item in payload["variant_plan"]]
            bad = {(plan[-1].variant_key, plan[-1].mutation_key)} if len(self.calls) == 1 else set()
            return _compile_output(payload, bad_keys=bad)

    llm = LLM()
    result = ReplicationBatchService(repository, llm).generate(
        mother_id="m", product_ids=["source"], per_product_count=2
    )
    assert result.batch.status == BatchStatus.COMPLETED
    assert result.saved_prompts == 2
    assert len(llm.calls) == 2
    assert llm.calls[0]["task_type"] == "REPLICATION_COMPILE"
    assert llm.calls[1]["task_type"] == "REPLICATION_QA_REPAIR"
    assert llm.calls[1]["user_payload"]["repair_mode"] == "failed_items_only"
    assert len(llm.calls[1]["user_payload"]["variant_plan"]) == 1
    repaired_payload = llm.calls[1]["user_payload"]
    assert repaired_payload["failed_items"][0]["full_prompt"] == "缺少资源规则"
    assert repaired_payload["qa_failures"][0]["issues"] == ["signature_voiceover_not_in_full_prompt"]
    assert "局部编辑" in repaired_payload["repair_scope"]


def test_interrupted_batch_resumes_under_same_idempotency_key():
    repository = _seed_repository(["source"])

    class LLM:
        calls = 0

        def call(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise KeyboardInterrupt("synthetic interruption")
            return _compile_output(kwargs["user_payload"])

    llm = LLM()
    service = ReplicationBatchService(repository, llm)
    with pytest.raises(KeyboardInterrupt):
        service.generate(mother_id="m", product_ids=["source"], per_product_count=1)
    only_batch = next(iter(repository.batches.values()))
    assert only_batch.status == BatchStatus.PARTIAL

    resumed = service.generate(mother_id="m", product_ids=["source"], per_product_count=1)
    assert resumed.created is False
    assert resumed.batch.batch_id == only_batch.batch_id
    assert resumed.batch.status == BatchStatus.COMPLETED
    assert resumed.saved_prompts == 1


def test_replication_supports_bounded_product_concurrency():
    repository = _seed_repository(["source", "p2"])

    class LLM:
        def __init__(self):
            self.lock = threading.Lock()
            self.active = 0
            self.max_active = 0

        def call(self, **kwargs):
            with self.lock:
                self.active += 1
                self.max_active = max(self.max_active, self.active)
            try:
                time.sleep(0.03)
                return _compile_output(kwargs["user_payload"])
            finally:
                with self.lock:
                    self.active -= 1

    llm = LLM()
    result = ReplicationBatchService(repository, llm, max_product_workers=2).generate(
        mother_id="m", product_ids=["source", "p2"], per_product_count=1
    )
    assert result.batch.status == BatchStatus.COMPLETED
    assert result.saved_prompts == 2
    assert llm.max_active == 2


def test_mother_contract_schema_has_compact_limits_without_shape_change():
    schema = mother_contract().model_json_schema()
    definitions = schema["$defs"]
    assert schema["properties"]["segments"]["maxItems"] == 8
    assert definitions["Segment"]["properties"]["slots"]["maxItems"] == 12
    assert definitions["Slot"]["properties"]["source_content"]["maxLength"] == 600
