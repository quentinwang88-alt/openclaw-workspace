from __future__ import annotations

import json

from wig_success_replication.application import build_application
from wig_success_replication.deterministic_qa import inspect_compile_output
from wig_success_replication.models import (
    Appearance,
    BatchStatus,
    CreativeSignature,
    ProductFactCard,
    ProductFactStatus,
    PromptQA,
    ReplicationCompileOutput,
    ReplicationPrompt,
)
from wig_success_replication.repository import ProductFactRecord, InMemoryRepository, MotherVersionRecord
from wig_success_replication.replication_service import ReplicationBatchService
from wig_success_replication.structured_llm import MODEL, REASONING_EFFORT, StructuredResponsesClient
from wig_success_replication.variant_planner import VariantPlanner


def fact(product_id: str, *, forbidden: list[str] | None = None) -> ProductFactCard:
    return ProductFactCard(
        product_id=product_id,
        market=["MX"],
        appearance=Appearance(
            length="long", texture="wave", color="negro", bangs="none", layers="normal", face_framing="unknown"
        ),
        confirmed_selling_points=["ondas visibles"],
        visual_proof_actions=["girar suavemente"],
        forbidden_claims=forbidden or [],
        uncertain_points=[],
        evidence_notes=["imagen"],
        human_confirmed=True,
    )


VOICEOVERS = (
    "Amiga mira mi cabello sin volumen y con las puntas apagadas, pero esta peluca cambia todo: aparece antes del segundo cinco, muestro el contraste, giro y peino para probar el resultado, toca el carrito para verla.",
    "Amiga mira mi cabello con poco volumen y las puntas apagadas, pero esta peluca lo cambia todo: aparece antes del segundo cinco, enseño el contraste, giro y peino para probar el resultado, toca el carrito para verla.",
    "Mira amiga mi cabello sin volumen y con puntas apagadas, esta peluca cambia todo: la revelo antes del segundo cinco, muestro el contraste, peino y giro para probar el resultado, toca el carrito para verla.",
    "Mira el problema: mi cabello se ve plano. Antes del segundo cinco aparece la peluca; comparo ambos lados, separo los mechones y giro para demostrar el cambio. Si quieres este look, toca el carrito.",
    "Amiga, mi cabello tiene puntas apagadas. Esta peluca aparece antes del segundo cinco y cambia todo: acerco el color a cámara, peino para mostrar la forma y después giro; el contraste queda claro. Encuéntrala en el carrito.",
    "Mi cabello hoy está sin volumen. Revelo esta peluca antes del segundo cinco: primero giro para mostrar el movimiento, luego peino y cierro con el contraste del antes y después. Si quieres cambiar tu look, toca el carrito.",
)


def _voiceover(sequence_no: int) -> str:
    return VOICEOVERS[min(sequence_no - 1, len(VOICEOVERS) - 1)]


def _signature(item) -> CreativeSignature:
    proof_order = ["mostrar frente", "girar cabello", "peinar puntas"]
    if item.replication_mode == "general":
        proof_order = (
            ["girar cabello", "peinar puntas", "mostrar frente"]
            if item.sequence_no % 2 == 0
            else ["peinar puntas", "mostrar frente", "girar cabello"]
        )
    return CreativeSignature(
        core_anchors=["conflict", "early_product_reveal", "strong_contrast", "proof", "cta"],
        hook_type=f"hook-{item.sequence_no}",
        opening_action=f"opening-{item.sequence_no}",
        reveal_method=f"reveal-{item.sequence_no}",
        voiceover_text=_voiceover(item.sequence_no),
        timing_pattern=[f"timing-{item.sequence_no}"],
        proof_actions=["girar cabello", "peinar puntas"],
        proof_order=proof_order,
        cta_expression=f"cta-{item.sequence_no}",
        ending_composition=f"ending-{item.sequence_no}",
        changed_dimensions=item.change_dimensions,
    )


def full_prompt(extra: str = "", voiceover: str = "") -> str:
    return (
        "人物参考图只参考脸部，不使用其中的头发。产品图片定义发色、长度、卷度、刘海和层次。"
        "不上传参考视频。0-3秒建立悬念，5秒前揭示是假发。"
        + voiceover
        + extra
    )


def output_for(batch_id: str, mother_id: str, version: int, product_id: str, relationship: str, plan):
    return ReplicationCompileOutput(
        batch_id=batch_id,
        mother_id=mother_id,
        mother_version=version,
        product_id=product_id,
        relationship=relationship,
        outputs=[
            ReplicationPrompt(
                variant_type=item.variant_type,
                sequence_no=item.sequence_no,
                replication_mode=item.replication_mode,
                creative_route=item.creative_route,
                variant_key=item.variant_key,
                mutation_key=item.mutation_key,
                change_summary=item.instruction,
                full_prompt=full_prompt(f" 独立版本 {item.variant_key}", _voiceover(item.sequence_no)),
                creative_signature=_signature(item),
                qa=PromptQA(passed=True, issues=[]),
            )
            for item in plan
        ],
    )


def test_variant_defaults_and_cumulative_routes():
    planner = VariantPlanner()
    same = planner.plan("same_product")
    cross = planner.plan("cross_product")
    assert len(same) == 12
    assert len(cross) == 4
    assert [item.creative_route for item in same[:3]] == ["H1", "H2", "H3"]
    assert all(item.replication_mode == "general" for item in same[3:])
    assert all(len(set(item.change_dimensions)) >= 3 for item in same[3:])
    assert planner.plan("same_product", 6, existing_sequences=[1, 2, 3]) == same[3:6]
    assert [item.sequence_no for item in cross] == [1, 2, 3, 4]


def test_deterministic_qa_rejects_human_forbidden_claim_and_non_self_contained():
    planner = VariantPlanner()
    plan = planner.plan("same_product", 1)
    output = output_for("b", "m", 1, "p", "same_product", plan)
    bad = output.outputs[0].model_copy(update={"full_prompt": full_prompt(" 耐热，其余同母版")})
    output = output.model_copy(update={"outputs": [bad]})
    results = inspect_compile_output(output, plan, fact("p", forbidden=["耐热"]))
    assert any("forbidden_claim:耐热" in issue for issue in results[0].issues)
    assert any("non_self_contained:其余同母版" in issue for issue in results[0].issues)


def test_deterministic_qa_rejects_internal_control_language():
    planner = VariantPlanner()
    plan = planner.plan("same_product", 1)
    output = output_for("b", "m", 1, "p", "same_product", plan)
    blocked = output.outputs[0].model_copy(
        update={"full_prompt": full_prompt("【条件锁，当前禁止执行】等待事实核验")}
    )
    results = inspect_compile_output(output.model_copy(update={"outputs": [blocked]}), plan, fact("p"))
    issues = results[0].issues
    assert any("internal_control_language:条件锁" in issue for issue in issues)
    assert any("internal_control_language:当前禁止执行" in issue for issue in issues)
    assert any("internal_control_language:等待事实核验" in issue for issue in issues)


def test_structured_client_uses_fixed_model_and_native_strict_schema():
    class Transport:
        def __init__(self): self.kwargs = None
        def create(self, **kwargs):
            self.kwargs = kwargs
            return {"id": "resp_1", "output_text": json.dumps(fact("p").model_dump(mode="json"), ensure_ascii=False)}

    transport = Transport()
    client = StructuredResponsesClient(transport, max_retries=0)
    result = client.call(
        task_type="TEST", entity_id="p", system_prompt="test", user_payload={},
        response_model=ProductFactCard, schema_name="product_fact_v1",
    )
    assert result.product_id == "p"
    assert transport.kwargs["model"] == MODEL == "gpt-5.6-sol"
    assert transport.kwargs["reasoning"] == {"effort": REASONING_EFFORT}
    fmt = transport.kwargs["text"]["format"]
    assert fmt["type"] == "json_schema" and fmt["strict"] is True
    assert set(fmt["schema"]["required"]) == set(fmt["schema"]["properties"])


def test_batch_idempotency_and_partial_failure():
    from test_fixtures import mother_contract, mother_review

    repo = InMemoryRepository()
    contract = mother_contract("m", 1, "h")
    review = mother_review(contract)
    mother = MotherVersionRecord(
        mother_id="m", version=1, feishu_record_id="r", name="mother", source_product_id="source",
        source_script="script", source_hash="h", draft=contract, review=review, contract=contract,
    )
    mother.status = __import__("wig_success_replication.models", fromlist=["MotherStatus"]).MotherStatus.CONFIRMED
    repo.save_mother(mother)
    repo.save_product_fact(ProductFactRecord("source", 1, "f", fact("source"), "ok", ProductFactStatus.CONFIRMED))
    repo.save_product_fact(ProductFactRecord("broken", 1, "f2", fact("broken"), "ok", ProductFactStatus.CONFIRMED))

    class LLM:
        def call(self, **kwargs):
            product_id = kwargs["user_payload"]["target_product_fact"]["product_id"]
            if product_id == "broken":
                raise RuntimeError("synthetic failure")
            plan = [__import__("wig_success_replication.models", fromlist=["VariantPlanItem"]).VariantPlanItem.model_validate(x)
                    for x in kwargs["user_payload"]["variant_plan"]]
            return output_for(kwargs["user_payload"]["batch_id"], "m", 1, product_id,
                              kwargs["user_payload"]["relationship"], plan)

    service = ReplicationBatchService(repo, LLM())
    first = service.generate(mother_id="m", product_ids=["source", "broken"], per_product_count=1)
    assert first.batch.status == BatchStatus.PARTIAL
    assert first.saved_prompts == 1 and first.failed_products == ("broken",)
    second = service.generate(mother_id="m", product_ids=["source", "broken"], per_product_count=1)
    assert not second.created and second.saved_prompts == 0
    assert len(repo.prompts) == 1


def test_cumulative_target_only_generates_missing_tail():
    from test_fixtures import mother_contract, mother_review
    from wig_success_replication.models import MotherStatus, VariantPlanItem

    repo = InMemoryRepository()
    contract = mother_contract("m", 1, "h")
    repo.save_mother(MotherVersionRecord(
        mother_id="m", version=1, feishu_record_id="r", name="mother",
        source_product_id="source", source_script="script", source_hash="h",
        draft=contract, review=mother_review(contract), contract=contract,
        status=MotherStatus.CONFIRMED,
    ))
    repo.save_product_fact(ProductFactRecord(
        "source", 1, "f", fact("source"), "ok", ProductFactStatus.CONFIRMED
    ))

    class LLM:
        def __init__(self):
            self.planned_sequences = []

        def call(self, **kwargs):
            payload = kwargs["user_payload"]
            plan = [VariantPlanItem.model_validate(item) for item in payload["variant_plan"]]
            self.planned_sequences.append([item.sequence_no for item in plan])
            return output_for(
                payload["batch_id"], "m", 1, "source", payload["relationship"], plan
            )

    llm = LLM()
    service = ReplicationBatchService(repo, llm)
    first = service.generate(mother_id="m", product_ids=["source"], per_product_count=3)
    raised = service.generate(mother_id="m", product_ids=["source"], per_product_count=6)
    unchanged = service.generate(mother_id="m", product_ids=["source"], per_product_count=6)

    assert first.saved_prompts == 3
    assert raised.saved_prompts == 3
    assert unchanged.saved_prompts == 0
    assert unchanged.errors == ()
    assert llm.planned_sequences == [[1, 2, 3], [4, 5, 6]]
    rows = repo.list_prompts("m", 1, "source")
    assert [row.sequence_no for row in rows] == [1, 2, 3, 4, 5, 6]
    assert [row.replication_mode for row in rows] == [
        "high_fidelity", "high_fidelity", "high_fidelity", "general", "general", "general"
    ]


def test_application_public_api_is_side_effect_free():
    app = build_application()
    assert callable(app.run_pending)
    assert callable(app.retry_feishu_outbox)
    runtime = app.check_runtime()
    assert runtime["model"] == "gpt-5.6-sol"
