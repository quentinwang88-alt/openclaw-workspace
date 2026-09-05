from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict
import json
from pathlib import Path

import pytest

from test_v1_1 import _compile_output, _seed_repository
from wig_success_replication.deterministic_qa import inspect_compile_output
from wig_success_replication.hashing import batch_signature, deterministic_id
from wig_success_replication.models import BatchStatus, ReplicationCompileOutput, VariantPlanItem
from wig_success_replication.publishing import resolve_publish_settings
from wig_success_replication.replication_service import ReplicationBatchService
from wig_success_replication.repository import InMemoryRepository, MySQLRepository, ScriptPoolBindingRecord
from wig_success_replication.variant_planner import PLANNER_VERSION, VariantPlanner


def silent_output(payload):
    output = _compile_output(payload)
    for prompt in output.outputs:
        prompt.creative_signature.core_anchors = ["母版吸引开场", "关键动作", "视觉变化回报"]
        prompt.creative_signature.voiceover_text = ""
        prompt.creative_signature.proof_actions = []
        prompt.creative_signature.proof_order = []
        prompt.creative_signature.cta_expression = ""
        prompt.creative_signature.hook_type = f"养号动作第{prompt.sequence_no}种"
        prompt.creative_signature.opening_action = f"镜头前回头第{prompt.sequence_no}种"
        prompt.creative_signature.reveal_method = f"自然转身第{prompt.sequence_no}种"
        prompt.full_prompt = (
            "人物参考图只参考脸部，不使用其中的头发。产品图片定义发色、长度、卷度、刘海和层次。"
            f"不上传参考视频。纯音乐，无口播。第{prompt.sequence_no}种自然转身展示视觉变化。"
        )
    return output


class LLM:
    def __init__(self):
        self.calls = []

    def call(self, **kwargs):
        self.calls.append(kwargs)
        payload = kwargs["user_payload"]
        return silent_output(payload) if payload["publish_purpose"] == "养号" else _compile_output(payload)


@pytest.mark.parametrize("purpose,setting,expected", [
    ("带货", None, True), ("养号", None, False), ("养号", "按用途默认", False),
    ("带货", "不挂车", False), ("养号", "挂车", True), ("养号", "是", True),
    ("带货", "否", False), ("养号", True, True), ("带货", False, False),
])
def test_publishing_policy(purpose, setting, expected):
    assert resolve_publish_settings(purpose, setting) == (purpose, expected)


@pytest.mark.parametrize("purpose,setting", [("", None), ("原创", None), ("养号", "maybe")])
def test_publishing_policy_rejects_ambiguous_settings(purpose, setting):
    with pytest.raises(ValueError):
        resolve_publish_settings(purpose, setting)


def test_cumulative_scopes_are_separate_and_legacy_ids_stay_stable():
    repo = _seed_repository(["source"])
    llm = LLM()
    service = ReplicationBatchService(repo, llm)
    commercial = service.generate(mother_id="m", product_ids=["source"], per_product_count=3)
    nurture = service.generate(mother_id="m", product_ids=["source"], per_product_count=3, publish_purpose="养号")
    tail = service.generate(mother_id="m", product_ids=["source"], per_product_count=6, publish_purpose="养号")
    no_op = service.generate(mother_id="m", product_ids=["source"], per_product_count=6, publish_purpose="养号")
    assert [r.saved_prompts for r in (commercial, nurture, tail, no_op)] == [3, 3, 3, 0]
    assert all(r.batch.status == BatchStatus.COMPLETED for r in (commercial, nurture, tail, no_op))
    assert len(llm.calls) == 3
    assert len(repo.list_prompts("m", 1, "source")) == 3
    assert len(repo.list_prompts("m", 1, "source", "养号")) == 6
    assert len(repo.list_prompts("m", 1, "source", None)) == 9
    assert repo.list_prompts("m", 1, "source")[0].prompt_id == deterministic_id("prompt", "m", 1, "source", 1)
    assert commercial.batch.batch_id == deterministic_id("batch", batch_signature("m", 1, ["source"], "", 3, PLANNER_VERSION))
    assert [row.creative_route for row in repo.list_prompts("m", 1, "source", "养号")] == ["H1", "H2", "H3", "G1", "G2", "G3"]


def test_cart_change_does_not_regenerate_or_mutate_frozen_output():
    repo = _seed_repository(["source"])
    llm = LLM()
    service = ReplicationBatchService(repo, llm)
    service.generate(mother_id="m", product_ids=["source"], per_product_count=1, publish_purpose="养号")
    changed = service.generate(mother_id="m", product_ids=["source"], per_product_count=1, publish_purpose="养号", cart_enabled=True)
    assert changed.saved_prompts == 0
    assert len(llm.calls) == 1
    assert changed.batch.cart_enabled is True
    assert repo.list_prompts("m", 1, "source", "养号")[0].cart_enabled is False


def test_nurture_general_has_no_compulsory_voiceover_sales_or_proof_dimensions():
    plan = VariantPlanner().plan("cross_product", 20, publish_purpose="养号")
    assert all(not ({"voiceover", "cta", "proof_order", "proof_selection"} & set(item.change_dimensions)) for item in plan)
    assert all(len(item.change_dimensions) >= 3 for item in plan[3:])
    assert all("不强制销售CTA" in item.instruction for item in plan)


def test_commercial_qa_does_not_invent_voiceover_proof_or_cta_requirements():
    repo = _seed_repository(["source"])
    llm = LLM()
    ReplicationBatchService(repo, llm).generate(mother_id="m", product_ids=["source"], per_product_count=1)
    payload = llm.calls[0]["user_payload"]
    output = silent_output(payload)
    plan = [VariantPlanItem.model_validate(item) for item in payload["variant_plan"]]
    issues = inspect_compile_output(output, plan, repo.latest_product_fact("source").fact)[0].issues
    assert issues == ()
    # Relaxing generic commercial requirements must not relax task identity.
    output.outputs[0].sequence_no = 2
    issues = inspect_compile_output(output, plan, repo.latest_product_fact("source").fact)[0].issues
    assert "sequence_no_mismatch" in issues


def test_asset_and_publishing_snapshot_survive_retry_and_callback_failure(caplog):
    repo = _seed_repository(["source"])
    llm = LLM()
    service = ReplicationBatchService(repo, llm)
    context = {"source": {"mother_name": "母版", "persona_images": [{"file_token": "face-old"}], "product_images": [{"file_token": "product-old"}]}}
    observed = []

    def callback():
        observed.append([row.operation for row in repo.pending_outbox()])
        raise RuntimeError("synthetic Feishu unavailable")

    result = service.generate(mother_id="m", product_ids=["source"], per_product_count=1,
                              publish_purpose="养号", handoff_context_by_product=context,
                              on_prompts_persisted=callback)
    assert result.batch.status == BatchStatus.COMPLETED
    assert observed == [["create_prompt_row", "upsert_script_pool"]]
    assert "delivery pending" in caplog.text
    context["source"]["persona_images"][0]["file_token"] = "face-new"
    row = repo.list_prompts("m", 1, "source", "养号")[0]
    assert row.handoff_context["persona_images"][0]["file_token"] == "face-old"
    assert result.batch.handoff_context_by_product["source"]["persona_images"][0]["file_token"] == "face-old"
    # Simulate interrupted enqueue, then a no-op recovery. No second model call.
    repo.outbox.clear()
    service.generate(mother_id="m", product_ids=["source"], per_product_count=1,
                     publish_purpose="养号", handoff_context_by_product=context)
    assert len(llm.calls) == 1
    pool = next(row for row in repo.pending_outbox() if row.operation == "upsert_script_pool")
    assert pool.payload["voiceover_text"] == ""
    assert pool.payload["cart_enabled"] is False
    assert pool.payload["handoff_context"]["persona_images"][0]["file_token"] == "face-old"
    assert {"mother_id", "mother_version", "product_id", "sequence_no", "full_prompt"}.issubset(pool.payload)


def test_legacy_rows_without_asset_snapshot_do_not_implicitly_enter_pool():
    repo = _seed_repository(["source"])
    service = ReplicationBatchService(repo, LLM())
    service.generate(mother_id="m", product_ids=["source"], per_product_count=1)
    service.generate(mother_id="m", product_ids=["source"], per_product_count=1,
                     handoff_context_by_product={"source": {"product_images": [{"file_token": "today"}]}})
    assert [row.operation for row in repo.pending_outbox()] == ["create_prompt_row"]


def test_resumed_new_prompts_freeze_actual_current_assets_only():
    class InterruptedLLM(LLM):
        def call(self, **kwargs):
            if len(self.calls) == 1:
                self.calls.append(kwargs)
                raise RuntimeError("synthetic interrupted second chunk")
            return super().call(**kwargs)

    repo = _seed_repository(["source"])
    llm = InterruptedLLM()
    service = ReplicationBatchService(repo, llm)
    old = {"source": {"product_images": [{"file_token": "old"}]}}
    new = {"source": {"product_images": [{"file_token": "new"}]}}
    first = service.generate(mother_id="m", product_ids=["source"], per_product_count=6,
                             publish_purpose="养号", handoff_context_by_product=old)
    resumed = service.generate(mother_id="m", product_ids=["source"], per_product_count=6,
                               publish_purpose="养号", handoff_context_by_product=new)
    assert first.saved_prompts == resumed.saved_prompts == 3
    assert resumed.batch.status == BatchStatus.COMPLETED
    assert resumed.batch.batch_id == first.batch.batch_id
    assert len(llm.calls) == 3
    rows = repo.list_prompts("m", 1, "source", "养号")
    assert [row.handoff_context["product_images"][0]["file_token"] for row in rows] == ["old"] * 3 + ["new"] * 3
    assert resumed.batch.handoff_context_by_product == old


def test_repository_deduplicates_only_within_purpose():
    repo = _seed_repository(["source"])
    ReplicationBatchService(repo, LLM()).generate(mother_id="m", product_ids=["source"], per_product_count=1)
    commercial = repo.list_prompts("m", 1, "source")[0]
    nurture = deepcopy(commercial)
    nurture.prompt_id = "nurture-independent"
    nurture.publish_purpose = "养号"
    assert repo.save_prompt(nurture)
    assert not repo.save_prompt(deepcopy(nurture))
    assert repo.prompt_sequence_exists("m", 1, "source", 1, "养号")
    assert repo.prompt_exists("m", 1, "source", nurture.prompt_hash, "养号")


def test_script_pool_binding_upsert():
    repo = InMemoryRepository()
    binding = ScriptPoolBindingRecord("p", "r", "hash", "script", {"attachment_cache": {"a": "b"}})
    assert repo.get_script_pool_binding("p") is None
    repo.save_script_pool_binding(binding)
    assert repo.get_script_pool_binding("p") == binding
    updated = deepcopy(binding)
    updated.last_exported_hash = "new"
    repo.save_script_pool_binding(updated)
    assert repo.get_script_pool_binding("p").last_exported_hash == "new"


def test_mysql_insert_placeholders_and_binding_roundtrip_without_network():
    class RecordingRepository(MySQLRepository):
        def __init__(self):
            self.statements = []

        def _fetchone(self, sql, params):
            self.statements.append((sql, params))
            return None

        def _execute(self, sql, params):
            assert sql.count("%s") == len(params)
            self.statements.append((sql, params))
            return 1

    memory = _seed_repository(["source"])
    result = ReplicationBatchService(memory, LLM()).generate(mother_id="m", product_ids=["source"], per_product_count=1)
    sql = RecordingRepository()
    sql.get_or_create_batch(result.batch)
    sql.save_prompt(memory.list_prompts("m", 1, "source")[0])
    sql.save_script_pool_binding(ScriptPoolBindingRecord("p", "r", "hash"))
    sql.save_script_pool_binding(ScriptPoolBindingRecord("pending", "", ""))
    assert sql.statements[-1][1][1] is None
    sql.prompt_exists("m", 1, "source", "hash", "养号")
    sql.prompt_sequence_exists("m", 1, "source", 1, "养号")
    assert "养号" in sql.statements[-1][1]


def test_checked_in_replication_schema_matches_runtime():
    checked_in = json.loads((Path(__file__).parents[1] / "schemas/replication_output.schema.json").read_text())
    assert checked_in == ReplicationCompileOutput.model_json_schema()


@pytest.mark.parametrize("requested_purpose", ["带货", "养号"])
def test_unclassified_historical_results_block_before_any_model_or_batch_creation(requested_purpose):
    repo = _seed_repository(["source"])
    service = ReplicationBatchService(repo, LLM())
    service.generate(mother_id="m", product_ids=["source"], per_product_count=1)
    old = repo.list_prompts("m", 1, "source")[0]
    old.publish_purpose = "未分类"
    old.cart_enabled = None
    batch_ids = set(repo.batches)
    no_calls = LLM()
    with pytest.raises(ValueError, match="历史用途未分类.*先完成人工用途归类"):
        ReplicationBatchService(repo, no_calls).generate(
            mother_id="m", product_ids=["source"], per_product_count=3,
            publish_purpose=requested_purpose,
        )
    assert no_calls.calls == []
    assert set(repo.batches) == batch_ids
    assert old.cart_enabled is None


def test_unclassified_other_product_does_not_block_a_selected_classified_scope():
    repo = _seed_repository(["source", "p2"])
    llm = LLM()
    service = ReplicationBatchService(repo, llm)
    service.generate(mother_id="m", product_ids=["source"], per_product_count=1)
    old = repo.list_prompts("m", 1, "source")[0]
    old.publish_purpose = "未分类"
    old.cart_enabled = None
    result = service.generate(mother_id="m", product_ids=["p2"], per_product_count=1, publish_purpose="养号")
    assert result.saved_prompts == 1
    assert result.batch.status == BatchStatus.COMPLETED


def test_mysql_unknown_and_null_snapshots_are_not_invented_as_commercial_defaults():
    repo = _seed_repository(["source"])
    result = ReplicationBatchService(repo, LLM()).generate(mother_id="m", product_ids=["source"], per_product_count=1)
    prompt = asdict(repo.list_prompts("m", 1, "source")[0])
    prompt["publish_purpose"] = "未分类"
    prompt["cart_enabled"] = None
    parsed = MySQLRepository._prompt(prompt)
    assert parsed.publish_purpose == "未分类"
    assert parsed.cart_enabled is None
    del prompt["publish_purpose"]
    assert MySQLRepository._prompt(prompt).publish_purpose == "未分类"

    class UnknownBatchRepository(MySQLRepository):
        def __init__(self):
            pass

        def _fetchone(self, sql, params):
            row = asdict(result.batch)
            row["status"] = result.batch.status.value
            row["product_ids_json"] = json.dumps(row["product_ids"])
            row["publish_purpose"] = "未分类"
            row["cart_enabled"] = None
            return row

    batch, created = UnknownBatchRepository().get_or_create_batch(result.batch)
    assert not created
    assert batch.publish_purpose == "未分类"
    assert batch.cart_enabled is None


def test_unclassified_empty_historical_batch_is_not_used_for_new_model_calls():
    repo = _seed_repository(["source"])
    service = ReplicationBatchService(repo, LLM())
    result = service.generate(mother_id="m", product_ids=["source"], per_product_count=1)
    repo.prompts.clear()
    result.batch.publish_purpose = "未分类"
    result.batch.cart_enabled = None
    llm = LLM()
    with pytest.raises(ValueError, match="历史批次.*未分类"):
        ReplicationBatchService(repo, llm).generate(mother_id="m", product_ids=["source"], per_product_count=1)
    assert not llm.calls


def test_migration_never_assumes_historical_purpose_or_cart():
    migration = (Path(__file__).parents[1] / "migrations/003_add_publish_purpose_script_pool_mysql.sql").read_text()
    assert migration.count("DEFAULT '未分类'") == 2
    assert migration.count("cart_enabled TINYINT(1) NULL DEFAULT NULL") == 2
    assert "DEFAULT '带货'" not in migration
