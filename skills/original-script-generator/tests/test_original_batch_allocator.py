"""V1 批次编排器单元测试 — 分配算法 + 存储 + 幂等"""
import contextlib
import json
import os
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

import sys
SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.original_batch_models import (
    PlanItem, BatchRecord, BatchRequest,
    generate_batch_id, generate_batch_item_id, generate_request_id,
    build_allocation_signature, ITEM_ROLES, HOOK_ID_BLACKLIST_FOR_NO_TENSION,
)
from core.original_batch_storage import BatchStorage, POLICY_VERSION
from core.original_batch_allocator import (
    allocate_batch_items,
    build_content_bundle_candidates,
    _creator_weighted_directions,
    _annotate_variant_fit,
    _annotate_selling_argument_lineage,
    _build_proof_execution_intent,
    _build_argument_context_alignment,
    _eligible_hooks_for_bundle,
    _mixed_history_references,
    _relationship_device_for_hook,
    _relationship_schedule,
)
from core.accessory_mixed_templates import (
    MIXED_HISTORY_METADATA_KEY,
    MIXED_SIGNATURE_VERSION,
    compile_mixed_template_contract,
    judge_mixed_candidate,
    ledger_row_identity,
    mixed_reference_signature,
    mixed_signature_bundle,
    mixed_template_ids,
    mixed_visual_signature,
    select_environment_recipe_id,
    select_template_id,
)
from core.storage import PipelineStorage
from core.complete_script_v3 import (
    _perceptual_action_family,
    _perceptual_signature,
    _usage_perceptual_signature,
)

_MIXED_GATE_ENV = "ORIGINAL_SCRIPT_ACCESSORY_MIXED_TEMPLATE_V1_ENABLED"

_ISO_ROOT = tempfile.mkdtemp(prefix="alloc_iso_")
_ENV_GUARD = None


def setUpModule():
    """Run this whole file against scratch stores, never the production ledger."""

    global _ENV_GUARD
    _ENV_GUARD = _isolated_databases(_ISO_ROOT)
    _ENV_GUARD.__enter__()


def tearDownModule():
    import shutil

    if _ENV_GUARD is not None:
        _ENV_GUARD.__exit__(None, None, None)
    shutil.rmtree(_ISO_ROOT, ignore_errors=True)


@contextlib.contextmanager
def _isolated_databases(root: str):
    """Point every known local store at a scratch directory.

    ``run_plan_only`` builds its own ``BatchStorage`` / ``PipelineStorage`` from
    the environment, so without this the planning tests would write the
    production ledger.  The RDS URLs are removed for the same reason: a URL in
    the ambient environment outranks the sqlite path.
    """

    removed = {
        key: os.environ.pop(key)
        for key in ("ORIGINAL_SCRIPT_GENERATOR_DATABASE_URL", "LIKEU_AI_DATABASE_URL")
        if key in os.environ
    }
    overrides = {
        "OPENCLAW_SHARED_DATA_DIR": root,
        "ORIGINAL_SCRIPT_GENERATOR_DB_PATH": str(Path(root) / "gen.sqlite3"),
        "ORIGINAL_SCRIPT_GENERATOR_CONFIG_PATH": str(Path(root) / "cfg.json"),
        "ORIGINAL_SCRIPT_OUTFIT_TEMPLATE_DB_PATH": str(Path(root) / "outfit.sqlite3"),
        "SHORT_VIDEO_AUTO_PUBLISH_DB_PATH": str(Path(root) / "publish.sqlite3"),
        "ORGANIC_SEEDING_DB_PATH": str(Path(root) / "seeding.sqlite3"),
        "ORIGINAL_SCRIPT_LONGFORM_DB_PATH": str(Path(root) / "longform.sqlite3"),
    }
    try:
        with patch.dict(os.environ, overrides, clear=False):
            yield
    finally:
        os.environ.update(removed)


# ── Helpers ────────────────────────────────────────────────────────────

def _fake_direction(
    da_id: str = "SRA_TEST_001",
    output_slot: str = "S1",
    cluster_id: int = 1,
    macro_family: str = "HOOK>PROOF",
    carrier: str = "WEARER_ACTIVE",
) -> dict:
    return {
        "direction_assignment_id": da_id,
        "output_slot": output_slot,
        "selection_run_id": "SR_TEST",
        "cluster_id": cluster_id,
        "cluster_version": "v1",
        "evidence_tier": "BOOTSTRAP",
        "structure_contract": {
            "direction_identity": {"macro_family_key": macro_family},
            "hard_constraints": {
                "content_carrier": carrier,
                "continuity_mode": "MULTI_CUT",
            },
            "evidence": {"evidence_tier": "BOOTSTRAP"},
        },
        "execution_reference": {
            "execution_card_id": f"EC_{da_id}",
            "source_video_id": "V_TEST",
            "content_carrier": carrier,
        },
        "country": "泰国",
        "category": "女装",
    }


def _fake_anchor_card() -> dict:
    return {
        "hard_anchors": [{"anchor": "短款衣长", "why_must_show": "腰线位置"}],
        "display_anchors": [
            {"anchor": "立领", "why_must_show": "领口结构"},
            {"anchor": "金属拉链", "why_must_show": "正面细节"},
            {"anchor": "翻盖贴袋", "why_must_show": "胸前口袋"},
        ],
        "category_execution_contract": {"display_family": "apparel"},
    }


def _fake_selling_catalog() -> list:
    return [
        {
            "value_id": "ARG_FRONT_STRUCTURE",
            "primary_selling_point": "立领和拉链让正面结构更清楚",
            "proof_thesis": "立领和金属拉链形成清楚的正面结构",
            "argument_kind": "SELLING_ARGUMENT",
            "compatible_carriers": ["WEARER_ACTIVE", "STATIC_PRODUCT", "MIXED"],
        },
        {
            "value_id": "ARG_DETAIL_COMBINATION",
            "primary_selling_point": "口袋和拉链细节集中在正面",
            "proof_thesis": "正面口袋与拉链细节可以直接观察",
            "argument_kind": "SELLING_ARGUMENT",
            "compatible_carriers": ["WEARER_ACTIVE", "STATIC_PRODUCT", "MIXED"],
        },
    ]


# ── Storage tests ──────────────────────────────────────────────────────


class PerceptualRepeatContractTest(unittest.TestCase):
    def test_signature_ignores_surface_wording_but_tracks_viewer_pattern(self):
        first = _perceptual_signature(
            product_code="P1",
            item={
                "scene_family_key": "HOME_ROUTINE",
                "action_grammar": "拿起随身包→走向门口",
            },
            outfit_contract={"silhouette_key": "SHORT_TOP_HIGH_WAIST"},
            direction_carrier="WEARER_ACTIVE",
            structure_family="HOOK>PROOF",
        )
        second = _perceptual_signature(
            product_code="P1",
            item={
                "scene_family_key": "HOME_ROUTINE",
                "action_grammar": "拿起钥匙后自然走向出口",
            },
            outfit_contract={"silhouette_key": "SHORT_TOP_HIGH_WAIST"},
            direction_carrier="WEARER_ACTIVE",
            structure_family="HOOK>PROOF",
        )
        self.assertEqual(first, second)
        self.assertEqual(
            "TRANSIT_WALK", _perceptual_action_family("沿走廊走向出口")
        )
        self.assertEqual(
            first,
            _usage_perceptual_signature({
                "metadata_json": json.dumps({"perceptual_signature": first})
            }),
        )


class BatchStorageTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self.db_path = Path(self._tmpdir) / "test_batch.sqlite3"
        self.storage = BatchStorage(db_path=self.db_path)
        self.storage.ensure_schema()

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _make_batch(self, request_id="OP_TEST_001", product_code="P1", count=5):
        return BatchRecord(
            batch_id=generate_batch_id(request_id, product_code, "hash_in", "hash_snap", POLICY_VERSION, 42),
            request_id=request_id,
            product_code=product_code,
            requested_count=count,
            test_phase="INITIAL",
            execution_mode="PLAN_ONLY",
            policy_version=POLICY_VERSION,
            random_seed=42,
            data_snapshot_hash="hash_snap",
            input_snapshot_json="{}",
            planned_count=count,
            status="PLANNED",
        )

    def _make_item(self, batch_id, idx, role="STRUCTURE_MOTHER"):
        sig = build_allocation_signature(f"DA_{idx}", "FACT_DISCOVERY", [f"CLM_{idx}"], "DETAIL_SURPRISE", f"VS_{idx}")
        return PlanItem(
            batch_item_id=generate_batch_item_id(batch_id, idx, sig),
            batch_id=batch_id,
            item_index=idx,
            item_role=role,
            product_code="P1",
            selection_run_id="SR_TEST",
            direction_assignment_id=f"DA_{idx}",
            compatibility_slot=f"S{idx}",
            structure_contract_json="{}",
            allocation_signature=sig,
            policy_version=POLICY_VERSION,
            item_snapshot_hash=f"SN_{idx}",
            content_bundle_id=f"CB_{idx}",
            content_bundle_json="{}",
            content_angle_key="FACT_DISCOVERY",
            audience_tension_status="UNAVAILABLE",
            claim_keys_json='["CLM_1"]',
            requested_hook_id="DETAIL_SURPRISE",
            eligible_hook_ids_json='["DETAIL_SURPRISE","AUDIENCE_NEED_CALLOUT"]',
            frozen_direction_package_json=json.dumps({
                "schema_version": "original-frozen-direction-package-v1",
                "creative_diversity_contract": {"contract_id": f"CDC_{idx}"},
            }, ensure_ascii=False),
            status="PLANNED",
        )

    def test_create_and_get_batch(self):
        batch = self._make_batch()
        self.storage.create_batch(batch)
        got = self.storage.get_batch(batch.batch_id)
        self.assertIsNotNone(got)
        self.assertEqual(got.product_code, "P1")

    def test_get_by_request_id(self):
        batch = self._make_batch(request_id="OP_IDEM_001")
        self.storage.create_batch(batch)
        got = self.storage.get_batch_by_request_id("OP_IDEM_001")
        self.assertIsNotNone(got)

    def test_insert_and_get_item(self):
        batch = self._make_batch()
        self.storage.create_batch(batch)
        item = self._make_item(batch.batch_id, 1)
        self.storage.insert_item(item)
        items = self.storage.get_items(batch.batch_id)
        self.assertEqual(len(items), 1)
        frozen = json.loads(items[0].frozen_direction_package_json)
        self.assertEqual(frozen["creative_diversity_contract"]["contract_id"], "CDC_1")
        self.assertEqual(items[0].stage_checkpoint_json, "")

    def test_schema_migrates_existing_item_table(self):
        legacy_path = Path(self._tmpdir) / "legacy.sqlite3"
        with sqlite3.connect(str(legacy_path)) as conn:
            conn.execute("CREATE TABLE original_content_item (batch_item_id TEXT PRIMARY KEY)")
        legacy = BatchStorage(db_path=legacy_path)
        legacy.ensure_schema()
        with sqlite3.connect(str(legacy_path)) as conn:
            columns = {row[1] for row in conn.execute("PRAGMA table_info(original_content_item)")}
        self.assertIn("frozen_direction_package_json", columns)
        self.assertIn("stage_checkpoint_json", columns)

    def test_update_item_checkpoint_roundtrip(self):
        batch = self._make_batch()
        self.storage.create_batch(batch)
        item = self._make_item(batch.batch_id, 1)
        self.storage.insert_item(item)
        checkpoint = {
            "schema_version": "original-batch-stage-checkpoint-v1",
            "stages": {"blueprint": {"status": "INVALID", "normalized": {"x": 1}}},
        }
        self.storage.update_item_checkpoint(item.batch_item_id, checkpoint)
        got = self.storage.get_item(item.batch_item_id)
        self.assertEqual(json.loads(got.stage_checkpoint_json), checkpoint)

    def test_update_item_status(self):
        batch = self._make_batch()
        self.storage.create_batch(batch)
        item = self._make_item(batch.batch_id, 1)
        self.storage.insert_item(item)
        self.storage.update_item_status(item.batch_item_id, "SCRIPT_READY", actual_hook_id="DETAIL_SURPRISE", script_id="SCR_1")
        got = self.storage.get_item(item.batch_item_id)
        self.assertEqual(got.status, "SCRIPT_READY")
        self.assertEqual(got.actual_hook_id, "DETAIL_SURPRISE")

    def test_success_status_can_clear_previous_error(self):
        batch = self._make_batch()
        self.storage.create_batch(batch)
        item = self._make_item(batch.batch_id, 1)
        item.status = "SCRIPT_FAILED"
        item.error_code = "RUNTIME_ERROR"
        item.error_message = "old error"
        self.storage.insert_item(item)
        self.storage.update_item_status(
            item.batch_item_id,
            "SCRIPT_READY",
            error_code="",
            error_message="",
        )
        got = self.storage.get_item(item.batch_item_id)
        self.assertEqual(got.status, "SCRIPT_READY")
        self.assertEqual(got.error_code, "")
        self.assertEqual(got.error_message, "")

    def test_batch_update_status(self):
        batch = self._make_batch()
        self.storage.create_batch(batch)
        self.storage.update_batch_status(batch.batch_id, "SCRIPT_READY", ready_count=5)
        got = self.storage.get_batch(batch.batch_id)
        self.assertEqual(got.status, "SCRIPT_READY")
        self.assertEqual(got.ready_count, 5)


# ── Allocator tests ────────────────────────────────────────────────────

class BatchAllocatorTest(unittest.TestCase):
    def test_current_black_variant_defers_not_only_black_white_argument(self):
        result = _annotate_variant_fit(
            {
                "selling_argument": {
                    "operator_expression": "不只有黑白，这个颜色更有复古感",
                }
            },
            anchor_card={"product_name": "近黑色短款外套"},
        )

        self.assertEqual("DEFERRED", result["variant_fit_status"])
        self.assertEqual("VARIANT_MISMATCH", result["variant_fit_reason"])

    def test_current_black_variant_defers_pure_black_white_comparison(self):
        result = _annotate_variant_fit(
            {
                "selling_argument": {
                    "source_argument_id": "PCS_HUMAN_1",
                    "operator_expression": "区别于纯黑白，更有复古调性",
                }
            },
            anchor_card={"product_name": "近黑色短款外套"},
        )
        self.assertEqual("DEFERRED", result["variant_fit_status"])

    def test_unmapped_human_argument_keeps_authority_from_argument_id(self):
        result = _annotate_selling_argument_lineage(
            {
                "content_mode": "SELLING_ARGUMENT",
                "selling_argument": {
                    "source_argument_id": "PCS_HUMAN_1",
                    "source_claim_ids": [],
                    "mapping_status": "UNMAPPED",
                },
            },
            authoritative_catalog=True,
        )
        self.assertEqual("CONFIRMED", result["selling_argument_lineage"]["status"])
        self.assertEqual(
            "PCS_HUMAN_1",
            result["selling_argument_lineage"]["source_argument_id"],
        )

    def test_scene_usage_compiles_to_use_process_proof_intent(self):
        intent = _build_proof_execution_intent(
            {"selling_argument": {"proof_subject": "GENERAL_EXPRESSION"}},
            {"scene_request_contract": {"scene_intent": "SCENE_USAGE"}},
        )
        self.assertEqual("SCENE_USAGE", intent["proof_subject"])
        self.assertEqual(["use_process"], intent["required_any_parts"])

    def test_existing_commute_affinity_compiles_scene_usage(self):
        intent = _build_proof_execution_intent(
            {"selling_argument": {"proof_subject": "GENERAL_EXPRESSION"}},
            {
                "scene_request_contract": {"scene_intent": "GENERAL_USE"},
                "selling_scene_affinity_preferences": ["COMMUTE"],
            },
        )
        self.assertEqual("SCENE_USAGE", intent["proof_subject"])

    def test_style_theme_is_soft_relation_intent(self):
        intent = _build_proof_execution_intent(
            {
                "selling_argument": {
                    "proof_subject": "GENERAL_EXPRESSION",
                    "claim_theme": "style",
                }
            },
            {},
        )
        self.assertEqual("STYLE_RELATION", intent["proof_subject"])
        self.assertFalse(intent["hard_required_part_match"])

    def test_unselected_shirt_example_is_generalized_not_rejected(self):
        result = _build_argument_context_alignment(
            {
                "selling_argument": {
                    "operator_expression": "换不同颜色的衬衫和领型都能搭",
                }
            },
            {
                "outfit_selection_contract": {
                    "outfit_recipe": {"top": "白色修身短袖T恤"},
                }
            },
        )

        self.assertEqual(
            "GENERALIZE_UNSELECTED_STYLING_EXAMPLE", result["status"]
        )
        self.assertFalse(result["hard_required"])

    def test_wearable_five_script_batch_targets_four_creator_directions(self):
        directions = [
            _fake_direction("DA_W1", "S1", cluster_id=1, carrier="WEARER_ACTIVE"),
            _fake_direction("DA_W2", "S2", cluster_id=2, carrier="WEARER_ACTIVE"),
            _fake_direction("DA_H", "S3", cluster_id=3, carrier="HAND_ONLY"),
            _fake_direction("DA_S", "S4", cluster_id=4, carrier="STATIC_PRODUCT"),
        ]

        items, summary = allocate_batch_items(
            product_code="P1",
            requested_count=5,
            directions=directions,
            anchor_card=_fake_anchor_card(),
            active_hook_ids=[
                "DETAIL_SURPRISE", "AUDIENCE_NEED_CALLOUT",
                "DISCOVERY_RESULT_PROMISE", "GENERAL_PRODUCT_SHARE",
            ],
            creative_policy_version="test-v1",
            random_seed=42,
            selling_point_catalog=_fake_selling_catalog(),
            product_type="外套",
            top_category="女装",
            scene_reference_contexts={},
        )

        person_count = sum(
            item.carrier_mode in {"WEARER_ACTIVE", "MIXED", "PERSON_ON_CAMERA"}
            for item in items
        )
        self.assertEqual(len(items), 5)
        self.assertEqual(person_count, 4)
        self.assertEqual(
            summary["capture_mode_distribution"],
            {"CREATOR_SELF_SHOT": 4, "HANDS_PRODUCT_SHARE": 1},
        )

    def test_relationship_schedule_delegates_surface_language_to_hook(self):
        first = _relationship_schedule(10, __import__("random").Random(17))
        second = _relationship_schedule(10, __import__("random").Random(17))
        self.assertEqual(first, second)
        self.assertEqual(first, ["HOOK_DECIDES"] * 10)

        small = _relationship_schedule(3, __import__("random").Random(17))
        self.assertEqual(small, ["HOOK_DECIDES"] * 3)

    def test_relationship_device_is_hook_compatible_without_batch_address_spam(self):
        assigned = []
        for hook_id in (
            "USER_ADVOCACY_STANCE",
            "AUDIENCE_NEED_CALLOUT",
            "DETAIL_SURPRISE",
            "DISCOVERY_RESULT_PROMISE",
            "USER_ADVOCACY_STANCE",
        ):
            assigned.append(_relationship_device_for_hook(hook_id, assigned))
        self.assertEqual(assigned.count("AUDIENCE_ADDRESS"), 1)
        self.assertEqual(assigned[1], "VIEWER_REFERENCE")
        self.assertEqual(assigned[2], "VIEWER_INVITATION")
        self.assertFalse(any(
            left == right for left, right in zip(assigned, assigned[1:])
        ))

    def test_allocate_two_items_returns_two_structure_mothers(self):
        items, summary = allocate_batch_items(
            product_code="P1",
            requested_count=2,
            directions=[_fake_direction("DA1", "S1"), _fake_direction("DA2", "S4", cluster_id=2, carrier="STATIC_PRODUCT", macro_family="HOOK>PROOF>ENDING")],
            anchor_card=_fake_anchor_card(),
            active_hook_ids=["DETAIL_SURPRISE", "AUDIENCE_NEED_CALLOUT", "DISCOVERY_RESULT_PROMISE", "GENERAL_PRODUCT_SHARE", "USER_ADVOCACY_STANCE"],
            creative_policy_version="test-v1",
            random_seed=42,
            selling_point_catalog=_fake_selling_catalog(),
        )
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].item_role, "STRUCTURE_MOTHER")
        self.assertEqual(items[1].item_role, "STRUCTURE_MOTHER")
        frozen = json.loads(items[0].frozen_direction_package_json)
        self.assertEqual(
            frozen["simplified_creative_seed"]["schema_version"],
            "simplified-creative-seed-v25-apparel-action-soft-match",
        )
        self.assertIn(
            frozen["simplified_creative_seed"]["creative_direction"]["opening_visual_job"]["job"],
            {"SHOW_RESULT", "SHOW_DETAIL", "SHOW_USE_SCENE", "PRODUCT_FIRST"},
        )
        self.assertIn(
            frozen["simplified_creative_seed"]["voiceover_surface_contract"]["relationship_device"],
            {"AUDIENCE_ADDRESS", "VIEWER_REFERENCE", "VIEWER_INVITATION", "PERSONAL_STANCE", "NO_ADDRESS", "HOOK_DECIDES"},
        )
        self.assertTrue(
            frozen["simplified_creative_seed"]["product_truth"]["visible_detail_anchors"]
        )

    def test_allocate_four_items_covers_content_variants(self):
        items, summary = allocate_batch_items(
            product_code="P1",
            requested_count=4,
            directions=[_fake_direction("DA1", "S1"), _fake_direction("DA2", "S4", cluster_id=2, carrier="STATIC_PRODUCT", macro_family="HOOK>PROOF>ENDING")],
            anchor_card=_fake_anchor_card(),
            active_hook_ids=["DETAIL_SURPRISE", "AUDIENCE_NEED_CALLOUT", "DISCOVERY_RESULT_PROMISE", "GENERAL_PRODUCT_SHARE", "USER_ADVOCACY_STANCE", "VISUAL_RESULT_DIRECT"],
            creative_policy_version="test-v1",
            random_seed=42,
            selling_point_catalog=_fake_selling_catalog(),
        )
        self.assertEqual(len(items), 4)
        roles = [it.item_role for it in items]
        self.assertIn("CONTENT_VARIANT", roles)

    def test_six_item_batch_uses_selling_argument_breadth_before_repeating(self):
        catalog = [
            {
                "value_id": f"ARG_{index}",
                "primary_selling_point": f"用户价值方向{index}",
                "argument_kind": "SELLING_ARGUMENT",
                "source": "CENTRAL_VOICEOVER_VERIFIED_CLAIM",
                "claim_type": "benefit",
                "visual_dependency": "FLEXIBLE",
                "compatible_carriers": [],
            }
            for index in range(1, 7)
        ]
        items, summary = allocate_batch_items(
            product_code="P1",
            requested_count=6,
            directions=[
                _fake_direction("DA1", "S1"),
                _fake_direction(
                    "DA2", "S4", cluster_id=2,
                    carrier="STATIC_PRODUCT", macro_family="HOOK>PROOF>ENDING",
                ),
            ],
            anchor_card=_fake_anchor_card(),
            active_hook_ids=[
                "DETAIL_SURPRISE", "AUDIENCE_NEED_CALLOUT",
                "DISCOVERY_RESULT_PROMISE", "GENERAL_PRODUCT_SHARE",
                "USER_ADVOCACY_STANCE", "VISUAL_RESULT_DIRECT",
            ],
            creative_policy_version="test-v1",
            random_seed=42,
            selling_point_catalog=catalog,
        )

        self.assertEqual(len(items), 6)
        self.assertEqual(len({item.content_angle_key for item in items}), 6)
        self.assertEqual(summary["unique_angles"], 6)
        self.assertEqual({item.carrier_mode for item in items}, {"WEARER_ACTIVE", "STATIC_PRODUCT"})

    def test_verified_arguments_rotate_safe_hook_ids_and_families(self):
        catalog = [
            {
                "value_id": f"ARG_{index}",
                "primary_selling_point": f"已确认用户价值{index}",
                "argument_kind": "SELLING_ARGUMENT",
                "source": "FEISHU_OPERATOR_CONFIRMED_ARGUMENT",
                "claim_type": "benefit",
                "visual_dependency": "FLEXIBLE",
                "compatible_carriers": [],
            }
            for index in range(1, 7)
        ]
        items, summary = allocate_batch_items(
            product_code="P1",
            requested_count=6,
            directions=[
                _fake_direction("DA1", "S1"),
                _fake_direction(
                    "DA2", "S4", cluster_id=2,
                    carrier="STATIC_PRODUCT", macro_family="HOOK>PROOF>ENDING",
                ),
            ],
            anchor_card=_fake_anchor_card(),
            active_hook_ids=[
                "DISCOVERY_RESULT_PROMISE",
                "USER_ADVOCACY_STANCE",
                "GENERAL_PRODUCT_SHARE",
            ],
            creative_policy_version="test-v1",
            random_seed=42,
            selling_point_catalog=catalog,
        )

        self.assertEqual(len(items), 6)
        self.assertEqual(
            {item.requested_hook_id for item in items},
            {
                "DISCOVERY_RESULT_PROMISE",
                "GENERAL_PRODUCT_SHARE",
            },
        )
        self.assertEqual(summary["unique_hooks"], 2)
        self.assertEqual(summary["unique_hook_families"], 2)
        for item in items:
            frozen = json.loads(item.frozen_direction_package_json)
            contract = frozen["hook_allocation_contract"]
            self.assertEqual(contract["requested_hook_id"], item.requested_hook_id)
            self.assertEqual(contract["allocation_status"], "COMPATIBLE_ROTATION")

    def test_no_duplicate_allocation_signatures(self):
        items, summary = allocate_batch_items(
            product_code="P1",
            requested_count=10,
            directions=[_fake_direction("DA1", "S1"), _fake_direction("DA2", "S4", cluster_id=2, carrier="STATIC_PRODUCT", macro_family="HOOK>PROOF>ENDING")],
            anchor_card=_fake_anchor_card(),
            active_hook_ids=["DETAIL_SURPRISE", "AUDIENCE_NEED_CALLOUT", "DISCOVERY_RESULT_PROMISE", "GENERAL_PRODUCT_SHARE", "USER_ADVOCACY_STANCE", "VISUAL_RESULT_DIRECT"],
            creative_policy_version="test-v1",
            random_seed=42,
            selling_point_catalog=_fake_selling_catalog(),
        )
        sigs = [it.allocation_signature for it in items]
        self.assertEqual(len(sigs), len(set(sigs)), "Duplicate allocation signatures found")
        self.assertEqual(len(items), 4)
        self.assertEqual(summary["allocation_status"], "PARTIAL_CONTENT_CAPACITY")
        self.assertEqual(summary["requested_count"], 10)
        self.assertEqual(summary["shortage_count"], 6)

    def test_authorised_argument_is_allocated_even_without_visual_proof_match(self):
        items, summary = allocate_batch_items(
            product_code="P1",
            requested_count=1,
            directions=[_fake_direction("DA1", "S1")],
            anchor_card=_fake_anchor_card(),
            active_hook_ids=["GENERAL_PRODUCT_SHARE", "USER_ADVOCACY_STANCE"],
            creative_policy_version="test-v1",
            random_seed=42,
            selling_point_catalog=[{
                "value_id": "CENTRAL_COOLING_LAYER",
                "primary_selling_point": "适合作为降温环境的外搭",
                "argument_kind": "SELLING_ARGUMENT",
                "source": "CENTRAL_VOICEOVER_VERIFIED_CLAIM",
                "compatible_carriers": ["WEARER_ACTIVE", "MIXED"],
            }],
        )
        self.assertEqual(len(items), 1)
        bundle = json.loads(items[0].content_bundle_json)
        self.assertEqual(summary["allocation_status"], "COMPLETE")
        self.assertEqual(bundle["content_mode"], "SELLING_ARGUMENT")
        self.assertEqual(bundle["argument_readiness"], "READY")
        self.assertEqual(bundle["proof_match_status"], "UNMATCHED")
        self.assertTrue(bundle["original_15s_eligible"])

    def test_wearer_required_visual_result_is_deferred_for_static_structure(self):
        items, summary = allocate_batch_items(
            product_code="P1",
            requested_count=2,
            directions=[
                _fake_direction("DA1", "S1", carrier="WEARER_ACTIVE"),
                _fake_direction("DA2", "S4", cluster_id=2, carrier="STATIC_PRODUCT"),
            ],
            anchor_card=_fake_anchor_card(),
            active_hook_ids=["GENERAL_PRODUCT_SHARE", "VISUAL_RESULT_DIRECT"],
            creative_policy_version="test-v1",
            random_seed=42,
            selling_point_catalog=[{
                "value_id": "CENTRAL_LEG_LINE",
                "primary_selling_point": "腿部线条视觉更修长",
                "argument_kind": "SELLING_ARGUMENT",
                "source": "CENTRAL_VOICEOVER_VERIFIED_CLAIM",
                "visual_dependency": "WEARER_REQUIRED",
                "compatible_carriers": ["WEARER_ACTIVE", "MIXED"],
            }],
        )
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].carrier_mode, "WEARER_ACTIVE")
        self.assertEqual(summary["allocation_status"], "PARTIAL_CONTENT_CAPACITY")
        self.assertTrue(any(
            entry.get("downgrade_reason") == "WEARER_VISUAL_REQUIRED"
            and entry.get("output_slot") == "S4"
            for entry in summary["deferred_content"]
        ))

    def test_scarf_worn_usage_is_not_allocated_to_static_product_structure(self):
        items, summary = allocate_batch_items(
            product_code="P-SCARF",
            requested_count=2,
            directions=[
                _fake_direction("DA_W", "S1", carrier="WEARER_ACTIVE"),
                _fake_direction("DA_S", "S4", cluster_id=2, carrier="STATIC_PRODUCT"),
            ],
            anchor_card=_fake_anchor_card(),
            active_hook_ids=["AUDIENCE_NEED_CALLOUT", "GENERAL_PRODUCT_SHARE"],
            creative_policy_version="test-v1",
            random_seed=42,
            selling_point_catalog=[{
                "value_id": "ARG_SUN_SHADE",
                "primary_selling_point": "外出时作为头部遮阳造型",
                "argument_kind": "SELLING_ARGUMENT",
                "source": "FEISHU_OPERATOR_CONFIRMED_ARGUMENT",
                "argument_theme": "SUN_SHADE",
                "primary_demonstration_mode": "HEAD_WORN",
                "visual_dependency": "WEARER_REQUIRED",
                "proof_subject": "SCENE_USAGE",
                "compatible_carriers": [
                    "WEARER_ACTIVE", "PERSON_ON_CAMERA", "MIXED",
                ],
            }],
        )
        self.assertEqual(1, len(items))
        self.assertEqual("WEARER_ACTIVE", items[0].carrier_mode)
        self.assertEqual("PARTIAL_CONTENT_CAPACITY", summary["allocation_status"])
        self.assertTrue(any(
            entry.get("output_slot") == "S4"
            and entry.get("downgrade_reason") == "WEARER_VISUAL_REQUIRED"
            for entry in summary["deferred_content"]
        ))

    def test_explicit_product_variant_mismatch_is_deferred_before_generation(self):
        anchor = _fake_anchor_card()
        anchor["identity_anchors"] = ["当前参考商品为米白色短款外套"]
        items, summary = allocate_batch_items(
            product_code="P1",
            requested_count=1,
            directions=[_fake_direction("DA1", "S1")],
            anchor_card=anchor,
            active_hook_ids=["GENERAL_PRODUCT_SHARE"],
            creative_policy_version="test-v1",
            random_seed=42,
            selling_point_catalog=[{
                "value_id": "ARG_BLACK_STYLE",
                "primary_selling_point": "黑色款有酷感和机车感",
                "operator_expression": "黑色款有酷感和机车感",
                "argument_kind": "SELLING_ARGUMENT",
            }],
        )
        self.assertEqual(items, [])
        self.assertTrue(any(
            entry.get("downgrade_reason") == "VARIANT_MISMATCH"
            for entry in summary["deferred_content"]
        ))

    def test_unknown_variant_does_not_block_operator_argument(self):
        items, _summary = allocate_batch_items(
            product_code="P1",
            requested_count=1,
            directions=[_fake_direction("DA1", "S1")],
            anchor_card=_fake_anchor_card(),
            active_hook_ids=["GENERAL_PRODUCT_SHARE"],
            creative_policy_version="test-v1",
            random_seed=42,
            selling_point_catalog=[{
                "value_id": "ARG_BLACK_STYLE",
                "primary_selling_point": "黑色款有酷感和机车感",
                "operator_expression": "黑色款有酷感和机车感",
                "argument_kind": "SELLING_ARGUMENT",
            }],
        )
        self.assertEqual(len(items), 1)

    def test_two_arguments_fill_four_distinct_compatible_structures(self):
        catalog = [
            {
                "value_id": "ARG_LEG_LINE",
                "primary_selling_point": "腿部线条视觉更修长",
                "argument_kind": "SELLING_ARGUMENT",
                "visual_dependency": "WEARER_REQUIRED",
                "compatible_carriers": ["WEARER_ACTIVE", "MIXED"],
            },
            {
                "value_id": "ARG_BODY_EASE",
                "primary_selling_point": "版型对身形有视觉包容感",
                "argument_kind": "SELLING_ARGUMENT",
                "visual_dependency": "WEARER_REQUIRED",
                "compatible_carriers": ["WEARER_ACTIVE", "MIXED"],
            },
        ]
        directions = [
            _fake_direction(f"DA{index}", f"S{index}", cluster_id=index,
                            carrier="WEARER_ACTIVE" if index == 1 else "MIXED",
                            macro_family=f"HOOK>PROOF>{index}")
            for index in range(1, 5)
        ]
        items, summary = allocate_batch_items(
            product_code="P1",
            requested_count=4,
            directions=directions,
            anchor_card=_fake_anchor_card(),
            active_hook_ids=["GENERAL_PRODUCT_SHARE", "VISUAL_RESULT_DIRECT"],
            creative_policy_version="test-v1",
            random_seed=42,
            selling_point_catalog=catalog,
        )
        argument_counts = {}
        for item in items:
            bundle = json.loads(item.content_bundle_json)
            argument_id = bundle["selling_argument"]["argument_id"]
            argument_counts[argument_id] = argument_counts.get(argument_id, 0) + 1
        self.assertEqual(len(items), 4)
        self.assertEqual(len({item.cluster_id for item in items}), 4)
        self.assertEqual(sorted(argument_counts.values()), [2, 2])
        self.assertEqual(summary["allocation_status"], "COMPLETE")

    def test_one_argument_can_rotate_across_four_distinct_structures(self):
        catalog = [{
            "value_id": "ARG_ONLY",
            "primary_selling_point": "版型对身形有视觉包容感",
            "argument_kind": "SELLING_ARGUMENT",
            "visual_dependency": "WEARER_REQUIRED",
            "compatible_carriers": ["WEARER_ACTIVE", "MIXED"],
        }]
        items, summary = allocate_batch_items(
            product_code="P1",
            requested_count=4,
            directions=[
                _fake_direction(f"DA{index}", f"S{index}", cluster_id=index,
                                carrier="WEARER_ACTIVE" if index == 1 else "MIXED")
                for index in range(1, 5)
            ],
            anchor_card=_fake_anchor_card(),
            active_hook_ids=["GENERAL_PRODUCT_SHARE", "VISUAL_RESULT_DIRECT"],
            creative_policy_version="test-v1",
            random_seed=42,
            selling_point_catalog=catalog,
        )
        self.assertEqual(len(items), 4)
        self.assertEqual(len({item.cluster_id for item in items}), 4)
        self.assertEqual(summary["allocation_status"], "COMPLETE")

    def test_five_arguments_are_balanced_across_twelve_plans(self):
        catalog = [
            {
                "value_id": f"OPERATOR_S{index}",
                "source_argument_id": f"S{index}",
                "primary_selling_point": f"人工确认卖点{index}",
                "argument_kind": "SELLING_ARGUMENT",
                "source": "FEISHU_OPERATOR_CONFIRMED_ARGUMENT",
                "authority": "FEISHU_OPERATOR_CONFIRMED",
                "verification_status": "OPERATOR_CONFIRMED",
                "mapping_status": "UNMAPPED" if index > 2 else "MAPPED",
                "allowed_strength": "soft_only",
                "visual_dependency": "FLEXIBLE",
                "compatible_carriers": [],
            }
            for index in range(1, 6)
        ]
        directions = [
            _fake_direction(
                f"DA{index}", f"S{index}", cluster_id=index,
                carrier=("STATIC_PRODUCT" if index == 4 else "WEARER_ACTIVE"),
                macro_family=f"HOOK>PROOF>{index}",
            )
            for index in range(1, 5)
        ]
        items, summary = allocate_batch_items(
            product_code="P1",
            requested_count=12,
            directions=directions,
            anchor_card=_fake_anchor_card(),
            active_hook_ids=[
                "DETAIL_SURPRISE", "AUDIENCE_NEED_CALLOUT",
                "DISCOVERY_RESULT_PROMISE", "GENERAL_PRODUCT_SHARE",
                "USER_ADVOCACY_STANCE", "VISUAL_RESULT_DIRECT",
            ],
            creative_policy_version="test-v1",
            random_seed=42,
            selling_point_catalog=catalog,
        )
        counts = sorted(summary["selling_argument_distribution"].values())
        self.assertEqual(len(items), 12)
        self.assertEqual(summary["allocation_status"], "COMPLETE")
        self.assertEqual(summary["used_selling_argument_count"], 5)
        self.assertEqual(summary["selling_argument_usage_spread"], 1)
        self.assertEqual(counts, [2, 2, 2, 3, 3])
        self.assertEqual(len({item.allocation_signature for item in items}), 12)

    def test_flexible_benefit_remains_available_for_static_structure(self):
        items, summary = allocate_batch_items(
            product_code="P1",
            requested_count=2,
            directions=[
                _fake_direction("DA1", "S1", carrier="WEARER_ACTIVE"),
                _fake_direction("DA2", "S4", cluster_id=2, carrier="STATIC_PRODUCT"),
            ],
            anchor_card=_fake_anchor_card(),
            active_hook_ids=["GENERAL_PRODUCT_SHARE", "AUDIENCE_NEED_CALLOUT"],
            creative_policy_version="test-v1",
            random_seed=42,
            selling_point_catalog=[{
                "value_id": "CENTRAL_COOLING_LAYER",
                "primary_selling_point": "适合作为降温环境的外搭",
                "argument_kind": "SELLING_ARGUMENT",
                "source": "CENTRAL_VOICEOVER_VERIFIED_CLAIM",
                "visual_dependency": "FLEXIBLE",
                "compatible_carriers": [],
            }],
        )
        self.assertEqual(len(items), 2)
        self.assertEqual(summary["allocation_status"], "COMPLETE")
        self.assertEqual({item.carrier_mode for item in items}, {"WEARER_ACTIVE", "STATIC_PRODUCT"})

    def test_reproducible_with_same_seed(self):
        common_args = dict(
            product_code="P1",
            requested_count=4,
            directions=[_fake_direction("DA1", "S1"), _fake_direction("DA2", "S4", cluster_id=2, carrier="STATIC_PRODUCT", macro_family="HOOK>PROOF>ENDING")],
            anchor_card=_fake_anchor_card(),
            active_hook_ids=["DETAIL_SURPRISE", "AUDIENCE_NEED_CALLOUT", "DISCOVERY_RESULT_PROMISE", "GENERAL_PRODUCT_SHARE"],
            creative_policy_version="test-v1",
            random_seed=42,
        )
        items1, _ = allocate_batch_items(**common_args)
        items2, _ = allocate_batch_items(**common_args)
        sigs1 = [it.allocation_signature for it in items1]
        sigs2 = [it.allocation_signature for it in items2]
        self.assertEqual(sigs1, sigs2)

    def test_no_pain_reframe_when_tension_unavailable(self):
        items, _ = allocate_batch_items(
            product_code="P1",
            requested_count=3,
            directions=[_fake_direction("DA1", "S1")],
            anchor_card=_fake_anchor_card(),
            active_hook_ids=["DETAIL_SURPRISE", "PAIN_REFRAME", "AUDIENCE_NEED_CALLOUT"],
            creative_policy_version="test-v1",
            random_seed=42,
        )
        for it in items:
            self.assertNotEqual(it.requested_hook_id, "PAIN_REFRAME",
                f"PAIN_REFRAME should not be assigned when tension=UNAVAILABLE, got item {it.item_index}")

    def test_only_one_fact_does_not_fabricate_or_clone_second_direction(self):
        # Build content candidates with minimal anchor
        ref = {"execution_card_id": "EC_1", "source_video_id": "V_1", "content_carrier": "WEARER_ACTIVE"}
        candidates = build_content_bundle_candidates(
            _fake_anchor_card(),
            ref,
            product_type="外套",
            max_candidates=2,
        )
        self.assertEqual(len(candidates), 1)
        atoms = candidates[0].get("claim_atoms", [])
        self.assertGreaterEqual(len(atoms), 1)
        self.assertLessEqual(len(atoms), 4)
        self.assertFalse(candidates[0]["original_15s_eligible"])
        self.assertEqual(candidates[0]["recommended_flow"], "LIGHT_VIDEO_OR_MIXCUT")

    def test_item_roles_are_valid(self):
        items, summary = allocate_batch_items(
            product_code="P1",
            requested_count=5,
            directions=[_fake_direction("DA1", "S1"), _fake_direction("DA2", "S4", cluster_id=2, carrier="STATIC_PRODUCT", macro_family="HOOK>PROOF>ENDING")],
            anchor_card=_fake_anchor_card(),
            active_hook_ids=["DETAIL_SURPRISE", "AUDIENCE_NEED_CALLOUT", "DISCOVERY_RESULT_PROMISE", "GENERAL_PRODUCT_SHARE", "USER_ADVOCACY_STANCE", "VISUAL_RESULT_DIRECT"],
            creative_policy_version="test-v1",
            random_seed=42,
        )
        for it in items:
            self.assertIn(it.item_role, ITEM_ROLES)

    def test_each_item_has_required_ids(self):
        items, _ = allocate_batch_items(
            product_code="P1",
            requested_count=2,
            directions=[_fake_direction("DA1", "S1"), _fake_direction("DA2", "S4", cluster_id=2, carrier="STATIC_PRODUCT", macro_family="HOOK>PROOF>ENDING")],
            anchor_card=_fake_anchor_card(),
            active_hook_ids=["DETAIL_SURPRISE", "AUDIENCE_NEED_CALLOUT"],
            creative_policy_version="test-v1",
            random_seed=42,
        )
        for it in items:
            self.assertTrue(it.direction_assignment_id)
            self.assertTrue(it.allocation_signature)
            self.assertTrue(it.requested_hook_id)
            self.assertTrue(it.content_bundle_id)
            self.assertTrue(it.item_snapshot_hash)
            frozen = json.loads(it.frozen_direction_package_json)
            self.assertEqual(frozen["schema_version"], "original-frozen-direction-package-v1")
            self.assertEqual(
                frozen["creative_diversity_contract"]["contract_id"],
                it.creative_contract_id,
            )
            self.assertIn("semantic_spine_contract", frozen)
            self.assertIn("context_bridge_contract", frozen)
            self.assertEqual(
                frozen["content_bundle_brief"]["semantic_spine_contract"],
                frozen["semantic_spine_contract"],
            )

    def test_eligible_hooks_exclude_tension_dependent_archetypes_without_tension(self):
        bundle = {
            "eligible_hook_ids": [
                "AUDIENCE_NEED_CALLOUT", "PAIN_REFRAME",
                "USER_ADVOCACY_STANCE", "DETAIL_SURPRISE",
            ],
            "audience_tension_status": "UNAVAILABLE",
        }
        eligible, suppressed = _eligible_hooks_for_bundle(
            bundle,
            [
                "AUDIENCE_NEED_CALLOUT", "PAIN_REFRAME",
                "USER_ADVOCACY_STANCE", "DETAIL_SURPRISE",
            ],
        )
        self.assertEqual(eligible, ["DETAIL_SURPRISE"])
        self.assertEqual(
            suppressed,
            ["AUDIENCE_NEED_CALLOUT", "PAIN_REFRAME", "USER_ADVOCACY_STANCE"],
        )

    def test_eligible_hooks_allows_pain_reframe_with_tension(self):
        bundle = {
            "eligible_hook_ids": ["PAIN_REFRAME", "DETAIL_SURPRISE"],
            "audience_tension_status": "AVAILABLE",
        }
        eligible, _ = _eligible_hooks_for_bundle(bundle, ["PAIN_REFRAME", "DETAIL_SURPRISE"])
        self.assertIn("PAIN_REFRAME", eligible)

    def test_eligible_hooks_reads_nested_content_bundle_tension(self):
        bundle = {
            "preferred_hook_angles": ["PAIN_REFRAME", "AUDIENCE_NEED_CALLOUT", "DETAIL_SURPRISE"],
            "audience_tension": {
                "status": "AVAILABLE",
                "text": "基础穿搭怎么快速有层次？",
            },
        }
        eligible, _ = _eligible_hooks_for_bundle(
            bundle,
            ["PAIN_REFRAME", "AUDIENCE_NEED_CALLOUT", "DETAIL_SURPRISE"],
        )
        self.assertIn("AUDIENCE_NEED_CALLOUT", eligible)
        self.assertIn("PAIN_REFRAME", eligible)

    def test_eligible_hooks_accepts_governed_central_concept_authority(self):
        bundle = {
            "preferred_hook_angles": [
                "PAIN_REFRAME", "AUDIENCE_NEED_CALLOUT", "GENERAL_PRODUCT_SHARE",
            ],
            "audience_tension_status": "UNAVAILABLE",
            "hook_tension_authority": "CENTRAL_CONCEPT",
        }
        eligible, suppressed = _eligible_hooks_for_bundle(
            bundle,
            ["PAIN_REFRAME", "AUDIENCE_NEED_CALLOUT", "GENERAL_PRODUCT_SHARE"],
        )
        self.assertIn("PAIN_REFRAME", eligible)
        self.assertIn("AUDIENCE_NEED_CALLOUT", eligible)
        self.assertEqual([], suppressed)

    def test_scarf_support_prefers_hand_over_static_without_changing_apparel(self):
        directions = [
            _fake_direction("DA_W", "S1", carrier="WEARER_ACTIVE"),
            _fake_direction("DA_S", "S4", carrier="STATIC_PRODUCT"),
            _fake_direction("DA_H", "S3", carrier="HAND_ONLY"),
        ]
        scarf = _creator_weighted_directions(
            directions,
            requested_count=5,
            product_type="丝巾",
            top_category="配饰",
        )
        apparel = _creator_weighted_directions(
            directions,
            requested_count=5,
            product_type="外套",
            top_category="女装",
        )
        scarf_ids = [item["direction_assignment_id"] for item in scarf]
        apparel_ids = [item["direction_assignment_id"] for item in apparel]
        self.assertIn("DA_H", scarf_ids)
        self.assertNotIn("DA_S", scarf_ids)
        self.assertIn("DA_S", apparel_ids)
        self.assertNotIn("DA_H", apparel_ids)

    def test_static_direction_may_use_authorised_wearer_preferred_argument(self):
        items, summary = allocate_batch_items(
            product_code="P1",
            requested_count=1,
            directions=[_fake_direction("DA_STATIC", "S4", carrier="STATIC_PRODUCT")],
            anchor_card=_fake_anchor_card(),
            active_hook_ids=["DETAIL_SURPRISE", "GENERAL_PRODUCT_SHARE"],
            creative_policy_version="test-v1",
            random_seed=42,
            selling_point_catalog=[{
                "value_id": "CENTRAL_WEARER_ONLY",
                "primary_selling_point": "腰线视觉更清晰",
                "argument_kind": "SELLING_ARGUMENT",
                "source": "CENTRAL_VOICEOVER_VERIFIED_CLAIM",
                "compatible_carriers": ["WEARER_ACTIVE", "MIXED"],
            }],
        )
        self.assertEqual(len(items), 1)
        bundle = json.loads(items[0].content_bundle_json)
        self.assertEqual(summary["allocation_status"], "COMPLETE")
        self.assertEqual(bundle["content_mode"], "SELLING_ARGUMENT")
        self.assertTrue(bundle["original_15s_eligible"])


# ── Idempotency test ───────────────────────────────────────────────────

class BatchIdempotencyTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self.db_path = Path(self._tmpdir) / "test_batch.sqlite3"
        self.storage = BatchStorage(db_path=self.db_path)
        self.storage.ensure_schema()

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_plan_only_idempotent(self):
        from core.original_batch_executor import run_plan_only

        # Mock load_product_context and build_reality_direction_packages
        with patch("core.original_batch_executor.load_product_context") as mock_ctx, \
             patch("core.reality_reference.build_reality_direction_packages") as mock_pkg:

            mock_ctx.return_value = {
                "source_run_id": 1, "source_record_id": "rec_test", "input_hash": "hash1",
                "product_code": "P_TEST", "target_country": "泰国", "target_language": "泰语",
                "product_type": "外套", "top_category": "女装",
                "anchor_card": _fake_anchor_card(),
                "structure_route": {"selection_run_id": "SR_TEST", "assignments": [
                    {"direction_assignment_id": "DA1", "output_slot": "S1", "cluster_id": 1, "cluster_version": "v1", "evidence_tier": "BOOTSTRAP", "structure_contract": _fake_direction("DA1", "S1")["structure_contract"]},
                    {"direction_assignment_id": "DA2", "output_slot": "S4", "cluster_id": 2, "cluster_version": "v1", "evidence_tier": "BOOTSTRAP", "structure_contract": _fake_direction("DA2", "S4", cluster_id=2, carrier="STATIC_PRODUCT", macro_family="HOOK>PROOF>ENDING")["structure_contract"]},
                ]},
                "selling_point_catalog": [], "product_selling_note": "",
            }
            mock_pkg.return_value = {"directions": [
                _fake_direction("DA1", "S1"),
                _fake_direction("DA2", "S4", cluster_id=2, carrier="STATIC_PRODUCT", macro_family="HOOK>PROOF>ENDING"),
            ]}

            request = BatchRequest(
                request_id="OP_IDEM_TEST",
                product_code="P_TEST",
                requested_count=2,
                test_phase="INITIAL",
                random_seed=42,
            )

            batch1, items1, _ = run_plan_only(request)
            batch2, items2, _ = run_plan_only(request)

            self.assertEqual(batch1.batch_id, batch2.batch_id, "Idempotent plan should return same batch")
            self.assertEqual(len(items1), len(items2))


class FrozenSeedMixedContractTest(unittest.TestCase):
    """The frozen creative seed must own the authored per-shot contract.

    The script stage consumes ``frozen["simplified_creative_seed"]`` verbatim --
    it never rebuilds the seed when the batch already carries one.  A contract
    injected into the frozen package *after* the seed was built therefore never
    reached the generator: the plan looked correct, and the generated script
    silently fell back to the legacy single-carrier framing advice.
    """

    def _accessory_anchor_card(self) -> dict:
        return {
            "hard_anchors": [{"anchor": "蝴蝶造型", "why_must_show": "商品主体造型"}],
            "display_anchors": [
                {"anchor": "细链条", "why_must_show": "耳线结构"},
                {"anchor": "满钻面", "why_must_show": "表面细节"},
            ],
            "category_execution_contract": {"display_family": "accessory"},
        }

    def _allocate(self, gate: str):
        with patch.dict(
            os.environ, {"ORIGINAL_SCRIPT_ACCESSORY_MIXED_TEMPLATE_V1_ENABLED": gate},
            clear=False,
        ):
            return allocate_batch_items(
                product_code="P_ACC",
                requested_count=1,
                directions=[_fake_direction("DA_ACC", "S1", carrier="WEARER_ACTIVE")],
                anchor_card=self._accessory_anchor_card(),
                active_hook_ids=["DETAIL_SURPRISE"],
                creative_policy_version="test-v1",
                random_seed=42,
                selling_point_catalog=_fake_selling_catalog(),
                product_type="耳饰",
                top_category="配饰",
            )

    def test_gate_on_freezes_the_contract_inside_the_seed(self):
        items, _ = self._allocate("1")
        frozen = json.loads(items[0].frozen_direction_package_json)

        seed_extension = frozen["simplified_creative_seed"]["category_execution_extension"]
        seed_contract = seed_extension.get("mixed_template_contract")
        package_contract = frozen["category_execution_extension"][
            "mixed_template_contract"
        ]

        self.assertTrue(seed_contract, "冻结晶种必须自带逐镜合同")
        self.assertEqual(
            seed_contract,
            package_contract,
            "种子内合同必须与冻结包内合同逐字一致",
        )
        self.assertEqual(frozen["mixed_template_contract_status"], "FROZEN")

    def test_gate_off_leaves_the_seed_extension_untouched(self):
        items, _ = self._allocate("0")
        frozen = json.loads(items[0].frozen_direction_package_json)

        seed_extension = (
            frozen["simplified_creative_seed"].get("category_execution_extension") or {}
        )
        self.assertNotIn("mixed_template_contract", seed_extension)
        self.assertNotIn(
            "mixed_template_contract", frozen.get("category_execution_extension") or {}
        )
        self.assertNotIn("mixed_template_contract_status", frozen)


# ── Stage E: final-shot difference judgement (Review #3) ───────────────
#
# T16/T17 fixtures.  Kept self-contained: the accessory anchor/catalogue pair
# below is what the mixed compiler actually validates, so a change in the
# women's-wear fixtures above cannot silently move these assertions.

_ACC_ANCHOR_CARD = {
    "hard_anchors": [{"anchor": "蝴蝶造型", "why_must_show": "商品主体造型"}],
    "display_anchors": [
        {"anchor": "镂空轮廓", "why_must_show": "耳饰外轮廓"},
        {"anchor": "耳线长度比例", "why_must_show": "佩戴比例"},
    ],
    "category_execution_contract": {"display_family": "accessory"},
}

_ACC_CATALOG = [
    {
        "value_id": "ARG_HOLLOW",
        "primary_selling_point": "镂空轮廓让耳饰边缘更清楚",
        "proof_thesis": "镂空轮廓在近景里可以看清边缘",
        "truth_status": "VERIFIED",
        "visual_dependency": "WEARER_REQUIRED",
        "argument_kind": "SELLING_ARGUMENT",
    },
    {
        "value_id": "ARG_LENGTH",
        "primary_selling_point": "耳线长度比例修饰脸型",
        "proof_thesis": "耳线长度比例在侧脸关系里可见",
        "truth_status": "VERIFIED",
        "visual_dependency": "WEARER_REQUIRED",
        "argument_kind": "SELLING_ARGUMENT",
    },
]


def _acc_direction(da_id: str, output_slot: str) -> dict:
    return {
        "direction_assignment_id": da_id,
        "output_slot": output_slot,
        "selection_run_id": "SR_ACC",
        "cluster_id": 1,
        "cluster_version": "v1",
        "evidence_tier": "BOOTSTRAP",
        "structure_contract": {
            "direction_identity": {"macro_family_key": "HOOK>PROOF>RESULT"},
            "hard_constraints": {
                "content_carrier": "WEARER_ACTIVE",
                "continuity_mode": "MULTI_CUT",
            },
            "evidence": {"evidence_tier": "BOOTSTRAP"},
        },
        "execution_reference": {
            "execution_card_id": f"EC_{da_id}",
            "source_video_id": "V_ACC",
            "content_carrier": "WEARER_ACTIVE",
        },
        "country": "泰国",
        "category": "配饰",
    }


def _acc_allocate(requested_count: int, *, recent_usage=None, exclusions=None):
    with patch.dict(os.environ, {_MIXED_GATE_ENV: "1"}, clear=False):
        return allocate_batch_items(
            product_code="P_ACC",
            requested_count=requested_count,
            directions=[_acc_direction("DA_ACC", "S1"), _acc_direction("DA_ACC2", "S2")],
            anchor_card=dict(_ACC_ANCHOR_CARD),
            active_hook_ids=["DETAIL_SURPRISE", "AUDIENCE_NEED_CALLOUT"],
            creative_policy_version="test-v1",
            random_seed=42,
            recent_creative_usage=recent_usage,
            selling_point_catalog=list(_ACC_CATALOG),
            product_type="耳饰",
            top_category="配饰",
            execution_scope=None,
            mixed_history_exclusions=exclusions,
        )


def _acc_contract(template_id: str, recipe_id: str = "") -> dict:
    return compile_mixed_template_contract(
        product_type="耳饰",
        top_category="饰品",
        template_id=template_id,
        environment_recipe_id=recipe_id,
        content_theme={
            "theme_id": "TH_1",
            "thesis": "怕买了不会戴",
            "approved_claim_refs": ["C1"],
            "evidence_refs": ["C1"],
        },
    )


def _acc_history_row(contract: dict, product_code: str = "P_ACC") -> dict:
    """One ledger row as it is really written.

    ``product_code`` is not optional in production (``creative_usage_row`` always
    writes it) and the mixed history filter needs it: content-level hard
    de-duplication applies within one product only (I2).  A fixture without it
    would be testing a row shape that cannot occur.
    """

    return {
        "usage_id": f"CPU_{contract.get('template_id')}",
        "product_code": product_code,
        "visual_signature": "OLD_SCENE_SIG|WHICH|MUST|NOT|BE|REUSED",
        "scene_motif": "OLD_SCENE",
        "persona_role": "OLD_PERSONA",
        "metadata": {
            "batch_item_id": f"HIST_{contract.get('template_id')}",
            MIXED_HISTORY_METADATA_KEY: mixed_signature_bundle(contract),
        },
    }


def _acc_contract_of(item) -> dict:
    frozen = json.loads(item.frozen_direction_package_json or "{}")
    extension = frozen.get("category_execution_extension") or {}
    return extension.get("mixed_template_contract") or {}


def _mixed_rejections(summary) -> list:
    return [
        row
        for row in summary.get("deferred_content") or []
        if str(row.get("downgrade_reason") or "").startswith("MIXED_")
    ]


class MixedDifferenceAccountingTest(unittest.TestCase):
    """T16: 请求 20 条、只有少量足够差异候选时，按实报告不足。

    The mechanism must never paper over a shortage with randomly re-worded
    scripts, and it must not keep retrying until the quota is full either.
    """

    def test_a_shortage_is_reported_instead_of_synthesised(self):
        items, summary = _acc_allocate(20)

        self.assertEqual(summary["requested_count"], 20)
        self.assertLess(summary["planned_count"], 20, "差异不足时必须少交付")
        self.assertEqual(summary["planned_count"], len(items))
        self.assertEqual(summary["shortage_count"], 20 - len(items))
        self.assertEqual(summary["mixed_shortage_reason"], "DIFFERENCE_INSUFFICIENT")
        self.assertEqual(summary["allocation_status"], "PARTIAL_CONTENT_CAPACITY")
        self.assertEqual(
            (summary["mixed_history_coverage"] or {}).get("scope_status"), "IN_SCOPE",
        )

        # 每条交付的脚本都要有可核查的比较依据，并且最终镜头互不相同。
        digests = set()
        for item in items:
            contract = _acc_contract_of(item)
            report = contract.get("difference_report") or {}
            self.assertIn(report.get("review_status"), {"DISTINCT_THEME", "EXECUTION_VARIANT"})
            self.assertTrue(report.get("counts_as_independent"))
            digests.add(mixed_visual_signature(contract)["digest"])
        self.assertEqual(len(digests), len(items), "交付的最终镜头不得互相重复")

        self.assertEqual(
            summary["mixed_usable_count"], len(items),
            "可用数必须等于实际交付条目数（不补随机同义脚本）",
        )
        self.assertEqual(
            summary["mixed_distinct_theme_count"]
            + summary["mixed_execution_variant_count"],
            len(items),
            "同一候选不得同时落进两个桶",
        )

    def test_rejected_candidates_are_counted_once_each(self):
        _items, summary = _acc_allocate(20)
        rejected = _mixed_rejections(summary)
        self.assertTrue(rejected, "差异不足必须留下可核查的剔除记录")

        keys = {
            (
                row.get("direction_assignment_id"),
                row.get("output_slot"),
                row.get("content_bundle_id"),
                row.get("downgrade_reason"),
                (row.get("difference_report") or {}).get("review_status"),
            )
            for row in rejected
        }
        self.assertEqual(
            len(keys), len(rejected), "同一候选不得重复累计",
        )
        self.assertEqual(
            summary["mixed_duplicate_rejected_count"]
            + summary["mixed_insufficient_evidence_count"],
            len(rejected),
        )
        self.assertLessEqual(
            len(rejected), 5,
            "差异不足时不得反复重试去填满名额",
        )
        for row in rejected:
            self.assertEqual(row.get("recommended_flow"), "REPLAN_MIXED_THEME")
            report = row.get("difference_report") or {}
            self.assertTrue(report.get("difference_summary"), "剔除必须给出理由")
            self.assertTrue(report.get("difference_dimensions"), "剔除必须给出比较依据")
            self.assertFalse(report.get("counts_as_independent"))


class MixedReservationAndResumeTest(unittest.TestCase):
    """T17: 历史重复被排除；重复/并发预留不产生新内容；resume 幂等复用。"""

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self.addCleanup(
            lambda: __import__("shutil").rmtree(self._tmpdir, ignore_errors=True)
        )
        self.db_path = Path(self._tmpdir) / "mixed.sqlite3"

    def test_a_history_row_alone_blocks_the_same_montage(self):
        history = [
            _acc_history_row(_acc_contract(template_id, select_environment_recipe_id(0)))
            for template_id in mixed_template_ids()
        ]
        items, summary = _acc_allocate(1, recent_usage=history)

        self.assertEqual(items, [], "整条历史已占用时不得再产出同款蒙太奇")
        self.assertEqual(summary["planned_count"], 0)
        self.assertEqual(summary["mixed_usable_count"], 0)
        self.assertEqual(
            summary["mixed_duplicate_rejected_count"], len(_mixed_rejections(summary)),
        )
        self.assertEqual(summary["mixed_shortage_reason"], "DIFFERENCE_INSUFFICIENT")

        report = _mixed_rejections(summary)[0]["difference_report"]
        self.assertEqual(report["review_status"], "EXACT_DUPLICATE")
        self.assertEqual(report["comparison_scope"], "BATCH_AND_HISTORY")
        self.assertEqual(
            report["nearest_script_id"], f"HIST_{select_template_id(0)}",
            "必须指出重复的是历史里的哪一条",
        )
        self.assertFalse(report["counts_as_independent"])
        self.assertEqual(
            (summary["mixed_history_coverage"] or {}).get("history_compared"), 3,
        )

    def test_incomplete_history_is_counted_but_never_used_as_a_reference(self):
        legacy = [
            {
                "usage_id": "CPU_LEGACY",
                "visual_signature": "A|B|C|D|E|F",
                "scene_motif": "OLD_SCENE",
                "persona_role": "OLD_PERSONA",
            }
        ]
        items, summary = _acc_allocate(2, recent_usage=legacy)
        coverage = summary["mixed_history_coverage"]

        self.assertEqual(coverage["history_compared"], 0)
        self.assertEqual(coverage["history_incomplete"], 1, "缺最终镜头必须标注")
        self.assertEqual(coverage["history_rows_seen"], 1)
        self.assertEqual(
            len(items), 2, "不完整的历史记录不得被当作\"已比对\"而阻断新内容",
        )
        self.assertEqual(summary["mixed_duplicate_rejected_count"], 0)
        for item in items:
            self.assertTrue(_acc_contract_of(item).get("difference_report"))

    def test_cross_product_history_never_exhausts_the_templates(self):
        """I2: 不同商品不得互相耗尽 A/B/C 模板。

        同一套 A/B/C 蒙太奇在目录里跨商品重复出现是**预期的**；跨商品复用属于
        账号风格/排期问题，不是重复成片。所以只有同商品的历史参与内容硬去重，
        而跨商品的行必须**单独可见**，不能被读成"已比对、未发现重复"。
        """

        # 别款商品把三种模板 + 本 index 会用的光影全部占用。
        other = [
            _acc_history_row(
                _acc_contract(template_id, select_environment_recipe_id(0)),
                product_code="P_OTHER",
            )
            for template_id in mixed_template_ids()
        ]

        references, skipped = _mixed_history_references(other, "P_ACC")
        self.assertEqual(references, [], "别款商品的历史不作本商品的内容硬去重引用")
        self.assertEqual(skipped["other_product"], len(mixed_template_ids()))
        self.assertEqual(
            skipped["incomplete"], 0,
            "它们完整、只是不同商品，不得被记成『历史不完整』",
        )

        items, summary = _acc_allocate(1, recent_usage=other)
        self.assertTrue(items, "别款商品用过同一模板不得使本商品规划出 0 条")
        self.assertEqual(summary["mixed_duplicate_rejected_count"], 0)
        coverage = summary["mixed_history_coverage"]
        self.assertEqual(coverage["history_compared"], 0)
        self.assertEqual(coverage["history_other_product"], len(mixed_template_ids()))
        self.assertEqual(coverage["product_code"], "P_ACC")

    def test_same_product_history_still_hard_deduplicates(self):
        """修 I2 不能把真正的同商品去重一起打掉。"""

        mine = [
            _acc_history_row(
                _acc_contract(template_id, select_environment_recipe_id(0)),
                product_code="P_ACC",
            )
            for template_id in mixed_template_ids()
        ]
        items, summary = _acc_allocate(1, recent_usage=mine)
        self.assertEqual(items, [], "同商品、同模板、同光影的历史仍须硬去重")
        self.assertGreater(summary["mixed_duplicate_rejected_count"], 0)

    def test_the_same_candidate_reserved_twice_is_still_one_duplicate(self):
        # 重复/并发预留同一个候选，不得让它变成"多一条独立内容"。

        # 每个模板都写了两条等价的历史（模拟并发或失败重试各写了一次）。
        rows = [
            _acc_history_row(_acc_contract(template_id, select_environment_recipe_id(0)))
            for template_id in mixed_template_ids()
            for _ in range(2)
        ]

        references, skipped = _mixed_history_references(rows, "P_ACC")
        self.assertEqual((len(references), skipped["incomplete"]), (6, 0))
        self.assertEqual(skipped["other_product"], 0)

        contract = _acc_contract("AMX_A_WORN_FIRST", select_environment_recipe_id(0))
        report = judge_mixed_candidate(contract, references)
        self.assertEqual(report["review_status"], "EXACT_DUPLICATE")
        self.assertFalse(report["counts_as_independent"])

        items, summary = _acc_allocate(1, recent_usage=rows)
        self.assertEqual(items, [], "两条等价历史不得让候选看起来还是新的")
        self.assertEqual(summary["mixed_usable_count"], 0)

        # 历史条目翻倍不得把剔除数也翻倍。
        single, single_summary = _acc_allocate(1, recent_usage=rows[::2])
        self.assertEqual(single, [])
        self.assertEqual(
            summary["mixed_duplicate_rejected_count"],
            single_summary["mixed_duplicate_rejected_count"],
            "同一候选被预留两次也只能算一条重复",
        )

    def test_concurrent_reservations_keep_every_signature_intact(self):
        storage = PipelineStorage(db_path=self.db_path)
        contract = _acc_contract("AMX_B_FORM_FIRST")
        signature = mixed_signature_bundle(contract)
        rows = [
            {
                "usage_id": f"CPU_CONC_{index}",
                "product_code": "P_ACC",
                "country": "泰国",
                "category": "配饰",
                "status": "RESERVED",
                "visual_signature": "LEGACY_SIG",
                "metadata": {
                    "batch_item_id": f"ITEM_CONC_{index}",
                    MIXED_HISTORY_METADATA_KEY: signature,
                },
            }
            for index in range(4)
        ]
        errors: list = []

        def _reserve(row):
            try:
                storage.reserve_creative_pattern(row)
            except Exception as exc:  # noqa: BLE001 - recorded and asserted below
                errors.append(exc)

        threads = [threading.Thread(target=_reserve, args=(row,)) for row in rows]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [], "并发预留不得丢行或损坏写入")
        stored = storage.list_recent_creative_patterns(country="泰国", category="配饰")
        self.assertEqual(len(stored), len(rows))
        for row in stored:
            reference = mixed_reference_signature(row)
            self.assertTrue(
                reference["complete"], "台账读取路径必须能取回最终镜头签名",
            )
            self.assertEqual(reference["signature"], signature)

    def test_resume_reuses_the_settled_batch_and_its_reservation(self):
        from core.original_batch_executor import run_plan_only

        storage = PipelineStorage(db_path=self.db_path)
        request = BatchRequest(
            request_id="OP_MIXED_RESUME",
            product_code="P_ACC",
            requested_count=2,
            test_phase="INITIAL",
            duration_seconds=15.0,
            script_mode="simplified_v1",
            random_seed=42,
        )
        directions = [_acc_direction("DA_ACC", "S1"), _acc_direction("DA_ACC2", "S2")]
        context = {
            "source_run_id": 1,
            "source_record_id": "rec_acc",
            "input_hash": "hash_acc",
            "product_code": "P_ACC",
            "target_country": "泰国",
            "target_language": "泰语",
            "product_type": "耳饰",
            "top_category": "配饰",
            "anchor_card": dict(_ACC_ANCHOR_CARD),
            "selling_point_catalog": list(_ACC_CATALOG),
            "product_selling_note": "",
            "structure_route": {
                "selection_run_id": "SR_ACC",
                "assignments": [
                    {
                        "direction_assignment_id": direction["direction_assignment_id"],
                        "output_slot": direction["output_slot"],
                        "cluster_id": 1,
                        "cluster_version": "v1",
                        "evidence_tier": "BOOTSTRAP",
                        "structure_contract": direction["structure_contract"],
                    }
                    for direction in directions
                ],
            },
        }
        env = {
            _MIXED_GATE_ENV: "1",
            "ORIGINAL_SCRIPT_GENERATOR_DB_PATH": str(self.db_path),
            "OPENCLAW_SHARED_DATA_DIR": self._tmpdir,
        }

        with patch.dict(os.environ, env, clear=False), patch(
            "core.structure_router_adapter.select_original_structure_directions",
            return_value=context["structure_route"],
        ), patch(
            "core.reality_reference.build_reality_direction_packages",
            return_value={"directions": directions},
        ), patch(
            "core.original_batch_executor.allocate_batch_items",
            wraps=allocate_batch_items,
        ) as planner:
            first_batch, first_items, _ = run_plan_only(
                request, product_context_override=context,
            )
            reserved_once = storage.list_recent_creative_patterns(
                country="泰国", category="配饰",
            )
            second_batch, second_items, _ = run_plan_only(
                request, product_context_override=context,
            )
            reserved_twice = storage.list_recent_creative_patterns(
                country="泰国", category="配饰",
            )

        self.assertTrue(first_items, "混合模式下必须产出脚本")
        for item in first_items:
            self.assertTrue(
                _acc_contract_of(item), "交付条目必须带冻结的混合合同",
            )

        self.assertEqual(first_batch.batch_id, second_batch.batch_id)
        self.assertEqual(
            [item.batch_item_id for item in first_items],
            [item.batch_item_id for item in second_items],
        )
        self.assertEqual(reserved_once and len(reserved_once), len(first_items))
        self.assertEqual(
            len(reserved_twice), len(reserved_once),
            "resume 必须复用自身预留，不得重复占用名额",
        )
        self.assertEqual(planner.call_count, 1, "resume 不得重新规划")

        for row in reserved_once:
            reference = mixed_reference_signature(row)
            self.assertTrue(
                reference["complete"],
                "预留台账必须带上最终镜头签名，下一批才能按镜头比对",
            )
            self.assertTrue(reference["identity"])


class MixedHistoryOwnershipTest(unittest.TestCase):
    """B4: 恢复时历史不得包含本批次自己。

    重跑一个批次会重新规划它。它上一次已经写进台账的记录会被当成"历史"读回来，
    于是每条候选都在和自己的前世比较、全部读成重复，批次交付 0 条 —— Review R3。
    同时**不能**为了排除自己就把整个当前批次排除掉：同批兄弟是合法的比较对象
    （执行变体就是这么被发现的），它走的是运行内的预留引用列表。
    """

    _SELF_BATCH = "B_ACC_SELF"

    def _own_rows(self):
        rows = [
            _acc_history_row(
                _acc_contract(template_id, select_environment_recipe_id(0))
            )
            for template_id in mixed_template_ids()
        ]
        for row in rows:
            row["metadata"]["batch_id"] = self._SELF_BATCH
        return rows

    def test_own_rows_are_excluded_and_counted(self):
        rows = self._own_rows()
        references, skipped = _mixed_history_references(
            rows, "P_ACC", exclusions={"batch_ids": [self._SELF_BATCH]}
        )
        self.assertEqual(references, [], "本批次自己写的台账记录不是历史")
        self.assertEqual(skipped["own_record"], len(mixed_template_ids()))
        self.assertEqual(skipped["incomplete"], 0)
        self.assertEqual(skipped["other_product"], 0)
        self.assertEqual(
            sum(skipped.values()),
            len(rows),
            "被排除的行必须计数，不能静默丢弃",
        )

    def test_the_persisted_metadata_column_is_read(self):
        """台账回来的是 ``metadata_json`` 文本；只读 ``metadata`` 会让排除静默失效。

        ``SELECT *`` 不会把 JSON 列解析好，而调用方传进来的是解析后的字典。
        两种形状都必须认；只认一种时每行都看起来"不属于任何人"，排除就变成了
        空操作 —— 而且不会报错。
        """

        rows = [
            {
                "usage_id": row["usage_id"],
                "product_code": row["product_code"],
                "metadata_json": json.dumps(row["metadata"]),
            }
            for row in self._own_rows()
        ]
        first_item_id = f"HIST_{mixed_template_ids()[0]}"
        identity = ledger_row_identity(rows[0])
        self.assertEqual(identity["batch_item_id"], first_item_id)
        self.assertEqual(identity["batch_id"], self._SELF_BATCH)

        references, skipped = _mixed_history_references(
            rows, "P_ACC", exclusions={"batch_item_ids": [first_item_id]}
        )
        self.assertEqual(skipped["own_record"], 1)
        self.assertEqual(len(references), len(mixed_template_ids()) - 1)

    def test_a_resumed_batch_can_plan_again(self):
        rows = self._own_rows()
        blocked, _blocked_summary = _acc_allocate(2, recent_usage=rows)
        self.assertEqual(
            blocked, [], "不加排除时，批次被自己的记录挡住（这正是 R3 的现象）",
        )

        items, summary = _acc_allocate(
            2, recent_usage=rows, exclusions={"batch_ids": [self._SELF_BATCH]}
        )
        self.assertTrue(items, "排除自身记录后，同一请求必须能再次规划")
        self.assertEqual(
            summary["mixed_history_coverage"]["history_excluded_own"],
            len(mixed_template_ids()),
        )

    def test_excluding_own_rows_keeps_in_batch_sibling_comparison(self):
        items, _summary = _acc_allocate(
            2, recent_usage=[], exclusions={"batch_ids": [self._SELF_BATCH]}
        )
        self.assertEqual(len(items), 2, "排除历史里的自己不得影响同批互相比较")
        sibling = _acc_contract_of(items[1])["difference_report"]
        self.assertEqual(
            sibling["comparison_scope"],
            "BATCH_AND_HISTORY",
            "同批兄弟仍然要真比一次",
        )
        self.assertTrue(sibling["counts_as_independent"])

    def test_an_empty_exclusion_set_changes_nothing(self):
        rows = self._own_rows()
        with_none, _ = _mixed_history_references(rows, "P_ACC", exclusions=None)
        with_empty, skipped = _mixed_history_references(
            rows, "P_ACC", exclusions={"batch_ids": [], "usage_ids": []}
        )
        self.assertEqual(len(with_none), len(with_empty))
        self.assertEqual(skipped["own_record"], 0)

    def test_incomparable_history_is_reported_as_uncompared_not_distinct(self):
        """历史版本不同必须**明确不可比**，不得默认成 ``DISTINCT_THEME``。"""

        rows = [
            _acc_history_row(
                _acc_contract(template_id, select_environment_recipe_id(0))
            )
            for template_id in mixed_template_ids()
        ]
        for row in rows:
            row["metadata"][MIXED_HISTORY_METADATA_KEY]["signature_version"] = (
                "mixed-signature-v0"
            )
        self.assertNotEqual(MIXED_SIGNATURE_VERSION, "mixed-signature-v0")

        references, skipped = _mixed_history_references(rows, "P_ACC")
        self.assertEqual(references, [], "版本不同的旧签名不得当引用")
        self.assertEqual(skipped["incomplete"], len(mixed_template_ids()))

        items, summary = _acc_allocate(1, recent_usage=rows)
        self.assertTrue(items, "旧版历史不得阻断新内容")
        report = _acc_contract_of(items[0])["difference_report"]
        self.assertEqual(
            report["comparison_scope"],
            "HISTORY_INCOMPARABLE",
            "未完成跨批比对必须写明，不能读成『已比对且确认不同』",
        )
        self.assertIn("HISTORY_INCOMPARABLE", report["difference_dimensions"])
        # 第一条只有不可比的旧历史可看 -> 未判定；第二条与同批第一条**真比过**
        # -> 独立主题。两个桶必须分别是 1，既不能把未判定的算成主题，也不能
        # 因为历史不可比就停掉同批比较。
        self.assertEqual(summary["mixed_uncompared_count"], 1)
        self.assertEqual(summary["mixed_distinct_theme_count"], 1)
        self.assertEqual(
            _acc_contract_of(items[1])["difference_report"]["comparison_scope"],
            "BATCH_AND_HISTORY",
        )

    def test_a_shared_batch_id_is_the_only_row_shape_excluded(self):
        """只有"本批次自己的行"被排除，别款商品的行仍按商品过滤计数。"""

        rows = []
        for template_id in mixed_template_ids():
            row = _acc_history_row(
                _acc_contract(template_id, select_environment_recipe_id(0))
            )
            rows.append(row)
            other = _acc_history_row(
                _acc_contract(template_id, select_environment_recipe_id(0)),
                product_code="P_OTHER",
            )
            rows.append(other)
        rows[0]["metadata"]["batch_id"] = self._SELF_BATCH

        references, skipped = _mixed_history_references(
            rows, "P_ACC", exclusions={"batch_ids": [self._SELF_BATCH]}
        )
        self.assertEqual(skipped["own_record"], 1)
        self.assertEqual(skipped["other_product"], len(mixed_template_ids()))
        self.assertEqual(len(references), len(mixed_template_ids()) - 1)


if __name__ == "__main__":
    unittest.main()
