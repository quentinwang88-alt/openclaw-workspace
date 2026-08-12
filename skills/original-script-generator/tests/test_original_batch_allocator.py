"""V1 批次编排器单元测试 — 分配算法 + 存储 + 幂等"""
import json
import os
import sqlite3
import tempfile
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
    _eligible_hooks_for_bundle,
    _relationship_device_for_hook,
    _relationship_schedule,
)

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
            "simplified-creative-seed-v17-wearable-visual",
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


if __name__ == "__main__":
    unittest.main()
