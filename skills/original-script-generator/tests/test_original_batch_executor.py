"""Focused tests for the batch execution integrity boundary."""
import unittest
import json
from copy import deepcopy
from pathlib import Path
import sys
from unittest.mock import MagicMock, patch

SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.original_batch_executor import (
    STAGE_CHECKPOINT_SCHEMA_VERSION,
    _checkpoint_identity,
    _execute_single_item,
    _load_stage_checkpoint,
    _repair_frozen_seed_mixed_extension,
    run_script_only,
    validate_batch_script_integrity,
    _generate_simplified_visual_script_with_fallback,
)
from core.original_batch_models import BatchRecord, PlanItem


def _direction():
    return {
        "content_bundle_brief": {
            "claim_atoms": [
                {"claim_key": "C1", "fact_text": "短款衣长落在腰线附近"},
                {"claim_key": "C2", "fact_text": "袖部有抽褶和银色椭圆扣"},
            ]
        }
    }


def _script(presentation="PERSON_ON_CAMERA"):
    on_camera = presentation == "PERSON_ON_CAMERA"
    return {
        "creative_blueprint": {"creative_blueprint_id": "CBP_1"},
        "creative_diversity_contract": {"contract_id": "CDC_1"},
        "continuous_voiceover": {
            "chinese_translation": "这件是短款，袖部还有抽褶和银色椭圆扣。"
        },
        "production_design": {
            "presentation_mode": presentation,
            "character_setting": {
                "on_camera": on_camera,
                "identity": "下班前准备离开的年轻上班族" if on_camera else "",
                "appearance": "自然短发，淡妆" if on_camera else "",
                "hair_makeup": "" if on_camera else "UNAVAILABLE",
                "speaking_personality": "像朋友分享刚发现的细节" if on_camera else "",
            },
            "scene_setting": {
                "location": "办公室衣帽区",
                "lighting": "窗边自然光",
                "background": "浅色墙与衣架",
            },
            "outfit_setting": {
                "styling": "黑色内搭、深色牛仔裤、短款外套" if on_camera else ""
            },
            "performance_setting": {
                "behavior_motivation": "拿包离开前顺手穿好外套" if on_camera else ""
            },
            "event_setting": {"natural_event": "整理好商品后结束一次连续观察"},
        },
    }


class BatchIntegrityTest(unittest.TestCase):
    def test_simplified_visual_script_retries_sol_then_falls_back_to_terra(self):
        first = MagicMock()
        second = MagicMock()
        first.call_json.side_effect = [
            Exception("Our servers are currently overloaded"),
            Exception("Our servers are currently overloaded"),
        ]
        second.call_json.return_value = {"script_concept": {"one_sentence_idea": "ok"}}
        with patch("core.llm_client.OriginalScriptLLMClient", side_effect=[first, second]) as client_mock, \
             patch("core.original_batch_executor.time.sleep"):
            raw, provenance = _generate_simplified_visual_script_with_fallback(
                prompt="test", primary_model="gpt-5.6-sol", reasoning_effort="high", max_tokens=100,
            )
        self.assertEqual(raw["script_concept"]["one_sentence_idea"], "ok")
        self.assertEqual(provenance["model"], "gpt-5.6-terra")
        self.assertTrue(provenance["fallback_used"])
        self.assertEqual(
            [call.kwargs["primary_model"] for call in client_mock.call_args_list],
            ["gpt-5.6-sol", "gpt-5.6-terra"],
        )

    def test_peer_closed_is_retried_then_falls_back_to_terra(self):
        first = MagicMock()
        second = MagicMock()
        first.call_json.side_effect = [
            Exception("peer closed connection without sending complete message body (incomplete chunked read)"),
            Exception("peer closed connection without sending complete message body (incomplete chunked read)"),
        ]
        second.call_json.return_value = {"script_concept": {"one_sentence_idea": "ok"}}
        with patch("core.llm_client.OriginalScriptLLMClient", side_effect=[first, second]), \
             patch("core.original_batch_executor.time.sleep"):
            raw, provenance = _generate_simplified_visual_script_with_fallback(
                prompt="test", primary_model="gpt-5.6-sol", reasoning_effort="high", max_tokens=100,
            )
        self.assertEqual(raw["script_concept"]["one_sentence_idea"], "ok")
        self.assertEqual(provenance["model"], "gpt-5.6-terra")
        self.assertTrue(provenance["fallback_used"])
        self.assertEqual(first.call_json.call_count, 2)
        self.assertEqual(second.call_json.call_count, 1)

    def test_person_direction_requires_full_production_design(self):
        script = _script()
        script["production_design"]["outfit_setting"]["styling"] = ""
        result = validate_batch_script_integrity(script, direction=_direction())
        self.assertFalse(result["valid"])
        self.assertTrue(any("人物方向缺少" in issue for issue in result["issues"]))

    def test_static_direction_does_not_require_person_or_outfit(self):
        result = validate_batch_script_integrity(
            _script("STATIC_PRODUCT"), direction=_direction()
        )
        self.assertTrue(result["valid"], result["issues"])

    def test_unsupported_effect_is_blocked(self):
        script = _script()
        script["continuous_voiceover"]["chinese_translation"] += "还能显腿长。"
        result = validate_batch_script_integrity(script, direction=_direction())
        self.assertFalse(result["valid"])
        self.assertIn("口播出现未授权效果词：显腿长", result["issues"])

    def test_authorized_effect_is_allowed(self):
        direction = deepcopy(_direction())
        direction["content_bundle_brief"]["value_proposition"] = {
            "text": "上身显腿长"
        }
        script = _script()
        script["continuous_voiceover"]["chinese_translation"] += "上身显腿长。"
        result = validate_batch_script_integrity(script, direction=direction)
        self.assertTrue(result["valid"], result["issues"])


def _checkpoint_item() -> PlanItem:
    return PlanItem(
        batch_item_id="OCI_1",
        batch_id="OCB_1",
        item_index=1,
        item_role="STRUCTURE_MOTHER",
        product_code="P1",
        selection_run_id="SR_1",
        direction_assignment_id="DA_1",
        compatibility_slot="S1",
        structure_contract_json="{}",
        allocation_signature="SIG_1",
        policy_version="v2",
        item_snapshot_hash="SNAP_1",
        content_bundle_id="CB_1",
        content_bundle_json="{}",
        content_angle_key="FACT_DISCOVERY",
        audience_tension_status="UNAVAILABLE",
        claim_keys_json="[]",
        requested_hook_id="DETAIL_SURPRISE",
        eligible_hook_ids_json="[]",
        frozen_direction_package_json=json.dumps({
            "schema_version": "original-frozen-direction-package-v1",
            "creative_diversity_contract": {"contract_id": "CDC_1"},
        }),
    )


class BatchStageCheckpointTest(unittest.TestCase):
    def setUp(self):
        self.provenance = {
            "stage": "complete_script_blueprint",
            "route": "primary",
            "model": "gpt-5.6-sol",
            "reasoning_effort": "high",
        }

    def test_matching_checkpoint_preserves_invalid_normalized_blueprint(self):
        item = _checkpoint_item()
        checkpoint = {
            "schema_version": STAGE_CHECKPOINT_SCHEMA_VERSION,
            "identity": _checkpoint_identity(item, self.provenance),
            "stages": {
                "blueprint": {
                    "status": "INVALID",
                    "normalized": {"creative_blueprint_id": "CBP_OLD"},
                    "validation": {"valid": False, "issues": ["old rule"]},
                }
            },
        }
        item.stage_checkpoint_json = json.dumps(checkpoint)
        loaded, matched = _load_stage_checkpoint(item, self.provenance)
        self.assertTrue(matched)
        self.assertEqual(
            loaded["stages"]["blueprint"]["normalized"]["creative_blueprint_id"],
            "CBP_OLD",
        )

    def test_model_change_invalidates_checkpoint(self):
        item = _checkpoint_item()
        checkpoint = {
            "schema_version": STAGE_CHECKPOINT_SCHEMA_VERSION,
            "identity": _checkpoint_identity(item, self.provenance),
            "stages": {"blueprint": {"normalized": {"x": 1}}},
        }
        item.stage_checkpoint_json = json.dumps(checkpoint)
        changed = dict(self.provenance, model="gpt-5.7-sol")
        loaded, matched = _load_stage_checkpoint(item, changed)
        self.assertFalse(matched)
        self.assertEqual(loaded["stages"], {})

    def test_partial_capacity_batch_finishes_when_every_planned_item_is_ready(self):
        item = _checkpoint_item()
        item.status = "PLANNED"
        ready_item = deepcopy(item)
        ready_item.status = "SCRIPT_READY"
        batch = BatchRecord(
            batch_id="OCB_PARTIAL",
            request_id="OP_PARTIAL",
            product_code="P1",
            requested_count=10,
            planned_count=1,
            test_phase="INITIAL",
            execution_mode="SCRIPT_ONLY",
            policy_version="v9",
            random_seed=1,
            data_snapshot_hash="D1",
            input_snapshot_json="{}",
            status="PARTIAL_PLANNED",
        )
        storage = MagicMock()
        storage.get_batch.side_effect = [batch, batch]
        storage.get_items.side_effect = [[item], [ready_item], [ready_item]]
        with patch("core.original_batch_executor.BatchStorage", return_value=storage), patch(
            "core.original_batch_executor._execute_single_item",
            return_value={"status": "SUCCESS"},
        ):
            run_script_only("OCB_PARTIAL")
        self.assertEqual(
            storage.update_batch_status.call_args.kwargs["status"],
            "SCRIPT_READY",
        )
        self.assertEqual(storage.update_batch_status.call_args.kwargs["ready_count"], 1)

    def test_validation_fix_reuses_blueprint_then_downstream_retry_reuses_all_stages(self):
        item = _checkpoint_item()
        item.frozen_direction_package_json = json.dumps({
            "schema_version": "original-frozen-direction-package-v1",
            "structure_contract": {"hard_constraints": {"content_carrier": "WEARER_ACTIVE"}},
            "structure_execution_plan": {"shot_plan": [{"shot_no": 1}]},
            "execution_reference": {"execution_card_id": "EC_1"},
            "content_bundle_brief": {"claim_atoms": [{"claim_key": "C1", "fact_text": "短款"}]},
            "creative_diversity_contract": {"contract_id": "CDC_1"},
        })
        batch = BatchRecord(
            batch_id="OCB_1",
            request_id="OP_1",
            product_code="P1",
            requested_count=1,
            test_phase="INITIAL",
            execution_mode="SCRIPT_ONLY",
            policy_version="v2",
            random_seed=1,
            data_snapshot_hash="D1",
            input_snapshot_json="{}",
            target_country="泰国",
            target_language="泰语",
            top_category="女装",
            product_type="外套",
        )
        storage = MagicMock()
        llm = MagicMock()
        llm.route = "primary"
        llm.primary_model = "gpt-5.6-sol"
        llm.primary_reasoning_effort = "high"
        llm.call_json.return_value = {"raw": "blueprint"}
        blueprint = {
            "creative_blueprint_id": "CBP_1",
            "generation_provenance": self.provenance,
        }
        visual = {"shots": [{"shot_no": 1, "shot_content": "展示短款", "observable_action": "拿起外套"}]}
        voiceover = {"hook_id": "DETAIL_SURPRISE", "chinese_translation": "先看这里，这件是短款。"}
        script = {"storyboard": [], "continuous_voiceover": voiceover}
        valid = {"valid": True, "issues": []}
        invalid = {"valid": False, "issues": ["old validation rule"]}
        context = {
            "anchor_card": {"category_execution_contract": {}},
            "target_country": "泰国",
            "target_language": "泰语",
            "top_category": "女装",
            "product_type": "外套",
        }

        with patch("core.original_batch_executor.load_product_context", return_value=context), \
             patch("core.llm_client.OriginalScriptLLMClient", return_value=llm), \
             patch("core.reality_reference_prompts.build_complete_script_blueprint_prompt", return_value="prompt"), \
             patch("scripts.run_reality_reference_stage0._normalize_blueprint", return_value=blueprint), \
             patch("core.complete_script_v3.validate_complete_blueprint", side_effect=[invalid, valid, valid]), \
             patch("core.complete_script_v3.video_prompt_projection", return_value={}), \
             patch("core.reality_reference.project_event_blueprint_to_visual_plan", return_value=visual) as visual_mock, \
             patch("core.reality_reference.validate_visual_adaptation", return_value=valid), \
             patch("core.complete_voiceover_direct.run_central_complete_voiceover", return_value=voiceover) as voice_mock, \
             patch("core.reality_reference.assemble_reality_script", return_value=script), \
             patch("core.complete_script_v3.validate_complete_script", return_value=valid), \
             patch("core.original_batch_executor.validate_batch_script_integrity", return_value=valid), \
             patch("core.reality_reference.validate_voiceover_visual_grounding", return_value=valid), \
             patch("core.structure_router_adapter.bind_structure_application", return_value="BIND_1"):
            with self.assertRaisesRegex(ValueError, "完整蓝图校验失败"):
                _execute_single_item(item, batch, storage)
            second = _execute_single_item(item, batch, storage)
            third = _execute_single_item(item, batch, storage)

        self.assertEqual(second["stage_cache"]["blueprint"], "HIT_NORMALIZED")
        self.assertEqual(second["stage_cache"]["visual"], "MISS")
        self.assertEqual(second["stage_cache"]["voiceover"], "MISS")
        self.assertEqual(third["stage_cache"]["blueprint"], "HIT_NORMALIZED")
        self.assertEqual(third["stage_cache"]["visual"], "HIT")
        self.assertEqual(third["stage_cache"]["voiceover"], "HIT")
        self.assertEqual(llm.call_json.call_count, 1)
        self.assertEqual(visual_mock.call_count, 1)
        self.assertEqual(voice_mock.call_count, 1)

    def test_frozen_direction_change_invalidates_checkpoint(self):
        item = _checkpoint_item()
        checkpoint = {
            "schema_version": STAGE_CHECKPOINT_SCHEMA_VERSION,
            "identity": _checkpoint_identity(item, self.provenance),
            "stages": {"visual": {"plan": {"shots": []}}},
        }
        item.stage_checkpoint_json = json.dumps(checkpoint)
        frozen = json.loads(item.frozen_direction_package_json)
        frozen["creative_diversity_contract"]["contract_id"] = "CDC_2"
        item.frozen_direction_package_json = json.dumps(frozen)
        loaded, matched = _load_stage_checkpoint(item, self.provenance)
        self.assertFalse(matched)
        self.assertEqual(loaded["stages"], {})

class FrozenSeedMixedExtensionRepairTest(unittest.TestCase):
    """Batches frozen before the allocator ordering fix must still work."""

    def _contract(self):
        return {"template_id": "AMX_A_WORN_FIRST", "execution_profile": "x"}

    def _frozen(self, with_contract=True):
        extension = {"schema_version": "accessory-execution-profile-v5"}
        if with_contract:
            extension["mixed_template_contract"] = self._contract()
        return {
            "category_execution_extension": extension,
            "simplified_creative_seed": {
                "category_execution_extension": {"schema_version": "accessory-execution-profile-v5"}
            },
        }

    def test_contract_is_re_attached_when_the_seed_lost_it(self):
        frozen = self._frozen(with_contract=True)
        repaired = _repair_frozen_seed_mixed_extension(
            frozen["simplified_creative_seed"], frozen
        )
        self.assertEqual(
            repaired["category_execution_extension"]["mixed_template_contract"],
            self._contract(),
        )
        self.assertEqual(
            repaired["category_execution_extension"]["schema_version"],
            "accessory-execution-profile-v5",
        )

    def test_seed_is_not_mutated_in_place(self):
        frozen = self._frozen(with_contract=True)
        seed = frozen["simplified_creative_seed"]
        original = deepcopy(seed)
        _repair_frozen_seed_mixed_extension(seed, frozen)
        self.assertEqual(seed, original)

    def test_existing_seed_contract_wins_and_is_left_alone(self):
        frozen = self._frozen(with_contract=True)
        seed = {
            "category_execution_extension": {
                "mixed_template_contract": {"template_id": "AMX_C_DETAIL_FIRST"}
            }
        }
        repaired = _repair_frozen_seed_mixed_extension(seed, frozen)
        self.assertEqual(
            repaired["category_execution_extension"]["mixed_template_contract"],
            {"template_id": "AMX_C_DETAIL_FIRST"},
        )

    def test_gate_off_frozen_package_is_untouched(self):
        frozen = self._frozen(with_contract=False)
        seed = frozen["simplified_creative_seed"]
        self.assertEqual(_repair_frozen_seed_mixed_extension(seed, frozen), seed)

    def test_missing_frozen_package_is_tolerated(self):
        seed = {"category_execution_extension": {"schema_version": "s"}}
        self.assertEqual(_repair_frozen_seed_mixed_extension(seed, {}), seed)
        self.assertEqual(_repair_frozen_seed_mixed_extension(seed, None), seed)


def _necklace_frozen_package(*, profile_hash: str = "cafe0123456789ab", version: int = 1) -> str:
    """A frozen package carrying the NECKLACE_MIXED_V1 namespace."""

    return json.dumps(
        {
            "schema_version": "original-frozen-direction-package-v1",
            "creative_diversity_contract": {"contract_id": "CDC_1"},
            "category_execution_extension": {
                "mixed_template_contract": {
                    "execution_profile": "ACCESSORY_MIXED_TEMPLATE_V1",
                    "template_id": "NMX_01_WEAR_DETAIL_STATIC",
                    "feature_profile": "NECKLACE_MIXED_V1",
                    "feature_version": version,
                    "necklace_contract": {
                        "subtype": "SINGLE_LAYER_SINGLE_PENDANT",
                        "interaction_mode": "NONE",
                        "profile_config_hash": profile_hash,
                        "feature_profile": "NECKLACE_MIXED_V1",
                        "feature_version": version,
                    },
                }
            },
        },
        ensure_ascii=False,
    )


class NecklaceCheckpointMaterialTest(unittest.TestCase):
    """Section 7's local cache version: only the necklace item may gain keys.

    The checkpoint identity is what decides whether a cached voiceover /
    assembly stage is reused.  A necklace template edit has to invalidate the
    necklace items -- and nothing else, because a global bump would throw away
    every other category's cached work for a change that cannot affect it.
    """

    def setUp(self):
        self.provenance = {
            "stage": "complete_script_blueprint",
            "route": "primary",
            "model": "gpt-5.6-sol",
            "reasoning_effort": "high",
        }

    def test_a_non_necklace_item_gains_no_local_material(self):
        item = _checkpoint_item()
        identity = _checkpoint_identity(item, self.provenance)
        self.assertNotIn("necklace_profile", identity)
        # And it is *identical* to what the same item produces when the necklace
        # module cannot even be imported -- i.e. the new code adds nothing at all
        # to a non-necklace identity.
        with patch.dict("sys.modules", {"core.necklace_mixed_profile": None}):
            without_module = _checkpoint_identity(item, self.provenance)
        self.assertEqual(identity, without_module)

    def test_a_necklace_item_carries_its_own_profile_version(self):
        item = _checkpoint_item()
        item.frozen_direction_package_json = _necklace_frozen_package()
        material = _checkpoint_identity(item, self.provenance)["necklace_profile"]
        self.assertEqual(material["feature_profile"], "NECKLACE_MIXED_V1")
        self.assertEqual(material["feature_version"], 1)
        self.assertEqual(material["profile_config_hash"], "cafe0123456789ab")
        self.assertTrue(material["frozen_contract_hash"])

    def test_a_profile_edit_invalidates_the_necklace_item_only(self):
        necklace = _checkpoint_item()
        necklace.batch_item_id = "OCI_N"
        necklace.frozen_direction_package_json = _necklace_frozen_package()
        other = _checkpoint_item()
        other.batch_item_id = "OCI_E"
        other.frozen_direction_package_json = json.dumps(
            {
                "schema_version": "original-frozen-direction-package-v1",
                "category_execution_extension": {
                    "mixed_template_contract": {
                        "execution_profile": "ACCESSORY_MIXED_TEMPLATE_V1",
                        "template_id": "AMX_A_WORN_FIRST",
                    }
                },
            },
            ensure_ascii=False,
        )
        before_other = _checkpoint_identity(other, self.provenance)

        necklace.frozen_direction_package_json = _necklace_frozen_package(
            profile_hash="0000000000000000"
        )
        after_necklace = _checkpoint_identity(necklace, self.provenance)
        after_other = _checkpoint_identity(other, self.provenance)

        visited = _checkpoint_item()
        visited.frozen_direction_package_json = _necklace_frozen_package()
        self.assertNotEqual(
            after_necklace["necklace_profile"]["frozen_contract_hash"],
            _checkpoint_identity(visited, self.provenance)["necklace_profile"][
                "frozen_contract_hash"
            ],
        )
        self.assertEqual(before_other, after_other)
        self.assertNotIn("necklace_profile", after_other)

    def test_a_feature_version_bump_also_invalidates_it(self):
        first = _checkpoint_item()
        first.frozen_direction_package_json = _necklace_frozen_package(version=1)
        second = _checkpoint_item()
        second.frozen_direction_package_json = _necklace_frozen_package(version=2)
        self.assertNotEqual(
            _checkpoint_identity(first, self.provenance),
            _checkpoint_identity(second, self.provenance),
        )

    def test_a_broken_profile_degrades_instead_of_failing_the_item(self):
        item = _checkpoint_item()
        item.frozen_direction_package_json = _necklace_frozen_package()
        with patch.dict("sys.modules", {"core.necklace_mixed_profile": None}):
            identity = _checkpoint_identity(item, self.provenance)
        self.assertNotIn("necklace_profile", identity)
        self.assertEqual(identity["item_snapshot_hash"], "SNAP_1")

    def test_an_unparsable_frozen_package_is_still_identified(self):
        item = _checkpoint_item()
        item.frozen_direction_package_json = "这不是 JSON"
        identity = _checkpoint_identity(item, self.provenance)
        self.assertNotIn("necklace_profile", identity)
        self.assertIn("frozen_direction_hash", identity)


if __name__ == "__main__":
    unittest.main()
