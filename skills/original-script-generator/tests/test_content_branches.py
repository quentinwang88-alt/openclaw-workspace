from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path


SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.content_branches import DIRECT_RESPONSE, SEEDING_ORGANIC, get_branch, list_branches
from core.content_branches.contracts import BranchVersions, stable_id
from core.content_branches.product_evidence import (
    merge_product_visual_evidence,
    normalize_product_visual_evidence,
)
from core.content_branches.storage import BranchArtifactStorage
from core.content_branches.video_execution import (
    compile_video_execution_brief,
    render_video_execution_prompt,
)


class ContentBranchArchitectureTest(unittest.TestCase):
    def test_builtin_branches_are_separate_plugins(self):
        self.assertEqual(set(list_branches()), {DIRECT_RESPONSE, SEEDING_ORGANIC})
        self.assertEqual(get_branch(DIRECT_RESPONSE).script_type, "原创脚本")
        self.assertEqual(get_branch(SEEDING_ORGANIC).script_type, "种草脚本")

    def test_seed_publish_policy_is_fail_closed(self):
        policy = get_branch(SEEDING_ORGANIC).publish_policy()
        policy.assert_safe_for_sync()
        self.assertEqual(policy.cart_policy, "FORBIDDEN")
        self.assertFalse(policy.shoppable_endpoint_allowed)
        self.assertEqual(policy.product_id_transport, "INTERNAL_ANALYTICS_ONLY")

    def test_branch_and_policy_versions_isolate_ids(self):
        direct = BranchVersions("shared-v1", "direct-v1", "direct-prompt-v1")
        seed = BranchVersions("shared-v1", "seed-v1", "seed-prompt-v1")
        material = {"product_code": "P1", "theme": "same"}
        self.assertNotEqual(
            stable_id("ID_", {**material, "fingerprint": direct.fingerprint(DIRECT_RESPONSE)}),
            stable_id("ID_", {**material, "fingerprint": seed.fingerprint(SEEDING_ORGANIC)}),
        )

    def test_storage_is_injected_and_branch_owned(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "organic.sqlite3"
            storage = BranchArtifactStorage(path)
            storage.start_run(
                {
                    "run_id": "R1", "branch_key": SEEDING_ORGANIC,
                    "request_id": "REQ1", "product_code": "P1", "input_hash": "H1",
                    "shared_kernel_version": "K1", "branch_policy_version": "B1",
                    "branch_prompt_version": "P1", "request": {},
                }
            )
            self.assertEqual(storage.get_run("R1")["branch_key"], SEEDING_ORGANIC)
            self.assertTrue(path.exists())
            storage.save_stage_artifact(
                {
                    "run_id": "R1", "item_id": "I1", "item_index": 1,
                    "stage_key": "VISUAL", "attempt": 1,
                    "payload": {"passed": True},
                }
            )
            stages = storage.list_stage_artifacts("R1")
            self.assertEqual(stages[0]["stage_key"], "VISUAL")

    def test_visual_evidence_is_normalized_and_merged_without_branch_policy(self):
        evidence = normalize_product_visual_evidence(
            {
                "anchors": [
                    {
                        "anchor_type": "COLLAR",
                        "description": "可见小立领",
                        "visible_zones": ["COLLAR"],
                        "confidence": "HIGH",
                    }
                ]
            }
        )
        merged = merge_product_visual_evidence(
            {"identity_anchors": ["参考图是目标商品唯一权威"]}, evidence
        )
        self.assertTrue(evidence["anchors"][0]["anchor_id"].startswith("VISUAL_ANCHOR_"))
        self.assertEqual(merged["identity_anchors"], ["可见小立领"])

    def test_shared_video_execution_projection_separates_reference_authority(self):
        brief = compile_video_execution_brief(
            product_truth={
                "facts": [{"fact_id": "F1", "text": "浅蓝色短款轮廓"}],
                "identity_anchors": ["保持立领和前襟扣件"],
                "negative_constraints": [],
            },
            visual_blueprint={
                "creative_design": {
                    "creator": "泰国女性创作者",
                    "scene": "室内镜前",
                    "lived_moment": "出门前看完整造型",
                },
                "capture_units": [
                    {
                        "unit_id": "C1", "duration_seconds": 5, "shot": "中景",
                        "camera_action": "固定机位", "subject_action": "自然站定",
                        "product_evidence": "完整轮廓", "fact_refs": ["F1"],
                    }
                ],
            },
            duration_seconds=15,
        )
        text = render_video_execution_prompt(brief)
        self.assertEqual(len(brief["product_identity_lock"]), 2)
        self.assertIn("不控制人物、脸、身形", text)
        self.assertIn("正式目标语言口播由后期独立配音", text)
        self.assertNotIn("发布策略", text)

    def test_video_prompt_strips_duplicate_role_prefix_and_lists_authorized_props(self):
        brief = compile_video_execution_brief(
            product_truth={"facts": [], "identity_anchors": ["蓝色轮廓"]},
            visual_blueprint={
                "creative_design": {"creator": "创作设计：女性创作者", "scene": "室内"},
                "capture_units": [
                    {
                        "subject_action": "人物人物整理衣领",
                        "authorized_props": ["帆布包"],
                        "camera_action": "固定机位",
                    }
                ],
            },
            duration_seconds=15,
        )
        text = render_video_execution_prompt(brief)
        self.assertNotIn("人物人物", text)
        self.assertIn("【已授权场景道具】", text)
        self.assertIn("帆布包", text)


if __name__ == "__main__":
    unittest.main()
