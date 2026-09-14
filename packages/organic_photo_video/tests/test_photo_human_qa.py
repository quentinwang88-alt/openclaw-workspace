"""Deterministic human-presentation QA tests (TH persona realism handoff 6.6).

Model output fixtures only — no paid vision calls in unit tests.
"""

import unittest

from services.photo_human_qa import (
    HumanPresentationQAError,
    evaluate_human_presentation,
    load_human_presentation_policy,
    normalize_human_presentation_review,
)


def role_observation(role, *, pose_family="RELAXED_STAND", gaze="CAMERA",
                     expression="NEUTRAL", head_tilt="NONE",
                     head_tilt_direction="NONE", ai_face_signs=None,
                     limb=False, drift=False, scores=None, passed=None):
    return {
        "role": role,
        "scores": scores or {
            "face_realism": 90, "head_posture": 92, "body_posture": 88,
            "gesture_naturalness": 85, "expression_naturalness": 86,
            "creator_photo_feel": 90,
        },
        "observations": {
            "head_tilt": head_tilt,
            "head_tilt_direction": head_tilt_direction,
            "gaze": gaze,
            "pose_family": pose_family,
            "expression": expression,
            "ai_face_signs": ai_face_signs or [],
            "limb_structure_implausible": limb,
            "identity_drift": drift,
        },
        "issues": [],
        "repair_instruction": "",
        # 模型即使自带 passed=true，程序也必须按规则重算。
        "passed": passed if passed is not None else True,
    }


def passing_group_review():
    return {"roles": [
        role_observation("look_a", pose_family="RELAXED_STAND", gaze="CAMERA",
                         expression="SOFT_SMILE"),
        role_observation("look_b", pose_family="WALKING_CANDID", gaze="FORWARD"),
        role_observation("look_c", pose_family="SCENE_INTERACTION", gaze="SIDE"),
        role_observation("look_d", pose_family="TURN_BACK", gaze="CAMERA"),
    ]}


class HumanPresentationEvaluateTest(unittest.TestCase):
    def test_standard_level_ignores_score_thresholds(self):
        review = passing_group_review()
        review["roles"][1] = role_observation(
            "look_b", pose_family="WALKING_CANDID", gaze="FORWARD",
            scores={"face_realism": 60, "head_posture": 62, "body_posture": 65,
                    "gesture_naturalness": 60, "expression_naturalness": 61,
                    "creator_photo_feel": 55},
        )
        verdict = evaluate_human_presentation(review)
        self.assertEqual(verdict["qa_level"], "standard")
        self.assertTrue(verdict["passed"], "分数不达阈值在 standard 下不拦截")
        verdict_strict = evaluate_human_presentation(review, level="strict")
        self.assertIn("look_b", verdict_strict["failed_roles"])

    def test_standard_level_skips_group_diversity_rules(self):
        review = passing_group_review()
        for index, pose in enumerate(
                ("RELAXED_STAND", "WALKING_CANDID", "RELAXED_STAND", "WALKING_CANDID")):
            role = "look_" + "abcd"[index]
            review["roles"][index] = role_observation(
                role, pose_family=pose, gaze="CAMERA", expression="SOFT_SMILE")
        verdict = evaluate_human_presentation(review)
        self.assertTrue(verdict["passed"], "组级多样性规则在 standard 下不拦截")
        self.assertEqual(verdict["group"].get("reason"), "identical_pose_gate")

    def test_identical_quartet_regenerates_one_page(self):
        review = passing_group_review()
        for index in range(4):
            review["roles"][index] = role_observation(
                "look_" + "abcd"[index], pose_family="RELAXED_STAND",
                gaze="CAMERA", expression="SOFT_SMILE")
        verdict = evaluate_human_presentation(review)
        self.assertFalse(verdict["passed"])
        self.assertEqual(verdict["failed_roles"], ["look_d"], "只重生最重复的一张，保留 A 锚点")

    def test_full_passing_group_passes(self):
        verdict = evaluate_human_presentation(passing_group_review())
        self.assertTrue(verdict["passed"], verdict)
        self.assertEqual(verdict["failed_roles"], [])
        strict = evaluate_human_presentation(passing_group_review(), level="strict")
        self.assertEqual(len(strict["group"]["pose_families"]), 4)
        self.assertEqual(len(strict["group"]["gaze_directions"]), 3)

    def test_obvious_head_tilt_always_fails(self):
        review = passing_group_review()
        review["roles"][1] = role_observation(
            "look_b", pose_family="WALKING_CANDID", gaze="FORWARD",
            head_tilt="OBVIOUS", head_tilt_direction="RIGHT",
        )
        verdict = evaluate_human_presentation(review, level="strict")
        self.assertFalse(verdict["passed"])
        self.assertEqual(verdict["failed_roles"], ["look_b"])
        finding = verdict["roles"][1]
        self.assertFalse(finding["passed"])
        self.assertIn("head_tilt=OBVIOUS", finding["issues"])

    def test_model_passed_flag_never_overrides_program_verdict(self):
        review = passing_group_review()
        review["roles"][0] = role_observation(
            "look_a", head_tilt="OBVIOUS", head_tilt_direction="LEFT", passed=True,
        )
        verdict = evaluate_human_presentation(review, level="strict")
        self.assertFalse(verdict["passed"])
        self.assertIn("look_a", verdict["failed_roles"])

    def test_ai_face_hard_failures(self):
        # DOLL_EYES / FACE_GEOMETRY_ARTIFACT 单独出现即判死。
        # PLASTIC_SKIN 例外，见紧随其后的四条（2026-09-14 口径修订）。
        for sign in ("DOLL_EYES", "FACE_GEOMETRY_ARTIFACT"):
            review = passing_group_review()
            review["roles"][2] = role_observation(
                "look_c", pose_family="SCENE_INTERACTION", gaze="SIDE",
                ai_face_signs=[sign],
            )
            verdict = evaluate_human_presentation(review)
            self.assertFalse(verdict["passed"], sign)
            finding = verdict["roles"][2]
            self.assertTrue(any(
                item.startswith("ai_face_signs=") for item in finding["issues"]
            ), sign)

    def test_lone_plastic_skin_with_healthy_face_realism_is_warning_only(self):
        """孤证 PLASTIC_SKIN 不再判死：留证据、不烧一轮整组重生。

        实测依据见 services/photo_human_qa.py 的
        PLASTIC_SKIN_CORROBORATION_FACE_REALISM 注释。
        """
        review = passing_group_review()
        review["roles"][0] = role_observation(
            "look_a", pose_family="RELAXED_STAND",
            ai_face_signs=["PLASTIC_SKIN"],
            scores={"face_realism": 82, "head_posture": 95, "body_posture": 80,
                    "gesture_naturalness": 78, "expression_naturalness": 85,
                    "creator_photo_feel": 80},
        )
        verdict = evaluate_human_presentation(review)
        self.assertTrue(verdict["passed"], verdict["failed_roles"])
        self.assertNotIn("look_a", verdict["failed_roles"])
        self.assertEqual(verdict["roles"][0]["issues"], [])
        # 降级不等于抹掉：原始观察照旧留在证据里供人工复核。
        self.assertEqual(
            verdict["roles"][0]["observations"]["ai_face_signs"], ["PLASTIC_SKIN"]
        )
        codes = [warning["code"] for warning in verdict["quality_warnings"]]
        self.assertIn("PLASTIC_SKIN_UNCONFIRMED", codes)

    def test_lone_plastic_skin_with_low_face_realism_still_fails(self):
        review = passing_group_review()
        review["roles"][0] = role_observation(
            "look_a", ai_face_signs=["PLASTIC_SKIN"],
            scores={"face_realism": 72, "head_posture": 90, "body_posture": 85,
                    "gesture_naturalness": 80, "expression_naturalness": 84,
                    "creator_photo_feel": 80},
        )
        verdict = evaluate_human_presentation(review)
        self.assertIn("look_a", verdict["failed_roles"])
        self.assertIn("ai_face_signs=PLASTIC_SKIN", verdict["roles"][0]["issues"])

    def test_plastic_skin_next_to_another_sign_still_fails(self):
        review = passing_group_review()
        review["roles"][1] = role_observation(
            "look_b", pose_family="WALKING_CANDID", gaze="FORWARD",
            ai_face_signs=["PLASTIC_SKIN", "DOLL_EYES"],
        )
        verdict = evaluate_human_presentation(review)
        self.assertIn("look_b", verdict["failed_roles"])
        self.assertIn("ai_face_signs=DOLL_EYES,PLASTIC_SKIN",
                      verdict["roles"][1]["issues"])

    def test_strict_level_still_fails_lone_plastic_skin(self):
        """strict 档完整保留旧口径：任何硬失败 AI 脸特征都判死。"""
        review = passing_group_review()
        review["roles"][0] = role_observation(
            "look_a", ai_face_signs=["PLASTIC_SKIN"],
        )
        strict = evaluate_human_presentation(review, level="strict")
        self.assertIn("look_a", strict["failed_roles"])
        self.assertNotIn(
            "PLASTIC_SKIN_UNCONFIRMED",
            [warning["code"] for warning in strict["quality_warnings"]],
        )

    def test_limb_hard_failure_and_identity_drift_is_observed_only(self):
        review = passing_group_review()
        review["roles"][3] = role_observation(
            "look_d", pose_family="TURN_BACK", limb=True,
        )
        self.assertIn("look_d", evaluate_human_presentation(review)["failed_roles"])
        # AI persona + AI 生成链路下身份漂移是能力常态：只记录观察，不硬失败。
        review["roles"][3] = role_observation(
            "look_d", pose_family="TURN_BACK", drift=True,
        )
        verdict = evaluate_human_presentation(review)
        self.assertTrue(verdict["passed"])
        self.assertTrue(
            verdict["roles"][3]["observations"]["identity_drift"]
        )

    def test_static_mannequin_pose_family_hard_fails(self):
        review = passing_group_review()
        review["roles"][3] = role_observation(
            "look_d", pose_family="STATIC_MANNEQUIN", gaze="CAMERA",
            expression="SOFT_SMILE",
        )
        verdict = evaluate_human_presentation(review)
        self.assertIn("look_d", verdict["failed_roles"])

    def test_low_scores_fail_thresholds(self):
        review = passing_group_review()
        scores = {
            "face_realism": 90, "head_posture": 92, "body_posture": 88,
            "gesture_naturalness": 85, "expression_naturalness": 86,
            "creator_photo_feel": 60,
        }
        review["roles"][1] = role_observation(
            "look_b", pose_family="WALKING_CANDID", gaze="FORWARD", scores=scores,
        )
        verdict = evaluate_human_presentation(review, level="strict")
        self.assertIn("look_b", verdict["failed_roles"])
        self.assertTrue(any(
            item.startswith("creator_photo_feel=") for item in verdict["roles"][1]["issues"]
        ))

    def test_missing_scores_default_to_failure(self):
        review = {"roles": [{
            "role": "look_a",
            "observations": role_observation("look_a")["observations"],
            "issues": [], "repair_instruction": "",
        }]}
        verdict = evaluate_human_presentation(review, group_rules=False, level="strict")
        self.assertFalse(verdict["passed"])

    def test_group_needs_three_pose_families(self):
        review = passing_group_review()
        review["roles"][2] = role_observation(
            "look_c", pose_family="RELAXED_STAND", gaze="SIDE",
        )
        review["roles"][3] = role_observation(
            "look_d", pose_family="WALKING_CANDID", gaze="CAMERA",
        )
        verdict = evaluate_human_presentation(review, level="strict")
        self.assertFalse(verdict["passed"])
        self.assertTrue(any("动作族仅 2 类" in item for item in verdict["group_issues"]))

    def test_group_needs_two_gaze_directions(self):
        review = passing_group_review()
        review["roles"][1] = role_observation(
            "look_b", pose_family="WALKING_CANDID", gaze="CAMERA",
            expression="CANDID",
        )
        review["roles"][2] = role_observation(
            "look_c", pose_family="SCENE_INTERACTION", gaze="CAMERA",
        )
        review["roles"][3] = role_observation(
            "look_d", pose_family="TURN_BACK", gaze="CAMERA",
        )
        verdict = evaluate_human_presentation(review, level="strict")
        self.assertFalse(verdict["passed"])
        self.assertTrue(any("视线方向仅 1 类" in item for item in verdict["group_issues"]))

    def test_two_same_direction_minor_tilts_fail(self):
        review = passing_group_review()
        review["roles"][1] = role_observation(
            "look_b", pose_family="WALKING_CANDID", gaze="FORWARD",
            head_tilt="MINOR", head_tilt_direction="RIGHT",
        )
        review["roles"][3] = role_observation(
            "look_d", pose_family="TURN_BACK", head_tilt="MINOR",
            head_tilt_direction="RIGHT",
        )
        verdict = evaluate_human_presentation(review, level="strict")
        self.assertFalse(verdict["passed"])
        self.assertTrue(any("同向（RIGHT）轻微歪头" in item for item in verdict["group_issues"]))

    def test_single_minor_tilt_is_tolerated(self):
        review = passing_group_review()
        review["roles"][1] = role_observation(
            "look_b", pose_family="WALKING_CANDID", gaze="FORWARD",
            head_tilt="MINOR", head_tilt_direction="RIGHT",
        )
        verdict = evaluate_human_presentation(review)
        self.assertTrue(verdict["passed"], verdict["group_issues"])

    def test_more_than_one_static_camera_smile_fails(self):
        review = passing_group_review()
        review["roles"][3] = role_observation(
            "look_d", pose_family="RELAXED_STAND", gaze="CAMERA",
            expression="SOFT_SMILE",
        )
        verdict = evaluate_human_presentation(review, level="strict")
        self.assertFalse(verdict["passed"])
        self.assertTrue(any("静态照超过 1 张" in item for item in verdict["group_issues"]))

    def test_gate_mode_skips_group_rules(self):
        review = {"roles": [role_observation("look_a")]}
        verdict = evaluate_human_presentation(review, group_rules=False, level="strict")
        self.assertTrue(verdict["passed"])
        # 单张门禁走 identical_pose 门（单张永不触发），strict 组规则仍跳过。
        self.assertEqual(verdict["group"].get("reason"), "identical_pose_gate")


class NormalizeAndPolicyTest(unittest.TestCase):
    def test_invalid_structure_raises(self):
        with self.assertRaises(HumanPresentationQAError):
            normalize_human_presentation_review({"roles": []})
        with self.assertRaises(HumanPresentationQAError):
            normalize_human_presentation_review(
                {"roles": [{"role": "look_a", "scores": "bad"}]}
            )
        with self.assertRaises(HumanPresentationQAError):
            normalize_human_presentation_review("not a dict")

    def test_missing_roles_fail_coverage(self):
        with self.assertRaises(HumanPresentationQAError):
            normalize_human_presentation_review(
                {"roles": [role_observation("look_a")]},
                role_order=["look_a", "look_b"],
            )

    def test_scores_clamped_and_enums_normalized(self):
        review = {"roles": [role_observation(
            "look_a", scores={"face_realism": 120, "head_posture": -5},
            gaze="camera", pose_family="walking_candid",
        )]}
        normalized = normalize_human_presentation_review(review)
        item = normalized["roles"][0]
        self.assertEqual(item["scores"]["face_realism"], 100.0)
        self.assertEqual(item["scores"]["head_posture"], 0.0)
        self.assertEqual(item["observations"]["gaze"], "CAMERA")
        self.assertEqual(item["observations"]["pose_family"], "WALKING_CANDID")

    def test_policy_loader_returns_th_creator_realism_v1(self):
        policy = load_human_presentation_policy()
        self.assertEqual(policy["policy_id"], "TH_CREATOR_REALISM_V1")
        self.assertTrue(policy["head"]["require_level_neck"])
        self.assertEqual(policy["group"]["min_pose_families"], 3)

    def test_unknown_policy_id_fails_loudly(self):
        with self.assertRaises(HumanPresentationQAError):
            load_human_presentation_policy("NOT_A_POLICY")

    def test_policy_group_rules_drive_verdict(self):
        review = passing_group_review()
        review["roles"][2] = role_observation(
            "look_c", pose_family="RELAXED_STAND", gaze="SIDE",
        )
        review["roles"][3] = role_observation(
            "look_d", pose_family="WALKING_CANDID", gaze="CAMERA",
        )
        # 默认规则：动作族 2 类 < 3 → 失败。
        self.assertFalse(evaluate_human_presentation(review, level="strict")["passed"])
        # 放宽策略：动作族下限 2 → 通过。
        policy = load_human_presentation_policy()
        policy["group"]["min_pose_families"] = 2
        self.assertTrue(evaluate_human_presentation(review, policy=policy, level="strict")["passed"])


if __name__ == "__main__":
    unittest.main()
