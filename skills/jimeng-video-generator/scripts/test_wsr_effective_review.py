"""Offline fake-only coverage: no model, API, real video or production writes."""
import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

from PIL import Image

from review_checkpoint_policy import freeze_checkpoints
from wsr_content_review import (input_bundle, normalize_sampling, plan_frame_indices,
                                prepare_evidence, review_job, MAX_FRAME_BUDGET)
from test_wsr_content_review import response


class EffectiveCheckpointTest(unittest.TestCase):
    def setUp(self):
        self.original = [{"point_id": "cover", "requirement": "胸前展示双掌，双掌同时推进遮镜", "evidence_source": "model_extracted"},
                         {"point_id": "order", "requirement": "完整实体遮挡后才揭晓新发型"}]
        self.change = {"point_id": "cover", "operation": "replace", "replacement_requirement": "使用单掌自然推进遮镜", "reason": "用户同意单掌简化测试"}

    def test_exact_replacement_preserves_original_and_other_points(self):
        before = copy.deepcopy(self.original)
        policy = freeze_checkpoints(self.original, [self.change])
        self.assertEqual(self.original, before)
        self.assertEqual(policy["frozen_mother_core_points"], before)
        self.assertEqual(policy["effective_checkpoints"][0]["requirement"], "使用单掌自然推进遮镜")
        self.assertEqual(policy["effective_checkpoints"][1], before[1])
        self.assertEqual(policy["effective_checkpoints"][0]["evidence_source"], "model_extracted")

    def test_unknown_point_rejected_without_prefix_or_fuzzy_matching(self):
        for point in ("Cover", " cover", "cover.extra", "unknown"):
            with self.subTest(point=point), self.assertRaisesRegex(ValueError, "UNKNOWN_POINT_ID"):
                freeze_checkpoints(self.original, [{**self.change, "point_id": point}])

    def test_duplicate_changes_and_duplicate_original_rejected(self):
        with self.assertRaisesRegex(ValueError, "DUPLICATE_POINT_ID"):
            freeze_checkpoints(self.original, [self.change, self.change])
        with self.assertRaisesRegex(ValueError, "DUPLICATE_POINT_ID"):
            freeze_checkpoints([self.original[0], self.original[0]])

    def test_deleting_or_blank_or_reasonless_change_rejected(self):
        for altered in ({"operation": "remove"}, {"replacement_requirement": " "}, {"reason": ""}, {"extra": True}):
            with self.subTest(altered=altered), self.assertRaises(ValueError):
                freeze_checkpoints(self.original, [{**self.change, **altered}])

    def test_direct_effective_override_cannot_bypass_explicit_changes(self):
        changed = freeze_checkpoints(self.original, [self.change])["effective_checkpoints"]
        with self.assertRaisesRegex(ValueError, "NOT_EXPLICITLY_AUTHORIZED"):
            freeze_checkpoints(self.original, effective_checkpoints=changed)
        self.assertEqual(freeze_checkpoints(self.original, [self.change], changed)["effective_checkpoints"], changed)


class EffectiveReviewIntegrationTest(EffectiveCheckpointTest):
    def setUp(self):
        super().setUp()
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        video = self.root / "fake.mp4"
        video.write_bytes(b"fake-not-a-real-video-never-external")
        self.job = {"script_id": "wsr_revision", "video_path": str(video), "prompt": "单掌遮镜后换发",
                    "mother_core_points": copy.deepcopy(self.original), "allowed_changes": [self.change]}

    def test_model_receives_only_effective_action_requirements_original_frozen_locally(self):
        reviewer = Mock(return_value=response())
        result = review_job(self.job, self.root / "state", apply_model=True, reviewer=reviewer,
                            preparer=Mock(return_value=({"duration_seconds": 15}, [])))
        context = reviewer.call_args.args[0]
        self.assertEqual(context["mother_core_points"], context["effective_checkpoints"])
        self.assertNotIn("胸前展示双掌", json.dumps(context, ensure_ascii=False))
        self.assertIn("单掌自然推进遮镜", json.dumps(context, ensure_ascii=False))
        frozen = json.loads((self.root / "state" / result["review_id"] / "requirements.json").read_text())
        self.assertEqual(frozen["frozen_mother_core_points"], self.original)
        self.assertEqual(frozen["effective_checkpoints"], context["effective_checkpoints"])
        self.assertEqual(frozen["allowed_changes"], [self.change])
        reviewer.assert_called_once()

    def test_cache_identity_includes_full_effective_policy_and_revision_description(self):
        base = input_bundle(self.job)["review_id"]
        for edit in ("requirement", "reason", "instruction", "interval"):
            job = copy.deepcopy(self.job)
            if edit == "requirement":
                job["allowed_changes"][0]["replacement_requirement"] += "且手掌覆盖镜头"
            elif edit == "reason":
                job["allowed_changes"][0]["reason"] += "第二次确认"
            elif edit == "instruction":
                job["revision_instruction"] = "节奏更自然，非核心要求不豁免"
            else:
                job["review_intervals"] = [{"start_seconds": 6, "end_seconds": 9}]
            self.assertNotEqual(base, input_bundle(job)["review_id"])

    def test_policy_version_invalidates_cache(self):
        base = input_bundle(self.job)["review_id"]
        with patch("wsr_content_review.POLICY_VERSION", "next-test-policy"):
            self.assertNotEqual(base, input_bundle(self.job)["review_id"])

    def test_revision_instruction_does_not_implicitly_exempt_original_details(self):
        job = {**self.job, "allowed_changes": [], "revision_instruction": "改成单掌"}
        self.assertEqual(input_bundle(job)["effective_checkpoints"], self.original)

    def test_invalid_policy_stops_before_state_preparer_and_model(self):
        reviewer, preparer = Mock(), Mock()
        self.job["allowed_changes"] = [{**self.change, "point_id": "not_known"}]
        with self.assertRaisesRegex(ValueError, "UNKNOWN_POINT_ID"):
            review_job(self.job, self.root / "state", apply_model=True, reviewer=reviewer, preparer=preparer)
        reviewer.assert_not_called(); preparer.assert_not_called()
        self.assertFalse((self.root / "state").exists())

    def test_conflicting_frozen_and_original_input_rejected(self):
        self.job["frozen_mother_core_points"] = []
        with self.assertRaisesRegex(ValueError, "FROZEN_MOTHER"):
            input_bundle(self.job)


class SamplingTest(unittest.TestCase):
    def test_short_video_dense_coverage_includes_unpredicted_transition(self):
        timestamps = [i / 30 for i in range(450)]
        indices = plan_frame_indices(timestamps, 15, normalize_sampling([]))
        chosen = [timestamps[i] for i in indices]
        self.assertLessEqual(len(chosen), MAX_FRAME_BUDGET)
        self.assertEqual(chosen[0], 0); self.assertEqual(chosen[-1], timestamps[-1])
        self.assertLessEqual(max(b - a for a, b in zip(chosen, chosen[1:])), .134)
        # Actual reveal at an unexpected late time is covered as densely as the start.
        self.assertGreaterEqual(len([t for t in chosen if 12 <= t < 13]), 8)

    def test_explicit_intervals_densified_without_losing_full_clip_coverage(self):
        timestamps = [i / 30 for i in range(1800)]
        sampling = normalize_sampling([{"start_seconds": 37, "end_seconds": 39, "point_id": "cover"}])
        chosen = [timestamps[i] for i in plan_frame_indices(timestamps, 60, sampling)]
        self.assertLessEqual(len(chosen), MAX_FRAME_BUDGET)
        self.assertEqual(chosen[0], 0); self.assertEqual(chosen[-1], timestamps[-1])
        self.assertGreaterEqual(len([t for t in chosen if 37 <= t <= 39]), 24)
        self.assertLessEqual(max(b - a for a, b in zip(chosen, chosen[1:])), 1.034)

    def test_very_broad_intervals_bounded_and_not_head_truncated(self):
        ts = [i / 30 for i in range(1800)]
        indices = plan_frame_indices(ts, 60, normalize_sampling([{"start_seconds": 0, "end_seconds": 60}]))
        self.assertLessEqual(len(indices), MAX_FRAME_BUDGET)
        self.assertEqual(indices[-1], len(ts) - 1)

    def test_invalid_intervals_rejected(self):
        for intervals in ([{"start_seconds": 2, "end_seconds": 1}], [{"start_seconds": float("nan"), "end_seconds": 2}],
                          [{"start_seconds": True, "end_seconds": 2}], [{"start_seconds": 0, "end_seconds": 61}]):
            with self.subTest(intervals=intervals), self.assertRaises(ValueError):
                normalize_sampling(intervals)
        with self.assertRaisesRegex(ValueError, "OUTSIDE_VIDEO"):
            plan_frame_indices([0, 1], 2, normalize_sampling([{"start_seconds": 1, "end_seconds": 3}]))

    def test_extraction_uses_real_vfr_pts_and_exact_frame_indices_not_index_over_fps(self):
        pts = [5.0, 5.033, 5.091, 5.25, 5.38, 5.54, 5.61, 5.8, 5.999]
        probe = {"format": {"duration": "1"}, "frames": [{"best_effort_timestamp_time": str(t)} for t in pts]}
        commands = []
        with tempfile.TemporaryDirectory() as tmp:
            def fake_run(args, **_kwargs):
                commands.append(args)
                if args[0] == "ffprobe":
                    return subprocess.CompletedProcess(args, 0, stdout=json.dumps(probe).encode())
                count = int(args[args.index("-frames:v") + 1])
                for i in range(count):
                    Image.new("RGB", (4, 4), "pink").save(args[-1].replace("%03d", f"{i + 1:03d}"))
                return subprocess.CompletedProcess(args, 0, stdout=b"")
            with patch("wsr_content_review.binary", side_effect=lambda value: value), patch("wsr_content_review._run", side_effect=fake_run):
                media, sheets = prepare_evidence({"video_path": "/fake-never-opened.mp4", "sampling": normalize_sampling([])}, Path(tmp))
            self.assertTrue(sheets)
            self.assertEqual(media["timestamp_origin_seconds"], 5)
            for frame in media["frames"]:
                self.assertAlmostEqual(frame["seconds"], pts[frame["source_frame_index"]] - 5)
            self.assertIn("select=", commands[-1][commands[-1].index("-vf") + 1])
            self.assertNotIn("-ss", commands[-1])
            self.assertTrue(media["frame_timestamps_are_approximate"])
            self.assertIn("帧间动作", media["timestamp_note"])


if __name__ == "__main__":
    unittest.main()
