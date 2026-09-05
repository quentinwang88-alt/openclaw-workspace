import copy
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock

from PIL import Image
from wsr_content_review import CHECK_IDS, file_digest, input_bundle, normalize_result, review_job


def response(status="pass"):
    return {"summary": "已对照实际抽帧", "checks": [
        {"check_id": key, "status": status, "reason": "观察结果", "evidence": [
            {"start_seconds": 1, "end_seconds": 2, "description": "在对应时间看见动作"}]} for key in CHECK_IDS]}


class ReviewTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        video = self.root / "video.mp4"
        video.write_bytes(b"local-test-video-not-sent-to-model")
        ref = self.root / "ref.png"
        Image.new("RGB", (8, 8), "pink").save(ref)
        self.job = {"script_id": "wsr_test", "record_id": "recTest", "video_path": str(video), "prompt": "混色、遮镜、揭晓",
                    "mother_core_points": [{"point_id": "reveal", "requirement": "遮镜后揭晓"}],
                    "reference_assets": [{"role": role, "path": str(ref), "original_sha256": file_digest(ref)}
                                         for role in ("person_identity", "product")]}
        self.prepare = Mock(return_value=({"duration_seconds": 15}, []))
        self.state = self.root / "state"

    def run_review(self, reviewer):
        return review_job(self.job, self.state, apply_model=True, reviewer=reviewer, preparer=self.prepare)

    def test_dry_run_no_state_or_model(self):
        reviewer = Mock()
        result = review_job(self.job, self.state, reviewer=reviewer, preparer=self.prepare)
        self.assertTrue(result["dry_run"])
        self.assertFalse(self.state.exists())
        reviewer.assert_not_called()
        self.prepare.assert_not_called()

    def test_idempotent_and_locator_independent(self):
        reviewer = Mock(return_value=response())
        first = self.run_review(reviewer)
        self.job["record_id"] = "recOther"
        self.job["reference_assets"][0]["file_token"] = "new-token"
        second = self.run_review(reviewer)
        self.assertEqual(first["status"], "pass")
        self.assertTrue(second["cached"])
        self.assertEqual(reviewer.call_count, 1)

    def test_changed_prompt_invalidates(self):
        key = input_bundle(self.job)["review_id"]
        self.job["prompt"] += "厨房"
        self.assertNotEqual(key, input_bundle(self.job)["review_id"])

    def test_changed_role_changes_identity(self):
        key = input_bundle(self.job)["review_id"]
        self.job["reference_assets"].reverse()
        self.assertNotEqual(key, input_bundle(self.job)["review_id"])

    def test_hash_mismatch_stops_before_model(self):
        self.job["reference_assets"][0]["original_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "HASH_MISMATCH"):
            self.run_review(Mock())

    def test_derivative_is_not_original_hash(self):
        asset = self.job["reference_assets"][0]
        asset["derived_sha256"] = asset["original_sha256"]
        asset["original_sha256"] = "0" * 64
        bundle = input_bundle(self.job)
        self.assertNotEqual(bundle["reference_assets"][0]["original_sha256"], bundle["reference_assets"][0]["input_sha256"])

    def test_transient_retries_only_review_twice(self):
        reviewer = Mock(side_effect=TimeoutError("secret-must-not-appear"))
        first = self.run_review(reviewer)
        second = self.run_review(reviewer)
        self.assertEqual(first["status"], "unknown")
        self.assertEqual(first["attempts"], 2)
        self.assertEqual(reviewer.call_count, 2)
        self.assertTrue(second["cached"])
        self.assertNotIn("secret", json.dumps(first))

    def test_defect_is_not_content_failure_or_retry(self):
        reviewer = Mock(side_effect=ValueError("invalid response"))
        result = self.run_review(reviewer)
        self.assertEqual(result["status"], "unknown")
        self.assertEqual(reviewer.call_count, 1)

    def test_content_failure_keeps_evidence_no_retry(self):
        reviewer = Mock(return_value=response("fail"))
        result = self.run_review(reviewer)
        self.assertEqual(result["status"], "fail")
        self.assertTrue(result["evidence"])
        self.assertEqual(reviewer.call_count, 1)

    def test_failure_without_evidence_is_unknown(self):
        raw = response("fail")
        for check in raw["checks"]:
            check["evidence"] = []
        self.assertEqual(normalize_result(raw, 15, has_mother=True)["status"], "unknown")

    def test_missing_mother_not_pass(self):
        self.assertEqual(normalize_result(response(), 15, has_mother=False)["status"], "unknown")

    def test_invalid_times_not_pass(self):
        raw = response()
        raw["checks"][0]["evidence"][0]["end_seconds"] = 20
        self.assertEqual(normalize_result(raw, 15, has_mother=True)["status"], "unknown")

    def test_missing_reference_not_pass(self):
        self.assertEqual(normalize_result(response(), 15, has_mother=True, reference_roles=set())["status"], "unknown")

    def test_missing_duplicate_check_not_pass(self):
        raw = response()
        raw["checks"][-1] = copy.deepcopy(raw["checks"][0])
        self.assertEqual(normalize_result(raw, 15, has_mother=True)["status"], "unknown")

    def test_crashed_reserved_attempt_not_reset(self):
        self.state.mkdir()
        key = input_bundle(self.job)["review_id"]
        with sqlite3.connect(self.state / "reviews.sqlite3") as db:
            db.execute("CREATE TABLE reviews(review_id TEXT PRIMARY KEY, attempts INTEGER NOT NULL, result_json TEXT)")
            db.execute("INSERT INTO reviews VALUES (?,2,NULL)", (key,))
        reviewer = Mock()
        self.assertEqual(self.run_review(reviewer)["attempts"], 2)
        reviewer.assert_not_called()


if __name__ == "__main__":
    unittest.main()
