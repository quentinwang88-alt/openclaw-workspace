import unittest
from repositories.rds_repository import RdsRepository
from services.styling_normalizer import outfit_fingerprint


class DiversityHistoryTests(unittest.TestCase):
    def test_failed_planning_is_excluded_but_rendered_content_remains_usage(self):
        repository = RdsRepository.__new__(RdsRepository)
        calls = []
        frozen = {"recipe": {"bottom": "白色阔腿裤", "top_inner": "白色T恤", "footwear": "白鞋"}}
        def fetch(sql, params):
            calls.append((sql, params))
            return [
                {"task_id": "T", "plan_json": {"content_signature": {"axes": {"look_ref": "LOOK", "visible_silhouette": "BLUE_DISTRESSED_DENIM"}},
                                               "look": {"snapshot": frozen}}, "task_status": "video_review"},
                {"task_id": "EMPTY", "plan_json": {}},
            ]
        repository._fetch_all = fetch
        rows = repository.list_recent_diversity_axes("A", exclude_source_record_id="record")
        sql, params = calls[0]
        self.assertIn("task_status NOT IN ('failed','cancelled','canceled')", sql)
        self.assertIn("r.qc_status='passed'", sql)
        self.assertIn("record", params)
        self.assertIn("record:%", params)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["outfit_fingerprint"], outfit_fingerprint(frozen))
        self.assertEqual(rows[0]["silhouette_key"], "WHITE_WIDE_PANTS")

    def test_batch_alternate_axes_are_retained_with_legacy_signature(self):
        repository = RdsRepository.__new__(RdsRepository)
        repository._fetch_all = lambda *args: [{"task_id": "T", "plan_json": {
            "batch_diversity": {"axes": {"alternate_look_ref": "A", "alternate_outfit_fingerprint": "fingerprint-a"}},
            "content_signature": {"axes": {"look_ref": "B", "visible_silhouette": "WHITE_WIDE_PANTS"}},
        }}]
        rows = repository.list_recent_diversity_axes("A")
        self.assertEqual(rows[0]["alternate_look_ref"], "A")
        self.assertEqual(rows[0]["alternate_outfit_fingerprint"], "fingerprint-a")


if __name__ == "__main__":
    unittest.main()
