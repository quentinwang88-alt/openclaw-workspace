import unittest

from scripts.ensure_feishu_listing_statuses import classify_legacy_error


STATUSES = {
    "pending_verification": "待核验",
    "needs_input": "待补资料",
}


class ListingStatusMigrationTest(unittest.TestCase):
    def test_pending_submission_is_reclassified(self):
        self.assertEqual(
            classify_legacy_error("发布已提交，禁止自动重发", STATUSES), "待核验"
        )

    def test_material_error_is_reclassified(self):
        self.assertEqual(
            classify_legacy_error("SIZE_CHART_REQUIRED：缺尺码图", STATUSES),
            "待补资料",
        )

    def test_system_error_is_preserved(self):
        self.assertEqual(classify_legacy_error("LOGIN_EXPIRED", STATUSES), "")


if __name__ == "__main__":
    unittest.main()
