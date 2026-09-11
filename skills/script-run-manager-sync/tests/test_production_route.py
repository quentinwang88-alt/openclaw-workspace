import unittest

from core.production_route import ProductionRoute, classify_production_route


MAPPING = {
    "script_source": "脚本来源",
    "video_duration": "视频时长",
    "video_format": "视频形态（系统）",
}


class ProductionRouteTests(unittest.TestCase):
    def test_blank_format_long_remake_is_segmented(self):
        decision = classify_production_route(
            {"脚本来源": "视频复刻", "视频时长": "41"}, MAPPING
        )
        self.assertEqual(decision.route, ProductionRoute.REMAKE_SEGMENTED)

    def test_short_remake_keeps_existing_run_manager(self):
        decision = classify_production_route(
            {"脚本来源": "视频复刻", "视频时长": 10}, MAPPING
        )
        self.assertEqual(decision.route, ProductionRoute.SHORT_VIDEO_RUN_MANAGER)

    def test_original_longform_keeps_plan_c_owner(self):
        decision = classify_production_route(
            {"脚本来源": "原创生成", "视频时长": 40, "视频形态（系统）": "长视频"}, MAPPING
        )
        self.assertEqual(decision.route, ProductionRoute.ORIGINAL_LONGFORM)

    def test_wrong_format_does_not_force_long_remake_into_short_worker(self):
        decision = classify_production_route(
            {"脚本来源": "视频复刻", "视频时长": 39, "视频形态（系统）": "短视频"}, MAPPING
        )
        self.assertEqual(decision.route, ProductionRoute.REMAKE_SEGMENTED)
        self.assertTrue(decision.conflict)


if __name__ == "__main__":
    unittest.main()
