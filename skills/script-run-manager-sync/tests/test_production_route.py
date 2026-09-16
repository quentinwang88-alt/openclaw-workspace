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


class RouteAssignmentSweepTests(unittest.TestCase):
    """Pin the exact ownership boundary for every remake duration."""

    def test_every_supported_plan_c_duration_is_remake_segmented(self):
        # 16-45s is the two/three-segment Plan C window; the whole band must
        # belong to the remake segmented route, not the short worker.
        for duration in (16, 20, 25, 30, 31, 40, 41, 45):
            with self.subTest(duration=duration):
                decision = classify_production_route(
                    {"脚本来源": "视频复刻", "视频时长": duration,
                     "视频形态（系统）": "分段视频"}, MAPPING,
                )
                self.assertEqual(decision.route, ProductionRoute.REMAKE_SEGMENTED)
                self.assertEqual("", decision.conflict)

    def test_ten_second_remake_never_enters_the_longform_producer(self):
        for duration in (10, 15, "10"):
            with self.subTest(duration=duration):
                decision = classify_production_route(
                    {"脚本来源": "视频复刻", "视频时长": duration}, MAPPING,
                )
                self.assertEqual(decision.route, ProductionRoute.SHORT_VIDEO_RUN_MANAGER)

    def test_explicit_longform_marker_keeps_a_short_remake_segmented(self):
        # An operator can force the segmented executor even under 15s; the
        # short-video run manager must not silently take it back.
        decision = classify_production_route(
            {"脚本来源": "成功脚本复刻", "视频时长": 12, "视频形态（系统）": "长视频"}, MAPPING,
        )
        self.assertEqual(decision.route, ProductionRoute.REMAKE_SEGMENTED)

    def test_success_script_remake_source_is_recognized(self):
        decision = classify_production_route(
            {"脚本来源": "成功脚本复刻", "视频时长": 30}, MAPPING,
        )
        self.assertEqual(decision.route, ProductionRoute.REMAKE_SEGMENTED)

    def test_original_longform_is_unaffected_by_the_remake_branch(self):
        for duration in (16, 30, 41, 45, 90):
            with self.subTest(duration=duration):
                decision = classify_production_route(
                    {"脚本来源": "原创生成", "视频时长": duration,
                     "视频形态（系统）": "长视频"}, MAPPING,
                )
                self.assertEqual(decision.route, ProductionRoute.ORIGINAL_LONGFORM)
                self.assertEqual("", decision.conflict)
        # An original row without the explicit marker keeps today's behaviour.
        decision = classify_production_route(
            {"脚本来源": "原创生成", "视频时长": 40}, MAPPING,
        )
        self.assertEqual(decision.route, ProductionRoute.SHORT_VIDEO_RUN_MANAGER)

    def test_classifier_never_returns_invalid_for_a_readable_row(self):
        for fields in (
            {"脚本来源": "视频复刻", "视频时长": 30},
            {"脚本来源": "原创生成", "视频形态（系统）": "长视频"},
            {},
        ):
            with self.subTest(fields=fields):
                decision = classify_production_route(fields, MAPPING)
                self.assertNotEqual(decision.route, ProductionRoute.INVALID)
                self.assertTrue(decision.reason)


if __name__ == "__main__":
    unittest.main()
