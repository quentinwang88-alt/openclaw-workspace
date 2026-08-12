import unittest

from miaoshou_auto_listing.browser.session import validate_cdp_targets


class CdpTargetValidationTest(unittest.TestCase):
    def test_accepts_dedicated_miaoshou_browser(self) -> None:
        validate_cdp_targets(
            [
                {
                    "type": "page",
                    "title": "妙手-采集箱",
                    "url": "https://erp.91miaoshou.com/tiktok/collect_box/items",
                },
                {"type": "page", "title": "", "url": "about:blank"},
            ],
            "erp.91miaoshou.com",
        )

    def test_rejects_port_without_miaoshou(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "wrong port"):
            validate_cdp_targets(
                [
                    {
                        "type": "page",
                        "title": "NeoBund",
                        "url": "https://www.neobund.ai/np",
                    }
                ],
                "erp.91miaoshou.com",
            )

    def test_rejects_shared_browser_even_with_miaoshou_open(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "foreign pages"):
            validate_cdp_targets(
                [
                    {
                        "type": "page",
                        "title": "妙手-采集箱",
                        "url": "https://erp.91miaoshou.com/tiktok/collect_box/items",
                    },
                    {
                        "type": "page",
                        "title": "NeoBund",
                        "url": "https://www.neobund.ai/np",
                    },
                ],
                "erp.91miaoshou.com",
            )


if __name__ == "__main__":
    unittest.main()
