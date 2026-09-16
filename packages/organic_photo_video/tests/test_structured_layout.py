"""structured_v1 结构化排版（2026-09-15 排版轮批次 B）。"""
from __future__ import annotations

import json
import unittest
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from services.photo_package import normalize_photo_template
from services.photo_structured_layout import (
    STRUCTURED_RENDERER_VERSION, StructuredLayoutError, parse_page_spec,
    render_structured_page, wrap_text,
)

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
FONT_REGULAR = PACKAGE_ROOT / "assets/fonts/Sarabun-Regular.ttf"
FONT_BOLD = PACKAGE_ROOT / "assets/fonts/Sarabun-Bold.ttf"

THAI_COMBINING = set(
    chr(c) for c in (list(range(0x0E31, 0x0E32)) + list(range(0x0E34, 0x0E3B))
                     + list(range(0x0E47, 0x0E4F))))


class PageSpecParseTest(unittest.TestCase):
    def test_cover_two_lines_and_single_line(self):
        spec = parse_page_spec("อเมริกามูระ\nอากาศเย็นใส่อะไรดี?", index=1, cover_index=1, total=4)
        self.assertEqual(spec["page_kind"], "cover")
        self.assertEqual(spec["kicker"], "อเมริกามูระ")
        self.assertEqual(spec["headline"], "อากาศเย็นใส่อะไรดี?")
        single = parse_page_spec("อากาศเย็นใส่อะไรดี?", index=1, cover_index=1, total=4)
        self.assertEqual((single["kicker"], single["headline"]), ("", "อากาศเย็นใส่อะไรดี?"))

    def test_detail_name_reason_split(self):
        spec = parse_page_spec(
            "B · กางเกงผ้าสแลค — ช่วงล่างเรียวยาวช่วยให้ดูสูง",
            index=2, cover_index=1, total=4)
        self.assertEqual(spec["page_kind"], "detail")
        self.assertEqual(spec["headline"], "B · กางเกงผ้าสแลค")
        self.assertEqual(spec["body"], "ช่วงล่างเรียวยาวช่วยให้ดูสูง")
        self.assertEqual(spec["cta"], "")

    def test_final_headline_and_cta(self):
        spec = parse_page_spec(
            "D · ลุคคลาสสิก — สีพื้นเรียบง่าย\nเซฟไว้ก่อนไปเที่ยวนะ",
            index=4, cover_index=1, total=4)
        self.assertEqual(spec["page_kind"], "final")
        self.assertEqual(spec["headline"], "D · ลุคคลาสสิก")
        self.assertEqual(spec["body"], "สีพื้นเรียบง่าย")
        self.assertEqual(spec["cta"], "เซฟไว้ก่อนไปเที่ยวนะ")


class ThaiWrapTest(unittest.TestCase):
    def setUp(self):
        self.font = ImageFont.truetype(str(FONT_REGULAR), 40)
        self.draw = ImageDraw.Draw(Image.new("RGB", (1080, 1920)))

    def test_long_thai_never_breaks_combining_marks(self):
        text = "ลุคเดินเล่นในเมืองเก่าสไตล์ญี่ปุ่นอากาศเย็นลมพัดเบาๆ" * 2
        lines = wrap_text(text, self.font, max_width=560, draw=self.draw)
        self.assertGreater(len(lines), 1)
        for line in lines:
            self.assertFalse(line[0] in THAI_COMBINING, "断行不得以组合字符开头")
        # 无字符丢失
        self.assertEqual("".join(lines), text)

    def test_short_text_single_line(self):
        self.assertEqual(wrap_text("สวัสดี", self.font, max_width=560, draw=self.draw), ["สวัสดี"])


class RenderTest(unittest.TestCase):
    def _template(self, name):
        payload = json.loads((PACKAGE_ROOT / "config/layouts" / f"{name}.json")
                             .read_text(encoding="utf-8"))
        return normalize_photo_template(payload)

    def test_clean_and_scene_render_all_pages(self):
        for name, style in (("PHOTO_STRUCTURED_CLEAN_V1", "clean"),
                            ("PHOTO_STRUCTURED_SCENE_V1", "scene")):
            template = self._template(name)
            self.assertEqual(template["overlay_style"], STRUCTURED_RENDERER_VERSION)
            texts = [
                "อเมริกามูระ\nอากาศเย็นใส่อะไรดี?",
                "B · กางเกงผ้าสแลค — ช่วยให้ดูสูงและเดินสบาย",
                "C · ลุคไปคาเฟ่ — โทนอบอุ่นเหมาะกับร้านกาแฟ",
                "D · ลุคคลาสสิก — สีพื้นเรียบง่าย\nเซฟไว้ก่อนไปเที่ยวนะ",
            ]
            for index, text in enumerate(texts, 1):
                image = Image.new("RGB", (template["width"], template["height"]), "#FAF8F4")
                info = render_structured_page(
                    image, text, template, index=index, cover_index=1, total=4)
                self.assertEqual(info["renderer"], STRUCTURED_RENDERER_VERSION)
                self.assertEqual(info["style"], style)
                # 文字确实画上去了（不再是纯底色）
                colors = image.getcolors(maxcolors=8)
                self.assertIsNone(colors, "渲染后应有多种颜色（文字/渐变）")

    def test_deployed_fonts_exist(self):
        self.assertTrue(FONT_REGULAR.is_file() and FONT_BOLD.is_file())
        self.assertEqual(ImageFont.truetype(str(FONT_BOLD), 40).getname()[1], "Bold")

    def test_unknown_overlay_style_rejected(self):
        bad = {"schema_version": "opv-photo-layout-v2", "layout_id": "X",
               "layout_version": 1,
               "render_options": {"overlay_style": "fancy_v9"}}
        from services.photo_package import PhotoPackageError
        with self.assertRaises(PhotoPackageError):
            normalize_photo_template(bad)

    def test_text_never_dropped_when_tight(self):
        template = self._template("PHOTO_STRUCTURED_CLEAN_V1")
        long_reason = "คำอธิบายที่ยาวมาก" * 200
        image = Image.new("RGB", (template["width"], template["height"]), "#FFFFFF")
        with self.assertRaises(StructuredLayoutError):
            render_structured_page(
                image, f"B · ลุค — {long_reason}", template,
                index=2, cover_index=1, total=4)

    def test_scrim_gradient_single_pass_trapezoid(self):
        # 2026-09-16 修复回归（评审 §C 探针）：文字带内必须有稳定衬底，
        # 边缘淡出，且不得因同层叠画导致 alpha 累积成整块实色。
        from services.photo_structured_layout import _draw_scrim
        image = Image.new("RGB", (200, 400), "#FFFFFF")
        template = {"scrim_color": "#000000", "scrim_alpha": 128}
        _draw_scrim(image, top_zone=True, top=100, bottom=220,
                    left=10, right=190, template=template)
        def lum(y):
            px = image.getpixel((100, y))
            return sum(px[:3]) / 3
        self.assertLess(lum(160), 200)      # 文字带中心：明显衬底
        self.assertGreater(lum(160), 40)    # 但不是实黑（单次绘制）
        self.assertGreater(lum(60), 240)    # 带外上方：接近原底
        self.assertGreater(lum(360), 240)   # 带外下方：接近原底
        # 带内稳定、向两侧淡出单调
        self.assertEqual(lum(105), lum(215))          # 带内左右对称处同亮度
        self.assertLess(lum(100), lum(90))            # 带边比淡出区更暗
        self.assertLess(lum(220), lum(240))


if __name__ == "__main__":
    unittest.main()
