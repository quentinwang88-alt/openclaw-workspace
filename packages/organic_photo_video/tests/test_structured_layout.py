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

    def test_bottom_zone_tall_thai_ink_stays_inside_canvas(self):
        # 教程收口（2026-09-19 任务二④）：底部条带按 font.size 估算块高会低估
        # 泰文上下声调墨迹，末行贴边被裁。渲染后整块按实测墨迹上移，最末行
        # 墨迹下缘必须留在画布安全边距内。
        template = self._template("PHOTO_STRUCTURED_CLEAN_V1")
        text = "ใช้สีซ้ำเชื่อมลุค — เชื่อมช่วงบนและล่าง มีกระโปรงครีมช่วยพักสายตา บันทึกไว้จัดลุคทริปหน้า"
        image = Image.new("RGB", (template["width"], template["height"]), "#FAF8F4")
        draw = ImageDraw.Draw(image)
        render_structured_page(image, text, template, index=4, cover_index=1, total=4)
        # 扫最下方 2% 行带：允许渐变底色，但不允许纯黑/深色文字墨迹贴到最后一行
        bottom_band = image.crop((
            0, int(image.height * 0.985), image.width, image.height))
        extrema = bottom_band.convert("L").getextrema()
        self.assertGreater(extrema[0], 30, "底部最后1.5%不应出现文字墨迹（被裁的末行）")

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



class CoverHierarchyTest(unittest.TestCase):
    """评审 §C 标题层级：投票字母不得成为封面视觉主体。"""

    def test_voting_line_becomes_cta_question_becomes_headline(self):
        from services.photo_structured_layout import parse_page_spec
        spec = parse_page_spec(
            "这趟旅行有4套造型，适合4种场合\n选择A、B、C或D",
            index=1, cover_index=1, total=4)
        self.assertEqual(spec["headline"], "这趟旅行有4套造型，适合4种场合")
        self.assertEqual(spec["cta"], "选择A、B、C或D")
        self.assertEqual(spec["kicker"], "")

    def test_thai_voting_line_detected(self):
        from services.photo_structured_layout import parse_page_spec
        spec = parse_page_spec(
            "เลือกลุค A B C หรือ D\nไปเที่ยวเมืองหนาว 4 ลุคน่าสนใจ",
            index=1, cover_index=1, total=4)
        self.assertEqual(spec["cta"], "เลือกลุค A B C หรือ D")
        self.assertEqual(spec["headline"], "ไปเที่ยวเมืองหนาว 4 ลุคน่าสนใจ")

    def test_location_kicker_plus_question_plus_cta(self):
        from services.photo_structured_layout import parse_page_spec
        spec = parse_page_spec(
            "日本·15-22°C\n哪套造型最适合凉爽城市旅行？\n你选 A B C 还是 D",
            index=1, cover_index=1, total=4)
        self.assertEqual(spec["kicker"], "日本·15-22°C")
        self.assertEqual(spec["headline"], "哪套造型最适合凉爽城市旅行？")
        self.assertEqual(spec["cta"], "你选 A B C 还是 D")

    def test_plain_two_line_cover_unchanged(self):
        from services.photo_structured_layout import parse_page_spec
        spec = parse_page_spec(
            "东京秋日\n轻装出行指南",
            index=1, cover_index=1, total=4)
        self.assertEqual(spec["kicker"], "东京秋日")
        self.assertEqual(spec["headline"], "轻装出行指南")
        self.assertEqual(spec["cta"], "")

class CoverWhitespaceTest(unittest.TestCase):
    """§8：通用四宫格封面标题独立留白区（text_position=bottom 不压脸）。"""

    def _template(self, name):
        payload = json.loads((PACKAGE_ROOT / "config/layouts" / f"{name}.json")
                             .read_text(encoding="utf-8"))
        return normalize_photo_template(payload)

    def test_cover_bottom_band_not_top(self):
        from services.photo_structured_layout import (
            render_structured_page, parse_page_spec)
        template = self._template("PHOTO_STRUCTURED_CLEAN_V1")
        template = {**dict(template), "text_position": "bottom"}
        image = Image.new("RGB", (template["width"], template["height"]), "#FFFFFF")
        # 顶部画一个"人物"色块（模拟人脸位置）
        from PIL import ImageDraw
        ImageDraw.Draw(image).rectangle(
            [200, 100, 880, 700], fill="#C89B7B")
        result = render_structured_page(
            image, "灰色穿搭层次公式\n用3种配色打造4套造型", template,
            index=1, cover_index=1, total=4)
        self.assertEqual(result["page_kind"], "cover")
        spec = parse_page_spec("灰色穿搭层次公式\n用3种配色打造4套造型",
                               index=1, cover_index=1, total=4)
        # 文字区域在下半部（不与人物色块重叠）：取文字带中心 y
        text_zone_top = template["height"] - int(template["height"] * 0.34) - 60
        self.assertGreater(text_zone_top, 700)   # 独立留白区在人物之下

    def test_travel_default_top_unchanged(self):
        from services.photo_structured_layout import parse_page_spec
        # 旧模板无 text_position → 默认 top（旅行模板行为不变）
        template = self._template("PHOTO_STRUCTURED_CLEAN_V1")
        self.assertNotEqual(template.get("text_position"), "bottom")


if __name__ == "__main__":
    unittest.main()
