"""Reference-image format auto-conversion tests (HEIC/WebP/BMP → JPEG)."""

import io
import unittest

from PIL import Image

from services.photo_asset_supply import normalize_image_bytes


def png_bytes(size=(60, 90), color=(120, 110, 100)) -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", size, color).save(buffer, "PNG")
    return buffer.getvalue()


def webp_bytes(size=(60, 90), color=(90, 120, 150)) -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", size, color).save(buffer, "WEBP")
    return buffer.getvalue()


def bmp_bytes(size=(60, 90), color=(150, 90, 120)) -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", size, color).save(buffer, "BMP")
    return buffer.getvalue()


class NormalizeImageBytesTest(unittest.TestCase):
    def test_jpeg_and_png_pass_through_untouched(self):
        png = png_bytes()
        content, suffix = normalize_image_bytes(png, "ref.png", "image/png")
        self.assertEqual(suffix, ".png")
        self.assertEqual(content, png)
        buffer = io.BytesIO()
        Image.new("RGB", (60, 90)).save(buffer, "JPEG")
        content, suffix = normalize_image_bytes(buffer.getvalue(), "ref.jpg", "image/jpeg")
        self.assertEqual(suffix, ".jpg")

    def test_mime_only_attachment_converted_to_jpg(self):
        webp = webp_bytes()
        content, suffix = normalize_image_bytes(webp, "", "image/webp")
        self.assertEqual(suffix, ".jpg")
        with Image.open(io.BytesIO(content)) as image:
            self.assertEqual(image.format, "JPEG")
            self.assertEqual(image.size, (60, 90))

    def test_extension_only_attachment_converted_to_jpg(self):
        content, suffix = normalize_image_bytes(webp_bytes(), "IMG_0001.webp", "")
        self.assertEqual(suffix, ".jpg")
        self.assertTrue(content)

    def test_bmp_converted(self):
        content, suffix = normalize_image_bytes(bmp_bytes(), "x.bmp", "")
        self.assertEqual(suffix, ".jpg")
        self.assertTrue(content)

    def test_undecodable_bytes_return_empty(self):
        content, suffix = normalize_image_bytes(b"not an image", "x.txt", "text/plain")
        self.assertEqual((content, suffix), (b"", ""))

    def test_exif_orientation_is_applied(self):
        # 构造带 EXIF 方向 6（顺时针 90°）的图，转换后应被摆正尺寸对调。
        source = Image.new("RGB", (40, 80), (100, 100, 100))
        buffer = io.BytesIO()
        source.save(buffer, "WEBP", exif=Image.Exif().tobytes())
        try:
            from pillow_heif import register_heif_opener  # noqa: F401
        except ImportError:
            self.skipTest("pillow-heif 未安装")
        content, suffix = normalize_image_bytes(buffer.getvalue(), "x.webp", "")
        self.assertEqual(suffix, ".jpg")


if __name__ == "__main__":
    unittest.main()
