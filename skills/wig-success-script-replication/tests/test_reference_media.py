import io
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from production_runtime import FeishuClient
from wig_success_replication.reference_manifest import bytes_sha256, build_reference_manifest


class ReferenceMediaTest(unittest.TestCase):
    def setUp(self):
        from PIL import Image
        output = io.BytesIO()
        Image.new("RGB", (16, 12), "white").save(output, format="PNG")
        self.original = output.getvalue()
        self.client = FeishuClient("test", "test")
        self.client.token = Mock(return_value="test-token")
        self.client.request = Mock(side_effect=lambda *args, **kwargs: {"tmp_download_urls": [
            {"file_token": f"t{i}", "tmp_download_url": f"https://example.test/{i}"} for i in range(9)
        ]})

    def response(self, content):
        return Mock(content=content, headers={"Content-Type": "image/png"})

    @patch("production_runtime.requests.get")
    def test_all_selected_images_and_original_derived_hashes_are_preserved(self, get):
        get.return_value = self.response(self.original)
        attachments = [{"file_token": f"t{i}", "name": f"{i}.png"} for i in range(9)]
        data_urls = self.client.attachment_data_urls(attachments)
        self.assertEqual(len(data_urls), 9)
        asset = self.client.reference_asset(attachments[0], role="person_identity", index=1, source_record_id="source")
        self.assertEqual(asset["original_sha256"], bytes_sha256(self.original))
        self.assertNotEqual(asset["derived_sha256"], asset["original_sha256"])
        self.assertEqual(asset["transform"], "compiler-jpeg1024-v1")
        self.client.attachment_data_urls(attachments)
        self.assertEqual(get.call_count, 9)
        self.assertTrue(build_reference_manifest([asset])["manifest_id"])

    def test_explicit_limit_fails_instead_of_silently_dropping_selected_images(self):
        with self.assertRaisesRegex(ValueError, "silent truncation"):
            self.client.attachment_data_urls([{"file_token": f"t{i}"} for i in range(4)], limit=2)
        self.client.request.assert_not_called()

    @patch("production_runtime.requests.get")
    def test_invalid_or_missing_reference_is_not_silently_skipped(self, get):
        get.return_value = self.response(b"not-an-image")
        with self.assertRaisesRegex(ValueError, "decodable image"):
            self.client.attachment_data_urls([{"file_token": "t0"}])
        with self.assertRaisesRegex(ValueError, "resolved"):
            self.client.attachment_data_urls([{"file_token": "missing"}])

    @patch("production_runtime.requests.post")
    @patch("production_runtime.requests.get")
    def test_changed_original_blocks_copy_before_upload(self, get, post):
        get.return_value = self.response(self.original)
        with self.assertRaisesRegex(ValueError, "changed since compilation"):
            self.client.copy_attachment_to_base({"file_token": "t0", "original_sha256": bytes_sha256(b"wrong")}, "base")
        post.assert_not_called()
