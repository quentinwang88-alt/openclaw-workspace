#!/usr/bin/env python3

from __future__ import annotations

import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path

from services.operation_product_pack_source import (
    FeishuOperationProductPackSource,
    normalize_product_category,
)


@dataclass
class Record:
    record_id: str
    fields: dict


class FakeClient:
    def __init__(self, records, payloads):
        self.records = records
        self.payloads = payloads
        self.downloads = []

    def list_records(self, page_size=500):
        assert page_size == 500
        return self.records

    def download_attachment_bytes(self, attachment):
        token = attachment["file_token"]
        self.downloads.append(token)
        content = self.payloads[token]
        return (
            content,
            attachment.get("name", f"{token}.jpg"),
            attachment.get("type", "image/jpeg"),
            len(content),
        )


class OperationProductPackSourceTest(unittest.TestCase):
    def test_client_factory_is_lazy_until_candidates_are_requested(self):
        calls = []
        client = FakeClient([], {})
        with tempfile.TemporaryDirectory() as directory:
            source = FeishuOperationProductPackSource(
                client_factory=lambda: calls.append("built") or client,
                cache_root=Path(directory),
            )
            self.assertEqual(calls, [])
            self.assertEqual(source.list_candidates("P1"), [])
            self.assertEqual(calls, ["built"])

    def test_each_matching_record_remains_an_independent_cached_group(self):
        records = [
            Record("rec-a", {
                "产品编码（需填写）": [{"text": "P1"}],
                "产品图片（需填写）": [
                    {"file_token": "a1", "name": "front.jpg"},
                    {"file_token": "a2", "name": "back.jpg"},
                ],
            }),
            Record("rec-b", {
                "产品编码": "P1",
                "产品图片（需填写）": [
                    {"file_token": "b1", "name": "other.jpg"},
                ],
            }),
            Record("rec-other", {
                "产品编码": "P2",
                "产品图片（需填写）": [
                    {"file_token": "x1", "name": "ignored.jpg"},
                ],
            }),
        ]
        client = FakeClient(records, {
            "a1": b"front", "a2": b"back", "b1": b"other", "x1": b"ignored",
        })
        with tempfile.TemporaryDirectory() as directory:
            source = FeishuOperationProductPackSource(
                client, cache_root=Path(directory)
            )
            candidates = source.list_candidates("P1")
            self.assertEqual([row.record_id for row in candidates], ["rec-a", "rec-b"])
            self.assertEqual([len(row.references) for row in candidates], [2, 1])
            self.assertNotEqual(
                Path(candidates[0].references[0]).parent,
                Path(candidates[1].references[0]).parent,
            )
            self.assertEqual(client.downloads, ["a1", "a2", "b1"])

            source.list_candidates("P1")
            self.assertEqual(client.downloads, ["a1", "a2", "b1"])

    def test_non_image_attachment_is_ignored(self):
        client = FakeClient([
            Record("rec-a", {
                "产品编码": "P1",
                "产品图片": [{
                    "file_token": "doc", "name": "notes.pdf", "type": "application/pdf",
                }],
            }),
        ], {"doc": b"not-an-image"})
        with tempfile.TemporaryDirectory() as directory:
            source = FeishuOperationProductPackSource(
                client, cache_root=Path(directory)
            )
            self.assertEqual(source.list_candidates("P1"), [])

    def test_current_required_field_names_are_supported(self):
        client = FakeClient([
            Record("rec-current", {
                "产品编码（需填写）": "P1",
                "产品类型（需填写）": "外套",
                "产品图片（需填写）": [{
                    "file_token": "image", "name": "front.jpg",
                }],
            }),
        ], {"image": b"image"})
        with tempfile.TemporaryDirectory() as directory:
            candidates = FeishuOperationProductPackSource(
                client, cache_root=Path(directory)
            ).list_candidates("P1")
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].record_id, "rec-current")
        self.assertEqual(candidates[0].category, "outerwear")

    def test_product_category_is_normalized_for_look_compatibility(self):
        self.assertEqual(normalize_product_category("外套"), "outerwear")
        self.assertEqual(normalize_product_category("outerwear"), "outerwear")
        self.assertEqual(normalize_product_category("未配置类型"), "")


if __name__ == "__main__":
    unittest.main()
