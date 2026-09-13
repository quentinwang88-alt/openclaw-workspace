from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from config.loader import ConfigLoadError, load_photo_copy_pack
from services.photo_copy_review import (
    PhotoCopyReviewError, approve_copy_rows, copy_review_sha256,
    validate_native_approval,
)


def row():
    return {
        "recipe_id": "PHOTO_TH_THERMAL_TRANSITION_V1",
        "profile_id": "thermal_outdoor_bts_office_normal_office",
        "copy_id": "native-reviewed",
        "status": "active", "title": "หัวข้อ", "caption": "คำอธิบาย",
        "hashtags": "#หนึ่ง|#สอง", "slide_1": "หนึ่ง", "slide_2": "สอง",
        "slide_3": "สาม", "slide_4": "สี่", "slide_5": "ห้า",
        "language_review_status": "DRAFT", "reviewed_by": "",
        "reviewed_at": "", "review_sha256": "",
    }


class PhotoCopyReviewTest(unittest.TestCase):
    def test_approval_is_bound_to_exact_reviewed_copy(self):
        value = row()
        digest = copy_review_sha256(value)
        approve_copy_rows(
            [value], reviewer="Somchai", reviewed_at="2026-09-12T12:00:00+00:00",
            expected_hashes={"native-reviewed": digest},
        )
        audit = validate_native_approval(value)
        self.assertEqual(audit["reviewed_by"], "Somchai")
        value["caption"] += " เปลี่ยน"
        with self.assertRaisesRegex(PhotoCopyReviewError, "review_sha256"):
            validate_native_approval(value)

    def test_wrong_expected_hash_never_promotes(self):
        value = row()
        with self.assertRaisesRegex(PhotoCopyReviewError, "哈希已变化"):
            approve_copy_rows(
                [value], reviewer="Somchai", reviewed_at="now",
                expected_hashes={"native-reviewed": "0" * 64},
            )
        self.assertEqual(value["language_review_status"], "DRAFT")

    def test_loader_preserves_audit_and_rejects_manual_status_flip(self):
        value = row()
        digest = copy_review_sha256(value)
        approve_copy_rows(
            [value], reviewer="Somchai", reviewed_at="2026-09-12T12:00:00+00:00",
            expected_hashes={"native-reviewed": digest},
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "pack.tsv"
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(value), delimiter="\t")
                writer.writeheader()
                writer.writerow(value)
            variants = load_photo_copy_pack(
                path, expected_recipe_id=value["recipe_id"],
                expected_profile_id=value["profile_id"],
            )
            self.assertEqual(
                variants[0]["copy"]["language_review"]["review_sha256"], digest,
            )
            value["review_sha256"] = "0" * 64
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(value), delimiter="\t")
                writer.writeheader()
                writer.writerow(value)
            with self.assertRaisesRegex(ConfigLoadError, "NATIVE_APPROVED"):
                load_photo_copy_pack(
                    path, expected_recipe_id=value["recipe_id"],
                    expected_profile_id=value["profile_id"],
                )


if __name__ == "__main__":
    unittest.main()
