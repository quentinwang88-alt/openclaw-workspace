#!/usr/bin/env python3
"""Task Intake tests with an in-memory fake repository (no real RDS)."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest
from datetime import datetime, timezone

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from domain.models import AccountProfile, MarketPack, RenderPreset, ThemeCatalog
from services.task_intake import (
    IntakeResult,
    TaskIntakeError,
    TaskIntakeService,
    TaskRequest,
    derive_idempotency_key,
)


class FakeRepository:
    def __init__(self) -> None:
        self.accounts = {}
        self.packs = {}
        self.presets = {}
        self.themes = {}
        self.tasks_by_key = {}
        self.inserted = []

    def get_account_profile(self, account_id):
        return self.accounts.get(account_id)

    def get_market_pack(self, pack_id):
        return self.packs.get(pack_id)

    def get_render_preset(self, preset_id):
        return self.presets.get(preset_id)

    def get_theme(self, theme_id):
        return self.themes.get(theme_id)

    def create_task_idempotent(self, task):
        if task.idempotency_key in self.tasks_by_key:
            return self.tasks_by_key[task.idempotency_key], False
        self.tasks_by_key[task.idempotency_key] = task
        self.inserted.append(task)
        return task, True

    def get_task(self, task_id):
        for task in self.tasks_by_key.values():
            if task.task_id == task_id:
                return task
        return None

    def update_task_product_snapshot(self, task_id, product_snapshot_json):
        task = self.get_task(task_id)
        if task and task.task_status == "draft" and task.current_stage == "intake":
            task.product_snapshot_json = product_snapshot_json


def market_pack() -> MarketPack:
    return MarketPack(
        market_pack_id="MP_TH_DEFAULT_V1",
        pack_key="MP_TH_DEFAULT",
        target_country="TH",
        target_locale="th-TH",
        pack_name="泰国默认",
        status="active",
    )


def render_preset() -> RenderPreset:
    return RenderPreset(
        render_preset_id="RP_STILL_VERTICAL_12S_V1",
        preset_key="RP_STILL_VERTICAL_12S",
        preset_name="12.5s",
        status="active",
    )


def account(status="testing") -> AccountProfile:
    return AccountProfile(
        account_id="OPV_TH_TEST_1",
        account_code="opv-th-test-1",
        account_name="test",
        target_country="TH",
        default_locale="th-TH",
        timezone="Asia/Bangkok",
        status=status,
        persona_ref_id="PERSONA_1",
        allowed_look_refs_json=["LOOK_1", "LOOK_2"],
        core_scene_refs_json=["SCENE_1"],
        default_market_pack_id="MP_TH_DEFAULT_V1",
        default_render_preset_id="RP_STILL_VERTICAL_12S_V1",
    )


def request(**overrides) -> TaskRequest:
    base = dict(
        account_id="OPV_TH_TEST_1",
        product_id="PROD_88",
        product_snapshot={"reference_images": ["oss://ref/1.jpg", "oss://ref/2.jpg"]},
        theme_id=None,
        topic_text=None,
        business_moment=datetime(2026, 8, 30, 4, 0, 0, tzinfo=timezone.utc),
    )
    base.update(overrides)
    return TaskRequest(**base)


class TaskIntakeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.repo = FakeRepository()
        self.repo.packs["MP_TH_DEFAULT_V1"] = market_pack()
        self.repo.presets["RP_STILL_VERTICAL_12S_V1"] = render_preset()
        self.repo.accounts["OPV_TH_TEST_1"] = account()
        self.service = TaskIntakeService(self.repo)

    def test_happy_path_creates_draft_task_with_snapshot_context(self) -> None:
        result = self.service.create_task(request(feishu_record_id="rec_1"))
        self.assertTrue(result.created)
        self.assertIsInstance(result, IntakeResult)
        task = result.task
        self.assertEqual(task.task_status, "draft")
        self.assertEqual(task.current_stage, "intake")
        self.assertEqual(task.target_country, "TH")
        self.assertEqual(task.market_pack_id, "MP_TH_DEFAULT_V1")
        self.assertEqual(task.feishu_record_id, "rec_1")
        context = task.product_snapshot_json["intake_context"]
        self.assertEqual(context["market_pack_version"], 1)
        self.assertEqual(context["persona_ref_id"], "PERSONA_1")
        self.assertEqual(context["render_preset_id"], "RP_STILL_VERTICAL_12S_V1")
        self.assertEqual(
            task.product_snapshot_json["product"]["reference_images"],
            ["oss://ref/1.jpg", "oss://ref/2.jpg"],
        )

    def test_same_input_same_day_is_idempotent(self) -> None:
        first = self.service.create_task(request())
        second = self.service.create_task(request())
        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertEqual(first.task.task_id, second.task.task_id)
        self.assertEqual(len(self.repo.inserted), 1)

    def test_intake_draft_refreshes_stale_product_snapshot_on_retry(self) -> None:
        first = self.service.create_task(request(
            product_snapshot={"reference_images": ["/old.jpg"], "category": "old"}
        ))
        second = self.service.create_task(request(
            product_snapshot={"reference_images": ["/new.jpg"], "category": "outerwear"}
        ))
        self.assertFalse(second.created)
        self.assertEqual(
            second.task.product_snapshot_json["product"]["reference_images"],
            ["/new.jpg"],
        )
        self.assertEqual(first.task.task_id, second.task.task_id)

    def test_same_product_next_day_creates_fresh_content_task(self) -> None:
        first = self.service.create_task(request())
        later = request(
            business_moment=datetime(2026, 8, 31, 4, 0, 0, tzinfo=timezone.utc)
        )
        second = self.service.create_task(later)
        self.assertTrue(first.created)
        self.assertTrue(second.created)
        self.assertNotEqual(first.task.idempotency_key, second.task.idempotency_key)

    def test_explicit_key_is_hashed_to_64_chars(self) -> None:
        result = self.service.create_task(request(idempotency_key="run-42"))
        self.assertEqual(len(result.task.idempotency_key), 64)
        self.assertTrue(all(c in "0123456789abcdef" for c in result.task.idempotency_key))

    def test_optional_theme_must_exist(self) -> None:
        with self.assertRaises(TaskIntakeError):
            self.service.create_task(request(theme_id="THEME_MISSING_V1"))
        self.repo.themes["THEME_TH_CAFE_DATE_V1"] = ThemeCatalog(
            theme_id="THEME_TH_CAFE_DATE_V1",
            theme_key="THEME_TH_CAFE_DATE",
            theme_name="cafe",
        )
        result = self.service.create_task(request(theme_id="THEME_TH_CAFE_DATE_V1"))
        self.assertEqual(result.task.theme_id, "THEME_TH_CAFE_DATE_V1")

    def test_missing_reference_images_rejected(self) -> None:
        with self.assertRaises(TaskIntakeError) as ctx:
            self.service.create_task(request(product_snapshot={"reference_images": []}))
        self.assertIn("reference_images", str(ctx.exception))

    def test_unknown_account_rejected(self) -> None:
        with self.assertRaises(TaskIntakeError):
            self.service.create_task(request(account_id="OPV_GHOST"))

    def test_disabled_account_rejected(self) -> None:
        self.repo.accounts["OPV_TH_TEST_1"] = account(status="disabled")
        with self.assertRaises(TaskIntakeError):
            self.service.create_task(request())

    def test_missing_market_pack_binding_rejected(self) -> None:
        self.repo.accounts["OPV_TH_TEST_1"] = account()
        self.repo.accounts["OPV_TH_TEST_1"].default_market_pack_id = None
        with self.assertRaises(TaskIntakeError) as ctx:
            self.service.create_task(request())
        self.assertIn("market pack", str(ctx.exception))

    def test_inactive_market_pack_rejected(self) -> None:
        self.repo.packs["MP_TH_DEFAULT_V1"].status = "draft"
        with self.assertRaises(TaskIntakeError):
            self.service.create_task(request())

    def test_shot_count_bounds_and_priority_validated(self) -> None:
        with self.assertRaises(TaskIntakeError):
            self.service.create_task(request(requested_shot_count=0))
        with self.assertRaises(TaskIntakeError):
            self.service.create_task(request(requested_shot_count=50))
        with self.assertRaises(TaskIntakeError):
            self.service.create_task(request(priority="urgent"))


class DeriveKeyTest(unittest.TestCase):
    def test_derived_key_is_sha256_hex_and_stable(self) -> None:
        moment = datetime(2026, 8, 30, 4, 0, 0, tzinfo=timezone.utc)
        key_a = derive_idempotency_key(request(), moment=moment)
        key_b = derive_idempotency_key(request(), moment=moment)
        self.assertEqual(key_a, key_b)
        self.assertEqual(len(key_a), 64)

    def test_topic_change_changes_key(self) -> None:
        moment = datetime(2026, 8, 30, 4, 0, 0, tzinfo=timezone.utc)
        key_a = derive_idempotency_key(request(), moment=moment)
        key_b = derive_idempotency_key(request(topic_text="ออกงาน"), moment=moment)
        self.assertNotEqual(key_a, key_b)


if __name__ == "__main__":
    unittest.main()
