from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path

from services.photo_content_planner import (
    ADDITIVE_CONTRACT_KEYS, LegacyPlanLocaleUpgrade, PhotoContentPlanError,
    PhotoContentPlanStore, _item_visual as plan_item_visual, adopt_repaired_copy,
    additive_only_contract_change, copy_language_matches_locale, plan_copy_texts,
    plan_th_choice_batch, recipe_has_planning_policy, validate_batch_plan,
)
from services.photo_theme import resolve_photo_theme
from tests.test_photo_reference_vision import recommendation


class PhotoContentPlannerTest(unittest.TestCase):
    def setUp(self):
        self.theme = resolve_photo_theme("秋季穿搭")

    def plan(self, mode="STYLE", count=3):
        return plan_th_choice_batch(
            record_id="rec-plan", recipe_id="PHOTO_TH_PICK_YOUR_LOOK_V3",
            theme=self.theme, reference_mode=mode, count=count,
        )

    def test_three_posts_freeze_different_real_looks_and_cover_copy(self):
        plan = self.plan()
        self.assertEqual(len(plan["items"]), 3)
        self.assertEqual(len({item["family_id"] for item in plan["items"]}), 3)
        self.assertEqual(len({item["copy"]["cover"] for item in plan["items"]}), 3)
        signatures = {
            (look["outerwear"], look["bottom"], look["shoes"])
            for item in plan["items"] for look in item["looks"]
        }
        self.assertEqual(len(signatures), 12)

    def test_product_mode_keeps_product_truth_but_changes_companion_items(self):
        plan = self.plan(mode="PRODUCT")
        outerwear = {
            look["outerwear"] for item in plan["items"] for look in item["looks"]
        }
        bottoms = {
            look["bottom"] for item in plan["items"] for look in item["looks"]
        }
        self.assertEqual(len(outerwear), 1)
        self.assertEqual(len(bottoms), 12)

    def test_warm_flat_lay_reference_selects_compatible_families_and_route(self):
        profile = {
            "schema_version": "opv-photo-style-profile-v1",
            "presentation_type": "FLAT_LAY", "season": "autumn",
            "palette": ["warm_brown", "camel", "cream"],
            "temperature": "warm", "style_tags": ["warm_neutral", "heritage", "layered"],
        }
        plan = plan_th_choice_batch(
            record_id="rec-warm-flat", recipe_id="PHOTO_TH_PICK_YOUR_LOOK_V3",
            theme=self.theme, reference_mode="STYLE", count=3, style_profile=profile,
        )
        self.assertEqual(
            [item["family_id"] for item in plan["items"]],
            ["warm_neutral", "soft_earth", "color_point"],
        )
        self.assertTrue(all(item["presentation_type"] == "FLAT_LAY" for item in plan["items"]))
        self.assertFalse({"monochrome", "sporty_layer"} & {item["family_id"] for item in plan["items"]})

    def test_doubao_contract_drives_dynamic_scene_looks_and_copy(self):
        profile = {
            "analysis_method": "doubao_seed_2_1", "presentation_type": "SCENE_MODEL",
            "palette": ["camel", "cream", "burgundy"], "temperature": "warm",
            "aggregate": {"background": "欧洲街角咖啡店"},
            "recommended_sets": [recommendation(1), recommendation(2)],
        }
        plan = plan_th_choice_batch(
            record_id="rec-dynamic", recipe_id="PHOTO_TH_PICK_YOUR_LOOK_V3",
            theme=self.theme, reference_mode="STYLE", count=2, style_profile=profile,
        )
        self.assertEqual([item["family_id"] for item in plan["items"]],
                         ["vision_dynamic_1", "vision_dynamic_2"])
        self.assertTrue(all(item["presentation_type"] == "SCENE_MODEL" for item in plan["items"]))
        self.assertEqual(plan["items"][0]["looks"][0]["outerwear"], "复古外套1a")
        self.assertEqual(plan["items"][1]["copy"]["title"], "แฟชั่นวินเทจ 2")

    def test_complete_look_mode_only_uses_neutral_visible_claims(self):
        plan = self.plan(mode="COMPLETE_LOOK", count=3)
        self.assertTrue(all(not item["looks"] for item in plan["items"]))
        self.assertEqual(len({item["copy"]["cover"] for item in plan["items"]}), 3)
        visible = " ".join(
            value for item in plan["items"] for value in item["copy"].values()
        )
        self.assertNotIn("สูง", visible)
        self.assertNotIn("°C", visible)

    def test_duplicate_family_is_rejected_before_generation(self):
        plan = self.plan()
        broken = copy.deepcopy(plan)
        broken["items"][1] = copy.deepcopy(broken["items"][0])
        broken["items"][1]["index"] = 2
        with self.assertRaisesRegex(PhotoContentPlanError, "重复"):
            validate_batch_plan(broken)

    def test_local_store_resumes_exact_plan_and_rejects_changed_input(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = PhotoContentPlanStore(Path(tmp))
            first = store.load_or_create(
                record_id="rec", input_contract={"quantity": 3}, create=self.plan,
            )
            second = store.load_or_create(
                record_id="rec", input_contract={"quantity": 3},
                create=lambda: self.fail("should load frozen plan"),
            )
            self.assertEqual(first, second)
            with self.assertRaisesRegex(PhotoContentPlanError, "已变化"):
                store.load_or_create(
                    record_id="rec", input_contract={"quantity": 2}, create=self.plan,
                )


class LegacyPlanLocaleUpgradeTest(unittest.TestCase):
    """新增 ``publish_locale`` 只能做「窄兼容」，不能重规划也不能归档源图。

    2026-09-14：契约里新增 ``input_contract['publish_locale']`` 改变了 hash，
    ``load_or_create`` 直接抛「已变化」，``_is_replannable_photo_error`` 把它判成
    可重规划 ⇒ 归档 content_plan **和该记录全部 style_reference_supply** ⇒ 已付费
    的 4 张 look 被重新生成。这里的四条用例钉住新的判定。
    """

    def setUp(self):
        self.theme = resolve_photo_theme("秋季穿搭")

    def plan(self):
        return plan_th_choice_batch(
            record_id="rec-locale", recipe_id="PHOTO_TH_PICK_YOUR_LOOK_V3",
            theme=self.theme, reference_mode="STYLE", count=1,
        )

    def contract(self, **extra):
        return {"quantity": 1, **extra}

    def seed(self, store, contract=None):
        """冻结一份（泰语文案的）旧计划，契约里没有 publish_locale。"""
        plan = store.load_or_create(
            record_id="rec-locale", input_contract=contract or self.contract(),
            create=self.plan,
        )
        self.assertTrue(any(
            "\u0e00" <= char <= "\u0e7f" for char in json.dumps(plan, ensure_ascii=False)
        ), "前提：这份旧计划的文案是泰语")
        return plan

    def stored_payload(self, tmp):
        return json.loads(
            (Path(tmp) / "content_plans" / "rec-locale" / "plan.json").read_text(encoding="utf-8")
        )

    def test_same_language_plan_is_reused_after_backfilling_the_new_key(self):
        """验收 1：同语言旧计划 + 只新增 publish_locale ⇒ 计划不变、不重新规划。"""
        with tempfile.TemporaryDirectory() as tmp:
            store = PhotoContentPlanStore(Path(tmp))
            frozen = self.seed(store)
            before = self.stored_payload(tmp)
            reused = store.load_or_create(
                record_id="rec-locale",
                input_contract=self.contract(publish_locale="th-TH"),
                create=lambda: self.fail("同语言升级不得重新规划"),
                tolerate_additive_keys=ADDITIVE_CONTRACT_KEYS,
            )
            self.assertEqual(reused, frozen)
            after = self.stored_payload(tmp)
            self.assertEqual(after["plan"], before["plan"], "计划本体必须逐字不变")
            self.assertEqual(after["input_contract"]["publish_locale"], "th-TH")
            self.assertNotEqual(after["input_sha256"], before["input_sha256"])
            # 再跑一次必须直接命中新 hash，不需要第三次补记。
            store.load_or_create(
                record_id="rec-locale",
                input_contract=self.contract(publish_locale="th-TH"),
                create=lambda: self.fail("不得重新规划"),
                tolerate_additive_keys=ADDITIVE_CONTRACT_KEYS,
            )

    def test_old_thai_copy_on_a_vietnamese_row_is_reported_as_copy_language(self):
        """验收 2 前半：旧计划是泰语文案、当前要发布越南语 ⇒ 明确要求重建文案。"""
        with tempfile.TemporaryDirectory() as tmp:
            store = PhotoContentPlanStore(Path(tmp))
            self.seed(store)
            before = self.stored_payload(tmp)
            with self.assertRaises(LegacyPlanLocaleUpgrade) as caught:
                store.load_or_create(
                    record_id="rec-locale",
                    input_contract=self.contract(publish_locale="vi-VN"),
                    create=lambda: self.fail("不得在这里重新规划"),
                    tolerate_additive_keys=ADDITIVE_CONTRACT_KEYS,
                )
            self.assertEqual(caught.exception.reason, "copy_language")
            # 键：旧计划与磁盘状态一字未动（不归档、不生图）。
            self.assertEqual(self.stored_payload(tmp), before)
            # 而且不能被 _is_replannable_photo_error 认成可重规划。
            from services.feishu_workflow import FeishuTaskWorkflow
            self.assertFalse(FeishuTaskWorkflow._is_replannable_photo_error(caught.exception))

    def test_unprovable_language_keeps_the_old_plan_instead_of_guessing(self):
        """验收 1／2 的安全分支：证不出语言一致 ⇒ 保留旧计划与素材，不猜。"""
        with tempfile.TemporaryDirectory() as tmp:
            store = PhotoContentPlanStore(Path(tmp))
            self.seed(store)
            # 把冻结文案与 look 标签都换成纯拉丁字母：泰语行从此证不出「这就是泰语」。
            # （look 标签同样归语言包，所以只改 copy 字段是不够的。）
            path = Path(tmp) / "content_plans" / "rec-locale" / "plan.json"
            payload = json.loads(path.read_text(encoding="utf-8"))
            for item in payload["plan"]["items"]:
                copy_block = item.get("copy") or {}
                for key in ("title", "caption", "cover", "cta"):
                    copy_block[key] = "Look A"
                copy_block["hashtags"] = ["#LookA"]
                copy_block["slide_texts"] = ["Look A"]
                for letter, look in zip("ABCD", item.get("looks") or []):
                    look["display_label"] = f"Look {letter}"
            plan_body = {key: value for key, value in payload["plan"].items()
                         if key != "plan_sha256"}
            from services.photo_content_planner import _fingerprint
            payload["plan"]["plan_sha256"] = _fingerprint(plan_body)
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            before = self.stored_payload(tmp)
            with self.assertRaises(LegacyPlanLocaleUpgrade) as caught:
                store.load_or_create(
                    record_id="rec-locale",
                    input_contract=self.contract(publish_locale="th-TH"),
                    create=lambda: self.fail("不得在这里重新规划"),
                    tolerate_additive_keys=ADDITIVE_CONTRACT_KEYS,
                )
            self.assertEqual(caught.exception.reason, "unverified")
            self.assertEqual(self.stored_payload(tmp), before)

    def test_a_real_input_change_is_never_swallowed_by_the_compatibility(self):
        """验收 3：真实改变（生成篇数）仍走原硬失败，不被窄兼容吞掉。"""
        with tempfile.TemporaryDirectory() as tmp:
            store = PhotoContentPlanStore(Path(tmp))
            self.seed(store)
            with self.assertRaisesRegex(PhotoContentPlanError, "已变化") as caught:
                store.load_or_create(
                    record_id="rec-locale",
                    input_contract={"quantity": 2, "publish_locale": "th-TH"},
                    create=self.plan, tolerate_additive_keys=ADDITIVE_CONTRACT_KEYS,
                )
            self.assertNotIsInstance(caught.exception, LegacyPlanLocaleUpgrade)

    # --- 第二趟：只替换文案，画面字段逐字不动 (2026-09-14) -------------------

    def rebuilt_with_latin_copy(self, **item_overrides):
        """按当前语言重建的计划：非文案字段与 ``self.plan()`` 逐字相同。"""
        rebuilt = self.plan()
        for item in rebuilt["items"]:
            item["copy"] = {
                "title": "Gợi ý phối đồ du lịch", "cover": "Look du lịch",
                "caption": "Bạn thích look nào?", "hashtags": ["#OOTD"],
                "slide_texts": ["Look A"],
            }
            # 真实语言包里 look 标签也归 Locale Pack（``family_copy.look_labels``），
            # 所以重建侧必须连标签一起换。
            for letter, look in zip("ABCD", item.get("looks") or []):
                look["display_label"] = f"Look {letter}"
        for key, value in item_overrides.items():
            rebuilt["items"][0][key] = value
        return rebuilt

    def test_copy_repair_pass_replaces_only_the_publish_copy(self):
        """验收 2：旧泰文文案就地换成当前语言，画面字段与旧计划逐字相同。"""
        with tempfile.TemporaryDirectory() as tmp:
            store = PhotoContentPlanStore(Path(tmp))
            frozen = self.seed(store)
            before = self.stored_payload(tmp)
            repaired = store.load_or_create(
                record_id="rec-locale",
                input_contract=self.contract(publish_locale="vi-VN"),
                create=lambda: self.fail("修复趟不得走 create"),
                tolerate_additive_keys=ADDITIVE_CONTRACT_KEYS,
                repair_copy=self.rebuilt_with_latin_copy,
            )
            # 画面侧逐字保留：除文案与 look 标签外，条目与顶层都与旧计划相同。
            for stored_item, repaired_item in zip(frozen["items"], repaired["items"]):
                self.assertEqual(
                    plan_item_visual(stored_item), plan_item_visual(repaired_item))
            self.assertEqual(
                {k: v for k, v in frozen.items() if k not in ("items", "plan_sha256")},
                {k: v for k, v in repaired.items() if k not in ("items", "plan_sha256")},
            )
            # 文案与 look 标签都换成了当前语言的。
            self.assertEqual(repaired["items"][0]["copy"]["title"], "Gợi ý phối đồ du lịch")
            self.assertEqual(
                [look["display_label"] for look in repaired["items"][0]["looks"]],
                ["Look A", "Look B", "Look C", "Look D"],
            )
            self.assertIs(
                copy_language_matches_locale(plan_copy_texts(repaired), locale="vi-VN"), True)
            self.assertFalse(any(
                "\u0e00" <= char <= "\u0e7f"
                for char in json.dumps(repaired, ensure_ascii=False)
            ), "修复后不该还有泰文字符")
            # plan_sha256 自洽（load_or_create 会校验，这里再钉一次）。
            after = self.stored_payload(tmp)
            self.assertEqual(after["plan"], repaired)
            self.assertEqual(after["input_contract"]["publish_locale"], "vi-VN")
            self.assertNotEqual(after["input_sha256"], before["input_sha256"])
            # 再跑一次直接命中新 hash，不再需要修复。
            store.load_or_create(
                record_id="rec-locale",
                input_contract=self.contract(publish_locale="vi-VN"),
                create=lambda: self.fail("不得重新规划"),
                tolerate_additive_keys=ADDITIVE_CONTRACT_KEYS,
                repair_copy=lambda: self.fail("不得再次修复"),
            )

    def test_copy_repair_refuses_when_the_rebuild_moves_a_visual_field(self):
        """验收 3：重建会动画面字段（穿搭）⇒ 拒绝，磁盘一字不动。"""
        with tempfile.TemporaryDirectory() as tmp:
            store = PhotoContentPlanStore(Path(tmp))
            self.seed(store)
            before = self.stored_payload(tmp)
            with self.assertRaises(LegacyPlanLocaleUpgrade) as caught:
                store.load_or_create(
                    record_id="rec-locale",
                    input_contract=self.contract(publish_locale="vi-VN"),
                    create=lambda: self.fail("修复趟不得走 create"),
                    tolerate_additive_keys=ADDITIVE_CONTRACT_KEYS,
                    repair_copy=lambda: self.rebuilt_with_latin_copy(
                        looks=[{"role": "look_a", "outerwear": "换一件完全不同的外套"}]),
                )
            self.assertEqual(caught.exception.reason, "visual_changed")
            self.assertEqual(self.stored_payload(tmp), before, "拒绝路径不得写盘")
            from services.feishu_workflow import FeishuTaskWorkflow
            self.assertFalse(FeishuTaskWorkflow._is_replannable_photo_error(caught.exception))

    def test_copy_repair_refuses_a_rebuild_with_a_different_item_count(self):
        """篇数不同 ⇒ 同样拒绝（不能靠「只换文案」跨越篇数变化）。"""
        with tempfile.TemporaryDirectory() as tmp:
            store = PhotoContentPlanStore(Path(tmp))
            self.seed(store)
            before = self.stored_payload(tmp)
            with self.assertRaises(LegacyPlanLocaleUpgrade) as caught:
                store.load_or_create(
                    record_id="rec-locale",
                    input_contract=self.contract(publish_locale="vi-VN"),
                    create=lambda: self.fail("修复趟不得走 create"),
                    tolerate_additive_keys=ADDITIVE_CONTRACT_KEYS,
                    repair_copy=lambda: {**self.rebuilt_with_latin_copy(),
                                         "items": self.rebuilt_with_latin_copy()["items"] * 2},
                )
            self.assertEqual(caught.exception.reason, "visual_changed")
            self.assertEqual(self.stored_payload(tmp), before)

    def test_copy_repair_refuses_an_empty_copy_from_the_rebuild(self):
        """重建没给出文案 ⇒ ``unverified``，不猜也不写盘。"""
        with tempfile.TemporaryDirectory() as tmp:
            store = PhotoContentPlanStore(Path(tmp))
            self.seed(store)
            before = self.stored_payload(tmp)

            def blank_copy():
                rebuilt = self.plan()
                rebuilt["items"][0]["copy"] = {}
                return rebuilt

            with self.assertRaises(LegacyPlanLocaleUpgrade) as caught:
                store.load_or_create(
                    record_id="rec-locale",
                    input_contract=self.contract(publish_locale="vi-VN"),
                    create=lambda: self.fail("修复趟不得走 create"),
                    tolerate_additive_keys=ADDITIVE_CONTRACT_KEYS,
                    repair_copy=blank_copy,
                )
            self.assertEqual(caught.exception.reason, "unverified")
            self.assertEqual(self.stored_payload(tmp), before)

    def test_adopt_repaired_copy_swaps_the_locale_owned_text_only(self):
        """采纳文案：换文案与 look 标签，画面字段与顶层一律保留旧计划。"""
        stored = self.plan()
        stored_item = stored["items"][0]
        stored_item["copy_source"] = "template_fill"
        stored_item["template_review_status"] = "DRAFT"
        rebuilt = self.rebuilt_with_latin_copy()
        rebuilt["items"][0]["copy_source"] = "travel_topic_model"
        adopted = adopt_repaired_copy(stored, rebuilt)
        item = adopted["items"][0]
        self.assertEqual(item["copy"], rebuilt["items"][0]["copy"])
        self.assertEqual(item["copy_source"], "travel_topic_model")
        # 重建侧没有的文案标记键要从条目里去掉（否则会留下旧语言的来源标记）。
        self.assertNotIn("template_review_status", item)
        # look 的画面字段原样、标签换成新语言的。
        self.assertEqual(plan_item_visual(stored_item), plan_item_visual(item))
        self.assertEqual(
            [look["display_label"] for look in item["looks"]],
            ["Look A", "Look B", "Look C", "Look D"],
        )
        # 顶层仍是旧计划那套。
        self.assertEqual(
            {k: v for k, v in stored.items() if k not in ("items", "plan_sha256")},
            {k: v for k, v in adopted.items() if k not in ("items", "plan_sha256")},
        )
        self.assertNotEqual(adopted["plan_sha256"], stored["plan_sha256"])

    def test_adopt_repaired_copy_refuses_a_moved_top_level_field(self):
        """顶层画面字段（如旅行地点）变了 ⇒ 拒绝，不能只换文案。"""
        stored = self.plan()
        rebuilt = self.rebuilt_with_latin_copy()
        rebuilt["travel_place"] = "别处"
        with self.assertRaises(LegacyPlanLocaleUpgrade) as caught:
            adopt_repaired_copy(stored, rebuilt)
        self.assertEqual(caught.exception.reason, "visual_changed")


    def test_read_frozen_returns_the_old_items_for_evidence(self):
        """``read_frozen`` 只读：给修复路径提供「旧计划长什么样」。"""
        with tempfile.TemporaryDirectory() as tmp:
            store = PhotoContentPlanStore(Path(tmp))
            self.assertEqual(store.read_frozen("rec-locale"), {})
            frozen = self.seed(store)
            payload = store.read_frozen("rec-locale")
            self.assertEqual(payload["plan"], frozen)
            self.assertIn("input_contract", payload)


class LegacyLocaleHelperTest(unittest.TestCase):
    def test_script_check_only_settles_the_thai_axis(self):
        self.assertIs(copy_language_matches_locale(["ลุค A"], locale="th-TH"), True)
        self.assertIs(copy_language_matches_locale(["ลุค A"], locale="vi-VN"), False)
        self.assertIs(copy_language_matches_locale(["Look A"], locale="vi-VN"), True)
        # 拉丁文案对泰语行证不出「这就是泰语」；空文案同样证不出来。
        self.assertIsNone(copy_language_matches_locale(["Look A"], locale="th-TH"))
        self.assertIsNone(copy_language_matches_locale(["  "], locale="vi-VN"))
        self.assertIsNone(copy_language_matches_locale([], locale="vi-VN"))

    def test_only_a_sole_additive_key_is_tolerated(self):
        self.assertEqual(
            additive_only_contract_change(
                {"a": 1}, {"a": 1, "publish_locale": "vi-VN"}, ADDITIVE_CONTRACT_KEYS),
            "publish_locale",
        )
        # 其余任何差异都不算「只是新增了键」。
        self.assertEqual(
            additive_only_contract_change(
                {"a": 1}, {"a": 2, "publish_locale": "vi-VN"}, ADDITIVE_CONTRACT_KEYS), "")
        self.assertEqual(
            additive_only_contract_change(
                {"a": 1, "publish_locale": "th-TH"}, {"a": 1, "publish_locale": "vi-VN"},
                ADDITIVE_CONTRACT_KEYS), "")

    def test_plan_copy_texts_reads_every_published_string(self):
        plan = {"items": [{"copy": {"title": "T", "caption": "C", "cover": "V", "cta": "X",
                                    "hashtags": ["#a"], "slide_texts": ["s1"]},
                           "looks": [{"role": "look_a", "display_label": "LBL"}],
                           "topic_zh": "zh"}]}
        self.assertEqual(set(plan_copy_texts(plan)),
                         {"T", "C", "V", "X", "#a", "s1", "LBL", "zh"},
                         "look 的上片标签同样由语言包拥有，必须计入语言自证")

class TravelOutfitPlannerTest(unittest.TestCase):
    def setUp(self):
        self.theme = resolve_photo_theme("凉爽旅行")

    def test_registry_routes_both_recipes_and_rejects_unregistered(self):
        self.assertTrue(recipe_has_planning_policy("PHOTO_TH_PICK_YOUR_LOOK_V3"))
        self.assertTrue(recipe_has_planning_policy("PHOTO_TH_TRAVEL_OUTFIT_V2"))
        self.assertFalse(recipe_has_planning_policy("PHOTO_TH_PETITE_STYLING_V1"))
        with self.assertRaisesRegex(PhotoContentPlanError, "尚未接入"):
            plan_th_choice_batch(
                record_id="rec-x", recipe_id="PHOTO_TH_PETITE_STYLING_V1",
                theme=self.theme, reference_mode="STYLE", count=1,
            )

    def test_travel_plan_freezes_travel_families_with_travel_copy(self):
        plan = plan_th_choice_batch(
            record_id="rec-travel", recipe_id="PHOTO_TH_TRAVEL_OUTFIT_V2",
            theme=self.theme, reference_mode="STYLE", count=3,
        )
        self.assertEqual(plan["policy_id"], "TH_TRAVEL_OUTFIT_V1")
        self.assertEqual(len(plan["items"]), 3)
        self.assertEqual(len({item["family_id"] for item in plan["items"]}), 3)
        signatures = {
            (look["outerwear"], look["bottom"], look["shoes"])
            for item in plan["items"] for look in item["looks"]
        }
        self.assertEqual(len(signatures), 12)
        for item in plan["items"]:
            self.assertEqual([look["role"] for look in item["looks"]],
                             ["look_a", "look_b", "look_c", "look_d"])
            self.assertIn("A B C หรือ D", item["copy"]["cover"])
            self.assertTrue(item["scene_zh"] and item["angle_zh"])
        travel_families = {
            "airport_transit", "city_walk", "cafe_hopping", "night_market",
            "old_town_photo", "seaside_stroll", "mountain_town", "shopping_mall",
        }
        self.assertTrue(
            {item["family_id"] for item in plan["items"]} <= travel_families
        )

    def test_travel_recipe_rejects_product_mode_until_outfit_supply_exists(self):
        with self.assertRaisesRegex(PhotoContentPlanError, "不支持参考模式：PRODUCT"):
            plan_th_choice_batch(
                record_id="rec-travel", recipe_id="PHOTO_TH_TRAVEL_OUTFIT_V2",
                theme=self.theme, reference_mode="PRODUCT", count=1,
            )

    def test_travel_doubao_contract_reuses_shared_vision_schema(self):
        profile = {
            "analysis_method": "doubao_seed_2_1", "presentation_type": "SCENE_MODEL",
            "palette": ["camel", "navy"], "temperature": "cool",
            "aggregate": {"background": "老城石板街"},
            "recommended_sets": [recommendation(1), recommendation(2)],
        }
        plan = plan_th_choice_batch(
            record_id="rec-travel-dynamic", recipe_id="PHOTO_TH_TRAVEL_OUTFIT_V2",
            theme=self.theme, reference_mode="STYLE", count=2, style_profile=profile,
        )
        self.assertEqual([item["family_id"] for item in plan["items"]],
                         ["vision_dynamic_1", "vision_dynamic_2"])
        self.assertEqual(plan["items"][0]["copy"]["title"], "แฟชั่นวินเทจ 1")


if __name__ == "__main__":
    unittest.main()
