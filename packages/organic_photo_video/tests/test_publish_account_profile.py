"""账号定位贯穿（2026-09-15）：目标账号解析、默认主题、表达模式与投递冻结。

覆盖四段链路：
1. ``publish_account_profile``：账号表/种子配置 → 可冻结 profile + 指纹；
2. ``photo_theme``：原生图文「一衣多穿」注册 + 实用指南表达不被短标签裁剪；
3. ``photo_reference_vision``：账号视觉基准/表达进入旅行提示词与 color_grading_plan；
4. ``release_gate``/``models``：目标账号进入冻结清单与任务行，提交前一致性。
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from domain.models import ContentTask
from services.photo_reference_vision import PhotoReferenceVisionService
from services.photo_theme import (
    THEME_OPTIONS, _display_slide_texts, build_theme_copy, resolve_photo_theme,
)
from services.publish_account_profile import (
    CLAIM_SCOPE_STORE_POOL, PublishAccountProfileError,
    PublishAccountProfileResolver, normalize_expression_mode,
    normalize_photo_claim_scope, normalize_profile_payload, profile_fingerprint,
)
from services.release_gate import _assert_target_account_consistency

WORKSPACE_ROOT = Path(__file__).resolve().parents[2]


def seed_file(tmp: Path, handle: str, profile: dict, **extra) -> Path:
    path = tmp / f"{handle}.json"
    payload = {
        "schema_version": "opv-publish-account-seed-v1",
        "account_id": handle, "account_name": handle,
        "store_id": "THFZ01", "target_country": "TH",
        "profile": profile,
    }
    payload.update(extra)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


class FakePublisherDB:
    def __init__(self, rows):
        self.rows = rows

    def get_account_config(self, account_id):
        return self.rows.get(account_id)

    def list_account_configs(self):
        return list(self.rows.values())


class PublishAccountProfileTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_file_seed_resolution_and_fingerprint(self):
        seed_file(self.root, "acc_a", {
            "default_theme": "凉爽旅行", "positioning": "旅行高级感",
            "expression_mode": "STYLE_INSPIRATION",
            "visual_baseline": "中性奶白底、自然肤色",
            "photo_claim_scope": "own_tasks_only",
        })
        resolver = PublishAccountProfileResolver(config_dir=self.root)
        binding = resolver.resolve("acc_a")
        self.assertEqual(binding.store_id, "THFZ01")
        self.assertEqual(binding.target_country, "TH")
        self.assertEqual(binding.profile["default_theme"], "凉爽旅行")
        self.assertEqual(binding.claim_scope, "own_tasks_only")
        self.assertEqual(binding.source, "file")
        # 指纹稳定：同名配置两次解析一致；改视觉基准则变化。
        again = PublishAccountProfileResolver(config_dir=self.root).resolve("acc_a")
        self.assertEqual(binding.fingerprint, again.fingerprint)
        seed_file(self.root, "acc_a", {
            "default_theme": "凉爽旅行", "positioning": "旅行高级感",
            "expression_mode": "STYLE_INSPIRATION",
            "visual_baseline": "改过的视觉基准",
            "photo_claim_scope": "own_tasks_only",
        })
        changed = PublishAccountProfileResolver(config_dir=self.root).resolve("acc_a")
        self.assertNotEqual(binding.fingerprint, changed.fingerprint)

    def test_publisher_db_wins_and_file_fills_gaps(self):
        seed_file(self.root, "acc_a", {"positioning": "种子定位"})
        row = {
            "account_id": "acc_a", "account_name": "A", "store_id": "THFZ01",
            "photo_content_profile_json": json.dumps({
                "expression_mode": "实用指南",
                "photo_claim_scope": "仅本账号任务",
            }, ensure_ascii=False),
        }
        binding = PublishAccountProfileResolver(
            publisher_db=FakePublisherDB({"acc_a": row}), config_dir=self.root,
        ).resolve("acc_a")
        self.assertEqual(binding.source, "publisher_db")
        # 账号表值优先，种子只补缺省键。
        self.assertEqual(binding.profile["expression_mode"], "PRACTICAL_GUIDE")
        self.assertEqual(binding.profile["positioning"], "种子定位")
        self.assertEqual(binding.claim_scope, "own_tasks_only")

    def test_unknown_account_fails_loudly(self):
        resolver = PublishAccountProfileResolver(
            publisher_db=FakePublisherDB({}), config_dir=self.root)
        with self.assertRaises(PublishAccountProfileError) as ctx:
            resolver.resolve("ghost")
        self.assertIn("无法解析", str(ctx.exception))  # 不静默回退公共池

    def test_normalize_helpers(self):
        self.assertEqual(normalize_expression_mode("实用指南"), "PRACTICAL_GUIDE")
        self.assertEqual(normalize_expression_mode("STYLE_INSPIRATION"), "STYLE_INSPIRATION")
        self.assertEqual(normalize_expression_mode(""), "")
        with self.assertRaises(PublishAccountProfileError):
            normalize_expression_mode("随便")
        self.assertEqual(normalize_photo_claim_scope(""), CLAIM_SCOPE_STORE_POOL)
        self.assertEqual(normalize_photo_claim_scope("仅本账号任务"), "own_tasks_only")
        profile = normalize_profile_payload({"style_image": {"file_token": "tok", "name": "a.png"}})
        self.assertEqual(profile["style_image"], {"file_token": "tok", "name": "a.png"})
        self.assertEqual(profile_fingerprint(profile)[:4], profile_fingerprint(profile)[:4])


class PhotoThemeExpressionTest(unittest.TestCase):
    def test_native_multiway_theme_registered_and_routed(self):
        self.assertIn("一衣多穿", THEME_OPTIONS)
        theme = resolve_photo_theme("一衣多穿")
        self.assertEqual(theme["theme_key"], "COOL_WEATHER_TRAVEL")
        self.assertTrue(theme["native_multiway"])
        self.assertFalse(theme.get("travel_theme_type"))  # 不进入旅行六型
        # 别名
        self.assertTrue(resolve_photo_theme("一件多穿")["native_multiway"])

    def test_default_mode_still_strips_reason_lines(self):
        slides = ["封面行1\n封面行2", "A · 造型名 — 一句理由", "B · 造型名 — 理由",
                  "C · 造型名", "D · 造型名 เลือกลุคไหน"]
        result = _display_slide_texts(slides, cta="A B C หรือ D?")
        self.assertEqual(result[1], "A · 造型名")
        self.assertEqual(result[2], "B · 造型名")
        # CTA 唯一来源：末页自带的 CTA 文本优先于传入的 planned/theme CTA。
        self.assertEqual(result[4], "D · 造型名\nเลือกลุคไหน")

    def test_guide_mode_keeps_reason_lines(self):
        slides = ["封面行1\n封面行2", "A · 造型名 — 步行舒服的层次",
                  "B · 造型名 — 进寺庙可披", "C · 造型名 — 防风",
                  "D · 造型名 เซฟไว้ไว้ใช้"]
        result = _display_slide_texts(
            slides, cta="เซฟไว้เลย", expression_mode="PRACTICAL_GUIDE")
        self.assertEqual(result[1], "A · 造型名 — 步行舒服的层次")
        self.assertEqual(result[2], "B · 造型名 — 进寺庙可披")
        self.assertTrue(result[4].startswith("D · 造型名"))
        # 模型自带的收藏 CTA 原样保留（唯一来源），不被 planned CTA 覆盖。
        self.assertIn("เซฟไว้", result[4].splitlines()[-1])

    def test_build_theme_copy_passes_expression_mode(self):
        theme = resolve_photo_theme("凉爽旅行")
        assets = [
            {"role": f"look_{letter}", "display_label": {"th-TH": f"ชุด {letter.upper()}"}}
            for letter in "abcd"
        ]
        variation = {"copy": {
            "place_localized": "เชียงใหม่",
            "title": "เชียงใหม่ ใส่อะไรดี",
            "caption": "4 ลุคสำหรับเดินเมืองเก่า",
            "hashtags": ["#เชียงใหม่"],
            "slide_texts": ["เชียงใหม่\nเดินเมืองเก่า",
                            "A · ลุคอบอุ่น — กันลมหน้าผาก",
                            "B · ลุคบางเบา — ถอดง่ายเข้าวัด",
                            "C · ลุคยืดหยุ่น — เดินทั้งวัน",
                            "D · ลุคคลาสสิก เลือกลุคไหนดี"],
        }}
        guide = build_theme_copy(
            theme, assets, variation, expression_mode="PRACTICAL_GUIDE")
        self.assertIn("—", guide["slide_texts"][1])
        self.assertIn("กันลมหน้าผาก", guide["slide_texts"][1])
        default = build_theme_copy(theme, assets, variation)
        self.assertNotIn("กันลมหน้าผาก", default["slide_texts"][1])


class TravelAccountBaselineTest(unittest.TestCase):
    CONTRACT = {"moments": [
        {"key": m, "label_zh": m, "evidence_zh": "e", "label_th": "t",
         "forbidden_footwear_types": []}
        for m in ("old_town_walk", "cafe_visit", "evening_stroll", "shopping_day")
    ]}

    def test_prompt_without_account_config_is_byte_unchanged(self):
        prompt = PhotoReferenceVisionService._travel_plan_prompt(
            analysis={}, travel_contract=self.CONTRACT, variables={},
            content_requirement="", count=1)
        self.assertNotIn("【账号长期定位", prompt)
        self.assertNotIn("color_grading_plan", prompt)

    def test_prompt_with_account_blocks(self):
        prompt = PhotoReferenceVisionService._travel_plan_prompt(
            analysis={}, travel_contract=self.CONTRACT, variables={},
            content_requirement="", count=1,
            account_positioning="旅行高级感；融入景点",
            expression_mode="PRACTICAL_GUIDE",
            account_visual_baseline="中性奶白底、自然肤色")
        self.assertIn("【账号长期定位", prompt)
        self.assertIn("旅行高级感；融入景点", prompt)
        self.assertIn("中性奶白底、自然肤色", prompt)
        self.assertIn("实用指南：整篇回答一个具体穿搭问题", prompt)
        self.assertIn("color_grading_plan", prompt)
        self.assertIn("商品真实颜色与自然肤色优先", prompt)

    def test_guide_copy_rules_only_render_for_topic_branch(self):
        base = dict(analysis={}, travel_contract=self.CONTRACT, variables={},
                    content_requirement="", count=1,
                    expression_mode="PRACTICAL_GUIDE")
        without_topic = PhotoReferenceVisionService._travel_plan_prompt(**base)
        self.assertNotIn("一句具体穿搭理由", without_topic)
        with_topic = PhotoReferenceVisionService._travel_plan_prompt(
            **{**base, "travel_topic": {
                "theme_type": "CHECK_IN", "place": "清迈",
                "planning_focus": "打卡"}})
        # 2026-09-15 收敛：表达规则并入主题联动文案合同（单一规则源），
        # 不再有后置覆盖块。
        self.assertIn("一句具体穿搭理由", with_topic)
        self.assertIn("不强制 A/B/C/D 投票", with_topic)
        self.assertNotIn("【实用指南文案要求", with_topic)

    def test_travel_plan_normalizes_color_grading_plan(self):
        payload = {
            "travel_variables": {},
            "posts": [{
                "content_angle_zh": "角度", "looks": [
                    {"role": f"look_{letter}", "travel_moment": moment,
                     "scene_prompt": f"场景{letter}", "weather_logic": "w",
                     "outerwear": f"外套{letter}", "top_inner": f"内搭{letter}",
                     "bottom": f"下装{letter}", "shoes": f"鞋{letter}",
                     "footwear_type": "SNEAKER"}
                    for letter, moment in zip(
                        "abcd", ("old_town_walk", "cafe_visit", "evening_stroll", "shopping_day"))
                ],
            }],
            "color_grading_plan": {
                "temperature": "neutral", "saturation": "low",
                "contrast": "gentle", "skin_tone_anchor": "自然肤色",
                "tone_note_zh": "奶白底"},
        }
        plan, errors = PhotoReferenceVisionService(
            root=Path("/tmp"))._normalize_travel_plan(
            payload, self.CONTRACT, 1)
        self.assertEqual(errors, [])
        self.assertEqual(plan["color_grading_plan"]["saturation"], "low")
        self.assertEqual(plan["color_grading_plan"]["tone_note_zh"], "奶白底")

    def test_travel_style_profile_carries_color_grading_plan(self):
        plan = {"travel_variables": {}, "travel_contract": {},
                "color_grading_plan": {"temperature": "neutral"},
                "posts": []}
        profile = PhotoReferenceVisionService.build_travel_style_profile(
            {}, plan, count=1)
        self.assertEqual(profile["color_grading_plan"], {"temperature": "neutral"})
        legacy = PhotoReferenceVisionService.build_travel_style_profile(
            {}, {"travel_variables": {}, "travel_contract": {}, "posts": []}, count=1)
        self.assertNotIn("color_grading_plan", legacy)

    def test_reference_analysis_prompt_baseline_block(self):
        prompt = PhotoReferenceVisionService._reference_analysis_prompt(
            theme={"label_zh": "凉爽旅行"}, category_key="apparel",
            content_requirement="",
            account_visual_baseline="日系自然光、适度留白")
        self.assertIn("账号长期视觉基准", prompt)
        self.assertIn("日系自然光、适度留白", prompt)
        legacy = PhotoReferenceVisionService._reference_analysis_prompt(
            theme={"label_zh": "凉爽旅行"}, category_key="apparel",
            content_requirement="")
        self.assertNotIn("账号长期视觉基准", legacy)


class TargetAccountFreezeTest(unittest.TestCase):
    def test_content_task_roundtrip_target_account(self):
        task = ContentTask(
            task_id="opv_task_1", idempotency_key="k", account_id="OPV_TH_TEST_001",
            product_id=None, target_country="TH", target_locale="th-TH",
            target_publish_account_id="tocrystal66",
        )
        row = task.to_row()
        self.assertEqual(row["target_publish_account_id"], "tocrystal66")
        loaded = ContentTask.from_row(row)
        self.assertEqual(loaded.target_publish_account_id, "tocrystal66")
        legacy = ContentTask.from_row(
            {**row, "target_publish_account_id": None})
        self.assertEqual(legacy.target_publish_account_id, "")

    def test_target_account_consistency_rules(self):
        manifest = {"target_publish_account_id": "acc-a"}
        # 队列 context 与清单一致、领取账号一致 → 放行
        _assert_target_account_consistency(
            manifest, {"target_publish_account_id": "acc-a"}, "acc-a")
        # 清单无目标（旧任务）→ 任何账号按旧语义放行
        _assert_target_account_consistency({}, {}, "acc-any")
        # context 改绑 → 拒绝
        from services.release_gate import ReleaseGateError
        with self.assertRaises(ReleaseGateError):
            _assert_target_account_consistency(
                manifest, {"target_publish_account_id": "acc-b"}, "acc-a")
        # 领取账号不是目标 → 拒绝
        with self.assertRaises(ReleaseGateError):
            _assert_target_account_consistency(manifest, {}, "acc-b")
        # 清单有目标、context 无目标（列缺失的历史行）→ 领取账号仍必须一致
        with self.assertRaises(ReleaseGateError):
            _assert_target_account_consistency(manifest, {}, "acc-b")


if __name__ == "__main__":
    unittest.main()
