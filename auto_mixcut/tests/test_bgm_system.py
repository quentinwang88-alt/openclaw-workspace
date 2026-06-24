from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.skills.bgm_library_skill import BgmLibrarySkill
from auto_mixcut.skills.bgm_tag_fusion_skill import BgmTagFusionSkill
from auto_mixcut.skills.bgm_tagging_skill import BgmTaggingSkill
from auto_mixcut.skills.bgm_usage_skill import BgmUsageSkill, refresh_bgm_track_usage
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill
from auto_mixcut.skills.render_skill import _choose_bgm_candidate_for_batch
from scripts.dedupe_bgm_tracks import apply_dedupe_plan, build_dedupe_plan


class BgmSystemTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        os.environ["AUTO_MIXCUT_ROOT"] = str(Path(__file__).resolve().parents[1])
        os.environ["AUTO_MIXCUT_DB"] = str(root / "db.sqlite")
        os.environ["AUTO_MIXCUT_OSS_ROOT"] = str(root / "oss")
        os.environ["AUTO_MIXCUT_OSS_PROVIDER"] = "local"
        os.environ["AUTO_MIXCUT_TEMP_ROOT"] = str(root / "tmp")
        os.environ["AUTO_MIXCUT_MOCK_FFMPEG"] = "1"
        os.environ["AUTO_MIXCUT_MOCK_LLM"] = "1"
        self.ctx = build_context()
        init = RDSRepositorySkill(self.ctx).init_db()
        self.assertTrue(init.success, init.to_dict())

    def tearDown(self):
        self.tmp.cleanup()

    def test_local_library_tagging_and_audio_fusion_are_consumable(self):
        sync = BgmLibrarySkill(self.ctx).sync_local_library()
        self.assertTrue(sync.success, sync.to_dict())
        self.assertGreater(sync.data["synced"], 0)

        track = self.ctx.repo.list_where("bgm_tracks", "1=1 ORDER BY bgm_id")[0]
        self.assertEqual(track["bgm_tag_status"], "fallback")
        self.assertTrue(track["mood_tags_json"])
        tag = BgmTaggingSkill(self.ctx).tag_track(track["bgm_id"], force=True)
        self.assertTrue(tag.success, tag.to_dict())

        self.ctx.repo.update(
            "bgm_tracks",
            "bgm_id",
            track["bgm_id"],
            {
                "audio_analysis_json": {
                    "audio_suggested_tags": {
                        "mood_tags": ["fashion_chic"],
                        "energy_level": "high",
                        "vocal_type": "instrumental",
                    },
                    "mix_suggestions": {
                        "recommended_start_sec": 4,
                        "default_volume": 0.18,
                        "fade_in_ms": 300,
                        "fade_out_ms": 700,
                        "voiceover_friendly": True,
                    },
                    "tag_confidence": "high",
                },
                "audio_tag_confidence": "high",
            },
        )
        fuse = BgmTagFusionSkill(self.ctx).fuse_track(track["bgm_id"])
        self.assertTrue(fuse.success, fuse.to_dict())

        row = self.ctx.repo.get("bgm_tracks", "bgm_id", track["bgm_id"])
        self.assertEqual(row["energy_level"], "high")
        self.assertEqual(row["vocal_type"], "instrumental")
        self.assertIn("fashion_chic", row["mood_tags_json"])
        self.assertEqual(row["recommended_start_sec"], 4)
        self.assertIn("tag_fusion", row["mix_constraints_json"])

        rec = BgmLibrarySkill(self.ctx).get_recommendation(category="generic_fashion", mood="fashion_chic")
        self.assertTrue(rec.success, rec.to_dict())
        self.assertTrue(rec.data["recommendations"])

    def test_bgm_recommendation_prefers_matching_template_tag(self):
        self.ctx.repo.upsert(
            "bgm_tracks",
            "bgm_id",
            {
                "bgm_id": "BGM_GENERIC_MATCH",
                "track_name": "Generic chic",
                "mood_tags_json": json.dumps(["fashion_chic"]),
                "category_tags_json": json.dumps(["generic_fashion"]),
                "template_tags_json": json.dumps([]),
                "bgm_tag_status": "tagged",
                "tag_confidence": "high",
                "default_volume": 0.2,
            },
        )
        self.ctx.repo.upsert(
            "bgm_tracks",
            "bgm_id",
            {
                "bgm_id": "BGM_OUTERWEAR_TEMPLATE_MATCH",
                "track_name": "Outerwear product first",
                "mood_tags_json": json.dumps(["fashion_chic"]),
                "category_tags_json": json.dumps(["womens_outerwear"]),
                "template_tags_json": json.dumps(["AI_OUTERWEAR_PRODUCT_FIRST_20S"]),
                "bgm_tag_status": "tagged",
                "tag_confidence": "high",
                "default_volume": 0.18,
            },
        )

        rec = BgmLibrarySkill(self.ctx).get_recommendation(
            category="womens_outerwear",
            mood="fashion_chic",
            template_id="AI_OUTERWEAR_PRODUCT_FIRST_20S",
        )
        self.assertTrue(rec.success, rec.to_dict())
        self.assertEqual(rec.data["recommendations"][0]["bgm_id"], "BGM_OUTERWEAR_TEMPLATE_MATCH")
        self.assertEqual(rec.data["recommendations"][0]["default_volume"], 0.18)
        self.assertEqual(rec.data["recommendations"][0]["degrade_mode"], "observe")
        self.assertEqual(rec.data["recommendations"][0]["usage_count"], 0)

    def test_bgm_recommendation_keeps_untagged_pool_with_penalty(self):
        self.ctx.repo.upsert(
            "bgm_tracks",
            "bgm_id",
            {
                "bgm_id": "BGM_ONLY_FALLBACK",
                "track_name": "Known fallback",
                "mood_tags_json": json.dumps(["daily_clean"]),
                "category_tags_json": json.dumps(["generic_fashion"]),
                "template_tags_json": json.dumps([]),
                "bgm_tag_status": "fallback",
                "tag_confidence": "low",
                "status": "active",
                "license_status": "verified",
            },
        )
        self.ctx.repo.upsert(
            "bgm_tracks",
            "bgm_id",
            {
                "bgm_id": "BGM_UNTAGGED_STILL_USABLE",
                "track_name": "Fresh synced track",
                "mood_tags_json": json.dumps(["fashion_chic"]),
                "category_tags_json": json.dumps(["womens_outerwear"]),
                "template_tags_json": json.dumps([]),
                "energy_level": "high",
                "vocal_type": "instrumental",
                "bgm_tag_status": "untagged",
                "tag_confidence": "low",
                "status": "active",
                "license_status": "verified",
            },
        )

        rec = BgmLibrarySkill(self.ctx).get_recommendation(
            category="womens_outerwear",
            mood="fashion_chic",
            energy="high",
            preferred_vocal_types=["instrumental"],
        )

        self.assertTrue(rec.success, rec.to_dict())
        ids = {item["bgm_id"] for item in rec.data["recommendations"]}
        self.assertIn("BGM_UNTAGGED_STILL_USABLE", ids)
        untagged = next(item for item in rec.data["recommendations"] if item["bgm_id"] == "BGM_UNTAGGED_STILL_USABLE")
        self.assertEqual(untagged["degrade_mode"], "untagged_penalty")

    def test_bgm_recommendation_uses_energy_vocal_and_filters_blocked_tracks(self):
        base = {
            "mood_tags_json": json.dumps(["fashion_chic"]),
            "category_tags_json": json.dumps(["womens_outerwear"]),
            "template_tags_json": json.dumps([]),
            "bgm_tag_status": "tagged",
            "tag_confidence": "high",
            "status": "active",
            "license_status": "verified",
        }
        self.ctx.repo.upsert("bgm_tracks", "bgm_id", {"bgm_id": "BGM_HIGH_INST", "track_name": "High inst", **base, "energy_level": "high", "vocal_type": "instrumental"})
        self.ctx.repo.upsert("bgm_tracks", "bgm_id", {"bgm_id": "BGM_LOW_VOCAL", "track_name": "Low vocal", **base, "energy_level": "low", "vocal_type": "vocal"})
        self.ctx.repo.upsert("bgm_tracks", "bgm_id", {"bgm_id": "BGM_PAUSED", "track_name": "Paused", **base, "energy_level": "high", "vocal_type": "instrumental", "status": "paused"})
        self.ctx.repo.upsert("bgm_tracks", "bgm_id", {"bgm_id": "BGM_LICENSE_BLOCKED", "track_name": "Blocked", **base, "energy_level": "high", "vocal_type": "instrumental", "license_status": "unavailable"})

        rec = BgmLibrarySkill(self.ctx).get_recommendation(
            category="womens_outerwear",
            mood="fashion_chic",
            energy="high",
            preferred_vocal_types=["instrumental"],
        )

        self.assertTrue(rec.success, rec.to_dict())
        ids = [item["bgm_id"] for item in rec.data["recommendations"]]
        self.assertEqual(ids[0], "BGM_HIGH_INST")
        self.assertNotIn("BGM_PAUSED", ids)
        self.assertNotIn("BGM_LICENSE_BLOCKED", ids)

    def test_bgm_recommendation_dedupes_track_identity_and_prefers_operational_record(self):
        base = {
            "track_name": "Duplicate Song",
            "artist_name": "Same Artist",
            "mood_tags_json": json.dumps(["fashion_chic"]),
            "category_tags_json": json.dumps(["womens_outerwear"]),
            "template_tags_json": json.dumps([]),
            "bgm_tag_status": "tagged",
            "tag_confidence": "high",
            "status": "active",
            "energy_level": "high",
            "vocal_type": "instrumental",
        }
        self.ctx.repo.upsert("bgm_tracks", "bgm_id", {"bgm_id": "BGM_OLD_DUP", **base})
        self.ctx.repo.upsert(
            "bgm_tracks",
            "bgm_id",
            {
                "bgm_id": "BGM_NEW_DUP",
                **base,
                "license_status": "pending",
                "oss_object_id": "OSS_DUP",
            },
        )

        rec = BgmLibrarySkill(self.ctx).get_recommendation(
            category="womens_outerwear",
            mood="fashion_chic",
            energy="high",
            preferred_vocal_types=["instrumental"],
        )

        ids = [item["bgm_id"] for item in rec.data["recommendations"]]
        self.assertIn("BGM_NEW_DUP", ids)
        self.assertNotIn("BGM_OLD_DUP", ids)
        names = [item["track_name"] for item in rec.data["recommendations"]]
        self.assertEqual(names.count("Duplicate Song"), 1)

    def test_bgm_dedupe_archives_duplicates_and_canonicalizes_references(self):
        base = {
            "track_name": "Duplicate Migration Song",
            "artist_name": "Same Artist",
            "mood_tags_json": json.dumps(["fashion_chic"]),
            "category_tags_json": json.dumps(["womens_outerwear"]),
            "template_tags_json": json.dumps([]),
            "bgm_tag_status": "tagged",
            "tag_confidence": "high",
            "status": "active",
            "license_status": "pending",
        }
        self.ctx.repo.upsert("bgm_tracks", "bgm_id", {"bgm_id": "BGM_OLD_MIGRATE", **base})
        self.ctx.repo.upsert(
            "bgm_tracks",
            "bgm_id",
            {
                "bgm_id": "BGM_20260618010101_NEW",
                **base,
                "oss_object_id": "OSS_NEW_MIGRATE",
            },
        )
        self.ctx.repo.upsert(
            "outputs",
            "output_id",
            {
                "output_id": "OUT_BGM_MIGRATE",
                "render_status": "rendered",
                "machine_quality_status": "publish_ready",
                "bgm_plan_json": {"bgm_id": "BGM_OLD_MIGRATE", "track_name": "Duplicate Migration Song"},
            },
        )
        self.ctx.repo.insert(
            "bgm_usage_events",
            {
                "event_id": "BGMUSE_MIGRATE",
                "bgm_id": "BGM_OLD_MIGRATE",
                "output_id": "OUT_BGM_MIGRATE",
                "usage_status": "rendered",
                "quality_status": "publish_ready",
            },
        )

        plan = build_dedupe_plan(self.ctx.repo.list_where("bgm_tracks", "1=1"))
        apply_dedupe_plan(self.ctx, plan)

        output = self.ctx.repo.get("outputs", "output_id", "OUT_BGM_MIGRATE")
        self.assertEqual(output["bgm_plan_json"]["bgm_id"], "BGM_20260618010101_NEW")
        self.assertEqual(output["bgm_plan_json"]["legacy_bgm_id"], "BGM_OLD_MIGRATE")
        event = self.ctx.repo.get("bgm_usage_events", "event_id", "BGMUSE_MIGRATE")
        self.assertEqual(event["bgm_id"], "BGM_20260618010101_NEW")
        old = self.ctx.repo.get("bgm_tracks", "bgm_id", "BGM_OLD_MIGRATE")
        self.assertEqual(old["status"], "paused")
        self.assertIn("duplicate_archived", old["bgm_tag_reason"])
        new = self.ctx.repo.get("bgm_tracks", "bgm_id", "BGM_20260618010101_NEW")
        self.assertEqual(new["usage_count"], 1)

    def test_batch_bgm_picker_avoids_reused_track_name(self):
        for idx in range(2):
            self.ctx.repo.upsert(
                "outputs",
                "output_id",
                {
                    "output_id": f"OUT_BGM_USED_{idx}",
                    "batch_id": "BATCH_BGM_DIVERSITY",
                    "product_id": "PROD_BGM_DIVERSITY",
                    "variant_no": idx + 1,
                    "template_id": "AI_PRODUCT_FIRST_20S",
                    "duration_ms": 20000,
                    "render_status": "rendered",
                    "machine_quality_status": "pending",
                    "human_quality_status": "pending",
                    "bgm_plan_json": {"bgm_id": f"BGM_USED_{idx}", "track_name": "Same Song"},
                },
            )

        best = _choose_bgm_candidate_for_batch(
            self.ctx,
            [
                {"bgm_id": "BGM_USED_0", "track_name": "Same Song", "score": 100},
                {"bgm_id": "BGM_NEW", "track_name": "Fresh Song", "score": 80},
            ],
            {"batch_id": "BATCH_BGM_DIVERSITY", "variant_no": 3},
        )

        self.assertEqual(best["bgm_id"], "BGM_NEW")

    def test_ad_bgm_recommendation_avoids_recent_product_track(self):
        base = {
            "mood_tags_json": json.dumps(["premium_clean"]),
            "category_tags_json": json.dumps(["hair_accessories"]),
            "template_tags_json": json.dumps(["AD_FAST_HOOK_8S"]),
            "energy_level": "high",
            "vocal_type": "instrumental",
            "bgm_tag_status": "tagged",
            "tag_confidence": "high",
            "status": "active",
            "license_status": "verified",
        }
        self.ctx.repo.upsert("bgm_tracks", "bgm_id", {"bgm_id": "BGM_USED_AD", "track_name": "Used Ad Song", **base, "usage_count": 8})
        self.ctx.repo.upsert("bgm_tracks", "bgm_id", {"bgm_id": "BGM_FRESH_AD", "track_name": "Fresh Ad Song", **base, "usage_count": 0})
        self.ctx.repo.upsert(
            "outputs",
            "output_id",
            {
                "output_id": "OUT_RECENT_AD_BGM",
                "batch_id": "BATCH_RECENT_AD_BGM",
                "product_id": "PROD_RECENT_AD_BGM",
                "variant_no": 1,
                "template_id": "AD_FAST_HOOK_8S",
                "duration_ms": 8000,
                "render_status": "rendered",
                "machine_quality_status": "publish_ready",
                "human_quality_status": "pending",
                "bgm_plan_json": {"bgm_id": "BGM_USED_AD", "track_name": "Used Ad Song"},
            },
        )

        rec = BgmLibrarySkill(self.ctx).get_recommendation(
            product_id="PROD_RECENT_AD_BGM",
            category="hair_accessories",
            mood="premium_clean",
            template_id="AD_FAST_HOOK_8S",
            energy="high",
            preferred_vocal_types=["instrumental"],
        )

        self.assertTrue(rec.success, rec.to_dict())
        self.assertEqual(rec.data["recommendations"][0]["bgm_id"], "BGM_FRESH_AD")

    def test_bgm_usage_feedback_counts_rejected_outputs(self):
        self.ctx.repo.upsert(
            "bgm_tracks",
            "bgm_id",
            {
                "bgm_id": "BGM_FEEDBACK",
                "track_name": "Feedback Song",
                "mood_tags_json": json.dumps(["fashion_chic"]),
                "category_tags_json": json.dumps(["generic_fashion"]),
                "template_tags_json": json.dumps([]),
                "bgm_tag_status": "tagged",
                "tag_confidence": "high",
                "default_volume": 0.2,
            },
        )
        self.ctx.repo.upsert(
            "outputs",
            "output_id",
            {
                "output_id": "OUT_BGM_FEEDBACK",
                "batch_id": "BATCH_BGM_FEEDBACK",
                "product_id": "PROD_BGM_FEEDBACK",
                "variant_no": 1,
                "template_id": "AI_PRODUCT_FIRST_20S",
                "duration_ms": 20000,
                "render_status": "rendered",
                "machine_quality_status": "publish_ready",
                "human_quality_status": "pending",
                "bgm_plan_json": {"bgm_id": "BGM_FEEDBACK", "track_name": "Feedback Song"},
            },
        )
        self.ctx.repo.insert(
            "bgm_usage_events",
            {
                "event_id": "BGMUSE_FEEDBACK",
                "bgm_id": "BGM_FEEDBACK",
                "output_id": "OUT_BGM_FEEDBACK",
                "batch_id": "BATCH_BGM_FEEDBACK",
                "product_id": "PROD_BGM_FEEDBACK",
                "template_id": "AI_PRODUCT_FIRST_20S",
                "usage_status": "rendered",
                "quality_status": "pending",
                "reason": "render_success",
            },
        )

        res = BgmUsageSkill(self.ctx).record_output_feedback("OUT_BGM_FEEDBACK", "human_rejected", "feishu:不可发布")

        self.assertTrue(res.success, res.to_dict())
        event = self.ctx.repo.get("bgm_usage_events", "event_id", "BGMUSE_FEEDBACK")
        track = self.ctx.repo.get("bgm_tracks", "bgm_id", "BGM_FEEDBACK")
        self.assertEqual(event["quality_status"], "human_rejected")
        self.assertEqual(track["usage_count"], 1)
        self.assertEqual(track["rejected_usage_count"], 1)
        self.assertEqual(track["last_feedback_status"], "human_rejected")

    def test_bgm_usage_counts_reconciled_rejected_events(self):
        self.ctx.repo.upsert(
            "bgm_tracks",
            "bgm_id",
            {
                "bgm_id": "BGM_RECONCILED_REJECTED",
                "track_name": "Rejected event only",
                "bgm_tag_status": "tagged",
            },
        )
        self.ctx.repo.insert(
            "bgm_usage_events",
            {
                "event_id": "BGMUSE_RECONCILED_REJECTED",
                "bgm_id": "BGM_RECONCILED_REJECTED",
                "output_id": "OUT_RECONCILED_REJECTED",
                "usage_status": "rejected",
                "quality_status": "rejected",
                "reason": "reconciled_from_outputs",
            },
        )

        counters = refresh_bgm_track_usage(self.ctx, "BGM_RECONCILED_REJECTED")

        self.assertEqual(counters["rendered_usage_count"], 0)
        self.assertEqual(counters["rejected_usage_count"], 1)
        track = self.ctx.repo.get("bgm_tracks", "bgm_id", "BGM_RECONCILED_REJECTED")
        self.assertEqual(track["rejected_usage_count"], 1)

    def test_bgm_recommendation_lightly_penalizes_repeated_rejections(self):
        base = {
            "mood_tags_json": json.dumps(["fashion_chic"]),
            "category_tags_json": json.dumps(["womens_outerwear"]),
            "template_tags_json": json.dumps(["AI_OUTERWEAR_PRODUCT_FIRST_20S"]),
            "bgm_tag_status": "tagged",
            "tag_confidence": "high",
            "energy_level": "high",
            "default_volume": 0.2,
        }
        self.ctx.repo.upsert("bgm_tracks", "bgm_id", {"bgm_id": "BGM_CLEAN", "track_name": "Clean", **base, "usage_count": 2, "rejected_usage_count": 0})
        self.ctx.repo.upsert("bgm_tracks", "bgm_id", {"bgm_id": "BGM_REJECTED", "track_name": "Rejected", **base, "usage_count": 4, "rejected_usage_count": 3})

        rec = BgmLibrarySkill(self.ctx).get_recommendation(
            category="womens_outerwear",
            mood="fashion_chic",
            template_id="AI_OUTERWEAR_PRODUCT_FIRST_20S",
        )

        self.assertTrue(rec.success, rec.to_dict())
        self.assertEqual(rec.data["recommendations"][0]["bgm_id"], "BGM_CLEAN")


if __name__ == "__main__":
    unittest.main()
