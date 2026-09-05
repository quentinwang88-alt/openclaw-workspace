#!/usr/bin/env python3

from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import sys
import unittest


SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from app import bgm  # noqa: E402


class BgmPolicyTest(unittest.TestCase):
    def test_chinese_mexico_country_normalized_before_music_search(self):
        from types import SimpleNamespace
        observed = []
        class Source:
            def __init__(self, client):
                pass
            def fetch(self, account_id, country, **kwargs):
                observed.append((country, kwargs['language']))
                return []
        class DB:
            def recent_bgm_use_counts(self, *args, **kwargs):
                return {}
        with self.assertRaises(bgm.BgmSelectionError):
            bgm.select_platform_bgm(DB(), SimpleNamespace(client=object()),
                account_id='mx-account', now=datetime(2026, 9, 4), source_factory=Source,
                candidate=SimpleNamespace(target_country='墨西哥',
                    script_text='{"video_duration_ms":15000}', audio_mode='generated_nonvoice'))
        self.assertEqual(observed, [('MX', 'es-MX')])

    def test_selects_fresh_country_music_and_builds_organic_contract(self):
        first = bgm.neobund_music.BgmCandidate(
            "used", "Used", rank=1, mood_tags=("dance",),
            duration_ms=30_000,
            raw={"author": "A", "play_url": "https://audio/used", "cover_url": ""},
        )
        second = bgm.neobund_music.BgmCandidate(
            "fresh", "Fresh", rank=2, mood_tags=("dance",),
            duration_ms=30_000,
            raw={"author": "B", "play_url": "https://audio/fresh", "cover_url": "https://cover"},
        )

        class Source:
            def __init__(self, client):
                self.client = client

            def fetch(self, *args, **kwargs):
                return [first, second]

        class DB:
            def recent_bgm_use_counts(self, account_id, since):
                return {"used": 2}

        class Publisher:
            client = object()

        class Candidate:
            target_country = "TH"
            script_text = json.dumps({
                "bgm_mood_hints": ["dance"], "video_duration_ms": 12_000,
                "audio_mode": "silent_source_platform_bgm",
            })

        selection, audit_json = bgm.select_platform_bgm(
            DB(), Publisher(), account_id="acc", candidate=Candidate(),
            now=datetime(2026, 9, 1, 10, 0), source_factory=Source,
        )
        self.assertEqual(selection["music_id"], "fresh")
        self.assertEqual(selection["video_original_sound_volume"], 0)
        self.assertEqual(selection["music_sound_volume"], 70)
        self.assertEqual(json.loads(audit_json)["country"], "TH")

    def test_shared_policy_accepts_organic_remake_but_not_cart_video(self):
        class Opv:
            script_source = "图文养号"
            publish_purpose = "养号"
            cart_enabled = "否"
            audio_mode = "silent_source_platform_bgm"

        class Other:
            script_source = "养号复刻"
            publish_purpose = "养号"
            cart_enabled = "否"
            audio_mode = "generated_nonvoice"

        class CartVideo:
            script_source = "成功脚本复刻"
            publish_purpose = "带货"
            cart_enabled = "是"
            audio_mode = "generated_nonvoice"

        self.assertTrue(bgm.requires_platform_bgm(Opv()))
        self.assertTrue(bgm.requires_platform_bgm(Other()))
        self.assertFalse(bgm.requires_platform_bgm(CartVideo()))

    def test_remake_profile_requires_real_audio_for_strong_rhythm(self):
        class Candidate:
            script_source = "成功脚本复刻"
            publish_purpose = "养号"
            content_branch = "SUCCESS_SCRIPT_REPLICATION"
            product_type = "假发"
            short_video_title = "变装揭晓"
            script_text = "{}"
            audio_mode = "generated_nonvoice"

        profile = bgm.derive_bgm_profile(Candidate(), {})
        self.assertEqual(profile["rhythm_preference"], "strong")
        self.assertEqual(profile["sync_mode"], "reveal")

    def test_plain_script_and_false_voiceover_do_not_imply_audio_state(self):
        class Candidate:
            script_source = "养号复刻"
            publish_purpose = "养号"
            content_branch = "SUCCESS_SCRIPT_REPLICATION"
            product_type = "假发"
            short_video_title = "日常展示"
            script_text = '口播讲解：这顶假发很自然'
            cart_enabled = "否"

        profile = bgm.derive_bgm_profile(Candidate(), {})
        self.assertEqual(profile["audio_mode"], "unknown")
        self.assertFalse(bgm.requires_platform_bgm(Candidate()))

        profile = bgm.derive_bgm_profile(
            Candidate(), {"voiceover_requested": False, "audio_mode": "generated_nonvoice"},
        )
        self.assertFalse(profile["voice_present"])
        self.assertEqual(profile["video_original_sound_volume"], 0)

    def test_calm_remake_is_not_promoted_to_transformation_without_template(self):
        class Candidate:
            script_source = "成功脚本复刻"
            publish_purpose = "养号"
            content_branch = "SUCCESS_SCRIPT_REPLICATION"
            product_type = "咖啡"
            short_video_title = "咖啡生活氛围"

        profile = bgm.derive_bgm_profile(Candidate(), {})
        self.assertEqual(profile["rhythm_preference"], "soft")
        self.assertEqual(profile["sync_mode"], "none")

    def test_strong_selection_expands_after_used_candidates_before_analysis(self):
        candidates = [
            bgm.neobund_music.BgmCandidate(
                f"used-{index}", "Used", rank=index + 1, mood_tags=("dance",), duration_ms=30_000,
                raw={"author": "A", "play_url": f"https://audio/{index}"},
            )
            for index in range(8)
        ]
        ninth = bgm.neobund_music.BgmCandidate(
            "ninth", "Verified", rank=9, mood_tags=("dance",), duration_ms=30_000,
            raw={"author": "B", "play_url": "https://audio/ninth"},
        )
        candidates.append(ninth)

        class Source:
            def __init__(self, client): self.client = client
            def fetch(self, *args, **kwargs): return candidates

        class DB:
            def recent_bgm_use_counts(self, account_id, since):
                return {f"used-{index}": 2 for index in range(8)}

        class Publisher: client = object()
        class Candidate:
            target_country = "MX"
            script_source = "养号复刻"
            publish_purpose = "养号"
            content_branch = "SUCCESS_SCRIPT_REPLICATION"
            product_type = "假发"
            short_video_title = "变装揭晓"
            cart_enabled = "否"
            audio_mode = "generated_nonvoice"
            script_text = json.dumps({"video_duration_ms": 10_000})

        def enrich(items):
            for item in items:
                item.raw["audio_analysis"] = {
                    "status": "ready", "strong_rhythm": item.music_id == "ninth",
                    "features": {"rhythm_strength": 0.9 if item.music_id == "ninth" else 0.1},
                }
            return items

        selection, _audit = bgm.select_platform_bgm(
            DB(), Publisher(), account_id="acc", candidate=Candidate(), now=datetime(2026, 9, 1),
            source_factory=Source, audio_enricher=enrich,
        )
        self.assertEqual(selection["music_id"], "ninth")

    def test_short_track_is_never_selected_for_long_video(self):
        class Source:
            def __init__(self, client): self.client = client
            def fetch(self, *args, **kwargs):
                return [bgm.neobund_music.BgmCandidate(
                    "short", "Short", mood_tags=("bright",), duration_ms=8_000,
                    raw={"author": "A", "play_url": "https://audio/short"},
                )]
        class DB:
            def recent_bgm_use_counts(self, account_id, since): return {}
        class Publisher: client = object()
        class Candidate:
            target_country = "TH"
            cart_enabled = "否"
            audio_mode = "silent_source_platform_bgm"
            script_text = json.dumps({"video_duration_ms": 15_000})

        with self.assertRaisesRegex(bgm.BgmSelectionError, "时长足够"):
            bgm.select_platform_bgm(DB(), Publisher(), account_id="acc", candidate=Candidate(),
                                    now=datetime(2026, 9, 1), source_factory=Source)

    def test_remake_selects_only_audio_verified_strong_candidate(self):
        weak = bgm.neobund_music.BgmCandidate(
            "weak", "Dance by name only", rank=1, mood_tags=("dance",),
            duration_ms=30_000,
            raw={"author": "A", "play_url": "https://audio/weak"},
        )
        strong = bgm.neobund_music.BgmCandidate(
            "strong", "Verified", rank=2, mood_tags=("dance",),
            duration_ms=30_000,
            raw={"author": "B", "play_url": "https://audio/strong"},
        )

        class Source:
            def __init__(self, client):
                self.client = client

            def fetch(self, *args, **kwargs):
                return [weak, strong]

        class DB:
            def recent_bgm_use_counts(self, account_id, since):
                return {}

        class Publisher:
            client = object()

        class Candidate:
            target_country = "MX"
            script_source = "成功脚本复刻"
            publish_purpose = "养号"
            content_branch = "SUCCESS_SCRIPT_REPLICATION"
            product_type = "假发"
            short_video_title = "变装"
            script_text = json.dumps({"video_duration_ms": 10_000})
            local_file_path = ""
            audio_mode = "generated_nonvoice"

        def enrich(items):
            strong.raw["audio_analysis"] = {
                "status": "ready", "strong_rhythm": True,
                "features": {"rhythm_strength": 0.91},
                "beat_times_ms": [500, 1000, 1500, 2000],
            }
            weak.raw["audio_analysis"] = {
                "status": "ready", "strong_rhythm": False,
                "features": {"rhythm_strength": 0.20},
                "beat_times_ms": [900],
            }
            return items

        selection, audit_json = bgm.select_platform_bgm(
            DB(), Publisher(), account_id="acc", candidate=Candidate(),
            now=datetime(2026, 9, 1, 10, 0), source_factory=Source,
            audio_enricher=enrich,
        )
        self.assertEqual(selection["music_id"], "strong")
        audit = json.loads(audit_json)
        self.assertEqual(audit["profile"]["sync_mode"], "reveal")
        self.assertEqual(audit["timing_plan"]["status"], "platform_offset_unverified")


if __name__ == "__main__":
    unittest.main()
