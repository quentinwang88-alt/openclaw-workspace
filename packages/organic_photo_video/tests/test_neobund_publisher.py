#!/usr/bin/env python3
"""Publish flow tests: human gate, audio gating, ambiguity never resubmits."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys
import unittest
from unittest import mock

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from domain.models import ContentShot, ContentTask, PublishRecord, VideoRender
from domain.statuses import (
    PUBLISH_PUBLISHED,
    PUBLISH_SUBMITTED,
    TASK_FAILED,
    TASK_PUBLISH_PREPARING,
    TASK_PUBLISHING,
    TASK_PUBLISHED,
    TASK_READY_TO_PUBLISH,
    TASK_VIDEO_REVIEW,
)
from services import neobund_publisher
from services.neobund_music import BgmCandidate
from services.neobund_publisher import (
    NeoBundMusicFieldsPendingError,
    NeoBundTrendingMusicSource,
    OpvPublishFlow,
    PublishFlowError,
)


class FakeRepository:
    def __init__(self) -> None:
        self.tasks = {}
        self.shots = {}
        self.renders = {}
        self.records = {}
        self.themes = {}
        self.transitions = []

    def get_task(self, task_id):
        return self.tasks.get(task_id)

    def list_shots(self, task_id):
        return [s for s in self.shots.values() if s.task_id == task_id]

    def transition_task(self, task_id, frm, to, **kwargs):
        from domain import statuses

        statuses.task_ensure_transition(frm, to)
        task = self.tasks[task_id]
        assert task.task_status == frm, f"{task.task_status} != {frm}"
        task.task_status = to
        if to == TASK_FAILED:
            task.failure_code = kwargs.get("failure_code")
        self.transitions.append((frm, to))

    def get_render(self, render_id):
        return self.renders.get(render_id)

    def get_theme(self, theme_id):
        return self.themes.get(theme_id)

    def get_publish_record(self, publish_id):
        return self.records.get(publish_id)

    def get_publish_record_by_render(self, render_id):
        for record in self.records.values():
            if record.render_id == render_id:
                return record
        return None

    def list_publish_records_by_account(self, account_id, limit=50):
        return [r for r in self.records.values() if r.account_id == account_id]

    def insert_publish_record(self, record):
        self.records[record.publish_id] = record

    def update_publish_result(self, publish_id, **fields):
        record = self.records[publish_id]
        for key, value in fields.items():
            setattr(record, key, value)


class FakeAdapter:
    def __init__(self, behavior="ok") -> None:
        self.behavior = behavior
        self.calls = []
        self.status_results = []

    def create_scheduled_task(self, **kwargs):
        self.calls.append(kwargs)
        if self.behavior == "ok":
            return "NB12345"
        if self.behavior == "ambiguous":
            return ""
        raise RuntimeError("neobund down")

    def query_task_status(self, *, task_id, scheduled_for):
        return self.status_results.pop(0)


class PendingMusicSource(NeoBundTrendingMusicSource):
    def fetch(self, account_id, country, *, language=None):
        raise NeoBundMusicFieldsPendingError()


class OkMusicSource(NeoBundTrendingMusicSource):
    def fetch(self, account_id, country, *, language=None):
        return [
            BgmCandidate(
                "music_1", "Bright Pop", rank=1, mood_tags=("bright",),
                duration_ms=30000,
                raw={"author": "Daniel", "play_url": "https://cdn/music_1.mp3",
                     "cover_url": "https://cdn/music_1.webp"},
            ),
            BgmCandidate(
                "music_2", "Other", rank=2, mood_tags=("sad",),
                duration_ms=30000,
                raw={"author": "a2", "play_url": "https://cdn/music_2.mp3",
                     "cover_url": "https://cdn/music_2.webp"},
            ),
        ]


class RotatingMusicSource(NeoBundTrendingMusicSource):
    """Song music_1 has rotated out of the hot list at submit time."""

    def fetch(self, account_id, country, *, language=None):
        return [BgmCandidate("music_9", "New Hit", rank=1, duration_ms=30000)]


def build_world(repo, music_source):
    task = ContentTask(
        task_id="opv_task_1",
        idempotency_key="a" * 64,
        account_id="OPV_TH_TEST_001",
        product_id="P1",
        target_country="TH",
        target_locale="th-TH",
        task_status=TASK_VIDEO_REVIEW,
        current_stage="video_review",
        selected_render_id="render_1",
        copy_json={"title": "ลุคสนามบิน", "caption": "c", "hashtags": [], "cover_text": "x"},
        plan_json={"theme": {"id": "THEME_T"}, "shots": []},
    )
    repo.tasks["opv_task_1"] = task
    repo.renders["render_1"] = VideoRender(
        render_id="render_1",
        task_id="opv_task_1",
        render_preset_id="RP",
        render_status="completed",
        duration_ms=12534,
        output_url="/tmp/opv_task_1_v1.mp4",
        qc_status="passed",
        publish_ready=True,
    )
    repo.shots["shot_1"] = ContentShot(
        shot_id="shot_1", task_id="opv_task_1", slot_index=1,
        slot_role="hero", duration_ms=2200, is_selected=True,
    )
    adapter = FakeAdapter()
    flow = OpvPublishFlow(repo, adapter=adapter, music_source=music_source)
    return task, adapter, flow


class PreparePublishTest(unittest.TestCase):
    def setUp(self) -> None:
        self.repo = FakeRepository()
        self.task, self.adapter, self.flow = build_world(self.repo, PendingMusicSource())

    def test_prepare_records_human_gate_and_pending_audio(self) -> None:
        record = self.flow.prepare_publish("opv_task_1", operator="老板")
        self.assertEqual(self.task.task_status, TASK_PUBLISH_PREPARING)
        self.assertEqual(record.publish_status, "ready")
        self.assertEqual(record.operator_name, "老板")
        self.assertEqual(record.caption_snapshot_json["title"], "ลุคสนามบิน")
        audio = record.platform_metadata_json["audio"]
        self.assertEqual(audio["status"], "pending_field_capture")
        self.assertEqual(audio["country"], "TH")
        self.assertEqual(record.cover_shot_id, "shot_1")

    def test_prepare_requires_operator(self) -> None:
        with self.assertRaises(PublishFlowError):
            self.flow.prepare_publish("opv_task_1", operator="  ")

    def test_prepare_rejects_non_video_review_status(self) -> None:
        self.task.task_status = TASK_PUBLISHING
        with self.assertRaises(PublishFlowError):
            self.flow.prepare_publish("opv_task_1", operator="老板")

    def test_music_source_without_client_raises_clear_error(self) -> None:
        with self.assertRaises(PublishFlowError) as ctx:
            NeoBundTrendingMusicSource().fetch("a", "TH")
        self.assertIn("no client configured", str(ctx.exception))

    def test_arm_blocked_while_audio_pending(self) -> None:
        self.flow.prepare_publish("opv_task_1", operator="老板")
        with self.assertRaises(PublishFlowError) as ctx:
            self.flow.arm_for_publish("opv_task_1")
        self.assertIn("audio decision unresolved", str(ctx.exception))

    def test_arm_with_explicit_silent_fallback(self) -> None:
        self.flow.prepare_publish("opv_task_1", operator="老板")
        self.flow.arm_for_publish("opv_task_1", allow_without_bgm=True)
        self.assertEqual(self.task.task_status, TASK_READY_TO_PUBLISH)
        record = self.repo.get_publish_record_by_render("render_1")
        audio = record.platform_metadata_json["audio"]
        self.assertEqual(audio["audio_strategy"], "no_bgm")
        self.assertEqual(audio["fallback_reason"], "operator_choice_silent")

    def test_arm_blocked_when_selection_failed(self) -> None:
        class BrokenSource(NeoBundTrendingMusicSource):
            def fetch(self, account_id, country, *, language=None):
                raise RuntimeError("network down")

        repo = FakeRepository()
        task, _adapter, flow = build_world(repo, BrokenSource())
        flow.prepare_publish("opv_task_1", operator="老板")
        record = repo.get_publish_record_by_render("render_1")
        self.assertEqual(record.platform_metadata_json["audio"]["status"], "selection_failed")
        with self.assertRaises(PublishFlowError):
            flow.arm_for_publish("opv_task_1")

    def test_arm_ok_when_bgm_selected(self) -> None:
        repo = FakeRepository()
        task, adapter, flow = build_world(repo, OkMusicSource())
        flow.prepare_publish("opv_task_1", operator="老板")
        record = repo.get_publish_record_by_render("render_1")
        self.assertEqual(record.platform_metadata_json["audio"]["selected"]["music_id"], "music_1")
        flow.arm_for_publish("opv_task_1")
        self.assertEqual(task.task_status, TASK_READY_TO_PUBLISH)

    def test_prepare_twice_rejected_by_existing_record(self) -> None:
        self.flow.prepare_publish("opv_task_1", operator="老板")
        with self.assertRaises(PublishFlowError):
            self.flow.prepare_publish("opv_task_1", operator="老板")

    def test_refresh_bgm_selection_upgrades_pending_record(self) -> None:
        self.flow.prepare_publish("opv_task_1", operator="老板")
        record = self.repo.get_publish_record_by_render("render_1")
        self.assertEqual(record.platform_metadata_json["audio"]["status"], "pending_field_capture")
        self.flow._music_source = OkMusicSource()
        refreshed = self.flow.refresh_bgm_selection("opv_task_1")
        audio = refreshed.platform_metadata_json["audio"]
        self.assertNotIn("status", audio)
        self.assertEqual(audio["selected"]["music_id"], "music_1")


def music_page(ids, *, next_token="", has_more="False"):
    return {
        "music": [
            {
                "id": music_id,
                "title": f"song {music_id}",
                "author": f"author {music_id}",
                "duration": "28",
                "cover_thumb": {"url_list": ["https://cdn/x.webp"]},
                "play_url": {"url_list": ["https://cdn/x.mp3"]},
            }
            for music_id in ids
        ],
        "next_page_token": next_token,
        "has_more": has_more,
    }


class TrendingMusicSourceTest(unittest.TestCase):
    """Real list contract captured 2026-08-31 (POST search/music)."""

    def test_fetch_builds_contract_payload_and_parses_candidates(self) -> None:
        class FakeClient:
            def __init__(self):
                self.posts = []

            def post(self, path, payload):
                self.posts.append((path, payload))
                return music_page(["m1", "m2"])

        client = FakeClient()
        source = NeoBundTrendingMusicSource(client)
        candidates = source.fetch("OPV_TH_TEST_001", "TH")
        path, payload = client.posts[0]
        self.assertEqual(path, "/shoppable/image/search/music")  # client adds /np base
        self.assertEqual(payload["type"], 2)
        self.assertEqual(payload["keyword"], "hot")
        self.assertEqual(payload["page_size"], 20)
        self.assertEqual(payload["region"], "TH")
        self.assertEqual(payload["language"], "th-TH")
        self.assertNotIn("page_token", payload)
        self.assertEqual(len(candidates), 2)
        self.assertEqual(candidates[0].music_id, "m1")
        self.assertEqual(candidates[0].rank, 1)
        self.assertEqual(candidates[0].duration_ms, 28000)
        self.assertEqual(candidates[0].raw["author"], "author m1")

    def test_fetch_paginates_within_pool_then_switches_pool(self) -> None:
        class FakeClient:
            def __init__(self):
                self.posts = []

            def post(self, path, payload):
                self.posts.append((path, dict(payload)))
                if len(self.posts) == 1:
                    return music_page(["m1"], next_token="20", has_more="True")
                if len(self.posts) == 2:
                    return music_page(["m2"])
                return music_page(["m3"])

        source = NeoBundTrendingMusicSource(FakeClient())
        candidates = source.fetch("a", "VN")
        self.assertEqual([c.music_id for c in candidates], ["m1", "m2", "m3"])
        self.assertEqual(candidates[2].rank, 3)
        hot_second = source._client.posts[1][1]
        self.assertEqual(hot_second["keyword"], "hot")
        self.assertEqual(hot_second["page_token"], "20")
        dance_first = source._client.posts[2][1]
        self.assertEqual(dance_first["keyword"], "dance")
        self.assertNotIn("page_token", dance_first)  # new pool starts fresh

    def test_fetch_stops_at_pool_cap(self) -> None:
        class EndlessClient:
            def __init__(self):
                self.calls = 0

            def post(self, path, payload):
                self.calls += 1
                return music_page(
                    [f"m{self.calls}"], next_token=str(self.calls * 20), has_more="True"
                )

        source = NeoBundTrendingMusicSource(EndlessClient())
        candidates = source.fetch("a", "TH")
        # hot pool capped at 2 pages + dance pool 1 page, even with has_more
        self.assertEqual(len(candidates), 3)
        self.assertEqual(source._client.calls, 3)

    def test_fetch_maps_locale_and_skips_bad_items(self) -> None:
        class FakeClient:
            def post(self, path, payload):
                assert payload["region"] == "MX"
                assert payload["language"] == "es-MX"
                return {
                    "music": [{"id": "", "title": ""}, {"id": "ok", "title": "t"}],
                    "has_more": "False",
                }

        candidates = NeoBundTrendingMusicSource(FakeClient()).fetch("a", "MX")
        self.assertEqual([c.music_id for c in candidates], ["ok"])

    def test_dance_pool_gets_rhythm_mood_tags(self) -> None:
        class FakeClient:
            def post(self, path, payload):
                if payload["keyword"] == "hot":
                    return {"music": [{"id": "h1", "title": "Hot Song", "duration": "60"}], "has_more": "False"}
                return {
                    "music": [
                        {"id": "d1", "title": "Dance Remix", "author": "DJ", "duration": "60"},
                        {"id": "d2", "title": "Sad Piano", "author": "x", "duration": "60"},
                    ],
                    "has_more": "False",
                }

        candidates = NeoBundTrendingMusicSource(FakeClient()).fetch("a", "TH")
        by_id = {c.music_id: c for c in candidates}
        self.assertIn("dance", by_id["d1"].mood_tags)
        self.assertIn("high_energy", by_id["d1"].mood_tags)
        self.assertIn("soft", by_id["d2"].mood_tags)


class SubmitAndConfirmTest(unittest.TestCase):
    def setUp(self) -> None:
        # These tests exercise the post-capture world where the NeoBund music
        # commit fields exist; the flag is the capture gate in production.
        patcher = mock.patch.object(neobund_publisher, "MUSIC_FIELDS_CAPTURED", True)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.repo = FakeRepository()
        self.task, self.adapter, self.flow = build_world(self.repo, OkMusicSource())
        self.flow.prepare_publish("opv_task_1", operator="老板")
        self.flow.arm_for_publish("opv_task_1")

    def test_submit_refuses_without_audio_resolution(self) -> None:
        repo2 = FakeRepository()
        task2, _a, flow2 = build_world(repo2, PendingMusicSource())
        flow2.prepare_publish("opv_task_1", operator="老板")
        flow2.arm_for_publish("opv_task_1", allow_without_bgm=True)
        record = repo2.get_publish_record_by_render("render_1")
        # make audio look unresolved-but-not-pending to hit the submit guard
        record.platform_metadata_json["audio"].pop("audio_strategy")
        with self.assertRaises(PublishFlowError):
            flow2.submit("opv_task_1", publish_at=datetime(2026, 8, 31, 12, 0, 0))
        self.assertEqual(len(_a.calls), 0)

    def test_submit_confirmed_published(self) -> None:
        self.adapter.status_results.append(type("S", (), {"state": "published"})())
        external = self.flow.submit("opv_task_1", publish_at=datetime(2026, 8, 31, 12, 0, 0))
        self.assertEqual(external, "NB12345")
        self.assertEqual(len(self.adapter.calls), 1)
        call = self.adapter.calls[0]
        self.assertEqual(call["account_id"], "OPV_TH_TEST_001")
        self.assertEqual(call["script_id"], "opv_task_1")
        # captured commit contract: the selection rides on the organic commit
        self.assertEqual(call["music_selection"]["music_id"], "music_1")
        self.assertEqual(call["music_selection"]["music_url"], "https://cdn/music_1.mp3")
        self.assertEqual(call["music_selection"]["music_sound_volume"], 50)
        self.assertEqual(self.task.task_status, TASK_PUBLISHING)
        record = self.repo.get_publish_record_by_render("render_1")
        self.assertEqual(record.publish_status, PUBLISH_SUBMITTED)
        self.assertEqual(record.external_post_id, "NB12345")
        self.assertEqual(
            record.platform_metadata_json["music_attached"]["music_title"], "Bright Pop"
        )

        result = self.flow.confirm_result("opv_task_1")
        self.assertEqual(result, "published")
        self.assertEqual(self.task.task_status, TASK_PUBLISHED)
        self.assertEqual(record.publish_status, PUBLISH_PUBLISHED)
        self.assertIsNotNone(record.published_at)

    def test_submit_legacy_no_url_and_unfindable_refuses(self) -> None:
        repo2 = FakeRepository()
        task2, adapter2, flow2 = build_world(repo2, OkMusicSource())
        flow2.prepare_publish("opv_task_1", operator="老板")
        flow2.arm_for_publish("opv_task_1")
        # simulate a legacy record without persisted urls; the track also
        # cannot be re-found by pools or title search
        record2 = repo2.get_publish_record_by_render("render_1")
        selected = record2.platform_metadata_json["audio"]["selected"]
        selected.pop("music_url")
        selected.pop("music_cover_url")
        flow2._music_source = RotatingMusicSource()
        with self.assertRaises(PublishFlowError) as ctx:
            flow2.submit("opv_task_1", publish_at=datetime(2026, 8, 31, 12, 0, 0))
        self.assertIn("no persisted url", str(ctx.exception))
        self.assertEqual(len(adapter2.calls), 0)
        self.assertEqual(task2.task_status, TASK_READY_TO_PUBLISH)

    def test_ambiguous_submit_never_resubmits(self) -> None:
        self.adapter.behavior = "ambiguous"
        external = self.flow.submit("opv_task_1", publish_at=datetime(2026, 8, 31, 12, 0, 0))
        self.assertEqual(external, "")
        record = self.repo.get_publish_record_by_render("render_1")
        self.assertEqual(record.publish_status, PUBLISH_SUBMITTED)
        self.assertTrue(record.platform_metadata_json["needs_requery"])
        self.assertEqual(self.task.task_status, TASK_PUBLISHING)

        # a second submit must be impossible from publishing state
        with self.assertRaises(PublishFlowError):
            self.flow.submit("opv_task_1", publish_at=datetime(2026, 8, 31, 13, 0, 0))
        self.assertEqual(len(self.adapter.calls), 1)  # still exactly one commit

        # requery resolves it
        self.adapter.status_results.append(type("S", (), {"state": "published"})())
        self.assertEqual(self.flow.confirm_result("opv_task_1"), "published")
        self.assertEqual(len(self.adapter.calls), 1)

    def test_submit_failure_fails_task_and_record(self) -> None:
        self.adapter.behavior = "raise"
        with self.assertRaises(RuntimeError):
            self.flow.submit("opv_task_1", publish_at=datetime(2026, 8, 31, 12, 0, 0))
        self.assertEqual(self.task.task_status, TASK_FAILED)
        self.assertEqual(self.task.failure_code, "neobund_submit_failed")
        record = self.repo.get_publish_record_by_render("render_1")
        self.assertEqual(record.publish_status, "failed")

    def test_confirm_pending_keeps_publishing_state(self) -> None:
        self.adapter.status_results.append(type("S", (), {"state": "processing"})())
        self.flow.submit("opv_task_1", publish_at=datetime(2026, 8, 31, 12, 0, 0))
        self.assertEqual(self.flow.confirm_result("opv_task_1"), "pending")
        self.assertEqual(self.task.task_status, TASK_PUBLISHING)
        record = self.repo.get_publish_record_by_render("render_1")
        self.assertEqual(record.publish_status, PUBLISH_SUBMITTED)


if __name__ == "__main__":
    unittest.main()
