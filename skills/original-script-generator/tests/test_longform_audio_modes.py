"""Audio-mode contract tests for the shared Plan C long-form executor.

Original generated jobs and frozen remake jobs share one executor, so the only
safe distinction is the declared audio mode:

* ``PRESERVE_SOURCE_COPY`` — remake copy is approved copy; real Edge audio is
  measured but the text hash must never change and the rewrite model is never
  called.
* ``NO_VOICEOVER`` — no TTS at all, yet the master still needs a real audio
  stream for validation and publishing.
* ``UNSPECIFIED`` / ``DIALOGUE_REQUIRES_PROVIDER`` — block before any paid H3
  submission instead of guessing.
* no declared mode at all — an original job keeps its historical behaviour,
  where the central voiceover model may perform exactly one targeted revision.
"""

from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from core.longform import workflow
from core.longform.audio import _binary, validate_finalized_media
from core.longform.voiceover import (
    SOURCE_VOICEOVER_OVERFLOW,
    measure_frozen_source_voiceover_with_edge,
    voiceover_text_hash,
)


def _make_silent_video(path: Path, seconds: float = 2.0) -> Path:
    """A real H3-shaped master: video stream only, no audio stream at all."""

    subprocess.run(
        [
            _binary("ffmpeg"), "-v", "error", "-y", "-f", "lavfi", "-i",
            f"testsrc2=size=180x320:rate=24:duration={seconds}",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "26",
            "-pix_fmt", "yuv420p", "-an", str(path),
        ],
        check=True, capture_output=True,
    )
    return path


class FakeStorage:
    """Minimal storage stub that keeps row mutations visible to the runner."""

    def __init__(self, row: dict):
        self.row = dict(row)
        self.status_history: list[str] = []
        self.segment_updates: list[tuple] = []

    def get_job(self, _job_id):
        return dict(self.row)

    def update_job(self, _job_id, status, **kwargs):
        self.status_history.append(str(status))
        self.row["status"] = status
        self.row.update(kwargs)

    def update_segment(self, _job_id, segment_id, **kwargs):
        self.segment_updates.append((segment_id, kwargs))
        for item in self.row.get("segments") or []:
            if str(item.get("segment_id")) == segment_id:
                item.update(kwargs)
                return


def _plan(*, audio_mode: str | None, segment_ids=("A", "B"), seconds_per_segment=15):
    plan = {
        "segments": [
            {"segment_id": segment_id, "duration_seconds": seconds_per_segment,
             "generation_mode": "first_frame"}
            for segment_id in segment_ids
        ],
        "target_duration_seconds": seconds_per_segment * len(segment_ids),
    }
    if audio_mode:
        plan["audio_contract"] = {"mode": audio_mode}
    return json.dumps(plan, ensure_ascii=False)


class AudioModeResolutionTests(unittest.TestCase):
    def test_voiceover_contract_wins_over_plan_contract(self):
        self.assertEqual(
            workflow._audio_mode(
                {"audio_contract": {"mode": "NO_VOICEOVER"}},
                {"mode": "PRESERVE_SOURCE_COPY"},
            ),
            "PRESERVE_SOURCE_COPY",
        )
        self.assertEqual(
            workflow._audio_mode({"audio_contract": {"mode": "NO_VOICEOVER"}}, {}),
            "NO_VOICEOVER",
        )
        self.assertEqual(
            workflow._audio_mode({"audio_contract": {"mode": "no_voiceover"}}, {}),
            "NO_VOICEOVER",
        )

    def test_absent_mode_is_empty_not_a_guess(self):
        # An original job declares nothing; the executor reads that as "use the
        # historical TTS path", never as "this remake must be silent".
        self.assertEqual(workflow._audio_mode({}, {}), "")
        self.assertNotIn("", workflow.AUDIO_MODE_UNRESOLVED)


class UnresolvedModeBlocksTests(unittest.TestCase):
    def _run(self, mode: str):
        row = {
            "status": "PLANNED",
            "plan_json": _plan(audio_mode=mode),
            "voiceover_json": "{}",
            "segments": [
                {"segment_id": "A", "status": "PLANNED"},
                {"segment_id": "B", "status": "PLANNED"},
            ],
        }
        storage = FakeStorage(row)
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(workflow, "H3Gateway") as gateway, mock.patch.object(
                workflow, "calibrate_longform_voiceover_with_edge"
            ) as rewrite, mock.patch.object(
                workflow, "measure_frozen_source_voiceover_with_edge"
            ) as frozen:
                result = workflow.run_to_final(
                    storage, "LFJ_AUDIO", asset_root=directory,
                    allow_real_submit=True, allow_external_tts=True,
                )
        return storage, gateway, rewrite, frozen, result

    def test_unspecified_and_dialogue_modes_block_before_any_paid_call(self):
        for mode in ("UNSPECIFIED", "DIALOGUE_REQUIRES_PROVIDER"):
            with self.subTest(mode=mode):
                storage, gateway, rewrite, frozen, result = self._run(mode)
                self.assertEqual(result["status"], "BLOCKED_AUDIO_MODE")
                self.assertIn(mode, result["message"])
                # The gateway may be constructed, but no paid action may follow:
                # no credential preflight, no submit, no polling, no download.
                for action in ("preflight", "submit", "query", "download"):
                    getattr(gateway.return_value, action).assert_not_called()
                rewrite.assert_not_called()
                frozen.assert_not_called()
                # The job must not look submitted in any way.
                self.assertTrue(
                    all(status == "PLANNED" for status in storage.status_history),
                    storage.status_history,
                )
                stages = [item["stage"] for item in result["report"]["events"]]
                self.assertIn("BLOCKED_AUDIO_MODE", stages)


class OriginalJobCompatibilityTests(unittest.TestCase):
    def test_original_job_without_declared_mode_keeps_tts_behaviour(self):
        # No audio_contract, no voiceover mode: an original job must stay on the
        # historical path (voiceover required), not be blocked as a remake.
        row = {
            "status": "PLANNED",
            "plan_json": _plan(audio_mode=None),
            "voiceover_json": "{}",
            "segments": [
                {"segment_id": "A", "status": "PLANNED"},
                {"segment_id": "B", "status": "PLANNED"},
            ],
        }
        storage = FakeStorage(row)
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(workflow, "H3Gateway") as gateway:
                result = workflow.run_to_final(
                    storage, "LFJ_ORIGINAL", asset_root=directory,
                    allow_real_submit=True, allow_external_tts=True,
                )
        gateway.return_value.preflight.assert_not_called()
        self.assertEqual(result["status"], "WAITING_VOICEOVER")
        self.assertNotEqual(result["status"], "BLOCKED_AUDIO_MODE")


class NoVoiceoverTests(unittest.TestCase):
    def test_no_tts_yet_final_master_still_carries_an_audio_stream(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            merged = _make_silent_video(root / "merged_silent.mp4")
            row = {
                "status": "MERGED",
                "product_code": "P_TEST",
                "plan_json": _plan(audio_mode="NO_VOICEOVER"),
                "voiceover_json": json.dumps({"mode": "NO_VOICEOVER", "target_text": ""}),
                "merged_video_path": str(merged),
                "segments": [
                    {"segment_id": "A", "status": "READY", "output_video_path": str(merged)},
                    {"segment_id": "B", "status": "READY", "output_video_path": str(merged)},
                ],
            }
            storage = FakeStorage(row)
            with mock.patch.object(workflow, "H3Gateway") as gateway, mock.patch.object(
                workflow, "finalize_with_voiceover"
            ) as spoken, mock.patch.object(
                workflow, "calibrate_longform_voiceover_with_edge"
            ) as rewrite, mock.patch.object(
                workflow, "measure_frozen_source_voiceover_with_edge"
            ) as frozen:
                gateway.return_value.preflight.return_value = {"ready": True}
                result = workflow.run_to_final(
                    storage, "LFJ_SILENT", asset_root=root,
                    allow_real_submit=True, allow_external_tts=True,
                )

            self.assertEqual(result["status"], "FINAL_READY")
            self.assertNotIn("WAITING_VOICEOVER", storage.status_history)
            spoken.assert_not_called()
            rewrite.assert_not_called()
            frozen.assert_not_called()

            final_path = Path(result["final_video_path"])
            self.assertTrue(final_path.is_file())
            validation = validate_finalized_media(final_path)
            self.assertIn("audio", validation["stream_types"])
            self.assertIn("video", validation["stream_types"])
            self.assertTrue(final_path.stat().st_size >= 10_240)

            stages = [item["stage"] for item in result["report"]["events"]]
            self.assertIn("FINAL_READY_SILENT", stages)


class FrozenSourceVoiceoverTests(unittest.TestCase):
    PLAN = {
        "segments": [
            {"segment_id": "A", "duration_seconds": 15},
            {"segment_id": "B", "duration_seconds": 15},
        ],
        "target_duration_seconds": 30,
    }

    def _measure(self, voiceover, *, effective_seconds, sections_mock=None):
        written = {}

        def fake_tts(_text, path, **_kwargs):
            Path(path).write_bytes(b"mp3-fixture")
            written["path"] = Path(path)

        with tempfile.TemporaryDirectory() as directory:
            with mock.patch(
                "core.longform.voiceover._synthesize_edge", side_effect=fake_tts
            ) as synthesize, mock.patch(
                "core.longform.voiceover.audio_duration_seconds", return_value=effective_seconds
            ), mock.patch(
                "core.longform.voiceover.measure_speech_window",
                return_value={"effective_tts_seconds": effective_seconds,
                              "raw_tts_seconds": effective_seconds,
                              "trim_status": "MEASURED"},
            ), mock.patch(
                "core.longform.voiceover.synthesize_segment_preflight"
            ) as preflight, mock.patch(
                "core.longform.voiceover._invoke_voiceover_model"
            ) as model:
                if sections_mock is not None:
                    preflight.return_value = sections_mock
                result = measure_frozen_source_voiceover_with_edge(
                    self.PLAN, dict(voiceover), directory, voice_id="test-voice",
                )
            return result, synthesize, model, preflight, written

    def test_per_shot_copy_produces_abc_sections_and_whole_paragraph_stays_continuous(self):
        # A/B/C per-segment copy: the executor mirrors the segment layout.
        sections = [
            {"segment_id": "A", "duration_seconds": 15, "target_text": "第一段"},
            {"segment_id": "B", "duration_seconds": 15, "target_text": "第二段"},
        ]
        result, synthesize, model, preflight, _ = self._measure(
            {"mode": "PRESERVE_SOURCE_COPY", "target_text": "第一段 第二段",
             "semantic_sections": sections},
            effective_seconds=12.0,
            sections_mock={"sections": [
                {"segment_id": item["segment_id"], "effective_tts_seconds": 11.0,
                 "planned_segment_seconds": 15.0} for item in sections
            ], "actual_tts_seconds_total": 22.0, "all_acceptable": True},
        )
        preflight.assert_called_once()
        synthesize.assert_not_called()
        model.assert_not_called()
        self.assertFalse(result["tts_preflight"]["revision_attempted"])
        self.assertEqual(result["tts_preflight"]["revision_limit"], 0)
        self.assertEqual(result["duration_fit"]["method"], "EDGE_TTS_FROZEN_SOURCE_COPY")

        # A whole-paragraph read (no per-segment split) stays one continuous TTS.
        whole, synthesize, model, preflight, written = self._measure(
            {"mode": "PRESERVE_SOURCE_COPY", "target_text": "整段口播一次读完",
             "semantic_sections": []},
            effective_seconds=20.0,
        )
        preflight.assert_not_called()
        model.assert_not_called()
        synthesize.assert_called_once()
        self.assertEqual(written["path"].suffix, ".mp3")
        self.assertEqual(
            whole["tts_preflight"]["layout"], "WHOLE_PARAGRAPH_CONTINUOUS",
        )
        self.assertEqual(whole["tts_preflight"]["revision_limit"], 0)
        self.assertFalse(whole["tts_preflight"]["revision_attempted"])
        self.assertEqual(whole["tts_preflight"]["readiness"], "READY")

    def test_frozen_text_hash_is_never_rewritten(self):
        text = "拒绝任何改写：这是冻结原口播"
        digest = voiceover_text_hash(text)
        result, synthesize, model, _preflight, _ = self._measure(
            {"mode": "PRESERVE_SOURCE_COPY", "target_text": text,
             "target_text_sha256": digest, "semantic_sections": []},
            effective_seconds=10.0,
        )
        self.assertEqual(result["target_text"], text)
        self.assertEqual(voiceover_text_hash(result["target_text"]), digest)
        self.assertEqual(result["target_text_sha256"], digest)
        model.assert_not_called()
        synthesize.assert_called_once()

    def test_mutated_frozen_text_blocks_before_tts(self):
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch("core.longform.voiceover._synthesize_edge") as synthesize:
                with self.assertRaises(ValueError) as raised:
                    measure_frozen_source_voiceover_with_edge(
                        self.PLAN,
                        {"mode": "PRESERVE_SOURCE_COPY", "target_text": "已被改写的新口播",
                         "target_text_sha256": voiceover_text_hash("原始冻结口播")},
                        directory,
                    )
        self.assertIn("SOURCE_VOICEOVER_TEXT_MUTATED", str(raised.exception))
        synthesize.assert_not_called()

    def test_overflow_blocks_instead_of_speeding_down_or_rewriting(self):
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch(
                "core.longform.voiceover._synthesize_edge",
                side_effect=lambda _text, path, **_kwargs: Path(path).write_bytes(b"mp3"),
            ), mock.patch(
                "core.longform.voiceover.audio_duration_seconds", return_value=600.0
            ), mock.patch(
                "core.longform.voiceover.measure_speech_window",
                return_value={"effective_tts_seconds": 600.0, "trim_status": "MEASURED"},
            ), mock.patch(
                "core.longform.voiceover.choose_narration_rate", return_value=0
            ), mock.patch(
                "core.longform.voiceover._invoke_voiceover_model"
            ) as model:
                with self.assertRaises(ValueError) as raised:
                    measure_frozen_source_voiceover_with_edge(
                        self.PLAN,
                        {"mode": "PRESERVE_SOURCE_COPY", "target_text": "超长原口播",
                         "semantic_sections": []},
                        directory, voice_id="test-voice",
                    )
        self.assertIn(SOURCE_VOICEOVER_OVERFLOW, str(raised.exception))
        model.assert_not_called()

    def test_short_frozen_copy_is_never_slowed_down_to_fill_time(self):
        # 6s of copy inside a 30s video must stay at rate 0.
        _result, synthesize, _model, _preflight, written = self._measure(
            {"mode": "PRESERVE_SOURCE_COPY", "target_text": "很短的口播",
             "semantic_sections": []},
            effective_seconds=6.0,
        )
        synthesize.assert_called_once()
        self.assertIn("rate_0", written["path"].name)


if __name__ == "__main__":
    unittest.main()
