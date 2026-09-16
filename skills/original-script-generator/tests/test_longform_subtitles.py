"""Frozen on-screen copy is reproduced in post processing, never by the model.

A remake source may name on-screen text.  That copy is stripped from the video
prompt and carried in ``postprocess_contract.subtitles``; these tests pin the
consumer of that contract: cue extraction, ASS generation, font resolution and a
real burn that provably draws glyphs on the frozen timeline.

An original generated job declares no subtitle contract, so it must stay a no-op.
"""

from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from core.longform.audio import _binary
from core.longform.subtitles import (
    SUBTITLE_MODE_PRESERVE,
    build_ass_document,
    burn_subtitles,
    resolve_subtitle_font,
    subtitle_cues,
)


THAI_TEXT = "เสื้อโค้ทปุ่มทอง"


def plan_with(cues, *, mode: str = SUBTITLE_MODE_PRESERVE, language: str = "泰语") -> dict:
    return {
        "target_language": language,
        "postprocess_contract": {
            "subtitles": {"mode": mode, "items": list(cues)},
        },
    }


def _black_video(path: Path, seconds: float = 3.0, size: str = "540x960") -> Path:
    subprocess.run(
        [
            _binary("ffmpeg"), "-v", "error", "-y", "-f", "lavfi", "-i",
            f"color=c=black:s={size}:d={seconds}:r=24",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "20",
            "-pix_fmt", "yuv420p", "-an", str(path),
        ],
        check=True, capture_output=True,
    )
    return path


def _make_colored_video(path: Path, seconds: float = 2.0) -> Path:
    """An H3-shaped merged master: video only, no audio, above the size floor."""

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


def _bottom_band_brightness(path: Path, at_seconds: float) -> tuple[int, int]:
    """Return (peak, mean) luma of the lower half of one decoded frame."""

    completed = subprocess.run(
        [
            _binary("ffmpeg"), "-v", "error", "-ss", f"{at_seconds:.3f}", "-i", str(path),
            "-vf", "crop=iw:ih/2:0:ih/2,format=gray,scale=iw:ih", "-frames:v", "1",
            "-f", "rawvideo", "-",
        ],
        check=True, capture_output=True,
    )
    data = completed.stdout
    if not data:
        raise AssertionError("没有解出任何像素")
    return max(data), sum(data) // len(data)


class SubtitleContractTests(unittest.TestCase):
    def test_original_plan_has_no_cues_and_is_never_burned(self):
        for plan in ({}, {"segments": []}, {"postprocess_contract": {}},
                     {"postprocess_contract": {"subtitles": {}}}):
            with self.subTest(plan=plan):
                self.assertEqual([], subtitle_cues(plan))

    def test_only_preserve_source_timeline_mode_is_burned(self):
        cues = [{"cue_id": "CUE_01", "start_ms": 0, "end_ms": 1000, "text": "x"}]
        self.assertEqual(1, len(subtitle_cues(plan_with(cues))))
        # The contract is read case-insensitively ...
        self.assertEqual(
            1, len(subtitle_cues(plan_with(cues, mode="preserve_source_timeline"))),
        )
        # ... but any other mode means the master is delivered without burned copy.
        for mode in ("NONE", "", "POST_PROCESS", "BURN_ALWAYS"):
            with self.subTest(mode=mode):
                self.assertEqual([], subtitle_cues(plan_with(cues, mode=mode)))

    def test_frozen_wording_and_timing_are_carried_verbatim(self):
        cues = [
            {"cue_id": "CUE_02", "start_ms": 3000, "end_ms": 6000, "text": "第二句"},
            {"cue_id": "CUE_01", "start_ms": 1000, "end_ms": 2500, "text": THAI_TEXT,
             "source_shot_id": "SHOT_01"},
            {"cue_id": "CUE_03", "start_ms": 5000, "end_ms": 4000, "text": "倒挂区间"},
            {"cue_id": "CUE_04", "start_ms": 0, "end_ms": 1000, "text": "   "},
            "not-a-mapping",
        ]
        parsed = subtitle_cues(plan_with(cues))
        self.assertEqual([THAI_TEXT, "第二句"], [cue["text"] for cue in parsed])
        self.assertEqual(1000, parsed[0]["start_ms"])
        self.assertEqual(6000, parsed[1]["end_ms"])
        self.assertEqual("SHOT_01", parsed[0]["source_shot_id"])
        # Nothing is re-worded, translated or trimmed.
        self.assertIn(THAI_TEXT, json.dumps(parsed, ensure_ascii=False))

    def test_ass_document_keeps_special_characters_intact(self):
        document = build_ass_document(
            [{"start_ms": 3000, "end_ms": 4500, "text": 'A: "金色按扣" {促销} 50%'}],
            font_name="Thonburi",
        )
        self.assertIn("[Script Info]", document)
        self.assertIn("Style: FrozenCopy,Thonburi,", document)
        self.assertIn(
            "Dialogue: 0,0:00:03.00,0:00:04.50,FrozenCopy,,0,0,0,,"
            'A: "金色按扣" \\{促销\\} 50%',
            document,
        )

    def test_multiline_copy_becomes_ass_line_breaks(self):
        document = build_ass_document(
            [{"start_ms": 0, "end_ms": 1000, "text": "第一行\n第二行"}],
            font_name="Thonburi",
        )
        self.assertIn("第一行\\N第二行", document)
        self.assertNotIn("第一行\n第二行", document)

    def test_thai_target_resolves_a_font_that_exists(self):
        font = resolve_subtitle_font("泰语")
        self.assertTrue(font.is_file(), font)
        self.assertEqual(font, resolve_subtitle_font("th-TH"))
        chinese = resolve_subtitle_font("中文")
        self.assertTrue(chinese.is_file(), chinese)

    def test_missing_font_fails_loudly_instead_of_drawing_boxes(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(RuntimeError, "字体"):
                resolve_subtitle_font(
                    "泰语", override=Path(directory) / "no-such-font.ttf",
                )


class SubtitleBurnTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_burn_draws_the_frozen_copy_on_the_source_timeline(self):
        merged = _black_video(self.root / "merged_silent.mp4")
        cues = subtitle_cues(plan_with([
            {"cue_id": "CUE_01", "start_ms": 500, "end_ms": 2500, "text": THAI_TEXT},
        ]))
        output = self.root / "merged_captioned.mp4"
        result = burn_subtitles(merged, cues, output, target_language="泰语")

        self.assertEqual("SUBTITLES_BURNED", result["action"])
        self.assertEqual(1, result["cue_count"])
        self.assertEqual("PASS", result["media_validation"]["status"])
        self.assertTrue(output.is_file())
        self.assertTrue(Path(result["ass_path"]).is_file())
        self.assertTrue((self.root / "subtitles.json").is_file())
        # The frozen copy is inside the rendered artifact...
        self.assertIn(THAI_TEXT, Path(result["ass_path"]).read_text(encoding="utf-8"))

        # ...and it is really on screen: a black source frame gains glyphs.
        source_peak, _ = _bottom_band_brightness(merged, 1.5)
        burned_peak, burned_mean = _bottom_band_brightness(output, 1.5)
        self.assertLess(source_peak, 16)
        self.assertGreater(burned_peak, 128)
        self.assertGreater(burned_mean, 0)

        # The burn is idempotent and duration-preserving.
        again = burn_subtitles(merged, cues, output, target_language="泰语")
        self.assertEqual("IDEMPOTENT_REUSE", again["action"])
        self.assertEqual(result["text_sha256"], again["text_sha256"])
        self.assertAlmostEqual(3.0, again["video_seconds"], delta=0.2)

    def test_copy_outside_its_window_is_not_drawn(self):
        merged = _black_video(self.root / "merged_silent.mp4")
        cues = subtitle_cues(plan_with([
            {"cue_id": "CUE_01", "start_ms": 500, "end_ms": 1500, "text": THAI_TEXT},
        ]))
        output = self.root / "merged_captioned.mp4"
        burn_subtitles(merged, cues, output, target_language="泰语")
        inside_peak, _ = _bottom_band_brightness(output, 1.0)
        after_peak, _ = _bottom_band_brightness(output, 2.5)
        self.assertGreater(inside_peak, 128)
        self.assertLess(after_peak, 16)

    def test_empty_cues_are_rejected_rather_than_silently_skipped(self):
        merged = _black_video(self.root / "merged_silent.mp4")
        with self.assertRaisesRegex(ValueError, "至少一条"):
            burn_subtitles(merged, [], self.root / "out.mp4")

    def test_missing_source_video_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "不存在"):
            burn_subtitles(
                self.root / "missing.mp4",
                [{"start_ms": 0, "end_ms": 1000, "text": "x"}],
                self.root / "out.mp4",
            )


class SubtitleWiringTests(unittest.TestCase):
    """The shared executor must consume the contract, not merely carry it."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    class Storage:
        def __init__(self, row):
            self.row = dict(row)

        def get_job(self, _job_id):
            return dict(self.row)

        def update_job(self, _job_id, status, **fields):
            self.row["status"] = status
            self.row.update(fields)

        def update_segment(self, _job_id, _segment_id, **fields):
            pass

    def _job_row(self, plan: dict, merged: Path) -> dict:
        return {
            "status": "MERGED",
            "product_code": "P_SUB",
            "plan_json": json.dumps(plan, ensure_ascii=False),
            "voiceover_json": json.dumps({"mode": "NO_VOICEOVER", "target_text": ""}),
            "merged_video_path": str(merged),
            "segments": [
                {"segment_id": "A", "status": "READY", "output_video_path": str(merged)},
                {"segment_id": "B", "status": "READY", "output_video_path": str(merged)},
            ],
        }

    def test_remake_plan_burns_subtitles_before_finalizing(self):
        from core.longform import workflow

        merged = _make_colored_video(self.root / "merged_silent.mp4")
        plan = {
            "segments": [
                {"segment_id": "A", "duration_seconds": 1, "generation_mode": "first_frame"},
                {"segment_id": "B", "duration_seconds": 1, "generation_mode": "first_frame"},
            ],
            "target_duration_seconds": 2,
            "target_language": "泰语",
            "audio_contract": {"mode": "NO_VOICEOVER"},
            **plan_with([{"cue_id": "CUE_01", "start_ms": 0, "end_ms": 2000,
                          "text": THAI_TEXT}]),
        }
        storage = self.Storage(self._job_row(plan, merged))
        with mock.patch.object(workflow, "H3Gateway") as gateway, mock.patch.object(
            workflow, "finalize_silent", wraps=workflow.finalize_silent
        ) as finalize:
            gateway.return_value.preflight.return_value = {"ready": True}
            result = workflow.run_to_final(
                storage, "LFJ_SUB", asset_root=self.root,
                allow_real_submit=True, allow_external_tts=True,
            )

        self.assertEqual("FINAL_READY", result["status"])
        captioned = self.root / "LFJ_SUB" / "merged_captioned.mp4"
        self.assertTrue(captioned.is_file())
        stages = [item["stage"] for item in result["report"]["events"]]
        self.assertIn("SUBTITLES_BURNED", stages)
        # The finalized master is built from the captioned video, not the raw merge.
        self.assertEqual(
            str(captioned.resolve()),
            str(Path(finalize.call_args.args[0]).resolve()),
        )
        self.assertTrue(Path(result["final_video_path"]).is_file())

    def test_original_plan_without_a_contract_is_never_burned(self):
        from core.longform import workflow

        merged = _make_colored_video(self.root / "merged_silent.mp4")
        plan = {
            "segments": [
                {"segment_id": "A", "duration_seconds": 1, "generation_mode": "first_frame"},
                {"segment_id": "B", "duration_seconds": 1, "generation_mode": "first_frame"},
            ],
            "target_duration_seconds": 2,
            "audio_contract": {"mode": "NO_VOICEOVER"},
        }
        storage = self.Storage(self._job_row(plan, merged))
        with mock.patch.object(workflow, "H3Gateway") as gateway, mock.patch.object(
            workflow, "burn_subtitles"
        ) as burn:
            gateway.return_value.preflight.return_value = {"ready": True}
            result = workflow.run_to_final(
                storage, "LFJ_NOSUB", asset_root=self.root,
                allow_real_submit=True, allow_external_tts=True,
            )
        self.assertEqual("FINAL_READY", result["status"])
        burn.assert_not_called()
        self.assertFalse((self.root / "LFJ_NOSUB" / "merged_captioned.mp4").exists())
        self.assertEqual(
            "FINAL_READY_SILENT",
            [item["stage"] for item in result["report"]["events"]][-2],
        )


if __name__ == "__main__":
    unittest.main()
