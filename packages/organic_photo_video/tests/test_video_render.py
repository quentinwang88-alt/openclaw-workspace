#!/usr/bin/env python3
"""Video renderer + render flow tests (scripted ffmpeg/ffprobe, no binaries)."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from domain import statuses
from domain.models import ContentShot, ContentTask, RenderPreset, VideoRender
from domain.statuses import (
    RENDER_COMPLETED,
    RENDER_FAILED,
    RENDER_QUEUED,
    RENDER_RENDERING,
    SHOT_APPROVED,
    SHOT_GENERATED,
    TASK_IMAGE_REVIEW,
    TASK_RENDERING,
    TASK_VIDEO_REVIEW,
)
from services.video_render_flow import VideoRenderFlow, VideoRenderFlowError
from services.video_renderer import (
    FFmpegStillRenderer,
    build_filter_graph,
    build_timeline,
)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeRepository:
    def __init__(self) -> None:
        self.tasks = {}
        self.shots = {}
        self.renders = {}
        self.feedback = []
        self.transitions = []

    def get_task(self, task_id):
        return self.tasks.get(task_id)

    def list_shots(self, task_id):
        return [s for s in self.shots.values() if s.task_id == task_id]

    def update_shot_status(self, shot_id, frm, to):
        statuses.ensure_transition(statuses.SHOT_STATUS_TRANSITIONS, frm, to)
        shot = self.shots[shot_id]
        assert shot.shot_status == frm
        shot.shot_status = to

    def insert_look_feedback(self, feedback):
        self.feedback.append(feedback)

    def update_task_plan(self, task_id, **fields):
        task = self.tasks[task_id]
        for key, value in fields.items():
            setattr(task, key, value)

    def transition_task(self, task_id, frm, to, **kwargs):
        statuses.task_ensure_transition(frm, to)
        task = self.tasks[task_id]
        assert task.task_status == frm
        task.task_status = to
        self.transitions.append((frm, to))

    def get_account_profile(self, account_id):
        from domain.models import AccountProfile

        return AccountProfile(
            account_id="OPV_TH_TEST_001",
            account_code="opv-th-test-001",
            account_name="test",
            target_country="TH",
            default_locale="th-TH",
            timezone="Asia/Bangkok",
            default_render_preset_id="RP_STILL_VERTICAL_12S_V1",
        ) if account_id == "OPV_TH_TEST_001" else None

    def get_render_preset(self, preset_id):
        return RenderPreset(
            render_preset_id=preset_id,
            preset_key="RP",
            preset_name="12.5s",
            status="active",
        ) if preset_id in {"RP_STILL_VERTICAL_12S_V1", "RP_RECIPE_V1"} else None

    def insert_render(self, row):
        self.renders[row.render_id] = row

    def get_render(self, render_id):
        return self.renders.get(render_id)

    def list_renders(self, task_id):
        rows = [r for r in self.renders.values() if r.task_id == task_id]
        return sorted(rows, key=lambda r: r.render_version, reverse=True)

    def update_render_status(self, render_id, frm, to):
        statuses.ensure_transition(statuses.RENDER_STATUS_TRANSITIONS, frm, to)
        row = self.renders[render_id]
        assert row.render_status == frm
        row.render_status = to

    def update_render_output(self, render_id, **fields):
        row = self.renders[render_id]
        for key, value in fields.items():
            setattr(row, key, value)


class FakeRunner:
    """Records argv; ffprobe returns canned JSON, blackdetect returns clean."""

    def __init__(self, out_file: Path, probe_payload: dict, ffmpeg_rc: int = 0):
        self.out_file = out_file
        self.probe_payload = probe_payload
        self.ffmpeg_rc = ffmpeg_rc
        self.commands = []

    def __call__(self, argv):
        self.commands.append(list(argv))
        if Path(argv[0]).name == "ffprobe":
            return 0, json.dumps(self.probe_payload), ""
        if any("blackdetect" in str(arg) for arg in argv):
            return 0, "", ""
        # ffmpeg render command: simulate output file creation at argv[-1]
        if self.ffmpeg_rc == 0:
            Path(argv[-1]).write_bytes(b"fake mp4 bytes")
        return self.ffmpeg_rc, "", "some ffmpeg error" if self.ffmpeg_rc else ""


PROBE_OK = {
    "streams": [
        {
            "codec_type": "video",
            "codec_name": "h264",
            "width": 1080,
            "height": 1920,
            "pix_fmt": "yuv420p",
            "avg_frame_rate": "30/1",
        }
    ],
    "format": {"duration": "12.5"},
}


def plan_shots():
    roles = ["hero", "full_look", "lifestyle", "detail", "second_angle"]
    durations = [2200, 2600, 2500, 2200, 3000]
    return [
        {
            "slot_index": i,
            "slot_role": role,
            "purpose": f"p{i}",
            "duration_ms": durations[i - 1],
            "motion_preset": ["slow_push", "light_pan", "static_hold", "detail_zoom", "light_pan"][i - 1],
            "transition_out": "short_dissolve" if i < 5 else "cut",
            "overlay_text": "",
            "generation_prompt": "",
            "source_refs": [],
        }
        for i, role in enumerate(roles, start=1)
    ]


def build_task_and_shots(repo: FakeRepository, out_dir: Path):
    task = ContentTask(
        task_id="opv_task_1",
        idempotency_key="a" * 64,
        account_id="OPV_TH_TEST_001",
        product_id="P1",
        target_country="TH",
        target_locale="th-TH",
        task_status=TASK_IMAGE_REVIEW,
        current_stage="image_review",
        plan_json={
            "schema_version": "opv-plan-v1",
            "shots": plan_shots(),
            "look": {"ref_id": "L1", "snapshot": {}},
        },
        copy_json={"title": "t", "caption": "c", "hashtags": [], "cover_text": "x"},
    )
    repo.tasks["opv_task_1"] = task
    for i in range(1, 6):
        image = out_dir / f"P{i}.png"
        image.write_bytes(b"png")
        shot = ContentShot(
            shot_id=f"shot_{i}",
            task_id="opv_task_1",
            slot_index=i,
            slot_role=plan_shots()[i - 1]["slot_role"],
            duration_ms=plan_shots()[i - 1]["duration_ms"],
            shot_status=SHOT_GENERATED,
            qa_status="passed",
            image_url=str(image),
            image_sha256="f" * 64,
            image_width=941,
            image_height=1672,
        )
        repo.shots[f"shot_{i}"] = shot
    return task


# ---------------------------------------------------------------------------
# Renderer unit tests
# ---------------------------------------------------------------------------

class TimelineAndGraphTest(unittest.TestCase):
    def _slots(self):
        plan = plan_shots()
        shots = {
            i: ContentShot(
                shot_id=f"shot_{i}", task_id="t", slot_index=i,
                slot_role="hero", duration_ms=plan[i - 1]["duration_ms"],
                shot_version=1,
            )
            for i in range(1, 6)
        }
        images = {i: f"/tmp/P{i}.png" for i in range(1, 6)}
        return build_timeline(plan, shots, images)

    def test_timeline_keeps_total_duration_at_12500(self) -> None:
        slots = self._slots()
        # 4 dissolves (P1-P4 transition_out short_dissolve, P5 cut): total 12.5s
        total = sum(s.effective_ms for s in slots) - 4 * 300
        self.assertEqual(total, 12500)
        # Clip starts overlap by the dissolve window (fade runs start..start+300ms)
        self.assertEqual(slots[0].start_ms, 0)
        self.assertEqual(slots[1].start_ms, 2050)
        self.assertEqual(slots[4].start_ms, 9350)
        self.assertEqual(slots[4].effective_ms, 3150)
        # every planned slot keeps at least its planned duration of output time
        for slot in slots:
            self.assertGreaterEqual(slot.effective_ms, slot.planned_ms)

    def test_filter_graph_has_four_xfades_and_offsets(self) -> None:
        graph = build_filter_graph(self._slots())
        self.assertEqual(graph.count("xfade=transition=fade"), 4)
        self.assertNotIn("concat", graph)
        self.assertIn("offset=2.050", graph)
        self.assertIn("offset=4.650", graph)
        self.assertIn("offset=7.150", graph)
        self.assertIn("offset=9.350", graph)
        self.assertIn("s=1080x1920", graph)
        self.assertIn("format=yuv420p[vout]", graph)

    def test_contain_mode_preserves_the_full_board(self) -> None:
        plan = plan_shots()
        plan[0]["fit_mode"] = "contain"
        shots = {
            i: ContentShot(
                shot_id=f"s{i}", task_id="t", slot_index=i,
                slot_role="hero", duration_ms=plan[i - 1]["duration_ms"],
            )
            for i in range(1, 6)
        }
        timeline = build_timeline(plan, shots, {i: f"/p{i}" for i in range(1, 6)})
        self.assertEqual(timeline[0].fit_mode, "contain")
        graph = build_filter_graph(timeline)
        self.assertIn("force_original_aspect_ratio=decrease", graph)
        self.assertIn("pad=2160:3840", graph)

    def test_cut_transition_uses_concat(self) -> None:
        plan = plan_shots()
        plan[1]["transition_out"] = "cut"  # between P2 and P3
        shots = {
            i: ContentShot(shot_id=f"s{i}", task_id="t", slot_index=i,
                           slot_role="hero", duration_ms=plan[i - 1]["duration_ms"])
            for i in range(1, 6)
        }
        graph = build_filter_graph(build_timeline(plan, shots, {i: f"/p{i}" for i in range(1, 6)}))
        self.assertEqual(graph.count("xfade"), 3)
        self.assertIn("concat=n=2:v=1:a=0", graph)

    def test_upper_body_focus_uses_stronger_upward_biased_crop(self) -> None:
        plan = plan_shots()
        plan[3]["motion_preset"] = "upper_body_focus"
        shots = {
            i: ContentShot(
                shot_id=f"s{i}", task_id="t", slot_index=i,
                slot_role=plan[i - 1]["slot_role"], duration_ms=plan[i - 1]["duration_ms"],
                shot_version=1,
            )
            for i in range(1, 6)
        }
        graph = build_filter_graph(
            build_timeline(plan, shots, {i: f"/tmp/P{i}.png" for i in range(1, 6)})
        )
        self.assertIn("1.28+0.07*on/", graph)
        self.assertIn("(ih-ih/zoom)*0.30", graph)

    def test_build_command_flags(self) -> None:
        renderer = FFmpegStillRenderer(ffmpeg_bin="ffmpeg", ffprobe_bin="ffprobe")
        argv = renderer.build_command(self._slots(), Path("/tmp/out.mp4"))
        self.assertEqual(argv[0], "ffmpeg")
        self.assertEqual(argv.count("-i"), 5)
        self.assertIn("-an", argv)  # no audio stream: dual-audio impossible
        self.assertIn("libx264", argv)
        self.assertIn("yuv420p", argv)
        self.assertIn("1080x1920", " ".join(argv))

    @patch("services.video_renderer.shutil.which", return_value=None)
    @patch("services.video_renderer.Path.is_file", return_value=True)
    def test_default_binary_falls_back_to_user_local_bin(self, _is_file, _which) -> None:
        renderer = FFmpegStillRenderer()
        self.assertTrue(renderer._ffmpeg.endswith("/.local/bin/ffmpeg"))
        self.assertTrue(renderer._ffprobe.endswith("/.local/bin/ffprobe"))

    def test_missing_ffmpeg_becomes_render_failure_instead_of_exception(self) -> None:
        def missing(_argv):
            raise FileNotFoundError("ffmpeg")

        renderer = FFmpegStillRenderer(
            ffmpeg_bin="missing-ffmpeg", ffprobe_bin="missing-ffprobe", runner=missing
        )
        ok, error = renderer.render(self._slots(), Path("/tmp/not-created.mp4"))
        self.assertFalse(ok)
        self.assertIn("ffmpeg launch failed", error)


class VideoQcTest(unittest.TestCase):
    def test_qc_rejects_low_or_missing_fps(self):
        for rate in ("15/1", "0/1"):
            payload = json.loads(json.dumps(PROBE_OK))
            payload["streams"][0]["avg_frame_rate"] = rate
            with tempfile.TemporaryDirectory() as tmp:
                output = Path(tmp) / "out.mp4"
                output.write_bytes(b"fixture")
                qc = FFmpegStillRenderer(runner=FakeRunner(output, payload)).qc_video(output, 12500)
                self.assertFalse(qc["passed"])
                self.assertFalse(qc["checks"]["fps_30"])

    def test_decode_failure_is_a_failed_qc_not_a_clean_black_scan(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "out.mp4"
            output.write_bytes(b"fixture")
            def runner(argv):
                if Path(argv[0]).name == "ffprobe":
                    return 0, json.dumps(PROBE_OK), ""
                self.assertIn("-xerror", argv)
                return 1, "", "decode failed"
            qc = FFmpegStillRenderer(runner=runner).qc_video(output, 12500)
            self.assertFalse(qc["passed"])
            self.assertFalse(qc["checks"]["video_decodable"])

    def test_qc_passes_on_clean_probe(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out.mp4"
            out.write_bytes(b"x")
            runner = FakeRunner(out, PROBE_OK)
            renderer = FFmpegStillRenderer(runner=runner)
            qc = renderer.qc_video(out, 12500)
            self.assertTrue(qc["passed"], qc)
            self.assertEqual(qc["duration_ms"], 12500)
            self.assertEqual(qc["audio_streams"], 0)
            self.assertTrue(qc["sha256"])

    def test_qc_fails_on_bad_duration(self) -> None:
        payload = json.loads(json.dumps(PROBE_OK))
        payload["format"]["duration"] = "8.0"
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out.mp4"
            out.write_bytes(b"x")
            renderer = FFmpegStillRenderer(runner=FakeRunner(out, payload))
            qc = renderer.qc_video(out, 12500)
            self.assertFalse(qc["passed"])
            self.assertFalse(qc["checks"]["duration_10000_15000ms"])

    def test_qc_fails_when_audio_stream_present(self) -> None:
        payload = json.loads(json.dumps(PROBE_OK))
        payload["streams"].append({"codec_type": "audio", "codec_name": "aac"})
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out.mp4"
            out.write_bytes(b"x")
            renderer = FFmpegStillRenderer(runner=FakeRunner(out, payload))
            qc = renderer.qc_video(out, 12500)
            self.assertFalse(qc["checks"]["no_audio_stream"])


# ---------------------------------------------------------------------------
# Flow tests
# ---------------------------------------------------------------------------

class RenderFlowTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = FakeRepository()
        self.task = build_task_and_shots(self.repo, Path(self.tmp.name))
        self.out_file = Path(self.tmp.name) / "opv_task_1_v1.mp4"
        self.runner = FakeRunner(self.out_file, PROBE_OK)
        self.renderer = FFmpegStillRenderer(runner=self.runner)
        self.flow = VideoRenderFlow(
            self.repo, self.renderer, output_root=Path(self.tmp.name)
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_approve_group_records_decision_and_transitions(self) -> None:
        task = self.flow.approve_group("opv_task_1", reviewer="老板")
        self.assertEqual(task.task_status, TASK_RENDERING)
        for shot in self.repo.list_shots("opv_task_1"):
            self.assertEqual(shot.shot_status, SHOT_APPROVED)
        self.assertEqual(len(self.repo.feedback), 1)
        feedback = self.repo.feedback[0]
        self.assertEqual(feedback.decision, "approve")
        self.assertEqual(feedback.reviewer, "老板")
        self.assertEqual(
            task.group_qa_json["stage_d"]["decision"], "group_approved"
        )

    def test_operator_auto_release_is_not_recorded_as_human_review(self) -> None:
        task = self.flow.approve_group(
            "opv_task_1",
            reviewer="feishu_operator_auto_render",
            approval_mode="operator_auto",
        )
        self.assertEqual(task.task_status, TASK_RENDERING)
        self.assertEqual(
            task.group_qa_json["stage_d"]["decision"],
            "auto_released_for_render",
        )
        self.assertEqual(
            task.group_qa_json["stage_d"]["approval_mode"], "operator_auto"
        )
        self.assertEqual(self.repo.feedback[0].feedback_type, "automatic_technical_gate")
        self.assertEqual(
            self.repo.feedback[0].reason_codes_json,
            ["operator_owned_source_auto_render"],
        )
        with self.assertRaises(VideoRenderFlowError):
            self.flow.approve_group("opv_task_1", reviewer="老板")  # not image_review

    def test_render_happy_path_reaches_video_review(self) -> None:
        self.flow.approve_group("opv_task_1", reviewer="老板")
        row = self.flow.render("opv_task_1")
        self.assertEqual(row.render_status, RENDER_COMPLETED)
        self.assertTrue(row.publish_ready)
        self.assertEqual(row.qc_status, "passed")
        self.assertEqual(row.duration_ms, 12500)
        self.assertEqual(row.render_preset_id, "RP_STILL_VERTICAL_12S_V1")
        self.assertEqual(len(row.shot_selection_json), 5)
        self.assertEqual(len(row.timeline_json), 5)
        self.assertEqual(row.copy_snapshot_json["title"], "t")
        self.assertIn("video_review", [t for _, t in self.repo.transitions])
        task = self.repo.tasks["opv_task_1"]
        self.assertEqual(task.task_status, TASK_VIDEO_REVIEW)
        render_cmds = [
            c for c in self.runner.commands
            if Path(c[0]).name == "ffmpeg" and "blackdetect" not in " ".join(c)
        ]
        self.assertEqual(len(render_cmds), 1)
        self.assertIn("-an", render_cmds[0])

    def test_render_failure_then_requeue_reuses_row(self) -> None:
        self.flow.approve_group("opv_task_1", reviewer="老板")
        self.runner.ffmpeg_rc = 1
        row = self.flow.render("opv_task_1")
        self.assertEqual(row.render_status, RENDER_FAILED)
        self.assertIn("ffmpeg exit 1", row.output_metadata_json["error"])
        # task stays in rendering after a failed render attempt
        self.assertEqual(self.repo.tasks["opv_task_1"].task_status, TASK_RENDERING)

        self.runner.ffmpeg_rc = 0
        row = self.flow.render("opv_task_1")
        self.assertEqual(row.render_status, RENDER_COMPLETED)
        self.assertEqual(row.render_version, 1)  # same row reused, not duplicated
        renders = self.repo.list_renders("opv_task_1")
        self.assertEqual(len(renders), 1)

    def test_render_requires_approved_group(self) -> None:
        with self.assertRaises(VideoRenderFlowError):
            self.flow.render("opv_task_1")

    def test_render_uses_snapshotted_plan_preset(self) -> None:
        self.task.plan_json["render_contract"] = {
            "preset_id": "RP_RECIPE_V1",
            "fps": 30,
        }
        self.flow.approve_group("opv_task_1", reviewer="老板")
        row = self.flow.render("opv_task_1")
        self.assertEqual(row.render_preset_id, "RP_RECIPE_V1")

    def test_overlay_rerender_creates_v2_and_ass_filter(self) -> None:
        self.task.recipe_id = "RECIPE_SCENE_SOLUTION_V1"
        self.flow.approve_group("opv_task_1", reviewer="老板")
        first = self.flow.render("opv_task_1")
        self.assertEqual(first.render_version, 1)
        self.repo.transition_task(
            "opv_task_1", TASK_VIDEO_REVIEW, TASK_RENDERING
        )
        second = self.flow.render(
            "opv_task_1", overlay_profile_id="OVERLAY_LIGHT_V1"
        )
        self.assertEqual(second.render_version, 2)
        render_commands = [
            command for command in self.runner.commands
            if Path(command[0]).name == "ffmpeg" and "blackdetect" not in " ".join(command)
        ]
        self.assertIn("ass=filename=", " ".join(render_commands[-1]))


if __name__ == "__main__":
    unittest.main()
