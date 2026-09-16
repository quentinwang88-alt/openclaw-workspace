"""One operator entry point owns both long-form sources.

The shared production-script pool feeds the long-form runner.  An original row
already owns its Plan C job; a remake row is compiled into one here and then
follows the identical keyframes/H3/resume/TTS/merge/validation/write-back path.
These tests pin the operator-visible contract:

* a dry run never mutates Feishu, the schema or the state database;
* a remake row without a job id gets one in the same round and keeps running;
* ``WAITING_REMOTE`` keeps the checkbox, ``FINAL_READY`` uploads and clears it;
* a failure keeps the checkbox and records the reason;
* an existing paid submission is never overwritten or re-submitted;
* original rows keep their previous behaviour exactly.
"""

from __future__ import annotations

import io
import json
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from core.longform.audio import _binary
from core.longform.storage import SOURCE_KIND_REMAKE, LongformStorage

from scripts import run_feishu_longform_production_tasks as runner


F = runner.PRODUCTION_SCRIPT_FIELD_NAMES

PROMPT_30 = """【0-10 秒】
开场正面展示外套。
口播/短句：这是开头。

【10-20 秒】
侧身展示细节。
口播/短句：这是中段。

【20-30 秒】
转身收尾。
口播/短句：这是结尾。"""

PROMPT_60 = "\n\n".join(
    f"【{index}-{index + 10} 秒】\n动作{index}。\n口播/短句：第{index}句。"
    for index in range(0, 60, 10)
)


class FakeRecord:
    def __init__(self, record_id: str, fields: dict):
        self.record_id = record_id
        self.fields = dict(fields)


class FakeClient:
    """Records every write so a dry run can be proven read-only."""

    def __init__(self, records):
        self.records = list(records)
        self.field_updates: list[tuple] = []
        self.upload_calls: list[dict] = []
        self.download_calls: list[str] = []

    def list_records(self, page_size=500):
        return list(self.records)

    def update_record_fields(self, record_id, fields):
        self.field_updates.append((str(record_id), dict(fields)))
        return {"record_id": record_id}

    def download_attachment(self, attachment, target_prefix):
        token = str(attachment.get("file_token") or "token")
        self.download_calls.append(token)
        target = Path(f"{target_prefix}_{token}.png")
        target.parent.mkdir(parents=True, exist_ok=True)
        # Distinct bytes per token keep role de-duplication observable.
        target.write_bytes(b"\x89PNG\r\n\x1a\n" + (token * 40).encode("utf-8"))
        return str(target)

    def upload_attachment(self, **kwargs):
        self.upload_calls.append(dict(kwargs))
        return {"file_token": f"token-{len(self.upload_calls)}"}

    def written_fields(self, record_id: str) -> list[dict]:
        return [fields for rid, fields in self.field_updates if rid == record_id]


def remake_fields(**overrides) -> dict:
    fields = {
        "脚本ID": "vs_remake_1",
        "产品编码": "P_REMAKE",
        "脚本来源": "视频复刻",
        "视频时长": 30,
        "视频形态（系统）": "分段视频",
        "视频生成提示词": PROMPT_30,
        "口播_目标语言": "这是开头。这是中段。这是结尾。",
        "目标国家": "TH",
        "目标语言": "泰语",
        "发布用途": "养号",
        "是否挂车": "否",
        "进入生产": True,
        "处理状态": "待生产",
        "产品图片": [{"file_token": "product-1"}],
    }
    fields.update(overrides)
    return fields


def original_fields(**overrides) -> dict:
    fields = {
        "脚本ID": "vs_original_1",
        "产品编码": "P_ORIGINAL",
        "脚本来源": "原创生成",
        "视频时长": 40,
        "视频形态（系统）": "长视频",
        "进入生产": True,
        "长视频任务ID（系统）": "LFJ_ORIGINAL_1",
        "处理状态": "待生产",
    }
    fields.update(overrides)
    return fields


def short_fields(**overrides) -> dict:
    fields = {
        "脚本ID": "vs_short_1",
        "产品编码": "P_SHORT",
        "脚本来源": "视频复刻",
        "视频时长": 10,
        "视频形态（系统）": "15秒原创",
        "进入生产": True,
        "产品图片": [{"file_token": "product-short"}],
    }
    fields.update(overrides)
    return fields


def _make_video(path: Path, seconds: float = 2.0) -> Path:
    subprocess.run(
        [
            _binary("ffmpeg"), "-v", "error", "-y", "-f", "lavfi", "-i",
            f"testsrc2=size=180x320:rate=24:duration={seconds}",
            "-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=44100",
            "-shortest", "-c:v", "libx264", "-preset", "veryfast", "-crf", "26",
            "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "96k", str(path),
        ],
        check=True, capture_output=True,
    )
    return path


def _run_main(argv, client, storage, *, run_result=None, run_error=None):
    """Run ``main()`` against a fake Feishu client and a real state database."""

    stdout, stderr = io.StringIO(), io.StringIO()
    patches = {
        "argv": mock.patch.object(sys, "argv", ["runner", *argv]),
        "client": mock.patch.object(runner, "_client", return_value=client),
        "ensure_fields": mock.patch.object(runner, "ensure_fields"),
        "storage": mock.patch.object(runner, "LongformStorage", lambda *a, **k: storage),
        "lock": mock.patch.object(runner, "_lock", return_value=mock.MagicMock()),
        "run": mock.patch.object(
            runner, "run_longform_job_to_final",
            **({"side_effect": run_error} if run_error else {"return_value": run_result}),
        ),
        "publish": mock.patch.object(
            runner, "enqueue_longform_final",
            return_value={"canonical_script_key": "P_REMAKE::x"},
        ),
    }
    mocks = {name: patch.start() for name, patch in patches.items()}
    try:
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = runner.main()
    finally:
        for patch in reversed(list(patches.values())):
            patch.stop()
    return SimpleNamespace(
        code=code, out=stdout.getvalue(), err=stderr.getvalue(),
        ensure_fields=mocks["ensure_fields"], lock=mocks["lock"],
        run=mocks["run"], publish=mocks["publish"],
    )


def _formal_argv(asset_root: Path) -> list:
    return [
        "--allow-real-submit", "--allow-external-tts", "--limit", "5",
        "--asset-root", str(asset_root),
    ]


def _jobs(storage: LongformStorage) -> list[dict]:
    with storage.connect() as conn:
        return [dict(row) for row in conn.execute("SELECT * FROM longform_job")]


class DryRunTests(unittest.TestCase):
    def test_dry_run_lists_both_sources_and_writes_nothing(self):
        client = FakeClient([
            FakeRecord("rec_remake", remake_fields()),
            FakeRecord("rec_original", original_fields()),
            FakeRecord("rec_short", short_fields()),
            FakeRecord("rec_remake_short", remake_fields(视频时长=10)),
        ])
        storage = mock.MagicMock()
        result = _run_main(["--dry-run", "--limit", "5"], client, storage)

        self.assertEqual(result.code, 0)
        self.assertIn("待执行长视频生产/发布脚本: 2", result.out)
        self.assertIn("rec_remake", result.out)
        self.assertIn("(待创建)", result.out)
        self.assertIn("rec_original", result.out)
        self.assertNotIn("rec_short", result.out)
        self.assertNotIn("rec_remake_short", result.out)
        # A read-only check must not touch the schema, the table or the state db.
        result.ensure_fields.assert_not_called()
        result.lock.assert_not_called()
        storage.ensure_schema.assert_not_called()
        self.assertEqual([], client.field_updates)
        self.assertEqual([], client.upload_calls)
        self.assertEqual([], client.download_calls)

    def test_formal_run_requires_both_paid_authorizations(self):
        client = FakeClient([])
        storage = mock.MagicMock()
        for argv in ([], ["--allow-real-submit"], ["--allow-external-tts"]):
            with self.subTest(argv=argv):
                with self.assertRaises(SystemExit) as raised:
                    _run_main(argv, client, storage)
                self.assertEqual(raised.exception.code, 2)
        self.assertEqual([], client.field_updates)
        storage.ensure_schema.assert_not_called()


class RemakeProductionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.storage = LongformStorage(self.root / "longform.sqlite3")
        self.asset_root = self.root / "assets"

    def _seed_frozen_job(self, job_id: str, *, record_id: str, revision: str):
        """Seed a job whose source identity matches the current remake row."""

        self.storage.ensure_schema()
        self.storage.save_plan(
            job_id,
            {"product_code": "P_REMAKE", "source_kind": SOURCE_KIND_REMAKE,
             "source_record_id": record_id},
            {"segments": [
                {"segment_id": "A", "duration_seconds": 15,
                 "generation_mode": "first_last", "video_prompt": "片段A"},
                {"segment_id": "B", "duration_seconds": 15,
                 "generation_mode": "first_frame", "video_prompt": "片段B"},
            ], "target_duration_seconds": 30},
            {"K0": {"role": "MASTER_FIRST_FRAME", "prompt": "x"}},
            source_kind=SOURCE_KIND_REMAKE, source_record_id=record_id,
            source_revision_hash=revision,
        )

    @staticmethod
    def _revision(record_id: str = "rec_remake", **overrides) -> str:
        return runner.freeze_record(
            record_id, remake_fields(**overrides), {},
        ).source_revision_hash

    def test_remake_row_without_job_id_gets_one_and_continues_in_the_same_round(self):
        client = FakeClient([FakeRecord("rec_remake", remake_fields())])
        result = _run_main(
            _formal_argv(self.asset_root), client, self.storage,
            run_result={"status": "WAITING_REMOTE", "message": "远端生成中"},
        )
        self.assertEqual(result.code, 0)

        jobs = _jobs(self.storage)
        self.assertEqual(1, len(jobs))
        job = jobs[0]
        self.assertEqual(job["source_kind"], SOURCE_KIND_REMAKE)
        self.assertEqual(job["source_record_id"], "rec_remake")
        self.assertTrue(str(job["job_id"]).startswith("LFR_"))
        self.assertEqual(job["status"], "PLANNED")
        self.assertIsNotNone(self.storage.find_job_by_source("rec_remake", job["source_revision_hash"]))

        # The frozen remake was compiled into a Plan C plan, never re-authored.
        plan = json.loads(job["plan_json"])
        self.assertEqual(plan["source_kind"], SOURCE_KIND_REMAKE)
        self.assertEqual([item["segment_id"] for item in plan["segments"]], ["A", "B"])
        self.assertEqual(plan["audio_contract"]["mode"], "PRESERVE_SOURCE_COPY")
        self.assertFalse(plan["audio_contract"]["rewrite_allowed"])
        self.assertEqual(
            [item["segment_id"] for item in
             self.storage.get_job(job["job_id"])["segments"]],
            ["A", "B"],
        )

        writes = client.written_fields("rec_remake")
        # 1) the job id is written back at creation time...
        self.assertEqual(writes[0][F["longform_job_id"]], job["job_id"])
        # 2) ...and the same row continues straight into the shared executor.
        self.assertEqual(job["job_id"], result.run.call_args.args[0])
        self.assertEqual(str(self.asset_root), result.run.call_args.kwargs["asset_root"])
        self.assertIn("长视频任务ID（系统）", writes[0])
        self.assertTrue(any(f.get(F["longform_status"]) == "生成中" for f in writes))
        final = writes[-1]
        self.assertEqual(final["运行任务ID"], job["job_id"])
        self.assertEqual(final["处理状态"], "已送生产")
        self.assertIn("复刻长视频H3远端生成中", final["同步结果"])
        # WAITING_REMOTE must keep the operator checkbox so the next round resumes.
        for fields in writes:
            self.assertNotIn(F["production_enabled"], fields)
        self.assertEqual(["product-1"], client.download_calls)

    def test_final_ready_uploads_master_and_clears_the_checkboxes(self):
        job_id = "LFR_FINAL"
        revision = self._revision()
        self._seed_frozen_job(job_id, record_id="rec_remake", revision=revision)
        video = _make_video(self.root / "final_video.mp4")
        k0 = self.root / "K0.png"
        k0.write_bytes(b"\x89PNG\r\n\x1a\n" + b"k0" * 64)
        self.storage.update_segment(job_id, "A", start_frame_path=str(k0))

        client = FakeClient([FakeRecord(
            "rec_remake", remake_fields(**{"长视频任务ID（系统）": job_id, "确认发布": True}),
        )])
        result = _run_main(
            _formal_argv(self.asset_root), client, self.storage,
            run_result={"status": "FINAL_READY", "final_video_path": str(video)},
        )
        self.assertEqual(result.code, 0)
        self.assertTrue(result.publish.called)

        final = client.written_fields("rec_remake")[-1]
        self.assertEqual(final[F["longform_status"]], "已完成")
        # The K0 projection is uploaded first, then the validated master.
        self.assertEqual(final[F["longform_first_frame"]][0]["file_token"], "token-1")
        self.assertEqual(final[F["longform_video"]][0]["file_token"], "token-2")
        self.assertEqual(final[F["longform_error"]], "")
        self.assertEqual(final[F["publish_confirmed"]], False)
        self.assertEqual(final[F["production_enabled"]], False)
        self.assertEqual(final["处理状态"], "已送生产")
        self.assertEqual(final["运行任务ID"], job_id)
        self.assertIn("复刻长视频完整生产已完成", final["同步结果"])
        self.assertIn("已进入主发布队列待排期", final["同步结果"])
        # The uploaded master is the validated artifact, not an empty shell.
        video_uploads = [
            call for call in client.upload_calls if call.get("content_type") == "video/mp4"
        ]
        self.assertEqual(1, len(video_uploads))
        self.assertGreater(video_uploads[0]["size"], 10_240)
        # A reused frozen job must not be rebuilt or re-downloaded.
        self.assertEqual(1, len(_jobs(self.storage)))
        self.assertEqual([], client.download_calls)

    def test_failure_keeps_the_checkbox_and_records_the_reason(self):
        client = FakeClient([FakeRecord("rec_remake", remake_fields())])
        result = _run_main(
            _formal_argv(self.asset_root), client, self.storage,
            run_error=RuntimeError("H3 片段A 提交失败"),
        )
        self.assertEqual(result.code, 1)
        final = client.written_fields("rec_remake")[-1]
        self.assertEqual(final[F["longform_status"]], "生成失败")
        self.assertEqual(final["处理状态"], "同步失败")
        self.assertIn("H3 片段A 提交失败", final[F["longform_error"]])
        self.assertIn("复刻长视频生成失败", final["同步结果"])
        for fields in client.written_fields("rec_remake"):
            self.assertNotIn(F["production_enabled"], fields)

    def test_blocked_remake_fails_before_any_paid_call(self):
        client = FakeClient([FakeRecord(
            "rec_remake",
            remake_fields(**{"视频时长": 60, "视频生成提示词": PROMPT_60}),
        )])
        result = _run_main(
            _formal_argv(self.asset_root), client, self.storage,
            run_result={"status": "FINAL_READY"},
        )
        self.assertEqual(result.code, 1)
        # No Plan C job and no executor entry: a 60s source is blocked, never cut.
        self.assertEqual([], _jobs(self.storage))
        result.run.assert_not_called()
        final = client.written_fields("rec_remake")[-1]
        self.assertEqual(final[F["longform_status"]], "生成失败")
        self.assertEqual(final["处理状态"], "同步失败")
        self.assertIn("REMAKE_SEGMENT_COUNT_UNSUPPORTED", final[F["longform_error"]])
        for fields in client.written_fields("rec_remake"):
            self.assertNotIn(F["production_enabled"], fields)


class ResubmitGuardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.storage = LongformStorage(self.root / "longform.sqlite3")
        self.storage.ensure_schema()
        self.asset_root = self.root / "assets"

    def _ensure(self, fields: dict):
        client = FakeClient([FakeRecord("rec_remake", fields)])
        job_id, error = runner._ensure_remake_job(
            client, self.storage, FakeRecord("rec_remake", fields),
            asset_root=str(self.asset_root),
        )
        return job_id, error, client

    def _seed_stale_job(self, *, revision: str, status: str = "SUBMITTED",
                        task_id: str = "remote-1"):
        self.storage.save_plan(
            "LFR_STALE",
            {"product_code": "P_REMAKE", "source_kind": SOURCE_KIND_REMAKE},
            {"segments": [{"segment_id": "A", "duration_seconds": 15,
                           "generation_mode": "first_frame", "video_prompt": "片段A"}],
             "target_duration_seconds": 15},
            {},
            source_kind=SOURCE_KIND_REMAKE, source_record_id="rec_remake",
            source_revision_hash=revision,
        )
        self.storage.update_segment(
            "LFR_STALE", "A", status=status, platform_task_id=task_id,
        )

    def test_source_changed_after_paid_submit_is_blocked_not_replaced(self):
        self._seed_stale_job(revision="an-older-revision")
        job_id, error, client = self._ensure(
            remake_fields(**{"长视频任务ID（系统）": "LFR_STALE"}),
        )
        self.assertEqual("", job_id)
        self.assertIn("SOURCE_CHANGED_AFTER_SUBMIT", error)
        # The paid job survives untouched and nothing new is created or frozen.
        self.assertEqual(["LFR_STALE"], [job["job_id"] for job in _jobs(self.storage)])
        self.assertEqual([], client.download_calls)
        self.assertEqual([], [f for _rid, f in client.field_updates if F["longform_job_id"] in f])

    def test_unpaid_stale_revision_is_safely_recompiled(self):
        self._seed_stale_job(revision="an-older-revision", status="PLANNED", task_id="")
        job_id, error, client = self._ensure(
            remake_fields(**{"长视频任务ID（系统）": "LFR_STALE"}),
        )
        self.assertEqual("", error)
        self.assertTrue(job_id.startswith("LFR_"))
        self.assertNotEqual("LFR_STALE", job_id)
        self.assertIn(job_id, [job["job_id"] for job in _jobs(self.storage)])
        self.assertIn(
            job_id,
            [f[F["longform_job_id"]] for _rid, f in client.field_updates
             if F["longform_job_id"] in f],
        )
        self.assertEqual(["product-1"], client.download_calls)

    def test_staging_directory_and_saved_job_share_one_id(self):
        """The attachment staging path and the compiled job must never diverge."""

        fields = remake_fields()
        handoff = runner.build_plan_c_handoff(
            record_id="rec_remake", fields=fields,
            frozen_assets={"assets": [{"role": "PRODUCT_REFERENCE",
                                       "local_path": "/tmp/p.png", "sha256": "a" * 64}]},
        )
        self.assertEqual(
            handoff["job_id"],
            runner.remake_job_id("rec_remake", handoff["source_revision_hash"]),
        )
        created_id, error, _client = self._ensure(fields)
        self.assertEqual("", error)
        self.assertEqual(handoff["job_id"], created_id)
        self.assertEqual(
            created_id, _jobs(self.storage)[0]["job_id"],
        )

    def test_existing_frozen_job_is_reused_without_redownloading(self):
        first_id, error, first = self._ensure(remake_fields())
        self.assertEqual("", error)
        self.assertEqual(["product-1"], first.download_calls)
        revision = _jobs(self.storage)[0]["source_revision_hash"]

        # A second round of the same frozen revision must reuse the job as-is.
        fields = remake_fields(**{"长视频任务ID（系统）": first_id})
        reused_id, error, second = self._ensure(fields)
        self.assertEqual(first_id, reused_id)
        self.assertEqual("", error)
        self.assertEqual([], second.download_calls)
        self.assertEqual([], second.field_updates)
        self.assertEqual(1, len(_jobs(self.storage)))

        # If the recorded id drifted, only the pointer is repaired — nothing else.
        drifted_id, error, third = self._ensure(remake_fields(**{
            "长视频任务ID（系统）": "LFR_SOMETHING_ELSE",
        }))
        self.assertEqual(first_id, drifted_id)
        self.assertEqual("", error)
        self.assertEqual([], third.download_calls)
        self.assertEqual(
            [{F["longform_job_id"]: first_id}], [f for _rid, f in third.field_updates],
        )
        self.assertEqual(1, len(_jobs(self.storage)))
        self.assertEqual(revision, _jobs(self.storage)[0]["source_revision_hash"])


class OriginalRowRegressionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.storage = LongformStorage(self.root / "longform.sqlite3")
        self.storage.ensure_schema()

    def test_original_row_keeps_its_own_job_and_never_compiles_a_remake(self):
        client = FakeClient([FakeRecord("rec_original", original_fields())])
        result = _run_main(
            _formal_argv(self.root / "assets"), client, self.storage,
            run_result={"status": "WAITING_REMOTE", "message": "远端生成中"},
        )
        self.assertEqual(result.code, 0)
        self.assertEqual("LFJ_ORIGINAL_1", result.run.call_args.args[0])
        # An original row downloads no remake attachments and creates no job.
        self.assertEqual([], client.download_calls)
        self.assertEqual([], _jobs(self.storage))
        writes = client.written_fields("rec_original")
        final = writes[-1]
        self.assertIn("长视频H3远端生成中，下一轮自动续跑", final["同步结果"])
        self.assertEqual(final["运行任务ID"], "LFJ_ORIGINAL_1")
        self.assertNotIn(F["production_enabled"], final)

    def test_original_publish_only_queues_without_generating(self):
        fields = original_fields(**{
            "进入生产": False, "长视频执行状态（系统）": "已完成", "确认发布": True,
        })
        client = FakeClient([FakeRecord("rec_original", fields)])
        result = _run_main(_formal_argv(self.root / "assets"), client, self.storage)
        self.assertEqual(result.code, 0)
        result.run.assert_not_called()
        self.assertTrue(result.publish.called)
        final = client.written_fields("rec_original")[-1]
        self.assertEqual(final[F["publish_confirmed"]], False)
        self.assertEqual(final["运行任务ID"], "LFJ_ORIGINAL_1")
        self.assertIn("长视频成片已进入主发布队列待排期", final["同步结果"])


if __name__ == "__main__":
    unittest.main()
