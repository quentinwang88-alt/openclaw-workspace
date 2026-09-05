from __future__ import annotations

import copy
from dataclasses import asdict
from datetime import datetime, timedelta
import fcntl
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from domain.models import ProductionBatch, TaskRevision
from services.feishu_workflow import FeishuTaskWorkflow, PresetTask
from services.production_batch import BatchLease
from services.workflow_v2 import canonical_hash
from test_feishu_v2 import Repo, Client, Renderer, DecodableGenerator
from test_hero_first import ScriptedGenerator, build_task
from test_rds_repository import FakeConnection
from repositories.rds_repository import RdsRepository


class BatchRepo(Repo):
    def __init__(self, batch):
        super().__init__()
        self.batch = batch

    def get_production_batch(self, record_id):
        return self.batch if self.batch.source_record_id == record_id else None

    def claim_batch_run(self, batch_id, *, owner, lease_seconds):
        if self.batch.run_owner and self.batch.lease_until > datetime.utcnow():
            return False
        self.batch.run_owner = owner
        self.batch.lease_until = datetime.utcnow() + timedelta(seconds=lease_seconds)
        self.batch.batch_status = "running"
        return True

    def heartbeat_batch_run(self, batch_id, *, owner, lease_seconds):
        return self.batch.run_owner == owner

    def finish_batch_run(self, batch_id, *, owner, status):
        if owner == self.batch.run_owner:
            self.batch.run_owner, self.batch.lease_until = None, None
            self.batch.batch_status = status

    def queue_batch_projection(self, batch_id, fields):
        self.batch.pending_fields_json = copy.deepcopy(fields)

    def acknowledge_batch_projection(self, batch_id, fields):
        if self.batch.pending_fields_json == fields:
            self.batch.pending_fields_json = None


class ProductionBatchTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.spec = PresetTask("acct", "TH", "th-TH", "recipe", "theme", "hook", "persona", "look", "scene")
        self.batch = ProductionBatch("batch", "rec", 3, {
            "workflow_version": 2, "product_id": "product", "preset": "frozen",
            "entries": [{"spec": asdict(self.spec), "product": {"reference_pack_id": f"pack{i}"}} for i in range(3)],
        })
        self.repo, self.client = BatchRepo(self.batch), Client()
        self.generator = DecodableGenerator(self.root, {})
        self.workflow = FeishuTaskWorkflow(self.repo, self.client, generator=self.generator,
            renderer=Renderer(), output_root=self.root, asset_readiness_gate=lambda *_: None)
        self.workflow.catalog.overlay_profile_id = ""  # this fixture has no recipe overlay contract
        self.client.fields = {"执行": True, "生成数量": 9, "产品编码": "changed", "生产预设": "changed"}

    def create(self, repository, **kwargs):
        index = int(kwargs["source_record_id_prefix"].split(":")[1])
        task = build_task()
        task.task_id = f"task{index}"
        task.source_record_id = kwargs["source_record_id_prefix"] + ":1"
        task.product_snapshot_json = {"product": kwargs["product_snapshot"]}
        task.workflow_version = 1
        self.repo.tasks[task.task_id] = task
        return [{"task_id": task.task_id}]

    def test_partial_creation_resumes_exact_frozen_three_not_existing_subset(self):
        attempts = []
        def crashing(repository, **kwargs):
            attempts.append(kwargs["source_record_id_prefix"])
            if len(attempts) == 2:
                raise RuntimeError("creation interrupted")
            return self.create(repository, **kwargs)
        with patch("services.feishu_workflow.generate_product_image_story", side_effect=crashing):
            first = self.workflow.scan(record_id="rec")
        self.assertEqual(len(first["errors"]), 1)
        self.assertEqual(len(self.repo.tasks), 1)
        self.assertEqual(self.generator.calls, [])
        self.client.fields["执行"] = True
        with patch("services.feishu_workflow.generate_product_image_story", side_effect=self.create) as creator:
            second = self.workflow.scan(record_id="rec")
        self.assertEqual(second["errors"], [])
        self.assertEqual(creator.call_count, 2)
        self.assertEqual(len(self.repo.tasks), 3)
        self.assertTrue(all(t.workflow_version == 2 and t.active_revision_id == t.released_revision_id for t in self.repo.tasks.values()))
        self.assertEqual([t.product_snapshot_json["product"]["reference_pack_id"] for t in self.repo.tasks.values()], ["pack0", "pack1", "pack2"])
        self.assertEqual(len(self.generator.calls), 15)

    def test_incomplete_batch_cannot_review_or_publish(self):
        self.create(self.repo, source_record_id_prefix="rec:1:recipe", product_snapshot={})
        with self.assertRaisesRegex(RuntimeError, "1/3"):
            self.workflow._assert_batch_complete("rec", list(self.repo.tasks.values()))

    def test_projection_is_replayed_without_generation(self):
        self.client.fields = {}
        self.batch.pending_fields_json = {"审核阶段": "组图审核", "进度": "待审核", "审核": "待审核"}
        result = self.workflow.scan(record_id="rec")
        self.assertEqual(result["processed"][0]["action"], "recover_projection")
        self.assertEqual(self.client.fields["审核阶段"], "组图审核")
        self.assertIsNone(self.batch.pending_fields_json)
        self.assertEqual(self.generator.calls, [])

    def test_expired_run_never_resubmits_media(self):
        self.client.fields = {"进度": "生成中"}
        self.batch.batch_status = "running"
        self.batch.run_owner = "dead"
        self.batch.lease_until = datetime.utcnow() - timedelta(seconds=1)
        self.workflow.scan(record_id="rec")
        self.assertEqual(self.client.fields["进度"], "需处理")
        self.assertEqual(self.generator.calls, [])

    def test_lease_excludes_second_worker_and_is_owner_fenced(self):
        first = BatchLease(self.repo, "batch")
        try:
            with self.assertRaisesRegex(RuntimeError, "租约"):
                BatchLease(self.repo, "batch")
            self.repo.finish_batch_run("batch", owner="stale", status="waiting")
            self.assertEqual(self.batch.run_owner, first.owner)
        finally:
            first.close()
        self.assertIsNone(self.batch.run_owner)

    def test_busy_scanner_does_not_consume_operator_command(self):
        first = BatchLease(self.repo, "batch")
        try:
            before = copy.deepcopy(self.client.fields)
            result = self.workflow.scan(record_id="rec")
            self.assertEqual(result["processed"][0]["action"], "leased_skip")
            self.assertEqual(self.client.fields, before)
            self.assertEqual(self.client.updates, [])
        finally:
            first.close()

    def test_failed_feishu_write_preserves_success_projection_for_replay(self):
        desired = {"进度": "已完成", "审核阶段": "已验收"}
        with patch.object(self.client, "update_record_fields", side_effect=ConnectionError("offline")):
            with self.assertRaisesRegex(RuntimeError, "投影等待重放"):
                self.workflow._write_fields("rec", desired)
        self.assertEqual(self.batch.pending_fields_json, desired)
        self.client.fields = {}
        self.workflow.scan(record_id="rec")
        self.assertEqual(self.client.fields, desired)

    def test_release_rejects_selection_changed_after_review(self):
        conn = FakeConnection([("rows", [{"active_revision_id": "revision", "row_version": 1}]),
                               ("rows", [{"selection_hash": "new", "input_snapshot_hash": "input"}])])
        with self.assertRaisesRegex(RuntimeError, "inputs changed"):
            RdsRepository(lambda: conn).release_revision("task", "revision", expected_task_row_version=1,
                render_id="render", review_id="review", expected_selection_hash="old", expected_input_snapshot_hash="input")
        self.assertEqual(conn.commits, 0)
        self.assertEqual(conn.rollbacks, 1)

    def test_batch_row_roundtrip(self):
        restored = ProductionBatch.from_row(self.batch.to_row())
        self.assertEqual(restored.manifest_json, self.batch.manifest_json)

    def test_rework_revision_activation_rolls_back_pointer_and_status_together(self):
        revision = TaskRevision("child", "task", 2, {}, canonical_hash({}), parent_revision_id="parent")
        conn = FakeConnection([("rows", [{"row_version": 1, "active_revision_id": "parent", "task_status": "video_review"}]),
                               ("rowcount", 1), ("raise", RuntimeError("commit interruption"))])
        with self.assertRaisesRegex(RuntimeError, "interruption"):
            RdsRepository(lambda: conn).create_revision_and_activate(revision, expected_task_row_version=1, transition_to="rework_pending")
        self.assertEqual(conn.commits, 0)
        self.assertEqual(conn.rollbacks, 1)
        update = conn.statements[-1][0]
        self.assertIn("active_revision_id", update)
        self.assertIn("task_status", update)
        self.assertIn("plan_json", update)


class ScannerLockTest(unittest.TestCase):
    def test_contention_skips_and_stale_metadata_does_not_block(self):
        path = Path(__file__).parents[1] / "scripts/run_feishu_scanner_lock.py"
        spec = importlib.util.spec_from_file_location("scanner_lock", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as root:
            lock_path = Path(root) / "scanner.lock"
            with lock_path.open("w+") as owned:
                fcntl.flock(owned, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with patch.object(module.os, "execv") as execute:
                    self.assertEqual(module.run(["/bin/true"], lock_path=str(lock_path)), 0)
                    execute.assert_not_called()
            # The metadata file remains, but kernel ownership disappeared.
            def mocked_exec(*_):
                raise RuntimeError("exec reached")
            # Capture/close fd because a real exec owns it, unlike this mock.
            opened = []
            real_open = module.os.open
            def capture(*args):
                fd = real_open(*args)
                opened.append(fd)
                return fd
            with patch.object(module.os, "open", side_effect=capture), patch.object(module.os, "execv", side_effect=mocked_exec):
                try:
                    with self.assertRaisesRegex(RuntimeError, "exec reached"):
                        module.run(["/bin/true"], lock_path=str(lock_path))
                finally:
                    for fd in opened:
                        module.os.close(fd)
