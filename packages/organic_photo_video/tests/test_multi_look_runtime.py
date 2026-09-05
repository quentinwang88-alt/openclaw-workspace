"""Variable-page release contracts; local fakes only, no model or database."""
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

PACKAGE_ROOT = Path(__file__).resolve().parent.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from domain import contracts
from services.video_renderer import build_timeline, build_filter_graph
from services.video_render_flow import VideoRenderFlow, VideoRenderFlowError
from services.workflow_v2 import AnchorReviewService, RevisionService
from services.technical_production import TechnicalProductionFlow
from services.release_gate import expected_render_fingerprint, ReleaseGateError
from test_contracts import plan_payload
from test_workflow_v2 import MemoryRepo


def multi_plan(count):
    from services.multi_look_planner import multi_look_durations
    plan = plan_payload()
    plan["recipe"] = {"id": "RECIPE_MULTI_LOOK_V1", "content_goal": "multi_look"}
    plan["actual_shot_count"] = count
    plan["shots"] = plan["shots"][:count]
    for shot, duration in zip(plan["shots"], multi_look_durations(count)):
        shot.update(duration_ms=duration, transition_out="cut")
    return plan


class MultiLookRuntimeTest(unittest.TestCase):
    def test_only_multi_look_allows_one_to_five_and_six_second_single_page(self):
        for count in range(1, 6):
            plan = multi_plan(count)
            self.assertEqual(contracts.validate_plan_json(plan), [])
            self.assertEqual(contracts.expected_plan_shot_count(plan), count)
        legacy = multi_plan(1)
        legacy.pop("recipe")
        self.assertTrue(contracts.validate_plan_json(legacy))
        self.assertEqual(contracts.validate_shot_payload(multi_plan(1)["shots"][0]), [])

    def test_count_missing_mismatched_duplicate_or_reordered_rejected(self):
        for value in (None, 0, 6, True, "3", 2):
            plan = multi_plan(3)
            plan["actual_shot_count"] = value
            self.assertTrue(contracts.validate_plan_json(plan))
        for indices in ([1, 1, 3], [1, 3, 2], [2, 3, 4]):
            plan = multi_plan(3)
            for shot, index in zip(plan["shots"], indices):
                shot["slot_index"] = index
            self.assertTrue(contracts.validate_plan_json(plan))
        plan = multi_plan(2)
        plan["shots"][0]["duration_ms"] = 6000
        self.assertTrue(contracts.validate_plan_json(plan))

    def test_all_timeline_counts_remain_six_seconds(self):
        for count in range(1, 6):
            plan = multi_plan(count)
            rows = {i: SimpleNamespace(shot_id=f"s{i}", shot_version=1) for i in range(1, count + 1)}
            timeline = build_timeline(plan["shots"], rows, {i: f"/tmp/p{i}.png" for i in rows})
            self.assertEqual(timeline[-1].start_ms + timeline[-1].effective_ms, 6000)
            graph = build_filter_graph(timeline)
            self.assertIn("[vout]", graph)
            if count == 1:
                self.assertIn("[v0]copy[vout]", graph)

    def test_anchor_selection_does_not_select_same_slot_final_board(self):
        repo = MemoryRepo()
        repo.task.plan_json.update(multi_plan(3))
        service = RevisionService(repo)
        revision = service.ensure_working(repo.task)
        revision = service.register_candidate(revision, {
            "asset_id": "anchor_photo", "asset_type": "shot", "slot_index": 1,
            "path": "/tmp/photo.png", "sha256": "a" * 64,
        })
        result = AnchorReviewService(repo).record("task", asset_id="anchor_photo", decision="passed",
            dimensions={}, reviewer_type="human", reviewer="qa")
        self.assertEqual(result.revision.asset_manifest_json["selected"], {"anchor": "anchor_photo"})
        # Legacy workflow still selects the continuity photo as its final slot.
        legacy = MemoryRepo()
        manager = RevisionService(legacy)
        revision = manager.register_candidate(manager.ensure_working(legacy.task), {
            "asset_id": "legacy_photo", "asset_type": "shot", "slot_index": 1,
            "path": "/tmp/photo.png", "sha256": "b" * 64,
        })
        result = AnchorReviewService(legacy).record("task", asset_id="legacy_photo", decision="passed",
            dimensions={}, reviewer_type="human", reviewer="qa")
        self.assertEqual(result.revision.asset_manifest_json["selected"]["shot:1"], "legacy_photo")

    def test_v2_content_gate_accepts_only_all_frozen_slots(self):
        flow = VideoRenderFlow(SimpleNamespace(), None)
        for count in range(1, 6):
            task = SimpleNamespace(plan_json=multi_plan(count))
            shots = [SimpleNamespace(slot_index=i) for i in range(1, count + 1)]
            with patch("services.media_qc.require_shot_media_qc"), patch("services.workflow_v2.ScopedReviewService.require_passed", return_value="passed"):
                self.assertEqual(flow._require_v2_content_gate(task, shots), "passed")
                with self.assertRaises(VideoRenderFlowError):
                    flow._require_v2_content_gate(task, shots[:-1])

    def test_release_fingerprint_rejects_count_tampering(self):
        plan = multi_plan(2)
        revision = SimpleNamespace(revision_id="r", plan_snapshot_json={"plan": plan},
            asset_manifest_json={"selected": {"shot:1": "a", "shot:2": "b"},
                "candidates": {"a": {"asset_id": "a", "sha256": "a" * 64}, "b": {"asset_id": "b", "sha256": "b" * 64}}})
        self.assertEqual(len(expected_render_fingerprint(revision)), 64)
        plan["actual_shot_count"] = 3
        with self.assertRaises(ReleaseGateError):
            expected_render_fingerprint(revision)

    def test_local_production_group_render_release_and_retry_one_and_three_pages(self):
        from test_feishu_v2 import Repo, Renderer
        from test_hero_first import build_task
        from test_multi_look_production import LocalGenerator
        from services.hero_first import HeroFirstProducer
        from services.image_generator import GenerationOutcome
        from services.shot_producer_router import ShotProducerRouter
        from services.workflow_v2 import WorkflowV2Error

        class CountingRenderer(Renderer):
            def __init__(self):
                self.timelines = []
            def render(self, slots, output):
                self.timelines.append(slots)
                return super().render(slots, output)

        class RetryPhoto(LocalGenerator):
            failed = False
            def generate_shot(self, request):
                if request.slot_index == 2 and not self.failed:
                    self.failed = True
                    self.requests.append(request)
                    return GenerationOutcome(ok=False, error="scripted local transient failure")
                return super().generate_shot(request)

        for count in (1, 3):
            with self.subTest(count=count), tempfile.TemporaryDirectory() as directory:
                root, repo, task = Path(directory), Repo(), build_task()
                task.workflow_version = 2
                task.plan_json.update(workflow_version=2, anchor_slot=1, actual_shot_count=count,
                    recipe={"id": "RECIPE_MULTI_LOOK_V1", "content_goal": "multi_look"},
                    recipe_execution={"content_goal": "multi_look", "anchor_slot": 1},
                    outfit_states={f"LOOK_{i:02d}": {"source_look_id": f"L{i}", "bottom": {"type": f"裤{i}"}} for i in range(1, count + 1)})
                task.plan_json["shots"] = task.plan_json["shots"][:count]
                task.plan_json.setdefault("render_contract", {})["target_duration_ms"] = 10000
                for index, shot in enumerate(task.plan_json["shots"], 1):
                    shot.update(shot_kind="composite_board" if index == 1 else "generated_photo",
                        slot_role="full_look", outfit_state_ref=f"LOOK_{index:02d}",
                        duration_ms=10000 // count + (10000 % count if index == 1 else 0), transition_out="cut")
                task.plan_json["shots"][0]["board_spec"] = {"source_person_slot": 1, "decomposition_required": False}
                repo.tasks[task.task_id] = task
                photo, board = RetryPhoto(root, "photo"), LocalGenerator(root, "board")
                producer = HeroFirstProducer(repo, ShotProducerRouter(photo, board), technical_only=True,
                    output_root=root, asset_readiness_gate=lambda *a: None)
                renderer = CountingRenderer()
                flow = TechnicalProductionFlow(repo, producer, VideoRenderFlow(repo, renderer, root))
                if count == 3:
                    with self.assertRaises(WorkflowV2Error):
                        flow.run(task.task_id)
                flow.run(task.task_id)
                self.assertEqual(task.released_revision_id, task.active_revision_id)
                expected_requests = count + (1 if count == 3 else 0)
                self.assertEqual(len(photo.requests), expected_requests)
                self.assertEqual(sum(r.slot_index == 1 for r in photo.requests), 1)
                self.assertEqual(len(board.requests), 1)
                self.assertEqual(len(renderer.timelines), 1)
                timeline = renderer.timelines[0]
                self.assertEqual(len(timeline), count)
                self.assertEqual(timeline[-1].start_ms + timeline[-1].effective_ms, 10000)
                flow.run(task.task_id)
                self.assertEqual(len(photo.requests), expected_requests)
                self.assertEqual(len(board.requests), 1)
                self.assertEqual(len(renderer.timelines), 1)


if __name__ == "__main__":
    unittest.main()
