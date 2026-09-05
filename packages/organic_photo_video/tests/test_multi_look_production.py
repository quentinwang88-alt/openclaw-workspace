"""Offline proof: independent LOOK_01 source + board, then distinct page states."""
import tempfile
import unittest
from pathlib import Path
from PIL import Image

from test_hero_first import FakeRepository, build_task
from services.hero_first import HeroFirstProducer
from services.image_generator import GenerationOutcome, compose_shot_prompt
from services.shot_producer_router import ShotProducerRouter
from services.technical_production import TechnicalCheckService
from services.text_overlay import apply_overlay_profile
from services.workflow_v2 import ReworkService


class LocalGenerator:
    def __init__(self, root, kind):
        self.root, self.kind, self.requests = root, kind, []
    def generate_shot(self, request):
        self.requests.append(request)
        path = self.root / f"{self.kind}-{request.slot_index}-{request.shot_version}.png"
        Image.new("RGB", (1080, 1920), "#dfe3e9").save(path)
        return GenerationOutcome(ok=True, image_path=str(path), width=1080, height=1920)


class MultiRepo(FakeRepository):
    def list_quality_reviews(self, revision_id, *, scope=None, target_id=None):
        return [r for r in self.reviews if r.revision_id == revision_id
                and (scope is None or r.scope == scope) and (target_id is None or r.target_id == target_id)]


class MultiLookProductionTest(unittest.TestCase):
    def run_case(self, count):
        root = Path(self.addCleanupContext(tempfile.TemporaryDirectory()))
        repo = MultiRepo()
        task = build_task()
        task.workflow_version = 2
        task.plan_json.update(workflow_version=2, anchor_slot=1, actual_shot_count=count,
            recipe={"id": "RECIPE_MULTI_LOOK_V1", "content_goal": "multi_look"},
            recipe_execution={"content_goal": "multi_look", "anchor_slot": 1, "transform_mode": "controlled_outfit_change"},
            outfit_states={f"LOOK_{i:02d}": {"source_look_id": f"L{i}", "bottom": {"type": f"裤{i}"}}
                           for i in range(1, count + 1)})
        task.plan_json["shots"] = task.plan_json["shots"][:count]
        for i, shot in enumerate(task.plan_json["shots"], 1):
            shot.update(shot_kind="composite_board" if i == 1 else "generated_photo", slot_role="full_look",
                        outfit_state_ref=f"LOOK_{i:02d}", duration_ms=10000 // count + (10000 % count if i == 1 else 0),
                        transition_out="cut", fit_mode="contain", motion_preset="static_hold")
        task.plan_json["shots"][0]["board_spec"] = {"source_person_slot": 1, "decomposition_required": False}
        repo.tasks[task.task_id] = task
        photo, board = LocalGenerator(root, "photo"), LocalGenerator(root, "board")
        producer = HeroFirstProducer(repo, ShotProducerRouter(photo, board), technical_only=True,
            output_root=root, asset_readiness_gate=lambda *a: None)
        producer.produce(task.task_id)
        self.assertEqual([r.slot_index for r in photo.requests], [1])
        self.assertEqual(photo.requests[0].plan_shot["shot_kind"], "generated_photo")
        self.assertEqual(photo.requests[0].outfit_state["source_look_id"], "L1")
        TechnicalCheckService(repo).anchor(task.task_id)
        revision = repo.get_task_revision(task.active_revision_id)
        anchor_id = revision.asset_manifest_json["selected"]["anchor"]
        self.assertNotIn("shot:1", revision.asset_manifest_json["selected"])
        producer.produce(task.task_id)
        selected = revision.asset_manifest_json["selected"]
        self.assertNotEqual(anchor_id, selected["shot:1"])
        self.assertEqual([r.outfit_state["source_look_id"] for r in photo.requests], [f"L{i}" for i in range(1, count + 1)])
        self.assertEqual(board.requests[0].outfit_state["source_look_id"], "L1")
        self.assertEqual(board.requests[0].continuity_reference_images, [repo.shots[anchor_id].image_url])
        self.assertEqual(len([k for k in selected if k.startswith("shot:")]), count)
        self.assertNotIn(anchor_id, [v for k, v in selected.items() if k.startswith("shot:")])
        if count > 1:
            prompt = compose_shot_prompt(photo.requests[1])
            self.assertIn("不得照抄参考图的补充穿搭", prompt)
            self.assertIn("不靠夸张换机位", prompt)
        return repo, task, producer, photo, board

    def addCleanupContext(self, context):
        value = context.__enter__()
        self.addCleanup(context.__exit__, None, None, None)
        return value

    def test_five_distinct_photo_states_with_independent_p1_board(self):
        self.run_case(5)

    def test_single_available_look_still_outputs_board_not_source(self):
        self.run_case(1)

    def test_board_rework_preserves_all_photo_assets(self):
        repo, task, producer, photo, board = self.run_case(3)
        parent = repo.get_task_revision(task.active_revision_id)
        old_anchor = parent.asset_manifest_json["selected"]["anchor"]
        ReworkService(repo, publication_guard=lambda _: None).begin(task.task_id,
            expected_revision_id=parent.revision_id, expected_lock_version=parent.lock_version,
            scope="board", reason="local board adjustment", idempotency_key="board-test")
        producer.produce(task.task_id)
        self.assertEqual(len(photo.requests), 3)
        self.assertEqual(len(board.requests), 2)
        self.assertEqual(repo.get_task_revision(task.active_revision_id).asset_manifest_json["selected"]["anchor"], old_anchor)

    def test_overlay_count_tracks_actual_pages(self):
        for count in (1, 3, 5):
            plan = {"shots": [{"slot_index": i, "outfit_state_ref": f"LOOK_{i:02d}"} for i in range(1, count + 1)]}
            output = apply_overlay_profile(plan, recipe_id="RECIPE_MULTI_LOOK_V1", locale="th-TH", profile_id="OVERLAY_LIGHT_V1")
            if count > 1:
                self.assertIn(f"{count} ลุค", output["shots"][0]["overlay_text"])
                self.assertIn(f"LOOK {count}", output["shots"][-1]["overlay_text"])
            else:
                self.assertNotIn("5", output["shots"][0]["overlay_text"])
