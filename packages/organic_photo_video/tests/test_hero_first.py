#!/usr/bin/env python3
"""Hero-first orchestrator tests with a scripted fake generator (no API)."""

from __future__ import annotations

import struct
import copy
import tempfile
from pathlib import Path
import sys
import unittest

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from domain import statuses
from domain.models import ContentPackage, ContentShot, ContentTask, generate_prefixed_id
from domain.statuses import (
    SHOT_FAILED,
    SHOT_GENERATED,
    SHOT_PLANNED,
    TASK_FAILED,
    TASK_HERO_GENERATING,
    TASK_IMAGE_REVIEW,
    TASK_PLANNED,
)
from services.hero_first import HeroFirstError, HeroFirstProducer
from services.workflow_v2 import AnchorReviewService
from services.image_generator import (
    GenerationOutcome,
    ShotGenerationRequest,
    compose_shot_prompt,
    read_image_dimensions,
)


def tiny_png(width: int = 1024, height: int = 1536) -> bytes:
    signature = b"\x89PNG\r\n\x1a\n"
    ihdr = struct.pack(">II5B", width, height, 8, 2, 0, 0, 0)
    chunk = struct.pack(">I", 13) + b"IHDR" + ihdr + b"\x00\x00\x00\x00"
    return signature + chunk


def plan_shots() -> list:
    roles = ["hero", "full_look", "lifestyle", "detail", "second_angle"]
    durations = [2200, 2600, 2500, 2200, 3000]
    return [
        {
            "slot_index": i,
            "slot_role": role,
            "purpose": f"purpose {i}",
            "duration_ms": durations[i - 1],
            "motion_preset": "slow_push",
            "transition_out": "short_dissolve",
            "overlay_text": "hook" if i == 1 else "",
            "generation_prompt": f"prompt {i}",
            "source_refs": ["persona:P1", "look:L1", "scene:S1"],
        }
        for i, (role, _) in enumerate(zip(roles, durations), start=1)
    ]


class FakeRepository:
    def __init__(self) -> None:
        self.tasks = {}
        self.shots = {}
        self.transitions = []
        self.packages = {}
        self.revisions = {}
        self.reviews = []

    def get_task(self, task_id):
        return self.tasks.get(task_id)

    def insert_shot(self, shot):
        self.shots[shot.shot_id] = shot

    def list_shots(self, task_id):
        return [s for s in self.shots.values() if s.task_id == task_id]

    def update_shot_status(self, shot_id, from_status, to_status):
        statuses.ensure_transition(statuses.SHOT_STATUS_TRANSITIONS, from_status, to_status)
        shot = self.shots[shot_id]
        assert shot.shot_status == from_status, f"{shot.shot_status} != {from_status}"
        shot.shot_status = to_status

    def update_shot_generation_result(self, shot_id, **fields):
        shot = self.shots[shot_id]
        for key, value in fields.items():
            setattr(shot, key, value)

    def update_shot_qa(self, shot_id, *, qa_status, qa_json, failure_detail=None):
        shot = self.shots[shot_id]
        shot.qa_status = qa_status
        shot.qa_json = qa_json
        shot.failure_detail = failure_detail

    def set_shot_selected(self, shot_id, selected):
        self.shots[shot_id].is_selected = selected

    def transition_task(self, task_id, from_status, to_status, **kwargs):
        statuses.task_ensure_transition(from_status, to_status)
        task = self.tasks[task_id]
        assert task.task_status == from_status, f"{task.task_status} != {from_status}"
        task.task_status = to_status
        if to_status == TASK_FAILED:
            task.failure_code = kwargs.get("failure_code")
            task.failure_detail = kwargs.get("failure_detail")
        self.transitions.append((from_status, to_status))

    def get_content_package(self, package_id):
        return self.packages.get(package_id)

    def get_content_package_by_task(self, task_id):
        return next(
            (p for p in self.packages.values() if p.task_id == task_id), None
        )

    def update_content_package(self, package_id, **fields):
        for key, value in fields.items():
            setattr(self.packages[package_id], key, value)

    # Workflow V2 repository surface used by the V2-only test below.
    def get_task_revision(self, revision_id):
        return self.revisions.get(revision_id)

    def list_task_revisions(self, task_id):
        return [r for r in self.revisions.values() if r.task_id == task_id]

    def create_revision_and_activate(self, revision, *, expected_task_row_version, transition_to=None):
        task = self.tasks[revision.task_id]
        assert task.row_version == expected_task_row_version
        self.revisions[revision.revision_id] = revision
        task.workflow_version = 2
        task.active_revision_id = revision.revision_id
        if transition_to:
            task.task_status = transition_to
            task.plan_json = copy.deepcopy(revision.plan_snapshot_json.get("plan") or {})
            task.group_qa_json = {}
        task.row_version += 1
        return task

    def update_revision_manifest(self, revision_id, *, expected_lock_version, asset_manifest_json, selection_hash):
        revision = self.revisions[revision_id]
        assert revision.lock_version == expected_lock_version
        revision.asset_manifest_json = asset_manifest_json
        revision.selection_hash = selection_hash
        revision.lock_version += 1
        return revision

    def insert_quality_review(self, review):
        self.reviews.append(review)


class ScriptedGenerator:
    """slot_index -> behavior: 'ok' | 'fail' | 'bad_size'."""

    def __init__(self, out_dir: Path, script: dict) -> None:
        self.out_dir = out_dir
        self.script = dict(script)
        self.calls = []
        self.requests = []

    def generate_shot(self, request):
        self.calls.append(request.slot_index)
        self.requests.append(request)
        behavior = self.script.get(request.slot_index, "ok")
        request_id = f"req_P{request.slot_index}_v{request.shot_version}"
        if behavior == "fail":
            return GenerationOutcome(ok=False, request_id=request_id, error="boom")
        width, height = (100, 100) if behavior == "bad_size" else (1024, 1536)
        path = self.out_dir / f"P{request.slot_index}_v{request.shot_version}.png"
        path.write_bytes(tiny_png(width, height))
        return GenerationOutcome(
            ok=width == 1024 and height == 1536,
            image_path=str(path),
            request_id=request_id,
            width=width,
            height=height,
            error="" if (width, height) == (1024, 1536) else f"unexpected image dimensions {(width, height)}",
        )


def build_task(status=TASK_PLANNED, with_plan=True) -> ContentTask:
    return ContentTask(
        task_id="opv_task_1",
        idempotency_key="a" * 64,
        account_id="OPV_TH_TEST_001",
        product_id="1737141103233042426",
        target_country="TH",
        target_locale="th-TH",
        task_status=status,
        current_stage="planning",
        plan_json=(
            {
                "schema_version": "opv-plan-v1",
                "theme": {"id": "T", "topic": "topic"},
                "persona": {"ref_id": "P1", "snapshot": {"name": "泰国甜妹"}},
                "look": {"ref_id": "L1", "source_type": "look_template", "snapshot": {"recipe": {"bottom": "白色牛仔裤"}}},
                "scene": {"ref_id": "S1", "snapshot": {"prompt_core": "airport"}},
                "copy": {"title": "t", "caption": "c", "hashtags": [], "cover_text": "x"},
                "audio_policy": {"strategy": "platform_hot_bgm", "fallback": "no_bgm"},
                "shots": plan_shots(),
            }
            if with_plan
            else None
        ),
        product_snapshot_json={
            "product": {
                "product_id": "1737141103233042426",
                "product_name": "浅蓝色短款蓬松外套",
                "reference_images": ["/tmp/ref_01.jpg"],
            }
        },
    )


class HeroFirstFlowTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = FakeRepository()
        self.repo.tasks["opv_task_1"] = build_task()
        self.generator = ScriptedGenerator(Path(self.tmp.name), {})
        self.producer = HeroFirstProducer(
            self.repo, self.generator, output_root=Path(self.tmp.name), asset_readiness_gate=lambda *_: None,
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_full_flow_generates_all_five_and_reaches_image_review(self) -> None:
        report = self.producer.produce("opv_task_1")
        self.assertTrue(report.hero_ok)
        self.assertEqual(report.task_status, TASK_IMAGE_REVIEW)
        self.assertEqual(self.generator.calls, [1, 2, 3, 4, 5])
        shots = sorted(self.repo.list_shots("opv_task_1"), key=lambda s: s.slot_index)
        self.assertEqual(len(shots), 5)
        for shot in shots:
            self.assertEqual(shot.shot_status, SHOT_GENERATED)
            self.assertEqual(shot.qa_status, "passed")
            self.assertEqual((shot.image_width, shot.image_height), (1024, 1536))
            self.assertTrue(shot.image_sha256)
        self.assertTrue(all(shot.is_selected for shot in shots))
        self.assertEqual(self.generator.requests[0].continuity_reference_images, [])
        hero_path = self.generator.requests[1].continuity_reference_images
        self.assertEqual(len(hero_path), 1)
        self.assertTrue(hero_path[0].endswith("P1_v1.png"))
        self.assertTrue(all(
            request.continuity_reference_images == hero_path
            for request in self.generator.requests[1:]
        ))
        self.assertEqual(
            [(f, t) for f, t in self.repo.transitions],
            [
                (TASK_PLANNED, TASK_HERO_GENERATING),
                (TASK_HERO_GENERATING, "image_generating"),
                ("image_generating", TASK_IMAGE_REVIEW),
            ],
        )

    def test_real_admission_blocks_missing_assets_before_any_provider_call(self):
        from services.asset_readiness import AssetReadinessError
        producer = HeroFirstProducer(self.repo, self.generator, output_root=Path(self.tmp.name))
        with self.assertRaises(AssetReadinessError):
            producer.produce("opv_task_1")
        self.assertEqual(self.generator.calls, [])

    def test_v2_stops_after_anchor_until_scoped_review_selects_it(self) -> None:
        task = self.repo.tasks["opv_task_1"]
        task.workflow_version = 2
        task.plan_json["workflow_version"] = 2
        task.plan_json["anchor_slot"] = 2
        first = self.producer.produce("opv_task_1")
        self.assertTrue(first.hero_ok)
        self.assertEqual(first.task_status, "anchor_review")
        self.assertEqual(self.generator.calls, [2])
        self.assertTrue(all(
            shot.shot_status == SHOT_PLANNED
            for shot in self.repo.list_shots("opv_task_1") if shot.slot_index != 2
        ))
        # A resume without an explicit review is a safe no-op.
        self.producer.produce("opv_task_1")
        self.assertEqual(self.generator.calls, [2])
        revision = self.repo.get_task_revision(task.active_revision_id)
        result = AnchorReviewService(self.repo).record(
            "opv_task_1", asset_id=next(iter(revision.asset_manifest_json["candidates"])),
            decision="passed", dimensions={"product_fidelity": 90}, reviewer_type="human",
            expected_lock_version=revision.lock_version,
        )
        self.assertTrue(result.advanced)
        completed = self.producer.produce("opv_task_1")
        self.assertEqual(completed.task_status, TASK_IMAGE_REVIEW)
        self.assertEqual(self.generator.calls, [2, 1, 3, 4, 5])

    def test_hero_failure_blocks_remaining_slots(self) -> None:
        self.generator.script = {1: "fail"}
        report = self.producer.produce("opv_task_1")
        self.assertFalse(report.hero_ok)
        self.assertEqual(report.task_status, TASK_FAILED)
        self.assertEqual(self.generator.calls, [1])
        task = self.repo.tasks["opv_task_1"]
        self.assertEqual(task.failure_code, "hero_generation_failed")
        self.assertEqual(len(self.repo.list_shots("opv_task_1")), 5)
        others = [s for s in self.repo.list_shots("opv_task_1") if s.slot_index != 1]
        self.assertTrue(all(s.shot_status == SHOT_PLANNED for s in others))

    def test_resume_after_hero_failure_regenerates_everything(self) -> None:
        self.generator.script = {1: "fail"}
        self.producer.produce("opv_task_1")
        self.generator.script = {}
        report = self.producer.produce("opv_task_1")
        self.assertTrue(report.hero_ok)
        self.assertEqual(report.task_status, TASK_IMAGE_REVIEW)
        hero = next(s for s in self.repo.list_shots("opv_task_1") if s.slot_index == 1)
        self.assertEqual(hero.shot_version, 1)  # retry reuses the row via failed->generating
        self.assertEqual(hero.shot_status, SHOT_GENERATED)

    def test_partial_failure_continues_and_regenerate_adds_version(self) -> None:
        self.generator.script = {3: "fail"}
        report = self.producer.produce("opv_task_1")
        self.assertTrue(report.hero_ok)
        self.assertEqual(report.task_status, TASK_IMAGE_REVIEW)
        slot3 = [s for s in self.repo.list_shots("opv_task_1") if s.slot_index == 3]
        self.assertEqual(len(slot3), 1)
        self.assertEqual(slot3[0].shot_status, SHOT_FAILED)
        self.assertEqual(slot3[0].qa_status, "failed")

        self.generator.script = {}
        retry = self.producer.regenerate_slot("opv_task_1", 3)
        self.assertEqual(retry.status, SHOT_GENERATED)
        self.assertEqual(retry.shot_version, 2)
        versions = sorted(
            (s for s in self.repo.list_shots("opv_task_1") if s.slot_index == 3),
            key=lambda s: s.shot_version,
        )
        self.assertEqual(len(versions), 2)
        self.assertEqual(versions[0].shot_status, SHOT_FAILED)  # terminal per version
        self.assertEqual(versions[1].shot_status, SHOT_GENERATED)

    def test_wrong_dimensions_fail_media_qc(self) -> None:
        self.generator.script = {1: "bad_size"}
        report = self.producer.produce("opv_task_1")
        self.assertFalse(report.hero_ok)
        self.assertEqual(report.task_status, TASK_FAILED)
        hero = next(s for s in self.repo.list_shots("opv_task_1") if s.slot_index == 1)
        self.assertEqual(hero.shot_status, SHOT_FAILED)
        self.assertIn("dimensions", hero.failure_detail)

    def test_produce_requires_plan(self) -> None:
        self.repo.tasks["no_plan"] = build_task(with_plan=False)
        self.repo.tasks["no_plan"].task_id = "no_plan"
        with self.assertRaises(HeroFirstError):
            self.producer.produce("no_plan")

    def test_read_image_dimensions_handles_png_and_garbage(self) -> None:
        good = Path(self.tmp.name) / "good.png"
        good.write_bytes(tiny_png(1024, 1536))
        self.assertEqual(read_image_dimensions(str(good)), (1024, 1536))
        bad = Path(self.tmp.name) / "bad.png"
        bad.write_bytes(b"not an image")
        self.assertIsNone(read_image_dimensions(str(bad)))

    def test_recipe_p5_anchor_drives_generation_and_regeneration(self) -> None:
        task = self.repo.tasks["opv_task_1"]
        task.plan_json["anchor_slot"] = 5
        task.plan_json["recipe_execution"] = {
            "transform_mode": "controlled_outfit_change"
        }
        task.plan_json["outfit_states"] = {
            "BASE": {"state_purpose": "before"},
            "FINAL": {"state_purpose": "after"},
        }
        for shot in task.plan_json["shots"]:
            shot["outfit_state_ref"] = "BASE" if shot["slot_index"] == 2 else "FINAL"
        report = self.producer.produce("opv_task_1")
        self.assertTrue(report.hero_ok)
        self.assertEqual(self.generator.calls, [5, 1, 2, 3, 4])
        self.assertTrue(
            all(
                request.continuity_reference_images[0].endswith("P5_v1.png")
                for request in self.generator.requests[1:]
            )
        )
        retry = self.producer.regenerate_slot("opv_task_1", 2)
        self.assertEqual(retry.status, SHOT_GENERATED)
        self.assertTrue(
            self.generator.requests[-1].continuity_reference_images[0].endswith(
                "P5_v1.png"
            )
        )
        prompt = compose_shot_prompt(
            next(
                request
                for request in self.generator.requests
                if request.slot_index == 2 and request.shot_version == 1
            )
        )
        self.assertIn("当前穿搭状态：BASE", prompt)
        self.assertIn("不得照抄参考图的补充穿搭", prompt)

    def test_composition_contract_prevents_pose_copy_and_unplanned_props(self) -> None:
        task = self.repo.tasks["opv_task_1"]
        task.plan_json["shots"][2]["composition_contract"] = {
            "framing": "medium_full",
            "camera_angle": "three_quarter_dynamic",
            "pose": "scene_matched_action",
            "prop_policy": "only_from_scene_or_purpose",
            "instruction": "斜侧动态中全身",
            "forbidden": ["repeat_previous_pose", "unplanned_props"],
        }
        task.plan_json["shots"][3]["composition_contract"] = {
            "framing": "waist_up_detail",
            "camera_angle": "front_or_three_quarter_close",
            "pose": "show_confirmed_product_or_proportion_detail",
            "prop_policy": "no_new_props",
            "instruction": "腰部至头部近景",
            "forbidden": ["full_body", "shoes", "large_floor_area"],
        }
        self.producer.produce(task.task_id)
        lifestyle = compose_shot_prompt(self.generator.requests[2])
        detail = compose_shot_prompt(self.generator.requests[3])
        self.assertNotIn("推行李", lifestyle)
        self.assertNotIn("旅行箱", lifestyle)
        self.assertIn("不得自行添加任何无关道具", lifestyle)
        self.assertIn("不得用于照抄上一张的景别、构图、站姿、手势或机位", lifestyle)
        self.assertIn("腰部至头部的安全近景", detail)
        self.assertIn("禁止出现完整腿部、鞋子和大面积地面", detail)
        self.assertIn("full_body", detail)

    def test_outfit_breakdown_anchor_requests_a_clean_cutout_source(self) -> None:
        task = self.repo.tasks["opv_task_1"]
        task.plan_json["anchor_slot"] = 2
        task.plan_json["recipe_execution"] = {
            "content_goal": "outfit_breakdown",
            "anchor_slot": 2,
            "transform_mode": "single_frozen_outfit",
        }
        task.plan_json["shots"][1]["camera_hint"] = "正面全身，纯净近白背景"
        self.producer.produce(task.task_id)
        anchor_request = next(
            request for request in self.generator.requests if request.slot_index == 2
        )
        prompt = compose_shot_prompt(anchor_request)
        self.assertIn("首图人物锚点源", prompt)
        self.assertIn("保持当前已选场景", prompt)

    def test_pure_color_profile_replaces_scene_background_contract(self) -> None:
        request = ShotGenerationRequest(
            task_id="pure-color", slot_index=2, slot_role="full_look", shot_version=1,
            plan_shot={"slot_index": 2, "slot_role": "full_look", "purpose": "完整穿搭"},
            product={"category": "outerwear", "reference_images": ["/tmp/product.jpg"]},
            persona_snapshot={"name": "persona"}, look_snapshot={},
            scene_snapshot={"name": "airport", "prompt_core": "airport terminal"},
            output_dir="/tmp",
            recipe_execution={
                "content_goal": "outfit_breakdown", "anchor_slot": 2,
                "presentation_profile": {
                    "background_mode": "solid_color", "background_color": "#F6F5F2",
                    "proportion_direction": "自然修长时装比例，约7.6-8头身；不得机械拉伸",
                },
            },
        )
        prompt = compose_shot_prompt(request)
        self.assertIn("#F6F5F2", prompt)
        self.assertIn("7.6-8头身", prompt)
        self.assertIn("不得把它画成实际背景", prompt)
        self.assertNotIn("保持当前已选场景", prompt)

    def test_product_reference_overrides_legacy_look_and_colored_name(self) -> None:
        request = ShotGenerationRequest(
            task_id="brown-pack",
            slot_index=1,
            slot_role="hero",
            shot_version=1,
            plan_shot={"slot_index": 1, "slot_role": "hero", "purpose": "展示商品"},
            product={
                "product_id": "1737141103233042426",
                "product_name": "浅蓝色短款蓬松外套",
                "category": "outerwear",
                "reference_images": ["/tmp/brown-pack.jpg"],
            },
            product_facts={
                "facts": {"color": "brown", "material": "unknown"},
            },
            persona_snapshot={"name": "persona"},
            look_snapshot={
                "name": "legacy blue look",
                "prompt_core": "wear the light blue outerwear",
                "recipe": {
                    "target_outer": "浅蓝色短款外套",
                    "bottom": "白色直筒裤",
                },
            },
            scene_snapshot={"name": "bright room"},
            output_dir="/tmp",
        )
        prompt = compose_shot_prompt(request)
        self.assertIn("商品参考图是", prompt)
        self.assertIn("color=brown", prompt)
        self.assertIn("下装：白色直筒裤", prompt)
        self.assertNotIn("浅蓝", prompt)
        self.assertNotIn("light blue", prompt.lower())

    def test_content_package_receives_fresh_ordered_shot_ids(self) -> None:
        task = self.repo.tasks["opv_task_1"]
        task.content_package_id = "pkg_1"
        self.repo.packages["pkg_1"] = ContentPackage(
            content_package_id="pkg_1", task_id=task.task_id
        )
        self.producer.produce(task.task_id)
        package = self.repo.packages["pkg_1"]
        latest = sorted(
            self.repo.list_shots(task.task_id), key=lambda shot: shot.slot_index
        )
        self.assertEqual(
            package.selected_image_ids_json,
            [shot.shot_id for shot in latest],
        )
        self.assertTrue(all(package.selected_image_ids_json))


if __name__ == "__main__":
    unittest.main()
