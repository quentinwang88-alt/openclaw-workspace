"""Offline acceptance tests for the MX wig four-choice flow (mx_wig_choice_v1).

Bar from the handoff (§12): a real four-page pass through request build →
validate → intake → REAL planner → REAL producer → REAL exporter; targeted
resume/retake; TH dispatch protection; product misfill rejection before any
paid call.  Only the paid image API and the vision model are stubbed here.
"""
from __future__ import annotations

import copy
import hashlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from PIL import Image

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from config.loader import load_board_layouts, load_content_recipe_file  # noqa: E402
from domain import statuses  # noqa: E402
from domain.contracts import validate_photo_plan_payload  # noqa: E402
from domain.models import AssetSet  # noqa: E402
from services.feishu_workflow import FeishuTaskWorkflow  # noqa: E402
from services.image_generator import GenerationOutcome, ShotGenerationRequest  # noqa: E402
from services.photo_planner import PhotoReusePlannerService  # noqa: E402
from services.photo_request_factory import (  # noqa: E402
    PhotoRequestFactory, fingerprint, validate_frozen_request,
)
from services.photo_wig_flow import (  # noqa: E402
    MX_WIG_CHOICE_FLOW, MX_WIG_RECIPE_ID, batch_is_mx_wig_choice,
    recipe_is_mx_wig_choice, request_is_mx_wig_choice,
)
from services.photo_wig_planner import (  # noqa: E402
    MX_WIG_DEFAULT_THEME, PhotoWigPlannerError, freeze_wig_content_card,
    plan_from_items, resolve_mx_wig_theme,
)
from services.photo_wig_qa import (  # noqa: E402
    PhotoWigQaError, check_wig_plan, check_wig_sources, normalize_wig_group_qa,
)
from services.photo_wig_supply import (  # noqa: E402
    PhotoWigSupplyError, PhotoWigSupplyService, compose_wig_prompt,
)
from test_hero_first import FakeRepository  # noqa: E402

MX_RECIPE_PATH = PACKAGE_ROOT / "config" / "recipes" / "PHOTO_MX_PICK_YOUR_HAIR_V2.json"
MX_PRESET_NAME = "图文｜MX｜四选一发型V2TEST"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def make_face_image(path: Path, color=(150, 110, 90)) -> str:
    Image.new("RGB", (900, 1200), color).save(path, format="PNG")
    return str(path)


def persona_snapshot(root: Path) -> dict:
    path = make_face_image(Path(root) / "persona_a_face.png")
    return {
        "persona_id": "MX_WIG_CAST_A_001",
        "ref_id": "MX_WIG_CAST_A_001",
        "name": "MX 假发图文人物 A｜日常亲和",
        "reference_items": [{
            "local_path": path, "sha256": _sha256(Path(path)),
            "role": "FACE_FRONT_NEUTRAL", "approved": True, "is_primary": True,
        }],
    }


def wig_options(variant: int = 1) -> list:
    if variant == 1:
        return [
            {"role": "hair_a", "label_es": "Bob corto", "label_zh": "利落短Bob",
             "length": "chin_bob", "texture": "straight", "color": "dark_brown",
             "parting": "center", "silhouette": "sleek_bell", "framing": "chest_up"},
            {"role": "hair_b", "label_es": "Bob a la clavícula", "label_zh": "锁骨弧度",
             "length": "collarbone", "texture": "soft_wave", "color": "dark_brown",
             "parting": "center", "silhouette": "soft_arc", "framing": "chest_up"},
            {"role": "hair_c", "label_es": "Largo liso", "label_zh": "长直发",
             "length": "long", "texture": "straight", "color": "dark_brown",
             "parting": "center", "silhouette": "sleek_long", "framing": "waist_up"},
            {"role": "hair_d", "label_es": "Ondas largas", "label_zh": "长波浪",
             "length": "long", "texture": "soft_wave", "color": "warm_brown",
             "parting": "center", "silhouette": "voluminous_wave", "framing": "waist_up"},
        ]
    return [
        {"role": "hair_a", "label_es": "Bob estilo garçon", "label_zh": "帅气短碎",
         "length": "chin_bob", "texture": "wave", "color": "black",
         "parting": "side", "silhouette": "textured", "framing": "chest_up"},
        {"role": "hair_b", "label_es": "Rizos definidos", "label_zh": "定型卷",
         "length": "collarbone", "texture": "curl", "color": "black",
         "parting": "center", "silhouette": "springy_curl", "framing": "chest_up"},
        {"role": "hair_c", "label_es": "Negro pulido", "label_zh": "黑长直",
         "length": "long", "texture": "straight", "color": "black",
         "parting": "center", "silhouette": "glass_long", "framing": "waist_up"},
        {"role": "hair_d", "label_es": "Hollywood waves", "label_zh": "好莱坞波浪",
         "length": "long", "texture": "wave", "color": "warm_brown",
         "parting": "side", "silhouette": "glam_wave", "framing": "waist_up"},
    ]


def wig_item(index: int, recipe_payload: dict) -> dict:
    copies = recipe_payload["recipe_spec"]["execution_profiles"][0]["copy_variants"]
    copy_block = copy.deepcopy(copies[(index - 1) % len(copies)]["copy"])
    item = {
        "item_index": index,
        "topic_zh": "周末换个发型，你选哪款？" if index == 1 else "换发型迎接周末，选你的款",
        "photography_direction_zh": "窗边柔和自然光，简洁暖色背景",
        "wardrobe_direction_zh": "简单奶油色上衣，与头发形成明暗对比",
        "makeup_direction_zh": "自然精致日常妆",
        "scene_prompt_zh": "室内窗边，背景轻虚化",
        "reference_uses": [{"image_index": 1, "uses": ["persona"]}],
        "options": wig_options(index),
        "copy": copy_block,
    }
    problems = check_wig_plan(item)
    assert not problems, problems
    return item


class FakeWigVision:
    """Vision-model stub: deterministic plan + scripted group/copy verdicts."""

    def __init__(self, items_recipe: dict, *, group_script=None, copy_ok=True):
        self.items_recipe = items_recipe
        self.group_script = list(group_script or [])
        self.group_calls = 0
        self.copy_ok = copy_ok
        self.copy_calls = []

    def plan_wig_choice_content(self, *, record_id, persona_paths, reference_paths=(),
                                content_requirement="", count=1, theme_label_zh="",
                                choice_axis="style"):
        plan = plan_from_items(
            [wig_item(index, self.items_recipe) for index in range(1, count + 1)]
        )
        plan["model"] = "fake-planner-model"
        plan["model_routing"] = {"requested_model": "fake", "actual_model": "fake-planner-model",
                                 "provider_used": "injected"}
        plan["prompt_version"] = "fake-plan-prompt-v1"
        return plan

    def review_wig_group(self, *, persona_image_path, sources, plan_item):
        self.group_calls += 1
        if self.group_script:
            verdict = self.group_script.pop(0)
            if callable(verdict):
                return verdict(sources)
            return verdict
        return self._pass(sources)

    @staticmethod
    def _pass(sources):
        return {"passed": True, "failed_roles": [], "issues": {},
                "roles": {str(item["role"]): {"passed": True, "hard_flags": [], "notes": ""}
                          for item in sources},
                "group_notes": ""}

    def review_wig_copy_semantics(self, *, copy_block, plan_item):
        self.copy_calls.append(copy.deepcopy(dict(copy_block)))
        if not self.copy_ok:
            return {"passed": False, "issues": ["copy is not natural es-MX"], "notes": ""}
        return {"passed": True, "issues": [], "notes": "ok"}


class FakeWigGenerator:
    """Paid-API stub: deterministic distinct 9:16 PNGs, optional failures."""

    def __init__(self, root: Path):
        self.root = Path(root)
        self.calls: list = []
        self.fail_roles: dict = {}

    def generate_shot(self, request: ShotGenerationRequest) -> GenerationOutcome:
        role = request.slot_role
        self.calls.append((role, request.prompt_override, request.shot_version))
        if self.fail_roles.get(role, 0) > 0:
            self.fail_roles[role] -= 1
            return GenerationOutcome(ok=False, provider="fake", model="fake-model",
                                     error="boom", request_id=f"req-{len(self.calls)}")
        self.root.mkdir(parents=True, exist_ok=True)
        seed = sum(bytearray(role.encode())) + request.shot_version * 7 + len(self.calls) * 13
        path = self.root / f"{request.task_id}_{role}_a{request.shot_version}.png"
        Image.new("RGB", (960, 1708), (seed % 255, 90, 140)).save(path, format="PNG")
        return GenerationOutcome(ok=True, image_path=str(path), provider="fake",
                                 model="fake-model", request_id=f"req-{len(self.calls)}",
                                 width=960, height=1708)


class WigBatchRepo(FakeRepository):
    """FakeRepository + photo config, batches, accounts and asset sets."""

    def __init__(self, root: Path):
        super().__init__()
        self.root = Path(root)
        self.recipe = load_content_recipe_file(MX_RECIPE_PATH)
        self.recipes = {self.recipe.recipe_id: self.recipe}
        self.asset_sets: list = []
        self.batch = None
        self.pending: dict = {}
        self.pack = SimpleNamespace(
            market_pack_id="MP_MX_DEFAULT_V1", pack_version=1,
            target_country="MX", target_locale="es-MX", status="active",
        )
        self.account = SimpleNamespace(
            account_id="OPV_MX_PHOTO_001", status="testing",
            default_market_pack_id="MP_MX_DEFAULT_V1", default_render_preset_id=None,
            persona_ref_id="MX_WIG_CAST_A_001", allowed_look_refs_json=[],
            allowed_scene_refs_json=[], core_scene_refs_json=[],
            operating_rules_json={"category_key": "wig", "default_product_mode": "NO_PRODUCT"},
        )

    # photo config ---------------------------------------------------------
    def get_content_recipe(self, recipe_id):
        return self.recipes.get(recipe_id)

    def get_market_pack(self, pack_id):
        return self.pack if pack_id == self.pack.market_pack_id else None

    def get_account_profile(self, account_id):
        return self.account if account_id == self.account.account_id else None

    def get_render_preset(self, _preset_id):
        return None

    # intake ---------------------------------------------------------------
    def create_task_idempotent(self, task):
        for existing in self.tasks.values():
            if existing.idempotency_key == task.idempotency_key:
                return existing, False
        self.tasks[task.task_id] = task
        return task, True

    def list_tasks_by_source_prefix(self, _source_type, prefix):
        return [t for t in self.tasks.values()
                if str(t.source_record_id or "").startswith(prefix)]

    # planner / package writes --------------------------------------------
    def update_task_plan(self, task_id, **fields):
        task = self.tasks[task_id]
        for key, value in fields.items():
            setattr(task, key, copy.deepcopy(value))

    def insert_content_package(self, package):
        self.packages[package.content_package_id] = package

    # batches --------------------------------------------------------------
    def get_production_batch(self, record_id):
        if self.batch is not None and self.batch.source_record_id == record_id:
            return self.batch
        return None

    def create_production_batch_idempotent(self, batch):
        if self.batch is None:
            self.batch = copy.deepcopy(batch)
        return self.batch

    def claim_batch_run(self, identity, **kwargs):
        if getattr(self.batch, "run_owner", None):
            return False
        self.batch.run_owner = kwargs["owner"]
        return True

    def heartbeat_batch_run(self, identity, **kwargs):
        return self.batch.run_owner == kwargs["owner"]

    def finish_batch_run(self, identity, **kwargs):
        self.batch.run_owner = None

    def queue_batch_projection(self, identity, fields):
        self.pending = copy.deepcopy(fields)

    def acknowledge_batch_projection(self, identity, fields):
        if self.pending == fields:
            self.pending = {}

    # asset sets -----------------------------------------------------------
    def upsert_asset_set(self, asset_set):
        self.asset_sets.append(copy.deepcopy(asset_set))

    def get_asset_set(self, asset_set_id):
        for item in self.asset_sets:
            if item.asset_set_id == asset_set_id:
                return copy.deepcopy(item)
        return None

    def list_asset_sets(self, **_kwargs):
        return list(self.asset_sets)

    def list_photo_content_signatures(self, exclude_record_id=None):
        return set()


def build_workflow(root: Path, *, vision: FakeWigVision, generator: FakeWigGenerator):
    presets = {
        "default_overlay_profile_id": "",
        "presets": [{
            "name": MX_PRESET_NAME, "routing_policy": "native_photo_v1",
            "media_kind": "native_photo", "category_key": "wig",
            "default_product_mode": "NO_PRODUCT", "default_asset_mode": "ASSET_REUSE",
            "tasks": [{
                "account_id": "OPV_MX_PHOTO_001", "market": "MX", "language": "es-MX",
                "recipe_id": MX_WIG_RECIPE_ID, "theme_id": "",
                "hook_strategy": "cual_elegirias", "persona_ref": "",
                "look_ref": "", "scene_ref": "",
            }],
        }],
    }
    presets_path = Path(root) / "presets_mx_test.json"
    presets_path.write_text(json.dumps(presets, ensure_ascii=False, indent=1), encoding="utf-8")
    from services.feishu_workflow import ProductionPresetCatalog
    catalog = ProductionPresetCatalog(presets_path)
    repo = WigBatchRepo(root)
    client = SimpleNamespace(
        fields={"生产预设": MX_PRESET_NAME, "生成篇数": 1, "执行": True},
        uploaded=[],
    )
    workflow = FeishuTaskWorkflow(
        repo, client, generator=generator, renderer=object(),
        output_root=Path(root) / "output", catalog=catalog,
        photo_reference_vision=vision,
    )
    workflow._upload_files = lambda paths, **kwargs: [
        {"file_token": str(path)} for path in paths
    ]
    workflow._v2_released = lambda task: bool(getattr(task, "released_revision_id", None))

    def _update_record_fields(record_id, fields):
        client.fields.update(copy.deepcopy(fields))
    client.update_record_fields = _update_record_fields
    client.get_record = lambda identity: SimpleNamespace(
        record_id=identity, fields=copy.deepcopy(client.fields))
    client.list_records = lambda page_size=500: []
    return workflow, repo, client


class WigChoiceEndToEnd(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.recipe_payload = json.loads(MX_RECIPE_PATH.read_text())
        self.vision = FakeWigVision(self.recipe_payload)
        self.generator = FakeWigGenerator(self.root / "gen")
        self.workflow, self.repo, self.client = build_workflow(
            self.root, vision=self.vision, generator=self.generator)

    def scan(self):
        return self.workflow.scan(record_id="mxrec1")

    # -- the four-page pass --------------------------------------------------

    def test_four_page_e2e_with_real_planner_producer_exporter(self):
        report = self.scan()
        self.assertEqual(report["errors"], [], report["errors"])
        self.assertEqual(report["processed"][0]["action"], "generate_native_photo")
        self.assertEqual(report["processed"][0]["photo_count"], 4)
        task = next(iter(self.repo.tasks.values()))
        # Frozen request contract ------------------------------------------
        batch = self.repo.batch
        self.assertTrue(batch_is_mx_wig_choice(batch.manifest_json))
        self.assertEqual(batch.manifest_json.get("execution_flow"), MX_WIG_CHOICE_FLOW)
        request = batch.manifest_json["entries"][0]["request"]
        self.assertEqual(request["recipe_id"], MX_WIG_RECIPE_ID)
        self.assertEqual(request["market"], "MX")
        self.assertEqual(request["locale"], "es-MX")
        self.assertEqual(request["product_mode"], "NO_PRODUCT")
        self.assertEqual(request["execution_flow"], MX_WIG_CHOICE_FLOW)
        validate_frozen_request(request)  # dispatches into the MX validator
        asset_manifest = request["asset_snapshot"]["manifest_json"]
        if isinstance(asset_manifest, str):
            asset_manifest = json.loads(asset_manifest)
        roles = [item["role"] for item in asset_manifest["assets"]]
        self.assertEqual(roles, ["hair_a", "hair_b", "hair_c", "hair_d"])
        self.assertNotIn("cover_seed", json.dumps(request))
        # Plan is the real planner output with four slides ------------------
        self.assertEqual(task.requested_shot_count, 4)
        plan = task.plan_json
        self.assertEqual([s["slot_role"] for s in plan["slides"]],
                         ["hair_a", "hair_b", "hair_c", "hair_d"])
        self.assertEqual([s["slot_role"] for s in plan["shots"]],
                         ["hair_a", "hair_b", "hair_c", "hair_d"])
        self.assertEqual(plan["production_policy"], {"mode": "ASSET_REUSE", "ai_image_calls": 0})
        self.assertEqual(validate_photo_plan_payload(plan), [])
        # Real exporter wrote four ordered JPEG pages -----------------------
        slides = report and self.repo.get_content_package(
            task.content_package_id).photo_manifest_json["slides"]
        self.assertEqual([s["index"] for s in slides], [1, 2, 3, 4])
        for slide in slides:
            path = Path(slide["path"])
            self.assertTrue(path.is_file() and path.stat().st_size > 1000, path)
        package_manifest = self.repo.get_content_package(
            task.content_package_id).photo_manifest_json
        self.assertEqual(package_manifest["copy"]["slide_texts"],
                         plan["copy"]["slide_texts"])
        self.assertEqual(package_manifest["cover_index"], 1)
        # Qualified hair evidence, never clothing evidence ------------------
        asset_set = self.repo.get_asset_set(request["asset_set_id"])
        self.assertTrue(asset_set.asset_set_id.startswith("ASSET_MX_WIG_CHOICE_GEN_"))
        approval = asset_set.manifest_json["content_approval"]
        self.assertEqual(approval["allowed_logic_keys"], ["wig_four_choice_es"])
        blob = json.dumps(asset_set.manifest_json)
        self.assertNotIn("outerwear_id", blob)
        self.assertNotIn("bottom_id", blob)
        identity_ids = {attrs.get("identity_id")
                        for attrs in approval["attributes"].values()}
        self.assertEqual(len(identity_ids), 1)
        # Identity evidence must bind to the persona actually resolved through
        # the account binding (recorded in the supply manifest).
        supply_manifest = json.loads((Path(self.workflow.output_root) /
                                      "wig_choice_supply" / "mxrec1_item_1" /
                                      "wig_supply_manifest.json").read_text())
        persona_sha = supply_manifest["persona"]["identity_reference_sha256s"][0]
        self.assertIn(persona_sha[:16], next(iter(identity_ids)))
        # Prompt contract: identity lock + hair change, no clothing lock ----
        self.assertEqual(len(self.generator.calls), 4)
        for role, prompt, _attempt in self.generator.calls:
            self.assertIn("【人物身份锁】", prompt)
            self.assertIn("【本页发型计划】", prompt)
            self.assertIn("整体更换发型", prompt)
            self.assertIn("输入图片1用途：人物身份", prompt)
            for banned in ("【冻结穿搭】", "外套", "头身比", "保持原发型"):
                self.assertNotIn(banned, prompt)
        self.assertEqual([role for role, _p, _a in self.generator.calls],
                         ["hair_a", "hair_b", "hair_c", "hair_d"])

    def test_resume_after_intake_keeps_frozen_sources(self):
        self.scan()
        generator_calls_first = len(self.generator.calls)
        task = next(iter(self.repo.tasks.values()))
        task.task_status = statuses.TASK_DRAFT  # force a resume through intake+plan
        report = self.scan()
        self.assertEqual(report["errors"], [])
        self.assertEqual(len(self.generator.calls), generator_calls_first,
                         "resume must not regenerate or re-bill finished sources")

    def test_product_code_rejected_before_any_paid_call(self):
        self.client.fields["产品编码"] = "SKU-123"
        report = self.scan()
        self.assertTrue(report["errors"], report)
        self.assertIn("单款商品模式暂未开放", report["errors"][0]["error"])
        self.assertEqual(self.generator.calls, [])
        self.assertEqual(self.client.fields.get("产品编码"), "SKU-123",
                         "operator input must be preserved")

    def test_english_copy_blocked_before_any_paid_call(self):
        self.vision.copy_ok = False
        report = self.scan()
        self.assertTrue(report["errors"], report)
        self.assertIn("西语文案审校未通过", report["errors"][0]["error"])
        self.assertEqual(self.generator.calls, [])


class WigSupplyBehaviour(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.recipe_payload = json.loads(MX_RECIPE_PATH.read_text())
        self.plan_item = wig_item(1, self.recipe_payload)
        self.persona = persona_snapshot(self.root)
        self.generator = FakeWigGenerator(self.root / "gen")
        self.vision = FakeWigVision(self.recipe_payload)
        self.supply = PhotoWigSupplyService(
            generator=self.generator, root=self.root,
            qa_reviewer=FakeGroupReviewer(self.vision),
        )
        self.record_id = "mx_item_resume"

    def prepare(self, **kwargs):
        return self.supply.prepare(
            record_id=self.record_id, plan_item=self.plan_item,
            persona=self.persona, **kwargs)

    def test_group_qa_failure_repairs_role_once_then_passes(self):
        self.vision.group_script = [
            {"passed": False,
             "failed_roles": ["hair_b"],
             "issues": {"hair_b": ["face_changed"]},
             "roles": {"hair_a": {"passed": True, "hard_flags": [], "notes": ""},
                       "hair_b": {"passed": False, "hard_flags": ["face_changed"], "notes": ""},
                       "hair_c": {"passed": True, "hard_flags": [], "notes": ""},
                       "hair_d": {"passed": True, "hard_flags": [], "notes": ""}},
             "group_notes": "B 换脸"},
        ]
        prepared = self.prepare()
        self.assertTrue(prepared["group_qa"]["passed"])
        roles_attempts = {role: attempt for role, _p, attempt in self.generator.calls}
        self.assertEqual(roles_attempts.get("hair_b"), 2, "B repaired exactly once")
        self.assertEqual([role for role, _p, _a in self.generator.calls].count("hair_a"), 1)
        manifest = self.supply.load_manifest(self.record_id)
        self.assertEqual(len(manifest["retired"]), 1)

    def test_group_qa_hard_failure_after_repair_stops_with_roles(self):
        def always_fail(sources):
            return {"passed": False, "failed_roles": ["hair_c"],
                    "issues": {"hair_c": ["hairstyle_mismatch"]},
                    "roles": {str(item["role"]): {"passed": str(item["role"]) != "hair_c",
                                                  "hard_flags": ["hairstyle_mismatch"]
                                                  if str(item["role"]) == "hair_c" else [],
                                                  "notes": ""}
                              for item in sources},
                    "group_notes": ""}
        self.vision.group_script = [always_fail, always_fail]
        with self.assertRaises(PhotoWigSupplyError) as ctx:
            self.prepare()
        self.assertIn("hair_c", str(ctx.exception))
        # Sources stay on disk for targeted retake; nothing else regenerated.
        manifest = self.supply.load_manifest(self.record_id)
        self.assertEqual(len(manifest["sources"]), 4)

    def test_resume_regenerates_only_missing_role(self):
        prepared = self.prepare()
        before = {item["role"]: (item["path"], item["sha256"])
                  for item in prepared["sources"]}
        calls_first = len(self.generator.calls)
        # Simulate a lost/interrupted hair_b result.
        manifest = self.supply.load_manifest(self.record_id)
        manifest["sources"] = [item for item in manifest["sources"]
                               if item["role"] != "hair_b"]
        manifest["status"] = "incomplete"
        manifest.pop("group_qa", None)
        manifest_path = self.supply.manifest_path(self.record_id)
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=1),
                                 encoding="utf-8")
        repared = self.prepare()
        after = {item["role"]: (item["path"], item["sha256"])
                 for item in repared["sources"]}
        self.assertEqual(len(self.generator.calls) - calls_first, 1)
        self.assertEqual(self.generator.calls[-1][0], "hair_b")
        for role in ("hair_a", "hair_c", "hair_d"):
            self.assertEqual(before[role], after[role],
                             f"{role} must not be regenerated on resume")

    def test_retake_regenerates_only_target_role(self):
        prepared = self.prepare()
        before = {item["role"]: (item["path"], item["sha256"])
                  for item in prepared["sources"]}
        calls_first = len(self.generator.calls)
        self.supply.regenerate_roles(self.record_id, ["hair_c"], reason="运营手动重拍")
        repared = self.prepare()
        after = {item["role"]: (item["path"], item["sha256"])
                 for item in repared["sources"]}
        self.assertEqual(len(self.generator.calls) - calls_first, 1)
        self.assertEqual(self.generator.calls[-1][0], "hair_c")
        self.assertNotEqual(before["hair_c"], after["hair_c"])
        for role in ("hair_a", "hair_b", "hair_d"):
            self.assertEqual(before[role], after[role])

    def test_input_change_refuses_to_mix_old_and_new_plan(self):
        self.prepare()
        changed = copy.deepcopy(self.plan_item)
        changed["copy"]["title"] = "¿Otro plan para el finde?"
        with self.assertRaises(PhotoWigSupplyError):
            self.supply.prepare(record_id=self.record_id, plan_item=changed,
                                persona=self.persona)

    def test_source_tampering_is_detected(self):
        prepared = self.prepare()
        sources = prepared["sources"]
        sources[0]["sha256"] = "0" * 64
        errors = check_wig_sources(sources)
        self.assertTrue(any("哈希" in message for message in errors))


class FakeGroupReviewer:
    """Adapter mirroring WigGroupQaReviewer but over the fake vision."""

    def __init__(self, vision):
        self.vision = vision

    def review_group(self, *, persona_reference_path, sources, plan_item):
        return self.vision.review_wig_group(
            persona_image_path=persona_reference_path,
            sources=sources, plan_item=plan_item)


def prep(supply, record_id, plan_item, persona):
    return supply.prepare(record_id=record_id, plan_item=plan_item,
                          persona=persona)["sources"]


class WigDispatchProtection(unittest.TestCase):
    """MX never captures TH traffic, and TH validation keeps its exact shape."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.recipe_payload = json.loads(MX_RECIPE_PATH.read_text())

    def test_recipe_and_request_predicates(self):
        recipe = load_content_recipe_file(MX_RECIPE_PATH)
        self.assertTrue(recipe_is_mx_wig_choice(recipe))
        th_recipe = next(r for r in __import__("config.loader", fromlist=["load_content_recipes"])
                         .load_content_recipes() if r.recipe_id == "PHOTO_TH_TRAVEL_OUTFIT_V2")
        self.assertFalse(recipe_is_mx_wig_choice(th_recipe))
        mx_v1 = next(r for r in __import__("config.loader", fromlist=["load_content_recipes"])
                     .load_content_recipes() if r.recipe_id == "PHOTO_MX_PICK_YOUR_HAIR_V1")
        self.assertFalse(recipe_is_mx_wig_choice(mx_v1),
                         "old MX V1 rows must keep their original path")
        self.assertFalse(request_is_mx_wig_choice({"recipe_id": MX_WIG_RECIPE_ID}))

    def test_theme_dispatch_keeps_travel_out_of_mx(self):
        self.assertEqual(resolve_mx_wig_theme("")["theme_key"], MX_WIG_DEFAULT_THEME["theme_key"])
        self.assertEqual(resolve_mx_wig_theme("自动")["theme_key"], MX_WIG_DEFAULT_THEME["theme_key"])
        with self.assertRaises(PhotoWigPlannerError):
            resolve_mx_wig_theme("清迈周末旅行")
        with self.assertRaises(PhotoWigPlannerError):
            resolve_mx_wig_theme("凉爽旅行")

    def test_plan_validator_allows_four_only_for_the_explicit_flow(self):
        plan_item = wig_item(1, self.recipe_payload)
        repo = WigBatchRepo(self.root)
        # Build a real MX request through the factory dispatch.
        supply = PhotoWigSupplyService(generator=FakeWigGenerator(self.root / "g"),
                                       root=self.root)
        prepared = supply.prepare(record_id="mxdisp", plan_item=plan_item,
                                  persona=persona_snapshot(self.root))
        asset_set = supply.register_asset_set(
            repository=repo, record_id="mxdisp", sources=prepared["sources"],
            plan_item=plan_item, persona=persona_snapshot(self.root))
        spec = SimpleNamespace(account_id="OPV_MX_PHOTO_001", market="MX",
                               language="es-MX", recipe_id=MX_WIG_RECIPE_ID,
                               theme_id="", hook_strategy="", persona_ref="",
                               look_ref="", scene_ref="")
        factory = PhotoRequestFactory(repo, layouts=load_board_layouts())
        requests = factory.build_batch(
            record_id="mxrec2", specs=[spec], category_key="wig",
            product_mode="NO_PRODUCT",
            overrides=[{"asset_set_id": asset_set.asset_set_id,
                        "wig_plan_item": plan_item}])
        self.assertEqual(len(requests), 1)
        request = requests[0]
        validate_frozen_request(request)
        plan = {
            "schema_version": "opv-photo-plan-v1", "workflow_version": 2,
            "media_kind": "native_photo", "category_key": "wig",
            "product_mode": "NO_PRODUCT", "execution_flow": MX_WIG_CHOICE_FLOW,
            "market_pack": {"id": "MP_MX_DEFAULT_V1", "version": 1,
                            "country": "MX", "locale": "es-MX"},
            "recipe": {"id": MX_WIG_RECIPE_ID, "version": 1},
            "template": {"id": "PHOTO_MX_HAIR_CARD_V1", "version": 1},
            "variables": {"choice_axis": "style"}, "cover_index": 1,
            "copy": request["copy"], "product": None,
            "slides": [
                {"slot_index": index, "slot_role": role, "source_kind": "reused_asset",
                 "source_refs": [f"a{index}"], "source_slots": [index],
                 "overlay_text": request["copy"]["slide_texts"][index - 1],
                 "layout_snapshot": {"template_id": "PHOTO_MX_HAIR_CARD_V1",
                                     "template_version": 1, "layout": "single"}}
                for index, role in enumerate(
                    ["hair_a", "hair_b", "hair_c", "hair_d"], 1)
            ],
            "shots": [
                {"slot_index": index, "slot_role": role, "shot_kind": "reused_asset",
                 "asset_path": f"/tmp/{role}.png", "asset_sha256": "0" * 64,
                 "source_asset_id": f"a{index}"}
                for index, role in enumerate(
                    ["hair_a", "hair_b", "hair_c", "hair_d"], 1)
            ],
            "asset_set": {"id": asset_set.asset_set_id, "key": "MX_WIG_CHOICE_GEN",
                          "version": 1},
            "source_binding": "roles-v1",
            "source_roles": ["hair_a", "hair_b", "hair_c", "hair_d"],
            "production_policy": {"mode": "ASSET_REUSE", "ai_image_calls": 0},
        }
        self.assertEqual(validate_photo_plan_payload(plan), [])
        stripped = dict(plan)
        stripped.pop("execution_flow")
        errors = validate_photo_plan_payload(stripped)
        self.assertTrue(any("exactly 5" in message for message in errors),
                         "four-page plans without the explicit flow stay rejected")

    def test_tampered_mx_request_fails_fingerprint(self):
        plan_item = wig_item(1, self.recipe_payload)
        repo = WigBatchRepo(self.root)
        supply = PhotoWigSupplyService(generator=FakeWigGenerator(self.root / "g2"),
                                       root=self.root)
        prepared = supply.prepare(record_id="mxtamper", plan_item=plan_item,
                                  persona=persona_snapshot(self.root))
        asset_set = supply.register_asset_set(
            repository=repo, record_id="mxtamper", sources=prepared["sources"],
            plan_item=plan_item, persona=persona_snapshot(self.root))
        spec = SimpleNamespace(account_id="OPV_MX_PHOTO_001", market="MX",
                               language="es-MX", recipe_id=MX_WIG_RECIPE_ID,
                               theme_id="", hook_strategy="", persona_ref="",
                               look_ref="", scene_ref="")
        factory = PhotoRequestFactory(repo, layouts=load_board_layouts())
        request = factory.build_batch(
            record_id="mxrec3", specs=[spec], category_key="wig",
            product_mode="NO_PRODUCT",
            overrides=[{"asset_set_id": asset_set.asset_set_id,
                        "wig_plan_item": plan_item}])[0]
        tampered = copy.deepcopy(dict(request))
        tampered["copy"]["title"] = "Otro título completamente distinto"
        with self.assertRaises(Exception) as ctx:
            validate_frozen_request(tampered)
        self.assertIn("fingerprint", str(ctx.exception))

    def test_plan_item_mismatch_with_asset_evidence_rejected(self):
        plan_item = wig_item(1, self.recipe_payload)
        repo = WigBatchRepo(self.root)
        supply = PhotoWigSupplyService(generator=FakeWigGenerator(self.root / "g3"),
                                       root=self.root)
        prepared = supply.prepare(record_id="mxmismatch", plan_item=plan_item,
                                  persona=persona_snapshot(self.root))
        asset_set = supply.register_asset_set(
            repository=repo, record_id="mxmismatch", sources=prepared["sources"],
            plan_item=plan_item, persona=persona_snapshot(self.root))
        other_item = wig_item(2, self.recipe_payload)
        spec = SimpleNamespace(account_id="OPV_MX_PHOTO_001", market="MX",
                               language="es-MX", recipe_id=MX_WIG_RECIPE_ID,
                               theme_id="", hook_strategy="", persona_ref="",
                               look_ref="", scene_ref="")
        factory = PhotoRequestFactory(repo, layouts=load_board_layouts())
        with self.assertRaises(Exception) as ctx:
            factory.build_batch(
                record_id="mxrec4", specs=[spec], category_key="wig",
                product_mode="NO_PRODUCT",
                overrides=[{"asset_set_id": asset_set.asset_set_id,
                            "wig_plan_item": other_item}])
        self.assertIn("证据不一致", str(ctx.exception))

    def test_registration_is_idempotent_and_never_overwrites_versions(self):
        """Regression: opv_asset_set has UNIQUE(asset_set_key, asset_set_version).

        Registering two different source sets under MX_WIG_CHOICE_GEN must
        create two rows (v1, v2); re-registering the first must return the
        original row instead of overwriting anything.
        """
        repo = WigBatchRepo(self.root)
        supply = PhotoWigSupplyService(generator=FakeWigGenerator(self.root / "gv"),
                                       root=self.root)
        persona = persona_snapshot(self.root)
        item_a = wig_item(1, self.recipe_payload)
        item_b = wig_item(2, self.recipe_payload)
        first = supply.register_asset_set(
            repository=repo, record_id="mxver1", sources=prep(supply, "mxver1", item_a, persona),
            plan_item=item_a, persona=persona)
        second = supply.register_asset_set(
            repository=repo, record_id="mxver2", sources=prep(supply, "mxver2", item_b, persona),
            plan_item=item_b, persona=persona)
        self.assertNotEqual(first.asset_set_id, second.asset_set_id)
        self.assertEqual(second.asset_set_version, first.asset_set_version + 1)
        rows = {s.asset_set_id: s for s in repo.list_asset_sets(category_key="wig", market="MX")}
        self.assertIn(first.asset_set_id, rows)
        self.assertIn(second.asset_set_id, rows)
        again = supply.register_asset_set(
            repository=repo, record_id="mxver1-again",
            sources=prep(supply, "mxver1", item_a, persona),
            plan_item=item_a, persona=persona)
        self.assertEqual(again.asset_set_id, first.asset_set_id)
        self.assertEqual(rows[first.asset_set_id].manifest_json, again.manifest_json)

    def test_old_mx_choice_asset_cannot_pass_as_four_options(self):
        """The legacy ASSET_MX_WIG_CHOICE_V2 shape (duplicate labels, cover_seed,
        no qualification) is not acceptable evidence for the new flow."""
        old_asset_path = (PACKAGE_ROOT / "config" / "asset_sets" /
                          "ASSET_MX_WIG_CHOICE_V2.json")
        payload = json.loads(old_asset_path.read_text())
        assets = []
        for index, item in enumerate(payload.get("assets") or [], 1):
            path = self.root / f"old-{index}.png"
            Image.new("RGB", (100, 178), (10 * index, 20, 30)).save(path)
            assets.append({
                "asset_id": item["asset_id"], "role": item["role"],
                "path": str(path), "sha256": _sha256(path),
                "display_label": {"es-MX": "Pink Glow Long Curls"},
            })
        asset_set = SimpleNamespace(
            asset_set_id=payload["asset_set_id"],
            asset_set_key=payload.get("asset_set_key") or "MX_WIG_CHOICE",
            asset_set_version=1, category_key="wig", market="MX",
            status="enabled",
            manifest_json={"assets": assets, "pairs": []},
        )
        recipe = load_content_recipe_file(MX_RECIPE_PATH)
        card = recipe.recipe_spec_json["content_card"]
        with self.assertRaises(PhotoWigPlannerError):
            freeze_wig_content_card(
                card, asset_set, recipe.recipe_spec_json["visual_rules"])

    def test_mx_publish_routing_targets_mxjf01(self):
        payload = json.loads((PACKAGE_ROOT / "config" / "main_publish_routes.json")
                             .read_text())
        routes = payload.get("routes") or payload
        self.assertEqual(routes["MX"]["default_store_id"], "MXJF01")
        from services.main_schedule_bridge import MainScheduleBridge
        bridge = MainScheduleBridge.__new__(MainScheduleBridge)
        bridge.routes = routes
        self.assertEqual(bridge._store_id("MX"), "MXJF01")

    def test_group_qa_normalizer_is_strict(self):
        with self.assertRaises(PhotoWigQaError):
            normalize_wig_group_qa({"roles": [{"role": "hair_a", "passed": "true"}]},
                                   roles=["hair_a", "hair_b", "hair_c", "hair_d"])
        with self.assertRaises(PhotoWigQaError):
            normalize_wig_group_qa({"roles": []},
                                   roles=["hair_a", "hair_b", "hair_c", "hair_d"])


if __name__ == "__main__":
    unittest.main()
