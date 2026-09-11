"""Render the first VIEWABLE MX wig four-page piece from real generated images.

Offline test artifact only — never touches Feishu, RDS or the publish queue.
Pages 1/4 reuse the two Phase-0 persona samples; pages 2/3 are generated here
through the production channel (paid).  The full real chain runs locally:
real intake, real four-page planner dispatch, real reuse producer, real
``PhotoPackageExporter`` with ``PHOTO_MX_HAIR_CARD_V1``.

Run (from packages/organic_photo_video):
  /usr/bin/python3 scripts/render_mx_wig_first_piece.py
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
sys.path.insert(0, str(WORKSPACE_ROOT))
from workspace_support import load_repo_env  # noqa: E402

load_repo_env()
# PACKAGE_ROOT must stay ahead of WORKSPACE_ROOT so "config"/"services"
# resolve to this package, not to workspace-root lookalikes.
sys.path.insert(0, str(PACKAGE_ROOT))

# Production channel parity with the locked scanner.
os.environ.setdefault("OPV_PHOTO_CHANNEL", "openai-image")
os.environ.setdefault("OPENAI_CODEX_IMAGE_MODEL", "gpt-image-2.5-sunburst")
os.environ.setdefault("OPENAI_IMAGE_TOTAL_TIMEOUT", "240")
os.environ.setdefault("OPENAI_IMAGE_FIRST_EVENT_TIMEOUT", "120")

from domain.models import ContentPackage  # noqa: E402
from services.asset_resolver import LightTryonAssetReader  # noqa: E402
from services.content_package import ContentPackageService  # noqa: E402
from services.hero_first import HeroFirstProducer  # noqa: E402
from services.image_generator import (  # noqa: E402
    OpenAIImageGenerator, ShotGenerationRequest, read_image_dimensions,
)
from services.photo_package import NativePhotoProductionFlow  # noqa: E402
from services.photo_planner import PhotoReusePlannerService  # noqa: E402
from services.photo_request_factory import fingerprint  # noqa: E402
from services.photo_wig_supply import WIG_PROMPT_VERSION, compose_wig_prompt  # noqa: E402
from services.photo_wig_planner import plan_from_items  # noqa: E402
from services.task_intake import TaskIntakeService, TaskRequest  # noqa: E402
from services.workflow_v2 import RevisionService  # noqa: E402

PERSONA_REF_ID = "MX_WIG_CAST_A_001"
OUT_DIR = Path.home() / "output/imagegen/mx-wig-first-piece-v1"
SAMPLES_DIR = Path.home() / "output/imagegen/mx-wig-persona-samples-v1"

PLAN_ITEM = {
    "item_index": 1,
    "topic_zh": "周末换个发型，你选哪款？",
    "photography_direction_zh": "室内窗边柔和自然光，暖色简单墙面轻虚化，胸像/半身取景",
    "wardrobe_direction_zh": "奶油白色简单上衣，与头发形成明暗对比",
    "makeup_direction_zh": "自然精致日常妆，不过度磨皮",
    "scene_prompt_zh": "室内窗边柔和自然光，背景简洁虚化",
    "reference_uses": [{"image_index": 1, "uses": ["persona"]}],
    "options": [
        {"role": "hair_a", "label_es": "Bob corto", "label_zh": "利落短Bob",
         "length": "chin_bob", "texture": "straight", "color": "dark_brown",
         "parting": "center", "silhouette": "sleek_bell", "framing": "chest_up"},
        {"role": "hair_b", "label_es": "Bob a la clavícula", "label_zh": "锁骨柔和弧度",
         "length": "collarbone", "texture": "soft_wave", "color": "dark_brown",
         "parting": "center", "silhouette": "soft_arc", "framing": "chest_up"},
        {"role": "hair_c", "label_es": "Largo liso", "label_zh": "长直发",
         "length": "long", "texture": "straight", "color": "dark_brown",
         "parting": "center", "silhouette": "sleek_long", "framing": "waist_up"},
        {"role": "hair_d", "label_es": "Ondas largas", "label_zh": "长波浪",
         "length": "long", "texture": "soft_wave", "color": "warm_brown",
         "parting": "center", "silhouette": "voluminous_wave", "framing": "waist_up"},
    ],
}


def load_seed_copy() -> dict:
    recipe = json.loads(
        (PACKAGE_ROOT / "config" / "recipes" / "PHOTO_MX_PICK_YOUR_HAIR_V2.json")
        .read_text())
    return copy.deepcopy(
        recipe["recipe_spec"]["execution_profiles"][0]["copy_variants"][0]["copy"])


def generate_missing(generator: OpenAIImageGenerator, persona: dict,
                     out_dir: Path) -> list:
    """Generate hair_b and hair_c via the production channel."""
    persona_paths = [
        str(item.get("local_path"))
        for item in persona.get("reference_items") or []
        if item.get("approved", True)
    ]
    reference_path = persona_paths[0]
    records = []
    for option in PLAN_ITEM["options"]:
        if option["role"] in {"hair_b", "hair_c"}:
            prompt = compose_wig_prompt(
                option, PLAN_ITEM, persona=persona,
                reference_roles={"persona_identity_images": [reference_path]},
                ordered_reference_paths=[reference_path],
            )
            request = ShotGenerationRequest(
                task_id=f"mx_first_piece_{option['role']}", slot_index=1,
                slot_role=option["role"], shot_version=1,
                plan_shot={"slot_index": 1, "slot_role": option["role"]},
                product={}, persona_snapshot=persona,
                look_snapshot={}, scene_snapshot={},
                output_dir=str(out_dir),
                continuity_reference_images=[reference_path],
                reference_roles={"persona_identity_images": [reference_path]},
                prompt_override=prompt,
            )
            started = time.time()
            outcome = generator.generate_shot(request)
            if not outcome.ok or not outcome.image_path:
                raise SystemExit(
                    f"{option['role']} generation failed: {outcome.error}")
            records.append({
                "role": option["role"],
                "path": str(Path(outcome.image_path).resolve()),
                "sha256": hashlib.sha256(
                    Path(outcome.image_path).read_bytes()).hexdigest(),
                "provider": outcome.provider, "model": outcome.model,
                "request_id": outcome.request_id,
                "width": outcome.width, "height": outcome.height,
                "elapsed_seconds": round(time.time() - started, 1),
                "prompt": prompt, "prompt_version": WIG_PROMPT_VERSION,
                "ordered_reference_paths": [reference_path],
            })
    return records


class MiniRepo:
    """Just enough repository for one offline intake→plan→produce→export run."""

    def __init__(self, recipe):
        self.recipe = recipe
        self.recipes = {recipe.recipe_id: recipe}
        self.tasks: dict = {}
        self.packages: dict = {}
        self.revisions: dict = {}
        self.asset_sets: list = []
        self.pack = SimpleNamespace(
            market_pack_id="MP_MX_DEFAULT_V1", pack_version=1,
            target_country="MX", target_locale="es-MX", status="active")
        self.account = SimpleNamespace(
            account_id="OPV_MX_PHOTO_001", status="testing",
            default_market_pack_id="MP_MX_DEFAULT_V1", default_render_preset_id=None,
            persona_ref_id=PERSONA_REF_ID, allowed_look_refs_json=[],
            allowed_scene_refs_json=[], core_scene_refs_json=[],
            operating_rules_json={"category_key": "wig"})

    def get_content_recipe(self, recipe_id):
        return self.recipes.get(recipe_id)

    def get_market_pack(self, pack_id):
        return self.pack if pack_id == self.pack.market_pack_id else None

    def get_account_profile(self, account_id):
        return self.account if account_id == self.account.account_id else None

    def get_render_preset(self, _preset_id):
        return None

    def create_task_idempotent(self, task):
        for existing in self.tasks.values():
            if existing.idempotency_key == task.idempotency_key:
                return existing, False
        self.tasks[task.task_id] = task
        return task, True

    def get_task(self, task_id):
        return self.tasks.get(task_id)

    def list_tasks_by_source_prefix(self, _source_type, prefix):
        return [t for t in self.tasks.values()
                if str(t.source_record_id or "").startswith(prefix)]

    def update_task_plan(self, task_id, **fields):
        task = self.tasks[task_id]
        for key, value in fields.items():
            setattr(task, key, copy.deepcopy(value))

    def transition_task(self, task_id, from_status, to_status, **kwargs):
        from domain import statuses
        task = self.tasks[task_id]
        assert task.task_status == from_status, (task.task_status, from_status)
        statuses.task_ensure_transition(from_status, to_status)
        task.task_status = to_status
        return task

    def get_content_package(self, package_id):
        return self.packages.get(package_id)

    def get_content_package_by_task(self, task_id):
        return next((p for p in self.packages.values() if p.task_id == task_id), None)

    def insert_content_package(self, package):
        self.packages[package.content_package_id] = package

    def update_content_package(self, package_id, **fields):
        for key, value in fields.items():
            setattr(self.packages[package_id], key, copy.deepcopy(value))

    def get_task_revision(self, revision_id):
        return self.revisions.get(revision_id)

    def list_task_revisions(self, task_id):
        return [r for r in self.revisions.values() if r.task_id == task_id]

    def create_revision_and_activate(self, revision, *, expected_task_row_version,
                                     transition_to=None):
        self.revisions[revision.revision_id] = revision
        task = self.tasks[revision.task_id]
        task.active_revision_id = revision.revision_id
        task.workflow_version = 2
        task.row_version += 1
        return task

    def update_revision_manifest(self, revision_id, *, expected_lock_version,
                                 asset_manifest_json, selection_hash):
        revision = self.revisions[revision_id]
        assert expected_lock_version == revision.lock_version
        revision.asset_manifest_json = copy.deepcopy(asset_manifest_json)
        revision.selection_hash = selection_hash
        revision.lock_version += 1
        return revision

    def list_shots(self, task_id):
        return []

    def insert_shot(self, shot):
        pass

    def update_shot_status(self, *_args, **_kwargs):
        pass

    def update_shot_generation_result(self, *_args, **_kwargs):
        pass

    def update_shot_qa(self, *_args, **_kwargs):
        pass

    def set_shot_selected(self, *_args, **_kwargs):
        pass

    def upsert_asset_set(self, asset_set):
        self.asset_sets.append(asset_set)

    def get_asset_set(self, asset_set_id):
        return next((a for a in self.asset_sets
                     if a.asset_set_id == asset_set_id), None)

    def list_asset_sets(self, **_kwargs):
        return list(self.asset_sets)

    def list_photo_content_signatures(self, exclude_record_id=None):
        return set()


def main() -> int:
    from config.loader import load_board_layouts, load_content_recipe_file

    out_dir = OUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    persona = LightTryonAssetReader().get_persona(PERSONA_REF_ID)
    print(f"persona: {persona['persona_id']} "
          f"ref_sha={persona['reference_items'][0]['sha256'][:16]}…")

    generation_meta = []
    paths = {}
    sample_bob = SAMPLES_DIR / "mx_wig_persona_sample_short_bob_P1_v1_1.png"
    sample_waves = SAMPLES_DIR / "mx_wig_persona_sample_long_waves_P1_v1_1.png"
    if not sample_bob.is_file() or not sample_waves.is_file():
        raise SystemExit(f"Phase-0 samples missing under {SAMPLES_DIR}")
    paths["hair_a"] = sample_bob
    paths["hair_d"] = sample_waves
    if ((out_dir / "mx_first_piece_hair_b_P1_v1_1.png").is_file()
            and (out_dir / "mx_first_piece_hair_c_P1_v1_1.png").is_file()):
        paths["hair_b"] = out_dir / "mx_first_piece_hair_b_P1_v1_1.png"
        paths["hair_c"] = out_dir / "mx_first_piece_hair_c_P1_v1_1.png"
        meta_path = out_dir / "generation_meta.json"
        if meta_path.is_file():
            generation_meta = json.loads(meta_path.read_text(encoding="utf-8"))
        print("hair_b/hair_c already generated; reusing")
    else:
        generator = OpenAIImageGenerator()
        generation_meta = generate_missing(generator, persona, out_dir)
        for record in generation_meta:
            paths[record["role"]] = Path(record["path"])
        (out_dir / "generation_meta.json").write_text(
            json.dumps(generation_meta, ensure_ascii=False, indent=1),
            encoding="utf-8")

    plan_item = copy.deepcopy(PLAN_ITEM)
    plan_item["copy"] = load_seed_copy()
    plan = plan_from_items([plan_item])
    print("plan copy:", plan["items"][0]["copy"]["slide_texts"])

    recipe = load_content_recipe_file(
        PACKAGE_ROOT / "config" / "recipes" / "PHOTO_MX_PICK_YOUR_HAIR_V2.json")
    spec_json = recipe.recipe_spec_json
    repo = MiniRepo(recipe)

    # Qualify the four real images into one asset set (real registration).
    from services.asset_set_service import AssetSetService
    from services.photo_wig_supply import _hash_payload

    assets = []
    approval_attributes = {}
    source_hashes = {}
    identity_id = "mx_wig_identity_" + persona[
        "reference_items"][0]["sha256"][:16]
    from services.photo_wig_flow import MX_WIG_SOURCE_ROLES

    for role in MX_WIG_SOURCE_ROLES:
        path = paths[role]
        option = next(o for o in plan_item["options"] if o["role"] == role)
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        asset_id = f"mx_first_piece_{role}_{digest[:10]}"
        source_hashes[asset_id] = digest
        approval_attributes[asset_id] = {
            "identity_id": identity_id,
            "camera_scale": "portrait_half_body",
            "length": option["length"], "texture": option["texture"],
            "color": option["color"], "parting": option["parting"],
            "silhouette": option["silhouette"], "hairline_visible": True,
            "label_es": option["label_es"],
            "planned_option_hash": _hash_payload(option),
        }
        assets.append({
            "asset_id": asset_id, "role": role, "path": str(path),
            "sha256": digest,
            "tags": {"length": option["length"], "texture": option["texture"]},
            "display_label": {"es-MX": option["label_es"], "zh-CN": option["label_zh"]},
            "content_plan": {"item_id": "mx_first_piece",
                             "look_signature": _hash_payload(option),
                             "attributes": approval_attributes[asset_id]},
        })
    from domain.models import AssetSet as AssetSetModel
    asset_set = AssetSetModel(
        asset_set_id="ASSET_MX_WIG_CHOICE_GEN_" + hashlib.sha256(
            ":".join(source_hashes.values()).encode()).hexdigest()[:16],
        asset_set_key="MX_WIG_CHOICE_GEN", asset_set_version=1,
        category_key="wig", market="MX", status="enabled",
        tags_json={"use_cases": "pick_your_hair", "choice_axis": "style",
                   "source": "mx_first_piece_offline"},
        manifest_json={"assets": assets, "pairs": [], "content_approval": {
            "schema_version": "opv-source-qualification-v1",
            "reviewer": "offline_first_piece", "reviewer_type": "technical",
            "allowed_logic_keys": ["wig_four_choice_es"],
            "source_hashes": source_hashes,
            "attributes": approval_attributes,
        }})
    saved = AssetSetService(repo).save(asset_set)
    print("asset set:", saved.asset_set_id)

    # Freeze the request through the real factory dispatch.
    from services.photo_request_factory import PhotoRequestFactory
    from services.photo_wig_flow import MX_WIG_RECIPE_ID
    layouts = load_board_layouts()
    spec = SimpleNamespace(account_id="OPV_MX_PHOTO_001", market="MX",
                           language="es-MX", recipe_id=MX_WIG_RECIPE_ID,
                           theme_id="", hook_strategy="", persona_ref="",
                           look_ref="", scene_ref="")
    request = PhotoRequestFactory(repo, layouts=layouts).build_batch(
        record_id="mx_first_piece", specs=[spec], category_key="wig",
        product_mode="NO_PRODUCT",
        overrides=[{"asset_set_id": saved.asset_set_id,
                    "wig_plan_item": plan_item}])[0]
    print("request frozen:", request["request_sha256"][:16], "…")

    # Real intake → real four-page planner → real producer → real exporter.
    from services.feishu_workflow import SOURCE_TYPE
    from services.photo_package import PhotoPackageExporter
    task = TaskIntakeService(repo).create_task(TaskRequest(
        account_id="OPV_MX_PHOTO_001", product_id=None, product_snapshot={},
        media_kind="native_photo", category_key="wig", product_mode="NO_PRODUCT",
        source_type=SOURCE_TYPE, source_record_id="mx_first_piece:1:PHOTO_MX_PICK_YOUR_HAIR_V2:1",
        feishu_record_id="OFFLINE_TEST", requested_shot_count=4,
        created_by="mx_first_piece_offline",
        idempotency_key="mx_first_piece:1")).task
    PhotoReusePlannerService(repo).plan_task(
        task.task_id, recipe_id=request["recipe_id"], variables=request["variables"],
        copy_block=request["copy"], layout=request["layout_snapshot"],
        asset_set_id=request["asset_set_id"],
        recipe_snapshot=request["recipe_snapshot"],
        asset_snapshot=request["asset_snapshot"],
        execution_profile_id=request["profile_id"],
        copy_variant_id=request["copy_variant_id"],
        content_card=request.get("content_card"),
        theme_brief=None, operator="mx_first_piece_offline")
    package_root = out_dir / "package"
    producer = HeroFirstProducer(repo, object(), output_root=package_root,
                                 technical_only=True)
    exporter = PhotoPackageExporter(repo, output_root=package_root)
    flow = NativePhotoProductionFlow(repo, producer, output_root=package_root)
    result = flow.prepare(task.task_id, template=request["layout_snapshot"])
    manifest = result["photo_manifest"]
    final_dir = out_dir / "final"
    final_dir.mkdir(parents=True, exist_ok=True)
    copied = []
    for slide in manifest["slides"]:
        target = final_dir / f"{slide['index']:02d}.jpg"
        target.write_bytes(Path(slide["path"]).read_bytes())
        copied.append(str(target))
    (out_dir / "plan.json").write_text(
        json.dumps(plan, ensure_ascii=False, indent=1), encoding="utf-8")
    (out_dir / "frozen_request.json").write_text(
        json.dumps(request, ensure_ascii=False, indent=1), encoding="utf-8")
    (out_dir / "package_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")
    print("FINAL PAGES:")
    for path in copied:
        print(" -", path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
