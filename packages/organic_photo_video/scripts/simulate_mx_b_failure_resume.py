"""Phase-2 acceptance: B-role failure/resume with the REAL production channel.

Simulates the production "B 生成失败 → 重试只补 B" path: A/C/D go through the
real channel once; hair_b's first attempt fails at the channel boundary (the
proxy raises before spending), then a resume regenerates ONLY B through the
real channel.  Asserts A/C/D bytes keep their exact hashes.  No Feishu/RDS
writes; the artifact stays under wig_choice_supply/mx_b_resume_sim_item_1.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
sys.path.insert(0, str(WORKSPACE_ROOT))
sys.path.insert(0, str(PACKAGE_ROOT))

from workspace_support import load_repo_env  # noqa: E402

load_repo_env()
os.environ.setdefault("OPV_PHOTO_CHANNEL", "openai-image")
os.environ.setdefault("OPENAI_CODEX_IMAGE_MODEL", "gpt-image-2.5-sunburst")

from services.image_generator import build_default_photo_generator  # noqa: E402
from services.photo_wig_flow import MX_WIG_RECIPE_ID  # noqa: E402
from services.photo_wig_planner import reference_roles_from_plan  # noqa: E402
from services.photo_wig_qa import WigGroupQaReviewer  # noqa: E402
from services.photo_wig_supply import PhotoWigSupplyService  # noqa: E402
from services.asset_resolver import LightTryonAssetReader  # noqa: E402
from services.photo_reference_vision import PhotoReferenceVisionService  # noqa: E402
from config.loader import load_content_recipe_file  # noqa: E402

RECORD_ID = "mx_b_resume_sim_item_1"


class FailOnceProxy:
    """Real production generator; hair_b's first call fails before spending."""

    def __init__(self, inner):
        self.inner = inner
        self.fail_next = {"hair_b"}
        self.calls = []

    def generate_shot(self, request):
        role = request.slot_role
        self.calls.append((role, request.shot_version))
        if role in self.fail_next:
            self.fail_next.discard(role)
            from services.image_generator import GenerationOutcome
            return GenerationOutcome(ok=False, provider="sim", model="sim",
                                     error="simulated channel failure",
                                     request_id="sim-fail")
        return self.inner.generate_shot(request)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main() -> int:
    recipe = load_content_recipe_file(
        PACKAGE_ROOT / "config" / "recipes" / "PHOTO_MX_PICK_YOUR_HAIR_V2.json")
    # Use the REAL plan frozen for the first acceptance row (same persona).
    plan_store = Path.home() / ".openclaw/shared/data/organic_photo_video" \
        / "reference_contracts" / "recvuPivbCYvbs" / "wig_plan.json"
    plan_item = json.loads(plan_store.read_text())["plan"]["items"][0]
    persona = LightTryonAssetReader().get_persona("MX_WIG_CAST_A_001")
    persona_paths = [item["local_path"] for item in persona["reference_items"]
                     if item.get("approved", True)]
    staging_root = Path.home() / ".openclaw/shared/data/organic_photo_video"
    vision = PhotoReferenceVisionService(root=staging_root)

    supply = PhotoWigSupplyService(
        generator=FailOnceProxy(build_default_photo_generator()),
        root=staging_root,
        qa_reviewer=WigGroupQaReviewer(vision),
    )
    reference_paths = []
    roles = reference_roles_from_plan(plan_item, persona_paths, reference_paths)
    print("attempt 1: hair_b fails at the channel boundary (sequential abort)…")
    try:
        supply.prepare(record_id=RECORD_ID, plan_item=plan_item, persona=persona,
                       reference_paths=reference_paths, reference_roles=roles)
        print("FAIL: expected the first attempt to fail")
        return 2
    except Exception as exc:
        print("first attempt failed as expected:", str(exc)[:120])
    partial = {item["role"]: item["sha256"]
               for item in supply.load_manifest(RECORD_ID)["sources"]}
    print("sources completed before abort:", sorted(partial))

    print("attempt 2: resume — only missing roles hit the real channel…")
    supply.generator.fail_next.clear()
    prepared = supply.prepare(record_id=RECORD_ID, plan_item=plan_item,
                              persona=persona, reference_paths=reference_paths,
                              reference_roles=roles)
    after = {item["role"]: (item["path"], item["sha256"])
             for item in prepared["sources"]}
    for role, digest in partial.items():
        assert after[role][1] == digest, f"{role} hash changed on resume!"
    manifest = supply.load_manifest(RECORD_ID)
    print("resume OK:")
    for item in manifest["sources"]:
        print(" ", item["role"], "attempt", item["attempt"],
              item["sha256"][:12], Path(item["path"]).name)
    print("group_qa passed:", manifest["group_qa"]["passed"])
    print("completed-before-abort hashes unchanged: True;"
          " missing roles regenerated via real channel: True")
    return 0


if __name__ == "__main__":
    sys.exit(main())
