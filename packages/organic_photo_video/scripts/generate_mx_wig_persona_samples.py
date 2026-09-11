"""Phase 0 gate: generate two persona-A hairstyle samples via the PRODUCTION channel.

Per docs/MX_WIG_PHOTO_MVP_HANDOFF_20260910.md §2.1: use persona A's cast photo
as the identity reference on the same image channel production will use, and
produce (1) a chin-length bob and (2) long waves with visible ends.  Light,
wardrobe and makeup stay close to the cast photo; the hair is the only variable.

This is a manual, paid, one-shot validation script.  It never touches Feishu,
the database or any frozen task, and it does not register production assets.

Run (from packages/organic_photo_video):
  /usr/bin/python3 scripts/generate_mx_wig_persona_samples.py [--out DIR]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

# Production channel parity with scripts/run_feishu_scanner_locked.sh.
# （生图固定走 codex.auth；Sunburst 把第一张参考规范到 9:16 画布。）
os.environ.setdefault("OPV_PHOTO_CHANNEL", "openai-image")
os.environ.setdefault("OPENAI_CODEX_IMAGE_MODEL", "gpt-image-2.5-sunburst")
os.environ.setdefault("OPENAI_IMAGE_TOTAL_TIMEOUT", "240")
os.environ.setdefault("OPENAI_IMAGE_FIRST_EVENT_TIMEOUT", "120")

from services.asset_resolver import LightTryonAssetReader  # noqa: E402
from services.image_generator import OpenAIImageGenerator, ShotGenerationRequest  # noqa: E402
from services.photo_wig_supply import WIG_PROMPT_VERSION, compose_wig_prompt  # noqa: E402
from services.persona_pack import select_identity_references  # noqa: E402

PERSONA_REF_ID = "MX_WIG_CAST_A_001"
DEFAULT_OUT = Path.home() / "output/imagegen/mx-wig-persona-samples-v1"

SAMPLES = [
    {
        "sample_id": "short_bob",
        "zh_name": "下颌附近短Bob",
        "option": {
            "role": "sample_short_bob", "label_es": "Bob corto",
            "length": "chin_bob", "length_zh": "下颌附近的经典短 Bob，发尾自然内扣收拢",
            "texture": "straight", "texture_zh": "整体顺直、发尾轻微内扣",
            "color_zh": "与参考图一致的深棕色",
            "parting_zh": "自然中分",
            "silhouette_zh": "利落干净的钟形轮廓，颈部线条清楚",
            "framing_zh": "胸部以上半身胸像，完整露出新发型轮廓",
            "style_zh": "利落短 Bob",
        },
    },
    {
        "sample_id": "long_waves",
        "zh_name": "看得清发尾的长波浪",
        "option": {
            "role": "sample_long_waves", "label_es": "Ondas largas",
            "length": "long", "length_zh": "长发过胸，发尾位置在画面中完整可见",
            "texture": "soft_wave", "texture_zh": "大而柔和的波浪，非小卷",
            "color_zh": "与参考图一致的深棕色，仅在发尾带轻微暖棕反光",
            "parting_zh": "自然中分或轻微偏分",
            "silhouette_zh": "蓬松柔顺的长波浪轮廓",
            "framing_zh": "半身取景（腰以上），保证发尾完整入画",
            "style_zh": "长波浪",
        },
    },
]

PLAN_ITEM = {
    "topic_zh": "周末换个发型（人物接入验证样片）",
    "wardrobe_direction_zh": "奶油白色简单上衣（与选角图一致）",
    "makeup_direction_zh": "自然精致日常妆（与选角图一致，不加重）",
    "scene_prompt_zh": "室内窗边柔和自然光，暖色简单墙面轻虚化（与选角图一致）",
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=str(DEFAULT_OUT))
    args = parser.parse_args()
    output_dir = Path(args.out).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)

    persona = LightTryonAssetReader().get_persona(PERSONA_REF_ID)
    identity = select_identity_references(persona)
    if not identity:
        print("FAIL: persona has no usable identity reference")
        return 2
    reference_path = str(identity[0]["local_path"])
    print(f"persona={persona['persona_id']} identity_ref={reference_path} "
          f"sha256={identity[0]['sha256'][:16]}…")

    generator = OpenAIImageGenerator()
    records = []
    for sample in SAMPLES:
        option = sample["option"]
        prompt = compose_wig_prompt(
            option, PLAN_ITEM, persona=persona,
            reference_roles={"persona_identity_images": [reference_path]},
            ordered_reference_paths=[reference_path],
        )
        request = ShotGenerationRequest(
            task_id=f"mx_wig_persona_sample_{sample['sample_id']}",
            slot_index=1, slot_role=str(option["role"]), shot_version=1,
            plan_shot={"slot_index": 1, "slot_role": str(option["role"])},
            product={}, persona_snapshot=persona,
            look_snapshot={}, scene_snapshot={},
            output_dir=str(output_dir),
            continuity_reference_images=[reference_path],
            reference_roles={"persona_identity_images": [reference_path]},
            prompt_override=prompt,
        )
        started = time.time()
        outcome = generator.generate_shot(request)
        record = {
            "sample_id": sample["sample_id"],
            "zh_name": sample["zh_name"],
            "ok": bool(outcome.ok),
            "image_path": outcome.image_path,
            "width": outcome.width, "height": outcome.height,
            "provider": outcome.provider, "model": outcome.model,
            "request_id": outcome.request_id,
            "error": outcome.error,
            "elapsed_seconds": round(time.time() - started, 1),
            "sha256": (hashlib.sha256(Path(outcome.image_path).read_bytes()).hexdigest()
                       if outcome.image_path and Path(outcome.image_path).is_file() else ""),
            "prompt": prompt,
            "prompt_version": WIG_PROMPT_VERSION,
            "ordered_reference_paths": [reference_path],
            "ordered_reference_sha256s": [identity[0]["sha256"]],
        }
        records.append(record)
        print(f"[{sample['sample_id']}] ok={record['ok']} "
              f"model={record['model']} {record['elapsed_seconds']}s "
              f"{record['image_path'] or record['error']}")

    manifest = {
        "schema_version": "opv-mx-wig-persona-samples-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "channel": {
            "OPV_PHOTO_CHANNEL": os.environ.get("OPV_PHOTO_CHANNEL"),
            "OPENAI_CODEX_IMAGE_MODEL": os.environ.get("OPENAI_CODEX_IMAGE_MODEL"),
        },
        "persona_ref_id": PERSONA_REF_ID,
        "records": records,
    }
    manifest_path = output_dir / "samples_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    print(f"manifest: {manifest_path}")
    failed = [item for item in records if not item["ok"]]
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
