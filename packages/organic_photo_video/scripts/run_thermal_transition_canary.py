#!/usr/bin/env python3
"""Build a local five-page TH daily thermal-transition canary from three photos.

The daily line answers "outside is hot / the office air-con is strong — how do
I dress for one day?" with three states of the *same* outfit:

    base  → outside heat        (breathable base layer, 1 visible layer)
    mid   → BTS / mall, cooler  (+ one removable light layer, 2 layers)
    outer → office, strong A/C  (+ one light structured layer, 3 layers)

No RDS or Feishu writes are performed.  By default visual observations are
requested from the configured vision route; ``--observation-json`` provides the
offline/reproducible path used by tests and by previously reviewed samples.

The three source photos must be real business assets.  Any procedurally
generated placeholder is a *structural* regression sample only and must never
be treated as approved content.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
from typing import Any, Mapping

from PIL import Image


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from config.loader import load_board_layouts, load_content_recipes  # noqa: E402
from services.photo_content_planner import load_planning_policy  # noqa: E402
from services.photo_package import (  # noqa: E402
    _compose, _draw_overlay, normalize_photo_template,
)
from services.photo_reference_vision import PhotoReferenceVisionService  # noqa: E402
from services.photo_theme import resolve_photo_theme  # noqa: E402
from services.photo_thermal_transition_flow import (  # noqa: E402
    build_thermal_transition_content_plan,
)
from services.photo_thermal_transition_qa import (  # noqa: E402
    evaluate_thermal_transition_qa, failed_roles_from_thermal_qa,
    thermal_transition_qa_markdown, thermal_transition_qa_note_zh,
)


RECIPE_ID = "PHOTO_TH_THERMAL_TRANSITION_V1"
THEME_LABEL = "冷热切换"
ROLE_ORDER = ("base", "mid", "outer")

# The single Phase 1 canary combination.  Anything else must fail loudly rather
# than silently render a neighbouring scene.
CANARY_VARIABLES = {
    "transition_key": "outdoor_bts_office",
    "thermal_sensitivity": "normal",
    "dress_code": "office",
    "style_series": "minimal_city",
    "temperature_label_mode": "QUALITATIVE",
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def build_canary_plan(
    *, record_id: str = "local-thermal-transition-canary",
    variables: Mapping[str, Any] | None = None,
) -> tuple[Any, dict[str, Any], dict[str, Any]]:
    """Return ``(recipe, recipe_spec, frozen_plan)`` for the canary."""
    recipe = next(
        item for item in load_content_recipes() if item.recipe_id == RECIPE_ID
    )
    spec = dict(recipe.recipe_spec_json or {})
    plan = build_thermal_transition_content_plan(
        record_id=record_id, recipe_id=RECIPE_ID, recipe_spec=spec,
        policy=load_planning_policy(RECIPE_ID),
        theme=resolve_photo_theme(THEME_LABEL), reference_mode="COMPLETE_LOOK",
        variables=dict(variables or CANARY_VARIABLES),
    )
    return recipe, spec, plan


def run_canary(
    *, base: Path, mid: Path, outer: Path, output_dir: Path,
    observation: Mapping[str, Any] | None = None,
    vision_service: Any = None,
    variables: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    sources = {
        role: Path(path).expanduser().resolve()
        for role, path in zip(ROLE_ORDER, (base, mid, outer))
    }
    for role, path in sources.items():
        if not path.is_file():
            raise ValueError(f"{role} 图片不存在：{path}")
        try:
            with Image.open(path) as image:
                image.verify()
        except Exception as exc:
            raise ValueError(f"{role} 不是可解码图片：{path}") from exc
    hashes = {role: _sha256(path) for role, path in sources.items()}
    if len(set(hashes.values())) != len(ROLE_ORDER):
        raise ValueError("base/mid/outer 必须是三张内容不同的图片")

    output = Path(output_dir).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    recipe, spec, plan = build_canary_plan(variables=variables)
    post = plan["items"][0]
    _write_json(output / "content_plan.json", plan)

    if observation is not None:
        qa = evaluate_thermal_transition_qa(
            observation, look_plans=post["looks"],
            profile_binding=post["profile_binding"],
            thermal_transition_contract=spec["thermal_transition_contract"],
        )
    else:
        vision = vision_service or PhotoReferenceVisionService(root=output / "vision")
        qa = vision.review_thermal_transition_pages(
            reference_paths=(), look_plans=post["looks"],
            image_paths=[str(sources[role]) for role in ROLE_ORDER],
            thermal_transition_contract=spec["thermal_transition_contract"],
            profile_binding=post["profile_binding"],
        )
    _write_json(output / "thermal_transition_qa.json", qa)
    (output / "thermal_transition_qa.md").write_text(
        thermal_transition_qa_markdown(qa), encoding="utf-8",
    )

    copy_block = dict(post["copy"])
    result = {
        "schema_version": "opv-thermal-transition-canary-v1",
        "recipe_id": RECIPE_ID, "mode": "local_no_external_writes",
        "status": "QA_FAILED" if not qa.get("passed") else "RENDERED",
        "source_hashes": hashes,
        "failed_roles": failed_roles_from_thermal_qa(qa, ROLE_ORDER),
        "content_plan_path": str(output / "content_plan.json"),
        "qa_path": str(output / "thermal_transition_qa.json"),
        "qa_report_path": str(output / "thermal_transition_qa.md"),
        "qa_note_zh": thermal_transition_qa_note_zh(qa),
        "thermal_contexts": dict(qa.get("thermal_contexts") or {}),
        "copy": copy_block, "slides": [],
        "release_ready": bool(
            qa.get("passed")
            and copy_block.get("language_review_status") == "NATIVE_APPROVED"
        ),
    }
    if qa.get("passed"):
        layout = next(
            item for item in load_board_layouts()
            if item.get("layout_id") == spec["template_id"]
            and item.get("layout_version") == spec["template_version"]
        )
        template = normalize_photo_template(layout)
        slide_texts = list(copy_block.get("slide_texts") or [])
        pages = list((spec.get("content_card") or {}).get("pages") or [])
        if len(slide_texts) != len(pages):
            raise ValueError(
                f"canary 文案必须正好包含 {len(pages)} 页 slide_texts"
            )
        slides = []
        for page, text in zip(pages, slide_texts):
            roles = [str(role) for role in page.get("source_roles") or []]
            slide_layout = str(page.get("layout") or "single")
            index = int(page.get("index") or 0)
            image = _compose(
                [str(sources[role]) for role in roles], layout=slide_layout,
                width=int(template["width"]), height=int(template["height"]),
                background=str(template.get("background") or "#F4F1EA"),
                template=template,
                column_labels=list(page.get("column_labels") or []),
            )
            _draw_overlay(
                image, str(text), template, index=index,
                cover_index=int(template.get("cover_index") or 1), total=len(pages),
            )
            path = output / f"page_{index:02d}_{slide_layout}.jpg"
            image.save(path, "JPEG", quality=int(template.get("jpeg_quality") or 90))
            slides.append({
                "index": index, "layout": slide_layout, "source_roles": roles,
                "path": str(path), "sha256": _sha256(path),
            })
        if len(slides) != 5:
            raise ValueError("冷热切换 canary 必须是五页")
        result["slides"] = slides
    _write_json(output / "canary_result.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True, type=Path)
    parser.add_argument("--mid", required=True, type=Path)
    parser.add_argument("--outer", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--observation-json", type=Path)
    args = parser.parse_args()
    observation = (
        json.loads(args.observation_json.read_text(encoding="utf-8"))
        if args.observation_json else None
    )
    result = run_canary(
        base=args.base, mid=args.mid, outer=args.outer,
        output_dir=args.output_dir, observation=observation,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "RENDERED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
