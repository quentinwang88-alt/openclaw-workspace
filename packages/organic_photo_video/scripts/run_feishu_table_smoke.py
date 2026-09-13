#!/usr/bin/env python3
"""Table-driven local smoke run for the TH photo lines.

Source data is read from the **live** OPV Feishu workbench (rows that already
exist), never from hand-written fixtures.  Two lines are exercised:

* **A 线 冷热切换** (``PHOTO_TH_THERMAL_TRANSITION_V1``): build the frozen
  five-page plan from a real source row, then prove the executable contract gate
  accepts it and rejects tampered variants.
* **B 线 旅行·温度穿搭** (``PHOTO_TH_TRAVEL_OUTFIT_V2``): rebuild the topic brief
  and the v9 planning prompt from real travel rows and prove the optional
  ``thermal_sensitivity`` behaves (empty → default, explicit → applied, other
  themes → untouched).

``--render`` additionally composes the *final ordered sheets* locally with the
shipped composer (``PHOTO_THERMAL_ROUTE_V1`` / ``PHOTO_TRAVEL_CARD_V3``) from
real photos downloaded out of the table, and ``--write`` uploads those sheets
into the ``预览/成片`` attachment column.

The run is local only: no RDS writes and no external vision / image model calls,
so image-level visual QA is **not** executed and is reported as such.  Sheet
composition is geometry + Thai text overlay, never generation.

Write-back into Feishu is opt-in (``--write``) and idempotent: the labelled
rows are reused when they already exist instead of being duplicated.  Every
such row carries ``执行=false`` and no progress state, so the production scanner
(``run_feishu_tasks.py``) can never pick it up.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import mimetypes
import sys
from pathlib import Path
from typing import Any, Mapping

from PIL import Image

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
BITABLE_SKILL = WORKSPACE_ROOT / "skills" / "script-run-manager-sync"
for value in (str(WORKSPACE_ROOT), str(BITABLE_SKILL), str(PACKAGE_ROOT)):
    if value not in sys.path:
        sys.path.insert(0, value)

from workspace_support import load_repo_env  # noqa: E402

load_repo_env()

from config.loader import load_board_layouts, load_content_recipes  # noqa: E402
from core.bitable import (  # noqa: E402
    FeishuBitableClient, resolve_wiki_bitable_app_token,
)
from services.feishu_workflow import (  # noqa: E402
    FIELD_OUTPUT, FIELD_THERMAL_SENSITIVITY, FeishuWorkflowError,
    build_travel_topic,
)
from services.photo_content_planner import load_planning_policy  # noqa: E402
from services.photo_copy import resolve_photo_copy  # noqa: E402
from services.photo_package import (  # noqa: E402
    PhotoPackageError, _compose, _draw_overlay, normalize_photo_template,
)
from services.photo_reference_vision import (  # noqa: E402
    TRAVEL_PROMPT_VERSION, PhotoReferenceVisionService,
)
from services.photo_theme import resolve_photo_theme  # noqa: E402
from services.photo_thermal_transition_contract import (  # noqa: E402
    ThermalTransitionContractError, contract_states,
    validate_thermal_transition_plan,
)
from services.photo_thermal_transition_flow import (  # noqa: E402
    PhotoThermalTransitionFlowError, build_thermal_transition_content_plan,
)


DEFAULT_WIKI_TOKEN = "TR10wxEXHiCYIhk8clActVdenpc"
DEFAULT_TABLE_ID = "tblj3x846gU3rshB"

TRANSITION_RECIPE = "PHOTO_TH_THERMAL_TRANSITION_V1"
TRAVEL_RECIPE = "PHOTO_TH_TRAVEL_OUTFIT_V2"
TRANSITION_THEME = "冷热切换"
TRAVEL_THERMAL_THEME = "旅行·温度穿搭"
ROLE_ORDER = ("base", "mid", "outer")

# The only combination the canary gate opens (policy.canary_only == true).
CANARY_VARIABLES = {
    "transition_key": "outdoor_bts_office",
    "thermal_sensitivity": "normal",
    "dress_code": "office",
    "style_series": "minimal_city",
    "temperature_label_mode": "QUALITATIVE",
}

DEFAULT_MARKER = "OPV表格冒烟"

# Sheet rendering.  Every photo used below is a real attachment downloaded out
# of the live table, verified as a decodable image and hashed; none is drawn or
# synthesized.  The role order is the table attachment order, recorded in the
# report so the mapping is auditable.
TRANSITION_ASSET_ROW = "recvuAVkNnY34S"   # 恰好 3 张真实参考图
TRAVEL_ASSET_ROW = "recvuDYB1RsfGV"       # 恰好 4 张真实参考图 → look_a..look_d
TRAVEL_SOURCE_COUNTRY = "韩国"             # B 线套版素材所属行
TRAVEL_SOURCE_PLACE = "景福宫"
LOOK_ROLES = ("look_a", "look_b", "look_c", "look_d")
TRAVEL_COPY_ID = "travel_which_look_v1"
TRAVEL_DESTINATION_KEY = "seoul"
TRAVEL_TEMPERATURE_KEY = "10_15c"


# --------------------------------------------------------------------------- #
# Feishu helpers
# --------------------------------------------------------------------------- #
def flatten(value: Any) -> str:
    if isinstance(value, list):
        return " / ".join(
            str(item.get("text") or item.get("name") or "")
            if isinstance(item, dict) else str(item)
            for item in value
        )
    return str(value or "")


def fetch_rows(client: FeishuBitableClient) -> dict[str, Any]:
    """Pick the concrete source rows the smoke run consumes."""
    records = client.list_records(page_size=500)
    travel: list[dict[str, Any]] = []
    three_ref_rows: list[dict[str, Any]] = []
    by_id: dict[str, dict[str, Any]] = {}
    for record in records:
        fields = record.fields or {}
        row = {
            "record_id": record.record_id,
            "preset": flatten(fields.get("生产预设")),
            "theme": flatten(fields.get("图文主题")),
            "country": flatten(fields.get("旅行国家")),
            "place": flatten(fields.get("旅行地点（可选）")),
            "requirement": flatten(fields.get("内容要求（可选）")),
            "reference_type": flatten(fields.get("参考图类型")),
            "progress": flatten(fields.get("进度")),
            "attachments": [
                {"name": str(a.get("name") or ""), "file_token": str(a.get("file_token") or "")}
                for a in (fields.get("参考图（可选）") or []) if isinstance(a, dict)
            ],
        }
        if row["theme"] == TRAVEL_THERMAL_THEME:
            travel.append(row)
        if len(row["attachments"]) == 3:
            three_ref_rows.append(row)
        by_id[record.record_id] = {
            **row,
            "full_attachments": [
                {"name": str(a.get("name") or ""), "file_token": str(a.get("file_token") or "")}
                for a in (fields.get("完整穿搭素材（可选）") or []) if isinstance(a, dict)
            ],
        }
    travel.sort(key=lambda item: item["record_id"])
    three_ref_rows.sort(key=lambda item: item["record_id"])
    return {
        "total_rows": len(records),
        "travel_rows": travel,
        "transition_asset_candidates": three_ref_rows,
        "by_id": by_id,
    }


# --------------------------------------------------------------------------- #
# A 线 — 冷热切换
# --------------------------------------------------------------------------- #
def build_transition_plan(*, record_id: str, variables: Mapping[str, Any] | None = None):
    recipe = next(item for item in load_content_recipes()
                  if item.recipe_id == TRANSITION_RECIPE)
    spec = dict(recipe.recipe_spec_json or {})
    plan = build_thermal_transition_content_plan(
        record_id=record_id, recipe_id=TRANSITION_RECIPE, recipe_spec=spec,
        policy=load_planning_policy(TRANSITION_RECIPE),
        theme=resolve_photo_theme(TRANSITION_THEME), reference_mode="COMPLETE_LOOK",
        variables=dict(variables or CANARY_VARIABLES),
    )
    return spec, plan


def _expect_failure(label: str, fn) -> dict[str, Any]:
    """Run a gate that must fail loudly; record exactly what it said."""
    try:
        fn()
    except (PhotoThermalTransitionFlowError, ThermalTransitionContractError,
            FeishuWorkflowError, ValueError) as exc:
        return {"control": label, "rejected": True,
                "error_type": type(exc).__name__, "message": str(exc)[:240]}
    return {"control": label, "rejected": False,
            "error_type": "", "message": "闸门未拦截（异常缺失）"}


def run_transition_line(source_row: Mapping[str, Any]) -> dict[str, Any]:
    spec, plan = build_transition_plan(record_id=str(source_row["record_id"]))
    post = plan["items"][0]
    contract = dict(spec.get("thermal_transition_contract") or {})
    states = contract_states(contract)
    binding = dict(post["profile_binding"])

    look_rows = []
    for look in post["looks"]:
        stack = [str(item.get("item_type") or "") for item in look["expected_layer_stack"]]
        look_rows.append({
            "role": str(look.get("role") or ""),
            "thermal_context": str(look.get("thermal_context") or ""),
            "visible_layer_count": int(look.get("expected_visible_layer_count") or 0),
            "layer_stack": stack,
            "added_garment_id": str(look.get("added_garment_id") or ""),
            "top_inner": str(look.get("top_inner") or ""),
            "outerwear": str(look.get("outerwear") or ""),
            "bottom": str(look.get("bottom") or ""),
            "shoes": str(look.get("shoes") or ""),
        })

    # The vision question the operator surface would have to answer.  Building
    # it proves the QA contract is renderable from the frozen plan; answering it
    # needs real base/mid/outer photographs, which this local run does not take.
    qa_prompt = PhotoReferenceVisionService._thermal_transition_qa_prompt(
        roles=ROLE_ORDER, transition_key=binding["transition_key"],
        contexts=[str(state.get("thermal_context") or "") for state in states],
        reference_count=0, look_plans=post["looks"],
        allowed_item_types=list(contract.get("allowed_item_types") or []),
        forbidden_item_types=list(contract.get("forbidden_item_types") or []),
    )

    controls = [
        _expect_failure(
            "未开放的 transition_key",
            lambda: build_transition_plan(
                record_id=str(source_row["record_id"]),
                variables={**CANARY_VARIABLES, "transition_key": "outdoor_mall_cinema"}),
        ),
        _expect_failure(
            "canary 外的体感值",
            lambda: build_transition_plan(
                record_id=str(source_row["record_id"]),
                variables={**CANARY_VARIABLES, "thermal_sensitivity": "feels_cold"}),
        ),
        _expect_failure(
            "不支持的参考模式",
            lambda: build_thermal_transition_content_plan(
                record_id=str(source_row["record_id"]), recipe_id=TRANSITION_RECIPE,
                recipe_spec=spec, policy=load_planning_policy(TRANSITION_RECIPE),
                theme=resolve_photo_theme(TRANSITION_THEME),
                reference_mode="SINGLE_REFERENCE", variables=dict(CANARY_VARIABLES),
            ),
        ),
        _expect_failure(
            "主题与 policy 不一致",
            lambda: build_thermal_transition_content_plan(
                record_id=str(source_row["record_id"]), recipe_id=TRANSITION_RECIPE,
                recipe_spec=spec, policy=load_planning_policy(TRANSITION_RECIPE),
                theme={"theme_key": "NOT_A_THERMAL_THEME"},
                reference_mode="COMPLETE_LOOK", variables=dict(CANARY_VARIABLES),
            ),
        ),
    ]

    tampered = copy.deepcopy(plan)
    tampered["items"][0]["looks"][1]["thermal_context"] = (
        tampered["items"][0]["looks"][0]["thermal_context"]
    )
    controls.append(_expect_failure(
        "篡改：中态体感重复",
        lambda: validate_thermal_transition_plan(
            tampered, thermal_transition_contract=contract),
    ))

    tampered2 = copy.deepcopy(plan)
    tampered2["items"][0]["looks"][2]["expected_layer_stack"] = (
        tampered2["items"][0]["looks"][2]["expected_layer_stack"][:-1]
    )
    controls.append(_expect_failure(
        "篡改：外层页丢掉一层",
        lambda: validate_thermal_transition_plan(
            tampered2, thermal_transition_contract=contract),
    ))

    copy_block = dict(post["copy"])
    return {
        "line": "A 线 · 冷热切换",
        "recipe_id": TRANSITION_RECIPE,
        "source_row": {
            "record_id": source_row["record_id"],
            "preset": source_row["preset"],
            "attachments": source_row["attachments"],
            "note": "表内真实附件仅作素材凭证引用；结构性测试输入，不是已验收三态实拍",
        },
        "plan_sha256": str(plan.get("plan_sha256") or ""),
        "profile_binding": {
            "transition_key": binding["transition_key"],
            "thermal_sensitivity": binding["thermal_sensitivity"],
            "dress_code": binding["dress_code"],
            "style_series": binding["style_series"],
            "temperature_label_mode": binding["temperature_label_mode"],
        },
        "presentation_order": list(post.get("presentation_order") or []),
        "generation_order": list(post.get("generation_order") or []),
        "page_count": len((spec.get("content_card") or {}).get("pages") or []),
        "looks": look_rows,
        "copy_variant_id": str(post.get("copy_variant_id") or ""),
        "copy_slide_count": len(list(copy_block.get("slide_texts") or [])),
        "language_review_status": copy_block.get("language_review_status"),
        "contract_gate": "PASS（真实取材计划通过 executable contract 校验）",
        "visual_qa": "NOT_EXECUTED（本轮不调用外部视觉模型）",
        "qa_prompt_chars": len(qa_prompt),
        "negative_controls": controls,
        "negative_controls_all_rejected": all(c["rejected"] for c in controls),
    }


# --------------------------------------------------------------------------- #
# B 线 — 旅行·温度穿搭
# --------------------------------------------------------------------------- #
def _travel_spec() -> dict[str, Any]:
    recipe = next(item for item in load_content_recipes()
                  if item.recipe_id == TRAVEL_RECIPE)
    return dict(recipe.recipe_spec_json or {})


def _travel_prompt(*, theme, place: str, spec, fields, variables) -> tuple[dict, str]:
    topic = build_travel_topic(
        theme=theme, travel_place=place, travel_variables=variables,
        content_requirement="", fields=fields, recipe_spec=spec,
    )
    prompt = PhotoReferenceVisionService._travel_plan_prompt(
        analysis={}, travel_contract=dict(spec.get("travel_contract") or {}),
        variables=variables, content_requirement="", count=1, travel_topic=topic,
    )
    return topic, prompt


def run_travel_line(source_rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    spec = _travel_spec()
    variables = dict((spec["execution_profiles"][0] or {}).get("variables") or {})
    theme = resolve_photo_theme(TRAVEL_THERMAL_THEME)

    rows: list[dict[str, Any]] = []
    for row in source_rows:
        place = str(row.get("place") or "").strip()
        if not place:
            continue
        fields = {"旅行地点（可选）": place}
        topic, prompt = _travel_prompt(
            theme=theme, place=place, spec=spec, fields=fields, variables=variables)
        rows.append({
            "record_id": row["record_id"],
            "place": place,
            "country": row["country"],
            "source_progress": row["progress"],
            "theme_type": topic.get("theme_type"),
            "theme_version": topic.get("theme_version"),
            "thermal_sensitivity": topic.get("thermal_sensitivity"),
            "sensitivity_source": "线上暂无「体感倾向」列 → 取配方默认",
            "prompt_version": TRAVEL_PROMPT_VERSION,
            "has_sensitivity_block": "【体感倾向" in prompt,
            "has_no_digit_rule": "不得出现具体温度数字" in prompt,
            "has_abcd_rule": "不得分别代表怕冷或怕热" in prompt,
        })

    # Explicit sensitivity is simulated because the column is not synced to the
    # live table yet (§9 forbids schema sync); the value path itself is real.
    probe_place = str(source_rows[0].get("place") or "银座") if source_rows else "银座"
    probes = []
    for raw in ("正常体感", "怕冷", "怕热"):
        topic, prompt = _travel_prompt(
            theme=theme, place=probe_place, spec=spec,
            fields={FIELD_THERMAL_SENSITIVITY: raw}, variables=variables)
        probes.append({
            "field_value": raw, "resolved": topic.get("thermal_sensitivity"),
            "cold_modifier": "整体偏保暖" in prompt,
            "warm_modifier": "整体偏轻量" in prompt,
        })

    invalid = _expect_failure(
        "非法体感值",
        lambda: _travel_prompt(
            theme=theme, place=probe_place, spec=spec,
            fields={FIELD_THERMAL_SENSITIVITY: "非常怕冷"}, variables=variables),
    )

    other_theme = resolve_photo_theme("旅行·环境协调")
    _, prompt_without = _travel_prompt(
        theme=other_theme, place=probe_place, spec=spec, fields={}, variables=variables)
    _, prompt_with = _travel_prompt(
        theme=other_theme, place=probe_place, spec=spec,
        fields={FIELD_THERMAL_SENSITIVITY: "怕冷"}, variables=variables)

    return {
        "line": "B 线 · 旅行·温度穿搭",
        "recipe_id": TRAVEL_RECIPE,
        "policy_version": int(load_planning_policy(TRAVEL_RECIPE).get("policy_version") or 0),
        "prompt_version": TRAVEL_PROMPT_VERSION,
        "thermal_rule": dict((spec.get("variables_schema") or {}).get(
            "thermal_sensitivity") or {}),
        "rows": rows,
        "simulated_probes": probes,
        "other_theme_untouched": prompt_without == prompt_with,
        "other_theme_has_sensitivity_block": "【体感倾向" in prompt_without,
        "invalid_value_control": invalid,
        "all_checks_passed": bool(
            rows
            and all(r["thermal_sensitivity"] == "normal" for r in rows)
            and [p["resolved"] for p in probes] == ["normal", "feels_cold", "feels_warm"]
            and probes[1]["cold_modifier"] and probes[2]["warm_modifier"]
            and invalid["rejected"]
            and prompt_without == prompt_with
            and not ("【体感倾向" in prompt_without)
        ),
    }


# --------------------------------------------------------------------------- #
# 渲染：本地套版出最终成片（几何合成 + 文字叠加，不调用生图模型）
# --------------------------------------------------------------------------- #
SHEET_IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp"}


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def resolve_layout_template(layout_id: str, layout_version: int) -> dict[str, Any]:
    layout = next(
        (item for item in load_board_layouts()
         if item.get("layout_id") == layout_id
         and int(item.get("layout_version") or 0) == int(layout_version)),
        None,
    )
    if layout is None:
        raise PhotoPackageError(f"版式缺失：{layout_id} v{layout_version}")
    return normalize_photo_template(layout)


def download_row_images(client: FeishuBitableClient, record_id: str, field: str,
                        output_dir: Path, prefix: str) -> list[Path]:
    """Download one attachment column and prove every file decodes."""
    record = client.get_record(record_id)
    attachments = [
        item for item in ((record.fields or {}).get(field) or [])
        if isinstance(item, dict) and item.get("file_token")
    ]
    if not attachments:
        raise SystemExit(f"{record_id} 的「{field}」没有可用附件")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    saved: list[Path] = []
    for index, attachment in enumerate(attachments, start=1):
        content, name, _ctype, _size = client.download_attachment_bytes(attachment)
        suffix = Path(str(name)).suffix.lower()
        if suffix not in SHEET_IMAGE_SUFFIXES:
            suffix = ".img"
        target = output_dir / f"{prefix}_{index:02d}{suffix}"
        target.write_bytes(content)
        try:
            with Image.open(target) as image:
                image.verify()
        except Exception as exc:  # noqa: BLE001 - surfaced as a hard stop
            raise SystemExit(
                f"{record_id} 的第 {index} 张附件不是可解码图片：{name}") from exc
        saved.append(target)
    return saved


def render_pages(*, spec: Mapping[str, Any], slide_texts: list[str],
                 sources: Mapping[str, Path], output_dir: Path,
                 prefix: str) -> list[dict[str, Any]]:
    """Compose the final ordered sheets for one post."""
    pages = list((spec.get("content_card") or {}).get("pages") or [])
    if len(pages) != len(slide_texts):
        raise PhotoPackageError(
            f"{prefix} 的文案必须与页面一一对应（{len(slide_texts)} vs {len(pages)}）")
    template = resolve_layout_template(
        str(spec["template_id"]), int(spec["template_version"]))
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    slides: list[dict[str, Any]] = []
    for position, (page, text) in enumerate(zip(pages, slide_texts), start=1):
        roles = [str(role) for role in page.get("source_roles") or []]
        missing = [role for role in roles if role not in sources]
        if missing:
            raise PhotoPackageError(f"{prefix} 第 {position} 页缺少素材角色：{missing}")
        layout = str(page.get("layout") or "single")
        image = _compose(
            [str(sources[role]) for role in roles], layout=layout,
            width=int(template["width"]), height=int(template["height"]),
            background=str(template.get("background") or "#FFFFFF"),
            template=template, column_labels=list(page.get("column_labels") or []),
        )
        _draw_overlay(
            image, text, template, index=int(page.get("index") or position),
            cover_index=int(template.get("cover_index") or 1), total=len(pages),
        )
        path = output_dir / f"{prefix}_p{position:02d}_{layout}.jpg"
        image.save(path, "JPEG", quality=int(template.get("jpeg_quality") or 90))
        slides.append({
            "index": position, "layout": layout, "source_roles": roles,
            "text": text, "path": str(path), "sha256": _sha256_file(path),
            "bytes": path.stat().st_size,
        })
    return slides


def render_transition_sheets(*, source_row: Mapping[str, Any], images: list[Path],
                             output_dir: Path) -> dict[str, Any]:
    """A 线：三态素材 + 冻结计划 → 五页成片。"""
    if len(images) != len(ROLE_ORDER):
        raise SystemExit(f"A 线需要恰好 {len(ROLE_ORDER)} 张素材，实际 {len(images)} 张")
    hashes = [_sha256_file(path) for path in images]
    if len(set(hashes)) != len(hashes):
        raise SystemExit("A 线三态素材必须是三张内容不同的图片")
    spec, plan = build_transition_plan(record_id=str(source_row["record_id"]))
    post = plan["items"][0]
    copy_block = dict(post["copy"])
    sources = {role: Path(path) for role, path in zip(ROLE_ORDER, images)}
    slides = render_pages(
        spec=spec, slide_texts=list(copy_block.get("slide_texts") or []),
        sources=sources, output_dir=Path(output_dir), prefix="A_line",
    )
    return {
        "recipe_id": TRANSITION_RECIPE,
        "layout": f"{spec['template_id']} v{spec['template_version']}",
        "copy_variant_id": str(post.get("copy_variant_id") or ""),
        "language_review_status": copy_block.get("language_review_status"),
        "plan_sha256": str(plan.get("plan_sha256") or ""),
        "role_order_note": "表内附件顺序 → base/mid/outer（顺序映射，非三态实拍）",
        "sources": [
            {"role": role, "row": str(source_row["record_id"]),
             "file": Path(path).name, "sha256": _sha256_file(path)}
            for role, path in sources.items()
        ],
        "slides": slides,
    }


def render_travel_sheets(*, source_row: Mapping[str, Any], images: list[Path],
                         output_dir: Path) -> dict[str, Any]:
    """B 线：四套 Look 素材 + 冻结文案 → 五页成片（ASSET_REUSE 套版）。"""
    if len(images) != len(LOOK_ROLES):
        raise SystemExit(f"B 线需要恰好 {len(LOOK_ROLES)} 张素材，实际 {len(images)} 张")
    hashes = [_sha256_file(path) for path in images]
    if len(set(hashes)) != len(hashes):
        raise SystemExit("B 线四套 Look 素材必须是四张内容不同的图片")
    spec = _travel_spec()
    contract = dict(spec.get("travel_contract") or {})
    profile = dict((spec.get("execution_profiles") or [{}])[0])
    variant = next(
        (item for item in (profile.get("copy_variants") or [])
         if str(item.get("copy_id")) == TRAVEL_COPY_ID), None)
    if variant is None:
        raise SystemExit(f"旅行配方里没有文案变体 {TRAVEL_COPY_ID}")
    moments = list(contract.get("moments") or [])[:len(LOOK_ROLES)]
    if len(moments) != len(LOOK_ROLES):
        raise SystemExit("旅行合同的 moments 不足以支撑四套 Look")
    look_labels = {letter: str(moment.get("label_th") or "")
                   for letter, moment in zip("abcd", moments)}
    destination = str((contract.get("destination_labels_th") or {}).get(
        TRAVEL_DESTINATION_KEY) or "")
    temperature = str((contract.get("temperature_labels_th") or {}).get(
        TRAVEL_TEMPERATURE_KEY) or "")
    if not destination or not temperature:
        raise SystemExit("旅行合同里缺少 destination / temperature 的已审核填充值")
    resolved = resolve_photo_copy(
        dict(variant.get("copy") or {}),
        assets=[{"role": f"look_{letter}", "display_label": {"th-TH": look_labels[letter]}}
                for letter in "abcd"],
        locale="th-TH",
        extra_tokens={"destination": destination, "temperature": temperature},
        expected_slide_count=len(LOOK_ROLES) + 1,
    )
    sources = {role: Path(path) for role, path in zip(LOOK_ROLES, images)}
    slides = render_pages(
        spec=spec, slide_texts=list(resolved.get("slide_texts") or []),
        sources=sources, output_dir=Path(output_dir), prefix="B_line",
    )
    return {
        "recipe_id": TRAVEL_RECIPE,
        "layout": f"{spec['template_id']} v{spec['template_version']}",
        "copy_variant_id": TRAVEL_COPY_ID,
        "language_review_status": resolved.get("language_review_status"),
        "look_labels": look_labels,
        "tokens": {"destination": destination, "temperature": temperature,
                   "destination_key": TRAVEL_DESTINATION_KEY,
                   "temperature_key": TRAVEL_TEMPERATURE_KEY},
        "role_order_note": "表内附件顺序 → look_a..look_d（ASSET_REUSE 套版，非 AI 生图）",
        "sources": [
            {"role": role, "row": str(source_row["record_id"]),
             "file": Path(path).name, "sha256": _sha256_file(path)}
            for role, path in sources.items()
        ],
        "slides": slides,
    }


def upload_sheets(client: FeishuBitableClient, record_id: str,
                  slides: list[Mapping[str, Any]]) -> list[dict[str, str]]:
    """Push the composed sheets into the 预览/成片 attachment column."""
    uploaded: list[dict[str, str]] = []
    for slide in slides:
        path = Path(str(slide["path"]))
        content_type = mimetypes.guess_type(path.name)[0] or "image/jpeg"
        item = client.upload_attachment(
            path.read_bytes(), path.name, content_type, path.stat().st_size,
            parent_type="bitable_image",
        )
        uploaded.append({"file_token": item["file_token"]})
    client.update_record_fields(record_id, {FIELD_OUTPUT: uploaded})
    return uploaded


# --------------------------------------------------------------------------- #
# Report + write-back
# --------------------------------------------------------------------------- #
def transition_summary_zh(result: Mapping[str, Any]) -> str:
    binding = result["profile_binding"]
    looks = "；".join(
        f"{item['role']}={item['thermal_context']}/{item['visible_layer_count']}层"
        f"({' + '.join(item['layer_stack'])})"
        for item in result["looks"]
    )
    return (
        f"【{result['line']}】场景={binding['transition_key']}；体感={binding['thermal_sensitivity']}；"
        f"着装={binding['dress_code']}；系列={binding['style_series']}；"
        f"温度口径={binding['temperature_label_mode']}（禁止具体温度数字）；"
        f"页数={result['page_count']}；{looks}；"
        f"生成顺序={'→'.join(result['generation_order'])}；"
        f"文案变体={result['copy_variant_id']}；plan_sha256={result['plan_sha256'][:16]}"
    )


def travel_summary_zh(result: Mapping[str, Any]) -> str:
    parts = []
    for row in result["rows"]:
        parts.append(
            f"{row['place']}（{row['country']}）：体感={row['thermal_sensitivity']}"
            f"（{row['sensitivity_source']}）；主题={row['theme_type']} v{row['theme_version']}"
        )
    probes = "；".join(
        f"{p['field_value']}→{p['resolved']}" for p in result["simulated_probes"])
    return (
        f"【{result['line']}】提示词版本={result['prompt_version']}；"
        f"policy_version={result['policy_version']}；真实取材行：" + "｜".join(parts)
        + f"；体感解析（模拟列值）：{probes}"
        + f"；非温度主题提示词不变={result['other_theme_untouched']}"
    )


def render_summary_zh(render: Mapping[str, Any]) -> str:
    parts = []
    for slide in render["slides"]:
        parts.append(
            f"P{slide['index']} {slide['layout']} "
            f"[{'+'.join(slide['source_roles'])}] {slide['text'].replace(chr(10), ' / ')}"
        )
    return "；".join(parts)


def build_report(sources: Mapping[str, Any], transition: Mapping[str, Any],
                 travel: Mapping[str, Any],
                 sheets: Mapping[str, Any] | None = None) -> str:
    lines = [
        "# 飞书表驱动冒烟报告（本地，无外部模型调用）",
        "",
        f"- 数据来源：线上表 `{DEFAULT_TABLE_ID}`，共 {sources['total_rows']} 行",
        f"- 取材行：B 线 {len(sources['travel_rows'])} 条「{TRAVEL_THERMAL_THEME}」真实行",
        f"- A 线素材引用：{transition['source_row']['record_id']}（3 张真实参考图）",
        "",
        "## A 线 · 冷热切换",
        "",
        f"- 结论：{transition['contract_gate']}",
        f"- 视觉质检：{transition['visual_qa']}",
        f"- 文案变体：{transition['copy_variant_id']}；语言审校状态："
        f"`{transition['language_review_status']}`",
        f"- plan_sha256：`{transition['plan_sha256'][:32]}…`",
        "",
        "| 角色 | 体感 | 可见层数 | 层栈 | 新增单品 |",
        "| --- | --- | --- | --- | --- |",
    ]
    for item in transition["looks"]:
        lines.append(
            f"| {item['role']} | {item['thermal_context']} | {item['visible_layer_count']} "
            f"| {' + '.join(item['layer_stack'])} | {item['added_garment_id'] or '—'} |"
        )
    lines += ["", "### 反向控制（闸门必须报错）", "",
              "| 控制项 | 是否拦截 | 异常类型 | 信息 |", "| --- | --- | --- | --- |"]
    for control in transition["negative_controls"]:
        lines.append(
            f"| {control['control']} | {'✅' if control['rejected'] else '❌'} "
            f"| {control['error_type']} | {control['message'][:90]} |"
        )
    lines += [
        "",
        "## B 线 · 旅行·温度穿搭",
        "",
        "| 取材行 | 地点 | 国家 | 体感 | 主题 | 版本 |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in travel["rows"]:
        lines.append(
            f"| {row['record_id']} | {row['place']} | {row['country']} "
            f"| {row['thermal_sensitivity']} | {row['theme_type']} | v{row['theme_version']} |"
        )
    lines += [
        "",
        f"- 提示词版本：`{travel['prompt_version']}`",
        f"- 非温度主题提示词保持不变：{'✅' if travel['other_theme_untouched'] else '❌'}",
        f"- 非法体感值被拒：{'✅' if travel['invalid_value_control']['rejected'] else '❌'}"
        f"（{travel['invalid_value_control']['error_type']}）",
        f"- 全部校验通过：{'✅' if travel['all_checks_passed'] else '❌'}",
        "",
        "| 模拟体感列值 | 解析结果 | 偏保暖修正 | 偏轻量修正 |",
        "| --- | --- | --- | --- |",
    ]
    for probe in travel["simulated_probes"]:
        lines.append(
            f"| {probe['field_value']} | {probe['resolved']} "
            f"| {'✅' if probe['cold_modifier'] else '—'} "
            f"| {'✅' if probe['warm_modifier'] else '—'} |"
        )
    lines += [
        "",
        "> 说明：线上表当前没有「冷热切换场景 / 体感 / 着装要求 / 体感倾向」列"
        "（本轮未做 schema 同步），因此 A 线冻结变量取自 canary 合同，"
        "B 线的显式体感为**模拟列值**，真实取材只走「空列 → 配方默认」路径。",
    ]

    sheets = dict(sheets or {})
    if sheets:
        lines += ["", "## 本地套版成片", ""]
        for key, label in (("transition", "A 线 · 冷热切换"), ("travel", "B 线 · 旅行·温度穿搭")):
            render = sheets.get(key)
            if not render:
                continue
            lines += [
                f"### {label}",
                "",
                f"- 版式：`{render['layout']}`；文案变体：`{render['copy_variant_id']}`；"
                f"语言审校状态：`{render['language_review_status']}`",
                f"- 素材源行：`{render['sources'][0]['row']}`（附件顺序 → 角色，"
                f"顺序映射，见下方哈希）",
                "",
                "| 页 | 版式 | 素材角色 | 页文字 | SHA256 |",
                "| --- | --- | --- | --- | --- |",
            ]
            for slide in render["slides"]:
                lines.append(
                    f"| P{slide['index']} | {slide['layout']} "
                    f"| {'+'.join(slide['source_roles'])} "
                    f"| {slide['text'].replace(chr(10), ' / ')} "
                    f"| `{slide['sha256'][:16]}…` |"
                )
            lines += [
                "",
                "| 角色 | 源文件 | SHA256 |",
                "| --- | --- | --- |",
            ]
            for source in render["sources"]:
                lines.append(
                    f"| {source['role']} | {source['file']} | `{source['sha256'][:16]}…` |"
                )
            if key == "travel":
                tokens = render["tokens"]
                lines += [
                    "",
                    f"- 语料填充：`{{destination}}`→{tokens['destination']}"
                    f"（{tokens['destination_key']}）、`{{temperature}}`→{tokens['temperature']}"
                    f"（{tokens['temperature_key']}）",
                    f"- Look 标签（取自合同 moments 顺序）："
                    + "；".join(f"{k.upper()}={v}" for k, v in render["look_labels"].items()),
                ]
            lines += [""]
        lines += [
            "> 成片性质：**几何套版 + 泰语文字叠加**，不调用生图模型。"
            "A 线三张素材是表内真实参考图按附件顺序映射成 base/mid/outer，"
            "**不是同一套搭配的三态实拍**；B 线四套 Look 是 ASSET_REUSE 套版，"
            "**不是 AI 生成的四个不同造型**。视觉质检未执行。",
            "",
            "> ⚠️ 素材风险：源参考图**带第三方平台水印**（成片右下角可见）。"
            "这两组成片只能用于验证渲染链路与版式，"
            "**不得作为发布素材，也不得直接用于对外投放**。",
        ]
    return "\n".join(lines)


def transition_note_text(*, marker: str, transition: Mapping[str, Any],
                         sheet_count: int, source_row: str) -> str:
    sheet_note = (
        f"；成片={sheet_count} 页已写入「预览/成片」（本地套版合成，未发布）"
        if sheet_count else "；成片未生成")
    caveat = (
        "；素材为表内真实参考图，**含平台水印**，仅用于验证渲染链路，不得作为发布素材"
        if sheet_count else "")
    return (
        f"{marker}｜A线·冷热切换｜本地规划+合同校验通过；视觉质检未执行（无外部模型调用）；"
        f"文案状态={transition['language_review_status']}（泰语待母语审校）；"
        f"结构性测试，禁止发布；源行={transition['source_row']['record_id']}；"
        f"plan_sha256={transition['plan_sha256'][:16]}{sheet_note}"
        f"；三态素材源行={source_row}{caveat}"
    )


def travel_note_text(*, marker: str, travel: Mapping[str, Any],
                     sheet_count: int) -> str:
    sheet_note = (
        f"；成片={sheet_count} 页已写入「预览/成片」"
        f"（ASSET_REUSE 套版，非 AI 生图，未发布）"
        if sheet_count else "；成片未生成")
    caveat = (
        "；素材为表内真实参考图，**含平台水印**，仅用于验证渲染链路，不得作为发布素材"
        if sheet_count else "")
    extra = (
        f"；Look 素材源行={TRAVEL_ASSET_ROW}；"
        f"目的地/温度语料={TRAVEL_DESTINATION_KEY}/{TRAVEL_TEMPERATURE_KEY}"
        if sheet_count else "")
    return (
        f"{marker}｜B线·旅行温度｜本地主题简报+v9 提示词校验通过；"
        f"体感仅走「空列→配方默认」真实路径，显式为模拟；结构性测试，禁止发布；"
        f"提示词={travel['prompt_version']}{sheet_note}{extra}{caveat}"
    )


def find_marker_rows(client: FeishuBitableClient, marker: str) -> dict[str, str]:
    """Locate the labelled rows from an earlier run so they are reused."""
    found: dict[str, str] = {}
    for record in client.list_records(page_size=500):
        note = flatten((record.fields or {}).get("备注"))
        if not note.startswith(marker):
            continue
        if "A线" in note:
            found.setdefault("A", record.record_id)
        elif "B线" in note:
            found.setdefault("B", record.record_id)
    return found


def write_back(client: FeishuBitableClient, *, marker: str,
               transition: Mapping[str, Any], travel: Mapping[str, Any],
               sources: Mapping[str, Any],
               sheets: Mapping[str, Any] | None = None,
               notes_only: bool = False) -> list[dict[str, Any]]:
    """Create-or-reuse the labelled test rows and attach the composed sheets.

    Deliberately inert: 执行=false and no 进度, so the production scanner
    (``run_feishu_tasks.py``) can never pick these rows up.

    ``notes_only`` refreshes the text fields of the rows from an earlier run
    without re-uploading attachments, so the 预览/成片 column is not churned.
    """
    sheets = dict(sheets or {})
    transition_sheets = list((sheets.get("transition") or {}).get("slides") or [])
    travel_sheets = list((sheets.get("travel") or {}).get("slides") or [])

    payload = [
        {
            "tag": "A",
            "kind": "A 线 · 冷热切换",
            "slides": transition_sheets,
            "fields": {
                "生成篇数": 1,
                "执行": False,
                "内容方案摘要": transition_summary_zh(transition),
            },
        },
        {
            "tag": "B",
            "kind": "B 线 · 旅行·温度穿搭",
            "slides": travel_sheets,
            "default_country": str(
                (sources["travel_rows"][0] if sources["travel_rows"] else {})
                .get("country") or "日本"),
            "default_place": (
                travel["rows"][0]["place"] if travel["rows"] else "银座"),
            "fields": {
                "生产预设": "图文｜TH｜旅行穿搭",
                "图文主题": TRAVEL_THERMAL_THEME,
                "生成篇数": 1,
                "执行": False,
                "内容方案摘要": travel_summary_zh(travel),
            },
        },
    ]

    def _existing_sheet_count(record_id: str) -> int:
        """Count 预览/成片 attachments already on a row (notes-only refresh)."""
        if not record_id:
            return 0
        try:
            record = client.get_record(record_id)
        except Exception:  # noqa: BLE001 - falls back to the local count
            return 0
        return len([
            item for item in ((record.fields or {}).get(FIELD_OUTPUT) or [])
            if isinstance(item, dict) and item.get("file_token")
        ])

    existing = find_marker_rows(client, marker) if (
        notes_only or any(item["slides"] for item in payload)) else {}
    written: list[dict[str, Any]] = []
    for item in payload:
        record_id = str(existing.get(item["tag"]) or "")
        action = "reused" if record_id else "created"
        sheet_count = len(item["slides"]) or _existing_sheet_count(record_id)
        note = (
            transition_note_text(
                marker=marker, transition=transition, sheet_count=sheet_count,
                source_row=TRANSITION_ASSET_ROW)
            if item["tag"] == "A" else
            travel_note_text(
                marker=marker, travel=travel, sheet_count=sheet_count)
        )
        fields: dict[str, Any] = {**item["fields"], "备注": note}
        if item["tag"] == "B":
            # The travel row's country/place must describe the photos that were
            # actually composed into 预览/成片; without sheets we keep the
            # metadata derived from the sampled 旅行·温度穿搭 rows instead.
            fields["旅行国家"] = (
                TRAVEL_SOURCE_COUNTRY if sheet_count else item["default_country"])
            fields["旅行地点（可选）"] = (
                TRAVEL_SOURCE_PLACE if sheet_count else item["default_place"])
        if notes_only:
            if not record_id:
                written.append({"kind": item["kind"], "record_id": "", "ok": False,
                                "error": "notes_only 需要已存在的标记行"})
                continue
            try:
                client.update_record_fields(record_id, fields)
            except Exception as exc:  # noqa: BLE001 - reported, never silent
                written.append({"kind": item["kind"], "record_id": record_id,
                                "ok": False, "error": str(exc)})
                continue
            written.append({"kind": item["kind"], "ok": True, "record_id": record_id,
                            "action": "notes_only", "sheets": sheet_count,
                            "cleared_default_fields": []})
            continue
        if item["slides"]:
            attachments: list[dict[str, str]] = []
            for slide in item["slides"]:
                path = Path(str(slide["path"]))
                content_type = mimetypes.guess_type(path.name)[0] or "image/jpeg"
                uploaded = client.upload_attachment(
                    path.read_bytes(), path.name, content_type, path.stat().st_size,
                    parent_type="bitable_image",
                )
                attachments.append({"file_token": uploaded["file_token"]})
            fields[FIELD_OUTPUT] = attachments

        if not record_id:
            response = client._request(
                "POST",
                f"https://open.feishu.cn/open-apis/bitable/v1/apps/{client.app_token}"
                f"/tables/{client.table_id}/records",
                headers=client._headers(), json={"fields": fields},
            )
            result = response.json()
            if result.get("code") != 0:
                written.append({"kind": item["kind"], "record_id": "",
                                "ok": False, "error": str(result.get("msg"))})
                continue
            record_id = str((result.get("data") or {}).get("record", {})
                            .get("record_id") or "")
        else:
            try:
                client.update_record_fields(record_id, fields)
            except Exception as exc:  # noqa: BLE001 - reported, never silent
                written.append({"kind": item["kind"], "record_id": record_id,
                                "ok": False, "error": str(exc)})
                continue

        cleared = []
        # The table carries a create-time default for 旅行国家; on the A-line row
        # that default is noise, so it is removed again.  B 线 keeps it because a
        # travel row legitimately has a destination country.
        if item["tag"] == "A" and record_id:
            try:
                client.update_record_fields(record_id, {"旅行国家": None})
                cleared.append("旅行国家")
            except Exception:
                pass
        written.append({
            "kind": item["kind"], "ok": True, "record_id": record_id,
            "action": action, "sheets": len(item["slides"]),
            "cleared_default_fields": cleared,
        })
    return written


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wiki-token", default=DEFAULT_WIKI_TOKEN)
    parser.add_argument("--table-id", default=DEFAULT_TABLE_ID)
    parser.add_argument("--marker", default=DEFAULT_MARKER)
    parser.add_argument("--output-dir", type=Path,
                        default=PACKAGE_ROOT / "tmp" / "feishu_table_smoke")
    parser.add_argument("--render", action="store_true",
                        help="本地套版合成五页成片（需下载表内真实素材，不调生图模型）")
    parser.add_argument("--write", action="store_true",
                        help="把带标记的测试行建/更新，并回写成片到「预览/成片」")
    parser.add_argument("--notes-only", action="store_true",
                        help="只刷新已存在标记行的文本字段，不重传附件（需与 --write 同用）")
    parser.add_argument("--transition-source-row", default=TRANSITION_ASSET_ROW)
    parser.add_argument("--travel-source-row", default=TRAVEL_ASSET_ROW)
    parser.add_argument("--json", action="store_true", help="打印完整 JSON")
    args = parser.parse_args()

    client = FeishuBitableClient(
        resolve_wiki_bitable_app_token(args.wiki_token), args.table_id)

    sources = fetch_rows(client)
    if not sources["travel_rows"]:
        raise SystemExit("线上表里没有「旅行·温度穿搭」真实行，无法取材")
    if not sources["transition_asset_candidates"]:
        raise SystemExit("线上表里没有恰好 3 张参考图的行，无法为 A 线取材")

    transition_row = sources["by_id"].get(args.transition_source_row)
    if transition_row is None:
        raise SystemExit(f"线上表里找不到 A 线素材源行 {args.transition_source_row}")
    if len(transition_row["attachments"]) != len(ROLE_ORDER):
        raise SystemExit(
            f"A 线素材源行需要恰好 {len(ROLE_ORDER)} 张参考图，"
            f"{args.transition_source_row} 有 {len(transition_row['attachments'])} 张")
    travel_row = sources["by_id"].get(args.travel_source_row)
    if travel_row is None:
        raise SystemExit(f"线上表里找不到 B 线素材源行 {args.travel_source_row}")
    if len(travel_row["attachments"]) != len(LOOK_ROLES):
        raise SystemExit(
            f"B 线素材源行需要恰好 {len(LOOK_ROLES)} 张参考图，"
            f"{args.travel_source_row} 有 {len(travel_row['attachments'])} 张")

    transition = run_transition_line(transition_row)
    travel = run_travel_line(sources["travel_rows"])

    output = Path(args.output_dir).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)

    sheets: dict[str, Any] = {}
    if args.render:
        sheets["transition"] = render_transition_sheets(
            source_row=transition_row,
            images=download_row_images(
                client, args.transition_source_row, "参考图（可选）",
                output / "sheets" / "inputs" / "A_line", "A_src"),
            output_dir=output / "sheets",
        )
        sheets["travel"] = render_travel_sheets(
            source_row=travel_row,
            images=download_row_images(
                client, args.travel_source_row, "参考图（可选）",
                output / "sheets" / "inputs" / "B_line", "B_src"),
            output_dir=output / "sheets",
        )

    (output / "sources.json").write_text(
        json.dumps(sources, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "transition_plan.json").write_text(
        json.dumps(transition, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "travel_line.json").write_text(
        json.dumps(travel, ensure_ascii=False, indent=2), encoding="utf-8")
    if sheets:
        (output / "sheet_render.json").write_text(
            json.dumps(sheets, ensure_ascii=False, indent=2), encoding="utf-8")
    report = build_report(sources, transition, travel, sheets)
    (output / "report.md").write_text(report, encoding="utf-8")

    result: dict[str, Any] = {
        "schema_version": "opv-feishu-table-smoke-v2",
        "mode": "local_sheet_composition_no_external_model_calls",
        "table_id": args.table_id,
        "source_rows": sources["total_rows"],
        "report_sha256": hashlib.sha256(report.encode("utf-8")).hexdigest(),
        "transition_line": transition,
        "travel_line": travel,
        "output_dir": str(output),
    }
    if sheets:
        result["sheets"] = sheets
    if args.write:
        result["written_rows"] = write_back(
            client, marker=args.marker, transition=transition,
            travel=travel, sources=sources, sheets=sheets,
            notes_only=args.notes_only)
    (output / "result.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    print(report)
    print()
    print(f"[report] {output / 'report.md'}")
    if sheets:
        for key in ("transition", "travel"):
            for slide in sheets[key]["slides"]:
                print(f"[sheet] {key} P{slide['index']} {Path(slide['path']).name} "
                      f"({slide['bytes']} bytes)")
    if args.write:
        for row in result["written_rows"]:
            state = row["record_id"] if row["ok"] else f"FAILED: {row.get('error')}"
            print(f"[feishu] {row['kind']} -> {state} "
                  f"({row.get('action')}, {row.get('sheets', 0)} 页成片)")
    else:
        print("[feishu] dry-run：未建行、未上传（加 --write 才写入）")
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))

    ok = (transition["negative_controls_all_rejected"] and travel["all_checks_passed"]
          and transition["contract_gate"].startswith("PASS"))
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
