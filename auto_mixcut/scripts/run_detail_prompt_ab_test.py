#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
from datetime import UTC, datetime
import json
from pathlib import Path
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = ROOT.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(WORKSPACE / "skills" / "script-run-manager-sync") not in sys.path:
    sys.path.insert(0, str(WORKSPACE / "skills" / "script-run-manager-sync"))

from auto_mixcut.core.bootstrap import build_context  # noqa: E402
from auto_mixcut.skills.product_reference_image_skill import ProductReferenceImageSkill  # noqa: E402
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill  # noqa: E402
from auto_mixcut.skills.segment_prompt_factory_skill import SegmentPromptFactorySkill  # noqa: E402
from scripts.sync_prompt_package_workbench_from_tasks import (  # noqa: E402
    PROMPT_WORKBENCH_URL,
    _compact,
    _feishu_url,
    _field_names,
    _format_prompt_package,
    _safe_batch_create_prompt,
    resolve_client,
)


BATCH_KEY = "DETAIL_PROMPT_AB_20260723_V1"
PRODUCT_ID = "1736444730937804794"
PRODUCT_NAME = "短款仿皮立领夹克"
MARKET = "TH"
MODEL = "seedance 2.0"
REFERENCE_ROOT = ROOT / "var" / "detail_prompt_ab" / BATCH_KEY / "source_refs"
REPORT_PATH = ROOT / "var" / "detail_prompt_ab" / BATCH_KEY / "stage1_prepare_report.json"
STAGE2_REPORT_PATH = ROOT / "var" / "detail_prompt_ab" / BATCH_KEY / "stage2_prepare_report.json"


NEW_POSITIVE = """生成4秒9:16竖屏单镜头。第一参考图中的米杏白短款夹克领口是唯一主体。画面从第一帧就是领口局部近景，立领、两个金属按扣和拉链顶部占画面高度60%至75%，不得出现完整人物、手机或下半身。

0至1秒保持正面稳定；1至2.5秒人物上身只向右轻转约5度，让侧光扫过领口和按扣；2.5至4秒停止并保持清晰。镜头固定，不推近、不变焦、不切镜。

领口高度、按扣数量、按扣位置、拉链形状和米杏白颜色必须严格跟随参考图。参考图看不清的结构不得补充或虚构。真实自然光，商品结构清晰，适合作为口播中的商品细节证据镜头。"""

NEW_NEGATIVE = """禁止增加或减少金属按扣，禁止改变按扣位置、立领高度和拉链结构；禁止领口、拉链、走线或面料变形、漂移、液化；禁止手部遮挡领口；禁止完整人物、手机、下半身、切镜、推拉、变焦、字幕、文字、Logo、水印、价格和促销标签。"""

NEW_MOTION = "0-1秒正面稳定 -> 1-2.5秒上身向右轻转约5度、侧光扫过领口 -> 2.5-4秒停止并保持清晰"

CUFF_POSITIVE = """生成4秒9:16竖屏单镜头。第一参考图中的米杏白短款夹克一侧袖口是唯一主体。画面从第一帧就是袖口局部近景，袖口收边、走线和自然折痕占画面高度60%至75%，人物最多只露出前臂边缘，不得出现完整人物、手机或下半身。

0至1秒保持正面稳定；1至2.5秒前臂只向外轻转约8度，让侧光扫过袖口收边；2.5至4秒停止并保持清晰。镜头固定，不推近、不变焦、不切镜，不用手揉搓或拉扯袖口。

袖口形状、宽度、收边方式、走线和米杏白颜色必须严格跟随参考图。参考图看不清的结构不得补充或虚构。真实自然光，袖口结构清晰，适合作为口播中的商品细节证据镜头。"""

CUFF_NEGATIVE = """禁止给袖口增加按扣、拉链、松紧带或装饰；禁止改变袖口宽度、收边方式和走线；禁止袖口、手腕、手指或面料变形、漂移、液化；禁止手部揉搓遮挡袖口；禁止完整人物、手机、下半身、切镜、推拉、变焦、字幕、文字、Logo、水印、价格和促销标签。"""

CUFF_MOTION = "0-1秒袖口正面稳定 -> 1-2.5秒前臂向外轻转约8度、侧光扫过收边 -> 2.5-4秒停止并保持清晰"

MATERIAL_POSITIVE = """生成4秒9:16竖屏单镜头。第一参考图中的米杏白短款仿皮夹克胸前连续面料是唯一主体。画面从第一帧就是面料局部近景，连续的米杏白面料、自然折痕和低反光质感占画面高度70%以上，不得出现完整人物、手机、脸或下半身。

0至1秒保持稳定；1至2.5秒人物上身只轻转约5度，让一条柔和侧光缓慢扫过面料，展示哑光低反光而非镜面高光；2.5至4秒停止并保持清晰。镜头固定，不推近、不变焦、不切镜，不用手揉搓面料。

面料颜色、平整度、折痕尺度和反光强度必须跟随参考图。参考图看不清的纹理不得补充或虚构，不生成夸张皮纹、颗粒、裂纹或水滴。真实自然光，适合作为口播中的材质证据镜头。"""

MATERIAL_NEGATIVE = """禁止生成夸张皮纹、颗粒、裂纹、鳞片纹、水滴、油亮镜面高光或闪粉；禁止面料鼓包、融化、漂移、颜色跳变；禁止手部揉搓、拉扯或遮挡面料；禁止完整人物、脸、手机、下半身、切镜、推拉、变焦、字幕、文字、Logo、水印、价格和促销标签。"""

MATERIAL_MOTION = "0-1秒面料稳定 -> 1-2.5秒上身轻转约5度、柔和侧光扫过面料 -> 2.5-4秒停止并保持清晰"


def main() -> int:
    parser = argparse.ArgumentParser(description="Prepare the collar-detail 2x2 prompt/reference A/B test.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--stage2", action="store_true", help="Prepare cuff/material validation tasks using the winning full-reference + focused-prompt setup.")
    args = parser.parse_args()

    ctx = build_context()
    ready = RDSRepositorySkill(ctx).init_db()
    if not ready.success:
        return _print(ready.to_dict(), 1)
    if args.status:
        return _print(status(ctx), 0)

    source_check = _source_check()
    if not source_check["success"]:
        return _print(source_check, 1)
    planned_rows = _planned_stage2_rows() if args.stage2 else _planned_rows()
    if args.dry_run:
        return _print({"success": True, "dry_run": True, "batch_key": BATCH_KEY, "source_check": source_check, "planned": planned_rows}, 0)

    reference_packs = _ensure_reference_packs(ctx)
    if not reference_packs["success"]:
        return _print(reference_packs, 1)
    prepared = _prepare_stage(ctx, reference_packs["packs"], planned_rows)
    payload = {
        "success": not prepared["failed"],
        "batch_key": BATCH_KEY,
        "product_id": PRODUCT_ID,
        "model": MODEL,
        "reference_packs": reference_packs["packs"],
        **prepared,
    }
    report_path = STAGE2_REPORT_PATH if args.stage2 else REPORT_PATH
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return _print(payload, 0 if payload["success"] else 1)


def _source_check() -> dict[str, Any]:
    full = REFERENCE_ROOT / "01_main.jpg"
    local = REFERENCE_ROOT / "04_main.jpg"
    missing = [str(path) for path in (full, local) if not path.is_file()]
    return {"success": not missing, "full_reference": str(full), "local_reference": str(local), "missing": missing}


def _ensure_reference_packs(ctx: Any) -> dict[str, Any]:
    skill = ProductReferenceImageSkill(ctx)
    specs = {
        "full": {
            "sku_id": "AB_COLLAR_FULL",
            "sku_label": f"{BATCH_KEY}-完整穿搭参考",
            "path": REFERENCE_ROOT / "01_main.jpg",
            "image_role": "main",
        },
        "local": {
            "sku_id": "AB_COLLAR_LOCAL",
            "sku_label": f"{BATCH_KEY}-领口局部参考",
            "path": REFERENCE_ROOT / "04_main.jpg",
            "image_role": "detail",
        },
    }
    packs: dict[str, Any] = {}
    for name, spec in specs.items():
        result = skill.ensure_pack(
            PRODUCT_ID,
            market=MARKET,
            sku_id=spec["sku_id"],
            sku_label=spec["sku_label"],
            source_images=[{"path": str(spec["path"]), "image_role": spec["image_role"]}],
            source="detail_prompt_ab_test",
            anchor_snapshot={"batch_key": BATCH_KEY, "test_dimension": name, "target_detail": "立领双金属按扣"},
        )
        if not result.success:
            return {"success": False, "failed_dimension": name, "error": result.to_dict()}
        pack = dict(result.data.get("pack") or {})
        packs[name] = {
            "reference_image_pack_id": pack.get("reference_image_pack_id"),
            "reference_image_version": pack.get("version"),
            "reference_image_preview_url": pack.get("primary_preview_url"),
            "reference_image_status": "active",
            "sku_id": spec["sku_id"],
        }
    return {"success": True, "packs": packs}


def _prepare_stage(ctx: Any, packs: dict[str, Any], planned_rows: list[dict[str, Any]]) -> dict[str, Any]:
    prompt_client = resolve_client(PROMPT_WORKBENCH_URL)
    field_names = _field_names(prompt_client)
    existing_feishu = {
        _text((record.fields or {}).get("提示词包ID")): record.record_id
        for record in prompt_client.list_records(page_size=500)
        if _text((record.fields or {}).get("提示词包ID"))
    }
    factory = SegmentPromptFactorySkill(ctx)
    baseline = _baseline_package(factory, packs["full"])
    created: list[dict[str, Any]] = []
    existing: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    for spec in planned_rows:
        prompt_id = spec["segment_prompt_id"]
        pack = packs[spec["reference_type"]]
        package = _test_package(baseline, spec, pack)
        current = ctx.repo.get("segment_prompt_packages", "segment_prompt_id", prompt_id)
        if not current:
            saved = factory.save_package(package, status="pending_feishu_sync")
            if not saved.success:
                failed.append({"segment_prompt_id": prompt_id, "stage": "rds", "error": saved.to_dict()})
                continue
        record_id = existing_feishu.get(prompt_id) or str((current or {}).get("feishu_record_id") or "")
        if not record_id:
            fields = _feishu_fields(package, spec, pack)
            try:
                field_names, record_id = _safe_batch_create_prompt(prompt_client, fields, field_names)
            except Exception as exc:
                ctx.repo.update("segment_prompt_packages", "segment_prompt_id", prompt_id, {"package_status": "feishu_sync_failed", "failure_reason": str(exc)})
                failed.append({"segment_prompt_id": prompt_id, "stage": "feishu", "error": str(exc)})
                continue
        updated = ctx.repo.update(
            "segment_prompt_packages",
            "segment_prompt_id",
            prompt_id,
            {
                "package_status": "created",
                "feishu_record_id": record_id,
                "failure_reason": "",
                "submit_channel": "imini",
            },
        )
        if not updated.success:
            failed.append({"segment_prompt_id": prompt_id, "stage": "rds_finalize", "error": updated.to_dict()})
            continue
        item = {**spec, "feishu_record_id": record_id, "reference_image_pack_id": pack["reference_image_pack_id"]}
        if current or prompt_id in existing_feishu:
            existing.append(item)
        else:
            created.append(item)
    return {"created": created, "existing": existing, "failed": failed, "status": status(ctx)}


def _baseline_package(factory: SegmentPromptFactorySkill, pack: dict[str, Any]) -> dict[str, Any]:
    brief = {
        "material_anchor_brief": {
            "product_id": PRODUCT_ID,
            "sku_id": pack["sku_id"],
            "category": "womens_outerwear",
            "display_family": "女装外套",
            "product_subtype": "米杏白短款仿皮立领夹克",
            "primary_visual_result": "立领门襟带两个金属按扣，正面金属拉链顶部清楚",
            "must_show": ["立领门襟", "两个金属按扣", "正面金属拉链顶部"],
            "hard_anchors": ["米杏白短款仿皮夹克", "立领双金属按扣", "正面金属拉链"],
            "display_anchors": ["立领门襟带双按扣", "正面金属拉链"],
            "key_visual_constraints": ["按扣数量和位置不能改变", "立领和拉链结构不能改变"],
            "safe_micro_actions": ["上身轻转展示领口侧面"],
            "forbidden_actions": ["增减按扣", "改变拉链结构", "领口变形"],
        },
        "ai_local_human_brief": {
            "enabled": True,
            "micro_behavior_options": ["上身轻转约5度"],
            "body_language_options": ["局部裁切，领口优先"],
            "forbidden_performance": ["完整人物主导", "夸张广告表演"],
        },
    }
    slot = {
        "template_id": BATCH_KEY,
        "slot_index": 1,
        "slot_role": "detail",
        "hook_intent": "material_closeup",
        "ai_gen_grade": "B",
        "segment_type": "detail_atmosphere",
        "person_framing": "ai_local",
        "duration_sec": 4,
        **pack,
    }
    result = factory.build_package(brief, slot, persist=False)
    if not result.success:
        raise RuntimeError(json.dumps(result.to_dict(), ensure_ascii=False, default=str))
    return result.data


def _test_package(baseline: dict[str, Any], spec: dict[str, Any], pack: dict[str, Any]) -> dict[str, Any]:
    package = copy.deepcopy(baseline)
    package.update(
        {
            "segment_prompt_id": spec["segment_prompt_id"],
            "segment_script_id": f"SPK-{spec['segment_prompt_id'][-8:]}",
            "sku_id": pack["sku_id"],
            "reference_image_pack_id": pack["reference_image_pack_id"],
            "reference_image_version": int(pack["reference_image_version"] or 1),
            "reference_image_preview_url": pack["reference_image_preview_url"],
            "reference_image_status": "active",
            "template_id": BATCH_KEY,
            "slot_index": int(spec["slot_index"]),
            "slot_role": "detail",
            "hook_intent": "material_closeup",
            "duration_sec": 4,
            "submit_channel": "imini",
            "created_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "ab_test": {
                "batch_key": BATCH_KEY,
                "stage": int(spec.get("stage", 1)),
                "group": spec["group"],
                "repeat": spec["repeat"],
                "reference_type": spec["reference_type"],
                "prompt_type": spec["prompt_type"],
                "target_detail": spec.get("target_detail", "立领双金属按扣"),
            },
        }
    )
    if spec["prompt_type"] == "new":
        package["prompt"] = spec.get("prompt") or {"positive": NEW_POSITIVE, "negative": NEW_NEGATIVE, "motion_arc": NEW_MOTION}
    return package


def _feishu_fields(package: dict[str, Any], spec: dict[str, Any], pack: dict[str, Any]) -> dict[str, Any]:
    return _compact(
        {
            "提示词包ID": package["segment_prompt_id"],
            "商品ID": PRODUCT_ID,
            "商品名称": PRODUCT_NAME,
            "SKU ID": pack["sku_id"],
            "参考图包ID": pack["reference_image_pack_id"],
            "参考图版本": int(pack["reference_image_version"] or 1),
            "参考图预览地址": _feishu_url(pack["reference_image_preview_url"], "查看测试参考图"),
            "参考图状态": "可用",
            "市场": MARKET,
            "归一类目": "女装外套",
            "素材角色": "detail",
            "镜头意图": "material_closeup",
            "片段类型": "细节氛围",
            "生成档位": "B-支撑位",
            "包状态": "待提单",
            "人工审核结论": "待审核",
            "是否可提单": True,
            "提单优先级": "普通",
            "渠道": "Imini",
            "模型": MODEL,
            "视频比例": "9:16",
            "时长": "4S",
            "生成次数": 1,
            "已提交次数": 0,
            "短视频片段提示词": _format_prompt_package(package),
            "备注": f"{BATCH_KEY} 第{spec.get('stage', 1)}阶段；组{spec['group']}；目标={spec.get('target_detail', '立领双金属按扣')}；参考图={spec['reference_type']}；提示词={spec['prompt_type']}；重复={spec['repeat']}；只测试不发布、不自动入正式素材库",
        }
    )


def _planned_rows() -> list[dict[str, Any]]:
    definitions = {
        "A": ("full", "old"),
        "B": ("local", "old"),
        "C": ("full", "new"),
        "D": ("local", "new"),
    }
    output = []
    slot = 0
    for group, (reference_type, prompt_type) in definitions.items():
        for repeat in (1, 2):
            slot += 1
            output.append(
                {
                    "segment_prompt_id": f"DETAILAB_20260723_V1_S1_{group}_R{repeat}",
                    "slot_index": slot,
                    "group": group,
                    "repeat": repeat,
                    "reference_type": reference_type,
                    "prompt_type": prompt_type,
                    "stage": 1,
                    "target_detail": "立领双金属按扣",
                }
            )
    return output


def _planned_stage2_rows() -> list[dict[str, Any]]:
    definitions = {
        "E": ("袖口收边与走线", {"positive": CUFF_POSITIVE, "negative": CUFF_NEGATIVE, "motion_arc": CUFF_MOTION}),
        "F": ("哑光低反光面料", {"positive": MATERIAL_POSITIVE, "negative": MATERIAL_NEGATIVE, "motion_arc": MATERIAL_MOTION}),
    }
    output = []
    slot = 8
    for group, (target_detail, prompt) in definitions.items():
        for repeat in (1, 2):
            slot += 1
            output.append(
                {
                    "segment_prompt_id": f"DETAILAB_20260723_V1_S2_{group}_R{repeat}",
                    "slot_index": slot,
                    "group": group,
                    "repeat": repeat,
                    "reference_type": "full",
                    "prompt_type": "new",
                    "stage": 2,
                    "target_detail": target_detail,
                    "prompt": prompt,
                }
            )
    return output


def status(ctx: Any) -> dict[str, Any]:
    rows = ctx.repo.list_where("segment_prompt_packages", "template_id=? ORDER BY slot_index", (BATCH_KEY,))
    return {
        "batch_key": BATCH_KEY,
        "count": len(rows),
        "packages": [
            {
                "segment_prompt_id": row.get("segment_prompt_id"),
                "slot_index": row.get("slot_index"),
                "status": row.get("package_status"),
                "result_sync_status": row.get("result_sync_status"),
                "external_job_id": row.get("external_job_id"),
                "generated_asset_id": row.get("generated_asset_id"),
                "generated_segment_id": row.get("generated_segment_id"),
                "feishu_record_id": row.get("feishu_record_id"),
                "failure_reason": row.get("failure_reason"),
            }
            for row in rows
        ],
    }


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        return str(value.get("text") or value.get("link") or value.get("value") or "").strip()
    if isinstance(value, list):
        return " ".join(_text(item) for item in value if _text(item)).strip()
    return str(value).strip()


def _print(payload: dict[str, Any], code: int) -> int:
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
