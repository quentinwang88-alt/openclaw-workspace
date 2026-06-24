#!/usr/bin/env python3
"""Generate Prompt Package workbench rows from product task gaps."""
from __future__ import annotations

import argparse
import json
import re
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List

import yaml


WORKSPACE = Path("/Users/likeu3/.openclaw/workspace")
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(WORKSPACE / "skills" / "script-run-manager-sync"))

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # type: ignore  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # type: ignore  # noqa: E402

from auto_mixcut.core.bootstrap import build_context  # noqa: E402
from auto_mixcut.skills.product_reference_image_skill import ProductReferenceImageSkill  # noqa: E402
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill  # noqa: E402
from auto_mixcut.skills.segment_prompt_factory_skill import SegmentPromptFactorySkill  # noqa: E402


PRODUCT_TASK_URL = "https://gcngopvfvo0q.feishu.cn/wiki/PO2bwgrGaiOPcnkxXI8cq3fsnzg?table=tblIy2XkKc2144Pm&view=vew84aAgfU"
ANCHOR_QUEUE_URL = "https://gcngopvfvo0q.feishu.cn/wiki/V35wwjDLYiMFeTkiVFPc7SM5nvd?table=tbl2QRHwF7g9CmaF&view=vewv752AHQ"
PROMPT_WORKBENCH_URL = "https://gcngopvfvo0q.feishu.cn/wiki/PufTwQtBUizcPXk4fpycNwoOnKb?table=tblQb6SsNgYSYY8Q&view=vewIYG2wPN"


CATEGORY_CN_TO_KEY = {
    "发饰": "hair_accessories",
    "hair_accessories": "hair_accessories",
    "耳饰": "earrings",
    "耳环": "earrings",
    "earrings": "earrings",
    "手链": "bracelets",
    "手镯": "bracelets",
    "手串": "bracelets",
    "腕饰": "bracelets",
    "bracelet": "bracelets",
    "bracelets": "bracelets",
    "bangle": "bracelets",
    "wrist_accessory": "bracelets",
    "สร้อยข้อมือ": "bracelets",
    "กำไล": "bracelets",
    "项链": "necklaces",
    "吊坠项链": "necklaces",
    "颈链": "necklaces",
    "necklace": "necklaces",
    "necklaces": "necklaces",
    "pendant": "necklaces",
    "jewelry_necklace": "necklaces",
    "kalung": "necklaces",
    "สร้อยคอ": "necklaces",
    "女装轻上装": "womens_outerwear",
    "女装上衣": "womens_outerwear",
    "女装外套": "womens_outerwear",
    "womens_top": "womens_outerwear",
    "womens_tops": "womens_outerwear",
    "womens_outerwear": "womens_outerwear",
    "围巾帽子": "scarves_hats",
    "围巾/帽子": "scarves_hats",
    "scarves_hats": "scarves_hats",
    "scarf_hat": "scarves_hats",
    "小饰品": "generic_fashion",
    "general": "generic_fashion",
    "通用服饰": "generic_fashion",
}
CATEGORY_KEY_TO_CN = {
    "hair_accessories": "发饰",
    "earrings": "耳环",
    "bracelets": "手链",
    "necklaces": "项链",
    "scarves_hats": "围巾/帽子",
    "womens_outerwear": "女装外套",
    "generic_fashion": "通用服饰",
}
SEGMENT_CN = {
    "product_display": "商品展示",
    "handheld_product": "手持商品",
    "detail_atmosphere": "细节氛围",
    "tryon_result": "试戴/上身效果",
    "mirror_routine": "镜前日常",
    "home_lifestyle": "居家生活",
    "before_go_out": "出门前",
    "seasonal_scene": "季节场景",
    "product_still": "纯物静物",
    "unboxing": "拆包装",
    "flatlay": "平铺摆拍",
}
GRADE_CN = {"A": "A-核心位", "B": "B-支撑位", "C": "C-氛围位"}
STATIC_VOC_PROOF_SEGMENT_TYPES = {"product_display", "handheld_product", "product_still", "flatlay"}
_CATEGORY_CONTRACT_CACHE: Dict[str, Any] | None = None
PROMPT_RECORD_BLOCKING_STATUSES = {
    "",
    "待提单",
    "已创建",
    "created",
    "submitted",
    "已提单",
    "生成中",
    "generating",
    "已生成",
    "returned",
    "参考图异常",
}
PROMPT_RECORD_REFRESHABLE_STATUSES = {"", "待提单", "参考图异常"}


def _submit_channel_label(value: str) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"imini", "i-mini", "i_mini", "im"}:
        return "Imini"
    return "即梦"


def resolve_client(feishu_url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise RuntimeError(f"无法解析飞书 URL: {feishu_url}")
    app_token = info.app_token
    if "/wiki/" in info.original_url:
        app_token = resolve_wiki_bitable_app_token(info.app_token)
    return FeishuBitableClient(app_token=app_token, table_id=info.table_id)


def sync_workbench(
    product_task_url: str,
    anchor_queue_url: str,
    prompt_workbench_url: str,
    dry_run: bool = False,
    product_id_filter: str = "",
    max_packages_per_product: int = 6,
    refresh_existing_prompts: bool = False,
    use_voc_ads_package: bool = False,
    submit_channel: str = "jimeng",
) -> Dict[str, Any]:
    try:
        ctx = build_context()
    except Exception as exc:
        return {"created": [], "skipped": [], "failed": [{"reason": "context_init_failed", "error": str(exc)}]}
    db_ready = RDSRepositorySkill(ctx).init_db()
    if not db_ready.success:
        return {"created": [], "skipped": [], "failed": [{"reason": "rds_init_failed", "error": db_ready.to_dict()}]}
    factory = SegmentPromptFactorySkill(ctx)
    reference_images = ProductReferenceImageSkill(ctx)
    task_client = resolve_client(product_task_url)
    anchor_client = resolve_client(anchor_queue_url)
    prompt_client = resolve_client(prompt_workbench_url)
    prompt_field_names = _field_names(prompt_client)

    task_records = _latest_task_records(task_client.list_records(page_size=100))
    anchor_by_product = _index_latest_anchor(anchor_client.list_records(page_size=100))
    existing_prompt_records = _existing_prompt_records(prompt_client.list_records(page_size=100))
    existing_keys = set(existing_prompt_records)
    reference_pack_cache: Dict[tuple[str, str, str], Dict[str, Any]] = {}

    created: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    failed: List[Dict[str, Any]] = []

    for task in task_records:
        fields = task.fields or {}
        product_id = _text(fields.get("商品ID"))
        if not product_id or (product_id_filter and product_id != product_id_filter):
            continue
        voc_package = _load_ready_voc_ads_package(ctx, product_id) if use_voc_ads_package else {}
        gap_count = _gap_count(fields)
        if voc_package:
            gap_count = int(voc_package.get("requested_hook_count") or gap_count or 0)
        if gap_count <= 0:
            skipped.append({"product_id": product_id, "reason": "no_gap"})
            continue

        anchor_record = anchor_by_product.get(product_id)
        if not anchor_record:
            skipped.append({"product_id": product_id, "reason": "missing_anchor_card"})
            continue
        anchor_fields = anchor_record.fields or {}
        if _text(anchor_fields.get("人工确认状态")) not in {"已确认", "confirmed"}:
            skipped.append({"product_id": product_id, "reason": "anchor_not_confirmed", "anchor_status": _text(anchor_fields.get("人工确认状态"))})
            continue

        product_name = _text(fields.get("商品名称")) or _text(anchor_fields.get("商品名称")) or product_id
        market = _text(fields.get("市场")) or _text(anchor_fields.get("市场")) or "VN"
        sku_id = _sku_id(fields, anchor_fields)
        sku_label = _sku_label(fields, anchor_fields)
        category = _category_key(_text(fields.get("类目")) or _text(anchor_fields.get("类目")), product_name)
        category_cn = CATEGORY_KEY_TO_CN.get(category, "通用服饰")
        brief = _anchor_brief(product_id, product_name, category, anchor_fields)
        brief["material_anchor_brief"]["sku_id"] = sku_id
        if not brief["material_anchor_brief"]["hard_anchors"]:
            skipped.append({"product_id": product_id, "reason": "anchor_without_hard_anchors"})
            continue

        slots = (
            _voc_ads_hook_slots(voc_package, category, max_packages_per_product)
            if voc_package
            else _gap_slots(_text(fields.get("素材缺口说明")), category, gap_count, max_packages_per_product)
        )
        reference_key = (market, product_id, sku_id)
        reference_pack = reference_pack_cache.get(reference_key)
        if reference_pack is None:
            reference_pack = _ensure_reference_pack(reference_images, anchor_client, product_id, market, sku_id, sku_label, anchor_fields, fields, dry_run)
            reference_pack_cache[reference_key] = reference_pack
        for slot in slots:
            slot["sku_id"] = sku_id
            slot["reference_image_pack_id"] = reference_pack.get("reference_image_pack_id", "")
            slot["reference_image_version"] = reference_pack.get("reference_image_version", 0)
            slot["reference_image_preview_url"] = reference_pack.get("primary_preview_url", "")
            slot["reference_image_status"] = reference_pack.get("reference_image_status", "缺失")
        if not reference_pack.get("reference_image_pack_id") and not dry_run:
            skipped.append({"product_id": product_id, "reason": "reference_image_pack_missing", "sku_id": sku_id, "detail": reference_pack.get("error")})
            continue

        for idx, slot in enumerate(slots):
            dedupe_key = _prompt_dedupe_key(product_id, sku_id, slot)
            legacy_dedupe_key = _legacy_prompt_dedupe_key(product_id, sku_id, slot)
            role_dedupe_key = _role_prompt_dedupe_key(product_id, sku_id, slot)
            existing_key = dedupe_key if dedupe_key in existing_keys else (legacy_dedupe_key if legacy_dedupe_key in existing_keys else None)
            if not existing_key and refresh_existing_prompts and str(slot.get("slot_role") or "") in {"detail", "result"} and role_dedupe_key in existing_keys:
                existing_key = role_dedupe_key
            if existing_key:
                if refresh_existing_prompts:
                    existing_record = existing_prompt_records[existing_key]
                    existing_status = _text((existing_record.fields or {}).get("包状态"))
                    if not _prompt_record_refreshable(existing_status):
                        skipped.append({
                            "product_id": product_id,
                            "reason": "existing_prompt_not_refreshable",
                            "key": "|".join(existing_key),
                            "status": existing_status,
                        })
                        continue
                    refresh = _refresh_existing_prompt(
                        factory,
                        prompt_client,
                        existing_record,
                        _brief_with_voc_candidate(brief, slot),
                        slot,
                        dry_run,
                        submit_channel,
                    )
                    if refresh.get("failed"):
                        failed.append({"product_id": product_id, **refresh["failed"]})
                    else:
                        skipped.append({"product_id": product_id, "reason": "refreshed_existing_prompt", "key": "|".join(existing_key), **refresh})
                    continue
                skipped.append({"product_id": product_id, "reason": "already_exists", "key": "|".join(existing_key)})
                continue
            slot_brief = _brief_with_voc_candidate(brief, slot)
            package_result = factory.build_package(slot_brief, slot, persist=False)
            if not package_result.success:
                failed.append({"product_id": product_id, "reason": "prompt_build_failed", "error": package_result.to_dict()})
                continue
            package = package_result.data
            package["segment_script_id"] = _segment_script_id(package["segment_prompt_id"])
            if slot.get("voc_hook_candidate"):
                package["voc_hook"] = _slot_voc_meta(slot)
            reference_ready = _package_reference_ready(package)
            row_fields = {
                "提示词包ID": package["segment_prompt_id"],
                "商品ID": product_id,
                "商品名称": product_name,
                "SKU ID": sku_id,
                "参考图包ID": reference_pack.get("reference_image_pack_id", ""),
                "参考图版本": reference_pack.get("reference_image_version", 0),
                "参考图预览地址": _feishu_url(reference_pack.get("primary_preview_url", ""), "查看参考图"),
                "参考图状态": reference_pack.get("reference_image_status", "缺失"),
                "市场": market,
                "归一类目": category_cn,
                "素材角色": slot.get("slot_role") or "",
                "镜头意图": slot.get("hook_intent") or "",
                "片段类型": SEGMENT_CN.get(package["segment_type"], package["segment_type"]),
                "生成档位": GRADE_CN.get(package["ai_gen_grade"], package["ai_gen_grade"]),
                "包状态": "待提单" if reference_ready else "参考图异常",
                "人工审核结论": "待审核",
                "是否可提单": reference_ready,
                "提单优先级": _priority(fields),
                "渠道": _submit_channel_label(submit_channel),
                "短视频片段提示词": _format_prompt_package(package),
                "备注": _note(fields, idx + 1, len(slots), slot.get("voc_hook_candidate")),
            }
            existing_keys.add(dedupe_key)
            if dry_run:
                created.append({"product_id": product_id, "segment_prompt_id": package["segment_prompt_id"], "fields": _without_large_prompt(row_fields)})
            else:
                try:
                    # Phase 1: save RDS with pending status
                    saved = factory.save_package(package)
                    if not saved.success:
                        existing_keys.discard(dedupe_key)
                        failed.append({"product_id": product_id, "segment_prompt_id": package["segment_prompt_id"], "reason": "prompt_package_rds_failed", "error": saved.to_dict()})
                        continue
                    ctx.repo.update(
                        "segment_prompt_packages", "segment_prompt_id",
                        package["segment_prompt_id"],
                        {"package_status": "pending_feishu_sync"},
                    )

                    # Phase 2: create Feishu record
                    try:
                        prompt_field_names, feishu_record_id = _safe_batch_create_prompt(prompt_client, row_fields, prompt_field_names)
                        ctx.repo.update(
                            "segment_prompt_packages", "segment_prompt_id",
                            package["segment_prompt_id"],
                            {"package_status": "created", "feishu_record_id": feishu_record_id, "failure_reason": ""},
                        )
                        created.append({
                            "product_id": product_id,
                            "segment_prompt_id": package["segment_prompt_id"],
                            "segment_type": row_fields["片段类型"],
                            "grade": row_fields["生成档位"],
                            "feishu_record_id": feishu_record_id,
                            "voc_hook": package.get("voc_hook") or {},
                        })
                    except Exception as exc:
                        ctx.repo.update(
                            "segment_prompt_packages", "segment_prompt_id",
                            package["segment_prompt_id"],
                            {"package_status": "feishu_sync_failed", "failure_reason": str(exc)},
                        )
                        existing_keys.discard(dedupe_key)
                        failed.append({"product_id": product_id, "segment_prompt_id": package["segment_prompt_id"], "reason": "feishu_create_failed", "error": str(exc)})
                except Exception as exc:
                    existing_keys.discard(dedupe_key)
                    failed.append({"product_id": product_id, "segment_prompt_id": package["segment_prompt_id"], "reason": "rds_write_failed", "error": str(exc)})

    return {"created": created, "skipped": skipped, "failed": failed}


def _safe_batch_create_prompt(client: FeishuBitableClient, row_fields: Dict[str, Any], field_names: set[str]):
    """Create a single Feishu record. Returns (field_names, record_id)."""
    try:
        payload = {"fields": _compact(_filter_fields(row_fields, field_names))}
        ids = client.batch_create_records([payload])
        if ids:
            return field_names, ids[0]
        raise RuntimeError("飞书创建成功但未返回 record_id")
    except Exception as exc:
        if "FieldNameNotFound" not in str(exc):
            raise
        latest = _field_names(client)
        payload = {"fields": _compact(_filter_fields(row_fields, latest))}
        ids = client.batch_create_records([payload])
        if ids:
            return latest, ids[0]
        raise RuntimeError("飞书创建成功但未返回 record_id")


def _filter_fields(row_fields: Dict[str, Any], field_names: set[str]) -> Dict[str, Any]:
    if not field_names:
        return row_fields
    return {key: value for key, value in row_fields.items() if key in field_names}


def _field_names(client: FeishuBitableClient) -> set[str]:
    try:
        return {field.field_name for field in client.list_fields()}
    except Exception:
        return set()


def _index_latest_anchor(records: Iterable[Any]) -> Dict[str, Any]:
    grouped: Dict[str, List[Any]] = {}
    for record in records:
        product_id = _text((record.fields or {}).get("商品ID"))
        if product_id:
            grouped.setdefault(product_id, []).append(record)
    indexed: Dict[str, Any] = {}
    for product_id, items in grouped.items():
        confirmed = [
            item
            for item in items
            if _text((item.fields or {}).get("人工确认状态")) in {"已确认", "confirmed"}
        ]
        selected = confirmed[-1] if confirmed else items[-1]
        if not _reference_image_attachments(selected.fields or {}, {})[1]:
            image_record = next(
                (item for item in reversed(items) if _reference_image_attachments(item.fields or {}, {})[1]),
                None,
            )
            if image_record:
                selected_fields = selected.fields or {}
                image_fields = image_record.fields or {}
                for field_name in ("商品主图", "产品图片", "商品图片", "图片", "主图", "参考图"):
                    if not selected_fields.get(field_name) and image_fields.get(field_name):
                        selected_fields[field_name] = image_fields.get(field_name)
                        break
        indexed[product_id] = selected
    return indexed


def _latest_task_records(records: Iterable[Any]) -> List[Any]:
    indexed: Dict[str, Any] = {}
    for record in records:
        fields = record.fields or {}
        product_id = _text(fields.get("商品ID"))
        if not product_id:
            continue
        current = indexed.get(product_id)
        if current is None or _task_sort_key(fields) >= _task_sort_key(current.fields or {}):
            indexed[product_id] = record
    return list(indexed.values())


def _task_sort_key(fields: Dict[str, Any]) -> tuple[int, str]:
    task_no = _int(fields.get("任务编号"))
    status = _text(fields.get("混剪状态"))
    return (task_no, status)


def _existing_prompt_records(records: Iterable[Any]) -> Dict[tuple[str, str, str, str, str, str], Any]:
    indexed: Dict[tuple[str, str, str, str, str, str], Any] = {}
    for record in records:
        fields = record.fields or {}
        product_id = _text(fields.get("商品ID"))
        sku_id = _text(fields.get("SKU ID")) or "DEFAULT"
        role = _text(fields.get("素材角色")) or _text(fields.get("片段角色"))
        segment_type = _text(fields.get("片段类型"))
        grade = _text(fields.get("生成档位"))
        hook_intent = _text(fields.get("镜头意图")) or _text(fields.get("Hook意图"))
        status = _text(fields.get("包状态"))
        if product_id and segment_type and grade and _prompt_record_blocks_new_package(status):
            indexed[(product_id, sku_id, role, segment_type, grade, hook_intent)] = record
            if not role and not hook_intent:
                indexed[(product_id, sku_id, "", segment_type, grade, "")] = record
            if role in {"detail", "result"} and hook_intent:
                indexed[(product_id, sku_id, role, "", grade, hook_intent)] = record
    return indexed


def _prompt_record_blocks_new_package(status: str) -> bool:
    return _text(status) in PROMPT_RECORD_BLOCKING_STATUSES


def _prompt_record_refreshable(status: str) -> bool:
    return _text(status) in PROMPT_RECORD_REFRESHABLE_STATUSES


def _prompt_dedupe_key(product_id: str, sku_id: str, slot: Dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    return (
        product_id,
        sku_id or "DEFAULT",
        _text(slot.get("slot_role") or slot.get("role")),
        SEGMENT_CN.get(slot["segment_type"], slot["segment_type"]),
        GRADE_CN.get(slot["ai_gen_grade"], slot["ai_gen_grade"]),
        _text(slot.get("hook_intent")),
    )


def _legacy_prompt_dedupe_key(product_id: str, sku_id: str, slot: Dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    return (
        product_id,
        sku_id or "DEFAULT",
        "",
        SEGMENT_CN.get(slot["segment_type"], slot["segment_type"]),
        GRADE_CN.get(slot["ai_gen_grade"], slot["ai_gen_grade"]),
        "",
    )


def _role_prompt_dedupe_key(product_id: str, sku_id: str, slot: Dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    return (
        product_id,
        sku_id or "DEFAULT",
        _text(slot.get("slot_role") or slot.get("role")),
        "",
        GRADE_CN.get(slot["ai_gen_grade"], slot["ai_gen_grade"]),
        _text(slot.get("hook_intent")),
    )


def _refresh_existing_prompt(
    factory: SegmentPromptFactorySkill,
    prompt_client: FeishuBitableClient,
    record: Any,
    brief: Dict[str, Any],
    slot: Dict[str, Any],
    dry_run: bool,
    submit_channel: str = "jimeng",
) -> Dict[str, Any]:
    package_result = factory.build_package(brief, slot, persist=False)
    if not package_result.success:
        return {"failed": {"reason": "prompt_refresh_build_failed", "error": package_result.to_dict()}}
    package = package_result.data
    fields = record.fields or {}
    existing_prompt_id = _text(fields.get("提示词包ID"))
    if existing_prompt_id:
        package["segment_prompt_id"] = existing_prompt_id
    package["segment_script_id"] = _segment_script_id(package["segment_prompt_id"])
    if slot.get("voc_hook_candidate"):
        package["voc_hook"] = _slot_voc_meta(slot)
    update_fields = {
        "提示词包ID": package["segment_prompt_id"],
        "SKU ID": package.get("sku_id") or "DEFAULT",
        "素材角色": slot.get("slot_role") or "",
        "镜头意图": slot.get("hook_intent") or "",
        "片段类型": SEGMENT_CN.get(package["segment_type"], package["segment_type"]),
        "生成档位": GRADE_CN.get(package["ai_gen_grade"], package["ai_gen_grade"]),
        "参考图包ID": package.get("reference_image_pack_id") or "",
        "参考图版本": package.get("reference_image_version") or 0,
        "参考图预览地址": _feishu_url(package.get("reference_image_preview_url") or "", "查看参考图"),
        "参考图状态": package.get("reference_image_status") or "缺失",
        "渠道": _submit_channel_label(submit_channel),
        "短视频片段提示词": _format_prompt_package(package),
        "备注": _note(fields, 1, 1, slot.get("voc_hook_candidate")),
    }
    if not _package_reference_ready(package):
        update_fields["包状态"] = "参考图异常"
        update_fields["是否可提单"] = False
    if dry_run:
        return {
            "record_id": record.record_id,
            "segment_prompt_id": package["segment_prompt_id"],
            "action": "would_refresh",
            "fields": _without_large_prompt(update_fields),
        }
    saved = factory.save_package(package)
    if not saved.success:
        return {"failed": {"reason": "prompt_refresh_rds_failed", "error": saved.to_dict()}}
    prompt_client.update_record_fields(record.record_id, update_fields)
    return {"record_id": record.record_id, "segment_prompt_id": package["segment_prompt_id"], "action": "refreshed"}


def _gap_count(fields: Dict[str, Any]) -> int:
    gap_text = _text(fields.get("素材缺口说明"))
    explicit = _explicit_ai_supplement_count(gap_text)
    if explicit > 0:
        return explicit
    target = _int(fields.get("目标生成数量"))
    actual = _int(fields.get("实际生成数量"))
    material_status = _text(fields.get("素材状态"))
    if target > actual and material_status in {"not_ready", "blocked", "review_required"}:
        return target - actual
    if gap_text or material_status in {"not_ready", "blocked", "review_required"}:
        return 1
    return 0


def _load_ready_voc_ads_package(ctx: Any, product_id: str) -> Dict[str, Any]:
    try:
        with ctx.repo.connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT package_id, product_id, requested_hook_count, payload_json, updated_at
                FROM voc_ads_hook_package
                WHERE product_id=%s
                  AND usecase=%s
                  AND readiness_status=%s
                  AND manual_confirmation_status LIKE %s
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (product_id, "ads_mixcut", "ready_for_hook_package", "confirmed%"),
            )
            row = cur.fetchone()
    except Exception:
        return {}
    if not row:
        return {}
    payload = _json_loads(row.get("payload_json"))
    if not payload:
        return {}
    payload.setdefault("package_id", row.get("package_id"))
    payload.setdefault("product_id", row.get("product_id"))
    payload.setdefault("requested_hook_count", row.get("requested_hook_count"))
    payload.setdefault("updated_at", row.get("updated_at"))
    return payload


def _json_loads(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="ignore")
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _voc_ads_hook_slots(voc_package: Dict[str, Any], category: str, max_packages_per_product: int) -> List[Dict[str, Any]]:
    candidates = [item for item in (voc_package.get("hook_candidates") or []) if isinstance(item, dict)]
    target = min(max(1, int(voc_package.get("requested_hook_count") or 1)), max(max_packages_per_product, 1))
    slots: List[Dict[str, Any]] = []
    for candidate in candidates:
        count = max(1, int(candidate.get("requested_hook_count") or 1))
        plans = _voc_slot_plans(candidate, category)
        for offset in range(count):
            if len(slots) >= target:
                return slots
            segment_type, grade, role = plans[offset % len(plans)]
            hook_intent = _text(candidate.get("hook_intent")) or "product_clarity"
            slot = _slot(len(slots), segment_type, grade, role, hook_intent)
            slot.update(
                {
                    "template_id": "VOC_ADS_HOOK_PACKAGE",
                    "is_hook": True,
                    "voc_ads_package_id": voc_package.get("package_id"),
                    "voc_hook_candidate": candidate,
                    "voc_hook_variant_index": offset + 1,
                    "voc_hook_variant_total": count,
                }
            )
            slots.append(slot)
    return slots


def _voc_slot_plans(candidate: Dict[str, Any], category: str) -> List[tuple[str, str, str]]:
    allowed = _listish(candidate.get("allowed_segment_types"))
    if allowed:
        if _voc_candidate_needs_action_proof(candidate):
            allowed = [seg for seg in allowed if seg not in STATIC_VOC_PROOF_SEGMENT_TYPES]
        if not allowed:
            return _voc_action_proof_slot_plans(candidate, category)
        roles = ["hero", "result", "detail", "hero"]
        plans = [(seg, "A" if i < 2 else "B", roles[i]) for i, seg in enumerate(allowed[:4])]
        if plans:
            filtered = _filter_voc_plans_for_category(plans, category)
            if filtered:
                return filtered
    action_plans = _voc_action_proof_slot_plans(candidate, category)
    if action_plans:
        filtered = _filter_voc_plans_for_category(action_plans, category)
        if filtered:
            return filtered
    fallback = [(segment, grade, role) for segment, grade, role, _intent in _slot_plans_for_role("hero", category)]
    return _filter_voc_plans_for_category(fallback, category) or fallback


def _voc_candidate_needs_action_proof(candidate: Dict[str, Any]) -> bool:
    insight_id = _text(candidate.get("insight_id"))
    hook_intent = _text(candidate.get("hook_intent"))
    required_action = _text(candidate.get("required_action_zh"))
    return bool(required_action) or insight_id in {"selling_appearance_cute_color", "selling_hold_quality"} or hook_intent in {"tryon_result", "contrast_reveal"}


def _voc_action_proof_slot_plans(candidate: Dict[str, Any], category: str) -> List[tuple[str, str, str]]:
    insight_id = _text(candidate.get("insight_id"))
    hook_intent = _text(candidate.get("hook_intent"))
    if insight_id == "selling_appearance_cute_color" or hook_intent == "tryon_result":
        return [
            ("tryon_result", "A", "hero"),
            ("mirror_routine", "A", "hero"),
            ("before_go_out", "A", "result"),
            ("detail_atmosphere", "B", "detail"),
        ]
    if insight_id == "selling_hold_quality" or hook_intent == "contrast_reveal":
        return [
            ("tryon_result", "A", "hero"),
            ("mirror_routine", "A", "hero"),
            ("before_go_out", "A", "result"),
            ("detail_atmosphere", "B", "detail"),
        ]
    return []


def _filter_voc_plans_for_category(plans: List[tuple[str, str, str]], category: str) -> List[tuple[str, str, str]]:
    contract = _category_execution_contract(category)
    allowed = set(_listish(contract.get("allowed_segment_types")))
    forbidden = set(_listish(contract.get("forbidden_segment_types")))
    filtered = []
    for segment_type, grade, role in plans:
        if segment_type in forbidden:
            continue
        if allowed and segment_type not in allowed:
            continue
        filtered.append((segment_type, grade, role))
    return filtered


def _category_execution_contract(category: str) -> Dict[str, Any]:
    global _CATEGORY_CONTRACT_CACHE
    if _CATEGORY_CONTRACT_CACHE is None:
        path = REPO_ROOT / "config" / "ai_segment_factory.yaml"
        try:
            with open(path, "r", encoding="utf-8") as fh:
                config = yaml.safe_load(fh) or {}
        except Exception:
            config = {}
        _CATEGORY_CONTRACT_CACHE = config.get("category_execution_contract") or {}
    category_key = _category_key(category, "")
    return dict((_CATEGORY_CONTRACT_CACHE or {}).get(category_key) or (_CATEGORY_CONTRACT_CACHE or {}).get(category) or {})


def _brief_with_voc_candidate(brief: Dict[str, Any], slot: Dict[str, Any]) -> Dict[str, Any]:
    candidate = slot.get("voc_hook_candidate") if isinstance(slot.get("voc_hook_candidate"), dict) else {}
    if not candidate:
        return brief
    cloned = json.loads(json.dumps(brief, ensure_ascii=False, default=str))
    material = cloned.get("material_anchor_brief") or cloned
    selling_point = _text(candidate.get("product_selling_point"))
    visual_goal = _text(candidate.get("visual_goal") or candidate.get("visual_goal_zh") or candidate.get("visual_proof_zh") or selling_point)
    required_action = _text(candidate.get("required_action_zh"))
    proof_shots = _listish(candidate.get("shot_plan")) or _listish(candidate.get("proof_shot_list"))
    if visual_goal:
        material["primary_visual_result"] = visual_goal
        must_show = _listish(material.get("must_show"))
        proof_constraints = []
        proof_constraints.append(f"VOC证明目标：{visual_goal}")
        if required_action:
            proof_constraints.append(f"VOC证明动作：{required_action}")
        proof_constraints.extend(f"VOC证明镜头：{shot}" for shot in proof_shots[:3])
        if selling_point:
            proof_constraints.append(f"VOC来源卖点：{selling_point}")
        material["must_show"] = _dedupe([*proof_constraints, *must_show])
    safe_micro = _listish(candidate.get("safe_micro_actions")) or _voc_safe_micro_actions(candidate)
    material["safe_micro_actions"] = _dedupe(safe_micro + _listish(material.get("safe_micro_actions")))
    neg_constraints = _listish(candidate.get("negative_constraints"))
    if neg_constraints:
        material["forbidden_actions"] = _dedupe(neg_constraints + _listish(material.get("forbidden_actions")))
    material["voc_ads_hook"] = _slot_voc_meta(slot)
    if "material_anchor_brief" in cloned:
        cloned["material_anchor_brief"] = material
    return cloned


def _voc_safe_micro_actions(candidate: Dict[str, Any]) -> List[str]:
    safe = _listish(candidate.get("safe_micro_actions"))
    if safe:
        return _dedupe(safe)
    actions = []
    required_action = _text(candidate.get("required_action_zh"))
    if required_action:
        actions.append(required_action)
    actions.extend(_listish(candidate.get("proof_shot_list"))[:4])
    if actions:
        return _dedupe(actions)
    return []


def _slot_voc_meta(slot: Dict[str, Any]) -> Dict[str, Any]:
    candidate = slot.get("voc_hook_candidate") if isinstance(slot.get("voc_hook_candidate"), dict) else {}
    return {
        "voc_ads_package_id": slot.get("voc_ads_package_id"),
        "candidate_id": candidate.get("candidate_id"),
        "insight_id": candidate.get("insight_id"),
        "insight_role": candidate.get("insight_role"),
        "product_selling_point": candidate.get("product_selling_point"),
        "voc_signal": candidate.get("voc_signal"),
        "proof_archetype": candidate.get("proof_archetype"),
        "usage_lane": candidate.get("usage_lane"),
        "video_fit_score": candidate.get("video_fit_score"),
        "visual_proof_zh": candidate.get("visual_goal") or candidate.get("visual_proof_zh"),
        "required_action_zh": candidate.get("required_action_zh"),
        "proof_shot_list": candidate.get("shot_plan") or candidate.get("proof_shot_list") or [],
        "forbidden_claims": candidate.get("forbidden_claims") or [],
        "category_adapter": candidate.get("category_adapter"),
        "category_reference_title": candidate.get("category_reference_title"),
        "category_reference_local_voice": candidate.get("category_reference_local_voice"),
        "category_reference_hooks": candidate.get("category_reference_hooks") or [],
        "requested_hook_count": candidate.get("requested_hook_count"),
        "variant_index": slot.get("voc_hook_variant_index"),
        "variant_total": slot.get("voc_hook_variant_total"),
    }


def _gap_slots(gap_text: str, category: str, count: int, max_packages_per_product: int) -> List[Dict[str, Any]]:
    lower = gap_text.lower()
    planned: List[tuple[str, str, str, str]] = []
    explicit_roles = _explicit_ai_supplement_roles(gap_text)
    for role, amount in explicit_roles:
        role_plans = _slot_plans_for_role(role, category)
        for index in range(max(1, amount)):
            planned.append(role_plans[index % len(role_plans)])
    if not explicit_roles:
        if "hero" in lower or "首镜" in gap_text:
            planned.append(("product_display", "A", "hero", "product_clarity"))
        if "result" in lower or "效果" in gap_text or "佩戴" in gap_text or "上身" in gap_text:
            planned.append(("tryon_result", "A", "result", "tryon_result"))
        if "detail" in lower or "细节" in gap_text:
            planned.append(("detail_atmosphere", "B", "detail", "material_closeup"))
        if "scene" in lower or "场景" in gap_text:
            planned.append(_slot_plan_for_role("scene", category))
    if "usable" in lower or "可用" in gap_text or "多样性" in gap_text or not planned:
        planned.extend(_default_slot_plan(category))

    unique: List[tuple[str, str, str, str]] = []
    for item in planned:
        if item not in unique:
            unique.append(item)
    target = min(max(count, 1), max(max_packages_per_product, 1))
    while len(unique) < target:
        before = len(unique)
        for item in _default_slot_plan(category):
            if len(unique) >= target:
                break
            if item not in unique:
                unique.append(item)
        if len(unique) == before:
            break
    return [_slot(idx, *item) for idx, item in enumerate(unique[:target])]


def _explicit_ai_supplement_count(gap_text: str) -> int:
    return sum(amount for _role, amount in _explicit_ai_supplement_roles(gap_text))


def _explicit_ai_supplement_roles(gap_text: str) -> List[tuple[str, int]]:
    if "AI补素材" not in gap_text and "ai补素材" not in gap_text.lower():
        return []
    role_aliases = {
        "hero": ["hero", "首镜"],
        "detail": ["detail", "细节"],
        "result": ["result", "上身", "效果", "试穿", "试戴"],
        "scene": ["scene", "场景"],
        "ending": ["ending", "结尾"],
    }
    results: List[tuple[str, int]] = []
    for role, aliases in role_aliases.items():
        amount = 0
        for alias in aliases:
            for match in re.finditer(rf"{re.escape(alias)}\D{{0,8}}(\d+)", gap_text, flags=re.IGNORECASE):
                amount = max(amount, int(match.group(1)))
        if amount > 0:
            results.append((role, amount))
    return results


def _slot_plan_for_role(role: str, category: str) -> tuple[str, str, str, str]:
    return _slot_plans_for_role(role, category)[0]


def _slot_plans_for_role(role: str, category: str) -> List[tuple[str, str, str, str]]:
    if role == "hero":
        plans = [
            ("product_display", "A", "hero", "product_clarity"),
            ("unboxing", "A", "hero", "product_clarity"),
            ("product_still", "A", "hero", "product_clarity"),
        ]
        if category in {"earrings", "bracelets", "necklaces"}:
            plans[1] = ("product_still", "A", "hero", "product_clarity")
            plans[2] = ("flatlay", "A", "hero", "product_clarity")
        return plans
    if role == "result":
        if category in {"earrings", "bracelets", "necklaces"}:
            return [
                ("mirror_routine", "A", "result", "tryon_result"),
                ("product_display", "A", "result", "tryon_result"),
                ("product_still", "A", "result", "product_clarity"),
            ]
        return [
            ("tryon_result", "A", "result", "tryon_result"),
            ("mirror_routine", "A", "result", "tryon_result"),
            ("before_go_out", "A", "result", "tryon_result"),
        ]
    if role == "detail":
        if category in {"earrings", "bracelets", "necklaces"}:
            return [
                ("product_still", "B", "detail", "material_closeup"),
                ("flatlay", "B", "detail", "material_closeup"),
                ("product_display", "B", "detail", "material_closeup"),
            ]
        if category == "womens_outerwear":
            return [
                ("product_still", "B", "detail", "material_closeup"),
                ("detail_atmosphere", "B", "detail", "material_closeup"),
                ("flatlay", "B", "detail", "material_closeup"),
            ]
        return [
            ("detail_atmosphere", "B", "detail", "material_closeup"),
            ("product_still", "B", "detail", "material_closeup"),
            ("flatlay", "B", "detail", "material_closeup"),
        ]
    if role == "ending":
        if category in {"earrings", "bracelets", "necklaces"}:
            return [
                ("flatlay", "C", "ending", "atmosphere"),
                ("product_still", "C", "ending", "atmosphere"),
            ]
        return [
            ("home_lifestyle", "C", "ending", "atmosphere"),
            ("seasonal_scene", "C", "ending", "atmosphere"),
        ]
    if category in {"earrings", "bracelets"}:
        return [
            ("flatlay", "C", "scene", "atmosphere"),
            ("product_still", "C", "scene", "atmosphere"),
            ("mirror_routine", "C", "scene", "atmosphere"),
        ]
    return [
        ("home_lifestyle", "C", "scene", "atmosphere"),
        ("seasonal_scene", "C", "scene", "atmosphere"),
        ("mirror_routine", "C", "scene", "atmosphere"),
    ]


def _default_slot_plan(category: str) -> List[tuple[str, str, str, str]]:
    if category in {"earrings", "bracelets"}:
        return [
            ("product_display", "A", "hero", "product_clarity"),
            ("product_still", "B", "detail", "material_closeup"),
            ("flatlay", "B", "detail", "material_closeup"),
            ("mirror_routine", "A", "result", "tryon_result"),
            ("product_display", "B", "detail", "material_closeup"),
        ]
    if category == "scarves_hats":
        return [
            ("product_display", "A", "hero", "product_clarity"),
            ("product_still", "B", "detail", "material_closeup"),
            ("tryon_result", "B", "result", "tryon_result"),
            ("flatlay", "B", "detail", "material_closeup"),
            ("seasonal_scene", "C", "scene", "atmosphere"),
        ]
    if category == "womens_outerwear":
        return [
            ("product_display", "A", "hero", "product_clarity"),
            ("product_still", "B", "detail", "material_closeup"),
            ("detail_atmosphere", "B", "detail", "material_closeup"),
            ("tryon_result", "B", "result", "tryon_result"),
            ("unboxing", "A", "hero", "product_clarity"),
            ("mirror_routine", "C", "scene", "atmosphere"),
            ("home_lifestyle", "C", "ending", "atmosphere"),
            ("seasonal_scene", "C", "scene", "atmosphere"),
        ]
    return [
        ("product_display", "A", "hero", "product_clarity"),
        ("product_still", "B", "detail", "material_closeup"),
        ("detail_atmosphere", "B", "detail", "material_closeup"),
        ("home_lifestyle", "C", "scene", "atmosphere"),
    ]


def _slot(index: int, segment_type: str, grade: str, role: str, hook_intent: str) -> Dict[str, Any]:
    return {
        "template_id": "PROMPT_WORKBENCH_GAP_SYNC",
        "slot_index": index,
        "slot_role": role,
        "hook_intent": hook_intent,
        "ai_gen_grade": grade,
        "segment_type": segment_type,
        "person_framing": "product_only" if segment_type in {"product_still", "unboxing", "flatlay"} else ("ai_local" if grade in {"A", "B"} else "real_preferred"),
        "duration_sec": 4,
    }


def _anchor_brief(product_id: str, product_name: str, category: str, anchor_fields: Dict[str, Any]) -> Dict[str, Any]:
    anchor_json = _jsonish(anchor_fields.get("AI生成锚点卡"))
    core_points = (
        _anchor_texts(anchor_fields.get("核心视觉点"))
        or _anchor_texts(anchor_json.get("core_visual_points"))
        or _anchor_texts(anchor_json.get("hard_anchors"), ("anchor", "constraint", "text"))
        or _anchor_texts(anchor_json.get("structure_anchors"))
        or _anchor_texts(anchor_json.get("display_anchors"), ("anchor", "why_must_show", "text"))
    )
    must_not_change = (
        _anchor_texts(anchor_fields.get("不可错识别点"))
        or _anchor_texts(anchor_json.get("must_not_change_points"))
        or _anchor_texts(anchor_json.get("key_visual_constraints"), ("constraint", "anchor", "text"))
        or _anchor_texts(anchor_json.get("fixation_result_anchors"))
        or core_points
    )
    forbidden = (
        _anchor_texts(anchor_fields.get("禁用错配项"))
        or _anchor_texts(anchor_json.get("forbidden_mismatch"))
        or _anchor_texts(anchor_json.get("forbidden_actions"))
        or _anchor_texts(anchor_json.get("distortion_alerts"))
    )
    hard = _dedupe(core_points + must_not_change)
    # 改动3B：从锚点卡提取 AI 分析的卖点，注入 primary_visual_result / must_show。
    # candidate_primary_selling_points 来自 original-script-generator P1 锚点卡。
    # 卖点经 _selling_positive_layer 融进即梦提示词 positive。
    selling_points = _extract_selling_points(anchor_json)
    primary_visual = "；".join(core_points[:3]) or product_name
    must_show = core_points
    if selling_points:
        primary_visual = selling_points[0] or primary_visual
        must_show = selling_points + must_show
    return {
        "material_anchor_brief": {
            "product_id": product_id,
            "display_family": CATEGORY_KEY_TO_CN.get(category, category),
            "product_subtype": _product_label(product_name, category),
            "category": category,
            "primary_visual_result": primary_visual,
            "must_show": must_show,
            "must_not_show": forbidden,
            "hard_anchors": hard,
            "display_anchors": core_points,
            "key_visual_constraints": must_not_change or core_points,
            "safe_micro_actions": ["自然手持展示", "小幅试穿或细节近景动作"],
            "forbidden_actions": forbidden,
        },
        "ai_local_human_brief": {
            "enabled": True,
            "gaze_options": ["自然看向旁边，避免直视镜头"],
            "micro_behavior_options": ["手部小幅整理商品"],
            "body_language_options": ["局部裁切，商品优先"],
            "forbidden_performance": ["夸张广告表演", "正脸主导的美妆广告感"],
        },
}


def _anchor_texts(value: Any, keys: tuple[str, ...] = ("anchor", "constraint", "text")) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        result: List[str] = []
        for item in value:
            if isinstance(item, dict):
                for key in keys:
                    text = _text(item.get(key))
                    if text:
                        result.append(text)
                        break
            else:
                result.extend(_listish(item))
        return _dedupe(result)
    if isinstance(value, dict):
        for key in keys:
            text = _text(value.get(key))
            if text:
                return [text]
    return _listish(value)


def _extract_selling_points(anchor_json: Dict[str, Any]) -> List[str]:
    """从锚点卡提取 AI 分析的卖点（candidate_primary_selling_points）。

    来自 original-script-generator P1 锚点卡。提取 selling_point 文本。
    不做规则转译（留待 LLM 升级），直接输出中文卖点。
    _selling_positive_layer 会以"核心视觉结果：xxx；必须出现：xxx"格式融进 prompt。
    """
    candidates = anchor_json.get("candidate_primary_selling_points")
    if not candidates or not isinstance(candidates, list):
        return []
    result: List[str] = []
    for item in candidates:
        if not isinstance(item, dict):
            continue
        sp = _text(item.get("selling_point"))
        if sp:
            result.append(sp)
    return result[:3]  # 最多 3 个卖点，避免 prompt 过长


def _dedupe(values: List[str]) -> List[str]:
    seen = set()
    result = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return result


def _product_label(product_name: str, category: str) -> str:
    name = product_name.strip()
    if name and _contains_cjk(name):
        return name
    return CATEGORY_KEY_TO_CN.get(category, "商品")


def _contains_cjk(text: str) -> bool:
    return any("\u4e00" <= char <= "\u9fff" for char in text)


def _format_prompt_package(package: Dict[str, Any]) -> str:
    prompt = package.get("prompt") or {}
    anchor = package.get("anchor_ref") or {}
    script_id = package.get("segment_script_id") or _segment_script_id(package.get("segment_prompt_id"))
    parts = [
        f"片段脚本ID：{script_id}",
        f"正向提示词：\n{prompt.get('positive') or ''}",
        f"负向提示词：\n{prompt.get('negative') or ''}",
        f"运镜/动作弧线：\n{prompt.get('motion_arc') or ''}",
        f"参考图锚点提示：\n{_join(_short_list(anchor.get('hard_anchors'), 2))}",
    ]
    return "\n\n".join(part for part in parts if not part.endswith("\n"))


def _segment_script_id(segment_prompt_id: Any) -> str:
    compact = "".join(ch for ch in str(segment_prompt_id or "") if ch.isalnum()).upper()
    return f"SPK-{compact[:8]}" if compact else ""


def _short_list(value: Any, limit: int) -> List[str]:
    return _listish(value)[: max(0, limit)]


def _ensure_reference_pack(
    reference_images: ProductReferenceImageSkill,
    anchor_client: FeishuBitableClient,
    product_id: str,
    market: str,
    sku_id: str,
    sku_label: str,
    anchor_fields: Dict[str, Any],
    task_fields: Dict[str, Any],
    dry_run: bool,
) -> Dict[str, Any]:
    active = reference_images.get_active_pack(product_id, market=market, sku_id=sku_id)
    active_pack = active.data.get("pack") if active.success else None
    if active_pack and active_pack.get("source") != "mixcut_anchor_pass_segment_frame":
        return _reference_pack_summary(active.data)
    if dry_run:
        return {"reference_image_pack_id": "", "reference_image_version": 0, "primary_preview_url": "", "reference_image_status": "缺失"}
    image_source, images = _reference_image_attachments(anchor_fields, task_fields)
    if active_pack and not images:
        return _reference_pack_summary(active.data)
    if not images:
        return {"reference_image_pack_id": "", "reference_image_version": 0, "primary_preview_url": "", "reference_image_status": "缺失", "error": "reference_images_missing_in_anchor_and_task"}
    source_images: List[Dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix=f"refpack_{product_id}_") as tmpdir:
        tmp_root = Path(tmpdir)
        for index, attachment in enumerate(images, start=1):
            try:
                content, file_name, content_type, _size = anchor_client.download_attachment_bytes(attachment)
            except Exception as exc:
                return {"reference_image_pack_id": "", "reference_image_version": 0, "primary_preview_url": "", "reference_image_status": "更新失败", "error": str(exc)}
            safe_name = _safe_file_name(file_name or f"reference_{index}.jpg")
            path = tmp_root / f"{index:03d}_{safe_name}"
            path.write_bytes(content)
            source_images.append(
                {
                    "path": str(path),
                    "image_role": "main" if index == 1 else "detail",
                    "source_file_token": _text(attachment.get("file_token") if isinstance(attachment, dict) else ""),
                    "source_url": _text(attachment),
                }
            )
        packed = reference_images.ensure_pack(
            product_id,
            market=market,
            sku_id=sku_id,
            sku_label=sku_label,
            source_images=source_images,
            source=image_source,
            anchor_snapshot={"商品主图数量": len(images), "图片来源": image_source, "AI生成锚点卡": _text(anchor_fields.get("AI生成锚点卡"))[:2000]},
        )
    if not packed.success:
        return {"reference_image_pack_id": "", "reference_image_version": 0, "primary_preview_url": "", "reference_image_status": "更新失败", "error": packed.to_dict()}
    return _reference_pack_summary(packed.data)


def _reference_image_attachments(anchor_fields: Dict[str, Any], task_fields: Dict[str, Any]) -> tuple[str, List[Dict[str, Any]]]:
    for source, fields in (("feishu_anchor_card", anchor_fields), ("feishu_product_task", task_fields)):
        for field_name in ("商品主图", "产品图片", "商品图片", "图片", "主图", "参考图"):
            images = _attachments(fields.get(field_name))
            if images:
                return source, images
    return "", []


def _reference_pack_summary(data: Dict[str, Any]) -> Dict[str, Any]:
    pack = data.get("pack") or {}
    images = data.get("images") or []
    preview = (images[0].get("preview_url") if images else "") or pack.get("primary_preview_url") or ""
    return {
        "reference_image_pack_id": pack.get("reference_image_pack_id") or "",
        "reference_image_version": int(pack.get("version") or 0),
        "primary_preview_url": preview,
        "reference_image_status": "可用" if pack.get("reference_image_pack_id") else "缺失",
    }


def _package_reference_ready(package: Dict[str, Any]) -> bool:
    return bool(
        _text(package.get("reference_image_pack_id"))
        and _text(package.get("reference_image_status")) == "可用"
        and _text(package.get("reference_image_preview_url"))
    )


def _safe_file_name(value: str) -> str:
    name = Path(str(value or "reference.jpg")).name
    return "".join(ch if ch.isalnum() or ch in {".", "-", "_"} else "_" for ch in name) or "reference.jpg"


def _priority(fields: Dict[str, Any]) -> str:
    raw = _text(fields.get("优先级"))
    return {"urgent": "紧急", "high": "高", "normal": "普通", "low": "暂缓", "紧急": "紧急", "高": "高", "普通": "普通", "低": "暂缓"}.get(raw, "普通")


def _note(fields: Dict[str, Any], index: int, total: int, voc_candidate: Any = None) -> str:
    chunks = [f"由商品内容任务表缺口自动生成 ({index}/{total})"]
    if isinstance(voc_candidate, dict):
        selling_point = _text(voc_candidate.get("product_selling_point"))
        visual_proof = _text(voc_candidate.get("visual_proof_zh")) or selling_point
        required_action = _text(voc_candidate.get("required_action_zh"))
        insight_id = _text(voc_candidate.get("insight_id"))
        requested = _int(voc_candidate.get("requested_hook_count"))
        if visual_proof:
            chunks.append(f"VOC证明点：{visual_proof}" + (f" x{requested}" if requested else ""))
        if required_action:
            chunks.append(f"VOC证明动作：{required_action}")
        if selling_point and selling_point != visual_proof:
            chunks.append(f"VOC来源卖点：{selling_point}")
        if insight_id:
            chunks.append(f"VOC洞察：{insight_id}")
    gap_text = _text(fields.get("素材缺口说明"))
    if gap_text:
        chunks.append(f"缺口说明：{gap_text}")
    material_tier = _text(fields.get("素材等级"))
    if material_tier:
        chunks.append(f"素材等级：{material_tier}")
    return "\n".join(chunks)


def _without_large_prompt(fields: Dict[str, Any]) -> Dict[str, Any]:
    cloned = dict(fields)
    if cloned.get("短视频片段提示词"):
        cloned["短视频片段提示词"] = str(cloned["短视频片段提示词"])[:160] + "..."
    return cloned


def _compact(fields: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in fields.items() if value not in (None, "", [], {})}


def _feishu_url(url: Any, text: str) -> Dict[str, str] | str:
    link = _text(url)
    return {"link": link, "text": text, "type": "url"} if link else ""


def _sku_id(task_fields: Dict[str, Any], anchor_fields: Dict[str, Any]) -> str:
    for name in ("SKU ID", "SKU", "skuID", "sku_id", "SKU编码", "颜色SKU"):
        value = _text(task_fields.get(name)) or _text(anchor_fields.get(name))
        if value:
            return value
    return "DEFAULT"


def _sku_label(task_fields: Dict[str, Any], anchor_fields: Dict[str, Any]) -> str:
    for name in ("SKU名称", "SKU标签", "颜色", "颜色名称", "款式"):
        value = _text(task_fields.get(name)) or _text(anchor_fields.get(name))
        if value:
            return value
    return ""


def _category_key(value: str, hint_text: str = "") -> str:
    key = CATEGORY_CN_TO_KEY.get(value, value or "generic_fashion")
    if key in {"generic_fashion", "general", "小饰品", ""}:
        hint = f"{value} {hint_text}".lower()
        if any(token in hint for token in ("สร้อยคอ", "项链", "吊坠项链", "颈链", "necklace", "necklaces", "pendant", "kalung")):
            return "necklaces"
        if any(token in hint for token in ("สร้อยข้อมือ", "กำไล", "手链", "手镯", "手串", "腕饰", "bracelet", "bangle")):
            return "bracelets"
    return CATEGORY_CN_TO_KEY.get(key, key or "generic_fashion")


def _jsonish(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    text = _text(value)
    if text.startswith("{"):
        try:
            parsed = json.loads(text)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _listish(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        result = []
        for item in value:
            if isinstance(item, str):
                result.extend(_split_text(item))
            elif isinstance(item, dict):
                result.append(_text(item))
        return [item for item in result if item]
    if isinstance(value, dict):
        return [_text(value)] if _text(value) else []
    return _split_text(str(value))


def _attachments(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict) and item.get("file_token")]
    if isinstance(value, dict) and value.get("file_token"):
        return [value]
    return []


def _split_text(value: str) -> List[str]:
    text = str(value or "").strip()
    if not text:
        return []
    for sep in ["\n", "；", ";", "、", ","]:
        if sep in text:
            return [item.strip("- ").strip() for item in text.split(sep) if item.strip("- ").strip()]
    return [text]


def _join(value: Any) -> str:
    return "\n".join(_listish(value))


def _int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    text = _text(value)
    if not text:
        return 0
    try:
        return int(float(text))
    except ValueError:
        return 0


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("text", "name", "value", "link", "url"):
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                return item.strip()
        return ""
    if isinstance(value, list):
        return ",".join(item for item in (_text(item) for item in value) if item).strip()
    return str(value).strip()


def _repair_rds_only_packages(args) -> Dict[str, Any]:
    """Repair RDS packages that exist but have no Feishu record."""
    try:
        ctx = build_context()
    except Exception as exc:
        return {"repaired": [], "skipped": [], "failed": [{"reason": "context_init_failed", "error": str(exc)}]}
    RDSRepositorySkill(ctx).init_db()
    factory = SegmentPromptFactorySkill(ctx)
    prompt_client = resolve_client(args.prompt_workbench_url)
    prompt_field_names = _field_names(prompt_client)

    with ctx.repo.connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM segment_prompt_packages WHERE product_id=%s "
            "AND package_status IN ('created','pending_feishu_sync','feishu_sync_failed') "
            "AND (feishu_record_id IS NULL OR feishu_record_id NOT LIKE 'rec%%') "
            "ORDER BY id", (args.product_id,),
        )
        orphan_packages = [dict(r) for r in cur.fetchall()]
    if args.dry_run:
        return {"dry_run": True, "orphan_count": len(orphan_packages), "packages": [
            {"segment_prompt_id": p["segment_prompt_id"], "package_status": p["package_status"], "segment_type": p.get("segment_type", "")}
            for p in orphan_packages
        ]}

    # scan feishu workbench for existing records
    feishu_by_prompt_id = {}
    try:
        wb_records = prompt_client.list_records(page_size=200)
        for rec in wb_records:
            fid = _text((rec.fields or {}).get("提示词包ID"))
            if fid:
                feishu_by_prompt_id[fid] = rec.record_id
    except Exception:
        pass

    repaired, skipped, failed = [], [], []
    for package in orphan_packages:
        prompt_id = package["segment_prompt_id"]
        prompt_json = json.loads(package.get("prompt_package_json") or "{}") if isinstance(package.get("prompt_package_json"), str) else (package.get("prompt_package_json") or {})
        if not isinstance(prompt_json, dict):
            prompt_json = {}
        pkg_slot_role = package.get("slot_role") or prompt_json.get("slot_role") or ""
        pkg_segment_type = package.get("segment_type") or prompt_json.get("segment_type") or ""

        # check if feishu already has this prompt_id
        if prompt_id in feishu_by_prompt_id:
            feishu_rid = feishu_by_prompt_id[prompt_id]
            ctx.repo.update("segment_prompt_packages", "segment_prompt_id", prompt_id, {
                "package_status": "created", "feishu_record_id": feishu_rid, "failure_reason": "",
            })
            repaired.append({"segment_prompt_id": prompt_id, "action": "feishu_found", "feishu_record_id": feishu_rid})
            continue

        # build feishu row from package data
        try:
            row_fields = {
                "商品ID": package.get("product_id") or "",
                "SKU ID": package.get("sku_id") or "DEFAULT",
                "提示词包ID": prompt_id,
                "片段角色": pkg_slot_role or "",
                "片段类型": pkg_segment_type or "",
                "生成档位": package.get("prompt_grade") or "A",
                "短视频片段提示词": _format_prompt_package(package),
            }
            prompt_field_names, feishu_record_id = _safe_batch_create_prompt(prompt_client, row_fields, prompt_field_names)
            ctx.repo.update("segment_prompt_packages", "segment_prompt_id", prompt_id, {
                "package_status": "created", "feishu_record_id": feishu_record_id, "failure_reason": "",
            })
            repaired.append({"segment_prompt_id": prompt_id, "action": "created", "feishu_record_id": feishu_record_id})
        except Exception as exc:
            ctx.repo.update("segment_prompt_packages", "segment_prompt_id", prompt_id, {
                "package_status": "feishu_sync_failed", "failure_reason": str(exc),
            })
            failed.append({"segment_prompt_id": prompt_id, "error": str(exc)})

    return {"repaired": repaired, "skipped": skipped, "failed": failed}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--product-task-url", default=PRODUCT_TASK_URL)
    parser.add_argument("--anchor-queue-url", default=ANCHOR_QUEUE_URL)
    parser.add_argument("--prompt-workbench-url", default=PROMPT_WORKBENCH_URL)
    parser.add_argument("--product-id", default="")
    parser.add_argument("--max-packages-per-product", type=int, default=6)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--refresh-existing-prompts", action="store_true")
    parser.add_argument("--use-voc-ads-package", action="store_true", help="use ready voc_ads_hook_package as hook prompt source")
    parser.add_argument("--submit-channel", choices=["jimeng", "imini"], default="jimeng")
    parser.add_argument("--repair-rds-only-packages", action="store_true", help="repair RDS packages missing Feishu records")
    args = parser.parse_args()
    if args.repair_rds_only_packages:
        result = _repair_rds_only_packages(args)
    else:
        result = sync_workbench(
            args.product_task_url,
            args.anchor_queue_url,
            args.prompt_workbench_url,
            dry_run=args.dry_run,
            product_id_filter=args.product_id,
            max_packages_per_product=args.max_packages_per_product,
            refresh_existing_prompts=args.refresh_existing_prompts,
            use_voc_ads_package=args.use_voc_ads_package,
            submit_channel=args.submit_channel,
        )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if result["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
