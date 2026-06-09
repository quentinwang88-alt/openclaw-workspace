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
        gap_count = _gap_count(fields)
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
        category = _category_key(_text(fields.get("类目")) or _text(anchor_fields.get("类目")))
        category_cn = CATEGORY_KEY_TO_CN.get(category, "通用服饰")
        brief = _anchor_brief(product_id, product_name, category, anchor_fields)
        brief["material_anchor_brief"]["sku_id"] = sku_id
        if not brief["material_anchor_brief"]["hard_anchors"]:
            skipped.append({"product_id": product_id, "reason": "anchor_without_hard_anchors"})
            continue

        slots = _gap_slots(_text(fields.get("素材缺口说明")), category, gap_count, max_packages_per_product)
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
                    refresh = _refresh_existing_prompt(factory, prompt_client, existing_prompt_records[existing_key], brief, slot, dry_run)
                    if refresh.get("failed"):
                        failed.append({"product_id": product_id, **refresh["failed"]})
                    else:
                        skipped.append({"product_id": product_id, "reason": "refreshed_existing_prompt", "key": "|".join(existing_key), **refresh})
                    continue
                skipped.append({"product_id": product_id, "reason": "already_exists", "key": "|".join(existing_key)})
                continue
            package_result = factory.build_package(brief, slot, persist=not dry_run)
            if not package_result.success:
                failed.append({"product_id": product_id, "reason": "prompt_build_failed", "error": package_result.to_dict()})
                continue
            package = package_result.data
            package["segment_script_id"] = _segment_script_id(package["segment_prompt_id"])
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
                "短视频片段提示词": _format_prompt_package(package),
                "备注": _note(fields, idx + 1, len(slots)),
            }
            existing_keys.add(dedupe_key)
            if dry_run:
                created.append({"product_id": product_id, "segment_prompt_id": package["segment_prompt_id"], "fields": _without_large_prompt(row_fields)})
            else:
                try:
                    prompt_field_names = _safe_batch_create_prompt(prompt_client, row_fields, prompt_field_names)
                    created.append({"product_id": product_id, "segment_prompt_id": package["segment_prompt_id"], "segment_type": row_fields["片段类型"], "grade": row_fields["生成档位"]})
                except Exception as exc:
                    existing_keys.discard(dedupe_key)
                    failed.append({"product_id": product_id, "segment_prompt_id": package["segment_prompt_id"], "reason": "feishu_create_failed", "error": str(exc)})

    return {"created": created, "skipped": skipped, "failed": failed}


def _safe_batch_create_prompt(client: FeishuBitableClient, row_fields: Dict[str, Any], field_names: set[str]) -> set[str]:
    try:
        client.batch_create_records([{"fields": _compact(_filter_fields(row_fields, field_names))}])
        return field_names
    except Exception as exc:
        if "FieldNameNotFound" not in str(exc):
            raise
        latest = _field_names(client)
        client.batch_create_records([{"fields": _compact(_filter_fields(row_fields, latest))}])
        return latest


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
        if product_id and segment_type and grade and status not in {"失败", "质检废弃"}:
            indexed[(product_id, sku_id, role, segment_type, grade, hook_intent)] = record
            if not role and not hook_intent:
                indexed[(product_id, sku_id, "", segment_type, grade, "")] = record
            if role in {"detail", "result"} and hook_intent:
                indexed[(product_id, sku_id, role, "", grade, hook_intent)] = record
    return indexed


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
        "短视频片段提示词": _format_prompt_package(package),
    }
    if not _package_reference_ready(package):
        update_fields["包状态"] = "参考图异常"
        update_fields["是否可提单"] = False
    if dry_run:
        return {"record_id": record.record_id, "segment_prompt_id": package["segment_prompt_id"], "action": "would_refresh"}
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
        if category == "earrings":
            plans[1] = ("product_still", "A", "hero", "product_clarity")
            plans[2] = ("flatlay", "A", "hero", "product_clarity")
        return plans
    if role == "result":
        if category == "earrings":
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
        if category == "earrings":
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
        if category == "earrings":
            return [
                ("flatlay", "C", "ending", "atmosphere"),
                ("product_still", "C", "ending", "atmosphere"),
            ]
        return [
            ("home_lifestyle", "C", "ending", "atmosphere"),
            ("seasonal_scene", "C", "ending", "atmosphere"),
        ]
    if category == "earrings":
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
    if category == "earrings":
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
    return {
        "material_anchor_brief": {
            "product_id": product_id,
            "display_family": CATEGORY_KEY_TO_CN.get(category, category),
            "product_subtype": _product_label(product_name, category),
            "category": category,
            "primary_visual_result": "；".join(core_points[:3]) or product_name,
            "must_show": core_points,
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


def _note(fields: Dict[str, Any], index: int, total: int) -> str:
    chunks = [f"由商品内容任务表缺口自动生成 ({index}/{total})"]
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


def _category_key(value: str) -> str:
    return CATEGORY_CN_TO_KEY.get(value, value or "generic_fashion")


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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--product-task-url", default=PRODUCT_TASK_URL)
    parser.add_argument("--anchor-queue-url", default=ANCHOR_QUEUE_URL)
    parser.add_argument("--prompt-workbench-url", default=PROMPT_WORKBENCH_URL)
    parser.add_argument("--product-id", default="")
    parser.add_argument("--max-packages-per-product", type=int, default=6)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--refresh-existing-prompts", action="store_true")
    args = parser.parse_args()
    result = sync_workbench(
        args.product_task_url,
        args.anchor_queue_url,
        args.prompt_workbench_url,
        dry_run=args.dry_run,
        product_id_filter=args.product_id,
        max_packages_per_product=args.max_packages_per_product,
        refresh_existing_prompts=args.refresh_existing_prompts,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if result["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
