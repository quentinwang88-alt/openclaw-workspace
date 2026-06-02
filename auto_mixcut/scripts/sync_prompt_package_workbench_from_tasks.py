#!/usr/bin/env python3
"""Generate Prompt Package workbench rows from product task gaps."""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List


WORKSPACE = Path("/Users/likeu3/.openclaw/workspace")
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(WORKSPACE / "skills" / "script-run-manager-sync"))

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # type: ignore  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # type: ignore  # noqa: E402

from auto_mixcut.core.bootstrap import build_context  # noqa: E402
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
    _prefer_local_oss_when_unconfigured()
    ctx = build_context()
    factory = SegmentPromptFactorySkill(ctx)
    task_client = resolve_client(product_task_url)
    anchor_client = resolve_client(anchor_queue_url)
    prompt_client = resolve_client(prompt_workbench_url)

    task_records = task_client.list_records(page_size=100)
    anchor_by_product = _index_latest_anchor(anchor_client.list_records(page_size=100))
    existing_prompt_records = _existing_prompt_records(prompt_client.list_records(page_size=100))
    existing_keys = set(existing_prompt_records)
    image_cache: Dict[str, List[Dict[str, Any]]] = {}

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
        category = _category_key(_text(fields.get("类目")) or _text(anchor_fields.get("类目")))
        category_cn = CATEGORY_KEY_TO_CN.get(category, "通用服饰")
        brief = _anchor_brief(product_id, product_name, category, anchor_fields)
        if not brief["material_anchor_brief"]["hard_anchors"]:
            skipped.append({"product_id": product_id, "reason": "anchor_without_hard_anchors"})
            continue

        slots = _gap_slots(_text(fields.get("素材缺口说明")), category, gap_count, max_packages_per_product)
        copied_image = image_cache.get(product_id)
        if copied_image is None:
            copied_image = [] if dry_run else _copy_product_image(anchor_client, prompt_client, anchor_fields)
            image_cache[product_id] = copied_image

        for idx, slot in enumerate(slots):
            dedupe_key = (product_id, SEGMENT_CN.get(slot["segment_type"], slot["segment_type"]), GRADE_CN.get(slot["ai_gen_grade"], slot["ai_gen_grade"]))
            if dedupe_key in existing_keys:
                if refresh_existing_prompts:
                    refresh = _refresh_existing_prompt(factory, prompt_client, existing_prompt_records[dedupe_key], brief, slot, dry_run)
                    if refresh.get("failed"):
                        failed.append({"product_id": product_id, **refresh["failed"]})
                    else:
                        skipped.append({"product_id": product_id, "reason": "refreshed_existing_prompt", "key": "|".join(dedupe_key), **refresh})
                    continue
                skipped.append({"product_id": product_id, "reason": "already_exists", "key": "|".join(dedupe_key)})
                continue
            package_result = factory.build_package(brief, slot, persist=not dry_run)
            if not package_result.success:
                failed.append({"product_id": product_id, "reason": "prompt_build_failed", "error": package_result.to_dict()})
                continue
            package = package_result.data
            row_fields = {
                "提示词包ID": package["segment_prompt_id"],
                "商品ID": product_id,
                "商品名称": product_name,
                "商品图片": copied_image,
                "市场": market,
                "归一类目": category_cn,
                "片段类型": SEGMENT_CN.get(package["segment_type"], package["segment_type"]),
                "生成档位": GRADE_CN.get(package["ai_gen_grade"], package["ai_gen_grade"]),
                "包状态": "待提单",
                "人工审核结论": "待审核",
                "是否可提单": True,
                "提单优先级": _priority(fields),
                "短视频片段提示词": _format_prompt_package(package),
                "备注": _note(fields, idx + 1, len(slots)),
            }
            existing_keys.add(dedupe_key)
            if dry_run:
                created.append({"product_id": product_id, "segment_prompt_id": package["segment_prompt_id"], "fields": _without_large_prompt(row_fields)})
            else:
                try:
                    prompt_client.batch_create_records([{"fields": _compact(row_fields)}])
                    created.append({"product_id": product_id, "segment_prompt_id": package["segment_prompt_id"], "segment_type": row_fields["片段类型"], "grade": row_fields["生成档位"]})
                except Exception as exc:
                    existing_keys.discard(dedupe_key)
                    failed.append({"product_id": product_id, "segment_prompt_id": package["segment_prompt_id"], "reason": "feishu_create_failed", "error": str(exc)})

    return {"created": created, "skipped": skipped, "failed": failed}


def _prefer_local_oss_when_unconfigured() -> None:
    provider = os.environ.get("AUTO_MIXCUT_OSS_PROVIDER", "").lower()
    required = ["ALIYUN_OSS_BUCKET", "ALIYUN_OSS_ENDPOINT", "ALIYUN_OSS_ACCESS_KEY_ID", "ALIYUN_OSS_ACCESS_KEY_SECRET"]
    if provider in {"", "aliyun"} and any(not os.environ.get(key) for key in required):
        os.environ["AUTO_MIXCUT_OSS_PROVIDER"] = "local"


def _index_latest_anchor(records: Iterable[Any]) -> Dict[str, Any]:
    indexed: Dict[str, Any] = {}
    for record in records:
        product_id = _text((record.fields or {}).get("商品ID"))
        if product_id:
            indexed[product_id] = record
    return indexed


def _existing_prompt_records(records: Iterable[Any]) -> Dict[tuple[str, str, str], Any]:
    indexed: Dict[tuple[str, str, str], Any] = {}
    for record in records:
        fields = record.fields or {}
        product_id = _text(fields.get("商品ID"))
        segment_type = _text(fields.get("片段类型"))
        grade = _text(fields.get("生成档位"))
        status = _text(fields.get("包状态"))
        if product_id and segment_type and grade and status not in {"失败", "质检废弃"}:
            indexed[(product_id, segment_type, grade)] = record
    return indexed


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
    update_fields = {
        "提示词包ID": package["segment_prompt_id"],
        "短视频片段提示词": _format_prompt_package(package),
    }
    if dry_run:
        return {"record_id": record.record_id, "segment_prompt_id": package["segment_prompt_id"], "action": "would_refresh"}
    saved = factory.save_package(package)
    if not saved.success:
        return {"failed": {"reason": "prompt_refresh_rds_failed", "error": saved.to_dict()}}
    prompt_client.update_record_fields(record.record_id, update_fields)
    return {"record_id": record.record_id, "segment_prompt_id": package["segment_prompt_id"], "action": "refreshed"}


def _gap_count(fields: Dict[str, Any]) -> int:
    allowed = _int(fields.get("系统允许生成数量"))
    target = _int(fields.get("目标生成数量"))
    actual = _int(fields.get("实际生成数量"))
    if allowed > 0:
        return allowed
    if target > actual:
        return target - actual
    gap_text = _text(fields.get("素材缺口说明"))
    material_status = _text(fields.get("素材状态"))
    if gap_text or material_status in {"not_ready", "blocked", "review_required"}:
        return 1
    return 0


def _gap_slots(gap_text: str, category: str, count: int, max_packages_per_product: int) -> List[Dict[str, Any]]:
    lower = gap_text.lower()
    planned: List[tuple[str, str, str, str]] = []
    if "hero" in lower or "首镜" in gap_text:
        planned.append(("product_display", "A", "hero", "product_clarity"))
    if "result" in lower or "效果" in gap_text or "佩戴" in gap_text or "上身" in gap_text:
        planned.append(("tryon_result", "A", "result", "tryon_result"))
    if "detail" in lower or "细节" in gap_text:
        planned.append(("detail_atmosphere", "B", "detail", "material_closeup"))
    if "usable" in lower or "可用" in gap_text or not planned:
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


def _default_slot_plan(category: str) -> List[tuple[str, str, str, str]]:
    if category == "earrings":
        return [
            ("detail_atmosphere", "A", "detail", "material_closeup"),
            ("product_display", "A", "hero", "product_clarity"),
            ("home_lifestyle", "C", "scene", "atmosphere"),
        ]
    if category == "scarves_hats":
        return [
            ("product_display", "A", "hero", "product_clarity"),
            ("tryon_result", "B", "result", "tryon_result"),
            ("seasonal_scene", "C", "scene", "atmosphere"),
        ]
    if category == "womens_outerwear":
        return [
            ("product_display", "A", "hero", "product_clarity"),
            ("detail_atmosphere", "B", "detail", "material_closeup"),
            ("tryon_result", "B", "result", "tryon_result"),
            ("mirror_routine", "C", "scene", "atmosphere"),
            ("home_lifestyle", "C", "ending", "atmosphere"),
            ("seasonal_scene", "C", "scene", "atmosphere"),
        ]
    return [
        ("product_display", "A", "hero", "product_clarity"),
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
        "person_framing": "ai_local" if grade in {"A", "B"} else "real_preferred",
        "duration_sec": 4,
    }


def _anchor_brief(product_id: str, product_name: str, category: str, anchor_fields: Dict[str, Any]) -> Dict[str, Any]:
    anchor_json = _jsonish(anchor_fields.get("AI生成锚点卡"))
    core_points = _listish(anchor_fields.get("核心视觉点")) or _listish(anchor_json.get("core_visual_points"))
    must_not_change = _listish(anchor_fields.get("不可错识别点")) or _listish(anchor_json.get("must_not_change_points"))
    forbidden = _listish(anchor_fields.get("禁用错配项")) or _listish(anchor_json.get("forbidden_mismatch"))
    hard = core_points or must_not_change
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
    parts = [
        f"正向提示词：\n{prompt.get('positive') or ''}",
        f"负向提示词：\n{prompt.get('negative') or ''}",
        f"运镜/动作弧线：\n{prompt.get('motion_arc') or ''}",
        f"参考图锚点提示：\n{_join(_short_list(anchor.get('hard_anchors'), 2))}",
    ]
    return "\n\n".join(part for part in parts if not part.endswith("\n"))


def _short_list(value: Any, limit: int) -> List[str]:
    return _listish(value)[: max(0, limit)]


def _copy_product_image(anchor_client: FeishuBitableClient, prompt_client: FeishuBitableClient, anchor_fields: Dict[str, Any]) -> List[Dict[str, Any]]:
    images = _attachments(anchor_fields.get("商品主图"))
    if not images:
        return []
    attachment = images[0]
    try:
        content, file_name, content_type, size = anchor_client.download_attachment_bytes(attachment)
        return [prompt_client.upload_attachment(content, file_name, content_type, size=size, parent_type="bitable_file")]
    except Exception:
        return []


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
    if cloned.get("商品图片"):
        cloned["商品图片"] = f"{len(cloned['商品图片'])} attachment(s)"
    return cloned


def _compact(fields: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in fields.items() if value not in (None, "", [], {})}


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
