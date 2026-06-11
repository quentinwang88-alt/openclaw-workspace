#!/usr/bin/env python3
"""Bridge auto_mixcut approved outputs into the auto-publisher queue."""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List

from app.db import AutoPublishDB, default_video_dir
from app.models import ScriptMetadata


AUTO_MIXCUT_ROOT = Path("/Users/likeu3/.openclaw/workspace/auto_mixcut")
if str(AUTO_MIXCUT_ROOT) not in sys.path:
    sys.path.insert(0, str(AUTO_MIXCUT_ROOT))


@dataclass(frozen=True)
class MixcutSyncItem:
    output_id: str
    canonical_script_key: str
    product_id: str
    store_id: str
    material_id: str
    local_file_path: str


def sync_mixcut_videos(
    *,
    auto_publish_db: AutoPublishDB,
    product_id: str = "",
    batch_id: str = "",
    output_id: str = "",
    video_dir: Path | None = None,
    limit: int | None = None,
    store_id_override: str = "",
    pull_output_qc: bool = True,
    dry_run: bool = False,
) -> Dict[str, Any]:
    from auto_mixcut.core.bootstrap import build_context
    _ensure_auto_mixcut_env_defaults()
    ctx = build_context()
    qc_sync = _pull_publishable_output_qc(ctx) if pull_output_qc and not dry_run else {"skipped": True}
    rows = _candidate_outputs(ctx, product_id=product_id, batch_id=batch_id, output_id=output_id, limit=limit)
    base_dir = Path(video_dir) if video_dir else default_video_dir()
    base_dir.mkdir(parents=True, exist_ok=True)

    synced: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    metadata_rows: List[ScriptMetadata] = []
    video_asset_rows: List[Dict[str, str]] = []
    product_task_store_cache: Dict[str, str] = {}
    product_ids_to_sync: set[str] = set()

    for output in rows:
        oid = str(output.get("output_id") or "").strip()
        product = dict(ctx.repo.get("products", "product_id", output.get("product_id")) or {})
        _ensure_product_store_id(ctx, output, product, store_id_override, product_task_store_cache, persist=not dry_run)
        validation = _validate_ids(output, product, store_id_override=store_id_override)
        if validation:
            skipped.append({"output_id": oid, "reason": validation})
            continue

        local_target = base_dir / "mixcut" / str(output["product_id"]) / f"{oid}.mp4"
        if not dry_run:
            copied = _copy_mixcut_output_file(ctx, output, local_target)
            if not copied["success"]:
                skipped.append({"output_id": oid, "reason": copied["reason"]})
                continue

        meta = build_mixcut_metadata(output, product, store_id_override=store_id_override)
        metadata_rows.append(meta)
        video_asset_rows.append(
            {
                "canonical_script_key": meta.canonical_script_key,
                "script_id": meta.script_id,
                "run_manager_record_id": oid,
                "video_source_type": "mixcut_output",
                "video_source_value": oid,
                "local_file_path": str(local_target),
                "download_status": "下载成功",
                "run_video_status": "混剪成片已通过",
                "publish_status": "待排期",
            }
        )
        synced.append(
            {
                "output_id": oid,
                "canonical_script_key": meta.canonical_script_key,
                "product_id": meta.product_id,
                "store_id": meta.store_id,
                "material_id": oid,
                "local_file_path": str(local_target),
            }
        )
        if meta.product_id:
            product_ids_to_sync.add(meta.product_id)

    task_sync: List[Dict[str, Any]] = []
    if not dry_run and metadata_rows:
        auto_publish_db.upsert_script_metadata(metadata_rows)
        for item in video_asset_rows:
            auto_publish_db.upsert_video_asset(**item)
        task_sync = [_sync_product_task_best_effort(ctx, pid) for pid in sorted(product_ids_to_sync)]

    return {
        "candidates": len(rows),
        "synced": synced,
        "skipped": skipped,
        "task_sync": task_sync,
        "qc_sync": qc_sync,
        "dry_run": dry_run,
    }


def sync_mixcut_publish_results(*, auto_publish_db: AutoPublishDB, product_id: str = "", dry_run: bool = False) -> Dict[str, Any]:
    from auto_mixcut.core.bootstrap import build_context

    _ensure_auto_mixcut_env_defaults()
    ctx = build_context()
    rows = _published_mixcut_assets(auto_publish_db, product_id=product_id)
    updated: List[Dict[str, str]] = []
    skipped: List[Dict[str, str]] = []
    product_ids_to_sync: set[str] = set()
    for row in rows:
        canonical = str(row.get("canonical_script_key") or "")
        output_id = canonical.replace("mixcut:", "", 1) if canonical.startswith("mixcut:") else str(row.get("script_id") or "")
        if not output_id:
            skipped.append({"canonical_script_key": canonical, "reason": "output_id_missing"})
            continue
        output = ctx.repo.get("outputs", "output_id", output_id)
        if not output:
            skipped.append({"output_id": output_id, "reason": "auto_mixcut_output_missing"})
            continue
        if dry_run:
            updated.append({"output_id": output_id, "published_at": str(row.get("published_at") or ""), "dry_run": "true"})
            continue
        ctx.repo.update(
            "outputs",
            "output_id",
            output_id,
            {
                "published_at": str(row.get("published_at") or ""),
                "publish_task_id": str(row.get("publish_task_id") or ""),
                "publish_result": str(row.get("publish_result") or ""),
            },
        )
        if output.get("product_id"):
            product_ids_to_sync.add(str(output.get("product_id")))
        updated.append({"output_id": output_id, "published_at": str(row.get("published_at") or "")})
    task_sync = [] if dry_run else [_sync_product_task_best_effort(ctx, pid) for pid in sorted(product_ids_to_sync)]
    return {"published_assets": len(rows), "updated": updated, "skipped": skipped, "task_sync": task_sync, "dry_run": dry_run}


def _ensure_auto_mixcut_env_defaults() -> None:
    if not os.environ.get("AUTO_MIXCUT_DB_PROVIDER") and os.environ.get("LIKEU_AI_DATABASE_URL"):
        os.environ["AUTO_MIXCUT_DB_PROVIDER"] = "mysql"
    if not os.environ.get("AUTO_MIXCUT_OSS_PROVIDER"):
        os.environ["AUTO_MIXCUT_OSS_PROVIDER"] = "local"


def _pull_publishable_output_qc(ctx: Any, limit: int = 500) -> Dict[str, Any]:
    """Lightweight Feishu -> RDS sync for publishable output QC flags.

    The full auto_mixcut `pull-output-qc` also reconciles usage and syncs product
    task tables, which is too heavy for the auto-publisher's two-hour loop.
    """
    try:
        from auto_mixcut.adapters.feishu import AutoMixcutFeishuClient
    except Exception as exc:
        return {"failed": 1, "error": str(exc)}

    try:
        client = AutoMixcutFeishuClient("成片质检表")
        records = client.list_records(limit=limit)
    except Exception as exc:
        return {"failed": 1, "error": str(exc)}

    checked = 0
    updated = 0
    skipped = 0
    for record in records:
        fields = record.fields or {}
        output_id = _text(fields.get("输出ID") or fields.get("成片ID") or fields.get("output_id"))
        if not output_id:
            skipped += 1
            continue
        if not _is_publishable_output_qc(fields):
            checked += 1
            continue
        output = ctx.repo.get("outputs", "output_id", output_id)
        if not output:
            skipped += 1
            continue
        checked += 1
        if str(output.get("human_quality_status") or "").strip() == "passed":
            continue
        result = ctx.repo.update("outputs", "output_id", output_id, {"human_quality_status": "passed"})
        if getattr(result, "success", False):
            updated += 1
        else:
            skipped += 1
    return {"records_checked": checked, "outputs_marked_passed": updated, "skipped": skipped}


def _is_publishable_output_qc(fields: Dict[str, Any]) -> bool:
    human_status = _text(fields.get("人工质检状态"))
    publishable = fields.get("是否可发布")
    if isinstance(publishable, bool):
        return publishable
    text = _text(publishable).lower()
    if text in {"true", "1", "yes", "y", "是", "可发布", "通过", "checked"}:
        return True
    return human_status in {"可发布", "通过", "发布"}


def build_mixcut_metadata(output: Dict[str, Any], product: Dict[str, Any], store_id_override: str = "") -> ScriptMetadata:
    output_id = str(output.get("output_id") or "").strip()
    product_id = str(output.get("product_id") or product.get("product_id") or "").strip()
    template_id = str(output.get("template_id") or "default").strip() or "default"
    title_pack = build_simple_mixcut_title(output, product, product_id=product_id)
    return ScriptMetadata(
        canonical_script_key=f"mixcut:{output_id}",
        script_id=output_id,
        source_record_id=output_id,
        script_slot="mixcut",
        task_no=str(output.get("batch_id") or ""),
        store_id=str(store_id_override or product.get("shop_id") or "").strip(),
        product_id=product_id,
        parent_slot="mixcut",
        direction_label="混剪视频",
        variant_strength=str(output.get("template_id") or ""),
        target_country=str(product.get("market") or ""),
        product_type=str(product.get("category") or ""),
        content_family_key=f"mixcut:{product_id}:{template_id}",
        script_text=f"混剪成片 material_id={output_id} template_id={template_id}",
        short_video_title=title_pack["title"],
        title_source=title_pack["source"],
        script_source="混剪视频",
        publish_purpose="混剪视频",
        cart_enabled="是",
        content_branch="商品展示型",
    )


def build_simple_mixcut_title(output: Dict[str, Any], product: Dict[str, Any], product_id: str = "") -> Dict[str, str]:
    manual_title = _first_text(output, product, ["short_video_title", "短视频标题", "manual_title", "人工标题", "标题"])
    if manual_title:
        return {"title": _clip_title(_clean_title(manual_title)), "source": "mixcut_manual"}

    product_name = _clean_product_title(_first_text(product, output, ["product_name", "商品名称", "产品名称", "title", "商品标题"]))
    anchor = _clean_anchor_phrase(_anchor_phrase(output, product))
    title = _join_title(product_name, anchor)
    if title:
        return {"title": _clip_title(title), "source": "mixcut_simple_product_anchor" if anchor else "mixcut_simple_product_title"}
    if product_id:
        return {"title": f"Product {product_id}", "source": "mixcut_product_id_fallback"}
    return {"title": "", "source": "mixcut_title_empty"}


def _candidate_outputs(ctx: Any, *, product_id: str, batch_id: str, output_id: str, limit: int | None) -> List[Dict[str, Any]]:
    clauses = [
        "render_status='rendered'",
        "human_quality_status='passed'",
        "published_at IS NULL",
    ]
    params: List[Any] = []
    if product_id:
        clauses.append("product_id=?")
        params.append(product_id)
    if batch_id:
        clauses.append("batch_id=?")
        params.append(batch_id)
    if output_id:
        clauses.append("output_id=?")
        params.append(output_id)
    sql = " AND ".join(clauses) + " ORDER BY id ASC"
    if limit:
        sql += f" LIMIT {int(limit)}"
    return ctx.repo.list_where("outputs", sql, tuple(params))


def _validate_ids(output: Dict[str, Any], product: Dict[str, Any], store_id_override: str = "") -> str:
    if not str(output.get("output_id") or "").strip():
        return "material_id_missing"
    if not str(output.get("product_id") or "").strip():
        return "product_id_missing"
    if str(product.get("product_id") or output.get("product_id") or "").strip() != str(output.get("product_id") or "").strip():
        return "product_id_mismatch"
    if not str(store_id_override or product.get("shop_id") or "").strip():
        return "store_id_missing"
    if not str(output.get("feishu_record_id") or "").strip() and not str(output.get("output_oss_object_id") or "").strip():
        return "output_file_source_missing"
    return ""


def _ensure_product_store_id(
    ctx: Any,
    output: Dict[str, Any],
    product: Dict[str, Any],
    store_id_override: str,
    product_task_store_cache: Dict[str, str],
    *,
    persist: bool = True,
) -> str:
    if str(store_id_override or "").strip():
        return str(store_id_override or "").strip()
    if str(product.get("shop_id") or "").strip():
        return str(product.get("shop_id") or "").strip()

    product_id = _text(output.get("product_id") or product.get("product_id"))
    if not product_id:
        return ""
    if product_id not in product_task_store_cache:
        product_task_store_cache[product_id] = _lookup_store_id_from_product_task(product_id)
    store_id = product_task_store_cache.get(product_id, "")
    if not store_id:
        return ""

    product["shop_id"] = store_id
    if product_id and not _text(product.get("product_id")):
        product["product_id"] = product_id
    if persist:
        try:
            ctx.repo.update("products", "product_id", product_id, {"shop_id": store_id})
        except Exception:
            pass
    return store_id


def _lookup_store_id_from_product_task(product_id: str) -> str:
    try:
        from auto_mixcut.adapters.feishu import AutoMixcutFeishuClient

        client = AutoMixcutFeishuClient("商品内容任务表")
        for record in client.list_records(limit=500):
            fields = record.fields or {}
            if _text(fields.get("商品ID")) == product_id:
                return _text(fields.get("店铺"))
    except Exception:
        return ""
    return ""


def _sync_product_task_best_effort(ctx: Any, product_id: str) -> Dict[str, Any]:
    try:
        from auto_mixcut.skills.feishu_review_skill import sync_product_task_best_effort

        return sync_product_task_best_effort(ctx, product_id)
    except Exception as exc:
        return {"product_id": str(product_id or ""), "status": "failed", "error": str(exc)}


def _first_text(primary: Dict[str, Any], secondary: Dict[str, Any], keys: Iterable[str]) -> str:
    for source in (primary, secondary):
        for key in keys:
            text = _text(source.get(key))
            if text:
                return text
    return ""


def _clean_title(value: str) -> str:
    text = _text(value)
    text = re.sub(r"^(?:短视频标题|视频标题|标题|title|caption)\s*[:：-]\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text)
    return text.strip(" ，,。.;；|")


def _clean_product_title(value: str) -> str:
    text = _clean_title(value)
    text = re.sub(r"[\[\]【】()（）{}]", " ", text)
    text = re.sub(r"\b(?:new|hot|sale|free shipping|cod)\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"(?:พร้อมส่ง|ส่งไว|ของแท้|ลดราคา)", " ", text)
    text = re.sub(r"\s+", " ", text).strip(" ，,。.;；|")
    if len(text) > 28:
        parts = re.split(r"[,，/|｜\-–—]", text)
        text = next((part.strip() for part in parts if 3 <= len(part.strip()) <= 28), text[:28].strip())
    return text


def _anchor_phrase(output: Dict[str, Any], product: Dict[str, Any]) -> str:
    direct = _first_text(
        product,
        output,
        [
            "核心视觉点",
            "core_visual_points",
            "core_visual_point",
            "不可错识别点",
            "material_anchor_brief",
            "anchor_brief",
            "核心锚点",
        ],
    )
    if direct:
        return direct
    for key in ("anchor_json", "product_anchor_json", "AI生成锚点卡", "ai_anchor_card_json"):
        parsed = _parse_jsonish(product.get(key) or output.get(key))
        text = _anchor_from_mapping(parsed)
        if text:
            return text
    return ""


def _parse_jsonish(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    text = _text(value)
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _anchor_from_mapping(value: Any) -> str:
    if isinstance(value, list):
        parts = [_anchor_from_mapping(item) for item in value]
        return " ".join(part for part in parts if part).strip()
    if not isinstance(value, dict):
        return _text(value)
    preferred_keys = [
        "core_visual_points",
        "core_visual_point",
        "核心视觉点",
        "不可错识别点",
        "must_not_mismatch",
        "material_anchor_brief",
        "anchor_brief",
        "颜色",
        "材质",
        "版型",
        "品类",
    ]
    parts = [_text(value.get(key)) for key in preferred_keys]
    return " ".join(part for part in parts if part).strip()


def _clean_anchor_phrase(value: str) -> str:
    text = _clean_title(value)
    text = re.sub(r"[{}\"']", " ", text)
    text = re.sub(r"(?:商品主体|核心视觉点|不可错识别点|禁用错配项|产品锚点|商品锚点)\s*[:：]?", " ", text)
    text = re.sub(r"\s+", " ", text).strip(" ，,。.;；|")
    if len(text) > 12:
        parts = re.split(r"[,，;；/|｜、\s]+", text)
        selected = [part for part in parts if 1 < len(part) <= 8][:2]
        text = " ".join(selected) if selected else text[:12]
    return text


def _join_title(product_name: str, anchor: str) -> str:
    product_name = _clean_product_title(product_name)
    anchor = _clean_anchor_phrase(anchor)
    if product_name and anchor and anchor not in product_name:
        return f"{product_name}，{anchor}"
    return product_name or anchor


def _clip_title(title: str) -> str:
    return _clean_title(title)[:40].strip(" ，,。.;；|")


def _copy_mixcut_output_file(ctx: Any, output: Dict[str, Any], local_target: Path) -> Dict[str, Any]:
    local_target.parent.mkdir(parents=True, exist_ok=True)
    if local_target.exists() and local_target.stat().st_size > 0:
        return {"success": True, "source": "existing"}
    feishu_result = _copy_from_feishu_output_attachment(output, local_target)
    if feishu_result["success"]:
        return feishu_result
    oss_result = _copy_from_oss_output(ctx, output, local_target)
    if oss_result["success"]:
        return oss_result
    return {"success": False, "reason": f"feishu:{feishu_result['reason']}; oss:{oss_result['reason']}"}


def _copy_from_feishu_output_attachment(output: Dict[str, Any], local_target: Path) -> Dict[str, Any]:
    record_id = str(output.get("feishu_record_id") or "").strip()
    if not record_id:
        return {"success": False, "reason": "feishu_record_id_missing"}
    try:
        from auto_mixcut.adapters.feishu import AutoMixcutFeishuClient

        client = AutoMixcutFeishuClient("成片质检表")
        fields = client.get_record(record_id)
        attachment = _first_attachment(fields.get("成片文件"))
        if not attachment:
            return {"success": False, "reason": "feishu_output_attachment_missing"}
        content, _file_name, _content_type, _size = client.download_attachment_bytes(attachment)
        local_target.write_bytes(content)
        return {"success": True, "source": "feishu_attachment"}
    except Exception as exc:
        return {"success": False, "reason": str(exc)}


def _copy_from_oss_output(ctx: Any, output: Dict[str, Any], local_target: Path) -> Dict[str, Any]:
    try:
        from auto_mixcut.core.storage_paths import require_oss_object_path

        source = require_oss_object_path(ctx, output.get("output_oss_object_id"), "publish_mixcut_outputs")
        if not source or not source.exists():
            return {"success": False, "reason": "oss_output_unavailable"}
        shutil.copy2(source, local_target)
        return {"success": True, "source": "oss"}
    except Exception as exc:
        return {"success": False, "reason": str(exc)}


def _first_attachment(value: Any) -> Dict[str, Any] | None:
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict) and item.get("file_token"):
                return item
    if isinstance(value, dict) and value.get("file_token"):
        return value
    return None


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value).strip()
    if isinstance(value, dict):
        for key in ("text", "name", "value", "link"):
            text = _text(value.get(key))
            if text:
                return text
        return ""
    if isinstance(value, list):
        return " ".join(_text(item) for item in value if _text(item)).strip()
    return str(value).strip()


def _published_mixcut_assets(db: AutoPublishDB, product_id: str = "") -> List[Dict[str, Any]]:
    sql = """
        SELECT va.canonical_script_key, va.script_id, va.published_at, va.publish_task_id, va.publish_result, sm.product_id
        FROM video_assets va
        INNER JOIN script_metadata sm ON sm.canonical_script_key = va.canonical_script_key
        WHERE va.canonical_script_key LIKE 'mixcut:%'
          AND va.publish_status = '已发布'
          AND COALESCE(va.published_at, '') <> ''
    """
    params: List[Any] = []
    if product_id:
        sql += " AND sm.product_id = ?"
        params.append(product_id)
    with db._connect() as conn:  # noqa: SLF001 - local bridge needs direct lightweight query
        rows = conn.execute(sql, tuple(params)).fetchall()
    return [dict(row) for row in rows]
