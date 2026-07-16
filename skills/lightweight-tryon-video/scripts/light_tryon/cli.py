from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from .database import LightTryonDB, TEMPLATE_TABLES
from .feishu_client import load_feishu_config, make_client, resolve_endpoints, resolve_run_manager_endpoint, resolve_source_endpoint
from .feishu_sync import (
    build_clients,
    cleanup_review_duplicates,
    ensure_schema,
    initialize_template_records,
    inspect_tables,
    pull_manual_reviews,
    pull_visual_plan_reviews,
    pull_templates,
    push_reviews,
    push_visual_plans,
)
from .review_video_processing import process_review_videos
from .run_manager_sync import ensure_run_manager_schema, pull_generation_preferences, pull_run_manager_results, sync_jobs_to_run_manager
from .models import ProductInput
from .planner import plan_product
from .prompting import PROMPT_BUILDER_VERSION, build_prompt
from .review import export_review_html, set_manual_review
from .utils import read_json
from .workers import (
    export_job_records,
    qc_job,
    qc_pending_jobs,
    render_subtitles,
    render_brand_overlay,
    render_postprocessed_video,
    run_generation_worker,
)
from .source_script_sync import ensure_source_schema, find_source_records, process_source_requests, set_source_request
from .visual_plans import confirm_outfit_image, create_confirmed_video_jobs, run_outfit_generation_worker


SKILL_DIR = Path(__file__).resolve().parents[2]


def _default_media_bin(name: str) -> str:
    env_value = str(os.environ.get(f"{name.upper()}_BIN") or "").strip()
    if env_value:
        return env_value
    discovered = shutil.which(name)
    if discovered:
        return discovered
    local_bin = Path.home() / ".local" / "bin" / name
    return str(local_bin) if local_bin.is_file() else name


DEFAULT_FFMPEG = _default_media_bin("ffmpeg")
DEFAULT_FFPROBE = _default_media_bin("ffprobe")
DEFAULT_DB = Path(os.environ.get("LIGHT_TRYON_DB", SKILL_DIR / "var" / "light_tryon.sqlite3"))
DEFAULT_TEMPLATES = SKILL_DIR / "assets" / "default_templates.json"


def emit(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, default=str))


def _db(args: argparse.Namespace) -> LightTryonDB:
    db = LightTryonDB(args.db)
    db.init_schema()
    return db


def cmd_init(args: argparse.Namespace) -> dict[str, Any]:
    db = _db(args)
    counts = db.seed_templates(args.templates) if args.seed else {}
    return {"database": str(db.path), "schema_version": "2.2.0", "seeded": counts}


def cmd_template_list(args: argparse.Namespace) -> dict[str, Any]:
    rows = _db(args).list_templates(args.kind, args.status)
    return {"kind": args.kind, "count": len(rows), "items": rows}


def cmd_template_upsert(args: argparse.Namespace) -> dict[str, Any]:
    db = _db(args)
    payload = read_json(args.json_file)
    if not isinstance(payload, dict):
        raise ValueError("模板 JSON 顶层必须是对象")
    db.upsert_template(args.kind, payload)
    id_key = f"{args.kind}_id" if args.kind != "duration" else "duration_id"
    return {"kind": args.kind, "template_id": payload.get(id_key), "status": "upserted"}


def cmd_template_status(args: argparse.Namespace) -> dict[str, Any]:
    db = _db(args)
    db.set_template_status(args.kind, args.template_id, args.status)
    return {"kind": args.kind, "template_id": args.template_id, "status": args.status}


def cmd_template_delete(args: argparse.Namespace) -> dict[str, Any]:
    db = _db(args)
    db.delete_template(args.kind, args.template_id)
    return {"kind": args.kind, "template_id": args.template_id, "deleted": True}


def _products_from_file(path: str | Path) -> list[dict[str, Any]]:
    source = Path(path).expanduser().resolve()
    if source.suffix.lower() == ".jsonl":
        return [json.loads(line) for line in source.read_text(encoding="utf-8").splitlines() if line.strip()]
    payload = read_json(source)
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("products"), list):
        return payload["products"]
    if isinstance(payload, dict):
        return [payload]
    raise ValueError("商品文件必须是 JSON 对象、JSON 数组或 JSONL")


def cmd_product_add(args: argparse.Namespace) -> dict[str, Any]:
    db = _db(args)
    if args.file:
        payloads = _products_from_file(args.file)
    else:
        payloads = [
            {
                "product_id": args.product_id,
                "product_name": args.product_name,
                "market": args.market,
                "language": args.language,
                "category": args.category,
                "sub_category": args.sub_category,
                "product_title": args.product_title or args.product_name,
                "product_images": args.image or [],
                "core_selling_points": args.selling_point or [],
                "target_publish_count": args.count,
                "notes": args.notes,
            }
        ]
    items = [db.upsert_product(ProductInput.from_dict(payload)) for payload in payloads]
    return {"upserted": len(items), "product_ids": [item["product_id"] for item in items]}


def cmd_product_list(args: argparse.Namespace) -> dict[str, Any]:
    items = _db(args).list_products(args.status)
    return {"count": len(items), "items": items}


def _build_job_prompts(db: LightTryonDB, jobs: list[dict[str, Any]], *, force: bool = False) -> dict[str, Any]:
    built = 0
    skipped = 0
    fingerprints: dict[str, str] = {}
    for job in jobs:
        if not force and job.get("prompt_version") != "unbuilt" and job.get("prompt_payload"):
            skipped += 1
            continue
        context = db.get_job_context(job["job_id"])
        payload = build_prompt(context)
        db.update_prompt(job["job_id"], payload, PROMPT_BUILDER_VERSION)
        built += 1
        fingerprints[job["job_id"]] = payload["prompt_fingerprint"]
    return {"built": built, "skipped": skipped, "fingerprints": fingerprints}


def cmd_plan(args: argparse.Namespace) -> dict[str, Any]:
    db = _db(args)
    jobs = plan_product(db, args.product_id, count=args.count, plan_version=args.plan_version)
    result = db.create_jobs(jobs)
    prompt_result: dict[str, Any] = {}
    if not args.no_build_prompts:
        stored = db.list_jobs(product_id=args.product_id)
        stored = [job for job in stored if job.get("plan_version") == args.plan_version]
        prompt_result = _build_job_prompts(db, stored, force=args.force_prompts)
    return {
        "product_id": args.product_id,
        "plan_version": args.plan_version,
        "requested": len(jobs),
        **result,
        "jobs": [job.to_dict() for job in jobs],
        "prompts": prompt_result,
    }


def cmd_prompt_build(args: argparse.Namespace) -> dict[str, Any]:
    db = _db(args)
    if args.job_id:
        job = db.get_job(args.job_id)
        if not job:
            raise KeyError(f"找不到任务: {args.job_id}")
        jobs = [job]
    else:
        jobs = db.list_jobs(product_id=args.product_id)
    return _build_job_prompts(db, jobs, force=args.force)


def cmd_job_list(args: argparse.Namespace) -> dict[str, Any]:
    items = _db(args).list_jobs(
        product_id=args.product_id,
        generation_status=args.generation_status,
        qc_status=args.qc_status,
        limit=args.limit,
    )
    if not args.full:
        keep = {
            "job_id", "product_id", "scene_id", "action_id", "styling_id", "subtitle_id", "persona_id",
            "duration_seconds", "variant_no", "generation_status", "qc_status", "output_video_path",
            "prompt_version", "retry_count", "last_error",
        }
        items = [{key: value for key, value in item.items() if key in keep} for item in items]
    return {"count": len(items), "items": items}


def cmd_job_reset(args: argparse.Namespace) -> dict[str, Any]:
    db = _db(args)
    db.reset_job(args.job_id, clear_output=args.clear_output)
    return {"job_id": args.job_id, "generation_status": "pending", "clear_output": args.clear_output}


def cmd_export_jimeng(args: argparse.Namespace) -> dict[str, Any]:
    return export_job_records(
        _db(args),
        args.output,
        product_id=args.product_id,
        status=args.generation_status,
    )


def cmd_generate(args: argparse.Namespace) -> dict[str, Any]:
    return run_generation_worker(
        _db(args),
        args.command,
        limit=args.limit,
        timeout=args.timeout,
        provider=args.provider,
        max_attempts=args.max_attempts,
    )


def cmd_qc(args: argparse.Namespace) -> dict[str, Any]:
    db = _db(args)
    vision_result = read_json(args.vision_result) if args.vision_result else None
    if vision_result is not None and not args.job_id:
        raise ValueError("--vision-result 只能与 --job-id 一起使用")
    if args.job_id:
        return qc_job(
            db,
            args.job_id,
            vision_result=vision_result,
            vision_command=args.vision_command,
            ffprobe_bin=args.ffprobe,
            timeout=args.timeout,
        )
    return qc_pending_jobs(
        db,
        limit=args.limit,
        vision_command=args.vision_command,
        ffprobe_bin=args.ffprobe,
        timeout=args.timeout,
    )


def cmd_review_export(args: argparse.Namespace) -> dict[str, Any]:
    return export_review_html(_db(args), args.output, product_id=args.product_id)


def cmd_review_set(args: argparse.Namespace) -> dict[str, Any]:
    return set_manual_review(_db(args), args.job_id, args.decision, args.note)


def cmd_subtitle_render(args: argparse.Namespace) -> dict[str, Any]:
    db = _db(args)
    job = db.get_job(args.job_id)
    if not job:
        raise KeyError(f"找不到任务: {args.job_id}")
    payload = job.get("prompt_payload") or {}
    subtitle_plan = payload.get("subtitle_plan") or {}
    source = job.get("raw_video_path") or job.get("output_video_path")
    if not source:
        raise ValueError("任务没有可用原片")
    output = args.output or str(Path(source).with_name(Path(source).stem + "_captioned.mp4"))
    result = render_subtitles(source, output, subtitle_plan, ffmpeg_bin=args.ffmpeg, font_name=args.font_name)
    if not args.no_set_output:
        db.set_postprocessed_video(args.job_id, result["output_video_path"])
        result["database_output_updated"] = True
    else:
        result["database_output_updated"] = False
    result["job_id"] = args.job_id
    return result


def cmd_brand_render(args: argparse.Namespace) -> dict[str, Any]:
    db = _db(args)
    job = db.get_job(args.job_id)
    if not job:
        raise KeyError(f"找不到任务: {args.job_id}")
    source = job.get("output_video_path") or job.get("raw_video_path")
    if not source:
        raise ValueError("任务没有可用视频")
    brand_plan = (job.get("prompt_payload") or {}).get("brand_plan") or {}
    output = args.output or str(Path(source).with_name(Path(source).stem + "_branded.mp4"))
    cover = args.cover_output or str(Path(output).with_name(Path(output).stem + "_cover.jpg"))
    result = render_brand_overlay(
        source, output, brand_plan, cover_output=cover, ffmpeg_bin=args.ffmpeg, ffprobe_bin=args.ffprobe,
    )
    if not args.no_set_output:
        db.set_postprocessed_assets(args.job_id, result["output_video_path"], result["output_cover_path"])
        result["database_output_updated"] = True
    else:
        result["database_output_updated"] = False
    result["job_id"] = args.job_id
    return result


def cmd_postprocess_render(args: argparse.Namespace) -> dict[str, Any]:
    db = _db(args)
    job = db.get_job(args.job_id)
    if not job:
        raise KeyError(f"找不到任务: {args.job_id}")
    source = job.get("raw_video_path") or job.get("output_video_path")
    if not source:
        raise ValueError("任务没有可用原片")
    prompt_payload = job.get("prompt_payload") or {}
    output = args.output or str(Path(source).with_name(Path(source).stem + "_final.mp4"))
    cover = args.cover_output or str(Path(output).with_name(Path(output).stem + "_cover.jpg"))
    result = render_postprocessed_video(
        source,
        output,
        prompt_payload.get("subtitle_plan") or {},
        prompt_payload.get("brand_plan") or {},
        cover_output=cover,
        ffmpeg_bin=args.ffmpeg,
        ffprobe_bin=args.ffprobe,
        subtitle_font_name=args.font_name,
    )
    if not args.no_set_output:
        db.set_postprocessed_assets(args.job_id, result["output_video_path"], result.get("output_cover_path") or "")
        result["database_output_updated"] = True
    else:
        result["database_output_updated"] = False
    result["job_id"] = args.job_id
    return result


def cmd_stats(args: argparse.Namespace) -> dict[str, Any]:
    return _db(args).stats()


def cmd_visual_plan_list(args: argparse.Namespace) -> dict[str, Any]:
    items = _db(args).list_visual_plans(
        source_record_id=args.source_record_id,
        product_id=args.product_id,
        plan_status=args.status,
    )
    return {"count": len(items), "items": items}


def cmd_visual_plan_generate(args: argparse.Namespace) -> dict[str, Any]:
    return run_outfit_generation_worker(
        _db(args), args.command, visual_plan_ids=args.visual_plan_id, limit=args.limit,
        timeout=args.timeout, provider=args.provider,
        allow_scene_text_fallback=args.allow_scene_text_fallback,
    )


def cmd_visual_plan_confirm(args: argparse.Namespace) -> dict[str, Any]:
    return confirm_outfit_image(
        _db(args), args.visual_plan_id, image_path=args.image_path or "", image_url=args.image_url or "",
        image_version=args.image_version or "", feedback=args.feedback or "",
    )


def cmd_visual_plan_create_jobs(args: argparse.Namespace) -> dict[str, Any]:
    return create_confirmed_video_jobs(_db(args), args.visual_plan_id)


def _feishu(args: argparse.Namespace, *, require_review: bool = False) -> tuple[dict[str, Any], dict[str, Any]]:
    config = load_feishu_config(args.config)
    if not config.get("sync_enabled", True):
        raise RuntimeError("飞书同步已被 LIGHT_VIDEO_FEISHU_SYNC_ENABLED/config 关闭")
    if require_review and not config.get("review_enabled", True):
        raise RuntimeError("飞书复核同步已被 LIGHT_VIDEO_FEISHU_REVIEW_ENABLED/config 关闭")
    endpoints = resolve_endpoints(config)
    required = {"review"} if require_review else {"persona", "scene", "action", "shot_plan", "styling", "subtitle", "review"}
    missing = sorted(required - set(endpoints))
    if missing:
        raise ValueError(f"飞书配置缺少表链接: {', '.join(missing)}")
    return config, build_clients(endpoints)


def cmd_feishu_inspect(args: argparse.Namespace) -> dict[str, Any]:
    config, clients = _feishu(args)
    return {"config": config.get("_config_path"), "tables": inspect_tables(clients)}


def cmd_feishu_ensure_schema(args: argparse.Namespace) -> dict[str, Any]:
    config, clients = _feishu(args)
    return {"config": config.get("_config_path"), **ensure_schema(clients, dry_run=args.dry_run)}


def cmd_feishu_init_records(args: argparse.Namespace) -> dict[str, Any]:
    _, clients = _feishu(args)
    return initialize_template_records(_db(args), clients, roles=args.role, business_ids=args.business_id)


def cmd_feishu_pull_templates(args: argparse.Namespace) -> dict[str, Any]:
    config, clients = _feishu(args)
    db = _db(args)
    result: dict[str, Any] = {
        "templates": pull_templates(db, clients, roles=args.role, business_ids=args.business_id, changed_after=args.changed_after)
    }
    if not args.role or {"scene", "styling", "action", "shot_plan"} & set(args.role):
        source_endpoint = resolve_source_endpoint(config)
        if source_endpoint:
            result["source_dropdowns"] = ensure_source_schema(make_client(source_endpoint), db=db)
    return result


def cmd_feishu_push_review(args: argparse.Namespace) -> dict[str, Any]:
    _, clients = _feishu(args, require_review=True)
    return push_reviews(_db(args), clients["review"], limit=args.limit, job_ids=args.job_id, changed_after=args.changed_after)


def cmd_feishu_cleanup_review_duplicates(args: argparse.Namespace) -> dict[str, Any]:
    _, clients = _feishu(args, require_review=True)
    return cleanup_review_duplicates(_db(args), clients["review"], dry_run=args.dry_run)


def cmd_feishu_pull_review(args: argparse.Namespace) -> dict[str, Any]:
    _, clients = _feishu(args, require_review=True)
    return pull_manual_reviews(_db(args), clients["review"], job_ids=args.job_id, changed_after=args.changed_after)


def cmd_feishu_process_review_videos(args: argparse.Namespace) -> dict[str, Any]:
    _, clients = _feishu(args, require_review=True)
    return process_review_videos(
        _db(args), clients["review"], job_ids=args.job_id, limit=args.limit, force=args.force,
        ffmpeg_bin=args.ffmpeg, ffprobe_bin=args.ffprobe, subtitle_font_name=args.font_name,
    )


def _run_manager_feishu(args: argparse.Namespace) -> tuple[dict[str, Any], Any, Any]:
    config, clients = _feishu(args, require_review=True)
    endpoint = resolve_run_manager_endpoint(config)
    if not endpoint:
        raise ValueError("飞书配置缺少 run_manager.url / FEISHU_RUN_MANAGER_URL")
    return config, clients["review"], make_client(endpoint)


def cmd_feishu_ensure_run_manager_schema(args: argparse.Namespace) -> dict[str, Any]:
    _, _, run_client = _run_manager_feishu(args)
    return ensure_run_manager_schema(run_client, dry_run=args.dry_run)


def cmd_feishu_sync_run_manager(args: argparse.Namespace) -> dict[str, Any]:
    config, review_client, run_client = _run_manager_feishu(args)
    db = _db(args)
    result: dict[str, Any] = {
        "preferences": pull_generation_preferences(db, review_client, job_ids=args.job_id, dry_run=args.dry_run),
    }
    result["queue"] = sync_jobs_to_run_manager(
        db, review_client, run_client, job_ids=args.job_id, limit=args.limit, dry_run=args.dry_run,
        store_id=str((config.get("run_manager") or {}).get("store_id") or "myps01"),
    )
    if not args.dry_run:
        result["results"] = pull_run_manager_results(db, review_client, run_client, job_ids=args.job_id)
        returned_ids = [item["job_id"] for item in result["results"].get("items", []) if item.get("status") == "returned"]
        if returned_ids and not args.no_postprocess:
            result["postprocess"] = process_review_videos(
                db, review_client, job_ids=returned_ids, ffmpeg_bin=args.ffmpeg, ffprobe_bin=args.ffprobe,
                subtitle_font_name=args.font_name,
            )
            completed_ids = [item["job_id"] for item in result["postprocess"].get("items", []) if item.get("status") == "success"]
            if completed_ids and not args.no_bgm:
                command = [
                    sys.executable, str(SKILL_DIR / "scripts" / "apply_review_bgm.py"), "--db", str(db.path),
                    "--ffmpeg", args.ffmpeg, "--ffprobe", args.ffprobe,
                ]
                for job_id in completed_ids:
                    command.extend(["--job-id", job_id])
                proc = subprocess.run(command, capture_output=True, text=True, timeout=1800)
                try:
                    result["bgm"] = json.loads(proc.stdout) if proc.stdout.strip() else {}
                except json.JSONDecodeError:
                    result["bgm"] = {"ok": False, "stdout": proc.stdout[-2000:]}
                if proc.returncode != 0:
                    result["bgm"] = {**result.get("bgm", {}), "ok": False, "error": proc.stderr[-2000:]}
    return result


def cmd_feishu_push_visual_plans(args: argparse.Namespace) -> dict[str, Any]:
    _, clients = _feishu(args)
    if "visual_plan" not in clients:
        raise ValueError("飞书配置缺少 tables.visual_plan.url / FEISHU_VISUAL_PLAN_URL")
    return push_visual_plans(_db(args), clients["visual_plan"])


def cmd_feishu_pull_visual_plans(args: argparse.Namespace) -> dict[str, Any]:
    _, clients = _feishu(args)
    if "visual_plan" not in clients:
        raise ValueError("飞书配置缺少 tables.visual_plan.url / FEISHU_VISUAL_PLAN_URL")
    return pull_visual_plan_reviews(_db(args), clients["visual_plan"])


def cmd_feishu_create_visual_plan_table(args: argparse.Namespace) -> dict[str, Any]:
    config = load_feishu_config(args.config)
    endpoints = resolve_endpoints(config)
    if "visual_plan" in endpoints:
        return {"created": False, "reason": "already_configured", "url": endpoints["visual_plan"].url}
    host = endpoints.get(args.host_role)
    if not host:
        raise ValueError(f"飞书配置缺少宿主表: {args.host_role}")
    client = make_client(host)
    existing = [item for item in client.list_tables() if str(item.get("name") or "") == "产品视觉方案表"]
    if existing:
        table_id = str(existing[0].get("table_id") or "")
        created = False
    elif args.dry_run:
        return {"dry_run": True, "created": False, "host_role": args.host_role, "table_name": "产品视觉方案表"}
    else:
        table = client.create_table("产品视觉方案表", "视觉方案")
        table_id = str(table.get("table_id") or "")
        if not table_id:
            raise RuntimeError(f"飞书已响应创建，但未返回 table_id: {table}")
        created = True
    domain = host.url.split("/", 3)[2]
    url = f"https://{domain}/base/{host.app_token}?table={table_id}"
    return {"dry_run": bool(args.dry_run), "created": created, "host_role": args.host_role, "table_id": table_id, "url": url}


def cmd_feishu_create_shot_plan_table(args: argparse.Namespace) -> dict[str, Any]:
    config = load_feishu_config(args.config)
    endpoints = resolve_endpoints(config)
    if "shot_plan" in endpoints:
        return {"created": False, "reason": "already_configured", "url": endpoints["shot_plan"].url}
    host = endpoints.get(args.host_role)
    if not host:
        raise ValueError(f"飞书配置缺少宿主表: {args.host_role}")
    client = make_client(host)
    existing = [item for item in client.list_tables() if str(item.get("name") or "") == "镜头方案库"]
    if existing:
        table_id = str(existing[0].get("table_id") or "")
        created = False
    elif args.dry_run:
        return {"dry_run": True, "created": False, "host_role": args.host_role, "table_name": "镜头方案库"}
    else:
        table = client.create_table("镜头方案库", "镜头方案配置", "镜头方案ID")
        table_id = str(table.get("table_id") or "")
        if not table_id:
            raise RuntimeError(f"飞书已响应创建，但未返回 table_id: {table}")
        created = True
    domain = host.url.split("/", 3)[2]
    url = f"https://{domain}/base/{host.app_token}?table={table_id}"
    return {"dry_run": bool(args.dry_run), "created": created, "host_role": args.host_role, "table_id": table_id, "url": url}


def _source_feishu(args: argparse.Namespace) -> tuple[dict[str, Any], Any]:
    config = load_feishu_config(args.config)
    if not config.get("sync_enabled", True):
        raise RuntimeError("飞书同步已被 LIGHT_VIDEO_FEISHU_SYNC_ENABLED/config 关闭")
    endpoint = resolve_source_endpoint(config)
    if not endpoint:
        raise ValueError("飞书配置缺少 source_script.url / FEISHU_ORIGINAL_SCRIPT_URL")
    return config, make_client(endpoint)


def cmd_feishu_ensure_source_schema(args: argparse.Namespace) -> dict[str, Any]:
    config, client = _source_feishu(args)
    return {"config": config.get("_config_path"), **ensure_source_schema(client, db=_db(args), dry_run=args.dry_run)}


def cmd_feishu_pull_source(args: argparse.Namespace) -> dict[str, Any]:
    _, client = _source_feishu(args)
    return process_source_requests(_db(args), client, record_ids=args.record_id, limit=args.limit, dry_run=args.dry_run)


def cmd_feishu_inspect_source(args: argparse.Namespace) -> dict[str, Any]:
    _, client = _source_feishu(args)
    items = find_source_records(client, args.product_ref)
    return {"product_ref": args.product_ref, "count": len(items), "items": items}


def cmd_feishu_set_source_request(args: argparse.Namespace) -> dict[str, Any]:
    _, client = _source_feishu(args)
    return set_source_request(client, args.product_ref, args.count, record_id=args.record_id)


def cmd_feishu_sync_all(args: argparse.Namespace) -> dict[str, Any]:
    config, clients = _feishu(args)
    db = _db(args)
    result: dict[str, Any] = {"templates": pull_templates(db, clients)}
    if "visual_plan" in clients:
        result["visual_plan_pull"] = pull_visual_plan_reviews(db, clients["visual_plan"])
    source_endpoint = resolve_source_endpoint(config)
    if source_endpoint:
        source_client = make_client(source_endpoint)
        result["source_schema"] = ensure_source_schema(source_client, db=db)
        result["source"] = process_source_requests(db, source_client)
    if "visual_plan" in clients:
        result["visual_plan_push"] = push_visual_plans(db, clients["visual_plan"])
    result["review_push"] = push_reviews(db, clients["review"])
    result["review_pull"] = pull_manual_reviews(db, clients["review"])
    run_endpoint = resolve_run_manager_endpoint(config)
    if run_endpoint:
        run_client = make_client(run_endpoint)
        result["generation_preferences"] = pull_generation_preferences(db, clients["review"])
        result["run_manager_queue"] = sync_jobs_to_run_manager(
            db, clients["review"], run_client,
            store_id=str((config.get("run_manager") or {}).get("store_id") or "myps01"),
        )
        result["run_manager_results"] = pull_run_manager_results(db, clients["review"], run_client)
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AI 轻量试穿视频模板系统 V1.0")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="SQLite 数据库路径")
    sub = parser.add_subparsers(dest="command_name", required=True)

    init = sub.add_parser("init", help="初始化数据库和默认模板")
    init.add_argument("--templates", default=str(DEFAULT_TEMPLATES))
    init.add_argument("--seed", action=argparse.BooleanOptionalAction, default=True)
    init.set_defaults(func=cmd_init)

    template = sub.add_parser("template", help="模板 CRUD")
    template_sub = template.add_subparsers(dest="template_command", required=True)
    template_list = template_sub.add_parser("list")
    template_list.add_argument("--kind", choices=sorted(TEMPLATE_TABLES), required=True)
    template_list.add_argument("--status", choices=["enabled", "disabled", "testing"])
    template_list.set_defaults(func=cmd_template_list)
    template_upsert = template_sub.add_parser("upsert")
    template_upsert.add_argument("--kind", choices=sorted(TEMPLATE_TABLES), required=True)
    template_upsert.add_argument("--json-file", required=True)
    template_upsert.set_defaults(func=cmd_template_upsert)
    template_status = template_sub.add_parser("status")
    template_status.add_argument("--kind", choices=sorted(TEMPLATE_TABLES), required=True)
    template_status.add_argument("--template-id", required=True)
    template_status.add_argument("--status", choices=["enabled", "disabled", "testing"], required=True)
    template_status.set_defaults(func=cmd_template_status)
    template_delete = template_sub.add_parser("delete")
    template_delete.add_argument("--kind", choices=sorted(TEMPLATE_TABLES), required=True)
    template_delete.add_argument("--template-id", required=True)
    template_delete.set_defaults(func=cmd_template_delete)

    product = sub.add_parser("product", help="商品输入管理")
    product_sub = product.add_subparsers(dest="product_command", required=True)
    product_add = product_sub.add_parser("add")
    product_add.add_argument("--file", help="JSON / JSONL 商品文件")
    product_add.add_argument("--product-id")
    product_add.add_argument("--product-name")
    product_add.add_argument("--market", default="TH")
    product_add.add_argument("--language", default="th")
    product_add.add_argument("--category", default="top")
    product_add.add_argument("--sub-category", default="")
    product_add.add_argument("--product-title", default="")
    product_add.add_argument("--image", action="append")
    product_add.add_argument("--selling-point", action="append")
    product_add.add_argument("--count", type=int, default=4)
    product_add.add_argument("--notes", default="")
    product_add.set_defaults(func=cmd_product_add)
    product_list = product_sub.add_parser("list")
    product_list.add_argument("--status")
    product_list.set_defaults(func=cmd_product_list)

    plan = sub.add_parser("plan", help="为商品生成任务单并组装 Prompt")
    plan.add_argument("--product-id", required=True)
    plan.add_argument("--count", type=int)
    plan.add_argument("--plan-version", default="v1")
    plan.add_argument("--no-build-prompts", action="store_true")
    plan.add_argument("--force-prompts", action="store_true")
    plan.set_defaults(func=cmd_plan)

    prompt = sub.add_parser("prompt", help="重新构建 Prompt")
    prompt_sub = prompt.add_subparsers(dest="prompt_command", required=True)
    prompt_build = prompt_sub.add_parser("build")
    prompt_build.add_argument("--job-id")
    prompt_build.add_argument("--product-id")
    prompt_build.add_argument("--force", action="store_true")
    prompt_build.set_defaults(func=cmd_prompt_build)

    job = sub.add_parser("job", help="任务查询与重试")
    job_sub = job.add_subparsers(dest="job_command", required=True)
    job_list = job_sub.add_parser("list")
    job_list.add_argument("--product-id")
    job_list.add_argument("--generation-status", choices=["pending", "generating", "success", "failed", "retrying"])
    job_list.add_argument("--qc-status", choices=["pending", "passed", "failed", "manual_review"])
    job_list.add_argument("--limit", type=int)
    job_list.add_argument("--full", action="store_true")
    job_list.set_defaults(func=cmd_job_list)
    job_reset = job_sub.add_parser("reset")
    job_reset.add_argument("--job-id", required=True)
    job_reset.add_argument("--clear-output", action="store_true")
    job_reset.set_defaults(func=cmd_job_reset)

    export = sub.add_parser("export-jimeng", help="导出可写入现有即梦任务表的 JSONL")
    export.add_argument("--output", required=True)
    export.add_argument("--product-id")
    export.add_argument("--generation-status", default="pending", choices=["pending", "retrying", "failed", "success"])
    export.set_defaults(func=cmd_export_jimeng)

    generate = sub.add_parser("generate", help="通过 JSON stdin/stdout 命令协议调用视频生成 worker")
    generate.add_argument("--command", required=True, help="外部 worker 命令；stdin 接收 JSON，stdout 返回结果 JSON")
    generate.add_argument("--provider", default="command")
    generate.add_argument("--limit", type=int, default=1)
    generate.add_argument("--timeout", type=int, default=900)
    generate.add_argument("--max-attempts", type=int, default=2)
    generate.set_defaults(func=cmd_generate)

    qc = sub.add_parser("qc", help="运行结构 QC，可选外部视觉 QC")
    qc.add_argument("--job-id")
    qc.add_argument("--limit", type=int, default=20)
    qc.add_argument("--vision-result", help="单任务视觉 QC JSON")
    qc.add_argument("--vision-command", help="外部视觉 QC JSON 命令协议")
    qc.add_argument("--ffprobe", default=DEFAULT_FFPROBE)
    qc.add_argument("--timeout", type=int, default=600)
    qc.set_defaults(func=cmd_qc)

    subtitle = sub.add_parser("subtitle-render", help="按任务字幕计划稳定烧录字幕")
    subtitle.add_argument("--job-id", required=True)
    subtitle.add_argument("--output")
    subtitle.add_argument("--ffmpeg", default=DEFAULT_FFMPEG)
    subtitle.add_argument("--font-name", default="Arial Unicode MS")
    subtitle.add_argument("--no-set-output", action="store_true")
    subtitle.set_defaults(func=cmd_subtitle_render)
    brand = sub.add_parser("brand-render", help="按账号视觉身份为视频叠加首屏品牌字标并输出封面")
    brand.add_argument("--job-id", required=True)
    brand.add_argument("--output")
    brand.add_argument("--cover-output")
    brand.add_argument("--ffmpeg", default=DEFAULT_FFMPEG)
    brand.add_argument("--ffprobe", default=DEFAULT_FFPROBE)
    brand.add_argument("--no-set-output", action="store_true")
    brand.set_defaults(func=cmd_brand_render)
    postprocess = sub.add_parser("postprocess-render", help="依次烧录字幕、品牌字标并输出统一封面")
    postprocess.add_argument("--job-id", required=True)
    postprocess.add_argument("--output")
    postprocess.add_argument("--cover-output")
    postprocess.add_argument("--ffmpeg", default=DEFAULT_FFMPEG)
    postprocess.add_argument("--ffprobe", default=DEFAULT_FFPROBE)
    postprocess.add_argument("--font-name", default="Arial Unicode MS")
    postprocess.add_argument("--no-set-output", action="store_true")
    postprocess.set_defaults(func=cmd_postprocess_render)

    review_export = sub.add_parser("review-export", help="导出本地只读人工复核页")
    review_export.add_argument("--output", required=True)
    review_export.add_argument("--product-id")
    review_export.set_defaults(func=cmd_review_export)
    review_set = sub.add_parser("review-set", help="写入人工复核结论")
    review_set.add_argument("--job-id", required=True)
    review_set.add_argument("--decision", choices=["passed", "failed"], required=True)
    review_set.add_argument("--note", default="")
    review_set.set_defaults(func=cmd_review_set)

    stats = sub.add_parser("stats", help="查看生产状态汇总")
    stats.set_defaults(func=cmd_stats)

    visual = sub.add_parser("visual-plan", help="产品视觉方案、产品穿搭图和确认后视频任务")
    visual_sub = visual.add_subparsers(dest="visual_plan_command", required=True)
    visual_list = visual_sub.add_parser("list", help="查询视觉方案")
    visual_list.add_argument("--source-record-id")
    visual_list.add_argument("--product-id")
    visual_list.add_argument("--status", choices=["active", "superseded", "disabled", "failed"])
    visual_list.set_defaults(func=cmd_visual_plan_list)
    visual_generate = visual_sub.add_parser("generate-outfit", help="通过 JSON 命令协议生成产品穿搭图，生成后进入待确认")
    visual_generate.add_argument("--command", required=True)
    visual_generate.add_argument("--visual-plan-id", action="append")
    visual_generate.add_argument("--limit", type=int, default=1)
    visual_generate.add_argument("--timeout", type=int, default=900)
    visual_generate.add_argument("--provider", default="command")
    visual_generate.add_argument("--allow-scene-text-fallback", action="store_true")
    visual_generate.set_defaults(func=cmd_visual_plan_generate)
    visual_confirm = visual_sub.add_parser("confirm-outfit", help="确认产品穿搭图")
    visual_confirm.add_argument("--visual-plan-id", required=True)
    visual_confirm.add_argument("--image-path")
    visual_confirm.add_argument("--image-url")
    visual_confirm.add_argument("--image-version")
    visual_confirm.add_argument("--feedback", default="")
    visual_confirm.set_defaults(func=cmd_visual_plan_confirm)
    visual_jobs = visual_sub.add_parser("create-jobs", help="仅为已确认穿搭图的视觉方案创建视频任务")
    visual_jobs.add_argument("--visual-plan-id", required=True)
    visual_jobs.set_defaults(func=cmd_visual_plan_create_jobs)

    feishu = sub.add_parser("feishu", help="运营配置表与任务复核台同步")
    feishu.add_argument("--config", help="飞书表配置 JSON；默认读取 config/feishu_tables.json")
    feishu_sub = feishu.add_subparsers(dest="feishu_command", required=True)
    feishu_inspect = feishu_sub.add_parser("inspect", help="只读检查字段和记录")
    feishu_inspect.set_defaults(func=cmd_feishu_inspect)
    feishu_schema = feishu_sub.add_parser("ensure-schema", help="幂等建立字段、表名和视图")
    feishu_schema.add_argument("--dry-run", action="store_true")
    feishu_schema.set_defaults(func=cmd_feishu_ensure_schema)
    feishu_init = feishu_sub.add_parser("init-records", help="将本地默认模板初始化到飞书")
    feishu_init.add_argument("--role", action="append", choices=["persona", "scene", "action", "shot_plan", "styling", "subtitle"])
    feishu_init.add_argument("--business-id", action="append", help="只推送指定模板 ID，可重复传入")
    feishu_init.set_defaults(func=cmd_feishu_init_records)
    feishu_pull = feishu_sub.add_parser("pull-templates", help="将飞书运营配置增量同步到 SQLite")
    feishu_pull.add_argument("--role", action="append", choices=["persona", "scene", "action", "shot_plan", "styling", "subtitle"])
    feishu_pull.add_argument("--business-id", action="append", help="只重跑指定模板 ID，可重复传入")
    feishu_pull.add_argument("--changed-after", help="只处理该 ISO 8601 时间之后修改的记录")
    feishu_pull.set_defaults(func=cmd_feishu_pull_templates)
    feishu_push_review = feishu_sub.add_parser("push-review", help="将本地任务幂等推送到复核台")
    feishu_push_review.add_argument("--limit", type=int)
    feishu_push_review.add_argument("--job-id", action="append", help="只推送指定任务 ID，可重复传入")
    feishu_push_review.add_argument("--changed-after", help="只推送该 ISO 8601 时间之后更新的本地任务")
    feishu_push_review.set_defaults(func=cmd_feishu_push_review)
    feishu_cleanup_review = feishu_sub.add_parser("cleanup-review-duplicates", help="保留本地绑定记录并删除复核台重复视频任务行")
    feishu_cleanup_review.add_argument("--dry-run", action="store_true")
    feishu_cleanup_review.set_defaults(func=cmd_feishu_cleanup_review_duplicates)
    feishu_pull_review = feishu_sub.add_parser("pull-review", help="回读人工复核并创建补生成子任务")
    feishu_pull_review.add_argument("--job-id", action="append", help="只回读指定任务 ID，可重复传入")
    feishu_pull_review.add_argument("--changed-after", help="只处理该 ISO 8601 时间之后修改的飞书记录")
    feishu_pull_review.set_defaults(func=cmd_feishu_pull_review)
    feishu_process_videos = feishu_sub.add_parser("process-review-videos", help="下载复核台初始成片，叠加字幕与品牌后上传最终视频")
    feishu_process_videos.add_argument("--job-id", action="append", help="只处理指定任务 ID，可重复传入")
    feishu_process_videos.add_argument("--limit", type=int)
    feishu_process_videos.add_argument("--force", action="store_true")
    feishu_process_videos.add_argument("--ffmpeg", default=DEFAULT_FFMPEG)
    feishu_process_videos.add_argument("--ffprobe", default=DEFAULT_FFPROBE)
    feishu_process_videos.add_argument("--font-name", default="Arial Unicode MS")
    feishu_process_videos.set_defaults(func=cmd_feishu_process_review_videos)
    feishu_run_schema = feishu_sub.add_parser("ensure-run-manager-schema", help="幂等补齐短视频自动运行管理表的轻视频字段")
    feishu_run_schema.add_argument("--dry-run", action="store_true")
    feishu_run_schema.set_defaults(func=cmd_feishu_ensure_run_manager_schema)
    feishu_run_sync = feishu_sub.add_parser("sync-run-manager", help="回读生成配置、幂等入队并将生成视频回流到复核表")
    feishu_run_sync.add_argument("--job-id", action="append", help="只处理指定轻视频任务 ID，可重复传入")
    feishu_run_sync.add_argument("--limit", type=int)
    feishu_run_sync.add_argument("--dry-run", action="store_true")
    feishu_run_sync.add_argument("--no-postprocess", action="store_true")
    feishu_run_sync.add_argument("--no-bgm", action="store_true")
    feishu_run_sync.add_argument("--ffmpeg", default=DEFAULT_FFMPEG)
    feishu_run_sync.add_argument("--ffprobe", default=DEFAULT_FFPROBE)
    feishu_run_sync.add_argument("--font-name", default="Arial Unicode MS")
    feishu_run_sync.set_defaults(func=cmd_feishu_sync_run_manager)
    feishu_push_visual = feishu_sub.add_parser("push-visual-plans", help="将本地产品视觉方案幂等推送到飞书")
    feishu_push_visual.set_defaults(func=cmd_feishu_push_visual_plans)
    feishu_pull_visual = feishu_sub.add_parser("pull-visual-plans", help="回读产品穿搭图确认并按门禁创建视频任务")
    feishu_pull_visual.set_defaults(func=cmd_feishu_pull_visual_plans)
    feishu_create_visual = feishu_sub.add_parser("create-visual-plan-table", help="在指定轻量视频 Base 内幂等创建产品视觉方案表")
    feishu_create_visual.add_argument("--host-role", default="review", choices=["persona", "scene", "action", "styling", "subtitle", "review"])
    feishu_create_visual.add_argument("--dry-run", action="store_true")
    feishu_create_visual.set_defaults(func=cmd_feishu_create_visual_plan_table)
    feishu_create_shot = feishu_sub.add_parser("create-shot-plan-table", help="在指定轻量视频 Base 内幂等创建镜头方案库")
    feishu_create_shot.add_argument("--host-role", default="action", choices=["persona", "scene", "action", "styling", "subtitle", "review"])
    feishu_create_shot.add_argument("--dry-run", action="store_true")
    feishu_create_shot.set_defaults(func=cmd_feishu_create_shot_plan_table)
    feishu_source_schema = feishu_sub.add_parser("ensure-source-schema", help="在原始脚本表迁移轻量视频下拉并新增系统字段")
    feishu_source_schema.add_argument("--dry-run", action="store_true")
    feishu_source_schema.set_defaults(func=cmd_feishu_ensure_source_schema)
    feishu_pull_source = feishu_sub.add_parser("pull-source", help="从原始脚本表幂等认领轻量视频请求")
    feishu_pull_source.add_argument("--record-id", action="append", help="只处理指定飞书 record_id，可重复传入")
    feishu_pull_source.add_argument("--limit", type=int)
    feishu_pull_source.add_argument("--dry-run", action="store_true")
    feishu_pull_source.set_defaults(func=cmd_feishu_pull_source)
    feishu_inspect_source = feishu_sub.add_parser("inspect-source", help="按产品ID/产品编码只读定位原始脚本记录")
    feishu_inspect_source.add_argument("--product-ref", required=True)
    feishu_inspect_source.set_defaults(func=cmd_feishu_inspect_source)
    feishu_set_source = feishu_sub.add_parser("set-source-request", help="为唯一原始脚本记录设置轻量视频数量")
    feishu_set_source.add_argument("--product-ref", required=True)
    feishu_set_source.add_argument("--record-id")
    feishu_set_source.add_argument("--count", type=int, choices=[0, 1, 5], required=True)
    feishu_set_source.set_defaults(func=cmd_feishu_set_source_request)
    feishu_all = feishu_sub.add_parser("sync-all", help="模板回读、任务推送、人工结论回读")
    feishu_all.set_defaults(func=cmd_feishu_sync_all)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        emit(args.func(args))
        return 0
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc), "type": type(exc).__name__}, ensure_ascii=False, indent=2), file=sys.stderr)
        return 2
