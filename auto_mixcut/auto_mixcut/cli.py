from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
import signal
import sys
from pathlib import Path

from auto_mixcut.agent.ai_diversity_budget import AIDiversityBudget
from auto_mixcut.agent.orchestrator import AutoMixcutOrchestratorAgent
from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.skills.ai_segment_factory_skill import AISegmentFactorySkill
from auto_mixcut.skills.ai_supplement_workbench_skill import AISupplementWorkbenchSkill
from auto_mixcut.skills.batch_control_skill import BatchControlSkill
from auto_mixcut.skills.ai_tagging_skill import AITaggingSkill
from auto_mixcut.skills.batch_report_skill import BatchReportSkill
from auto_mixcut.skills.capacity_counter_skill import CapacityCounterSkill
from auto_mixcut.skills.cleanup_skill import CleanupSkill
from auto_mixcut.skills.effective_role_skill import EffectiveRoleSkill
from auto_mixcut.skills.feishu_review_skill import FeishuReviewSkill
from auto_mixcut.skills.final_video_qc_skill import FinalVideoQCSkill
from auto_mixcut.skills.frame_sample_skill import FrameSampleSkill
from auto_mixcut.skills.golden_benchmark_skill import GoldenBenchmarkSkill
from auto_mixcut.skills.media_probe_skill import MediaProbeSkill
from auto_mixcut.skills.oss_storage_skill import OSSStorageSkill
from auto_mixcut.skills.product_anchor_skill import ProductAnchorSkill
from auto_mixcut.skills.quality_gate_skill import QualityGateSkill
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill
from auto_mixcut.skills.readiness_check_skill import ReadinessCheckSkill
from auto_mixcut.skills.remix_skill import RemixSkill
from auto_mixcut.skills.render_plan_skill import RenderPlanSkill
from auto_mixcut.skills.render_skill import RenderSkill
from auto_mixcut.skills.segment_fingerprint_skill import SegmentFingerprintSkill
from auto_mixcut.skills.segment_skill import SegmentSkill
from auto_mixcut.skills.usage_counter_skill import is_good_rendered_output
from auto_mixcut.skills.watermark_detect_skill import WatermarkDetectSkill
from auto_mixcut.skills.watermark_process_skill import WatermarkProcessSkill
from auto_mixcut.skills.bgm_audio_analysis_skill import BgmAudioAnalysisSkill
from auto_mixcut.skills.bgm_library_skill import BgmLibrarySkill
from auto_mixcut.skills.bgm_tag_fusion_skill import BgmTagFusionSkill
from auto_mixcut.skills.bgm_tagging_skill import BgmTaggingSkill


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="auto_mixcut")
    parser.add_argument("--config")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init-db")
    create = sub.add_parser("create-task")
    create.add_argument("--product-id", required=True)
    create.add_argument("--name", required=True)
    create.add_argument("--market", required=True)
    create.add_argument("--category", required=True)
    create.add_argument("--count", type=int, default=5)
    for name in ["anchor", "confirm-anchor", "probe", "watermark-check", "watermark-process", "segment", "frames", "tag", "check", "batch"]:
        p = sub.add_parser(name)
        p.add_argument("--product-id", required=True)
    effective_roles = sub.add_parser("compute-effective-roles")
    effective_roles.add_argument("--product-id", required=True)
    effective_roles.add_argument("--source-type", action="append", default=[], help="limit role computation to one or more source types")
    render_plan = sub.add_parser("render-plan")
    render_plan.add_argument("--product-id", required=True)
    render_plan.add_argument("--count", type=int)
    render_plan.add_argument("--full-refresh", action="store_true", help="ignore existing usable outputs and regenerate a full batch")
    render_plan.add_argument("--confirm-full-refresh", action="store_true", help="required with --full-refresh to prevent accidental full-batch reruns")
    top_up = sub.add_parser("top-up")
    top_up.add_argument("--product-id", required=True)
    top_up.add_argument("--count", type=int)
    top_up.add_argument("--max-rounds", type=int, default=2, help="max small fill-gap rounds; prevents accidental full-batch reruns")
    fingerprint = sub.add_parser("fingerprint")
    fingerprint.add_argument("--product-id", required=True)
    fingerprint.add_argument("--all-segments", action="store_true")
    budget = sub.add_parser("ai-diversity-budget")
    budget.add_argument("--product-id", required=True)
    ai_supplement = sub.add_parser("ai-supplement-workbench")
    ai_supplement.add_argument("--product-id", required=True)
    ai_supplement.add_argument("--max-packages", type=int, default=6)
    ai_supplement.add_argument("--gap-text", default="")
    upload = sub.add_parser("upload")
    upload.add_argument("--product-id", required=True)
    upload.add_argument("--file", required=True)
    upload.add_argument("--source-type", default="self_shot")
    upload.add_argument("--source-trust-level", default="high")
    upload.add_argument("--product-binding-type", default="exact_sku")
    render = sub.add_parser("render")
    render.add_argument("--batch-id", required=True)
    abort_batch = sub.add_parser("abort-batch")
    abort_batch.add_argument("--batch-id", required=True)
    abort_batch.add_argument("--reason", default="operator_abort")
    final_qc = sub.add_parser("final-video-qc")
    final_qc.add_argument("--batch-id", required=True)
    remix = sub.add_parser("execute-remix")
    remix.add_argument("--output-id")
    remix.add_argument("--batch-id")
    remix.add_argument("--limit", type=int, default=10)
    sync_anchor = sub.add_parser("sync-anchor")
    sync_anchor.add_argument("--product-id", required=True)
    sync_review = sub.add_parser("sync-review-segments")
    sync_review.add_argument("--product-id", required=True)
    pull_anchor = sub.add_parser("pull-anchor-confirmations")
    pull_anchor.add_argument("--product-id")
    sync = sub.add_parser("sync-feishu")
    sync.add_argument("--product-id")
    sync.add_argument("--batch-id")
    pull_qc = sub.add_parser("pull-output-qc")
    pull_qc.add_argument("--batch-id")
    pull_qc.add_argument("--product-id")
    report = sub.add_parser("report")
    report.add_argument("--batch-id", required=True)
    cleanup = sub.add_parser("cleanup")
    cleanup.add_argument("--task-id")
    bench = sub.add_parser("benchmark")
    bench.add_argument("--category", required=True)
    bench.add_argument("--prompt-version", default="v1.0")
    ai_sf = sub.add_parser("ai-segment-factory")
    ai_sf.add_argument("--product-id", required=True)
    ai_sf.add_argument("--segment-type", required=True, help="product_display|handheld_product|detail_atmosphere|tryon_result|mirror_routine|home_lifestyle|before_go_out|seasonal_scene|product_still|unboxing|flatlay")
    ai_sf.add_argument("--count", type=int, default=5)
    ai_sf.add_argument("--scene-preference", default="")
    ai_sf.add_argument("--style-preference", default="")
    ai_sf.add_argument("--character-requirement", default="")
    bgm_sync = sub.add_parser("bgm-library-sync")
    bgm_tag = sub.add_parser("bgm-library-tag")
    bgm_tag.add_argument("--bgm-id")
    bgm_tag.add_argument("--force", action="store_true")
    bgm_audio = sub.add_parser("analyze-bgm-audio")
    bgm_audio.add_argument("--bgm-id")
    bgm_audio.add_argument("--limit", type=int)
    bgm_audio.add_argument("--only-missing", action="store_true")
    bgm_audio.add_argument("--no-apply-tags", action="store_true")
    bgm_fuse = sub.add_parser("fuse-bgm-tags")
    bgm_fuse.add_argument("--bgm-id")
    bgm_fuse.add_argument("--limit", type=int)
    bgm_calibrate = sub.add_parser("bgm-library-calibrate")
    bgm_calibrate.add_argument("--only-low-confidence", action="store_true")
    bgm_calibrate.add_argument("--force", action="store_true")
    bgm_rec = sub.add_parser("bgm-library-recommend")
    bgm_rec.add_argument("--product-id", default="")
    bgm_rec.add_argument("--category", default="")
    bgm_rec.add_argument("--mood", default="")
    bgm_rec.add_argument("--template-id", default="")
    args = parser.parse_args(argv)
    ctx = build_context(args.config)
    dispatch = {
        "init-db": lambda: RDSRepositorySkill(ctx).init_db(),
        "create-task": lambda: RDSRepositorySkill(ctx).create_product_task(args.product_id, args.name, args.market, args.category, args.count),
        "anchor": lambda: ProductAnchorSkill(ctx).draft_anchor(args.product_id),
        "confirm-anchor": lambda: ProductAnchorSkill(ctx).confirm_anchor(args.product_id),
        "upload": lambda: OSSStorageSkill(ctx).upload_asset(args.product_id, args.file, args.source_type, args.source_trust_level, args.product_binding_type),
        "probe": lambda: MediaProbeSkill(ctx).probe_product(args.product_id),
        "watermark-check": lambda: WatermarkDetectSkill(ctx).check_product(args.product_id),
        "watermark-process": lambda: WatermarkProcessSkill(ctx).process_product(args.product_id),
        "segment": lambda: SegmentSkill(ctx).segment_product(args.product_id),
        "frames": lambda: FrameSampleSkill(ctx).sample_product(args.product_id),
        "fingerprint": lambda: SegmentFingerprintSkill(ctx).fingerprint_product(args.product_id, only_ai_generated=not args.all_segments),
        "tag": lambda: _tag(ctx, args.product_id),
        "compute-effective-roles": lambda: EffectiveRoleSkill(ctx).compute_product(args.product_id, source_types=args.source_type or None),
        "ai-diversity-budget": lambda: AIDiversityBudget(ctx).evaluate(args.product_id),
        "ai-supplement-workbench": lambda: AISupplementWorkbenchSkill(ctx).sync_for_product(args.product_id, max_packages=args.max_packages, gap_text=args.gap_text),
        "check": lambda: ReadinessCheckSkill(ctx).check_product(args.product_id),
        "render-plan": lambda: _render_plan(ctx, args.product_id, args.count, args.full_refresh, args.confirm_full_refresh),
        "top-up": lambda: _top_up(ctx, args.product_id, args.count, args.max_rounds),
        "render": lambda: RenderSkill(ctx).render_batch(args.batch_id),
        "abort-batch": lambda: BatchControlSkill(ctx).abort_batch(args.batch_id, reason=args.reason),
        "final-video-qc": lambda: FinalVideoQCSkill(ctx).check_batch(args.batch_id),
        "execute-remix": lambda: RemixSkill(ctx).execute_output(args.output_id) if args.output_id else RemixSkill(ctx).execute_pending(args.batch_id, args.limit),
        "sync-anchor": lambda: FeishuReviewSkill(ctx).sync_anchor_queue(args.product_id),
        "sync-review-segments": lambda: FeishuReviewSkill(ctx).sync_review_segments(args.product_id),
        "pull-anchor-confirmations": lambda: FeishuReviewSkill(ctx).pull_anchor_confirmations(args.product_id),
        "sync-feishu": lambda: _sync_feishu(ctx, args.product_id, args.batch_id),
        "pull-output-qc": lambda: FeishuReviewSkill(ctx).pull_output_qc(args.batch_id, args.product_id),
        "report": lambda: BatchReportSkill(ctx).generate(args.batch_id),
        "cleanup": lambda: CleanupSkill(ctx).cleanup_task(args.task_id),
        "benchmark": lambda: GoldenBenchmarkSkill(ctx).run(args.category, args.prompt_version),
        "batch": lambda: AutoMixcutOrchestratorAgent(ctx).run_product(args.product_id, auto_confirm_anchor=True),
        "ai-segment-factory": lambda: AutoMixcutOrchestratorAgent(ctx).run_ai_segment_factory(args.product_id, args.segment_type, args.count, args.scene_preference, args.style_preference, args.character_requirement),
        "bgm-library-sync": lambda: BgmLibrarySkill(ctx).sync_local_library(),
        "bgm-library-tag": lambda: _bgm_tag(ctx, args.bgm_id, args.force),
        "analyze-bgm-audio": lambda: BgmAudioAnalysisSkill(ctx).analyze_track(args.bgm_id, apply_tags=not args.no_apply_tags)
        if args.bgm_id
        else BgmAudioAnalysisSkill(ctx).analyze_all(limit=args.limit, only_missing=args.only_missing, apply_tags=not args.no_apply_tags),
        "fuse-bgm-tags": lambda: BgmTagFusionSkill(ctx).fuse_track(args.bgm_id) if args.bgm_id else BgmTagFusionSkill(ctx).fuse_all(limit=args.limit),
        "bgm-library-calibrate": lambda: BgmTaggingSkill(ctx).calibrate_all(only_low_confidence=args.only_low_confidence, force=args.force),
        "bgm-library-recommend": lambda: BgmLibrarySkill(ctx).get_recommendation(product_id=args.product_id, category=args.category, mood=args.mood, template_id=args.template_id),
    }
    res = dispatch[args.cmd]()
    print(json.dumps(res.to_dict(), ensure_ascii=False, indent=2, default=str))
    return 0 if res.success else 1


def _tag(ctx, product_id):
    first = AITaggingSkill(ctx).submit_batch(product_id)
    return AITaggingSkill(ctx).poll_results(product_id) if first.success else first


def _bgm_tag(ctx, bgm_id, force):
    if bgm_id:
        return BgmTaggingSkill(ctx).tag_track(bgm_id, force=force)
    return BgmTaggingSkill(ctx).calibrate_all(force=force)


def _render_plan(ctx, product_id, count, full_refresh, confirm_full_refresh):
    if full_refresh and not confirm_full_refresh:
        from auto_mixcut.core.result import Result

        return Result.fail(
            "FULL_REFRESH_CONFIRMATION_REQUIRED",
            "--full-refresh will ignore existing usable outputs; pass --confirm-full-refresh only when intentionally regenerating a full batch",
            {"product_id": product_id},
        )
    return RenderPlanSkill(ctx).create_plans(product_id, count=count, fill_gap_only=not full_refresh)


def _top_up(ctx, product_id, count, max_rounds=2):
    from auto_mixcut.core.result import Result

    max_rounds = max(1, int(max_rounds or 1))
    rounds = []
    batch_ids = []
    stop_reason = ""
    for round_no in range(1, max_rounds + 1):
        before = _top_up_snapshot(ctx, product_id, count, refresh_capacity=False)
        if before.get("error"):
            return Result.fail("TOP_UP_SNAPSHOT_FAILED", "failed to read top-up snapshot", {"product_id": product_id, "snapshot": before, "rounds": rounds})
        if before["target_remaining_variant_count"] <= 0:
            stop_reason = "target_already_filled"
            break
        before = _top_up_snapshot(ctx, product_id, count, refresh_capacity=True)
        if before.get("error"):
            return Result.fail("TOP_UP_SNAPSHOT_FAILED", "failed to read top-up snapshot", {"product_id": product_id, "snapshot": before, "rounds": rounds})
        capacity_supplement = None
        capacity_gap_text = _capacity_ai_supplement_gap_text(before)
        if capacity_gap_text:
            capacity_supplement = AISupplementWorkbenchSkill(ctx).sync_for_product(product_id, gap_text=capacity_gap_text)
            if not capacity_supplement.success:
                return _top_up_fail("ai_supplement_workbench", capacity_supplement, product_id, rounds, before)
        if before["material_pool_extra_capacity"] <= 0:
            stop_reason = "ai_supplement_created" if capacity_supplement else "no_material_pool_capacity"
            if capacity_supplement:
                rounds.append(
                    _top_up_round_summary(
                        ctx,
                        round_no,
                        "",
                        before,
                        Result.ok({"skipped": True, "reason": "capacity_shortfall", "gaps": [capacity_gap_text]}),
                        Result.ok({"render_plan_ids": [], "skipped": True, "reason": "waiting_ai_supplement"}),
                        supplement=capacity_supplement,
                    )
                )
            break

        readiness = ReadinessCheckSkill(ctx).check_product(product_id, count)
        if not readiness.success:
            return _top_up_fail("check", readiness, product_id, rounds, before)

        supplement = capacity_supplement
        if _readiness_needs_ai_supplement(readiness.data or {}):
            supplement = AISupplementWorkbenchSkill(ctx).sync_for_product(
                product_id,
                gap_text="; ".join(str(item) for item in ((readiness.data or {}).get("gaps") or [])),
            )
            if not supplement.success:
                return _top_up_fail("ai_supplement_workbench", supplement, product_id, rounds, before)

        planned = _create_render_plans_with_timeout(ctx, product_id, count=_cap_round_count(before, count))
        if not planned.success:
            return _top_up_fail("render_plan", planned, product_id, rounds, before)
        batch_id = (planned.data or {}).get("batch_id") or ""
        plan_ids = (planned.data or {}).get("render_plan_ids") or []
        if not batch_id or not plan_ids:
            stop_reason = "render_plan_empty"
            rounds.append(_top_up_round_summary(ctx, round_no, batch_id, before, readiness, planned, supplement=supplement))
            break

        rendered = RenderSkill(ctx).render_batch(batch_id)
        if not rendered.success:
            return _top_up_fail("render", rendered, product_id, rounds, before, batch_id)

        quality = QualityGateSkill(ctx).check_batch(batch_id)
        if not quality.success:
            return _top_up_fail("quality", quality, product_id, rounds, before, batch_id)

        final_qc = None
        if not _skip_final_video_qc():
            final_qc = FinalVideoQCSkill(ctx).check_batch(batch_id)
            if not final_qc.success:
                return _top_up_fail("final_video_qc", final_qc, product_id, rounds, before, batch_id)

        synced = FeishuReviewSkill(ctx).sync_output_qc(batch_id)
        if not synced.success:
            return _top_up_fail("sync_feishu", synced, product_id, rounds, before, batch_id)

        after = _top_up_snapshot(ctx, product_id, count)
        rounds.append(_top_up_round_summary(ctx, round_no, batch_id, before, readiness, planned, rendered, quality, final_qc, synced, after, supplement))
        batch_ids.append(batch_id)
        if after["target_remaining_variant_count"] <= 0:
            stop_reason = "target_filled"
            break
        if after["material_pool_extra_capacity"] <= 0:
            stop_reason = "no_material_pool_capacity_after_round"
            break
    else:
        stop_reason = "max_rounds_reached"

    if batch_ids:
        task_sync = FeishuReviewSkill(ctx).sync_task(product_id)
        if not task_sync.success:
            return _top_up_fail("sync_task", task_sync, product_id, rounds, _top_up_snapshot(ctx, product_id, count, refresh_capacity=False))
    else:
        task_sync = Result.ok({"status": "skipped", "reason": "no_new_batch"})
    final = _top_up_snapshot(ctx, product_id, count, refresh_capacity=False)
    if final.get("error"):
        return Result.fail("TOP_UP_SNAPSHOT_FAILED", "failed to read final top-up snapshot", {"product_id": product_id, "snapshot": final, "rounds": rounds})
    return Result.ok({
        "product_id": product_id,
        "batch_id": batch_ids[-1] if batch_ids else "",
        "batch_ids": batch_ids,
        "rounds": rounds,
        "final": final,
        "stop_reason": stop_reason,
        "task_sync": task_sync.data,
    })


def _top_up_fail(stage, result, product_id, rounds, snapshot, batch_id=""):
    from auto_mixcut.core.result import Result

    return Result.fail(
        result.error.code if result.error else "TOP_UP_FAILED",
        result.error.message if result.error else f"top-up failed at {stage}",
        {
            "stage": stage,
            "product_id": product_id,
            "batch_id": batch_id,
            "rounds": rounds,
            "snapshot": snapshot,
            "cause": result.to_dict(),
        },
    )


def _create_render_plans_with_timeout(ctx, product_id, count):
    from auto_mixcut.core.result import Result

    timeout_seconds = max(1, int(os.environ.get("AUTO_MIXCUT_RENDER_PLAN_TIMEOUT", "180") or "180"))
    try:
        with _operation_timeout(timeout_seconds):
            return RenderPlanSkill(ctx).create_plans(product_id, count=count, fill_gap_only=True)
    except TimeoutError:
        aborted = _abort_latest_planning_batch(ctx, product_id, "planning_timeout")
        message = f"render planning timed out after {timeout_seconds}s"
        _write_task_timeout(ctx, product_id, message, aborted)
        return Result.fail(
            "RENDER_PLAN_TIMEOUT",
            message,
            {"product_id": product_id, "timeout_seconds": timeout_seconds, "aborted": aborted},
        )


@contextmanager
def _operation_timeout(seconds: int):
    if not hasattr(signal, "SIGALRM"):
        yield
        return
    previous_handler = signal.getsignal(signal.SIGALRM)

    def _raise_timeout(signum, frame):
        raise TimeoutError(f"operation timed out after {seconds}s")

    signal.signal(signal.SIGALRM, _raise_timeout)
    signal.alarm(max(1, int(seconds)))
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous_handler)


def _abort_latest_planning_batch(ctx, product_id: str, reason: str) -> dict:
    batches = ctx.repo.list_where(
        "mixcut_batches",
        "product_id=? AND batch_status='planning' ORDER BY id DESC LIMIT 1",
        (product_id,),
    )
    if not batches:
        return {"status": "skipped", "reason": "no_planning_batch"}
    batch = batches[0]
    batch_id = batch.get("batch_id")
    outputs = ctx.repo.list_where("outputs", "batch_id=? LIMIT 1", (batch_id,))
    if outputs:
        return {"status": "skipped", "reason": "batch_has_outputs", "batch_id": batch_id}
    plans = ctx.repo.list_where("render_plans", "batch_id=?", (batch_id,))
    for plan in plans:
        ctx.repo.update(
            "render_plans",
            "render_plan_id",
            plan["render_plan_id"],
            {"render_status": f"aborted_{reason}", "quality_gate_status": "aborted"},
        )
    ctx.repo.update("mixcut_batches", "batch_id", batch_id, {"batch_status": f"aborted_{reason}"})
    return {"status": "aborted", "batch_id": batch_id, "plan_count": len(plans), "reason": reason}


def _write_task_timeout(ctx, product_id: str, message: str, aborted: dict) -> None:
    tasks = ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC LIMIT 1", (product_id,))
    if not tasks:
        return
    ctx.repo.update(
        "content_tasks",
        "task_id",
        tasks[0]["task_id"],
        {
            "task_status": "RENDER_PLAN_TIMEOUT",
            "pipeline_status": "BLOCKED",
            "next_action": "CHECK_PIPELINE_LOG",
            "last_error": message,
            "last_batch_id": aborted.get("batch_id") or tasks[0].get("last_batch_id"),
        },
    )


def _top_up_round_summary(ctx, round_no, batch_id, before, readiness, planned, rendered=None, quality=None, final_qc=None, synced=None, after=None, supplement=None):
    plan_data = planned.data or {}
    outputs = ctx.repo.list_where("outputs", "batch_id=? ORDER BY id ASC", (batch_id,)) if batch_id else []
    output_items = [
        {
            "output_id": row.get("output_id"),
            "machine_quality_status": row.get("machine_quality_status"),
            "human_quality_status": row.get("human_quality_status"),
            "is_effective": is_good_rendered_output(row),
        }
        for row in outputs
    ]
    return {
        "round": round_no,
        "batch_id": batch_id,
        "before": before,
        "target_variant_count": plan_data.get("target_variant_count"),
        "existing_usable_outputs": plan_data.get("existing_usable_outputs"),
        "fill_gap_count": plan_data.get("fill_gap_count"),
        "planned_count": len(plan_data.get("render_plan_ids") or []),
        "skipped_plan_count": len(plan_data.get("skipped_render_plan_ids") or []),
        "rendered_count": len(outputs),
        "effective_count": sum(1 for row in outputs if is_good_rendered_output(row)),
        "publish_ready_count": sum(1 for row in outputs if row.get("machine_quality_status") == "publish_ready"),
        "needs_review_count": sum(1 for row in outputs if row.get("machine_quality_status") == "needs_review"),
        "draft_only_count": sum(1 for row in outputs if row.get("machine_quality_status") == "draft_only"),
        "synced_count": (synced.data or {}).get("synced_count") if synced else None,
        "new_outputs": output_items,
        "after": after,
        "steps": {
            "check": readiness.to_dict(),
            "ai_supplement_workbench": supplement.to_dict() if supplement else None,
            "render_plan": planned.to_dict(),
            "render": rendered.to_dict() if rendered else None,
            "quality": quality.to_dict() if quality else None,
            "final_video_qc": final_qc.to_dict() if final_qc else None,
            "sync_feishu": synced.to_dict() if synced else None,
        },
    }


def _top_up_snapshot(ctx, product_id, count=None, refresh_capacity=True):
    task = _latest_product_task(ctx, product_id)
    if not task:
        return {
            "target_variant_count": 0,
            "effective_outputs": 0,
            "target_remaining_variant_count": 0,
            "draft_only_outputs": 0,
            "needs_review_outputs": 0,
            "publish_ready_outputs": 0,
            "material_pool_extra_capacity": 0,
            "first_slot_remaining_capacity": 0,
            "current_bottleneck": "",
            "capacity_note": "",
            "error": {"code": "TASK_NOT_FOUND", "message": "task not found", "detail": {"product_id": product_id}},
        }
    allowed = int((task or {}).get("allowed_variant_count") or 0)
    requested = int((task or {}).get("requested_variant_count") or allowed or 0)
    target = int(count or requested or allowed or 0)
    outputs = ctx.repo.list_where("outputs", "product_id=?", (product_id,))
    effective = sum(1 for row in outputs if is_good_rendered_output(row))
    target_remaining = max(0, target - effective)
    if refresh_capacity:
        capacity = CapacityCounterSkill(ctx).refresh_product(product_id)
        if not capacity.success:
            return {
                "target_variant_count": target,
                "effective_outputs": effective,
                "target_remaining_variant_count": target_remaining,
                "draft_only_outputs": sum(1 for row in outputs if row.get("machine_quality_status") == "draft_only"),
                "needs_review_outputs": sum(1 for row in outputs if row.get("machine_quality_status") == "needs_review"),
                "publish_ready_outputs": sum(1 for row in outputs if row.get("machine_quality_status") == "publish_ready"),
                "material_pool_extra_capacity": 0,
                "first_slot_remaining_capacity": 0,
                "current_bottleneck": "",
                "capacity_note": "",
                "error": capacity.to_dict(),
            }
        cap_data = capacity.data or {}
        task = _latest_product_task(ctx, product_id) or task
    else:
        cap_data = task
    return {
        "target_variant_count": target,
        "effective_outputs": effective,
        "target_remaining_variant_count": target_remaining,
        "draft_only_outputs": sum(1 for row in outputs if row.get("machine_quality_status") == "draft_only"),
        "needs_review_outputs": sum(1 for row in outputs if row.get("machine_quality_status") == "needs_review"),
        "publish_ready_outputs": sum(1 for row in outputs if row.get("machine_quality_status") == "publish_ready"),
        "material_pool_extra_capacity": int(cap_data.get("material_pool_extra_capacity") or 0),
        "first_slot_remaining_capacity": int(cap_data.get("first_slot_remaining_capacity") or 0),
        "current_bottleneck": cap_data.get("current_bottleneck") or "",
        "capacity_note": cap_data.get("capacity_note") or "",
    }


def _latest_product_task(ctx, product_id):
    rows = ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC", (product_id,))
    return rows[0] if rows else {}


def _readiness_needs_ai_supplement(data):
    return any("AI补素材" in str(item) for item in (data.get("gaps") or []))


def _cap_round_count(snapshot: dict, count: int | None) -> int:
    max_per_round = int(os.environ.get("AUTO_MIXCUT_TOP_UP_MAX_PER_ROUND", "5") or "5")
    remaining = int(snapshot.get("target_remaining_variant_count") or 0)
    extra = int(snapshot.get("material_pool_extra_capacity") or 0)
    allowed = min(remaining, extra)
    requested = int(count or snapshot.get("target_variant_count") or remaining or allowed or 0)
    if requested <= 0:
        requested = allowed
    return max(0, min(allowed, max_per_round, requested))


def _skip_final_video_qc() -> bool:
    value = str(os.environ.get("AUTO_MIXCUT_SKIP_FINAL_VIDEO_QC", "1")).strip().lower()
    return value not in {"0", "false", "no", "off"}


def _capacity_ai_supplement_gap_text(snapshot: dict) -> str:
    remaining = int(snapshot.get("target_remaining_variant_count") or 0)
    extra_capacity = int(snapshot.get("material_pool_extra_capacity") or 0)
    shortfall = max(0, remaining - extra_capacity)
    if shortfall <= 0:
        return ""
    need = min(max(shortfall, 1), 6)
    bottleneck = str(snapshot.get("current_bottleneck") or snapshot.get("capacity_note") or "")
    if "首镜" in bottleneck or int(snapshot.get("first_slot_remaining_capacity") or 0) <= 0:
        hero = min(max(need, 1), 6)
        return f"AI补素材: hero首镜{hero}"
    hero = max(1, min(2, need))
    detail = max(1, min(2, need - 1)) if need >= 2 else 0
    result = 1 if need >= 3 else 0
    scene = 1 if need >= 4 else 0
    parts = [f"hero首镜{hero}"]
    if detail:
        parts.append(f"detail细节{detail}")
    if result:
        parts.append(f"result上身{result}")
    if scene:
        parts.append(f"scene场景{scene}")
    return "AI补素材: " + "; ".join(parts)


def _sync_feishu(ctx, product_id, batch_id):
    if batch_id:
        return FeishuReviewSkill(ctx).sync_output_qc(batch_id)
    if product_id:
        return FeishuReviewSkill(ctx).sync_task(product_id)
    from auto_mixcut.core.result import Result

    return Result.fail("ARGUMENT_REQUIRED", "--product-id or --batch-id is required")


if __name__ == "__main__":
    sys.exit(main())
