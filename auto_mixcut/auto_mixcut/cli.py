from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from auto_mixcut.agent.ai_diversity_budget import AIDiversityBudget
from auto_mixcut.agent.orchestrator import AutoMixcutOrchestratorAgent
from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.skills.ai_segment_factory_skill import AISegmentFactorySkill
from auto_mixcut.skills.ai_tagging_skill import AITaggingSkill
from auto_mixcut.skills.batch_report_skill import BatchReportSkill
from auto_mixcut.skills.cleanup_skill import CleanupSkill
from auto_mixcut.skills.effective_role_skill import EffectiveRoleSkill
from auto_mixcut.skills.feishu_review_skill import FeishuReviewSkill
from auto_mixcut.skills.final_video_qc_skill import FinalVideoQCSkill
from auto_mixcut.skills.frame_sample_skill import FrameSampleSkill
from auto_mixcut.skills.golden_benchmark_skill import GoldenBenchmarkSkill
from auto_mixcut.skills.media_probe_skill import MediaProbeSkill
from auto_mixcut.skills.oss_storage_skill import OSSStorageSkill
from auto_mixcut.skills.product_anchor_skill import ProductAnchorSkill
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill
from auto_mixcut.skills.readiness_check_skill import ReadinessCheckSkill
from auto_mixcut.skills.remix_skill import RemixSkill
from auto_mixcut.skills.render_plan_skill import RenderPlanSkill
from auto_mixcut.skills.render_skill import RenderSkill
from auto_mixcut.skills.segment_fingerprint_skill import SegmentFingerprintSkill
from auto_mixcut.skills.segment_skill import SegmentSkill
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
    for name in ["anchor", "confirm-anchor", "probe", "watermark-check", "watermark-process", "segment", "frames", "tag", "compute-effective-roles", "check", "render-plan", "batch"]:
        p = sub.add_parser(name)
        p.add_argument("--product-id", required=True)
    fingerprint = sub.add_parser("fingerprint")
    fingerprint.add_argument("--product-id", required=True)
    fingerprint.add_argument("--all-segments", action="store_true")
    budget = sub.add_parser("ai-diversity-budget")
    budget.add_argument("--product-id", required=True)
    upload = sub.add_parser("upload")
    upload.add_argument("--product-id", required=True)
    upload.add_argument("--file", required=True)
    upload.add_argument("--source-type", default="self_shot")
    upload.add_argument("--source-trust-level", default="high")
    upload.add_argument("--product-binding-type", default="exact_sku")
    render = sub.add_parser("render")
    render.add_argument("--batch-id", required=True)
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
    report = sub.add_parser("report")
    report.add_argument("--batch-id", required=True)
    cleanup = sub.add_parser("cleanup")
    cleanup.add_argument("--task-id")
    bench = sub.add_parser("benchmark")
    bench.add_argument("--category", required=True)
    bench.add_argument("--prompt-version", default="v1.0")
    ai_sf = sub.add_parser("ai-segment-factory")
    ai_sf.add_argument("--product-id", required=True)
    ai_sf.add_argument("--segment-type", required=True, help="product_display|handheld_product|detail_atmosphere|tryon_result|mirror_routine|home_lifestyle|before_go_out|seasonal_scene")
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
        "compute-effective-roles": lambda: EffectiveRoleSkill(ctx).compute_product(args.product_id),
        "ai-diversity-budget": lambda: AIDiversityBudget(ctx).evaluate(args.product_id),
        "check": lambda: ReadinessCheckSkill(ctx).check_product(args.product_id),
        "render-plan": lambda: RenderPlanSkill(ctx).create_plans(args.product_id),
        "render": lambda: RenderSkill(ctx).render_batch(args.batch_id),
        "final-video-qc": lambda: FinalVideoQCSkill(ctx).check_batch(args.batch_id),
        "execute-remix": lambda: RemixSkill(ctx).execute_output(args.output_id) if args.output_id else RemixSkill(ctx).execute_pending(args.batch_id, args.limit),
        "sync-anchor": lambda: FeishuReviewSkill(ctx).sync_anchor_queue(args.product_id),
        "sync-review-segments": lambda: FeishuReviewSkill(ctx).sync_review_segments(args.product_id),
        "pull-anchor-confirmations": lambda: FeishuReviewSkill(ctx).pull_anchor_confirmations(args.product_id),
        "sync-feishu": lambda: _sync_feishu(ctx, args.product_id, args.batch_id),
        "pull-output-qc": lambda: FeishuReviewSkill(ctx).pull_output_qc(args.batch_id),
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


def _sync_feishu(ctx, product_id, batch_id):
    if batch_id:
        return FeishuReviewSkill(ctx).sync_output_qc(batch_id)
    if product_id:
        return FeishuReviewSkill(ctx).sync_task(product_id)
    from auto_mixcut.core.result import Result

    return Result.fail("ARGUMENT_REQUIRED", "--product-id or --batch-id is required")


if __name__ == "__main__":
    sys.exit(main())
