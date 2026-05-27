from __future__ import annotations

from auto_mixcut.core.result import Result
from auto_mixcut.skills.ai_generated_consistency_skill import AIGeneratedConsistencySkill
from auto_mixcut.skills.ai_segment_factory_skill import AISegmentFactorySkill
from auto_mixcut.skills.ai_tagging_skill import AITaggingSkill
from auto_mixcut.skills.cleanup_skill import CleanupSkill
from auto_mixcut.skills.effective_role_skill import EffectiveRoleSkill
from auto_mixcut.skills.feishu_review_skill import FeishuReviewSkill
from auto_mixcut.skills.frame_sample_skill import FrameSampleSkill
from auto_mixcut.skills.media_probe_skill import MediaProbeSkill
from auto_mixcut.skills.product_anchor_skill import ProductAnchorSkill
from auto_mixcut.skills.quality_gate_skill import QualityGateSkill
from auto_mixcut.skills.readiness_check_skill import ReadinessCheckSkill
from auto_mixcut.skills.render_plan_skill import RenderPlanSkill
from auto_mixcut.skills.render_skill import RenderSkill
from auto_mixcut.skills.segment_skill import SegmentSkill
from auto_mixcut.skills.watermark_detect_skill import WatermarkDetectSkill
from auto_mixcut.skills.context import SkillContext


class AutoMixcutOrchestratorAgent:
    """State-machine coordinator. Skills own concrete work."""

    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def run_product(self, product_id: str, requested_count: int | None = None, auto_confirm_anchor: bool = False) -> Result:
        steps = []
        anchor = ProductAnchorSkill(self.ctx)
        product = self.ctx.repo.get("products", "product_id", product_id)
        if not product:
            return Result.fail("PRODUCT_NOT_FOUND", "product must be created before orchestration", {"product_id": product_id})
        if product.get("anchor_status") != "confirmed":
            drafted = anchor.draft_anchor(product_id)
            steps.append(("anchor_draft", drafted.to_dict()))
            if not drafted.success:
                return drafted
            FeishuReviewSkill(self.ctx).sync_anchor_queue(product_id)
            if not auto_confirm_anchor:
                self.ctx.repo.update("content_tasks", "product_id", product_id, {"task_status": "ANCHOR_PENDING", "blocked_reason": "waiting for anchor confirmation"})
                return Result.fail("ANCHOR_PENDING", "anchor drafted and waiting for Feishu confirmation", {"product_id": product_id})
            confirmed = anchor.confirm_anchor(product_id, "auto")
            steps.append(("anchor_confirm", confirmed.to_dict()))
            if not confirmed.success:
                return confirmed
        pipeline = [
            ("probe", lambda: MediaProbeSkill(self.ctx).probe_product(product_id)),
            ("watermark", lambda: WatermarkDetectSkill(self.ctx).check_product(product_id)),
            ("segment", lambda: SegmentSkill(self.ctx).segment_product(product_id)),
            ("frames", lambda: FrameSampleSkill(self.ctx).sample_product(product_id)),
            ("tag_submit", lambda: AITaggingSkill(self.ctx).submit_batch(product_id)),
            ("tag_poll", lambda: AITaggingSkill(self.ctx).poll_results(product_id)),
            ("consistency", lambda: AIGeneratedConsistencySkill(self.ctx).check_product(product_id)),
            ("effective_roles", lambda: EffectiveRoleSkill(self.ctx).compute_product(product_id)),
            ("readiness", lambda: ReadinessCheckSkill(self.ctx).check_product(product_id, requested_count)),
            ("render_plan", lambda: RenderPlanSkill(self.ctx).create_plans(product_id, requested_count)),
        ]
        batch_id = None
        for name, fn in pipeline:
            res = fn()
            steps.append((name, res.to_dict()))
            if not res.success:
                self.ctx.repo.update("content_tasks", "product_id", product_id, {"task_status": name.upper() + "_FAILED", "failure_reason": res.error.message if res.error else ""})
                return res
            if name == "render_plan":
                batch_id = res.data["batch_id"]
        rendered = RenderSkill(self.ctx).render_batch(batch_id)
        steps.append(("render", rendered.to_dict()))
        if not rendered.success:
            return rendered
        qc = QualityGateSkill(self.ctx).check_batch(batch_id)
        steps.append(("quality", qc.to_dict()))
        FeishuReviewSkill(self.ctx).sync_task(product_id)
        FeishuReviewSkill(self.ctx).sync_review_segments(product_id)
        FeishuReviewSkill(self.ctx).sync_output_qc(batch_id)
        return Result.ok({"product_id": product_id, "batch_id": batch_id, "steps": steps})

    def run_ai_segment_factory(self, product_id: str, segment_type: str, requested_count: int = 5, scene_preference: str = "", style_preference: str = "", character_requirement: str = "") -> Result:
        product = self.ctx.repo.get("products", "product_id", product_id)
        if not product:
            return Result.fail("PRODUCT_NOT_FOUND", "product must be created before ai segment generation", {"product_id": product_id})
        if product.get("anchor_status") != "confirmed":
            return Result.fail("ANCHOR_NOT_CONFIRMED", "product anchor must be confirmed first", {"product_id": product_id})

        return AISegmentFactorySkill(self.ctx).run(
            product_id=product_id,
            segment_type=segment_type,
            requested_count=requested_count,
            scene_preference=scene_preference,
            style_preference=style_preference,
            character_requirement=character_requirement,
        )

    def cleanup(self, task_id: str | None = None) -> Result:
        return CleanupSkill(self.ctx).cleanup_task(task_id)
