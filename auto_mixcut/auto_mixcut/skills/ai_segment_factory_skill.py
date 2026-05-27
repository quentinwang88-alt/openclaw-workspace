from __future__ import annotations

from typing import Any, Dict, List, Optional

from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result

from .ai_anchor_check_skill import AIAnchorCheckSkill
from .ai_generated_consistency_skill import AIGeneratedConsistencySkill
from .ai_generation_import_skill import AIGenerationImportSkill
from .ai_generation_provider_skill import AIGenerationProvider, AIGenerationProviderSkill
from .ai_generation_qc_skill import AIGenerationQCSkill
from .ai_segment_factory_config import AISegmentFactoryConfig, get_config
from .ai_tagging_skill import AITaggingSkill
from .context import SkillContext
from .effective_role_skill import EffectiveRoleSkill
from .frame_sample_skill import FrameSampleSkill
from .segment_prompt_generator_skill import SegmentPromptGeneratorSkill


class AISegmentFactorySkill:
    def __init__(self, ctx: SkillContext, provider: Optional[AIGenerationProvider] = None, config: Optional[AISegmentFactoryConfig] = None):
        self.ctx = ctx
        self._provider = provider
        self.config = config or get_config()

    def run(self, product_id: str, segment_type: str, requested_count: int = 5, scene_preference: str = "", style_preference: str = "", character_requirement: str = "") -> Result:
        product = self.ctx.repo.get("products", "product_id", product_id)
        if not product:
            return Result.fail("PRODUCT_NOT_FOUND", "product must be created first", {"product_id": product_id})

        rule = self.config.get_segment_type_rule(segment_type)
        steps: List[tuple[str, Dict[str, Any]]] = []

        anchor_res = self._step_anchor_check(product_id, product)
        steps.append(("anchor_check", anchor_res.to_dict()))
        if not anchor_res.success:
            return anchor_res

        prompt_res = self._step_generate_prompt(product_id, segment_type, scene_preference, style_preference, character_requirement)
        steps.append(("prompt_generation", prompt_res.to_dict()))
        if not prompt_res.success:
            return prompt_res

        job_id = self._create_job(product_id, segment_type, requested_count, prompt_res.data, scene_preference, style_preference, character_requirement)
        steps.append(("job_created", {"job_id": job_id}))

        gen_res = self._step_generate(job_id)
        steps.append(("generation", gen_res.to_dict()))
        if not gen_res.success:
            return gen_res

        import_res = self._step_import(job_id)
        steps.append(("import", import_res.to_dict()))
        if not import_res.success:
            return import_res

        sample_res = self._step_frame_sample(product_id)
        steps.append(("frame_sample", sample_res.to_dict()))
        if not sample_res.success:
            return sample_res

        tagging_res = self._step_ai_tagging(product_id)
        steps.append(("ai_tagging", tagging_res.to_dict()))
        if not tagging_res.success:
            return tagging_res

        consistency_res = self._step_consistency(product_id)
        steps.append(("consistency", consistency_res.to_dict()))
        if not consistency_res.success:
            return consistency_res

        qc_res = self._step_qc(job_id)
        steps.append(("qc", qc_res.to_dict()))
        if not qc_res.success:
            return qc_res

        anchor_check_res = self._step_anchor_match(job_id)
        steps.append(("anchor_match", anchor_check_res.to_dict()))

        effective_res = self._step_effective_roles(product_id)
        steps.append(("effective_roles", effective_res.to_dict()))

        grading = self._step_grading(job_id)
        steps.append(("grading", grading.to_dict()))

        return Result.ok({
            "product_id": product_id,
            "job_id": job_id,
            "segment_type": segment_type,
            "segment_type_risk": rule.risk_level,
            "requested_count": requested_count,
            "grading": grading.data if grading.success else {},
            "steps": steps,
        })

    def _step_anchor_check(self, product_id: str, product: Dict[str, Any]) -> Result:
        if product.get("anchor_status") == "confirmed":
            return Result.ok({"product_id": product_id, "anchor_status": "confirmed"})
        return Result.fail("ANCHOR_NOT_CONFIRMED", "product anchor must be confirmed before AI segment generation", {"product_id": product_id})

    def _step_generate_prompt(self, product_id: str, segment_type: str, scene_preference: str, style_preference: str, character_requirement: str) -> Result:
        return SegmentPromptGeneratorSkill(self.ctx, self.config).generate(product_id, segment_type, scene_preference, style_preference, character_requirement)

    def _create_job(self, product_id: str, segment_type: str, requested_count: int, prompt_data: Dict[str, Any], scene_preference: str, style_preference: str, character_requirement: str) -> str:
        product = self.ctx.repo.get("products", "product_id", product_id) or {}
        job_id = new_id("AIJOB")
        job_row = {
            "job_id": job_id, "product_id": product_id,
            "market": product.get("market") or "", "category": product.get("category") or "",
            "segment_type": segment_type, "requested_count": requested_count,
            "generated_count": 0, "accepted_count": 0, "imported_segment_count": 0,
            "strict_pass_count": 0, "soft_pass_count": 0, "uncertain_count": 0, "fail_count": 0,
            "prompt_version": "segment_prompt_v1", "prompt_text": str(prompt_data.get("prompt") or ""),
            "scene_preference": scene_preference, "style_preference": style_preference,
            "character_requirement": character_requirement, "status": "PROMPT_GENERATED",
        }
        self.ctx.repo.upsert("ai_generation_jobs", "job_id", job_row)
        return job_id

    def _step_generate(self, job_id: str) -> Result:
        return AIGenerationProviderSkill(self.ctx, self._provider).generate_for_job(job_id)

    def _step_import(self, job_id: str) -> Result:
        return AIGenerationImportSkill(self.ctx).import_generated_assets(job_id)

    def _step_frame_sample(self, product_id: str) -> Result:
        return FrameSampleSkill(self.ctx).sample_product(product_id)

    def _step_ai_tagging(self, product_id: str) -> Result:
        tagger = AITaggingSkill(self.ctx)
        submit = tagger.submit_batch(product_id)
        return tagger.poll_results(product_id) if submit.success else submit

    def _step_consistency(self, product_id: str) -> Result:
        return AIGeneratedConsistencySkill(self.ctx).check_product(product_id)

    def _step_qc(self, job_id: str) -> Result:
        return AIGenerationQCSkill(self.ctx, self.config).check_job(job_id)

    def _step_anchor_match(self, job_id: str) -> Result:
        job = self.ctx.repo.get("ai_generation_jobs", "job_id", job_id) or {}
        return AIAnchorCheckSkill(self.ctx).check_product(str(job.get("product_id") or ""))

    def _step_effective_roles(self, product_id: str) -> Result:
        return EffectiveRoleSkill(self.ctx, self.config).compute_product(product_id)

    def _step_grading(self, job_id: str) -> Result:
        job = self.ctx.repo.get("ai_generation_jobs", "job_id", job_id) or {}
        product_id = str(job.get("product_id") or "")
        segments = self.ctx.repo.list_where("segments", "product_id=? AND source_type='ai_generated'", (product_id,))

        result: Dict[str, Any] = {"total": len(segments), "A_core": 0, "B_scene": 0, "C_reference": 0, "D_reject": 0, "details": []}
        for s in segments:
            tag = self._latest_tag(s["segment_id"])
            grade = self.config.classify_grade(
                anchor_match_level=str(s.get("anchor_match_level") or ""),
                frame_consistency_status=str(s.get("frame_consistency_status") or ""),
                product_visibility=str(tag.get("product_visibility") or "medium"),
                risk_level=str(tag.get("risk_level") or "medium"),
                mixcut_usability=str(tag.get("mixcut_usability") or "no"),
            )
            result[grade] = result.get(grade, 0) + 1
            result["details"].append({"segment_id": s["segment_id"], "grade": grade, "roles": s.get("effective_roles_json") or []})

        total = max(result["total"], 1)
        result["core_rate"] = round(result["A_core"] / total * 100, 1)
        result["scene_rate"] = round((result["A_core"] + result["B_scene"]) / total * 100, 1)
        result["reject_rate"] = round(result["D_reject"] / total * 100, 1)

        return Result.ok(result)

    def _latest_tag(self, segment_id: str) -> Dict[str, Any]:
        rows = self.ctx.repo.list_where("segment_tags", "segment_id=? ORDER BY id DESC", (segment_id,))
        return rows[0] if rows else {}
