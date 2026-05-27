from __future__ import annotations

from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result

from .context import SkillContext


class GoldenBenchmarkSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def run(self, category: str, prompt_version: str = "v1.0") -> Result:
        goldens = self.ctx.repo.list_where("golden_segments", "category=? AND active=1", (category,))
        total = len(goldens)
        passed = 0
        for golden in goldens:
            label = self.ctx.repo.list_where("golden_labels", "golden_segment_id=? ORDER BY id DESC", (golden["golden_segment_id"],))
            tag = self.ctx.repo.list_where("segment_tags", "segment_id=? ORDER BY id DESC", (golden["segment_id"],))
            if label and tag and label[0].get("expected_primary_shot_role") == tag[0].get("primary_shot_role"):
                passed += 1
        score = passed / total if total else 0
        run_id = new_id("BENCH")
        self.ctx.repo.upsert(
            "golden_benchmark_runs",
            "benchmark_run_id",
            {"benchmark_run_id": run_id, "model_tier": "medium_vision", "model_name": "mock-medium-vision", "prompt_version": prompt_version, "category": category, "total_segments": total, "passed_segments": passed, "failed_segments": total - passed, "overall_score": score, "role_accuracy": score, "visibility_accuracy": score, "hook_accuracy": score, "risk_recall": score, "risk_precision": score},
        )
        return Result.ok({"benchmark_run_id": run_id, "total_segments": total, "passed_segments": passed, "overall_score": score})
