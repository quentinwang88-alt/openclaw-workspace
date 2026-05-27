from __future__ import annotations

import json
import hashlib
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result

from .context import SkillContext


@dataclass
class GeneratedAsset:
    local_path: Path
    generation_model: str
    generation_type: str
    generation_prompt: str
    metadata: Dict[str, Any]


class AIGenerationProvider(ABC):
    @abstractmethod
    def name(self) -> str:
        ...

    @abstractmethod
    def generate(self, prompt: str, count: int, segment_type: str, ctx: SkillContext, temp_dir: Path) -> List[GeneratedAsset]:
        ...


class MockGenerationProvider(AIGenerationProvider):
    def name(self) -> str:
        return "mock-generator"

    def generate(self, prompt: str, count: int, segment_type: str, ctx: SkillContext, temp_dir: Path) -> List[GeneratedAsset]:
        assets = []
        for i in range(count):
            filename = f"{segment_type}_{i + 1:03d}.mp4"
            local = temp_dir / filename
            mock_content = json.dumps({"mock": True, "segment_type": segment_type, "prompt_hash": hashlib.sha256(prompt.encode()).hexdigest()[:8], "index": i}, ensure_ascii=False).encode("utf-8")
            local.write_bytes(mock_content)
            assets.append(GeneratedAsset(
                local_path=local,
                generation_model=self.name(),
                generation_type="text_to_video",
                generation_prompt=prompt,
                metadata={"index": i, "segment_type": segment_type},
            ))
        return assets


class OpenAIVideoProvider(AIGenerationProvider):
    def __init__(self, api_key: str = "", base_url: str = ""):
        self.api_key = api_key
        self.base_url = base_url

    def name(self) -> str:
        return "openai-video"

    def generate(self, prompt: str, count: int, segment_type: str, ctx: SkillContext, temp_dir: Path) -> List[GeneratedAsset]:
        try:
            from openai import OpenAI
        except ImportError:
            return [self._mock_fallback(prompt, segment_type, temp_dir, 0)]

        client = OpenAI(api_key=self.api_key) if self.api_key else OpenAI()
        assets = []
        for i in range(count):
            filename = f"{segment_type}_{i + 1:03d}.mp4"
            local = temp_dir / filename
            try:
                response = client.chat.completions.create(
                    model=ctx.settings.__dict__.get("generation_model", "gpt-4o"),
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=500,
                )
                local.write_text(response.choices[0].message.content or "", encoding="utf-8")
            except Exception:
                return [self._mock_fallback(prompt, segment_type, temp_dir, 0)]
            assets.append(GeneratedAsset(
                local_path=local,
                generation_model=self.name(),
                generation_type="text_to_video",
                generation_prompt=prompt,
                metadata={"index": i, "segment_type": segment_type},
            ))
        return assets

    def _mock_fallback(self, prompt: str, segment_type: str, temp_dir: Path, index: int) -> GeneratedAsset:
        local = temp_dir / f"{segment_type}_fallback.mp4"
        local.write_bytes(b"mock fallback video")
        return GeneratedAsset(local_path=local, generation_model="mock-fallback", generation_type="text_to_video", generation_prompt=prompt, metadata={})


def _default_provider() -> AIGenerationProvider:
    return MockGenerationProvider()


class AIGenerationProviderSkill:
    def __init__(self, ctx: SkillContext, provider: AIGenerationProvider | None = None):
        self.ctx = ctx
        self._provider = provider

    def _get_provider(self) -> AIGenerationProvider:
        return self._provider or _default_provider()

    def generate_for_job(self, job_id: str) -> Result:
        job = self.ctx.repo.get("ai_generation_jobs", "job_id", job_id)
        if not job:
            return Result.fail("JOB_NOT_FOUND", "ai_generation_jobs not found", {"job_id": job_id})
        if not job.get("prompt_text"):
            return Result.fail("PROMPT_MISSING", "generation prompt not yet generated", {"job_id": job_id})

        self.ctx.repo.update("ai_generation_jobs", "job_id", job_id, {"status": "GENERATING"})
        temp_dir = self.ctx.settings.temp_root / "ai_generated" / job["product_id"] / job_id
        temp_dir.mkdir(parents=True, exist_ok=True)

        provider = self._get_provider()
        count = int(job.get("requested_count") or 5)
        segment_type = str(job.get("segment_type") or "")
        prompt = str(job.get("prompt_text") or "")

        try:
            assets = provider.generate(prompt, count, segment_type, self.ctx, temp_dir)
        except Exception as exc:
            self.ctx.repo.update("ai_generation_jobs", "job_id", job_id, {"status": "GENERATION_FAILED", "failure_reason": str(exc)})
            return Result.fail("GENERATION_FAILED", str(exc), {"job_id": job_id})

        if not assets:
            self.ctx.repo.update("ai_generation_jobs", "job_id", job_id, {"status": "GENERATION_FAILED", "failure_reason": "no assets generated"})
            return Result.fail("GENERATION_FAILED", "no assets generated", {"job_id": job_id})

        self.ctx.repo.update("ai_generation_jobs", "job_id", job_id, {
            "status": "GENERATED",
            "generated_count": len(assets),
            "generation_type": provider.name(),
            "model_name": provider.name(),
        })

        return Result.ok({
            "job_id": job_id,
            "generated_count": len(assets),
            "temp_dir": str(temp_dir),
            "files": [str(a.local_path) for a in assets],
        })
