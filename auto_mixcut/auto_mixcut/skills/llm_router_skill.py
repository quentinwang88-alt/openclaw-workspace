from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from auto_mixcut.adapters.llm_provider import LLMProvider, LLMResponse, MockLLMProvider
from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result
from auto_mixcut.core.storage_paths import require_oss_object_path

from .context import SkillContext

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "llm_router.json"

RETRYABLE_ERRORS = [
    "ConnectionError", "TimeoutError", "RemoteProtocolError", "ReadTimeout",
    "ConnectTimeout", "RateLimitError",
]
RETRYABLE_STATUS_CODES = {"429", "500", "502", "503", "504"}


class LLMRouterSkill:
    def __init__(self, ctx: SkillContext, config_path: Optional[str] = None):
        self.ctx = ctx
        self._config = _load_config(config_path or str(DEFAULT_CONFIG_PATH))
        self._providers: Dict[str, LLMProvider] = {}
        self._mock_provider: LLMProvider = MockLLMProvider()
        self._concurrency_locks: Dict[str, threading.Semaphore] = {}

    def call(
        self,
        call_type: str,
        payload: Dict[str, Any],
        *,
        product_id: str = "",
        segment_id: str = "",
        asset_id: str = "",
        output_id: str = "",
        task_id: str = "",
        force_tier: Optional[str] = None,
        force_escalation: bool = False,
    ) -> Result:
        routing = self._config.get("call_type_routing", {}).get(call_type)
        if not routing:
            return Result.fail("UNKNOWN_CALL_TYPE", f"no routing config for call_type: {call_type}", {"call_type": call_type})

        tier_name = force_tier or routing.get("tier", "medium_vision")
        tier = self._config.get("model_tiers", {}).get(tier_name)
        if not tier:
            return Result.fail("UNKNOWN_TIER", f"no tier config for: {tier_name}", {"tier": tier_name})

        prompt_version = payload.get("prompt_version", "v1.0")
        input_hash = _compute_input_hash(call_type, payload, product_id, segment_id, asset_id, tier_name, prompt_version, self.ctx)

        if self.ctx.settings.mock_llm:
            return self._execute_mock(call_type, payload, tier, tier_name, prompt_version, input_hash, product_id, segment_id, asset_id, output_id, task_id)

        cached = self._read_cache(input_hash)
        if cached is not None:
            self._write_log(
                call_id=new_id("LLM"), call_type=call_type, tier=tier, model_name=tier.get("model_name", "cached"),
                prompt_version=prompt_version, input_hash=input_hash, result_status="cache_hit",
                product_id=product_id, segment_id=segment_id, asset_id=asset_id,
                output_id=output_id, task_id=task_id, cache_hit=1, latency_ms=0,
            )
            return Result.ok({
                "route": {"model_tier": tier_name, "model_name": "cache", "provider": "cache"},
                "response": cached, "cache_hit": True,
                "meta": {"call_id": "CACHE", "call_type": call_type, "model_tier": tier_name, "retry_count": 0},
            })

        image_paths = _frame_paths(self.ctx, segment_id, max_count=9) if segment_id else payload.get("image_paths") or []

        audio_path = ""
        if self._is_audio_call(call_type):
            audio_path = payload.get("audio_path") or _find_audio_path(self.ctx, asset_id)

        provider_name = tier.get("primary_provider", "mock")
        fallback_name = tier.get("fallback_provider", "")
        escalation_tier = routing.get("escalate_tier") if routing.get("escalate_on_failure") else None

        route_policy = self._resolve_route_policy(call_type, tier_name)

        def exec_fn(tier_cfg: dict, tier_label: str, escalated_from: str = "") -> Tuple[Optional[Dict[str, Any]], str, int, int]:
            model = tier_cfg.get("model_name", "unknown")
            provider = self._get_provider(provider_name)
            if self._is_mock_provider(provider):
                return _mock_response(call_type, payload), "mock", 0, 0

            max_retries = int(tier_cfg.get("max_retries", 2))
            retry_delays = tier_cfg.get("retry_delays_ms", [5000, 20000])
            timeout = int(tier_cfg.get("timeout_ms", 120000))
            max_tokens = int(tier_cfg.get("max_output_tokens", 2000))

            with self._acquire_concurrency(tier_label):
                for attempt in range(max_retries + 1):
                    started = time.time()
                    try:
                        if self._is_audio_call(call_type) and audio_path:
                            resp = provider.call_audio(prompt=self._build_prompt(call_type, payload),
                                                         audio_path=audio_path, model=model,
                                                         max_output_tokens=max_tokens, timeout_ms=timeout)
                            if resp.parsed is None and resp.text:
                                from auto_mixcut.adapters.llm_provider import _parse_json_safe
                                resp = LLMResponse(text=resp.text, parsed=_parse_json_safe(resp.text), raw_response=resp.raw_response, usage=resp.usage, model=resp.model, provider_name=resp.provider_name, latency_ms=resp.latency_ms, retry_count=resp.retry_count)
                        elif self._is_vision_call(call_type):
                            resp = provider.call_json(prompt=self._build_prompt(call_type, payload),
                                                       image_paths=image_paths, model=model,
                                                       max_output_tokens=max_tokens, timeout_ms=timeout)
                        else:
                            resp = provider.call_text(prompt=self._build_prompt(call_type, payload),
                                                       image_paths=image_paths, model=model,
                                                       max_output_tokens=max_tokens, timeout_ms=timeout)
                        latency = int((time.time() - started) * 1000)
                        if resp.parsed is not None:
                            return resp.parsed, model, attempt, latency
                        if resp.text:
                            return {"text": resp.text}, model, attempt, latency
                        if attempt < max_retries:
                            time.sleep(retry_delays[min(attempt, len(retry_delays) - 1)] / 1000.0)
                            continue
                        return None, model, attempt, latency
                    except Exception as exc:
                        latency = int((time.time() - started) * 1000)
                        if not _is_retryable(exc) or attempt >= max_retries:
                            return None, model, attempt, latency
                        delay = retry_delays[min(attempt, len(retry_delays) - 1)] / 1000.0
                        time.sleep(delay)
            return None, model, max_retries, 0

        response, model_used, retry_count, latency_ms = exec_fn(tier, tier_name)

        escalation_count = 0
        escalated_from = ""
        if response is None and escalation_tier and escalation_tier != tier_name and (routing.get("escalate_on_failure") or force_escalation):
            escalated = self._config.get("model_tiers", {}).get(escalation_tier)
            if escalated:
                escalation_count = 1
                escalated_from = tier_name
                response, model_used, retry_count2, latency_ms2 = exec_fn(escalated, escalation_tier, escalated_from=tier_name)
                retry_count += retry_count2
                latency_ms += latency_ms2
                tier_name = escalation_tier
                model_used = escalated.get("model_name", model_used)

        if response is None:
            self._write_log(
                call_id=new_id("LLM"), call_type=call_type, tier=tier, model_name=model_used,
                prompt_version=prompt_version, input_hash=input_hash, result_status="failed",
                error_code="LLM_CALL_EXHAUSTED", error_message="all retries and escalations exhausted",
                product_id=product_id, segment_id=segment_id, asset_id=asset_id,
                output_id=output_id, task_id=task_id, retry_count=retry_count,
                escalation_count=escalation_count, escalated_from=escalated_from,
                image_count=len(image_paths), latency_ms=latency_ms,
                route_policy=route_policy, provider=provider_name,
            )
            return Result.fail("LLM_CALL_EXHAUSTED", "all retries and escalations exhausted", {
                "call_type": call_type, "tier": tier_name, "retry_count": retry_count,
                "escalation_count": escalation_count,
            })

        response = self._normalize_response(call_type, response, payload)
        self._write_cache(input_hash, call_type, response, payload, product_id, segment_id, asset_id, tier)
        self._write_log(
            call_id=new_id("LLM"), call_type=call_type, tier=tier,
            model_name=model_used, prompt_version=prompt_version, input_hash=input_hash,
            result_status="success", product_id=product_id, segment_id=segment_id,
            asset_id=asset_id, output_id=output_id, task_id=task_id,
            retry_count=retry_count, escalation_count=escalation_count,
            escalated_from=escalated_from, image_count=len(image_paths),
            latency_ms=latency_ms, route_policy=route_policy, provider=provider_name,
        )

        return Result.ok({
            "route": {"model_tier": tier_name, "model_name": model_used, "provider": provider_name, "route_policy": route_policy},
            "response": response,
            "cache_hit": False,
            "meta": {"call_id": "OK", "call_type": call_type, "model_tier": tier_name, "model_name": model_used, "prompt_version": prompt_version, "retry_count": retry_count, "latency_ms": latency_ms, "escalation_count": escalation_count},
        })

    def _get_provider(self, provider_name: str) -> LLMProvider:
        if provider_name not in self._providers:
            provider_config = self._config.get("providers", {}).get(provider_name)
            if provider_config:
                self._providers[provider_name] = LLMProvider.create(provider_config)
            else:
                self._providers[provider_name] = self._mock_provider
        return self._providers[provider_name]

    def _is_mock_provider(self, provider: LLMProvider) -> bool:
        return isinstance(provider, MockLLMProvider)

    def _is_audio_call(self, call_type: str) -> bool:
        return call_type in {
            "bgm_metadata_tagging",
        }

    def _is_vision_call(self, call_type: str) -> bool:
        return call_type in {
            "segment_tagging_default", "product_anchor_check", "product_anchor_generation",
            "watermark_detection", "ai_anchor_check", "ai_generated_consistency_check",
            "final_video_qc", "core_anchor_review", "core_consistency_review",
            "golden_benchmark_analysis", "risk_escalation",
        }

    def _resolve_route_policy(self, call_type: str, tier_name: str) -> str:
        tier_to_policy = {
            "cheap_text": "cheap_text",
            "medium_vision": "medium_vision",
            "high_vision": "high_vision",
        }
        primary = tier_to_policy.get(tier_name, "medium_vision")
        routing = self._config.get("call_type_routing", {}).get(call_type, {})
        if routing.get("escalate_on_failure"):
            return primary + "_with_fallback"
        return primary

    def _acquire_concurrency(self, tier_name: str):
        if not self._config.get("concurrency", {}).get("enabled", True):
            from contextlib import nullcontext
            return nullcontext()

        tier = self._config.get("model_tiers", {}).get(tier_name, {})
        max_conc = int(tier.get("max_concurrency", 2))
        wait_timeout = self._config.get("concurrency", {}).get("wait_timeout_ms", 300000) / 1000.0

        if tier_name not in self._concurrency_locks:
            self._concurrency_locks[tier_name] = threading.Semaphore(max_conc)

        return _TimedSemaphore(self._concurrency_locks[tier_name], wait_timeout)

    def _normalize_response(self, call_type: str, response: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
        from auto_mixcut.skills.llm_prompts import (
            normalize_segment_tag, normalize_consistency, normalize_anchor_check,
            normalize_prompt_refinement, normalize_product_anchor, normalize_bgm_tag,
        )
        if call_type == "segment_tagging_default":
            return normalize_segment_tag(response)
        if call_type == "ai_generated_consistency_check":
            return normalize_consistency(response)
        if call_type == "ai_anchor_check":
            return normalize_anchor_check(response)
        if call_type == "segment_prompt_refinement":
            return normalize_prompt_refinement(response)
        if call_type in ("product_anchor_check", "product_anchor_generation"):
            return normalize_product_anchor(
                response,
                payload.get("category", ""),
                payload.get("product_name", ""),
            )
        if call_type == "bgm_metadata_tagging":
            return normalize_bgm_tag(response)
        return response

    def _build_prompt(self, call_type: str, payload: Dict[str, Any]) -> str:
        from auto_mixcut.skills.llm_prompts import (
            consistency_prompt, watermark_prompt, ai_anchor_check_prompt,
            segment_tagging_prompt, segment_prompt_refinement_prompt,
            product_anchor_prompt, bgm_tagging_prompt,
        )
        if call_type == "segment_tagging_default":
            segment_id = payload.get("segment_id", "")
            product_id = payload.get("product_id", "")
            product = self.ctx.repo.get("products", "product_id", product_id) or {}
            asset = self.ctx.repo.get("assets", "asset_id", payload.get("asset_id", "")) or {}
            segment = self.ctx.repo.get("segments", "segment_id", segment_id) or {}
            return segment_tagging_prompt(product, asset, segment)
        if call_type == "ai_generated_consistency_check":
            return consistency_prompt()
        if call_type == "watermark_detection":
            return watermark_prompt()
        if call_type == "ai_anchor_check":
            segment_id = payload.get("segment_id", "")
            product_id = payload.get("product_id", "")
            product = self.ctx.repo.get("products", "product_id", product_id) or {}
            segment = self.ctx.repo.get("segments", "segment_id", segment_id) or {}
            return ai_anchor_check_prompt(product, segment)
        if call_type == "segment_prompt_refinement":
            return segment_prompt_refinement_prompt(
                str(payload.get("anchor_json") or "{}"),
                str(payload.get("segment_type") or ""),
                str(payload.get("segment_type_cn") or ""),
                str(payload.get("category") or ""),
            )
        if call_type in ("product_anchor_check", "product_anchor_generation"):
            return product_anchor_prompt(
                payload.get("product_id", ""),
                payload.get("product_name", ""),
                payload.get("category", ""),
                payload.get("market", ""),
            )
        if call_type == "bgm_metadata_tagging":
            return bgm_tagging_prompt(payload)
        return str(payload.get("prompt_text") or payload.get("prompt") or "{}")

    def _read_cache(self, input_hash: str) -> Optional[Dict[str, Any]]:
        if not self._config.get("cache", {}).get("enabled", True):
            return None
        row = self.ctx.repo.get("llm_cache", "cache_key", input_hash)
        if not row:
            return None
        data = row.get("response_json")
        if isinstance(data, dict):
            return data
        if isinstance(data, str):
            try:
                return json.loads(data)
            except json.JSONDecodeError:
                return None
        return None

    def _write_cache(self, input_hash: str, call_type: str, response: Dict[str, Any], payload: Dict[str, Any], product_id: str, segment_id: str, asset_id: str, tier: Dict[str, Any]) -> None:
        self.ctx.repo.upsert(
            "llm_cache",
            "cache_key",
            {
                "cache_key": input_hash,
                "call_type": call_type,
                "product_id": product_id,
                "asset_id": asset_id,
                "segment_id": segment_id,
                "model_tier": tier.get("tier", ""),
                "model_name": tier.get("model_name", ""),
                "prompt_version": payload.get("prompt_version", "v1.0"),
                "input_hash": input_hash,
                "response_json": response,
            },
        )

    def _write_log(
        self, *, call_id: str, call_type: str, tier: Dict[str, Any], model_name: str,
        prompt_version: str, input_hash: str, result_status: str,
        product_id: str = "", segment_id: str = "", asset_id: str = "",
        output_id: str = "", task_id: str = "",
        error_code: str = "", error_message: str = "",
        retry_count: int = 0, escalation_count: int = 0,
        escalated_from: str = "", cache_hit: int = 0,
        latency_ms: int = 0, image_count: int = 0,
        route_policy: str = "", provider: str = "",
    ) -> None:
        self.ctx.repo.insert("llm_call_logs", {
            "call_id": call_id,
            "call_type": call_type,
            "route_policy": route_policy,
            "product_id": product_id,
            "asset_id": asset_id,
            "segment_id": segment_id,
            "output_id": output_id,
            "task_id": task_id,
            "model_tier": tier.get("tier", ""),
            "model_name": model_name,
            "provider": provider or tier.get("primary_provider", ""),
            "fallback_provider": tier.get("fallback_provider", ""),
            "prompt_version": prompt_version,
            "input_hash": input_hash,
            "cache_hit": cache_hit,
            "result_status": result_status,
            "error_code": error_code,
            "error_message": error_message,
            "retry_count": retry_count,
            "escalation_count": escalation_count,
            "escalated_from": escalated_from,
            "image_count": image_count,
            "latency_ms": latency_ms,
        })
        self.ctx.repo.upsert("llm_calls", "call_id", {
            "call_id": call_id,
            "task_id": task_id,
            "product_id": product_id,
            "asset_id": asset_id,
            "segment_id": segment_id,
            "output_id": output_id,
            "call_type": call_type,
            "model_tier": tier.get("tier", ""),
            "model_name": model_name,
            "prompt_version": prompt_version,
            "input_hash": input_hash,
            "cache_hit": cache_hit,
            "token_input": 0,
            "token_output": 0,
            "image_count": image_count,
            "estimated_cost": 0,
            "latency_ms": latency_ms,
            "result_status": result_status,
        })

    def _execute_mock(
        self, call_type: str, payload: Dict[str, Any], tier: Dict[str, Any],
        tier_name: str, prompt_version: str, input_hash: str,
        product_id: str, segment_id: str, asset_id: str,
        output_id: str, task_id: str,
    ) -> Result:
        response = _mock_response(call_type, payload)
        self._write_log(
            call_id=new_id("LLM"), call_type=call_type, tier=tier,
            model_name="mock", prompt_version=prompt_version, input_hash=input_hash,
            result_status="mock_success", product_id=product_id, segment_id=segment_id,
            asset_id=asset_id, output_id=output_id, task_id=task_id,
            route_policy=self._resolve_route_policy(call_type, tier_name),
            provider="mock",
        )
        return Result.ok({
            "route": {"model_tier": tier_name, "model_name": "mock", "provider": "mock", "route_policy": "mock"},
            "response": response,
            "cache_hit": False,
            "meta": {"call_id": "MOCK", "call_type": call_type, "model_tier": tier_name, "retry_count": 0, "mock": True},
        })


class _TimedSemaphore:
    def __init__(self, sem: threading.Semaphore, timeout: float):
        self._sem = sem
        self._timeout = timeout

    def __enter__(self):
        if not self._sem.acquire(timeout=self._timeout):
            raise RuntimeError(f"concurrency semaphore acquisition timed out after {self._timeout}s")

    def __exit__(self, *args):
        self._sem.release()


def _load_config(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return _default_config()
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return _default_config()


def _default_config() -> Dict[str, Any]:
    return {
        "providers": {"mock": {"type": "mock", "display_name": "Mock"}},
        "model_tiers": {
            "cheap_text":     {"tier": "cheap_text",     "primary_provider": "mock", "model_name": "mock-cheap", "max_concurrency": 5, "max_retries": 0, "retry_delays_ms": [], "timeout_ms": 60000,  "max_output_tokens": 1000},
            "medium_vision":  {"tier": "medium_vision",  "primary_provider": "mock", "model_name": "mock-mid",   "max_concurrency": 2, "max_retries": 0, "retry_delays_ms": [], "timeout_ms": 120000, "max_output_tokens": 2000},
            "high_vision":    {"tier": "high_vision",    "primary_provider": "mock", "model_name": "mock-high",  "max_concurrency": 1, "max_retries": 0, "retry_delays_ms": [], "timeout_ms": 180000, "max_output_tokens": 2000},
        },
        "call_type_routing": {
            "failure_summary":               {"tier": "cheap_text",    "escalate_on_failure": False},
            "bgm_metadata_tagging":          {"tier": "cheap_text",    "escalate_on_failure": False},
            "segment_tagging_default":        {"tier": "medium_vision", "escalate_on_failure": False},
            "product_anchor_check":           {"tier": "medium_vision", "escalate_on_failure": True,  "escalate_tier": "high_vision"},
            "product_anchor_generation":      {"tier": "medium_vision", "escalate_on_failure": False},
            "watermark_detection":            {"tier": "medium_vision", "escalate_on_failure": False},
            "ai_anchor_check":                {"tier": "medium_vision", "escalate_on_failure": False},
            "ai_generated_consistency_check": {"tier": "medium_vision", "escalate_on_failure": False},
            "segment_prompt_refinement":      {"tier": "cheap_text",    "escalate_on_failure": False},
            "final_video_qc":                {"tier": "medium_vision", "escalate_on_failure": True,  "escalate_tier": "high_vision"},
            "core_anchor_review":             {"tier": "high_vision",   "escalate_on_failure": False},
            "core_consistency_review":        {"tier": "high_vision",   "escalate_on_failure": False},
            "golden_benchmark_analysis":      {"tier": "high_vision",   "escalate_on_failure": False},
            "risk_escalation":                {"tier": "high_vision",   "escalate_on_failure": False},
        },
        "cache": {"enabled": True},
        "concurrency": {"enabled": False},
        "escalation_rules": {"max_total_retries_per_call": 3},
    }


def _compute_input_hash(call_type: str, payload: Dict[str, Any], product_id: str, segment_id: str, asset_id: str, tier_name: str, prompt_version: str, ctx: SkillContext) -> str:
    material = {
        "call_type": call_type,
        "payload": payload,
        "product_id": product_id,
        "segment_id": segment_id,
        "asset_id": asset_id,
        "model_tier": tier_name,
        "prompt_version": prompt_version,
        "frame_hashes": _frame_hashes(ctx, segment_id),
        "image_path_hashes": _image_path_hashes(payload.get("image_paths") or []),
    }
    return hashlib.sha256(json.dumps(material, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")).hexdigest()


def _frame_hashes(ctx: SkillContext, segment_id: str) -> List[str]:
    if not segment_id:
        return []
    try:
        rows = ctx.repo.list_where("segment_frames", "segment_id=? ORDER BY id DESC", (segment_id,))
    except Exception:
        return []
    hashes = []
    for row in reversed(rows[:10]):
        obj = ctx.repo.get("oss_objects", "object_id", row.get("oss_object_id"))
        if obj:
            hashes.append(str(obj.get("file_hash") or obj.get("object_key") or ""))
    return hashes


def _image_path_hashes(paths: List[str]) -> List[str]:
    hashes = []
    for path in paths:
        try:
            with open(path, "rb") as fh:
                hashes.append(hashlib.sha256(fh.read()).hexdigest())
        except OSError:
            hashes.append(str(path))
    return hashes


def _find_audio_path(ctx: SkillContext, asset_id: str) -> str:
    if not asset_id:
        return ""
    path = require_oss_object_path(ctx, asset_id, "llm_audio")
    if path and path.exists():
        return str(path)
    return ""


def _frame_paths(ctx: SkillContext, segment_id: str, max_count: int) -> List[str]:
    rows = ctx.repo.list_where("segment_frames", "segment_id=? ORDER BY id DESC", (segment_id,))
    selected = list(reversed(rows[:max_count]))
    paths = []
    for row in selected:
        path = require_oss_object_path(ctx, row.get("oss_object_id"), "llm_frames")
        if path and path.exists() and path.stat().st_size > 1024:
            paths.append(str(path))
    return paths


def _is_retryable(exc: Exception) -> bool:
    exc_type = type(exc).__name__
    exc_str = str(exc)
    for token in RETRYABLE_ERRORS:
        if token in exc_type or token in exc_str:
            return True
    for code in RETRYABLE_STATUS_CODES:
        if code in exc_str:
            return True
    if "JSON" in exc_type or "json" in exc_str.lower():
        return True
    if not exc_str.strip():
        return True
    return False


def _mock_response(call_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if call_type == "segment_tagging_default":
        index = int(payload.get("index", 0))
        roles = ["hero", "detail", "result", "scene", "ending"]
        role = roles[index % len(roles)]
        secondary = {"hero": ["detail"], "detail": [], "result": [], "scene": ["ending"], "ending": []}[role]
        return {
            "primary_shot_role": role,
            "secondary_roles": secondary,
            "product_visibility": "high" if role in {"hero", "detail", "result"} else "medium",
            "hook_strength": "strong" if role in {"hero", "result"} else "medium",
            "mixcut_usability": "yes",
            "risk_level": "low",
            "confidence": "high",
            "needs_human_review": False,
            "reason": f"mock tag selected {role} from sampled frames",
        }
    if call_type == "ai_generated_consistency_check":
        return {"frame_consistency_score": 92, "frame_consistency_status": "pass", "frame_consistency_reason": "mock frames remain visually consistent"}
    if call_type == "watermark_detection":
        return {"has_watermark": "no", "confidence": "medium"}
    if call_type == "ai_anchor_check":
        segment_type = str(payload.get("segment_type") or "")
        if segment_type in {"detail_atmosphere", "tryon_result"}:
            return {
                "anchor_match_level": "soft_pass",
                "product_category_correct": True,
                "core_visual_points_status": {"product_shape": "likely", "key_features": "unclear"},
                "forbidden_mismatch_detected": False,
                "distortion_risk": "medium",
                "allowed_core_roles": [],
                "allowed_soft_roles": ["scene", "ending"],
                "needs_human_review": False,
                "reason": "mock ai anchor soft_pass",
            }
        return {
            "anchor_match_level": "strict_pass",
            "product_category_correct": True,
            "core_visual_points_status": {},
            "forbidden_mismatch_detected": False,
            "distortion_risk": "low",
            "allowed_core_roles": ["hero", "detail", "result"],
            "allowed_soft_roles": ["scene", "ending"],
            "needs_human_review": False,
            "reason": "mock ai anchor strict_pass",
        }
    if call_type == "segment_prompt_refinement":
        segment_type = str(payload.get("segment_type") or "home_lifestyle")
        segment_type_cn = str(payload.get("segment_type_cn") or "居家生活场景")
        return {
            "visual_description": f"mock visual: {segment_type_cn}",
            "key_anchor_points": ["mock anchor point 1", "mock anchor point 2"],
            "scene_description": f"mock scene for {segment_type}",
            "forbidden_items": ["字幕", "文字", "logo"],
        }
    if call_type in ("product_anchor_check", "product_anchor_generation"):
        product_id = payload.get("product_id", "MOCK")
        product_name = payload.get("product_name", "Mock Product")
        category = payload.get("category", "uncategorized")
        return {
            "category": category,
            "product_subtype": product_name,
            "core_visual_points": ["商品主体形状清楚", "主要材质和颜色可见"],
            "must_not_change_points": ["商品类型不能变", "核心外观特征必须可见"],
            "forbidden_mismatch": ["其他类目商品", "无关配饰"],
            "strict_roles": ["hero", "detail", "result"],
            "allowed_scene_usage": True,
            "drafted_by": "mock_tier2_vision_zh",
            "confidence": "high",
        }
    if call_type == "bgm_metadata_tagging":
        track_name = str(payload.get("track_name") or "Mock BGM")
        return {
            "ai_suggested_tags": {
                "mood_tags": ["daily_clean", "calm_lifestyle"],
                "energy_level": "medium",
                "vocal_type": "instrumental",
                "category_tags": ["generic_fashion"],
                "template_tags": [],
            },
            "mix_suggestions": {
                "recommended_start_sec": 12,
                "default_volume": 0.2,
                "fade_in_ms": 500,
                "fade_out_ms": 800,
                "suitable_for_intro": True,
                "loop_friendly": False,
                "voiceover_friendly": True,
            },
            "tag_confidence": "high",
            "tag_review_required": False,
            "tag_diff_json": {},
            "reason": f"mock bgm tag for {track_name}",
        }
    return {"ok": True}
