#!/usr/bin/env python3
"""Hermes 调用与 V2 JSON 校验。"""

from __future__ import annotations

import json
import base64
import math
import mimetypes
import os
import subprocess
import tempfile
from json import JSONDecoder
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import requests
try:
    from openai import OpenAI
except Exception:  # pragma: no cover - optional dependency guard
    OpenAI = None

from src.enums import (
    FEATURE_FIELDS_BY_CATEGORY,
    FEATURE_LEVEL_VALUES,
    HERMES_CONFIDENCE_VALUES,
    PREDICTED_CATEGORIES,
    RISK_TAG_VALUES_BY_CATEGORY,
    SUPPORTED_CATEGORIES,
)
from src.feishu import FeishuOpenClient
from src.models import (
    CandidateTask,
    CategoryIdentificationResult,
    FeatureAnalysisResult,
    HermesOutputError,
)


DEFAULT_HERMES_BIN = Path.home() / ".hermes" / "hermes-agent" / "venv" / "bin" / "hermes"
DEFAULT_HERMES_MODEL = os.environ.get("HERMES_PRODUCT_ANALYSIS_MODEL", "gpt-5.5").strip() or "gpt-5.5"
DEFAULT_HERMES_PROVIDER = os.environ.get("HERMES_PRODUCT_ANALYSIS_PROVIDER", "openai-codex").strip() or "openai-codex"
DEFAULT_LLM_BACKEND = (
    os.environ.get("HERMES_PRODUCT_ANALYSIS_LLM_BACKEND", "openclaw").strip().lower()
    or "openclaw"
)
DEFAULT_OPENCLAW_MODEL = (
    os.environ.get("HERMES_PRODUCT_ANALYSIS_OPENCLAW_MODEL", "openai-codex/gpt-5.5").strip()
    or "openai-codex/gpt-5.5"
)
DEFAULT_OPENCLAW_MODELS_CONFIG = (
    Path.home() / ".openclaw" / "agents" / "main" / "agent" / "models.json"
)
DEFAULT_OPENCLAW_BIN = os.environ.get("HERMES_PRODUCT_ANALYSIS_OPENCLAW_BIN", "openclaw").strip() or "openclaw"
OPENCLAW_AUTH_PROFILE_PATH = Path(
    os.environ.get(
        "HERMES_PRODUCT_ANALYSIS_OPENCLAW_AUTH_PROFILE_PATH",
        str(Path.home() / ".openclaw" / "agents" / "main" / "agent" / "auth-profiles.json"),
    )
)
CODEX_AUTH_PATH = Path(os.environ.get("HERMES_PRODUCT_ANALYSIS_CODEX_AUTH_PATH", str(Path.home() / ".codex" / "auth.json")))
HERMES_AUTH_PATH = Path(os.environ.get("HERMES_PRODUCT_ANALYSIS_HERMES_AUTH_PATH", str(Path.home() / ".hermes" / "auth.json")))
ALLOW_TEXT_ONLY_OPENCLAW = (
    os.environ.get("HERMES_PRODUCT_ANALYSIS_ALLOW_TEXT_ONLY_OPENCLAW", "").strip().lower()
    in {"1", "true", "yes"}
)


class HermesAnalyzer(object):
    def __init__(
        self,
        skill_dir: Path,
        hermes_bin: Optional[str] = None,
        model: Optional[str] = None,
        provider: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        command_runner=None,
        llm_backend: Optional[str] = None,
        openclaw_model: Optional[str] = None,
        openclaw_models_config: Optional[str] = None,
        openclaw_bin: Optional[str] = None,
        request_post=None,
    ):
        self.skill_dir = Path(skill_dir)
        self.prompts_dir = self.skill_dir / "prompts"
        self.hermes_bin = Path(
            hermes_bin or os.environ.get("HERMES_PRODUCT_ANALYSIS_HERMES_BIN", str(DEFAULT_HERMES_BIN))
        ).expanduser()
        self.model = (model or DEFAULT_HERMES_MODEL).strip()
        self.provider = (provider or DEFAULT_HERMES_PROVIDER).strip()
        self.timeout_seconds = int(
            timeout_seconds
            or os.environ.get("HERMES_PRODUCT_ANALYSIS_TIMEOUT_SECONDS", "180")
        )
        self.command_runner = command_runner or subprocess.run
        self.llm_backend = (llm_backend or DEFAULT_LLM_BACKEND).strip().lower()
        self.openclaw_model = (openclaw_model or DEFAULT_OPENCLAW_MODEL).strip()
        self.openclaw_models_config = Path(
            openclaw_models_config
            or os.environ.get("HERMES_PRODUCT_ANALYSIS_OPENCLAW_MODELS_CONFIG", str(DEFAULT_OPENCLAW_MODELS_CONFIG))
        ).expanduser()
        self.openclaw_bin = (openclaw_bin or DEFAULT_OPENCLAW_BIN).strip()
        self.request_post = request_post or requests.post
        self._feishu_open_client = None

    def identify_category(self, task: CandidateTask) -> CategoryIdentificationResult:
        if self._force_rule_fallback():
            return self._fallback_category_result(task)
        payload = {
            "product_title": task.product_title,
            "title_keyword_tags": task.title_keyword_tags,
            "title_category_hint": task.title_category_hint,
            "title_category_confidence": task.title_category_confidence,
            "product_notes": task.product_notes,
            "competitor_notes": task.competitor_notes,
            "target_market": task.target_market,
            "images": task.product_images,
        }
        response_payload = self._run_prompt(
            prompt_name="category_identification_prompt_v2.txt",
            payload=payload,
            product_images=task.product_images,
        )
        return self.validate_category_result(response_payload)

    def analyze_features(self, task: CandidateTask, final_category: str) -> FeatureAnalysisResult:
        if final_category not in SUPPORTED_CATEGORIES:
            raise HermesOutputError("不支持的分析类目: {category}".format(category=final_category))
        if self._force_rule_fallback():
            return self._fallback_feature_result(task, final_category, reason="LLM图片调用跳过")
        payload = {
            "product_title": task.product_title,
            "title_keyword_tags": task.title_keyword_tags,
            "cost_price": task.cost_price,
            "target_price": task.target_price,
            "product_notes": task.product_notes,
            "competitor_notes": task.competitor_notes,
            "target_market": task.target_market,
            "images": task.product_images,
        }
        prompt_name = (
            "hair_accessory_feature_prompt_v2.txt"
            if final_category == "发饰"
            else "light_tops_feature_prompt_v2.txt"
        )
        try:
            response_payload = self._run_prompt(
                prompt_name=prompt_name,
                payload=payload,
                product_images=task.product_images,
            )
        except subprocess.TimeoutExpired:
            if not self._allow_rule_fallback():
                raise
            return self._fallback_feature_result(task, final_category, reason="LLM图片调用超时")
        return self.validate_feature_result(response_payload, expected_category=final_category)

    def _force_rule_fallback(self) -> bool:
        return str(os.environ.get("HERMES_PRODUCT_ANALYSIS_FORCE_RULE_FALLBACK", "") or "").strip().lower() in {
            "1",
            "true",
            "yes",
        }

    def _allow_rule_fallback(self) -> bool:
        return str(os.environ.get("HERMES_PRODUCT_ANALYSIS_RULE_FALLBACK", "") or "").strip().lower() in {
            "1",
            "true",
            "yes",
        } or self._force_rule_fallback()

    def _fallback_category_result(self, task: CandidateTask) -> CategoryIdentificationResult:
        manual_category = str(getattr(task, "manual_category", "") or "").strip()
        if manual_category in SUPPORTED_CATEGORIES:
            return CategoryIdentificationResult(
                predicted_category=manual_category,
                confidence="medium",
                reason="人工类目兜底",
            )
        text = self._fallback_search_text(task)
        hair_keywords = ["发夹", "抓夹", "鲨鱼夹", "发箍", "发圈", "头饰", "盘发", "发饰"]
        top_keywords = ["上衣", "衬衫", "开衫", "罩衫", "针织", "防晒", "短上衣", "T恤"]
        if any(keyword in text for keyword in hair_keywords):
            return CategoryIdentificationResult("发饰", "medium", "标题规则兜底")
        if any(keyword in text for keyword in top_keywords):
            return CategoryIdentificationResult("轻上装", "medium", "标题规则兜底")
        return CategoryIdentificationResult("无法判断", "low", "规则证据不足")

    def _fallback_feature_result(self, task: CandidateTask, final_category: str, reason: str) -> FeatureAnalysisResult:
        text = self._fallback_search_text(task)
        has_image = bool(getattr(task, "product_images", []) or [])
        keyword_count = len(list(getattr(task, "title_keyword_tags", []) or []))
        if final_category == "发饰":
            return self._fallback_hair_feature_result(text, has_image, keyword_count, reason)
        return self._fallback_light_top_feature_result(text, has_image, keyword_count, reason)

    def _fallback_hair_feature_result(
        self,
        text: str,
        has_image: bool,
        keyword_count: int,
        reason: str,
    ) -> FeatureAnalysisResult:
        demo_terms = ["盘发", "懒人", "抓夹", "鲨鱼夹", "发箍", "压碎发", "固定", "整理"]
        visual_terms = ["蝴蝶结", "花", "珍珠", "金属", "亮", "大号", "复古", "高级感", "礼物", "套装"]
        generic_terms = ["爆款", "新款", "高级感", "头饰", "发饰", "百搭"]
        feature_scores = {
            "wearing_change_strength": "高" if any(term in text for term in demo_terms) else "中",
            "demo_ease": "高" if any(term in text for term in demo_terms) else "中",
            "visual_memory_point": "高" if any(term in text for term in visual_terms) else "中",
            "homogenization_risk": "高" if sum(1 for term in generic_terms if term in text) >= 2 else "中",
            "title_selling_clarity": "高" if keyword_count >= 3 else "中",
            "info_completeness": "中" if has_image and text else "低",
        }
        return FeatureAnalysisResult(
            analysis_category="发饰",
            feature_scores=feature_scores,
            risk_tag="图片信息不足",
            risk_note="视觉未LLM复核",
            brief_observation="{reason}，按标题规则兜底".format(reason=reason)[:50],
        )

    def _fallback_light_top_feature_result(
        self,
        text: str,
        has_image: bool,
        keyword_count: int,
        reason: str,
    ) -> FeatureAnalysisResult:
        scene_terms = ["防晒", "空调", "通勤", "轻薄", "显比例", "遮手臂", "开衫", "外搭"]
        design_terms = ["短款", "微宽松", "针织", "衬衫", "罩衫", "轻熟", "韩系"]
        feature_scores = {
            "upper_body_change_strength": "高" if any(term in text for term in scene_terms) else "中",
            "camera_readability": "中",
            "design_signal_strength": "高" if any(term in text for term in design_terms) else "中",
            "basic_style_escape_strength": "中",
            "title_selling_clarity": "高" if keyword_count >= 3 else "中",
            "info_completeness": "中" if has_image and text else "低",
        }
        return FeatureAnalysisResult(
            analysis_category="轻上装",
            feature_scores=feature_scores,
            risk_tag="图片信息不足",
            risk_note="视觉未LLM复核",
            brief_observation="{reason}，按标题规则兜底".format(reason=reason)[:50],
        )

    def _fallback_search_text(self, task: CandidateTask) -> str:
        parts = [
            str(getattr(task, "product_title", "") or ""),
            str(getattr(task, "product_notes", "") or ""),
            str(getattr(task, "competitor_notes", "") or ""),
            " ".join(str(item or "") for item in getattr(task, "title_keyword_tags", []) or []),
        ]
        return " ".join(part for part in parts if part).strip()

    def validate_category_result(self, payload: Dict[str, Any]) -> CategoryIdentificationResult:
        predicted_category = self._require_string(payload, "predicted_category")
        confidence = self._require_string(payload, "confidence")
        reason = self._require_string(payload, "reason")

        if predicted_category not in PREDICTED_CATEGORIES:
            raise HermesOutputError("predicted_category 不合法: {value}".format(value=predicted_category))
        if confidence not in HERMES_CONFIDENCE_VALUES:
            raise HermesOutputError("confidence 不合法: {value}".format(value=confidence))
        if len(reason) > 40:
            raise HermesOutputError("reason 长度超过 40")
        return CategoryIdentificationResult(
            predicted_category=predicted_category,
            confidence=confidence,
            reason=reason,
        )

    def validate_feature_result(self, payload: Dict[str, Any], expected_category: Optional[str] = None) -> FeatureAnalysisResult:
        analysis_category = self._require_string(payload, "analysis_category")
        if analysis_category not in SUPPORTED_CATEGORIES:
            raise HermesOutputError("analysis_category 不合法: {value}".format(value=analysis_category))
        if expected_category and analysis_category != expected_category:
            raise HermesOutputError(
                "analysis_category 与目标类目不一致: expected={expected} actual={actual}".format(
                    expected=expected_category,
                    actual=analysis_category,
                )
            )

        feature_scores = {}
        for field_name in FEATURE_FIELDS_BY_CATEGORY[analysis_category]:
            field_value = self._require_string(payload, field_name)
            if field_value not in FEATURE_LEVEL_VALUES:
                raise HermesOutputError("{field} 不合法: {value}".format(field=field_name, value=field_value))
            feature_scores[field_name] = field_value

        risk_tag = self._require_string(payload, "risk_tag")
        risk_note = self._require_string(payload, "risk_note")
        brief_observation = self._require_string(payload, "brief_observation")

        if risk_tag not in RISK_TAG_VALUES_BY_CATEGORY[analysis_category]:
            raise HermesOutputError("risk_tag 不合法: {value}".format(value=risk_tag))
        if len(risk_note) > 30:
            raise HermesOutputError("risk_note 长度超过 30")
        if len(brief_observation) > 50:
            raise HermesOutputError("brief_observation 长度超过 50")

        return FeatureAnalysisResult(
            analysis_category=analysis_category,
            feature_scores=feature_scores,
            risk_tag=risk_tag,
            risk_note=risk_note,
            brief_observation=brief_observation,
        )

    def _run_prompt(self, prompt_name: str, payload: Dict[str, Any], product_images: List[str]) -> Dict[str, Any]:
        prompt_text = (self.prompts_dir / prompt_name).read_text(encoding="utf-8").strip()
        image_path, image_count = self._materialize_analysis_image(product_images)
        image_note = ""
        if image_count > 1:
            image_note = (
                "附图为 product_images 中按原顺序拼接的多图拼板。"
                "请结合各分图里的细节图、佩戴图、对比图一起判断，不要只基于第一张图。"
            )
        query = (
            "{prompt}\n\n"
            "{image_note}\n"
            "输入数据如下，请严格按上述 JSON 返回，不要输出额外说明：\n"
            "{payload}"
        ).format(
            prompt=prompt_text,
            image_note=image_note,
            payload=json.dumps(payload, ensure_ascii=False, indent=2),
        )
        if self.llm_backend == "openclaw":
            return self._run_prompt_with_openclaw(query=query, image_path=image_path)
        if self.llm_backend not in {"hermes", "hermes_cli"}:
            raise HermesOutputError("未知 LLM backend: {backend}".format(backend=self.llm_backend))
        return self._run_prompt_with_hermes_cli(query=query, image_path=image_path)

    def _run_prompt_with_hermes_cli(self, query: str, image_path: Path) -> Dict[str, Any]:
        if not self.hermes_bin.exists():
            raise HermesOutputError("Hermes binary not found: {path}".format(path=self.hermes_bin))
        command = [
            str(self.hermes_bin),
            "chat",
            "-Q",
            "--source",
            "tool",
            "-m",
            self.model,
            "--provider",
            self.provider,
            "-q",
            query,
            "--image",
            str(image_path),
        ]
        completed = self.command_runner(
            command,
            capture_output=True,
            text=True,
            timeout=self.timeout_seconds,
            check=False,
            env=os.environ.copy(),
        )
        if getattr(completed, "returncode", 1) != 0:
            stderr = getattr(completed, "stderr", "") or getattr(completed, "stdout", "")
            raise HermesOutputError("Hermes 调用失败: {error}".format(error=stderr.strip()))
        response_text = self._extract_response_text(getattr(completed, "stdout", ""))
        if not response_text:
            raise HermesOutputError("Hermes 返回为空")
        try:
            response_payload = json.loads(response_text)
        except ValueError as exc:
            raise HermesOutputError("Hermes 返回非 JSON: {error}".format(error=exc)) from exc
        if not isinstance(response_payload, dict):
            raise HermesOutputError("Hermes 返回必须是 JSON object")
        return response_payload

    def _run_prompt_with_openclaw(self, query: str, image_path: Path) -> Dict[str, Any]:
        provider_id, model_id = self._split_openclaw_model(self.openclaw_model)
        provider = self._load_openclaw_provider(provider_id)
        api_type = str(provider.get("api") or "").strip()
        if api_type == "google-generative-ai":
            response_text = self._call_openclaw_google_provider(provider, model_id, query, image_path)
        elif api_type == "openai-completions":
            response_text = self._call_openclaw_openai_completions(provider, model_id, query, image_path)
        elif api_type == "openai-responses":
            response_text = self._call_openclaw_openai_responses(provider, model_id, query, image_path)
        elif api_type == "openai-codex-responses":
            response_text = self._call_openclaw_codex_responses_direct(provider, model_id, query, image_path)
        else:
            raise HermesOutputError(
                "OpenClaw provider 暂不支持直接图片 JSON 调用: {provider}/{model} api={api}".format(
                    provider=provider_id,
                    model=model_id,
                    api=api_type or "unknown",
                )
            )
        response_text = self._extract_response_text(response_text)
        if not response_text:
            raise HermesOutputError("OpenClaw 返回为空")
        try:
            response_payload = json.loads(response_text)
        except ValueError as exc:
            raise HermesOutputError("OpenClaw 返回非 JSON: {error}".format(error=exc)) from exc
        if not isinstance(response_payload, dict):
            raise HermesOutputError("OpenClaw 返回必须是 JSON object")
        return response_payload

    def _call_openclaw_gateway_model_run(
        self,
        provider_id: str,
        model_id: str,
        query: str,
        image_path: Path,
    ) -> str:
        if not ALLOW_TEXT_ONLY_OPENCLAW:
            raise HermesOutputError(
                "OpenClaw Gateway gpt-5.5 当前只验通文本输入，未验通产品图片输入；"
                "为避免误判，图片关键分析已阻断。"
                "如需临时按标题/URL文本分析，请显式设置 "
                "HERMES_PRODUCT_ANALYSIS_ALLOW_TEXT_ONLY_OPENCLAW=1。"
            )
        gateway_prompt = (
            "{query}\n\n"
            "【注意】当前 OpenClaw Gateway 只按文本运行，未接入真实图片输入。"
            "请只依据商品标题、备注和输入数据里的图片URL文本判断，并保持 JSON 输出。"
        ).format(query=query, image_path=str(image_path))
        command = [
            self.openclaw_bin,
            "infer",
            "model",
            "run",
            "--gateway",
            "--json",
            "--model",
            "{provider}/{model}".format(provider=provider_id, model=model_id),
            "--prompt",
            gateway_prompt,
        ]
        completed = self.command_runner(
            command,
            capture_output=True,
            text=True,
            timeout=self.timeout_seconds,
            check=False,
            env=os.environ.copy(),
        )
        if getattr(completed, "returncode", 1) != 0:
            stderr = getattr(completed, "stderr", "") or getattr(completed, "stdout", "")
            raise HermesOutputError("OpenClaw Gateway 调用失败: {error}".format(error=stderr.strip()))
        try:
            payload = json.loads(getattr(completed, "stdout", "") or "{}")
        except ValueError as exc:
            raise HermesOutputError("OpenClaw Gateway 返回非 JSON: {error}".format(error=exc)) from exc
        outputs = payload.get("outputs") or []
        if not outputs:
            raise HermesOutputError("OpenClaw Gateway 返回为空")
        return str(outputs[0].get("text") or "").strip()

    def _call_openclaw_codex_responses_direct(
        self,
        provider: Dict[str, Any],
        model_id: str,
        query: str,
        image_path: Path,
    ) -> str:
        """Use the same true multimodal Responses path as original-script-generator.

        `openclaw infer model run --gateway` is text-only. For openai-codex models we
        must call the Codex Responses endpoint directly and attach the image as a
        data-url `input_image`, otherwise product image analysis silently degrades.
        """
        if OpenAI is None:
            raise HermesOutputError("缺少 openai Python SDK，无法调用 openai-codex 图片链路")
        api_key = str(provider.get("apiKey") or "").strip() or self._extract_codex_access_token()
        if not api_key:
            raise HermesOutputError("未找到 openai-codex access token，请先登录 OpenClaw / Codex / Hermes")
        base_url = self._normalize_codex_base_url(str(provider.get("baseUrl") or ""))
        image_url, _ = self._image_as_data_url(image_path)
        client = OpenAI(
            api_key=api_key,
            base_url=base_url,
            timeout=self.timeout_seconds,
            max_retries=0,
        )
        text_chunks: List[str] = []
        fallback_text = ""
        try:
            with client.responses.stream(
                model=model_id,
                reasoning={"effort": os.environ.get("HERMES_PRODUCT_ANALYSIS_REASONING_EFFORT", "high")},
                instructions=(
                    "You are a multimodal product-analysis worker. "
                    "Follow the user prompt exactly. "
                    "If the prompt asks for JSON, output only valid JSON with no extra prose."
                ),
                store=False,
                input=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "input_text", "text": query},
                            {"type": "input_image", "image_url": image_url},
                        ],
                    }
                ],
            ) as stream:
                for event in stream:
                    event_type = str(getattr(event, "type", "") or "")
                    if event_type == "response.output_text.delta":
                        delta = str(getattr(event, "delta", "") or "")
                        if delta:
                            text_chunks.append(delta)
                    elif event_type == "response.output_text.done":
                        done_text = str(getattr(event, "text", "") or "")
                        if done_text:
                            fallback_text = done_text
                response = stream.get_final_response()
        except Exception as exc:
            raise HermesOutputError("OpenClaw openai-codex 图片调用失败: {error}".format(error=exc)) from exc
        text = "".join(text_chunks).strip() or fallback_text.strip()
        if not text:
            text = str(getattr(response, "output_text", "") or "").strip()
        if not text:
            dumped = response.model_dump(mode="json") if hasattr(response, "model_dump") else {}
            text = self._extract_openai_responses_text(dumped)
        return text

    def _split_openclaw_model(self, model_ref: str) -> tuple[str, str]:
        text = str(model_ref or "").strip()
        if "/" not in text:
            raise HermesOutputError(
                "OpenClaw 模型需使用 provider/model 格式: {model}".format(model=text or "<empty>")
            )
        provider_id, model_id = text.split("/", 1)
        provider_id = provider_id.strip()
        model_id = model_id.strip()
        if not provider_id or not model_id:
            raise HermesOutputError("OpenClaw 模型格式不完整: {model}".format(model=text))
        return provider_id, model_id

    def _load_openclaw_provider(self, provider_id: str) -> Dict[str, Any]:
        config_path = self.openclaw_models_config
        if not config_path.exists():
            raise HermesOutputError("OpenClaw models config not found: {path}".format(path=config_path))
        try:
            config = json.loads(config_path.read_text(encoding="utf-8"))
        except ValueError as exc:
            raise HermesOutputError("OpenClaw models config 非 JSON: {error}".format(error=exc)) from exc
        providers = config.get("providers") or {}
        provider = providers.get(provider_id)
        if not isinstance(provider, dict):
            raise HermesOutputError("OpenClaw provider 不存在: {provider}".format(provider=provider_id))
        return provider

    def _extract_codex_access_token(self) -> str:
        token = self._extract_openclaw_agent_access_token()
        if token:
            return token
        token = self._extract_codex_cli_access_token()
        if token:
            return token
        return self._extract_hermes_codex_access_token()

    @staticmethod
    def _safe_read_json(path: Path) -> Dict[str, Any]:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _extract_openclaw_agent_access_token(self) -> str:
        payload = self._safe_read_json(OPENCLAW_AUTH_PROFILE_PATH)
        profiles = payload.get("profiles") if isinstance(payload, dict) else {}
        profile = profiles.get("openai-codex:default") if isinstance(profiles, dict) else {}
        if isinstance(profile, dict):
            access_token = str(profile.get("access") or "").strip()
            if access_token:
                return access_token
        return ""

    def _extract_codex_cli_access_token(self) -> str:
        payload = self._safe_read_json(CODEX_AUTH_PATH)
        tokens = payload.get("tokens") if isinstance(payload, dict) else {}
        if isinstance(tokens, dict):
            access_token = str(tokens.get("access_token") or "").strip()
            if access_token:
                return access_token
        return ""

    def _extract_hermes_codex_access_token(self) -> str:
        payload = self._safe_read_json(HERMES_AUTH_PATH)
        providers = payload.get("providers") if isinstance(payload, dict) else {}
        provider = providers.get("openai-codex") if isinstance(providers, dict) else {}
        if isinstance(provider, dict):
            tokens = provider.get("tokens")
            if isinstance(tokens, dict):
                access_token = str(tokens.get("access_token") or "").strip()
                if access_token:
                    return access_token
        credential_pool = payload.get("credential_pool") if isinstance(payload, dict) else {}
        pool = credential_pool.get("openai-codex") if isinstance(credential_pool, dict) else []
        if isinstance(pool, list):
            for item in pool:
                if not isinstance(item, dict):
                    continue
                access_token = str(item.get("access_token") or "").strip()
                if access_token:
                    return access_token
        return ""

    @staticmethod
    def _normalize_codex_base_url(base_url: str) -> str:
        normalized = (base_url or "https://chatgpt.com/backend-api/codex").strip().rstrip("/")
        if normalized.endswith("/backend-api"):
            return normalized + "/codex"
        return normalized

    @staticmethod
    def _extract_openai_responses_text(result: Dict[str, Any]) -> str:
        output = result.get("output")
        if not isinstance(output, list):
            return ""
        text_parts: List[str] = []
        for item in output:
            if not isinstance(item, dict):
                continue
            content = item.get("content")
            if not isinstance(content, list):
                continue
            for block in content:
                if isinstance(block, dict) and block.get("type") in {"output_text", "text"}:
                    text = str(block.get("text") or "").strip()
                    if text:
                        text_parts.append(text)
        return "\n".join(text_parts).strip()

    def _image_as_data_url(self, image_path: Path) -> tuple[str, str]:
        mime_type = mimetypes.guess_type(str(image_path))[0] or "image/jpeg"
        encoded = base64.b64encode(image_path.read_bytes()).decode("ascii")
        return "data:{mime};base64,{data}".format(mime=mime_type, data=encoded), mime_type

    def _call_openclaw_openai_completions(
        self,
        provider: Dict[str, Any],
        model_id: str,
        query: str,
        image_path: Path,
    ) -> str:
        base_url = str(provider.get("baseUrl") or "").rstrip("/")
        api_key = str(provider.get("apiKey") or "").strip()
        if not base_url or not api_key:
            raise HermesOutputError("OpenClaw provider 缺少 baseUrl 或 apiKey")
        image_url, _ = self._image_as_data_url(image_path)
        headers = {
            "Authorization": "Bearer {api_key}".format(api_key=api_key),
            "Content-Type": "application/json",
        }
        headers.update(provider.get("headers") or {})
        response = self.request_post(
            "{base}/chat/completions".format(base=base_url),
            headers=headers,
            json={
                "model": model_id,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": query},
                            {"type": "image_url", "image_url": {"url": image_url}},
                        ],
                    }
                ],
                "temperature": 0.2,
                "max_tokens": 2000,
            },
            timeout=self.timeout_seconds,
        )
        if not response.ok:
            raise HermesOutputError("OpenClaw provider 调用失败: {error}".format(error=response.text[:500]))
        data = response.json()
        return str((data.get("choices") or [{}])[0].get("message", {}).get("content") or "").strip()

    def _call_openclaw_openai_responses(
        self,
        provider: Dict[str, Any],
        model_id: str,
        query: str,
        image_path: Path,
    ) -> str:
        base_url = str(provider.get("baseUrl") or "").rstrip("/")
        api_key = str(provider.get("apiKey") or "").strip()
        if not base_url or not api_key:
            raise HermesOutputError("OpenClaw provider 缺少 baseUrl 或 apiKey")
        image_url, _ = self._image_as_data_url(image_path)
        response = self.request_post(
            "{base}/responses".format(base=base_url),
            headers={
                "Authorization": "Bearer {api_key}".format(api_key=api_key),
                "Content-Type": "application/json",
            },
            json={
                "model": model_id,
                "input": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "input_text", "text": query},
                            {"type": "input_image", "image_url": image_url},
                        ],
                    }
                ],
                "temperature": 0.2,
                "max_output_tokens": 2000,
            },
            timeout=self.timeout_seconds,
        )
        if not response.ok:
            raise HermesOutputError("OpenClaw provider 调用失败: {error}".format(error=response.text[:500]))
        data = response.json()
        if data.get("output_text"):
            return str(data.get("output_text") or "").strip()
        parts = []
        for item in data.get("output") or []:
            for content in item.get("content") or []:
                if content.get("type") in {"output_text", "text"}:
                    parts.append(str(content.get("text") or ""))
        return "\n".join(part for part in parts if part).strip()

    def _call_openclaw_google_provider(
        self,
        provider: Dict[str, Any],
        model_id: str,
        query: str,
        image_path: Path,
    ) -> str:
        base_url = str(provider.get("baseUrl") or "").rstrip("/")
        api_key = str(provider.get("apiKey") or "").strip()
        if not base_url or not api_key:
            raise HermesOutputError("OpenClaw provider 缺少 baseUrl 或 apiKey")
        data_url, mime_type = self._image_as_data_url(image_path)
        encoded_image = data_url.split(",", 1)[1]
        response = self.request_post(
            "{base}/models/{model}:generateContent?key={key}".format(
                base=base_url,
                model=model_id,
                key=api_key,
            ),
            json={
                "contents": [
                    {
                        "role": "user",
                        "parts": [
                            {"text": query},
                            {"inline_data": {"mime_type": mime_type, "data": encoded_image}},
                        ],
                    }
                ],
                "generationConfig": {"temperature": 0.2, "maxOutputTokens": 2000},
            },
            timeout=self.timeout_seconds,
        )
        if not response.ok:
            raise HermesOutputError("OpenClaw provider 调用失败: {error}".format(error=response.text[:500]))
        data = response.json()
        parts = []
        for candidate in data.get("candidates") or []:
            for part in (candidate.get("content") or {}).get("parts") or []:
                if part.get("text"):
                    parts.append(str(part.get("text") or ""))
        return "\n".join(parts).strip()

    def _materialize_analysis_image(self, product_images: List[str]) -> tuple[Path, int]:
        if not product_images:
            raise HermesOutputError("缺少产品图片")
        prepared_paths = []
        seen_paths = set()
        for image_ref in product_images:
            path = self._try_materialize_single_image(image_ref)
            if not path:
                continue
            normalized_path = str(path.resolve())
            if normalized_path in seen_paths:
                continue
            seen_paths.add(normalized_path)
            prepared_paths.append(path)
            if len(prepared_paths) >= 4:
                break
        if not prepared_paths:
            raise HermesOutputError("图片字段存在，但未能准备 Hermes 可读图片")
        if len(prepared_paths) == 1:
            return prepared_paths[0], 1
        return self._build_contact_sheet(prepared_paths), len(prepared_paths)

    def _try_materialize_single_image(self, image_ref: str) -> Optional[Path]:
        reference = str(image_ref or "").strip()
        if not reference:
            return None
        lazy_resolved = self._resolve_lazy_feishu_image_reference(reference)
        if lazy_resolved:
            reference = lazy_resolved
        candidate_path = Path(reference).expanduser()
        if candidate_path.exists() and candidate_path.is_file():
            return candidate_path
        if reference.startswith("http://") or reference.startswith("https://"):
            try:
                response = requests.get(reference, timeout=60)
                response.raise_for_status()
            except requests.RequestException:
                return None

            suffix = Path(reference.split("?", 1)[0]).suffix
            if not suffix:
                content_type = response.headers.get("content-type", "")
                suffix = mimetypes.guess_extension(content_type.split(";", 1)[0].strip()) or ".img"
            temp_dir = Path(tempfile.mkdtemp(prefix="hermes_product_analysis_"))
            file_path = temp_dir / ("primary_image{suffix}".format(suffix=suffix))
            file_path.write_bytes(response.content)
            return file_path
        return None

    def _resolve_lazy_feishu_image_reference(self, image_ref: str) -> str:
        reference = str(image_ref or "").strip()
        if not reference:
            return ""
        file_token = ""
        if reference.startswith("feishu-file-token:"):
            file_token = reference.split(":", 1)[1].strip()
        elif "batch_get_tmp_download_url" in reference:
            parsed = urlparse(reference)
            file_token = str((parse_qs(parsed.query).get("file_tokens") or [""])[0] or "").strip()
        elif "/open-apis/drive/v1/medias/" in reference and reference.rstrip("/").endswith("/download"):
            parts = [part for part in urlparse(reference).path.split("/") if part]
            if len(parts) >= 2:
                try:
                    media_index = parts.index("medias")
                    file_token = str(parts[media_index + 1] or "").strip()
                except Exception:
                    file_token = ""
        if not file_token:
            return reference
        try:
            return str(self._get_feishu_open_client().get_tmp_download_url(file_token) or "").strip() or reference
        except Exception:
            return reference

    def _get_feishu_open_client(self) -> FeishuOpenClient:
        if self._feishu_open_client is None:
            self._feishu_open_client = FeishuOpenClient()
        return self._feishu_open_client

    def _build_contact_sheet(self, image_paths: List[Path]) -> Path:
        try:
            from PIL import Image, ImageDraw, ImageFont
        except Exception:
            return image_paths[0]

        cell_size = 768
        cell_padding = 24
        gutter = 20
        columns = 2 if len(image_paths) > 1 else 1
        rows = int(math.ceil(float(len(image_paths)) / float(columns)))
        canvas_width = columns * cell_size + (columns + 1) * gutter
        canvas_height = rows * cell_size + (rows + 1) * gutter
        canvas = Image.new("RGB", (canvas_width, canvas_height), color=(255, 255, 255))
        draw = ImageDraw.Draw(canvas)
        font = ImageFont.load_default()

        opened_images = []
        try:
            for index, image_path in enumerate(image_paths):
                image = Image.open(image_path)
                opened_images.append(image)
                panel = image.convert("RGB")
                panel.thumbnail((cell_size - cell_padding * 2, cell_size - cell_padding * 2))
                row = index // columns
                column = index % columns
                x0 = gutter + column * (cell_size + gutter)
                y0 = gutter + row * (cell_size + gutter)
                x1 = x0 + cell_size
                y1 = y0 + cell_size
                draw.rounded_rectangle((x0, y0, x1, y1), radius=18, fill=(248, 248, 248), outline=(225, 225, 225), width=2)
                panel_x = x0 + (cell_size - panel.width) // 2
                panel_y = y0 + (cell_size - panel.height) // 2
                canvas.paste(panel, (panel_x, panel_y))
                label = str(index + 1)
                draw.rounded_rectangle((x0 + 14, y0 + 14, x0 + 56, y0 + 52), radius=10, fill=(0, 0, 0))
                draw.text((x0 + 28, y0 + 22), label, fill=(255, 255, 255), font=font)
            temp_dir = Path(tempfile.mkdtemp(prefix="hermes_product_analysis_sheet_"))
            file_path = temp_dir / "analysis_contact_sheet.jpg"
            canvas.save(file_path, format="JPEG", quality=92)
            return file_path
        finally:
            for image in opened_images:
                image.close()
            canvas.close()

    def _extract_response_text(self, stdout: str) -> str:
        text = (stdout or "").strip()
        if not text:
            return ""
        if "\nsession_id:" in text:
            text = text.rsplit("\nsession_id:", 1)[0].strip()
        decoder = JSONDecoder()
        for index, char in enumerate(text):
            if char not in "{[":
                continue
            try:
                _, end = decoder.raw_decode(text[index:])
            except ValueError:
                continue
            return text[index : index + end].strip()
        return text

    def _require_string(self, payload: Dict[str, Any], key: str) -> str:
        value = payload.get(key)
        if not isinstance(value, str):
            raise HermesOutputError("{key} 必须是字符串".format(key=key))
        text = value.strip()
        if not text:
            raise HermesOutputError("{key} 不能为空".format(key=key))
        return text
