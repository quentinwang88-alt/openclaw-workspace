"""Semantic reference understanding and alignment QA via the shared Doubao route."""
from __future__ import annotations

import hashlib
import base64
import json
import mimetypes
import os
import re
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

from services.photo_flow_registry import (
    DEFAULT_TRAVEL_SOURCE_ROLES, PhotoFlowRegistryError,
    resolve_required_roles, validate_ordered_roles,
)
from services.photo_category_registry import apply_target_product_to_look

from PIL import Image, ImageOps
import requests


PROMPT_VERSION = "opv-photo-reference-v2"
TRAVEL_PROMPT_VERSION = "opv-photo-travel-plan-v9-thermal-sensitivity"
PRESENTATIONS = {"FLAT_LAY", "MODEL_FULL_BODY", "SCENE_MODEL", "EDITORIAL_COLLAGE"}
ROLES = list(DEFAULT_TRAVEL_SOURCE_ROLES)  # compatibility alias for travel-only helpers
REFERENCE_USES = {"OUTFIT", "ENVIRONMENT", "VISUAL_STYLE", "LAYER_PROGRESSION"}

TRAVEL_WEATHER_TEMP_PATTERN = re.compile(r"\d+\s*(?:°\s*C|℃|摄氏度|度)")
from services.photo_travel_qa import FOOTWEAR_TYPES
from services.locale_quality import copy_locale_issues

# "靴"是唯一靴类字根（LOW_BOOT 是枚举中唯一靴型），可覆盖切尔西靴/马丁靴/
# 系带靴/雪地靴等所有变体；其余按品类词映射。
FOOTWEAR_KEYWORD_RULES = (
    ("细跟", "STILETTO"), ("高跟", "HIGH_HEEL"), ("运动鞋", "SNEAKER"),
    ("乐福", "LOAFER"), ("平底", "FLAT"), ("靴", "LOW_BOOT"),
    ("玛丽珍", "MARY_JANE"), ("凉鞋", "SANDAL"), ("低跟", "LOW_HEEL"),
)



def _footwear_text_types(shoes_text: str) -> set:
    detected = set()
    for keyword, footwear_type in FOOTWEAR_KEYWORD_RULES:
        if keyword in str(shoes_text or ""):
            detected.add(footwear_type)
    return detected


def _moment_footwear_rules(travel_contract: Mapping[str, Any]) -> dict:
    rules = {}
    for item in travel_contract.get("moments") or []:
        rules[str(item.get("key") or "")] = {
            "allowed": [str(value) for value in item.get("allowed_footwear_types") or []],
            "forbidden": [str(value) for value in item.get("forbidden_footwear_types") or []],
        }
    return rules


def _background_feature_tokens(features: Sequence[Any]) -> set:
    """Loose tokens for zero-overlap detection: full phrase or leading
    two characters of the modifier ("积雪的街道" -> {"积雪的街道", "积雪"})."""
    tokens = set()
    for feature in features or []:
        text = str(feature or "").strip()
        if text:
            tokens.add(text)
            tokens.add(text.split("的")[0][:2])
    return tokens


def _normalize_reference_uses(raw: Any) -> list[str]:
    """Keep per-image uses composable; old role labels remain readable."""
    values = raw if isinstance(raw, list) else [raw] if raw else []
    aliases = {
        "OUTFIT_REFERENCE": "OUTFIT", "GARMENT": "OUTFIT",
        "MOOD_REFERENCE": "VISUAL_STYLE", "STYLE": "VISUAL_STYLE",
        "SCENE_REFERENCE": "ENVIRONMENT", "BACKGROUND": "ENVIRONMENT",
    }
    normalized = []
    for value in values:
        key = aliases.get(str(value or "").strip().upper(),
                          str(value or "").strip().upper())
        if key in REFERENCE_USES and key not in normalized:
            normalized.append(key)
    return normalized


def _classified_reference_summary(per_reference: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Build small, purpose-separated summaries without a second model call."""
    outfit, environment, visual, layering, normalized_items = [], [], [], [], []
    for position, raw in enumerate(per_reference, 1):
        item = dict(raw or {})
        index = int(item.get("index") or position)
        uses = _normalize_reference_uses(
            item.get("reference_uses") or item.get("roles")
        )
        # Historical model responses had no typed use. Preserve their broad
        # behavior while every new v3 analysis is explicitly typed.
        if not uses:
            uses = ["OUTFIT", "ENVIRONMENT", "VISUAL_STYLE"]
        item["index"] = index
        item["reference_uses"] = uses
        item["use_source"] = str(item.get("use_source") or "auto")
        normalized_items.append(item)
        if "OUTFIT" in uses:
            outfit.append({
                "index": index,
                "garment_cues": list(item.get("garment_cues") or []),
                "palette_cues": list(item.get("palette_cues") or []),
                "material_cues": list(item.get("material_cues") or []),
                "outfit_formula": str(item.get("outfit_formula") or ""),
                "palette_relation": str(item.get("palette_relation") or ""),
                "styling_details": str(item.get("styling_details") or ""),
            })
        if "ENVIRONMENT" in uses:
            environment.append({
                "index": index,
                "background_cues": list(item.get("background_cues") or []),
                "setting": str(item.get("setting") or ""),
            })
        if "VISUAL_STYLE" in uses:
            visual.append({
                "index": index,
                "style_cues": list(item.get("style_cues") or []),
                "lighting": str(item.get("lighting") or ""),
                "composition": str(item.get("composition") or ""),
            })
        if "LAYER_PROGRESSION" in uses:
            layering.append({
                "index": index,
                "garment_cues": list(item.get("garment_cues") or []),
                "outfit_formula": str(item.get("outfit_formula") or ""),
                "styling_details": str(item.get("styling_details") or ""),
                "composition": str(item.get("composition") or ""),
            })
    outfit_palette = list(dict.fromkeys(
        str(value) for item in outfit for value in item.get("palette_cues") or [] if value
    ))
    environment_features = list(dict.fromkeys(
        str(value) for item in environment
        for value in item.get("background_cues") or [] if value
    ))
    visual_styles = list(dict.fromkeys(
        str(value) for item in visual for value in item.get("style_cues") or [] if value
    ))
    return {
        "per_reference": normalized_items,
        "outfit_reference": {"indices": [v["index"] for v in outfit],
                             "palette": outfit_palette, "items": outfit},
        "environment_reference": {"indices": [v["index"] for v in environment],
                                  "background_features": environment_features,
                                  "items": environment},
        "visual_style_reference": {"indices": [v["index"] for v in visual],
                                   "visual_styles": visual_styles, "items": visual},
        "layer_progression_reference": {
            "indices": [v["index"] for v in layering], "items": layering,
        },
    }


class PhotoReferenceVisionError(RuntimeError):
    pass


class _DoubaoVisionClient:
    """Small deterministic client: visual extraction does not need deep reasoning."""
    def __init__(self, *, api_url: str, api_key: str, model: str, timeout: int = 90):
        self.api_url, self.api_key, self.model, self.timeout = api_url, api_key, model, timeout

    def chat_with_multiple_images(self, paths, prompt, max_tokens):
        content = []
        for value in paths:
            path = Path(value)
            mime = mimetypes.guess_type(path.name)[0] or "image/jpeg"
            encoded = base64.b64encode(path.read_bytes()).decode("ascii")
            content.append({"type": "image_url", "image_url": {
                "url": f"data:{mime};base64,{encoded}",
            }})
        content.append({"type": "text", "text": prompt})
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": content}],
            "max_tokens": max_tokens,
            "thinking": {"type": "disabled"},
            "response_format": {"type": "json_object"},
        }
        try:
            response = requests.post(
                self.api_url.rstrip("/") + "/chat/completions",
                headers={"Authorization": "Bearer " + self.api_key,
                         "Content-Type": "application/json"},
                json=payload, timeout=self.timeout,
            )
        except requests.RequestException as exc:
            raise PhotoReferenceVisionError(f"图文视觉模型网络调用失败：{exc}") from exc
        if response.status_code != 200:
            raise PhotoReferenceVisionError(
                f"图文视觉模型调用失败（HTTP {response.status_code}）"
            )
        return response.json()

    @staticmethod
    def parse_json_response(response):
        return parse_vision_envelope(response)


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest()


def _vision_failure_detail(response: Any) -> str:
    """把失败响应的关键事实带进报错，避免只有一句泛化文案无法定位。"""
    try:
        choice = (response.get("choices") or [{}])[0]
        message = choice.get("message") or {}
        content = message.get("content")
        parts = [f"finish_reason={choice.get('finish_reason') or 'unknown'}"]
        if content:
            text = str(content)
            parts.append(f"content_len={len(text)}")
            parts.append("head=" + text[:120].replace("\n", " "))
        else:
            parts.append("content为空")
        if response.get("error"):
            parts.append("error=" + str(response["error"])[:160])
        return "；".join(parts)
    except Exception:  # noqa: BLE001 - 诊断信息自身不允许再抛
        return "响应结构未知"


def _trace_vision_call(route: str, image_count: int, max_tokens: int,
                       response: Any, outcome: str = "ok") -> None:
    """Opt-in one-line accounting for every vision call attempt.

    Set ``OPV_VISION_DEBUG=1`` to see which route answered, how much of the
    output budget was actually consumed and how close the reply came to the
    truncation limit.  Silent unless asked for.
    """
    if not os.environ.get("OPV_VISION_DEBUG"):
        return
    try:
        choice = (response.get("choices") or [{}])[0]
        content = str((choice.get("message") or {}).get("content") or "")
        usage = response.get("usage") or {}
        print(
            f"[vision] route={route} outcome={outcome} images={image_count} "
            f"max_tokens={max_tokens} finish={choice.get('finish_reason')} "
            f"completion_tokens={usage.get('completion_tokens')} "
            f"prompt_tokens={usage.get('prompt_tokens')} chars={len(content)}",
            file=sys.stderr, flush=True,
        )
    except Exception:  # noqa: BLE001 - 诊断日志自身不允许再抛
        pass


def _trace_vision_exception(route: str, image_count: int, max_tokens: int,
                            exc: Exception, response: Any = None) -> None:
    """Same accounting for the failing attempt, so truncation is attributable."""
    if not os.environ.get("OPV_VISION_DEBUG"):
        return
    print(f"[vision] route={route} outcome=raised images={image_count} "
          f"max_tokens={max_tokens} error={exc}"[:1500],
          file=sys.stderr, flush=True)
    if response is not None:
        _trace_vision_call(route, image_count, max_tokens, response,
                           outcome="raised_with_response")


def parse_vision_envelope(response: Any) -> Dict[str, Any]:
    """Tolerant JSON extraction shared by every vision provider envelope."""
    try:
        choice = (response.get("choices") or [{}])[0]
        message = choice.get("message") or {}
        content = str(message.get("content") or "").strip()
        if not content:
            raise ValueError(f"模型返回空内容（{_vision_failure_detail(response)}）")
        if content.startswith("```"):
            content = content[3:]
            if content.startswith("json"):
                content = content[4:]
        if content.endswith("```"):
            content = content[:-3]
        starts = [index for index in (content.find("{"), content.find("[")) if index >= 0]
        if starts:
            content = content[min(starts):]
        try:
            value, _ = json.JSONDecoder().raw_decode(content.strip())
            return value
        except json.JSONDecodeError as exc:
            if str(choice.get("finish_reason") or "") == "length":
                raise ValueError(
                    "响应被截断（finish_reason=length，max_tokens 不足）；"
                    + _vision_failure_detail(response)) from exc
            raise ValueError("内容不可解析；" + _vision_failure_detail(response)) from exc
    except PhotoReferenceVisionError:
        raise
    except (KeyError, IndexError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise PhotoReferenceVisionError(f"图文视觉模型没有返回有效 JSON：{exc}") from exc


DEFAULT_COLOR_GRADING_PLAN = {
    "temperature": "neutral",
    "saturation": "medium",
    "contrast": "gentle",
    "skin_tone_anchor": "以人物参考图为唯一标准，四张完全一致",
    "tone_note_zh": "",
}


def _normalize_color_grading_plan(raw: Any) -> Dict[str, Any]:
    """Global color plan; missing/invalid entries fall back to safe defaults."""
    plan = dict(DEFAULT_COLOR_GRADING_PLAN)
    if isinstance(raw, Mapping):
        for key in plan:
            value = str(raw.get(key) or "").strip()
            if value:
                plan[key] = value
    return plan


def _normalize_palette_hex(raw: Any) -> list[str]:
    values = raw if isinstance(raw, list) else []
    normalized = []
    for value in values:
        text = str(value or "").strip().upper()
        if not text.startswith("#"):
            text = "#" + text
        if re.fullmatch(r"#[0-9A-F]{6}", text) and text not in normalized:
            normalized.append(text)
    return normalized[:4]


def _normalize_outfit_aesthetic(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, Mapping):
        # 模型/旧 fixture 未提供自评时视为"未打分"，不触发审美重规划。
        return {}
    aesthetic: Dict[str, Any] = {
        "harmony": 0, "layering": 0, "color_balance": 0, "proportion": 0,
        "issues": [], "revise_zh": "",
    }
    for key in ("harmony", "layering", "color_balance", "proportion"):
        try:
            aesthetic[key] = max(0, min(100, int(float(raw.get(key) or 0))))
        except (TypeError, ValueError):
            aesthetic[key] = 0
    issues = raw.get("issues")
    if isinstance(issues, list):
        aesthetic["issues"] = [str(value) for value in issues]
    aesthetic["revise_zh"] = str(raw.get("revise_zh") or "")
    return aesthetic


def _travel_copy_template(
    locale_pack: Mapping[str, Any] | None, index: int,
    travel_topic: Mapping[str, Any] | None, topic_zh: str = "",
) -> Dict[str, Any]:
    """Template used when the model's publish copy fails the language check.

    Review fix (2026-09-13), P0-2: with a Locale Pack bound the degraded copy is
    authored in the pack's own publish language, so a VN task can never fall
    back to Thai.  With no pack bound this reproduces the legacy inline Thai
    template byte-for-byte, so TH V2 degradation is unchanged.
    """
    from services.photo_locale import travel_copy_template
    localized = travel_copy_template(locale_pack, index, topic_zh=topic_zh)
    if localized is not None:
        return localized
    fallback = dict((travel_topic or {}).get("thai_fallback") or {})
    return {
        "title": str(fallback.get("title") or "4 ลุคทริป"),
        "cover": str(fallback.get("cover") or topic_zh or "ลุคทริป"),
        "caption": str(fallback.get("caption") or ""),
        "cta": str(fallback.get("cta") or "คุณชอบลุคไหน?"),
        "look_label": "ลุค {letter}",
        "hashtags": [str(v) for v in fallback.get("hashtags") or []],
    }


class PhotoReferenceVisionService:
    def __init__(self, *, root: Path, client: Any = None):
        self.root = Path(root)
        self.client = client
        self._load_local_environment()
        self.provider = os.environ.get("OPV_PHOTO_VISION_PROVIDER", "codex").strip().lower()
        if self.provider not in {"codex", "doubao"}:
            self.provider = "codex"
        self.model = os.environ.get("OPV_PHOTO_VISION_MODEL", "").strip()
        self.api_url = os.environ.get("OPV_PHOTO_VISION_API_URL", "").strip()
        self.api_key = os.environ.get("OPV_PHOTO_VISION_API_KEY", "").strip()
        self.codex_model = os.environ.get(
            "OPV_PHOTO_VISION_CODEX_MODEL", "gpt-5.6-sol"
        ).strip()
        self.codex_effort = os.environ.get(
            "OPV_PHOTO_VISION_CODEX_EFFORT", "medium"
        ).strip() or "medium"
        # Only travel outfit planning needs the higher-judgement route.
        self.travel_planning_model = os.environ.get(
            "OPV_PHOTO_TRAVEL_PLANNING_MODEL", "gpt-6-astra"
        ).strip() or "gpt-6-astra"
        self.travel_planning_effort = os.environ.get(
            "OPV_PHOTO_TRAVEL_PLANNING_EFFORT", "medium"
        ).strip() or "medium"

    @staticmethod
    def _load_local_environment() -> None:
        """Load only the dedicated OPV variables without overriding runtime env."""
        path = Path(__file__).resolve().parents[3] / ".env.local"
        if not path.is_file():
            return
        allowed = {
            "OPV_PHOTO_VISION_MODEL", "OPV_PHOTO_VISION_API_URL",
            "OPV_PHOTO_VISION_API_KEY", "OPV_PHOTO_VISION_PROVIDER",
            "OPV_PHOTO_QA_LEVEL",
            "OPV_PHOTO_VISION_CODEX_MODEL", "OPV_PHOTO_VISION_CODEX_EFFORT",
            "OPV_PHOTO_TRAVEL_PLANNING_MODEL",
            "OPV_PHOTO_TRAVEL_PLANNING_EFFORT",
        }
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if key in allowed and key not in os.environ:
                os.environ[key] = value.strip().strip('"').strip("'")

    def _build_client(self, provider: str) -> Any:
        if provider == "codex":
            from services.codex_vision_client import CodexVisionClient
            return CodexVisionClient(
                model=self.codex_model, reasoning_effort=self.codex_effort,
            )
        if not self.model or not self.api_url or not self.api_key:
            raise PhotoReferenceVisionError("图文视觉模型配置不完整")
        return _DoubaoVisionClient(
            api_url=self.api_url, api_key=self.api_key, model=self.model, timeout=90,
        )

    def _client(self) -> Any:
        if self.client is not None:
            return self.client
        return self._build_client(self.provider)

    def _chat(self, paths: Sequence[str], prompt: str,
              max_tokens: int, instructions: str = "",
              prefer: str = "") -> tuple[Any, str]:
        """Provider call with explicit fallback direction.

        Default (planning): codex → doubao. ``prefer="fast"`` (QA/observation
        calls): doubao → codex — reasoning adds latency without adding verdict
        value since pass/fail is computed programmatically. An injected client
        (deterministic/test mode) never falls back. The used provider is
        returned so callers record it in manifests/evidence.
        """
        if prefer == "fast" and self.client is None and all(
                (self.model, self.api_url, self.api_key)):
            # 预解析校验：偶发的空内容/坏 JSON 在此就地重试一次，而不是
            # 让整轮 QA 失败落进"失败可重试"。
            doubao_error: Exception | None = None
            for _ in range(2):
                try:
                    doubao = self._build_client("doubao")
                    response = doubao.chat_with_multiple_images(
                        paths, prompt, max_tokens)
                    parse_vision_envelope(response)
                    _trace_vision_call("fast_path_doubao", len(paths), max_tokens, response)
                    return response, "doubao"
                except Exception as exc:  # noqa: BLE001 - fallback boundary
                    _trace_vision_exception(
                        "fast_path_doubao", len(paths), max_tokens, exc,
                        locals().get("response"))
                    doubao_error = exc
            del doubao_error
        try:
            client = self._client()
            response = client.chat_with_multiple_images(paths, prompt, max_tokens)
            parse_vision_envelope(response)
            _trace_vision_call(self.provider, len(paths), max_tokens, response)
            return response, self.provider
        except Exception as exc:  # noqa: BLE001 - fallback boundary
            _trace_vision_exception(
                self.provider, len(paths), max_tokens, exc, locals().get("response"))
            injectable = self.client is not None
            doubao_ready = self.model and self.api_url and self.api_key
            if self.provider != "codex" or injectable or not doubao_ready:
                raise
            doubao = self._build_client("doubao")
            response = doubao.chat_with_multiple_images(paths, prompt, max_tokens)
            try:
                parse_vision_envelope(response)
            except Exception as exc:  # noqa: BLE001 - 归因后原样抛出
                _trace_vision_exception(
                    "doubao_fallback", len(paths), max_tokens, exc, response)
                raise
            _trace_vision_call("doubao_fallback", len(paths), max_tokens, response)
            return response, "doubao_fallback"

    def provider_signature(self) -> Dict[str, str]:
        """Routing facts included in cache hashes and saved evidence."""
        return {
            "provider": self.provider,
            "model": (
                self.codex_model if self.provider == "codex" else self.model
            ),
            "reasoning_effort": (
                self.codex_effort if self.provider == "codex" else ""
            ),
        }

    def travel_planning_signature(self) -> Dict[str, str]:
        return {
            "provider": "codex",
            "model": self.travel_planning_model,
            "reasoning_effort": self.travel_planning_effort,
        }

    def _travel_planning_chat(
        self, paths: Sequence[str], prompt: str, max_tokens: int,
    ) -> tuple[Any, Dict[str, Any]]:
        """Astra planning route with the existing Doubao fallback."""
        if self.client is not None:
            response = self.client.chat_with_multiple_images(paths, prompt, max_tokens)
            return response, {
                "requested_model": self.travel_planning_model,
                "actual_model": str(getattr(self.client, "model", "")
                                    or self.travel_planning_model),
                "reasoning_effort": self.travel_planning_effort,
                "provider_used": "injected",
                "fallback_used": False,
            }
        try:
            from services.codex_vision_client import CodexVisionClient
            client = CodexVisionClient(
                model=self.travel_planning_model,
                reasoning_effort=self.travel_planning_effort,
            )
            response = client.chat_with_multiple_images(paths, prompt, max_tokens)
            return response, {
                "requested_model": self.travel_planning_model,
                "actual_model": self.travel_planning_model,
                "reasoning_effort": self.travel_planning_effort,
                "provider_used": "codex",
                "fallback_used": False,
            }
        except Exception:
            if not all((self.model, self.api_url, self.api_key)):
                raise
            client = self._build_client("doubao")
            response = client.chat_with_multiple_images(paths, prompt, max_tokens)
            return response, {
                "requested_model": self.travel_planning_model,
                "actual_model": self.model,
                "reasoning_effort": "",
                "provider_used": "doubao_fallback",
                "fallback_used": True,
            }

    def analyze(
        self, *, record_id: str, paths: Sequence[str], theme: Mapping[str, Any],
        category_key: str, content_requirement: str = "", count: int = 1,
        product_context: Mapping[str, Any] = None,
        planning_flow: str = "", required_roles: Sequence[str] = (),
    ) -> dict[str, Any]:
        images = [str(Path(value).expanduser().resolve()) for value in paths]
        if not images or any(not Path(value).is_file() for value in images):
            raise PhotoReferenceVisionError("参考图缺失或不可读取")
        source_hashes = [hashlib.sha256(Path(value).read_bytes()).hexdigest() for value in images]
        try:
            frozen_required_roles = resolve_required_roles(
                planning_flow=planning_flow, required_roles=required_roles,
            )
        except PhotoFlowRegistryError as exc:
            raise PhotoReferenceVisionError(str(exc)) from exc
        input_contract = {
            "prompt_version": PROMPT_VERSION, "model": self.model,
            "reference_hashes": source_hashes, "theme": dict(theme),
            "category_key": category_key, "content_requirement": content_requirement,
            "count": count,
            "product_context": dict(product_context or {}),
            "routing": self.provider_signature(),
        }
        # Preserve the frozen hash contract for historical choice tasks.  New
        # flows explicitly carry both fields and therefore get an isolated cache.
        if planning_flow or required_roles:
            input_contract["planning_flow"] = str(
                planning_flow or "reference_contract_v1"
            )
            input_contract["required_roles"] = list(frozen_required_roles)
        input_sha256 = _hash(input_contract)
        folder = self.root / "reference_contracts" / self._safe(record_id)
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / "contract.json"
        if path.is_file():
            cached = json.loads(path.read_text(encoding="utf-8"))
            if cached.get("input_sha256") != input_sha256:
                raise PhotoReferenceVisionError("参考图或内容要求已经变化；请新建任务避免混用旧视觉合同")
            return dict(cached["contract"])
        prompt = self._analysis_prompt(
            theme=theme, category_key=category_key,
            content_requirement=content_requirement, count=count,
            product_context=product_context,
            required_roles=frozen_required_roles,
        )
        raw, provider_used = self._chat_with_truncation_retry(
            self._model_images(images), prompt,
            self.analysis_token_budget(
                count=count, image_count=len(images),
                role_count=len(frozen_required_roles),
            ),
        )
        raw = parse_vision_envelope(raw)
        contract = self._normalize_contract(
            raw, source_hashes, content_requirement, count,
            planning_flow=planning_flow, required_roles=frozen_required_roles,
        )
        # 穿搭审美自评只记录：模型应在上一次规划内自行把关，
        # 不因自评低分再调一次模型（2026-09-07 用户裁决）。
        contract["vision_provider"] = provider_used
        contract["model_routing"] = {
            "provider_used": provider_used,
            "model_used": (
                self.codex_model if provider_used.startswith("codex")
                else self.model if provider_used.startswith("doubao")
                else self.codex_model
            ),
            "reasoning_effort": self.codex_effort if provider_used.startswith("codex") else "",
            "fallback_used": "fallback" in provider_used,
        }
        payload = {
            "schema_version": "opv-photo-reference-contract-store-v1",
            "record_id": record_id, "input_sha256": input_sha256,
            "input_contract": input_contract, "contract": contract,
        }
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(path)
        return contract

    @staticmethod
    def _weak_aesthetic_looks(contract: Mapping[str, Any]) -> list[str]:
        weak: list[str] = []
        for item in contract.get("recommended_sets") or []:
            for look in item.get("looks") or []:
                aesthetic = look.get("outfit_aesthetic") or {}
                if not isinstance(aesthetic, Mapping) or not aesthetic:
                    continue
                low = [
                    f"{key}={value}"
                    for key, value in aesthetic.items()
                    if key in {"harmony", "layering", "color_balance", "proportion"}
                    and isinstance(value, (int, float)) and value < 85
                ]
                if low:
                    role = str(look.get("role") or "?")
                    weak.append(
                        f"{role} 穿搭审美低分（{'，'.join(low)}）；"
                        f"{aesthetic.get('revise_zh') or '重新搭配为成套协调的日常时尚穿搭'}"
                    )
        return weak

    def review_alignment(
        self, *, reference_paths: Sequence[str], generated_paths: Sequence[str],
        contract: Mapping[str, Any], scope: str,
        generated_roles: Sequence[str] = None,
        persona_based: bool = False,
        product_reference_paths: Sequence[str] = (),
        product_context: Mapping[str, Any] = None,
    ) -> dict[str, Any]:
        # 指定商品参考图必须排在最前，并且**真的进模型输入**：提示词里说"有商品"
        # 而图片没送到，等于没有检查（review 修复 P0-3 的同源要求）。
        # 风格参考列表保持原样（含顺序与重复）：没指定商品时参考图数量必须逐字不变。
        product_references = list(dict.fromkeys(
            str(Path(value).resolve()) for value in product_reference_paths
            if Path(str(value)).is_file()
        ))
        references = product_references + [
            str(Path(item).resolve()) for item in reference_paths
            if str(Path(item).resolve()) not in product_references
        ]
        generated = [str(Path(value).resolve()) for value in generated_paths]
        if not references or not generated or any(
                not Path(value).is_file() for value in references + generated):
            raise PhotoReferenceVisionError("视觉一致性检查缺少图片")
        prompt = self._alignment_prompt(
            reference_count=len(references), generated_count=len(generated),
            contract=contract, scope=scope, generated_roles=generated_roles,
            persona_based=persona_based,
            product_reference_count=len(product_references),
            product_context=dict(product_context or {}),
        )
        response, provider_used = self._chat(
            self._model_images(references + generated), prompt, max_tokens=1800,
            # 单张门禁是"是否继续生成 B/C/D"的关键决策点：合同里的组级要求
            # （四张动作不同）会误导轻量模型把"当前只有一张"判为违规，
            # scope 语义判断保留 codex；组级检查走 fast。
            prefer="" if str(scope).startswith("FIRST_LOOK") else "fast",
        )
        raw = parse_vision_envelope(response)
        if not isinstance(raw, Mapping) or not isinstance(raw.get("passed"), bool):
            raise PhotoReferenceVisionError("视觉一致性检查返回结构无效")
        scores = dict(raw.get("scores") or {})
        required = {"presentation_alignment", "style_alignment", "scene_alignment", "palette_alignment"}
        if not required.issubset(scores) or any(not isinstance(scores[key], (int, float)) for key in required):
            raise PhotoReferenceVisionError("视觉一致性检查缺少必要评分")
        passed = bool(raw["passed"]) and all(float(scores[key]) >= 75 for key in required)
        return {
            "schema_version": "opv-photo-reference-alignment-v1",
            "scope": scope, "passed": passed, "scores": scores,
            "reason_codes": [str(value) for value in raw.get("reason_codes") or []],
            "notes": str(raw.get("notes") or ""), "model": self.model,
            "provider_used": provider_used,
            "role_findings": self._normalize_role_findings(raw, generated_roles),
            "model_routing": {
                "provider_used": provider_used,
                "model_used": (
                    self.model if provider_used.startswith("doubao")
                    else self.codex_model
                ),
                "reasoning_effort": "" if provider_used.startswith("doubao") else self.codex_effort,
                "fallback_used": "fallback" in provider_used,
            },
        }

    @staticmethod
    def _normalize_role_findings(raw: Mapping[str, Any],
                                 generated_roles: Sequence[str]) -> list[dict[str, Any]]:
        """Map group-level failures onto roles; empty list means no attribution."""
        if not generated_roles:
            return []
        allowed = [str(value) for value in generated_roles]
        findings: list[dict[str, Any]] = []
        per_look = raw.get("per_look")
        if isinstance(per_look, list):
            for item in per_look:
                if not isinstance(item, Mapping):
                    continue
                role = str(item.get("role") or "")
                if role not in allowed or not isinstance(item.get("passed"), bool):
                    continue
                findings.append({
                    "role": role, "passed": bool(item["passed"]),
                    "issues": [str(value) for value in item.get("issues") or []],
                    "missing_major_garment": bool(item.get("missing_major_garment")),
                })
        return findings

    # ------------------------------------------------------------------
    # Travel two-step flow: describe references, then plan per-moment looks.
    # ------------------------------------------------------------------

    # Output budgets for the two planning prompts.  Both emit a per-reference
    # description (13~18 fields per image) plus one look panel per requested
    # post, so a budget keyed only on the post count truncates as soon as a line
    # carries several references at once — 2026-09-14 越南围巾线：3 张参考图 +
    # 1 篇四选一算得 2200 tokens，doubao 正好把 2200 用尽（finish_reason=length）
    # 导致整行规划失败。两份预算都随「参考图张数 × 每篇 look 数」增长。
    VISION_OUTPUT_CEILING = 12000
    VISION_RETRY_CEILING = 16000
    REFERENCE_ANALYSIS_BASE_TOKENS = 2200
    REFERENCE_ANALYSIS_TOKENS_PER_IMAGE = 1000
    ANALYSIS_BASE_TOKENS = 2600
    ANALYSIS_TOKENS_PER_IMAGE = 700
    ANALYSIS_TOKENS_PER_LOOK = 700

    def reference_analysis_token_budget(self, image_count: int) -> int:
        """Scale the description budget with the number of reference images."""
        count = max(1, int(image_count))
        return min(
            self.VISION_OUTPUT_CEILING,
            self.REFERENCE_ANALYSIS_BASE_TOKENS
            + self.REFERENCE_ANALYSIS_TOKENS_PER_IMAGE * count,
        )

    def analysis_token_budget(self, *, count: int, image_count: int,
                              role_count: int) -> int:
        """Scale the analysis contract budget with references and look panels."""
        posts = max(1, int(count))
        images = max(1, int(image_count))
        looks = max(1, int(role_count)) * posts
        return min(
            self.VISION_OUTPUT_CEILING,
            self.ANALYSIS_BASE_TOKENS
            + self.ANALYSIS_TOKENS_PER_IMAGE * images
            + self.ANALYSIS_TOKENS_PER_LOOK * looks,
        )

    def _chat_with_truncation_retry(self, paths: Sequence[str], prompt: str,
                                    budget: int) -> tuple[Any, str]:
        """One retry with a doubled budget when the reply was cut off.

        Truncation is a budget accident, not a bad reference set: the identical
        request with more room succeeds (verified 2026-09-14).  Anything else —
        unreadable image, invalid structure — must bubble up unchanged.
        """
        try:
            return self._chat(paths, prompt, max_tokens=budget)
        except PhotoReferenceVisionError as exc:
            enlarged = min(self.VISION_RETRY_CEILING, budget * 2)
            if "截断" not in str(exc) or enlarged <= budget:
                raise
            return self._chat(paths, prompt, max_tokens=enlarged)

    def analyze_reference(
        self, *, record_id: str, paths: Sequence[str], theme: Mapping[str, Any],
        category_key: str, content_requirement: str = "",
    ) -> dict[str, Any]:
        """Step 1: describe references only — no scene planning, no copy."""
        images = [str(Path(value).expanduser().resolve()) for value in paths]
        if not images or any(not Path(value).is_file() for value in images):
            raise PhotoReferenceVisionError("参考图缺失或不可读取")
        source_hashes = [hashlib.sha256(Path(value).read_bytes()).hexdigest() for value in images]
        input_contract = {
            "prompt_version": TRAVEL_PROMPT_VERSION, "model": self.model,
            "reference_hashes": source_hashes, "theme": dict(theme),
            "category_key": category_key, "content_requirement": content_requirement,
            "routing": self.provider_signature(),
        }
        folder = self.root / "reference_contracts" / self._safe(record_id)
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / "reference_analysis.json"
        input_sha256 = _hash(input_contract)
        if path.is_file():
            cached = json.loads(path.read_text(encoding="utf-8"))
            if cached.get("input_sha256") != input_sha256:
                raise PhotoReferenceVisionError(
                    "参考图或内容要求已变化；请新建任务避免混用旧参考分析"
                )
            return dict(cached["analysis"])
        prompt = self._reference_analysis_prompt(
            theme=theme, category_key=category_key,
            content_requirement=content_requirement)
        budget = self.reference_analysis_token_budget(len(images))
        response, _ = self._chat_with_truncation_retry(
            self._model_images(images), prompt, budget)
        raw = parse_vision_envelope(response)
        analysis = self._normalize_reference_analysis(raw, source_hashes)
        payload = {
            "schema_version": "opv-photo-travel-reference-analysis-store-v1",
            "record_id": record_id, "input_sha256": input_sha256,
            "input_contract": input_contract, "analysis": analysis,
        }
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(path)
        return analysis

    def plan_travel_content(
        self, *, record_id: str, analysis: Mapping[str, Any],
        travel_contract: Mapping[str, Any], variables: Mapping[str, Any],
        content_requirement: str = "", count: int = 1,
        travel_topic: Mapping[str, Any] = None,
        product_context: Mapping[str, Any] = None,
        reference_paths: Sequence[str] = (),
        product_reference_paths: Sequence[str] = (),
        locale_pack: Mapping[str, Any] = None,
    ) -> Dict[str, Any]:
        """Step 2: recipe-bound travel plan; strongly validated with one auto-revise.

        ``travel_topic``（地点+六类主题）present = new-theme branch: the four
        looks may share one travel_moment, and the planning response carries
        the publish copy（title/caption/逐页 slide_texts）alongside the looks.
        """
        moments = list(travel_contract.get("moments") or [])
        if not moments:
            raise PhotoReferenceVisionError("旅行合同缺少场景枚举")
        topic = dict(travel_topic or {})
        topic_theme_type = str(topic.get("theme_type") or "")
        planning_paths, planning_images = self._travel_planning_images(
            analysis=analysis, reference_paths=reference_paths,
            product_reference_paths=product_reference_paths,
        )
        input_contract = {
            "prompt_version": TRAVEL_PROMPT_VERSION,
            "analysis_sha256": _hash(dict(analysis)),
            "moments": [str(item.get("key") or "") for item in moments],
            "variables": dict(variables), "content_requirement": content_requirement,
            "count": count,
            "travel_topic": topic,
            "product_context": dict(product_context or {}),
            "planning_route": self.travel_planning_signature(),
            "planning_images": planning_images,
        }
        folder = self.root / "reference_contracts" / self._safe(record_id)
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / "travel_plan.json"
        input_sha256 = _hash(input_contract)
        if path.is_file():
            cached = json.loads(path.read_text(encoding="utf-8"))
            if cached.get("input_sha256") != input_sha256:
                raise PhotoReferenceVisionError(
                    "参考分析或旅行变量已变化；请新建任务避免混用旧旅行计划"
                )
            return dict(cached["plan"])
        base_prompt = self._travel_plan_prompt(
            analysis=analysis, travel_contract=travel_contract,
            variables=variables, content_requirement=content_requirement, count=count,
            travel_topic=topic,
            product_context=product_context,
        )
        if planning_images:
            base_prompt += self._travel_planning_image_prompt(planning_images)
        background_features = [
            str(value) for value in dict(analysis).get("background_features") or []
            if str(value or "").strip()
        ]
        response, model_routing = self._travel_planning_chat(
            self._model_images(planning_paths), base_prompt, max_tokens=3600,
        )
        raw = parse_vision_envelope(response)
        outfit_reference_indices = list(
            (dict(analysis).get("outfit_reference") or {}).get("indices") or []
        )
        plan, errors = self._normalize_travel_plan(
            raw, travel_contract, count, background_features=background_features,
            travel_topic=topic, outfit_reference_indices=outfit_reference_indices,
            product_context=product_context, locale_pack=locale_pack)
        if errors:
            revise_prompt = base_prompt + "\n\n上一次输出存在以下结构错误，必须全部修正后重新输出完整 JSON：\n- " + "\n- ".join(errors)
            response, model_routing = self._travel_planning_chat(
                self._model_images(planning_paths), revise_prompt, max_tokens=3600,
            )
            raw = parse_vision_envelope(response)
            plan, errors = self._normalize_travel_plan(
                raw, travel_contract, count, background_features=background_features,
                travel_topic=topic, outfit_reference_indices=outfit_reference_indices,
                product_context=product_context)
            if errors:
                raise PhotoReferenceVisionError(
                    "旅行内容计划两次未通过结构校验：" + "；".join(errors[:6])
                )
        plan["model"] = model_routing["actual_model"]
        plan["model_routing"] = dict(model_routing)
        payload = {
            "schema_version": "opv-photo-travel-plan-store-v1",
            "record_id": record_id, "input_sha256": input_sha256,
            "input_contract": input_contract, "plan": plan,
        }
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(path)
        return plan

    @staticmethod
    def _travel_planning_image_prompt(images: Sequence[Mapping[str, Any]]) -> str:
        lines = []
        for index, item in enumerate(images, 1):
            uses = "、".join(str(value) for value in item.get("uses") or [])
            lines.append(f"- 图片{index}：{uses}")
        return (
            "\n\n【本次直接提供给规划模型的图片】\n"
            + "\n".join(lines)
            + "\n商品图只决定指定商品的颜色、版型与结构；OUTFIT 参考只用于搭配比例、层次、"
              "配色关系和穿法；ENVIRONMENT/VISUAL_STYLE 参考中的服装不得成为商品或搭配权威。"
              "请直接观察图片中的全身比例以及下装与鞋履衔接，不只依赖前面的文字摘要。"
        )

    @staticmethod
    def _travel_planning_images(
        *, analysis: Mapping[str, Any], reference_paths: Sequence[str],
        product_reference_paths: Sequence[str],
    ) -> tuple[list[str], list[dict[str, Any]]]:
        """Select and fingerprint existing inputs without new operator fields."""
        selected: dict[str, dict[str, Any]] = {}
        per_reference = {
            int(item.get("index") or 0): dict(item)
            for item in analysis.get("per_reference") or []
            if isinstance(item, Mapping)
        }
        for index, raw in enumerate(reference_paths or (), 1):
            path = Path(str(raw)).expanduser().resolve()
            if not path.is_file():
                raise PhotoReferenceVisionError(f"旅行规划参考图不存在：{path}")
            uses = _normalize_reference_uses(
                per_reference.get(index, {}).get("reference_uses")
            ) or ["OUTFIT", "ENVIRONMENT", "VISUAL_STYLE"]
            selected[str(path)] = {
                "uses": uses,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        for raw in product_reference_paths or ():
            path = Path(str(raw)).expanduser().resolve()
            if not path.is_file():
                raise PhotoReferenceVisionError(f"旅行规划商品参考图不存在：{path}")
            key = str(path)
            if key in selected:
                uses = list(selected[key]["uses"])
                if "PRODUCT" not in uses:
                    uses.insert(0, "PRODUCT")
                selected[key]["uses"] = uses
            else:
                selected[key] = {
                    "uses": ["PRODUCT"],
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                }
        paths = list(selected)
        metadata = [
            {"uses": list(selected[path]["uses"]),
             "sha256": selected[path]["sha256"]}
            for path in paths
        ]
        return paths, metadata

    def review_travel_pages(
        self, *, reference_paths: Sequence[str] = (),
        product_reference_paths: Sequence[str] = (),
        style_reference_paths: Sequence[str] = (),
        product_context: Mapping[str, Any] = None, travel_place: str = "",
        look_plans: Sequence[Mapping[str, Any]],
        image_paths: Sequence[str], travel_contract: Mapping[str, Any] = None,
        persona_based: bool = False,
    ) -> dict[str, Any]:
        """Per-page travel semantic QA against the frozen per-look moments.

        Structural QA failures get exactly one retry; a second incomplete
        response stops with the raw payload preserved for inspection.
        """
        from services.photo_travel_qa import (
            FOOTWEAR_TYPES, TravelSemanticQAError, moment_rules_from_contract,
            normalize_travel_qa,
        )
        product_references = list(dict.fromkeys(
            str(Path(value).resolve()) for value in product_reference_paths
        ))
        style_references = [
            value for value in dict.fromkeys(
                str(Path(item).resolve()) for item in style_reference_paths
            ) if value not in product_references
        ]
        references = product_references + style_references or [
            str(Path(value).resolve()) for value in reference_paths
        ]
        generated = [str(Path(value).resolve()) for value in image_paths]
        if not references or not generated or any(
                not Path(value).is_file() for value in references + generated):
            raise PhotoReferenceVisionError("旅行语义质检缺少图片")
        contract = dict(travel_contract or {})
        # 类目适配器声明本类目的逐项商品质检字段（review 修复 P0-3）。未注册
        # 类目或 womenswear（qa_fields 为空）时全部退化为历史行为。
        from services.photo_category_registry import adapter_for_product_category
        adapter = adapter_for_product_category(
            dict(product_context or {}).get("category"))
        qa_fields = tuple(getattr(adapter, "qa_fields", ()) or ())
        qa_rules = tuple(getattr(adapter, "qa_rules", ()) or ())
        base_outfit_keys = ("outerwear", "top_inner", "bottom", "shoes")
        main_slot = str(getattr(adapter, "main_product_slot", "") or "")
        extra_slots = (main_slot,) if main_slot and main_slot not in base_outfit_keys else ()
        page_plans = []
        for look in look_plans:
            page_plans.append({
                "role": str(look.get("role") or ""),
                "travel_moment": str(look.get("travel_moment") or ""),
                "scene_prompt": str(look.get("scene_prompt") or ""),
                "weather_logic": str(look.get("weather_logic") or ""),
                "footwear_type": str(look.get("footwear_type") or ""),
                "outfit": {
                    key: str(look.get(key) or "")
                    for key in base_outfit_keys + extra_slots
                },
            })
        prompt = self._travel_qa_prompt(
            page_plans=page_plans, reference_count=len(references),
            product_reference_count=len(product_references),
            style_reference_count=len(style_references),
            image_count=len(generated), persona_based=persona_based,
            product_context=dict(product_context or {}), travel_place=travel_place,
            qa_fields=qa_fields, qa_rules=qa_rules,
        )
        response, _ = self._chat(
            self._model_images(references + generated), prompt, max_tokens=2600,
            prefer="fast",
        )
        raw = parse_vision_envelope(response)
        try:
            return normalize_travel_qa(
                raw, look_plans=look_plans,
                moment_rules=moment_rules_from_contract(contract),
                footwear_types=FOOTWEAR_TYPES,
                has_product=bool(product_context), travel_place=travel_place,
                product_qa_fields=qa_fields,
            )
        except TravelSemanticQAError as first_error:
            retry_prompt = (
                prompt
                + "\n\n上一次质检结果结构不完整或类型错误（"
                + str(first_error)
                + "）。必须重新输出覆盖全部页面的完整 JSON，所有布尔字段必须是真正的 true/false，"
                "不得缺失或写为字符串。"
            )
            retry_response, _ = self._chat(
                self._model_images(references + generated), retry_prompt, max_tokens=2600,
                prefer="fast",
            )
            raw_retry = parse_vision_envelope(retry_response)
            try:
                return normalize_travel_qa(
                    raw_retry, look_plans=look_plans,
                    moment_rules=moment_rules_from_contract(contract),
                    footwear_types=FOOTWEAR_TYPES,
                    has_product=bool(product_context), travel_place=travel_place,
                    product_qa_fields=qa_fields,
                )
            except TravelSemanticQAError as second_error:
                raise PhotoReferenceVisionError(
                    f"旅行语义质检两次结构不完整：{second_error}；"
                    f"原始响应已保留：{json.dumps(raw_retry, ensure_ascii=False)[:1200]}"
                ) from second_error

    def observe_layering_pages(
        self, *, image_paths: Sequence[str], roles: Sequence[str],
        band_key: str, reference_paths: Sequence[str] = (),
        look_plans: Sequence[Mapping[str, Any]] = (),
        allowed_item_types: Sequence[str] = (),
        forbidden_item_types: Sequence[str] = (),
    ) -> dict[str, Any]:
        """Return visual facts for one base/mid/outer progression group.

        The model only observes.  Pass/fail remains in photo_layering_qa so an
        incomplete response can never default to success.
        """
        references = [str(Path(value).resolve()) for value in reference_paths]
        images = [str(Path(value).resolve()) for value in image_paths]
        if (not images or len(images) != len(roles)
                or any(not Path(value).is_file() for value in references + images)):
            raise PhotoReferenceVisionError("温度分层质检缺少完整的角色图片")
        prompt = self._layering_qa_prompt(
            roles=roles, band_key=band_key, reference_count=len(references),
            look_plans=look_plans, allowed_item_types=allowed_item_types,
            forbidden_item_types=forbidden_item_types,
        )
        response, _ = self._chat(
            self._model_images(references + images), prompt, max_tokens=2600,
            prefer="fast",
        )
        return parse_vision_envelope(response)

    def review_layering_pages(
        self, *, reference_paths: Sequence[str] = (),
        look_plans: Sequence[Mapping[str, Any]], image_paths: Sequence[str],
        layering_contract: Mapping[str, Any],
        profile_binding: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Observe and deterministically judge one layering source group."""
        from services.photo_layering_qa import (
            LayeringSemanticQAError, normalize_layering_qa,
        )
        roles = [str(value) for value in layering_contract.get("source_roles") or ()]
        band_key = str(profile_binding.get("band_key") or "")
        band = next(
            (item for item in layering_contract.get("bands") or []
             if isinstance(item, Mapping) and str(item.get("key") or "") == band_key),
            {},
        )
        raw = self.observe_layering_pages(
            image_paths=image_paths, roles=roles, band_key=band_key,
            reference_paths=reference_paths, look_plans=look_plans,
            allowed_item_types=list(band.get("allowed_item_types") or []),
            forbidden_item_types=list(band.get("forbidden_item_types") or []),
        )
        try:
            return normalize_layering_qa(
                raw, look_plans=look_plans, profile_binding=profile_binding,
                layering_contract=layering_contract,
            )
        except LayeringSemanticQAError as first_error:
            prompt = self._layering_qa_prompt(
                roles=roles, band_key=band_key,
                reference_count=len(reference_paths), look_plans=look_plans,
                allowed_item_types=list(band.get("allowed_item_types") or []),
                forbidden_item_types=list(band.get("forbidden_item_types") or []),
            ) + (
                "\n\n上一次输出结构不完整（" + str(first_error)
                + "）。重新输出完整 JSON；不得缺字段，不得把布尔值写成字符串。"
            )
            all_paths = [str(Path(value).resolve()) for value in reference_paths]
            all_paths += [str(Path(value).resolve()) for value in image_paths]
            response, _ = self._chat(
                self._model_images(all_paths), prompt, max_tokens=2600,
                prefer="fast",
            )
            raw_retry = parse_vision_envelope(response)
            try:
                return normalize_layering_qa(
                    raw_retry, look_plans=look_plans,
                    profile_binding=profile_binding,
                    layering_contract=layering_contract,
                )
            except LayeringSemanticQAError as second_error:
                raise PhotoReferenceVisionError(
                    f"温度分层质检两次结构不完整：{second_error}；"
                    f"原始响应已保留：{json.dumps(raw_retry, ensure_ascii=False)[:1200]}"
                ) from second_error

    def observe_thermal_transition_pages(
        self, *, image_paths: Sequence[str], roles: Sequence[str],
        transition_key: str, contexts: Sequence[str],
        reference_paths: Sequence[str] = (),
        look_plans: Sequence[Mapping[str, Any]] = (),
        allowed_item_types: Sequence[str] = (),
        forbidden_item_types: Sequence[str] = (),
    ) -> Mapping[str, Any]:
        prompt = self._thermal_transition_qa_prompt(
            roles=roles, transition_key=transition_key, contexts=contexts,
            reference_count=len(reference_paths), look_plans=look_plans,
            allowed_item_types=allowed_item_types,
            forbidden_item_types=forbidden_item_types,
        )
        all_paths = [str(Path(value).resolve()) for value in reference_paths]
        all_paths += [str(Path(value).resolve()) for value in image_paths]
        response, _ = self._chat(
            self._model_images(all_paths), prompt, max_tokens=2600, prefer="fast",
        )
        return parse_vision_envelope(response)

    def review_thermal_transition_pages(
        self, *, reference_paths: Sequence[str] = (),
        look_plans: Sequence[Mapping[str, Any]], image_paths: Sequence[str],
        thermal_transition_contract: Mapping[str, Any],
        profile_binding: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Observe one daily hot→cold source group and judge it deterministically."""
        from services.photo_layering_qa import LayeringSemanticQAError
        from services.photo_thermal_transition_qa import (
            ThermalTransitionQAError, evaluate_thermal_transition_qa,
        )
        roles = [
            str(value) for value in thermal_transition_contract.get("source_roles") or ()
        ]
        transition_key = str(profile_binding.get("transition_key") or "")
        entry = next(
            (item for item in thermal_transition_contract.get("transitions") or []
             if isinstance(item, Mapping) and str(item.get("key") or "") == transition_key),
            {},
        )
        contexts = [str(value) for value in entry.get("thermal_contexts") or []]
        allowed = list(thermal_transition_contract.get("allowed_item_types") or [])
        forbidden = list(thermal_transition_contract.get("forbidden_item_types") or [])
        raw = self.observe_thermal_transition_pages(
            image_paths=image_paths, roles=roles, transition_key=transition_key,
            contexts=contexts, reference_paths=reference_paths, look_plans=look_plans,
            allowed_item_types=allowed, forbidden_item_types=forbidden,
        )
        try:
            return evaluate_thermal_transition_qa(
                raw, look_plans=look_plans, profile_binding=profile_binding,
                thermal_transition_contract=thermal_transition_contract,
            )
        except (ThermalTransitionQAError, LayeringSemanticQAError) as first_error:
            prompt = self._thermal_transition_qa_prompt(
                roles=roles, transition_key=transition_key, contexts=contexts,
                reference_count=len(reference_paths), look_plans=look_plans,
                allowed_item_types=allowed, forbidden_item_types=forbidden,
            ) + (
                "\n\n上一次输出结构不完整（" + str(first_error)
                + "）。重新输出完整 JSON；不得缺字段，不得把布尔值写成字符串。"
            )
            all_paths = [str(Path(value).resolve()) for value in reference_paths]
            all_paths += [str(Path(value).resolve()) for value in image_paths]
            response, _ = self._chat(
                self._model_images(all_paths), prompt, max_tokens=2600, prefer="fast",
            )
            raw_retry = parse_vision_envelope(response)
            try:
                return evaluate_thermal_transition_qa(
                    raw_retry, look_plans=look_plans,
                    profile_binding=profile_binding,
                    thermal_transition_contract=thermal_transition_contract,
                )
            except (ThermalTransitionQAError, LayeringSemanticQAError) as second_error:
                raise PhotoReferenceVisionError(
                    f"冷热切换质检两次结构不完整：{second_error}；"
                    f"原始响应已保留：{json.dumps(raw_retry, ensure_ascii=False)[:1200]}"
                ) from second_error

    MX_WIG_PLAN_PROMPT_VERSION = "opv-photo-mx-wig-plan-v1"

    def plan_wig_choice_content(
        self, *, record_id: str, persona_paths: Sequence[str],
        reference_paths: Sequence[str] = (), content_requirement: str = "",
        count: int = 1, theme_label_zh: str = "", choice_axis: str = "style",
    ) -> dict[str, Any]:
        """MX wig four-choice planning route (Astra path), mirror of travel.

        Emits one frozen plan with ``count`` items; each item carries four
        distinct hair options plus natural es-MX copy.  No Thai or clothing
        fallback exists — structural failure after one auto-revise raises.
        """
        from services.photo_wig_planner import normalize_wig_choice_plan

        planning_paths = list(dict.fromkeys(
            [str(Path(value).resolve()) for value in persona_paths]
            + [str(Path(value).resolve()) for value in reference_paths]
        ))
        if not planning_paths or any(
                not Path(value).is_file() for value in planning_paths):
            raise PhotoReferenceVisionError("假发规划缺少人物/参考图片")
        persona_sha = [
            hashlib.sha256(Path(value).read_bytes()).hexdigest()
            for value in persona_paths
        ]
        reference_sha = [
            hashlib.sha256(Path(value).read_bytes()).hexdigest()
            for value in reference_paths
        ]
        input_contract = {
            "prompt_version": self.MX_WIG_PLAN_PROMPT_VERSION,
            "persona_sha256": persona_sha,
            "reference_sha256": reference_sha,
            "content_requirement": content_requirement,
            "count": count, "theme_label_zh": theme_label_zh,
            "choice_axis": choice_axis,
            "planning_route": self.travel_planning_signature(),
        }
        folder = self.root / "reference_contracts" / self._safe(record_id)
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / "wig_plan.json"
        input_sha256 = _hash(input_contract)
        if path.is_file():
            cached = json.loads(path.read_text(encoding="utf-8"))
            if cached.get("input_sha256") != input_sha256:
                raise PhotoReferenceVisionError(
                    "主题、人物或参考图已变化；请新建任务避免混用旧假发计划"
                )
            return dict(cached["plan"])
        base_prompt = self._wig_plan_prompt(
            theme_label_zh=theme_label_zh, choice_axis=choice_axis,
            content_requirement=content_requirement, count=count,
            image_count=len(planning_paths),
        )
        response, model_routing = self._travel_planning_chat(
            self._model_images(planning_paths), base_prompt, max_tokens=3800,
        )
        raw = parse_vision_envelope(response)
        plan, errors = normalize_wig_choice_plan(
            raw, count=count, choice_axis=choice_axis,
        )
        if errors:
            revise_prompt = (
                base_prompt
                + "\n\n上一次输出存在以下结构错误，必须全部修正后重新输出完整 JSON：\n- "
                + "\n- ".join(errors)
            )
            response, model_routing = self._travel_planning_chat(
                self._model_images(planning_paths), revise_prompt, max_tokens=3800,
            )
            raw = parse_vision_envelope(response)
            plan, errors = normalize_wig_choice_plan(
                raw, count=count, choice_axis=choice_axis,
            )
            if errors:
                raise PhotoReferenceVisionError(
                    "假发内容计划两次未通过结构校验：" + "；".join(errors[:6])
                )
        plan["model"] = model_routing["actual_model"]
        plan["model_routing"] = dict(model_routing)
        plan["prompt_version"] = self.MX_WIG_PLAN_PROMPT_VERSION
        payload = {
            "schema_version": "opv-photo-mx-wig-plan-store-v1",
            "record_id": record_id, "input_sha256": input_sha256,
            "input_contract": input_contract, "plan": plan,
        }
        temporary = path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        temporary.replace(path)
        return plan

    @staticmethod
    def _wig_plan_prompt(*, theme_label_zh: str, choice_axis: str,
                         content_requirement: str, count: int,
                         image_count: int) -> str:
        return f"""你是墨西哥西语（es-MX）假发账号的原生图文策划。为“四选一发型”帖子输出 {count} 篇互相有差异的内容计划 JSON。

输入图片共 {image_count} 张：第一张是人物身份参考（只代表人物的脸，不代表目标发型）；其余（若有）是发型/环境/风格灵感，只取其标注用途。

每篇固定结构（角色不可改变）：
- hair_a：第 1 页，同时是首图；短发方向（如利落短 Bob）。
- hair_b：第 2 页；中长方向（如锁骨长度、柔和弧度）。
- hair_c：第 3 页；长直方向。
- hair_d：第 4 页；长波浪方向，并承载选择 CTA。

硬性要求：
1. 四个选项必须肉眼可辨：length/texture/color/parting/silhouette 至少三项不同；同一篇内不得出现实质重复的发型。
2. 每个选项给 label_es（≤22 字符的西语发型名，如 "Bob corto"）和中文描述字段（*_zh）供生图 prompt 使用。
3. 文案全部为自然墨西哥西语：title ≤72 字符（自然提问句），caption 2-3 句（描述这组选择，不提价格/促销/生发/真人发），hashtags 3-5 个（# 开头），slide_texts 恰好 4 条且每条 ≤28 字符。
4. slide_texts[0] 是主题短标题并以 "· A" 结尾（小号 A 标识）；slide_texts[1..3] 分别以 "B · "、"C · "、"D · " 开头；slide_texts[3] 结尾含 "¿A, B, C o D?"。
5. 不出现中文、泰文或英语句子；不用 {{label_x}} 占位符。
6. 多篇之间轮换选项差异轴（卷度/发色/分缝/造型细节），不得产出四篇完全相同的选项组合。
7. 只根据图片里真实可见的信息使用参考图，不虚构品牌或商品。

默认主题：{theme_label_zh or "周末换个发型，你选哪款？"}（theme_key 固定为 MX_WEEKEND_HAIR_CHOICE）。选择轴：{choice_axis}。
{("运营补充要求（必须遵守）：" + content_requirement) if content_requirement else ""}

只输出一个 JSON 对象，结构：
{{
  "theme_key": "MX_WEEKEND_HAIR_CHOICE",
  "locale": "es-MX",
  "choice_axis": "{choice_axis}",
  "photography_direction_zh": "整篇统一的摄影方向（中文，给生图用）",
  "items": [
    {{
      "item_index": 1,
      "topic_zh": "本篇主题方向（中文）",
      "photography_direction_zh": "…",
      "wardrobe_direction_zh": "…",
      "makeup_direction_zh": "…",
      "scene_prompt_zh": "…",
      "reference_uses": [{{"image_index": 1, "uses": ["persona"]}}, {{"image_index": 2, "uses": ["hair_inspiration"]}}],
      "options": [
        {{"role": "hair_a", "label_es": "…", "label_zh": "…", "length": "chin_bob", "length_zh": "…", "texture": "straight", "texture_zh": "…", "color_zh": "…", "parting_zh": "…", "silhouette_zh": "…", "framing_zh": "…", "bangs_zh": "…", "note_zh": "…"}},
        {{"role": "hair_b", "…": "…"}},
        {{"role": "hair_c", "…": "…"}},
        {{"role": "hair_d", "…": "…"}}
      ],
      "copy": {{
        "title": "…",
        "caption": "…",
        "hashtags": ["#…"],
        "slide_texts": ["…· A", "B · …", "C · …", "D · … ¿A, B, C o D?"]
      }}
    }}
  ]
}}"""

    def review_wig_group(
        self, *, persona_image_path: str, sources: Sequence[Mapping[str, Any]],
        plan_item: Mapping[str, Any],
    ) -> dict[str, Any]:
        """One group-level visual check for the four generated hair options.

        Hard failures only (face swap/deformity, severe hair-face artifacts,
        substantively duplicated options, core hairstyle mismatch); small
        deviations are recorded as notes, never as failures.
        """
        from services.photo_wig_qa import normalize_wig_group_qa

        generated = [str(Path(item["path"]).resolve()) for item in sources]
        paths = [str(Path(persona_image_path).resolve())] + generated
        if any(not Path(value).is_file() for value in paths):
            raise PhotoReferenceVisionError("假发组级质检缺少人物或生成图片")
        options = {str(item.get("role")): item
                   for item in plan_item.get("options") or []}
        page_plans = []
        for item in sources:
            option = options.get(str(item.get("role"))) or {}
            page_plans.append({
                "role": str(item.get("role")),
                "label_es": str(option.get("label_es") or ""),
                "length_zh": str(option.get("length_zh") or option.get("length") or ""),
                "texture_zh": str(option.get("texture_zh") or option.get("texture") or ""),
                "color_zh": str(option.get("color_zh") or option.get("color") or ""),
                "silhouette_zh": str(option.get("silhouette_zh") or ""),
            })
        prompt = f"""你是严格的图文技术质检引擎。输入图片第 1 张是人物身份参考原图，其后 {len(generated)} 张是按计划生成的发型选项页（顺序如下）。

逐张检查，只把“硬伤”记为失败：
- face_changed：生成页主角的脸与人物参考明显不是同一人（换脸、五官/脸型/肤色/年龄感明显偏离）。
- deformity：明显畸形（手、肢体、眼睛、皮肤塑料感严重）。
- hair_artifact：头发严重粘连脸部/发际线严重失真。
- hairstyle_mismatch：核心发型与该页计划严重不符（长度级别错误、卷度方向相反、发色方向完全不同）。
- duplicate_of：该页发型与另一页实质重复（同一长度+同一卷度+同一轮廓）。

以下情况只记入 notes，不作为失败：自然光导致的肤色明暗差、个别发丝、轻微表情/视线变化、非核心卷度细节、刘海轻微遮挡发际线、背景轻微差异。

页面计划：
{json.dumps(page_plans, ensure_ascii=False, indent=1)}

只输出 JSON：
{{"roles": [{{"role": "hair_a", "passed": true, "hard_flags": [], "notes": "…"}}, …全部四页…], "group_notes": "…"}}
所有 passed 必须是真正的 true/false。"""
        response, _ = self._chat(
            self._model_images(paths), prompt, max_tokens=1600, prefer="fast",
        )
        raw = parse_vision_envelope(response)
        roles = [str(item.get("role")) for item in sources]
        try:
            return normalize_wig_group_qa(raw, roles=roles)
        except PhotoReferenceVisionError as first_error:
            retry_response, _ = self._chat(
                self._model_images(paths),
                prompt + "\n\n上一次结果结构不完整（" + str(first_error)
                + "）。必须重新输出覆盖全部四页的完整 JSON，布尔字段必须是真正的 true/false。",
                max_tokens=1600, prefer="fast",
            )
            raw_retry = parse_vision_envelope(retry_response)
            return normalize_wig_group_qa(raw_retry, roles=roles)

    def review_wig_copy_semantics(
        self, *, copy_block: Mapping[str, Any], plan_item: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Text-only es-MX semantic review (same QA pass, no extra pipeline)."""
        from services.photo_wig_qa import normalize_wig_copy_review

        prompt = f"""你是墨西哥西语（es-MX）母语内容审校。检查下面这条四选一发型帖子的文案是否合格。

文案：
{json.dumps(dict(copy_block), ensure_ascii=False, indent=1)}

本篇计划主题：{plan_item.get('topic_zh') or '周末换个发型，你选哪款？'}；四个选项：{json.dumps([o.get('label_es') for o in plan_item.get('options') or []], ensure_ascii=False)}

职责边界（重要）：格式规范——slide_texts 的 "· A"/"B · "/"C · "/"D · " 前缀、CTA 是否存在、字数上限、选项标签与计划名称是否一致、caption 是否逐条对应选项——全部由程序规则负责并已另行校验，你一律不检查、不评论、不据此判 fail。文字被 28 字上限压缩、CTA 与选项标签同行，都是规范的既定形态。

你只负责以下三类问题（任一命中才 fail）：
1. 语言不是自然的墨西哥西语：整句英语、机翻腔、混入中文/泰文。
2. 主题明显无关：文案讲的不是"从几款发型里做选择"（例如在讲穿搭教程、化妆步骤、价格或促销）。
3. 违禁内容：价格、促销、医疗/生发功效、"100% 真人发"等未经提供的产品事实宣称。

除以上三类之外的一切（措辞偏好、标签简繁、信息完整度）都必须 passed=true。

只输出 JSON：{{"passed": true, "issues": [], "notes": "…"}}；passed 必须是真正的 true/false。"""
        response, _ = self._chat([], prompt, max_tokens=700, prefer="fast")
        raw = parse_vision_envelope(response)
        try:
            return normalize_wig_copy_review(raw)
        except PhotoReferenceVisionError:
            retry_response, _ = self._chat(
                [], prompt + "\n\n上一次结果结构不完整。必须重新输出完整 JSON。",
                max_tokens=700, prefer="fast",
            )
            return normalize_wig_copy_review(parse_vision_envelope(retry_response))

    @staticmethod
    def build_travel_style_profile(analysis: Mapping[str, Any],
                                   travel_plan: Mapping[str, Any], *,
                                   count: int) -> dict[str, Any]:
        """Merge the two-step outputs into the downstream style_profile shape."""
        posts = list(travel_plan.get("posts") or [])[:count]
        sets = []
        for post in posts:
            sets.append({
                "content_angle_zh": post.get("content_angle_zh"),
                "scene_zh": post.get("scene_zh"),
                "palette_zh": post.get("palette_zh"),
                "background_prompt": post.get("background_prompt"),
                "style_modifier": post.get("style_modifier"),
                "looks": list(post.get("looks") or []),
                "copy": dict(post.get("copy") or {}),
                "topic_zh": str(post.get("topic_zh") or ""),
            })
        return {
            **{key: value for key, value in dict(analysis).items()
               if key not in {"schema_version"}},
            "analysis_method": "doubao_seed_2_1",
            "planning_flow": "travel_two_step",
            "travel_variables": dict(travel_plan.get("travel_variables") or {}),
            "travel_contract": dict(travel_plan.get("travel_contract") or {}),
            "recommended_sets": sets,
        }

    def review_travel_final_pages(
        self, *, image_paths: Sequence[str], expected_texts: Sequence[str],
        role_order: Sequence[str],
    ) -> dict[str, Any]:
        """QA final travel pages; four-page output plus legacy five-page batches."""
        images = [str(Path(value).resolve()) for value in image_paths]
        page_count = len(images)
        if (page_count not in {4, 5} or len(expected_texts) != page_count
                or len(role_order) != page_count):
            raise PhotoReferenceVisionError(
                f"旅行最终页面质检必须覆盖新版 4 页或历史 5 页；收到图片 {len(images)}、"
                f"文字 {len(expected_texts)}、角色 {len(role_order)}"
            )
        if any(not Path(value).is_file() for value in images):
            raise PhotoReferenceVisionError("旅行最终页面质检缺少成片文件")
        pages_payload = [
            {"index": index, "role": str(role), "expected_text": str(text)}
            for index, (role, text) in enumerate(zip(role_order, expected_texts), 1)
        ]
        page_description = (
            "以下 4 张是新版最终成片页：第 1 页是 Look A 并直接承担封面，"
            "第 2-4 页依次是 Look B/C/D，没有额外复制的封面页。"
            if page_count == 4 else
            "以下 5 张是历史批次成片页：第 1 页封面，第 2-5 页为 A/B/C/D。"
        )
        prompt = (
            f"你是图文套版终审质检员。{page_description}每页已叠加泰语文字。"
            "逐页对照预期文字检查并只返回 JSON：\n"
            f"{json.dumps(pages_payload, ensure_ascii=False, indent=1)}\n"
            '返回格式：{"pages":[{"index":1,"text_readable":true,'
            '"text_matches_expected":true,"text_clipped":false,"text_garbled":false,'
            '"subject_obscured":false,"issues":[]}],"notes":"中文简述"}\n'
            "判定要求：text_readable=页面文字清晰可读；text_matches_expected=页面文字与"
            "预期文字一致（内容与角色对应，不是别的页的文字）；text_clipped=文字是否被裁切；"
            "text_garbled=是否有乱码变形字符；subject_obscured=文字是否遮挡人物主体或关键穿搭。"
            "所有布尔字段必须为真正的 true/false，issues 用中文短语列出问题。"
        )
        response, _ = self._chat(
            self._model_images(images), prompt, max_tokens=2200, prefer="fast",
        )
        raw = parse_vision_envelope(response)
        if not isinstance(raw, Mapping) or not isinstance(raw.get("pages"), list):
            raise PhotoReferenceVisionError("旅行最终页面质检没有返回逐页结果")
        by_index = {}
        for page in raw["pages"]:
            if isinstance(page, Mapping):
                by_index[int(page.get("index") or 0)] = page
        required_bools = ("text_readable", "text_matches_expected",
                          "text_clipped", "text_garbled", "subject_obscured")
        normalized = []
        for index in range(1, page_count + 1):
            page = by_index.get(index)
            if not isinstance(page, Mapping):
                raise PhotoReferenceVisionError(f"旅行最终页面质检缺少第 {index} 页结果")
            entry = {"index": index, "issues": [str(v) for v in page.get("issues") or []]}
            for field in required_bools:
                value = page.get(field)
                if not isinstance(value, bool):
                    raise PhotoReferenceVisionError(
                        f"旅行最终页面质检第 {index} 页缺少布尔字段 {field}"
                    )
                entry[field] = value
            normalized.append(entry)
        passed = all(
            page["text_readable"] and page["text_matches_expected"]
            and not page["text_clipped"] and not page["text_garbled"]
            and not page["subject_obscured"]
            for page in normalized
        )
        return {
            "schema_version": "opv-photo-travel-final-qa-v1",
            "passed": passed,
            "pages": normalized,
            "notes": str(raw.get("notes") or ""),
        }

    def review_layering_final_pages(
        self, *, image_paths: Sequence[str], expected_texts: Sequence[str],
        role_order: Sequence[str],
    ) -> dict[str, Any]:
        """Reuse the proven overlay QA contract for the five layering pages."""
        if list(role_order) != ["hook", "layer_base", "layer_mid", "layer_outer", "cta"]:
            raise PhotoReferenceVisionError("温度分层最终页面角色顺序无效")
        qa = self.review_travel_final_pages(
            image_paths=image_paths, expected_texts=expected_texts,
            role_order=role_order,
        )
        return {**qa, "schema_version": "opv-photo-layering-final-qa-v1"}

    def _normalize_reference_analysis(self, raw, source_hashes):
        if not isinstance(raw, Mapping):
            raise PhotoReferenceVisionError("参考图分析返回结构无效")
        aggregate = dict(raw.get("aggregate") or {})
        presentation = str(aggregate.get("primary_presentation") or "")
        if presentation not in PRESENTATIONS:
            raise PhotoReferenceVisionError("参考图展示方式识别无效")
        confidence = float(aggregate.get("confidence") or 0)
        if confidence < .65 or confidence > 1:
            raise PhotoReferenceVisionError("参考图视觉分析置信度不足")
        per_reference = list(raw.get("per_reference") or [])
        if len(per_reference) != len(source_hashes):
            raise PhotoReferenceVisionError("视觉模型没有逐张解释全部参考图")
        classified = _classified_reference_summary(per_reference)
        typed_uses = any(item.get("reference_uses") for item in per_reference
                         if isinstance(item, Mapping))
        outfit_palette = list(classified["outfit_reference"].get("palette") or [])
        environment_features = list(
            classified["environment_reference"].get("background_features") or []
        )
        visual_styles = list(
            classified["visual_style_reference"].get("visual_styles") or []
        )
        return {
            "schema_version": "opv-photo-travel-reference-analysis-v2",
            "analysis_method": "doubao_seed_2_1",
            "prompt_version": TRAVEL_PROMPT_VERSION, "model": self.model,
            "source_hashes": list(source_hashes),
            "per_reference": classified["per_reference"], "aggregate": aggregate,
            "outfit_reference": classified["outfit_reference"],
            "environment_reference": classified["environment_reference"],
            "visual_style_reference": classified["visual_style_reference"],
            "layer_progression_reference": classified["layer_progression_reference"],
            "presentation_type": presentation,
            "season": str(aggregate.get("season") or ""),
            "palette": outfit_palette if typed_uses else list(aggregate.get("palette") or []),
            "temperature": str(aggregate.get("temperature") or "neutral"),
            "materials": list(aggregate.get("materials") or []),
            "style_tags": visual_styles if typed_uses else list(aggregate.get("visual_styles") or []),
            "lighting": str(aggregate.get("lighting") or ""),
            "background": str(aggregate.get("background") or ""),
            "climate": str(aggregate.get("climate") or ""),
            "destination_visual_style": str(aggregate.get("destination_visual_style") or ""),
            "background_features": environment_features if typed_uses else [
                str(value) for value in aggregate.get("background_features") or []
                if str(value or "").strip()
            ],
            "avoid_tags": list(aggregate.get("avoid_tags") or []),
            "confidence": confidence,
        }

    def _normalize_travel_plan(self, raw, travel_contract, count,
                               background_features=None,
                               travel_topic: Mapping[str, Any] = None,
                               outfit_reference_indices: Sequence[int] = (),
                               product_context: Mapping[str, Any] = None,
                               locale_pack: Mapping[str, Any] = None):
        if not isinstance(raw, Mapping):
            return {}, ["输出不是 JSON 对象"]
        moments = {str(item.get("key") or ""): item for item in travel_contract.get("moments") or []}
        # Audited moment labels come from the Locale Pack when one is bound; the
        # legacy ``label_th`` table is read through the same resolver otherwise,
        # so TH V2 keeps its exact labels while recipe v2 needs no ``_th`` field.
        from services.photo_locale import travel_moment_labels
        moment_labels = travel_moment_labels(travel_contract, locale_pack=locale_pack)
        footwear_rules = _moment_footwear_rules(travel_contract)
        background_tokens = _background_feature_tokens(background_features)
        posts_raw = raw.get("posts")
        if not isinstance(posts_raw, list) or len(posts_raw) < count:
            return {}, [f"posts 数量不足，需要 {count} 篇"]
        errors: list[str] = []
        posts = []
        for post_index, post in enumerate(posts_raw[:count], 1):
            post = dict(post or {})
            if not str(post.get("content_angle_zh") or "").strip():
                errors.append(f"第 {post_index} 篇缺少内容角度")
            looks = list(post.get("looks") or [])
            if [str(look.get("role") or "") for look in looks] != ROLES:
                errors.append(f"第 {post_index} 篇 looks 必须是有序 look_a..look_d")
                continue
            normalized_looks = []
            signatures = set()
            plan_moments = []
            for look in looks:
                look = dict(look or {})
                moment = str(look.get("travel_moment") or "")
                if moment not in moments:
                    errors.append(f"第 {post_index} 篇 {look.get('role')} 的 travel_moment 不在枚举内：{moment or '缺失'}")
                    continue
                for field in ("scene_prompt", "weather_logic", "outerwear", "top_inner", "bottom", "shoes"):
                    if not str(look.get(field) or "").strip():
                        errors.append(f"第 {post_index} 篇 {look.get('role')} 缺少 {field}")
                signature = "|".join(str(look.get(key) or "") for key in ("outerwear", "top_inner", "bottom", "shoes"))
                if signature in signatures:
                    errors.append(f"第 {post_index} 篇存在重复 Look")
                signatures.add(signature)
                # 改造三：weather_logic 禁止出现具体地点/时段的精确温度。
                if TRAVEL_WEATHER_TEMP_PATTERN.search(str(look.get("weather_logic") or "")):
                    errors.append(
                        f"第 {post_index} 篇 {look.get('role')} 的 weather_logic 出现精确温度，"
                        "只能表达室内外切换/时段温差/穿脱理由，温度只允许文章级温度档"
                    )
                # 改造二：footwear_type 必填枚举 + moment 规则 + 文字矛盾。
                footwear = str(look.get("footwear_type") or "").strip().upper()
                if footwear not in FOOTWEAR_TYPES:
                    errors.append(
                        f"第 {post_index} 篇 {look.get('role')} 的 footwear_type 必须是枚举值："
                        f"{footwear or '缺失'}"
                    )
                else:
                    rules = footwear_rules.get(moment) or {}
                    forbidden = set(rules.get("forbidden") or [])
                    allowed = set(rules.get("allowed") or [])
                    if footwear in forbidden or (allowed and footwear not in allowed):
                        errors.append(
                            f"第 {post_index} 篇 {look.get('role')} 的鞋履 {footwear} 不符合"
                            f"{moment} 场景的步行实用性规则"
                        )
                    detected = _footwear_text_types(look.get("shoes"))
                    # 多属性描述（如"平底低筒皮靴"→{FLAT, LOW_BOOT}）命中声明
                    # 类型即放行；描述类别与声明完全不相交（含跟型冲突）才算矛盾。
                    if detected and footwear not in detected:
                        errors.append(
                            f"第 {post_index} 篇 {look.get('role')} 的 shoes 描述与"
                            f" footwear_type（{footwear}）矛盾：{look.get('shoes')}"
                        )
                # 2026-09-08 背景继承放松：scene_prompt 与参考背景特征零重叠
                # 不再拒绝（改为规划提示里的氛围延续要求 + 记录 actual 特征）。
                # 场景优先级：当页场所合理 → 整篇旅行氛围一致 → 可选地标。
                plan_moments.append(moment)
                selected_indices = []
                valid_outfit_indices = {int(value) for value in outfit_reference_indices or []}
                for value in look.get("outfit_reference_indices") or []:
                    try:
                        selected = int(value)
                    except (TypeError, ValueError):
                        continue
                    if selected in valid_outfit_indices and selected not in selected_indices:
                        selected_indices.append(selected)
                normalized_look = {
                    **look,
                    "footwear_type": footwear,
                    "background_feature_zh": str(look.get("background_feature_zh") or ""),
                    # Audited enum labels only; never trust model-written Thai.
                    "display_label": str(moment_labels.get(moment) or ""),
                    "outfit_reference_indices": selected_indices,
                    "styling_intent": str(look.get("styling_intent") or ""),
                }
                product = dict(product_context or {})
                if str(product.get("product_id") or ""):
                    # 类目→商品槽位映射改由 Category Adapter 提供（Phase 1 等价搬迁），
                    # 未注册的类目只写 target_product、不做槽位覆盖，与搬迁前一致。
                    normalized_look = apply_target_product_to_look(
                        adapter=None, look=normalized_look, product_snapshot=product,
                    )
                normalized_looks.append(normalized_look)
            topic_active = bool(travel_topic and travel_topic.get("theme_type"))
            if (len(plan_moments) == len(ROLES)
                    and len(set(plan_moments)) != len(ROLES) and not topic_active):
                errors.append(f"第 {post_index} 篇四个 travel_moment 必须互不相同")
            post["looks"] = normalized_looks
            if topic_active:
                topic_zh = str(post.get("topic_zh") or "").strip()
                copy_block = post.get("copy") if isinstance(post.get("copy"), Mapping) else {}
                place = str((travel_topic or {}).get("place") or "").strip()
                place_localized = str(copy_block.get("place_localized") or "").strip()
                title = str(copy_block.get("title") or "").strip()
                caption = str(copy_block.get("caption") or "").strip()
                hashtags = [str(v) for v in copy_block.get("hashtags") or []]
                slide_texts = [str(v).strip() for v in copy_block.get("slide_texts") or []]
                # 发布语言纯度按绑定的 Locale Pack 判定；未绑定时仍是 th-TH
                # 契约，TH V2 行为逐字不变（review 修复 P0-2）。
                copy_locale = str((locale_pack or {}).get("locale") or "") or "th-TH"
                copy_valid = bool(
                    title and caption and len(slide_texts) == 5 and all(slide_texts)
                    and not copy_locale_issues({
                        "title": title,
                        "caption": caption,
                        "hashtags": hashtags,
                        "slide_texts": slide_texts,
                    }, copy_locale)
                )
                if not copy_valid:
                    # 方案 §7.3：文案结构无效→同语言模板降级，不阻塞规划与生图；
                    # 单页说明缺失退化为 Look 名称。绑定 Locale Pack 时模板取自
                    # 该发布语言自身，未绑定时沿用主题内联泰语模板。
                    template = _travel_copy_template(
                        locale_pack, post_index, travel_topic, topic_zh)
                    look_label = str(template.get("look_label") or "{letter}")
                    localized_labels = [
                        str(look.get("display_label") or look_label.replace("{letter}", letter))
                        for look, letter in zip(normalized_looks, "ABCD")
                    ]
                    slide_texts = [
                        str(template.get("cover") or ""),
                        f"A · {localized_labels[0]}",
                        f"B · {localized_labels[1]}",
                        f"C · {localized_labels[2]}",
                        f"D · {localized_labels[3]}\n{str(template.get('cta') or '')}",
                    ]
                    title = title or str(template.get("title") or "")
                    caption = caption or str(template.get("caption") or "")
                    hashtags = hashtags or [str(v) for v in template.get("hashtags") or []]
                    post["copy_degraded"] = True
                if place:
                    if not place_localized:
                        errors.append(
                            f"第 {post_index} 篇缺少当地语言地点名 place_localized（{place}）"
                        )
                    elif place_localized not in title or place_localized not in (
                            slide_texts[0] if slide_texts else ""):
                        errors.append(
                            f"第 {post_index} 篇发布标题和封面必须同时包含地点名 "
                            f"{place_localized}"
                        )
                post["topic_zh"] = topic_zh
                post["copy"] = {
                    "place_localized": place_localized,
                    "title": title,
                    "caption": caption,
                    "hashtags": hashtags,
                    "slide_texts": slide_texts,
                    "language_review_status": (
                        "DRAFT_FALLBACK" if post.get("copy_degraded")
                        else "DRAFT_TRAVEL_TOPIC"
                    ),
                }
            posts.append(post)
        if errors:
            return {}, errors
        return {
            "schema_version": "opv-photo-travel-plan-v1",
            "prompt_version": TRAVEL_PROMPT_VERSION, "model": self.model,
            "travel_variables": dict(raw.get("travel_variables") or {}),
            "travel_contract": dict(travel_contract or {}),
            "posts": posts,
        }, []

    @staticmethod
    def _reference_analysis_prompt(*, theme, category_key, content_requirement):
        return f"""你是图文参考图分析员。逐张描述参考图中的服装、人物造型、色调、材质、风格与背景环境；只描述画面可见事实，不推断画面外信息。
不要规划穿搭，不要决定旅行场景，不要生成任何文案。
业务类目：{category_key}
固定主题：{theme.get('label_zh') or theme.get('theme_key')}
运营补充要求：{content_requirement or '无'}
必须区分：FLAT_LAY=无人物服装平铺；MODEL_FULL_BODY=真人纯色/简单背景；SCENE_MODEL=真人生活场景；EDITORIAL_COLLAGE=信息卡或拼贴。

每张参考图的用途可以多选：OUTFIT=借鉴服装审美、版型比例、层次、配色关系和穿法；ENVIRONMENT=借鉴建筑、街道、景观和场所；VISUAL_STYLE=借鉴光线、色调、构图和摄影氛围。运营补充要求中明确指定“图1/第一张”等用途时必须优先执行并标记 use_source=operator，否则根据画面判断并标记 auto。服装风格（韩系/日系/法式等）属于 OUTFIT，不等同于摄影风格。
- OUTFIT 图需额外概括 outfit_formula、palette_relation、styling_details；不要求复制同款。
- ENVIRONMENT 图中的服装不得进入穿搭依据；OUTFIT 图的背景不得进入环境依据。
- 一张图可同时承担多个用途。

背景描述要求：
- 每张图的 background_cues 用中文名词短语列出背景中可见的具体特征，覆盖以下类别中画面实际出现的：气候与地面（积雪街道、石板路、沙滩）、水体（海面、河流）、植被（落叶乔木、棕榈）、建筑（红砖欧式老楼、玻璃幕墙航站楼）、地标物（灯塔、木栈桥、风车、雪山）、光线氛围（暖金色夕晒、阴天柔光）；
- 只写看得见的，每条不超过 12 个字，禁止出现品牌名、店铺名或画面文字；
- aggregate.background_features 合并全部图片出现过的背景特征并去重，出现张数多的排在前面；
- aggregate.climate 用一个词概括背景气候气质，常见值：snow / coastal / urban_old_town / suburban / alpine / subtropical / indoor，选最接近的；
- aggregate.destination_visual_style 用 20 字内一句话概括整体背景体系，例如"北欧雪国小镇街道""地中海海滨度假小镇"。

只返回 JSON 对象：
{{
  "per_reference":[{{"index":1,"reference_uses":["ENVIRONMENT","VISUAL_STYLE"],"use_source":"auto","presentation":"SCENE_MODEL","garment_cues":[],"palette_cues":[],"material_cues":[],"style_cues":[],"background_cues":["积雪的街道","木质路灯"],"outfit_formula":"","palette_relation":"","styling_details":"","lighting":"","composition":"","notes_zh":""}}],
  "aggregate":{{"primary_presentation":"SCENE_MODEL","confidence":0.0,"visual_styles":[],"season":"","palette":[],"temperature":"","materials":[],"lighting":"","background":"","climate":"snow","destination_visual_style":"北欧雪国小镇街道","background_features":["积雪的街道","木质路灯"],"avoid_tags":[]}}
}}"""

    @staticmethod
    def _travel_plan_prompt(*, analysis, travel_contract, variables, content_requirement, count,
                            travel_topic=None, product_context=None, locale_pack=None):
        ordered_moments = sorted(
            list(travel_contract.get("moments") or []),
            key=lambda item: str(item.get("key") or "") == "airport_departure",
        )
        # Label values always come through the locale layer.  With no pack bound
        # this reads the legacy ``label_th`` table and keeps the exact Thai
        # prompt wording; a bound pack switches to neutral wording so a VN task
        # is not told its labels are Thai.
        from services.photo_locale import travel_moment_labels
        moment_labels = travel_moment_labels(travel_contract, locale_pack=locale_pack)
        label_caption = "固定泰语标签" if locale_pack is None else "固定发布语言标签"
        moments_text = "\n".join(
            f"- {item.get('key')}：{item.get('label_zh')}"
            f"（画面证据：{item.get('evidence_zh')}；{label_caption}："
            f"{moment_labels.get(str(item.get('key') or ''), '')}；"
            f"步行强度：{item.get('mobility_level') or '未标注'}；"
            f"禁用鞋型：{'、'.join(item.get('forbidden_footwear_types') or []) or '无'}）"
            for item in ordered_moments
        )
        topic = dict(travel_topic or {})
        product = dict(product_context or {})
        topic_theme_type = str(topic.get("theme_type") or "")
        outfit_indices = list(
            (dict(analysis).get("outfit_reference") or {}).get("indices") or []
        )
        topic_block = ""
        copy_rules = (
            "9. 主题联动分支必须同时输出 topic_zh 与 copy（结构见上）；"
            "title/caption/逐页说明围绕同一选题和地点。"
            if topic_theme_type else
            "9. 不要生成标题、正文或 CTA 文案。"
        )
        moment_rule = (
            "；主题联动分支可共用同一 key" if topic_theme_type
            else "，每篇四个必须互不相同"
        )
        same_moment_rule = (
            "主题联动分支允许四套共用同一个 travel_moment"
            "（在选定地点内规划不同画面），系统以 scene_prompt 区分四页；"
            if topic_theme_type else "每套绑定一个不同的 travel_moment；"
        )
        if topic_theme_type:
            topic_block = (
                "\n【旅行主题联动】主题类型：{tt}；地点：{place}；"
                "规划重点：{focus}\n"
                "本任务启用主题联动分支：\n"
                "- 允许四套 Look 使用同一个 travel_moment（都发生在选定地点/同一场所类型内）；"
                "scene_prompt 按每套穿搭自然安排画面，不为凑差异强制四种机位或四类活动；\n"
                "- 场所必须符合选定地点与主题；travel_moment 选取要与选定地点的合理活动匹配（如自然景区优先观景、漫步、咖啡等场所，不强行使用机场、商场等城市型场所）；允许室内、近景或被遮挡的视角；具体地标不必每页出现；\n"
                "- 运营补充要求与地点描述只用于场景与穿搭语境，内容市场语言仍按既定市场输出。\n"
                "\n【发布文案（主题联动必须生成）】以四套 Look 为依据，同时输出：\n"
                '{{"topic_zh":"中文选题（可用主题句式，填入地点）",'
                '"copy":{{"place_localized":"目标市场常用地点名","title":"当地语言发布标题","caption":"当地语言发布正文",'
                '"hashtags":["当地语言标签"],"slide_texts":["两行短封面","A · 当地语言短名称","B · 当地语言短名称","C · 当地语言短名称","D · 当地语言短名称\\n完整选择 CTA"]}}}}\n'
                "文案要求：title、caption、封面必须由同一个具体选题驱动。title 用地点加一个明确的穿搭问题或利益点，"
                "不能退化成‘某地 4 套穿搭’；caption 用一两句补充为什么这些搭配适合该地点/主题，不机械复述 A/B/C/D 名称。"
                "只有当四张最终画面都明确支持某种审美风格时，title/caption 才能写法式、学院风等风格名。"
                "slide_texts 只用于图片排版，必须短、完整、易扫读。slide_texts 必须 5 条且顺序为封面+A/B/C/D："
                "封面固定两层信息，第一行是 place_localized，第二行是与 topic_zh 对应的短钩子；A-C 每页只写一个当地语言造型短名称，不编造英文杂志式名称；"
                "第 5 页固定两行，第一行是 D 的短名称，第二行只写简短的 A/B/C/D 选择 CTA，不重复四套名称。禁止省略号和不完整选项。\n"
                "标题规则（最重要）：title 与封面必须包含 place_localized（目标市场常用地点名，"
                "如富士山→ภูเขาไฟฟูจิ、河口湖→คาวากุจิโกะ、浅草寺→วัดอาซากุสะ、涩谷→ชิบุยะ），"
                "地点是旅行内容最大的吸引力点，绝不允许只写泛泛的\u201c旅行穿搭\u201d。\n"
            ).format(tt=topic_theme_type, place=topic.get("place") or "未指定（使用参考图目的地氛围，不猜测具体地名）",
                     focus=topic.get("planning_focus") or "")
        # 旅行温度增强：只有 TEMPERATURE 主题渲染体感块，其余五个旅行主题的
        # 提示词与 v8 完全一致（体感指引来自 travel_theme_templates.json）。
        sensitivity_block = ""
        if topic_theme_type == "TEMPERATURE":
            sensitivity_planning = dict(topic.get("thermal_sensitivity_planning") or {})
            modifiers = {
                str(key): str(value)
                for key, value in dict(sensitivity_planning.get("modifiers") or {}).items()
            }
            sensitivity = str(
                topic.get("thermal_sensitivity")
                or sensitivity_planning.get("default") or ""
            )
            modifier = modifiers.get(sensitivity, "")
            if modifier:
                constraints = [
                    str(item) for item in sensitivity_planning.get("constraints") or []
                    if str(item or "").strip()
                ]
                sensitivity_block = (
                    "\n【体感倾向（仅温度主题消费，作用于整篇文章）】"
                    f"体感：{sensitivity}；同温度档下的调整方向：{modifier}\n"
                    + ("体感规则：\n" + "".join(f"- {item}\n" for item in constraints)
                       if constraints else "")
                )
        difference_rule = (
            "四套 Look 要提供读者第一眼就能感知的不同搭配选择；区别主要来自整体轮廓、配色关系、"
            "层次或穿法。只更换场景、姿势、面料名称、配饰或相近颜色称呼，不算新的搭配方向。"
            "允许复用协调的鞋履、成熟裤型和指定商品，不为凑字段数量拆散好看的组合。"
            "输出前横向比较四套；若两套第一眼近似，只调整其中一套最有必要的搭配部分，并同步相关文案。"
            + (
                "从完整参考搭配出发，保留好看的长短/宽窄比例、层次、配色关系和穿法；"
                "每套在 outfit_reference_indices 中填写主要借鉴的参考图序号（可复用），"
                "并用 styling_intent 说明保留什么；"
                if outfit_indices else ""
            )
        )
        topic_schema = (
            '{{"travel_variables":{{}},"posts":[{{"content_angle_zh":"","scene_zh":"","palette_zh":"","background_prompt":"","style_modifier":"","topic_zh":"中文选题（填入地点）","copy":{{"place_localized":"目标市场常用地点名","title":"当地语言发布标题","caption":"当地语言发布正文","hashtags":["当地语言标签"],"slide_texts":["两行短封面","A · 当地语言短名称","B · 当地语言短名称","C · 当地语言短名称","D · 当地语言短名称\\n完整选择 CTA"]}},"looks":[\n'
            '{{"role":"look_a","travel_moment":"old_town_walk","scene_prompt":"中文场景描述","weather_logic":"中文逻辑（无具体温度）","display_label":"","footwear_type":"SNEAKER","background_feature_zh":"该场景延续的参考背景特征","outfit_reference_indices":[],"styling_intent":"","outerwear":"","top_inner":"","bottom":"","shoes":"","outerwear_type":"","bottom_type":""}},\n'
            '{{"role":"look_b","travel_moment":"shopping_day","scene_prompt":"","weather_logic":"","display_label":"","footwear_type":"LOAFER","background_feature_zh":"","outfit_reference_indices":[],"styling_intent":"","outerwear":"","top_inner":"","bottom":"","shoes":"","outerwear_type":"","bottom_type":""}},\n'
            '{{"role":"look_c","travel_moment":"cafe_visit","scene_prompt":"","weather_logic":"","display_label":"","footwear_type":"LOW_HEEL","background_feature_zh":"","outfit_reference_indices":[],"styling_intent":"","outerwear":"","top_inner":"","bottom":"","shoes":"","outerwear_type":"","bottom_type":""}},\n'
            '{{"role":"look_d","travel_moment":"evening_stroll","scene_prompt":"","weather_logic":"","display_label":"","footwear_type":"FLAT","background_feature_zh":"","outfit_reference_indices":[],"styling_intent":"","outerwear":"","top_inner":"","bottom":"","shoes":"","outerwear_type":"","bottom_type":""}}]}}]}}'
            if topic_theme_type else
            '{{"travel_variables":{{}},"posts":[{{"content_angle_zh":"","scene_zh":"","palette_zh":"","background_prompt":"","style_modifier":"","looks":[\n'
            '{{"role":"look_a","travel_moment":"old_town_walk","scene_prompt":"中文场景描述","weather_logic":"中文逻辑（无具体温度）","display_label":"","footwear_type":"SNEAKER","background_feature_zh":"该场景延续的参考背景特征","outfit_reference_indices":[],"styling_intent":"","outerwear":"","top_inner":"","bottom":"","shoes":"","outerwear_type":"","bottom_type":""}},\n'
            '{{"role":"look_b","travel_moment":"shopping_day","scene_prompt":"","weather_logic":"","display_label":"","footwear_type":"LOAFER","background_feature_zh":"","outfit_reference_indices":[],"styling_intent":"","outerwear":"","top_inner":"","bottom":"","shoes":"","outerwear_type":"","bottom_type":""}},\n'
            '{{"role":"look_c","travel_moment":"cafe_visit","scene_prompt":"","weather_logic":"","display_label":"","footwear_type":"LOW_HEEL","background_feature_zh":"","outfit_reference_indices":[],"styling_intent":"","outerwear":"","top_inner":"","bottom":"","shoes":"","outerwear_type":"","bottom_type":""}},\n'
            '{{"role":"look_d","travel_moment":"evening_stroll","scene_prompt":"","weather_logic":"","display_label":"","footwear_type":"FLAT","background_feature_zh":"","outfit_reference_indices":[],"styling_intent":"","outerwear":"","top_inner":"","bottom":"","shoes":"","outerwear_type":"","bottom_type":""}}]}}]}}'
        )
        return f"""你是旅行穿搭内容规划器。基于参考图分析与旅行变量，规划 {count} 篇四选一旅行穿搭。
【参考图分析】{json.dumps(dict(analysis), ensure_ascii=False)}
【旅行变量】{json.dumps(dict(variables), ensure_ascii=False)}
【指定商品】{json.dumps(product, ensure_ascii=False) if product else '无；可自由规划完整穿搭'}
【运营补充要求】{content_requirement or '无'}
【旅行场景枚举（travel_moment 只能取以下 key{moment_rule}）】
{moments_text}{topic_block}{sensitivity_block}
规则：
1. 每篇 looks 必须是有序 look_a..look_d；{same_moment_rule}
2. {difference_rule}
2.1 如有指定商品，四套都必须保留该商品，只改变配套单品、穿法或场景；商品颜色、版型与结构以商品参考图为唯一权威，不得根据商品名称或普通穿搭参考猜测；对应商品字段写“指定商品（以商品参考图为准）”，穿搭参考中的同类单品不得替换指定商品；
3. 背景延续（按优先级）：当页场所合理 → 整篇旅行氛围一致 → 可选地标与景观。延续参考图的旅行氛围、季节与环境气质（气候/地域/建筑气质），travel_moment 决定场所类型；整组只需 1-2 页清晰展示代表性地标，其余页可用街道、湖畔、店内、局部景观等延续氛围，具体地标和景观不必在每页出现。background_feature_zh 字段列出该场景实际延续的参考背景特征（没有就留空，不强制文字重叠）；
4. scene_prompt 用中文具体描述该 Look 的独立场景画面，必须包含枚举中的画面证据要素（如机场：航站楼、行李箱、登机区域）；四页自然变化人物动作、视线方向与景别，避免全部正面站立微笑；不为差异强制固定机位配额。以当页冻结的 scene_prompt 为最终画面依据；
5. weather_logic 只能用中文说明室内外切换、白天/傍晚温差、防风、方便穿脱、步行舒适等搭配理由；禁止出现任何具体温度数字（如 20°C、16度、21℃）；温度只以文章级温度档呈现；
6. 穿搭单品用中文具体描述（外套/内搭/下装/鞋履），符合温度档与参考图风格；display_label 不要自己编写，系统会按枚举固定泰语标签；
6.1 下装和鞋履必须联合规划：先确定整套轮廓，再决定裤型或裙型、材质垂坠感、下装长度、鞋口位置、鞋面和鞋底体量。舒适度是旅行条件之一，不能代替搭配审美。避免无意的裤脚堆积、腰胯鼓包、裙摆与靴口之间形成笨重截断或上下同时过度膨胀；允许有意的宽松轮廓、长裤覆盖鞋面和厚底搭配，不把露脚踝、尖头鞋或某一种裤型设为统一答案。styling_intent 用一句具体话说明为何选择该鞋，以及它怎样承接裙摆或裤脚；禁止只写“整体协调、显高显瘦、适合旅行”等空泛结论；
7. footwear_type 必须从枚举 SNEAKER/LOAFER/FLAT/MARY_JANE/LOW_BOOT/LOW_HEEL/HIGH_HEEL/STILETTO/SANDAL 中选择，并与 shoes 中文描述一致。参考图中的鞋履只能作为审美参考；机场、老城步行、傍晚散步等高步行场景必须优先使用舒适可行走鞋履，不能照搬细跟高跟鞋；
8. 不同篇的内容角度和单品组合必须有明显差异；
8.1 机场不是默认开场或必选场景。只有选题/运营要求明确涉及机场、出发日或飞行穿搭时才使用 airport_departure；Look A 优先直接表达选定地点、主题和穿搭亮点；
{copy_rules}

只返回 JSON 对象：
{topic_schema}
posts 数量必须等于 {count}。不要输出 Markdown。"""

    @staticmethod
    def _layering_qa_prompt(*, roles, band_key, reference_count, look_plans=(),
                            allowed_item_types=(), forbidden_item_types=()):
        return f"""你是温度分层穿搭的视觉观察员。输入图片中前 {reference_count} 张是参考图，后 {len(roles)} 张依次对应 {list(roles)}。
只报告画面可见事实，不自行决定通过或失败。温度档：{band_key}。
冻结计划：{json.dumps(list(look_plans or []), ensure_ascii=False)}
observed_item_types 允许枚举：{list(allowed_item_types)}
禁用单品枚举：{list(forbidden_item_types)}

要求：
1. 三张必须分别观察人物身份、机位/构图、全身完整度和可见上身服装层。
2. garment_signature 是本组三图内稳定的短 ID；同一件衣服跨页必须使用同一个 ID。
3. visible_layer_stack 按由内到外排列，只包含视觉上有证据的上身层；裤装、鞋、包、帽、围巾等配件不得算作新增上身层。
4. collar_compatible 表示本页相邻层领口关系可叠；sleeve_conflict/hem_conflict 只在出现明显冲突时为 true。
5. layer_evidence 至少写一条具体可见证据；所有布尔字段必须是真正的 true/false。
6. observed_item_types 必须只使用上面的允许/禁用枚举中最接近的值，不得自造同义词。
7. temperature_claim_matches 只判断画面是否支持该温度档的参考性穿搭，不得声称保证保暖。

只返回 JSON：
{{"thermal_index":0,"notes":"","pages":[
{{"role":"base","observed_band":"{band_key}","visible_layer_count":1,"visible_layer_stack":[{{"garment_signature":"core_base"}}],"observed_item_types":["shirt"],"layer_evidence":["可见一件基础上衣"],"identity_id":"person_1","camera_signature":"camera_1","full_body":true,"collar_compatible":true,"sleeve_conflict":false,"hem_conflict":false,"temperature_claim_matches":true,"person_flags":{{"face_or_limb_deformity":false,"obvious_unnatural_tilt":false}},"repair_instruction":""}}
]}}
pages 必须完整覆盖且只覆盖 {list(roles)}，不得输出 Markdown。"""

    @staticmethod
    def _thermal_transition_qa_prompt(*, roles, transition_key, contexts,
                                      reference_count, look_plans=(),
                                      allowed_item_types=(), forbidden_item_types=()):
        context_list = list(contexts)
        return f"""你是日常冷热切换穿搭的视觉观察员。输入图片中前 {reference_count} 张是参考图，后 {len(roles)} 张依次对应 {list(roles)}。
只报告画面可见事实，不自行决定通过或失败。切换场景：{transition_key}。
体感顺序（必须按此顺序逐页对应）：{context_list}
冻结计划：{json.dumps(list(look_plans or []), ensure_ascii=False)}
observed_item_types 允许枚举：{list(allowed_item_types)}
禁用单品枚举：{list(forbidden_item_types)}

要求：
1. 三张必须分别观察人物身份、机位/构图、全身完整度和可见上身服装层。
2. observed_thermal_context 只能使用上面体感顺序里的枚举值，必须与所在页一一对应。
3. garment_signature 是本组三图内稳定的短 ID；同一件衣服跨页必须使用同一个 ID。
4. visible_layer_stack 按由内到外排列，只包含视觉上有证据的上身层；裤装、鞋、包、帽、围巾等配件不得算作新增上身层。
5. observed_base_signature / observed_bottom_signature / observed_shoes_signature 分别是本页基础上衣、下装、鞋的稳定短 ID；三者跨页必须一致才代表同一套基础穿搭。
6. added_garment_visible：本页是否真的能看见比上一态多出的那一件可穿上身层；base 页也填 true。
7. collar_compatible 表示本页相邻层领口关系可叠；sleeve_conflict/hem_conflict 只在出现明显冲突时为 true。
8. layer_evidence 至少写一条具体可见证据；所有布尔字段必须是真正的 true/false。
9. temperature_claim_matches 只判断画面是否支持该环境下的参考性穿搭，不得声称保证保暖或保证降温。
10. temperature_claim_text 只填画面上真实出现过的温度文字（没有就留空字符串），不要自己编造温度数字。

只返回 JSON：
{{"thermal_index":0,"notes":"","pages":[
{{"role":"base","observed_thermal_context":"{context_list[0] if context_list else ''}","visible_layer_count":1,"visible_layer_stack":[{{"garment_signature":"core_base"}}],"observed_item_types":["breathable_top"],"layer_evidence":["可见一件透气基础上衣"],"identity_id":"person_1","camera_signature":"camera_1","full_body":true,"collar_compatible":true,"sleeve_conflict":false,"hem_conflict":false,"temperature_claim_matches":true,"temperature_claim_text":"","observed_base_signature":"core_base","observed_bottom_signature":"bottom_main","observed_shoes_signature":"shoes_main","added_garment_visible":true,"person_flags":{{"face_or_limb_deformity":false,"obvious_unnatural_tilt":false}},"repair_instruction":""}}
]}}
pages 必须完整覆盖且只覆盖 {list(roles)}，不得输出 Markdown。"""

    @staticmethod
    def _travel_qa_prompt(*, page_plans, reference_count, image_count,
                          product_reference_count=0, style_reference_count=0,
                          product_context=None, travel_place="",
                          persona_based=False, qa_fields=(), qa_rules=()):
        plans_text = json.dumps(page_plans, ensure_ascii=False, indent=1)
        footwear_enum = "、".join(FOOTWEAR_TYPES)
        identity_clause = (
            "\n人物身份规则：生成图的人物来自系统人物资产（有独立的身份参考与检查），"
            "风格参考图中的人物仅用于提取风格、场景、氛围与穿搭语言。"
            "生成图人物与风格参考图中的人物长相、发型、发色不同是预期行为，"
            "绝不作为任何页面的失败理由，也不要写进 notes 或 repair_instruction。"
            "style_uniform 只评价四页生成图之间的人物是否为同一人、视觉风格是否统一。"
            if persona_based else ""
        )
        product_clause = (
            f"前 {product_reference_count} 张是指定商品参考图，是商品身份的唯一权威；"
            f"随后 {style_reference_count} 张是环境、穿搭或画面风格参考，只提供灵感，不能替换指定商品。"
            if product_reference_count else
            f"前 {reference_count} 张是环境、穿搭或画面风格参考，只提供对应职责的灵感。"
        )
        # 类目声明的逐项商品质检字段（review 修复 P0-3）。womenswear 的
        # qa_fields 为空，下面两项都退化为空串，提示词与历史逐字一致。
        extra_fields = [
            str(name) for name in qa_fields
            if name not in {"role", "product_matches", "repair_instruction"}
        ]
        extra_json = "".join(f'"{name}":true,' for name in extra_fields)
        extra_rules = ""
        if extra_fields:
            extra_rules = (
                "\n【指定商品逐项判定】以下每个字段都必须是真正的布尔值，"
                "任一为 false 即判定该页失败，不得按 MINOR 放行，"
                "并在 repair_instruction 写明具体修复要求：\n"
                + "\n".join(f"- {rule}" for rule in qa_rules)
                + "\n- 本类目的指定商品必须完整出现在画面中；缺失、被遮挡占比过小、"
                  "主色或图案家族不符、边缘或流苏结构错误、长度体积感明显变化、"
                  "遮挡人物面部，一律按失败处理。"
            )
        return f"""你是旅行图文逐页语义质检员。{product_clause}后 {image_count} 张是按顺序对应下列页面计划的生成图。穿搭灵感不要求同款。
指定商品：{json.dumps(dict(product_context or {}), ensure_ascii=False) if product_context else '无'}
指定旅行地点：{travel_place or '无具体地点'}
【页面计划（按生成图顺序）】{plans_text}{identity_clause}
逐页检查并只返回 JSON（本阶段图片没有叠加文字，不要评价标题或文字渲染）：
{{"pages":[{{"role":"look_a","observed_moment":"airport_departure 或枚举 key；看不清就写 unknown","scene_evidence":["画面中实际看见的证据，逐条中文短语"],"product_matches":true,{extra_json}"outfit_matches":true,"outfit_severity":"NONE","weather_matches":true,"mobility_matches":true,"observed_footwear_type":"SNEAKER","person_flags":{{"face_or_limb_deformity":false,"obvious_unnatural_tilt":false,"similar_fixed_smile":false,"similar_gaze":false,"similar_head_pose":false}},"repair_instruction":"中文修复要求，未通过时必填"}}],"style_uniform":true,"destination_conflict":false,"destination_evidence":["画面可见的地点证据"],"notes":"中文简述"}}
person_flags 说明（只报告明显情况，轻微偏差一律 false）：face_or_limb_deformity=明显脸部或肢体畸形；obvious_unnatural_tilt=明显不自然的头部倾斜（轻微歪头算 false）；similar_fixed_smile/similar_gaze/similar_head_pose=该页与另一页出现明显相似的表情/视线/头姿。
判定要求：
- observed_moment 只能使用页面计划里出现过的 travel_moment 枚举 key，无法判断必须写 unknown；
- scene_evidence 必须是字符串数组，逐条写出画面中实际看见的证据要素（如机场的航站楼落地窗、登机箱），不得照抄计划文本，看不见就少写或写"证据不足"；
- product_matches：有指定商品时，检查商品是否保留核心颜色、版型与结构；无商品时填 true。商品明显缺失或被替换才填 false，细微纹理差异不算失败；
- outfit_matches：画面穿搭是否与计划方向一致；outfit_severity 只能是 NONE/MINOR/MAJOR。少一层内搭、配饰、纽扣、鞋型或颜色深浅差异属于 MINOR；主体单品完全缺失或季节/类目彻底错误才属于 MAJOR；
- weather_matches：光线/时段/温度感是否与 weather_logic 一致（如傍晚场景出现强白天阳光则为 false）；
- mobility_matches：鞋履是否适合该场景的步行强度（高步行场景出现细跟鞋应为 false）；observed_footwear_type 从枚举中选最接近的：{footwear_enum}；
- repair_instruction：未通过时必须给出具体修复要求；
- style_uniform：只有四页人物明显换人，或照片风格彻底改变（例如其中一页变成插画）才为 false；室内外、白天傍晚、背景颜色及自然光线差异都应为 true；
- destination_conflict：只有出现与指定地点明显矛盾的地标，或同组混入两个不可能共存的目的地时才为 true；没有地标、室内场景或普通街道不能判冲突。{extra_rules}"""

    def _normalize_contract(
        self, raw, source_hashes, content_requirement, count,
        planning_flow: str = "", required_roles: Sequence[str] = (),
    ):
        if not isinstance(raw, Mapping):
            raise PhotoReferenceVisionError("参考图视觉分析返回结构无效")
        aggregate = dict(raw.get("aggregate") or {})
        presentation = str(aggregate.get("primary_presentation") or "")
        if presentation not in PRESENTATIONS:
            raise PhotoReferenceVisionError("参考图展示方式识别无效")
        confidence = float(aggregate.get("confidence") or 0)
        if confidence < .65 or confidence > 1:
            raise PhotoReferenceVisionError("参考图视觉分析置信度不足")
        sets = list(raw.get("recommended_sets") or [])
        if len(sets) < count:
            raise PhotoReferenceVisionError("视觉模型没有返回足够的内容方案")
        color_plan = _normalize_color_grading_plan(raw.get("color_grading_plan"))
        try:
            frozen_required_roles = resolve_required_roles(
                planning_flow=planning_flow, required_roles=required_roles,
            )
        except PhotoFlowRegistryError as exc:
            raise PhotoReferenceVisionError(str(exc)) from exc
        normalized_sets = []
        valid_reference_indices = set(range(1, len(source_hashes) + 1))
        for index, item in enumerate(sets[:count], 1):
            value = dict(item or {})
            looks = list(value.get("looks") or [])
            try:
                validate_ordered_roles(
                    looks, planning_flow=planning_flow,
                    required_roles=frozen_required_roles,
                )
            except PhotoFlowRegistryError as exc:
                raise PhotoReferenceVisionError(f"第 {index} 篇视觉方案{exc}") from exc
            for look in looks:
                if any(not str(look.get(key) or "").strip() for key in (
                    "display_label", "outerwear", "top_inner", "bottom", "shoes"
                )):
                    raise PhotoReferenceVisionError(f"第 {index} 篇存在不完整穿搭")
                look["palette_hex"] = _normalize_palette_hex(look.get("palette_hex"))
                look["outfit_aesthetic"] = _normalize_outfit_aesthetic(
                    look.get("outfit_aesthetic"))
                look["outfit_reference_indices"] = [
                    int(value) for value in look.get("outfit_reference_indices") or []
                    if str(value).isdigit() and int(value) in valid_reference_indices
                ]
                look["styling_intent"] = str(look.get("styling_intent") or "")
            copy_block = dict(value.get("copy") or {})
            if any(not str(copy_block.get(key) or "").strip() for key in ("title", "cover", "caption", "cta")):
                raise PhotoReferenceVisionError(f"第 {index} 篇缺少泰语文案")
            value["index"] = index
            value["looks"] = [dict(look) for look in looks]
            value["copy"] = copy_block
            normalized_sets.append(value)
        per_reference = list(raw.get("per_reference") or [])
        if len(per_reference) != len(source_hashes):
            raise PhotoReferenceVisionError("视觉模型没有逐张解释全部参考图")
        classified = _classified_reference_summary(per_reference)
        typed_uses = any(item.get("reference_uses") for item in per_reference
                         if isinstance(item, Mapping))
        outfit_palette = list(classified["outfit_reference"].get("palette") or [])
        environment_features = list(
            classified["environment_reference"].get("background_features") or []
        )
        visual_styles = list(
            classified["visual_style_reference"].get("visual_styles") or []
        )
        return {
            "schema_version": "opv-photo-reference-contract-v2",
            "analysis_method": "doubao_seed_2_1",
            "prompt_version": PROMPT_VERSION, "model": self.model,
            "source_hashes": list(source_hashes), "content_requirement": content_requirement,
            "planning_flow": str(planning_flow or "reference_contract_v1"),
            "required_roles": list(frozen_required_roles),
            "per_reference": classified["per_reference"], "aggregate": aggregate,
            "outfit_reference": classified["outfit_reference"],
            "environment_reference": classified["environment_reference"],
            "visual_style_reference": classified["visual_style_reference"],
            "layer_progression_reference": classified["layer_progression_reference"],
            "recommended_sets": normalized_sets,
            "color_grading_plan": color_plan,
            # Compatibility fields consumed by the current planner/generator.
            "presentation_type": presentation,
            "season": str(aggregate.get("season") or ""),
            "palette": outfit_palette if typed_uses else list(aggregate.get("palette") or []),
            "temperature": str(aggregate.get("temperature") or "neutral"),
            "materials": list(aggregate.get("materials") or []),
            "style_tags": visual_styles if typed_uses else list(aggregate.get("visual_styles") or []),
            "lighting": str(aggregate.get("lighting") or ""),
            "background": str(aggregate.get("background") or ""),
            "climate": str(aggregate.get("climate") or ""),
            "destination_visual_style": str(aggregate.get("destination_visual_style") or ""),
            "background_features": environment_features if typed_uses else [
                str(value) for value in aggregate.get("background_features") or []
                if str(value or "").strip()
            ],
            "avoid_tags": list(aggregate.get("avoid_tags") or []),
            "confidence": confidence,
        }

    @staticmethod
    def _analysis_prompt(*, theme, category_key, content_requirement, count,
                         product_context=None, required_roles=()):
        roles = tuple(required_roles or DEFAULT_TRAVEL_SOURCE_ROLES)
        role_sequence = "/".join(roles)
        role_examples = []
        for role in roles:
            role_examples.append({
                "role": role,
                "display_label": "简短自然泰语标签",
                "outfit_reference_indices": [1],
                "styling_intent": "保留参考搭配的比例、层次和穿法，允许更换具体款式",
                "outerwear": "中文具体描述",
                "top_inner": "中文具体描述",
                "bottom": "中文具体描述",
                "shoes": "中文具体描述",
                "outerwear_type": "",
                "bottom_type": "",
                "palette_hex": ["#C9B99A", "#F5F1E8"],
                "outfit_aesthetic": {
                    "harmony": 0, "layering": 0, "color_balance": 0,
                    "proportion": 0, "issues": [], "revise_zh": "",
                },
            })
        output_example = {
            "per_reference": [{
                "index": 1, "reference_uses": ["OUTFIT", "VISUAL_STYLE"],
                "use_source": "auto", "presentation": "SCENE_MODEL",
                "visual_styles": [], "scenes": [], "garment_cues": [],
                "palette_cues": [], "material_cues": [], "background_cues": [],
                "outfit_formula": "", "palette_relation": "",
                "styling_details": "", "lighting": "", "composition": "",
                "layout_cues": [], "notes_zh": "",
            }],
            "aggregate": {
                "primary_presentation": "SCENE_MODEL", "secondary_presentations": [],
                "visual_styles": [], "season": "autumn", "palette": [],
                "temperature": "warm", "materials": [], "scenes": [],
                "garment_cues": [], "accent_cues": [], "lighting": "",
                "background": "", "composition": "", "layout_inspiration": [],
                "avoid_tags": [], "confidence": 0.0,
            },
            "color_grading_plan": {
                "temperature": "warm_4800k", "saturation": "medium_soft",
                "contrast": "gentle",
                "skin_tone_anchor": "冷白透亮带自然血色，以人物参考图为唯一标准",
                "tone_note_zh": "全组统一暖调自然光，低对比轻饱和",
            },
            "recommended_sets": [{
                "content_angle_zh": "", "scene_zh": "", "palette_zh": "",
                "background_prompt": "", "style_modifier": "",
                "looks": role_examples,
                "copy": {"title": "泰语标题", "cover": "两行以内泰语封面文案",
                         "caption": "自然泰语短文案", "cta": "泰语互动句"},
            }],
        }
        return f"""你是图文穿搭内容的视觉分析与内容规划器。按输入顺序逐张理解参考图，不要使用肤色比例等像素规则猜测。
业务类目：{category_key}
固定主题：{theme.get('label_zh') or theme.get('theme_key')}
运营补充要求：{content_requirement or '无'}
指定商品：{json.dumps(dict(product_context or {}), ensure_ascii=False) if product_context else '无'}
需要规划：{count} 篇，每篇按 {role_sequence} 的顺序输出完整穿搭。
如有指定商品，每个角色必须保留该商品，只借鉴参考图的其他搭配关系；参考图中的同类单品不能替换指定商品。

必须区分：FLAT_LAY=无人物服装平铺；MODEL_FULL_BODY=真人纯色/简单背景；SCENE_MODEL=真人生活场景；EDITORIAL_COLLAGE=信息卡或拼贴。若参考图既有真人又有信息卡，为四选一单页选择最适合展示完整穿搭的 primary_presentation，信息卡仅作为 layout_inspiration。
每张图的 reference_uses 可多选 OUTFIT/ENVIRONMENT/VISUAL_STYLE/LAYER_PROGRESSION。运营补充要求明确指定逐图用途时 use_source=operator，否则自动判断。OUTFIT 只提取服装审美、完整搭配比例/层次/配色关系/穿法；ENVIRONMENT 只提取场所与景观；VISUAL_STYLE 只提取光线、色调、构图与摄影氛围；LAYER_PROGRESSION 只提取层数、累计层栈、人物机位一致性和逐层轮廓变化。环境图服装不得进入搭配依据，穿搭图背景不得进入环境依据。
提取可迁移的风格、场景、构图、服装语言、配色和点缀；禁止复刻人物身份、Logo、水印、来源文字和具体品牌。运营补充要求优先于参考图的非硬事实。
穿搭审美规则：每套（外套/内搭/下装/鞋履）必须是整体协调、有审美水准的成套日常穿搭——主色不超过三种且上下有颜色呼应；鞋型与裤型平衡，避免笨重厚底鞋配阔腿裤的沉重组合；外套与内搭层次清楚；整体显高显瘦、气质干净高级，像会认真搭配的时尚博主，不要随机单品堆叠。规划后按 rubric 自评每一套：harmony（单品协调）、layering（层次）、color_balance（配色平衡）、proportion（显高显瘦比例），0-100 整数，低于 85 必须自行重配并在 revise_zh 说明改法。
全局调色计划：为整组内容制定统一 color_grading_plan（temperature 色温基准 / saturation 饱和度倾向 / contrast 对比度倾向 / skin_tone_anchor 肤色基准说明 / tone_note_zh 一句中文调色说明）；所有篇与每篇所有角色必须共用同一份计划，画面之间不允许色调漂移。

只返回 JSON 对象：
{json.dumps(output_example, ensure_ascii=False)}
recommended_sets 数量必须等于 {count}；不同篇要有明显内容角度和服装差异。palette_hex 每套 2-4 个该套主色（#RRGGBB）。不要输出 Markdown。"""

    @staticmethod
    def _alignment_prompt(*, reference_count, generated_count, contract, scope,
                          generated_roles=None, persona_based=False,
                          product_reference_count=0, product_context=None):
        identity_clause = (
            "\n人物身份规则：生成图的人物来自系统人物资产（有独立的身份参考与检查），"
            "风格参考图中的人物仅用于提取风格、场景、配色与氛围。"
            "生成图人物与风格参考图中的人物长相、发型、发色不同是预期行为，"
            "绝不作为失败理由，也不要写进 notes。"
            if persona_based else ""
        )
        role_clause = ""
        if generated_roles:
            role_clause = (
                f"\n生成结果按顺序对应角色：{', '.join(str(value) for value in generated_roles)}。"
                "\n必须在 JSON 中额外返回 per_look 数组，逐张归因："
                "\"per_look\":[{\"role\":\"look_a\",\"passed\":true,\"issues\":[],\"missing_major_garment\":false}]；"
                "passed 为 false 时 issues 必须用中文短语列出该张的具体问题；无法归因到单张时全部 passed 为 true。"
                "missing_major_garment 仅在该张完全缺失计划中的主体单品（如计划有外套但整张没有外套）时为 true；"
                "款式或颜色细节偏差不算缺失。"
            )
        # 指定商品属于核心商品（review 修复，2026-09-14）：本段宽松边界把"配饰有无"
        # 列为不受罚，那对围巾线是自相矛盾的——围巾本身就是被指定的那件商品。类目
        # 适配器（如 SCARF_V1）已声明主槽位在 accessories，这里按同一语义把指定商品
        # 从"可免责配饰"里摘出来。只有真的指定了商品才追加，且只拦明显的缺失/换款/
        # 主色图案结构错误，围法、褶皱与细微纹理仍按 MINOR 记录，不加新的评分门槛。
        product_clause = ""
        if product_context:
            product_label = str(
                product_context.get("product_name")
                or product_context.get("product_id") or "指定商品"
            )
            product_clause = (
                f"\n【核心商品】指定商品「{product_label}」是本次画面的核心商品，"
                "不是可以免责的普通配饰。"
                + (f"前 {product_reference_count} 张是它的商品参考图。" if product_reference_count
                   else "注意：本轮未随附商品参考图，只能按合同描述判断，不得据此放宽。")
                + "以下三种情况必须判 passed=false 并在 per_look.issues 写明是哪一张："
                "该张完全看不到指定商品；指定商品被换成明显不同的款式或另一颜色家族；"
                "主色、图案家族或结构（如围巾边缘与流苏）明显错误。"
                "指定商品所在槽位之外的普通配饰仍按原宽松规则处理；"
                "围法、褶皱、佩戴位置与细微纹理差异只写 notes，不作为失败理由。"
            )
        # 没有指定商品时保持原文逐字不变（TH 普通配饰线不漂移）。
        if product_reference_count:
            intro = (
                f"前 {product_reference_count} 张是指定商品参考图，"
                f"随后 {reference_count - product_reference_count} 张是原始风格参考，"
                f"后 {generated_count} 张是生成结果。"
            )
        else:
            intro = (
                f"前 {reference_count} 张是原始风格参考，"
                f"后 {generated_count} 张是生成结果。"
            )
        return f"""你是独立图文内容质检员。{intro}直接对照原图判断，不能只相信给定合同。
检查范围：{scope}{identity_clause}{product_clause}
{"本次是首张（Look A）单张灾难门禁：只有出现展示方式错配（真人变平铺/平铺出人物/场景退化为纯色棚拍）、明显肢体畸形、明显多人等灾难级问题时 passed 才为 false；风格、场景氛围、配色倾向、穿搭呈现与参考图的偏差一律写入 notes 供参考，不得作为本次单张的失败理由——这些属于后续整组检查。" if str(scope).startswith("FIRST_LOOK") else ""}
目标合同：{json.dumps(dict(contract), ensure_ascii=False)}
参考用途规则：逐图 reference_uses 是比较边界。ENVIRONMENT 只比较环境，OUTFIT 只比较搭配关系，VISUAL_STYLE 只比较摄影表达；不得用环境图中的衣服或穿搭图中的背景判定失败。穿搭参考不要求同款。
如目标合同包含 product_context，前置的商品参考图用于检查指定商品身份；指定商品被明显替换或核心颜色、版型、结构明显错误时才判失败，细微纹理与配饰差异记录即可。
核心要求：真人参考不能变成平铺；平铺参考不能出现人物；场景参考不能退化成纯色棚拍；核心风格、场景、配色和服装语言必须肉眼可见；不得复制 Logo、水印、来源文字或人物身份。{"整组还要检查 A/B/C/D 差异和风格统一。" if generated_count > 1 else ""}
宽容边界：只在整体维度（展示方式/风格/场景/配色/构图完整性）偏离时判失败；单品级呈现细节不作为失败理由，可写入 notes 供参考——生成模型不保证像素级服从单品描述。具体不受罚的例子：内搭少一层开衫或薄衫、配饰有无、鞋型款式差异、衣服颜色深浅微差——这些只写 notes。真正的失败仅限：完全缺失计划中的主体单品（有外套计划但整张无外套）、场景类型完全错配、展示方式退化。{"（上一条「配饰有无不受罚」不适用于上面【核心商品】里点名的指定商品。）" if product_clause else ""}
只返回 JSON：{{"passed":true,"scores":{{"presentation_alignment":0,"style_alignment":0,"scene_alignment":0,"palette_alignment":0,"look_difference":0}},"reason_codes":[],"notes":"中文简述"}}。任一必要维度低于75时 passed 必须为 false。{role_clause}"""

    def review_human_presentation(
        self, *, image_paths: Sequence[str], role_order: Sequence[str],
        persona_reference_paths: Sequence[str] = (),
        pose_contracts: Mapping[str, Any] = None,
    ) -> dict[str, Any]:
        """Observation-only human presentation review (verdict computed in
        services/photo_human_qa.py — the model never returns a pass flag)."""
        generated = [str(Path(value).resolve()) for value in image_paths]
        identity = [str(Path(value).resolve()) for value in persona_reference_paths or []]
        if not generated or any(not Path(value).is_file() for value in generated) \
                or any(not Path(value).is_file() for value in identity):
            raise PhotoReferenceVisionError("人物表现质检缺少图片")
        prompt = self._human_presentation_prompt(
            identity_count=len(identity), generated_count=len(generated),
            role_order=role_order, pose_contracts=dict(pose_contracts or {}),
        )
        raw = None
        provider_used = self.provider
        for attempt in (1, 2):
            response, provider_used = self._chat(
                self._model_images(identity + generated), prompt, max_tokens=2600,
                prefer="fast",
            )
            candidate = parse_vision_envelope(response)
            if isinstance(candidate, list):
                candidate = {"roles": candidate}
            roles = candidate.get("roles") if isinstance(candidate, Mapping) else None
            if isinstance(roles, list) and len(roles) == len(generated) and all(
                    isinstance(item, Mapping) for item in roles):
                raw = candidate
                break
            # 结构不完整：附纠正要求重试一次。
            prompt = (
                f"{prompt}\n\n上一次输出不完整：roles 必须是一条不多一条不少、"
                f"共 {len(generated)} 条的数组，按生成图顺序逐字使用 role："
                f"{json.dumps(list(role_order), ensure_ascii=False)}。重新输出完整 JSON。"
            )
        if raw is None:
            raise PhotoReferenceVisionError("人物表现观察两次未返回完整逐张结构")
        raw = dict(raw)
        raw["provider_used"] = provider_used
        return raw

    def review_group_consistency(
        self, *, image_paths: Sequence[str], role_order: Sequence[str],
        persona_reference_paths: Sequence[str] = (),
        color_grading_plan: Mapping[str, Any] = None,
    ) -> dict[str, Any]:
        """Cross-image consistency observations (skin tone / color grading /
        lighting) — program-side verdict in services/photo_color_consistency.py."""
        generated = [str(Path(value).resolve()) for value in image_paths]
        identity = [str(Path(value).resolve()) for value in persona_reference_paths or []]
        if not generated or any(not Path(value).is_file() for value in generated) \
                or any(not Path(value).is_file() for value in identity):
            raise PhotoReferenceVisionError("跨图一致性质检缺少图片")
        prompt = self._group_consistency_prompt(
            identity_count=len(identity), generated_count=len(generated),
            role_order=role_order, color_grading_plan=dict(color_grading_plan or {}),
        )
        response, provider_used = self._chat(
            self._model_images(identity + generated), prompt, max_tokens=1600,
            prefer="fast",
        )
        candidate = parse_vision_envelope(response)
        if isinstance(candidate, list):
            candidate = {"roles": candidate}
        if not isinstance(candidate, Mapping) or not isinstance(candidate.get("roles"), list):
            raise PhotoReferenceVisionError("跨图一致性观察返回结构无效")
        candidate = dict(candidate)
        candidate["provider_used"] = provider_used
        return candidate

    @staticmethod
    def _group_consistency_prompt(*, identity_count, generated_count, role_order,
                                  color_grading_plan):
        plan_text = json.dumps(color_grading_plan, ensure_ascii=False)
        return f"""你是图文组内一致性观察员。第 1 张是人物身份参考图（全组肤色的唯一权威源），后 {generated_count} 张是按顺序对应角色 {json.dumps(list(role_order), ensure_ascii=False)} 的同一篇真人穿搭生成图。
全组调色计划：{plan_text}
只报告每张与参考图肤色及全组基准的偏离观察并打分，不要给出整体 passed 结论；程序会按规则判定。
逐张返回 JSON：
{{"roles":[{{"role":"look_a","skin_tone_match":0,"color_grading_match":0,"lighting_match":0,"drift_note_zh":""}}]}}
评分 0-100：skin_tone_match 生成图人物肤色与身份参考图的一致程度（偏黄/偏黑/过白都算偏离）；color_grading_match 色温与饱和度与全组调色计划的一致程度；lighting_match 光线方向与亮度的组内一致程度。
drift_note_zh 用中文描述具体偏离（如"肤色偏黄、整体偏暗"），一致则留空。不要输出 Markdown。"""

    @staticmethod
    def _human_presentation_prompt(*, identity_count, generated_count, role_order,
                                   pose_contracts):
        roles_text = json.dumps(list(role_order), ensure_ascii=False)
        contracts_text = json.dumps(pose_contracts, ensure_ascii=False, indent=1)
        return f"""你是人物表现观察员。前 {identity_count} 张是同一人物的身份参考图{"（如无则为 0 张）" if identity_count == 0 else ""}，后 {generated_count} 张是按顺序对应角色 {roles_text} 的真人穿搭生成图。
只描述可见事实并打分，不要给出整体 passed 结论；程序会按规则判定。
逐张返回 JSON：
{{"roles":[{{"role":"look_a","scores":{{"face_realism":0,"head_posture":0,"body_posture":0,"gesture_naturalness":0,"expression_naturalness":0,"creator_photo_feel":0}},"observations":{{"head_tilt":"NONE","head_tilt_direction":"NONE","gaze":"CAMERA","pose_family":"RELAXED_STAND","expression":"NEUTRAL","ai_face_signs":[],"limb_structure_implausible":false,"identity_drift":false}},"issues":[],"repair_instruction":""}}]}}
枚举约束：
- head_tilt: NONE=头颈水平；MINOR=轻微倾斜；OBVIOUS=明显倾斜。head_tilt_direction: NONE/LEFT/RIGHT。
- gaze: CAMERA=看镜头；FORWARD=看前方；SIDE=看侧方；DOWN=看下方或手部。
- pose_family: RELAXED_STAND=放松站立；WALKING_CANDID=行走抓拍；SCENE_INTERACTION=与场景或自身物品互动；TURN_BACK=回身/转头走过；STATIC_MANNEQUIN=僵直人台式站立的兜底归类。
- expression: NEUTRAL=自然；SOFT_SMILE=轻微微笑；CANDID=抓拍表情；FIXED_BEAUTY_SMILE=固定美颜式微笑。
- ai_face_signs: 仅收集可见的 PLASTIC_SKIN（磨皮塑料皮肤）、DOLL_EYES（玻璃娃娃眼）、FACE_GEOMETRY_ARTIFACT（五官几何畸变）。
- identity_drift: 生成图人物是否明显不像身份参考图里的同一人；identity_count 为 0 时填 false。
- limb_structure_implausible: 手臂、腿、手指结构是否不合理。
评分 0-100：face_realism 皮肤真实感与非 AI 感；head_posture 头颈水平程度；body_posture 重心与肩髋受力可信度；gesture_naturalness 手势自然度；expression_naturalness 表情自然度；creator_photo_feel 手机抓拍内容感。
各角色动作合同（当前页应有动作）：{contracts_text}
repair_instruction 用中文写修复要求：先指出本张头位/表情/动作/皮肤的具体问题，再说明应有的动作；合格则留空。不要输出 Markdown。"""

    @staticmethod
    def _safe(value: str) -> str:
        return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in str(value))[:120]

    def _model_images(self, paths: Sequence[str]) -> list[str]:
        """Bound payload size while keeping originals as the immutable source."""
        folder = self.root / "reference_contracts" / "_model_inputs"
        folder.mkdir(parents=True, exist_ok=True)
        output = []
        for value in paths:
            source = Path(value)
            digest = hashlib.sha256(source.read_bytes()).hexdigest()
            target = folder / f"{digest}.jpg"
            if not target.is_file():
                with Image.open(source) as opened:
                    image = ImageOps.exif_transpose(opened).convert("RGB")
                    image.thumbnail((1600, 1600), Image.Resampling.LANCZOS)
                    temporary = target.with_suffix(".tmp.jpg")
                    image.save(temporary, "JPEG", quality=86, optimize=True)
                    temporary.replace(target)
            output.append(str(target))
        return output
