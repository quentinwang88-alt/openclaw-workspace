from __future__ import annotations

import base64
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Sequence

import httpx
from openai import OpenAI


CODEX_BASE_URL = "https://chatgpt.com/backend-api/codex"


class SizeChartDetectionError(RuntimeError):
    pass


class VisionJSONClient(Protocol):
    def call_json(
        self, prompt: str, image_paths: Sequence[str], max_output_tokens: int
    ) -> Any: ...


@dataclass(frozen=True)
class SizeChartSelection:
    index: int
    confidence: float
    evidence: str


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        result = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return result if isinstance(result, dict) else {}


def _codex_access_token() -> str:
    environment = str(os.environ.get("OPENAI_CODEX_ACCESS_TOKEN", "")).strip()
    if environment:
        return environment

    openclaw = _read_json(
        Path.home() / ".openclaw" / "agents" / "main" / "agent" / "auth-profiles.json"
    )
    profiles = openclaw.get("profiles")
    profile = profiles.get("openai-codex:default") if isinstance(profiles, dict) else {}
    if isinstance(profile, dict) and str(profile.get("access") or "").strip():
        return str(profile["access"]).strip()

    codex = _read_json(Path.home() / ".codex" / "auth.json")
    tokens = codex.get("tokens")
    if isinstance(tokens, dict) and str(tokens.get("access_token") or "").strip():
        return str(tokens["access_token"]).strip()

    hermes = _read_json(Path.home() / ".hermes" / "auth.json")
    providers = hermes.get("providers")
    provider = providers.get("openai-codex") if isinstance(providers, dict) else {}
    tokens = provider.get("tokens") if isinstance(provider, dict) else {}
    if isinstance(tokens, dict) and str(tokens.get("access_token") or "").strip():
        return str(tokens["access_token"]).strip()
    raise SizeChartDetectionError("No current OpenClaw/Codex access token is available")


def _http_client(timeout: int) -> Optional[httpx.Client]:
    proxy = (
        os.environ.get("MIAOSHOU_VISION_PROXY")
        or os.environ.get("ALL_PROXY")
        or os.environ.get("HTTPS_PROXY")
        or "socks5://127.0.0.1:10808"
    )
    try:
        return httpx.Client(
            proxy=proxy,
            timeout=httpx.Timeout(float(timeout), connect=15.0),
        )
    except Exception:
        return None


def _image_data_url(path_text: str) -> str:
    path = Path(path_text)
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    extension = path.suffix.lower().lstrip(".") or "jpeg"
    if extension == "jpg":
        extension = "jpeg"
    return f"data:image/{extension};base64,{encoded}"


def _parse_json_object(text: str) -> Dict[str, Any]:
    value = text.strip()
    if value.startswith("```"):
        lines = value.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        value = "\n".join(lines).strip()
    try:
        result = json.loads(value)
    except json.JSONDecodeError as exc:
        raise SizeChartDetectionError("Vision model returned invalid JSON") from exc
    if not isinstance(result, dict):
        raise SizeChartDetectionError("Vision model did not return a JSON object")
    return result


class CodexVisionJSONClient:
    def __init__(self, model: str = "gpt-5.5", timeout: int = 180) -> None:
        self.model = os.environ.get("MIAOSHOU_VISION_MODEL", model).strip() or model
        self.timeout = timeout

    def call_json(
        self, prompt: str, image_paths: Sequence[str], max_output_tokens: int = 1800
    ) -> Dict[str, Any]:
        client = OpenAI(
            api_key=_codex_access_token(),
            base_url=os.environ.get("OPENAI_CODEX_BASE_URL", CODEX_BASE_URL).rstrip("/"),
            timeout=self.timeout,
            max_retries=1,
            http_client=_http_client(self.timeout),
        )
        content: List[Dict[str, Any]] = [{"type": "input_text", "text": prompt}]
        content.extend(
            {"type": "input_image", "image_url": _image_data_url(path)}
            for path in image_paths
        )
        chunks: List[str] = []
        completed = ""
        try:
            with client.responses.stream(
                model=self.model,
                reasoning={"effort": "medium"},
                instructions=(
                    "Classify ecommerce images conservatively. "
                    "Return only valid JSON when requested."
                ),
                store=False,
                input=[{"role": "user", "content": content}],
            ) as stream:
                for event in stream:
                    event_type = str(getattr(event, "type", "") or "")
                    if event_type == "response.output_text.delta":
                        chunks.append(str(getattr(event, "delta", "") or ""))
                    elif event_type == "response.output_text.done":
                        completed = str(getattr(event, "text", "") or "")
        except TypeError as exc:
            if "NoneType" not in str(exc) or not chunks:
                raise
        text = completed.strip() or "".join(chunks).strip()
        if not text:
            raise SizeChartDetectionError("Vision model returned no text")
        return _parse_json_object(text)


def build_size_chart_prompt(image_count: int) -> str:
    return f"""
你是服装电商详情图中的尺码表检测器。输入图片按顺序编号为 0 到 {image_count - 1}。
只判断整张图片是否主要展示可用于当前商品的服装尺码表或尺码推荐表。
尺码表通常包含 S/M/L/XL、胸围、衣长、肩宽、腰围、臀围等字段和数字表格。
模特图、商品展示图、材质图、营销图都不是尺码表。
不要抄录、推测或生成任何尺码数值。
只输出合法 JSON，格式为：
{{"results":[{{"index":0,"is_size_chart":true,"confidence":0.99,"evidence":"可见服装尺码网格"}}]}}
results 必须覆盖全部 {image_count} 张输入图片，index 不得重复。
""".strip()


class SizeChartDetector:
    def __init__(
        self,
        client: Optional[VisionJSONClient] = None,
        confidence_threshold: float = 0.95,
    ) -> None:
        self.client = client or CodexVisionJSONClient()
        self.confidence_threshold = confidence_threshold

    def detect(self, image_paths: Sequence[str]) -> SizeChartSelection:
        if not image_paths:
            raise SizeChartDetectionError("No detail images are available")
        result = self.client.call_json(
            build_size_chart_prompt(len(image_paths)),
            image_paths,
            max_output_tokens=max(1200, len(image_paths) * 120),
        )
        rows = result.get("results") if isinstance(result, dict) else None
        if not isinstance(rows, list) or len(rows) != len(image_paths):
            raise SizeChartDetectionError(
                "Vision result does not cover every detail image"
            )
        by_index: Dict[int, Dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict):
                raise SizeChartDetectionError("Vision result contains a non-object row")
            try:
                index = int(row.get("index"))
            except (TypeError, ValueError) as exc:
                raise SizeChartDetectionError("Vision result has an invalid index") from exc
            if index < 0 or index >= len(image_paths) or index in by_index:
                raise SizeChartDetectionError("Vision result has duplicate/out-of-range indexes")
            by_index[index] = row
        candidates = []
        for index, row in by_index.items():
            confidence = float(row.get("confidence") or 0)
            if row.get("is_size_chart") is True and confidence >= self.confidence_threshold:
                candidates.append(
                    SizeChartSelection(
                        index=index,
                        confidence=confidence,
                        evidence=str(row.get("evidence") or "").strip(),
                    )
                )
        if len(candidates) != 1:
            raise SizeChartDetectionError(
                f"Expected exactly one high-confidence size chart, found {len(candidates)}"
            )
        return candidates[0]
