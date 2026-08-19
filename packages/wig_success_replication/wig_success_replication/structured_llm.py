"""Fixed OpenAI Responses Structured Output client.

There is intentionally no alternate model, CLI route, or weak-model fallback.
Tests inject a transport implementing ``create(**kwargs)``.
"""

from __future__ import annotations

import json
import time
from typing import Any, Protocol, TypeVar

from pydantic import BaseModel, ValidationError

from .model_run_logger import ModelRunLogger


MODEL = "gpt-5.6-sol"
REASONING_EFFORT = "high"
T = TypeVar("T", bound=BaseModel)


class ResponsesTransport(Protocol):
    def create(self, **kwargs: Any) -> Any: ...


class StructuredModelError(RuntimeError):
    pass


class NonRetryableStructuredModelError(StructuredModelError):
    """A request/schema/output defect that must be fixed instead of retried."""


class StructuredResponsesClient:
    def __init__(
        self,
        transport: ResponsesTransport,
        *,
        logger: ModelRunLogger | None = None,
        max_retries: int = 1,
        retry_delay_seconds: float = 0.5,
    ):
        if max_retries < 0 or max_retries > 1:
            raise ValueError("max_retries must be between 0 and 1")
        if retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must be non-negative")
        self.transport = transport
        self.logger = logger
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds

    @classmethod
    def from_openai_client(cls, client: Any, **kwargs: Any) -> "StructuredResponsesClient":
        """Bind an SDK client without importing or authenticating during package import."""
        return cls(client.responses, **kwargs)

    def call(
        self,
        *,
        task_type: str,
        entity_id: str,
        system_prompt: str,
        user_payload: dict[str, Any],
        response_model: type[T],
        schema_name: str,
        image_urls: list[str] | None = None,
        max_output_tokens: int | None = None,
    ) -> T:
        schema = self._strict_schema(response_model.model_json_schema())
        content: list[dict[str, Any]] = [
            {
                "type": "input_text",
                "text": json.dumps(user_payload, ensure_ascii=False, sort_keys=True),
            }
        ]
        for image_url in image_urls or []:
            content.append({"type": "input_image", "image_url": image_url})
        started = time.monotonic()
        last_error: Exception | None = None
        provider_request_id = ""
        for attempt in range(self.max_retries + 1):
            try:
                request = dict(
                    model=MODEL,
                    reasoning={"effort": REASONING_EFFORT},
                    instructions=system_prompt,
                    store=False,
                    input=[{"role": "user", "content": content}],
                    text={
                        "format": {
                            "type": "json_schema",
                            "name": schema_name,
                            "schema": schema,
                            "strict": True,
                        }
                    },
                )
                if max_output_tokens is not None:
                    request["max_output_tokens"] = max(1, int(max_output_tokens))
                if callable(getattr(self.transport, "stream", None)):
                    raw_text, provider_request_id = self._stream_output(request)
                else:
                    response = self.transport.create(**request)
                    provider_request_id = self._value(response, "id")
                    raw_text = self._output_text(response)
                result = response_model.model_validate_json(raw_text)
                self._log(
                    task_type=task_type,
                    entity_id=entity_id,
                    schema_name=schema_name,
                    provider_request_id=provider_request_id,
                    retry_count=attempt,
                    duration_ms=int((time.monotonic() - started) * 1000),
                    status="success",
                )
                return result
            except Exception as exc:
                last_error = exc
                retryable = self._is_retryable(exc)
                will_retry = retryable and attempt < self.max_retries
                self._log(
                    task_type=task_type,
                    entity_id=entity_id,
                    schema_name=schema_name,
                    provider_request_id=provider_request_id,
                    retry_count=attempt,
                    duration_ms=int((time.monotonic() - started) * 1000),
                    status="retrying" if will_retry else "failed",
                    error_summary=f"{type(exc).__name__}: {exc}",
                )
                if not will_retry:
                    break
                if self.retry_delay_seconds:
                    time.sleep(self.retry_delay_seconds)
        attempts = min(self.max_retries + 1, attempt + 1)
        raise StructuredModelError(
            f"{task_type} failed after {attempts} attempts: {last_error}"
        ) from last_error

    @classmethod
    def _is_retryable(cls, exc: Exception) -> bool:
        """Retry only capacity/transport failures; defects fail immediately."""
        if isinstance(exc, ValidationError):
            return False
        if isinstance(exc, NonRetryableStructuredModelError):
            return False

        status = cls._status_code(exc)
        if status is not None:
            if status == 429 or 500 <= status <= 599:
                return True
            if 400 <= status <= 499:
                return False

        name = type(exc).__name__.casefold()
        message = str(exc).casefold()
        non_retryable_markers = (
            "json schema", "json_schema", "schema validation", "validation error",
            "invalid_request", "bad request", "status code 400", "http 400",
        )
        if any(marker in message for marker in non_retryable_markers):
            return False
        if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
            return True
        if name in {
            "apiconnectionerror", "apitimeouterror", "ratelimiterror",
            "internalservererror", "connecterror", "connecttimeout",
            "readtimeout", "remoteprotocolerror",
        }:
            return True
        retryable_markers = (
            "429", "rate limit", "too many requests", "timed out", "timeout",
            "connection reset", "connection error", "connection aborted",
            "peer closed", "temporarily unavailable", "service unavailable",
            "internal server error", "bad gateway", "gateway timeout",
        )
        return any(marker in message for marker in retryable_markers)

    @staticmethod
    def _status_code(exc: Exception) -> int | None:
        candidates = [getattr(exc, "status_code", None)]
        response = getattr(exc, "response", None)
        if response is not None:
            candidates.append(getattr(response, "status_code", None))
        for value in candidates:
            try:
                if value is not None:
                    return int(value)
            except (TypeError, ValueError):
                continue
        return None

    def _stream_output(self, request: dict[str, Any]) -> tuple[str, str]:
        chunks: list[str] = []
        done_text = ""
        request_id = ""
        final_response: Any = None
        with self.transport.stream(**request) as stream:
            for event in stream:
                event_type = str(getattr(event, "type", "") or "")
                if event_type == "response.output_text.delta":
                    delta = str(getattr(event, "delta", "") or "")
                    if delta:
                        chunks.append(delta)
                elif event_type == "response.output_text.done":
                    done_text = str(getattr(event, "text", "") or "") or done_text
                    if done_text.strip() or chunks:
                        break
                elif event_type == "response.completed":
                    final_response = getattr(event, "response", None)
                    request_id = self._value(final_response, "id")
                elif event_type in {"response.failed", "response.incomplete"}:
                    response = getattr(event, "response", None)
                    detail = response.model_dump(mode="json") if hasattr(response, "model_dump") else response
                    raise StructuredModelError(f"Responses stream ended with {event_type}: {detail}")
        text = done_text.strip() or "".join(chunks).strip()
        if not text and final_response is not None:
            text = self._output_text(final_response).strip()
        if not text:
            raise StructuredModelError("Responses stream returned no output text")
        return text, request_id

    def _log(self, **kwargs: Any) -> None:
        if self.logger:
            self.logger.record(schema_version="1.0", model=MODEL, reasoning_effort=REASONING_EFFORT, **kwargs)

    @staticmethod
    def _value(response: Any, key: str) -> str:
        if isinstance(response, dict):
            return str(response.get(key) or "")
        return str(getattr(response, key, "") or "")

    @classmethod
    def _output_text(cls, response: Any) -> str:
        direct = response.get("output_text") if isinstance(response, dict) else getattr(response, "output_text", "")
        if direct:
            return str(direct)
        dumped = response
        if not isinstance(response, dict) and hasattr(response, "model_dump"):
            dumped = response.model_dump(mode="json")
        if not isinstance(dumped, dict):
            raise StructuredModelError("Responses API returned no output text")
        parts: list[str] = []
        for item in dumped.get("output") or []:
            for block in item.get("content") or []:
                if block.get("type") in {"output_text", "text"} and block.get("text"):
                    parts.append(str(block["text"]))
        if not parts:
            raise StructuredModelError("Responses API returned no output text")
        return "\n".join(parts)

    @classmethod
    def _strict_schema(cls, value: Any) -> Any:
        """Normalize Pydantic schemas for OpenAI strict Structured Outputs."""
        if isinstance(value, list):
            return [cls._strict_schema(item) for item in value]
        if not isinstance(value, dict):
            return value
        result = {key: cls._strict_schema(item) for key, item in value.items()}
        if result.get("type") == "object" or "properties" in result:
            properties = result.get("properties") or {}
            result["additionalProperties"] = False
            result["required"] = list(properties.keys())
        return result
