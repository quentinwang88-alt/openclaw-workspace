#!/usr/bin/env python3
"""Runtime configuration for the openai-image skill."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from workspace_support import load_repo_env

load_repo_env()


DEFAULT_OUTPUT_DIR = "/Users/likeu3/.openclaw/workspace/runtime/image_outputs"

CODEX_DEFAULT_BASE_URL = "https://chatgpt.com/backend-api/codex"
CODEX_DEFAULT_CHAT_MODEL = "gpt-5.5"
CODEX_DEFAULT_IMAGE_MODEL = "gpt-image-2"

OPENCLAW_CONFIG_PATH = Path(
    os.environ.get("OPENCLAW_CONFIG_PATH", str(Path.home() / ".openclaw" / "openclaw.json"))
)
OPENCLAW_AGENT_AUTH_PROFILE_PATH = Path(
    os.environ.get(
        "OPENCLAW_AGENT_AUTH_PROFILE_PATH",
        str(Path.home() / ".openclaw" / "agents" / "main" / "agent" / "auth-profiles.json"),
    )
)
CODEX_AUTH_PATH = Path(
    os.environ.get("CODEX_AUTH_PATH", str(Path.home() / ".codex" / "auth.json"))
)
HERMES_AUTH_PATH = Path(
    os.environ.get("HERMES_AUTH_PATH", str(Path.home() / ".hermes" / "auth.json"))
)


def _safe_read_json(path: Path) -> dict:
    try:
        import json
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _extract_openclaw_primary_model() -> str:
    payload = _safe_read_json(OPENCLAW_CONFIG_PATH)
    agents = payload.get("agents") if isinstance(payload, dict) else {}
    selected = ""
    if isinstance(agents, dict):
        agent_list = agents.get("list")
        if isinstance(agent_list, list):
            for item in agent_list:
                if isinstance(item, dict) and item.get("default") is True:
                    selected = str((item.get("model") or {}).get("primary") or "").strip()
                    break
        if not selected:
            selected = str((agents.get("defaults") or {}).get("model", {}).get("primary") or "").strip()
    if selected.startswith("openai-codex/"):
        model = selected.split("/", 1)[1].strip()
        if model:
            return model
    return ""


def _extract_openclaw_agent_access_token() -> str:
    payload = _safe_read_json(OPENCLAW_AGENT_AUTH_PROFILE_PATH)
    profiles = payload.get("profiles") if isinstance(payload, dict) else {}
    profile = profiles.get("openai-codex:default") if isinstance(profiles, dict) else {}
    if isinstance(profile, dict):
        access_token = str(profile.get("access") or "").strip()
        if access_token:
            return access_token
    return ""


def _extract_codex_cli_access_token() -> str:
    payload = _safe_read_json(CODEX_AUTH_PATH)
    tokens = payload.get("tokens") if isinstance(payload, dict) else {}
    if isinstance(tokens, dict):
        access_token = str(tokens.get("access_token") or "").strip()
        if access_token:
            return access_token
    return ""


def resolve_codex_access_token() -> str:
    """Resolve the codex access token from OpenClaw agent auth, Codex CLI auth, or Hermes auth."""
    openclaw_agent_access = _extract_openclaw_agent_access_token()
    if openclaw_agent_access:
        return openclaw_agent_access

    codex_cli_access = _extract_codex_cli_access_token()
    if codex_cli_access:
        return codex_cli_access

    payload = _safe_read_json(HERMES_AUTH_PATH)
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


def resolve_codex_base_url() -> str:
    """Resolve the codex API base URL from env or OpenClaw config."""
    env_value = str(os.environ.get("OPENAI_CODEX_BASE_URL", "") or "").strip()
    if env_value:
        return env_value.rstrip("/")

    payload = _safe_read_json(OPENCLAW_CONFIG_PATH)
    providers = payload.get("models", {}).get("providers", {}) if isinstance(payload, dict) else {}
    codex = providers.get("openai-codex") if isinstance(providers, dict) else {}
    if isinstance(codex, dict):
        base_url = str(codex.get("baseUrl") or "").strip()
        if base_url:
            return base_url.rstrip("/")

    return CODEX_DEFAULT_BASE_URL


def resolve_codex_model() -> str:
    """Resolve the codex image model from env or OpenClaw config.

    For image generation via the Codex Responses API, the model should be
    an image-capable model like ``gpt-image-2``, NOT the chat model (gpt-5.5).
    OpenClaw's built-in image_generate tool uses ``gpt-image-2`` as default.

    Resolution order:
      1. OPENAI_CODEX_IMAGE_MODEL env var
      2. CODEX_DEFAULT_IMAGE_MODEL (gpt-image-2)
    """
    env_value = str(os.environ.get("OPENAI_CODEX_IMAGE_MODEL", "") or "").strip()
    if env_value:
        return env_value

    return CODEX_DEFAULT_IMAGE_MODEL


def resolve_socks5_proxy() -> str:
    """Resolve SOCKS5 proxy from environment variables.

    Looks for ALL_PROXY, all_proxy, SOCKS_PROXY, or socks_proxy.
    Returns empty string if not configured.
    """
    for key in ("ALL_PROXY", "all_proxy", "SOCKS_PROXY", "socks_proxy"):
        value = str(os.environ.get(key, "") or "").strip()
        if value:
            return value
    return ""


@dataclass(frozen=True)
class Settings:
    """Environment-backed runtime settings."""

    # --- Legacy OpenAI API mode ---
    openai_api_key: str = field(default_factory=lambda: os.environ.get("OPENAI_API_KEY", "").strip())
    openai_base_url: str = field(
        default_factory=lambda: os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1").strip()
        or "https://api.openai.com/v1"
    )
    image_model: str = field(
        default_factory=lambda: os.environ.get("OPENAI_IMAGE_MODEL", "gpt-image-2").strip() or "gpt-image-2"
    )

    # --- Codex mode (new) ---
    api_mode: str = field(
        default_factory=lambda: os.environ.get("OPENAI_IMAGE_API_MODE", "codex").strip().lower() or "codex"
    )
    codex_base_url: str = field(default_factory=resolve_codex_base_url)
    codex_api_key: str = field(default_factory=resolve_codex_access_token)
    codex_model: str = field(default_factory=resolve_codex_model)

    # --- SOCKS5 proxy ---
    socks5_proxy: str = field(default_factory=resolve_socks5_proxy)

    # --- Shared settings ---
    codex_instructions: str = field(
        default_factory=lambda: os.environ.get(
            "OPENAI_CODEX_INSTRUCTIONS",
            "You are an image generation assistant. Generate images as requested.",
        ).strip()
    )
    image_output_dir: Path = field(
        default_factory=lambda: Path(
            os.environ.get("OPENAI_IMAGE_OUTPUT_DIR", DEFAULT_OUTPUT_DIR).strip() or DEFAULT_OUTPUT_DIR
        ).expanduser()
    )
    default_size: str = field(
        default_factory=lambda: os.environ.get("OPENAI_IMAGE_DEFAULT_SIZE", "1024x1024").strip() or "1024x1024"
    )
    default_quality: str = field(
        default_factory=lambda: os.environ.get("OPENAI_IMAGE_DEFAULT_QUALITY", "medium").strip() or "medium"
    )
    default_output_format: str = field(
        default_factory=lambda: os.environ.get("OPENAI_IMAGE_DEFAULT_FORMAT", "png").strip() or "png"
    )
    timeout_seconds: int = field(
        default_factory=lambda: int(os.environ.get("OPENAI_IMAGE_TIMEOUT", "120").strip() or "120")
    )
    max_retries: int = field(
        default_factory=lambda: int(os.environ.get("OPENAI_IMAGE_MAX_RETRIES", "3").strip() or "3")
    )

    @property
    def effective_api_key(self) -> str:
        """Return the API key for the active api_mode."""
        if self.api_mode == "codex":
            return self.codex_api_key
        return self.openai_api_key

    @property
    def effective_base_url(self) -> str:
        """Return the base URL for the active api_mode."""
        if self.api_mode == "codex":
            return self.codex_base_url
        return self.openai_base_url

    @property
    def effective_model(self) -> str:
        """Return the model for the active api_mode."""
        if self.api_mode == "codex":
            return self.codex_model
        return self.image_model


def get_settings() -> Settings:
    return Settings()
