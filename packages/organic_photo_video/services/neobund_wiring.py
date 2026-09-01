"""Wire OPV publish components to the local NeoBund credentials.

Reuses the SAME local config file the hourly auto-publisher already uses
(``short_video_auto_publisher_config.json``: neobund_access_token +
neobund_cookie + neobund_base_url). No new credentials are introduced and
no secret value is ever logged.

OPV account -> NeoBund authId binding lives in
``opv_account_profile.operating_rules_json["neobund_auth_id"]`` and is merged
into the adapter's ``account_id_map`` at build time.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
SKILL_DIR = WORKSPACE_ROOT / "skills" / "short-video-auto-publisher"
DEFAULT_AUTO_PUBLISHER_CONFIG = (
    Path.home() / ".openclaw" / "shared" / "data" / "short_video_auto_publisher_config.json"
)


class WiringError(RuntimeError):
    pass


def neobund_settings_from_config(config_path: Optional[Path] = None) -> Dict[str, str]:
    """Extract connection settings (never log or return them to stdout)."""
    path = Path(config_path) if config_path else DEFAULT_AUTO_PUBLISHER_CONFIG
    if not path.exists():
        raise WiringError(f"auto-publisher config not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        config = json.load(handle)
    settings = {
        "base_url": str(config.get("neobund_base_url") or "https://www.neobund.ai/np").strip(),
        "access_token": str(config.get("neobund_access_token") or "").strip(),
        "cookie": str(config.get("neobund_cookie") or "").strip(),
    }
    if not settings["access_token"] and not settings["cookie"]:
        raise WiringError(
            "auto-publisher config has neither neobund_access_token nor neobund_cookie"
        )
    return settings


def opv_bindings_from_profiles(repository) -> Dict[str, Dict[str, Any]]:
    """Read {account_id: {"neobund_auth_id": ...}} from account profiles."""
    bindings: Dict[str, Dict[str, Any]] = {}
    get_account = getattr(repository, "list_account_profiles", None)
    accounts = get_account() if callable(get_account) else []
    for account in accounts or []:
        auth_id = str((account.operating_rules_json or {}).get("neobund_auth_id") or "").strip()
        if auth_id:
            bindings[account.account_id] = {"neobund_auth_id": auth_id}
    return bindings


@dataclass
class NeoBundWiring:
    client: Any
    adapter: Any
    music_source: Any


def build_opv_neobund(
    repository=None,
    config_path: Optional[Path] = None,
    account_id_map: Optional[Dict[str, Any]] = None,
) -> NeoBundWiring:
    """Build NeoBundClient + NeoBundPublishAdapter + music source.

    Requires the short-video-auto-publisher skill on disk (same workspace).
    """
    settings = neobund_settings_from_config(config_path)
    if str(SKILL_DIR) not in sys_path():
        add_to_sys_path(str(SKILL_DIR))
    from app.neobund_publish import NeoBundClient, NeoBundPublishAdapter

    client = NeoBundClient(
        base_url=settings["base_url"],
        access_token=settings["access_token"],
        cookie=settings["cookie"],
    )
    merged_map: Dict[str, Any] = dict(account_id_map or {})
    if repository is not None:
        for account_id, binding in opv_bindings_from_profiles(repository).items():
            merged_map.setdefault(account_id, binding)
    adapter = NeoBundPublishAdapter(
        client=client,
        account_id_map=merged_map,
        # Captured 2026-08-31 from real NeoBund frontend traffic: the AIGC
        # flag rides as "isAigc" (lowercase g), not the legacy "isAIGC".
        ai_generated_field="isAigc",
    )
    from services.neobund_publisher import NeoBundTrendingMusicSource

    music_source = NeoBundTrendingMusicSource(client)
    return NeoBundWiring(client=client, adapter=adapter, music_source=music_source)


def sys_path():
    import sys

    return sys.path


def add_to_sys_path(path: str) -> None:
    import sys

    sys.path.insert(0, path)
