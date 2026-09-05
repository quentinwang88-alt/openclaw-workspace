"""Small, side-effect-free publishing policy shared by task and export adapters."""

from __future__ import annotations

from typing import Any


PUBLISH_PURPOSES = {"带货", "养号"}


def resolve_publish_settings(publish_purpose: str, cart_setting: Any = None) -> tuple[str, bool]:
    purpose = str(publish_purpose).strip()
    if purpose not in PUBLISH_PURPOSES:
        raise ValueError("发布用途必须明确选择带货或养号")
    if isinstance(cart_setting, bool):
        return purpose, cart_setting
    setting = str(cart_setting or "").strip()
    if setting in {"", "按用途默认"}:
        return purpose, purpose == "带货"
    if setting in {"是", "挂车"}:
        return purpose, True
    if setting in {"否", "不挂车"}:
        return purpose, False
    raise ValueError("挂车设置必须为按用途默认、挂车或不挂车")
