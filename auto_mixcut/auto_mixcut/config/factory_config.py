from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Mapping


TRUE_VALUES = {"1", "true", "yes", "on", "是"}
FALSE_VALUES = {"0", "false", "no", "off", "否"}


@dataclass(frozen=True)
class FactoryConfig:
    ads_fast_mode: bool = False
    allow_direct_submit: bool = False
    guard_submit_ai_packages: bool = False
    ads_allow_low_trust_first_slot: bool = False

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "FactoryConfig":
        env = env if env is not None else os.environ
        return cls(
            ads_fast_mode=env_flag(env, "AUTO_MIXCUT_ADS_FAST_MODE", False),
            allow_direct_submit=env_flag(env, "AUTO_MIXCUT_ALLOW_DIRECT_SUBMIT", False),
            guard_submit_ai_packages=env_flag(env, "AUTO_MIXCUT_GUARD_SUBMIT_AI_PACKAGES", False),
            ads_allow_low_trust_first_slot=env_flag(env, "AUTO_MIXCUT_ADS_ALLOW_LOW_TRUST_FIRST_SLOT", False),
        )


def factory_config(env: Mapping[str, str] | None = None) -> FactoryConfig:
    return FactoryConfig.from_env(env)


def env_flag(env: Mapping[str, str], key: str, default: bool = False) -> bool:
    value = env.get(key)
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in TRUE_VALUES:
        return True
    if normalized in FALSE_VALUES:
        return False
    return default
