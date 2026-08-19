"""Compatibility bridge to the separately versioned domain package."""

from __future__ import annotations

import dataclasses
import importlib
import inspect
import json
import os
import sys
from pathlib import Path
from typing import Any, Callable


WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
PACKAGES_ROOT = WORKSPACE_ROOT / "packages"
DOMAIN_PROJECT_ROOT = PACKAGES_ROOT / "wig_success_replication"
for import_root in (PACKAGES_ROOT, DOMAIN_PROJECT_ROOT):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))


class RuntimeBridgeError(RuntimeError):
    """Raised when the domain package contract cannot be satisfied."""


def _load_symbol(spec: str) -> Any:
    module_name, separator, attr_name = spec.partition(":")
    if not separator or not module_name or not attr_name:
        raise RuntimeBridgeError(
            "WIG_REPLICATION_APP_FACTORY must use module:callable format"
        )
    module = importlib.import_module(module_name)
    return getattr(module, attr_name)


def load_application() -> Any:
    """Load the public application facade without importing implementation modules."""
    candidates = []
    override = os.environ.get("WIG_REPLICATION_APP_FACTORY", "").strip()
    if override:
        candidates.append(override)
    candidates.extend(
        (
            "production_runtime:build_application",
            "wig_success_replication.application:build_application",
            "wig_success_replication.runtime:build_application",
            "wig_success_replication:build_application",
        )
    )
    errors: list[str] = []
    for candidate in candidates:
        try:
            factory = _load_symbol(candidate)
            if not callable(factory):
                raise TypeError("symbol is not callable")
            return factory()
        except (ImportError, AttributeError, TypeError, RuntimeError) as exc:
            errors.append(f"{candidate}: {exc}")
    raise RuntimeBridgeError(
        "cannot load wig_success_replication application facade; "
        "set WIG_REPLICATION_APP_FACTORY=module:callable. Attempts: "
        + " | ".join(errors)
    )


def invoke(application: Any, names: tuple[str, ...], **kwargs: Any) -> Any:
    """Invoke the first compatible public method and pass only accepted kwargs."""
    for name in names:
        method = getattr(application, name, None)
        if not callable(method):
            continue
        signature = inspect.signature(method)
        accepts_any = any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD
            for parameter in signature.parameters.values()
        )
        selected = kwargs if accepts_any else {
            key: value for key, value in kwargs.items() if key in signature.parameters
        }
        return method(**selected)
    raise RuntimeBridgeError(
        "domain application is missing a supported method: " + ", ".join(names)
    )


def invoke_preview(
    application: Any,
    preview_names: tuple[str, ...],
    fallback_name: str,
    **kwargs: Any,
) -> Any:
    """Invoke a provably read-only method; never drop the dry_run guard."""
    for name in preview_names:
        if callable(getattr(application, name, None)):
            return invoke(application, (name,), **kwargs)
    fallback = getattr(application, fallback_name, None)
    if not callable(fallback):
        raise RuntimeBridgeError(
            "domain application has no read-only preview method: "
            + ", ".join((*preview_names, fallback_name))
        )
    signature = inspect.signature(fallback)
    accepts_any = any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    )
    if "dry_run" not in signature.parameters and not accepts_any:
        raise RuntimeBridgeError(
            f"refusing to call {fallback_name}: it does not expose a dry_run guard"
        )
    return invoke(application, (fallback_name,), **kwargs)


def jsonable(value: Any) -> Any:
    if dataclasses.is_dataclass(value):
        return jsonable(dataclasses.asdict(value))
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [jsonable(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "model_dump") and callable(value.model_dump):
        return jsonable(value.model_dump())
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return jsonable(value.to_dict())
    return str(value)


def print_result(value: Any) -> None:
    print(json.dumps(jsonable(value), ensure_ascii=False, indent=2, sort_keys=True))
