"""Assemble the frozen resolved execution snapshot required by spec §4.

A task freezes its recipe / category / market / locale / destination /
reference / product / persona binding once.  Retries must reuse the same
snapshot, and a later Market or Locale Pack upgrade must never silently change
an already frozen task, so every version, id and hash is written down here.

The assembler is deliberately explicit: callers pass already-resolved values, so
there is no IO, no network and no Feishu dependency in this module.
"""
from __future__ import annotations

from typing import Any, Mapping, Sequence

from domain.contracts import (
    PHOTO_EXECUTION_CONTEXT_SCHEMA_VERSION, ensure_valid,
    validate_photo_execution_context_payload,
)

__all__ = ["PhotoExecutionContextError", "assert_unchanged", "build_execution_context"]


class PhotoExecutionContextError(ValueError):
    """Raised when a snapshot cannot be frozen because a binding is missing."""


def _require(value: Any, name: str) -> Any:
    if value in (None, "") or (isinstance(value, (list, dict)) and not value):
        raise PhotoExecutionContextError(f"冻结执行上下文缺少 {name}")
    return value


def build_execution_context(
    *,
    recipe_id: str,
    recipe_version: int,
    planning_flow: str,
    category_key: str,
    category_profile_id: str,
    category_profile_version: int,
    main_product_slot: str,
    market_country: str,
    market_pack_id: str,
    market_pack_version: int,
    locale: str,
    locale_pack_id: str,
    locale_pack_version: int,
    copy_pack_id: str,
    reference_mode: str,
    input_fingerprint: str,
    reference_hashes: Sequence[str] = (),
    destination: Mapping[str, Any] | None = None,
    product: Mapping[str, Any] | None = None,
    persona: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a validated, deep-freezeable execution snapshot.

    ``destination`` accepts a Destination Catalog entry (id/country/city) or
    ``None`` for a destination-free post.  ``product`` and ``persona`` are the
    already resolved product snapshot and persona binding.
    """
    snapshot: dict[str, Any] = {
        "schema_version": PHOTO_EXECUTION_CONTEXT_SCHEMA_VERSION,
        "recipe": {
            "id": _require(recipe_id, "recipe.id"),
            "version": _require(recipe_version, "recipe.version"),
            "planning_flow": _require(planning_flow, "recipe.planning_flow"),
        },
        "category": {
            "key": _require(category_key, "category.key"),
            "profile_id": _require(category_profile_id, "category.profile_id"),
            "profile_version": _require(category_profile_version, "category.profile_version"),
            "main_product_slot": _require(main_product_slot, "category.main_product_slot"),
        },
        "market": {
            "country": _require(market_country, "market.country"),
            "market_pack_id": _require(market_pack_id, "market.market_pack_id"),
            "market_pack_version": _require(market_pack_version, "market.market_pack_version"),
        },
        "locale": {
            "locale": _require(locale, "locale.locale"),
            "locale_pack_id": _require(locale_pack_id, "locale.locale_pack_id"),
            "locale_pack_version": _require(locale_pack_version, "locale.locale_pack_version"),
            "copy_pack_id": _require(copy_pack_id, "locale.copy_pack_id"),
        },
        "reference": {
            "mode": _require(reference_mode, "reference.mode"),
            "input_fingerprint": _require(input_fingerprint, "reference.input_fingerprint"),
            "reference_hashes": [str(value) for value in reference_hashes or ()],
        },
        "persona": {
            "persona_ref_id": _require(
                (persona or {}).get("persona_ref_id"), "persona.persona_ref_id"
            ),
            "persona_pack_id": _require(
                (persona or {}).get("persona_pack_id"), "persona.persona_pack_id"
            ),
            "reference_hashes": [
                str(value) for value in (persona or {}).get("reference_hashes") or ()
            ],
        },
    }
    if destination is not None:
        snapshot["destination"] = {
            "destination_id": _require(
                destination.get("destination_id"), "destination.destination_id"
            ),
            "destination_country": _require(
                destination.get("destination_country"), "destination.destination_country"
            ),
            "destination_city": _require(
                destination.get("destination_city"), "destination.destination_city"
            ),
        }
    if product is not None:
        snapshot["product"] = {
            key: value for key, value in dict(product).items() if value not in (None, "")
        }
    ensure_valid(
        validate_photo_execution_context_payload(snapshot), "photo execution context"
    )
    return snapshot


def assert_unchanged(frozen: Mapping[str, Any], rebuilt: Mapping[str, Any]) -> None:
    """Raise when a rebuilt snapshot would silently change a frozen task.

    Spec §4: after a task is created, versions, ids, image hashes and the input
    fingerprint may not change silently; a changed reference, product, persona,
    destination, theme or count requires a new task.
    """
    if dict(frozen) != dict(rebuilt):
        raise PhotoExecutionContextError("已冻结任务的执行上下文发生变化；必须新建任务")
