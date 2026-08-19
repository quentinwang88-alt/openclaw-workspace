"""Explicit state transitions; services may not silently skip human gates."""

from __future__ import annotations

from enum import Enum
from typing import Mapping

from .models import BatchStatus, MotherStatus, ProductFactStatus


class InvalidTransition(ValueError):
    pass


MOTHER_TRANSITIONS: Mapping[MotherStatus, frozenset[MotherStatus]] = {
    MotherStatus.PENDING: frozenset({MotherStatus.PROCESSING}),
    MotherStatus.PROCESSING: frozenset({MotherStatus.PENDING_CONFIRMATION, MotherStatus.FAILED}),
    MotherStatus.PENDING_CONFIRMATION: frozenset({MotherStatus.CONFIRMED, MotherStatus.PROCESSING, MotherStatus.DISABLED}),
    MotherStatus.CONFIRMED: frozenset({MotherStatus.PROCESSING, MotherStatus.DISABLED}),
    MotherStatus.FAILED: frozenset({MotherStatus.PROCESSING, MotherStatus.DISABLED}),
    MotherStatus.DISABLED: frozenset(),
}

PRODUCT_TRANSITIONS: Mapping[ProductFactStatus, frozenset[ProductFactStatus]] = {
    ProductFactStatus.PENDING: frozenset({ProductFactStatus.PROCESSING}),
    ProductFactStatus.PROCESSING: frozenset(
        {ProductFactStatus.PENDING_CONFIRMATION, ProductFactStatus.NEEDS_IMAGES, ProductFactStatus.FAILED}
    ),
    ProductFactStatus.PENDING_CONFIRMATION: frozenset(
        {ProductFactStatus.CONFIRMED, ProductFactStatus.PROCESSING, ProductFactStatus.DISABLED}
    ),
    ProductFactStatus.CONFIRMED: frozenset({ProductFactStatus.PROCESSING, ProductFactStatus.DISABLED}),
    ProductFactStatus.NEEDS_IMAGES: frozenset({ProductFactStatus.PROCESSING, ProductFactStatus.DISABLED}),
    ProductFactStatus.FAILED: frozenset({ProductFactStatus.PROCESSING, ProductFactStatus.DISABLED}),
    ProductFactStatus.DISABLED: frozenset(),
}

BATCH_TRANSITIONS: Mapping[BatchStatus, frozenset[BatchStatus]] = {
    BatchStatus.PENDING: frozenset({BatchStatus.RUNNING}),
    BatchStatus.RUNNING: frozenset({BatchStatus.COMPLETED, BatchStatus.PARTIAL, BatchStatus.FAILED}),
    BatchStatus.COMPLETED: frozenset(),
    BatchStatus.PARTIAL: frozenset(),
    BatchStatus.FAILED: frozenset(),
}


def require_transition(current: Enum, target: Enum, transitions: Mapping[Enum, frozenset[Enum]]) -> None:
    if target not in transitions.get(current, frozenset()):
        raise InvalidTransition(f"invalid transition: {current.value} -> {target.value}")
