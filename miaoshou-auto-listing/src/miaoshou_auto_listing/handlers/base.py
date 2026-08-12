from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..browser.selectors import SelectorRegistry
from ..models import AppConfig, ProductTask, Step
from ..services.pricing_engine import PricingEngine


@dataclass
class HandlerContext:
    page: Any
    task: ProductTask
    config: AppConfig
    selectors: SelectorRegistry
    pricing: PricingEngine
    submission_store: Any = None
    acquisition_store: Any = None
    image_translation_store: Any = None
    linear: bool = False


class Handler:
    step: Step

    async def run(self, context: HandlerContext) -> None:
        raise NotImplementedError
