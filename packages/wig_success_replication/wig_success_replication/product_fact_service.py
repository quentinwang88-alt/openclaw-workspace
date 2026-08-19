"""Product fact organization with an explicit human confirmation gate."""

from __future__ import annotations

from dataclasses import dataclass

from .hashing import product_fact_hash
from .models import ProductFactExtraction, ProductFactStatus
from .prompts import load_prompt
from .repository import ProductFactRecord, Repository
from .state_machine import PRODUCT_TRANSITIONS, require_transition
from .structured_llm import StructuredResponsesClient


@dataclass(frozen=True)
class ProductFactInput:
    product_id: str
    product_name: str
    image_urls: tuple[str, ...]
    supplementary_notes: str = ""


class ProductFactService:
    def __init__(self, repository: Repository, llm: StructuredResponsesClient):
        self.repository = repository
        self.llm = llm

    def organize(self, request: ProductFactInput) -> tuple[ProductFactRecord, bool]:
        if not request.image_urls:
            raise ValueError("at least one product image is required")
        digest = product_fact_hash(request.product_id, request.image_urls, request.supplementary_notes)
        current = self.repository.latest_product_fact(request.product_id)
        if current and current.source_hash == digest:
            return current, False
        version = 1 if current is None else current.version + 1
        extracted = self.llm.call(
            task_type="PRODUCT_FACT_EXTRACT",
            entity_id=f"{request.product_id}:V{version}",
            system_prompt=load_prompt("product_fact"),
            user_payload={
                "product_id": request.product_id,
                "product_name": request.product_name,
                "market": ["MX"],
                "category": "wig",
                "supplementary_notes": request.supplementary_notes,
            },
            response_model=ProductFactExtraction,
            schema_name="product_fact_v1",
            image_urls=list(request.image_urls),
        )
        fact = extracted.fact.model_copy(
            update={"product_id": request.product_id, "market": ["MX"], "category": "wig", "human_confirmed": False}
        )
        record = ProductFactRecord(
            product_id=request.product_id,
            version=version,
            source_hash=digest,
            fact=fact,
            human_summary=extracted.human_summary,
        )
        self.repository.save_product_fact(record)
        return record, True

    def confirm(self, product_id: str) -> ProductFactRecord:
        record = self.repository.latest_product_fact(product_id)
        if record is None:
            raise KeyError(product_id)
        require_transition(record.status, ProductFactStatus.CONFIRMED, PRODUCT_TRANSITIONS)
        self.repository.set_product_status(product_id, record.version, ProductFactStatus.CONFIRMED)
        record.status = ProductFactStatus.CONFIRMED
        record.fact.human_confirmed = True
        return record
