"""Analyze, independently review, version, and human-confirm mother contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .hashing import source_hash
from .models import MotherContract, MotherReview, MotherStatus, ProductFactCard
from .prompts import load_prompt
from .repository import MotherVersionRecord, Repository
from .state_machine import MOTHER_TRANSITIONS, require_transition
from .structured_llm import StructuredResponsesClient


# Included in the mother digest so a rule change cannot silently reuse a
# contract produced under the retired per-claim confirmation policy.
MOTHER_POLICY_VERSION = "human_selected_claim_transfer_v2"


@dataclass(frozen=True)
class MotherInput:
    mother_id: str
    feishu_record_id: str
    name: str
    source_product_id: str
    source_script: str
    validation_notes: str
    source_product_fact: ProductFactCard
    source_image_urls: tuple[str, ...]
    human_notes: str = ""


@dataclass(frozen=True)
class MotherProcessResult:
    record: MotherVersionRecord
    created: bool
    message: str


class MotherTemplateService:
    def __init__(self, repository: Repository, llm: StructuredResponsesClient):
        self.repository = repository
        self.llm = llm

    def process(self, request: MotherInput) -> MotherProcessResult:
        if not request.source_script.strip():
            raise ValueError("source script is required")
        if not request.source_image_urls:
            raise ValueError("at least one source product image is required")
        digest = source_hash(
            request.source_script,
            {
                "policy_version": MOTHER_POLICY_VERSION,
                "source_product_fact": request.source_product_fact.model_dump(mode="json"),
            },
        )
        current = self.repository.latest_mother(request.mother_id)
        if current and current.source_hash == digest:
            return MotherProcessResult(current, False, "source unchanged")
        version = 1 if current is None else current.version + 1
        common: dict[str, Any] = {
            "mother_id": request.mother_id,
            "mother_version": version,
            "market": "MX",
            "category": "wig",
            "source_script": request.source_script,
            "source_script_hash": digest,
            "source_product_fact": request.source_product_fact.model_dump(mode="json"),
            "validation_notes": request.validation_notes,
            "human_notes": request.human_notes,
            "claim_transfer_policy": (
                "母版声明已验证；运营选中目标产品即授权迁移，程序不得再次分析匹配度或要求逐项确认"
            ),
        }
        draft = self.llm.call(
            task_type="MOTHER_ANALYZE",
            entity_id=f"{request.mother_id}:V{version}",
            system_prompt=load_prompt("mother_analyze"),
            user_payload=common,
            response_model=MotherContract,
            schema_name="mother_contract_v1",
            image_urls=list(request.source_image_urls),
        )
        draft = self._pin_identity(draft, request.mother_id, version, digest)
        review = self.llm.call(
            task_type="MOTHER_REVIEW",
            entity_id=f"{request.mother_id}:V{version}",
            system_prompt=load_prompt("mother_review"),
            user_payload={**common, "draft": draft.model_dump(mode="json")},
            response_model=MotherReview,
            schema_name="mother_review_v1",
            # Review consumes the immutable script, confirmed fact card and
            # draft. Re-sending the same product images adds cost and may
            # tempt the reviewer to infer a new product truth.
            image_urls=None,
        )
        final_contract = self._pin_identity(review.final_contract, request.mother_id, version, digest)
        review = review.model_copy(update={"final_contract": final_contract, "human_summary": final_contract.human_summary})
        record = MotherVersionRecord(
            mother_id=request.mother_id,
            version=version,
            feishu_record_id=request.feishu_record_id,
            name=request.name,
            source_product_id=request.source_product_id,
            source_script=request.source_script,
            source_hash=digest,
            draft=draft,
            review=review,
            contract=final_contract,
        )
        self.repository.save_mother(record)
        return MotherProcessResult(record, True, "pending human confirmation")

    def confirm(self, mother_id: str) -> MotherVersionRecord:
        record = self.repository.latest_mother(mother_id)
        if record is None:
            raise KeyError(mother_id)
        require_transition(record.status, MotherStatus.CONFIRMED, MOTHER_TRANSITIONS)
        self.repository.set_mother_status(mother_id, record.version, MotherStatus.CONFIRMED)
        record.status = MotherStatus.CONFIRMED
        return record

    @staticmethod
    def _pin_identity(contract: MotherContract, mother_id: str, version: int, digest: str) -> MotherContract:
        integrity = contract.source_integrity.model_copy(
            update={"source_script_hash": digest, "source_script_must_not_be_overwritten": True}
        )
        return contract.model_copy(
            update={
                "mother_id": mother_id,
                "mother_version": version,
                "market": "MX",
                "category": "wig",
                "source_integrity": integrity,
            }
        )
