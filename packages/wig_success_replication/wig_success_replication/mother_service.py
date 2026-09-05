"""One-pass mother structuring, immutable versions, optional test review."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .hashing import source_hash, stable_hash
from .models import MotherContract, MotherReview, MotherStatus, ProductFactCard, MotherCorePoint, MotherProcessingProvenance
from .core_points import mother_core_points
from .prompts import load_prompt, prompt_fingerprint
from .execution_contract import execution_summary
from .repository import MotherVersionRecord, Repository
from .state_machine import MOTHER_TRANSITIONS, require_transition
from .structured_llm import StructuredResponsesClient


# Included in the mother digest so a rule change cannot silently reuse a
# contract produced under the retired per-claim confirmation policy.
MOTHER_POLICY_VERSION = "mechanism_first_single_pass_v3"


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

    def process(self, request: MotherInput, *, independent_review: bool = False, refresh: bool = False) -> MotherProcessResult:
        if not request.source_script.strip():
            raise ValueError("source script is required")
        if not request.source_image_urls:
            raise ValueError("at least one source product image is required")
        input_fingerprint = source_hash(request.source_script, {
            "source_product_id": request.source_product_id,
            "source_product_fact": request.source_product_fact.model_dump(mode="json"),
            "human_notes": request.human_notes.strip(),
            "validation_notes": request.validation_notes.strip(),
        })
        implementation_fingerprint = stable_hash({
                "policy_version": MOTHER_POLICY_VERSION,
                "prompt_fingerprint": prompt_fingerprint("mother_analyze", *(["mother_review"] if independent_review else [])),
                "schema_fingerprint": stable_hash(MotherContract.model_json_schema(),
                                                   MotherReview.model_json_schema() if independent_review else None),
                "independent_review": independent_review,
        })
        digest = stable_hash(input_fingerprint, implementation_fingerprint)
        current = self.repository.latest_mother(request.mother_id)
        if current and not refresh:
            provenance = current.contract.processing_provenance
            if (provenance and provenance.input_fingerprint == input_fingerprint
                    and (not independent_review or current.review.review_status == "completed")):
                return MotherProcessResult(current, False, "business input unchanged; frozen version retained")
            # A deploy must never create a new cumulative namespace just to
            # upgrade the compiler. Old hashes did not include human notes or
            # validation notes; explicit mother notes require a fresh version,
            # while legacy validation-note refresh must be explicitly requested.
            legacy_hashes = {
                source_hash(request.source_script, {
                    "policy_version": "human_selected_claim_transfer_v2",
                    "source_product_fact": request.source_product_fact.model_dump(mode="json"),
                }),
                source_hash(request.source_script, request.source_product_fact.model_dump(mode="json")),
            }
            if (not provenance and not independent_review and not request.human_notes.strip()
                    and current.source_product_id == request.source_product_id
                    and current.source_hash in legacy_hashes):
                return MotherProcessResult(current, False, "legacy source unchanged; frozen version retained; validation-note refresh is explicit")
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
        processing_provenance = MotherProcessingProvenance(
            input_fingerprint=input_fingerprint, implementation_fingerprint=implementation_fingerprint,
            policy_version=MOTHER_POLICY_VERSION)
        draft = draft.model_copy(update={"processing_provenance": processing_provenance})
        if independent_review:
            review = self.llm.call(
                task_type="MOTHER_REVIEW",
                entity_id=f"{request.mother_id}:V{version}",
                system_prompt=load_prompt("mother_review"),
                user_payload={**common, "draft": draft.model_dump(mode="json")},
                response_model=MotherReview,
                schema_name="mother_review_v1",
                image_urls=None,
            )
            final_contract = self._pin_identity(review.final_contract, request.mother_id, version, digest)
            final_contract = final_contract.model_copy(update={"processing_provenance": processing_provenance})
            review = review.model_copy(update={"final_contract": final_contract,
                "human_summary": final_contract.human_summary, "review_status": "completed"})
        else:
            final_contract = draft
            review = MotherReview(final_contract=final_contract, issues=[], review_status="not_run",
                                  human_summary="未运行独立审查；母版已由一次分析完成结构整理。")
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
        return MotherProcessResult(record, True, "structured; ready for activation")

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
        from .models import ExecutionSummary
        summary = ExecutionSummary.model_validate(execution_summary(contract))
        return contract.model_copy(
            update={
                "execution_summary": summary,
                "core_points": ([MotherCorePoint.model_validate(point) for point in summary.model_dump(mode="json")["core_mechanisms"]]
                                if len(summary.core_mechanisms) >= 3 else []),
                "mother_id": mother_id,
                "mother_version": version,
                "market": "MX",
                "category": "wig",
                "source_integrity": integrity,
            }
        )
