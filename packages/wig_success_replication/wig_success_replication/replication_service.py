"""Resumable, cumulative per-product prompt compilation."""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from copy import deepcopy
import base64
from dataclasses import dataclass
import logging
from typing import Any, Callable, Iterable

from .deterministic_qa import PromptQAResult, creative_history_snapshot, inspect_compile_output
from .core_points import mother_core_points
from .reference_manifest import validate_reference_manifest, bytes_sha256
from .hashing import batch_signature, deterministic_id
from .models import (
    BatchStatus,
    MotherStatus,
    ProductFactStatus,
    ReplicationCompileOutput,
    VariantPlanItem,
)
from .execution_contract import execution_summary, compilation_provenance, load_compile_prompt
from .publishing import PUBLISH_PURPOSES, resolve_publish_settings
from .repository import (
    BatchProductRecord,
    BatchRecord,
    MotherVersionRecord,
    ProductFactRecord,
    PromptRecord,
    Repository,
    make_outbox_record,
)
from .structured_llm import StructuredResponsesClient
from .variant_planner import PLANNER_VERSION, VariantPlanner


@dataclass(frozen=True)
class BatchResult:
    batch: BatchRecord
    created: bool
    saved_prompts: int
    failed_products: tuple[str, ...]
    errors: tuple[str, ...]


@dataclass(frozen=True)
class _ProductResult:
    product_id: str
    saved_prompts: int
    completed: bool
    error: str = ""


class ReplicationBatchService:
    """Generate only the missing sequence slots up to each product's target."""

    COMPILE_CHUNK_SIZE = 3

    def __init__(
        self,
        repository: Repository,
        llm: StructuredResponsesClient,
        planner: VariantPlanner | None = None,
        *,
        max_product_workers: int = 1,
    ):
        if not 1 <= max_product_workers <= 4:
            raise ValueError("max_product_workers must be between 1 and 4")
        self.repository = repository
        self.llm = llm
        self.planner = planner or VariantPlanner()
        self.max_product_workers = max_product_workers

    def generate(
        self,
        *,
        mother_id: str,
        product_ids: list[str],
        special_requirements: str = "",
        per_product_count: int | None = None,
        product_image_urls: dict[str, list[str]] | None = None,
        face_reference_image_urls: list[str] | None = None,
        max_product_workers: int | None = None,
        publish_purpose: str = "带货",
        cart_enabled: bool | str | None = None,
        handoff_context_by_product: dict[str, dict[str, Any]] | None = None,
        on_prompts_persisted: Callable[[], None] | None = None,
    ) -> BatchResult:
        publish_purpose, cart_enabled = resolve_publish_settings(publish_purpose, cart_enabled)
        mother = self.repository.latest_mother(mother_id)
        if mother is None or mother.status != MotherStatus.CONFIRMED:
            raise ValueError("mother must be human-confirmed")
        selected = tuple(sorted({item.strip() for item in product_ids if item.strip()}))
        if not selected:
            raise ValueError("at least one target product is required")
        if per_product_count is not None and not 1 <= per_product_count <= 20:
            raise ValueError("per_product_count must be between 1 and 20")
        workers = self.max_product_workers if max_product_workers is None else max_product_workers
        if not 1 <= workers <= 4:
            raise ValueError("max_product_workers must be between 1 and 4")

        # Unknown historical rows must not look like an empty purpose scope:
        # doing so would regenerate their cumulative slots after migration.
        for product_id in selected:
            unknown = [row for row in self.repository.list_prompts(mother_id, mother.version, product_id, None)
                       if row.publish_purpose not in PUBLISH_PURPOSES]
            if unknown:
                raise ValueError(
                    f"历史用途未分类：母版 {mother_id} V{mother.version} / 产品 {product_id} "
                    f"有 {len(unknown)} 条历史结果；请先完成人工用途归类，再生成或续跑，避免重复累计"
                )

        signature = batch_signature(
            mother_id,
            mother.version,
            selected,
            special_requirements,
            per_product_count,
            PLANNER_VERSION,
            publish_purpose,
            cart_enabled,
        )
        proposed = BatchRecord(
            batch_id=deterministic_id("batch", signature),
            idempotency_key=signature,
            mother_id=mother_id,
            mother_version=mother.version,
            product_ids=selected,
            special_requirements=special_requirements,
            publish_purpose=publish_purpose,
            cart_enabled=cart_enabled,
            handoff_context_by_product=deepcopy(handoff_context_by_product or {}),
        )
        batch, created = self.repository.get_or_create_batch(proposed)
        if batch.publish_purpose not in PUBLISH_PURPOSES or batch.cart_enabled is None:
            raise ValueError(f"历史批次 {batch.batch_id} 发布用途或挂车设置未分类，请先完成人工归类后续跑")
        if batch.status == BatchStatus.COMPLETED and self._all_targets_satisfied(
            mother, selected, per_product_count, publish_purpose
        ):
            # Recover a crash between saving a result and enqueueing delivery
            # without re-running the model or borrowing fresh asset settings.
            for product_id in selected:
                for row in self.repository.list_prompts(mother_id, mother.version, product_id, publish_purpose):
                    self.enqueue_prompt_handoffs(row)
            # Reaching an unchanged cumulative target is a successful no-op,
            # not an operator-facing failure.
            return BatchResult(batch, False, 0, tuple(), tuple())

        # A completed historical batch is deliberately reopened if durable
        # sequence rows are missing. Sequence uniqueness, not batch state, is
        # the source of truth for cumulative generation.
        self.repository.set_batch_status(
            batch.batch_id,
            BatchStatus.RUNNING,
            "resuming" if not created else "",
        )
        batch.status = BatchStatus.RUNNING
        results: list[_ProductResult] = []
        try:
            kwargs = {
                "batch": batch,
                "mother": mother,
                "special_requirements": special_requirements,
                "per_product_count": per_product_count,
                "product_image_urls": product_image_urls or {},
                "face_reference_image_urls": face_reference_image_urls or [],
                "on_prompts_persisted": on_prompts_persisted,
                "handoff_context_by_product": deepcopy(handoff_context_by_product or {}),
            }
            if workers == 1 or len(selected) <= 1:
                results = [
                    self._process_product(product_id=product_id, **kwargs)
                    for product_id in selected
                ]
            else:
                results = self._run_products_concurrently(
                    workers=workers,
                    product_ids=list(selected),
                    **kwargs,
                )
        except BaseException:
            self.repository.set_batch_status(
                batch.batch_id, BatchStatus.PARTIAL, "interrupted; resumable"
            )
            batch.status = BatchStatus.PARTIAL
            batch.summary = "interrupted; resumable"
            raise

        saved = sum(item.saved_prompts for item in results)
        failed_products: list[str] = []
        errors: list[str] = []
        for product_id in selected:
            row = self.repository.get_batch_product(batch.batch_id, product_id)
            if row is None or row.status != "completed":
                failed_products.append(product_id)
                detail = row.error_detail if row else "product was not processed"
                errors.append(f"{product_id}: {detail}")

        if not failed_products:
            status = BatchStatus.COMPLETED
        elif saved or any(self._product_completed(batch.batch_id, item) for item in selected):
            status = BatchStatus.PARTIAL
        else:
            status = BatchStatus.FAILED
        summary = f"new_saved={saved}; failed_products={len(failed_products)}"
        self.repository.set_batch_status(batch.batch_id, status, summary)
        batch.status = status
        batch.summary = summary
        return BatchResult(batch, created, saved, tuple(failed_products), tuple(errors))

    def _run_products_concurrently(
        self, *, workers: int, product_ids: list[str], **kwargs: Any
    ) -> list[_ProductResult]:
        executor = ThreadPoolExecutor(
            max_workers=min(workers, len(product_ids)),
            thread_name_prefix="wig-replication",
        )
        futures: dict[Future[_ProductResult], str] = {
            executor.submit(self._process_product, product_id=product_id, **kwargs): product_id
            for product_id in product_ids
        }
        results: list[_ProductResult] = []
        try:
            for future in as_completed(futures):
                results.append(future.result())
        except BaseException:
            for future in futures:
                future.cancel()
            executor.shutdown(wait=False, cancel_futures=True)
            raise
        executor.shutdown(wait=True)
        return results

    def _process_product(
        self,
        *,
        batch: BatchRecord,
        mother: MotherVersionRecord,
        product_id: str,
        special_requirements: str,
        per_product_count: int | None,
        product_image_urls: dict[str, list[str]],
        face_reference_image_urls: list[str],
        on_prompts_persisted: Callable[[], None] | None = None,
        handoff_context_by_product: dict[str, dict[str, Any]] | None = None,
    ) -> _ProductResult:
        product_record = self.repository.latest_product_fact(product_id)
        relationship = "same_product" if product_id == mother.source_product_id else "cross_product"
        if product_record is None or product_record.status != ProductFactStatus.CONFIRMED:
            error = "product fact must be human-confirmed"
            self.repository.save_batch_product(
                BatchProductRecord(
                    batch.batch_id,
                    product_id,
                    relationship,
                    product_record.version if product_record else 0,
                    status="failed",
                    error_detail=error,
                )
            )
            return _ProductResult(product_id, 0, False, error)

        existing = self.repository.list_prompts(mother.mother_id, mother.version, product_id, batch.publish_purpose)
        for row in existing:
            self.enqueue_prompt_handoffs(row)
        target = self.planner.target_count(relationship, per_product_count)
        plan = self.planner.plan(
            relationship,
            target,
            existing_sequences=(row.sequence_no for row in existing),
            publish_purpose=batch.publish_purpose,
        )
        if not plan:
            self.repository.save_batch_product(
                BatchProductRecord(
                    batch.batch_id,
                    product_id,
                    relationship,
                    product_record.version,
                    status="completed",
                    error_detail=f"target={target}; existing={target}; new=0",
                )
            )
            return _ProductResult(product_id, 0, True)

        images = [*face_reference_image_urls, *(product_image_urls.get(product_id) or [])]
        total_saved = 0
        all_errors: list[str] = []
        try:
            frozen_manifest = ((handoff_context_by_product or {}).get(product_id) or {}).get("reference_manifest")
            if frozen_manifest:
                manifest = validate_reference_manifest(frozen_manifest)
                assets = manifest["reference_assets"]
                roles = ["person_identity"] * len(face_reference_image_urls) + ["product"] * len(product_image_urls.get(product_id) or [])
                if len(assets) != len(images) or [asset["role"] for asset in assets] != roles:
                    raise ValueError("compile images differ from frozen reference roles/count")
                for asset, data_url in zip(assets, images):
                    if not data_url.startswith("data:image/") or ";base64," not in data_url:
                        raise ValueError("frozen compile reference requires image bytes")
                    digest = bytes_sha256(base64.b64decode(data_url.split(";base64,", 1)[1], validate=True))
                    if digest != asset.get("derived_sha256"):
                        raise ValueError("compile image bytes differ from frozen manifest")
            for chunk in self._chunks(plan, self.COMPILE_CHUNK_SIZE):
                base_payload = self._compile_payload(
                    batch=batch,
                    mother=mother,
                    product=product_record,
                    relationship=relationship,
                    plan=chunk,
                    existing_prompts=existing,
                    special_requirements=special_requirements,
                    face_reference_count=len(face_reference_image_urls),
                    product_image_count=len(product_image_urls.get(product_id) or []),
                )
                if frozen_manifest:
                    base_payload["reference_manifest"] = frozen_manifest
                sequence_range = f"{chunk[0].sequence_no}-{chunk[-1].sequence_no}"
                output = self._call_compiler(
                    task_type="REPLICATION_COMPILE",
                    entity_id=f"{batch.batch_id}:{product_id}:{sequence_range}",
                    payload=base_payload,
                    images=images,
                )
                self._validate_identity(output, batch, mother, product_id, relationship)
                accepted, failures = self._partition_qa(
                    output, chunk, product_record, existing, batch.publish_purpose, mother.contract
                )

                if failures:
                    repair_plan = [
                        item
                        for item in chunk
                        if (item.variant_key, item.mutation_key) in failures
                    ]
                    repair_payload = {
                        **base_payload,
                        "repair_mode": "failed_items_only",
                        "repair_scope": "从失败原文局部编辑，只修列出的错误；保留未涉及段落、声音、人物商品与身份字段。最多一轮，不重写已通过条目。",
                        "failed_items": [item.model_dump(mode="json") for item in output.outputs
                                         if (item.variant_key, item.mutation_key) in failures],
                        "accepted_current_signatures": [
                            qa.prompt.creative_signature.model_dump(mode="json")
                            for qa in accepted.values()
                        ],
                        "variant_plan": [item.model_dump(mode="json") for item in repair_plan],
                        "qa_failures": [
                            {
                                "variant_key": item.variant_key,
                                "mutation_key": item.mutation_key,
                                "issues": failures[(item.variant_key, item.mutation_key)],
                            }
                            for item in repair_plan
                        ],
                    }
                    try:
                        repaired = self._call_compiler(
                            task_type="REPLICATION_QA_REPAIR",
                            entity_id=f"{batch.batch_id}:{product_id}:{sequence_range}:repair",
                            payload=repair_payload,
                            images=images,
                        )
                        self._validate_identity(repaired, batch, mother, product_id, relationship)
                        repair_history = [
                            *existing,
                            *(qa.prompt for qa in accepted.values()),
                        ]
                        repaired_ok, repair_failures = self._partition_qa(
                            repaired, repair_plan, product_record, repair_history, batch.publish_purpose, mother.contract
                        )
                        accepted.update(repaired_ok)
                        failures = repair_failures
                    except Exception as exc:
                        # A failed repair must not discard sibling outputs that
                        # already passed this compile. Persist accepted below;
                        # the next cumulative run only plans missing slots.
                        detail = f"repair_failed:{type(exc).__name__}:{str(exc)[:500]}"
                        failures = {key: [*issues, detail] for key, issues in failures.items()}

                saved, save_errors = self._persist_accepted(
                    batch=batch,
                    mother=mother,
                    product_id=product_id,
                    plan=chunk,
                    accepted=accepted,
                    handoff_context=(handoff_context_by_product or {}).get(product_id) or {},
                )
                total_saved += saved
                if saved and on_prompts_persisted is not None:
                    try:
                        # Caller serializes delivery when product workers run
                        # in parallel. A failed delivery never redoes creation.
                        on_prompts_persisted()
                    except Exception as exc:
                        logging.getLogger(__name__).warning("delivery pending after persisted prompts: %s", exc)
                all_errors.extend(
                    f"{variant_key}/{mutation_key}: {','.join(issues)}"
                    for (variant_key, mutation_key), issues in failures.items()
                )
                all_errors.extend(save_errors)
                existing = self.repository.list_prompts(
                    mother.mother_id, mother.version, product_id, batch.publish_purpose
                )
                if failures or save_errors:
                    # Keep the sequence contiguous and let the next run resume
                    # from the first missing slot.
                    break

            existing_sequences = {
                row.sequence_no for row in existing if 1 <= row.sequence_no <= target
            }
            completed = existing_sequences == set(range(1, target + 1))
            error = "; ".join(all_errors)
            if not completed and not error:
                missing = sorted(set(range(1, target + 1)) - existing_sequences)
                error = f"missing_sequences={missing}"
            self.repository.save_batch_product(
                BatchProductRecord(
                    batch.batch_id,
                    product_id,
                    relationship,
                    product_record.version,
                    status="completed" if completed else "partial",
                    error_detail=(
                        f"target={target}; existing={len(existing_sequences)}; new={total_saved}"
                        if completed
                        else error
                    ),
                )
            )
            return _ProductResult(product_id, total_saved, completed, error)
        except Exception as exc:
            self.repository.save_batch_product(
                BatchProductRecord(
                    batch.batch_id,
                    product_id,
                    relationship,
                    product_record.version,
                    status="failed",
                    error_detail=str(exc),
                )
            )
            return _ProductResult(product_id, total_saved, False, str(exc))

    @staticmethod
    def _compile_payload(
        *,
        batch: BatchRecord,
        mother: MotherVersionRecord,
        product: ProductFactRecord,
        relationship: str,
        plan: list[VariantPlanItem],
        existing_prompts: list[Any],
        special_requirements: str,
        face_reference_count: int,
        product_image_count: int,
    ) -> dict[str, Any]:
        history = [
            {
                "sequence_no": row.sequence_no,
                "replication_mode": row.replication_mode,
                "creative_route": row.creative_route,
                "creative_signature": creative_history_snapshot(row),
            }
            for row in existing_prompts
            if row.sequence_no > 0
        ]
        return {
            "batch_id": batch.batch_id,
            "publish_purpose": batch.publish_purpose,
            "cart_enabled": batch.cart_enabled,
            "planner_version": PLANNER_VERSION,
            "relationship": relationship,
            "mother_id": mother.mother_id,
            "mother_version": mother.version,
            "execution_summary": execution_summary(mother.contract),
            "generation_provenance": compilation_provenance(mother),
            "target_product_fact": product.fact.model_dump(mode="json"),
            "attached_image_order": {
                "face_reference_count": face_reference_count,
                "product_image_count": product_image_count,
                "rule": "先人物脸部参考图，后目标假发产品图；两类素材不得混用",
            },
            "existing_creative_signatures": history,
            "variant_plan": [item.model_dump(mode="json") for item in plan],
            "special_requirements": special_requirements,
        }

    def _call_compiler(
        self,
        *,
        task_type: str,
        entity_id: str,
        payload: dict[str, Any],
        images: list[str],
    ) -> ReplicationCompileOutput:
        return self.llm.call(
            task_type=task_type,
            entity_id=entity_id,
            system_prompt=load_compile_prompt(payload["publish_purpose"],
                          {item["replication_mode"] for item in payload["variant_plan"]}),
            user_payload=payload,
            response_model=ReplicationCompileOutput,
            schema_name="replication_output_v1",
            image_urls=images,
        )

    @staticmethod
    def _validate_identity(
        output: ReplicationCompileOutput,
        batch: BatchRecord,
        mother: MotherVersionRecord,
        product_id: str,
        relationship: str,
    ) -> None:
        actual = (
            output.batch_id,
            output.mother_id,
            output.mother_version,
            output.product_id,
            output.relationship,
        )
        expected = (
            batch.batch_id,
            mother.mother_id,
            mother.version,
            product_id,
            relationship,
        )
        if actual != expected:
            raise ValueError("compiler output identity mismatch")

    @staticmethod
    def _partition_qa(
        output: ReplicationCompileOutput,
        plan: list[VariantPlanItem],
        product: ProductFactRecord,
        existing_prompts: list[PromptRecord],
        publish_purpose: str = "带货",
        mother_contract: Any = None,
    ) -> tuple[dict[tuple[str, str], PromptQAResult], dict[tuple[str, str], list[str]]]:
        planned = {(item.variant_key, item.mutation_key): item for item in plan}
        grouped: dict[tuple[str, str], list[PromptQAResult]] = {key: [] for key in planned}
        for result in inspect_compile_output(
            output,
            plan,
            product.fact,
            existing_prompts=existing_prompts,
            publish_purpose=publish_purpose,
            mother_contract=mother_contract,
        ):
            key = (result.prompt.variant_key, result.prompt.mutation_key)
            if key in grouped:
                grouped[key].append(result)
        accepted: dict[tuple[str, str], PromptQAResult] = {}
        failures: dict[tuple[str, str], list[str]] = {}
        for key, rows in grouped.items():
            if len(rows) == 1 and rows[0].passed:
                accepted[key] = rows[0]
                continue
            issues = [issue for row in rows for issue in row.issues]
            failures[key] = issues or ["missing_output"]
        return accepted, failures

    def _persist_accepted(
        self,
        *,
        batch: BatchRecord,
        mother: MotherVersionRecord,
        product_id: str,
        plan: list[VariantPlanItem],
        accepted: dict[tuple[str, str], PromptQAResult],
        handoff_context: dict[str, Any] | None = None,
    ) -> tuple[int, list[str]]:
        saved = 0
        errors: list[str] = []
        for item in plan:
            key = (item.variant_key, item.mutation_key)
            qa = accepted.get(key)
            if qa is None:
                continue
            if self.repository.prompt_sequence_exists(
                mother.mother_id, mother.version, product_id, item.sequence_no, batch.publish_purpose
            ):
                continue
            identity = (mother.mother_id, mother.version, product_id, item.sequence_no)
            if batch.publish_purpose != "带货":
                identity += (batch.publish_purpose,)
            prompt_id = deterministic_id("prompt", *identity)
            row = PromptRecord(
                prompt_id=prompt_id,
                batch_id=batch.batch_id,
                mother_id=mother.mother_id,
                mother_version=mother.version,
                product_id=product_id,
                variant_type=qa.prompt.variant_type,
                variant_key=qa.prompt.variant_key,
                mutation_key=qa.prompt.mutation_key,
                change_summary=qa.prompt.change_summary,
                full_prompt=qa.prompt.full_prompt,
                prompt_hash=qa.digest,
                sequence_no=item.sequence_no,
                replication_mode=item.replication_mode,
                creative_route=item.creative_route,
                creative_signature=qa.prompt.creative_signature.model_dump(mode="json"),
                planner_version=PLANNER_VERSION,
                publish_purpose=batch.publish_purpose,
                cart_enabled=batch.cart_enabled,
                # A resumed chunk may use newly selected images. Freeze the
                # context read alongside THIS call's image URLs, not the
                # batch's first-run snapshot. Existing rows are never changed.
                handoff_context=({**deepcopy(handoff_context),
                    "execution_summary": execution_summary(mother.contract),
                    "generation_provenance": compilation_provenance(mother),
                    "reference_role_policy": "person_identity=脸部与可见人物身份，不继承原图头发；product=目标假发外观；按冻结reference_manifest绑定全部图片",
                    "mother_core_points": mother_core_points(mother.contract),
                    "prompt_qa": {"mechanical_passed": qa.passed, "assessment_scope": qa.assessment_scope,
                                  "semantic_status": qa.semantic_status, "issues": list(qa.issues)}} if handoff_context else {}),
            )
            if self.repository.save_prompt(row):
                saved += 1
                self.enqueue_prompt_handoffs(row)
            elif not self.repository.prompt_sequence_exists(
                mother.mother_id, mother.version, product_id, item.sequence_no, batch.publish_purpose
            ):
                errors.append(
                    f"{item.variant_key}/{item.mutation_key}: duplicate_prompt_content"
                )
        return saved, errors

    def enqueue_prompt_handoffs(self, row: PromptRecord) -> None:
        """Idempotent delivery recovery from the persisted generation snapshot."""
        payload = {
            "prompt_id": row.prompt_id,
            "mother_id": row.mother_id,
            "mother_version": row.mother_version,
            "product_id": row.product_id,
            "sequence_no": row.sequence_no,
            "replication_mode": row.replication_mode,
            "creative_route": row.creative_route,
            "variant_type": row.variant_type,
            "change_summary": row.change_summary,
            "full_prompt": row.full_prompt,
            "voiceover_text": str(row.creative_signature.get("voiceover_text") or ""),
            "publish_purpose": row.publish_purpose,
            "cart_enabled": row.cart_enabled,
            "handoff_context": deepcopy(row.handoff_context),
            "status": "待审核",
        }
        # Legacy rows lack a generation-time asset snapshot. Only an explicit
        # backfill may reconstruct that context; a no-op rerun must not import
        # historical prompts under today's images or settings.
        operations = ["create_prompt_row"]
        if row.handoff_context:
            operations.append("upsert_script_pool")
        for operation in operations:
            self.repository.enqueue_outbox(make_outbox_record("prompt", row.prompt_id, operation, payload))

    def _all_targets_satisfied(
        self,
        mother: MotherVersionRecord,
        product_ids: Iterable[str],
        per_product_count: int | None,
        publish_purpose: str = "带货",
    ) -> bool:
        for product_id in product_ids:
            relationship = (
                "same_product" if product_id == mother.source_product_id else "cross_product"
            )
            target = self.planner.target_count(relationship, per_product_count)
            existing = {
                row.sequence_no
                for row in self.repository.list_prompts(
                    mother.mother_id, mother.version, product_id, publish_purpose
                )
            }
            if not set(range(1, target + 1)).issubset(existing):
                return False
        return True

    @staticmethod
    def _chunks(items: list[VariantPlanItem], size: int) -> Iterable[list[VariantPlanItem]]:
        for start in range(0, len(items), size):
            yield items[start : start + size]

    def _product_completed(self, batch_id: str, product_id: str) -> bool:
        row = self.repository.get_batch_product(batch_id, product_id)
        return row is not None and row.status == "completed"
