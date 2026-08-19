from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from .models import ProductTask, Step, StepEvent


SUBMISSION_PENDING = "SUBMITTED"
SUBMISSION_PUBLISHED = "PUBLISHED"
SUBMISSION_FAILED = "FAILED"


class JsonlStateSink:
    """Append-only local progress stream for OpenClaw and resume tooling."""

    def __init__(self, path: Path) -> None:
        self.path = path.resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, event: StepEvent) -> None:
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(event.model_dump_json() + "\n")


def task_fingerprint(task: ProductTask) -> str:
    """Identify the exact task inputs that a saved draft belongs to."""
    payload = task.model_dump(mode="json", exclude={"current_step"})
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def acquisition_fingerprint(task: ProductTask) -> str:
    """Identify the claimed Miaoshou item without volatile listing inputs."""
    payload = {
        "miaoshou_product_id": task.miaoshou_product_id,
        "target_shop": task.target_shop,
        "market": task.market,
    }
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def image_translation_fingerprint(task: ProductTask) -> str:
    payload = {
        "acquisition": acquisition_fingerprint(task),
        "size_chart_source": (
            task.size_chart_file_token
            or task.size_chart_url
            or task.size_chart_path
        ),
        "size_chart_file_name": task.size_chart_file_name,
        "market": task.market,
        "category_group": task.category_group,
    }
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def approval_payload(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """Select stable, business-relevant fields for approval integrity checks."""
    translations = {}
    for region, details in sorted((snapshot.get("image_translation") or {}).items()):
        translations[region] = {
            key: details.get(key)
            for key in (
                "status",
                "image_count",
                "target_language",
                "result_fingerprint",
            )
            if details.get(key) not in (None, "")
        }
    return {
        "shop": snapshot.get("shop", ""),
        "market": snapshot.get("market", ""),
        "pricing": snapshot.get("pricing", {}),
        "title": snapshot.get("title", ""),
        "skus": [
            {
                "label": str(sku.get("label", "")).split(" CNY CNY", 1)[0],
                **{
                    key: sku.get(key, "")
                    for key in (
                        "purchase_cny",
                        "cny_price",
                        "expected_cny_price",
                        "stock",
                        "weight_kg",
                    )
                },
            }
            for sku in snapshot.get("skus", [])
        ],
        "package": snapshot.get("package", {}),
        "size_chart_count": snapshot.get("size_chart_count", 0),
        "image_translation": translations,
    }


def approval_fingerprint(snapshot: Dict[str, Any]) -> str:
    encoded = json.dumps(
        approval_payload(snapshot),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


class TaskCheckpoint(BaseModel):
    task_id: str
    task_fingerprint: str
    resume_step: Optional[Step] = None
    completed_steps: List[Step] = Field(default_factory=list)
    draft_saved: bool = False
    editor_session_retained: bool = False
    image_translation: Dict[str, Any] = Field(default_factory=dict)
    awaiting_approval: bool = False
    approval_fingerprint: str = ""
    approval_snapshot: Dict[str, Any] = Field(default_factory=dict)
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class CheckpointStore:
    """Atomic per-task checkpoints; only saved drafts are eligible for resume."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory.resolve()
        self.directory.mkdir(parents=True, exist_ok=True)

    def path_for(self, task_id: str) -> Path:
        safe_id = "".join(
            character if character.isalnum() or character in "-_." else "_"
            for character in task_id
        )
        return self.directory / f"{safe_id}.json"

    def load(self, task: ProductTask) -> Optional[TaskCheckpoint]:
        path = self.path_for(task.task_id)
        if not path.exists():
            return None
        try:
            checkpoint = TaskCheckpoint.model_validate_json(
                path.read_text(encoding="utf-8")
            )
        except Exception:
            return None
        if checkpoint.task_fingerprint != task_fingerprint(task):
            return None
        return checkpoint

    def save(
        self,
        task: ProductTask,
        *,
        resume_step: Step,
        completed_steps: List[Step],
        draft_saved: bool,
        editor_session_retained: bool = False,
        image_translation: Optional[Dict[str, Any]] = None,
        awaiting_approval: bool = False,
        approval_fingerprint: str = "",
        approval_snapshot: Optional[Dict[str, Any]] = None,
        task_fingerprint_override: str = "",
    ) -> TaskCheckpoint:
        checkpoint = TaskCheckpoint(
            task_id=task.task_id,
            task_fingerprint=task_fingerprint_override or task_fingerprint(task),
            resume_step=resume_step,
            completed_steps=list(dict.fromkeys(completed_steps)),
            draft_saved=draft_saved,
            editor_session_retained=editor_session_retained,
            image_translation=image_translation or {},
            awaiting_approval=awaiting_approval,
            approval_fingerprint=approval_fingerprint,
            approval_snapshot=approval_snapshot or {},
        )
        path = self.path_for(task.task_id)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(checkpoint.model_dump_json(indent=2), encoding="utf-8")
        temporary.replace(path)
        return checkpoint

    def clear(self, task_id: str) -> None:
        path = self.path_for(task_id)
        if path.exists():
            path.unlink()


class ImageTranslationReceipt(BaseModel):
    task_id: str
    cache_fingerprint: str
    image_translation: Dict[str, Any] = Field(default_factory=dict)
    draft_saved: bool = False
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ImageTranslationReceiptStore:
    """Persist image fingerprints for safe reuse after a saved linear draft."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory.resolve()
        self.directory.mkdir(parents=True, exist_ok=True)

    def path_for(self, task_id: str) -> Path:
        safe_id = "".join(
            character if character.isalnum() or character in "-_." else "_"
            for character in task_id
        )
        return self.directory / f"{safe_id}.json"

    def load(self, task: ProductTask) -> Optional[ImageTranslationReceipt]:
        path = self.path_for(task.task_id)
        if not path.exists():
            return None
        try:
            receipt = ImageTranslationReceipt.model_validate_json(
                path.read_text(encoding="utf-8")
            )
        except Exception:
            return None
        if receipt.cache_fingerprint != image_translation_fingerprint(task):
            return None
        return receipt

    def update(
        self,
        task: ProductTask,
        image_translation: Dict[str, Any],
        *,
        dirty: bool = False,
    ) -> ImageTranslationReceipt:
        receipt = self.load(task) or ImageTranslationReceipt(
            task_id=task.task_id,
            cache_fingerprint=image_translation_fingerprint(task),
        )
        receipt.image_translation = dict(image_translation)
        if dirty:
            receipt.draft_saved = False
        receipt.updated_at = datetime.now(timezone.utc)
        self._write(task, receipt)
        return receipt

    def mark_draft_saved(self, task: ProductTask) -> Optional[ImageTranslationReceipt]:
        receipt = self.load(task)
        if receipt is None:
            return None
        receipt.draft_saved = True
        receipt.updated_at = datetime.now(timezone.utc)
        self._write(task, receipt)
        return receipt

    def _write(self, task: ProductTask, receipt: ImageTranslationReceipt) -> None:
        path = self.path_for(task.task_id)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(receipt.model_dump_json(indent=2), encoding="utf-8")
        temporary.replace(path)


def submission_idempotency_key(task: ProductTask) -> str:
    """Identify one logical listing independently of its Feishu record ID."""
    payload = {
        "miaoshou_product_id": task.miaoshou_product_id.strip(),
        "target_shop": task.target_shop.strip().upper(),
        "market": task.market.strip().upper(),
    }
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


class SubmissionReceipt(BaseModel):
    """Durable proof that the irreversible publish click has already happened."""

    idempotency_key: str
    task_id: str
    miaoshou_product_id: str
    target_shop: str
    market: str
    status: str = SUBMISSION_PENDING
    approval_fingerprint: str = ""
    product_title: str = ""
    platform_product_id: str = ""
    submit_page_url: str = ""
    submission_signal: str = ""
    submitted_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_verified_at: Optional[datetime] = None
    verification_attempts: int = 0
    last_verification_error: str = ""
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class SubmissionReceiptStore:
    """Atomic cross-restart idempotency ledger keyed by source + shop + market."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory.resolve()
        self.directory.mkdir(parents=True, exist_ok=True)

    def path_for(self, task: ProductTask) -> Path:
        return self.directory / f"{submission_idempotency_key(task)}.json"

    def load(self, task: ProductTask) -> Optional[SubmissionReceipt]:
        path = self.path_for(task)
        if not path.exists():
            return None
        try:
            receipt = SubmissionReceipt.model_validate_json(
                path.read_text(encoding="utf-8")
            )
        except Exception:
            return None
        if receipt.idempotency_key != submission_idempotency_key(task):
            return None
        return receipt

    def record_submitted(
        self,
        task: ProductTask,
        *,
        approval_fingerprint: str = "",
        product_title: str = "",
        submit_page_url: str = "",
        submission_signal: str = "",
    ) -> SubmissionReceipt:
        existing = self.load(task)
        if existing is not None and existing.status == SUBMISSION_PUBLISHED:
            return existing
        if existing is not None and existing.status == SUBMISSION_PENDING:
            existing.approval_fingerprint = (
                approval_fingerprint or existing.approval_fingerprint
            )
            existing.product_title = product_title or existing.product_title
            existing.submit_page_url = submit_page_url or existing.submit_page_url
            existing.submission_signal = (
                submission_signal or existing.submission_signal
            )
            existing.updated_at = datetime.now(timezone.utc)
            self._write(task, existing)
            return existing
        receipt = SubmissionReceipt(
            idempotency_key=submission_idempotency_key(task),
            task_id=task.task_id,
            miaoshou_product_id=task.miaoshou_product_id,
            target_shop=task.target_shop,
            market=task.market,
            approval_fingerprint=approval_fingerprint,
            product_title=product_title,
            submit_page_url=submit_page_url,
            submission_signal=submission_signal,
        )
        self._write(task, receipt)
        return receipt

    def mark_verification_attempt(
        self, task: ProductTask, error_message: str
    ) -> Optional[SubmissionReceipt]:
        receipt = self.load(task)
        if receipt is None:
            return None
        now = datetime.now(timezone.utc)
        receipt.last_verified_at = now
        receipt.verification_attempts += 1
        receipt.last_verification_error = error_message[:1000]
        receipt.updated_at = now
        self._write(task, receipt)
        return receipt

    def mark_published(
        self, task: ProductTask, platform_product_id: str
    ) -> Optional[SubmissionReceipt]:
        receipt = self.load(task)
        if receipt is None:
            return None
        now = datetime.now(timezone.utc)
        receipt.status = SUBMISSION_PUBLISHED
        receipt.platform_product_id = platform_product_id
        receipt.last_verified_at = now
        receipt.verification_attempts += 1
        receipt.last_verification_error = ""
        receipt.updated_at = now
        self._write(task, receipt)
        return receipt

    def mark_failed(
        self, task: ProductTask, error_message: str
    ) -> Optional[SubmissionReceipt]:
        receipt = self.load(task)
        if receipt is None:
            return None
        now = datetime.now(timezone.utc)
        receipt.status = SUBMISSION_FAILED
        receipt.last_verified_at = now
        receipt.verification_attempts += 1
        receipt.last_verification_error = error_message[:1000]
        receipt.updated_at = now
        self._write(task, receipt)
        return receipt

    def _write(self, task: ProductTask, receipt: SubmissionReceipt) -> None:
        path = self.path_for(task)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(receipt.model_dump_json(indent=2), encoding="utf-8")
        temporary.replace(path)


class AcquisitionReceipt(BaseModel):
    """Proof that collection and platform claim completed for this exact task."""

    task_id: str
    task_fingerprint: str
    miaoshou_product_id: str
    target_shop: str
    # Stable identity from Miaoshou's claimed-product row. Older receipts do
    # not have it, so keep the field optional for backward compatibility.
    miaoshou_row_id: str = ""
    acquired_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class AcquisitionReceiptStore:
    """Prevent linear retries from collecting or claiming the same source again."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory.resolve()
        self.directory.mkdir(parents=True, exist_ok=True)

    def path_for(self, task_id: str) -> Path:
        safe_id = "".join(
            character if character.isalnum() or character in "-_." else "_"
            for character in task_id
        )
        return self.directory / f"{safe_id}.json"

    def load(self, task: ProductTask) -> Optional[AcquisitionReceipt]:
        path = self.path_for(task.task_id)
        if not path.exists():
            return None
        try:
            receipt = AcquisitionReceipt.model_validate_json(
                path.read_text(encoding="utf-8")
            )
        except Exception:
            return None
        expected = acquisition_fingerprint(task)
        if receipt.task_fingerprint != expected:
            # Migrate receipts from the older full-task fingerprint. Price,
            # stock, and expiring 1688 share parameters do not change which
            # Miaoshou row was collected and must never trigger re-collection.
            if (
                receipt.miaoshou_product_id != task.miaoshou_product_id
                or receipt.target_shop != task.target_shop
            ):
                return None
            receipt.task_fingerprint = expected
            receipt.updated_at = datetime.now(timezone.utc)
            self._write(task, receipt)
        return receipt

    def record_acquired(
        self, task: ProductTask, *, miaoshou_row_id: str = ""
    ) -> AcquisitionReceipt:
        existing = self.load(task)
        if existing is not None:
            if miaoshou_row_id and not existing.miaoshou_row_id:
                existing.miaoshou_row_id = miaoshou_row_id
                existing.updated_at = datetime.now(timezone.utc)
                self._write(task, existing)
            elif (
                miaoshou_row_id
                and existing.miaoshou_row_id
                and existing.miaoshou_row_id != miaoshou_row_id
            ):
                raise ValueError(
                    "Refusing to replace the locked Miaoshou row identity "
                    f"{existing.miaoshou_row_id!r} with {miaoshou_row_id!r}"
                )
            return existing
        receipt = AcquisitionReceipt(
            task_id=task.task_id,
            task_fingerprint=acquisition_fingerprint(task),
            miaoshou_product_id=task.miaoshou_product_id,
            target_shop=task.target_shop,
            miaoshou_row_id=miaoshou_row_id,
        )
        self._write(task, receipt)
        return receipt

    def _write(self, task: ProductTask, receipt: AcquisitionReceipt) -> None:
        path = self.path_for(task.task_id)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(receipt.model_dump_json(indent=2), encoding="utf-8")
        temporary.replace(path)
