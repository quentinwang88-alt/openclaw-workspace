"""Task Intake: create idempotent OPV content tasks (Stage A).

Scope for Phase 0: validate the request, resolve account-bound reference data
(market pack / render preset / persona refs), snapshot them next to the
product, and create the ``opv_content_task`` row exactly once per
``idempotency_key``.

The idempotency contract:

- explicit ``idempotency_key`` wins (stored as its sha256, fitting CHAR(64));
- otherwise the key is derived from the canonical request content
  (account, product, source, theme, topic, UTC business date), so resubmitting
  the same input on the same day cannot create a second task, while the same
  product on a later day intentionally produces fresh content.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

from domain.models import AccountProfile, ContentTask, MarketPack, RenderPreset, ThemeCatalog, generate_prefixed_id, utc_now
from domain.statuses import STAGE_INTAKE, TASK_DRAFT

INTAKE_CONTRACT = "opv-intake-v1"

ALLOWED_ACCOUNT_STATUSES = ("active", "testing")
ALLOWED_TASK_PRIORITIES = ("low", "normal", "high")
MIN_SHOT_COUNT = 1
MAX_SHOT_COUNT = 10
DEFAULT_SHOT_COUNT = 5


class TaskIntakeError(ValueError):
    pass


@dataclass
class TaskRequest:
    account_id: str
    product_id: Optional[str]
    product_snapshot: Dict[str, Any]
    media_kind: str = "video"
    category_key: Optional[str] = None
    product_mode: str = "SOFT_PRODUCT"
    theme_id: Optional[str] = None
    topic_text: Optional[str] = None
    source_type: str = "manual"
    source_record_id: Optional[str] = None
    feishu_record_id: Optional[str] = None
    requested_shot_count: int = DEFAULT_SHOT_COUNT
    priority: str = "normal"
    created_by: str = "manual"
    idempotency_key: Optional[str] = None
    # UTC datetime overriding "today" for the derived key (tests/seeding).
    business_moment: Optional[datetime] = None


@dataclass
class IntakeResult:
    task: ContentTask
    created: bool
    market_pack: MarketPack
    account: AccountProfile
    render_preset: Optional[RenderPreset]
    theme: Optional[ThemeCatalog] = None


def derive_idempotency_key(
    request: TaskRequest, moment: Optional[datetime] = None
) -> str:
    """sha256 hex (64 chars) of the canonical request + UTC business date."""
    stamp = (moment or request.business_moment or utc_now()).strftime("%Y-%m-%d")
    canonical = json.dumps(
        {
            "account_id": request.account_id,
            "product_id": request.product_id,
            "media_kind": request.media_kind,
            "category_key": request.category_key,
            "product_mode": request.product_mode,
            "source_type": request.source_type,
            "source_record_id": request.source_record_id,
            "theme_id": request.theme_id,
            "topic_text": request.topic_text,
            "business_date": stamp,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class TaskIntakeService:
    def __init__(self, repository, id_generator=None):
        """``repository`` only needs the read/upsert/create methods used below
        (any object satisfying the RdsRepository surface works; tests use an
        in-memory fake)."""
        self._repository = repository
        self._id_generator = id_generator or (
            lambda: generate_prefixed_id("opv_task")
        )

    # ------------------------------------------------------------------

    def create_task(self, request: TaskRequest) -> IntakeResult:
        self._validate_request(request)

        account = self._repository.get_account_profile(request.account_id)
        if account is None:
            raise TaskIntakeError(f"account {request.account_id} not found")
        if account.status not in ALLOWED_ACCOUNT_STATUSES:
            raise TaskIntakeError(
                f"account {account.account_id} status {account.status!r} "
                "is not allowed for intake"
            )

        pack = (
            self._repository.get_market_pack(account.default_market_pack_id)
            if account.default_market_pack_id
            else None
        )
        if pack is None:
            raise TaskIntakeError(
                f"account {account.account_id} has no resolvable default market pack"
            )
        if pack.status != "active":
            raise TaskIntakeError(
                f"market pack {pack.market_pack_id} status {pack.status!r} is not active"
            )

        preset = (
            self._repository.get_render_preset(account.default_render_preset_id)
            if account.default_render_preset_id
            else None
        )
        if preset is None and request.media_kind != "native_photo":
            raise TaskIntakeError(
                f"account {account.account_id} has no resolvable default render preset"
            )

        theme = None
        if request.theme_id:
            theme = self._repository.get_theme(request.theme_id)
            if theme is None:
                raise TaskIntakeError(f"theme {request.theme_id} not found")

        if request.idempotency_key:
            idempotency_key = hashlib.sha256(
                request.idempotency_key.encode("utf-8")
            ).hexdigest()
        else:
            idempotency_key = derive_idempotency_key(request)

        snapshot = {
            "intake_contract": INTAKE_CONTRACT,
            "product": request.product_snapshot,
            "intake_context": {
                "account_id": account.account_id,
                "account_status": account.status,
                "persona_ref_id": account.persona_ref_id,
                "allowed_look_refs": account.allowed_look_refs_json,
                "allowed_scene_refs": account.allowed_scene_refs_json,
                "core_scene_refs": account.core_scene_refs_json,
                "market_pack_id": pack.market_pack_id,
                "market_pack_version": pack.pack_version,
                "render_preset_id": preset.render_preset_id if preset else None,
                "target_country": pack.target_country,
                "target_locale": pack.target_locale,
            },
        }

        task = ContentTask(
            task_id=self._id_generator(),
            idempotency_key=idempotency_key,
            source_type=request.source_type,
            source_record_id=request.source_record_id,
            account_id=account.account_id,
            product_id=request.product_id,
            media_kind=request.media_kind,
            category_key=request.category_key,
            product_mode=request.product_mode,
            product_snapshot_json=snapshot,
            target_country=pack.target_country,
            target_locale=pack.target_locale,
            market_pack_id=pack.market_pack_id,
            theme_id=theme.theme_id if theme else None,
            topic_text=request.topic_text,
            task_status=TASK_DRAFT,
            current_stage=STAGE_INTAKE,
            priority=request.priority,
            requested_shot_count=request.requested_shot_count,
            created_by=request.created_by,
            feishu_record_id=request.feishu_record_id,
        )
        task, created = self._repository.create_task_idempotent(task)
        if (
            not created
            and task.task_status == TASK_DRAFT
            and task.current_stage == STAGE_INTAKE
        ):
            # A planner failure can leave an idempotent row at intake with a
            # stale product pack snapshot.  Refresh only this untouched state;
            # planned/rendered tasks remain immutable for reproducibility.
            updater = getattr(
                self._repository, "update_task_product_snapshot", None
            )
            if callable(updater):
                updater(task.task_id, snapshot)
                refreshed = self._repository.get_task(task.task_id)
                if refreshed is not None:
                    task = refreshed
        return IntakeResult(
            task=task,
            created=created,
            market_pack=pack,
            account=account,
            render_preset=preset,
            theme=theme,
        )

    # ------------------------------------------------------------------

    @staticmethod
    def _validate_request(request: TaskRequest) -> None:
        if not request.account_id or not request.account_id.strip():
            raise TaskIntakeError("account_id is required")
        if request.media_kind not in {"video", "native_photo"}:
            raise TaskIntakeError("media_kind must be video or native_photo")
        if request.product_mode not in {"NO_PRODUCT", "SOFT_PRODUCT", "PRODUCT_LED"}:
            raise TaskIntakeError("unknown product_mode")
        if request.media_kind == "native_photo" and not str(request.category_key or "").strip():
            raise TaskIntakeError("native_photo requires category_key")
        if request.product_mode != "NO_PRODUCT" and not str(request.product_id or "").strip():
            raise TaskIntakeError("product_id is required unless product_mode=NO_PRODUCT")
        if not isinstance(request.product_snapshot, dict):
            raise TaskIntakeError("product_snapshot must be an object")
        reference_images = request.product_snapshot.get("reference_images")
        if (request.product_mode != "NO_PRODUCT"
                and (not isinstance(reference_images, list) or not reference_images)):
            raise TaskIntakeError(
                "product_snapshot.reference_images must contain at least one "
                "product reference image"
            )
        if not MIN_SHOT_COUNT <= request.requested_shot_count <= MAX_SHOT_COUNT:
            raise TaskIntakeError(
                f"requested_shot_count must be {MIN_SHOT_COUNT}-{MAX_SHOT_COUNT}, "
                f"got {request.requested_shot_count}"
            )
        if request.priority not in ALLOWED_TASK_PRIORITIES:
            raise TaskIntakeError(
                f"priority must be one of {ALLOWED_TASK_PRIORITIES}, "
                f"got {request.priority!r}"
            )
