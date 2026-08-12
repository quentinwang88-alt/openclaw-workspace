from __future__ import annotations

from typing import Any, Awaitable, Callable, Iterable, List, Optional, Sequence

from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from ..browser.evidence import EvidenceRecorder
from ..browser.selectors import SelectorRegistry
from ..errors import ExecutorError
from ..handlers import (
    ClaimHandler,
    CollectHandler,
    DuplicateCheckHandler,
    DetailImageTranslationHandler,
    LocateProductHandler,
    LogisticsHandler,
    MainImageTranslationHandler,
    OpenEditorHandler,
    PriceHandler,
    PreflightHandler,
    PublishHandler,
    ShopHandler,
    StockHandler,
    SizeChartTranslationHandler,
    TranslationHandler,
    VerifyPublishHandler,
    WarehouseHandler,
)
from ..handlers.base import Handler, HandlerContext
from ..handlers.draft import matching_editor_session, save_editor_draft
from ..models import AppConfig, ErrorCode, ExecutionResult, ProductTask, Step, StepEvent
from ..services.pricing_engine import PricingEngine
from ..state import (
    AcquisitionReceiptStore,
    CheckpointStore,
    ImageTranslationReceiptStore,
    JsonlStateSink,
    SUBMISSION_FAILED,
    SUBMISSION_PENDING,
    SUBMISSION_PUBLISHED,
    SubmissionReceiptStore,
    approval_fingerprint,
    task_fingerprint,
)


DEFAULT_HANDLERS: Sequence[type] = (
    DuplicateCheckHandler,
    CollectHandler,
    ClaimHandler,
    LocateProductHandler,
    ShopHandler,
    OpenEditorHandler,
    PriceHandler,
    StockHandler,
    WarehouseHandler,
    LogisticsHandler,
    TranslationHandler,
    SizeChartTranslationHandler,
    DetailImageTranslationHandler,
    MainImageTranslationHandler,
    PreflightHandler,
    PublishHandler,
    VerifyPublishHandler,
)


class PublishProductWorkflow:
    def __init__(
        self,
        config: AppConfig,
        selectors: SelectorRegistry,
        evidence: EvidenceRecorder,
        state_sink: JsonlStateSink,
        handlers: Optional[Iterable[Handler]] = None,
        checkpoint_store: Optional[CheckpointStore] = None,
        submission_store: Optional[SubmissionReceiptStore] = None,
        acquisition_store: Optional[AcquisitionReceiptStore] = None,
        image_translation_store: Optional[ImageTranslationReceiptStore] = None,
        draft_saver: Optional[Callable[[Any, int], Awaitable[bool]]] = None,
        editor_matcher: Optional[
            Callable[[Any, str, str], Awaitable[bool]]
        ] = None,
    ) -> None:
        self.config = config
        self.selectors = selectors
        self.evidence = evidence
        self.state_sink = state_sink
        self.checkpoint_store = checkpoint_store
        self.submission_store = submission_store
        self.acquisition_store = acquisition_store
        self.image_translation_store = image_translation_store
        self.draft_saver = draft_saver or save_editor_draft
        self.editor_matcher = editor_matcher or matching_editor_session
        self.handlers: List[Handler] = (
            list(handlers)
            if handlers is not None
            else [handler() for handler in DEFAULT_HANDLERS]
        )
        self.pricing = PricingEngine(config.pricing_rules)
        self._handler_by_step = {handler.step: handler for handler in self.handlers}

    async def execute(
        self,
        page: Any,
        task: ProductTask,
        *,
        allow_publish: bool = False,
        prepare_for_approval: bool = False,
        require_approval: bool = False,
        linear: bool = False,
    ) -> ExecutionResult:
        if prepare_for_approval and allow_publish:
            raise ValueError("Preparation and publishing cannot run in the same execution")
        if require_approval and not allow_publish:
            raise ValueError("Approval can only be required for a publishing execution")
        events: List[StepEvent] = []
        input_fingerprint = task_fingerprint(task)
        context = HandlerContext(
            page=page,
            task=task,
            config=self.config,
            selectors=self.selectors,
            pricing=self.pricing,
            submission_store=self.submission_store,
            acquisition_store=self.acquisition_store,
            image_translation_store=self.image_translation_store,
            linear=linear,
        )
        acquisition_receipt = (
            self.acquisition_store.load(task)
            if linear and self.acquisition_store is not None
            else None
        )
        if acquisition_receipt is not None:
            setattr(context, "acquisition_completed", True)
            setattr(context, "already_claimed", True)
            setattr(context, "acquisition_receipt", acquisition_receipt)
        image_receipt = (
            self.image_translation_store.load(task)
            if self.image_translation_store is not None
            else None
        )
        if image_receipt is not None and image_receipt.draft_saved:
            setattr(context, "image_translation", image_receipt.image_translation)
        checkpoint = (
            self.checkpoint_store.load(task)
            if self.checkpoint_store is not None and not linear
            else None
        )
        approval_checkpoint = (
            checkpoint
            if checkpoint is not None and checkpoint.awaiting_approval
            else None
        )
        if checkpoint is not None and checkpoint.awaiting_approval and not (
            prepare_for_approval or require_approval
        ):
            checkpoint = None
        start_step = task.current_step
        submission_receipt = (
            self.submission_store.load(task) if self.submission_store else None
        )
        if (
            allow_publish
            and start_step == Step.VERIFY
            and submission_receipt is None
            and self.submission_store is not None
        ):
            # An explicit verification-only execution is authoritative evidence
            # that submission already happened in an older executor version.
            submission_receipt = self.submission_store.record_submitted(
                task,
                submission_signal="legacy_verification_only",
            )
        if allow_publish and submission_receipt is not None:
            if submission_receipt.status == SUBMISSION_PENDING:
                start_step = Step.VERIFY
                setattr(context, "publish_submitted", True)
            elif submission_receipt.status == SUBMISSION_PUBLISHED:
                start_step = Step.VERIFY
                setattr(context, "publish_submitted", True)
                setattr(
                    context,
                    "platform_product_id",
                    submission_receipt.platform_product_id,
                )
                setattr(context, "published_status", "发布成功")
        resume_in_live_editor = False
        shop_name = str(self.config.shops[task.target_shop]["display_name"])
        if (
            start_step is None
            and checkpoint is not None
            and checkpoint.resume_step is not None
        ):
            if checkpoint.draft_saved:
                start_step = checkpoint.resume_step
            elif checkpoint.editor_session_retained:
                resume_in_live_editor = await self.editor_matcher(
                    page, task.miaoshou_product_id, shop_name
                )
                if resume_in_live_editor:
                    start_step = checkpoint.resume_step
        if checkpoint is not None and (checkpoint.draft_saved or resume_in_live_editor):
            setattr(context, "image_translation", checkpoint.image_translation)
        selected = self._selected_handlers(start_step, allow_publish=allow_publish)
        total_retries = 0
        current_step: Optional[Step] = start_step
        completed_steps: List[Step] = []
        try:
            if (
                allow_publish
                and submission_receipt is not None
                and submission_receipt.status == SUBMISSION_PUBLISHED
                and submission_receipt.platform_product_id
            ):
                return ExecutionResult(
                    task_id=task.task_id,
                    success=True,
                    current_step=Step.VERIFY,
                    platform_product_id=submission_receipt.platform_product_id,
                    published_status="发布成功",
                    events=events,
                )
            if (
                allow_publish
                and submission_receipt is not None
                and submission_receipt.status == SUBMISSION_FAILED
                and not task.allow_republish
            ):
                raise ExecutorError(
                    ErrorCode.PUBLISH_FAILED,
                    "A previous submission was verified as failed; a new approved "
                    "task is required before republishing",
                    step=Step.VERIFY,
                )
            if require_approval and (
                submission_receipt is None
                and (
                    approval_checkpoint is None
                    or not approval_checkpoint.approval_fingerprint
                    or not approval_checkpoint.draft_saved
                )
            ):
                raise ExecutorError(
                    ErrorCode.PUBLISH_VALIDATION_FAILED,
                    "No valid saved approval checkpoint exists; prepare the product again",
                    step=Step.PREFLIGHT,
                )
            if (
                allow_publish
                and start_step not in (None, Step.CHECK_DUPLICATE, Step.VERIFY)
                and Step.CHECK_DUPLICATE in self._handler_by_step
            ):
                # Duplicate verification navigates to publish history, so it must
                # run before reopening the saved editor rather than inside PUBLISH.
                _, retries = await self._run_with_retry(
                    self._handler_by_step[Step.CHECK_DUPLICATE], context
                )
                total_retries += retries
            if start_step and not resume_in_live_editor:
                # Restore deterministic prerequisites before a resumed step.
                prerequisites = self._resume_prerequisites(start_step)
                for prelude_step in prerequisites:
                    _, retries = await self._run_with_retry(
                        self._handler_by_step[prelude_step], context
                    )
                    total_retries += retries
            for handler in selected:
                if handler.step == Step.PUBLISH and require_approval:
                    current_approval = approval_fingerprint(
                        getattr(context, "preflight_snapshot", {})
                    )
                    if current_approval != approval_checkpoint.approval_fingerprint:
                        raise ExecutorError(
                            ErrorCode.PUBLISH_VALIDATION_FAILED,
                            "Current draft differs from the approved preflight snapshot",
                            step=Step.PREFLIGHT,
                        )
                    setattr(
                        context,
                        "approval_fingerprint",
                        approval_checkpoint.approval_fingerprint,
                    )
                current_step = handler.step
                started = StepEvent(
                    task_id=task.task_id, step=handler.step, state="STARTED"
                )
                self._emit(started, events)
                attempt, retries = await self._run_with_retry(handler, context)
                total_retries += retries
                if (
                    linear
                    and handler.step == Step.CLAIM
                    and task.source_url
                    and self.acquisition_store is not None
                ):
                    acquisition_receipt = self.acquisition_store.record_acquired(task)
                    setattr(context, "acquisition_completed", True)
                    setattr(context, "acquisition_receipt", acquisition_receipt)
                if (
                    handler.step == Step.PUBLISH
                    and self.submission_store is not None
                    and getattr(context, "publish_submitted", False)
                    and self.submission_store.load(task) is None
                ):
                    snapshot = getattr(context, "preflight_snapshot", {}) or {}
                    self.submission_store.record_submitted(
                        task,
                        approval_fingerprint=getattr(
                            context, "approval_fingerprint", ""
                        ),
                        product_title=str(snapshot.get("title", "")),
                        submit_page_url=str(getattr(page, "url", "")),
                        submission_signal="publish_handler_completed",
                    )
                if (
                    handler.step == Step.VERIFY
                    and self.submission_store is not None
                    and getattr(context, "platform_product_id", "")
                ):
                    self.submission_store.mark_published(
                        task, getattr(context, "platform_product_id")
                    )
                completed = StepEvent(
                    task_id=task.task_id,
                    step=handler.step,
                    state="COMPLETED",
                    attempt=attempt,
                )
                self._emit(completed, events)
                completed_steps.append(handler.step)
            if prepare_for_approval:
                snapshot = dict(getattr(context, "preflight_snapshot", {}))
                if not snapshot:
                    raise ExecutorError(
                        ErrorCode.PREFLIGHT_FAILED,
                        "Approval preparation produced no preflight snapshot",
                        step=Step.PREFLIGHT,
                    )
                screenshot = await self.evidence.capture(
                    page,
                    task.task_id,
                    {
                        "task_id": task.task_id,
                        "step": Step.PREFLIGHT,
                        "status": "WAITING_APPROVAL_NOT_PUBLISHED",
                        "preflight": snapshot,
                    },
                )
                snapshot["approval_screenshot"] = screenshot
                setattr(context, "preflight_snapshot", snapshot)
                if self.checkpoint_store is None:
                    raise ExecutorError(
                        ErrorCode.PREFLIGHT_FAILED,
                        "Approval preparation requires a checkpoint store",
                        step=Step.PREFLIGHT,
                    )
                draft_saved = await self.draft_saver(
                    page, self.config.browser.navigation_timeout_ms
                )
                if not draft_saved:
                    raise ExecutorError(
                        ErrorCode.PREFLIGHT_FAILED,
                        "Miaoshou refused to save the prepared draft",
                        step=Step.PREFLIGHT,
                    )
                fingerprint = approval_fingerprint(snapshot)
                self.checkpoint_store.save(
                    task,
                    resume_step=Step.PREFLIGHT,
                    completed_steps=completed_steps,
                    draft_saved=True,
                    image_translation=getattr(context, "image_translation", {}),
                    awaiting_approval=True,
                    approval_fingerprint=fingerprint,
                    approval_snapshot=snapshot,
                    task_fingerprint_override=input_fingerprint,
                )
                return ExecutionResult(
                    task_id=task.task_id,
                    success=True,
                    current_step=Step.PREFLIGHT,
                    published_status="WAITING_APPROVAL_NOT_PUBLISHED",
                    approval_fingerprint=fingerprint,
                    retry_count=total_retries,
                    preflight=snapshot,
                    events=events,
                )
            if self.checkpoint_store is not None and not linear:
                self.checkpoint_store.clear(task.task_id)
            return ExecutionResult(
                task_id=task.task_id,
                success=True,
                current_step=current_step,
                platform_product_id=getattr(context, "platform_product_id", ""),
                published_status=(
                    getattr(context, "published_status", "")
                    if allow_publish
                    else "DRY_RUN_COMPLETE_NOT_PUBLISHED"
                ),
                retry_count=total_retries,
                preflight=getattr(context, "preflight_snapshot", {}),
                events=events,
            )
        except Exception as raw_exc:
            total_retries += int(getattr(raw_exc, "retry_count", 0))
            error = self._normalize_error(raw_exc, current_step)
            failed_step = error.step or current_step or Step.LOCATE_PRODUCT
            failed = StepEvent(
                task_id=task.task_id,
                step=failed_step,
                state="FAILED",
                message=error.message,
            )
            self._emit(failed, events)
            page_url = getattr(page, "url", "")
            screenshot = await self.evidence.capture(
                page,
                task.task_id,
                {
                    "task_id": task.task_id,
                    "step": failed_step,
                    "error_code": error.code,
                    "error_message": error.message,
                    "page_url": page_url,
                    "retry_count": total_retries,
                },
            )
            draft_saved = False
            may_save_draft = self._can_save_failed_draft(failed_step) and not (
                failed_step == Step.PUBLISH
                and getattr(context, "publish_submitted", False)
            )
            if may_save_draft:
                try:
                    draft_saved = await self.draft_saver(
                        page, self.config.browser.navigation_timeout_ms
                    )
                except Exception:
                    draft_saved = False
            if (
                draft_saved
                and self.image_translation_store is not None
                and getattr(context, "image_translation", {})
            ):
                self.image_translation_store.update(
                    task, getattr(context, "image_translation", {})
                )
                self.image_translation_store.mark_draft_saved(task)
            editor_session_retained = False
            if not draft_saved:
                editor_session_retained = await self.editor_matcher(
                    page, task.miaoshou_product_id, shop_name
                )
            if self.checkpoint_store is not None and not linear:
                self.checkpoint_store.save(
                    task,
                    resume_step=failed_step,
                    completed_steps=completed_steps,
                    draft_saved=draft_saved,
                    editor_session_retained=editor_session_retained,
                    image_translation=getattr(context, "image_translation", {}),
                    task_fingerprint_override=input_fingerprint,
                )
            pending_verification = (
                (
                    getattr(context, "publish_submitted", False)
                    and not (
                        failed_step == Step.VERIFY
                        and error.code == ErrorCode.PUBLISH_FAILED
                    )
                )
                or (
                    failed_step == Step.VERIFY
                    and error.code != ErrorCode.PUBLISH_FAILED
                    and start_step == Step.VERIFY
                )
            )
            if self.submission_store is not None and failed_step == Step.VERIFY:
                if error.code == ErrorCode.PUBLISH_FAILED:
                    self.submission_store.mark_failed(task, error.message)
                elif pending_verification:
                    self.submission_store.mark_verification_attempt(
                        task, error.message
                    )
            return ExecutionResult(
                task_id=task.task_id,
                success=False,
                current_step=failed_step,
                error_code=error.code,
                error_message=error.message,
                published_status=(
                    "SUBMITTED_PENDING_VERIFICATION"
                    if pending_verification
                    else ""
                ),
                page_url=page_url,
                screenshot=screenshot,
                retry_count=total_retries,
                preflight=getattr(context, "preflight_snapshot", {}),
                events=events,
            )

    @staticmethod
    def _can_save_failed_draft(step: Step) -> bool:
        return step in {
            Step.SET_PRICE,
            Step.SET_STOCK,
            Step.SET_WAREHOUSE,
            Step.SET_LOGISTICS,
            Step.TRANSLATE,
            Step.TRANSLATE_SIZE_CHART,
            Step.TRANSLATE_DETAIL_IMAGES,
            Step.TRANSLATE_MAIN_IMAGES,
            Step.PREFLIGHT,
            Step.PUBLISH,
        }

    def _selected_handlers(
        self, start_step: Optional[Step], *, allow_publish: bool
    ) -> List[Handler]:
        handlers = self.handlers
        if start_step is not None:
            handlers = [
                handler
                for handler in handlers
                if self._step_index(handler.step) >= self._step_index(start_step)
            ]
        if not allow_publish:
            handlers = [
                handler
                for handler in handlers
                if handler.step not in (Step.PUBLISH, Step.VERIFY)
            ]
        return handlers

    def _step_index(self, step: Step) -> int:
        order = [handler.step for handler in self.handlers]
        return order.index(step)

    def _resume_prerequisites(self, start_step: Step) -> List[Step]:
        if start_step == Step.VERIFY:
            return []
        candidates: List[Step] = []
        if start_step == Step.SET_SHOP:
            candidates = [Step.LOCATE_PRODUCT]
        elif start_step == Step.OPEN_EDITOR:
            candidates = [Step.LOCATE_PRODUCT, Step.SET_SHOP]
        elif (
            Step.OPEN_EDITOR in self._handler_by_step
            and self._step_index(start_step) > self._step_index(Step.OPEN_EDITOR)
        ):
            candidates = [Step.LOCATE_PRODUCT, Step.SET_SHOP, Step.OPEN_EDITOR]
        return [step for step in candidates if step in self._handler_by_step]

    async def _run_with_retry(
        self, handler: Handler, context: HandlerContext
    ) -> tuple[int, int]:
        max_attempts = int(self.config.retry.get("max_attempts", 3))
        delays = list(self.config.retry.get("delays_seconds", [0, 3, 10]))
        retryable_codes = {
            ErrorCode(value)
            for value in self.config.retry.get(
                "retryable_codes", ["NETWORK_ERROR", "ELEMENT_TIMEOUT"]
            )
        }
        for attempt in range(1, max_attempts + 1):
            try:
                await handler.run(context)
                return attempt, attempt - 1
            except Exception as raw_exc:
                error = self._normalize_error(raw_exc, handler.step)
                may_retry = error.retryable or error.code in retryable_codes
                if not may_retry or attempt >= max_attempts:
                    setattr(error, "retry_count", attempt - 1)
                    raise error
                delay = float(delays[min(attempt, len(delays) - 1)]) if delays else 0
                if delay:
                    await context.page.wait_for_timeout(int(delay * 1000))
        raise AssertionError("retry loop exhausted unexpectedly")

    def _normalize_error(
        self, exc: Exception, step: Optional[Step]
    ) -> ExecutorError:
        if isinstance(exc, ExecutorError):
            if exc.step is None:
                exc.step = step
            return exc
        if isinstance(exc, PlaywrightTimeoutError):
            return ExecutorError(
                ErrorCode.ELEMENT_TIMEOUT,
                str(exc),
                step=step,
                retryable=True,
            )
        lowered = str(exc).lower()
        if any(token in lowered for token in ("network", "connection", "net::err_")):
            return ExecutorError(
                ErrorCode.NETWORK_ERROR,
                str(exc),
                step=step,
                retryable=True,
            )
        return ExecutorError(ErrorCode.UNKNOWN_ERROR, str(exc), step=step)

    def _emit(self, event: StepEvent, events: List[StepEvent]) -> None:
        events.append(event)
        self.state_sink.emit(event)
