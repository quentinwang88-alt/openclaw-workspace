from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from miaoshou_auto_listing.browser.evidence import EvidenceRecorder
from miaoshou_auto_listing.browser.selectors import SelectorRegistry
from miaoshou_auto_listing.config import load_config
from miaoshou_auto_listing.errors import ExecutorError
from miaoshou_auto_listing.handlers.base import Handler
from miaoshou_auto_listing.handlers.product import (
    LocateProductHandler,
    infer_miaoshou_category,
    miaoshou_row_identity,
)
from miaoshou_auto_listing.handlers.shop import infer_category_group
from miaoshou_auto_listing.models import ErrorCode, ProductTask, Step
from miaoshou_auto_listing.state import (
    AcquisitionReceiptStore,
    CheckpointStore,
    ImageTranslationReceiptStore,
    JsonlStateSink,
    SUBMISSION_FAILED,
    SUBMISSION_PENDING,
    SUBMISSION_PUBLISHED,
    SubmissionReceiptStore,
)
from miaoshou_auto_listing.workflows.publish_product import PublishProductWorkflow


ROOT = Path(__file__).resolve().parents[1]


class FakePage:
    url = "https://example.invalid/editor"

    async def wait_for_timeout(self, timeout_ms: int) -> None:
        return None

    async def screenshot(self, path: str, full_page: bool) -> None:
        Path(path).write_bytes(b"fake-png")


class RecordingHandler(Handler):
    def __init__(self, step: Step, calls: list, fail_once: bool = False) -> None:
        self.step = step
        self.calls = calls
        self.fail_once = fail_once

    async def run(self, context) -> None:
        self.calls.append(self.step)
        if self.fail_once:
            self.fail_once = False
            raise ExecutorError(
                ErrorCode.NETWORK_ERROR, "temporary", step=self.step, retryable=True
            )


def task(current_step=None) -> ProductTask:
    return ProductTask(
        task_id="T1",
        miaoshou_product_id="M1",
        target_shop="TH_WOMEN_01",
        market="TH",
        category_group="ACCESSORY",
        pricing_rule_id="TH_ACCESSORY_V1",
        fixed_sale_price=100,
        stock_per_sku=500,
        current_step=current_step,
    )


def workflow(
    tmp_path: Path,
    handlers,
    *,
    checkpoint_store=None,
    submission_store=None,
    acquisition_store=None,
    image_translation_store=None,
    draft_saver=None,
    editor_matcher=None,
) -> PublishProductWorkflow:
    config = load_config(ROOT / "config")
    config.retry["delays_seconds"] = [0, 0, 0]
    return PublishProductWorkflow(
        config=config,
        selectors=SelectorRegistry(ROOT / "config" / "selectors"),
        evidence=EvidenceRecorder(tmp_path / "artifacts"),
        state_sink=JsonlStateSink(tmp_path / "state.jsonl"),
        handlers=handlers,
        checkpoint_store=checkpoint_store,
        submission_store=submission_store,
        acquisition_store=acquisition_store,
        image_translation_store=image_translation_store,
        draft_saver=draft_saver,
        editor_matcher=editor_matcher,
    )


class WorkflowTest(unittest.IsolatedAsyncioTestCase):
    async def test_hair_clip_synonyms_use_accessory_business_group(self) -> None:
        for title in (
            "高级感小号侧边刘海鸭嘴夹",
            "彩钻前额碎发发卡",
            "法式鲨鱼抓夹",
            "古风发簪",
        ):
            self.assertEqual(infer_category_group(title), "ACCESSORY")

    async def test_known_hair_accessories_use_exact_miaoshou_category(self) -> None:
        self.assertEqual(
            infer_miaoshou_category("法式野猪鬃透明鲨鱼抓夹"),
            "发夹发簪",
        )
        self.assertEqual(infer_miaoshou_category("纯银项链"), "")

    async def test_price_ui_failure_retries_in_the_same_step(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            calls = []
            handler = RecordingHandler(Step.SET_PRICE, calls)
            handler.fail_once = False

            class PriceFailureOnce(RecordingHandler):
                async def run(self, context) -> None:
                    self.calls.append(self.step)
                    if len(self.calls) == 1:
                        raise ExecutorError(
                            ErrorCode.PRICE_FILL_FAILED,
                            "currency options are still loading",
                            step=self.step,
                        )

            result = await workflow(
                Path(temp_dir), [PriceFailureOnce(Step.SET_PRICE, calls)]
            ).execute(FakePage(), task())
            self.assertTrue(result.success)
            self.assertEqual(calls, [Step.SET_PRICE, Step.SET_PRICE])
            self.assertEqual(result.retry_count, 1)

    async def test_image_translation_receipt_requires_a_saved_draft(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = ImageTranslationReceiptStore(Path(temp_dir) / "images")
            listing_task = task()
            store.update(
                listing_task,
                {
                    "detail": {
                        "status": "TRANSLATED",
                        "source_fingerprint": "source",
                        "result_fingerprint": "result",
                        "target_language": "th",
                    }
                },
            )
            self.assertFalse(store.load(listing_task).draft_saved)
            store.mark_draft_saved(listing_task)
            receipt = store.load(listing_task)
            self.assertTrue(receipt.draft_saved)
            self.assertEqual(
                receipt.image_translation["detail"]["result_fingerprint"],
                "result",
            )

            repriced = listing_task.model_copy(update={"fixed_sale_price": 120})
            self.assertIsNotNone(store.load(repriced))

            new_size_chart = listing_task.model_copy(
                update={"size_chart_file_token": "new-size-chart-token"}
            )
            self.assertIsNone(store.load(new_size_chart))

    async def test_current_miaoshou_dom_uses_collection_time_as_row_identity(self) -> None:
        class EmptyLocator:
            async def count(self):
                return 0

        class CurrentDomRow:
            async def get_attribute(self, name):
                return None

            def locator(self, selector):
                return EmptyLocator()

            async def inner_text(self):
                return "货源：(970530901429)\n2026-08-11 18:28:04\n编辑"

        self.assertEqual(
            await miaoshou_row_identity(CurrentDomRow()),
            "collected-at:2026-08-11 18:28:04",
        )

    async def test_locked_miaoshou_row_wins_over_duplicate_source_rows(self) -> None:
        class EmptyLocator:
            async def count(self):
                return 0

            def nth(self, index):
                raise AssertionError("empty locator")

        class FakeRow:
            def __init__(self, row_id, text):
                self.row_id = row_id
                self.text = text

            async def get_attribute(self, name):
                return self.row_id if name == "data-row-key" else None

            def locator(self, selector):
                return EmptyLocator()

            async def inner_text(self):
                return self.text

        first = FakeRow("row-1", "M1 LikeU shop")
        locked = FakeRow("row-2", "M1 LikeU shop")
        context = SimpleNamespace(
            task=task(),
            config=load_config(ROOT / "config"),
            acquisition_receipt=SimpleNamespace(
                miaoshou_row_id="attribute:data-row-key:row-2"
            ),
            linear=False,
            acquisition_store=None,
        )
        await LocateProductHandler()._select_and_bind(context, [first, locked])
        self.assertIs(context.product_row, locked)

    async def test_missing_locked_miaoshou_row_never_falls_back(self) -> None:
        class EmptyLocator:
            async def count(self):
                return 0

        class FakeRow:
            async def get_attribute(self, name):
                return "row-new" if name == "data-row-key" else None

            def locator(self, selector):
                return EmptyLocator()

            async def inner_text(self):
                return "M1 LikeU shop"

        context = SimpleNamespace(
            task=task(),
            config=load_config(ROOT / "config"),
            acquisition_receipt=SimpleNamespace(
                miaoshou_row_id="attribute:data-row-key:row-original"
            ),
            linear=False,
            acquisition_store=None,
        )
        with self.assertRaises(ExecutorError) as raised:
            await LocateProductHandler()._select_and_bind(context, [FakeRow()])
        self.assertEqual(raised.exception.code, ErrorCode.PRODUCT_NOT_FOUND)

    async def test_linear_retry_skips_completed_acquisition(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            acquisitions = AcquisitionReceiptStore(root / "acquisitions")
            calls = []

            class AcquisitionStep(RecordingHandler):
                async def run(self, context) -> None:
                    if not getattr(context, "acquisition_completed", False):
                        self.calls.append(self.step)

            handlers = [
                AcquisitionStep(Step.COLLECT, calls),
                AcquisitionStep(Step.CLAIM, calls),
            ]
            listing_task = task().model_copy(
                update={"source_url": "https://detail.1688.com/offer/M1.html"}
            )
            first = await workflow(
                root,
                handlers,
                acquisition_store=acquisitions,
            ).execute(FakePage(), listing_task, linear=True)
            self.assertTrue(first.success)
            self.assertEqual(calls, [Step.COLLECT, Step.CLAIM])
            self.assertIsNotNone(acquisitions.load(listing_task))

            calls.clear()
            second = await workflow(
                root,
                [
                    AcquisitionStep(Step.COLLECT, calls),
                    AcquisitionStep(Step.CLAIM, calls),
                ],
                acquisition_store=acquisitions,
            ).execute(FakePage(), listing_task, linear=True)
            self.assertTrue(second.success)
            self.assertEqual(calls, [])

    async def test_acquisition_receipt_can_only_be_enriched_once(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = AcquisitionReceiptStore(Path(temp_dir) / "acquisitions")
            listing_task = task().model_copy(
                update={"source_url": "https://detail.1688.com/offer/M1.html"}
            )
            initial = store.record_acquired(listing_task)
            self.assertEqual(initial.miaoshou_row_id, "")
            enriched = store.record_acquired(
                listing_task,
                miaoshou_row_id="attribute:data-row-key:row-1",
            )
            self.assertEqual(
                enriched.miaoshou_row_id, "attribute:data-row-key:row-1"
            )
            with self.assertRaises(ValueError):
                store.record_acquired(
                    listing_task,
                    miaoshou_row_id="attribute:data-row-key:row-2",
                )

    async def test_acquisition_lock_ignores_share_token_price_and_stock_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = AcquisitionReceiptStore(Path(temp_dir) / "acquisitions")
            original = task().model_copy(
                update={
                    "source_url": "https://detail.1688.com/offer/M1.html?share_token=old",
                    "fixed_sale_price": 45,
                    "stock_per_sku": 100,
                }
            )
            store.record_acquired(original)
            changed = original.model_copy(
                update={
                    "source_url": "https://detail.1688.com/offer/M1.html?share_token=new",
                    "fixed_sale_price": 55,
                    "stock_per_sku": 500,
                }
            )
            self.assertIsNotNone(store.load(changed))

    async def test_acquisition_lock_rejects_a_different_target_shop(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = AcquisitionReceiptStore(Path(temp_dir) / "acquisitions")
            original = task().model_copy(
                update={"source_url": "https://detail.1688.com/offer/M1.html"}
            )
            store.record_acquired(original)
            changed = original.model_copy(update={"target_shop": "LIKEU_SHOP"})
            self.assertIsNone(store.load(changed))

    async def test_linear_execution_ignores_draft_resume_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = CheckpointStore(root / "checkpoints")
            store.save(
                task(),
                resume_step=Step.SET_STOCK,
                completed_steps=[Step.SET_PRICE],
                draft_saved=True,
            )
            calls = []
            result = await workflow(
                root,
                [
                    RecordingHandler(Step.LOCATE_PRODUCT, calls),
                    RecordingHandler(Step.OPEN_EDITOR, calls),
                    RecordingHandler(Step.SET_PRICE, calls),
                    RecordingHandler(Step.SET_STOCK, calls),
                ],
                checkpoint_store=store,
            ).execute(FakePage(), task(), linear=True)
            self.assertTrue(result.success)
            self.assertEqual(
                calls,
                [
                    Step.LOCATE_PRODUCT,
                    Step.OPEN_EDITOR,
                    Step.SET_PRICE,
                    Step.SET_STOCK,
                ],
            )

    async def test_submission_lock_is_shared_across_feishu_record_ids(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            submissions = SubmissionReceiptStore(Path(temp_dir) / "submissions")
            first = task()
            second = first.model_copy(update={"task_id": "T2"})
            submissions.record_submitted(first, submission_signal="test")
            self.assertEqual(
                submissions.path_for(first), submissions.path_for(second)
            )
            self.assertEqual(submissions.load(second).status, SUBMISSION_PENDING)

    async def test_submission_receipt_forces_verify_and_never_republishes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            submissions = SubmissionReceiptStore(root / "submissions")
            submissions.record_submitted(task(), submission_signal="test")
            calls = []

            class Verified(RecordingHandler):
                async def run(self, context) -> None:
                    self.calls.append(self.step)
                    context.platform_product_id = "1736000000000000001"
                    context.published_status = "发布成功"

            result = await workflow(
                root,
                [
                    RecordingHandler(Step.PUBLISH, calls),
                    Verified(Step.VERIFY, calls),
                ],
                submission_store=submissions,
            ).execute(
                FakePage(), task(), allow_publish=True, require_approval=True
            )
            self.assertTrue(result.success)
            self.assertEqual(calls, [Step.VERIFY])
            receipt = submissions.load(task())
            self.assertEqual(receipt.status, SUBMISSION_PUBLISHED)
            self.assertEqual(
                receipt.platform_product_id, "1736000000000000001"
            )

    async def test_pending_verification_survives_restart_without_publish(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            submissions = SubmissionReceiptStore(root / "submissions")
            submissions.record_submitted(task(), submission_signal="test")
            calls = []

            class StillPending(RecordingHandler):
                async def run(self, context) -> None:
                    self.calls.append(self.step)
                    raise ExecutorError(
                        ErrorCode.PUBLISH_VALIDATION_FAILED,
                        "not visible yet",
                        step=self.step,
                    )

            async def no_editor(page, product_id, shop_name):
                return False

            result = await workflow(
                root,
                [
                    RecordingHandler(Step.PUBLISH, calls),
                    StillPending(Step.VERIFY, calls),
                ],
                submission_store=submissions,
                editor_matcher=no_editor,
            ).execute(FakePage(), task(), allow_publish=True)
            self.assertFalse(result.success)
            self.assertEqual(result.published_status, "SUBMITTED_PENDING_VERIFICATION")
            self.assertEqual(calls, [Step.VERIFY])
            receipt = submissions.load(task())
            self.assertEqual(receipt.status, SUBMISSION_PENDING)
            self.assertEqual(receipt.verification_attempts, 1)

    async def test_failure_after_irreversible_click_stays_pending(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            submissions = SubmissionReceiptStore(root / "submissions")

            class ClickedThenTimedOut(RecordingHandler):
                async def run(self, context) -> None:
                    context.publish_submitted = True
                    context.submission_store.record_submitted(
                        context.task, submission_signal="确认发布已点击"
                    )
                    raise ExecutorError(
                        ErrorCode.PUBLISH_FAILED,
                        "success dialog timed out",
                        step=self.step,
                    )

            async def no_editor(page, product_id, shop_name):
                return False

            result = await workflow(
                root,
                [ClickedThenTimedOut(Step.PUBLISH, [])],
                submission_store=submissions,
                editor_matcher=no_editor,
            ).execute(FakePage(), task(), allow_publish=True)
            self.assertFalse(result.success)
            self.assertEqual(
                result.published_status, "SUBMITTED_PENDING_VERIFICATION"
            )
            self.assertEqual(submissions.load(task()).status, SUBMISSION_PENDING)

    async def test_verified_failure_is_not_reported_as_pending(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            submissions = SubmissionReceiptStore(root / "submissions")
            submissions.record_submitted(task(), submission_signal="test")

            class Failed(RecordingHandler):
                async def run(self, context) -> None:
                    raise ExecutorError(
                        ErrorCode.PUBLISH_FAILED,
                        "TikTok rejected the listing",
                        step=self.step,
                    )

            async def no_editor(page, product_id, shop_name):
                return False

            result = await workflow(
                root,
                [Failed(Step.VERIFY, [])],
                submission_store=submissions,
                editor_matcher=no_editor,
            ).execute(FakePage(), task(), allow_publish=True)
            self.assertFalse(result.success)
            self.assertEqual(result.published_status, "")
            self.assertEqual(submissions.load(task()).status, SUBMISSION_FAILED)

    async def test_prepare_then_approved_publish_uses_matching_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = CheckpointStore(root / "checkpoints")
            calls = []

            class SnapshotHandler(Handler):
                step = Step.PREFLIGHT

                async def run(self, context) -> None:
                    calls.append(self.step)
                    context.preflight_snapshot = {
                        "shop": "LikeU shop",
                        "market": "TH",
                        "title": "สินค้าไทย",
                        "sku_count": 1,
                        "skus": [
                            {
                                "label": "S",
                                "cny_price": "100",
                                "stock": "500",
                                "weight_kg": "0.2",
                            }
                        ],
                        "package": {
                            "weight_kg": "0.2",
                            "length_cm": "10",
                            "width_cm": "4",
                            "height_cm": "5",
                        },
                        "size_chart_count": 1,
                        "image_translation": {},
                    }

            async def saved(page, timeout_ms):
                return True

            handlers = [
                SnapshotHandler(),
                RecordingHandler(Step.PUBLISH, calls),
                RecordingHandler(Step.VERIFY, calls),
            ]
            prepared = await workflow(
                root,
                handlers,
                checkpoint_store=store,
                draft_saver=saved,
            ).execute(FakePage(), task(), prepare_for_approval=True)
            self.assertTrue(prepared.success)
            self.assertEqual(
                prepared.published_status, "WAITING_APPROVAL_NOT_PUBLISHED"
            )
            self.assertTrue(prepared.approval_fingerprint)
            self.assertTrue(store.load(task()).awaiting_approval)

            calls.clear()
            published = await workflow(
                root,
                handlers,
                checkpoint_store=store,
                draft_saver=saved,
            ).execute(
                FakePage(),
                task(),
                allow_publish=True,
                require_approval=True,
            )
            self.assertTrue(published.success)
            self.assertEqual(
                calls, [Step.PREFLIGHT, Step.PUBLISH, Step.VERIFY]
            )
            self.assertIsNone(store.load(task()))

    async def test_approved_publish_rejects_changed_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = CheckpointStore(root / "checkpoints")

            class SnapshotHandler(Handler):
                step = Step.PREFLIGHT

                def __init__(self, title):
                    self.title = title

                async def run(self, context) -> None:
                    context.preflight_snapshot = {
                        "shop": "LikeU shop",
                        "market": "TH",
                        "title": self.title,
                        "skus": [],
                        "package": {},
                        "size_chart_count": 0,
                        "image_translation": {},
                    }

            async def saved(page, timeout_ms):
                return True

            await workflow(
                root,
                [SnapshotHandler("approved")],
                checkpoint_store=store,
                draft_saver=saved,
            ).execute(FakePage(), task(), prepare_for_approval=True)
            result = await workflow(
                root,
                [
                    SnapshotHandler("changed"),
                    RecordingHandler(Step.PUBLISH, []),
                ],
                checkpoint_store=store,
                draft_saver=saved,
            ).execute(
                FakePage(),
                task(),
                allow_publish=True,
                require_approval=True,
            )
            self.assertFalse(result.success)
            self.assertEqual(
                result.error_code, ErrorCode.PUBLISH_VALIDATION_FAILED
            )

    @staticmethod
    def resumable_handlers(calls, failing=False):
        class StockStep(RecordingHandler):
            async def run(self, context) -> None:
                self.calls.append(self.step)
                if failing:
                    raise ExecutorError(
                        ErrorCode.STOCK_FILL_FAILED,
                        "stock dialog changed",
                        step=self.step,
                    )

        return [
            RecordingHandler(Step.LOCATE_PRODUCT, calls),
            RecordingHandler(Step.SET_SHOP, calls),
            RecordingHandler(Step.OPEN_EDITOR, calls),
            RecordingHandler(Step.SET_PRICE, calls),
            StockStep(Step.SET_STOCK, calls),
        ]

    async def test_saved_failure_checkpoint_resumes_at_failed_step(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = CheckpointStore(root / "checkpoints")

            async def saved(page, timeout_ms):
                return True

            first = await workflow(
                root,
                self.resumable_handlers([], failing=True),
                checkpoint_store=store,
                draft_saver=saved,
            ).execute(FakePage(), task())
            self.assertFalse(first.success)
            self.assertEqual(first.current_step, Step.SET_STOCK)

            calls = []
            second = await workflow(
                root,
                self.resumable_handlers(calls),
                checkpoint_store=store,
                draft_saver=saved,
            ).execute(FakePage(), task())
            self.assertTrue(second.success)
            self.assertEqual(
                calls,
                [
                    Step.LOCATE_PRODUCT,
                    Step.SET_SHOP,
                    Step.OPEN_EDITOR,
                    Step.SET_STOCK,
                ],
            )
            self.assertIsNone(store.load(task()))

    async def test_unsaved_failure_checkpoint_is_not_resumed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = CheckpointStore(root / "checkpoints")

            async def not_saved(page, timeout_ms):
                return False

            await workflow(
                root,
                self.resumable_handlers([], failing=True),
                checkpoint_store=store,
                draft_saver=not_saved,
            ).execute(FakePage(), task())
            calls = []
            result = await workflow(
                root,
                self.resumable_handlers(calls),
                checkpoint_store=store,
                draft_saver=not_saved,
            ).execute(FakePage(), task())
            self.assertTrue(result.success)
            self.assertEqual(
                calls,
                [
                    Step.LOCATE_PRODUCT,
                    Step.SET_SHOP,
                    Step.OPEN_EDITOR,
                    Step.SET_PRICE,
                    Step.SET_STOCK,
                ],
            )

    async def test_matching_live_editor_resumes_without_saved_draft(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = CheckpointStore(root / "checkpoints")

            async def not_saved(page, timeout_ms):
                return False

            async def matching(page, product_id, shop_name):
                return True

            await workflow(
                root,
                self.resumable_handlers([], failing=True),
                checkpoint_store=store,
                draft_saver=not_saved,
                editor_matcher=matching,
            ).execute(FakePage(), task())
            calls = []
            result = await workflow(
                root,
                self.resumable_handlers(calls),
                checkpoint_store=store,
                draft_saver=not_saved,
                editor_matcher=matching,
            ).execute(FakePage(), task())
            self.assertTrue(result.success)
            self.assertEqual(calls, [Step.SET_STOCK])

    async def test_changed_task_inputs_invalidate_saved_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = CheckpointStore(root / "checkpoints")

            async def saved(page, timeout_ms):
                return True

            await workflow(
                root,
                self.resumable_handlers([], failing=True),
                checkpoint_store=store,
                draft_saver=saved,
            ).execute(FakePage(), task())
            changed = task()
            changed.stock_per_sku = 777
            calls = []
            result = await workflow(
                root,
                self.resumable_handlers(calls),
                checkpoint_store=store,
                draft_saver=saved,
            ).execute(FakePage(), changed)
            self.assertTrue(result.success)
            self.assertIn(Step.SET_PRICE, calls)

    async def test_preflight_snapshot_is_returned(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            class SnapshotHandler(Handler):
                step = Step.PREFLIGHT

                async def run(self, context) -> None:
                    context.preflight_snapshot = {
                        "shop": "LikeU shop",
                        "sku_count": 4,
                    }

            result = await workflow(
                Path(temp_dir), [SnapshotHandler()]
            ).execute(FakePage(), task(), allow_publish=False)
            self.assertTrue(result.success)
            self.assertEqual(result.preflight["sku_count"], 4)

    async def test_dry_run_skips_publish_and_verify(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            calls = []
            handlers = [
                RecordingHandler(Step.LOCATE_PRODUCT, calls),
                RecordingHandler(Step.OPEN_EDITOR, calls),
                RecordingHandler(Step.TRANSLATE, calls),
                RecordingHandler(Step.PUBLISH, calls),
                RecordingHandler(Step.VERIFY, calls),
            ]
            result = await workflow(Path(temp_dir), handlers).execute(
                FakePage(), task(), allow_publish=False
            )
            self.assertTrue(result.success)
            self.assertEqual(
                calls, [Step.LOCATE_PRODUCT, Step.OPEN_EDITOR, Step.TRANSLATE]
            )
            self.assertEqual(
                result.published_status, "DRY_RUN_COMPLETE_NOT_PUBLISHED"
            )

    async def test_transient_error_is_retried(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            calls = []
            handlers = [RecordingHandler(Step.LOCATE_PRODUCT, calls, fail_once=True)]
            result = await workflow(Path(temp_dir), handlers).execute(FakePage(), task())
            self.assertTrue(result.success)
            self.assertEqual(calls, [Step.LOCATE_PRODUCT, Step.LOCATE_PRODUCT])
            self.assertEqual(result.retry_count, 1)

    async def test_business_error_is_not_retried_and_has_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            calls = []

            class FailingHandler(RecordingHandler):
                async def run(self, context) -> None:
                    self.calls.append(self.step)
                    raise ExecutorError(
                        ErrorCode.SIZE_CHART_REQUIRED,
                        "missing trustworthy size chart",
                        step=self.step,
                    )

            handlers = [FailingHandler(Step.LOCATE_PRODUCT, calls)]
            result = await workflow(Path(temp_dir), handlers).execute(
                FakePage(), task()
            )
            self.assertFalse(result.success)
            self.assertEqual(result.error_code, ErrorCode.SIZE_CHART_REQUIRED)
            self.assertEqual(result.retry_count, 0)
            self.assertTrue(Path(result.screenshot).exists())

    async def test_resume_reopens_product_then_starts_at_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            calls = []
            handlers = [
                RecordingHandler(Step.LOCATE_PRODUCT, calls),
                RecordingHandler(Step.OPEN_EDITOR, calls),
                RecordingHandler(Step.SET_SHOP, calls),
                RecordingHandler(Step.SET_WAREHOUSE, calls),
                RecordingHandler(Step.PUBLISH, calls),
            ]
            result = await workflow(Path(temp_dir), handlers).execute(
                FakePage(), task(Step.SET_WAREHOUSE), allow_publish=False
            )
            self.assertTrue(result.success)
            self.assertEqual(
                calls,
                [
                    Step.LOCATE_PRODUCT,
                    Step.SET_SHOP,
                    Step.OPEN_EDITOR,
                    Step.SET_WAREHOUSE,
                ],
            )

    async def test_resume_verify_does_not_reopen_removed_collect_item(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            calls = []
            handlers = [
                RecordingHandler(Step.LOCATE_PRODUCT, calls),
                RecordingHandler(Step.SET_SHOP, calls),
                RecordingHandler(Step.OPEN_EDITOR, calls),
                RecordingHandler(Step.VERIFY, calls),
            ]
            result = await workflow(Path(temp_dir), handlers).execute(
                FakePage(), task(Step.VERIFY), allow_publish=True
            )
            self.assertTrue(result.success)
            self.assertEqual(calls, [Step.VERIFY])

    async def test_exhausted_retries_are_reported(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            calls = []

            class AlwaysTransient(RecordingHandler):
                async def run(self, context) -> None:
                    self.calls.append(self.step)
                    raise ExecutorError(
                        ErrorCode.NETWORK_ERROR,
                        "still temporary",
                        step=self.step,
                        retryable=True,
                    )

            result = await workflow(
                Path(temp_dir), [AlwaysTransient(Step.LOCATE_PRODUCT, calls)]
            ).execute(FakePage(), task())
            self.assertFalse(result.success)
            self.assertEqual(len(calls), 3)
            self.assertEqual(result.retry_count, 2)
