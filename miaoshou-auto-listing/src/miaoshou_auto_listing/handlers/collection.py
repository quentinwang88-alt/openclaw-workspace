from __future__ import annotations

from time import monotonic
from urllib.parse import urljoin

from ..errors import ExecutorError
from ..models import ErrorCode, Step
from .base import Handler, HandlerContext


def _url(context: HandlerContext, path: str) -> str:
    base = context.config.browser.base_url.rstrip("/") + "/"
    return urljoin(base, path.lstrip("/"))


async def _matching_rows(page, source_id: str):
    rows = page.locator(".pro-virtual-table__row")
    return rows.filter(has_text=source_id)


async def _existing_rows_after_load(page, source_id: str, timeout_ms: int = 5_000):
    """Avoid duplicate collection while Miaoshou's virtual table is still hydrating."""
    await _apply_source_filter(page, source_id)
    deadline = monotonic() + timeout_ms / 1000
    rows = await _matching_rows(page, source_id)
    while monotonic() < deadline:
        if await rows.count():
            return rows
        empty = page.get_by_text("暂无数据", exact=False)
        if await empty.count() and await empty.first.is_visible():
            return rows
        await page.wait_for_timeout(300)
        rows = await _matching_rows(page, source_id)
    return rows


async def _apply_source_filter(page, source_id: str) -> None:
    form_item = page.locator(".jx-form-item").filter(has_text="货源ID")
    field = form_item.locator("input:visible, textarea:visible")
    if await field.count() == 0:
        return
    await field.first.fill(source_id)
    search = page.get_by_role("button", name="搜索", exact=True)
    if await search.count():
        await search.first.click()
        await page.wait_for_timeout(800)


async def _wait_for_row(page, source_id: str, timeout_ms: int):
    deadline = monotonic() + timeout_ms / 1000
    while monotonic() < deadline:
        await _apply_source_filter(page, source_id)
        rows = await _matching_rows(page, source_id)
        if await rows.count():
            return rows.first
        await page.wait_for_timeout(1200)
    raise TimeoutError(f"Timed out waiting for source item {source_id}")


async def wait_for_history_rows(page, timeout_ms: int):
    rows = page.locator("tbody tr.jx-table__row")
    await rows.first.wait_for(timeout=timeout_ms)
    return rows


class DuplicateCheckHandler(Handler):
    step = Step.CHECK_DUPLICATE

    async def run(self, context: HandlerContext) -> None:
        if context.task.allow_republish:
            setattr(context, "duplicate_checked", True)
            return
        shop_name = str(context.config.shops[context.task.target_shop]["display_name"])
        try:
            await context.page.goto(
                _url(context, context.config.browser.publish_history_path),
                wait_until="domcontentloaded",
            )
            if "/?redirect=" in context.page.url:
                raise ExecutorError(
                    ErrorCode.LOGIN_EXPIRED,
                    "Miaoshou redirected the publish-history check to login",
                    step=self.step,
                )
            rows = (await wait_for_history_rows(
                context.page, context.config.browser.navigation_timeout_ms
            )).filter(
                has_text=context.task.miaoshou_product_id
            ).filter(has_text=shop_name)
            for index in range(await rows.count()):
                text = await rows.nth(index).inner_text()
                if "发布成功" in text:
                    raise ExecutorError(
                        ErrorCode.ALREADY_PUBLISHED,
                        (
                            f"Source {context.task.miaoshou_product_id} is already "
                            f"published to {shop_name}; set allow_republish=true only "
                            "for an intentional duplicate"
                        ),
                        step=self.step,
                    )
            setattr(context, "duplicate_checked", True)
        except ExecutorError:
            raise
        except Exception as exc:
            raise ExecutorError(
                ErrorCode.NETWORK_ERROR,
                f"Could not check publish history safely: {exc}",
                step=self.step,
                retryable=True,
            ) from exc


class CollectHandler(Handler):
    step = Step.COLLECT

    async def run(self, context: HandlerContext) -> None:
        if getattr(context, "acquisition_completed", False):
            setattr(context, "already_claimed", True)
            return
        if not context.task.source_url:
            return
        settings = context.config.browser
        source_id = context.task.miaoshou_product_id
        try:
            # Idempotency before collection: an item in either downstream box is enough.
            await context.page.goto(
                _url(context, settings.product_list_path), wait_until="domcontentloaded"
            )
            await context.page.wait_for_timeout(500)
            if await (await _existing_rows_after_load(
                context.page, source_id
            )).count():
                setattr(context, "already_claimed", True)
                return
            await context.page.goto(
                _url(context, settings.public_collect_box_path),
                wait_until="domcontentloaded",
            )
            await context.page.wait_for_timeout(500)
            if await (await _existing_rows_after_load(
                context.page, source_id
            )).count():
                return

            await context.page.goto(
                _url(context, settings.collect_input_path),
                wait_until="domcontentloaded",
            )
            link_fields = context.page.locator(
                "textarea:visible, input[placeholder*='链接']:visible"
            )
            await link_fields.first.wait_for(
                state="visible", timeout=settings.navigation_timeout_ms
            )
            auto_claim = context.page.get_by_text("自动认领(发布)到平台", exact=True)
            if await auto_claim.count():
                label = auto_claim.first.locator("xpath=ancestor::label[1]")
                checkbox = label.locator("input[type='checkbox']")
                if await checkbox.count() and await checkbox.first.is_checked():
                    await auto_claim.first.click()

            await link_fields.first.fill(context.task.source_url)
            await context.page.get_by_role(
                "button", name="开始采集", exact=True
            ).click()
            success = context.page.get_by_text("已提交采集任务", exact=False)
            await success.first.wait_for(timeout=settings.navigation_timeout_ms)

            await context.page.goto(
                _url(context, settings.public_collect_box_path),
                wait_until="domcontentloaded",
            )
            await _wait_for_row(
                context.page,
                source_id,
                max(settings.navigation_timeout_ms, 180_000),
            )
        except Exception as exc:
            if isinstance(exc, ExecutorError):
                raise
            raise ExecutorError(
                ErrorCode.COLLECT_FAILED,
                f"1688 collection failed for {source_id}: {exc}",
                step=self.step,
            ) from exc


class ClaimHandler(Handler):
    step = Step.CLAIM

    async def run(self, context: HandlerContext) -> None:
        if getattr(context, "acquisition_completed", False):
            return
        if not context.task.source_url or getattr(context, "already_claimed", False):
            return
        settings = context.config.browser
        source_id = context.task.miaoshou_product_id
        try:
            await context.page.goto(
                _url(context, settings.product_list_path), wait_until="domcontentloaded"
            )
            await context.page.wait_for_timeout(500)
            if await (await _existing_rows_after_load(
                context.page, source_id
            )).count():
                return

            await context.page.goto(
                _url(context, settings.public_collect_box_path),
                wait_until="domcontentloaded",
            )
            row = await _wait_for_row(
                context.page, source_id, settings.navigation_timeout_ms
            )
            await row.get_by_text("认领到", exact=True).click()
            menu = context.page.locator(".cliam-platform-menu:visible")
            await menu.wait_for(timeout=settings.timeout_ms)
            await menu.get_by_text("TikTok", exact=True).click()
            await context.page.get_by_text("认领成功", exact=False).first.wait_for(
                timeout=settings.navigation_timeout_ms
            )

            await context.page.goto(
                _url(context, settings.product_list_path), wait_until="domcontentloaded"
            )
            await _wait_for_row(context.page, source_id, settings.navigation_timeout_ms)
        except Exception as exc:
            if isinstance(exc, ExecutorError):
                raise
            raise ExecutorError(
                ErrorCode.CLAIM_FAILED,
                f"TikTok claim failed for {source_id}: {exc}",
                step=self.step,
            ) from exc
