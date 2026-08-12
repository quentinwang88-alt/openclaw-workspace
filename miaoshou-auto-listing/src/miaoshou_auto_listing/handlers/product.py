from __future__ import annotations

import re
from time import monotonic
from urllib.parse import urljoin

from ..browser.helpers import wait_for_key, wait_for_loading_to_finish
from ..errors import ExecutorError, SelectorNotFound
from ..models import ErrorCode, Step
from .base import Handler, HandlerContext
from .collection import _apply_source_filter, _matching_rows, _wait_for_row
from .editor_dom import editor_root, real_sku_rows, title_value


_ROW_ID_ATTRIBUTES = ("data-row-key", "row-key", "data-key", "data-id")

CATEGORY_FALLBACKS = (
    (
        ("发夹", "抓夹", "发簪", "发卡", "鲨鱼夹", "鸭嘴夹"),
        "发夹发簪",
    ),
)


def infer_miaoshou_category(text: str) -> str:
    compact = "".join(str(text or "").split())
    for tokens, category in CATEGORY_FALLBACKS:
        if any(token in compact for token in tokens):
            return category
    return ""


async def miaoshou_row_identity(row) -> str:
    """Return a stable identity only when Miaoshou exposes one on the row.

    Virtual-list indexes and generated DOM ids are intentionally excluded:
    either can change after a refresh and would turn a safety lock into noise.
    """
    candidates = [row]
    descendants = row.locator(
        "[data-row-key], [row-key], [data-key], [data-id]"
    )
    for index in range(min(await descendants.count(), 20)):
        candidates.append(descendants.nth(index))
    for candidate in candidates:
        for attribute in _ROW_ID_ATTRIBUTES:
            value = (await candidate.get_attribute(attribute) or "").strip()
            if value:
                return f"attribute:{attribute}:{value}"

    # Some table implementations keep the record key on the row checkbox.
    checkboxes = row.locator("input[type='checkbox'][value]")
    for index in range(min(await checkboxes.count(), 5)):
        value = (await checkboxes.nth(index).get_attribute("value") or "").strip()
        if value and value.casefold() not in {"on", "true", "false"}:
            return f"checkbox:value:{value}"

    # The current Miaoshou virtual table (verified 2026-08-11) does not expose
    # its database id in the DOM, but it does expose the immutable collection
    # timestamp. Combined with the receipt's source id and target shop, this
    # distinguishes duplicate collections of the same 1688 offer.
    text = await row.inner_text()
    timestamp = re.search(r"\b20\d{2}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\b", text)
    if timestamp:
        return f"collected-at:{timestamp.group(0)}"
    return ""


class LocateProductHandler(Handler):
    step = Step.LOCATE_PRODUCT

    async def run(self, context: HandlerContext) -> None:
        page, task, settings = context.page, context.task, context.config.browser
        url = urljoin(settings.base_url.rstrip("/") + "/", settings.product_list_path.lstrip("/"))
        await page.goto(url, wait_until="domcontentloaded")
        if await context.selectors.exists(page, "login_form"):
            raise ExecutorError(
                ErrorCode.LOGIN_EXPIRED,
                "Miaoshou login page detected; refresh the dedicated browser profile manually",
                step=self.step,
            )
        await _apply_source_filter(page, task.miaoshou_product_id)
        real_rows = await _matching_rows(page, task.miaoshou_product_id)
        if await real_rows.count() == 0:
            await _wait_for_row(
                page, task.miaoshou_product_id, settings.navigation_timeout_ms
            )
            real_rows = await _matching_rows(page, task.miaoshou_product_id)
        if await real_rows.count():
            await self._select_and_bind(
                context,
                [real_rows.nth(index) for index in range(await real_rows.count())],
            )
            return
        rows = await context.selectors.resolve_all(page, "product_result_rows")
        if not await self._contains_id(rows, task.miaoshou_product_id):
            search = await self._source_id_search(context)
            await search.fill(task.miaoshou_product_id)
            button = await context.selectors.resolve(page, "product_search_button")
            await button.click()
            await wait_for_loading_to_finish(
                page, context.selectors, timeout_ms=settings.navigation_timeout_ms
            )
            rows = await context.selectors.resolve_all(page, "product_result_rows")
        matches = []
        for index in range(await rows.count()):
            row = rows.nth(index)
            try:
                id_locator = await context.selectors.resolve(
                    row, "product_row_id", visible=False
                )
                text = (await id_locator.text_content() or "").strip()
            except SelectorNotFound:
                text = (await row.text_content() or "").strip()
            escaped = re.escape(task.miaoshou_product_id)
            if text == task.miaoshou_product_id or re.search(
                rf"(?<![A-Za-z0-9]){escaped}(?![A-Za-z0-9])", text
            ):
                matches.append(row)
        if not matches:
            raise ExecutorError(
                ErrorCode.PRODUCT_NOT_FOUND,
                f"Miaoshou product ID not found: {task.miaoshou_product_id}",
                step=self.step,
            )
        await self._select_and_bind(context, matches)

    async def _select_and_bind(self, context: HandlerContext, candidates) -> None:
        task = context.task
        receipt = getattr(context, "acquisition_receipt", None)
        locked_row_id = str(getattr(receipt, "miaoshou_row_id", "") or "")
        if locked_row_id:
            locked = [
                row
                for row in candidates
                if await miaoshou_row_identity(row) == locked_row_id
            ]
            if len(locked) != 1:
                raise ExecutorError(
                    ErrorCode.PRODUCT_NOT_FOUND,
                    (
                        "The previously locked Miaoshou product row is no longer "
                        f"available: {locked_row_id}. Refusing to select another row."
                    ),
                    step=self.step,
                )
            await self._bind(context, locked[0], locked_row_id)
            return

        shop_name = str(context.config.shops[task.target_shop]["display_name"])
        shop_matches = []
        for row in candidates:
            if shop_name in (await row.inner_text()):
                shop_matches.append(row)
        if len(shop_matches) == 1:
            await self._bind(context, shop_matches[0])
            return
        if len(candidates) == 1:
            await self._bind(context, candidates[0])
            return
        raise ExecutorError(
            ErrorCode.PRODUCT_MATCH_AMBIGUOUS,
            (
                f"Multiple rows matched product ID {task.miaoshou_product_id} "
                f"and target shop {shop_name} was not unique"
            ),
            step=self.step,
        )

    async def _bind(
        self, context: HandlerContext, row, row_id: str = ""
    ) -> None:
        setattr(context, "product_row", row)
        row_id = row_id or await miaoshou_row_identity(row)
        if (
            row_id
            and context.linear
            and context.acquisition_store is not None
            and getattr(context, "acquisition_completed", False)
        ):
            receipt = context.acquisition_store.record_acquired(
                context.task, miaoshou_row_id=row_id
            )
            setattr(context, "acquisition_receipt", receipt)

    async def _contains_id(self, rows, product_id: str) -> bool:
        for index in range(await rows.count()):
            if product_id in (await rows.nth(index).inner_text()):
                return True
        return False

    async def _source_id_search(self, context: HandlerContext):
        page = context.page
        labels = page.get_by_text("货源ID", exact=True)
        if await labels.count():
            item = labels.first.locator(
                "xpath=ancestor::*[contains(@class,'jx-form-item')][1]"
            )
            fields = item.locator("input[type='text']:not([role='combobox'])")
            if await fields.count():
                return fields.first
        return await wait_for_key(
            page,
            context.selectors,
            "product_search_input",
            timeout_ms=context.config.browser.timeout_ms,
        )


class OpenEditorHandler(Handler):
    step = Step.OPEN_EDITOR

    async def run(self, context: HandlerContext) -> None:
        if await context.selectors.exists(context.page, "editor_ready"):
            if await real_sku_rows(context.page) is not None:
                return
        else:
            row = getattr(context, "product_row", None)
            if row is None:
                raise ExecutorError(
                    ErrorCode.PRODUCT_NOT_FOUND,
                    "Product row is unavailable before opening editor",
                    step=self.step,
                )
            try:
                button = await context.selectors.resolve(
                    row, "product_row_edit_button"
                )
            except SelectorNotFound as exc:
                exc.step = self.step
                raise
            try:
                await button.click()
            except Exception:
                # Miaoshou occasionally leaves a new-user guide above the row.
                # A forced DOM click is deterministic and still targets the exact row button.
                await button.click(force=True)
            await wait_for_key(
                context.page,
                context.selectors,
                "editor_ready",
                timeout_ms=context.config.browser.navigation_timeout_ms,
            )
        editor = await editor_root(context.page)
        if editor is None or await real_sku_rows(context.page) is not None:
            return
        if not getattr(context, "category_fallback_attempted", False):
            setattr(context, "category_fallback_attempted", True)
            if await self._apply_known_category(context, editor):
                return
        resync = editor.get_by_role("button", name="重新同步类目", exact=True)
        if await resync.count() != 1:
            return
        await resync.click()
        first_wait_ms = max(context.config.browser.navigation_timeout_ms, 30_000)
        deadline = monotonic() + first_wait_ms / 1000
        while monotonic() < deadline:
            if await real_sku_rows(context.page) is not None:
                return
            await context.page.wait_for_timeout(500)
        if not getattr(context, "category_refresh_attempted", False):
            setattr(context, "category_refresh_attempted", True)
            await context.page.reload(wait_until="domcontentloaded")
            await context.page.wait_for_timeout(800)
            await LocateProductHandler().run(context)
            await self.run(context)
            return
        raise ExecutorError(
            ErrorCode.PAGE_STRUCTURE_CHANGED,
            "Miaoshou category/SKU data was still unavailable after one automatic refresh",
            step=self.step,
        )

    async def _apply_known_category(self, context: HandlerContext, editor) -> bool:
        category = infer_miaoshou_category(
            f"{await title_value(editor)}\n{await editor.inner_text()}"
        )
        if not category:
            return False
        resync = editor.get_by_role("button", name="重新同步类目", exact=True)
        if await resync.count() != 1:
            return False
        category_item = resync.locator(
            "xpath=ancestor::*[contains(concat(' ', normalize-space(@class), ' '), "
            "' jx-form-item ')][1]"
        )
        fields = category_item.locator(
            "input[placeholder*='请选择']:visible, input[placeholder*='输入搜索']:visible"
        )
        if await fields.count() != 1:
            return False
        await fields.first.click()
        poppers = context.page.locator(".jx-popper:visible, [role='listbox']:visible")
        await poppers.last.wait_for(timeout=context.config.browser.timeout_ms)
        option = poppers.last.get_by_text(
            re.compile(rf"^{re.escape(category)}(?:\(|$)")
        )
        if await option.count() != 1:
            await fields.first.fill(category)
            await context.page.wait_for_timeout(500)
            option = poppers.last.get_by_text(
                re.compile(rf"^{re.escape(category)}(?:\(|$)")
            )
        if await option.count() != 1:
            return False
        await option.first.click()
        deadline = monotonic() + max(
            context.config.browser.navigation_timeout_ms, 60_000
        ) / 1000
        while monotonic() < deadline:
            if await real_sku_rows(context.page) is not None:
                return True
            await context.page.wait_for_timeout(500)
        return False
