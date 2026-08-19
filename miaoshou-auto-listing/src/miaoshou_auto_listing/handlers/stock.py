from __future__ import annotations

from time import monotonic

from ..browser.helpers import fill_and_verify
from ..errors import ExecutorError
from ..models import ErrorCode, Step
from .base import Handler, HandlerContext
from .editor_dom import (
    all_sku_snapshots,
    choose_radio,
    dismiss_dialog,
    editor_root,
    real_sku_rows,
    sku_header,
)


def warehouse_selection_matches(warehouse_name: str, evidence: str) -> bool:
    expected = " ".join(str(warehouse_name).split()).casefold()
    actual = " ".join(str(evidence).split()).casefold()
    return bool(expected and expected in actual)


class StockHandler(Handler):
    step = Step.SET_STOCK

    async def run(self, context: HandlerContext) -> None:
        default_stock = str(
            context.task.stock_per_sku
            if context.task.stock_per_sku is not None
            else context.config.stock_rules[context.task.category_group]["default_stock"]
        )
        try:
            rows = await real_sku_rows(context.page)
            if rows is not None:
                await self._configure_real_rows(context, rows, default_stock)
                setattr(context, "stock_warehouse_configured", True)
                return
            mode = await context.selectors.resolve(context.page, "stock_by_warehouse")
            try:
                checked = await mode.is_checked()
            except Exception:
                checked = False
            if not checked:
                await mode.click()
            inputs = await context.selectors.resolve_all(context.page, "stock_inputs")
            for index in range(await inputs.count()):
                await fill_and_verify(inputs.nth(index), default_stock)
        except Exception as exc:
            if isinstance(exc, ExecutorError):
                exc.step = self.step
                raise
            raise ExecutorError(
                ErrorCode.STOCK_FILL_FAILED,
                f"Failed to set warehouse stock: {exc}",
                step=self.step,
            ) from exc

    async def _configure_real_rows(
        self, context: HandlerContext, rows, default_stock: str
    ) -> None:
        warehouse_name = str(context.config.warehouse[context.task.market]["name"])
        for attempt in range(2):
            snapshots = await all_sku_snapshots(context.page)
            if all(item["stock"] == default_stock for item in snapshots):
                return
            await self._bulk_set_stock(
                context, warehouse_name=warehouse_name, stock=default_stock
            )
            await context.page.wait_for_timeout(300)
        values = [
            item["stock"] for item in await all_sku_snapshots(context.page)
        ]
        raise ValueError(
            f"Bulk stock did not cover every SKU after a current-step retry: "
            f"values={values}, expected={default_stock!r}"
        )

    async def _bulk_set_stock(
        self, context: HandlerContext, *, warehouse_name: str, stock: str
    ) -> None:
        editor = await editor_root(context.page)
        if editor is None:
            raise ValueError("Miaoshou editor is unavailable for bulk stock")
        await dismiss_dialog(context.page, "批量修改库存")
        header = await sku_header(editor, "库存")
        await header.get_by_role("button", name="批量", exact=True).click()
        dialog = context.page.locator("[role='dialog']:visible").filter(
            has_text="批量修改库存"
        )
        await dialog.wait_for(timeout=context.config.browser.timeout_ms)
        await choose_radio(dialog, "warehouse")
        await choose_radio(dialog, "all")

        shop_name = str(context.config.shops[context.task.target_shop]["display_name"])
        warehouse_row = dialog.locator(".pro-virtual-table__row-body").filter(
            has_text=shop_name
        )
        if await warehouse_row.count() != 1:
            raise ValueError(
                f"Bulk stock warehouse row for {shop_name!r} was not unique"
            )
        selector = warehouse_row.locator(".jx-select:visible")
        if await selector.count() != 1:
            raise ValueError("Bulk stock warehouse selector was not unique")
        selected_evidence = " ".join(
            value
            for value in (
                await selector.inner_text(),
                await selector.get_attribute("title") or "",
                await selector.get_attribute("aria-label") or "",
            )
            if value
        )
        if not warehouse_selection_matches(warehouse_name, selected_evidence):
            await selector.click()
            options = context.page.locator(
                ".jx-select-dropdown:visible [role='option'], "
                ".jx-select-dropdown:visible .jx-select-dropdown__item, "
                "[role='listbox']:visible [role='option']"
            )
            deadline = monotonic() + context.config.browser.timeout_ms / 1000
            while await options.count() == 0:
                if monotonic() >= deadline:
                    break
                await context.page.wait_for_timeout(200)
            option_texts = await options.all_inner_texts()
            matches = [
                options.nth(index)
                for index, text in enumerate(option_texts)
                if text.splitlines()[0].strip() == warehouse_name
            ]
            if len(matches) != 1:
                raise ValueError(
                    f"Warehouse {warehouse_name!r} matched {len(matches)} options; "
                    f"selected={selected_evidence!r}; available={option_texts}"
                )
            await matches[0].click()
        stock_inputs = warehouse_row.locator(
            "input[type='text']:not([readonly]):not([role='combobox'])"
        )
        if await stock_inputs.count() != 1:
            raise ValueError("Bulk warehouse stock input was not unique")
        await fill_and_verify(stock_inputs.first, stock)
        await dialog.get_by_role("button", name="确定", exact=True).last.click()
        await dialog.wait_for(state="hidden", timeout=context.config.browser.timeout_ms)
