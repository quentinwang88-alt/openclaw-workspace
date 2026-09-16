from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP

from ..browser.helpers import element_value, fill_and_verify, parse_decimal
from ..errors import ExecutorError
from ..models import ErrorCode, PricingMode, Step
from .base import Handler, HandlerContext
from .editor_dom import (
    all_sku_snapshots,
    choose_radio,
    dismiss_dialog,
    editor_root,
    real_sku_rows,
    row_label,
    sku_column_indexes,
    sku_header,
)


def multiplier_sale_price(purchase_price: Decimal, multiplier: Decimal) -> Decimal:
    return (purchase_price * multiplier).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )


def multiplier_price_matches(
    actual: Decimal, expected: Decimal, tolerance: Decimal = Decimal("0.02")
) -> bool:
    # Miaoshou applies the formula in the target currency and echoes a reverse-
    # converted CNY value. A two-cent tolerance only absorbs conversion rounding.
    return abs(actual - expected) <= tolerance


class PriceHandler(Handler):
    step = Step.SET_PRICE

    async def run(self, context: HandlerContext) -> None:
        try:
            real_rows = await real_sku_rows(context.page)
            if real_rows is not None:
                await self._fill_real_rows(context, real_rows)
                return
            rows = await context.selectors.resolve_all(context.page, "sku_rows")
            count = await rows.count()
            if count == 0:
                raise ValueError("SKU table contains no rows")
            for index in range(count):
                row = rows.nth(index)
                target = await context.selectors.resolve(row, "sku_sale_price", visible=False)
                if context.task.pricing_mode == PricingMode.FIXED:
                    sale_price = context.task.fixed_sale_price
                elif context.task.pricing_mode == PricingMode.PURCHASE_MULTIPLIER:
                    source = await context.selectors.resolve(
                        row, "sku_purchase_price", visible=False
                    )
                    purchase_cost = parse_decimal(await element_value(source))
                    sale_price = multiplier_sale_price(
                        purchase_cost, context.task.purchase_price_multiplier
                    )
                else:
                    source = await context.selectors.resolve(
                        row, "sku_purchase_price", visible=False
                    )
                    purchase_cost = parse_decimal(await element_value(source))
                    sale_price = context.pricing.calculate(
                        purchase_cost, context.task.pricing_rule_id
                    )
                await fill_and_verify(target, format(sale_price, "f"))
        except Exception as exc:
            if isinstance(exc, ExecutorError):
                exc.step = self.step
                raise
            raise ExecutorError(
                ErrorCode.PRICE_FILL_FAILED,
                f"Failed to fill dynamic SKU prices: {exc}",
                step=self.step,
            ) from exc

    async def _fill_real_rows(self, context: HandlerContext, rows) -> None:
        if context.task.pricing_mode == PricingMode.FIXED:
            await self._fill_all_fixed_price(context)
            return
        if context.task.pricing_mode == PricingMode.PURCHASE_MULTIPLIER:
            await self._fill_purchase_multiplier(context)
            return
        platform_prices = []
        columns = await sku_column_indexes(context.page)
        for index in range(await rows.count()):
            row = rows.nth(index)
            cells = row.locator(".pro-virtual-table__row-cell")
            target = cells.nth(columns["price"]).locator("input").first
            source = cells.nth(columns["source"]).locator("input").first
            purchase_cost = parse_decimal(await element_value(source))
            sale_price = context.pricing.calculate(
                purchase_cost, context.task.pricing_rule_id
            )
            expected = format(sale_price, "f")
            await fill_and_verify(target, expected)
            await target.press("Tab")
            actual = (await target.input_value()).strip()
            if actual != expected:
                label = await row_label(row, index)
                raise ValueError(
                    f"{label}: final CNY price drifted from {expected} to {actual}"
                )
            converted = cells.nth(columns["price"]).locator(".pro-readonly-component")
            platform_prices.append(
                (await converted.first.inner_text()).strip()
                if await converted.count()
                else ""
            )
        setattr(context, "platform_prices", platform_prices)

    async def _fill_purchase_multiplier(self, context: HandlerContext) -> None:
        multiplier = context.task.purchase_price_multiplier
        if multiplier is None:
            raise ValueError("Purchase-price multiplier is missing")
        current = await all_sku_snapshots(context.page)
        if not current:
            raise ValueError("SKU rows are unavailable before multiplier price")
        if self._all_multiplier_prices_match(current, multiplier):
            setattr(
                context,
                "platform_prices",
                [item["platform_price"] for item in current],
            )
            return

        editor = await editor_root(context.page)
        if editor is None:
            raise ValueError("Miaoshou editor is unavailable for multiplier price")
        await dismiss_dialog(context.page, "批量修改本地展示价")
        header = await sku_header(editor, "本地展示价")
        await header.get_by_role("button", name="批量", exact=True).click()
        dialog = context.page.locator("[role='dialog']:visible").filter(
            has_text="批量修改本地展示价"
        )
        await dialog.wait_for(timeout=context.config.browser.timeout_ms)

        await choose_radio(dialog, "formula")
        base = dialog.locator(".base-value-type-select")
        if await base.count() != 1:
            raise ValueError("Formula base-price selector was not unique")
        if "来源原价" not in " ".join((await base.inner_text()).split()):
            await base.click()
            option = context.page.locator(".jx-select-dropdown:visible").get_by_role(
                "option", name="来源原价", exact=True
            )
            await option.wait_for(
                state="visible", timeout=context.config.browser.timeout_ms
            )
            if await option.count() != 1:
                raise ValueError("Formula source-price option was not unique")
            await option.click()
        if "来源原价" not in " ".join((await base.inner_text()).split()):
            raise ValueError("Formula base price did not switch to 来源原价")

        await fill_and_verify(
            dialog.get_by_placeholder("倍数", exact=True), format(multiplier, "f")
        )
        await fill_and_verify(dialog.get_by_placeholder("加数", exact=True), "0")
        await fill_and_verify(dialog.get_by_placeholder("减数", exact=True), "0")
        await choose_radio(dialog, "round")
        await choose_radio(dialog, "twoPoint")
        await choose_radio(dialog, "all")
        await dialog.get_by_role("button", name="确定", exact=True).last.click()
        await dialog.wait_for(
            state="hidden", timeout=context.config.browser.timeout_ms
        )
        await context.page.wait_for_timeout(500)

        current = await all_sku_snapshots(context.page)
        if not self._all_multiplier_prices_match(current, multiplier):
            details = [
                {
                    "sku": item["label"],
                    "purchase": item.get("purchase_price", ""),
                    "actual": item["price"],
                    "expected": format(
                        multiplier_sale_price(
                            parse_decimal(item.get("purchase_price", "")), multiplier
                        ),
                        "f",
                    ),
                }
                for item in current
            ]
            raise ValueError(f"Multiplier price verification failed: {details}")
        setattr(
            context,
            "platform_prices",
            [item["platform_price"] for item in current],
        )

    @staticmethod
    def _all_multiplier_prices_match(current, multiplier: Decimal) -> bool:
        try:
            return all(
                multiplier_price_matches(
                    parse_decimal(item["price"]),
                    multiplier_sale_price(
                        parse_decimal(item.get("purchase_price", "")), multiplier
                    ),
                )
                for item in current
            )
        except (KeyError, ValueError):
            return False

    async def _fill_all_fixed_price(self, context: HandlerContext) -> None:
        expected = format(context.task.fixed_sale_price, "f")
        rows = await real_sku_rows(context.page)
        if rows is None:
            raise ValueError("SKU rows are unavailable before bulk price")
        current = await all_sku_snapshots(context.page)
        try:
            already_correct = all(
                parse_decimal(item["price"]) == context.task.fixed_sale_price
                for item in current
            )
        except ValueError:
            already_correct = False
        if already_correct:
            setattr(
                context,
                "platform_prices",
                [item["platform_price"] for item in current],
            )
            return
        editor = await editor_root(context.page)
        if editor is None:
            raise ValueError("Miaoshou editor is unavailable for bulk price")
        await dismiss_dialog(context.page, "批量修改本地展示价")
        header = await sku_header(editor, "本地展示价")
        await header.get_by_role("button", name="批量", exact=True).click()
        dialog = context.page.locator("[role='dialog']:visible").filter(
            has_text="批量修改本地展示价"
        )
        await dialog.wait_for(timeout=context.config.browser.timeout_ms)

        await choose_radio(dialog, "newValue")
        price_input = dialog.get_by_placeholder("输入要设置的价格", exact=True)
        if await price_input.count() != 1:
            raise ValueError("Bulk fixed-price input was not unique")
        group = price_input.first.locator(
            "xpath=ancestor::div[contains(@class,'pro-input-group')][1]"
        )
        currency = group.locator(".currency-select")
        if await currency.count() != 1:
            raise ValueError("Bulk fixed-price currency selector was not unique")
        if "CNY" not in (await currency.inner_text()).split():
            await currency.click()
            cny = context.page.locator(
                ".jx-select-dropdown:visible"
            ).get_by_role("option", name="CNY", exact=True)
            await cny.first.wait_for(
                state="visible", timeout=context.config.browser.timeout_ms
            )
            if await cny.count() != 1:
                raise ValueError(
                    f"CNY matched {await cny.count()} visible bulk-price options"
                )
            await cny.first.click()
        await fill_and_verify(price_input.first, expected)
        await choose_radio(dialog, "all")
        await dialog.get_by_role("button", name="确定", exact=True).last.click()
        await dialog.wait_for(state="hidden", timeout=context.config.browser.timeout_ms)
        await context.page.wait_for_timeout(300)

        current = await all_sku_snapshots(context.page)
        values = [item["price"] for item in current]
        if any(
            parse_decimal(value) != context.task.fixed_sale_price for value in values
        ):
            raise ValueError(
                f"Bulk price did not cover every SKU: values={values}, expected={expected}"
            )
        setattr(
            context,
            "platform_prices",
            [item["platform_price"] for item in current],
        )
