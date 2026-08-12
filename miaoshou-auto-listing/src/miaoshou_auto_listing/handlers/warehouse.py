from __future__ import annotations

from ..browser.helpers import element_value
from ..errors import ExecutorError
from ..models import ErrorCode, Step
from .base import Handler, HandlerContext
from .editor_dom import all_sku_snapshots, real_sku_rows
from .stock import StockHandler


class WarehouseHandler(Handler):
    step = Step.SET_WAREHOUSE

    async def run(self, context: HandlerContext) -> None:
        warehouse = context.config.warehouse[context.task.market]
        name = str(warehouse["name"])
        value = str(warehouse.get("value", ""))
        try:
            rows = await real_sku_rows(context.page)
            if rows is not None:
                if not getattr(context, "stock_warehouse_configured", False):
                    # Resume-from SET_WAREHOUSE must restore the per-SKU modal state.
                    await StockHandler().run(context)
                    rows = await real_sku_rows(context.page)
                    if rows is None:
                        raise ValueError("SKU rows disappeared after stock setup")
                expected = str(
                    context.task.stock_per_sku
                    if context.task.stock_per_sku is not None
                    else context.config.stock_rules[context.task.category_group][
                        "default_stock"
                    ]
                )
                values = [
                    item["stock"]
                    for item in await all_sku_snapshots(context.page)
                ]
                if any(value != expected for value in values):
                    raise ValueError(
                        f"SKU stocks are {values!r}, expected every value to be {expected!r}"
                    )
                return
            selector = await context.selectors.resolve(context.page, "warehouse_selector")
            current = ""
            try:
                current = await selector.input_value()
            except Exception:
                current = await selector.text_content() or ""
            if name in current:
                return
            if value:
                try:
                    await selector.select_option(value=value)
                    selected = await selector.input_value()
                    if selected == value:
                        return
                except Exception:
                    pass
            await selector.click()
            option = await context.selectors.resolve(
                context.page, "warehouse_option", warehouse_name=name
            )
            await option.click()
            selected_text = await element_value(selector)
            if name not in selected_text:
                raise ValueError(
                    f"Warehouse selection was not reflected in control: {selected_text!r}"
                )
        except Exception as exc:
            raise ExecutorError(
                ErrorCode.WAREHOUSE_NOT_FOUND,
                f"Cannot select warehouse {name!r}: {exc}",
                step=self.step,
            ) from exc
