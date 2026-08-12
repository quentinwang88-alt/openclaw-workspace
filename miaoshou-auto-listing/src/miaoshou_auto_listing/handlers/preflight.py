from __future__ import annotations

from decimal import Decimal
import re

from ..errors import ExecutorError
from ..models import ErrorCode, PricingMode, Step
from .base import Handler, HandlerContext
from .editor_dom import (
    all_sku_snapshots,
    contains_thai,
    editor_root,
    grams_to_kg_text,
    logistics_inputs,
    real_sku_rows,
    title_value,
)
from .price import (
    PriceHandler,
    multiplier_price_matches,
    multiplier_sale_price,
)
from .stock import StockHandler


def numeric_equal(actual: str, expected: str) -> bool:
    try:
        return Decimal(actual) == Decimal(expected)
    except Exception:
        return False


def shorten_option_name(value: str, max_length: int = 50) -> str:
    value = str(value or "").strip()
    if len(value) <= max_length:
        return value
    head_length = (max_length - 1) // 2
    tail_length = max_length - 1 - head_length
    return f"{value[:head_length].rstrip()}…{value[-tail_length:].lstrip()}"


def unique_short_option_names(values, max_length: int = 50):
    results = []
    used = set()
    for index, value in enumerate(values, start=1):
        candidate = shorten_option_name(value, max_length)
        if candidate in used:
            suffix = f"·{index}"
            candidate = (
                shorten_option_name(value, max_length - len(suffix)) + suffix
            )
        used.add(candidate)
        results.append(candidate)
    return results


async def normalize_option_name_lengths(editor) -> int:
    candidates = []
    inputs = editor.locator("input:visible")
    for index in range(await inputs.count()):
        field = inputs.nth(index)
        parent = field.locator(
            "xpath=ancestor::*[contains(concat(' ', normalize-space(@class), ' '), "
            "' jx-form-item__content ')][1]"
        )
        if await parent.count() != 1:
            continue
        if not await field.is_editable():
            continue
        value = await field.input_value()
        counter_text = " ".join((await parent.inner_text()).split())
        if not re.search(
            rf"\b{len(value)}\s*/\s*50\b", counter_text
        ):
            continue
        if value:
            candidates.append((field, value))
    normalized = unique_short_option_names([value for _, value in candidates])
    changed = 0
    for (field, value), replacement in zip(candidates, normalized):
        if replacement != value:
            await field.fill(replacement)
            changed += 1
    return changed


class PreflightHandler(Handler):
    step = Step.PREFLIGHT

    async def run(self, context: HandlerContext) -> None:
        editor = await editor_root(context.page)
        rows = await real_sku_rows(context.page)
        if editor is None or rows is None:
            raise ExecutorError(
                ErrorCode.PREFLIGHT_FAILED,
                "Calibrated Miaoshou editor/SKU table is unavailable",
                step=self.step,
            )

        normalized_options = await normalize_option_name_lengths(editor)
        if normalized_options:
            await context.page.wait_for_timeout(500)

        # Exchange rates can change while a draft is open. Re-write price at the
        # last possible deterministic step instead of trusting a saved CNY echo.
        await PriceHandler().run(context)
        if not getattr(context, "stock_warehouse_configured", False):
            await StockHandler().run(context)
        # Bulk dialogs can cause Miaoshou's virtual table to recycle row DOM.
        # Never keep the locator captured before those interactions.
        rows = await real_sku_rows(context.page)
        if rows is None:
            raise ExecutorError(
                ErrorCode.PREFLIGHT_FAILED,
                "SKU rows disappeared after bulk price/stock updates",
                step=self.step,
            )

        profile = context.config.logistics_profiles[context.task.category_group]
        expected_weight = grams_to_kg_text(profile["weight_g"])
        expected_stock = str(
            context.task.stock_per_sku
            if context.task.stock_per_sku is not None
            else context.config.stock_rules[context.task.category_group]["default_stock"]
        )
        expected_price = (
            format(context.task.fixed_sale_price, "f")
            if context.task.pricing_mode == PricingMode.FIXED
            else None
        )
        issues = []
        sku_results = []
        snapshots = await all_sku_snapshots(context.page)
        for item in snapshots:
            label = item["label"]
            price = item["price"]
            purchase_price = item.get("purchase_price", "")
            stock = item["stock"]
            weight = item["weight"]
            platform_price = item["platform_price"]
            if expected_price is not None and not numeric_equal(price, expected_price):
                issues.append(f"{label}: price={price}, expected={expected_price}")
            expected_multiplier_price = ""
            if context.task.pricing_mode == PricingMode.PURCHASE_MULTIPLIER:
                try:
                    purchase_decimal = Decimal(purchase_price)
                    expected_decimal = multiplier_sale_price(
                        purchase_decimal, context.task.purchase_price_multiplier
                    )
                    expected_multiplier_price = format(expected_decimal, "f")
                    if not multiplier_price_matches(Decimal(price), expected_decimal):
                        issues.append(
                            f"{label}: purchase={purchase_price}, "
                            f"multiplier={context.task.purchase_price_multiplier}, "
                            f"price={price}, expected={expected_multiplier_price}"
                        )
                except Exception:
                    issues.append(
                        f"{label}: multiplier pricing has invalid purchase/price "
                        f"values {purchase_price!r}/{price!r}"
                    )
            if stock != expected_stock:
                issues.append(f"{label}: stock={stock}, expected={expected_stock}")
            if not numeric_equal(weight, expected_weight):
                issues.append(f"{label}: weight={weight}, expected={expected_weight}")
            if not platform_price:
                issues.append(f"{label}: platform converted price is empty")
            sku_results.append(
                {
                    "label": label,
                    "purchase_cny": purchase_price,
                    "cny_price": price,
                    "expected_cny_price": expected_multiplier_price,
                    "platform_price": platform_price,
                    "stock": stock,
                    "weight_kg": weight,
                }
            )

        fields = await logistics_inputs(editor)
        expected_logistics = [
            expected_weight,
            str(profile["length_cm"]),
            str(profile["width_cm"]),
            str(profile["height_cm"]),
        ]
        actual_logistics = [
            (await field.input_value()).strip() for field in fields[:4]
        ]
        if len(fields) < 4 or actual_logistics != expected_logistics:
            issues.append(
                f"package logistics={actual_logistics}, expected={expected_logistics}"
            )

        shop_name = str(context.config.shops[context.task.target_shop]["display_name"])
        if shop_name not in await editor.inner_text():
            issues.append(f"target shop {shop_name!r} is not visible in editor")
        title = await title_value(editor)
        if context.task.market == "TH" and not contains_thai(title):
            issues.append("product title is not Thai")

        size_chart_count = await editor.locator(".size-chart-box img").count()
        if context.task.category_group == "CLOTHING" and size_chart_count == 0:
            raise ExecutorError(
                ErrorCode.SIZE_CHART_REQUIRED,
                "CLOTHING product has no persisted size-chart image",
                step=self.step,
            )
        validation_errors = await editor.locator(
            ".jx-form-item__error:visible"
        ).all_text_contents()
        if validation_errors:
            issues.append(f"visible validation errors: {validation_errors}")

        snapshot = {
            "shop": shop_name,
            "market": context.task.market,
            "pricing": {
                "mode": context.task.pricing_mode.value,
                "fixed_sale_price": (
                    format(context.task.fixed_sale_price, "f")
                    if context.task.fixed_sale_price is not None
                    else ""
                ),
                "purchase_price_multiplier": (
                    format(context.task.purchase_price_multiplier, "f")
                    if context.task.purchase_price_multiplier is not None
                    else ""
                ),
            },
            "title": title,
            "sku_count": len(snapshots),
            "skus": sku_results,
            "package": {
                "weight_kg": actual_logistics[0] if actual_logistics else "",
                "length_cm": actual_logistics[1] if len(actual_logistics) > 1 else "",
                "width_cm": actual_logistics[2] if len(actual_logistics) > 2 else "",
                "height_cm": actual_logistics[3] if len(actual_logistics) > 3 else "",
            },
            "size_chart_count": size_chart_count,
            "image_translation": getattr(context, "image_translation", {}),
            "validation_errors": validation_errors,
        }
        setattr(context, "preflight_snapshot", snapshot)
        if issues:
            raise ExecutorError(
                ErrorCode.PREFLIGHT_FAILED,
                "; ".join(issues),
                step=self.step,
            )
