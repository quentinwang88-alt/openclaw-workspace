from __future__ import annotations

from ..browser.helpers import fill_and_verify
from ..errors import ExecutorError
from ..models import ErrorCode, Step
from .base import Handler, HandlerContext
from .editor_dom import (
    all_sku_snapshots,
    choose_radio,
    dismiss_dialog,
    editor_root,
    grams_to_kg_text,
    logistics_inputs,
    real_sku_rows,
    sku_header,
)


class LogisticsHandler(Handler):
    step = Step.SET_LOGISTICS

    async def run(self, context: HandlerContext) -> None:
        profile = context.config.logistics_profiles[context.task.category_group]
        weight_kg = grams_to_kg_text(profile["weight_g"])
        fields = {
            "weight_input": weight_kg,
            "length_input": profile["length_cm"],
            "width_input": profile["width_cm"],
            "height_input": profile["height_cm"],
        }
        try:
            rows = await real_sku_rows(context.page)
            if rows is not None:
                snapshots = await all_sku_snapshots(context.page)
                if any(item["weight"] != weight_kg for item in snapshots):
                    editor = await editor_root(context.page)
                    if editor is None:
                        raise ValueError("Editor disappeared before bulk weight")
                    await dismiss_dialog(context.page, "批量修改重量")
                    header = await sku_header(editor, "重量")
                    await header.get_by_role(
                        "button", name="批量", exact=True
                    ).click()
                    dialog = context.page.locator(
                        "[role='dialog']:visible"
                    ).filter(has_text="批量修改重量")
                    await dialog.wait_for(
                        timeout=context.config.browser.timeout_ms
                    )
                    await choose_radio(dialog, "newValue")
                    await choose_radio(dialog, "all")
                    form = dialog.locator(
                        "input[type='radio'][value='newValue']"
                    ).locator(
                        "xpath=ancestor::*[contains(concat(' ',normalize-space(@class),' '),"
                        "' jx-form-item ')][1]"
                    )
                    weight_inputs = form.locator(
                        "input[type='text']:not([readonly]):not([role='combobox'])"
                    )
                    if await weight_inputs.count() != 1:
                        raise ValueError("Bulk SKU weight input was not unique")
                    await fill_and_verify(weight_inputs.first, weight_kg)
                    await dialog.get_by_role(
                        "button", name="确定", exact=True
                    ).last.click()
                    await dialog.wait_for(
                        state="hidden", timeout=context.config.browser.timeout_ms
                    )
                    snapshots = await all_sku_snapshots(context.page)
                    if any(item["weight"] != weight_kg for item in snapshots):
                        raise ValueError(
                            "Bulk weight did not cover every SKU: "
                            f"{[item['weight'] for item in snapshots]}"
                        )
                editor_fields = await logistics_inputs(
                    context.page.locator("[role='dialog']:visible").filter(
                        has_text="保存并发布"
                    ).first
                )
                if len(editor_fields) < 4:
                    raise ValueError(
                        f"Expected 4 logistics inputs, found {len(editor_fields)}"
                    )
                values = (
                    weight_kg,
                    str(profile["length_cm"]),
                    str(profile["width_cm"]),
                    str(profile["height_cm"]),
                )
                for locator, value in zip(editor_fields[:4], values):
                    await fill_and_verify(locator, value)
                return
            for key, value in fields.items():
                locator = await context.selectors.resolve(context.page, key)
                await fill_and_verify(locator, str(value))
        except Exception as exc:
            if isinstance(exc, ExecutorError):
                exc.step = self.step
                raise
            raise ExecutorError(
                ErrorCode.LOGISTICS_FILL_FAILED,
                f"Failed to fill logistics profile: {exc}",
                step=self.step,
            ) from exc
