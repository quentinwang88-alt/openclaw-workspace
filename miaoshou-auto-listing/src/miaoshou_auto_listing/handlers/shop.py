from __future__ import annotations

from time import monotonic

from ..browser.helpers import wait_for_text_in_key
from ..errors import ExecutorError, SelectorNotFound
from ..models import ErrorCode, Step
from .base import Handler, HandlerContext
from .editor_dom import editor_root


CLOTHING_CATEGORY_TOKENS = (
    "女士上装",
    "男士上装",
    "女装",
    "男装",
    "夹克",
    "外套",
    "开衫",
    "上衣",
    "防晒衣",
    "防晒服",
    "罩衫",
    "衬衫",
    "针织衫",
    "毛衣",
    "背心",
    "T恤",
    "T-shirt",
    "连衣裙",
    "半身裙",
    "裤装",
    "套装",
    "卫衣",
    "长裤",
    "运动服",
)
ACCESSORY_CATEGORY_TOKENS = (
    "发饰",
    "发夹",
    "发卡",
    "发簪",
    "抓夹",
    "鲨鱼夹",
    "鸭嘴夹",
    "围巾",
    "披肩",
    "首饰",
    "饰品",
    "耳饰",
    "耳钉",
    "耳环",
    "耳坠",
    "耳夹",
    "耳扣",
    "耳圈",
    "项链",
    "吊坠",
    "手链",
    "脚链",
    "手镯",
    "戒指",
    "胸针",
    "帽子",
    "棒球帽",
    "渔夫帽",
    "毛线帽",
    "针织帽",
    "贝雷帽",
    "服饰配件",
)


def infer_category_group(text: str) -> str:
    if any(token in text for token in ACCESSORY_CATEGORY_TOKENS):
        return "ACCESSORY"
    if any(token in text for token in CLOTHING_CATEGORY_TOKENS):
        return "CLOTHING"
    return ""


class ShopHandler(Handler):
    step = Step.SET_SHOP

    async def run(self, context: HandlerContext) -> None:
        shop = context.config.shops[context.task.target_shop]
        display_name = str(shop["display_name"])
        if display_name.startswith("REPLACE_"):
            raise ExecutorError(
                ErrorCode.SHOP_BIND_FAILED,
                f"Configure the real display_name for {context.task.target_shop}",
                step=self.step,
            )
        editor = await editor_root(context.page)
        if editor is not None and display_name in await editor.inner_text():
            self._ensure_category(context, await editor.inner_text())
            return
        row = getattr(context, "product_row", None)
        if editor is None and row is not None:
            await self._set_shop_on_real_row(context, row, display_name)
            self._ensure_category(context, await row.inner_text())
            return
        try:
            chips = await context.selectors.resolve_all(context.page, "selected_shop_chips")
            selected = " ".join(
                (await chips.nth(index).text_content() or "")
                for index in range(await chips.count())
            )
            if display_name in selected:
                self._ensure_category(context, selected)
                return
        except SelectorNotFound:
            pass
        try:
            selector = await context.selectors.resolve(context.page, "shop_selector")
            await selector.click()
            option = await context.selectors.resolve(
                context.page, "shop_option", shop_name=display_name
            )
            await option.click()
            if await context.selectors.exists(context.page, "shop_apply_button"):
                apply_button = await context.selectors.resolve(context.page, "shop_apply_button")
                await apply_button.click()
            await wait_for_text_in_key(
                context.page,
                context.selectors,
                "selected_shop_chips",
                display_name,
                timeout_ms=context.config.browser.timeout_ms,
            )
            self._ensure_category(context, await context.page.locator("body").inner_text())
        except Exception as exc:
            raise ExecutorError(
                ErrorCode.SHOP_BIND_FAILED,
                f"Failed to ensure target shop {display_name}: {exc}",
                step=self.step,
            ) from exc

    async def _set_shop_on_real_row(
        self, context: HandlerContext, row, display_name: str
    ) -> None:
        if display_name in await row.inner_text():
            return
        shop = context.config.shops[context.task.target_shop]
        shop_id = str(shop.get("shop_id", ""))
        try:
            trigger = row.get_by_text("选择店铺", exact=True)
            if await trigger.count() == 0:
                raise ValueError("row has no exact 选择店铺 control")
            await trigger.first.click()
            dialog = context.page.locator("[role='dialog']:visible").filter(
                has_text="选择店铺"
            )
            await dialog.last.wait_for(timeout=context.config.browser.timeout_ms)
            dialog = dialog.last
            if shop_id:
                choice = dialog.locator(
                    f"input[type='radio'][value='{shop_id}'], "
                    f"input[type='checkbox'][value='{shop_id}']"
                )
                if await choice.count() != 1:
                    raise ValueError(
                        f"shop ID {shop_id!r} matched {await choice.count()} controls"
                    )
                if not await choice.first.is_checked():
                    await choice.first.check(force=True)
            else:
                await dialog.get_by_text(display_name, exact=True).click()
            confirm = dialog.get_by_role("button", name="确定", exact=True)
            if await confirm.count() == 0:
                raise ValueError("shop dialog has no exact 确定 button")
            await confirm.last.click()
            deadline = monotonic() + context.config.browser.timeout_ms / 1000
            while monotonic() < deadline:
                if display_name in await row.inner_text():
                    await self._dismiss_shop_progress(context)
                    return
                await context.page.wait_for_timeout(200)
            raise ValueError("selected shop was not reflected in product row")
        except Exception as exc:
            raise ExecutorError(
                ErrorCode.SHOP_BIND_FAILED,
                f"Failed to bind {display_name} on quick-listing row: {exc}",
                step=self.step,
            ) from exc

    async def _dismiss_shop_progress(self, context: HandlerContext) -> None:
        dialog = context.page.locator("[role='dialog']:visible").filter(
            has_text="总计"
        )
        if await dialog.count() == 0:
            return
        close = dialog.last.get_by_role("button", name="关闭", exact=True)
        if await close.count():
            await close.last.click()
            await dialog.last.wait_for(
                state="hidden", timeout=context.config.browser.timeout_ms
            )

    def _ensure_category(self, context: HandlerContext, page_text: str) -> None:
        if context.task.category_group != "AUTO":
            return
        inferred = infer_category_group(page_text)
        if not inferred:
            raise ExecutorError(
                ErrorCode.SHOP_BIND_FAILED,
                "Target shop is bound but product category group could not be inferred",
                step=self.step,
            )
        context.task.category_group = inferred
