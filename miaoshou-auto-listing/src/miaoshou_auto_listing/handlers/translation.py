from __future__ import annotations

from time import monotonic

from ..browser.helpers import wait_for_any_key
from ..errors import ExecutorError
from ..models import ErrorCode, Step
from .base import Handler, HandlerContext
from .editor_dom import contains_thai, editor_root, title_value


class TranslationHandler(Handler):
    step = Step.TRANSLATE

    async def run(self, context: HandlerContext) -> None:
        language = str(context.config.markets[context.task.market]["language"])
        editor = await editor_root(context.page)
        if editor is not None:
            await self._translate_real_editor(context, editor, language)
            return
        if await context.selectors.exists(context.page, "translation_language_selector"):
            language_selector = await context.selectors.resolve(
                context.page, "translation_language_selector"
            )
            current = ""
            try:
                current = await language_selector.input_value()
            except Exception:
                current = await language_selector.text_content() or ""
            if language not in current:
                await language_selector.click()
                language_option = await context.selectors.resolve(
                    context.page,
                    "translation_language_option",
                    language=language,
                )
                await language_option.click()
        button = await context.selectors.resolve(context.page, "translate_button")
        await button.click()
        try:
            signal, locator = await wait_for_any_key(
                context.page,
                context.selectors,
                ("translation_success", "error_toast"),
                timeout_ms=context.config.browser.navigation_timeout_ms,
            )
            if signal == "error_toast":
                message = (await locator.text_content() or "translation failed").strip()
                raise ExecutorError(
                    ErrorCode.TRANSLATE_FAILED, message, step=self.step
                )
        except ExecutorError as exc:
            # No signal is treated as a transient page timeout; an explicit
            # error toast above remains the non-retryable business failure.
            raise

    async def _translate_real_editor(
        self, context: HandlerContext, editor, language: str
    ) -> None:
        current_title = await title_value(editor)
        if context.task.market == "TH" and contains_thai(current_title):
            return
        try:
            button = editor.get_by_role("button", name="一键翻译", exact=True)
            if await button.count() != 1:
                raise ValueError(
                    f"Expected one translate button, found {await button.count()}"
                )
            await button.click()
            option = context.page.get_by_text(language, exact=True)
            await option.last.wait_for(timeout=context.config.browser.timeout_ms)
            async with context.page.expect_response(
                lambda response: "translateCollectItemInfo" in response.url,
                timeout=context.config.browser.navigation_timeout_ms,
            ) as response_info:
                await option.last.click()
            response = await response_info.value
            if response.status != 200:
                raise ValueError(f"Translation API returned HTTP {response.status}")
            deadline = monotonic() + context.config.browser.navigation_timeout_ms / 1000
            translated_title = ""
            while monotonic() < deadline:
                translated_title = await title_value(editor)
                if context.task.market != "TH" or contains_thai(translated_title):
                    return
                await context.page.wait_for_timeout(500)
            raise ValueError(
                "Translation API succeeded but title still contains no Thai "
                f"characters after waiting; title={translated_title!r}"
            )
        except ExecutorError:
            raise
        except Exception as exc:
            raise ExecutorError(
                ErrorCode.TRANSLATE_FAILED,
                f"Two-step Miaoshou translation failed: {exc}",
                step=self.step,
            ) from exc
