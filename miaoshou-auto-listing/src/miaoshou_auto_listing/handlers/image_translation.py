from __future__ import annotations

import asyncio
from hashlib import sha256
import json
import os
import tempfile
from time import monotonic
from typing import Any, List
from urllib.parse import urldefrag

from ..errors import ExecutorError
from ..models import ErrorCode, Step
from ..services.size_chart_detector import (
    SizeChartDetectionError,
    SizeChartDetector,
)
from ..services.size_chart_coverage import (
    SizeChartCoverageError,
    SizeChartCoverageValidator,
)
from ..services.source_size_chart import SourceSizeChartExtractor
from .base import Handler, HandlerContext
from .editor_dom import all_sku_snapshots, dismiss_dialog, editor_root


IMAGE_LANGUAGE_BY_MARKET = {
    "TH": "th",
    "VN": "vi",
    "MY": "ms",
}

DETAIL_IMAGE_LIMIT = 30


def image_language_code(market: str) -> str:
    try:
        return IMAGE_LANGUAGE_BY_MARKET[market.upper()]
    except KeyError as exc:
        raise ValueError(
            f"Miaoshou image translation has no language mapping for {market}"
        ) from exc


def image_url_fingerprint(urls: List[str]) -> str:
    normalized = [urldefrag(str(url).strip())[0] for url in urls if str(url).strip()]
    if not normalized:
        return ""
    payload = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))
    return sha256(payload.encode("utf-8")).hexdigest()


def detail_image_delete_indices(total: int, limit: int = DETAIL_IMAGE_LIMIT) -> List[int]:
    """Keep the first images in source order and return the trailing excess."""
    if total <= limit:
        return []
    return list(range(limit, total))


async def _limit_detail_images(
    page: Any,
    processor: Any,
    *,
    limit: int = DETAIL_IMAGE_LIMIT,
    timeout_ms: int,
) -> int:
    """Delete trailing detail images using Miaoshou's bulk processor.

    Miaoshou opens the processor with every image selected. We explicitly set the
    desired selection instead of assuming that state, then delete only indices
    at or beyond the platform limit.
    """
    cards = processor.locator(".product-picture-item")
    total = await cards.count()
    delete_indices = set(detail_image_delete_indices(total, limit))
    if not delete_indices:
        return total

    for index in range(total):
        card = cards.nth(index)
        classes = str(await card.get_attribute("class") or "")
        selected = "is-selected" in classes.split()
        should_select = index in delete_indices
        if selected != should_select:
            await card.click(force=True)

    selected_count = await processor.locator(
        ".product-picture-item.is-selected"
    ).count()
    if selected_count != len(delete_indices):
        raise ValueError(
            "Could not select only the trailing excess detail images: "
            f"expected {len(delete_indices)}, selected {selected_count}"
        )

    delete_button = processor.get_by_role("button", name="批量删除", exact=True)
    if await delete_button.count() != 1 or await delete_button.is_disabled():
        raise ValueError("Detail-image bulk delete button is unavailable")
    await delete_button.click()

    confirmation = page.locator(
        ".jx-message-box:visible, [role='alertdialog']:visible"
    )
    deadline = monotonic() + min(timeout_ms / 1000, 3)
    while monotonic() < deadline:
        if await confirmation.count():
            confirm = confirmation.last.get_by_role(
                "button", name="确定", exact=True
            )
            if await confirm.count() == 0:
                confirm = confirmation.last.get_by_role(
                    "button", name="确认", exact=True
                )
            if await confirm.count() == 1:
                await confirm.click()
            break
        if await cards.count() <= limit:
            break
        await page.wait_for_timeout(100)

    deadline = monotonic() + timeout_ms / 1000
    while monotonic() < deadline:
        remaining = await cards.count()
        if remaining == limit:
            return remaining
        await page.wait_for_timeout(200)
    raise TimeoutError(
        f"Detail-image count did not reach {limit}; current count is {await cards.count()}"
    )


async def _image_fingerprint(container: Any) -> str:
    images = container.locator("img")
    urls = [
        str(await images.nth(index).get_attribute("src") or "")
        for index in range(await images.count())
    ]
    return image_url_fingerprint(urls)


async def _reuse_saved_translation(
    context: HandlerContext, region: str, container: Any, image_count: int
) -> bool:
    cached = dict(getattr(context, "image_translation", {}).get(region, {}))
    expected = str(
        cached.get("result_fingerprint")
        or (
            cached.get("source_fingerprint")
            if cached.get("status")
            in {"TRANSLATED", "REUSED_SAVED_TRANSLATION"}
            else ""
        )
        or ""
    )
    target = str(cached.get("target_language") or "")
    if not expected or target != image_language_code(context.task.market):
        return False
    if await _image_fingerprint(container) != expected:
        return False
    _record(
        context,
        region,
        "REUSED_SAVED_TRANSLATION",
        image_count,
        **{
            key: value
            for key, value in cached.items()
            if key not in {"status", "image_count"}
        },
    )
    return True


def _record(
    context: HandlerContext,
    region: str,
    status: str,
    image_count: int,
    **details: Any,
) -> None:
    results = dict(getattr(context, "image_translation", {}))
    current = dict(results.get(region, {}))
    current.update({"status": status, "image_count": image_count, **details})
    results[region] = current
    setattr(context, "image_translation", results)
    store = getattr(context, "image_translation_store", None)
    if store is not None:
        store.update(
            context.task,
            results,
            dirty=status in {"TRANSLATED", "SOURCE_IDENTIFIED"},
        )


async def _wait_for_visible_dialog(page: Any, heading: str, timeout_ms: int):
    deadline = monotonic() + timeout_ms / 1000
    dialogs = page.locator("[role='dialog']:visible").filter(
        has=page.get_by_role("heading", name=heading, exact=True)
    )
    while monotonic() < deadline:
        if await dialogs.count():
            return dialogs.last
        errors = page.locator(
            ".jx-message--error:visible, .jx-notification:visible"
        )
        if await errors.count():
            message = " ".join((await errors.last.inner_text()).split())
            if message:
                raise ValueError(message)
        await page.wait_for_timeout(500)
    raise TimeoutError(f"Miaoshou dialog did not appear: {heading}")


IMAGE_DIALOG_HEADINGS = ("翻译结果预览", "图片翻译", "批量翻译/处理图片")


def _image_dialog(page: Any, heading: str):
    return page.locator("[role='dialog']:visible").filter(
        has=page.get_by_role("heading", name=heading, exact=True)
    )


async def image_dialog_state(page: Any) -> List[str]:
    """Return the visible image dialogs from top-level state, not screenshots."""
    visible = []
    for heading in IMAGE_DIALOG_HEADINGS:
        if await _image_dialog(page, heading).count():
            visible.append(heading)
    return visible


async def _close_image_dialogs_except(page: Any, keep: set[str]) -> None:
    for heading in IMAGE_DIALOG_HEADINGS:
        if heading in keep:
            continue
        await dismiss_dialog(page, heading)
        if await _image_dialog(page, heading).count():
            raise ValueError(f"Could not close stale image dialog: {heading}")


async def _recover_translation_preview(
    context: HandlerContext,
    region: str,
    image_count: int,
    image_container: Any,
) -> bool:
    """Commit an already-generated preview instead of starting a paid retry."""
    preview = _image_dialog(context.page, "翻译结果预览")
    if await preview.count() == 0:
        return False
    preview = preview.last
    if await preview.locator("img").count() == 0:
        raise ValueError("Existing translation preview has no generated images")
    confirm = preview.get_by_text("确认并保存", exact=True)
    if await confirm.count() != 1:
        raise ValueError("Existing translation preview has no unique save control")
    await confirm.click(force=True)
    await preview.wait_for(
        state="hidden", timeout=context.config.browser.navigation_timeout_ms
    )
    translator = _image_dialog(context.page, "图片翻译")
    if await translator.count():
        await translator.last.wait_for(
            state="hidden", timeout=context.config.browser.navigation_timeout_ms
        )
    result_fingerprint = await _image_fingerprint(image_container)
    _record(
        context,
        region,
        "RECOVERED_TRANSLATION_PREVIEW",
        image_count,
        result_fingerprint=result_fingerprint,
        target_language=image_language_code(context.task.market),
    )
    return True


async def _ensure_selected(region: Any, trigger: Any) -> None:
    button = trigger.locator("xpath=ancestor::button[1]")
    if await trigger.evaluate("element => element.tagName === 'BUTTON'"):
        button = trigger
    if await button.count() and not await button.is_disabled():
        return
    select_all = region.get_by_role("button", name="全选", exact=True)
    if await select_all.count():
        await select_all.first.click()
    if await button.count() and await button.is_disabled():
        raise ValueError("Image translation button is disabled after selecting images")


def _is_ali_translation_provider(text: str) -> bool:
    normalized = "".join(str(text or "").split())
    return normalized.startswith(("阿里翻译", "阿里AI翻译"))


async def _choose_ali_translation_provider(menu: Any) -> bool:
    candidates = menu.locator(
        ".image-translate-panel-item, [role='menuitem'], li, button, div"
    )
    for index in range(await candidates.count() - 1, -1, -1):
        candidate = candidates.nth(index)
        try:
            if (
                _is_ali_translation_provider(await candidate.inner_text())
                and await candidate.is_visible()
            ):
                await candidate.click(force=True)
                return True
        except Exception:
            continue
    return False


async def _open_translation_dialog(page: Any, button: Any, timeout_ms: int):
    deadline = monotonic() + timeout_ms / 1000
    translator = page.locator("[role='dialog']:visible").filter(
        has=page.get_by_role("heading", name="图片翻译", exact=True)
    )
    quick_menu = page.locator(".jx-popper:visible").filter(has_text="选择翻译渠道")
    preview = _image_dialog(page, "翻译结果预览")
    if await preview.count():
        raise ValueError("Translation preview is already visible and must be recovered")
    if await translator.count():
        return translator.last
    if await quick_menu.count() == 0:
        await button.click()
    chose_channel = False
    while monotonic() < deadline:
        if await translator.count():
            return translator.last
        if not chose_channel and await quick_menu.count():
            chose_channel = await _choose_ali_translation_provider(quick_menu.last)
        await page.wait_for_timeout(300)
    raise TimeoutError("Miaoshou image translation settings did not appear")


async def _click_visible_text_control(container: Any, label: str) -> bool:
    """Click Miaoshou's button/card controls across both current UI variants."""
    expected = "".join(label.split())
    candidates = container.locator("*").filter(has_text=label)
    for index in range(await candidates.count() - 1, -1, -1):
        candidate = candidates.nth(index)
        try:
            actual = "".join((await candidate.inner_text()).split())
            if actual == expected and await candidate.is_visible():
                await candidate.click(force=True)
                return True
        except Exception:
            continue
    return False


async def _select_language_control(
    page: Any,
    translator: Any,
    *,
    radio_value: str,
    label: str,
    timeout_ms: int,
) -> None:
    deadline = monotonic() + timeout_ms / 1000
    while monotonic() < deadline:
        radios = translator.locator(
            f"input[type='radio'][value='{radio_value}']"
        )
        for index in range(await radios.count() - 1, -1, -1):
            radio = radios.nth(index)
            if await radio.is_visible():
                await radio.click(force=True)
                return
        if await _click_visible_text_control(translator, label):
            return
        await page.wait_for_timeout(300)
    raise ValueError(f"Image language control is unavailable: {label}")


async def _translate_selected_images(
    context: HandlerContext,
    trigger: Any,
    region: str,
    image_count: int,
    image_container: Any,
) -> None:
    page = context.page
    timeout_ms = context.config.browser.image_translation_timeout_ms
    source_fingerprint = await _image_fingerprint(image_container)
    button = trigger.locator("xpath=ancestor::button[1]")
    if await trigger.evaluate("element => element.tagName === 'BUTTON'"):
        button = trigger
    if await button.count() != 1:
        raise ValueError("Could not resolve the image translation button")
    translator = await _open_translation_dialog(
        page, button, context.config.browser.timeout_ms
    )

    target_code = image_language_code(context.task.market)
    await _select_language_control(
        page,
        translator,
        radio_value="zh",
        label="简体中文",
        timeout_ms=context.config.browser.timeout_ms,
    )
    target_language = str(context.config.markets[context.task.market]["language"])
    await _select_language_control(
        page,
        translator,
        radio_value=target_code,
        label=target_language,
        timeout_ms=context.config.browser.timeout_ms,
    )

    preserve_product_text = translator.locator(
        "input[type='checkbox'][value='textInTheProduct']"
    )
    preserve_text = translator.get_by_text("不翻译商品上的文字", exact=True)
    if await preserve_product_text.count() == 1:
        if await preserve_product_text.is_checked():
            await preserve_text.click()
        if await preserve_product_text.is_checked():
            raise ValueError("Could not disable '不翻译商品上的文字'")
    else:
        label = preserve_text.locator("xpath=ancestor::label[1]")
        if await label.count() != 1:
            raise ValueError("Miaoshou product-text preservation option is unavailable")
        classes = str(await label.get_attribute("class") or "")
        if "is-checked" in classes:
            await preserve_text.click()

    await translator.get_by_role("button", name="开始翻译", exact=True).click()
    preview = await _wait_for_visible_dialog(page, "翻译结果预览", timeout_ms)
    if await preview.locator("img").count() == 0:
        raise ValueError("Image translation preview contains no generated images")
    await preview.get_by_text("确认并保存", exact=True).click()
    await preview.wait_for(
        state="hidden", timeout=context.config.browser.navigation_timeout_ms
    )
    await translator.wait_for(
        state="hidden", timeout=context.config.browser.navigation_timeout_ms
    )
    result_fingerprint = await _image_fingerprint(image_container)
    if result_fingerprint == source_fingerprint:
        # The UI sometimes refreshes the generated URLs just after closing preview.
        deadline = monotonic() + 10
        while monotonic() < deadline and result_fingerprint == source_fingerprint:
            await page.wait_for_timeout(250)
            result_fingerprint = await _image_fingerprint(image_container)
    _record(
        context,
        region,
        "TRANSLATED",
        image_count,
        source_fingerprint=source_fingerprint,
        result_fingerprint=(
            result_fingerprint if result_fingerprint != source_fingerprint else ""
        ),
        target_language=image_language_code(context.task.market),
    )


class DetailImageTranslationHandler(Handler):
    step = Step.TRANSLATE_DETAIL_IMAGES

    async def run(self, context: HandlerContext) -> None:
        try:
            editor = await editor_root(context.page)
            if editor is None:
                raise ValueError("Calibrated Miaoshou editor is unavailable")
            processor = _image_dialog(context.page, "批量翻译/处理图片")
            if await processor.count():
                processor = processor.last
            else:
                await _close_image_dialogs_except(context.page, set())
                entry = editor.get_by_text("批量翻译/处理图片", exact=True)
                if await entry.count() != 1:
                    raise ValueError(
                        "Expected one detail-image processor, "
                        f"found {await entry.count()}"
                    )
                await entry.click()
                processor = await _wait_for_visible_dialog(
                    context.page,
                    "批量翻译/处理图片",
                    context.config.browser.navigation_timeout_ms,
                )
            image_count = await processor.locator(".product-picture-item").count()
            if image_count and await _recover_translation_preview(
                context, "detail", image_count, processor
            ):
                await processor.get_by_role("button", name="保存", exact=True).click()
                await processor.wait_for(
                    state="hidden",
                    timeout=context.config.browser.navigation_timeout_ms,
                )
                return
            translator_open = bool(
                await _image_dialog(context.page, "图片翻译").count()
            )
            if image_count and not translator_open:
                image_count = await _limit_detail_images(
                    context.page,
                    processor,
                    timeout_ms=context.config.browser.navigation_timeout_ms,
                )
            if image_count == 0:
                await processor.get_by_role("button", name="取消", exact=True).click()
                _record(context, "detail", "SKIPPED_NO_IMAGES", 0)
                return
            if await _reuse_saved_translation(
                context, "detail", processor, image_count
            ):
                await processor.get_by_role("button", name="保存", exact=True).click()
                await processor.wait_for(
                    state="hidden",
                    timeout=context.config.browser.navigation_timeout_ms,
                )
                return
            trigger = processor.get_by_text("图片翻译", exact=True)
            if await trigger.count() != 1:
                raise ValueError("Detail-image translation button is unavailable")
            await _ensure_selected(processor, trigger)
            await _translate_selected_images(
                context, trigger, "detail", image_count, processor
            )
            await processor.get_by_role("button", name="保存", exact=True).click()
            await processor.wait_for(
                state="hidden", timeout=context.config.browser.navigation_timeout_ms
            )
        except ExecutorError:
            raise
        except Exception as exc:
            dialogs = await image_dialog_state(context.page)
            raise ExecutorError(
                ErrorCode.IMAGE_TRANSLATE_FAILED,
                "Miaoshou detail-image translation failed: "
                f"{exc}; visible_image_dialogs={dialogs}",
                step=self.step,
            ) from exc


class MainImageTranslationHandler(Handler):
    step = Step.TRANSLATE_MAIN_IMAGES

    async def run(self, context: HandlerContext) -> None:
        try:
            # A processor dialog belongs to the previous detail-image step.
            # Close that stack top-down; otherwise keep an inline translator or
            # preview so this same step can resume without paying for a retry.
            if await _image_dialog(
                context.page, "批量翻译/处理图片"
            ).count():
                await _close_image_dialogs_except(context.page, set())
            editor = await editor_root(context.page)
            if editor is None:
                raise ValueError("Calibrated Miaoshou editor is unavailable")
            region = editor.locator(".product-picture-list")
            if await region.count() != 1:
                raise ValueError(
                    f"Expected one main-image region, found {await region.count()}"
                )
            image_count = await region.locator("img").count()
            if image_count == 0:
                raise ValueError("Product has no main images")
            if await _recover_translation_preview(
                context, "main", image_count, region
            ):
                return
            if await _reuse_saved_translation(context, "main", region, image_count):
                return
            trigger = region.get_by_text("图片翻译", exact=True)
            if await trigger.count() != 1:
                raise ValueError("Main-image translation button is unavailable")
            await _ensure_selected(region, trigger)
            await _translate_selected_images(
                context, trigger, "main", image_count, region
            )
        except ExecutorError:
            raise
        except Exception as exc:
            dialogs = await image_dialog_state(context.page)
            raise ExecutorError(
                ErrorCode.IMAGE_TRANSLATE_FAILED,
                "Miaoshou main-image translation failed: "
                f"{exc}; visible_image_dialogs={dialogs}",
                step=self.step,
            ) from exc


class SizeChartTranslationHandler(Handler):
    step = Step.TRANSLATE_SIZE_CHART

    async def run(self, context: HandlerContext) -> None:
        if context.task.category_group != "CLOTHING":
            _record(context, "size_chart", "SKIPPED_NOT_CLOTHING", 0)
            return
        try:
            if await _image_dialog(
                context.page, "批量翻译/处理图片"
            ).count():
                await _close_image_dialogs_except(context.page, set())
            editor = await editor_root(context.page)
            if editor is None:
                raise ValueError("Calibrated Miaoshou editor is unavailable")
            needs_translation = await self._ensure_verified_size_chart(context, editor)
            region = editor.locator(".size-chart-box")
            image_count = await region.locator("img").count()
            if image_count == 0:
                _record(context, "size_chart", "MISSING_VERIFIED_SOURCE", 0)
                return
            if await _recover_translation_preview(
                context, "size_chart", image_count, region
            ):
                return
            if not needs_translation:
                _record(
                    context,
                    "size_chart",
                    "REUSED_TRANSLATED_DETAIL",
                    image_count,
                )
                return
            if await _reuse_saved_translation(
                context, "size_chart", region, image_count
            ):
                return
            trigger = region.get_by_text("图片翻译", exact=True)
            if await trigger.count() != 1:
                raise ValueError("Size-chart image translation button is unavailable")
            await _ensure_selected(region, trigger)
            await _translate_selected_images(
                context, trigger, "size_chart", image_count, region
            )
        except ExecutorError:
            raise
        except Exception as exc:
            dialogs = await image_dialog_state(context.page)
            raise ExecutorError(
                ErrorCode.IMAGE_TRANSLATE_FAILED,
                "Miaoshou size-chart translation failed: "
                f"{exc}; visible_image_dialogs={dialogs}",
                step=self.step,
            ) from exc

    async def _ensure_verified_size_chart(
        self, context: HandlerContext, editor: Any
    ) -> bool:
        box = editor.locator(".size-chart-box")
        if await box.count() != 1:
            raise ValueError("Miaoshou size-chart box is unavailable")
        if await box.locator("img").count():
            return True
        sources = context.config.feishu.get("verified_size_chart_sources", {})
        manual_path = str(context.task.size_chart_path or "").strip()
        source_url = str(context.task.size_chart_url or "").strip()
        if manual_path or source_url:
            # The Feishu attachment is an operator-approved, market-localized
            # final chart. Upload it as-is; retranslating Thai/Vietnamese/Malay
            # text can corrupt labels and ranges.
            source_already_translated = bool(manual_path)
            _record(
                context,
                "size_chart",
                "SOURCE_IDENTIFIED",
                1,
                source="feishu_attachment",
            )
        else:
            source_url = str(sources.get(context.task.miaoshou_product_id, ""))
        if not manual_path and source_url:
            source_already_translated = False
            if not context.task.size_chart_url:
                _record(
                    context,
                    "size_chart",
                    "SOURCE_IDENTIFIED",
                    1,
                    source="configured",
                )
        elif not manual_path:
            try:
                source_url, selection = await self._discover_size_chart_source(
                    context, editor
                )
            except SizeChartDetectionError as exc:
                try:
                    capture = await SourceSizeChartExtractor().extract(
                        context.page,
                        context.task.source_url,
                        context.task.miaoshou_product_id,
                        context.config.browser.navigation_timeout_ms,
                    )
                except SizeChartDetectionError as source_exc:
                    raise ExecutorError(
                        ErrorCode.SIZE_CHART_REQUIRED,
                        "No unique trustworthy size chart was identified in "
                        f"detail images ({exc}) or the 1688 structured spec table "
                        f"({source_exc})",
                        step=self.step,
                    ) from source_exc
                manual_path = capture.path
                source_url = ""
                source_already_translated = False
                _record(
                    context,
                    "size_chart",
                    "SOURCE_IDENTIFIED",
                    1,
                    source="1688_structured_spec_table",
                    cached=capture.cached,
                    evidence=capture.evidence,
                )
            except Exception as exc:
                raise ExecutorError(
                    ErrorCode.SIZE_CHART_DETECTION_FAILED,
                    f"Size-chart visual detection failed: {exc}",
                    step=self.step,
                ) from exc
            else:
                _record(
                    context,
                    "size_chart",
                    "SOURCE_IDENTIFIED",
                    1,
                    source="detail_image",
                    source_index=selection.index,
                    confidence=selection.confidence,
                    evidence=selection.evidence,
                )
                detail_result = getattr(context, "image_translation", {}).get(
                    "detail", {}
                )
                source_already_translated = (
                    detail_result.get("status")
                    in {"TRANSLATED", "REUSED_SAVED_TRANSLATION"}
                )
        temporary_path = ""
        delete_temporary = False
        try:
            if manual_path:
                if not os.path.isfile(manual_path):
                    raise ValueError(f"Manual size-chart file does not exist: {manual_path}")
                temporary_path = manual_path
                try:
                    snapshots = await all_sku_snapshots(context.page)
                    await asyncio.to_thread(
                        SizeChartCoverageValidator().validate,
                        manual_path,
                        [str(item.get("label") or "") for item in snapshots],
                    )
                except SizeChartCoverageError as exc:
                    raise ExecutorError(
                        ErrorCode.SIZE_CHART_REQUIRED,
                        str(exc),
                        step=self.step,
                    ) from exc
                except ExecutorError:
                    raise
                except Exception as exc:
                    raise ExecutorError(
                        ErrorCode.SIZE_CHART_DETECTION_FAILED,
                        f"尺码图与 SKU 覆盖核对失败：{exc}",
                        step=self.step,
                    ) from exc
            else:
                response = await context.page.request.get(source_url)
                if not response.ok:
                    raise ValueError(
                        f"Verified size-chart download returned HTTP {response.status}"
                    )
                with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as handle:
                    handle.write(await response.body())
                    temporary_path = handle.name
                    delete_temporary = True
            await box.locator(".product-picture-item-add").click()
            upload = context.page.locator("input.jx-upload__input[accept*='image']")
            if await upload.count() == 0:
                raise ValueError("Size-chart image upload input was not created")
            await upload.last.set_input_files(temporary_path)
            deadline = (
                monotonic()
                + context.config.browser.navigation_timeout_ms / 1000
            )
            while monotonic() < deadline:
                images = box.locator("img")
                if await images.count():
                    source = str(await images.first.get_attribute("src") or "")
                    if source.startswith("http"):
                        return not source_already_translated
                await context.page.wait_for_timeout(500)
            raise ValueError("Verified size chart did not finish uploading")
        finally:
            if delete_temporary and temporary_path and os.path.exists(temporary_path):
                os.unlink(temporary_path)

    async def _discover_size_chart_source(
        self, context: HandlerContext, editor: Any
    ):
        entry = editor.get_by_text("批量翻译/处理图片", exact=True)
        if await entry.count() != 1:
            raise SizeChartDetectionError("Detail-image processor is unavailable")
        await entry.click()
        processor = await _wait_for_visible_dialog(
            context.page,
            "批量翻译/处理图片",
            context.config.browser.navigation_timeout_ms,
        )
        urls: List[str] = []
        try:
            images = processor.locator("img")
            for index in range(await images.count()):
                source = str(await images.nth(index).get_attribute("src") or "")
                if source.startswith("http") and source not in urls:
                    urls.append(source)
        finally:
            cancel = processor.get_by_role("button", name="取消", exact=True)
            if await cancel.count():
                await cancel.click()
                await processor.wait_for(
                    state="hidden",
                    timeout=context.config.browser.navigation_timeout_ms,
                )
        if not urls:
            raise SizeChartDetectionError("No HTTP detail-image URLs were found")

        with tempfile.TemporaryDirectory(prefix="miaoshou-size-chart-") as directory:
            paths = []
            for index, url in enumerate(urls):
                response = await context.page.request.get(url)
                if not response.ok:
                    raise SizeChartDetectionError(
                        f"Detail image {index} returned HTTP {response.status}"
                    )
                path = os.path.join(directory, f"detail-{index}.jpg")
                with open(path, "wb") as handle:
                    handle.write(await response.body())
                paths.append(path)
            detector = SizeChartDetector()
            selection = await asyncio.to_thread(detector.detect, paths)
        return urls[selection.index], selection
