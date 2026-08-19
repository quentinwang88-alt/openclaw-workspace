from __future__ import annotations

import re
from time import monotonic
from urllib.parse import urljoin, urlparse

from ..browser.helpers import element_value, wait_for_any_key, wait_for_key
from ..errors import ExecutorError
from ..models import ErrorCode, Step
from .base import Handler, HandlerContext
from .collection import DuplicateCheckHandler, _apply_source_filter, wait_for_history_rows
from .editor_dom import editor_root
from .preflight import PreflightHandler


def published_product_id(record_text: str) -> str:
    if "发布成功" not in record_text:
        return ""
    match = re.search(r"产品ID\s*:\s*(\d{6,})", record_text)
    return match.group(1) if match else ""


def online_product_id(record_text: str) -> str:
    match = re.search(r"产品ID\s*[:：]\s*(\d{6,})", record_text)
    return match.group(1) if match else ""


def validate_publish_api_receipt(
    endpoint: str, response_url: str, status: int, payload
) -> str:
    """Return a compact receipt or fail closed on a rejected publish API."""
    path = urlparse(response_url).path
    if endpoint not in path:
        raise ValueError(f"Expected {endpoint} response, got {path}")
    if status < 200 or status >= 300:
        raise ValueError(f"{endpoint} returned HTTP {status}")
    if isinstance(payload, dict) and payload.get("code") not in (None, 0, "0"):
        raise ValueError(
            f"{endpoint} returned code={payload.get('code')}: "
            f"{payload.get('msg') or payload.get('message') or ''}"
        )
    return f"POST {path} HTTP {status}"


class PublishHandler(Handler):
    step = Step.PUBLISH

    async def run(self, context: HandlerContext) -> None:
        if getattr(context, "publish_submitted", False):
            return
        if not getattr(context, "duplicate_checked", False):
            await DuplicateCheckHandler().run(context)
        if not getattr(context, "preflight_snapshot", None):
            await PreflightHandler().run(context)

        editor = await editor_root(context.page)
        if editor is not None:
            await self._publish_real_editor(context, editor)
            return
        await self._publish_registry_fallback(context)

    async def _publish_real_editor(self, context: HandlerContext, editor) -> None:
        shop = context.config.shops[context.task.target_shop]
        shop_name = str(shop["display_name"])
        shop_id = str(shop.get("shop_id", ""))
        country_name = str(context.config.markets[context.task.market]["country_name"])
        try:
            await editor.get_by_role(
                "button", name="保存并发布", exact=True
            ).click()
            dialog = context.page.locator("[role='dialog']:visible").filter(
                has_text="发布产品"
            ).filter(has_text="确认发布")
            await dialog.last.wait_for(timeout=context.config.browser.timeout_ms)
            dialog = dialog.last
            dialog_text = await dialog.inner_text()
            if shop_name not in dialog_text:
                raise ValueError(f"Publish dialog does not contain shop {shop_name!r}")
            if f"{country_name}站" not in dialog_text:
                raise ValueError(
                    f"Publish dialog does not contain market {country_name!r}"
                )
            if shop_id:
                checkbox = dialog.locator(
                    f"input[type='checkbox'][value='{shop_id}']"
                )
                if await checkbox.count() != 1:
                    raise ValueError(
                        f"Target shop checkbox {shop_id!r} was not unique"
                    )
                if not await checkbox.first.is_checked():
                    label = checkbox.first.locator("xpath=ancestor::label[1]")
                    if await label.count() != 1:
                        raise ValueError(
                            f"Target shop checkbox {shop_id!r} has no visible label"
                        )
                    await label.click()
                    await context.page.wait_for_timeout(200)
                if not await checkbox.first.is_checked():
                    raise ValueError(
                        f"Target shop checkbox {shop_id!r} could not be selected"
                    )
                checked = dialog.locator("input[type='checkbox']:checked")
                extra_values = []
                for index in range(await checked.count()):
                    value = str(await checked.nth(index).get_attribute("value") or "")
                    if value and value != shop_id:
                        extra_values.append(value)
                if extra_values:
                    raise ValueError(
                        f"Unexpected additional shop IDs are selected: {extra_values}"
                    )
            immediate = dialog.locator("input[type='radio'][value='0']")
            if await immediate.count() and not await immediate.first.is_checked():
                loading = context.page.locator(
                    ".jx-loading-mask.is-fullscreen:visible"
                )
                if await loading.count():
                    await loading.first.wait_for(
                        state="hidden",
                        timeout=context.config.browser.navigation_timeout_ms,
                    )
                label = immediate.first.locator("xpath=ancestor::label[1]")
                if await label.count():
                    await label.click()
                else:
                    await immediate.first.check(force=True)
                if not await immediate.first.is_checked():
                    raise ValueError("Immediate publish radio did not become selected")

            confirm = dialog.get_by_role("button", name="确认发布", exact=True)
            if await confirm.count() != 1:
                raise ValueError(
                    f"Expected one confirm-publish button, found {await confirm.count()}"
                )
            setting_response_info = context.page.expect_response(
                lambda response: (
                    response.request.method.upper() == "POST"
                    and "saveSettingList" in response.url
                ),
                timeout=context.config.browser.navigation_timeout_ms,
            )
            task_response_info = context.page.expect_response(
                lambda response: (
                    response.request.method.upper() == "POST"
                    and "saveMoveCollectTask" in response.url
                ),
                timeout=context.config.browser.navigation_timeout_ms,
            )
            async with setting_response_info as setting_pending, task_response_info as task_pending:
                await confirm.click()
                self._mark_submitted(
                    context,
                    "确认发布已点击，等待发布接口回执",
                )
            setting_response = await setting_pending.value
            task_response = await task_pending.value
            try:
                setting_payload = await setting_response.json()
            except Exception:
                setting_payload = {}
            try:
                task_payload = await task_response.json()
            except Exception:
                task_payload = {}
            receipts = [
                validate_publish_api_receipt(
                    "saveSettingList",
                    setting_response.url,
                    int(setting_response.status),
                    setting_payload,
                ),
                validate_publish_api_receipt(
                    "saveMoveCollectTask",
                    task_response.url,
                    int(task_response.status),
                    task_payload,
                ),
            ]
            success = context.page.locator("[role='dialog']:visible").filter(
                has_text="产品发布成功"
            )
            await success.last.wait_for(
                timeout=context.config.browser.navigation_timeout_ms
            )
            self._mark_submitted(
                context,
                self._submission_signal("产品发布成功", receipts),
            )
        except ExecutorError:
            raise
        except Exception as exc:
            raise ExecutorError(
                ErrorCode.PUBLISH_FAILED,
                f"Failed while submitting calibrated publish dialog: {exc}",
                step=self.step,
            ) from exc

    async def _publish_registry_fallback(self, context: HandlerContext) -> None:
        country_name = str(context.config.markets[context.task.market]["country_name"])
        try:
            if await context.selectors.exists(context.page, "published_status"):
                return
            if await context.selectors.exists(context.page, "publish_dialog"):
                dialog = await context.selectors.resolve(context.page, "publish_dialog")
            else:
                button = await context.selectors.resolve(context.page, "publish_button")
                await button.click()
                dialog = await wait_for_key(
                    context.page,
                    context.selectors,
                    "publish_dialog",
                    timeout_ms=context.config.browser.timeout_ms,
                )
            market = await context.selectors.resolve(
                dialog, "market_option", country_name=country_name
            )
            try:
                selected = await market.is_checked()
            except Exception:
                selected = False
            if not selected:
                await market.click()
            confirm = await context.selectors.resolve(dialog, "publish_confirm_button")
            responses, listener = self._start_response_capture(context)
            try:
                await confirm.click()
                self._mark_submitted(
                    context,
                    self._submission_signal("确认发布已点击", responses),
                )
                signal, locator = await wait_for_any_key(
                    context.page,
                    context.selectors,
                    ("publish_success", "error_toast"),
                    timeout_ms=context.config.browser.navigation_timeout_ms,
                )
            finally:
                self._stop_response_capture(context, listener)
            if signal == "error_toast":
                message = (await locator.text_content() or "publish failed").strip()
                raise ExecutorError(ErrorCode.PUBLISH_FAILED, message, step=self.step)
            self._mark_submitted(
                context, self._submission_signal("publish_success", responses)
            )
        except ExecutorError:
            raise
        except Exception as exc:
            raise ExecutorError(
                ErrorCode.PUBLISH_FAILED,
                f"Failed while submitting publish dialog: {exc}",
                step=self.step,
            ) from exc

    @staticmethod
    def _mark_submitted(context: HandlerContext, signal: str) -> None:
        # Set the in-memory guard before touching disk because the publish click
        # is irreversible even if local receipt persistence unexpectedly fails.
        setattr(context, "publish_submitted", True)
        store = getattr(context, "submission_store", None)
        if store is None:
            return
        snapshot = getattr(context, "preflight_snapshot", {}) or {}
        store.record_submitted(
            context.task,
            approval_fingerprint=getattr(context, "approval_fingerprint", ""),
            product_title=str(snapshot.get("title", "")),
            submit_page_url=str(getattr(context.page, "url", "")),
            submission_signal=signal,
        )

    @staticmethod
    def _start_response_capture(context: HandlerContext):
        responses = []
        expected_host = urlparse(context.config.browser.base_url).netloc

        def listener(response) -> None:
            try:
                parsed = urlparse(response.url)
                if (
                    response.request.method.upper() == "POST"
                    and parsed.netloc == expected_host
                ):
                    responses.append(
                        f"POST {parsed.path} HTTP {int(response.status)}"
                    )
            except Exception:
                return

        context.page.on("response", listener)
        return responses, listener

    @staticmethod
    def _stop_response_capture(context: HandlerContext, listener) -> None:
        try:
            context.page.remove_listener("response", listener)
        except Exception:
            pass

    @staticmethod
    def _submission_signal(success_signal: str, responses) -> str:
        network = " | ".join(responses[-8:])
        return f"{success_signal} | {network}" if network else success_signal


class VerifyPublishHandler(Handler):
    step = Step.VERIFY

    async def run(self, context: HandlerContext) -> None:
        if "91miaoshou.com" in context.config.browser.base_url:
            await self._verify_history(context)
            return
        # Backward-compatible path for synthetic tests or legacy pages.
        await self._verify_registry_fallback(context)

    async def _verify_history(self, context: HandlerContext) -> None:
        base = context.config.browser.base_url.rstrip("/") + "/"
        history_url = urljoin(
            base, context.config.browser.publish_history_path.lstrip("/")
        )
        shop_name = str(
            context.config.shops[context.task.target_shop]["display_name"]
        )
        try:
            deadline = (
                monotonic()
                + context.config.browser.publish_verify_timeout_ms / 1000
            )
            poll_intervals = list(
                context.config.browser.publish_verify_poll_intervals_seconds
            ) or [2]
            poll_attempt = 0
            while monotonic() < deadline:
                for status in ("success", "fail", "processing", "cancel"):
                    status_url = re.sub(
                        r"status=[^&]+", f"status={status}", history_url
                    )
                    await context.page.goto(
                        status_url, wait_until="domcontentloaded"
                    )
                    await context.page.wait_for_timeout(500)
                    await _apply_source_filter(
                        context.page, context.task.miaoshou_product_id
                    )
                    rows = context.page.locator("tbody tr.jx-table__row").filter(
                        has_text=context.task.miaoshou_product_id
                    ).filter(has_text=shop_name)
                    if not await rows.count():
                        continue
                    text = await rows.first.inner_text()
                    if status in {"fail", "cancel"} or any(
                        token in text for token in ("发布失败", "已取消")
                    ):
                        raise ExecutorError(
                            ErrorCode.PUBLISH_FAILED,
                            f"Miaoshou publish record failed: {' '.join(text.split())}",
                            step=self.step,
                        )
                    if status == "success" or "发布成功" in text:
                        product_id = published_product_id(text)
                        if not product_id:
                            raise ValueError(
                                "Publish record is successful but has no platform product ID"
                            )
                        setattr(context, "published_status", "发布成功")
                        setattr(context, "platform_product_id", product_id)
                        return
                if await self._verify_store_products(context, shop_name):
                    return
                delay = float(
                    poll_intervals[min(poll_attempt, len(poll_intervals) - 1)]
                )
                poll_attempt += 1
                remaining = max(0.0, deadline - monotonic())
                if remaining:
                    await context.page.wait_for_timeout(
                        int(min(delay, remaining) * 1000)
                    )
            raise TimeoutError(
                f"Publish history did not finish within "
                f"{context.config.browser.publish_verify_timeout_ms}ms"
            )
        except ExecutorError:
            raise
        except Exception as exc:
            raise ExecutorError(
                ErrorCode.PUBLISH_VALIDATION_FAILED,
                f"Publish history could not be verified: {exc}",
                step=self.step,
            ) from exc

    async def _verify_store_products(
        self, context: HandlerContext, shop_name: str
    ) -> bool:
        """Cross-check the online-product index without touching an editor."""
        base = context.config.browser.base_url.rstrip("/") + "/"
        await context.page.goto(
            urljoin(base, "tiktok/item/item"), wait_until="domcontentloaded"
        )
        await context.page.wait_for_timeout(600)
        # Store-product filters persist across sessions. Clear stale shop/name
        # filters before applying the source ID, otherwise a valid product in
        # another shop can be hidden for the entire verification window.
        reset = context.page.get_by_role("button", name="重置", exact=True)
        if await reset.count():
            await reset.first.click()
            await context.page.wait_for_timeout(500)
        source_label = context.page.get_by_text("货源ID", exact=True)
        if not await source_label.count():
            return False
        source_form = source_label.first.locator(
            "xpath=ancestor::*[contains(@class,'el-form-item')][1]"
        )
        source_input = source_form.locator("input:visible:not([disabled])")
        if not await source_input.count():
            return False
        await source_input.first.fill(context.task.miaoshou_product_id)
        search = context.page.locator("button:visible").filter(has_text="搜索")
        if not await search.count():
            return False
        await search.first.click()
        await context.page.wait_for_timeout(800)
        statuses = (
            ("onsale", "在售中", True),
            ("forsale", "已下架", True),
            ("suspendSales", "暂停销售", True),
            ("draft", "草稿", False),
            ("deleted", "已删除", False),
        )
        for value, label, is_created_product in statuses:
            tab = context.page.locator(
                f"label:has(input[type='radio'][value='{value}'])"
            )
            if not await tab.count():
                continue
            await tab.first.click()
            await context.page.wait_for_timeout(500)
            rows = context.page.locator(
                "tbody tr.el-table__row, tbody tr.jx-table__row, "
                ".pro-virtual-table__row"
            ).filter(has_text=context.task.miaoshou_product_id).filter(
                has_text=shop_name
            )
            if not await rows.count():
                continue
            text = await rows.first.inner_text()
            product_id = online_product_id(text)
            if is_created_product and product_id:
                setattr(context, "published_status", label)
                setattr(context, "platform_product_id", product_id)
                return True
            if value == "deleted":
                raise ExecutorError(
                    ErrorCode.PUBLISH_FAILED,
                    f"Submitted product was found in deleted products: "
                    f"{' '.join(text.split())}",
                    step=self.step,
                )
        return False

    async def _verify_registry_fallback(self, context: HandlerContext) -> None:
        try:
            status = await wait_for_key(
                context.page,
                context.selectors,
                "published_status",
                timeout_ms=context.config.browser.navigation_timeout_ms,
            )
            product_id_element = await wait_for_key(
                context.page,
                context.selectors,
                "platform_product_id",
                timeout_ms=context.config.browser.navigation_timeout_ms,
            )
            status_text = await element_value(status)
            id_text = await element_value(product_id_element)
            match = re.search(r"\d{6,}", id_text)
            if not any(token in status_text for token in ("已发布", "发布成功")) or not match:
                raise ValueError(
                    f"Expected published status + platform ID, got "
                    f"{status_text!r} / {id_text!r}"
                )
            setattr(context, "published_status", status_text)
            setattr(context, "platform_product_id", match.group(0))
        except Exception as exc:
            if isinstance(exc, ExecutorError) and exc.code != ErrorCode.ELEMENT_TIMEOUT:
                raise
            raise ExecutorError(
                ErrorCode.PUBLISH_VALIDATION_FAILED,
                f"Publish result could not be verified: {exc}",
                step=self.step,
            ) from exc
