from __future__ import annotations

from time import monotonic
from typing import Any

from .editor_dom import editor_root


async def matching_editor_session(
    page: Any, product_id: str, shop_name: str
) -> bool:
    """Require both business identifiers before trusting an unsaved live editor.

    Miaoshou renders the source ID in the selected virtual-table row behind the
    editor drawer, while the target shop is rendered inside the drawer. Neither
    identifier alone is sufficient to authorize a resumed publish.
    """
    try:
        editor = await editor_root(page)
        if editor is None:
            return False
        text = " ".join((await editor.inner_text()).split())
        sources = page.locator(".source-id-box:visible")
        matching_sources = 0
        for index in range(await sources.count()):
            source_text = " ".join((await sources.nth(index).inner_text()).split())
            if product_id in source_text:
                matching_sources += 1
        return matching_sources == 1 and shop_name in text
    except Exception:
        return False


async def _close_dialog(page: Any, heading: str, action: str) -> None:
    dialogs = page.locator("[role='dialog']:visible").filter(
        has=page.get_by_role("heading", name=heading, exact=True)
    )
    while await dialogs.count():
        dialog = dialogs.last
        button = dialog.get_by_role("button", name=action, exact=True)
        if await button.count() == 0:
            button = dialog.get_by_text(action, exact=True)
        if await button.count() == 0:
            close_icon = dialog.locator(".jx-dialog__headerbtn:visible")
            if await close_icon.count() == 0:
                return
            await close_icon.last.click(force=True)
        else:
            await button.last.click(force=True)
        try:
            await dialog.wait_for(state="hidden", timeout=10_000)
        except Exception:
            return


async def save_editor_draft(page: Any, timeout_ms: int) -> bool:
    """Best-effort non-publishing persistence after an editor-step failure."""
    editor = await editor_root(page)
    if editor is None:
        return False

    # Preserve completed image work while unwinding any failed nested dialog.
    await _close_dialog(page, "翻译结果预览", "取消")
    await _close_dialog(page, "图片翻译", "关闭")
    await _close_dialog(page, "批量翻译/处理图片", "保存")

    editor = await editor_root(page)
    if editor is None:
        return False
    save = editor.get_by_role("button", name="保存修改", exact=True)
    if await save.count() != 1 or await save.is_disabled():
        return False
    await save.click()
    deadline = monotonic() + timeout_ms / 1000
    notices = page.locator(
        ".jx-message:visible, .jx-notification:visible, .jx-message-box:visible"
    )
    while monotonic() < deadline:
        if not await editor.is_visible():
            return True
        for index in range(await notices.count()):
            text = " ".join((await notices.nth(index).inner_text()).split())
            if any(token in text for token in ("有误", "失败", "错误")):
                return False
            if any(token in text for token in ("保存成功", "修改成功")):
                return True
        await page.wait_for_timeout(250)
    return False
