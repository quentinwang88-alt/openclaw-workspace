from __future__ import annotations

import re
from decimal import Decimal
from typing import Any, Dict, Optional


THAI_START = "\u0e00"
THAI_END = "\u0e7f"


def grams_to_kg_text(weight_g: Any) -> str:
    value = Decimal(str(weight_g)) / Decimal("1000")
    return format(value.normalize(), "f")


def contains_thai(value: str) -> bool:
    return any(THAI_START <= character <= THAI_END for character in value)


async def editor_root(page: Any) -> Optional[Any]:
    """Return the real Miaoshou editor overlay when the calibrated DOM is present."""
    try:
        root = page.locator("[role='dialog']:visible").filter(has_text="保存并发布")
        if await root.count() == 1:
            return root.first
    except Exception:
        pass
    return None


async def real_sku_rows(page: Any) -> Optional[Any]:
    root = await editor_root(page)
    if root is None:
        return None
    rows = root.locator(
        ".pro-virtual-table__row-body:has(.price-currency input):has(.package-weight-input input)"
    )
    if await rows.count() == 0:
        return None
    return rows


async def row_label(row: Any, index: int) -> str:
    text = " ".join((await row.inner_text()).split())
    return text[:120] or f"SKU#{index + 1}"


async def form_item(root: Any, label: str) -> Optional[Any]:
    try:
        labels = root.get_by_text(label, exact=True)
        if await labels.count() == 0:
            labels = root.get_by_text(label, exact=False)
        if await labels.count() == 0:
            return None
        item = labels.first.locator(
            "xpath=ancestor::*[contains(@class,'jx-form-item')][1]"
        )
        if await item.count():
            return item
    except Exception:
        pass
    return None


async def first_editable_text_input(root: Any) -> Optional[Any]:
    candidates = root.locator(
        "input[type='text']:not([readonly]):not([disabled]):not([role='combobox'])"
    )
    for index in range(await candidates.count()):
        candidate = candidates.nth(index)
        if await candidate.is_visible():
            return candidate
    return None


async def title_value(editor: Any) -> str:
    candidates = editor.locator(
        "input[type='text'].jx-input__inner:not([readonly]):not([disabled]):"
        "not([role='combobox'])"
    )
    for index in range(await candidates.count()):
        candidate = candidates.nth(index)
        if not await candidate.is_visible():
            continue
        for levels in range(1, 6):
            ancestor = candidate.locator("xpath=" + "/".join([".."] * levels))
            text = await ancestor.inner_text()
            if re.search(r"/\s*255\b", text):
                return (await candidate.input_value()).strip()
    item = await form_item(editor, "产品标题")
    if item is None:
        return ""
    field = await first_editable_text_input(item)
    return (await field.input_value()).strip() if field is not None else ""


async def logistics_inputs(editor: Any) -> list[Any]:
    panes = editor.locator(".scroll-menu-pane").filter(has_text="包裹重量").filter(
        has_text="包裹尺寸"
    )
    if await panes.count() == 0:
        return []
    fields = panes.first.locator(
        "input.jx-input__inner:not([readonly]):not([disabled]):not([role='combobox'])"
    )
    return [fields.nth(index) for index in range(await fields.count())]


async def nearest_editable_input(origin: Any, max_levels: int = 7) -> Optional[Any]:
    """Find the stock input in the same warehouse row without positional selectors."""
    for levels in range(1, max_levels + 1):
        ancestor = origin.locator("xpath=" + "/".join([".."] * levels))
        candidates = ancestor.locator(
            "input[type='text']:not([readonly]):not([disabled]):not([role='combobox'])"
        )
        visible = []
        for index in range(await candidates.count()):
            candidate = candidates.nth(index)
            if await candidate.is_visible():
                visible.append(candidate)
        if visible:
            return visible[-1]
    return None


async def choose_radio(dialog: Any, value: str) -> None:
    radio = dialog.locator(f"input[type='radio'][value='{value}']")
    if await radio.count() != 1:
        raise ValueError(f"Expected one radio with value {value!r}")
    if await radio.first.is_checked():
        return
    label = radio.first.locator("xpath=ancestor::label[1]")
    if await label.count():
        await label.click()
    else:
        await radio.first.check(force=True)
    if not await radio.first.is_checked():
        raise ValueError(f"Radio {value!r} did not become checked")


async def sku_header(editor: Any, label: str) -> Any:
    headers = editor.locator(".pro-virtual-table__header-cell").filter(
        has_text=label
    ).filter(has_text="批量")
    if await headers.count() != 1:
        raise ValueError(
            f"Expected one SKU header for {label!r}, found {await headers.count()}"
        )
    return headers.first


async def dismiss_dialog(page: Any, heading: str) -> None:
    dialogs = page.locator("[role='dialog']:visible").filter(
        has=page.get_by_role("heading", name=heading, exact=True)
    )
    while await dialogs.count():
        dialog = dialogs.last
        cancel = dialog.get_by_role("button", name="取消", exact=True)
        if await cancel.count():
            await cancel.last.click(force=True)
        else:
            close = dialog.locator(
                ".jx-dialog__headerbtn, button[aria-label='Close']"
            )
            if await close.count():
                await close.last.click(force=True)
            else:
                await page.keyboard.press("Escape")
        try:
            await dialog.wait_for(state="hidden", timeout=3_000)
        except Exception:
            break


async def atomic_row_input_values(rows: Any, selector: str) -> list[str]:
    """Read one field per SKU in one browser evaluation.

    Miaoshou recycles virtual-row DOM nodes while Playwright scrolls for a
    click. A single evaluate_all does not scroll and therefore produces a
    consistent snapshot for verification.
    """
    fields = rows.locator(selector)
    return await fields.evaluate_all(
        "elements => elements.map(element => String(element.value || '').trim())"
    )


async def sku_total_count(editor: Any) -> int:
    label = editor.get_by_text("SKU列表", exact=True)
    if await label.count() != 1:
        raise ValueError("SKU list label was not unique")
    count_text = await label.first.locator("xpath=parent::*").inner_text()
    match = re.search(r"[，,]\s*(\d+)\s*个", count_text)
    if not match:
        raise ValueError(f"Could not parse total SKU count from {count_text!r}")
    return int(match.group(1))


async def all_sku_snapshots(page: Any) -> list[Dict[str, str]]:
    """Scroll Miaoshou's recycled table and return every SKU exactly once."""
    editor = await editor_root(page)
    rows = await real_sku_rows(page)
    if editor is None or rows is None:
        raise ValueError("Miaoshou SKU table is unavailable")
    total = await sku_total_count(editor)
    scroller = rows.first.locator(
        "xpath=ancestor::*[contains(concat(' ',normalize-space(@class),' '),"
        "' vue-recycle-scroller ')][1]"
    )
    if await scroller.count() != 1:
        raise ValueError("SKU virtual scroller was not unique")
    metrics = await scroller.evaluate(
        "element => ({top: element.scrollTop, height: element.clientHeight, "
        "max: Math.max(0, element.scrollHeight - element.clientHeight)})"
    )
    step = max(64, int(metrics["height"] * 0.7))
    positions = list(range(0, int(metrics["max"]) + 1, step))
    if not positions or positions[-1] != int(metrics["max"]):
        positions.append(int(metrics["max"]))
    snapshots: Dict[str, Dict[str, str]] = {}
    script = """
    elements => elements.map(row => {
      const cells = [...row.querySelectorAll('.pro-virtual-table__row-cell')];
      const specs = [];
      for (const cell of cells) {
        if (cell.querySelector('.currency-input input')) break;
        if (cell.classList.contains('is-selection-column')) continue;
        const text = String(cell.innerText || '').trim().replace(/\s+/g, ' ');
        if (text) specs.push(text);
      }
      const value = selector => String(row.querySelector(selector)?.value || '').trim();
      return {
        key: specs.join(' / '),
        label: specs.join(' / '),
        price: value('.price-currency input'),
        purchase_price: value('.currency-input input'),
        stock: value('input.jx-input__inner[readonly]'),
        weight: value('.package-weight-input input'),
        platform_price: String(row.querySelector('.pro-readonly-component')?.innerText || '').trim()
      };
    })
    """
    try:
        for position in positions:
            await scroller.evaluate(
                "(element, top) => { element.scrollTop = top; "
                "element.dispatchEvent(new Event('scroll')); }",
                position,
            )
            await page.wait_for_timeout(100)
            current = await real_sku_rows(page)
            if current is None:
                raise ValueError("SKU rows disappeared while scrolling")
            for snapshot in await current.evaluate_all(script):
                key = str(snapshot.get("key", "")).strip()
                if key:
                    snapshots[key] = snapshot
    finally:
        await scroller.evaluate(
            "(element, top) => { element.scrollTop = top; "
            "element.dispatchEvent(new Event('scroll')); }",
            int(metrics["top"]),
        )
        await page.wait_for_timeout(100)
    if len(snapshots) != total:
        raise ValueError(
            f"Virtual SKU scan collected {len(snapshots)} unique rows, expected {total}"
        )
    return list(snapshots.values())
