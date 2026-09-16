from __future__ import annotations

import re
from decimal import Decimal
from typing import Any, Dict, Optional


THAI_START = "\u0e00"
THAI_END = "\u0e7f"
DESCRIPTION_CHAR_LIMIT = 10_000


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


def description_counter_value(
    text: str, limit: int = DESCRIPTION_CHAR_LIMIT
) -> Optional[int]:
    # Trailing \b never matches before a CJK character, and Miaoshou's 2026-09
    # editor renders the counter as "531 /10000图片：…" with the next label
    # glued to the limit, so only guard against more digits.
    matches = re.findall(rf"(\d+)\s*/\s*{limit}(?!\d)", str(text or ""))
    return int(matches[-1]) if matches else None


def description_text_cutoff(text: str, target_length: int) -> int:
    """Return a safe trailing-text cutoff without splitting a word/sentence."""
    value = str(text or "")
    if len(value) <= target_length:
        return len(value)
    if target_length <= 0:
        return 0
    floor = max(0, target_length - 200)
    window = value[floor:target_length]
    for pattern in (r"[。！？.!?]\s*", r"[；;]\s*", r"\n+", r"\s+"):
        boundaries = list(re.finditer(pattern, window))
        if boundaries:
            return floor + boundaries[-1].end()
    return target_length


async def _is_description_region(region: Any) -> bool:
    try:
        text = await region.inner_text()
        if "产品描述" not in text or description_counter_value(text) is None:
            return False
        return await region.locator("[contenteditable='true']:visible").count() == 1
    except Exception:
        return False


async def description_region(editor: Any) -> Optional[Any]:
    """Find the smallest real description region using label, limit and editor."""
    item = await form_item(editor, "产品描述")
    if item is not None and await _is_description_region(item):
        return item

    anchors = editor.get_by_text("产品描述", exact=True)
    for index in range(await anchors.count()):
        anchor = anchors.nth(index)
        for levels in range(1, 13):
            ancestor = anchor.locator("xpath=" + "/".join([".."] * levels))
            if await _is_description_region(ancestor):
                return ancestor

    counters = editor.get_by_text(re.compile(r"\d+\s*/\s*10000(?!\d)"))
    for index in range(await counters.count()):
        counter = counters.nth(index)
        for levels in range(1, 13):
            ancestor = counter.locator("xpath=" + "/".join([".."] * levels))
            if await _is_description_region(ancestor):
                return ancestor

    editables = editor.locator("[contenteditable='true']:visible")
    for index in range(await editables.count()):
        editable = editables.nth(index)
        for levels in range(1, 13):
            ancestor = editable.locator("xpath=" + "/".join([".."] * levels))
            if await _is_description_region(ancestor):
                return ancestor
    return None


async def description_character_count(editor: Any) -> Optional[int]:
    # Counting must not depend on successfully locating the rich-text editor.
    # Miaoshou renders the authoritative "current/10000" counter in the editor
    # dialog even when contenteditable is wrapped by a changing component DOM.
    try:
        global_count = description_counter_value(await editor.inner_text())
        if global_count is not None:
            return global_count
    except Exception:
        pass
    region = await description_region(editor)
    if region is None:
        return None
    count = description_counter_value(await region.inner_text())
    if count is not None:
        return count
    editable = region.locator("[contenteditable='true']:visible")
    if await editable.count() == 0:
        return None
    return len(str(await editable.first.text_content() or ""))


def has_visible_description_text(text: str) -> bool:
    return bool(re.sub(r"[\s\u200b\ufeff]+", "", str(text or "")))


async def description_text_content(editor: Any) -> Optional[str]:
    region = await description_region(editor)
    if region is None:
        return None
    editable = region.locator("[contenteditable='true']:visible")
    if await editable.count() != 1:
        return None
    return str(await editable.first.text_content() or "")


async def clear_description_text(editor: Any) -> Dict[str, int]:
    """Clear description text nodes only, preserving elements and all images."""
    region = await description_region(editor)
    if region is None:
        raise ValueError("Product-description editor region is unavailable")
    editable = region.locator("[contenteditable='true']:visible")
    if await editable.count() != 1:
        raise ValueError(
            f"Expected one product-description editor, found {await editable.count()}"
        )
    editable = editable.first
    before_text = str(await editable.text_content() or "")
    images_before = await editable.locator("img").count()
    await editable.evaluate(
        """element => {
            const walker = document.createTreeWalker(
                element, NodeFilter.SHOW_TEXT
            );
            const nodes = [];
            while (walker.nextNode()) nodes.push(walker.currentNode);
            for (const node of nodes) node.data = "";
            element.dispatchEvent(new InputEvent("input", {
                bubbles: true,
                inputType: "deleteContentBackward",
                data: null,
            }));
            element.dispatchEvent(new Event("change", {bubbles: true}));
        }"""
    )
    after_text = str(await editable.text_content() or "")
    images_after = await editable.locator("img").count()
    if images_after != images_before:
        raise ValueError(
            "Clearing description text changed the image count: "
            f"{images_before} -> {images_after}"
        )
    if has_visible_description_text(after_text):
        raise ValueError("Product-description text remains after explicit clear")
    return {
        "text_characters_before": len(before_text),
        "text_characters_after": len(after_text),
        "images_before": images_before,
        "images_after": images_after,
    }


async def normalize_description_length(
    editor: Any,
    current_count: int,
    limit: int = DESCRIPTION_CHAR_LIMIT,
) -> bool:
    """Trim only trailing text nodes; keep the rich-text DOM and images intact."""
    if current_count <= limit:
        return False
    region = await description_region(editor)
    if region is None:
        raise ValueError("Product-description editor region is unavailable")
    editable = region.locator("[contenteditable='true']:visible")
    if await editable.count() != 1:
        raise ValueError(
            f"Expected one product-description editor, found {await editable.count()}"
        )
    editable = editable.first
    text = str(await editable.text_content() or "")
    excess = current_count - limit
    if len(text) <= excess:
        raise ValueError(
            "Product description cannot be shortened safely without removing images"
        )
    cutoff = description_text_cutoff(text, len(text) - excess)
    await editable.evaluate(
        """(element, cutoff) => {
            const walker = document.createTreeWalker(
                element, NodeFilter.SHOW_TEXT
            );
            const nodes = [];
            while (walker.nextNode()) nodes.push(walker.currentNode);
            let remaining = cutoff;
            for (const node of nodes) {
                if (remaining >= node.data.length) {
                    remaining -= node.data.length;
                    continue;
                }
                node.data = node.data.slice(0, Math.max(0, remaining))
                    .replace(/\s+$/u, "");
                remaining = 0;
            }
            element.dispatchEvent(new InputEvent("input", {
                bubbles: true,
                inputType: "deleteContentBackward",
                data: null,
            }));
            element.dispatchEvent(new Event("change", {bubbles: true}));
        }""",
        cutoff,
    )
    return True


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
    # Miaoshou's 2026-09 editor gives the purchase-price and display-price
    # inputs identical wrappers (`.price-currency` around `.currency-input`),
    # so columns must be resolved from the header text of the row's own table
    # instead of input classes.
    script = """
    elements => elements.map(row => {
      const cells = [...row.querySelectorAll('.pro-virtual-table__row-cell')];
      const table = row.closest('.pro-virtual-table');
      const headers = table
        ? [...table.querySelectorAll('.pro-virtual-table__header-cell')]
        : [];
      const columnIndex = keyword => headers.findIndex(header =>
        String(header.innerText || '').replace(/\s+/g, ' ').includes(keyword));
      const columns = {
        source: columnIndex('货源价格'),
        price: columnIndex('本地展示价'),
        stock: columnIndex('库存'),
        weight: columnIndex('重量')
      };
      const missing = Object.entries(columns)
        .filter(([, index]) => index < 0)
        .map(([name]) => name);
      if (missing.length || !cells.length) {
        return { error: `SKU table columns missing: ${missing.join(', ') || 'row cells'}` };
      }
      const cellValue = (index, selector) => {
        const cell = cells[index];
        if (!cell) return '';
        const el = cell.querySelector(selector) || cell.querySelector('input');
        return String(el?.value ?? '').trim();
      };
      const specs = [];
      for (let index = 0; index < cells.length && index < columns.source; index++) {
        if (cells[index].classList.contains('is-selection-column')) continue;
        const text = String(cells[index].innerText || '').trim().replace(/\s+/g, ' ');
        if (text) specs.push(text);
      }
      const priceCell = cells[columns.price];
      return {
        key: specs.join(' / '),
        label: specs.join(' / '),
        price: cellValue(columns.price, '.price-currency input'),
        purchase_price: cellValue(columns.source, '.currency-input input'),
        stock: cellValue(columns.stock, 'input'),
        weight: cellValue(columns.weight, '.package-weight-input input'),
        platform_price: String(priceCell?.querySelector('.pro-readonly-component')?.innerText || '').trim()
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
                if snapshot.get("error"):
                    raise ValueError(str(snapshot["error"]))
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


async def sku_column_indexes(page: Any) -> Dict[str, int]:
    """Resolve SKU table column indexes from the editor table header text.

    Purchase-price and display-price inputs share the same wrapper classes
    since Miaoshou's 2026-09 editor update, so handlers that fill or read a
    specific column must locate it via the header instead of input classes.
    """
    rows = await real_sku_rows(page)
    if rows is None:
        raise ValueError("Miaoshou SKU table is unavailable")
    mapping = await rows.first.evaluate(
        """
    row => {
      const table = row.closest('.pro-virtual-table');
      const headers = table
        ? [...table.querySelectorAll('.pro-virtual-table__header-cell')]
        : [];
      const find = keyword => headers.findIndex(header =>
        String(header.innerText || '').replace(/\s+/g, ' ').includes(keyword));
      return {
        source: find('货源价格'),
        price: find('本地展示价'),
        stock: find('库存'),
        weight: find('重量')
      };
    }
    """
    )
    missing = sorted(name for name, index in mapping.items() if int(index) < 0)
    if missing:
        raise ValueError(f"SKU table header columns missing: {', '.join(missing)}")
    return {name: int(index) for name, index in mapping.items()}
