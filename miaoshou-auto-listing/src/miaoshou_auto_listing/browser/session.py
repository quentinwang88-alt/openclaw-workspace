from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List
from urllib.parse import parse_qs, urlparse

from playwright.async_api import BrowserContext, Page, async_playwright
import requests

from ..errors import ExecutorError
from ..models import BrowserSettings, ErrorCode
from .selectors import SelectorRegistry


def validate_cdp_targets(
    targets: List[Dict[str, Any]], expected_host: str
) -> None:
    """Reject a shared or wrong Chrome before Playwright attaches every tab."""
    pages = [target for target in targets if target.get("type") == "page"]
    expected_pages = []
    foreign_pages = []
    for target in pages:
        url = str(target.get("url") or "")
        parsed = urlparse(url)
        if parsed.hostname == expected_host or (
            parsed.hostname and parsed.hostname.endswith(f".{expected_host}")
        ):
            expected_pages.append(target)
        elif url not in {"", "about:blank", "chrome://newtab/"}:
            foreign_pages.append(target)
    if not expected_pages:
        raise RuntimeError(
            f"CDP Chrome has no {expected_host} page; refusing the wrong port"
        )
    if foreign_pages:
        labels = [
            str(target.get("title") or urlparse(str(target.get("url") or "")).hostname)
            for target in foreign_pages[:5]
        ]
        raise RuntimeError(
            "CDP Chrome is not the dedicated Miaoshou instance; foreign pages: "
            + ", ".join(labels)
        )


def inspect_cdp_endpoint(cdp_url: str, base_url: str) -> None:
    response = requests.get(f"{cdp_url.rstrip('/')}/json/list", timeout=5)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise RuntimeError("CDP /json/list returned an invalid payload")
    expected_host = str(urlparse(base_url).hostname or "")
    validate_cdp_targets(payload, expected_host)


class BrowserSession:
    def __init__(self, settings: BrowserSettings, selectors: SelectorRegistry) -> None:
        self.settings = settings
        self.selectors = selectors

    @asynccontextmanager
    async def page(self) -> AsyncIterator[Page]:
        async with async_playwright() as playwright:
            if self.settings.cdp_url:
                inspect_cdp_endpoint(
                    self.settings.cdp_url, self.settings.base_url
                )
                browser = await playwright.chromium.connect_over_cdp(
                    self.settings.cdp_url,
                    timeout=self.settings.cdp_connect_timeout_ms,
                )
                if not browser.contexts:
                    raise RuntimeError("Connected Chrome has no browser context")
                context = browser.contexts[0]
                context.set_default_timeout(self.settings.timeout_ms)
                context.set_default_navigation_timeout(
                    self.settings.navigation_timeout_ms
                )
                page = next(
                    (
                        candidate
                        for candidate in reversed(context.pages)
                        if "91miaoshou.com" in candidate.url
                    ),
                    None,
                )
                created = page is None
                if page is None:
                    page = await context.new_page()
                await page.bring_to_front()
                try:
                    yield page
                finally:
                    if created:
                        await page.close()
                return

            self.settings.profile_dir.mkdir(parents=True, exist_ok=True)
            context: BrowserContext = await playwright.chromium.launch_persistent_context(
                user_data_dir=str(self.settings.profile_dir),
                headless=self.settings.headless,
                channel=self.settings.channel or None,
                slow_mo=self.settings.slow_mo_ms,
                locale=self.settings.locale,
                viewport={"width": 1440, "height": 1000},
            )
            context.set_default_timeout(self.settings.timeout_ms)
            context.set_default_navigation_timeout(self.settings.navigation_timeout_ms)
            page = context.pages[0] if context.pages else await context.new_page()
            try:
                yield page
            finally:
                await context.close()

    async def assert_logged_in(self, page: Page) -> None:
        parsed = urlparse(str(page.url or ""))
        redirected_to_home = (
            parsed.path in {"", "/"}
            and bool(parse_qs(parsed.query).get("redirect"))
        )
        if redirected_to_home or await self.selectors.exists(page, "login_form"):
            raise ExecutorError(
                ErrorCode.LOGIN_EXPIRED,
                "Miaoshou login page detected; refresh the dedicated browser profile manually",
            )
