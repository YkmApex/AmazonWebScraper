from __future__ import annotations

import random
from pathlib import Path
from typing import Optional

from playwright.async_api import Browser, BrowserContext, Page, Playwright, async_playwright

from .config import CrawlerSettings
from .utils import build_logger, ensure_directories

try:
    from playwright_stealth import stealth_async
except ImportError:  # pragma: no cover - optional dependency fallback
    stealth_async = None


ANTI_BOT_INIT_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
Object.defineProperty(navigator, 'language', { get: () => 'en-US' });
Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
window.chrome = window.chrome || { runtime: {} };
Object.defineProperty(navigator, 'plugins', {
  get: () => [
    { name: 'Chrome PDF Plugin' },
    { name: 'Chrome PDF Viewer' },
    { name: 'Native Client' }
  ],
});
const originalQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (parameters) => (
  parameters.name === 'notifications'
    ? Promise.resolve({ state: Notification.permission })
    : originalQuery(parameters)
);
"""


class BrowserManager:
    def __init__(self, settings: CrawlerSettings) -> None:
        self.settings = settings
        self.logger = build_logger()
        self.playwright: Optional[Playwright] = None
        self.browser: Optional[Browser] = None

    async def start(self) -> None:
        ensure_directories(
            [
                self.settings.output_dir,
                self.settings.temp_dir,
                self.settings.image_dir,
                self.settings.log_dir,
            ]
        )
        self.playwright = await async_playwright().start()
        chromium = self.playwright.chromium
        self.browser = await chromium.launch(
            headless=self.settings.headless,
            slow_mo=self.settings.slow_mo_ms,
            channel=self.settings.browser_channel,
            proxy=self.settings.proxy.as_playwright_proxy() if self.settings.proxy else None,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--disable-infobars",
                "--disable-popup-blocking",
                "--lang=en-US",
                "--no-first-run",
                "--no-default-browser-check",
            ],
        )
        self.logger.info("Browser launched successfully")

    async def stop(self) -> None:
        if self.browser:
            await self.browser.close()
            self.browser = None
        if self.playwright:
            await self.playwright.stop()
            self.playwright = None
        self.logger.info("Browser closed")

    async def new_context(self) -> BrowserContext:
        if not self.browser:
            raise RuntimeError("Browser is not started")

        user_agent = random.choice(self.settings.user_agents)
        viewport = random.choice(self.settings.viewports)

        context = await self.browser.new_context(
            user_agent=user_agent,
            viewport=viewport,
            locale=self.settings.locale,
            timezone_id=self.settings.timezone_id,
            java_script_enabled=True,
            extra_http_headers=self.settings.base_headers,
            geolocation=self.settings.geolocation,
            ignore_https_errors=False,
        )
        await context.add_init_script(ANTI_BOT_INIT_SCRIPT)
        return context

    async def new_page(self, context: BrowserContext) -> Page:
        page = await context.new_page()
        page.set_default_timeout(self.settings.page_timeout_ms)
        page.set_default_navigation_timeout(self.settings.navigation_timeout_ms)

        if self.settings.verbose_console_log:
            page.on("console", lambda msg: self.logger.info("PAGE CONSOLE: %s", msg.text))

        page.on("pageerror", lambda exc: self.logger.warning("Page error: %s", exc))
        page.on("requestfailed", lambda request: self.logger.warning("Request failed: %s", request.url))

        if stealth_async:
            try:
                await stealth_async(page)
            except Exception as exc:  # pragma: no cover - defensive
                self.logger.warning("Failed to apply stealth plugin, fallback to init script only: %s", exc)

        return page

    async def safe_goto(self, page: Page, url: str, wait_until: str = "domcontentloaded") -> None:
        last_error: Optional[Exception] = None
        for attempt in range(1, self.settings.max_navigation_retries + 1):
            try:
                self.logger.info("Navigating to %s (attempt %s)", url, attempt)
                await page.goto(url, wait_until=wait_until)
                return
            except Exception as exc:
                last_error = exc
                self.logger.warning("Navigation failed for %s: %s", url, exc)
                await page.wait_for_timeout(random.randint(900, 2200))
        raise RuntimeError(f"Failed to open page after retries: {url}") from last_error

    async def capture_debug_snapshot(self, page: Page, file_stem: str) -> Optional[Path]:
        if not self.settings.screenshot_on_error:
            return None

        snapshot_path = self.settings.log_dir / f"{file_stem}.png"
        try:
            await page.screenshot(path=str(snapshot_path), full_page=True)
            return snapshot_path
        except Exception as exc:  # pragma: no cover - defensive
            self.logger.warning("Failed to capture screenshot %s: %s", snapshot_path, exc)
            return None
