from __future__ import annotations

from typing import Awaitable, Callable, TypeVar

from .browser import BrowserManager
from .config import CrawlerSettings
from .models import AmazonProductRecord
from .utils import ConcurrencyController, DelayController, build_logger

T = TypeVar("T")


class BaseCrawler:
    def __init__(self, settings: CrawlerSettings) -> None:
        self.settings = settings
        self.logger = build_logger()
        self.browser_manager = BrowserManager(settings)
        self.delay = DelayController(self.logger)
        self.detail_concurrency = ConcurrencyController(settings.max_detail_concurrency)

    async def start(self) -> None:
        await self.browser_manager.start()

    async def stop(self) -> None:
        await self.browser_manager.stop()

    async def run_with_detail_limit(self, task_factory: Callable[[], Awaitable[T]]) -> T:
        return await self.detail_concurrency.run(task_factory())

    def append_error(self, record: AmazonProductRecord, stage: str, exc: Exception) -> None:
        self.logger.warning("%s failed for ASIN=%s URL=%s: %s", stage, record.asin, record.url, exc)
        record.add_error(stage=stage, message=str(exc))
