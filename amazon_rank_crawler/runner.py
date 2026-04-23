from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional

from .base import BaseCrawler
from .config import CrawlerSettings
from .detail_page import AmazonDetailPageCrawler
from .exporter import AmazonExcelExporter
from .list_page import AmazonListPageCrawler
from .models import AmazonProductRecord
from .retry_manager import AmazonRetryManager, RetryRunResult


class AmazonRankCrawler(BaseCrawler):
    """
    High-level orchestrator.

    Step 1 only wires the runtime, anti-bot browser setup, and execution flow.
    Step 2-5 will fill in the actual list page scraping, detail extraction,
    export, and retry logic.
    """

    def __init__(self, settings: CrawlerSettings) -> None:
        super().__init__(settings)
        self.settings = settings
        self.list_page_crawler = AmazonListPageCrawler(
            settings=settings,
            browser_manager=self.browser_manager,
            delay=self.delay,
            logger=self.logger,
        )
        self.detail_page_crawler = AmazonDetailPageCrawler(
            settings=settings,
            browser_manager=self.browser_manager,
            delay=self.delay,
            logger=self.logger,
        )
        self.exporter = AmazonExcelExporter(settings=settings)
        self.retry_manager = AmazonRetryManager(settings=settings)

    async def bootstrap(self) -> None:
        await self.start()
        context = await self.browser_manager.new_context()
        page = await self.browser_manager.new_page(context)
        try:
            await self.browser_manager.safe_goto(page, self.settings.start_url)
            self.logger.info("Bootstrap warmup completed for %s", self.settings.start_url)
        finally:
            await context.close()
            await self.stop()

    async def crawl_list_page(self) -> list[AmazonProductRecord]:
        await self.start()
        try:
            return await self.list_page_crawler.crawl(target_count=100)
        finally:
            await self.stop()

    async def crawl_detail_pages(self, records: list[AmazonProductRecord]) -> list[AmazonProductRecord]:
        await self.start()
        try:
            tasks = [
                asyncio.create_task(
                    self.run_with_detail_limit(
                        lambda record=record: self.detail_page_crawler.crawl_record(record)
                    )
                )
                for record in records
            ]
            return await asyncio.gather(*tasks)
        finally:
            await self.stop()

    def export_to_excel(
        self,
        records: list[AmazonProductRecord],
        output_path: Optional[Path] = None,
    ) -> Path:
        return self.exporter.export(records, output_path=output_path)

    def generate_retry_list(self, records: list[AmazonProductRecord]) -> Path:
        return self.retry_manager.generate_retry_list(records)

    def save_records_snapshot(self, records: list[AmazonProductRecord]) -> Path:
        return self.retry_manager.save_records_snapshot(records)

    def load_records_snapshot(self) -> list[AmazonProductRecord]:
        return self.retry_manager.load_records_snapshot()

    async def retry_missing_asins(
        self,
        records: list[AmazonProductRecord],
        retry_asins: Optional[list[str]] = None,
        retry_urls: Optional[list[str]] = None,
        retry_list_path: Optional[Path] = None,
        report_path: Optional[Path] = None,
    ) -> RetryRunResult:
        retry_targets = self.retry_manager.select_retry_targets(
            records=records,
            retry_asins=retry_asins,
            retry_urls=retry_urls,
            retry_list_path=retry_list_path,
        )
        if not retry_targets:
            return self.retry_manager.build_retry_output(
                records=records,
                retry_targets=[],
                report_path=report_path,
            )

        await self.start()
        try:
            tasks = [
                asyncio.create_task(
                    self.run_with_detail_limit(
                        lambda record=record: self.detail_page_crawler.crawl_record(record)
                    )
                )
                for record in retry_targets
            ]
            retried_records = await asyncio.gather(*tasks)
        finally:
            await self.stop()

        merged_records = self.retry_manager.merge_retry_results(records, retried_records)
        final_report_path = report_path
        if report_path is not None:
            final_report_path = self.export_to_excel(merged_records, output_path=report_path)

        return self.retry_manager.build_retry_output(
            records=merged_records,
            retry_targets=retried_records,
            report_path=final_report_path,
        )
