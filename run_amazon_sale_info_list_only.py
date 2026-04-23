from __future__ import annotations

import asyncio
import logging

from amazon_rank_crawler import AmazonRankCrawler, CrawlerSettings


async def main() -> None:
    settings = CrawlerSettings(
        start_url="https://www.amazon.com/Best-Sellers/zgbs",
        headless=False,
        browser_channel=None,
        slow_mo_ms=50,
        max_detail_concurrency=1,
    )
    crawler = AmazonRankCrawler(settings)

    crawler.logger.setLevel(logging.ERROR)
    for handler in crawler.logger.handlers:
        handler.setLevel(logging.ERROR)

    list_records = await crawler.crawl_list_page()
    report_path = settings.output_dir / "amazon_sale_info.xlsx"
    crawler.export_to_excel(list_records, output_path=report_path)

    print(f"list_records={len(list_records)}")
    print(f"report_path={report_path}")


if __name__ == "__main__":
    asyncio.run(main())
