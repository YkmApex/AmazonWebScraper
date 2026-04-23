from __future__ import annotations

import asyncio
from pathlib import Path

from amazon_rank_crawler import AmazonRankCrawler, CrawlerSettings


async def main() -> None:
    settings = CrawlerSettings(
        start_url="https://www.amazon.com/Best-Sellers/zgbs",
        headless=False,
        browser_channel=None,
        slow_mo_ms=50,
        max_detail_concurrency=3,
    )
    crawler = AmazonRankCrawler(settings)

    list_records = await crawler.crawl_list_page()
    detail_records = await crawler.crawl_detail_pages(list_records)

    report_path = settings.output_dir / "amazon_sale_info.xlsx"

    crawler.export_to_excel(detail_records, output_path=report_path)
    crawler.save_records_snapshot(detail_records)
    retry_list_path = crawler.generate_retry_list(detail_records)

    retry_result = await crawler.retry_missing_asins(
        records=detail_records,
        retry_list_path=retry_list_path,
        report_path=report_path,
    )

    print(f"list_records={len(list_records)}")
    print(f"detail_records={len(detail_records)}")
    print(f"retry_targets={len(retry_result.retry_targets)}")
    print(f"report_path={report_path}")
    print(f"snapshot_path={retry_result.snapshot_path}")
    print(f"retry_list_path={retry_result.retry_list_path}")


if __name__ == "__main__":
    asyncio.run(main())
