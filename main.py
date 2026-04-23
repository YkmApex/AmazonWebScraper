from __future__ import annotations

import asyncio

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
    print(f"List page records collected: {len(list_records)}")
    for item in list_records[:3]:
        print(
            {
                "rank": item.rank,
                "asin": item.asin,
                "title": item.title[:80],
                "price": item.price,
                "rating": item.rating,
                "review_count": item.review_count,
            }
        )

    detail_records = await crawler.crawl_detail_pages(list_records)
    print(f"Detail page records enriched: {len(detail_records)}")
    for item in detail_records[:3]:
        print(
            {
                "asin": item.asin,
                "brand": item.brand,
                "monthly_sales": item.monthly_sales,
                "coupon_discount": item.coupon_discount,
                "dimensions_weight": item.dimensions_weight[:80],
                "feature_1": item.feature_1[:80],
                "sub_category_rank": item.sub_category_rank,
                "a_plus_content_flag": item.a_plus_content_flag,
                "bad_review_1": item.bad_review_1[:100],
            }
        )

    report_path = crawler.export_to_excel(detail_records)
    print(f"Excel report exported: {report_path}")

    snapshot_path = crawler.save_records_snapshot(detail_records)
    retry_list_path = crawler.generate_retry_list(detail_records)
    print(f"Snapshot saved: {snapshot_path}")
    print(f"Retry list saved: {retry_list_path}")

    retry_result = await crawler.retry_missing_asins(
        records=detail_records,
        retry_list_path=retry_list_path,
        report_path=report_path,
    )
    print(f"Retry targets executed: {len(retry_result.retry_targets)}")
    print(f"Updated retry list: {retry_result.retry_list_path}")
    print(f"Updated snapshot: {retry_result.snapshot_path}")
    if retry_result.report_path:
        print(f"Updated Excel report: {retry_result.report_path}")


if __name__ == "__main__":
    asyncio.run(main())
