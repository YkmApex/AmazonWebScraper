from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path


def install_playwright_stubs() -> None:
    if "playwright.async_api" not in sys.modules:
        playwright_module = types.ModuleType("playwright")
        async_api_module = types.ModuleType("playwright.async_api")

        class DummyObject:
            pass

        class DummyTimeoutError(Exception):
            pass

        async def async_playwright():
            raise RuntimeError("Playwright runtime is not available in smoke tests")

        async_api_module.Browser = DummyObject
        async_api_module.BrowserContext = DummyObject
        async_api_module.Page = DummyObject
        async_api_module.Playwright = DummyObject
        async_api_module.TimeoutError = DummyTimeoutError
        async_api_module.async_playwright = async_playwright

        sys.modules["playwright"] = playwright_module
        sys.modules["playwright.async_api"] = async_api_module

    if "playwright_stealth" not in sys.modules:
        stealth_module = types.ModuleType("playwright_stealth")

        async def stealth_async(page):
            return None

        stealth_module.stealth_async = stealth_async
        sys.modules["playwright_stealth"] = stealth_module


install_playwright_stubs()

from bs4 import BeautifulSoup

from amazon_rank_crawler.config import CrawlerSettings
from amazon_rank_crawler.detail_page import AmazonDetailPageCrawler
from amazon_rank_crawler.list_page import AmazonListPageCrawler
from amazon_rank_crawler.utils import build_logger


class ParserSmokeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = CrawlerSettings(
            start_url="https://www.amazon.com/Best-Sellers/zgbs",
            workspace_dir=Path.cwd(),
        )
        self.logger = build_logger("parser_smoke_test")

    def test_list_page_parser_extracts_expected_fields(self) -> None:
        html = """
        <html>
          <body>
            <div id="p13n-asin-index-0" data-asin="B0TEST1234" class="p13n-sc-uncoverable-faceout">
              <span class="zg-bdg-text">#1</span>
              <a href="/dp/B0TEST1234?psc=1">
                <img src="https://images.example.com/item.jpg" alt="Sample Phone Case for iPhone 15 Pro Max" />
                <span class="a-size-base-plus">Sample Phone Case for iPhone 15 Pro Max</span>
              </a>
              <span class="p13n-sc-price">$19.99</span>
              <span class="a-icon-alt">4.5 out of 5 stars</span>
              <a class="a-size-small" href="/product-reviews/B0TEST1234">1,234</a>
            </div>
          </body>
        </html>
        """
        crawler = AmazonListPageCrawler(
            settings=self.settings,
            browser_manager=None,
            delay=None,
            logger=self.logger,
        )

        records = crawler.parse_listing_html(html)
        self.assertEqual(len(records), 1)

        record = records[0]
        self.assertEqual(record.rank, "#1")
        self.assertEqual(record.asin, "B0TEST1234")
        self.assertEqual(record.url, "https://www.amazon.com/dp/B0TEST1234")
        self.assertEqual(record.title, "Sample Phone Case for iPhone 15 Pro Max")
        self.assertEqual(record.main_image_url, "https://images.example.com/item.jpg")
        self.assertEqual(record.price, "$19.99")
        self.assertEqual(record.rating, "4.5 out of 5 stars")
        self.assertEqual(record.review_count, "1,234")

    def test_detail_page_parser_extracts_expected_fields(self) -> None:
        html = """
        <html>
          <body>
            <a id="bylineInfo">Visit the SampleBrand Store</a>
            <span id="productTitle">Sample Case</span>
            <div id="social-proofing-faceout-title-tk_bought">10K+ bought in past month</div>
            <div id="couponText">Save 10% with coupon</div>
            <div id="feature-bullets">
              <ul>
                <li><span class="a-list-item">Shockproof design for daily drops</span></li>
                <li><span class="a-list-item">Compatible with MagSafe charging</span></li>
                <li><span class="a-list-item">Raised bezels protect camera lens</span></li>
                <li><span class="a-list-item">Slim fit with anti-slip texture</span></li>
                <li><span class="a-list-item">Easy access to all buttons and ports</span></li>
              </ul>
            </div>
            <div id="detailBullets_feature_div">
              <ul>
                <li><span class="a-list-item">Product Dimensions: 6 x 3 x 0.5 inches</span></li>
                <li><span class="a-list-item">Item Weight: 2 ounces</span></li>
                <li><span class="a-list-item">Best Sellers Rank: #12 in Cell Phone Basic Cases (#345 in Cell Phones & Accessories)</span></li>
              </ul>
            </div>
            <div id="aplus_feature_div">A+ marketing content</div>
          </body>
        </html>
        """
        soup = BeautifulSoup(html, "lxml")
        crawler = AmazonDetailPageCrawler(
            settings=self.settings,
            browser_manager=None,
            delay=None,
            logger=self.logger,
        )

        payload = crawler.parse_detail_html(soup)
        self.assertEqual(payload["brand"], "SampleBrand")
        self.assertEqual(payload["monthly_sales"], "10K+ bought in past month")
        self.assertEqual(payload["coupon_discount"], "Save 10% with coupon")
        self.assertIn("Product Dimensions: 6 x 3 x 0.5 inches", payload["dimensions_weight"])
        self.assertIn("Item Weight: 2 ounces", payload["dimensions_weight"])
        self.assertEqual(payload["feature_1"], "Shockproof design for daily drops")
        self.assertEqual(payload["feature_5"], "Easy access to all buttons and ports")
        self.assertEqual(payload["sub_category_rank"], "#12 in Cell Phone Basic Cases")
        self.assertTrue(payload["a_plus_content_flag"])

    def test_negative_review_parser_filters_low_ratings(self) -> None:
        html = """
        <html>
          <body>
            <div data-hook="review">
              <i data-hook="review-star-rating"><span>2.0 out of 5 stars</span></i>
              <a data-hook="review-title"><span>Poor fit</span></a>
              <span data-hook="review-body">It did not align with the buttons and felt flimsy.</span>
            </div>
            <div data-hook="review">
              <i data-hook="review-star-rating"><span>4.0 out of 5 stars</span></i>
              <a data-hook="review-title"><span>Pretty good</span></a>
              <span data-hook="review-body">Works well enough for the price.</span>
            </div>
          </body>
        </html>
        """
        soup = BeautifulSoup(html, "lxml")
        crawler = AmazonDetailPageCrawler(
            settings=self.settings,
            browser_manager=None,
            delay=None,
            logger=self.logger,
        )

        reviews = crawler._parse_negative_review_texts(soup)
        self.assertEqual(len(reviews), 1)
        self.assertIn("Poor fit", reviews[0])
        self.assertNotIn("Pretty good", reviews[0])


if __name__ == "__main__":
    unittest.main()
