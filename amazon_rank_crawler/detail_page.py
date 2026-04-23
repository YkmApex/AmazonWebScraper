from __future__ import annotations

import re
from typing import Optional
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup, Tag
from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

from .browser import BrowserManager
from .config import CrawlerSettings
from .models import AmazonProductRecord
from .utils import DelayController

MONTHLY_SALES_RE = re.compile(
    r"\b\d[\d.,]*(?:[KMB]?\+)?\s+(?:bought|sold)\s+in\s+(?:past|last)\s+month\b",
    re.IGNORECASE,
)
BEST_SELLERS_RANK_RE = re.compile(r"#\d[\d,]*\s+in\s+[^#\n\r\(\)\[\]\|]+", re.IGNORECASE)
COUPON_RE = re.compile(
    r"(?:Apply|Save)\s+(?:an?\s+)?(?:extra\s+)?(?:[$€£]\s?\d[\d,]*(?:\.\d{2})?|\d{1,2}%)"
    r"(?:\s+coupon|\s+with coupon)?(?:\s+at checkout)?",
    re.IGNORECASE,
)
STAR_VALUE_RE = re.compile(r"([1-5](?:\.\d)?)\s*out of 5 stars", re.IGNORECASE)
PRICE_RE = re.compile(r"[$€£]\s?\d[\d,]*(?:\.\d{2})?")
REVIEW_COUNT_RE = re.compile(r"^\d[\d,]*(?:\.\d+)?$")

DETAIL_READY_SELECTORS = (
    "#productTitle",
    "#centerCol",
    "#feature-bullets",
    "#dp",
    "#bylineInfo",
)

DETAIL_TABLE_SELECTORS = (
    "#productDetails_techSpec_section_1 tr",
    "#productDetails_detailBullets_sections1 tr",
    "#productOverview_feature_div tr",
    "#technicalSpecifications_section_1 tr",
    "#productDetails_db_sections tr",
)

DETAIL_BULLET_SELECTORS = (
    "#detailBullets_feature_div li",
    "#detailBulletsWrapper_feature_div li",
    "#detailBullets li",
)

FEATURE_BULLET_SELECTORS = (
    "#feature-bullets ul li span.a-list-item",
    "#feature-bullets li span.a-list-item",
    "#feature-bullets li",
)

COUPON_SELECTORS = (
    "#couponTextpctch",
    "#couponText",
    "#vpcButton .a-color-success",
    "[data-csa-c-content-id='coupon_feature_div']",
    "#promoPriceBlockMessage_feature_div",
)

A_PLUS_SELECTORS = (
    "#aplus",
    "#aplus_feature_div",
    "#productDescription_feature_div + #aplus_feature_div",
    "div[id*='aplus']",
    "section[id*='aplus']",
)

REVIEW_READY_SELECTORS = (
    "#cm_cr-review_list",
    "div[data-hook='review']",
    "div.review",
)

DETAIL_PRICE_SELECTORS = (
    "#corePrice_feature_div span.a-price span.a-offscreen",
    "#corePriceDisplay_desktop_feature_div span.a-price span.a-offscreen",
    "#apex_desktop span.a-price span.a-offscreen",
    "#priceblock_ourprice",
    "#priceblock_dealprice",
    "#priceblock_saleprice",
)

DETAIL_RATING_SELECTORS = (
    "#acrPopover span.a-size-base.a-color-base",
    "#acrPopover .a-icon-alt",
    "i[data-hook='average-star-rating'] span.a-icon-alt",
)

DETAIL_REVIEW_COUNT_SELECTORS = (
    "#acrCustomerReviewText",
    "span[data-hook='total-review-count']",
)


class AmazonDetailPageCrawler:
    def __init__(
        self,
        settings: CrawlerSettings,
        browser_manager: BrowserManager,
        delay: DelayController,
        logger,
    ) -> None:
        self.settings = settings
        self.browser_manager = browser_manager
        self.delay = delay
        self.logger = logger
        self.amazon_base = self._build_amazon_base_url(settings.start_url)

    async def crawl_record(self, record: AmazonProductRecord) -> AmazonProductRecord:
        if not record.url:
            record.add_error("detail_page", "Missing detail page URL")
            return record

        await self.delay.sleep_random(
            self.settings.min_detail_pause,
            self.settings.max_detail_pause,
            f"before detail page {record.asin or record.rank or 'unknown'}",
        )

        context = await self.browser_manager.new_context()
        page = await self.browser_manager.new_page(context)

        try:
            await self.browser_manager.safe_goto(page, record.url)
            await self._post_navigation_stabilize(page)
            await self._raise_if_bot_challenge(page)
            await self._dismiss_popups(page)
            await self._wait_for_detail_ready(page)
            await self._raise_if_bot_challenge(page)

            html = await page.content()
            soup = BeautifulSoup(html, "lxml")
            payload = self.parse_detail_html(soup)

            try:
                negative_reviews = await self.fetch_negative_reviews(page=page, soup=soup, record=record)
            except Exception as exc:
                negative_reviews = []
                record.add_error("negative_reviews", str(exc))
                self.logger.warning(
                    "Negative review crawl failed for ASIN=%s URL=%s: %s",
                    record.asin,
                    record.url,
                    exc,
                )

            payload.update(self._negative_reviews_payload(negative_reviews))
            record.merge(payload)
            return record
        except Exception as exc:
            self.logger.warning("Detail crawl failed for ASIN=%s URL=%s: %s", record.asin, record.url, exc)
            record.add_error("detail_page", str(exc))
            await self.browser_manager.capture_debug_snapshot(page, self._safe_file_stem(record))
            return record
        finally:
            await context.close()

    async def _post_navigation_stabilize(self, page: Page) -> None:
        await self.delay.sleep_random(1.0, 2.2, "detail page settle")
        try:
            await page.wait_for_load_state("networkidle", timeout=8_000)
        except PlaywrightTimeoutError:
            self.logger.info("Detail page network idle wait timed out, continue")

    async def _dismiss_popups(self, page: Page) -> None:
        popup_selectors = (
            "input#sp-cc-accept",
            "input[name='accept']",
            "button[data-action='a-popover-close']",
            "span[data-action='a-popover-close'] input.a-button-input",
        )
        for selector in popup_selectors:
            try:
                locator = page.locator(selector)
                if await locator.count() > 0 and await locator.first.is_visible():
                    await locator.first.click(delay=120)
                    await self.delay.sleep_random(0.4, 1.0, f"dismiss detail popup {selector}")
            except Exception:
                continue

    async def _wait_for_detail_ready(self, page: Page) -> None:
        for selector in DETAIL_READY_SELECTORS:
            try:
                await page.locator(selector).first.wait_for(timeout=7_500)
                return
            except Exception:
                continue

        html = await page.content()
        if "productTitle" in html or "feature-bullets" in html or 'id="dp"' in html:
            return
        raise RuntimeError("Amazon detail page DOM did not become ready")

    async def _raise_if_bot_challenge(self, page: Page) -> None:
        title = ""
        try:
            title = await page.title()
        except Exception:
            title = ""

        html = await page.content()
        captcha_markers = (
            "Robot Check",
            "Enter the characters you see below",
            "Type the characters you see in this image",
            "/errors/validateCaptcha",
            "Sorry, we just need to make sure you're not a robot",
        )

        if any(marker.lower() in title.lower() for marker in captcha_markers):
            raise RuntimeError("Amazon bot challenge detected on detail page")
        if any(marker.lower() in html.lower() for marker in captcha_markers):
            raise RuntimeError("Amazon bot challenge detected on detail page")

    def parse_detail_html(self, soup: BeautifulSoup) -> dict[str, object]:
        attributes = self._extract_detail_attributes(soup)
        page_text = self._clean_text(soup.get_text(" ", strip=True))

        bullet_points = self._extract_bullet_points(soup)
        negative_placeholders = [""] * self.settings.negative_review_limit

        payload: dict[str, object] = {
            "title": self._extract_title(soup),
            "price": self._extract_price(soup, page_text),
            "rating": self._extract_rating(soup),
            "review_count": self._extract_review_count(soup),
            "main_image_url": self._extract_main_image_url(soup),
            "brand": self._extract_brand(soup, attributes),
            "monthly_sales": self._extract_monthly_sales(soup, page_text),
            "coupon_discount": self._extract_coupon_discount(soup, page_text),
            "dimensions_weight": self._extract_dimensions_weight(attributes, page_text),
            "feature_1": bullet_points[0] if len(bullet_points) > 0 else "",
            "feature_2": bullet_points[1] if len(bullet_points) > 1 else "",
            "feature_3": bullet_points[2] if len(bullet_points) > 2 else "",
            "feature_4": bullet_points[3] if len(bullet_points) > 3 else "",
            "feature_5": bullet_points[4] if len(bullet_points) > 4 else "",
            "sub_category_rank": self._extract_sub_category_rank(soup, attributes, page_text),
            "a_plus_content_flag": self._extract_a_plus_flag(soup),
            "bad_review_1": negative_placeholders[0],
            "bad_review_2": negative_placeholders[1],
            "bad_review_3": negative_placeholders[2],
            "bad_review_4": negative_placeholders[3],
            "bad_review_5": negative_placeholders[4],
        }
        return payload

    async def fetch_negative_reviews(
        self,
        page: Page,
        soup: BeautifulSoup,
        record: AmazonProductRecord,
    ) -> list[str]:
        review_base_url = self._extract_review_base_url(soup, record)
        if not review_base_url:
            return []

        collected: list[str] = []
        seen: set[str] = set()

        for filter_by_star in ("one_star", "two_star"):
            for page_number in range(1, self.settings.max_negative_review_pages + 1):
                if len(collected) >= self.settings.negative_review_limit:
                    return collected[: self.settings.negative_review_limit]

                review_url = self._build_review_page_url(
                    base_url=review_base_url,
                    filter_by_star=filter_by_star,
                    page_number=page_number,
                )
                await self.delay.sleep_random(0.8, 1.8, f"negative review {filter_by_star} p{page_number}")
                await self.browser_manager.safe_goto(page, review_url)
                await self._wait_for_review_ready(page)
                await self._raise_if_bot_challenge(page)

                review_html = await page.content()
                review_soup = BeautifulSoup(review_html, "lxml")
                review_texts = self._parse_negative_review_texts(review_soup)
                if not review_texts:
                    if page_number == 1:
                        self.logger.info(
                            "No negative reviews found for ASIN=%s on %s filter",
                            record.asin,
                            filter_by_star,
                        )
                    break

                for review_text in review_texts:
                    key = review_text.lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    collected.append(review_text)
                    if len(collected) >= self.settings.negative_review_limit:
                        return collected[: self.settings.negative_review_limit]

        return collected[: self.settings.negative_review_limit]

    async def _wait_for_review_ready(self, page: Page) -> None:
        for selector in REVIEW_READY_SELECTORS:
            try:
                await page.locator(selector).first.wait_for(timeout=7_000)
                return
            except Exception:
                continue

        html = await page.content()
        if "customer reviews" in html.lower() or "/product-reviews/" in page.url:
            return
        raise RuntimeError("Amazon review page DOM did not become ready")

    def _extract_detail_attributes(self, soup: BeautifulSoup) -> dict[str, str]:
        attributes: dict[str, str] = {}

        for selector in DETAIL_TABLE_SELECTORS:
            for row in soup.select(selector):
                cells = row.find_all(["th", "td"])
                if len(cells) < 2:
                    continue

                label = self._normalize_label(cells[0].get_text(" ", strip=True))
                value = self._clean_text(cells[1].get_text(" ", strip=True))
                if label and value and label not in attributes:
                    attributes[label] = value

        for selector in DETAIL_BULLET_SELECTORS:
            for item in soup.select(selector):
                text = self._clean_text(item.get_text(" ", strip=True))
                if ":" not in text:
                    continue

                label, value = text.split(":", 1)
                norm_label = self._normalize_label(label)
                clean_value = self._clean_text(value)
                if norm_label and clean_value and norm_label not in attributes:
                    attributes[norm_label] = clean_value

        return attributes

    def _extract_title(self, soup: BeautifulSoup) -> str:
        node = soup.select_one("#productTitle")
        if isinstance(node, Tag):
            return self._clean_text(node.get_text(" ", strip=True))
        return ""

    def _extract_price(self, soup: BeautifulSoup, page_text: str) -> str:
        for selector in DETAIL_PRICE_SELECTORS:
            node = soup.select_one(selector)
            if isinstance(node, Tag):
                text = self._clean_text(node.get_text(" ", strip=True))
                match = PRICE_RE.search(text)
                if match:
                    return match.group(0).replace(" ", "")

        match = PRICE_RE.search(page_text)
        return match.group(0).replace(" ", "") if match else ""

    def _extract_rating(self, soup: BeautifulSoup) -> str:
        for selector in DETAIL_RATING_SELECTORS:
            node = soup.select_one(selector)
            if isinstance(node, Tag):
                text = self._clean_text(node.get_text(" ", strip=True))
                if "out of 5 stars" in text.lower():
                    return text

        node = soup.select_one("span.a-icon-alt")
        if isinstance(node, Tag):
            text = self._clean_text(node.get_text(" ", strip=True))
            if "out of 5 stars" in text.lower():
                return text
        return ""

    def _extract_review_count(self, soup: BeautifulSoup) -> str:
        for selector in DETAIL_REVIEW_COUNT_SELECTORS:
            node = soup.select_one(selector)
            if isinstance(node, Tag):
                text = self._clean_text(node.get_text(" ", strip=True))
                digits = re.sub(r"[^\d,]", "", text)
                if digits and REVIEW_COUNT_RE.fullmatch(digits):
                    return digits
        return ""

    def _extract_main_image_url(self, soup: BeautifulSoup) -> str:
        image = soup.select_one("#landingImage")
        if not isinstance(image, Tag):
            image = soup.select_one("#imgBlkFront, img[data-old-hires], img#main-image")
        if not isinstance(image, Tag):
            return ""

        for attr in ("data-old-hires", "src", "data-a-dynamic-image"):
            value = self._clean_text(image.get(attr, ""))
            if value and attr != "data-a-dynamic-image":
                return value
            if value and attr == "data-a-dynamic-image":
                matches = re.findall(r'"(https://[^"]+)"', value)
                if matches:
                    return matches[0]
        return ""

    def _extract_brand(self, soup: BeautifulSoup, attributes: dict[str, str]) -> str:
        byline = soup.select_one("#bylineInfo, a#bylineInfo")
        if isinstance(byline, Tag):
            text = self._clean_text(byline.get_text(" ", strip=True))
            visit_match = re.search(r"Visit the (.+?) Store", text, re.IGNORECASE)
            if visit_match:
                return self._clean_text(visit_match.group(1))

            brand_match = re.search(r"Brand:\s*(.+)", text, re.IGNORECASE)
            if brand_match:
                return self._clean_text(brand_match.group(1))

            if text:
                return text.replace("Brand:", "").strip()

        for key in ("brand", "manufacturer", "publisher", "manufacturer reference"):
            value = attributes.get(key, "")
            if value:
                return value
        return ""

    def _extract_monthly_sales(self, soup: BeautifulSoup, page_text: str) -> str:
        sales_selectors = (
            "#social-proofing-faceout-title-tk_bought",
            "#social-proofing-faceout-title-tk_sold",
            "[id*='social-proofing-faceout-title']",
        )
        for selector in sales_selectors:
            node = soup.select_one(selector)
            if isinstance(node, Tag):
                text = self._clean_text(node.get_text(" ", strip=True))
                if MONTHLY_SALES_RE.search(text):
                    return text

        match = MONTHLY_SALES_RE.search(page_text)
        return self._clean_text(match.group(0)) if match else ""

    def _extract_coupon_discount(self, soup: BeautifulSoup, page_text: str) -> str:
        for selector in COUPON_SELECTORS:
            node = soup.select_one(selector)
            if isinstance(node, Tag):
                text = self._clean_text(node.get_text(" ", strip=True))
                coupon_match = COUPON_RE.search(text)
                if coupon_match:
                    return self._clean_text(coupon_match.group(0))
                if "coupon" in text.lower() or "save" in text.lower():
                    return text

        match = COUPON_RE.search(page_text)
        return self._clean_text(match.group(0)) if match else ""

    def _extract_dimensions_weight(self, attributes: dict[str, str], page_text: str) -> str:
        parts: list[str] = []
        dimension_keys = (
            "product dimensions",
            "item dimensions l x w x h",
            "package dimensions",
        )
        weight_keys = (
            "item weight",
            "package weight",
            "shipping weight",
        )

        for key in dimension_keys:
            value = attributes.get(key, "")
            if value:
                parts.append(f"{key.title()}: {value}")

        for key in weight_keys:
            value = attributes.get(key, "")
            if value:
                parts.append(f"{key.title()}: {value}")

        if parts:
            return " | ".join(dict.fromkeys(parts))

        fallback_matches: list[str] = []
        for pattern in (
            r"(?:Product|Package)\s+Dimensions\s*[:\-]\s*[^|]{3,80}",
            r"(?:Item|Package|Shipping)\s+Weight\s*[:\-]\s*[^|]{2,60}",
        ):
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                fallback_matches.append(self._clean_text(match.group(0)))

        return " | ".join(dict.fromkeys(fallback_matches))

    def _extract_bullet_points(self, soup: BeautifulSoup) -> list[str]:
        bullets: list[str] = []
        seen: set[str] = set()

        for selector in FEATURE_BULLET_SELECTORS:
            for node in soup.select(selector):
                text = self._clean_text(node.get_text(" ", strip=True))
                if not text or len(text) < 8:
                    continue
                if text.lower().startswith("make sure this fits"):
                    continue
                if text in seen:
                    continue
                seen.add(text)
                bullets.append(text)
                if len(bullets) >= 5:
                    return bullets

        return bullets[:5]

    def _extract_sub_category_rank(
        self,
        soup: BeautifulSoup,
        attributes: dict[str, str],
        page_text: str,
    ) -> str:
        rank_text = attributes.get("best sellers rank", "")
        if not rank_text:
            for selector in (
                "#detailBulletsWrapper_feature_div",
                "#detailBullets_feature_div",
                "#productDetails_detailBullets_sections1",
                "#productDetails_db_sections",
            ):
                node = soup.select_one(selector)
                if isinstance(node, Tag):
                    rank_text = self._clean_text(node.get_text(" ", strip=True))
                    if rank_text:
                        break

        if not rank_text:
            rank_text = page_text

        matches = [self._clean_text(match) for match in BEST_SELLERS_RANK_RE.findall(rank_text)]
        if matches:
            return matches[0]
        return ""

    def _extract_a_plus_flag(self, soup: BeautifulSoup) -> bool:
        for selector in A_PLUS_SELECTORS:
            if soup.select_one(selector) is not None:
                return True
        return False

    def _extract_review_base_url(self, soup: BeautifulSoup, record: AmazonProductRecord) -> str:
        for anchor in soup.select(
            "a[href*='/product-reviews/'], a[data-hook='see-all-reviews-link-foot'], a[data-hook='see-all-reviews-link']"
        ):
            href = anchor.get("href", "")
            if "/product-reviews/" in href:
                return urljoin(self.amazon_base, href)

        if record.asin:
            return urljoin(self.amazon_base, f"/product-reviews/{record.asin}/")
        return ""

    def _build_review_page_url(self, base_url: str, filter_by_star: str, page_number: int) -> str:
        parsed = urlparse(base_url)
        query = parse_qs(parsed.query)
        query["reviewerType"] = ["all_reviews"]
        query["sortBy"] = ["recent"]
        query["filterByStar"] = [filter_by_star]
        query["pageNumber"] = [str(page_number)]
        return urlunparse(parsed._replace(query=urlencode(query, doseq=True), fragment=""))

    def _parse_negative_review_texts(self, soup: BeautifulSoup) -> list[str]:
        reviews: list[str] = []
        seen: set[str] = set()

        for container in soup.select("div[data-hook='review'], div.review"):
            if not isinstance(container, Tag):
                continue

            rating = self._extract_review_star_value(container)
            if rating is not None and rating >= 3:
                continue

            title = self._select_first_text(
                container,
                (
                    "a[data-hook='review-title'] span",
                    "span[data-hook='review-title']",
                    "a[data-hook='review-title']",
                ),
            )
            body = self._select_first_text(
                container,
                (
                    "span[data-hook='review-body']",
                    "div[data-hook='review-collapsed'] span",
                    "div.review-data",
                ),
            )
            review_text = self._clean_text(" ".join(part for part in (title, body) if part))
            if len(review_text) < 20:
                continue

            key = review_text.lower()
            if key in seen:
                continue
            seen.add(key)
            reviews.append(review_text)
            if len(reviews) >= self.settings.negative_review_limit:
                return reviews

        return reviews

    def _extract_review_star_value(self, container: Tag) -> Optional[float]:
        rating_text = self._select_first_text(
            container,
            (
                "i[data-hook='review-star-rating'] span",
                "i[data-hook='cmps-review-star-rating'] span",
                "span.a-icon-alt",
            ),
        )
        if not rating_text:
            return None
        match = STAR_VALUE_RE.search(rating_text)
        if not match:
            return None
        try:
            return float(match.group(1))
        except ValueError:
            return None

    def _select_first_text(self, container: Tag, selectors: tuple[str, ...]) -> str:
        for selector in selectors:
            node = container.select_one(selector)
            if isinstance(node, Tag):
                text = self._clean_text(node.get_text(" ", strip=True))
                if text:
                    return text
        return ""

    def _negative_reviews_payload(self, reviews: list[str]) -> dict[str, str]:
        reviews = reviews[: self.settings.negative_review_limit]
        padded = reviews + [""] * max(0, self.settings.negative_review_limit - len(reviews))
        return {
            "bad_review_1": padded[0],
            "bad_review_2": padded[1],
            "bad_review_3": padded[2],
            "bad_review_4": padded[3],
            "bad_review_5": padded[4],
        }

    def _normalize_label(self, value: str) -> str:
        return self._clean_text(value).rstrip(":").lower()

    def _build_amazon_base_url(self, url: str) -> str:
        parsed = urlparse(url)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
        return "https://www.amazon.com"

    def _safe_file_stem(self, record: AmazonProductRecord) -> str:
        raw = record.asin or record.rank or "detail_page_error"
        cleaned = re.sub(r"[^A-Za-z0-9_-]+", "_", raw)
        return f"detail_{cleaned}"

    def _clean_text(self, value: str) -> str:
        return " ".join((value or "").split())
