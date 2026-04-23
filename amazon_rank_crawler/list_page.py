from __future__ import annotations

import random
import re
from typing import Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Tag
from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

from .browser import BrowserManager
from .config import CrawlerSettings
from .models import AmazonProductRecord
from .utils import DelayController

PRODUCT_URL_RE = re.compile(r"/(?:dp|gp/product)/([A-Z0-9]{10})", re.IGNORECASE)
RANK_RE = re.compile(r"#\s?\d{1,3}(?:,\d{3})*")
PRICE_RE = re.compile(r"[$€£]\s?\d[\d,]*(?:\.\d{2})?")
REVIEW_COUNT_RE = re.compile(r"^\d[\d,]*(?:\.\d+)?$")

WAIT_PRODUCT_SELECTORS = (
    "div.zg-grid-general-faceout",
    "div.p13n-sc-uncoverable-faceout",
    "div[id^='p13n-asin-index-']",
    "div[data-asin]",
    "a[href*='/dp/']",
)

LIST_CONTAINER_HINTS = (
    "zg-grid-general-faceout",
    "p13n-sc-uncoverable-faceout",
    "zg-item-immersion",
    "p13n-asin",
    "p13n-sc-list-item",
)

TITLE_SELECTORS = (
    "div[class*='line-clamp']",
    "span.a-size-base-plus",
    "span.a-size-medium",
    "span.a-size-base",
    "a[title]",
)

PRICE_SELECTORS = (
    "span.p13n-sc-price",
    "span.a-price span.a-offscreen",
    "span.a-color-price",
    "span[class*='price']",
)

RATING_SELECTORS = (
    "span.a-icon-alt",
    "i.a-icon-star-small span.a-icon-alt",
    "i.a-icon-star-mini span.a-icon-alt",
)

REVIEW_COUNT_SELECTORS = (
    "a.a-size-small",
    "span.a-size-small",
    "a[href*='customerReviews'] span",
    "a[href*='#customerReviews'] span",
)


class AmazonListPageCrawler:
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

    async def crawl(self, target_count: int = 100) -> list[AmazonProductRecord]:
        context = await self.browser_manager.new_context()
        page = await self.browser_manager.new_page(context)

        try:
            await self.browser_manager.safe_goto(page, self.settings.start_url)
            await self._post_navigation_stabilize(page)
            await self._dismiss_popups(page)
            await self._wait_for_product_cards(page)
            await self._scroll_until_loaded(page, target_count=target_count)
            html = await page.content()
            records = self.parse_listing_html(html)
            records = records[:target_count]
            self.logger.info("List page parse completed, %s records extracted", len(records))
            return records
        except Exception:
            await self.browser_manager.capture_debug_snapshot(page, "list_page_error")
            raise
        finally:
            await context.close()

    async def _post_navigation_stabilize(self, page: Page) -> None:
        await self.delay.sleep_random(1.2, 2.6, "initial page settle")
        try:
            await page.wait_for_load_state("networkidle", timeout=8_000)
        except PlaywrightTimeoutError:
            self.logger.info("Network idle wait timed out, continue with DOM-based checks")

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
                    await self.delay.sleep_random(0.5, 1.2, f"dismiss popup {selector}")
            except Exception:
                continue

    async def _wait_for_product_cards(self, page: Page) -> None:
        for selector in WAIT_PRODUCT_SELECTORS:
            try:
                await page.locator(selector).first.wait_for(timeout=8_000)
                self.logger.info("Detected list container via selector: %s", selector)
                return
            except Exception:
                continue
        raise RuntimeError("No Amazon ranking product cards were detected on the list page")

    async def _count_rendered_products(self, page: Page) -> int:
        try:
            count = await page.evaluate(
                """
                () => {
                  const seen = new Set();
                  const anchors = Array.from(
                    document.querySelectorAll("a[href*='/dp/'], a[href*='/gp/product/']")
                  );
                  for (const anchor of anchors) {
                    const href = anchor.getAttribute('href') || '';
                    const match = href.match(/\\/(?:dp|gp\\/product)\\/([A-Z0-9]{10})/i);
                    if (match) {
                      seen.add(match[1].toUpperCase());
                    }
                  }
                  return seen.size || anchors.length;
                }
                """
            )
            return int(count or 0)
        except Exception:
            return 0

    async def _scroll_until_loaded(self, page: Page, target_count: int = 100) -> None:
        previous_count = 0
        idle_rounds = 0

        for round_index in range(1, 90):
            viewport_height = await page.evaluate("window.innerHeight")
            scroll_distance = random.randint(
                max(450, int(viewport_height * 0.60)),
                max(650, int(viewport_height * 1.35)),
            )

            await page.mouse.move(300 + round_index % 80, 220 + round_index % 150)
            await page.mouse.wheel(0, scroll_distance)
            await self.delay.sleep_random(
                self.settings.min_list_scroll_pause,
                self.settings.max_list_scroll_pause,
                f"list scroll #{round_index}",
            )

            if round_index % 6 == 0:
                await page.mouse.wheel(0, -120)
                await self.delay.sleep_random(0.3, 0.8, f"micro bounce #{round_index}")

            current_count = await self._count_rendered_products(page)
            at_bottom = await page.evaluate(
                "() => window.scrollY + window.innerHeight >= document.body.scrollHeight - 10"
            )

            if current_count > previous_count:
                self.logger.info(
                    "List page rendered products increased from %s to %s",
                    previous_count,
                    current_count,
                )
                previous_count = current_count
                idle_rounds = 0
            else:
                idle_rounds += 1

            if current_count >= target_count and idle_rounds >= 2:
                self.logger.info("Reached target count %s with stable DOM, stop scrolling", current_count)
                break

            if at_bottom and idle_rounds >= 5:
                self.logger.info(
                    "Reached page bottom with no further growth after %s idle rounds", idle_rounds
                )
                break

        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await self.delay.sleep_random(1.0, 2.2, "final list settle")

    def parse_listing_html(self, html: str) -> list[AmazonProductRecord]:
        soup = BeautifulSoup(html, "lxml")
        containers = self._collect_product_containers(soup)
        records: list[AmazonProductRecord] = []
        seen_keys: set[str] = set()

        for index, container in enumerate(containers, start=1):
            try:
                record = self._parse_product_container(container)
                if not any((record.asin, record.url, record.title)):
                    continue

                record.source_category_url = self.settings.start_url
                dedupe_key = record.asin or record.url or f"{record.rank}|{record.title}"
                if dedupe_key in seen_keys:
                    continue

                seen_keys.add(dedupe_key)
                records.append(record)
            except Exception as exc:
                self.logger.warning("Failed to parse list item #%s: %s", index, exc)
                continue

        records.sort(key=self._sort_key_for_rank)
        return records

    def _collect_product_containers(self, soup: BeautifulSoup) -> list[Tag]:
        static_candidates: list[Tag] = []
        for selector in (
            "div.zg-grid-general-faceout",
            "div.p13n-sc-uncoverable-faceout",
            "div[id^='p13n-asin-index-']",
            "div[data-asin]",
            "li.zg-item-immersion",
        ):
            static_candidates.extend(soup.select(selector))

        inferred_candidates: list[Tag] = []
        for anchor in soup.select("a[href*='/dp/'], a[href*='/gp/product/']"):
            if not isinstance(anchor, Tag):
                continue
            container = self._find_product_container(anchor)
            if container is not None:
                inferred_candidates.append(container)

        merged: list[Tag] = []
        seen_markers: set[str] = set()
        for candidate in static_candidates + inferred_candidates:
            marker = self._container_marker(candidate)
            if marker in seen_markers:
                continue
            seen_markers.add(marker)
            merged.append(candidate)

        return merged

    def _find_product_container(self, node: Tag) -> Optional[Tag]:
        current: Optional[Tag] = node
        depth = 0
        while current is not None and depth < 8:
            if self._looks_like_product_container(current):
                return current
            parent = current.parent
            current = parent if isinstance(parent, Tag) else None
            depth += 1
        return None

    def _looks_like_product_container(self, node: Tag) -> bool:
        class_names = " ".join(node.get("class", []))
        node_id = node.get("id", "")
        data_asin = self._clean_text(node.get("data-asin", ""))

        if data_asin and len(data_asin) == 10:
            return True

        if any(hint in class_names for hint in LIST_CONTAINER_HINTS):
            return True

        if node_id.startswith("p13n-asin-index-"):
            return True

        text = self._clean_text(node.get_text(" ", strip=True))
        has_link = node.find("a", href=lambda href: bool(href and PRODUCT_URL_RE.search(href)))
        has_image = node.find("img") is not None
        return bool(has_link and has_image and (RANK_RE.search(text) or PRICE_RE.search(text)))

    def _parse_product_container(self, container: Tag) -> AmazonProductRecord:
        url = self._extract_product_url(container)
        asin = self._extract_asin(container, url)
        title = self._extract_title(container)
        main_image_url = self._extract_image_url(container)
        rank = self._extract_rank(container)
        price = self._extract_price(container)
        rating = self._extract_rating(container)
        review_count = self._extract_review_count(container, rating=rating)

        return AmazonProductRecord(
            rank=rank,
            title=title,
            url=url,
            asin=asin,
            main_image_url=main_image_url,
            price=price,
            rating=rating,
            review_count=review_count,
        )

    def _extract_product_url(self, container: Tag) -> str:
        for anchor in container.select("a[href*='/dp/'], a[href*='/gp/product/']"):
            href = anchor.get("href", "")
            asin = self._extract_asin_from_string(href)
            if asin:
                return urljoin(self.amazon_base, f"/dp/{asin}")
            if href:
                return urljoin(self.amazon_base, href.split("?")[0])
        return ""

    def _extract_asin(self, container: Tag, url: str) -> str:
        for candidate in (container.get("data-asin", ""), url):
            asin = self._extract_asin_from_string(candidate)
            if asin:
                return asin

        for descendant in container.select("[data-asin]"):
            asin = self._extract_asin_from_string(descendant.get("data-asin", ""))
            if asin:
                return asin
        return ""

    def _extract_title(self, container: Tag) -> str:
        candidates: list[str] = []

        for selector in TITLE_SELECTORS:
            for node in container.select(selector):
                text = self._clean_text(node.get_text(" ", strip=True))
                if self._is_probable_title(text):
                    candidates.append(text)

        for anchor in container.select("a[href*='/dp/'], a[href*='/gp/product/']"):
            text = self._clean_text(anchor.get("title", "") or anchor.get_text(" ", strip=True))
            if self._is_probable_title(text):
                candidates.append(text)

        img = container.find("img")
        if isinstance(img, Tag):
            alt_text = self._clean_text(img.get("alt", ""))
            if self._is_probable_title(alt_text):
                candidates.append(alt_text)

        return max(candidates, key=len, default="")

    def _extract_image_url(self, container: Tag) -> str:
        best_image_url = ""
        best_score = -1

        for image in container.find_all("img"):
            if not isinstance(image, Tag):
                continue
            candidate = self._normalize_image_url(image)
            if not candidate:
                continue

            score = 0
            alt_text = self._clean_text(image.get("alt", ""))
            if alt_text:
                score += len(alt_text)
            src = image.get("src", "") or ""
            if "images" in src.lower():
                score += 20

            if score > best_score:
                best_score = score
                best_image_url = candidate

        return best_image_url

    def _extract_rank(self, container: Tag) -> str:
        rank_node = container.select_one("span.zg-bdg-text")
        if rank_node is not None:
            text = self._clean_text(rank_node.get_text(" ", strip=True))
            match = RANK_RE.search(text)
            if match:
                return match.group(0).replace(" ", "")

        text = self._clean_text(container.get_text(" ", strip=True))
        match = RANK_RE.search(text)
        if match:
            return match.group(0).replace(" ", "")
        return ""

    def _extract_price(self, container: Tag) -> str:
        for selector in PRICE_SELECTORS:
            for node in container.select(selector):
                text = self._clean_text(node.get_text(" ", strip=True))
                match = PRICE_RE.search(text)
                if match:
                    return match.group(0).replace(" ", "")

        for text_node in container.stripped_strings:
            text = self._clean_text(text_node)
            if "stars" in text.lower():
                continue
            match = PRICE_RE.search(text)
            if match:
                return match.group(0).replace(" ", "")
        return ""

    def _extract_rating(self, container: Tag) -> str:
        for selector in RATING_SELECTORS:
            for node in container.select(selector):
                text = self._clean_text(node.get_text(" ", strip=True))
                if "out of 5 stars" in text.lower():
                    return text

        for text_node in container.stripped_strings:
            text = self._clean_text(text_node)
            if "out of 5 stars" in text.lower():
                return text
        return ""

    def _extract_review_count(self, container: Tag, rating: str = "") -> str:
        for selector in REVIEW_COUNT_SELECTORS:
            for node in container.select(selector):
                text = self._clean_text(node.get_text(" ", strip=True))
                if self._looks_like_review_count(text):
                    return text

        text_parts = [self._clean_text(item) for item in container.stripped_strings]
        if rating and rating in text_parts:
            rating_index = text_parts.index(rating)
            for candidate in text_parts[rating_index + 1 : rating_index + 5]:
                if self._looks_like_review_count(candidate):
                    return candidate
        return ""

    def _normalize_image_url(self, image: Tag) -> str:
        srcset = image.get("srcset", "")
        if srcset:
            parts = [chunk.strip() for chunk in srcset.split(",") if chunk.strip()]
            if parts:
                last = parts[-1].split(" ")[0].strip()
                if last:
                    return last

        for attr in ("src", "data-src", "data-old-hires"):
            value = self._clean_text(image.get(attr, ""))
            if value and not value.endswith(".gif"):
                return value
        return ""

    def _is_probable_title(self, text: str) -> bool:
        if len(text) < 10:
            return False
        if "out of 5 stars" in text.lower():
            return False
        if RANK_RE.fullmatch(text):
            return False
        if PRICE_RE.fullmatch(text):
            return False
        if REVIEW_COUNT_RE.fullmatch(text):
            return False
        if text.lower().startswith("visit the"):
            return False
        return True

    def _looks_like_review_count(self, text: str) -> bool:
        if not text:
            return False
        if not REVIEW_COUNT_RE.fullmatch(text):
            return False
        return "," in text or text.isdigit()

    def _extract_asin_from_string(self, value: str) -> str:
        if not value:
            return ""
        match = PRODUCT_URL_RE.search(value)
        if match:
            return match.group(1).upper()

        value = value.strip().upper()
        if len(value) == 10 and value.isalnum():
            return value
        return ""

    def _container_marker(self, container: Tag) -> str:
        asin = self._extract_asin(container, "")
        if asin:
            return f"asin:{asin}"

        product_url = self._extract_product_url(container)
        if product_url:
            return f"url:{product_url}"

        rank = self._extract_rank(container)
        title = self._extract_title(container)
        return f"fallback:{rank}:{title[:60]}"

    def _sort_key_for_rank(self, record: AmazonProductRecord) -> int:
        if not record.rank:
            return 10_000
        digits = re.sub(r"[^\d]", "", record.rank)
        return int(digits) if digits else 10_000

    def _build_amazon_base_url(self, url: str) -> str:
        parsed = urlparse(url)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
        return "https://www.amazon.com"

    def _clean_text(self, value: str) -> str:
        return " ".join((value or "").split())
