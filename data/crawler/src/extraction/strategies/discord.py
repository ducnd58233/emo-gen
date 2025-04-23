import re
from time import sleep
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from bs4 import BeautifulSoup
from core.config import config
from core.logger import get_logger
from extraction.strategies.base import (
    FetchStrategy,
    ItemExtractionStrategy,
    PaginationStrategy,
    SourceDiscoveryStrategy,
)
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

logger = get_logger("extraction.strategies.discord")


class DiscordSeleniumFetchStrategy(FetchStrategy):
    """Selenium-based fetch strategy for Discord"""

    def __init__(
        self,
        driver: webdriver.Chrome,
        wait_time: int = config.crawler.wait_time,
        sleep_time: int = config.crawler.sleep_time,
    ):
        self.driver = driver
        self.wait_time = wait_time
        self.sleep_time = sleep_time

    def fetch(self, url: str) -> str:
        """Fetch HTML content using Selenium"""
        logger.info(f"Fetching page: {url}")
        self.driver.get(url)

        # For the main page, we need to click "More tags"
        if url.endswith("emoji-list"):
            try:
                more_btn = WebDriverWait(self.driver, self.wait_time).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, "//button[normalize-space()='More tags']")
                    )
                )
                more_btn.click()
                sleep(self.sleep_time)
            except Exception as e:
                logger.warning(f"Could not click 'More tags' button: {e}")

        sleep(self.sleep_time)
        return self.driver.page_source


class DiscordTopicDiscoveryStrategy(SourceDiscoveryStrategy):
    """Strategy for discovering emoji topics on Discord"""

    def discover(self, html: str, base_url: str) -> List[str]:
        """Discover emoji topics from the main page"""
        soup = BeautifulSoup(html, "html.parser")
        results = []

        # Extract tag links
        tag_hrefs = {
            a["href"] for a in soup.find_all("a", href=re.compile(r"^/emoji-list/tag/"))
        }

        # Convert to full URLs and add page parameter
        for href in tag_hrefs:
            topic_url = f'{base_url.rstrip("/")}{href}'
            results.append(topic_url)

        logger.info(f"Discovered {len(results)} topics")
        return results


class DiscordEmojiExtractionStrategy(ItemExtractionStrategy):
    """Strategy for extracting emoji items from Discord topics"""

    def extract(self, html: str, base_url: str) -> List[Dict[str, Any]]:
        """Extract emoji data from a topic page"""
        soup = BeautifulSoup(html, "html.parser")
        results = []

        emoji_hrefs = {
            (a["href"], a["download"])
            for a in soup.find_all("a", href=re.compile(r"^/emoji-list/download\?id="))
        }

        for href, name in emoji_hrefs:
            emoji_url = f'{base_url.rstrip("/")}{href}'
            emoji_data = {"image_url": emoji_url, "name": name}
            results.append(emoji_data)

        logger.info(f"Extracted {len(results)} emojis")
        return results


class DiscordPaginationStrategy(PaginationStrategy):
    """Strategy for handling pagination on Discord emoji pages"""

    def __init__(self, max_pages: int = 20):
        """
        Initialize the pagination strategy.

        Args:
            max_pages: Maximum number of pages to process (default: 20)
        """
        self.max_pages = max_pages

    def get_next_page_url(self, html: str, current_url: str) -> Optional[str]:
        """
        Get URL for the next page.

        Args:
            html: Current page HTML
            current_url: Current page URL

        Returns:
            Next page URL or None if no next page exists
        """
        # Parse the current URL to extract page number
        parsed_url = urlparse(current_url)
        query_params = parse_qs(parsed_url.query)

        # Get current page or default to 1
        current_page = int(query_params.get("page", ["1"])[0])
        next_page = current_page + 1

        # Check if next page exists in the HTML
        soup = BeautifulSoup(html, "html.parser")
        pagination_links = soup.find_all("a", href=re.compile(r"page=\d+"))

        if not pagination_links:
            # No pagination links found, check if content exists
            if not self._has_content(html):
                return None

            # If content exists but no pagination yet, create page 2 URL
            if current_page == 1:
                query_params["page"] = ["2"]
                new_query = urlencode(query_params, doseq=True)
                parts = list(parsed_url)
                parts[4] = new_query
                return urlparse("").geturl()
            return None

        max_page = 1
        for link in pagination_links:
            href = link["href"]
            page_match = re.search(r"page=(\d+)", href)
            if page_match:
                page_num = int(page_match.group(1))
                max_page = max(max_page, page_num)

        if next_page <= max_page and next_page <= self.max_pages:
            query_params["page"] = [str(next_page)]
            new_query = urlencode(query_params, doseq=True)
            parts = list(parsed_url)
            parts[4] = new_query
            return urlunparse(tuple(parts))

        return None

    def get_pagination_urls(
        self, html: str, base_url: str, max_pages: int = None
    ) -> List[str]:
        """
        Generate URLs for all pages from 1 to max_pages.
        We will check for content and stop crawling when a page is empty.
        """
        if not max_pages:
            max_pages = self.max_pages

        # Base URL with no page parameter
        parsed_url = urlparse(base_url)
        query_params = parse_qs(parsed_url.query)

        # Remove page parameter if present, to get clean base URL
        if "page" in query_params:
            del query_params["page"]

        parts = list(parsed_url)
        base_query = urlencode(query_params, doseq=True) if query_params else ""

        # Generate URLs for all pages from 1 to max_pages
        page_urls = []
        for page_num in range(1, max_pages + 1):
            # Add page parameter
            if base_query:
                # If other parameters exist
                page_query = f"{base_query}&page={page_num}"
            else:
                page_query = f"page={page_num}"

            # Update query part
            parts_copy = parts.copy()
            parts_copy[4] = page_query

            # Convert back to URL
            page_url = urlunparse(tuple(parts_copy))
            page_urls.append(page_url)

        logger.info(f"Generated {len(page_urls)} pagination URLs from 1 to {max_pages}")
        return page_urls

    def is_valid_page(self, html: str) -> bool:
        """
        Check if the page contains valid content (not empty, not error).

        Args:
            html: HTML content

        Returns:
            True if page has valid content
        """
        return self._has_content(html)

    def _has_content(self, html: str) -> bool:
        """
        Check if the page contains emoji content.

        Args:
            html: HTML content

        Returns:
            True if page has emoji content
        """
        soup = BeautifulSoup(html, "html.parser")

        # Check for download links which indicate emojis
        emoji_links = soup.find_all("a", href=re.compile(r"^/emoji-list/download\?id="))

        return len(emoji_links) > 0

    def _extract_page_number(self, url: str) -> int:
        """Extract page number from URL for sorting"""
        match = re.search(r"page=(\d+)", url)
        if match:
            return int(match.group(1))
        return 1
