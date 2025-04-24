from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from core.decorator import request_delay


class SourceDiscoveryStrategy(ABC):
    """Strategy for discovering sources (topics, categories, etc.)"""

    @abstractmethod
    def discover(self, html: str, base_url: str) -> List[str]:
        """
        Discover sources to crawl from the HTML content.

        Args:
            html: The HTML content
            base_url: The base URL for resolving relative URLs

        Returns:
            List of source URLs
        """


class ItemExtractionStrategy(ABC):
    """Strategy for extracting items from a source"""

    @abstractmethod
    def extract(self, html: str, base_url: str) -> List[Dict[str, Any]]:
        """
        Extract items from the HTML content.

        Args:
            html: The HTML content
            base_url: The base URL for resolving relative URLs

        Returns:
            List of extracted items
        """


class FetchStrategy(ABC):
    """Strategy for fetching content from a URL"""

    def __init__(self, delay_seconds: float = 5.0):
        """
        Initialize fetch strategy with configurable delay

        Args:
            delay_seconds: Number of seconds to wait between requests (default: 5.0)
        """
        self.delay_seconds = delay_seconds

    @request_delay()
    @abstractmethod
    def fetch(self, url: str) -> str:
        """
        Fetch content from a URL.
        This method will automatically wait between requests based on the delay_seconds parameter.

        Args:
            url: The URL to fetch

        Returns:
            The fetched content
        """


class PaginationStrategy(ABC):
    """Strategy for handling pagination across multiple pages"""

    @abstractmethod
    def get_next_page_url(self, html: str, current_url: str) -> Optional[str]:
        """
        Extract the URL for the next page from the current page.

        Args:
            html: The HTML content of the current page
            current_url: The URL of the current page

        Returns:
            URL of the next page, or None if there is no next page
        """

    @abstractmethod
    def get_pagination_urls(
        self, html: str, base_url: str, max_pages: int = None
    ) -> List[str]:
        """
        Extract all pagination URLs from the current page.

        Args:
            html: The HTML content
            base_url: The base URL for resolving relative URLs
            max_pages: Maximum number of pages to return (default: all)

        Returns:
            List of all pagination URLs
        """

    def is_valid_page(self, html: str) -> bool:
        """
        Check if the page contains valid content (not empty, not error).

        Args:
            html: The HTML content to check

        Returns:
            True if the page is valid, False otherwise
        """
        return True
