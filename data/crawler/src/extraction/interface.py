from abc import ABC, abstractmethod
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List

from core.logger import get_logger

logger = get_logger("extraction.interface")

DEFAULT_BATCH_SIZE = 100
DEFAULT_MAX_PAGES = 1000
DEFAULT_MAX_WORKERS = 4


class ICrawler(ABC):
    """Base crawler interface with template method pattern and parallel source processing"""

    def __init__(
        self,
        max_workers: int = DEFAULT_MAX_WORKERS,
        batch_size: int = DEFAULT_BATCH_SIZE,
        max_pages: int = DEFAULT_MAX_PAGES,
    ):
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.max_pages = max_pages
        self.processed_urls = set()
        self.results_queue = deque()

    def crawl(self, url: str) -> List[Dict[str, Any]]:
        """
        Template method defining the crawling workflow with parallel source processing.

        Steps:
        1. Discover all sources (topics, categories, etc.)
        2. Process sources in parallel - each thread handles a complete source
        3. Each source processing includes pagination and item extraction
        4. Combine and return all results

        Args:
            url: The starting URL to crawl

        Returns:
            List of extracted items
        """
        logger.info(f"Starting crawl from {url}")
        all_results = []

        try:
            # Step 1: Discover sources to crawl
            sources = self.discover_sources(url)

            if not sources:
                logger.warning(f"No sources discovered from {url}")
                return []

            logger.info(f"Discovered {len(sources)} sources to process")

            # Step 2: Process sources in parallel
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_source = {
                    executor.submit(self.process_source, source_url): source_url
                    for source_url in sources
                }

                for future in as_completed(future_to_source):
                    source_url = future_to_source[future]
                    try:
                        source_results = future.result()
                        if source_results:
                            all_results.extend(source_results)
                            logger.info(
                                f"Successfully processed source {source_url} with {len(source_results)} items"
                            )
                    except Exception as e:
                        logger.error(f"Error processing source {source_url}: {e}")
                        import traceback

                        logger.error(traceback.format_exc())

            logger.info(
                f"Crawl completed, processed {len(self.processed_urls)} URLs, found {len(all_results)} items"
            )
            return all_results

        except Exception as e:
            logger.error(f"Error during crawl: {e}")
            import traceback

            logger.error(traceback.format_exc())
            return all_results
        finally:
            self.cleanup()

    def process_source(self, source_url: str) -> List[Dict[str, Any]]:
        """
        Process a single source completely including all its pagination.

        Args:
            source_url: The source URL to process

        Returns:
            List of items extracted from this source
        """
        source_results = []
        logger.info(f"Processing source: {source_url}")

        pagination_urls = self.get_pagination_urls(source_url)

        if not pagination_urls:
            pagination_urls = [source_url]

        logger.info(f"Found {len(pagination_urls)} pages for source {source_url}")

        for page_url in pagination_urls:
            if self.is_processed(page_url):
                logger.info(f"Skipping already processed page: {page_url}")
                continue

            page_results = self.extract_items(page_url)

            self.mark_processed(page_url)

            if page_results:
                for i in range(0, len(page_results), self.batch_size):
                    batch = page_results[i : i + self.batch_size]
                    processed_batch = self.post_process(batch)
                    source_results.extend(processed_batch)
            else:
                if self.should_stop_on_empty_page():
                    logger.info(
                        f"Empty page found at {page_url}, stopping pagination for this source"
                    )
                    break

        logger.info(
            f"Completed processing source {source_url} with {len(source_results)} items"
        )
        return source_results

    def mark_processed(self, url: str) -> None:
        """
        Mark URL as processed to avoid duplicates

        Args:
            url: URL to mark as processed
        """
        self.processed_urls.add(url)

    def is_processed(self, url: str) -> bool:
        """
        Check if URL has already been processed

        Args:
            url: URL to check

        Returns:
            True if already processed, False otherwise
        """
        return url in self.processed_urls

    def should_stop_on_empty_page(self) -> bool:
        """
        Determine if pagination should stop when an empty page is encountered.
        Default implementation returns True.

        Returns:
            True if pagination should stop on empty page, False otherwise
        """
        return True

    @abstractmethod
    def fetch(self, url: str) -> str:
        """Fetch raw content from the given URL."""

    @abstractmethod
    def extract_items(self, url: str) -> List[Dict[str, Any]]:
        """Extract items from a specific URL."""

    def discover_sources(self, url: str) -> List[str]:
        """
        Discover sources to crawl (e.g., topics, categories).
        Default implementation returns the input URL as the only source.
        Subclasses should override this method if they need to discover multiple sources.
        """
        return [url]

    def get_pagination_urls(self, url: str) -> List[str]:
        """
        Get pagination URLs for a source URL.
        Default implementation returns empty list.
        Subclasses should override this method to implement pagination.

        Args:
            url: Source URL

        Returns:
            List of pagination URLs
        """
        return []

    def post_process(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Post-process the results.
        Default implementation returns the results as-is.
        Subclasses can override this method for custom processing.
        """
        return results

    def cleanup(self) -> None:
        """
        Clean up resources.
        Called when crawling is complete or when an error occurs.
        """

    def __del__(self):
        """Clean up resources"""
        self.cleanup()
