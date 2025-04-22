from concurrent.futures import ThreadPoolExecutor, as_completed
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set
from collections import deque
from contextlib import contextmanager

from core.logger import get_logger

logger = get_logger("extraction.interface")


class ICrawler(ABC):
    """Base crawler interface with template method pattern and batch processing"""

    # Default batch size for processing
    DEFAULT_BATCH_SIZE = 100
    DEFAULT_MAX_PAGES = 1000

    def __init__(self, max_workers: int = 4, batch_size: int = None, max_pages: int = None):
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.results_queue = deque()
        self.processed_urls = set()  # Track processed URLs
        self.batch_size = batch_size or self.DEFAULT_BATCH_SIZE
        self.max_pages = max_pages or self.DEFAULT_MAX_PAGES

    def crawl(self, url: str) -> List[Dict[str, Any]]:
        """
        Template method defining the crawling workflow with batch processing.

        Steps:
        1. Discover sources (topics, categories, etc.)
        2. Process sources in batches
        3. For each batch, extract items in parallel
        4. Process and save results in batches

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

            logger.info(f"Discovered {len(sources)} topics/sources to process")

            # Step 2: Process each source (topic)
            for source_url in sources:
                logger.info(f"Processing source: {source_url}")

                # Step 2a: Get all pagination URLs for this source
                pagination_urls = self.get_pagination_urls(source_url)

                if not pagination_urls:
                    # If no pagination detected, process as single page
                    pagination_urls = [source_url]

                logger.info(
                    f"Found {len(pagination_urls)} pages for source {source_url}")

                # Step 2b: Process each page
                for page_url in pagination_urls:
                    if self.is_processed(page_url):
                        logger.info(
                            f"Skipping already processed page: {page_url}")
                        continue

                    # Extract items from this page
                    page_results = self.extract_items(page_url)

                    # Mark as processed
                    self.mark_processed(page_url)

                    # If we found results, process them
                    if page_results:
                        # Process in batches for efficiency
                        for i in range(0, len(page_results), self.batch_size):
                            batch = page_results[i:i+self.batch_size]
                            processed_batch = self.post_process(batch)
                            all_results.extend(processed_batch)

            logger.info(
                f"Crawl completed, processed {len(self.processed_urls)} URLs")
            return all_results

        except Exception as e:
            logger.error(f"Error during crawl: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return all_results
        finally:
            self.cleanup()

    def get_pagination_urls(self, url: str) -> List[str]:
        """
        Get pagination URLs for a source URL.
        Default implementation returns empty list.

        Args:
            url: Source URL

        Returns:
            List of pagination URLs
        """
        return []

    def process_batch(self, urls: List[str]) -> List[Dict[str, Any]]:
        """
        Process a batch of URLs in parallel

        Args:
            urls: List of URLs to process

        Returns:
            Combined results from all URLs
        """
        all_results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all URLs for processing
            future_to_url = {executor.submit(
                self.extract_items, url): url for url in urls}

            # Collect results as they complete
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    results = future.result()
                    if results:
                        all_results.extend(results)
                        logger.info(f"Successfully processed {url}")
                except Exception as e:
                    logger.error(f"Error processing {url}: {e}")

        return all_results

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

    @abstractmethod
    def fetch(self, url: str) -> str:
        """Fetch raw content from the given URL."""
        pass

    @abstractmethod
    def extract_items(self, url: str) -> List[Dict[str, Any]]:
        """Extract items from a specific URL."""
        pass

    def discover_sources(self, url: str) -> List[str]:
        """
        Discover sources to crawl (e.g., topics, categories).
        Default implementation returns empty list.
        Subclasses should override this method if they need to discover sources.
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
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=False)

    def __del__(self):
        """Clean up resources"""
        self.cleanup()
