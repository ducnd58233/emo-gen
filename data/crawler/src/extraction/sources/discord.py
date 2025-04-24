from typing import Any, Dict, List

from core.config import config
from core.decorator import lazy_property, retry, timer
from core.logger import get_logger
from extraction.interface import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_MAX_PAGES,
    DEFAULT_MAX_WORKERS,
    ICrawler,
)
from extraction.strategies.discord import (
    DiscordEmojiExtractionStrategy,
    DiscordPaginationStrategy,
    DiscordSeleniumFetchStrategy,
    DiscordTopicDiscoveryStrategy,
)
from infra.messaging.kafka.producer import KafkaMessageProducer
from infra.messaging.schema import Message
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from storage.database.models.emoji import CrawlEmoji, Status
from storage.database.repositories.crawl_emoji_repository import CrawlEmojiRepository
from webdriver_manager.chrome import ChromeDriverManager

logger = get_logger("extraction.sources.discord")

SOURCE = "discords"


class DiscordEmojiCrawler(ICrawler):
    """Discord emoji crawler using the enhanced ICrawler interface"""

    ROOT_URL = "https://discords.com/"

    def __init__(
        self,
        headless: bool = config.crawler.headless,
        max_workers: int = DEFAULT_MAX_WORKERS,
        batch_size: int = DEFAULT_BATCH_SIZE,
        max_pages: int = DEFAULT_MAX_PAGES,
    ):
        super().__init__(
            max_workers=max_workers, batch_size=batch_size, max_pages=max_pages
        )
        self.headless = headless
        self.driver = None
        self.initialize_driver()
        self.repo = CrawlEmojiRepository()
        self.producer = KafkaMessageProducer()

    @lazy_property
    def fetch_strategy(self):
        return DiscordSeleniumFetchStrategy(
            self.driver,
            wait_time=config.crawler.wait_time,
            sleep_time=config.crawler.sleep_time,
            delay_seconds=config.crawler.request_delay_seconds,
        )

    @lazy_property
    def discovery_strategy(self):
        return DiscordTopicDiscoveryStrategy()

    @lazy_property
    def extraction_strategy(self):
        return DiscordEmojiExtractionStrategy()

    @lazy_property
    def pagination_strategy(self):
        return DiscordPaginationStrategy(max_pages=self.max_pages)

    @timer(log_level="info", name="Initialize WebDriver")
    def initialize_driver(self):
        """Initialize or re-initialize the WebDriver"""
        try:
            if self.driver:
                self.driver.quit()

            opts = webdriver.ChromeOptions()
            if self.headless:
                opts.add_argument("--headless=new")
                opts.add_argument("--disable-gpu")
                opts.add_argument("--no-sandbox")
                opts.add_argument("--disable-dev-shm-usage")

            opts.add_argument("--disable-extensions")
            opts.add_argument("--disable-infobars")
            opts.add_argument("--window-size=1920,1080")
            opts.add_argument("--disk-cache-size=52428800")  # 50MB cache

            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=opts)
            self.driver.set_page_load_timeout(30)

            logger.info("Chrome WebDriver initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing ChromeDriver: {e}")
            raise

    @retry(max_attempts=3, delay=1, backoff=2, exceptions=(Exception,))
    @timer(log_level="debug", name="Fetch URL")
    def fetch(self, url: str) -> str:
        """Fetch HTML content using the fetch strategy with retry mechanism"""
        return self.fetch_strategy.fetch(url)

    @timer(log_level="debug", name="Discover Sources")
    def discover_sources(self, url: str) -> List[str]:
        """Discover topics to crawl with duplicate filtering"""
        if self.is_processed(url):
            logger.info(f"URL already processed: {url}")
            return []

        self.mark_processed(url)

        if not url.endswith("/emoji-list"):
            return [url]

        html = self.fetch(url)
        urls = self.discovery_strategy.discover(html, self.ROOT_URL)

        new_urls = [u for u in urls if not self.is_processed(u)]
        logger.info(f"Discovered {len(new_urls)} new URLs from {url}")

        return new_urls

    @timer(log_level="debug", name="Get Pagination URLs")
    def get_pagination_urls(self, url: str) -> List[str]:
        """Get pagination URLs for a topic page"""
        html = self.fetch(url)
        return self.pagination_strategy.get_pagination_urls(html, url, self.max_pages)

    @timer(log_level="debug", name="Extract Items")
    def extract_items(self, url: str) -> List[Dict[str, Any]]:
        """Extract emoji data from a topic page"""
        if self.is_processed(url):
            logger.info(f"URL already processed for extraction: {url}")
            return []

        html = self.fetch(url)

        if not self.pagination_strategy.is_valid_page(html):
            logger.warning(f"Invalid or empty page: {url}")
            self.mark_processed(url)
            return []

        items = self.extraction_strategy.extract(html, self.ROOT_URL)

        # If no emojis found on this page, mark as empty page
        if not items:
            logger.info(f"No emojis found on page: {url}")

        self.mark_processed(url)
        logger.info(f"Extracted {len(items)} emojis from {url}")
        return items

    @timer(log_level="info", name="Post Process Results")
    def post_process(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Save to database and publish to Kafka in batches"""
        if not results:
            return results

        emoji_objects = [
            CrawlEmoji(
                name=item["name"],
                image_url=item["image_url"],
                status=Status.CRAWLED,
                source=SOURCE,
            )
            for item in results
        ]

        try:
            saved_emojis = self.repo.bulk_upsert(emoji_objects, ["name"])

            for i in range(0, len(saved_emojis), self.batch_size):
                batch = saved_emojis[i : i + self.batch_size]

                new_emojis = [
                    emoji for emoji in batch if emoji.status == Status.CRAWLED
                ]

                if new_emojis:
                    emoji_data = [
                        {
                            "id": emoji.id,
                            "name": emoji.name,
                            "image_url": emoji.image_url,
                            "status": emoji.status.value,
                            "source": emoji.source,
                        }
                        for emoji in new_emojis
                    ]

                    batch_message = Message(
                        type="emoji_batch", payload={"emojis": emoji_data}
                    )

                    self.producer.produce(config.kafka.emoji_topic, batch_message)
                    logger.info(
                        f"Published batch of {len(new_emojis)} new emojis to Kafka"
                    )

            logger.info(f"Completed processing {len(saved_emojis)} emojis")
            return results

        except Exception as e:
            logger.error(f"Error in post_process: {e}")
            import traceback

            logger.error(traceback.format_exc())
            return []

    def should_stop_on_empty_page(self) -> bool:
        """
        Stop pagination when we encounter an empty page.
        For Discord, once a page has no emojis, further pages will also be empty.
        """
        return True

    def cleanup(self):
        """Clean up resources"""
        if hasattr(self, "driver") and self.driver:
            try:
                self.driver.quit()
                logger.info("WebDriver closed successfully")
            except Exception as e:
                logger.error(f"Error closing WebDriver: {e}")

        super().cleanup()
