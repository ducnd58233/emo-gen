from typing import List, Dict, Any, Optional
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from core.logger import get_logger
from core.config import config
from storage.database.models.emoji import CrawlEmoji, Status
from extraction.interface import ICrawler
from extraction.strategies.discord import (
    DiscordPaginationStrategy,
    DiscordSeleniumFetchStrategy,
    DiscordTopicDiscoveryStrategy,
    DiscordEmojiExtractionStrategy
)
from messaging.kafka.producer import KafkaMessageProducer
from storage.database.repositories.emoji_repository import CrawlEmojiRepository
from messaging.schema import Message

logger = get_logger("extraction.sources.discord")


class DiscordEmojiCrawler(ICrawler):
    """Discord emoji crawler using the enhanced ICrawler interface"""

    ROOT_URL = 'https://discords.com/'

    def __init__(self, headless: bool = config.crawler.headless, max_workers: int = 4, batch_size: int = 20, max_pages: int = 1000):
        super().__init__(max_workers=max_workers, batch_size=batch_size)
        self.headless = headless
        self.driver = None
        self.initialize_driver()

        self.fetch_strategy = DiscordSeleniumFetchStrategy(self.driver)
        self.discovery_strategy = DiscordTopicDiscoveryStrategy()
        self.extraction_strategy = DiscordEmojiExtractionStrategy()
        self.pagination_strategy = DiscordPaginationStrategy(
            max_pages=max_pages)

        self.repo = CrawlEmojiRepository()
        self.producer = KafkaMessageProducer()

    def initialize_driver(self):
        """Initialize or re-initialize the WebDriver"""
        try:
            if self.driver:
                self.driver.quit()

            opts = webdriver.ChromeOptions()
            if self.headless:
                opts.add_argument('--headless=new')
                opts.add_argument('--disable-gpu')
                opts.add_argument('--no-sandbox')
                opts.add_argument('--disable-dev-shm-usage')

            opts.add_argument('--disable-extensions')
            opts.add_argument('--disable-infobars')
            opts.add_argument('--window-size=1920,1080')
            opts.add_argument('--disk-cache-size=52428800')  # 50MB cache

            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=opts)
            self.driver.set_page_load_timeout(30)

            logger.info("Chrome WebDriver initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing ChromeDriver: {e}")
            raise

    def fetch(self, url: str) -> str:
        """Fetch HTML content using the fetch strategy with retry mechanism"""
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                return self.fetch_strategy.fetch(url)
            except Exception as e:
                retry_count += 1
                logger.warning(
                    f"Fetch attempt {retry_count} failed for {url}: {e}")

                if retry_count >= max_retries:
                    logger.error(
                        f"Failed to fetch {url} after {max_retries} attempts")
                    raise

                self.initialize_driver()
                self.fetch_strategy = DiscordSeleniumFetchStrategy(self.driver)
                
        return ""

    def discover_sources(self, url: str) -> List[str]:
        """Discover topics to crawl with duplicate filtering"""
        if self.is_processed(url):
            logger.info(f"URL already processed: {url}")
            return []
        
        self.mark_processed(url)
        
        if not url.endswith('/emoji-list'):
            return [url]

        html = self.fetch(url)
        urls = self.discovery_strategy.discover(html, self.ROOT_URL)

        new_urls = [u for u in urls if not self.is_processed(u)]
        logger.info(f"Discovered {len(new_urls)} new URLs from {url}")

        return new_urls
    
    def get_pagination_urls(self, url: str) -> List[str]:
        """Get pagination URLs for a topic page"""
        # Fetch the page content
        html = self.fetch(url)

        # Use pagination strategy to extract all pagination URLs
        return self.pagination_strategy.get_pagination_urls(html, url, self.max_pages)


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
        
        self.mark_processed(url)


        logger.info(f"Extracted {len(items)} emojis from {url}")
        return items

    def post_process(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Save to database and publish to Kafka in batches"""
        if not results:
            return results

        # Convert to model objects
        emoji_objects = [
            CrawlEmoji(
                name=item['name'],
                image_url=item['image_url'],
                status=Status.CRAWLED
            )
            for item in results
        ]

        try:
            # Save to database with batch upsert
            saved_emojis = self.repo.bulk_upsert(emoji_objects, ['name'])

            # Process in batches for Kafka publishing
            for i in range(0, len(saved_emojis), self.batch_size):
                batch = saved_emojis[i:i+self.batch_size]

                # Filter new emojis only
                new_emojis = [
                    emoji for emoji in batch if emoji.status == Status.CRAWLED]

                if new_emojis:
                    emoji_data = [
                        {
                            "id": emoji.id,
                            "name": emoji.name,
                            "image_url": emoji.image_url,
                            "status": emoji.status.name
                        }
                        for emoji in new_emojis
                    ]

                    batch_message = Message(
                        type="emoji_batch",
                        payload={"emojis": emoji_data}
                    )

                    # Publish to Kafka
                    self.producer.produce(
                        config.kafka.emoji_topic, batch_message)
                    logger.info(
                        f"Published batch of {len(new_emojis)} new emojis to Kafka")

            logger.info(f"Completed processing {len(saved_emojis)} emojis")
            return results

        except Exception as e:
            logger.error(f"Error in post_process: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []

    def cleanup(self):
        """Clean up resources"""
        if hasattr(self, 'driver') and self.driver:
            try:
                self.driver.quit()
                logger.info("WebDriver closed successfully")
            except Exception as e:
                logger.error(f"Error closing WebDriver: {e}")

        super().cleanup()
