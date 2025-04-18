import re
from time import sleep
from typing import List, Dict, Any
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from core.logger import get_logger
from core.config import config
from extraction.strategies.base import SourceDiscoveryStrategy, ItemExtractionStrategy, FetchStrategy

logger = get_logger("extraction.strategies.discord")


class DiscordSeleniumFetchStrategy(FetchStrategy):
    """Selenium-based fetch strategy for Discord"""

    def __init__(self, driver: webdriver.Chrome, wait_time: int = config.crawler.wait_time,
                 sleep_time: int = config.crawler.sleep_time):
        self.driver = driver
        self.wait_time = wait_time
        self.sleep_time = sleep_time

    def fetch(self, url: str) -> str:
        """Fetch HTML content using Selenium"""
        logger.info(f"Fetching page: {url}")
        self.driver.get(url)

        # For the main page, we need to click "More tags"
        if url.endswith('emoji-list'):
            try:
                more_btn = WebDriverWait(self.driver, self.wait_time).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, "//button[normalize-space()='More tags']"))
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
        soup = BeautifulSoup(html, 'html.parser')
        results = []

        # Extract tag links
        tag_hrefs = {a['href'] for a in soup.find_all(
            'a', href=re.compile(r'^/emoji-list/tag/'))}

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
        soup = BeautifulSoup(html, 'html.parser')
        results = []
        
        emoji_hrefs = {(a['href'], a['download']) for a in soup.find_all('a', href=re.compile(r'^/emoji-list/download\?id='))}

        for href, name in emoji_hrefs:
            emoji_url = f'{base_url.rstrip("/")}{href}'
            emoji_data = {
                'image_url': emoji_url,
                'name': name
            }
            results.append(emoji_data)

        logger.info(f"Extracted {len(results)} emojis")
        return results

