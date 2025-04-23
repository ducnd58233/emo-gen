import abc
import random
from io import BytesIO
from time import sleep, time
from typing import Dict, Optional
from urllib.parse import urlparse

import requests
from core.decorator import retry, timer
from core.logger import get_logger

logger = get_logger("downloader.http")

# Constants
DEFAULT_TIMEOUT = 30
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 1.0
MIN_DELAY_SECONDS = 2.0
MAX_DELAY_SECONDS = 5.0
MAX_RETRY_DELAY = 60.0

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


class RateLimitStrategy(abc.ABC):
    """Abstract strategy for implementing rate limiting"""

    @abc.abstractmethod
    def before_request(self, url: str) -> None:
        """Called before a request is made"""

    @abc.abstractmethod
    def after_request(self, url: str, status_code: int) -> None:
        """Called after a request completes"""


class NoRateLimitStrategy(RateLimitStrategy):
    """Strategy that implements no rate limiting"""

    def before_request(self, url: str) -> None:
        pass

    def after_request(self, url: str, status_code: int) -> None:
        pass


class SimpleRateLimitStrategy(RateLimitStrategy):
    """Simple strategy with fixed delay between requests"""

    def __init__(self, delay_seconds: float = 1.0):
        self.delay_seconds = delay_seconds
        self.last_request_time = 0

    def before_request(self, url: str) -> None:
        current_time = time()
        time_since_last = current_time - self.last_request_time

        if time_since_last < self.delay_seconds:
            sleep_time = self.delay_seconds - time_since_last
            logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
            sleep(sleep_time)

        self.last_request_time = time()

    def after_request(self, url: str, status_code: int) -> None:
        # Update last request time to current time
        self.last_request_time = time()


class DomainBasedRateLimitStrategy(RateLimitStrategy):
    """Advanced rate limiting strategy based on domain with dynamic backoff"""

    def __init__(
        self,
        min_delay: float = MIN_DELAY_SECONDS,
        max_delay: float = MAX_DELAY_SECONDS,
        max_retry_delay: float = MAX_RETRY_DELAY,
    ):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.max_retry_delay = max_retry_delay
        self.domain_info = {}

    def _get_domain_from_url(self, url: str) -> str:
        """Extract domain from URL"""
        return urlparse(url).netloc

    def _get_domain_delay(self, domain: str) -> float:
        """Get appropriate delay for a domain based on its current rate limit state"""
        if domain not in self.domain_info:
            self.domain_info[domain] = {
                "last_request_time": 0,
                "consecutive_429_errors": 0,
            }

        domain_info = self.domain_info[domain]

        # Increase delay if we've seen 429 errors
        if domain_info["consecutive_429_errors"] > 0:
            # Apply exponential backoff based on consecutive errors (cap at 2^4 = 16x)
            multiplier = 2 ** min(domain_info["consecutive_429_errors"], 4)
            delay = min(self.max_delay * multiplier, self.max_retry_delay)
            logger.info(
                f"Using increased delay of {delay:.2f}s for {domain} due to rate limiting"
            )
            return delay

        return random.uniform(self.min_delay, self.max_delay)

    def before_request(self, url: str) -> None:
        domain = self._get_domain_from_url(url)

        if domain not in self.domain_info:
            self.domain_info[domain] = {
                "last_request_time": 0,
                "consecutive_429_errors": 0,
            }

        domain_info = self.domain_info[domain]
        current_time = time()
        time_since_last = current_time - domain_info["last_request_time"]
        required_delay = self._get_domain_delay(domain)

        if time_since_last < required_delay:
            sleep_time = required_delay - time_since_last
            logger.debug(
                f"Rate limiting: sleeping {sleep_time:.2f}s before request to {domain}"
            )
            sleep(sleep_time)

        # Pre-emptively update the last request time
        self.domain_info[domain]["last_request_time"] = time()

    def after_request(self, url: str, status_code: int) -> None:
        domain = self._get_domain_from_url(url)

        if domain not in self.domain_info:
            return

        if status_code == 429:
            # Increment consecutive errors counter
            self.domain_info[domain]["consecutive_429_errors"] += 1
            logger.warning(
                f"Rate limit hit for {domain} - consecutive errors: "
                f"{self.domain_info[domain]['consecutive_429_errors']}"
            )
        else:
            # Reset consecutive errors if request was successful
            if self.domain_info[domain]["consecutive_429_errors"] > 0:
                logger.info(f"Rate limiting recovered for {domain}")
                self.domain_info[domain]["consecutive_429_errors"] = 0


class BaseDownloader(abc.ABC):
    """Base interface for all downloaders"""

    @abc.abstractmethod
    def download(self, url: str, headers: Optional[Dict[str, str]] = None) -> BytesIO:
        """Download content from URL to a BytesIO object"""

    @abc.abstractmethod
    def download_to_bytes(
        self, url: str, headers: Optional[Dict[str, str]] = None
    ) -> bytes:
        """Download content from URL and return as bytes"""


class HttpDownloader(BaseDownloader):
    """HTTP content downloader with retry mechanism and configurable rate limiting"""

    def __init__(
        self,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_delay: float = DEFAULT_RETRY_DELAY,
        timeout: int = DEFAULT_TIMEOUT,
        mimic_browser: bool = True,
        session: Optional[requests.Session] = None,
        rate_limit_strategy: Optional[RateLimitStrategy] = None,
    ):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = timeout
        self.mimic_browser = mimic_browser
        self._session = session
        self.rate_limit_strategy = rate_limit_strategy or NoRateLimitStrategy()

    @property
    def session(self) -> requests.Session:
        """Lazy initialization of requests session"""
        if self._session is None:
            self._session = requests.Session()
            # Apply browser headers if mimicking browser
            if self.mimic_browser:
                self._session.headers.update(BROWSER_HEADERS)
        return self._session

    def download(self, url: str, headers: Optional[Dict[str, str]] = None) -> BytesIO:
        """
        Download content from URL to a BytesIO object

        Args:
            url: URL to download from
            headers: Additional headers to use

        Returns:
            BytesIO object containing the downloaded content
        """
        content = self.download_to_bytes(url, headers)
        return BytesIO(content)

    @timer(name="Downloader - Download to bytes")
    @retry(max_attempts=3, delay=1, backoff=2, exceptions=(Exception,))
    def download_to_bytes(
        self, url: str, headers: Optional[Dict[str, str]] = None
    ) -> bytes:
        """
        Download content from URL and return as bytes

        Args:
            url: URL to download from
            headers: Additional headers to use

        Returns:
            Downloaded content as bytes
        """
        if not self.is_valid_url(url):
            raise ValueError(f"Invalid URL: {url}")

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.debug(f"Download attempt {attempt}/{self.max_retries}: {url}")

                # Apply rate limiting before request
                self.rate_limit_strategy.before_request(url)

                response = self.session.get(
                    url, headers=headers, timeout=self.timeout, stream=True
                )
                response.raise_for_status()

                # Get content and notify rate limiter of success
                content = response.content
                self.rate_limit_strategy.after_request(url, response.status_code)

                logger.info(f"Successfully downloaded {url} ({len(content)} bytes)")
                return content

            except requests.RequestException as e:
                status_code = e.response.status_code if hasattr(e, "response") else 0
                # Notify rate limiter of failure
                self.rate_limit_strategy.after_request(url, status_code)

                logger.warning(f"Download attempt {attempt} failed: {e}")

                if attempt < self.max_retries:
                    sleep_time = self.retry_delay * (
                        2 ** (attempt - 1)
                    )  # Exponential backoff
                    logger.debug(f"Waiting {sleep_time:.2f}s before retry")
                    sleep(sleep_time)
                else:
                    logger.error(
                        f"Failed to download {url} after {self.max_retries} attempts"
                    )
                    raise

    def is_valid_url(self, url: str) -> bool:
        """Check if URL is valid"""
        try:
            result = urlparse(url)
            return all([result.scheme in ["http", "https"], result.netloc])
        except:
            return False


class DownloaderFactory:
    """Factory for creating different types of downloaders"""

    @staticmethod
    def create_standard_downloader(
        max_retries: int = DEFAULT_MAX_RETRIES, timeout: int = DEFAULT_TIMEOUT
    ) -> HttpDownloader:
        """Create a standard downloader with no rate limiting"""
        return HttpDownloader(
            max_retries=max_retries,
            timeout=timeout,
            rate_limit_strategy=NoRateLimitStrategy(),
        )

    @staticmethod
    def create_simple_rate_limited_downloader(
        delay_seconds: float = 1.0,
        max_retries: int = DEFAULT_MAX_RETRIES,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> HttpDownloader:
        """Create a downloader with simple fixed delay rate limiting"""
        return HttpDownloader(
            max_retries=max_retries,
            timeout=timeout,
            rate_limit_strategy=SimpleRateLimitStrategy(delay_seconds),
        )

    @staticmethod
    def create_domain_rate_limited_downloader(
        min_delay: float = MIN_DELAY_SECONDS,
        max_delay: float = MAX_DELAY_SECONDS,
        max_retry_delay: float = MAX_RETRY_DELAY,
        max_retries: int = DEFAULT_MAX_RETRIES,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> HttpDownloader:
        """Create a downloader with domain-based advanced rate limiting"""
        return HttpDownloader(
            max_retries=max_retries,
            timeout=timeout,
            rate_limit_strategy=DomainBasedRateLimitStrategy(
                min_delay=min_delay,
                max_delay=max_delay,
                max_retry_delay=max_retry_delay,
            ),
        )
