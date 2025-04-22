import requests
from io import BytesIO
from typing import Dict, Optional
from urllib.parse import urlparse
from time import sleep

from core.logger import get_logger
from core.decorator import retry, timer

logger = get_logger("downloader.http")

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


class HttpDownloader:
    """Simple HTTP content downloader with retry mechanism"""

    def __init__(
        self,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        timeout: int = 30,
        mimic_browser: bool = True,
        session: requests.Session = None,
    ):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = timeout
        self.mimic_browser = mimic_browser
        self._session = session

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
    def download_to_bytes(self, url: str, headers: Optional[Dict[str, str]] = None) -> bytes:
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
                logger.debug(
                    f"Download attempt {attempt}/{self.max_retries}: {url}")

                response = self.session.get(
                    url,
                    headers=headers,
                    timeout=self.timeout,
                    stream=True
                )
                response.raise_for_status()

                logger.info(
                    f"Successfully downloaded {url} ({len(response.content)} bytes)")
                return response.content

            except requests.RequestException as e:
                logger.warning(f"Download attempt {attempt} failed: {e}")

                if attempt < self.max_retries:
                    sleep_time = self.retry_delay * \
                        (2 ** (attempt - 1))  # Exponential backoff
                    logger.debug(f"Waiting {sleep_time:.2f}s before retry")
                    sleep(sleep_time)
                else:
                    logger.error(
                        f"Failed to download {url} after {self.max_retries} attempts")
                    raise

    def is_valid_url(self, url: str) -> bool:
        """Check if URL is valid"""
        try:
            result = urlparse(url)
            return all([result.scheme in ['http', 'https'], result.netloc])
        except:
            return False
