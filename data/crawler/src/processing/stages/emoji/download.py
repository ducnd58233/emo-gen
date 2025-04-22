import random
import time
from concurrent.futures import ThreadPoolExecutor
from logging import getLogger
from typing import Dict, List, Optional, Tuple, Union

import requests
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when

from core.config import config
from processing.stages.base import BaseStage
from processing.interface import PipelineContext

logger = getLogger("processing.stages.emoji.download")

# Constants
DEFAULT_MAX_WORKERS = 5
DEFAULT_BATCH_SIZE = 50
MIN_DELAY_SECONDS = 2.0
MAX_DELAY_SECONDS = 5.0
BASE_RETRY_DELAY = 1.0
MAX_RETRY_DELAY = 60.0
MAX_RETRIES = 5

# List of user agents to rotate through for requests
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Edg/92.0.902.84",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36 OPR/78.0.4093.147",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36",
]

# Request headers
DEFAULT_HEADERS = {
    "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://discords.com/emoji-list/",
    "sec-ch-ua": '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
    "sec-ch-ua-mobile": "?0",
    "sec-fetch-dest": "image",
    "sec-fetch-mode": "no-cors",
    "sec-fetch-site": "same-origin",
}


class EmojiDownloadStage(BaseStage):
    def __init__(
        self,
        max_workers: int = DEFAULT_MAX_WORKERS,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ):
        """Initialize the emoji download stage.

        Args:
            max_workers: Maximum number of download workers
            batch_size: Size of emoji batches to process
        """
        super().__init__("emoji_download")
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.session = self._create_session()
        self._request_timestamps = []

        # Track domains and their rate limits
        self._domain_rate_limits = {
            "discords.com": {
                "min_delay": MIN_DELAY_SECONDS,
                "max_delay": MAX_DELAY_SECONDS,
                "last_request_time": 0,
                "consecutive_429_errors": 0
            }
        }

    def _create_session(self) -> requests.Session:
        """Create and configure a requests session for downloads.

        Returns:
            Configured requests session
        """
        session = requests.Session()
        session.headers.update(DEFAULT_HEADERS)
        return session

    def _get_random_user_agent(self) -> str:
        """Get a random user agent from the list.

        Returns:
            Random user agent string
        """
        return random.choice(USER_AGENTS)

    def _get_domain_from_url(self, url: str) -> str:
        """Extract domain from URL.

        Args:
            url: URL to extract domain from

        Returns:
            Domain name
        """
        from urllib.parse import urlparse
        return urlparse(url).netloc

    def _get_domain_delay(self, domain: str) -> float:
        """Get appropriate delay for a domain based on its current rate limit state.

        Args:
            domain: Domain name

        Returns:
            Delay in seconds
        """
        if domain not in self._domain_rate_limits:
            self._domain_rate_limits[domain] = {
                "min_delay": MIN_DELAY_SECONDS,
                "max_delay": MAX_DELAY_SECONDS,
                "last_request_time": 0,
                "consecutive_429_errors": 0
            }

        domain_info = self._domain_rate_limits[domain]

        # Increase delay if we've seen 429 errors
        if domain_info["consecutive_429_errors"] > 0:
            # Apply exponential backoff based on consecutive errors
            # Cap at 2^4 = 16x
            multiplier = 2 ** min(domain_info["consecutive_429_errors"], 4)
            delay = min(domain_info["max_delay"] * multiplier, MAX_RETRY_DELAY)
            logger.info(
                f"Using increased delay of {delay:.2f}s for {domain} due to rate limiting")
            return delay

        return random.uniform(domain_info["min_delay"], domain_info["max_delay"])

    def _enforce_rate_limit(self, url: str) -> None:
        """Enforce rate limiting for requests to a specific domain.

        Args:
            url: URL to enforce rate limit for
        """
        domain = self._get_domain_from_url(url)
        domain_info = self._domain_rate_limits.get(
            domain,
            {
                "min_delay": MIN_DELAY_SECONDS,
                "max_delay": MAX_DELAY_SECONDS,
                "last_request_time": 0,
                "consecutive_429_errors": 0
            }
        )

        current_time = time.time()
        time_since_last = current_time - domain_info["last_request_time"]

        # Determine necessary delay
        required_delay = self._get_domain_delay(domain)

        # Sleep if needed
        if time_since_last < required_delay:
            sleep_time = required_delay - time_since_last
            logger.debug(
                f"Rate limiting: sleeping {sleep_time:.2f}s before request to {domain}")
            time.sleep(sleep_time)

        # Update last request time
        self._domain_rate_limits[domain]["last_request_time"] = time.time()

    def _update_rate_limit_status(self, url: str, status_code: int) -> None:
        """Update rate limit status based on response.

        Args:
            url: The URL that was requested
            status_code: HTTP status code from response
        """
        domain = self._get_domain_from_url(url)

        if domain not in self._domain_rate_limits:
            return

        if status_code == 429:
            # Increment consecutive errors counter
            self._domain_rate_limits[domain]["consecutive_429_errors"] += 1
            logger.warning(
                f"Rate limit hit for {domain} - consecutive errors: "
                f"{self._domain_rate_limits[domain]['consecutive_429_errors']}"
            )
        else:
            # Reset consecutive errors if request was successful
            if self._domain_rate_limits[domain]["consecutive_429_errors"] > 0:
                logger.info(f"Rate limiting recovered for {domain}")
                self._domain_rate_limits[domain]["consecutive_429_errors"] = 0

    def _process_impl(self, df: DataFrame, context: PipelineContext) -> DataFrame:
        """Process emoji DataFrame by downloading emoji images.

        Args:
            df: Input DataFrame with emoji data
            context: Pipeline context

        Returns:
            DataFrame with image data column added
        """
        # Define schema
        schema = df.schema.add("image_data", "binary")

        # Create initial dataframe with empty image data
        result_df = df.withColumn("image_data", lit(None))

        # Get emoji data as a list of dictionaries
        emoji_data = df.select("id", "image_url").collect()

        # Process in batches
        total_batches = (len(emoji_data) + self.batch_size -
                         1) // self.batch_size
        for batch_idx, batch_start in enumerate(range(0, len(emoji_data), self.batch_size)):
            batch = emoji_data[batch_start:batch_start + self.batch_size]

            logger.info(
                f"Processing batch {batch_idx + 1}/{total_batches} ({len(batch)} emojis)")

            # Download batch of emojis
            download_results = self._download_batch(batch)

            # Update result dataframe with downloaded image data
            for emoji_id, image_data in download_results.items():
                if image_data:
                    # Update the row with image data
                    result_df = result_df.withColumn(
                        "image_data",
                        when(col("id") == emoji_id,
                             image_data).otherwise(col("image_data"))
                    )

        logger.info(f"Completed downloading {len(emoji_data)} emojis")
        return result_df

    def _download_batch(self, batch: List[Dict]) -> Dict[str, Optional[bytes]]:
        """Download a batch of emoji images in parallel.

        Args:
            batch: List of emoji data dictionaries

        Returns:
            Dictionary mapping emoji IDs to image data
        """
        results = {}

        # Use ThreadPoolExecutor for parallel downloads
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit download tasks
            future_to_emoji = {
                executor.submit(
                    self._download_with_delay, emoji["image_url"], emoji["id"]
                ): emoji["id"]
                for emoji in batch
            }

            # Process completed tasks
            for future in future_to_emoji:
                emoji_id = future_to_emoji[future]
                try:
                    image_data = future.result()
                    results[emoji_id] = image_data
                except Exception as e:
                    logger.error(f"Error downloading emoji {emoji_id}: {e}")
                    results[emoji_id] = None

        # Log summary
        success_count = sum(1 for data in results.values() if data is not None)
        logger.info(
            f"Downloaded {success_count}/{len(batch)} emojis successfully")

        return results

    def _download_with_delay(self, url: str, emoji_id: str) -> Optional[bytes]:
        """Download image with rate limiting and retry logic.

        Args:
            url: URL to download image from
            emoji_id: Emoji ID for logging

        Returns:
            Image data as bytes if successful, None otherwise
        """
        retries = 0

        while retries < MAX_RETRIES:
            try:
                # Apply rate limiting before request
                self._enforce_rate_limit(url)

                # Make the request
                image_data = self._download_image(url)

                # Update rate limit status on success
                self._update_rate_limit_status(url, 200)

                return image_data

            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if hasattr(
                    e, 'response') else 0

                # Update rate limit tracking
                self._update_rate_limit_status(url, status_code)

                # Special handling for rate limiting (429)
                if status_code == 429:
                    retry_after = int(e.response.headers.get(
                        'Retry-After', BASE_RETRY_DELAY * (2 ** retries)))
                    logger.warning(
                        f"Rate limited (429) when downloading emoji {emoji_id}. "
                        f"Retrying after {retry_after}s (attempt {retries + 1}/{MAX_RETRIES})"
                    )
                    time.sleep(retry_after)
                else:
                    # For other HTTP errors, use exponential backoff
                    backoff = BASE_RETRY_DELAY * (2 ** retries)
                    logger.warning(
                        f"HTTP error {status_code} when downloading emoji {emoji_id}. "
                        f"Retrying in {backoff:.1f}s (attempt {retries + 1}/{MAX_RETRIES}): {e}"
                    )
                    time.sleep(backoff)

                retries += 1

            except Exception as e:
                # For general exceptions, use exponential backoff
                backoff = BASE_RETRY_DELAY * (2 ** retries)
                logger.warning(
                    f"Error downloading emoji {emoji_id}. "
                    f"Retrying in {backoff:.1f}s (attempt {retries + 1}/{MAX_RETRIES}): {e}"
                )
                time.sleep(backoff)
                retries += 1

        logger.error(
            f"Failed to download emoji {emoji_id} after {MAX_RETRIES} attempts")
        return None

    def _download_image(self, url: str) -> bytes:
        """Download image from URL.

        Args:
            url: URL to download image from

        Returns:
            Image data as bytes

        Raises:
            requests.exceptions.HTTPError: If HTTP error occurs
            Exception: For other errors
        """
        try:
            # Set a random user agent for each request
            headers = self.session.headers.copy()
            headers["User-Agent"] = self._get_random_user_agent()

            response = self.session.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            return response.content

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download image from {url}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error downloading image from {url}: {e}")
            raise
