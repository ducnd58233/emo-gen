from concurrent.futures import ThreadPoolExecutor
from logging import getLogger
from typing import Dict, List, Optional

from processing.downloader import (
    DEFAULT_MAX_RETRIES,
    MAX_DELAY_SECONDS,
    MAX_RETRY_DELAY,
    MIN_DELAY_SECONDS,
    DownloaderFactory,
    HttpDownloader,
)
from processing.interface import PipelineContext
from processing.stages.base import BaseStage
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when

logger = getLogger("processing.stages.emoji.download")

# Constants
DEFAULT_MAX_WORKERS = 5
DEFAULT_BATCH_SIZE = 50


class EmojiDownloadStage(BaseStage):
    """Stage for downloading emoji images with rate limiting and batch processing"""

    def __init__(
        self,
        max_workers: int = DEFAULT_MAX_WORKERS,
        batch_size: int = DEFAULT_BATCH_SIZE,
        downloader: Optional[HttpDownloader] = None,
    ):
        """Initialize the emoji download stage.

        Args:
            max_workers: Maximum number of download workers
            batch_size: Size of emoji batches to process
            downloader: Optional custom downloader (created if not provided)
        """
        super().__init__("emoji_download")
        self.max_workers = max_workers
        self.batch_size = batch_size

        self.downloader = (
            downloader
            or DownloaderFactory.create_domain_rate_limited_downloader(
                min_delay=MIN_DELAY_SECONDS,
                max_delay=MAX_DELAY_SECONDS,
                max_retry_delay=MAX_RETRY_DELAY,
                max_retries=DEFAULT_MAX_RETRIES,
                timeout=10,  # 10 seconds
            )
        )

    def _process_impl(self, df: DataFrame, context: PipelineContext) -> DataFrame:
        """Process emoji DataFrame by downloading emoji images.

        Args:
            df: Input DataFrame with emoji data
            context: Pipeline context

        Returns:
            DataFrame with image data column added
        """
        # Initialize context values
        context.set("downloaded_emoji_count", 0)
        context.set("successful_emoji_ids", [])
        context.set("failed_emoji_ids", [])

        # Add image_data column to schema
        df.schema.add("image_data", "binary")
        result_df = df.withColumn("image_data", lit(None))

        # Extract data for processing
        emoji_data = df.select("id", "image_url").collect()
        if not emoji_data:
            logger.warning("No emoji data to download")
            context.set("all_emoji_ids", [])
            return result_df

        all_emoji_ids = [str(emoji["id"]) for emoji in emoji_data]
        context.set("all_emoji_ids", all_emoji_ids)

        # Track successful and failed downloads
        successful_emoji_ids = []
        failed_emoji_ids = []

        # Process in batches for better performance
        total_batches = (len(emoji_data) + self.batch_size - 1) // self.batch_size
        for batch_idx, batch_start in enumerate(
            range(0, len(emoji_data), self.batch_size)
        ):
            batch = emoji_data[batch_start : batch_start + self.batch_size]

            logger.info(
                f"Processing batch {batch_idx + 1}/{total_batches} ({len(batch)} emojis)"
            )

            # Download batch and update DataFrame
            download_results = self._download_batch(batch)
            result_df = self._update_dataframe_with_results(result_df, download_results)

            # Track successful and failed downloads
            for emoji_id, image_data in download_results.items():
                # Ensure all IDs are strings for consistency
                emoji_id_str = str(emoji_id)
                if image_data:
                    successful_emoji_ids.append(emoji_id_str)
                else:
                    failed_emoji_ids.append(emoji_id_str)

        # Store download results in context for later stages to use
        context.set("downloaded_emoji_count", len(successful_emoji_ids))
        context.set("successful_emoji_ids", successful_emoji_ids)
        context.set("failed_emoji_ids", failed_emoji_ids)

        logger.info(
            f"Completed downloading {len(emoji_data)} emojis: {len(successful_emoji_ids)} succeeded, {len(failed_emoji_ids)} failed"
        )
        return result_df

    def _update_dataframe_with_results(
        self, df: DataFrame, results: Dict[str, Optional[bytes]]
    ) -> DataFrame:
        """Update DataFrame with downloaded image data

        Args:
            df: Input DataFrame
            results: Dictionary of emoji IDs to image data

        Returns:
            Updated DataFrame
        """
        result_df = df
        for emoji_id, image_data in results.items():
            if image_data:
                result_df = result_df.withColumn(
                    "image_data",
                    when(col("id") == emoji_id, image_data).otherwise(
                        col("image_data")
                    ),
                )
        return result_df

    def _download_batch(self, batch: List[Dict]) -> Dict[str, Optional[bytes]]:
        """Download a batch of emoji images in parallel.

        Args:
            batch: List of emoji data dictionaries

        Returns:
            Dictionary mapping emoji IDs to image data
        """
        results = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_emoji = {
                executor.submit(
                    self._download_emoji, emoji["image_url"], emoji["id"]
                ): emoji["id"]
                for emoji in batch
            }

            for future in future_to_emoji:
                emoji_id = future_to_emoji[future]
                try:
                    image_data = future.result()
                    results[emoji_id] = image_data
                except Exception as e:
                    logger.error(f"Error downloading emoji {emoji_id}: {e}")
                    results[emoji_id] = None

        success_count = sum(1 for data in results.values() if data is not None)
        logger.info(f"Downloaded {success_count}/{len(batch)} emojis successfully")

        return results

    def _download_emoji(self, url: str, emoji_id: str) -> Optional[bytes]:
        """Download emoji image with built-in rate limiting and retry logic.

        Args:
            url: URL to download image from
            emoji_id: Emoji ID for logging

        Returns:
            Image data as bytes if successful, None otherwise
        """
        try:
            # Use the downloader (which handles rate limiting and retries)
            return self.downloader.download_to_bytes(url)
        except Exception as e:
            logger.error(f"Failed to download emoji {emoji_id} from {url}: {e}")
            return None

    def _handle_error(
        self, error: Exception, input_data: DataFrame, context: PipelineContext
    ) -> None:
        """Handle error during stage execution."""
        super()._handle_error(error, input_data, context)

        import traceback

        logger.error(f"Error in download stage: {error}")
        logger.error(traceback.format_exc())

        # Mark all emojis as failed
        all_emoji_ids = context.get("all_emoji_ids", [])
        context.set("successful_emoji_ids", [])
        context.set("failed_emoji_ids", all_emoji_ids)
