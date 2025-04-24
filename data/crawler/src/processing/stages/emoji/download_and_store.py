from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from logging import getLogger
from typing import Dict, List, Optional, Tuple

import pyspark.sql.functions as F
from core.decorator import retry, timer
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
from storage.minio.client import MinioClient

logger = getLogger("processing.stages.emoji.download_and_store")

# Constants
DEFAULT_MAX_WORKERS = 5
DEFAULT_BATCH_SIZE = 50
DEFAULT_RETRY_DELAY = 5
EMOJI_STORAGE_PREFIX = "emoji"


class EmojiDownloadAndStoreStage(BaseStage):
    """Stage for downloading emoji images and storing them in MinIO using parallel processing"""

    def __init__(
        self,
        max_workers: int = DEFAULT_MAX_WORKERS,
        batch_size: int = DEFAULT_BATCH_SIZE,
        downloader: Optional[HttpDownloader] = None,
        minio_client: Optional[MinioClient] = None,
    ):
        super().__init__("emoji_download_and_store")
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.downloader = (
            downloader
            or DownloaderFactory.create_domain_rate_limited_downloader(
                min_delay=MIN_DELAY_SECONDS,
                max_delay=MAX_DELAY_SECONDS,
                max_retry_delay=MAX_RETRY_DELAY,
                max_retries=DEFAULT_MAX_RETRIES,
                timeout=10,
            )
        )

        self.minio_client = minio_client or MinioClient()
        _ = self.minio_client.client

    @timer(name="Download and Store Process")
    def _process_impl(self, df: DataFrame, context: PipelineContext) -> DataFrame:
        """
        Download emoji images and store them in MinIO

        Args:
            df: DataFrame with emoji data
            context: Pipeline context

        Returns:
            DataFrame with storage_path column added
        """
        # Initialize tracking data in context
        self._initialize_context(context)

        # Extract data for processing
        emoji_data = df.select("id", "image_url", "name", "source").collect()
        if not emoji_data:
            logger.warning("No emoji data to process")
            return df

        # Prepare result DataFrame
        result_df = (
            df.withColumn("storage_path", F.lit(None))
            if "storage_path" not in df.columns
            else df
        )

        # Set all emoji IDs in context
        all_emoji_ids = [str(emoji["id"]) for emoji in emoji_data]
        context.set("all_emoji_ids", all_emoji_ids)

        # Process all emojis in batches
        successful_ids = []
        failed_ids = []
        storage_paths = {}

        total_batches = (len(emoji_data) + self.batch_size - 1) // self.batch_size
        for batch_idx, batch_start in enumerate(
            range(0, len(emoji_data), self.batch_size)
        ):
            batch = emoji_data[batch_start : batch_start + self.batch_size]
            logger.info(
                f"Processing batch {batch_idx + 1}/{total_batches} ({len(batch)} emojis)"
            )

            batch_results = self._process_batch(batch)

            for emoji_id, storage_path, success in batch_results:
                emoji_id_str = str(emoji_id)
                if success:
                    successful_ids.append(emoji_id_str)
                    storage_paths[emoji_id_str] = storage_path

                    result_df = result_df.withColumn(
                        "storage_path",
                        F.when(F.col("id") == emoji_id, storage_path).otherwise(
                            F.col("storage_path")
                        ),
                    )
                else:
                    failed_ids.append(emoji_id_str)

        # Update context with results
        self._update_context(context, successful_ids, failed_ids, storage_paths)

        logger.info(
            f"Completed processing {len(emoji_data)} emojis: {len(successful_ids)} succeeded, {len(failed_ids)} failed"
        )
        return result_df

    def _initialize_context(self, context: PipelineContext) -> None:
        """Initialize tracking data in context"""
        context.set("all_emoji_ids", [])
        context.set("downloaded_emoji_count", 0)
        context.set("successful_emoji_ids", [])
        context.set("failed_emoji_ids", [])
        context.set("emoji_storage_paths", {})

    def _update_context(
        self,
        context: PipelineContext,
        successful_ids: List[str],
        failed_ids: List[str],
        storage_paths: Dict[str, str],
    ) -> None:
        """Update context with processing results"""
        context.set("downloaded_emoji_count", len(successful_ids))
        context.set("successful_emoji_ids", successful_ids)
        context.set("failed_emoji_ids", failed_ids)
        context.set("emoji_storage_paths", storage_paths)
        context.set("minio_stored_emoji_ids", successful_ids)
        context.set("minio_failed_emoji_ids", failed_ids)

    def _process_batch(self, batch: List[Dict]) -> List[Tuple[str, str, bool]]:
        """Process a batch of emojis in parallel using ThreadPoolExecutor"""
        results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(
                    self._download_and_store_emoji,
                    emoji["image_url"],
                    emoji["id"],
                    emoji["name"],
                    emoji["source"],
                ): emoji["id"]
                for emoji in batch
            }

            for future in as_completed(futures):
                emoji_id = futures[future]
                try:
                    storage_path, success = future.result()
                    results.append((emoji_id, storage_path, success))
                except Exception as e:
                    logger.error(f"Error processing emoji {emoji_id}: {e}")
                    results.append((emoji_id, "", False))

        success_count = sum(1 for _, _, success in results if success)
        logger.info(f"Batch results: {success_count}/{len(batch)} successful")
        return results

    def _download_and_store_emoji(
        self, url: str, emoji_id: str, name: str, source: str
    ) -> Tuple[str, bool]:
        """Download emoji and store it in MinIO"""
        try:
            image_data = self._download_emoji(url, emoji_id)
            if not image_data:
                return "", False
            storage_path = self._generate_storage_path(source, name)

            # Store in MinIO
            success = self._store_in_minio(
                emoji_id, name, source, image_data, storage_path
            )

            return (storage_path, success)
        except Exception as e:
            logger.error(f"Error processing emoji {emoji_id}: {e}")
            import traceback

            logger.error(traceback.format_exc())
            return "", False

    def _download_emoji(self, url: str, emoji_id: str) -> Optional[bytes]:
        """Download emoji image"""
        try:
            return self.downloader.download_to_bytes(url)
        except Exception as e:
            logger.error(f"Failed to download emoji {emoji_id} from {url}: {e}")
            return None

    @retry(
        max_attempts=DEFAULT_MAX_RETRIES,
        delay=DEFAULT_RETRY_DELAY,
        backoff=2,
        exceptions=(Exception,),
    )
    def _store_in_minio(
        self,
        emoji_id: str,
        name: str,
        source: str,
        image_data: bytes,
        storage_path: str,
    ) -> bool:
        """Store emoji image in MinIO"""
        try:
            metadata = {
                "content_type": self._detect_content_type(name),
                "emoji_id": str(emoji_id),
                "emoji_name": name,
                "emoji_source": source,
            }

            # Convert to BytesIO for MinIO
            image_bytes = BytesIO(image_data)

            # Store in MinIO
            minio_uri = self.minio_client.store(
                data=image_bytes, key=storage_path, metadata=metadata
            )

            logger.info(f"Successfully stored emoji {emoji_id} at {minio_uri}")
            return True
        except Exception as e:
            logger.error(f"Failed to store emoji {emoji_id} in MinIO: {e}")
            import traceback

            logger.error(traceback.format_exc())
            return False

    def _detect_content_type(self, filename: str) -> str:
        """Detect content type based on file extension"""
        if filename.lower().endswith(".gif"):
            return "image/gif"
        elif filename.lower().endswith((".jpg", ".jpeg")):
            return "image/jpeg"
        else:
            return "image/png"

    def _generate_storage_path(self, source: str, name: str) -> str:
        """Generate MinIO storage path for an emoji"""
        safe_source = self._sanitize_filename(source)
        safe_name = self._sanitize_filename(name)
        return f"{EMOJI_STORAGE_PREFIX}/{safe_source}_{safe_name}"

    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename to be safe for storage"""
        import re

        # Remove path separators and other problematic characters
        sanitized = re.sub(r'[\\/*?:"<>|]', "_", filename)
        # Ensure filename isn't too long
        if len(sanitized) > 100:
            # Keep extension if present
            parts = sanitized.rsplit(".", 1)
            if len(parts) > 1:
                sanitized = f"{parts[0][:96]}.{parts[1]}"
            else:
                sanitized = sanitized[:100]
        return sanitized

    def _handle_error(
        self, error: Exception, input_data: DataFrame, context: PipelineContext
    ) -> None:
        """Handle error during stage execution"""
        super()._handle_error(error, input_data, context)

        import traceback

        logger.error(f"Error in download and store stage: {error}")
        logger.error(traceback.format_exc())

        # Set all emojis as failed
        all_emoji_ids = context.get("all_emoji_ids", [])
        context.set("successful_emoji_ids", [])
        context.set("failed_emoji_ids", all_emoji_ids)
        context.set("emoji_storage_paths", {})
        context.set("minio_stored_emoji_ids", [])
        context.set("minio_failed_emoji_ids", all_emoji_ids)
