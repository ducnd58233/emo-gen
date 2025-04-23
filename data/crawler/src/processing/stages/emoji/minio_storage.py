from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple

import pyspark.sql.functions as F
from core.decorator import retry, timer
from core.logger import get_logger
from processing.interface import PipelineContext
from processing.stages.base import BaseStage
from pyspark.sql import DataFrame, Row
from storage.minio.client import MinioClient

logger = get_logger("processing.stages.emoji.minio_storage")

# Constants
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 2
EMOJI_STORAGE_PREFIX = "emoji"
BATCH_SIZE = 10  # Reduced batch size for better reliability


class MinioStorageStage(BaseStage[DataFrame, DataFrame]):
    """Stage for storing emoji images to MinIO storage"""

    def __init__(
        self,
        minio_client: Optional[MinioClient] = None,
    ):
        """Initialize MinioStorageStage with optional MinIO client.

        Args:
            minio_client: MinIO client for storing images
        """
        super().__init__("emoji_minio_storage")
        self.minio_client = minio_client or MinioClient()

        # Access the client property to ensure client and buckets are initialized
        _ = self.minio_client.client

    @timer(name="MinIO Storage Stage Process")
    def _process_impl(self, input_df: DataFrame, context: PipelineContext) -> DataFrame:
        """Process downloaded emojis by storing them in MinIO.

        Args:
            input_df: DataFrame with emoji data and downloaded image content
            context: Pipeline context

        Returns:
            DataFrame with updated storage_path information
        """
        if input_df.rdd.isEmpty():
            logger.warning("Empty DataFrame, skipping MinIO storage stage")
            return input_df

        all_emoji_ids = context.get("all_emoji_ids", [])
        if not all_emoji_ids:
            logger.warning(
                "No emoji IDs found in context, skipping MinIO storage")
            return input_df

        # Extract data from DataFrame with image data
        emoji_data = input_df.select(
            "id", "name", "image_data", "source").collect()

        # Track successful and failed operations
        storage_results = self._process_emoji_batches(emoji_data)

        # Update context with results
        successful_ids = [emoji_id for emoji_id, _,
                          success in storage_results if success]
        failed_ids = [emoji_id for emoji_id, _,
                      success in storage_results if not success]
        storage_paths = {emoji_id: path for emoji_id,
                         path, success in storage_results if success}

        context.set("minio_stored_emoji_ids", successful_ids)
        context.set("minio_failed_emoji_ids", failed_ids)
        context.set("emoji_storage_paths", storage_paths)

        # Add storage_path column to DataFrame for downstream stages
        result_df = self._update_dataframe_with_storage_paths(
            input_df, storage_results)

        logger.info(
            f"MinIO storage complete: {len(successful_ids)} succeeded, {len(failed_ids)} failed"
        )

        return result_df

    def _process_emoji_batches(
        self, emoji_data: List[Row]
    ) -> List[Tuple[str, str, bool]]:
        """Process emojis in batches for better performance.

        Args:
            emoji_data: List of emoji rows from DataFrame

        Returns:
            List of tuples (emoji_id, storage_path, success_flag)
        """
        results = []
        total_batches = (len(emoji_data) + BATCH_SIZE - 1) // BATCH_SIZE

        for batch_idx, batch_start in enumerate(range(0, len(emoji_data), BATCH_SIZE)):
            batch = emoji_data[batch_start: batch_start + BATCH_SIZE]
            logger.info(
                f"Processing MinIO storage batch {batch_idx + 1}/{total_batches} ({len(batch)} emojis)"
            )

            batch_results = self._store_emoji_batch(batch)
            results.extend(batch_results)

            success_count = sum(
                1 for _, _, success in batch_results if success)
            logger.info(
                f"Batch {batch_idx + 1} results: {success_count}/{len(batch)} successful"
            )

        return results

    def _store_emoji_batch(
        self, emoji_batch: List[Row]
    ) -> List[Tuple[str, str, bool]]:
        """Store a batch of emoji images in MinIO.

        Args:
            emoji_batch: List of emoji data dictionaries

        Returns:
            List of tuples (emoji_id, storage_path, success_flag)
        """
        results = []

        for emoji in emoji_batch:
            emoji_id = str(emoji["id"])
            name = emoji["name"]
            image_data = emoji["image_data"]
            source = emoji["source"]

            if image_data is None:
                logger.warning(
                    f"No image data for emoji {emoji_id}, skipping MinIO storage")
                results.append((emoji_id, "", False))
                continue

            # Generate storage path
            storage_path = self._generate_storage_path(source, name)

            # Try to store in MinIO with retry
            success = self._store_in_minio_with_retry(
                emoji_id, name, source, image_data, storage_path
            )

            results.append((emoji_id, storage_path, success))

        return results

    @retry(
        max_attempts=DEFAULT_MAX_RETRIES,
        delay=DEFAULT_RETRY_DELAY,
        backoff=2,
        exceptions=(Exception,),
        logger=logger,
    )
    def _store_in_minio_with_retry(
        self, emoji_id: str, name: str, source: str, image_data: Any, storage_path: str
    ) -> bool:
        """Store a single emoji image in MinIO with retry logic.

        Args:
            emoji_id: Emoji ID
            name: Emoji name
            source: Source of the emoji
            image_data: Binary image data
            storage_path: Path to store in MinIO

        Returns:
            True if storage was successful, False otherwise
        """
        try:
            metadata = {
                "content_type": self._detect_content_type(name),
                "emoji_id": str(emoji_id),
                "emoji_name": name,
                "emoji_source": source
            }

            # Convert to BytesIO if needed
            if isinstance(image_data, (bytes, bytearray)):
                image_bytes = BytesIO(image_data)
            else:
                image_bytes = image_data

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
        """Detect content type based on file extension.

        Args:
            filename: Name of the file

        Returns:
            MIME type string
        """
        if filename.lower().endswith('.gif'):
            return "image/gif"
        elif filename.lower().endswith(('.jpg', '.jpeg')):
            return "image/jpeg"
        else:
            # Default to PNG for most emoji
            return "image/png"

    def _update_dataframe_with_storage_paths(
        self, df: DataFrame, storage_results: List[Tuple[str, str, bool]]
    ) -> DataFrame:
        """
        Update DataFrame with storage paths for successfully stored emojis.

        Args:
            df: Input DataFrame
            storage_results: List of (emoji_id, storage_path, success) tuples

        Returns:
            Updated DataFrame
        """
        result_df = df

        if "storage_path" not in df.columns:
            result_df = df.withColumn("storage_path", F.lit(None))

        # Update storage paths for successful emojis
        for emoji_id, storage_path, success in storage_results:
            if success:
                result_df = result_df.withColumn(
                    "storage_path",
                    F.when(F.col("id") == emoji_id, storage_path).otherwise(
                        F.col("storage_path")
                    ),
                )

        return result_df

    def _generate_storage_path(self, source: str, name: str) -> str:
        """Generate MinIO storage path for an emoji using source and name.

        Args:
            source: Source of the emoji
            name: Original name of the emoji

        Returns:
            Storage path for MinIO
        """
        safe_source = self._sanitize_filename(source)
        safe_name = self._sanitize_filename(name)

        # Create path with pattern: emoji/{source}_{name}
        return f"{EMOJI_STORAGE_PREFIX}/{safe_source}_{safe_name}"

    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename to be safe for storage.

        Args:
            filename: Original filename

        Returns:
            Sanitized filename
        """
        import re
        # Remove path separators and other problematic characters
        sanitized = re.sub(r'[\\/*?:"<>|]', '_', filename)
        # Ensure filename isn't too long
        if len(sanitized) > 100:
            # Keep extension if present
            parts = sanitized.rsplit('.', 1)
            if len(parts) > 1:
                sanitized = f"{parts[0][:96]}.{parts[1]}"
            else:
                sanitized = sanitized[:100]
        return sanitized

    def _handle_error(
        self, error: Exception, input_data: DataFrame, context: PipelineContext
    ) -> None:
        """Handle error during stage execution.

        Args:
            error: Exception that occurred
            input_data: Input DataFrame
            context: Pipeline context
        """
        super()._handle_error(error, input_data, context)

        import traceback
        logger.error(f"Error in MinIO storage stage: {error}")
        logger.error(traceback.format_exc())

        # Set failed emoji IDs for context
        all_emoji_ids = context.get("all_emoji_ids", [])
        successful_ids = context.get("minio_stored_emoji_ids", [])

        failed_ids = [
            emoji_id for emoji_id in all_emoji_ids if emoji_id not in successful_ids
        ]

        context.set("minio_failed_emoji_ids", failed_ids)

        logger.error(
            f"MinIO storage stage failed, marked {len(failed_ids)} emojis as failed"
        )
