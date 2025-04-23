from typing import Dict, List, Optional, Tuple

from core.decorator import timer
from core.logger import get_logger
from processing.interface import PipelineContext
from processing.stages.base import BaseStage
from pyspark.sql import DataFrame
from storage.database.repositories.source_emoji_repository import SourceEmojiRepository

logger = get_logger("processing.stages.emoji.metadata_storage")

BATCH_SIZE = 20


class MetadataStorageStage(BaseStage[DataFrame, DataFrame]):
    """Stage for storing emoji metadata in the database for successful MinIO uploads"""

    def __init__(
        self,
        source_emoji_repo: Optional[SourceEmojiRepository] = None,
    ):
        super().__init__("emoji_metadata_storage")
        self.source_emoji_repo = source_emoji_repo or SourceEmojiRepository()

    @timer(name="Metadata Storage Stage Process")
    def _process_impl(self, input_df: DataFrame, context: PipelineContext) -> DataFrame:
        """Create source emoji records for emojis successfully stored in MinIO.

        Args:
            input_df: DataFrame with emoji data
            context: Pipeline context with MinIO storage results

        Returns:
            Input DataFrame (unchanged)
        """
        # Initialize default values in context to avoid issues downstream
        context.set("metadata_stored_emoji_ids", [])
        context.set("metadata_failed_emoji_ids", [])
        context.set("successful_emoji_ids", [])

        # Get successfully stored emoji IDs from MinIO stage
        minio_successful_ids = context.get("minio_stored_emoji_ids", [])
        if not minio_successful_ids:
            logger.warning("No successfully stored emojis found in MinIO")
            return input_df

        # Get storage paths from MinIO stage
        storage_paths = context.get("emoji_storage_paths", {})
        if not storage_paths:
            logger.warning("No storage paths found in context")
            return input_df

        emoji_data = input_df.select("id", "name", "source").collect()

        db_records = []
        for row in emoji_data:
            emoji_id = str(row["id"])

            if emoji_id not in minio_successful_ids:
                continue

            if emoji_id not in storage_paths:
                logger.warning(
                    f"No storage path for emoji {emoji_id}, skipping metadata creation"
                )
                continue

            # Get the path that was used in MinIO storage
            storage_path = storage_paths[emoji_id]

            # Create database record
            db_records.append(
                {
                    "id": int(emoji_id),
                    "name": row["name"],
                    "image_path": storage_path,
                    "source": row["source"],
                }
            )

        # Store in database if we have any records
        successful_ids = []
        failed_ids = []

        if db_records:
            successful_ids, failed_ids = self._store_records_in_batches(db_records)
            logger.info(
                f"Metadata storage: {len(successful_ids)} succeeded, {len(failed_ids)} failed"
            )
        else:
            logger.warning("No emoji records to store in database")

        context.set("metadata_stored_emoji_ids", successful_ids)
        context.set("metadata_failed_emoji_ids", failed_ids)

        minio_successful_set = set(minio_successful_ids)
        metadata_successful_set = set(successful_ids)
        final_successful_ids = list(
            minio_successful_set.intersection(metadata_successful_set)
        )

        context.set("successful_emoji_ids", final_successful_ids)
        logger.info(
            f"Final successful emojis (both MinIO and metadata): {len(final_successful_ids)}"
        )

        return input_df

    def _store_records_in_batches(
        self, db_records: List[Dict]
    ) -> Tuple[List[str], List[str]]:
        """Store emoji records in the database in batches.

        Args:
            db_records: List of prepared database records

        Returns:
            Tuple of (successful_ids, failed_ids)
        """
        successful_ids = []
        failed_ids = []

        # Process in batches for better performance and reliability
        total_batches = (len(db_records) + BATCH_SIZE - 1) // BATCH_SIZE

        for batch_idx, batch_start in enumerate(range(0, len(db_records), BATCH_SIZE)):
            batch = db_records[batch_start : batch_start + BATCH_SIZE]
            batch_ids = [str(record["id"]) for record in batch]

            logger.info(
                f"Processing metadata batch {batch_idx + 1}/{total_batches} ({len(batch)} emojis)"
            )

            try:
                self.source_emoji_repo.bulk_create_from_crawl_emojis(batch)
                successful_ids.extend(batch_ids)
                logger.info(
                    f"Created {len(batch)} source emoji records in batch {batch_idx + 1}"
                )
            except Exception as e:
                logger.error(
                    f"Failed to create source emoji records for batch {batch_idx + 1}: {e}"
                )
                import traceback

                logger.error(traceback.format_exc())
                failed_ids.extend(batch_ids)

        return successful_ids, failed_ids

    def _handle_error(
        self, error: Exception, input_data: DataFrame, context: PipelineContext
    ) -> None:
        """Handle error during stage execution."""
        super()._handle_error(error, input_data, context)

        logger.error(f"Error in metadata storage stage: {error}")
        import traceback

        logger.error(traceback.format_exc())

        context.set("metadata_stored_emoji_ids", [])
        context.set(
            "metadata_failed_emoji_ids", context.get("minio_stored_emoji_ids", [])
        )
        context.set("successful_emoji_ids", [])
