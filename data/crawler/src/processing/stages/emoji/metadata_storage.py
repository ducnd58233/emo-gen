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

    def __init__(self, source_emoji_repo: Optional[SourceEmojiRepository] = None):
        super().__init__("emoji_metadata_storage")
        self.source_emoji_repo = source_emoji_repo or SourceEmojiRepository()

    @timer(name="Metadata Storage Stage Process")
    def _process_impl(self, input_df: DataFrame, context: PipelineContext) -> DataFrame:
        """Create source emoji records for emojis successfully stored in MinIO.

        Args:
            input_df: DataFrame with emoji data including storage_path from previous stage
            context: Pipeline context with MinIO storage results

        Returns:
            Input DataFrame (unchanged)
        """
        # Initialize context values
        context.set("metadata_stored_emoji_ids", [])
        context.set("metadata_failed_emoji_ids", [])

        stored_emoji_ids = context.get("successful_emoji_ids", [])
        if not stored_emoji_ids:
            logger.warning("No successfully stored emojis found from previous stage")
            context.set("successful_emoji_ids", [])
            return input_df

        # Get storage paths from context
        storage_paths = context.get("emoji_storage_paths", {})
        if not storage_paths:
            logger.warning("No storage paths found in context")
            context.set("successful_emoji_ids", [])
            return input_df

        # Prepare data for database records
        emoji_info = {str(row["id"]): row for row in input_df.collect()}
        db_records = []

        for emoji_id in stored_emoji_ids:
            # Skip if no storage path (should not happen with combined stage)
            if emoji_id not in storage_paths:
                logger.warning(
                    f"No storage path for emoji {emoji_id}, skipping metadata creation"
                )
                continue

            # Skip if no metadata
            if emoji_id not in emoji_info:
                logger.warning(
                    f"No metadata for emoji {emoji_id}, skipping metadata creation"
                )
                continue

            # Get emoji metadata
            emoji_data = emoji_info[emoji_id]
            storage_path = storage_paths[emoji_id]

            # Create database record
            db_records.append(
                {
                    "id": int(emoji_id),
                    "name": emoji_data["name"],
                    "image_path": storage_path,
                    "source": emoji_data["source"],
                }
            )

        # Store in database
        successful_ids = []
        failed_ids = []

        if db_records:
            successful_ids, failed_ids = self._store_records_in_batches(db_records)
            logger.info(
                f"Metadata storage: {len(successful_ids)} succeeded, {len(failed_ids)} failed"
            )
        else:
            logger.warning("No emoji records to store in database")

        # Update context for status update stage
        context.set("metadata_stored_emoji_ids", successful_ids)
        context.set("metadata_failed_emoji_ids", failed_ids)

        stored_emoji_set = set(stored_emoji_ids)
        metadata_successful_set = set(successful_ids)
        final_successful_ids = list(
            stored_emoji_set.intersection(metadata_successful_set)
        )

        context.set("successful_emoji_ids", final_successful_ids)
        logger.info(f"Final successful emojis: {len(final_successful_ids)}")

        return input_df

    def _store_records_in_batches(
        self, db_records: List[Dict]
    ) -> Tuple[List[str], List[str]]:
        """Store emoji records in the database in batches."""
        successful_ids = []
        failed_ids = []

        # Process in batches
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

        import traceback

        logger.error(f"Error in metadata storage stage: {error}")
        logger.error(traceback.format_exc())

        # Set empty lists for downstream stages
        context.set("metadata_stored_emoji_ids", [])
        context.set(
            "metadata_failed_emoji_ids", context.get("successful_emoji_ids", [])
        )
        context.set("successful_emoji_ids", [])
