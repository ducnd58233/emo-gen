from io import BytesIO
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from core.logger import get_logger
from core.decorator import timer, retry
from processing.stages.base import BaseStage
from processing.interface import PipelineContext
from storage.minio.client import MinioClient
from storage.database.models.emoji import Status
from storage.database.repositories.emoji_repository import CrawlEmojiRepository

logger = get_logger("processing.stages.emoji.storage")


class StorageStage(BaseStage[DataFrame, DataFrame]):
    """Store emoji images and update metadata"""

    def __init__(self, minio_client=None, emoji_repo=None, batch_size=20):
        super().__init__("emoji_storage")
        self.minio = minio_client or MinioClient()
        self.emoji_repo = emoji_repo or CrawlEmojiRepository()
        self.batch_size = batch_size

    def _process_impl(self, input_df: DataFrame, context: PipelineContext) -> DataFrame:
        """
        Store emoji images in MinIO and update DB records

        Args:
            input_df: DataFrame with emoji data and image_data column
            context: Pipeline context

        Returns:
            Original DataFrame for potential further processing
        """
        if input_df.rdd.isEmpty():
            return input_df

        # Collect data to driver - avoid UDF serialization issues
        rows = input_df.collect()

        # Batch processing for better performance
        total_count = len(rows)
        success_count = 0
        failure_count = 0

        logger.info(
            f"Processing {total_count} emojis for storage in batches of {self.batch_size}")

        for i in range(0, total_count, self.batch_size):
            batch = rows[i:i+self.batch_size]
            logger.info(
                f"Processing batch {i//self.batch_size + 1}/{(total_count + self.batch_size - 1)//self.batch_size}")

            batch_success = 0
            emoji_ids_to_update = []
            failed_emoji_ids = []

            # Process storage on driver node
            for row in batch:
                row_dict = row.asDict()

                try:
                    # Extract data
                    emoji_id = row_dict["id"]
                    emoji_name = row_dict["name"]
                    image_data = row_dict.get("image_data")

                    if not image_data:
                        logger.warning(
                            f"Missing image data for emoji {emoji_id}")
                        failed_emoji_ids.append(emoji_id)
                        continue

                    # Store in MinIO - ensure data is BytesIO
                    storage_key = f"emojis/{emoji_id}.png"
                    bytes_data = BytesIO(image_data)

                    self.minio.store(
                        data=bytes_data,
                        key=storage_key,
                        metadata={
                            "content_type": "image/png",
                            "emoji_id": str(emoji_id),
                            "name": emoji_name
                        }
                    )

                    # Collect IDs for batch update
                    emoji_ids_to_update.append(emoji_id)
                    batch_success += 1

                except Exception as e:
                    logger.error(
                        f"Failed to store emoji {row_dict.get('id')}: {e}")
                    failed_emoji_ids.append(row_dict.get('id'))

            if emoji_ids_to_update:
                try:
                    self.emoji_repo.update_status_bulk(
                        emoji_ids_to_update, Status.PROCESSED)
                    logger.info(
                        f"Updated {len(emoji_ids_to_update)} emojis to PROCESSED status")
                except Exception as e:
                    logger.error(f"Error in batch status update: {e}")

            if failed_emoji_ids:
                try:
                    self.emoji_repo.update_status_bulk(
                        failed_emoji_ids, Status.FAILED)
                    logger.info(
                        f"Updated {len(failed_emoji_ids)} emojis to FAILED status")
                except Exception as e:
                    logger.error(f"Error updating failed statuses: {e}")

            success_count += batch_success
            failure_count += len(batch) - batch_success

        logger.info(
            f"Storage complete: {success_count} succeeded, {failure_count} failed out of {total_count} total")

        return input_df
