from core.decorator import timer
from core.logger import get_logger
from processing.interface import PipelineContext
from processing.stages.base import BaseStage
from pyspark.sql import DataFrame
from storage.database.models.emoji import Status
from storage.database.repositories.crawl_emoji_repository import CrawlEmojiRepository

logger = get_logger("processing.stages.emoji.status_update")


class FinalStatusUpdateStage(BaseStage[DataFrame, DataFrame]):
    """Final stage to update emoji statuses based on processing results"""

    def __init__(self, emoji_repo=None):
        super().__init__("final_status_update")
        self.emoji_repo = emoji_repo or CrawlEmojiRepository()

    @timer(name="Final Status Update Process")
    def _process_impl(self, input_df: DataFrame, context: PipelineContext) -> DataFrame:
        """
        Update emoji statuses based on processing results in the pipeline.

        Emojis are marked as PROCESSED only if they were successfully:
        1. Downloaded and stored in MinIO (combined in one stage now)
        2. Metadata stored in database

        Otherwise, they are marked as FAILED.
        """
        # Get all emoji IDs from context
        all_emoji_ids = context.get("all_emoji_ids", [])
        if not all_emoji_ids:
            logger.warning("No emoji IDs found in context, skipping status update")
            return input_df

        successful_ids = set(context.get("successful_emoji_ids", []))

        failed_ids = [id for id in all_emoji_ids if id not in successful_ids]

        logger.info(f"Final status update: {len(all_emoji_ids)} total emojis")
        logger.info(f"- Successfully processed: {len(successful_ids)}")
        logger.info(f"- Failed: {len(failed_ids)}")

        # Log detailed failure breakdown if needed
        if failed_ids and logger.isEnabledFor(10):  # 10 is DEBUG level
            download_store_failed = set(context.get("failed_emoji_ids", []))
            metadata_failed = set(context.get("metadata_failed_emoji_ids", []))

            # Count emojis that passed download+store but failed metadata
            download_store_success_metadata_failed = len(
                set(context.get("successful_emoji_ids", [])).intersection(
                    metadata_failed
                )
            )

            logger.debug("Failed emojis breakdown:")
            logger.debug(f"- Download/storage failures: {len(download_store_failed)}")
            logger.debug(f"- Metadata storage failures: {len(metadata_failed)}")
            logger.debug(
                f"- Download/storage passed but metadata failed: {download_store_success_metadata_failed}"
            )

        # Update database statuses
        self._update_emoji_statuses(list(successful_ids), failed_ids)

        logger.info(
            f"Status update complete: {len(successful_ids)} processed, {len(failed_ids)} failed"
        )
        return input_df

    def _update_emoji_statuses(self, process_ids, fail_ids):
        """Update emoji statuses in database in batches"""
        if process_ids:
            try:
                self.emoji_repo.update_status_bulk(process_ids, Status.PROCESSED)
                logger.info(f"Marked {len(process_ids)} emojis as PROCESSED")
            except Exception as e:
                logger.error(f"Error updating PROCESSED status: {e}")
                import traceback

                logger.error(traceback.format_exc())

        if fail_ids:
            try:
                self.emoji_repo.update_status_bulk(fail_ids, Status.FAILED)
                logger.info(f"Marked {len(fail_ids)} emojis as FAILED")
            except Exception as e:
                logger.error(f"Error updating FAILED status: {e}")
                import traceback

                logger.error(traceback.format_exc())

    def _handle_error(
        self, error: Exception, input_data: DataFrame, context: PipelineContext
    ) -> None:
        """Mark all emojis as failed if the stage itself encounters an error"""
        super()._handle_error(error, input_data, context)

        import traceback

        logger.error(f"Error in status update stage: {error}")
        logger.error(traceback.format_exc())

        all_emoji_ids = context.get("all_emoji_ids", [])
        if all_emoji_ids:
            try:
                self.emoji_repo.update_status_bulk(all_emoji_ids, Status.FAILED)
                logger.error(
                    f"Status update stage failed, marked all {len(all_emoji_ids)} emojis as FAILED"
                )
            except Exception as e:
                logger.error(f"Error marking emojis as failed after stage error: {e}")
                logger.error(traceback.format_exc())
