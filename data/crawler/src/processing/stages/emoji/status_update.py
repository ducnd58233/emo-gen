from core.logger import get_logger
from processing.interface import PipelineContext
from processing.stages.base import BaseStage
from pyspark.sql import DataFrame
from storage.database.models.emoji import Status
from storage.database.repositories.crawl_emoji_repository import CrawlEmojiRepository

logger = get_logger("processing.stages.emoji.status_update")


class FinalStatusUpdateStage(BaseStage[DataFrame, DataFrame]):
    """Final stage to ensure all emoji statuses are updated correctly"""

    def __init__(self, emoji_repo=None):
        super().__init__("final_status_update")
        self.emoji_repo = emoji_repo or CrawlEmojiRepository()

    def _process_impl(self, input_df: DataFrame, context: PipelineContext) -> DataFrame:
        """Ensure all emoji statuses are properly updated after pipeline execution"""
        all_emoji_ids = context.get("all_emoji_ids", [])
        if not all_emoji_ids:
            logger.warning("No emoji IDs found in context, skipping status update")
            return input_df

        stored_emoji_ids = set(context.get("successful_emoji_ids", []))
        emoji_ids_to_process = []
        emoji_ids_to_fail = []

        for emoji_id in all_emoji_ids:
            if emoji_id in stored_emoji_ids:
                emoji_ids_to_process.append(emoji_id)
            else:
                emoji_ids_to_fail.append(emoji_id)

        update_count = self._update_emoji_statuses(
            emoji_ids_to_process, emoji_ids_to_fail
        )
        logger.info(f"Final status update complete: {update_count} emojis updated")
        return input_df

    def _update_emoji_statuses(self, process_ids, fail_ids):
        """Update emoji statuses in bulk and return count of updated records"""
        update_count = 0

        if process_ids:
            try:
                self.emoji_repo.update_status_bulk(process_ids, Status.PROCESSED)
                logger.info(
                    f"Final update: marked {len(process_ids)} emojis as PROCESSED"
                )
                update_count += len(process_ids)
            except Exception as e:
                logger.error(f"Error updating PROCESSED status: {e}")

        if fail_ids:
            try:
                self.emoji_repo.update_status_bulk(fail_ids, Status.FAILED)
                logger.info(f"Final update: marked {len(fail_ids)} emojis as FAILED")
                update_count += len(fail_ids)
            except Exception as e:
                logger.error(f"Error updating FAILED status: {e}")

        return update_count
