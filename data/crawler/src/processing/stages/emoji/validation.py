from typing import Dict, Any, List

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

from core.logger import get_logger
from storage.database.models.emoji import Status
from processing.stages.base import BaseStage
from processing.interface import PipelineContext
from storage.database.repositories.emoji_repository import CrawlEmojiRepository

logger = get_logger("processing.stages.emoji.validation")


class MessageValidationStage(BaseStage[Dict[str, Any], DataFrame]):
    """Validate Kafka message and convert to DataFrame"""

    def __init__(self, spark_pipeline):
        super().__init__("emoji_message_validation")
        self.spark_pipeline = spark_pipeline

    def _process_impl(self, input_data: Dict[str, Any], context: PipelineContext) -> DataFrame:
        """
        Validate message and convert to DataFrame

        Args:
            input_data: Kafka message (dict)
            context: Pipeline context

        Returns:
            DataFrame of emoji data
        """
        # Message validation
        if 'type' not in input_data or input_data['type'] != 'emoji_batch':
            raise ValueError(f"Invalid message type: {input_data.get('type')}")

        if 'payload' not in input_data or 'emojis' not in input_data['payload']:
            raise ValueError("Message missing emoji payload")

        emojis = input_data['payload']['emojis']
        if not emojis:
            logger.warning("Empty emoji list in message")
            return self.spark_pipeline.create_dataframe_from_messages([])

        # Convert to DataFrame
        df = self.spark_pipeline.create_dataframe_from_messages(emojis)

        # Log stats
        emoji_count = df.count()
        logger.info(f"Validated message with {emoji_count} emojis")

        return df


class StatusCheckStage(BaseStage[DataFrame, DataFrame]):
    """Check emoji status in database and filter crawled ones"""

    def __init__(self, repo=None):
        super().__init__("emoji_status_check")
        self.repo = repo or CrawlEmojiRepository()

    def _process_impl(self, input_df: DataFrame, context: PipelineContext) -> DataFrame:
        """
        Check emoji status in database and filter ones with CRAWLED status

        Args:
            input_df: DataFrame with emoji data
            context: Pipeline context

        Returns:
            Filtered DataFrame
        """
        if input_df.rdd.isEmpty():
            return input_df

        emoji_ids = [int(row["id"]) for row in input_df.select("id").collect()]

        db_emojis = self.repo.get_by_ids(emoji_ids)

        crawled_ids = {
            str(emoji.id) for emoji in db_emojis
            if emoji.status == Status.CRAWLED
        }

        # Filter DataFrame using this set
        filtered_df = input_df.filter(
            F.col("id").cast("string").isin(list(crawled_ids)))

        # Log stats
        total_count = input_df.count()
        filtered_count = filtered_df.count()
        logger.info(
            f"Found {filtered_count}/{total_count} emojis with {Status.CRAWLED} status")

        return filtered_df
