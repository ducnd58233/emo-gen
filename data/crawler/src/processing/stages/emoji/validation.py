from typing import Any, Dict

import pyspark.sql.functions as F
from core.logger import get_logger
from processing.interface import PipelineContext
from processing.stages.base import BaseStage
from pyspark.sql import DataFrame
from storage.database.models.emoji import Status
from storage.database.repositories.crawl_emoji_repository import CrawlEmojiRepository

logger = get_logger("processing.stages.emoji.validation")


class MessageValidationStage(BaseStage[Dict[str, Any], DataFrame]):
    """Validate Kafka message and convert to DataFrame"""

    def __init__(self, spark_pipeline):
        super().__init__("emoji_message_validation")
        self.spark_pipeline = spark_pipeline

    def _process_impl(
        self, input_data: Dict[str, Any], context: PipelineContext
    ) -> DataFrame:
        """Validate message and convert to DataFrame"""
        self._validate_message_format(input_data)

        emojis = input_data["payload"]["emojis"]
        if not emojis:
            logger.warning("Empty emoji list in message")
            return self.spark_pipeline.create_dataframe_from_messages([])

        df = self.spark_pipeline.create_dataframe_from_messages(emojis)
        logger.info(f"Validated message with {df.count()} emojis")
        return df

    def _validate_message_format(self, message):
        """Validate the format of incoming message"""
        if "type" not in message or message["type"] != "emoji_batch":
            raise ValueError(f"Invalid message type: {message.get('type')}")

        if "payload" not in message or "emojis" not in message["payload"]:
            raise ValueError("Message missing emoji payload")


class StatusCheckStage(BaseStage[DataFrame, DataFrame]):
    """Check emoji status in database and filter crawled ones"""

    def __init__(self, repo=None):
        super().__init__("emoji_status_check")
        self.repo = repo or CrawlEmojiRepository()

    def _process_impl(self, input_df: DataFrame, context: PipelineContext) -> DataFrame:
        """Check emoji status and filter ones with CRAWLED status"""
        if input_df.rdd.isEmpty():
            return input_df

        emoji_ids = [int(row["id"]) for row in input_df.select("id").collect()]
        db_emojis = self.repo.get_by_ids(emoji_ids)
        crawled_ids = {
            str(emoji.id)
            for emoji in db_emojis
            if emoji.status.value == Status.CRAWLED.value
        }

        # Filter DataFrame to include only crawled emojis
        filtered_df = input_df.filter(
            F.col("id").cast("string").isin(list(crawled_ids))
        )

        total_count = input_df.count()
        filtered_count = filtered_df.count()
        logger.info(
            f"Found {filtered_count}/{total_count} emojis with {Status.CRAWLED} status"
        )

        return filtered_df
