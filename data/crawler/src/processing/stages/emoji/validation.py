from collections import defaultdict
from typing import Any, Dict

import pyspark.sql.functions as F
from core.decorator import timer
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
    """Check emoji status in database and filter only those with CRAWLED status"""

    def __init__(self, repo=None):
        super().__init__("emoji_status_check")
        self.repo = repo or CrawlEmojiRepository()

    @timer(name="Status Check Process")
    def _process_impl(self, input_df: DataFrame, context: PipelineContext) -> DataFrame:
        """
        Check emoji status and filter ones with CRAWLED status.
        """
        if input_df.rdd.isEmpty():
            logger.warning("Empty input DataFrame, nothing to process")
            context.set("all_emoji_ids", [])
            return input_df

        emoji_rows = input_df.collect()

        all_emoji_ids = [str(row["id"]) for row in emoji_rows]
        context.set("all_emoji_ids", all_emoji_ids)

        emoji_by_source = defaultdict(list)
        for row in emoji_rows:
            source = row["source"]
            emoji_by_source[source].append(row)

        crawled_emoji_ids = set()

        for source, emojis in emoji_by_source.items():
            emoji_names = [emoji["name"] for emoji in emojis]

            matching_emojis = self.repo.filter_by_source_and_status(
                source=source,
                status=Status.CRAWLED,
                identifiers=emoji_names,
            )

            for matching_emoji in matching_emojis:
                crawled_emoji_ids.add(str(matching_emoji.id))

        if crawled_emoji_ids:
            filtered_df = input_df.filter(
                F.col("id").cast("string").isin(list(crawled_emoji_ids))
            )
        else:
            filtered_df = input_df.limit(0)

        total_count = len(emoji_rows)
        filtered_count = len(crawled_emoji_ids)

        logger.info(
            f"Status check complete: found {filtered_count}/{total_count} emojis with {Status.CRAWLED} status"
        )

        return filtered_df
