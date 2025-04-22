from typing import Any, Dict, List

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from storage.database.models.emoji import Status
from processing.pipelines.base import SparkPipeline


class EmojiPipeline(SparkPipeline):
    def __init__(self):
        super().__init__("emoji_processing")

    def create_dataframe_from_messages(self, messages: List[Dict[str, Any]]) -> DataFrame:
        """Create Spark DataFrame from Kafka messages"""
        if not messages:
            return self.spark.createDataFrame([], schema=self.get_emoji_schema())

        return self.spark.createDataFrame(messages)

    def get_emoji_schema(self):
        """Return schema for emoji DataFrame"""
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType

        return StructType([
            StructField("id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("image_url", StringType(), nullable=False),
            StructField("status", StringType(), nullable=True),
            StructField("created_at", TimestampType(), nullable=True),
            StructField("updated_at", TimestampType(), nullable=True)
        ])

    def filter_crawled_emojis(self, df: DataFrame) -> DataFrame:
        """Filter emojis with CRAWLED status"""
        return df.filter(F.col("status") == Status.CRAWLED.value)
