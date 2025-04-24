from typing import Any, Dict, Optional

from core.logger import get_logger
from processing.interface import PipelineContext
from processing.stages.base import BaseStage, ChainStage, ConditionalStage
from processing.stages.emoji.download_and_store import EmojiDownloadAndStoreStage
from processing.stages.emoji.metadata_storage import MetadataStorageStage
from processing.stages.emoji.status_update import FinalStatusUpdateStage
from processing.stages.emoji.validation import MessageValidationStage, StatusCheckStage
from pyspark.sql import DataFrame

logger = get_logger("processing.stages.emoji.processing_chain")


def emoji_download_success_condition(
    input_data: DataFrame, context: PipelineContext
) -> bool:
    """Check if emoji download and storage was successful for any emojis"""
    successful_ids = context.get("successful_emoji_ids", [])
    return len(successful_ids) > 0


class ChainedEmojiProcessingStage(BaseStage):
    """
    Combined emoji processing pipeline with conditional stages

    This stage implements an optimized processing flow using conditional execution:

    1. Message Validation - Convert message to DataFrame
    2. Status Check - Filter only CRAWLED emojis
    3. Download and Store - Download emojis and store in MinIO using parallel processing
    4. Conditional Metadata Storage - Only executed if downloads were successful
    5. Status Update - Update status for all emojis (whether successful or failed)
    """

    def __init__(
        self,
        spark_pipeline,
        max_workers: int = 5,
        emoji_download_stage: Optional[EmojiDownloadAndStoreStage] = None,
        metadata_stage: Optional[MetadataStorageStage] = None,
        status_update_stage: Optional[FinalStatusUpdateStage] = None,
    ):
        super().__init__("emoji_processing_chain")
        self.pipeline = spark_pipeline

        # Create core stages
        self.validation_stage = MessageValidationStage(spark_pipeline)
        self.status_check_stage = StatusCheckStage()
        self.download_stage = emoji_download_stage or EmojiDownloadAndStoreStage(
            max_workers=max_workers
        )
        self.metadata_stage = metadata_stage or MetadataStorageStage()
        self.status_update_stage = status_update_stage or FinalStatusUpdateStage()

        self.conditional_metadata_stage = ConditionalStage(
            emoji_download_success_condition,
            self.metadata_stage,
            "conditional_metadata_storage",
        )

        self.processing_chain = ChainStage(
            [
                self.status_check_stage,
                self.download_stage,
                self.conditional_metadata_stage,
                self.status_update_stage,
            ],
            "emoji_processing_sequence",
        )

    def _process_impl(
        self, input_data: Dict[str, Any], context: PipelineContext
    ) -> DataFrame:
        """
        Process message through the emoji pipeline

        Args:
            input_data: Raw message data
            context: Pipeline context

        Returns:
            DataFrame with processing results
        """
        # Step 1: Validate message and convert to DataFrame
        validated_df = self.validation_stage.process(input_data, context)

        # If validation produced an empty DataFrame, skip processing
        if validated_df.rdd.isEmpty():
            logger.warning("Validation produced empty DataFrame, skipping processing")
            return validated_df

        # Step 2-5: Process through the chain of remaining stages
        result_df = self.processing_chain.process(validated_df, context)

        return result_df
