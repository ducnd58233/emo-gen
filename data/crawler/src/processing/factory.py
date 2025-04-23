from typing import Dict, List, Type

from core.logger import get_logger
from processing.interface import Pipeline, PipelineStage
from processing.pipelines.emoji import EmojiPipeline
from processing.stages.emoji.download import EmojiDownloadStage
from processing.stages.emoji.metadata_storage import MetadataStorageStage
from processing.stages.emoji.minio_storage import MinioStorageStage
from processing.stages.emoji.status_update import FinalStatusUpdateStage
from processing.stages.emoji.validation import MessageValidationStage, StatusCheckStage

logger = get_logger("processing.factory")


class StageFactory:
    """Factory for creating pipeline stages"""

    @classmethod
    def create_emoji_stages(cls, pipeline: Pipeline) -> List[PipelineStage]:
        """Create stages for emoji processing pipeline

        The stages are executed in the following order:
        1. MessageValidationStage - Validates incoming message format
        2. StatusCheckStage - Checks emoji status in database
        3. EmojiDownloadStage - Downloads emoji images
        4. MinioStorageStage - Stores emoji images in MinIO
        5. MetadataStorageStage - Stores emoji metadata in database (only if MinIO storage succeeded)
        6. FinalStatusUpdateStage - Updates emoji status in database
        """
        return [
            MessageValidationStage(pipeline),
            StatusCheckStage(),
            EmojiDownloadStage(),
            MinioStorageStage(),
            MetadataStorageStage(),
            FinalStatusUpdateStage(),
        ]


class PipelineBuilder:
    """Builder for constructing pipelines with stages"""

    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline

    def with_stages(self, stages: List[PipelineStage]) -> "PipelineBuilder":
        """Add multiple stages to the pipeline"""
        for stage in stages:
            self.pipeline.add_stage(stage)
        return self

    def with_stage(self, stage: PipelineStage) -> "PipelineBuilder":
        """Add a single stage to the pipeline"""
        self.pipeline.add_stage(stage)
        return self

    def build(self) -> Pipeline:
        """Return the constructed pipeline"""
        return self.pipeline


class PipelineFactory:
    """Abstract factory for creating and configuring pipelines"""

    # Registry of available pipeline types
    _pipeline_registry: Dict[str, Type[Pipeline]] = {"emoji": EmojiPipeline}

    @classmethod
    def create_pipeline(cls, pipeline_type: str) -> Pipeline:
        """
        Create and configure a pipeline of the specified type

        Args:
            pipeline_type: Type of pipeline to create

        Returns:
            Configured pipeline with appropriate stages

        Raises:
            ValueError: If pipeline_type is not registered
        """
        if pipeline_type not in cls._pipeline_registry:
            raise ValueError(f"Unknown pipeline type: {pipeline_type}")

        pipeline_class = cls._pipeline_registry[pipeline_type]
        pipeline = pipeline_class()

        builder = PipelineBuilder(pipeline)

        if pipeline_type == "emoji":
            stages = StageFactory.create_emoji_stages(pipeline)
            builder.with_stages(stages)
            logger.info("Created emoji processing pipeline with all required stages")

        logger.info(f"Created and configured pipeline of type: {pipeline_type}")
        return builder.build()

    @classmethod
    def register_pipeline_type(cls, name: str, pipeline_class: Type[Pipeline]) -> None:
        """
        Register new pipeline type

        Args:
            name: Name for pipeline type
            pipeline_class: Pipeline class to register
        """
        cls._pipeline_registry[name] = pipeline_class
        logger.info(f"Registered pipeline type: {name}")
