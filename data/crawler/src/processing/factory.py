from typing import Dict, Type

from core.logger import get_logger
from processing.interface import Pipeline
from processing.pipelines.emoji import EmojiPipeline
from processing.stages.emoji.validation import MessageValidationStage, StatusCheckStage
from processing.stages.emoji.download import EmojiDownloadStage
from processing.stages.emoji.storage import StorageStage

logger = get_logger("processing.factory")


class PipelineFactory:
    """Factory for creating pipelines"""

    # Registry of available pipeline types
    _pipeline_registry: Dict[str, Type[Pipeline]] = {
        "emoji": EmojiPipeline
    }

    @classmethod
    def create_pipeline(cls, pipeline_type: str) -> Pipeline:
        """
        Create pipeline of specified type

        Args:
            pipeline_type: Type of pipeline to create

        Returns:
            Configured pipeline

        Raises:
            ValueError: If pipeline_type is not registered
        """
        if pipeline_type not in cls._pipeline_registry:
            raise ValueError(f"Unknown pipeline type: {pipeline_type}")

        pipeline_class = cls._pipeline_registry[pipeline_type]

        pipeline = pipeline_class()

        if pipeline_type == "emoji":
            cls._configure_emoji_pipeline(pipeline)

        logger.info(f"Created pipeline of type: {pipeline_type}")
        return pipeline

    @classmethod
    def _configure_emoji_pipeline(cls, pipeline: Pipeline) -> None:
        """
        Configure emoji pipeline with stages

        Args:
            pipeline: Pipeline to configure
        """
        # Add stages to pipeline
        pipeline.add_stage(MessageValidationStage(pipeline))
        pipeline.add_stage(StatusCheckStage())
        pipeline.add_stage(EmojiDownloadStage())
        pipeline.add_stage(StorageStage())

        logger.info("Configured emoji pipeline with standard stages")

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
