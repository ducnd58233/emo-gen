from typing import Any, List, Optional

from core.decorator import lazy_property, timer
from core.logger import get_logger
from processing.interface import Pipeline, PipelineContext, PipelineStage
from processing.spark.session import SparkSessionManager
from pyspark.sql import DataFrame

logger = get_logger("processing.pipelines.base")


class SparkPipeline(Pipeline):
    """Pipeline implementation using PySpark for data processing"""

    def __init__(self, name: str):
        self.pipeline_name = name
        self.stages: List[PipelineStage] = []
        self._spark = None
        self._stage_results = {}

    @lazy_property
    def spark(self):
        """Lazy initialization of Spark session"""
        if self._spark is None:
            self._spark = SparkSessionManager().session
        return self._spark

    def name(self) -> str:
        return self.pipeline_name

    def add_stage(self, stage: PipelineStage) -> "SparkPipeline":
        """Add a processing stage to the pipeline"""
        self.stages.append(stage)
        return self

    def get_stage_result(self, stage_name: str) -> Any:
        """Get the result from a previous stage by name"""
        return self._stage_results.get(stage_name)

    @timer(name="Pipeline Execution")
    def execute(
        self, input_data: Any, context: Optional[PipelineContext] = None
    ) -> Any:
        """
        Template method for pipeline execution with the following phases:
        1. Pre-process - Prepare data and context
        2. Execute stages - Run each stage in order
        3. Post-process - Final processing after all stages
        4. Cleanup - Clean up resources
        """
        if not context:
            context = PipelineContext()

        try:
            # Phase 1: Pre-process
            self._stage_results = {}
            preprocessed_data = self._pre_process(input_data, context)
            context.set("preprocessed_data", preprocessed_data)

            # Phase 2: Execute stages
            current_data = preprocessed_data
            for i, stage in enumerate(self.stages):
                stage_name = stage.name()
                context.set("current_stage_index", i)
                context.set("current_stage_name", stage_name)

                # Execute the stage and store result
                logger.info(f"Executing stage {stage_name} ({i+1}/{len(self.stages)})")
                current_data = stage.process(current_data, context)
                self._stage_results[stage_name] = current_data

                # Cache DataFrame results to optimize performance
                if isinstance(current_data, DataFrame):
                    current_data = current_data.cache()
                    row_count = current_data.count()
                    context.set(f"stage_{i}_count", row_count)
                    logger.info(f"Stage {stage_name} produced {row_count} rows")

            # Phase 3: Post-process
            final_result = self._post_process(current_data, context)
            context.set("final_result", final_result)

            logger.info(f"Pipeline {self.pipeline_name} completed successfully")
            return final_result

        except Exception as e:
            logger.error(f"Pipeline execution error: {e}")
            self._handle_error(e, context)
            raise
        finally:
            # Phase 4: Cleanup
            self._cleanup(context)

    def _pre_process(self, input_data: Any, context: PipelineContext) -> Any:
        """Pre-process input data before running stages (can be overridden)"""
        context.set("original_input", input_data)
        return input_data

    def _post_process(self, result: Any, context: PipelineContext) -> Any:
        """Post-process results after all stages (can be overridden)"""
        return result

    def _handle_error(self, error: Exception, context: PipelineContext) -> None:
        """Handle error during pipeline execution (can be overridden)"""
        import traceback

        error_trace = traceback.format_exc()
        logger.error(f"Pipeline error: {error_trace}")
        context.set("pipeline_error", str(error))
        context.set("pipeline_error_trace", error_trace)

    def _cleanup(self, context: PipelineContext) -> None:
        """Clean up resources after pipeline execution (can be overridden)"""
        # Uncache any cached DataFrames
        for result in self._stage_results.values():
            if isinstance(result, DataFrame) and result.is_cached:
                try:
                    result.unpersist()
                except:
                    pass
