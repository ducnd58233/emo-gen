from collections import deque
from typing import Any, List, Optional

from core.decorator import lazy_property, timer
from core.logger import get_logger
from infra.spark.session import SparkSessionManager
from processing.interface import Pipeline, PipelineContext, PipelineStage
from pyspark.sql import DataFrame

logger = get_logger("processing.pipelines.base")


class SparkPipeline(Pipeline):
    """Pipeline implementation using PySpark for data processing"""

    def __init__(self, name: str):
        self.pipeline_name = name
        self.stages: List[PipelineStage] = []
        self._spark = None
        self._stage_results = {}
        # Keep track of processed stages for efficient cleanup
        self._processed_stages = deque()
        # Keep track of stage names that should be preserved in memory
        self._preserved_stages = set()

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

    def preserve_stage_result(self, stage_name: str) -> None:
        """Mark a stage result to be preserved (not automatically cleaned up)"""
        self._preserved_stages.add(stage_name)

    @timer(name="Pipeline Execution")
    def execute(
        self, input_data: Any, context: Optional[PipelineContext] = None
    ) -> Any:
        """
        Template method for pipeline execution with the following phases:
        1. Pre-process - Prepare data and context
        2. Execute stages - Run each stage in order with immediate cleanup of oldest stages
        3. Post-process - Final processing after all stages
        4. Final cleanup - Clean up all resources
        """
        if not context:
            context = PipelineContext()

        try:
            # Phase 1: Pre-process
            self._stage_results = {}
            self._processed_stages = deque()
            self._preserved_stages = set()

            preprocessed_data = self._pre_process(input_data, context)
            context.set("preprocessed_data", preprocessed_data)

            # Phase 2: Execute stages with aggressive cleanup
            current_data = preprocessed_data
            for i, stage in enumerate(self.stages):
                stage_name = stage.name()
                context.set("current_stage_index", i)
                context.set("current_stage_name", stage_name)

                # Execute the stage
                logger.info(f"Executing stage {stage_name} ({i+1}/{len(self.stages)})")

                try:
                    current_data = stage.process(current_data, context)
                    self._stage_results[stage_name] = current_data
                    self._processed_stages.append(stage_name)

                    if isinstance(current_data, DataFrame):
                        row_count = (
                            current_data.count()
                            if self._should_count_rows(stage_name)
                            else "unknown"
                        )
                        logger.info(
                            f"Stage {stage_name} produced a DataFrame with {row_count} rows"
                        )

                    self._cleanup_oldest_stages(max_stages_to_keep=2)

                except Exception as e:
                    logger.error(f"Error in stage {stage_name}: {e}")
                    import traceback

                    logger.error(traceback.format_exc())
                    # Update context with error info
                    context.set(f"error_stage", stage_name)
                    context.set(f"error_message", str(e))
                    context.set(f"error_traceback", traceback.format_exc())
                    raise

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
            # Phase 4: Final cleanup
            self._cleanup(context)

    def _cleanup_oldest_stages(self, max_stages_to_keep: int = 2) -> None:
        """Clean up oldest stages first (FIFO), keeping only the most recent stages"""

        while len(self._processed_stages) > max_stages_to_keep:
            oldest_stage = self._processed_stages.popleft()

            if oldest_stage in self._preserved_stages:
                continue

            if oldest_stage not in self._stage_results:
                continue

            result = self._stage_results[oldest_stage]

            # Clean up DataFrame
            if isinstance(result, DataFrame):
                try:
                    if result.is_cached:
                        result.unpersist()
                    logger.debug(f"Unpersisted oldest stage: {oldest_stage}")
                except Exception as e:
                    logger.warning(
                        f"Error unpersisting DataFrame from stage {oldest_stage}: {e}"
                    )

            # Remove reference to free memory
            self._stage_results.pop(oldest_stage, None)
            logger.debug(f"Cleaned up oldest stage: {oldest_stage}")

    def _should_count_rows(self, stage_name: str) -> bool:
        """Determine if we should count rows for this stage"""
        return stage_name not in ["emoji_download_and_store"]

    def _should_cache(self, stage_name: str) -> bool:
        """Determine if the stage result should be cached"""
        no_cache_stages = ["emoji_download_and_store", "final_status_update"]
        return stage_name not in no_cache_stages

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
        logger.error(f"Pipeline error: {error}")
        context.set("pipeline_error", str(error))
        context.set("pipeline_error_trace", error_trace)

    def _cleanup(self, context: PipelineContext) -> None:
        """Clean up all resources after pipeline execution"""
        # Uncache any cached DataFrames
        for stage_name, result in list(self._stage_results.items()):
            if isinstance(result, DataFrame) and result.is_cached:
                try:
                    result.unpersist()
                    logger.debug(f"Cleaned up cached DataFrame from stage {stage_name}")
                except Exception:
                    pass

            # Remove reference
            self._stage_results.pop(stage_name, None)

        self._processed_stages.clear()
        self._preserved_stages.clear()

        import gc

        gc.collect()

        logger.debug("Cleanup complete, released all resources")
