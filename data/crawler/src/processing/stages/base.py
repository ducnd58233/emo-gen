from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Generic, List, Optional, TypeVar

from core.decorator import timer
from core.logger import get_logger
from processing.interface import PipelineContext, PipelineStage

logger = get_logger("processing.stages.base")

T = TypeVar("T")
U = TypeVar("U")


class BaseStage(PipelineStage[T, U], Generic[T, U], ABC):
    """Base implementation of pipeline stage with error handling"""

    def __init__(self, stage_name: str = None):
        self._stage_name = stage_name or self.__class__.__name__

    def name(self) -> str:
        return self._stage_name

    @timer(name="Stage Execution")
    def process(self, input_data: T, context: PipelineContext) -> U:
        """Process input data with proper error handling and timing"""
        logger.info(f"Starting stage: {self.name()}")
        try:
            result = self._process_impl(input_data, context)
            logger.info(f"Completed stage: {self.name()}")
            return result
        except Exception as e:
            logger.error(f"Error in stage {self.name()}: {e}")
            self._handle_error(e, input_data, context)
            raise

    @abstractmethod
    def _process_impl(self, input_data: T, context: PipelineContext) -> U:
        """Implementation of stage processing logic"""

    def _handle_error(
        self, error: Exception, input_data: T, context: PipelineContext
    ) -> None:
        """Handle error during stage execution (can be overridden by subclasses)"""
        import traceback

        logger.error(traceback.format_exc())
        context.set(f"error_{self.name()}", str(error))
        context.set(f"error_{self.name()}_traceback", traceback.format_exc())


class ConditionalStage(BaseStage[T, T], Generic[T]):
    """Stage that conditionally processes data based on a predicate"""

    def __init__(
        self,
        condition_func: Callable[[T, PipelineContext], bool],
        next_stage: PipelineStage,
        stage_name: str = None,
    ):
        """
        Initialize with condition and next stage

        Args:
            condition_func: Function that takes input_data and context, returns bool
            next_stage: Stage to execute if condition is True
            stage_name: Optional name
        """
        super().__init__(stage_name or f"Conditional({next_stage.name()})")
        self.condition_func = condition_func
        self.next_stage = next_stage

    def _process_impl(self, input_data: T, context: PipelineContext) -> T:
        """Apply processing only if condition is met"""
        if self.condition_func(input_data, context):
            logger.info(f"Condition met, executing {self.next_stage.name()}")
            return self.next_stage.process(input_data, context)
        else:
            logger.info(f"Condition not met, skipping {self.next_stage.name()}")
            return input_data


class ChainStage(BaseStage[T, Any], Generic[T]):
    """Stage that chains multiple stages together"""

    def __init__(self, stages: List[PipelineStage], stage_name: str = None):
        """
        Initialize with a list of stages to chain

        Args:
            stages: List of stages to execute in sequence
            stage_name: Optional name
        """
        super().__init__(stage_name or "ChainStage")
        self.stages = stages

    def _process_impl(self, input_data: T, context: PipelineContext) -> Any:
        """Process input through all stages in sequence"""
        current_data = input_data

        for i, stage in enumerate(self.stages):
            stage_name = stage.name()
            logger.info(
                f"Executing chained stage {stage_name} ({i+1}/{len(self.stages)})"
            )
            current_data = stage.process(current_data, context)

        return current_data


class BranchStage(BaseStage[T, U], Generic[T, U]):
    """Stage that branches execution based on a selector function"""

    def __init__(
        self,
        selector_func: Callable[[T, PipelineContext], str],
        branches: Dict[str, PipelineStage],
        default_branch: Optional[PipelineStage] = None,
        stage_name: str = None,
    ):
        """
        Initialize with selector and branch stages

        Args:
            selector_func: Function that selects which branch to take
            branches: Dictionary mapping branch keys to stages
            default_branch: Optional stage to use if selector result not in branches
            stage_name: Optional name
        """
        super().__init__(stage_name or "BranchStage")
        self.selector_func = selector_func
        self.branches = branches
        self.default_branch = default_branch

    def _process_impl(self, input_data: T, context: PipelineContext) -> U:
        """Select and execute the appropriate branch"""
        branch_key = self.selector_func(input_data, context)
        context.set("selected_branch", branch_key)

        if branch_key in self.branches:
            branch = self.branches[branch_key]
            logger.info(f"Taking branch '{branch_key}' -> {branch.name()}")
            return branch.process(input_data, context)
        elif self.default_branch:
            logger.info(f"Taking default branch -> {self.default_branch.name()}")
            return self.default_branch.process(input_data, context)
        else:
            logger.warning(
                f"No branch found for key '{branch_key}' and no default branch"
            )
            return input_data
