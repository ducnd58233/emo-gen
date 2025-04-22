from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, Optional, TypeVar

T = TypeVar("T")
U = TypeVar("U")


class PipelineContext:
    """Context shared across pipeline stages"""

    def __init__(self, **kwargs):
        self._data = {}
        for key, value in kwargs.items():
            self._data[key] = value

    def set(self, key: str, value: Any) -> None:
        self._data[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def update(self, values: Dict[str, Any]) -> None:
        self._data.update(values)


class PipelineStage(Generic[T, U], ABC):
    """Abstract base class for pipeline stages"""

    @abstractmethod
    def process(self, data: T, context: PipelineContext) -> U:
        pass

    @abstractmethod
    def name(self) -> str:
        pass


class Pipeline(ABC):
    """Abstract base class for pipelines"""

    @abstractmethod
    def add_stage(self, stage: PipelineStage) -> 'Pipeline':
        """Add a stage to the pipeline"""
        pass

    @abstractmethod
    def execute(self, input_data: Any, context: Optional[PipelineContext] = None) -> Any:
        """Execute pipeline on input data"""
        pass

    @abstractmethod
    def name(self) -> str:
        """Return name of this pipeline"""
        pass
