from __future__ import annotations
from abc import ABC
from enum import Enum, auto

import numpy as np


class BaseDataLoader(ABC):
    def provide(self) -> np.ndarray:
        ...

    def post_process(self, data: np.ndarray) -> np.ndarray:
        ...


class ProcessorStage(Enum):
    PENDING = auto()
    PREPROCESS = auto()
    PROCESS = auto()
    POSTPROCESS = auto()


class BaseProcessor(ABC):
    def pre_process(self, data: np.ndarray) -> np.ndarray:
        ...

    def process(self, data: np.ndarray) -> np.ndarray:
        ...

    def post_process(self, data: np.ndarray) -> np.ndarray:
        ...


class PipelineStage(Enum):
    PENDING = auto()
    PROVIDE_DATA = auto()
    PROCESS = auto()
    FINALIZE = auto()
    FINISHED = auto()


class BasePipeline(ABC):

    data_loader: BaseDataLoader
    processor: BaseProcessor

    def setup(self, config: dict) -> None:
        ...

    def execute(self) -> None:
        ...
