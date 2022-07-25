from __future__ import annotations
from time import sleep

from dramatiq import Actor, Message
from dramatiq.results.errors import ResultMissing
from ..analysis_base.base import *
from ..dram_app import r_backend, r_broker


class CumSumDataLoader(BaseDataLoader, Actor):
    def __init__(self):
        super().__init__(
            fn=self.fn,
            broker=r_broker,
            actor_name=__class__.__name__,
            queue_name="default",
            priority=0,
            options={"store_results": True},
        )

    def fn(self, nd_data: np.ndarray) -> np.ndarray:
        sleep(1)
        return np.arange(100).reshape(2, 50)


class CumSumProcessor(BaseProcessor, Actor):
    def __init__(self) -> None:
        self.current_stage: ProcessorStage = ProcessorStage.PREPROCESS
        self.stage2task = {
            ProcessorStage.PREPROCESS: self.pre_process,
            ProcessorStage.PROCESS: self.process,
            ProcessorStage.POSTPROCESS: self.post_process,
        }
        super().__init__(
            fn=self.fn,
            broker=r_broker,
            actor_name=__class__.__name__,
            queue_name="default",
            priority=0,
            options={"store_results": True},
        )

    def pre_process(self, nd_data: np.ndarray) -> np.ndarray:
        sleep(1)
        return nd_data

    def process(self, nd_data: np.ndarray) -> np.ndarray:
        sleep(1)
        data = np.array(nd_data)
        return data.cumsum(axis=0).tolist()

    def post_process(self, nd_data: np.ndarray) -> np.ndarray:
        sleep(1)
        return nd_data

    def fn(self, nd_data: np.ndarray) -> np.ndarray:

        data = self.pre_process(nd_data)
        data1 = self.process(data)
        data2 = self.post_process(data1)
        return data2


cum_sum_loader = CumSumDataLoader()
cum_sum_processor = CumSumProcessor()


class CumSumPipeline(BasePipeline):

    data_loader: CumSumDataLoader
    processor: CumSumProcessor

    def __init__(self, config: dict):
        self.data_loader = cum_sum_loader
        self.processor = cum_sum_processor
        self.config = config
        self.pipeline_name = config["name"]
        self.priority = config["priority"]
        self.delay = None
        if "delay" in config:
            self.delay = config["delay"]
        self.current_state = PipelineStage.PENDING
        self.loader_result: Message | None = None
        self.processor_result: Message | None = None
        self.initial_data = np.arange(20)

    @property
    def finished(self) -> bool:
        return self.current_state == PipelineStage.FINISHED

    def execute(self) -> bool:
        try:

            if self.current_state == PipelineStage.PENDING:
                print(f"{self.config['name']} started.")
                self.current_state = PipelineStage.PROVIDE_DATA
                if self.delay:
                    self.loader_result = self.data_loader.send_with_options(
                        kwargs={"nd_data": self.initial_data}, delay=5, priority=self.priority, name=self.pipeline_name
                    )
                else:
                    self.loader_result = self.data_loader.send_with_options(
                        kwargs={"nd_data": self.initial_data}, priority=self.priority, name=self.pipeline_name
                    )
                print(f"{self.pipeline_name} with msg_id: {self.loader_result.message_id}")

            elif self.loader_result and self.current_state == PipelineStage.PROVIDE_DATA:
                print("in execute")
                res = self.loader_result.get_result()
                self.current_state = PipelineStage.PROCESS
                self.processor_result = self.processor.send_with_options(
                    kwargs={"nd_data": res}, priority=self.priority
                )

            elif self.processor_result and self.current_state == PipelineStage.PROCESS:
                res = self.processor_result.get_result()
                self.current_state = PipelineStage.FINALIZE
                self.current_state = PipelineStage.FINISHED

                print("result: ", res[:2])
                print(f"{self.config['name']} finished.")
        except ResultMissing as ex:
            print(f"{self.config['name']} not ready yet.")
        except ValueError as ex:
            print(f"in {self.config['name']}, error happened: ", ex)

        return self.finished
