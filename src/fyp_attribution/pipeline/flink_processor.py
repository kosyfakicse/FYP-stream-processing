from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.datastream import DataStream
from pyflink.datastream.connectors import Source
from pyflink.common import WatermarkStrategy
from typing import Callable

from src.fyp_attribution.core.config import load_config, FlinkConfig

class FlinkProcessor:
    def __init__(self, flink_config: FlinkConfig):
        self.config = flink_config
        self.env = None
        self.current_stream = None
        self.sink = None

    def setup_environment(self):
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_runtime_mode(self._get_execute_mode(self.config.mode))
        self.env.set_parallelism(self.config.parallelism)
        return self

    def from_collection(self, collection: list[tuple]):
        self.current_stream = self.env.from_collection(collection=collection)
        return self

    def from_source(
        self,
        source: Source,
        source_name: str,
        watermark_strategy: WatermarkStrategy = WatermarkStrategy.for_monotonous_timestamps()
        ):
        self.current_stream = self.env.from_source(
            source=source,
            source_name=source_name,
            watermark_strategy=watermark_strategy
        )

        return self


    def map(self, operator: Callable[[tuple], tuple]):
        if self.current_stream is None:
            raise Exception()
        self.current_stream = self.current_stream.map(operator)
        return self

    def filter(self, predicate):
        if self.current_stream is None:
            raise Exception()
        self.current_stream = self.current_stream.filter(predicate)
        return self

    def window(self, window_assigner):
        if self.current_stream is None:
            raise Exception()
        self.current_stream = self.current_stream.window(window_assigner)
        return self

    def sink_to(self, sink):
        if self.current_stream is None:
            raise Exception()
        self.current_stream.sink_to(sink)
        return self

    def execute(self, job_name: str ="Running a Job") -> None:
        if self.env is None:
            raise Exception()
        self.env.execute(job_name)

    def _get_execute_mode(self, mode: int = 1):
        """Return the execution mode, default and unexpected mode will go BATCH mode"""
        match mode:
            case 0:
                return RuntimeExecutionMode.STREAMING
            case 1:
                return RuntimeExecutionMode.BATCH
            case 2:
                return RuntimeExecutionMode.AUTOMATIC
            case _:
                return RuntimeExecutionMode.BATCH

    def run():
        pass

if __name__ == "__main__":
    # load config
    APP_CONFIG = load_config()
    # testing components
    test_collection = [
        (1, "Python"),
        (2, "AI"),
        (3, "Programming")
    ]
    swap = lambda x: (x[1], x[0])

    fp = FlinkProcessor(flink_config=APP_CONFIG.flink)
    fp.setup_environment().from_collection(test_collection).map(swap).current_stream.print()
    fp.execute("Hi this is job")


        




        



