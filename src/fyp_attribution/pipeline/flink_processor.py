"""Flink stream processor for the FYP attribution pipeline."""

import sys
from typing import Callable
from pyflink.common.time import Time, Duration
from pyflink.common import WatermarkStrategy
from pyflink.common.configuration import Configuration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import DataStream, StreamExecutionEnvironment
from pyflink.datastream.connectors import Source
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.datastream.data_stream import KeyedStream, WindowedStream
from pyflink.datastream.window import (
    TumblingEventTimeWindows,
    SlidingEventTimeWindows
)

from src.fyp_attribution.core.config import FlinkConfig, load_config


from pyflink.common.watermark_strategy import TimestampAssigner
class CustomTimestampAssigner(TimestampAssigner):
    """Extract timestamp from record."""

    # override
    def extract_timestamp(self, record, record_timestamp: int) -> int:
        """Return the timestamp from the record.
        assume index 2 must be timestamp
        """
        return record[2]


from pyflink.datastream.functions import AggregateFunction
class AttributionCount(AggregateFunction):
    """
    """

    def create_accumulator(self):
        return {"user_id": None, "count": 0}
    
    def add(self, value, acc):
        user_id = value[0]
        acc["user_id"] = user_id
        acc["count"] += 1
        return acc
    
    def get_result(self, acc):
        return acc
    
    def merge(self, acc_a, acc_b):
        return {
            "user_id": acc_a["user_id"],
            "count": acc_a["count"] + acc_b["count"]
        }


class FlinkProcessor:
    """Functional-style Flink processor.

    Each method takes a DataStream and returns a DataStream.
    No internal state tracking -- caller manages the stream variable.
    """

    def __init__(self, flink_config: FlinkConfig):
        self.config = flink_config
        self.env = None

    def setup_environment(self) -> None:
        """Initialize Flink execution environment."""
        config = Configuration()
        # Tell Flink workers to use the same Python executable as the driver
        config.set_string("python.executable", sys.executable)
        self.env = StreamExecutionEnvironment.get_execution_environment(config)
        self.env.set_runtime_mode(self._get_execute_mode(self.config.mode))
        self.env.set_parallelism(self.config.parallelism)

    def from_collection(self, collection: list) -> DataStream:
        """Create a DataStream from a Python collection."""
        return self.env.from_collection(collection)

    def from_source(
        self,
        source: Source,
        source_name: str,
        watermark_strategy: WatermarkStrategy = WatermarkStrategy.for_monotonous_timestamps(),
    ) -> DataStream:
        """Create a DataStream from a Flink source (e.g. Kafka)."""
        return self.env.from_source(
            source=source,
            source_name=source_name,
            watermark_strategy=watermark_strategy,
        )

    def assign_timestamp_and_watermarks(self, stream: DataStream, assigner: TimestampAssigner):
        return stream.assign_timestamps_and_watermarks(
            WatermarkStrategy
            #.for_monotonous_timestamps()
            .for_bounded_out_of_orderness(Duration.of_seconds(10))
            .with_timestamp_assigner(assigner)
        ) 

    def key_by(self, stream: DataStream, operator: Callable) -> KeyedStream:
        return stream.key_by(operator)

    def map(self, stream: DataStream, operator: Callable) -> DataStream:
        """Apply a map operator to the stream."""
        return stream.map(operator)

    def filter(self, stream: DataStream, predicate: Callable) -> DataStream:
        """Apply a filter to the stream."""
        return stream.filter(predicate)

    def window_tumbling(self, stream: KeyedStream, size_seconds: int) -> WindowedStream:
        return stream.window(
            TumblingEventTimeWindows.of(Time.seconds(size_seconds)) 
        )

    def window_sliding(self, stream: KeyedStream, size_seconds: int, slide_seconds: int) -> WindowedStream:
        return stream.window(
            SlidingEventTimeWindows.of(Time.seconds(size_seconds), Time.seconds(slide_seconds))
        )

    def aggregate(self, stream: WindowedStream, aggregate_function: AggregateFunction) -> DataStream:
        return stream.aggregate(aggregate_function)

    def execute(self, job_name: str = "Flink Job") -> None:
        """Execute the Flink job."""
        if self.env is None:
            raise RuntimeError("Environment not set up. Call setup_environment() first.")
        self.env.execute(job_name)

    def execute_and_collect(self, stream: DataStream) -> list:
        temp = None
        with stream.execute_and_collect() as results:
            temp = list(results)
        return temp

    def _get_execute_mode(self, mode: int = 1) -> RuntimeExecutionMode:
        """Return the execution mode. Default and unexpected mode go to BATCH."""
        match mode:
            case 0:
                return RuntimeExecutionMode.STREAMING
            case 1:
                return RuntimeExecutionMode.BATCH
            case 2:
                return RuntimeExecutionMode.AUTOMATIC
            case _:
                return RuntimeExecutionMode.BATCH


def main():
    """Test FlinkProcessor with sample data."""
    APP_CONFIG = load_config()

    test_collection = [
        (1, "Python"),
        (2, "AI"),
        (3, "Programming"),
    ]
    swap = lambda x: (x[1], x[0])

    fp = FlinkProcessor(flink_config=APP_CONFIG.flink)
    fp.setup_environment()

    stream = fp.from_collection(test_collection)
    stream = fp.map(stream, operator=swap)

    stream.print()
    fp.execute("Test Job")


if __name__ == "__main__":
    main()
