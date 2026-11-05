from collections import Counter
from typing import Iterable
from pyflink.common import Types, WatermarkStrategy, Time, Encoder
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows, TimeWindow, TumblingProcessingTimeWindows
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from src.fyp_attribution.pipeline import DatasetLoader
from src.fyp_attribution.core import load_config


class TweetTimestampAssigner(TimestampAssigner):
    """
    Timestamp assigner for (user_id, text, timestamp)
    index = 2
    """
    @classmethod
    def extract_timestamp(self, value, record_timestamp) -> int:
        return int(value[2])

class CountWindowProcessFunction(ProcessWindowFunction[tuple, tuple, str, TimeWindow]):
    def process(self,
                key: str,
                context: ProcessWindowFunction.Context[TimeWindow],
                elements: Iterable[tuple]) -> Iterable[tuple]:
        return [(key, len([e for e in elements]), context.window().start, context.window().end)]

import json
class HashtagAttributionProcessFunction(ProcessWindowFunction):
    def process(self, key, context, elements):
        user_id = key
        window_start = context.window().start
        window_end = context.window().end
        
        hashtag_counts = Counter()
        
        for record in elements:
            uid, hashtags, timestamp = record
            hashtag_counts.update(hashtags)
        
        # Convert to JSON string
        hashtag_json = json.dumps(dict(hashtag_counts))
        # Example: '{"PYTHON": 3, "DATA": 2}'
        
        return [(user_id, hashtag_json, window_start, window_end)]



import re
def extract_hashtags(record) -> tuple:
    user_id, text, timestamp = record
    
    # Find all words starting with #
    hashtags = re.findall(r'#(\w+)', text)
    return (user_id, hashtags, timestamp) 
    
def key(record):
    return record[0]

def has_python(record) -> bool:
    user_id, hashtags, timestamp = record
    return "python" in str(hashtags).lower()

def all(record):
    return True



def main():

    APP_CONFIG = load_config()
    # Load dataset
    ds_loader = DatasetLoader()
    dataset_header, dataset_rows = ds_loader.read_csv()
    dataset_rows_list = list(dataset_rows)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)

    data_stream = env.from_collection(
        collection=dataset_rows_list,
        type_info=Types.TUPLE([Types.STRING(), Types.STRING(), Types.LONG()])
    )

    # define the watermark strategy
    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
        .with_timestamp_assigner(TweetTimestampAssigner())

    ds = data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
        .map(extract_hashtags) \
        .filter(all) \
        .key_by(key, key_type=Types.STRING()) \
        .window(TumblingEventTimeWindows.of(Time.seconds(300))) \
        .process(HashtagAttributionProcessFunction(),
                 Types.TUPLE([Types.STRING(), Types.STRING(), Types.LONG(), Types.LONG()])) 

    ds.print()
    env.execute()
    
    
if __name__ == "__main__":
    main()

