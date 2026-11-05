import json
import re
from collections import Counter
from typing import Iterable
from pyflink.common import Types, WatermarkStrategy, Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction, ProcessFunction, OutputTag
from pyflink.datastream.window import TumblingEventTimeWindows, TimeWindow
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from src.fyp_attribution.pipeline import DatasetLoader
from src.fyp_attribution.core import load_config


# ==================== Timestamp Assigner ====================

class TweetTimestampAssigner(TimestampAssigner):
    @classmethod
    def extract_timestamp(self, value, record_timestamp) -> int:
        return int(value[2])


# ==================== Helper Functions ====================

def extract_hashtags(record) -> tuple:
    user_id, text, timestamp = record
    hashtags = re.findall(r'#(\w+)', text)
    return (user_id, hashtags, timestamp)


def key(record):
    return record[0]


def no_filter(record) -> bool:
    return True


# ==================== Process Functions ====================

# Side output tag for raw records (provenance)
raw_data_tag = OutputTag("raw-data", Types.STRING())

class PreWindowProcessFunction(ProcessFunction):
    def process_element(self, value, ctx):
        user_id, hashtags, timestamp = value
        
        # Convert hashtags list to JSON string
        hashtags_json = json.dumps(hashtags)
        
        # Main output: yield with string hashtags
        yield (user_id, hashtags_json, timestamp)
        
        # Side output: store raw record for provenance
        raw_record = json.dumps({
            'user_id': user_id,
            'hashtags': hashtags,
            'timestamp': timestamp
        })
        ctx.output(raw_data_tag, raw_record)

class HashtagAttributionProcessFunction(ProcessWindowFunction):
    def process(self, key, context, elements):
        user_id = key
        window_start = context.window().start
        window_end = context.window().end
        
        hashtag_counts = Counter()
        
        for record in elements:
            uid, hashtags_json, timestamp = record
            hashtags = json.loads(hashtags_json)  # Parse JSON string back to list
            hashtag_counts.update(hashtags)
        
        hashtag_json = json.dumps(dict(hashtag_counts))
        
        return [(user_id, hashtag_json, window_start, window_end)]
    

class MergeContributionsProcessFunction(ProcessWindowFunction):
    """Second aggregation: merge all users' contributions per hashtag per window."""
    
    def process(self, key, context, elements):
        hashtag = key
        window_start = context.window().start
        window_end = context.window().end
        
        total = 0
        contributions = {}
        
        for record in elements:
            user_id, user_count = record
            total += user_count
            contributions[user_id] = user_count
        
        contributions_json = json.dumps(contributions)
        
        return [(hashtag, total, contributions_json, window_start, window_end)]


# ==================== Flatten Function ====================

def flatten_hashtags(record):
    """Expand hashtag JSON into individual (hashtag, (user_id, count)) records."""
    user_id, hashtag_json, start, end = record
    hashtag_dict = json.loads(hashtag_json)
    for hashtag, count in hashtag_dict.items():
        yield (hashtag, (user_id, count), start, end)


# ==================== Main ====================

def main():
    APP_CONFIG = load_config()
    
    # Load dataset
    ds_loader = DatasetLoader()
    dataset_header, dataset_rows = ds_loader.read_csv()
    dataset_rows_list = list(dataset_rows)
    
    print(f"Headers: {dataset_header}")
    print(f"Loaded {len(dataset_rows_list)} records")
    
    # Setup environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)
    
    # Create data stream
    data_stream = env.from_collection(
        collection=dataset_rows_list,
        type_info=Types.TUPLE([Types.STRING(), Types.STRING(), Types.LONG()])
    )
    
    # Watermark strategy
    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
        .with_timestamp_assigner(TweetTimestampAssigner())
    
    # ===== Step 1: Pre-window process with side output =====
    pre_window_type = Types.TUPLE([
        Types.STRING(),  # user_id
        Types.STRING(),  # hashtags (as string)
        Types.LONG()     # timestamp
    ])
    
    pre_window_stream = data_stream \
        .assign_timestamps_and_watermarks(watermark_strategy) \
        .map(extract_hashtags) \
        .filter(no_filter) \
        .process(PreWindowProcessFunction(), pre_window_type)
    
    # Get side output (raw records for provenance)
    raw_data_stream = pre_window_stream.get_side_output(raw_data_tag)
    
    # ===== Step 2: First aggregation - per user per window =====
    first_agg_type = Types.TUPLE([
        Types.STRING(),  # user_id
        Types.STRING(),  # hashtag JSON
        Types.LONG(),    # window_start
        Types.LONG()     # window_end
    ])
    
    first_agg_stream = pre_window_stream \
        .key_by(key, key_type=Types.STRING()) \
        .window(TumblingEventTimeWindows.of(Time.seconds(300))) \
        .process(HashtagAttributionProcessFunction(), first_agg_type)
    
    # ===== Step 3: Flatten hashtag JSON =====
    flatten_type = Types.TUPLE([
        Types.STRING(),  # hashtag
        Types.TUPLE([Types.STRING(), Types.INT()]),  # (user_id, count)
        Types.LONG(),    # window_start
        Types.LONG()     # window_end
    ])
    
    flattened_stream = first_agg_stream.flat_map(flatten_hashtags, flatten_type)
    
    # ===== Step 4: Second aggregation - per hashtag per window =====
    second_agg_type = Types.TUPLE([
        Types.STRING(),  # hashtag
        Types.INT(),     # total count
        Types.STRING(),  # contributions JSON
        Types.LONG(),    # window_start
        Types.LONG()     # window_end
    ])
    
    final_stream = flattened_stream \
        .assign_timestamps_and_watermarks(watermark_strategy) \
        .key_by(lambda x: x[0], key_type=Types.STRING()) \
        .window(TumblingEventTimeWindows.of(Time.seconds(300))) \
        .process(MergeContributionsProcessFunction(), second_agg_type)
    
    # ===== Output all streams =====
    raw_data_stream.print("Raw Data (Provenance)")
    first_agg_stream.print("First Agg (Per User)")
    final_stream.print("Final Agg (Per Hashtag)")
    
    env.execute("Data Provenance Job")


if __name__ == "__main__":
    main()