from pyflink.common import Types, WatermarkStrategy, Time
from pyflink.common.configuration import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingProcessingTimeWindows
import re
import sys
import json

from src.fyp_attribution.core import load_config
from src.fyp_attribution.pipeline import (
    TweetTimestampAssigner,
    HashtagAttributionProcessFunction,
    CategoryAttributionProcessFunction,
    get_kafka_source,
    DatabaseBatchWriter,
    PrintDB
)

def parse_json(raw):
    try:
        data = json.loads(raw)
        return (data['user_id'], data['text'], data['timestamp'])
    except Exception as e:
        print(f"Parse error: {e}")
        return None
    
def extract_hashtags(record) -> tuple:
    user_id, text, timestamp = record
    
    # Find all words starting with #
    hashtags = re.findall(r'#(\w+)', text)
    # Convert list to JSON string for PyFlink serialization
    return (user_id, json.dumps(hashtags), timestamp)

def flatten_hashtags(record):
    """
    Flatten a tweet with multiple hashtags into multiple records.
    
    Input: (user_id, hashtags_json, timestamp) where hashtags_json = '["PYTHON", "DATA"]'
    Output: List of (category, user_id, timestamp) tuples
    
    Example:
        Input: ("user_1", '["PYTHON", "DATA"]', 1735696800000)
        Output: [
            ("PYTHON", "user_1", 1735696800000),
            ("DATA", "user_1", 1735696800000)
        ]
    """
    user_id, hashtags_json, timestamp = record
    hashtags = json.loads(hashtags_json)
    # Return list of (category, user_id, timestamp)
    return [(hashtag.upper(), user_id, timestamp) for hashtag in hashtags]

def key_by_category(record):
    """Key function to extract category (hashtag) from flattened record."""
    return record[0]  # category is at index 0 
    
def key(record):
    """Legacy key function - extract user_id from record."""
    return record[0]

def log_before_window(record):
    """Log records before entering window."""
    with open("window_debug.log", "a") as f:
        f.write(f"[BEFORE_WINDOW] {record}\n")
    return record

def has_python(record) -> bool:
    user_id, hashtags, timestamp = record
    return "python" in str(hashtags).lower()


def temp(record):
    return True

# Global database writer instance (initialized in main)
db_writer = None

def write_to_db(record):
    """Write window result to database using batch writer."""
    global db_writer
    if db_writer is None:
        # Initialize on first call (happens in worker process)
        db = PrintDB()
        db_writer = DatabaseBatchWriter(db=db, collection_name="attribution_results", batch_size=1)
    
    db_writer.write(record)
    return record

def write_category_to_db(record):
    """
    Write category-based window result to database.
    
    Input format: (window_start, window_end, category, output_count, attribution_json)
    Output format: Same as input (pass-through for database writing)
    """
    global db_writer
    if db_writer is None:
        db = PrintDB()
        db_writer = DatabaseBatchWriter(db=db, collection_name="window_outputs", batch_size=1)
    
    db_writer.write(record)
    return record

# Debug logging functions
def log_parsed(record):
    """Log parsed JSON records."""
    if record:
        user_id, text, timestamp = record
        print(f"[PARSE] user={user_id}, text={text[:30]}..., ts={timestamp}", flush=True)
    return record

def log_hashtags(record):
    """Log extracted hashtags."""
    user_id, hashtags_json, timestamp = record
    print(f"[HASHTAG] user={user_id}, hashtags={hashtags_json}, ts={timestamp}", flush=True)
    return record

def log_keyed(record):
    """Log keyed records."""
    print(f"[KEY] record={record}", flush=True)
    return record[0]

def legacy_main():
    """Legacy workflow: groups by user_id, outputs all hashtags per user."""
    try:
        # 1. Load config
        config = load_config("configs/config.yaml")

        # 2. Configure PyFlink
        flink_config = Configuration()
        flink_config.set_string("python.executable", sys.executable)
                
        # Add Kafka JAR
        jar_path = config.project_root / config.kafka.sql_connector_jar
        jar_uri = f"file:///{str(jar_path).replace(chr(92), '/')}"
        flink_config.set_string("pipeline.jars", jar_uri)
                
        # Windows stability settings
        flink_config.set_string("taskmanager.numberOfTaskSlots", "1")
        flink_config.set_string("parallelism.default", "1")

        env = StreamExecutionEnvironment.get_execution_environment(flink_config)
        env.set_parallelism(1)  # Force parallelism=1 for Windows stability

        # 3. Create Kafka streaming source
        kafka_source = get_kafka_source(
            config.kafka.topic,
            config.kafka.bootstrap_servers,
            config.kafka.consumer_group
        )
        
        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
            .with_timestamp_assigner(TweetTimestampAssigner())
        
        data_stream = env.from_source(
            source=kafka_source,
            watermark_strategy=watermark_strategy,
            source_name="Kafka tweets Source"
        )

        # 4. Process stream with windowing (LEGACY: group by user_id)
        print("\n[INFO] Starting LEGACY stream processing...")
        print("[INFO] Using 5-second PROCESSING TIME windows for TESTING")
        print("[INFO] Grouping by USER_ID, outputting all hashtags per user")
        print("[INFO] Results will be written to database via batch writer\n")
        
        ds = data_stream \
            .map(parse_json, output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.LONG()])) \
            .filter(lambda x: x is not None) \
            .map(extract_hashtags, output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.LONG()])) \
            .key_by(key, key_type=Types.STRING()) \
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5))) \
            .process(HashtagAttributionProcessFunction(),
                    Types.TUPLE([Types.STRING(), Types.STRING(), Types.LONG(), Types.LONG()])) \
            .map(write_to_db, output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.LONG(), Types.LONG()]))

        print("[INFO] Executing job... (will stream continuously)\n")
        ds.print()
        
        try:
            env.execute("Legacy Kafka Hashtag Attribution")
        finally:
            # Cleanup: close database writer
            if db_writer is not None:
                print("\n[INFO] Closing database writer...")
                db_writer.close()

    
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


def main():
    """
    New workflow: flattens tweets by hashtag, groups by category.
    Output format matches window_outputs table schema:
    (window_start, window_end, category, output_count, attribution_json)
    """
    try:
        # 1. Load config
        config = load_config("configs/config.yaml")

        # 2. Configure PyFlink
        flink_config = Configuration()
        flink_config.set_string("python.executable", sys.executable)
                
        # Add Kafka JAR
        jar_path = config.project_root / config.kafka.sql_connector_jar
        jar_uri = f"file:///{str(jar_path).replace(chr(92), '/')}"
        flink_config.set_string("pipeline.jars", jar_uri)
                
        # Windows stability settings
        flink_config.set_string("taskmanager.numberOfTaskSlots", "1")
        flink_config.set_string("parallelism.default", "1")

        env = StreamExecutionEnvironment.get_execution_environment(flink_config)
        env.set_parallelism(1)  # Force parallelism=1 for Windows stability

        # 3. Create Kafka streaming source
        kafka_source = get_kafka_source(
            config.kafka.topic,
            config.kafka.bootstrap_servers,
            config.kafka.consumer_group
        )
        
        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
            .with_timestamp_assigner(TweetTimestampAssigner())
        
        data_stream = env.from_source(
            source=kafka_source,
            watermark_strategy=watermark_strategy,
            source_name="Kafka tweets Source"
        )

        # 4. Process stream with windowing (NEW: group by category/hashtag)
        print("\n[INFO] Starting NEW stream processing...")
        print("[INFO] Using 5-second PROCESSING TIME windows for TESTING")
        print("[INFO] Flattening tweets and grouping by CATEGORY (hashtag)")
        print("[INFO] Output format: (window_start, window_end, category, output_count, attribution_json)")
        print("[INFO] Results will be written to window_outputs table\n")
        
        ds = data_stream \
            .map(parse_json, output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.LONG()])) \
            .filter(lambda x: x is not None) \
            .map(extract_hashtags, output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.LONG()])) \
            .flat_map(flatten_hashtags, output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.LONG()])) \
            .key_by(key_by_category, key_type=Types.STRING()) \
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5))) \
            .process(CategoryAttributionProcessFunction(),
                    Types.TUPLE([Types.LONG(), Types.LONG(), Types.STRING(), Types.LONG(), Types.STRING()])) \
            .map(write_category_to_db, 
                 output_type=Types.TUPLE([Types.LONG(), Types.LONG(), Types.STRING(), Types.LONG(), Types.STRING()]))

        print("[INFO] Executing job... (will stream continuously)\n")
        ds.print()
        
        try:
            env.execute("Category-based Hashtag Attribution")
        finally:
            # Cleanup: close database writer
            if db_writer is not None:
                print("\n[INFO] Closing database writer...")
                db_writer.close()

    
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

