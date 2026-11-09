from pyflink.common import Types, WatermarkStrategy, Time
from pyflink.common.configuration import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingProcessingTimeWindows
import re
import sys
import json

from src.fyp_attribution.storage.duckdb_adapter import DuckDB
from src.fyp_attribution.core import load_config
from src.fyp_attribution.pipeline import (
    TweetTimestampAssigner,
    HashtagAttributionProcessFunction,
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
    
def key(record):
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
        db = DuckDB()
        db_writer = DatabaseBatchWriter(db=db, collection_name="attribution_results", batch_size=1)
    
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

def main():

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

        # 4. Process stream with windowing
        print("\n[INFO] Starting stream processing...")
        print("[INFO] Using 5-second PROCESSING TIME windows for TESTING")
        print("[INFO] Windows will trigger every 5 seconds based on system clock")
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
            env.execute("Kafka Hashtag Attribution with Database Writer")
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

