"""Main entry point for the FYP attribution pipeline."""

from src.fyp_attribution.core.config import load_config
from src.fyp_attribution.pipeline import DatasetLoader, FlinkProcessor, CustomTimestampAssigner, AttributionCount

from src.fyp_attribution.core import TweetContribution

def main():

    #
    filtered_result = None
    windowed_result = None

    # Load configuration
    APP_CONFIG = load_config()

    # Load dataset
    ds_loader = DatasetLoader()
    dataset_header, dataset_rows = ds_loader.read_csv()
    dataset_rows_list = list(dataset_rows)

    print(f"Headers: {dataset_header}")
    print(f"Loaded {len(dataset_rows_list)} records")

    # Define operators
    def key(record):
        return record[0]

    def has_python(record) -> bool:
        user_id, text, timestamp = record
        return "python" in text.lower()

    # Setup Flink processor
    fp = FlinkProcessor(flink_config=APP_CONFIG.flink)
    fp.setup_environment()

    # Build pipeline
    stream = fp.from_collection(dataset_rows_list)
    stream = fp.assign_timestamp_and_watermarks(stream, CustomTimestampAssigner())
    stream = fp.filter(stream, has_python)

    filtered_result = fp.execute_and_collect(stream)

    #with stream.execute_and_collect() as results:
     #   print("After Filter:")
      #  for i, result in enumerate(results):
       #     print(f"[{i}]:{result}")
        #print()
    
    stream = fp.key_by(stream, key)
    stream = fp.window_tumbling(stream, 60)
    stream = fp.aggregate(stream, AttributionCount())
    windowed_result = fp.execute_and_collect(stream)
    
    #result = stream.aggregate(AttributionCount())
    #
    #temp = None
    #with result.execute_and_collect() as results:
    #    temp = list(results)
    print("\n--- Job completed ---")

    print(f"filtered result: ")
    for i, record in enumerate(filtered_result):
        print(f"{i}: {record}")

    print(f"windowed result: ")
    for i, record in enumerate(filtered_result):
        print(f"{i}: {record}")


if __name__ == "__main__":
    main()
