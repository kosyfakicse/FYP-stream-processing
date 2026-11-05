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

    # Filtered results
    stream1 = fp.from_collection(dataset_rows_list)
    stream1 = fp.assign_timestamp_and_watermarks(stream1, CustomTimestampAssigner())
    stream1 = fp.filter(stream1, has_python)
    filtered_result = fp.execute_and_collect(stream1)

    print("\n--- Pipeline 1 ---")
    print(f"filtered result: ")
    for i, record in enumerate(filtered_result):
        print(f"{i}: {record}")

    # Keyed results
    stream2 = fp.key_by(stream1, key)
    keyed_result = fp.execute_and_collect(stream2)

    import pickle
    print(f"Keyed result: ")
    for i, row in enumerate(keyed_result):
        record = (pickle.loads(row.f0) , pickle.loads(row.f1)[1]  ,pickle.loads(row.f1)[2])
        print(f"{i}: {record}")


    # Window output
    # Pipeline 3: Windowed aggregation (separate pipeline)
    fp.setup_environment()
    dataset_rows_list.sort(key=lambda x: x[2])
    stream3 = fp.from_collection(dataset_rows_list)
    stream3 = fp.assign_timestamp_and_watermarks(stream3, CustomTimestampAssigner())
    stream3 = fp.filter(stream3, has_python)
    stream3 = fp.key_by(stream3, key)
    stream3 = fp.window_tumbling(stream3, 60)
    stream3 = fp.aggregate(stream3, AttributionCount())
    print("\n--- Pipeline 3: Windowed ---")
    stream3.print()
    fp.execute("Windowed Job")



    """
    # Pipeline 2: Windowed aggregation (separate pipeline)
    fp.setup_environment()  # Reset environment
    stream2 = fp.from_collection(dataset_rows_list)
    stream2 = fp.assign_timestamp_and_watermarks(stream2, CustomTimestampAssigner())
    stream2 = fp.filter(stream2, has_python)
    stream2 = fp.key_by(stream2, key)
    stream2 = fp.window_tumbling(stream2, 60)
    stream2 = fp.aggregate(stream2, AttributionCount())

    # Use print + execute for event time windows
    stream2.print()
    fp.execute("Windowed Job")

    
    """

    #print(f"windowed result: ")
    #for i, record in enumerate(windowed_result):
     #   print(f"{i}: {record}")


if __name__ == "__main__":
    main()
