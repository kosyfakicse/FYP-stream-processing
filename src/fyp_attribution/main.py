from src.fyp_attribution.core.config import load_config
from src.fyp_attribution.pipeline import DatasetLoader, FlinkProcessor


def main():
    APP_CONFIG = load_config()
    swap = lambda x :(x[1], x[0], x[2])
    ds_loader = DatasetLoader()
    dataset_header, dataset_rows = ds_loader.read_csv()
    print(dataset_header)
    fp = FlinkProcessor(flink_config=APP_CONFIG.flink)
    fp.setup_environment().from_collection(dataset_rows).map(swap).current_stream.print()
    fp.execute("Main Job")
  
 
if __name__ == "__main__":
    main()