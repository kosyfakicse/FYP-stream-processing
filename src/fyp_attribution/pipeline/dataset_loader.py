import csv
import json
from pathlib import Path
from typing import Iterator
from loguru import logger

from fyp_attribution.core.config import load_config

class DatasetLoader:
    """Load Dataset from CSV and send to Kafka"""

    def __init__ (self, config_path: str ="configs/config.yaml"):
        self.config = load_config()
        self.dataset_config = self.config.dataset.get_dataset_config()

    def __str__ (self):
        return "Dataset Object"

    def read_csv(self) -> tuple[tuple[str, ...], Iterator[tuple]]:
        """Read CSV file and return headers and iterator of data tuples.
        
        Returns:
            tuple: (headers_tuple, iterator_of_data_tuples)
            Example:
                headers = ("user_id", "text", "timestamp")
                data_iter = <iterator yielding tuples>
        """
        csv_path = Path(self.dataset_config.file)
        
        if not csv_path.exists():
            raise FileNotFoundError(f"Dataset not found: {csv_path}")
        
        logger.info(f"Reading dataset from {csv_path}")
        
        def data_iterator():
            """Generator that yields one row at a time."""
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                next(reader)  # Skip header row
                
                for row in reader:
                    yield (
                        row[0],           # user_id (str)
                        row[1],           # text (str)
                        int(row[2])       # timestamp (int)
                    )
        
        # Read headers separately
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            headers = tuple(next(reader))
        
        return headers, data_iterator()
        

    def run(self) -> int:
        pass


if __name__ == "__main__":
    dataset_loader = DatasetLoader() 
    dataset = dataset_loader.read_csv()
    dataset_headers = dataset[0]
    dataset_rows = dataset[1]

    print(dataset_headers)
    for row in dataset_rows:
        print(row)

