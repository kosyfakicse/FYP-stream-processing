








class IDatabase:
    def write(self, record_collection_name):
        ...

    def bulk_write(self, records, collection_name):
        ...

class PrintDB(IDatabase):
    def __init__(self):
        super().__init__()

    def bulk_write(self, records, collection_name):
        print(f"Total {len(records)} records insert into {collection_name} ")

class DuckDB(IDatabase):
    def __init__(self):
        ...

        
class DatabaseBatchWriter:
    def __init__(self, db: IDatabase, collection_name: str, batch_size: int = 100):
        self.db = db
        self.batch = []
        self.batch_size = batch_size
        self.collection_name = collection_name

    def write(self, record) -> None:
        self.batch.append(record)
        if len(self.batch) >= self.batch_size:
            self._flush()

    def close(self):
        """Flush any remaining records and close the writer."""
        if len(self.batch) > 0:
            self._flush()

    def _flush(self):
        # Bulk Write
        self.db.bulk_write(self.batch, self.collection_name)

        # reset
        self.batch = []

def main():
    db = PrintDB()
    writer = DatabaseBatchWriter(db=db, collection_name="tweets", batch_size=5)

    # Test data: window results format (user_id, hashtag_json, window_start, window_end)
    records = [
        ("user_1", '{"PYTHON": 10, "DATA": 5}', 1793994000000, 1793994005000),
        ("user_2", '{"KAFKA": 8, "FLINK": 12}', 1793994000000, 1793994005000),
        ("user_3", '{"AI": 15, "ML": 7}', 1793994000000, 1793994005000),
        ("user_1", '{"PYTHON": 12, "DATA": 8}', 1793994005000, 1793994010000),
        ("user_2", '{"KAFKA": 10, "FLINK": 9}', 1793994005000, 1793994010000),
        ("user_3", '{"AI": 18, "ML": 6}', 1793994005000, 1793994010000),
        ("user_1", '{"PYTHON": 8, "DATA": 11}', 1793994010000, 1793994015000),
    ]

    print("Testing DatabaseBatchWriter with PrintDB")
    print(f"Batch size: {writer.batch_size}")
    print(f"Total records to write: {len(records)}")
    print("=" * 60)

    # Write records one by one
    for i, record in enumerate(records, 1):
        print(f"\n[Record {i}] Writing: {record}")
        writer.write(record)
        print(f"  Current batch size: {len(writer.batch)}")

    # Close writer (flushes remaining records)
    print(f"\n[Closing Writer] Flushing remaining {len(writer.batch)} records...")
    writer.close()

    print("\n" + "=" * 60)
    print("Test complete!")


if __name__ == "__main__":
    main()