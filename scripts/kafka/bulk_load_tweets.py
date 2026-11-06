# scripts/kafka/bulk_load_tweets.py
"""Bulk load dataset into Kafka storage topic (instant, no delays)."""

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import json
from confluent_kafka import Producer
from src.fyp_attribution.pipeline import DatasetLoader

KAFKA_SERVER = 'localhost:9092'
STORAGE_TOPIC = 'tweets-storage'


def bulk_load(csv_path):
    """Load entire dataset into Kafka storage topic immediately."""
    # Load data
    loader = DatasetLoader()
    header, dataset = loader.read_csv()
    dataset = list(dataset)
    
    print("=" * 60)
    print("Kafka Bulk Loader")
    print("=" * 60)
    print(f"Dataset: {len(dataset)} records")
    print(f"Storage topic: {STORAGE_TOPIC}")
    print(f"Mode: INSTANT (no delays)")
    print()
    
    # Create producer
    producer = Producer({'bootstrap.servers': KAFKA_SERVER})
    
    # Send all records immediately to storage topic
    for i, record in enumerate(dataset, 1):
        user_id, text, timestamp = record
        message = {
            'user_id': user_id,
            'text': text,
            'timestamp': timestamp,
            'partition_key': user_id
        }
        
        producer.produce(
            STORAGE_TOPIC,
            key=user_id.encode('utf-8'),
            value=json.dumps(message).encode('utf-8')
        )
        
        if i % 100 == 0:
            producer.flush()
            print(f"Loaded {i}/{len(dataset)} messages...")
    
    # Final flush
    producer.flush()
    
    print()
    print("=" * 60)
    print(f"Bulk load complete!")
    print(f"  Total messages: {len(dataset)}")
    print(f"  Stored in topic: {STORAGE_TOPIC}")
    print("=" * 60)
    print()
    print("Next steps:")
    print("  1. Clear tweets-raw topic (if needed):")
    print("     docker exec fyp-kafka /opt/kafka/bin/kafka-topics.sh --delete --topic tweets-raw --bootstrap-server localhost:9092")
    print("     docker exec fyp-kafka /opt/kafka/bin/kafka-topics.sh --create --topic tweets-raw --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1")
    print()
    print("  2. Run replay script to stream at controlled rate:")
    print("     python scripts/kafka/replay_tweets.py")
    print()
    print("  3. Run PyFlink pipeline (in another terminal):")
    print("     python -m src.fyp_attribution.main")


if __name__ == "__main__":
    bulk_load('data/raw/tweet_hashtag.csv')
