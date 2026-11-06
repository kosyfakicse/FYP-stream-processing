# scripts/kafka/produce_tweets.py
import json
from confluent_kafka import Producer
from src.fyp_attribution.pipeline import DatasetLoader

KAFKA_SERVER = 'localhost:9092'
TWEETS_TOPIC = 'tweets-raw'

def produce_tweets(csv_path):
    """Read CSV and send to Kafka"""
    # Load data
    loader = DatasetLoader()
    header , dataset = loader.read_csv()
    dataset = list(dataset)
    print(f"Loaded {len(dataset)} records from {csv_path}")
    
    # Create producer
    producer = Producer({'bootstrap.servers': KAFKA_SERVER})
    
    # Send each record
    for i, record in enumerate(dataset):
        user_id, text, timestamp = record
        message = {
            'user_id': user_id,
            'text': text,
            'timestamp': timestamp,
            'partition_key': user_id  # Same user goes to same partition
        }
        
        producer.produce(
            TWEETS_TOPIC,
            key=user_id.encode('utf-8'),
            value=json.dumps(message).encode('utf-8')
        )
        
        if i % 100 == 0:
            producer.flush()
            print(f"Sent {i} messages...")
    
    # Final flush
    producer.flush()
    print(f"\nAll {len(dataset)} messages sent to Kafka topic: {TWEETS_TOPIC}")

if __name__ == "__main__":
    produce_tweets('data/raw/tweet_hashtag.csv')
