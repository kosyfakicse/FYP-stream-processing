# scripts/kafka/replay_tweets.py
"""Replay tweets from storage topic to processing topic at controlled rate."""

import json
import time
from confluent_kafka import Producer, Consumer, KafkaError

# Kafka settings
KAFKA_SERVER = 'localhost:9092'
STORAGE_TOPIC = 'tweets-storage'  # Source: where bulk data is stored
REPLAY_TOPIC = 'tweets-raw'       # Target: where PyFlink reads from

# Replay speed control (adjust this to change speed)
MESSAGES_PER_SECOND = 10
DELAY_BETWEEN_MESSAGES = 1.0 / MESSAGES_PER_SECOND


def replay_tweets():
    """Read from storage topic and replay to processing topic with delays."""
    print("=" * 60)
    print("Kafka Replay Script")
    print("=" * 60)
    print(f"Source topic: {STORAGE_TOPIC}")
    print(f"Target topic: {REPLAY_TOPIC}")
    print(f"Replay speed: {MESSAGES_PER_SECOND} msgs/sec")
    print(f"Delay: {DELAY_BETWEEN_MESSAGES:.3f}s between messages")
    print()
    
    # Create consumer for storage topic
    consumer = Consumer({
        'bootstrap.servers': KAFKA_SERVER,
        'group.id': 'replay-consumer-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    })
    consumer.subscribe([STORAGE_TOPIC])
    
    # Create producer for replay topic
    producer = Producer({'bootstrap.servers': KAFKA_SERVER})
    
    print("Starting replay... (Press Ctrl+C to stop)")
    print()
    
    start_time = time.time()
    message_count = 0
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                # No more messages
                print(f"\nNo more messages in {STORAGE_TOPIC}")
                break
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition
                    print(f"\nReached end of {STORAGE_TOPIC}")
                    break
                else:
                    print(f"Error: {msg.error()}")
                    continue
            
            # Parse message
            try:
                value = json.loads(msg.value().decode('utf-8'))
                key = msg.key().decode('utf-8') if msg.key() else None
                
                # Send to replay topic
                producer.produce(
                    REPLAY_TOPIC,
                    key=key.encode('utf-8') if key else None,
                    value=json.dumps(value).encode('utf-8')
                )
                producer.poll(0)
                
                message_count += 1
                
                # Progress update every 50 messages
                if message_count % 50 == 0:
                    elapsed = time.time() - start_time
                    rate = message_count / elapsed if elapsed > 0 else 0
                    print(f"[{message_count}] Replayed | Rate: {rate:.1f} msg/sec | Elapsed: {elapsed:.1f}s")
                
                # Control replay speed
                time.sleep(DELAY_BETWEEN_MESSAGES)
                
            except Exception as e:
                print(f"Error processing message: {e}")
                continue
    
    except KeyboardInterrupt:
        print("\n\nReplay stopped by user")
    
    finally:
        # Cleanup
        producer.flush()
        consumer.close()
        
        elapsed = time.time() - start_time
        print()
        print("=" * 60)
        print(f"Replay complete!")
        print(f"  Total messages replayed: {message_count}")
        print(f"  Total time: {elapsed:.1f} seconds")
        if elapsed > 0:
            print(f"  Average rate: {message_count/elapsed:.1f} msg/sec")
        print("=" * 60)


if __name__ == "__main__":
    replay_tweets()
