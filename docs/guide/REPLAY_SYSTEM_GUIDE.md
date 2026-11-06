# Two-Topic Kafka Replay System - Usage Guide

## Overview

This system allows you to:
- Load your dataset into Kafka **once** (bulk storage)
- **Replay** it multiple times at controlled speeds
- Simulate **real-time streaming** for PyFlink processing

## Architecture

```
CSV Dataset (1000 records)
    ↓
bulk_load_tweets.py (instant)
    ↓
tweets-storage (Kafka Topic 1 - permanent storage)
    ↓
replay_tweets.py (10 msgs/sec, adjustable)
    ↓
tweets-raw (Kafka Topic 2 - real-time feed)
    ↓
PyFlink Pipeline (processing)
```

## Quick Start

### Step 1: Bulk Load (Do This ONCE)

```powershell
python scripts/kafka/bulk_load_tweets.py
```

**What it does:**
- Reads `data/raw/tweet_hashtag.csv`
- Loads all 1000 records into `tweets-storage` topic
- Takes <1 second (instant, no delays)
- Data persists in Kafka for replay

**Expected output:**
```
============================================================
Kafka Bulk Loader
============================================================
Dataset: 1000 records
Storage topic: tweets-storage
Mode: INSTANT (no delays)

Loaded 100/1000 messages...
Loaded 200/1000 messages...
...
Loaded 1000/1000 messages...

============================================================
Bulk load complete!
  Total messages: 1000
  Stored in topic: tweets-storage
============================================================
```

### Step 2: Clear Processing Topic (Before Each Replay)

```powershell
# Delete old data
docker exec fyp-kafka /opt/kafka/bin/kafka-topics.sh --delete --topic tweets-raw --bootstrap-server localhost:9092

# Recreate empty topic
docker exec fyp-kafka /opt/kafka/bin/kafka-topics.sh --create --topic tweets-raw --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

**Why:** Ensures PyFlink starts fresh with only new replay data

### Step 3: Run Replay Script

```powershell
python scripts/kafka/replay_tweets.py
```

**What it does:**
- Reads from `tweets-storage`
- Sends to `tweets-raw` at 10 messages/second
- Takes ~100 seconds for 1000 records
- Simulates real-time streaming

**Expected output:**
```
============================================================
Kafka Replay Script
============================================================
Source topic: tweets-storage
Target topic: tweets-raw
Replay speed: 10 msgs/sec
Delay: 0.100s between messages

Starting replay... (Press Ctrl+C to stop)

[50] Replayed | Rate: 10.0 msg/sec | Elapsed: 5.0s
[100] Replayed | Rate: 10.0 msg/sec | Elapsed: 10.0s
[150] Replayed | Rate: 10.0 msg/sec | Elapsed: 15.0s
...

============================================================
Replay complete!
  Total messages replayed: 1000
  Total time: 100.0 seconds
  Average rate: 10.0 msg/sec
============================================================
```

### Step 4: Run PyFlink Pipeline (In Another Terminal)

```powershell
python -m src.fyp_attribution.main
```

**What it does:**
- Reads from `tweets-raw` topic
- Processes data as it arrives (real-time streaming)
- Windows trigger every 5 seconds with ~50 records each
- Shows hashtag aggregation per user

**Expected behavior:**
- Debug logs show records arriving gradually
- Windows trigger with small batches (not all 1000 at once)
- Processing continues for ~100 seconds (duration of replay)

## Adjust Replay Speed

Edit `scripts/kafka/replay_tweets.py` line 14:

```python
MESSAGES_PER_SECOND = 10  # Change this value
```

**Examples:**
- `5` = Slower (200 seconds for 1000 records)
- `10` = Default (100 seconds)
- `20` = Faster (50 seconds)
- `50` = Very fast (20 seconds)

## Replay Multiple Times

The beauty of this system: **you can replay the same data as many times as you want!**

```powershell
# First replay
python scripts/kafka/replay_tweets.py
python -m src.fyp_attribution.main

# Want to test again? Just clear tweets-raw and replay!
docker exec fyp-kafka /opt/kafka/bin/kafka-topics.sh --delete --topic tweets-raw --bootstrap-server localhost:9092
docker exec fyp-kafka /opt/kafka/bin/kafka-topics.sh --create --topic tweets-raw --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

python scripts/kafka/replay_tweets.py
python -m src.fyp_attribution.main

# Replay as many times as you need!
```

The original data stays safely in `tweets-storage` topic.

## Topic Management

### List All Topics
```powershell
docker exec fyp-kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Check Topic Details
```powershell
# Storage topic
docker exec fyp-kafka /opt/kafka/bin/kafka-topics.sh --describe --topic tweets-storage --bootstrap-server localhost:9092

# Processing topic
docker exec fyp-kafka /opt/kafka/bin/kafka-topics.sh --describe --topic tweets-raw --bootstrap-server localhost:9092
```

### Delete Both Topics (Complete Reset)
```powershell
docker exec fyp-kafka /opt/kafka/bin/kafka-topics.sh --delete --topic tweets-storage --bootstrap-server localhost:9092
docker exec fyp-kafka /opt/kafka/bin/kafka-topics.sh --delete --topic tweets-raw --bootstrap-server localhost:9092

# Recreate storage topic
docker exec fyp-kafka /opt/kafka/bin/kafka-topics.sh --create --topic tweets-storage --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Recreate processing topic
docker exec fyp-kafka /opt/kafka/bin/kafka-topics.sh --create --topic tweets-raw --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Then run bulk load again
python scripts/kafka/bulk_load_tweets.py
```

## Comparison: Old vs New

### Old Way (Single Topic, Instant Load)
```
CSV → producer → tweets-raw → PyFlink
     (instant)   (all 1000 at once)
```
- All data arrives simultaneously
- Windows trigger with 300-400 records
- Not real-time simulation
- Can't replay (data already consumed)

### New Way (Two Topics, Controlled Replay)
```
CSV → bulk_load → tweets-storage → replay → tweets-raw → PyFlink
     (once)      (permanent)     (10/sec)  (streaming)
```
- Data arrives gradually (10/sec)
- Windows trigger with ~50 records
- True real-time simulation
- Can replay multiple times

## Troubleshooting

### "No more messages in tweets-storage"
- You need to run bulk load first: `python scripts/kafka/bulk_load_tweets.py`

### Replay finishes too quickly
- Check `MESSAGES_PER_SECOND` in replay script
- Lower value = slower replay

### PyFlink not seeing any data
- Make sure `tweets-raw` topic is cleared before replay
- Run replay script BEFORE starting PyFlink
- Check that both scripts use same Kafka server (localhost:9092)

### Want to change dataset
- Update `data/raw/tweet_hashtag.csv`
- Delete and recreate `tweets-storage` topic
- Run bulk load again

## Summary

**Two-Topic Replay System** gives you:
- Persistent data storage
- Controlled streaming simulation
- Repeatable testing
- Real-time processing behavior

Perfect for developing and testing your PyFlink pipeline!
