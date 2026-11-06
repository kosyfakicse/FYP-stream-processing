# Kafka Data Management Guide

## Delete/Clear Kafka Topic Data

### Method 1: Delete and Recreate Topic (Recommended for Testing)

```powershell
# Find your Kafka container name
docker ps | findstr kafka

# Delete the topic
docker exec -it <kafka-container-name> kafka-topics.sh --delete --topic tweets-raw --bootstrap-server localhost:9092

# Recreate it
docker exec -it <kafka-container-name> kafka-topics.sh --create --topic tweets-raw --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### Method 2: Using Docker Compose (Complete Reset)

```powershell
# Stop Kafka
docker-compose stop kafka

# Remove Kafka container and volumes
docker-compose rm -v -f kafka

# Start Kafka again
docker-compose up -d kafka

# Wait for Kafka to be ready
timeout /t 10
```

### Method 3: Delete Records Only (Keep Topic)

```powershell
# Create delete-records.json
echo {"partitions":[{"topic":"tweets-raw","partition":0,"offset":-1}],"version":1} > delete-records.json

# Copy to container
docker cp delete-records.json <kafka-container-name>:/tmp/

# Delete records
docker exec -it <kafka-container-name> kafka-delete-records.sh --bootstrap-server localhost:9092 --offset-json-file /tmp/delete-records.json
```

---

## Insert New Dataset

### Option 1: Produce Sample Data (Built-in)

```powershell
# Run the producer script
.venv\Scripts\python.exe produce_tweets.py
```

This will produce 8 sample tweets to the `tweets-raw` topic.

### Option 2: Load from CSV File

```powershell
# Default: loads from data/tweets.csv
.venv\Scripts\python.exe produce_from_csv.py

# Custom CSV file
.venv\Scripts\python.exe produce_from_csv.py path/to/your/file.csv
```

**Expected CSV format:**
```csv
user_id,text,timestamp
user_1,Learning #Python today,1793844000000
user_2,#Flink is awesome,1793844001000
```

### Option 3: Use Kafka Console Producer

```powershell
# Interactive mode
docker exec -it <kafka-container-name> kafka-console-producer.sh --topic tweets-raw --bootstrap-server localhost:9092

# Then type JSON messages:
{"user_id":"user_1","text":"Hello #World","timestamp":1793844000000}
{"user_id":"user_2","text":"Testing #Kafka","timestamp":1793844001000}
# Press Ctrl+C to exit
```

---

## Verify Data in Kafka

### Check Topic Info

```powershell
docker exec -it <kafka-container-name> kafka-topics.sh --describe --topic tweets-raw --bootstrap-server localhost:9092
```

### Consume Messages (View Data)

```powershell
# From beginning
docker exec -it <kafka-container-name> kafka-console-consumer.sh --topic tweets-raw --from-beginning --bootstrap-server localhost:9092

# Latest messages only
docker exec -it <kafka-container-name> kafka-console-consumer.sh --topic tweets-raw --bootstrap-server localhost:9092

# Limit to 10 messages
docker exec -it <kafka-container-name> kafka-console-consumer.sh --topic tweets-raw --max-messages 10 --bootstrap-server localhost:9092
```

---

## Complete Workflow Example

```powershell
# 1. Stop your Flink pipeline (Ctrl+C if running)

# 2. Clear Kafka topic
docker exec -it <kafka-container-name> kafka-topics.sh --delete --topic tweets-raw --bootstrap-server localhost:9092
docker exec -it <kafka-container-name> kafka-topics.sh --create --topic tweets-raw --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# 3. Produce new data
.venv\Scripts\python.exe produce_from_csv.py data/new_tweets.csv

# 4. Verify data
docker exec -it <kafka-container-name> kafka-console-consumer.sh --topic tweets-raw --max-messages 5 --bootstrap-server localhost:9092

# 5. Run Flink pipeline
.venv\Scripts\python.exe -m src.fyp_attribution.main
```

---

## Troubleshooting

### Topic Not Found
```powershell
# List all topics
docker exec -it <kafka-container-name> kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Connection Refused
```powershell
# Check if Kafka is running
docker ps | findstr kafka

# Restart Kafka if needed
docker-compose restart kafka
```

### Permission Denied
```powershell
# Run Docker commands with appropriate permissions
# On Windows, run PowerShell as Administrator if needed
```
