

# Delete a topic

```powershell
docker exec fyp-kafka /opt/kafka/bin/kafka-topics.sh --delete --topic tweets-storage --bootstrap-server localhost:9092

docker exec fyp-kafka /opt/kafka/bin/kafka-topics.sh --delete --topic tweets-raw --bootstrap-server localhost:9092

```

# Create a topic 
```powershell
docker exec -it fyp-kafka /opt/kafka/bin/kafka-topics.sh --create --topic tweets-raw --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

docker exec -it fyp-kafka /opt/kafka/bin/kafka-topics.sh --create --topic tweets-storage --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```