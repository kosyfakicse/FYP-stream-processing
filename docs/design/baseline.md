# Streaming baseline 

## Introduction
This is a application pipeline before capture 
Week 3-4 Deliverable



## pipeline structure
Event Input -> Kafka -> Flink -> Storage


## Implementation
### 1. Event Generator
-> It is a Producer to Kafka
-> All messages is from a generated tweets dataset (50 records) 
-> Either send all messages once for testing purpose 
-> or send message with delay for 

-> Unified the records into common event format (Not done yet)

### 2. Kafka on Docker
-> Two topics for baseline: tweets_storage and tweets_raw
-> tweets_storage is used for testing
-> tweets_raw is used for simulate the real streaming dataflow

### 3. Flink
-> It is a Consumer to Kafka
-> Parse to json, flter,extract hashtag from text, flat, key by hashtag
-> 5 second window with processing time
-> write to database


### 4. Storage 
-> DuckDB
