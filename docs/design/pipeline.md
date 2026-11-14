Dataset -> Adapter -> Kafka -> Flink -> DBWriter -> db file

1. Tweets dataset
-> Tweets generated with parameter
-> Save at path data/raw/tweet_hashtag.csv

2. Load to Kafka
-> Unified the records into common event format (Not done yet)
-> Load the dataset into Kafka as Producer
-> for a dataset we have two topic, one is {dataset}-storage, another one is {dataset}-raw
-> storage topic is for testing purpose that send all record into Kafka with no delay
-> raw topic is for simulation that send all record into kafka with delay


3. Flink
-> Program path: src/fyp_attribution/main
-> Flink receive message from kafka as Consumer
-> Process: 
-> Write to Sink 
(We are using "print" as sink now, Flink does not support DuckDB as Sink/Connector, Custom Sink class also no working at PyFlink because it requires Java implementation)