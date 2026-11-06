from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema

def get_kafka_source(topic: str, server: str, consumer_group: str) -> KafkaSource:
    return KafkaSource.builder() \
        .set_bootstrap_servers(server) \
        .set_topics(topic) \
        .set_group_id(consumer_group) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

if __name__ == "__main__":
    print()