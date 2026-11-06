from pathlib import Path
from dotenv import load_dotenv
import os
from typing import Optional
import yaml
from pydantic import BaseModel, Field

load_dotenv()

class KafkaProducerConfig(BaseModel):
    """Kafka producer settings."""
    batch_size: int = 100
    linger_ms: int = 5
    replay_speed: float = 0.1

class KafkaConfig(BaseModel):
    """Kafka connection settings."""
    bootstrap_servers: str = "localhost:9092"  # Overridden by config.yaml
    topic: str = "tweets-raw"  # Overridden by config.yaml
    consumer_group: str = os.getenv('KAFKA_CONSUMER_GROUP', 'flink-group')  # From .env
    producer: KafkaProducerConfig = Field(default_factory=KafkaProducerConfig)

    # Kafka connector JAR paths (from .env)
    connector_jar: Path = Path(os.getenv('KAFKA_CONNECTOR_JAR', 'lib/flink-connector-kafka-3.4.0-1.20.jar'))
    kafka_client_jar: Path = Path(os.getenv('KAFKA_CLIENT_JAR', 'lib/kafka-clients-3.7.0.jar'))
    connector_base_jar: Path = Path(os.getenv('KAFKA_CONNECTOR_BASE_JAR', 'lib/flink-connector-base-1.20.0.jar'))
    
    # Table API SQL connector (fat JAR with all dependencies)
    sql_connector_jar: Path = Path(os.getenv('KAFKA_SQL_CONNECTOR_JAR', 'lib/flink-sql-connector-kafka-3.1.0-1.18.jar'))

    def get_jar_uris(self, project_root: Path) -> list[str]:
        jars = []
        for jar_path in [self.connector_jar, self.kafka_client_jar, self.connector_base_jar]:
            jar = jar_path if jar_path.is_absolute() else project_root / jar_path
            jars.append(f"file:///{str(jar).replace(os.sep, '/')}")
        return jars

class FlinkConfig(BaseModel):
    """Flink job settings."""
    mode: int = 1  # 0: STREAMING, 1: BATCH, 2: AUTOMATIC
    parallelism: int = 1
    checkpoint_interval: int = 10000
    window_size_seconds: int = 60
    max_records: int = 10000  # Maximum records to read from socket




class DatasetEntryConfig(BaseModel):
    """Configuration for a single dataset."""
    description: Optional[str] = None
    file: Optional[str] = None


class DatasetConfig(BaseModel):
    """Dataset configuration."""
    target_dataset: str
    tweet_hashtag: Optional[DatasetEntryConfig] = Field(default_factory=DatasetEntryConfig)
    nyc_taxi: Optional[DatasetEntryConfig] = Field(default_factory=DatasetEntryConfig)
    nexmark: Optional[DatasetEntryConfig] = Field(default_factory=DatasetEntryConfig)
    
    def get_dataset_config(self) -> DatasetEntryConfig:
        """Get config for the target dataset."""
        dataset = getattr(self, self.target_dataset.replace("-", "_"))
        if dataset is None:
            raise ValueError(f"Dataset config not found: {self.target_dataset}")
        if dataset.file is None:
            raise ValueError(f"Dataset file path not set: {self.target_dataset}")
        return dataset

    


class PipelineConfig(BaseModel):
    """Pipeline settings."""
    run_id: str = "run-001"
    window_size_seconds: int = 60



class AppConfig(BaseModel):
    """Main application configuration."""
    project_root: Path = Path(os.getenv('PROJECT_ROOT', Path(__file__).parent.parent.parent.parent))
    dataset: DatasetConfig
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)
    flink: FlinkConfig = Field(default_factory=FlinkConfig)

    class Config:
        arbitrary_types_allowed = True


def load_config(config_path: str = "configs/config.yaml") -> AppConfig:
    """Load configuration from YAML file.
    
    Args:
        config_path: Path to config.yaml
        
    Returns:
        AppConfig: Parsed configuration
        
    Example:
        >>> config = load_config("configs/config.yaml")
        >>> print(config.dataset.target_dataset)
        'tweet_hashtag'
        >>> print(config.kafka.topic)
        'tweets_input'
    """
    path = Path(config_path)
    
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    
    with open(path, "r", encoding="utf-8") as f:
        raw_config = yaml.safe_load(f)
    
    return AppConfig(**raw_config)


if __name__ == "__main__":
    print("=" * 60)
    print("Testing Config Loader")
    print("=" * 60)
    
    config = load_config("configs/config.yaml")
    
    print(f"\nProject root: {config.project_root}")
    print(f"Target dataset: {config.dataset.target_dataset}")
    print(f"File path: {config.dataset.get_dataset_config().file}")
    print(f"Kafka bootstrap: {config.kafka.bootstrap_servers}")
    print(f"Kafka topic: {config.kafka.topic}")
    print(f"Kafka consumer group: {config.kafka.consumer_group}")
    print(f"Kafka connector JAR: {config.kafka.connector_jar}")
    print(f"Kafka JAR URI: {config.kafka.get_jar_uri(config.project_root)}")
    print(f"Flink mode: {config.flink.mode} (0=STREAMING, 1=BATCH, 2=AUTOMATIC)")
    print(f"Flink parallelism: {config.flink.parallelism}")
    print(f"Flink checkpoint interval: {config.flink.checkpoint_interval}ms")
    print(f"Flink window size: {config.flink.window_size_seconds}s")
    print(f"Pipeline run ID: {config.pipeline.run_id}")
    print(f"Pipeline window size: {config.pipeline.window_size_seconds}s")
    
    print("\n[OK] Config loaded successfully!")
