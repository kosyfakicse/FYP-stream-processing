"""Configuration loader using Pydantic."""

from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field


class KafkaProducerConfig(BaseModel):
    """Kafka producer settings."""
    batch_size: int = 100
    linger_ms: int = 5
    replay_speed: float = 0.1


class FlinkConfig(BaseModel):
    """Flink job settings."""
    mode: int = 1  # 0: STREAMING, 1: BATCH, 2: AUTOMATIC
    parallelism: int = 1
    checkpoint_interval: int = 10000
    window_size_seconds: int = 60

class KafkaConfig(BaseModel):
    """Kafka connection settings."""
    bootstrap_servers: str = "localhost:9092"
    topic: str = "tweets_input"
    producer: KafkaProducerConfig = Field(default_factory=KafkaProducerConfig)


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
    dataset: DatasetConfig
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)
    flink: FlinkConfig = Field(default_factory=FlinkConfig)


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
    
    print(f"\nTarget dataset: {config.dataset.target_dataset}")
    print(f"File path: {config.dataset.get_dataset_config().file}")
    print(f"Kafka topic: {config.kafka.topic}")
    print(f"Flink mode: {config.flink.mode} (0=STREAMING, 1=BATCH, 2=AUTOMATIC)")
    print(f"Flink parallelism: {config.flink.parallelism}")
    print(f"Flink checkpoint interval: {config.flink.checkpoint_interval}ms")
    print(f"Run ID: {config.pipeline.run_id}")
    
    print("\n[OK] Config loaded successfully!")
