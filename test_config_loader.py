"""Test script to verify config loader works."""

from fyp_attribution.core.config import load_config


def main():
    print("=" * 60)
    print("Testing Config Loader")
    print("=" * 60)
    
    try:
        # Load config
        print("\n1. Loading config from configs/config.yaml...")
        config = load_config("configs/config.yaml")
        print("   [OK] Config loaded successfully")
        
        # Test dataset config
        print("\n2. Dataset Configuration:")
        print(f"   Target dataset: {config.dataset.target_dataset}")
        
        dataset_config = config.dataset.get_dataset_config()
        print(f"   Description: {dataset_config.description}")
        print(f"   File path: {dataset_config.file}")
        
        # Test Kafka config
        print("\n3. Kafka Configuration:")
        print(f"   Bootstrap servers: {config.kafka.bootstrap_servers}")
        print(f"   Topic: {config.kafka.topic}")
        print(f"   Batch size: {config.kafka.producer.batch_size}")
        print(f"   Linger ms: {config.kafka.producer.linger_ms}")
        print(f"   Replay speed: {config.kafka.producer.replay_speed}s")
        
        # Test pipeline config
        print("\n4. Pipeline Configuration:")
        print(f"   Run ID: {config.pipeline.run_id}")
        print(f"   Window size: {config.pipeline.window_size_seconds}s")
        
        print("\n" + "=" * 60)
        print("[SUCCESS] All config loaded correctly!")
        print("=" * 60)
        
    except FileNotFoundError as e:
        print(f"\n[ERROR] File not found: {e}")
        print("Make sure configs/config.yaml exists")
        
    except ValueError as e:
        print(f"\n[ERROR] Invalid config value: {e}")
        
    except Exception as e:
        print(f"\n[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
