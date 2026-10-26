"""Shared test fixtures and configuration."""

import pytest
from pathlib import Path


@pytest.fixture
def sample_config_path():
    """Path to test configuration file."""
    return Path(__file__).parent.parent / "configs" / "config.yaml"


@pytest.fixture
def sample_tweet_data():
    """Sample tweet data for testing."""
    return [
        {
            "tweet_id": "t1",
            "user_id": "user_1",
            "text": "Loving #python!",
            "timestamp": "2024-01-01T00:00:00",
        },
        {
            "tweet_id": "t2",
            "user_id": "user_2",
            "text": "#python is awesome",
            "timestamp": "2024-01-01T00:00:30",
        },
        {
            "tweet_id": "t3",
            "user_id": "user_3",
            "text": "Check out #java",
            "timestamp": "2024-01-01T00:01:00",
        },
    ]


@pytest.fixture
def sample_attribution():
    """Sample attribution dictionary for testing."""
    return {"user_1": 2, "user_2": 1, "user_4": 1}
