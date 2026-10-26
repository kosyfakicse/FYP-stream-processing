.PHONY: help install install-dev test test-unit test-integration test-e2e lint format clean run-dashboard run-kafka run-flink

help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:  ## Install dependencies
	pip install -r requirements.txt
	pip install -e .

install-dev:  ## Install development dependencies
	pip install -r requirements.txt
	pip install -e ".[dev]"

test:  ## Run all tests
	pytest

test-unit:  ## Run unit tests only
	pytest tests/unit/

test-integration:  ## Run integration tests only
	pytest tests/integration/

test-e2e:  ## Run end-to-end tests only
	pytest tests/e2e/

test-cov:  ## Run tests with coverage report
	pytest --cov=src/fyp_attribution --cov-report=html

lint:  ## Run linters
	ruff check src/ tests/
	mypy src/

format:  ## Format code
	black src/ tests/
	ruff check --fix src/ tests/

clean:  ## Clean up build artifacts
	rm -rf build/ dist/ *.egg-info .pytest_cache htmlcov .coverage
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

run-dashboard:  ## Run Streamlit dashboard
	streamlit run src/fyp_attribution/dashboard/app.py

run-kafka:  ## Start Kafka (requires Kafka installation)
	@echo "Starting Kafka..."
	# Add your Kafka start command here

run-flink:  ## Start Flink cluster (requires Flink installation)
	@echo "Starting Flink cluster..."
	# Add your Flink start command here

setup-data:  ## Generate test data
	python scripts/data/generate_tweet_data.py

verify-schema:  ## Verify schema correctness
	python scripts/utils/verify_schema.py
