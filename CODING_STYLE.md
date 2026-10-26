# Coding Style Guide
# Temporal Attribution in Streaming Dataflows

**Python Version:** 3.10+ (recommended: 3.10.x)  
**Typing:** Strict type hints with Pydantic models  
**Style:** PEP 8 compliant

---

## Overview

This project uses **Pydantic** for data validation and **strict typing** throughout. All data structures are defined as Pydantic models in `models.py`.

---

## 1. Type Hints

### Basic Rules

**Always use type hints for:**
- Function parameters
- Function return types
- Class attributes
- Variable annotations (when type is not obvious)

**Never use:**
- Bare `dict`, `list`, `tuple` without type parameters
- `Any` unless absolutely necessary (e.g., raw data from external sources)

### Examples

```python
# Good
def process_event(event: UnifiedEvent, operator_id: str) -> TINState:
    """Process a unified event through an operator."""
    ...

def merge_attributions(attrs: list[Attribution]) -> Attribution:
    """Merge multiple attribution dictionaries."""
    ...

# Bad
def process_event(event, operator_id):  # Missing type hints
    ...

def merge_attributions(attrs: list) -> dict:  # Too generic
    ...
```

### Generic Types

```python
from typing import Dict, List, Optional, Union

# Use built-in generics (Python 3.9+)
user_ids: list[str] = ["user_1", "user_2"]
attributions: dict[str, int] = {"user_1": 2, "user_2": 1}

# Or use typing module for older Python
from typing import List, Dict
user_ids: List[str] = ["user_1", "user_2"]
attributions: Dict[str, int] = {"user_1": 2, "user_2": 1}
```

---

## 2. Pydantic Models

### Model Definition

All data structures use Pydantic models with:
- **Field descriptions** for documentation
- **Validators** for data integrity
- **Type constraints** (ge, le, etc.)

```python
from pydantic import BaseModel, Field, field_validator

class UnifiedEvent(BaseModel):
    """Unified event schema for all datasets."""
    
    # Identification
    run_id: str = Field(..., description="Experiment run identifier")
    source_id: str = Field(..., description="Origin source ID")
    event_id: int = Field(..., ge=1, description="Unique event number")
    
    # Data
    quantity: int = Field(default=1, ge=0, description="Contributed quantity")
    category: Optional[str] = Field(None, description="Grouping key")
    timestamp: int = Field(..., description="Event time in milliseconds")
    
    # Provenance
    attribution: Attribution = Field(
        default_factory=lambda: Attribution(contributions={}),
        description="How-much provenance dictionary"
    )
    
    @field_validator('timestamp')
    @classmethod
    def validate_timestamp(cls, v: int) -> int:
        """Ensure timestamp is positive."""
        if v <= 0:
            raise ValueError(f"Timestamp must be positive, got {v}")
        return v
```

### Model Usage

```python
# Create model instance
event = UnifiedEvent(
    run_id="run-001",
    source_id="user_1",
    event_id=1,
    quantity=1,
    category="#python",
    timestamp=1735696800000,
    attribution=Attribution(contributions={"user_1": 1})
)

# Serialize to dict
event_dict = event.to_dict()
# or
event_dict = event.model_dump()

# Serialize to JSON
event_json = event.model_dump_json()

# Parse from dict
event = UnifiedEvent(**event_dict)

# Parse from JSON
event = UnifiedEvent.model_validate_json(event_json)
```

### Validation

```python
# Automatic validation on creation
try:
    event = UnifiedEvent(
        run_id="run-001",
        source_id="user_1",
        event_id=-1,  # Invalid: must be >= 1
        quantity=1,
        timestamp=1735696800000
    )
except ValidationError as e:
    print(f"Validation error: {e}")

# Manual validation
event.model_validate(event_dict)
```

---

## 3. Enums

Use enums for fixed sets of values:

```python
from enum import Enum

class VertexType(str, Enum):
    """Types of operators in the TIN dataflow graph."""
    SOURCE = "source"
    MAP = "map"
    FILTER = "filter"
    WINDOW = "window"
    SINK = "sink"

class DatasetName(str, Enum):
    """Available dataset configurations."""
    TWEET_HASHTAG = "tweet_hashtag"
    NYC_TAXI = "nyc_taxi"
    NEXMARK = "nexmark"
```

Usage:

```python
vertex = TINVertex(
    vertex_id="M1",
    run_id="run-001",
    vertex_type=VertexType.MAP,  # Use enum
    description="Identity map"
)

# Compare
if vertex.vertex_type == VertexType.MAP:
    print("This is a map operator")
```

---

## 4. Function Signatures

### Docstrings

Use Google-style docstrings:

```python
def backward_attribution(
    vertex_id: str,
    time_t: int,
    run_id: Optional[str] = None
) -> list[BackwardAttributionResult]:
    """
    Trace backward from a vertex to find source contributions.
    
    Args:
        vertex_id: Target operator to query
        time_t: Time point to query (milliseconds)
        run_id: Optional experiment run filter
    
    Returns:
        List of backward attribution results, each containing:
        - source: Source ID that contributed
        - quantity: Quantity contributed
        - time: Time of contribution
        - path: Path from source to target
    
    Raises:
        ValueError: If vertex_id not found in TIN
        RuntimeError: If provenance data is incomplete
    
    Example:
        >>> results = backward_attribution("W1", 1735696860000, "run-001")
        >>> for r in results:
        ...     print(f"{r.source}: {r.quantity} via {r.path}")
        user_1: 2 via user_1 -> M1 -> W1
        user_2: 1 via user_2 -> M1 -> W1
    """
    ...
```

### Type Aliases

For complex types, use type aliases:

```python
from typing import TypeAlias

AttributionDict: TypeAlias = dict[str, int]
EventStream: TypeAlias = list[UnifiedEvent]
TINGraph: TypeAlias = tuple[list[TINVertex], list[TINEdge], list[TINInteraction]]
```

---

## 5. Error Handling

### Custom Exceptions

```python
class AttributionError(Exception):
    """Base exception for attribution-related errors."""
    pass

class IncompleteProvenanceError(AttributionError):
    """Raised when provenance data is incomplete."""
    def __init__(self, vertex_id: str, time_t: int):
        self.vertex_id = vertex_id
        self.time_t = time_t
        super().__init__(
            f"Incomplete provenance for vertex {vertex_id} at time {time_t}"
        )

class InvalidAttributionError(AttributionError):
    """Raised when attribution violates invariants."""
    def __init__(self, expected: int, actual: int):
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"Attribution invariant violated: expected sum={expected}, got {actual}"
        )
```

### Usage

```python
def verify_attribution_invariant(state: TINState) -> None:
    """Verify that sum(attribution) == quantity."""
    import json
    attr_dict = json.loads(state.attribution_json)
    total = sum(attr_dict.values())
    
    if total != state.quantity:
        raise InvalidAttributionError(
            expected=state.quantity,
            actual=total
        )
```

---

## 6. Configuration

### Pydantic Settings

Use `pydantic-settings` for configuration management:

```python
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    """Application settings loaded from environment/config.yaml."""
    
    # Database
    duckdb_path: str = "data/attribution.duckdb"
    
    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "attribution-events"
    
    # Flink
    flink_parallelism: int = 1
    flink_window_size_seconds: int = 60
    
    # Dashboard
    streamlit_port: int = 8501
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

# Usage
settings = Settings()
print(settings.duckdb_path)  # "data/attribution.duckdb"
```

---

## 7. Testing

### Type Checking

Use `mypy` for static type checking:

```bash
# Install
pip install mypy

# Run
mypy models.py scripts/ tests/
```

### Pytest with Type Hints

```python
import pytest
from models import UnifiedEvent, Attribution

def test_create_event():
    """Test event creation with proper attribution."""
    event = UnifiedEvent(
        run_id="run-001",
        source_id="user_1",
        event_id=1,
        quantity=1,
        category="#python",
        timestamp=1735696800000,
        attribution=Attribution(contributions={"user_1": 1})
    )
    
    assert event.source_id == "user_1"
    assert event.attribution.contributions == {"user_1": 1}

def test_merge_attributions():
    """Test attribution merging."""
    attr1 = Attribution(contributions={"user_1": 2})
    attr2 = Attribution(contributions={"user_2": 1})
    merged = attr1.merge(attr2)
    
    assert merged.contributions == {"user_1": 2, "user_2": 1}
    assert merged.total_quantity() == 3
```

---

## 8. Import Organization

Follow this order:

```python
# 1. Standard library
import json
from typing import Optional
from datetime import datetime

# 2. Third-party
from pydantic import BaseModel, Field, field_validator
import duckdb

# 3. Local/application
from models import UnifiedEvent, Attribution, TINState
from config import AppConfig
```

---

## 9. Naming Conventions

| Type | Convention | Example |
|------|------------|---------|
| Classes | PascalCase | `UnifiedEvent`, `TINState` |
| Functions | snake_case | `backward_attribution()`, `merge_attributions()` |
| Variables | snake_case | `run_id`, `source_contributions` |
| Constants | UPPER_SNAKE_CASE | `MAX_WINDOW_SIZE`, `DEFAULT_PORT` |
| Enums | PascalCase (class), UPPER_SNAKE_CASE (values) | `VertexType.SOURCE` |
| Private | _prefix | `_internal_state`, `_validate()` |

---

## 10. Common Patterns

### Pattern 1: Model with Computed Fields

```python
from pydantic import BaseModel, computed_field

class TINState(BaseModel):
    vertex_id: str
    quantity: int
    attribution_json: str
    
    @computed_field
    @property
    def attribution_dict(self) -> dict[str, int]:
        """Serialize attribution JSON to dict."""
        import json
        return json.loads(self.attribution_json)
    
    @computed_field
    @property
    def total_attributed(self) -> int:
        """Sum of all attributed quantities."""
        return sum(self.attribution_dict.values())
```

### Pattern 2: Builder Function

```python
def create_tweet_event(
    run_id: str,
    user_id: str,
    event_id: int,
    text: str,
    hashtags: list[str],
    timestamp: int
) -> TweetEvent:
    """
    Create a tweet event with proper attribution initialization.
    
    This is a builder function that ensures correct initialization.
    """
    raw_data = TweetRawData(
        text=text,
        user_id=user_id,
        hashtags=hashtags
    )
    
    attribution = Attribution(contributions={user_id: 1})
    
    return TweetEvent(
        run_id=run_id,
        source_id=user_id,
        event_id=event_id,
        quantity=1,
        category=hashtags[0] if hashtags else None,
        timestamp=timestamp,
        attribution=attribution,
        raw=raw_data
    )
```

### Pattern 3: Model with Custom Serialization

```python
class Attribution(BaseModel):
    contributions: dict[str, int]
    
    def to_json(self) -> str:
        """Serialize to JSON string for database storage."""
        import json
        return json.dumps(self.contributions)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Attribution':
        """Deserialize from JSON string."""
        import json
        contributions = json.loads(json_str)
        return cls(contributions=contributions)
```

---

## 11. IDE Setup

### VS Code

Add to `.vscode/settings.json`:

```json
{
    "python.analysis.typeCheckingMode": "basic",
    "python.analysis.autoImportCompletions": true,
    "python.formatting.provider": "black",
    "python.linting.mypyEnabled": true,
    "python.linting.pylintEnabled": false
}
```

### PyCharm

1. Enable type checking: Settings -> Editor -> Inspections -> Python -> Type checker
2. Enable Pydantic plugin: Settings -> Plugins -> Search "Pydantic"
3. Set Python interpreter to project venv

---

## 12. Pre-commit Hooks

Add to `.pre-commit-config.yaml`:

```yaml
repos:
  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.8.0
    hooks:
      - id: mypy
        additional_dependencies: [pydantic>=2.5.0]
  
  - repo: https://github.com/psf/black
    rev: 24.1.0
    hooks:
      - id: black
  
  - repo: https://github.com/pycqa/isort
    rev: 5.13.0
    hooks:
      - id: isort
```

---

## Summary

**Always:**
- Use type hints for all function signatures
- Use Pydantic models for data structures
- Add field descriptions and validators
- Write Google-style docstrings
- Run `mypy` before committing

**Never:**
- Use bare `dict`, `list` without type parameters
- Skip type hints for public functions
- Use `Any` unless absolutely necessary
- Ignore validation errors

**Reference:**
- [Pydantic V2 Documentation](https://docs.pydantic.dev/latest/)
- [Python Type Hints Cheat Sheet](https://mypy.readthedocs.io/en/stable/cheat_sheet_py3.html)
- [PEP 8 Style Guide](https://peps.python.org/pep-0008/)
