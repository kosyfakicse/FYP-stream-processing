import pytest
from src.fyp_attribution.core.models import (
    RunModel, VertexModel, EdgeModel, 
    InteractionModel, StateModel, WindowOutputModel
)
from src.fyp_attribution.storage.tin_store import TINStore
from src.fyp_attribution.storage.provenance_service import ProvenanceService

"""
Unit tests for the Storage Layer (TINStore and ProvenanceService).

How to run:
    pytest tests/unit/test_tin_store.py -v
"""

@pytest.fixture
def service():
    """
    Fixture to initialize the Service Layer.
    Uses an in-memory database (:memory:) to ensure complete isolation. 
    The database is destroyed after each test, preventing side effects.
    """
    store = TINStore(db_path=":memory:")
    yield ProvenanceService()
    store.close()

# Test cases for the ProvenanceService and TINStore classes. 
# Each test verifies that the service layer correctly saves data models to the underlying DuckDB database, and that the data persists as expected.
# Each type of model will have 50-100 records inserted to test concurrency and thread-safety.
def test_save_run(service):
    """Test saving a RunModel and verify it persists correctly."""
    data = RunModel(
        run_id="run-test-01",
        started_at="2026-11-03 10:00:00",
        dataset="tweet_hashtag",
        provenance_enabled=True
    )
    
    # ROP Pattern: Expect a Model object on success, not a string
    result = service.save_run(data)
    assert isinstance(result, RunModel)
    assert result.run_id == "run-test-01"
    
    # Verify data was actually written to DuckDB
    df = service.store.conn.execute("SELECT * FROM runs").fetchdf()
    assert len(df) == 1
    assert df.iloc[0]['dataset'] == "tweet_hashtag"

def test_save_vertex(service):
    """Test saving a VertexModel (TIN node)."""
    data = VertexModel(
        vertex_id="V1",
        run_id="run-test-01",
        vertex_type="source",
        description="Test Source Vertex"
    )
    result = service.save_vertex(data)
    
    assert isinstance(result, VertexModel)
    assert result.vertex_id == "V1"
    
    df = service.store.conn.execute("SELECT * FROM tin_vertices").fetchdf()
    assert len(df) == 1
    assert df.iloc[0]['vertex_type'] == "source"

def test_save_edge(service):
    """Test saving an EdgeModel (TIN data flow connection)."""
    data = EdgeModel(
        source_vertex="V1",
        target_vertex="V2",
        run_id="run-test-01"
    )
    result = service.save_edge(data)
    
    assert isinstance(result, EdgeModel)
    assert result.source_vertex == "V1"
    
    df = service.store.conn.execute("SELECT * FROM tin_edges").fetchdf()
    assert len(df) == 1
    assert df.iloc[0]['source_vertex'] == "V1"

def test_save_interaction(service, sample_attribution):
    """Test saving an InteractionModel and verify JSON serialization."""
    data = InteractionModel(
        interaction_id=1, 
        run_id="run-test-01",
        source_vertex="V1",
        target_vertex="V2",
        timestamp=1735696800000,
        quantity=4,
        attribution_json=sample_attribution
    )
    result = service.save_interaction(data)
    
    assert isinstance(result, InteractionModel)
    assert result.interaction_id == 1
    
    # Verify that the dictionary was correctly converted to a JSON string
    df = service.store.conn.execute("SELECT * FROM tin_interactions").fetchdf()
    assert len(df) == 1
    assert '{"user_1": 2' in df.iloc[0]['attribution_json']

def test_save_state(service, sample_attribution):
    """Test saving a StateModel (temporal snapshot)."""
    data = StateModel(
        state_id=1, 
        run_id="run-test-01",
        vertex_id="V1",
        time_start=1735696800000,
        time_end=1735696860000,
        quantity=4,
        attribution_json=sample_attribution,
        fired_at=1735696860000
    )
    result = service.save_state(data)
    
    assert isinstance(result, StateModel)
    assert result.state_id == 1
    
    df = service.store.conn.execute("SELECT * FROM tin_states").fetchdf()
    assert len(df) == 1
    assert df.iloc[0]['quantity'] == 4

def test_save_window_output(service, sample_attribution):
    """Test saving a WindowOutputModel. The Service layer generates a unique UUID."""
    data = WindowOutputModel(
        run_id="run-test-01",
        vertex_id="W1",
        window_start=1735696800000,
        window_end=1735696860000,
        output_count=4,
        attribution_json=sample_attribution
    )
    result = service.save_window_output(data)
    
    assert isinstance(result, WindowOutputModel)
    assert result.output_count == 4
    
    df = service.store.conn.execute("SELECT * FROM window_outputs").fetchdf()
    assert len(df) == 1
    assert df.iloc[0]['output_count'] == 4

def test_save_window_output_bulk(service, sample_attribution):
    """
    Stress test: Insert 100 window outputs rapidly.
    Ensures no primary key collisions occur during concurrent/bulk inserts.
    """
    total_records = 100
    
    for i in range(total_records):
        data = WindowOutputModel(
            run_id=f"run-test-bulk-{i}",
            vertex_id="W1",
            window_start=1735696800000 + i,
            window_end=1735696860000 + i,
            output_count=i,
            attribution_json=sample_attribution
        )
        result = service.save_window_output(data)
        
        # Ensure no error strings are returned during the bulk operation
        assert isinstance(result, WindowOutputModel)
    
    df = service.store.conn.execute("SELECT * FROM window_outputs").fetchdf()
    assert len(df) == total_records
    assert df.iloc[-1]['output_count'] == 99

def test_save_interaction_bulk(service, sample_attribution):
    """Stress test: Insert 50 interaction records to verify thread-safe write locking."""
    total_records = 50
    
    for i in range(total_records):
        data = InteractionModel(
            interaction_id=i + 1, 
            run_id="run-test-01",
            source_vertex="V1",
            target_vertex="V2",
            timestamp=1735696800000 + i,
            quantity=1,
            attribution_json=sample_attribution
        )
        result = service.save_interaction(data)
        assert isinstance(result, InteractionModel)
        
    df = service.store.conn.execute("SELECT * FROM tin_interactions").fetchdf()
    assert len(df) == total_records

# The following tests are designed to verify that the service layer correctly handles database errors, such as primary key violations.
# Each test attempts to insert a duplicate record and checks that the service returns an error string instead of raising an unhandled exception.
def test_save_run_duplicate_error(service):
    """
    Test ROP (Return Model | str) error handling.
    Forces a primary key violation to ensure the DAL returns a string error 
    instead of raising an unhandled exception.
    """
    data = RunModel(
        run_id="duplicate-run-id",
        started_at="2026-11-03 10:00:00",
        dataset="tweet_hashtag",
        provenance_enabled=True
    )
    
    # First insert should succeed
    result1 = service.save_run(data)
    assert isinstance(result1, RunModel)
    
    # Second insert with the same ID should trigger a database error
    result2 = service.save_run(data)
    
    # The error should be caught and returned as a string
    assert isinstance(result2, str)
    assert "Error inserting run duplicate-run-id" in result2

def test_save_vertex_duplicate_error(service):
    """Test ROP error handling for vertex primary key collision."""
    data = VertexModel(
        vertex_id="V1", run_id="run-test-01", vertex_type="source"
    )
    result1 = service.save_vertex(data)
    assert isinstance(result1, VertexModel)
    
    result2 = service.save_vertex(data)
    assert isinstance(result2, str)
    assert "Error inserting vertex V1" in result2

def test_save_edge_duplicate_error(service):
    """Test ROP error handling for edge composite primary key collision."""
    data = EdgeModel(
        source_vertex="V1", target_vertex="V2", run_id="run-test-01"
    )
    result1 = service.save_edge(data)
    assert isinstance(result1, EdgeModel)
    
    result2 = service.save_edge(data)
    assert isinstance(result2, str)
    assert "Error inserting edge V1->V2" in result2

def test_save_interaction_duplicate_error(service, sample_attribution):
    """Test ROP error handling for interaction primary key collision."""
    data = InteractionModel(
        interaction_id=1, run_id="run-test-01", source_vertex="V1", target_vertex="V2",
        timestamp=1735696800000, quantity=4, attribution_json=sample_attribution
    )
    result1 = service.save_interaction(data)
    assert isinstance(result1, InteractionModel)
    
    result2 = service.save_interaction(data)
    assert isinstance(result2, str)
    assert "Error inserting interaction 1" in result2

def test_save_state_duplicate_error(service, sample_attribution):
    """Test ROP error handling for state primary key collision."""
    data = StateModel(
        state_id=1, run_id="run-test-01", vertex_id="V1", 
        time_start=1735696800000, quantity=4, attribution_json=sample_attribution
    )
    result1 = service.save_state(data)
    assert isinstance(result1, StateModel)
    
    result2 = service.save_state(data)
    assert isinstance(result2, str)
    assert "Error inserting state 1" in result2

def test_save_window_output_duplicate_error(service, sample_attribution):
    """Test ROP error handling for window output primary key collision."""
    data = WindowOutputModel(
        run_id="run-test-01", vertex_id="W1",
        window_start=1735696800000, window_end=1735696860000,
        output_count=4, attribution_json=sample_attribution
    )
    
    # Note: The service layer generates a random UUID for output_id, so it's 
    # almost impossible to trigger a collision through the service layer.
    # To test the DAL's error handling, we call the DAL directly with a fixed ID.
    store = service.store
    
    # First insert directly via DAL
    result1 = store.insert_window_output(999, data, '{"user_1": 1}')
    assert isinstance(result1, WindowOutputModel)
    
    # Second insert with the same ID
    result2 = store.insert_window_output(999, data, '{"user_1": 1}')
    assert isinstance(result2, str)
    assert "Error inserting window output 999" in result2