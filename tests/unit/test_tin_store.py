import pytest
from src.fyp_attribution.core.models import (
    RunModel, VertexModel, EdgeModel, 
    InteractionModel, StateModel, WindowOutputModel
)
from src.fyp_attribution.storage.tin_store import TINStore
from src.fyp_attribution.storage.provenance_service import ProvenanceService
from src.fyp_attribution.storage.duckdb_adapter import DuckDB
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
    """
    db = DuckDB(db_path=":memory:")
    service = ProvenanceService(db=db)
    yield service
    service.store.close()

def test_save_run(service):
    """Test saving a RunModel."""
    data = RunModel(
        run_id="run-test-01", started_at="2026-11-03 10:00:00", 
        dataset="tweet_hashtag", provenance_enabled=True
    )
    result = service.save_run(data)
    assert isinstance(result, RunModel)
    
    df = service.store.db.conn.execute("SELECT * FROM runs").fetchdf()
    assert len(df) == 1
    assert df.iloc[0]['dataset'] == "tweet_hashtag"

def test_save_vertex(service):
    """Test saving a VertexModel."""
    data = VertexModel(vertex_id="V1", run_id="run-test-01", vertex_type="source")
    result = service.save_vertex(data)
    assert isinstance(result, VertexModel)
    
    df = service.store.db.conn.execute("SELECT * FROM tin_vertices").fetchdf()
    assert len(df) == 1
    assert df.iloc[0]['vertex_type'] == "source"

def test_save_edge(service):
    """Test saving an EdgeModel."""
    data = EdgeModel(source_vertex="V1", target_vertex="V2", run_id="run-test-01")
    result = service.save_edge(data)
    assert isinstance(result, EdgeModel)
    
    df = service.store.db.conn.execute("SELECT * FROM tin_edges").fetchdf()
    assert len(df) == 1
    assert df.iloc[0]['source_vertex'] == "V1"

def test_save_interaction(service, sample_attribution):
    """Test saving an InteractionModel."""
    data = InteractionModel(
        interaction_id=1, run_id="run-test-01", source_vertex="V1", 
        target_vertex="V2", timestamp=1735696800000, quantity=4, 
        attribution_json=sample_attribution
    )
    result = service.save_interaction(data)
    assert isinstance(result, InteractionModel)
    
    df = service.store.db.conn.execute("SELECT * FROM tin_interactions").fetchdf()
    assert len(df) == 1
    assert '{"user_1": 2' in df.iloc[0]['attribution_json']

def test_save_state(service, sample_attribution):
    """Test saving a StateModel."""
    data = StateModel(
        state_id=1, run_id="run-test-01", vertex_id="V1", time_start=1735696800000, 
        time_end=1735696860000, quantity=4, attribution_json=sample_attribution
    )
    result = service.save_state(data)
    assert isinstance(result, StateModel)
    
    df = service.store.db.conn.execute("SELECT * FROM tin_states").fetchdf()
    assert len(df) == 1
    assert df.iloc[0]['quantity'] == 4

def test_save_window_output(service, sample_attribution):
    """Test saving a WindowOutputModel and ensure ID is generated."""
    data = WindowOutputModel(
        run_id="run-test-01", vertex_id="W1", category="#python", window_start=1735696800000, 
        window_end=1735696860000, output_count=4, attribution_json=sample_attribution
    )
    result = service.save_window_output(data)
    
    # Verify object is returned and UUID is generated
    assert isinstance(result, WindowOutputModel)
    assert result.output_id is not None 
    
    df = service.store.db.conn.execute("SELECT * FROM window_outputs").fetchdf()
    assert len(df) == 1
    assert df.iloc[0]['output_count'] == 4

def test_bulk_save_window_outputs(service, sample_attribution):
    """Stress test: Bulk insert 100 window outputs."""
    total_records = 100
    models = []
    
    for i in range(total_records):
        models.append(WindowOutputModel(
            run_id=f"run-bulk-{i}", vertex_id="W1", category="#python",
            window_start=1735696800000 + i, window_end=1735696860000 + i,
            output_count=i, attribution_json=sample_attribution
        ))
        
    # Call the bulk insert method
    results = service.bulk_save_window_outputs(models)
    
    # Verify list of models is returned with IDs
    assert len(results) == total_records
    assert all(isinstance(m.output_id, int) for m in results)
    
    # Verify DB count
    df = service.store.db.conn.execute("SELECT * FROM window_outputs").fetchdf()
    assert len(df) == total_records

# --- Exception Handling Tests ---

def test_save_run_duplicate_raises_exception(service):
    """Test that duplicate insert raises RuntimeError."""
    data = RunModel(run_id="dup-id", started_at="2026-11-07 10:00:00", dataset="test", provenance_enabled=True)
    service.save_run(data)
    
    with pytest.raises(RuntimeError) as exc_info:
        service.save_run(data)
    # 改成检查 Database error
    assert "Database error" in str(exc_info.value)
    assert "Duplicate key" in str(exc_info.value)

def test_save_vertex_duplicate_raises_exception(service):
    data = VertexModel(vertex_id="V1", run_id="run", vertex_type="source")
    service.save_vertex(data)
    with pytest.raises(RuntimeError) as exc_info:
        service.save_vertex(data)
    assert "Database error" in str(exc_info.value)
    assert "Duplicate key" in str(exc_info.value)

def test_save_edge_duplicate_raises_exception(service):
    data = EdgeModel(source_vertex="V1", target_vertex="V2", run_id="run")
    service.save_edge(data)
    with pytest.raises(RuntimeError) as exc_info:
        service.save_edge(data)
    assert "Database error" in str(exc_info.value)
    assert "Duplicate key" in str(exc_info.value)

def test_save_interaction_duplicate_raises_exception(service, sample_attribution):
    data = InteractionModel(interaction_id=1, run_id="run", source_vertex="V1", target_vertex="V2", timestamp=1, quantity=1, attribution_json=sample_attribution)
    service.save_interaction(data)
    with pytest.raises(RuntimeError) as exc_info:
        service.save_interaction(data)
    assert "Database error" in str(exc_info.value)
    assert "Duplicate key" in str(exc_info.value)

def test_save_state_duplicate_raises_exception(service, sample_attribution):
    data = StateModel(state_id=1, run_id="run", vertex_id="V1", time_start=1, quantity=1, attribution_json=sample_attribution)
    service.save_state(data)
    with pytest.raises(RuntimeError) as exc_info:
        service.save_state(data)
    assert "Database error" in str(exc_info.value)
    assert "Duplicate key" in str(exc_info.value)

def test_save_window_output_duplicate_raises_exception(service, sample_attribution):
    """Test DAL exception for window output collision (since Service generates UUIDs)."""
    data = WindowOutputModel(
        run_id="run", vertex_id="W1", category="#python", window_start=1, window_end=2, 
        output_count=4, attribution_json=sample_attribution
    )
    store = service.store
    
    store.insert_window_output(999, data, '{"user_1": 1}')
    
    with pytest.raises(RuntimeError) as exc_info:
        store.insert_window_output(999, data, '{"user_1": 1}')
    assert "Database error" in str(exc_info.value)
    assert "Duplicate key" in str(exc_info.value)

def test_close_connection():
    """Test that the connection closes properly and can be reopened."""
    db = DuckDB(db_path=":memory:")
    store = TINStore(db=db)
    assert db.conn is not None
    
    # Close the connection
    store.close()
    assert db.conn is None
    
    # Call close again to hit the edge case (should not crash)
    store.close()