import pytest
from src.fyp_attribution.core.models import (
    InsertRunDTO, InsertVertexDTO, InsertEdgeDTO, 
    InsertInteractionDTO, InsertStateDTO, WindowOutputDTO
)
from src.fyp_attribution.storage.tin_store import TINStore
from src.fyp_attribution.storage.provenance_service import ProvenanceService

""" 
Run the Test: 
pytest tests/unit/test_tin_store.py -v 
"""

@pytest.fixture
def service():
    # CRITICAL: Use ":memory:" for unit tests. 
    store = TINStore(db_path=":memory:")
    yield ProvenanceService()
    store.close()

def test_save_run(service):
    data = InsertRunDTO(
        run_id="run-test-01",
        started_at="2026-11-03 10:00:00",
        dataset="tweet_hashtag",
        provenance_enabled=True
    )
    result = service.save_run(data)
    assert result == "run-test-01"
    
    df = service.store.conn.execute("SELECT * FROM runs").fetchdf()
    assert len(df) == 1
    assert df.iloc[0]['dataset'] == "tweet_hashtag"

def test_save_vertex(service):
    data = InsertVertexDTO(
        vertex_id="V1",
        run_id="run-test-01",
        vertex_type="source",
        description="Test Source Vertex"
    )
    result = service.save_vertex(data)
    assert result == "V1"
    
    df = service.store.conn.execute("SELECT * FROM tin_vertices").fetchdf()
    assert len(df) == 1
    assert df.iloc[0]['vertex_type'] == "source"

def test_save_edge(service):
    data = InsertEdgeDTO(
        source_vertex="V1",
        target_vertex="V2",
        run_id="run-test-01"
    )
    result = service.save_edge(data)
    assert result is True
    
    df = service.store.conn.execute("SELECT * FROM tin_edges").fetchdf()
    assert len(df) == 1
    assert df.iloc[0]['source_vertex'] == "V1"

def test_save_interaction(service, sample_attribution):
    # Added back the single interaction test for complete coverage
    data = InsertInteractionDTO(
        interaction_id=1, 
        run_id="run-test-01",
        source_vertex="V1",
        target_vertex="V2",
        timestamp=1735696800000,
        quantity=4,
        attribution_json=sample_attribution
    )
    result = service.save_interaction(data)
    assert result == 1
    
    df = service.store.conn.execute("SELECT * FROM tin_interactions").fetchdf()
    assert len(df) == 1
    assert '{"user_1": 2' in df.iloc[0]['attribution_json']

def test_save_state(service, sample_attribution):
    data = InsertStateDTO(
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
    assert result == 1
    
    df = service.store.conn.execute("SELECT * FROM tin_states").fetchdf()
    assert len(df) == 1
    assert df.iloc[0]['quantity'] == 4

def test_save_window_output(service, sample_attribution):
    data = WindowOutputDTO(
        run_id="run-test-01",
        vertex_id="W1",
        window_start=1735696800000,
        window_end=1735696860000,
        output_count=4,
        attribution_json=sample_attribution
    )
    inserted_id = service.save_window_output(data)
    
    # Removed the ID limit assertion as requested. Just check if it returns an int.
    assert isinstance(inserted_id, int)
    
    df = service.store.conn.execute("SELECT * FROM window_outputs").fetchdf()
    assert len(df) == 1
    assert df.iloc[0]['output_count'] == 4

def test_save_window_output_bulk(service, sample_attribution):
    """Test inserting 100 window outputs."""
    total_records = 100
    
    for i in range(total_records):
        data = WindowOutputDTO(
            run_id=f"run-test-bulk-{i}",
            vertex_id="W1",
            window_start=1735696800000 + i,
            window_end=1735696860000 + i,
            output_count=i,
            attribution_json=sample_attribution
        )
        service.save_window_output(data)
    
    df = service.store.conn.execute("SELECT * FROM window_outputs").fetchdf()
    assert len(df) == total_records
    assert df.iloc[-1]['output_count'] == 99

def test_save_interaction_bulk(service, sample_attribution):
    """Test inserting 50 interactions."""
    total_records = 50
    
    for i in range(total_records):
        data = InsertInteractionDTO(
            interaction_id=i + 1, 
            run_id="run-test-01",
            source_vertex="V1",
            target_vertex="V2",
            timestamp=1735696800000 + i,
            quantity=1,
            attribution_json=sample_attribution
        )
        service.save_interaction(data)
        
    df = service.store.conn.execute("SELECT * FROM tin_interactions").fetchdf()
    assert len(df) == total_records