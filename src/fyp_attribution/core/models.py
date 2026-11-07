from pydantic import BaseModel
from typing import Dict, Optional

class RunModel(BaseModel):
    # Data model for the 'runs' table
    run_id: str
    started_at: str
    dataset: str
    config_json: Optional[str] = None
    provenance_enabled: bool
    finished_at: Optional[str] = None

class VertexModel(BaseModel):
    # Data model for the 'tin_vertices' table
    vertex_id: str
    run_id: str
    vertex_type: str
    description: Optional[str] = None

class EdgeModel(BaseModel):
    # Data model for the 'tin_edges' table
    source_vertex: str
    target_vertex: str
    run_id: str

class InteractionModel(BaseModel):
    # Data model for the 'tin_interactions' table
    interaction_id: int
    run_id: str
    source_vertex: str
    target_vertex: str
    timestamp: int
    quantity: int
    attribution_json: Dict[str, int]

class StateModel(BaseModel):
    # Data model for the 'tin_states' table
    state_id: int
    run_id: str
    vertex_id: str
    time_start: int
    time_end: Optional[int] = None
    quantity: int
    attribution_json: Dict[str, int]
    fired_at: Optional[int] = None

class WindowOutputModel(BaseModel):
    # Data model for the 'window_outputs' table
    output_id: Optional[int] = None
    run_id: str
    vertex_id: str
    category: str
    window_start: int
    window_end: int
    output_count: int
    attribution_json: Dict[str, int]