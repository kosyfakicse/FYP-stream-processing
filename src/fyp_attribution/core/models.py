from pydantic import BaseModel
from typing import Any, Dict, Optional

class InsertRunDTO(BaseModel):
    # Data Transfer Object for inserting a run
    run_id: str
    started_at: str
    dataset: str
    config_json: Optional[str] = None
    provenance_enabled: bool
    finished_at: Optional[str] = None

class InsertVertexDTO(BaseModel):
    # Data Transfer Object for inserting a vertex
    vertex_id: str
    run_id: str
    vertex_type: str
    description: Optional[str] = None

class InsertEdgeDTO(BaseModel):
    # Data Transfer Object for inserting an edge
    source_vertex: str
    target_vertex: str
    run_id: str

class InsertInteractionDTO(BaseModel):
    # Data Transfer Object for inserting an interaction
    interaction_id: int
    run_id: str
    source_vertex: str
    target_vertex: str
    timestamp: int
    quantity: int
    attribution_json: Dict[str, int]

class InsertStateDTO(BaseModel):
    # Data Transfer Object for inserting a state
    state_id: int
    run_id: str
    vertex_id: str
    time_start: int
    time_end: Optional[int] = None
    quantity: int
    attribution_json: Dict[str, int]
    fired_at: Optional[int] = None

class WindowOutputDTO(BaseModel):
    # Data Transfer Object for Window Output
    run_id: str
    vertex_id: str
    window_start: int
    window_end: int
    output_count: int
    attribution_json: Dict[str, int]