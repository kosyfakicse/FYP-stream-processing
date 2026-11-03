import json
import time
import uuid
from loguru import logger
from src.fyp_attribution.core.models import (
    InsertRunDTO, InsertVertexDTO, InsertEdgeDTO, 
    InsertInteractionDTO, InsertStateDTO, WindowOutputDTO
)
from src.fyp_attribution.storage.tin_store import TINStore

class ProvenanceService:
    # Service Layer responsible for handling provenance data operations and interactions with the TINStore.
    def __init__(self):
        self.store = TINStore()

    # Service methods for saving different types of provenance data to the database.
    def save_run(self, data: InsertRunDTO) -> str:
        logger.debug(f"Saving run: {data.run_id}")
        return self.store.insert_run(
            run_id=data.run_id,
            started_at=data.started_at,
            dataset=data.dataset,
            config_json=data.config_json,
            provenance_enabled=data.provenance_enabled
        )

    def save_vertex(self, data: InsertVertexDTO) -> str:
        return self.store.insert_vertex(
            vertex_id=data.vertex_id,
            run_id=data.run_id,
            vertex_type=data.vertex_type,
            description=data.description
        )

    def save_edge(self, data: InsertEdgeDTO) -> bool:
        return self.store.insert_edge(
            source_vertex=data.source_vertex,
            target_vertex=data.target_vertex,
            run_id=data.run_id
        )

    def save_interaction(self, data: InsertInteractionDTO) -> int:
        # Convert the attribution dictionary to a JSON string for storage
        attr_str = json.dumps(data.attribution_json)
        return self.store.insert_interaction(
            interaction_id=data.interaction_id,
            run_id=data.run_id,
            source_vertex=data.source_vertex,
            target_vertex=data.target_vertex,
            timestamp=data.timestamp,
            quantity=data.quantity,
            attribution_json=attr_str
        )
    
    def save_state(self, data: InsertStateDTO) -> int:
        # Convert the attribution dictionary to a JSON string for storage
        attr_str = json.dumps(data.attribution_json)
        return self.store.insert_state(
            state_id=data.state_id,
            run_id=data.run_id,
            vertex_id=data.vertex_id,
            time_start=data.time_start,
            time_end=data.time_end,
            quantity=data.quantity,
            attribution_json=attr_str,
            fired_at=data.fired_at
        )

    def save_window_output(self, data: WindowOutputDTO) -> int:
        # Generate a unique output_id using UUID and ensure it's within the BIGINT range
        output_id = uuid.uuid4().int & ((1 << 63) - 1)
        
        # Convert the attribution dictionary to a JSON string for storage
        attr_str = json.dumps(data.attribution_json)
        
        logger.info(f"Service: Generating output_id={output_id} for run={data.run_id}")
        
        return self.store.insert_window_output(
            output_id=output_id,
            run_id=data.run_id,
            vertex_id=data.vertex_id,
            window_start=data.window_start,
            window_end=data.window_end,
            output_count=data.output_count,
            attribution_json=attr_str
        )
    