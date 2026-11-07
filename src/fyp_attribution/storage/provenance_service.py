import json
import uuid
from loguru import logger
from src.fyp_attribution.pipeline.db_writer import IDatabase
from src.fyp_attribution.core.models import (
    RunModel, VertexModel, EdgeModel, 
    InteractionModel, StateModel, WindowOutputModel
)
from src.fyp_attribution.storage.duckdb_adapter import DuckDB
from src.fyp_attribution.storage.tin_store import TINStore

class ProvenanceService:
    """
    Service Layer responsible for handling provenance data operations.
    """
    # The ProvenanceService class acts as a service layer that interacts with the TINStore to handle provenance data operations. 
    # It provides methods to save runs, vertices, edges, interactions, states, and window outputs. 
    # Each method takes a corresponding data model as input and delegates the insertion operation to the TINStore instance. 
    # The service also handles the conversion of attribution JSON data to string format before passing it to the store's insert methods.
    def __init__(self, db: IDatabase = None):
        if db is None:
            db = DuckDB(db_path="data/attribution.duckdb")
        self.store = TINStore(db=db)

    def save_run(self, data: RunModel) -> RunModel:
        return self.store.insert_run(data)

    def save_vertex(self, data: VertexModel) -> VertexModel:
        return self.store.insert_vertex(data)

    def save_edge(self, data: EdgeModel) -> EdgeModel:
        return self.store.insert_edge(data)

    # save_interaction, save_state, and save_window_output methods are responsible for saving interactions, states, and window outputs respectively. 
    # They convert the attribution_json to a string before passing it to the store's insert methods.
    def save_interaction(self, data: InteractionModel) -> InteractionModel:
        attr_str = json.dumps(data.attribution_json)
        return self.store.insert_interaction(data, attr_str)

    def save_state(self, data: StateModel) -> StateModel:
        attr_str = json.dumps(data.attribution_json)
        return self.store.insert_state(data, attr_str)

    def save_window_output(self, data: WindowOutputModel) -> WindowOutputModel:
        output_id = uuid.uuid4().int & ((1 << 63) - 1)
        data.output_id = output_id 
        attr_str = json.dumps(data.attribution_json)
        self.store.insert_window_output(output_id, data, attr_str)
        return data

    # bulk_save_window_outputs method allows for the bulk saving of multiple window outputs.
    def bulk_save_window_outputs(self, models: list[WindowOutputModel]) -> list[WindowOutputModel]:
        records_to_insert = []
        for data in models:
            output_id = uuid.uuid4().int & ((1 << 63) - 1)
            data.output_id = output_id
            attr_str = json.dumps(data.attribution_json)
            records_to_insert.append(
                    (output_id, data.run_id, data.vertex_id, data.category, data.window_start, 
                    data.window_end, data.output_count, attr_str)
                )
            
        self.store.bulk_insert_window_outputs(records_to_insert)
        
        return models