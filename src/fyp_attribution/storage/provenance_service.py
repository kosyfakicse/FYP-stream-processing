import json
import uuid
from loguru import logger
from src.fyp_attribution.core.models import (
    RunModel, VertexModel, EdgeModel, 
    InteractionModel, StateModel, WindowOutputModel
)
from src.fyp_attribution.storage.tin_store import TINStore

class ProvenanceService:
    """
    Service Layer responsible for handling provenance data operations.
    """
    # The ProvenanceService class acts as a service layer that interacts with the TINStore to handle provenance data operations. 
    # It provides methods to save runs, vertices, edges, interactions, states, and window outputs. 
    # Each method takes a corresponding data model as input and delegates the insertion operation to the TINStore instance. 
    # The service also handles the conversion of attribution JSON data to string format before passing it to the store's insert methods.
    def __init__(self):
        self.store = TINStore()

    def save_run(self, data: RunModel) -> RunModel | str:
        return self.store.insert_run(data)

    def save_vertex(self, data: VertexModel) -> VertexModel | str:
        return self.store.insert_vertex(data)

    def save_edge(self, data: EdgeModel) -> EdgeModel | str:
        return self.store.insert_edge(data)

    # save_interaction, save_state, and save_window_output methods are responsible for saving interactions, states, and window outputs respectively. 
    # They convert the attribution_json to a string before passing it to the store's insert methods.
    def save_interaction(self, data: InteractionModel) -> InteractionModel | str:
        attr_str = json.dumps(data.attribution_json)
        return self.store.insert_interaction(data, attr_str)

    def save_state(self, data: StateModel) -> StateModel | str:
        attr_str = json.dumps(data.attribution_json)
        return self.store.insert_state(data, attr_str)

    def save_window_output(self, data: WindowOutputModel) -> WindowOutputModel | str:
        output_id = uuid.uuid4().int & ((1 << 63) - 1)
        logger.info(f"Service: Generated output_id={output_id} for run={data.run_id}")
        
        attr_str = json.dumps(data.attribution_json)
        return self.store.insert_window_output(output_id, data, attr_str)