1. Single Insertion: Run
**Function**: `save_run(data: RunModel) -> RunModel`

**How to call:**
```python
service = ProvenanceService()

data = RunModel(
    run_id="run-001",
    started_at="2026-11-07 10:00:00",
    dataset="tweet_hashtag",
    provenance_enabled=True
)
```
# Call the function
result = service.save_run(data)
Return:
Returns the RunModel object.

2. Single Insertion: Vertex
Function: save_vertex(data: VertexModel) -> VertexModel

**How to call:**
```python
service = ProvenanceService()

data = VertexModel(
    vertex_id="V1",
    run_id="run-001",
    vertex_type="source",
    description="Twitter Source"
)
```
# Call the function
result = service.save_vertex(data)
Return:
Returns the VertexModel object.

3. Single Insertion: Edge
Function: save_edge(data: EdgeModel) -> EdgeModel

**How to call:**
```python
service = ProvenanceService()

data = EdgeModel(
    source_vertex="V1",
    target_vertex="V2",
    run_id="run-001"
)
```
# Call the function
result = service.save_edge(data)
Return:
Returns the EdgeModel object.

4. Single Insertion: Interaction
Function: save_interaction(data: InteractionModel) -> InteractionModel

**How to call:**
```python
service = ProvenanceService()

data = InteractionModel(
    interaction_id=1001,
    run_id="run-001",
    source_vertex="V1",
    target_vertex="V2",
    timestamp=1735696800000,
    quantity=4,
    attribution_json={"user_1": 2, "user_2": 1}
)
```
# Call the function
result = service.save_interaction(data)
Return:
Returns the InteractionModel object.

5. Single Insertion: State
Function: save_state(data: StateModel) -> StateModel

**How to call:**
```python
service = ProvenanceService()

data = StateModel(
    state_id=2001,
    run_id="run-001",
    vertex_id="V1",
    time_start=1735696800000,
    time_end=1735696860000,
    quantity=4,
    attribution_json={"user_1": 2, "user_2": 1}
)
```
# Call the function
result = service.save_state(data)
Return:
Returns the StateModel object.

6. Single Insertion: Window Output
Function: save_window_output(data: WindowOutputModel) -> WindowOutputModel

**How to call:**
```python
service = ProvenanceService()

data = WindowOutputModel(
    run_id="run-001",
    vertex_id="W1",
    window_start=1735696800000,
    window_end=1735696860000,
    output_count=4,
    attribution_json={"user_1": 2, "user_2": 1}
)
```
# Call the function
result = service.save_window_output(data)
Return:
Returns the WindowOutputModel object

7. Bulk Insertion: Window Output
Function: bulk_save_window_outputs(models: list[WindowOutputModel]) -> list[WindowOutputModel]

**How to call:**
```python
service = ProvenanceService()

data_list = [
    WindowOutputModel(run_id="run-001", vertex_id="W1", window_start=1000, window_end=2000, output_count=10, attribution_json={"user_1": 10}),
    WindowOutputModel(run_id="run-001", vertex_id="W1", window_start=1000, window_end=2000, output_count=5, attribution_json={"user_2": 5})
]
```
# Call the function
results = service.bulk_save_window_outputs(data_list)
Return:
Returns the list[WindowOutputModel]