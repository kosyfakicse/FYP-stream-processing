from pydantic import BaseModel

class TweetContribution(BaseModel):
    output_id: int
    run_id: str
    window_start: int
    window_end: int
    output_count: int = 0
    attribution: dict = {}

    