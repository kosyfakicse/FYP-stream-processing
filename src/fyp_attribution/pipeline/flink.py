from collections import Counter
from typing import Iterable
import json

from pyflink.datastream.functions import AggregateFunction
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import ProcessWindowFunction
from pyflink.datastream.window import TimeWindow


class TweetTimestampAssigner(TimestampAssigner):
    """
    Timestamp assigner for (user_id, text, timestamp)
    index = 2
    """
    @classmethod
    def extract_timestamp(self, value, record_timestamp) -> int:
        return int(value[2])

class CountWindowProcessFunction(ProcessWindowFunction[tuple, tuple, str, TimeWindow]):
    def process(self,
                key: str,
                context: ProcessWindowFunction.Context[TimeWindow],
                elements: Iterable[tuple]) -> Iterable[tuple]:
        return [(key, len([e for e in elements]), context.window().start, context.window().end)]


class HashtagAttributionProcessFunction(ProcessWindowFunction):
    """Legacy window function - groups by user_id, outputs all hashtags per user."""
    def process(self, key, context, elements):
        user_id = key
        window_start = context.window().start
        window_end = context.window().end
        
        hashtag_counts = Counter()
        element_count = 0
        
        for record in elements:
            uid, hashtags_json, timestamp = record
            # Parse JSON string back to list
            hashtags = json.loads(hashtags_json)
            hashtag_counts.update(hashtags)
            element_count += 1
        
        # Convert to JSON string
        hashtag_json = json.dumps(dict(hashtag_counts))
        
        # Log window trigger
        import datetime
        print(f"[WINDOW] user={user_id}, elements={element_count}, "
              f"window={datetime.datetime.fromtimestamp(window_start/1000)} to "
              f"{datetime.datetime.fromtimestamp(window_end/1000)}, "
              f"hashtags={hashtag_json}")
        
        return [(user_id, hashtag_json, window_start, window_end)]


class CategoryAttributionProcessFunction(ProcessWindowFunction):
    """
    Window function that outputs per-category (hashtag) attribution.
    
    Input: Records keyed by category (hashtag)
           Each record: (category, user_id, timestamp)
    
    Output: (window_start, window_end, category, output_count, attribution_json)
            Matches window_outputs table schema
    
    Example:
        Input: 100 records with category="PYTHON" from user_1(30), user_2(40), user_3(30)
        Output: (1735696800000, 1735696860000, "PYTHON", 100, '{"user_1": 30, "user_2": 40, "user_3": 30}')
    """
    def process(self, key, context, elements):
        category = key  # The hashtag (e.g., "PYTHON", "DATA")
        window_start = context.window().start
        window_end = context.window().end
        
        user_attribution = Counter()
        output_count = 0
        
        for record in elements:
            cat, user_id, timestamp = record
            user_attribution[user_id] += 1
            output_count += 1
        
        # Convert attribution to JSON string
        attribution_json = json.dumps(dict(user_attribution))
        
        
        # Output format matches window_outputs table schema
        return [(window_start, window_end, category, output_count, attribution_json)]

class AttributionCount(AggregateFunction):
    """
    """

    def create_accumulator(self):
        return {"user_id": None, "count": 0}
    
    def add(self, value, acc):
        user_id = value[0]
        acc["user_id"] = user_id
        acc["count"] += 1
        return acc
    
    def get_result(self, acc):
        return acc
    
    def merge(self, acc_a, acc_b):
        return {
            "user_id": acc_a["user_id"],
            "count": acc_a["count"] + acc_b["count"]
        }
