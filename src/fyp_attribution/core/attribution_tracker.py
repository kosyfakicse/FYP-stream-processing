from typing import Dict, List

class AttributionTracker:
    """
    Utility class for merging attribution dictionaries.
    Used for backward queries and data aggregation.
    """

    @staticmethod
    def merge_attributions(attr1: Dict[str, int], attr2: Dict[str, int]) -> Dict[str, int]:
        merged = attr1.copy()
        for source_id, qty in attr2.items():
            merged[source_id] = merged.get(source_id, 0) + qty
        return merged

    @staticmethod
    def sum_all(attributions: List[Dict[str, int]]) -> Dict[str, int]:
        result = {}
        for attr in attributions:
            result = AttributionTracker.merge_attributions(result, attr)
        return result
