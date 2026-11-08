from src.fyp_attribution.core.attribution_tracker import AttributionTracker

def test_merge_attributions():
    attr1 = {"user_1": 2}
    attr2 = {"user_1": 1, "user_2": 3}
    result = AttributionTracker.merge_attributions(attr1, attr2)
    assert result == {"user_1": 3, "user_2": 3}

def test_sum_all():
    attributions = [{"user_1": 2}, {"user_2": 1}, {"user_1": 1, "user_3": 3}]
    result = AttributionTracker.sum_all(attributions)
    assert result == {"user_1": 3, "user_2": 1, "user_3": 3}