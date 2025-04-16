from analysis.node.intersection_score import compute_intersection_score

def test_compute_intersection_importance(sedona, point_rdd):
    result_df = compute_intersection_score(point_rdd, sedona)
    result_df.show()

    assert "intersection_score" in result_df.columns
    assert result_df.count() > 0
