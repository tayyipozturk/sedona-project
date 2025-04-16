from analysis.edge.speed_estimator import compute_speed_estimation

def test_estimate_road_speeds(sedona, line_rdd):
    result_df = compute_speed_estimation(line_rdd, sedona)
    result_df.show()

    assert "estimated_speed" in result_df.columns
    assert result_df.count() > 0
