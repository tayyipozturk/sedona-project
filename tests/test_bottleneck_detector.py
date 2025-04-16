from analysis.edge.bottleneck_detector import compute_bottleneck_score

def test_detect_bottleneck_roads(sedona, line_rdd):
    result_df = compute_bottleneck_score(line_rdd, sedona)
    result_df.show()

    assert "bottleneck_score" in result_df.columns
    assert result_df.count() > 0
