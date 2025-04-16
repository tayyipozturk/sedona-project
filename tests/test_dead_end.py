from analysis.node.dead_end import compute_dead_end_nodes

def test_detect_dead_end_nodes(sedona, point_rdd):
    result_df = compute_dead_end_nodes(point_rdd, sedona)
    result_df.show()

    assert result_df.count() > 0