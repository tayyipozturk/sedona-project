from analysis.node.accessibility import compute_accessibility_score

def test_compute_accessibility_score(sedona, nodes_df):
    result_df = compute_accessibility_score(nodes_df, sedona)
    result_df.show()

    assert "accessibility_score" in result_df.columns
    assert result_df.count() > 0