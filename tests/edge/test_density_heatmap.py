from analysis.edge.density_heatmap import compute_density_heatmap

def test_compute_road_density(sedona, line_rdd):
    result_df = compute_density_heatmap(line_rdd, sedona, cell_size=0.01)
    result_df.show()

    assert "total_road_length" in result_df.columns
