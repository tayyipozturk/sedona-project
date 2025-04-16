from analysis.advanced_queries import *

def test_advanced_queries(sedona, nodes_df, edges_df):
    print("\n=== Nearest Node to (39.0, 33.5) ===")
    find_nearest_node(nodes_df, sedona, 39.0, 33.5).show(1, truncate=False)

    print("\n=== Longest Roads Per Road Type ===")
    summarize_roads_by_type(edges_df, sedona).show(20, truncate=False)

    print("\n=== Road Coverage by Grid Cell ===")
    compute_grid_coverage(edges_df, sedona, cell_size=0.01).show(20, truncate=False)

    print("\n=== Clipped Roads in Bounding Box ===")
    clip_edges_to_bbox(edges_df, sedona, 33.4, 38.9, 33.6, 39.1).show(20, truncate=False)

    print("\n=== Shortest Segments (length < 5) ===")
    find_short_segments(edges_df, sedona, 5.0).show(20, truncate=False)

    print("\n=== Major Intersections (degree ≥ 4) ===")
    detect_major_intersections(nodes_df, sedona, 4).show(20, truncate=False)

    print("\n=== Road Type Distribution ===")
    summarize_road_types(edges_df, sedona).show(20, truncate=False)