from analysis.edge.edge_queries import *

def test_edge_queries(sedona, edges_df):
    print("\n=== TEST EDGE TYPE IMPLEMENTATIONS ===")

    print("\n=== Road Type Summary ===")
    summarize_roads_by_type(edges_df, sedona).show(5, truncate=False)

    print("\n=== Grid Coverage ===")
    compute_grid_coverage(edges_df, sedona).show(5, truncate=False)

    print("\n=== Clipped Edges (BBox 33.4,38.9 to 33.6,39.1) ===")
    clip_edges_to_bbox(edges_df, sedona, 33.4, 38.9, 33.6, 39.1).show(5, truncate=False)

    print("\n=== Short Segments (length < 5) ===")
    find_short_segments(edges_df, sedona, 5.0).show(5, truncate=False)

    print("\n=== Road Type Distribution ===")
    summarize_road_types(edges_df, sedona).show(5, truncate=False)

    print("\n=== Connected Component Estimation (Buffered) ===")
    estimate_connected_components(edges_df, sedona).show()

    print("\n=== Road Intersections (Line-Line Intersects) ===")
    compute_road_intersections(edges_df, sedona).show(5, truncate=False)

    print("\n=== Bridges and Tunnels Summary ===")
    summarize_bridges_tunnels(edges_df, sedona).show()

    print("\n=== Top 10 Longest Named Roads ===")
    top_longest_named_roads(edges_df, sedona, 10).show()