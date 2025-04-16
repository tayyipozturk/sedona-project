from analysis.advanced_queries2 import *

def test_advanced_queries2(sedona, nodes_df, edges_df):
    print("\n=== Connected Components Estimation ===")
    estimate_connected_components(edges_df, sedona).show()

    print("\n=== Intersection Density Grid ===")
    compute_intersection_density(nodes_df, sedona, cell_size=0.01).show(20, truncate=False)

    print("\n=== Edges Near Nodes ===")
    find_edges_near_nodes(edges_df, nodes_df, sedona, distance=0.001).show(20, truncate=False)

    print("\n=== Road Intersections ===")
    compute_road_intersections(edges_df, sedona).show(20, truncate=False)

    print("\n=== Urban Center Radius ===")
    estimate_urban_radius(nodes_df, sedona).show()

    print("\n=== Radial Intersection Distribution ===")
    intersection_distribution_by_radius(nodes_df, sedona, center_x=33.5, center_y=39.0, bin_size=0.01).show(10)

    print("\n=== Bridges and Tunnels Summary ===")
    summarize_bridges_tunnels(edges_df, sedona).show()

    print("\n=== Top 10 Longest Named Roads ===")
    top_longest_named_roads(edges_df, sedona, top_n=10).show()