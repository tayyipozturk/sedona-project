from analysis.cross.cross_queries import *

def test_cross_queries(sedona, nodes_df, edges_df):
    print("\n=== TEST CROSS TYPE IMPLEMENTATIONS ===")

    print("\n=== Edges Near Nodes (Distance < 0.001) ===")
    find_edges_near_nodes(edges_df, nodes_df, sedona, 0.001).show(5, truncate=False)

    print("\n=== Anchor Edges to Nodes ===")
    anchor_edges_to_nodes(edges_df, nodes_df, sedona).show(5, truncate=False)

    print("\n=== Node Road Centrality ===")
    estimate_node_road_centrality(edges_df, nodes_df, sedona).show(5, truncate=False)

    print("\n=== Local Average Speed Per Node ===")
    local_average_speed_per_node(edges_df, nodes_df, sedona).show(5, truncate=False)

    # print("\n=== Classify Intersection Types ===")
    # classify_intersection_types(nodes_df, edges_df, sedona, 3).show(5, truncate=False)

    # print("\n=== Traffic Proxy to Edges ===")
    # assign_traffic_proxy_to_edges(edges_df, nodes_df, sedona).show(5, truncate=False)

    print("\n=== Road Class Accessibility ===")
    compute_road_class_accessibility(nodes_df, edges_df, sedona).show(5, truncate=False)

    # print("\n=== Node-to-Node Travel Estimation ===")
    # estimate_node_to_node_travel(edges_df, nodes_df, sedona).show(5, truncate=False)

    print("\n=== Entry/Exit Nodes of Bounding Box ===")
    detect_entry_exit_nodes(nodes_df, edges_df, sedona, 33.4, 38.9, 33.6, 39.1).show(5, truncate=False)
