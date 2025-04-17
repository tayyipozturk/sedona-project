from analysis.topological_inference_queries import *

def test_topological_inference(sedona, nodes_df, edges_df):
    print("\n=== Betweenness Centrality Approximation ===")
    estimate_betweenness_proxy(nodes_df, edges_df, sedona).show(5, truncate=False)

    print("\n=== Triangle Cycle Detection ===")
    detect_cycles(edges_df, sedona).show(5, truncate=False)

    print("\n=== Subnetworks by Highway Attribute ===")
    detect_subnetworks_by_attribute(edges_df, sedona).show(5, truncate=False)

    print("\n=== Directional Conflicts (Oneway Check) ===")
    check_directional_conflicts(edges_df, sedona).show(5, truncate=False)

    print("\n=== Junction Shape Classification ===")
    classify_junction_shapes(nodes_df, edges_df, sedona).show(5, truncate=False)