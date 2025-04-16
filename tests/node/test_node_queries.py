from analysis.node.node_queries import *

def test_node_queries(sedona, nodes_df):
    print("\n=== TEST NODE TYPE IMPLEMENTATIONS ===")

    print("\n=== Nearest Node to (39.0, 33.5) ===")
    find_nearest_node(nodes_df, sedona, 39.0, 33.5).show(1, truncate=False)

    print("\n=== Major Intersections (degree ≥ 4) ===")
    detect_major_intersections(nodes_df, sedona, 4).show(5, truncate=False)

    print("\n=== Intersection Density Grid ===")
    compute_intersection_density(nodes_df, sedona, cell_size=0.01).show(5, truncate=False)

    print("\n=== Urban Center Radius ===")
    estimate_urban_radius(nodes_df, sedona).show()

    print("\n=== Radial Intersection Distribution ===")
    intersection_distribution_by_radius(nodes_df, sedona, center_x=33.5, center_y=39.0, bin_size=0.01).show(10)
