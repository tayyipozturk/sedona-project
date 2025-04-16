
from analysis.edge.speed_profile import average_speed_by_road_type, average_travel_time, fastest_road_segments
from analysis.spatial.connectivity import count_intersections, high_degree_nodes, calculate_edge_density
from analysis.spatial.spatial_summary import summarize_spatial_rdd


def test_spatial_summary(sedona, nodes_rdd, edges_rdd):
    print("[SUMMARY] Nodes:", summarize_spatial_rdd(nodes_rdd, "Nodes"))
    print("[SUMMARY] Edges:", summarize_spatial_rdd(edges_rdd, "Edges"))

    nodes_df = sedona.read.option("header", True).csv("data/nodes_ankara.csv")
    edges_df = sedona.read.option("header", True).csv("data/edges_ankara.csv")

    print("[CONNECTIVITY] Unique Intersections:", count_intersections(nodes_df))
    high_deg = high_degree_nodes(nodes_df)
    print("[CONNECTIVITY] High-degree intersections:")
    high_deg.show()

    density = calculate_edge_density(edges_df, nodes_df)
    print("[CONNECTIVITY] Edge density:", density)

    print("[SPEED] Average speed by road type:")
    average_speed_by_road_type(edges_df).show()

    print("[SPEED] Average travel time:", average_travel_time(edges_df))

    print("[SPEED] Fastest road segments:")
    fastest_road_segments(edges_df).show()