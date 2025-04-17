from datetime import datetime
from src.config.sedona_context import create_sedona_session
from src.config.log import log_to_file
from src.loader.sedona_loader import load_csv_as_dataframe, load_nodes_as_point_rdd, load_edges_as_linestring_rdd
from tests.edge.test_bottleneck_detector import test_detect_bottleneck_roads
from tests.edge.test_density_heatmap import test_compute_road_density
from tests.edge.test_speed_estimator import test_estimate_road_speeds
from tests.node.test_accessibility import test_compute_accessibility_score
from tests.node.test_dead_end import test_detect_dead_end_nodes
from tests.node.test_intersection_score import test_compute_intersection_importance
from tests.spatial.test_spatial_joins import test_spatial_joins1, test_spatial_joins2, test_spatial_joins3
from tests.spatial.test_spatial_ops import test_spatial_ops
from tests.spatial.test_spatial_summary import test_spatial_summary
from tests.node.test_node_queries import *
from tests.edge.test_edge_queries import *
from tests.cross.test_cross_queries import *
from tests.test_topological_inference_queries import *
from tests.test_matching import *

timestamp = datetime.now().strftime("%y_%m_%d_%H_%M_%S")
filepath = f"logs/{timestamp}.log"
with log_to_file(filepath):
    sedona = create_sedona_session()

    nodes_df = load_csv_as_dataframe(sedona, "data/nodes_ankara.csv")
    edges_df = load_csv_as_dataframe(sedona, "data/edges_ankara.csv")

    nodes_rdd = load_nodes_as_point_rdd(sedona, "data/nodes_ankara.csv")
    edges_rdd = load_edges_as_linestring_rdd(sedona, "data/edges_ankara.csv")

    test_spatial_summary(sedona, nodes_rdd, edges_rdd) # Works! 0

    test_spatial_ops(sedona, nodes_rdd) # Works! 1
    test_spatial_joins1(sedona, nodes_rdd, edges_rdd) # Works! 2
    test_spatial_joins2(sedona, nodes_rdd, edges_rdd) # Works! 3
    test_spatial_joins3(sedona, nodes_rdd) # Works! 4

    test_compute_accessibility_score(sedona, nodes_df)
    test_detect_bottleneck_roads(sedona, edges_df)
    test_detect_dead_end_nodes(sedona, nodes_df)
    test_compute_road_density(sedona, edges_df)
    test_compute_intersection_importance(sedona, nodes_df)
    test_estimate_road_speeds(sedona, edges_df)

    test_node_queries(sedona, nodes_df)
    test_edge_queries(sedona, edges_df)
    test_cross_queries(sedona, nodes_df, edges_df)

    test_topological_inference(sedona, nodes_df, edges_df)

    test_match_line_to_road(sedona, edges_df, tolerance=0.001)
    test_match_point_to_road(sedona, edges_df, tolerance=0.001)
    test_find_nearest_intersection_to_point(sedona, nodes_df, tolerance=0.001)
    test_find_connected_intersection_from_line(sedona, nodes_df, tolerance=0.001)

    sedona.stop()
    print(f"Log file created at {filepath}")