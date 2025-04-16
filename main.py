from src.config.sedona_context import create_sedona_session
from src.loader.sedona_loader import load_csv_as_dataframe, load_nodes_as_point_rdd, load_edges_as_linestring_rdd
from sedona.core.SpatialRDD import PolygonRDD
from tests.test_spatial_summary import test_spatial_summary
from tests.test_spatial_ops import test_spatial_ops
from tests.test_spatial_joins import test_spatial_joins1, test_spatial_joins2, test_spatial_joins3
from tests.test_accessibility import test_compute_accessibility_score
from tests.test_bottleneck_detector import test_detect_bottleneck_roads
from tests.test_dead_end import test_detect_dead_end_nodes
from tests.test_density_heatmap import test_compute_road_density
from tests.test_intersection_score import test_compute_intersection_importance
from tests.test_speed_estimator import test_estimate_road_speeds
from tests.test_advanced_queries import test_advanced_queries
from tests.test_advanced_queries2 import test_advanced_queries2

sedona = create_sedona_session()

nodes_df = load_csv_as_dataframe(sedona, "data/nodes_ankara.csv")
edges_df = load_csv_as_dataframe(sedona, "data/edges_ankara.csv")

nodes_rdd = load_nodes_as_point_rdd(sedona, "data/nodes_ankara.csv")
edges_rdd = load_edges_as_linestring_rdd(sedona, "data/edges_ankara.csv")

# test_spatial_summary(sedona, nodes_rdd, edges_rdd) # Works! 0

# test_spatial_ops(sedona, nodes_rdd) # Works! 1
# test_spatial_joins1(sedona, nodes_rdd, edges_rdd) # Works! 2
# test_spatial_joins2(sedona, nodes_rdd, edges_rdd) # Works! 3
# test_spatial_joins3(sedona, nodes_rdd) # Works! 4

# test_compute_accessibility_score(sedona, nodes_df)
# test_detect_bottleneck_roads(sedona, edges_df)
# test_detect_dead_end_nodes(sedona, nodes_df)
# test_compute_road_density(sedona, edges_df)
# test_compute_intersection_importance(sedona, nodes_df)
# test_estimate_road_speeds(sedona, edges_df)

# test_advanced_queries(sedona, nodes_df, edges_df)
test_advanced_queries2(sedona, nodes_df, edges_df)