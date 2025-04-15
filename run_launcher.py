from src.config.sedona_context import create_spark_session
from src.loader.sedona_loader import load_nodes_as_point_rdd, load_edges_as_linestring_rdd
from sedona.core.SpatialRDD import PolygonRDD
from tests.test_spatial_summary import test_spatial_summary
from tests.test_spatial_ops import test_spatial_ops
from tests.test_spatial_joins import test_spatial_joins1, test_spatial_joins2, test_spatial_joins3

spark = create_spark_session()

nodes_rdd = load_nodes_as_point_rdd(spark, "data/nodes_ankara.csv")
edges_rdd = load_edges_as_linestring_rdd(spark, "data/edges_ankara.csv")

test_spatial_summary(spark, nodes_rdd, edges_rdd) # Works! 0

test_spatial_ops(spark, nodes_rdd) # Works! 1
test_spatial_joins1(spark, nodes_rdd, edges_rdd) # Works! 2
test_spatial_joins2(spark, nodes_rdd, edges_rdd) # Works! 3
test_spatial_joins3(spark, nodes_rdd) # Works! 4