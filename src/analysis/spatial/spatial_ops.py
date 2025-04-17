from sedona.core.SpatialRDD import PointRDD, PolygonRDD
from sedona.core.enums import IndexType, GridType
from sedona.core.spatialOperator import JoinQuery
from config.performance_util import performance_logged

@performance_logged(label="spatial_partition_rdd", show=False, save_path="spatial_partition_rdd")
def spatial_partition_rdd(rdd, grid_type=GridType.KDBTREE):
    rdd.analyze()
    rdd.spatialPartitioning(grid_type)
    rdd.rawSpatialRDD.persist()
    return rdd

@performance_logged(label="spatial_join_points_with_polygon", show=False, save_path="spatial_join_points_with_polygon")
def spatial_join_points_with_polygon(points_rdd: PointRDD, polygon_rdd: PolygonRDD):
    spatial_partition_rdd(points_rdd)
    polygon_rdd.analyze()
    polygon_rdd.spatialPartitioning(points_rdd.getPartitioner())
    return JoinQuery.SpatialJoinQuery(points_rdd, polygon_rdd, False, True)

@performance_logged(label="spatial_join_with_index", show=False, save_path="spatial_join_with_index")
def spatial_join_with_index(points_rdd: PointRDD, polygon_rdd: PolygonRDD):
    points_rdd.analyze()
    polygon_rdd.analyze()

    points_rdd.spatialPartitioning(GridType.KDBTREE)
    polygon_rdd.spatialPartitioning(points_rdd.getPartitioner())

    points_rdd.buildIndex(IndexType.RTREE, True)
    return JoinQuery.SpatialJoinQuery(points_rdd, polygon_rdd, True, True)

@performance_logged(label="buffer_and_filter_points", show=False, save_path="buffer_and_filter_points")
def buffer_and_filter_points(points_rdd: PointRDD, buffer_radius: float):
    return points_rdd.rawSpatialRDD.map(lambda geom: geom.buffer(buffer_radius))

@performance_logged(label="filter_points_within_polygon", show=False, save_path="filter_points_within_polygon")
def filter_points_within_polygon(points_rdd: PointRDD, polygon_rdd: PolygonRDD):
    spatial_partition_rdd(points_rdd)
    polygon_rdd.analyze()
    polygon_rdd.spatialPartitioning(points_rdd.getPartitioner())
    return JoinQuery.SpatialJoinQuery(points_rdd, polygon_rdd, False, False)
