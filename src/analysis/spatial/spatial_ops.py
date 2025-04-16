from sedona.core.SpatialRDD import PointRDD, PolygonRDD
from sedona.core.enums import IndexType, GridType
from sedona.core.spatialOperator import JoinQuery

def spatial_partition_rdd(rdd, grid_type=GridType.KDBTREE):
    rdd.analyze()
    rdd.spatialPartitioning(grid_type)
    rdd.rawSpatialRDD.persist()
    return rdd

def spatial_join_points_with_polygon(points_rdd: PointRDD, polygon_rdd: PolygonRDD):
    spatial_partition_rdd(points_rdd)
    polygon_rdd.analyze()
    polygon_rdd.spatialPartitioning(points_rdd.getPartitioner())
    return JoinQuery.SpatialJoinQuery(points_rdd, polygon_rdd, False, True)

def spatial_join_with_index(points_rdd: PointRDD, polygon_rdd: PolygonRDD):
    points_rdd.analyze()
    polygon_rdd.analyze()

    points_rdd.spatialPartitioning(GridType.KDBTREE)
    polygon_rdd.spatialPartitioning(points_rdd.getPartitioner())

    points_rdd.buildIndex(IndexType.RTREE, True)
    return JoinQuery.SpatialJoinQuery(points_rdd, polygon_rdd, True, True)

def buffer_and_filter_points(points_rdd: PointRDD, buffer_radius: float):
    return points_rdd.rawSpatialRDD.map(lambda geom: geom.buffer(buffer_radius))

def filter_points_within_polygon(points_rdd: PointRDD, polygon_rdd: PolygonRDD):
    spatial_partition_rdd(points_rdd)
    polygon_rdd.analyze()
    polygon_rdd.spatialPartitioning(points_rdd.getPartitioner())
    return JoinQuery.SpatialJoinQuery(points_rdd, polygon_rdd, False, False)
