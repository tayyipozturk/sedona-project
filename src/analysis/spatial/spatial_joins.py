from sedona.core.SpatialRDD import PointRDD, LineStringRDD, PolygonRDD
from sedona.core.enums import GridType, IndexType
from sedona.core.spatialOperator import JoinQuery, RangeQuery
from sedona.core.geom.envelope import Envelope
from sedona.utils.adapter import Adapter
from sedona.sql.types import GeometryType

from config.performance_util import performance_logged


# Phase 1

@performance_logged(label="point_in_polygon_join", show=False, save_path="point_in_polygon_join")
def point_in_polygon_join(points_rdd: PointRDD, polygon_rdd: PolygonRDD):
    points_rdd.analyze()
    points_rdd.spatialPartitioning(GridType.KDBTREE)
    polygon_rdd.spatialPartitioning(points_rdd.getPartitioner())
    return JoinQuery.SpatialJoinQuery(points_rdd, polygon_rdd, False, False)

@performance_logged(label="line_intersects_polygon_join", show=False, save_path="line_intersects_polygon_join")
def line_intersects_polygon_join(lines_rdd: LineStringRDD, polygon_rdd: PolygonRDD):
    lines_rdd.analyze()
    lines_rdd.spatialPartitioning(GridType.KDBTREE)
    polygon_rdd.spatialPartitioning(lines_rdd.getPartitioner())
    return JoinQuery.SpatialJoinQuery(lines_rdd, polygon_rdd, False, False)

@performance_logged(label="polygon_overlaps_join", show=False, save_path="polygon_overlaps_join")
def polygon_overlaps_join(poly_rdd1: PolygonRDD, poly_rdd2: PolygonRDD):
    safe_partition(poly_rdd1)
    safe_partition(poly_rdd2)
    poly_rdd2.spatialPartitioning(poly_rdd1.getPartitioner())
    return JoinQuery.SpatialJoinQuery(poly_rdd1, poly_rdd2, False, False)

@performance_logged(label="point_distance_join", show=False, save_path="point_distance_join")
def point_distance_join(sedona, points_rdd: PointRDD, polygon_rdd: PolygonRDD, distance: float):

    polygon_df = Adapter.toDf(polygon_rdd, sedona)
    polygon_df.createOrReplaceTempView("polygons")

    buffered_df = sedona.sql(f"""
        SELECT ST_Buffer(geometry, {distance}) AS geometry
        FROM polygons
    """)
    buffered_df = buffered_df.withColumn("geometry", buffered_df["geometry"].cast(GeometryType()))

    buffered_rdd = Adapter.toSpatialRdd(buffered_df, "geometry")
    buffered_rdd.analyze()
    buffered_rdd.spatialPartitioning(points_rdd.getPartitioner())

    points_rdd.analyze()
    points_rdd.spatialPartitioning(buffered_rdd.getPartitioner())

    return JoinQuery.SpatialJoinQuery(points_rdd, buffered_rdd, False, False)


# Phase 2

@performance_logged(label="range_query_polygon", show=False, save_path="range_query_polygon")
def range_query_polygon(points_rdd: PointRDD, envelope: Envelope):
    points_rdd.analyze()
    return RangeQuery.SpatialRangeQuery(points_rdd, envelope, False, False)

@performance_logged(label="linestring_touches_polygon", show=False, save_path="linestring_touches_polygon")
def linestring_touches_polygon(lines_rdd: LineStringRDD, polygon_rdd: PolygonRDD):
    lines_rdd.analyze()
    lines_rdd.spatialPartitioning(GridType.KDBTREE)
    polygon_rdd.spatialPartitioning(lines_rdd.getPartitioner())
    return JoinQuery.SpatialJoinQuery(lines_rdd, polygon_rdd, False, True)

@performance_logged(label="point_crosses_linestring", show=False, save_path="point_crosses_linestring")
def point_crosses_linestring(points_rdd: PointRDD, lines_rdd: LineStringRDD):
    points_rdd.analyze()
    points_rdd.spatialPartitioning(GridType.KDBTREE)
    lines_rdd.spatialPartitioning(points_rdd.getPartitioner())
    return JoinQuery.SpatialJoinQuery(points_rdd, lines_rdd, False, True)


# Phase 3

@performance_logged(label="indexed_point_in_polygon", show=False, save_path="indexed_point_in_polygon")
def indexed_point_in_polygon(points_rdd: PointRDD, polygon_rdd: PolygonRDD):
    points_rdd.analyze()
    points_rdd.spatialPartitioning(GridType.KDBTREE)
    points_rdd.buildIndex(IndexType.RTREE, True)
    polygon_rdd.analyze()
    polygon_rdd.spatialPartitioning(points_rdd.getPartitioner())
    return JoinQuery.SpatialJoinQuery(points_rdd, polygon_rdd, True, False)

@performance_logged(label="indexed_polygon_polygon_overlap", show=False, save_path="indexed_polygon_polygon_overlap")
def indexed_polygon_polygon_overlap(poly_rdd1: PolygonRDD, poly_rdd2: PolygonRDD):
    safe_partition(poly_rdd1)
    poly_rdd2.analyze()
    poly_rdd2.spatialPartitioning(poly_rdd1.getPartitioner())
    poly_rdd1.buildIndex(IndexType.RTREE, True)
    return JoinQuery.SpatialJoinQuery(poly_rdd1, poly_rdd2, True, False)

@performance_logged(label="safe_partition", show=False, save_path="safe_partition")
def safe_partition(rdd, grid_type=GridType.KDBTREE):
    rdd.analyze()
    if rdd.approximateTotalCount > 2 and rdd.getPartitioner() is None:
        rdd.spatialPartitioning(grid_type)