from analysis.spatial.spatial_joins import indexed_point_in_polygon, indexed_polygon_polygon_overlap, line_intersects_polygon_join, linestring_touches_polygon, point_crosses_linestring, point_distance_join, point_in_polygon_join, polygon_overlaps_join, range_query_polygon
from sedona.utils.adapter import Adapter
from pyspark.sql.functions import expr
from shapely.geometry import Point, box
from sedona.core.geom.envelope import Envelope
from sedona.core.enums import IndexType



def create_test_polygon_rdd(sedona):
    polygon_wkt = "POLYGON((32.5 39.6, 32.5 40.1, 33.0 40.1, 33.0 39.6, 32.5 39.6))"
    polygon_df = sedona.createDataFrame([(polygon_wkt,)], ["geometry"])
    polygon_df = polygon_df.withColumn("geometry", expr("ST_GeomFromWKT(geometry)"))
    polygon_rdd = Adapter.toSpatialRdd(polygon_df, "geometry")
    polygon_rdd.analyze()
    return polygon_rdd


# Phase 1

def test_point_in_polygon_join(points_rdd, polygon_rdd):
    print("[TEST] Point-in-Polygon Join")
    result = point_in_polygon_join(points_rdd, polygon_rdd)
    print("[RESULT] Points in Polygon:", result.count())


def test_line_intersects_polygon(lines_rdd, polygon_rdd):
    print("[TEST] Line Intersects Polygon")
    result = line_intersects_polygon_join(lines_rdd, polygon_rdd)
    print("[RESULT] Lines intersecting polygon:", result.count())


def test_polygon_polygon_overlap_join(polygon_rdd1, polygon_rdd2):
    print("[TEST] Polygon-Polygon Overlap Join")
    result = polygon_overlaps_join(polygon_rdd1, polygon_rdd2)
    print("[RESULT] Polygon Overlaps:", result.count())


def test_point_distance_join(sedona, points_rdd, polygon_rdd, distance):
    print("[TEST] Point Distance Join")
    result = point_distance_join(sedona, points_rdd, polygon_rdd, distance)
    print("[RESULT] Points within distance:", result.count())


# Phase 2

def test_range_query_polygon(points_rdd, envelope):
    print("[TEST] Range Query Polygon")
    result = range_query_polygon(points_rdd, envelope)
    print("[RESULT] Points in Range:", result.count())


def test_linestring_touches_polygon(lines_rdd, polygon_rdd):
    print("[TEST] Linestring Touches Polygon")
    result = linestring_touches_polygon(lines_rdd, polygon_rdd)
    print("[RESULT] Linestring touches Polygon:", result.count())


def test_point_crosses_linestring(points_rdd, lines_rdd):
    print("[TEST] Point Crosses Linestring")
    result = point_crosses_linestring(points_rdd, lines_rdd)
    print("[RESULT] Points crossing Linestring:", result.count())


# Phase 3

def test_indexed_point_in_polygon(points_rdd, polygon_rdd):
    print("[TEST] Indexed Point-in-Polygon Join")
    result = indexed_point_in_polygon(points_rdd, polygon_rdd)
    print("[RESULT] Indexed Points in Polygon:", result.count())


def test_indexed_polygon_polygon_overlap(polygon_rdd1, polygon_rdd2):
    print("[TEST] Indexed Polygon-Polygon Overlap Join")
    polygon_rdd1.buildIndex(IndexType.RTREE, True)
    result = indexed_polygon_polygon_overlap(polygon_rdd1, polygon_rdd2)
    print("[RESULT] Indexed Polygon Overlaps:", result.count())


# Accumulated tests

def test_spatial_joins1(sedona, points_rdd, lines_rdd):
    polygon_rdd1 = create_test_polygon_rdd(sedona)
    polygon_rdd2 = create_test_polygon_rdd(sedona)
    
    print("Test Spatial Joins Phase 1")
    print("===================================")
    
    test_point_in_polygon_join(points_rdd, polygon_rdd1)
    test_line_intersects_polygon(lines_rdd, polygon_rdd1)
    test_polygon_polygon_overlap_join(polygon_rdd1, polygon_rdd2)
    test_point_distance_join(sedona, points_rdd, polygon_rdd1, 0.5)


def test_spatial_joins2(sedona, points_rdd, lines_rdd):
    polygon_rdd1 = create_test_polygon_rdd(sedona)
    polygon_rdd2 = create_test_polygon_rdd(sedona)
    
    print("Test Spatial Joins Phase 2")
    print("===================================")

    min_x, max_x = 32.5, 33.0
    min_y, max_y = 39.6, 40.1
    envelope = Envelope(min_x, max_x, min_y, max_y)

    test_range_query_polygon(points_rdd, envelope)
    test_linestring_touches_polygon(lines_rdd, polygon_rdd1)
    test_point_crosses_linestring(points_rdd, lines_rdd)
    print("\n")


def test_spatial_joins3(sedona, points_rdd):
    polygon_rdd1 = create_test_polygon_rdd(sedona)
    polygon_rdd2 = create_test_polygon_rdd(sedona)
    
    print("Test Spatial Joins Phase 3")
    print("===================================")
    
    test_indexed_point_in_polygon(points_rdd, polygon_rdd1)
    test_indexed_polygon_polygon_overlap(polygon_rdd1, polygon_rdd2)
    
    
def add_dummy_polygons(sedona, count=20):
    dummy_polys = [(box(32 + i * 0.01, 39, 32 + i * 0.01 + 0.005, 39.005).wkt,) for i in range(count)]
    dummy_df = sedona.createDataFrame(dummy_polys, ["geometry"])
    dummy_df = dummy_df.withColumn("geometry", expr("ST_GeomFromWKT(geometry)"))
    dummy_rdd = Adapter.toSpatialRdd(dummy_df, "geometry")
    return dummy_rdd