from pyspark.sql.functions import expr
from config.performance_util import performance_logged

@performance_logged(label="match_line_to_road", show=False, save_path="match_line_to_road.csv")
def match_line_to_road(sedona, edges_df, line_wkt, tolerance=0.0005):
    df = edges_df.withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("edges")

    sedona.sql(f"""
        SELECT *, ST_Distance(geom, ST_GeomFromWKT('{line_wkt}')) AS dist
        FROM edges
        WHERE ST_DWithin(geom, ST_GeomFromWKT('{line_wkt}'), {tolerance})
        ORDER BY dist ASC
        LIMIT 1
    """).createOrReplaceTempView("matched_road")

    return sedona.sql("SELECT name, highway, ref, length, geometry, dist FROM matched_road")

@performance_logged(label="find_road_from_point", show=False, save_path="find_road_from_point.csv")
def find_road_from_point(sedona, edges_df, point_wkt, tolerance=0.0005):
    df = edges_df.withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")
    df.createOrReplaceTempView("edges")

    sedona.sql(f"""
        SELECT *, ST_Distance(geom, ST_GeomFromWKT('{point_wkt}')) AS dist
        FROM edges
        WHERE ST_DWithin(geom, ST_GeomFromWKT('{point_wkt}'), {tolerance})
        ORDER BY dist ASC
        LIMIT 1
    """).createOrReplaceTempView("matched_road")

    return sedona.sql("SELECT name, highway, ref, length, geometry, dist FROM matched_road")

@performance_logged(label="find_nearest_intersection", show=False, save_path="find_nearest_intersection.csv")
def find_nearest_intersection(sedona, nodes_df, point_wkt, tolerance=0.0005):
    df = nodes_df.withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")
    df.createOrReplaceTempView("nodes")

    sedona.sql(f"""
        SELECT *, ST_Distance(geom, ST_GeomFromWKT('{point_wkt}')) AS dist
        FROM nodes
        WHERE ST_DWithin(geom, ST_GeomFromWKT('{point_wkt}'), {tolerance})
        ORDER BY dist ASC
        LIMIT 1
    """).createOrReplaceTempView("matched_node")

    return sedona.sql("SELECT x, y, street_count, geometry, dist FROM matched_node")

@performance_logged(label="find_connected_intersection_from_line", show=False, save_path="find_connected_intersection_from_line.csv")
def find_connected_intersection_from_line(sedona, nodes_df, line_wkt, tolerance=0.0005):
    df = nodes_df.withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")
    df.createOrReplaceTempView("nodes")

    sedona.sql(f"""
        SELECT *, ST_Distance(geom, ST_StartPoint(ST_GeomFromWKT('{line_wkt}'))) AS dist
        FROM nodes
        WHERE ST_DWithin(geom, ST_StartPoint(ST_GeomFromWKT('{line_wkt}')), {tolerance})
           OR ST_DWithin(geom, ST_EndPoint(ST_GeomFromWKT('{line_wkt}')), {tolerance})
        ORDER BY dist ASC
        LIMIT 1
    """).createOrReplaceTempView("connected_node")

    return sedona.sql("SELECT x, y, street_count, geometry, dist FROM connected_node")