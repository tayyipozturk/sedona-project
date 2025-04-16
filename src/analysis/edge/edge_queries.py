from pyspark.sql.functions import col, expr

def summarize_roads_by_type(edges_df, sedona):
    df = edges_df.withColumn("length", col("length").cast("double")) \
                 .withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("edges")

    result = sedona.sql("""
        SELECT highway,
               COUNT(*) AS num_segments,
               SUM(length) AS total_length,
               MAX(length) AS max_length,
               AVG(length) AS avg_length
        FROM edges
        WHERE length IS NOT NULL
        GROUP BY highway
        ORDER BY total_length DESC
    """)

    return result


def compute_grid_coverage(edges_df, sedona, cell_size=0.01):
    df = edges_df.withColumn("length", col("length").cast("double")) \
                 .withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .withColumn("x", expr("ST_X(ST_StartPoint(geom))")) \
                 .withColumn("y", expr("ST_Y(ST_StartPoint(geom))")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("edges")

    result = sedona.sql(f"""
        SELECT
            CAST(x / {cell_size} AS INT) AS cell_x,
            CAST(y / {cell_size} AS INT) AS cell_y,
            COUNT(*) AS num_roads,
            SUM(length) AS total_length,
            AVG(length) AS avg_length
        FROM edges
        WHERE length IS NOT NULL
        GROUP BY cell_x, cell_y
    """)

    return result


def clip_edges_to_bbox(edges_df, sedona, min_x, min_y, max_x, max_y):
    df = edges_df.withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("edges")

    result = sedona.sql(f"""
        SELECT *
        FROM edges
        WHERE ST_Within(geom, ST_PolygonFromEnvelope({min_x}, {min_y}, {max_x}, {max_y}))
    """)

    return result


def find_short_segments(edges_df, sedona, length_threshold=5.0):
    df = edges_df.withColumn("length", col("length").cast("double")) \
                 .withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("edges")

    result = sedona.sql(f"""
        SELECT *
        FROM edges
        WHERE length IS NOT NULL AND length < {length_threshold}
        ORDER BY length ASC
    """)

    return result


def summarize_road_types(edges_df, sedona):
    df = edges_df.withColumn("length", col("length").cast("double")) \
                 .withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("edges")

    result = sedona.sql("""
        SELECT highway,
               COUNT(*) AS num_segments,
               SUM(length) AS total_length
        FROM edges
        WHERE length IS NOT NULL
        GROUP BY highway
        ORDER BY num_segments DESC
    """)

    return result


def estimate_connected_components(edges_df, sedona, buffer_distance=0.0005):
    df = edges_df.withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .withColumn("buffered", expr(f"ST_Buffer(geom, {buffer_distance})")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("edges")

    result = sedona.sql("""
        SELECT COUNT(*) AS total_edges,
               COUNT(DISTINCT ST_GeometryType(buffered)) AS type_count
        FROM edges
    """)

    return result


def compute_road_intersections(edges_df, sedona):
    df = edges_df.withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")
    df.createOrReplaceTempView("edges")

    result = sedona.sql("""
        SELECT a.geometry AS road_a, b.geometry AS road_b
        FROM edges a, edges b
        WHERE ST_Intersects(a.geom, b.geom) AND a.geometry != b.geometry
    """)

    return result


def summarize_bridges_tunnels(edges_df, sedona):
    df = edges_df.withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("edges")

    result = sedona.sql("""
        SELECT bridge, tunnel, COUNT(*) AS count
        FROM edges
        WHERE bridge IS NOT NULL OR tunnel IS NOT NULL
        GROUP BY bridge, tunnel
    """)

    return result


def top_longest_named_roads(edges_df, sedona, top_n=10):
    df = edges_df.withColumn("length", col("length").cast("double")) \
                 .withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("edges")

    result = sedona.sql(f"""
        SELECT name, SUM(length) AS total_length, COUNT(*) AS segments
        FROM edges
        WHERE name IS NOT NULL
        GROUP BY name
        ORDER BY total_length DESC
        LIMIT {top_n}
    """)

    return result