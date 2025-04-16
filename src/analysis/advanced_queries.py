
from pyspark.sql.functions import col, expr
from sedona.register import SedonaRegistrator

def find_nearest_node(nodes_df, spark, latitude, longitude):
    SedonaRegistrator.registerAll(spark)

    df = nodes_df.withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("nodes")

    result = spark.sql(f"""
        SELECT *,
               ST_Distance(geom, ST_Point({longitude}, {latitude})) AS distance
        FROM nodes
        ORDER BY distance ASC
        LIMIT 1
    """)

    return result


def summarize_roads_by_type(edges_df, spark):
    SedonaRegistrator.registerAll(spark)

    df = edges_df.withColumn("length", col("length").cast("double")) \
                 .withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("edges")

    result = spark.sql("""
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


def compute_grid_coverage(edges_df, spark, cell_size=0.01):
    SedonaRegistrator.registerAll(spark)

    df = edges_df.withColumn("length", col("length").cast("double")) \
                 .withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .withColumn("x", expr("ST_X(ST_StartPoint(geom))")) \
                 .withColumn("y", expr("ST_Y(ST_StartPoint(geom))")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("edges")

    result = spark.sql(f"""
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


def clip_edges_to_bbox(edges_df, spark, min_x, min_y, max_x, max_y):
    SedonaRegistrator.registerAll(spark)

    df = edges_df.withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("edges")

    result = spark.sql(f"""
        SELECT *
        FROM edges
        WHERE ST_Within(geom, ST_PolygonFromEnvelope({min_x}, {min_y}, {max_x}, {max_y}))
    """)

    return result


def find_short_segments(edges_df, spark, length_threshold=5.0):
    SedonaRegistrator.registerAll(spark)

    df = edges_df.withColumn("length", col("length").cast("double")) \
                 .withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("edges")

    result = spark.sql(f"""
        SELECT *
        FROM edges
        WHERE length IS NOT NULL AND length < {length_threshold}
        ORDER BY length ASC
    """)

    return result


def detect_major_intersections(nodes_df, spark, min_degree=4):
    SedonaRegistrator.registerAll(spark)

    df = nodes_df.withColumn("street_count", col("street_count").cast("int")) \
                 .withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("nodes")

    result = spark.sql(f"""
        SELECT *
        FROM nodes
        WHERE street_count >= {min_degree}
    """)

    return result


def summarize_road_types(edges_df, spark):
    SedonaRegistrator.registerAll(spark)

    df = edges_df.withColumn("length", col("length").cast("double")) \
                 .withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("edges")

    result = spark.sql("""
        SELECT highway,
               COUNT(*) AS num_segments,
               SUM(length) AS total_length
        FROM edges
        WHERE length IS NOT NULL
        GROUP BY highway
        ORDER BY num_segments DESC
    """)

    return result
