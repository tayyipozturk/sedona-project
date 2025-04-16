from pyspark.sql.functions import col, expr
from sedona.register import SedonaRegistrator

def estimate_connected_components(edges_df, spark, buffer_distance=0.0005):
    SedonaRegistrator.registerAll(spark)

    df = edges_df.withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .withColumn("buffered", expr(f"ST_Buffer(geom, {buffer_distance})")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("edges")

    result = spark.sql("""
        SELECT COUNT(*) AS total_edges,
               COUNT(DISTINCT ST_GeometryType(buffered)) AS type_count
        FROM edges
    """)

    return result


def compute_intersection_density(nodes_df, spark, cell_size=0.01):
    SedonaRegistrator.registerAll(spark)

    df = nodes_df.withColumn("x", col("x").cast("double")) \
                 .withColumn("y", col("y").cast("double")) \
                 .withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("nodes")

    result = spark.sql(f"""
        SELECT
            CAST(x / {cell_size} AS INT) AS cell_x,
            CAST(y / {cell_size} AS INT) AS cell_y,
            COUNT(*) AS intersection_count
        FROM nodes
        GROUP BY cell_x, cell_y
        ORDER BY intersection_count DESC
    """)

    return result


def find_edges_near_nodes(edges_df, nodes_df, spark, distance=0.001):
    SedonaRegistrator.registerAll(spark)

    e = edges_df.withColumn("geometry", expr("trim(geometry)")) \
                .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                .filter("geom IS NOT NULL")
    n = nodes_df.withColumn("geometry", expr("trim(geometry)")) \
                .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                .filter("geom IS NOT NULL")

    e.createOrReplaceTempView("edges")
    n.createOrReplaceTempView("nodes")

    result = spark.sql(f"""
        SELECT e.*, n.x, n.y
        FROM edges e, nodes n
        WHERE ST_Distance(e.geom, n.geom) < {distance}
    """)

    return result


def compute_road_intersections(edges_df, spark):
    SedonaRegistrator.registerAll(spark)

    df = edges_df.withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")
    df.createOrReplaceTempView("edges")

    result = spark.sql("""
        SELECT a.geometry AS road_a, b.geometry AS road_b
        FROM edges a, edges b
        WHERE ST_Intersects(a.geom, b.geom) AND a.geometry != b.geometry
    """)

    return result


def estimate_urban_radius(nodes_df, spark):
    SedonaRegistrator.registerAll(spark)

    df = nodes_df.withColumn("x", col("x").cast("double")) \
                 .withColumn("y", col("y").cast("double")) \
                 .withColumn("street_count", col("street_count").cast("double")) \
                 .withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL AND street_count IS NOT NULL")

    df.createOrReplaceTempView("nodes")

    centroid = spark.sql("SELECT SUM(x * street_count)/SUM(street_count) AS cx, SUM(y * street_count)/SUM(street_count) AS cy FROM nodes").first()
    cx, cy = centroid["cx"], centroid["cy"]

    result = spark.sql(f"""
        SELECT MAX(ST_Distance(geom, ST_Point({cx}, {cy}))) AS urban_radius,
               COUNT(*) AS total_nodes
        FROM nodes
    """)

    return result


def intersection_distribution_by_radius(nodes_df, spark, center_x, center_y, bin_size=0.01):
    SedonaRegistrator.registerAll(spark)

    df = nodes_df.withColumn("x", col("x").cast("double")) \
                 .withColumn("y", col("y").cast("double")) \
                 .withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("nodes")

    result = spark.sql(f"""
        SELECT
            FLOOR(ST_Distance(geom, ST_Point({center_x}, {center_y})) / {bin_size}) AS radius_bin,
            COUNT(*) AS node_count
        FROM nodes
        GROUP BY radius_bin
        ORDER BY radius_bin
    """)

    return result


def summarize_bridges_tunnels(edges_df, spark):
    SedonaRegistrator.registerAll(spark)

    df = edges_df.withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("edges")

    result = spark.sql("""
        SELECT bridge, tunnel, COUNT(*) AS count
        FROM edges
        WHERE bridge IS NOT NULL OR tunnel IS NOT NULL
        GROUP BY bridge, tunnel
    """)

    return result


def top_longest_named_roads(edges_df, spark, top_n=10):
    SedonaRegistrator.registerAll(spark)

    df = edges_df.withColumn("length", col("length").cast("double")) \
                 .withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("edges")

    result = spark.sql(f"""
        SELECT name, SUM(length) AS total_length, COUNT(*) AS segments
        FROM edges
        WHERE name IS NOT NULL
        GROUP BY name
        ORDER BY total_length DESC
        LIMIT {top_n}
    """)

    return result

