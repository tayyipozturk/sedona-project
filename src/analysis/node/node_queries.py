
from pyspark.sql.functions import col, expr
from config.performance_util import performance_logged

@performance_logged(label="find_nearest_node", show=False, save_path="find_nearest_node")
def find_nearest_node(nodes_df, sedona, latitude, longitude):
    df = nodes_df.withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("nodes")

    result = sedona.sql(f"""
        SELECT *,
               ST_Distance(geom, ST_Point({longitude}, {latitude})) AS distance
        FROM nodes
        ORDER BY distance ASC
        LIMIT 1
    """)

    return result

@performance_logged(label="detect_major_intersections", show=False, save_path="detect_major_intersections")
def detect_major_intersections(nodes_df, sedona, min_degree=4):
    df = nodes_df.withColumn("street_count", col("street_count").cast("int")) \
                 .withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("nodes")

    result = sedona.sql(f"""
        SELECT *
        FROM nodes
        WHERE street_count >= {min_degree}
    """)

    return result

@performance_logged(label="compute_intersection_density", show=False, save_path="intersection_density")
def compute_intersection_density(nodes_df, sedona, cell_size=0.01):
    df = nodes_df.withColumn("x", col("x").cast("double")) \
                 .withColumn("y", col("y").cast("double")) \
                 .withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("nodes")

    result = sedona.sql(f"""
        SELECT
            CAST(x / {cell_size} AS INT) AS cell_x,
            CAST(y / {cell_size} AS INT) AS cell_y,
            COUNT(*) AS intersection_count
        FROM nodes
        GROUP BY cell_x, cell_y
        ORDER BY intersection_count DESC
    """)

    return result

@performance_logged(label="estimate_urban_radius", show=False, save_path="estimate_urban_radius")
def estimate_urban_radius(nodes_df, sedona):
    df = nodes_df.withColumn("x", col("x").cast("double")) \
                 .withColumn("y", col("y").cast("double")) \
                 .withColumn("street_count", col("street_count").cast("double")) \
                 .withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL AND street_count IS NOT NULL")

    df.createOrReplaceTempView("nodes")

    centroid = sedona.sql("SELECT SUM(x * street_count)/SUM(street_count) AS cx, SUM(y * street_count)/SUM(street_count) AS cy FROM nodes").first()
    cx, cy = centroid["cx"], centroid["cy"]

    result = sedona.sql(f"""
        SELECT MAX(ST_Distance(geom, ST_Point({cx}, {cy}))) AS urban_radius,
               COUNT(*) AS total_nodes
        FROM nodes
    """)

    return result

@performance_logged(label="intersection_distribution_by_radius", show=False, save_path="intersection_distribution_by_radius")
def intersection_distribution_by_radius(nodes_df, sedona, center_x, center_y, bin_size=0.01):
    df = nodes_df.withColumn("x", col("x").cast("double")) \
                 .withColumn("y", col("y").cast("double")) \
                 .withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("nodes")

    result = sedona.sql(f"""
        SELECT
            FLOOR(ST_Distance(geom, ST_Point({center_x}, {center_y})) / {bin_size}) AS radius_bin,
            COUNT(*) AS node_count
        FROM nodes
        GROUP BY radius_bin
        ORDER BY radius_bin
    """)

    return result
