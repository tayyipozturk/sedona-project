from pyspark.sql.functions import col, expr
from sedona.register import SedonaRegistrator

def compute_intersection_score(nodes_df, spark):
    df = nodes_df.withColumn("x", col("x").cast("double")) \
                 .withColumn("y", col("y").cast("double")) \
                 .withColumn("street_count", col("street_count").cast("double")) \
                 .withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL AND street_count IS NOT NULL")

    df.createOrReplaceTempView("nodes")

    centroid = spark.sql("SELECT AVG(x) AS cx, AVG(y) AS cy FROM nodes").first()
    cx, cy = centroid["cx"], centroid["cy"]

    result_df = spark.sql(f"""
        SELECT
            x,
            y,
            street_count,
            geometry,
            ST_Distance(geom, ST_Point({cx}, {cy})) AS distance_to_center,
            0.6 * street_count + 0.4 * (1 / (ST_Distance(geom, ST_Point({cx}, {cy})) + 1e-6)) AS intersection_score
        FROM nodes
    """)

    return result_df
