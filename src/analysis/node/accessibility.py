from pyspark.sql.functions import col, expr

def compute_accessibility_score(nodes_df, spark):
    # Cast numeric columns and create geometry
    df = nodes_df.withColumn("x", col("x").cast("double")) \
           .withColumn("y", col("y").cast("double")) \
           .withColumn("street_count", col("street_count").cast("double")) \
           .withColumn("geometry", expr("trim(geometry)")) \
           .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
           .filter("geom IS NOT NULL AND street_count IS NOT NULL")

    df.createOrReplaceTempView("nodes")

    # Compute centroid from x, y
    centroid = spark.sql("SELECT AVG(x) AS cx, AVG(y) AS cy FROM nodes").first()
    cx, cy = centroid["cx"], centroid["cy"]

    # Compute accessibility score using Sedona SQL
    result_df = spark.sql(f"""
        SELECT
            x,
            y,
            street_count,
            geometry,
            ST_Distance(geom, ST_Point({cx}, {cy})) AS distance_to_center,
            street_count / (ST_Distance(geom, ST_Point({cx}, {cy})) + 1e-6) AS accessibility_score
        FROM nodes
    """)

    return result_df
