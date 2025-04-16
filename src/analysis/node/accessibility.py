from pyspark.sql.functions import col, expr

def compute_accessibility_score(nodes_df, sedona):
    df = nodes_df.withColumn("x", col("x").cast("double")) \
           .withColumn("y", col("y").cast("double")) \
           .withColumn("street_count", col("street_count").cast("double")) \
           .withColumn("geometry", expr("trim(geometry)")) \
           .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
           .filter("geom IS NOT NULL AND street_count IS NOT NULL")

    df.createOrReplaceTempView("nodes")

    centroid = sedona.sql("SELECT AVG(x) AS cx, AVG(y) AS cy FROM nodes").first()
    cx, cy = centroid["cx"], centroid["cy"]

    result_df = sedona.sql(f"""
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
