from pyspark.sql.functions import col, expr

def compute_dead_end_nodes(nodes_df, sedona):
    df = nodes_df.withColumn("x", col("x").cast("double")) \
                 .withColumn("y", col("y").cast("double")) \
                 .withColumn("street_count", col("street_count").cast("double")) \
                 .withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL AND street_count IS NOT NULL")

    df.createOrReplaceTempView("nodes")

    result_df = sedona.sql("""
        SELECT x, y, street_count, geometry
        FROM nodes
        WHERE street_count = 1
    """)

    return result_df
